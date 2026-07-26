use std::{
    ops::Deref,
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{
    io,
    utils::observe::{NoopObserver, Observer},
};
use serde::{Deserialize, Serialize};

use super::OpCode;

fn dir_parent_for_sync(path: &Path) -> Option<&Path> {
    match path.parent() {
        Some(parent) if parent.as_os_str().is_empty() => Some(Path::new(".")),
        Some(parent) => Some(parent),
        None => None,
    }
}

fn create_dir_all_and_sync(fs: &dyn io::FileSystem, path: &Path) -> std::io::Result<()> {
    if fs.try_exists(path)? {
        return Ok(());
    }

    let mut created = Vec::new();
    let mut cursor = Some(path);
    while let Some(dir) = cursor {
        if fs.try_exists(dir)? {
            break;
        }
        created.push(dir.to_path_buf());
        cursor = dir.parent();
    }

    fs.create_dir_all(path)?;

    created.reverse();
    for dir in created {
        if let Some(parent) = dir_parent_for_sync(&dir) {
            fs.sync_dir(parent)?;
        }
    }
    Ok(())
}

/// Configuration options for the Mace storage engine.
#[derive(Clone)]
pub struct Options {
    /// Force-sync data to disk for every wal/data write.
    ///
    /// The default value is `true` (use fsync or else use fdatasync). Turning it off may result in
    /// data loss, while turning it on may reduce performance.
    pub sync_on_write: bool,
    /// Writer group count. Default is [`Self::CONCURRENT_WRITE`] and it must be in the range `[1, 128]`
    ///
    /// **Once set, it cannot be modified**
    pub concurrent_write: u8,
    /// Garbage collection cycle interval (milliseconds).
    pub gc_timeout: u64,
    /// Proactive page-checkpoint trigger interval (milliseconds).
    ///
    /// When a bucket has pending dirty pages but no foreground write reaches checkpoint thresholds,
    /// the evictor triggers checkpoint near this interval to prevent WAL checkpoint stalling.
    ///
    /// Set to 0 to disable proactive triggering.
    pub checkpoint_nudge_ms: u64,
    /// Perform compaction when the garbage ratio exceeds this value, in the range `[0, 100]`
    pub data_garbage_ratio: u32,
    /// If true, compact immediately when [`Self::data_garbage_ratio`] is reached.
    pub gc_eager: bool,
    /// Size limit of a blob file. Default is [`Self::BLOB_FILE_SIZE`]
    pub blob_file_size: usize,
    /// Trigger blob GC when the garbage ratio exceeds this value, in the range `[0, 100]`
    pub blob_garbage_ratio: u32,
    /// Whether this is temporary storage.
    ///
    /// If true, `db_root` will be removed on exit.
    pub tmp_store: bool,
    /// Directory where database files are stored.
    pub(crate) db_root: PathBuf,
    /// Directory where log files are stored.
    ///
    /// The default value is `db_root/log`.
    pub log_root: PathBuf,
    /// Shared logical-address cache capacity in bytes.
    ///
    /// This cache keeps file-loaded blob values and auxiliary history/sibling pages.
    /// Resident tree pages and dirty pool pages are accounted elsewhere and are not inserted here.
    /// Trimming is best-effort and happens in small rounds, so short-term overshoot is possible.
    ///
    /// Different subsystems may transiently hold refs to the same allocation.
    pub lru_capacity: usize,
    /// Bitmap-cache entry count for data and blob stats.
    pub stat_mask_cache_count: usize,
    /// Maximum number of open data-file handles cached concurrently, used for loading data pages.
    pub data_handle_cache_capacity: usize,
    /// Maximum number of open blob-file handles cached concurrently, used for loading blob pages.
    pub blob_handle_cache_capacity: usize,
    /// Size limit of a data file. Minimum is [`Self::DATA_FILE_SIZE`]
    pub data_file_size: usize,
    /// WAL ring buffer size. Must be greater than the page size and a power of two.
    pub wal_buffer_size: usize,
    /// Number of checkpoints a transaction can span (i.e., transaction length limit).
    ///
    /// If a transaction exceeds this limit, it is forcibly aborted.
    pub max_ckpt_per_txn: usize,
    /// WAL file size limit that triggers switching to a new WAL file, up to 2GB.
    pub wal_file_size: u32,
    /// If true, remove unused stable WAL files (never used in recovery).
    ///
    /// Default is `false`.
    pub keep_stable_wal_file: bool,
    /// If true, corrupted WAL is truncated during recovery; otherwise recovery panics.
    ///
    /// Default is true.
    pub truncate_corrupted_wal: bool,
    /// Observability callback. Default is no-op.
    pub observer: Arc<dyn Observer>,
    /// Filesystem hook for namespace operations and runtime file opens.
    ///
    /// The default value uses the host operating system filesystem.
    pub(crate) fs: Arc<dyn io::FileSystem>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct BucketOptions {
    /// Per-bucket target resident bytes for mapped B+Tree pages.
    pub cache_capacity: usize,
    /// Percentage of items evicted per round. Range is `[10, 80]`, default is `20%`
    pub cache_evict_pct: usize,
    /// Per-bucket pool target bytes. Default is [`Self::POOL_CAP`]
    pub pool_capacity: usize,
    /// Maximum bytes a single checkpoint round should emit. Default is [`Self::CHECKPOINT_SIZE`]
    pub checkpoint_size: usize,
    /// For branch nodes, keys and indexes are always inlined. For leaf nodes, only values smaller
    /// than `inline_size` are inlined; values whose size is equal to or greater than `inline_size`
    /// are stored as blobs. Default is [`Self::MIN_INLINE_SIZE`]
    pub inline_size: usize,
    /// Maximum number of elements in an SST (B+Tree node). Default is [`Self::MAX_SPLIT_ELEMS`]
    pub split_elems: u16,
    /// Threshold for consolidating delta chains. Range is `[16, Self::split_elems / 2]`
    pub consolidate_threshold: u16,
    /// Enable zstd compression for persisted data/blob records generated by this bucket
    pub enable_compression: bool,
    /// Enable foreground write backpressure. Default is `true`
    pub enable_backpressure: bool,
    pub _padding: [u8; 2],
}

impl Default for BucketOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl BucketOptions {
    pub const MIN_SPLIT_ELEMS: u16 = 64;
    pub const MAX_SPLIT_ELEMS: u16 = 512;
    pub const CACHE_CAP: usize = 1 << 30; // 1GB
    pub const POOL_CAP: usize = 1 << 30; // 1GB
    pub const CHECKPOINT_SIZE: usize = 256 << 20; // 256MB
    pub const MIN_INLINE_SIZE: usize = 4096;
    pub const MAX_INLINE_SIZE: usize = 16384;

    pub fn new() -> Self {
        Self {
            cache_capacity: Self::CACHE_CAP,
            cache_evict_pct: 20,
            pool_capacity: Self::POOL_CAP,
            checkpoint_size: Self::CHECKPOINT_SIZE,
            consolidate_threshold: Self::MAX_SPLIT_ELEMS / 2,
            inline_size: Self::MIN_INLINE_SIZE,
            split_elems: Self::MAX_SPLIT_ELEMS,
            enable_compression: false,
            enable_backpressure: true,
            _padding: [0u8; 2],
        }
    }

    pub fn validate(mut self) -> BucketOptions {
        if self.checkpoint_size == 0 {
            self.checkpoint_size = Self::CHECKPOINT_SIZE;
        }
        if self.pool_capacity == 0 {
            self.pool_capacity = Self::POOL_CAP;
        }
        if self.cache_capacity == 0 {
            self.cache_capacity = Self::CACHE_CAP;
        }
        if self.checkpoint_size > self.pool_capacity {
            self.checkpoint_size = self.pool_capacity;
        }
        self.cache_evict_pct = self.cache_evict_pct.clamp(10, 80);
        self.split_elems = self
            .split_elems
            .clamp(Self::MIN_SPLIT_ELEMS, Self::MAX_SPLIT_ELEMS);
        self.consolidate_threshold = self.consolidate_threshold.clamp(16, self.split_elems / 2);
        self.inline_size = self
            .inline_size
            .clamp(Self::MIN_INLINE_SIZE, Self::MAX_INLINE_SIZE);
        self
    }
}

impl Options {
    pub const CONCURRENT_WRITE: u8 = 16;
    pub const MAX_CONCURRENT_WRITE: u8 = 128;
    pub const DATA_FILE_SIZE: usize = 64 << 20; // 64MB
    pub const BLOB_FILE_SIZE: usize = 256 << 20; // 256MB
    pub const LRU_CAPACITY: usize = 256 << 20; // 256MB
    // Assuming a MemData/BlobStat is 32 KB, 16,384 stats use ~512 MB of memory, which is reasonable.
    pub const STAT_MASK_CACHE_CNT: usize = 16384;
    pub const WAL_BUF_SZ: usize = 16 << 20; // 16MB
    pub const WAL_FILE_SZ: usize = 64 << 20; // 64MB

    pub(crate) const MAX_KEY_SIZE: usize = 64 << 10;
    pub(crate) const MAX_KV_SIZE: usize = 1 << 30; // 1GB

    /// Creates a new Options instance with default values and the given database root.
    pub fn new<P: AsRef<Path>>(db_root: P) -> Self {
        Self {
            sync_on_write: true,
            concurrent_write: Self::CONCURRENT_WRITE,
            tmp_store: false,
            gc_timeout: 60 * 1000,          // 1min
            checkpoint_nudge_ms: 60 * 1000, // 1min
            data_garbage_ratio: 20,         // 20%
            gc_eager: true,
            blob_file_size: Self::BLOB_FILE_SIZE,
            blob_garbage_ratio: 50, // 50%
            db_root: db_root.as_ref().to_path_buf(),
            log_root: db_root.as_ref().to_path_buf(),
            lru_capacity: Self::LRU_CAPACITY,
            stat_mask_cache_count: Self::STAT_MASK_CACHE_CNT,
            data_handle_cache_capacity: 128,
            blob_handle_cache_capacity: 128,
            data_file_size: Self::DATA_FILE_SIZE,
            wal_buffer_size: Self::WAL_BUF_SZ,
            max_ckpt_per_txn: 1_000_000, // 1 million
            wal_file_size: Self::WAL_FILE_SZ as u32,
            keep_stable_wal_file: false,
            truncate_corrupted_wal: true,
            observer: Arc::new(NoopObserver),
            fs: Arc::new(io::OsFileSystem),
        }
    }

    /// Validates the options and returns a ParsedOptions instance.
    pub fn validate(mut self) -> Result<ParsedOptions, OpCode> {
        self.concurrent_write = self
            .concurrent_write
            .clamp(1, Self::MAX_CONCURRENT_WRITE)
            .next_power_of_two();
        if self.stat_mask_cache_count == 0 {
            self.stat_mask_cache_count = Self::STAT_MASK_CACHE_CNT;
        }
        if self.lru_capacity == 0 {
            self.lru_capacity = Self::LRU_CAPACITY;
        }
        if self.data_file_size == 0 {
            self.data_file_size = Self::DATA_FILE_SIZE;
        }
        if self.blob_file_size == 0 {
            self.blob_file_size = Self::BLOB_FILE_SIZE;
        }

        self.create_dir().map_err(|e| {
            eprintln!("create dir fail {e:?}");
            OpCode::IoError
        })?;
        Ok(ParsedOptions { inner: self })
    }

    /// Creates the directory structure for the database.
    pub fn create_dir(&self) -> std::io::Result<()> {
        let (db_root, data_root, log_root) = (self.db_root(), self.data_root(), self.log_root());

        if !self.fs.try_exists(&db_root)? {
            create_dir_all_and_sync(self.fs.as_ref(), &db_root)?;
        }
        if !self.fs.try_exists(&data_root)? {
            create_dir_all_and_sync(self.fs.as_ref(), &data_root)?;
        }
        if !self.fs.try_exists(&log_root)? {
            create_dir_all_and_sync(self.fs.as_ref(), &log_root)?;
        }
        Ok(())
    }
}

pub struct ParsedOptions {
    pub(crate) inner: Options,
}

impl Deref for ParsedOptions {
    type Target = Options;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Options {
    pub const SEP: &'static str = "_";
    pub const DATA_PREFIX: &'static str = "data";
    pub const BLOB_PREFIX: &'static str = "blob";
    pub const WAL_PREFIX: &'static str = "wal";
    pub const WAL_STABLE: &'static str = "stable-wal";
    pub const MANIFEST: &'static str = "manifest";

    pub fn data_root(&self) -> PathBuf {
        self.db_root().join("data")
    }

    pub fn data_file(&self, id: u64) -> PathBuf {
        self.data_root()
            .join(format!("{}{}{}", Self::DATA_PREFIX, Self::SEP, id))
    }

    pub fn blob_file(&self, id: u64) -> PathBuf {
        self.data_root()
            .join(format!("{}{}{}", Self::BLOB_PREFIX, Self::SEP, id))
    }

    pub fn log_root(&self) -> PathBuf {
        if self.log_root == self.db_root {
            self.db_root.join("log")
        } else {
            self.log_root.clone()
        }
    }

    pub fn db_root(&self) -> PathBuf {
        self.db_root.clone()
    }

    pub fn wal_file(&self, group_id: u8, seq: u64) -> PathBuf {
        self.log_root().join(format!(
            "{}{}{}{}{}",
            Self::WAL_PREFIX,
            Self::SEP,
            group_id,
            Self::SEP,
            seq
        ))
    }

    pub fn wal_backup(&self, group_id: u8, seq: u64) -> PathBuf {
        self.log_root().join(format!(
            "{}{}{}{}{}",
            Self::WAL_STABLE,
            Self::SEP,
            group_id,
            Self::SEP,
            seq
        ))
    }

    pub fn manifest(&self) -> PathBuf {
        self.log_root().join(Self::MANIFEST)
    }

    pub(crate) fn sync_data_dir(&self) {
        self.fs
            .as_ref()
            .sync_dir(&self.data_root())
            .unwrap_or_else(|e| panic!("can't fail, {:?}", e));
    }

    pub(crate) fn sync_log_dir(&self) {
        self.fs
            .as_ref()
            .sync_dir(&self.log_root())
            .unwrap_or_else(|e| panic!("can't fail, {:?}", e));
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct PersistedOptions {
    pub concurrent_write: u8,
    pub sync_on_write: bool,
    pub gc_timeout: u64,
    pub checkpoint_nudge_ms: u64,
    pub data_garbage_ratio: u32,
    pub gc_eager: bool,
    pub blob_file_size: usize,
    pub blob_garbage_ratio: u32,
    pub lru_capacity: usize,
    pub stat_mask_cache_count: usize,
    pub data_handle_cache_capacity: usize,
    pub blob_handle_cache_capacity: usize,
    pub data_file_size: usize,
    pub wal_buffer_size: usize,
    pub max_ckpt_per_txn: usize,
    pub wal_file_size: u32,
    pub keep_stable_wal_file: bool,
    pub truncate_corrupted_wal: bool,
}

impl Default for PersistedOptions {
    fn default() -> Self {
        Self {
            concurrent_write: Options::CONCURRENT_WRITE,
            sync_on_write: true,
            gc_timeout: 60 * 1000,
            checkpoint_nudge_ms: 60 * 1000,
            data_garbage_ratio: 20,
            gc_eager: true,
            blob_file_size: Options::BLOB_FILE_SIZE,
            blob_garbage_ratio: 50,
            lru_capacity: Options::LRU_CAPACITY,
            stat_mask_cache_count: Options::STAT_MASK_CACHE_CNT,
            data_handle_cache_capacity: 128,
            blob_handle_cache_capacity: 128,
            data_file_size: Options::DATA_FILE_SIZE,
            wal_buffer_size: Options::WAL_BUF_SZ,
            max_ckpt_per_txn: 1_000_000,
            wal_file_size: Options::WAL_FILE_SZ as u32,
            keep_stable_wal_file: false,
            truncate_corrupted_wal: true,
        }
    }
}

impl PersistedOptions {
    pub(crate) fn from_options(opt: &Options) -> Self {
        Self {
            concurrent_write: opt.concurrent_write,
            sync_on_write: opt.sync_on_write,
            gc_timeout: opt.gc_timeout,
            checkpoint_nudge_ms: opt.checkpoint_nudge_ms,
            data_garbage_ratio: opt.data_garbage_ratio,
            gc_eager: opt.gc_eager,
            blob_file_size: opt.blob_file_size,
            blob_garbage_ratio: opt.blob_garbage_ratio,
            lru_capacity: opt.lru_capacity,
            stat_mask_cache_count: opt.stat_mask_cache_count,
            data_handle_cache_capacity: opt.data_handle_cache_capacity,
            blob_handle_cache_capacity: opt.blob_handle_cache_capacity,
            data_file_size: opt.data_file_size,
            wal_buffer_size: opt.wal_buffer_size,
            max_ckpt_per_txn: opt.max_ckpt_per_txn,
            wal_file_size: opt.wal_file_size,
            keep_stable_wal_file: opt.keep_stable_wal_file,
            truncate_corrupted_wal: opt.truncate_corrupted_wal,
        }
    }

    pub(crate) fn from_json(buf: &[u8]) -> Result<Self, OpCode> {
        serde_json::from_slice(buf).map_err(|_| OpCode::Corruption)
    }

    pub(crate) fn to_json(&self) -> Result<Vec<u8>, OpCode> {
        serde_json::to_vec(self).map_err(|_| OpCode::Corruption)
    }

    pub(crate) fn check_compatible(&self, opt: &Options) -> Result<(), OpCode> {
        if self.concurrent_write != opt.concurrent_write {
            return Err(OpCode::Invalid);
        }
        Ok(())
    }
}

impl Drop for ParsedOptions {
    fn drop(&mut self) {
        if self.inner.tmp_store {
            log::info!("remove db_root {:?}", self.inner.db_root);
            let _ = std::fs::remove_dir_all(&self.inner.db_root);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{io::ErrorKind, sync::Arc};

    use crate::{
        OpCode, RandomPath,
        io::testfs::{InjectOp, InjectedFileSystem},
    };

    use super::{Options, PersistedOptions};

    #[test]
    fn validate_returns_io_error_when_create_dir_all_fails() {
        let root = RandomPath::tmp();
        let mut opt = Options::new(&*root);
        let fs = Arc::new(InjectedFileSystem::new());
        fs.fail_once(
            InjectOp::CreateDirAll,
            opt.db_root(),
            ErrorKind::PermissionDenied,
        );
        opt.fs = fs;
        let err = opt.validate().err().expect("validate must fail");
        assert_eq!(err, OpCode::IoError);
    }

    #[test]
    fn validate_returns_io_error_when_parent_sync_fails() {
        let root = RandomPath::tmp();
        let mut opt = Options::new(&*root);
        let parent = opt
            .db_root()
            .parent()
            .expect("db_root must have parent")
            .to_path_buf();
        let fs = Arc::new(InjectedFileSystem::new());
        fs.fail_once(InjectOp::SyncDir, parent, ErrorKind::PermissionDenied);
        opt.fs = fs;
        let err = opt.validate().err().expect("validate must fail");
        assert_eq!(err, OpCode::IoError);
    }

    #[test]
    fn persisted_options_fill_missing_fields_with_defaults() {
        let json = br#"{"concurrent_write":8}"#;
        let opt = PersistedOptions::from_json(json).expect("decode persisted options");
        let expected = PersistedOptions {
            concurrent_write: 8,
            ..Default::default()
        };
        assert_eq!(opt, expected);
    }

    #[test]
    fn persisted_options_ignore_removed_fields() {
        let json = br#"{"concurrent_write":16,"old_field":true}"#;
        let opt = PersistedOptions::from_json(json).expect("decode persisted options");
        let expected = PersistedOptions {
            concurrent_write: 16,
            ..Default::default()
        };
        assert_eq!(opt, expected);
    }

    #[test]
    fn persisted_options_reject_concurrent_write_conflicts() {
        let root = RandomPath::tmp();
        let mut opt = Options::new(&*root);
        opt.concurrent_write = 8;
        let persisted = PersistedOptions::from_options(&opt.validate().unwrap());

        let reopen_root = RandomPath::tmp();
        let mut reopen = Options::new(&*reopen_root);
        reopen.concurrent_write = 16;
        let reopen = reopen.validate().unwrap();

        assert_eq!(persisted.check_compatible(&reopen), Err(OpCode::Invalid));
    }
}
