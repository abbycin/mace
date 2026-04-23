use std::{
    ops::Deref,
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::utils::observe::{NoopObserver, Observer};

use super::OpCode;

/// Configuration options for the Mace storage engine.
#[derive(Clone)]
pub struct Options {
    /// Force-sync data to disk for every log write.
    ///
    /// The default value is `true`. Turning it off may result in data loss,
    /// while turning it on may reduce performance.
    pub sync_on_write: bool,
    /// Defaults to the hardware concurrency count and must be a power of two.
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
    /// Perform compaction when the garbage ratio exceeds this value, in the range [0, 100].
    pub data_garbage_ratio: u32,
    /// If true, compact immediately when [`Self::data_garbage_ratio`] is reached.
    pub gc_eager: bool,
    /// If [`Self::gc_eager`] is false, compaction is triggered only when active data
    /// can be compacted into [`Self::gc_compacted_size`].
    ///
    /// The default value is [`Self::data_file_size`].
    pub gc_compacted_size: usize,
    /// Size limit of a blob file.
    pub blob_file_size: usize,
    /// Trigger blob GC when the garbage ratio exceeds this value, in the range [0, 100].
    pub blob_garbage_ratio: usize,
    /// At each blob GC cycle, pick the oldest [`Self::blob_gc_ratio`]% of blob files as candidates.
    pub blob_gc_ratio: usize,
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
    /// Per-bucket logical-address page/blob cache capacity in bytes.
    ///
    /// This cache holds data by logical address for the loader fast path.
    /// Trimming is best-effort and happens in small rounds, so short-term overshoot is possible.
    ///
    /// Entries here can overlap with [`Self::cache_capacity`] and [`Self::pool_capacity`]
    /// because all three may point to the same ref-counted allocation.
    pub lru_capacity: usize,
    /// Per-bucket pool target bytes.
    ///
    /// Exceeding this does not block allocation directly; it triggers checkpoint scheduling.
    /// Therefore, this is a pressure threshold, not a strict memory ceiling.
    ///
    /// When set to 0, it is derived as `checkpoint_size * 2` during validation.
    pub pool_capacity: usize,
    /// Per-bucket target resident bytes for mapped B+Tree pages.
    ///
    /// This is a soft pressure threshold instead of a hard cap:
    /// - Evictor is nudged at ~80% usage (by default).
    /// - Eviction is activity-based and asynchronous.
    ///
    /// This is not an independent memory pool. The same data may also be referenced by
    /// [`Self::pool_capacity`] (dirty pages) and/or [`Self::lru_capacity`] (cold data),
    /// so byte accounting among these three knobs overlaps by design.
    pub cache_capacity: usize,
    /// Percentage of items evicted per round. Default is 10%.
    pub cache_evict_pct: usize,
    /// Optional hard fuse for per-bucket page/blob LRU entry count. 0 means unlimited.
    pub lru_max_entries: usize,
    /// Bitmap-cache entry count for data and blob stats.
    pub stat_mask_cache_count: usize,
    /// Ratio of high-priority cache (for non-blob data) within [`Self::lru_capacity`], in [0, 100].
    pub high_priority_ratio: usize,
    /// Maximum number of open data-file handles cached concurrently, used for loading data pages.
    pub data_handle_cache_capacity: usize,
    /// Maximum number of open blob-file handles cached concurrently, used for loading blob pages.
    pub blob_handle_cache_capacity: usize,
    /// For branch nodes, keys and indexes are always inlined.
    ///
    /// For leaf nodes, keys, value headers, and values smaller than [`Self::INLINE_SIZE`]
    /// are also always inlined.
    pub inline_size: usize,
    /// Size limit of a data file.
    pub data_file_size: usize,
    /// Maximum bytes a single checkpoint round should emit.
    ///
    /// 0 means use `data_file_size`.
    pub checkpoint_size: usize,
    /// Threshold for consolidating delta chains.
    ///
    /// The default is half of [`Self::split_elems`], which is also the maximum.
    /// Lower values may improve query performance, especially for large key-value workloads.
    pub consolidate_threshold: u16,
    /// WAL ring buffer size. Must be greater than the page size and a power of two.
    pub wal_buffer_size: usize,
    /// Number of checkpoints a transaction can span (i.e., transaction length limit).
    ///
    /// If a transaction exceeds this limit, it is forcibly aborted.
    ///
    /// NOTE: checkpoints are taken when a buffer is flushed; however, real arena data may be sparse,
    /// so this is an estimated limit.
    pub max_ckpt_per_txn: usize,
    /// WAL file size limit that triggers switching to a new WAL file, up to 2GB.
    pub wal_file_size: u32,
    /// If true, remove unused stable WAL files (never used in recovery).
    ///
    /// Default is `false`.
    pub keep_stable_wal_file: bool,
    /// Maximum number of elements in an SST (B+Tree node).
    ///
    /// The default is 512. SST size is roughly [`Self::INLINE_SIZE`] * [`Self::split_elems`].
    /// Large key-values are stored outside SST.
    ///
    /// **Once set, it cannot be modified**
    pub split_elems: u16,
    /// If true, corrupted WAL is truncated during recovery; otherwise recovery panics.
    ///
    /// Default is true.
    pub truncate_corrupted_wal: bool,
    /// Observability callback. Default is no-op.
    pub observer: Arc<dyn Observer>,
    /// Enable foreground write backpressure.
    pub enable_backpressure: bool,
}

impl Options {
    pub const DATA_FILE_SIZE: usize = 64 << 20; // 64MB
    pub const BLOB_FILE_SIZE: usize = 256 << 20; // 256MB
    pub const MAX_CONCURRENT_WRITE: u8 = 128;
    pub const MIN_CACHE_CAP: usize = Self::DATA_FILE_SIZE;
    pub const CACHE_CAP: usize = 1 << 30; // 1GB
    pub const LRU_CAPACITY: usize = 256 << 20; // 256MB
    // Assuming a MemData/BlobStat is 32 KB, 16,384 stats use ~512 MB of memory, which is reasonable.
    pub const STAT_MASK_CACHE_CNT: usize = 16384;
    pub const FILE_CACHE: usize = 512;
    pub const WAL_BUF_SZ: usize = 4 << 20; // 4MB
    pub const WAL_FILE_SZ: usize = 24 << 20; // 24MB
    pub const MIN_INLINE_SIZE: usize = 8192;
    pub const MAX_INLINE_SIZE: usize = 64 << 10;
    pub const MAX_SPLIT_ELEMS: u16 = 512;
    const MIN_SPLIT_ELEMS: u16 = 64;
    pub(crate) const MAX_KEY_SIZE: usize = 64 << 10;
    pub(crate) const MAX_KV_SIZE: usize = 1 << 30; // 1GB

    /// Creates a new Options instance with default values and the given database root.
    pub fn new<P: AsRef<Path>>(db_root: P) -> Self {
        let cores = std::thread::available_parallelism()
            .unwrap()
            .get()
            .min(Self::MAX_CONCURRENT_WRITE as usize)
            .next_power_of_two() as u8;
        Self {
            sync_on_write: true,
            concurrent_write: cores,
            tmp_store: false,
            gc_timeout: 60 * 1000,          // 1min
            checkpoint_nudge_ms: 60 * 1000, // 1min
            data_garbage_ratio: 20,         // 20%
            gc_eager: true,
            gc_compacted_size: Self::DATA_FILE_SIZE,
            blob_file_size: Self::BLOB_FILE_SIZE,
            blob_garbage_ratio: 50, // 50%
            blob_gc_ratio: 25,      // 25%
            db_root: db_root.as_ref().to_path_buf(),
            log_root: db_root.as_ref().to_path_buf(),
            cache_capacity: Self::CACHE_CAP,
            cache_evict_pct: 10, // 10%
            lru_capacity: Self::LRU_CAPACITY,
            lru_max_entries: 0,
            stat_mask_cache_count: Self::STAT_MASK_CACHE_CNT,
            high_priority_ratio: 80, // 80%
            data_handle_cache_capacity: 128,
            blob_handle_cache_capacity: 128,
            inline_size: Self::MIN_INLINE_SIZE,
            data_file_size: Self::DATA_FILE_SIZE,
            // Derived during validation so later tuning of `data_file_size` is reflected automatically.
            checkpoint_size: 0,
            pool_capacity: 0,
            consolidate_threshold: Self::MAX_SPLIT_ELEMS / 2,
            wal_buffer_size: Self::WAL_BUF_SZ,
            max_ckpt_per_txn: 1_000_000, // 1 million
            wal_file_size: Self::WAL_FILE_SZ as u32,
            keep_stable_wal_file: false,
            split_elems: Self::MAX_SPLIT_ELEMS,
            truncate_corrupted_wal: true,
            observer: Arc::new(NoopObserver),
            enable_backpressure: false,
        }
    }

    pub(crate) fn max_delta_len(&self) -> usize {
        self.split_elems as usize / 4
    }

    /// Validates the options and returns a ParsedOptions instance.
    pub fn validate(mut self) -> Result<ParsedOptions, OpCode> {
        self.concurrent_write = self.concurrent_write.clamp(1, Self::MAX_CONCURRENT_WRITE);
        self.split_elems = self
            .split_elems
            .clamp(Self::MIN_SPLIT_ELEMS, Self::MAX_SPLIT_ELEMS);
        self.inline_size = self
            .inline_size
            .clamp(Self::MIN_INLINE_SIZE, Self::MAX_INLINE_SIZE);

        if self.consolidate_threshold > self.split_elems / 2 {
            self.consolidate_threshold = self.split_elems / 2;
        }
        if self.stat_mask_cache_count == 0 {
            self.stat_mask_cache_count = Self::STAT_MASK_CACHE_CNT;
        }
        if self.cache_capacity < Self::MIN_CACHE_CAP {
            self.cache_capacity = Self::MIN_CACHE_CAP;
        }
        if self.lru_capacity == 0 {
            self.lru_capacity = Self::LRU_CAPACITY;
        }
        self.high_priority_ratio = self.high_priority_ratio.min(100);

        if self.checkpoint_size == 0 {
            self.checkpoint_size = self.data_file_size;
        }

        if self.pool_capacity == 0 {
            self.pool_capacity = self.checkpoint_size.saturating_mul(2);
        }

        if self.checkpoint_size > self.pool_capacity {
            self.checkpoint_size = self.pool_capacity;
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

        if !db_root.exists() {
            std::fs::create_dir_all(db_root)?;
        }
        if !data_root.exists() {
            std::fs::create_dir_all(data_root)?;
        }
        if !log_root.exists() {
            std::fs::create_dir_all(log_root)?;
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
}

impl Drop for ParsedOptions {
    fn drop(&mut self) {
        if self.inner.tmp_store {
            log::info!("remove db_root {:?}", self.inner.db_root);
            let _ = std::fs::remove_dir_all(&self.inner.db_root);
        }
    }
}
