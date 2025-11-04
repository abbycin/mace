use std::{
    ops::Deref,
    path::{Path, PathBuf},
};

use crate::utils::lru::LRU_SHARD;

use super::OpCode;

#[derive(Clone)]
pub struct Options {
    /// force sync data to disk for every log write, the default value is `true`, turning it off may
    /// result in data loss, while turning it on may result in performance degradation
    pub sync_on_write: bool,
    /// compact all cached node on exit. default value is false, it only compact cached nodes that
    /// must be compacted
    pub compact_on_exit: bool,
    /// allows allocation of more arenas than a fixed value when arena exhaustion occurs, rather than
    /// waiting for arenas to become available, the default value is false
    pub over_provision: bool,
    /// hardware concurrency count, range in [1, cores]
    ///
    /// **Once set, it cannot be modified**
    pub workers: u16,
    /// garbage collection cycle (milliseconds)
    pub gc_timeout: u64,
    /// perform compaction when garbage ratio exceed this value, range [0, 100]
    pub gc_ratio: u32,
    /// perform compaction when [`Self::gc_ratio`] is reached
    pub gc_eager: bool,
    /// if [`Self::gc_eager`] is not set, the compaction will only be triggered when active data
    /// size can be compacted into [`Self::gc_compacted_size`], the default setting is [`Self::data_file_size`]
    pub gc_compacted_size: usize,
    /// is temperary storage, if true, db_root will be unlinked on exit
    pub tmp_store: bool,
    /// where to store database files
    pub(crate) db_root: PathBuf,
    /// where to store log files, the default value is `db_root/log`
    pub log_root: PathBuf,
    /// node cache memory size
    pub cache_capacity: usize,
    /// percent of items will be evicted at once, default is 10%
    pub cache_evict_pct: usize,
    /// delta cache count, shard into 32 slots, which act as a secondary cache to node cache
    pub cache_count: usize,
    /// for branch node, the key and index are always inlined, for leaf node, the key and val header
    /// and value that less than [`Self::INLINE_SIZE`] are always inlined too
    pub inline_size: usize,
    /// a size limit to trigger data file flush, which also control max key-value size, NOTE: too
    /// large a file size will cause the flushing to be slow
    pub data_file_size: usize,
    /// if wal log file size large than this value, a checkpoint will be created
    pub max_log_size: usize,
    /// when should we consolidate delta chain, the default value is set to half [`Self::split_elems`]
    /// and it also the maximum value, shrink this value may get better query performance (especially
    /// in large key-value workload)
    pub consolidate_threshold: u16,
    /// WAL ring buffer size, must greater than [`Self::page_size`] and must be power of 2
    pub wal_buffer_size: usize,
    /// the count of checkpoints that a txn can span, ie, the length limit of a txn, if a txn length
    /// exceed the limit, it will be forcibly aborted, NOTE: checkpoint was taken when a buffer was
    /// flushed, while the real data in arena is very few, so this option is an estimated value
    pub max_ckpt_per_txn: usize,
    /// WAL file size limit which will trigger switch to new WAL file, at most 2GB
    pub wal_file_size: u32,
    /// if set to true, the unused stable wal file (never used in recovery) will be removed, default
    /// value is `false`
    pub keep_stable_wal_file: bool,
    /// control max elements in sst (B+ Tree Node), the default value is 512, the sst size is around
    /// [`Self::INLINE_SIZE`] * [`Self::split_elems`], big KVs will be stored outside of sst
    ///
    /// **Once set, it cannot be modified**
    pub split_elems: u16,
}

impl Options {
    pub const DATA_FILE_SIZE: usize = 64 << 20; // 64MB
    pub const MAX_WORKERS: usize = 128;
    pub const MIN_CACHE_CAP: usize = Self::DATA_FILE_SIZE;
    pub const CACHE_CAP: usize = 1 << 30; // 1GB
    pub const CACHE_CNT: usize = 16384;
    pub const FILE_CACHE: usize = 512;
    pub const WAL_BUF_SZ: usize = 8 << 20; // 8MB
    pub const WAL_FILE_SZ: usize = 24 << 20; // 24MB
    pub const INLINE_SIZE: usize = 8192;
    pub const MAX_SPLIT_ELEMS: u16 = 512;
    pub const MAX_LOG_SIZE: usize = 72 << 20;
    const MIN_SPLIT_ELEMS: u16 = 64;
    pub(crate) const MAX_KV_SIZE: usize = 1 << 30; // 1GB

    pub fn new<P: AsRef<Path>>(db_root: P) -> Self {
        Self {
            sync_on_write: true,
            compact_on_exit: false,
            over_provision: false,
            workers: std::thread::available_parallelism()
                .unwrap()
                .get()
                .min(Self::MAX_WORKERS) as u16,
            tmp_store: false,
            gc_timeout: 60 * 1000, // 1min
            gc_ratio: 20,          // 20%
            gc_eager: true,
            gc_compacted_size: Self::DATA_FILE_SIZE,
            db_root: db_root.as_ref().to_path_buf(),
            log_root: db_root.as_ref().to_path_buf(),
            cache_capacity: Self::CACHE_CAP,
            cache_evict_pct: 10, // 10%
            cache_count: Self::CACHE_CNT,
            inline_size: Self::INLINE_SIZE,
            data_file_size: Self::DATA_FILE_SIZE,
            max_log_size: Self::MAX_LOG_SIZE,
            consolidate_threshold: Self::MAX_SPLIT_ELEMS / 2,
            wal_buffer_size: Self::WAL_BUF_SZ,
            max_ckpt_per_txn: 1_000_000, // 1 million
            wal_file_size: Self::WAL_FILE_SZ as u32,
            keep_stable_wal_file: false,
            split_elems: Self::MAX_SPLIT_ELEMS,
        }
    }

    pub(crate) fn max_delta_len(&self) -> usize {
        self.split_elems as usize / 100
    }

    pub fn validate(mut self) -> Result<ParsedOptions, OpCode> {
        self.split_elems = self
            .split_elems
            .clamp(Self::MIN_SPLIT_ELEMS, Self::MAX_SPLIT_ELEMS);
        if self.consolidate_threshold > self.split_elems / 2 {
            self.consolidate_threshold = self.split_elems / 2;
        }
        if self.cache_count < LRU_SHARD {
            self.cache_count = Self::CACHE_CNT;
        }
        if self.cache_capacity < Self::MIN_CACHE_CAP {
            self.cache_capacity = Self::CACHE_CAP;
        }

        self.create_dir().map_err(|e| {
            eprintln!("create dir fail {e:?}");
            OpCode::IoError
        })?;
        Ok(ParsedOptions { inner: self })
    }

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

#[derive(Clone)]
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
    pub const WAL_PREFIX: &'static str = "wal";
    pub const WAL_STABLE: &'static str = "stable-wal";
    pub const MANIFEST_PREFIX: &'static str = "manifest";

    pub fn data_root(&self) -> PathBuf {
        self.db_root().join("data")
    }

    pub fn data_file(&self, id: u64) -> PathBuf {
        self.data_root()
            .join(format!("{}{}{}", Self::DATA_PREFIX, Self::SEP, id))
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

    pub fn wal_file(&self, wid: u16, seq: u64) -> PathBuf {
        self.log_root().join(format!(
            "{}{}{}{}{}",
            Self::WAL_PREFIX,
            Self::SEP,
            wid,
            Self::SEP,
            seq
        ))
    }

    pub fn wal_backup(&self, wid: u16, seq: u64) -> PathBuf {
        self.log_root().join(format!(
            "{}{}{}{}{}",
            Self::WAL_STABLE,
            Self::SEP,
            wid,
            Self::SEP,
            seq
        ))
    }

    pub fn desc_file(&self, wid: u16) -> PathBuf {
        self.log_root().join(format!("meta_{wid}"))
    }

    pub fn manifest(&self, seq: u64) -> PathBuf {
        self.log_root()
            .join(format!("{}{}{}", Self::MANIFEST_PREFIX, Self::SEP, seq))
    }

    pub fn snapshot(&self, seq: u64) -> PathBuf {
        self.db_root.join(format!(
            "{}{}{}.snapshot",
            Self::MANIFEST_PREFIX,
            Self::SEP,
            seq
        ))
    }
}

impl Drop for Options {
    fn drop(&mut self) {
        if self.tmp_store {
            log::info!("remove db_root {:?}", self.db_root);
            let _ = std::fs::remove_dir_all(&self.db_root);
        }
    }
}
