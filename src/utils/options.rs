use std::{
    ops::Deref,
    path::{Path, PathBuf},
};

use super::OpCode;

#[derive(Clone)]
pub struct Options {
    /// force sync data to disk for every log write, the default value is `true`, turning it off may
    /// result in data loss, while turning it on may result in performance degradation
    pub sync_on_write: bool,
    /// hardware concurrency count, range in [1, cores]
    pub workers: usize,
    /// garbage collection cycle (milliseconds)
    pub gc_timeout: u64,
    /// perform compaction when garbage ratio exceed this value, range [0, 100]
    pub gc_ratio: u32,
    /// perform compaction when [`Self::gc_ratio`] is reached
    pub gc_eager: bool,
    /// if [`Self::gc_eager`] is not set, the compaction will only be triggered when active data
    /// size can be compacted into [`Self::gc_compacted_size`], the default setting is [`Self::buffer_size`]
    pub gc_compacted_size: u32,
    /// is temperary storage, if true, db_root will be unlinked when quit
    pub tmp_store: bool,
    /// where to store database files
    pub db_root: PathBuf,
    /// number of file meta will be cached, it will be sharded into 32 slots
    pub file_cache: usize,
    /// cache memory size, unit in element count, not element size
    pub cache_capacity: usize,
    /// percent of items will evict at once, default is 10%
    pub cache_evict_pct: usize,
    /// buffer pool element size, also the flushed file size
    pub buffer_size: u32,
    /// buffer pool element count, must > 2
    pub buffer_count: u32,
    /// B+ Tree node size, mut less than half of [`Self::buffer_size`]
    pub page_size: u32,
    /// when should we consolidate delta chain, the best value is 16
    pub consolidate_threshold: u8,
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

    // private to crate
    /// max kv size that can store directly into page
    pub(crate) inline_size: u32,
    pub(crate) intl_split_size: u32,
    pub(crate) max_kv_size: u32,
    pub(crate) intl_consolidate_threshold: u8,
}

impl Options {
    pub const DEFAULT_BUFFER_SIZE: u32 = 64 << 20; // 64MB
    pub const MAX_WORKERS: usize = 128;

    pub fn new<P: AsRef<Path>>(db_root: P) -> Self {
        Self {
            sync_on_write: true,
            workers: std::thread::available_parallelism()
                .unwrap()
                .get()
                .min(Self::MAX_WORKERS),
            tmp_store: false,
            gc_timeout: 60 * 1000, // 60s
            gc_ratio: 80,          // 80%
            gc_eager: false,
            gc_compacted_size: Self::DEFAULT_BUFFER_SIZE,
            db_root: db_root.as_ref().to_path_buf(),
            file_cache: 512,      // each shard has 16 entries
            cache_capacity: 2048, // 2048 items
            cache_evict_pct: 10,  // 10%
            buffer_size: Self::DEFAULT_BUFFER_SIZE,
            buffer_count: 3,    // total 192MB
            page_size: 8 << 10, // 8KB
            consolidate_threshold: 24,
            wal_buffer_size: 4 << 20,    // 4MB
            max_ckpt_per_txn: 1_000_000, // 1 million
            wal_file_size: 100 << 20,    // 100MB
            // private to
            inline_size: 0,
            intl_split_size: 0,
            max_kv_size: 0,
            intl_consolidate_threshold: 0,
            keep_stable_wal_file: false,
        }
    }

    /// the length of the kv plus the length of the metadata must be less than this value for now
    #[inline(always)]
    pub(crate) fn max_data_size(&self) -> usize {
        self.max_kv_size as usize
    }

    pub fn validate(mut self) -> Result<ParsedOptions, OpCode> {
        self.inline_size = self.page_size / 2;
        self.intl_split_size = self.page_size / 2;
        self.max_kv_size = self.buffer_size / 2;
        self.intl_consolidate_threshold = self.consolidate_threshold / 2;
        Ok(ParsedOptions { inner: self })
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
    pub const DATA_PREFIX: &'static str = "data_";
    pub const MAP_PREFIX: &'static str = "map_";
    pub const WAL_PREFIX: &'static str = "wal_";
    pub const WAL_STABLE: &'static str = "stable_wal_";

    pub fn state_file(&self) -> PathBuf {
        self.db_root.join("state")
    }

    pub fn map_file_tmp(&self, id: u32) -> PathBuf {
        self.db_root.join(format!("{}{}_tmp", Self::MAP_PREFIX, id))
    }

    pub fn map_file(&self, id: u32) -> PathBuf {
        self.db_root.join(format!("{}{}", Self::MAP_PREFIX, id))
    }

    pub fn data_file(&self, id: u32) -> PathBuf {
        self.db_root.join(format!("{}{}", Self::DATA_PREFIX, id))
    }

    pub fn wal_root(&self) -> PathBuf {
        self.db_root.join("wal")
    }

    pub fn wal_file(&self, wid: u16, seq: u32) -> PathBuf {
        self.wal_root()
            .join(format!("{}{}_{}", Self::WAL_PREFIX, wid, seq))
    }

    pub fn wal_backup(&self, wid: u16, seq: u32) -> PathBuf {
        self.wal_root()
            .join(format!("{}{}_{}", Self::WAL_STABLE, wid, seq))
    }

    pub fn desc_file(&self, wid: u16) -> PathBuf {
        if wid == u16::MAX {
            self.db_root.join("meta")
        } else {
            self.wal_root().join(format!("meta_{}", wid))
        }
    }
    pub fn meta_file(&self) -> PathBuf {
        self.desc_file(u16::MAX)
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
