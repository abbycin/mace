use std::path::{Path, PathBuf};

#[derive(Clone)]
pub struct Options {
    /// force sync data to disk for every log write, the default value is `true`, turning it off may
    /// result in data loss, while turning it on may result in performance degradation
    pub sync_on_write: bool,
    /// hardware concurrency count, range in [1, cores]
    pub workers: usize,
    /// garbage collection cycle (milliseconds)
    pub gc_timeout: u64,
    /// perform GC when garbage ratio exceed this value, range [0, 100]
    pub gc_ratio: u32,
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
    pub page_size: usize,
    /// when should we consolidate delta chain
    pub consolidate_threshold: u8,
    /// WAL ring buffer size, must greater than [`Self::page_size`]
    pub wal_buffer_size: usize,
    /// the count of checkpoints that a txn can span, ie, the length limit of a txn, if a txn length
    /// exceed the limit, it will be forcibly aborted, NOTE: checkpoint was taken when a buffer was
    /// flushed, while the real data in arena is very few, so this option is an estimated value
    pub max_ckpt_per_txn: usize,
    /// WAL file size limit which will trigger switch to new WAL file, at most 2GB
    pub wal_file_size: u32,
}

impl Options {
    pub fn new<P: AsRef<Path>>(db_root: P) -> Self {
        Self {
            sync_on_write: true,
            workers: std::thread::available_parallelism().unwrap().get(),
            tmp_store: false,
            gc_timeout: 60 * 1000, // 60s
            gc_ratio: 80,          // 80%
            db_root: db_root.as_ref().to_path_buf(),
            file_cache: 512,       // each shard has 16 entries
            cache_capacity: 2048,  // 2048 items
            cache_evict_pct: 10,   // 10%
            buffer_size: 64 << 20, // 64MB
            buffer_count: 3,       // total 192MB
            page_size: 8 << 10,    // 8KB
            consolidate_threshold: 16,
            wal_buffer_size: 4 << 20,    // 4MB
            max_ckpt_per_txn: 1_000_000, // 1 million
            wal_file_size: 100 << 20,    // 100MB
        }
    }
}

impl Options {
    pub const DATA_PREFIX: &'static str = "data_";
    pub const WAL_PREFIX: &'static str = "wal_";
    pub const WAL_FREEZED: &'static str = "freezed_wal_";

    pub fn data_file(&self, id: u32) -> PathBuf {
        self.db_root.join(format!("{}{}", Self::DATA_PREFIX, id))
    }

    pub fn wal_file(&self, id: u32) -> PathBuf {
        self.db_root.join(format!("{}{}", Self::WAL_PREFIX, id))
    }

    pub fn wal_backup(&self, id: u32) -> PathBuf {
        self.db_root.join(format!("{}{}", Self::WAL_FREEZED, id))
    }

    pub fn meta_file(&self) -> PathBuf {
        self.db_root.join("meta")
    }
}

impl Drop for Options {
    fn drop(&mut self) {
        if self.tmp_store {
            let _ = std::fs::remove_dir_all(&self.db_root);
        }
    }
}
