use std::path::{Path, PathBuf};

#[derive(Clone)]
pub struct Options {
    /// hardware concurrency count, range in [1, cores]
    pub workers: usize,
    /// garbage collection cycle (milliseconds)
    pub gc_timeout: u64,
    /// percentage of active data in [`Self::buffer_size`], ratio below that will trigger GC
    pub gc_ratio: u8,
    /// is temperary storage, if true, db_root will be unlinked when quit
    pub tmp_store: bool,
    /// where to store database files
    pub db_root: PathBuf,
    /// cache memory size, unit in element count, not element size
    pub cache_capacity: usize,
    /// percent of items will evict at once, default is 10%
    pub cache_evict_pct: usize,
    /// buffer pool element size
    pub buffer_size: u32,
    /// buffer pool element count, must > 2
    pub buffer_count: u32,
    /// B+ Tree node size, mut less than half of [`Self::buffer_size`]
    pub page_size: usize,
    /// when should we consolidate delta chain
    pub consolidate_threshold: u8,
    /// WAL ring buffer size, must greater than [`Self::page_size`]
    pub wal_buffer_size: usize,
    /// start checkpoint every [`Self::chkpt_per_bytes`] data was written to WAL
    pub ckpt_per_bytes: usize,
    /// the number checkpoints that a txn can cross, ie, the length limit of a txn, if a txn length
    /// exceed the limit, it will be forcibly aborted
    pub max_ckpt_per_txn: usize,
    /// WAL file size limit which will trigger switch to new WAL file
    pub wal_file_size: usize,
    /// the available disk space at least two [`Self::data_file_size`] <br>
    /// must greater than [`Self::buffer_size`] and less than 2^40 <br>
    /// the maximum size that can be managed by mace is: (2^16 - 1) * 2^40 ~= 64PB
    pub data_file_size: usize,
}

impl Options {
    pub fn new<P: AsRef<Path>>(db_root: P) -> Self {
        Self {
            workers: std::thread::available_parallelism().unwrap().get(),
            tmp_store: false,
            gc_timeout: 60 * 1000, // 60s
            gc_ratio: 80,          // 80%
            db_root: db_root.as_ref().to_path_buf(),
            cache_capacity: 2048, // 2048 items
            cache_evict_pct: 10,  // 10 %
            buffer_size: 4 << 20, // 4MB
            buffer_count: 3,      // total 12MB
            page_size: 16 << 10,  // 16KB
            consolidate_threshold: 16,
            wal_buffer_size: 4 << 20, // 4MB
            ckpt_per_bytes: 4 << 20,  // 4MB
            max_ckpt_per_txn: 1024,   // 4GB
            wal_file_size: 100 << 20, // 100MB
            data_file_size: 4 << 30,  // 4GB
        }
    }
}

impl Options {
    pub const DATA_PREFIX: &'static str = "data_";
    pub const WAL_PREFIX: &'static str = "wal_";
    pub const WAL_FREEZED: &'static str = "freezed_wal_";

    pub fn data_file(&self, id: u16) -> PathBuf {
        self.db_root.join(format!("{}{}", Self::DATA_PREFIX, id))
    }

    pub fn wal_file(&self, id: u16) -> PathBuf {
        self.db_root.join(format!("{}{}", Self::WAL_PREFIX, id))
    }

    pub fn wal_backup(&self, id: u16) -> PathBuf {
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
