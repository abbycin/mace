use std::path::{Path, PathBuf};

use super::OpCode;

const DEFAULT_WRITE_BUFFER_SIZE: usize = 1 << 20;
const DEFAULT_PAGE_SIZE: usize = 16 << 10;
const DEFAULT_CACHE_CAPACITY: usize = 256;
const DEFAULT_EVICT_PCT: usize = 10;

/// mace database configuration options
#[derive(Clone)]
pub struct Options {
    /// should we create a new instance or panic when recover a database was failed
    pub panic_on_recover: bool,
    /// allow bind thread to current running CPU core
    pub bind_core: bool,
    /// is temperary storage, if true, db_root will be unlinked when quit
    pub tmp_store: bool,
    /// where to store database files
    pub db_root: PathBuf,
    ///  meta file
    pub meta_name: String,
    /// how many worker thread will be created <br>
    /// **NOTE: the value can't be changed once set**
    pub worker_thread_count: u8,
    /// page arena size
    pub write_buffer_size: usize,
    /// smaller than half of arena size
    pub page_size_threshold: usize,
    /// cache memory size, unit in element count, not element size
    pub cache_capacity: usize,
    /// percent of items will evict at once, default is 10%
    pub cache_evict_pct: usize,
    /// buffer pool element size
    pub buffer_size: u32,
    /// buffer pool element count
    pub buffer_count: u32,
    /// when should we consolidate delta chain
    pub consolidate_threshold: u8,
    /// WAL ring buffer size
    pub wal_buffer_size: usize,
}

impl Options {
    pub fn new<P: AsRef<Path>>(db_root: P) -> Self {
        Self {
            panic_on_recover: false,
            bind_core: false,
            tmp_store: false,
            db_root: db_root.as_ref().to_path_buf(),
            meta_name: "meta.json".into(),
            worker_thread_count: 4,
            page_size_threshold: DEFAULT_PAGE_SIZE,
            write_buffer_size: DEFAULT_WRITE_BUFFER_SIZE,
            cache_capacity: DEFAULT_CACHE_CAPACITY,
            cache_evict_pct: DEFAULT_EVICT_PCT,
            buffer_size: 4 << 20, // 4MB
            buffer_count: 256,    // total 100MB
            consolidate_threshold: 8,
            wal_buffer_size: 2 << 20,
        }
    }
}

impl Options {
    pub const MAP_PREFIX: &'static str = "db.map_";
    pub const PAGE_PREFIX: &'static str = "db.page_";

    pub fn map_file(&self, id: u32) -> PathBuf {
        self.db_root.join(format!("{}{}", Self::MAP_PREFIX, id))
    }

    pub fn page_file(&self, id: u32) -> PathBuf {
        self.db_root.join(format!("{}{}", Self::PAGE_PREFIX, id))
    }

    pub fn wal_file(&self) -> PathBuf {
        self.db_root.join("db.wal")
    }

    #[allow(dead_code)]
    fn validate(&self) -> Result<(), OpCode> {
        if self.write_buffer_size < self.page_size_threshold * 2 {
            return Err(OpCode::Invalid);
        }
        if self.page_size_threshold == 0 {
            return Err(OpCode::Invalid);
        }
        Ok(())
    }
}

impl Drop for Options {
    fn drop(&mut self) {
        if self.tmp_store {
            let _ = std::fs::remove_dir_all(&self.db_root);
        }
    }
}
