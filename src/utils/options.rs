use std::path::{Path, PathBuf};

use super::OpCode;

const FIXED_TMP_DIR: &str = "mace_tmp";
const DEFAULT_WRITE_BUFFER_SIZE: usize = 1 << 20;
const DEFAULT_PAGE_SIZE: usize = 16 << 10;
const DEFAULT_CACHE_CAPACITY: usize = 256;
const DEFAULT_EVICT_PCT: usize = 10;

/// mace database configuration options
pub struct Options {
    /// should we create a new instance or panic when recover a database was failed
    pub panic_on_recover: bool,
    /// where to store database files
    pub db_path: String,
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
}

impl Options {
    pub const MAP_PREFIX: &'static str = "db.map_";
    pub const PAGE_PREFIX: &'static str = "db.page_";

    pub fn map_file(&self, id: u32) -> PathBuf {
        Path::new(&self.db_path).join(format!("{}{}", Self::MAP_PREFIX, id))
    }

    pub fn page_file(&self, id: u32) -> PathBuf {
        Path::new(&self.db_path).join(format!("{}{}", Self::PAGE_PREFIX, id))
    }

    fn validate(&self) -> Result<(), OpCode> {
        if self.write_buffer_size < self.page_size_threshold * 2 {
            return Err(OpCode::Invalid);
        }
        if self.page_size_threshold == 0 {
            return Err(OpCode::Invalid);
        }
        return Ok(());
    }
}

impl Default for Options {
    fn default() -> Self {
        let tmp = std::env::temp_dir().join(FIXED_TMP_DIR);
        Self {
            panic_on_recover: false,
            db_path: tmp.to_string_lossy().to_string(),
            meta_name: "meta.json".into(),
            worker_thread_count: 4,
            page_size_threshold: DEFAULT_PAGE_SIZE,
            write_buffer_size: DEFAULT_WRITE_BUFFER_SIZE,
            cache_capacity: DEFAULT_CACHE_CAPACITY,
            cache_evict_pct: DEFAULT_EVICT_PCT,
            buffer_size: 4 << 20, // 4MB
            buffer_count: 256,    // total 100MB
            consolidate_threshold: 8,
        }
    }
}

#[test]
fn test_conf_default() {
    let c: Options = Default::default();

    assert_eq!(c.db_path, "/tmp/".to_string() + FIXED_TMP_DIR);
    assert_eq!(c.worker_thread_count, 4);
}
