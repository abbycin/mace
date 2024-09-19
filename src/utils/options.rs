const DEFAULT_PAGE_SIZE: u32 = 4096;

/// zion database configuration options
pub struct Options {
    /// where to store database files
    pub db_path: String,
    /// how many worker thread will be created
    /// **NOTE: the value can't be changed once set**
    pub worker_thread_count: u8,
    /// B+ Tree node size
    pub page_size: u32,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            db_path: std::env::temp_dir().to_string_lossy().to_string(),
            worker_thread_count: 4,
            page_size: DEFAULT_PAGE_SIZE,
        }
    }
}

#[test]
fn test_conf_default() {
    let c: Options = Default::default();

    assert_ne!(c.db_path, "/tmp");
    assert_eq!(c.worker_thread_count, 4);
    assert_eq!(c.page_size, DEFAULT_PAGE_SIZE);
}
