use std::{
    ops::Deref,
    path::{Path, PathBuf},
};

use crate::{types::imtree::NODE_SIZE, utils::lru::LRU_SHARD};

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
    /// size can be compacted into [`Self::gc_compacted_size`], the default setting is [`Self::arena_size`]
    pub gc_compacted_size: usize,
    /// is temperary storage, if true, db_root will be unlinked on exit
    pub tmp_store: bool,
    /// where to store database files
    pub db_root: PathBuf,
    /// number of file meta will be cached, it will be sharded into 32 slots
    pub file_cache: usize,
    /// node cache memory size
    pub cache_capacity: usize,
    /// percent of items will be evicted at once, default is 10%
    pub cache_evict_pct: usize,
    /// delta cache count, shard into 32 slots, which act as a secondary cache to node cache
    pub cache_count: usize,
    /// a size limit to trigger data file flush, which also control max key-value size, NOTE: too
    /// large a file size will cause the flushing to be slow
    pub data_file_size: u32,
    /// when should we consolidate delta chain, must less than [`NODE_SIZE`] which is 64
    pub consolidate_threshold: u32,
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
    /// max key-value size support, it must less than 4GB (because we have to add extra headers or
    /// something else), the default value is **half** of [`Self::arena_size`]
    pub max_kv_size: u32,
    /// max size that key-val can be save into sst (B+ Tree Node) rather than save as a pointer to
    /// remote address the default value is 2048, the same must less than [`Self::max_kv_size`]
    pub max_inline_size: u32,
    /// control max elements in sst (B+ Tree Node), the default value is 512, the sst size which is
    /// approximate [`Self::max_inline_size`] * [`Self::split_elem`] must less than [`Self::max_kv_size`]
    pub split_elem: u16,
}

impl Options {
    pub const DATA_FILE_SIZE: usize = 64 << 20; // 64MB
    pub const MAX_WORKERS: usize = 128;
    pub const CONSOLIDATE_THRESHOLD: u32 = NODE_SIZE as u32;
    pub const MIN_CACHE_CAP: usize = Self::DATA_FILE_SIZE;
    pub const CACHE_CAP: usize = 1 << 30; // 1GB
    pub const CACHE_CNT: usize = 16384;
    pub const FILE_CACHE: usize = 512;
    pub const WAL_BUF_SZ: usize = 8 << 20; // 8MB
    pub const WAL_FILE_SZ: usize = 24 << 20; // 24MB
    pub const INLINE_SIZE: u32 = 2048;
    pub const SPLIT_ELEM: u16 = 512;

    pub fn new<P: AsRef<Path>>(db_root: P) -> Self {
        Self {
            sync_on_write: true,
            workers: std::thread::available_parallelism()
                .unwrap()
                .get()
                .min(Self::MAX_WORKERS),
            tmp_store: false,
            gc_timeout: 50, // 50ms
            gc_ratio: 20,   // 20%
            gc_eager: true,
            gc_compacted_size: Self::DATA_FILE_SIZE,
            db_root: db_root.as_ref().to_path_buf(),
            file_cache: Self::FILE_CACHE, // each shard has 16 entries
            cache_capacity: Self::CACHE_CAP,
            cache_evict_pct: 10, // 10% evict 100MB every time
            cache_count: Self::CACHE_CNT,
            data_file_size: Self::DATA_FILE_SIZE as u32,
            consolidate_threshold: Self::CONSOLIDATE_THRESHOLD,
            wal_buffer_size: Self::WAL_BUF_SZ,
            max_ckpt_per_txn: 1_000_000, // 1 million
            wal_file_size: Self::WAL_FILE_SZ as u32,
            max_kv_size: Self::DATA_FILE_SIZE as u32 / 2,
            keep_stable_wal_file: false,
            max_inline_size: Self::INLINE_SIZE,
            split_elem: Self::SPLIT_ELEM,
        }
    }

    /// the length of the kv plus the length of the metadata must be less than this value
    pub(crate) fn max_data_size(&self) -> usize {
        self.max_kv_size as usize
    }

    pub fn validate(mut self) -> Result<ParsedOptions, OpCode> {
        if self.consolidate_threshold > Self::CONSOLIDATE_THRESHOLD {
            self.consolidate_threshold = Self::CONSOLIDATE_THRESHOLD;
        }
        if self.file_cache < LRU_SHARD {
            self.file_cache = Self::FILE_CACHE;
        }
        if self.cache_count < LRU_SHARD {
            self.cache_count = Self::CACHE_CNT;
        }
        if self.cache_capacity < Self::MIN_CACHE_CAP {
            self.cache_capacity = Self::CACHE_CAP;
        }
        self.max_kv_size = self.data_file_size / 2;
        if self.max_inline_size > self.max_kv_size {
            self.max_inline_size = Self::INLINE_SIZE;
        }
        if self.max_inline_size as usize * self.split_elem as usize > self.max_kv_size as usize {
            self.max_inline_size = Self::INLINE_SIZE;
            self.split_elem = Self::SPLIT_ELEM;
        }
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
            self.wal_root().join(format!("meta_{wid}"))
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
