pub(crate) mod builder;
mod entry;
mod manifest;
mod stat;
mod txn;

use crate::map::buffer::BucketContext;
use crate::utils::interval::IntervalMap;
use crate::utils::options::ParsedOptions;
pub use entry::{
    BucketDurableFrontier, BucketMeta, DelInterval, Delete, FileKind, IntervalPair, MemStat,
    MetaKind, MetaOp, PageTable, PersistStat, Sequences, StatInner, WalRecycleState,
};
pub(crate) use manifest::Manifest;
use parking_lot::RwLock;
pub(crate) use stat::{FileReader, new_reader};
use std::path::PathBuf;
pub(crate) use txn::Txn;

pub(crate) const BUCKET_MISC: &str = "misc";
pub(crate) const SEQUENCES_KEY: &str = "sequences";
pub(crate) const BUCKET_DATA_STAT: &str = "data_stat";
pub(crate) const BUCKET_BLOB_STAT: &str = "blob_stat";
pub(crate) const BUCKET_OBSOLETE_DATA: &str = "obsolete_data";
pub(crate) const BUCKET_OBSOLETE_BLOB: &str = "obsolete_blob";
pub(crate) const BUCKET_METAS: &str = "bucket_metas";
pub(crate) const BUCKET_PENDING_DEL: &str = "pending_del";
pub(crate) const BUCKET_FRONTIER: &str = "bucket_frontier";
pub(crate) const BUCKET_VERSION: &str = "version";
pub(crate) const MAX_BUCKETS: u64 = 1024;
pub(crate) const VERSION_KEY: &str = "current_version";
pub(crate) const OPTIONS_KEY: &str = "options";
pub(crate) const ORPHAN_DATA_MARKER_PREFIX: &str = "odf_";
pub(crate) const ORPHAN_BLOB_MARKER_PREFIX: &str = "obf_";
pub(crate) const WAL_RECYCLE_PREFIX: &str = "wrc_";
/// storage format version
pub(crate) const CURRENT_VERSION: u64 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WalRecycleIntent {
    pub(crate) group_id: u8,
    pub(crate) from_file_id: u64,
    pub(crate) to_file_id: u64,
}

pub(crate) fn page_table_name(bucket_id: u64) -> String {
    format!("page_table_{}", bucket_id)
}

pub(crate) fn data_interval_name(bucket_id: u64) -> String {
    format!("data_interval_{}", bucket_id)
}

pub(crate) fn blob_interval_name(bucket_id: u64) -> String {
    format!("blob_interval_{}", bucket_id)
}

pub(crate) fn orphan_data_marker_key(file_id: u64) -> Vec<u8> {
    format!("{}{}", ORPHAN_DATA_MARKER_PREFIX, file_id).into_bytes()
}

pub(crate) fn orphan_blob_marker_key(file_id: u64) -> Vec<u8> {
    format!("{}{}", ORPHAN_BLOB_MARKER_PREFIX, file_id).into_bytes()
}

pub(crate) fn wal_recycle_key(group_id: u8) -> Vec<u8> {
    format!("{}{}", WAL_RECYCLE_PREFIX, group_id).into_bytes()
}

pub(crate) fn stat_bucket(kind: FileKind) -> &'static str {
    match kind {
        FileKind::Data => BUCKET_DATA_STAT,
        FileKind::Blob => BUCKET_BLOB_STAT,
    }
}

pub(crate) fn delete_meta_kind(kind: FileKind) -> MetaKind {
    match kind {
        FileKind::Data => MetaKind::DataDelete,
        FileKind::Blob => MetaKind::BlobDelete,
    }
}

pub(crate) fn delete_done_meta_kind(kind: FileKind) -> MetaKind {
    match kind {
        FileKind::Data => MetaKind::DataDeleteDone,
        FileKind::Blob => MetaKind::BlobDeleteDone,
    }
}

pub(crate) fn stat_file_path(opt: &ParsedOptions, kind: FileKind, file_id: u64) -> PathBuf {
    match kind {
        FileKind::Data => opt.data_file(file_id),
        FileKind::Blob => opt.blob_file(file_id),
    }
}

pub(crate) fn interval_bucket_name(kind: FileKind, bucket_id: u64) -> String {
    match kind {
        FileKind::Data => data_interval_name(bucket_id),
        FileKind::Blob => blob_interval_name(bucket_id),
    }
}

pub(crate) fn stat_intervals(kind: FileKind, ctx: &BucketContext) -> &RwLock<IntervalMap> {
    match kind {
        FileKind::Data => &ctx.data_intervals,
        FileKind::Blob => &ctx.blob_intervals,
    }
}

pub(crate) fn stat_handle_cache_capacity(opt: &ParsedOptions, kind: FileKind) -> usize {
    match kind {
        FileKind::Data => opt.data_handle_cache_capacity,
        FileKind::Blob => opt.blob_handle_cache_capacity,
    }
}
