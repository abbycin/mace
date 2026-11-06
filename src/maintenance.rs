//! internal on-disk inspection surface for the compatible maintenance CLI
//!
//! these are the format definitions the engine writes with. they are grouped
//! here, behind a non-default feature, so the engine's stable API does not
//! carry the checker's vocabulary. the CLI depends on a semver-compatible
//! library release; changes to this surface or the on-disk format require
//! coordinated package updates.

pub use crate::cc::wal::{
    EntryType, IWalCodec, PayloadType, WalAbort, WalCheckpoint, WalUpdate, wal_abort_at,
    wal_checkpoint_at, wal_record_sz, wal_update_at,
};
pub use crate::io::{File, FileSystem, GatherIO, OpenOptions};
pub use crate::map::data::{FileReader, MetaReader, try_new_reader};
pub use crate::meta::{
    BUCKET_BLOB_STAT, BUCKET_DATA_STAT, BUCKET_FRONTIER, BUCKET_METAS, BUCKET_MISC,
    BUCKET_OBSOLETE_BLOB, BUCKET_OBSOLETE_DATA, BUCKET_PENDING_DEL, BUCKET_VERSION,
    BucketDurableFrontier, BucketMeta, CURRENT_VERSION, FileKind, IMetaCodec, IntervalPair,
    OPTIONS_KEY, ORPHAN_BLOB_MARKER_PREFIX, ORPHAN_DATA_MARKER_PREFIX, PersistStat, StatInner,
    VERSION_KEY, WAL_RECYCLE_PREFIX, WalRecycleState, data_interval_name, interval_bucket_name,
    orphan_data_marker_key, page_table_name, wal_recycle_key,
};
pub use crate::types::header::{BaseHeader, DeltaHeader, NodeType, TagFlag, TagKind};
pub use crate::types::refbox::{BaseView, BoxRef, DeltaView};
pub use crate::types::traits::{IAsSlice, IHeader};
pub use crate::utils::data::{AddrPair, Interval, Position};
pub use crate::utils::options::{Options, PersistedOptions};
pub use crate::utils::{
    NULL_ADDR, NULL_PID, OpCode, bitmap::BitMap, compress::DecompressorPool, varint::Varint32,
};

pub const ROOT_PID: u64 = crate::utils::ROOT_PID;
