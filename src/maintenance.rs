//! internal on-disk inspection surface for the pinned maintenance CLI
//!
//! these are the format definitions the engine writes with. they are grouped
//! here, behind a non-default feature, so the engine's stable API does not
//! carry the checker's vocabulary. the CLI depends on this module with an
//! exact version pin; changing one of these items is a compile error there.

pub use crate::cc::wal::{
    EntryType, IWalCodec, PayloadType, WalAbort, WalCheckpoint, WalUpdate, ptr_to, wal_record_sz,
};
pub use crate::io::{File, FileSystem, GatherIO, OpenOptions};
pub use crate::map::data::MetaReader;
pub use crate::meta::{
    BUCKET_BLOB_STAT, BUCKET_DATA_STAT, BUCKET_FRONTIER, BUCKET_METAS, BUCKET_MISC,
    BUCKET_OBSOLETE_BLOB, BUCKET_OBSOLETE_DATA, BUCKET_PENDING_DEL, BUCKET_VERSION,
    BucketDurableFrontier, BucketMeta, CURRENT_VERSION, FileKind, FileReader, IMetaCodec,
    IntervalPair, OPTIONS_KEY, ORPHAN_BLOB_MARKER_PREFIX, ORPHAN_DATA_MARKER_PREFIX, PersistStat,
    StatInner, VERSION_KEY, WAL_RECYCLE_PREFIX, WalRecycleState, data_interval_name,
    interval_bucket_name, orphan_data_marker_key, page_table_name, try_new_reader, wal_recycle_key,
};
pub use crate::types::header::{BaseHeader, DeltaHeader, NodeType, TagFlag, TagKind};
pub use crate::types::refbox::{BaseView, BoxRef, DeltaView};
pub use crate::types::traits::{IAsSlice, IHeader};
pub use crate::utils::data::{AddrPair, Interval};
pub use crate::utils::options::{Options, PersistedOptions};
pub use crate::utils::{
    NULL_ADDR, NULL_PID, OpCode, bitmap::BitMap, compress::DecompressorPool, varint::Varint32,
};
