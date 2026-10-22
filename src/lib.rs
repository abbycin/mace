pub use index::txn::{TxnKV, TxnView};
pub use store::VacuumStats;
pub use store::checkpoint::{BucketFrontier, CheckpointInfo, GroupPosition};
pub(crate) use store::store::Store;
pub use store::store::{Bucket, Mace};
#[cfg(feature = "metrics")]
pub use utils::observe;
pub use utils::{
    OpCode, RandomPath,
    merge_operator::{MergeOperator, U64AddOperator, u64_add_operator},
    options::{BucketOptions, Options, PersistedBucketOptions},
};

mod cc;
mod error;
#[cfg(any(feature = "extra_check", feature = "failpoints"))]
pub mod failpoint_testing;
mod index;
mod io;
mod map;
mod meta;
mod store;
#[cfg(feature = "extra_check")]
pub mod testing;
mod utils;

mod types;
pub use index::{Iter, ValRef};

/// internal on-disk inspection surface for the pinned maintenance CLI
///
/// nothing in here is part of the engine's stable API. it exists so the
/// offline checker can read the same format definitions the engine writes;
/// the CLI pins this crate's exact version, so a change is a compile error
/// there rather than a silent format drift.
#[cfg(feature = "maintenance")]
#[doc(hidden)]
pub mod maintenance;
