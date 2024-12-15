mod cc;
pub(crate) mod context;
pub(crate) mod data;
pub(crate) mod log;
pub(crate) mod wal;
pub(crate) mod worker;
/// NOTE: must larger than oldest_txid (which is 0 by default)
pub const INIT_ORACLE: u64 = 1;
