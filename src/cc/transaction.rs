use crate::utils::{IsolationLevel, TxnMode, TxnState};

#[repr(C)]
pub struct Transaction {
    pub tx_id: u64,
    pub start_ts: u64,
    pub commit_ts: u64,
    pub modified: bool,
    pub level: IsolationLevel,
    pub state: TxnState,
    pub mode: TxnMode,
    _padding: [u8; 4],
}

const _: () = assert!(size_of::<Transaction>() == 32);
