use crate::cc::cc::ConcurrencyControl;
use crate::cc::log::Logging;
use crate::meta::Numerics;
use crate::utils::data::Position;
use crate::utils::options::ParsedOptions;
use parking_lot::{Mutex, RwLock};
use std::collections::BTreeMap;
use std::sync::Arc;

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};

pub struct ActiveTxns {
    // bottleneck
    map: RwLock<BTreeMap<u64, Position>>,
    min_txid: AtomicU64,
    min_file_id: AtomicU64,
}

impl ActiveTxns {
    pub fn new() -> Self {
        Self {
            map: RwLock::new(BTreeMap::new()),
            min_txid: AtomicU64::new(u64::MAX),
            min_file_id: AtomicU64::new(u64::MAX),
        }
    }

    pub fn insert(&self, txid: u64, pos: Position) {
        let old_pos = {
            let mut map = self.map.write();
            map.insert(txid, pos)
        };
        self.min_txid.fetch_min(txid, Relaxed);
        if let Some(old_pos) = old_pos
            && old_pos.file_id == self.min_file_id.load(Relaxed)
            && old_pos.file_id != pos.file_id
        {
            self.min_file_id.store(u64::MAX, Relaxed);
        }
        self.min_file_id.fetch_min(pos.file_id, Relaxed);
    }

    pub fn remove(&self, txid: &u64) {
        let pos = {
            let mut map = self.map.write();
            map.remove(txid)
        };
        if let Some(pos) = pos {
            if *txid == self.min_txid.load(Relaxed) {
                self.min_txid.store(u64::MAX, Relaxed);
            }
            if pos.file_id == self.min_file_id.load(Relaxed) {
                self.min_file_id.store(u64::MAX, Relaxed);
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        let map = self.map.read();
        map.is_empty()
    }

    pub fn min_txid(&self) -> Option<u64> {
        let mut min = self.min_txid.load(Relaxed);
        if min == u64::MAX {
            let map = self.map.read();
            if let Some(&k) = map.keys().next()
                && k < min
            {
                min = k;
            }
            if min != u64::MAX {
                self.min_txid.store(min, Relaxed);
            } else {
                return None;
            }
        }
        Some(min)
    }

    pub fn min_position_file_id(&self) -> u64 {
        let mut min = self.min_file_id.load(Relaxed);
        if min == u64::MAX {
            let map = self.map.read();
            for pos in map.values() {
                if pos.file_id < min {
                    min = pos.file_id;
                }
            }

            if min != u64::MAX {
                self.min_file_id.store(min, Relaxed);
            }
        }
        min
    }

    pub fn min_lsn(&self) -> Option<Position> {
        let map = self.map.read();
        map.values().min().cloned()
    }

    pub fn for_each_txid<F>(&self, mut f: F)
    where
        F: FnMut(u64),
    {
        let map = self.map.read();
        for &txid in map.keys() {
            f(txid);
        }
    }
}

pub struct WriterGroup {
    pub id: usize,
    pub cc: ConcurrencyControl,
    pub logging: Mutex<Logging>,
    pub active_txns: ActiveTxns,
    pub ckpt_cnt: Arc<AtomicUsize>,
    inflight: AtomicUsize,
}

impl WriterGroup {
    pub fn new(
        id: usize,
        checkpoint: Position,
        latest_id: u64,
        oldest_id: u64,
        numerics: Arc<Numerics>,
        opt: Arc<ParsedOptions>,
    ) -> Self {
        let ckpt_cnt = Arc::new(AtomicUsize::new(0));
        Self {
            id,
            cc: ConcurrencyControl::new(opt.concurrent_write as usize),
            logging: Mutex::new(Logging::new(
                id as u8,
                latest_id,
                oldest_id,
                checkpoint,
                numerics,
                opt,
                ckpt_cnt.clone(),
            )),
            active_txns: ActiveTxns::new(),
            ckpt_cnt,
            inflight: AtomicUsize::new(0),
        }
    }

    #[inline]
    pub fn enter_inflight(&self) {
        self.inflight.fetch_add(1, Relaxed);
    }

    #[inline]
    pub fn try_enter_inflight_if_free(&self) -> bool {
        self.inflight
            .compare_exchange(0, 1, Relaxed, Relaxed)
            .is_ok()
    }

    #[inline]
    pub fn leave_inflight(&self) {
        let prev = self.inflight.fetch_sub(1, Relaxed);
        debug_assert!(prev > 0);
    }

    #[inline]
    pub fn inflight(&self) -> usize {
        self.inflight.load(Relaxed)
    }
}

#[derive(Debug)]
pub struct TxnState {
    pub start_ts: u64,
    pub modified: bool,
    pub prev_lsn: Position,
    pub group_id: usize,
    pub cmd_id: u32,
    pub start_ckpt: usize,
}

impl TxnState {
    pub fn new(group_id: usize, start_ts: u64, start_ckpt: usize) -> Self {
        Self {
            start_ts,
            modified: false,
            prev_lsn: Position::default(),
            group_id,
            cmd_id: 0,
            start_ckpt,
        }
    }
}
