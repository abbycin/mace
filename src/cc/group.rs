use crate::cc::log::Logging;
use crate::must_exist;
use crate::must_true;
use crate::utils::CachePad;
use crate::utils::INIT_CMD;
use crate::utils::NULL_ORACLE;
use crate::utils::data::Position;
use dashmap::DashMap;
use parking_lot::Mutex;
use rustc_hash::FxBuildHasher;
use std::sync::Arc;

use std::sync::atomic::{
    AtomicU64, AtomicUsize,
    Ordering::{AcqRel, Acquire, Relaxed, Release, SeqCst},
};

const FACT_SHARDS: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnFact {
    Active(Position),
    Committed(u64),
    Aborted,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ActiveBounds {
    min_lsn: Option<Position>,
    min_file_id: u64,
}

pub struct WriterGroup {
    /// group id used to route transactions to a fixed writer group
    pub id: usize,
    /// shared logger in durable mode, group-local logger otherwise
    pub logging: Arc<Mutex<Logging>>,
    /// published txn facts in this group, keyed by start_ts
    pub facts: DashMap<u64, TxnFact, FxBuildHasher>,
    /// exact set of retained aborts that can still appear in reachable tree state
    /// `DashMap` is used here so the shard count stays configurable
    pub retained_aborts: DashMap<u64, (), FxBuildHasher>,
    /// minimum txid in `retained_aborts`, or `u64::MAX` when the set is empty
    pub retained_abort_floor: CachePad<AtomicU64>,
    /// retained-abort publication sequence, providing a stable observation window for readers
    abort_seq: CachePad<AtomicU64>,
    /// checkpoint active count for this group, used for maintenance and reclamation decisions
    pub ckpt_cnt: Arc<AtomicUsize>,
    /// parity sequence for begin registration, used by the collector to detect a stable registration window
    txn_seq: CachePad<AtomicU64>,
    /// start_ts currently being registered, exposed together with txn_seq as the registration window
    stable_ts: CachePad<AtomicU64>,
    /// current in-flight writer count, used for load-aware group selection
    inflight: AtomicUsize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RegistrationTs {
    None,
    Published(u64),
    Pending,
}

impl WriterGroup {
    pub fn new(id: usize, logging: Arc<Mutex<Logging>>, ckpt_cnt: Arc<AtomicUsize>) -> Self {
        Self {
            id,
            logging,
            facts: DashMap::with_hasher_and_shard_amount(FxBuildHasher, FACT_SHARDS),
            retained_aborts: DashMap::with_hasher_and_shard_amount(FxBuildHasher, FACT_SHARDS),
            retained_abort_floor: CachePad::from(AtomicU64::new(u64::MAX)),
            abort_seq: CachePad::default(),
            ckpt_cnt,
            txn_seq: CachePad::default(),
            stable_ts: CachePad::from(AtomicU64::new(NULL_ORACLE)),
            inflight: AtomicUsize::new(0),
        }
    }

    #[inline]
    pub fn read_fact(&self, txid: u64) -> Option<TxnFact> {
        self.facts.get(&txid).map(|x| *x)
    }

    // safety: guard by logging mutex
    pub fn start_reg(&self) {
        self.stable_ts.store(NULL_ORACLE, Relaxed);
        let seq = self.txn_seq.load(Relaxed);
        must_true!(seq.is_multiple_of(2));
        self.txn_seq.store(seq + 1, SeqCst); // must use SeqCst
    }

    // safety: guard by logging mutex
    pub fn reg_start_ts(&self, start_ts: u64) {
        self.stable_ts.store(start_ts, Release);
    }

    // safety: guard by logging mutex
    pub fn reg_end(&self) {
        let seq = self.txn_seq.load(Relaxed);
        must_true!(!seq.is_multiple_of(2));
        self.txn_seq.store(seq + 1, SeqCst); // must use SeqCst
    }

    // safety: guard by logging mutex
    pub fn reg_abot(&self) {
        self.stable_ts.store(NULL_ORACLE, Relaxed);
        let seq = self.txn_seq.load(Relaxed);
        if !seq.is_multiple_of(2) {
            self.txn_seq.store(seq + 1, SeqCst); // must use SeqCst
        }
    }

    pub fn stable_ts(&self) -> RegistrationTs {
        let seq = self.txn_seq.load(SeqCst); // must use SeqCst
        if seq.is_multiple_of(2) {
            return RegistrationTs::None;
        }
        let start_ts = self.stable_ts.load(Acquire);
        if start_ts == NULL_ORACLE {
            RegistrationTs::Pending
        } else {
            RegistrationTs::Published(start_ts)
        }
    }

    fn current_active_bounds(&self) -> ActiveBounds {
        let mut bounds = ActiveBounds {
            min_lsn: None,
            min_file_id: u64::MAX,
        };
        for fact in self.facts.iter() {
            if let TxnFact::Active(pos) = *fact.value() {
                bounds.min_lsn = Some(bounds.min_lsn.map_or(pos, |cur| cur.min(pos)));
                bounds.min_file_id = bounds.min_file_id.min(pos.file_id);
            }
        }
        bounds
    }

    // caller must hold logging so a begin registration cannot leave a fresh wal pin outside `facts`
    pub(crate) fn min_active_wal_file_id(&self, _log: &mut Logging) -> u64 {
        self.current_active_bounds().min_file_id
    }

    // caller must hold logging so a begin registration cannot leave a fresh wal pin outside `facts`
    pub(crate) fn min_active_lsn(&self, _log: &mut Logging) -> Option<Position> {
        self.current_active_bounds().min_lsn
    }

    #[inline]
    pub fn active_fact(&self, start_ts: u64, lsn: Position) {
        self.facts.insert(start_ts, TxnFact::Active(lsn));
    }

    pub fn commit_fact<C>(&self, start_ts: u64, c: C)
    where
        C: Fn() -> u64,
    {
        let mut fact = must_exist!(
            self.facts.get_mut(&start_ts),
            "active fact must exist before commit publication"
        );
        must_true!(matches!(*fact, TxnFact::Active(_)));
        #[cfg(feature = "extra_check")]
        crate::testing::fire_txn_commit_sync_point(
            crate::testing::TxnCommitSyncPoint::AfterFactWriteGuardBeforeCommitTimestamp,
            start_ts,
        );
        let commit_ts = c();
        #[cfg(feature = "extra_check")]
        crate::testing::fire_txn_commit_sync_point(
            crate::testing::TxnCommitSyncPoint::AfterCommitTimestampBeforeOutcomePublish,
            start_ts,
        );
        *fact = TxnFact::Committed(commit_ts);
    }

    pub fn remove_fact(&self, start_ts: u64) {
        self.facts.remove(&start_ts);
    }

    pub fn abort_fact(&self, start_ts: u64) {
        let _guard = AbortSeqGuard::enter(self);
        self.retained_aborts.insert(start_ts, ());
        self.retained_abort_floor.fetch_min(start_ts, Release);
        #[cfg(feature = "extra_check")]
        crate::testing::fire_txn_abort_sync_point(
            crate::testing::TxnAbortSyncPoint::AfterAbortFloorBeforeAbortedFactPublish,
            start_ts,
        );
        let mut fact = self
            .facts
            .get_mut(&start_ts)
            .expect("active fact must exist before abort publication");
        must_true!(matches!(*fact, TxnFact::Active(_)));
        *fact = TxnFact::Aborted;
    }

    #[inline]
    pub fn abort_seq(&self) -> u64 {
        self.abort_seq.load(Acquire)
    }

    #[inline]
    pub fn is_retained_abort(&self, txid: u64) -> bool {
        self.retained_aborts.contains_key(&txid)
    }

    fn recompute_retained_abort_floor(&self) {
        let mut floor = u64::MAX;
        for retained in self.retained_aborts.iter() {
            floor = floor.min(*retained.key());
        }
        self.retained_abort_floor.store(floor, Release);
    }

    pub fn recover_retained_abort(&self, start_ts: u64) {
        self.retained_aborts.insert(start_ts, ());
        self.retained_abort_floor.fetch_min(start_ts, Release);
        self.facts.insert(start_ts, TxnFact::Aborted);
    }

    pub fn retire_aborted_fact(&self, txid: u64) {
        let _log = self.logging.lock();
        let _guard = AbortSeqGuard::enter(self);
        self.retained_aborts.remove(&txid);
        let Some(fact) = self.facts.get(&txid) else {
            self.recompute_retained_abort_floor();
            return;
        };
        if !matches!(*fact, TxnFact::Aborted) {
            self.recompute_retained_abort_floor();
            return;
        }
        drop(fact);
        self.facts.remove(&txid);
        self.recompute_retained_abort_floor();
    }

    #[inline]
    pub fn enter_inflight(&self) {
        self.inflight.fetch_add(1, Relaxed);
    }

    #[inline]
    pub fn leave_inflight(&self) {
        let prev = self.inflight.fetch_sub(1, Release);
        must_true!(prev > 0);
    }

    #[inline]
    pub fn inflight(&self) -> usize {
        self.inflight.load(Relaxed)
    }

    /// acquire load for the shutdown barrier
    #[inline]
    pub fn inflight_acquire(&self) -> usize {
        self.inflight.load(Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RandomPath;
    use crate::utils::options::Options;
    use std::sync::Arc;

    fn new_group() -> WriterGroup {
        let root = RandomPath::tmp();
        let mut opt = Options::new(&*root);
        opt.concurrent_write = 1;
        let opt = Arc::new(opt.validate().expect("group options must validate"));
        let ckpt_cnt = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let logging = Arc::new(Mutex::new(Logging::new(
            0,
            0,
            0,
            Position::MIN,
            opt,
            vec![ckpt_cnt.clone()],
        )));
        WriterGroup::new(0, logging, ckpt_cnt)
    }

    #[test]
    fn active_bounds_follow_live_facts_without_collector_publication() {
        let group = new_group();
        group.active_fact(10, Position::new(1, 9));
        group.active_fact(20, Position::new(3, 7));
        group.commit_fact(10, || 30);

        let mut log = group.logging.lock();
        assert_eq!(group.min_active_wal_file_id(&mut log), 3);
        let bounds = group.current_active_bounds();
        assert_eq!(bounds.min_lsn, Some(Position::new(3, 7)));
    }

    #[test]
    fn live_bounds_clear_after_last_active_commits() {
        let group = new_group();
        group.active_fact(10, Position::new(1, 9));
        group.commit_fact(10, || 20);

        let mut log = group.logging.lock();
        assert_eq!(group.min_active_lsn(&mut log), None);
        assert_eq!(group.min_active_wal_file_id(&mut log), u64::MAX);
    }

    #[test]
    fn begin_registration_blocks_collection_until_start_ts_is_published() {
        let group = new_group();

        group.start_reg();
        assert_eq!(group.stable_ts(), RegistrationTs::Pending);

        group.reg_start_ts(10);
        assert_eq!(group.stable_ts(), RegistrationTs::Published(10));

        group.reg_end();
        assert_eq!(group.stable_ts(), RegistrationTs::None);
    }
}

struct AbortSeqGuard<'a> {
    group: &'a WriterGroup,
    even_seq: u64,
}

impl<'a> AbortSeqGuard<'a> {
    fn enter(group: &'a WriterGroup) -> Self {
        let seq = group.abort_seq.fetch_add(1, AcqRel);
        must_true!(seq.is_multiple_of(2));
        Self {
            group,
            even_seq: seq + 2,
        }
    }
}

impl Drop for AbortSeqGuard<'_> {
    fn drop(&mut self) {
        self.group.abort_seq.store(self.even_seq, Release);
    }
}

#[derive(Debug)]
pub struct TxnState {
    pub start_ts: u64,
    pub modified: bool,
    pub group_id: u8,
    pub cmd_id: u32,
    pub begin_lsn: Position,
    pub prev_lsn: Position,
    pub start_ckpt: usize,
}

impl TxnState {
    pub fn new(group_id: u8, start_ts: u64, start_ckpt: usize) -> Self {
        Self {
            start_ts,
            modified: false,
            begin_lsn: Position::MIN,
            prev_lsn: Position::MIN,
            group_id,
            cmd_id: INIT_CMD,
            start_ckpt,
        }
    }

    #[inline]
    pub fn group(&self) -> usize {
        self.group_id as usize
    }
}
