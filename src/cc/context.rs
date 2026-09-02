use crate::cc::log::{Logging, SyncTicket};
use crate::meta::Sequences;
use crate::{must_ok, must_true};
use crossbeam_epoch::Guard;
use parking_lot::{Mutex, MutexGuard, RwLock};
use rustc_hash::FxHashMap;
use std::collections::BTreeMap;

use crate::OpCode;
use crate::utils::data::Position;
use crate::utils::observe::{
    HistogramMetric, LATENCY_SAMPLE_SHIFT, observe_elapsed, sampled_instant,
};
use crate::utils::options::{Options, ParsedOptions};
use crate::utils::seqlock::SeqLock;
use crate::utils::{CachePad, Handle, NULL_ORACLE};

use super::group::{RegistrationTs, TxnFact, WriterGroup};
use std::sync::Arc;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release, SeqCst};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, TryRecvError, channel};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug)]
pub(crate) struct GroupBoot {
    /// physical WAL stream id
    pub physical_wal_id: u8,
    pub oldest_id: u64,
    pub latest_id: u64,
    /// conservative recovery scan start
    pub scan_start: Position,
    pub has_files: bool,
    /// first file id of the active epoch
    pub start_id: u64,
    /// manifest checkpoint floor for this logical group
    pub checkpoint_floor: Position,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct AbortCleanTask {
    pub txid: u64,
    pub bucket_id: u64,
    /// physical wal stream containing the abort-clean chain
    pub physical_wal_id: u8,
    /// logical group owning the retained-abort fact
    pub logical_group_id: u8,
    pub tail_lsn: Position,
    pub pin_file_id: u64,
    pub state: AbortCleanState,
    pub quiesced: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AbortCleanState {
    Pending,
    WaitingQuiesce,
}

#[cfg_attr(not(feature = "extra_check"), allow(dead_code))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CollectorSignal {
    Wake,
    Quit,
}

#[inline]
fn two_choices(ticket: usize, nr: usize) -> (usize, usize) {
    debug_assert!(nr > 0);
    let home = ticket % nr;
    if nr == 1 {
        return (home, home);
    }

    // advance the offset after each full home round so every home visits every other choice
    let round = ticket / nr;
    let offset = 1 + round % (nr - 1);
    let second = home + offset;
    (home, if second >= nr { second - nr } else { second })
}

pub struct Context {
    pub(crate) opt: Arc<ParsedOptions>,
    pub(crate) sequences: Arc<Sequences>,
    safe_exclusive: Arc<AtomicU64>,
    /// recovery roots use `Position::MIN` until phase2 completes
    recovering: AtomicBool,
    /// shared durable WAL logger
    shared_logging: Option<Arc<Mutex<Logging>>>,
    pool: Arc<CCPool>,
    groups: Arc<Vec<WriterGroup>>,
    group_rr: CachePad<AtomicUsize>,
    pending_abort_clean: Vec<RwLock<BTreeMap<u64, AbortCleanTask>>>,
    pending_abort_clean_buckets: Vec<RwLock<FxHashMap<u64, usize>>>,
    pending_abort_clean_nr: CachePad<AtomicUsize>,
    pending_abort_clean_floor: CachePad<AtomicU64>,
    pending_abort_clean_seqlock: SeqLock,
    abort_clean_events: Arc<Mutex<Vec<u64>>>,
    tx: Sender<CollectorSignal>,
    collector_rx: Mutex<Option<Receiver<CollectorSignal>>>,
    collector: Mutex<Option<JoinHandle<()>>>,
}

impl Context {
    const SHARDS: usize = 64;
    const MASK: usize = Self::SHARDS - 1;

    const fn shard_of(txid: u64) -> usize {
        (txid as usize) & Self::MASK
    }

    pub fn new(
        opt: Arc<ParsedOptions>,
        sequences: Arc<Sequences>,
        group_boot: &[GroupBoot],
    ) -> Self {
        let cores = opt.concurrent_write as usize;
        // per-group boots followed by the shared-stream boot
        let default_boot = |physical_wal_id: u8| GroupBoot {
            oldest_id: 0,
            latest_id: 0,
            scan_start: Position::MIN,
            has_files: false,
            start_id: 0,
            checkpoint_floor: Position::MIN,
            physical_wal_id,
        };
        let per_group_boot = |i: usize| {
            group_boot
                .get(i)
                .copied()
                .unwrap_or_else(|| default_boot(i as u8))
        };

        let ckpt_cnts: Vec<Arc<AtomicUsize>> =
            (0..cores).map(|_| Arc::new(AtomicUsize::new(0))).collect();

        let mut groups_logging: Vec<Arc<Mutex<Logging>>> = Vec::with_capacity(cores);
        let mut shared_logging = None;
        if opt.sync_on_write {
            // durable mode shares one logger across all logical groups
            let shared_boot = group_boot
                .get(cores)
                .copied()
                .unwrap_or_else(|| default_boot(Options::SHARED_ID));
            let shared = Arc::new(Mutex::new(Logging::new(
                Options::SHARED_ID,
                shared_boot.start_id,
                shared_boot.oldest_id,
                Position::MIN,
                opt.clone(),
                ckpt_cnts.clone(),
            )));
            for _ in 0..cores {
                groups_logging.push(shared.clone());
            }
            shared_logging = Some(shared);
        } else {
            // relaxed: every writer group owns its writable per-group stream
            for (i, ckpt_cnt) in ckpt_cnts.iter().enumerate() {
                let boot = per_group_boot(i);
                groups_logging.push(Arc::new(Mutex::new(Logging::new(
                    i as u8,
                    boot.start_id,
                    boot.oldest_id,
                    Position::MIN,
                    opt.clone(),
                    vec![ckpt_cnt.clone()],
                ))));
            }
        }

        let mut groups = Vec::with_capacity(cores);
        for i in 0..cores {
            groups.push(WriterGroup::new(
                i,
                groups_logging[i].clone(),
                ckpt_cnts[i].clone(),
            ));
        }

        let manifest_floors: Vec<Position> = (0..cores)
            .map(|i| per_group_boot(i).checkpoint_floor)
            .collect();
        if opt.sync_on_write {
            if let Some(shared) = &shared_logging {
                shared
                    .lock()
                    .set_logical_checkpoint_floors(&manifest_floors);
            }
        } else {
            for (i, g) in groups.iter().enumerate() {
                let mut floors = vec![Position::MAX; cores];
                floors[i] = manifest_floors[i];
                g.logging.lock().set_logical_checkpoint_floors(&floors);
            }
        }

        let pool = Arc::new(CCPool::new());
        let groups = Arc::new(groups);
        let safe_exclusive = Arc::new(AtomicU64::new(sequences.oracle.load(Acquire)));
        let (tx, rx) = channel();

        Self {
            opt: opt.clone(),
            sequences,
            safe_exclusive,
            recovering: AtomicBool::new(true),
            shared_logging,
            pool,
            groups,
            group_rr: CachePad::default(),
            pending_abort_clean: (0..Self::SHARDS)
                .map(|_| RwLock::new(BTreeMap::new()))
                .collect(),
            pending_abort_clean_buckets: (0..Self::SHARDS)
                .map(|_| RwLock::new(FxHashMap::default()))
                .collect(),
            pending_abort_clean_nr: CachePad::default(),
            pending_abort_clean_floor: CachePad::from(AtomicU64::new(u64::MAX)),
            pending_abort_clean_seqlock: SeqLock::new(),
            abort_clean_events: Arc::new(Mutex::new(Vec::new())),
            tx,
            collector_rx: Mutex::new(Some(rx)),
            collector: Mutex::new(None),
        }
    }

    #[inline]
    fn update_pending_abort_clean_floor(&self, txid: u64) {
        let mut floor = self.pending_abort_clean_floor.load(Relaxed);
        while txid < floor {
            match self
                .pending_abort_clean_floor
                .compare_exchange_weak(floor, txid, Relaxed, Relaxed)
            {
                Ok(_) => return,
                Err(actual) => floor = actual,
            }
        }
    }

    fn recompute_pending_abort_clean_floor(&self) {
        let mut floor = u64::MAX;
        for shard in &self.pending_abort_clean {
            if let Some((txid, _)) = shard.read().first_key_value() {
                floor = floor.min(*txid);
            }
        }
        self.pending_abort_clean_floor.store(floor, Relaxed);
    }

    pub(crate) fn alloc_view_pin(&self) -> Handle<CCNode> {
        let pin = self.pool.alloc();
        pin.begin_reg();
        #[cfg(feature = "extra_check")]
        crate::testing::fire_view_sync_point(
            crate::testing::ViewSyncPoint::AfterCcnodeRegisteringBeforeTimestampSample,
        );
        let start_ts = self.sample_view_oracle();
        pin.activate(start_ts);
        pin
    }

    pub fn free_view_pin(&self, pin: Handle<CCNode>) {
        pin.clear_idle();
        self.pool.free(pin);
    }

    /// test-determinism wakeup (extra_check): nudges the collector so a test
    /// can drive safe_exclusive advancement synchronously instead of waiting
    /// for the background duty-cycle. production keeps the pure polling
    /// cadence — delayed reclamation is a designed tradeoff, not a leak.
    #[cfg(feature = "extra_check")]
    pub(crate) fn request_collect(&self) {
        let _ = self.tx.send(CollectorSignal::Wake);
    }

    #[inline]
    pub fn group(&self, gid: usize) -> &WriterGroup {
        &self.groups[gid]
    }

    /// the shared durable logging (group_wal_x) in durable mode; None in
    /// relaxed mode
    #[inline]
    pub fn shared_logging(&self) -> Option<&Arc<Mutex<Logging>>> {
        self.shared_logging.as_ref()
    }

    /// publish shared-stream floors before runtime checkpointing or GC
    pub(crate) fn publish_shared_logical_checkpoint_floors(&self, floors: &[Position]) {
        if let Some(shared) = &self.shared_logging {
            shared.lock().set_logical_checkpoint_floors(floors);
        }
    }

    /// deactivate shared-stream floors after an era wipe
    pub(crate) fn reset_shared_logical_checkpoint_floors(&self) {
        if let Some(shared) = &self.shared_logging {
            shared.lock().reset_logical_checkpoint_floors();
        }
    }

    /// rebase writers after recovery truncation
    pub(crate) fn rebase_logging_positions_to_physical_eof(&self) {
        if let Some(shared) = &self.shared_logging {
            shared.lock().rebase_positions_to_physical_eof();
        } else {
            for group in self.groups.iter() {
                group.logging.lock().rebase_positions_to_physical_eof();
            }
        }
    }

    pub fn groups(&self) -> &Vec<WriterGroup> {
        &self.groups
    }

    pub(crate) fn next_group_id(&self) -> usize {
        let nr = self.groups.len();
        let ticket = self.group_rr.fetch_add(1, Relaxed);
        let (home, second) = two_choices(ticket, nr);
        let chosen = {
            let home_load = self.groups[home].inflight();
            if nr == 1 {
                home
            } else {
                let second_load = self.groups[second].inflight();
                if second_load < home_load {
                    second
                } else {
                    home
                }
            }
        };
        self.groups[chosen].enter_inflight();
        chosen
    }

    #[inline]
    pub(crate) fn safe_exclusive(&self) -> u64 {
        self.safe_exclusive.load(Acquire)
    }

    #[inline(always)]
    pub(crate) fn recovering(&self) -> bool {
        self.recovering.load(Acquire)
    }

    pub(crate) fn init_safe_exclusive(&self, recovered_oracle: u64) {
        // recovery must not regress a collector-published boundary
        self.safe_exclusive.fetch_max(recovered_oracle, AcqRel);
        self.recovering.store(false, Release);
    }

    #[inline]
    fn sample_view_oracle(&self) -> u64 {
        self.sequences.oracle.load(SeqCst) // must use seqcst
    }

    pub(crate) fn alloc_oracle(&self) -> u64 {
        self.sequences.oracle.fetch_add(1, AcqRel)
    }

    #[inline]
    pub(crate) fn alloc_begin_oracle(&self) -> u64 {
        self.sequences.oracle.fetch_add(1, SeqCst) // must use seqcst
    }

    #[inline]
    pub(crate) fn build_abort_clean_task(
        &self,
        txid: u64,
        bucket_id: u64,
        logical_group_id: u8,
        physical_wal_id: u8,
        tail_lsn: Position,
        pin_file_id: u64,
    ) -> AbortCleanTask {
        AbortCleanTask {
            txid,
            bucket_id,
            physical_wal_id,
            logical_group_id,
            tail_lsn,
            pin_file_id: pin_file_id.min(tail_lsn.file_id),
            state: AbortCleanState::Pending,
            quiesced: false,
        }
    }

    #[inline]
    pub(crate) fn enqueue_abort_clean_task(&self, task: AbortCleanTask) {
        let _seq = self.pending_abort_clean_seqlock.write_lock();
        let shard = Self::shard_of(task.txid);
        let old = self.pending_abort_clean[shard]
            .write()
            .insert(task.txid, task);
        if old.is_none() {
            self.pending_abort_clean_nr.fetch_add(1, Relaxed);
            self.update_pending_abort_clean_floor(task.txid);
            let bucket_shard = Self::shard_of(task.bucket_id);
            let mut buckets = self.pending_abort_clean_buckets[bucket_shard].write();
            *buckets.entry(task.bucket_id).or_insert(0) += 1;
        }
    }

    #[inline]
    pub(crate) fn enqueue_abort_clean(
        &self,
        txid: u64,
        bucket_id: u64,
        logical_group_id: u8,
        physical_wal_id: u8,
        tail_lsn: Position,
        pin_file_id: u64,
    ) {
        self.enqueue_abort_clean_task(self.build_abort_clean_task(
            txid,
            bucket_id,
            logical_group_id,
            physical_wal_id,
            tail_lsn,
            pin_file_id,
        ));
    }

    pub(crate) fn remove_abort_clean(&self, txid: u64) {
        let old = {
            let _seq = self.pending_abort_clean_seqlock.write_lock();
            let shard = Self::shard_of(txid);
            let old = self.pending_abort_clean[shard].write().remove(&txid);
            if let Some(task) = old {
                self.pending_abort_clean_nr.fetch_sub(1, Relaxed);
                if self.pending_abort_clean_nr.load(Relaxed) == 0 {
                    self.pending_abort_clean_floor.store(u64::MAX, Relaxed);
                } else if txid <= self.pending_abort_clean_floor.load(Relaxed) {
                    self.recompute_pending_abort_clean_floor();
                }
                let bucket_shard = Self::shard_of(task.bucket_id);
                let mut buckets = self.pending_abort_clean_buckets[bucket_shard].write();
                if let Some(cnt) = buckets.get_mut(&task.bucket_id) {
                    *cnt -= 1;
                    if *cnt == 0 {
                        buckets.remove(&task.bucket_id);
                    }
                }
                Some(task)
            } else {
                None
            }
        };
        if let Some(task) = old {
            self.groups[task.logical_group_id as usize].retire_aborted_fact(txid);
        }
    }

    pub(crate) fn abort_clean_tasks(&self) -> Vec<AbortCleanTask> {
        let mut tasks = Vec::new();
        for shard in &self.pending_abort_clean {
            tasks.extend(shard.read().values().copied());
        }
        tasks
    }

    pub(crate) fn mark_abort_clean_wait_quiesce(&self, txid: u64) {
        let shard = Self::shard_of(txid);
        if let Some(task) = self.pending_abort_clean[shard].write().get_mut(&txid)
            && task.state == AbortCleanState::Pending
        {
            task.state = AbortCleanState::WaitingQuiesce;
            task.quiesced = false;
        }
    }

    pub(crate) fn mark_abort_clean_quiesced(&self, txid: u64) {
        let shard = Self::shard_of(txid);
        if let Some(task) = self.pending_abort_clean[shard].write().get_mut(&txid) {
            task.quiesced = true;
        }
    }

    pub(crate) fn abort_clean_event_sink(&self) -> Arc<Mutex<Vec<u64>>> {
        self.abort_clean_events.clone()
    }

    pub(crate) fn drain_abort_clean_events(&self) -> Vec<u64> {
        std::mem::take(&mut *self.abort_clean_events.lock())
    }

    pub(crate) fn min_abort_clean_file_id(&self, physical_wal_id: u8) -> Option<u64> {
        let mut min_id = None;
        for shard in &self.pending_abort_clean {
            let candidate = shard
                .read()
                .values()
                .filter(|x| x.physical_wal_id == physical_wal_id)
                .map(|x| x.pin_file_id)
                .min();
            if let Some(candidate) = candidate {
                min_id = Some(min_id.map_or(candidate, |v: u64| v.min(candidate)));
            }
        }
        min_id
    }

    #[inline]
    pub(crate) fn has_pending_abort_clean_bucket(&self, bucket_id: u64) -> bool {
        let shard = Self::shard_of(bucket_id);
        self.pending_abort_clean_buckets[shard]
            .read()
            .contains_key(&bucket_id)
    }

    #[inline]
    pub(crate) fn compact_safe_txid(&self) -> u64 {
        let mut safe = self.safe_exclusive().saturating_sub(1);
        let pending_floor = self
            .pending_abort_clean_seqlock
            .read(|| self.pending_abort_clean_floor.load(Relaxed));
        if pending_floor != u64::MAX {
            safe = safe.min(pending_floor.saturating_sub(1));
        }
        safe
    }

    pub(crate) fn start(&self) {
        // phase2 establishes the collector's initial safe waterline
        let rx = self
            .collector_rx
            .lock()
            .take()
            .expect("collector receiver must exist once");
        let collector = collect_thread(
            rx,
            self.sequences.clone(),
            self.groups.clone(),
            self.safe_exclusive.clone(),
            self.pool.clone(),
        );
        *self.collector.lock() = Some(collector);
        for w in self.groups.iter() {
            let log = w.logging.lock();
            log.enable_checkpoint();
        }
    }

    /// wait for terminal publication before the shutdown barrier
    pub(crate) fn drain_inflight(&self) {
        while self.groups.iter().any(|g| g.inflight_acquire() != 0) {
            std::thread::yield_now();
        }
    }

    pub(crate) fn quit(&self) {
        self.drain_inflight();
        if self.opt.sync_on_write {
            let ticket = {
                let mut log = self.lock_shared_logging(0);
                must_ok!(log.barrier_register())
            };
            must_ok!(self.drive_sync(&ticket));
        } else {
            for x in self.groups.iter() {
                let mut log = x.logging.lock();
                must_ok!(log.sync(true));
            }
        }
        must_ok!(self.tx.send(CollectorSignal::Quit));

        if let Some(h) = self.collector.lock().take() {
            let _ = h.join();
        }
    }

    /// bounded merge window without holding the logging mutex
    fn merge_window(&self, ticket: &SyncTicket) {
        if !self.opt.sync_on_write {
            return;
        }
        let cap_us = self.opt.sync_merge_window_us;
        if cap_us == 0 {
            return;
        }
        let cap = Duration::from_micros(cap_us);
        let quiet = cap.min(Duration::from_micros(40));
        let started = Instant::now();
        while started.elapsed() < cap {
            if !ticket.has_followers() && started.elapsed() > quiet {
                break;
            }
            std::thread::sleep(Duration::from_micros(1));
        }
    }

    /// lock a logical group's WAL logger
    #[inline]
    pub(crate) fn lock_wal_logging(&self, gid: usize, seed: u64) -> MutexGuard<'_, Logging> {
        let logging = &self.groups[gid].logging;
        if self.opt.sync_on_write {
            let started = sampled_instant(seed, LATENCY_SAMPLE_SHIFT);
            let guard = logging.lock();
            observe_elapsed(
                self.opt.observer.as_ref(),
                HistogramMetric::WalLockWaitMicros,
                started,
            );
            guard
        } else {
            logging.lock()
        }
    }

    #[inline]
    pub(crate) fn lock_shared_logging(&self, seed: u64) -> MutexGuard<'_, Logging> {
        let shared = self
            .shared_logging()
            .expect("durable mode must construct the shared logging");
        let started = sampled_instant(seed, LATENCY_SAMPLE_SHIFT);
        let guard = shared.lock();
        observe_elapsed(
            self.opt.observer.as_ref(),
            HistogramMetric::WalLockWaitMicros,
            started,
        );
        guard
    }

    /// complete a leader ticket or wait for a follower ticket
    pub(crate) fn drive_sync(&self, ticket: &SyncTicket) -> Result<(), OpCode> {
        if ticket.is_completed() {
            return Ok(());
        }
        let wait_started = sampled_instant(
            ticket.target().file_id ^ ticket.target().offset,
            LATENCY_SAMPLE_SHIFT,
        );
        #[cfg(feature = "extra_check")]
        crate::testing::fire_wal_sync_point(if ticket.is_leader() {
            crate::testing::WalSyncPoint::AfterLeaderRegisterBeforeSeal
        } else {
            crate::testing::WalSyncPoint::AfterFollowerRegister
        });
        if ticket.is_leader() {
            self.merge_window(ticket);
            let mut log =
                self.lock_shared_logging(ticket.target().file_id ^ ticket.target().offset);
            log.leader_seal_sync_and_complete(ticket);
        } else {
            ticket.wait();
        }
        observe_elapsed(
            self.opt.observer.as_ref(),
            HistogramMetric::WalGenerationWaitMicros,
            wait_started,
        );
        let result = ticket.check_result();
        #[cfg(feature = "extra_check")]
        if result.is_ok() {
            let log = self.lock_shared_logging(0);
            must_true!(
                log.durable_pos() >= ticket.target(),
                "generation ticket must be covered by durable_pos"
            );
            if let Some(cut) = ticket.sealed_cut() {
                must_true!(
                    log.durable_pos() >= cut,
                    "durable_pos must reach the sealed generation cut"
                );
            }
        }
        result
    }

    pub fn sync(&self) -> Result<(), OpCode> {
        if self.opt.sync_on_write {
            let ticket = {
                let mut log = self.lock_shared_logging(0);
                log.barrier_register()?
            };
            self.drive_sync(&ticket)?;
        } else {
            for group in self.groups.iter() {
                let mut log = group.logging.lock();
                log.sync(true)?;
            }
        }
        Ok(())
    }
}

fn collect_thread(
    reader: Receiver<CollectorSignal>,
    sequences: Arc<Sequences>,
    groups: Arc<Vec<WriterGroup>>,
    safe_exclusive: Arc<AtomicU64>,
    pool: Arc<CCPool>,
) -> JoinHandle<()> {
    // one collector per mace instance; the token distinguishes this
    // instance's collector from every other instance's in the process
    #[cfg(feature = "extra_check")]
    let collector_token = Arc::as_ptr(&sequences) as usize;
    std::thread::Builder::new()
        .name("collector".into())
        .spawn(move || {
            let mut committed = Vec::new();
            let mut registry_nodes = Vec::with_capacity(CCPOOL_SHARD);
            let mut idle_delay = collector_idle_delay(Duration::ZERO);
            while let Err(RecvTimeoutError::Timeout) | Ok(CollectorSignal::Wake) =
                reader.recv_timeout(idle_delay)
            {
                let cost = run_collect_cycle(
                    &sequences,
                    groups.as_ref(),
                    safe_exclusive.as_ref(),
                    &pool,
                    &mut committed,
                    &mut registry_nodes,
                    #[cfg(feature = "extra_check")]
                    collector_token,
                );
                idle_delay = collector_idle_delay(cost);
                if drain_pending_wakes(&reader) {
                    break;
                }
            }
        })
        .expect("can't start collector thread")
}

const MAINTENANCE_BUDGET: Duration = Duration::from_micros(200);
const COLLECTOR_MIN_SLEEP: Duration = Duration::from_millis(10);
const PRUNE_LOCK_BATCH: usize = 256;
const DUTY_CYCLE_SLEEP_MULTIPLIER: u32 = 19;

fn run_collect_cycle(
    sequences: &Sequences,
    groups: &[WriterGroup],
    safe_exclusive: &AtomicU64,
    pool: &CCPool,
    committed: &mut Vec<(usize, u64, u64)>,
    registry_nodes: &mut Vec<*mut CCNode>,
    #[cfg(feature = "extra_check")] collector_token: usize,
) -> Duration {
    let proof_scan_started = Instant::now();
    // `SeqCst` linearizes this cut with writer/view registration and timestamp sampling (`oracle`,
    // `WriterGroup::txn_seq`, and `CCNode::state`)
    //
    // forbidden execution: the collector misses a live writer or view registration while it
    // samples start_ts < cut
    //
    // if the scan does not cover a registration that stays live, its state load must precede the
    // `Registering` store in the `SeqCst` order; because this cut precedes the scan and registration
    // precedes the oracle allocation/sample, the resulting cut < scan < registration < oracle order
    // proves start_ts >= cut
    let cut = sequences.oracle.load(SeqCst);
    #[cfg(feature = "extra_check")]
    crate::testing::fire_collector_sync_point(
        crate::testing::CollectorSyncPoint::AfterCollectorCutBeforeFactScan,
        cut,
    );

    let mut floor = cut;
    let mut can_publish = true;
    committed.clear();

    for group in groups.iter() {
        match group.stable_ts() {
            RegistrationTs::None => {}
            RegistrationTs::Published(start_ts) => {
                floor = floor.min(start_ts);
            }
            RegistrationTs::Pending => {
                can_publish = false;
            }
        }
        for fact in group.facts.iter() {
            let start_ts = *fact.key();
            match *fact.value() {
                TxnFact::Active(_) => {
                    floor = floor.min(start_ts);
                }
                TxnFact::Committed(commit_ts) => {
                    committed.push((group.id, start_ts, commit_ts));
                }
                TxnFact::Aborted => {}
            }
        }
    }

    let _guard = crossbeam_epoch::pin();
    snapshot_registry_nodes(pool, registry_nodes);
    for &node in registry_nodes.iter() {
        let cc = unsafe { &*node };
        match cc.collect() {
            ViewState::Idle => {}
            ViewState::Registering => {
                can_publish = false;
            }
            ViewState::Active(start_ts) => {
                floor = floor.min(start_ts);
            }
        }
    }

    let mut published_safe = safe_exclusive.load(Acquire);
    if can_publish {
        let mut committed_floor = u64::MAX;
        for &(_, start_ts, commit_ts) in committed.iter() {
            if commit_ts >= floor {
                committed_floor = committed_floor.min(start_ts);
            }
        }
        let candidate = floor.min(committed_floor);
        debug_assert!(candidate <= cut);
        debug_assert!(candidate >= published_safe);
        if candidate > published_safe {
            safe_exclusive.store(candidate, Release);
            published_safe = candidate;
        }
    }
    let proof_scan_cost = proof_scan_started.elapsed();

    let deadline = Instant::now() + MAINTENANCE_BUDGET;
    if published_safe != 0 {
        #[cfg(feature = "extra_check")]
        crate::testing::fire_collector_sync_point(
            crate::testing::CollectorSyncPoint::AfterSafePublishBeforeCommittedFactPrune,
            published_safe,
        );
        prune_committed_facts(groups, committed, published_safe, deadline);
    }
    #[cfg(feature = "extra_check")]
    crate::testing::fire_collector_completed(collector_token);
    if Instant::now() < deadline {
        pool.maybe_shrink_until(deadline);
    }
    proof_scan_cost
}

fn prune_committed_facts(
    groups: &[WriterGroup],
    committed: &[(usize, u64, u64)],
    safe_exclusive: u64,
    deadline: Instant,
) {
    for (idx, &(group_id, start_ts, _)) in committed.iter().enumerate() {
        if start_ts >= safe_exclusive {
            continue;
        }
        if idx > 0 && idx.is_multiple_of(PRUNE_LOCK_BATCH) && Instant::now() >= deadline {
            break;
        }
        groups[group_id].facts.remove(&start_ts);
    }
}

fn collector_idle_delay(proof_scan_cost: Duration) -> Duration {
    proof_scan_cost
        .checked_mul(DUTY_CYCLE_SLEEP_MULTIPLIER)
        .map_or(Duration::MAX, |delay| delay.max(COLLECTOR_MIN_SLEEP))
}

fn drain_pending_wakes(reader: &Receiver<CollectorSignal>) -> bool {
    loop {
        match reader.try_recv() {
            Ok(CollectorSignal::Wake) => {}
            Ok(CollectorSignal::Quit) | Err(TryRecvError::Disconnected) => return true,
            Err(TryRecvError::Empty) => return false,
        }
    }
}

fn snapshot_registry_nodes(pool: &CCPool, registry_nodes: &mut Vec<*mut CCNode>) {
    let registry = pool.registry.read();
    registry_nodes.clear();
    registry_nodes.extend(registry.iter().map(|node| node.inner()));
}

const CCPOOL_SHARD: usize = 32;
const CCPOOL_SHARD_MASK: usize = CCPOOL_SHARD - 1;

const VIEW_STATE_IDLE: u64 = 0;
const VIEW_STATE_REGISTERING: u64 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ViewState {
    Idle,
    Registering,
    Active(u64),
}

#[inline]
fn encode_view_state(state: ViewState) -> u64 {
    match state {
        ViewState::Idle => VIEW_STATE_IDLE,
        ViewState::Registering => VIEW_STATE_REGISTERING,
        ViewState::Active(start_ts) => {
            must_true!(start_ts <= u64::MAX - 2);
            start_ts + 2
        }
    }
}

#[inline]
fn decode_view_state(raw: u64) -> ViewState {
    match raw {
        VIEW_STATE_IDLE => ViewState::Idle,
        VIEW_STATE_REGISTERING => ViewState::Registering,
        x => ViewState::Active(x - 2),
    }
}

pub(crate) struct CCNode {
    state: AtomicU64,
    shard_index: usize,
    registry_index: usize,
}

impl CCNode {
    fn new() -> Self {
        Self {
            state: AtomicU64::new(encode_view_state(ViewState::Idle)),
            shard_index: 0,
            registry_index: 0,
        }
    }

    fn begin_reg(&self) {
        must_true!(matches!(
            decode_view_state(self.state.load(Relaxed)),
            ViewState::Idle
        ));
        self.state
            .store(encode_view_state(ViewState::Registering), SeqCst);
    }

    fn activate(&self, start_ts: u64) {
        must_true!(matches!(
            decode_view_state(self.state.load(Relaxed)),
            ViewState::Registering
        ));
        self.state
            .store(encode_view_state(ViewState::Active(start_ts)), Release);
    }

    fn clear_idle(&self) {
        self.state
            .store(encode_view_state(ViewState::Idle), Release);
    }

    pub(crate) fn start_ts(&self) -> u64 {
        match decode_view_state(self.state.load(Acquire)) {
            ViewState::Active(start_ts) => start_ts,
            ViewState::Idle | ViewState::Registering => NULL_ORACLE,
        }
    }

    fn collect(&self) -> ViewState {
        decode_view_state(self.state.load(SeqCst))
    }
}

struct CCPool {
    shards: [Mutex<Vec<Handle<CCNode>>>; CCPOOL_SHARD],
    shard_index: CachePad<AtomicUsize>,
    // TODO: maybe change to seqlock ?
    registry: RwLock<Vec<Handle<CCNode>>>,
    registry_len: CachePad<AtomicUsize>,
}

impl CCPool {
    fn new() -> Self {
        let mut registry = Vec::with_capacity(CCPOOL_SHARD);
        let mut shards = std::array::from_fn(|_| Mutex::new(Vec::new()));
        for shard in shards.iter_mut() {
            let mut h = Handle::new(CCNode::new());
            h.registry_index = registry.len();
            registry.push(h);
            shard.get_mut().push(h);
        }
        Self {
            shards,
            shard_index: CachePad::default(),
            registry: RwLock::new(registry),
            registry_len: CachePad::from(AtomicUsize::new(CCPOOL_SHARD)),
        }
    }

    fn next_ticket(&self) -> usize {
        self.shard_index.fetch_add(1, Relaxed)
    }

    fn try_pop_shard(&self, index: usize) -> Option<Handle<CCNode>> {
        let mut h = self.shards[index].lock().pop()?;
        h.shard_index = index;
        Some(h)
    }

    fn push_shard(&self, index: usize, cc: Handle<CCNode>) {
        self.shards[index].lock().push(cc);
    }

    fn alloc(&self) -> Handle<CCNode> {
        let ticket = self.next_ticket();
        let (shard, second) = two_choices(ticket, CCPOOL_SHARD);

        if let Some(x) = self.try_pop_shard(shard) {
            x
        } else if let Some(x) = self.try_pop_shard(second) {
            x
        } else {
            let mut cc = Handle::new(CCNode::new());
            cc.shard_index = shard;
            let mut r = self.registry.write();
            cc.registry_index = r.len();
            r.push(cc);
            self.registry_len.store(r.len(), Release);
            cc
        }
    }

    fn free(&self, cc: Handle<CCNode>) {
        self.push_shard(cc.shard_index, cc);
    }

    fn maybe_shrink_one(&self, start: usize, guard: &Guard) -> bool {
        let mut victim = None;
        for i in 0..CCPOOL_SHARD {
            let idx = (start + i) & CCPOOL_SHARD_MASK;
            if let Some(cc) = self.try_pop_shard(idx) {
                victim = Some((idx, cc));
                break;
            }
        }
        let Some((idx, cc)) = victim else {
            return false;
        };
        let Some(mut r) = self.registry.try_write() else {
            self.push_shard(idx, cc);
            return false;
        };
        if r.len() <= CCPOOL_SHARD {
            drop(r);
            self.push_shard(idx, cc);
            return false;
        }

        let last = r.swap_remove(cc.registry_index);
        must_true!(eq last.inner(), cc.inner());
        if cc.registry_index < r.len() {
            r[cc.registry_index].registry_index = cc.registry_index;
        }
        self.registry_len.store(r.len(), Release);
        drop(r);
        guard.defer(move || cc.reclaim());
        true
    }

    fn maybe_shrink_until(&self, deadline: Instant) {
        let len = self.registry_len.load(Acquire);
        if len <= CCPOOL_SHARD {
            return;
        }
        let backlog = len - CCPOOL_SHARD;
        let mut batch = backlog / 8;
        if !backlog.is_multiple_of(8) {
            batch += 1;
        }
        batch = batch.clamp(1, 16);

        let guard = crossbeam_epoch::pin();
        let mut start = self.next_ticket() & CCPOOL_SHARD_MASK;
        let mut done = 0;
        while done < batch
            && self.registry_len.load(Acquire) > CCPOOL_SHARD
            && Instant::now() < deadline
        {
            if !self.maybe_shrink_one(start, &guard) {
                break;
            }
            done += 1;
            start = (start + 1) & CCPOOL_SHARD_MASK;
        }
    }
}

impl Drop for CCPool {
    fn drop(&mut self) {
        let mut r = self.registry.write();
        while let Some(x) = r.pop() {
            x.reclaim();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CCNode, CCPOOL_SHARD, CCPool, COLLECTOR_MIN_SLEEP, CollectorSignal, Context,
        DUTY_CYCLE_SLEEP_MULTIPLIER, collector_idle_delay, drain_pending_wakes, two_choices,
    };
    use crate::cc::context::ViewState;
    use crate::utils::observe::InMemoryObserver;
    use crate::{Options, RandomPath, meta::Sequences};
    use std::sync::Arc;
    use std::sync::atomic::Ordering::Relaxed;
    use std::sync::mpsc::channel;
    use std::time::{Duration, Instant};

    fn new_context_with_groups(groups: u8) -> (RandomPath, Context) {
        let observer = Arc::new(InMemoryObserver::default());
        new_context_with_groups_and_observer(groups, observer)
    }

    fn new_context_with_groups_and_observer(
        groups: u8,
        observer: Arc<InMemoryObserver>,
    ) -> (RandomPath, Context) {
        let root = RandomPath::tmp();
        let mut opt = Options::new(&*root);
        opt.concurrent_write = groups;
        opt.observer = observer;
        let ctx = Context::new(
            Arc::new(opt.validate().expect("context options must validate")),
            Arc::new(Sequences::default()),
            &[],
        );
        (root, ctx)
    }

    #[test]
    fn ccpool_shrink_reclaims_idle_nodes() {
        let pool = CCPool::new();
        let total = CCPOOL_SHARD + 8;
        let mut handles = Vec::with_capacity(total);
        for i in 0..total {
            let h = pool.alloc();
            h.begin_reg();
            h.activate(i as u64 + 1);
            handles.push(h);
        }
        assert!(pool.registry_len.load(Relaxed) >= total);

        for h in handles {
            h.clear_idle();
            pool.free(h);
        }
        for _ in 0..(total * 4) {
            pool.maybe_shrink_until(Instant::now() + Duration::from_millis(10));
            if pool.registry_len.load(Relaxed) == CCPOOL_SHARD {
                break;
            }
        }
        assert_eq!(pool.registry_len.load(Relaxed), CCPOOL_SHARD);
    }

    #[test]
    fn ccpool_alloc_free_fast_path_keeps_registry_len() {
        let pool = CCPool::new();
        let base_len = pool.registry_len.load(Relaxed);
        let h = pool.alloc();
        h.begin_reg();
        h.activate(10);
        h.clear_idle();
        pool.free(h);
        assert_eq!(pool.registry_len.load(Relaxed), base_len);

        let h2 = pool.alloc();
        h2.begin_reg();
        h2.activate(11);
        assert_eq!(h2.start_ts(), 11);
        h2.clear_idle();
        pool.free(h2);
    }

    #[test]
    fn ccnode_registration_state_is_visible_to_collector() {
        let node = CCNode::new();
        assert_eq!(node.collect(), ViewState::Idle);

        node.begin_reg();
        assert_eq!(node.collect(), ViewState::Registering);

        node.activate(10);
        assert_eq!(node.collect(), ViewState::Active(10));

        node.clear_idle();
        assert_eq!(node.collect(), ViewState::Idle);
    }

    #[test]
    fn next_group_id_only_compares_home_and_second_candidate() {
        let (_root, ctx) = new_context_with_groups(4);
        ctx.group_rr.store(0, Relaxed);

        ctx.group(0).enter_inflight();
        ctx.group(0).enter_inflight();
        ctx.group(1).enter_inflight();
        ctx.group(1).enter_inflight();
        ctx.group(1).enter_inflight();

        let chosen = ctx.next_group_id();
        assert_eq!(chosen, 0);
        assert_eq!(ctx.group(0).inflight(), 3);
        assert_eq!(ctx.group(1).inflight(), 3);
        assert_eq!(ctx.group(2).inflight(), 0);
        assert_eq!(ctx.group(3).inflight(), 0);

        ctx.group(0).leave_inflight();
        ctx.group(0).leave_inflight();
        ctx.group(0).leave_inflight();
        ctx.group(1).leave_inflight();
        ctx.group(1).leave_inflight();
        ctx.group(1).leave_inflight();
        ctx.quit();
    }

    #[test]
    fn quit_waits_for_inflight_then_joins_collector() {
        let (_root, ctx) = new_context_with_groups(2);
        // start() spawns the collector (it must not run during recovery);
        // the test needs it alive to prove quit joins it
        ctx.start();
        let ctx = Arc::new(ctx);
        let (started_tx, started_rx) = channel();
        let (release_tx, release_rx) = channel();

        let holder_ctx = ctx.clone();
        let holder = std::thread::spawn(move || {
            holder_ctx.group(0).enter_inflight();
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            holder_ctx.group(0).leave_inflight();
        });
        started_rx.recv().unwrap();

        let quitter_ctx = ctx.clone();
        let quitter = std::thread::spawn(move || quitter_ctx.quit());
        std::thread::sleep(Duration::from_millis(20));
        assert!(
            !quitter.is_finished(),
            "quit must wait for in-flight transactions before force barrier"
        );
        release_tx.send(()).unwrap();
        holder.join().unwrap();
        quitter.join().unwrap();

        assert!(
            ctx.collector.lock().is_none(),
            "collector must be joined before quit returns"
        );
    }

    #[test]
    fn two_choices_are_distinct_and_cover_every_pair() {
        for nr in 1..=CCPOOL_SHARD {
            let mut seen = vec![vec![false; nr]; nr];
            for round in 0..nr.max(2) - 1 {
                for expected_home in 0..nr {
                    let ticket = round * nr + expected_home;
                    let (home, second) = two_choices(ticket, nr);
                    assert_eq!(home, expected_home);
                    if nr == 1 {
                        assert_eq!(second, home);
                    } else {
                        assert_ne!(second, home);
                        seen[home][second] = true;
                    }
                }
            }

            if nr > 1 {
                for (home, choices) in seen.iter().enumerate() {
                    assert!(
                        choices
                            .iter()
                            .enumerate()
                            .all(|(second, &visited)| second == home || visited)
                    );
                }
            }
        }
    }

    #[test]
    fn collector_idle_delay_enforces_minimum_and_duty_cycle_bound() {
        assert_eq!(collector_idle_delay(Duration::ZERO), COLLECTOR_MIN_SLEEP);
        assert_eq!(
            collector_idle_delay(Duration::from_micros(100)),
            COLLECTOR_MIN_SLEEP
        );
        assert_eq!(
            collector_idle_delay(Duration::from_millis(3)),
            Duration::from_millis(3 * DUTY_CYCLE_SLEEP_MULTIPLIER as u64)
        );
    }

    #[test]
    fn collector_wake_drain_coalesces_pending_signals() {
        let (_tx, reader) = channel();
        assert!(!drain_pending_wakes(&reader));

        let (tx, reader) = channel();
        tx.send(CollectorSignal::Wake).unwrap();
        tx.send(CollectorSignal::Wake).unwrap();
        assert!(!drain_pending_wakes(&reader));
        assert!(!drain_pending_wakes(&reader));

        let (tx, reader) = channel();
        tx.send(CollectorSignal::Wake).unwrap();
        tx.send(CollectorSignal::Quit).unwrap();
        assert!(drain_pending_wakes(&reader));
    }
}
