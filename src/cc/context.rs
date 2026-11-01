use crate::cc::log::{Logging, SyncTicket};
use crate::meta::Sequences;
use crate::{must_ok, must_true};
use crossbeam_epoch::Guard;
use parking_lot::{Mutex, MutexGuard, RwLock};
use rustc_hash::FxHashMap;
use std::collections::BTreeMap;

use crate::OpCode;
use crate::utils::data::Position;
use crate::utils::instance_local::LocalSlot;
#[cfg(feature = "metrics")]
use crate::utils::observe::{
    HistogramMetric, LATENCY_SAMPLE_SHIFT, observe_elapsed, sampled_instant,
};
use crate::utils::options::{Options, ParsedOptions};
use crate::utils::seqlock::SeqLock;
use crate::utils::{CachePad, Handle, NULL_ORACLE};

use super::group::{RegistrationTs, TxnFact, WriterGroup};
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release, SeqCst};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, TryRecvError, channel};
use std::sync::{Arc, OnceLock, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

mod abort_clean;
mod collector;
mod drive;

pub(crate) use abort_clean::{AbortCleanState, AbortCleanTask};
pub(crate) use collector::CCNode;
#[cfg(test)]
use collector::{
    CCPOOL_SHARD, COLLECTOR_MIN_SLEEP, DUTY_CYCLE_SLEEP_MULTIPLIER, ViewState,
    collector_idle_delay, drain_pending_wakes,
};
use collector::{CCPool, CollectorSignal, collect_thread, two_choices};

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

pub struct Context {
    pub(crate) opt: Arc<ParsedOptions>,
    pub(crate) sequences: Arc<Sequences>,
    safe_exclusive: Arc<AtomicU64>,
    /// recovery roots use `Position::MIN` until phase2 completes
    recovering: AtomicBool,
    /// a rebuild open detached its loggers and has not installed the full new era
    /// yet: the runtime WAL barrier is meaningless in this window, so `quit` and
    /// `sync` must not run and every error leaves through the recovery-open abort
    recovery_abort_only: AtomicBool,
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
        groups: u8,
        sequences: Arc<Sequences>,
        group_boot: &[GroupBoot],
    ) -> Self {
        // the group count is not always `opt.concurrent_write`: recovery builds its Context with
        // the persisted (source) width while sharing the target options allocation, and only the
        // runtime Context uses the target width
        // the route is always the requested one, never the persisted era's: the retained scan
        // enumerates physical boots at the source width (shared streams included), while the
        // loggers lay out the new era, so a route-only switch carries this Context over as the
        // runtime one and never hands off
        let cores = groups as usize;
        // the boot list must describe exactly this Context's width: a caller that cannot produce a
        // boot for every slot must fix its plan instead of relying on a silent default slot
        assert!(
            group_boot.len() == cores + 1,
            "boot slice must hold one boot per logical group plus the shared boot, got {} for {} groups",
            group_boot.len(),
            cores
        );
        // per-group boots followed by the shared-stream boot
        let per_group_boot = |i: usize| group_boot[i];

        let ckpt_cnts: Vec<Arc<AtomicUsize>> =
            (0..cores).map(|_| Arc::new(AtomicUsize::new(0))).collect();

        let mut groups_logging: Vec<Arc<Mutex<Logging>>> = Vec::with_capacity(cores);
        let mut shared_logging = None;
        if opt.sync_on_write {
            // durable mode shares one logger across all logical groups
            let shared_boot = group_boot[cores];
            let shared = Arc::new(Mutex::new(Logging::new(
                Options::SHARED_ID,
                cores,
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
                    cores,
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

        let pool = CCPool::new();
        let groups = Arc::new(groups);
        let safe_exclusive = Arc::new(AtomicU64::new(sequences.oracle.load(Acquire)));
        let (tx, rx) = channel();

        Self {
            opt: opt.clone(),
            sequences,
            safe_exclusive,
            recovering: AtomicBool::new(true),
            recovery_abort_only: AtomicBool::new(false),
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
    pub fn group(&self, gid: usize) -> &WriterGroup {
        &self.groups[gid]
    }

    /// the shared durable logging (group_wal_x) in durable mode; None in
    /// relaxed mode
    #[inline]
    pub fn shared_logging(&self) -> Option<&Arc<Mutex<Logging>>> {
        self.shared_logging.as_ref()
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

    /// the loggers this context writes through: one shared logger in durable mode,
    /// one per logical group in relaxed mode. every one of them owns a physical
    /// stream the era wipe covers
    fn writer_loggings(&self) -> Vec<Arc<Mutex<Logging>>> {
        match &self.shared_logging {
            Some(shared) => vec![shared.clone()],
            None => self
                .groups
                .iter()
                .map(|group| group.logging.clone())
                .collect(),
        }
    }

    /// a rebuild open is between detaching its recovery writers and installing the
    /// full new era: neither the runtime barrier nor a foreground sync is defined
    pub(crate) fn recovery_abort_only(&self) -> bool {
        self.recovery_abort_only.load(Acquire)
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
}

#[cfg(test)]
mod tests {
    use super::{
        CCNode, CCPOOL_SHARD, CCPool, COLLECTOR_MIN_SLEEP, CollectorSignal, Context,
        DUTY_CYCLE_SLEEP_MULTIPLIER, GroupBoot, collector_idle_delay, drain_pending_wakes,
        two_choices,
    };
    use crate::cc::context::ViewState;
    use crate::utils::data::Position;
    #[cfg(feature = "metrics")]
    use crate::utils::observe::InMemoryObserver;
    use crate::{Options, RandomPath, meta::Sequences};
    use std::sync::Arc;
    use std::sync::atomic::Ordering::Relaxed;
    use std::sync::mpsc::channel;
    use std::time::{Duration, Instant};

    fn new_context_with_groups(groups: u8) -> (RandomPath, Context) {
        let root = RandomPath::tmp();
        let mut opt = Options::new(&*root);
        opt.concurrent_write = groups;
        #[cfg(feature = "metrics")]
        {
            opt.observer = Arc::new(InMemoryObserver::default());
        }
        let mut boots: Vec<GroupBoot> = (0..groups).map(test_boot).collect();
        boots.push(test_boot(Options::SHARED_ID));
        let ctx = Context::new(
            Arc::new(opt.validate().expect("context options must validate")),
            groups,
            Arc::new(Sequences::default()),
            &boots,
        );
        (root, ctx)
    }

    /// an empty, file-less boot for the given physical stream
    fn test_boot(physical_wal_id: u8) -> GroupBoot {
        GroupBoot {
            physical_wal_id,
            oldest_id: 0,
            latest_id: 0,
            scan_start: Position::MIN,
            has_files: false,
            start_id: 0,
            checkpoint_floor: Position::MIN,
        }
    }

    #[test]
    fn context_takes_its_width_from_the_explicit_group_count() {
        // the target options width and the Context's width differ during a width migration:
        // recovery builds a source-width Context on the target options allocation, so a boot
        // slice shaped for the explicit group count must be accepted
        let root = RandomPath::tmp();
        let mut opt = Options::new(&*root);
        opt.concurrent_write = 4;
        let boots: Vec<GroupBoot> = (0..2)
            .map(test_boot)
            .chain([test_boot(Options::SHARED_ID)])
            .collect();
        let _ = Context::new(
            Arc::new(opt.validate().expect("context options must validate")),
            2,
            Arc::new(Sequences::default()),
            &boots,
        );
    }

    #[test]
    #[should_panic(expected = "boot slice must hold one boot per logical group")]
    fn context_rejects_a_boot_slice_that_does_not_match_its_width() {
        // a slice shaped for the options width instead of the Context's own width is rejected
        // rather than silently filling the missing slots with a default boot
        let root = RandomPath::tmp();
        let mut opt = Options::new(&*root);
        opt.concurrent_write = 4;
        let boots: Vec<GroupBoot> = (0..4)
            .map(test_boot)
            .chain([test_boot(Options::SHARED_ID)])
            .collect();
        let _ = Context::new(
            Arc::new(opt.validate().expect("context options must validate")),
            2,
            Arc::new(Sequences::default()),
            &boots,
        );
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
    fn ccpool_local_path_reuses_same_node_on_same_thread() {
        let _alloc = crate::utils::instance_local::alloc_test_guard();
        let pool = CCPool::new();
        let h = pool.alloc();
        let inner = h.inner();
        h.begin_reg();
        h.activate(10);
        h.clear_idle();
        pool.free(h); // enters this thread's local cache

        // the instance-local path must hand the just-freed node back without
        // touching a shard; the legacy path could return a different node
        let h2 = pool.alloc();
        assert_eq!(h2.inner(), inner, "local cache must reuse the freed node");
        h2.clear_idle();
        pool.free(h2);
        drop(pool);
    }

    #[test]
    fn ccpool_local_alloc_free_keeps_registry_bounded() {
        let _alloc = crate::utils::instance_local::alloc_test_guard();
        let pool = CCPool::new();
        let base_len = pool.registry_len.load(Relaxed);
        for i in 0..64u64 {
            let h = pool.alloc();
            h.begin_reg();
            h.activate(i + 1);
            h.clear_idle();
            pool.free(h);
        }
        // steady alloc/free on one thread never grows the registry: nodes cycle
        // through the local cache and spill back to shards
        assert_eq!(pool.registry_len.load(Relaxed), base_len);
        drop(pool);
    }

    #[test]
    fn ccpool_thread_exit_returns_local_pins_to_shard() {
        let _alloc = crate::utils::instance_local::alloc_test_guard();
        let pool = CCPool::new();
        let (tx, rx) = std::sync::mpsc::channel::<(usize, usize)>();
        let worker_pool = Arc::clone(&pool);
        std::thread::spawn(move || {
            let h = worker_pool.alloc();
            h.begin_reg();
            h.activate(1);
            h.clear_idle();
            let shard = h.shard_index;
            let inner = h.inner() as usize;
            worker_pool.free(h); // enters the worker thread's local cache
            tx.send((inner, shard)).expect("parent is waiting");
            // worker exits: the exit hook must push the cached node back to its shard
        })
        .join()
        .expect("worker completed");

        let (inner, shard) = rx.recv().expect("worker result");
        let back = pool.shards[shard]
            .lock()
            .iter()
            .any(|n| n.inner() as usize == inner);
        assert!(
            back,
            "exit hook must return the idle node to its original shard"
        );
    }

    #[test]
    fn ccpool_local_never_hands_one_node_to_two_live_views() {
        // regression for the init-path double-push: the duplicate could enter
        // the local list only when cache_local_pin's init closure runs, i.e. on
        // a thread whose FIRST slot operation is a free. so a fresh worker
        // frees a node (init path), then allocates twice and keeps both live
        let _alloc = crate::utils::instance_local::alloc_test_guard();
        let pool = CCPool::new();
        for round in 0..3u64 {
            let h = pool.alloc();
            h.begin_reg();
            h.activate(round * 3 + 1);
            h.clear_idle();
            let worker_pool = Arc::clone(&pool);
            let (done_tx, done_rx) = std::sync::mpsc::channel();
            let worker = std::thread::spawn(move || {
                // first slot op on this thread is a free: exercises the init path
                worker_pool.free(h);
                let a = worker_pool.alloc();
                a.begin_reg();
                a.activate(round * 3 + 2);
                let b = worker_pool.alloc();
                b.begin_reg();
                b.activate(round * 3 + 3);
                assert_ne!(
                    a.inner() as usize,
                    b.inner() as usize,
                    "two live views must hold distinct CCNodes (round {round})"
                );
                a.clear_idle();
                b.clear_idle();
                worker_pool.free(a);
                worker_pool.free(b);
                done_tx.send(()).expect("parent is waiting");
            });
            done_rx.recv().expect("worker finished");
            worker.join().expect("worker completed");
        }
        drop(pool);
    }

    #[test]
    fn ccpool_local_pool_drop_concurrent_with_live_foreign_cell_does_not_deadlock() {
        // ordering smoke test: the shard return must never run CCPool::drop while
        // holding the registry threads lock. here a worker holds a LocalPins cell
        // (node cached) while the last external Arc is dropped on the main
        // thread; CCPool::drop drains the foreign cell (upgrade-failure branch)
        // and the worker then exits with its cell already gone. the structural
        // guarantee (LocalPins::drop runs only at drop(retired)/drop(entries),
        // both outside the lock) is verified by the review; this test pins the
        // drain-foreign-cell ordering and post-drop thread exit end to end
        let _alloc = crate::utils::instance_local::alloc_test_guard();
        let pool = CCPool::new();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let worker_pool = Arc::clone(&pool);
        let worker = std::thread::spawn(move || {
            let h = worker_pool.alloc();
            h.begin_reg();
            h.activate(1);
            h.clear_idle();
            worker_pool.free(h); // cached in the worker's LocalPins
            drop(worker_pool); // worker still holds its cell; only main ref left
            ready_tx.send(()).expect("parent is waiting");
            release_rx.recv().expect("parent releases worker");
        });

        ready_rx.recv().expect("worker cached a pin");
        drop(pool); // last external Arc; CCPool::drop drains the worker's cell
        release_tx.send(()).expect("worker is alive");
        worker.join().expect("worker completed");
    }

    #[test]
    fn ccpool_local_pins_drop_with_pool_reclaims_once() {
        let _alloc = crate::utils::instance_local::alloc_test_guard();
        // worker allocates and frees (node cached in worker local), exits (hook
        // returns it); then the pool drops and reclaims the registry exactly once
        let pool = CCPool::new();
        let worker_pool = Arc::clone(&pool);
        std::thread::spawn(move || {
            let h = worker_pool.alloc();
            h.begin_reg();
            h.activate(1);
            h.clear_idle();
            worker_pool.free(h);
        })
        .join()
        .expect("worker completed");
        drop(pool); // must not panic; registry reclaim runs once
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

    /// the same-width rebuild walks this context through its abort window and back:
    /// detaching arms it, a full install disarms it, and every stream owns the era
    #[test]
    fn rebuild_handoff_arms_and_disarms_the_abort_window() {
        let (_root, ctx) = new_context_with_groups(2);
        assert!(
            !ctx.recovery_abort_only(),
            "a fresh recovery context is not abort-only"
        );
        ctx.detach_recovery_writers(0)
            .expect("a quiescent context must detach its streams");
        assert!(
            ctx.recovery_abort_only(),
            "detaching a writer must arm the abort window"
        );

        ctx.install_epoch_writers(9)
            .expect("install must publish every era writer");
        assert!(
            !ctx.recovery_abort_only(),
            "a full install must disarm the abort window"
        );
        for group in ctx.groups().iter() {
            let logging = group.logging.lock();
            assert_eq!(logging.current_pos(), Position::new(9, 0));
            assert_eq!(logging.oldest_wal_id(), 9);
        }
        ctx.sync()
            .expect("the runtime sync must work once every era writer exists");
    }
}
