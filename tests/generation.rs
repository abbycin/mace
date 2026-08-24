#![cfg(feature = "extra_check")]

//! phase b focused checks: caller-led sync generations on the shared durable
//! wal stream (new_sync_final.md §2/§3)

use mace::observe::{CounterMetric, HistogramMetric, InMemoryObserver};
use mace::testing::{self, WalRecordKind, WalSyncPoint};
use mace::{BucketOptions, Mace, OpCode, Options, RandomPath};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

fn counter(snap: &mace::observe::ObserveSnapshot, metric: CounterMetric) -> u64 {
    snap.counters
        .iter()
        .find(|(k, _)| *k == metric)
        .map(|(_, v)| *v)
        .unwrap_or(0)
}

fn histogram_max(snap: &mace::observe::ObserveSnapshot, metric: HistogramMetric) -> u64 {
    snap.histograms
        .iter()
        .find(|(k, _)| *k == metric)
        .map(|(_, s)| s.max)
        .unwrap_or(0)
}

struct HookReset;

impl Drop for HookReset {
    fn drop(&mut self) {
        testing::clear_hooks();
    }
}

/// hook-based tests share the process-global testing hooks, so they must run
/// one at a time (same pattern as tests/si.rs); routed through the single
/// global hooks lock so parallel HookReset drops cannot erase our slots
fn suite_lock() -> parking_lot::MutexGuard<'static, ()> {
    mace::testing::hooks_lock()
}

#[test]
fn concurrent_commits_coalesce_into_one_generation_with_single_sync() -> Result<(), OpCode> {
    let _guard = suite_lock();
    let observer = Arc::new(InMemoryObserver::new(8));
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.sync_on_write = true;
    opt.concurrent_write = 2;
    opt.observer = observer.clone();
    let mace = Mace::new(opt.validate()?)?;
    let db = mace.new_bucket("x", BucketOptions::default())?;

    let leader_entered = Arc::new(Barrier::new(2));
    let leader_release = Arc::new(Barrier::new(2));
    let first_leader_blocked = Arc::new(AtomicBool::new(false));
    let follower_registered = Arc::new(AtomicBool::new(false));
    let _reset = HookReset;
    testing::set_wal_sync_hook(Some(Arc::new({
        let leader_entered = leader_entered.clone();
        let leader_release = leader_release.clone();
        let first_leader_blocked = first_leader_blocked.clone();
        let follower_registered = follower_registered.clone();
        move |point| match point {
            WalSyncPoint::AfterLeaderRegisterBeforeSeal
                if !first_leader_blocked.swap(true, Ordering::AcqRel) =>
            {
                leader_entered.wait();
                leader_release.wait();
            }
            WalSyncPoint::AfterFollowerRegister => {
                follower_registered.store(true, Ordering::Release);
            }
            _ => {}
        }
    })));

    std::thread::scope(|s| -> Result<(), OpCode> {
        let t1 = s.spawn(|| -> Result<(), OpCode> {
            let tx = db.begin()?;
            tx.put("a", b"1")?;
            tx.commit()
        });
        let t2 = s.spawn(|| -> Result<(), OpCode> {
            let tx = db.begin()?;
            tx.put("b", b"2")?;
            tx.commit()
        });

        // wait until the first commit registered as the generation leader and
        // blocks before sealing
        leader_entered.wait();
        // wait until the second commit joined the same generation as follower
        while !follower_registered.load(Ordering::Acquire) {
            std::thread::yield_now();
        }
        leader_release.wait();
        t1.join().unwrap()?;
        t2.join().unwrap()?;
        Ok(())
    })?;

    let snap = observer.snapshot();
    assert_eq!(
        counter(&snap, CounterMetric::WalSync),
        1,
        "two concurrent commits must share one generation sync"
    );
    assert_eq!(counter(&snap, CounterMetric::WalGeneration), 1);
    assert_eq!(counter(&snap, CounterMetric::WalGenerationLeader), 1);
    assert_eq!(counter(&snap, CounterMetric::WalGenerationFollower), 1);
    assert_eq!(
        histogram_max(&snap, HistogramMetric::WalGenerationBatch),
        2,
        "generation batch must cover both commits"
    );

    let view = db.view()?;
    assert_eq!(view.get("a")?.slice(), b"1");
    assert_eq!(view.get("b")?.slice(), b"2");
    drop(view);
    Ok(())
}

#[test]
fn force_barrier_joins_inflight_generation_and_waits_for_its_own_target() -> Result<(), OpCode> {
    let _guard = suite_lock();
    let observer = Arc::new(InMemoryObserver::new(8));
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.sync_on_write = true;
    opt.concurrent_write = 1;
    opt.observer = observer.clone();
    let mace = Mace::new(opt.validate()?)?;
    let db = mace.new_bucket("x", BucketOptions::default())?;

    let leader_entered = Arc::new(Barrier::new(2));
    let leader_release = Arc::new(Barrier::new(2));
    let first_leader_blocked = Arc::new(AtomicBool::new(false));
    let barrier_joined = Arc::new(AtomicBool::new(false));
    let _reset = HookReset;
    testing::set_wal_sync_hook(Some(Arc::new({
        let leader_entered = leader_entered.clone();
        let leader_release = leader_release.clone();
        let first_leader_blocked = first_leader_blocked.clone();
        let barrier_joined = barrier_joined.clone();
        move |point| match point {
            WalSyncPoint::AfterLeaderRegisterBeforeSeal
                if !first_leader_blocked.swap(true, Ordering::AcqRel) =>
            {
                leader_entered.wait();
                leader_release.wait();
            }
            WalSyncPoint::AfterFollowerRegister => {
                barrier_joined.store(true, Ordering::Release);
            }
            _ => {}
        }
    })));

    std::thread::scope(|s| -> Result<(), OpCode> {
        let commit = s.spawn(|| -> Result<(), OpCode> {
            let tx = db.begin()?;
            tx.put("k", b"v")?;
            tx.commit()
        });
        let sync_result = Arc::new(std::sync::Mutex::new(None));
        leader_entered.wait();

        // the barrier registers its own target and joins the in-flight
        // generation as a follower instead of holding the shared logging lock
        let sync_result = sync_result.clone();
        let sync_thread = s.spawn({
            let sync_result = sync_result.clone();
            move || {
                *sync_result.lock().unwrap() = Some(mace.sync());
            }
        });
        while !barrier_joined.load(Ordering::Acquire) {
            std::thread::yield_now();
        }
        leader_release.wait();
        commit.join().unwrap()?;
        sync_thread.join().unwrap();
        assert_eq!(
            *sync_result.lock().unwrap(),
            Some(Ok(())),
            "force barrier must complete through the joined generation"
        );
        Ok(())
    })?;

    let snap = observer.snapshot();
    assert_eq!(
        counter(&snap, CounterMetric::WalSync),
        1,
        "barrier + commit must share one generation sync"
    );
    assert_eq!(counter(&snap, CounterMetric::WalGeneration), 1);
    Ok(())
}

#[test]
fn checkpoint_publish_runs_durable_barrier_without_private_syncs() -> Result<(), OpCode> {
    let _guard = suite_lock();
    let observer = Arc::new(InMemoryObserver::new(16));
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.sync_on_write = true;
    opt.concurrent_write = 1;
    opt.observer = observer.clone();
    let mace = Mace::new(opt.validate()?)?;
    let db = mace.new_bucket("x", BucketOptions::default())?;

    // committed data advances the frontier so the durable checkpoint observer
    // runs its barrier through the shared stream
    let tx = db.begin()?;
    tx.put("k", b"v")?;
    tx.commit()?;
    testing::checkpoint_and_wait(&db);

    // every physical sync in durable mode is a generation sync: the commit,
    // the checkpoint barrier and the force barrier at shutdown must never
    // perform a private fsync
    let snap = observer.snapshot();
    assert_eq!(
        counter(&snap, CounterMetric::WalSync),
        counter(&snap, CounterMetric::WalGeneration),
        "durable mode must only sync through generations"
    );
    assert_eq!(counter(&snap, CounterMetric::WalGenerationError), 0);
    assert!(counter(&snap, CounterMetric::WalGeneration) > 0);

    // the checkpoint frontier and the wal hint must survive reopen
    drop(db);
    drop(mace);
    let mut reopen = Options::new(&*path);
    reopen.sync_on_write = true;
    reopen.concurrent_write = 1;
    let mace = Mace::new(reopen.validate()?)?;
    let db = mace.get_bucket("x").expect("bucket must reopen");
    let view = db.view()?;
    assert_eq!(view.get("k")?.slice(), b"v");
    Ok(())
}

#[test]
fn empty_force_barrier_short_circuits_without_generation() -> Result<(), OpCode> {
    let _guard = suite_lock();
    let observer = Arc::new(InMemoryObserver::new(8));
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.sync_on_write = true;
    opt.concurrent_write = 1;
    opt.observer = observer.clone();
    let log_root = opt.log_root();
    let mace = Mace::new(opt.validate()?)?;
    let db = mace.new_bucket("x", BucketOptions::default())?;

    let tx = db.begin()?;
    tx.put("k", b"v")?;
    tx.commit()?;
    let snap = observer.snapshot();
    assert_eq!(counter(&snap, CounterMetric::WalSync), 1);
    assert_eq!(counter(&snap, CounterMetric::WalGeneration), 1);

    // a force barrier with nothing new appended must return a completed ticket
    // without creating a generation, electing a leader or syncing
    mace.sync()?;
    mace.sync()?;
    let snap = observer.snapshot();
    assert_eq!(
        counter(&snap, CounterMetric::WalSync),
        1,
        "empty force barrier must not sync"
    );
    assert_eq!(
        counter(&snap, CounterMetric::WalGeneration),
        1,
        "empty force barrier must not create a generation"
    );

    // a fresh commit starts the next generation
    let tx = db.begin()?;
    tx.put("k2", b"v2")?;
    let txid = testing::txn_start_ts(&tx);
    tx.commit()?;
    let snap = observer.snapshot();
    assert_eq!(counter(&snap, CounterMetric::WalSync), 2);
    assert_eq!(counter(&snap, CounterMetric::WalGeneration), 2);

    // after the commit the cut is durable at the commit record end
    let durable = testing::wal_durable_pos(&db, 0);
    let probes = testing::wal_record_probes(&log_root, 1);
    let commit = probes
        .iter()
        .find(|p| p.kind == WalRecordKind::Commit && p.txid == txid)
        .expect("commit record must exist");
    let cut = (commit.file_id, commit.offset + commit.len as u64);
    assert!(
        durable >= cut,
        "durable_pos {durable:?} must cover the last commit cut {cut:?}"
    );
    Ok(())
}

#[test]
fn modified_abort_joins_generation_and_handles_pin_under_logging_lock() -> Result<(), OpCode> {
    let _guard = suite_lock();
    let observer = Arc::new(InMemoryObserver::new(8));
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.sync_on_write = true;
    opt.concurrent_write = 1;
    opt.observer = observer.clone();
    let mace = Mace::new(opt.validate()?)?;
    let db = mace.new_bucket("x", BucketOptions::default())?;

    // baseline commit so the abort has a wal chain to clean
    let tx = db.begin()?;
    tx.put("k", b"v0")?;
    tx.commit()?;

    // block the abort's fact -> abort-clean enqueue handoff (both inside the
    // re-acquired shared logging critical section)
    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let _reset = HookReset;
    testing::set_txn_abort_hook(Some(Arc::new({
        let entered = entered.clone();
        let release = release.clone();
        move |point, _txid| {
            if point == testing::TxnAbortSyncPoint::AfterAbortFactBeforeAbortCleanEnqueue {
                entered.wait();
                release.wait();
            }
        }
    })));

    let (txid_tx, txid_rx) = std::sync::mpsc::channel();
    std::thread::scope(|s| -> Result<(), OpCode> {
        let db_for_abort = db.clone();
        let handle = s.spawn(move || -> Result<(), OpCode> {
            let tx = db_for_abort.begin()?;
            txid_tx.send(testing::txn_start_ts(&tx)).unwrap();
            tx.update("k", b"v1")?;
            drop(tx); // modified abort through the generation protocol
            Ok(())
        });
        txid_rx.recv().unwrap();
        entered.wait();
        // the abort is between abort_fact and enqueue while holding the shared
        // logging lock: gc's boundary computation (same lock) cannot observe a
        // pinless window
        assert!(
            !testing::try_lock_wal_logging(&db),
            "abort handoff must hold the shared logging lock so gc cannot interleave"
        );
        release.wait();
        handle.join().unwrap()?;
        Ok(())
    })?;

    // after the handoff the lock is free again and the abort was rolled back
    assert!(testing::try_lock_wal_logging(&db));
    let view = db.view()?;
    assert_eq!(view.get("k")?.slice(), b"v0");
    drop(view);

    // the modified abort must have gone through the generation protocol:
    // baseline commit (generation 1) + abort (generation 2), each with one sync
    let snap = observer.snapshot();
    assert_eq!(
        counter(&snap, CounterMetric::WalGeneration),
        2,
        "baseline commit plus the modified abort must each form a generation"
    );
    assert_eq!(
        counter(&snap, CounterMetric::WalSync),
        2,
        "the modified abort must sync through a generation, never a private fsync"
    );
    Ok(())
}

#[test]
fn unmodified_abort_stays_record_only_without_generation() -> Result<(), OpCode> {
    let _guard = suite_lock();
    let observer = Arc::new(InMemoryObserver::new(8));
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.sync_on_write = true;
    opt.concurrent_write = 1;
    opt.observer = observer.clone();
    let mace = Mace::new(opt.validate()?)?;
    let db = mace.new_bucket("x", BucketOptions::default())?;

    // an unmodified abort (no update chain) must stay record-only: no
    // generation, no sync, no wait
    let tx = db.begin()?;
    drop(tx);
    let snap = observer.snapshot();
    assert_eq!(counter(&snap, CounterMetric::WalGeneration), 0);
    assert_eq!(counter(&snap, CounterMetric::WalSync), 0);

    // a modified abort does sync through a generation
    let tx = db.begin()?;
    tx.put("k", b"v")?;
    drop(tx);
    let snap = observer.snapshot();
    assert_eq!(counter(&snap, CounterMetric::WalGeneration), 1);
    assert_eq!(counter(&snap, CounterMetric::WalSync), 1);
    Ok(())
}

#[test]
fn generation_sync_failure_broadcasts_and_isolates_next_generation() -> Result<(), OpCode> {
    let _guard = suite_lock();
    let observer = Arc::new(InMemoryObserver::new(8));
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.sync_on_write = true;
    opt.concurrent_write = 2;
    opt.observer = observer.clone();
    let mace = Mace::new(opt.validate()?)?;
    let db = mace.new_bucket("x", BucketOptions::default())?;

    let leader_entered = Arc::new(Barrier::new(2));
    let leader_release = Arc::new(Barrier::new(2));
    let first_leader_blocked = Arc::new(AtomicBool::new(false));
    let follower_registered = Arc::new(AtomicBool::new(false));
    let _reset = HookReset;
    testing::set_wal_sync_hook(Some(Arc::new({
        let leader_entered = leader_entered.clone();
        let leader_release = leader_release.clone();
        let first_leader_blocked = first_leader_blocked.clone();
        let follower_registered = follower_registered.clone();
        move |point| match point {
            WalSyncPoint::AfterLeaderRegisterBeforeSeal
                if !first_leader_blocked.swap(true, Ordering::AcqRel) =>
            {
                leader_entered.wait();
                leader_release.wait();
            }
            WalSyncPoint::AfterFollowerRegister => {
                follower_registered.store(true, Ordering::Release);
            }
            _ => {}
        }
    })));
    // the first generation's physical sync fails once
    testing::fail_next_wal_syncs(&db, 1);

    let (r1, r2) = std::thread::scope(|s| {
        let t1 = s.spawn(|| -> Result<(), OpCode> {
            let tx = db.begin()?;
            tx.put("a", b"1")?;
            tx.commit()
        });
        let t2 = s.spawn(|| -> Result<(), OpCode> {
            let tx = db.begin()?;
            tx.put("b", b"2")?;
            tx.commit()
        });
        leader_entered.wait();
        while !follower_registered.load(Ordering::Acquire) {
            std::thread::yield_now();
        }
        leader_release.wait();
        (t1.join().unwrap(), t2.join().unwrap())
    });
    assert_eq!(
        r1,
        Err(OpCode::IoError),
        "leader must surface the generation sync error"
    );
    assert_eq!(
        r2,
        Err(OpCode::IoError),
        "follower must receive the same generation sync error"
    );

    // no committed fact: both txns were dropped and aborted
    let view = db.view()?;
    assert!(matches!(view.get("a"), Err(OpCode::NotFound)));
    assert!(matches!(view.get("b"), Err(OpCode::NotFound)));
    drop(view);
    assert_eq!(
        counter(&observer.snapshot(), CounterMetric::WalGenerationError),
        1,
        "exactly one generation must record a sync error"
    );

    // the next generation must not inherit the previous error
    let tx = db.begin()?;
    tx.put("c", b"3")?;
    tx.commit()?;
    let view = db.view()?;
    assert_eq!(view.get("c")?.slice(), b"3");
    drop(view);
    Ok(())
}

#[test]
fn relaxed_commits_never_touch_generation_state() -> Result<(), OpCode> {
    let _guard = suite_lock();
    let observer = Arc::new(InMemoryObserver::new(8));
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.sync_on_write = false;
    opt.concurrent_write = 2;
    opt.observer = observer.clone();
    let mace = Mace::new(opt.validate()?)?;
    let db = mace.new_bucket("x", BucketOptions::default())?;

    let tx1 = db.begin()?;
    tx1.put("a", b"1")?;
    tx1.commit()?;
    let tx2 = db.begin()?;
    tx2.put("b", b"2")?;
    tx2.commit()?;

    // relaxed commits only flush: no generation, no waiter, no wal sync
    let snap = observer.snapshot();
    assert_eq!(counter(&snap, CounterMetric::WalGeneration), 0);
    assert_eq!(counter(&snap, CounterMetric::WalGenerationLeader), 0);
    assert_eq!(counter(&snap, CounterMetric::WalGenerationFollower), 0);
    assert_eq!(counter(&snap, CounterMetric::WalSync), 0);

    let view = db.view()?;
    assert_eq!(view.get("a")?.slice(), b"1");
    assert_eq!(view.get("b")?.slice(), b"2");
    drop(view);
    Ok(())
}

#[test]
fn rotation_writers_are_synced_by_generation_and_survive_reopen() -> Result<(), OpCode> {
    let _guard = suite_lock();
    let observer = Arc::new(InMemoryObserver::new(16));
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.sync_on_write = true;
    opt.concurrent_write = 2;
    opt.wal_file_size = 4 << 10;
    opt.wal_buffer_size = 8 << 10;
    opt.observer = observer.clone();
    let mace = Mace::new(opt.validate()?)?;
    let db = mace.new_bucket("x", BucketOptions::default())?;

    // payloads crossing wal_file_size force rotation; detached writers must be
    // synced by the generation leader, never by a private fsync
    let payload = vec![b'x'; 3 << 10];
    for round in 0..3usize {
        std::thread::scope(|s| {
            for i in 0..4usize {
                let db = db.clone();
                let payload = payload.clone();
                s.spawn(move || -> Result<(), OpCode> {
                    let key = format!("r{round}-{i}");
                    let tx = db.begin()?;
                    tx.put(key.as_bytes(), &payload)?;
                    tx.commit()
                });
            }
        });
    }

    let snap = observer.snapshot();
    assert_eq!(
        counter(&snap, CounterMetric::WalSync),
        counter(&snap, CounterMetric::WalGeneration),
        "every physical sync must be a generation sync, rotation must not fsync by itself"
    );
    assert_eq!(counter(&snap, CounterMetric::WalGenerationError), 0);

    drop(db);
    drop(mace);

    let mut reopen = Options::new(&*path);
    reopen.sync_on_write = true;
    reopen.concurrent_write = 2;
    reopen.wal_file_size = 4 << 10;
    reopen.wal_buffer_size = 8 << 10;
    let mace = Mace::new(reopen.validate()?)?;
    let db = mace.get_bucket("x").expect("bucket must reopen");
    let view = db.view()?;
    for round in 0..3usize {
        for i in 0..4usize {
            let key = format!("r{round}-{i}");
            assert_eq!(
                view.get(key.as_bytes())?.slice(),
                payload.as_slice(),
                "committed value must survive rotation and reopen"
            );
        }
    }
    Ok(())
}

#[test]
fn concurrent_commits_with_sync_and_shutdown_terminate() -> Result<(), OpCode> {
    let _guard = suite_lock();
    let observer = Arc::new(InMemoryObserver::new(64));
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.sync_on_write = true;
    opt.concurrent_write = 4;
    opt.wal_file_size = 8 << 10;
    opt.wal_buffer_size = 16 << 10;
    opt.observer = observer.clone();
    let mace = Mace::new(opt.validate()?)?;
    let db = mace.new_bucket("x", BucketOptions::default())?;

    let mut handles = Vec::new();
    for t in 0..6usize {
        let db = db.clone();
        handles.push(std::thread::spawn(move || {
            let mut committed = 0usize;
            for i in 0..200usize {
                let Ok(tx) = db.begin() else {
                    break; // admission closed by shutdown
                };
                let key = format!("k{t}-{i}");
                let Ok(()) = tx.put(key.as_bytes(), b"v") else {
                    break;
                };
                match tx.commit() {
                    Ok(()) => committed += 1,
                    Err(OpCode::Invalid) => break,
                    Err(e) => panic!("unexpected commit error during shutdown: {e:?}"),
                }
            }
            committed
        }));
    }

    // let writers form generations, then force barriers while they run
    std::thread::sleep(Duration::from_millis(30));
    for _ in 0..4 {
        let _ = mace.sync();
        std::thread::sleep(Duration::from_millis(5));
    }
    drop(db);
    drop(mace);

    let committed: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
    assert!(
        committed > 0,
        "some commits must land before shutdown (no deadlock, no lost commit)"
    );
    Ok(())
}

#[test]
fn sync_merge_window_option_is_honored_without_breaking_durability() -> Result<(), OpCode> {
    let _guard = suite_lock();
    // window 0 disables the merge window (natural contention only); a large
    // window must still complete a solo commit quickly via the quiet exit
    for window_us in [0u64, 10_000u64] {
        let path = RandomPath::tmp();
        let mut opt = Options::new(&*path);
        opt.sync_on_write = true;
        opt.concurrent_write = 1;
        opt.sync_merge_window_us = window_us;
        let mace = Mace::new(opt.validate()?)?;
        let db = mace.new_bucket("x", BucketOptions::default())?;
        let tx = db.begin()?;
        tx.put("k", b"v")?;
        tx.commit()?;
        drop(db);
        drop(mace);

        let mut reopen = Options::new(&*path);
        reopen.sync_on_write = true;
        reopen.concurrent_write = 1;
        reopen.sync_merge_window_us = window_us;
        let mace = Mace::new(reopen.validate()?)?;
        let db = mace.get_bucket("x").expect("bucket must reopen");
        let view = db.view()?;
        assert_eq!(view.get("k")?.slice(), b"v");
    }
    Ok(())
}

#[test]
fn leader_panic_orphans_generation_fails_follower_and_state_resets() -> Result<(), OpCode> {
    let _guard = suite_lock();
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.sync_on_write = true;
    opt.concurrent_write = 2;
    let mace = Mace::new(opt.validate()?)?;
    let db = mace.new_bucket("x", BucketOptions::default())?;

    // seed a commit so durable_pos is established and the next commit is
    // guaranteed to register a fresh generation
    {
        let tx = db.begin()?;
        tx.put("seed", b"0")?;
        tx.commit()?;
    }

    let leader_entered = Arc::new(Barrier::new(2));
    let leader_release = Arc::new(Barrier::new(2));
    let first_leader_blocked = Arc::new(AtomicBool::new(false));
    let follower_registered = Arc::new(AtomicBool::new(false));
    let will_panic = Arc::new(AtomicBool::new(false));
    let _reset = HookReset;
    testing::set_wal_sync_hook(Some(Arc::new({
        let leader_entered = leader_entered.clone();
        let leader_release = leader_release.clone();
        let first_leader_blocked = first_leader_blocked.clone();
        let follower_registered = follower_registered.clone();
        let will_panic = will_panic.clone();
        move |point| match point {
            // hold the leader between register and seal so a follower can join
            // the in-flight generation
            WalSyncPoint::AfterLeaderRegisterBeforeSeal
                if !first_leader_blocked.swap(true, Ordering::AcqRel) =>
            {
                leader_entered.wait();
                leader_release.wait();
            }
            WalSyncPoint::AfterFollowerRegister => {
                follower_registered.store(true, Ordering::Release);
            }
            // the leader panics after sealing but before the physical sync,
            // leaving its SyncTicket unpublished: the ticket's Drop must fail
            // the orphaned generation and wake the waiting follower
            WalSyncPoint::AfterSealBeforeFileSync if will_panic.swap(false, Ordering::AcqRel) => {
                panic!("injected leader panic before generation sync");
            }
            _ => {}
        }
    })));

    let (leader_outcome, follower_result) = std::thread::scope(|s| {
        let leader = s.spawn(|| {
            let tx = db.begin().expect("leader begin");
            tx.put("leader", b"1").expect("leader put");
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| tx.commit()))
        });
        // wait for the leader to register and block before sealing
        leader_entered.wait();
        let follower = s.spawn(|| -> Result<(), OpCode> {
            let tx = db.begin()?;
            tx.put("follower", b"2")?;
            tx.commit()
        });
        // wait until the follower joined the in-flight generation, then let
        // the leader proceed to the seal hook where it panics
        while !follower_registered.load(Ordering::Acquire) {
            std::thread::yield_now();
        }
        will_panic.store(true, Ordering::Release);
        leader_release.wait();
        (leader.join().unwrap(), follower.join().unwrap())
    });

    assert!(
        leader_outcome.is_err(),
        "the injected hook panic must unwind through the leader commit"
    );
    // the follower must wake with the orphaned generation's failure instead of
    // hanging forever (design invariant 10)
    assert_eq!(
        follower_result,
        Err(OpCode::IoError),
        "follower must observe the orphaned generation's failure"
    );

    // no committed fact from the panicked leader or the failed follower
    let view = db.view()?;
    assert!(matches!(view.get("leader"), Err(OpCode::NotFound)));
    assert!(matches!(view.get("follower"), Err(OpCode::NotFound)));
    drop(view);

    // the sync state was reset: the next commit drives a fresh generation and
    // succeeds
    let tx = db.begin()?;
    tx.put("after", b"3")?;
    tx.commit()?;
    let view = db.view()?;
    assert_eq!(view.get("after")?.slice(), b"3");
    assert!(matches!(view.get("leader"), Err(OpCode::NotFound)));
    drop(view);
    Ok(())
}

#[test]
fn force_barrier_surfaces_generation_sync_error_and_next_generation_is_isolated()
-> Result<(), OpCode> {
    let _guard = suite_lock();
    let observer = Arc::new(InMemoryObserver::new(8));
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.sync_on_write = true;
    opt.concurrent_write = 1;
    opt.observer = observer.clone();
    let mace = Mace::new(opt.validate()?)?;
    let db = mace.new_bucket("x", BucketOptions::default())?;

    // seed so the barrier has a durable baseline
    {
        let tx = db.begin()?;
        tx.put("seed", b"0")?;
        tx.commit()?;
    }

    let leader_entered = Arc::new(Barrier::new(2));
    let leader_release = Arc::new(Barrier::new(2));
    let first_leader_blocked = Arc::new(AtomicBool::new(false));
    let barrier_joined = Arc::new(AtomicBool::new(false));
    let _reset = HookReset;
    testing::set_wal_sync_hook(Some(Arc::new({
        let leader_entered = leader_entered.clone();
        let leader_release = leader_release.clone();
        let first_leader_blocked = first_leader_blocked.clone();
        let barrier_joined = barrier_joined.clone();
        move |point| match point {
            WalSyncPoint::AfterLeaderRegisterBeforeSeal
                if !first_leader_blocked.swap(true, Ordering::AcqRel) =>
            {
                leader_entered.wait();
                leader_release.wait();
            }
            WalSyncPoint::AfterFollowerRegister => {
                barrier_joined.store(true, Ordering::Release);
            }
            _ => {}
        }
    })));

    let (barrier_tx, barrier_rx) = std::sync::mpsc::channel();
    let (commit_result, barrier_outcome) = std::thread::scope(|s| {
        let commit = s.spawn(|| -> Result<(), OpCode> {
            let tx = db.begin()?;
            tx.put("k", b"v")?;
            tx.commit()
        });
        // wait for the commit to register as the generation leader
        leader_entered.wait();

        // the force barrier registers its own target and joins the in-flight
        // generation as a follower instead of holding the shared logging lock
        let sync_thread = s.spawn(move || {
            let _ = barrier_tx.send(mace.sync());
        });
        while !barrier_joined.load(Ordering::Acquire) {
            std::thread::yield_now();
        }
        // the barrier is a follower of the blocked leader's generation; arm
        // the fault before the leader is released so its physical sync fails
        testing::fail_next_wal_syncs(&db, 1);
        leader_release.wait();
        let commit_result = commit.join().unwrap();
        sync_thread.join().unwrap();
        let barrier_outcome = barrier_rx.recv().expect("barrier must report");
        (commit_result, barrier_outcome)
    });

    assert_eq!(
        commit_result,
        Err(OpCode::IoError),
        "the generation leader commit must surface the sync error"
    );
    assert_eq!(
        barrier_outcome,
        Err(OpCode::IoError),
        "the force barrier must surface the generation sync error"
    );
    let view = db.view()?;
    assert!(
        matches!(view.get("k"), Err(OpCode::NotFound)),
        "the failed commit must publish no fact"
    );
    drop(view);
    assert_eq!(
        counter(&observer.snapshot(), CounterMetric::WalGenerationError),
        1,
        "exactly one generation must record a sync error"
    );

    // the next generation must not inherit the barrier's error
    let tx = db.begin()?;
    tx.put("after", b"3")?;
    tx.commit()?;
    let view = db.view()?;
    assert_eq!(view.get("after")?.slice(), b"3");
    drop(view);
    Ok(())
}
