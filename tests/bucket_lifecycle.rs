#![cfg(feature = "extra_check")]

//! bucket lifecycle side effects that need the process-global checkpoint hook.
//!
//! the hook is not scoped to one engine (it only observes a bucket id), so these cases live in
//! their own test binary: any other engine in the same process could otherwise intercept the
//! pause or the published-result witness.

mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mace::testing::{self, BucketDeleteSyncPoint, CheckpointSyncPoint};
use mace::{BucketOptions, Mace, OpCode, Options, RandomPath};

struct HookReset;

impl Drop for HookReset {
    fn drop(&mut self) {
        testing::clear_checkpoint_hook();
        testing::clear_bucket_delete_hook();
    }
}

/// a checkpoint round queued before the delete must not publish after it
///
/// the queued task is held right before its flush directive is resolved; the delete starts while
/// it is held, and once the discard mode is recorded the released task sees `Skip` and no
/// checkpoint result is ever synchronized.
#[test]
fn bucket_delete_discards_sealed_batch() -> Result<(), OpCode> {
    let _lock = testing::checkpoint_test_lock();
    let _reset = HookReset;

    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.sync_on_write = true;
    opt.gc_timeout = 60_000;
    let mace = Mace::new(opt.validate()?)?;
    mace.disable_gc();

    let bucket = mace.new_bucket("discard", BucketOptions::default())?;
    let bucket_id = bucket.id();
    let txn = bucket.begin()?;
    for idx in 0..64 {
        txn.put(format!("key_{idx:04}"), vec![b'v'; 256])?;
    }
    txn.commit()?;

    let published = Arc::new(AtomicBool::new(false));
    let paused = Arc::new(AtomicBool::new(false));
    let (queued_tx, queued_rx) = channel();
    let (release_tx, release_rx) = channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    testing::set_checkpoint_hook(Some(Arc::new({
        let published = published.clone();
        let paused = paused.clone();
        move |point, observed_bucket_id| {
            if observed_bucket_id != bucket_id {
                return;
            }
            match point {
                CheckpointSyncPoint::BeforeFlushDirective => {
                    if !paused.swap(true, Ordering::SeqCst) {
                        queued_tx.send(()).expect("signal queued flush task");
                        release_rx
                            .lock()
                            .expect("lock release receiver")
                            .recv_timeout(Duration::from_secs(10))
                            .expect("release queued flush task");
                    }
                }
                CheckpointSyncPoint::AfterResultSync => published.store(true, Ordering::SeqCst),
                _ => {}
            }
        }
    })));

    // leave one checkpoint round queued in front of the directive
    let queued = bucket.clone();
    let enqueue = std::thread::spawn(move || queued.checkpoint());
    queued_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the queued checkpoint must reach the directive");
    enqueue.join().expect("checkpoint enqueue must return");
    drop(bucket);

    let (unpublished_tx, unpublished_rx) = channel();
    testing::set_bucket_delete_hook(Some(Arc::new(move |point, _| {
        if point == BucketDeleteSyncPoint::AfterDiscardModeRecorded {
            let _ = unpublished_tx.send(());
        }
    })));

    let delete_mace = mace.clone();
    let (result_tx, result_rx) = channel();
    let delete = std::thread::spawn(move || {
        let result = delete_mace.del_bucket("discard");
        result_tx.send(result).expect("signal delete result");
    });
    // the discard mode is recorded before the delete waits for the queued round: that is the
    // release point
    unpublished_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the delete must record the discard mode before waiting for the round");
    release_tx.send(()).expect("release queued flush task");
    result_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("the delete must return")?;
    delete.join().expect("delete thread must finish");

    assert!(
        !published.load(Ordering::SeqCst),
        "a logical delete must discard the sealed batch instead of publishing it"
    );
    assert!(
        !mace.active_buckets().iter().any(|name| name == "discard"),
        "a logically deleted bucket must leave the active names"
    );

    drop(mace);
    let reopened = Mace::new(Options::new(&*path).validate()?)?;
    assert_eq!(
        reopened.del_bucket("discard"),
        Err(OpCode::NotFound),
        "the logical delete must be durable"
    );
    let recreated = reopened.new_bucket("discard", BucketOptions::default())?;
    assert_ne!(
        recreated.id(),
        bucket_id,
        "a deleted bucket id is never reused"
    );
    Ok(())
}

/// a same-name create that overlaps a delete must not lose its durable record
///
/// the delete waits for a queued checkpoint round with the name still published, so a create
/// that starts inside that window must be refused. Before the fix the name was already removed
/// from the catalog at that point, the create committed, and the delete's `del` erased the new
/// record: the next open aborted on the orphaned stat row.
#[test]
fn bucket_create_overlapping_delete_keeps_its_record() -> Result<(), OpCode> {
    let _lock = testing::checkpoint_test_lock();
    let _reset = HookReset;

    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.sync_on_write = true;
    opt.gc_timeout = 60_000;
    let mace = Mace::new(opt.validate()?)?;
    mace.disable_gc();

    let bucket = mace.new_bucket("race", BucketOptions::default())?;
    let bucket_id = bucket.id();
    let txn = bucket.begin()?;
    for idx in 0..64 {
        txn.put(format!("key_{idx:04}"), vec![b'v'; 256])?;
    }
    txn.commit()?;

    let paused = Arc::new(AtomicBool::new(false));
    let (queued_tx, queued_rx) = channel();
    let (release_tx, release_rx) = channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    testing::set_checkpoint_hook(Some(Arc::new({
        let paused = paused.clone();
        move |point, observed| {
            if observed != bucket_id || point != CheckpointSyncPoint::BeforeFlushDirective {
                return;
            }
            if !paused.swap(true, Ordering::SeqCst) {
                queued_tx.send(()).expect("signal queued flush task");
                release_rx
                    .lock()
                    .expect("lock release receiver")
                    .recv_timeout(Duration::from_secs(10))
                    .expect("release queued flush task");
            }
        }
    })));

    let queued = bucket.clone();
    let enqueue = std::thread::spawn(move || queued.checkpoint());
    queued_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the queued checkpoint must reach the directive");
    enqueue.join().expect("checkpoint enqueue must return");
    drop(bucket);

    let (unpublished_tx, unpublished_rx) = channel();
    testing::set_bucket_delete_hook(Some(Arc::new(move |point, _| {
        if point == BucketDeleteSyncPoint::AfterDiscardModeRecorded {
            let _ = unpublished_tx.send(());
        }
    })));

    let delete_mace = mace.clone();
    let (delete_tx, delete_rx) = channel();
    let delete = std::thread::spawn(move || {
        let result = delete_mace.del_bucket("race");
        delete_tx.send(result).expect("signal delete result");
    });
    unpublished_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the delete must record the discard mode before waiting for the round");

    // the delete waits for the paused round with the name still published, so a create that
    // starts now must be refused instead of writing the record the delete will erase
    let create_mace = mace.clone();
    let (create_tx, create_rx) = channel();
    let create = std::thread::spawn(move || {
        let result = create_mace.new_bucket("race", BucketOptions::default());
        create_tx
            .send(result.map(|_| ()))
            .expect("signal create result");
    });
    let create_result = create_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("the create must return");
    assert_eq!(
        create_result,
        Err(OpCode::Exist),
        "a create that overlaps a delete must not commit a record the delete will erase"
    );
    create.join().expect("create thread must finish");
    release_tx.send(()).expect("release queued flush task");

    delete_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("the delete must return")?;
    delete.join().expect("delete thread must finish");

    // the name is free once the delete completed
    {
        let recreated = mace.new_bucket("race", BucketOptions::default())?;
        recreated.checkpoint_and_wait();
    }

    drop(mace);
    let reopened = Mace::new(Options::new(&*path).validate()?)?;
    assert!(
        reopened.open_bucket("race").is_ok(),
        "a bucket created after the delete completed must reopen"
    );
    assert!(
        reopened.active_buckets().iter().any(|name| name == "race"),
        "the recreated bucket must be listed after reopen"
    );
    Ok(())
}

/// an option update during a manual unload keeps the documented `Again` answer
///
/// the unload keeps the bucket in the `Dropping` lifecycle while its final checkpoint runs;
/// the update must report the bucket as busy, not as missing
#[test]
fn update_bucket_opt_during_unload_returns_again() -> Result<(), OpCode> {
    let _lock = testing::checkpoint_test_lock();
    let _reset = HookReset;

    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.sync_on_write = true;
    opt.gc_timeout = 60_000;
    let mace = Mace::new(opt.validate()?)?;
    mace.disable_gc();

    let bucket_id;
    {
        let bucket = mace.new_bucket("unload", BucketOptions::default())?;
        bucket_id = bucket.id();
        let txn = bucket.begin()?;
        for idx in 0..32 {
            txn.put(format!("key_{idx:04}"), vec![b'v'; 256])?;
        }
        txn.commit()?;
    }

    let paused = Arc::new(AtomicBool::new(false));
    let (paused_tx, paused_rx) = channel();
    let (release_tx, release_rx) = channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    testing::set_checkpoint_hook(Some(Arc::new({
        let paused = paused.clone();
        move |point, observed| {
            if observed != bucket_id || point != CheckpointSyncPoint::BeforeFlushDirective {
                return;
            }
            if !paused.swap(true, Ordering::SeqCst) {
                paused_tx.send(()).expect("signal paused unload checkpoint");
                release_rx
                    .lock()
                    .expect("lock release receiver")
                    .recv_timeout(Duration::from_secs(10))
                    .expect("release unload checkpoint");
            }
        }
    })));

    let unload_mace = mace.clone();
    let (unload_tx, unload_rx) = channel();
    let unload = std::thread::spawn(move || {
        let result = unload_mace.drop_bucket("unload");
        unload_tx.send(result).expect("signal unload result");
    });
    paused_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the unload must reach its final checkpoint");

    assert_eq!(
        mace.update_bucket_opt("unload", BucketOptions::default()),
        Err(OpCode::Again),
        "a dropping bucket is busy, not missing"
    );

    release_tx.send(()).expect("release unload checkpoint");
    unload_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("the unload must return")?;
    unload.join().expect("unload thread must finish");

    // once unloaded, the update is accepted again
    mace.update_bucket_opt("unload", BucketOptions::default())?;
    Ok(())
}

/// a same-name create must not commit while the delete holds the commit lock
///
/// the sync point fires after the delete unpublished the name and before the metadata
/// transaction commits; the create is blocked on the structural lock until that commit
/// returns, so it cannot write a record the delete would erase
#[test]
fn bucket_create_is_blocked_until_delete_commit() -> Result<(), OpCode> {
    let _lock = testing::checkpoint_test_lock();
    let _reset = HookReset;

    let path = RandomPath::tmp();
    let mace = Mace::new(Options::new(&*path).validate()?)?;
    {
        let bucket = mace.new_bucket("blocked", BucketOptions::default())?;
        let txn = bucket.begin()?;
        txn.put("k", vec![1u8; 8])?;
        txn.commit()?;
    }

    let (paused_tx, paused_rx) = channel();
    let (release_tx, release_rx) = channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    testing::set_bucket_delete_hook(Some(Arc::new(move |point, _| {
        if point == BucketDeleteSyncPoint::AfterUnpublishBeforeCommit {
            paused_tx.send(()).expect("signal unpublish");
            release_rx
                .lock()
                .expect("lock release receiver")
                .recv_timeout(Duration::from_secs(10))
                .expect("release delete commit");
        }
    })));

    let delete_mace = mace.clone();
    let (delete_tx, delete_rx) = channel();
    let delete = std::thread::spawn(move || {
        let result = delete_mace.del_bucket("blocked");
        delete_tx.send(result).expect("signal delete result");
    });
    paused_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the delete must unpublish before its commit");

    let create_mace = mace.clone();
    let (started_tx, started_rx) = channel();
    let (create_tx, create_rx) = channel();
    let create = std::thread::spawn(move || {
        started_tx.send(()).expect("signal create start");
        let result = create_mace.new_bucket("blocked", BucketOptions::default());
        create_tx
            .send(result.map(|_| ()))
            .expect("signal create result");
    });
    started_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the create probe must start");
    assert!(
        create_rx.recv_timeout(Duration::from_millis(200)).is_err(),
        "a same-name create must not commit while the delete holds the commit lock"
    );

    release_tx.send(()).expect("release delete commit");
    delete_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("the delete must return")?;
    delete.join().expect("delete thread must finish");
    create_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("the create must return")?;
    create.join().expect("create thread must finish");

    drop(mace);
    let reopened = Mace::new(Options::new(&*path).validate()?)?;
    assert!(
        reopened.open_bucket("blocked").is_ok(),
        "the bucket created after the delete commit must reopen"
    );
    Ok(())
}

/// an option update during a delete is rejected once the logical unpublish starts
///
/// the delete has dropped the sealed batch and removed the context but has not committed;
/// the name is still published, so only the lifecycle guard reports the bucket as gone
#[test]
fn update_bucket_opt_during_delete_returns_not_found() -> Result<(), OpCode> {
    let _lock = testing::checkpoint_test_lock();
    let _reset = HookReset;

    let path = RandomPath::tmp();
    let mace = Mace::new(Options::new(&*path).validate()?)?;
    {
        let bucket = mace.new_bucket("updating", BucketOptions::default())?;
        let txn = bucket.begin()?;
        txn.put("k", vec![1u8; 8])?;
        txn.commit()?;
    }

    let (paused_tx, paused_rx) = channel();
    let (release_tx, release_rx) = channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    testing::set_bucket_delete_hook(Some(Arc::new(move |point, _| {
        if point == BucketDeleteSyncPoint::AfterDiscardBeforeCommit {
            paused_tx.send(()).expect("signal reclaimed delete");
            release_rx
                .lock()
                .expect("lock release receiver")
                .recv_timeout(Duration::from_secs(10))
                .expect("release delete commit");
        }
    })));

    let delete_mace = mace.clone();
    let (delete_tx, delete_rx) = channel();
    let delete = std::thread::spawn(move || {
        let result = delete_mace.del_bucket("updating");
        delete_tx.send(result).expect("signal delete result");
    });
    paused_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the delete must drop the batch before the commit");

    assert_eq!(
        mace.update_bucket_opt("updating", BucketOptions::default()),
        Err(OpCode::NotFound),
        "a deleting bucket must not accept an option update its own commit erases"
    );

    release_tx.send(()).expect("release delete commit");
    delete_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("the delete must return")?;
    delete.join().expect("delete thread must finish");

    drop(mace);
    let reopened = Mace::new(Options::new(&*path).validate()?)?;
    assert!(
        reopened.open_bucket("updating").is_err(),
        "the delete must be durable"
    );
    Ok(())
}
