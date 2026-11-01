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
use std::time::{Duration, Instant};

use mace::testing::{self, CheckpointSyncPoint};
use mace::{BucketOptions, Mace, OpCode, Options, RandomPath};

struct HookReset;

impl Drop for HookReset {
    fn drop(&mut self) {
        testing::clear_checkpoint_hook();
    }
}

/// a checkpoint round queued before the delete must not publish after it
///
/// the queued task is held right before its flush directive is resolved; the delete starts while
/// it is held, and once the name left the catalog the delete has already recorded the discard
/// mode, so the released task sees `Skip` and no checkpoint result is ever synchronized.
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

    let delete_mace = mace.clone();
    let (result_tx, result_rx) = channel();
    let delete = std::thread::spawn(move || {
        let result = delete_mace.del_bucket("discard");
        result_tx.send(result).expect("signal delete result");
    });
    // the name leaves the catalog inside the delete's logical phase, after the discard mode was
    // recorded and before the delete waits for the queued round: that is the release point
    let deadline = Instant::now() + Duration::from_secs(10);
    while mace.active_buckets().iter().any(|name| name == "discard") {
        assert!(
            Instant::now() < deadline,
            "the delete must remove the active name"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
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
