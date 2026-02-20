mod common;

use common::{TestEnv, env_usize, is_retryable_txn_err};
use mace::{Bucket, OpCode};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

fn upsert_retry(bucket: &Bucket, key: &[u8], value: &[u8]) -> Result<(), OpCode> {
    const RETRY_LIMIT: usize = 4096;

    for _ in 0..RETRY_LIMIT {
        let txn = bucket.begin()?;
        match txn.upsert(key, value) {
            Ok(_) => match txn.commit() {
                Ok(()) => return Ok(()),
                Err(err) if is_retryable_txn_err(err) => {
                    std::thread::yield_now();
                    continue;
                }
                Err(err) => return Err(err),
            },
            Err(err) if is_retryable_txn_err(err) => {
                std::thread::yield_now();
                continue;
            }
            Err(err) => return Err(err),
        }
    }

    Err(OpCode::Again)
}

#[test]
fn fast_snapshot_view_stable() -> Result<(), OpCode> {
    let env = TestEnv::new();
    let engine = env.open_with(|options| {
        options.sync_on_write = false;
    })?;
    let bucket = engine.new_bucket("prod_cc")?;

    let init_txn = bucket.begin()?;
    init_txn.put("anchor", "rev_0")?;
    init_txn.commit()?;

    let snapshot = bucket.view()?;
    assert_eq!(snapshot.get("anchor")?.slice(), b"rev_0");

    let begin_barrier = Arc::new(Barrier::new(2));
    let writer_bucket = bucket.clone();
    let writer_barrier = begin_barrier.clone();
    let writer = thread::spawn(move || -> Result<(), OpCode> {
        writer_barrier.wait();
        for revision in 1..=64 {
            let value = format!("rev_{revision}");
            upsert_retry(&writer_bucket, b"anchor", value.as_bytes())?;
        }
        Ok(())
    });

    begin_barrier.wait();

    for _ in 0..64 {
        let seen = snapshot.get("anchor")?;
        assert_eq!(seen.slice(), b"rev_0");
        thread::sleep(Duration::from_millis(1));
    }

    writer.join().expect("writer thread panicked")?;

    let latest_view = bucket.view()?;
    let latest = latest_view.get("anchor")?;
    assert_ne!(latest.slice(), b"rev_0");

    Ok(())
}

#[test]
#[ignore]
fn stress_bucket_churn() -> Result<(), OpCode> {
    let rounds = env_usize("MACE_PROD_BUCKET_CHURN_ROUNDS", 2000);
    let workers = env_usize("MACE_PROD_BUCKET_CHURN_WORKERS", 8).max(2);

    let env = TestEnv::new();
    let engine = env.open_with(|options| {
        options.sync_on_write = false;
    })?;

    thread::scope(|scope| {
        for worker_id in 0..workers {
            let local_engine = engine.clone();
            scope.spawn(move || {
                for round in 0..rounds {
                    let name = format!("prod_churn_{worker_id}_{round}");

                    let bucket = loop {
                        match local_engine.new_bucket(&name) {
                            Ok(bucket) => break bucket,
                            Err(OpCode::Exist) => match local_engine.get_bucket(&name) {
                                Ok(bucket) => break bucket,
                                Err(OpCode::NotFound) => continue,
                                Err(err) => panic!("open existing bucket failed: {err:?}"),
                            },
                            Err(OpCode::NoSpace) => {
                                local_engine.start_gc();
                                thread::sleep(Duration::from_millis(2));
                            }
                            Err(err) => panic!("create bucket failed: {err:?}"),
                        }
                    };

                    let txn = bucket.begin().expect("begin failed");
                    txn.put("k", "v").expect("put failed");
                    txn.commit().expect("commit failed");
                    drop(bucket);

                    loop {
                        match local_engine.del_bucket(&name) {
                            Ok(()) | Err(OpCode::NotFound) => break,
                            Err(OpCode::Again) => thread::sleep(Duration::from_millis(2)),
                            Err(err) => panic!("delete bucket failed: {err:?}"),
                        }
                    }

                    if round % 32 == 0 {
                        local_engine.start_gc();
                    }
                }
            });
        }
    });

    assert!(engine.nr_buckets() <= 1024);
    Ok(())
}
