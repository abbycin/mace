mod common;

use common::{TestEnv, is_retryable_txn_err};
use mace::{Bucket, BucketOptions, OpCode};
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
    let bucket = engine.new_bucket("prod_cc", BucketOptions::default())?;

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
