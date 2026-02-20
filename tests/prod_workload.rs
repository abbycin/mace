mod common;

use common::{TestEnv, env_usize, is_retryable_txn_err};
use mace::{Bucket, OpCode};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::thread;

fn upsert_retry(bucket: &Bucket, key: &[u8], value: &[u8]) -> Result<bool, OpCode> {
    const RETRY_LIMIT: usize = 4096;

    for _ in 0..RETRY_LIMIT {
        let txn = bucket.begin()?;
        match txn.upsert(key, value) {
            Ok(_) => match txn.commit() {
                Ok(()) => return Ok(true),
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

    Ok(false)
}

fn run_mixed_hotspot(
    ops_per_writer: usize,
    writer_count: usize,
    reader_count: usize,
) -> Result<(), OpCode> {
    let env = TestEnv::new();
    let engine = env.open_with(|options| {
        options.sync_on_write = false;
        options.gc_timeout = 10_000;
    })?;
    let bucket = engine.new_bucket("prod_hot")?;

    let hotspot = env_usize("MACE_PROD_HOT_KEYS", 32).max(4);
    let write_checks = Arc::new(AtomicUsize::new(0));
    let write_soft_failures = Arc::new(AtomicUsize::new(0));
    let read_checks = Arc::new(AtomicUsize::new(0));

    thread::scope(|scope| {
        for writer_id in 0..writer_count {
            let writer_bucket = bucket.clone();
            let write_checks = write_checks.clone();
            let write_soft_failures = write_soft_failures.clone();
            scope.spawn(move || {
                for sequence in 0..ops_per_writer {
                    let target = if sequence % 10 < 8 {
                        sequence % hotspot
                    } else {
                        hotspot + ((writer_id * ops_per_writer + sequence) % hotspot)
                    };
                    let key = format!("hot_{target:04}");
                    let value = format!("writer_{writer_id}_seq_{sequence}");
                    match upsert_retry(&writer_bucket, key.as_bytes(), value.as_bytes()) {
                        Ok(true) => {
                            write_checks.fetch_add(1, Relaxed);
                        }
                        Ok(false) => {
                            write_soft_failures.fetch_add(1, Relaxed);
                        }
                        Err(err) => panic!("writer upsert failed: {err:?}"),
                    }
                }
            });
        }

        for _reader_id in 0..reader_count {
            let reader_bucket = bucket.clone();
            let read_checks = read_checks.clone();
            scope.spawn(move || {
                for sequence in 0..(ops_per_writer / 2 + 1) {
                    let view = reader_bucket.view().expect("open view failed");
                    let probe = format!("hot_{:04}", sequence % hotspot);
                    let _ = view.get(probe.as_bytes());

                    let mut scanned = 0usize;
                    for item in view.seek("hot_") {
                        assert!(item.key().starts_with(b"hot_"));
                        let _ = item.val();
                        scanned += 1;
                        if scanned >= 16 {
                            break;
                        }
                    }
                    read_checks.fetch_add(1, Relaxed);
                }
            });
        }
    });

    let successful_writes = write_checks.load(Relaxed);
    let soft_failures = write_soft_failures.load(Relaxed);
    let expected_writes = writer_count * ops_per_writer;
    let min_success = expected_writes / 8;

    assert!(
        successful_writes >= min_success,
        "successful writes too low: {successful_writes}, soft failures: {soft_failures}, expected: {expected_writes}"
    );
    assert!(read_checks.load(Relaxed) > 0);

    let verify = bucket.view()?;
    let mut found = 0usize;
    for index in 0..hotspot {
        let key = format!("hot_{index:04}");
        if verify.get(key.as_bytes()).is_ok() {
            found += 1;
        }
    }
    assert!(found > 0, "expected at least one hotspot key");

    Ok(())
}

#[test]
fn fast_mixed_hotspot() -> Result<(), OpCode> {
    run_mixed_hotspot(120, 4, 2)
}

#[test]
#[ignore]
fn stress_hotspot() -> Result<(), OpCode> {
    let ops = env_usize("MACE_PROD_STRESS_OPS", 4000);
    let writers = env_usize("MACE_PROD_STRESS_WRITERS", 8).max(2);
    let readers = env_usize("MACE_PROD_STRESS_READERS", 8).max(2);
    run_mixed_hotspot(ops, writers, readers)
}
