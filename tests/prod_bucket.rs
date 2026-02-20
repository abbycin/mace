mod common;

use common::{TestEnv, env_usize, wait_until};
use mace::OpCode;
use std::thread;
use std::time::Duration;

#[test]
fn fast_lifecycle_quota_guard() -> Result<(), OpCode> {
    let env = TestEnv::new();
    let engine = env.open_with(|options| {
        options.sync_on_write = false;
        options.data_file_size = 4096;
    })?;

    let rounds = env_usize("MACE_PROD_BUCKET_FAST_ROUNDS", 96);

    for round in 0..rounds {
        let name = format!("prod_bucket_{round}");
        let bucket = engine.new_bucket(&name)?;

        let txn = bucket.begin()?;
        txn.put("k", "v")?;
        txn.commit()?;
        drop(bucket);

        loop {
            match engine.del_bucket(&name) {
                Ok(()) => break,
                Err(OpCode::Again) => thread::sleep(Duration::from_millis(4)),
                Err(err) => return Err(err),
            }
        }

        if round % 8 == 0 {
            engine.start_gc();
        }

        assert!(engine.nr_buckets() <= 1024);
    }

    Ok(())
}

#[test]
fn fast_pending_delete_counter() -> Result<(), OpCode> {
    let env = TestEnv::new();
    let engine = env.open_with(|options| {
        options.sync_on_write = false;
        options.gc_timeout = 10;
    })?;

    let rounds = env_usize("MACE_PROD_BUCKET_PENDING_ROUNDS", 64).max(24);
    let mut created = Vec::with_capacity(rounds);

    for round in 0..rounds {
        let name = format!("prod_bucket_pending_{round}");
        let bucket = engine.new_bucket(&name)?;

        let txn = bucket.begin()?;
        txn.put("seed", "v")?;
        txn.commit()?;

        created.push(name);
    }

    let baseline = engine.nr_buckets();
    engine.disable_gc();

    for name in created.iter().take(24) {
        loop {
            match engine.del_bucket(name) {
                Ok(()) => break,
                Err(OpCode::Again) => thread::sleep(Duration::from_millis(2)),
                Err(err) => return Err(err),
            }
        }
    }

    assert_eq!(
        engine.nr_buckets(),
        baseline,
        "pending delete should keep bucket count before gc"
    );

    engine.enable_gc();
    let released = wait_until(Duration::from_secs(5), Duration::from_millis(20), || {
        engine.start_gc();
        engine.nr_buckets() < baseline
    });
    assert!(
        released,
        "gc should eventually release pending deleted buckets"
    );

    Ok(())
}

#[test]
#[ignore]
fn stress_create_delete() -> Result<(), OpCode> {
    let env = TestEnv::new();
    let engine = env.open_with(|options| {
        options.sync_on_write = false;
        options.over_provision = true;
    })?;

    let rounds = env_usize("MACE_PROD_BUCKET_STRESS_ROUNDS", 4096);

    for round in 0..rounds {
        let name = format!("prod_bucket_stress_{round}");
        let bucket = loop {
            match engine.new_bucket(&name) {
                Ok(bucket) => break bucket,
                Err(OpCode::NoSpace) => {
                    engine.start_gc();
                    thread::sleep(Duration::from_millis(2));
                }
                Err(err) => return Err(err),
            }
        };

        let txn = bucket.begin()?;
        txn.put("seed", "v")?;
        txn.commit()?;
        drop(bucket);

        loop {
            match engine.del_bucket(&name) {
                Ok(()) => break,
                Err(OpCode::Again) => thread::sleep(Duration::from_millis(2)),
                Err(err) => return Err(err),
            }
        }

        if round % 64 == 0 {
            engine.start_gc();
        }
    }

    Ok(())
}
