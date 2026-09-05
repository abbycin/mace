mod common;

use common::{TestEnv, env_usize};
use mace::{BucketOptions, OpCode};

#[test]
fn fast_lifecycle_quota_guard() -> Result<(), OpCode> {
    let env = TestEnv::new();
    let engine = env.open_with(|options| {
        options.sync_on_write = false;
        options.data_file_size = 4096;
        common::deterministic_gc(options);
    })?;

    let rounds = env_usize("MACE_PROD_BUCKET_FAST_ROUNDS", 96);

    for round in 0..rounds {
        let name = format!("prod_bucket_{round}");
        let bucket = engine.new_bucket(&name, BucketOptions::default())?;

        let txn = bucket.begin()?;
        txn.put("k", "v")?;
        txn.commit()?;
        drop(bucket);

        loop {
            match engine.del_bucket(&name) {
                Ok(()) => break,
                Err(OpCode::Again) => engine.start_gc(),
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
        common::deterministic_gc(options);
    })?;

    let rounds = env_usize("MACE_PROD_BUCKET_PENDING_ROUNDS", 64).max(24);
    let mut created = Vec::with_capacity(rounds);

    for round in 0..rounds {
        let name = format!("prod_bucket_pending_{round}");
        let bucket = engine.new_bucket(&name, BucketOptions::default())?;

        let txn = bucket.begin()?;
        txn.put("seed", "v")?;
        txn.commit()?;

        created.push(name);
    }

    let baseline = engine.nr_buckets();
    // extra_check: gc_timeout=0 arms no timer. other builds keep the default
    // timer, so pause the collector to protect the pending-count assertion;
    // explicit start_gc rounds still run while paused
    #[cfg(not(feature = "extra_check"))]
    engine.disable_gc();

    for name in created.iter().take(24) {
        loop {
            match engine.del_bucket(name) {
                Ok(()) => break,
                Err(OpCode::Again) => engine.start_gc(),
                Err(err) => return Err(err),
            }
        }
    }

    assert_eq!(
        engine.nr_buckets(),
        baseline,
        "pending delete should keep bucket count before gc"
    );

    let released = common::gc_rounds_until(&engine, 8, || engine.nr_buckets() < baseline);
    assert!(
        released,
        "gc should release pending deleted buckets within 8 explicit rounds"
    );

    Ok(())
}

#[test]
#[ignore]
fn stress_create_delete() -> Result<(), OpCode> {
    let env = TestEnv::new();
    let engine = env.open_with(|options| {
        options.sync_on_write = false;
    })?;

    let rounds = env_usize("MACE_PROD_BUCKET_STRESS_ROUNDS", 4096);

    for round in 0..rounds {
        let name = format!("prod_bucket_stress_{round}");
        let bucket = loop {
            match engine.new_bucket(&name, BucketOptions::default()) {
                Ok(bucket) => break bucket,
                Err(OpCode::NoSpace) => {
                    engine.start_gc();
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
                Err(OpCode::Again) => engine.start_gc(),
                Err(err) => return Err(err),
            }
        }

        if round % 64 == 0 {
            engine.start_gc();
        }
    }

    Ok(())
}
