mod common;

use common::{TestEnv, env_usize, is_retryable_txn_err};
use mace::{Bucket, OpCode};
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
fn fast_active_view_blocks_drop() -> Result<(), OpCode> {
    let env = TestEnv::new();
    let engine = env.open_default()?;

    let name = "prod_evict_view";
    let bucket = engine.new_bucket(name)?;

    let txn = bucket.begin()?;
    txn.put("key", "value")?;
    txn.commit()?;

    let view = bucket.view()?;
    let drop_err = engine.drop_bucket(name);
    assert_eq!(drop_err, Err(OpCode::Again));

    drop(view);
    drop(bucket);

    for _ in 0..32 {
        match engine.drop_bucket(name) {
            Ok(()) => return Ok(()),
            Err(OpCode::Again) => thread::sleep(Duration::from_millis(5)),
            Err(err) => return Err(err),
        }
    }

    Err(OpCode::Again)
}

#[test]
#[ignore]
fn stress_drop_reload_loop() -> Result<(), OpCode> {
    let env = TestEnv::new();
    let engine = env.open_with(|options| {
        options.sync_on_write = false;
        options.cache_evict_pct = 20;
    })?;

    let rounds = env_usize("MACE_PROD_EVICTOR_STRESS_ROUNDS", 2000);

    let name = "prod_evict_stress";
    let bucket = engine.new_bucket(name).or_else(|err| {
        if err == OpCode::Exist {
            engine.get_bucket(name)
        } else {
            Err(err)
        }
    })?;
    drop(bucket);

    for round in 0..rounds {
        let bucket = engine.get_bucket(name)?;

        let expected = format!("v_{round}");
        upsert_retry(&bucket, b"k", expected.as_bytes())?;

        drop(bucket);
        loop {
            match engine.drop_bucket(name) {
                Ok(()) => break,
                Err(OpCode::Again) => thread::sleep(Duration::from_millis(2)),
                Err(err) => return Err(err),
            }
        }

        let reopened = engine.get_bucket(name)?;
        let view = reopened.view()?;
        assert_eq!(view.get("k")?.slice(), expected.as_bytes());

        drop(view);
        drop(reopened);

        if round % 64 == 0 {
            engine.start_gc();
        }
    }

    loop {
        match engine.del_bucket(name) {
            Ok(()) => break,
            Err(OpCode::Again) => thread::sleep(Duration::from_millis(2)),
            Err(OpCode::NotFound) => break,
            Err(err) => return Err(err),
        }
    }

    Ok(())
}
