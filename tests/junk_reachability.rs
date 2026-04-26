use mace::observe::{CounterMetric, InMemoryObserver, ObserveSnapshot};
use mace::{Bucket, Mace, OpCode, Options, RandomPath};
use std::sync::Arc;

// Regression guard for the reachable-junk lifecycle changes bug class:
// reachable junk pages (via sibling/remote edges) were retired too early, so read path
// could hit "not in dirty pages" + "not in interval" hole before durability closure.
//
// Why this test exists:
// - future graph/lifecycle changes can add new reachable edges without updating junk-retire rules.
// - this test keeps a lagging view alive while forcing split/compact/checkpoint churn to ensure
//   reachable pages are never reclaimed prematurely.
//
// What a failure means:
// - if lag-view reads fail (NotFound / panic), dirty lifecycle closure is broken again
//   (reachable page got reclaimed before becoming durable).
// - if split/consolidate counters stay zero, the scenario is not exercising the risky path.
fn counter(snapshot: &ObserveSnapshot, metric: CounterMetric) -> u64 {
    snapshot
        .counters
        .iter()
        .find(|(m, _)| *m == metric)
        .map(|(_, v)| *v)
        .unwrap_or_default()
}

fn parity_payload(round: usize, value_size: usize) -> Vec<u8> {
    let mut p = vec![b'x'; value_size];
    p[0] = (round as u8).wrapping_add(1);
    p
}

fn upsert_parity_retry(
    bucket: &Bucket,
    keys: &[String],
    parity: usize,
    payload: &[u8],
) -> Result<(), OpCode> {
    const RETRY_LIMIT: usize = 2048;
    for _ in 0..RETRY_LIMIT {
        let tx = bucket.begin().unwrap();
        let mut retry = false;
        for (idx, key) in keys.iter().enumerate() {
            if idx % 2 != parity {
                continue;
            }
            match tx.upsert(key, payload) {
                Ok(_) => {}
                Err(OpCode::Again | OpCode::AbortTx) => {
                    retry = true;
                    break;
                }
                Err(e) => return Err(e),
            }
        }
        if retry {
            std::thread::yield_now();
            continue;
        }
        match tx.commit() {
            Ok(()) => return Ok(()),
            Err(OpCode::Again | OpCode::AbortTx) => std::thread::yield_now(),
            Err(e) => return Err(e),
        }
    }
    panic!("upsert parity retry exhausted");
}

#[test]
fn reachable_junk_regression_guard() -> Result<(), OpCode> {
    // Purpose:
    // 1) force sibling/remote-producing churn (split + consolidate),
    // 2) keep lagging readers alive across rounds and checkpoints,
    // 3) assert old view never loses reachability before persistence closure.
    const KEYS: usize = 128;
    const VALUE_SIZE: usize = 4096;
    const ROUNDS: usize = 48;

    let path = RandomPath::new();
    let observer = Arc::new(InMemoryObserver::new(256));
    let mut opt = Options::new(&*path);
    opt.tmp_store = true;
    opt.sync_on_write = false;
    opt.concurrent_write = 1;
    opt.inline_size = 128;
    opt.split_elems = 16;
    opt.consolidate_threshold = 4;
    opt.data_file_size = 64 << 10;
    opt.checkpoint_size = 256 << 10;
    opt.pool_capacity = 2 << 20;
    opt.max_ckpt_per_txn = 64;
    opt.gc_eager = false;
    opt.gc_timeout = 60_000;
    opt.observer = observer.clone();

    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let bucket = mace.new_bucket("x").unwrap();
    let keys: Vec<String> = (0..KEYS).map(|i| format!("k_{i:04}")).collect();

    let seed_payload = vec![b'a'; VALUE_SIZE];
    for parity in 0..2 {
        upsert_parity_retry(&bucket, &keys, parity, &seed_payload)?;
    }
    bucket.checkpoint();
    let mut expected = vec![b'a'; KEYS];

    for round in 0..ROUNDS {
        // Keep a lagging snapshot alive: this is exactly where premature junk reclaim used
        // to break (reachable old addresses disappeared before becoming durable).
        let lag = bucket.view().unwrap();
        let before = expected.clone();
        let payload = parity_payload(round, VALUE_SIZE);
        upsert_parity_retry(&bucket, &keys, round % 2, &payload)?;
        let byte = payload[0];
        for (idx, value) in expected.iter_mut().enumerate() {
            if idx % 2 == round % 2 {
                *value = byte;
            }
        }

        for idx in (0..KEYS).step_by(9) {
            let value = lag.get(&keys[idx]).expect("lag view key must exist");
            assert_eq!(value.slice().len(), VALUE_SIZE);
            assert_eq!(value.slice()[0], before[idx]);
        }
        for item in lag.seek("k_") {
            assert_eq!(item.val().len(), VALUE_SIZE);
        }

        if round % 3 == 0 {
            bucket.checkpoint();
        }
        let view = bucket.view().unwrap();
        let mut count = 0;
        for item in view.seek("k_") {
            count += 1;
            assert_eq!(item.val().len(), VALUE_SIZE);
        }
        assert_eq!(count, KEYS);
    }

    bucket.checkpoint();
    let final_view = bucket.view().unwrap();
    for (idx, key) in keys.iter().enumerate() {
        let value = final_view.get(key).expect("final key must exist");
        assert_eq!(value.slice().len(), VALUE_SIZE);
        assert_eq!(value.slice()[0], expected[idx]);
    }

    let snapshot = observer.snapshot();
    let split_count = counter(&snapshot, CounterMetric::TreeNodeSplit);
    let consolidate_count = counter(&snapshot, CounterMetric::TreeNodeConsolidate);
    // If these stay zero, this test is no longer covering the intended risky path.
    assert!(split_count > 0, "split was not exercised");
    assert!(consolidate_count > 0, "compaction path was not exercised");
    Ok(())
}
