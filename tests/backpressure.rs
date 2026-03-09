use mace::observe::{
    CounterMetric, HistogramMetric, HistogramSample, InMemoryObserver, ObserveSnapshot,
};
use mace::{Bucket, Mace, OpCode, Options, RandomPath};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

fn counter(snapshot: &ObserveSnapshot, metric: CounterMetric) -> u64 {
    snapshot
        .counters
        .iter()
        .find(|(m, _)| *m == metric)
        .map(|(_, v)| *v)
        .unwrap_or_default()
}

fn histogram(snapshot: &ObserveSnapshot, metric: HistogramMetric) -> HistogramSample {
    snapshot
        .histograms
        .iter()
        .find(|(m, _)| *m == metric)
        .map(|(_, v)| *v)
        .unwrap_or_default()
}

fn write_workload(db: Bucket, prefix: &str, stop: Arc<AtomicBool>) {
    let value = vec![b'w'; 2048];
    let mut i = 0u64;
    while !stop.load(Relaxed) {
        let tx = match db.begin() {
            Ok(tx) => tx,
            Err(OpCode::Again | OpCode::AbortTx) => {
                std::thread::yield_now();
                continue;
            }
            Err(e) => panic!("begin failed: {e:?}"),
        };
        let key = format!("{prefix}_{i:08}");
        match tx.put(&key, &value) {
            Ok(_) => match tx.commit() {
                Ok(_) => {
                    i += 1;
                }
                Err(OpCode::Again | OpCode::AbortTx) => {
                    std::thread::yield_now();
                }
                Err(e) => panic!("commit failed: {e:?}"),
            },
            Err(OpCode::Again | OpCode::AbortTx) => {
                std::thread::yield_now();
            }
            Err(e) => panic!("put failed: {e:?}"),
        }
    }
}

fn write_workload_count_commits(
    db: Bucket,
    prefix: &str,
    stop: Arc<AtomicBool>,
    commit_cnt: Arc<AtomicU64>,
) {
    let value = vec![b'w'; 2048];
    let mut i = 0u64;
    while !stop.load(Relaxed) {
        let tx = match db.begin() {
            Ok(tx) => tx,
            Err(OpCode::Again | OpCode::AbortTx) => {
                std::thread::yield_now();
                continue;
            }
            Err(e) => panic!("begin failed: {e:?}"),
        };
        let key = format!("{prefix}_{i:08}");
        match tx.put(&key, &value) {
            Ok(_) => match tx.commit() {
                Ok(_) => {
                    i += 1;
                    commit_cnt.fetch_add(1, Relaxed);
                }
                Err(OpCode::Again | OpCode::AbortTx) => {
                    std::thread::yield_now();
                }
                Err(e) => panic!("commit failed: {e:?}"),
            },
            Err(OpCode::Again | OpCode::AbortTx) => {
                std::thread::yield_now();
            }
            Err(e) => panic!("put failed: {e:?}"),
        }
    }
}

#[test]
fn write_backpressure_emits_observe_metrics() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let observer = Arc::new(InMemoryObserver::new(1024));
    let mut opt = Options::new(&*path);
    opt.tmp_store = true;
    opt.sync_on_write = false;
    opt.concurrent_write = 1;
    opt.data_file_size = 8 << 10;
    opt.max_log_size = 64 << 10;
    opt.enable_backpressure = true;
    opt.enable_flush_pacing = false;
    opt.bp_soft_debt_units = 1;
    opt.bp_hard_debt_units = 2;
    opt.bp_stop_debt_units = 3;
    opt.bp_cold_start_fail_safe_units = 1;
    opt.bp_flush_unit_bytes = opt.data_file_size as u64;
    opt.bp_warmup_min_samples = 1;
    opt.bp_floor_bps = 1_000;
    opt.bp_max_delay_us = 3_000;
    opt.observer = observer.clone();

    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let db = mace.new_bucket("x").unwrap();
    let value = vec![b'v'; 2048];
    for i in 0..500 {
        let key = format!("k{i:06}");
        let tx = db.begin().unwrap();
        tx.put(&key, &value)?;
        tx.commit()?;
    }

    let snapshot = observer.snapshot();
    let delay_count = counter(&snapshot, CounterMetric::FlowFgDelay);
    let delay_hist = histogram(&snapshot, HistogramMetric::FlowFgDelayMicros);
    assert!(delay_count > 0, "foreground delay counter should be > 0");
    assert!(
        delay_hist.count > 0,
        "foreground delay histogram count should be > 0"
    );
    assert!(
        delay_hist.sum > 0,
        "foreground delay histogram sum should be > 0"
    );
    Ok(())
}

#[test]
fn flush_pacing_avoids_high_debt_bypass_under_low_debt() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let observer = Arc::new(InMemoryObserver::new(1024));
    let mut opt = Options::new(&*path);
    opt.tmp_store = true;
    opt.sync_on_write = false;
    opt.concurrent_write = 1;
    opt.default_arenas = 64;
    opt.arena_spill_limit = 64;
    opt.data_file_size = 8 << 10;
    opt.max_log_size = 4 << 20;
    opt.enable_backpressure = false;
    opt.enable_flush_pacing = true;
    opt.bp_soft_debt_units = 30_000;
    opt.bp_hard_debt_units = 40_000;
    opt.bp_stop_debt_units = 50_000;
    opt.bp_pacing_soft_debt_units = 30_000;
    opt.bp_flush_unit_bytes = opt.data_file_size as u64;
    opt.bp_floor_bps = 1_000;
    opt.bp_max_delay_us = 1_000_000;
    opt.observer = observer.clone();

    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let db = mace.new_bucket("x").unwrap();
    let value = vec![b'p'; 2048];
    for i in 0..160 {
        let key = format!("k{i:06}");
        let tx = db.begin().unwrap();
        tx.put(&key, &value)?;
        tx.commit()?;
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        let snapshot = observer.snapshot();
        if counter(&snapshot, CounterMetric::FlowFlushPacingSleep) > 0 {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    let snapshot = observer.snapshot();
    let pacing_count = counter(&snapshot, CounterMetric::FlowFlushPacingSleep);
    let pacing_hist = histogram(&snapshot, HistogramMetric::FlowFlushPacingSleepMicros);
    let bypass_high = counter(&snapshot, CounterMetric::FlowFlushPacingBypassHighDebt);
    let bypass_teardown = counter(&snapshot, CounterMetric::FlowFlushPacingBypassTeardown);
    let bypass_starving = counter(
        &snapshot,
        CounterMetric::FlowFlushPacingBypassArenaStarvation,
    );
    assert_eq!(
        bypass_high, 0,
        "low debt pacing path should not enter high-debt bypass"
    );
    assert_eq!(
        bypass_teardown, 0,
        "low debt pacing path should not enter teardown bypass"
    );
    if pacing_count > 0 {
        assert!(
            pacing_hist.count > 0,
            "flush pacing histogram count should be > 0 when sleep happened"
        );
        assert!(
            pacing_hist.sum > 0,
            "flush pacing histogram sum should be > 0 when sleep happened"
        );
    }
    assert!(
        pacing_count > 0 || bypass_starving > 0 || (bypass_high == 0 && bypass_teardown == 0),
        "low debt pacing should not fall into high-debt or teardown bypass (sleep_count={pacing_count}, bypass_high={bypass_high}, bypass_teardown={bypass_teardown}, bypass_starving={bypass_starving})"
    );
    Ok(())
}

#[test]
fn flush_pacing_emits_bypass_metrics_under_high_pressure() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let observer = Arc::new(InMemoryObserver::new(1024));
    let mut opt = Options::new(&*path);
    opt.tmp_store = true;
    opt.sync_on_write = false;
    opt.concurrent_write = 1;
    opt.data_file_size = 8 << 10;
    opt.max_log_size = 64 << 10;
    opt.enable_backpressure = false;
    opt.enable_flush_pacing = true;
    opt.bp_soft_debt_units = 1;
    opt.bp_hard_debt_units = 2;
    opt.bp_stop_debt_units = 3;
    opt.bp_pacing_soft_debt_units = 1;
    opt.bp_flush_unit_bytes = opt.data_file_size as u64;
    opt.bp_warmup_min_samples = 1;
    opt.bp_floor_bps = 1_000;
    opt.bp_max_delay_us = 3_000;
    opt.observer = observer.clone();

    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let db = mace.new_bucket("x").unwrap();
    let value = vec![b'h'; 16 << 10];
    let mut seq = 0u64;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        for _ in 0..128 {
            let tx = match db.begin() {
                Ok(tx) => tx,
                Err(OpCode::Again | OpCode::AbortTx) => {
                    std::thread::yield_now();
                    continue;
                }
                Err(e) => return Err(e),
            };
            let key = format!("k{seq:010}");
            seq += 1;
            match tx.upsert(&key, &value) {
                Ok(_) => match tx.commit() {
                    Ok(_) => {}
                    Err(OpCode::Again | OpCode::AbortTx) => std::thread::yield_now(),
                    Err(e) => return Err(e),
                },
                Err(OpCode::Again | OpCode::AbortTx) => std::thread::yield_now(),
                Err(e) => return Err(e),
            }
        }
        let snapshot = observer.snapshot();
        let bypass_count = counter(&snapshot, CounterMetric::FlowFlushPacingBypassHighDebt);
        if bypass_count > 0 {
            return Ok(());
        }
    }

    let snapshot = observer.snapshot();
    let bypass_count = counter(&snapshot, CounterMetric::FlowFlushPacingBypassHighDebt);
    assert!(
        bypass_count > 0,
        "high pressure pacing bypass counter should be > 0"
    );
    Ok(())
}

#[test]
fn teardown_and_starvation_bypass_hold_under_multi_bucket_concurrency() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let observer = Arc::new(InMemoryObserver::new(2048));
    let mut opt = Options::new(&*path);
    opt.tmp_store = true;
    opt.sync_on_write = false;
    opt.concurrent_write = 8;
    opt.default_arenas = 2;
    opt.arena_spill_limit = 1;
    opt.data_file_size = 4 << 10;
    opt.max_log_size = 64 << 10;
    opt.enable_backpressure = false;
    opt.enable_flush_pacing = true;
    opt.bp_soft_debt_units = 1_000_000;
    opt.bp_hard_debt_units = 1_200_000;
    opt.bp_stop_debt_units = 1_500_000;
    opt.bp_pacing_soft_debt_units = 1_000_000;
    opt.bp_flush_unit_bytes = opt.data_file_size as u64;
    opt.bp_floor_bps = 1_000;
    opt.bp_max_delay_us = 20_000;
    opt.observer = observer.clone();

    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let b0 = mace.new_bucket("b0")?;
    let b1 = mace.new_bucket("b1")?;
    let b2 = mace.new_bucket("b2")?;
    let b3 = mace.new_bucket("b3")?;

    let stop_main = Arc::new(AtomicBool::new(false));
    let stop_drop = Arc::new(AtomicBool::new(false));
    let h0 = {
        let stop = stop_main.clone();
        let db = b0.clone();
        std::thread::spawn(move || write_workload(db, "b0", stop))
    };
    let h1 = {
        let stop = stop_main.clone();
        let db = b1.clone();
        std::thread::spawn(move || write_workload(db, "b1", stop))
    };
    let h3 = {
        let stop = stop_main.clone();
        let db = b3.clone();
        std::thread::spawn(move || write_workload(db, "b3", stop))
    };
    let h2 = {
        let stop = stop_drop.clone();
        let db = b2.clone();
        std::thread::spawn(move || write_workload(db, "b2", stop))
    };

    std::thread::sleep(Duration::from_millis(200));

    stop_drop.store(true, Relaxed);
    h2.join().unwrap();
    drop(b2);

    let before = observer.snapshot();
    let mace_drop = mace.clone();
    let dropper = std::thread::spawn(move || {
        loop {
            match mace_drop.drop_bucket("b2") {
                Ok(_) => break,
                Err(OpCode::Again) => std::thread::sleep(Duration::from_millis(1)),
                Err(e) => panic!("drop_bucket failed: {e:?}"),
            }
        }
    });
    dropper.join().unwrap();
    let after = observer.snapshot();

    stop_main.store(true, Relaxed);
    h0.join().unwrap();
    h1.join().unwrap();
    h3.join().unwrap();

    let teardown_delta = counter(&after, CounterMetric::FlowFlushPacingBypassTeardown)
        .saturating_sub(counter(
            &before,
            CounterMetric::FlowFlushPacingBypassTeardown,
        ));
    assert!(
        teardown_delta > 0,
        "teardown bypass counter should increase during drop_bucket window"
    );

    let end = observer.snapshot();
    let starvation_bypass = counter(&end, CounterMetric::FlowFlushPacingBypassArenaStarvation);
    assert!(
        starvation_bypass > 0,
        "arena starvation bypass should trigger under multi-bucket contention"
    );
    let spill_create = counter(&end, CounterMetric::FlowArenaSpillCreate);
    assert!(
        spill_create > 0,
        "spill creation counter should be > 0 under arena starvation"
    );
    let spill_cap_hit = counter(&end, CounterMetric::FlowArenaSpillCapHit);
    assert!(
        spill_cap_hit > 0,
        "spill cap hit counter should be > 0 under tight spill limit"
    );
    Ok(())
}

#[test]
fn arena_spill_exhaustion_keeps_progress_with_internal_retry() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let observer = Arc::new(InMemoryObserver::new(4096));
    let mut opt = Options::new(&*path);
    opt.tmp_store = true;
    opt.sync_on_write = false;
    opt.concurrent_write = 8;
    opt.default_arenas = 2;
    opt.arena_spill_limit = 1;
    opt.data_file_size = 4 << 10;
    opt.max_log_size = 64 << 10;
    opt.enable_backpressure = false;
    opt.enable_flush_pacing = true;
    opt.bp_soft_debt_units = 1_000_000;
    opt.bp_hard_debt_units = 1_200_000;
    opt.bp_stop_debt_units = 1_500_000;
    opt.bp_pacing_soft_debt_units = 1_000_000;
    opt.bp_flush_unit_bytes = opt.data_file_size as u64;
    opt.bp_floor_bps = 1_000;
    opt.bp_max_delay_us = 20_000;
    opt.observer = observer.clone();

    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let db = mace.new_bucket("x")?;
    let stop = Arc::new(AtomicBool::new(false));
    let commit_cnt = Arc::new(AtomicU64::new(0));

    let mut workers = Vec::new();
    for i in 0..4 {
        let db = db.clone();
        let stop = stop.clone();
        let commit_cnt = commit_cnt.clone();
        workers.push(std::thread::spawn(move || {
            write_workload_count_commits(db, &format!("w{i}"), stop, commit_cnt)
        }));
    }

    std::thread::sleep(Duration::from_secs(2));

    stop.store(true, Relaxed);
    for h in workers {
        h.join().unwrap();
    }

    let snapshot = observer.snapshot();
    let cap_hit = counter(&snapshot, CounterMetric::FlowArenaSpillCapHit);
    assert!(
        cap_hit > 0,
        "spill cap should be hit under constrained arena budget"
    );
    assert!(
        commit_cnt.load(Relaxed) > 0,
        "foreground writes should keep making progress under internal retry"
    );
    Ok(())
}
