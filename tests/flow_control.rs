mod common;

use common::TestEnv;
use mace::observe::{CounterMetric, InMemoryObserver};
use mace::{OpCode, Options};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

fn counter(snapshot: &mace::observe::ObserveSnapshot, metric: CounterMetric) -> u64 {
    snapshot
        .counters
        .iter()
        .find(|(m, _)| *m == metric)
        .map(|(_, v)| *v)
        .unwrap_or(0)
}

fn heavy_write_workload(
    bucket: mace::Bucket,
    workers: usize,
    rounds: usize,
    val_size: usize,
) -> Result<(), OpCode> {
    let value = vec![b'x'; val_size];
    thread::scope(|scope| -> Result<(), OpCode> {
        let mut joins = Vec::with_capacity(workers);
        for w in 0..workers {
            let b = bucket.clone();
            let v = value.clone();
            let h = scope.spawn(move || -> Result<(), OpCode> {
                for i in 0..rounds {
                    let key = format!("flow_{w}_{i}");
                    let tx = b.begin()?;
                    tx.put(&key, &v)?;
                    tx.commit()?;
                }
                Ok(())
            });
            joins.push(h);
        }

        for h in joins {
            h.join().expect("worker thread should not panic")?;
        }

        Ok(())
    })?;
    Ok(())
}

#[test]
fn burst_should_touch_metrics() -> Result<(), OpCode> {
    let env = TestEnv::new();
    let observer = Arc::new(InMemoryObserver::new(4096));
    let engine = env.open_with(|opt| {
        opt.sync_on_write = false;
        opt.default_arenas = 8;
        opt.concurrent_write = 8;
        opt.data_file_size = 8 << 10;
        opt.max_log_size = 8 << 10;
        opt.observer = observer.clone();
    })?;
    let bucket = engine.new_bucket("flow_burst")?;

    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(8) {
        heavy_write_workload(bucket.clone(), 8, 128, 2048)?;
        let snapshot = observer.snapshot();
        let grant = counter(&snapshot, CounterMetric::FlowAllocBurstGrant);
        let deny = counter(&snapshot, CounterMetric::FlowAllocBurstDeny);
        if grant + deny > 0 {
            return Ok(());
        }
    }

    let snapshot = observer.snapshot();
    panic!(
        "burst counters were not triggered, grant={}, deny={}",
        counter(&snapshot, CounterMetric::FlowAllocBurstGrant),
        counter(&snapshot, CounterMetric::FlowAllocBurstDeny)
    );
}

#[test]
fn gauge_metrics_should_be_exposed() -> Result<(), OpCode> {
    let env = TestEnv::new();
    let observer = Arc::new(InMemoryObserver::new(1024));
    let mut opt = Options::new(env.path());
    opt.sync_on_write = false;
    opt.data_file_size = 16 << 10;
    opt.max_log_size = 16 << 10;
    opt.observer = observer.clone();
    let engine = mace::Mace::new(opt.validate()?)?;

    let bucket = engine.new_bucket("flow_gauge")?;
    heavy_write_workload(bucket, 2, 64, 1024)?;

    let snapshot = observer.snapshot();
    let has_struct_grant = snapshot
        .counters
        .iter()
        .any(|(m, _)| *m == CounterMetric::FlowAllocStructGrant);
    let has_struct_deny = snapshot
        .counters
        .iter()
        .any(|(m, _)| *m == CounterMetric::FlowAllocStructDeny);
    let has_pending = snapshot
        .gauges
        .iter()
        .any(|(m, _)| *m == mace::observe::GaugeMetric::FlowPendingFlushGlobal);
    let has_burst = snapshot
        .gauges
        .iter()
        .any(|(m, _)| *m == mace::observe::GaugeMetric::FlowBurstInUseGlobal);
    let has_gate = snapshot
        .gauges
        .iter()
        .any(|(m, _)| *m == mace::observe::GaugeMetric::FlowGateBucketCount);
    assert!(has_pending && has_burst && has_gate && has_struct_grant && has_struct_deny);
    Ok(())
}
