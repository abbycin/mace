use mace::observe::{
    CounterMetric, HistogramMetric, HistogramSample, InMemoryObserver, ObserveSnapshot,
};
use mace::{Mace, OpCode, Options, RandomPath};
use std::sync::{Arc, Barrier};

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

#[test]
fn write_backpressure_emits_observe_metrics() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let observer = Arc::new(InMemoryObserver::new(1024));
    let mut opt = Options::new(&*path);
    opt.tmp_store = true;
    opt.sync_on_write = false;
    opt.concurrent_write = 4;
    opt.checkpoint_size = 8 << 10;
    opt.pool_capacity = 8 << 10;
    opt.enable_backpressure = true;
    opt.observer = observer.clone();

    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let db = mace.new_bucket("x").unwrap();
    let value = vec![b'v'; 64 << 10];
    let workers = 4;
    let rounds = 120;
    let gate = Arc::new(Barrier::new(workers));
    let mut handles = Vec::with_capacity(workers);

    for worker in 0..workers {
        let db = db.clone();
        let gate = gate.clone();
        let value = value.clone();
        handles.push(std::thread::spawn(move || -> Result<(), OpCode> {
            gate.wait();
            for i in 0..rounds {
                let key = format!("k{worker:02}_{i:06}");
                let tx = db.begin()?;
                tx.put(&key, &value)?;
                tx.commit()?;
            }
            Ok(())
        }));
    }

    for handle in handles {
        handle.join().expect("writer thread panicked")?;
    }

    let snapshot = observer.snapshot();
    let wait_count = counter(&snapshot, CounterMetric::FlowFgAdmissionWait);
    let wait_hist = histogram(&snapshot, HistogramMetric::FlowFgAdmissionWaitMicros);
    assert!(
        wait_count > 0,
        "foreground admission wait counter should be > 0"
    );
    assert!(
        wait_hist.count > 0,
        "foreground admission wait histogram count should be > 0"
    );
    assert!(
        wait_hist.sum > 0,
        "foreground admission wait histogram sum should be > 0"
    );
    Ok(())
}
