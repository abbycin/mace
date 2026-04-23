use mace::observe::{
    CounterMetric, HistogramMetric, HistogramSample, InMemoryObserver, ObserveSnapshot,
};
use mace::{Mace, OpCode, Options, RandomPath};
use std::sync::Arc;

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
    opt.concurrent_write = 1;
    opt.data_file_size = 4 << 10;
    opt.enable_backpressure = true;
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
