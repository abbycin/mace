use std::sync::Arc;

use mace::observe::{
    CounterMetric, HistogramMetric, InMemoryObserver, ObserveEvent, ObserveSnapshot,
};
use mace::{Mace, OpCode, Options};

fn main() -> Result<(), OpCode> {
    let path = std::env::temp_dir().join(format!("mace_observer_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);

    let observer = Arc::new(InMemoryObserver::new(256));
    let mut opt = Options::new(&path);
    opt.observer = observer.clone();

    let db = Mace::new(opt.validate()?)?;
    let bucket = db.new_bucket("observe")?;

    let tx = bucket.begin()?;
    tx.put("k1", "v1")?;

    let r = tx.put("k1", "v2");
    assert_eq!(r.err(), Some(OpCode::AbortTx));
    drop(tx);

    let tx = bucket.begin()?;
    tx.put("k2", "v2")?;
    tx.commit()?;

    let snapshot = observer.snapshot();
    print_snapshot(&snapshot);
    Ok(())
}

fn print_snapshot(snapshot: &ObserveSnapshot) {
    println!("== non-zero counters ==");
    for (metric, value) in &snapshot.counters {
        if *value > 0 {
            println!("{metric:?}: {value}");
        }
    }

    println!("\n== non-empty histograms ==");
    for (metric, sample) in &snapshot.histograms {
        if sample.count > 0 {
            let avg = sample.sum as f64 / sample.count as f64;
            println!(
                "{metric:?}: count={}, sum={}, max={}, avg={avg:.2}",
                sample.count, sample.sum, sample.max
            );
        }
    }

    println!("\n== events ==");
    for event in &snapshot.events {
        print_event(event);
    }

    assert_counter_at_least(snapshot, CounterMetric::TxnBegin, 2);
    assert_counter_at_least(snapshot, CounterMetric::TxnConflictAbort, 1);
    assert_counter_at_least(snapshot, CounterMetric::TxnRollback, 1);
    assert_counter_at_least(snapshot, CounterMetric::TxnCommit, 1);
    assert_hist_not_empty(snapshot, HistogramMetric::WalAppendBytes);
}

fn print_event(event: &ObserveEvent) {
    println!(
        "{:?}: bucket_id={}, txid={}, file_id={}, value={}",
        event.kind, event.bucket_id, event.txid, event.file_id, event.value
    );
}

fn assert_counter_at_least(snapshot: &ObserveSnapshot, metric: CounterMetric, min_value: u64) {
    let value = snapshot
        .counters
        .iter()
        .find(|(m, _)| *m == metric)
        .map(|(_, v)| *v)
        .unwrap_or(0);
    assert!(
        value >= min_value,
        "counter {metric:?} expected >= {min_value}, got {value}"
    );
}

fn assert_hist_not_empty(snapshot: &ObserveSnapshot, metric: HistogramMetric) {
    let count = snapshot
        .histograms
        .iter()
        .find(|(m, _)| *m == metric)
        .map(|(_, s)| s.count)
        .unwrap_or(0);
    assert!(
        count > 0,
        "histogram {metric:?} should have at least one sample"
    );
}
