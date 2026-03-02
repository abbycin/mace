use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering::Relaxed};
use std::time::Instant;

pub const LATENCY_SAMPLE_SHIFT: u8 = 6;

#[inline]
pub fn should_sample(seed: u64, shift: u8) -> bool {
    if shift >= 63 {
        return false;
    }
    let mask = (1u64 << shift) - 1;
    (seed & mask) == 0
}

#[inline]
pub fn sampled_instant(seed: u64, shift: u8) -> Option<Instant> {
    should_sample(seed, shift).then(Instant::now)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CounterMetric {
    TxnBegin,
    TxnCommit,
    TxnAbort,
    TxnConflictAbort,
    TxnRollback,
    TxnRetryAgain,
    TreeRetryAgain,
    TreeNodeSplit,
    TreeNodeMerge,
    TreeNodeConsolidate,
    WalAppend,
    WalSync,
    FlushOrphanDataStaged,
    FlushOrphanBlobStaged,
    FlushOrphanDataCleared,
    FlushOrphanBlobCleared,
    RecoveryRedoRecord,
    RecoveryUndoTxn,
    RecoveryWalTruncate,
    GcRun,
    GcWalRecycleFile,
    GcPendingBucketClean,
    GcScavengePageScan,
    GcScavengePageCompact,
    GcDataRewrite,
    GcBlobRewrite,
    GcDataObsoleteFile,
    GcBlobObsoleteFile,
    FlowAllocWaitCount,
    FlowAllocBurstGrant,
    FlowAllocBurstDeny,
    FlowAllocStructGrant,
    FlowAllocStructDeny,
    FlowGateEnterCount,
    FlowGateExitCount,
}

impl CounterMetric {
    pub const COUNT: usize = 35;
    pub const ALL: [CounterMetric; Self::COUNT] = [
        CounterMetric::TxnBegin,
        CounterMetric::TxnCommit,
        CounterMetric::TxnAbort,
        CounterMetric::TxnConflictAbort,
        CounterMetric::TxnRollback,
        CounterMetric::TxnRetryAgain,
        CounterMetric::TreeRetryAgain,
        CounterMetric::TreeNodeSplit,
        CounterMetric::TreeNodeMerge,
        CounterMetric::TreeNodeConsolidate,
        CounterMetric::WalAppend,
        CounterMetric::WalSync,
        CounterMetric::FlushOrphanDataStaged,
        CounterMetric::FlushOrphanBlobStaged,
        CounterMetric::FlushOrphanDataCleared,
        CounterMetric::FlushOrphanBlobCleared,
        CounterMetric::RecoveryRedoRecord,
        CounterMetric::RecoveryUndoTxn,
        CounterMetric::RecoveryWalTruncate,
        CounterMetric::GcRun,
        CounterMetric::GcWalRecycleFile,
        CounterMetric::GcPendingBucketClean,
        CounterMetric::GcScavengePageScan,
        CounterMetric::GcScavengePageCompact,
        CounterMetric::GcDataRewrite,
        CounterMetric::GcBlobRewrite,
        CounterMetric::GcDataObsoleteFile,
        CounterMetric::GcBlobObsoleteFile,
        CounterMetric::FlowAllocWaitCount,
        CounterMetric::FlowAllocBurstGrant,
        CounterMetric::FlowAllocBurstDeny,
        CounterMetric::FlowAllocStructGrant,
        CounterMetric::FlowAllocStructDeny,
        CounterMetric::FlowGateEnterCount,
        CounterMetric::FlowGateExitCount,
    ];

    #[inline]
    pub const fn idx(self) -> usize {
        self as usize
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum GaugeMetric {
    RecoveryDirtyEntries,
    RecoveryUndoEntries,
    FlowPendingFlushGlobal,
    FlowBurstInUseGlobal,
    FlowGateBucketCount,
}

impl GaugeMetric {
    pub const COUNT: usize = 5;
    pub const ALL: [GaugeMetric; Self::COUNT] = [
        GaugeMetric::RecoveryDirtyEntries,
        GaugeMetric::RecoveryUndoEntries,
        GaugeMetric::FlowPendingFlushGlobal,
        GaugeMetric::FlowBurstInUseGlobal,
        GaugeMetric::FlowGateBucketCount,
    ];

    #[inline]
    pub const fn idx(self) -> usize {
        self as usize
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum HistogramMetric {
    TxnCommitMicros,
    TxnRollbackMicros,
    TreeLinkHoldMicros,
    WalAppendBytes,
    WalSyncMicros,
    RecoveryPhase2Micros,
    RecoveryAnalyzeMicros,
    RecoveryRedoMicros,
    RecoveryUndoMicros,
    GcRunMicros,
    GcScavengeMicros,
    GcDataRewriteMicros,
    GcBlobRewriteMicros,
    GcDataRewriteVictimFiles,
    GcBlobRewriteVictimFiles,
    FlowAllocWaitMicros,
}

impl HistogramMetric {
    pub const COUNT: usize = 16;
    pub const ALL: [HistogramMetric; Self::COUNT] = [
        HistogramMetric::TxnCommitMicros,
        HistogramMetric::TxnRollbackMicros,
        HistogramMetric::TreeLinkHoldMicros,
        HistogramMetric::WalAppendBytes,
        HistogramMetric::WalSyncMicros,
        HistogramMetric::RecoveryPhase2Micros,
        HistogramMetric::RecoveryAnalyzeMicros,
        HistogramMetric::RecoveryRedoMicros,
        HistogramMetric::RecoveryUndoMicros,
        HistogramMetric::GcRunMicros,
        HistogramMetric::GcScavengeMicros,
        HistogramMetric::GcDataRewriteMicros,
        HistogramMetric::GcBlobRewriteMicros,
        HistogramMetric::GcDataRewriteVictimFiles,
        HistogramMetric::GcBlobRewriteVictimFiles,
        HistogramMetric::FlowAllocWaitMicros,
    ];

    #[inline]
    pub const fn idx(self) -> usize {
        self as usize
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum EventKind {
    TxnConflictAbort,
    TxnRollbackComplete,
    FlushOrphanDataStaged,
    FlushOrphanBlobStaged,
    FlushOrphanDataCleared,
    FlushOrphanBlobCleared,
    RecoveryPhase2Begin,
    RecoveryPhase2End,
    GcPendingBucketCleaned,
    GcDataRewriteComplete,
    GcBlobRewriteComplete,
    FlowGateEnter,
    FlowGateExit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObserveEvent {
    pub kind: EventKind,
    pub bucket_id: u64,
    pub txid: u64,
    pub file_id: u64,
    pub value: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HistogramSample {
    pub count: u64,
    pub sum: u64,
    pub max: u64,
}

#[derive(Debug, Clone, Default)]
pub struct ObserveSnapshot {
    pub counters: Vec<(CounterMetric, u64)>,
    pub gauges: Vec<(GaugeMetric, i64)>,
    pub histograms: Vec<(HistogramMetric, HistogramSample)>,
    pub events: Vec<ObserveEvent>,
}

pub trait Observer: Send + Sync {
    fn counter(&self, _metric: CounterMetric, _delta: u64) {}

    fn gauge(&self, _metric: GaugeMetric, _value: i64) {}

    fn histogram(&self, _metric: HistogramMetric, _value: u64) {}

    fn event(&self, _event: ObserveEvent) {}
}

#[inline]
pub fn observe_elapsed(observer: &dyn Observer, metric: HistogramMetric, started: Option<Instant>) {
    if let Some(started) = started {
        observer.histogram(metric, started.elapsed().as_micros() as u64);
    }
}

#[derive(Default)]
pub struct NoopObserver;

impl Observer for NoopObserver {}

pub struct InMemoryObserver {
    counters: [AtomicU64; CounterMetric::COUNT],
    gauges: [AtomicI64; GaugeMetric::COUNT],
    hist_count: [AtomicU64; HistogramMetric::COUNT],
    hist_sum: [AtomicU64; HistogramMetric::COUNT],
    hist_max: [AtomicU64; HistogramMetric::COUNT],
    events: Mutex<VecDeque<ObserveEvent>>,
    event_cap: usize,
}

impl InMemoryObserver {
    pub fn new(event_cap: usize) -> Self {
        Self {
            counters: std::array::from_fn(|_| AtomicU64::new(0)),
            gauges: std::array::from_fn(|_| AtomicI64::new(0)),
            hist_count: std::array::from_fn(|_| AtomicU64::new(0)),
            hist_sum: std::array::from_fn(|_| AtomicU64::new(0)),
            hist_max: std::array::from_fn(|_| AtomicU64::new(0)),
            events: Mutex::new(VecDeque::new()),
            event_cap: event_cap.max(1),
        }
    }

    pub fn snapshot(&self) -> ObserveSnapshot {
        let mut counters = Vec::with_capacity(CounterMetric::COUNT);
        for metric in CounterMetric::ALL {
            counters.push((metric, self.counters[metric.idx()].load(Relaxed)));
        }

        let mut gauges = Vec::with_capacity(GaugeMetric::COUNT);
        for metric in GaugeMetric::ALL {
            gauges.push((metric, self.gauges[metric.idx()].load(Relaxed)));
        }

        let mut histograms = Vec::with_capacity(HistogramMetric::COUNT);
        for metric in HistogramMetric::ALL {
            let idx = metric.idx();
            histograms.push((
                metric,
                HistogramSample {
                    count: self.hist_count[idx].load(Relaxed),
                    sum: self.hist_sum[idx].load(Relaxed),
                    max: self.hist_max[idx].load(Relaxed),
                },
            ));
        }

        let events = self
            .events
            .lock()
            .expect("observer events mutex poisoned")
            .iter()
            .copied()
            .collect();

        ObserveSnapshot {
            counters,
            gauges,
            histograms,
            events,
        }
    }
}

impl Default for InMemoryObserver {
    fn default() -> Self {
        Self::new(1024)
    }
}

impl Observer for InMemoryObserver {
    fn counter(&self, metric: CounterMetric, delta: u64) {
        self.counters[metric.idx()].fetch_add(delta, Relaxed);
    }

    fn gauge(&self, metric: GaugeMetric, value: i64) {
        self.gauges[metric.idx()].store(value, Relaxed);
    }

    fn histogram(&self, metric: HistogramMetric, value: u64) {
        let idx = metric.idx();
        self.hist_count[idx].fetch_add(1, Relaxed);
        self.hist_sum[idx].fetch_add(value, Relaxed);

        let slot = &self.hist_max[idx];
        let mut current = slot.load(Relaxed);
        while value > current {
            match slot.compare_exchange_weak(current, value, Relaxed, Relaxed) {
                Ok(_) => break,
                Err(next) => current = next,
            }
        }
    }

    fn event(&self, event: ObserveEvent) {
        let mut events = self.events.lock().expect("observer events mutex poisoned");
        if events.len() >= self.event_cap {
            events.pop_front();
        }
        events.push_back(event);
    }
}

#[cfg(test)]
mod test {
    use super::{
        CounterMetric, EventKind, GaugeMetric, HistogramMetric, InMemoryObserver, ObserveEvent,
        Observer,
    };

    #[test]
    fn snapshot_counts() {
        let observer = InMemoryObserver::new(2);
        observer.counter(CounterMetric::TxnBegin, 3);
        observer.gauge(GaugeMetric::RecoveryDirtyEntries, 7);
        observer.histogram(HistogramMetric::WalAppendBytes, 11);
        observer.histogram(HistogramMetric::WalAppendBytes, 19);
        observer.event(ObserveEvent {
            kind: EventKind::RecoveryPhase2Begin,
            bucket_id: 0,
            txid: 0,
            file_id: 0,
            value: 0,
        });
        observer.event(ObserveEvent {
            kind: EventKind::RecoveryPhase2End,
            bucket_id: 0,
            txid: 0,
            file_id: 0,
            value: 1,
        });
        observer.event(ObserveEvent {
            kind: EventKind::TxnRollbackComplete,
            bucket_id: 1,
            txid: 2,
            file_id: 3,
            value: 4,
        });

        let snapshot = observer.snapshot();
        let begin = snapshot
            .counters
            .iter()
            .find(|(m, _)| *m == CounterMetric::TxnBegin)
            .map(|(_, v)| *v)
            .unwrap();
        assert_eq!(begin, 3);

        let dirty = snapshot
            .gauges
            .iter()
            .find(|(m, _)| *m == GaugeMetric::RecoveryDirtyEntries)
            .map(|(_, v)| *v)
            .unwrap();
        assert_eq!(dirty, 7);

        let wal = snapshot
            .histograms
            .iter()
            .find(|(m, _)| *m == HistogramMetric::WalAppendBytes)
            .map(|(_, v)| *v)
            .unwrap();
        assert_eq!(wal.count, 2);
        assert_eq!(wal.sum, 30);
        assert_eq!(wal.max, 19);

        assert_eq!(snapshot.events.len(), 2);
        assert_eq!(snapshot.events[0].kind, EventKind::RecoveryPhase2End);
        assert_eq!(snapshot.events[1].kind, EventKind::TxnRollbackComplete);
    }
}
