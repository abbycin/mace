use parking_lot::{Condvar, Mutex, MutexGuard};
use std::collections::HashMap;

use crate::must_true;

use crate::meta::FileKind;

/// runtime-only junk state shared by one manifest.
///
/// every file kind owns exactly one [`JunkCoordinator`]; the coordinator is the
/// single owner of its gate, producer permits, phase and collected queues.
pub(crate) struct JunkRegistry {
    coordinators: [JunkCoordinator; FileKind::ALL.len()],
}

impl JunkRegistry {
    pub(crate) fn new() -> Self {
        Self {
            coordinators: [
                JunkCoordinator::new(FileKind::Data),
                JunkCoordinator::new(FileKind::Blob),
            ],
        }
    }

    pub(crate) fn coordinator(&self, kind: FileKind) -> &JunkCoordinator {
        &self.coordinators[kind.slot()]
    }

    /// acquire a permit for every requested kind, all or nothing.
    ///
    /// already-held permits are released before waiting for a closed gate and
    /// the whole set is retried afterwards, so a producer never waits while
    /// holding a permit, manifest txn or stat lock.
    pub(crate) fn acquire_permits(&self, kinds: &[FileKind]) -> JunkPermits<'_> {
        let mut permits = JunkPermits::new();
        loop {
            let mut missing = None;
            for &kind in kinds {
                if permits.permit(kind).is_some() {
                    continue;
                }
                match self.coordinator(kind).try_acquire_permit() {
                    Some(permit) => permits.permits[kind.slot()] = Some(permit),
                    None => {
                        missing = Some(kind);
                        break;
                    }
                }
            }
            let Some(missing) = missing else {
                return permits;
            };
            // a waiter must not pin permits for the kinds that already opened
            permits.clear();
            self.coordinator(missing).wait_gate_open();
        }
    }

    /// close the producer gate for `kind`; the returned guard drains and reopens it.
    pub(crate) fn close_gate(&self, kind: FileKind) -> JunkGate<'_> {
        self.coordinator(kind).close_gate()
    }

    /// register rewrite victims while the initial gate is still closed, then
    /// consume that gate so the kind reopens immediately.
    pub(crate) fn start_rewrite<'a>(
        &'a self,
        kind: FileKind,
        gate: JunkGate<'a>,
        victims: &[u64],
    ) -> RewriteRegistration<'a> {
        let coordinator = self.coordinator(kind);
        must_true!(std::ptr::eq(gate.coordinator, coordinator));
        coordinator.start(victims);
        drop(gate);
        RewriteRegistration {
            coordinator,
            armed: true,
        }
    }

    /// enter the publishing cut. the registration is disarmed before the cut
    /// exists, so a later drop cannot cancel the same collection twice.
    pub(crate) fn begin_publish<'a>(
        &'a self,
        kind: FileKind,
        mut registration: RewriteRegistration<'a>,
        gate: JunkGate<'a>,
    ) -> PublishCut<'a> {
        let coordinator = self.coordinator(kind);
        must_true!(eq registration.kind(), kind);
        must_true!(std::ptr::eq(gate.coordinator, coordinator));
        registration.disarm();
        let collected = coordinator.begin_publish();
        PublishCut {
            registration,
            collected,
            gate: Some(gate),
            finished: false,
        }
    }

    /// consume the cut: restore `Idle` while the publish gate is still closed,
    /// then reopen the gate before returning. physical deletion may start only
    /// after this call returns.
    pub(crate) fn finish_publish(mut cut: PublishCut<'_>) {
        cut.coordinator().finish_publish();
        cut.finished = true;
        // consume the publish gate before returning; physical deletion starts after
        drop(cut.gate.take());
    }

    pub(crate) fn handoff(&self, kind: FileKind, file_ids: &[u64]) {
        self.coordinator(kind).handoff_to_ordinary(file_ids);
    }

    pub(crate) fn clear_ordinary(&self, kind: FileKind, file_ids: &[u64]) {
        self.coordinator(kind).clear_ordinary(file_ids);
    }

    pub(crate) fn lock(&self, kind: FileKind) -> JunkView<'_> {
        self.coordinator(kind).lock()
    }

    #[cfg(any(test, feature = "extra_check"))]
    pub(crate) fn collected_count(&self, kind: FileKind) -> usize {
        self.coordinator(kind).collected_count()
    }
}

/// fixed-size permit set; no heap allocation and no ordering assumptions.
pub(crate) struct JunkPermits<'a> {
    permits: [Option<JunkPermit<'a>>; FileKind::ALL.len()],
}

impl<'a> JunkPermits<'a> {
    fn new() -> Self {
        Self {
            permits: std::array::from_fn(|_| None),
        }
    }

    pub(crate) fn permit(&self, kind: FileKind) -> Option<&JunkPermit<'a>> {
        self.permits[kind.slot()].as_ref()
    }

    pub(crate) fn permit_required(&self, kind: FileKind) -> &JunkPermit<'a> {
        crate::must_exist!(
            self.permit(kind),
            "checkpoint requires a junk permit for {:?}",
            kind
        )
    }

    fn clear(&mut self) {
        self.permits = std::array::from_fn(|_| None);
    }
}

/// a producer permit for one file kind. dropping it wakes gate drainers.
pub(crate) struct JunkPermit<'a> {
    coordinator: &'a JunkCoordinator,
    kind: FileKind,
}

impl JunkPermit<'_> {
    pub(crate) fn kind(&self) -> FileKind {
        self.kind
    }
}

impl Drop for JunkPermit<'_> {
    fn drop(&mut self) {
        let mut inner = self.coordinator.inner.lock();
        must_true!(inner.permits > 0);
        inner.permits -= 1;
        self.coordinator.cv.notify_all();
    }
}

/// a closed producer gate. it owns only `gate_closed`/`permits`, never the
/// collector phase; dropping it reopens the gate.
pub(crate) struct JunkGate<'a> {
    coordinator: &'a JunkCoordinator,
}

impl JunkGate<'_> {
    pub(crate) fn drain(&self) {
        self.coordinator.drain_gate();
    }
}

impl Drop for JunkGate<'_> {
    fn drop(&mut self) {
        let mut inner = self.coordinator.inner.lock();
        must_true!(inner.permits == 0);
        must_true!(inner.gate_closed);
        inner.gate_closed = false;
        self.coordinator.cv.notify_all();
    }
}

/// armed early-return guard for a rewrite that has registered its victims.
///
/// it deliberately does not own the gate, phase or collected queue: only an
/// armed registration cancels, and `begin_publish` disarms it before the cut.
pub(crate) struct RewriteRegistration<'a> {
    coordinator: &'a JunkCoordinator,
    armed: bool,
}

impl RewriteRegistration<'_> {
    fn disarm(&mut self) {
        self.armed = false;
    }

    pub(crate) fn kind(&self) -> FileKind {
        self.coordinator.kind
    }
}

impl Drop for RewriteRegistration<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.coordinator.cancel();
        }
    }
}

/// the publish cut owns the closed publish gate, the disarmed registration and
/// the collected map. dropping it before `finish_publish` restores `Idle`
/// while the gate is still closed, then reopens the gate.
pub(crate) struct PublishCut<'a> {
    registration: RewriteRegistration<'a>,
    collected: HashMap<u64, Vec<u64>>,
    // declared last so the gate reopens after the phase is restored
    gate: Option<JunkGate<'a>>,
    finished: bool,
}

impl PublishCut<'_> {
    pub(crate) fn collected(&self) -> &HashMap<u64, Vec<u64>> {
        &self.collected
    }

    fn coordinator(&self) -> &JunkCoordinator {
        self.registration.coordinator
    }
}

impl Drop for PublishCut<'_> {
    fn drop(&mut self) {
        if !self.finished {
            // the gate is still closed here: restore Idle before it reopens
            self.coordinator().finish_publish();
        }
        drop(self.gate.take());
    }
}

/// per-file-kind owner classification queried under the collector lock.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum JunkOwner {
    RewriteVictim,
    Ordinary,
    Unregistered,
}

/// guard over one file kind's coordinator state, held for owner query,
/// stat update and rewrite enqueue.
pub(crate) struct JunkView<'a> {
    inner: MutexGuard<'a, CoordinatorInner>,
}

impl JunkView<'_> {
    pub(crate) fn is_collecting(&self) -> bool {
        self.inner.state == JunkState::Collecting
    }

    pub(crate) fn owner(&self, file_id: u64) -> JunkOwner {
        if self.inner.state != JunkState::Collecting {
            return JunkOwner::Unregistered;
        }
        match self.inner.junks.get(&file_id) {
            Some(CollectingEntry::Rewrite(_)) => JunkOwner::RewriteVictim,
            Some(CollectingEntry::Ordinary) => JunkOwner::Ordinary,
            None => JunkOwner::Unregistered,
        }
    }

    pub(crate) fn enqueue(&mut self, file_id: u64, addr: u64) -> bool {
        match self.inner.junks.get_mut(&file_id) {
            Some(CollectingEntry::Rewrite(queue)) => {
                queue.push(addr);
                true
            }
            _ => false,
        }
    }
}

/// the single owner of one file kind's gate, permits, phase and collected map.
pub(crate) struct JunkCoordinator {
    kind: FileKind,
    inner: Mutex<CoordinatorInner>,
    cv: Condvar,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum JunkState {
    Idle,
    Collecting,
    Publishing,
}

#[derive(Debug)]
enum CollectingEntry {
    Rewrite(Vec<u64>),
    Ordinary,
}

struct CoordinatorInner {
    state: JunkState,
    junks: HashMap<u64, CollectingEntry>,
    gate_closed: bool,
    permits: usize,
}

impl JunkCoordinator {
    fn new(kind: FileKind) -> Self {
        Self {
            kind,
            inner: Mutex::new(CoordinatorInner {
                state: JunkState::Idle,
                junks: HashMap::new(),
                gate_closed: false,
                permits: 0,
            }),
            cv: Condvar::new(),
        }
    }

    fn try_acquire_permit(&self) -> Option<JunkPermit<'_>> {
        let mut inner = self.inner.lock();
        if inner.gate_closed {
            return None;
        }
        inner.permits += 1;
        Some(JunkPermit {
            coordinator: self,
            kind: self.kind,
        })
    }

    fn close_gate(&self) -> JunkGate<'_> {
        let mut inner = self.inner.lock();
        inner.gate_closed = true;
        JunkGate { coordinator: self }
    }

    fn drain_gate(&self) {
        let mut inner = self.inner.lock();
        while inner.permits != 0 {
            self.cv.wait(&mut inner);
        }
    }

    #[cfg(test)]
    fn drain_gate_timeout(&self, timeout: std::time::Duration) -> bool {
        let mut inner = self.inner.lock();
        while inner.permits != 0 {
            if self.cv.wait_for(&mut inner, timeout).timed_out() {
                return inner.permits == 0;
            }
        }
        true
    }

    fn wait_gate_open(&self) {
        let mut inner = self.inner.lock();
        while inner.gate_closed {
            self.cv.wait(&mut inner);
        }
    }

    fn start(&self, file_ids: &[u64]) {
        let mut inner = self.inner.lock();
        must_true!(inner.gate_closed);
        must_true!(inner.permits == 0);
        must_true!(eq inner.state, JunkState::Idle);
        must_true!(inner.junks.is_empty());
        for &file_id in file_ids {
            inner
                .junks
                .insert(file_id, CollectingEntry::Rewrite(Vec::new()));
        }
        inner.state = JunkState::Collecting;
    }

    pub(crate) fn lock(&self) -> JunkView<'_> {
        JunkView {
            inner: self.inner.lock(),
        }
    }

    fn handoff_to_ordinary(&self, file_ids: &[u64]) {
        let mut inner = self.inner.lock();
        must_true!(eq inner.state, JunkState::Collecting);
        for &file_id in file_ids {
            if let Some(entry) = inner.junks.get_mut(&file_id) {
                *entry = CollectingEntry::Ordinary;
            }
        }
    }

    fn clear_ordinary(&self, file_ids: &[u64]) {
        let mut inner = self.inner.lock();
        for &file_id in file_ids {
            if let Some(entry) = inner.junks.remove(&file_id) {
                must_true!(matches!(entry, CollectingEntry::Ordinary));
            }
        }
    }

    fn cancel(&self) {
        let mut inner = self.inner.lock();
        must_true!(eq inner.state, JunkState::Collecting);
        inner.junks.clear();
        inner.state = JunkState::Idle;
    }

    fn begin_publish(&self) -> HashMap<u64, Vec<u64>> {
        let mut inner = self.inner.lock();
        must_true!(inner.gate_closed);
        must_true!(inner.permits == 0);
        must_true!(eq inner.state, JunkState::Collecting);
        must_true!(
            inner
                .junks
                .values()
                .all(|entry| matches!(entry, CollectingEntry::Rewrite(_)))
        );
        inner.state = JunkState::Publishing;
        std::mem::take(&mut inner.junks)
            .into_iter()
            .map(|(file_id, entry)| match entry {
                CollectingEntry::Rewrite(queue) => (file_id, queue),
                CollectingEntry::Ordinary => unreachable!("ordinary entry before publish"),
            })
            .collect()
    }

    fn finish_publish(&self) {
        let mut inner = self.inner.lock();
        must_true!(eq inner.state, JunkState::Publishing);
        inner.state = JunkState::Idle;
    }

    #[cfg(any(test, feature = "extra_check"))]
    fn collected_count(&self) -> usize {
        self.inner
            .lock()
            .junks
            .values()
            .map(|entry| match entry {
                CollectingEntry::Rewrite(queue) => queue.len(),
                CollectingEntry::Ordinary => 0,
            })
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, mpsc::channel},
        time::Duration,
    };

    use crate::meta::FileKind;

    use super::JunkRegistry;

    #[test]
    fn junk_gate_rejects_permits_until_drain_and_drop() {
        let registry = JunkRegistry::new();
        let coordinator = registry.coordinator(FileKind::Data);
        let permit = coordinator.try_acquire_permit().expect("permit");
        assert_eq!(permit.kind(), FileKind::Data);
        let gate = registry.close_gate(FileKind::Data);
        assert!(coordinator.try_acquire_permit().is_none());
        let (tx, rx) = channel();
        std::thread::scope(|scope| {
            scope.spawn(|| {
                gate.drain();
                tx.send(()).expect("drain completed");
            });
            assert!(rx.recv_timeout(Duration::from_millis(50)).is_err());
            drop(permit);
            rx.recv_timeout(Duration::from_secs(1))
                .expect("drain must wait for permit release");
        });
        drop(gate);
        assert!(coordinator.try_acquire_permit().is_some());
    }

    #[test]
    fn acquire_permits_waits_for_every_kind_and_retries() {
        let registry = Arc::new(JunkRegistry::new());
        let gate = registry.close_gate(FileKind::Blob);
        let (acquired_tx, acquired_rx) = channel();
        std::thread::scope(|scope| {
            let registry_ref = &registry;
            scope.spawn(move || {
                let permits = registry_ref.acquire_permits(&[FileKind::Data, FileKind::Blob]);
                assert!(permits.permit(FileKind::Data).is_some());
                assert!(permits.permit(FileKind::Blob).is_some());
                acquired_tx.send(()).expect("acquired permits");
            });
            assert!(
                acquired_rx.recv_timeout(Duration::from_millis(50)).is_err(),
                "all-or-nothing acquisition must wait for the closed kind"
            );
            drop(gate);
            acquired_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("acquire must complete once the gate opens");
        });
    }

    #[test]
    fn multi_kind_permit_wait_releases_held_permits() {
        let registry = Arc::new(JunkRegistry::new());
        let blob_gate = registry.close_gate(FileKind::Blob);
        let (acquired_tx, acquired_rx) = channel();
        std::thread::scope(|scope| {
            let registry_ref = &registry;
            scope.spawn(move || {
                let permits = registry_ref.acquire_permits(&[FileKind::Data, FileKind::Blob]);
                assert!(permits.permit(FileKind::Data).is_some());
                assert!(permits.permit(FileKind::Blob).is_some());
                acquired_tx.send(()).expect("acquired permits");
            });
            assert!(
                acquired_rx.recv_timeout(Duration::from_millis(50)).is_err(),
                "acquisition waits for the closed kind"
            );
            // let the waiter reach the blob gate with a data permit already taken
            std::thread::sleep(Duration::from_millis(50));
            let data_gate = registry.close_gate(FileKind::Data);
            assert!(
                registry
                    .coordinator(FileKind::Data)
                    .drain_gate_timeout(Duration::from_secs(1)),
                "a waiting acquirer must release permits it already holds"
            );
            drop(data_gate);
            drop(blob_gate);
            acquired_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("acquire must complete once every gate opens");
        });
    }

    #[test]
    fn junk_collection_cancels_on_drop() {
        for kind in FileKind::ALL {
            let registry = JunkRegistry::new();
            let coordinator = registry.coordinator(kind);
            let gate = registry.close_gate(kind);
            let registration = registry.start_rewrite(kind, gate, &[7]);
            assert!(coordinator.try_acquire_permit().is_some());
            drop(registration);
            // the kind is collecting no file again, so a new collection starts
            let gate = registry.close_gate(kind);
            let registration = registry.start_rewrite(kind, gate, &[8]);
            drop(registration);
            assert!(coordinator.try_acquire_permit().is_some());
        }
    }

    #[test]
    fn rewrite_registration_drop_cancels_once_when_armed() {
        for kind in FileKind::ALL {
            let registry = JunkRegistry::new();
            let gate = registry.close_gate(kind);
            let registration = registry.start_rewrite(kind, gate, &[7]);
            {
                let mut view = registry.lock(kind);
                assert_eq!(view.owner(7), super::JunkOwner::RewriteVictim);
                assert!(view.enqueue(7, 100));
            }
            assert_eq!(registry.collected_count(kind), 1);
            drop(registration);
            // cancel cleared the queue and returned to idle exactly once
            assert_eq!(registry.collected_count(kind), 0);
            let gate = registry.close_gate(kind);
            let registration = registry.start_rewrite(kind, gate, &[7]);
            assert_eq!(registry.collected_count(kind), 0);
            drop(registration);
        }
    }

    #[test]
    fn begin_publish_disarms_registration_so_cut_drop_does_not_cancel() {
        for kind in FileKind::ALL {
            let registry = JunkRegistry::new();
            let gate = registry.close_gate(kind);
            let registration = registry.start_rewrite(kind, gate, &[8]);
            let gate = registry.close_gate(kind);
            let cut = registry.begin_publish(kind, registration, gate);
            assert_eq!(cut.collected().len(), 1);
            // a second cancel would panic because the cut restored Idle first
            drop(cut);
            let gate = registry.close_gate(kind);
            let registration = registry.start_rewrite(kind, gate, &[9]);
            drop(registration);
            assert!(registry.coordinator(kind).try_acquire_permit().is_some());
        }
    }

    #[test]
    fn publish_cut_early_return_resets_to_idle() {
        for kind in FileKind::ALL {
            let registry = JunkRegistry::new();
            let gate = registry.close_gate(kind);
            let registration = registry.start_rewrite(kind, gate, &[9]);
            let gate = registry.close_gate(kind);
            let cut = registry.begin_publish(kind, registration, gate);
            assert_eq!(cut.collected().len(), 1);
            drop(cut);
            // the gate reopened and the phase is Idle for the next collection
            let gate = registry.close_gate(kind);
            let registration = registry.start_rewrite(kind, gate, &[10]);
            assert_eq!(registry.collected_count(kind), 0);
            drop(registration);
            assert!(registry.coordinator(kind).try_acquire_permit().is_some());
        }
    }

    #[test]
    fn rewrite_junk_collector_registers_victims_and_closes_publish_cut() {
        let registry = JunkRegistry::new();
        let kind = FileKind::Data;

        // publish extracts only rewrite queues
        let gate = registry.close_gate(kind);
        let registration = registry.start_rewrite(kind, gate, &[3, 5]);
        let mut view = registry.lock(kind);
        assert_eq!(view.owner(3), super::JunkOwner::RewriteVictim);
        assert!(view.enqueue(3, 100));
        assert!(view.enqueue(5, 200));
        drop(view);
        let gate = registry.close_gate(kind);
        let cut = registry.begin_publish(kind, registration, gate);
        assert_eq!(cut.collected().len(), 2, "both registered victims publish");
        assert_eq!(cut.collected().get(&3), Some(&vec![100]));
        assert_eq!(cut.collected().get(&5), Some(&vec![200]));
        JunkRegistry::finish_publish(cut);

        // empty output cancels collection through the armed registration
        let gate = registry.close_gate(kind);
        let registration = registry.start_rewrite(kind, gate, &[7]);
        drop(registration);

        // ordinary handoffs must clear before the rewrite-only publish cut
        let gate = registry.close_gate(kind);
        let registration = registry.start_rewrite(kind, gate, &[8]);
        registry.handoff(kind, &[8]);
        assert_eq!(registry.lock(kind).owner(8), super::JunkOwner::Ordinary);
        registry.clear_ordinary(kind, &[8]);
        let gate = registry.close_gate(kind);
        let cut = registry.begin_publish(kind, registration, gate);
        assert!(
            cut.collected().is_empty(),
            "a cleared ordinary entry must not publish as a victim"
        );
        JunkRegistry::finish_publish(cut);
    }

    #[test]
    fn rewrite_publish_waits_for_collector_critical_section() {
        let registry = JunkRegistry::new();
        let kind = FileKind::Data;
        let gate = registry.close_gate(kind);
        let registration = registry.start_rewrite(kind, gate, &[3]);
        let gate = registry.close_gate(kind);
        let (locked_tx, locked_rx) = channel();
        let (release_tx, release_rx) = channel();
        let (published_tx, published_rx) = channel();

        std::thread::scope(|scope| {
            let registry_ref = &registry;
            scope.spawn(move || {
                let _guard = registry_ref.lock(kind);
                locked_tx.send(()).expect("signal collector lock");
                release_rx.recv().expect("release collector lock");
            });
            locked_rx.recv().expect("collector must lock");
            let registry_ref = &registry;
            scope.spawn(move || {
                let cut = registry_ref.begin_publish(kind, registration, gate);
                published_tx.send(()).expect("signal publish");
                JunkRegistry::finish_publish(cut);
            });
            assert!(
                published_rx
                    .recv_timeout(Duration::from_millis(100))
                    .is_err(),
                "publish must wait for the collector critical section"
            );
            release_tx.send(()).expect("release collector");
            published_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("publish must finish after collector release");
        });
    }
}
