use parking_lot::{Mutex, MutexGuard};
use std::{
    cell::UnsafeCell,
    marker::PhantomData,
    sync::{
        OnceLock,
        atomic::{AtomicBool, Ordering},
    },
};

thread_local! {
    static THREAD: ThreadTable = const { ThreadTable::new() };
}

/// per-instance thread-affine storage: one slot id per owner, one entry per
/// (owner slot, thread). the owner holds the unique slot token; entries live in
/// the owning thread's private table and are taken/put under take/put semantics
/// (the cell is empty while the value is borrowed out).
pub(crate) struct LocalSlot<T: Send + 'static> {
    key: u32,
    drained: AtomicBool,
    _marker: PhantomData<fn() -> T>,
}

impl<T: Send + 'static> LocalSlot<T> {
    pub(crate) fn new() -> Self {
        let mut alloc = lock(&registry().alloc);
        let key = alloc.free.pop().unwrap_or_else(|| {
            let key = alloc.next;
            alloc.next = alloc
                .next
                .checked_add(1)
                .expect("instance-local slot id space exhausted");
            key
        });
        Self {
            key,
            drained: AtomicBool::new(false),
            _marker: PhantomData,
        }
    }

    /// Take the current thread's value, creating it under the registry lock when
    /// the cell is empty. `init` must be bounded, nonblocking, non-reentrant,
    /// and must not panic.
    pub(crate) fn try_take<E>(&self, init: impl FnOnce() -> Result<T, E>) -> Result<T, E> {
        self.assert_open();
        THREAD.with(|table| {
            table.ensure_registered();
            if let Some(value) = table.take_existing(self.key) {
                return Ok(value);
            }

            let _threads = lock(&registry().threads);
            table.take_slow(self.key, init)
        })
    }

    /// Put a value back into the current thread's cell. Returns the value when
    /// the cell is already occupied (reentrant put won the race); callers spill
    /// it to their own overflow or drop it.
    pub(crate) fn try_put(&self, value: T) -> Result<(), T> {
        self.assert_open();
        THREAD.with(|table| {
            table.ensure_registered();
            if table.has_entry(self.key) {
                return table.put_existing(self.key, value);
            }

            let _threads = lock(&registry().threads);
            table.put_slow(self.key, value)
        })
    }

    /// take + closure + put-back. cannot express a value escaping the closure to
    /// a later Drop; use explicit try_take/try_put for that.
    pub(crate) fn with<R>(&self, init: impl FnOnce() -> T, f: impl FnOnce(&mut T) -> R) -> R {
        let mut value = PutBack {
            slot: self,
            value: Some(self.take(init)),
        };
        f(value
            .value
            .as_mut()
            .expect("instance-local value is present"))
    }

    /// Remove this slot's entry from every live thread table after the owner has
    /// become quiescent, then return the id to the allocator (last, so a reused
    /// id can never observe a stale entry). Idempotent: explicit drain and the
    /// Drop fallback both route here.
    pub(crate) fn drain(&mut self) {
        if self.drained.swap(true, Ordering::Relaxed) {
            return;
        }

        let retired = {
            let threads = lock(&registry().threads);
            threads
                .iter()
                .filter_map(|node| unsafe {
                    // safety: a node remains registered until its ThreadTable drop holds this
                    // lock; owner quiescence guarantees no concurrent owner-thread access
                    (*node.table).take_entry(self.key)
                })
                .collect::<Vec<_>>()
        };
        drop(retired);

        lock(&registry().alloc).free.push(self.key);
    }

    fn take(&self, init: impl FnOnce() -> T) -> T {
        match self.try_take(|| Ok::<T, std::convert::Infallible>(init())) {
            Ok(value) => value,
            Err(never) => match never {},
        }
    }

    fn put(&self, value: T) {
        let _ = self.try_put(value);
    }

    fn assert_open(&self) {
        assert!(
            !self.drained.load(Ordering::Relaxed),
            "instance-local slot accessed after drain"
        );
    }
}

impl<T: Send + 'static> Drop for LocalSlot<T> {
    fn drop(&mut self) {
        self.drain();
    }
}

struct PutBack<'a, T: Send + 'static> {
    slot: &'a LocalSlot<T>,
    value: Option<T>,
}

impl<T: Send + 'static> Drop for PutBack<'_, T> {
    fn drop(&mut self) {
        self.slot.put(
            self.value
                .take()
                .expect("instance-local put-back value is present"),
        );
    }
}

struct Registry {
    /// live thread tables; held while registering, growing, draining, and
    /// tearing a table down so those never race each other
    threads: Mutex<Vec<ThreadNode>>,
    alloc: Mutex<SlotAlloc>,
}

impl Registry {
    fn new() -> Self {
        Self {
            threads: Mutex::new(Vec::new()),
            alloc: Mutex::new(SlotAlloc::default()),
        }
    }
}

#[derive(Default)]
struct SlotAlloc {
    next: u32,
    free: Vec<u32>,
}

struct ThreadNode {
    table: *const ThreadTable,
}

// safety: pointers are only dereferenced while Registry::threads holds the table registration lock
unsafe impl Send for ThreadNode {}

/// one thread's private table, indexed densely by slot id. hot take/put only
/// touch this thread's own cells; other threads reach a cell only through drain
/// (owner quiescent) or this table's own teardown, both under Registry::threads.
struct ThreadTable {
    entries: UnsafeCell<Vec<EntryCell>>,
    registered: AtomicBool,
}

impl ThreadTable {
    const fn new() -> Self {
        Self {
            entries: UnsafeCell::new(Vec::new()),
            registered: AtomicBool::new(false),
        }
    }

    fn ensure_registered(&self) {
        if self.registered.load(Ordering::Acquire) {
            return;
        }

        let mut threads = lock(&registry().threads);
        if !self.registered.load(Ordering::Relaxed) {
            threads.push(ThreadNode {
                table: self as *const _,
            });
            self.registered.store(true, Ordering::Release);
        }
    }

    fn take_existing<T: Send + 'static>(&self, key: u32) -> Option<T> {
        let entries = unsafe {
            // safety: this fast path never takes the threads lock, so the lock cannot exclude
            // it; concurrent drain is excluded by owner quiescence (drain takes &mut self, so
            // no other thread holds a live &self while it runs). the threads lock only
            // serializes foreign table growth and thread-exit teardown against drain.
            &*self.entries.get()
        };
        let cell = entries.get(key as usize)?;
        let entry = unsafe {
            // safety: owner-thread access is exclusive for this slot until owner-quiescent drain
            &mut *cell.entry.get()
        }
        .as_mut()?;
        unsafe {
            // safety: the entry belongs to this LocalSlot<T> by id-reuse ordering;
            // owner-thread access is exclusive for this slot
            entry.take::<T>()
        }
    }

    fn take_slow<T: Send + 'static, E>(
        &self,
        key: u32,
        init: impl FnOnce() -> Result<T, E>,
    ) -> Result<T, E> {
        let entries = unsafe {
            // safety: caller holds Registry::threads, serializing growth, drain, and thread exit
            &mut *self.entries.get()
        };
        ensure_entry_capacity(entries, key);
        let entry = unsafe {
            // safety: caller holds Registry::threads and this owner thread is the sole hot-path writer
            &mut *entries[key as usize].entry.get()
        };
        if let Some(entry) = entry.as_mut() {
            if let Some(value) = unsafe {
                // safety: caller holds Registry::threads and this owner thread is the
                // sole hot-path writer for this entry
                entry.take::<T>()
            } {
                return Ok(value);
            }
            // entry exists but the value is out (reentrant borrow): init a
            // temporary and return it without installing, so only one value is
            // ever cached per thread
            return init();
        }

        let value = init()?;
        *entry = Some(Entry::new(value));
        Ok(unsafe {
            // safety: this entry was just installed with T for this LocalSlot<T>
            entry
                .as_mut()
                .expect("new instance-local entry is present")
                .take::<T>()
                .expect("new instance-local value is present")
        })
    }

    fn has_entry(&self, key: u32) -> bool {
        let entries = unsafe {
            // safety: owner-quiescent drain (&mut self) cannot race this owner-thread read;
            // the threads lock serializes only foreign growth/teardown against drain
            &*self.entries.get()
        };
        entries
            .get(key as usize)
            .is_some_and(|cell| unsafe { (&*cell.entry.get()).is_some() })
    }

    fn put_existing<T: Send + 'static>(&self, key: u32, value: T) -> Result<(), T> {
        let entries = unsafe {
            // safety: owner-quiescent drain (&mut self) cannot race this owner-thread write;
            // the threads lock serializes only foreign growth/teardown against drain
            &*self.entries.get()
        };
        let cell = entries
            .get(key as usize)
            .expect("instance-local entry exists on owner thread");
        let entry = unsafe {
            // safety: owner-thread access is exclusive for this slot until owner-quiescent drain
            &mut *cell.entry.get()
        }
        .as_mut()
        .expect("instance-local entry exists on owner thread");
        unsafe {
            // safety: the entry belongs to this LocalSlot<T> by id-reuse ordering;
            // owner-thread access is exclusive for this slot
            entry.put::<T>(value)
        }
    }

    fn put_slow<T: Send + 'static>(&self, key: u32, value: T) -> Result<(), T> {
        let entries = unsafe {
            // safety: caller holds Registry::threads, serializing growth, drain, and thread exit
            &mut *self.entries.get()
        };
        ensure_entry_capacity(entries, key);
        let entry = unsafe {
            // safety: caller holds Registry::threads and this owner thread is the sole hot-path writer
            &mut *entries[key as usize].entry.get()
        };
        match entry {
            Some(entry) => unsafe {
                // safety: the entry belongs to this LocalSlot<T> by id-reuse ordering;
                // caller holds Registry::threads
                entry.put::<T>(value)
            },
            None => {
                *entry = Some(Entry::new(value));
                Ok(())
            }
        }
    }

    /// take a slot's entry out for the owner's drain; caller holds
    /// Registry::threads and owner quiescence excludes owner-thread access
    unsafe fn take_entry(&self, key: u32) -> Option<Entry> {
        let entries = unsafe {
            // safety: caller holds Registry::threads, which excludes growth and table drop
            &*self.entries.get()
        };
        let cell = entries.get(key as usize)?;
        unsafe {
            // safety: owner quiescence excludes owner-thread access to this slot during drain
            (&mut *cell.entry.get()).take()
        }
    }
}

impl Drop for ThreadTable {
    fn drop(&mut self) {
        if !self.registered.load(Ordering::Acquire) {
            return;
        }

        let entries = {
            let mut threads = lock(&registry().threads);
            let table = self as *const _;
            let Some(pos) = threads.iter().position(|node| node.table == table) else {
                return;
            };
            threads.swap_remove(pos);
            // take the cells out while holding the lock so no drain can reach a
            // half-torn table; the values are destroyed after the lock releases
            std::mem::take(self.entries.get_mut())
        };
        drop(entries);
    }
}

struct EntryCell {
    entry: UnsafeCell<Option<Entry>>,
}

impl EntryCell {
    fn empty() -> Self {
        Self {
            entry: UnsafeCell::new(None),
        }
    }
}

// safety: accesses to an entry are either owner-thread local or guarded by Registry::threads
unsafe impl Sync for EntryCell {}

/// type-erased cell value backed by a heap `Option<T>` that survives across
/// borrows (take empties it, put refills it; no re-allocation per borrow).
/// the payload pointer is paired with a static drop fn; take/put dereference
/// the payload directly with NO per-operation type check, so the hot path cost
/// is one pointer chase plus an Option take/put — identical to a plain
/// `Option<T>` field.
struct Entry {
    value: *mut (),
    drop_value: unsafe fn(*mut ()),
}

// safety: Entry is created only for T: Send and is moved across threads only for destruction
unsafe impl Send for Entry {}

impl Entry {
    fn new<T: Send + 'static>(value: T) -> Self {
        Self {
            value: Box::into_raw(Box::new(Some(value))).cast(),
            drop_value: drop_slot_cell::<T>,
        }
    }

    /// take the cached value out; caller has exclusive owner-thread access to
    /// this slot's entry (hot path) or holds Registry::threads (drain/teardown)
    unsafe fn take<T: Send + 'static>(&mut self) -> Option<T> {
        let slot = unsafe {
            // safety: Entry::new allocated this pointer as Box<Option<T>>
            &mut *self.value.cast::<Option<T>>()
        };
        slot.take()
    }

    /// put a value back; returns it when the slot is already occupied so the
    /// caller can spill it (reentrant put won the race)
    unsafe fn put<T: Send + 'static>(&mut self, value: T) -> Result<(), T> {
        let slot = unsafe {
            // safety: Entry::new allocated this pointer as Box<Option<T>>
            &mut *self.value.cast::<Option<T>>()
        };
        if slot.is_some() {
            Err(value)
        } else {
            *slot = Some(value);
            Ok(())
        }
    }
}

impl Drop for Entry {
    fn drop(&mut self) {
        unsafe {
            // safety: value and drop_value are paired by Entry::new; Drop runs exactly once
            (self.drop_value)(self.value);
        }
    }
}

/// static destructor for the erased payload (monomorphized per T)
unsafe fn drop_slot_cell<T: Send + 'static>(value: *mut ()) {
    unsafe {
        // safety: Entry::new allocated this pointer as Box<Option<T>>
        drop(Box::from_raw(value.cast::<Option<T>>()));
    }
}

fn ensure_entry_capacity(entries: &mut Vec<EntryCell>, key: u32) {
    let required = key as usize + 1;
    if entries.len() < required {
        entries.resize_with(required, EntryCell::empty);
    }
}

fn registry() -> &'static Registry {
    static REGISTRY: OnceLock<Registry> = OnceLock::new();
    REGISTRY.get_or_init(Registry::new)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock()
}

#[cfg(test)]
static TEST_ALLOC_LOCK: Mutex<()> = Mutex::new(());

/// serializes LocalSlot id allocation across test modules for deterministic
/// slot-id sequencing in the debug/test harness. the registry free list is
/// process-global; production constructors (codec pools, group cursors) now
/// allocate slots in many tests, and the key-reuse test is functional (immune
/// to foreign allocations), so this guard exists only to keep allocation order
/// stable while debugging. tests that construct slots should hold it.
#[cfg(test)]
pub(crate) fn alloc_test_guard() -> MutexGuard<'static, ()> {
    TEST_ALLOC_LOCK.lock()
}

#[cfg(test)]
mod tests {
    use std::{
        cell::Cell,
        panic::{AssertUnwindSafe, catch_unwind},
        sync::{
            Arc, Barrier,
            atomic::{AtomicUsize, Ordering},
            mpsc,
        },
        thread,
    };

    use super::{LocalSlot, alloc_test_guard};

    fn test_lock() -> parking_lot::MutexGuard<'static, ()> {
        alloc_test_guard()
    }

    #[test]
    fn same_thread_slots_do_not_share_values() {
        let _guard = test_lock();
        let slot_a = LocalSlot::new();
        let slot_b = LocalSlot::new();

        assert_eq!(slot_a.take(|| 10), 10);
        slot_a.put(11);
        assert_eq!(slot_b.take(|| 20), 20);
        slot_b.put(21);
        assert_eq!(slot_a.take(|| 0), 11);
        assert_eq!(slot_b.take(|| 0), 21);
    }

    #[test]
    fn each_thread_keeps_a_distinct_value_for_one_slot() {
        let _guard = test_lock();
        let slot = Arc::new(LocalSlot::new());
        let mut workers = Vec::new();
        for initial in [3usize, 7] {
            let slot = Arc::clone(&slot);
            workers.push(thread::spawn(move || {
                let value = slot.take(|| initial);
                slot.put(value + 1);
                let value = slot.take(|| 0);
                slot.put(value);
                value
            }));
        }

        let mut values = workers
            .into_iter()
            .map(|worker| worker.join().expect("worker completed"))
            .collect::<Vec<_>>();
        values.sort_unstable();
        assert_eq!(values, [4, 8]);
    }

    #[test]
    fn drain_clears_long_lived_worker_before_id_reuse() {
        // production constructors (codec pools, ccpool) allocate slots from
        // other tests concurrently, so exact key-sequence reuse is not
        // assertable. the invariant that matters: after an owner drains, a
        // long-lived worker re-touching the slot must see a fresh init value,
        // never the prior owner's residue, across repeated drain/reuse cycles
        let _guard = test_lock();
        let (commands, command_rx) =
            mpsc::channel::<(Arc<LocalSlot<usize>>, usize, mpsc::Sender<usize>)>();
        let worker = thread::spawn(move || {
            while let Ok((slot, expected, done)) = command_rx.recv() {
                // if drain failed to clear the slot, take() would return the old
                // owner's value instead of the fresh init
                let value = slot.take(|| expected);
                assert_eq!(value, expected, "stale value survived drain");
                slot.put(value);
                drop(slot); // release the Arc clone before signalling so the
                // parent can drain via Arc::get_mut
                done.send(value).expect("parent receives result");
            }
        });

        for round in 0..24 {
            // owner A: the long-lived worker caches a value, then A drains
            let mut owner_a = Arc::new(LocalSlot::new());
            let (done, result) = mpsc::channel();
            commands
                .send((Arc::clone(&owner_a), round, done))
                .expect("worker is alive");
            result.recv().expect("worker result");
            Arc::get_mut(&mut owner_a)
                .expect("worker released owner")
                .drain();

            // owner B (may reuse A's freed id): the worker must see a fresh
            // value, never A's residue
            let owner_b = Arc::new(LocalSlot::new());
            let (done2, result2) = mpsc::channel();
            commands
                .send((Arc::clone(&owner_b), round + 1000, done2))
                .expect("worker is alive");
            let v = result2.recv().expect("worker result");
            assert_eq!(v, round + 1000, "reused slot must not expose prior value");
            drop(owner_b);
        }

        drop(commands);
        worker.join().expect("worker completed");
    }

    #[test]
    fn repeated_drain_and_drop_destroy_value_once() {
        let _guard = test_lock();
        let drops = Arc::new(AtomicUsize::new(0));
        let mut slot = LocalSlot::new();
        slot.put(DropProbe(Arc::clone(&drops)));
        slot.drain();
        slot.drain();
        drop(slot);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn drain_before_thread_exit_destroys_value_once() {
        let _guard = test_lock();
        let drops = Arc::new(AtomicUsize::new(0));
        let mut owner = Arc::new(LocalSlot::new());
        let worker_owner = Arc::clone(&owner);
        let worker_drops = Arc::clone(&drops);
        let (ready_tx, ready_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let worker = thread::spawn(move || {
            worker_owner.put(DropProbe(worker_drops));
            drop(worker_owner);
            ready_tx.send(()).expect("parent is waiting");
            release_rx.recv().expect("parent releases worker");
        });

        ready_rx.recv().expect("worker initialized slot");
        Arc::get_mut(&mut owner)
            .expect("worker released owner")
            .drain();
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        release_tx.send(()).expect("worker is alive");
        worker.join().expect("worker completed");
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn thread_exit_before_drain_destroys_value_once() {
        let _guard = test_lock();
        let drops = Arc::new(AtomicUsize::new(0));
        let mut owner = Arc::new(LocalSlot::new());
        let worker_owner = Arc::clone(&owner);
        let worker_drops = Arc::clone(&drops);
        thread::spawn(move || worker_owner.put(DropProbe(worker_drops)))
            .join()
            .expect("worker completed");

        assert_eq!(drops.load(Ordering::Relaxed), 1);
        Arc::get_mut(&mut owner)
            .expect("worker released owner")
            .drain();
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn drain_removes_entry_from_two_live_tables() {
        let _guard = test_lock();
        let drops = Arc::new(AtomicUsize::new(0));
        let mut owner = Arc::new(LocalSlot::new());
        let ready = Arc::new(Barrier::new(3));
        let gate = Arc::new(Barrier::new(3));
        for _ in 0..2 {
            let worker_owner = Arc::clone(&owner);
            let worker_drops = Arc::clone(&drops);
            let ready = Arc::clone(&ready);
            let gate = Arc::clone(&gate);
            thread::spawn(move || {
                worker_owner.put(DropProbe(worker_drops));
                drop(worker_owner);
                ready.wait();
                gate.wait();
            });
        }

        ready.wait();
        Arc::get_mut(&mut owner)
            .expect("workers released owner")
            .drain();
        assert_eq!(drops.load(Ordering::Relaxed), 2);
        gate.wait();
        assert_eq!(drops.load(Ordering::Relaxed), 2);
    }

    #[test]
    #[should_panic(expected = "accessed after drain")]
    fn take_after_drain_panics() {
        let _guard = test_lock();
        let mut slot = LocalSlot::new();
        slot.put(1u64);
        slot.drain();
        let _ = slot.take(|| 0u64);
    }

    #[test]
    fn reentrant_take_and_try_put_keep_one_cached_value() {
        let _guard = test_lock();
        let slot = LocalSlot::new();
        let outer = slot.take(|| 1);
        let inner = slot.take(|| 2);
        assert_eq!(slot.try_put(inner), Ok(()));
        assert_eq!(slot.try_put(outer), Err(1));
        assert_eq!(slot.take(|| 0), 2);
    }

    #[test]
    fn with_puts_value_back_after_unwind() {
        let _guard = test_lock();
        let slot = LocalSlot::new();
        let result = catch_unwind(AssertUnwindSafe(|| {
            slot.with(Vec::new, |value| {
                value.push(1);
                panic!("expected unwind");
            });
        }));
        assert!(result.is_err());
        assert_eq!(slot.take(Vec::new), vec![1]);
    }

    #[test]
    fn local_slot_owner_is_sync_when_value_is_only_send() {
        let _guard = test_lock();
        fn assert_send_sync<T: Send + Sync>() {}

        assert_send_sync::<LocalSlot<SendNotSync>>();
    }

    struct DropProbe(Arc<AtomicUsize>);

    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    struct SendNotSync(#[allow(dead_code)] Cell<u8>);
}
