use parking_lot::{Condvar, Mutex};
use std::collections::BTreeSet;
use std::mem::MaybeUninit;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering};

use crate::OpCode;
use crate::utils::{NULL_ADDR, ROOT_PID};

const SLOT_SIZE: u64 = 1u64 << 16;

pub struct PageMap {
    l1: Box<Layer1>,
    l2: Box<Layer2>,
    l3: Box<Layer3>,
    next: AtomicU64,
    pub scavenge_cursor: AtomicU64,
    free: Mutex<BTreeSet<u64>>,
}

// currently in stable rust we can't create large object directly using Box, since it will create the
// object on stack, and then copy to heap, for large object, it may cause stack overflow
// https://github.com/rust-lang/rust/issues/53827
/// NOTE: require `T` is zeroable
fn box_new<T>() -> Box<T> {
    use std::alloc;
    let layout = alloc::Layout::new::<T>();
    unsafe {
        let ptr = alloc::alloc_zeroed(layout) as *mut T;
        Box::from_raw(ptr)
    }
}

impl Default for PageMap {
    fn default() -> Self {
        Self {
            l1: box_new(),
            l2: box_new(),
            l3: box_new(),
            next: AtomicU64::new(ROOT_PID),
            scavenge_cursor: AtomicU64::new(ROOT_PID),
            free: Mutex::new(BTreeSet::new()),
        }
    }
}

impl PageMap {
    pub fn map(&self, data: u64) -> Option<u64> {
        let mut lk = self.free.lock();
        if let Some(pid) = lk.pop_first() {
            self.index(pid)
                .compare_exchange(NULL_ADDR, data, Ordering::AcqRel, Ordering::Relaxed)
                .expect("impossible");
            return Some(pid);
        }
        drop(lk);

        let mut pid = self.next.fetch_add(1, Ordering::Relaxed);
        let mut cnt = 0;

        while cnt < MAX_ID {
            match self.index(pid).compare_exchange(
                NULL_ADDR,
                data,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Some(pid),
                Err(_) => pid = self.next.fetch_add(1, Ordering::Relaxed),
            }
            cnt += 1;
        }
        None
    }

    pub fn map_to(&self, pid: u64, data: u64) {
        // pid may have been placed in free list by a rollback; ensure it is not reused concurrently
        let mut lk = self.free.lock();
        lk.remove(&pid);
        drop(lk);

        self.index(pid).store(data, Ordering::Release);
        self.next.fetch_max(pid + 1, Ordering::AcqRel);
    }

    pub fn unmap(&self, pid: u64, addr: u64) -> Result<(), OpCode> {
        self.index(pid)
            .compare_exchange(addr, NULL_ADDR, Ordering::AcqRel, Ordering::Relaxed)
            .map_err(|_| OpCode::Again)?;

        let mut lk = self.free.lock();
        assert!(!lk.contains(&pid));
        lk.insert(pid);
        Ok(())
    }

    pub fn get(&self, pid: u64) -> u64 {
        self.index(pid).load(Ordering::Relaxed)
    }

    pub fn recover(&self, bucket_id: u64, btree: Option<&btree_store::BTree>) {
        let mut next_pid = ROOT_PID + 1;

        if let Some(btree) = btree {
            let bucket_table = crate::meta::page_table_name(bucket_id);
            let _ = btree.view(&bucket_table, |txn| {
                let mut iter = txn.iter();
                let mut k = Vec::new();
                let mut v = Vec::new();
                while iter.next_ref(&mut k, &mut v) {
                    let pid_bytes: [u8; 8] = k[..8]
                        .try_into()
                        .map_err(|_| btree_store::Error::Corruption)?;
                    let addr_bytes: [u8; 8] = v[..8]
                        .try_into()
                        .map_err(|_| btree_store::Error::Corruption)?;
                    let pid = <u64>::from_be_bytes(pid_bytes);
                    let addr = <u64>::from_be_bytes(addr_bytes);

                    self.index(pid)
                        .fetch_max(Swip::tagged(addr), Ordering::Relaxed);
                    next_pid = next_pid.max(pid + 1);
                }
                Ok(())
            });
        }

        self.next.fetch_max(next_pid, Ordering::Relaxed);
        self.scavenge_cursor.store(
            rand::random_range(ROOT_PID..self.next.load(Ordering::Relaxed).max(ROOT_PID + 1)),
            Ordering::Relaxed,
        );

        let mut lk = self.free.lock();
        let max_pid = next_pid;
        for pid in ROOT_PID..max_pid {
            if self.index(pid).load(Ordering::Relaxed) == NULL_ADDR {
                lk.insert(pid);
            }
        }
    }

    pub fn index(&self, pid: u64) -> &AtomicU64 {
        if pid < L3_FANOUT {
            self.l3.index(pid)
        } else if pid < L2_FANOUT {
            // excluding mapping in l3
            self.l2.index(pid - L3_FANOUT)
        } else if pid < L1_FANOUT {
            // excluding mapping in l2
            self.l1.index(pid - L2_FANOUT)
        } else {
            unreachable!()
        }
    }

    /// return `old` on success, `current value` on error
    pub fn cas(&self, pid: u64, old: u64, new: u64) -> Result<u64, u64> {
        self.index(pid)
            .compare_exchange(old, new, Ordering::AcqRel, Ordering::Relaxed)
    }

    pub fn len(&self) -> u64 {
        self.next.load(Ordering::Acquire)
    }
}

impl Drop for PageMap {
    fn drop(&mut self) {}
}

struct Layer3([AtomicU64; SLOT_SIZE as usize]);

impl Default for Layer3 {
    fn default() -> Self {
        Self(unsafe { MaybeUninit::zeroed().assume_init() })
    }
}

impl Layer3 {
    fn index(&self, pos: u64) -> &AtomicU64 {
        &self.0[pos as usize]
    }
}

macro_rules! build_layer {
    ($layer:ident, $indirect:ty, $fanout:expr) => {
        struct $layer([AtomicPtr<$indirect>; SLOT_SIZE as usize]);

        impl Default for $layer {
            fn default() -> Self {
                Self(unsafe { MaybeUninit::zeroed().assume_init() })
            }
        }

        impl Drop for $layer {
            fn drop(&mut self) {
                for indirect in &self.0 {
                    let ptr = indirect.load(Ordering::Relaxed);
                    if !ptr.is_null() {
                        unsafe {
                            drop(Box::from_raw(ptr));
                        }
                    }
                }
            }
        }

        impl $layer {
            fn index(&self, pos: u64) -> &AtomicU64 {
                let r = pos / $fanout;
                let c = pos % $fanout;
                let layer = self.0[r as usize].load(Ordering::Acquire);
                let layer = unsafe { layer.as_ref().unwrap_or_else(|| self.get(r as usize)) };
                layer.index(c)
            }

            fn get(&self, pos: usize) -> &$indirect {
                let mut new = Box::into_raw(box_new());

                if let Err(curr) = self.0[pos].compare_exchange(
                    null_mut(),
                    new,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    unsafe {
                        drop(Box::from_raw(new));
                    }
                    new = curr;
                }
                unsafe { &*new }
            }
        }
    };
}
const L3_FANOUT: u64 = SLOT_SIZE;
const L2_FANOUT: u64 = L3_FANOUT * SLOT_SIZE;
const L1_FANOUT: u64 = L2_FANOUT * SLOT_SIZE;

const MAX_ID: u64 = L1_FANOUT - 1;

build_layer!(Layer2, Layer3, L3_FANOUT);
build_layer!(Layer1, Layer2, L2_FANOUT);

#[derive(Clone, Copy)]
pub(crate) struct Swip(u64);

impl Swip {
    pub(crate) const TAG: u64 = 1 << 63;
    pub(crate) fn new(x: u64) -> Self {
        Self(x)
    }

    pub(crate) fn is_null(&self) -> bool {
        self.0 == NULL_ADDR
    }

    pub(crate) fn is_tagged(&self) -> bool {
        self.0 & Self::TAG != 0
    }

    pub(crate) fn untagged(&self) -> u64 {
        self.0 & !Self::TAG
    }

    pub(crate) fn raw(&self) -> u64 {
        self.0
    }

    pub(crate) fn tagged(x: u64) -> u64 {
        x | Self::TAG
    }
}

pub(crate) struct BucketState {
    pub(crate) txn_ref: AtomicUsize,
    pub(crate) next_addr: AtomicU64,
    pub(crate) is_deleting: AtomicBool,
    pub(crate) is_drop: AtomicBool,
    pub(crate) vacuum_epoch: AtomicU64,
    pub(crate) vacuum_inflight: AtomicBool,
    pub(crate) vacuum_lock: Mutex<()>,
    pub(crate) vacuum_cv: Condvar,
}

impl BucketState {
    pub(crate) fn new() -> Self {
        Self {
            txn_ref: AtomicUsize::new(0),
            is_deleting: AtomicBool::new(false),
            is_drop: AtomicBool::new(false),
            vacuum_epoch: AtomicU64::new(0),
            vacuum_inflight: AtomicBool::new(false),
            vacuum_lock: Mutex::new(()),
            vacuum_cv: Condvar::new(),
            next_addr: AtomicU64::new(crate::utils::INIT_ADDR),
        }
    }

    #[inline]
    pub(crate) fn inc_txn_ref(&self) {
        self.txn_ref.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn dec_txn_ref(&self) {
        self.txn_ref.fetch_sub(1, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn set_deleting(&self) {
        self.is_deleting.store(true, Ordering::Relaxed);
    }

    pub(crate) fn set_drop(&self) {
        self.is_drop.store(true, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn is_deleting(&self) -> bool {
        self.is_deleting.load(Ordering::Relaxed)
    }

    pub(crate) fn is_drop(&self) -> bool {
        self.is_drop.load(Ordering::Relaxed)
    }

    pub(crate) fn is_vacuuming(&self) -> bool {
        self.vacuum_inflight.load(Ordering::Acquire)
    }

    pub(crate) fn vacuum_epoch(&self) -> u64 {
        self.vacuum_epoch.load(Ordering::Acquire)
    }

    pub(crate) fn try_begin_vacuum(&self) -> bool {
        self.vacuum_inflight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
    }

    pub(crate) fn end_vacuum(&self) {
        let _guard = self.vacuum_lock.lock();
        self.vacuum_epoch.fetch_add(1, Ordering::AcqRel);
        self.vacuum_inflight.store(false, Ordering::Release);
        self.vacuum_cv.notify_all();
    }

    pub(crate) fn wait_vacuum(&self, epoch: u64) {
        if self.vacuum_epoch.load(Ordering::Acquire) != epoch {
            return;
        }
        let mut guard = self.vacuum_lock.lock();
        while self.vacuum_epoch.load(Ordering::Acquire) == epoch {
            self.vacuum_cv.wait(&mut guard);
        }
    }

    pub(crate) fn is_busy(&self) -> bool {
        self.txn_ref.load(Ordering::Relaxed) > 0
    }

    pub(crate) fn reserve_addr_span(&self, span: u64) -> u64 {
        self.next_addr.fetch_add(span, Ordering::AcqRel)
    }
}

#[cfg(test)]
mod test {
    use crate::{
        map::table::{L1_FANOUT, L2_FANOUT, L3_FANOUT, PageMap},
        utils::NULL_ADDR,
    };

    fn addr(a: u64) -> u64 {
        a * 2
    }

    #[test]
    fn test_page_map() {
        let table = PageMap::default();
        let pids = vec![
            0,
            1,
            L3_FANOUT - 1,
            L3_FANOUT,
            L3_FANOUT + 1,
            L2_FANOUT - 1,
            L2_FANOUT,
            L2_FANOUT + 1,
            L1_FANOUT - 1,
        ];
        let mut mapped_pid = vec![];

        for &i in &pids {
            table
                .index(i)
                .store(addr(i), std::sync::atomic::Ordering::Relaxed);
            mapped_pid.push(i);
            assert_eq!(table.get(i), addr(i));
        }

        for (idx, pid) in mapped_pid.iter().enumerate() {
            table.unmap(*pid, addr(pids[idx])).unwrap();
            assert_eq!(table.get(*pid), NULL_ADDR);
        }

        assert_eq!(pids.len(), mapped_pid.len());
    }
}
