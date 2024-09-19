use std::alloc;
use std::cmp::min;
use std::mem::MaybeUninit;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};

const SLOT_SIZE: u64 = 1u64 << 16;
const INIT_ID: u64 = 0;

/// NOTE: zero addr indicate empty
pub struct PageMap {
    l1: Box<Layer1>,
    l2: Box<Layer2>,
    l3: Box<Layer3>,
    next: AtomicU64,
}

// currently in stable rust we can't create large object directly using Box, since it will create the
// object on stack, and then copy to heap, for large object, it may cause stack overflow
// https://github.com/rust-lang/rust/issues/53827
/// NOTE: require `T` is zeroable
fn box_new<T>() -> Box<T> {
    unsafe {
        let ptr = alloc::alloc(alloc::Layout::new::<T>()) as *mut T;
        Box::from_raw(ptr)
    }
}

impl Default for PageMap {
    fn default() -> Self {
        Self {
            l1: box_new(),
            l2: box_new(),
            l3: box_new(),
            next: AtomicU64::new(INIT_ID),
        }
    }
}

impl PageMap {
    /// return page id on success
    pub fn map(&mut self, addr: u64) -> Option<u64> {
        let mut pid = self.next.load(Ordering::Acquire);
        let mut cnt = 0;

        while cnt < MAX_ID {
            match self.index(pid).compare_exchange(
                INIT_ID,
                addr,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(x) => {
                    assert_eq!(x, INIT_ID);
                    self.next.fetch_add(1, Ordering::Release);
                    break;
                }
                Err(cur) => {
                    // here the `next` may not equal to `pid`
                    let new = if pid + 1 == MAX_ID { INIT_ID } else { pid + 1 };
                    match self
                        .next
                        .compare_exchange(pid, new, Ordering::AcqRel, Ordering::Acquire)
                    {
                        Ok(x) => pid = new,
                        Err(x) => pid = x,
                    }
                }
            }
            cnt += 1;
        }

        if cnt == MAX_ID {
            None
        } else {
            Some(pid)
        }
    }

    pub fn unmap(&mut self, mut pid: u64) {
        self.index(pid).store(INIT_ID, Ordering::Release);
        loop {
            let curr = self.next.load(Ordering::Relaxed);
            let smaller = min(pid, curr);
            match self
                .next
                .compare_exchange(curr, smaller, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(x) => pid = x,
            }
        }
    }

    pub fn get(&self, pid: u64) -> u64 {
        self.index(pid).load(Ordering::Acquire)
    }

    fn index(&self, pid: u64) -> &AtomicU64 {
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
        /// NOTE: `SLOT_SIZE - 1` since we extract one as standalone layer in `PageMap`
        struct $layer([AtomicPtr<$indirect>; SLOT_SIZE as usize - 1]);

        impl Default for $layer {
            fn default() -> Self {
                Self(unsafe { MaybeUninit::zeroed().assume_init() })
            }
        }

        impl Drop for $layer {
            fn drop(&mut self) {
                for indirect in &self.0 {
                    let ptr = indirect.load(Ordering::Acquire);
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
                let layer = self.0[r as usize].load(Ordering::Relaxed);
                let layer = unsafe { layer.as_ref().unwrap_or_else(|| self.get(r as usize)) };
                layer.index(c)
            }

            fn get(&self, pos: usize) -> &$indirect {
                let mut new = Box::into_raw(box_new());

                if let Err(curr) = self.0[pos].compare_exchange(
                    null_mut(),
                    new,
                    Ordering::AcqRel,
                    Ordering::Acquire,
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

build_layer!(Layer2, Layer3, L2_FANOUT);
build_layer!(Layer1, Layer2, L1_FANOUT);

#[cfg(test)]
mod tests {
    use crate::map::page_map::{Layer1, PageMap, INIT_ID, L1_FANOUT, L2_FANOUT, L3_FANOUT};

    #[test]
    fn test_page_map() {
        let mut table = PageMap::default();
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
            L1_FANOUT,
            L1_FANOUT + 1,
        ];

        let mut mapped_pid = vec![];

        for i in &pids {
            let pid = table.map(*i * 2).unwrap();
            mapped_pid.push(pid);
            assert_eq!(table.get(pid), *i * 2);
        }

        for i in &mapped_pid {
            table.unmap(*i);
            assert_eq!(table.get(*i), INIT_ID);
        }

        assert_eq!(pids.len(), mapped_pid.len());
    }
}
