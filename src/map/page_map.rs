use std::cmp::min;
use std::fs::File;
use std::mem::MaybeUninit;
use std::path::{Path, PathBuf};
use std::ptr::null_mut;
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};
use std::sync::Arc;

use crate::utils::data::PageTable;
use crate::utils::{NULL_PID, ROOT_PID};
use crate::{OpCode, Options};

const SLOT_SIZE: u64 = 1u64 << 16;

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
    use std::alloc;
    unsafe {
        let ptr = alloc::alloc_zeroed(alloc::Layout::new::<T>()) as *mut T;
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
        }
    }
}

impl PageMap {
    /// alloc a pid with value euqal to pid
    pub fn alloc(&self) -> Option<u64> {
        let mut pid = self.next.load(Ordering::Relaxed);
        let mut cnt = 0;

        while cnt < MAX_ID {
            match self.index(pid).compare_exchange(
                NULL_PID,
                pid,
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

    pub fn map(&self, addr: u64) -> Option<u64> {
        let mut pid = self.next.fetch_add(1, Ordering::Release);
        let mut cnt = 0;

        while cnt < MAX_ID {
            match self.index(pid).compare_exchange(
                NULL_PID,
                addr,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(x) => {
                    assert_eq!(x, NULL_PID);
                    break;
                }
                Err(_) => {
                    pid = self.next.fetch_add(1, Ordering::Release);
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

    pub fn unmap(&self, pid: u64, addr: u64) {
        self.index(pid)
            .compare_exchange(addr, NULL_PID, Ordering::AcqRel, Ordering::Relaxed)
            .expect("can't fail");
        let mut curr = self.next.load(Ordering::Relaxed);
        loop {
            let smaller = min(pid, curr);
            match self
                .next
                .compare_exchange(curr, smaller, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(x) => curr = x,
            }
        }
    }

    pub fn get(&self, pid: u64) -> u64 {
        self.index(pid).load(Ordering::Relaxed)
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

build_layer!(Layer2, Layer3, L2_FANOUT);
build_layer!(Layer1, Layer2, L1_FANOUT);

impl PageMap {
    fn load_map(this: &Self, name: &PathBuf) {
        let mut f = match File::options().read(true).open(name) {
            Ok(f) => f,
            Err(e) => {
                log::error!("{}", e);
                panic!("can't open file {}", e);
            }
        };

        PageTable::deserialize(&mut f, |e| {
            let (pid, addr) = (e.page_id(), e.page_addr());
            // NOTE: it's correct iff when file_id is not wrapping
            if this.get(pid) < addr {
                this.index(pid).store(addr, Ordering::Relaxed);
            }
        });
    }

    pub fn rebuild(this: &Self, opt: &Arc<Options>) -> Result<(), OpCode> {
        let dir = std::fs::read_dir(&opt.db_root).map_err(|_| OpCode::IoError)?;
        for i in dir {
            let filename = i.map_err(|_| OpCode::IoError)?.file_name();
            let name = filename.to_str().ok_or(OpCode::Unknown)?;
            if name.starts_with(Options::MAP_PREFIX) {
                let path = Path::new(&opt.db_root).join(name);
                Self::load_map(this, &path);
            }
        }
        Ok(())
    }

    pub fn new(opt: Arc<Options>) -> Result<Self, OpCode> {
        let this = Self::default();
        Self::rebuild(&this, &opt)?;
        Ok(this)
    }
}

#[cfg(test)]
mod test {
    use crate::map::page_map::{PageMap, L1_FANOUT, L2_FANOUT, L3_FANOUT, NULL_PID};

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
            L1_FANOUT,
            L1_FANOUT + 1,
        ];
        let mut mapped_pid = vec![];

        for i in &pids {
            let pid = table.map(addr(*i)).unwrap();
            mapped_pid.push(pid);
            assert_eq!(table.get(pid), addr(*i));
        }

        for (idx, pid) in mapped_pid.iter().enumerate() {
            table.unmap(*pid, addr(pids[idx]));
            assert_eq!(table.get(*pid), NULL_PID);
        }

        assert_eq!(pids.len(), mapped_pid.len());
    }
}
