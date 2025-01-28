use crate::utils::next_power_of_2;
use crate::utils::options::Options;
use crate::utils::queue::Queue;
use crate::OpCode;
use dashmap::DashMap;
use rand::Rng;
use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::collections::HashSet;
use std::sync::atomic::{
    AtomicU8,
    Ordering::{Relaxed, Release},
};
use std::sync::Arc;

use super::data::FrameOwner;

#[derive(Clone, Copy, Default)]
#[repr(C, align(64))]
struct Addr(u64);

impl From<u64> for Addr {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

pub const CD_WARM: u8 = 2;
pub const CD_COOL: u8 = 1;
pub const CD_EVICT: u8 = 0;

struct Entry {
    addr: u64,
    data: Option<Arc<FrameOwner>>,
    slot: usize,
    state: AtomicU8,
}

impl Entry {
    fn cool_down(&self) -> Result<u8, OpCode> {
        let old = self.state.load(Relaxed);
        let new = std::cmp::max(CD_EVICT, old.saturating_sub(1));

        match self.state.compare_exchange(old, new, Release, Relaxed) {
            Ok(o) => Ok(o),
            Err(_) => Err(OpCode::Again),
        }
    }

    fn warm_up(&self) {
        let mut old = self.state.load(Relaxed);

        loop {
            let new = std::cmp::min(old + 1, CD_WARM);
            match self.state.compare_exchange(old, new, Release, Relaxed) {
                Ok(_) => break,
                Err(e) => old = e,
            }
        }
    }

    fn reset(&mut self) {
        self.addr = 0;
        self.data.take();
        self.state.store(CD_EVICT, Relaxed);
    }

    fn new(slot: usize) -> Self {
        Self {
            addr: 0,
            data: None,
            slot,
            state: AtomicU8::new(CD_WARM),
        }
    }

    fn set(&mut self, addr: u64, data: Arc<FrameOwner>) {
        self.addr = addr;
        self.data = Some(data);
        self.state.store(CD_WARM, Relaxed);
    }
}

pub(crate) struct Cache {
    slots: *mut Addr,
    /// map disk address to cache slot id
    map: DashMap<u64, Box<Entry>>,
    /// contains removed slot id
    freelist: Queue<Box<Entry>>,
    cap: usize,
    pct: usize,
}

impl Cache {
    pub(crate) fn new(opt: &Options) -> Self {
        let cap = opt.cache_capacity;
        let pct = cap * opt.cache_evict_pct / 100;

        let this = Self {
            slots: unsafe { alloc_zeroed(Layout::array::<Addr>(cap).unwrap()) as *mut Addr },
            map: DashMap::new(),
            freelist: Queue::new(next_power_of_2(cap) as u32, None),
            cap,
            pct,
        };

        for i in 0..this.cap {
            this.freelist
                .push(Box::new(Entry::new(i)))
                .expect("no space");
        }
        this
    }

    fn get_entry(&self) -> Box<Entry> {
        loop {
            if let Ok(e) = self.freelist.pop() {
                break e;
            } else {
                self.try_evict();
            }
        }
    }

    fn write(&self, slot: usize, addr: u64) {
        unsafe {
            self.slots.add(slot).write(addr.into());
        }
    }

    fn read(&self, slot: usize) -> u64 {
        unsafe { self.slots.add(slot).read().0 }
    }

    pub(crate) fn put(&self, addr: u64, data: Arc<FrameOwner>) {
        let mut p = self.get_entry();
        p.set(addr, data);
        self.write(p.slot, addr);
        // this will happen when multiple threads are load data from same addr, we must handle this
        // case to avoid resource leak
        if let Some(old) = self.map.insert(addr, p) {
            self.freelist.push(old).expect("no space");
        }
    }

    /// NOTE: the variables are guarded by lock (from dashmap)
    pub(crate) fn get(&self, addr: u64) -> Option<Arc<FrameOwner>> {
        let item = self.map.get_mut(&addr)?;
        let e = item.value();
        e.warm_up();
        Some(e.data.as_ref().expect("none frame").clone())
    }

    fn try_evict(&self) {
        let cap = self.cap;
        let cnt = self.pct;
        let mut rng = rand::thread_rng();
        let mut set = HashSet::new();

        for _ in 0..cnt {
            // NOTE: same addr may be selected multiple times, since it's random selection
            let slot = rng.gen_range(0..cap);
            let addr = self.read(slot);
            if set.contains(&addr) {
                continue;
            }
            set.insert(addr);

            if let Some((_, v)) = self.map.remove_if_mut(&addr, |k, v| {
                assert_eq!(addr, *k);
                v.cool_down().map_or(CD_EVICT, |x| {
                    if x == CD_COOL {
                        v.reset();
                    }
                    x
                }) == CD_COOL
            }) {
                self.freelist.push(v).expect("no space");
            }
        }
    }
}

impl Drop for Cache {
    fn drop(&mut self) {
        while let Ok(_box) = self.freelist.pop() {}
        unsafe {
            dealloc(
                self.slots as *mut u8,
                Layout::array::<Addr>(self.cap).unwrap(),
            );
        }
    }
}

unsafe impl Send for Cache {}
unsafe impl Sync for Cache {}

#[cfg(test)]
mod test {
    use crate::map::cache::Cache;
    use crate::map::data::{Frame, FrameFlag, FrameOwner};
    use crate::utils::options::Options;
    use crate::RandomPath;
    use std::sync::Arc;

    fn runner() -> bool {
        let path = RandomPath::tmp();
        let mut opt = Options::new(&*path);
        opt.cache_capacity = 256;
        let c = Arc::new(Cache::new(&opt));
        let size = Frame::FRAME_LEN;

        for i in 0..4 {
            let frame = FrameOwner::alloc(size);
            if i == 3 {
                let mut view = frame.view();
                view.init(0, FrameFlag::Unknown);
                view.set_pid(233);
            }
            c.put(i, Arc::new(frame));
            c.get(3);
        }

        let mut v = Vec::new();
        for i in 1..5 {
            let cache = c.clone();
            let b = i * 1000;
            let e = b + 50;
            v.push(std::thread::spawn(move || {
                for j in b..e {
                    cache.get(3);
                    let frame = FrameOwner::alloc(size);
                    frame.view().init(0, FrameFlag::Unknown);
                    cache.put(j, Arc::new(frame));
                }
            }));
        }

        for h in v {
            h.join().unwrap();
        }

        if let Some(b) = c.get(3) {
            assert_eq!(b.page_id(), 233);
            true
        } else {
            false
        }
    }

    #[test]
    fn test_cache() {
        let mut cnt = 0;
        for _ in 0..10 {
            if runner() {
                cnt += 1;
            }
        }
        // low possibility
        assert!(cnt > 0);
    }
}
