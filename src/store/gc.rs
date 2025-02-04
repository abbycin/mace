use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::Ordering::Relaxed,
        mpsc::{channel, Receiver, RecvTimeoutError, Sender},
        Arc,
    },
    thread::JoinHandle,
    time::Duration,
};

use io::{File, SeekableGatherIO};

use crate::{
    cc::wal::{ptr_to, WalCheckpoint},
    index::{page::PageHeader, registry::Registry},
    map::data::DataLoader,
    utils::{
        countblock::Countblock, data::Meta, pack_id, unpack_id, NEXT_ID, NULL_ID, NULL_ORACLE,
        NULL_PID,
    },
    OpCode,
};

const GC_QUIT: i32 = -1;
const GC_PAUSE: i32 = 3;
const GC_RESUME: i32 = 5;

fn gc_thread(mut gc: GarbageCollector, rx: Receiver<i32>, sem: Arc<Countblock>) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("garbage_collector".into())
        .spawn(move || {
            let timeout = Duration::from_millis(gc.mgr.store.opt.gc_timeout);
            let mut pause = false;

            loop {
                match rx.recv_timeout(timeout) {
                    Ok(x) => match x {
                        GC_PAUSE => {
                            pause = true;
                            sem.post();
                        }
                        GC_RESUME => {
                            pause = false;
                            sem.post();
                        }
                        GC_QUIT => break,
                        _ => unreachable!("invalid instruction  {}", x),
                    },
                    Err(RecvTimeoutError::Timeout) => {
                        if !pause {
                            gc.process_data();
                            gc.process_wal();
                        }
                    }
                    Err(e) => {
                        log::error!("gc receive error {}", e);
                        break;
                    }
                }
            }

            sem.post();
        })
        .unwrap()
}

#[derive(Clone)]
pub(crate) struct Handle {
    tx: Arc<Sender<i32>>,
    sem: Arc<Countblock>,
}

impl Handle {
    pub(crate) fn quit(&self) {
        self.tx.send(GC_QUIT).unwrap();
        self.sem.wait();
    }

    pub(crate) fn pause(&self) {
        self.tx.send(GC_PAUSE).unwrap();
        self.sem.wait();
    }

    pub(crate) fn resume(&self) {
        self.tx.send(GC_RESUME).unwrap();
        self.sem.wait();
    }
}

pub(crate) fn start_gc(mgr: Registry, meta: Arc<Meta>) -> Handle {
    let (tx, rx) = channel();
    let sem = Arc::new(Countblock::new(0));
    let (id, off) = unpack_id(meta.next_gc.load(Relaxed));
    let last_data = meta.flushed.load(Relaxed);
    let last_ckpt = meta.ckpt.load(Relaxed);
    let gc = GarbageCollector {
        meta,
        mgr,
        next_id: id,
        next_pos: off,
        last_data,
        force_move: 0,
        last_ckpt,
        invalid: Vec::new(),
        moved: VecDeque::new(),
        junks: VecDeque::new(),
    };
    gc_thread(gc, rx, sem.clone());
    Handle {
        tx: Arc::new(tx),
        sem,
    }
}

#[derive(Default)]
struct Segment {
    id: u16,
    pos: u64,
    map: HashMap<u64, u64>,
    total: usize,
}

impl Segment {
    fn should_clean(&self, ratio: u8) -> bool {
        let percent = (self.map.len() * 100 / self.total) as u8;
        percent <= ratio
    }
}

/// almost all garbages are produced by consolidation, on the other hand, consolidation makes arena
/// more denser and will more the old delta from old arena to the recent one
///
/// BTW, we can significantly reduce the flush size by mark frames to `TombStone`` on successfully
/// conslidated
/// but this will face the race condition: one thread successfully consolidated while other threads
/// still perform lookup or consolidate on the same node, they will trying to load frame from file
/// with the old offset already be marked as `TombStone` that not flushed to file
/// this problem can be solved, but it's very complicated or inefficient, we will solve it later
///
/// NOTE: current implementation is very naive!
struct GarbageCollector {
    meta: Arc<Meta>,
    mgr: Registry,
    next_id: u16,
    next_pos: u64,
    last_data: u64,
    force_move: u32,
    last_ckpt: u64,
    invalid: Vec<u64>,
    moved: VecDeque<u64>,
    junks: VecDeque<Segment>,
}

impl GarbageCollector {
    fn process_data(&mut self) {
        // load mapping from data file and filter out the clean data
        self.load_junk();
        // trigger consolidate, also filter out the clean data
        self.trigger();
        // check if all cosolidates were completed
        self.check();
        // if clean complete, rolling forward
        self.roll();
    }

    fn process_wal(&mut self) {
        let ckpt = self.meta.ckpt.load(Relaxed);
        if self.last_ckpt == ckpt {
            return;
        }
        self.last_ckpt = ckpt;

        let mut oldest_txid = NULL_ORACLE;
        let mut oldest_ckpt = NULL_ORACLE;
        for w in self.mgr.store.context.workers().iter() {
            let ckpt = w.ckpt.load(Relaxed);
            let txid = w.tx_id.load(Relaxed);

            if txid < oldest_txid {
                oldest_txid = txid;
                oldest_ckpt = ckpt;
            }
        }

        let opt = self.mgr.store.opt.clone();
        let mut buf = [0u8; size_of::<WalCheckpoint>()];
        let (cur_id, _) = unpack_id(oldest_ckpt);
        let mut end = None;
        let mut beg = cur_id;

        let null = pack_id(NEXT_ID, 0);
        while oldest_ckpt != null {
            let (id, pos) = unpack_id(oldest_ckpt);
            let path = opt.wal_file(id);
            if !path.exists() {
                break;
            }
            let f = File::options().read(true).open(&path).unwrap();

            f.read(&mut buf, pos).unwrap();
            let c = ptr_to::<WalCheckpoint>(buf.as_ptr());
            oldest_ckpt = c.prev_addr;

            let (oldest_id, _) = unpack_id(oldest_ckpt);

            if oldest_id != cur_id {
                if end.is_none() {
                    end = Some(oldest_id);
                }
                beg = oldest_id;
            }
        }

        if end.is_some() {
            let end = end.unwrap();
            for i in beg..=end {
                let from = opt.wal_file(i);
                if !from.exists() {
                    continue;
                }
                let to = opt.wal_backup(i);
                std::fs::rename(&from, &to)
                    .inspect_err(|e| {
                        log::error!("can't rename {:?} to {:?}, error {:?}", from, to, e);
                    })
                    .unwrap();
            }
        }
    }

    fn load_junk(&mut self) {
        let Some(mut loader) = self.get_loader() else {
            return;
        };

        let meta = loader.get_meta().unwrap();

        let mut seg = Segment::default();
        meta.maps().iter().for_each(|x| {
            seg.total += 1;
            Self::filter_cleaned(&self.mgr, &mut seg.map, x.page_id(), x.page_addr());
        });
        self.next_pos = loader.offset();

        seg.id = self.next_id;
        seg.pos = self.next_pos;

        self.junks.push_back(seg);
        self.force_move = self.force_move.saturating_add(1);
    }

    fn trigger(&mut self) {
        let ratio = self.mgr.store.opt.gc_ratio;
        let force_move = self.mgr.store.opt.buffer_count;

        if let Some(seg) = self.junks.front_mut() {
            if seg.should_clean(ratio) {
                self.force_move = self.force_move.wrapping_sub(1);
                Self::move_data(&self.mgr, &mut self.invalid, seg);
            }
            if self.force_move == force_move {
                self.force_move = 0;
                Self::move_data(&self.mgr, &mut self.invalid, seg);
            }
        }

        for (i, seg) in self.junks.iter_mut().enumerate() {
            if i > 0 && seg.should_clean(ratio) {
                Self::move_data(&self.mgr, &mut self.invalid, seg);
            }
        }
    }

    fn check(&mut self) {
        for seg in self.junks.iter_mut() {
            let mut map = HashMap::new();
            std::mem::swap(&mut map, &mut seg.map);
            assert_eq!(seg.map.len(), 0);
            for (pid, addr) in map {
                Self::filter_cleaned(&self.mgr, &mut seg.map, pid, addr);
            }
        }
    }

    fn roll(&mut self) {
        while let Some(seg) = self.junks.pop_front() {
            if !seg.map.is_empty() {
                self.junks.push_front(seg);
                break;
            }
            log::info!(
                "===> roll, rest {} total {} gc {:?}",
                seg.map.len(),
                seg.total,
                (seg.id, seg.pos)
            );
            self.moved.push_back(pack_id(seg.id, seg.pos));
        }

        // advance the gc data when there are at least two seg was cleaned, since the delta chain
        // may cross two arenas
        if self.moved.len() > 1 {
            let mut x = 0;
            while self.moved.len() > 1 {
                x = self.moved.pop_front().unwrap();
            }
            let old = self.meta.next_gc.load(Relaxed);
            let new = x;
            self.meta.next_gc.store(x, Relaxed);
            self.meta.sync(self.mgr.store.opt.meta_file(), false);

            let (old_id, _) = unpack_id(old);
            let (new_id, _) = unpack_id(new);

            if old_id != new_id {
                if new_id > old_id {
                    self.remove_data_file(old_id, new_id);
                } else {
                    self.remove_data_file(old_id, NULL_ID);
                    self.remove_data_file(NEXT_ID, new_id);
                }
            }
        }
    }

    fn remove_data_file(&self, from: u16, to: u16) {
        for i in from..to {
            let path = self.mgr.store.opt.data_file(i);
            let e = std::fs::remove_file(&path);
            if e.is_err() {
                log::info!("unlink junk {:?}, error {}", path, e.err().unwrap());
            } else {
                log::info!("unlink junk {:?} ok", path);
            }
        }
    }

    fn move_data(mgr: &Registry, invalid: &mut Vec<u64>, seg: &mut Segment) {
        for (pid, addr) in seg.map.iter() {
            let f = mgr.store.buffer.load(*addr);
            let h: PageHeader = f.payload().into();
            drop(f);

            let tree = mgr.search(h.tree_id);
            if let Some(t) = tree {
                let _ = t.force_consolidate(*pid);
            } else {
                log::error!("can't find tree {}", h.tree_id);
                // node of removed tree, it's already clean
                invalid.push(*pid);
            }
        }
        while let Some(x) = invalid.pop() {
            seg.map.remove(&x);
        }
    }

    /// load the latest smallest addr point by pid and then compare with the record addr
    fn filter_cleaned(mgr: &Registry, map: &mut HashMap<u64, u64>, pid: u64, addr: u64) {
        let mut cur = mgr.store.page.index(pid).load(Relaxed);
        if cur == NULL_PID {
            return;
        }

        assert_ne!(cur, 0);

        let mut oldest = 0;
        // here we traverse the delta chain, since the base page addr is not saved in header, we'll
        // do that later (maybe)
        while cur != 0 {
            oldest = cur;
            let f = mgr.store.buffer.load(cur);
            let hdr: PageHeader = f.payload().into();
            cur = hdr.link();
        }
        let (record_id, record_pos) = unpack_id(addr);
        let (oldest_id, oldest_pos) = unpack_id(oldest);

        let moved = if record_id != oldest_id {
            true
        } else {
            oldest_pos > record_pos
        };

        if !moved {
            map.insert(pid, addr);
        }
    }

    fn get_loader(&mut self) -> Option<DataLoader> {
        let flush_data = self.meta.flushed.load(Relaxed);

        // if no arena was flushed, junks will be consumed to the reach such a balance that: no more
        // insert and no more gc
        if self.last_data == flush_data {
            return None;
        }

        self.last_data = flush_data;

        loop {
            let path = self.mgr.store.opt.data_file(self.next_id);
            if !path.exists() {
                return None;
            }
            match DataLoader::new(path, self.next_pos) {
                Err(OpCode::NoSpace) => {
                    self.next_id = self.next_id.wrapping_add(1);
                    if self.next_id == 0 {
                        self.next_id = NEXT_ID;
                    }
                    self.next_pos = 0;
                }
                Err(_) => return None,
                Ok(l) => return Some(l),
            }
        }
    }
}
