use std::{
    sync::{
        Arc,
        mpsc::{Receiver, RecvTimeoutError, Sender},
    },
    time::Duration,
};

use crossbeam_epoch::Guard;

use crate::{
    map::{
        SharedState,
        buffer::{Buffers, Loader},
        cache::NodeCache,
        data::Arena,
        table::{PageMap, Swip},
    },
    meta::Numerics,
    types::{
        header::TagFlag,
        node::Junks,
        page::Page,
        refbox::{BoxRef, BoxView},
        traits::{IAlloc, IHeader},
    },
    utils::{Handle, ROOT_PID, data::JUNK_LEN, options::ParsedOptions},
};

pub(crate) struct Evictor {
    opt: Arc<ParsedOptions>,
    cache: Arc<NodeCache>,
    table: Arc<PageMap>,
    numerics: Arc<Numerics>,
    rx: Receiver<SharedState>,
    tx: Sender<()>,
    buffer: Handle<Buffers>,
    frames: Vec<(Handle<Arena>, BoxView)>,
    garbage: Vec<u64>,
}

unsafe impl Send for Evictor {}

impl Evictor {
    pub(crate) fn new(
        opt: Arc<ParsedOptions>,
        cache: Arc<NodeCache>,
        table: Arc<PageMap>,
        numerics: Arc<Numerics>,
        buffer: Handle<Buffers>,
        rx: Receiver<SharedState>,
        tx: Sender<()>,
    ) -> Self {
        Self {
            opt,
            cache,
            table,
            numerics,
            rx,
            tx,
            buffer,
            frames: vec![],
            garbage: vec![],
        }
    }

    fn evict(&mut self, g: &Guard, limit: usize, safe_txid: u64) {
        let pids = self.cache.evict();

        for &pid in &pids {
            let swip = Swip::new(self.table.get(pid));
            // it's passible when a node was unmapped, but not removed from cache yet (concurrently)
            if swip.is_null() {
                continue;
            }
            assert!(!swip.is_tagged());

            let mut compacted = false;
            let old = Page::<Loader>::from_swip(swip.untagged());
            let (addr, junks) = if old.delta_len() > limit {
                let (mut node, j) = old.compact(self, safe_txid);
                node.set_pid(old.pid());
                let addr = node.latest_addr();
                assert_eq!(addr, node.base_addr());
                compacted = true;
                (addr, j)
            } else {
                (old.latest_addr(), Junks::new())
            };

            // NOTE: other threads may perform cas in the same time
            match self.table.cas(pid, old.swip(), Swip::tagged(addr)) {
                Ok(_) => {
                    self.cache.evict_one(pid);
                    if compacted {
                        old.garbage_collect(self, &junks);
                    }
                    // must guarded by EBR, other threads may still use the old page
                    g.defer(move || old.reclaim());
                    self.commit();
                }
                Err(_) => {
                    // it has been replaced by other thread, in this case, we do NOT retry, it will
                    // be evicted next time
                    self.rollback();
                }
            }
        }
    }

    fn evict_all(&mut self, safe_txid: u64, limit: usize) {
        let end = self.table.len();

        for pid in ROOT_PID..end {
            let swip = Swip::new(self.table.get(pid));
            if swip.is_null() || swip.is_tagged() {
                continue;
            }
            let old = Page::<Loader>::from_swip(swip.untagged());
            if self.opt.compact_on_exit || old.delta_len() >= limit {
                let (mut node, junks) = old.compact(self, safe_txid);
                node.set_pid(old.pid());
                old.garbage_collect(self, &junks);
                self.commit();
            }
            old.reclaim();
        }
    }

    fn compact(&mut self, pids: &[u64], g: &Guard, limit: usize, safe_txid: u64) {
        for &pid in pids {
            let swip = Swip::new(self.table.get(pid));
            // it's passible when a node was unmapped, but not removed from cache yet (concurrently)
            if swip.is_null() {
                continue;
            }
            assert!(!swip.is_tagged());
            let old = Page::<Loader>::from_swip(swip.untagged());
            if old.delta_len() > limit {
                let Ok(_lk) = old.try_lock() else {
                    continue;
                };
                let (mut node, junks) = old.compact(self, safe_txid);
                node.set_pid(old.pid());
                let new = Page::new(node);
                // never retry, probability of successful CAS > 70%
                // NOTE: other threads may perform cas in the same time
                match self.table.cas(pid, old.swip(), new.swip()) {
                    Ok(_) => {
                        old.garbage_collect(self, &junks);
                        g.defer(move || old.reclaim());
                        self.commit();
                    }
                    Err(_) => {
                        new.reclaim();
                        self.rollback();
                    }
                }
            }
        }
    }

    fn commit(&mut self) {
        while let Some((a, _)) = self.frames.pop() {
            a.dec_ref();
        }

        let mut garbage: Vec<u64> = Vec::new();
        std::mem::swap(&mut garbage, &mut self.garbage);
        self.buffer.recycle(&garbage, |addr| {
            self.garbage.push(addr);
        });

        if !self.garbage.is_empty() {
            #[cfg(feature = "extra_check")]
            {
                let old = self.garbage.len();
                self.garbage.sort();
                self.garbage.dedup();
                assert_eq!(old, self.garbage.len());
            }
            let sz = self.garbage.len() * JUNK_LEN;
            let (h, mut junk) = self.buffer.alloc(sz as u32).expect("never happen");
            junk.header_mut().flag = TagFlag::Junk;
            let dst = junk.data_slice_mut::<u64>();
            dst.copy_from_slice(&self.garbage);
            self.garbage.clear();
            h.dec_ref();
        }
    }

    fn rollback(&mut self) {
        while let Some((a, b)) = self.frames.pop() {
            let h = b.header();
            a.dealloc(h.addr, h.total_size as usize);
            a.dec_ref();
        }
        self.garbage.clear();
    }

    pub(crate) fn start(self) {
        std::thread::Builder::new()
            .name("evictor".into())
            .spawn(move || {
                evictor_loop(self);
            })
            .expect("can't start evictor thread");
    }
}

impl IAlloc for Evictor {
    fn allocate(&mut self, size: usize) -> BoxRef {
        let (h, b) = self.buffer.alloc(size as u32).expect("never happen");
        self.frames.push((h, b.view()));
        b
    }

    fn arena_size(&mut self) -> usize {
        self.opt.data_file_size
    }

    fn collect(&mut self, addr: &[u64]) {
        self.garbage.extend_from_slice(addr);
    }

    fn inline_size(&self) -> usize {
        self.opt.inline_size
    }
}

fn evictor_loop(mut e: Evictor) {
    const TMO_MS: u64 = 200;
    const COMPACT_TMO: u64 = 5 * TMO_MS;
    let mut compact_cnt = 0;
    let limit = e.opt.max_delta_len();
    loop {
        let r = e.rx.recv_timeout(Duration::from_millis(200));
        let g = crossbeam_epoch::pin();
        let safe_txid = e.numerics.safe_tixd();

        match r {
            Ok(s) => match s {
                SharedState::Quit => {
                    e.evict_all(safe_txid, limit);
                    break;
                }
                SharedState::Evict => {
                    if e.cache.full() {
                        e.evict(&g, limit, safe_txid);
                    }
                }
            },
            Err(RecvTimeoutError::Timeout) => {
                compact_cnt += TMO_MS;
                if e.cache.almost_full() {
                    e.evict(&g, limit, safe_txid);
                } else if compact_cnt >= COMPACT_TMO {
                    let pids = e.cache.compact();
                    e.compact(&pids, &g, limit, safe_txid);
                }
            }
            Err(_) => break,
        }
    }

    let _ = e.tx.send(());
    log::info!("evictor thread exit");
}
