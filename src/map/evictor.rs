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
    types::{
        header::TagFlag,
        page::Page,
        refbox::{BoxRef, BoxView},
        traits::{IAlloc, IHeader, IInlineSize},
    },
    utils::{
        Handle,
        data::{JUNK_LEN, Meta},
        options::ParsedOptions,
    },
};

pub(crate) struct Evictor {
    opt: Arc<ParsedOptions>,
    cache: Arc<NodeCache>,
    table: Arc<PageMap>,
    meta: Arc<Meta>,
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
        meta: Arc<Meta>,
        buffer: Handle<Buffers>,
        rx: Receiver<SharedState>,
        tx: Sender<()>,
    ) -> Self {
        Self {
            opt,
            cache,
            table,
            meta,
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
            assert!(!swip.is_null());
            assert!(!swip.is_tagged());

            let old = Page::<Loader>::from_swip(swip.untagged());
            if old.delta_len() > limit {
                let mut node = old.compact(self, safe_txid, true);
                node.set_pid(old.pid());
                let addr = node.latest_addr();
                assert_eq!(addr, node.base_addr());

                match self.table.cas(pid, old.swip(), Swip::tagged(addr)) {
                    Ok(_) => {
                        old.garbage_collect(self);
                        g.defer(move || old.reclaim());
                        g.flush();
                        self.commit();
                    }
                    Err(_) => {
                        // it has been load from disk just after being evict from cache, in this case
                        // we do NOT retry, it will be compacted next time either here or somewhere
                        self.rollback();
                    }
                }
            }
        }
    }

    fn evict_all(&mut self, safe_txid: u64) {
        let mut pids = Vec::new();
        self.cache.reclaim(|x| pids.push(x));

        for pid in pids {
            let swip = Swip::new(self.table.get(pid));
            if swip.is_null() || swip.is_tagged() {
                continue;
            }
            let old = Page::<Loader>::from_swip(swip.untagged());
            let mut node = old.compact(self, safe_txid, true);
            node.set_pid(old.pid());
            old.reclaim();
            self.commit();
        }
    }

    fn compact(&mut self, pids: &[u64], g: &Guard, limit: usize, safe_txid: u64) {
        for &pid in pids {
            let swip = Swip::new(self.table.get(pid));
            assert!(!swip.is_null());
            assert!(!swip.is_tagged());
            let old = Page::<Loader>::from_swip(swip.untagged());
            if old.delta_len() > limit {
                let Ok(_lk) = old.try_lock() else {
                    continue;
                };
                let mut node = old.compact(self, safe_txid, true);
                node.set_pid(old.pid());
                let new = Page::new(node);
                // never retry
                // probability of successful CAS > 70%
                match self.table.cas(pid, old.swip(), new.swip()) {
                    Ok(_) => {
                        old.garbage_collect(self);
                        g.defer(move || old.reclaim());
                        g.flush();
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
        self.buffer.tombstone_active(&garbage, |addr| {
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

    fn arena_size(&mut self) -> u32 {
        self.opt.data_file_size
    }

    fn collect(&mut self, addr: &[u64]) {
        self.garbage.extend_from_slice(addr);
    }
}

impl IInlineSize for Evictor {
    fn inline_size(&self) -> u32 {
        self.opt.max_inline_size
    }
}

fn evictor_loop(mut e: Evictor) {
    loop {
        let r = e.rx.recv_timeout(Duration::from_millis(200));
        let g = crossbeam_epoch::pin();
        let limit = e.opt.split_elems as usize / 4;
        let safe_txid = e.meta.safe_tixd();

        {}

        match r {
            Ok(s) => match s {
                SharedState::Quit => {
                    e.evict_all(safe_txid);
                    break;
                }
                SharedState::Evict => {
                    e.evict(&g, limit, safe_txid);
                }
            },
            Err(RecvTimeoutError::Timeout) => {
                if e.cache.almost_full() {
                    e.evict(&g, limit, safe_txid);
                } else {
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
