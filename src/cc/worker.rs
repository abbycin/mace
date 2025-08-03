use std::{
    ops::{Deref, DerefMut},
    ptr::null_mut,
    sync::{
        Arc,
        atomic::{
            AtomicU64, AtomicUsize,
            Ordering::{Relaxed, Release},
        },
    },
};

use super::{cc::ConcurrencyControl, context::Context, log::Logging, wal::WalReader};
use crate::utils::unpack_id;
use crate::utils::{
    Handle,
    data::{Meta, WalDescHandle},
};
use crate::{cc::wal::Location, utils::options::ParsedOptions};
use crate::{index::tree::Tree, map::buffer::Buffers, utils::block::Block};
use crossbeam_epoch::Guard;

pub struct Worker {
    pub cc: ConcurrencyControl,
    pub ckpt_cnt: Arc<AtomicUsize>,
    pub tx_id: AtomicU64,
    // a copy of tx_id not shared among threads
    pub start_ts: u64,
    pub id: u16,
    pub logging: Logging,
    pub modified: bool,
}

impl Worker {
    fn new(
        desc: WalDescHandle,
        meta: Arc<Meta>,
        opt: Arc<ParsedOptions>,
        buffer: Handle<Buffers>,
    ) -> Self {
        let cnt = Arc::new(AtomicUsize::new(0));
        Self {
            cc: ConcurrencyControl::new(opt.workers),
            ckpt_cnt: cnt.clone(),
            tx_id: AtomicU64::new(0),
            start_ts: 0,
            id: desc.worker,
            logging: Logging::new(cnt, desc, meta, opt, buffer),
            modified: false,
        }
    }
}

#[derive(Clone, Copy)]
pub struct SyncWorker {
    w: *mut Worker,
}

unsafe impl Send for SyncWorker {}
unsafe impl Sync for SyncWorker {}

impl Default for SyncWorker {
    fn default() -> Self {
        Self { w: null_mut() }
    }
}

impl SyncWorker {
    pub fn new(
        desc: WalDescHandle,
        meta: Arc<Meta>,
        opt: Arc<ParsedOptions>,
        buffer: Handle<Buffers>,
    ) -> Self {
        let w = Box::new(Worker::new(desc, meta, opt, buffer));
        Self {
            w: Box::into_raw(w),
        }
    }

    pub fn reclaim(&self) {
        unsafe { drop(Box::from_raw(self.w)) };
    }

    fn init(&mut self, ctx: &Context, start_ts: u64) {
        let id = self.id;
        self.ckpt_cnt.store(0, Relaxed);
        self.tx_id.store(start_ts, Relaxed);
        self.start_ts = start_ts;
        self.cc.global_wmk_tx.store(ctx.wmk_oldest(), Relaxed);
        self.cc.commit_tree.compact(ctx, id);
    }

    pub(crate) fn view(&mut self, ctx: &Context) {
        self.init(ctx, ctx.load_oracle());
    }

    pub(crate) fn begin(&mut self, ctx: &Context) -> u64 {
        let start_ts = ctx.alloc_oracle();
        self.init(ctx, start_ts);
        self.logging.record_begin(start_ts);
        start_ts
    }

    pub(crate) fn commit(&self, ctx: &Context) {
        let mut ms = *self;
        let w = ms.deref_mut();
        let txid = w.tx_id.load(Relaxed);

        if !w.modified {
            w.logging.record_commit(txid);
            return;
        }

        let commit_ts = ctx.alloc_oracle();
        w.cc.commit_tree.append(txid, commit_ts);
        w.cc.latest_cts.store(commit_ts, Relaxed);

        w.tx_id.store(0, Release); // sync with cc

        w.logging.record_commit(txid);
        w.cc.collect_wmk(ctx);
        w.logging.stabilize();
    }

    pub(crate) fn rollback(&self, g: &Guard, ctx: &Context, tree: &Tree) {
        const SMALL_SIZE: usize = 256;
        let mut ms = *self;
        let w = ms.deref_mut();
        let txid = w.tx_id.load(Relaxed);
        if !w.modified {
            w.logging.record_abort(txid);
            return;
        }
        w.logging.stabilize();
        let mut block = Block::alloc(SMALL_SIZE);
        let reader = WalReader::new(ctx, g);
        let (seq, off) = unpack_id(w.logging.lsn());
        let location = Location {
            wid: self.id as u32,
            seq,
            off,
            len: 0,
        };
        reader.rollback(&mut block, txid, location, tree, Some(*self));

        // since we are append-only, we must update CommitTree to make the rollbacked data visible
        // for example: worker 1 set foo = bar then commit, worker 2 del foo, then rollback, if we
        // don't update CommitTree, then foo is not visible to any other worker except worker 2
        let commit_ts = ctx.alloc_oracle();
        w.cc.commit_tree.append(txid, commit_ts);
        w.tx_id.store(0, Release); // sync with cc
        w.cc.latest_cts.store(commit_ts, Relaxed);
        w.cc.collect_wmk(ctx);
        w.logging.stabilize();
    }
}

impl Deref for SyncWorker {
    type Target = Worker;
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.w }
    }
}

impl DerefMut for SyncWorker {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.w }
    }
}
