use parking_lot::RwLock;
use std::{
    fmt::Debug,
    ops::{Deref, DerefMut},
    ptr::null_mut,
    sync::{
        Arc,
        atomic::{
            AtomicU64,
            Ordering::{Relaxed, Release},
        },
    },
};

use super::{cc::ConcurrencyControl, context::Context, log::Logging};
use crate::{
    cc::wal::Location,
    meta::Numerics,
    utils::{data::Position, options::ParsedOptions},
};
use crate::{cc::wal::WalReader, utils::data::WalDescHandle};
use crate::{index::tree::Tree, utils::block::Block};
use crossbeam_epoch::Guard;

pub struct Worker {
    pub cc: ConcurrencyControl,
    pub start_ckpt: RwLock<Position>,
    pub tx_id: AtomicU64,
    pub id: u8,
    // TODO: detach logging from worker
    pub logging: Logging,
    pub modified: bool,
}

impl Worker {
    fn new(desc: WalDescHandle, numerics: Arc<Numerics>, opt: Arc<ParsedOptions>) -> Self {
        let id = { desc.lock().worker };
        Self {
            cc: ConcurrencyControl::new(opt.concurrent_write as usize),
            start_ckpt: RwLock::new(Position::default()),
            tx_id: AtomicU64::new(0),
            id,
            logging: Logging::new(desc, numerics, opt),
            modified: false,
        }
    }
}

#[derive(Clone, Copy)]
pub struct SyncWorker {
    w: *mut Worker,
}

impl Debug for SyncWorker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{:p}", self.w))
    }
}

unsafe impl Send for SyncWorker {}
unsafe impl Sync for SyncWorker {}

impl Default for SyncWorker {
    fn default() -> Self {
        Self { w: null_mut() }
    }
}

impl SyncWorker {
    pub fn new(desc: WalDescHandle, numerics: Arc<Numerics>, opt: Arc<ParsedOptions>) -> Self {
        let w = Box::new(Worker::new(desc, numerics, opt));
        Self {
            w: Box::into_raw(w),
        }
    }

    pub fn reclaim(&self) {
        unsafe { drop(Box::from_raw(self.w)) };
    }

    fn init(&mut self, ctx: &Context, start_ts: u64) {
        let id = self.id;
        self.tx_id.store(start_ts, Relaxed);
        self.logging.reset_ckpt_cnt();
        let mut lk = self.start_ckpt.write();
        *lk = self.logging.last_ckpt();
        drop(lk);
        self.cc.start_ts = start_ts;
        self.cc.global_wmk_tx = ctx.safe_txid();
        self.cc.commit_tree.compact(ctx, id);
    }

    pub(crate) fn begin(&mut self, ctx: &Context) {
        let start_ts = ctx.alloc_oracle();
        self.init(ctx, start_ts);
        self.logging.record_begin(start_ts);
    }

    pub(crate) fn commit(&self, ctx: &Context) {
        let mut ms = *self;
        let w = ms.deref_mut();
        let txid = w.cc.start_ts;
        w.tx_id.store(0, Release); // sync with cc

        if !w.modified {
            w.logging.record_commit(txid);
            return;
        }

        let commit_ts = ctx.alloc_oracle();
        w.cc.commit_tree.append(txid, commit_ts);
        w.cc.latest_cts.store(commit_ts, Relaxed);

        w.logging.record_commit(txid);
        w.cc.collect_wmk(ctx);
        w.logging.stabilize();
    }

    pub(crate) fn rollback(&self, g: &Guard, ctx: &Context, tree: &Tree) {
        const SMALL_SIZE: usize = 256;
        let mut ms = *self;
        let w = ms.deref_mut();
        let txid = w.cc.start_ts;
        w.tx_id.store(0, Release); // sync with cc
        if !w.modified {
            w.logging.record_abort(txid);
            return;
        }
        w.logging.stabilize();
        let mut block = Block::alloc(SMALL_SIZE);
        let reader = WalReader::new(ctx, g);
        let location = Location {
            wid: self.id as u32,
            pos: w.logging.lsn(),
            len: 0,
        };
        reader.rollback(&mut block, txid, location, tree, Some(*self));

        // since we are append-only, we must update CommitTree to make the rollbacked data visible
        // for example: worker 1 set foo = bar then commit, worker 2 del foo, then rollback, if we
        // don't update CommitTree, then foo is not visible to any other worker except worker 2
        let commit_ts = ctx.alloc_oracle();
        w.cc.commit_tree.append(txid, commit_ts);
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
