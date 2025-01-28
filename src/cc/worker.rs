use std::{
    ops::{Deref, DerefMut},
    sync::{
        atomic::{
            AtomicU64,
            Ordering::{Relaxed, Release},
        },
        Arc,
    },
};

use crate::{
    index::registry::Registry,
    utils::{block::Block, countblock::Countblock},
    IsolationLevel, Options,
};

use super::{
    cc::{ConcurrencyControl, Transaction},
    context::Context,
    log::Logging,
    wal::WalReader,
};

pub struct Worker {
    pub cc: ConcurrencyControl,
    pub txn: Transaction,
    pub logging: Logging,
    /// a snapshot of [`Transaction::start_ts`] which will be used across threads
    pub tx_id: AtomicU64,
    pub id: u16,
}

impl Worker {
    fn new(fsn: Arc<AtomicU64>, id: u16, opt: Arc<Options>, sem: Arc<Countblock>) -> Self {
        Self {
            cc: ConcurrencyControl::new(opt.workers),
            txn: Transaction::new(),
            logging: Logging::new(fsn, opt, id, sem),
            tx_id: AtomicU64::new(0),
            id,
        }
    }
}

#[derive(Clone, Copy)]
pub struct SyncWorker {
    w: *mut Worker,
}

impl SyncWorker {
    pub fn new(fsn: Arc<AtomicU64>, id: u16, opt: Arc<Options>, sem: Arc<Countblock>) -> Self {
        let w = Box::new(Worker::new(fsn, id, opt, sem));
        Self {
            w: Box::into_raw(w),
        }
    }

    pub fn reclaim(&self) {
        let b = unsafe { Box::from_raw(self.w) };
        drop(b);
    }

    fn init(&mut self, level: IsolationLevel, ctx: &Context, start_ts: u64) {
        assert!(
            level == IsolationLevel::SI,
            "only SI is supported at present"
        );

        let id = self.id;
        self.txn.reset(start_ts, level);
        self.tx_id.store(start_ts, Relaxed);
        self.cc.global_wmk_tx = ctx.wmk_oldest();
        self.cc.commit_tree.compact(ctx, id);
    }

    pub(crate) fn view(&mut self, ctx: &Arc<Context>, level: IsolationLevel) {
        self.init(level, ctx, ctx.load_oracle());
    }

    pub(crate) fn begin(&mut self, ctx: &Context, level: IsolationLevel) -> u64 {
        let start_ts = ctx.alloc_oracle();
        self.init(level, ctx, start_ts);
        self.logging.record_begin(start_ts);
        start_ts
    }

    pub(crate) fn commit(&self, ctx: &Context) {
        let mut txn = self.txn;
        let txid = txn.start_ts;

        if !self.txn.modified {
            self.logging.record_commit(txid);
            return;
        }

        let mut w = *self;
        txn.commit_ts = ctx.alloc_oracle();
        w.cc.commit_tree.append(txid, txn.commit_ts);
        w.cc.latest_cts.store(txn.commit_ts, Relaxed);

        w.tx_id.store(0, Release); // sync with cc
        txn.max_fsn = w.logging.fsn();

        // we have no remote dependency, since we are append-only
        w.logging.append_txn(txn);

        w.logging.record_commit(txid);
        w.cc.collect_wmk(ctx);
        w.logging.wait_commit(txn.commit_ts);
    }

    pub(crate) fn rollback(&self, mgr: Registry, ctx: &Context) {
        let txid = self.txn.start_ts;
        if !self.txn.modified {
            self.logging.record_abort(txid);
            return;
        }
        let mut w = *self;
        w.logging.wait_flush();
        let mut block = Block::alloc(1024);
        let reader = WalReader::new(mgr);
        reader.rollback(&mut block, txid, w.logging.lsn.load(Relaxed));

        // since we are append-only, we must update CommitTree to make the rollbacked data visible
        // for example: worker 1 set foo = bar then commit, worker 2 del foo, then rollback, if we
        // don't update CommitTree, then foo is not visible to any other worker except worker 2
        let commit_ts = ctx.alloc_oracle();
        w.cc.commit_tree.append(txid, commit_ts);
        w.tx_id.store(0, Release); // sync with cc
        w.cc.latest_cts.store(commit_ts, Relaxed);
        w.cc.collect_wmk(ctx);
    }
}

unsafe impl Sync for SyncWorker {}
unsafe impl Send for SyncWorker {}

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
