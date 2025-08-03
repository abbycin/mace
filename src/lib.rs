use std::sync::Arc;

use index::tree::Tree;
pub use index::txn::{TxnKV, TxnView};
pub(crate) use store::store::Store;
use store::{
    gc::{GCHandle, start_gc},
    recovery::Recovery,
};
pub use utils::{OpCode, RandomPath, options::Options};
use utils::{ROOT_PID, data::Meta, options::ParsedOptions};

mod cc;
mod index;
mod map;
mod store;
mod utils;

mod types;
pub use index::ValRef;

struct Inner {
    store: Arc<Store>,
    meta: Arc<Meta>,
    gc: GCHandle,
    tree: Tree,
    opt: Arc<ParsedOptions>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.gc.quit();
        self.store.quit();
        self.meta.sync(self.store.opt.meta_file(), true);
    }
}

#[derive(Clone)]
pub struct Mace {
    inner: Arc<Inner>,
}

impl Mace {
    fn open(store: Arc<Store>) -> Tree {
        if store.is_fresh() {
            Tree::new(store, ROOT_PID)
        } else {
            Tree::load(store, ROOT_PID)
        }
    }
    pub fn new(opt: ParsedOptions) -> Result<Self, OpCode> {
        let opt = Arc::new(opt);
        let mut recover = Recovery::new(opt.clone());
        let (meta, table, mapping, desc) = recover.phase1()?;
        let store = Arc::new(Store::new(
            table,
            opt.clone(),
            meta.clone(),
            mapping,
            &desc,
        )?);
        let tree = Self::open(store.clone());

        let g = crossbeam_epoch::pin();
        recover.phase2(&g, meta.clone(), &desc, &tree);
        meta.sync(opt.meta_file(), false);
        store.start();
        let handle = start_gc(store.clone(), meta.clone(), store.buffer.mapping);

        Ok(Self {
            inner: Arc::new(Inner {
                store,
                meta,
                gc: handle,
                tree,
                opt,
            }),
        })
    }

    pub fn begin(&self) -> Result<TxnKV, OpCode> {
        TxnKV::new(&self.inner.store.context, &self.inner.tree)
    }

    pub fn view(&self) -> Result<TxnView, OpCode> {
        TxnView::new(&self.inner.store.context, &self.inner.tree)
    }

    pub fn options(&self) -> &Options {
        &self.inner.opt
    }

    pub fn pause_gc(&self) {
        self.inner.gc.pause();
    }

    pub fn resume_gc(&self) {
        self.inner.gc.resume();
    }
}
