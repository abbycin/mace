use std::sync::Arc;

pub use cc::data::Record;
pub use index::txn::{Tx, TxnKV, TxnView};
use index::{registry::Registry, tree::Tree};
pub(crate) use store::store::Store;
use store::{
    gc::{start_gc, Handle},
    recovery::Recovery,
};
use utils::data::Meta;
pub use utils::{options::Options, IsolationLevel, OpCode, RandomPath};

mod cc;
mod index;
mod map;
mod store;
mod utils;
pub use index::ValRef;

#[derive(Clone)]
pub struct Mace {
    store: Arc<Store>,
    mgr: Registry,
    meta: Arc<Meta>,
    gc: Handle,
    default_tree: Tree,
    opt: Arc<Options>,
}

impl Mace {
    pub fn new(opt: Options) -> Result<Self, OpCode> {
        let opt = Arc::new(opt);
        let mut recover = Recovery::new(opt.clone());
        let (meta, table, mapping) = recover.phase1();
        let store = Arc::new(Store::new(table, opt.clone(), meta.clone(), mapping)?);
        let mgr = Registry::new(store.clone());

        recover.phase2(meta.clone(), &mgr);
        meta.sync(opt.meta_file(), false);
        store.start();
        let handle = start_gc(mgr.clone(), meta.clone(), store.buffer.mapping.clone());
        let default_tree = mgr.open_default()?;

        Ok(Self {
            store,
            mgr,
            meta,
            gc: handle,
            default_tree,
            opt,
        })
    }

    pub fn default(&self) -> Tx {
        Tx {
            ctx: self.store.context.clone(),
            tree: self.default_tree.clone(),
            mgr: self.mgr.clone(),
        }
    }

    pub fn alloc(&self) -> Result<Tx, OpCode> {
        let tree = self.mgr.create_tree()?;
        Ok(Tx {
            ctx: self.store.context.clone(),
            tree,
            mgr: self.mgr.clone(),
        })
    }

    pub fn get(&self, id: u64) -> Result<Tx, OpCode> {
        let tree = self.mgr.get_tree(id)?;

        Ok(Tx {
            ctx: self.store.context.clone(),
            tree,
            mgr: self.mgr.clone(),
        })
    }

    pub fn remove(&self, id: u64) -> Result<(), OpCode> {
        self.gc.pause();
        let r = self.mgr.remove_tree(id);
        self.gc.resume();
        r
    }

    pub fn options(&self) -> &Options {
        &self.opt
    }

    fn quit(&self) {
        self.gc.quit();
        self.store.quit();
        self.meta.sync(self.store.opt.meta_file(), true);
    }
}

impl Drop for Mace {
    fn drop(&mut self) {
        self.quit();
    }
}
