use std::sync::Arc;

pub use cc::data::Record;
use index::registry::Registry;
pub use index::txn::{Tx, TxnKV, TxnView};
use store::recovery::Recovery;
pub(crate) use store::store::Store;
use utils::data::Meta;
pub use utils::{options::Options, IsolationLevel, OpCode, RandomPath};

mod cc;
mod index;
mod map;
mod store;
mod utils;
pub use index::Val;
#[derive(Clone)]
pub struct Mace {
    store: Arc<Store>,
    mgr: Registry,
    meta: Arc<Meta>,
    default_tree: String,
}

impl Mace {
    const DEFAULT_TREE: &'static str = "MACE";

    pub fn open(opt: Options) -> Result<Self, OpCode> {
        let opt = Arc::new(opt);
        let mut recover = Recovery::new(opt.clone());
        let (meta, table) = recover.phase1();
        let store = Arc::new(Store::new(table, opt.clone(), meta.clone()));
        let mut mgr = Registry::new(store.clone());

        recover.phase2(meta.clone(), &mut mgr);
        meta.sync(opt.meta_file(), false);
        store.start();

        Ok(Self {
            store,
            mgr,
            meta,
            default_tree: Self::DEFAULT_TREE.into(),
        })
    }

    pub fn default(&self) -> Tx {
        let tree = self.mgr.open(&self.default_tree);
        Tx {
            ctx: self.store.context.clone(),
            tree,
            mgr: self.mgr.clone(),
        }
    }

    pub fn get<A: AsRef<str>>(&self, name: A) -> Tx {
        let tree = self.mgr.open(name);

        Tx {
            ctx: self.store.context.clone(),
            tree,
            mgr: self.mgr.clone(),
        }
    }

    pub fn set_default<A: AsRef<str>>(&mut self, name: A) {
        let mut new = name.as_ref().into();
        std::mem::swap(&mut self.default_tree, &mut new);
    }

    pub fn remove(&self, tx: Tx) -> Result<(), OpCode> {
        self.mgr.remove(&tx.tree)
    }

    fn quit(&self) {
        self.store.quit();
        self.meta.sync(self.store.opt.meta_file(), true);
    }
}

impl Drop for Mace {
    fn drop(&mut self) {
        self.quit();
    }
}
