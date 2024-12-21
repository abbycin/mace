use std::sync::Arc;

pub use cc::data::Record;
use index::tree::Registry;
pub use index::txn::{Tx, TxnKV, TxnView};
pub(crate) use store::store::Store;
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
    tree: Registry,
    default_tree: String,
}

impl Mace {
    const DEFAULT_TREE: &'static str = "MACE";

    pub fn open(opt: Options) -> Result<Self, OpCode> {
        let store = Arc::new(Store::new(opt)?);
        let tree = Registry::new(store.clone())?;
        Ok(Self {
            store,
            tree,
            default_tree: Self::DEFAULT_TREE.into(),
        })
    }

    pub fn default(&self) -> Tx {
        let tree = self.tree.open(&self.default_tree).expect("can't open tree");
        Tx {
            ctx: self.store.context.clone(),
            tree,
        }
    }

    pub fn get(&self, name: impl AsRef<str>) -> Result<Tx, OpCode> {
        let tree = self.tree.open(name)?;

        Ok(Tx {
            ctx: self.store.context.clone(),
            tree,
        })
    }

    pub fn set_default(&mut self, name: impl AsRef<str>) {
        let mut new = name.as_ref().into();
        std::mem::swap(&mut self.default_tree, &mut new);
    }

    pub fn remove(&self, tx: Tx) -> Result<(), OpCode> {
        self.tree.remove(&tx.tree)
    }
}
