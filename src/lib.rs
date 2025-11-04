use std::sync::Arc;

use index::tree::Tree;
pub use index::txn::{TxnKV, TxnView};
pub(crate) use store::store::Store;
use store::{
    gc::{GCHandle, start_gc},
    recovery::Recovery,
};
pub use utils::{OpCode, RandomPath, options::Options};
use utils::{ROOT_PID, options::ParsedOptions};

mod cc;
mod index;
mod map;
mod meta;
mod store;
mod utils;

mod types;
pub use index::{Iter, ValRef};

use crate::utils::MutRef;

struct Inner {
    store: MutRef<Store>,
    gc: GCHandle,
    tree: Tree,
    opt: Arc<ParsedOptions>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        #[cfg(feature = "metric")]
        {
            use crate::index::{g_alloc_status, g_cas_status};
            use crate::map::{g_flush_status, g_pool_status};

            log::info!("\n{:#?}", g_alloc_status());
            log::info!("\n{:#?}", g_cas_status());
            log::info!("\n{:#?}", g_flush_status());
            log::info!("\n{:#?}", g_pool_status());
        }
        self.gc.quit();
        self.store.quit();
    }
}

#[derive(Clone)]
pub struct Mace {
    inner: Arc<Inner>,
}

impl Mace {
    fn open(store: MutRef<Store>) -> Tree {
        if store.is_fresh() {
            Tree::new(store)
        } else {
            Tree::load(store)
        }
    }
    pub fn new(opt: ParsedOptions) -> Result<Self, OpCode> {
        let opt = Arc::new(opt);
        let mut recover = Recovery::new(opt.clone());
        let (table, desc, ctx) = recover.phase1()?;
        let store = MutRef::new(Store::new(table, opt.clone(), ctx));
        let tree = Self::open(store.clone());

        recover.phase2(ctx, &desc, &tree);
        store.start();
        let handle = start_gc(store.clone(), store.context);

        Ok(Self {
            inner: Arc::new(Inner {
                store,
                gc: handle,
                tree,
                opt,
            }),
        })
    }

    pub fn begin(&'_ self) -> Result<TxnKV<'_>, OpCode> {
        TxnKV::new(&self.inner.store.context, &self.inner.tree)
    }

    pub fn view(&'_ self) -> Result<TxnView<'_>, OpCode> {
        TxnView::new(&self.inner.store.context, &self.inner.tree)
    }

    pub fn options(&self) -> &Options {
        &self.inner.opt
    }

    /// notify gc has been disabled, gc will not start when timeout
    pub fn disable_gc(&self) {
        self.inner.gc.pause();
    }

    /// notify gc has been enabled, gc will start when timeout
    pub fn enable_gc(&self) {
        self.inner.gc.resume();
    }

    /// notify gc to work even when gc was disabled, block until gc has been finished
    pub fn start_gc(&self) {
        self.inner.gc.start();
    }
}
