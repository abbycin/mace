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
        #[cfg(feature = "metric")]
        {
            use crate::index::{g_alloc_status, g_cas_status};
            use crate::map::g_flush_status;

            log::info!("\n{:#?}", g_alloc_status());
            log::info!("\n{:#?}", g_cas_status());
            log::info!("\n{:#?}", g_flush_status());
        }
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

        recover.phase2(meta.clone(), &desc, &tree);
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
