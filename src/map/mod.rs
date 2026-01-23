pub(crate) mod buffer;
pub(crate) mod cache;
pub(crate) mod chunk;
pub(crate) mod data;
pub(crate) mod evictor;

mod flush;
pub mod table;
use std::sync::{Arc, mpsc::channel};

use crate::{
    cc::context::Context,
    map::{buffer::Buffers, cache::NodeCache, evictor::Evictor, table::PageMap},
    meta::Numerics,
    utils::{
        Handle,
        data::{AddrPair, Interval},
        options::ParsedOptions,
    },
};

pub(crate) enum SharedState {
    Quit,
    Evict,
}

pub(crate) fn create_buffer(
    page: Arc<PageMap>,
    ctx: Handle<Context>,
    opt: Arc<ParsedOptions>,
    numerics: Arc<Numerics>,
) -> Handle<Buffers> {
    let (tx, rx) = channel();
    let (qtx, qrx) = channel();
    let node_cache = Arc::new(NodeCache::new(opt.cache_capacity, opt.cache_evict_pct));
    let buffer = Handle::new(Buffers::new(page.clone(), ctx, node_cache.clone(), tx, qrx));
    let evictor = Evictor::new(opt, node_cache, page, numerics, buffer, rx, qtx);
    evictor.start();
    buffer
}

#[cfg(feature = "metric")]
pub use buffer::g_pool_status;
#[cfg(feature = "metric")]
pub use flush::g_flush_status;

pub(crate) trait IFooter: Default {
    const LEN: usize = size_of::<Self>();

    fn nr_interval(&self) -> usize;

    fn nr_reloc(&self) -> usize;

    fn reloc_crc(&self) -> u32;

    fn interval_crc(&self) -> u32;

    fn interval_len(&self) -> usize {
        self.nr_interval() * Interval::LEN
    }

    fn reloc_len(&self) -> usize {
        self.nr_reloc() * AddrPair::LEN
    }
}
