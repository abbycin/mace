pub(crate) mod buffer;
pub(crate) mod cache;
pub(crate) mod data;
mod evictor;
mod flush;
pub mod table;
use std::sync::{Arc, mpsc::channel};

use crate::{
    OpCode,
    cc::context::Context,
    map::{buffer::Buffers, cache::NodeCache, evictor::Evictor, table::PageMap},
    meta::Numerics,
    utils::{Handle, options::ParsedOptions},
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
) -> Result<Handle<Buffers>, OpCode> {
    let (tx, rx) = channel();
    let (qtx, qrx) = channel();
    let node_cache = Arc::new(NodeCache::new(opt.cache_capacity, opt.cache_evict_pct));
    let buffer = Handle::new(Buffers::new(
        page.clone(),
        ctx,
        node_cache.clone(),
        tx,
        qrx,
    )?);
    let evictor = Evictor::new(opt, node_cache, page, numerics, buffer, rx, qtx);
    evictor.start();
    Ok(buffer)
}

#[cfg(feature = "metric")]
pub use flush::g_flush_status;
