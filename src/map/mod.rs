pub(crate) mod buffer;
pub(crate) mod cache;
pub(crate) mod data;
pub(crate) mod evictor;
pub(crate) mod flow;
pub(crate) mod publish;

pub(crate) mod flush;
pub mod table;

use std::sync::Arc;

use dashmap::{DashMap, Entry};
use rustc_hash::FxHasher;
use std::hash::BuildHasherDefault;

use crate::{
    OpCode, Options,
    cc::context::Context,
    map::buffer::Pool,
    static_assert,
    types::{
        refbox::{BoxRef, BoxView},
        traits::{IHeader, ILoader},
    },
    utils::{
        Handle, MutRef,
        data::{AddrPair, GroupPositions, Interval, Position},
        lru::{CachePriority, ShardPriorityLru},
    },
};

pub(crate) type PagesMap = DashMap<u64, BoxRef, BuildHasherDefault<FxHasher>>;
pub(crate) type JunksMap = DashMap<u64, RetiredChain, BuildHasherDefault<FxHasher>>;
pub(crate) type Node = crate::types::node::Node<Loader>;
pub(crate) type Page = crate::types::page::Page<Loader>;

#[derive(Clone, Copy)]
struct CacheKey(u128);

impl CacheKey {
    const fn new(bucket_id: u64, addr: u64) -> Self {
        Self((bucket_id as u128) << 64 | addr as u128)
    }

    const fn raw(self) -> u128 {
        self.0
    }
}

#[derive(Default, Clone)]
pub(crate) struct SparseFrontier {
    mask: u128,
    items: Vec<(u8, Position)>,
}

impl SparseFrontier {
    pub(crate) fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub(crate) fn merge_group(&mut self, group: u8, lsn: Position) {
        static_assert!(Options::MAX_CONCURRENT_WRITE <= 128);
        let bit = 1u128 << group;
        if self.mask & bit == 0 {
            self.mask |= bit;
            self.items.push((group, lsn));
            return;
        }
        for (g, pos) in self.items.iter_mut() {
            if *g == group {
                *pos = (*pos).max(lsn);
                return;
            }
        }
        unreachable!("frontier mask/item mismatch for group {}", group);
    }

    pub(crate) fn merge_sparse(&mut self, other: &SparseFrontier) {
        for &(group, lsn) in &other.items {
            self.merge_group(group, lsn);
        }
    }

    pub(crate) fn apply_to(&self, dst: &mut GroupPositions) {
        for &(group, lsn) in &self.items {
            let idx = group as usize;
            debug_assert!(idx < dst.len());
            dst[idx] = dst[idx].max(lsn);
        }
    }
}

#[derive(Default, Clone)]
pub(crate) struct RetiredChain {
    // append-only retired logical addresses carried with a base lineage
    //
    // expected invariant: no duplicate logical addr inside one chain
    // why this is valid:
    // 1) each successful replace/evict moves one old_base lineage exactly once
    // 2) old_base inheritance uses remove/take semantics from hot retired map
    // 3) current-round junks are validated as logical-unique under extra_check
    //
    // if duplicates show up here, treat it as an upstream invariant violation
    pub(crate) addrs: Vec<u64>,
    pub(crate) frontier: SparseFrontier,
}

pub struct Loader {
    pub(crate) pool: Handle<Pool>,
    pub(crate) ctx: Handle<Context>,
    pub(crate) lru: Handle<ShardPriorityLru<BoxRef>>,
    pub(crate) pinned: MutRef<DashMap<u64, BoxRef>>,
    pub(crate) bucket_id: u64,
    pub(crate) reader: Arc<dyn DataReader>,
}

impl Drop for Loader {
    fn drop(&mut self) {}
}

impl Loader {
    pub const PIN_CAP: usize = 64;

    #[inline]
    fn cache_key(&self, addr: u64) -> u128 {
        // logical addr is only bucket-local, while this LRU is shared by all bucket contexts.
        CacheKey::new(self.bucket_id, addr).raw()
    }

    pub fn find(&self, addr: u64) -> Result<BoxRef, OpCode> {
        let key = self.cache_key(addr);
        if let Some(x) = self.lru.get(key) {
            return Ok(x.clone());
        }
        if let Some(x) = self.pool.get_dirty_page(addr) {
            self.lru.add(
                CachePriority::High,
                key,
                x.header().total_size as usize,
                x.clone(),
            );
            return Ok(x);
        }
        self.reader.load_data(self.bucket_id, addr, &|b| {
            self.lru
                .add(CachePriority::High, key, b.header().total_size as usize, b);
        })
    }
}

impl ILoader for Loader {
    fn copy_without_pin(&self) -> Self {
        Self {
            pool: self.pool,
            ctx: self.ctx,
            lru: self.lru,
            pinned: MutRef::new(DashMap::new()),
            bucket_id: self.bucket_id,
            reader: self.reader.clone(),
        }
    }

    fn copy_with_pin(&self) -> Self {
        Self {
            pool: self.pool,
            ctx: self.ctx,
            lru: self.lru,
            pinned: self.pinned.clone(),
            bucket_id: self.bucket_id,
            reader: self.reader.clone(),
        }
    }

    fn pin(&self, data: BoxRef) {
        self.pinned.insert(data.header().addr, data);
    }

    fn load(&self, addr: u64) -> Result<BoxView, OpCode> {
        if let Some(p) = self.pinned.get(&addr) {
            return Ok(p.view());
        }
        let x = self.find(addr)?;
        let e = self.pinned.entry(addr);
        match e {
            Entry::Occupied(o) => Ok(o.get().view()),
            Entry::Vacant(v) => {
                let r = x.view();
                v.insert(x);
                Ok(r)
            }
        }
    }

    fn load_remote(&self, addr: u64) -> Result<BoxRef, OpCode> {
        if let Some(x) = self.pinned.get(&addr) {
            return Ok(x.value().clone());
        }
        let key = self.cache_key(addr);
        if let Some(x) = self.lru.get(key) {
            return Ok(x.clone());
        }
        if let Some(x) = self.pool.get_dirty_page(addr) {
            self.lru.add(
                CachePriority::Low,
                key,
                x.header().total_size as usize,
                x.clone(),
            );
            return Ok(x);
        }
        self.reader.load_blob(self.bucket_id, addr, &|b| {
            self.lru
                .add(CachePriority::Low, key, b.header().total_size as usize, b);
        })
    }

    fn load_remote_uncached(&self, addr: u64) -> BoxRef {
        if let Some(x) = self.pinned.get(&addr) {
            return x.value().clone();
        }
        if let Some(x) = self.pool.get_dirty_page(addr) {
            return x;
        }
        self.reader
            .load_blob_uncached(self.bucket_id, addr)
            .expect("must exist")
    }
}

pub(crate) enum SharedState {
    Quit,
    Evict,
}

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

pub trait DataReader: Send + Sync {
    fn load_data(
        &self,
        bucket_id: u64,
        addr: u64,
        cache: &dyn Fn(BoxRef),
    ) -> Result<BoxRef, OpCode>;

    fn load_blob(
        &self,
        bucket_id: u64,
        addr: u64,
        cache: &dyn Fn(BoxRef),
    ) -> Result<BoxRef, OpCode>;

    fn load_blob_uncached(&self, bucket_id: u64, addr: u64) -> Result<BoxRef, OpCode>;
}
