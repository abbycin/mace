//! dirty-generation slots and the checkpoint cut
//!
//! `EpochRegistry::cut` performs the single critical section that rotates every
//! checkpoint input: dirty pages and their byte accounting, retired lineage, new
//! junk addresses, dirty roots, unmap markers and writer inflight state.

use parking_lot::{Mutex, RwLock};
use std::sync::{
    Arc, LazyLock,
    atomic::{AtomicU64, AtomicUsize, Ordering::Acquire},
};
use std::time::Instant;

use crate::map::{
    JunksMap, PagesMap,
    data::{AddrSet, EpochInflight, PidMap, PidSet},
};
use crate::must_exist;

pub(super) const DIRTY_PAGE_INIT_CAP: usize = 16 << 10;

pub(super) fn mono_ms() -> u64 {
    static START: LazyLock<Instant> = LazyLock::new(Instant::now);
    START.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

pub(crate) struct Current {
    pub(crate) pages: Arc<PagesMap>,
    pub(crate) bytes: Arc<AtomicUsize>,
    pub(crate) retired: Arc<JunksMap>,
    pub(crate) new_junks: Arc<AddrSet>,
    pub(crate) dirty_roots: Arc<PidMap>,
    pub(crate) unmap_pid: Arc<PidSet>,
    pub(crate) inflight: Arc<EpochInflight>,
}

pub(crate) struct WriteEpoch {
    pub(super) inner: Option<Arc<Current>>,
    pub(super) inflight: Arc<EpochInflight>,
}

impl std::ops::Deref for WriteEpoch {
    type Target = Current;

    fn deref(&self) -> &Self::Target {
        must_exist!(self.inner.as_deref())
    }
}

impl Drop for WriteEpoch {
    fn drop(&mut self) {
        // drop generation Arc first, then publish inflight leave to close the
        // tiny window where cnt can reach zero while the generation Arc is
        // still held by this WriteEpoch during drop glue.
        let _ = self.inner.take();
        self.inflight.leave();
    }
}

pub(crate) struct PageSlot {
    // current writable generation
    pub(crate) hot: Arc<PagesMap>,
    pub(crate) hot_bytes: Arc<AtomicUsize>,
    // last swapped generation kept visible while checkpoint is in-flight
    pub(crate) sealed: Option<Arc<PagesMap>>,
    pub(crate) sealed_bytes: Option<Arc<AtomicUsize>>,
}

pub(crate) struct JunkSlot {
    // current writable retired-chain generation
    pub(crate) hot: Arc<JunksMap>,
    // last swapped retired-chain generation visible to concurrent writers
    pub(crate) sealed: Option<Arc<JunksMap>>,
}

pub(crate) struct JunkAddrSlot {
    // current writable generation of newly produced junk addrs
    pub(crate) hot: Arc<AddrSet>,
    // last swapped generation visible to checkpoint snapshot
    pub(crate) sealed: Option<Arc<AddrSet>>,
}

pub(crate) struct PidSlot {
    // pid->addr roots and pid-unmap marks for the current writable generation
    pub(crate) root_map: Arc<PidMap>,
    pub(crate) unmap_pid: Arc<PidSet>,
    // tracks writers holding the current generation
    pub(crate) inflight: Arc<EpochInflight>,
}

// groups all epoch-state handles to keep Pool fields readable
pub(super) struct EpochRegistry {
    // mutable per-generation slots (hot/sealed)
    pub(super) page: Arc<RwLock<PageSlot>>,
    pub(super) retired: Arc<RwLock<JunkSlot>>,
    pub(super) junk_addr: Arc<RwLock<JunkAddrSlot>>,
    pub(super) root: Arc<RwLock<PidSlot>>,
    // fast writer snapshot to avoid cloning 3 locks in capture_epoch()
    pub(super) hot: RwLock<Arc<Current>>,
    // barrier that forms a complete epoch cut with checkpoint()
    pub(super) gate: RwLock<()>,
}

// recyclable containers reused across checkpoints
pub(super) struct RecycleBins {
    pub(super) pages: Arc<Mutex<Vec<PagesMap>>>,
    pub(super) retired: Arc<Mutex<Vec<JunksMap>>>,
}

impl RecycleBins {
    pub(super) fn take_pages(&self) -> PagesMap {
        self.pages.lock().pop().unwrap_or_else(|| {
            PagesMap::with_capacity_and_hasher(DIRTY_PAGE_INIT_CAP, Default::default())
        })
    }

    pub(super) fn take_retired(&self) -> JunksMap {
        self.retired.lock().pop().unwrap_or_default()
    }
}

/// one complete epoch cut: the sealed checkpoint inputs plus the new hot epoch
pub(super) struct Cut {
    pub(super) pages: Arc<PagesMap>,
    pub(super) sealed_bytes: Arc<AtomicUsize>,
    pub(super) retired: Arc<JunksMap>,
    pub(super) new_junks: Arc<AddrSet>,
    pub(super) dirty_roots: Arc<PidMap>,
    pub(super) unmap_pid: Arc<PidSet>,
    pub(super) epoch_inflight: Arc<EpochInflight>,
    pub(super) snap_addr: u64,
}

impl EpochRegistry {
    pub(super) fn cut(&self, recycle: &RecycleBins, next_addr: &AtomicU64) -> Cut {
        // single critical section that performs a full epoch cut:
        // swap hot generations, publish sealed generations, and rotate writer roots/inflight
        let _gate = self.gate.write();
        let mut page_epoch = self.page.write();
        let mut retired_epoch = self.retired.write();
        let mut junk_addr_epoch = self.junk_addr.write();
        let mut root_epoch = self.root.write();
        let old_pages = std::mem::replace(&mut page_epoch.hot, Arc::new(recycle.take_pages()));
        let old_hot_bytes =
            std::mem::replace(&mut page_epoch.hot_bytes, Arc::new(AtomicUsize::new(0)));
        let old_retired =
            std::mem::replace(&mut retired_epoch.hot, Arc::new(recycle.take_retired()));
        let old_new_junks =
            std::mem::replace(&mut junk_addr_epoch.hot, Arc::new(AddrSet::default()));
        let old_dirty = std::mem::replace(&mut root_epoch.root_map, Arc::new(PidMap::default()));
        let old_unmap = std::mem::replace(&mut root_epoch.unmap_pid, Arc::new(PidSet::default()));
        let old_inflight =
            std::mem::replace(&mut root_epoch.inflight, Arc::new(EpochInflight::new()));
        let snap_addr = next_addr.load(Acquire).saturating_sub(1);
        page_epoch.sealed = Some(old_pages.clone());
        page_epoch.sealed_bytes = Some(old_hot_bytes.clone());
        retired_epoch.sealed = Some(old_retired.clone());
        junk_addr_epoch.sealed = Some(old_new_junks.clone());
        let next_hot_epoch = Arc::new(Current {
            pages: page_epoch.hot.clone(),
            bytes: page_epoch.hot_bytes.clone(),
            retired: retired_epoch.hot.clone(),
            new_junks: junk_addr_epoch.hot.clone(),
            dirty_roots: root_epoch.root_map.clone(),
            unmap_pid: root_epoch.unmap_pid.clone(),
            inflight: root_epoch.inflight.clone(),
        });
        *self.hot.write() = next_hot_epoch;
        Cut {
            pages: old_pages,
            sealed_bytes: old_hot_bytes,
            retired: old_retired,
            new_junks: old_new_junks,
            dirty_roots: old_dirty,
            unmap_pid: old_unmap,
            epoch_inflight: old_inflight,
            snap_addr,
        }
    }
}
