use crate::must_true;
use btree_store::BTree;
use crc32c::Crc32cHasher;
use dashmap::DashMap;
use parking_lot::{Condvar, Mutex, MutexGuard};
use std::{
    collections::{BTreeMap, HashMap},
    hash::Hasher,
    ops::Deref,
    path::PathBuf,
    sync::Arc,
};

#[cfg(feature = "extra_check")]
use std::sync::atomic::{
    AtomicU64,
    Ordering::{AcqRel, Acquire, Relaxed, Release},
};

use crate::{
    io::{File, FileSystem, GatherIO},
    map::{
        buffer::BucketContext,
        data::{FileVersion, MetaReader},
    },
    must_exist, must_ok,
    types::{refbox::BoxRef, traits::IHeader},
    utils::{
        OpCode,
        bitmap::BitMap,
        compress::DecompressorPool,
        data::{AddrPair, LenSeq, Reloc},
        lru::{Lru, ShardLru},
        options::ParsedOptions,
    },
};

use super::{
    FileKind, MemStat, PersistStat, stat_bucket, stat_file_path, stat_handle_cache_capacity,
    stat_intervals,
};

pub(crate) struct StatCtx {
    kind: FileKind,
    map: DashMap<u64, MemStat>,
    common: StatCommon,
    #[cfg(feature = "extra_check")]
    total_size: AtomicU64,
    #[cfg(feature = "extra_check")]
    active_size: AtomicU64,
}

impl StatCtx {
    pub(crate) fn new(opt: Arc<ParsedOptions>, kind: FileKind) -> Self {
        Self {
            kind,
            map: DashMap::new(),
            common: StatCommon::new(
                opt.clone(),
                stat_handle_cache_capacity(&opt, kind),
                opt.stat_mask_cache_count,
            ),
            #[cfg(feature = "extra_check")]
            total_size: AtomicU64::new(0),
            #[cfg(feature = "extra_check")]
            active_size: AtomicU64::new(0),
        }
    }

    pub(crate) fn bucket_files(&self) -> &DashMap<u64, Vec<u64>> {
        &self.common.bucket_files
    }

    pub(crate) fn remove_cache(&self, file_id: u64) {
        self.common.cache.del(file_id);
    }

    pub(crate) fn start_collect_junks(&self, file_ids: &[u64]) {
        self.common.junk.start(file_ids);
    }

    pub(crate) fn try_acquire_junk_permit(&self) -> Option<JunkPermit<'_>> {
        self.common.junk.try_acquire_permit(self.kind)
    }

    pub(crate) fn close_junk_gate(&self) -> JunkGate<'_> {
        self.common.junk.close_gate()
    }

    pub(crate) fn wait_junk_gate_open(&self) {
        self.common.junk.wait_gate_open()
    }

    pub(crate) fn cancel_collect_junks(&self) {
        self.common.junk.cancel();
    }

    pub(crate) fn handoff_collect_junks_to_ordinary(&self, file_ids: &[u64]) {
        self.common.junk.handoff_to_ordinary(file_ids);
    }

    pub(crate) fn clear_ordinary_collect_junks(&self, file_ids: &[u64]) {
        self.common.junk.clear_ordinary(file_ids);
    }

    #[cfg(feature = "extra_check")]
    pub(crate) fn collected_junk_count(&self) -> usize {
        self.common.junk.collected_count()
    }

    #[cfg(feature = "extra_check")]
    pub(crate) fn update_size(&self, active_size: u64, total_size: u64) {
        self.active_size.fetch_add(active_size, Relaxed);
        self.total_size.fetch_add(total_size, Relaxed);
    }

    #[cfg(feature = "extra_check")]
    pub(crate) fn sizes(&self) -> (u64, u64) {
        (
            self.active_size.load(Acquire),
            self.total_size.load(Acquire),
        )
    }

    #[cfg(feature = "extra_check")]
    fn decrease(&self, active_size: u64, total_size: u64) {
        let old = self.active_size.fetch_sub(active_size, AcqRel);
        must_true!(old >= active_size);

        let old = self.total_size.fetch_sub(total_size, AcqRel);
        must_true!(old >= total_size);
    }

    pub(crate) fn load(&self, addr: u64, ctx: &BucketContext) -> BoxRef {
        let file_id = {
            let ivl_map = stat_intervals(self.kind, ctx).read();
            must_exist!(ivl_map.find(addr), "addr {} not found in intervals", addr)
        };
        loop {
            if let Some(reader) = self.common.cache.get(file_id).map(|r| r.clone()) {
                return reader.read_at(addr);
            }

            let lk = self.common.cache.lock_shard(file_id);
            let decoders = self.common.decoders.clone();
            lk.add_if_missing(|| {
                new_reader(
                    stat_file_path(&self.common.opt, self.kind, file_id),
                    decoders.clone(),
                    self.common.opt.fs.clone(),
                )
            });
        }
    }

    // it's possible that during multiple compaction the intermediate sibling/blob addr may be removed
    // from dirty page and never flush to disk, but meanwhile someone may still hold the old ref to
    // those addr, in this case we can't find reloc by addr
    fn try_get_reloc(&self, file_id: u64, pos: u64) -> Option<Reloc> {
        loop {
            if let Some(reader) = self.common.cache.get(file_id).map(|x| x.clone()) {
                return reader.find_reloc(pos);
            }

            let lk = self.common.cache.lock_shard(file_id);
            let decoders = self.common.decoders.clone();
            lk.add_if_missing(|| {
                new_reader(
                    stat_file_path(&self.common.opt, self.kind, file_id),
                    decoders.clone(),
                    self.common.opt.fs.clone(),
                )
            });
        }
    }

    pub(crate) fn load_mask_from_btree(
        &self,
        file_id: u64,
        total_elems: u32,
        btree: &BTree,
    ) -> Result<BitMap, OpCode> {
        let buf = btree
            .view(stat_bucket(self.kind), |txn| txn.get(file_id.to_le_bytes()))
            .map_err(OpCode::from)?;
        Ok(PersistStat::decode_mask_only(&buf, total_elems))
    }

    pub(crate) fn load_persisted_stat_inner(
        &self,
        file_id: u64,
        btree: &BTree,
    ) -> Result<super::StatInner, OpCode> {
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::check("mace_stat_load_persisted_inner")?;
        let buf = btree
            .view(stat_bucket(self.kind), |txn| txn.get(file_id.to_le_bytes()))
            .map_err(OpCode::from)?;
        Ok(PersistStat::decode_inner_only(&buf))
    }

    fn record_mask_use(&self, file_id: u64) {
        if self.common.mask_cache.get(&file_id).is_some() {
            return;
        }
        if let Some((evicted_id, _)) =
            self.common
                .mask_cache
                .add_with_evict(self.common.mask_capacity, file_id, ())
            && evicted_id != file_id
            && let Some(mut stat) = self.map.get_mut(&evicted_id)
        {
            stat.mask = None;
        }
    }

    pub(crate) fn ensure_mask(&self, file_id: u64, btree: &BTree) -> Result<(), OpCode> {
        let total_elems = {
            let stat = self.map.get(&file_id).ok_or(OpCode::NotFound)?;
            if stat.mask.is_some() {
                drop(stat);
                self.record_mask_use(file_id);
                return Ok(());
            }
            stat.total_elems
        };

        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash("mace_stat_mask_before_load");

        let mask = self.load_mask_from_btree(file_id, total_elems, btree)?;
        let mut loaded = false;
        if let Some(mut stat) = self.map.get_mut(&file_id) {
            if stat.mask.is_none() {
                stat.mask = Some(mask);
            }
            loaded = true;
        }
        if loaded {
            self.record_mask_use(file_id);
            Ok(())
        } else {
            Err(OpCode::NotFound)
        }
    }

    pub(crate) fn insert_loaded_stat(&self, stat: MemStat) {
        self.common
            .bucket_files
            .entry(stat.bucket_id)
            .or_default()
            .push(stat.file_id);
        let file_id = stat.file_id;
        let has_mask = stat.mask.is_some();
        let old = self.map.insert(file_id, stat);
        must_true!(old.is_none());
        if has_mask {
            self.record_mask_use(file_id);
        }
    }

    pub(crate) fn add_stat_mem(&self, stat: MemStat) {
        must_true!(
            stat.active_size == stat.total_size,
            "active {}, total {}",
            stat.active_size,
            stat.total_size
        );
        #[cfg(feature = "extra_check")]
        self.update_size(stat.active_size as u64, stat.total_size as u64);
        self.insert_loaded_stat(stat);
    }

    fn add_replacement_stat_mem(&self, stat: MemStat) {
        must_true!(stat.active_size <= stat.total_size);
        must_true!(stat.active_elems <= stat.total_elems);
        must_true!(stat.mask.is_some());
        #[cfg(feature = "extra_check")]
        self.update_size(stat.active_size as u64, stat.total_size as u64);
        self.insert_loaded_stat(stat);
    }

    pub(crate) fn prepare_stat_intervals(
        &self,
        mut fstats: Vec<MemStat>,
        relocs: HashMap<u64, (u64, LenSeq)>,
    ) -> (Vec<MemStat>, Vec<PersistStat>) {
        for fstat in &fstats {
            must_true!(eq fstat.active_size, fstat.total_size);
        }
        let output_index: HashMap<u64, usize> = fstats
            .iter()
            .enumerate()
            .map(|(idx, stat)| (stat.file_id, idx))
            .collect();

        // apply deactived frames while we are performing compaction
        let junks = self.common.junk.begin_publish();
        apply_collected_junks(&mut fstats, &output_index, &relocs, junks);

        let mut stats = Vec::with_capacity(fstats.len());
        for fstat in &fstats {
            stats.push(persist_full_mask(fstat));
        }
        (fstats, stats)
    }

    pub(crate) fn publish_stat_intervals(&self, fstats: Vec<MemStat>, obsoleted: &[u64]) {
        for &id in obsoleted {
            self.remove_stat(id);
            self.common.cache.del(id);
        }
        for fstat in fstats {
            self.add_replacement_stat_mem(fstat);
        }
        self.common.junk.finish_publish();
    }

    pub(crate) fn remove_stat_interval(&self, data: &[u64]) {
        for x in data {
            self.remove_stat(*x);
        }
    }

    pub(crate) fn remove_stat(&self, file_id: u64) {
        if let Some((_, v)) = self.map.remove(&file_id) {
            self.common.mask_cache.del(&file_id);
            #[cfg(feature = "extra_check")]
            self.decrease(v.active_size as u64, v.total_size as u64);
            #[allow(clippy::collapsible_if)]
            if let Some(mut files) = self.common.bucket_files.get_mut(&v.bucket_id) {
                if let Some(pos) = files.iter().position(|&x| x == file_id) {
                    files.swap_remove(pos);
                }
            }
        }
    }

    pub(crate) fn apply_junks(
        &self,
        permit: &JunkPermit<'_>,
        tick: u64,
        junks: &[u64],
        ctx: &BucketContext,
        btree: &BTree,
        is_retired: impl Fn(u64) -> bool,
    ) -> Result<Vec<PersistStat>, OpCode> {
        must_true!(eq permit.kind, self.kind);
        let mut pending = junks.to_vec();
        let mut updates = BTreeMap::<u64, PersistStat>::new();
        while !pending.is_empty() {
            let grouped = self.group_junks_by_owner(ctx, &pending);
            let mut retry = Vec::new();
            {
                let mut apply = JunkApply {
                    tick,
                    ctx,
                    btree,
                    is_retired: &is_retired,
                    retry: &mut retry,
                    updates: &mut updates,
                };
                for (file_id, addrs) in grouped {
                    if is_retired(file_id) {
                        continue;
                    }
                    self.apply_owner_group(&mut apply, file_id, &addrs)?;
                }
            }
            pending = retry;
        }
        Ok(updates.into_values().collect())
    }

    /// group addresses by live interval owner
    fn group_junks_by_owner(
        &self,
        ctx: &BucketContext,
        pending: &[u64],
    ) -> BTreeMap<u64, Vec<u64>> {
        let lk = stat_intervals(self.kind, ctx).read();
        let mut grouped = BTreeMap::<u64, Vec<u64>>::new();
        for &addr in pending {
            let Some(file_id) = lk.find(addr) else {
                continue;
            };
            grouped.entry(file_id).or_default().push(addr);
        }
        grouped
    }

    /// apply a group and append post-publication retries
    fn apply_owner_group(
        &self,
        apply: &mut JunkApply<'_>,
        file_id: u64,
        addrs: &[u64],
    ) -> Result<(), OpCode> {
        let mut collector = self.common.junk.lock();
        match collector.state {
            JunkCollector::PUBLISHING => unreachable!("publishing requires a drained gate"),
            JunkCollector::COLLECTING => {
                self.apply_collecting_group(&mut collector, apply, file_id, addrs)?;
            }
            JunkCollector::IDLE => {
                self.apply_idle_group(collector, apply, file_id, addrs)?;
            }
            _ => unreachable!("invalid junk collector state"),
        }
        Ok(())
    }

    /// apply a collecting group: final victims hold the collector; ordinary
    /// handoffs retain ordinary retirement rules
    fn apply_collecting_group(
        &self,
        collector: &mut parking_lot::MutexGuard<'_, CollectorInner>,
        apply: &mut JunkApply<'_>,
        file_id: u64,
        addrs: &[u64],
    ) -> Result<(), OpCode> {
        if matches!(collector.get(&file_id), Some(CollectingEntry::Ordinary)) {
            // ordinary handoff keeps ordinary reclassification; addrs[0] is a
            // representative check because the group's owner state is uniform:
            // process_obsoleted_files removes all of a file's intervals in one
            // write lock (all-or-nothing), an ordinary file's addresses are never
            // copied into a rewrite replacement (it is not a Rewrite victim), and
            // flush output never overlaps retired addresses. a moved owner retries
            // the whole group; a still-owned or gone owner drops the group because
            // ordinary reclaim unconditionally removes the runtime stat, so the
            // junk needs no accounting
            let owner = stat_intervals(self.kind, apply.ctx).read().find(addrs[0]);
            if owner.is_some() && owner != Some(file_id) {
                apply.retry.extend(addrs);
            }
            return Ok(());
        }
        if matches!(collector.get(&file_id), Some(CollectingEntry::Rewrite(_))) {
            return self.apply_final_victim_group(collector, apply, file_id, addrs);
        }
        // unregistered files use ordinary accounting under the collector
        if let Err(err) = self.ensure_mask(file_id, apply.btree) {
            return self.reclassify_mask_failure(apply, file_id, addrs, err);
        }
        self.apply_ordinary_group_locked(collector, apply, file_id, addrs, false)
    }

    /// final-victim accounting stays under the collector
    fn apply_final_victim_group(
        &self,
        collector: &mut MutexGuard<'_, CollectorInner>,
        apply: &mut JunkApply<'_>,
        file_id: u64,
        addrs: &[u64],
    ) -> Result<(), OpCode> {
        let owner = stat_intervals(self.kind, apply.ctx).read().find(addrs[0]);
        if owner != Some(file_id) {
            if owner.is_some() {
                apply.retry.extend(addrs);
            }
            return Ok(());
        }
        #[cfg(feature = "extra_check")]
        if self
            .map
            .get(&file_id)
            .is_some_and(|stat| stat.mask.is_none())
        {
            crate::testing::fire_gc_stat_sync_point(
                match self.kind {
                    FileKind::Data => {
                        crate::testing::GcStatSyncPoint::DataCheckpointRewriteMaskLoad
                    }
                    FileKind::Blob => {
                        crate::testing::GcStatSyncPoint::BlobCheckpointRewriteMaskLoad
                    }
                },
                apply.ctx.bucket_id,
                &self.common.opt.db_root,
            );
        }
        self.ensure_mask(file_id, apply.btree)?;
        self.apply_ordinary_group_locked(collector, apply, file_id, addrs, true)
    }

    /// reacquire the collector before accounting to classify a concurrent rewrite
    fn apply_idle_group(
        &self,
        collector: MutexGuard<'_, CollectorInner>,
        apply: &mut JunkApply<'_>,
        file_id: u64,
        addrs: &[u64],
    ) -> Result<(), OpCode> {
        let mask_result = self.ensure_mask(file_id, apply.btree);
        drop(collector);
        if let Err(err) = mask_result {
            return self.reclassify_mask_failure(apply, file_id, addrs, err);
        }
        let mut collector = self.common.junk.lock();
        self.apply_ordinary_group_locked(&mut collector, apply, file_id, addrs, false)
    }

    /// apply under the collector; final victims treat a missing stat as fatal
    fn apply_ordinary_group_locked(
        &self,
        collector: &mut MutexGuard<'_, CollectorInner>,
        apply: &mut JunkApply<'_>,
        file_id: u64,
        addrs: &[u64],
        strict_missing_stat: bool,
    ) -> Result<(), OpCode> {
        if let Some(delta) =
            self.apply_stat_group_locked(collector, apply, file_id, addrs, strict_missing_stat)?
        {
            apply.updates.insert(file_id, delta);
        }
        Ok(())
    }

    /// reclassify a failed mask load against the current owner
    fn reclassify_mask_failure(
        &self,
        apply: &mut JunkApply<'_>,
        file_id: u64,
        addrs: &[u64],
        err: OpCode,
    ) -> Result<(), OpCode> {
        if (apply.is_retired)(file_id) {
            return Ok(());
        }
        let lk = stat_intervals(self.kind, apply.ctx).read();
        for &addr in addrs {
            match lk.find(addr) {
                Some(current) if current == file_id && err == OpCode::NotFound => {}
                Some(current) if current == file_id => return Err(err),
                Some(_) => apply.retry.push(addr),
                None => {}
            }
        }
        Ok(())
    }

    /// reclassify a vanished stat against the current owner
    fn reclassify_missing_stat(
        &self,
        apply: &mut JunkApply<'_>,
        file_id: u64,
        addrs: &[u64],
    ) -> Result<(), OpCode> {
        let lk = stat_intervals(self.kind, apply.ctx).read();
        for &addr in addrs {
            match lk.find(addr) {
                Some(current) if current == file_id => return Err(OpCode::NotFound),
                Some(_) => apply.retry.push(addr),
                None => {}
            }
        }
        Ok(())
    }

    /// apply a group under the collector after loading its inactive mask.
    /// final victims make a missing stat fatal; moved owners retry.
    fn apply_stat_group_locked(
        &self,
        collector: &mut MutexGuard<'_, CollectorInner>,
        apply: &mut JunkApply<'_>,
        file_id: u64,
        addrs: &[u64],
        strict_missing_stat: bool,
    ) -> Result<Option<PersistStat>, OpCode> {
        match collector.state {
            JunkCollector::PUBLISHING => unreachable!("publishing requires a drained gate"),
            JunkCollector::COLLECTING
                if matches!(collector.get(&file_id), Some(CollectingEntry::Ordinary)) =>
            {
                apply.retry.extend(addrs);
                return Ok(None);
            }
            _ => {}
        }
        let enqueue = match collector.state {
            JunkCollector::COLLECTING => {
                matches!(collector.get(&file_id), Some(CollectingEntry::Rewrite(_)))
            }
            _ => false,
        };
        let Some(mut stat) = self.map.get_mut(&file_id) else {
            if strict_missing_stat {
                return Err(OpCode::NotFound);
            }
            self.reclassify_missing_stat(apply, file_id, addrs)?;
            return Ok(None);
        };
        let mut changed = false;
        for &addr in addrs {
            let Some(reloc) = self.try_get_reloc(file_id, addr) else {
                continue;
            };
            if stat.mask.as_ref().expect("mask loaded").test(reloc.seq) {
                continue;
            }
            #[cfg(feature = "extra_check")]
            self.active_size
                .fetch_sub(reloc.active_len() as u64, Release);
            stat.update(apply.tick, &reloc);
            if enqueue {
                let Some(CollectingEntry::Rewrite(queue)) = collector.get_mut(&file_id) else {
                    return Err(OpCode::NotFound);
                };
                queue.push(addr);
            }
            changed = true;
        }
        Ok(if changed {
            Some(persist_full_mask(&stat))
        } else {
            None
        })
    }

    pub(crate) fn bucket_ratio(
        &self,
        bucket_id: u64,
        is_unsynced: impl Fn(u64) -> bool,
    ) -> Option<u64> {
        let files = self.bucket_files().get(&bucket_id)?;
        let mut total = 0u64;
        let mut active = 0u64;
        for &fid in files.value().iter() {
            if is_unsynced(fid) {
                continue;
            }
            if let Some(s) = self.get(&fid) {
                if s.active_elems == 0 {
                    continue;
                }
                total += s.total_size as u64;
                active += s.active_size as u64;
            }
        }
        if total == 0 {
            return None;
        }
        Some((total - active) * 100 / total)
    }
}

/// per-call junk classification, retries, and stat updates
struct JunkApply<'a> {
    tick: u64,
    ctx: &'a BucketContext,
    btree: &'a BTree,
    is_retired: &'a dyn Fn(u64) -> bool,
    retry: &'a mut Vec<u64>,
    updates: &'a mut BTreeMap<u64, PersistStat>,
}

fn persist_full_mask(stat: &MemStat) -> PersistStat {
    let inactive = stat
        .mask
        .as_ref()
        .expect("mask loaded")
        .set_bits(stat.total_elems);
    PersistStat::from_parts(stat.inner, inactive)
}

fn apply_collected_junks(
    fstats: &mut [MemStat],
    output_index: &HashMap<u64, usize>,
    relocs: &HashMap<u64, (u64, LenSeq)>,
    junks: HashMap<u64, Vec<u64>>,
) {
    for q in junks.into_values() {
        for addr in q {
            let Some((file_id, reloc)) = relocs.get(&addr) else {
                continue;
            };
            let Some(&idx) = output_index.get(file_id) else {
                continue;
            };
            let fstat = &mut fstats[idx];
            if fstat.mask.as_ref().expect("mask loaded").test(reloc.seq) {
                continue;
            }
            fstat.active_size -= reloc.active_len() as usize;
            fstat.active_elems -= 1;
            fstat.mask.as_mut().expect("mask loaded").set(reloc.seq);
        }
    }
}

struct JunkCollector {
    inner: Mutex<CollectorInner>,
    cv: Condvar,
}

struct CollectorInner {
    state: u8,
    junks: HashMap<u64, CollectingEntry>,
    gate_closed: bool,
    permits: usize,
}

impl std::ops::Deref for CollectorInner {
    type Target = HashMap<u64, CollectingEntry>;

    fn deref(&self) -> &Self::Target {
        &self.junks
    }
}

impl std::ops::DerefMut for CollectorInner {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.junks
    }
}

pub(crate) struct JunkPermit<'a> {
    collector: &'a JunkCollector,
    kind: FileKind,
}

impl Drop for JunkPermit<'_> {
    fn drop(&mut self) {
        let mut inner = self.collector.inner.lock();
        must_true!(inner.permits > 0);
        inner.permits -= 1;
        self.collector.cv.notify_all();
    }
}

pub(crate) struct JunkGate<'a> {
    collector: &'a JunkCollector,
}

impl JunkGate<'_> {
    pub(crate) fn drain(&self) {
        self.collector.drain_gate();
    }
}

impl Drop for JunkGate<'_> {
    fn drop(&mut self) {
        let mut inner = self.collector.inner.lock();
        must_true!(inner.permits == 0);
        // recover the collector if publishing unwinds before finish_publish
        if inner.state == JunkCollector::PUBLISHING {
            inner.state = JunkCollector::IDLE;
        }
        inner.gate_closed = false;
        self.collector.cv.notify_all();
    }
}

#[derive(Debug)]
enum CollectingEntry {
    Rewrite(Vec<u64>),
    Ordinary,
}

struct StatCommon {
    pub(crate) bucket_files: DashMap<u64, Vec<u64>>,
    cache: ShardLru<Arc<FileReader>>,
    decoders: Arc<DecompressorPool>,
    mask_cache: Lru<u64, ()>,
    mask_capacity: usize,
    opt: Arc<ParsedOptions>,
    junk: JunkCollector,
}

impl JunkCollector {
    const IDLE: u8 = 0;
    const COLLECTING: u8 = 1;
    const PUBLISHING: u8 = 2;

    fn new() -> Self {
        Self {
            inner: Mutex::new(CollectorInner {
                state: Self::IDLE,
                junks: HashMap::new(),
                gate_closed: false,
                permits: 0,
            }),
            cv: Condvar::new(),
        }
    }

    fn try_acquire_permit(&self, kind: FileKind) -> Option<JunkPermit<'_>> {
        let mut inner = self.inner.lock();
        if inner.gate_closed {
            return None;
        }
        inner.permits += 1;
        Some(JunkPermit {
            collector: self,
            kind,
        })
    }

    fn close_gate(&self) -> JunkGate<'_> {
        let mut inner = self.inner.lock();
        inner.gate_closed = true;
        JunkGate { collector: self }
    }

    fn drain_gate(&self) {
        let mut inner = self.inner.lock();
        while inner.permits != 0 {
            self.cv.wait(&mut inner);
        }
    }

    fn wait_gate_open(&self) {
        let mut inner = self.inner.lock();
        while inner.gate_closed {
            self.cv.wait(&mut inner);
        }
    }

    fn start(&self, file_ids: &[u64]) {
        let mut inner = self.inner.lock();
        must_true!(inner.gate_closed);
        must_true!(inner.permits == 0);
        must_true!(eq inner.state, Self::IDLE);
        must_true!(inner.junks.is_empty());
        for &file_id in file_ids {
            inner
                .junks
                .insert(file_id, CollectingEntry::Rewrite(Vec::new()));
        }
        inner.state = Self::COLLECTING;
    }

    fn lock(&self) -> MutexGuard<'_, CollectorInner> {
        self.inner.lock()
    }

    fn handoff_to_ordinary(&self, file_ids: &[u64]) {
        let mut inner = self.inner.lock();
        must_true!(eq inner.state, Self::COLLECTING);
        for &file_id in file_ids {
            if let Some(entry) = inner.junks.get_mut(&file_id) {
                *entry = CollectingEntry::Ordinary;
            }
        }
    }

    fn clear_ordinary(&self, file_ids: &[u64]) {
        let mut inner = self.inner.lock();
        for &file_id in file_ids {
            if let Some(entry) = inner.junks.remove(&file_id) {
                must_true!(matches!(entry, CollectingEntry::Ordinary));
            }
        }
    }

    fn cancel(&self) {
        let mut inner = self.inner.lock();
        must_true!(eq inner.state, Self::COLLECTING);
        inner.junks.clear();
        inner.state = Self::IDLE;
    }

    fn begin_publish(&self) -> HashMap<u64, Vec<u64>> {
        let mut inner = self.inner.lock();
        must_true!(inner.gate_closed);
        must_true!(inner.permits == 0);
        must_true!(eq inner.state, Self::COLLECTING);
        must_true!(
            inner
                .junks
                .values()
                .all(|entry| matches!(entry, CollectingEntry::Rewrite(_)))
        );
        inner.state = Self::PUBLISHING;
        std::mem::take(&mut inner.junks)
            .into_iter()
            .map(|(file_id, entry)| match entry {
                CollectingEntry::Rewrite(queue) => (file_id, queue),
                CollectingEntry::Ordinary => unreachable!("ordinary entry before publish"),
            })
            .collect()
    }

    fn finish_publish(&self) {
        let mut inner = self.inner.lock();
        must_true!(eq inner.state, Self::PUBLISHING);
        inner.state = Self::IDLE;
    }

    #[cfg(feature = "extra_check")]
    fn collected_count(&self) -> usize {
        self.inner
            .lock()
            .values()
            .map(|entry| match entry {
                CollectingEntry::Rewrite(queue) => queue.len(),
                CollectingEntry::Ordinary => 0,
            })
            .sum()
    }
}

impl StatCommon {
    fn new(opt: Arc<ParsedOptions>, cache_capacity: usize, mask_capacity: usize) -> Self {
        Self {
            bucket_files: DashMap::new(),
            cache: ShardLru::new(cache_capacity),
            decoders: DecompressorPool::new(),
            mask_cache: Lru::new(),
            mask_capacity,
            opt,
            junk: JunkCollector::new(),
        }
    }
}

impl Deref for StatCtx {
    type Target = DashMap<u64, MemStat>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

pub(crate) struct FileReader {
    file: File,
    version: FileVersion,
    relocs: Box<[AddrPair]>,
    decoders: Arc<DecompressorPool>,
}

pub(crate) fn new_reader(
    path: PathBuf,
    decoders: Arc<DecompressorPool>,
    fs: Arc<dyn FileSystem>,
) -> Arc<FileReader> {
    let mut loader = MetaReader::new(fs.as_ref(), &path);
    let version = loader.version();
    let relocs = loader.get_reloc();
    let relocs = {
        #[cfg(feature = "extra_check")]
        for w in relocs.windows(2) {
            let prev = w[0].key;
            let next = w[1].key;
            debug_assert!(prev <= next, "reloc table must be sorted by addr");
        }
        relocs
    };
    let file = loader.take();
    Arc::new(FileReader {
        file,
        version,
        relocs,
        decoders,
    })
}

pub(crate) fn try_new_reader(
    path: PathBuf,
    decoders: Arc<DecompressorPool>,
    fs: Arc<dyn FileSystem>,
) -> Result<Arc<FileReader>, OpCode> {
    let mut loader = MetaReader::try_new(fs.as_ref(), &path)?;
    let version = loader.version();
    let relocs = loader.try_get_reloc()?;
    Ok(Arc::new(FileReader {
        file: loader.take(),
        version,
        relocs,
        decoders,
    }))
}

impl FileReader {
    fn try_read_exact(&self, buf: &mut [u8], mut off: u64) -> Result<(), OpCode> {
        let mut done = 0;
        while done < buf.len() {
            let got = self
                .file
                .read(&mut buf[done..], off)
                .map_err(OpCode::from)?;
            if got == 0 {
                return Err(OpCode::Corruption);
            }
            done += got;
            off += got as u64;
        }
        Ok(())
    }

    pub(crate) fn version(&self) -> FileVersion {
        self.version
    }

    pub(crate) fn file(&self) -> &File {
        &self.file
    }

    #[inline]
    pub(crate) fn find_reloc(&self, pos: u64) -> Option<Reloc> {
        let idx = self.relocs.binary_search_by_key(&pos, |x| x.key).ok()?;
        Some(self.relocs[idx].val)
    }

    pub(crate) fn read_at(&self, pos: u64) -> BoxRef {
        must_ok!(self.try_read_at(pos), "must read payload record")
    }

    pub(crate) fn try_read_at(&self, pos: u64) -> Result<BoxRef, OpCode> {
        let m = self.find_reloc(pos).ok_or(OpCode::NotFound)?;
        let raw_len = m.raw_len() as usize;
        if raw_len < BoxRef::DUMP_HDR_LEN {
            return Err(OpCode::Corruption);
        }
        let Some(alloc_size) = raw_len
            .checked_add(std::mem::size_of::<std::sync::atomic::AtomicU32>())
            .and_then(|size| u32::try_from(size).ok())
        else {
            return Err(OpCode::Corruption);
        };
        let mut page = match self.version {
            FileVersion::V1 => BoxRef::alloc_exact(alloc_size, pos),
            _ => return Err(OpCode::BadVersion),
        };
        let result = (|| {
            let dst = page.load_slice(raw_len);
            if !m.is_compressed() {
                let mut crc = Crc32cHasher::default();
                self.try_read_exact(dst, m.off as u64)?;
                crc.write(dst);
                let actual_crc = crc.finish() as u32;
                if actual_crc != m.crc {
                    return Err(OpCode::Corruption);
                }
            } else {
                let actual_crc = self.decoders.with_decoder(|decoder| {
                    decoder.decode_reader_into(
                        &self.file,
                        m.off as u64,
                        m.compressed_len() as usize,
                        dst,
                    )
                })?;
                if actual_crc != m.crc {
                    return Err(OpCode::Corruption);
                }
            }
            page.decode_persisted_v1_in_place(pos, raw_len)
        })();
        match result {
            Ok(()) => Ok(page),
            Err(err) => {
                // corrupted payload headers must not make BoxRef cleanup abort
                page.header_mut().payload_size = alloc_size - BoxRef::HDR_LEN as u32;
                page.header_mut().total_size = alloc_size;
                Err(err)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{Arc, mpsc::channel},
        time::Duration,
    };

    use crate::{
        meta::{FileKind, MemStat, StatInner},
        utils::{bitmap::BitMap, data::LenSeq},
    };

    use super::{JunkCollector, apply_collected_junks, persist_full_mask};

    #[test]
    fn junk_gate_rejects_permits_until_drain_and_drop() {
        let collector = Arc::new(JunkCollector::new());
        let permit = collector
            .try_acquire_permit(FileKind::Data)
            .expect("permit");
        assert_eq!(permit.kind, FileKind::Data);
        let gate = collector.close_gate();
        assert!(collector.try_acquire_permit(FileKind::Blob).is_none());
        let (tx, rx) = channel();
        let collector_for_thread = collector.clone();
        std::thread::scope(|scope| {
            scope.spawn(move || {
                collector_for_thread.drain_gate();
                tx.send(()).expect("drain completed");
            });
            assert!(rx.recv_timeout(Duration::from_millis(50)).is_err());
            drop(permit);
            rx.recv_timeout(Duration::from_secs(1))
                .expect("drain must wait for permit release");
        });
        drop(gate);
        assert!(collector.try_acquire_permit(FileKind::Blob).is_some());
    }

    #[test]
    fn junk_gate_drop_recovers_each_kind_after_publish_early_return() {
        for kind in [FileKind::Data, FileKind::Blob] {
            let collector = JunkCollector::new();
            let gate = collector.close_gate();
            collector.start(&[7]);
            let published = collector.begin_publish();
            assert_eq!(published.len(), 1);
            drop(gate);

            let gate = collector.close_gate();
            collector.start(&[8]);
            assert!(collector.try_acquire_permit(kind).is_none());
            collector.cancel();
            drop(gate);
            assert!(collector.try_acquire_permit(kind).is_some());
        }
    }

    #[test]
    fn junk_gate_drop_recovers_after_publish_panic_for_each_kind() {
        for kind in [FileKind::Data, FileKind::Blob] {
            let collector = JunkCollector::new();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _gate = collector.close_gate();
                collector.start(&[9]);
                let _ = collector.begin_publish();
                panic!("injected publish failure");
            }));
            assert!(result.is_err());
            assert!(collector.try_acquire_permit(kind).is_some());
        }
    }

    fn mem_stat(file_id: u64) -> MemStat {
        let inner = StatInner {
            file_id,
            up1: 0,
            up2: 0,
            active_elems: 2,
            total_elems: 2,
            active_size: 20,
            total_size: 20,
            bucket_id: 1,
        };
        MemStat::from_parts(inner, Some(BitMap::new(inner.total_elems)))
    }

    #[test]
    fn full_mask_persistence_keeps_prior_inactive_sequences() {
        let inner = StatInner {
            file_id: 7,
            up1: 0,
            up2: 0,
            active_elems: 2,
            total_elems: 4,
            active_size: 20,
            total_size: 40,
            bucket_id: 1,
        };
        let mut mask = BitMap::new(inner.total_elems);
        mask.set(0);
        let mut stat = MemStat::from_parts(inner, Some(mask));
        stat.mask.as_mut().unwrap().set(3);

        assert_eq!(persist_full_mask(&stat).inactive_elems, vec![0, 3]);
    }

    #[test]
    fn rewrite_junk_collector_registers_victims_and_closes_publish_cut() {
        let collector = JunkCollector::new();

        // publish extracts only rewrite queues
        let gate = collector.close_gate();
        collector.start(&[3, 5]);
        let collected = collector.begin_publish();
        assert_eq!(collected.len(), 2, "both registered victims publish");
        assert_eq!(collected.get(&3), Some(&Vec::new()));
        assert_eq!(collected.get(&5), Some(&Vec::new()));
        collector.finish_publish();
        drop(gate);

        // empty output cancels collection
        let gate = collector.close_gate();
        collector.start(&[7]);
        collector.cancel();
        drop(gate);

        // ordinary handoffs must clear before the rewrite-only publish cut
        let gate = collector.close_gate();
        collector.start(&[8]);
        collector.handoff_to_ordinary(&[8]);
        collector.clear_ordinary(&[8]);
        let collected = collector.begin_publish();
        assert!(
            collected.is_empty(),
            "a cleared ordinary entry must not publish as a victim"
        );
        collector.finish_publish();
        drop(gate);
    }

    #[test]
    fn rewrite_publish_waits_for_collector_critical_section() {
        let collector = JunkCollector::new();
        let gate = collector.close_gate();
        collector.start(&[3]);
        let (locked_tx, locked_rx) = channel();
        let (release_tx, release_rx) = channel();
        let (published_tx, published_rx) = channel();

        std::thread::scope(|scope| {
            let collector_ref = &collector;
            scope.spawn(move || {
                let _guard = collector_ref.lock();
                locked_tx.send(()).expect("signal collector lock");
                release_rx.recv().expect("release collector lock");
            });
            locked_rx.recv().expect("collector must lock");
            let collector_ref = &collector;
            scope.spawn(move || {
                collector_ref.begin_publish();
                published_tx.send(()).expect("signal publish");
            });
            assert!(
                published_rx
                    .recv_timeout(Duration::from_millis(100))
                    .is_err(),
                "publish must wait for the collector critical section"
            );
            release_tx.send(()).expect("release collector");
            published_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("publish must finish after collector release");
        });
        collector.finish_publish();
        drop(gate);
    }

    #[test]
    fn collected_rewrite_junk_is_applied_once_to_each_output_stat() {
        let mut fstats = vec![mem_stat(10), mem_stat(11)];
        let output_index = HashMap::from([(10, 0), (11, 1)]);
        let relocs = HashMap::from([
            (100, (10, LenSeq::new(10, 0, 0))),
            (200, (11, LenSeq::new(10, 0, 1))),
        ]);
        let junks = HashMap::from([(3, vec![100, 100]), (5, vec![200, 999])]);

        apply_collected_junks(&mut fstats, &output_index, &relocs, junks);

        assert_eq!(fstats[0].active_elems, 1);
        assert_eq!(fstats[0].active_size, 10);
        assert_eq!(persist_full_mask(&fstats[0]).inactive_elems, vec![0]);
        assert_eq!(fstats[1].active_elems, 1);
        assert_eq!(fstats[1].active_size, 10);
        assert_eq!(persist_full_mask(&fstats[1]).inactive_elems, vec![1]);
    }
}
