use crate::must_true;
use btree_store::BTree;
use dashmap::DashMap;
use std::{collections::HashMap, sync::Arc};

#[cfg(feature = "extra_check")]
use std::sync::atomic::{
    AtomicU64,
    Ordering::{AcqRel, Acquire, Relaxed},
};

use crate::{
    map::{
        buffer::BucketContext,
        data::{FileReader, new_reader},
    },
    must_exist,
    types::refbox::BoxRef,
    utils::{
        OpCode,
        bitmap::BitMap,
        compress::DecompressorPool,
        data::{LenSeq, Reloc},
        lru::{Lru, ShardLru},
        options::ParsedOptions,
    },
};

use super::{
    FileKind, MemStat, PersistStat, StatInner, stat_bucket, stat_file_path,
    stat_handle_cache_capacity, stat_intervals,
};

mod apply;
pub(crate) mod junk;

pub(crate) use apply::{JunkApplier, apply_collected_junks, persist_full_mask};
pub(crate) use junk::{JunkPermit, JunkPermits, JunkRegistry, PublishCut, RewriteRegistration};

/// per-file accounting: file counts, masks and bucket ownership
pub(crate) struct FileAccounting {
    map: DashMap<u64, MemStat>,
    bucket_files: DashMap<u64, Vec<u64>>,
    mask_cache: Lru<u64, ()>,
    mask_capacity: usize,
    #[cfg(feature = "extra_check")]
    total_size: AtomicU64,
    #[cfg(feature = "extra_check")]
    active_size: AtomicU64,
}

impl FileAccounting {
    fn new(mask_capacity: usize) -> Self {
        Self {
            map: DashMap::new(),
            bucket_files: DashMap::new(),
            mask_cache: Lru::new(),
            mask_capacity,
            #[cfg(feature = "extra_check")]
            total_size: AtomicU64::new(0),
            #[cfg(feature = "extra_check")]
            active_size: AtomicU64::new(0),
        }
    }

    fn stat_inner(&self, file_id: u64) -> Option<StatInner> {
        self.map.get(&file_id).map(|stat| stat.inner)
    }

    #[cfg(any(test, feature = "extra_check"))]
    fn contains(&self, file_id: u64) -> bool {
        self.map.contains_key(&file_id)
    }

    fn mask_state(&self, file_id: u64) -> Option<MaskState> {
        let stat = self.map.get(&file_id)?;
        Some(MaskState {
            total_elems: stat.total_elems,
            present: stat.mask.is_some(),
        })
    }

    fn for_each_bucket_file(&self, mut f: impl FnMut(u64, &[u64])) {
        for entry in self.bucket_files.iter() {
            f(*entry.key(), entry.value());
        }
    }

    fn bucket_file_ids(&self, bucket_id: u64) -> Vec<u64> {
        self.bucket_files
            .get(&bucket_id)
            .map(|x| x.clone())
            .unwrap_or_default()
    }

    fn pin_mask(&self, file_id: u64) -> Option<MaskPin<'_>> {
        let mut stat = self.map.get_mut(&file_id)?;
        stat.mask.as_ref()?;
        stat.mask_pins = stat.mask_pins.saturating_add(1);
        Some(MaskPin {
            accounting: self,
            file_id,
        })
    }

    fn install_and_pin(&self, file_id: u64, mask: BitMap) -> Result<MaskPin<'_>, OpCode> {
        let mut stat = self.map.get_mut(&file_id).ok_or(OpCode::NotFound)?;
        if stat.mask.is_none() {
            stat.mask = Some(mask);
        }
        stat.mask_pins = stat.mask_pins.saturating_add(1);
        Ok(MaskPin {
            accounting: self,
            file_id,
        })
    }

    fn record_mask_use(&self, file_id: u64) {
        if self.mask_cache.get(&file_id).is_some() {
            return;
        }
        if let Some((evicted_id, _)) =
            self.mask_cache
                .add_with_evict(self.mask_capacity, file_id, ())
            && evicted_id != file_id
            && let Some(mut stat) = self.map.get_mut(&evicted_id)
            && stat.mask_pins == 0
        {
            stat.mask = None;
        }
    }

    fn insert_loaded(&self, stat: MemStat) {
        self.bucket_files
            .entry(stat.bucket_id)
            .or_default()
            .push(stat.file_id);
        let file_id = stat.file_id;
        let old = self.map.insert(file_id, stat);
        must_true!(old.is_none());
    }

    fn add_complete(&self, stat: MemStat) {
        must_true!(
            stat.active_size == stat.total_size,
            "active {}, total {}",
            stat.active_size,
            stat.total_size
        );
        #[cfg(feature = "extra_check")]
        self.update_size(stat.active_size as u64, stat.total_size as u64);
        self.insert_loaded(stat);
    }

    fn add_replacement(&self, stat: MemStat) {
        must_true!(stat.active_size <= stat.total_size);
        must_true!(stat.active_elems <= stat.total_elems);
        must_true!(stat.mask.is_some());
        #[cfg(feature = "extra_check")]
        self.update_size(stat.active_size as u64, stat.total_size as u64);
        self.insert_loaded(stat);
    }

    fn remove(&self, file_id: u64) -> Option<MemStat> {
        let removed = self.map.remove(&file_id).map(|x| x.1);
        if let Some(stat) = removed.as_ref() {
            self.mask_cache.del(&file_id);
            #[cfg(feature = "extra_check")]
            self.decrease(stat.active_size as u64, stat.total_size as u64);
            #[allow(clippy::collapsible_if)]
            if let Some(mut files) = self.bucket_files.get_mut(&stat.bucket_id) {
                if let Some(pos) = files.iter().position(|&x| x == file_id) {
                    files.swap_remove(pos);
                }
            }
        }
        removed
    }

    fn ratio(&self, bucket_id: u64, is_unsynced: impl Fn(u64) -> bool) -> Option<u64> {
        let files = self.bucket_files.get(&bucket_id)?;
        let mut total = 0u64;
        let mut active = 0u64;
        for &fid in files.value().iter() {
            if is_unsynced(fid) {
                continue;
            }
            if let Some(s) = self.map.get(&fid) {
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

    #[cfg(feature = "extra_check")]
    fn update_size(&self, active_size: u64, total_size: u64) {
        self.active_size.fetch_add(active_size, Relaxed);
        self.total_size.fetch_add(total_size, Relaxed);
    }

    #[cfg(feature = "extra_check")]
    fn sizes(&self) -> (u64, u64) {
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

    fn apply_junks(
        &self,
        _pin: MaskPin<'_>,
        file_id: u64,
        tick: u64,
        relocs: &[(u64, Reloc)],
        retired: &mut Vec<u64>,
    ) -> Result<JunkApplyOutcome, OpCode> {
        let Some(mut stat) = self.map.get_mut(&file_id) else {
            return Ok(JunkApplyOutcome::MissingFile);
        };
        if stat.mask.is_none() {
            return Err(OpCode::NotFound);
        }
        let mut changed = false;
        for &(addr, reloc) in relocs {
            if stat
                .mask
                .as_ref()
                .expect("pinned mask must exist")
                .test(reloc.seq)
            {
                continue;
            }
            #[cfg(feature = "extra_check")]
            self.active_size.fetch_sub(
                reloc.active_len() as u64,
                std::sync::atomic::Ordering::Release,
            );
            stat.update(tick, &reloc);
            retired.push(addr);
            changed = true;
        }
        Ok(JunkApplyOutcome::Updated {
            stat: changed.then(|| persist_full_mask(&stat)),
        })
    }
}

pub(crate) struct StatReaders {
    kind: FileKind,
    opt: Arc<ParsedOptions>,
    cache: ShardLru<Arc<FileReader>>,
    decoders: Arc<DecompressorPool>,
}

impl StatReaders {
    fn new(opt: Arc<ParsedOptions>, kind: FileKind) -> Self {
        Self {
            kind,
            cache: ShardLru::new(stat_handle_cache_capacity(&opt, kind)),
            decoders: DecompressorPool::new(),
            opt,
        }
    }

    fn reader(&self, file_id: u64) -> Arc<FileReader> {
        self.cache.lock_shard(file_id).get_or_insert_with(|| {
            new_reader(
                stat_file_path(&self.opt, self.kind, file_id),
                self.decoders.clone(),
                self.opt.fs.clone(),
            )
        })
    }

    fn read_at(&self, file_id: u64, addr: u64) -> BoxRef {
        self.reader(file_id).read_at(addr)
    }

    fn find_reloc(&self, file_id: u64, pos: u64) -> Option<Reloc> {
        self.reader(file_id).find_reloc(pos)
    }

    fn load_mask(&self, file_id: u64, total_elems: u32, btree: &BTree) -> Result<BitMap, OpCode> {
        let buf = btree
            .view(stat_bucket(self.kind), |txn| txn.get(file_id.to_le_bytes()))
            .map_err(OpCode::from)?;
        Ok(PersistStat::decode_mask_only(&buf, total_elems))
    }

    fn forget(&self, file_id: u64) {
        self.cache.del(file_id);
    }
}

/// per-kind facade: accounting + reader cache
pub(crate) struct StatCtx {
    accounting: FileAccounting,
    readers: StatReaders,
}

impl StatCtx {
    pub(crate) fn new(opt: Arc<ParsedOptions>, kind: FileKind) -> Self {
        Self {
            accounting: FileAccounting::new(opt.stat_mask_cache_count),
            readers: StatReaders::new(opt, kind),
        }
    }

    pub(crate) fn kind(&self) -> FileKind {
        self.readers.kind
    }

    pub(crate) fn opt(&self) -> &ParsedOptions {
        &self.readers.opt
    }

    pub(crate) fn accounting(&self) -> &FileAccounting {
        &self.accounting
    }

    pub(crate) fn readers(&self) -> &StatReaders {
        &self.readers
    }

    pub(crate) fn stat_inner(&self, file_id: u64) -> Option<StatInner> {
        self.accounting.stat_inner(file_id)
    }

    #[cfg(feature = "extra_check")]
    pub(crate) fn contains(&self, file_id: u64) -> bool {
        self.accounting.contains(file_id)
    }

    pub(crate) fn for_each_bucket_file(&self, f: impl FnMut(u64, &[u64])) {
        self.accounting.for_each_bucket_file(f);
    }

    pub(crate) fn bucket_file_ids(&self, bucket_id: u64) -> Vec<u64> {
        self.accounting.bucket_file_ids(bucket_id)
    }

    #[cfg(feature = "extra_check")]
    pub(crate) fn update_size(&self, active_size: u64, total_size: u64) {
        self.accounting.update_size(active_size, total_size);
    }

    #[cfg(feature = "extra_check")]
    pub(crate) fn sizes(&self) -> (u64, u64) {
        self.accounting.sizes()
    }

    pub(crate) fn load(&self, addr: u64, ctx: &BucketContext) -> BoxRef {
        let file_id = {
            let ivl_map = stat_intervals(self.readers.kind, ctx).read();
            must_exist!(ivl_map.find(addr), "addr {} not found in intervals", addr)
        };
        self.readers.read_at(file_id, addr)
    }

    pub(crate) fn load_mask_from_btree(
        &self,
        file_id: u64,
        total_elems: u32,
        btree: &BTree,
    ) -> Result<BitMap, OpCode> {
        self.readers.load_mask(file_id, total_elems, btree)
    }

    pub(crate) fn load_persisted_stat_inner(
        &self,
        file_id: u64,
        btree: &BTree,
    ) -> Result<StatInner, OpCode> {
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::check("mace_stat_load_persisted_inner")?;
        let buf = btree
            .view(stat_bucket(self.readers.kind), |txn| {
                txn.get(file_id.to_le_bytes())
            })
            .map_err(OpCode::from)?;
        Ok(PersistStat::decode_inner_only(&buf))
    }

    pub(crate) fn insert_loaded_stat(&self, stat: MemStat) {
        let file_id = stat.file_id;
        let has_mask = stat.mask.is_some();
        self.accounting.insert_loaded(stat);
        if has_mask {
            self.accounting.record_mask_use(file_id);
        }
    }

    pub(crate) fn add_stat_mem(&self, stat: MemStat) {
        let file_id = stat.file_id;
        let has_mask = stat.mask.is_some();
        self.accounting.add_complete(stat);
        if has_mask {
            self.accounting.record_mask_use(file_id);
        }
    }

    fn add_replacement_stat_mem(&self, stat: MemStat) {
        let file_id = stat.file_id;
        self.accounting.add_replacement(stat);
        self.accounting.record_mask_use(file_id);
    }

    pub(crate) fn prepare_stat_intervals(
        &self,
        mut fstats: Vec<MemStat>,
        relocs: HashMap<u64, (u64, LenSeq)>,
        cut: &PublishCut<'_>,
    ) -> (Vec<MemStat>, Vec<PersistStat>) {
        for fstat in &fstats {
            must_true!(eq fstat.active_size, fstat.total_size);
        }
        let output_index: HashMap<u64, usize> = fstats
            .iter()
            .enumerate()
            .map(|(idx, stat)| (stat.file_id, idx))
            .collect();

        apply_collected_junks(&mut fstats, &output_index, &relocs, cut.collected());

        let mut stats = Vec::with_capacity(fstats.len());
        for fstat in &fstats {
            stats.push(persist_full_mask(fstat));
        }
        (fstats, stats)
    }

    pub(crate) fn publish_stat_intervals(&self, fstats: Vec<MemStat>, obsoleted: &[u64]) {
        for &id in obsoleted {
            self.remove_stat(id);
        }
        for fstat in fstats {
            self.add_replacement_stat_mem(fstat);
        }
    }

    pub(crate) fn remove_stat_interval(&self, data: &[u64]) {
        for x in data {
            self.remove_stat(*x);
        }
    }

    pub(crate) fn remove_stat(&self, file_id: u64) {
        self.accounting.remove(file_id);
        self.readers.forget(file_id);
    }

    pub(crate) fn bucket_ratio(
        &self,
        bucket_id: u64,
        is_unsynced: impl Fn(u64) -> bool,
    ) -> Option<u64> {
        self.accounting.ratio(bucket_id, is_unsynced)
    }
}

#[derive(Clone, Copy)]
struct MaskState {
    total_elems: u32,
    #[cfg_attr(not(feature = "extra_check"), allow(dead_code))]
    present: bool,
}

enum JunkApplyOutcome {
    Updated { stat: Option<PersistStat> },
    MissingFile,
}

pub(crate) struct MaskPin<'a> {
    accounting: &'a FileAccounting,
    file_id: u64,
}

impl Drop for MaskPin<'_> {
    fn drop(&mut self) {
        let mut reinsert = false;
        if let Some(mut stat) = self.accounting.map.get_mut(&self.file_id) {
            stat.mask_pins = stat.mask_pins.saturating_sub(1);
            // an eviction that ran while this pin was held skipped the mask; re-admit it once
            // the last pin is gone so the mask cache budget still bounds the resident masks
            reinsert = stat.mask_pins == 0 && stat.mask.is_some();
            drop(stat);
        }
        if reinsert {
            self.accounting.record_mask_use(self.file_id);
        }
    }
}

pub(crate) fn ensure_mask<'a>(
    accounting: &'a FileAccounting,
    readers: &StatReaders,
    btree: &BTree,
    file_id: u64,
) -> Result<MaskPin<'a>, OpCode> {
    if let Some(pin) = accounting.pin_mask(file_id) {
        accounting.record_mask_use(file_id);
        return Ok(pin);
    }
    let state = accounting.mask_state(file_id).ok_or(OpCode::NotFound)?;
    #[cfg(feature = "failpoints")]
    crate::utils::failpoint::crash("mace_stat_mask_before_load");
    let mask = readers.load_mask(file_id, state.total_elems, btree)?;
    let pin = accounting.install_and_pin(file_id, mask)?;
    accounting.record_mask_use(file_id);
    Ok(pin)
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn pinned_mask_survives_lru_eviction() {
        let accounting = FileAccounting::new(1);
        accounting.insert_loaded(mem_stat(1));
        accounting.record_mask_use(1);
        let pin = accounting.pin_mask(1).expect("mask exists");

        accounting.insert_loaded(mem_stat(2));
        accounting.record_mask_use(2);

        assert!(accounting.map.get(&1).unwrap().mask.is_some());
        assert_eq!(accounting.map.get(&1).unwrap().mask_pins, 1);
        drop(pin);
        assert_eq!(accounting.map.get(&1).unwrap().mask_pins, 0);
        // the eviction skipped this mask while it was pinned, so the unpin must re-admit it
        assert!(accounting.mask_cache.get(&1).is_some());
    }

    #[test]
    fn remove_clears_mask_cache_and_bucket_index() {
        let accounting = FileAccounting::new(1);
        accounting.add_complete(mem_stat(1));
        accounting.record_mask_use(1);

        accounting.remove(1);

        assert!(!accounting.contains(1));
        assert!(accounting.mask_cache.get(&1).is_none());
        assert!(accounting.bucket_file_ids(1).is_empty());
    }

    #[test]
    fn ensure_mask_returns_a_pin_on_the_fast_path() {
        let path = crate::RandomPath::tmp();
        std::fs::create_dir_all(&*path).expect("create temp dir");
        let opt = crate::utils::options::Options::new(&*path)
            .validate()
            .expect("valid options");
        let readers = StatReaders::new(Arc::new(opt), FileKind::Data);
        let btree = BTree::open(path.join("btree")).expect("open btree");
        let accounting = FileAccounting::new(1);
        accounting.insert_loaded(mem_stat(1));
        accounting.record_mask_use(1);

        let pin = ensure_mask(&accounting, &readers, &btree, 1).expect("resident mask");
        assert_eq!(accounting.map.get(&1).unwrap().mask_pins, 1);
        drop(pin);
        assert_eq!(accounting.map.get(&1).unwrap().mask_pins, 0);
    }

    #[test]
    fn replacement_mask_starts_unpinned() {
        let mut stat = mem_stat(1);
        stat.mask_pins = 7;
        let replacement = stat.clone_mem();
        assert_eq!(replacement.mask_pins, 0);
    }
}
