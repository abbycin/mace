use crate::must_true;
use btree_store::BTree;
use crc32c::Crc32cHasher;
use dashmap::DashMap;
use parking_lot::Mutex;
use std::{
    collections::{BTreeMap, HashMap},
    hash::Hasher,
    ops::Deref,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{
            AtomicU8, AtomicU64,
            Ordering::{AcqRel, Acquire, Relaxed, Release, SeqCst},
        },
    },
};

use crate::{
    io::{File, FileSystem, GatherIO},
    map::{
        buffer::BucketContext,
        data::{FileVersion, MetaReader},
    },
    must_exist, must_ok,
    types::refbox::BoxRef,
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
    total_size: AtomicU64,
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
            total_size: AtomicU64::new(0),
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

    pub(crate) fn cancel_collect_junks(&self) {
        self.common.junk.cancel();
    }

    #[cfg(feature = "extra_check")]
    pub(crate) fn collected_junk_count(&self) -> usize {
        self.common.junk.collected_count()
    }

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

    fn load_mask_from_btree(
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

    pub(crate) fn load_mask_clone(&self, file_id: u64, btree: &BTree) -> Result<BitMap, OpCode> {
        self.ensure_mask(file_id, btree)?;
        let stat = self.map.get(&file_id).ok_or(OpCode::NotFound)?;
        Ok(stat.mask.as_ref().expect("mask loaded").clone())
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
        self.update_size(stat.active_size as u64, stat.total_size as u64);
        self.insert_loaded_stat(stat);
    }

    fn add_replacement_stat_mem(&self, stat: MemStat) {
        must_true!(stat.active_size <= stat.total_size);
        must_true!(stat.active_elems <= stat.total_elems);
        must_true!(stat.mask.is_some());
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
        tick: u64,
        junks: &[u64],
        ctx: &BucketContext,
        btree: &BTree,
        is_retired: impl Fn(u64) -> bool,
    ) -> Result<Vec<PersistStat>, OpCode> {
        let mut pending = junks.to_vec();
        let mut updates = BTreeMap::<u64, PersistStat>::new();
        while !pending.is_empty() {
            let (grouped, mut retry) = {
                let lk = stat_intervals(self.kind, ctx).read();
                let mut grouped = BTreeMap::<u64, Vec<u64>>::new();
                let mut retry = Vec::new();
                for &addr in &pending {
                    let Some(file_id) = lk.find(addr) else {
                        continue;
                    };
                    if self.common.junk.is_publishing() {
                        retry.push(addr);
                    } else {
                        grouped.entry(file_id).or_default().push(addr);
                    }
                }
                (grouped, retry)
            };

            for (file_id, addrs) in grouped {
                if is_retired(file_id) {
                    continue;
                }
                if let Err(err) = self.ensure_mask(file_id, btree) {
                    if is_retired(file_id) {
                        continue;
                    }
                    let lk = stat_intervals(self.kind, ctx).read();
                    let mut stale = false;
                    for addr in addrs {
                        match lk.find(addr) {
                            Some(current) if current == file_id => return Err(err),
                            Some(_) => retry.push(addr),
                            None => stale = true,
                        }
                    }
                    if stale {
                        continue;
                    }
                    continue;
                }
                if let Some(mut stat) = self.map.get_mut(&file_id) {
                    let mut changed = false;
                    for addr in addrs {
                        let Some(reloc) = self.try_get_reloc(file_id, addr) else {
                            continue;
                        };
                        if stat.mask.as_ref().expect("mask loaded").test(reloc.seq) {
                            continue;
                        }
                        let capture = self.update_stat(&mut stat, addr, &reloc, tick);
                        if capture == JunkCapture::Publishing {
                            retry.push(addr);
                            continue;
                        }
                        changed = true;
                    }
                    if changed {
                        updates.insert(file_id, persist_full_mask(&stat));
                    }
                } else {
                    let lk = stat_intervals(self.kind, ctx).read();
                    for addr in addrs {
                        match lk.find(addr) {
                            Some(current) if current == file_id => return Err(OpCode::NotFound),
                            Some(_) => retry.push(addr),
                            None => {}
                        }
                    }
                }
            }

            if !retry.is_empty() {
                #[cfg(feature = "extra_check")]
                if self.common.junk.is_publishing() {
                    crate::testing::fire_gc_stat_sync_point(
                        match self.kind {
                            FileKind::Data => {
                                crate::testing::GcStatSyncPoint::DataCheckpointPublishingWait
                            }
                            FileKind::Blob => {
                                crate::testing::GcStatSyncPoint::BlobCheckpointPublishingWait
                            }
                        },
                        ctx.bucket_id,
                        &self.common.opt.db_root,
                    );
                }
                self.common.junk.wait_publish();
            }
            pending = retry;
        }
        Ok(updates.into_values().collect())
    }

    fn update_stat(&self, stat: &mut MemStat, junk: u64, reloc: &Reloc, tick: u64) -> JunkCapture {
        self.common.junk.apply_or_defer(stat.file_id, junk, || {
            self.active_size
                .fetch_sub(reloc.active_len() as u64, Release);
            stat.update(tick, reloc);
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
    state: AtomicU8,
    junks: Mutex<HashMap<u64, Vec<u64>>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum JunkCapture {
    Idle,
    Collected,
    Publishing,
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
            state: AtomicU8::new(Self::IDLE),
            junks: Mutex::new(HashMap::new()),
        }
    }

    fn start(&self, file_ids: &[u64]) {
        let mut junks = self.junks.lock();
        must_true!(eq self.state.load(Acquire), Self::IDLE);
        must_true!(junks.is_empty());
        for &file_id in file_ids {
            junks.entry(file_id).or_default();
        }
        self.state.store(Self::COLLECTING, SeqCst);
    }

    fn cancel(&self) {
        let mut junks = self.junks.lock();
        must_true!(eq self.state.load(Acquire), Self::COLLECTING);
        junks.clear();
        self.state.store(Self::IDLE, SeqCst);
    }

    fn begin_publish(&self) -> HashMap<u64, Vec<u64>> {
        let mut junks = self.junks.lock();
        must_true!(eq self.state.load(Acquire), Self::COLLECTING);
        self.state.store(Self::PUBLISHING, SeqCst);
        std::mem::take(&mut *junks)
    }

    fn finish_publish(&self) {
        must_true!(eq self.state.load(Acquire), Self::PUBLISHING);
        self.state.store(Self::IDLE, SeqCst);
    }

    fn is_publishing(&self) -> bool {
        self.state.load(Acquire) == Self::PUBLISHING
    }

    fn wait_publish(&self) {
        while self.is_publishing() {
            std::thread::yield_now();
        }
    }

    #[cfg(feature = "extra_check")]
    fn collected_count(&self) -> usize {
        self.junks.lock().values().map(Vec::len).sum()
    }

    fn apply_or_defer(&self, file_id: u64, junk: u64, update: impl FnOnce()) -> JunkCapture {
        let mut junks = self.junks.lock();
        match self.state.load(Acquire) {
            Self::IDLE => {
                update();
                JunkCapture::Idle
            }
            Self::COLLECTING => {
                if let Some(q) = junks.get_mut(&file_id) {
                    update();
                    q.push(junk);
                    JunkCapture::Collected
                } else {
                    update();
                    JunkCapture::Idle
                }
            }
            Self::PUBLISHING => JunkCapture::Publishing,
            _ => unreachable!("invalid junk collector state"),
        }
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

impl FileReader {
    fn read_exact(&self, buf: &mut [u8], mut off: u64) {
        let mut done = 0;
        while done < buf.len() {
            let got = must_ok!(self.file.read(&mut buf[done..], off), "must read payload");
            if got == 0 {
                must_ok!(
                    Result::<(), OpCode>::Err(OpCode::Corruption),
                    "incomplete payload read: expect {} get {}",
                    buf.len(),
                    done
                );
            }
            done += got;
            off += got as u64;
        }
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
        let m = must_exist!(self.find_reloc(pos), "can't find addr {} in reloc", pos);
        let raw_len = m.raw_len() as usize;
        let mut page = match self.version {
            FileVersion::V1 => BoxRef::alloc_exact(BoxRef::real_size_from_dump(m.raw_len()), pos),
            _ => must_ok!(
                Result::<BoxRef, OpCode>::Err(OpCode::BadVersion),
                "unsupported file version {}",
                self.version.raw()
            ),
        };
        let dst = page.load_slice(raw_len);

        if !m.is_compressed() {
            let mut crc = Crc32cHasher::default();
            self.read_exact(dst, m.off as u64);
            crc.write(dst);
            let actual_crc = crc.finish() as u32;
            if actual_crc != m.crc {
                must_ok!(
                    Result::<(), OpCode>::Err(OpCode::Corruption),
                    "checksum mismatch, expect {} get {}, key {pos}",
                    { m.crc },
                    actual_crc
                );
            }
        } else {
            let actual_crc = must_ok!(self.decoders.with_decoder(|decoder| {
                decoder.decode_reader_into(
                    &self.file,
                    m.off as u64,
                    m.compressed_len() as usize,
                    dst,
                )
            }));
            if actual_crc != m.crc {
                must_ok!(
                    Result::<(), OpCode>::Err(OpCode::Corruption),
                    "checksum mismatch, expect {} get {}, key {pos}",
                    { m.crc },
                    actual_crc
                );
            }
        }

        match self.version {
            FileVersion::V1 => must_ok!(
                page.decode_persisted_v1_in_place(pos, raw_len),
                "decode persisted record at {}, raw_len {}",
                pos,
                raw_len
            ),
            _ => must_ok!(
                Result::<(), OpCode>::Err(OpCode::BadVersion),
                "unsupported file version {}",
                self.version.raw()
            ),
        }
        page
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, collections::HashMap};

    use crate::{
        meta::{MemStat, StatInner},
        utils::{bitmap::BitMap, data::LenSeq},
    };

    use super::{JunkCapture, JunkCollector, apply_collected_junks, persist_full_mask};

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
        let updates = Cell::new(0);
        collector.start(&[3, 5]);

        assert_eq!(
            collector.apply_or_defer(3, 100, || updates.set(updates.get() + 1)),
            JunkCapture::Collected
        );
        assert_eq!(
            collector.apply_or_defer(4, 200, || updates.set(updates.get() + 1)),
            JunkCapture::Idle
        );

        let collected = collector.begin_publish();
        assert_eq!(collected.get(&3), Some(&vec![100]));
        assert_eq!(collected.get(&5), Some(&Vec::new()));
        assert_eq!(
            collector.apply_or_defer(3, 300, || updates.set(updates.get() + 1)),
            JunkCapture::Publishing
        );
        assert_eq!(updates.get(), 2, "publishing must defer the stat update");

        collector.finish_publish();
        assert_eq!(
            collector.apply_or_defer(3, 400, || updates.set(updates.get() + 1)),
            JunkCapture::Idle
        );
        assert_eq!(updates.get(), 3);

        collector.start(&[7]);
        collector.cancel();
        assert_eq!(
            collector.apply_or_defer(7, 500, || updates.set(updates.get() + 1)),
            JunkCapture::Idle
        );
        assert_eq!(updates.get(), 4);
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
