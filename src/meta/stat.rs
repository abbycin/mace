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
            AtomicBool, AtomicU64,
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

    pub(crate) fn start_collect_junks(&self) {
        // Release is enough for ARM, but it's no-op on x86, so use SeqCst instead
        self.common.junk.start();
    }

    pub(crate) fn update_size(&self, active_size: u64, total_size: u64) {
        self.active_size.fetch_add(active_size, Relaxed);
        self.total_size.fetch_add(total_size, Relaxed);
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

    pub(crate) fn update_stat_intervals(
        &self,
        mut fstats: Vec<MemStat>,
        relocs: HashMap<u64, (u64, LenSeq)>,
        obsoleted: &[u64],
    ) -> Vec<PersistStat> {
        for fstat in &fstats {
            must_true!(eq fstat.active_size, fstat.total_size);
        }
        let output_index: HashMap<u64, usize> = fstats
            .iter()
            .enumerate()
            .map(|(idx, stat)| (stat.file_id, idx))
            .collect();

        // apply deactived frames while we are performing compaction
        let mut seqs = vec![];
        let mut junks = self.common.junk.take();
        for (_, q) in junks.iter_mut() {
            for &addr in q.iter() {
                if let Some(ls) = relocs.get(&addr)
                    && let Some(&idx) = output_index.get(&ls.0)
                {
                    let fstat = &mut fstats[idx];
                    fstat.active_size -= ls.1.active_len() as usize;
                    fstat.active_elems -= 1;
                    fstat.mask.as_mut().expect("mask loaded").set(ls.1.seq);
                    seqs.push((ls.0, ls.1.seq));
                }
            }
        }

        for &id in obsoleted {
            self.remove_stat(id);
            self.common.cache.del(id);
        }

        let mut stats = Vec::with_capacity(fstats.len());
        for fstat in fstats {
            let inactive = seqs
                .iter()
                .filter_map(|(file_id, seq)| (*file_id == fstat.file_id).then_some(*seq))
                .collect();
            let stat = PersistStat::from_parts(fstat.inner, inactive);
            self.add_stat_mem(fstat);
            stats.push(stat);
        }
        stats
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
        let grouped: BTreeMap<u64, Vec<u64>> = {
            let lk = stat_intervals(self.kind, ctx).read();
            let mut grouped = BTreeMap::<u64, Vec<u64>>::new();
            for &addr in junks {
                // race condition: gc might have already removed the interval containing this junk
                // 1. flush thread holds a junk addr
                // 2. gc thread rewrites the file containing addr, and since it is junk, it is not moved
                // 3. gc thread removes the interval from interval map if it becomes empty
                // 4. flush thread tries to find the file_id of the junk, but the interval is gone
                if let Some(file_id) = lk.find(addr) {
                    grouped.entry(file_id).or_default().push(addr);
                }
            }
            grouped
        };

        // Merge all updates on the same file_id into one stat record to avoid
        // generating many duplicate per-file meta puts in a single publish round.
        let mut v: Vec<PersistStat> = Vec::with_capacity(grouped.len());
        for (file_id, addrs) in grouped {
            if is_retired(file_id) {
                continue;
            }
            if let Err(err) = self.ensure_mask(file_id, btree) {
                // reclaim marks a file retired before removing its in-memory stat. A
                // second check distinguishes that valid handoff from a retained file
                // whose newly observed junk would otherwise be silently forgotten.
                if is_retired(file_id) {
                    continue;
                }
                return Err(err);
            }
            if let Some(mut stat) = self.map.get_mut(&file_id) {
                let mut changed = false;
                for addr in addrs {
                    // race condition: gc might have already removed the interval containing this junk
                    let Some(reloc) = self.try_get_reloc(file_id, addr) else {
                        // interval ranges can cover sparse logical addresses, so a junk addr may
                        // resolve to a file without having ever been written into its reloc table
                        continue;
                    };
                    if stat.mask.as_ref().expect("mask loaded").test(reloc.seq) {
                        continue;
                    }
                    self.update_stat(&mut stat, addr, &reloc, tick);
                    changed = true;
                }
                if changed {
                    // Metadata updates replace the complete stat value. Persist every inactive
                    // sequence, not just this checkpoint's delta, so a later checkpoint cannot
                    // resurrect previously collected garbage after reopen.
                    v.push(persist_full_mask(&stat));
                }
            }
        }
        Ok(v)
    }

    pub(crate) fn update_stat(&self, stat: &mut MemStat, junk: u64, reloc: &Reloc, tick: u64) {
        self.active_size
            .fetch_sub(reloc.active_len() as u64, Release);
        stat.update(tick, reloc);
        self.common.junk.push_if_collecting(stat.file_id, junk);
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

struct JunkCollector {
    should_collect_junk: AtomicBool,
    junks: Mutex<HashMap<u64, Vec<u64>>>,
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
    fn new() -> Self {
        Self {
            should_collect_junk: AtomicBool::new(false),
            junks: Mutex::new(HashMap::new()),
        }
    }

    fn start(&self) {
        self.should_collect_junk.store(true, SeqCst);
    }

    fn stop(&self) {
        self.should_collect_junk.store(false, SeqCst);
    }

    fn take(&self) -> HashMap<u64, Vec<u64>> {
        let mut junklk = self.junks.lock();
        self.stop();
        std::mem::take(&mut *junklk)
    }

    fn push_if_collecting(&self, file_id: u64, junk: u64) {
        let mut m = self.junks.lock();
        #[allow(clippy::collapsible_if)]
        if self.should_collect_junk.load(Acquire)
            && let Some(q) = m.get_mut(&file_id)
        {
            q.push(junk);
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
    use crate::{
        meta::{MemStat, StatInner},
        utils::bitmap::BitMap,
    };

    use super::persist_full_mask;

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
}
