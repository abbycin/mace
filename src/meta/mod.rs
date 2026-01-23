use btree_store::BTree;
use parking_lot::{Mutex, RwLock};
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
            fence,
        },
    },
};

use crc32c::Crc32cHasher;
use dashmap::DashMap;

use crate::{
    io::{File, GatherIO},
    map::{
        IFooter,
        data::{BlobFooter, DataFooter, MetaReader},
        table::PageMap,
    },
    meta::entry::BlobStat,
    types::refbox::BoxRef,
    utils::{
        OpCode,
        data::{LenSeq, Reloc},
        interval::IntervalMap,
        lru::ShardLru,
        options::ParsedOptions,
    },
};

pub(crate) const BUCKET_NUMERICS: &str = "numerics";
pub(crate) const NUMERICS_KEY: &str = "numeric";
pub(crate) const ORPHAN_DATA: &str = "orphan_data";
pub(crate) const ORPHAN_BLOB: &str = "orphan_blob";
pub(crate) const BUCKET_PAGE_TABLE: &str = "page_table";
pub(crate) const BUCKET_DATA_STAT: &str = "data_stat";
pub(crate) const BUCKET_BLOB_STAT: &str = "blob_stat";
pub(crate) const BUCKET_DATA_INTERVAL: &str = "data_interval";
pub(crate) const BUCKET_BLOB_INTERVAL: &str = "blob_interval";
pub(crate) const BUCKET_OBSOLETE_DATA: &str = "obsolete_data";
pub(crate) const BUCKET_OBSOLETE_BLOB: &str = "obsolete_blob";
pub(crate) const BUCKET_VERSION: &str = "version";
pub(crate) const VERSION_KEY: &str = "current_version";
/// storage format version
pub(crate) const CURRENT_VERSION: u64 = 1;

pub(crate) mod builder;
mod entry;
pub use entry::{
    BlobStatInner, DataStat, DataStatInner, DelInterval, Delete, IntervalPair, MemBlobStat,
    MemDataStat, MetaKind, Numerics, PageTable,
};

pub(crate) trait IMetaCodec {
    fn packed_size(&self) -> usize;

    fn encode(&self, to: &mut [u8]);

    fn decode(src: &[u8]) -> Self;
}

pub(crate) trait MetaRecord: IMetaCodec {
    fn record(&self, kind: MetaKind, ops: &mut HashMap<&str, Vec<MetaOp>>);
}

impl MetaRecord for Numerics {
    fn record(&self, _kind: MetaKind, ops: &mut HashMap<&str, Vec<MetaOp>>) {
        let mut buf = vec![0u8; self.packed_size()];
        self.encode(&mut buf);
        ops.entry(BUCKET_NUMERICS)
            .or_default()
            .push(MetaOp::Put(NUMERICS_KEY.as_bytes().to_vec(), buf));
    }
}

impl MetaRecord for PageTable {
    fn record(&self, _kind: MetaKind, ops: &mut HashMap<&str, Vec<MetaOp>>) {
        let bucket_ops = ops.entry(BUCKET_PAGE_TABLE).or_default();
        for (&pid, &addr) in self.iter() {
            bucket_ops.push(MetaOp::Put(
                pid.to_be_bytes().to_vec(),
                addr.to_be_bytes().to_vec(),
            ));
        }
    }
}

impl MetaRecord for DataStat {
    fn record(&self, kind: MetaKind, ops: &mut HashMap<&str, Vec<MetaOp>>) {
        let mut buf = vec![0u8; self.packed_size()];
        self.encode(&mut buf);
        let bucket = if kind == MetaKind::DataStat {
            BUCKET_DATA_STAT
        } else {
            BUCKET_BLOB_STAT
        };
        ops.entry(bucket)
            .or_default()
            .push(MetaOp::Put(self.file_id.to_be_bytes().to_vec(), buf));
    }
}

impl MetaRecord for BlobStat {
    fn record(&self, _kind: MetaKind, ops: &mut HashMap<&str, Vec<MetaOp>>) {
        let mut buf = vec![0u8; self.packed_size()];
        self.encode(&mut buf);
        ops.entry(BUCKET_BLOB_STAT)
            .or_default()
            .push(MetaOp::Put(self.file_id.to_be_bytes().to_vec(), buf));
    }
}

impl MetaRecord for IntervalPair {
    fn record(&self, kind: MetaKind, ops: &mut HashMap<&str, Vec<MetaOp>>) {
        let mut buf = vec![0u8; self.packed_size()];
        self.encode(&mut buf);
        let bucket = if kind == MetaKind::DataInterval {
            BUCKET_DATA_INTERVAL
        } else {
            BUCKET_BLOB_INTERVAL
        };
        ops.entry(bucket)
            .or_default()
            .push(MetaOp::Put(self.lo_addr.to_be_bytes().to_vec(), buf));
    }
}

impl MetaRecord for Delete {
    fn record(&self, kind: MetaKind, ops: &mut HashMap<&str, Vec<MetaOp>>) {
        match kind {
            MetaKind::DataDelete | MetaKind::BlobDelete => {
                let (obs_bucket, stat_bucket) = if kind == MetaKind::DataDelete {
                    (BUCKET_OBSOLETE_DATA, BUCKET_DATA_STAT)
                } else {
                    (BUCKET_OBSOLETE_BLOB, BUCKET_BLOB_STAT)
                };
                for &id in self.iter() {
                    let key = id.to_be_bytes().to_vec();
                    ops.entry(obs_bucket)
                        .or_default()
                        .push(MetaOp::Put(key.clone(), vec![]));
                    ops.entry(stat_bucket).or_default().push(MetaOp::Del(key));
                }
            }
            MetaKind::DataDeleteDone | MetaKind::BlobDeleteDone => {
                let bucket = if kind == MetaKind::DataDeleteDone {
                    BUCKET_OBSOLETE_DATA
                } else {
                    BUCKET_OBSOLETE_BLOB
                };
                let bucket_ops = ops.entry(bucket).or_default();
                for &id in self.iter() {
                    bucket_ops.push(MetaOp::Del(id.to_be_bytes().to_vec()));
                }
            }
            _ => unreachable!(),
        }
    }
}

impl MetaRecord for DelInterval {
    fn record(&self, kind: MetaKind, ops: &mut HashMap<&str, Vec<MetaOp>>) {
        let bucket = if kind == MetaKind::DataDelInterval {
            BUCKET_DATA_INTERVAL
        } else {
            BUCKET_BLOB_INTERVAL
        };
        let bucket_ops = ops.entry(bucket).or_default();
        for &lo in self.iter() {
            bucket_ops.push(MetaOp::Del(lo.to_be_bytes().to_vec()));
        }
    }
}

#[derive(Clone)]
pub(crate) enum MetaOp {
    Put(Vec<u8>, Vec<u8>),
    Del(Vec<u8>),
}

pub(crate) struct Manifest {
    pub(crate) numerics: Arc<Numerics>,
    pub(crate) map: Arc<PageMap>,
    pub(crate) data_stat: DataStatCtx,
    pub(crate) blob_stat: BlobStatCtx,
    obsolete_data: Mutex<Vec<u64>>,
    obsolete_blob: Mutex<Vec<u64>>,
    opt: Arc<ParsedOptions>,
    pub(crate) btree: BTree,
}

pub(crate) struct Txn<'a> {
    manifest: &'a Manifest,
    btree: BTree,
    // bucket_name -> operations
    ops: HashMap<&'a str, Vec<MetaOp>>,
}

impl<'a> Txn<'a> {
    pub(crate) fn commit(mut self) -> u64 {
        self.internal_commit();
        0
    }

    fn internal_commit(&mut self) -> u64 {
        if self.ops.is_empty() {
            return 0;
        }
        loop {
            // btree-store supports SI. refresh handle to start a fresh session
            self.btree = self.manifest.btree.clone();

            // perform an atomic multi-bucket commit
            // all updates across different buckets are applied and flushed to disk
            // in a single SuperBlock write, significantly reducing I/O overhead
            let res = self.btree.exec_multi(|multi_txn| {
                for (bucket, bucket_ops) in &self.ops {
                    multi_txn.execute(bucket, |tree_txn| {
                        for op in bucket_ops {
                            match op {
                                MetaOp::Put(k, v) => tree_txn.put(k, v)?,
                                MetaOp::Del(k) => tree_txn.del(k)?,
                            }
                        }
                        Ok(())
                    })?;
                }
                Ok(())
            });

            match res {
                Ok(_) => {
                    self.ops.clear();
                    break;
                }
                Err(btree_store::Error::Conflict) => {
                    // retry with a refreshed session handle
                    continue;
                }
                Err(e) => {
                    log::error!("Metadata multi-bucket commit fail: {:?}", e);
                    panic!("Metadata multi-bucket commit fail: {:?}", e)
                }
            }
        }
        0
    }

    pub(crate) fn record<T>(&mut self, kind: MetaKind, x: &T)
    where
        T: MetaRecord,
    {
        x.record(kind, &mut self.ops);
    }
}

struct FileReader {
    file: File,
    map: HashMap<u64, Reloc>,
}

fn new_reader<T: IFooter>(path: PathBuf) -> Result<FileReader, OpCode> {
    let mut loader = MetaReader::<T>::new(&path)?;
    let relocs = loader.get_reloc()?;
    let mut map = HashMap::with_capacity(relocs.len());
    for x in relocs {
        map.insert(x.key, x.val);
    }

    let file = loader.take();
    Ok(FileReader { file, map })
}

impl FileReader {
    fn read_at(&self, pos: u64) -> Result<BoxRef, OpCode> {
        let m = self.map.get(&pos).expect("never happen");
        let real_size = BoxRef::real_size_from_dump(m.len);
        let mut p = BoxRef::alloc_exact(real_size, pos);
        let mut crc = Crc32cHasher::default();

        let dst = p.load_slice();
        self.file.read(dst, m.off as u64).map_err(OpCode::from)?;
        crc.write(dst);
        let actual_crc = crc.finish() as u32;
        if actual_crc != m.crc {
            log::error!(
                "checksum mismatch, expect {} get {}, key {pos}",
                { m.crc },
                actual_crc
            );
            return Err(OpCode::Corruption);
        }

        Ok(p)
    }
}

impl Manifest {
    fn new(opt: Arc<ParsedOptions>) -> Self {
        let path = opt.manifest();
        let btree = BTree::open(path).expect("can't open btree-store");
        let buckets = [
            BUCKET_NUMERICS,
            BUCKET_PAGE_TABLE,
            BUCKET_DATA_STAT,
            BUCKET_BLOB_STAT,
            BUCKET_DATA_INTERVAL,
            BUCKET_BLOB_INTERVAL,
            BUCKET_OBSOLETE_DATA,
            BUCKET_OBSOLETE_BLOB,
            BUCKET_VERSION,
        ];

        for name in buckets {
            btree
                .exec(name, |_| Ok(()))
                .expect("can't ensure bucket exists");
        }

        Self {
            numerics: Arc::new(Numerics::default()),
            map: Arc::new(PageMap::default()),
            data_stat: DataStatCtx::new(opt.clone()),
            blob_stat: BlobStatCtx::new(opt.clone()),
            obsolete_data: Mutex::new(Vec::new()),
            obsolete_blob: Mutex::new(Vec::new()),
            opt,
            btree,
        }
    }

    pub(crate) fn init(&mut self, data_id: u64, blob_id: u64) {
        self.numerics.next_data_id.store(data_id + 1, Relaxed);
        self.numerics.next_blob_id.store(blob_id + 1, Relaxed);
    }

    pub(crate) fn add_data_stat(&self, stat: MemDataStat, ivl: IntervalPair) {
        self.data_stat.add_stat_interval(stat, ivl);
    }

    pub(crate) fn add_blob_stat(&self, stat: MemBlobStat, ivl: IntervalPair) {
        self.blob_stat.add_stat_interval(stat, ivl);
    }

    pub(crate) fn update_data_stat_interval(
        &self,
        fstat: MemDataStat,
        relocs: HashMap<u64, LenSeq>,
        obsoleted: &[u64],
        del_intervals: &[u64],
        remap_intervals: &[IntervalPair],
    ) -> DataStat {
        self.data_stat.update_stat_interval(
            fstat,
            relocs,
            obsoleted,
            del_intervals,
            remap_intervals,
        )
    }

    pub(crate) fn update_blob_stat_interval(
        &self,
        bstat: MemBlobStat,
        relocs: HashMap<u64, LenSeq>,
        obsoleted: &[u64],
        del_intervals: &[u64],
        remap_intervals: &[IntervalPair],
    ) -> BlobStat {
        self.blob_stat.update_stat_interval(
            bstat,
            relocs,
            obsoleted,
            del_intervals,
            remap_intervals,
        )
    }

    /// the junks must be ordered
    pub(crate) fn apply_data_junks(&self, tick: u64, junks: &[u64]) -> Vec<DataStat> {
        self.data_stat.apply_junks(tick, junks)
    }

    /// the junks must be ordered
    pub(crate) fn apply_blob_junks(&self, junks: &[u64]) -> Vec<BlobStat> {
        self.blob_stat.apply_junks(junks)
    }

    pub(crate) fn begin(&self) -> Txn<'_> {
        let btree = self.btree.clone();
        Txn {
            manifest: self,
            btree,
            ops: HashMap::new(),
        }
    }

    pub(crate) fn load_data(&self, addr: u64) -> Result<BoxRef, OpCode> {
        self.data_stat.load(addr)
    }

    pub(crate) fn load_blob(&self, addr: u64) -> Result<BoxRef, OpCode> {
        self.blob_stat.load(addr)
    }

    pub(crate) fn save_obsolete_data(&self, id: &[u64]) {
        if !id.is_empty() {
            let mut lk = self.obsolete_data.lock();
            lk.extend_from_slice(id);
        }
    }

    pub(crate) fn save_obsolete_blob(&self, id: &[u64]) {
        if !id.is_empty() {
            let mut lk = self.obsolete_blob.lock();
            lk.extend_from_slice(id);
        }
    }

    pub(crate) fn delete_files(&self) {
        let mut data_ids = Vec::new();
        {
            let mut lk = self.obsolete_data.lock();
            lk.retain(|&id| {
                let path = self.opt.data_file(id);
                if !path.exists() || std::fs::remove_file(&path).is_ok() {
                    data_ids.push(id);
                    false
                } else {
                    true
                }
            });
        }
        let mut blob_ids = Vec::new();
        {
            let mut lk = self.obsolete_blob.lock();
            lk.retain(|&id| {
                let path = self.opt.blob_file(id);
                if !path.exists() || std::fs::remove_file(&path).is_ok() {
                    blob_ids.push(id);
                    false
                } else {
                    true
                }
            });
        }

        if !data_ids.is_empty() || !blob_ids.is_empty() {
            #[cfg(unix)]
            if let Ok(f) = std::fs::File::open(self.opt.data_root()) {
                let _ = f.sync_all();
            }

            let mut txn = self.begin();
            if !data_ids.is_empty() {
                txn.record(MetaKind::DataDeleteDone, &Delete::from(data_ids));
            }
            if !blob_ids.is_empty() {
                txn.record(MetaKind::BlobDeleteDone, &Delete::from(blob_ids));
            }
            txn.commit();
        }
    }
}

pub(crate) struct DataStatCtx {
    /// interval to file_id map, multiple intervals may point to same file_id after compaction
    pub(crate) interval: RwLock<IntervalMap>,
    map: DashMap<u64, MemDataStat>,
    junks: Mutex<HashMap<u64, Vec<u64>>>,
    cache: ShardLru<FileReader>,
    opt: Arc<ParsedOptions>,
    pub(crate) should_collect_junk: AtomicBool,
    total_size: AtomicU64,
    active_size: AtomicU64,
}

impl Deref for DataStatCtx {
    type Target = DashMap<u64, MemDataStat>;
    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl DataStatCtx {
    fn new(opt: Arc<ParsedOptions>) -> Self {
        Self {
            interval: RwLock::new(IntervalMap::new()),
            map: DashMap::new(),
            junks: Mutex::new(HashMap::new()),
            cache: ShardLru::new(opt.data_handle_cache_capacity),
            opt,
            should_collect_junk: AtomicBool::new(false),
            total_size: AtomicU64::new(0),
            active_size: AtomicU64::new(0),
        }
    }

    pub(crate) fn total_active(&self) -> (u64, u64) {
        (
            self.total_size.load(Acquire),
            self.active_size.load(Acquire),
        )
    }

    pub(crate) fn update_size(&self, active_size: u64, total_size: u64) {
        self.active_size.fetch_add(active_size, Relaxed);
        self.total_size.fetch_add(total_size, Relaxed);
    }

    fn add_stat_unlocked(&self, stat: MemDataStat) {
        assert_eq!(stat.active_size, stat.total_size);
        self.update_size(stat.active_size as u64, stat.total_size as u64);
        let r = self.map.insert(stat.file_id, stat);
        assert!(r.is_none());
    }

    pub(crate) fn add_stat_interval(&self, stat: MemDataStat, ivl: IntervalPair) {
        // this lock guard protect both interval and file_stat in Manifest so that partail lookup
        // will not happen
        let mut lk = self.interval.write();
        lk.insert(ivl.lo_addr, ivl.hi_addr, ivl.file_id);
        self.add_stat_unlocked(stat);
    }

    fn update_stat_interval(
        &self,
        mut fstat: MemDataStat,
        relocs: HashMap<u64, LenSeq>,
        obsoleted: &[u64],     // no longer referenced
        del_intervals: &[u64], // maybe remapped
        remap_intervals: &[IntervalPair],
    ) -> DataStat {
        assert_eq!(fstat.active_size, fstat.total_size);

        // this lock guard protect both interval and file_stat in Manifest so that partail lookup
        // will not happen
        let mut lk = self.interval.write();
        // must be guarded by lock, or else if we are failed to lock interval, the frames deactived
        // by flush thread will be lost
        self.stop_collect_junks();

        // apply deactived frames while we are performing compaction
        let mut seqs = vec![];
        let mut junks = std::mem::take(&mut *self.junks.lock());
        for (_, q) in junks.iter_mut() {
            for &addr in q.iter() {
                if let Some(ls) = relocs.get(&addr) {
                    fstat.active_size -= ls.len as usize;
                    fstat.active_elems -= 1;
                    fstat.mask.set(ls.seq);
                    seqs.push(ls.seq);
                }
            }
        }

        for &lo in del_intervals {
            let r = lk.remove(lo);
            assert!(r.is_some());
        }
        for i in remap_intervals {
            lk.update(i.lo_addr, i.hi_addr, i.file_id);
        }

        let stat = DataStat {
            inner: fstat.inner,
            inactive_elems: seqs,
        };

        // drop lock before performing heavy map/cache operations
        drop(lk);

        for &id in obsoleted {
            self.remove_stat(id);
            self.cache.del(id);
        }

        self.add_stat_unlocked(fstat);
        stat
    }

    pub(crate) fn remove_stat_interval(&self, blobs: &[u64], ivls: &[u64]) {
        let mut lk = self.interval.write();
        for &x in ivls {
            let r = lk.remove(x);
            assert!(r.is_some());
        }
        for x in blobs {
            self.map.remove(x);
        }
    }

    pub(crate) fn remove_stat(&self, file_id: u64) {
        if let Some((_, v)) = self.map.remove(&file_id) {
            self.decrease(v.active_size as u64, v.total_size as u64);
        }
    }

    pub(crate) fn load(&self, addr: u64) -> Result<BoxRef, OpCode> {
        let file_id = {
            let lk = self.interval.read();
            let Some(file_id) = lk.find(addr) else {
                log::error!("can't find file_id in interval by {}", addr);
                unreachable!("can't find file_id in interval by {}", addr);
            };
            file_id
        };

        loop {
            if let Some(r) = self.cache.get(file_id as u64) {
                return r.read_at(addr);
            }

            let lk = self.cache.lock_shard(file_id);
            lk.add_if_missing(|| new_reader::<DataFooter>(self.opt.data_file(file_id)))?;
        }
    }

    pub(crate) fn apply_junks(&self, tick: u64, junks: &[u64]) -> Vec<DataStat> {
        // optimization: resolve file_ids under read lock, then process outside lock
        // to avoid blocking update_stat_interval (which needs write lock) during IO
        let candidates: Vec<(u64, u64)> = {
            let lk = self.interval.read();
            junks
                .iter()
                .map(|&addr| {
                    let file_id = lk.find(addr).expect("must exist");
                    (addr, file_id)
                })
                .collect()
        };

        let mut v: Vec<DataStat> = Vec::with_capacity(junks.len());
        for (addr, file_id) in candidates {
            if let Some(mut stat) = self.map.get_mut(&file_id) {
                let reloc = self.get_reloc(file_id, addr);
                self.update_stat(&mut stat, addr, &reloc, tick);
                if let Some(b) = v.last_mut()
                    && b.inner.file_id == file_id
                {
                    b.inner = stat.inner;
                    b.inactive_elems.push(reloc.seq);
                } else {
                    v.push(DataStat {
                        inner: stat.inner,
                        inactive_elems: vec![reloc.seq],
                    });
                }
            }
        }
        v
    }

    pub(crate) fn update_stat(&self, stat: &mut MemDataStat, junk: u64, reloc: &Reloc, tick: u64) {
        self.active_size.fetch_sub(reloc.len as u64, Release);
        stat.update(tick, reloc);
        if self.should_collect_junk.load(Relaxed) {
            let mut m = self.junks.lock();
            if let Some(q) = m.get_mut(&stat.file_id) {
                q.push(junk);
            }
        }
    }

    pub(crate) fn start_collect_junks(&self) {
        self.should_collect_junk.store(true, Relaxed);
        // Release is enough for ARM, but it's no-op on x86, so use SeqCst instead
        fence(SeqCst); // make sure previous store finish first
    }

    pub(crate) fn stop_collect_junks(&self) {
        self.should_collect_junk.store(false, Relaxed);
    }

    fn decrease(&self, active_size: u64, total_size: u64) {
        let old = self.active_size.fetch_sub(active_size, AcqRel);
        assert!(old >= active_size);

        let old = self.total_size.fetch_sub(total_size, AcqRel);
        assert!(old >= total_size);
    }

    fn get_reloc(&self, file_id: u64, pos: u64) -> Reloc {
        loop {
            if let Some(x) = self.cache.get(file_id) {
                let Some(&tmp) = x.map.get(&pos) else {
                    log::error!("can't find reloc, file_id {file_id} key {pos}");
                    unreachable!("can't find reloc, file_id {file_id} key {pos}");
                };
                return tmp;
            }

            let lk = self.cache.lock_shard(file_id);
            lk.add_if_missing(|| new_reader::<DataFooter>(self.opt.data_file(file_id)))
                .expect("can't fail");
        }
    }
}

pub(crate) struct BlobStatCtx {
    pub(crate) interval: RwLock<IntervalMap>,
    /// use BTreeMap so that blob files are sorted by blob id
    map: RwLock<BTreeMap<u64, MemBlobStat>>,
    cache: ShardLru<FileReader>,
    pub(crate) should_collect_junk: AtomicBool,
    junks: Mutex<HashMap<u64, Vec<u64>>>,
    opt: Arc<ParsedOptions>,
}

impl Deref for BlobStatCtx {
    type Target = RwLock<BTreeMap<u64, MemBlobStat>>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl BlobStatCtx {
    fn new(opt: Arc<ParsedOptions>) -> Self {
        Self {
            interval: RwLock::new(IntervalMap::new()),
            map: RwLock::new(BTreeMap::new()),
            cache: ShardLru::new(opt.blob_handle_cache_capacity),
            should_collect_junk: AtomicBool::new(false),
            junks: Mutex::new(HashMap::new()),
            opt,
        }
    }

    pub(crate) fn read(&self) -> parking_lot::RwLockReadGuard<'_, BTreeMap<u64, MemBlobStat>> {
        self.map.read()
    }

    pub(crate) fn write(&self) -> parking_lot::RwLockWriteGuard<'_, BTreeMap<u64, MemBlobStat>> {
        self.map.write()
    }

    pub(crate) fn get_victims(
        &self,
        file_ratio: usize,
        garbage_ratio: usize,
    ) -> (Vec<u64>, Vec<(u64, usize)>) {
        let lk = self.map.read();
        let n = lk.len() * file_ratio / 100;
        let mut v = Vec::new();
        let mut o = Vec::new();
        let mut nr_total = 0;
        let mut nr_active = 0;

        for (_, x) in lk.iter().take(n) {
            if x.nr_active == 0 {
                o.push(x.file_id);
            } else {
                nr_total += x.nr_total;
                nr_active += x.nr_active;
                v.push((x.file_id, x.active_size));
            }
        }
        if nr_total > 0 {
            assert!(!v.is_empty());
            let ratio = (nr_total - nr_active) * 100 / nr_total;
            if (ratio as usize) < garbage_ratio {
                v.clear();
            }
        }
        (o, v)
    }

    pub(crate) fn remove_stat_interval(&self, blobs: &[u64], ivls: &[u64]) {
        let mut lk = self.map.write();
        for x in blobs {
            let r = lk.remove(x);
            assert!(r.is_some());
        }
        drop(lk);
        let mut lk = self.interval.write();
        for &x in ivls {
            lk.remove(x);
        }
    }

    pub(crate) fn start_collect_junks(&self) {
        self.should_collect_junk.store(true, Relaxed);
        fence(SeqCst);
    }

    fn stop_collect_junks(&self) {
        self.should_collect_junk.store(false, Relaxed);
    }

    fn update_stat_interval(
        &self,
        mut bstat: MemBlobStat,
        reloc: HashMap<u64, LenSeq>,
        obsoleted: &[u64],
        del_intervals: &[u64],
        remap_intervals: &[IntervalPair],
    ) -> BlobStat {
        let mut lk = self.interval.write();
        self.stop_collect_junks();

        let mut seqs = vec![];
        let mut junks = std::mem::take(&mut *self.junks.lock());
        for (_, q) in junks.iter_mut() {
            for &addr in q.iter() {
                if let Some(ls) = reloc.get(&addr) {
                    bstat.active_size -= ls.len as usize;
                    bstat.nr_active -= 1;
                    seqs.push(ls.seq);
                }
            }
        }

        for &lo in del_intervals {
            let r = lk.remove(lo);
            assert!(r.is_some());
        }
        for i in remap_intervals {
            lk.update(i.lo_addr, i.hi_addr, i.file_id);
        }

        let ret = BlobStat {
            inner: bstat.inner,
            inactive_elems: seqs,
        };

        // drop lock before performing heavy map/cache operations
        drop(lk);

        for &id in obsoleted {
            self.remove_stat(id);
            self.cache.del(id);
        }
        self.add_stat_unlocked(bstat);
        ret
    }

    fn load(&self, addr: u64) -> Result<BoxRef, OpCode> {
        let file_id = {
            let lk = self.interval.read();
            let Some(file_id) = lk.find(addr) else {
                log::error!("can't find file_id in interval by {addr}");
                unreachable!("can't find file_id in interval by {addr}");
            };
            file_id
        };

        loop {
            if let Some(r) = self.cache.get(file_id as u64) {
                return r.read_at(addr);
            }

            let lk = self.cache.lock_shard(file_id);
            lk.add_if_missing(|| new_reader::<BlobFooter>(self.opt.blob_file(file_id)))?;
        }
    }

    fn get_reloc(&self, file_id: u64, addr: u64) -> Reloc {
        loop {
            if let Some(x) = self.cache.get(file_id) {
                let Some(&tmp) = x.map.get(&addr) else {
                    log::error!("can't find reloc, blob_id {file_id} key {addr}");
                    unreachable!("can't find reloc, blob_id {file_id} key {addr}");
                };
                return tmp;
            }

            let lk = self.cache.lock_shard(file_id);
            lk.add_if_missing(|| new_reader::<BlobFooter>(self.opt.blob_file(file_id)))
                .expect("can't fail");
        }
    }

    fn apply_junks(&self, junks: &[u64]) -> Vec<BlobStat> {
        let mut v: Vec<BlobStat> = Vec::with_capacity(junks.len());
        let lk = self.interval.read();
        for &addr in junks {
            let file_id = lk.find(addr).expect("must exist");
            let mut map = self.map.write();
            if let Some(stat) = map.get_mut(&file_id) {
                let reloc = self.get_reloc(file_id, addr);
                self.update_stat(stat, &reloc, addr);
                if let Some(b) = v.last_mut()
                    && b.inner.file_id == file_id
                {
                    b.inner = stat.inner;
                    b.inactive_elems.push(reloc.seq);
                } else {
                    v.push(BlobStat {
                        inner: stat.inner,
                        inactive_elems: vec![reloc.seq],
                    });
                }
            }
        }
        v
    }

    fn update_stat(&self, stat: &mut MemBlobStat, reloc: &Reloc, addr: u64) {
        stat.update(reloc);

        if self.should_collect_junk.load(Relaxed) {
            let mut m = self.junks.lock();
            if let Some(q) = m.get_mut(&stat.file_id) {
                q.push(addr);
            }
        }
    }

    fn add_stat_unlocked(&self, stat: MemBlobStat) {
        let mut map = self.map.write();
        map.insert(stat.file_id, stat);
    }

    pub(crate) fn remove_stat(&self, file_id: u64) -> Option<MemBlobStat> {
        let mut map = self.map.write();
        map.remove(&file_id)
    }

    fn add_stat_interval(&self, stat: MemBlobStat, ivl: IntervalPair) {
        let mut lk = self.interval.write();
        lk.insert(ivl.lo_addr, ivl.hi_addr, ivl.file_id);
        self.add_stat_unlocked(stat);
    }
}
