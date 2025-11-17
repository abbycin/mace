use std::{
    cell::{Cell, RefCell},
    collections::{BTreeMap, HashMap},
    hash::Hasher,
    ops::Deref,
    path::PathBuf,
    ptr::null_mut,
    sync::{
        Arc, Mutex, RwLock,
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
    Options,
    io::{File, GatherIO},
    map::{
        IFooter,
        buffer::Loader,
        data::{BlobFooter, DataFooter, MetaReader},
        table::{PageMap, Swip},
    },
    meta::entry::{BlobStat, Commit, GenericHdr},
    types::{
        page::Page,
        refbox::BoxRef,
        traits::{IAsSlice, IHeader},
    },
    utils::{
        Handle, MutRef, ROOT_PID,
        block::{Block, Ring},
        data::{GatherWriter, LenSeq, Reloc},
        interval::IntervalMap,
        lru::ShardLru,
        options::ParsedOptions,
    },
};

pub(crate) mod builder;
mod entry;
pub use entry::{
    Begin, BlobStatInner, DataStat, DataStatInner, DelInterval, Delete, FileId, IntervalPair,
    MemBlobStat, MemDataStat, MetaKind, Numerics, PageTable, TxnKind,
};

const LOG_BUF_SZ: usize = 64 << 20;

pub(crate) trait IMetaCodec {
    fn packed_size(&self) -> usize;

    fn encode(&self, to: &mut [u8]);

    fn decode(src: &[u8]) -> Self;
}

struct WriteCtx {
    ring: Ring,
    writer: GatherWriter,
}

pub(crate) struct Manifest {
    pub(crate) numerics: Arc<Numerics>,
    pub(crate) map: Arc<PageMap>,
    pub(crate) data_stat: DataStatCtx,
    pub(crate) blob_stat: BlobStatCtx,
    pub(crate) is_cleaning: AtomicBool,
    txid: AtomicU64,
    obsolete_data: Mutex<Vec<u64>>,
    obsolete_blob: Mutex<Vec<u64>>,
    opt: Arc<ParsedOptions>,
    wctx: Handle<Mutex<WriteCtx>>,
}

impl Drop for Manifest {
    fn drop(&mut self) {
        self.wctx.reclaim();
    }
}

pub(crate) struct Txn {
    wctx: Handle<Mutex<WriteCtx>>,
    txid: u64,
    h: Crc32cHasher,
    nbytes: Cell<u64>,
}

impl Txn {
    pub(crate) fn commit(mut self) -> u64 {
        let c = Commit {
            txid: self.txid,
            checksum: self.h.finish() as u32,
        };
        self.record(MetaKind::Commit, &c);
        self.sync();
        self.nbytes.get()
    }

    pub(crate) fn record<T>(&mut self, kind: MetaKind, x: &T)
    where
        T: IMetaCodec,
    {
        let hdr = GenericHdr::new(kind, self.txid);
        let mut wctx = self.wctx.lock().expect("lock fail");
        let size = x.packed_size() + hdr.len();
        if size > wctx.ring.len() {
            return self.record_large(&mut wctx, &hdr, x);
        }

        let buf = Self::alloc(&mut wctx, size);
        let hdr_dst = &mut buf[0..hdr.len()];
        hdr_dst.copy_from_slice(hdr.as_slice());
        x.encode(&mut buf[hdr.len()..]);
        self.nbytes.set(self.nbytes.get() + buf.len() as u64);
        self.h.write(buf);
    }

    pub(crate) fn sync(&self) {
        let mut wctx = self.wctx.lock().expect("lock fail");
        Self::flush(&mut wctx);
    }

    fn record_large<T>(&self, wctx: &mut WriteCtx, hdr: &GenericHdr, x: &T)
    where
        T: IMetaCodec,
    {
        let buf = Block::alloc(x.packed_size());
        let s = buf.mut_slice(0, buf.len());
        let hdr_dst = &mut s[0..hdr.len()];
        hdr_dst.copy_from_slice(hdr.as_slice());
        x.encode(&mut s[hdr.len()..]);
        self.nbytes.set(self.nbytes.get() + buf.len() as u64);
        Self::flush(wctx);
        wctx.writer.write(s);
    }

    fn alloc<'a>(wctx: &mut WriteCtx, size: usize) -> &'a mut [u8] {
        let rest = wctx.ring.len() - wctx.ring.tail();
        if rest < size {
            Self::flush(wctx);
            wctx.ring.prod(rest);
            wctx.ring.cons(rest);
        }
        wctx.ring.prod(size)
    }

    fn flush(wctx: &mut WriteCtx) {
        let len = wctx.ring.distance();
        if len > 0 {
            wctx.writer.write(wctx.ring.slice(wctx.ring.head(), len));
            wctx.ring.cons(len);

            wctx.writer.sync();
        }
    }
}

struct FileReader {
    file: File,
    map: HashMap<u64, Reloc>,
}

fn new_reader<T: IFooter>(path: PathBuf) -> Option<FileReader> {
    let mut loader = MetaReader::<T>::new(&path).ok()?;
    let relocs = loader.get_reloc().expect("never happen");
    let mut map = HashMap::with_capacity(relocs.len());
    for x in relocs {
        map.insert(x.key, x.val);
    }

    let file = loader.take();
    Some(FileReader { file, map })
}

impl FileReader {
    fn read_at(&self, pos: u64) -> BoxRef {
        let m = self.map.get(&pos).expect("never happen");
        let real_size = BoxRef::real_size_from_dump(m.len);
        let mut p = BoxRef::alloc_exact(real_size, pos);
        let mut crc = Crc32cHasher::default();

        let dst = p.load_slice();
        self.file.read(dst, m.off as u64).expect("can't read");
        crc.write(dst);
        debug_assert_eq!(p.view().refcnt(), 1);
        debug_assert_eq!(
            p.header().payload_size,
            (real_size - BoxRef::HDR_LEN as u32)
        );
        if crc.finish() as u32 != m.crc {
            log::error!(
                "checksum mismatch, expect {} get {}, key {pos}",
                { m.crc },
                crc.finish()
            );
            log::error!("hdr {:?}", p.header());
            panic!(
                "checksum mismatch, expect {} get {}",
                { m.crc },
                crc.finish()
            );
        }

        p
    }
}

impl Manifest {
    const CLEAN_SIZE: u64 = 1 << 30;

    fn new(opt: Arc<ParsedOptions>) -> Self {
        Self {
            numerics: Arc::new(Numerics::default()),
            map: Arc::new(PageMap::default()),
            data_stat: DataStatCtx::new(opt.clone()),
            blob_stat: BlobStatCtx::new(opt.clone()),
            is_cleaning: AtomicBool::new(false),
            txid: AtomicU64::new(0),
            obsolete_data: Mutex::new(Vec::new()),
            obsolete_blob: Mutex::new(Vec::new()),
            opt,
            wctx: null_mut::<Mutex<WriteCtx>>().into(),
        }
    }

    pub(crate) fn init(&mut self, data_id: u64, blob_id: u64, txid: u64) {
        let id = self.numerics.next_manifest_id.load(Relaxed);
        self.wctx = Handle::new(Mutex::new(WriteCtx {
            ring: Ring::new(LOG_BUF_SZ),
            writer: GatherWriter::append(&self.opt.manifest(id), 32),
        }));

        self.numerics.next_data_id.store(data_id + 1, Relaxed);
        self.numerics.next_blob_id.store(blob_id + 1, Relaxed);
        self.txid.store(txid + 1, Relaxed);
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

    pub(crate) fn begin(&self, kind: TxnKind) -> Txn {
        let txid = self.txid.fetch_add(1, Relaxed);
        let mut txn = Txn {
            wctx: self.wctx,
            txid,
            h: Crc32cHasher::default(),
            nbytes: Cell::new(0),
        };
        txn.record(MetaKind::Begin, &Begin(kind));
        txn
    }

    pub(crate) fn try_clean(&mut self) {
        let Ok(wctx) = self.wctx.try_lock() else {
            return;
        };
        if wctx.writer.pos() < Self::CLEAN_SIZE {
            return;
        }
        drop(wctx);
        if self
            .is_cleaning
            .compare_exchange(false, true, Relaxed, Relaxed)
            .is_err()
        {
            return;
        }

        let prev_id = self.bump_id();
        let snap_id = prev_id + 1;
        let tmp_path = self.opt.snapshot(snap_id).with_extension("tmp");
        let last_pid = self.map.len();
        let mut table = PageTable::default();
        const LIMIT: usize = 8192; // estimate 128KB
        // automatically reclaim
        let b = MutRef::new(Mutex::new(WriteCtx {
            ring: Ring::new(LOG_BUF_SZ),
            writer: GatherWriter::append(&tmp_path, 32),
        }));

        let txid = self.txid.fetch_add(1, Relaxed);

        let mut txn = Txn {
            wctx: b.raw_ptr().into(),
            txid,
            h: Crc32cHasher::default(),
            nbytes: Cell::new(0),
        };

        txn.record(MetaKind::Begin, &Begin(TxnKind::Dump));

        let numerics = self.numerics.deref().clone();
        txn.record(MetaKind::Numerics, &numerics);

        let g = crossbeam_epoch::pin(); // guard page
        for pid in ROOT_PID..last_pid {
            let swip = Swip::new(self.map.get(pid));
            if swip.is_null() {
                continue;
            }
            let addr = if swip.is_tagged() {
                swip.untagged()
            } else {
                let p = Page::<Loader>::from_swip(swip.raw());
                p.latest_addr()
            };
            table.add(pid, addr);
            if table.len() == LIMIT {
                txn.record(MetaKind::Map, &table);
                table.clear();
            }
        }
        drop(g);

        for fstat in self.data_stat.iter() {
            let stat = fstat.copy();
            txn.record(MetaKind::DataStat, &stat);
        }

        let lk = self.data_stat.interval.read().expect("can't lock read");
        for (&lo, &(hi, id)) in lk.iter() {
            let ivl = IntervalPair::new(lo, hi, id);
            txn.record(MetaKind::DataInterval, &ivl);
        }
        drop(lk);

        let nbytes = txn.commit();
        log::info!("write manifest snapshot: {nbytes} bytes");

        std::fs::rename(tmp_path, self.opt.snapshot(snap_id)).expect("can't fail");

        self.unlink_old(snap_id);
        self.is_cleaning.store(false, Release);
    }

    // unlink all manifest files smaller than snap_id
    pub(crate) fn unlink_old(&self, snap_id: u64) {
        let dir = std::fs::read_dir(self.opt.db_root()).expect("can't read dir");

        for d in dir.flatten() {
            let name = d.file_name();
            let s = name.to_str().unwrap();
            if !s.starts_with(Options::MANIFEST_PREFIX) {
                continue;
            }
            let v: Vec<&str> = s.split(Options::SEP).collect();
            assert_eq!(v.len(), 2);
            let id = v[1].parse::<u64>().unwrap();
            if id < snap_id {
                let _ = std::fs::remove_file(d.path());
            }
        }
    }

    fn bump_id(&mut self) -> u64 {
        let mut wctx = self.wctx.lock().unwrap();
        let old = self.numerics.next_manifest_id.fetch_add(2, Release);
        wctx.writer.reset(&self.opt.manifest(old + 2));
        old
    }

    pub(crate) fn load_data(&self, addr: u64) -> Option<BoxRef> {
        self.data_stat.load(addr)
    }

    pub(crate) fn load_blob(&self, addr: u64) -> Option<BoxRef> {
        self.blob_stat.load(addr)
    }

    pub(crate) fn save_obsolete_data(&self, id: &[u64]) {
        if !id.is_empty() {
            let mut lk = self.obsolete_data.lock().unwrap();
            lk.extend_from_slice(id);
        }
    }

    pub(crate) fn save_obsolete_blob(&self, id: &[u64]) {
        if !id.is_empty() {
            let mut lk = self.obsolete_blob.lock().unwrap();
            lk.extend_from_slice(id);
        }
    }

    pub(crate) fn delete_files(&self) {
        if let Ok(mut lk) = self.obsolete_data.try_lock() {
            while let Some(id) = lk.pop() {
                let path = self.opt.data_file(id);
                if path.exists() {
                    log::info!("unlink {path:?}");
                    let _ = std::fs::remove_file(path);
                }
            }
        }
        if let Ok(mut lk) = self.obsolete_blob.try_lock() {
            while let Some(id) = lk.pop() {
                let path = self.opt.blob_file(id);
                if path.exists() {
                    log::info!("unlink {path:?}");
                    let _ = std::fs::remove_file(path);
                }
            }
        }
    }
}

pub(crate) struct DataStatCtx {
    /// interval to file_id map, multiple intervals may point to same file_id after compaction
    interval: RwLock<IntervalMap>,
    map: DashMap<u64, MemDataStat>,
    junks: RefCell<HashMap<u64, Vec<u64>>>,
    cache: ShardLru<FileReader>,
    opt: Arc<ParsedOptions>,
    should_collect_junk: AtomicBool,
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
            junks: RefCell::new(HashMap::new()),
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
        let mut lk = self.interval.write().expect("can't lock write");
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
        let mut lk = self.interval.write().expect("can't lock write");
        // must be guarded by lock, or else if we are failed to lock interval, the frames deactived
        // by flush thread will be lost
        self.stop_collect_junks();

        // apply deactived frames while we are performing compaction
        let mut seqs = vec![];
        let mut junks = self.junks.borrow_mut();
        for (_, q) in junks.iter_mut() {
            for addr in q.iter() {
                if let Some(ls) = relocs.get(addr) {
                    fstat.active_size -= ls.len as usize;
                    fstat.active_elems -= 1;
                    fstat.mask.set(ls.seq);
                    seqs.push(ls.seq);
                }
            }
        }
        junks.clear();

        for &lo in del_intervals {
            let r = lk.remove(lo);
            assert!(r.is_some());
        }
        for i in remap_intervals {
            lk.update(i.lo_addr, i.hi_addr, i.file_id);
        }

        for &id in obsoleted {
            self.remove_stat(id);
            self.cache.del(id);
        }

        let stat = DataStat {
            inner: fstat.inner,
            inactive_elems: seqs,
        };
        self.add_stat_unlocked(fstat);
        stat
    }

    pub(crate) fn remove_stat_interval(&self, blobs: &[u64], ivls: &[u64]) {
        let mut lk = self.interval.write().expect("can't lock write");
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

    pub(crate) fn load(&self, addr: u64) -> Option<BoxRef> {
        let file_id = {
            let lk = self.interval.read().expect("can't lock read");
            let Some(file_id) = lk.find(addr) else {
                log::error!("can't find file_id in interval by {}", addr);
                panic!("can't find file_id in interval by {}", addr);
            };
            file_id
        };

        loop {
            if let Some(r) = self.cache.get(file_id as u64) {
                return Some(r.read_at(addr));
            }

            let lk = self.cache.lock_shard(file_id);
            lk.add_if_missing(|| new_reader::<DataFooter>(self.opt.data_file(file_id)))?;
        }
    }

    pub(crate) fn apply_junks(&self, tick: u64, junks: &[u64]) -> Vec<DataStat> {
        let mut v: Vec<DataStat> = Vec::with_capacity(junks.len());
        // this lock guard protect both interval and file_stat in Manifest so that partail lookup
        // will not happen
        let lk = self.interval.read().expect("can't lock read");
        for &addr in junks {
            let file_id = lk.find(addr).expect("must exist");
            if let Some(mut stat) = self.map.get_mut(&file_id) {
                let reloc = self.get_reloc(file_id, addr);
                self.update_stat(&mut stat, addr, &reloc, tick);
                if let Some(b) = v.last_mut()
                    && b.file_id == file_id
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
            let mut m = self.junks.borrow_mut();
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
                    panic!("can't find reloc, file_id {file_id} key {pos}");
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
    interval: RwLock<IntervalMap>,
    /// use BTreeMap so that blob files are sorted by blob id
    map: RwLock<BTreeMap<u64, MemBlobStat>>,
    cache: ShardLru<FileReader>,
    should_collect_junk: AtomicBool,
    junks: RefCell<HashMap<u64, Vec<u64>>>,
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
            junks: RefCell::new(HashMap::new()),
            opt,
        }
    }

    pub(crate) fn get_victims(
        &self,
        file_ratio: usize,
        garbage_ratio: usize,
    ) -> (Vec<u64>, Vec<(u64, usize)>) {
        let lk = self.map.read().unwrap();
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
        let mut lk = self.map.write().expect("can't lock write");
        for x in blobs {
            let r = lk.remove(x);
            assert!(r.is_some());
        }
        drop(lk);
        let mut lk = self.interval.write().expect("can't lock write");
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
        let mut lk = self.interval.write().expect("can't lock write");
        self.stop_collect_junks();

        let mut seqs = vec![];
        let mut junks = self.junks.borrow_mut();
        for (_, q) in junks.iter_mut() {
            for addr in q.iter() {
                if let Some(ls) = reloc.get(addr) {
                    bstat.active_size -= ls.len as usize;
                    bstat.nr_active -= 1;
                    seqs.push(ls.seq);
                }
            }
        }
        junks.clear();

        for &lo in del_intervals {
            let r = lk.remove(lo);
            assert!(r.is_some());
        }
        for i in remap_intervals {
            lk.update(i.lo_addr, i.hi_addr, i.file_id);
        }

        for &id in obsoleted {
            self.remove_stat(id);
            self.cache.del(id);
        }
        self.add_stat_unlocked(bstat.clone());
        BlobStat {
            inner: bstat.inner,
            inactive_elems: seqs,
        }
    }

    fn load(&self, addr: u64) -> Option<BoxRef> {
        let file_id = {
            let lk = self.interval.read().expect("can't lock read");
            let Some(file_id) = lk.find(addr) else {
                log::error!("can't find file_id in interval by {addr}");
                panic!("can't find file_id in interval by {addr}");
            };
            file_id
        };

        loop {
            if let Some(r) = self.cache.get(file_id as u64) {
                return Some(r.read_at(addr));
            }

            let lk = self.cache.lock_shard(file_id);
            lk.add_if_missing(|| new_reader::<BlobFooter>(self.opt.blob_file(file_id)))?
        }
    }

    fn get_reloc(&self, file_id: u64, addr: u64) -> Reloc {
        loop {
            if let Some(x) = self.cache.get(file_id) {
                let Some(&tmp) = x.map.get(&addr) else {
                    log::error!("can't find reloc, blob_id {file_id} key {addr}");
                    panic!("can't find reloc, blob_id {file_id} key {addr}");
                };
                return tmp;
            }

            let lk = self.cache.lock_shard(file_id);
            lk.add_if_missing(|| new_reader::<BlobFooter>(self.opt.blob_file(file_id)))
                .expect("can't fail")
        }
    }

    fn apply_junks(&self, junks: &[u64]) -> Vec<BlobStat> {
        let mut v: Vec<BlobStat> = Vec::with_capacity(junks.len());
        let lk = self.interval.read().expect("can't lock read");
        for &addr in junks {
            let file_id = lk.find(addr).expect("must exist");
            let mut map = self.map.write().expect("can't lock write");
            if let Some(stat) = map.get_mut(&file_id) {
                let reloc = self.get_reloc(file_id, addr);
                self.update_stat(stat, &reloc, addr);
                if let Some(b) = v.last_mut()
                    && b.file_id == file_id
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
            let mut m = self.junks.borrow_mut();
            if let Some(q) = m.get_mut(&stat.file_id) {
                q.push(addr);
            }
        }
    }

    fn add_stat_unlocked(&self, stat: MemBlobStat) {
        let mut map = self.map.write().expect("can't lock write");
        map.insert(stat.file_id, stat);
    }

    pub(crate) fn remove_stat(&self, file_id: u64) -> Option<MemBlobStat> {
        let mut map = self.map.write().expect("can't lock write");
        map.remove(&file_id)
    }

    fn add_stat_interval(&self, stat: MemBlobStat, ivl: IntervalPair) {
        let mut lk = self.interval.write().expect("can't lock write");
        lk.insert(ivl.lo_addr, ivl.hi_addr, ivl.file_id);
        self.add_stat_unlocked(stat);
    }
}
