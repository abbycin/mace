use std::{
    cell::{Cell, RefCell},
    collections::{HashMap, HashSet},
    hash::Hasher,
    ops::Deref,
    path::PathBuf,
    ptr::null_mut,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{
            AtomicBool, AtomicU64,
            Ordering::{AcqRel, Acquire, Relaxed, Release},
        },
    },
};

use crc32c::Crc32cHasher;
use dashmap::DashMap;
use io::{File, GatherIO};

use crate::{
    Options,
    map::{
        buffer::Loader,
        data::DataMetaReader,
        table::{PageMap, Swip},
    },
    meta::entry::{Commit, GenericHdr},
    types::{
        page::Page,
        refbox::BoxRef,
        traits::{IAsSlice, IHeader},
    },
    utils::{
        Handle, MutRef, ROOT_PID,
        block::{Block, Ring},
        data::{GatherWriter, Reloc},
        interval::IntervalMap,
        lru::Lru,
        options::ParsedOptions,
    },
};

pub(crate) mod builder;
mod entry;
pub use entry::{
    Begin, DelInterval, Delete, FileId, FileStat, IntervalPair, MetaKind, Numerics, PageTable,
    Stat, StatInner, TxnKind,
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
    /// interval to file_id map, multiple intervals may point to same file_id after compaction
    pub(crate) interval: RwLock<IntervalMap>,
    /// file_id to stat map
    pub(crate) stat_ctx: StatContext,
    pub(crate) is_cleaning: AtomicBool,
    txid: AtomicU64,
    obsolete_files: Mutex<Vec<u64>>,
    cache: Lru<FileReader>,
    opt: Arc<ParsedOptions>,
    wctx: Handle<Mutex<WriteCtx>>,
    /// when multiple thread are trying to load data from a file, only one can successfully lock
    non_dup_lock: Mutex<()>,
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

impl FileReader {
    fn open(path: PathBuf) -> Option<Self> {
        let mut loader = DataMetaReader::new(&path, false).ok()?;
        let mut map = HashMap::new();
        let d = loader.get_meta().expect("never happen");
        d.relocs().iter().map(|x| map.insert(x.key, x.val)).count();

        let file = loader.take();
        Some(Self { file, map })
    }

    fn read_at(&self, pos: u64) -> BoxRef {
        let m = self.map.get(&pos).expect("never happen");
        let mut p = BoxRef::alloc(m.len - BoxRef::HDR_LEN as u32, pos);

        let dst = p.load_slice();
        self.file.read(dst, m.off as u64).expect("can't read");
        debug_assert_eq!(p.view().refcnt(), 1);
        debug_assert!(p.header().payload_size <= (m.len - BoxRef::HDR_LEN as u32));

        p
    }
}

impl Manifest {
    const CLEAN_SIZE: u64 = 1 << 30;

    fn new(opt: Arc<ParsedOptions>) -> Self {
        Self {
            numerics: Arc::new(Numerics::default()),
            map: Arc::new(PageMap::default()),
            interval: RwLock::new(IntervalMap::new()),
            stat_ctx: StatContext::new(),
            is_cleaning: AtomicBool::new(false),
            txid: AtomicU64::new(0),
            obsolete_files: Mutex::new(Vec::new()),
            cache: Lru::new(256),
            opt,
            wctx: null_mut::<Mutex<WriteCtx>>().into(),
            non_dup_lock: Mutex::new(()),
        }
    }

    pub(crate) fn init(&mut self, file_id: u64, txid: u64) {
        let id = self.numerics.next_manifest_id.load(Relaxed);
        self.wctx = Handle::new(Mutex::new(WriteCtx {
            ring: Ring::new(LOG_BUF_SZ),
            writer: GatherWriter::append(&self.opt.manifest(id), 32),
        }));

        self.numerics.next_file_id.store(file_id + 1, Relaxed);
        self.txid.store(txid + 1, Relaxed);
    }

    pub(crate) fn add_stat_interval(&mut self, stat: FileStat, ivl: IntervalPair) {
        // this lock guard protect both interval and file_stat in Manifest so that partail lookup
        // will not happen
        let mut lk = self.interval.write().expect("can't lock write");
        lk.insert(ivl.lo_addr, ivl.hi_addr, ivl.file_id);
        assert_eq!(stat.active_size, stat.total_size);
        self.stat_ctx.add_stat(stat);
    }

    pub(crate) fn update_stat_interval(
        &self,
        mut fstat: FileStat,
        relocs: HashMap<u64, (u32, u32)>,
        obsoleted: &[u64],
        del_intervals: &[u64],
        remap_intervals: &[IntervalPair],
    ) -> Stat {
        assert_eq!(fstat.active_size, fstat.total_size);
        let mut set: HashSet<u64> = HashSet::from_iter(del_intervals.iter().cloned());
        for i in remap_intervals {
            set.remove(&i.lo_addr);
        }

        // this lock guard protect both interval and file_stat in Manifest so that partail lookup
        // will not happen
        let mut lk = self.interval.write().expect("can't lock write");
        // must be guarded by lock, or else if we are failed to lock interval, the frames deactived
        // by flush thread will be lost
        self.stat_ctx.stop_collect_junks();

        // apply deactived frames while we are performing compaction
        let mut seqs = vec![];
        let mut junks = self.stat_ctx.junks.borrow_mut();
        for (_, q) in junks.iter_mut() {
            for addr in q.iter() {
                if let Some(&(size, seq)) = relocs.get(addr) {
                    fstat.active_size -= size as usize;
                    fstat.active_elems -= 1;
                    fstat.deleted_elems.set(seq);
                    seqs.push(seq);
                }
            }
        }
        junks.clear();

        for lo in set {
            let r = lk.remove(lo);
            assert!(r.is_some());
        }
        for i in remap_intervals {
            lk.update(i.lo_addr, i.hi_addr, i.file_id);
        }

        for &id in obsoleted {
            self.stat_ctx.remove_stat(id);
            self.cache.del(id);
        }

        let stat = Stat {
            inner: fstat.inner,
            deleted_elems: seqs,
        };
        self.stat_ctx.add_stat(fstat);
        stat
    }

    pub(crate) fn apply_junks(&self, tick: u64, junks: &[u64]) -> Vec<Stat> {
        let mut h: HashMap<u64, Stat> = HashMap::with_capacity(junks.len());
        // this lock guard protect both interval and file_stat in Manifest so that partail lookup
        // will not happen
        let lk = self.interval.read().expect("can't lock read");
        for &addr in junks {
            let file_id = lk.find(addr).expect("must exist");
            if let Some(mut stat) = self.stat_ctx.get_mut(&file_id) {
                let reloc = self.get_reloc(file_id, addr);
                self.stat_ctx.update_stat(&mut stat, addr, &reloc, tick);
                let e = h.entry(stat.file_id);
                e.and_modify(|x| {
                    x.inner = stat.inner;
                    x.deleted_elems.push(reloc.seq);
                })
                .or_insert(Stat {
                    inner: stat.inner,
                    deleted_elems: vec![reloc.seq],
                });
            };
        }
        h.values().cloned().collect()
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

        for fstat in self.stat_ctx.iter() {
            let stat = fstat.copy();
            txn.record(MetaKind::Stat, &stat);
        }

        let lk = self.interval.read().expect("can't lock read");
        for (&lo, &(hi, id)) in lk.iter() {
            let ivl = IntervalPair::new(lo, hi, id);
            txn.record(MetaKind::Interval, &ivl);
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

    pub(crate) fn load_impl(&self, addr: u64) -> Option<BoxRef> {
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

            let Ok(_lk) = self.non_dup_lock.try_lock() else {
                continue;
            };
            self.load_cache(file_id)?;
        }
    }

    pub(crate) fn save_obsolete_files(&self, id: &[u64]) {
        if !id.is_empty() {
            let mut lk = self.obsolete_files.lock().unwrap();
            lk.extend_from_slice(id);
        }
    }

    pub(crate) fn delete_files(&mut self) {
        if let Ok(mut lk) = self.obsolete_files.try_lock() {
            while let Some(id) = lk.pop() {
                let path = self.opt.data_file(id);
                if path.exists() {
                    log::info!("unlink {path:?}");
                    let _ = std::fs::remove_file(path);
                }
            }
        }
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

            let Ok(_lk) = self.non_dup_lock.try_lock() else {
                continue;
            };
            self.load_cache(file_id).expect("file must exist");
        }
    }

    fn load_cache(&self, file_id: u64) -> Option<()> {
        let f = FileReader::open(self.opt.data_file(file_id))?;
        self.cache.add(file_id, f);
        Some(())
    }
}

pub(crate) struct StatContext {
    map: DashMap<u64, FileStat>,
    junks: RefCell<HashMap<u64, Vec<u64>>>,
    should_collect_junk: AtomicBool,
    total_size: AtomicU64,
    active_size: AtomicU64,
}

impl Deref for StatContext {
    type Target = DashMap<u64, FileStat>;
    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl StatContext {
    fn new() -> Self {
        Self {
            map: DashMap::new(),
            junks: RefCell::new(HashMap::new()),
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

    pub(crate) fn add_stat(&self, stat: FileStat) {
        self.update_size(stat.active_size as u64, stat.total_size as u64);

        let r = self.map.insert(stat.file_id, stat);
        assert!(r.is_none());
    }

    pub(crate) fn remove_stat(&self, file_id: u64) {
        if let Some((_, v)) = self.map.remove(&file_id) {
            self.decrease(v.active_size as u64, v.total_size as u64);
        }
    }

    pub(crate) fn update_stat(&self, stat: &mut FileStat, junk: u64, reloc: &Reloc, tick: u64) {
        self.active_size.fetch_sub(reloc.len as u64, Release);
        stat.update(tick, reloc);
        if self.should_collect_junk.load(Acquire) {
            let mut m = self.junks.borrow_mut();
            if let Some(q) = m.get_mut(&stat.file_id) {
                q.push(junk);
            }
        }
    }

    pub(crate) fn start_collect_junks(&self) {
        self.should_collect_junk.store(true, Release);
    }

    pub(crate) fn stop_collect_junks(&self) {
        self.should_collect_junk.store(false, Release);
    }

    fn decrease(&self, active_size: u64, total_size: u64) {
        let old = self.active_size.fetch_sub(active_size, AcqRel);
        assert!(old >= active_size);

        let old = self.total_size.fetch_sub(total_size, AcqRel);
        assert!(old >= total_size);
    }
}
