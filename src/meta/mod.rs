use std::{
    collections::HashMap,
    hash::Hasher,
    ops::Deref,
    path::PathBuf,
    ptr::null_mut,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{
            AtomicBool, AtomicU64,
            Ordering::{Relaxed, Release},
        },
    },
};

use crc32c::Crc32cHasher;
use dashmap::{DashMap, Entry};
use io::{File, GatherIO};

use crate::{
    Options,
    map::{
        buffer::Loader,
        data::DataMetaReader,
        table::{PageMap, Swip},
    },
    meta::entry::{Begin, Commit},
    types::{page::Page, refbox::BoxRef, traits::IHeader},
    utils::{
        Handle, MutRef, NULL_EPOCH, ROOT_PID,
        block::{Block, Ring},
        data::{GatherWriter, Reloc},
        lru::Lru,
        options::ParsedOptions,
        unpack_id,
    },
};

pub(crate) mod builder;
mod entry;
pub use entry::{Delete, FileStat, Lid, Numerics, PageTable, Stat, StatInner};

const LOG_BUF_SZ: usize = 64 << 20;

pub(crate) trait IMetaCodec {
    fn packed_size(&self) -> usize;

    fn encode(&self, to: &mut [u8]);

    fn decode(src: &[u8]) -> Self;
}

pub(crate) struct Manifest {
    pub(crate) numerics: Arc<Numerics>,
    pub(crate) map: Arc<PageMap>,
    pub(crate) file_stat: DashMap<u32, MutRef<FileStat>>,
    pub(crate) is_cleaning: AtomicBool,
    txid: AtomicU64,
    obsolete_files: Mutex<Vec<u32>>,
    cache: Lru<FileReader>,
    opt: Arc<ParsedOptions>,
    writer: Handle<GatherWriter>,
    ring: Ring,
    mtx: Mutex<()>,
}

impl Drop for Manifest {
    fn drop(&mut self) {
        self.writer.reclaim();
    }
}

pub(crate) struct Txn<'a> {
    _guard: MutexGuard<'a, ()>,
    txid: u64,
    ring: &'a mut Ring,
    writer: &'a mut GatherWriter,
    h: Crc32cHasher,
    nbytes: u64,
}

impl Txn<'_> {
    pub(crate) fn commit(mut self) -> u64 {
        let c = Commit {
            txid: self.txid,
            checksum: self.h.finish() as u32,
        };
        self.record(&c);
        self.nbytes
    }

    pub(crate) fn record<T>(&mut self, x: &T)
    where
        T: IMetaCodec,
    {
        let size = x.packed_size();
        if size > self.ring.len() {
            return self.record_large(x);
        }

        let buf = self.alloc(size);
        x.encode(buf);
        self.nbytes += buf.len() as u64;
        self.h.write(buf);
    }

    fn record_large<T>(&mut self, x: &T)
    where
        T: IMetaCodec,
    {
        let buf = Block::alloc(x.packed_size());
        let s = buf.mut_slice(0, buf.len());
        x.encode(s);
        self.nbytes += buf.len() as u64;
        self.flush();
        self.writer.write(s);
    }

    fn alloc<'a>(&mut self, size: usize) -> &'a mut [u8] {
        let rest = self.ring.len() - self.ring.tail();
        if rest < size {
            self.flush();
            self.ring.prod(rest);
            self.ring.cons(rest);
        }
        self.ring.prod(size)
    }

    pub(crate) fn flush(&mut self) {
        let len = self.ring.distance();
        if len > 0 {
            self.writer.write(self.ring.slice(self.ring.head(), len));
            self.ring.cons(len);

            self.writer.sync();
        }
    }
}

impl Drop for Txn<'_> {
    fn drop(&mut self) {
        self.flush();
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
        let mut p = BoxRef::alloc(m.len - BoxRef::HDR_LEN as u32, pos, NULL_EPOCH);

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
            file_stat: DashMap::new(),
            is_cleaning: AtomicBool::new(false),
            txid: AtomicU64::new(0),
            obsolete_files: Mutex::new(Vec::new()),
            cache: Lru::new(256),
            opt,
            writer: null_mut::<GatherWriter>().into(),
            ring: Ring::new(LOG_BUF_SZ),
            mtx: Mutex::new(()),
        }
    }

    pub(crate) fn add_stat(&mut self, stat: FileStat) {
        self.file_stat.insert(stat.file_id, MutRef::new(stat));
    }

    pub(crate) fn update_stat(&mut self, lids: &[u32], stat: FileStat) {
        let shared = MutRef::new(stat);
        for &id in lids {
            let e = self.file_stat.entry(id);
            // if we update first, the reader will read from new mapped file
            // or else the new reader will read from new mapped file, the old reader will still get
            // a copy from old file (but correct data), and it's cache will be removed by us, so we
            // don't need a mechanism such as version reference
            match e {
                Entry::Occupied(mut o) => {
                    self.cache.del(id as u64);
                    o.insert(shared.clone());
                }
                Entry::Vacant(v) => {
                    v.insert(shared.clone());
                }
            }
        }
    }

    pub(crate) fn apply_junks(&mut self, tick: u64, junks: &[u64]) -> Vec<Stat> {
        let mut h: HashMap<u32, Stat> = HashMap::with_capacity(junks.len());
        for &pos in junks {
            let (lid, _) = unpack_id(pos);
            if let Some(mut stat) = self.file_stat.get_mut(&lid) {
                let reloc = self.get_reloc(stat.file_id, pos);
                stat.update(tick, reloc);
                let e = h.entry(stat.file_id);
                e.and_modify(|x| {
                    x.inner = stat.inner;
                    x.deleted_elems.push(reloc.seq);
                })
                .or_insert(Stat {
                    inner: stat.inner,
                    deleted_elems: MutRef::new(vec![reloc.seq]),
                });
            }
        }
        h.values().cloned().collect()
    }

    pub(crate) fn begin(&'_ mut self, file_id: u32) -> Txn<'_> {
        let guard = self.mtx.lock().unwrap();
        let mut numerics = self.numerics.deref().clone();
        let txid = self.txid.fetch_add(1, Relaxed);
        numerics.flushed_id = file_id;
        numerics.txid = txid;
        let mut txn = Txn {
            _guard: guard,
            txid,
            ring: &mut self.ring,
            writer: &mut self.writer,
            h: Crc32cHasher::default(),
            nbytes: 0,
        };
        let b = Begin { txid };
        txn.record(&b);
        txn.record(&numerics);
        txn
    }

    pub(crate) fn try_clean(&mut self) {
        let Ok(_lk) = self.mtx.try_lock() else {
            return;
        };
        if self.writer.pos() < Self::CLEAN_SIZE {
            return;
        }
        drop(_lk);
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
        let mut numerics = self.numerics.deref().clone();
        let last_pid = self.map.len();
        let mut table = PageTable::default();
        const LIMIT: usize = 8192; // estimate 128KB
        let mut w = GatherWriter::append(&tmp_path, 32);
        let mut ring = Ring::new(LOG_BUF_SZ);
        let mtx = Mutex::new(());

        let txid = self.txid.fetch_add(1, Relaxed);
        numerics.txid = txid;

        let mut txn = Txn {
            txid,
            _guard: mtx.lock().unwrap(),
            h: Crc32cHasher::default(),
            ring: &mut ring,
            writer: &mut w,
            nbytes: 0,
        };

        txn.record(&numerics);

        let g = crossbeam_epoch::pin(); // guard page
        for i in ROOT_PID..last_pid {
            let swip = Swip::new(self.map.get(i));
            if swip.is_null() {
                continue;
            }
            let addr = if swip.is_tagged() {
                swip.untagged()
            } else {
                let p = Page::<Loader>::from_swip(swip.raw());
                p.latest_addr()
            };
            table.add(i, addr, 0);
            if table.len() == LIMIT {
                txn.record(&table);
                table.clear();
            }
        }
        drop(g);

        let mut group: HashMap<u32, Vec<u32>> = HashMap::new();
        for item in self.file_stat.iter() {
            group.entry(item.file_id).or_default().push(*item.key());
        }

        let mut lid = Lid::empty();
        for (k, v) in group.iter() {
            // it's possible that stat has been removed
            if let Some(fstat) = self.file_stat.get(k) {
                let stat = fstat.copy();
                // stat must flush before lid
                txn.record(&stat);
            }

            lid.reset(*k);
            lid.add_multiple(v);
            txn.record(&lid);
        }

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
        let _lk = self.mtx.lock().unwrap();
        let old = self.numerics.next_manifest_id.fetch_add(2, Release);
        self.writer.reset(&self.opt.manifest(old + 2));
        old
    }

    pub(crate) fn load_impl(&self, addr: u64) -> Option<BoxRef> {
        let (id, _) = unpack_id(addr);
        // a read lock guard the whole function, it's necessary
        let Some(stat) = self.file_stat.get(&id) else {
            log::error!("can't get physical id by {:?}", unpack_id(addr));
            for item in self.file_stat.iter() {
                log::debug!("=> {} => {:?}", item.key(), item.value().inner);
            }
            panic!("can't get physical id by logical id {id}");
        };
        let file_id = stat.file_id;

        loop {
            if let Some(r) = self.cache.get(file_id as u64) {
                return Some(r.read_at(addr));
            }

            let Ok(_lk) = self.mtx.try_lock() else {
                continue;
            };
            self.load_cache(file_id)?;
        }
    }

    pub(crate) fn retain(&mut self, set: &[u32]) {
        for &id in set {
            self.file_stat.remove(&id);
            self.cache.del(id as u64);
        }
        self.save_obsolete_files(set);
    }

    pub(crate) fn save_obsolete_files(&self, id: &[u32]) {
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

    fn get_reloc(&self, file_id: u32, pos: u64) -> Reloc {
        loop {
            if let Some(x) = self.cache.get(file_id as u64) {
                return *x.map.get(&pos).expect("addr in Junk but not flushed");
            }

            let Ok(_lk) = self.mtx.try_lock() else {
                continue;
            };
            self.load_cache(file_id).expect("file must exist");
        }
    }

    fn load_cache(&self, file_id: u32) -> Option<()> {
        let f = FileReader::open(self.opt.data_file(file_id))?;
        self.cache.add(file_id as u64, f);
        Some(())
    }
}
