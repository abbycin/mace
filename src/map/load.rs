use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::{Arc, Mutex, RwLock, atomic::AtomicU32},
};

use dashmap::DashMap;
use io::{File, GatherIO};

use super::data::{DataMetaReader, FileStat, StatHandle};
use crate::types::{refbox::BoxRef, traits::IHeader};
use crate::{
    OpCode,
    utils::{bitmap::BitMap, data::Reloc, lru::Lru, options::ParsedOptions, unpack_id},
};

pub(crate) struct FileReader {
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

pub struct Mapping {
    // logical to physical map, which may be N to 1 when GC happend, the FileStat's file_id will be
    // replaced to new segment, while the rest field still belong to old file, when active count is
    // reduced to 0, then the entry should be removed from map
    pub(crate) map: RwLock<HashMap<u32, u32>>,
    pub(crate) stats: DashMap<u32, StatHandle>,
    pub(crate) cache: Lru<FileReader>,
    pub(crate) opt: Arc<ParsedOptions>,
    lk: Mutex<()>,
}

impl Mapping {
    pub(crate) fn new(opt: Arc<ParsedOptions>) -> Self {
        Self {
            map: RwLock::new(HashMap::new()),
            stats: DashMap::new(),
            cache: Lru::new(opt.file_cache),
            opt,
            lk: Mutex::new(()),
        }
    }

    pub(crate) fn apply_junks(&self, now: u32, junks: &[u64]) {
        for &pos in junks {
            let (logical_id, _) = unpack_id(pos);
            if let Some(mut stat) = self.stats.get_mut(&logical_id) {
                let reloc = self.get_reloc(stat.file_id, pos);
                stat.update(reloc, now);
            }
        }
    }

    pub(crate) fn add(&self, id: u32, validate: bool) -> Result<(), OpCode> {
        let mut loader = DataMetaReader::new(self.opt.data_file(id), validate)?;
        let hdr = loader.get_meta()?;
        let mut map = HashMap::new();
        let stat = Box::new(FileStat {
            file_id: id,
            up1: hdr.up2,
            up2: hdr.up2,
            nr_active: hdr.nr_active,
            active_size: hdr.active_size,
            total: hdr.nr_active,
            total_size: hdr.active_size,
            dealloc: BitMap::new(hdr.nr_active),
            refcnt: AtomicU32::new(1),
        });
        let handle: StatHandle = Box::into_raw(stat).into();

        hdr.relocs().iter().for_each(|x| {
            map.insert(x.key, x.val);
        });
        let mut lk = self.map.write().unwrap();

        hdr.lids().iter().for_each(|x| {
            lk.insert(*x, id);
            self.stats.insert(*x, handle.clone());
        });

        lk.insert(id, id);
        self.stats.insert(id, handle);

        let file = loader.take();
        let reader = FileReader { file, map };
        self.cache.add(id as u64, reader);
        Ok(())
    }

    pub(crate) fn del(&self, id: u32) {
        self.stats.remove(&id);
        let mut lk = self.map.write().unwrap();
        lk.remove(&id);
    }

    pub(crate) fn retain(&self, set: &HashSet<u32>) {
        self.stats.retain(|_, v| !set.contains(&v.file_id));

        let mut lk = self.map.write().unwrap();
        lk.retain(|_, file_id| !set.contains(file_id));
    }

    pub(crate) fn load_impl(&self, addr: u64) -> BoxRef {
        let (id, _) = unpack_id(addr);
        let lk = self.map.read().unwrap();
        let Some(&file_id) = lk.get(&id) else {
            log::error!("==>> {:?}", unpack_id(addr));
            panic!("xxx");
        };
        // let file_id = *lk.get(&id).unwrap();

        loop {
            if let Some(r) = self.cache.get(file_id as u64) {
                return r.read_at(addr);
            }

            self.fill_cache(file_id);
        }
    }

    pub(crate) fn evict(&self, file_id: u32) {
        self.cache.del(file_id as u64);
    }

    fn get_reloc(&self, file_id: u32, key: u64) -> Reloc {
        loop {
            if let Some(x) = self.cache.get(file_id as u64) {
                return *x.map.get(&key).unwrap();
            }

            let mut cnt: usize = 0;
            while self.fill_cache(file_id).is_none() {
                std::hint::spin_loop();
                cnt += 1;
                if cnt > 1000000 {
                    cnt = 0;
                    log::warn!("spin too long");
                }
            }
        }
    }

    fn fill_cache(&self, file_id: u32) -> Option<()> {
        let _lk = self.lk.try_lock().ok()?;
        let r = FileReader::open(self.opt.data_file(file_id))?;
        self.cache.add(file_id as u64, r);
        Some(())
    }
}
