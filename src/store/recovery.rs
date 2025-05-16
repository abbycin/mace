use core::panic;
use std::cmp::max;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;

use io::{File, GatherIO};

use crate::cc::data::Ver;
use crate::cc::wal::{
    EntryType, Location, PayloadType, WalAbort, WalBegin, WalReader, WalSpan, WalUpdate, ptr_to,
    wal_record_sz,
};
use crate::index::Key;
use crate::index::data::Value;
use crate::index::tree::Tree;
use crate::map::Mapping;
use crate::map::data::DataFooter;
use crate::utils::block::Block;
use crate::utils::data::{MapEntry, MetaInner, WalDescHandle};
use crate::utils::lru::LruInner;
use crate::utils::{NULL_CMD, NULL_ORACLE, pack_id, raw_ptr_to_ref, unpack_id};
use crate::{
    Options,
    map::table::PageMap,
    utils::{NEXT_ID, data::Meta},
};
use crate::{Record, static_assert};

#[derive(Debug, PartialEq, Eq)]
enum State {
    New,
    Damaged,
    Ok,
}

pub(crate) struct Recovery {
    opt: Arc<Options>,
    buf: Block,
    state: State,
    dirty_table: BTreeMap<Ver, Location>,
    /// txid, last lsn
    undo_table: BTreeMap<u64, Location>,
}

impl Recovery {
    pub(crate) fn new(opt: Arc<Options>) -> Self {
        if !opt.db_root.exists() {
            log::info!("create db_root {:?}", opt.db_root);
            std::fs::create_dir_all(&opt.db_root).expect("can't create db_root");
            std::fs::create_dir_all(opt.wal_root()).expect("can't create wal root");
        }

        Self {
            opt,
            buf: Block::alloc(512),
            state: State::New,
            dirty_table: BTreeMap::new(),
            undo_table: BTreeMap::new(),
        }
    }

    pub(crate) fn phase1(&mut self) -> (Arc<Meta>, PageMap, Mapping, Vec<WalDescHandle>) {
        let (meta, mut desc) = self.check();
        let mut mapping = Mapping::new(self.opt.clone());
        let map = match self.state {
            State::New => PageMap::default(),
            State::Damaged => self.enumerate(&meta, &mut desc, &mut mapping),
            State::Ok => self.load(&meta, &mut mapping),
        };
        (Arc::new(meta), map, mapping, desc)
    }

    pub(crate) fn phase2(&mut self, meta: Arc<Meta>, desc: &[WalDescHandle], tree: &Tree) {
        let mut oracle = meta.oracle.load(Relaxed);
        if self.state == State::Damaged {
            let mut block = Block::alloc(self.opt.max_data_size());
            for d in desc.iter() {
                // analyze and redo starts from latest checkpoint
                let cur_oracle = self.analyze(d.worker, d.checkpoint, &mut block, tree);
                oracle = max(oracle, cur_oracle);
            }

            if !self.dirty_table.is_empty() {
                self.redo(&mut block, tree);
            }
            if !self.undo_table.is_empty() {
                self.undo(&mut block, tree);
            }
            // that's why we call it oracle, or else keep using the intact oracle in meta
            oracle += 1;
        }
        log::trace!("oracle {}", oracle);
        meta.oracle.store(oracle, Relaxed);
        meta.wmk_oldest.store(oracle, Relaxed);
    }

    fn get_size(e: EntryType, len: usize) -> Option<usize> {
        let sz = wal_record_sz(e);
        if len < sz { None } else { Some(sz) }
    }

    fn handle_update(&mut self, f: &mut File, loc: &mut Location, buf: &mut [u8], tree: &Tree) {
        assert!((loc.len as usize) < buf.len());
        f.read(&mut buf[0..loc.len as usize], loc.off as u64)
            .unwrap();

        let u = ptr_to::<WalUpdate>(buf.as_ptr());
        let ver = Ver::new(u.txid, u.cmd_id);

        debug_assert!(!self.dirty_table.contains_key(&ver));

        let raw = u.key();
        let r = tree
            .get::<Key, Record>(Key::new(raw, NULL_ORACLE, NULL_CMD))
            .map(|(k, _)| *k.ver());

        // check whether the key is exits in data or is latest
        let lost = r.map(|v| v > ver).map_err(|_| true).unwrap_or_else(|x| x);
        if lost {
            self.dirty_table.insert(ver, *loc);
        }
    }

    fn analyze(&mut self, wid: u16, addr: u64, block: &mut Block, tree: &Tree) -> u64 {
        let (seq, mut off) = unpack_id(addr);
        let mut pos;
        let mut oracle = 0;
        let mut loc = Location {
            wid: wid as u32,
            seq: 0,
            off: 0,
            len: 0,
        };

        // TODO: handle wal seq wrapping
        for i in seq..=u32::MAX {
            let path = self.opt.wal_file(wid, i);
            if !path.exists() {
                break; // no more wal file
            }
            let mut f = File::options().read(true).write(true).open(&path).unwrap();
            let end = f.size().unwrap();
            if end == 0 {
                break; // empty wal file
            }
            let buf = block.mut_slice(0, block.len());
            static_assert!(size_of::<EntryType>() == 1);

            loc.seq = seq;
            pos = off as u64;
            off = 0;

            log::trace!("{:?} pos {} end {}", path, pos, end);
            while pos < end {
                let hdr = {
                    let hdr = &mut buf[0..1];
                    f.read(hdr, pos).unwrap();
                    hdr[0]
                };
                let et: EntryType = hdr.into();

                let Some(sz) = Self::get_size(et, (end - pos) as usize) else {
                    break;
                };

                log::trace!("pos {} sz {} {:?}", pos, sz, et);
                f.read(&mut buf[0..sz], pos).unwrap();

                pos += sz as u64;
                let ptr = buf.as_ptr();
                match et {
                    EntryType::Commit | EntryType::Abort => {
                        let a = ptr_to::<WalAbort>(ptr);
                        log::trace!("{:?}", a);
                        self.undo_table.remove(&{ a.txid });
                    }
                    EntryType::Begin => {
                        let b = ptr_to::<WalBegin>(ptr);
                        log::trace!("{:?}", b);
                        self.undo_table.insert(
                            b.txid,
                            Location {
                                wid: wid as u32,
                                seq,
                                off: pos as u32 - sz as u32,
                                len: 0,
                            },
                        );
                        oracle = max(b.txid, oracle);
                    }
                    EntryType::CheckPoint | EntryType::Padding => {
                        // do nothing
                    }
                    EntryType::Span => {
                        let p = ptr_to::<WalSpan>(ptr);
                        pos += p.span as u64;
                    }
                    EntryType::Update => {
                        let u = ptr_to::<WalUpdate>(ptr);
                        if pos + u.payload_len() as u64 > end {
                            break;
                        }
                        loc.len = (sz + u.payload_len()) as u32;
                        log::trace!("{pos} => {:?}", u);
                        loc.off = (pos - sz as u64) as u32;

                        if let Some(l) = self.undo_table.get_mut(&{ u.txid }) {
                            // update to latest record position
                            l.seq = seq;
                            l.off = loc.off;
                        }
                        self.handle_update(&mut f, &mut loc, buf, tree);
                        pos += u.payload_len() as u64;
                    }
                    _ => {
                        unreachable!("invalid entry type {}", hdr);
                    }
                }
            }

            if pos < end {
                // truncate the WAL if it's imcomplete
                log::trace!("truncate {:?} from {} to {}", path, end, pos);
                f.truncate(pos).expect("can't truncate file");
            }
        }

        oracle
    }

    fn get_file(
        cache: &LruInner<u64, Rc<File>>,
        cap: usize,
        opt: &Options,
        wid: u32,
        seq: u32,
    ) -> Option<Rc<File>> {
        let id = pack_id(wid, seq);
        if let Some(f) = cache.get(&id) {
            Some(f.clone())
        } else {
            let path = opt.wal_file(wid as u16, seq);
            if !path.exists() {
                return None;
            }
            let f = Rc::new(File::options().read(true).open(&path).unwrap());
            cache.add(cap, id, f.clone());
            Some(f)
        }
    }

    fn redo(&mut self, block: &mut Block, tree: &Tree) {
        let cache = LruInner::new();
        let cap = 32;

        // NOTE: because the `Ver` is descending ordered by txid first, we call `rev` here to make
        //  smaller txid to apply first
        for (_, table) in self.dirty_table.iter().rev() {
            let Location { wid, seq, off, len } = *table;
            let Some(f) = Self::get_file(&cache, cap, &self.opt, wid, seq) else {
                break;
            };
            f.read(block.mut_slice(0, len as usize), off as u64)
                .unwrap();
            let c = ptr_to::<WalUpdate>(block.data());
            let ok = c.key();
            let key = Key::new(ok, c.txid, c.cmd_id);
            // redo never write log, so we manually make arena flush work
            tree.store.buffer.update_flsn();
            let r = match c.sub_type() {
                PayloadType::Insert => {
                    let i = c.put();
                    let val = Value::Put(Record::normal(c.worker_id, i.val()));
                    tree.put(key, val)
                }
                PayloadType::Update => {
                    let u = c.update();
                    let val = Value::Put(Record::normal(c.worker_id, u.new_val()));
                    tree.put(key, val)
                }
                PayloadType::Delete => {
                    let val = Value::Del(Record::remove(c.worker_id));
                    tree.put(key, val)
                }
                PayloadType::Clr => {
                    let r = c.clr();
                    let val = if r.is_tombstone() {
                        Value::Del(Record::remove(c.worker_id))
                    } else {
                        Value::Put(Record::normal(c.worker_id, r.val()))
                    };
                    tree.put(key, val)
                }
            };
            assert!(r.is_ok());
        }
    }

    fn undo(&self, block: &mut Block, tree: &Tree) {
        let reader = WalReader::new(&tree.store.context);
        for (txid, addr) in &self.undo_table {
            reader.rollback(block, *txid, *addr, tree);
        }
    }

    /// if either of WAL or meta file is not exist, we treat it as a new database
    fn check(&mut self) -> (Meta, Vec<WalDescHandle>) {
        let f = self.opt.meta_file();
        let mut desc = Vec::with_capacity(self.opt.workers);
        (0..self.opt.workers as u16).for_each(|x| desc.push(WalDescHandle::new(x)));

        if !f.exists() {
            self.state = State::New;
            return (Meta::new(self.opt.workers), desc);
        }

        let file_sz = f.metadata().expect("can't get metadata of meta file").len() as usize;
        if file_sz < size_of::<MetaInner>() {
            self.state = State::Damaged;
            log::warn!("corrupted meta file, ignore it");
            return (Meta::new(self.opt.workers), desc);
        }

        if file_sz > self.buf.len() {
            self.buf.realloc(file_sz);
        }

        let f = File::options()
            .read(true)
            .open(&f)
            .expect("can't open meta file");
        let nbytes = f
            .read(self.buf.mut_slice(0, self.buf.len()), 0)
            .expect("can't read meata file");

        let Ok(meta) = Meta::deserialize(self.buf.slice(0, nbytes), self.opt.workers) else {
            self.state = State::Damaged;
            return (Meta::new(self.opt.workers), desc);
        };

        if !meta.is_complete() {
            self.state = State::Damaged;
            return (Meta::new(self.opt.workers), desc);
        }

        self.load_desc(&meta, &mut desc);

        self.state = State::Ok;
        (meta, desc)
    }

    // because we sync `desc` before `meta`, to simplify the recover procedure, we assume that when
    // `meta` is intact the `desc` is also intact
    fn load_desc(&self, meta: &Meta, desc: &mut [WalDescHandle]) {
        let lk = meta.mask.read().unwrap();
        for (ok, i) in lk.iter() {
            if !ok {
                continue;
            }
            let path = self.opt.desc_file(i as u16);
            let f = File::options()
                .read(true)
                .open(&path)
                .unwrap_or_else(|_| panic!("{:?} must exist", path));
            let dst = desc[i as usize].as_mut_slice();
            f.read(dst, 0).unwrap();
        }
    }

    fn load(&mut self, meta: &Meta, mapping: &mut Mapping) -> PageMap {
        let table = PageMap::default();
        let mut maps = Vec::new();

        Self::readdir(&self.opt.db_root, |name| {
            if name.starts_with(Options::DATA_PREFIX) {
                let v: Vec<&str> = name.split(Options::DATA_PREFIX).collect();
                let id = v[1].parse::<u32>().expect("invalid number");
                maps.push(id);
            }
        });

        maps.sort_unstable();

        self.load_data(&maps, meta, &table, mapping);
        assert_eq!(self.state, State::Ok);
        table
    }

    // NOTE: althrough id wrap around is not handled, it's enough for PB-level data storeage
    fn readdir<F>(path: &PathBuf, mut f: F)
    where
        F: FnMut(&str),
    {
        let dir = std::fs::read_dir(path).expect("can't readdir");
        for i in dir {
            let tmp = i.expect("can't get dir entry");
            if !tmp.file_type().unwrap().is_file() {
                continue;
            }
            let p = tmp.file_name();
            let name = p.to_str().expect("can't filename");
            f(name);
        }
    }

    fn load_wal_one(&self, logs: &[u32], meta: &Meta, desc: &mut WalDescHandle) {
        let mut oracle = meta.oracle.load(Relaxed);
        let mut ckpt = 0;
        // assume WAL header is less than it
        let mut buf = [0u8; 128];
        // we prefer to use the latest file's last checkpoint
        let mut find_ckpt = false;

        // new to old
        for i in logs.iter().rev() {
            let path = self.opt.wal_file(desc.worker, *i);
            if !path.exists() {
                panic!("lost wal file {:?}", path);
            }
            let f = File::options()
                .read(true)
                .write(true) // for truncate
                .open(&path)
                .unwrap();
            let end = f.size().unwrap();
            let mut pos = 0;
            while pos < end {
                f.read(&mut buf[0..1], pos).unwrap();
                let h = buf[0].into();
                let Some(sz) = Self::get_size(h, (end - pos) as usize) else {
                    break;
                };

                f.read(&mut buf[0..sz], pos).unwrap();
                pos += sz as u64;

                log::trace!("load wal {:?}", h);

                match h {
                    EntryType::Begin => {
                        let b = ptr_to::<WalBegin>(buf.as_ptr());
                        oracle = max(oracle, b.txid);
                    }
                    EntryType::Padding => {
                        // do nothing
                    }
                    EntryType::Span => {
                        let p = ptr_to::<WalSpan>(buf.as_ptr());
                        pos += p.span as u64;
                    }
                    EntryType::Update => {
                        let u = ptr_to::<WalUpdate>(buf.as_ptr());
                        pos += u.size as u64
                    }
                    EntryType::CheckPoint => {
                        // we will keep looking for the next checkpoint (the latest one)
                        ckpt = pack_id(*i, (pos - sz as u64) as u32);
                        find_ckpt = true;
                    }
                    _ => {}
                }
            }
            if find_ckpt {
                break;
            }
        }
        meta.oracle.store(oracle, Relaxed);
        desc.checkpoint = ckpt;
    }

    fn load_data(&self, maps: &[u32], meta: &Meta, table: &PageMap, mapping: &mut Mapping) {
        let mut last_id = NEXT_ID;
        // old to new
        for i in maps.iter() {
            let i = *i;
            if let Ok(data) = mapping.add(i, true) {
                let d = raw_ptr_to_ref(data.data().cast::<DataFooter>());
                self.build_table(d.entries(), table);
                last_id = i;
            } else {
                // either last data file or compacted data file, ignore them
                let name = self.opt.data_file(i);
                log::info!("unlink imcomplete file {:?}", name);
                std::fs::remove_file(name).expect("never happen");
            }
        }

        meta.next_data.store(last_id + 1, Relaxed);
    }

    fn enumerate(
        &mut self,
        meta: &Meta,
        desc: &mut [WalDescHandle],
        mapping: &mut Mapping,
    ) -> PageMap {
        let table = PageMap::default();
        let mut logs: Vec<Vec<u32>> = vec![Vec::new(); desc.len()];
        let mut maps = Vec::new();

        Self::readdir(&self.opt.db_root, |name| {
            if name.starts_with(Options::DATA_PREFIX) {
                let v: Vec<&str> = name.split(Options::DATA_PREFIX).collect();
                let id = v[1].parse::<u32>().expect("invalid number");
                maps.push(id);
            }
        });

        Self::readdir(&self.opt.wal_root(), |name| {
            if name.starts_with(Options::WAL_PREFIX) {
                let v: Vec<&str> = name.split("_").collect();
                assert_eq!(v.len(), 3);
                let wid = v[1].parse::<u16>().expect("invalid number");
                let seq = v[2].parse::<u32>().expect("invalid number");
                logs[wid as usize].push(seq);
            }
        });

        // sort by modified time, the latest wal is the last element in ids
        // NOTE: the order of log files is important, while it's not for data files, but we sort
        // them anyway (in GC, the compacted data file may get a bigger id than the flushed one, for
        // example: flush get id 13 while gc get id 14, in this case, file data_13 contains newer
        // data than compacted file data_14)
        logs.iter_mut().for_each(|x| {
            x.sort_unstable();
        });
        maps.sort_unstable();

        // if there's no data file we have to traverse all wal (if wal is not cleaned) in phase2, or
        // else we traverse from the last checkpoint here
        if !maps.is_empty() {
            for (idx, x) in logs.iter().enumerate() {
                if x.is_empty() {
                    // this worker has no wal record
                    continue;
                }
                let d = &mut desc[idx];
                // correct the initial value
                d.wal_id = *x.last().unwrap();
                d.checkpoint = pack_id(*x.first().unwrap(), 0);
                self.load_wal_one(x, meta, &mut desc[idx]);
            }
            self.load_data(&maps, meta, &table, mapping);
        }

        table
    }

    fn build_table(&self, map: &[MapEntry], table: &PageMap) {
        for e in map {
            if table.get(e.page_id()) < e.page_addr() {
                table.index(e.page_id()).store(e.page_addr(), Relaxed);
            }
        }
    }
}
