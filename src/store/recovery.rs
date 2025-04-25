use core::panic;
use std::cmp::max;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::SystemTime;

use io::{File, GatherIO};

use crate::cc::data::Ver;
use crate::cc::wal::{
    ptr_to, wal_record_sz, EntryType, PayloadType, WalAbort, WalBegin, WalCheckpoint, WalReader,
    WalSpan, WalUpdate,
};
use crate::index::data::Value;
use crate::index::tree::Tree;
use crate::index::Key;
use crate::map::data::DataFooter;
use crate::map::Mapping;
use crate::utils::block::Block;
use crate::utils::data::MapEntry;
use crate::utils::{pack_id, raw_ptr_to_ref, unpack_id, NULL_CMD, NULL_ORACLE};
use crate::{
    map::table::PageMap,
    utils::{data::Meta, NEXT_ID},
    Options,
};
use crate::{static_assert, Record};

#[derive(Debug, PartialEq, Eq)]
enum State {
    New,
    Damaged,
    Ok,
}

pub(crate) struct Recovery {
    opt: Arc<Options>,
    buf: [u8; Meta::LEN],
    state: State,
    /// file_id, (ver, (offset, len))
    dirty_table: Vec<(u32, BTreeMap<Ver, (u64, u32)>)>,
    /// txid, last lsn
    undo_table: BTreeMap<u64, u64>,
}

impl Recovery {
    pub(crate) fn new(opt: Arc<Options>) -> Self {
        if !opt.db_root.exists() {
            std::fs::create_dir_all(&opt.db_root).expect("can't create db_root");
        }

        Self {
            opt,
            buf: [const { 0u8 }; Meta::LEN],
            state: State::New,
            dirty_table: Vec::new(),
            undo_table: BTreeMap::new(),
        }
    }

    pub(crate) fn phase1(&mut self) -> (Arc<Meta>, PageMap, Mapping) {
        let meta = self.check();
        let mut mapping = Mapping::new(self.opt.clone());
        let map = match self.state {
            State::New => PageMap::default(),
            State::Damaged => self.enumerate(&meta, &mut mapping),
            State::Ok => self.load(&meta, &mut mapping),
        };
        (Arc::new(meta), map, mapping)
    }

    pub(crate) fn phase2(&mut self, meta: Arc<Meta>, tree: &Tree) {
        let mut oracle = meta.oracle.load(Relaxed);
        if self.state == State::Damaged {
            // NOTE: the wal_id is already the latest one, but there maybe wal file after it and there
            // is no checkpoint record in it
            let addr = meta.ckpt.load(Relaxed);
            let mut block = Block::alloc(self.opt.buffer_size as usize);

            // analyze and redo starts from latest checkpoint
            let cur_oracle = self.analyze(addr, &mut block, tree);
            if !self.dirty_table.is_empty() {
                self.redo(&mut block, tree);
            }
            if !self.undo_table.is_empty() {
                self.undo(&mut block, tree);
            }
            // that's why we call it oracle, or else keep using the intact oracle in meta
            oracle = max(oracle, cur_oracle) + 1;
        }
        log::trace!("oracle {}", oracle);
        meta.oracle.store(oracle, Relaxed);
        meta.wmk_oldest.store(oracle, Relaxed);
    }

    fn get_size(e: EntryType, len: usize) -> Option<usize> {
        let sz = wal_record_sz(e);
        if len < sz {
            None
        } else {
            Some(sz)
        }
    }

    fn handle_update(
        &mut self,
        len: usize,
        f: &mut File,
        map: &mut BTreeMap<Ver, (u64, u32)>,
        beg: u64,
        buf: &mut [u8],
        tree: &Tree,
    ) {
        assert!(len < buf.len());
        f.read(&mut buf[0..len], beg).unwrap();

        let u = ptr_to::<WalUpdate>(buf.as_ptr());
        let ver = Ver::new(u.txid, u.cmd_id);

        debug_assert!(!map.contains_key(&ver));

        let raw = u.key();
        let sz = len as u32;
        let r = tree
            .get::<Key, Record>(Key::new(raw, NULL_ORACLE, NULL_CMD))
            .map(|(k, _)| *k.ver());

        // check whether the key is exits in data or is latest
        let lost = r.map(|v| v > ver).map_err(|_| true).unwrap_or_else(|x| x);
        if lost {
            map.insert(ver, (beg, sz));
        }
    }

    fn analyze(&mut self, addr: u64, block: &mut Block, tree: &Tree) -> u64 {
        let (cur, mut off) = unpack_id(addr);
        let mut pos;
        let mut oracle = 0;

        for i in 0..=u32::MAX {
            let id = cur.wrapping_add(i);
            let path = self.opt.wal_file(id);
            if !path.exists() {
                break; // no more wal file
            }
            let mut f = File::options().read(true).write(true).open(&path).unwrap();
            let end = f.size().unwrap();
            if end == 0 {
                break; // empty wal file
            }
            let buf = block.get_mut_slice(0, block.len());
            static_assert!(size_of::<EntryType>() == 1);
            let mut map = BTreeMap::new();

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
                        self.undo_table
                            .insert(b.txid, pack_id(id, pos as u32 - sz as u32));
                        oracle = max(b.txid, oracle);
                    }
                    EntryType::CheckPoint => {
                        let c = ptr_to::<WalCheckpoint>(ptr);
                        oracle = max(c.txid, oracle);
                        if pos + c.payload_len() as u64 > end {
                            break;
                        }
                        log::trace!("{:?}", c);
                        if c.payload_len() > 0 {
                            f.read(&mut buf[sz..sz + c.payload_len()], pos).unwrap();
                            for item in c.active_txid() {
                                if let std::collections::btree_map::Entry::Vacant(e) =
                                    self.undo_table.entry(item.txid)
                                {
                                    log::trace!("{:?}", item);
                                    let (id, _) = unpack_id(item.addr);
                                    assert_ne!(id, 0);
                                    e.insert(item.addr);
                                }
                            }
                        }
                        pos += c.payload_len() as u64;
                    }
                    EntryType::Padding => {
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
                        let len = sz + u.payload_len();
                        log::trace!("{pos} => {:?}", u);
                        let beg = pos - sz as u64;

                        if let Some(lsn) = self.undo_table.get_mut(&{ u.txid }) {
                            *lsn = pack_id(id, beg as u32); // update to latest record position
                        }
                        self.handle_update(len, &mut f, &mut map, beg, buf, tree);
                        pos += u.payload_len() as u64;
                    }
                    _ => {
                        unreachable!("invalid entry type {}", hdr);
                    }
                }
            }
            if !map.is_empty() {
                self.dirty_table.push((id, map));
            }

            if pos < end {
                // truncate the WAL if it's imcomplete
                log::trace!("truncate {:?} from {} to {}", path, end, pos);
                f.truncate(pos).expect("can't truncate file");
            }
        }

        oracle
    }

    fn redo(&self, block: &mut Block, tree: &Tree) {
        for (id, table) in &self.dirty_table {
            let path = self.opt.wal_file(*id);
            if !path.exists() {
                break; // no more wal file
            }
            let f = File::options().read(true).open(&path).unwrap();
            // NOTE: we reverse the order let smaller txid to apply first
            for (_, (pos, len)) in table.iter().rev() {
                f.read(block.get_mut_slice(0, *len as usize), *pos).unwrap();
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
    }

    fn undo(&self, block: &mut Block, tree: &Tree) {
        let reader = WalReader::new(&tree.store.context);
        for (txid, addr) in &self.undo_table {
            reader.rollback(block, *txid, *addr, tree);
        }
    }

    /// if either of WAL or meta file is not exist, we treat it as a new database
    fn check(&mut self) -> Meta {
        let f = self.opt.meta_file();
        if !f.exists() {
            self.state = State::New;
            return Meta::new();
        }

        let stat = f.metadata().expect("can't get metadata of meta file");
        if stat.len() as usize != Meta::LEN {
            self.state = State::Damaged;
            log::warn!("corrupted meta file, ignore it");
            return Meta::new();
        }

        let f = File::options()
            .read(true)
            .open(&f)
            .expect("can't open meta file");
        f.read(&mut self.buf, 0).expect("can't read meata file");

        let meta = Meta::deserialize(&self.buf);

        if !meta.is_complete() || meta.crc32() != meta.checksum.load(Relaxed) {
            self.state = State::Damaged;
            return Meta::new();
        }

        self.state = State::Ok;
        meta
    }

    fn load(&mut self, meta: &Meta, mapping: &mut Mapping) -> PageMap {
        let table = PageMap::default();
        let mut maps = Vec::new();

        Self::readdir(&self.opt.db_root, |name, modified| {
            if name.starts_with(Options::DATA_PREFIX) {
                let v: Vec<&str> = name.split(Options::DATA_PREFIX).collect();
                let id = v[1].parse::<u32>().expect("invalid number");
                maps.push((modified, id));
            }
        });

        maps.sort_unstable_by(|x, y| x.0.cmp(&y.0));
        let maps: Vec<u32> = maps.iter().map(|x| x.1).collect();

        self.load_data(&maps, meta, &table, mapping);

        assert_eq!(self.state, State::Ok);
        table
    }

    fn readdir<F>(path: &PathBuf, mut f: F)
    where
        F: FnMut(&str, SystemTime),
    {
        let dir = std::fs::read_dir(path).expect("can't readdir");
        for i in dir {
            let tmp = i.expect("can't get dir entry");
            let p = tmp.file_name();
            let m = tmp.metadata().expect("can't get file metadata");
            let name = p.to_str().expect("can't filename");
            f(name, m.modified().expect("can't get modified time"));
        }
    }

    fn load_wal(&self, logs: &[u32], meta: &Meta) {
        let mut oracle = meta.oracle.load(Relaxed);
        let mut ckpt = meta.ckpt.load(Relaxed);
        // assume WAL header is less than it
        let mut buf = [0u8; 128];
        // we prefer to use the latest file's last checkpoint
        let mut find_ckpt = false;

        // new to old
        for i in logs.iter().rev() {
            let path = self.opt.wal_file(*i);
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
                        let c = ptr_to::<WalCheckpoint>(buf.as_ptr());
                        oracle = max(oracle, c.txid);
                        ckpt = pack_id(*i, (pos - sz as u64) as u32);
                        pos += c.payload_len() as u64;
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
        meta.ckpt.store(ckpt, Relaxed);
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

        // TODO: handle id overflow
        meta.next_data.store(last_id + 1, Relaxed);
    }

    fn enumerate(&mut self, meta: &Meta, mapping: &mut Mapping) -> PageMap {
        let table = PageMap::default();
        let mut logs = Vec::new();
        let mut maps = Vec::new();

        Self::readdir(&self.opt.db_root, |name, modified| {
            if name.starts_with(Options::WAL_PREFIX) {
                let v: Vec<&str> = name.split(Options::WAL_PREFIX).collect();
                let id = v[1].parse::<u32>().expect("inavlid number");
                logs.push((modified, id));
            } else if name.starts_with(Options::DATA_PREFIX) {
                let v: Vec<&str> = name.split(Options::DATA_PREFIX).collect();
                let id = v[1].parse::<u32>().expect("invalid number");
                maps.push((modified, id));
            }
        });

        if logs.is_empty() {
            meta.reset();
            return table;
        }

        // sort by modified time, the latest wal is the last element in ids
        logs.sort_unstable_by(|x, y| x.0.cmp(&y.0));
        maps.sort_unstable_by(|x, y| x.0.cmp(&y.0));

        let logs: Vec<u32> = logs.iter().map(|(_, i)| *i).collect();
        let maps: Vec<u32> = maps.iter().map(|(_, i)| *i).collect();

        // correct the initial value
        meta.update_chkpt(*logs.first().unwrap(), 0);
        meta.next_wal.store(*logs.last().unwrap(), Relaxed);

        // if there's no data file we have to traverse all wal (if wal is not cleaned), or else we
        // traverse from the last checkpoint
        if !maps.is_empty() {
            self.load_wal(&logs, meta);
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
