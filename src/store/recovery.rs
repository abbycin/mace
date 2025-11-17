use core::panic;
use std::cmp::max;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Mutex};

use crate::io::{File, GatherIO};

use crate::cc::context::Context;
use crate::cc::wal::{
    EntryType, Location, PayloadType, WalAbort, WalBegin, WalCommit, WalReader, WalUpdate, ptr_to,
    wal_record_sz,
};
use crate::index::tree::Tree;
use crate::meta::Manifest;
use crate::meta::builder::ManifestBuilder;
use crate::types::data::{Key, Record, Ver};
use crate::utils::block::Block;
use crate::utils::data::{Position, WalDesc, WalDescHandle};
use crate::utils::lru::Lru;
use crate::utils::options::ParsedOptions;
use crate::utils::{Handle, NULL_CMD, NULL_ORACLE};
use crate::{OpCode, static_assert};
use crate::{Options, map::table::PageMap};
use crossbeam_epoch::Guard;

/// there are some cases can't recover:
/// 1. manifest file checksum mismatch
/// 2. log desc file checksum mismatch
/// 3. data file lost (if wal file is complete, manual recovery is possible)
///
/// and there is only one case can recover:
/// - abnormal shutdown happened before transaction commit
///
/// in this case we can parse the log and perform necessary redo/undo to bring data back to consistent
pub(crate) struct Recovery {
    opt: Arc<ParsedOptions>,
    dirty_table: BTreeMap<Ver, Location>,
    /// txid, last lsn
    undo_table: BTreeMap<u64, Location>,
}

impl Recovery {
    const INIT_BLOCK_SIZE: usize = 1 << 20;

    pub(crate) fn new(opt: Arc<ParsedOptions>) -> Self {
        Self {
            opt,
            dirty_table: BTreeMap::new(),
            undo_table: BTreeMap::new(),
        }
    }

    pub(crate) fn phase1(
        &mut self,
    ) -> Result<(Arc<PageMap>, Vec<WalDescHandle>, Handle<Context>), OpCode> {
        let desc = self.load_desc()?;
        let manifest = self.load_manifest()?;

        let ctx = Handle::new(Context::new(self.opt.clone(), manifest, &desc));
        Ok((ctx.manifest.map.clone(), desc, ctx))
    }

    /// we must perform phase2, in case crash happened before data flush and log checkpoint
    pub(crate) fn phase2(&mut self, ctx: Handle<Context>, desc: &[WalDescHandle], tree: &Tree) {
        let g = crossbeam_epoch::pin();
        let numerics = &ctx.manifest.numerics;
        let mut oracle = numerics.oracle.load(Relaxed);
        let mut block = Block::alloc(Self::INIT_BLOCK_SIZE);

        for d in desc.iter() {
            let (worker, checkpoint) = {
                let x = d.lock().expect("cant' lock");
                (x.worker, x.checkpoint)
            };
            // analyze and redo starts from latest checkpoint
            let cur_oracle = self.analyze(&g, worker, checkpoint, &mut block, tree);
            oracle = max(oracle, cur_oracle);
            g.flush();
        }

        if !self.dirty_table.is_empty() {
            self.redo(&mut block, &g, tree);
        }
        if !self.undo_table.is_empty() {
            self.undo(&mut block, &g, tree);
        }
        // that's why we call it oracle, or else keep using the intact oracle in numerics
        oracle += 1;
        log::trace!("oracle {oracle}");
        numerics.oracle.store(oracle, Relaxed);
        numerics.wmk_oldest.store(oracle, Relaxed);
    }

    fn get_size(e: EntryType, len: usize) -> Option<usize> {
        let sz = wal_record_sz(e);
        if len < sz { None } else { Some(sz) }
    }

    fn handle_update(
        &mut self,
        g: &Guard,
        f: &mut File,
        loc: &mut Location,
        buf: &mut [u8],
        tree: &Tree,
    ) {
        assert!((loc.len as usize) < buf.len());
        f.read(&mut buf[0..loc.len as usize], loc.pos.offset)
            .unwrap();

        let u = ptr_to::<WalUpdate>(buf.as_ptr());
        let ver = Ver::new(u.txid, u.cmd_id);

        debug_assert!(!self.dirty_table.contains_key(&ver));

        let raw = u.key();
        let r = tree
            .get(g, Key::new(raw, Ver::new(NULL_ORACLE, NULL_CMD)))
            .map(|(k, _)| *k.ver());

        // check whether the key is exits in data or is latest
        let lost = r.map(|v| v > ver).map_err(|_| true).unwrap_or_else(|x| x);
        if lost {
            self.dirty_table.insert(ver, *loc);
        }
    }

    fn analyze(
        &mut self,
        g: &Guard,
        wid: u16,
        addr: Position,
        block: &mut Block,
        tree: &Tree,
    ) -> u64 {
        let Position {
            file_id,
            mut offset,
        } = addr;
        let mut pos;
        let mut oracle = 0;
        let mut loc = Location {
            wid: wid as u32,
            pos: Position::default(),
            len: 0,
        };

        for i in file_id.. {
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

            loc.pos.file_id = i;
            pos = offset;
            offset = 0;

            log::trace!("{path:?} pos {pos} end {end}");
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
                debug_assert!(sz < Self::INIT_BLOCK_SIZE);

                log::trace!("pos {pos} sz {sz} {et:?}");
                f.read(&mut buf[0..sz], pos).unwrap();

                pos += sz as u64;
                let ptr = buf.as_ptr();
                match et {
                    EntryType::Commit => {
                        let a = ptr_to::<WalCommit>(ptr);
                        log::trace!("{a:?}");
                        self.undo_table.remove(&{ a.txid });
                    }
                    EntryType::Abort => {
                        let a = ptr_to::<WalAbort>(ptr);
                        log::trace!("{a:?}");
                        self.undo_table.remove(&{ a.txid });
                    }
                    EntryType::Begin => {
                        let b = ptr_to::<WalBegin>(ptr);
                        log::trace!("{b:?}");
                        self.undo_table.insert(
                            b.txid,
                            Location {
                                wid: wid as u32,
                                pos: Position {
                                    file_id: i,
                                    offset: pos - sz as u64,
                                },
                                len: 0,
                            },
                        );
                        oracle = max(b.txid, oracle);
                    }
                    EntryType::CheckPoint => {
                        // do nothing
                    }
                    EntryType::Update => {
                        let u = ptr_to::<WalUpdate>(ptr);
                        if pos + u.payload_len() as u64 > end {
                            break;
                        }
                        loc.len = (sz + u.payload_len()) as u32;
                        if block.len() < loc.len as usize {
                            block.realloc(loc.len as usize);
                        }
                        log::trace!("{pos} => {u:?}");
                        loc.pos.offset = pos - sz as u64;

                        if let Some(l) = self.undo_table.get_mut(&{ u.txid }) {
                            // update to latest record position
                            l.pos.file_id = i;
                            l.pos.offset = loc.pos.offset;
                        }
                        self.handle_update(g, &mut f, &mut loc, buf, tree);
                        pos += u.payload_len() as u64;
                    }
                    _ => {
                        unreachable!("invalid entry type {}", hdr);
                    }
                }
            }

            if pos < end {
                // truncate the WAL if it's imcomplete
                log::trace!("truncate {path:?} from {end} to {pos}");
                f.truncate(pos).expect("can't truncate file");
            }
        }

        oracle
    }

    fn get_file(
        cache: &Lru<(u32, u64), Rc<File>>,
        cap: usize,
        opt: &Options,
        wid: u32,
        seq: u64,
    ) -> Option<Rc<File>> {
        let id = (wid, seq);
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

    fn redo(&mut self, block: &mut Block, g: &Guard, tree: &Tree) {
        let cache = Lru::new();
        let cap = 32;

        // NOTE: because the `Ver` is descending ordered by txid first, we call `rev` here to make
        //  smaller txid to apply first
        for (_, table) in self.dirty_table.iter().rev() {
            let Location { wid, pos, len } = *table;
            let Some(f) = Self::get_file(&cache, cap, &self.opt, wid, pos.file_id) else {
                break;
            };
            assert!(len as usize <= block.len());
            f.read(block.mut_slice(0, len as usize), pos.offset)
                .unwrap();
            let c = ptr_to::<WalUpdate>(block.data());
            let ok = c.key();
            let key = Key::new(ok, Ver::new(c.txid, c.cmd_id));

            let r = match c.sub_type() {
                PayloadType::Insert => {
                    let i = c.put();
                    let val = Record::normal(c.worker_id, i.val());
                    tree.put(g, key, val)
                }
                PayloadType::Update => {
                    let u = c.update();
                    let val = Record::normal(c.worker_id, u.new_val());
                    tree.put(g, key, val)
                }
                PayloadType::Delete => {
                    let val = Record::remove(c.worker_id);
                    tree.put(g, key, val)
                }
                PayloadType::Clr => {
                    let r = c.clr();
                    let val = if r.is_tombstone() {
                        Record::remove(c.worker_id)
                    } else {
                        Record::normal(c.worker_id, r.val())
                    };
                    tree.put(g, key, val)
                }
            };
            assert!(r.is_ok());
        }
    }

    fn undo(&self, block: &mut Block, g: &Guard, tree: &Tree) {
        let reader = WalReader::new(&tree.store.context, g);
        for (txid, addr) in &self.undo_table {
            reader.rollback(block, *txid, *addr, tree, None);
        }
    }

    fn load_desc(&self) -> Result<Vec<WalDescHandle>, OpCode> {
        let mut desc: Vec<WalDescHandle> = (0..self.opt.workers)
            .map(|x| WalDescHandle::new(Mutex::new(WalDesc::new(x))))
            .collect();

        for d in &mut desc {
            let mut x = d.lock().expect("can't lock");
            let path = self.opt.desc_file(x.worker);
            if !path.exists() {
                log::trace!("no log record for worker {}", x.worker);
                continue;
            }
            let f = File::options()
                .read(true)
                .open(&path)
                .unwrap_or_else(|_| panic!("{:?} must exist", path));
            let dst = x.as_mut_slice();
            f.read(dst, 0).unwrap();
            if !x.is_valid() {
                log::error!("{path:?} was corrupted");
                return Err(OpCode::BadData);
            }
        }

        Ok(desc)
    }

    /// there are a few number of manifest files, we can iterate them to build latest manifest
    fn load_manifest(&self) -> Result<Manifest, OpCode> {
        let mut nums = Vec::new();
        let mut snap = None; // latest snapshot
        Self::readdir(&self.opt.log_root(), |path, name| {
            if name.ends_with("tmp") {
                // remove imcomplete dump
                let _ = std::fs::remove_file(path);
                return;
            }
            if name.starts_with(Options::MANIFEST_PREFIX) {
                let v: Vec<&str> = name.split(Options::SEP).collect();
                assert_eq!(v.len(), 2);
                let num = v[1].parse::<u64>().expect("bad manifest file name");
                if name.ends_with("snap") {
                    if let Some(x) = snap.as_mut() {
                        if *x < num {
                            *x = num;
                        }
                    } else {
                        snap = Some(num);
                    }
                }
                nums.push(num);
            }
        });

        // if has snapshot, keep latest snapshot and later manifest
        nums.retain(|x| {
            if let Some(s) = snap.as_ref() {
                *x >= *s
            } else {
                true
            }
        });

        let mut b = ManifestBuilder::new(self.opt.clone());
        nums.sort_unstable();
        for (idx, i) in nums.iter().enumerate() {
            let is_last = idx == nums.len() - 1;
            let path = if let Some(snap_id) = snap.as_ref()
                && snap_id == i
            {
                assert_eq!(idx, 0);
                self.opt.snapshot(*snap_id)
            } else {
                self.opt.manifest(*i)
            };
            let e = b.add(path, is_last);
            match e {
                Ok(()) => {}
                Err(OpCode::BadData) => return Err(OpCode::BadData),
                Err(e) => {
                    log::error!("parse manifest fail: {e:?}");
                    return Err(e);
                }
            }
        }

        // log desc is complete (we checked in the very beginning), but data file is not, in this case
        // the wal must has records after checkpoint in log desc, so we can recover data from wal records
        Ok(b.finish(snap))
    }

    fn readdir<F>(path: &PathBuf, mut f: F)
    where
        F: FnMut(PathBuf, &str),
    {
        let dir = std::fs::read_dir(path).expect("can't readdir");
        for i in dir.flatten() {
            if !i.file_type().unwrap().is_file() {
                continue;
            }
            let p = i.file_name();
            let name = p.to_str().expect("can't filename");
            f(i.path(), name);
        }
    }
}
