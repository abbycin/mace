use parking_lot::Mutex;
use std::cmp::max;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;

use crate::io::{File, GatherIO};

use crate::cc::context::Context;
use crate::cc::wal::{
    EntryType, Location, PayloadType, WalAbort, WalBegin, WalCommit, WalReader, WalUpdate, ptr_to,
    wal_record_sz,
};
use crate::index::tree::Tree;
use crate::map::table::PageMap;
use crate::meta::Manifest;
use crate::meta::builder::ManifestBuilder;
use crate::types::data::{Key, Record, Ver};
use crate::utils::block::Block;
use crate::utils::data::{Position, WalDesc, WalDescHandle};
use crate::utils::lru::Lru;
use crate::utils::options::ParsedOptions;
use crate::utils::{Handle, NULL_CMD, NULL_ORACLE, OpCode};
use crate::{Options, static_assert};
use crossbeam_epoch::Guard;

/// there are some cases can't recover:
/// 1. manifest file missing or corrupted
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
    pub(crate) fn phase2(
        &mut self,
        ctx: Handle<Context>,
        desc: &[WalDescHandle],
        tree: &Tree,
    ) -> Result<(), OpCode> {
        let g = crossbeam_epoch::pin();
        let numerics = &ctx.manifest.numerics;
        let mut oracle = numerics.oracle.load(Relaxed);
        let mut block = Block::alloc(Self::INIT_BLOCK_SIZE);

        for d in desc.iter() {
            let (group, checkpoint) = {
                let x = d.lock();
                (x.group, x.checkpoint)
            };
            // analyze and redo starts from latest checkpoint
            let cur_oracle = self.analyze(&g, group, checkpoint, &mut block, tree)?;
            oracle = max(oracle, cur_oracle);
            g.flush();
        }

        let recovered = !self.dirty_table.is_empty() || !self.undo_table.is_empty();
        if !self.dirty_table.is_empty() {
            self.redo(&mut block, &g, tree)?;
        }
        if !self.undo_table.is_empty() {
            self.undo(&mut block, &g, tree)?;
        }
        if recovered {
            // that's why we call it oracle, or else keep using the intact oracle in numerics
            oracle += 1;
        }
        log::trace!("oracle {oracle}");
        numerics.oracle.store(oracle, Relaxed);
        numerics.wmk_oldest.store(oracle, Relaxed);
        Ok(())
    }

    fn get_size(e: EntryType, len: usize) -> Result<Option<usize>, OpCode> {
        let sz = wal_record_sz(e)?;
        if len < sz { Ok(None) } else { Ok(Some(sz)) }
    }

    fn handle_update(
        &mut self,
        g: &Guard,
        f: &mut File,
        loc: &mut Location,
        buf: &mut [u8],
        tree: &Tree,
    ) -> Result<bool, OpCode> {
        assert!((loc.len as usize) < buf.len());
        f.read(&mut buf[0..loc.len as usize], loc.pos.offset)?;

        let u = ptr_to::<WalUpdate>(buf.as_ptr());

        if !u.is_intact() {
            return Ok(false);
        }

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
        Ok(true)
    }

    fn analyze(
        &mut self,
        g: &Guard,
        group_id: u8,
        addr: Position,
        block: &mut Block,
        tree: &Tree,
    ) -> Result<u64, OpCode> {
        let Position {
            file_id,
            mut offset,
        } = addr;
        let mut pos;
        let mut oracle = 0;
        let mut loc = Location {
            group_id: group_id as u32,
            pos: Position::default(),
            len: 0,
        };

        for i in file_id.. {
            let path = self.opt.wal_file(group_id, i);
            if !path.exists() {
                break; // no more wal file
            }
            let mut f = File::options().read(true).write(true).open(&path)?;
            let end = f.size()?;
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
                let start_pos = pos;
                let hdr = {
                    let hdr = &mut buf[0..1];
                    f.read(hdr, pos)?;
                    hdr[0]
                };
                let et: EntryType = hdr.try_into()?;

                let Some(sz) = Self::get_size(et, (end - pos) as usize)? else {
                    pos = start_pos;
                    break;
                };
                debug_assert!(sz < Self::INIT_BLOCK_SIZE);

                log::trace!("pos {pos} sz {sz} {et:?}");
                f.read(&mut buf[0..sz], pos)?;

                pos += sz as u64;
                let ptr = buf.as_ptr();
                match et {
                    EntryType::Commit => {
                        let a = ptr_to::<WalCommit>(ptr);
                        if !a.is_intact() {
                            pos = start_pos;
                            break;
                        }
                        log::trace!("{a:?}");
                        self.undo_table.remove(&{ a.txid });
                    }
                    EntryType::Abort => {
                        let a = ptr_to::<WalAbort>(ptr);
                        if !a.is_intact() {
                            pos = start_pos;
                            break;
                        }
                        log::trace!("{a:?}");
                        self.undo_table.remove(&{ a.txid });
                    }
                    EntryType::Begin => {
                        let b = ptr_to::<WalBegin>(ptr);
                        if !b.is_intact() {
                            pos = start_pos;
                            break;
                        }
                        log::trace!("{b:?}");
                        self.undo_table.insert(
                            b.txid,
                            Location {
                                group_id: group_id as u32,
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
                        use crate::cc::wal::WalCheckpoint;
                        let c = ptr_to::<WalCheckpoint>(ptr);
                        if !c.is_intact() {
                            pos = start_pos;
                            break;
                        }
                    }
                    EntryType::Update => {
                        let u = ptr_to::<WalUpdate>(ptr);
                        if pos + u.payload_len() as u64 > end {
                            pos = start_pos;
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
                        if !self.handle_update(g, &mut f, &mut loc, buf, tree)? {
                            pos = start_pos;
                            break;
                        }
                        pos += u.payload_len() as u64;
                    }
                    _ => {
                        return Err(OpCode::Corruption);
                    }
                }
            }

            if pos < end {
                if !self.opt.truncate_corrupted_wal {
                    return Err(OpCode::Corruption);
                }
                // truncate the WAL if it's imcomplete
                log::trace!("truncate {path:?} from {end} to {pos}");
                f.truncate(pos)?;
                // if we truncated the file, we must stop here
                break;
            }
        }

        Ok(oracle)
    }

    fn get_file(
        cache: &Lru<(u32, u64), Rc<File>>,
        cap: usize,
        opt: &Options,
        group_id: u32,
        seq: u64,
    ) -> Result<Option<Rc<File>>, OpCode> {
        let id = (group_id, seq);
        if let Some(f) = cache.get(&id) {
            Ok(Some(f.clone()))
        } else {
            let path = opt.wal_file(group_id as u8, seq);
            if !path.exists() {
                return Ok(None);
            }
            let f = Rc::new(File::options().read(true).open(&path)?);
            cache.add(cap, id, f.clone());
            Ok(Some(f))
        }
    }

    fn redo(&mut self, block: &mut Block, g: &Guard, tree: &Tree) -> Result<(), OpCode> {
        let cache = Lru::new();
        let cap = 32;

        // NOTE: because the `Ver` is descending ordered by txid first, we call `rev` here to make
        //  smaller txid to apply first
        for (_, table) in self.dirty_table.iter().rev() {
            let Location { group_id, pos, len } = *table;
            let Some(f) = Self::get_file(&cache, cap, &self.opt, group_id, pos.file_id)? else {
                break;
            };
            assert!(len as usize <= block.len());
            f.read(block.mut_slice(0, len as usize), pos.offset)?;
            let c = ptr_to::<WalUpdate>(block.data());
            if !c.is_intact() {
                return Err(OpCode::Corruption);
            }
            let ok = c.key();
            let key = Key::new(ok, Ver::new(c.txid, c.cmd_id));

            let r = match c.sub_type() {
                PayloadType::Insert => {
                    let i = c.put();
                    let val = Record::normal(c.group_id, i.val());
                    tree.put(g, key, val)
                }
                PayloadType::Update => {
                    let u = c.update();
                    let val = Record::normal(c.group_id, u.new_val());
                    tree.put(g, key, val)
                }
                PayloadType::Delete => {
                    let val = Record::remove(c.group_id);
                    tree.put(g, key, val)
                }
                PayloadType::Clr => {
                    let r = c.clr();
                    let val = if r.is_tombstone() {
                        Record::remove(c.group_id)
                    } else {
                        Record::normal(c.group_id, r.val())
                    };
                    tree.put(g, key, val)
                }
            };
            assert!(r.is_ok());
        }
        Ok(())
    }

    fn undo(&self, block: &mut Block, g: &Guard, tree: &Tree) -> Result<(), OpCode> {
        let reader = WalReader::new(&tree.store.context, g);
        for (txid, addr) in &self.undo_table {
            reader.rollback(block, *txid, *addr, tree)?;
        }
        Ok(())
    }

    fn load_desc(&self) -> Result<Vec<WalDescHandle>, OpCode> {
        let mut desc: Vec<WalDescHandle> = (0..self.opt.concurrent_write)
            .map(|x| WalDescHandle::new(Mutex::new(WalDesc::new(x))))
            .collect();

        for d in &mut desc {
            let mut x = d.lock();
            let path = self.opt.desc_file(x.group);
            if !path.exists() {
                log::trace!("no log record for group {}", x.group);
                continue;
            }
            let f = File::options()
                .read(true)
                .open(&path)
                .map_err(|_e| OpCode::IoError)?;
            let dst = x.as_mut_slice();
            f.read(dst, 0)?;
            if !x.is_valid() {
                log::error!("{path:?} was corrupted");
                return Err(crate::utils::OpCode::BadData);
            }
        }

        Ok(desc)
    }

    /// load latest manifest from btree-store
    fn load_manifest(&self) -> Result<Manifest, OpCode> {
        let mut b = ManifestBuilder::new(self.opt.clone());
        b.load()?;
        Ok(b.finish())
    }
}
