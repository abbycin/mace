use std::cell::RefCell;
use std::cmp::max;
use std::collections::{BTreeMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;

use crate::io::{File, GatherIO};

use crate::cc::context::{Context, GroupBoot};
use crate::cc::wal::{
    EntryType, Location, PayloadType, WalAbort, WalBegin, WalCheckpoint, WalCommit, WalReader,
    WalUpdate, ptr_to, wal_record_sz,
};
use crate::index::tree::Tree;
use crate::types::data::{Key, Record, Ver};
use crate::types::traits::{IKey, ITree, IVal};
use crate::utils::block::Block;
use crate::utils::data::Position;
use crate::utils::lru::Lru;
use crate::utils::options::ParsedOptions;
use crate::utils::{Handle, MutRef, NULL_CMD, NULL_ORACLE, OpCode, ROOT_PID};
use crate::{Options, Store, static_assert};
use crossbeam_epoch::Guard;

struct RecoveryTree(Option<Tree>);

impl ITree for RecoveryTree {
    fn put<K, V>(&self, g: &Guard, k: K, v: V)
    where
        K: IKey,
        V: IVal,
    {
        if let Some(tree) = &self.0 {
            tree.put(g, k, v).unwrap();
        }
    }
}

/// there are some cases can't recover:
/// 1. manifest file missing or corrupted
/// 2. data file lost
///
/// for the last point, if wal file and manifest file are intact, manually recover is possible, we can parse the log and
/// perform necessary redo/undo to bring data back to consistent, and this only apply to the lost of latest data file
pub(crate) struct Recovery {
    opt: Arc<ParsedOptions>,
    dirty_table: BTreeMap<Ver, Location>,
    /// txid, last lsn
    undo_table: BTreeMap<u64, Location>,
    bucket_cache_cap: usize,
    trees: Lru<u64, Tree>,
    loaded_buckets: RefCell<HashSet<u64>>,
}

impl Recovery {
    const INIT_BLOCK_SIZE: usize = 1 << 20;
    const BUCKET_CACHE_CAP: usize = 16;

    pub(crate) fn new(opt: Arc<ParsedOptions>) -> Self {
        Self {
            opt,
            dirty_table: BTreeMap::new(),
            undo_table: BTreeMap::new(),
            bucket_cache_cap: Self::BUCKET_CACHE_CAP,
            trees: Lru::new(),
            loaded_buckets: RefCell::new(HashSet::new()),
        }
    }

    fn get_tree(&self, bucket_id: u64, store: MutRef<Store>) -> RecoveryTree {
        if let Some(tree) = self.trees.get(&bucket_id) {
            return RecoveryTree(Some(tree.clone()));
        }

        // it's possible that bucket has been logically removed, we simply skip process wal entry in redo/undo for that
        // bucket
        if let Some(meta) = store
            .manifest
            .bucket_metas_by_id
            .get(&bucket_id)
            .map(|m| m.clone())
        {
            assert_eq!(meta.bucket_id, bucket_id);
            let bucket_ctx = store
                .manifest
                .load_bucket_context(bucket_id)
                .expect("must exist");
            let tree = Tree::new(store.clone(), ROOT_PID, bucket_ctx);
            if let Some((evicted_id, evicted_tree)) =
                self.trees
                    .add_with_evict(self.bucket_cache_cap, bucket_id, tree.clone())
            {
                drop(evicted_tree);
                self.loaded_buckets.borrow_mut().remove(&evicted_id);
                self.evict_bucket(evicted_id, store.clone());
            }
            self.loaded_buckets.borrow_mut().insert(bucket_id);
            RecoveryTree(Some(tree))
        } else {
            RecoveryTree(None)
        }
    }

    fn evict_bucket(&self, bucket_id: u64, store: MutRef<Store>) {
        store.manifest.buckets.del_bucket(bucket_id);
    }

    fn evict_all(&self, store: MutRef<Store>) {
        let mut loaded = self.loaded_buckets.borrow_mut();
        for bucket_id in loaded.drain() {
            self.trees.del(&bucket_id);
            self.evict_bucket(bucket_id, store.clone());
        }
    }

    pub(crate) fn phase1(
        &mut self,
        numerics: Arc<crate::meta::Numerics>,
    ) -> Result<(Vec<GroupBoot>, Handle<Context>), OpCode> {
        let wal_boot = self.load_wal_boot()?;
        let ctx = Handle::new(Context::new(self.opt.clone(), numerics, &wal_boot));
        Ok((wal_boot, ctx))
    }

    /// we must perform phase2, in case crash happened before data flush and log checkpoint
    pub(crate) fn phase2(
        &mut self,
        wal_boot: &[GroupBoot],
        store: MutRef<Store>,
    ) -> Result<(), OpCode> {
        let g = crossbeam_epoch::pin();
        let mut oracle = store.manifest.numerics.oracle.load(Relaxed);
        let mut block = Block::alloc(Self::INIT_BLOCK_SIZE);

        for (group_id, boot) in wal_boot.iter().enumerate() {
            // analyze and redo starts from latest checkpoint
            let cur_oracle = self.analyze(
                &g,
                group_id as u8,
                boot.checkpoint,
                &mut block,
                store.clone(),
            )?;
            oracle = max(oracle, cur_oracle);
            g.flush();
        }

        let recovered = !self.dirty_table.is_empty() || !self.undo_table.is_empty();
        if !self.dirty_table.is_empty() {
            self.redo(&mut block, &g, store.clone())?;
        }
        if !self.undo_table.is_empty() {
            self.undo(&mut block, &g, store.clone())?;
        }
        if recovered {
            // that's why we call it oracle, or else keep using the intact oracle in numerics
            oracle += 1;
        }
        log::trace!("oracle {oracle}");
        store.manifest.numerics.oracle.store(oracle, Relaxed);
        store.manifest.numerics.wmk_oldest.store(oracle, Relaxed);
        self.evict_all(store);
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
        store: MutRef<Store>,
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
        let target_tree = self.get_tree(u.bucket_id, store);
        if let Some(target_tree) = &target_tree.0 {
            let r = target_tree
                .get(g, Key::new(raw, Ver::new(NULL_ORACLE, NULL_CMD)))
                .map(|(k, _)| *k.ver());

            // check whether the key exists in data or is latest
            let lost = r.map(|v| v > ver).map_err(|_| true).unwrap_or_else(|x| x);
            if lost {
                self.dirty_table.insert(ver, *loc);
            }
        }
        Ok(true)
    }

    fn analyze(
        &mut self,
        g: &Guard,
        group_id: u8,
        addr: Position,
        block: &mut Block,
        store: MutRef<Store>,
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
                        if !self.handle_update(g, &mut f, &mut loc, buf, store.clone())? {
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
                // truncate the WAL if it's incomplete
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

    fn redo(&mut self, block: &mut Block, g: &Guard, store: MutRef<Store>) -> Result<(), OpCode> {
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

            let target_tree = self.get_tree(c.bucket_id, store.clone());

            match c.sub_type() {
                PayloadType::Insert => {
                    let i = c.put();
                    let val = Record::normal(c.group_id, i.val());
                    target_tree.put(g, key, val)
                }
                PayloadType::Update => {
                    let u = c.update();
                    let val = Record::normal(c.group_id, u.new_val());
                    target_tree.put(g, key, val)
                }
                PayloadType::Delete => {
                    let val = Record::remove(c.group_id);
                    target_tree.put(g, key, val)
                }
                PayloadType::Clr => {
                    let r = c.clr();
                    let val = if r.is_tombstone() {
                        Record::remove(c.group_id)
                    } else {
                        Record::normal(c.group_id, r.val())
                    };
                    target_tree.put(g, key, val)
                }
            };
        }
        Ok(())
    }

    fn undo(&self, block: &mut Block, g: &Guard, store: MutRef<Store>) -> Result<(), OpCode> {
        let reader = WalReader::new(&store.context, g);
        for (txid, addr) in &self.undo_table {
            reader.rollback(block, *txid, *addr, |bucket_id| {
                self.get_tree(bucket_id, store.clone())
            })?;
        }
        Ok(())
    }

    fn wal_file_range(&self, group: u8) -> Option<(u64, u64)> {
        let mut min_id = u64::MAX;
        let mut max_id = 0;
        let mut found = false;
        let prefix = format!(
            "{}{}{}{}",
            Options::WAL_PREFIX,
            Options::SEP,
            group,
            Options::SEP
        );

        let iter = std::fs::read_dir(self.opt.log_root()).ok()?;
        for entry in iter.flatten() {
            let name = entry.file_name();
            let Some(raw) = name.to_str() else {
                continue;
            };
            if !raw.starts_with(&prefix) {
                continue;
            }
            let Ok(id) = raw[prefix.len()..].parse::<u64>() else {
                continue;
            };
            found = true;
            min_id = min_id.min(id);
            max_id = max_id.max(id);
        }

        if found { Some((min_id, max_id)) } else { None }
    }

    fn load_wal_boot(&self) -> Result<Vec<GroupBoot>, OpCode> {
        let mut out = Vec::with_capacity(self.opt.concurrent_write as usize);
        for group in 0..self.opt.concurrent_write {
            let group_id = group;
            if let Some((min_id, max_id)) = self.wal_file_range(group_id) {
                let checkpoint = self
                    .find_latest_checkpoint(group_id, min_id, max_id)?
                    .unwrap_or(Position {
                        file_id: min_id,
                        offset: 0,
                    });
                out.push(GroupBoot {
                    oldest_id: min_id,
                    latest_id: max_id,
                    checkpoint,
                });
            } else {
                out.push(GroupBoot {
                    oldest_id: 0,
                    latest_id: 0,
                    checkpoint: Position::default(),
                });
            }
        }
        Ok(out)
    }

    fn find_latest_checkpoint(
        &self,
        group_id: u8,
        min_file: u64,
        max_file: u64,
    ) -> Result<Option<Position>, OpCode> {
        let block = Block::alloc(Self::INIT_BLOCK_SIZE);
        for file_id in (min_file..=max_file).rev() {
            let path = self.opt.wal_file(group_id, file_id);
            if !path.exists() {
                continue;
            }
            let file = File::options().read(true).write(true).open(&path)?;
            let end = file.size()?;
            if end == 0 {
                continue;
            }
            let mut pos = 0;
            let buf = block.mut_slice(0, block.len());
            let mut latest = None;
            while pos < end {
                let hdr = {
                    let hdr = &mut buf[0..1];
                    file.read(hdr, pos)?;
                    hdr[0]
                };
                let Ok(et) = EntryType::try_from(hdr) else {
                    break;
                };
                let Some(sz) = Self::get_size(et, (end - pos) as usize)? else {
                    break;
                };
                file.read(&mut buf[0..sz], pos)?;
                pos += sz as u64;
                let ptr = buf.as_ptr();
                match et {
                    EntryType::Commit => {
                        let c = ptr_to::<WalCommit>(ptr);
                        if !c.is_intact() {
                            break;
                        }
                    }
                    EntryType::Abort => {
                        let a = ptr_to::<WalAbort>(ptr);
                        if !a.is_intact() {
                            break;
                        }
                    }
                    EntryType::Begin => {
                        let b = ptr_to::<WalBegin>(ptr);
                        if !b.is_intact() {
                            break;
                        }
                    }
                    EntryType::CheckPoint => {
                        let c = ptr_to::<WalCheckpoint>(ptr);
                        if !c.is_intact() {
                            break;
                        }
                        latest = Some(c.checkpoint);
                    }
                    EntryType::Update => {
                        let u = ptr_to::<WalUpdate>(ptr);
                        if pos + u.payload_len() as u64 > end {
                            break;
                        }
                        pos += u.payload_len() as u64;
                    }
                    _ => break,
                }
            }
            if latest.is_some() {
                return Ok(latest);
            }
        }
        Ok(None)
    }
}
