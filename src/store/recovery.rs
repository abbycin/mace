use std::cell::RefCell;
use std::cmp::max;
use std::collections::{BTreeMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;

use crate::io::{File, GatherIO};

use crate::cc::context::{Context, GroupBoot};
use crate::cc::wal::{
    EntryType, Location, PayloadType, WalAbort, WalBegin, WalCheckpoint, WalCommit, WalUpdate,
    ptr_to, wal_record_sz,
};
use crate::index::tree::Tree;
use crate::types::data::{Key, Record, Ver};
use crate::utils::block::Block;
use crate::utils::data::Position;
use crate::utils::lru::Lru;
use crate::utils::observe::{CounterMetric, EventKind, GaugeMetric, HistogramMetric, ObserveEvent};
use crate::utils::options::ParsedOptions;
use crate::utils::{Handle, MutRef, NULL_CMD, NULL_ORACLE, OpCode, ROOT_PID};
use crate::{Options, Store, static_assert};
use crossbeam_epoch::Guard;
use std::time::Instant;

/// there are some cases can't recover:
/// 1. manifest file missing or corrupted
/// 2. data file lost
///
/// for the last point, if wal file and manifest file are intact, manually recover is possible, we can parse the log and
/// perform necessary redo/undo to bring data back to consistent, and this only apply to the lost of latest data file
pub(crate) struct Recovery {
    opt: Arc<ParsedOptions>,
    dirty_table: BTreeMap<Ver, Location>,
    committed_txns: HashSet<u64>,
    in_progress_txns: HashSet<u64>,
    tx_last_update: BTreeMap<u64, Location>,
    tx_pin_file: BTreeMap<u64, u64>,
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
            committed_txns: HashSet::new(),
            in_progress_txns: HashSet::new(),
            tx_last_update: BTreeMap::new(),
            tx_pin_file: BTreeMap::new(),
            bucket_cache_cap: Self::BUCKET_CACHE_CAP,
            trees: Lru::new(),
            loaded_buckets: RefCell::new(HashSet::new()),
        }
    }

    fn mark_tx_aborted_and_enqueue_clean(&mut self, store: MutRef<Store>, txid: u64) {
        let tail = self.tx_last_update.remove(&txid);
        self.in_progress_txns.remove(&txid);
        self.committed_txns.remove(&txid);

        if let Some(tail) = tail {
            let pin_file_id = self
                .tx_pin_file
                .remove(&txid)
                .unwrap_or(tail.pos.file_id)
                .min(tail.pos.file_id);
            store.context.mark_tx_aborted(txid);
            store
                .context
                .enqueue_abort_clean(txid, tail.group_id as u8, tail.pos, pin_file_id);
        } else {
            store.context.clear_tx_outcome(txid);
            store.context.remove_abort_clean(txid);
            self.tx_pin_file.remove(&txid);
        }
    }

    fn mark_tx_committed(&mut self, store: MutRef<Store>, txid: u64) {
        self.in_progress_txns.remove(&txid);
        self.committed_txns.insert(txid);
        self.tx_last_update.remove(&txid);
        self.tx_pin_file.remove(&txid);
        store.context.clear_tx_outcome(txid);
        store.context.remove_abort_clean(txid);
    }

    fn get_tree(&self, bucket_id: u64, store: MutRef<Store>) -> Option<Tree> {
        if let Some(tree) = self.trees.get(&bucket_id) {
            return Some(tree.clone());
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
            Some(tree)
        } else {
            None
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
        let phase2_started = Instant::now();
        self.opt.observer.event(ObserveEvent {
            kind: EventKind::RecoveryPhase2Begin,
            bucket_id: 0,
            txid: 0,
            file_id: 0,
            value: wal_boot.len() as u64,
        });

        let mut oracle = store.manifest.numerics.oracle.load(Relaxed);
        let mut block = Block::alloc(Self::INIT_BLOCK_SIZE);

        for (group_id, boot) in wal_boot.iter().enumerate() {
            // redo correctness depends on rebuilding transaction outcomes and pending abort-clean chains
            // from all retained WAL files, not just latest checkpoint window
            let analyze_started = Instant::now();
            let cur_oracle = self.analyze(
                group_id as u8,
                Position {
                    file_id: boot.oldest_id,
                    offset: 0,
                },
                boot.oldest_id,
                boot.latest_id,
                &mut block,
                store.clone(),
            )?;
            self.opt.observer.histogram(
                HistogramMetric::RecoveryAnalyzeMicros,
                analyze_started.elapsed().as_micros() as u64,
            );
            // txid allocation starts from 1, so zero means no txn record observed in this group
            if cur_oracle != 0 {
                oracle = max(oracle, cur_oracle.saturating_add(1));
            }
        }

        let in_progress: Vec<u64> = self.in_progress_txns.iter().copied().collect();
        for txid in in_progress {
            self.mark_tx_aborted_and_enqueue_clean(store.clone(), txid);
        }

        self.dirty_table
            .retain(|ver, _| self.committed_txns.contains(&ver.txid));

        let recovered =
            !self.dirty_table.is_empty() || !store.context.pending_abort_clean_tasks().is_empty();
        self.opt.observer.gauge(
            GaugeMetric::RecoveryDirtyEntries,
            self.dirty_table.len() as i64,
        );
        self.opt.observer.gauge(GaugeMetric::RecoveryUndoEntries, 0);
        if !self.dirty_table.is_empty() {
            let redo_started = Instant::now();
            let count = self.redo(&mut block, store.clone())?;
            self.opt
                .observer
                .counter(CounterMetric::RecoveryRedoRecord, count);
            self.opt.observer.histogram(
                HistogramMetric::RecoveryRedoMicros,
                redo_started.elapsed().as_micros() as u64,
            );
        }
        log::trace!("oracle {oracle}");
        store.manifest.numerics.oracle.store(oracle, Relaxed);
        store.manifest.numerics.wmk_oldest.store(oracle, Relaxed);
        self.evict_all(store);
        self.opt.observer.histogram(
            HistogramMetric::RecoveryPhase2Micros,
            phase2_started.elapsed().as_micros() as u64,
        );
        self.opt.observer.event(ObserveEvent {
            kind: EventKind::RecoveryPhase2End,
            bucket_id: 0,
            txid: oracle,
            file_id: 0,
            value: recovered as u64,
        });
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
        block: &mut Block,
        store: MutRef<Store>,
    ) -> Result<bool, OpCode> {
        assert!((loc.len as usize) <= block.len());
        f.read(block.mut_slice(0, loc.len as usize), loc.pos.offset)?;

        let u = ptr_to::<WalUpdate>(block.data());

        if !u.is_intact() {
            return Ok(false);
        }

        let ver = Ver::new(u.txid, u.cmd_id);

        debug_assert!(!self.dirty_table.contains_key(&ver));

        // correctness gate: if this WAL record is already covered by bucket durable frontier,
        // it has been materialized in persisted page image and must not enter redo.
        let durable_lsn = store.manifest.durable_frontier_lsn(u.bucket_id, u.group_id);
        if loc.pos <= durable_lsn {
            return Ok(true);
        }

        let raw = u.key();
        let target_tree = self.get_tree(u.bucket_id, store);
        if let Some(target_tree) = &target_tree {
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
        group_id: u8,
        addr: Position,
        oldest_file_id: u64,
        latest_file_id: u64,
        block: &mut Block,
        store: MutRef<Store>,
    ) -> Result<u64, OpCode> {
        let Position { file_id, offset } = addr;
        let mut pos;
        let mut oracle = 0;
        let mut loc = Location {
            group_id: group_id as u32,
            pos: Position::default(),
            len: 0,
        };
        let g = crossbeam_epoch::pin();

        for i in file_id..=latest_file_id {
            let path = self.opt.wal_file(group_id, i);
            if !path.exists() {
                if i < oldest_file_id {
                    continue;
                }
                log::error!(
                    "wal gap detected after recycled prefix, group={group_id} file_id={i} oldest={oldest_file_id} latest={latest_file_id}"
                );
                return Err(OpCode::Corruption);
            }
            let mut f = File::options().read(true).write(true).open(&path)?;
            let end = f.size()?;
            if end == 0 {
                continue;
            }
            static_assert!(size_of::<EntryType>() == 1);

            loc.pos.file_id = i;
            pos = if i == file_id { offset } else { 0 };

            log::trace!("{path:?} pos {pos} end {end}");
            while pos < end {
                let start_pos = pos;
                let hdr = {
                    let hdr = block.mut_slice(0, 1);
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
                f.read(block.mut_slice(0, sz), pos)?;

                pos += sz as u64;
                let ptr = block.data();
                match et {
                    EntryType::Commit => {
                        let a = ptr_to::<WalCommit>(ptr);
                        if !a.is_intact() {
                            pos = start_pos;
                            break;
                        }
                        let txid = { a.txid };
                        log::trace!("{a:?}");
                        self.mark_tx_committed(store.clone(), txid);
                        oracle = max(txid, oracle);
                    }
                    EntryType::Abort => {
                        let a = ptr_to::<WalAbort>(ptr);
                        if !a.is_intact() {
                            pos = start_pos;
                            break;
                        }
                        let txid = { a.txid };
                        log::trace!("{a:?}");
                        self.mark_tx_aborted_and_enqueue_clean(store.clone(), txid);
                        oracle = max(txid, oracle);
                    }
                    EntryType::Begin => {
                        let b = ptr_to::<WalBegin>(ptr);
                        if !b.is_intact() {
                            pos = start_pos;
                            break;
                        }
                        let txid = { b.txid };
                        log::trace!("{b:?}");
                        self.in_progress_txns.insert(txid);
                        self.committed_txns.remove(&txid);
                        self.tx_pin_file
                            .entry(txid)
                            .and_modify(|x| *x = (*x).min(i))
                            .or_insert(i);
                        store.context.mark_tx_in_progress(txid);
                        oracle = max(txid, oracle);
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
                        let payload_len = u.payload_len();
                        if pos + payload_len as u64 > end {
                            pos = start_pos;
                            break;
                        }
                        loc.len = (sz + payload_len) as u32;
                        let txid = u.txid;
                        if block.len() < loc.len as usize {
                            block.realloc(loc.len as usize);
                        }
                        log::trace!("{pos} => update txid={txid}");
                        loc.pos.offset = pos - sz as u64;

                        if !self.handle_update(&g, &mut f, &mut loc, block, store.clone())? {
                            pos = start_pos;
                            break;
                        }
                        oracle = max(txid, oracle);
                        self.tx_last_update.insert(
                            txid,
                            Location {
                                group_id: group_id as u32,
                                pos: loc.pos,
                                len: 0,
                            },
                        );
                        self.tx_pin_file
                            .entry(txid)
                            .and_modify(|x| *x = (*x).min(i))
                            .or_insert(i);

                        if !self.committed_txns.contains(&txid)
                            && self.in_progress_txns.insert(txid)
                        {
                            // txn may have begun before the last checkpoint and therefore has no begin record in this scan range
                            store.context.mark_tx_in_progress(txid);
                        }

                        pos += payload_len as u64;
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
                self.opt
                    .observer
                    .counter(CounterMetric::RecoveryWalTruncate, 1);
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

    fn redo(&mut self, block: &mut Block, store: MutRef<Store>) -> Result<u64, OpCode> {
        let cache = Lru::new();
        let cap = 32;
        let mut applied = 0u64;
        let g = crossbeam_epoch::pin();

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
            let txid = { c.txid };
            if !self.committed_txns.contains(&txid) {
                continue;
            }
            let ok = c.key();
            let key = Key::new(ok, Ver::new(c.txid, c.cmd_id));

            let target_tree = self.get_tree(c.bucket_id, store.clone());
            let Some(target_tree) = target_tree else {
                continue;
            };

            let apply_res = match c.sub_type() {
                PayloadType::Insert => {
                    let i = c.put();
                    let val = Record::normal(c.group_id, i.val());
                    target_tree.put(&g, key, val)
                }
                PayloadType::Update => {
                    let u = c.update();
                    let val = Record::normal(c.group_id, u.new_val());
                    target_tree.put(&g, key, val)
                }
                PayloadType::Delete => {
                    let val = Record::remove(c.group_id);
                    target_tree.put(&g, key, val)
                }
            };
            apply_res?;
            applied += 1;
        }
        Ok(applied)
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
