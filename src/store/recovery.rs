use crate::{must_ok, must_true};
use std::cell::RefCell;
use std::cmp::max;
use std::collections::{BTreeMap, BTreeSet, HashSet};
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
use crate::meta::{Manifest, Sequences, WalRecycleIntent};
use crate::store::gc::drain_abort_clean_during_recovery;
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
    /// route switches and legacy durable layouts rebuild the WAL epoch
    rebuild_epoch: bool,
    /// epoch start consumed after recovery flushes and eviction
    pending_switch: Option<u64>,
    dirty_table: BTreeMap<Ver, Location>,
    committed_txns: HashSet<u64>,
    in_progress_txns: HashSet<u64>,
    last_update: BTreeMap<u64, Location>,
    pin_file: BTreeMap<u64, u64>,
    /// shared-stream groups with retained updates
    shared_update_groups: BTreeSet<usize>,
    bucket_cache_cap: usize,
    trees: Lru<u64, Tree>,
    loaded_buckets: RefCell<HashSet<u64>>,
}

impl Recovery {
    const INIT_BLOCK_SIZE: usize = 1 << 20;
    const BUCKET_CACHE_CAP: usize = 16;

    pub(crate) fn new(opt: Arc<ParsedOptions>, rebuild_epoch: bool) -> Self {
        Self {
            opt,
            rebuild_epoch,
            pending_switch: None,
            dirty_table: BTreeMap::new(),
            committed_txns: HashSet::new(),
            in_progress_txns: HashSet::new(),
            last_update: BTreeMap::new(),
            pin_file: BTreeMap::new(),
            shared_update_groups: BTreeSet::new(),
            bucket_cache_cap: Self::BUCKET_CACHE_CAP,
            trees: Lru::new(),
            loaded_buckets: RefCell::new(HashSet::new()),
        }
    }

    fn mark_aborted(&mut self, store: &Store, txid: u64) {
        let tail = self.last_update.remove(&txid);
        self.in_progress_txns.remove(&txid);
        self.committed_txns.remove(&txid);

        if let Some(tail) = tail {
            let pin_file_id = self
                .pin_file
                .remove(&txid)
                .unwrap_or(tail.pos.file_id)
                .min(tail.pos.file_id);
            store
                .context
                .group(tail.logical_group_id as usize)
                .recover_retained_abort(txid);
            store.context.enqueue_abort_clean(
                txid,
                tail.bucket_id,
                tail.logical_group_id as u8,
                tail.physical_wal_id as u8,
                tail.pos,
                pin_file_id,
            );
        } else {
            store.context.remove_abort_clean(txid);
            self.pin_file.remove(&txid);
        }
    }

    fn mark_committed(&mut self, store: &Store, txid: u64) {
        self.in_progress_txns.remove(&txid);
        self.committed_txns.insert(txid);
        self.last_update.remove(&txid);
        self.pin_file.remove(&txid);
        store.context.remove_abort_clean(txid);
    }

    fn get_tree(&self, bucket_id: u64, store: MutRef<Store>) -> Option<Tree> {
        if let Some(tree) = self.trees.get(&bucket_id) {
            return Some(tree.clone());
        }

        // it's possible that bucket has been logically removed, we simply skip process wal entry in
        // redo/undo for that bucket
        if let Some(meta) = store
            .manifest
            .bucket_metas_by_id
            .get(&bucket_id)
            .map(|m| m.clone())
        {
            must_true!(eq meta.id, bucket_id);
            let bucket_ctx = must_ok!(store.manifest.load_bucket_context(bucket_id), "must exist");
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
        if self.pending_switch.is_some() {
            // on a switch open the wipe deletes the wal that still
            // guards the recovery-redone tail, so every eviction flush (not
            // only the forced checkpoint over still-loaded buckets) must be
            // full-fsync. fdatasync-only leaves file-size metadata un-durable
            // and the tail can truncate on power loss once the wal is gone.
            store.manifest.buckets.del_bucket(bucket_id, true);
            #[cfg(feature = "failpoints")]
            crate::utils::failpoint::crash("mace_recovery_eviction_force_fsync");
        } else {
            store.manifest.buckets.del_bucket(bucket_id, false);
            #[cfg(feature = "failpoints")]
            crate::utils::failpoint::crash("mace_recovery_eviction_flush");
        }
    }

    fn evict_all(&self, store: MutRef<Store>) {
        let mut loaded = self.loaded_buckets.borrow_mut();
        for bucket_id in loaded.drain() {
            self.trees.del(&bucket_id);
            self.evict_bucket(bucket_id, store.clone());
        }
        store.manifest.buckets.unload_all();
    }

    pub(crate) fn abort(&self, store: MutRef<Store>) {
        self.evict_all(store);
    }

    pub(crate) fn phase1(
        &mut self,
        manifest: Handle<Manifest>,
        sequences: Arc<Sequences>,
    ) -> Result<(Vec<GroupBoot>, Handle<Context>), OpCode> {
        self.finish_pending_wal_recycle(manifest)?;
        let mut wal_boot = self.load_wal_boot(manifest)?;
        // seed per-logical-group checkpoint floors from the manifest on
        // EVERY open (independent of the epoch skip) so a same-route reopen
        // does not freeze the shared-stream recycle boundary
        self.seed_checkpoint_floors(manifest, &mut wal_boot);
        if self.rebuild_epoch {
            // compute the global epoch start; the old wal is
            // wiped in phase2 (after the forced checkpoint of the redone tail)
            // and the new era begins at start
            let (high_water, has_history) = self.epoch_high_water(manifest)?;
            let start = if has_history {
                high_water.checked_add(1).ok_or(OpCode::Corruption)?
            } else {
                0
            };
            for boot in wal_boot.iter_mut() {
                boot.start_id = start;
            }
            self.pending_switch = Some(start);
        }
        let ctx = Handle::new(Context::new(self.opt.clone(), sequences, &wal_boot));
        Ok((wal_boot, ctx))
    }

    /// seed logical floors from persisted bucket frontiers
    fn seed_checkpoint_floors(&self, manifest: Handle<Manifest>, wal_boot: &mut [GroupBoot]) {
        let floors = self.manifest_checkpoint_floors(manifest);
        for (i, boot) in wal_boot.iter_mut().enumerate().take(floors.len()) {
            boot.checkpoint_floor = floors[i];
        }
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

        let mut oracle = store.manifest.sequences.oracle.load(Relaxed);
        let mut block = Block::alloc(Self::INIT_BLOCK_SIZE);

        for boot in wal_boot.iter() {
            if !boot.has_files {
                continue;
            }
            // redo correctness depends on rebuilding transaction outcomes and pending abort-clean chains
            // from all retained WAL files, not just latest checkpoint window
            let analyze_started = Instant::now();
            let cur_oracle = self.analyze(
                boot.physical_wal_id,
                boot.scan_start,
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
            self.mark_aborted(&store, txid);
        }

        self.dirty_table
            .retain(|ver, _| self.committed_txns.contains(&ver.txid));

        let recovered =
            !self.dirty_table.is_empty() || !store.context.abort_clean_tasks().is_empty();
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
        if !store.context.abort_clean_tasks().is_empty() {
            drain_abort_clean_during_recovery(
                store.clone(),
                store.context,
                self.pending_switch.is_some(),
            )?;
        }
        // analyze may truncate a writer opened during phase1
        store.context.rebase_logging_positions_to_physical_eof();
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash("mace_recovery_abort_clean_after_drain_before_start");
        log::trace!("oracle {oracle}");
        store.manifest.sequences.oracle.store(oracle, Relaxed);
        // a rebuild must durable-flush every redone bucket before wiping its WAL
        if self.pending_switch.is_some() {
            #[cfg(feature = "failpoints")]
            crate::utils::failpoint::crash("mace_switch_before_forced_checkpoint");
            for bucket_id in self.loaded_buckets.borrow().iter() {
                if let Some(tree) = self.trees.get(bucket_id) {
                    tree.bucket.checkpoint_before_reclaim(true);
                }
            }
        }
        let manifest = store.manifest;
        let context = store.context;
        self.evict_all(store);
        let switching = self.pending_switch.is_some();
        // wipe the old era only after every redone tail is durable
        if switching {
            let start = self
                .pending_switch
                .take()
                .expect("pending switch start must exist");
            self.wipe_old_wal(manifest, wal_boot, start)?;
            // keep the runtime recycle cache aligned with the durable done
            // boundary before the collector can build its first intent
            if let Some(shared) = context.shared_logging() {
                shared.lock().advance_oldest_wal_id(start);
            } else {
                for group in context.groups() {
                    group.logging.lock().advance_oldest_wal_id(start);
                }
            }
            #[cfg(feature = "failpoints")]
            crate::utils::failpoint::crash("mace_switch_after_wipe_before_writeback");
        }
        // publish shared-stream floors before runtime checkpointing or GC
        if self.opt.sync_on_write {
            if switching {
                context.reset_shared_logical_checkpoint_floors();
            } else {
                let floors = self.recovery_logical_checkpoint_floors(manifest);
                context.publish_shared_logical_checkpoint_floors(&floors);
            }
        }
        // runtime roots may use append positions only after recovery finishes
        debug_assert!(context.recovering());
        context.init_safe_exclusive(oracle);
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

    fn validate_update_group(&self, u: &WalUpdate) -> Result<(), OpCode> {
        if u.group_id >= self.opt.concurrent_write {
            return Err(OpCode::Corruption);
        }
        Ok(())
    }

    fn handle_update(
        &mut self,
        g: &Guard,
        f: &mut File,
        loc: &mut Location,
        block: &mut Block,
        store: MutRef<Store>,
    ) -> Result<bool, OpCode> {
        must_true!(
            (loc.len as usize) <= block.len(),
            "loc.len {}, block.len {}",
            loc.len,
            block.len()
        );
        f.read(block.mut_slice(0, loc.len as usize), loc.pos.offset)?;

        if PayloadType::try_from(block.slice::<u8>(1, 1)[0]).is_err() {
            return Ok(false);
        }
        let u = ptr_to::<WalUpdate>(block.data());

        if !u.is_intact() || u.validate_record(block.slice(0, loc.len as usize)).is_err() {
            return Ok(false);
        }

        let ver = Ver::new(u.txid, u.cmd_id);
        self.validate_update_group(u)?;
        if self.dirty_table.contains_key(&ver) {
            return Err(OpCode::Corruption);
        }

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
            bucket_id: 0,
            physical_wal_id: group_id as u32,
            logical_group_id: 0,
            pos: Position::MIN,
            len: 0,
        };
        let g = crossbeam_epoch::pin();

        for i in file_id..=latest_file_id {
            let path = self.opt.physical_wal_path(group_id, i);
            if !self.opt.fs.try_exists(&path)? {
                log::error!(
                    "wal gap detected after recycled prefix, group={group_id} file_id={i} oldest={oldest_file_id} latest={latest_file_id}"
                );
                return Err(OpCode::Corruption);
            }
            let mut f = File::options()
                .read(true)
                .write(true)
                .open(self.opt.fs.as_ref(), &path)?;
            let end = f.size()?;
            if i == file_id && offset > end {
                return Err(OpCode::Corruption);
            }
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
                let et: EntryType = match hdr.try_into() {
                    Ok(et) => et,
                    // leave malformed tails to the common truncate path
                    Err(_) => {
                        pos = start_pos;
                        break;
                    }
                };

                let Some(sz) = Self::get_size(et, (end - pos) as usize)? else {
                    pos = start_pos;
                    break;
                };
                must_true!(sz < Self::INIT_BLOCK_SIZE);

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
                        self.mark_committed(&store, txid);
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
                        self.mark_aborted(&store, txid);
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
                        self.pin_file
                            .entry(txid)
                            .and_modify(|x| *x = (*x).min(i))
                            .or_insert(i);
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
                        if PayloadType::try_from(block.slice::<u8>(1, 1)[0]).is_err() {
                            pos = start_pos;
                            break;
                        }
                        let u = ptr_to::<WalUpdate>(ptr);
                        let payload_size = { u.size };
                        let payload_len = match WalUpdate::checked_payload_len(payload_size) {
                            Ok(len) => len,
                            Err(_) => {
                                pos = start_pos;
                                break;
                            }
                        };
                        let total = match WalUpdate::checked_encoded_len(payload_len) {
                            Ok(total) => total,
                            Err(_) => {
                                pos = start_pos;
                                break;
                            }
                        };
                        let payload_len_u64 = match u64::try_from(payload_len) {
                            Ok(len) => len,
                            Err(_) => {
                                pos = start_pos;
                                break;
                            }
                        };
                        let record_end = match pos.checked_add(payload_len_u64) {
                            Some(end) => end,
                            None => {
                                pos = start_pos;
                                break;
                            }
                        };
                        if record_end > end {
                            pos = start_pos;
                            break;
                        }
                        // copy before possible realloc
                        let bucket_id = u.bucket_id;
                        let txid = u.txid;
                        loc.logical_group_id = u.group_id as u32;
                        loc.len = match u32::try_from(total) {
                            Ok(len) => len,
                            Err(_) => {
                                pos = start_pos;
                                break;
                            }
                        };
                        loc.bucket_id = bucket_id;
                        if block.len() < loc.len as usize {
                            block.realloc(loc.len as usize);
                        }
                        log::trace!("{pos} => update txid={txid}");
                        loc.pos.offset = pos - sz as u64;

                        if !self.handle_update(&g, &mut f, &mut loc, block, store.clone())? {
                            pos = start_pos;
                            break;
                        }
                        if group_id == Options::SHARED_ID {
                            self.shared_update_groups.insert(u.group_id as usize);
                        }
                        oracle = max(txid, oracle);
                        self.last_update.insert(
                            txid,
                            Location {
                                bucket_id,
                                physical_wal_id: group_id as u32,
                                logical_group_id: u.group_id as u32,
                                pos: loc.pos,
                                len: 0,
                            },
                        );
                        self.pin_file
                            .entry(txid)
                            .and_modify(|x| *x = (*x).min(i))
                            .or_insert(i);

                        if !self.committed_txns.contains(&txid) {
                            // txn may have begun before the last checkpoint and therefore has no begin record in this scan range
                            let _ = self.in_progress_txns.insert(txid);
                        }

                        pos = record_end;
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
        physical_wal_id: u32,
        seq: u64,
    ) -> Result<Option<Rc<File>>, OpCode> {
        let id = (physical_wal_id, seq);
        if let Some(f) = cache.get(&id) {
            Ok(Some(f.clone()))
        } else {
            let path = opt.physical_wal_path(physical_wal_id as u8, seq);
            if !opt.fs.try_exists(&path)? {
                return Ok(None);
            }
            let f = Rc::new(File::options().read(true).open(opt.fs.as_ref(), &path)?);
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
            let Location {
                physical_wal_id,
                pos,
                len,
                ..
            } = *table;
            let Some(f) = Self::get_file(&cache, cap, &self.opt, physical_wal_id, pos.file_id)?
            else {
                return Err(OpCode::Corruption);
            };
            must_true!(len as usize <= block.len());
            f.read(block.mut_slice(0, len as usize), pos.offset)?;
            PayloadType::try_from(block.slice::<u8>(1, 1)[0])?;
            let c = ptr_to::<WalUpdate>(block.data());
            if !c.is_intact() || c.validate_record(block.slice(0, len as usize)).is_err() {
                return Err(OpCode::Corruption);
            }
            if c.group_id >= self.opt.concurrent_write {
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

    fn wal_file_range(&self, physical_wal_id: u8) -> Result<Option<(u64, u64)>, OpCode> {
        let mut min_id = u64::MAX;
        let mut max_id = 0;
        let mut found = false;
        let prefix = if physical_wal_id == Options::SHARED_ID {
            format!("{}{}", Options::GROUP_WAL_PREFIX, Options::SEP)
        } else {
            format!(
                "{}{}{}{}",
                Options::WAL_PREFIX,
                Options::SEP,
                physical_wal_id,
                Options::SEP
            )
        };

        for entry in self.opt.fs.read_dir(&self.opt.log_root())? {
            let Some(name) = entry.file_name() else {
                continue;
            };
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

        Ok(if found { Some((min_id, max_id)) } else { None })
    }

    fn load_wal_boot(&self, manifest: Handle<Manifest>) -> Result<Vec<GroupBoot>, OpCode> {
        let mut out = Vec::with_capacity(self.opt.concurrent_write as usize + 1);
        for physical in 0..self.opt.concurrent_write {
            let boot = self.load_physical_stream_boot(manifest, physical)?;
            out.push(boot);
        }
        // inspect the shared stream even on relaxed opens
        let shared = self.load_physical_stream_boot(manifest, Options::SHARED_ID)?;
        out.push(shared);
        Ok(out)
    }

    fn load_physical_stream_boot(
        &self,
        manifest: Handle<Manifest>,
        physical_wal_id: u8,
    ) -> Result<GroupBoot, OpCode> {
        let recycle_state = manifest.load_wal_recycle_state(physical_wal_id);
        if let Some((min_id, max_id)) = self.wal_file_range(physical_wal_id)? {
            // done state defines the contiguous prefix removed by recycle
            let oldest_id = if recycle_state.is_done() {
                recycle_state.oldest_id()
            } else {
                min_id
            };
            let latest_id = max_id.max(oldest_id);
            let oldest_pos = Position {
                file_id: oldest_id,
                offset: 0,
            };
            let scan_start = self
                .find_latest_checkpoint(physical_wal_id, oldest_id, max_id)?
                .unwrap_or(oldest_pos)
                .max(oldest_pos);
            if scan_start.file_id > latest_id {
                return Err(OpCode::Corruption);
            }
            Ok(GroupBoot {
                physical_wal_id,
                oldest_id,
                latest_id,
                scan_start,
                has_files: true,
                start_id: latest_id,
                checkpoint_floor: Position::MIN,
            })
        } else {
            let oldest_id = if recycle_state.is_done() {
                recycle_state.oldest_id()
            } else {
                0
            };
            // start the new shared stream above every persisted frontier
            let start_id = if physical_wal_id == Options::SHARED_ID {
                let mut max_frontier = 0u64;
                for frontier in manifest.bucket_frontier.iter() {
                    for pos in frontier.value().iter() {
                        max_frontier = max_frontier.max(pos.file_id);
                    }
                }
                oldest_id.max(max_frontier.checked_add(1).ok_or(OpCode::Corruption)?)
            } else {
                oldest_id
            };
            Ok(GroupBoot {
                physical_wal_id,
                oldest_id,
                latest_id: oldest_id,
                scan_start: Position {
                    file_id: oldest_id,
                    offset: 0,
                },
                has_files: false,
                start_id,
                checkpoint_floor: Position::MIN,
            })
        }
    }

    /// wipe old physical WAL streams after recovery data is durable
    fn wipe_old_wal(
        &self,
        manifest: Handle<Manifest>,
        wal_boot: &[GroupBoot],
        start: u64,
    ) -> Result<(), OpCode> {
        let mut ordered: Vec<&GroupBoot> = wal_boot.iter().collect();
        ordered.sort_by_key(|boot| {
            (
                boot.physical_wal_id != Options::SHARED_ID,
                boot.physical_wal_id,
            )
        });
        for boot in ordered {
            if boot.oldest_id >= start {
                continue;
            }
            let intent = WalRecycleIntent {
                group_id: boot.physical_wal_id,
                from_file_id: boot.oldest_id,
                to_file_id: start,
            };
            #[cfg(feature = "failpoints")]
            crate::utils::failpoint::crash("mace_wal_recycle_before_intent_commit");
            let Some(intent) = manifest.commit_wal_recycle_intent(intent) else {
                continue;
            };
            Self::remove_wal_prefix(&self.opt, intent)?;
            self.opt.sync_log_dir();
            #[cfg(feature = "failpoints")]
            crate::utils::failpoint::crash("mace_wal_recycle_after_dir_sync_before_done_commit");
            assert!(
                manifest.commit_wal_recycle_done(intent),
                "wal recycle owner must complete its matching intent"
            );
            #[cfg(feature = "failpoints")]
            crate::utils::failpoint::crash("mace_wal_recycle_after_done_commit_before_publish");
        }
        Ok(())
    }

    /// per-group lower bound over persisted bucket frontiers
    fn manifest_checkpoint_floors(&self, manifest: Handle<Manifest>) -> Vec<Position> {
        let n = self.opt.concurrent_write as usize;
        let mut floors = vec![Position::MAX; n];
        let mut seen = [false; Options::MAX_CONCURRENT_WRITE as usize];
        for frontier in manifest.bucket_frontier.iter() {
            for (i, pos) in frontier.value().iter().enumerate().take(n) {
                floors[i] = floors[i].min(*pos);
                seen[i] = true;
            }
        }
        for i in 0..n {
            if !seen[i] {
                floors[i] = Position::MIN;
            }
        }
        floors
    }

    /// shared-stream floors, with inactive groups set to `Position::MAX`
    fn recovery_logical_checkpoint_floors(&self, manifest: Handle<Manifest>) -> Vec<Position> {
        let n = self.opt.concurrent_write as usize;
        let manifest_floors = self.manifest_checkpoint_floors(manifest);
        let mut floors = vec![Position::MAX; n];
        for (i, floor) in manifest_floors.iter().enumerate().take(n) {
            if self.shared_update_groups.contains(&i) {
                floors[i] = *floor;
            }
        }
        floors
    }

    fn epoch_high_water(&self, manifest: Handle<Manifest>) -> Result<(u64, bool), OpCode> {
        let mut high_water = 0u64;
        let mut has_history = false;
        for entry in self.opt.fs.read_dir(&self.opt.log_root())? {
            let Some(name) = entry.file_name() else {
                continue;
            };
            let Some(raw) = name.to_str() else {
                continue;
            };
            let is_wal = raw.starts_with(&format!("{}_", Options::WAL_PREFIX))
                || raw.starts_with(&format!("{}_", Options::GROUP_WAL_PREFIX));
            if is_wal
                && let Some((_, file_id)) = raw.rsplit_once('_')
                && let Ok(file_id) = file_id.parse::<u64>()
            {
                high_water = high_water.max(file_id);
                has_history = true;
            }
        }
        for frontier in manifest.bucket_frontier.iter() {
            for pos in frontier.value().iter() {
                high_water = high_water.max(pos.file_id);
                has_history = true;
            }
        }
        // per-group recycle slots plus the reserved shared-stream slot
        let mut physical_streams: Vec<u8> = (0..self.opt.concurrent_write).collect();
        physical_streams.push(Options::SHARED_ID);
        for group in physical_streams {
            let state = manifest.load_wal_recycle_state(group);
            if !state.is_none() {
                high_water = high_water.max(state.from_file_id).max(state.to_file_id);
                has_history = true;
            }
        }
        Ok((high_water, has_history))
    }

    fn finish_pending_wal_recycle(&self, manifest: Handle<Manifest>) -> Result<(), OpCode> {
        // per-group recycle slots plus the reserved shared-stream slot
        let mut physical_streams: Vec<u8> = (0..self.opt.concurrent_write).collect();
        physical_streams.push(Options::SHARED_ID);
        for group_id in physical_streams {
            let state = manifest.load_wal_recycle_state(group_id);
            if state.is_none() || state.is_done() {
                continue;
            }
            let intent = WalRecycleIntent {
                group_id,
                from_file_id: state.from_file_id,
                to_file_id: state.to_file_id,
            };
            Self::remove_wal_prefix(&self.opt, intent)?;
            self.opt.sync_log_dir();
            #[cfg(feature = "failpoints")]
            crate::utils::failpoint::crash("mace_wal_recycle_after_dir_sync_before_done_commit");
            assert!(
                manifest.commit_wal_recycle_done(intent),
                "wal recycle owner must complete its matching intent"
            );
            #[cfg(feature = "failpoints")]
            crate::utils::failpoint::crash("mace_wal_recycle_after_done_commit_before_publish");
        }
        Ok(())
    }

    fn remove_wal_prefix(opt: &ParsedOptions, intent: WalRecycleIntent) -> Result<(), OpCode> {
        let prefix = if intent.group_id == Options::SHARED_ID {
            format!("{}{}", Options::GROUP_WAL_PREFIX, Options::SEP)
        } else {
            format!(
                "{}{}{}{}",
                Options::WAL_PREFIX,
                Options::SEP,
                intent.group_id,
                Options::SEP
            )
        };
        for path in opt.fs.read_dir(&opt.log_root())? {
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            let Some(raw_id) = name.strip_prefix(&prefix) else {
                continue;
            };
            let Ok(seq) = raw_id.parse::<u64>() else {
                continue;
            };
            if seq < intent.from_file_id || seq >= intent.to_file_id {
                continue;
            }
            opt.fs.remove_file_if_exists(&path)?;
            #[cfg(feature = "failpoints")]
            crate::utils::failpoint::crash("mace_wal_recycle_after_remove_before_dir_sync");
        }
        Ok(())
    }

    fn find_latest_checkpoint(
        &self,
        group_id: u8,
        min_file: u64,
        max_file: u64,
    ) -> Result<Option<Position>, OpCode> {
        let mut block = Block::alloc(Self::INIT_BLOCK_SIZE);
        for file_id in (min_file..=max_file).rev() {
            let path = self.opt.physical_wal_path(group_id, file_id);
            if !self.opt.fs.try_exists(&path)? {
                continue;
            }
            let file = File::options()
                .read(true)
                .write(true)
                .open(self.opt.fs.as_ref(), &path)?;
            let end = file.size()?;
            if end == 0 {
                continue;
            }
            let mut pos = 0;
            let mut latest = None;
            while pos < end {
                let hdr = {
                    let hdr = block.mut_slice(0, 1);
                    file.read(hdr, pos)?;
                    hdr[0]
                };
                let Ok(et) = EntryType::try_from(hdr) else {
                    break;
                };
                let Some(sz) = Self::get_size(et, (end - pos) as usize)? else {
                    break;
                };
                file.read(block.mut_slice(0, sz), pos)?;
                pos += sz as u64;
                let ptr = block.data();
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
                        if PayloadType::try_from(block.slice::<u8>(1, 1)[0]).is_err() {
                            break;
                        }
                        let u = ptr_to::<WalUpdate>(ptr);
                        let payload_size = { u.size };
                        let payload_len = match WalUpdate::checked_payload_len(payload_size) {
                            Ok(len) => len,
                            Err(_) => break,
                        };
                        let total = match WalUpdate::checked_encoded_len(payload_len) {
                            Ok(total) => total,
                            Err(_) => break,
                        };
                        let payload_len_u64 = match u64::try_from(payload_len) {
                            Ok(len) => len,
                            Err(_) => break,
                        };
                        let record_end = match pos.checked_add(payload_len_u64) {
                            Some(candidate) if candidate <= end => candidate,
                            _ => break,
                        };
                        if block.len() < total {
                            block.realloc(total);
                        }
                        file.read(block.mut_slice(sz, payload_len), pos)?;
                        let u = ptr_to::<WalUpdate>(block.data());
                        if !u.is_intact() || u.validate_record(block.slice(0, total)).is_err() {
                            break;
                        }
                        pos = record_end;
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

#[cfg(test)]
mod tests {
    use std::{
        io::ErrorKind,
        rc::Rc,
        sync::{Arc, mpsc::channel},
    };

    use crate::{
        BucketOptions, Mace, OpCode, RandomPath, Store,
        cc::wal::{EntryType, IWalCodec, PayloadType, WalCheckpoint, WalUpdate},
        io::{
            File,
            testfs::{InjectOp, InjectedFileSystem},
        },
        map::{
            SharedState,
            adapter::{ManifestCheckpointObserver, ManifestDataReader},
        },
        meta::WalRecycleIntent,
        meta::builder::ManifestBuilder,
        utils::lru::Lru,
        utils::{
            Handle, MutRef,
            data::{Position, init_group_pos},
        },
    };

    use super::Recovery;
    use crate::utils::options::{Options, ParsedOptions};

    fn new_opt() -> (RandomPath, Arc<InjectedFileSystem>, Arc<ParsedOptions>) {
        let root = RandomPath::tmp();
        let mut opt = Options::new(&*root);
        opt.concurrent_write = 1;
        let fs = Arc::new(InjectedFileSystem::new());
        opt.fs = fs.clone();
        let parsed = Arc::new(opt.validate().expect("recovery options must validate"));
        (root, fs, parsed)
    }

    #[test]
    fn wal_file_range_surfaces_read_dir_error() {
        let (_root, fs, opt) = new_opt();
        fs.fail_once(
            InjectOp::ReadDir,
            opt.log_root(),
            ErrorKind::PermissionDenied,
        );
        let recovery = Recovery::new(opt, false);
        let err = recovery
            .wal_file_range(0)
            .expect_err("wal_file_range must fail");
        assert_eq!(err, OpCode::IoError);
    }

    #[test]
    fn get_file_surfaces_open_error() {
        let (_root, fs, opt) = new_opt();
        let path = opt.wal_file(0, 0);
        std::fs::write(&path, b"x").expect("wal seed write must succeed");
        fs.fail_once(InjectOp::Open, &path, ErrorKind::PermissionDenied);
        let cache = Lru::<(u32, u64), Rc<File>>::new();
        let err = match Recovery::get_file(&cache, 4, &opt, 0, 0) {
            Err(err) => err,
            Ok(_) => panic!("get_file must fail"),
        };
        assert_eq!(err, OpCode::IoError);
    }

    #[test]
    fn recovery_rejects_invalid_update_group() {
        let (_root, _fs, opt) = new_opt();
        let recovery = Recovery::new(opt, false);
        let mut update = WalUpdate {
            wal_type: EntryType::Update,
            sub_type: PayloadType::Delete,
            bucket_id: 0,
            group_id: 1,
            size: 0,
            cmd_id: 7,
            klen: 0,
            txid: 9,
            prev_id: 0,
            prev_off: 0,
            checksum: 0,
        };
        assert_eq!(
            recovery.validate_update_group(&update),
            Err(OpCode::Corruption)
        );
        update.group_id = 0;
        assert_eq!(recovery.validate_update_group(&update), Ok(()));
    }

    #[test]
    fn remove_wal_prefix_surfaces_remove_error() {
        let (_root, fs, opt) = new_opt();
        let path = opt.wal_file(0, 0);
        std::fs::write(&path, b"x").expect("wal seed write must succeed");
        fs.fail_once(InjectOp::RemoveFile, &path, ErrorKind::PermissionDenied);
        let err = Recovery::remove_wal_prefix(
            &opt,
            WalRecycleIntent {
                group_id: 0,
                from_file_id: 0,
                to_file_id: 1,
            },
        )
        .expect_err("remove_wal_prefix must fail");
        assert_eq!(err, OpCode::IoError);
    }

    #[test]
    fn checkpoint_scan_stops_at_invalid_update_checksum() {
        let (_root, _fs, opt) = new_opt();
        let path = opt.wal_file(0, 0);
        let update = WalUpdate {
            wal_type: EntryType::Update,
            sub_type: PayloadType::Delete,
            bucket_id: 0,
            group_id: 0,
            size: 0,
            cmd_id: 0,
            klen: 0,
            txid: 1,
            prev_id: 0,
            prev_off: 0,
            checksum: 0,
        };
        let mut checkpoint = WalCheckpoint {
            wal_type: EntryType::CheckPoint,
            checkpoint: Position::new(0, 0),
            checksum: 0,
        };
        checkpoint.checksum = checkpoint.calc_checksum();

        let mut wal = Vec::new();
        wal.extend_from_slice(update.to_slice());
        wal.extend_from_slice(checkpoint.to_slice());
        std::fs::write(path, wal).expect("wal seed write must succeed");

        let recovery = Recovery::new(opt, false);
        assert_eq!(
            recovery
                .find_latest_checkpoint(0, 0, 0)
                .expect("checkpoint scan must complete"),
            None,
            "a bad update must stop checkpoint discovery before later records"
        );
    }

    #[test]
    fn invalid_update_subtype_is_truncated_when_enabled() {
        let root = RandomPath::tmp();
        let mut options = Options::new(&*root);
        // relaxed route: wal_<group>_<seq> is the active per-group stream, so
        // the malformed seed is analyzed and truncated in place (a durable
        // open would treat a stray per-group file as legacy layout and
        // migrate/wipe it instead)
        options.sync_on_write = false;
        options.concurrent_write = 1;
        let initial = Mace::new(
            options
                .clone()
                .validate()
                .expect("initial open must validate"),
        )
        .expect("initial open must succeed");
        drop(initial);

        let parsed = options
            .clone()
            .validate()
            .expect("recovery options must validate");
        let path = parsed.wal_file(0, 0);
        let update = WalUpdate {
            wal_type: EntryType::Update,
            sub_type: PayloadType::Delete,
            bucket_id: 0,
            group_id: 0,
            size: 0,
            cmd_id: 0,
            klen: 0,
            txid: 1,
            prev_id: 0,
            prev_off: 0,
            checksum: 0,
        };
        let mut raw = update.to_slice().to_vec();
        raw[1] = u8::MAX;
        std::fs::write(&path, raw).expect("wal seed write must succeed");

        let reopened = Mace::new(options.validate().expect("reopen options must validate"));
        assert!(
            reopened.is_ok(),
            "invalid subtype must follow truncate_corrupted_wal"
        );
        drop(reopened);
        assert_eq!(
            std::fs::metadata(path)
                .expect("truncated wal must exist")
                .len(),
            0,
            "invalid subtype must truncate from the bad record start"
        );
    }

    #[test]
    fn invalid_entry_type_is_truncated_when_enabled() {
        let root = RandomPath::tmp();
        let mut options = Options::new(&*root);
        options.sync_on_write = false;
        options.concurrent_write = 1;
        let initial = Mace::new(
            options
                .clone()
                .validate()
                .expect("initial open must validate"),
        )
        .expect("initial open must succeed");
        drop(initial);

        let parsed = options
            .clone()
            .validate()
            .expect("recovery options must validate");
        let path = parsed.wal_file(0, 0);
        // a torn tail whose first byte is not a valid entry type
        std::fs::write(&path, [0xFFu8; 16]).expect("wal seed write must succeed");

        let reopened = Mace::new(options.validate().expect("reopen options must validate"));
        assert!(
            reopened.is_ok(),
            "invalid entry type must follow truncate_corrupted_wal"
        );
        drop(reopened);
        assert_eq!(
            std::fs::metadata(path)
                .expect("truncated wal must exist")
                .len(),
            0,
            "invalid entry type must truncate from the bad record start"
        );
    }

    #[test]
    fn invalid_entry_type_returns_corruption_when_truncate_disabled() {
        let root = RandomPath::tmp();
        let mut options = Options::new(&*root);
        options.sync_on_write = false;
        options.concurrent_write = 1;
        let initial = Mace::new(
            options
                .clone()
                .validate()
                .expect("initial open must validate"),
        )
        .expect("initial open must succeed");
        drop(initial);

        let parsed = options
            .clone()
            .validate()
            .expect("recovery options must validate");
        let path = parsed.wal_file(0, 0);
        std::fs::write(&path, [0xFFu8; 16]).expect("wal seed write must succeed");

        options.truncate_corrupted_wal = false;
        let err = Mace::new(options.validate().expect("reopen options must validate"))
            .err()
            .expect("invalid entry type must surface corruption");
        assert_eq!(err, OpCode::Corruption);
        assert_eq!(
            std::fs::metadata(path)
                .expect("corrupted wal must stay intact")
                .len(),
            16,
            "truncate disabled must leave the wal untouched"
        );
    }

    #[test]
    fn oversized_update_header_is_truncated_when_enabled() {
        let root = RandomPath::tmp();
        let mut options = Options::new(&*root);
        options.sync_on_write = false;
        options.concurrent_write = 1;
        let initial = Mace::new(
            options
                .clone()
                .validate()
                .expect("initial open must validate"),
        )
        .expect("initial open must succeed");
        drop(initial);

        let parsed = options
            .clone()
            .validate()
            .expect("recovery options must validate");
        let path = parsed.wal_file(0, 0);
        let update = WalUpdate {
            wal_type: EntryType::Update,
            sub_type: PayloadType::Delete,
            bucket_id: 0,
            group_id: 0,
            size: u32::MAX,
            cmd_id: 0,
            klen: 0,
            txid: 1,
            prev_id: 0,
            prev_off: 0,
            checksum: 0,
        };
        std::fs::write(&path, update.to_slice()).expect("wal seed write must succeed");

        let reopened = Mace::new(options.validate().expect("reopen options must validate"));
        assert!(
            reopened.is_ok(),
            "oversized header must follow truncate_corrupted_wal"
        );
        drop(reopened);
        assert_eq!(
            std::fs::metadata(path)
                .expect("truncated wal must exist")
                .len(),
            0,
            "oversized header must truncate from the bad record start"
        );
    }

    #[test]
    fn abort_unloads_recovery_loaded_buckets_before_store_abort() {
        let root = RandomPath::tmp();
        let opt = Options::new(&*root);
        let mace = Mace::new(opt.clone().validate().expect("initial open must validate"))
            .expect("initial open must succeed");
        mace.new_bucket("bucket", BucketOptions::default())
            .expect("create bucket must succeed");
        drop(mace);

        let opt = Arc::new(opt.validate().expect("recovery options must validate"));
        let (tx, _erx) = channel();
        let (_etx, rx) = channel();
        let mut builder = ManifestBuilder::new_with_channels(opt.clone(), tx, rx);
        let persisted = builder.load().expect("manifest load must succeed");
        let manifest = Handle::new(builder.finish());
        if let Some(persisted) = persisted {
            manifest
                .store_persisted_options(&persisted)
                .expect("options writeback must succeed");
        }

        let bucket_id = manifest
            .bucket_metas
            .get("bucket")
            .expect("bucket meta must exist")
            .id;
        let mut recovery = Recovery::new(opt.clone(), false);
        let (_wal_boot, ctx) = recovery
            .phase1(manifest, manifest.sequences.clone())
            .expect("phase1 must succeed");
        let observer = Arc::new(ManifestCheckpointObserver::new(manifest, ctx));
        let reader = Arc::new(ManifestDataReader::new(manifest));
        manifest.set_context(ctx, reader, observer);
        let store = MutRef::new(Store::new(opt, manifest, ctx));

        assert!(
            recovery.get_tree(bucket_id, store.clone()).is_some(),
            "recovery must be able to load the persisted bucket"
        );
        assert!(
            !store.manifest.buckets.buckets.is_empty(),
            "loading the tree must publish a bucket context"
        );

        recovery.abort(store.clone());
        assert!(
            recovery.loaded_buckets.borrow().is_empty(),
            "recovery abort must clear the loaded bucket set"
        );
        assert!(
            store.manifest.buckets.buckets.is_empty(),
            "recovery abort must unload bucket contexts before store abort tears down flush/context"
        );

        store.raw_ref().abort();
    }

    #[test]
    fn epoch_high_water_covers_frontier_and_recycle_state_components() {
        let root = RandomPath::tmp();
        let mut opt = Options::new(&*root);
        opt.concurrent_write = 2;
        let parsed = Arc::new(opt.validate().expect("options must validate"));

        // seed wal files below both the frontier and the recycle state
        for seq in 0..6 {
            std::fs::write(parsed.wal_file(0, seq), b"").expect("seed wal file");
        }

        let (tx, _rx) = channel::<SharedState>();
        let (_ack_tx, ack_rx) = channel::<()>();
        let mut builder = ManifestBuilder::new_with_channels(parsed.clone(), tx, ack_rx);
        let _ = builder.load().expect("manifest must load");
        let manifest = Handle::new(builder.finish());

        // each component must be load-bearing: files alone, then the frontier,
        // then the recycle state
        let recovery = Recovery::new(parsed.clone(), false);
        assert_eq!(
            recovery.epoch_high_water(manifest).expect("high water").0,
            5,
            "the wal file scan alone must set the high water"
        );

        let mut frontier = init_group_pos();
        frontier[0] = Position::new(50, 100);
        manifest.merge_bucket_frontier(0, &frontier);
        assert_eq!(
            recovery.epoch_high_water(manifest).expect("high water").0,
            50,
            "the bucket frontier file id must raise the high water"
        );

        assert!(
            manifest
                .commit_wal_recycle_intent(WalRecycleIntent {
                    group_id: 1,
                    from_file_id: 70,
                    to_file_id: 80,
                })
                .is_some()
        );
        let (high, has_history) = recovery
            .epoch_high_water(manifest)
            .expect("high water must compute");
        assert!(has_history, "seeded files must count as history");
        assert_eq!(
            high, 80,
            "epoch high water must cover the recycle state's to_file_id"
        );

        // Handle has no Drop: reclaim the manifest explicitly so the btree and
        // the flush channel are freed before LSan runs
        manifest.reclaim();
    }

    #[test]
    fn stale_recycle_intent_cannot_regress_done_boundary() {
        let root = RandomPath::tmp();
        let mut opt = Options::new(&*root);
        opt.concurrent_write = 1;
        let parsed = Arc::new(opt.validate().expect("options must validate"));
        let (tx, _rx) = channel::<SharedState>();
        let (_ack_tx, ack_rx) = channel::<()>();
        let mut builder = ManifestBuilder::new_with_channels(parsed, tx, ack_rx);
        let _ = builder.load().expect("manifest must load");
        let manifest = Handle::new(builder.finish());

        let intent = manifest
            .commit_wal_recycle_intent(WalRecycleIntent {
                group_id: 0,
                from_file_id: 0,
                to_file_id: 10,
            })
            .expect("initial recycle intent must commit");
        assert!(manifest.commit_wal_recycle_done(intent));
        assert!(
            manifest
                .commit_wal_recycle_intent(WalRecycleIntent {
                    group_id: 0,
                    from_file_id: 0,
                    to_file_id: 7,
                })
                .is_none()
        );
        assert_eq!(manifest.load_wal_recycle_state(0).oldest_id(), 10);

        manifest.reclaim();
    }

    #[test]
    fn wal_recycle_intent_owns_matching_done_transition() {
        let root = RandomPath::tmp();
        let mut opt = Options::new(&*root);
        opt.concurrent_write = 1;
        let parsed = Arc::new(opt.validate().expect("options must validate"));
        let (tx, _rx) = channel::<SharedState>();
        let (_ack_tx, ack_rx) = channel::<()>();
        let mut builder = ManifestBuilder::new_with_channels(parsed, tx, ack_rx);
        let _ = builder.load().expect("manifest must load");
        let manifest = Handle::new(builder.finish());

        let first = manifest
            .commit_wal_recycle_intent(WalRecycleIntent {
                group_id: 0,
                from_file_id: 0,
                to_file_id: 10,
            })
            .expect("first recycle intent must own the stream");
        assert!(
            manifest
                .commit_wal_recycle_intent(WalRecycleIntent {
                    group_id: 0,
                    from_file_id: 0,
                    to_file_id: 20,
                })
                .is_none(),
            "a concurrent collector must not replace an active intent"
        );
        assert!(
            !manifest.commit_wal_recycle_done(WalRecycleIntent {
                group_id: 0,
                from_file_id: 0,
                to_file_id: 20,
            }),
            "only the intent owner may advance the done boundary"
        );
        assert!(manifest.commit_wal_recycle_done(first));
        assert_eq!(manifest.load_wal_recycle_state(0).oldest_id(), 10);

        manifest.reclaim();
    }

    #[test]
    fn shared_bootstrap_rejects_frontier_file_id_overflow() {
        let root = RandomPath::tmp();
        let mut opt = Options::new(&*root);
        opt.concurrent_write = 1;
        let parsed = Arc::new(opt.validate().expect("options must validate"));
        let (tx, _rx) = channel::<SharedState>();
        let (_ack_tx, ack_rx) = channel::<()>();
        let mut builder = ManifestBuilder::new_with_channels(parsed.clone(), tx, ack_rx);
        let _ = builder.load().expect("manifest must load");
        let manifest = Handle::new(builder.finish());

        let mut frontier = init_group_pos();
        frontier[0] = Position::new(u64::MAX, 0);
        manifest.merge_bucket_frontier(0, &frontier);

        let recovery = Recovery::new(parsed, false);
        assert!(matches!(
            recovery.load_physical_stream_boot(manifest, Options::SHARED_ID),
            Err(OpCode::Corruption)
        ));

        manifest.reclaim();
    }

    #[test]
    fn remove_wal_prefix_enumerates_present_files_instead_of_sparse_ids() {
        let (_root, fs, opt) = new_opt();
        let removable = opt.wal_file(0, 7);
        let boundary = opt.wal_file(0, 1_000_000);
        let other_stream = opt.wal_file(1, 3);
        std::fs::write(&removable, b"x").expect("seed removable wal");
        std::fs::write(&boundary, b"x").expect("seed boundary wal");
        std::fs::write(&other_stream, b"x").expect("seed other stream wal");
        let before = fs.calls().len();

        Recovery::remove_wal_prefix(
            &opt,
            WalRecycleIntent {
                group_id: 0,
                from_file_id: 0,
                to_file_id: 1_000_000,
            },
        )
        .expect("sparse recycle must succeed");

        let calls = fs.calls();
        let calls = &calls[before..];
        assert_eq!(
            calls
                .iter()
                .filter(|(op, _)| *op == InjectOp::ReadDir)
                .count(),
            1
        );
        assert_eq!(
            calls
                .iter()
                .filter(|(op, _)| *op == InjectOp::TryExists)
                .count(),
            1,
            "existence probes must scale with matching directory entries, not the million-id range"
        );
        assert!(!removable.exists());
        assert!(boundary.exists());
        assert!(other_stream.exists());
    }
}
