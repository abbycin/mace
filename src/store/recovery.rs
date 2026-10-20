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
use crate::meta::builder::OpenPlan;
use crate::meta::{Manifest, Sequences, WalRecycleIntent};
use crate::store::gc::drain_abort_clean_during_recovery;
use crate::types::data::{Key, Record, Ver};
use crate::utils::block::Block;
use crate::utils::data::Position;
use crate::utils::lru::Lru;
#[cfg(feature = "metrics")]
use crate::utils::observe::{CounterMetric, EventKind, GaugeMetric, HistogramMetric, ObserveEvent};
use crate::utils::options::ParsedOptions;
use crate::utils::{Handle, MutRef, NULL_CMD, NULL_ORACLE, OpCode, ROOT_PID};
use crate::{Options, Store, static_assert};
use crossbeam_epoch::Guard;
#[cfg(feature = "metrics")]
use std::time::Instant;

/// the target-width inputs a runtime Context is constructed from after the era wipe.
///
/// the boot list describes exactly `target_groups` logical streams plus the shared stream at index
/// `target_groups`, and every slot's `start_id`/`oldest_id` must already point at the new era.
/// `checkpoint_floor` carries the post-wipe floor seed `Context::new` publishes: the durable route
/// leaves every logical slot inactive (`Position::MAX`, consumed by the shared GC guard), the
/// relaxed route seeds each logger's own slot with a real position at the era start
pub(crate) struct TargetPlan {
    pub(crate) boots: Vec<GroupBoot>,
}

/// there are some cases can't recover:
/// 1. manifest file missing or corrupted
/// 2. data file lost
///
/// for the last point, if wal file and manifest file are intact, manually recover is possible, we can parse the log and
/// perform necessary redo/undo to bring data back to consistent, and this only apply to the lost of latest data file
pub(crate) struct Recovery {
    opt: Arc<ParsedOptions>,
    /// the single source/target decision record for this open: the retained WAL scan uses the
    /// source width, recycle completion and the high-water cover the durable slot space, and the
    /// era wipe uses `stream_bound()`
    plan: OpenPlan,
    /// epoch start of the era this open rebuilds: read by the era wipe and by the post-wipe target
    /// boot list. A non-rebuild open keeps 0 and never wipes
    epoch_start: u64,
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

    pub(crate) fn new(opt: Arc<ParsedOptions>, plan: OpenPlan) -> Self {
        Self {
            opt,
            plan,
            epoch_start: 0,
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
        if self.plan.rebuild_epoch() {
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

    /// epoch start this open consumes for its era wipe and for the post-wipe target boot list
    pub(crate) fn epoch_start(&self) -> u64 {
        self.epoch_start
    }

    pub(crate) fn phase1(
        &mut self,
        manifest: Handle<Manifest>,
        sequences: Arc<Sequences>,
    ) -> Result<(Vec<GroupBoot>, Handle<Context>), OpCode> {
        // one namespace snapshot for both phase1 consumers: the recycle sweep may delete files
        // before the high-water reads the namespace, and such a slot can only carry a `none` or a
        // `done` state whose boundary is at or below the era start the sweep recorded
        let present = self.present_wal_streams()?;
        self.finish_pending_wal_recycle(manifest, &present)?;
        let mut wal_boot = self.load_wal_boot(manifest)?;
        // seed per-logical-group checkpoint floors from the manifest on
        // EVERY open (independent of the epoch skip) so a same-route reopen
        // does not freeze the shared-stream recycle boundary
        self.seed_checkpoint_floors(manifest, &mut wal_boot);
        if self.plan.rebuild_epoch() {
            // only the epoch start is global. every retained stream keeps its own boot
            // position for the scan: overwriting them with `start` makes a logger open
            // `wal_<id>_<start>` while the stream's retained range still ends lower,
            // and the next open has to read that as a gap inside a retained range.
            // the runtime era writer is installed at `epoch_start` after the wipe
            let (high_water, has_history) = self.epoch_high_water(manifest, &present)?;
            self.epoch_start = if has_history {
                high_water.checked_add(1).ok_or(OpCode::Corruption)?
            } else {
                0
            };
        }
        let ctx = Handle::new(Context::new(
            self.opt.clone(),
            self.plan.source_groups,
            sequences,
            &wal_boot,
        ));
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
        #[cfg(feature = "metrics")]
        let phase2_started = Instant::now();
        #[cfg(feature = "metrics")]
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
            #[cfg(feature = "metrics")]
            let analyze_started = Instant::now();
            let cur_oracle = self.analyze(
                boot.physical_wal_id,
                boot.scan_start,
                boot.oldest_id,
                boot.latest_id,
                &mut block,
                store.clone(),
            )?;
            #[cfg(feature = "metrics")]
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

        #[cfg(feature = "metrics")]
        let recovered =
            !self.dirty_table.is_empty() || !store.context.abort_clean_tasks().is_empty();
        #[cfg(feature = "metrics")]
        {
            self.opt.observer.gauge(
                GaugeMetric::RecoveryDirtyEntries,
                self.dirty_table.len() as i64,
            );
            self.opt.observer.gauge(GaugeMetric::RecoveryUndoEntries, 0);
        }
        if !self.dirty_table.is_empty() {
            #[cfg(feature = "metrics")]
            let redo_started = Instant::now();
            #[cfg_attr(not(feature = "metrics"), allow(unused_variables))]
            let count = self.redo(&mut block, store.clone())?;
            #[cfg(feature = "metrics")]
            {
                self.opt
                    .observer
                    .counter(CounterMetric::RecoveryRedoRecord, count);
                self.opt.observer.histogram(
                    HistogramMetric::RecoveryRedoMicros,
                    redo_started.elapsed().as_micros() as u64,
                );
            }
        }
        if !store.context.abort_clean_tasks().is_empty() {
            drain_abort_clean_during_recovery(
                store.clone(),
                store.context,
                self.plan.rebuild_epoch(),
            )?;
        }
        // analyze may truncate a writer opened during phase1
        store.context.rebase_logging_positions_to_physical_eof();
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash("mace_recovery_abort_clean_after_drain_before_start");
        log::trace!("oracle {oracle}");
        store.manifest.sequences.oracle.store(oracle, Relaxed);
        // a rebuild must durable-flush every redone bucket before wiping its WAL
        if self.plan.rebuild_epoch() {
            #[cfg(feature = "failpoints")]
            crate::utils::failpoint::crash("mace_switch_before_forced_checkpoint");
            for bucket_id in self.loaded_buckets.borrow().iter() {
                if let Some(tree) = self.trees.get(bucket_id) {
                    tree.bucket.checkpoint_before_reclaim(true);
                }
            }
            // the redone tail is durable and the old era is still intact: a crash here must
            // still retry into the same era start and finish the wipe
            #[cfg(feature = "failpoints")]
            crate::utils::failpoint::crash("mace_switch_after_forced_checkpoint_before_wipe");
        }
        let mut manifest = store.manifest;
        let context = store.context;
        self.evict_all(store);
        let switching = self.plan.rebuild_epoch();
        // wipe the old era only after every redone tail is durable
        if switching {
            // the recovery flusher must be gone before any writer is detached: its
            // tasks outlive a `take` (`Checkpoint` has no `Drop`) and a checkpoint
            // record written after the wipe would land in the new era
            if let Some(flush) = manifest.take_checkpoint_flusher() {
                flush.quit();
            }
            let start = self.epoch_start;
            // close the old era's files before removing them: the wipe must not depend
            // on the filesystem accepting the removal of an open file
            context.detach_recovery_writers(start)?;
            #[cfg(feature = "extra_check")]
            context.assert_writers_detached();
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
            if !self.plan.group_resize {
                // a same-width rebuild hands this Context to the runtime, so it owns
                // the new era from here on: install its epoch writers and floors, then
                // make the new files' directory entries durable before the options
                // writeback records the migration
                context.install_epoch_writers(start)?;
                // data-first: the era files' directory entries must be durable before the
                // options writeback records the migration, and a failed sync must leave
                // through the open's cleanup rather than panic mid-handoff
                self.opt.try_sync_log_dir()?;
            }
        }
        // publish shared-stream floors before runtime checkpointing or GC
        if self.opt.sync_on_write {
            if switching {
                // a same-width rebuild seeded the new era's floors through
                // `install_epoch_writers`; a width change retires this Context, so
                // only its shared logger's stale floors are cleared here
                if self.plan.group_resize {
                    context.reset_shared_logical_checkpoint_floors();
                }
            } else {
                let floors = self.recovery_logical_checkpoint_floors(manifest);
                context.publish_shared_logical_checkpoint_floors(&floors);
            }
        }
        // runtime roots may use append positions only after recovery finishes
        debug_assert!(context.recovering());
        context.init_safe_exclusive(oracle);
        #[cfg(feature = "metrics")]
        {
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
        }
        Ok(())
    }

    fn get_size(e: EntryType, len: usize) -> Result<Option<usize>, OpCode> {
        let sz = wal_record_sz(e)?;
        if len < sz { Ok(None) } else { Ok(Some(sz)) }
    }

    fn validate_update_group(&self, u: &WalUpdate) -> Result<(), OpCode> {
        if u.group_id >= self.plan.source_groups {
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
                #[cfg(feature = "metrics")]
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
            if c.group_id >= self.plan.source_groups {
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
            let wal_group = c.group_id;
            let wal_pos = pos;

            let apply_res = match c.sub_type() {
                PayloadType::Insert => {
                    let i = c.put();
                    let val = Record::normal(c.group_id, i.val());
                    target_tree.put(&g, key, val, wal_group, wal_pos)
                }
                PayloadType::Update => {
                    let u = c.update();
                    let val = Record::normal(c.group_id, u.new_val());
                    target_tree.put(&g, key, val, wal_group, wal_pos)
                }
                PayloadType::Delete => {
                    let val = Record::remove(c.group_id);
                    target_tree.put(&g, key, val, wal_group, wal_pos)
                }
                PayloadType::Merge => {
                    // redo inserts the raw operand verbatim; interpretation is deferred to
                    // read/compact paths and never requires a runtime operator here
                    target_tree.bucket.mark_merge();
                    let m = c.put();
                    let val = Record::merge(c.group_id, m.val());
                    target_tree.put(&g, key, val, wal_group, wal_pos)
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
        let mut out = Vec::with_capacity(self.plan.source_groups as usize + 1);
        for physical in 0..self.plan.source_groups {
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

    /// build the target-width boot list and floor seeds from the post-wipe physical state.
    ///
    /// this is the boot-only path the design requires after the wipe: it never reuses the source
    /// boot list and never invents a missing slot. A slot that still reports a pre-era boundary
    /// means the wipe did not cover that stream, which is corruption for this open.
    pub(crate) fn target_plan(
        &self,
        manifest: Handle<Manifest>,
        era_start: u64,
    ) -> Result<TargetPlan, OpCode> {
        let target = self.plan.target_groups;
        let mut boots: Vec<GroupBoot> = Vec::with_capacity(target as usize + 1);
        for id in 0..target {
            boots.push(self.load_physical_stream_boot(manifest, id)?);
        }
        boots.push(self.load_physical_stream_boot(manifest, Options::SHARED_ID)?);
        for (slot, boot) in boots.iter().enumerate() {
            let expected = if slot == target as usize {
                Options::SHARED_ID
            } else {
                slot as u8
            };
            if boot.physical_wal_id != expected
                || boot.start_id < era_start
                || boot.oldest_id < era_start
            {
                return Err(OpCode::Corruption);
            }
        }
        // the post-wipe floor seed: the durable route's shared logger consumes a MAX sentinel for
        // every inactive slot, while the relaxed route needs a real own-slot position (a MAX own
        // floor would build a recycle intent with `to_file_id = u64::MAX`)
        let seed = if self.opt.sync_on_write {
            Position::MAX
        } else {
            Position::new(era_start, 0)
        };
        for boot in boots.iter_mut() {
            boot.checkpoint_floor = seed;
        }
        Ok(TargetPlan { boots })
    }

    /// every physical WAL stream id present in the log namespace (per-group and shared).
    ///
    /// the era wipe must sweep what exists, not only what the current plan can name: a failed
    /// open to a wider target can leave namespace entries above the current bound.
    fn present_wal_streams(&self) -> Result<BTreeSet<u8>, OpCode> {
        let shared_prefix = format!("{}{}", Options::GROUP_WAL_PREFIX, Options::SEP);
        let group_prefix = format!("{}{}", Options::WAL_PREFIX, Options::SEP);
        let mut streams = BTreeSet::new();
        for entry in self.opt.fs.read_dir(&self.opt.log_root())? {
            let Some(raw) = entry.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if raw.starts_with(&shared_prefix) {
                streams.insert(Options::SHARED_ID);
                continue;
            }
            let Some(rest) = raw.strip_prefix(&group_prefix) else {
                continue;
            };
            let Some((group, _seq)) = rest.split_once(Options::SEP) else {
                continue;
            };
            if let Ok(group) = group.parse::<u8>() {
                streams.insert(group);
            }
        }
        Ok(streams)
    }

    /// durable recycle slots may outlive the width that triggered their intent: `[0,
    /// MAX_CONCURRENT_WRITE)` is the whole writable slot space (`Options::validate` clamps the
    /// width to it, the shared stream is `SHARED_ID`, and any other id can only come from a file
    /// name that `present_wal_streams` reports), so a narrower open still completes the intents a
    /// wider, interrupted attempt left behind
    fn durable_recycle_streams(present: &BTreeSet<u8>) -> Vec<u8> {
        let mut streams: Vec<u8> = (0..Options::MAX_CONCURRENT_WRITE).collect();
        streams.extend(present.iter().copied());
        streams.push(Options::SHARED_ID);
        streams.sort_unstable();
        streams.dedup();
        streams
    }

    /// wipe old physical WAL streams after recovery data is durable.
    ///
    /// the stream bound is `OpenPlan::stream_bound()`, not the retained scan width: an
    /// interrupted attempt may have left era files for streams only the target width owns, and
    /// those streams need the same durable `done` boundary before the new era starts. Such
    /// files can never carry pre-migration data (every earlier width change wiped all streams
    /// of the then-persisted width), so recycling them is safe.
    fn wipe_old_wal(
        &self,
        manifest: Handle<Manifest>,
        wal_boot: &[GroupBoot],
        start: u64,
    ) -> Result<(), OpCode> {
        // sweep the union of the OpenPlan stream bound and every stream namespace present on disk: an
        // interrupted attempt may have written era files for a stream id above the current bound
        // (for example an aborted 4 -> 8 grow retried at 4 -> 2), and those files must not be
        // left behind. They carry no pre-migration data (every earlier width change wiped all
        // streams of the then-persisted width), so recycling them is safe.
        let synth = |physical_wal_id: u8| GroupBoot {
            physical_wal_id,
            oldest_id: 0,
            latest_id: 0,
            scan_start: Position::MIN,
            has_files: false,
            start_id: start,
            checkpoint_floor: Position::MIN,
        };
        let mut streams: BTreeSet<u8> = (0..self.plan.stream_bound()).collect();
        streams.extend(self.present_wal_streams()?);
        streams.extend(wal_boot.iter().map(|boot| boot.physical_wal_id));
        let mut ordered: Vec<GroupBoot> = Vec::with_capacity(streams.len());
        for id in streams {
            match wal_boot.iter().find(|boot| boot.physical_wal_id == id) {
                Some(boot) => ordered.push(*boot),
                // target-only stream: the retained scan produced no boot for it
                None => ordered.push(synth(id)),
            }
        }
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
            self.opt.try_sync_log_dir()?;
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
        let n = self.plan.source_groups as usize;
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
        let n = self.plan.source_groups as usize;
        let manifest_floors = self.manifest_checkpoint_floors(manifest);
        let mut floors = vec![Position::MAX; n];
        for (i, floor) in manifest_floors.iter().enumerate().take(n) {
            if self.shared_update_groups.contains(&i) {
                floors[i] = *floor;
            }
        }
        floors
    }

    fn epoch_high_water(
        &self,
        manifest: Handle<Manifest>,
        present: &BTreeSet<u8>,
    ) -> Result<(u64, bool), OpCode> {
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
        for group in Self::durable_recycle_streams(present) {
            let state = manifest.load_wal_recycle_state(group);
            if !state.is_none() {
                high_water = high_water.max(state.from_file_id).max(state.to_file_id);
                has_history = true;
            }
        }
        Ok((high_water, has_history))
    }

    fn finish_pending_wal_recycle(
        &self,
        manifest: Handle<Manifest>,
        present: &BTreeSet<u8>,
    ) -> Result<(), OpCode> {
        // this sweep runs on every open, not only on a rebuild, and each drained intent consumes a
        // hit of both crash hooks below: an `abort@N` targeting a later wipe slot must count them
        for group_id in Self::durable_recycle_streams(present) {
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
            self.opt.try_sync_log_dir()?;
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
        // reached only from the recovery open path (`Recovery::phase1` via
        // `finish_pending_wal_recycle`, `Recovery::phase2` via `wipe_old_wal`), so it can
        // never overlap a checkpoint (which requires an already-open instance) and does
        // not claim a `DeletionGuard` slot. moving era wiping to a runtime path requires
        // claiming a slot via `DeletionGuard::begin` first
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
                        // a checkpoint record whose position is Position::MAX
                        // carries no real floor (a fresh bucket with no
                        // committed writes seeds the logical checkpoint floor
                        // to MAX); treating it as the latest checkpoint would
                        // make the recovery scan start at file u64::MAX and
                        // fail the scan_start <= latest_id gate
                        let ckpt = { c.checkpoint };
                        if ckpt != Position::MAX {
                            latest = Some(ckpt);
                        }
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

    use super::{OpenPlan, Recovery};
    use crate::utils::options::{Options, ParsedOptions};

    /// a no-migration plan: the retained scan and the target runtime use the same width
    fn plan_for(opt: &ParsedOptions) -> OpenPlan {
        OpenPlan::derive(None, opt, false)
    }

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
        let recovery = Recovery::new(opt.clone(), plan_for(&opt));
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
        let recovery = Recovery::new(opt.clone(), plan_for(&opt));
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

        let recovery = Recovery::new(opt.clone(), plan_for(&opt));
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
        let mut builder = ManifestBuilder::new_with_channels(opt.clone(), tx, rx)
            .expect("manifest builder must open");
        let load = builder.load().expect("manifest load must succeed");
        let manifest = Handle::new(builder.finish().expect("manifest finish must succeed"));
        if let Some(persisted) = load.writeback {
            manifest
                .store_persisted_options(&persisted)
                .expect("options writeback must succeed");
        }

        let bucket_id = manifest
            .bucket_metas
            .get("bucket")
            .expect("bucket meta must exist")
            .id;
        let mut recovery = Recovery::new(opt.clone(), plan_for(&opt));
        let (_wal_boot, ctx) = recovery
            .phase1(manifest, manifest.sequences.clone())
            .expect("phase1 must succeed");
        let observer = Arc::new(ManifestCheckpointObserver::new(manifest, ctx));
        let reader = Arc::new(ManifestDataReader::new(manifest));
        manifest
            .set_context(ctx, reader, observer)
            .expect("recovery flusher must start");
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
        let mut builder = ManifestBuilder::new_with_channels(parsed.clone(), tx, ack_rx)
            .expect("manifest builder must open");
        let _ = builder.load().expect("manifest must load");
        let manifest = Handle::new(builder.finish().expect("manifest finish must succeed"));

        // each component must be load-bearing: files alone, then the frontier,
        // then the recycle state
        let recovery = Recovery::new(parsed.clone(), plan_for(&parsed));
        let present = recovery
            .present_wal_streams()
            .expect("namespace snapshot must read");
        assert_eq!(
            recovery
                .epoch_high_water(manifest, &present)
                .expect("high water")
                .0,
            5,
            "the wal file scan alone must set the high water"
        );

        let mut frontier = init_group_pos();
        frontier[0] = Position::new(50, 100);
        manifest.merge_bucket_frontier(0, &frontier);
        assert_eq!(
            recovery
                .epoch_high_water(manifest, &present)
                .expect("high water")
                .0,
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
            .epoch_high_water(manifest, &present)
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
        let mut builder = ManifestBuilder::new_with_channels(parsed, tx, ack_rx)
            .expect("manifest builder must open");
        let _ = builder.load().expect("manifest must load");
        let manifest = Handle::new(builder.finish().expect("manifest finish must succeed"));

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
        let mut builder = ManifestBuilder::new_with_channels(parsed, tx, ack_rx)
            .expect("manifest builder must open");
        let _ = builder.load().expect("manifest must load");
        let manifest = Handle::new(builder.finish().expect("manifest finish must succeed"));

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
        let mut builder = ManifestBuilder::new_with_channels(parsed.clone(), tx, ack_rx)
            .expect("manifest builder must open");
        let _ = builder.load().expect("manifest must load");
        let manifest = Handle::new(builder.finish().expect("manifest finish must succeed"));

        let mut frontier = init_group_pos();
        frontier[0] = Position::new(u64::MAX, 0);
        manifest.merge_bucket_frontier(0, &frontier);

        let recovery = Recovery::new(parsed.clone(), plan_for(&parsed));
        assert!(matches!(
            recovery.load_physical_stream_boot(manifest, Options::SHARED_ID),
            Err(OpCode::Corruption)
        ));

        manifest.reclaim();
    }

    #[test]
    fn era_wipe_advances_done_for_target_only_streams() {
        let root = RandomPath::tmp();
        let mut opt = Options::new(&*root);
        opt.concurrent_write = 2;
        let parsed = Arc::new(opt.validate().expect("options must validate"));
        // a leftover era file for a stream only the target width owns
        std::fs::write(parsed.wal_file(1, 3), b"").expect("seed target-only era file");
        // and a stray namespace entry above the OpenPlan bound, as an aborted wider attempt leaves
        std::fs::write(parsed.wal_file(5, 7), b"").expect("seed stream above the bound");

        let (tx, _rx) = channel::<SharedState>();
        let (_ack_tx, ack_rx) = channel::<()>();
        let mut builder = ManifestBuilder::new_with_channels(parsed.clone(), tx, ack_rx)
            .expect("manifest builder must open");
        let _ = builder.load().expect("manifest must load");
        let manifest = Handle::new(builder.finish().expect("manifest finish must succeed"));

        // persisted width 1, requested width 2: stream 1 is target-only
        let plan = OpenPlan::derive(Some((1, parsed.sync_on_write)), &parsed, false);
        assert_eq!(plan.stream_bound(), 2);
        let recovery = Recovery::new(parsed.clone(), plan);
        let wal_boot = recovery.load_wal_boot(manifest).expect("source boot list");
        assert_eq!(
            wal_boot.len(),
            2,
            "the retained scan stays at the source width"
        );

        let start = 9;
        recovery
            .wipe_old_wal(manifest, &wal_boot, start)
            .expect("era wipe must succeed");

        let state = manifest.load_wal_recycle_state(1);
        assert!(
            state.is_done(),
            "target-only stream must get a done boundary"
        );
        assert_eq!(state.oldest_id(), start);
        assert!(
            !parsed.wal_file(1, 3).exists(),
            "the leftover era file must be recycled"
        );
        assert!(
            !parsed.wal_file(5, 7).exists(),
            "namespace entries above the OpenPlan bound must be swept too"
        );
        assert!(manifest.load_wal_recycle_state(5).is_done());

        manifest.reclaim();
    }

    #[test]
    fn pending_recycle_intent_for_a_target_only_stream_is_completed() {
        let root = RandomPath::tmp();
        let mut opt = Options::new(&*root);
        opt.concurrent_write = 1;
        let parsed = Arc::new(opt.validate().expect("options must validate"));
        std::fs::write(parsed.wal_file(1, 0), b"").expect("seed target-only era file");

        // the open asks for a wider target than the options the Recovery shares, so stream 1 is
        // reachable only through the OpenPlan stream bound
        let mut requested = Options::new(&*root);
        requested.concurrent_write = 2;
        let requested = requested
            .validate()
            .expect("requested options must validate");

        let (tx, _rx) = channel::<SharedState>();
        let (_ack_tx, ack_rx) = channel::<()>();
        let mut builder = ManifestBuilder::new_with_channels(parsed.clone(), tx, ack_rx)
            .expect("manifest builder must open");
        let _ = builder.load().expect("manifest must load");
        let manifest = Handle::new(builder.finish().expect("manifest finish must succeed"));

        // an interrupted attempt left a durable intent on the target-only stream
        assert!(
            manifest
                .commit_wal_recycle_intent(WalRecycleIntent {
                    group_id: 1,
                    from_file_id: 0,
                    to_file_id: 5,
                })
                .is_some(),
            "the interrupted intent must be durable"
        );

        let plan = OpenPlan::derive(Some((1, parsed.sync_on_write)), &requested, false);
        assert_eq!(plan.stream_bound(), 2);
        assert!(plan.stream_bound() > parsed.concurrent_write);
        let recovery = Recovery::new(parsed.clone(), plan);
        let present = recovery
            .present_wal_streams()
            .expect("namespace snapshot must read");
        recovery
            .finish_pending_wal_recycle(manifest, &present)
            .expect("recycle completion must succeed");

        assert!(
            manifest.load_wal_recycle_state(1).is_done(),
            "the target-only stream's pending intent must be completed before the scan"
        );

        manifest.reclaim();
    }

    #[test]
    fn target_plan_covers_target_width_after_the_wipe() {
        let root = RandomPath::tmp();
        let mut opt = Options::new(&*root);
        opt.concurrent_write = 2;
        opt.sync_on_write = false;
        let parsed = Arc::new(opt.validate().expect("options must validate"));
        std::fs::write(parsed.wal_file(1, 3), b"").expect("seed target-only era file");

        let (tx, _rx) = channel::<SharedState>();
        let (_ack_tx, ack_rx) = channel::<()>();
        let mut builder = ManifestBuilder::new_with_channels(parsed.clone(), tx, ack_rx)
            .expect("manifest builder must open");
        let _ = builder.load().expect("manifest must load");
        let manifest = Handle::new(builder.finish().expect("manifest finish must succeed"));

        // persisted width 1, requested width 2: the target plan must cover both streams
        let plan = OpenPlan::derive(Some((1, parsed.sync_on_write)), &parsed, false);
        let recovery = Recovery::new(parsed.clone(), plan);
        let wal_boot = recovery.load_wal_boot(manifest).expect("source boot list");
        let era_start = 9;
        recovery
            .wipe_old_wal(manifest, &wal_boot, era_start)
            .expect("era wipe must succeed");

        let target = recovery
            .target_plan(manifest, era_start)
            .expect("target plan must build after the wipe");
        assert_eq!(
            target.boots.len(),
            3,
            "two logical streams plus the shared slot"
        );
        for (slot, boot) in target.boots.iter().enumerate() {
            let expected = if slot == 2 {
                Options::SHARED_ID
            } else {
                slot as u8
            };
            assert_eq!(boot.physical_wal_id, expected, "slot {slot} stream id");
            assert!(
                boot.start_id >= era_start,
                "slot {slot} start_id must point into the new era"
            );
            assert!(
                boot.oldest_id >= era_start,
                "slot {slot} oldest_id must point into the new era"
            );
        }
        for boot in target.boots.iter().take(2) {
            assert_eq!(
                boot.checkpoint_floor,
                Position::new(era_start, 0),
                "a relaxed target seeds a real own-slot floor instead of Position::MAX"
            );
        }

        manifest.reclaim();
    }

    #[test]
    fn target_plan_rejects_a_state_the_wipe_has_not_covered() {
        let root = RandomPath::tmp();
        let mut opt = Options::new(&*root);
        opt.concurrent_write = 2;
        opt.sync_on_write = false;
        let parsed = Arc::new(opt.validate().expect("options must validate"));
        std::fs::write(parsed.wal_file(1, 3), b"").expect("seed target-only era file");

        let (tx, _rx) = channel::<SharedState>();
        let (_ack_tx, ack_rx) = channel::<()>();
        let mut builder = ManifestBuilder::new_with_channels(parsed.clone(), tx, ack_rx)
            .expect("manifest builder must open");
        let _ = builder.load().expect("manifest must load");
        let manifest = Handle::new(builder.finish().expect("manifest finish must succeed"));

        // make the source-width slots pass on their own, so only the target-only stream can be
        // the reason the plan is rejected
        let era_start = 9;
        for group_id in [0u8, Options::SHARED_ID] {
            let intent = WalRecycleIntent {
                group_id,
                from_file_id: 0,
                to_file_id: era_start,
            };
            let intent = manifest
                .commit_wal_recycle_intent(intent)
                .expect("the boundary intent must be durable");
            assert!(manifest.commit_wal_recycle_done(intent));
        }

        let plan = OpenPlan::derive(Some((1, parsed.sync_on_write)), &parsed, false);
        let recovery = Recovery::new(parsed.clone(), plan);

        // the target-only stream still reports a pre-era boundary, which the plan must reject
        assert!(matches!(
            recovery.target_plan(manifest, era_start),
            Err(OpCode::Corruption)
        ));

        manifest.reclaim();
    }

    #[test]
    fn target_plan_follows_the_target_route_on_a_route_switch() {
        let root = RandomPath::tmp();
        let mut opt = Options::new(&*root);
        opt.concurrent_write = 2;
        opt.sync_on_write = true;
        let parsed = Arc::new(opt.validate().expect("options must validate"));

        let (tx, _rx) = channel::<SharedState>();
        let (_ack_tx, ack_rx) = channel::<()>();
        let mut builder = ManifestBuilder::new_with_channels(parsed.clone(), tx, ack_rx)
            .expect("manifest builder must open");
        let _ = builder.load().expect("manifest must load");
        let manifest = Handle::new(builder.finish().expect("manifest finish must succeed"));

        // persisted relaxed route, requested durable route: the floors must follow the target
        let plan = OpenPlan::derive(Some((1, false)), &parsed, false);
        assert!(plan.route_switch);
        let recovery = Recovery::new(parsed.clone(), plan);
        let wal_boot = recovery.load_wal_boot(manifest).expect("source boot list");
        let era_start = 4;
        recovery
            .wipe_old_wal(manifest, &wal_boot, era_start)
            .expect("era wipe must succeed");

        let target = recovery
            .target_plan(manifest, era_start)
            .expect("target plan must build after the wipe");
        for boot in target.boots.iter() {
            assert_eq!(
                boot.checkpoint_floor,
                Position::MAX,
                "the shared logger consumes a MAX sentinel, not a per-stream own slot"
            );
        }

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
    fn seed_merge_txn(
        path: &std::path::Path,
        txid: u64,
        bucket_id: u64,
        key: &[u8],
        operand: &[u8],
        terminal: EntryType,
    ) {
        assert!(matches!(terminal, EntryType::Commit | EntryType::Abort));
        let mut begin = crate::cc::wal::WalBegin {
            wal_type: EntryType::Begin,
            txid,
            checksum: 0,
        };
        begin.checksum = begin.calc_checksum();

        // payload covers key + length prefix + operand
        let payload_len = key.len() + size_of::<crate::cc::wal::WalMerge>() + operand.len();
        let mut update = WalUpdate {
            wal_type: EntryType::Update,
            sub_type: PayloadType::Merge,
            bucket_id,
            group_id: 0,
            size: payload_len as u32,
            cmd_id: 0,
            klen: key.len() as u32,
            txid,
            prev_id: 0,
            prev_off: 0,
            checksum: 0,
        };
        let mut record = vec![0u8; WalUpdate::size() + payload_len];
        record[..WalUpdate::size()].copy_from_slice(update.to_slice());
        record[WalUpdate::size()..WalUpdate::size() + key.len()].copy_from_slice(key);
        let voff = WalUpdate::size() + key.len();
        record[voff..voff + size_of::<u32>()]
            .copy_from_slice(&(operand.len() as u32).to_le_bytes());
        record[voff + size_of::<u32>()..].copy_from_slice(operand);
        let stored = crate::cc::wal::ptr_to::<WalUpdate>(record.as_ptr());
        update.checksum = stored.calc_checksum();
        record[..WalUpdate::size()].copy_from_slice(update.to_slice());

        let mut out = Vec::new();
        out.extend_from_slice(begin.to_slice());
        out.extend_from_slice(&record);
        if terminal == EntryType::Commit {
            let mut commit = crate::cc::wal::WalCommit {
                wal_type: EntryType::Commit,
                txid,
                checksum: 0,
            };
            commit.checksum = commit.calc_checksum();
            out.extend_from_slice(commit.to_slice());
        } else {
            let mut abort = crate::cc::wal::WalAbort {
                wal_type: EntryType::Abort,
                txid,
                checksum: 0,
            };
            abort.checksum = abort.calc_checksum();
            out.extend_from_slice(abort.to_slice());
        }
        std::fs::write(path, out).expect("merge wal seed write must succeed");
    }

    /// appends many committed merge transactions to one wal file (test seed helper)
    #[cfg_attr(not(feature = "metrics"), allow(dead_code))]
    fn seed_committed_merges(path: &std::path::Path, bucket_id: u64, txns: &[(u64, &[u8], &[u8])]) {
        let mut out = Vec::new();
        for (txid, key, operand) in txns {
            let mut begin = crate::cc::wal::WalBegin {
                wal_type: EntryType::Begin,
                txid: *txid,
                checksum: 0,
            };
            begin.checksum = begin.calc_checksum();
            out.extend_from_slice(begin.to_slice());

            let payload_len = key.len() + size_of::<crate::cc::wal::WalMerge>() + operand.len();
            let mut update = WalUpdate {
                wal_type: EntryType::Update,
                sub_type: PayloadType::Merge,
                bucket_id,
                group_id: 0,
                size: payload_len as u32,
                cmd_id: 0,
                klen: key.len() as u32,
                txid: *txid,
                prev_id: 0,
                prev_off: 0,
                checksum: 0,
            };
            let mut record = vec![0u8; WalUpdate::size() + payload_len];
            record[..WalUpdate::size()].copy_from_slice(update.to_slice());
            record[WalUpdate::size()..WalUpdate::size() + key.len()].copy_from_slice(key);
            let voff = WalUpdate::size() + key.len();
            record[voff..voff + size_of::<u32>()]
                .copy_from_slice(&(operand.len() as u32).to_le_bytes());
            record[voff + size_of::<u32>()..].copy_from_slice(operand);
            let stored = crate::cc::wal::ptr_to::<WalUpdate>(record.as_ptr());
            update.checksum = stored.calc_checksum();
            record[..WalUpdate::size()].copy_from_slice(update.to_slice());
            out.extend_from_slice(&record);

            let mut commit = crate::cc::wal::WalCommit {
                wal_type: EntryType::Commit,
                txid: *txid,
                checksum: 0,
            };
            commit.checksum = commit.calc_checksum();
            out.extend_from_slice(commit.to_slice());
        }
        std::fs::write(path, out).expect("batch merge wal seed must succeed");
    }

    #[test]
    fn committed_merge_redo_replays_raw_operand_without_operator() {
        let root = RandomPath::tmp();
        let mut options = Options::new(&*root);
        options.sync_on_write = false;
        options.concurrent_write = 1;
        let mace = Mace::new(
            options
                .clone()
                .validate()
                .expect("initial open must validate"),
        )
        .expect("initial open must succeed");
        let bucket_id = mace
            .new_bucket("bucket", BucketOptions::default())
            .expect("create bucket must succeed")
            .id();
        drop(mace);

        let parsed = options
            .clone()
            .validate()
            .expect("recovery options must validate");
        seed_merge_txn(
            &parsed.wal_file(0, 0),
            1_000_001,
            bucket_id,
            b"mk",
            b"+7",
            EntryType::Commit,
        );

        let reopened = Mace::new(parsed).expect("reopen must succeed");
        let db = reopened.open_bucket("bucket").expect("bucket must exist");
        // user-visible read reports Invalid: there is no fold resolver for this read, and the
        // head is a merge operand, not a plain value
        let view = db.view().expect("view must open");
        assert!(matches!(view.get("mk"), Err(OpCode::Invalid)));
        drop(view);

        // physical proof: the redo inserted the raw merge-tagged operand verbatim
        let g = crossbeam_epoch::pin();
        let key = crate::types::data::Key::new(
            "mk".as_bytes(),
            crate::types::data::Ver::new(u64::MAX, u32::MAX),
        );
        let (k, val) = db.tree.get(&g, key).expect("replayed key must exist");
        assert_eq!(k.txid, 1_000_001);
        assert!(val.is_merge());
        assert_eq!(val.slice(), b"+7");
        assert_eq!(val.group_id(), 0);
    }

    #[test]
    fn reopen_after_abort_only_wal_activity_with_max_checkpoint_floor() {
        let root = RandomPath::tmp();
        let mut options = Options::new(&*root);
        // durable mode (shared stream): the fresh bucket's frontier seeds the
        // shared logical checkpoint floor; with no committed writes the floor
        // stays Position::MAX and the checkpoint record carries it
        options.sync_on_write = true;
        options.concurrent_write = 1;
        options.data_file_size = 32 << 10;
        options.wal_file_size = 16 << 10;
        options.max_ckpt_per_txn = 64;
        let mace = Mace::new(
            options
                .clone()
                .validate()
                .expect("initial open must validate"),
        )
        .expect("initial open must succeed");
        let db = mace
            .new_bucket("bucket", BucketOptions::default())
            .expect("create bucket must succeed");

        // the only WAL activity is an unmodified abort (Begin + Abort records)
        let kv = db.begin().expect("begin txn");
        match kv.del("k") {
            Ok(_) => panic!("del of an absent key must report NotFound"),
            Err(OpCode::NotFound) => {}
            Err(e) => panic!("del failed: {e:?}"),
        }
        drop(kv);

        db.checkpoint();
        drop(db);
        drop(mace);

        let reopened = Mace::new(options.clone().validate().expect("reopen options"))
            .expect("reopen must succeed");
        let db = reopened
            .open_bucket("bucket")
            .expect("bucket must exist after reopen");
        let view = db.view().expect("view must open");
        let mut iter = view.seek("k");
        assert!(iter.next().is_none(), "no committed writes means no keys");
    }

    #[test]
    fn aborted_merge_redo_stays_invisible_before_abort_clean() {
        let root = RandomPath::tmp();
        let mut options = Options::new(&*root);
        options.sync_on_write = false;
        options.concurrent_write = 1;
        let mace = Mace::new(
            options
                .clone()
                .validate()
                .expect("initial open must validate"),
        )
        .expect("initial open must succeed");
        let bucket_id = mace
            .new_bucket("bucket", BucketOptions::default())
            .expect("create bucket must succeed")
            .id();
        drop(mace);

        let parsed = options
            .clone()
            .validate()
            .expect("recovery options must validate");
        seed_merge_txn(
            &parsed.wal_file(0, 0),
            1_000_002,
            bucket_id,
            b"mk",
            b"+9",
            EntryType::Abort,
        );

        let reopened = Mace::new(parsed).expect("reopen must succeed");
        let db = reopened.open_bucket("bucket").expect("bucket must exist");
        let view = db.view().expect("view must open");
        // NotFound (not Invalid): the aborted operand was never surfaced as a visible head
        assert!(matches!(view.get("mk"), Err(OpCode::NotFound)));
    }

    #[test]
    fn malformed_merge_wal_is_rejected_as_corruption() {
        let root = RandomPath::tmp();
        let mut options = Options::new(&*root);
        options.sync_on_write = false;
        options.concurrent_write = 1;
        options.truncate_corrupted_wal = false;
        let mace = Mace::new(
            options
                .clone()
                .validate()
                .expect("initial open must validate"),
        )
        .expect("initial open must succeed");
        let bucket_id = mace
            .new_bucket("bucket", BucketOptions::default())
            .expect("create bucket must succeed")
            .id();
        drop(mace);

        let parsed = options
            .clone()
            .validate()
            .expect("recovery options must validate");
        let path = parsed.wal_file(0, 0);
        // empty merge operand fails payload validation
        seed_merge_txn(&path, 5, bucket_id, b"mk", b"", EntryType::Commit);
        let err = match Mace::new(parsed) {
            Ok(_) => panic!("empty operand must be corruption"),
            Err(e) => e,
        };
        assert_eq!(err, OpCode::Corruption);
    }

    #[cfg(feature = "metrics")]
    fn counter_of(
        snap: &crate::utils::observe::ObserveSnapshot,
        m: crate::utils::observe::CounterMetric,
    ) -> u64 {
        snap.counters
            .iter()
            .find(|(k, _)| *k == m)
            .map(|(_, v)| *v)
            .unwrap_or(0)
    }

    #[test]
    #[cfg(feature = "metrics")]
    fn redo_never_splits_or_consolidates_until_recovery_completes() {
        use crate::utils::observe::{CounterMetric, InMemoryObserver};
        use std::sync::Arc;

        let root = RandomPath::tmp();
        let mut options = Options::new(&*root);
        options.sync_on_write = false;
        options.concurrent_write = 1;
        let observer = Arc::new(InMemoryObserver::new(16));
        options.observer = observer.clone();

        let mace = Mace::new(options.clone().validate().expect("initial open")).expect("open");
        let bucket_id = mace
            .new_bucket(
                "b",
                BucketOptions {
                    split_elems: 64,
                    consolidate_threshold: 16,
                    ..BucketOptions::default()
                },
            )
            .expect("create bucket")
            .id();
        // a compact single-leaf baseline well under the split bound
        let db = mace.open_bucket("b").expect("bucket must exist");
        let kv = db.begin().expect("begin");
        for i in 0..30 {
            kv.put(format!("k{i:04}"), vec![b'v'; 8]).unwrap();
        }
        kv.commit().unwrap();
        db.checkpoint_and_wait();
        // baseline after all pre-recovery activity so the assertions below measure
        // strictly what happens during the recovery reopen
        let baseline = observer.snapshot();
        drop(mace);

        // redo payload big enough to exceed both thresholds on that leaf
        let parsed = options.clone().validate().expect("recovery options");
        let mut txns: Vec<(u64, &[u8], &[u8])> = Vec::new();
        for i in 0..40u64 {
            txns.push((1_000_000 + i, b"hot", b"+1"));
        }
        seed_committed_merges(&parsed.wal_file(0, 0), bucket_id, &txns);

        let reopened = Mace::new(parsed).expect("reopen must succeed");
        let counters = observer.snapshot();
        let splits = counter_of(&counters, CounterMetric::TreeNodeSplit)
            - counter_of(&baseline, CounterMetric::TreeNodeSplit);
        let consolidates = counter_of(&counters, CounterMetric::TreeNodeConsolidate)
            - counter_of(&baseline, CounterMetric::TreeNodeConsolidate);
        assert_eq!(splits, 0, "recovery must not split pages");
        assert_eq!(
            consolidates, 0,
            "recovery must not consolidate delta chains"
        );

        // data written by redo is reachable
        let db = reopened.open_bucket("b").unwrap();
        let view = db.view().unwrap();
        assert!(view.get("k0000").is_ok());
        drop(view);

        // structural maintenance resumes only after recovery completes
        let kv = db.begin().unwrap();
        for i in 0..80 {
            kv.put(format!("z{i:04}"), vec![b'w'; 8]).unwrap();
        }
        kv.commit().unwrap();
        let db2 = reopened.open_bucket("b").unwrap();
        db2.checkpoint_and_wait();
        let counters = observer.snapshot();
        let resumed = (counter_of(&counters, CounterMetric::TreeNodeSplit)
            - counter_of(&baseline, CounterMetric::TreeNodeSplit))
            + (counter_of(&counters, CounterMetric::TreeNodeConsolidate)
                - counter_of(&baseline, CounterMetric::TreeNodeConsolidate));
        assert!(
            resumed > 0,
            "structural maintenance must resume post-recovery"
        );
    }

    #[derive(Default)]
    #[cfg_attr(not(feature = "metrics"), allow(dead_code))]
    struct TestAddOp;

    impl crate::MergeOperator for TestAddOp {
        fn combine_operands(&self, _key: &[u8], left: &[u8], right: &[u8]) -> Vec<u8> {
            let decode = |b: &[u8]| u64::from_le_bytes(b.try_into().unwrap());
            (decode(left) + decode(right)).to_le_bytes().to_vec()
        }

        fn apply(&self, _key: &[u8], base: Option<&[u8]>, operand: &[u8]) -> Option<Vec<u8>> {
            let decode = |b: &[u8]| u64::from_le_bytes(b.try_into().unwrap());
            let o = decode(operand);
            let v = match base {
                None => o,
                Some(b) => decode(b) + o,
            };
            Some(v.to_le_bytes().to_vec())
        }
    }

    #[test]
    #[cfg(feature = "metrics")]
    fn raw_preserving_maintenance_keeps_merge_chain_readable() {
        use crate::utils::observe::{CounterMetric, InMemoryObserver};
        use std::sync::Arc;

        let root = RandomPath::tmp();
        let mut options = Options::new(&*root);
        options.sync_on_write = false;
        options.concurrent_write = 1;
        let observer = Arc::new(InMemoryObserver::new(16));
        options.observer = observer.clone();

        // operator-less first open: recovery must not interpret operands
        let mace = Mace::new(options.clone().validate().expect("initial open")).expect("open");
        let bucket_id = mace
            .new_bucket(
                "b",
                BucketOptions {
                    split_elems: 64,
                    consolidate_threshold: 16,
                    ..BucketOptions::default()
                },
            )
            .expect("create bucket")
            .id();
        drop(mace);

        // committed four-version chain on "c050": operands 8,4,2,1 fold to 15.
        // the key lives inside the c-series so every maintained leaf carries a real
        // common prefix (prefix_len > 0), exercising the prescan suffix handling
        let parsed = options.clone().validate().expect("recovery options");
        let merge_key: Vec<u8> = b"c050".to_vec();
        let operands: Vec<[u8; 8]> = vec![
            8u64.to_le_bytes(),
            4u64.to_le_bytes(),
            2u64.to_le_bytes(),
            1u64.to_le_bytes(),
        ];
        let mut txns: Vec<(u64, &[u8], &[u8])> = Vec::new();
        for (i, operand) in operands.iter().enumerate() {
            txns.push((100 + i as u64, &merge_key, operand.as_slice()));
        }
        seed_committed_merges(&parsed.wal_file(0, 0), bucket_id, &txns);
        let reopened = Mace::new(parsed).expect("reopen must succeed");

        // load without an operator first: folding remains unavailable for this context
        let db = reopened.open_bucket("b").unwrap();
        db.checkpoint_and_wait(); // settle redo tail before asserting
        // without an operator the chain cannot be interpreted: reads surface Invalid
        assert!(matches!(
            db.view().unwrap().get(&merge_key),
            Err(OpCode::Invalid)
        ));

        // maintenance may run before the runtime merge operator is registered;
        // this path must preserve the complete merge chain verbatim
        let before_operator_maintenance = observer.snapshot();
        let kv = db.begin().unwrap();
        for i in 0..100 {
            if i == 50 {
                continue;
            }
            kv.put(format!("c{i:03}"), vec![b'x'; 8]).unwrap();
        }
        kv.commit().unwrap();
        db.checkpoint_and_wait();
        let after_operatorless_maintenance = observer.snapshot();
        assert!(
            counter_of(
                &after_operatorless_maintenance,
                CounterMetric::TreeNodeConsolidate
            ) > counter_of(
                &before_operator_maintenance,
                CounterMetric::TreeNodeConsolidate
            ),
            "operator-less maintenance must consolidate the recovered leaf"
        );

        let mut with_op = BucketOptions {
            split_elems: 64,
            consolidate_threshold: 16,
            ..BucketOptions::default()
        };
        with_op.merge_operator = Some(Arc::new(TestAddOp));
        assert!(matches!(
            reopened.open_bucket_with_options("b", with_op.clone()),
            Err(OpCode::Invalid)
        ));
        drop(db);
        reopened.drop_bucket("b").unwrap();
        let db = reopened.open_bucket_with_options("b", with_op).unwrap();

        let folded = |db: &crate::store::store::Bucket| -> u64 {
            let v = db.view().unwrap().get(&merge_key).unwrap();
            u64::from_le_bytes(v.slice().try_into().unwrap())
        };
        assert_eq!(folded(&db), 15, "chain must fold to the design value");

        // long-lived view pins the snapshot across maintenance
        let view_old = db.view().unwrap();

        // phase 1: force another threshold consolidation after reloading with an operator
        let baseline = observer.snapshot();
        let kv = db.begin().unwrap();
        for i in 0..100 {
            kv.put(format!("d{i:03}"), vec![b'x'; 8]).unwrap();
        }
        kv.commit().unwrap();

        let counters = observer.snapshot();
        let consolidates = counter_of(&counters, CounterMetric::TreeNodeConsolidate)
            - counter_of(&baseline, CounterMetric::TreeNodeConsolidate);
        assert_eq!(folded(&db), 15, "chain survives consolidation");
        assert_eq!(
            view_old.get(&merge_key).unwrap().slice(),
            15u64.to_le_bytes()
        );

        // phase 2: same-prefix writes force a second consolidation on a page that
        // already carries prefix_len > 0 — the state where double prefix-stripping
        // in the prescan would corrupt the exempt set
        let kv = db.begin().unwrap();
        for i in 0..100 {
            kv.put(format!("e{i:03}"), vec![b'y'; 8]).unwrap();
        }
        kv.commit().unwrap();

        let counters = observer.snapshot();
        let splits = counter_of(&counters, CounterMetric::TreeNodeSplit)
            - counter_of(&baseline, CounterMetric::TreeNodeSplit);
        assert!(splits + consolidates > 0, "maintenance must have run");
        assert_eq!(folded(&db), 15, "chain survives split");
        assert_eq!(
            view_old.get(&merge_key).unwrap().slice(),
            15u64.to_le_bytes()
        );

        // every written key is still reachable across the maintained leaves
        let view = db.view().unwrap();
        for i in 0..20 {
            assert!(view.get(format!("c{i:03}")).is_ok());
        }
        for i in 0..100 {
            if i == 50 {
                continue;
            }
            assert!(view.get(format!("c{i:03}")).is_ok());
        }
        for i in 0..100 {
            assert!(view.get(format!("d{i:03}")).is_ok());
        }
        for i in 0..100 {
            assert!(view.get(format!("e{i:03}")).is_ok());
        }
    }

    #[test]
    #[cfg(feature = "metrics")]
    fn prefixed_leaf_raw_preserving_survives_consolidation() {
        use crate::utils::observe::{CounterMetric, InMemoryObserver};
        use std::sync::Arc;

        let root = RandomPath::tmp();
        let mut options = Options::new(&*root);
        options.sync_on_write = false;
        options.concurrent_write = 1;
        let observer = Arc::new(InMemoryObserver::new(16));
        options.observer = observer.clone();

        // every key shares the "user_" prefix so any leaf holding them carries
        // prefix_len > 0 after compression — exactly the shape where double
        // prefix-stripping in the prescan breaks raw preservation
        let mace = Mace::new(options.clone().validate().expect("initial open")).expect("open");
        let bucket_id = mace
            .new_bucket(
                "p",
                BucketOptions {
                    consolidate_threshold: 16,
                    ..BucketOptions::default()
                },
            )
            .expect("create bucket")
            .id();
        drop(mace);

        let parsed = options.clone().validate().expect("recovery options");
        // one key, four committed operands (8,4,2,1 -> fold 15): versions three and
        // four land below the safe boundary where unguarded trimming would drop them
        let chain_key = b"user_0007".to_vec();
        let operands: Vec<[u8; 8]> = vec![
            8u64.to_le_bytes(),
            4u64.to_le_bytes(),
            2u64.to_le_bytes(),
            1u64.to_le_bytes(),
        ];
        let mut txns: Vec<(u64, &[u8], &[u8])> = Vec::new();
        for (i, operand) in operands.iter().enumerate() {
            txns.push((100 + i as u64, &chain_key, operand));
        }
        seed_committed_merges(&parsed.wal_file(0, 0), bucket_id, &txns);
        let reopened = Mace::new(parsed).expect("reopen must succeed");

        let db = reopened.open_bucket("p").unwrap();
        db.checkpoint_and_wait();

        let with_op = BucketOptions {
            merge_operator: Some(Arc::new(TestAddOp)),
            ..BucketOptions::default()
        };
        drop(db);
        reopened.drop_bucket("p").unwrap();
        let db = reopened.open_bucket_with_options("p", with_op).unwrap();

        let folded = |db: &crate::store::store::Bucket, key: &str| -> u64 {
            let v = db.view().unwrap().get(key).unwrap();
            u64::from_le_bytes(v.slice().try_into().unwrap())
        };

        let baseline = observer.snapshot();
        let kv = db.begin().unwrap();
        // writes overlap the chain-key range so the consolidated leaf is the one
        // holding the merge rows — the exact page whose prescan must exempt them
        for i in 0..56u64 {
            kv.put(format!("user_{:04}", 9 + i), vec![b'z'; 8]).unwrap();
        }
        kv.commit().unwrap();

        let counters = observer.snapshot();
        let consolidates = counter_of(&counters, CounterMetric::TreeNodeConsolidate)
            - counter_of(&baseline, CounterMetric::TreeNodeConsolidate);
        assert!(consolidates > 0, "consolidation must have run");

        assert_eq!(
            folded(&db, std::str::from_utf8(&chain_key).unwrap()),
            15,
            "the full four-operand chain on a prefixed leaf must survive consolidation"
        );
    }
}
