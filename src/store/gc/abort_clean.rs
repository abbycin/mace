use crossbeam_epoch::Guard;
use std::collections::HashSet;

use super::GarbageCollector;
#[cfg(feature = "metrics")]
use crate::utils::observe::CounterMetric;
use crate::{
    OpCode,
    cc::{
        context::{AbortCleanState, AbortCleanTask},
        wal::{EntryType, PayloadType, WalBegin, WalCommit, WalUpdate, ptr_to, wal_record_sz},
    },
    index::tree::Tree,
    io::{File, GatherIO},
    utils::{ROOT_PID, block::Block, data::Position, lru::Lru},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AbortCleanLoadMode {
    SteadyState,
    Recovery,
}

struct AbortCleanProgress {
    stabilize_buckets: HashSet<u64>,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct AbortCleanChainBounds {
    pub(super) lower: Position,
    pub(super) upper: Position,
    pub(super) step_budget: usize,
}

impl AbortCleanChainBounds {
    fn contains(&self, pos: Position) -> bool {
        self.lower <= pos && pos <= self.upper
    }
}

pub(super) fn validate_abort_clean_link(
    bounds: AbortCleanChainBounds,
    cursor: Position,
    prev: Position,
) -> Result<Position, OpCode> {
    if !bounds.contains(cursor) || !bounds.contains(prev) || prev >= cursor {
        return Err(OpCode::Corruption);
    }
    Ok(prev)
}

pub(super) fn consume_abort_clean_step(remaining: &mut usize) -> Result<(), OpCode> {
    if *remaining == 0 {
        return Err(OpCode::Corruption);
    }
    *remaining -= 1;
    Ok(())
}

impl GarbageCollector {
    const ABORT_CLEAN_TREE_CACHE_CAP: usize = 64;
    const ABORT_CLEAN_WAL_FILE_CACHE_CAP: usize = 16;

    pub(super) fn run_abort_clean_recovery(&mut self, force_fsync: bool) -> Result<(), OpCode> {
        loop {
            let tasks = self.ctx.abort_clean_tasks();
            if tasks.is_empty() {
                return Ok(());
            }

            let mut block = Block::alloc(1024);
            let trees = Lru::<u64, Option<Tree>>::new();
            let g = crossbeam_epoch::pin();
            let mut round_stabilize_buckets = HashSet::new();
            let mut cleaned_txids = Vec::new();

            for task in tasks {
                match task.state {
                    AbortCleanState::Pending => match self.clean_one_abort_task(
                        &g,
                        task,
                        &trees,
                        &mut block,
                        AbortCleanLoadMode::Recovery,
                    ) {
                        Ok(progress) => {
                            round_stabilize_buckets.extend(progress.stabilize_buckets);
                            cleaned_txids.push(task.txid);
                        }
                        Err(OpCode::Again) => {}
                        Err(e) => return Err(e),
                    },
                    AbortCleanState::WaitingQuiesce => {
                        self.ctx.remove_abort_clean(task.txid);
                    }
                }
            }

            if !round_stabilize_buckets.is_empty() {
                self.stabilize_cleaned_pages(
                    &round_stabilize_buckets,
                    &trees,
                    AbortCleanLoadMode::Recovery,
                    force_fsync,
                )?;
            }

            for txid in cleaned_txids {
                self.ctx.remove_abort_clean(txid);
            }
        }
    }

    pub(super) fn process_abort_clean(&mut self) -> usize {
        let drained_events = self.ctx.drain_abort_clean_events();
        for &txid in &drained_events {
            self.ctx.mark_abort_clean_quiesced(txid);
        }

        let tasks = self.ctx.abort_clean_tasks();
        if tasks.is_empty() {
            let removed = drained_events.len();
            for txid in drained_events {
                self.ctx.remove_abort_clean(txid);
            }
            return removed;
        }

        let mut block = Block::alloc(1024);
        let trees = Lru::<u64, Option<Tree>>::new();
        let g = crossbeam_epoch::pin();
        let mut queued_quiesce = false;
        let mut round_stabilize_buckets = HashSet::new();
        let mut cleaned_txids = Vec::new();
        for task in tasks {
            match task.state {
                AbortCleanState::Pending => {
                    match self.clean_one_abort_task(
                        &g,
                        task,
                        &trees,
                        &mut block,
                        AbortCleanLoadMode::SteadyState,
                    ) {
                        Ok(progress) => {
                            round_stabilize_buckets.extend(progress.stabilize_buckets);
                            cleaned_txids.push(task.txid);
                        }
                        Err(OpCode::Again) => {}
                        Err(e) => {
                            #[cfg(feature = "extra_check")]
                            if e == OpCode::Corruption {
                                crate::testing::fire_abort_clean_sync_point(
                                    crate::testing::AbortCleanSyncPoint::AfterCorruption,
                                    task.txid,
                                );
                            }
                            log::error!("abort clean failed, txid={} error={:?}", task.txid, e);
                        }
                    }
                }
                AbortCleanState::WaitingQuiesce => {
                    if task.quiesced {
                        self.ctx.remove_abort_clean(task.txid);
                    }
                }
            }
        }

        let checkpoint_ok = if round_stabilize_buckets.is_empty() {
            true
        } else {
            match self.stabilize_cleaned_pages(
                &round_stabilize_buckets,
                &trees,
                AbortCleanLoadMode::SteadyState,
                false,
            ) {
                Ok(()) => true,
                Err(e) => {
                    log::error!(
                        "abort clean checkpoint batch failed, tasks={}, buckets={}, error={:?}",
                        cleaned_txids.len(),
                        round_stabilize_buckets.len(),
                        e
                    );
                    false
                }
            }
        };

        let cleaned = if checkpoint_ok {
            cleaned_txids.len()
        } else {
            0
        };
        if checkpoint_ok {
            let sink = self.ctx.abort_clean_event_sink();
            for txid in cleaned_txids {
                self.ctx.mark_abort_clean_wait_quiesce(txid);
                let sink = sink.clone();
                g.defer(move || {
                    sink.lock().push(txid);
                    #[cfg(feature = "extra_check")]
                    crate::testing::fire_abort_clean_sync_point(
                        crate::testing::AbortCleanSyncPoint::AfterQuiesceCallback,
                        txid,
                    );
                });
                queued_quiesce = true;
            }
        }

        if queued_quiesce {
            g.flush();
        }

        let removed_quiesced = drained_events.len();
        for txid in drained_events {
            self.ctx.remove_abort_clean(txid);
        }
        cleaned + removed_quiesced
    }

    fn stabilize_cleaned_pages(
        &self,
        dirty_buckets: &HashSet<u64>,
        trees: &Lru<u64, Option<Tree>>,
        mode: AbortCleanLoadMode,
        force_fsync: bool,
    ) -> Result<(), OpCode> {
        for &bucket_id in dirty_buckets {
            let tree = match trees.get(&bucket_id) {
                Some(tree) => tree.clone(),
                None => self.get_tree(trees, bucket_id, mode)?,
            };
            if let Some(tree) = tree {
                // a switch wipe requires a durable abort-clean rewrite
                tree.bucket.checkpoint_and_wait(force_fsync);
                #[cfg(feature = "failpoints")]
                if force_fsync {
                    crate::utils::failpoint::crash(
                        "mace_recovery_abort_clean_stabilize_force_fsync",
                    );
                }
                #[cfg(feature = "metrics")]
                self.ctx
                    .opt
                    .observer
                    .counter(CounterMetric::GcAbortCleanCheckpointBucket, 1);
            }
        }
        Ok(())
    }

    fn get_tree(
        &self,
        cache: &Lru<u64, Option<Tree>>,
        bucket_id: u64,
        mode: AbortCleanLoadMode,
    ) -> Result<Option<Tree>, OpCode> {
        if let Some(tree) = cache.get(&bucket_id) {
            return Ok(tree.clone());
        }

        let bucket_missing = !self.store.manifest.catalog.contains(bucket_id);
        let unloaded_in_steady_state = mode == AbortCleanLoadMode::SteadyState
            && !self.store.manifest.buckets.contains_context(bucket_id);
        let tree = if bucket_missing || unloaded_in_steady_state {
            None
        } else {
            match self.store.manifest.load_bucket_context(bucket_id) {
                Ok(ctx) => Some(Tree::new(self.store.clone(), ROOT_PID, ctx)),
                Err(OpCode::NotFound) => None,
                Err(e) => return Err(e),
            }
        };
        cache.add(Self::ABORT_CLEAN_TREE_CACHE_CAP, bucket_id, tree.clone());
        Ok(tree)
    }

    /// read and validate one abort-chain record into the shared block
    fn read_abort_clean_record(
        &self,
        task: &AbortCleanTask,
        cursor: Position,
        block: &mut Block,
        wal_files: &Lru<u64, (File, u64)>,
    ) -> Result<EntryType, OpCode> {
        self.cache_abort_clean_wal_file(task, cursor.file_id, wal_files)?;
        let cache_guard = wal_files.get(&cursor.file_id).ok_or(OpCode::Corruption)?;
        let (file, end) = (&cache_guard.0, cache_guard.1);
        if cursor.offset >= end {
            return Err(OpCode::Corruption);
        }

        let header = block.mut_slice(0, 1);
        file.read(header, cursor.offset)?;
        let et: EntryType = header[0].try_into()?;
        let sz = wal_record_sz(et)?;
        let header_end = cursor
            .offset
            .checked_add(u64::try_from(sz).map_err(|_| OpCode::Corruption)?)
            .ok_or(OpCode::Corruption)?;
        if header_end > end {
            return Err(OpCode::Corruption);
        }
        if block.len() < sz {
            block.realloc(sz);
        }
        file.read(block.mut_slice(0, sz), cursor.offset)?;

        if et == EntryType::Update {
            PayloadType::try_from(block.slice::<u8>(1, 1)[0])?;
            let u = ptr_to::<WalUpdate>(block.data());
            let payload_size = { u.size };
            let payload_len = WalUpdate::checked_payload_len(payload_size)?;
            let total = WalUpdate::checked_encoded_len(payload_len)?;
            let record_end = cursor
                .offset
                .checked_add(u64::try_from(total).map_err(|_| OpCode::Corruption)?)
                .ok_or(OpCode::Corruption)?;
            if record_end > end {
                return Err(OpCode::Corruption);
            }
            if block.len() < total {
                block.realloc(total);
            }
            file.read(block.mut_slice(sz, payload_len), header_end)?;
            let u = ptr_to::<WalUpdate>(block.data());
            if !u.is_intact() {
                return Err(OpCode::Corruption);
            }
            u.validate_record(block.slice(0, total))?;
        }
        Ok(et)
    }

    /// advance the chain after validating the record's previous link
    fn advance_abort_clean_cursor(
        bounds: AbortCleanChainBounds,
        cursor: Position,
        next: Position,
    ) -> Result<Position, OpCode> {
        validate_abort_clean_link(bounds, cursor, next)
    }

    /// remove one aborted update from its bucket and advance the chain
    #[allow(clippy::too_many_arguments)]
    fn clean_abort_clean_update(
        &self,
        g: &Guard,
        task: &AbortCleanTask,
        trees: &Lru<u64, Option<Tree>>,
        block: &Block,
        mode: AbortCleanLoadMode,
        bounds: AbortCleanChainBounds,
        cursor: Position,
        next: Position,
        seen_keys: &mut HashSet<(u64, Vec<u8>)>,
        stabilize_buckets: &mut HashSet<u64>,
    ) -> Result<Position, OpCode> {
        let u = ptr_to::<WalUpdate>(block.data());
        let txid = { u.txid };
        if txid != task.txid {
            return Err(OpCode::Corruption);
        }

        let bucket_id = { u.bucket_id };
        if let Some(tree) = self.get_tree(trees, bucket_id, mode)? {
            // a foreground retry may have already removed the aborted head from memory
            // without checkpointing it, so any live bucket touched by this abort chain
            // still needs a durability barrier before we can retire the abort task
            stabilize_buckets.insert(bucket_id);
            let raw = u.key();
            if !seen_keys.insert((bucket_id, raw.to_vec())) {
                return Self::advance_abort_clean_cursor(bounds, cursor, next);
            }
            loop {
                match tree.remove_aborted(g, raw) {
                    Ok(_) => break,
                    Err(OpCode::Again) => g.flush(),
                    Err(e) => return Err(e),
                }
            }
        }
        Self::advance_abort_clean_cursor(bounds, cursor, next)
    }

    fn clean_one_abort_task(
        &self,
        g: &Guard,
        task: AbortCleanTask,
        trees: &Lru<u64, Option<Tree>>,
        block: &mut Block,
        mode: AbortCleanLoadMode,
    ) -> Result<AbortCleanProgress, OpCode> {
        let mut stabilize_buckets = HashSet::new();
        let wal_files = Lru::<u64, (File, u64)>::new();
        let bounds = self.abort_clean_chain_bounds(&task, &wal_files)?;
        let mut remaining_steps = bounds.step_budget;
        let mut cursor = task.tail_lsn;
        let mut seen_keys = HashSet::<(u64, Vec<u8>)>::new();

        loop {
            consume_abort_clean_step(&mut remaining_steps)?;
            if !bounds.contains(cursor) {
                return Err(OpCode::Corruption);
            }
            let et = self.read_abort_clean_record(&task, cursor, block, &wal_files)?;

            match et {
                EntryType::Update => {
                    let u = ptr_to::<WalUpdate>(block.data());
                    let next = Position {
                        file_id: { u.prev_id },
                        offset: { u.prev_off },
                    };
                    cursor = self.clean_abort_clean_update(
                        g,
                        &task,
                        trees,
                        block,
                        mode,
                        bounds,
                        cursor,
                        next,
                        &mut seen_keys,
                        &mut stabilize_buckets,
                    )?;
                }
                EntryType::Begin => {
                    let b = ptr_to::<WalBegin>(block.data());
                    if !b.is_intact() || { b.txid } != task.txid {
                        return Err(OpCode::Corruption);
                    }
                    return Ok(AbortCleanProgress { stabilize_buckets });
                }
                EntryType::Abort | EntryType::Commit => {
                    let c = ptr_to::<WalCommit>(block.data());
                    if !c.is_intact() || { c.txid } != task.txid {
                        return Err(OpCode::Corruption);
                    }
                    return Ok(AbortCleanProgress { stabilize_buckets });
                }
                _ => return Err(OpCode::Corruption),
            }
        }
    }

    fn abort_clean_chain_bounds(
        &self,
        task: &AbortCleanTask,
        wal_files: &Lru<u64, (File, u64)>,
    ) -> Result<AbortCleanChainBounds, OpCode> {
        if task.pin_file_id > task.tail_lsn.file_id {
            return Err(OpCode::Corruption);
        }

        let min_record_len =
            u64::try_from(wal_record_sz(EntryType::Begin)?).map_err(|_| OpCode::Corruption)?;
        let mut retained_bytes = 0u64;
        let mut file_id = task.pin_file_id;
        loop {
            self.cache_abort_clean_wal_file(task, file_id, wal_files)?;
            let file_end = wal_files
                .get(&file_id)
                .map(|entry| entry.1)
                .ok_or(OpCode::Corruption)?;
            let retained_end = if file_id == task.tail_lsn.file_id {
                let tail_record_end = task
                    .tail_lsn
                    .offset
                    .checked_add(min_record_len)
                    .ok_or(OpCode::Corruption)?;
                file_end.min(tail_record_end)
            } else {
                file_end
            };
            retained_bytes = retained_bytes
                .checked_add(retained_end)
                .ok_or(OpCode::Corruption)?;
            if file_id == task.tail_lsn.file_id {
                break;
            }
            file_id = file_id.checked_add(1).ok_or(OpCode::Corruption)?;
        }

        let step_budget = retained_bytes
            .checked_div(min_record_len)
            .ok_or(OpCode::Corruption)?;
        let step_budget = usize::try_from(step_budget).map_err(|_| OpCode::Corruption)?;
        if step_budget == 0 {
            return Err(OpCode::Corruption);
        }
        Ok(AbortCleanChainBounds {
            lower: Position::new(task.pin_file_id, 0),
            upper: task.tail_lsn,
            step_budget,
        })
    }

    fn cache_abort_clean_wal_file(
        &self,
        task: &AbortCleanTask,
        file_id: u64,
        wal_files: &Lru<u64, (File, u64)>,
    ) -> Result<(), OpCode> {
        if wal_files.get(&file_id).is_some() {
            return Ok(());
        }
        let path = self
            .ctx
            .opt
            .physical_wal_path(task.physical_wal_id, file_id);
        if !self.ctx.opt.fs.try_exists(&path)? {
            return Err(OpCode::Corruption);
        }
        let file = File::options()
            .read(true)
            .open(self.ctx.opt.fs.as_ref(), &path)?;
        let end = file.size()?;
        wal_files.add(Self::ABORT_CLEAN_WAL_FILE_CACHE_CAP, file_id, (file, end));
        #[cfg(feature = "metrics")]
        self.ctx
            .opt
            .observer
            .counter(CounterMetric::GcAbortCleanWalFileOpen, 1);
        Ok(())
    }
}
