use super::*;

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

impl Recovery {
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
    pub(super) fn wipe_old_wal(
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
    pub(super) fn manifest_checkpoint_floors(&self, manifest: Handle<Manifest>) -> Vec<Position> {
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
    pub(super) fn recovery_logical_checkpoint_floors(
        &self,
        manifest: Handle<Manifest>,
    ) -> Vec<Position> {
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

    pub(super) fn epoch_high_water(
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

    pub(super) fn finish_pending_wal_recycle(
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

    pub(super) fn remove_wal_prefix(
        opt: &ParsedOptions,
        intent: WalRecycleIntent,
    ) -> Result<(), OpCode> {
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
}
