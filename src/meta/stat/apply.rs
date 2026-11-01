use btree_store::BTree;
use std::collections::{BTreeMap, HashMap};

use crate::{
    OpCode,
    map::buffer::BucketContext,
    must_true,
    utils::data::{LenSeq, Reloc},
};

use super::{
    FileAccounting, JunkApplyOutcome, MaskPin, MemStat, PersistStat, StatCtx, StatReaders,
    ensure_mask, junk::JunkOwner, junk::JunkPermit, stat_intervals,
};

/// per-call junk classification, retries, and stat updates.
///
/// an applier exists only for a checkpoint/publish caller that owns a
/// [`JunkPermit`]: rewrite transfer reads the collected map through
/// `PublishCut` instead and never reaches this path.
pub(crate) struct JunkApplier<'a> {
    kind: super::FileKind,
    #[cfg_attr(not(feature = "extra_check"), allow(dead_code))]
    opt: &'a crate::utils::options::ParsedOptions,
    accounting: &'a FileAccounting,
    readers: &'a StatReaders,
    junk: &'a super::junk::JunkRegistry,
    ctx: &'a BucketContext,
    btree: &'a BTree,
    is_retired: &'a dyn Fn(u64) -> bool,
}

impl<'a> JunkApplier<'a> {
    pub(crate) fn new(
        stat: &'a StatCtx,
        junk: &'a super::junk::JunkRegistry,
        ctx: &'a BucketContext,
        btree: &'a BTree,
        is_retired: &'a dyn Fn(u64) -> bool,
    ) -> Self {
        Self {
            kind: stat.kind(),
            opt: stat.opt(),
            accounting: stat.accounting(),
            readers: stat.readers(),
            junk,
            ctx,
            btree,
            is_retired,
        }
    }

    /// apply collected addresses to their current owners and return only the
    /// old stats that the publish stage must persist.
    pub(crate) fn apply(
        &self,
        permit: &JunkPermit<'_>,
        tick: u64,
        junks: &[u64],
    ) -> Result<Vec<PersistStat>, OpCode> {
        must_true!(eq permit.kind(), self.kind);
        let mut pending = junks.to_vec();
        let mut updates = BTreeMap::<u64, PersistStat>::new();
        let mut retired = Vec::new();
        let mut relocs = Vec::new();
        while !pending.is_empty() {
            let grouped = self.group_junks_by_owner(&pending);
            let mut retry = Vec::new();
            {
                let mut apply = JunkApply {
                    tick,
                    retry: &mut retry,
                    updates: &mut updates,
                    retired: &mut retired,
                    relocs: &mut relocs,
                };
                for (file_id, addrs) in grouped {
                    if (self.is_retired)(file_id) {
                        continue;
                    }
                    self.apply_owner_group(&mut apply, file_id, &addrs)?;
                }
            }
            pending = retry;
        }
        Ok(updates.into_values().collect())
    }

    /// group addresses by live interval owner
    fn group_junks_by_owner(&self, pending: &[u64]) -> BTreeMap<u64, Vec<u64>> {
        let lk = stat_intervals(self.kind, self.ctx).read();
        let mut grouped = BTreeMap::<u64, Vec<u64>>::new();
        for &addr in pending {
            let Some(file_id) = lk.find(addr) else {
                continue;
            };
            grouped.entry(file_id).or_default().push(addr);
        }
        grouped
    }

    /// apply a group and append post-publication retries.
    ///
    /// a permit can only be held while the producer gate is open, so the
    /// collector can never be observed in `Publishing` here; anything that is
    /// not `Collecting` is the idle ordinary path.
    fn apply_owner_group(
        &self,
        apply: &mut JunkApply<'_>,
        file_id: u64,
        addrs: &[u64],
    ) -> Result<(), OpCode> {
        let mut collector = self.junk.lock(self.kind);
        if collector.is_collecting() {
            self.apply_collecting_group(&mut collector, apply, file_id, addrs)?;
        } else {
            self.apply_idle_group(collector, apply, file_id, addrs)?;
        }
        Ok(())
    }

    /// apply a collecting group: final victims hold the collector; ordinary
    /// handoffs retain ordinary retirement rules.
    fn apply_collecting_group(
        &self,
        collector: &mut super::junk::JunkView<'_>,
        apply: &mut JunkApply<'_>,
        file_id: u64,
        addrs: &[u64],
    ) -> Result<(), OpCode> {
        if collector.owner(file_id) == JunkOwner::Ordinary {
            // ordinary handoff keeps ordinary reclassification; addrs[0] is a
            // representative check because the group's owner state is uniform:
            // process_obsoleted_files removes all of a file's intervals in one
            // write lock (all-or-nothing), an ordinary file's addresses are never
            // copied into a rewrite replacement (it is not a Rewrite victim), and
            // flush output never overlaps retired addresses. a moved owner retries
            // the whole group; a still-owned or gone owner drops the group because
            // ordinary reclaim unconditionally removes the runtime stat, so the
            // junk needs no accounting.
            let owner = stat_intervals(self.kind, self.ctx).read().find(addrs[0]);
            if owner.is_some() && owner != Some(file_id) {
                apply.retry.extend(addrs);
            }
            return Ok(());
        }
        if collector.owner(file_id) == JunkOwner::RewriteVictim {
            return self.apply_final_victim_group(collector, apply, file_id, addrs);
        }
        // unregistered files use ordinary accounting under the collector
        let pin = match ensure_mask(self.accounting, self.readers, self.btree, file_id) {
            Ok(pin) => pin,
            Err(err) => {
                return self.reclassify(apply, file_id, addrs, SameOwner::MaskFailure(err));
            }
        };
        self.apply_ordinary_group_locked(collector, apply, file_id, addrs, pin, false)
    }

    /// final-victim accounting stays under the collector
    fn apply_final_victim_group(
        &self,
        collector: &mut super::junk::JunkView<'_>,
        apply: &mut JunkApply<'_>,
        file_id: u64,
        addrs: &[u64],
    ) -> Result<(), OpCode> {
        let owner = stat_intervals(self.kind, self.ctx).read().find(addrs[0]);
        if owner != Some(file_id) {
            if owner.is_some() {
                apply.retry.extend(addrs);
            }
            return Ok(());
        }
        #[cfg(feature = "extra_check")]
        if self
            .accounting
            .mask_state(file_id)
            .is_some_and(|state| !state.present)
        {
            crate::testing::fire_gc_stat_sync_point(
                match self.kind {
                    crate::meta::FileKind::Data => {
                        crate::testing::GcStatSyncPoint::DataCheckpointRewriteMaskLoad
                    }
                    crate::meta::FileKind::Blob => {
                        crate::testing::GcStatSyncPoint::BlobCheckpointRewriteMaskLoad
                    }
                },
                self.ctx.bucket_id,
                &self.opt.db_root,
            );
        }
        let pin = ensure_mask(self.accounting, self.readers, self.btree, file_id)?;
        self.apply_ordinary_group_locked(collector, apply, file_id, addrs, pin, true)
    }

    /// reacquire the collector before accounting to classify a concurrent rewrite
    fn apply_idle_group(
        &self,
        collector: super::junk::JunkView<'_>,
        apply: &mut JunkApply<'_>,
        file_id: u64,
        addrs: &[u64],
    ) -> Result<(), OpCode> {
        let mask_result = ensure_mask(self.accounting, self.readers, self.btree, file_id);
        drop(collector);
        let pin = match mask_result {
            Ok(pin) => pin,
            Err(err) => {
                return self.reclassify(apply, file_id, addrs, SameOwner::MaskFailure(err));
            }
        };
        let mut collector = self.junk.lock(self.kind);
        self.apply_ordinary_group_locked(&mut collector, apply, file_id, addrs, pin, false)
    }

    /// apply under the collector; final victims treat a missing stat as fatal
    fn apply_ordinary_group_locked(
        &self,
        collector: &mut super::junk::JunkView<'_>,
        apply: &mut JunkApply<'_>,
        file_id: u64,
        addrs: &[u64],
        pin: MaskPin<'_>,
        strict_missing_stat: bool,
    ) -> Result<(), OpCode> {
        if let Some(delta) = self.apply_stat_group_locked(
            collector,
            apply,
            file_id,
            addrs,
            pin,
            strict_missing_stat,
        )? {
            apply.updates.insert(file_id, delta);
        }
        Ok(())
    }

    /// reclassify a failed load against the current interval owner
    fn reclassify(
        &self,
        apply: &mut JunkApply<'_>,
        file_id: u64,
        addrs: &[u64],
        same_owner: SameOwner,
    ) -> Result<(), OpCode> {
        if matches!(same_owner, SameOwner::MaskFailure(_)) && (self.is_retired)(file_id) {
            return Ok(());
        }
        let lk = stat_intervals(self.kind, self.ctx).read();
        for &addr in addrs {
            match lk.find(addr) {
                Some(current) if current == file_id => match same_owner {
                    SameOwner::MaskFailure(OpCode::NotFound) => {}
                    SameOwner::MaskFailure(err) => return Err(err),
                    SameOwner::MissingStat => return Err(OpCode::NotFound),
                },
                Some(_) => apply.retry.push(addr),
                None => {}
            }
        }
        Ok(())
    }

    /// apply a group under the collector after loading its inactive mask.
    /// final victims make a missing stat fatal; moved owners retry.
    fn apply_stat_group_locked(
        &self,
        collector: &mut super::junk::JunkView<'_>,
        apply: &mut JunkApply<'_>,
        file_id: u64,
        addrs: &[u64],
        pin: MaskPin<'_>,
        strict_missing_stat: bool,
    ) -> Result<Option<PersistStat>, OpCode> {
        let owner = collector.owner(file_id);
        if collector.is_collecting() && owner == JunkOwner::Ordinary {
            apply.retry.extend(addrs);
            return Ok(None);
        }
        if self.accounting.stat_inner(file_id).is_none() {
            if strict_missing_stat {
                return Err(OpCode::NotFound);
            }
            self.reclassify(apply, file_id, addrs, SameOwner::MissingStat)?;
            return Ok(None);
        }
        apply.relocs.clear();
        for &addr in addrs {
            if let Some(reloc) = self.readers.find_reloc(file_id, addr) {
                apply.relocs.push((addr, reloc));
            }
        }
        let current_owner = stat_intervals(self.kind, self.ctx).read().find(addrs[0]);
        if current_owner != Some(file_id) {
            if current_owner.is_some() {
                apply.retry.extend(addrs);
            }
            return Ok(None);
        }
        apply.retired.clear();
        let enqueue = collector.is_collecting() && owner == JunkOwner::RewriteVictim;
        let outcome =
            self.accounting
                .apply_junks(pin, file_id, apply.tick, apply.relocs, apply.retired)?;
        match outcome {
            JunkApplyOutcome::Updated { stat } => {
                if enqueue {
                    for addr in apply.retired.drain(..) {
                        if !collector.enqueue(file_id, addr) {
                            return Err(OpCode::NotFound);
                        }
                    }
                } else {
                    apply.retired.clear();
                }
                Ok(stat)
            }
            JunkApplyOutcome::MissingFile => {
                apply.retired.clear();
                if strict_missing_stat {
                    return Err(OpCode::NotFound);
                }
                self.reclassify(apply, file_id, addrs, SameOwner::MissingStat)?;
                Ok(None)
            }
        }
    }
}

/// per-call classification used to decide whether a same-owner failure is
/// ignorable (`NotFound`) or fatal.
enum SameOwner {
    MaskFailure(OpCode),
    MissingStat,
}

struct JunkApply<'a> {
    tick: u64,
    retry: &'a mut Vec<u64>,
    updates: &'a mut BTreeMap<u64, PersistStat>,
    retired: &'a mut Vec<u64>,
    relocs: &'a mut Vec<(u64, Reloc)>,
}

pub(crate) fn persist_full_mask(stat: &MemStat) -> PersistStat {
    let inactive = stat
        .mask
        .as_ref()
        .expect("mask loaded")
        .set_bits(stat.total_elems);
    PersistStat::from_parts(stat.inner, inactive)
}

pub(crate) fn apply_collected_junks(
    fstats: &mut [MemStat],
    output_index: &HashMap<u64, usize>,
    relocs: &HashMap<u64, (u64, LenSeq)>,
    junks: &HashMap<u64, Vec<u64>>,
) {
    for q in junks.values() {
        for &addr in q {
            let Some((file_id, reloc)) = relocs.get(&addr) else {
                continue;
            };
            let Some(&idx) = output_index.get(file_id) else {
                continue;
            };
            let fstat = &mut fstats[idx];
            if fstat.mask.as_ref().expect("mask loaded").test(reloc.seq) {
                continue;
            }
            fstat.active_size -= reloc.active_len() as usize;
            fstat.active_elems -= 1;
            fstat.mask.as_mut().expect("mask loaded").set(reloc.seq);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::{
        Bucket, BucketOptions, Mace, OpCode, Options, RandomPath,
        meta::{FileKind, JunkRegistry, MemStat, StatInner},
        utils::{bitmap::BitMap, data::LenSeq},
    };

    use super::{JunkApplier, apply_collected_junks, persist_full_mask};

    fn test_bucket() -> (RandomPath, Mace, Bucket) {
        let path = RandomPath::tmp();
        let mut opt = Options::new(&*path);
        opt.tmp_store = true;
        opt.concurrent_write = 1;
        opt.gc_eager = false;
        opt.gc_timeout = 60_000;
        let mace = Mace::new(opt.validate().expect("valid options")).expect("open store");
        let bucket = mace
            .new_bucket("junk_apply", BucketOptions::default())
            .expect("create bucket");
        (path, mace, bucket)
    }

    fn apply_fixture_owner(bucket: &Bucket, kind: FileKind, addr: u64, file_id: u64) {
        assert_eq!(kind, FileKind::Data);
        bucket
            .tree
            .bucket
            .data_intervals
            .write()
            .insert(addr, addr, file_id);
    }

    fn mem_stat(file_id: u64) -> MemStat {
        let inner = StatInner {
            file_id,
            up1: 0,
            up2: 0,
            active_elems: 2,
            total_elems: 2,
            active_size: 20,
            total_size: 20,
            bucket_id: 1,
        };
        MemStat::from_parts(inner, Some(BitMap::new(inner.total_elems)))
    }

    #[test]
    fn full_mask_persistence_keeps_prior_inactive_sequences() {
        let inner = StatInner {
            file_id: 7,
            up1: 0,
            up2: 0,
            active_elems: 2,
            total_elems: 4,
            active_size: 20,
            total_size: 40,
            bucket_id: 1,
        };
        let mut mask = BitMap::new(inner.total_elems);
        mask.set(0);
        let mut stat = MemStat::from_parts(inner, Some(mask));
        stat.mask.as_mut().unwrap().set(3);

        assert_eq!(persist_full_mask(&stat).inactive_elems, vec![0, 3]);
    }

    #[test]
    fn collected_rewrite_junk_is_applied_once_to_each_output_stat() {
        let mut fstats = vec![mem_stat(10), mem_stat(11)];
        let output_index = HashMap::from([(10, 0), (11, 1)]);
        let relocs = HashMap::from([
            (100, (10, LenSeq::new(10, 0, 0))),
            (200, (11, LenSeq::new(10, 0, 1))),
        ]);
        let junks = HashMap::from([(3, vec![100, 100]), (5, vec![200, 999])]);

        apply_collected_junks(&mut fstats, &output_index, &relocs, &junks);

        assert_eq!(fstats[0].active_elems, 1);
        assert_eq!(fstats[0].active_size, 10);
        assert_eq!(persist_full_mask(&fstats[0]).inactive_elems, vec![0]);
        assert_eq!(fstats[1].active_elems, 1);
        assert_eq!(fstats[1].active_size, 10);
        assert_eq!(persist_full_mask(&fstats[1]).inactive_elems, vec![1]);
    }

    /// the publish cut is the only input to the output-stat transfer: the
    /// collected map is read through `PublishCut::collected()` and consumed by
    /// `finish_publish`, never re-derived from the collector.
    #[test]
    fn publish_cut_collected_map_feeds_output_stats() {
        let registry = JunkRegistry::new();
        let kind = FileKind::Data;
        let gate = registry.close_gate(kind);
        let registration = registry.start_rewrite(kind, gate, &[3]);
        {
            let mut view = registry.lock(kind);
            assert!(view.enqueue(3, 100));
        }
        let gate = registry.close_gate(kind);
        let cut = registry.begin_publish(kind, registration, gate);

        let mut fstats = vec![mem_stat(10)];
        let output_index = HashMap::from([(10, 0)]);
        let relocs = HashMap::from([(100, (10, LenSeq::new(10, 0, 0)))]);
        apply_collected_junks(&mut fstats, &output_index, &relocs, cut.collected());

        assert_eq!(fstats[0].active_elems, 1);
        assert_eq!(fstats[0].active_size, 10);
        assert_eq!(persist_full_mask(&fstats[0]).inactive_elems, vec![0]);
        JunkRegistry::finish_publish(cut);
    }
    #[test]
    fn junk_apply_final_victim_missing_stat_is_fatal() -> Result<(), OpCode> {
        let (_path, _mace, bucket) = test_bucket();
        let kind = FileKind::Data;
        let file_id = 7;
        let addr = 5000;
        apply_fixture_owner(&bucket, kind, addr, file_id);

        let registry = JunkRegistry::new();
        let gate = registry.close_gate(kind);
        let _registration = registry.start_rewrite(kind, gate, &[file_id]);
        let permits = registry.acquire_permits(&[kind]);
        let permit = permits.permit(kind).expect("permit");
        let manifest = &bucket.inner.store.manifest;
        let live = |_: u64| false;
        let applier = JunkApplier::new(
            &manifest.file_state(kind).stat,
            &registry,
            bucket.tree.bucket.as_ref(),
            &manifest.btree,
            &live,
        );

        let err = match applier.apply(permit, 1, &[addr]) {
            Err(err) => err,
            Ok(_) => panic!("a final rewrite victim must not lose its stat"),
        };
        assert_eq!(err, OpCode::NotFound);
        Ok(())
    }

    #[test]
    fn junk_apply_retired_owner_skips_stat() -> Result<(), OpCode> {
        let (_path, _mace, bucket) = test_bucket();
        let kind = FileKind::Data;
        let file_id = 7;
        let addr = 5000;
        apply_fixture_owner(&bucket, kind, addr, file_id);

        let registry = JunkRegistry::new();
        let gate = registry.close_gate(kind);
        let _registration = registry.start_rewrite(kind, gate, &[file_id]);
        let permits = registry.acquire_permits(&[kind]);
        let permit = permits.permit(kind).expect("permit");
        let manifest = &bucket.inner.store.manifest;

        let retired = |id: u64| id == file_id;
        let applier = JunkApplier::new(
            &manifest.file_state(kind).stat,
            &registry,
            bucket.tree.bucket.as_ref(),
            &manifest.btree,
            &retired,
        );
        assert!(
            applier.apply(permit, 1, &[addr])?.is_empty(),
            "a retired owner must not rebuild its stat"
        );

        // without the retirement classification the missing final-victim stat is fatal
        let live = |_: u64| false;
        let applier = JunkApplier::new(
            &manifest.file_state(kind).stat,
            &registry,
            bucket.tree.bucket.as_ref(),
            &manifest.btree,
            &live,
        );
        let err = match applier.apply(permit, 1, &[addr]) {
            Err(err) => err,
            Ok(_) => panic!("a live final victim must not lose its stat"),
        };
        assert_eq!(err, OpCode::NotFound);
        Ok(())
    }
}
