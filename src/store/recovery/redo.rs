use super::*;

impl Recovery {
    pub(super) fn get_tree(&self, bucket_id: u64, store: MutRef<Store>) -> Option<Tree> {
        if let Some(tree) = self.trees.get(&bucket_id) {
            return Some(tree.clone());
        }

        // it's possible that bucket has been logically removed, we simply skip process wal entry in
        // redo/undo for that bucket
        if store.manifest.catalog.contains(bucket_id) {
            let bucket_ctx = must_ok!(store.manifest.load_bucket_context(bucket_id), "must exist");
            let tree = Tree::new(store.clone(), ROOT_PID, bucket_ctx);
            if let Some((evicted_id, evicted_tree)) =
                self.trees
                    .add_with_evict(Self::BUCKET_CACHE_CAP, bucket_id, tree.clone())
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
            store.manifest.buckets.reclaim_bucket(
                bucket_id,
                crate::map::buffer::ReclaimMode::FinalCheckpoint { force_fsync: true },
            );
            #[cfg(feature = "failpoints")]
            crate::utils::failpoint::crash("mace_recovery_eviction_force_fsync");
        } else {
            store.manifest.buckets.reclaim_bucket(
                bucket_id,
                crate::map::buffer::ReclaimMode::FinalCheckpoint { force_fsync: false },
            );
            #[cfg(feature = "failpoints")]
            crate::utils::failpoint::crash("mace_recovery_eviction_flush");
        }
    }

    pub(super) fn evict_all(&self, store: MutRef<Store>) {
        let mut loaded = self.loaded_buckets.borrow_mut();
        for bucket_id in loaded.drain() {
            self.trees.del(&bucket_id);
            self.evict_bucket(bucket_id, store.clone());
        }
        store
            .manifest
            .buckets
            .unload_all(crate::map::buffer::ReclaimMode::FinalCheckpoint {
                force_fsync: self.plan.rebuild_epoch(),
            });
    }

    pub(super) fn redo(&mut self, block: &mut Block, store: MutRef<Store>) -> Result<u64, OpCode> {
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
}
