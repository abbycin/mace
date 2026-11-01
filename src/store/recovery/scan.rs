use super::*;

impl Recovery {
    pub(super) fn mark_aborted(&mut self, store: &Store, txid: u64, oldest_file_id: Option<u64>) {
        let tail = self.last_update.remove(&txid);
        self.in_progress_txns.remove(&txid);
        self.committed_txns.remove(&txid);

        if let Some(tail) = tail {
            let mut pin_file_id = self
                .pin_file
                .remove(&txid)
                .unwrap_or(tail.pos.file_id)
                .min(tail.pos.file_id);
            // a txn may begin before this scan window; its prev chain can still reach files
            // back to the stream's oldest retained file, so the cleanup bounds must allow them
            if let Some(oldest) = oldest_file_id {
                pin_file_id = pin_file_id.min(oldest);
            }
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

    fn get_size(e: EntryType, len: usize) -> Result<Option<usize>, OpCode> {
        let sz = wal_record_sz(e)?;
        if len < sz { Ok(None) } else { Ok(Some(sz)) }
    }

    pub(super) fn validate_update_group(&self, u: &WalUpdate) -> Result<(), OpCode> {
        if u.group_id >= self.plan.source_groups {
            return Err(OpCode::Corruption);
        }
        Ok(())
    }

    /// read and validate one WAL record's framing into the shared block
    pub(super) fn read_wal_record(
        f: &mut File,
        pos: u64,
        end: u64,
        block: &mut Block,
    ) -> Result<Option<(EntryType, usize)>, OpCode> {
        let hdr = {
            let hdr = block.mut_slice(0, 1);
            f.read(hdr, pos)?;
            hdr[0]
        };
        let et: EntryType = match hdr.try_into() {
            Ok(et) => et,
            // leave malformed tails to the common truncate path
            Err(_) => return Ok(None),
        };

        let Some(sz) = Self::get_size(et, (end - pos) as usize)? else {
            return Ok(None);
        };
        must_true!(sz < Self::INIT_BLOCK_SIZE);
        f.read(block.mut_slice(0, sz), pos)?;

        if et != EntryType::Update {
            return Ok(Some((et, sz)));
        }
        if PayloadType::try_from(block.slice::<u8>(1, 1)[0]).is_err() {
            return Ok(None);
        }
        // the record pointer is only valid until the block can realloc
        let payload_size = {
            let u = ptr_to::<WalUpdate>(block.data());
            u.size
        };
        let Ok(payload_len) = WalUpdate::checked_payload_len(payload_size) else {
            return Ok(None);
        };
        let Ok(total) = WalUpdate::checked_encoded_len(payload_len) else {
            return Ok(None);
        };
        let Ok(payload_len_u64) = u64::try_from(payload_len) else {
            return Ok(None);
        };
        let Some(record_end) = pos
            .checked_add(sz as u64)
            .and_then(|x| x.checked_add(payload_len_u64))
        else {
            return Ok(None);
        };
        if record_end > end {
            return Ok(None);
        }
        if block.len() < total {
            block.realloc(total);
        }
        f.read(block.mut_slice(sz, payload_len), pos + sz as u64)?;
        let u = ptr_to::<WalUpdate>(block.data());
        if !u.is_intact() || u.validate_record(block.slice(0, total)).is_err() {
            return Ok(None);
        }
        Ok(Some((et, total)))
    }

    fn handle_update(
        &mut self,
        g: &Guard,
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

    pub(super) fn analyze(
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
                let Some((et, total)) = Self::read_wal_record(&mut f, pos, end, block)? else {
                    pos = start_pos;
                    break;
                };
                pos = match start_pos.checked_add(total as u64) {
                    Some(next) => next,
                    None => {
                        pos = start_pos;
                        break;
                    }
                };
                log::trace!("pos {start_pos} total {total} {et:?}");
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
                        self.mark_aborted(&store, txid, Some(oldest_file_id));
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
                        let (bucket_id, txid, record_group) = {
                            let u = ptr_to::<WalUpdate>(ptr);
                            (u.bucket_id, u.txid, u.group_id)
                        };
                        loc.logical_group_id = record_group as u32;
                        loc.len = match u32::try_from(total) {
                            Ok(len) => len,
                            Err(_) => {
                                pos = start_pos;
                                break;
                            }
                        };
                        loc.bucket_id = bucket_id;
                        log::trace!("{start_pos} => update txid={txid}");
                        loc.pos.offset = start_pos;

                        if !self.handle_update(&g, &mut loc, block, store.clone())? {
                            pos = start_pos;
                            break;
                        }
                        if group_id == Options::SHARED_ID {
                            self.shared_update_groups.insert(record_group as usize);
                        }
                        oracle = max(txid, oracle);
                        self.last_update.insert(
                            txid,
                            Location {
                                bucket_id,
                                physical_wal_id: group_id as u32,
                                logical_group_id: record_group as u32,
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

    pub(super) fn get_file(
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

    pub(super) fn wal_file_range(&self, physical_wal_id: u8) -> Result<Option<(u64, u64)>, OpCode> {
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

    pub(super) fn load_wal_boot(
        &self,
        manifest: Handle<Manifest>,
    ) -> Result<Vec<GroupBoot>, OpCode> {
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

    pub(super) fn load_physical_stream_boot(
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

    /// every physical WAL stream id present in the log namespace (per-group and shared).
    ///
    /// the era wipe must sweep what exists, not only what the current plan can name: a failed
    /// open to a wider target can leave namespace entries above the current bound.
    pub(super) fn present_wal_streams(&self) -> Result<BTreeSet<u8>, OpCode> {
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

    pub(super) fn find_latest_checkpoint(
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
                        // the record pointer is only valid until the block can realloc
                        let payload_size = {
                            let u = ptr_to::<WalUpdate>(ptr);
                            u.size
                        };
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
