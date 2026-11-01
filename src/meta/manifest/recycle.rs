use super::*;

impl Manifest {
    pub(crate) fn load_wal_recycle_state(&self, group_id: u8) -> WalRecycleState {
        let key = wal_recycle_key(group_id);
        match self.btree.view(BUCKET_MISC, |txn| txn.get(&key)) {
            Ok(val) => WalRecycleState::decode(&val),
            Err(BTreeError::KeyNotFound) => WalRecycleState::none(group_id),
            Err(err) => panic!("load wal recycle state failed for group {group_id}: {err:?}"),
        }
    }

    pub(crate) fn commit_wal_recycle_intent(
        &self,
        intent: WalRecycleIntent,
    ) -> Option<WalRecycleIntent> {
        let key = wal_recycle_key(intent.group_id);
        self.btree
            .exec(BUCKET_MISC, |txn| {
                let state = match txn.get(&key) {
                    Ok(raw) => WalRecycleState::decode(&raw),
                    Err(BTreeError::KeyNotFound) => WalRecycleState::none(intent.group_id),
                    Err(err) => return Err(err),
                };
                // an intent serializes recycle completion
                if state.stage == WalRecycleState::STAGE_INTENT {
                    return Ok(None);
                }
                let from_file_id = intent.from_file_id.max(state.oldest_id());
                if from_file_id >= intent.to_file_id {
                    return Ok(None);
                }
                let intent = WalRecycleIntent {
                    group_id: intent.group_id,
                    from_file_id,
                    to_file_id: intent.to_file_id,
                };
                let state = WalRecycleState::intent(
                    intent.group_id,
                    intent.from_file_id,
                    intent.to_file_id,
                );
                let mut buf = vec![0u8; state.packed_size()];
                state.encode(&mut buf);
                txn.put(&key, &buf)?;
                Ok(Some(intent))
            })
            .unwrap_or_else(|err| panic!("commit wal recycle intent failed: {err:?}"))
    }

    pub(crate) fn commit_wal_recycle_done(&self, intent: WalRecycleIntent) -> bool {
        let key = wal_recycle_key(intent.group_id);
        self.btree
            .exec(BUCKET_MISC, |txn| {
                let state = match txn.get(&key) {
                    Ok(raw) => WalRecycleState::decode(&raw),
                    Err(BTreeError::KeyNotFound) => return Ok(false),
                    Err(err) => return Err(err),
                };
                if state
                    != WalRecycleState::intent(
                        intent.group_id,
                        intent.from_file_id,
                        intent.to_file_id,
                    )
                {
                    return Ok(false);
                }
                let state =
                    WalRecycleState::done(intent.group_id, intent.from_file_id, intent.to_file_id);
                let mut buf = vec![0u8; state.packed_size()];
                state.encode(&mut buf);
                txn.put(&key, &buf)?;
                Ok(true)
            })
            .unwrap_or_else(|err| panic!("commit wal recycle done failed: {err:?}"))
    }
}
