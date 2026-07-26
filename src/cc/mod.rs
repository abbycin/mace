use crate::cc::{context::Context, group::TxnFact};
use std::sync::atomic::Ordering::Acquire;

pub(crate) mod context;
pub(crate) mod group;
pub(crate) mod log;
pub(crate) mod wal;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotStamp {
    pub start_ts: u64,
    pub owner_group: Option<u8>,
}

impl SnapshotStamp {
    #[inline]
    pub fn txn(owner_group: u8, start_ts: u64) -> Self {
        Self {
            start_ts,
            owner_group: Some(owner_group),
        }
    }

    #[inline]
    pub fn view(start_ts: u64) -> Self {
        Self {
            start_ts,
            owner_group: None,
        }
    }
}

pub(crate) fn is_visible_to(
    ctx: &Context,
    snapshot: SnapshotStamp,
    record_gid: u8,
    record_txid: u64,
) -> bool {
    if record_txid == snapshot.start_ts {
        return snapshot.owner_group == Some(record_gid);
    }

    if record_txid >= snapshot.start_ts {
        return false;
    }

    let record_group = ctx.group(record_gid as usize);
    let safe_exclusive = ctx.safe_exclusive();
    if record_txid < safe_exclusive {
        return validate_positive_proof(record_group, record_txid);
    }

    #[cfg(feature = "extra_check")]
    crate::testing::fire_visibility_sync_point(
        crate::testing::VisibilitySyncPoint::AfterProofMissBeforeExactRead,
        record_txid,
    );
    match record_group.read_fact(record_txid) {
        Some(TxnFact::Committed(commit_ts)) => commit_ts < snapshot.start_ts,
        Some(TxnFact::Active(_) | TxnFact::Aborted) => false,
        None => {
            #[cfg(feature = "extra_check")]
            crate::testing::fire_visibility_sync_point(
                crate::testing::VisibilitySyncPoint::AfterExactMissBeforeSecondSafeRead,
                record_txid,
            );
            record_txid < ctx.safe_exclusive()
        }
    }
}

#[inline(always)]
fn validate_positive_proof(record_group: &crate::cc::group::WriterGroup, record_txid: u64) -> bool {
    loop {
        let seq1 = record_group.abort_seq();
        if !seq1.is_multiple_of(2) {
            std::hint::spin_loop();
            continue;
        }

        #[cfg(feature = "extra_check")]
        crate::testing::fire_visibility_sync_point(
            crate::testing::VisibilitySyncPoint::AfterPositiveFastPathBeforeRetainedAbortCheck,
            record_txid,
        );
        let floor = record_group.retained_abort_floor.load(Acquire);
        if record_txid < floor {
            let seq2 = record_group.abort_seq();
            if seq1 == seq2 {
                return true;
            }
            continue;
        }

        let aborted = record_group.is_retained_abort(record_txid);
        let seq2 = record_group.abort_seq();
        if seq1 == seq2 {
            return !aborted;
        }
    }
}
