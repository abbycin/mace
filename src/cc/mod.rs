use crate::cc::{context::Context, group::TxnFact};
use crate::must_true;
use crate::utils::options::Options;
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

/// whether a record's group id still belongs to the current Context.
///
/// a group id at or beyond the current width was written by an era this Context never served:
/// every width change wipes the full physical stream set of the era it leaves, so such a record's
/// transaction was already resolved (redo, abort-clean, forced checkpoint) before that wipe. The
/// branch is therefore unconditional — it does not depend on `safe_exclusive`, and it must run
/// before any group lookup so a retired group index is never dereferenced.
///
/// a persisted record group id never carries the runtime-only merge tag bit, so a value at or
/// beyond `Options::MAX_CONCURRENT_WRITE` is page corruption: rejecting it here keeps the
/// settled-history branch from mislabelling an impossible group as settled history
#[inline(always)]
fn is_settled_history(ctx: &Context, record_gid: u8) -> bool {
    must_true!(
        record_gid < Options::MAX_CONCURRENT_WRITE,
        "impossible record group id {record_gid}"
    );
    record_gid as usize >= ctx.groups().len()
}

/// retained-abort state of a record's group under the current Context.
///
/// a group beyond the current width cannot be a retained abort: its era was settled and wiped
/// before this Context existed, so the version is a committed historical fact
#[inline(always)]
pub(crate) fn is_retained_abort(ctx: &Context, record_gid: u8, record_txid: u64) -> bool {
    !is_settled_history(ctx, record_gid)
        && ctx
            .group(record_gid as usize)
            .is_retained_abort(record_txid)
}

#[inline]
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

    // read the boundary before dereferencing a group: the settled-history branch below bypasses
    // the group table entirely
    let safe_exclusive = ctx.safe_exclusive();
    if is_settled_history(ctx, record_gid) {
        return true;
    }
    let record_group = ctx.group(record_gid as usize);
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

#[cfg(test)]
mod tests {
    use super::{SnapshotStamp, is_retained_abort, is_visible_to};
    use crate::RandomPath;
    use crate::cc::context::{Context, GroupBoot};
    use crate::error::test_support::{assert_aborted, is_abort_child, run_child_test};
    use crate::meta::Sequences;
    use crate::utils::data::Position;
    use crate::utils::options::Options;
    use std::sync::Arc;

    const WIDTH: u8 = 2;
    /// a record group retired by a shrink below this Context's width
    const RETIRED_GID: u8 = 5;
    /// the highest group id page data can legitimately carry
    const LAST_PERSISTED_GID: u8 = Options::MAX_CONCURRENT_WRITE - 1;

    /// an empty, file-less boot for the given physical stream
    fn test_boot(physical_wal_id: u8) -> GroupBoot {
        GroupBoot {
            physical_wal_id,
            oldest_id: 0,
            latest_id: 0,
            scan_start: Position::MIN,
            has_files: false,
            start_id: 0,
            checkpoint_floor: Position::MIN,
        }
    }

    /// a narrow runtime Context: its group table cannot answer for `RETIRED_GID`
    fn narrow_context() -> (RandomPath, Context) {
        let root = RandomPath::tmp();
        let mut opt = Options::new(&*root);
        opt.concurrent_write = WIDTH;
        let mut boots: Vec<GroupBoot> = (0..WIDTH).map(test_boot).collect();
        boots.push(test_boot(Options::SHARED_ID));
        let ctx = Context::new(
            Arc::new(opt.validate().expect("context options must validate")),
            WIDTH,
            Arc::new(Sequences::default()),
            &boots,
        );
        assert_eq!(ctx.groups().len(), WIDTH as usize);
        (root, ctx)
    }

    #[test]
    fn settled_history_group_is_visible_without_a_group_lookup() {
        let (_root, ctx) = narrow_context();
        // a group id at or beyond the width, and one at the persisted maximum, belong to an era
        // that was settled and wiped before this Context existed: committing them must not index
        // the group table, and must not depend on the safe boundary (the oracle here is 0)
        for gid in [WIDTH, RETIRED_GID, LAST_PERSISTED_GID] {
            assert!(
                is_visible_to(&ctx, SnapshotStamp::view(100), gid, 10),
                "retired gid {gid} must read as a committed historical version"
            );
        }
    }

    #[test]
    fn retired_group_id_is_never_a_retained_abort() {
        let (_root, ctx) = narrow_context();
        // the live half of the contract still consults the group table
        ctx.group(1).recover_retained_abort(10);
        assert!(
            is_retained_abort(&ctx, 1, 10),
            "a live group's retained abort must stay observable"
        );
        assert!(
            !is_retained_abort(&ctx, 1, 11),
            "only the aborted txid is retained"
        );
        // ...and the retired half stays outside it
        for gid in [WIDTH, RETIRED_GID, LAST_PERSISTED_GID] {
            assert!(
                !is_retained_abort(&ctx, gid, 10),
                "retired gid {gid} cannot hold a retained abort"
            );
        }
    }

    /// an impossible persisted group id is corruption, so the process aborts with the
    /// report instead of unwinding and answering the record as settled history
    #[test]
    fn impossible_record_group_id_is_rejected() {
        let output = run_child_test("cc::tests::impossible_record_group_id_is_rejected_child");
        assert_aborted(
            &output,
            &[
                "fatal invariant failure",
                "check=must_true",
                "impossible record group id",
            ],
        );
    }

    /// the aborting side of the test above: it runs only in the child process
    #[test]
    fn impossible_record_group_id_is_rejected_child() {
        if !is_abort_child() {
            return;
        }
        let (_root, ctx) = narrow_context();
        // a persisted group id carries no runtime merge tag, so bit 7 set is corruption and must
        // not be answered as settled history
        let _ = is_visible_to(
            &ctx,
            SnapshotStamp::view(100),
            Options::MAX_CONCURRENT_WRITE,
            10,
        );
    }

    #[test]
    fn own_write_and_concurrent_records_precede_the_settled_history_branch() {
        let (_root, ctx) = narrow_context();
        // the transaction's own write stays visible, another writer's record at the same
        // timestamp stays invisible, and a record at or after the snapshot start stays invisible,
        // even when its group was retired by a shrink
        assert!(is_visible_to(
            &ctx,
            SnapshotStamp::txn(RETIRED_GID, 10),
            RETIRED_GID,
            10
        ));
        assert!(!is_visible_to(
            &ctx,
            SnapshotStamp::txn(1, 10),
            RETIRED_GID,
            10
        ));
        assert!(!is_visible_to(
            &ctx,
            SnapshotStamp::view(10),
            RETIRED_GID,
            10
        ));
    }
}
