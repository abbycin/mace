//! failpoints-only test surface for the failpoint state machine.
//!
//! lives outside `testing` (which is gated on `extra_check`) so that crash
//! children compiled with just `--features failpoints` — the prod_test.sh
//! failpoints matrix — can attribute wait_for_crash timeouts via hit counts
//! and rule snapshots.

#[cfg(feature = "failpoints")]
pub use crate::utils::failpoint::{any_rule_actioned, hit_count, hits_total, snapshot};
