//! offline maintenance checks shared by the `mace-kv` binary
//!
//! the checker is part of the tool's product, not the engine's API: it reads
//! the on-disk format through the engine's internal `maintenance` surface and
//! is versioned with this package.

pub mod checker;

pub use checker::{
    CheckCategory, CheckReport, Diagnostic, LeakReport, RepairRecord, check_database,
    leak_check_database,
};

#[cfg(feature = "extra_check")]
pub use checker::leak_check_reachable_for_test;
