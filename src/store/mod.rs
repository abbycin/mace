pub(crate) mod gc;
pub(crate) mod recovery;
pub(crate) mod store;

pub const META_VACUUM_TARGET_BYTES: u64 = 4 * 1024 * 1024;

pub type MetaVacuumStats = btree_store::CompactStats;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct VacuumStats {
    pub scanned: u64,
    pub compacted: u64,
}
