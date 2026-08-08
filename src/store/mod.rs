pub(crate) mod gc;
pub(crate) mod recovery;
pub(crate) mod store;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct VacuumStats {
    pub scanned: u64,
    pub compacted: u64,
}
