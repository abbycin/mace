pub(crate) mod buffer;
pub(crate) mod cache;
pub(crate) mod chunk;
pub(crate) mod data;
pub(crate) mod evictor;

pub(crate) mod flush;
pub mod table;

use crate::utils::data::{AddrPair, Interval};

pub(crate) enum SharedState {
    Quit,
    Evict,
}

#[cfg(feature = "metric")]
pub use flush::g_flush_status;

pub(crate) trait IFooter: Default {
    const LEN: usize = size_of::<Self>();

    fn nr_interval(&self) -> usize;

    fn nr_reloc(&self) -> usize;

    fn reloc_crc(&self) -> u32;

    fn interval_crc(&self) -> u32;

    fn interval_len(&self) -> usize {
        self.nr_interval() * Interval::LEN
    }

    fn reloc_len(&self) -> usize {
        self.nr_reloc() * AddrPair::LEN
    }
}
