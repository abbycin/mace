use std::cmp::Ordering;

use crate::{number_to_slice, slice_to_number, utils::traits::IValCodec};

#[derive(Clone, PartialEq, Eq, Copy, Debug)]
pub struct Record<'a> {
    worker_id: u16,
    data: &'a [u8],
}

impl<'a> Record<'a> {
    const TOMBSTONE_MARK: u16 = 1 << 15;

    pub fn normal(worker_id: u16, data: &'a [u8]) -> Self {
        Self { worker_id, data }
    }

    pub fn remove(worker_id: u16) -> Self {
        Self {
            worker_id: worker_id | Self::TOMBSTONE_MARK,
            data: [].as_slice(),
        }
    }

    pub fn is_tombstone(&self) -> bool {
        self.worker_id & Self::TOMBSTONE_MARK != 0
    }

    pub fn worker_id(&self) -> u16 {
        self.worker_id & !Self::TOMBSTONE_MARK
    }

    pub fn data(&self) -> &[u8] {
        self.data
    }

    pub fn from_slice(s: &'a [u8]) -> Self {
        let (l, r) = s.split_at(size_of::<u16>());
        Self {
            worker_id: slice_to_number!(l, u16),
            data: r,
        }
    }

    pub fn to_slice(&self, s: &'a mut [u8]) {
        let (l, r) = s.split_at_mut(size_of::<u16>());
        number_to_slice!(self.worker_id, l);
        debug_assert_eq!(r.len(), self.data.len());
        r.copy_from_slice(self.data);
    }

    pub fn size(&self) -> usize {
        size_of::<u16>() + self.data.len()
    }
}

impl IValCodec for Record<'_> {
    fn size(&self) -> usize {
        self.size()
    }

    fn encode(&self, to: &mut [u8]) {
        self.to_slice(to);
    }

    fn decode(raw: &[u8]) -> Self {
        let s = unsafe { std::slice::from_raw_parts(raw.as_ptr(), raw.len()) };
        Self::from_slice(s)
    }

    fn to_string(&self) -> String {
        format!(
            "Record {{ tombstone: {}, worker_id: {}, data: {} }}",
            self.is_tombstone(),
            self.worker_id(),
            self.data.to_string()
        )
    }

    fn data(&self) -> &[u8] {
        self.data
    }
}

#[derive(Default, PartialEq, Eq, Clone, Copy, Hash, Debug)]
pub struct Ver {
    pub txid: u64,
    pub cmd: u32,
}

impl Ver {
    pub fn new(txid: u64, cmd: u32) -> Self {
        Self { txid, cmd }
    }

    pub fn len() -> usize {
        size_of::<u64>() + size_of::<u32>()
    }
}

impl PartialOrd for Ver {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Ver {
    /// new to old
    fn cmp(&self, other: &Self) -> Ordering {
        match other.txid.cmp(&self.txid) {
            Ordering::Equal => other.cmd.cmp(&self.cmd),
            o => o,
        }
    }
}
