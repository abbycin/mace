use std::sync::atomic::AtomicU32;

use crate::static_assert;

#[derive(Copy, Clone, PartialEq, Debug)]
#[repr(u8)]
pub(crate) enum TagKind {
    Delta = 1,
    Base = 2,
    Remote = 3,
}

#[derive(Copy, Clone, PartialEq, Debug)]
#[repr(u8)]
pub(crate) enum NodeType {
    Leaf = 1,
    Intl = 2,
}

#[derive(Copy, Clone, PartialEq, Debug)]
#[repr(u16)]
pub(crate) enum TagFlag {
    Normal = 1,
    TombStone = 2,
    Junk = 3,
    Sibling = 4,
    Unmap = 5,
}

#[derive(Debug)]
#[repr(C, align(8))]
pub struct BoxHeader {
    pub(super) refs: AtomicU32,
    pub(crate) kind: TagKind,
    pub(crate) node_type: NodeType,
    pub(crate) flag: TagFlag,
    pub(crate) total_size: u32,
    pub(crate) payload_size: u32,
    pub(crate) pid: u64,
    pub(crate) txid: u64,
    /// current BoxRef's logical address which is monotonically increasing
    pub(crate) addr: u64,
    /// logical address link to next BoxRef
    pub(crate) link: u64,
}

#[derive(Debug)]
#[repr(C, align(8))]
pub(crate) struct DeltaHeader {
    pub(crate) klen: u32,
    pub(crate) vlen: u32,
} // key-val

#[derive(Debug, Copy, Clone)]
#[repr(C, align(8))]
pub(crate) struct BaseHeader {
    /// key-value count
    pub(crate) elems: u16,
    /// elems to trigger merge when necessary, set when split happen, deprecated, becuase we're not
    /// split by node size anymore, we keep it to avoid passing node elems limit configuration
    pub(crate) split_elems: u16,
    /// total size (including header, remote address)
    pub(crate) size: u32,
    /// pid of right sibling, when it the right most node, it should be 0
    pub(crate) right_sibling: u64,
    pub(crate) lo_len: u32,
    pub(crate) hi_len: u32,
    pub(crate) prefix_len: u32,
    pub(crate) is_index: bool,
    pub(crate) has_multiple_versions: bool,
    pub(crate) padding: u16,
} // sst

#[repr(C, align(8))]
pub(crate) struct RemoteHeader {
    pub(crate) size: usize,
}

static_assert!(align_of::<BoxHeader>() == align_of::<*const ()>());
static_assert!(align_of::<BaseHeader>() == align_of::<*const ()>());
static_assert!(align_of::<DeltaHeader>() == align_of::<*const ()>());
static_assert!(align_of::<RemoteHeader>() == align_of::<*const ()>());

static_assert!(size_of::<BoxHeader>().is_multiple_of(8));
static_assert!(size_of::<BaseHeader>().is_multiple_of(8));
static_assert!(size_of::<DeltaHeader>().is_multiple_of(8));
static_assert!(size_of::<RemoteHeader>().is_multiple_of(8));

pub(crate) type SlotType = u32;
pub(crate) const SLOT_LEN: usize = size_of::<SlotType>();
