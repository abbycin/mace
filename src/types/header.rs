use std::sync::atomic::AtomicU32;

use crate::types::traits::IAsSlice;
use crate::{static_assert, utils::data::Position};

#[derive(Copy, Clone, PartialEq, Debug)]
#[repr(u8)]
pub enum TagKind {
    Delta = 1,
    Base = 2,
    Remote = 3,
}

#[derive(Copy, Clone, PartialEq, Debug)]
#[repr(u8)]
pub enum NodeType {
    Leaf = 1,
    Intl = 2,
}

#[derive(Copy, Clone, PartialEq, Debug)]
#[repr(u8)]
pub enum TagFlag {
    Normal = 1,
    Sibling = 2,
}

#[derive(Debug)]
#[repr(C, align(8))]
pub struct BoxHeader {
    pub(super) refs: AtomicU32,
    pub kind: TagKind,
    pub node_type: NodeType,
    pub flag: TagFlag,
    pub group: u8,
    pub total_size: u32,
    pub payload_size: u32,
    pub pid: u64,
    pub txid: u64,
    /// current BoxRef's logical address which is monotonically increasing
    pub addr: u64,
    /// logical address link to next BoxRef
    pub link: u64,
    pub lsn: Position,
}

#[derive(Debug)]
#[repr(C, align(8))]
pub struct DeltaHeader {
    pub klen: u32,
    pub vlen: u32,
} // key-val

#[derive(Debug, Copy, Clone)]
#[repr(C, align(8))]
pub struct BaseHeader {
    /// key-value count
    pub elems: u16,
    /// elems to trigger merge when necessary, set when split happen, deprecated, becuase we're not
    /// split by node size anymore, we keep it to avoid passing node elems limit configuration
    pub split_elems: u16,
    /// total size (including header, remote address)
    pub size: u32,
    /// pid of right sibling, when it the right most node, it should be 0
    pub right_sibling: u64,
    pub merging_child: u64,
    pub lo_len: u32,
    pub hi_len: u32,
    pub prefix_len: u32,
    pub merging: bool,
    pub is_index: bool,
    pub has_multiple_versions: bool,
    pub padding: u8,
} // sst

#[repr(C, align(8))]
pub(crate) struct RemoteHeader {
    pub(crate) size: usize,
}

#[derive(Clone, Copy)]
#[repr(C, packed(1))]
pub(crate) struct DiskBoxHeader {
    pub(crate) kind: u8,
    pub(crate) node_type: u8,
    pub(crate) flag: u8,
    pub(crate) group: u8,
    pub(crate) total_size: u32,
    pub(crate) payload_size: u32,
    pub(crate) pid: u64,
    pub(crate) txid: u64,
    pub(crate) addr: u64,
    pub(crate) link: u64,
    pub(crate) lsn: Position,
}

#[derive(Clone, Copy)]
#[repr(C, packed(1))]
pub(crate) struct DiskBaseHeader {
    pub(crate) elems: u16,
    pub(crate) split_elems: u16,
    pub(crate) size: u32,
    pub(crate) right_sibling: u64,
    pub(crate) merging_child: u64,
    pub(crate) lo_len: u32,
    pub(crate) hi_len: u32,
    pub(crate) prefix_len: u32,
    pub(crate) merging: u8,
    pub(crate) is_index: u8,
    pub(crate) has_multiple_versions: u8,
    pub(crate) padding: u8,
}

#[derive(Clone, Copy)]
#[repr(C, packed(1))]
pub(crate) struct DiskDeltaHeader {
    pub(crate) klen: u32,
    pub(crate) vlen: u32,
}

#[derive(Clone, Copy)]
#[repr(C, packed(1))]
pub(crate) struct DiskRemoteHeader {
    pub(crate) size: usize,
}

impl IAsSlice for DiskBoxHeader {}
impl IAsSlice for DiskBaseHeader {}
impl IAsSlice for DiskDeltaHeader {}
impl IAsSlice for DiskRemoteHeader {}

#[derive(Clone, Copy)]
pub(crate) enum PersistedPayloadHeaderV1 {
    Base(DiskBaseHeader),
    Delta(DiskDeltaHeader),
    Remote(DiskRemoteHeader),
}

static_assert!(align_of::<BoxHeader>() == align_of::<*const ()>());
static_assert!(align_of::<BaseHeader>() == align_of::<*const ()>());
static_assert!(align_of::<DeltaHeader>() == align_of::<*const ()>());
static_assert!(align_of::<RemoteHeader>() == align_of::<*const ()>());

static_assert!(size_of::<BoxHeader>().is_multiple_of(8));
static_assert!(size_of::<BaseHeader>().is_multiple_of(8));
static_assert!(size_of::<DeltaHeader>().is_multiple_of(8));
static_assert!(size_of::<RemoteHeader>().is_multiple_of(8));
static_assert!(size_of::<DiskBoxHeader>() == size_of::<BoxHeader>() - size_of::<AtomicU32>());
static_assert!(size_of::<DiskBaseHeader>() == size_of::<BaseHeader>());
static_assert!(size_of::<DiskDeltaHeader>() == size_of::<DeltaHeader>());
static_assert!(size_of::<DiskRemoteHeader>() == size_of::<RemoteHeader>());

pub(crate) type SlotType = u32;
pub(crate) const SLOT_LEN: usize = size_of::<SlotType>();
