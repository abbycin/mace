use std::sync::atomic::AtomicU32;

use crate::{static_assert, types::traits::ICodec, utils::varint::Varint64};

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
pub(crate) struct BoxHeader {
    pub(super) refcnt: AtomicU32,
    pub(crate) kind: TagKind,
    pub(crate) node_type: NodeType,
    pub(crate) flag: TagFlag,
    pub(crate) total_size: u32,
    pub(crate) payload_size: u32,
    pub(crate) pid: u64,
    /// current TagPtr's logical address
    pub(crate) addr: u64,
    /// logical address link to next TagPtr
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
    /// elems to trigger merge when necessary, set when split happen
    pub(crate) split_elems: u16,
    /// total size (including header)
    pub(crate) size: u32,
    /// pid of right sibling, when it the right most node, it should be 0
    pub(crate) right_sibling: u64,
    /// pid of child, when there's no mergin child, it should be 0
    pub(crate) merging_child: u64,
    pub(crate) lo_len: u32,
    pub(crate) hi_len: u32,
    /// indirect addr count, including big key-val in sst, sibling itself and big ver-val in sibling
    pub(crate) nr_remote: u16,
    pub(crate) merging: bool,
    pub(crate) is_index: bool,
} // sst

#[repr(C, align(8))]
pub(crate) struct RemoteHeader {
    pub(crate) size: usize,
}

static_assert!(align_of::<BoxHeader>() == align_of::<*const ()>());
static_assert!(align_of::<BaseHeader>() == align_of::<*const ()>());
static_assert!(align_of::<DeltaHeader>() == align_of::<*const ()>());
static_assert!(align_of::<RemoteHeader>() == align_of::<*const ()>());

static_assert!(size_of::<BoxHeader>() % 8 == 0);
static_assert!(size_of::<BaseHeader>() % 8 == 0);
static_assert!(size_of::<DeltaHeader>() % 8 == 0);
static_assert!(size_of::<RemoteHeader>() % 8 == 0);

pub(crate) struct Slot {
    meta: u64,
}

impl Slot {
    const INDIRECT_BIT: u64 = 1 << 63;
    const MASK: u8 = 0x80; // highest byte mask
    pub const REMOTE_LEN: usize = size_of::<Self>();
    pub const LOCAL_LEN: usize = 1;

    pub(crate) const fn from_remote(addr: u64) -> Self {
        Self {
            meta: addr | Self::INDIRECT_BIT,
        }
    }

    pub(crate) const fn inline() -> Self {
        Self { meta: 0 }
    }

    #[inline]
    pub(crate) const fn is_inline(&self) -> bool {
        self.meta & Self::INDIRECT_BIT == 0
    }

    pub(crate) const fn addr(&self) -> u64 {
        debug_assert!(!self.is_inline());
        self.meta & !Self::INDIRECT_BIT
    }
}

impl ICodec for Slot {
    fn packed_size(&self) -> usize {
        if self.is_inline() {
            debug_assert_eq!(Varint64::size(self.meta), Self::LOCAL_LEN);
            Self::LOCAL_LEN
        } else {
            Self::REMOTE_LEN
        }
    }

    fn encode_to(&self, to: &mut [u8]) {
        if self.is_inline() {
            Varint64::encode(to, self.meta);
        } else {
            debug_assert!(to.len() == Self::REMOTE_LEN);
            let be = self.meta.to_be_bytes();
            to.copy_from_slice(&be);
        }
    }

    fn decode_from(raw: &[u8]) -> Self {
        let meta = raw[0];
        if meta & Self::MASK == 0 {
            Self::inline()
        } else {
            Self {
                meta: <u64>::from_be_bytes((&raw[..Self::REMOTE_LEN]).try_into().unwrap()),
            }
        }
    }
}

pub(crate) type SlotType = u32;
pub(crate) const SLOT_LEN: usize = size_of::<SlotType>();
