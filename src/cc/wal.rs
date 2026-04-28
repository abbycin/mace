use crc32c::Crc32cHasher;
use std::fmt::Debug;
use std::hash::Hasher;

use crate::{
    static_assert,
    utils::{OpCode, data::Position},
};

pub(crate) trait IWalCodec {
    fn encoded_len(&self) -> usize;

    fn size() -> usize;

    fn to_slice(&self) -> &[u8];
}

pub(crate) trait IWalPayload {
    fn sub_type(&self) -> PayloadType;
}

#[repr(u8)]
#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub(crate) enum EntryType {
    Update,
    Begin,
    Commit,
    Abort,
    CheckPoint,
    Unknown,
}

impl TryFrom<u8> for EntryType {
    type Error = OpCode;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        if value < EntryType::Unknown as u8 {
            unsafe { Ok(std::mem::transmute::<u8, EntryType>(value)) }
        } else {
            Err(OpCode::Corruption)
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PayloadType {
    Insert,
    Update,
    Delete,
}

impl TryFrom<u8> for PayloadType {
    type Error = OpCode;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        if value <= PayloadType::Delete as u8 {
            unsafe { Ok(std::mem::transmute::<u8, PayloadType>(value)) }
        } else {
            Err(OpCode::Corruption)
        }
    }
}

#[repr(C, packed(1))]
pub(crate) struct WalUpdate {
    pub(crate) wal_type: EntryType,
    pub(crate) sub_type: PayloadType,
    pub(crate) bucket_id: u64,
    pub(crate) group_id: u8,
    /// payload size
    pub(crate) size: u32,
    pub(crate) cmd_id: u32,
    pub(crate) klen: u32,
    pub(crate) txid: u64,
    pub(crate) prev_id: u64,
    pub(crate) prev_off: u64,
    pub(crate) checksum: u32,
}

impl Debug for WalUpdate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WalUpdate")
            .field("wal_type", &self.wal_type)
            .field("sub_type", &self.sub_type)
            .field("bucket_id", &{ self.bucket_id })
            .field("woker_id", &{ self.group_id })
            .field("size", &{ self.size })
            .field("cmd_id", &{ self.cmd_id })
            .field("klen", &{ self.klen })
            .field("txid", &{ self.txid })
            .field("prev_addr", &{ self.prev_id })
            .field("prev_off", &{ self.prev_off })
            .field("checksum", &{ self.checksum })
            .finish()
    }
}

#[repr(C, packed(1))]
#[derive(Debug)]
pub(crate) struct WalBegin {
    pub(crate) wal_type: EntryType,
    pub(crate) txid: u64,
    pub(crate) checksum: u32,
}

#[repr(C, packed(1))]
#[derive(Debug)]
pub(crate) struct WalAbort {
    pub(crate) wal_type: EntryType,
    pub(crate) txid: u64,
    pub(crate) checksum: u32,
}

#[repr(C, packed(1))]
#[derive(Debug)]
pub(crate) struct WalCommit {
    pub(crate) wal_type: EntryType,
    pub(crate) txid: u64,
    pub(crate) checksum: u32,
}

static_assert!(size_of::<WalCommit>() == size_of::<WalAbort>());
static_assert!(size_of::<WalCommit>() == size_of::<WalBegin>());

// NOTE: the wal is shared among txns in the same group
// the checkpoint is used to identify that the log buffer was flushed and stabilized
// it does not imply that all transactions are committed; active transactions may exist
#[repr(C, packed(1))]
#[derive(Debug)]
pub(crate) struct WalCheckpoint {
    pub(crate) wal_type: EntryType,
    pub(crate) checkpoint: Position,
    pub(crate) checksum: u32,
}

macro_rules! impl_checksum {
    ($t:ty) => {
        impl $t {
            pub(crate) fn calc_checksum(&self) -> u32 {
                let ptr = self as *const Self as *const u8;
                let len = self.encoded_len();
                let checksum_offset = len - size_of_val(&{ self.checksum });

                let mut h = Crc32cHasher::default();
                unsafe {
                    let slice = std::slice::from_raw_parts(ptr, checksum_offset);
                    h.write(slice);
                }
                h.finish() as u32
            }

            pub(crate) fn is_intact(&self) -> bool {
                self.checksum == self.calc_checksum()
            }
        }
    };
}

impl_checksum!(WalBegin);
impl_checksum!(WalCommit);
impl_checksum!(WalAbort);
impl_checksum!(WalCheckpoint);

impl WalUpdate {
    pub(crate) fn sub_type(&self) -> PayloadType {
        self.sub_type
    }

    pub(crate) fn key(&self) -> &[u8] {
        let ptr = self as *const Self as *const u8;
        unsafe { std::slice::from_raw_parts(ptr.add(self.encoded_len()), self.klen as usize) }
    }

    fn cast_to<T>(&self) -> &T {
        let ptr = self as *const Self as *const u8;
        unsafe { &*ptr.add(self.encoded_len() + self.klen as usize).cast::<T>() }
    }

    pub(crate) fn payload_len(&self) -> usize {
        self.size as usize
    }

    pub(crate) fn put(&self) -> &WalPut {
        self.cast_to::<WalPut>()
    }

    pub(crate) fn update(&self) -> &WalReplace {
        self.cast_to::<WalReplace>()
    }

    pub(crate) fn calc_checksum(&self) -> u32 {
        let ptr = self as *const Self as *const u8;
        let header_size = self.encoded_len();
        let checksum_offset = header_size - size_of_val(&{ self.checksum });

        let mut h = Crc32cHasher::default();
        unsafe {
            let header_slice = std::slice::from_raw_parts(ptr, checksum_offset);
            h.write(header_slice);

            let payload_slice =
                std::slice::from_raw_parts(ptr.add(header_size), self.payload_len());
            h.write(payload_slice);
        }
        h.finish() as u32
    }

    pub(crate) fn is_intact(&self) -> bool {
        let expected = { self.checksum };
        let actual = self.calc_checksum();
        expected == actual
    }
}

macro_rules! impl_codec {
    ($s: ty) => {
        impl IWalCodec for $s {
            fn encoded_len(&self) -> usize {
                size_of::<Self>()
            }

            fn size() -> usize {
                size_of::<Self>()
            }

            fn to_slice(&self) -> &[u8] {
                unsafe {
                    let ptr = self as *const Self as *const u8;
                    std::slice::from_raw_parts(ptr, self.encoded_len())
                }
            }
        }
    };
}

impl_codec!(WalUpdate);
impl_codec!(WalBegin);
impl_codec!(WalCommit);
impl_codec!(WalAbort);
impl_codec!(WalCheckpoint);

#[repr(C, packed(1))]
#[derive(Clone, Copy, Debug)]
pub(crate) struct WalPut {
    vlen: u32,
}

impl WalPut {
    pub(crate) fn new(vlen: usize) -> Self {
        Self { vlen: vlen as u32 }
    }

    fn get(&self, pos: usize, len: usize) -> &[u8] {
        unsafe {
            let ptr = self as *const Self as *const u8;
            std::slice::from_raw_parts(ptr.add(pos), len)
        }
    }

    pub(crate) fn val(&self) -> &[u8] {
        self.get(size_of::<Self>(), self.vlen as usize)
    }
}

impl_codec!(WalPut);

#[repr(C, packed(1))]
#[derive(Clone, Copy, Debug)]
pub(crate) struct WalReplace {
    pub(crate) nv_len: u32,
}

impl WalReplace {
    pub(crate) fn new(nv_len: usize) -> Self {
        Self {
            nv_len: nv_len as u32,
        }
    }

    fn get(&self, pos: usize, len: usize) -> &[u8] {
        unsafe {
            let ptr = self as *const Self as *const u8;
            std::slice::from_raw_parts(ptr.add(pos), len)
        }
    }

    pub(crate) fn new_val(&self) -> &[u8] {
        self.get(size_of::<Self>(), self.nv_len as usize)
    }
}

impl_codec!(WalReplace);

#[repr(C, packed(1))]
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct WalDel {}

impl WalDel {
    pub fn new() -> Self {
        Self {}
    }
}

impl_codec!(WalDel);

impl IWalPayload for WalPut {
    fn sub_type(&self) -> PayloadType {
        PayloadType::Insert
    }
}

impl IWalPayload for WalReplace {
    fn sub_type(&self) -> PayloadType {
        PayloadType::Update
    }
}

impl IWalPayload for WalDel {
    fn sub_type(&self) -> PayloadType {
        PayloadType::Delete
    }
}

impl IWalCodec for &[u8] {
    fn encoded_len(&self) -> usize {
        self.len()
    }

    fn size() -> usize {
        unreachable!("slice has no static size")
    }

    fn to_slice(&self) -> &[u8] {
        self
    }
}

pub(crate) fn wal_record_sz(e: EntryType) -> Result<usize, OpCode> {
    match e {
        EntryType::Abort | EntryType::Begin | EntryType::Commit => Ok(WalAbort::size()),
        EntryType::Update => Ok(WalUpdate::size()),
        EntryType::CheckPoint => Ok(WalCheckpoint::size()),
        _ => Err(OpCode::Corruption),
    }
}

#[derive(Copy, Clone, Debug)]
pub(crate) struct Location {
    pub(crate) group_id: u32,
    pub(crate) len: u32,
    pub(crate) pos: Position,
}

pub(crate) fn ptr_to<T>(x: *const u8) -> &'static T {
    unsafe { &*x.cast::<T>() }
}

#[cfg(test)]
mod test {
    use crate::cc::wal::{EntryType, IWalCodec, PayloadType, WalPut, WalUpdate, ptr_to};

    #[test]
    fn dump_load() {
        const KEY: &[u8] = "114514".as_bytes();
        const VAL: &[u8] = "1919810".as_bytes();

        let put = WalPut::new(VAL.len());
        let len = put.encoded_len() + KEY.len() + VAL.len();
        let c = WalUpdate {
            wal_type: EntryType::Update,
            sub_type: PayloadType::Insert,
            bucket_id: 0,
            group_id: 19,
            cmd_id: 8,
            txid: 26,
            size: len as u32,
            klen: KEY.len() as u32,
            prev_id: 0,
            prev_off: 0,
            checksum: 0, // will be calculated after record is assembled
        };
        let total_len = c.encoded_len() + len;
        let mut buf = vec![0u8; total_len * 2];
        let s = buf.as_mut_slice();

        let ce = c.encoded_len();
        let ke = c.encoded_len() + KEY.len();
        let pe = ke + put.encoded_len();
        let ve = pe + VAL.len();

        for i in 0..20 {
            let off = i as usize;
            {
                let pc = &mut s[off..off + ce];

                pc.copy_from_slice(c.to_slice());
            }
            {
                let pk = &mut s[off + ce..off + ke];
                pk.copy_from_slice(KEY);
            }
            {
                let pp = &mut s[off + ke..off + pe];
                pp.copy_from_slice(put.to_slice());
            }
            {
                let pv = &mut s[off + pe..off + ve];
                pv.copy_from_slice(VAL);
            }

            let a = &s[off..off + total_len];
            let nc = ptr_to::<WalUpdate>(a.as_ptr());

            assert_eq!({ nc.group_id }, 19);
            assert_eq!({ nc.txid }, 26);
            assert_eq!({ nc.cmd_id }, 8);
            assert_eq!({ nc.size }, len as u32);
            assert_eq!({ nc.klen }, KEY.len() as u32);
            assert_eq!(nc.key(), KEY);
            let np = nc.put();
            assert_eq!(np.val(), VAL);
        }
    }

    #[test]
    fn test_wal_entry_checksums() {
        use crate::cc::wal::{WalBegin, WalCheckpoint, WalCommit};

        let mut begin = WalBegin {
            wal_type: EntryType::Begin,
            txid: 12345,
            checksum: 0,
        };
        begin.checksum = begin.calc_checksum();
        assert!(begin.is_intact());

        // corrupt it
        begin.txid = 54321;
        assert!(!begin.is_intact());

        // restore
        begin.txid = 12345;
        assert!(begin.is_intact());

        // commit
        let mut commit = WalCommit {
            wal_type: EntryType::Commit,
            txid: 12345,
            checksum: 0,
        };
        commit.checksum = commit.calc_checksum();
        assert!(commit.is_intact());

        commit.wal_type = EntryType::Abort; // corrupt type
        assert!(!commit.is_intact());

        // checkpoint
        let mut ckpt = WalCheckpoint {
            wal_type: EntryType::CheckPoint,
            checkpoint: crate::utils::data::Position {
                file_id: 7,
                offset: 11,
            },
            checksum: 0,
        };
        ckpt.checksum = ckpt.calc_checksum();
        assert!(ckpt.is_intact());

        ckpt.wal_type = EntryType::Unknown;
        assert!(!ckpt.is_intact());
    }
}
