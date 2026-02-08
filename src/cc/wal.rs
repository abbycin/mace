use crc32c::Crc32cHasher;
use std::hash::Hasher;
use std::{
    cell::RefCell,
    cmp::max,
    collections::{BTreeMap, btree_map::Entry},
    fmt::Debug,
    rc::Rc,
};

use crate::{
    io::{File, GatherIO},
    static_assert,
    types::{
        data::{Key, Record, Ver},
        traits::ITree,
    },
    utils::{INIT_CMD, OpCode, block::Block, data::Position},
};
use crossbeam_epoch::Guard;

use super::context::Context;

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
    Clr,
}

impl TryFrom<u8> for PayloadType {
    type Error = OpCode;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        if value <= PayloadType::Clr as u8 {
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
    /// meaningful in txn rollback, unnecessary in recovery
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

    pub(crate) fn del(&self) -> &WalDel {
        self.cast_to::<WalDel>()
    }

    pub(crate) fn clr(&self) -> &WalClr {
        self.cast_to::<WalClr>()
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
    pub(crate) ov_len: u32,
    pub(crate) nv_len: u32,
}

impl WalReplace {
    pub(crate) fn new(ov_len: usize, nv_len: usize) -> Self {
        Self {
            ov_len: ov_len as u32,
            nv_len: nv_len as u32,
        }
    }

    fn get(&self, pos: usize, len: usize) -> &[u8] {
        unsafe {
            let ptr = self as *const Self as *const u8;
            std::slice::from_raw_parts(ptr.add(pos), len)
        }
    }

    pub(crate) fn old_val(&self) -> &[u8] {
        self.get(size_of::<Self>(), self.ov_len as usize)
    }

    pub(crate) fn new_val(&self) -> &[u8] {
        self.get(
            size_of::<Self>() + self.ov_len as usize,
            self.nv_len as usize,
        )
    }
}

impl_codec!(WalReplace);

#[repr(C, packed(1))]
#[derive(Debug)]
pub(crate) struct WalDel {
    pub(crate) vlen: u32,
}

impl WalDel {
    pub fn new(vlen: usize) -> Self {
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

impl_codec!(WalDel);

#[derive(Debug)]
#[repr(C, packed(1))]
pub(crate) struct WalClr {
    tombstone: bool,
    vlen: u32,
    pub(crate) undo_id: u64,
    pub(crate) undo_off: u64,
}

impl WalClr {
    pub(crate) fn new(tombstone: bool, vlen: usize, undo_id: u64, undo_off: u64) -> Self {
        Self {
            tombstone,
            vlen: vlen as u32,
            undo_id,
            undo_off,
        }
    }

    pub(crate) fn is_tombstone(&self) -> bool {
        self.tombstone
    }

    pub(crate) fn val(&self) -> &[u8] {
        let ptr = self as *const Self;
        unsafe { std::slice::from_raw_parts(ptr.add(1).cast::<u8>(), self.vlen as usize) }
    }
}

impl_codec!(WalClr);

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

impl IWalPayload for WalClr {
    fn sub_type(&self) -> PayloadType {
        PayloadType::Clr
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

pub(crate) struct WalReader<'a> {
    map: RefCell<BTreeMap<(u8, u64), (Rc<File>, u64)>>,
    ctx: &'a Context,
    guard: &'a Guard,
}

impl<'a> WalReader<'a> {
    pub(crate) fn new(ctx: &'a Context, guard: &'a Guard) -> Self {
        Self {
            map: RefCell::new(BTreeMap::new()),
            ctx,
            guard,
        }
    }

    fn get_file(&self, id: u8, seq: u64) -> Option<(Rc<File>, u64)> {
        const MAX_OPEN_FILES: usize = 10;
        let mut map = self.map.borrow_mut();
        while map.len() > MAX_OPEN_FILES {
            map.pop_first();
        }
        let key = (id, seq);
        if let Entry::Vacant(e) = map.entry(key) {
            let path = self.ctx.opt.wal_file(id, seq);
            if !path.exists() {
                return None;
            }
            let f = File::options().read(true).open(&path).unwrap();
            let len = f.size().unwrap();
            e.insert((Rc::new(f), len));
        }
        map.get(&key).map(|(x, y)| (x.clone(), *y))
    }

    // for rollback, the group should be same to caller, but can be arbitrary for recovery
    pub(crate) fn rollback<T: ITree, F>(
        &self,
        block: &mut Block,
        txid: u64,
        mut addr: Location,
        get_tree: F,
    ) -> Result<(), OpCode>
    where
        F: Fn(u64) -> T,
    {
        let group_id = addr.group_id;
        let mut cmd = INIT_CMD;
        let mut pos = addr.pos.offset;

        'outer: loop {
            let (f, end) = match self.get_file(group_id as u8, addr.pos.file_id) {
                None => break, // for rollback, this will not happen, but may happen in recovery
                Some(f) => {
                    if f.1 == 0 {
                        break; // empty file
                    }
                    f
                }
            };

            loop {
                let s = block.mut_slice(0, block.len());
                f.read(&mut s[0..1], pos).unwrap();
                let h: EntryType = s[0].try_into()?;
                let sz = wal_record_sz(h)?;
                assert!(pos + sz as u64 <= end);
                assert!(sz <= block.len());

                f.read(&mut s[0..sz], pos).unwrap();
                match h {
                    EntryType::Abort | EntryType::Commit => {
                        let r = ptr_to::<WalCommit>(s.as_ptr());
                        if !r.is_intact() {
                            return Err(OpCode::Corruption);
                        }
                        break 'outer;
                    }
                    EntryType::Begin => {
                        let r = ptr_to::<WalBegin>(s.as_ptr());
                        if !r.is_intact() {
                            return Err(OpCode::Corruption);
                        }
                        // we use the same group in the UPDATE record or else any group is ok, so
                        // that we can make sure these records will be flushed with the same order
                        // as they were queued
                        let g = self.ctx.group(group_id as usize);
                        g.logging.lock().record_abort(txid)?;
                        break 'outer;
                    }
                    EntryType::Update => {
                        let u = ptr_to::<WalUpdate>(s.as_ptr());
                        let usz = sz + u.payload_len();
                        assert_eq!({ u.txid }, txid);
                        assert!(pos + usz as u64 <= end);
                        if block.len() < usz {
                            block.realloc(usz);
                        }
                        let s = block.mut_slice(sz, usz - sz);
                        f.read(s, pos + sz as u64).unwrap();

                        let u = ptr_to::<WalUpdate>(block.data());
                        if !u.is_intact() {
                            return Err(OpCode::Corruption);
                        }

                        let (prev_id, prev_off) =
                            self.undo(u, &mut cmd, &get_tree, group_id as usize, addr.pos)?;
                        pos = prev_off;
                        if prev_id != addr.pos.file_id {
                            addr.pos.file_id = prev_id;
                            break;
                        }
                    }
                    _ => return Err(OpCode::Corruption),
                }
            }
        }
        Ok(())
    }

    fn undo<T: ITree, F>(
        &self,
        c: &WalUpdate,
        cmd: &mut u32,
        get_tree: F,
        group_id: usize,
        current_pos: Position,
    ) -> Result<(u64, u64), OpCode>
    where
        F: Fn(u64) -> T,
    {
        let (tombstone, data) = match c.sub_type() {
            PayloadType::Insert => {
                let i = c.put();
                (true, i.val())
            }
            PayloadType::Update => {
                let u = c.update();
                (false, u.old_val())
            }
            PayloadType::Delete => {
                let d = c.del();
                (false, d.val())
            }
            PayloadType::Clr => {
                let x = c.clr();
                return Ok((x.undo_id, x.undo_off));
            }
        };

        *cmd = max(c.cmd_id, *cmd);
        *cmd += 1; // make sure that cmd is increasing in same txn
        let raw = c.key();
        let val = if tombstone {
            Record::remove(c.group_id)
        } else {
            Record::normal(c.group_id, data)
        };

        let g = self.ctx.group(group_id);
        let key = Key::new(raw, Ver::new(c.txid, *cmd));
        g.logging.lock().record_update(
            &key,
            WalClr::new(tombstone, data.len(), c.prev_id, c.prev_off),
            [].as_slice(),
            data,
            current_pos,
            c.bucket_id,
        )?;

        let tree = get_tree(c.bucket_id);
        tree.put(self.guard, key, val);
        Ok((c.prev_id, c.prev_off))
    }
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
            checksum: 0,
        };
        ckpt.checksum = ckpt.calc_checksum();
        assert!(ckpt.is_intact());

        ckpt.wal_type = EntryType::Unknown;
        assert!(!ckpt.is_intact());
    }
}
