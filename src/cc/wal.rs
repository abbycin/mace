use std::{
    cell::RefCell,
    cmp::max,
    collections::{hash_map::Entry, BTreeMap, HashMap},
    rc::Rc,
};

use io::{File, GatherIO};

use crate::{
    index::{data::Value, tree::Tree, Key},
    static_assert,
    utils::{
        block::Block, bytes::ByteArray, unpack_id, INIT_CMD, NEXT_ID, NULL_ORACLE, ROOT_TREEID,
    },
    Record,
};

use super::{context::Context, data::Ver};

pub(crate) trait IWalRec {
    fn set_lsn(&mut self, lsn: u64);
}

pub(crate) trait IWalCodec {
    fn encoded_len(&self) -> usize;

    fn size() -> usize;

    fn encode_to(&self, b: ByteArray);

    fn to_slice(&self) -> &[u8];
}

pub(crate) trait IWalPayload {
    fn sub_type(&self) -> PayloadType;
}

#[repr(u8)]
#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub(crate) enum EntryType {
    Padding,
    Update,
    Begin,
    Commit,
    Abort,
    CheckPoint,
    TreePut,
    TreeDel,
    Unknown,
}

impl From<u8> for EntryType {
    fn from(value: u8) -> Self {
        assert!(value < EntryType::Unknown as u8);
        unsafe { std::mem::transmute(value) }
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

impl From<u8> for PayloadType {
    fn from(value: u8) -> Self {
        assert!(value <= PayloadType::Delete as u8);
        unsafe { std::mem::transmute(value) }
    }
}

#[repr(C, packed(1))]
#[derive(Debug)]
pub(crate) struct WalPadding {
    pub(crate) wal_type: EntryType,
    pub(crate) len: u32,
}

#[repr(C, packed(1))]
#[derive(Debug)]
pub(crate) struct WalUpdate {
    pub(crate) wal_type: EntryType,
    pub(crate) sub_type: PayloadType,
    /// meaningful in txn rollback, unnecessary in recovery
    pub(crate) worker_id: u16,
    /// payload size
    pub(crate) size: u32,
    pub(crate) cmd_id: u32,
    pub(crate) klen: u32,
    pub(crate) tree_id: u64,
    pub(crate) txid: u64,
    pub(crate) prev_addr: u64,
}

#[repr(C, packed(1))]
#[derive(Debug)]
pub(crate) struct WalBegin {
    pub(crate) wal_type: EntryType,
    pub(crate) txid: u64,
    pub(crate) prev_addr: u64,
}

#[repr(C, packed(1))]
#[derive(Debug)]
pub(crate) struct WalAbort {
    pub(crate) wal_type: EntryType,
    pub(crate) txid: u64,
    pub(crate) prev_addr: u64,
}

#[repr(C, packed(1))]
#[derive(Debug)]
pub(crate) struct WalCommit {
    pub(crate) wal_type: EntryType,
    pub(crate) txid: u64,
    pub(crate) prev_addr: u64,
}

static_assert!(size_of::<WalCommit>() == size_of::<WalAbort>());
static_assert!(size_of::<WalCommit>() == size_of::<WalBegin>());

#[repr(C, packed(1))]
#[derive(Debug)]
pub(crate) struct WalCheckpoint {
    pub(crate) wal_type: EntryType,
    /// snapshot of current oracle
    pub(crate) txid: u64,
    /// previous checkpoint's file + offset
    pub(crate) prev_addr: u64,
    // same to worker count
    pub(crate) active_txn_cnt: u16,
    // follows txid-addr map
}

#[repr(C, packed(1))]
#[derive(Debug)]
pub(crate) struct WalTree {
    pub(crate) wal_type: EntryType,
    pub(crate) txid: u64,
    /// same to txid on creation
    pub(crate) id: u64,
    pub(crate) pid: u64,
}

#[derive(Default, Clone, Copy, Debug)]
#[repr(C, packed(1))]
pub(crate) struct CkptItem {
    pub(crate) txid: u64,
    pub(crate) addr: u64,
}

pub(crate) struct CkptMem {
    pub(crate) hdr: WalCheckpoint,
    mask: Vec<CkptItem>,
    pub(crate) txn: Vec<CkptItem>,
}

unsafe impl Send for CkptItem {}

impl CkptMem {
    pub(crate) fn new(workers: usize) -> Self {
        let mut mask: Vec<CkptItem> = vec![CkptItem::default(); workers];
        for i in &mut mask {
            i.txid = NULL_ORACLE;
        }
        Self {
            hdr: WalCheckpoint {
                wal_type: EntryType::CheckPoint,
                txid: 0,
                prev_addr: 0,
                active_txn_cnt: 0,
            },
            mask,
            txn: Vec::new(),
        }
    }

    pub(crate) fn reset(&mut self, worker: u16) {
        #[cfg(debug_assertions)]
        assert!((worker as usize) < self.mask.len());
        self.mask[worker as usize].txid = NULL_ORACLE;
    }

    pub(crate) fn set(&mut self, worker: u16, txid: u64, addr: u64) {
        #[cfg(debug_assertions)]
        {
            assert!((worker as usize) < self.mask.len());
            let (id, _) = unpack_id(addr);
            debug_assert_ne!(id, 0);
            assert_ne!(txid, 0);
        }
        let x = &mut self.mask[worker as usize];
        x.txid = txid;
        x.addr = addr;
    }

    pub(crate) fn slice(
        &mut self,
        txid: u64,
        prev_addr: u64,
    ) -> ((*const u8, usize), (*const u8, usize)) {
        self.hdr.txid = txid;
        self.hdr.prev_addr = prev_addr;
        self.hdr.active_txn_cnt = self.fill_txn() as u16;

        (
            (self.hdr.to_slice().as_ptr(), self.hdr.encoded_len()),
            (
                self.txn.as_ptr().cast::<u8>(),
                self.txn.len() * size_of::<CkptItem>(),
            ),
        )
    }

    fn fill_txn(&mut self) -> usize {
        self.txn.clear();
        for i in &self.mask {
            if i.txid != NULL_ORACLE {
                assert_ne!({ i.txid }, 0);
                self.txn.push(*i);
            }
        }
        self.txn.len()
    }
}

impl WalCheckpoint {
    pub(crate) fn active_txid(&self) -> &[CkptItem] {
        let p = self as *const Self;
        unsafe {
            let ptr = p.add(1).cast::<CkptItem>();
            std::slice::from_raw_parts(ptr, self.active_txn_cnt as usize)
        }
    }

    pub(crate) fn payload_len(&self) -> usize {
        self.active_txn_cnt as usize * size_of::<CkptItem>()
    }
}

impl WalTree {
    pub(crate) fn new(is_put: bool, id: u64, txid: u64, pid: u64) -> Self {
        Self {
            wal_type: if is_put {
                EntryType::TreePut
            } else {
                EntryType::TreeDel
            },
            txid,
            id,
            pid,
        }
    }

    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    // fixed
    pub(crate) fn tree_id(&self) -> u64 {
        ROOT_TREEID
    }

    pub(crate) fn root_pid(&self) -> u64 {
        self.pid
    }
}

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

            fn encode_to(&self, b: ByteArray) {
                let src = self.to_slice();
                debug_assert_eq!(src.len(), b.len());
                let dst = b.as_mut_slice(0, b.len());
                dst.copy_from_slice(src);
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
impl IWalRec for WalPadding {
    fn set_lsn(&mut self, _lsn: u64) {}
}

macro_rules! impl_rec {
    ($s: ty) => {
        impl IWalRec for $s {
            fn set_lsn(&mut self, lsn: u64) {
                self.prev_addr = lsn;
            }
        }
    };
}

impl_rec!(WalUpdate);
impl_rec!(WalBegin);
impl_rec!(WalCommit);
impl_rec!(WalAbort);

impl_codec!(WalPadding);
impl_codec!(WalUpdate);
impl_codec!(WalBegin);
impl_codec!(WalCommit);
impl_codec!(WalAbort);
impl_codec!(WalCheckpoint);
impl_codec!(WalTree);

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
    pub(crate) undo_next: u64,
}

impl WalClr {
    pub(crate) fn new(tombstone: bool, vlen: usize, undo_next: u64) -> Self {
        Self {
            tombstone,
            vlen: vlen as u32,
            undo_next,
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

    fn encode_to(&self, b: ByteArray) {
        debug_assert_eq!(self.len(), b.len());
        if b.len() != 0 {
            let dst = b.as_mut_slice(0, b.len());
            dst.copy_from_slice(self);
        }
    }

    fn to_slice(&self) -> &[u8] {
        self
    }
}

pub(crate) fn wal_record_sz(e: EntryType) -> usize {
    match e {
        EntryType::TreeDel | EntryType::TreePut => WalTree::size(),
        EntryType::Abort | EntryType::Begin | EntryType::Commit => WalAbort::size(),
        EntryType::Update => WalUpdate::size(),
        EntryType::Padding => WalPadding::size(),
        EntryType::CheckPoint => WalCheckpoint::size(),
        _ => unreachable!("invalid type {}", e as u8),
    }
}

pub(crate) struct WalReader<'a> {
    map: RefCell<BTreeMap<u32, (Rc<File>, u64)>>,
    trees: RefCell<HashMap<u64, Tree>>,
    ctx: &'a Context,
}

impl<'a> WalReader<'a> {
    pub(crate) fn new(ctx: &'a Context) -> Self {
        Self {
            map: RefCell::new(BTreeMap::new()),
            trees: RefCell::new(HashMap::new()),
            ctx,
        }
    }

    fn get_file(&self, id: u32) -> Option<(Rc<File>, u64)> {
        assert!(id >= NEXT_ID);
        const MAX_OPEN_FILES: usize = 10;
        let mut map = self.map.borrow_mut();
        while map.len() > MAX_OPEN_FILES {
            map.pop_first();
        }
        if let std::collections::btree_map::Entry::Vacant(e) = map.entry(id) {
            let path = self.ctx.opt.wal_file(id);
            if !path.exists() {
                return None;
            }
            let f = File::options().read(true).open(&path).unwrap();
            let len = f.size().unwrap();
            e.insert((Rc::new(f), len));
        }
        map.get(&id).map(|(x, y)| (x.clone(), *y))
    }

    // for rollback, the worker should be same to caller, but can be arbitratry for recovery
    pub(crate) fn rollback<F>(&self, block: &mut Block, txid: u64, addr: u64, get_tree: F)
    where
        F: Fn(u64) -> Tree,
    {
        let (mut id, pos) = unpack_id(addr);
        let mut cmd = INIT_CMD;
        let mut last_worker = 0;
        let mut pos = pos as u64;

        'outer: loop {
            let (f, end) = match self.get_file(id) {
                None => break, // for rollback, this will not happen, but may happen in recovery
                Some(f) => {
                    if f.1 == 0 {
                        break; // empty file
                    }
                    f
                }
            };

            loop {
                let s = block.get_mut_slice(0, block.len());
                f.read(&mut s[0..1], pos).unwrap();
                let h: EntryType = s[0].into();
                let sz = wal_record_sz(h);
                assert!(pos + sz as u64 <= end);

                f.read(&mut s[0..sz], pos).unwrap();
                match h {
                    EntryType::Abort | EntryType::Commit => {
                        break 'outer;
                    }
                    EntryType::Begin => {
                        // we use the same worker in the UPDATE record or else any worker is ok, so
                        // that we can make sure these records will be flushed with the same order
                        // as they were queued
                        let w = self.ctx.worker(last_worker);
                        w.logging.record_abort(txid);
                        break 'outer;
                    }
                    EntryType::Update => {
                        let u = ptr_to::<WalUpdate>(s.as_ptr());
                        let usz = sz + u.payload_len();
                        assert_eq!({ u.txid }, txid);
                        assert!(pos + usz as u64 <= end);
                        last_worker = u.worker_id as usize;
                        if block.len() < usz {
                            block.realloc(usz);
                        }
                        let s = block.get_mut_slice(sz, usz - sz);
                        f.read(s, pos + sz as u64).unwrap();
                        let next =
                            self.undo(ptr_to::<WalUpdate>(block.data()), &mut cmd, &get_tree);
                        let (prev_id, prev_pos) = unpack_id(next);
                        pos = prev_pos as u64;
                        if prev_id != id {
                            id = prev_id;
                            break;
                        }
                    }
                    EntryType::Padding => unreachable!("we've excluded padding from the chain"),
                    _ => unreachable!("the chain will never point to {:?}", h),
                }
            }
        }
    }

    fn undo<F>(&self, c: &WalUpdate, cmd: &mut u32, get_tree: &F) -> u64
    where
        F: Fn(u64) -> Tree,
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
                return x.undo_next;
            }
        };

        *cmd = max(c.cmd_id, *cmd);
        *cmd += 1; // make sure that cmd is increasing in same txn
        let raw = c.key();
        let val = if tombstone {
            Value::Del(Record::remove(c.worker_id))
        } else {
            Value::Put(Record::normal(c.worker_id, data))
        };

        let w = self.ctx.worker(c.worker_id as usize);
        w.logging.record_update(
            Ver::new(c.txid, *cmd),
            c.tree_id,
            WalClr::new(tombstone, data.len(), c.prev_addr),
            raw,
            [].as_slice(),
            data,
        );

        let mut trees = self.trees.borrow_mut();
        let e = trees.entry(c.tree_id);
        let tree = match e {
            Entry::Occupied(ref x) => x.get(),
            Entry::Vacant(x) => {
                let t = get_tree(c.tree_id);
                x.insert(t)
            }
        };
        tree.put(Key::new(raw, c.txid, *cmd), val)
            .expect("can't go wrong");
        c.prev_addr
    }
}

pub(crate) fn ptr_to<T>(x: *const u8) -> &'static T {
    unsafe { &*x.cast::<T>() }
}

#[cfg(test)]
mod test {
    use crate::cc::wal::{ptr_to, EntryType, IWalCodec, PayloadType, WalPut, WalUpdate};

    #[test]
    fn dump_load() {
        const KEY: &[u8] = "114514".as_bytes();
        const VAL: &[u8] = "1919810".as_bytes();

        let put = WalPut::new(VAL.len());
        let len = put.encoded_len() + KEY.len() + VAL.len();
        let c = WalUpdate {
            wal_type: EntryType::Update,
            sub_type: PayloadType::Insert,
            worker_id: 19,
            tree_id: 233,
            cmd_id: 8,
            txid: 26,
            size: len as u32,
            klen: KEY.len() as u32,
            prev_addr: 0,
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

            assert_eq!({ nc.tree_id }, 233);
            assert_eq!({ nc.worker_id }, 19);
            assert_eq!({ nc.txid }, 26);
            assert_eq!({ nc.cmd_id }, 8);
            assert_eq!({ nc.size }, len as u32);
            assert_eq!({ nc.klen }, KEY.len() as u32);
            assert_eq!(nc.key(), KEY);
            let np = nc.put();
            assert_eq!(np.val(), VAL);
        }
    }
}
