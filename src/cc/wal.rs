#![allow(unused)]

use std::{
    cmp::min,
    ffi::c_int,
    fs::File,
    io,
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
};

use async_io::AsyncIO;

use crate::{
    utils::{block::Block, byte_array::ByteArray, raw_ptr_to_ref, traits::IVal},
    OpCode,
};

pub(crate) trait IWalCodec {
    fn encoded_len(&self) -> usize;

    fn size() -> usize;

    fn encode_to(&self, b: ByteArray);

    fn to_slice(&self) -> &[u8];
}

#[repr(u8)]
#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub(crate) enum EntryType {
    Padding,
    Update,
    Begin,
    End,
    Commit,
    Abort,
    CkptBeg,
    CkptEnd,
    Clr,
    Unknown,
}

impl From<u8> for EntryType {
    fn from(value: u8) -> Self {
        assert!(value < EntryType::Unknown as u8);
        unsafe { std::mem::transmute(value) }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub(crate) enum PayloadType {
    Insert,
    Update,
    Delete,
}

#[repr(packed(1))]
#[derive(Debug)]
pub(crate) struct WalPadding {
    pub(crate) wal_type: EntryType,
    pub(crate) len: u32,
}

#[repr(packed(1))]
#[derive(Debug)]
pub(crate) struct WalUpdate {
    pub(crate) wal_type: EntryType,
    /// payload size
    pub(crate) size: u32,
    pub(crate) tree_id: u16,
    pub(crate) worker_id: u16,
    pub(crate) cmd_id: u32,
    pub(crate) tx_id: u64,
    pub(crate) prev_gsn: u64,
    pub(crate) gsn: u64,
}

#[repr(packed(1))]
#[derive(Debug)]
pub(crate) struct WalBegin {
    pub(crate) wal_type: EntryType,
    pub(crate) txid: u64,
}

#[repr(packed(1))]
#[derive(Debug)]
pub(crate) struct WalEnd {
    pub(crate) wal_type: EntryType,
    pub(crate) txid: u64,
}

#[repr(packed(1))]
#[derive(Debug)]
pub(crate) struct WalAbort {
    pub(crate) wal_type: EntryType,
    pub(crate) txid: u64,
}

#[repr(packed(1))]
#[derive(Debug)]
pub(crate) struct WalCommit {
    pub(crate) wal_type: EntryType,
    pub(crate) txid: u64,
}

#[repr(packed(1))]
#[derive(Debug)]
pub(crate) struct WalCkptBeg {
    pub(crate) wal_type: EntryType,
}

#[repr(packed(1))]
#[derive(Debug)]
pub(crate) struct WalCkptEnd {
    pub(crate) wal_type: EntryType,
    /// followed by a list of `active_txn` txids
    pub(crate) active_txn: u32,
}

#[repr(packed(1))]
#[derive(Debug)]
pub(crate) struct WalCLR {
    pub(crate) wal_type: EntryType,
    pub(crate) txid: u64,
    pub(crate) next_undo_gsn: u64,
}

impl WalUpdate {
    pub(crate) fn new(
        tree: u16,
        worker: u16,
        txid: u64,
        cmd: u32,
        len: usize,
        gsn: u64,
        prev_gsn: u64,
    ) -> Self {
        Self {
            wal_type: EntryType::Update,
            tree_id: tree,
            worker_id: worker,
            tx_id: txid,
            cmd_id: cmd,
            size: len as u32,
            gsn,
            prev_gsn,
        }
    }

    pub(crate) fn payload_type(&self) -> PayloadType {
        let ptr = self as *const Self as *const u8;
        *ptr_to::<PayloadType>(unsafe { ptr.add(self.encoded_len()) })
    }

    fn cast_to<T>(&self) -> &T {
        unsafe {
            let ptr = self as *const Self;
            &*ptr.add(1).cast::<T>()
        }
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
impl_codec!(WalPadding);
impl_codec!(WalUpdate);
impl_codec!(WalBegin);
impl_codec!(WalEnd);
impl_codec!(WalCommit);
impl_codec!(WalAbort);
impl_codec!(WalCLR);
impl_codec!(WalCkptBeg);
impl_codec!(WalCkptEnd);

#[repr(packed(1))]
#[derive(Clone, Copy, Debug)]
pub(crate) struct WalPut {
    payload_type: PayloadType,
    klen: u32,
    vlen: u32,
}

impl WalPut {
    pub(crate) fn new(klen: usize, vlen: usize) -> Self {
        Self {
            payload_type: PayloadType::Insert,
            klen: klen as u32,
            vlen: vlen as u32,
        }
    }

    fn get(&self, pos: usize, len: usize) -> &[u8] {
        unsafe {
            let ptr = self as *const Self as *const u8;
            std::slice::from_raw_parts(ptr.add(pos), len)
        }
    }

    pub(crate) fn key(&self) -> &[u8] {
        self.get(size_of::<Self>(), self.klen as usize)
    }

    pub(crate) fn val(&self) -> &[u8] {
        self.get(size_of::<Self>() + self.klen as usize, self.vlen as usize)
    }
}

impl_codec!(WalPut);

#[repr(packed(1))]
#[derive(Clone, Copy, Debug)]
pub(crate) struct WalReplace {
    payload_type: PayloadType,
    prev_wid: u16,
    prev_cmd: u32,
    prev_txid: u64,
    pub(crate) klen: u32,
    pub(crate) ov_len: u32,
    pub(crate) nv_len: u32,
}

impl WalReplace {
    pub(crate) fn new(
        prev_wid: u16,
        prev_cmd: u32,
        prev_txid: u64,
        klen: usize,
        ov_len: usize,
        nv_len: usize,
    ) -> Self {
        Self {
            payload_type: PayloadType::Update,
            prev_wid,
            prev_cmd,
            prev_txid,
            klen: klen as u32,
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

    pub(crate) fn wid(&self) -> u16 {
        self.prev_wid
    }

    pub(crate) fn key(&self) -> &[u8] {
        self.get(size_of::<Self>(), self.klen as usize)
    }

    pub(crate) fn old_val(&self) -> &[u8] {
        self.get(size_of::<Self>() + self.klen as usize, self.ov_len as usize)
    }

    pub(crate) fn new_val(&self) -> &[u8] {
        self.get(
            size_of::<Self>() + (self.klen + self.ov_len) as usize,
            self.nv_len as usize,
        )
    }
}

impl_codec!(WalReplace);

#[repr(packed(1))]
#[derive(Debug)]
pub(crate) struct WalDel {
    payload_type: PayloadType,
    prev_wid: u16,
    prev_cmd: u32,
    pub(crate) prev_txid: u64,
    pub(crate) klen: u32,
    pub(crate) vlen: u32,
}

impl WalDel {
    pub fn new(prev_wid: u16, prev_cmd: u32, prev_txid: u64, klen: usize, vlen: usize) -> Self {
        Self {
            payload_type: PayloadType::Delete,
            prev_wid,
            prev_cmd,
            prev_txid,
            klen: klen as u32,
            vlen: vlen as u32,
        }
    }

    fn get(&self, pos: usize, len: usize) -> &[u8] {
        unsafe {
            let ptr = self as *const Self as *const u8;
            std::slice::from_raw_parts(ptr.add(pos), len)
        }
    }

    pub(crate) fn key(&self) -> &[u8] {
        self.get(size_of::<Self>(), self.klen as usize)
    }

    pub(crate) fn val(&self) -> &[u8] {
        self.get(size_of::<Self>() + self.klen as usize, self.vlen as usize)
    }
}

impl_codec!(WalDel);

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

pub(crate) struct WalFileReader {
    page_size: usize,
    file: File,
    pos: u64,
    eof: u64,
    wal: Vec<(u64, usize)>,
}

impl WalFileReader {
    pub(crate) fn new(path: PathBuf, wal_len: u64) -> Self {
        let file = match File::options().read(true).open(path) {
            Ok(f) => f,
            Err(e) => {
                log::error!("{}", e);
                panic!("can't open file {}", e);
            }
        };
        Self {
            page_size: AsyncIO::page_size(),
            file,
            pos: 0,
            eof: wal_len,
            wal: Vec::new(),
        }
    }

    pub(crate) fn seek_to_checkpoint(&mut self) {
        // checkpoint is not supported for now
    }

    fn inc_offset(&mut self, count: usize) {
        self.pos += count as u64;
    }

    fn valid(ty: EntryType, len: usize) -> bool {
        let sz = match ty {
            EntryType::Abort | EntryType::Begin | EntryType::Commit | EntryType::End => {
                WalAbort::size()
            }
            EntryType::Update => WalUpdate::size(),
            EntryType::Clr => WalCLR::size(),
            EntryType::Padding => WalPadding::size(),
            EntryType::CkptBeg => WalCkptBeg::size(),
            EntryType::CkptEnd => WalCkptEnd::size(),
            _ => unreachable!("invalid type {}", ty as u8),
        };
        len >= sz
    }

    pub fn apply<F>(&mut self, mut f: F)
    where
        F: FnMut(&WalUpdate),
    {
        let mut e = Block::alloc(4096);
        while let Some((pos, size)) = self.wal.pop() {
            if e.len() < size {
                e.realloc(size);
            }
            self.file
                .read_exact_at(e.get_mut_slice(0, size), pos)
                .expect("can't be wrong");
            let u = raw_ptr_to_ref(e.data().cast::<WalUpdate>());
            f(u);
        }
    }

    pub fn collect(&mut self, txn: u64) {
        // assume all entry headers size is less than 64
        let mut buf = [0u8; 64];
        while self.pos < self.eof {
            let rest = min((self.eof - self.pos) as usize, buf.len());
            let data = &mut buf[0..rest];
            if let Err(e) = self.file.read_exact_at(data, self.pos) {
                log::info!("read wal error {}", e);
                break;
            }
            let ty: EntryType = data[0].into();
            if !Self::valid(ty, data.len()) {
                break;
            }

            match ty {
                EntryType::Padding => {
                    let pad = ptr_to::<WalPadding>(data.as_mut_ptr());
                    self.inc_offset(pad.encoded_len() + pad.len as usize);
                }
                EntryType::Update => {
                    let u = ptr_to::<WalUpdate>(data.as_mut_ptr());
                    let len = u.encoded_len() + u.size as usize;
                    if u.tx_id != txn {
                        self.inc_offset(len);
                        continue;
                    }
                    let len = u.encoded_len() + u.size as usize;
                    self.wal.push((self.pos, len));
                    self.inc_offset(len);
                }
                EntryType::Clr => {
                    let c = ptr_to::<WalCLR>(data.as_mut_ptr());
                    self.inc_offset(c.encoded_len());
                }
                EntryType::Abort | EntryType::Begin | EntryType::Commit | EntryType::End => {
                    let w = ptr_to::<WalBegin>(data.as_mut_ptr());
                    self.inc_offset(size_of::<WalBegin>());
                }
                EntryType::CkptBeg => {
                    self.inc_offset(size_of::<WalCkptBeg>());
                }
                EntryType::CkptEnd => {
                    let c = ptr_to::<WalCkptEnd>(data.as_mut_ptr());
                    self.inc_offset(c.active_txn as usize * size_of::<u64>());
                }
                _ => panic!("invalid wal"),
            }
        }
    }
}

fn ptr_to<T>(x: *const u8) -> &'static T {
    unsafe { &*x.cast::<T>() }
}
