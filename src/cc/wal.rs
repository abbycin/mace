#![allow(unused)]

use std::{
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

            fn encode_to(&self, b: ByteArray) {
                let src = to_slice(self);
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
    pub(crate) vlen: u32,
}

impl WalReplace {
    pub(crate) fn new(
        prev_wid: u16,
        prev_cmd: u32,
        prev_txid: u64,
        klen: usize,
        vlen: usize,
    ) -> Self {
        Self {
            payload_type: PayloadType::Update,
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

    pub(crate) fn wid(&self) -> u16 {
        self.prev_wid
    }

    pub(crate) fn key(&self) -> &[u8] {
        self.get(size_of::<Self>(), self.klen as usize)
    }

    pub(crate) fn val(&self) -> &[u8] {
        self.get(size_of::<Self>() + self.klen as usize, self.vlen as usize)
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

    fn encode_to(&self, b: ByteArray) {
        debug_assert_eq!(self.len(), b.len());
        let dst = b.as_mut_slice(0, b.len());
        dst.copy_from_slice(self);
    }

    fn to_slice(&self) -> &[u8] {
        self
    }
}

fn to_slice<T>(x: &T) -> &[u8] {
    unsafe {
        let ptr = x as *const T as *const u8;
        std::slice::from_raw_parts(ptr, size_of::<T>())
    }
}

pub(crate) trait IWalReader {
    fn ok(&self) -> bool;

    fn offset(&self) -> u64;

    fn inc_offset(&mut self, count: usize);

    fn len(&self) -> u64;

    fn read_at(&mut self, buf: &mut [u8], off: u64) -> io::Result<()>;

    fn apply_update<F>(&mut self, off: u64, len: usize, f: F)
    where
        F: FnMut(&WalUpdate);

    fn read_txn<F>(&mut self, txn: u64, mut f: F)
    where
        F: FnMut(&WalUpdate),
    {
        // assume all entry headers size is less than 64
        let mut buf = [0u8; 64];
        while self.ok() {
            if let Err(e) = self.read_at(&mut buf, self.offset()) {
                log::info!("read wal error {}", e);
                break;
            }
            let ty: EntryType = buf[0].into();
            match ty {
                EntryType::Padding => {
                    let pad = ptr_to::<WalPadding>(buf.as_mut_ptr());
                    self.inc_offset(pad.encoded_len() + pad.len as usize);
                }
                EntryType::Update => {
                    let u = ptr_to::<WalUpdate>(buf.as_mut_ptr());
                    let len = u.encoded_len() + u.size as usize;
                    if u.tx_id != txn {
                        self.inc_offset(len);
                        continue;
                    }
                    let len = u.encoded_len() + u.size as usize;
                    self.apply_update(self.offset(), len, &mut f);
                    self.inc_offset(len);
                }
                EntryType::Clr => {
                    let c = ptr_to::<WalCLR>(buf.as_mut_ptr());
                    self.inc_offset(c.encoded_len());
                }
                EntryType::Abort | EntryType::Begin | EntryType::Commit | EntryType::End => {
                    let w = ptr_to::<WalBegin>(buf.as_mut_ptr());
                    self.inc_offset(size_of::<WalBegin>());
                }
                EntryType::CkptBeg => {
                    self.inc_offset(size_of::<WalCkptBeg>());
                }
                EntryType::CkptEnd => {
                    let c = ptr_to::<WalCkptEnd>(buf.as_mut_ptr());
                    self.inc_offset(c.active_txn as usize * size_of::<u64>());
                }
                _ => panic!("invalid wal"),
            }
        }
    }
}

pub(crate) struct WalBufReader {
    data: *mut u8,
    pos: usize,
    end: usize,
}

impl WalBufReader {
    pub(crate) fn new(data: *mut u8, beg: usize, end: usize) -> Self {
        Self {
            data,
            pos: beg,
            end,
        }
    }
}

impl IWalReader for WalBufReader {
    fn ok(&self) -> bool {
        self.pos != self.end
    }

    fn offset(&self) -> u64 {
        self.pos as u64
    }

    fn inc_offset(&mut self, count: usize) {
        self.pos += count;
    }

    fn len(&self) -> u64 {
        (self.end - self.pos) as u64
    }

    fn read_at(&mut self, buf: &mut [u8], off: u64) -> io::Result<()> {
        if buf.len() + off as usize > self.end {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "end of buffer",
            ));
        }
        let src = unsafe { std::slice::from_raw_parts(self.data.add(off as usize), buf.len()) };
        buf.copy_from_slice(src);
        Ok(())
    }

    fn apply_update<F>(&mut self, off: u64, len: usize, mut f: F)
    where
        F: FnMut(&WalUpdate),
    {
        debug_assert!(off as usize + len <= self.end);
        let ptr = unsafe { self.data.add(off as usize).cast::<WalUpdate>() };
        let u = raw_ptr_to_ref(ptr);
        f(u);
    }
}

pub(crate) struct WalFileReader {
    page_size: usize,
    // file: c_int,
    file: File,
    pos: u64,
    eof: u64,
}

impl WalFileReader {
    pub(crate) fn new(path: PathBuf, wal_len: u64) -> Self {
        // let file = AsyncIO::fopen(path, false);
        let file = File::options().read(true).open(path).unwrap();
        Self {
            page_size: AsyncIO::page_size(),
            file,
            pos: 0,
            eof: wal_len,
        }
    }

    pub(crate) fn seek_to_checkpoint(&mut self) {
        // checkpoint is not supported for now
    }
}

impl IWalReader for WalFileReader {
    fn ok(&self) -> bool {
        self.offset() < self.len()
    }

    fn offset(&self) -> u64 {
        self.pos
    }

    fn inc_offset(&mut self, count: usize) {
        self.pos += count as u64;
    }

    fn len(&self) -> u64 {
        self.eof
    }

    fn read_at(&mut self, buf: &mut [u8], off: u64) -> io::Result<()> {
        // AsyncIO::fread(self.file, buf, off)
        self.file.read_exact_at(buf, off)
    }

    fn apply_update<F>(&mut self, off: u64, len: usize, mut f: F)
    where
        F: FnMut(&WalUpdate),
    {
        // let e = Block::aligned_alloc(len, self.page_size);
        let mut e = Block::alloc(len);
        self.read_at(e.get_mut_slice(0, len), off)
            .expect("can't be wrong");
        let u = raw_ptr_to_ref(e.data().cast::<WalUpdate>());
        f(u);
    }
}

fn ptr_to<T>(x: *const u8) -> &'static T {
    unsafe { &*x.cast::<T>() }
}
