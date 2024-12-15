use crate::utils::align_up;
use crate::utils::{byte_array::ByteArray, raw_ptr_to_ref, raw_ptr_to_ref_mut};
use std::hash::Hasher;
use std::io::Write;
use std::{
    alloc::{alloc, dealloc, Layout},
    ops::{Deref, DerefMut},
};

/// a normal frame contains data, a deleted frame is table of addresses (file id + offset) point to
/// deleted data, a tombstone is a single deleted data in current page file
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u32)]
#[allow(unused)]
pub enum FrameFlag {
    Normal = 1,
    Deleted = 2,
    TombStone = 3,
    Slotted = 4,
    Unknown,
}

/// NOTE: the data's heighest 8 bits are used as frame flag
#[repr(C)]
pub(crate) struct Frame {
    pid: u64,
    /// encode_u64(file_id, offset)
    addr: u64,
    /// the size excluding [`Frame`] itself (i.e., it's payload size)
    size: u32,
    flag: FrameFlag,
}

impl Frame {
    pub(crate) const FRAME_LEN: usize = size_of::<Self>();

    pub fn alloc_size(request: u32) -> u32 {
        align_up(Self::FRAME_LEN + request as usize, size_of::<usize>()) as u32
    }

    /// NOTE: the `size` is payload size
    pub fn init(&mut self, addr: u64, flag: FrameFlag) {
        self.addr = addr;
        self.flag = flag;
        self.pid = 0;
    }

    pub fn size(&self) -> u32 {
        Self::alloc_size(self.size)
    }

    pub fn addr(&self) -> u64 {
        self.addr
    }

    pub fn payload_size(&self) -> u32 {
        self.size
    }

    pub fn set_size(&mut self, size: u32) {
        self.size = size;
    }

    pub fn set_pid(&mut self, pid: u64) {
        debug_assert_eq!(self.pid, 0);
        debug_assert!(self.flag == FrameFlag::Unknown || self.flag == FrameFlag::TombStone);
        self.pid = pid;
        self.flag = FrameFlag::Normal;
    }

    pub fn page_id(&self) -> u64 {
        self.pid
    }

    pub fn flag(&self) -> FrameFlag {
        self.flag
    }

    pub fn set_tombstone(&mut self) {
        self.flag = FrameFlag::TombStone;
    }

    pub fn set_delete(&mut self) {
        self.flag = FrameFlag::Deleted;
    }

    pub fn set_slotted(&mut self) {
        debug_assert_eq!(self.flag, FrameFlag::Unknown);
        self.flag = FrameFlag::Slotted;
    }

    pub fn load_data<'a>(&mut self) -> Option<&'a mut [u64]> {
        match self.flag {
            FrameFlag::Deleted => {
                let sz = self.payload_size() as usize / size_of::<u64>();
                // test align
                assert_eq!(sz * size_of::<u64>(), self.payload_size() as usize);
                let ids = unsafe {
                    let ptr = (self as *mut Frame).offset(1).cast::<u64>();
                    std::slice::from_raw_parts_mut(ptr, sz)
                };

                Some(ids)
            }
            _ => None,
        }
    }
}

pub(crate) struct FrameOwner {
    raw: *mut Frame,
    owned: bool,
}

unsafe impl Send for FrameOwner {}
unsafe impl Sync for FrameOwner {}

impl FrameOwner {
    pub(crate) fn alloc(size: usize) -> Self {
        let real_size = Frame::alloc_size(size as u32);
        let mut this = Self {
            raw: unsafe { alloc(Layout::array::<u8>(real_size as usize).unwrap()) as *mut Frame },
            owned: true,
        };
        this.flag = FrameFlag::Unknown;
        this.size = size as u32;
        this
    }

    pub(crate) fn shallow_copy(&self) -> Self {
        FrameOwner {
            raw: self.raw,
            owned: false,
        }
    }

    pub(crate) fn from(ptr: *mut Frame) -> Self {
        Self {
            raw: ptr,
            owned: false,
        }
    }

    pub(crate) fn data(&self) -> ByteArray {
        ByteArray::new(self.raw as *mut u8, self.size() as usize)
    }

    pub(crate) fn payload(&self) -> ByteArray {
        let ptr = unsafe { self.raw.add(1).cast::<u8>() };
        ByteArray::new(ptr, self.payload_size() as usize)
    }

    pub(crate) fn serialize<IO, H>(&self, file: &mut IO, hasher: &mut H)
    where
        IO: Write,
        H: Hasher,
    {
        let s = unsafe { std::slice::from_raw_parts(self.raw as *mut u8, self.size() as usize) };
        hasher.write(s);
        file.write_all(s).expect("can't write");
    }

    pub(crate) fn copy_to(&self, other: &Self) {
        unsafe {
            std::ptr::copy(
                self.raw as *mut u8,
                other.raw as *mut u8,
                self.size() as usize,
            );
        }
    }

    pub(crate) fn raw_mut(&self) -> &mut Frame {
        raw_ptr_to_ref_mut(self.raw)
    }
}

impl Deref for FrameOwner {
    type Target = Frame;
    fn deref(&self) -> &Self::Target {
        raw_ptr_to_ref(self.raw)
    }
}

impl DerefMut for FrameOwner {
    fn deref_mut(&mut self) -> &mut Self::Target {
        raw_ptr_to_ref_mut(self.raw)
    }
}

impl Drop for FrameOwner {
    fn drop(&mut self) {
        if self.owned {
            unsafe {
                dealloc(
                    self.raw as *mut u8,
                    Layout::array::<u8>(self.size() as usize).unwrap(),
                );
            }
        }
    }
}

pub struct FlushData {
    id: u32,
    data: ByteArray,
    cb: Box<dyn Fn()>,
}

impl FlushData {
    pub fn new(id: u32, data: ByteArray, cb: Box<dyn Fn()>) -> Self {
        Self { id, data, cb }
    }

    pub fn id(&self) -> u32 {
        self.id
    }

    pub fn mark_done(&self) {
        (self.cb)();
    }
}

unsafe impl Send for FlushData {}

impl Deref for FlushData {
    type Target = ByteArray;
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}
