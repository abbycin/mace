use crate::utils::align_up;
use crate::utils::{byte_array::ByteArray, raw_ptr_to_ref, raw_ptr_to_ref_mut};
use std::hash::Hasher;
use std::io::Write;
use std::{
    alloc::{alloc, dealloc, Layout},
    marker::PhantomData,
    ops::{Deref, DerefMut},
};

/// a normal frame contains data, a deleted frame is table of addresses (file id + offset) point to
/// deleted data, a tombstone is a single deleted data in current page file
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u32)]
pub enum FrameFlag {
    Normal = 1,
    Deleted = 2,
    TombStone = 3,
    Unknown,
}

/// NOTE: the data's heighest 8 bits are used as frame flag
#[repr(C)]
pub(crate) struct Frame {
    data: u64,
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
    }

    pub fn size(&self) -> u32 {
        Self::alloc_size(self.size as u32)
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
        assert_eq!(self.flag, FrameFlag::Normal);
        self.data = pid;
    }

    pub fn page_id(&self) -> u64 {
        self.data
    }

    pub fn is_normal(&self) -> bool {
        self.flag == FrameFlag::Normal
    }

    pub fn set_tombstone(&mut self) {
        self.flag = FrameFlag::TombStone;
    }

    pub fn is_tombstone(&self) -> bool {
        self.flag == FrameFlag::TombStone
    }

    pub fn set_delete(&mut self) {
        self.flag = FrameFlag::Deleted;
    }

    pub fn is_delete(&self) -> bool {
        self.flag == FrameFlag::Deleted
    }

    pub fn load_data<'a>(&mut self) -> Option<FrameRef<'a>> {
        match self.flag {
            FrameFlag::Deleted => {
                let sz = self.payload_size() as usize / size_of::<u64>();
                // test align
                assert_eq!(sz * size_of::<u64>(), self.payload_size() as usize);
                let ids = unsafe {
                    let ptr = (self as *mut Frame).offset(1).cast::<u64>();
                    std::slice::from_raw_parts_mut(ptr, sz)
                };

                Some(FrameRef::Dealloc(DeallocRef {
                    pages: ids,
                    page_idx: 0,
                }))
            }
            FrameFlag::Normal => {
                let ptr = unsafe { (self as *mut Frame).offset(1).cast::<u8>() };
                Some(FrameRef::Page(PageRef::new(
                    ptr,
                    self.payload_size() as usize,
                )))
            }
            _ => None,
        }
    }
}

pub(crate) struct FrameOwner {
    raw: *mut Frame,
    owned: bool,
}

impl FrameOwner {
    pub(crate) fn alloc(size: usize) -> Self {
        let real_size = Frame::alloc_size(size as u32);
        let mut this = Self {
            raw: unsafe { alloc(Layout::array::<u8>(real_size as usize).unwrap()) as *mut Frame },
            owned: true,
        };
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

pub enum FrameRef<'a> {
    Page(PageRef<'a>),
    Dealloc(DeallocRef<'a>),
}

impl<'a> FrameRef<'a> {
    pub fn to_page(&mut self) -> &mut PageRef<'a> {
        match self {
            FrameRef::Page(p) => p,
            _ => panic!("bad function call"),
        }
    }

    pub fn deleted_entries(&mut self) -> &mut [u64] {
        match self {
            FrameRef::Dealloc(x) => x.pages,
            _ => panic!("bad function all"),
        }
    }
}

pub struct PageRef<'a> {
    raw: ByteArray,
    _marker: PhantomData<&'a ()>,
}

pub struct DeallocRef<'a> {
    /// a list of disk addr
    pages: &'a mut [u64],
    /// current index in [`DeallocRef::pages`]
    page_idx: usize,
}

impl PageRef<'_> {
    fn new(ptr: *const u8, size: usize) -> Self {
        Self {
            raw: ByteArray::new(ptr as *mut u8, size),
            _marker: PhantomData,
        }
    }

    pub fn raw(&self) -> ByteArray {
        self.raw
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
