use std::{
    alloc::{Layout, alloc, dealloc},
    ptr::{NonNull, null_mut},
    sync::atomic::{
        AtomicU32,
        Ordering::{AcqRel, Relaxed},
    },
};

use crate::static_assert;

pub const CHUNK_SIZE: usize = 64 * 1024;
pub const CHUNK_MASK: usize = !(CHUNK_SIZE - 1);
pub const ALIGN: usize = 8;

pub struct Chunk {
    ptr: NonNull<u8>,
    offset: AtomicU32,
}

#[repr(C)]
pub(crate) struct ChunkHeader {
    refs: AtomicU32,
    _padding: u32,
}

static_assert!(size_of::<ChunkHeader>() == 8);

impl Chunk {
    pub fn new() -> Self {
        let layout = Layout::from_size_align(CHUNK_SIZE, CHUNK_SIZE).unwrap();
        let ptr = unsafe { alloc(layout) };
        let chunk = Self {
            ptr: unsafe { NonNull::new_unchecked(ptr) },
            offset: AtomicU32::new(size_of::<ChunkHeader>() as u32),
        };

        let header = chunk.header();
        header.refs.store(1, std::sync::atomic::Ordering::Relaxed); // 1 ref for Arena

        chunk
    }

    fn header(&self) -> &ChunkHeader {
        unsafe { &*(self.ptr.as_ptr() as *const ChunkHeader) }
    }

    pub fn alloc(&self, size: u32) -> *mut u8 {
        let aligned_size = (size + ALIGN as u32 - 1) & !(ALIGN as u32 - 1);

        loop {
            let current_offset = self.offset.load(Relaxed);
            if current_offset + aligned_size > CHUNK_SIZE as u32 {
                return null_mut();
            }

            if self
                .offset
                .compare_exchange(
                    current_offset,
                    current_offset + aligned_size,
                    Relaxed,
                    Relaxed,
                )
                .is_ok()
            {
                let ptr = unsafe { self.ptr.as_ptr().add(current_offset as usize) };
                self.header().refs.fetch_add(1, Relaxed);
                return ptr;
            }
        }
    }
}

impl Drop for Chunk {
    fn drop(&mut self) {
        unsafe { dec_ref(self.ptr.as_ptr()) };
    }
}

pub(crate) unsafe fn dec_ref(ptr: *mut u8) {
    let chunk_start = (ptr as usize & CHUNK_MASK) as *mut u8;
    let header = unsafe { &*(chunk_start as *const ChunkHeader) };

    if header.refs.fetch_sub(1, AcqRel) == 1 {
        let layout = Layout::from_size_align(CHUNK_SIZE, CHUNK_SIZE).unwrap();
        unsafe { dealloc(chunk_start, layout) };
    }
}
