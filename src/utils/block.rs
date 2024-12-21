use std::alloc::{alloc, dealloc, realloc, Layout};

use super::{align_up, byte_array::ByteArray};

pub(crate) struct Block {
    data: *mut u8,
    refs: *mut u32,
    len: u32,
    align: u32,
}

impl Block {
    fn get_ref(x: *mut u32) -> &'static mut u32 {
        unsafe { &mut *x.cast::<u32>() }
    }

    fn alloc_impl(len: usize, align: usize, layout: Layout) -> Self {
        let data = unsafe { alloc(layout) };
        let refs = unsafe { alloc(Layout::new::<u32>()).cast::<u32>() };
        *Self::get_ref(refs) = 1;
        Self {
            data,
            refs,
            len: len as u32,
            align: align as u32,
        }
    }

    pub(crate) fn alloc(size: usize) -> Self {
        let layout = Layout::array::<u8>(size).expect("bad layout");
        Self::alloc_impl(size, 0, layout)
    }

    pub(crate) fn aligned_alloc(size: usize, align: usize) -> Self {
        let len = align_up(size, align);
        let layout = Layout::from_size_align(len, align).expect("bad layout");
        Self::alloc_impl(size, align, layout)
    }

    #[allow(unused)]
    pub(crate) fn zero(&self) {
        let len = if self.align == 0 {
            self.len as usize
        } else {
            self.aligned_len()
        };
        unsafe {
            self.data.write_bytes(0, len);
        }
    }

    pub(crate) fn data(&self) -> *mut u8 {
        self.data
    }

    pub(crate) fn len(&self) -> usize {
        self.len as usize
    }

    pub(crate) fn aligned_len(&self) -> usize {
        align_up(self.len as usize, self.align as usize)
    }

    pub(crate) fn view(&self, from: usize, to: usize) -> ByteArray {
        debug_assert!(from <= to);
        debug_assert!(to <= self.len as usize);
        ByteArray::new(unsafe { self.data.add(from) }, to - from)
    }

    pub(crate) fn get_mut_slice(&mut self, off: usize, len: usize) -> &mut [u8] {
        debug_assert!(len <= self.len as usize);

        unsafe { std::slice::from_raw_parts_mut(self.data.add(off), len) }
    }

    pub(crate) fn realloc(&mut self, size: usize) {
        assert_eq!(self.align, 0);
        self.data = unsafe {
            let layout = Layout::array::<u8>(self.len as usize).unwrap();
            realloc(self.data, layout, size)
        };
        self.len = size as u32;
    }
}

impl Clone for Block {
    fn clone(&self) -> Self {
        *Self::get_ref(self.refs) += 1;
        Self {
            data: self.data,
            refs: self.refs,
            len: self.len,
            align: self.align,
        }
    }
}

impl Drop for Block {
    fn drop(&mut self) {
        *Self::get_ref(self.refs) -= 1;
        if *Self::get_ref(self.refs) == 0 {
            if self.align == 0 {
                unsafe {
                    dealloc(self.data, Layout::array::<u8>(self.len as usize).unwrap());
                }
            } else {
                let align = self.align as usize;
                unsafe {
                    dealloc(
                        self.data,
                        Layout::from_size_align(self.aligned_len(), align).expect("bad layout"),
                    );
                }
            }

            unsafe {
                dealloc(self.refs.cast::<u8>(), Layout::new::<u8>());
            }
        }
    }
}
