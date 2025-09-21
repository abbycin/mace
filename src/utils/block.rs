use std::alloc::{Layout, alloc, dealloc, realloc};

use super::align_up;

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
        let layout = Layout::array::<*const ()>(size).expect("bad layout");
        Self::alloc_impl(size, 0, layout)
    }

    pub(crate) fn aligned_alloc(size: usize, align: usize) -> Self {
        let len = align_up(size, align);
        let layout = Layout::from_size_align(len, align).expect("bad layout");
        Self::alloc_impl(size, align, layout)
    }

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

    pub(crate) fn mut_slice<'a>(&self, off: usize, len: usize) -> &'a mut [u8] {
        debug_assert!(len <= self.len as usize);

        unsafe { std::slice::from_raw_parts_mut(self.data.add(off), len) }
    }

    pub(crate) fn slice<'a>(&self, off: usize, len: usize) -> &'a [u8] {
        debug_assert!(len <= self.len as usize);

        unsafe { std::slice::from_raw_parts(self.data.add(off), len) }
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
                    dealloc(
                        self.data,
                        Layout::array::<*const ()>(self.len as usize).unwrap(),
                    );
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
                dealloc(self.refs.cast::<u8>(), Layout::new::<u32>());
            }
        }
    }
}

pub(crate) struct Ring {
    data: Block,
    head: usize,
    tail: usize,
}

impl Ring {
    pub(crate) fn new(cap: usize) -> Self {
        let data = Block::aligned_alloc(cap, 1);
        data.zero();
        assert!(data.len().is_power_of_two());
        Self {
            data,
            head: 0,
            tail: 0,
        }
    }

    pub(crate) fn avail(&self) -> usize {
        self.data.len() - self.distance()
    }

    // NOTE: the request buffer never wraps around
    pub(crate) fn prod<'a>(&mut self, size: usize) -> &'a mut [u8] {
        debug_assert!(self.avail() >= size);
        let mut b = self.tail;
        self.tail += size;

        b &= self.mask();
        self.data.mut_slice(b, size)
    }

    pub(crate) fn cons(&mut self, pos: usize) {
        self.head += pos;
    }

    pub(crate) fn distance(&self) -> usize {
        #[cfg(feature = "extra_check")]
        assert!(self.tail >= self.head);
        self.tail - self.head
    }

    pub(crate) fn head(&self) -> usize {
        self.head & self.mask()
    }

    pub(crate) fn tail(&self) -> usize {
        self.tail & self.mask()
    }

    pub(crate) fn slice(&self, pos: usize, len: usize) -> &[u8] {
        self.data.slice(pos, len)
    }

    pub(crate) fn mask(&self) -> usize {
        self.data.len() - 1
    }

    pub(crate) fn len(&self) -> usize {
        self.data.len()
    }
}
