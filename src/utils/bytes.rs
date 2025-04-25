use std::alloc::{alloc, dealloc, Layout};
use std::ptr;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ByteArray {
    data: *mut u8,
    size: usize,
}

unsafe impl Send for ByteArray {}

impl ByteArray {
    pub fn alloc(size: usize) -> Self {
        let ptr = unsafe { alloc(Layout::from_size_align(size, size_of::<*const ()>()).unwrap()) };
        Self::new(ptr, size)
    }

    pub fn new(data: *mut u8, size: usize) -> Self {
        Self { data, size }
    }

    pub fn free(b: Self) {
        unsafe {
            dealloc(
                b.data,
                Layout::from_size_align(b.size, size_of::<*const ()>()).unwrap(),
            );
        }
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn data(&self) -> *mut u8 {
        self.data
    }

    pub fn write<T>(&self, pos: usize, val: T) {
        unsafe {
            self.data.add(pos).cast::<T>().write_unaligned(val);
        }
    }

    pub fn read<T>(&self, pos: usize) -> T {
        unsafe { self.data.add(pos).cast::<T>().read_unaligned() }
    }

    #[allow(unused)]
    pub fn add(&self, n: usize) -> Self {
        debug_assert!(n <= self.size);
        ByteArray::new(unsafe { self.data.add(n) }, self.size - n)
    }

    pub fn sub_array(&self, off: usize, len: usize) -> Self {
        debug_assert!(off + len <= self.size);
        unsafe { Self::new(self.data.add(off), len) }
    }

    pub fn skip(&self, off: usize) -> Self {
        debug_assert!(off <= self.size);
        unsafe { Self::new(self.data.add(off), self.size - off) }
    }

    pub fn as_slice<'a, T>(&self, off: usize, cnt: usize) -> &'a [T] {
        debug_assert!(off + cnt <= self.size);
        unsafe { std::slice::from_raw_parts(self.data.add(off) as *const T, cnt) }
    }

    pub fn as_mut_slice<'a, T>(&self, off: usize, cnt: usize) -> &'a mut [T] {
        debug_assert!(off + cnt <= self.size);
        unsafe { std::slice::from_raw_parts_mut(self.data.add(off) as *mut T, cnt) }
    }
}

impl Default for ByteArray {
    fn default() -> Self {
        Self {
            data: ptr::null_mut(),
            size: 0,
        }
    }
}
