use std::alloc::{alloc, dealloc, Layout};
use std::ptr;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ByteArray {
    data: *mut u8,
    size: usize,
}

impl ByteArray {
    pub fn alloc(size: usize) -> Self {
        let ptr = unsafe { alloc(Layout::array::<u8>(size).unwrap()) };
        Self::new(ptr, size)
    }

    pub fn new(data: *mut u8, size: usize) -> Self {
        Self { data, size }
    }

    pub fn free(b: Self) {
        unsafe {
            dealloc(b.data, Layout::array::<u8>(b.size).unwrap());
        }
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn data(&self) -> *mut u8 {
        self.data
    }

    #[allow(dead_code)]
    pub fn reset(&mut self) {
        self.data = std::ptr::null_mut();
        self.size = 0;
    }

    #[allow(dead_code)]
    pub fn is_null(&self) -> bool {
        self.data.is_null()
    }

    pub fn add(&self, n: usize) -> Self {
        debug_assert!(n <= self.size);
        ByteArray::new(unsafe { self.data.add(n) }, self.size - n)
    }

    pub fn sub_array(&self, off: usize, len: usize) -> Self {
        debug_assert!(off + len <= self.size);
        unsafe { Self::new(self.data.add(off), len) }
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

#[cfg(test)]
mod test {
    use crate::utils::byte_array::ByteArray;
    use std::alloc::{alloc, dealloc, Layout};

    #[test]
    fn test_byte_array() {
        let size = 32;
        let b = unsafe {
            let ptr = alloc(Layout::array::<u8>(size).unwrap());
            ByteArray::new(ptr, size)
        };

        let mut a = b;
        a.reset();

        assert!(a.is_null());
        assert!(!b.is_null());

        unsafe {
            dealloc(b.data(), Layout::array::<u8>(size).unwrap());
        }
    }
}
