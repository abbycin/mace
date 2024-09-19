#[repr(C)]
#[derive(Clone, Copy)]
pub struct ByteArray {
    data: *mut u8,
    size: usize,
}

impl ByteArray {
    pub fn new(data: *mut u8, size: usize) -> Self {
        Self { data, size }
    }

    pub fn offset(&self, off: isize) -> *mut u8 {
        unsafe { self.data.offset(off) }
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn data(&self) -> *mut u8 {
        self.data
    }

    pub fn reset(&mut self) {
        self.data = std::ptr::null_mut();
    }

    pub fn is_null(&self) -> bool {
        self.data.is_null()
    }

    pub fn copy(&mut self, data: &[u8]) {
        unsafe {
            self.data.copy_from(data.as_ptr(), data.len());
        }
    }

    pub fn to_slice<T>(&self, off: isize, cnt: usize) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.data.offset(off) as *const T, cnt) }
    }

    pub fn to_mut_slice<T>(&self, off: isize, cnt: usize) -> &mut [T] {
        unsafe { std::slice::from_raw_parts_mut(self.data.offset(off) as *mut T, cnt) }
    }
}
