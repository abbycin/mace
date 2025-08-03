use std::io;

#[repr(C)]
pub struct IoVec {
    pub data: *const u8,
    pub len: usize,
}

impl IoVec {
    pub const fn new(data: *const u8, len: usize) -> Self {
        Self { data, len }
    }
}

impl From<&[u8]> for IoVec {
    fn from(value: &[u8]) -> Self {
        unsafe { std::mem::transmute(value) }
    }
}

pub trait GatherIO {
    fn read(&self, data: &mut [u8], pos: u64) -> Result<usize, io::Error>;

    fn write(&mut self, data: &[u8]) -> Result<usize, io::Error>;

    fn writev(&mut self, data: &mut [IoVec], total_len: usize) -> Result<(), io::Error>;

    fn sync(&mut self) -> Result<(), io::Error>;

    fn truncate(&self, to: u64) -> Result<(), io::Error>;

    fn size(&self) -> Result<u64, io::Error>;
}

pub struct OpenOptions {
    read: bool,
    write: bool,
    append: bool,
    create: bool,
    trunc: bool,
}

impl OpenOptions {
    fn new() -> Self {
        Self {
            read: false,
            write: false,
            append: false,
            create: false,
            trunc: false,
        }
    }

    pub fn read(&mut self, on: bool) -> &mut Self {
        self.read = on;
        self
    }

    pub fn write(&mut self, on: bool) -> &mut Self {
        self.write = on;
        self
    }

    // when truc is enabled, append will be ignored
    pub fn append(&mut self, on: bool) -> &mut Self {
        if !self.trunc {
            self.append = on;
        }
        self
    }

    pub fn create(&mut self, on: bool) -> &mut Self {
        self.create = on;
        self
    }

    pub fn trunc(&mut self, on: bool) -> &mut Self {
        self.trunc = on;
        self
    }
}

#[cfg(windows)]
pub mod win;
#[cfg(windows)]
pub use win::File;

#[cfg(unix)]
pub mod unix;
#[cfg(unix)]
pub use unix::File;
