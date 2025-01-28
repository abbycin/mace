use std::io;

pub struct IoVec {
    pub data: *const u8,
    pub len: usize,
}

impl IoVec {
    pub fn new(data: *const u8, len: usize) -> Self {
        Self { data, len }
    }
}

pub trait SeekableGatherIO {
    fn read(&self, data: &mut [u8], pos: u64) -> Result<(), io::Error>;

    fn write(&mut self, data: &mut [IoVec]) -> Result<(), io::Error>;

    fn flush(&mut self) -> Result<(), io::Error>;

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

    pub fn append(&mut self, on: bool) -> &mut Self {
        self.append = on;
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
