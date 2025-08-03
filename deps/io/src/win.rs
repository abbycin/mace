use std::{
    io::{self, Write},
    os::windows::fs::FileExt,
    path::PathBuf,
};

use crate::{GatherIO, IoVec, OpenOptions};

pub struct File {
    file: std::fs::File,
}

impl OpenOptions {
    pub fn open(&self, path: &PathBuf) -> Result<File, io::Error> {
        File::open(path, self)
    }
}

impl File {
    pub fn options() -> OpenOptions {
        OpenOptions::new()
    }

    fn open(path: &PathBuf, opt: &OpenOptions) -> Result<File, io::Error> {
        let file = std::fs::File::options()
            .read(opt.read)
            .write(opt.write)
            .append(opt.append)
            .truncate(opt.trunc)
            .create(opt.write)
            .open(path)?;
        Ok(Self { file })
    }
}

impl GatherIO for File {
    fn read(&self, data: &mut [u8], pos: u64) -> Result<usize, io::Error> {
        let mut n = 0;
        let sz = data.len();
        let mut off = pos;
        while n < sz {
            let s = &mut data[n..sz];
            let nbytes = self.file.seek_read(s, off)?;
            if nbytes == 0 {
                break;
            }
            n += nbytes;
            off += nbytes as u64;
        }
        Ok(n)
    }

    fn write(&mut self, data: &[u8]) -> Result<usize, io::Error> {
        self.file.write_all(data).map(|_| data.len())
    }

    fn writev(&mut self, iov: &mut [IoVec], _total_len: usize) -> Result<(), io::Error> {
        for x in iov {
            let s = unsafe { std::slice::from_raw_parts(x.data, x.len) };
            self.file.write_all(s)?;
        }
        Ok(())
    }

    fn sync(&mut self) -> Result<(), io::Error> {
        self.file.sync_all()
    }

    fn truncate(&self, to: u64) -> Result<(), io::Error> {
        self.file.set_len(to)
    }

    fn size(&self) -> Result<u64, io::Error> {
        let m = self.file.metadata()?;
        Ok(m.len())
    }
}
