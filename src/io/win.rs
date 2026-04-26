use std::{
    io::{self, Write},
    os::windows::fs::{FileExt, OpenOptionsExt},
    path::Path,
};

use crate::io::{GatherIO, IoVec, OpenOptions};

const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
const ERROR_ACCESS_DENIED: i32 = 5;
const ERROR_INVALID_HANDLE: i32 = 6;
const ERROR_INVALID_FUNCTION: i32 = 1;
const ERROR_NOT_SUPPORTED: i32 = 50;

pub fn sync_dir<P: AsRef<Path>>(path: P) -> Result<(), io::Error> {
    let dir = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)?;
    match dir.sync_all() {
        Ok(()) => Ok(()),
        // windows does not provide a portable directory fsync, so once the directory
        // handle is validated we treat the unsupported flush error as a no-op
        Err(err)
            if matches!(
                err.raw_os_error(),
                Some(
                    ERROR_ACCESS_DENIED
                        | ERROR_INVALID_HANDLE
                        | ERROR_INVALID_FUNCTION
                        | ERROR_NOT_SUPPORTED
                )
            ) =>
        {
            Ok(())
        }
        Err(err) => Err(err),
    }
}

pub struct File {
    file: std::fs::File,
}

impl OpenOptions {
    pub fn open<P: AsRef<Path>>(&self, path: P) -> Result<File, io::Error> {
        File::open(path, self)
    }
}

impl File {
    pub fn options() -> OpenOptions {
        OpenOptions::new()
    }

    fn open<P: AsRef<Path>>(path: P, opt: &OpenOptions) -> Result<File, io::Error> {
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

    fn sync_data(&mut self) -> Result<(), io::Error> {
        self.file.sync_data()
    }

    fn truncate(&self, to: u64) -> Result<(), io::Error> {
        self.file.set_len(to)
    }

    fn size(&self) -> Result<u64, io::Error> {
        let m = self.file.metadata()?;
        Ok(m.len())
    }
}
