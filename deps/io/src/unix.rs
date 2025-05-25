use std::{
    ffi::CString,
    io,
    os::{raw::c_void, unix::ffi::OsStrExt},
    path::PathBuf,
};

use libc::{
    EINTR, O_APPEND, O_CREAT, O_RDONLY, O_RDWR, O_TRUNC, O_WRONLY, c_int, close, fstat, fsync,
    ftruncate, iovec, open, pread, stat, write, writev,
};

#[cfg(any(target_os = "freebsd", target_os = "macos"))]
use libc::__error;

#[cfg(target_os = "linux")]
use libc::__errno_location;

use crate::{GatherIO, OpenOptions};

pub struct File {
    file: i32,
}

impl OpenOptions {
    pub fn open(&self, path: &PathBuf) -> Result<File, io::Error> {
        let mut flag = O_RDONLY; // file is implicitly readable

        if self.write {
            flag |= O_WRONLY;
        }
        if self.read && self.write {
            flag = O_RDWR;
        }

        if self.trunc {
            flag |= O_TRUNC;
        } else if self.append {
            flag |= O_APPEND;
        }

        if self.create {
            flag |= O_CREAT;
        }

        File::open(path, flag)
    }
}

#[cfg(any(target_os = "freebsd", target_os = "macos"))]
#[inline]
fn errno() -> i32 {
    unsafe { *__error() }
}

#[cfg(target_os = "linux")]
#[inline]
fn errno() -> i32 {
    unsafe { *__errno_location() }
}

impl File {
    pub fn options() -> OpenOptions {
        OpenOptions::new()
    }

    pub fn open(path: &PathBuf, flag: c_int) -> Result<Self, io::Error> {
        let osstr = path.as_os_str();
        let c_string = CString::new(osstr.as_bytes()).expect("can't translate path to c string");
        let file = unsafe { open(c_string.as_ptr(), flag, 0o644) };
        if file < 0 {
            return Err(io::Error::from_raw_os_error(errno()));
        }
        Ok(Self { file })
    }
}

impl Drop for File {
    fn drop(&mut self) {
        unsafe {
            close(self.file);
        }
    }
}

impl GatherIO for File {
    fn read(&self, data: &mut [u8], mut pos: u64) -> Result<usize, std::io::Error> {
        let mut sz = 0;
        let end = data.len();
        while sz < end {
            let buf = &mut data[sz..];
            unsafe {
                let n = pread(
                    self.file,
                    buf.as_mut_ptr().cast::<c_void>(),
                    buf.len(),
                    pos as i64,
                );
                if n < 0 {
                    if errno() == EINTR {
                        continue;
                    }
                    return Err(io::Error::from_raw_os_error(errno()));
                }
                if n == 0 {
                    break;
                }
                pos += n as u64;
                sz += n as usize;
            }
        }

        Ok(sz as usize)
    }

    fn write(&mut self, data: &[u8]) -> Result<usize, io::Error> {
        loop {
            unsafe {
                let rc = write(self.file, data.as_ptr().cast::<c_void>(), data.len());
                if rc <= 0 {
                    if errno() == EINTR {
                        // the call was interrupted by a signal before any data was written
                        continue;
                    }
                    return Err(io::Error::from_raw_os_error(errno()));
                }
                debug_assert_eq!(rc as usize, data.len());
                return Ok(rc as usize);
            }
        }
    }

    fn writev(&mut self, data: &mut [crate::IoVec]) -> Result<(), io::Error> {
        let mut count = data.len() as c_int;
        let mut iov = data.as_mut_ptr().cast::<iovec>();

        while count > 0 {
            unsafe {
                let n = writev(self.file, iov, count);
                if n <= 0 {
                    if errno() == EINTR {
                        continue;
                    }
                    return Err(io::Error::from_raw_os_error(errno()));
                }

                let mut n = n as usize;
                while count > 0 && n >= (*iov).iov_len {
                    n -= (*iov).iov_len;
                    iov = iov.add(1);
                    count -= 1;
                }

                if count > 0 {
                    (*iov).iov_base = (*iov).iov_base.add(n);
                    (*iov).iov_len -= n;
                }
            }
        }
        Ok(())
    }

    fn sync(&mut self) -> Result<(), std::io::Error> {
        let rc = unsafe { fsync(self.file) };
        if rc < 0 {
            return Err(io::Error::from_raw_os_error(errno()));
        }
        Ok(())
    }

    fn size(&self) -> Result<u64, io::Error> {
        unsafe {
            let mut stat: stat = std::mem::zeroed();
            let rc = fstat(self.file, &mut stat);
            if rc < 0 {
                return Err(io::Error::from_raw_os_error(errno()));
            }
            Ok(stat.st_size as u64)
        }
    }

    fn truncate(&self, to: u64) -> Result<(), io::Error> {
        unsafe {
            let rc = ftruncate(self.file, to as i64);
            if rc < 0 {
                return Err(io::Error::from_raw_os_error(errno()));
            }
            Ok(())
        }
    }
}
