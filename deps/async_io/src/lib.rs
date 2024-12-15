#![allow(non_snake_case, non_camel_case_types, non_upper_case_globals)]
include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

use std::{
    ffi::{c_int, CString},
    io,
    os::{raw::c_void, unix::ffi::OsStrExt},
    path::PathBuf,
};

pub struct AsyncIO {
    aio: *mut aio_ctx,
}

unsafe impl Send for AsyncIO {}
unsafe impl Sync for AsyncIO {}

impl AsyncIO {
    pub const SECTOR_SIZE: usize = 512;
    pub const BLOCK_SIZE: usize = 4096;

    pub fn new(max_req: usize) -> Self {
        Self {
            aio: unsafe { aio_init(max_req) },
        }
    }

    pub fn page_size() -> usize {
        unsafe { page_size() }
    }

    pub fn fopen(path: PathBuf, direct: bool, trunc: bool) -> c_int {
        let osstr = path.as_os_str();
        let c_string = CString::new(osstr.as_bytes()).expect("can't translate path to c string");
        unsafe { file_open(c_string.as_ptr(), direct, trunc) }
    }

    pub fn last_error() -> i32 {
        unsafe { last_error() }
    }

    pub fn fread(fd: c_int, buf: &mut [u8], off: u64) -> Result<(), io::Error> {
        let rc = unsafe { file_read(fd, buf.as_mut_ptr() as *mut c_void, buf.len(), off) };
        if rc < 0 {
            return Err(io::Error::from_raw_os_error(Self::last_error()));
        }
        if (rc as usize) < buf.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "end of file"));
        }
        Ok(())
    }

    pub fn fdsync(fd: c_int) -> c_int {
        unsafe { file_fdsync(fd) }
    }

    pub fn fsize(fd: c_int) -> u64 {
        unsafe { file_size(fd) }
    }

    pub fn fclose(fd: c_int) {
        unsafe {
            file_close(fd);
        }
    }

    pub fn prepare_read(&self, fd: c_int, buf: &mut [u8], off: u64) {
        unsafe {
            aio_prepare_read(
                self.aio,
                fd,
                buf.as_mut_ptr() as *mut c_void,
                buf.len(),
                off,
            );
        }
    }

    pub fn prepare_write(&self, fd: c_int, buf: *mut u8, count: usize, off: u64) {
        unsafe {
            aio_prepare_write(self.aio, fd, buf as *mut c_void, count, off);
        }
    }

    pub fn sumbit(&self) -> c_int {
        unsafe { aio_submit(self.aio) }
    }

    pub fn wait(&self, timeout_ms: u64) -> c_int {
        unsafe { aio_wait(self.aio, timeout_ms) }
    }

    pub fn fsync(&self, fd: c_int) {
        unsafe {
            aio_fsync(self.aio, fd);
        }
    }

    pub fn is_full(&self) -> bool {
        unsafe { aio_full(self.aio) }
    }

    pub fn is_empty(&self) -> bool {
        unsafe { aio_empty(self.aio) }
    }

    pub fn pending(&self) -> usize {
        unsafe { aio_pending(self.aio) }
    }
}

impl Drop for AsyncIO {
    fn drop(&mut self) {
        unsafe {
            aio_destroy(self.aio);
        }
    }
}
