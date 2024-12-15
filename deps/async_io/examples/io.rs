use std::{
    alloc::{alloc, dealloc, Layout},
    path::Path,
};

use async_io::AsyncIO;

fn main() {
    let aio = AsyncIO::new(10);
    let f = AsyncIO::create_file(Path::new("/tmp/").join("aio.xx"));

    let len = 4096;
    let buf = unsafe { alloc(Layout::from_size_align(len, len).unwrap()) };
    let s = unsafe { std::slice::from_raw_parts_mut(buf, len) };

    for i in 0..len {
        s[i] = 106;
    }

    aio.prepare_write(f, &s, 0);
    aio.sumbit();
    aio.wait();

    unsafe {
        dealloc(buf, Layout::from_size_align(len, len).unwrap());
    }
}
