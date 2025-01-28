use io::{File, IoVec, SeekableGatherIO};

#[test]
fn read_write() {
    let path = std::env::temp_dir().join("foo_bar");
    println!("path {:?}", path);
    let mut f = File::options()
        .write(true)
        .read(true)
        .append(true)
        .create(true)
        .open(&path)
        .unwrap();
    let mut v = vec![
        IoVec::new("foo".as_ptr().cast::<u8>(), 3),
        IoVec::new("bar".as_ptr().cast::<u8>(), 3),
    ];

    f.write(v.as_mut_slice()).unwrap();

    f.flush().unwrap();
}
