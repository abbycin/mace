use std::collections::HashMap;
use std::hash::Hasher;
use std::io::{Seek, Write};
use std::ops::{Deref, DerefMut};
use std::os::unix::fs::FileExt;

pub struct PageFooter {
    pub checksum: u64,
}

impl PageFooter {
    pub fn serialize<IO>(&self, f: &mut IO)
    where
        IO: Write,
    {
        f.write_all(&self.checksum.to_le_bytes())
            .expect("can't write page footer");
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct MapFooter {
    pub file_id: u32,
    pub nr_active: u32,
    pub nr_deleted: u32,
    pub nr_mapping: u32,
    pub checksum: u64,
}

impl MapFooter {
    const ACTIVE_ITEM_LEN: usize = size_of::<AddrMap>();

    pub fn active_delta_size(&self) -> usize {
        Self::ACTIVE_ITEM_LEN * self.nr_active as usize
    }

    pub fn serialize<IO>(&self, f: &mut IO)
    where
        IO: Write,
    {
        let s = unsafe {
            std::slice::from_raw_parts(self as *const MapFooter as *const u8, size_of::<Self>())
        };
        f.write_all(s).expect("can't write map footer");
    }
}

#[derive(Clone, Copy, Debug)]
#[repr(C, align(4))]
pub struct RelocMap {
    /// frame offset in page file
    pub(crate) off: u32,
    /// frame's payload length
    pub(crate) len: u32,
}

#[repr(C, align(4))]
pub struct AddrMap {
    /// offset in page map's address
    pub(crate) key: u32,
    pub(crate) val: RelocMap,
}

impl AddrMap {
    pub fn new(key: u32, off: u32, len: u32) -> Self {
        Self {
            key,
            val: RelocMap { off, len },
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe {
            let p = self as *const Self as *const u8;
            std::slice::from_raw_parts(p, size_of::<Self>())
        }
    }
}

pub struct MapEntry {
    page_id: u64,
    /// (file_id << 32) | arena_offset
    page_addr: u64,
}

impl MapEntry {
    fn as_slice(&self) -> &[u8] {
        unsafe {
            let p = self as *const MapEntry as *const u8;
            std::slice::from_raw_parts(p, size_of::<Self>())
        }
    }

    fn from_slice(x: &[u8]) -> &Self {
        unsafe { &*(x.as_ptr() as *const Self) }
    }

    pub fn page_id(&self) -> u64 {
        self.page_id
    }

    pub fn page_addr(&self) -> u64 {
        self.page_addr
    }
}

#[derive(Default)]
pub struct PageTable {
    // pid, gsn, addr, len(offset + len)
    data: HashMap<u64, MapEntry>,
}

impl PageTable {
    pub const ITEM_LEN: usize = size_of::<MapEntry>();

    pub fn serialize<F, H>(&self, file: &mut F, hasher: &mut H) -> usize
    where
        F: Write + FileExt,
        H: Hasher,
    {
        let mut buf = Vec::new();

        self.data
            .values()
            .map(|e| {
                buf.extend_from_slice(e.as_slice());
            })
            .count();
        hasher.write(buf.as_slice());
        file.write_all(buf.as_slice())
            .expect("can't write page table");
        buf.len()
    }

    pub fn add(&mut self, pid: u64, addr: u64) {
        if let Some(e) = self.get_mut(&pid) {
            // a delta chain in same arana, we use the latest mapping
            // NOTE: it's incorrect when addr was wrapped, but it's almost never happen in our spec
            if e.page_addr < addr {
                e.page_addr = addr;
            }
        } else {
            self.insert(
                pid,
                MapEntry {
                    page_id: pid,
                    page_addr: addr,
                },
            );
        }
    }

    /// NOTE: the file must be mapping file
    pub fn deserialize<F, IO>(file: &mut IO, mut f: F)
    where
        F: FnMut(&MapEntry),
        IO: FileExt + Seek,
    {
        let mut buf = [0u8; size_of::<MapFooter>()];
        let file_size = file.seek(std::io::SeekFrom::End(0)).unwrap();
        let mut off = file_size - buf.len() as u64;
        file.read_at(&mut buf, off).expect("can't read");
        let footer = unsafe { &*(buf.as_ptr() as *const MapFooter) };

        let mut data = vec![0u8; PageTable::ITEM_LEN * footer.nr_mapping as usize];
        off -= data.len() as u64;
        file.read_at(&mut data, off).expect("can't read");
        let chunks = data.chunks(Self::ITEM_LEN);

        for c in chunks {
            f(MapEntry::from_slice(c));
        }
    }
}

impl Deref for PageTable {
    type Target = HashMap<u64, MapEntry>;
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl DerefMut for PageTable {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

#[cfg(test)]
mod test {
    use std::{
        hash::Hasher,
        io::{Seek, SeekFrom, Write},
        os::unix::fs::FileExt,
    };

    use crate::{
        index::{
            data::{Key, Value},
            page::{DeltaType, NodeType, Page},
            Delta,
        },
        map::data::{FrameFlag, FrameOwner},
        utils::{data::MapFooter, decode_u64},
    };

    use super::PageTable;

    #[derive(Default)]
    struct FakeFile {
        data: Vec<u8>,
        off: u64,
    }

    impl FileExt for FakeFile {
        fn read_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
            let off = offset as usize;
            let s = self.data.as_slice();
            buf.copy_from_slice(&s[off..(off + buf.len())]);
            Ok(buf.len())
        }

        fn write_at(&self, _buf: &[u8], _offset: u64) -> std::io::Result<usize> {
            unimplemented!()
        }
    }

    impl Write for FakeFile {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.data.extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            unimplemented!()
        }
    }

    impl Seek for FakeFile {
        fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
            match pos {
                SeekFrom::Current(x) => self.off += x as u64,
                SeekFrom::Start(x) => self.off = x,
                SeekFrom::End(x) => self.off = self.data.len() as u64 - x as u64,
            }

            Ok(self.off)
        }
    }

    #[derive(Default)]
    struct FakeHasher;
    impl Hasher for FakeHasher {
        fn write(&mut self, _buf: &[u8]) {}
        fn finish(&self) -> u64 {
            unimplemented!()
        }
    }

    #[test]
    fn test_page_table() {
        let (pid, addr, gsn) = (114, 514, 19268);

        let (k, v) = ("key", "val");
        let mut delta = Delta::new(DeltaType::Data, NodeType::Leaf)
            .with_item((Key::new(k.as_bytes(), gsn, 0), Value::Put(v.as_bytes())));
        let alloc_size = delta.size();

        let mut f = FrameOwner::alloc(alloc_size);
        f.init(addr, FrameFlag::Unknown);
        f.set_pid(pid);

        assert_eq!(delta.size(), f.payload_size() as usize);

        let mut page = Page::from(f.payload());
        delta.build(&mut page);
        let h = page.header_mut();
        h.meta.set_epoch(666);
        h.set_link(addr);

        let mut table = PageTable::default();
        let mut file = FakeFile::default();
        let mut hasher = FakeHasher;

        let mut buf = [0u8; size_of::<MapFooter>()];
        let ft = unsafe { &mut *(buf.as_mut_ptr() as *mut MapFooter) };
        let (file_id, _) = decode_u64(addr);
        ft.checksum = 1;
        ft.nr_active = 1;
        ft.nr_deleted = 0;
        ft.file_id = file_id;
        ft.nr_mapping = 1;

        table.add(pid, addr);
        table.serialize(&mut file, &mut hasher);

        ft.serialize(&mut file);

        let (mut d_pid, mut d_addr) = (0, 0);
        PageTable::deserialize(&mut file, |e| {
            d_pid = e.page_id();
            d_addr = e.page_addr();
        });

        assert_eq!(pid, d_pid);
        assert_eq!(addr, d_addr);
    }
}
