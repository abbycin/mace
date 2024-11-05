use std::{
    collections::HashMap,
    fs::File,
    io::{Seek, SeekFrom},
    os::unix::fs::FileExt,
    sync::Arc,
};

use super::data::FrameOwner;
use crate::{
    utils::data::{AddrMap, MapFooter, RelocMap},
    Options,
};

#[derive(Clone)]
pub struct FileReader {
    file: Arc<File>,
    map: Arc<HashMap<u32, RelocMap>>,
}

impl FileReader {
    fn init_map(opt: &Arc<Options>, id: u32) -> HashMap<u32, RelocMap> {
        let mut file = File::options()
            .read(true)
            .open(opt.map_file(id))
            .expect("can't load map file");

        let mut map = HashMap::new();
        let mut buf = [0u8; size_of::<MapFooter>()];
        let off = file.seek(SeekFrom::End(0)).expect("can't seek file") - buf.len() as u64;

        file.read_exact_at(&mut buf, off).expect("can't read at");
        let footer = unsafe { &*(buf.as_ptr() as *const MapFooter) };

        let mut buf = vec![0u8; footer.active_delta_size()];
        file.read_exact_at(&mut buf, 0).expect("can't read at");

        let addrs = unsafe {
            std::slice::from_raw_parts(buf.as_ptr() as *const AddrMap, footer.nr_active as usize)
        };

        addrs
            .iter()
            .map(|x| {
                map.insert(x.key, x.val);
            })
            .count();
        map
    }

    pub fn new(opt: &Arc<Options>, id: u32) -> Self {
        let map = Self::init_map(opt, id);

        let file = File::options()
            .read(true)
            .open(opt.page_file(id))
            .expect("can't load page file");

        Self {
            file: Arc::new(file),
            map: Arc::new(map),
        }
    }

    /// NOTE: the `off` is in Arena offset, but when deserialize from file, there are: FileHeader
    /// and page_table mapping fields, we should skip them
    pub fn read_addr(&self, off: u32) -> FrameOwner {
        let m = self.map.get(&off).expect("invalid addr");
        let frame = FrameOwner::alloc(m.len as usize);

        log::info!("---->>> {:p} {}", frame.payload().data(), off);
        let b = frame.data();
        let dst = b.to_mut_slice(0, b.len());
        self.file
            .read_exact_at(dst, m.off as u64)
            .expect("can't read");

        frame
    }
}
