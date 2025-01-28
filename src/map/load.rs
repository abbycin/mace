use std::{collections::HashMap, path::PathBuf, sync::Arc};

use io::{File, SeekableGatherIO};

use super::data::{DataLoader, FrameOwner};
use crate::{utils::data::RelocMap, Options};

pub struct FileReader {
    path: PathBuf,
    file: File,
    off: u64,
    map: HashMap<u64, RelocMap>,
}

impl FileReader {
    fn open_file(path: &PathBuf, off: u64) -> Option<DataLoader> {
        let file = File::options()
            .read(true)
            .open(path)
            .inspect_err(|e| {
                log::error!("can't open {:?}, {}", path, e);
            })
            .ok()?;

        Some(DataLoader::read_only(file, off))
    }

    fn init_map(mut loader: DataLoader, map: &mut HashMap<u64, RelocMap>) -> u64 {
        while let Some(d) = loader.get_meta() {
            d.relocs().iter().map(|x| map.insert(x.key, x.val)).count();
        }

        loader.offset()
    }

    pub fn new(opt: &Arc<Options>, id: u16) -> Option<Self> {
        let path = opt.data_file(id);
        let loader = Self::open_file(&path, 0)?;
        let mut map = HashMap::new();
        let off = Self::init_map(loader, &mut map);

        let file = match File::options().read(true).open(&path) {
            Ok(f) => f,
            Err(e) => {
                log::error!("can't open {:?} {}", opt.data_file(id), e);
                std::process::abort();
            }
        };
        Some(Self {
            file,
            path,
            off,
            map,
        })
    }

    pub fn load(&mut self) {
        let loader = Self::open_file(&self.path, self.off).expect("can't open file");
        let off = Self::init_map(loader, &mut self.map);
        self.off = off;
    }

    pub fn read_at(&self, off: u64) -> Option<FrameOwner> {
        let m = self.map.get(&off)?;
        let frame = FrameOwner::alloc(m.len as usize);

        let b = frame.data();
        let dst = b.as_mut_slice(0, b.len());
        self.file.read(dst, m.off).expect("can't read");

        Some(frame)
    }
}
