use crate::map::data::{Frame, FrameOwner};
use crate::utils::data::{AddrMap, MapFooter};
use crate::utils::decode_u64;
use crate::{
    utils::data::{PageFooter, PageTable},
    Options,
};
use crc32c::Crc32cHasher;
use std::collections::VecDeque;
use std::hash::Hasher;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::{Condvar, Mutex};
use std::{
    fs::File,
    sync::{
        atomic::Ordering::Relaxed,
        mpsc::{channel, Receiver, Sender},
        Arc,
    },
    thread::JoinHandle,
    time::Duration,
};

use super::data::{FlushData, FrameFlag};

/// ```text
///  +-----------------+
///  |   delta data    |
///  +-----------------+
///  |   checksum      |
///  +-----------------+
/// ```
struct PageFileBuilder {
    file: File,
    normal: Vec<FrameOwner>,
    hasher: Crc32cHasher,
}

impl PageFileBuilder {
    fn new(path: PathBuf) -> Self {
        let file = File::options()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .expect("can't create page file");

        Self {
            file,
            normal: Vec::new(),
            hasher: Crc32cHasher::default(),
        }
    }

    /// deleted addresses were handled by mapping file, the tombstone is already reclaimed (as is)
    fn add(&mut self, frame: FrameOwner) {
        match frame.flag() {
            FrameFlag::Normal | FrameFlag::Slotted => {
                self.normal.push(frame);
            }
            _ => {}
        }
    }

    /// the frame has several type, such as: normal, delete, the delete type is disk address array
    /// we should store them in difference region of file
    ///
    /// for locating a page by PID we should establish a mapping table inside file header, which map
    /// the addr from page table to real offset of page file, that's to say, to load a delta from a
    /// page file needs two level of indirections
    fn build(&mut self) {
        for f in self.normal.iter() {
            f.serialize(&mut self.file, &mut self.hasher);
        }

        let footer = PageFooter {
            checksum: self.hasher.finish(),
        };

        footer.serialize(&mut self.file);
        self.file.sync_all().expect("can't sync");
    }
}

struct MapFileBuilder {
    file: File,
    file_id: u32, // a backup
    nr_active: u32,
    nr_delete: u32,
    active_delta: Vec<u8>,
    delete_delta: Vec<u8>,
    table: PageTable,
    off: u32,
}

impl MapFileBuilder {
    fn new(file_id: u32, path: PathBuf) -> Self {
        let file = File::options()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .expect("can't create map file");

        Self {
            file,
            file_id,
            nr_active: 0,
            nr_delete: 0,
            active_delta: Vec::new(),
            delete_delta: Vec::new(),
            table: PageTable::default(),
            off: 0,
        }
    }

    fn add(&mut self, frame: FrameOwner) {
        match frame.flag() {
            FrameFlag::Deleted => {
                let d = frame.raw_mut().load_data().expect("invalid frame");
                for i in d {
                    self.nr_delete += 1;
                    self.delete_delta.extend_from_slice(&i.to_le_bytes());
                }
            }
            FrameFlag::Normal => {
                self.nr_active += 1;
                let (pid, addr, sz) = (frame.page_id(), frame.addr(), frame.size());
                // NOTE: keep original pid -> addr mapping
                self.table.add(pid, addr);
                let map = AddrMap::new(decode_u64(addr).1, self.off, frame.payload_size());
                self.off += sz;
                self.active_delta.extend_from_slice(map.as_slice());
            }
            FrameFlag::Slotted => {
                self.off += frame.size();
            }
            FrameFlag::Unknown => {
                unreachable!("invalid frame {:?}", decode_u64(frame.addr()))
            }
            _ => {} // ignore tombstone
        }
    }

    /// ```text
    ///  +--------------------+
    ///  |  active offset     |
    ///  +--------------------+
    ///  |   deleted address  |
    ///  +--------------------+
    ///  |   mapping table    |
    ///  +--------------------+
    ///  |     footer         |
    ///  +--------------------+
    ///
    /// active offset: active delta's offset in current page file which is 32 bits
    /// deleted address: the file_id and in file offset which is 64 bits
    /// mapping table: page id to page address map and relocation
    /// ```
    fn build(&mut self) {
        let mut h = Crc32cHasher::default();

        h.write(self.active_delta.as_slice());
        h.write(self.delete_delta.as_slice());

        self.file
            .write_all(self.active_delta.as_slice())
            .expect("can't write active delta");

        self.file
            .write_all(self.delete_delta.as_slice())
            .expect("can't write delete delta");

        self.table.serialize(&mut self.file, &mut h);

        let footer = MapFooter {
            file_id: self.file_id,
            nr_active: self.nr_active,
            nr_deleted: self.nr_delete,
            nr_mapping: self.table.len() as u32,
            checksum: h.finish(),
        };
        footer.serialize(&mut self.file);

        self.file.sync_all().expect("can't sync file");
    }
}

fn flush_data(src: &FlushData, opt: &Arc<Options>) {
    let mut offset = 0;
    let size = src.len();
    let mut page_file = PageFileBuilder::new(opt.page_file(src.id()));
    let mut map_file = MapFileBuilder::new(src.id(), opt.map_file(src.id()));

    while offset < size {
        let b = src.add(offset);
        let frame = FrameOwner::from(b.data() as *mut Frame);
        offset += frame.size() as usize;
        page_file.add(frame.shallow_copy());
        map_file.add(frame.shallow_copy());
    }

    log::debug!("flush data {}", src.id());
    page_file.build();
    map_file.build();

    src.mark_done();
}

fn flush_thread(rx: Receiver<FlushData>, opt: Arc<Options>, sync: Arc<Notifier>) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("flush".into())
        .spawn(move || {
            log::debug!("start flush thread");
            let mut q = VecDeque::new();
            'outer: while !sync.is_quit() {
                loop {
                    match rx.recv_timeout(Duration::from_millis(1)) {
                        Ok(x) => q.push_back(x),
                        Err(RecvTimeoutError::Disconnected) => break 'outer,
                        _ => break,
                    }
                }

                while let Some(data) = q.pop_front() {
                    // NOTE: the data is a frame array
                    flush_data(&data, &opt);
                }
            }
            drop(rx);
            sync.notify_done();
            log::debug!("stop flush thread");
        })
        .expect("can't build flush thread")
}

struct Notifier {
    quit: AtomicBool,
    cond: Condvar,
    done: Mutex<bool>,
}

impl Notifier {
    fn new() -> Self {
        Self {
            quit: AtomicBool::new(false),
            cond: Condvar::new(),
            done: Mutex::new(false),
        }
    }

    fn is_quit(&self) -> bool {
        self.quit.load(Relaxed)
    }

    fn wait_done(&self) {
        let _guard = self.cond.wait_while(self.done.lock().unwrap(), |x| !(*x));
    }

    fn notify_quit(&self) {
        self.quit.store(true, Relaxed);
    }

    fn notify_done(&self) {
        let mut lk = self.done.lock().expect("can't lock");
        *lk = true;
        self.cond.notify_one();
    }
}

pub struct Flush {
    pub tx: Sender<FlushData>,
    sync: Arc<Notifier>,
}

impl Flush {
    pub fn new(opt: Arc<Options>) -> Self {
        let (tx, rx) = channel();
        let sync = Arc::new(Notifier::new());
        flush_thread(rx, opt.clone(), sync.clone());
        Self { tx, sync }
    }

    pub fn quit(&mut self) {
        self.sync.notify_quit();
        self.sync.wait_done();
    }
}
