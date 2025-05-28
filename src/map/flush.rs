use crate::map::data::DataBuilder;
use crate::utils::countblock::Countblock;
use crate::utils::data::JUNK_LEN;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::RecvTimeoutError;
use std::{
    sync::{
        Arc,
        atomic::Ordering::Relaxed,
        mpsc::{Receiver, Sender, channel},
    },
    thread::JoinHandle,
    time::Duration,
};

use super::data::{FlushData, MapBuilder};
use super::load::Mapping;

fn flush_data(msg: FlushData, map: &Mapping) {
    let id = msg.id();
    let mut data_builder = DataBuilder::new(id);
    let mut map_builder = MapBuilder::new();

    let mut size = 0;
    for f in msg.iter {
        size += f.size();
        data_builder.add(f);
        map_builder.add(f);
    }

    if !data_builder.is_empty() {
        let opt = &map.opt;
        if !opt.db_root.exists() {
            log::error!("db_root {:?} not exist", opt.db_root);
            panic!("db_root {:?} not exist", opt.db_root);
        }
        let mut state = io::File::options()
            .create(true)
            .trunc(true)
            .write(true)
            .open(&opt.state_file())
            .unwrap();

        data_builder.build(opt, &mut state);
        log::trace!(
            "flush to {:?} active {} frames, size {}",
            map.opt.data_file(id),
            data_builder.active_frames(),
            size,
        );
        map_builder.build(opt, &mut state);

        drop(state);
        let _ = std::fs::remove_file(opt.state_file());

        map.add(id, false).expect("never happen");
        for f in data_builder.junks.iter() {
            let payload = f.payload();
            let junks = payload.as_slice::<u64>(0, payload.len() / JUNK_LEN);
            map.apply_junks(id, junks);
        }
        // notify sender before map compaction
        msg.mark_done();
        map_builder.compact(opt);
    } else {
        msg.mark_done();
    }
}

fn flush_thread(rx: Receiver<FlushData>, map: Arc<Mapping>, sync: Arc<Notifier>) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("flush".into())
        .spawn(move || {
            log::debug!("start flush thread");
            while !sync.is_quit() {
                match rx.recv_timeout(Duration::from_millis(1)) {
                    Ok(x) => flush_data(x, &map),
                    Err(RecvTimeoutError::Disconnected) => break,
                    _ => {}
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
    sem: Countblock,
}

impl Notifier {
    fn new() -> Self {
        Self {
            quit: AtomicBool::new(false),
            sem: Countblock::new(0),
        }
    }

    fn is_quit(&self) -> bool {
        self.quit.load(Relaxed)
    }

    fn wait_done(&self) {
        self.sem.wait();
    }

    fn notify_quit(&self) {
        self.quit.store(true, Relaxed);
    }

    fn notify_done(&self) {
        self.sem.post();
    }
}

pub struct Flush {
    pub tx: Sender<FlushData>,
    sync: Arc<Notifier>,
}

impl Flush {
    pub fn new(mapping: Arc<Mapping>) -> Self {
        let (tx, rx) = channel();
        let sync = Arc::new(Notifier::new());
        flush_thread(rx, mapping, sync.clone());
        Self { tx, sync }
    }

    pub fn quit(&self) {
        self.sync.notify_quit();
        self.sync.wait_done();
    }
}
