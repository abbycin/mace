use crate::map::data::DataBuilder;
use crate::utils::countblock::Countblock;
use crate::utils::data::JUNK_LEN;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::RecvTimeoutError;
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

use super::data::FlushData;
use super::load::Mapping;

fn flush_data(msg: FlushData, map: &Mapping) {
    let id = msg.id();
    let mut builder = DataBuilder::new();

    let mut size = 0;
    for f in msg.iter {
        size += f.size();
        builder.add(f);
    }

    if !builder.is_empty() {
        let path = map.opt.data_file(id);
        let mut f = match File::options().append(true).create(true).open(&path) {
            Ok(f) => f,
            Err(e) => {
                log::error!("can't open {:?}, {:?}", path, e);
                panic!("fatal error, path {:?}, error {:?}", path, e);
            }
        };

        builder.build(&mut f, id);
        let _ = f.sync_all().map_err(|x| {
            log::error!("can't sync {:?} {}", path, x);
            panic!("fatal error");
        });
        log::trace!(
            "flush active {} frames, size {}",
            builder.active_frames(),
            size,
        );

        map.add(id, false).expect("never happen");
        for f in builder.junks.iter() {
            let payload = f.payload();
            let junks = payload.as_slice::<u64>(0, payload.len() / JUNK_LEN);
            map.apply_junks(id, junks);
        }
    }

    msg.mark_done();
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
