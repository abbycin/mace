use crate::map::data::{Arena, DataBuilder};
use crate::utils::Handle;
use crate::utils::countblock::Countblock;
use crate::utils::data::Meta;
use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::RecvTimeoutError;
use std::thread::JoinHandle;
use std::{
    sync::{
        Arc,
        atomic::Ordering::Relaxed,
        mpsc::{Receiver, Sender, channel},
    },
    time::Duration,
};

use super::data::{FlushData, MapBuilder};
use super::load::Mapping;

fn flush_data(msg: FlushData, map: Handle<Mapping>, meta: &Meta) {
    let id = msg.id();
    let mut data_builder = DataBuilder::new(id);
    let mut map_builder = MapBuilder::new();

    let mut size: usize = 0;
    for x in msg.iter() {
        let f = x.value();
        size += f.total_size() as usize;
        map_builder.add(f);
        data_builder.add(f.clone());
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
        meta.flush_data.fetch_add(1, Relaxed);
        log::debug!(
            "flush to {:?} active {} frames, size {} sizes {:?}",
            map.opt.data_file(id),
            data_builder.active_frames(),
            size,
            msg.sizes(),
        );
        map_builder.build(opt, &mut state);

        drop(state);
        let _ = std::fs::remove_file(opt.state_file());

        map.add(id, false).expect("never happen");
        for f in data_builder.junks.iter() {
            let junks = f.data_slice::<u64>();
            map.apply_junks(id, junks);
        }
        // notify sender before map compaction
        msg.mark_done();
        map_builder.compact(opt);
    } else {
        msg.mark_done();
    }
}

fn try_flush(q: &mut VecDeque<FlushData>, map: Handle<Mapping>, meta: &Meta) -> bool {
    if let Some(data) = q.pop_front() {
        if data.unref() && data.set_state(Arena::WARM, Arena::COLD) == Arena::WARM {
            flush_data(data, map, meta)
        } else {
            q.push_front(data);
        }
        true
    } else {
        false
    }
}

fn flush_thread(
    rx: Receiver<FlushData>,
    map: Handle<Mapping>,
    meta: Arc<Meta>,
    sync: Arc<Notifier>,
) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("flush".into())
        .spawn(move || {
            log::debug!("start flush thread");
            let mut q = VecDeque::new();
            while !sync.is_quit() {
                match rx.recv_timeout(Duration::from_millis(1)) {
                    Ok(x) => q.push_back(x),
                    Err(RecvTimeoutError::Disconnected) => break,
                    _ => {}
                }
                try_flush(&mut q, map, &meta);
            }

            while let Ok(data) = rx.try_recv() {
                q.push_back(data);
            }

            loop {
                if !try_flush(&mut q, map, &meta) {
                    break;
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
    pub fn new(mapping: Handle<Mapping>, meta: Arc<Meta>) -> Self {
        let (tx, rx) = channel();
        let sync = Arc::new(Notifier::new());
        flush_thread(rx, mapping, meta, sync.clone());
        Self { tx, sync }
    }

    pub fn quit(&self) {
        self.sync.notify_quit();
        self.sync.wait_done();
    }
}
