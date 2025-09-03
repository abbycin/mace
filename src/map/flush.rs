use crate::cc::context::Context;
use crate::map::data::{Arena, DataBuilder};
use crate::utils::Handle;
use crate::utils::countblock::Countblock;
use crate::utils::data::Meta;
use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
#[cfg(feature = "metric")]
use std::sync::atomic::AtomicUsize;
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

#[cfg(feature = "metric")]
macro_rules! record {
    ($x: ident) => {
        G_STATE.$x.fetch_add(1, Relaxed)
    };
}

#[cfg(not(feature = "metric"))]
macro_rules! record {
    ($x: ident) => {};
}

fn flush_data(msg: FlushData, map: Handle<Mapping>, meta: &Meta) {
    let id = msg.id();
    let tick = meta.tick.load(Relaxed);
    let mut data_builder = DataBuilder::new(tick, id);
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

        // must after state stabilized
        meta.flush_data.fetch_add(1, Relaxed);

        map.add(id, false).expect("never happen");
        for f in data_builder.junks.iter() {
            let junks = f.data_slice::<u64>();
            map.apply_junks(tick, junks);
        }
        // notify sender before map compaction
        msg.mark_done();
        map_builder.compact(opt);
    } else {
        msg.mark_done();
    }
}

fn safe_to_flush(data: &FlushData, ctx: Handle<Context>) -> bool {
    let workers = ctx.workers();
    debug_assert_eq!(data.flsn.len(), workers.len());
    for (i, w) in workers.iter().enumerate() {
        // first update dirty page, and later update flsn on flush
        if data.flsn[i].load(Relaxed) > w.logging.flsn() {
            return false;
        }
    }
    true
}

fn try_flush(q: &mut VecDeque<FlushData>, map: Handle<Mapping>, ctx: Handle<Context>) -> bool {
    while let Some(data) = q.pop_front() {
        record!(total);
        if safe_to_flush(&data, ctx) && data.set_state(Arena::WARM, Arena::COLD) == Arena::WARM {
            flush_data(data, map, &ctx.meta)
        } else {
            q.push_front(data);
            record!(retry);
            break;
        }
    }
    q.is_empty()
}

fn flush_thread(
    rx: Receiver<FlushData>,
    map: Handle<Mapping>,
    ctx: Handle<Context>,
    sync: Arc<Notifier>,
) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("flusher".into())
        .spawn(move || {
            log::debug!("start flush thread");
            let mut q = VecDeque::new();

            while !sync.is_quit() {
                match rx.recv_timeout(Duration::from_millis(50)) {
                    Ok(x) => q.push_back(x),
                    Err(RecvTimeoutError::Disconnected) => break,
                    _ => {}
                }
                try_flush(&mut q, map, ctx);
            }

            while let Ok(data) = rx.try_recv() {
                q.push_back(data);
            }

            while !try_flush(&mut q, map, ctx) {
                std::hint::spin_loop();
            }
            drop(rx);
            sync.notify_done();
            log::debug!("flusher thread eixt");
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
            sem: Countblock::new(1),
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
    pub fn new(mapping: Handle<Mapping>, ctx: Handle<Context>) -> Self {
        let (tx, rx) = channel();
        let sync = Arc::new(Notifier::new());
        flush_thread(rx, mapping, ctx, sync.clone());
        Self { tx, sync }
    }

    pub fn quit(&self) {
        self.sync.notify_quit();
        self.sync.wait_done();
    }
}

#[cfg(feature = "metric")]
#[derive(Debug)]
pub struct FlushStatus {
    retry: AtomicUsize,
    total: AtomicUsize,
}

#[cfg(feature = "metric")]
static G_STATE: FlushStatus = FlushStatus {
    retry: AtomicUsize::new(0),
    total: AtomicUsize::new(0),
};

#[cfg(feature = "metric")]
pub fn g_flush_status() -> &'static FlushStatus {
    &G_STATE
}
