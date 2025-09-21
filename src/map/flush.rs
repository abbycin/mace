use crate::cc::context::Context;
use crate::map::data::{Arena, DataBuilder};
use crate::utils::countblock::Countblock;
use crate::utils::{Handle, unpack_id};
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

fn flush_data(msg: FlushData, mut ctx: Handle<Context>) {
    let id = msg.id();
    let mut data_builder = DataBuilder::new();
    let mut map_builder = MapBuilder::new();

    let mut size: usize = 0;
    for x in msg.iter() {
        let f = x.value();
        size += f.total_size() as usize;
        map_builder.add(f);
        data_builder.add(f.clone());
    }

    if !data_builder.is_empty() {
        let path = ctx.opt.data_file(id);
        if !ctx.opt.db_root.exists() {
            log::error!("db_root {:?} not exist", ctx.opt.db_root);
            panic!("db_root {:?} not exist", ctx.opt.db_root);
        }

        let tick = ctx.manifest.numerics.tick.load(Relaxed);
        let fstat = data_builder.stat(id, tick);
        let stat = fstat.copy();
        let mut junks = Vec::with_capacity(data_builder.junks.len());
        for f in data_builder.junks.iter() {
            junks.extend_from_slice(f.data_slice::<u64>());
        }
        junks.sort_unstable_by(|x, y| unpack_id(*x).0.cmp(&unpack_id(*y).0));
        let stats = ctx.manifest.apply_junks(tick, &junks);

        let mut txn = ctx.manifest.begin(id);

        txn.record(&stat); // new entry
        txn.record(&map_builder.table());

        stats.iter().for_each(|x| txn.record(x));

        // flush before commit
        txn.flush();
        data_builder.build(tick, path);

        let nbytes = txn.commit();

        log::debug!(
            "flush to {:?} active {} frames, size {} sizes {:?} manifest {}",
            ctx.opt.data_file(id),
            data_builder.active_frames(),
            size,
            msg.sizes(),
            nbytes
        );

        // create a new mapping must after data has been flushed, so that GC can read fully flushed
        // data file
        ctx.manifest.add_stat(fstat);
        ctx.manifest.numerics.tick.fetch_add(1, Relaxed);
    }
    msg.mark_done();
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

fn try_flush(q: &mut VecDeque<FlushData>, ctx: Handle<Context>) -> bool {
    while let Some(data) = q.pop_front() {
        record!(total);
        if safe_to_flush(&data, ctx) && data.set_state(Arena::WARM, Arena::COLD) == Arena::WARM {
            flush_data(data, ctx)
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
                try_flush(&mut q, ctx);
            }

            while let Ok(data) = rx.try_recv() {
                q.push_back(data);
            }

            while !try_flush(&mut q, ctx) {
                std::hint::spin_loop();
            }
            drop(rx);
            sync.notify_done();
            log::info!("flusher thread eixt");
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
    pub fn new(ctx: Handle<Context>) -> Self {
        let (tx, rx) = channel();
        let sync = Arc::new(Notifier::new());
        flush_thread(rx, ctx, sync.clone());
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
