use crate::cc::context::Context;
use crate::map::data::{Arena, DataBuilder};
use crate::meta::IntervalPair;
use crate::utils::Handle;
use crate::utils::countblock::Countblock;
use crate::utils::data::Interval;
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
    let file_id = msg.id();
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
        let path = ctx.opt.data_file(file_id);
        if !ctx.opt.db_root.exists() {
            log::error!("db_root {:?} not exist", ctx.opt.db_root);
            panic!("db_root {:?} not exist", ctx.opt.db_root);
        }

        let tick = ctx.manifest.numerics.tick.load(Relaxed);
        let fstat = data_builder.stat(file_id, tick);
        let stat = fstat.copy();
        let mut junks = Vec::with_capacity(data_builder.junks.len());
        for f in data_builder.junks.iter() {
            junks.extend_from_slice(f.data_slice::<u64>());
        }
        junks.sort_unstable();

        let mut txn = ctx.manifest.begin(file_id);

        txn.record(&stat); // new entry
        txn.record(&map_builder.table());

        // we must protect stats collection in txn, or else Stat may be logged after the Delete
        // record (in GC thread), thus in recovery process will create wrong FileStat cause error
        // such as BitMap index out of range
        let stats = ctx.manifest.apply_junks(tick, &junks);
        stats.iter().for_each(|x| {
            assert_ne!(file_id, x.file_id);
            txn.record(x)
        });

        // flush before commit
        txn.flush();
        let Interval { lo, hi } = data_builder.build(tick, path);
        let ivl = IntervalPair::new(lo, hi, file_id);
        txn.record(&ivl);

        let nbytes = txn.commit();

        log::debug!(
            "flush to {:?} active {} frames, size {} sizes {:?} manifest {}",
            ctx.opt.data_file(file_id),
            data_builder.active_frames(),
            size,
            msg.sizes(),
            nbytes
        );

        // create a new mapping must after data has been flushed, so that GC can read fully flushed
        // data file
        ctx.manifest.add_stat_interval(fstat, ivl);
        ctx.manifest.numerics.tick.fetch_add(1, Relaxed);
    }
    msg.mark_done();
}

fn safe_to_flush(data: &FlushData, ctx: Handle<Context>) -> bool {
    if data.refcnt() != 0 {
        return false;
    }
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
