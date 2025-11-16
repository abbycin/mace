use crate::cc::context::Context;
use crate::map::data::{Arena, FileBuilder};
use crate::meta::{FileId, IntervalPair, MetaKind, TxnKind};
use crate::utils::Handle;
use crate::utils::countblock::Countblock;
use crate::utils::data::Interval;
use std::collections::VecDeque;
use std::ops::Deref;
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

fn flush_data(msg: FlushData, ctx: Handle<Context>) {
    let data_id = msg.id();
    let mut builder = FileBuilder::new();
    let mut map = MapBuilder::new();

    let mut size: usize = 0;
    for x in msg.iter() {
        let f = x.value();
        size += f.total_size() as usize;
        map.add(f);
        builder.add(f.clone());
    }

    if !builder.is_empty() {
        let data_path = ctx.opt.data_file(data_id);
        if !ctx.opt.db_root.exists() {
            log::error!("db_root {:?} not exist", ctx.opt.db_root);
            panic!("db_root {:?} not exist", ctx.opt.db_root);
        }

        let tick = ctx.manifest.numerics.next_data_id.load(Relaxed);
        let mem_data_stat = builder.data_stat(data_id, tick);
        let data_stat = mem_data_stat.copy();

        let data_stats = {
            builder.data_junks.sort_unstable();
            ctx.manifest.apply_data_junks(tick, &builder.data_junks)
        };
        let blob_stats = {
            builder.blob_junks.sort_unstable();
            ctx.manifest.apply_blob_junks(&builder.blob_junks)
        };

        let mut txn = ctx.manifest.begin(TxnKind::FLush);

        txn.record(MetaKind::FileId, &FileId::data(data_id));
        txn.sync(); // necessary, before data file was flushed

        txn.record(MetaKind::Numerics, ctx.manifest.numerics.deref());
        txn.record(MetaKind::DataStat, &data_stat); // new entry
        txn.record(MetaKind::Map, &map.table());

        data_stats.iter().for_each(|x| {
            assert_ne!(data_id, x.file_id);
            txn.record(MetaKind::DataStat, x)
        });

        blob_stats.iter().for_each(|x| {
            assert_ne!(data_id, x.file_id);
            txn.record(MetaKind::BlobStat, x);
        });

        // data file must be flushed after FileId flushed and before txn commit
        let Interval { lo, hi } = builder.build_data(tick, data_path);
        let data_ivl = IntervalPair::new(lo, hi, data_id);
        txn.record(MetaKind::DataInterval, &data_ivl);

        // create a new mapping must after data has been flushed, so that GC can read fully flushed
        // data file
        ctx.manifest.add_data_stat(mem_data_stat, data_ivl);

        // although the blob file is usually less than blob_max_size, and may larger than it in some
        // case, we simply flush blobs into a single file, the blob files will be processed by gc
        // runtime so that most of their size will finally near to blob_max_size
        if builder.has_blob() {
            let blob_id = ctx.manifest.numerics.next_blob_id.fetch_add(1, Relaxed);
            txn.record(MetaKind::Numerics, ctx.manifest.numerics.deref());

            let blob_path = ctx.opt.blob_file(blob_id);
            let mem_blob_stat = builder.blob_stat(blob_id);
            let blob_stat = mem_blob_stat.copy();

            txn.record(MetaKind::FileId, &FileId::blob(blob_id));
            txn.sync();

            txn.record(MetaKind::BlobStat, &blob_stat);
            let Interval { lo, hi } = builder.build_blob(blob_path);
            let blob_ivl = IntervalPair::new(lo, hi, blob_id);
            txn.record(MetaKind::BlobInterval, &blob_ivl);

            ctx.manifest.add_blob_stat(mem_blob_stat, blob_ivl);
        }

        let nbytes = txn.commit();

        log::debug!(
            "flush to {:?} active {} frames, size {} sizes {:?} manifest {}",
            ctx.opt.data_file(data_id),
            builder.active_frames(),
            size,
            msg.sizes(),
            nbytes
        );

        ctx.manifest.numerics.signal.fetch_add(1, Relaxed);
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
