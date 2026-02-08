use crate::cc::context::Context;
use crate::map::data::{Arena, FileBuilder};
use crate::meta::{BlobStat, DataStat, IntervalPair, MemBlobStat, MemDataStat, PageTable};
use crate::utils::Handle;
use crate::utils::OpCode;
use crate::utils::countblock::Countblock;
use crate::utils::data::{Interval, Position};
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

pub enum FlushDirective {
    Skip,
    Normal,
}

pub struct FlushResult {
    pub bucket_id: u64,
    pub data_id: u64,
    pub tick: u64,
    pub map_table: PageTable,
    pub data_ivl: IntervalPair,
    pub data_stat: DataStat,
    pub mem_data_stat: MemDataStat,
    pub data_junks: Vec<u64>,
    pub blob_ivl: Option<IntervalPair>,
    pub mem_blob_stat: Option<MemBlobStat>,
    pub blob_stat: Option<BlobStat>,
    pub blob_junks: Vec<u64>,
    pub flsn: Vec<Position>,
    pub done: FlushData,
}

pub trait FlushObserver: Send + Sync {
    fn flush_directive(&self, bucket_id: u64) -> FlushDirective;
    fn on_flush(&self, result: FlushResult) -> Result<(), OpCode>;
}

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

fn flush_data(msg: FlushData, ctx: Handle<Context>) -> Option<FlushResult> {
    let bucket_id = msg.bucket_id();
    let mut builder = FileBuilder::new(bucket_id);
    let mut map = MapBuilder::new(bucket_id);

    for x in msg.iter() {
        let f = x.value();
        #[cfg(feature = "extra_check")]
        {
            // bucket id is tracked by FlushData; BoxHeader no longer stores it
        }
        map.add(f);
        builder.add(f.clone());
    }

    if builder.is_empty() {
        msg.mark_done();
        return None;
    }

    if !ctx.opt.db_root.exists() {
        log::error!("db_root {:?} not exist", ctx.opt.db_root);
        panic!("db_root {:?} not exist", ctx.opt.db_root);
    }

    let data_id = msg.id();
    let tick = data_id;
    let data_path = ctx.opt.data_file(data_id);

    // 1. perform disk I/O
    let Interval { lo, hi } = builder.build_data(tick, data_path);
    let data_ivl = IntervalPair::new(lo, hi, data_id, bucket_id);

    let mut blob_ivl = None;
    let mut mem_blob_stat = None;
    let mut blob_stat = None;
    if builder.has_blob() {
        let blob_id = ctx.numerics.next_blob_id.fetch_add(1, Relaxed);
        let blob_path = ctx.opt.blob_file(blob_id);
        let Interval { lo, hi } = builder.build_blob(blob_path);
        let new_blob_ivl = IntervalPair::new(lo, hi, blob_id, bucket_id);
        let mut new_mem_blob_stat = builder.blob_stat(blob_id);
        new_mem_blob_stat.inner.bucket_id = bucket_id;
        blob_ivl = Some(new_blob_ivl);
        blob_stat = Some(new_mem_blob_stat.copy());
        mem_blob_stat = Some(new_mem_blob_stat);
    }

    // 2. prepare statistics
    let mut mem_data_stat = builder.data_stat(data_id, tick);
    mem_data_stat.inner.bucket_id = bucket_id;
    let data_stat = mem_data_stat.copy();
    builder.data_junks.sort_unstable();
    builder.blob_junks.sort_unstable();

    let flsn = msg.flsn.iter().map(|x| x.load()).collect();
    Some(FlushResult {
        bucket_id,
        data_id,
        tick,
        map_table: map.table(),
        data_ivl,
        data_stat,
        mem_data_stat,
        data_junks: builder.data_junks,
        blob_ivl,
        mem_blob_stat,
        blob_stat,
        blob_junks: builder.blob_junks,
        flsn,
        done: msg,
    })
}

fn safe_to_flush(data: &FlushData, ctx: Handle<Context>) -> bool {
    if data.refcnt() != 0 {
        return false;
    }
    safe_to_flush_force(data, ctx)
}

fn safe_to_flush_force(data: &FlushData, ctx: Handle<Context>) -> bool {
    let groups = ctx.groups();
    debug_assert_eq!(data.flsn.len(), groups.len());
    for (i, g) in groups.iter().enumerate() {
        // first update dirty page, and later update flsn on flush
        let pos = data.flsn[i].load();
        let flushed = g.logging.lock().flushed_pos();
        if pos.file_id > flushed.file_id
            || (pos.file_id == flushed.file_id && pos.offset > flushed.offset)
        {
            return false;
        }
    }
    true
}

fn try_flush(
    q: &mut VecDeque<FlushData>,
    ctx: Handle<Context>,
    observer: &dyn FlushObserver,
) -> bool {
    while let Some(data) = q.pop_front() {
        record!(total);
        let directive = observer.flush_directive(data.bucket_id());
        if let FlushDirective::Skip = directive {
            data.set_state(Arena::WARM, Arena::COLD);
            data.mark_done();
            continue;
        }

        let can_flush = safe_to_flush(&data, ctx);

        if can_flush && data.set_state(Arena::WARM, Arena::COLD) == Arena::WARM {
            if let Some(result) = flush_data(data, ctx) {
                observer.on_flush(result).unwrap();
            }
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
    observer: Arc<dyn FlushObserver>,
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
                try_flush(&mut q, ctx, observer.as_ref());
            }

            while let Ok(data) = rx.try_recv() {
                q.push_back(data);
            }

            while !try_flush(&mut q, ctx, observer.as_ref()) {
                std::thread::yield_now();
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

#[derive(Clone)]
pub struct Flush {
    pub tx: Sender<FlushData>,
    sync: Arc<Notifier>,
}

impl Flush {
    pub fn new(ctx: Handle<Context>, observer: Arc<dyn FlushObserver>) -> Self {
        let (tx, rx) = channel();
        let sync = Arc::new(Notifier::new());
        flush_thread(rx, ctx, observer, sync.clone());
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
