use crate::cc::context::Context;
use crate::map::data::{CheckpointTask, FileBuilder, MapBuilder};
use crate::meta::{BlobStat, DataStat, IntervalPair, MemBlobStat, MemDataStat, PageTable};
#[cfg(feature = "extra_check")]
use crate::utils::NULL_ADDR;
use crate::utils::countblock::Countblock;
use crate::utils::data::{GatherWriter, GroupPositions, Interval};
use crate::utils::observe::CounterMetric;
use crate::utils::{Handle, MutRef};
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
    time::{Duration, Instant},
};

pub enum FlushDirective {
    Skip,
    Normal,
}

pub struct FlushResult {
    sync_all: bool,
    pub bucket_id: u64,
    pub map_table: PageTable,
    pub data_ivls: Vec<IntervalPair>,
    pub data_stats: Vec<DataStat>,
    pub blob_ivls: Vec<IntervalPair>,
    pub blob_stats: Vec<BlobStat>,
    pub blob_junk: Vec<u64>,
    pub data_junk: Vec<u64>,
    pub writers: Vec<GatherWriter>,
    pub latest_chkpoint_lsn: MutRef<GroupPositions>,
}

impl FlushResult {
    pub fn sync(&mut self) {
        if self.sync_all {
            for mut x in self.writers.drain(..) {
                x.sync();
            }
        } else {
            for mut x in self.writers.drain(..) {
                x.sync_data();
            }
        }
    }
}

pub trait CheckpointObserver: Send + Sync {
    fn flush_directive(&self, bucket_id: u64) -> FlushDirective;
    fn stage_unsynced_data_file(&self, file_id: u64);
    fn stage_unsynced_blob_file(&self, file_id: u64);
    fn stage_orphan_data_file(&self, file_id: u64);
    fn stage_orphan_blob_file(&self, file_id: u64);
    fn update_data_mem_interval_stat(&self, ivl: IntervalPair, stat: MemDataStat);
    fn update_blob_mem_interval_stat(&self, ivl: IntervalPair, stat: MemBlobStat);
    fn on_flush(&self, result: FlushResult);
}

fn checkpoint(mut task: CheckpointTask, ctx: Handle<Context>, observer: &dyn CheckpointObserver) {
    let bucket_id = task.bucket_id;
    let mut snapshot = task.snapshot();
    let mut map_builder = MapBuilder::new(bucket_id, &snapshot.unmap_pid);
    let mut file_builder = FileBuilder::new(bucket_id);

    let pages = std::mem::take(&mut snapshot.pages);
    for b in pages {
        map_builder.add(&b);
        file_builder.add(b);
    }

    let mapping = map_builder.table();
    #[cfg(feature = "extra_check")]
    for (&pid, &addr) in mapping.iter() {
        assert!(
            addr == NULL_ADDR || addr <= task.snap_addr,
            "map addr {} for pid {} exceeds snap_addr {}",
            addr,
            pid,
            task.snap_addr
        );
    }

    if file_builder.is_empty() {
        observer.on_flush(FlushResult {
            sync_all: false,
            bucket_id,
            map_table: mapping,
            data_ivls: Vec::new(),
            data_stats: Vec::new(),
            blob_ivls: Vec::new(),
            blob_stats: Vec::new(),
            blob_junk: std::mem::take(&mut snapshot.blob_junk),
            data_junk: std::mem::take(&mut snapshot.data_junk),
            writers: Vec::new(),
            latest_chkpoint_lsn: task.last_chkpt_lsn.clone(),
        });
        task.done(snapshot);
        return;
    }

    let mut data_ivls = Vec::new();
    let mut data_stats = Vec::new();
    let mut blob_ivls = Vec::new();
    let mut blob_stats = Vec::new();
    let mut writers = Vec::new();
    let actual_bytes = file_builder.io_bytes();
    let io_started = Instant::now();

    if file_builder.has_data() {
        let data_files = file_builder.flush_data_files(
            ctx.opt.data_file_size,
            || {
                let data_file_id = ctx.numerics.next_data_id.fetch_add(1, Relaxed);
                observer.stage_orphan_data_file(data_file_id);
                observer.stage_unsynced_data_file(data_file_id);
                (data_file_id, ctx.opt.data_file(data_file_id))
            },
            |bytes| {
                task.mark_checkpoint_progress(bytes);
            },
            |file| {
                let ivl =
                    IntervalPair::new(file.interval.lo, file.interval.hi, file.file_id, bucket_id);
                observer.update_data_mem_interval_stat(ivl, file.stat.clone_mem());
                task.release_persisted_pages(&file.addrs);
            },
        );
        for file in data_files {
            let Interval { lo, hi } = file.interval;
            data_ivls.push(IntervalPair::new(lo, hi, file.file_id, bucket_id));
            data_stats.push(file.stat.copy());
            writers.push(file.writer);
        }
    }

    if file_builder.has_blob() {
        let blob_files = file_builder.flush_blob_files(
            ctx.opt.blob_file_size,
            || {
                let blob_file_id = ctx.numerics.next_blob_id.fetch_add(1, Relaxed);
                observer.stage_orphan_blob_file(blob_file_id);
                observer.stage_unsynced_blob_file(blob_file_id);
                (blob_file_id, ctx.opt.blob_file(blob_file_id))
            },
            |bytes| {
                task.mark_checkpoint_progress(bytes);
            },
            |file| {
                let ivl =
                    IntervalPair::new(file.interval.lo, file.interval.hi, file.file_id, bucket_id);
                observer.update_blob_mem_interval_stat(ivl, file.stat.clone_mem());
                task.release_persisted_pages(&file.addrs);
            },
        );
        for file in blob_files {
            let Interval { lo, hi } = file.interval;
            blob_ivls.push(IntervalPair::new(lo, hi, file.file_id, bucket_id));
            blob_stats.push(file.stat.copy());
            writers.push(file.writer);
        }
    }

    #[cfg(feature = "failpoints")]
    crate::utils::failpoint::crash("mace_flush_after_data_sync");

    task.mark_io_built(actual_bytes, io_started.elapsed());
    // all data has been fushed, the rest are junk pages that will never be accessed, clear them before
    // sync file to release memory
    task.pages.clear();
    observer.on_flush(FlushResult {
        sync_all: ctx.opt.sync_on_write,
        bucket_id,
        map_table: mapping,
        data_ivls,
        data_stats,
        blob_ivls,
        blob_stats,
        blob_junk: std::mem::take(&mut snapshot.blob_junk),
        data_junk: std::mem::take(&mut snapshot.data_junk),
        writers,
        latest_chkpoint_lsn: task.last_chkpt_lsn.clone(),
    });

    task.done(snapshot);
}

fn process_task(
    q: &mut VecDeque<CheckpointTask>,
    ctx: Handle<Context>,
    observer: &dyn CheckpointObserver,
) {
    let mut flushed = false;
    while let Some(task) = q.pop_front() {
        flushed = true;
        let directive = observer.flush_directive(task.bucket_id);
        if let FlushDirective::Skip = directive {
            // Skip is only used for unload/delete bucket
            // in this lifecycle the bucket is being reclaimed, so persisting this sealed batch,
            // reclaiming its generation resources, and settling flow-control accounting are all
            // intentionally unnecessary
            task.force_done();
            continue;
        }
        checkpoint(task, ctx, observer);
    }
    if flushed {
        process_wal_clean(ctx);
    }
}

fn process_wal_clean(ctx: Handle<Context>) {
    for g in ctx.groups().iter() {
        let mut checkpoint_id = g.active_txns.min_position_file_id();
        let (oldest_id, last_ckpt_file) = {
            let logging = g.logging.lock();
            (logging.oldest_wal_id(), logging.last_ckpt().file_id)
        };
        checkpoint_id = std::cmp::min(checkpoint_id, last_ckpt_file);
        if oldest_id >= checkpoint_id {
            continue;
        }

        // [oldest_id, checkpoint_id)
        let recycled = process_one_wal(ctx, g.id as u8, oldest_id, checkpoint_id);
        if recycled > 0 {
            ctx.opt
                .observer
                .counter(CounterMetric::GcWalRecycleFile, recycled);
        }
        g.logging.lock().advance_oldest_wal_id(checkpoint_id);
    }
}

fn process_one_wal(ctx: Handle<Context>, group: u8, beg: u64, end: u64) -> u64 {
    let mut recycled = 0;
    // NOTE: not including `end`
    for seq in beg..end {
        let from = ctx.opt.wal_file(group, seq);
        if !from.exists() {
            continue;
        }
        let to = ctx.opt.wal_backup(group, seq);
        if ctx.opt.keep_stable_wal_file {
            log::info!("rename {from:?} to {to:?}");
            std::fs::rename(&from, &to)
                .inspect_err(|e| {
                    log::error!("can't rename {from:?} to {to:?}, error {e:?}");
                })
                .unwrap();
        } else {
            log::info!("unlink {from:?}");
            std::fs::remove_file(&from)
                .inspect_err(|e| log::error!("can't remove {from:?}, error {e:?}"))
                .unwrap();
        }
        recycled += 1;
    }
    recycled
}

fn checkpoint_thread(
    rx: Receiver<CheckpointTask>,
    ctx: Handle<Context>,
    observer: Arc<dyn CheckpointObserver>,
    sync: Arc<Notifier>,
) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("checkpointer".into())
        .spawn(move || {
            let mut q = VecDeque::new();

            while !sync.is_quit() {
                match rx.recv_timeout(Duration::from_millis(1)) {
                    Ok(x) => q.push_back(x),
                    Err(RecvTimeoutError::Disconnected) => break,
                    _ => {}
                }
                process_task(&mut q, ctx, observer.as_ref());
            }

            process_task(&mut q, ctx, observer.as_ref());
            sync.notify_done();
            log::info!("checkpoint thread exit");
        })
        .expect("can't build checkpoint thread")
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

#[derive(Clone)]
pub struct Checkpoint {
    pub tx: Sender<CheckpointTask>,
    sync: Arc<Notifier>,
}

impl Checkpoint {
    pub fn new(ctx: Handle<Context>, observer: Arc<dyn CheckpointObserver>) -> Self {
        let (tx, rx) = channel();
        let sync = Arc::new(Notifier::new());
        checkpoint_thread(rx, ctx, observer, sync.clone());
        Self { tx, sync }
    }

    pub fn quit(&self) {
        self.sync.notify_quit();
        self.sync.wait_done();
    }
}
