use crate::cc::context::Context;
use crate::map::data::{CheckpointTask, FileBuilder, MapBuilder};
use crate::meta::{FileKind, IntervalPair, MemStat, PageTable, PersistStat};
#[cfg(feature = "extra_check")]
use crate::utils::NULL_ADDR;
use crate::utils::countblock::Countblock;
use crate::utils::data::{GatherWriter, GroupPositions, Interval};
use crate::utils::options::ParsedOptions;
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

#[derive(Default)]
pub struct FlushKindResult {
    pub ivls: Vec<IntervalPair>,
    pub stats: Vec<PersistStat>,
    pub junk: Vec<u64>,
}

pub struct FlushResult {
    opt: Arc<ParsedOptions>,
    pub bucket_id: u64,
    pub map_table: PageTable,
    pub kinds: [FlushKindResult; 2],
    pub writers: Vec<GatherWriter>,
    pub latest_chkpoint_lsn: MutRef<GroupPositions>,
}

impl FlushResult {
    fn new(
        opt: Arc<ParsedOptions>,
        bucket_id: u64,
        map_table: PageTable,
        latest_chkpoint_lsn: MutRef<GroupPositions>,
    ) -> Self {
        Self {
            opt,
            bucket_id,
            map_table,
            kinds: [FlushKindResult::default(), FlushKindResult::default()],
            writers: Vec::new(),
            latest_chkpoint_lsn,
        }
    }

    pub fn kind(&self, kind: FileKind) -> &FlushKindResult {
        &self.kinds[kind.slot()]
    }

    pub fn kind_mut(&mut self, kind: FileKind) -> &mut FlushKindResult {
        &mut self.kinds[kind.slot()]
    }

    pub fn has_new_files(&self) -> bool {
        FileKind::ALL
            .iter()
            .any(|&kind| !self.kind(kind).ivls.is_empty())
    }

    pub fn sync(&mut self) {
        let has_outputs = !self.writers.is_empty();
        if self.opt.sync_on_write {
            for mut x in self.writers.drain(..) {
                x.sync();
            }
        } else {
            for mut x in self.writers.drain(..) {
                x.sync_data();
            }
        }
        if has_outputs {
            self.opt.sync_data_dir();
        }
    }
}

pub trait CheckpointObserver: Send + Sync {
    fn flush_directive(&self, bucket_id: u64) -> FlushDirective;
    fn next_update_epoch(&self, bucket_id: u64, kind: FileKind) -> u64;
    fn stage_unsynced_file(&self, kind: FileKind, file_id: u64);
    fn stage_orphan_file(&self, kind: FileKind, file_id: u64);
    fn update_mem_interval_stat(&self, kind: FileKind, ivl: IntervalPair, stat: MemStat);
    fn on_checkpoint(&self, result: FlushResult);
    fn finish_checkpoint(&self);
}

fn checkpoint(mut task: CheckpointTask, ctx: Handle<Context>, observer: &dyn CheckpointObserver) {
    let bucket_id = task.bucket_id;
    let mut snapshot = task.snapshot();
    let mut map_builder = MapBuilder::new(bucket_id, &snapshot.unmap_pid);
    let mut file_builder = FileBuilder::new(
        bucket_id,
        task.enable_compression,
        task.compressors.clone(),
        ctx.opt.fs.clone(),
    );

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
        let mut result = FlushResult::new(
            ctx.opt.clone(),
            bucket_id,
            mapping,
            task.last_chkpt_lsn.clone(),
        );
        result.kind_mut(FileKind::Data).junk = std::mem::take(&mut snapshot.data_junk);
        result.kind_mut(FileKind::Blob).junk = std::mem::take(&mut snapshot.blob_junk);
        observer.on_checkpoint(result);
        task.done(snapshot);
        return;
    }

    let mut result = FlushResult::new(
        ctx.opt.clone(),
        bucket_id,
        mapping,
        task.last_chkpt_lsn.clone(),
    );
    result.kind_mut(FileKind::Data).junk = std::mem::take(&mut snapshot.data_junk);
    result.kind_mut(FileKind::Blob).junk = std::mem::take(&mut snapshot.blob_junk);
    let actual_bytes = file_builder.io_bytes();
    let io_started = Instant::now();

    for kind in FileKind::ALL {
        if !file_builder.has_kind(kind) {
            continue;
        }
        let tick = observer.next_update_epoch(bucket_id, kind);
        let files = file_builder.flush_files(
            kind,
            match kind {
                FileKind::Data => ctx.opt.data_file_size,
                FileKind::Blob => ctx.opt.blob_file_size,
            },
            tick,
            || {
                let file_id = ctx.sequences.next_file_id.fetch_add(1, Relaxed);
                observer.stage_orphan_file(kind, file_id);
                observer.stage_unsynced_file(kind, file_id);
                let path = match kind {
                    FileKind::Data => ctx.opt.data_file(file_id),
                    FileKind::Blob => ctx.opt.blob_file(file_id),
                };
                (file_id, path)
            },
            |bytes| {
                task.mark_checkpoint_progress(bytes);
            },
            |file| {
                let ivl =
                    IntervalPair::new(file.interval.lo, file.interval.hi, file.file_id, bucket_id);
                observer.update_mem_interval_stat(kind, ivl, file.stat.clone_mem());
                task.release_persisted_pages(&file.addrs);
            },
        );
        let mut built_writers = Vec::with_capacity(files.len());
        {
            let slot = result.kind_mut(kind);
            for file in files {
                let Interval { lo, hi } = file.interval;
                slot.ivls
                    .push(IntervalPair::new(lo, hi, file.file_id, bucket_id));
                slot.stats.push(file.stat.copy());
                built_writers.push(file.writer);
            }
        }
        result.writers.extend(built_writers);
    }

    #[cfg(feature = "failpoints")]
    crate::utils::failpoint::crash("mace_flush_after_data_sync");

    task.mark_io_built(actual_bytes, io_started.elapsed());
    observer.on_checkpoint(result);

    task.done(snapshot);
}

fn process_task(
    q: &mut VecDeque<CheckpointTask>,
    ctx: Handle<Context>,
    observer: &dyn CheckpointObserver,
) {
    let mut processed_checkpoint = false;
    while let Some(task) = q.pop_front() {
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
        processed_checkpoint = true;
    }
    if processed_checkpoint {
        observer.finish_checkpoint();
    }
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
