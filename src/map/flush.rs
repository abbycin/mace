use crate::cc::context::Context;
use crate::map::data::{Arena, FileBuilder};
use crate::map::flow::FlowOutcome;
use crate::meta::{BlobStat, DataStat, IntervalPair, MemBlobStat, MemDataStat, PageTable};
use crate::types::header::{NodeType, TagFlag, TagKind};
use crate::types::traits::IHeader;
use crate::utils::Handle;
use crate::utils::OpCode;
use crate::utils::countblock::Countblock;
use crate::utils::data::{Interval, Position};
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

use super::data::{FlushData, MapBuilder};

pub enum FlushDirective {
    Skip,
    Normal,
}

pub struct FlushResult {
    pub bucket_id: u64,
    pub data_id: Option<u64>,
    pub map_table: PageTable,
    pub data_ivl: Option<IntervalPair>,
    pub data_stat: Option<DataStat>,
    pub mem_data_stat: Option<MemDataStat>,
    pub data_junks: Vec<u64>,
    pub blob_ivl: Option<IntervalPair>,
    pub mem_blob_stat: Option<MemBlobStat>,
    pub blob_stat: Option<BlobStat>,
    pub blob_junks: Vec<u64>,
    pub pending_sibling_addrs: Vec<u64>,
    pub published_sibling_addrs: Vec<u64>,
    pub flsn: Vec<Position>,
    pub done: FlushData,
}

pub trait FlushObserver: Send + Sync {
    fn flush_directive(&self, bucket_id: u64) -> FlushDirective;
    fn stage_orphan_data_file(&self, file_id: u64);
    fn stage_orphan_blob_file(&self, file_id: u64);
    fn on_flush(&self, result: FlushResult) -> Result<(), OpCode>;
}

fn file_size(path: &std::path::Path) -> u64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

fn flush_data(
    mut msg: FlushData,
    ctx: Handle<Context>,
    observer: &dyn FlushObserver,
) -> Option<FlushResult> {
    let bucket_id = msg.bucket_id();
    let mut builder = FileBuilder::new(bucket_id);
    let mut map = MapBuilder::new(bucket_id);
    let mut sibling_addrs = Vec::new();
    let mut published_sibling_addrs = Vec::new();

    for x in msg.iter() {
        let f = x.value();
        let hdr = f.header();
        if hdr.flag == TagFlag::Sibling {
            sibling_addrs.push(hdr.addr);
        } else if hdr.flag == TagFlag::Normal
            && hdr.kind == TagKind::Base
            && hdr.node_type == NodeType::Leaf
        {
            let base = f.view().as_base();
            if base.header().has_multiple_versions {
                let has_hint = base.load_sibling_heads_hint(&mut published_sibling_addrs);
                assert!(has_hint, "missing sibling hint for base addr {}", hdr.addr);
            }
        }
        map.add(f);
        builder.add(f.clone());
    }
    let map_table = map.table();

    let (mut retire_data, mut retire_blob) = msg.take_retires();
    retire_data.sort_unstable();
    retire_data.dedup();
    retire_blob.sort_unstable();
    retire_blob.dedup();

    if builder.is_empty()
        && map_table.is_empty()
        && retire_data.is_empty()
        && retire_blob.is_empty()
    {
        msg.set_flow_outcome(FlowOutcome::Empty);
        msg.mark_done();
        return None;
    }
    let flsn = msg.flsn.iter().map(|x| x.load()).collect();

    if builder.is_empty() {
        msg.set_flow_outcome(FlowOutcome::MetaOnly);
        return Some(FlushResult {
            bucket_id,
            data_id: None,
            map_table,
            data_ivl: None,
            data_stat: None,
            mem_data_stat: None,
            data_junks: retire_data,
            blob_ivl: None,
            mem_blob_stat: None,
            blob_stat: None,
            blob_junks: retire_blob,
            pending_sibling_addrs: sibling_addrs,
            published_sibling_addrs,
            flsn,
            done: msg,
        });
    }

    if !ctx.opt.db_root.exists() {
        log::error!("db_root {:?} not exist", ctx.opt.db_root);
        panic!("db_root {:?} not exist", ctx.opt.db_root);
    }

    let data_id = msg.id();
    let data_path = ctx.opt.data_file(data_id);
    let io_started = Instant::now();

    observer.stage_orphan_data_file(data_id);
    // 1. perform disk I/O
    let data_ivl = builder
        .build_data(data_id, data_path)
        .map(|Interval { lo, hi }| IntervalPair::new(lo, hi, data_id, bucket_id));

    let mut blob_ivl = None;
    let mut mem_blob_stat = None;
    let mut blob_stat = None;
    let mut blob_bytes = 0;
    if builder.has_blob() {
        let blob_id = ctx.numerics.next_blob_id.fetch_add(1, Relaxed);
        let blob_path = ctx.opt.blob_file(blob_id);
        observer.stage_orphan_blob_file(blob_id);
        let Interval { lo, hi } = builder.build_blob(blob_path);
        blob_bytes = file_size(&ctx.opt.blob_file(blob_id));
        let new_blob_ivl = IntervalPair::new(lo, hi, blob_id, bucket_id);
        let mut new_mem_blob_stat = builder.blob_stat(blob_id);
        new_mem_blob_stat.inner.bucket_id = bucket_id;
        blob_ivl = Some(new_blob_ivl);
        blob_stat = Some(new_mem_blob_stat.copy());
        mem_blob_stat = Some(new_mem_blob_stat);
    }

    let actual_bytes = file_size(&ctx.opt.data_file(data_id)).saturating_add(blob_bytes);
    msg.mark_flow_io_built(actual_bytes, io_started.elapsed());

    #[cfg(feature = "failpoints")]
    crate::utils::failpoint::crash("mace_flush_after_data_sync");

    // 2. prepare statistics
    let mut mem_data_stat = builder.data_stat(data_id, data_id);
    mem_data_stat.inner.bucket_id = bucket_id;
    let data_stat = mem_data_stat.copy();
    Some(FlushResult {
        bucket_id,
        data_id: Some(data_id),
        map_table,
        data_ivl,
        data_stat: Some(data_stat),
        mem_data_stat: Some(mem_data_stat),
        data_junks: retire_data,
        blob_ivl,
        mem_blob_stat,
        blob_stat,
        blob_junks: retire_blob,
        pending_sibling_addrs: sibling_addrs,
        published_sibling_addrs,
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
        let directive = observer.flush_directive(data.bucket_id());
        if let FlushDirective::Skip = directive {
            data.set_flow_outcome(FlowOutcome::Skip);
            data.set_state(Arena::WARM, Arena::COLD);
            data.mark_done();
            continue;
        }

        let can_flush = safe_to_flush(&data, ctx);

        if can_flush && data.set_state(Arena::WARM, Arena::COLD) == Arena::WARM {
            data.pace_before_flush();
            if let Some(result) = flush_data(data, ctx, observer) {
                observer.on_flush(result).unwrap();
            }
        } else {
            q.push_front(data);
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
