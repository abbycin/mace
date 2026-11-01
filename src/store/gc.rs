use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering::Relaxed},
    mpsc::{Receiver, RecvTimeoutError, Sender, channel},
};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

#[cfg(feature = "metrics")]
use crate::utils::observe::{CounterMetric, HistogramMetric};
use crate::{
    OpCode, Store,
    cc::context::Context,
    meta::{FileKind, Manifest, Sequences},
    must_ok,
    utils::{Handle, MutRef, countblock::Countblock},
};

const GC_QUIT: i32 = -1;
const GC_PAUSE: i32 = 3;
const GC_RESUME: i32 = 5;
const GC_START: i32 = 7;
const GC_WAL: i32 = 11;

fn gc_thread(mut gc: GarbageCollector, rx: Receiver<i32>, sem: Arc<Countblock>) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("garbage-collector".into())
        .spawn(move || {
            let timeout = Duration::from_millis(gc.store.opt.gc_timeout);
            let mut pause = false;
            let mut next_run_at = Instant::now() + timeout;

            loop {
                let wait_timeout = if pause {
                    timeout
                } else {
                    next_run_at.saturating_duration_since(Instant::now())
                };
                // extra_check test mode: gc_timeout=0 means explicit-trigger-only
                // park on a long wait instead of a ZERO poll (which would spin)
                #[cfg(feature = "extra_check")]
                let wait_timeout = if timeout.is_zero() {
                    Duration::from_secs(3600)
                } else {
                    wait_timeout
                };
                match rx.recv_timeout(wait_timeout) {
                    Ok(x) => match x {
                        GC_PAUSE => {
                            pause = true;
                            sem.post();
                        }
                        GC_RESUME => {
                            pause = false;
                            next_run_at = Instant::now() + timeout;
                            sem.post();
                        }
                        GC_START => {
                            gc.run();
                            if !pause {
                                next_run_at = Instant::now() + timeout;
                            }
                            sem.post();
                        }
                        GC_WAL => gc.process_wal_clean(),
                        GC_QUIT => break,
                        _ => unreachable!("invalid instruction  {}", x),
                    },
                    Err(RecvTimeoutError::Timeout) => {}
                    Err(e) => {
                        log::error!("gc receive error {e}");
                        break;
                    }
                }

                if !pause && Instant::now() >= next_run_at {
                    // extra_check test mode: gc_timeout=0 arms no timer, the
                    // parked tick must not run a round
                    #[cfg(feature = "extra_check")]
                    if timeout.is_zero() {
                        continue;
                    }
                    gc.run();
                    next_run_at = Instant::now() + timeout;
                }
            }

            sem.post();
            log::info!("garbage-collector thread exit");
        })
        .expect("can't start garbage-collector thread")
}

#[derive(Clone)]
pub(crate) struct GCHandle {
    tx: Arc<Sender<i32>>,
    sem: Arc<Countblock>,
    data_runs: Arc<AtomicU64>,
    blob_runs: Arc<AtomicU64>,
}

impl GCHandle {
    pub(crate) fn quit(&self) {
        must_ok!(self.tx.send(GC_QUIT));
        self.sem.wait();
    }

    pub(crate) fn pause(&self) {
        must_ok!(self.tx.send(GC_PAUSE));
        self.sem.wait();
    }

    pub(crate) fn resume(&self) {
        must_ok!(self.tx.send(GC_RESUME));
        self.sem.wait();
    }

    pub(crate) fn start(&self) {
        must_ok!(self.tx.send(GC_START));
        self.sem.wait();
    }

    pub(crate) fn wal_clean(&self, manifest: Handle<Manifest>, ctx: Handle<Context>) {
        if self.tx.send(GC_WAL).is_err() {
            let mut gc = GarbageCollector {
                sequences: ctx.sequences.clone(),
                ctx,
                store: MutRef::default(),
                data_runs: Arc::new(AtomicU64::new(0)),
                blob_runs: Arc::new(AtomicU64::new(0)),
            };
            gc.process_wal_clean_with_manifest(manifest);
        }
    }

    pub(crate) fn data_gc_count(&self) -> u64 {
        self.data_runs.load(Relaxed)
    }

    pub(crate) fn blob_gc_count(&self) -> u64 {
        self.blob_runs.load(Relaxed)
    }
}

pub(crate) fn start_gc(store: MutRef<Store>, ctx: Handle<Context>) -> GCHandle {
    let (tx, rx) = channel();
    let sem = Arc::new(Countblock::new(0));
    let data_runs = Arc::new(AtomicU64::new(0));
    let blob_runs = Arc::new(AtomicU64::new(0));
    let gc = GarbageCollector {
        sequences: ctx.sequences.clone(),
        ctx,
        store,
        data_runs: data_runs.clone(),
        blob_runs: blob_runs.clone(),
    };
    gc_thread(gc, rx, sem.clone());
    GCHandle {
        tx: Arc::new(tx),
        sem,
        data_runs,
        blob_runs,
    }
}

pub(crate) fn drain_abort_clean_during_recovery(
    store: MutRef<Store>,
    ctx: Handle<Context>,
    force_fsync: bool,
) -> Result<(), OpCode> {
    drain_abort_clean(store, ctx, force_fsync)
}

fn drain_abort_clean(
    store: MutRef<Store>,
    ctx: Handle<Context>,
    force_fsync: bool,
) -> Result<(), OpCode> {
    let mut gc = GarbageCollector {
        sequences: ctx.sequences.clone(),
        ctx,
        store,
        data_runs: Arc::new(AtomicU64::new(0)),
        blob_runs: Arc::new(AtomicU64::new(0)),
    };
    gc.run_abort_clean_recovery(force_fsync)
}

/// drain abort-clean before the final exit checkpoint
pub(crate) fn drain_abort_clean_at_exit(store: MutRef<Store>, ctx: Handle<Context>) {
    must_ok!(drain_abort_clean(store, ctx, false));
}

macro_rules! kind_select {
    ($kind:expr, data => $data:expr, blob => $blob:expr $(,)?) => {
        match $kind {
            FileKind::Data => $data,
            FileKind::Blob => $blob,
        }
    };
}

struct GarbageCollector {
    sequences: Arc<Sequences>,
    ctx: Handle<Context>,
    store: MutRef<Store>,
    data_runs: Arc<AtomicU64>,
    blob_runs: Arc<AtomicU64>,
}

mod abort_clean;
mod bucket_cleanup;
mod obsolete;
mod rewrite;
mod wal_recycle;

impl GarbageCollector {
    const MAX_ELEMS: usize = 1024 * 100;

    fn run(&mut self) {
        #[cfg(feature = "metrics")]
        let started = Instant::now();
        #[cfg(feature = "metrics")]
        self.store.opt.observer.counter(CounterMetric::GcRun, 1);
        let _ = self.process_abort_clean();
        self.process_wal_clean();
        for kind in FileKind::ALL {
            self.process_files(kind);
        }
        must_ok!(self.process_pending_buckets());
        must_ok!(self.store.manifest.delete_files());
        #[cfg(feature = "metrics")]
        self.store.opt.observer.histogram(
            HistogramMetric::GcRunMicros,
            started.elapsed().as_micros() as u64,
        );
        #[cfg(feature = "extra_check")]
        crate::testing::fire_gc_completed();
    }

    fn next_tick(&self, bucket_id: u64, kind: FileKind) -> u64 {
        self.store
            .manifest
            .buckets
            .get_runtime(bucket_id)
            .load_update_epoch(kind)
    }

    fn alloc_file_id(&self) -> u64 {
        self.sequences.next_file_id.fetch_add(1, Relaxed)
    }

    fn target_ratio(&self, kind: FileKind) -> u64 {
        match kind {
            FileKind::Data => self.store.opt.data_garbage_ratio as u64,
            FileKind::Blob => self.store.opt.blob_garbage_ratio as u64,
        }
    }

    fn target_file_size(&self, kind: FileKind) -> usize {
        match kind {
            FileKind::Data => self.store.opt.data_file_size,
            FileKind::Blob => self.store.opt.blob_file_size,
        }
    }

    fn file_runs(&self, kind: FileKind) -> &AtomicU64 {
        match kind {
            FileKind::Data => self.data_runs.as_ref(),
            FileKind::Blob => self.blob_runs.as_ref(),
        }
    }

    fn process_files(&mut self, kind: FileKind) {
        let tgt_ratio = self.target_ratio(kind);
        let tgt_size = self.target_file_size(kind);
        let eager = self.store.opt.gc_eager;
        let (obsoleted, bucket_usage, candidates) = self.collect_candidates(kind);

        self.process_obsoleted_batches(kind, &obsoleted);

        let ranked = Self::rank_candidates(candidates, |bucket_id| self.next_tick(bucket_id, kind));
        if ranked.is_empty() {
            return;
        }

        let plans =
            Self::plan_rewrite_from_global(ranked, &bucket_usage, tgt_ratio, tgt_size, eager);
        for (bucket_id, victim) in plans {
            if !self.should_run_rewrite_live(kind, bucket_id, tgt_ratio) {
                continue;
            }
            self.rewrite_files(kind, &victim, bucket_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::abort_clean::{
        AbortCleanChainBounds, consume_abort_clean_step, validate_abort_clean_link,
    };
    use super::rewrite::{
        Entry, PendingReloc, RewriteBuilder, RewriteItem, Score, build_sorted_relocs,
        split_rewrite_items,
    };
    use crate::{
        OpCode, Options, RandomPath,
        map::data::{FileBuilder, FileVersion, MetaReader},
        meta::{FileKind, StatInner},
        types::{
            header::{DeltaHeader, NodeType, TagKind},
            refbox::BoxRef,
            traits::IHeader,
        },
        utils::{INIT_ID, compress::CompressorPool, data::Position},
    };

    #[test]
    fn abort_clean_chain_rejects_invalid_links_and_budget_exhaustion() {
        let bounds = AbortCleanChainBounds {
            lower: Position::new(3, 0),
            upper: Position::new(4, 100),
            step_budget: 2,
        };
        let cursor = Position::new(4, 80);

        assert_eq!(
            validate_abort_clean_link(bounds, cursor, cursor),
            Err(OpCode::Corruption)
        );
        assert_eq!(
            validate_abort_clean_link(bounds, cursor, Position::new(4, 81)),
            Err(OpCode::Corruption)
        );
        assert_eq!(
            validate_abort_clean_link(bounds, cursor, Position::new(2, 99)),
            Err(OpCode::Corruption)
        );
        assert_eq!(
            validate_abort_clean_link(bounds, Position::new(5, 0), Position::new(4, 79)),
            Err(OpCode::Corruption)
        );
        assert_eq!(
            validate_abort_clean_link(bounds, cursor, Position::new(4, 79)),
            Ok(Position::new(4, 79))
        );

        let mut remaining = bounds.step_budget;
        assert_eq!(consume_abort_clean_step(&mut remaining), Ok(()));
        assert_eq!(consume_abort_clean_step(&mut remaining), Ok(()));
        assert_eq!(
            consume_abort_clean_step(&mut remaining),
            Err(OpCode::Corruption)
        );
    }

    fn sample_pages() -> [BoxRef; 2] {
        let (pid, addr) = (114514, 1919810);
        let mut p = BoxRef::alloc(233, addr);
        p.header_mut().pid = pid;
        p.header_mut().kind = TagKind::Delta;
        p.header_mut().node_type = NodeType::Leaf;

        let (pid1, addr1) = (192, 68);
        let mut p1 = BoxRef::alloc(666, addr1);
        p1.header_mut().pid = pid1;
        p1.header_mut().kind = TagKind::Delta;
        p1.header_mut().node_type = NodeType::Leaf;
        [p, p1]
    }

    #[test]
    fn build_sorted_relocs_orders_by_addr_and_reassigns_seq() {
        let mut pending = vec![
            PendingReloc::new(30, 300, 30, 0, 3),
            PendingReloc::new(10, 100, 10, 4, 1),
            PendingReloc::new(20, 200, 20, 0, 2),
        ];

        let (relocs, map) = build_sorted_relocs(&mut pending);

        assert_eq!(relocs.len(), 3);
        let key0 = relocs[0].key;
        let seq0 = relocs[0].val.seq;
        let key1 = relocs[1].key;
        let seq1 = relocs[1].val.seq;
        let key2 = relocs[2].key;
        let seq2 = relocs[2].val.seq;
        assert_eq!(key0, 10);
        assert_eq!(seq0, 0);
        assert_eq!(key1, 20);
        assert_eq!(seq1, 1);
        assert_eq!(key2, 30);
        assert_eq!(seq2, 2);

        assert_eq!(map.get(&10).unwrap().seq, 0);
        assert_eq!(map.get(&10).unwrap().compressed_len, 4);
        assert_eq!(map.get(&20).unwrap().seq, 1);
        assert_eq!(map.get(&30).unwrap().seq, 2);
    }

    #[test]
    fn score_uses_shared_data_and_blob_formula() {
        let now = 100;
        let data = StatInner {
            file_id: 7,
            up1: 90,
            up2: 80,
            active_elems: 4,
            total_elems: 8,
            active_size: 40,
            total_size: 100,
            bucket_id: 3,
        };
        let blob = StatInner {
            file_id: 7,
            up1: 90,
            up2: 80,
            active_elems: 4,
            total_elems: 8,
            active_size: 40,
            total_size: 100,
            bucket_id: 3,
        };

        let data_score = Score::from(data, now);
        let blob_score = Score::from(blob, now);

        assert_eq!(data_score.rate, blob_score.rate);
        assert_eq!(data_score.size, blob_score.size);
        assert_eq!(data_score.up2, blob_score.up2);
    }

    fn test_score(id: u64, size: usize) -> Score {
        Score {
            id,
            size,
            rate: 1.0,
            up2: 0,
            bucket_id: 0,
        }
    }

    #[test]
    fn rewrite_batches_split_eager_tail_near_target() {
        let batches = super::GarbageCollector::select_rewrite_batches_for_bucket(
            vec![
                test_score(1, 40),
                test_score(2, 40),
                test_score(3, 40),
                test_score(4, 40),
            ],
            100,
            true,
        );
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].iter().map(|x| x.size).sum::<usize>(), 80);
        assert_eq!(batches[1].iter().map(|x| x.size).sum::<usize>(), 80);
    }

    #[test]
    fn rewrite_batches_non_eager_wait_for_target() {
        assert!(
            super::GarbageCollector::select_rewrite_batches_for_bucket(
                vec![test_score(1, 40), test_score(2, 40)],
                100,
                false,
            )
            .is_empty()
        );
        let batches = super::GarbageCollector::select_rewrite_batches_for_bucket(
            vec![test_score(1, 40), test_score(2, 40), test_score(3, 40)],
            100,
            false,
        );
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].iter().map(|x| x.size).sum::<usize>(), 120);
    }

    #[test]
    fn rewrite_batches_non_eager_keeps_underfilled_batch_across_oversized_victim() {
        let batches = super::GarbageCollector::select_rewrite_batches_for_bucket(
            vec![
                test_score(1, 40),
                test_score(2, 40),
                test_score(3, 150),
                test_score(4, 40),
            ],
            100,
            false,
        );
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].iter().map(|x| x.id).collect::<Vec<_>>(), vec![3]);
        assert_eq!(
            batches[1].iter().map(|x| x.id).collect::<Vec<_>>(),
            vec![1, 2, 4]
        );
        assert_eq!(batches[1].iter().map(|x| x.size).sum::<usize>(), 120);
    }

    #[test]
    fn rewrite_batches_keep_oversized_victim_alone() {
        let batches = super::GarbageCollector::select_rewrite_batches_for_bucket(
            vec![test_score(1, 150), test_score(2, 40), test_score(3, 40)],
            100,
            true,
        );
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].iter().map(|x| x.id).collect::<Vec<_>>(), vec![1]);
        assert_eq!(
            batches[1].iter().map(|x| x.id).collect::<Vec<_>>(),
            vec![2, 3]
        );
    }

    #[test]
    fn oversized_rewrite_victim_is_split_by_records() {
        let items = vec![RewriteItem::new(
            7,
            1,
            vec![
                Entry {
                    key: 10,
                    interval_lo: 10,
                    raw_len: 60,
                    compressed_len: 0,
                },
                Entry {
                    key: 20,
                    interval_lo: 10,
                    raw_len: 60,
                    compressed_len: 0,
                },
                Entry {
                    key: 30,
                    interval_lo: 10,
                    raw_len: 150,
                    compressed_len: 0,
                },
            ],
        )];
        let chunks = split_rewrite_items(items, 100, false);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].iter().map(|x| x.live_bytes).sum::<usize>(), 60);
        assert_eq!(chunks[1].iter().map(|x| x.live_bytes).sum::<usize>(), 60);
        assert_eq!(chunks[2].iter().map(|x| x.live_bytes).sum::<usize>(), 150);
    }

    #[test]
    fn rewrite_builder_emits_v1() {
        let path = RandomPath::tmp();
        let mut opt = Options::new(&*path);
        opt.tmp_store = true;
        let opt = opt.validate().unwrap();
        let _ = opt.create_dir();

        let [p, p1] = sample_pages();
        let mut file_id = INIT_ID;
        let mut writer = FileBuilder::new(0, false, CompressorPool::new(), opt.fs.clone());
        writer.add(p);
        writer.add(p1);
        let files = writer.flush_files(
            FileKind::Data,
            opt.data_file_size,
            7,
            || {
                let id = file_id;
                file_id += 1;
                (id, opt.data_file(id))
            },
            |_| {},
            |_| {},
        );
        assert_eq!(files.len(), 1);

        let mut loader = MetaReader::new(opt.fs.as_ref(), opt.data_file(INIT_ID));
        assert_eq!(loader.version(), FileVersion::V1);
        let relocs = loader.get_reloc();
        let intervals = loader.get_interval();

        let mut builder =
            RewriteBuilder::new(INIT_ID + 1, &opt, 1, 0, 9, false, CompressorPool::new());
        for ivl in intervals.iter().copied() {
            builder.add_interval(ivl.lo, ivl.hi);
        }
        let active = relocs
            .iter()
            .map(|m| Entry {
                key: m.key,
                interval_lo: intervals[0].lo,
                raw_len: m.val.raw_len(),
                compressed_len: m.val.compressed_len(),
            })
            .collect();
        builder.add_item(RewriteItem::new(INIT_ID, 7, active));

        let (_stat, _reloc_map) = builder.build(FileKind::Data).expect("rewrite must succeed");
        let reopened = MetaReader::new(opt.fs.as_ref(), opt.data_file(INIT_ID + 1));
        assert_eq!(reopened.version(), FileVersion::V1);
    }

    #[test]
    fn rewrite_builder_keeps_compressed_v1_records_compressed() {
        let path = RandomPath::tmp();
        let mut opt = Options::new(&*path);
        opt.tmp_store = true;
        let opt = opt.validate().unwrap();
        let _ = opt.create_dir();

        let mut page = BoxRef::alloc(4096, 114514);
        page.header_mut().pid = 1919810;
        page.header_mut().kind = TagKind::Delta;
        page.header_mut().node_type = NodeType::Leaf;
        page.data_slice_mut::<u8>()[size_of::<DeltaHeader>()..].fill(b'x');

        let mut file_id = INIT_ID;
        let mut writer = FileBuilder::new(0, true, CompressorPool::new(), opt.fs.clone());
        writer.add(page);
        let files = writer.flush_files(
            FileKind::Data,
            opt.data_file_size,
            7,
            || {
                let id = file_id;
                file_id += 1;
                (id, opt.data_file(id))
            },
            |_| {},
            |_| {},
        );
        assert_eq!(files.len(), 1);

        let mut loader = MetaReader::new(opt.fs.as_ref(), opt.data_file(INIT_ID));
        let relocs = loader.get_reloc();
        assert_eq!(relocs.len(), 1);
        assert!(relocs[0].val.compressed_len() > 0);
        let intervals = loader.get_interval();

        let mut builder =
            RewriteBuilder::new(INIT_ID + 1, &opt, 1, 0, 9, true, CompressorPool::new());
        for ivl in intervals.iter().copied() {
            builder.add_interval(ivl.lo, ivl.hi);
        }
        let active = relocs
            .iter()
            .map(|m| Entry {
                key: m.key,
                interval_lo: intervals[0].lo,
                raw_len: m.val.raw_len(),
                compressed_len: m.val.compressed_len(),
            })
            .collect();
        builder.add_item(RewriteItem::new(INIT_ID, 7, active));

        let (_stat, _reloc_map) = builder.build(FileKind::Data).expect("rewrite must succeed");
        let mut rewritten = MetaReader::new(opt.fs.as_ref(), opt.data_file(INIT_ID + 1));
        let rewritten_relocs = rewritten.get_reloc();
        assert_eq!(rewritten_relocs.len(), 1);
        assert!(rewritten_relocs[0].val.compressed_len() > 0);
    }
}
