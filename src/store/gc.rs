use crc32c::Crc32cHasher;
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap},
    hash::Hasher,
    ops::Deref,
    sync::{
        Arc,
        atomic::Ordering::Relaxed,
        mpsc::{Receiver, RecvTimeoutError, Sender, channel},
    },
    thread::JoinHandle,
    time::Duration,
};

use crate::{
    Options, Store,
    cc::context::Context,
    io::{File, GatherIO},
    map::data::{BlobFooter, DataFooter, MetaReader},
    meta::{
        BlobStatInner, DataStatInner, DelInterval, Delete, FileId, IntervalPair, MemBlobStat,
        MemDataStat, MetaKind, Numerics, TxnKind,
    },
    types::traits::IAsSlice,
    utils::{
        Handle, MutRef,
        bitmap::BitMap,
        block::Block,
        countblock::Countblock,
        data::{AddrPair, GatherWriter, Interval, LenSeq},
    },
};

const GC_QUIT: i32 = -1;
const GC_PAUSE: i32 = 3;
const GC_RESUME: i32 = 5;
const GC_START: i32 = 7;

fn gc_thread(mut gc: GarbageCollector, rx: Receiver<i32>, sem: Arc<Countblock>) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("garbage-collector".into())
        .spawn(move || {
            let timeout = Duration::from_millis(gc.store.opt.gc_timeout);
            let mut pause = false;

            loop {
                match rx.recv_timeout(timeout) {
                    Ok(x) => match x {
                        GC_PAUSE => {
                            pause = true;
                        }
                        GC_RESUME => {
                            pause = false;
                        }
                        GC_START => {
                            gc.run();
                            sem.post();
                        }
                        GC_QUIT => break,
                        _ => unreachable!("invalid instruction  {}", x),
                    },
                    Err(RecvTimeoutError::Timeout) => {
                        if !pause {
                            gc.run();
                        }
                    }
                    Err(e) => {
                        log::error!("gc receive error {e}");
                        break;
                    }
                }
            }

            sem.post();
            log::info!("garbage-collector thread exit");
        })
        .unwrap()
}

#[derive(Clone)]
pub(crate) struct GCHandle {
    tx: Arc<Sender<i32>>,
    sem: Arc<Countblock>,
}

impl GCHandle {
    pub(crate) fn quit(&self) {
        self.tx.send(GC_QUIT).unwrap();
        self.sem.wait();
    }

    pub(crate) fn pause(&self) {
        self.tx.send(GC_PAUSE).unwrap();
    }

    pub(crate) fn resume(&self) {
        self.tx.send(GC_RESUME).unwrap();
    }

    pub(crate) fn start(&self) {
        self.tx.send(GC_START).unwrap();
        self.sem.wait();
    }
}

pub(crate) fn start_gc(store: MutRef<Store>, ctx: Handle<Context>) -> GCHandle {
    let (tx, rx) = channel();
    let sem = Arc::new(Countblock::new(1));
    let gc = GarbageCollector {
        numerics: ctx.numerics.clone(),
        ctx,
        store,
    };
    gc_thread(gc, rx, sem.clone());
    GCHandle {
        tx: Arc::new(tx),
        sem,
    }
}

#[derive(Clone, Copy, Debug)]
struct Score {
    id: u64,
    size: usize,
    rate: f64,
    up2: u64,
}

impl Score {
    fn from(stat: &MemDataStat, now: u64) -> Self {
        Self {
            id: stat.file_id,
            size: stat.active_size,
            rate: Self::calc_decline_rate(stat, now),
            up2: stat.up2,
        }
    }

    fn calc_decline_rate(stat: &MemDataStat, now: u64) -> f64 {
        let free = stat.total_size.saturating_sub(stat.active_size);
        // no junk has been applied yet, or
        // it's possible gc and flush thread get same tick
        if free == 0 || stat.up2 == now {
            return f64::MIN;
        }

        -(stat.active_size as f64 / free as f64).powi(2)
            / (stat.total_elems as f64 * (now - stat.up2) as f64)
    }
}

impl Ord for Score {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match other.rate.partial_cmp(&self.rate) {
            Some(Ordering::Equal) => self.id.cmp(&other.id),
            Some(o) => o,
            None => Ordering::Equal,
        }
    }
}

impl PartialOrd for Score {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Score {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Score {}

struct GarbageCollector {
    numerics: Arc<Numerics>,
    ctx: Handle<Context>,
    store: MutRef<Store>,
}

impl GarbageCollector {
    const MAX_ELEMS: usize = 1024;

    fn run(&mut self) {
        self.process_wal();
        self.process_manifest();
        self.process_data();
        self.process_blob();
    }

    /// with SSD support, for TB level data, directly write manifest snapshot is acceptable
    fn process_manifest(&mut self) {
        self.ctx.manifest.try_clean();
    }

    fn process_obsoleted_blob(&self, obsoleted: Vec<u64>) {
        if !obsoleted.is_empty() {
            let mut unlinked = Delete::default();
            let mut del_intervals = DelInterval::default();
            obsoleted.iter().for_each(|&x| {
                let mut loader = MetaReader::<BlobFooter>::new(self.store.opt.blob_file(x))
                    .expect("never happen");
                let ivls = loader.get_interval().unwrap();
                for i in ivls {
                    del_intervals.push(i.lo);
                }
                unlinked.push(x);
            });
            let mut txn = self.ctx.manifest.begin(TxnKind::BlobGC);
            txn.record(MetaKind::BlobDelete, &unlinked);
            txn.record(MetaKind::BlobDelInterval, &del_intervals);
            txn.commit();

            self.ctx
                .manifest
                .blob_stat
                .remove_stat_interval(&obsoleted, &del_intervals);
            self.ctx.manifest.save_obsolete_blob(&obsoleted);
            self.ctx.manifest.delete_files();
        }
    }

    fn process_obsoleted_data(&self, obsoleted: Vec<u64>) {
        if !obsoleted.is_empty() {
            let mut unlinked = Delete::default();
            let mut del_intervals = DelInterval::default();
            obsoleted.iter().for_each(|&x| {
                let mut loader = MetaReader::<DataFooter>::new(self.store.opt.data_file(x))
                    .expect("never happen");
                let ivls = loader.get_interval().unwrap();
                for i in ivls {
                    del_intervals.push(i.lo);
                }
                unlinked.push(x);
            });
            let mut txn = self.ctx.manifest.begin(TxnKind::DataGC);
            txn.record(MetaKind::DataDelete, &unlinked);
            txn.record(MetaKind::DataDelInterval, &del_intervals);
            txn.commit();

            self.ctx
                .manifest
                .data_stat
                .remove_stat_interval(&obsoleted, &del_intervals);
            self.ctx.manifest.save_obsolete_data(&obsoleted);
            self.ctx.manifest.delete_files();
        }
    }

    fn process_blob(&mut self) {
        let (obsoleted, victims) = self.ctx.manifest.blob_stat.get_victims(
            self.store.opt.blob_gc_ratio,
            self.store.opt.blob_garbage_ratio,
        );

        self.process_obsoleted_blob(obsoleted);

        let mut dst_size = 0;
        let mut dst = Vec::new();
        for (file_id, size) in victims {
            dst_size += size;
            dst.push(file_id);
            if dst_size >= self.store.opt.blob_max_size && dst.len() >= 2 {
                self.rewrite_blob(&dst);
                dst.clear();
                dst_size = 0;
            } else {
                break;
            }
        }
    }

    fn process_data(&mut self) {
        let tgt_ratio = self.store.opt.data_garbage_ratio as u64;
        let tgt_size = self.store.opt.gc_compacted_size;
        let eager = self.store.opt.gc_eager;

        let (total, active) = self.ctx.manifest.data_stat.total_active();
        if total == 0 {
            return;
        }
        let ratio = (total - active) * 100 / total;
        // log::trace!("ratio {ratio} tgt_ratio {tgt_ratio} total {total} active {active}");
        if ratio < tgt_ratio {
            return;
        }

        let tick = self.numerics.next_data_id.load(Relaxed);
        let mut heap = BinaryHeap::new();
        let mut obsoleted = Vec::new();
        self.ctx.manifest.data_stat.iter().for_each(|x| {
            let s = x.value();
            // a data file has no active frame will be unlinked directly
            if s.active_elems == 0 {
                obsoleted.push(s.file_id);
            } else {
                let score = Score::from(s, tick);
                if heap.len() < Self::MAX_ELEMS {
                    heap.push(score);
                } else {
                    let top = heap.peek().unwrap();
                    // use is_gt here, see Score's Ord impl
                    if top.cmp(&score).is_gt() {
                        heap.pop();
                        heap.push(score);
                    }
                }
            }
        });

        self.process_obsoleted_data(obsoleted);

        let mut victims = vec![];
        let mut sum = 0;
        let mut tmp = Vec::from_iter(heap); // reclaim max rate first
        while let Some(x) = tmp.pop() {
            sum += x.size;
            victims.push(x);

            if sum >= tgt_size && victims.len() > 1 {
                self.rewrite_data(victims);
                return;
            }
        }
        if eager && victims.len() > 1 {
            self.rewrite_data(victims);
        }
    }

    fn process_wal(&mut self) {
        for w in self.store.context.workers().iter() {
            let lk = w.start_ckpt.read().unwrap();
            let checkpoint_id = lk.file_id;
            drop(lk);
            let lk = w.logging.desc.lock().unwrap();
            let oldest_id = lk.oldest_id;
            drop(lk);

            if oldest_id == checkpoint_id {
                continue;
            }

            // [oldest_id, checkpoint_id)
            Self::process_one_wal(&self.store.opt, w.id, oldest_id, checkpoint_id);
            let mut desc = w.logging.desc.lock().unwrap();
            desc.update_oldest(self.ctx.opt.desc_file(w.id), checkpoint_id);
        }
    }

    fn process_one_wal(opt: &Options, id: u16, beg: u64, end: u64) {
        // NOTE: not including `end`
        for seq in beg..end {
            let from = opt.wal_file(id, seq);
            if !from.exists() {
                continue;
            }
            let to = opt.wal_backup(id, seq);
            if opt.keep_stable_wal_file {
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
        }
    }

    fn rewrite_data(&mut self, candidate: Vec<Score>) {
        let opt = &self.store.opt;
        let file_id = self.numerics.next_data_id.fetch_add(1, Relaxed);
        let mut builder = DataReWriter::new(file_id, opt, candidate.len());
        let mut remap_intervals = Vec::with_capacity(candidate.len());
        let mut del_intervals = DelInterval::default();
        let mut obsoleted = Vec::new();

        self.ctx.manifest.data_stat.start_collect_junks(); // stop in update_stat_interval
        let victims: Vec<u64> = candidate
            .iter()
            .filter_map(|x| {
                let mut loader =
                    MetaReader::<DataFooter>::new(opt.data_file(x.id)).expect("never happen");
                let relocs = loader.get_reloc().unwrap();
                let ivls = loader.get_interval().unwrap();
                let mut im = InactiveMap::new(ivls);
                let stat = self.ctx.manifest.data_stat.get(&x.id).expect("must exist");
                let bitmap = stat.mask.clone();
                drop(stat);

                // collect active frames
                let active: Vec<Entry> = relocs
                    .iter()
                    .filter(|m| !bitmap.test(m.val.seq))
                    .map(|m| {
                        // test here because bitmap maybe full of garbage, it must be ignore
                        im.test(m.key);
                        Entry {
                            key: m.key,
                            off: m.val.off,
                            len: m.val.len,
                            crc: m.val.crc,
                        }
                    })
                    .collect();

                if active.is_empty() {
                    obsoleted.push(x.id);
                    return None;
                }
                im.collect(|unref, ivl| {
                    let Interval { lo, hi } = ivl;
                    if unref {
                        del_intervals.push(lo);
                    } else {
                        remap_intervals.push(IntervalPair::new(lo, hi, file_id));
                        builder.add_interval(lo, hi);
                    }
                });
                builder.add_frame(Item::new(x.id, x.up2, active));
                Some(x.id)
            })
            .collect();

        // it's possible that other thread deactived all data in data file while we are procesing
        self.process_obsoleted_data(obsoleted);
        // it's also possible that frames in all candidate were obsoleted, in this case one data id
        // is wasted, and a footer will be flush to data file, it will be removed in the future

        let mut txn = self.ctx.manifest.begin(TxnKind::DataGC);

        txn.record(MetaKind::FileId, &FileId::data(file_id));
        txn.sync(); // necessary, before data file was flushed

        txn.record(MetaKind::Numerics, self.ctx.manifest.numerics.deref());

        // data file must be flushed after FileId flushed and before txn commit
        let (fstat, relocs) = builder
            .build()
            .inspect_err(|e| {
                log::error!("error {e}");
            })
            .unwrap();

        // the only problem is junks collected by flush thread maybe too many
        let stat = self.ctx.manifest.update_data_stat_interval(
            fstat,
            relocs,
            &victims,
            &del_intervals,
            &remap_intervals,
        );

        txn.record(MetaKind::DataStat, &stat);

        // 1. record delete first
        if !del_intervals.is_empty() {
            txn.record(MetaKind::DataDelInterval, &del_intervals);
        }
        // 2. then record remapping, old intervals are point to new file_id
        for i in &remap_intervals {
            txn.record(MetaKind::DataInterval, i);
        }
        // in case crash happens before/during deleting files
        let tmp: Delete = victims.into();
        txn.record(MetaKind::DataDelete, &tmp);

        txn.commit();

        // 3. it's safe to clean obsolete files, becuase they are not referenced
        self.ctx.manifest.save_obsolete_data(&tmp);
        self.ctx.manifest.delete_files();
    }

    fn rewrite_blob(&mut self, candidate: &[u64]) {
        let opt = &self.ctx.opt;
        let mut remap_intervals = Vec::new();
        let mut del_intervals = DelInterval::default();
        let mut builder = BlobRewriter::new(opt);
        let blob_id = self.numerics.next_blob_id.fetch_add(1, Relaxed);
        let mut obsoleted = Vec::new();

        self.ctx.manifest.blob_stat.start_collect_junks();
        let victims: Vec<u64> = candidate
            .iter()
            .filter_map(|&victim_id| {
                let mut loader =
                    MetaReader::<BlobFooter>::new(opt.blob_file(victim_id)).expect("never happen");
                let relocs = loader.get_reloc().unwrap();
                let map = self.ctx.manifest.blob_stat.read().unwrap();
                // save a copy so don't block other thread
                let bitmap = map.get(&victim_id).unwrap().mask.clone();
                drop(map);
                let ivls = loader.get_interval().unwrap();
                let mut im = InactiveMap::new(ivls);

                let active: Vec<Entry> = relocs
                    .iter()
                    .filter(|x| !bitmap.test(x.val.seq))
                    .map(|x| {
                        // test here because bitmap maybe full of garbage, it must be ignore
                        im.test(x.key);
                        Entry {
                            key: x.key,
                            off: x.val.off,
                            len: x.val.len,
                            crc: x.val.crc,
                        }
                    })
                    .collect();

                if active.is_empty() {
                    obsoleted.push(victim_id);
                    return None;
                }
                im.collect(|unref, ivl| {
                    let Interval { lo, hi } = ivl;
                    if unref {
                        del_intervals.push(lo);
                    } else {
                        remap_intervals.push(IntervalPair::new(lo, hi, blob_id));
                        builder.add_interval(lo, hi);
                    }
                });
                builder.add_item(BlobItem::new(victim_id, active));
                Some(victim_id)
            })
            .collect();

        // it's possible that other thread deactived all data in blob file while we are procesing
        self.process_obsoleted_blob(obsoleted);
        // it's also possible that frames in all candidate were obsoleted, in this case one blob id
        // is wasted, and a footer will be flush to blob file, it will be removed in the future

        let mut txn = self.ctx.manifest.begin(TxnKind::BlobGC);

        txn.record(MetaKind::FileId, &FileId::blob(blob_id));
        txn.sync(); // necessary, before blob file was flushed

        txn.record(MetaKind::Numerics, self.ctx.manifest.numerics.deref());
        let (bstat, reloc) = builder
            .build(blob_id)
            .inspect_err(|e| {
                log::error!("error {e:?}");
            })
            .unwrap();
        let stat = self.ctx.manifest.update_blob_stat_interval(
            bstat,
            reloc,
            &victims,
            &del_intervals,
            &remap_intervals,
        );
        txn.record(MetaKind::BlobStat, &stat);

        if !del_intervals.is_empty() {
            txn.record(MetaKind::BlobDelInterval, &del_intervals);
        }

        for i in &remap_intervals {
            txn.record(MetaKind::BlobInterval, i);
        }

        let tmp: Delete = victims.into();
        txn.record(MetaKind::BlobDelete, &tmp);
        txn.commit();

        self.ctx.manifest.save_obsolete_blob(&tmp);
        self.ctx.manifest.delete_files();
    }
}

struct DataReWriter<'a> {
    file_id: u64,
    items: Vec<Item>,
    intervals: Vec<u8>,
    nr_interval: u32,
    sum_up2: u64,
    total: u64,
    opt: &'a Options,
}

impl<'a> DataReWriter<'a> {
    fn new(file_id: u64, opt: &'a Options, cap: usize) -> Self {
        Self {
            file_id,
            items: Vec::with_capacity(cap),
            intervals: Vec::with_capacity(cap),
            nr_interval: 0,
            sum_up2: 0,
            total: cap as u64,
            opt,
        }
    }

    fn add_frame(&mut self, item: Item) {
        self.sum_up2 += item.up2;
        self.items.push(item);
    }

    fn add_interval(&mut self, lo: u64, hi: u64) {
        let ivl = Interval::new(lo, hi);
        self.intervals.extend_from_slice(ivl.as_slice());
        self.nr_interval += 1;
    }

    fn build(&mut self) -> Result<(MemDataStat, HashMap<u64, LenSeq>), std::io::Error> {
        let up2 = self.sum_up2 / self.total;
        let block = Block::alloc(1 << 20);
        let mut seq = 0;
        let mut off = 0;
        let path = self.opt.data_file(self.file_id);
        let mut writer = GatherWriter::append(&path, 128);
        let mut reloc: Vec<u8> = Vec::new();
        let mut reloc_map = HashMap::new();
        let buf = block.mut_slice(0, block.len());

        self.items.sort_unstable_by(|x, y| x.id.cmp(&y.id));

        for item in &self.items {
            let reader = File::options()
                .read(true)
                .open(self.opt.data_file(item.id))
                .unwrap();
            for e in &item.pos {
                let len = e.len as usize;
                let crc = copy(&reader, &mut writer, buf, len, e.off as u64)?;
                assert_eq!(crc, e.crc);
                let m = AddrPair::new(e.key, off, e.len, seq, crc);
                reloc.extend_from_slice(m.as_slice());
                reloc_map.insert(e.key, LenSeq::new(e.len, seq));
                off += len;
                seq += 1;
            }
        }

        let mut interval_crc = Crc32cHasher::default();
        let is = self.intervals.as_slice();
        interval_crc.write(is);
        writer.queue(is);

        let mut reloc_crc = Crc32cHasher::default();
        let s = reloc.as_slice();
        reloc_crc.write(s);
        writer.queue(s);

        let footer = DataFooter {
            up2,
            nr_reloc: seq,
            nr_intervals: self.nr_interval,
            reloc_crc: reloc_crc.finish() as u32,
            interval_crc: interval_crc.finish() as u32,
        };

        writer.queue(footer.as_slice());

        writer.flush();
        writer.sync();
        log::info!("compacted to {path:?} {footer:?}");

        let stat = MemDataStat {
            inner: DataStatInner {
                file_id: self.file_id,
                up1: up2,
                up2,
                active_elems: seq,
                total_elems: seq,
                active_size: off,
                total_size: off,
            },
            mask: BitMap::new(seq),
        };
        Ok((stat, reloc_map))
    }
}

struct BlobRewriter<'a> {
    opt: &'a Options,
    items: Vec<BlobItem>,
    intervals: Vec<u8>,
    nr_interval: u32,
}

impl<'a> BlobRewriter<'a> {
    fn new(opt: &'a Options) -> Self {
        Self {
            opt,
            items: Vec::new(),
            intervals: Vec::new(),
            nr_interval: 0,
        }
    }

    fn add_item(&mut self, item: BlobItem) {
        self.items.push(item);
    }

    fn add_interval(&mut self, lo: u64, hi: u64) {
        let ivl = Interval::new(lo, hi);
        self.intervals.extend_from_slice(ivl.as_slice());
        self.nr_interval += 1;
    }

    fn build(
        &mut self,
        file_id: u64,
    ) -> Result<(MemBlobStat, HashMap<u64, LenSeq>), std::io::Error> {
        let path = self.opt.blob_file(file_id);
        let mut w = GatherWriter::trunc(&path, 8);
        let mut off = 0;
        let mut seq = 0;
        let mut reloc = Vec::new();
        let mut map = HashMap::new();
        let block = Block::alloc(4 << 20);
        let buf = block.mut_slice(0, block.len());

        #[cfg(feature = "extra_check")]
        assert!(self.items.is_sorted_by_key(|x| x.id));

        let mut beg = u64::MAX;
        let mut end = u64::MIN;
        for item in &self.items {
            beg = beg.min(item.id);
            end = end.max(item.id);

            let reader = File::options()
                .read(true)
                .open(self.opt.blob_file(item.id))
                .unwrap();

            for e in &item.pos {
                let len = e.len as usize;
                let crc = copy(&reader, &mut w, buf, len, e.off as u64)?;
                assert_eq!(crc, e.crc);
                let m = AddrPair::new(e.key, off, e.len, seq, crc);
                reloc.extend_from_slice(m.as_slice());
                map.insert(e.key, LenSeq::new(e.len, seq));
                off += len;
                seq += 1;
            }
        }

        let mut interval_crc = Crc32cHasher::default();
        let is = self.intervals.as_slice();
        interval_crc.write(is);
        w.queue(is);

        let mut reloc_crc = Crc32cHasher::default();
        let rs = reloc.as_slice();
        reloc_crc.write(rs);
        w.queue(rs);

        let footer = BlobFooter {
            nr_reloc: seq,
            nr_intervals: self.nr_interval,
            reloc_crc: reloc_crc.finish() as u32,
            interval_crc: interval_crc.finish() as u32,
        };

        w.queue(footer.as_slice());
        w.flush();
        w.sync();
        log::error!("compacted [{beg}, {end}] to {path:?} {footer:?}");
        let stat = MemBlobStat {
            inner: BlobStatInner {
                file_id,
                active_size: off,
                nr_active: seq,
                nr_total: seq,
            },
            mask: BitMap::new(seq),
        };
        Ok((stat, map))
    }
}

struct Item {
    id: u64,
    up2: u64,
    pos: Vec<Entry>,
}

struct BlobItem {
    id: u64,
    pos: Vec<Entry>,
}

impl Item {
    const fn new(id: u64, up2: u64, pos: Vec<Entry>) -> Self {
        Self { id, up2, pos }
    }
}

impl BlobItem {
    const fn new(id: u64, pos: Vec<Entry>) -> Self {
        Self { id, pos }
    }
}

struct Entry {
    /// logical address
    key: u64,
    /// offset in data file
    off: usize,
    /// length of dumpped BoxRef
    len: u32,
    /// old checksum
    crc: u32,
}

struct InactiveMap {
    ivls: Vec<Interval>,
    map: Vec<bool>,
}

impl InactiveMap {
    fn new(ivls: &[Interval]) -> Self {
        let mut tmp: Vec<Interval> = ivls.to_vec();
        tmp.sort_unstable_by(|x, y| { x.lo }.cmp(&{ y.lo }));

        Self {
            ivls: tmp,
            map: vec![false; ivls.len()],
        }
    }

    /// test if interval still has active addr, otherwise those interval will be collected and removed
    fn test(&mut self, addr: u64) {
        let pos = match self.ivls.binary_search_by(|x| { x.lo }.cmp(&addr)) {
            Ok(pos) => pos,
            Err(pos) => {
                if pos == 0 {
                    return;
                }
                pos - 1
            }
        };
        assert!(pos < self.ivls.len());
        assert!(addr >= self.ivls[pos].lo);
        self.map[pos] = true;
    }

    fn collect<F>(&self, mut f: F)
    where
        F: FnMut(bool, Interval),
    {
        for (idx, ivl) in self.ivls.iter().enumerate() {
            // true when not referenced
            f(!self.map[idx], *ivl);
        }
    }
}

fn copy<R>(
    r: &R,
    w: &mut GatherWriter,
    buf: &mut [u8],
    len: usize,
    mut off: u64,
) -> Result<u32, std::io::Error>
where
    R: GatherIO,
{
    let mut crc = Crc32cHasher::default();
    let mut n = 0;
    let buf_sz = buf.len();

    while n < len {
        let cnt = buf_sz.min(len - n);
        let s = &mut buf[0..cnt];
        r.read(s, off)?;
        crc.write(s);
        // the data will be reused next time, so we write data to file instead of queue it
        w.write(s);
        off += cnt as u64;
        n += cnt;
    }
    Ok(crc.finish() as u32)
}
