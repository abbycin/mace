use crc32c::Crc32cHasher;
use io::{File, GatherIO};
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap, HashSet},
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
    map::data::{DataFooter, DataMetaReader},
    meta::{
        DelInterval, Delete, FileId, FileStat, IntervalPair, MetaKind, Numerics, StatInner, TxnKind,
    },
    types::traits::IAsSlice,
    utils::{
        Handle, MutRef,
        bitmap::BitMap,
        block::Block,
        countblock::Countblock,
        data::{AddrPair, GatherWriter, Interval},
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
                            gc.process_wal();
                            gc.process_manifest();
                            gc.process_data();
                            sem.post();
                        }
                        GC_QUIT => break,
                        _ => unreachable!("invalid instruction  {}", x),
                    },
                    Err(RecvTimeoutError::Timeout) => {
                        if !pause {
                            gc.process_wal();
                            gc.process_manifest();
                            gc.process_data();
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
    let mut last_ckpt_seq = Vec::with_capacity(store.opt.workers as usize);
    store.context.workers().iter().for_each(|w| {
        let seq = w.logging.last_ckpt().file_id;
        last_ckpt_seq.push(seq);
    });
    let gc = GarbageCollector {
        numerics: ctx.numerics.clone(),
        ctx,
        store,
        last_ckpt_seq,
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
    fn from(stat: &FileStat, now: u64) -> Self {
        Self {
            id: stat.file_id,
            size: stat.active_size,
            rate: Self::calc_decline_rate(stat, now),
            up2: stat.up2,
        }
    }

    fn calc_decline_rate(stat: &FileStat, now: u64) -> f64 {
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
    last_ckpt_seq: Vec<u64>,
}

impl GarbageCollector {
    const MAX_ELEMS: usize = 1024;

    /// with SSD support, for TB level data directly write manifest snapshot is acceptable
    fn process_manifest(&mut self) {
        self.ctx.manifest.try_clean();
    }

    fn process_data(&mut self) {
        let tgt_ratio = self.store.opt.gc_ratio as u64;
        let tgt_size = self.store.opt.gc_compacted_size;
        let eager = self.store.opt.gc_eager;

        let (total, active) = self.ctx.manifest.stat_ctx.total_active();
        if total == 0 {
            return;
        }
        let ratio = (total - active) * 100 / total;
        // log::trace!("ratio {ratio} tgt_ratio {tgt_ratio} total {total} active {active}");
        if ratio < tgt_ratio {
            return;
        }

        let tick = self.numerics.next_file_id.load(Relaxed);
        let mut heap = BinaryHeap::new();
        let mut unlinked = HashSet::new();
        self.ctx.manifest.stat_ctx.iter().for_each(|x| {
            let s = x.value();
            // a data file has no active frame will be unlinked directly
            if s.active_elems == 0 {
                unlinked.insert(s.file_id);
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

        let mut victims = vec![];
        let mut sum = 0;
        let mut tmp = Vec::from_iter(heap); // reclaim max rate first
        while let Some(x) = tmp.pop() {
            sum += x.size;
            victims.push(x);

            if sum >= tgt_size && victims.len() > 1 {
                self.compact(victims, unlinked);
                return;
            }
        }
        if eager && victims.len() > 1 {
            self.compact(victims, unlinked);
        }
    }

    fn process_wal(&mut self) {
        for w in self.store.context.workers().iter() {
            let id = w.id as usize;
            let lk = w.start_ckpt.read().unwrap();
            let ckpt_seq = lk.file_id;
            drop(lk);
            if self.last_ckpt_seq[id] == ckpt_seq {
                continue;
            }
            Self::process_one_wal(&self.store.opt, w.id, self.last_ckpt_seq[id], ckpt_seq);
            self.last_ckpt_seq[id] = ckpt_seq;
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

    fn compact(&mut self, victims: Vec<Score>, unlinked: HashSet<u64>) {
        let opt = &self.store.opt;
        let file_id = self.numerics.next_file_id.fetch_add(1, Relaxed);
        let mut builder = ReWriter::new(file_id, opt, victims.len());
        let mut obsoleted: Delete = Delete::default();
        let mut remap_intervals = Vec::with_capacity(victims.len());
        let mut del_intervals = DelInterval::default();

        unlinked.iter().for_each(|x| {
            let mut loader = DataMetaReader::new(opt.data_file(*x), true).expect("never happen");
            let hdr = loader.get_meta().unwrap();
            let ivls = hdr.intervals();
            for i in ivls {
                del_intervals.push(i.lo);
            }
            obsoleted.push(*x);
        });

        self.ctx.manifest.stat_ctx.start_collect_junks(); // stop in update_stat_interval
        victims.iter().for_each(|x| {
            let mut loader = DataMetaReader::new(opt.data_file(x.id), true).expect("never happen");
            let hdr = loader.get_meta().unwrap();
            let ivls = hdr.intervals();
            let mut im = InactiveMap::new(ivls);
            let stat = self.ctx.manifest.stat_ctx.get(&x.id).expect("must exist");
            let bitmap = stat.deleted_elems.clone();
            drop(stat);

            // collect active frames
            let v: Vec<Entry> = hdr
                .relocs()
                .iter()
                .filter(|m| {
                    if !bitmap.test(m.val.seq) {
                        im.test(m.key);
                        true
                    } else {
                        false
                    }
                })
                .map(|m| Entry {
                    key: m.key,
                    off: m.val.off,
                    len: m.val.len,
                })
                .collect();

            builder.add_frame(Item::new(x.id, x.up2, v));
            obsoleted.push(x.id);
            im.collect(|x| {
                del_intervals.push(x);
            });
            for i in ivls {
                let Interval { lo, hi } = *i;
                remap_intervals.push(IntervalPair::new(lo, hi, file_id));
                builder.add_interval(lo, hi);
            }
        });

        let mut txn = self.ctx.manifest.begin(TxnKind::GC);

        let fid = FileId { file_id };
        txn.record(MetaKind::FileId, &fid);
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
        let stat = self.ctx.manifest.update_stat_interval(
            fstat,
            relocs,
            &obsoleted,
            &del_intervals,
            &remap_intervals,
        );

        txn.record(MetaKind::Stat, &stat);

        // 1. record delete first
        if !del_intervals.is_empty() {
            txn.record(MetaKind::DelInterval, &del_intervals);
        }
        // 2. then record remapping, old intervals are point to new file_id
        for i in &remap_intervals {
            txn.record(MetaKind::Interval, i);
        }
        // in case crash happens before/during deleting files
        txn.record(MetaKind::Delete, &obsoleted);

        txn.commit();

        // 3. it's safe to clean obsolete files, becuase they are not referenced
        self.ctx.manifest.save_obsolete_files(&obsoleted);
        self.ctx.manifest.delete_files();
    }
}

struct ReWriter<'a> {
    file_id: u64,
    items: Vec<Item>,
    intervals: Vec<u8>,
    nr_interval: u32,
    sum_up2: u64,
    total: u64,
    opt: &'a Options,
}

impl<'a> ReWriter<'a> {
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

    fn build(&mut self) -> Result<(FileStat, HashMap<u64, (u32, u32)>), std::io::Error> {
        let up2 = self.sum_up2 / self.total;
        let mut block = Block::alloc(1 << 20);
        let mut seq = 0;
        let mut off = 0;
        let path = self.opt.data_file(self.file_id);
        let mut writer = GatherWriter::append(&path, 128);
        let mut crc = Crc32cHasher::default();
        let mut reloc: Vec<u8> = Vec::new();
        let mut reloc_map = HashMap::new();

        self.items.sort_unstable_by(|x, y| x.id.cmp(&y.id));

        for item in &self.items {
            let reader = File::options()
                .read(true)
                .open(self.opt.data_file(item.id))
                .unwrap();
            for e in &item.pos {
                let len = e.len as usize;
                if block.len() < len {
                    block.realloc(len);
                }
                let data = block.mut_slice(0, len);
                reader.read(data, e.off as u64)?;
                crc.write(data);
                // the data will be reused next time, so we write data to file instead of queue it
                writer.write(data);

                let m = AddrPair::new(e.key, off, e.len, seq);
                reloc.extend_from_slice(m.as_slice());
                reloc_map.insert(e.key, (e.len, seq));
                off += e.len as usize;
                seq += 1;
            }
        }

        let s = reloc.as_slice();
        crc.write(s);
        writer.queue(s);

        let is = self.intervals.as_slice();
        crc.write(is);
        writer.queue(is);

        let footer = DataFooter {
            up2,
            nr_reloc: seq,
            nr_active: seq,
            active_size: off,
            nr_intervals: self.nr_interval,
            crc: crc.finish() as u32,
        };

        writer.queue(footer.as_slice());

        writer.flush();
        writer.sync();
        log::info!("compacted to {path:?} {footer:?}");

        let stat = FileStat {
            inner: StatInner {
                file_id: self.file_id,
                up1: up2,
                up2,
                active_elems: seq,
                total_elems: seq,
                active_size: off,
                total_size: off,
            },
            deleted_elems: BitMap::new(seq),
        };
        Ok((stat, reloc_map))
    }
}

struct Item {
    id: u64,
    up2: u64,
    pos: Vec<Entry>,
}

impl Item {
    fn new(id: u64, up2: u64, pos: Vec<Entry>) -> Self {
        Self { id, up2, pos }
    }
}

struct Entry {
    /// logical address
    key: u64,
    /// offset in data file
    off: usize,
    /// length of record
    len: u32,
}

struct InactiveMap {
    ivls: Vec<(u64, u64)>,
    map: Vec<bool>,
}

impl InactiveMap {
    fn new(ivls: &[Interval]) -> Self {
        let mut tmp: Vec<(u64, u64)> = ivls.iter().map(|x| (x.lo, x.hi)).collect();
        tmp.sort_unstable_by(|x, y| x.0.cmp(&y.0));

        Self {
            ivls: tmp,
            map: vec![false; ivls.len()],
        }
    }

    fn test(&mut self, addr: u64) {
        let pos = match self.ivls.binary_search_by(|x| x.0.cmp(&addr)) {
            Ok(pos) => pos,
            Err(pos) => {
                if pos == 0 {
                    return;
                }
                pos - 1
            }
        };
        assert!(pos < self.ivls.len());
        assert!(addr >= self.ivls[pos].0);
        self.map[pos] = true;
    }

    fn collect<F>(&self, mut f: F)
    where
        F: FnMut(u64),
    {
        for (idx, (lo, _)) in self.ivls.iter().enumerate() {
            if !self.map[idx] {
                f(*lo);
            }
        }
    }
}
