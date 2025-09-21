use crc32c::Crc32cHasher;
use io::{File, GatherIO};
use std::{
    collections::HashSet,
    hash::Hasher,
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
    meta::{Delete, FileStat, Lid, Numerics, StatInner},
    utils::{
        Handle, MutRef,
        bitmap::BitMap,
        block::Block,
        countblock::Countblock,
        data::{AddrPair, GatherWriter},
        unpack_id,
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
                            gc.process_manifest();
                            gc.process_data();
                            gc.process_wal();
                            sem.post();
                        }
                        GC_QUIT => break,
                        _ => unreachable!("invalid instruction  {}", x),
                    },
                    Err(RecvTimeoutError::Timeout) => {
                        if !pause {
                            gc.process_manifest();
                            gc.process_data();
                            gc.process_wal();
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
    let mut last_ckpt_seq = Vec::with_capacity(store.opt.workers);
    store.context.workers().iter().for_each(|w| {
        let seq = w.logging.last_ckpt().file_id;
        last_ckpt_seq.push(seq);
    });
    let gc = GarbageCollector {
        numerics: ctx.numerics.clone(),
        ctx,
        store,
        last_ckpt_seq,
        last_raio: 0,
        last_total: 0,
    };
    gc_thread(gc, rx, sem.clone());
    GCHandle {
        tx: Arc::new(tx),
        sem,
    }
}

#[derive(Clone, Copy, Debug)]
struct Score {
    id: u32,
    logical_id: u32,
    size: usize,
    rate: f64,
    up2: u64,
}

impl Score {
    fn from(logical_id: u32, stat: &FileStat, now: u64) -> Self {
        Self {
            logical_id,
            id: stat.file_id,
            size: stat.active_size,
            rate: Self::calc_decline_rate(stat, now),
            up2: stat.up2,
        }
    }

    fn calc_decline_rate(stat: &FileStat, now: u64) -> f64 {
        let free = stat.total_size.saturating_sub(stat.active_size);
        if free == 0 || stat.up2 == now {
            return f64::MIN; // skip newborn
        }

        -(stat.active_size as f64 / free as f64).powi(2)
            / (stat.total_elems as f64 * (now - stat.up2) as f64)
    }
}

struct GarbageCollector {
    numerics: Arc<Numerics>,
    ctx: Handle<Context>,
    store: MutRef<Store>,
    last_ckpt_seq: Vec<u64>,
    last_raio: u64,
    last_total: u64,
}

impl GarbageCollector {
    /// with SSD support, for TB level data directly write manifest snapshot is acceptable
    fn process_manifest(&mut self) {
        self.ctx.manifest.try_clean();
    }

    fn process_data(&mut self) {
        let tgt_ratio = self.store.opt.gc_ratio as u64;
        let tgt_size = self.store.opt.gc_compacted_size;
        let eager = self.store.opt.gc_eager;

        'again: loop {
            let mut total = 0u64;
            let mut active = 0u64;
            self.ctx.manifest.file_stat.iter().for_each(|x| {
                total += x.total_size as u64;
                active += x.active_size as u64;
            });

            if total == 0 {
                return;
            }
            let ratio = (total - active) * 100 / total;
            // log::trace!("ratio {ratio} tgt_ratio {tgt_ratio} total {total} active {active}");
            if ratio < tgt_ratio {
                return;
            }

            // we have cleaned some segments, but the total size and ratio remain unchanged, no need
            // to clean
            if self.last_raio == ratio && self.last_total == total {
                return;
            }

            self.last_raio = ratio;
            self.last_total = total;

            let tick = self.numerics.tick.load(Relaxed);
            let mut q = Vec::new();
            let mut unlinked = HashSet::new();
            self.ctx.manifest.file_stat.iter().for_each(|x| {
                let s = x.value();
                // a data file has no active frame will be unlinked directly
                if s.active_elems == 0 {
                    unlinked.insert(s.file_id);
                } else {
                    q.push(Score::from(*x.key(), s, tick));
                }
            });

            q.sort_unstable_by(|x, y| x.id.cmp(&y.id));
            // dedup Score share same physical id
            q.dedup_by(|x, y| x.id == y.id);

            q.sort_unstable_by(|x, y| {
                // ascending order so that we can `pop` the min decline rate one (since we applied
                // `-` to the rate)
                x.rate
                    .partial_cmp(&y.rate)
                    .unwrap_or_else(|| x.id.cmp(&y.id))
            });

            let mut victims = vec![];
            let mut sum = 0;
            while let Some(x) = q.pop() {
                // NOTE: junks are not take into account
                sum += x.size;
                victims.push(x);

                if sum >= tgt_size {
                    self.compact(victims, unlinked);
                    continue 'again;
                }
            }
            if eager {
                self.compact(victims, unlinked);
            }
            break;
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

    fn compact(&mut self, victims: Vec<Score>, unlinked: HashSet<u32>) {
        let opt = &self.store.opt;
        let mut builder = ReWriter::new(opt, victims.len());
        // including victims' logical id which are going to be mapped to new physical id
        let mut active_lids = HashSet::new();
        let mut inactive_lids = HashSet::new();
        // including logical_id mapped to victims phyiscal_id and victims themselves' mapping
        let mut unmapped_lids = Vec::with_capacity(victims.len());
        let mut del = Delete::default();
        let mut obsoleted = Vec::with_capacity(victims.len());

        unlinked.iter().for_each(|x| {
            del.push(*x);
        });

        victims.iter().for_each(|x| {
            let mut loader = DataMetaReader::new(opt.data_file(x.id), false).unwrap();
            let hdr = loader.get_meta().unwrap();
            let mut lid = Lid::new(x.id);
            let mut set = HashSet::new();

            // collect active frames
            let v: Vec<Entry> = hdr
                .relocs()
                .iter()
                .filter(|m| {
                    let stat = self.ctx.manifest.file_stat.get(&x.logical_id).unwrap();
                    let (lid, _) = unpack_id(m.key);
                    set.insert(lid);
                    // the frames may come from other data file when current file was created
                    // by compaction, we must move them to new location too
                    if !stat.deleted_elems.test(m.val.seq) {
                        active_lids.insert(lid);
                        true
                    } else {
                        // excluding victims' id which is going to be mapped to new data file
                        if lid != x.id {
                            inactive_lids.insert(lid);
                        }
                        false
                    }
                })
                .map(|x| Entry {
                    key: x.key,
                    off: x.val.off,
                    len: x.val.len,
                })
                .collect();

            // they are going to be unmapped or remapped
            for id in set.iter() {
                lid.del(*id);
            }
            unmapped_lids.push(lid);
            builder.add_frame(Item::new(x.id, x.up2, v));
            del.push(x.id);
            obsoleted.push(x.id);
        });

        let id = self.numerics.next_file_id.fetch_add(1, Relaxed);
        let fstat = builder.stat(id);
        let stat = fstat.copy();
        let mut remap_lid = Lid::new(id);
        // remap to new id
        for id in active_lids.iter() {
            remap_lid.add(*id);
        }

        let mut txn = self.ctx.manifest.begin(id);

        // new entry same as flush, stat must flush before lid
        txn.record(&stat);
        // 1. record delete first
        for lid in &unmapped_lids {
            txn.record(lid);
        }
        // 2. then record remapping
        txn.record(&remap_lid);
        // in case crah happens before/during deleting files
        txn.record(&del);

        // flush before commit, simplify recover process
        txn.flush();
        builder
            .build(id)
            .inspect_err(|e| {
                log::error!("error {e}");
            })
            .unwrap();

        txn.commit();

        // 1.
        // excluding victims, because they will be remapping to fstat later, we can't remove the old
        // mapping and then remap, which is not atomic and may cause mapping not found
        (0..victims.len()).for_each(|_| {
            del.pop();
        });
        // including those logical_id in data file have no active frames, their source files were
        // removed in previous compaction
        inactive_lids.iter().for_each(|&id| {
            del.push(id);
        });
        self.ctx.manifest.retain(&del);
        self.ctx.manifest.save_obsolete_files(&obsoleted);

        // 2.
        // update in-memory mapping
        let lids: Vec<u32> = active_lids.iter().copied().collect();
        self.ctx.manifest.update_stat(&lids, fstat);

        // 3. it's safe to clean obsolete files, becuase no reader is reference to them
        self.ctx.manifest.delete_files();
    }
}

struct ReWriter<'a> {
    items: Vec<Item>,
    active_elems: u32,
    active_size: usize,
    sum_up2: u64,
    total: u64,
    opt: &'a Options,
}

impl<'a> ReWriter<'a> {
    fn new(opt: &'a Options, cap: usize) -> Self {
        Self {
            items: Vec::with_capacity(cap),
            active_elems: 0,
            active_size: 0,
            sum_up2: 0,
            total: cap as u64,
            opt,
        }
    }

    fn add_frame(&mut self, item: Item) {
        self.sum_up2 += item.up2;
        self.active_elems += item.pos.len() as u32;
        self.active_size += item.pos.iter().map(|x| x.len).sum::<u32>() as usize;
        self.items.push(item);
    }

    fn stat(&self, id: u32) -> FileStat {
        let up2 = self.sum_up2 / self.total;
        FileStat {
            inner: StatInner {
                file_id: id,
                _padding: 0,
                up1: up2,
                up2,
                active_elems: self.active_elems,
                total_elems: self.active_elems,
                active_size: self.active_size,
                total_size: self.active_size,
            },
            deleted_elems: BitMap::new(self.active_elems),
        }
    }

    fn build(&mut self, id: u32) -> Result<(), std::io::Error> {
        let up2 = self.sum_up2 / self.total;
        let mut block = Block::alloc(1 << 20);
        let mut seq = 0;
        let mut off = 0;
        let path = self.opt.data_file(id);
        let mut writer = GatherWriter::append(&path, 128);
        let mut crc = Crc32cHasher::default();
        let mut reloc: Vec<u8> = Vec::new();

        self.items.sort_unstable_by(|x, y| x.id.cmp(&y.id));

        for item in &self.items {
            let reader = File::options()
                .read(true)
                .open(&self.opt.data_file(item.id))
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
                off += e.len as usize;
                seq += 1;
            }
        }

        let s = reloc.as_slice();
        crc.write(s);
        writer.queue(s);

        let footer = DataFooter {
            up2,
            nr_reloc: seq,
            nr_active: seq,
            active_size: off,
            padding: 0,
            crc: crc.finish() as u32,
        };

        writer.queue(footer.as_slice());

        writer.flush();
        writer.sync();
        log::info!("compacted to {path:?} {footer:?}");
        Ok(())
    }
}

struct Item {
    id: u32,
    up2: u64,
    pos: Vec<Entry>,
}

impl Item {
    fn new(id: u32, up2: u64, pos: Vec<Entry>) -> Self {
        Self { id, up2, pos }
    }
}

struct Entry {
    key: u64,
    off: usize,
    len: u32,
}
