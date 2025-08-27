use crc32c::Crc32cHasher;
use io::{File, GatherIO};
use std::{
    collections::HashSet,
    hash::Hasher,
    sync::{
        Arc,
        atomic::{
            Ordering::{Acquire, Relaxed, Release},
            fence,
        },
        mpsc::{Receiver, RecvTimeoutError, Sender, channel},
    },
    thread::JoinHandle,
    time::Duration,
};

use crate::{
    Options, Store,
    map::{
        Mapping,
        data::{DataFooter, DataMetaReader, FileStat},
    },
    utils::{
        Handle,
        block::Block,
        countblock::Countblock,
        data::{AddrMap, GatherWriter, ID_LEN, JUNK_LEN, Meta},
        unpack_id,
    },
};

const GC_QUIT: i32 = -1;
const GC_PAUSE: i32 = 3;
const GC_RESUME: i32 = 5;

fn gc_thread(mut gc: GarbageCollector, rx: Receiver<i32>, sem: Arc<Countblock>) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("garbage_collector".into())
        .spawn(move || {
            let timeout = Duration::from_millis(gc.store.opt.gc_timeout);
            let mut pause = false;

            loop {
                match rx.recv_timeout(timeout) {
                    Ok(x) => match x {
                        GC_PAUSE => {
                            pause = true;
                            sem.post();
                        }
                        GC_RESUME => {
                            pause = false;
                            sem.post();
                        }
                        GC_QUIT => break,
                        _ => unreachable!("invalid instruction  {}", x),
                    },
                    Err(RecvTimeoutError::Timeout) => {
                        if !pause {
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
        self.sem.wait();
    }

    pub(crate) fn resume(&self) {
        self.tx.send(GC_RESUME).unwrap();
        self.sem.wait();
    }
}

pub(crate) fn start_gc(store: Arc<Store>, meta: Arc<Meta>, map: Handle<Mapping>) -> GCHandle {
    let (tx, rx) = channel();
    let sem = Arc::new(Countblock::new(0));
    let mut last_ckpt_seq = Vec::with_capacity(store.opt.workers);
    store.context.workers().iter().for_each(|w| {
        let (seq, _) = unpack_id(w.logging.last_ckpt());
        last_ckpt_seq.push(seq);
    });
    let gc = GarbageCollector {
        meta,
        map,
        store,
        last_ckpt_seq,
    };
    gc_thread(gc, rx, sem.clone());
    GCHandle {
        tx: Arc::new(tx),
        sem,
    }
}

struct Score {
    id: u32,
    logical_id: u32,
    size: usize,
    rate: f64,
    up2: u32,
}

impl Score {
    fn from(logical_id: u32, stat: &FileStat, now: u32) -> Self {
        Self {
            logical_id,
            id: stat.file_id,
            size: stat.active_size,
            rate: Self::calc_decline_rate(stat, now),
            up2: stat.up2,
        }
    }

    fn calc_decline_rate(stat: &FileStat, now: u32) -> f64 {
        let free = stat.total_size.saturating_sub(stat.active_size);
        if free == 0 || stat.up2 == now {
            return f64::MIN; // skip new born
        }

        -(stat.active_size as f64 / free as f64).powi(2)
            / (stat.total as f64 * (now - stat.up2) as f64)
    }
}

struct GarbageCollector {
    meta: Arc<Meta>,
    map: Handle<Mapping>,
    store: Arc<Store>,
    last_ckpt_seq: Vec<u32>,
}

impl GarbageCollector {
    fn process_data(&mut self) {
        let tgt_ratio = self.store.opt.gc_ratio as u64;
        let tgt_size = self.store.opt.gc_compacted_size;
        let eager = self.store.opt.gc_eager;

        'again: loop {
            let mut total = 0u64;
            let mut active = 0u64;
            self.map.stats.iter().for_each(|x| {
                total += x.total_size as u64;
                active += x.active_size as u64;
            });

            if total == 0 {
                return;
            }
            let ratio = (total - active) * 100 / total;
            log::trace!("ratio {ratio} tgt_ratio {tgt_ratio} total {total} active {active}");
            if ratio < tgt_ratio {
                return;
            }

            let now = self.meta.next_data.load(Relaxed);
            let mut q = Vec::new();
            let mut unmapped = HashSet::new();
            let mut unlinked = HashSet::new();
            self.map.stats.iter().for_each(|x| {
                let s = x.value();
                // NOTE: file may have no active frames, but the junks may still active, they will
                // be filtered in `compact`
                q.push(Score::from(*x.key(), s, now));
                if s.nr_active == 0 {
                    unmapped.insert(*x.key());
                    unlinked.insert(s.file_id);
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
                    self.compact(victims, unmapped, unlinked);
                    continue 'again;
                }
            }
            if eager {
                self.compact(victims, unmapped, unlinked);
            }
            break;
        }
    }

    fn process_wal(&mut self) {
        for w in self.store.context.workers().iter() {
            let id = w.id as usize;
            let (ckpt_seq, _) = unpack_id(w.start_ckpt.load(Relaxed));
            if self.last_ckpt_seq[id] == ckpt_seq {
                continue;
            }
            Self::process_one_wal(&self.store.opt, w.id, self.last_ckpt_seq[id], ckpt_seq);
            self.last_ckpt_seq[id] = ckpt_seq;
        }
    }

    fn process_one_wal(opt: &Options, id: u16, beg: u32, end: u32) {
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

    fn compact(&mut self, victims: Vec<Score>, unmapped: HashSet<u32>, mut unlinked: HashSet<u32>) {
        let opt = &self.store.opt;
        let mut builder = ReWriter::new(opt, victims.len());
        let mut lids = HashSet::new();
        let mut dealloc = HashSet::new();

        let tmp: Vec<u32> = victims
            .iter()
            .map(|x| {
                let mut loader = DataMetaReader::new(opt.data_file(x.id), false).unwrap();
                let hdr = loader.get_meta().unwrap();
                let mut active_addr = HashSet::new();

                // collect active frames
                let v: Vec<Entry> = hdr
                    .relocs()
                    .iter()
                    .filter(|m| {
                        let stat = self.map.stats.get(&x.logical_id).unwrap();
                        // collect total logical id
                        dealloc.insert(unpack_id(m.key).0);
                        // filter-out deallocated frames
                        !stat.dealloc.test(m.val.seq)
                    })
                    .map(|x| {
                        let (lid, _) = unpack_id(x.key);
                        // the real active logical id is which still has reloc
                        lids.insert(lid);
                        // the real active pid -> addr entry
                        active_addr.insert(x.key);
                        Entry {
                            key: x.key,
                            off: x.val.off,
                            len: x.val.len, // NOTE: it's frame len not payload len of the frame
                        }
                    })
                    .collect();

                // collect active junks
                let mut junks = loader.get_junk().unwrap();
                junks.sort_unstable();
                junks.dedup();
                // remove entries that are about to be unmapped or do not exist
                junks.retain(|addr| {
                    let (logical_id, _) = unpack_id(*addr);
                    if logical_id == x.logical_id {
                        return false;
                    }
                    !unmapped.contains(&logical_id) && self.map.stats.contains_key(&logical_id)
                });

                builder.add_frame(Item::new(x.id, x.up2, v));
                builder.add_junk(junks);
                x.id
            })
            .collect();

        // filter-out active logical id, then the reset is about to removed from mapping
        lids.iter().for_each(|x| {
            dealloc.remove(x);
        });
        let id = self.meta.next_data.fetch_add(1, Relaxed);
        builder
            .build(lids, id)
            .inspect_err(|e| {
                log::error!("error {e}");
            })
            .unwrap();

        fence(Acquire);

        // create a new indirection
        // NOTE: it's unnecessary to set file_id map to id individually, since `add` will replace
        // the whole stat rather than a single file_id
        self.map.add(id, false).expect("never happen");

        unlinked.extend(tmp.iter());
        unlinked.iter().for_each(|x| {
            self.map.evict(*x);
            let name = opt.data_file(*x);
            log::info!("unlink {name:?}");
            let _ = std::fs::remove_file(name);
        });

        fence(Release);

        // remove those have no reloc
        dealloc.iter().for_each(|x| {
            self.map.del(*x);
        });

        // remove those have no active frames
        unmapped.iter().for_each(|x| {
            self.map.del(*x);
        });

        // remove those physical ids have been unlinked
        self.map.retain(&unlinked);
    }
}

struct ReWriter<'a> {
    items: Vec<Item>,
    junks: Vec<u64>,
    sum_up2: u64,
    total: u64,
    opt: &'a Options,
}

impl<'a> ReWriter<'a> {
    fn new(opt: &'a Options, cap: usize) -> Self {
        Self {
            items: Vec::with_capacity(cap),
            junks: Vec::new(),
            sum_up2: 0,
            total: cap as u64,
            opt,
        }
    }

    fn add_frame(&mut self, item: Item) {
        self.sum_up2 += item.up2 as u64;
        self.items.push(item);
    }

    fn add_junk(&mut self, junk: Vec<u64>) {
        self.junks.extend_from_slice(&junk);
    }

    fn build(&mut self, lids: HashSet<u32>, id: u32) -> Result<(), std::io::Error> {
        let up2 = (self.sum_up2 / self.total) as u32;
        let mut block = Block::alloc(1 << 20);
        let mut seq = 0;
        let mut off = 0;
        let path = self.opt.data_file(id);
        let mut writer = GatherWriter::new(&path, 128);
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

                let m = AddrMap::new(e.key, off, e.len, seq);
                reloc.extend_from_slice(m.as_slice());
                off += e.len as usize;
                seq += 1;
            }
        }

        let junks = unsafe {
            std::slice::from_raw_parts(
                self.junks.as_ptr().cast::<u8>(),
                self.junks.len() * JUNK_LEN,
            )
        };
        crc.write(junks);
        writer.queue(junks);

        let s = reloc.as_slice();
        crc.write(s);
        writer.queue(s);

        let lids: Vec<u32> = lids.iter().cloned().collect();

        let slid =
            unsafe { std::slice::from_raw_parts(lids.as_ptr().cast::<u8>(), lids.len() * ID_LEN) };
        crc.write(slid);
        writer.queue(slid);

        let footer = DataFooter {
            up2,
            nr_lid: lids.len() as u32,
            nr_reloc: seq,
            nr_junk: self.junks.len() as u32,
            nr_active: seq,
            padding: 0,
            active_size: off,
            padding2: 0,
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
    up2: u32,
    pos: Vec<Entry>,
}

impl Item {
    fn new(id: u32, up2: u32, pos: Vec<Entry>) -> Self {
        Self { id, up2, pos }
    }
}

struct Entry {
    key: u64,
    off: usize,
    len: u32,
}
