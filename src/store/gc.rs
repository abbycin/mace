use std::{
    collections::HashSet,
    hash::Hasher,
    io::Write,
    sync::{
        atomic::{
            fence,
            Ordering::{Acquire, Relaxed, Release},
        },
        mpsc::{channel, Receiver, RecvTimeoutError, Sender},
        Arc,
    },
    thread::JoinHandle,
    time::Duration,
};

use crc32c::Crc32cHasher;
use io::{File, GatherIO};

use crate::{
    cc::wal::{ptr_to, WalCheckpoint},
    map::{
        data::{DataFooter, DataMetaReader, FileStat},
        Mapping,
    },
    utils::{
        block::Block,
        countblock::Countblock,
        data::{AddrMap, MapEntry, Meta, PageTable, ID_LEN, JUNK_LEN},
        pack_id, unpack_id, NEXT_ID, NULL_ORACLE,
    },
    Options, Store,
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
                        log::error!("gc receive error {}", e);
                        break;
                    }
                }
            }

            sem.post();
        })
        .unwrap()
}

#[derive(Clone)]
pub(crate) struct Handle {
    tx: Arc<Sender<i32>>,
    sem: Arc<Countblock>,
}

impl Handle {
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

pub(crate) fn start_gc(store: Arc<Store>, meta: Arc<Meta>, map: Arc<Mapping>) -> Handle {
    let (tx, rx) = channel();
    let sem = Arc::new(Countblock::new(0));
    let last_ckpt = meta.ckpt.load(Relaxed);
    let gc = GarbageCollector {
        meta,
        map,
        store,
        last_ckpt,
    };
    gc_thread(gc, rx, sem.clone());
    Handle {
        tx: Arc::new(tx),
        sem,
    }
}

struct Score {
    id: u32,
    logical_id: u32,
    up2: u32,
    size: u32,
    rate: f64,
}

impl Score {
    fn from(logical_id: u32, stat: &FileStat, now: u32) -> Self {
        Self {
            logical_id,
            id: stat.file_id,
            up2: stat.up2,
            size: stat.active_size,
            rate: Self::calc_decline_rate(stat, now),
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

/// almost all garbages are produced by consolidation, on the other hand, consolidation makes arena
/// more denser and will move the old delta from old arena to the recent one
///
/// BTW, we can significantly reduce the flush size by mark frames to `TombStone` on successfully
/// conslidated
/// but this will face the race condition: one thread successfully consolidated while other threads
/// still perform lookup or consolidate on the same node, they will trying to load frame from file
/// with the old offset already be marked as `TombStone` that not flushed to file
/// this problem can be solved, but it's very complicated or inefficient, we will solve it later
struct GarbageCollector {
    meta: Arc<Meta>,
    map: Arc<Mapping>,
    store: Arc<Store>,
    last_ckpt: u64,
}

impl GarbageCollector {
    fn process_data(&mut self) {
        let tgt_ratio = self.store.opt.gc_ratio as u64;
        let tgt_size = self.store.opt.buffer_size;

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
            break;
        }
    }

    fn process_wal(&mut self) {
        let ckpt = self.meta.ckpt.load(Relaxed);
        if self.last_ckpt == ckpt {
            return;
        }
        self.last_ckpt = ckpt;

        let mut oldest_txid = NULL_ORACLE;
        let mut oldest_ckpt = NULL_ORACLE;
        for w in self.store.context.workers().iter() {
            let ckpt = w.ckpt.load(Relaxed);
            let txid = w.tx_id.load(Relaxed);

            if txid < oldest_txid {
                oldest_txid = txid;
                oldest_ckpt = ckpt;
            }
        }

        let opt = self.store.opt.clone();
        let mut buf = [0u8; size_of::<WalCheckpoint>()];
        let (cur_id, _) = unpack_id(oldest_ckpt);
        let mut end = None;
        let mut beg = cur_id;

        let null = pack_id(NEXT_ID, 0);
        while oldest_ckpt != null {
            let (id, pos) = unpack_id(oldest_ckpt);
            let path = opt.wal_file(id);
            if !path.exists() {
                break;
            }
            let f = File::options().read(true).open(&path).unwrap();

            f.read(&mut buf, pos as u64).unwrap();
            let c = ptr_to::<WalCheckpoint>(buf.as_ptr());
            oldest_ckpt = c.prev_addr;

            let (oldest_id, _) = unpack_id(oldest_ckpt);

            if oldest_id != cur_id {
                if end.is_none() {
                    end = Some(oldest_id);
                }
                beg = oldest_id;
            }
        }

        if end.is_some() {
            let end = end.unwrap();
            for i in beg..=end {
                let from = opt.wal_file(i);
                if !from.exists() {
                    continue;
                }
                let to = opt.wal_backup(i);
                std::fs::rename(&from, &to)
                    .inspect_err(|e| {
                        log::error!("can't rename {:?} to {:?}, error {:?}", from, to, e);
                    })
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
                            len: x.val.len,
                        }
                    })
                    .collect();

                // collect active pid-addr maps
                let e: Vec<MapEntry> = hdr
                    .entries()
                    .iter()
                    .filter(|e| active_addr.contains(&e.page_addr()))
                    .copied()
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
                builder.add_entry(e);
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
                log::error!("error {}", e);
            })
            .unwrap();

        fence(Acquire);

        // create a new indirection
        // NOTE: it's unnecessary to set file_id map to id individually, since `add` will replace
        // the whole stat rather than a single file_id
        self.map.add(id, false).expect("never happen");

        unlinked.extend(tmp.iter());
        unlinked.iter().for_each(|x| {
            self.map.cache.del(*x);
            let name = opt.data_file(*x);
            log::info!("unlink {:?}", name);
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

        // remove those physical id have already been unlinked
        self.map.retain(&unlinked);
    }
}

struct ReWriter<'a> {
    table: PageTable,
    items: Vec<Item>,
    junks: Vec<u64>,
    sum_up2: u64,
    total: u64,
    opt: &'a Options,
}

impl<'a> ReWriter<'a> {
    fn new(opt: &'a Options, cap: usize) -> Self {
        Self {
            table: PageTable::default(),
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

    fn add_entry(&mut self, e: Vec<MapEntry>) {
        for i in e {
            self.table.add(i.page_id(), i.page_addr());
        }
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
        let mut writer = std::fs::File::options()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
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
                let data = block.get_mut_slice(0, len);
                reader.read(data, e.off as u64)?;
                crc.write(data);
                writer.write_all(data)?;

                let m = AddrMap::new(e.key, off, e.len, seq);
                reloc.extend_from_slice(m.as_slice());
                off += e.len;
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
        writer.write_all(junks)?;

        crc.write(reloc.as_slice());
        writer.write_all(reloc.as_slice())?;

        self.table.hash(&mut crc);
        self.table.serialize(&mut writer);

        let lids: Vec<u32> = lids.iter().cloned().collect();

        let slid =
            unsafe { std::slice::from_raw_parts(lids.as_ptr().cast::<u8>(), lids.len() * ID_LEN) };
        crc.write(slid);
        writer.write_all(slid)?;

        let footer = DataFooter {
            up2,
            nr_lid: lids.len() as u32,
            nr_entry: self.table.len() as u32,
            nr_reloc: seq,
            nr_junk: self.junks.len() as u32,
            nr_active: seq,
            active_size: off,
            crc: crc.finish() as u32,
        };

        footer.serialize(&mut writer);

        writer.flush()?;
        writer.sync_all()?;
        log::info!("compacted to {:?} {:?}", path, footer);
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
    off: u32,
    len: u32,
}
