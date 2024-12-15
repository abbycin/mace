use std::{
    cmp::{max, min, Ordering},
    collections::{HashMap, VecDeque},
    ffi::c_int,
    sync::{
        atomic::{
            AtomicU32, AtomicU64, AtomicUsize,
            Ordering::{AcqRel, Acquire, Relaxed, Release},
        },
        Arc, RwLock,
    },
};

use async_io::AsyncIO;

use crate::{
    cc::wal::{EntryType, WalPadding},
    utils::{block::Block, byte_array::ByteArray},
    Options,
};

use super::{
    cc::Transaction,
    wal::{IWalCodec, WalAbort, WalBegin, WalCLR, WalCommit, WalEnd, WalUpdate},
    worker::SyncWorker,
};

struct SharedDesc {
    data: *mut WalDesc,
    version: AtomicU64,
}

impl SharedDesc {
    fn new() -> Self {
        Self {
            data: Box::into_raw(Box::new(WalDesc::default())),
            version: AtomicU64::new(0),
        }
    }

    fn get(&self, dst: &mut WalDesc) -> u64 {
        loop {
            let v = self.version.load(Acquire);
            if v & 1 != 0 {
                continue;
            }
            *dst = unsafe { *self.data };
            if v == self.version.load(Acquire) {
                return v;
            }
        }
    }

    // no councurrent write, no lock is required
    fn set(&self, src: &WalDesc) {
        self.version.fetch_add(1, AcqRel);
        unsafe { *self.data = *src };
        self.version.fetch_add(1, AcqRel);
    }

    fn update_wal(&self, tail: usize) {
        self.version.fetch_add(1, AcqRel);
        unsafe { (*self.data).wal_tail = tail }
        self.version.fetch_add(1, AcqRel);
    }
}

impl Drop for SharedDesc {
    fn drop(&mut self) {
        unsafe {
            drop(Box::from_raw(self.data));
        }
    }
}

unsafe impl Send for SharedDesc {}
unsafe impl Sync for SharedDesc {}

pub struct Logging {
    worker_id: u16,
    flushed_signal: AtomicUsize,
    prev_gsn: u64,
    /// circular buffer head and tail
    wal_head: AtomicUsize,
    wal_tail: AtomicUsize,
    desc: SharedDesc,
    wait_txn: RwLock<VecDeque<Transaction>>,
    buffer: Block,
    ctx: Arc<GroupCommitter>,
}

unsafe impl Sync for Logging {}
unsafe impl Send for Logging {}

pub struct LogBuilder<'a> {
    log: &'a mut Logging,
    buf: ByteArray,
    size: usize,
    desc: WalDesc,
}

impl<'a> LogBuilder<'a> {
    fn new(l: &'a mut Logging, b: ByteArray, txid: u64, gsn: u64) -> Self {
        let wal_tail = l.wal_tail.load(Relaxed) + b.len();
        Self {
            log: l,
            buf: b,
            size: b.len(),
            desc: WalDesc {
                version: 0,
                gsn,
                txid,
                wal_tail,
            },
        }
    }

    pub fn add<T>(&mut self, payload: T) -> &mut Self
    where
        T: IWalCodec,
    {
        let len = payload.encoded_len();
        let b = self.buf.sub_array(0, len);
        payload.encode_to(b);
        self.buf = self.buf.sub_array(len, self.buf.len() - len);
        self
    }

    pub fn build(&mut self) {
        self.log.wal_tail.fetch_add(self.size, Relaxed);
        self.log.desc.set(&self.desc);
    }
}

impl Logging {
    fn alloc_entry(&self, size: usize) -> ByteArray {
        let len = self.buffer.len();
        loop {
            let flushed = self.wal_head.load(Relaxed);
            let buffered = self.wal_tail.load(Relaxed);
            if flushed <= buffered {
                let rest = len - buffered;
                if rest < size {
                    if rest + flushed < size_of::<WalPadding>() {
                        continue; // no space for padding
                    }
                    self.wrap(len, flushed, buffered);
                    continue;
                }
                break;
            } else if flushed - buffered < size {
                continue; // wait entries to be comsumed by group commiter
            }
            break;
        }

        let beg = self.wal_tail.load(Acquire);
        let end = beg + size;
        self.buffer.view(beg, end)
    }

    fn wrap(&self, len: usize, flushed: usize, buffered: usize) {
        let rest = len - buffered;
        let sz = size_of::<WalPadding>();
        assert!(rest + flushed >= sz);
        let mut e = WalPadding {
            wal_type: EntryType::Padding,
            len: 0,
        };

        let new_tail = if rest >= sz {
            e.len = (rest - sz) as u32;
            e.encode_to(self.buffer.view(buffered, buffered + sz));
            0
        } else {
            e.len = 0;
            let beg = sz - rest;
            let src = e.to_slice();
            let (s1, s2) = src.split_at(rest);
            let b1 = self.buffer.view(buffered, len);
            let b2 = self.buffer.view(0, beg);

            let (d1, d2) = (b1.as_mut_slice(0, b1.len()), b2.as_mut_slice(0, b2.len()));

            d1.copy_from_slice(s1);
            d2.copy_from_slice(s2);

            beg
        };

        self.wal_tail.store(new_tail, Release);
        self.desc.update_wal(new_tail);
    }

    pub fn record_update<T>(&mut self, tree: u64, txid: u64, cmd: u32, w: T, k: &[u8], v: &[u8])
    where
        T: IWalCodec,
    {
        let payload_size = w.encoded_len() + k.len() + v.len();
        let gsn = self.ctx.gsn.fetch_add(1, Relaxed);
        let u = WalUpdate::new(
            tree as u16,
            self.worker_id,
            txid,
            cmd,
            payload_size,
            gsn,
            self.prev_gsn,
        );
        self.prev_gsn = gsn; // update to latest one
        let a = self.alloc_entry(u.encoded_len() + payload_size);
        let mut b = LogBuilder::new(self, a, txid, gsn);
        b.add(u).add(w).add(k).add(v).build();
    }

    fn add_entry<T: IWalCodec>(&mut self, w: T, txid: u64) {
        let size = w.encoded_len();
        let a = self.alloc_entry(size);
        w.encode_to(a.sub_array(0, size));
        let old = self.wal_tail.fetch_add(size, Release);
        self.desc.set(&WalDesc {
            version: 0,
            gsn: self.ctx.gsn.fetch_add(1, Relaxed),
            txid,
            wal_tail: old + size,
        });
    }

    pub fn record_begin(&mut self, txid: u64) {
        self.add_entry(
            WalBegin {
                wal_type: EntryType::Begin,
                txid,
            },
            txid,
        );
    }

    pub fn record_end(&mut self, txid: u64) {
        self.add_entry(
            WalEnd {
                wal_type: EntryType::End,
                txid,
            },
            txid,
        );
    }

    pub fn record_commit(&mut self, txid: u64) {
        self.add_entry(
            WalCommit {
                wal_type: EntryType::Commit,
                txid,
            },
            txid,
        );
    }

    pub fn record_abort(&mut self, txid: u64) {
        let w = WalAbort {
            wal_type: EntryType::Abort,
            txid,
        };
        self.add_entry(w, txid);
    }

    pub fn record_clr(&mut self, txid: u64, next_undo_gsn: u64) {
        let w = WalCLR {
            wal_type: EntryType::Clr,
            txid,
            next_undo_gsn,
        };
        self.add_entry(w, txid);
    }

    // FIXME: recover `prev_gsn`
    pub(crate) fn new(ctx: Arc<GroupCommitter>, buffer_size: usize, wid: u16) -> Self {
        let buffer = Block::aligned_alloc(buffer_size, AsyncIO::SECTOR_SIZE);
        buffer.zero();
        Self {
            flushed_signal: AtomicUsize::new(0),
            prev_gsn: 0,
            worker_id: wid,
            wal_head: AtomicUsize::new(0),
            wal_tail: AtomicUsize::new(0),
            desc: SharedDesc::new(),
            wait_txn: RwLock::new(VecDeque::new()),
            buffer,
            ctx,
        }
    }

    pub(crate) fn gsn(&mut self) -> u64 {
        self.ctx.gsn.load(Relaxed)
    }

    pub(crate) fn append_txn(&mut self, txn: Transaction) {
        self.wait_txn
            .write()
            .expect("can't lock write")
            .push_back(txn);
    }

    pub(crate) fn signal_flushed(&self, signal: usize) {
        self.flushed_signal.store(signal, Relaxed);
    }

    pub(crate) fn wait_commit(&self, commit_ts: u64) {
        while commit_ts > self.flushed_signal.load(Relaxed) as u64 {
            std::thread::yield_now();
        }
    }

    pub(crate) fn wait_flush(&self) {
        while self.wal_head.load(Relaxed) != self.wal_tail.load(Relaxed) {
            std::hint::spin_loop();
        }
    }
}

#[derive(Default, Clone, Copy, Debug)]
struct WalDesc {
    version: u64,
    gsn: u64,
    txid: u64,
    wal_tail: usize,
}

const GC_WORKING: u32 = 1926;
const GC_STOP: u32 = 114514;
const GC_STOPPED: u32 = 1919810;

pub struct GroupCommitter {
    /// global sequence number
    pub gsn: AtomicU64,
    wal_size: AtomicUsize,
    state: AtomicU32,
    fd: c_int,
    aio: AsyncIO,
}

impl Drop for GroupCommitter {
    fn drop(&mut self) {
        AsyncIO::fclose(self.fd);
    }
}

impl GroupCommitter {
    const PER_CORE_DEPTH: usize = 64;
    /// FIXME: recover `gsn`
    pub(crate) fn new(opt: Arc<Options>) -> Self {
        let fd = AsyncIO::fopen(opt.wal_file(), false, false);
        if fd < 0 {
            panic!("open wal file failed");
        }

        Self {
            gsn: AtomicU64::new(0),
            wal_size: AtomicUsize::new(AsyncIO::fsize(fd) as usize),
            state: AtomicU32::new(GC_WORKING),
            fd,
            aio: AsyncIO::new(coreid::cores_online() * Self::PER_CORE_DEPTH),
        }
    }

    pub(crate) fn run(&self, workers: Arc<RwLock<HashMap<usize, SyncWorker>>>) {
        let mut desc = HashMap::new();

        while self.state.load(Relaxed) == GC_WORKING {
            let (min_flush_txid, min_flush_gsn) = self.collect(&workers, &mut desc);

            if !self.aio.is_empty() {
                self.flush();
            }

            self.commit_txn(&workers, min_flush_txid, min_flush_gsn, &desc);
        }

        self.state.store(GC_STOPPED, Relaxed);
    }

    pub(crate) fn signal_stop(&self) {
        self.state.store(GC_STOP, Relaxed);
    }

    pub(crate) fn wait(&self) {
        while self.state.load(Relaxed) != GC_STOPPED {
            std::hint::spin_loop();
        }
    }

    pub(crate) fn wal_len(&self) -> usize {
        self.wal_size.load(Relaxed)
    }

    fn collect(
        &self,
        workers: &Arc<RwLock<HashMap<usize, SyncWorker>>>,
        desc: &mut HashMap<u16, WalDesc>,
    ) -> (u64, u64) {
        let mut min_flush_gsn = u64::MAX;
        let mut min_flush_txid = u64::MAX;

        for (_, w) in workers.read().expect("can't lock read").iter() {
            let mut d = if let Some(x) = desc.get(&w.id) {
                *x
            } else {
                WalDesc::default()
            };

            let saved_ver = d.version;
            let version = w.logging.desc.get(&mut d);
            d.version = version;

            if version == saved_ver {
                // current worker has no txn log since last collect
                continue;
            }
            desc.insert(w.id, d);

            min_flush_txid = min(min_flush_txid, d.txid);
            min_flush_gsn = min(min_flush_gsn, d.gsn);

            let wal_head = w.logging.wal_head.load(Relaxed);
            let wal_tail = d.wal_tail;
            let data = w.logging.buffer.data();

            match wal_tail.cmp(&wal_head) {
                Ordering::Greater => self.enqueue_buf(data, wal_head, wal_tail),
                Ordering::Less => {
                    self.enqueue_buf(data, wal_head, w.logging.buffer.len());
                    self.enqueue_buf(data, 0, wal_tail);
                }
                _ => {}
            }
        }

        if !self.aio.is_empty() {
            while self.aio.is_full() {
                self.wait_compelte();
            }
            self.aio.fsync(self.fd);
        }

        (min_flush_txid, min_flush_gsn)
    }

    fn flush(&self) {
        let mut rc = self.aio.sumbit();
        if rc < 0 {
            log::error!("aio sumbit failed, rc {}", rc);
        }

        self.wait_compelte();

        rc = AsyncIO::fdsync(self.fd);
        if rc < 0 {
            log::error!("can't sync file data, rc {}", rc);
        }
    }

    fn wait_compelte(&self) {
        while self.aio.wait(1000) < 0 {
            log::error!(
                "aio wait failed, rc {} pending {}",
                AsyncIO::last_error(),
                self.aio.pending()
            );
        }
    }

    fn commit_txn(
        &self,
        workers: &Arc<RwLock<HashMap<usize, SyncWorker>>>,
        min_flush_txid: u64,
        min_flush_gsn: u64,
        desc: &HashMap<u16, WalDesc>,
    ) {
        for (_, w) in workers.read().expect("can't lock read").iter() {
            let Some(d) = desc.get(&w.id) else {
                continue;
            };
            let mut max_flush_ts = 0;
            w.logging.wal_head.store(d.wal_tail, Relaxed);

            let mut lk = w.logging.wait_txn.write().expect("can't lock write");
            while let Some(txn) = lk.front() {
                // since the txns in same thread is ordered, the subsequent txns are not ready too
                if !txn.ready_to_commit(min_flush_txid, min_flush_gsn) {
                    break;
                }
                max_flush_ts = max(max_flush_ts, txn.commit_ts);
                lk.pop_front();
            }
            drop(lk);

            if max_flush_ts != 0 {
                w.logging.signal_flushed(max_flush_ts as usize);
            }
        }
    }

    fn enqueue_buf(&self, buf: *mut u8, from: usize, to: usize) {
        while self.aio.is_full() {
            self.wait_compelte();
        }
        let data = unsafe { buf.add(from) };
        let count = to - from;

        self.aio
            .prepare_write(self.fd, data, count, self.wal_size.load(Relaxed) as u64);
        self.wal_size.fetch_add(count, Relaxed);
    }
}

#[cfg(test)]
mod test {

    use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
    use std::sync::atomic::{fence, AtomicBool};
    use std::sync::Arc;

    use crate::slice_to_number;
    use crate::utils::raw_ptr_to_ref;
    use crate::{
        cc::{
            log::{IWalCodec, WalBegin, WalDesc, WalPadding, WalUpdate},
            wal::WalPut,
        },
        static_assert, Options, RandomPath,
    };

    use super::{GroupCommitter, Logging};

    #[test]
    fn logging() {
        let path = RandomPath::tmp();
        let opt = Arc::new(Options::new(&*path));
        let _ = std::fs::create_dir_all(&opt.db_root);
        let ctx = Arc::new(GroupCommitter::new(opt));
        let buffer_size = 512;
        let mut l = Logging::new(ctx, buffer_size, 0);

        l.record_begin(1);
        let mut desc = WalDesc::default();
        l.desc.get(&mut desc);
        assert_eq!(desc.wal_tail, size_of::<WalBegin>());

        assert_eq!(l.wal_tail.load(Relaxed), size_of::<WalBegin>());

        let (k, v) = ("mo".as_bytes(), "ha".as_bytes());
        let ins = WalPut::new(2, 2);
        l.record_update(1, 1, 1, ins, k, v);

        l.desc.get(&mut desc);
        let size = size_of::<WalBegin>()
            + size_of::<WalUpdate>()
            + ins.encoded_len()
            + k.encoded_len()
            + v.encoded_len();
        assert_eq!(desc.wal_tail, size);

        // simulate wrapping

        l.wal_tail.store(511, Relaxed);
        l.wal_head.store(3, Relaxed);
        let flag = AtomicBool::new(false);

        static_assert!(size_of::<WalPadding>() == 5);

        std::thread::scope(|s| {
            s.spawn(|| {
                l.alloc_entry(10);
                while !flag.load(Acquire) {
                    std::hint::spin_loop();
                }
                fence(Release);
                assert_eq!(l.wal_head.load(Relaxed), 15);
                let b = l.buffer.view(0, 4);
                let s = b.as_slice(0, 4);
                let n = slice_to_number!(s, u32);
                assert_eq!(n, 0);
                assert_eq!(l.wal_tail.load(Acquire), 4);
            });
            s.spawn(|| {
                loop {
                    let n = l.wal_head.fetch_add(1, Release);
                    if n == 14 {
                        break;
                    }
                }
                flag.store(true, Release);
            });
        });

        l.wal_tail.store(512, Relaxed);
        l.wal_head.store(3, Relaxed);
        flag.store(false, Release);

        std::thread::scope(|s| {
            s.spawn(|| {
                l.alloc_entry(10);
                while !flag.load(Acquire) {
                    std::hint::spin_loop();
                }
                fence(Release);
                assert_eq!(l.wal_head.load(Relaxed), 16);
                let b = l.buffer.view(0, 5);
                let s = b.as_slice(1, 4);
                let n = slice_to_number!(s, u32);
                assert_eq!(n, 0);
                assert_eq!(l.wal_tail.load(Acquire), 5);
            });

            s.spawn(|| {
                loop {
                    let n = l.wal_head.fetch_add(1, Release);
                    if n == 15 {
                        break;
                    }
                }
                flag.store(true, Release);
            });
        });

        l.wal_tail.store(503, Relaxed);
        l.wal_head.store(10, Relaxed);

        l.alloc_entry(10);
        let b = l.buffer.view(503, 503 + size_of::<WalPadding>());
        let w = raw_ptr_to_ref(b.data() as *mut WalPadding);
        assert_eq!({ w.len }, 512 - 503 - size_of::<WalPadding>() as u32);
    }
}
