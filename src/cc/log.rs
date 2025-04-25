use std::{
    cell::Cell,
    cmp::{max, min, Ordering},
    collections::{HashMap, VecDeque},
    path::PathBuf,
    sync::{
        atomic::{
            AtomicU32, AtomicU64, AtomicUsize,
            Ordering::{Acquire, Relaxed, Release},
        },
        Arc, Mutex,
    },
};

use io::{File, GatherIO, IoVec};

use crate::{
    cc::wal::{EntryType, WalPadding, WalSpan},
    map::buffer::Buffers,
    utils::{
        block::Block, bytes::ByteArray, countblock::Countblock, data::Meta, pack_id, unpack_id,
        NEXT_ID,
    },
    Options,
};

use super::{
    cc::Transaction,
    data::Ver,
    wal::{
        CkptMem, IWalCodec, IWalPayload, IWalRec, WalAbort, WalBegin, WalCheckpoint, WalCommit,
        WalUpdate,
    },
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
        self.version.fetch_add(1, Acquire);
        unsafe { *self.data = *src };
        self.version.fetch_add(1, Release);
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

fn set_lsn(data: *mut u8, mut off: usize, end: usize, lsn: &AtomicU64, beg: u64) -> Option<u64> {
    fn to<T>(x: *mut u8) -> &'static mut T {
        unsafe { &mut *x.cast::<T>() }
    }

    let mut last_record_pos = lsn.load(Relaxed);
    let mut sz = 0;
    let mut last = None;
    lsn.store(beg, Relaxed);
    while off < end {
        let (p, h) = unsafe {
            let p = data.add(off);
            let h = p.read().into();
            (p, h)
        };

        // the data is always valid, not need to check
        sz = match h {
            EntryType::Abort | EntryType::Begin | EntryType::Commit => {
                let x = to::<WalBegin>(p);
                last = Some((h, x.txid));
                WalAbort::size()
            }
            EntryType::Update => {
                let u = to::<WalUpdate>(p);
                last = Some((h, u.txid));
                u.set_lsn(last_record_pos);
                u.size as usize + u.encoded_len()
            }
            EntryType::Padding => WalPadding::size(),
            EntryType::Span => {
                let x = to::<WalSpan>(p);
                x.span as usize + x.encoded_len()
            }
            EntryType::CheckPoint => {
                let c = to::<WalCheckpoint>(p);
                c.payload_len() + WalCheckpoint::size()
            }
            _ => unreachable!("invalid type {}", h as u8),
        };

        off += sz;
        // excluding padding, it's not involved in rollback
        if !matches!(h, EntryType::Padding | EntryType::Span) {
            last_record_pos = lsn.fetch_add(sz as u64, Relaxed);
        }
    }
    // reset to record start position
    lsn.fetch_sub(sz as u64, Relaxed);

    if let Some((h, txid)) = last {
        if matches!(h, EntryType::Update | EntryType::Begin) {
            return Some(txid);
        }
    }
    None
}

struct Ring {
    data: Block,
    head: AtomicUsize,
    tail: Cell<usize>,
}

impl Ring {
    fn new(cap: usize) -> Self {
        let data = Block::aligned_alloc(cap, 1);
        data.zero();
        assert!(data.len().is_power_of_two());
        Self {
            data,
            head: AtomicUsize::new(0),
            tail: Cell::new(0),
        }
    }

    #[inline]
    fn avail(&self) -> usize {
        self.data.len() - (self.tail.get() - self.head.load(Relaxed))
    }

    // NOTE: the request buffer never wraps around
    fn prod(&self, size: usize) -> ByteArray {
        debug_assert!(self.avail() >= size);
        let mut b = self.tail.get();
        self.tail.set(b + size);

        b &= self.mask();
        self.data.view(b, b + size)
    }

    #[inline]
    fn cons(&self, pos: usize) {
        self.head.store(pos, Release);
    }

    #[inline]
    fn head(&self) -> usize {
        self.head.load(Relaxed) & self.mask()
    }

    #[inline]
    fn tail(&self) -> usize {
        self.tail.get() & self.mask()
    }

    #[inline]
    fn raw_head(&self) -> usize {
        self.head.load(Relaxed)
    }

    #[inline]
    fn raw_tail(&self) -> usize {
        self.tail.get()
    }

    #[inline]
    fn data(&self) -> *mut u8 {
        self.data.data()
    }

    #[inline]
    fn mask(&self) -> usize {
        self.data.len() - 1
    }

    #[inline]
    fn len(&self) -> usize {
        self.data.len()
    }
}

pub struct Logging {
    worker_id: u16,
    flushed_signal: AtomicUsize,
    desc: SharedDesc,
    wait_txn: Mutex<VecDeque<Transaction>>,
    sem: Arc<Countblock>,
    ring: Ring,
    fsn: Arc<AtomicU64>,
    pub(crate) lsn: AtomicU64,
    buffer: Arc<Buffers>,
}

unsafe impl Sync for Logging {}
unsafe impl Send for Logging {}

pub struct LogBuilder {
    buf: ByteArray,
    off: usize,
    desc: WalDesc,
}

impl LogBuilder {
    fn new(b: ByteArray, tail: usize, txid: u64, fsn: u64) -> Self {
        Self {
            buf: b,
            off: 0,
            desc: WalDesc {
                version: 0,
                fsn,
                txid,
                wal_tail: tail,
            },
        }
    }

    pub fn add<T>(&mut self, payload: T) -> &mut Self
    where
        T: IWalCodec,
    {
        let src = payload.to_slice();
        let dst = self.buf.as_mut_slice(self.off, src.len());
        dst.copy_from_slice(src);
        self.off += src.len();

        self
    }

    pub fn build(&self, log: &Logging) {
        log.update_desc(&self.desc);
    }
}

impl Logging {
    fn alloc(&self, size: usize) -> ByteArray {
        let rest = self.ring.len() - self.ring.tail();
        if rest < size {
            let a = self.ring.prod(rest);
            if rest < WalSpan::size() {
                a.as_mut_slice(0, a.len()).fill(WalPadding::default());
            } else {
                let span = WalSpan {
                    wal_type: EntryType::Span,
                    span: (rest - WalSpan::size()) as u32,
                };
                let dst = a.as_mut_slice(0, span.encoded_len());
                dst.copy_from_slice(span.to_slice());
            }
        }
        while self.ring.avail() < size {
            self.sem.post();
        }
        self.ring.prod(size)
    }

    fn update_desc(&self, desc: &WalDesc) {
        // notify that we are going to write data to arena
        self.buffer.mark_dirty();
        self.desc.set(desc);
        self.sem.post();
    }

    pub(crate) fn new(
        fsn: Arc<AtomicU64>,
        opt: Arc<Options>,
        wid: u16,
        sem: Arc<Countblock>,
        buffer: Arc<Buffers>,
    ) -> Self {
        Self {
            flushed_signal: AtomicUsize::new(0),
            worker_id: wid,
            desc: SharedDesc::new(),
            wait_txn: Mutex::new(VecDeque::new()),
            sem,
            ring: Ring::new(opt.wal_buffer_size),
            fsn,
            lsn: AtomicU64::new(0),
            buffer,
        }
    }

    #[inline]
    fn next_fsn(&self) -> u64 {
        self.fsn.fetch_add(1, Relaxed)
    }

    pub fn record_update<T>(&self, ver: Ver, w: T, k: &[u8], ov: &[u8], nv: &[u8])
    where
        T: IWalCodec + IWalPayload,
    {
        let payload_size = w.encoded_len() + k.len() + ov.len() + nv.len();
        let u = WalUpdate {
            wal_type: EntryType::Update,
            sub_type: w.sub_type(),
            worker_id: self.worker_id,
            size: payload_size as u32,
            cmd_id: ver.cmd,
            klen: k.len() as u32,
            txid: ver.txid,
            prev_addr: 0,
        };
        let a = self.alloc(u.encoded_len() + payload_size);
        let mut b = LogBuilder::new(a, self.ring.raw_tail(), ver.txid, self.next_fsn());
        b.add(u).add(k).add(w).add(ov).add(nv).build(self);
    }

    fn add_entry<T: IWalCodec>(&self, w: T, txid: u64) {
        let size = w.encoded_len();
        let a = self.alloc(size);
        let mut b = LogBuilder::new(a, self.ring.raw_tail(), txid, self.next_fsn());
        b.add(w).build(self);
    }

    pub fn record_begin(&self, txid: u64) {
        self.add_entry(
            WalBegin {
                wal_type: EntryType::Begin,
                txid,
            },
            txid,
        );
    }

    pub fn record_commit(&self, txid: u64) {
        self.add_entry(
            WalCommit {
                wal_type: EntryType::Commit,
                txid,
            },
            txid,
        );
    }

    pub fn record_abort(&self, txid: u64) {
        self.add_entry(
            WalAbort {
                wal_type: EntryType::Abort,
                txid,
            },
            txid,
        );
    }

    pub(crate) fn fsn(&mut self) -> u64 {
        self.fsn.load(Relaxed)
    }

    pub(crate) fn append_txn(&mut self, txn: Transaction) {
        self.wait_txn
            .lock()
            .expect("can't lock write")
            .push_back(txn);
    }

    pub(crate) fn signal_flushed(&self, signal: usize) {
        self.flushed_signal.store(signal, Relaxed);
    }

    pub(crate) fn wait_commit(&self, commit_ts: u64) {
        while commit_ts > self.flushed_signal.load(Relaxed) as u64 {
            self.sem.post();
        }
    }

    pub(crate) fn wait_flush(&self) {
        while self.ring.raw_head() != self.ring.raw_tail() {
            self.sem.post();
        }
    }
}

#[derive(Default, Clone, Copy, Debug)]
struct WalDesc {
    version: u64,
    fsn: u64,
    txid: u64,
    wal_tail: usize,
}

pub struct CState {
    state: AtomicU32,
}

impl CState {
    const GC_STARTED: u32 = 1;
    const GC_WORKING: u32 = 3;
    const GC_STOP: u32 = 5;
    const GC_STOPPED: u32 = 7;

    pub fn new() -> Arc<Self> {
        Arc::new(CState {
            state: AtomicU32::new(Self::GC_STARTED),
        })
    }

    pub fn mark_working(&self) {
        self.state.store(Self::GC_WORKING, Relaxed);
    }

    pub fn mark_done(&self) {
        self.state.store(Self::GC_STOPPED, Release);
    }

    pub fn mark_stop(&self) {
        self.state.store(Self::GC_STOP, Release);
    }

    pub fn is_stop(&self) -> bool {
        self.state.load(Relaxed) == Self::GC_STOP
    }

    pub fn is_working(&self) -> bool {
        self.state.load(Relaxed) == Self::GC_WORKING
    }

    pub fn wait(&self) {
        while self.state.load(Acquire) != Self::GC_STOPPED {
            std::hint::spin_loop();
        }
    }
}

pub struct GroupCommitter {
    /// flush sequence number to check whether a txn can be committed
    opt: Arc<Options>,
    meta: Arc<Meta>,
    last_flush: u32,
    ckpt: CkptMem,
    wal_size: u32,
    last_ckpt_id: u32,
    last_ckpt_pos: u32,
    checkpointed: bool,
    writer: WalWriter,
}

impl GroupCommitter {
    pub(crate) fn new(opt: Arc<Options>, meta: Arc<Meta>) -> Self {
        let workers = opt.workers;
        let (id, off) = unpack_id(meta.ckpt.load(Relaxed));
        assert!(id >= NEXT_ID);
        let next_wal = meta.next_wal.load(Relaxed);
        let writer = WalWriter::new(opt.wal_file(next_wal));
        let pos = writer.pos() as u32;

        Self {
            opt,
            last_flush: meta.next_data.load(Relaxed),
            meta,
            ckpt: CkptMem::new(workers),
            wal_size: pos,
            last_ckpt_id: id,
            last_ckpt_pos: off,
            checkpointed: false,
            writer,
        }
    }

    /// TODO: separate log buffer collection and checkpointing into [`Logging`]
    pub(crate) fn run(
        &mut self,
        ctrl: Arc<CState>,
        buffer: Arc<Buffers>,
        workers: Arc<Vec<SyncWorker>>,
        sem: Arc<Countblock>,
    ) {
        let mut desc = HashMap::new();
        let mut wait = true;
        let lsn = pack_id(self.last_ckpt_id, self.wal_size);
        for w in workers.iter() {
            w.logging.lsn.store(lsn, Release);
            desc.insert(w.id, WalDesc::default());
        }

        while !ctrl.is_stop() {
            if wait {
                sem.wait();
            }
            let (min_flush_txid, min_fsn) = self.collect(&workers, &mut desc);

            if !self.writer.is_empty() {
                self.flush();
                buffer.update_flsn();
            }

            wait = self.commit_txn(&workers, min_flush_txid, min_fsn, &desc);

            if self.should_switch() {
                self.switch();
            }

            if ctrl.is_working() && self.try_stabilize() {
                // count ckpt per txn
                for w in workers.iter() {
                    w.ckpt_cnt.fetch_add(1, Relaxed);
                }
            }
        }

        ctrl.mark_done();
    }

    fn collect(
        &mut self,
        workers: &Arc<Vec<SyncWorker>>,
        desc: &mut HashMap<u16, WalDesc>,
    ) -> (u64, u64) {
        let mut min_fsn = u64::MAX;
        let mut min_flush_txid = u64::MAX;
        let cur_id = self.meta.next_wal.load(Relaxed);

        for w in workers.iter() {
            let d = desc.get_mut(&w.id).unwrap();
            let saved_ver = d.version;
            let version = w.logging.desc.get(d);
            d.version = version;

            if version == saved_ver {
                // current worker has no txn log since last collect
                continue;
            }

            min_flush_txid = min(min_flush_txid, d.txid);
            min_fsn = min(min_fsn, d.fsn);

            let wal_head = w.logging.ring.head();
            let wal_tail = d.wal_tail & w.logging.ring.mask();
            let data = w.logging.ring.data();

            let beg = pack_id(cur_id, self.wal_size);
            let txid = match wal_tail.cmp(&wal_head) {
                Ordering::Greater => {
                    self.queue_data(data, wal_head, wal_tail);
                    set_lsn(data, wal_head, wal_tail, &w.logging.lsn, beg)
                }
                Ordering::Less => {
                    let len = w.logging.ring.len();

                    self.queue_data(data, wal_head, len);
                    let r = set_lsn(data, wal_head, len, &w.logging.lsn, beg);

                    let beg = w.logging.lsn.load(Relaxed);
                    self.queue_data(data, 0, wal_tail);
                    set_lsn(data, 0, wal_tail, &w.logging.lsn, beg).map_or(r, Some)
                }
                _ => None,
            };

            if let Some(txid) = txid {
                // save latest lsn to txid
                self.ckpt.set(w.id, txid, w.logging.lsn.load(Relaxed));
            } else {
                self.ckpt.reset(w.id);
            }
        }

        (min_flush_txid, min_fsn)
    }

    fn flush(&mut self) {
        self.writer.flush();
        if self.opt.sync_on_write {
            self.writer.sync();
        }
        if self.checkpointed {
            self.meta
                .update_chkpt(self.last_ckpt_id, self.last_ckpt_pos);
            self.meta.sync(self.opt.meta_file(), false);
            self.checkpointed = false;
        }
    }

    fn commit_txn(
        &self,
        workers: &Arc<Vec<SyncWorker>>,
        min_flush_txid: u64,
        min_fsn: u64,
        desc: &HashMap<u16, WalDesc>,
    ) -> bool {
        let mut wait_count = 0;
        for w in workers.iter() {
            let d = desc.get(&w.id).expect("never happen");
            let mut max_flush_ts = 0;
            w.logging.ring.cons(d.wal_tail);

            let mut lk = w.logging.wait_txn.lock().expect("can't lock");
            while let Some(txn) = lk.front() {
                // since the txns in same worker are ordered, the subsequent txns are not ready too
                if !txn.ready_to_commit(min_flush_txid, min_fsn) {
                    break;
                }
                max_flush_ts = max(max_flush_ts, txn.commit_ts);
                lk.pop_front();
            }
            wait_count += lk.len();
            drop(lk);

            if max_flush_ts != 0 {
                w.logging.signal_flushed(max_flush_ts as usize);
            }
        }
        wait_count == 0
    }

    fn queue_data(&mut self, buf: *const u8, from: usize, to: usize) {
        let data = unsafe { buf.add(from) };
        let count = to - from;

        self.writer.queue(data, count);
        self.wal_size += count as u32;
    }

    #[inline(always)]
    fn should_switch(&self) -> bool {
        self.wal_size >= self.opt.wal_file_size
    }

    fn switch(&mut self) {
        // we never use the number 0
        let current = self.meta.next_wal.load(Relaxed);
        let mut next_wal = current.wrapping_add(1);
        if next_wal == 0 {
            next_wal = NEXT_ID;
        }
        log::info!("roll wal from {} to {}", current, next_wal);
        self.wal_size = 0;
        self.writer.reset(self.opt.wal_file(next_wal));
        self.meta.next_wal.store(next_wal, Relaxed);
    }

    fn try_stabilize(&mut self) -> bool {
        let curr = self.meta.next_data.load(Relaxed);

        if curr == self.last_flush {
            return false;
        }

        self.last_flush = curr;

        log::info!("checkpoint {:?}", (self.last_ckpt_id, self.last_ckpt_pos));

        assert_ne!(self.last_ckpt_id, 0);
        let prev_addr = pack_id(self.last_ckpt_id, self.last_ckpt_pos);
        self.last_ckpt_id = self.meta.next_wal.load(Relaxed);
        self.last_ckpt_pos = self.wal_size; // point to current position

        let (hdr, data) = self.ckpt.slice(self.meta.oracle.load(Relaxed), prev_addr);
        self.queue_data(hdr.0, 0, hdr.1);
        self.queue_data(data.0, 0, data.1);
        self.writer.flush();
        self.checkpointed = true;
        true
    }
}

struct WalWriter {
    file: File,
    path: PathBuf,
    queue: Vec<IoVec>,
}

unsafe impl Send for WalWriter {}

impl WalWriter {
    fn open_file(path: PathBuf) -> File {
        File::options()
            .write(true)
            .append(true)
            .create(true)
            .open(&path)
            .inspect_err(|x| {
                log::error!("can't open {:?}, {}", path, x);
            })
            .unwrap()
    }

    fn new(path: PathBuf) -> Self {
        Self {
            path: path.clone(),
            file: Self::open_file(path),
            queue: Vec::new(),
        }
    }
    fn reset(&mut self, path: PathBuf) {
        self.path = path.clone();
        self.flush();
        self.file = Self::open_file(path);
    }

    fn queue(&mut self, data: *const u8, len: usize) {
        self.queue.push(IoVec::new(data, len));
    }

    fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    fn pos(&self) -> u64 {
        self.file
            .size()
            .inspect_err(|x| {
                log::error!("can't get metadata of {:?}, {}", self.path, x);
            })
            .unwrap()
    }

    fn flush(&mut self) {
        let iov = self.queue.as_mut_slice();
        self.file
            .writev(iov)
            .inspect_err(|x| log::error!("can't write iov, {}", x))
            .unwrap();
        self.queue.clear();
    }

    fn sync(&mut self) {
        self.file
            .sync()
            .inspect_err(|x| log::error!("can't sync {:?}, {}", self.path, x))
            .unwrap();
    }
}

impl Drop for WalWriter {
    fn drop(&mut self) {
        self.flush();
    }
}
