use std::{
    cmp::{max, min, Ordering},
    collections::{HashMap, VecDeque},
    path::PathBuf,
    sync::{
        atomic::{
            AtomicU32, AtomicU64, AtomicUsize,
            Ordering::{Acquire, Relaxed, Release},
        },
        mpsc::{Receiver, Sender},
        Arc, Mutex,
    },
};

use io::{File, IoVec, SeekableGatherIO};

use crate::{
    cc::wal::{EntryType, WalPadding},
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
        WalTree, WalUpdate,
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

    fn update_wal(&self, tail: usize) {
        self.version.fetch_add(1, Acquire);
        unsafe { (*self.data).wal_tail = tail }
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
            EntryType::TreePut | EntryType::TreeDel => WalTree::size(),
            EntryType::Abort | EntryType::Begin | EntryType::Commit => {
                let x = to::<WalBegin>(p);
                x.set_lsn(last_record_pos);
                last = Some((h, x.txid));
                WalAbort::size()
            }
            EntryType::Update => {
                let u = to::<WalUpdate>(p);
                last = Some((h, u.txid));
                u.set_lsn(last_record_pos);
                u.size as usize + u.encoded_len()
            }
            EntryType::Padding => {
                let x = to::<WalPadding>(p);
                x.len as usize + WalPadding::size()
            }
            EntryType::CheckPoint => {
                let c = to::<WalCheckpoint>(p);
                c.payload_len() + WalCheckpoint::size()
            }
            _ => unreachable!("invalid type {}", h as u8),
        };

        off += sz;
        // excluding padding and tree from txn chain, they are not involved in rollback
        if !matches!(
            h,
            EntryType::Padding | EntryType::TreeDel | EntryType::TreePut
        ) {
            last_record_pos = lsn.fetch_add(sz as u64, Relaxed);
        } else {
            sz = 0;
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

pub struct Logging {
    worker_id: u16,
    flushed_signal: AtomicUsize,
    /// circular buffer head and tail
    wal_head: AtomicUsize,
    wal_tail: AtomicUsize,
    desc: SharedDesc,
    wait_txn: Mutex<VecDeque<Transaction>>,
    sem: Arc<Countblock>,
    buffer: Block,
    fsn: Arc<AtomicU64>,
    pub(crate) lsn: AtomicU64,
}

unsafe impl Sync for Logging {}
unsafe impl Send for Logging {}

pub struct LogBuilder<'a> {
    log: &'a Logging,
    buf: ByteArray,
    desc: WalDesc,
}

impl<'a> LogBuilder<'a> {
    fn new(l: &'a Logging, b: ByteArray, txid: u64, fsn: u64) -> Self {
        let old = l.wal_tail.fetch_add(b.len(), Release);
        Self {
            log: l,
            buf: b,
            desc: WalDesc {
                version: 0,
                fsn,
                txid,
                wal_tail: old + b.len(),
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

    pub fn build(&self) {
        self.log.update_desc(&self.desc);
    }
}

impl Logging {
    fn alloc_entry(&self, size: usize) -> ByteArray {
        let len = self.buffer.len();
        loop {
            self.sem.post();
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

    fn update_desc(&self, desc: &WalDesc) {
        self.desc.set(desc);
        self.sem.post();
    }

    pub(crate) fn new(
        fsn: Arc<AtomicU64>,
        opt: Arc<Options>,
        wid: u16,
        sem: Arc<Countblock>,
    ) -> Self {
        let buffer = Block::aligned_alloc(opt.buffer_size as usize, 1);
        buffer.zero();
        Self {
            flushed_signal: AtomicUsize::new(0),
            worker_id: wid,
            wal_head: AtomicUsize::new(0),
            wal_tail: AtomicUsize::new(0),
            desc: SharedDesc::new(),
            wait_txn: Mutex::new(VecDeque::new()),
            sem,
            buffer,
            fsn,
            lsn: AtomicU64::new(0),
        }
    }

    pub fn record_update<T>(&self, ver: Ver, tree_id: u64, w: T, k: &[u8], ov: &[u8], nv: &[u8])
    where
        T: IWalCodec + IWalPayload,
    {
        let payload_size = w.encoded_len() + k.len() + ov.len() + nv.len();
        let fsn = self.fsn.fetch_add(1, Relaxed);
        let u = WalUpdate {
            wal_type: EntryType::Update,
            sub_type: w.sub_type(),
            worker_id: self.worker_id,
            size: payload_size as u32,
            cmd_id: ver.cmd,
            klen: k.len() as u32,
            tree_id,
            txid: ver.txid,
            prev_addr: 0,
        };
        let a = self.alloc_entry(u.encoded_len() + payload_size);
        let mut b = LogBuilder::new(self, a, ver.txid, fsn);
        b.add(u).add(k).add(w).add(ov).add(nv).build();
    }

    fn add_entry<T: IWalCodec>(&self, w: T, txid: u64) {
        let size = w.encoded_len();
        let a = self.alloc_entry(size);
        w.encode_to(a.sub_array(0, size));
        let old = self.wal_tail.fetch_add(size, Release);
        self.update_desc(&WalDesc {
            version: 0,
            fsn: self.fsn.fetch_add(1, Relaxed),
            txid,
            wal_tail: old + size,
        });
    }

    pub fn record_begin(&self, txid: u64) {
        self.add_entry(
            WalBegin {
                wal_type: EntryType::Begin,
                txid,
                prev_addr: 0,
            },
            txid,
        );
    }

    pub fn record_commit(&self, txid: u64) {
        self.add_entry(
            WalCommit {
                wal_type: EntryType::Commit,
                txid,
                prev_addr: 0,
            },
            txid,
        );
    }

    pub fn record_abort(&self, txid: u64) {
        let w = WalAbort {
            wal_type: EntryType::Abort,
            txid,
            prev_addr: 0,
        };
        self.add_entry(w, txid);
    }

    pub(crate) fn record_put(&self, id: u64, txid: u64, pid: u64) {
        self.add_entry(WalTree::new(true, id, txid, pid), txid);
    }

    pub(crate) fn record_del(&self, id: u64, txid: u64, pid: u64) {
        self.add_entry(WalTree::new(false, id, txid, pid), txid);
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
        while self.wal_head.load(Relaxed) != self.wal_tail.load(Relaxed) {
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
    /// AIO will use this field until it's completed
    ckpt: CkptMem,
    wal_size: u64,
    flushed_size: usize,
    last_ckpt_id: u16,
    last_ckpt_pos: u64,
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
        let pos = writer.pos();

        Self {
            opt,
            meta,
            ckpt: CkptMem::new(workers),
            wal_size: pos,
            flushed_size: 0,
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
        tx: Arc<Sender<()>>,
        rx: Receiver<()>,
        sem: Arc<Countblock>,
    ) {
        let mut desc = HashMap::new();
        let lsn = pack_id(self.last_ckpt_id, self.wal_size);
        for w in workers.iter() {
            w.logging.lsn.store(lsn, Release);
            desc.insert(w.id, WalDesc::default());
        }

        while !ctrl.is_stop() {
            sem.wait();
            let (min_flush_txid, min_fsn) = self.collect(&workers, &mut desc);

            if !self.writer.is_empty() {
                self.flush();
                buffer.update_flsn(pack_id(self.meta.next_wal.load(Acquire), self.wal_size));
            }

            self.commit_txn(&workers, min_flush_txid, min_fsn, &desc);

            if self.should_switch() {
                self.switch();
            }

            if ctrl.is_working() && self.should_stabilize() {
                // count ckpt per txn
                for w in workers.iter() {
                    w.ckpt_cnt.fetch_add(1, Relaxed);
                }
                self.request_stabilize(&buffer, &tx, &rx);
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

            let wal_head = w.logging.wal_head.load(Relaxed);
            let wal_tail = d.wal_tail;
            let data = w.logging.buffer.data();

            let beg = pack_id(cur_id, self.wal_size);
            let txid = match wal_tail.cmp(&wal_head) {
                Ordering::Greater => {
                    self.queue_data(data, wal_head, wal_tail);
                    set_lsn(data, wal_head, wal_tail, &w.logging.lsn, beg)
                }
                Ordering::Less => {
                    let len = w.logging.buffer.len();

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
        self.writer.sync();
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
    ) {
        for w in workers.iter() {
            let Some(d) = desc.get(&w.id) else {
                continue;
            };
            let mut max_flush_ts = 0;
            w.logging.wal_head.store(d.wal_tail, Relaxed);

            let mut lk = w.logging.wait_txn.lock().expect("can't lock");
            while let Some(txn) = lk.front() {
                // since the txns in same worker are ordered, the subsequent txns are not ready too
                if !txn.ready_to_commit(min_flush_txid, min_fsn) {
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

    fn queue_data(&mut self, buf: *const u8, from: usize, to: usize) {
        let data = unsafe { buf.add(from) };
        let count = to - from;

        self.writer.queue(data, count);
        self.wal_size += count as u64;
        self.flushed_size += count;
    }

    #[inline(always)]
    fn should_switch(&self) -> bool {
        self.wal_size >= self.opt.wal_file_size as u64
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

    #[inline(always)]
    fn should_stabilize(&self) -> bool {
        self.flushed_size >= self.opt.ckpt_per_bytes
    }

    fn request_stabilize(
        &mut self,
        buffer: &Arc<Buffers>,
        tx: &Arc<Sender<()>>,
        rx: &Receiver<()>,
    ) {
        if !buffer.should_stabilize() {
            return;
        }

        let tx = tx.clone();
        buffer.stabilize(Box::new(move |_| {
            let _ = tx.send(()).inspect_err(|e| {
                log::error!("can't notify flushed {}", e);
            });
        }));

        log::info!("checkpoint {:?}", (self.last_ckpt_id, self.last_ckpt_pos));
        self.flushed_size = 0;
        let r = rx.recv();
        assert!(r.is_ok());

        assert_ne!(self.last_ckpt_id, 0);
        let prev_addr = pack_id(self.last_ckpt_id, self.last_ckpt_pos);
        self.last_ckpt_id = self.meta.next_wal.load(Relaxed);
        self.last_ckpt_pos = self.wal_size; // point to current position

        let (hdr, data) = self.ckpt.slice(self.meta.oracle.load(Relaxed), prev_addr);
        self.queue_data(hdr.0, 0, hdr.1);
        self.queue_data(data.0, 0, data.1);
        self.writer.sync();
        self.checkpointed = true;
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
        self.sync();
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

    fn sync(&mut self) {
        let iov = self.queue.as_mut_slice();
        self.file
            .write(iov)
            .inspect_err(|x| log::error!("can't write iov, {}", x))
            .unwrap();
        self.file
            .flush()
            .inspect_err(|x| log::error!("can't sync {:?}, {}", self.path, x))
            .unwrap();
        self.queue.clear();
    }
}

impl Drop for WalWriter {
    fn drop(&mut self) {
        self.sync();
    }
}

#[cfg(test)]
mod test {

    use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
    use std::sync::atomic::{fence, AtomicBool, AtomicU64};
    use std::sync::Arc;

    use crate::cc::data::Ver;
    use crate::slice_to_number;
    use crate::utils::countblock::Countblock;
    use crate::utils::raw_ptr_to_ref;
    use crate::{
        cc::{
            log::{IWalCodec, WalBegin, WalDesc, WalPadding, WalUpdate},
            wal::WalPut,
        },
        static_assert, Options, RandomPath,
    };

    use super::Logging;

    #[test]
    fn logging() {
        let path = RandomPath::tmp();
        let mut opt = Options::new(&*path);
        opt.buffer_size = 512;
        let opt = Arc::new(opt);
        let _ = std::fs::create_dir_all(&opt.db_root);
        let fsn = Arc::new(AtomicU64::new(0));
        let l = Logging::new(fsn, opt, 0, Arc::new(Countblock::new(1)));
        let txid = 1;

        l.record_begin(txid);
        let mut desc = WalDesc::default();
        l.desc.get(&mut desc);
        assert_eq!(desc.wal_tail, size_of::<WalBegin>());

        assert_eq!(l.wal_tail.load(Relaxed), size_of::<WalBegin>());

        let (k, v) = ("mo".as_bytes(), "ha".as_bytes());
        let ins = WalPut::new(2);
        l.record_update(Ver::new(txid, 1), 1, ins, k, [].as_slice(), v);

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
