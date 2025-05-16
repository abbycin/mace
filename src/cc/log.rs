use super::{
    data::Ver,
    wal::{
        IWalCodec, IWalPayload, WalAbort, WalBegin, WalCheckpoint, WalCommit, WalUpdate, ptr_to,
    },
};
use crate::utils::data::WalDescHandle;
use crate::{
    Options,
    cc::wal::{EntryType, WalPadding, WalSpan},
    map::buffer::Buffers,
    utils::{
        block::Block,
        bytes::ByteArray,
        data::{GatherWriter, Meta},
        pack_id, unpack_id,
    },
};
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering::Relaxed},
};

struct Ring {
    data: Block,
    head: usize,
    tail: usize,
}

impl Ring {
    fn new(cap: usize) -> Self {
        let data = Block::aligned_alloc(cap, 1);
        data.zero();
        assert!(data.len().is_power_of_two());
        Self {
            data,
            head: 0,
            tail: 0,
        }
    }

    #[inline]
    fn avail(&self) -> usize {
        self.data.len() - (self.tail - self.head)
    }

    // NOTE: the request buffer never wraps around
    fn prod(&mut self, size: usize) -> ByteArray {
        debug_assert!(self.avail() >= size);
        let mut b = self.tail;
        self.tail += size;

        b &= self.mask();
        self.data.view(b, b + size)
    }

    #[inline]
    fn cons(&mut self, pos: usize) {
        self.head += pos;
    }

    #[inline]
    fn head(&self) -> usize {
        self.head & self.mask()
    }

    #[inline]
    fn tail(&self) -> usize {
        self.tail & self.mask()
    }

    #[inline]
    fn slice(&self, pos: usize, len: usize) -> &[u8] {
        self.data.slice(pos, len)
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

pub struct LogBuilder {
    buf: ByteArray,
    off: usize,
}

impl LogBuilder {
    fn new(b: ByteArray) -> Self {
        Self { buf: b, off: 0 }
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

    pub fn build(&self, log: &mut Logging) {
        log.advance(self.buf.len());
    }
}

pub struct Logging {
    ring: Ring,
    enable_ckpt: AtomicBool,
    // save last checkpoint position, used by gc
    pub last_ckpt: AtomicU64,
    ckpt_cnt: Arc<AtomicUsize>,
    // used for building traverse chain (lsn)
    log_id: u32,
    log_off: u32,
    lsn: u64,
    ops: usize,
    last_data: u32,
    buffer: Arc<Buffers>,
    writer: GatherWriter,
    opt: Arc<Options>,
    pub desc: WalDescHandle,
    meta: Arc<Meta>,
    #[cfg(feature = "extra_check")]
    last_id: u64,
}

unsafe impl Sync for Logging {}
unsafe impl Send for Logging {}

impl Logging {
    const AUTO_STABLE: u32 = <usize>::trailing_zeros(32);

    pub(crate) fn new(
        ckpt_cnt: Arc<AtomicUsize>,
        desc: WalDescHandle,
        meta: Arc<Meta>,
        opt: Arc<Options>,
        buffer: Arc<Buffers>,
    ) -> Self {
        let writer = GatherWriter::new(&opt.wal_file(desc.worker, desc.wal_id));
        Self {
            ring: Ring::new(opt.wal_buffer_size),
            enable_ckpt: AtomicBool::new(false),
            last_ckpt: AtomicU64::new(0),
            ckpt_cnt,
            log_id: desc.wal_id,
            log_off: writer.pos() as u32,
            lsn: pack_id(desc.wal_id, writer.pos() as u32),
            ops: 0,
            last_data: meta.next_data.load(Relaxed),
            buffer,
            writer,
            opt,
            desc,
            meta,
            #[cfg(feature = "extra_check")]
            last_id: 0,
        }
    }

    fn alloc(&mut self, size: usize) -> ByteArray {
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
            self.flush();
        }
        self.ring.prod(size)
    }

    fn advance(&mut self, data_len: usize) {
        self.lsn = pack_id(self.log_id, self.log_off);

        // maybe switch wal file
        self.log_off += data_len as u32;
        if self.log_off >= self.opt.wal_file_size {
            self.log_id += 1;
            self.log_off = 0;

            self.flush();
            self.writer
                .reset(&self.opt.wal_file(self.desc.worker, self.log_id));
        }

        // notify that we are going to write data to arena
        self.buffer.mark_dirty();

        self.ops = self.ops.wrapping_add(1);
        if self.ops.trailing_zeros() >= Self::AUTO_STABLE {
            self.flush();
        }

        self.checkpoint();
    }

    pub fn enable_checkpoint(&self) {
        self.enable_ckpt.store(true, Relaxed);
    }

    pub fn lsn(&self) -> u64 {
        self.lsn
    }

    pub fn record_update<T>(&mut self, ver: Ver, w: T, k: &[u8], ov: &[u8], nv: &[u8])
    where
        T: IWalCodec + IWalPayload,
    {
        let payload_size = w.encoded_len() + k.len() + ov.len() + nv.len();
        let u = WalUpdate {
            wal_type: EntryType::Update,
            sub_type: w.sub_type(),
            worker_id: self.desc.worker,
            size: payload_size as u32,
            cmd_id: ver.cmd,
            klen: k.len() as u32,
            txid: ver.txid,
            prev_addr: self.lsn,
        };
        let a = self.alloc(u.encoded_len() + payload_size);
        let mut b = LogBuilder::new(a);
        b.add(u).add(k).add(w).add(ov).add(nv).build(self);
    }

    fn add_entry<T: IWalCodec>(&mut self, w: T) {
        let size = w.encoded_len();
        let a = self.alloc(size);
        let mut b = LogBuilder::new(a);
        b.add(w).build(self);
    }

    pub fn record_begin(&mut self, txid: u64) {
        self.add_entry(WalBegin {
            wal_type: EntryType::Begin,
            txid,
        });
    }

    pub fn record_commit(&mut self, txid: u64) {
        #[cfg(feature = "extra_check")]
        {
            if self.last_id >= txid {
                log::error!("invalid txid old {} curr {}", self.last_id, txid);
                panic!("invalid txid old {} curr {}", self.last_id, txid);
            }
            self.last_id = txid;
        }
        self.add_entry(WalCommit {
            wal_type: EntryType::Commit,
            txid,
        });
    }

    pub fn record_abort(&mut self, txid: u64) {
        self.add_entry(WalAbort {
            wal_type: EntryType::Abort,
            txid,
        });
    }

    fn checkpoint(&mut self) {
        if !self.enable_ckpt.load(Relaxed) {
            return;
        }
        let cur = self.meta.next_data.load(Relaxed);

        if cur == self.last_data {
            return;
        }
        self.last_data = cur;
        let last_ckpt = self.last_ckpt.load(Relaxed);

        log::info!(
            "checkpoint {:?} curr {} last {}",
            unpack_id(last_ckpt),
            cur,
            self.last_data
        );

        let ckpt = WalCheckpoint {
            wal_type: EntryType::CheckPoint,
            prev_addr: last_ckpt,
        };

        // we must flush buffer in ring to make sure they are stabilized before flush checkpoint
        self.flush();
        self.writer.write(ckpt.to_slice());
        if self.opt.sync_on_write {
            self.writer.sync();
        }

        self.last_ckpt
            .store(pack_id(self.log_id, self.log_off), Relaxed);

        self.log_off += ckpt.encoded_len() as u32;
        self.desc.checkpoint = last_ckpt;
        self.desc.wal_id = self.log_id;
        let wid = self.desc.worker;
        self.desc.sync(self.opt.desc_file(wid));

        let lk = self.meta.mask.read().unwrap();
        if !lk.test(wid as u32) {
            drop(lk);
            let mut lk = self.meta.mask.write().unwrap();
            lk.set(wid as u32);
            drop(lk);
            self.meta.sync(self.opt.meta_file(), false);
        }

        self.ckpt_cnt.fetch_add(1, Relaxed);
    }

    fn flush(&mut self) -> bool {
        let head = self.ring.head();
        let tail = self.ring.tail();

        if head == tail {
            return false;
        }

        let len = if tail == 0 {
            self.ring.len() - head
        } else {
            tail - head
        };

        self.writer.write(self.ring.slice(head, len));
        self.ring.cons(len);

        if self.opt.sync_on_write {
            self.writer.sync();
        }

        // NOTE: the flsn is shared among all workers
        self.buffer.update_flsn();
        true
    }

    /// return true when checkpoint was taken
    pub fn stabilize(&mut self) {
        if self.flush() {
            self.checkpoint()
        }
    }

    #[allow(unused)]
    pub fn parse_log_range(&self) {
        let b = 0;
        let e = self.log_off as usize;
        let data = self.ring.data.data();

        parse_log(data, b, e);
        log::debug!("==> b {} e {} lsn {:?}", b, e, unpack_id(self.lsn()));
    }
}

pub fn parse_log(data: *mut u8, mut b: usize, e: usize) {
    while b < e {
        let (p, h) = unsafe {
            let p = data.add(b);
            let h = p.read().into();
            (p, h)
        };

        // the data is always valid, not need to check
        let sz = match h {
            EntryType::Abort | EntryType::Begin | EntryType::Commit => WalAbort::size(),
            EntryType::Update => {
                let u = ptr_to::<WalUpdate>(p);
                log::debug!("{:?}", u);
                u.size as usize + u.encoded_len()
            }
            EntryType::Padding => WalPadding::size(),
            EntryType::Span => {
                let x = ptr_to::<WalSpan>(p);
                x.span as usize + x.encoded_len()
            }
            EntryType::CheckPoint => WalCheckpoint::size(),
            _ => unreachable!("invalid type {}", h as u8),
        };

        log::debug!("=> {:?} pos {} new {}", h, b, b + sz);

        b += sz;
    }
}
