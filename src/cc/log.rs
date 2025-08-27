use super::wal::{IWalCodec, IWalPayload, WalAbort, WalBegin, WalCheckpoint, WalCommit, WalUpdate};
use crate::types::data::Ver;
use crate::utils::{data::WalDescHandle, options::ParsedOptions, unpack_id};
use crate::{
    cc::wal::{EntryType, WalPadding, WalSpan},
    utils::{
        block::Block,
        data::{GatherWriter, Meta},
        pack_id,
    },
};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, atomic::Ordering::Relaxed};

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

    fn avail(&self) -> usize {
        self.data.len() - (self.tail - self.head)
    }

    // NOTE: the request buffer never wraps around
    fn prod(&mut self, size: usize) -> &mut [u8] {
        debug_assert!(self.avail() >= size);
        let mut b = self.tail;
        self.tail += size;

        b &= self.mask();
        self.data.mut_slice(b, size)
    }

    fn cons(&mut self, pos: usize) {
        self.head += pos;
    }

    fn distance(&self) -> usize {
        self.tail - self.head
    }

    fn head(&self) -> usize {
        self.head & self.mask()
    }

    fn tail(&self) -> usize {
        self.tail & self.mask()
    }

    fn slice(&self, pos: usize, len: usize) -> &[u8] {
        self.data.slice(pos, len)
    }

    fn mask(&self) -> usize {
        self.data.len() - 1
    }

    fn len(&self) -> usize {
        self.data.len()
    }
}

pub struct LogBuilder {
    off: usize,
    len: usize,
}

impl LogBuilder {
    fn new(len: usize) -> Self {
        Self { off: 0, len }
    }

    pub fn add<T>(&mut self, buf: &mut [u8], payload: T) -> &mut Self
    where
        T: IWalCodec,
    {
        let src = payload.to_slice();
        let dst = &mut buf[self.off..self.off + src.len()];
        dst.copy_from_slice(src);
        self.off += src.len();

        self
    }

    pub fn build(&self, log: &mut Logging) {
        log.advance(self.len);
    }
}

pub struct Logging {
    ring: Ring,
    enable_ckpt: AtomicBool,
    // save last checkpoint position, used by gc
    last_ckpt: u64,
    ckpt_cnt: usize,
    // used for building traverse chain (lsn)
    log_id: u32,
    log_off: u32,
    lsn: u64,
    ops: usize,
    last_data: u32,
    writer: GatherWriter,
    opt: Arc<ParsedOptions>,
    pub desc: WalDescHandle,
    meta: Arc<Meta>,
    #[cfg(feature = "extra_check")]
    last_id: u64,
}

unsafe impl Sync for Logging {}
unsafe impl Send for Logging {}

impl Logging {
    const AUTO_STABLE: u32 = <usize>::trailing_zeros(32);

    pub(crate) fn new(desc: WalDescHandle, meta: Arc<Meta>, opt: Arc<ParsedOptions>) -> Self {
        let writer = GatherWriter::new(&opt.wal_file(desc.worker, desc.wal_id), 32);
        Self {
            ring: Ring::new(opt.wal_buffer_size),
            enable_ckpt: AtomicBool::new(false),
            last_ckpt: desc.checkpoint,
            ckpt_cnt: 0,
            log_id: desc.wal_id,
            log_off: writer.pos() as u32,
            lsn: pack_id(desc.wal_id, writer.pos() as u32),
            ops: 0,
            last_data: meta.flush_data.load(Relaxed),
            writer,
            opt,
            desc,
            meta,
            #[cfg(feature = "extra_check")]
            last_id: 0,
        }
    }

    fn alloc(&mut self, size: usize) -> &mut [u8] {
        let rest = self.ring.len() - self.ring.tail();
        if rest < size {
            let a = self.ring.prod(rest);
            if rest < WalSpan::size() {
                a.fill(WalPadding::default().into());
            } else {
                let span = WalSpan {
                    wal_type: EntryType::Span,
                    span: (rest - WalSpan::size()) as u32,
                };
                let dst = &mut a[0..span.encoded_len()];
                dst.copy_from_slice(span.to_slice());
            }
            // excluding Padding and Span
            self.log_off += rest as u32;
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

        if self.opt.sync_on_write {
            self.flush();
        } else {
            self.ops = self.ops.wrapping_add(1);
            if self.ops.trailing_zeros() >= Self::AUTO_STABLE {
                self.flush();
            }
        }

        self.checkpoint();
    }

    pub fn enable_checkpoint(&self) {
        self.enable_ckpt.store(true, Relaxed);
    }

    pub fn lsn(&self) -> u64 {
        self.lsn
    }

    pub fn ckpt_cnt(&self) -> usize {
        self.ckpt_cnt
    }

    pub fn reset_ckpt_cnt(&mut self) {
        self.ckpt_cnt = 0;
    }

    pub fn last_ckpt(&self) -> u64 {
        self.last_ckpt
    }

    #[cold]
    fn record_large(
        &mut self,
        u: &WalUpdate,
        k: &[u8],
        w: &[u8],
        ov: &[u8],
        nv: &[u8],
        size: usize,
    ) {
        self.flush(); // flush queued first, make sure log record is sequentail
        self.writer.queue(u.to_slice());
        self.writer.queue(k);
        self.writer.queue(w);
        self.writer.queue(ov);
        self.writer.queue(nv);
        self.writer.flush();
        self.advance(size);
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
        let total_sz = payload_size + u.encoded_len();
        if total_sz < self.ring.len() {
            let a = self.alloc(total_sz);
            let mut b = LogBuilder::new(a.len());
            b.add(a, u)
                .add(a, k)
                .add(a, w)
                .add(a, ov)
                .add(a, nv)
                .build(self);
        } else {
            self.record_large(&u, k, w.to_slice(), ov, nv, total_sz);
        }
    }

    fn add_entry<T: IWalCodec>(&mut self, w: T) {
        let size = w.encoded_len();
        let a = self.alloc(size);
        let mut b = LogBuilder::new(a.len());
        b.add(a, w).build(self);
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
        let cur = self.meta.flush_data.load(Relaxed);

        if cur == self.last_data {
            return;
        }
        let last_ckpt = self.last_ckpt;

        log::trace!(
            "worker {} checkpoint {:?} curr {} last {}",
            self.desc.worker,
            unpack_id(last_ckpt),
            cur,
            self.last_data
        );
        self.last_data = cur;

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

        self.last_ckpt = pack_id(self.log_id, self.log_off);

        self.log_off += ckpt.encoded_len() as u32;
        self.sync_desc();
        let wid = self.desc.worker as u32;

        let lk = self.meta.mask.read().unwrap();
        if !lk.test(wid) {
            drop(lk);
            let mut lk = self.meta.mask.write().unwrap();
            lk.set(wid);
            drop(lk);
            self.meta.sync(self.opt.meta_file(), false);
        }

        self.ckpt_cnt += 1;
    }

    pub(crate) fn sync_desc(&self) {
        let mut desc = self.desc.clone();
        desc.checkpoint = self.last_ckpt;
        desc.wal_id = self.log_id;
        desc.sync(self.opt.desc_file(self.desc.worker));
    }

    fn flush(&mut self) -> bool {
        if self.ring.distance() == 0 {
            return false;
        }

        let head = self.ring.head();
        let tail = self.ring.tail();

        let len = if tail >= head {
            tail - head
        } else {
            self.ring.len() - head
        };

        self.writer.write(self.ring.slice(head, len));
        self.ring.cons(len);

        if self.opt.sync_on_write {
            self.writer.sync();
        }

        true
    }

    pub fn stabilize(&mut self) {
        if self.flush() {
            self.checkpoint()
        }
    }
}
