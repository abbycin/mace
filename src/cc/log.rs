use super::wal::{IWalCodec, IWalPayload, WalAbort, WalBegin, WalCheckpoint, WalCommit, WalUpdate};
use crate::types::data::Ver;
use crate::utils::data::Position;
use crate::utils::{data::WalDescHandle, options::ParsedOptions};
use crate::{
    cc::wal::EntryType,
    utils::{
        block::Block,
        data::{GatherWriter, Meta},
    },
};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
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
        self.data.len() - self.distance()
    }

    // NOTE: the request buffer never wraps around
    fn prod<'a>(&mut self, size: usize) -> &'a mut [u8] {
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
        #[cfg(feature = "extra_check")]
        assert!(self.tail >= self.head);
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

pub struct LogBuilder<'a> {
    off: usize,
    buf: &'a mut [u8],
}

impl<'a> LogBuilder<'a> {
    fn new(buf: &'a mut [u8]) -> Self {
        Self { off: 0, buf }
    }

    pub fn add<T>(&mut self, payload: T) -> &mut Self
    where
        T: IWalCodec,
    {
        let src = payload.to_slice();
        let dst = &mut self.buf[self.off..self.off + src.len()];
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
    /// save last checkpoint position, used by gc
    last_ckpt: Position,
    ckpt_cnt: usize,
    /// used for building traverse chain (lsn)
    log_pos: Position,
    lsn: Position,
    /// what we want is happen before sequentail
    seq: u64,
    /// so use a number rather than real Position
    flushed_lsn: AtomicU64,
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
    const AUTO_STABLE_SIZE: usize = 4 << 20;
    const AUTO_STABLE_OPS: u32 = <usize>::trailing_zeros(32);

    pub(crate) fn new(desc: WalDescHandle, meta: Arc<Meta>, opt: Arc<ParsedOptions>) -> Self {
        let writer = GatherWriter::new(&opt.wal_file(desc.worker, desc.wal_id), 16);
        let pos = Position {
            file_id: desc.wal_id,
            offset: writer.pos(),
        };
        Self {
            ring: Ring::new(opt.wal_buffer_size),
            enable_ckpt: AtomicBool::new(false),
            last_ckpt: desc.checkpoint,
            ckpt_cnt: 0,
            log_pos: pos,
            lsn: pos,
            seq: 0,
            flushed_lsn: AtomicU64::new(0),
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

    fn alloc<'a>(&mut self, size: usize) -> &'a mut [u8] {
        let rest = self.ring.len() - self.ring.tail();
        if rest < size {
            self.flush();
            // skip the rest data, and restart from the begining
            self.ring.prod(rest);
            self.ring.cons(rest);
        }
        self.ring.prod(size)
    }

    fn advance(&mut self, data_len: usize) {
        self.lsn = self.log_pos;
        self.seq += 1;

        // maybe switch wal file
        self.log_pos.offset += data_len as u64;
        if self.log_pos.offset >= self.opt.wal_file_size as u64 {
            self.log_pos.file_id += 1;
            self.log_pos.offset = 0;

            self.flush();
            self.writer
                .reset(&self.opt.wal_file(self.desc.worker, self.log_pos.file_id));
            self.sync_desc();
        }

        self.ops = self.ops.wrapping_add(1);
        if self.ops.trailing_zeros() >= Self::AUTO_STABLE_OPS
            || self.ring.distance() >= Self::AUTO_STABLE_SIZE
        {
            self.flush();
        }

        self.checkpoint();
    }

    pub fn enable_checkpoint(&self) {
        self.enable_ckpt.store(true, Relaxed);
    }

    pub fn lsn(&self) -> Position {
        self.lsn
    }

    pub fn flsn(&self) -> u64 {
        self.flushed_lsn.load(Relaxed)
    }

    pub fn seq(&self) -> u64 {
        self.seq
    }

    pub fn ckpt_cnt(&self) -> usize {
        self.ckpt_cnt
    }

    pub fn reset_ckpt_cnt(&mut self) {
        self.ckpt_cnt = 0;
    }

    pub fn last_ckpt(&self) -> Position {
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
        {
            self.writer.queue(u.to_slice());
            self.writer.queue(k);
            self.writer.queue(w);
            self.writer.queue(ov);
            self.writer.queue(nv);
            self.writer.flush();
        }
        self.advance(size);
    }

    pub fn record_update<T>(&mut self, ver: Ver, w: T, k: &[u8], ov: &[u8], nv: &[u8])
    where
        T: IWalCodec + IWalPayload,
    {
        let payload_size = w.encoded_len() + k.len() + ov.len() + nv.len();
        let Position { file_id, offset } = self.lsn();

        let u = WalUpdate {
            wal_type: EntryType::Update,
            sub_type: w.sub_type(),
            worker_id: self.desc.worker,
            size: payload_size as u32,
            cmd_id: ver.cmd,
            klen: k.len() as u32,
            txid: ver.txid,
            prev_id: file_id,
            prev_off: offset,
        };
        let total_sz = payload_size + u.encoded_len();
        if total_sz < self.ring.len() {
            let a = self.alloc(total_sz);
            let mut b = LogBuilder::new(a);
            b.add(u).add(k).add(w).add(ov).add(nv).build(self);
        } else {
            self.record_large(&u, k, w.to_slice(), ov, nv, total_sz);
        }
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
        let cur = self.meta.flush_data.load(Relaxed);

        if cur == self.last_data {
            return;
        }
        let last_ckpt = self.last_ckpt;

        log::trace!(
            "worker {} checkpoint {:?} curr {} last {}",
            self.desc.worker,
            last_ckpt,
            cur,
            self.last_data
        );
        self.last_data = cur;

        let ckpt = WalCheckpoint {
            wal_type: EntryType::CheckPoint,
        };

        // we must flush buffer in ring to make sure they are stabilized before flush checkpoint
        self.flush();
        self.writer.write(ckpt.to_slice());
        if self.opt.sync_on_write {
            self.writer.sync();
        }

        self.last_ckpt = self.log_pos;

        self.log_pos.offset += ckpt.encoded_len() as u64;
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

    fn sync_desc(&self) {
        let mut desc = self.desc.clone();
        desc.checkpoint = self.last_ckpt;
        desc.wal_id = self.log_pos.file_id;
        desc.sync(self.opt.desc_file(self.desc.worker));
    }

    fn flush(&mut self) {
        let len = self.ring.distance();
        if len != 0 {
            self.writer.write(self.ring.slice(self.ring.head(), len));
            self.ring.cons(len);

            if self.opt.sync_on_write {
                self.writer.sync();
            }

            self.flushed_lsn.store(self.seq, Relaxed);
        }
    }

    pub fn stabilize(&mut self) {
        self.flush();
        self.checkpoint()
    }
}
