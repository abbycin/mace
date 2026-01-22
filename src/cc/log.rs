use super::wal::{IWalCodec, IWalPayload, WalAbort, WalBegin, WalCheckpoint, WalCommit, WalUpdate};
use crate::meta::Numerics;
use crate::types::data::Ver;
use crate::utils::block::Ring;
use crate::utils::data::Position;
use crate::utils::{data::WalDescHandle, options::ParsedOptions};
use crate::{cc::wal::EntryType, utils::data::GatherWriter};
use crc32c::Crc32cHasher;
use std::hash::Hasher;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, atomic::Ordering::Relaxed};

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
    pub group: u8,
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
    flushed_pos: Position,
    ops: usize,
    last_data: u64,
    writer: GatherWriter,
    opt: Arc<ParsedOptions>,
    pub desc: WalDescHandle,
    numerics: Arc<Numerics>,
    #[cfg(feature = "extra_check")]
    last_id: u64,
}

unsafe impl Sync for Logging {}
unsafe impl Send for Logging {}

impl Logging {
    const AUTO_STABLE_SIZE: usize = 4 << 20;
    const AUTO_STABLE_OPS: u32 = <usize>::trailing_zeros(32);

    pub(crate) fn new(
        desc: WalDescHandle,
        numerics: Arc<Numerics>,
        opt: Arc<ParsedOptions>,
    ) -> Self {
        let (group, last_id, ckpt_pos) = {
            let d = desc.lock();
            (d.group, d.latest_id, d.checkpoint)
        };
        let writer = GatherWriter::append(&opt.wal_file(group, last_id), 16);
        let pos = Position {
            file_id: last_id,
            offset: writer.pos(),
        };
        Self {
            ring: Ring::new(opt.wal_buffer_size),
            enable_ckpt: AtomicBool::new(false),
            last_ckpt: ckpt_pos,
            ckpt_cnt: 0,
            log_pos: pos,
            lsn: pos,
            seq: 0,
            flushed_lsn: AtomicU64::new(0),
            flushed_pos: pos,
            ops: 0,
            last_data: numerics.signal.load(Relaxed),
            group,
            writer,
            opt,
            desc,
            numerics,
            #[cfg(feature = "extra_check")]
            last_id: 0,
        }
    }

    fn alloc<'a>(&mut self, size: usize) -> &'a mut [u8] {
        let tail = self.ring.tail();
        let rest = self.ring.len() - tail;

        if rest < size {
            self.flush();
            // skip the rest data, and restart from the begining
            self.ring.prod(rest);
            self.ring.cons(rest);
        } else if tail == 0 && self.ring.distance() > 0 {
            // the tail is extactly euqal to the boundary, we must flush pending data
            self.flush();
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
                .reset(&self.opt.wal_file(self.group, self.log_pos.file_id));
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

    pub fn current_pos(&self) -> Position {
        self.log_pos
    }

    pub fn flushed_pos(&self) -> Position {
        self.flushed_pos
    }

    pub fn ckpt_cnt(&self) -> usize {
        self.ckpt_cnt
    }

    pub fn last_ckpt(&self) -> Position {
        self.last_ckpt
    }

    pub fn update_last_ckpt(&mut self, pos: Position) {
        if pos.file_id > self.last_ckpt.file_id
            || (pos.file_id == self.last_ckpt.file_id && pos.offset > self.last_ckpt.offset)
        {
            self.last_ckpt = pos;
        }
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

    /// since multiple transaction may use same Logging, they must provide their own LSN
    pub fn record_update<T>(
        &mut self,
        ver: Ver,
        w: T,
        k: &[u8],
        ov: &[u8],
        nv: &[u8],
        prev_lsn: Position,
    ) -> Position
    where
        T: IWalCodec + IWalPayload,
    {
        let payload_size = w.encoded_len() + k.len() + ov.len() + nv.len();
        let current_pos = self.current_pos();

        let mut u = WalUpdate {
            wal_type: EntryType::Update,
            sub_type: w.sub_type(),
            group_id: self.group,
            size: payload_size as u32,
            cmd_id: ver.cmd,
            klen: k.len() as u32,
            txid: ver.txid,
            prev_id: prev_lsn.file_id,
            prev_off: prev_lsn.offset,
            checksum: 0,
        };

        let mut h = Crc32cHasher::default();
        let header_size = u.encoded_len();
        let checksum_offset = header_size - size_of_val(&{ u.checksum });

        let header_ptr = &u as *const WalUpdate as *const u8;
        unsafe {
            let header_slice = std::slice::from_raw_parts(header_ptr, checksum_offset);
            h.write(header_slice);
        }

        h.write(k);
        h.write(w.to_slice());
        h.write(ov);
        h.write(nv);

        u.checksum = h.finish() as u32;

        let total_sz = payload_size + u.encoded_len();
        if total_sz < self.ring.len() {
            let a = self.alloc(total_sz);
            let mut b = LogBuilder::new(a);
            b.add(u).add(k).add(w).add(ov).add(nv).build(self);
        } else {
            self.record_large(&u, k, w.to_slice(), ov, nv, total_sz);
        }
        current_pos
    }

    fn add_entry<T: IWalCodec>(&mut self, w: T) {
        let size = w.encoded_len();
        let a = self.alloc(size);
        let mut b = LogBuilder::new(a);
        b.add(w).build(self);
    }

    pub fn record_begin(&mut self, txid: u64) -> Position {
        let pos = self.current_pos();
        let mut w = WalBegin {
            wal_type: EntryType::Begin,
            txid,
            checksum: 0,
        };
        w.checksum = w.calc_checksum();
        self.add_entry(w);
        pos
    }

    pub fn record_commit(&mut self, txid: u64) {
        #[cfg(feature = "extra_check")]
        {
            self.last_id = txid;
        }
        let mut w = WalCommit {
            wal_type: EntryType::Commit,
            txid,
            checksum: 0,
        };
        w.checksum = w.calc_checksum();
        self.add_entry(w);
    }

    pub fn record_abort(&mut self, txid: u64) {
        let mut w = WalAbort {
            wal_type: EntryType::Abort,
            txid,
            checksum: 0,
        };
        w.checksum = w.calc_checksum();
        self.add_entry(w);
    }

    fn checkpoint(&mut self) {
        if !self.enable_ckpt.load(Relaxed) {
            return;
        }
        let cur = self.numerics.signal.load(Relaxed);

        if cur == self.last_data {
            return;
        }
        let last_ckpt = self.last_ckpt;

        log::trace!(
            "group {} checkpoint {:?} curr {} last {}",
            self.group,
            last_ckpt,
            cur,
            self.last_data
        );
        self.last_data = cur;

        let mut ckpt = WalCheckpoint {
            wal_type: EntryType::CheckPoint,
            checksum: 0,
        };
        ckpt.checksum = ckpt.calc_checksum();

        // we must flush buffer in ring to make sure they are stabilized before flush checkpoint
        self.flush();
        self.writer.write(ckpt.to_slice());
        if self.opt.sync_on_write {
            self.writer.sync();
        }

        self.log_pos.offset += ckpt.encoded_len() as u64;
        self.ckpt_cnt += 1;
        self.sync_desc();
    }

    fn sync_desc(&self) {
        let mut desc = self.desc.lock();
        desc.update_ckpt(
            self.opt.desc_file(self.group),
            self.last_ckpt,
            self.log_pos.file_id,
        );
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
            self.flushed_pos = self.log_pos;
            self.numerics.log_size.fetch_add(len, Relaxed);
        }
    }

    pub fn stabilize(&mut self) {
        self.flush();
        self.checkpoint()
    }
}
