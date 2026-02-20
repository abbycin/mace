use super::wal::{IWalCodec, IWalPayload, WalAbort, WalBegin, WalCheckpoint, WalCommit, WalUpdate};
use crate::OpCode;
use crate::meta::Numerics;
use crate::types::data::Key;
use crate::utils::MutRef;
use crate::utils::block::Ring;
use crate::utils::data::Position;
use crate::utils::options::ParsedOptions;
use crate::{cc::wal::EntryType, utils::data::GatherWriter};
use crc32c::Crc32cHasher;
use std::hash::Hasher;
use std::sync::atomic::{AtomicBool, AtomicUsize};
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

    pub fn build(&self, log: &mut Logging) -> Result<(), OpCode> {
        log.advance(self.buf.len())
    }
}

pub struct Logging {
    pub group: u8,
    oldest_wal_id: u64,
    ring: Ring,
    enable_ckpt: AtomicBool,
    /// save last checkpoint position, used by gc
    last_ckpt: Position,
    /// log position after last durable checkpoint
    last_ckpt_log_pos: Position,
    /// last durable wal position
    durable_pos: Position,
    ckpt_cnt: Arc<AtomicUsize>,
    /// used for building traverse chain (lsn)
    log_pos: Position,
    flushed_pos: Position,
    ops: usize,
    pub(crate) writer: MutRef<GatherWriter>,
    opt: Arc<ParsedOptions>,
    numerics: Arc<Numerics>,
}

unsafe impl Sync for Logging {}
unsafe impl Send for Logging {}

impl Logging {
    const AUTO_STABLE_SIZE: usize = 4 << 20;
    const AUTO_STABLE_OPS: u32 = <usize>::trailing_zeros(32);

    pub(crate) fn new(
        group: u8,
        latest_id: u64,
        oldest_id: u64,
        checkpoint: Position,
        numerics: Arc<Numerics>,
        opt: Arc<ParsedOptions>,
        ckpt_cnt: Arc<AtomicUsize>,
    ) -> Self {
        let writer = GatherWriter::append(&opt.wal_file(group, latest_id), 16);
        let pos = Position {
            file_id: latest_id,
            offset: writer.pos(),
        };
        Self {
            oldest_wal_id: oldest_id.min(latest_id),
            ring: Ring::new(opt.wal_buffer_size),
            enable_ckpt: AtomicBool::new(false),
            last_ckpt: checkpoint,
            last_ckpt_log_pos: pos,
            durable_pos: pos,
            ckpt_cnt,
            log_pos: pos,
            flushed_pos: pos,
            ops: 0,
            group,
            writer: MutRef::new(writer),
            opt,
            numerics,
        }
    }

    fn alloc<'a>(&mut self, size: usize) -> Result<&'a mut [u8], OpCode> {
        let tail = self.ring.tail();
        let rest = self.ring.len() - tail;

        if rest < size {
            self.sync(false)?;
            // skip the rest data, and restart from the begining
            self.ring.prod(rest);
            self.ring.cons(rest);
        } else if tail == 0 && self.ring.distance() > 0 {
            // the tail is extactly euqal to the boundary, we must flush pending data
            self.sync(false)?;
        }
        Ok(self.ring.prod(size))
    }

    fn advance(&mut self, data_len: usize) -> Result<(), OpCode> {
        // maybe switch wal file
        self.log_pos.offset += data_len as u64;
        if self.log_pos.offset >= self.opt.wal_file_size as u64 {
            self.log_pos.file_id += 1;
            self.log_pos.offset = 0;

            self.sync(false)?;
            self.writer.reset(GatherWriter::append(
                &self.opt.wal_file(self.group, self.log_pos.file_id),
                16,
            ));
        }

        self.ops = self.ops.wrapping_add(1);
        let flush_by_ops = self.ops.trailing_zeros() >= Self::AUTO_STABLE_OPS;
        let flush_by_size = self.ring.distance() >= Self::AUTO_STABLE_SIZE;
        if flush_by_ops || flush_by_size {
            self.sync(false)?;
        }

        Ok(())
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

    pub fn last_ckpt(&self) -> Position {
        self.last_ckpt
    }

    pub fn oldest_wal_id(&self) -> u64 {
        self.oldest_wal_id
    }

    pub fn advance_oldest_wal_id(&mut self, next: u64) {
        if next > self.oldest_wal_id {
            self.oldest_wal_id = next;
        }
    }

    pub fn update_checkpoint(&mut self, pos: Position) -> bool {
        if pos > self.last_ckpt {
            self.last_ckpt = pos;
        }
        self.checkpoint()
    }

    pub fn should_durable(&mut self, target: Position) -> bool {
        target > self.durable_pos
    }

    #[cold]
    fn record_large(
        &mut self,
        u: &WalUpdate,
        k: &[u8],
        w: &[u8],
        ov: &[u8],
        nv: &[u8],
    ) -> Result<(), OpCode> {
        let size = u.encoded_len() + k.len() + w.len() + ov.len() + nv.len();
        self.sync(false)?; // flush queued first, make sure log record is sequentail
        {
            self.writer.queue(u.to_slice());
            self.writer.queue(k);
            self.writer.queue(w);
            self.writer.queue(ov);
            self.writer.queue(nv);
            self.writer.flush();
            if self.opt.sync_on_write {
                self.writer.sync();
            }
        }
        self.advance(size)?;
        self.flushed_pos = self.log_pos;
        if self.opt.sync_on_write {
            self.durable_pos = self.flushed_pos;
        }
        self.numerics.log_size.fetch_add(size, Relaxed);
        Ok(())
    }

    /// since multiple transaction may use same Logging, they must provide their own LSN
    pub fn record_update<T>(
        &mut self,
        k: &Key,
        w: T,
        ov: &[u8],
        nv: &[u8],
        prev_lsn: Position,
        bucket_id: u64,
    ) -> Result<Position, OpCode>
    where
        T: IWalCodec + IWalPayload,
    {
        let payload_size = w.encoded_len() + k.raw.len() + ov.len() + nv.len();
        let current_pos = self.current_pos();

        let mut u = WalUpdate {
            wal_type: EntryType::Update,
            sub_type: w.sub_type(),
            bucket_id,
            group_id: self.group,
            size: payload_size as u32,
            cmd_id: k.ver.cmd,
            klen: k.raw.len() as u32,
            txid: k.ver.txid,
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

        h.write(k.raw);
        h.write(w.to_slice());
        h.write(ov);
        h.write(nv);

        u.checksum = h.finish() as u32;

        let total_sz = payload_size + u.encoded_len();
        if total_sz < self.ring.len() {
            let a = self.alloc(total_sz)?;

            let mut b = LogBuilder::new(a);
            b.add(u).add(k.raw).add(w).add(ov).add(nv).build(self)?;
        } else {
            self.record_large(&u, k.raw, w.to_slice(), ov, nv)?;
        }
        Ok(current_pos)
    }

    fn add_entry<T: IWalCodec>(&mut self, w: T) -> Result<(), OpCode> {
        let size = w.encoded_len();
        let a = self.alloc(size)?;
        let mut b = LogBuilder::new(a);
        b.add(w).build(self)
    }

    pub fn record_begin(&mut self, txid: u64) -> Result<Position, OpCode> {
        let pos = self.current_pos();
        let mut w = WalBegin {
            wal_type: EntryType::Begin,
            txid,
            checksum: 0,
        };
        w.checksum = w.calc_checksum();
        self.add_entry(w)?;
        Ok(pos)
    }

    pub fn record_commit(&mut self, txid: u64) -> Result<(), OpCode> {
        let mut w = WalCommit {
            wal_type: EntryType::Commit,
            txid,
            checksum: 0,
        };
        w.checksum = w.calc_checksum();
        self.add_entry(w)
    }

    pub fn record_abort(&mut self, txid: u64) -> Result<(), OpCode> {
        let mut w = WalAbort {
            wal_type: EntryType::Abort,
            txid,
            checksum: 0,
        };
        w.checksum = w.calc_checksum();
        self.add_entry(w)
    }

    fn checkpoint(&mut self) -> bool {
        if !self.enable_ckpt.load(Relaxed) {
            return false;
        }
        if self.log_pos <= self.last_ckpt_log_pos {
            return false;
        }

        let mut ckpt = WalCheckpoint {
            wal_type: EntryType::CheckPoint,
            checkpoint: self.last_ckpt,
            checksum: 0,
        };
        ckpt.checksum = ckpt.calc_checksum();

        // we must flush buffer in ring to make sure they are stabilized before flush checkpoint
        if let Err(e) = self.sync(false) {
            log::warn!("checkpoint fail, {:?}", e);
            return false;
        }
        self.writer.write(ckpt.to_slice());
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash("mace_wal_after_checkpoint_write");

        self.log_pos.offset += ckpt.encoded_len() as u64;
        self.last_ckpt_log_pos = self.log_pos;
        self.ckpt_cnt.fetch_add(1, Relaxed);
        self.flushed_pos = self.log_pos;
        self.durable_pos = self.flushed_pos;
        self.numerics
            .log_size
            .fetch_add(ckpt.encoded_len(), Relaxed);
        true
    }

    pub fn sync(&mut self, force: bool) -> Result<(), OpCode> {
        let len = self.ring.distance();
        if len != 0 {
            self.writer.write(self.ring.slice(self.ring.head(), len));
            self.ring.cons(len);

            self.flushed_pos = self.log_pos;
            self.numerics.log_size.fetch_add(len, Relaxed);
        }
        if force || self.opt.sync_on_write {
            self.writer.sync();
            self.durable_pos = self.flushed_pos;
        }
        Ok(())
    }
}
