use super::wal::{IWalCodec, IWalPayload, WalAbort, WalBegin, WalCheckpoint, WalCommit, WalUpdate};
use crate::types::data::Key;
use crate::utils::MutRef;
use crate::utils::block::Ring;
use crate::utils::data::Position;
use crate::utils::observe::{
    CounterMetric, HistogramMetric, LATENCY_SAMPLE_SHIFT, observe_elapsed, sampled_instant,
};
use crate::utils::options::ParsedOptions;
use crate::{OpCode, must_ok};
use crate::{cc::wal::EntryType, utils::data::GatherWriter};
use crc32c::Crc32cHasher;
use parking_lot::{Condvar, Mutex};
use std::cell::UnsafeCell;
use std::hash::Hasher;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::{Arc, atomic::Ordering::Relaxed};

pub struct LogBuilder<'a> {
    off: usize,
    buf: &'a mut [u8],
}

/// completion published once by the generation leader under `SyncStateCore.inner`
struct Completion {
    inner: UnsafeCell<CompletionInner>,
}

struct CompletionInner {
    result: Option<Result<(), OpCode>>,
    /// consumed by SyncTicket::sealed_cut under extra_check diagnostics
    #[cfg_attr(not(feature = "extra_check"), allow(dead_code))]
    cut: Position,
}

// safety: every access to `inner` happens under `SyncStateCore.inner` (write by
// the leader, read by waiters after the mutex is re-acquired on wake) or on the
// leader's own thread after publication; the Logging itself is already Sync
unsafe impl Sync for Completion {}

impl Completion {
    fn new() -> Self {
        Self {
            inner: UnsafeCell::new(CompletionInner {
                result: None,
                cut: Position::MIN,
            }),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SyncPhase {
    /// no generation in flight; the next register becomes a leader
    Open,
    /// a generation is registered; followers may join until the leader seals
    Syncing,
}

struct SyncStateInner {
    phase: SyncPhase,
    /// completion held by the leader and every participant
    current: Option<Arc<Completion>>,
    /// followers that joined the in-flight generation
    followers: usize,
}

struct SyncStateCore {
    inner: Mutex<SyncStateInner>,
    cv: Condvar,
    /// lock-free mirror for merge-window polling
    has_follower: AtomicBool,
}

impl SyncStateCore {
    fn new() -> Self {
        Self {
            inner: Mutex::new(SyncStateInner {
                phase: SyncPhase::Open,
                current: None,
                followers: 0,
            }),
            cv: Condvar::new(),
            has_follower: AtomicBool::new(false),
        }
    }
}

/// one participant in a sync generation
pub(crate) struct SyncTicket {
    is_leader: bool,
    target: Position,
    core: Option<Arc<SyncStateCore>>,
    completion: Option<Arc<Completion>>,
}

impl SyncTicket {
    fn completed(target: Position) -> Self {
        Self {
            is_leader: false,
            target,
            core: None,
            completion: None,
        }
    }

    /// whether registration found the target already durable
    #[inline]
    pub(crate) fn is_completed(&self) -> bool {
        self.completion.is_none()
    }

    #[inline]
    pub(crate) fn is_leader(&self) -> bool {
        self.is_leader
    }

    #[inline]
    pub(crate) fn target(&self) -> Position {
        self.target
    }

    /// sealed cut, available after completion
    #[cfg_attr(not(feature = "extra_check"), allow(dead_code))]
    pub(crate) fn sealed_cut(&self) -> Option<Position> {
        self.completion
            .as_ref()
            .map(|completion| unsafe { (*completion.inner.get()).cut })
    }

    /// whether a follower joined, read without taking the sync-state mutex
    pub(crate) fn has_followers(&self) -> bool {
        let Some(core) = &self.core else {
            return false;
        };
        core.has_follower.load(Relaxed)
    }

    /// wait without holding the shared logging mutex
    pub(crate) fn wait(&self) {
        let (Some(core), Some(completion)) = (&self.core, &self.completion) else {
            return;
        };
        let mut inner = core.inner.lock();
        // safety: the result is written under `core.inner` by the leader's
        // complete; the lock re-acquisition on wake is the happens-before edge
        while unsafe { (*completion.inner.get()).result.is_none() } {
            core.cv.wait(&mut inner);
        }
    }

    /// result after `wait`, or after leader completion
    pub(crate) fn check_result(&self) -> Result<(), OpCode> {
        match &self.completion {
            None => Ok(()),
            Some(completion) => {
                // safety: wait() synchronized with the leader's complete and the
                // leader observes its own publication; immutable after publish
                unsafe { (*completion.inner.get()).result }.unwrap_or(Err(OpCode::IoError))
            }
        }
    }
}

impl Drop for SyncTicket {
    fn drop(&mut self) {
        if self.is_leader {
            // unblock followers if the leader exits before publication
            let (Some(core), Some(completion)) = (&self.core, &self.completion) else {
                return;
            };
            let mut inner = core.inner.lock();
            let is_current = inner
                .current
                .as_ref()
                .is_some_and(|c| Arc::ptr_eq(c, completion));
            // safety: the write is under the sync state mutex; waiters
            // re-acquire it on wake, the leader's own thread is unwinding
            let unpublished = unsafe { (*completion.inner.get()).result.is_none() };
            if is_current && unpublished {
                unsafe {
                    *completion.inner.get() = CompletionInner {
                        result: Some(Err(OpCode::IoError)),
                        cut: Position::MIN,
                    };
                }
                inner.phase = SyncPhase::Open;
                inner.current = None;
                inner.followers = 0;
                core.has_follower.store(false, Relaxed);
                drop(inner);
                core.cv.notify_all();
            }
        }
    }
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
    /// physical WAL stream id
    pub wal_id: u8,
    oldest_wal_id: u64,
    ring: Ring,
    enable_ckpt: AtomicBool,
    /// save last checkpoint position, used by gc
    last_ckpt: Position,
    /// checkpoint floors indexed by logical group
    logical_checkpoint: Vec<Position>,
    /// log position after last durable checkpoint
    last_ckpt_log_pos: Position,
    /// last durable wal position
    durable_pos: Position,
    /// checkpoint counters indexed by logical group
    ckpt_cnts: Vec<Arc<AtomicUsize>>,
    /// wal activity not yet charged to each logical group's checkpoint counter
    logical_wal_dirty: Vec<bool>,
    /// used for building traverse chain (lsn)
    log_pos: Position,
    flushed_pos: Position,
    ops: usize,
    pub(crate) writer: MutRef<GatherWriter>,
    /// writers detached by rotation but not yet synced by a commit/barrier
    pending_writers: Vec<MutRef<GatherWriter>>,
    log_dir_dirty: bool,
    /// shared-stream generation state
    sync_state: Arc<SyncStateCore>,
    #[cfg(feature = "extra_check")]
    /// test fault hook: fail the next N generation sync attempts (0 = disabled)
    sync_fault: AtomicUsize,
    opt: Arc<ParsedOptions>,
}

unsafe impl Sync for Logging {}
unsafe impl Send for Logging {}

impl Logging {
    const AUTO_STABLE_SIZE: usize = 4 << 20;
    const AUTO_STABLE_OPS: u32 = <usize>::trailing_zeros(32);

    pub(crate) fn new(
        wal_id: u8,
        start_id: u64,
        oldest_id: u64,
        checkpoint: Position,
        opt: Arc<ParsedOptions>,
        ckpt_cnts: Vec<Arc<AtomicUsize>>,
    ) -> Self {
        let path = opt.physical_wal_path(wal_id, start_id);
        let log_dir_dirty = !must_ok!(opt.fs.try_exists(&path), "can't stat {:?}", path);
        let writer = GatherWriter::append(opt.fs.as_ref(), &path, 16);
        let pos = Position {
            file_id: start_id,
            offset: writer.pos(),
        };
        Self {
            oldest_wal_id: oldest_id.min(start_id),
            ring: Ring::new(opt.wal_buffer_size),
            enable_ckpt: AtomicBool::new(false),
            last_ckpt: checkpoint,
            logical_checkpoint: vec![checkpoint; opt.concurrent_write as usize],
            last_ckpt_log_pos: pos,
            durable_pos: pos,
            ckpt_cnts,
            logical_wal_dirty: vec![false; opt.concurrent_write as usize],
            log_pos: pos,
            flushed_pos: pos,
            ops: 0,
            wal_id,
            writer: MutRef::new(writer),
            pending_writers: Vec::new(),
            log_dir_dirty,
            sync_state: Arc::new(SyncStateCore::new()),
            #[cfg(feature = "extra_check")]
            sync_fault: AtomicUsize::new(0),
            opt,
        }
    }

    fn alloc<'a>(&mut self, size: usize) -> Result<&'a mut [u8], OpCode> {
        let tail = self.ring.tail();
        let rest = self.ring.len() - tail;

        if rest < size {
            self.flush_ring_to_writer();
            // skip the rest data, and restart from the begining
            self.ring.prod(rest);
            self.ring.cons(rest);
        } else if tail == 0 && self.ring.distance() > 0 {
            // the tail is extactly euqal to the boundary, we must flush pending data
            self.flush_ring_to_writer();
        }
        Ok(self.ring.prod(size))
    }

    fn advance(&mut self, data_len: usize) -> Result<(), OpCode> {
        // maybe switch wal file
        self.log_pos.offset += data_len as u64;
        if self.log_pos.offset >= self.opt.wal_file_size as u64 {
            self.log_pos.file_id += 1;
            self.log_pos.offset = 0;

            self.flush_ring_to_writer();
            if self.opt.sync_on_write {
                // rotation must not fsync by itself; the next commit/barrier
                // generation syncs every writer detached before its sealed cut
                self.pending_writers.push(self.writer.clone());
            }
            let path = self
                .opt
                .physical_wal_path(self.wal_id, self.log_pos.file_id);
            let created = !must_ok!(self.opt.fs.try_exists(&path), "can't stat {:?}", path);
            self.writer
                .reset(GatherWriter::append(self.opt.fs.as_ref(), &path, 16));
            self.log_dir_dirty |= created;
            // crash window: the new wal file exists but its directory entry is
            // not yet synced and the detached writer is not yet synced
            #[cfg(feature = "failpoints")]
            crate::utils::failpoint::crash("mace_wal_rotation_after_file_create");
        }

        self.ops = self.ops.wrapping_add(1);
        let flush_by_ops = self.ops.trailing_zeros() >= Self::AUTO_STABLE_OPS;
        let flush_by_size = self.ring.distance() >= Self::AUTO_STABLE_SIZE;
        if flush_by_ops || flush_by_size {
            self.flush_ring_to_writer();
        }

        Ok(())
    }

    pub fn enable_checkpoint(&self) {
        self.enable_ckpt.store(true, Relaxed);
    }

    pub fn current_pos(&self) -> Position {
        self.log_pos
    }

    /// recovery may truncate the open file before runtime appends
    pub(crate) fn rebase_positions_to_physical_eof(&mut self) {
        debug_assert!(self.ring.distance() == 0);
        debug_assert!(self.pending_writers.is_empty());
        let pos = Position {
            file_id: self.log_pos.file_id,
            offset: self.writer.pos(),
        };
        self.log_pos = pos;
        self.flushed_pos = pos;
        self.durable_pos = pos;
        self.last_ckpt_log_pos = pos;
    }

    #[cfg_attr(not(feature = "extra_check"), allow(dead_code))]
    pub(crate) fn durable_pos(&self) -> Position {
        self.durable_pos
    }

    /// shared stream floor = min over all logical groups
    pub(crate) fn checkpoint_floor(&self) -> Position {
        self.logical_checkpoint
            .iter()
            .copied()
            .min()
            .unwrap_or(self.last_ckpt)
    }

    /// replace boot-time floors with manifest evidence
    pub(crate) fn set_logical_checkpoint_floors(&mut self, floors: &[Position]) {
        self.logical_checkpoint.clear();
        self.logical_checkpoint.extend_from_slice(floors);
        self.logical_wal_dirty.fill(false);
        self.last_ckpt = self.checkpoint_floor();
    }

    /// deactivate all slots after wiping an era
    pub(crate) fn reset_logical_checkpoint_floors(&mut self) {
        self.logical_checkpoint.fill(Position::MAX);
        self.logical_wal_dirty.fill(false);
        self.last_ckpt = self.checkpoint_floor();
    }

    pub fn last_ckpt(&self) -> Position {
        self.last_ckpt
    }

    /// per-logical-group checkpoint floor slot
    pub(crate) fn logical_checkpoint_at(&self, logical_group: usize) -> Position {
        self.logical_checkpoint
            .get(logical_group)
            .copied()
            .unwrap_or(Position::MIN)
    }

    pub fn oldest_wal_id(&self) -> u64 {
        self.oldest_wal_id
    }

    pub fn advance_oldest_wal_id(&mut self, next: u64) {
        if next > self.oldest_wal_id {
            self.oldest_wal_id = next;
        }
    }

    pub fn update_checkpoint_for(&mut self, pos: Position, logical_group: usize) -> bool {
        if !self.enable_ckpt.load(Relaxed) {
            return false;
        }
        // MAX is an inactive floor, never a durable position
        if pos == Position::MAX {
            return false;
        }
        if let Some(floor) = self.logical_checkpoint.get_mut(logical_group) {
            *floor = (*floor).max(pos);
            self.last_ckpt = self.checkpoint_floor();
        } else {
            return false;
        }
        let wrote = self.checkpoint();
        if self.logical_wal_dirty[logical_group] {
            self.logical_wal_dirty[logical_group] = false;
            let counter = if self.ckpt_cnts.len() == 1 {
                &self.ckpt_cnts[0]
            } else {
                &self.ckpt_cnts[logical_group]
            };
            counter.fetch_add(1, Relaxed);
        }
        wrote
    }

    pub fn sync_checkpoint_barrier(&mut self) -> Result<(), OpCode> {
        // durable barriers use the generation protocol
        assert!(
            !self.opt.sync_on_write,
            "durable route must sync through the generation protocol"
        );
        if self.durable_pos >= self.log_pos && !self.log_dir_dirty {
            return Ok(());
        }

        self.flush_ring_to_writer();

        let sync_started = sampled_instant(
            self.log_pos.file_id ^ self.log_pos.offset,
            LATENCY_SAMPLE_SHIFT,
        );
        self.sync_writer_data_and_dir();
        self.durable_pos = self.flushed_pos;
        self.opt.observer.counter(CounterMetric::WalSync, 1);
        observe_elapsed(
            self.opt.observer.as_ref(),
            HistogramMetric::WalSyncMicros,
            sync_started,
        );
        Ok(())
    }

    #[cold]
    fn record_large(&mut self, u: &WalUpdate, k: &[u8], w: &[u8], nv: &[u8]) -> Result<(), OpCode> {
        let size = u.encoded_len() + k.len() + w.len() + nv.len();
        self.flush_ring_to_writer();
        {
            self.writer.queue(u.to_slice());
            self.writer.queue(k);
            self.writer.queue(w);
            self.writer.queue(nv);
            self.writer.flush();
        }
        self.advance(size)?;
        self.flushed_pos = self.log_pos;
        Ok(())
    }

    /// since multiple transaction may use same Logging, they must provide their own LSN
    pub fn record_update<T>(
        &mut self,
        logical_group: u8,
        k: &Key,
        w: T,
        nv: &[u8],
        prev_lsn: Position,
        bucket_id: u64,
    ) -> Result<Position, OpCode>
    where
        T: IWalCodec + IWalPayload,
    {
        let payload_size = w.encoded_len() + k.raw.len() + nv.len();
        let current_pos = self.current_pos();

        let mut u = WalUpdate {
            wal_type: EntryType::Update,
            sub_type: w.sub_type(),
            bucket_id,
            group_id: logical_group,
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
        h.write(nv);

        u.checksum = h.finish() as u32;

        let total_sz = payload_size + u.encoded_len();
        if total_sz < self.ring.len() {
            let a = self.alloc(total_sz)?;

            let mut b = LogBuilder::new(a);
            b.add(u).add(k.raw).add(w).add(nv).build(self)?;
        } else {
            self.record_large(&u, k.raw, w.to_slice(), nv)?;
        }
        // publish the floor only after the update append succeeds
        if let Some(slot) = self.logical_checkpoint.get_mut(logical_group as usize)
            && *slot == Position::MAX
        {
            *slot = prev_lsn;
            self.last_ckpt = self.checkpoint_floor();
        }
        self.mark_logical_wal_activity(logical_group as usize);
        self.opt.observer.counter(CounterMetric::WalAppend, 1);
        self.opt
            .observer
            .histogram(HistogramMetric::WalAppendBytes, total_sz as u64);
        Ok(current_pos)
    }

    fn add_entry<T: IWalCodec>(&mut self, w: T) -> Result<(), OpCode> {
        let size = w.encoded_len();
        let a = self.alloc(size)?;
        let mut b = LogBuilder::new(a);
        b.add(w).build(self)
    }

    fn mark_logical_wal_activity(&mut self, logical_group: usize) {
        self.logical_wal_dirty[logical_group] = true;
    }

    pub fn record_begin(&mut self, logical_group: usize, txid: u64) -> Result<Position, OpCode> {
        let pos = self.current_pos();
        let mut w = WalBegin {
            wal_type: EntryType::Begin,
            txid,
            checksum: 0,
        };
        w.checksum = w.calc_checksum();
        self.add_entry(w)?;
        self.mark_logical_wal_activity(logical_group);
        Ok(pos)
    }

    pub fn record_commit(&mut self, logical_group: usize, txid: u64) -> Result<(), OpCode> {
        let mut w = WalCommit {
            wal_type: EntryType::Commit,
            txid,
            checksum: 0,
        };
        w.checksum = w.calc_checksum();
        self.add_entry(w)?;
        self.mark_logical_wal_activity(logical_group);
        Ok(())
    }

    pub fn record_abort(&mut self, logical_group: usize, txid: u64) -> Result<(), OpCode> {
        let mut w = WalAbort {
            wal_type: EntryType::Abort,
            txid,
            checksum: 0,
        };
        w.checksum = w.calc_checksum();
        self.add_entry(w)?;
        self.mark_logical_wal_activity(logical_group);
        Ok(())
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
        self.flush_ring_to_writer();
        self.writer.write(ckpt.to_slice());
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash("mace_wal_after_checkpoint_write");

        self.log_pos.offset += ckpt.encoded_len() as u64;
        self.last_ckpt_log_pos = self.log_pos;
        self.flushed_pos = self.log_pos;
        true
    }

    #[inline]
    fn sync_writer_data_and_dir(&mut self) {
        self.writer.sync_data();
        must_ok!(self.sync_log_dir_if_dirty());
    }

    fn sync_log_dir_if_dirty(&mut self) -> Result<(), OpCode> {
        if self.log_dir_dirty {
            #[cfg(feature = "failpoints")]
            crate::utils::failpoint::crash("mace_wal_after_file_sync_before_dir_sync");
            self.opt
                .fs
                .as_ref()
                .sync_dir(&self.opt.log_root())
                .map_err(OpCode::from)?;
            self.log_dir_dirty = false;
        }
        Ok(())
    }

    /// sync rotated writers, current writer, then a newly-created directory entry
    fn try_sync_generation_files(&mut self) -> Result<(), OpCode> {
        #[cfg(feature = "extra_check")]
        {
            let prev = self
                .sync_fault
                .fetch_update(Relaxed, Relaxed, |n| (n > 0).then(|| n - 1));
            if let Ok(failing) = prev
                && failing > 0
            {
                return Err(OpCode::IoError);
            }
        }
        for w in self.pending_writers.iter_mut() {
            w.try_sync()?;
        }
        self.pending_writers.clear();
        self.writer.try_sync()?;
        self.sync_log_dir_if_dirty()?;
        Ok(())
    }

    #[cfg(feature = "extra_check")]
    pub(crate) fn fail_next_syncs(&self, n: usize) {
        self.sync_fault.store(n, Relaxed);
    }

    /// register a sync target into the shared-stream generation protocol; the
    /// caller must hold the logging mutex. durable-route only.
    pub(crate) fn register_sync(&mut self, target: Position) -> Result<SyncTicket, OpCode> {
        debug_assert!(
            self.opt.sync_on_write,
            "generation sync is a durable-route mechanism"
        );
        let mut inner = self.sync_state.inner.lock();
        if self.durable_pos >= target {
            // completed-ticket short-circuit: the cut covering target is already
            // durable; no generation, no leader, no wait, no sync count
            return Ok(SyncTicket::completed(target));
        }
        match inner.phase {
            SyncPhase::Syncing => {
                // the leader has registered but not sealed yet: seal..complete
                // hold the logging mutex we hold now, so joining here is always
                // pre-seal and the leader's cut will cover this target
                let completion = inner
                    .current
                    .clone()
                    .expect("syncing phase must hold a completion");
                inner.followers += 1;
                self.sync_state.has_follower.store(true, Relaxed);
                Ok(SyncTicket {
                    is_leader: false,
                    target,
                    core: Some(self.sync_state.clone()),
                    completion: Some(completion),
                })
            }
            SyncPhase::Open => {
                inner.phase = SyncPhase::Syncing;
                let completion = Arc::new(Completion::new());
                inner.current = Some(completion.clone());
                Ok(SyncTicket {
                    is_leader: true,
                    target,
                    core: Some(self.sync_state.clone()),
                    completion: Some(completion),
                })
            }
        }
    }

    /// force-barrier registration: flush the ring, then register the current
    /// stream position; the caller must hold the logging mutex.
    /// durable-route only.
    pub(crate) fn barrier_register(&mut self) -> Result<SyncTicket, OpCode> {
        debug_assert!(
            self.opt.sync_on_write,
            "generation barrier is a durable-route mechanism"
        );
        self.flush_ring_to_writer();
        let target = self.current_pos();
        self.register_sync(target)
    }

    /// generation leader: seal the flushed cut, sync every writer detached by
    /// rotation plus the current writer and the log directory, advance
    /// durable_pos to the sealed cut on success, then publish the completion
    /// and wake every waiter. the caller must hold the logging mutex, and this
    /// is the only place a generation's completion is published so no register
    /// can observe a sealed-but-uncompleted generation.
    pub(crate) fn leader_seal_sync_and_complete(&mut self, ticket: &SyncTicket) {
        debug_assert!(ticket.is_leader);
        self.flush_ring_to_writer();
        let cut = self.flushed_pos;
        {
            let inner = self.sync_state.inner.lock();
            debug_assert_eq!(inner.phase, SyncPhase::Syncing);
            // the lock discipline guarantees inner.current is this leader's
            // completion here: only complete clears it, only the leader
            // completes, and the orphan guard runs only after the leader
            // ticket drops
        }
        #[cfg(feature = "extra_check")]
        crate::testing::fire_wal_sync_point(crate::testing::WalSyncPoint::AfterSealBeforeFileSync);
        // crash window: the sealed cut is flushed but the generation files are
        // not yet synced; recovery must reinterpret the flushed tail
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash("mace_wal_generation_before_file_sync");
        // crash window: a torn tail record (garbage appended after the last
        // valid record by the test child); recovery must truncate at the first
        // malformed record start and discard everything after it
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash("mace_wal_tail_corrupt");

        let sync_started = sampled_instant(cut.file_id ^ cut.offset, LATENCY_SAMPLE_SHIFT);
        let result = self.try_sync_generation_files();
        observe_elapsed(
            self.opt.observer.as_ref(),
            HistogramMetric::WalSyncMicros,
            sync_started,
        );
        self.opt.observer.counter(CounterMetric::WalSync, 1);
        if result.is_ok() {
            self.durable_pos = cut;
        }
        // crash window: the generation files are synced and durable_pos reached
        // the sealed cut, but the completion is not yet published, so no
        // participant published a fact; recovery must reconstruct the outcomes
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash("mace_wal_generation_after_file_sync_before_complete");

        let mut inner = self.sync_state.inner.lock();
        let followers = inner.followers;
        // the lock discipline makes this unconditional publish sound: the
        // leader holds the logging mutex, phase == Syncing means inner.current
        // is the completion this leader installed at register (only complete
        // clears it, and only the leader completes; the orphan guard runs only
        // after the leader ticket drops, so a completing leader's completion
        // is always current). per-generation isolation comes from each
        // generation installing its own completion Arc at register, not from
        // any generation counter
        if let Some(completion) = inner.current.as_ref() {
            // safety: the completion result is guarded by the sync state mutex
            unsafe {
                *completion.inner.get() = CompletionInner {
                    result: Some(result),
                    cut,
                };
            }
        }
        inner.phase = SyncPhase::Open;
        inner.current = None;
        inner.followers = 0;
        self.sync_state.has_follower.store(false, Relaxed);
        drop(inner);

        self.opt.observer.counter(CounterMetric::WalGeneration, 1);
        self.opt
            .observer
            .counter(CounterMetric::WalGenerationLeader, 1);
        if followers > 0 {
            self.opt
                .observer
                .counter(CounterMetric::WalGenerationFollower, followers as u64);
        }
        self.opt
            .observer
            .histogram(HistogramMetric::WalGenerationBatch, (followers + 1) as u64);
        if result.is_err() {
            self.opt
                .observer
                .counter(CounterMetric::WalGenerationError, 1);
        }
        self.sync_state.cv.notify_all();
        #[cfg(feature = "extra_check")]
        crate::testing::fire_wal_sync_point(crate::testing::WalSyncPoint::AfterGenerationComplete);
    }

    #[inline]
    fn flush_ring_to_writer(&mut self) {
        let len = self.ring.distance();
        if len != 0 {
            self.writer.write(self.ring.slice(self.ring.head(), len));
            self.ring.cons(len);

            self.flushed_pos = self.log_pos;
        }
    }

    pub fn sync(&mut self, force: bool) -> Result<(), OpCode> {
        // durable route must sync through the generation protocol; a direct
        // sync here would bypass B-SYNC-4's flush-only non-generation rule
        assert!(
            !self.opt.sync_on_write,
            "durable route must sync through the generation protocol"
        );
        self.flush_ring_to_writer();
        if force {
            let sync_started = sampled_instant(
                self.log_pos.file_id ^ self.log_pos.offset,
                LATENCY_SAMPLE_SHIFT,
            );
            self.sync_writer_data_and_dir();
            self.durable_pos = self.flushed_pos;
            self.opt.observer.counter(CounterMetric::WalSync, 1);
            observe_elapsed(
                self.opt.observer.as_ref(),
                HistogramMetric::WalSyncMicros,
                sync_started,
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::panic::AssertUnwindSafe;

    use super::Logging;
    #[cfg(feature = "extra_check")]
    use crate::OpCode;
    use crate::utils::options::Options;
    use crate::{
        RandomPath,
        io::testfs::{InjectOp, InjectedFileSystem},
        utils::data::Position,
    };
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

    fn new_logging() -> (RandomPath, Logging) {
        let root = RandomPath::tmp();
        let mut opt = Options::new(&*root);
        opt.concurrent_write = 1;
        let parsed = Arc::new(opt.validate().expect("log options must validate"));
        let logging = Logging::new(
            0,
            0,
            0,
            Position::MIN,
            parsed,
            vec![Arc::new(AtomicUsize::new(0))],
        );
        (root, logging)
    }

    /// durable-route logging for the sync generation protocol tests
    fn new_sync_logging() -> (RandomPath, Logging) {
        let root = RandomPath::tmp();
        let mut opt = Options::new(&*root);
        opt.concurrent_write = 1;
        opt.sync_on_write = true;
        let parsed = Arc::new(opt.validate().expect("log options must validate"));
        let logging = Logging::new(
            0,
            0,
            0,
            Position::MIN,
            parsed,
            vec![Arc::new(AtomicUsize::new(0))],
        );
        (root, logging)
    }

    #[test]
    fn generation_sync_advances_durable_pos_to_sealed_cut_and_short_circuits() {
        let (_root, mut logging) = new_sync_logging();
        logging
            .record_commit(0, 1)
            .expect("commit record must append");
        let target = logging.current_pos();

        let ticket = logging.register_sync(target).expect("register must work");
        assert!(ticket.is_leader(), "first uncovered register must lead");
        logging.leader_seal_sync_and_complete(&ticket);
        ticket.check_result().expect("generation must succeed");
        assert_eq!(
            logging.durable_pos(),
            target,
            "durable_pos must reach the sealed cut"
        );

        // a target already covered by durable_pos must short-circuit without a
        // new generation, a leader or a wait
        let ticket = logging.register_sync(target).expect("register must work");
        assert!(ticket.is_completed(), "covered target must short-circuit");
        assert_eq!(ticket.check_result(), Ok(()));
    }

    #[test]
    fn generation_follower_joins_and_receives_the_leader_result() {
        let (_root, mut logging) = new_sync_logging();
        logging
            .record_commit(0, 1)
            .expect("commit record must append");
        let leader_target = logging.current_pos();
        let leader = logging
            .register_sync(leader_target)
            .expect("leader register must work");

        logging
            .record_commit(0, 2)
            .expect("commit record must append");
        let follower_target = logging.current_pos();
        let follower = logging
            .register_sync(follower_target)
            .expect("follower register must work");
        assert!(
            !follower.is_leader(),
            "second register must join as follower"
        );

        logging.leader_seal_sync_and_complete(&leader);
        leader.wait();
        follower.wait();
        leader.check_result().expect("leader result must be ok");
        follower.check_result().expect("follower result must be ok");
        assert!(
            logging.durable_pos() >= follower_target,
            "the sealed cut must cover every registered target"
        );
    }

    #[cfg(feature = "extra_check")]
    #[test]
    fn generation_sync_failure_broadcasts_and_isolates_next_generation() {
        let (_root, mut logging) = new_sync_logging();
        logging
            .record_commit(0, 1)
            .expect("commit record must append");
        let target = logging.current_pos();
        let ticket = logging.register_sync(target).expect("register must work");

        logging.fail_next_syncs(1);
        logging.leader_seal_sync_and_complete(&ticket);
        ticket.wait();
        assert_eq!(
            ticket.check_result(),
            Err(OpCode::IoError),
            "the leader must observe the injected sync failure"
        );
        assert_ne!(
            logging.durable_pos(),
            target,
            "a failed generation must not advance durable_pos"
        );

        // the next generation must not inherit the previous error
        logging
            .record_commit(0, 2)
            .expect("commit record must append");
        let target2 = logging.current_pos();
        let ticket2 = logging.register_sync(target2).expect("register must work");
        logging.leader_seal_sync_and_complete(&ticket2);
        ticket2.wait();
        assert_eq!(ticket2.check_result(), Ok(()));
        assert!(logging.durable_pos() >= target2);
    }

    #[test]
    fn orphaned_leader_generation_is_reset_on_ticket_drop() {
        let (_root, mut logging) = new_sync_logging();
        logging
            .record_commit(0, 1)
            .expect("commit record must append");
        let target = logging.current_pos();

        // a leader ticket dropped without driving the generation (panic or a
        // caller regression) must not leave the sync state stuck in Syncing
        let ticket = logging.register_sync(target).expect("register must work");
        assert!(ticket.is_leader(), "first uncovered register must lead");
        drop(ticket);

        // the next register must start a fresh generation instead of joining
        // the orphaned one, and the round must complete normally
        logging
            .record_commit(0, 2)
            .expect("commit record must append");
        let target2 = logging.current_pos();
        let ticket2 = logging.register_sync(target2).expect("register must work");
        assert!(
            ticket2.is_leader(),
            "after an orphaned generation is reset the next register must lead"
        );
        logging.leader_seal_sync_and_complete(&ticket2);
        ticket2.wait();
        assert_eq!(ticket2.check_result(), Ok(()));
        assert!(
            logging.durable_pos() >= target2,
            "the fresh generation must advance durable_pos past the orphaned target"
        );
    }

    #[test]
    fn update_checkpoint_does_not_advance_before_checkpoint_is_enabled() {
        let (_root, mut logging) = new_logging();
        let newer = Position {
            file_id: 7,
            offset: 33,
        };

        assert!(!logging.update_checkpoint_for(newer, 0));
        assert_eq!(logging.last_ckpt(), Position::MIN);
    }

    #[test]
    fn update_checkpoint_advances_after_checkpoint_is_enabled() {
        let (_root, mut logging) = new_logging();
        let newer = Position {
            file_id: 7,
            offset: 33,
        };

        logging.enable_checkpoint();
        assert!(!logging.update_checkpoint_for(newer, 0));
        assert_eq!(logging.last_ckpt(), newer);
        assert_eq!(logging.ckpt_cnts[0].load(Relaxed), 0);
    }

    #[test]
    fn shared_checkpoint_ages_only_logical_groups_with_wal_activity() {
        let root = RandomPath::tmp();
        let mut opt = Options::new(&*root);
        opt.concurrent_write = 2;
        opt.sync_on_write = true;
        let parsed = Arc::new(opt.validate().expect("log options must validate"));
        let counters = vec![Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0))];
        let mut logging = Logging::new(
            Options::SHARED_ID,
            0,
            0,
            Position::MIN,
            parsed,
            counters.clone(),
        );
        logging.enable_checkpoint();

        logging
            .record_commit(0, 1)
            .expect("group 0 record must append");
        assert!(logging.update_checkpoint_for(Position::new(1, 0), 0));
        assert!(!logging.update_checkpoint_for(Position::new(1, 0), 1));
        assert_eq!(counters[0].load(Relaxed), 1);
        assert_eq!(counters[1].load(Relaxed), 0);

        logging
            .record_begin(1, 2)
            .expect("group 1 record must append");
        assert!(logging.update_checkpoint_for(Position::new(2, 0), 0));
        assert!(!logging.update_checkpoint_for(Position::new(2, 0), 1));
        assert_eq!(counters[0].load(Relaxed), 1);
        assert_eq!(counters[1].load(Relaxed), 1);
    }

    #[test]
    fn wal_rotate_open_failure_panics() {
        let root = RandomPath::tmp();
        let mut opt = Options::new(&*root);
        opt.concurrent_write = 1;
        opt.wal_file_size = 1;
        let next_path = opt.wal_file(0, 1);
        let fs = Arc::new(InjectedFileSystem::new());
        fs.fail_once(
            InjectOp::Open,
            next_path,
            std::io::ErrorKind::PermissionDenied,
        );
        opt.fs = fs;
        let parsed = Arc::new(opt.validate().expect("log options must validate"));
        let mut logging = Logging::new(
            0,
            0,
            0,
            Position::MIN,
            parsed,
            vec![Arc::new(AtomicUsize::new(0))],
        );

        let res = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let _ = logging.advance(1);
        }));
        assert!(res.is_err(), "wal rotate open failure must panic");
    }
}
