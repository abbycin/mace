use crc32c::Crc32cHasher;
use io::{File, GatherIO};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::Ordering::Relaxed;
use std::{hash::Hasher, path::PathBuf, sync::Arc};

use crate::map::table::Swip;
use crate::meta::entry::{
    DelInterval, DelIntervalStartHdr, DeleteHdr, GenericHdr, PageTableHdr, StatHdr, TxnKind,
    get_record_size,
};
use crate::meta::{Begin, Commit, Delete, FileId, FileStat, IntervalPair, Stat};
use crate::types::traits::IAsSlice;
use crate::utils::ROOT_PID;
use crate::utils::bitmap::BitMap;
use crate::{
    OpCode,
    meta::{
        IMetaCodec, Manifest,
        entry::{MetaKind, Numerics, PageTable},
    },
    utils::{NULL_ADDR, block::Block, options::ParsedOptions},
};

struct RecordHandle {
    offset: u64,
    size: usize,
}

impl RecordHandle {
    const fn new(off: u64, size: usize) -> Self {
        Self { offset: off, size }
    }
}

pub(crate) struct ManifestBuilder {
    inner: Manifest,
    // order is matter, so use btreemap
    redo_table: BTreeMap<u64, Vec<RecordHandle>>,
    undo_table: HashSet<u64>,
    checksum: HashMap<u64, Crc32cHasher>,
    /// txid -> file_id
    data_file: HashMap<u64, u64>,
    max_txid: u64,
    max_flush_id: u64,
    table: PageTable,
    buffer: Block,
}

impl ManifestBuilder {
    pub(crate) fn new(opt: Arc<ParsedOptions>) -> Self {
        Self {
            inner: Manifest::new(opt),
            redo_table: BTreeMap::new(),
            undo_table: HashSet::new(),
            checksum: HashMap::new(),
            data_file: HashMap::new(),
            max_txid: 0,
            max_flush_id: 0,
            table: PageTable::default(),
            buffer: Block::alloc(1 << 20),
        }
    }

    /// we assume data file was flushed before transaction commit, when transaction was committed,
    /// if the transaction is complete, the data file is complete too, or else simply rollback last
    /// imcomplete (or corrupted) transaction and remove the recorded data file
    ///
    /// currently, only two concurrent transactions are possible one is created by flush thread and
    /// another is created by gc thread, we assume there're at most two corrupted transactions or else
    /// refuse to work
    pub(crate) fn add(&mut self, path: PathBuf, is_last: bool) -> Result<(), OpCode> {
        let f = File::options()
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|e| {
                log::error!("open file fail: {e:?}");
                OpCode::IoError
            })?;

        match self.analyze(&f) {
            Err(OpCode::BadData) => return Err(OpCode::BadData),
            Err(_) if !is_last => {
                // non-last manifest is imcomplete, refuse to work
                return Err(OpCode::BadData);
            }
            Ok(txid) => {
                if self.undo_table.len() > 2 {
                    return Err(OpCode::BadData);
                }
                self.max_txid = self.max_txid.max(txid);
            }
            _ => {}
        }
        self.redo(&f);
        Ok(())
    }

    pub(crate) fn finish(mut self, snap: Option<u64>) -> Manifest {
        let mut next = ROOT_PID;
        for (&pid, &addr) in self.table.iter() {
            next = next.max(pid + 1);
            if addr == NULL_ADDR {
                self.inner.map.insert_free(pid);
            } else {
                self.inner
                    .map
                    .index(pid)
                    .fetch_max(Swip::tagged(addr), Relaxed);
            }
        }
        self.inner.map.set_next(next);

        self.inner.delete_files();
        if let Some(snap_id) = snap {
            self.inner.unlink_old(snap_id);
        }

        for item in self.inner.stat_ctx.iter() {
            let v = item.value();
            self.inner
                .stat_ctx
                .update_size(v.active_size as u64, v.total_size as u64);
        }

        // append to old file
        self.inner.init(self.max_flush_id, self.max_txid);

        self.inner
    }

    fn calc_crc(
        buffer: &mut Block,
        f: &File,
        h: &mut Crc32cHasher,
        pos: u64,
        size: usize,
    ) -> Result<(), OpCode> {
        if size > buffer.len() {
            buffer.realloc(size);
        }
        let data = buffer.mut_slice(0, size);
        let nbytes = f.read(data, pos).unwrap();
        if nbytes < data.len() {
            return Err(OpCode::NeedMore);
        }
        h.write(data);
        Ok(())
    }

    fn analyze(&mut self, f: &File) -> Result<u64, OpCode> {
        let end = f.size().unwrap();
        let mut pos = 0;
        let mut max_txid = u64::MIN;

        while pos < end {
            let hdr_s = self.buffer.mut_slice(0, GenericHdr::SIZE);
            let n = f.read(hdr_s, pos).map_err(|e| {
                log::error!("read error {e:?}");
                OpCode::IoError
            })?;

            if n < GenericHdr::SIZE {
                let _ = f.truncate(pos);
                return Err(OpCode::NeedMore);
            }

            let GenericHdr { kind, txid } = GenericHdr::decode(hdr_s)?;
            let Ok(sz) = get_record_size(kind, (end - pos) as usize) else {
                let _ = f.truncate(pos);
                return Err(OpCode::NeedMore);
            };

            let handles = self.redo_table.entry(txid).or_default();
            let crc = self.checksum.entry(txid).or_default();

            let buf = self.buffer.mut_slice(0, sz);
            let nbytes = f.read(buf, pos).unwrap();
            assert_eq!(nbytes, buf.len());

            if kind != MetaKind::Commit {
                crc.write(buf);
            }

            let p = &buf[GenericHdr::SIZE..];

            match kind {
                MetaKind::Begin => {
                    let _ = Begin::decode(p);
                    handles.push(RecordHandle::new(pos, sz));
                    if !self.undo_table.insert(txid) {
                        log::error!("duplicated txn Begin");
                        return Err(OpCode::BadData);
                    }
                }
                MetaKind::FileId => {
                    let fid = FileId::decode(p);
                    self.data_file.insert(txid, fid.file_id);
                    self.max_flush_id = self.max_flush_id.max(fid.file_id);
                }
                MetaKind::Commit => {
                    let x = Commit::decode(p);
                    // we can't handle this case, refuse to work
                    if x.checksum != crc.finish() as u32 {
                        return Err(OpCode::BadData);
                    }
                    let exist = self.undo_table.remove(&txid);
                    assert!(exist);
                    let r = self.data_file.remove(&txid);
                    assert!(r.is_some());
                    self.checksum.remove(&txid);
                    max_txid = max_txid.max(txid);
                }
                MetaKind::Map => {
                    let h = PageTableHdr::from_slice(p);
                    handles.push(RecordHandle::new(pos, sz + h.size));
                    Self::calc_crc(&mut self.buffer, f, crc, pos + sz as u64, h.size)?;
                    pos += h.size as u64;
                }
                MetaKind::Stat => {
                    let s = StatHdr::from_slice(p);
                    handles.push(RecordHandle::new(pos, sz + s.size as usize));
                    Self::calc_crc(&mut self.buffer, f, crc, pos + sz as u64, s.size as usize)?;
                    pos += s.size as u64;
                }
                MetaKind::Delete => {
                    let d = DeleteHdr::from_slice(p);
                    let len = d.size();
                    handles.push(RecordHandle::new(pos, sz + len));
                    Self::calc_crc(&mut self.buffer, f, crc, pos + sz as u64, len)?;
                    pos += len as u64;
                }
                MetaKind::DelInterval => {
                    let d = DelIntervalStartHdr::from_slice(p);
                    let len = d.size();
                    handles.push(RecordHandle::new(pos, sz + len));
                    Self::calc_crc(&mut self.buffer, f, crc, pos + sz as u64, len)?;
                    pos += len as u64;
                }
                MetaKind::Numerics | MetaKind::Interval => {
                    handles.push(RecordHandle::new(pos, sz));
                }
                MetaKind::KindEnd => {
                    unreachable!()
                }
            }
            pos += sz as u64;
        }

        Ok(max_txid)
    }

    fn redo(&mut self, f: &File) {
        // when last transaction is imcomplete, simply remove that transaction from redo_table
        for txid in self.undo_table.iter() {
            self.redo_table.remove(txid);
        }
        // and then remove data file in uncommit txn
        for (_, &file_id) in self.data_file.iter() {
            self.remove_data_file(file_id);
        }

        let mut table = BTreeMap::new();
        std::mem::swap(&mut table, &mut self.redo_table);
        // while flush thread is collecting stats delta, gc thread may already marked those stats as
        // Delete, in this case flush thread will record stats delta in log, in the recovery process,
        // we must ignore those delta or else, we will create mapping that will never be used, and
        // the stat itself is incomplete
        //
        // the table is iterate by txid order, thus if flush txid is smaller than gc's, delta stats
        // will be first inserted and then deleted by later parse gc's Delete, if flush txid bigger
        // than gc's, the delta stats will be skipped by `deleted` which recorded whe parse Delete
        // before parse flush txn
        let mut deleted = HashSet::new();
        let mut previous_is_not_gc = false;
        for (_, handles) in table.iter() {
            self.redo_impl(f, handles, &mut previous_is_not_gc, &mut deleted);
        }
    }

    fn redo_impl(
        &mut self,
        f: &File,
        handles: &[RecordHandle],
        non_gc: &mut bool,
        deleted: &mut HashSet<u64>,
    ) {
        for h in handles {
            if h.size > self.buffer.len() {
                self.buffer.realloc(h.size);
            }
            let buf = self.buffer.mut_slice(0, h.size);
            f.read(buf, h.offset).unwrap();
            let GenericHdr { kind, txid: _ } = GenericHdr::decode(buf).expect("must valid");
            let data = &buf[GenericHdr::SIZE..];

            match kind {
                MetaKind::Begin => {
                    let b = Begin::decode(data);
                    *non_gc = b.0 != TxnKind::GC;
                }
                MetaKind::Numerics => {
                    let src = Numerics::decode(data);
                    macro_rules! set {
                        ($dst:expr, $src:expr; $($field:ident),*) => {
                            $(
                                $dst.$field.fetch_max($src.$field.load(Relaxed), Relaxed);
                            )*
                        };
                    }
                    set!(
                        self.inner.numerics,
                        src;
                        signal,
                        next_file_id,
                        next_manifest_id,
                        oracle,
                        address,
                        wmk_oldest,
                        log_size
                    );
                }
                MetaKind::Stat => {
                    let stat = Stat::decode(data);
                    let e = self.inner.stat_ctx.entry(stat.file_id);
                    if deleted.remove(&stat.file_id) {
                        continue;
                    }

                    match e {
                        dashmap::Entry::Vacant(v) => {
                            let mut fstat = FileStat {
                                inner: stat.inner,
                                deleted_elems: BitMap::new(stat.active_elems),
                            };
                            for &seq in stat.deleted_elems.iter() {
                                fstat.deleted_elems.set(seq);
                            }
                            v.insert(fstat);
                        }
                        dashmap::Entry::Occupied(mut o) => {
                            let fstat = o.get_mut();
                            assert_eq!(stat.inner.file_id, fstat.inner.file_id);
                            fstat.inner = stat.inner;

                            for &seq in stat.deleted_elems.iter() {
                                fstat.deleted_elems.set(seq);
                            }
                        }
                    }
                }
                MetaKind::Map => {
                    let table = PageTable::decode(data);
                    for (k, v) in table.iter() {
                        self.table.add(*k, *v);
                    }
                }
                MetaKind::Delete => {
                    let mut lk = self.inner.obsolete_files.lock().unwrap();
                    lk.clear();
                    let del = Delete::decode(data);
                    lk.extend_from_slice(&del);
                    if *non_gc {
                        deleted.clear();
                    }
                    deleted.extend(del.iter());

                    for file_id in lk.iter() {
                        let r = self.inner.stat_ctx.remove(file_id);
                        assert!(r.is_some());
                    }
                }
                MetaKind::Interval => {
                    let ivl = IntervalPair::decode(data);
                    let mut lk = self.inner.interval.write().expect("can't lock write");
                    lk.insert(ivl.lo_addr, ivl.hi_addr, ivl.file_id);
                }
                MetaKind::DelInterval => {
                    let d = DelInterval::decode(data);
                    let mut lk = self.inner.interval.write().expect("can't lock write");

                    for lo in d.lo {
                        lk.remove(lo).expect("must exist");
                    }
                }
                _ => unreachable!("invalid kind: {kind:?}"),
            }
        }
    }

    fn remove_data_file(&self, file_id: u64) {
        let path = self.inner.opt.data_file(file_id);
        if path.exists() {
            log::info!("remove useless data file {path:?}");
            let _ = std::fs::remove_file(path);
        }
    }
}
