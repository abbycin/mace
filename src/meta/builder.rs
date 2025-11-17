use crc32c::Crc32cHasher;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::Ordering::Relaxed;
use std::{hash::Hasher, path::PathBuf, sync::Arc};

use crate::map::table::Swip;
use crate::meta::entry::{
    BlobStat, DelInterval, DelIntervalStartHdr, DeleteHdr, GenericHdr, PageTableHdr, StatHdr,
    get_record_size,
};
use crate::meta::{
    Begin, Commit, DataStat, Delete, FileId, IntervalPair, MemBlobStat, MemDataStat,
};
use crate::types::traits::IAsSlice;
use crate::utils::ROOT_PID;
use crate::utils::bitmap::BitMap;
use crate::{
    OpCode,
    io::{File, GatherIO},
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
    /// txid -> data file id
    data_file: HashMap<u64, u64>,
    /// txid -> blob file id
    blob_file: HashMap<u64, u64>,
    max_txid: u64,
    max_data_id: u64,
    max_blob_id: u64,
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
            blob_file: HashMap::new(),
            max_txid: 0,
            max_data_id: 0,
            max_blob_id: 0,
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

        for item in self.inner.data_stat.iter() {
            let v = item.value();
            self.inner
                .data_stat
                .update_size(v.active_size as u64, v.total_size as u64);
        }

        // append to old file
        self.inner
            .init(self.max_data_id, self.max_blob_id, self.max_txid);

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
                    if !self.undo_table.insert(txid) {
                        log::error!("duplicated txn Begin");
                        return Err(OpCode::BadData);
                    }
                }
                MetaKind::FileId => {
                    let fid = FileId::decode(p);
                    if fid.is_blob {
                        self.blob_file.insert(txid, fid.file_id);
                        self.max_blob_id = self.max_blob_id.max(fid.file_id);
                    } else {
                        self.data_file.insert(txid, fid.file_id);
                        self.max_data_id = self.max_data_id.max(fid.file_id);
                    }
                }
                MetaKind::Commit => {
                    let x = Commit::decode(p);
                    // we can't handle this case, refuse to work
                    if x.checksum != crc.finish() as u32 {
                        return Err(OpCode::BadData);
                    }
                    let exist = self.undo_table.remove(&txid);
                    assert!(exist);
                    self.data_file.remove(&txid);
                    self.blob_file.remove(&txid);
                    self.checksum.remove(&txid);
                    max_txid = max_txid.max(txid);
                }
                MetaKind::Map => {
                    let h = PageTableHdr::from_slice(p);
                    handles.push(RecordHandle::new(pos, sz + h.size));
                    Self::calc_crc(&mut self.buffer, f, crc, pos + sz as u64, h.size)?;
                    pos += h.size as u64;
                }
                MetaKind::DataStat | MetaKind::BlobStat => {
                    let s = StatHdr::from_slice(p);
                    handles.push(RecordHandle::new(pos, sz + s.size as usize));
                    Self::calc_crc(&mut self.buffer, f, crc, pos + sz as u64, s.size as usize)?;
                    pos += s.size as u64;
                }
                MetaKind::DataDelete | MetaKind::BlobDelete => {
                    let d = DeleteHdr::from_slice(p);
                    let len = d.size();
                    handles.push(RecordHandle::new(pos, sz + len));
                    Self::calc_crc(&mut self.buffer, f, crc, pos + sz as u64, len)?;
                    pos += len as u64;
                }
                MetaKind::DataDelInterval | MetaKind::BlobDelInterval => {
                    let d = DelIntervalStartHdr::from_slice(p);
                    let len = d.size();
                    handles.push(RecordHandle::new(pos, sz + len));
                    Self::calc_crc(&mut self.buffer, f, crc, pos + sz as u64, len)?;
                    pos += len as u64;
                }
                MetaKind::Numerics | MetaKind::DataInterval | MetaKind::BlobInterval => {
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
            self.remove_file(file_id, false);
        }

        for (_, &file_id) in self.blob_file.iter() {
            self.remove_file(file_id, true);
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
        // than gc's, the delta stats will be skipped by `xx_deleted` which was recorded when parse
        // Delete before parse flush txn
        // NOTE: the `xx_deleted` must not be `clear`, because multiple Delete may happen before the
        // delta stat was recorded (see process_obsoleted_data/blob and rewrite_data/blob)
        let mut data_deleted = HashSet::new();
        let mut blob_deleted = HashSet::new();
        for (_, handles) in table.iter() {
            self.redo_impl(f, handles, &mut data_deleted, &mut blob_deleted);
        }
    }

    fn redo_impl(
        &mut self,
        f: &File,
        handles: &[RecordHandle],
        data_deleted: &mut HashSet<u64>,
        blob_deleted: &mut HashSet<u64>,
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
                        next_data_id,
                        next_manifest_id,
                        oracle,
                        address,
                        wmk_oldest,
                        log_size
                    );
                }
                MetaKind::DataStat => {
                    let stat = DataStat::decode(data);
                    let e = self.inner.data_stat.entry(stat.file_id);
                    if data_deleted.contains(&stat.file_id) {
                        continue;
                    }

                    match e {
                        dashmap::Entry::Vacant(v) => {
                            let mut fstat = MemDataStat {
                                inner: stat.inner,
                                mask: BitMap::new(stat.active_elems),
                            };
                            for &seq in stat.inactive_elems.iter() {
                                fstat.mask.set(seq);
                            }
                            v.insert(fstat);
                        }
                        dashmap::Entry::Occupied(mut o) => {
                            let fstat = o.get_mut();
                            assert_eq!(stat.file_id, fstat.file_id);
                            fstat.inner = stat.inner;

                            for &seq in stat.inactive_elems.iter() {
                                fstat.mask.set(seq);
                            }
                        }
                    }
                }
                MetaKind::BlobStat => {
                    let stat = BlobStat::decode(data);
                    let mut b = self.inner.blob_stat.map.write().unwrap();
                    let e = b.entry(stat.file_id);
                    if blob_deleted.contains(&stat.file_id) {
                        continue;
                    }

                    match e {
                        Entry::Vacant(v) => {
                            let mut bstat = MemBlobStat {
                                inner: stat.inner,
                                mask: BitMap::new(stat.nr_active),
                            };
                            for &seq in stat.inactive_elems.iter() {
                                bstat.mask.set(seq);
                            }
                            v.insert(bstat);
                        }
                        Entry::Occupied(mut o) => {
                            let bstat = o.get_mut();
                            assert_eq!(bstat.file_id, stat.file_id);
                            bstat.inner = stat.inner;

                            for &seq in stat.inactive_elems.iter() {
                                bstat.mask.set(seq);
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
                MetaKind::DataDelete => {
                    let mut lk = self.inner.obsolete_data.lock().unwrap();
                    lk.clear();
                    let del = Delete::decode(data);
                    lk.extend_from_slice(&del);
                    data_deleted.extend(del.iter());

                    for file_id in del.iter() {
                        let r = self.inner.data_stat.remove(file_id);
                        assert!(r.is_some());
                    }
                }
                MetaKind::BlobDelete => {
                    let mut lk = self.inner.obsolete_blob.lock().unwrap();
                    lk.clear();
                    let del = Delete::decode(data);
                    lk.extend_from_slice(&del);
                    blob_deleted.extend(del.iter());

                    for &file_id in del.iter() {
                        let r = self.inner.blob_stat.remove_stat(file_id);
                        assert!(r.is_some());
                    }
                }
                MetaKind::DataInterval => {
                    let ivl = IntervalPair::decode(data);
                    let mut lk = self
                        .inner
                        .data_stat
                        .interval
                        .write()
                        .expect("can't lock write");
                    lk.upsert(ivl.lo_addr, ivl.hi_addr, ivl.file_id);
                }
                MetaKind::BlobInterval => {
                    let ivl = IntervalPair::decode(data);
                    let mut lk = self
                        .inner
                        .blob_stat
                        .interval
                        .write()
                        .expect("can't lock write");
                    lk.upsert(ivl.lo_addr, ivl.hi_addr, ivl.file_id);
                }
                MetaKind::DataDelInterval => {
                    let d = DelInterval::decode(data);
                    let mut lk = self
                        .inner
                        .data_stat
                        .interval
                        .write()
                        .expect("can't lock write");

                    for lo in d.lo {
                        lk.remove(lo).expect("must exist");
                    }
                }
                MetaKind::BlobDelInterval => {
                    let d = DelInterval::decode(data);
                    let mut lk = self
                        .inner
                        .blob_stat
                        .interval
                        .write()
                        .expect("can't lock write");

                    for lo in d.lo {
                        lk.remove(lo).expect("must exist");
                    }
                }
                _ => unreachable!("invalid kind: {kind:?}"),
            }
        }
    }

    fn remove_file(&self, file_id: u64, is_blob: bool) {
        let path = if is_blob {
            self.inner.opt.blob_file(file_id)
        } else {
            self.inner.opt.data_file(file_id)
        };
        if path.exists() {
            log::info!("remove useless data file {path:?}");
            let _ = std::fs::remove_file(path);
        }
    }
}
