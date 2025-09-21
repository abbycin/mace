use crc32c::Crc32cHasher;
use io::{File, GatherIO};
use std::collections::BTreeMap;
use std::ptr::addr_of;
use std::sync::atomic::Ordering::Relaxed;
use std::{hash::Hasher, path::PathBuf, sync::Arc};

use crate::map::table::Swip;
use crate::meta::entry::{
    Begin, DeleteHdr, ENTRY_KIND_LEN, Lid, LidHdr, PageTableHdr, StatHdr, get_record_size,
};
use crate::meta::{Commit, Delete, FileStat, Stat};
use crate::types::traits::IAsSlice;
use crate::utils::bitmap::BitMap;
use crate::utils::data::GatherWriter;
use crate::utils::{Handle, MutRef, NULL_ORACLE, ROOT_PID};
use crate::{
    OpCode,
    meta::{
        IMetaCodec, Manifest,
        entry::{EntryKind, Numerics, PageTable},
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

/// we assume there's no **duplicate** entry kind in a transaction
pub(crate) struct ManifestBuilder {
    inner: Manifest,
    dirty_table: BTreeMap<u64, Vec<RecordHandle>>,
    undo_txid: u64,
    table: PageTable,
    buffer: Block,
}

impl ManifestBuilder {
    pub(crate) fn new(opt: Arc<ParsedOptions>) -> Self {
        Self {
            inner: Manifest::new(opt),
            dirty_table: BTreeMap::new(),
            undo_txid: NULL_ORACLE,
            table: PageTable::default(),
            buffer: Block::alloc(1 << 20),
        }
    }

    /// we assume data file was flushed before transaction commit, when transaction was committed,
    /// if the manifest is complete, the data file is complete too, or else simply rollback the last
    /// imcomplete (or corrupted) transaction and remove last data file
    ///
    /// if transaction is imcomplete (or corrupted) in non-last manifest, we can't handle this case,
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
            _ => {}
        }
        self.redo(&f);
        Ok(())
    }

    pub(crate) fn finish(mut self, snap: Option<u64>) -> Manifest {
        let mut next = ROOT_PID;
        for (&k, v) in self.table.iter() {
            next = next.max(k);
            if v.addr == NULL_ADDR {
                self.inner.map.insert_free(k);
            } else {
                self.inner
                    .map
                    .index(k)
                    .fetch_max(Swip::tagged(v.addr), Relaxed);
            }
        }
        if next != ROOT_PID {
            self.inner.map.set_next(next + 1);
        }

        self.inner.delete_files();
        if let Some(snap_id) = snap {
            self.inner.unlink_old(snap_id);
        }

        // reset data file id to the latest stable one
        self.inner
            .numerics
            .next_file_id
            .store(self.inner.numerics.flushed_id.wrapping_add(1), Relaxed);

        self.inner.txid.store(self.inner.numerics.txid + 1, Relaxed);
        // append to old file
        let id = self.inner.numerics.next_manifest_id.load(Relaxed);
        self.inner.writer = Handle::new(GatherWriter::append(&self.inner.opt.manifest(id), 32));

        self.inner
    }

    fn calc_crc(
        &mut self,
        f: &File,
        h: &mut Crc32cHasher,
        pos: u64,
        size: usize,
    ) -> Result<(), OpCode> {
        if size > self.buffer.len() {
            self.buffer.realloc(size);
        }
        let data = self.buffer.mut_slice(0, size);
        let nbytes = f.read(data, pos).unwrap();
        if nbytes < data.len() {
            return Err(OpCode::NeedMore);
        }
        h.write(data);
        Ok(())
    }

    fn analyze(&mut self, f: &File) -> Result<(), OpCode> {
        let end = f.size().unwrap();
        let mut pos = 0;
        let mut crc = Crc32cHasher::default();
        let mut handles = Vec::new();

        while pos < end {
            let h = self.buffer.mut_slice(0, 1);
            f.read(h, pos).unwrap();

            let ek: EntryKind = h[0].try_into()?;
            let Ok(sz) = get_record_size(ek, (end - pos) as usize) else {
                let _ = f.truncate(pos);
                return Err(OpCode::NeedMore);
            };

            let hdr_s = self.buffer.mut_slice(0, sz);
            let nbytes = f.read(hdr_s, pos).unwrap();
            assert_eq!(nbytes, hdr_s.len());

            if ek != EntryKind::Commit {
                crc.write(hdr_s);
            }

            let p = &hdr_s[ENTRY_KIND_LEN..]; // excluding ek

            match ek {
                EntryKind::Begin => {
                    let x = Begin::decode(hdr_s);
                    if self.undo_txid != NULL_ORACLE {
                        // incomplete transaction
                        return Err(OpCode::BadData);
                    }
                    self.undo_txid = x.txid;
                }
                EntryKind::Commit => {
                    let x = Commit::decode(hdr_s);
                    // we can't handle this case, refuse to work
                    if x.checksum != crc.finish() as u32 {
                        return Err(OpCode::BadData);
                    }
                    self.undo_txid = NULL_ORACLE;
                    let mut tmp = Vec::new();
                    std::mem::swap(&mut tmp, &mut handles);
                    self.dirty_table.insert(x.txid, tmp);
                    crc = Crc32cHasher::default();
                }
                EntryKind::Map => {
                    let h = PageTableHdr::from_slice(p);
                    handles.push(RecordHandle::new(pos, sz + h.size));
                    self.calc_crc(f, &mut crc, pos + sz as u64, h.size)?;
                    pos += h.size as u64;
                }
                EntryKind::Stat => {
                    let s = StatHdr::from_slice(p);
                    handles.push(RecordHandle::new(pos, sz + s.size as usize));
                    self.calc_crc(f, &mut crc, pos + sz as u64, s.size as usize)?;
                    pos += s.size as u64;
                }
                EntryKind::Lid => {
                    let l = LidHdr::from_slice(p);
                    handles.push(RecordHandle::new(pos, sz + l.size as usize));
                    self.calc_crc(f, &mut crc, pos + sz as u64, l.size as usize)?;
                    pos += l.size as u64;
                }
                EntryKind::Delete => {
                    let d = DeleteHdr::from_slice(p);
                    handles.push(RecordHandle::new(pos, sz + d.size as usize));
                    self.calc_crc(f, &mut crc, pos + sz as u64, d.size as usize)?;
                    pos += d.size as u64;
                }
                EntryKind::Meta => {
                    handles.push(RecordHandle::new(pos, sz));
                }
            }
            pos += sz as u64;
        }

        Ok(())
    }

    fn redo(&mut self, f: &File) {
        // when last transaction is imcomplete, simply remove that transaction from dirty_table and
        // the last data file (if exist)
        if self.undo_txid != NULL_ORACLE
            && let Some(handles) = self.dirty_table.remove(&self.undo_txid)
        {
            self.remove_data_file(f, &handles);
        }

        let mut table = BTreeMap::new();
        std::mem::swap(&mut table, &mut self.dirty_table);
        for (_, handles) in table.iter() {
            self.redo_impl(f, handles);
        }
    }

    fn redo_impl(&mut self, f: &File, handles: &[RecordHandle]) {
        for h in handles {
            if h.size > self.buffer.len() {
                self.buffer.realloc(h.size);
            }
            let data = self.buffer.mut_slice(0, h.size);
            f.read(data, h.offset).unwrap();
            let ek = data[0].try_into().expect("must valid");

            match ek {
                EntryKind::Meta => {
                    let numerics = Numerics::decode(data);
                    assert!(
                        self.inner.numerics.epoch.load(Relaxed) <= numerics.epoch.load(Relaxed)
                    );
                    assert!(
                        self.inner.numerics.next_manifest_id.load(Relaxed)
                            <= numerics.next_manifest_id.load(Relaxed)
                    );
                    let dst = Arc::get_mut(&mut self.inner.numerics).expect("someone is borrowing");
                    let src = addr_of!(numerics);
                    unsafe {
                        std::ptr::copy_nonoverlapping(src, dst, 1);
                    }
                }
                EntryKind::Stat => {
                    let stat = Stat::decode(data);
                    let e = self.inner.file_stat.entry(stat.file_id);

                    match e {
                        dashmap::Entry::Vacant(v) => {
                            let mut fstat = MutRef::new(FileStat {
                                inner: stat.inner,
                                deleted_elems: BitMap::new(stat.inner.active_elems),
                            });
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
                EntryKind::Map => {
                    let table = PageTable::decode(data);
                    for (k, v) in table.iter() {
                        self.table.add(*k, v.addr, v.epoch);
                    }
                }
                EntryKind::Lid => {
                    let lid = Lid::decode(data);
                    // create a clone to avoid dead-lock
                    let fstat = {
                        self.inner
                            .file_stat
                            .get(&lid.physical_id)
                            .expect("invalid file id")
                            .clone()
                    };

                    // process deleted first, because same id maybe remapped later
                    for id in &lid.del_lids {
                        self.inner.file_stat.remove(id);
                    }

                    for &id in &lid.new_lids {
                        self.inner.file_stat.insert(id, fstat.clone());
                    }
                }
                EntryKind::Delete => {
                    let mut lk = self.inner.obsolete_files.lock().unwrap();
                    lk.clear();
                    let del = Delete::decode(data);
                    lk.extend_from_slice(&del);
                }
                _ => unreachable!("invalid kind: {ek:?}"),
            }
        }
    }

    fn remove_data_file(&mut self, f: &File, handles: &[RecordHandle]) {
        for h in handles {
            if h.size > self.buffer.len() {
                self.buffer.realloc(h.size);
            }
            let data = self.buffer.mut_slice(0, h.size);
            f.read(data, h.offset).unwrap();
            let ek: EntryKind = data[0].try_into().expect("must valid");
            if ek == EntryKind::Stat {
                let stat = Stat::decode(data);
                let path = self.inner.opt.data_file(stat.file_id);
                if path.exists() {
                    log::info!("remove useless data file {path:?}");
                    let _ = std::fs::remove_file(path);
                }
            }
        }
    }
}
