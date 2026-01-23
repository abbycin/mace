use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;

use crate::map::table::Swip;
use crate::meta::{
    BUCKET_VERSION, CURRENT_VERSION, DataStat, Delete, IMetaCodec, IntervalPair, MemBlobStat,
    MemDataStat, NUMERICS_KEY, ORPHAN_BLOB, ORPHAN_DATA, VERSION_KEY,
};
use crate::utils::ROOT_PID;
use crate::utils::bitmap::BitMap;
use crate::{
    OpCode,
    meta::{
        BUCKET_BLOB_INTERVAL, BUCKET_BLOB_STAT, BUCKET_DATA_INTERVAL, BUCKET_DATA_STAT,
        BUCKET_NUMERICS, BUCKET_OBSOLETE_BLOB, BUCKET_OBSOLETE_DATA, BUCKET_PAGE_TABLE, Manifest,
        MetaOp, entry::Numerics,
    },
    utils::options::ParsedOptions,
};

pub(crate) struct ManifestBuilder {
    inner: Manifest,
    max_data_id: u64,
    max_blob_id: u64,
    // accumulated sizes for DataStat to avoid re-iteration in finish()
    data_active_size: u64,
    data_total_size: u64,
}

impl ManifestBuilder {
    pub(crate) fn new(opt: Arc<ParsedOptions>) -> Self {
        Self {
            inner: Manifest::new(opt),
            max_data_id: 0,
            max_blob_id: 0,
            data_active_size: 0,
            data_total_size: 0,
        }
    }

    pub(crate) fn load(&mut self) -> Result<(), OpCode> {
        let map_ref = &self.inner.map;
        let data_stat_ref = &self.inner.data_stat;
        let blob_stat_ref = &self.inner.blob_stat;
        let obs_data_ref = &self.inner.obsolete_data;
        let obs_blob_ref = &self.inner.obsolete_blob;
        let numerics_ref = &self.inner.numerics;

        let mut next_pid = ROOT_PID;

        // 0. check version
        let mut has_version = false;
        if let Ok(val) = self
            .inner
            .btree
            .view(BUCKET_VERSION, |txn| txn.get(VERSION_KEY))
        {
            // version is stored as big endian u64
            let ver = u64::from_be_bytes(val[..8].try_into().map_err(|_| OpCode::BadData)?);
            if ver != CURRENT_VERSION {
                log::error!("Version mismatch, expect {} get {}", CURRENT_VERSION, ver);
                return Err(OpCode::BadVersion);
            }
            has_version = true;
        }

        // 1. load numerics
        if let Ok(val) = self
            .inner
            .btree
            .view(BUCKET_NUMERICS, |txn| txn.get(NUMERICS_KEY))
        {
            // if we have found numerics, it means the database is not empty
            // so we must have version, otherwise it's a legacy database
            if !has_version {
                log::error!("Version mismatch, please use migration tool");
                return Err(OpCode::BadVersion);
            }
            let src = Numerics::decode(&val);
            macro_rules! set {
                ($dst:expr, $src:expr; $($field:ident),*) => {
                    $(
                        $dst.$field.fetch_max($src.$field.load(Relaxed), Relaxed);
                    )*
                };
            }
            set!(
                numerics_ref,
                src;
                signal,
                next_data_id,
                next_blob_id,
                next_manifest_id,
                oracle,
                address,
                wmk_oldest,
                log_size
            );
            self.max_data_id = src.next_data_id.load(Relaxed).saturating_sub(1);
            self.max_blob_id = src.next_blob_id.load(Relaxed).saturating_sub(1);
        }

        // 2. load page table
        self.inner
            .btree
            .view(BUCKET_PAGE_TABLE, |txn| {
                let mut iter = txn.iter();
                let mut k = Vec::new();
                let mut v = Vec::new();
                while iter.next_ref(&mut k, &mut v) {
                    let pid_bytes: [u8; 8] = k[..8]
                        .try_into()
                        .map_err(|_| btree_store::Error::Corruption)?;
                    let addr_bytes: [u8; 8] = v[..8]
                        .try_into()
                        .map_err(|_| btree_store::Error::Corruption)?;
                    let pid = u64::from_be_bytes(pid_bytes);
                    let addr = u64::from_be_bytes(addr_bytes);
                    if pid + 1 > next_pid {
                        next_pid = pid + 1;
                    }
                    map_ref.index(pid).fetch_max(Swip::tagged(addr), Relaxed);
                }
                Ok(())
            })
            .map_err(|_| OpCode::IoError)?;

        // 3. load DataStat
        let mut active_size = 0;
        let mut total_size = 0;
        self.inner
            .btree
            .view(BUCKET_DATA_STAT, |txn| {
                let mut iter = txn.iter();
                let mut k = Vec::new();
                let mut v = Vec::new();
                while iter.next_ref(&mut k, &mut v) {
                    let stat = DataStat::decode(&v);
                    active_size += stat.active_size as u64;
                    total_size += stat.total_size as u64;
                    let mut fstat = MemDataStat {
                        inner: stat.inner,
                        mask: BitMap::new(stat.total_elems),
                    };
                    for &seq in stat.inactive_elems.iter() {
                        fstat.mask.set(seq);
                    }
                    data_stat_ref.insert(stat.file_id, fstat);
                }
                Ok(())
            })
            .map_err(|_| OpCode::IoError)?;
        self.data_active_size = active_size;
        self.data_total_size = total_size;

        // 4. load BlobStat
        self.inner
            .btree
            .view(BUCKET_BLOB_STAT, |txn| {
                let mut iter = txn.iter();
                let mut k = Vec::new();
                let mut v = Vec::new();
                while iter.next_ref(&mut k, &mut v) {
                    let stat = crate::meta::entry::BlobStat::decode(&v);
                    let mut bstat = MemBlobStat {
                        inner: stat.inner,
                        mask: BitMap::new(stat.nr_total),
                    };
                    for &seq in stat.inactive_elems.iter() {
                        bstat.mask.set(seq);
                    }
                    blob_stat_ref.write().insert(stat.file_id, bstat);
                }
                Ok(())
            })
            .map_err(|_| OpCode::IoError)?;

        // 5. load DataIntervals
        self.inner
            .btree
            .view(BUCKET_DATA_INTERVAL, |txn| {
                let mut iter = txn.iter();
                let mut k = Vec::new();
                let mut v = Vec::new();
                while iter.next_ref(&mut k, &mut v) {
                    let ivl = IntervalPair::decode(&v);
                    data_stat_ref
                        .interval
                        .write()
                        .upsert(ivl.lo_addr, ivl.hi_addr, ivl.file_id);
                }
                Ok(())
            })
            .map_err(|_| OpCode::IoError)?;

        self.inner
            .btree
            .view(BUCKET_BLOB_INTERVAL, |txn| {
                let mut iter = txn.iter();
                let mut k = Vec::new();
                let mut v = Vec::new();
                while iter.next_ref(&mut k, &mut v) {
                    let ivl = IntervalPair::decode(&v);
                    blob_stat_ref
                        .interval
                        .write()
                        .upsert(ivl.lo_addr, ivl.hi_addr, ivl.file_id);
                }
                Ok(())
            })
            .map_err(|_| OpCode::IoError)?;

        // 6. load Obsolete
        self.inner
            .btree
            .view(BUCKET_OBSOLETE_DATA, |txn| {
                let mut iter = txn.iter();
                let mut k = Vec::new();
                let mut v = Vec::new();
                let mut obs = obs_data_ref.lock();
                while iter.next_ref(&mut k, &mut v) {
                    let id_bytes: [u8; 8] = k[..8]
                        .try_into()
                        .map_err(|_| btree_store::Error::Corruption)?;
                    let id = u64::from_be_bytes(id_bytes);
                    obs.push(id);
                }
                Ok(())
            })
            .map_err(|_| OpCode::IoError)?;

        self.inner
            .btree
            .view(BUCKET_OBSOLETE_BLOB, |txn| {
                let mut iter = txn.iter();
                let mut k = Vec::new();
                let mut v = Vec::new();
                let mut obs = obs_blob_ref.lock();
                while iter.next_ref(&mut k, &mut v) {
                    let id_bytes: [u8; 8] = k[..8]
                        .try_into()
                        .map_err(|_| btree_store::Error::Corruption)?;
                    let id = u64::from_be_bytes(id_bytes);
                    obs.push(id);
                }
                Ok(())
            })
            .map_err(|_| OpCode::IoError)?;

        self.inner.map.set_next(next_pid);

        if !has_version {
            self.inner
                .btree
                .exec(BUCKET_VERSION, |txn| {
                    txn.put(VERSION_KEY, CURRENT_VERSION.to_be_bytes())
                })
                .map_err(|_| OpCode::IoError)?;
        }

        Ok(())
    }

    pub(crate) fn finish(mut self) -> Manifest {
        self.clean_orphans();
        self.inner.delete_files();

        self.inner
            .data_stat
            .update_size(self.data_active_size, self.data_total_size);

        self.inner.init(self.max_data_id, self.max_blob_id);

        self.inner
    }

    fn clean_orphans(&mut self) {
        let mut pending_data = Vec::new();
        let mut pending_blob = Vec::new();

        // 1. check for persisted orphan list from a previous crash during recovery
        let _ = self.inner.btree.view(BUCKET_NUMERICS, |txn| {
            if let Ok(val) = txn.get(b"orphan_data") {
                pending_data = Delete::decode(&val).id;
            }
            if let Ok(val) = txn.get(b"orphan_blob") {
                pending_blob = Delete::decode(&val).id;
            }
            Ok(())
        });

        // 2. if no persisted list, find them via linear probing starting from max_id + 1
        // if max_id + 1 doesn't exist, there are no orphans (or they were already cleared)
        if pending_data.is_empty() && pending_blob.is_empty() {
            let mut id = self.max_data_id + 1;
            while self.inner.opt.data_file(id).exists() {
                pending_data.push(id);
                id += 1;
            }

            let mut id = self.max_blob_id + 1;
            while self.inner.opt.blob_file(id).exists() {
                pending_blob.push(id);
                id += 1;
            }

            if pending_data.is_empty() && pending_blob.is_empty() {
                return;
            }

            // record the orphan list in btree-store for crash safety before deletion
            let mut txn = self.inner.begin();
            if !pending_data.is_empty() {
                let d = Delete {
                    id: pending_data.clone(),
                };
                let mut buf = vec![0u8; d.packed_size()];
                d.encode(&mut buf);
                txn.ops
                    .entry(BUCKET_NUMERICS)
                    .or_default()
                    .push(MetaOp::Put(ORPHAN_DATA.as_bytes().to_vec(), buf));
            }
            if !pending_blob.is_empty() {
                let d = Delete {
                    id: pending_blob.clone(),
                };
                let mut buf = vec![0u8; d.packed_size()];
                d.encode(&mut buf);
                txn.ops
                    .entry(BUCKET_NUMERICS)
                    .or_default()
                    .push(MetaOp::Put(ORPHAN_BLOB.as_bytes().to_vec(), buf));
            }
            txn.commit();
        }

        // 3. perform immediate deletion. SAFE because flushes haven't started
        log::info!(
            "removing orphans: data {:?}, blob {:?}",
            pending_data,
            pending_blob
        );
        for &id in &pending_data {
            let _ = std::fs::remove_file(self.inner.opt.data_file(id));
        }
        for &id in &pending_blob {
            let _ = std::fs::remove_file(self.inner.opt.blob_file(id));
        }

        // 4. clear the persisted record once deletion is complete
        let mut txn = self.inner.begin();
        txn.ops
            .entry(BUCKET_NUMERICS)
            .or_default()
            .push(MetaOp::Del(ORPHAN_DATA.as_bytes().to_vec()));
        txn.ops
            .entry(BUCKET_NUMERICS)
            .or_default()
            .push(MetaOp::Del(ORPHAN_BLOB.as_bytes().to_vec()));
        txn.commit();
    }
}
