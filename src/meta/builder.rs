use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;

use crate::map::table::Swip;
use crate::meta::{DataStat, IMetaCodec, IntervalPair, MemBlobStat, MemDataStat, NUMERICS_KEY};
use crate::utils::ROOT_PID;
use crate::utils::bitmap::BitMap;
use crate::{
    OpCode,
    meta::{
        BUCKET_BLOB_INTERVAL, BUCKET_BLOB_STAT, BUCKET_DATA_INTERVAL, BUCKET_DATA_STAT,
        BUCKET_NUMERICS, BUCKET_OBSOLETE_BLOB, BUCKET_OBSOLETE_DATA, BUCKET_PAGE_TABLE, Manifest,
        entry::Numerics,
    },
    utils::options::ParsedOptions,
};

pub(crate) struct ManifestBuilder {
    inner: Manifest,
    max_data_id: u64,
    max_blob_id: u64,
}

impl ManifestBuilder {
    pub(crate) fn new(opt: Arc<ParsedOptions>) -> Self {
        Self {
            inner: Manifest::new(opt),
            max_data_id: 0,
            max_blob_id: 0,
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
        let mut local_max_data_id = 0;
        let mut local_max_blob_id = 0;

        // 1. Load Numerics
        if let Ok(val) = self
            .inner
            .btree
            .view(BUCKET_NUMERICS, |txn| txn.get(NUMERICS_KEY))
        {
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
                next_manifest_id,
                oracle,
                address,
                wmk_oldest,
                log_size
            );
            local_max_data_id =
                local_max_data_id.max(src.next_data_id.load(Relaxed).saturating_sub(1));
            local_max_blob_id =
                local_max_blob_id.max(src.next_blob_id.load(Relaxed).saturating_sub(1));
        }

        // 2. Load PageTable
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

        // 3. Load DataStat
        self.inner
            .btree
            .view(BUCKET_DATA_STAT, |txn| {
                let mut iter = txn.iter();
                let mut k = Vec::new();
                let mut v = Vec::new();
                while iter.next_ref(&mut k, &mut v) {
                    let stat = DataStat::decode(&v);
                    let mut fstat = MemDataStat {
                        inner: stat.inner,
                        mask: BitMap::new(stat.total_elems),
                    };
                    for &seq in stat.inactive_elems.iter() {
                        fstat.mask.set(seq);
                    }
                    data_stat_ref.insert(stat.file_id, fstat);
                    if stat.file_id > local_max_data_id {
                        local_max_data_id = stat.file_id;
                    }
                }
                Ok(())
            })
            .map_err(|_| OpCode::IoError)?;

        // 4. Load BlobStat
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
                    if stat.file_id > local_max_blob_id {
                        local_max_blob_id = stat.file_id;
                    }
                }
                Ok(())
            })
            .map_err(|_| OpCode::IoError)?;

        // 5. Load Intervals
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

        // 6. Load Obsolete
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
        self.max_data_id = local_max_data_id;
        self.max_blob_id = local_max_blob_id;

        Ok(())
    }

    pub(crate) fn finish(mut self) -> Manifest {
        self.inner.delete_files();

        for item in self.inner.data_stat.iter() {
            let v = item.value();
            self.inner
                .data_stat
                .update_size(v.active_size as u64, v.total_size as u64);
        }

        self.inner.init(self.max_data_id, self.max_blob_id);

        self.inner
    }
}
