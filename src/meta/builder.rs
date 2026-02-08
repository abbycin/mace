use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;

use crate::map::SharedState;
use crate::meta::entry::BlobStat;
use crate::meta::{
    BUCKET_VERSION, CURRENT_VERSION, DataStat, Delete, IMetaCodec, MemBlobStat, MemDataStat,
    NUMERICS_KEY, ORPHAN_BLOB, ORPHAN_DATA, VERSION_KEY, entry::BucketMeta,
};
use crate::{
    OpCode,
    meta::{
        BUCKET_BLOB_STAT, BUCKET_DATA_STAT, BUCKET_METAS, BUCKET_NUMERICS, BUCKET_OBSOLETE_BLOB,
        BUCKET_OBSOLETE_DATA, BUCKET_PENDING_DEL, Manifest, MetaOp, entry::Numerics,
    },
    utils::options::ParsedOptions,
};
use std::sync::mpsc::{Receiver, Sender};

pub(crate) struct ManifestBuilder {
    inner: Manifest,
    max_data_id: u64,
    max_blob_id: u64,
    // accumulated sizes for DataStat to avoid re-iteration in finish()
    data_active_size: u64,
    data_total_size: u64,
}

impl ManifestBuilder {
    pub(crate) fn new_with_channels(
        opt: Arc<ParsedOptions>,
        tx: Sender<SharedState>,
        rx: Receiver<()>,
    ) -> Self {
        Self {
            inner: Manifest::new(opt, tx, rx),
            max_data_id: 0,
            max_blob_id: 0,
            data_active_size: 0,
            data_total_size: 0,
        }
    }

    pub(crate) fn load(&mut self) -> Result<(), OpCode> {
        let data_stat_ref = &self.inner.data_stat;
        let blob_stat_ref = &self.inner.blob_stat;
        let obs_data_ref = &self.inner.obsolete_data;
        let obs_blob_ref = &self.inner.obsolete_blob;
        let numerics_ref = &self.inner.numerics;

        // 0. check version
        let mut has_version = false;
        if let Ok(val) = self
            .inner
            .btree
            .view(BUCKET_VERSION, |txn| txn.get(VERSION_KEY))
        {
            let ver = u64::from_be_bytes(val[..8].try_into().map_err(|_| OpCode::Corruption)?);
            if ver != CURRENT_VERSION {
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
            if !has_version {
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
                next_bucket_id,
                oracle,
                wmk_oldest,
                log_size
            );
            self.max_data_id = src.next_data_id.load(Relaxed).saturating_sub(1);
            self.max_blob_id = src.next_blob_id.load(Relaxed).saturating_sub(1);
        }

        // 2. load BucketMeta
        self.inner
            .btree
            .view(BUCKET_METAS, |txn| {
                let mut iter = txn.iter();
                let mut k = Vec::new();
                let mut v = Vec::new();
                while iter.next_ref(&mut k, &mut v) {
                    let meta = BucketMeta::decode(&v);
                    let name =
                        std::str::from_utf8(&k).map_err(|_| btree_store::Error::Corruption)?;
                    let meta = Arc::new(meta);
                    self.inner
                        .bucket_metas_by_id
                        .insert(meta.bucket_id, meta.clone());
                    self.inner.bucket_metas.insert(name.into(), meta);
                }
                Ok(())
            })
            .map_err(|_| OpCode::IoError)?;

        // 2.1 count pending buckets
        let mut nr_buckets = self.inner.bucket_metas.len() as u64;
        let _ = self.inner.btree.view(BUCKET_PENDING_DEL, |txn| {
            let mut iter = txn.iter();
            let mut k = Vec::new();
            let mut v = Vec::new();
            while iter.next_ref(&mut k, &mut v) {
                nr_buckets += 1;
            }
            Ok(())
        });
        self.inner.nr_buckets.store(nr_buckets, Relaxed);

        // 4. load DataStat
        let mut active_size = 0;
        let mut total_size = 0;
        self.inner
            .btree
            .view(BUCKET_DATA_STAT, |txn| {
                let mut iter = txn.iter();
                let mut k = Vec::new();
                let mut v = Vec::new();
                while iter.next_ref(&mut k, &mut v) {
                    let inner = DataStat::decode_inner_only(&v);
                    active_size += inner.active_size as u64;
                    total_size += inner.total_size as u64;
                    let fstat = MemDataStat { inner, mask: None };
                    data_stat_ref
                        .bucket_files()
                        .entry(inner.bucket_id)
                        .or_default()
                        .push(inner.file_id);
                    data_stat_ref.insert(inner.file_id, fstat);
                }
                Ok(())
            })
            .map_err(|_| OpCode::IoError)?;
        self.data_active_size = active_size;
        self.data_total_size = total_size;

        // 5. load BlobStat
        self.inner
            .btree
            .view(BUCKET_BLOB_STAT, |txn| {
                let mut iter = txn.iter();
                let mut k = Vec::new();
                let mut v = Vec::new();
                while iter.next_ref(&mut k, &mut v) {
                    let inner = BlobStat::decode_inner_only(&v);
                    let bstat = MemBlobStat { inner, mask: None };
                    blob_stat_ref
                        .bucket_files()
                        .entry(inner.bucket_id)
                        .or_default()
                        .push(inner.file_id);
                    blob_stat_ref.write().insert(inner.file_id, bstat);
                }
                Ok(())
            })
            .map_err(|_| OpCode::IoError)?;

        // 7. load Obsolete
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

        self.inner
            .numerics
            .next_data_id
            .store(self.max_data_id + 1, Relaxed);
        self.inner
            .numerics
            .next_blob_id
            .store(self.max_blob_id + 1, Relaxed);

        self.inner
    }

    fn clean_orphans(&mut self) {
        let mut pending_data = Vec::new();
        let mut pending_blob = Vec::new();

        let _ = self.inner.btree.view(BUCKET_NUMERICS, |txn| {
            if let Ok(val) = txn.get(ORPHAN_DATA) {
                pending_data = Delete::decode(&val).id;
            }
            if let Ok(val) = txn.get(ORPHAN_BLOB) {
                pending_blob = Delete::decode(&val).id;
            }
            Ok(())
        });

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

            let mut txn = self.inner.begin();
            if !pending_data.is_empty() {
                let d = Delete {
                    id: pending_data.clone(),
                };
                let mut buf = vec![0u8; d.packed_size()];
                d.encode(&mut buf);
                txn.ops_mut()
                    .entry(BUCKET_NUMERICS.to_string())
                    .or_default()
                    .push(MetaOp::Put(ORPHAN_DATA.as_bytes().to_vec(), buf));
            }
            if !pending_blob.is_empty() {
                let d = Delete {
                    id: pending_blob.clone(),
                };
                let mut buf = vec![0u8; d.packed_size()];
                d.encode(&mut buf);
                txn.ops_mut()
                    .entry(BUCKET_NUMERICS.to_string())
                    .or_default()
                    .push(MetaOp::Put(ORPHAN_BLOB.as_bytes().to_vec(), buf));
            }
            txn.commit();
        }

        for &id in &pending_data {
            let _ = std::fs::remove_file(self.inner.opt.data_file(id));
        }
        for &id in &pending_blob {
            let _ = std::fs::remove_file(self.inner.opt.blob_file(id));
        }

        let mut txn = self.inner.begin();
        txn.ops_mut()
            .entry(BUCKET_NUMERICS.to_string())
            .or_default()
            .push(MetaOp::Del(ORPHAN_DATA.as_bytes().to_vec()));
        txn.ops_mut()
            .entry(BUCKET_NUMERICS.to_string())
            .or_default()
            .push(MetaOp::Del(ORPHAN_BLOB.as_bytes().to_vec()));
        txn.commit();
    }
}
