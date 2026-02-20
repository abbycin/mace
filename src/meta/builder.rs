use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;

use crate::map::SharedState;
use crate::meta::entry::BlobStat;
use crate::meta::{
    BUCKET_VERSION, CURRENT_VERSION, DataStat, IMetaCodec, MemBlobStat, MemDataStat, NUMERICS_KEY,
    ORPHAN_BLOB_MARKER_PREFIX, ORPHAN_DATA_MARKER_PREFIX, VERSION_KEY, entry::BucketMeta,
};
use crate::{
    OpCode,
    meta::{
        BUCKET_BLOB_STAT, BUCKET_DATA_STAT, BUCKET_METAS, BUCKET_NUMERICS, BUCKET_OBSOLETE_BLOB,
        BUCKET_OBSOLETE_DATA, BUCKET_PENDING_DEL, Manifest, entry::Numerics,
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
        let mut data_markers = Vec::new();
        let mut blob_markers = Vec::new();

        let _ = self.inner.btree.view(BUCKET_NUMERICS, |txn| {
            // file ids can be sparse because some ids are reserved before any file is flushed
            // rely on explicit per-file markers instead of max_id tail probing or directory scan
            let mut iter = txn.iter();
            let mut k = Vec::new();
            let mut v = Vec::new();
            while iter.next_ref(&mut k, &mut v) {
                if let Some(id) = Self::parse_orphan_marker_id(&k, ORPHAN_DATA_MARKER_PREFIX) {
                    data_markers.push((id, k.clone()));
                } else if let Some(id) = Self::parse_orphan_marker_id(&k, ORPHAN_BLOB_MARKER_PREFIX)
                {
                    blob_markers.push((id, k.clone()));
                }
            }
            Ok(())
        });

        let mut cleaned_data_marker_keys = Vec::new();
        for (id, key) in data_markers {
            let path = self.inner.opt.data_file(id);
            if !path.exists() || std::fs::remove_file(&path).is_ok() {
                cleaned_data_marker_keys.push(key);
            }
        }

        let mut cleaned_blob_marker_keys = Vec::new();
        for (id, key) in blob_markers {
            let path = self.inner.opt.blob_file(id);
            if !path.exists() || std::fs::remove_file(&path).is_ok() {
                cleaned_blob_marker_keys.push(key);
            }
        }

        if cleaned_data_marker_keys.is_empty() && cleaned_blob_marker_keys.is_empty() {
            return;
        }

        self.inner
            .btree
            .exec(BUCKET_NUMERICS, |txn| {
                for key in &cleaned_data_marker_keys {
                    let _ = txn.del(key);
                }
                for key in &cleaned_blob_marker_keys {
                    let _ = txn.del(key);
                }
                Ok(())
            })
            .expect("orphan marker update failed");
    }

    fn parse_orphan_marker_id(raw: &[u8], prefix: &str) -> Option<u64> {
        let prefix = prefix.as_bytes();
        if !raw.starts_with(prefix) {
            return None;
        }
        let raw_id = std::str::from_utf8(&raw[prefix.len()..]).ok()?;
        raw_id.parse::<u64>().ok()
    }
}
