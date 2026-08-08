use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;

use crate::map::SharedState;
use crate::meta::entry::IMetaCodec;
use crate::meta::{
    BUCKET_FRONTIER, BUCKET_VERSION, BucketDurableFrontier, CURRENT_VERSION, FileKind, MemStat,
    ORPHAN_BLOB_MARKER_PREFIX, ORPHAN_DATA_MARKER_PREFIX, PersistStat, SEQUENCES_KEY, VERSION_KEY,
    entry::BucketMeta,
};
use crate::must_ok;
use crate::{
    OpCode,
    meta::{
        BUCKET_BLOB_STAT, BUCKET_DATA_STAT, BUCKET_METAS, BUCKET_MISC, BUCKET_OBSOLETE_BLOB,
        BUCKET_OBSOLETE_DATA, BUCKET_PENDING_DEL, Manifest, entry::Sequences,
    },
    utils::options::{ParsedOptions, PersistedOptions},
};
use std::sync::mpsc::{Receiver, Sender};

pub(crate) struct ManifestBuilder {
    inner: Manifest,
    max_file_id: u64,
    // accumulated sizes for DataStat to avoid re-iteration in finish()
    data_active_size: u64,
    data_total_size: u64,
    blob_active_size: u64,
    blob_total_size: u64,
}

impl ManifestBuilder {
    pub(crate) fn new_with_channels(
        opt: Arc<ParsedOptions>,
        tx: Sender<SharedState>,
        rx: Receiver<()>,
    ) -> Self {
        Self {
            inner: Manifest::new(opt, tx, rx),
            max_file_id: 0,
            data_active_size: 0,
            data_total_size: 0,
            blob_active_size: 0,
            blob_total_size: 0,
        }
    }

    pub(crate) fn load(&mut self) -> Result<Option<PersistedOptions>, OpCode> {
        let sequences_ref = &self.inner.sequences;
        let current = PersistedOptions::from_options(self.inner.opt.as_ref());

        // 0. check version
        let mut has_version = false;
        if let Ok(val) = self
            .inner
            .btree
            .view(BUCKET_VERSION, |txn| txn.get(VERSION_KEY))
        {
            let ver = u64::from_le_bytes(val[..8].try_into().map_err(|_| OpCode::Corruption)?);
            if ver != CURRENT_VERSION {
                return Err(OpCode::BadVersion);
            }
            has_version = true;
        }

        // 1. load persisted global options
        let persisted = match self.inner.load_persisted_options_if_present()? {
            Some(persisted) => {
                persisted.check_compatible(self.inner.opt.as_ref())?;
                persisted
            }
            None if !has_version && self.inner.is_pristine_uninitialized()? => {
                self.inner.bootstrap_persisted_options(&current)?;
                has_version = true;
                current.clone()
            }
            None => return Err(OpCode::Corruption),
        };
        let needs_writeback = persisted != current;

        // 2. load sequences
        if let Ok(val) = self
            .inner
            .btree
            .view(BUCKET_MISC, |txn| txn.get(SEQUENCES_KEY))
        {
            if !has_version {
                return Err(OpCode::BadVersion);
            }
            let src = Sequences::decode(&val);
            macro_rules! set {
                ($dst:expr, $src:expr; $($field:ident),*) => {
                    $(
                        $dst.$field.fetch_max($src.$field.load(Relaxed), Relaxed);
                    )*
                };
            }
            set!(
                sequences_ref,
                src;
                next_file_id,
                next_bucket_id,
                oracle
            );
            self.max_file_id = src.next_file_id.load(Relaxed).saturating_sub(1);
        }

        // 3. load BucketMeta
        self.inner
            .btree
            .view(BUCKET_METAS, |txn| {
                let mut iter = txn.iter_uncached();
                let mut k = Vec::new();
                let mut v = Vec::new();
                while iter.next_ref(&mut k, &mut v) {
                    let meta = BucketMeta::decode(&v);
                    let name =
                        std::str::from_utf8(&k).expect("bucket metadata key must be valid utf-8");
                    let meta = Arc::new(meta);
                    let bucket_id = meta.id;
                    self.inner
                        .bucket_metas_by_id
                        .insert(bucket_id, meta.clone());
                    self.inner.bucket_metas.insert(name.into(), meta);
                    self.inner.ensure_bucket_runtime(bucket_id);
                }
                Ok(())
            })
            .map_err(|_| OpCode::IoError)?;

        // 2.1 count pending buckets
        let mut nr_buckets = self.inner.bucket_metas.len() as u64;
        let _ = self.inner.btree.view(BUCKET_PENDING_DEL, |txn| {
            let mut iter = txn.iter_uncached();
            let mut k = Vec::new();
            let mut v = Vec::new();
            while iter.next_ref(&mut k, &mut v) {
                nr_buckets += 1;
            }
            Ok(())
        });
        self.inner.nr_buckets.store(nr_buckets, Relaxed);

        // 3. load bucket durable frontier
        self.inner
            .btree
            .view(BUCKET_FRONTIER, |txn| {
                let mut iter = txn.iter_uncached();
                let mut k = Vec::new();
                let mut v = Vec::new();
                while iter.next_ref(&mut k, &mut v) {
                    let item = BucketDurableFrontier::decode(&v);
                    self.inner.bucket_frontier.insert(item.bucket_id, item.lsn);
                }
                Ok(())
            })
            .map_err(|_| OpCode::IoError)?;

        for kind in FileKind::ALL {
            let mut active_size = 0;
            let mut total_size = 0;
            self.inner
                .btree
                .view(
                    match kind {
                        FileKind::Data => BUCKET_DATA_STAT,
                        FileKind::Blob => BUCKET_BLOB_STAT,
                    },
                    |txn| {
                        let mut iter = txn.iter_uncached();
                        let mut k = Vec::new();
                        let mut v = Vec::new();
                        while iter.next_ref(&mut k, &mut v) {
                            let inner = PersistStat::decode_inner_only(&v);
                            active_size += inner.active_size as u64;
                            total_size += inner.total_size as u64;
                            let fstat = MemStat::from_parts(inner, None);
                            self.inner.stat_ctx(kind).insert_loaded_stat(fstat);
                            self.inner
                                .get_bucket_runtime(inner.bucket_id)
                                .observe_stat_epoch(kind, inner.up1, inner.up2);
                        }
                        Ok(())
                    },
                )
                .map_err(|_| OpCode::IoError)?;
            self.set_loaded_sizes(kind, active_size, total_size);

            self.inner
                .btree
                .view(
                    match kind {
                        FileKind::Data => BUCKET_OBSOLETE_DATA,
                        FileKind::Blob => BUCKET_OBSOLETE_BLOB,
                    },
                    |txn| {
                        let mut iter = txn.iter_uncached();
                        let mut k = Vec::new();
                        let mut v = Vec::new();
                        let mut obs = self.inner.file_state(kind).obsolete.lock();
                        while iter.next_ref(&mut k, &mut v) {
                            let id_bytes: [u8; 8] =
                                k[..8].try_into().expect("obsolete file key must be a u64");
                            let id = u64::from_le_bytes(id_bytes);
                            obs.push(id);
                        }
                        Ok(())
                    },
                )
                .map_err(|_| OpCode::IoError)?;
        }

        if !has_version {
            self.inner
                .btree
                .exec(BUCKET_VERSION, |txn| {
                    txn.put(VERSION_KEY, CURRENT_VERSION.to_le_bytes())
                })
                .map_err(|_| OpCode::IoError)?;
        }

        Ok(needs_writeback.then_some(current))
    }

    pub(crate) fn finish(mut self) -> Manifest {
        self.clean_orphans();
        self.inner.delete_files();

        self.inner
            .stat_ctx(FileKind::Data)
            .update_size(self.data_active_size, self.data_total_size);

        self.inner
            .stat_ctx(FileKind::Blob)
            .update_size(self.blob_active_size, self.blob_total_size);

        self.inner
            .sequences
            .next_file_id
            .store(self.max_file_id + 1, Relaxed);

        self.inner
    }

    fn set_loaded_sizes(&mut self, kind: FileKind, active_size: u64, total_size: u64) {
        match kind {
            FileKind::Data => {
                self.data_active_size = active_size;
                self.data_total_size = total_size;
            }
            FileKind::Blob => {
                self.blob_active_size = active_size;
                self.blob_total_size = total_size;
            }
        }
    }

    fn clean_orphans(&mut self) {
        let mut markers: [Vec<(u64, Vec<u8>)>; 2] = [Vec::new(), Vec::new()];

        let _ = self.inner.btree.view(BUCKET_MISC, |txn| {
            // file ids can be sparse because some ids are reserved before any file is flushed
            // rely on explicit per-file markers instead of max_id tail probing or directory scan
            let mut iter = txn.iter_uncached();
            let mut k = Vec::new();
            let mut v = Vec::new();
            while iter.next_ref(&mut k, &mut v) {
                for kind in FileKind::ALL {
                    let prefix = match kind {
                        FileKind::Data => ORPHAN_DATA_MARKER_PREFIX,
                        FileKind::Blob => ORPHAN_BLOB_MARKER_PREFIX,
                    };
                    if let Some(id) = Self::parse_orphan_marker_id(&k, prefix) {
                        markers[kind.slot()].push((id, k.clone()));
                        break;
                    }
                }
            }
            Ok(())
        });

        let mut cleaned_marker_keys: [Vec<Vec<u8>>; 2] = [Vec::new(), Vec::new()];
        for kind in FileKind::ALL {
            for (id, key) in markers[kind.slot()].drain(..) {
                self.observe_orphan_id(id);
                let path = match kind {
                    FileKind::Data => self.inner.opt.data_file(id),
                    FileKind::Blob => self.inner.opt.blob_file(id),
                };
                must_ok!(
                    self.inner.opt.fs.remove_file_if_exists(&path),
                    "path {:?}",
                    path
                );
                cleaned_marker_keys[kind.slot()].push(key);
            }
        }

        if FileKind::ALL
            .iter()
            .all(|&kind| cleaned_marker_keys[kind.slot()].is_empty())
        {
            return;
        }

        self.inner.opt.sync_data_dir();
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash(
            "mace_recovery_orphan_cleanup_after_data_dir_sync_before_marker_clear",
        );

        must_ok!(
            self.inner.btree.exec(BUCKET_MISC, |txn| {
                for kind in FileKind::ALL {
                    for key in &cleaned_marker_keys[kind.slot()] {
                        let _ = txn.del(key);
                    }
                }
                Ok(())
            }),
            "orphan marker update failed"
        );
    }

    fn observe_orphan_id(&mut self, id: u64) {
        self.max_file_id = self.max_file_id.max(id);
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
