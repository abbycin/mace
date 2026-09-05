use std::collections::BTreeMap;

use crate::meta::entry::{IMetaCodec, MetaOp, MetaRecord};
#[cfg(feature = "metrics")]
use crate::utils::observe::CounterMetric;

use super::entry::StatMissKind;
use super::{BUCKET_BLOB_STAT, BUCKET_DATA_STAT, Manifest, MetaKind, PersistStat};

pub(crate) struct Txn<'a> {
    manifest: &'a Manifest,
    // bucket_name -> operations
    ops: BTreeMap<String, Vec<MetaOp>>,
}

impl<'a> Txn<'a> {
    pub(crate) fn new(manifest: &'a Manifest) -> Self {
        Self {
            manifest,
            ops: BTreeMap::new(),
        }
    }

    pub(crate) fn ops_mut(&mut self) -> &mut BTreeMap<String, Vec<MetaOp>> {
        &mut self.ops
    }

    pub(crate) fn commit(&mut self) {
        if self.ops.is_empty() {
            return;
        }
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash("mace_manifest_before_multi_commit");

        #[cfg(feature = "metrics")]
        let mut missed_updates: Vec<StatMissKind> = Vec::new();
        // perform an atomic multi-bucket commit
        // all updates across different buckets are applied and flushed to disk
        // in a single SuperBlock write, significantly reducing I/O overhead
        let res = self.manifest.btree.exec_multi(|multi_txn| {
            for (bucket, bucket_ops) in &self.ops {
                multi_txn.exec(bucket, |tree_txn| {
                    for op in bucket_ops {
                        match op {
                            MetaOp::Put(k, v) => tree_txn.put(k, v)?,
                            MetaOp::Update(k, v, _miss_kind) => {
                                if !tree_txn.update(k, v)? {
                                    #[cfg(feature = "metrics")]
                                    missed_updates.push(*_miss_kind);
                                }
                            }
                            MetaOp::Del(k) => tree_txn.del(k)?,
                        }
                    }
                    Ok(())
                })?;
            }
            Ok(())
        });

        match res {
            Ok(_) => {
                #[cfg(feature = "metrics")]
                for miss_kind in &missed_updates {
                    let metric = match miss_kind {
                        StatMissKind::Data => CounterMetric::FlushConditionalDataStatPutMiss,
                        StatMissKind::Blob => CounterMetric::FlushConditionalBlobStatPutMiss,
                    };
                    self.manifest.opt.observer.counter(metric, 1);
                }
                self.ops.clear();
            }
            Err(e) => {
                log::error!("Metadata multi-bucket commit fail: {:?}", e);
                panic!("Metadata multi-bucket commit fail: {:?}", e)
            }
        }
    }

    pub(crate) fn record<T>(&mut self, kind: MetaKind, x: &T)
    where
        T: MetaRecord,
    {
        x.record(kind, &mut self.ops);
    }

    pub(crate) fn record_data_stat_update(&mut self, x: &PersistStat) {
        self.record_stat_update(BUCKET_DATA_STAT, x.file_id, x, StatMissKind::Data);
    }

    pub(crate) fn record_blob_stat_update(&mut self, x: &PersistStat) {
        self.record_stat_update(BUCKET_BLOB_STAT, x.file_id, x, StatMissKind::Blob);
    }

    fn record_stat_update<T>(&mut self, bucket: &str, file_id: u64, x: &T, miss_kind: StatMissKind)
    where
        T: IMetaCodec,
    {
        let mut buf = vec![0u8; x.packed_size()];
        x.encode(&mut buf);
        self.ops
            .entry(bucket.to_string())
            .or_default()
            .push(MetaOp::Update(
                file_id.to_le_bytes().to_vec(),
                buf,
                miss_kind,
            ));
    }
}
