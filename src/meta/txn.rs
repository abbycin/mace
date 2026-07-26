use std::collections::BTreeMap;

use crate::{
    meta::entry::{IMetaCodec, MetaOp, MetaRecord},
    utils::observe::CounterMetric,
};

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
        loop {
            #[cfg(feature = "failpoints")]
            crate::utils::failpoint::crash("mace_manifest_before_multi_commit");

            let mut missed_updates = Vec::new();
            // perform an atomic multi-bucket commit
            // all updates across different buckets are applied and flushed to disk
            // in a single SuperBlock write, significantly reducing I/O overhead
            let res = self.manifest.btree.exec_multi(|multi_txn| {
                for (bucket, bucket_ops) in &self.ops {
                    multi_txn.exec(bucket, |tree_txn| {
                        for op in bucket_ops {
                            match op {
                                MetaOp::Put(k, v) => tree_txn.put(k, v)?,
                                MetaOp::Update(k, v, miss_metric) => {
                                    if !tree_txn.update(k, v)? {
                                        missed_updates.push(*miss_metric);
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
                    for metric in missed_updates {
                        self.manifest.opt.observer.counter(metric, 1);
                    }
                    self.ops.clear();
                    break;
                }
                Err(btree_store::Error::Conflict) => {
                    // retry with a refreshed session handle
                    std::thread::yield_now();
                    continue;
                }
                Err(e) => {
                    log::error!("Metadata multi-bucket commit fail: {:?}", e);
                    panic!("Metadata multi-bucket commit fail: {:?}", e)
                }
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
        self.record_stat_update(
            BUCKET_DATA_STAT,
            x.file_id,
            x,
            CounterMetric::FlushConditionalDataStatPutMiss,
        );
    }

    pub(crate) fn record_blob_stat_update(&mut self, x: &PersistStat) {
        self.record_stat_update(
            BUCKET_BLOB_STAT,
            x.file_id,
            x,
            CounterMetric::FlushConditionalBlobStatPutMiss,
        );
    }

    fn record_stat_update<T>(
        &mut self,
        bucket: &str,
        file_id: u64,
        x: &T,
        miss_metric: CounterMetric,
    ) where
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
                miss_metric,
            ));
    }
}
