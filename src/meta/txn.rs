use std::collections::BTreeMap;

use crate::meta::entry::{IMetaCodec, MetaOp, MetaRecord};
use crate::must_true;
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
        let mut missed_stats: Vec<StatMissKind> = Vec::new();
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
                                    missed_stats.push(*_miss_kind);
                                }
                            }
                            // a key whose absence is a protocol violation must not be
                            // swallowed: report and abort instead of continuing
                            MetaOp::Del(k) => {
                                must_true!(
                                    tree_txn.del(k)?,
                                    "meta key must exist on delete, bucket {}, key {:?}",
                                    bucket,
                                    k
                                );
                            }
                            MetaOp::DelIfExists(k, _miss_kind) => {
                                if !tree_txn.del(k)? {
                                    #[cfg(feature = "metrics")]
                                    missed_stats.push(*_miss_kind);
                                }
                            }
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
                for miss_kind in &missed_stats {
                    let metric = match miss_kind {
                        StatMissKind::Data => CounterMetric::FlushConditionalDataStatPutMiss,
                        StatMissKind::Blob => CounterMetric::FlushConditionalBlobStatPutMiss,
                        StatMissKind::DataDel => CounterMetric::FlushConditionalDataDelMiss,
                        StatMissKind::BlobDel => CounterMetric::FlushConditionalBlobDelMiss,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::test_support::{assert_aborted, is_abort_child, run_child_test};
    use crate::meta::{BUCKET_MISC, Manifest, builder::ManifestBuilder};
    use crate::utils::Handle;
    use std::sync::Arc;
    use std::sync::mpsc::channel;

    fn open_manifest() -> Handle<Manifest> {
        let root = crate::RandomPath::tmp();
        let opt = Arc::new(
            crate::Options::new(&*root)
                .validate()
                .expect("valid options"),
        );
        let (tx, _rx) = channel();
        let (_ack_tx, ack_rx) = channel();
        let mut builder =
            ManifestBuilder::new_with_channels(opt, tx, ack_rx).expect("manifest store must open");
        builder.load().expect("manifest load must work");
        Handle::new(builder.finish().expect("manifest finish must succeed"))
    }

    fn record(manifest: &Manifest, op: MetaOp) {
        let mut txn = manifest.begin();
        txn.ops_mut()
            .entry(BUCKET_MISC.to_string())
            .or_default()
            .push(op);
        txn.commit();
    }

    /// a stat key that two producers may retire must tolerate the second delete:
    /// the tolerant variant commits whether the key was there or not
    #[test]
    fn del_if_exists_tolerates_an_absent_key() {
        let manifest = open_manifest();
        let key = b"tolerant".to_vec();
        record(&manifest, MetaOp::Put(key.clone(), b"v".to_vec()));
        record(
            &manifest,
            MetaOp::DelIfExists(key.clone(), StatMissKind::DataDel),
        );
        assert!(
            manifest
                .btree
                .view(BUCKET_MISC, |txn| txn.get(&key))
                .is_err(),
            "the tolerant delete must remove an existing key"
        );
        // the key is gone: the same op must still commit instead of failing the
        // whole multi-bucket transaction
        record(
            &manifest,
            MetaOp::DelIfExists(key.clone(), StatMissKind::BlobDel),
        );
        manifest.reclaim();
    }

    /// a strict metadata delete whose key is absent breaks a protocol invariant, so it
    /// reports and aborts the process instead of committing a partial transaction
    #[test]
    fn del_on_an_absent_key_aborts() {
        let output = run_child_test("meta::txn::tests::del_on_an_absent_key_child");
        assert_aborted(
            &output,
            &["fatal: must_true failed: meta key must exist on delete"],
        );
    }

    /// the aborting side of the test above: it runs only in the child process
    #[test]
    fn del_on_an_absent_key_child() {
        if !is_abort_child() {
            return;
        }
        let manifest = open_manifest();
        record(&manifest, MetaOp::Del(b"absent".to_vec()));
    }
}
