use super::GarbageCollector;
#[cfg(feature = "metrics")]
use crate::utils::observe::{CounterMetric, EventKind, ObserveEvent};
use crate::{
    OpCode,
    meta::{BUCKET_PENDING_DEL, blob_interval_name, data_interval_name, page_table_name},
};

impl GarbageCollector {
    pub(super) fn process_pending_buckets(&mut self) -> Result<(), OpCode> {
        let Some(bucket_id) = self
            .store
            .manifest
            .catalog
            .next_pending(&self.store.manifest.btree)?
        else {
            return Ok(());
        };

        {
            #[cfg_attr(not(feature = "metrics"), allow(unused_variables))]
            let removed_pages = self.clean_one_bucket(bucket_id)?;
            #[cfg(feature = "metrics")]
            {
                self.store
                    .opt
                    .observer
                    .counter(CounterMetric::GcPendingBucketClean, 1);
                self.store.opt.observer.event(ObserveEvent {
                    kind: EventKind::GcPendingBucketCleaned,
                    bucket_id,
                    txid: 0,
                    file_id: 0,
                    value: removed_pages,
                });
            }
        }
        Ok(())
    }

    fn clean_one_bucket(&mut self, bucket_id: u64) -> Result<u64, OpCode> {
        let bucket_table = page_table_name(bucket_id);
        let data_interval_table = data_interval_name(bucket_id);
        let blob_interval_table = blob_interval_name(bucket_id);

        let removed_pages = self.delete_bucket_batch(&bucket_table)?;
        if removed_pages != 0 {
            #[cfg(feature = "failpoints")]
            crate::utils::failpoint::crash("mace_pending_bucket_reap_after_batch_before_finalize");
            return Ok(removed_pages as u64);
        }
        let removed_data_intervals = self.delete_bucket_batch(&data_interval_table)?;
        if removed_data_intervals != 0 {
            #[cfg(feature = "failpoints")]
            crate::utils::failpoint::crash("mace_pending_bucket_reap_after_batch_before_finalize");
            return Ok(removed_data_intervals as u64);
        }
        let removed_blob_intervals = self.delete_bucket_batch(&blob_interval_table)?;
        if removed_blob_intervals != 0 {
            #[cfg(feature = "failpoints")]
            crate::utils::failpoint::crash("mace_pending_bucket_reap_after_batch_before_finalize");
            return Ok(removed_blob_intervals as u64);
        }

        // table is now empty, destroy the btree bucket and remove pending record
        for bucket in [&bucket_table, &data_interval_table, &blob_interval_table] {
            match self.store.manifest.btree.del_bucket(bucket) {
                Ok(()) | Err(btree_store::Error::BucketNotFound) => {}
                Err(err) => panic!("can't delete btree-store bucket {bucket}: {err:?}"),
            }
        }
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash(
            "mace_pending_bucket_reap_after_finalize_before_meta_commit",
        );
        let mut txn = self.store.manifest.begin();
        txn.del(BUCKET_PENDING_DEL, bucket_id.to_le_bytes().to_vec());
        txn.commit();
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash("mace_pending_bucket_reap_after_manifest_commit");
        // the pending record is durably gone and the physical btrees were destroyed above: this
        // is the only point where the catalog count may drop and the runtime may be retired
        self.store
            .manifest
            .catalog
            .finish_pending(&self.store.manifest.buckets, bucket_id);
        Ok(0)
    }

    fn delete_bucket_batch(&self, bucket: &str) -> Result<usize, OpCode> {
        let mut keys = Vec::with_capacity(Self::MAX_ELEMS);
        match self.store.manifest.btree.view(bucket, |txn| {
            let mut iter = txn.iter();
            let mut k = Vec::new();
            let mut v = Vec::new();
            while iter.next_ref(&mut k, &mut v) && keys.len() < Self::MAX_ELEMS {
                keys.push(k.clone());
            }
            Ok(())
        }) {
            Ok(()) => {}
            // the page-table/interval btree is destroyed before pending-del is
            // committed; a crash in that window leaves this modeled absence
            Err(btree_store::Error::BucketNotFound) => return Ok(0),
            Err(err) => return Err(OpCode::from(err)),
        }

        if keys.is_empty() {
            return Ok(0);
        }

        let mut txn = self.store.manifest.begin();
        for key in &keys {
            txn.del(bucket, key.clone());
        }
        txn.commit();
        Ok(keys.len())
    }
}
