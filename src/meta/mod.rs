use btree_store::BTree;
use parking_lot::{Mutex, RwLock};
use std::{
    collections::{BTreeMap, HashMap},
    hash::Hasher,
    marker::PhantomData,
    ops::Deref,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{
            AtomicBool, AtomicU64,
            Ordering::{AcqRel, Acquire, Relaxed, Release, SeqCst},
        },
    },
};

use crc32c::Crc32cHasher;
use dashmap::DashMap;

use crate::{
    cc::context::Context,
    io::{File, GatherIO},
    map::{
        IFooter, SharedState,
        buffer::{BucketContext, BucketMgr, DataReader},
        data::{BlobFooter, DataFooter, MetaReader},
        flush::FlushObserver,
        table::{BucketState, PageMap},
    },
    types::refbox::BoxRef,
    utils::{
        Handle, MutRef, OpCode,
        bitmap::BitMap,
        data::{LenSeq, Reloc},
        interval::IntervalMap,
        lru::{Lru, ShardLru},
        options::ParsedOptions,
    },
};
use std::sync::mpsc::{Receiver, Sender};

pub(crate) const BUCKET_NUMERICS: &str = "numerics";
pub(crate) const NUMERICS_KEY: &str = "numeric";
pub(crate) const BUCKET_DATA_STAT: &str = "data_stat";
pub(crate) const BUCKET_BLOB_STAT: &str = "blob_stat";
pub(crate) const BUCKET_OBSOLETE_DATA: &str = "obsolete_data";
pub(crate) const BUCKET_OBSOLETE_BLOB: &str = "obsolete_blob";
pub(crate) const BUCKET_METAS: &str = "bucket_metas";
pub(crate) const BUCKET_PENDING_DEL: &str = "pending_del";
pub(crate) const BUCKET_VERSION: &str = "version";
pub(crate) const MAX_BUCKETS: u64 = 1024;
pub(crate) const VERSION_KEY: &str = "current_version";
pub(crate) const ORPHAN_DATA: &str = "orphan_data";
pub(crate) const ORPHAN_BLOB: &str = "orphan_blob";
/// storage format version
pub(crate) const CURRENT_VERSION: u64 = 1;

pub(crate) mod builder;
mod entry;
pub use entry::{
    BlobStat, BlobStatInner, BucketMeta, DataStat, DataStatInner, DelInterval, Delete,
    IntervalPair, MemBlobStat, MemDataStat, MetaKind, Numerics, PageTable,
};

pub(crate) fn page_table_name(bucket_id: u64) -> String {
    format!("page_table_{}", bucket_id)
}

pub(crate) fn data_interval_name(bucket_id: u64) -> String {
    format!("data_interval_{}", bucket_id)
}

pub(crate) fn blob_interval_name(bucket_id: u64) -> String {
    format!("blob_interval_{}", bucket_id)
}

pub(crate) trait IMetaCodec {
    fn packed_size(&self) -> usize;

    fn encode(&self, to: &mut [u8]);

    fn decode(src: &[u8]) -> Self;
}

pub(crate) trait MetaRecord: IMetaCodec {
    fn record(&self, kind: MetaKind, ops: &mut BTreeMap<String, Vec<MetaOp>>);
}

impl MetaRecord for Numerics {
    fn record(&self, _kind: MetaKind, ops: &mut BTreeMap<String, Vec<MetaOp>>) {
        let mut buf = vec![0u8; self.packed_size()];
        self.encode(&mut buf);
        ops.entry(BUCKET_NUMERICS.to_string())
            .or_default()
            .push(MetaOp::Put(NUMERICS_KEY.as_bytes().to_vec(), buf));
    }
}

impl MetaRecord for PageTable {
    fn record(&self, _kind: MetaKind, ops: &mut BTreeMap<String, Vec<MetaOp>>) {
        let bucket_name = page_table_name(self.bucket_id);
        let bucket_ops = ops.entry(bucket_name).or_default();
        for (&pid, &addr) in self.iter() {
            bucket_ops.push(MetaOp::Put(
                pid.to_be_bytes().to_vec(),
                addr.to_be_bytes().to_vec(),
            ));
        }
    }
}

impl MetaRecord for DataStat {
    fn record(&self, kind: MetaKind, ops: &mut BTreeMap<String, Vec<MetaOp>>) {
        let mut buf = vec![0u8; self.packed_size()];
        self.encode(&mut buf);
        let bucket = if kind == MetaKind::DataStat {
            BUCKET_DATA_STAT
        } else {
            BUCKET_BLOB_STAT
        };
        ops.entry(bucket.to_string())
            .or_default()
            .push(MetaOp::Put(self.file_id.to_be_bytes().to_vec(), buf));
    }
}

impl MetaRecord for BlobStat {
    fn record(&self, _kind: MetaKind, ops: &mut BTreeMap<String, Vec<MetaOp>>) {
        let mut buf = vec![0u8; self.packed_size()];
        self.encode(&mut buf);
        ops.entry(BUCKET_BLOB_STAT.to_string())
            .or_default()
            .push(MetaOp::Put(self.file_id.to_be_bytes().to_vec(), buf));
    }
}

impl MetaRecord for IntervalPair {
    fn record(&self, kind: MetaKind, ops: &mut BTreeMap<String, Vec<MetaOp>>) {
        let mut buf = vec![0u8; self.packed_size()];
        self.encode(&mut buf);
        let bucket = if kind == MetaKind::DataInterval {
            data_interval_name(self.bucket_id)
        } else {
            blob_interval_name(self.bucket_id)
        };
        ops.entry(bucket)
            .or_default()
            .push(MetaOp::Put(self.lo_addr.to_be_bytes().to_vec(), buf));
    }
}

impl MetaRecord for Delete {
    fn record(&self, kind: MetaKind, ops: &mut BTreeMap<String, Vec<MetaOp>>) {
        match kind {
            MetaKind::DataDelete | MetaKind::BlobDelete => {
                let (obs_bucket, stat_bucket) = if kind == MetaKind::DataDelete {
                    (BUCKET_OBSOLETE_DATA, BUCKET_DATA_STAT)
                } else {
                    (BUCKET_OBSOLETE_BLOB, BUCKET_BLOB_STAT)
                };
                for &id in self.iter() {
                    let key = id.to_be_bytes().to_vec();
                    ops.entry(obs_bucket.to_string())
                        .or_default()
                        .push(MetaOp::Put(key.clone(), vec![]));
                    ops.entry(stat_bucket.to_string())
                        .or_default()
                        .push(MetaOp::Del(key));
                }
            }
            MetaKind::DataDeleteDone | MetaKind::BlobDeleteDone => {
                let bucket = if kind == MetaKind::DataDeleteDone {
                    BUCKET_OBSOLETE_DATA
                } else {
                    BUCKET_OBSOLETE_BLOB
                };
                let bucket_ops = ops.entry(bucket.to_string()).or_default();
                for &id in self.iter() {
                    bucket_ops.push(MetaOp::Del(id.to_be_bytes().to_vec()));
                }
            }
            _ => unreachable!(),
        }
    }
}

impl MetaRecord for DelInterval {
    fn record(&self, kind: MetaKind, ops: &mut BTreeMap<String, Vec<MetaOp>>) {
        let bucket = if kind == MetaKind::DataDelInterval {
            data_interval_name(self.bucket_id)
        } else {
            blob_interval_name(self.bucket_id)
        };
        let bucket_ops = ops.entry(bucket).or_default();
        for &lo in self.iter() {
            bucket_ops.push(MetaOp::Del(lo.to_be_bytes().to_vec()));
        }
    }
}

#[derive(Clone)]
pub(crate) enum MetaOp {
    Put(Vec<u8>, Vec<u8>),
    Del(Vec<u8>),
}

pub(crate) struct Manifest {
    pub(crate) numerics: Arc<Numerics>,
    pub(crate) data_stat: DataStatCtx,
    pub(crate) blob_stat: BlobStatCtx,
    pub(crate) buckets: Handle<BucketMgr>,
    pub(crate) bucket_metas: DashMap<String, Arc<BucketMeta>>,
    pub(crate) bucket_metas_by_id: DashMap<u64, Arc<BucketMeta>>,
    pub(crate) bucket_states: DashMap<u64, MutRef<BucketState>>,
    pub(crate) structural_lock: Mutex<()>,
    /// total bucket count including both active/pending_del
    pub(crate) nr_buckets: AtomicU64,
    pub(crate) obsolete_data: Mutex<Vec<u64>>,
    pub(crate) obsolete_blob: Mutex<Vec<u64>>,
    pub(crate) opt: Arc<ParsedOptions>,
    pub(crate) btree: BTree,
}

enum BucketRemoveMode {
    Drop,
    Delete,
}

pub(crate) struct Txn<'a> {
    manifest: &'a Manifest,
    btree: BTree,
    // bucket_name -> operations
    ops: BTreeMap<String, Vec<MetaOp>>,
}

impl<'a> Txn<'a> {
    pub(crate) fn ops_mut(&mut self) -> &mut BTreeMap<String, Vec<MetaOp>> {
        &mut self.ops
    }

    pub(crate) fn commit(&mut self) {
        if self.ops.is_empty() {
            return;
        }
        loop {
            // btree-store supports SI. refresh handle to start a fresh session
            self.btree = self.manifest.btree.clone();

            // perform an atomic multi-bucket commit
            // all updates across different buckets are applied and flushed to disk
            // in a single SuperBlock write, significantly reducing I/O overhead
            let res = self.btree.exec_multi(|multi_txn| {
                for (bucket, bucket_ops) in &self.ops {
                    multi_txn.execute(bucket, |tree_txn| {
                        for op in bucket_ops {
                            match op {
                                MetaOp::Put(k, v) => tree_txn.put(k, v)?,
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
}

struct FileReader {
    file: File,
    map: HashMap<u64, Reloc>,
}

fn new_reader<T: IFooter>(path: PathBuf) -> Result<FileReader, OpCode> {
    let mut loader = MetaReader::<T>::new(&path)?;
    let relocs = loader.get_reloc()?;
    let mut map = HashMap::with_capacity(relocs.len());
    for x in relocs {
        map.insert(x.key, x.val);
    }

    let file = loader.take();
    Ok(FileReader { file, map })
}

impl FileReader {
    fn read_at(&self, pos: u64) -> Result<BoxRef, OpCode> {
        let m = self.map.get(&pos).expect("never happen");
        let real_size = BoxRef::real_size_from_dump(m.len);
        let mut p = BoxRef::alloc_exact(real_size, pos);
        let mut crc = Crc32cHasher::default();

        let dst = p.load_slice();
        self.file.read(dst, m.off as u64).map_err(OpCode::from)?;
        crc.write(dst);
        let actual_crc = crc.finish() as u32;
        if actual_crc != m.crc {
            log::error!(
                "checksum mismatch, expect {} get {}, key {pos}",
                { m.crc },
                actual_crc
            );
            return Err(OpCode::Corruption);
        }

        Ok(p)
    }
}

impl Drop for Manifest {
    fn drop(&mut self) {
        self.buckets.reclaim();
    }
}

impl Manifest {
    pub(crate) fn new(opt: Arc<ParsedOptions>, tx: Sender<SharedState>, rx: Receiver<()>) -> Self {
        let path = opt.manifest();
        let btree = BTree::open(path).expect("can't open btree-store");
        let buckets_to_ensure = [
            BUCKET_NUMERICS,
            BUCKET_DATA_STAT,
            BUCKET_BLOB_STAT,
            BUCKET_OBSOLETE_DATA,
            BUCKET_OBSOLETE_BLOB,
            BUCKET_METAS,
            BUCKET_PENDING_DEL,
            BUCKET_VERSION,
        ];

        for name in buckets_to_ensure {
            btree
                .exec(name, |_| Ok(()))
                .expect("can't ensure bucket exists");
        }

        Self {
            numerics: Arc::new(Numerics::default()),
            data_stat: DataStatCtx::new(opt.clone()),
            blob_stat: BlobStatCtx::new(opt.clone()),
            buckets: Handle::new(BucketMgr::new(
                opt.clone(),
                Handle::from(std::ptr::null_mut()),
                tx,
                rx,
            )),
            bucket_metas: DashMap::new(),
            bucket_metas_by_id: DashMap::new(),
            bucket_states: DashMap::new(),
            structural_lock: Mutex::new(()),
            obsolete_data: Mutex::new(Vec::new()),
            obsolete_blob: Mutex::new(Vec::new()),
            nr_buckets: AtomicU64::new(0),
            opt,
            btree,
        }
    }

    pub(crate) fn set_context(
        &self,
        ctx: Handle<Context>,
        reader: Arc<dyn DataReader>,
        observer: Arc<dyn FlushObserver>,
    ) {
        unsafe {
            let mgr = &mut *self.buckets.inner();
            mgr.start(ctx, reader, observer);
        }
    }

    fn load_bucket_meta_locked(&self, name: &str) -> Result<Arc<BucketMeta>, OpCode> {
        if let Some(meta) = self.bucket_metas.get(name) {
            return Ok(meta.clone());
        }

        let mut meta = None;
        let _ = self.btree.view(BUCKET_METAS, |txn| {
            if let Ok(v) = txn.get(name.as_bytes()) {
                meta = Some(Arc::new(BucketMeta::decode(&v)));
            }
            Ok(())
        });

        let meta = meta.ok_or(OpCode::NotFound)?;
        self.bucket_metas.insert(name.to_string(), meta.clone());
        self.bucket_metas_by_id.insert(meta.bucket_id, meta.clone());
        Ok(meta)
    }

    pub(crate) fn load_bucket_meta(&self, name: &str) -> Result<Arc<BucketMeta>, OpCode> {
        if let Some(meta) = self.bucket_metas.get(name) {
            return Ok(meta.clone());
        }

        let _lock = self.structural_lock.lock();
        self.load_bucket_meta_locked(name)
    }

    pub(crate) fn create_bucket(
        &self,
        name: &str,
    ) -> Result<(Arc<BucketMeta>, Arc<BucketContext>), OpCode> {
        let _lock = self.structural_lock.lock();

        match self.load_bucket_meta_locked(name) {
            Ok(_) => return Err(OpCode::Exist),
            Err(OpCode::NotFound) => {}
            Err(e) => return Err(e),
        }

        if self.nr_buckets.load(Relaxed) >= MAX_BUCKETS {
            return Err(OpCode::NoSpace);
        }

        let bucket_id = self.numerics.next_bucket_id.fetch_add(1, Relaxed);
        self.nr_buckets.fetch_add(1, Relaxed);

        let meta = Arc::new(BucketMeta { bucket_id });
        // publish meta early so context creation sees it
        self.bucket_metas.insert(name.to_string(), meta.clone());
        self.bucket_metas_by_id.insert(bucket_id, meta.clone());

        // ensure state and pagemap are initialized
        let bucket_ctx = self.load_bucket_context_locked(bucket_id);

        let mut buf = vec![0u8; meta.as_ref().packed_size()];
        let mut txn = self.begin();
        txn.record(MetaKind::Numerics, self.numerics.as_ref());
        meta.as_ref().encode(&mut buf);
        txn.ops_mut()
            .entry(BUCKET_METAS.to_string())
            .or_default()
            .push(MetaOp::Put(name.as_bytes().to_vec(), buf));
        txn.commit();

        Ok((meta, bucket_ctx))
    }

    pub(crate) fn load_bucket_context(&self, bucket_id: u64) -> Result<Arc<BucketContext>, OpCode> {
        if let Some(ctx) = self.buckets.buckets.get(&bucket_id) {
            return Ok(ctx.value().clone());
        }

        let _lock = self.structural_lock.lock();
        if let Some(ctx) = self.buckets.buckets.get(&bucket_id) {
            return Ok(ctx.value().clone());
        }
        if !self.bucket_metas_by_id.contains_key(&bucket_id) {
            return Err(OpCode::NotFound);
        }
        Ok(self.load_bucket_context_locked(bucket_id))
    }

    pub(crate) fn get_bucket_context_must_exist(&self, bucket_id: u64) -> Arc<BucketContext> {
        self.buckets
            .buckets
            .get(&bucket_id)
            .expect("must exist")
            .value()
            .clone()
    }

    fn load_bucket_context_locked(&self, bucket_id: u64) -> Arc<BucketContext> {
        // double check
        if let Some(ctx) = self.buckets.buckets.get(&bucket_id) {
            return ctx.value().clone();
        }

        if !self.bucket_metas_by_id.contains_key(&bucket_id) {
            log::error!("bucket {} missing metadata when loading context", bucket_id);
            panic!("bucket {bucket_id} not found");
        }

        let state = self.ensure_bucket_state(bucket_id);

        // PageMap lazy load
        let table = {
            let table = MutRef::new(PageMap::default());
            if let Some(_meta) = self.bucket_metas_by_id.get(&bucket_id) {
                table.recover(bucket_id, Some(&self.btree));
            }
            table
        };

        let mut data_ivls = RwLock::new(IntervalMap::new());
        let mut blob_ivls = RwLock::new(IntervalMap::new());
        let next_addr = self.recover_intervals(bucket_id, &data_ivls, &blob_ivls);
        state.next_addr.fetch_max(next_addr, Relaxed);

        let flush = self
            .buckets
            .flush
            .as_ref()
            .expect("flusher started")
            .clone();
        let ctx = Arc::new(BucketContext::new(
            self.buckets.ctx,
            state,
            bucket_id,
            next_addr,
            table,
            flush,
            self.buckets.lru,
            self.buckets.reader.clone(),
            self.buckets.used.clone(),
            self.buckets.tx.clone(),
        ));

        // Inject recovered intervals
        {
            let mut dst = ctx.data_intervals.write();
            *dst = std::mem::take(data_ivls.get_mut());
        }
        {
            let mut dst = ctx.blob_intervals.write();
            *dst = std::mem::take(blob_ivls.get_mut());
        }

        self.buckets.buckets.insert(bucket_id, ctx.clone());
        ctx
    }

    fn get_bucket_state(&self, bucket_id: u64) -> MutRef<BucketState> {
        self.bucket_states
            .get(&bucket_id)
            .map(|state| state.value().clone())
            .expect("must exist")
    }

    // bucket state is in-memory only and can be dropped during unload/delete or after restart
    // context loading must recreate it on first touch, while repeated loads reuse the existing state
    fn ensure_bucket_state(&self, bucket_id: u64) -> MutRef<BucketState> {
        self.bucket_states
            .entry(bucket_id)
            .or_insert_with(|| MutRef::new(BucketState::new()))
            .value()
            .clone()
    }

    fn begin_bucket_remove_locked(
        &self,
        name: &str,
        mode: BucketRemoveMode,
    ) -> Result<u64, OpCode> {
        let meta = self.load_bucket_meta_locked(name)?;
        let bucket_id = meta.bucket_id;

        // remove from maps (unpublish)
        self.bucket_metas.remove(name);
        self.bucket_metas_by_id.remove(&bucket_id);

        let state = self.get_bucket_state(bucket_id);
        let mut busy = Arc::strong_count(&meta) > 1 || state.is_busy();
        if matches!(mode, BucketRemoveMode::Delete) && state.is_drop() {
            busy = true;
        }
        if busy {
            self.bucket_metas.insert(name.to_string(), meta.clone());
            self.bucket_metas_by_id.insert(bucket_id, meta);
            return Err(OpCode::Again);
        }

        match mode {
            BucketRemoveMode::Drop => state.set_drop(),
            BucketRemoveMode::Delete => state.set_deleting(),
        }

        Ok(bucket_id)
    }

    pub(crate) fn unload_bucket(&self, name: &str) -> Result<(), OpCode> {
        // serialize deletions and creations
        let _lock = self.structural_lock.lock();

        // remove from maps (unpublish)
        let bucket_id = self.begin_bucket_remove_locked(name, BucketRemoveMode::Drop)?;

        if let Some(ctx) = self
            .buckets
            .buckets
            .get(&bucket_id)
            .map(|x| x.value().clone())
        {
            ctx.flush_and_wait();
            let _ = self.buckets.buckets.remove(&bucket_id);
            ctx.reclaim();
        }

        self.bucket_states.remove(&bucket_id);
        Ok(())
    }

    pub(crate) fn delete_bucket(&self, name: &str) -> Result<(), OpCode> {
        // serialize deletions and creations
        let _lock = self.structural_lock.lock();

        // remove from maps (unpublish)
        let bucket_id = self.begin_bucket_remove_locked(name, BucketRemoveMode::Delete)?;

        // cleanup page maps and resources via manager
        self.buckets.del_bucket(bucket_id);
        self.bucket_states.remove(&bucket_id);

        // collect and record obsolete files
        let data_files = self
            .data_stat
            .bucket_files()
            .get(&bucket_id)
            .map(|x| x.clone())
            .unwrap_or_default();
        let blob_files = self
            .blob_stat
            .bucket_files()
            .get(&bucket_id)
            .map(|x| x.clone())
            .unwrap_or_default();

        // commit transaction (logical delete)
        let mut txn = self.begin();

        // remove from BUCKET_METAS
        txn.ops_mut()
            .entry(BUCKET_METAS.to_string())
            .or_default()
            .push(MetaOp::Del(name.as_bytes().to_vec()));

        // record for physical cleanup in background
        txn.ops_mut()
            .entry(BUCKET_PENDING_DEL.to_string())
            .or_default()
            .push(MetaOp::Put(bucket_id.to_be_bytes().to_vec(), vec![]));

        // record obsolete files
        if !data_files.is_empty() {
            txn.record(MetaKind::DataDelete, &Delete::from(data_files.clone()));
        }
        if !blob_files.is_empty() {
            txn.record(MetaKind::BlobDelete, &Delete::from(blob_files.clone()));
        }

        txn.commit();

        // in-memory cleanup for stats
        // remove from stat caches to release file handles
        for &id in &data_files {
            self.data_stat.remove_cache(id);
        }
        for &id in &blob_files {
            self.blob_stat.remove_cache(id);
        }

        // add to in-memory obsolete list for physical deletion by GC
        self.save_obsolete_data(&data_files);
        self.save_obsolete_blob(&blob_files);
        self.delete_files();

        Ok(())
    }

    pub(crate) fn loaded_bucket_names(&self) -> Vec<String> {
        self.bucket_metas.iter().map(|x| x.key().clone()).collect()
    }

    pub(crate) fn save_obsolete_blob(&self, id: &[u64]) {
        if !id.is_empty() {
            let mut lk = self.obsolete_blob.lock();
            lk.extend_from_slice(id);
        }
    }

    pub(crate) fn add_data_stat(&self, stat: MemDataStat, ivl: IntervalPair) {
        if let Some(ctx) = self.buckets.buckets.get(&stat.bucket_id) {
            ctx.data_intervals
                .write()
                .insert(ivl.lo_addr, ivl.hi_addr, ivl.file_id);
        }
        self.data_stat.add_stat_unlocked(stat);
    }

    pub(crate) fn add_blob_stat(&self, stat: MemBlobStat, ivl: IntervalPair) {
        if let Some(ctx) = self.buckets.buckets.get(&stat.bucket_id) {
            ctx.blob_intervals
                .write()
                .insert(ivl.lo_addr, ivl.hi_addr, ivl.file_id);
        }
        self.blob_stat.add_stat_unlocked(stat);
    }

    pub(crate) fn update_data_stat_interval(
        &self,
        fstat: MemDataStat,
        relocs: HashMap<u64, LenSeq>,
        obsoleted: &[u64],
        del_intervals: &[u64],
        remap_intervals: &[IntervalPair],
    ) -> DataStat {
        let bucket_id = fstat.bucket_id;
        if let Some(ctx) = self.buckets.buckets.get(&bucket_id) {
            let mut lk = ctx.data_intervals.write();
            for &lo in del_intervals {
                lk.remove(lo);
            }
            for i in remap_intervals {
                lk.update(i.lo_addr, i.hi_addr, i.file_id);
            }
        }

        self.data_stat
            .update_stat_interval(fstat, relocs, obsoleted)
    }

    pub(crate) fn update_blob_stat_interval(
        &self,
        bstat: MemBlobStat,
        relocs: HashMap<u64, LenSeq>,
        obsoleted: &[u64],
        del_intervals: &[u64],
        remap_intervals: &[IntervalPair],
    ) -> BlobStat {
        let bucket_id = bstat.bucket_id;
        if let Some(ctx) = self.buckets.buckets.get(&bucket_id) {
            let mut lk = ctx.blob_intervals.write();
            for &lo in del_intervals {
                lk.remove(lo);
            }
            for i in remap_intervals {
                lk.update(i.lo_addr, i.hi_addr, i.file_id);
            }
        }

        self.blob_stat
            .update_stat_interval(bstat, relocs, obsoleted)
    }

    pub(crate) fn apply_data_junks(
        &self,
        bucket_id: u64,
        tick: u64,
        junks: &[u64],
    ) -> Vec<DataStat> {
        let ctx = self.get_bucket_context_must_exist(bucket_id);
        self.data_stat.apply_junks(tick, junks, &ctx, &self.btree)
    }

    pub(crate) fn apply_blob_junks(&self, bucket_id: u64, junks: &[u64]) -> Vec<BlobStat> {
        let ctx = self.get_bucket_context_must_exist(bucket_id);
        self.blob_stat.apply_junks(junks, &ctx, &self.btree)
    }

    pub(crate) fn begin(&self) -> Txn<'_> {
        let btree = self.btree.clone();
        Txn {
            manifest: self,
            btree,
            ops: BTreeMap::new(),
        }
    }

    pub(crate) fn load_data<C>(&self, bucket_id: u64, addr: u64, cache: C) -> Result<BoxRef, OpCode>
    where
        C: Fn(BoxRef),
    {
        let ctx = self.get_bucket_context_must_exist(bucket_id);
        match self.data_stat.load(addr, &ctx) {
            Ok(b) => {
                cache(b.clone());
                Ok(b)
            }
            e => e,
        }
    }

    pub(crate) fn load_blob<C>(&self, bucket_id: u64, addr: u64, cache: C) -> Result<BoxRef, OpCode>
    where
        C: Fn(BoxRef),
    {
        let ctx = self.get_bucket_context_must_exist(bucket_id);
        match self.blob_stat.load(addr, &ctx) {
            Ok(b) => {
                cache(b.clone());
                Ok(b)
            }
            e => e,
        }
    }

    pub(crate) fn load_blob_uncached(&self, bucket_id: u64, addr: u64) -> Result<BoxRef, OpCode> {
        let ctx = self.get_bucket_context_must_exist(bucket_id);
        self.blob_stat.load(addr, &ctx)
    }

    pub(crate) fn save_obsolete_data(&self, id: &[u64]) {
        if !id.is_empty() {
            let mut lk = self.obsolete_data.lock();
            lk.extend_from_slice(id);
        }
    }

    pub(crate) fn delete_files(&self) {
        let mut data_ids = Vec::new();
        {
            let mut lk = self.obsolete_data.lock();
            lk.retain(|&id| {
                let path = self.opt.data_file(id);
                if !path.exists() || std::fs::remove_file(&path).is_ok() {
                    data_ids.push(id);
                    false
                } else {
                    true
                }
            });
        }
        let mut blob_ids = Vec::new();
        {
            let mut lk = self.obsolete_blob.lock();
            lk.retain(|&id| {
                let path = self.opt.blob_file(id);
                if !path.exists() || std::fs::remove_file(&path).is_ok() {
                    blob_ids.push(id);
                    false
                } else {
                    true
                }
            });
        }

        if !data_ids.is_empty() || !blob_ids.is_empty() {
            #[cfg(unix)]
            if let Ok(f) = std::fs::File::open(self.opt.data_root()) {
                let _ = f.sync_all();
            }

            let mut txn = self.begin();
            if !data_ids.is_empty() {
                txn.record(MetaKind::DataDeleteDone, &Delete::from(data_ids));
            }
            if !blob_ids.is_empty() {
                txn.record(MetaKind::BlobDeleteDone, &Delete::from(blob_ids));
            }
            txn.commit();
        }
    }

    pub(crate) fn recover_intervals(
        &self,
        bucket_id: u64,
        data_ivls: &RwLock<IntervalMap>,
        blob_ivls: &RwLock<IntervalMap>,
    ) -> u64 {
        let mut max_addr = crate::utils::INIT_ADDR;

        let data_ivl_table = data_interval_name(bucket_id);
        let _ = self.btree.view(&data_ivl_table, |txn| {
            let mut iter = txn.iter();
            let mut k = Vec::new();
            let mut v = Vec::new();
            let mut map = data_ivls.write();
            while iter.next_ref(&mut k, &mut v) {
                let ivl = IntervalPair::decode(&v);
                max_addr = max_addr.max(ivl.hi_addr);
                map.upsert(ivl.lo_addr, ivl.hi_addr, ivl.file_id);
            }
            Ok(())
        });

        let blob_ivl_table = blob_interval_name(bucket_id);
        let _ = self.btree.view(&blob_ivl_table, |txn| {
            let mut iter = txn.iter();
            let mut k = Vec::new();
            let mut v = Vec::new();
            let mut map = blob_ivls.write();
            while iter.next_ref(&mut k, &mut v) {
                let ivl = IntervalPair::decode(&v);
                max_addr = max_addr.max(ivl.hi_addr);
                map.upsert(ivl.lo_addr, ivl.hi_addr, ivl.file_id);
            }
            Ok(())
        });

        max_addr + 1
    }
}

pub(crate) struct DataKind;
pub(crate) struct BlobKind;

pub(crate) trait StatKind {
    type Footer: IFooter;

    fn file_path(opt: &ParsedOptions, file_id: u64) -> PathBuf;
    fn intervals(ctx: &BucketContext) -> &RwLock<IntervalMap>;
}

impl StatKind for DataKind {
    type Footer = DataFooter;

    fn file_path(opt: &ParsedOptions, file_id: u64) -> PathBuf {
        opt.data_file(file_id)
    }

    fn intervals(ctx: &BucketContext) -> &RwLock<IntervalMap> {
        &ctx.data_intervals
    }
}

impl StatKind for BlobKind {
    type Footer = BlobFooter;

    fn file_path(opt: &ParsedOptions, file_id: u64) -> PathBuf {
        opt.blob_file(file_id)
    }

    fn intervals(ctx: &BucketContext) -> &RwLock<IntervalMap> {
        &ctx.blob_intervals
    }
}

pub(crate) struct StatCtx<K, M> {
    map: M,
    common: StatCommon,
    total_size: AtomicU64,
    active_size: AtomicU64,
    _kind: PhantomData<K>,
}

pub(crate) type DataStatCtx = StatCtx<DataKind, DashMap<u64, MemDataStat>>;
pub(crate) type BlobStatCtx = StatCtx<BlobKind, RwLock<BTreeMap<u64, MemBlobStat>>>;

impl<K, M> StatCtx<K, M>
where
    K: StatKind,
{
    pub(crate) fn bucket_files(&self) -> &DashMap<u64, Vec<u64>> {
        &self.common.bucket_files
    }

    pub(crate) fn remove_cache(&self, file_id: u64) {
        self.common.cache.del(file_id);
    }

    pub(crate) fn start_collect_junks(&self) {
        // Release is enough for ARM, but it's no-op on x86, so use SeqCst instead
        self.common.junk.start();
    }

    fn load(&self, addr: u64, ctx: &BucketContext) -> Result<BoxRef, OpCode> {
        let file_id = {
            let ivl_map = K::intervals(ctx).read();
            ivl_map.find(addr).expect("must exist")
        };

        loop {
            if let Some(r) = self.common.cache.get(file_id) {
                return r.read_at(addr);
            }

            let lk = self.common.cache.lock_shard(file_id);
            lk.add_if_missing(|| new_reader::<K::Footer>(K::file_path(&self.common.opt, file_id)))?;
        }
    }

    fn get_reloc(&self, file_id: u64, pos: u64) -> Reloc {
        loop {
            if let Some(x) = self.common.cache.get(file_id) {
                let Some(&tmp) = x.map.get(&pos) else {
                    log::error!("can't find reloc, file_id {file_id} key {pos}");
                    unreachable!("can't find reloc, file_id {file_id} key {pos}");
                };
                return tmp;
            }

            let lk = self.common.cache.lock_shard(file_id);
            lk.add_if_missing(|| new_reader::<K::Footer>(K::file_path(&self.common.opt, file_id)))
                .expect("can't fail");
        }
    }
}

struct JunkCollector {
    should_collect_junk: AtomicBool,
    junks: Mutex<HashMap<u64, Vec<u64>>>,
}

struct StatCommon {
    pub(crate) bucket_files: DashMap<u64, Vec<u64>>,
    cache: ShardLru<FileReader>,
    mask_cache: Lru<u64, ()>,
    mask_capacity: usize,
    opt: Arc<ParsedOptions>,
    junk: JunkCollector,
}

impl JunkCollector {
    fn new() -> Self {
        Self {
            should_collect_junk: AtomicBool::new(false),
            junks: Mutex::new(HashMap::new()),
        }
    }

    fn start(&self) {
        self.should_collect_junk.store(true, SeqCst);
    }

    fn stop(&self) {
        self.should_collect_junk.store(false, SeqCst);
    }

    fn take(&self) -> HashMap<u64, Vec<u64>> {
        let mut junklk = self.junks.lock();
        self.stop();
        std::mem::take(&mut *junklk)
    }

    fn push_if_collecting(&self, file_id: u64, junk: u64) {
        let mut m = self.junks.lock();
        #[allow(clippy::collapsible_if)]
        if self.should_collect_junk.load(Acquire) {
            if let Some(q) = m.get_mut(&file_id) {
                q.push(junk);
            }
        }
    }
}

impl StatCommon {
    fn new(opt: Arc<ParsedOptions>, cache_capacity: usize, mask_capacity: usize) -> Self {
        Self {
            bucket_files: DashMap::new(),
            cache: ShardLru::new(cache_capacity),
            mask_cache: Lru::new(),
            mask_capacity,
            opt,
            junk: JunkCollector::new(),
        }
    }
}

impl Deref for StatCtx<DataKind, DashMap<u64, MemDataStat>> {
    type Target = DashMap<u64, MemDataStat>;
    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl StatCtx<DataKind, DashMap<u64, MemDataStat>> {
    fn new(opt: Arc<ParsedOptions>) -> Self {
        Self {
            map: DashMap::new(),
            common: StatCommon::new(
                opt.clone(),
                opt.data_handle_cache_capacity,
                opt.stat_mask_cache_count,
            ),
            total_size: AtomicU64::new(0),
            active_size: AtomicU64::new(0),
            _kind: PhantomData,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn total_active(&self) -> (u64, u64) {
        let mut total = 0;
        let mut active = 0;
        self.iter().for_each(|x| {
            let s = x.value();
            total += s.total_size as u64;
            active += s.active_size as u64;
        });
        (total, active)
    }

    pub(crate) fn update_size(&self, active_size: u64, total_size: u64) {
        self.active_size.fetch_add(active_size, Relaxed);
        self.total_size.fetch_add(total_size, Relaxed);
    }

    fn load_mask_from_btree(
        &self,
        file_id: u64,
        total_elems: u32,
        btree: &BTree,
    ) -> Result<BitMap, OpCode> {
        let mut buf = None;
        let _ = btree.view(BUCKET_DATA_STAT, |txn| {
            if let Ok(v) = txn.get(file_id.to_be_bytes()) {
                buf = Some(v);
            }
            Ok(())
        });
        let buf = buf.ok_or(OpCode::NotFound)?;
        Ok(DataStat::decode_mask_only(&buf, total_elems))
    }

    fn record_mask_use(&self, file_id: u64) {
        if self.common.mask_cache.get(&file_id).is_some() {
            return;
        }
        if let Some((evicted_id, _)) =
            self.common
                .mask_cache
                .add_with_evict(self.common.mask_capacity, file_id, ())
            && evicted_id != file_id
            && let Some(mut stat) = self.map.get_mut(&evicted_id)
        {
            stat.mask = None;
        }
    }

    pub(crate) fn ensure_mask(&self, file_id: u64, btree: &BTree) -> Result<(), OpCode> {
        let total_elems = {
            let stat = self.map.get(&file_id).ok_or(OpCode::NotFound)?;
            if stat.mask.is_some() {
                drop(stat);
                self.record_mask_use(file_id);
                return Ok(());
            }
            stat.total_elems
        };

        let mask = self.load_mask_from_btree(file_id, total_elems, btree)?;
        let mut loaded = false;
        if let Some(mut stat) = self.map.get_mut(&file_id) {
            if stat.mask.is_none() {
                stat.mask = Some(mask);
            }
            loaded = true;
        }
        if loaded {
            self.record_mask_use(file_id);
            Ok(())
        } else {
            Err(OpCode::NotFound)
        }
    }

    pub(crate) fn load_mask_clone(&self, file_id: u64, btree: &BTree) -> Result<BitMap, OpCode> {
        self.ensure_mask(file_id, btree)?;
        let stat = self.map.get(&file_id).ok_or(OpCode::NotFound)?;
        Ok(stat.mask.as_ref().expect("mask loaded").clone())
    }

    pub(crate) fn add_stat_unlocked(&self, stat: MemDataStat) {
        assert_eq!(stat.active_size, stat.total_size);
        self.update_size(stat.active_size as u64, stat.total_size as u64);
        {
            self.common
                .bucket_files
                .entry(stat.bucket_id)
                .or_default()
                .push(stat.file_id);
        }
        let file_id = stat.file_id;
        let has_mask = stat.mask.is_some();
        let r = self.map.insert(file_id, stat);
        assert!(r.is_none());
        if has_mask {
            self.record_mask_use(file_id);
        }
    }

    fn update_stat_interval(
        &self,
        mut fstat: MemDataStat,
        relocs: HashMap<u64, LenSeq>,
        obsoleted: &[u64], // no longer referenced
    ) -> DataStat {
        assert_eq!(fstat.active_size, fstat.total_size);

        // apply deactived frames while we are performing compaction
        let mut seqs = vec![];
        let mut junks = self.common.junk.take();
        for (_, q) in junks.iter_mut() {
            for &addr in q.iter() {
                if let Some(ls) = relocs.get(&addr) {
                    fstat.active_size -= ls.len as usize;
                    fstat.active_elems -= 1;
                    fstat.mask.as_mut().expect("mask loaded").set(ls.seq);
                    seqs.push(ls.seq);
                }
            }
        }

        let stat = DataStat {
            inner: fstat.inner,
            inactive_elems: seqs,
        };

        for &id in obsoleted {
            self.remove_stat(id);
            self.common.cache.del(id);
        }

        self.add_stat_unlocked(fstat);
        stat
    }

    pub(crate) fn remove_stat_interval(&self, data: &[u64]) {
        for x in data {
            self.remove_stat(*x);
        }
    }

    pub(crate) fn remove_stat(&self, file_id: u64) {
        if let Some((_, v)) = self.map.remove(&file_id) {
            self.common.mask_cache.del(&file_id);
            self.decrease(v.active_size as u64, v.total_size as u64);
            #[allow(clippy::collapsible_if)]
            if let Some(mut files) = self.common.bucket_files.get_mut(&v.bucket_id) {
                if let Some(pos) = files.iter().position(|&x| x == file_id) {
                    files.swap_remove(pos);
                }
            }
        }
    }

    pub(crate) fn apply_junks(
        &self,
        tick: u64,
        junks: &[u64],
        ctx: &BucketContext,
        btree: &BTree,
    ) -> Vec<DataStat> {
        let candidates: Vec<(u64, u64)> = {
            let lk = ctx.data_intervals.read();
            junks
                .iter()
                .filter_map(|&addr| {
                    // race condition: gc might have already removed the interval containing this junk
                    // 1. flush thread holds a junk addr
                    // 2. gc thread rewrites the file containing addr, and since it is junk, it is not moved
                    // 3. gc thread removes the interval from interval map if it becomes empty
                    // 4. flush thread tries to find the file_id of the junk, but the interval is gone
                    let file_id = lk.find(addr)?;
                    Some((addr, file_id))
                })
                .collect()
        };

        let mut v: Vec<DataStat> = Vec::with_capacity(junks.len());
        for (addr, file_id) in candidates {
            if self.ensure_mask(file_id, btree).is_err() {
                continue;
            }
            if let Some(mut stat) = self.map.get_mut(&file_id) {
                let reloc = self.get_reloc(file_id, addr);
                self.update_stat(&mut stat, addr, &reloc, tick);
                if let Some(b) = v.last_mut()
                    && b.inner.file_id == file_id
                {
                    b.inner = stat.inner;
                    b.inactive_elems.push(reloc.seq);
                } else {
                    v.push(DataStat {
                        inner: stat.inner,
                        inactive_elems: vec![reloc.seq],
                    });
                }
            }
        }
        v
    }

    pub(crate) fn update_stat(&self, stat: &mut MemDataStat, junk: u64, reloc: &Reloc, tick: u64) {
        self.active_size.fetch_sub(reloc.len as u64, Release);
        stat.update(tick, reloc);
        self.common.junk.push_if_collecting(stat.file_id, junk);
    }

    fn decrease(&self, active_size: u64, total_size: u64) {
        let old = self.active_size.fetch_sub(active_size, AcqRel);
        assert!(old >= active_size);

        let old = self.total_size.fetch_sub(total_size, AcqRel);
        assert!(old >= total_size);
    }
}

impl Deref for StatCtx<BlobKind, RwLock<BTreeMap<u64, MemBlobStat>>> {
    type Target = RwLock<BTreeMap<u64, MemBlobStat>>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl StatCtx<BlobKind, RwLock<BTreeMap<u64, MemBlobStat>>> {
    fn new(opt: Arc<ParsedOptions>) -> Self {
        Self {
            map: RwLock::new(BTreeMap::new()),
            common: StatCommon::new(
                opt.clone(),
                opt.blob_handle_cache_capacity,
                opt.stat_mask_cache_count,
            ),
            total_size: AtomicU64::new(0),
            active_size: AtomicU64::new(0),
            _kind: PhantomData,
        }
    }

    fn load_mask_from_btree(
        &self,
        file_id: u64,
        total_elems: u32,
        btree: &BTree,
    ) -> Result<BitMap, OpCode> {
        let mut buf = None;
        let _ = btree.view(BUCKET_BLOB_STAT, |txn| {
            if let Ok(v) = txn.get(file_id.to_be_bytes()) {
                buf = Some(v);
            }
            Ok(())
        });
        let buf = buf.ok_or(OpCode::NotFound)?;
        Ok(BlobStat::decode_mask_only(&buf, total_elems))
    }

    fn record_mask_use(&self, file_id: u64) {
        if self.common.mask_cache.get(&file_id).is_some() {
            return;
        }
        if let Some((evicted_id, _)) =
            self.common
                .mask_cache
                .add_with_evict(self.common.mask_capacity, file_id, ())
            && evicted_id != file_id
        {
            let mut map = self.map.write();
            if let Some(stat) = map.get_mut(&evicted_id) {
                stat.mask = None;
            }
        }
    }

    pub(crate) fn ensure_mask(&self, file_id: u64, btree: &BTree) -> Result<(), OpCode> {
        let total_elems = {
            let map = self.map.read();
            let stat = map.get(&file_id).ok_or(OpCode::NotFound)?;
            if stat.mask.is_some() {
                drop(map);
                self.record_mask_use(file_id);
                return Ok(());
            }
            stat.nr_total
        };

        let mask = self.load_mask_from_btree(file_id, total_elems, btree)?;
        {
            let mut map = self.map.write();
            if let Some(stat) = map.get_mut(&file_id) {
                if stat.mask.is_none() {
                    stat.mask = Some(mask);
                }
                drop(map);
                self.record_mask_use(file_id);
                return Ok(());
            }
        }
        Err(OpCode::NotFound)
    }

    pub(crate) fn load_mask_clone(&self, file_id: u64, btree: &BTree) -> Result<BitMap, OpCode> {
        self.ensure_mask(file_id, btree)?;
        let map = self.map.read();
        let stat = map.get(&file_id).ok_or(OpCode::NotFound)?;
        Ok(stat.mask.as_ref().expect("mask loaded").clone())
    }

    pub(crate) fn get_victims(
        &self,
        file_ratio: usize,
        garbage_ratio: usize,
    ) -> (Vec<u64>, Vec<(u64, usize, u64)>) {
        let lk = self.map.read();
        let n = lk.len() * file_ratio / 100;
        let mut v = Vec::new();
        let mut o = Vec::new();
        let mut nr_total = 0;
        let mut nr_active = 0;

        for (_, x) in lk.iter().take(n) {
            if x.nr_active == 0 {
                o.push(x.file_id);
            } else {
                nr_total += x.nr_total;
                nr_active += x.nr_active;
                v.push((x.file_id, x.active_size, x.bucket_id));
            }
        }
        if nr_total > 0 {
            assert!(!v.is_empty());
            let ratio = (nr_total - nr_active) * 100 / nr_total;
            if (ratio as usize) < garbage_ratio {
                v.clear();
            }
        }
        (o, v)
    }

    pub(crate) fn remove_stat_interval(&self, blobs: &[u64]) {
        let mut lk_map = self.map.write();
        for x in blobs {
            #[allow(clippy::collapsible_if)]
            if let Some(v) = lk_map.remove(x) {
                self.common.mask_cache.del(x);
                if let Some(mut files) = self.common.bucket_files.get_mut(&v.bucket_id) {
                    if let Some(pos) = files.iter().position(|&id| id == *x) {
                        files.swap_remove(pos);
                    }
                }
            }
        }
        drop(lk_map);
    }

    fn update_stat_interval(
        &self,
        mut bstat: MemBlobStat,
        relocs: HashMap<u64, LenSeq>,
        obsoleted: &[u64],
    ) -> BlobStat {
        let mut seqs = vec![];
        let mut junks = self.common.junk.take();

        for (_, q) in junks.iter_mut() {
            for &addr in q.iter() {
                if let Some(ls) = relocs.get(&addr) {
                    bstat.active_size -= ls.len as usize;
                    bstat.nr_active -= 1;
                    seqs.push(ls.seq);
                }
            }
        }

        let ret = BlobStat {
            inner: bstat.inner,
            inactive_elems: seqs,
        };

        for &id in obsoleted {
            self.remove_stat(id);
            self.common.cache.del(id);
        }
        self.add_stat_unlocked(bstat);
        ret
    }

    fn apply_junks(&self, junks: &[u64], ctx: &BucketContext, btree: &BTree) -> Vec<BlobStat> {
        let candidates: Vec<(u64, u64)> = {
            let lk = ctx.blob_intervals.read();
            junks
                .iter()
                .filter_map(|&addr| {
                    // race condition: gc might have already removed the interval containing this junk
                    // 1. flush thread holds a junk addr
                    // 2. gc thread rewrites the file containing addr, and since it is junk, it is not moved
                    // 3. gc thread removes the interval from interval map if it becomes empty
                    // 4. flush thread tries to find the file_id of the junk, but the interval is gone
                    let file_id = lk.find(addr)?;
                    Some((addr, file_id))
                })
                .collect()
        };
        let mut v: Vec<BlobStat> = Vec::with_capacity(junks.len());
        for (addr, file_id) in candidates {
            if self.ensure_mask(file_id, btree).is_err() {
                continue;
            }
            let mut map = self.map.write();
            if let Some(stat) = map.get_mut(&file_id) {
                let reloc = self.get_reloc(file_id, addr);
                self.update_stat(stat, &reloc, addr);
                if let Some(b) = v.last_mut()
                    && b.inner.file_id == file_id
                {
                    b.inner = stat.inner;
                    b.inactive_elems.push(reloc.seq);
                } else {
                    v.push(BlobStat {
                        inner: stat.inner,
                        inactive_elems: vec![reloc.seq],
                    });
                }
            }
        }
        v
    }

    fn update_stat(&self, stat: &mut MemBlobStat, reloc: &Reloc, addr: u64) {
        stat.update(reloc);
        self.common.junk.push_if_collecting(stat.file_id, addr);
    }

    pub(crate) fn add_stat_unlocked(&self, stat: MemBlobStat) {
        {
            self.common
                .bucket_files
                .entry(stat.bucket_id)
                .or_default()
                .push(stat.file_id);
        }
        let file_id = stat.file_id;
        let has_mask = stat.mask.is_some();
        let mut map = self.map.write();
        map.insert(file_id, stat);
        drop(map);
        if has_mask {
            self.record_mask_use(file_id);
        }
    }

    pub(crate) fn remove_stat(&self, file_id: u64) -> Option<MemBlobStat> {
        let mut map = self.map.write();
        let v = map.remove(&file_id)?;
        self.common.mask_cache.del(&file_id);
        #[allow(clippy::collapsible_if)]
        if let Some(mut files) = self.common.bucket_files.get_mut(&v.bucket_id) {
            if let Some(pos) = files.iter().position(|&x| x == file_id) {
                files.swap_remove(pos);
            }
        }
        Some(v)
    }
}
