use btree_store::BTree;
use parking_lot::{Mutex, RwLock};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    hash::Hasher,
    marker::PhantomData,
    ops::Deref,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{
            AtomicBool, AtomicU32, AtomicU64,
            Ordering::{AcqRel, Acquire, Relaxed, Release, SeqCst},
        },
    },
};

use crc32c::Crc32cHasher;
use dashmap::{DashMap, DashSet};

use crate::{
    Options,
    cc::context::Context,
    io::{File, GatherIO},
    map::{
        IDataReader, IFooter, SharedState,
        buffer::{BucketContext, BucketMgr},
        data::{BlobFooter, DataFooter, MetaReader},
        flush::CheckpointObserver,
        table::{BucketState, PageMap},
    },
    types::refbox::{BoxRef, BoxView},
    utils::{
        Handle, MutRef, OpCode,
        bitmap::BitMap,
        compress::{CompressorPool, DecompressorPool},
        data::{AddrPair, GroupPositions, LenSeq, Position, Reloc, init_group_pos},
        interval::IntervalMap,
        lru::{Lru, ShardLru},
        observe::{CounterMetric, EventKind, GaugeMetric, ObserveEvent},
        options::{BucketOptions, ParsedOptions},
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
pub(crate) const BUCKET_FRONTIER: &str = "bucket_frontier";
pub(crate) const BUCKET_VERSION: &str = "version";
pub(crate) const MAX_BUCKETS: u64 = 1024;
pub(crate) const VERSION_KEY: &str = "current_version";
// keep marker keys short to reduce numerics bucket write amplification
pub(crate) const ORPHAN_DATA_MARKER_PREFIX: &str = "odf_";
pub(crate) const ORPHAN_BLOB_MARKER_PREFIX: &str = "obf_";
pub(crate) const WAL_RECYCLE_PREFIX: &str = "wrc_";
/// storage format version
pub(crate) const CURRENT_VERSION: u64 = 1;

pub(crate) mod builder;
mod entry;
pub use entry::{
    BlobStat, BlobStatInner, BucketDurableFrontier, BucketMeta, DataStat, DataStatInner,
    DelInterval, Delete, IntervalPair, MemBlobStat, MemDataStat, MetaKind, Numerics, PageTable,
    WalRecycleState,
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

pub(crate) fn orphan_data_marker_key(file_id: u64) -> Vec<u8> {
    format!("{}{}", ORPHAN_DATA_MARKER_PREFIX, file_id).into_bytes()
}

pub(crate) fn orphan_blob_marker_key(file_id: u64) -> Vec<u8> {
    format!("{}{}", ORPHAN_BLOB_MARKER_PREFIX, file_id).into_bytes()
}

pub(crate) fn wal_recycle_key(group_id: u8) -> Vec<u8> {
    format!("{}{}", WAL_RECYCLE_PREFIX, group_id).into_bytes()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WalRecycleIntent {
    pub(crate) group_id: u8,
    pub(crate) from_file_id: u64,
    pub(crate) to_file_id: u64,
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

impl MetaRecord for BucketDurableFrontier {
    fn record(&self, _kind: MetaKind, ops: &mut BTreeMap<String, Vec<MetaOp>>) {
        let mut buf = vec![0u8; self.packed_size()];
        self.encode(&mut buf);
        ops.entry(BUCKET_FRONTIER.to_string())
            .or_default()
            .push(MetaOp::Put(self.bucket_id.to_be_bytes().to_vec(), buf));
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
        let mut dedup = BTreeSet::new();
        for &lo in self.iter() {
            if dedup.insert(lo) {
                bucket_ops.push(MetaOp::Del(lo.to_be_bytes().to_vec()));
            }
        }
    }
}

#[derive(Clone)]
pub(crate) enum MetaOp {
    Put(Vec<u8>, Vec<u8>),
    Update(Vec<u8>, Vec<u8>, CounterMetric),
    Del(Vec<u8>),
}

pub(crate) struct Manifest {
    pub(crate) numerics: Arc<Numerics>,
    pub(crate) data_stat: DataStatCtx,
    pub(crate) blob_stat: BlobStatCtx,
    pub(crate) buckets: Handle<BucketMgr>,
    pub(crate) bucket_metas: DashMap<String, Arc<BucketMeta>>,
    pub(crate) bucket_metas_by_id: DashMap<u64, Arc<BucketMeta>>,
    pub(crate) bucket_frontier: DashMap<u64, GroupPositions>,
    pub(crate) bucket_runtimes: DashMap<u64, Arc<BucketRuntime>>,
    pub(crate) structural_lock: Mutex<()>,
    /// total bucket count including both active/pending_del
    pub(crate) nr_buckets: AtomicU64,
    pub(crate) obsolete_data: Mutex<Vec<u64>>,
    pub(crate) obsolete_blob: Mutex<Vec<u64>>,
    pub(crate) data_unsync: RwLock<HashSet<u64>>,
    pub(crate) blob_unsync: RwLock<HashSet<u64>>,
    retired_stat_keys: RetiredStatKeys,
    pub(crate) opt: Arc<ParsedOptions>,
    pub(crate) btree: BTree,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum RetiredStatKind {
    Data,
    Blob,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct RetiredStatKey {
    kind: RetiredStatKind,
    bucket_id: u64,
    file_id: u64,
}

#[derive(Default)]
pub(crate) struct RetiredStatKeys {
    keys: DashSet<RetiredStatKey>,
}

pub(crate) struct RetiredStatFlushSnapshot {
    keys: Vec<RetiredStatKey>,
}

impl RetiredStatKeys {
    fn mark(&self, kind: RetiredStatKind, bucket_id: u64, file_ids: &[u64]) -> usize {
        let mut inserted = 0;
        for &file_id in file_ids {
            if self.keys.insert(RetiredStatKey {
                kind,
                bucket_id,
                file_id,
            }) {
                inserted += 1;
            }
        }
        inserted
    }

    fn contains(&self, kind: RetiredStatKind, bucket_id: u64, file_id: u64) -> bool {
        self.keys.contains(&RetiredStatKey {
            kind,
            bucket_id,
            file_id,
        })
    }

    fn snapshot_bucket(&self, bucket_id: u64) -> RetiredStatFlushSnapshot {
        let keys = self
            .keys
            .iter()
            .filter_map(|entry| {
                let key = *entry.key();
                (key.bucket_id == bucket_id).then_some(key)
            })
            .collect();
        RetiredStatFlushSnapshot { keys }
    }

    fn clear_snapshot(&self, snapshot: RetiredStatFlushSnapshot) -> usize {
        let mut removed = 0;
        for key in snapshot.keys {
            if self.keys.remove(&key).is_some() {
                removed += 1;
            }
        }
        removed
    }

    fn len(&self) -> usize {
        self.keys.len()
    }
}

enum BucketRemoveMode {
    Drop,
    Delete,
}

pub(crate) struct BucketRuntime {
    pub(crate) state: MutRef<BucketState>,
    pub(crate) compressors: Arc<CompressorPool>,
    rewrite_inflight: AtomicU32,
}

impl BucketRuntime {
    fn new() -> Self {
        Self {
            state: MutRef::new(BucketState::new()),
            compressors: CompressorPool::new(),
            rewrite_inflight: AtomicU32::new(0),
        }
    }

    fn begin_rewrite(self: &Arc<Self>) -> BucketRewriteGuard {
        self.rewrite_inflight.fetch_add(1, AcqRel);
        BucketRewriteGuard {
            runtime: self.clone(),
        }
    }

    pub(crate) fn has_rewrite(&self) -> bool {
        self.rewrite_inflight.load(Acquire) != 0
    }
}

pub(crate) struct BucketRewriteGuard {
    runtime: Arc<BucketRuntime>,
}

impl Drop for BucketRewriteGuard {
    fn drop(&mut self) {
        self.runtime.rewrite_inflight.fetch_sub(1, AcqRel);
    }
}

pub(crate) struct BucketRewritePermit {
    pub(crate) enable_compression: bool,
    pub(crate) compressors: Arc<CompressorPool>,
    _guard: BucketRewriteGuard,
}

pub(crate) struct Txn<'a> {
    manifest: &'a Manifest,
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

    pub(crate) fn record_data_stat_update(&mut self, x: &DataStat) {
        self.record_stat_update(
            BUCKET_DATA_STAT,
            x.file_id,
            x,
            CounterMetric::FlushConditionalDataStatPutMiss,
        );
    }

    pub(crate) fn record_blob_stat_update(&mut self, x: &BlobStat) {
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
                file_id.to_be_bytes().to_vec(),
                buf,
                miss_metric,
            ));
    }
}

struct FileReader {
    file: File,
    relocs: Box<[AddrPair]>,
    decoders: Arc<DecompressorPool>,
}

fn new_reader<T: IFooter>(
    path: PathBuf,
    decoders: Arc<DecompressorPool>,
) -> Result<Arc<FileReader>, OpCode> {
    let mut loader = MetaReader::<T>::new(&path).expect("not such path");
    let relocs = loader.get_reloc().expect("must exist");
    let relocs = {
        #[cfg(feature = "extra_check")]
        for w in relocs.windows(2) {
            let prev = w[0].key;
            let next = w[1].key;
            debug_assert!(prev <= next, "reloc table must be sorted by addr");
        }
        relocs
    };
    let file = loader.take();
    Ok(Arc::new(FileReader {
        file,
        relocs,
        decoders,
    }))
}

impl FileReader {
    #[inline]
    fn find_reloc(&self, pos: u64) -> Option<Reloc> {
        let idx = self.relocs.binary_search_by_key(&pos, |x| x.key).ok()?;
        Some(self.relocs[idx].val)
    }

    fn read_at(&self, pos: u64) -> Result<BoxRef, OpCode> {
        let m = self.find_reloc(pos).expect("can't find addr in reloc");
        let real_size = BoxRef::real_size_from_dump(m.raw_len());
        let mut p = BoxRef::alloc_exact(real_size, pos);
        let dst = p.load_slice();

        if !m.is_compressed() {
            let mut crc = Crc32cHasher::default();
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
            return Ok(p);
        }

        let actual_crc = self.decoders.with_decoder(|decoder| {
            decoder.decode_reader_into(&self.file, m.off as u64, m.compressed_len() as usize, dst)
        })?;
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
            BUCKET_FRONTIER,
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
            bucket_frontier: DashMap::new(),
            bucket_runtimes: DashMap::new(),
            structural_lock: Mutex::new(()),
            obsolete_data: Mutex::new(Vec::new()),
            obsolete_blob: Mutex::new(Vec::new()),
            data_unsync: RwLock::new(HashSet::new()),
            blob_unsync: RwLock::new(HashSet::new()),
            retired_stat_keys: RetiredStatKeys::default(),
            nr_buckets: AtomicU64::new(0),
            opt,
            btree,
        }
    }

    pub(crate) fn set_context(
        &self,
        ctx: Handle<Context>,
        reader: Arc<dyn IDataReader>,
        observer: Arc<dyn CheckpointObserver>,
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
        self.bucket_metas_by_id.insert(meta.id, meta.clone());
        self.ensure_bucket_runtime(meta.id);
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
        opt: BucketOptions,
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

        let meta = Arc::new(BucketMeta {
            id: bucket_id,
            options: opt,
        });
        // publish meta early so context creation sees it
        self.bucket_metas.insert(name.to_string(), meta.clone());
        self.bucket_metas_by_id.insert(bucket_id, meta.clone());

        // ensure state and pagemap are initialized
        let bucket_ctx = self.load_bucket_context_locked(bucket_id);
        let frontier = self.current_group_checkpoints();
        self.bucket_frontier.insert(bucket_id, frontier);
        let frontier_rec = BucketDurableFrontier::new(bucket_id, frontier);

        let mut buf = vec![0u8; meta.as_ref().packed_size()];
        let mut txn = self.begin();
        txn.record(MetaKind::Numerics, self.numerics.as_ref());
        txn.record(MetaKind::BucketFrontier, &frontier_rec);
        meta.as_ref().encode(&mut buf);
        txn.ops_mut()
            .entry(BUCKET_METAS.to_string())
            .or_default()
            .push(MetaOp::Put(name.as_bytes().to_vec(), buf));
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash("mace_bucket_create_before_manifest_commit");
        txn.commit();
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash("mace_bucket_create_after_manifest_commit");

        Ok((meta, bucket_ctx))
    }

    fn bucket_option_conflicts(old: BucketOptions, new: BucketOptions) -> Vec<&'static str> {
        let mut conflicts = Vec::new();
        if old.inline_size != new.inline_size {
            conflicts.push("inline_size");
        }
        if old.split_elems != new.split_elems {
            conflicts.push("split_elems");
        }
        conflicts
    }

    pub(crate) fn update_bucket_options(
        &self,
        name: &str,
        opt: BucketOptions,
    ) -> Result<(), OpCode> {
        let _lock = self.structural_lock.lock();
        let meta = self.load_bucket_meta_locked(name)?;
        let bucket_id = meta.id;
        let runtime = self.get_bucket_runtime(bucket_id);

        if self.buckets.buckets.contains_key(&bucket_id) || runtime.has_rewrite() {
            log::info!(
                "bucket {}({}) is busy, reject bucket option update",
                name,
                bucket_id
            );
            return Err(OpCode::Again);
        }

        if meta.options == opt {
            return Ok(());
        }

        let conflicts = Self::bucket_option_conflicts(meta.options, opt);
        if !conflicts.is_empty() {
            log::error!(
                "bucket {}({}) option update conflicts on [{}], old: {:?}, new: {:?}",
                name,
                bucket_id,
                conflicts.join(", "),
                meta.options,
                opt
            );
            return Err(OpCode::Invalid);
        }

        let new_meta = Arc::new(BucketMeta {
            id: bucket_id,
            options: opt,
        });
        let mut buf = vec![0u8; new_meta.as_ref().packed_size()];
        new_meta.as_ref().encode(&mut buf);

        let mut txn = self.begin();
        txn.ops_mut()
            .entry(BUCKET_METAS.to_string())
            .or_default()
            .push(MetaOp::Put(name.as_bytes().to_vec(), buf));
        txn.commit();

        self.bucket_metas.insert(name.to_string(), new_meta.clone());
        self.bucket_metas_by_id.insert(bucket_id, new_meta);
        Ok(())
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

    pub(crate) fn try_acquire_rewrite(&self, bucket_id: u64) -> Option<BucketRewritePermit> {
        let _lock = self.structural_lock.lock();
        let meta = self.bucket_metas_by_id.get(&bucket_id)?;
        let runtime = self.get_bucket_runtime(bucket_id);
        if runtime.state.is_deleting() || runtime.state.is_drop() {
            return None;
        }
        let enable_compression = meta.options.enable_compression;
        Some(BucketRewritePermit {
            enable_compression,
            compressors: runtime.compressors.clone(),
            _guard: runtime.begin_rewrite(),
        })
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

        let runtime = self.ensure_bucket_runtime(bucket_id);
        let state = runtime.state.clone();

        // PageMap lazy load
        let table = {
            let table = MutRef::new(PageMap::default());
            if let Some(_meta) = self.bucket_metas_by_id.get(&bucket_id) {
                table.recover(bucket_id, Some(&self.btree));
            }
            table
        };

        let flush = self
            .buckets
            .flush
            .as_ref()
            .expect("flusher started")
            .clone();
        let meta = self
            .bucket_metas_by_id
            .get(&bucket_id)
            .expect("bucket meta must exist")
            .clone();
        let ctx = Arc::new(BucketContext::new(
            self.buckets.ctx,
            &self.opt,
            Arc::new(meta.options),
            state,
            bucket_id,
            table,
            flush,
            self.buckets.lru,
            self.buckets.reader.clone(),
            self.buckets.tx.clone(),
            runtime.compressors.clone(),
        ));

        self.recover_intervals(bucket_id, &ctx);

        self.buckets.buckets.insert(bucket_id, ctx.clone());
        ctx
    }

    pub(crate) fn get_bucket_runtime(&self, bucket_id: u64) -> Arc<BucketRuntime> {
        self.bucket_runtimes
            .get(&bucket_id)
            .map(|runtime| runtime.value().clone())
            .expect("must exist")
    }

    // bucket runtime remains alive across load/unload so background GC can read lifecycle state
    // and reuse compression contexts without recreating the full bucket context
    pub(crate) fn ensure_bucket_runtime(&self, bucket_id: u64) -> Arc<BucketRuntime> {
        self.bucket_runtimes
            .entry(bucket_id)
            .or_insert_with(|| Arc::new(BucketRuntime::new()))
            .value()
            .clone()
    }

    fn current_group_checkpoints(&self) -> GroupPositions {
        let mut out = init_group_pos();
        let groups = self.buckets.ctx.groups();
        let n = groups.len().min(Options::MAX_CONCURRENT_WRITE as usize);
        for i in 0..n {
            out[i] = groups[i].logging.lock().last_ckpt();
        }
        out
    }

    fn begin_bucket_remove_locked(
        &self,
        name: &str,
        mode: BucketRemoveMode,
    ) -> Result<u64, OpCode> {
        let meta = self.load_bucket_meta_locked(name)?;
        let bucket_id = meta.id;

        // remove from maps (unpublish)
        self.bucket_metas.remove(name);
        if matches!(mode, BucketRemoveMode::Delete) {
            self.bucket_metas_by_id.remove(&bucket_id);
        }

        let runtime = self.get_bucket_runtime(bucket_id);
        let state = runtime.state.clone();
        let holder_baseline = match mode {
            BucketRemoveMode::Drop => 2,
            BucketRemoveMode::Delete => 1,
        };
        let mut busy =
            Arc::strong_count(&meta) > holder_baseline || state.is_busy() || state.is_vacuuming();
        if matches!(mode, BucketRemoveMode::Delete) && state.is_drop() {
            busy = true;
        }
        if busy {
            self.bucket_metas.insert(name.to_string(), meta.clone());
            if matches!(mode, BucketRemoveMode::Delete) {
                self.bucket_metas_by_id.insert(bucket_id, meta);
            }
            return Err(OpCode::Again);
        }

        match mode {
            BucketRemoveMode::Drop => state.set_drop(),
            BucketRemoveMode::Delete => state.set_deleting(),
        }

        if matches!(mode, BucketRemoveMode::Delete) && runtime.has_rewrite() {
            state.clear_deleting();
            self.bucket_metas.insert(name.to_string(), meta.clone());
            self.bucket_metas_by_id.insert(bucket_id, meta);
            return Err(OpCode::Again);
        }

        Ok(bucket_id)
    }

    pub(crate) fn unload_bucket(&self, name: &str) -> Result<(), OpCode> {
        // serialize deletions and creations
        let _lock = self.structural_lock.lock();
        let bucket_id = {
            let meta = self.load_bucket_meta_locked(name)?;
            meta.id
        };
        if self.buckets.ctx.has_pending_abort_clean_bucket(bucket_id) {
            return Err(OpCode::Again);
        }

        // remove from maps (unpublish)
        let bucket_id = self.begin_bucket_remove_locked(name, BucketRemoveMode::Drop)?;

        if let Some(ctx) = self
            .buckets
            .buckets
            .get(&bucket_id)
            .map(|x| x.value().clone())
        {
            ctx.checkpoint_before_reclaim();
        }
        let _ = self.buckets.buckets.remove(&bucket_id);

        self.get_bucket_runtime(bucket_id).state.clear_drop();
        Ok(())
    }

    pub(crate) fn delete_bucket(&self, name: &str) -> Result<(), OpCode> {
        // serialize deletions and creations
        let _lock = self.structural_lock.lock();

        // remove from maps (unpublish)
        let bucket_id = self.begin_bucket_remove_locked(name, BucketRemoveMode::Delete)?;

        // cleanup page maps and resources via manager
        self.buckets.del_bucket(bucket_id);

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
        if self.bucket_frontier.contains_key(&bucket_id) {
            txn.ops_mut()
                .entry(BUCKET_FRONTIER.to_string())
                .or_default()
                .push(MetaOp::Del(bucket_id.to_be_bytes().to_vec()));
        }

        // record obsolete files
        if !data_files.is_empty() {
            // bucket delete is already unpublished and must not publish retired keys
            txn.record(MetaKind::DataDelete, &Delete::from(data_files.clone()));
        }
        if !blob_files.is_empty() {
            // bucket delete is already unpublished and must not publish retired keys
            txn.record(MetaKind::BlobDelete, &Delete::from(blob_files.clone()));
        }

        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash("mace_bucket_delete_before_manifest_commit");
        txn.commit();
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash("mace_bucket_delete_after_manifest_commit");

        // in-memory cleanup for stats
        // once a bucket enters pending-delete, its file metadata must no longer participate in
        // GC victim selection; otherwise GC may try to update interval buckets that were already
        // physically dropped by pending-bucket cleanup.
        self.data_stat.remove_stat_interval(&data_files);
        self.blob_stat.remove_stat_interval(&blob_files);

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
        self.bucket_frontier.remove(&bucket_id);
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
        self.data_stat.add_stat_mem(stat);
    }

    pub(crate) fn add_blob_stat(&self, stat: MemBlobStat, ivl: IntervalPair) {
        if let Some(ctx) = self.buckets.buckets.get(&stat.bucket_id) {
            ctx.blob_intervals
                .write()
                .insert(ivl.lo_addr, ivl.hi_addr, ivl.file_id);
        }
        self.blob_stat.add_stat_mem(stat);
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
        if !junks.is_empty() {
            let ctx = self.get_bucket_context_must_exist(bucket_id);
            self.data_stat
                .apply_junks(tick, junks, &ctx, &self.btree, |file_id| {
                    let retired = self.is_retired_data_stat(bucket_id, file_id);
                    if retired {
                        self.opt
                            .observer
                            .counter(CounterMetric::FlushSkipRetiredDataStat, 1);
                    }
                    retired
                })
        } else {
            Vec::new()
        }
    }

    pub(crate) fn apply_blob_junks(&self, bucket_id: u64, junks: &[u64]) -> Vec<BlobStat> {
        if !junks.is_empty() {
            let ctx = self.get_bucket_context_must_exist(bucket_id);
            self.blob_stat
                .apply_junks(junks, &ctx, &self.btree, |file_id| {
                    let retired = self.is_retired_blob_stat(bucket_id, file_id);
                    if retired {
                        self.opt
                            .observer
                            .counter(CounterMetric::FlushSkipRetiredBlobStat, 1);
                    }
                    retired
                })
        } else {
            Vec::new()
        }
    }

    pub(crate) fn mark_retired_data_stats(&self, bucket_id: u64, file_ids: &[u64]) {
        self.mark_retired_stats(RetiredStatKind::Data, bucket_id, file_ids);
    }

    pub(crate) fn mark_retired_blob_stats(&self, bucket_id: u64, file_ids: &[u64]) {
        self.mark_retired_stats(RetiredStatKind::Blob, bucket_id, file_ids);
    }

    fn mark_retired_stats(&self, kind: RetiredStatKind, bucket_id: u64, file_ids: &[u64]) {
        if file_ids.is_empty() {
            return;
        }
        self.retired_stat_keys.mark(kind, bucket_id, file_ids);
        self.observe_retired_stat_keys();
    }

    pub(crate) fn is_retired_data_stat(&self, bucket_id: u64, file_id: u64) -> bool {
        self.retired_stat_keys
            .contains(RetiredStatKind::Data, bucket_id, file_id)
    }

    pub(crate) fn is_retired_blob_stat(&self, bucket_id: u64, file_id: u64) -> bool {
        self.retired_stat_keys
            .contains(RetiredStatKind::Blob, bucket_id, file_id)
    }

    pub(crate) fn snapshot_retired_stat_keys(&self, bucket_id: u64) -> RetiredStatFlushSnapshot {
        self.retired_stat_keys.snapshot_bucket(bucket_id)
    }

    pub(crate) fn clear_retired_stat_keys(&self, snapshot: RetiredStatFlushSnapshot) {
        self.retired_stat_keys.clear_snapshot(snapshot);
        self.observe_retired_stat_keys();
    }

    fn observe_retired_stat_keys(&self) {
        self.opt.observer.gauge(
            GaugeMetric::RetiredStatKeysCurrent,
            self.retired_stat_keys.len() as i64,
        );
    }

    pub(crate) fn durable_frontier_lsn(&self, bucket_id: u64, group: u8) -> Position {
        let idx = group as usize;
        debug_assert!(idx < Options::MAX_CONCURRENT_WRITE as usize);
        self.bucket_frontier
            .get(&bucket_id)
            .map(|x| x[idx])
            .unwrap_or(Position::MIN)
    }

    pub(crate) fn merge_bucket_frontier(
        &self,
        bucket_id: u64,
        delta: &GroupPositions,
    ) -> BucketDurableFrontier {
        let mut merged = self
            .bucket_frontier
            .get(&bucket_id)
            .map(|x| *x.value())
            .unwrap_or(init_group_pos());
        for i in 0..merged.len() {
            merged[i] = merged[i].max(delta[i]);
        }
        self.bucket_frontier.insert(bucket_id, merged);
        BucketDurableFrontier::new(bucket_id, merged)
    }

    pub(crate) fn global_frontier_lower_bound(&self, groups: usize) -> GroupPositions {
        let mut lower = init_group_pos();
        let mut init = [false; Options::MAX_CONCURRENT_WRITE as usize];
        let groups = groups.min(Options::MAX_CONCURRENT_WRITE as usize);
        let fallback = self.current_group_checkpoints();
        for meta in self.bucket_metas_by_id.iter() {
            let bucket_id = *meta.key();
            let bucket_ctx = self.buckets.buckets.get(&bucket_id);
            let has_pending_flush = bucket_ctx
                .as_ref()
                .is_some_and(|ctx| ctx.value().has_pending_flush_data());
            let frontier = self.bucket_frontier.get(&bucket_id);
            for i in 0..groups {
                // clean/read-only buckets must not pin global frontier with stale bucket frontier.
                // only buckets that still have pending flush data participate via durable frontier.
                let pos = if has_pending_flush {
                    frontier.as_ref().map_or(fallback[i], |x| x[i])
                } else {
                    fallback[i]
                };
                if !init[i] || pos < lower[i] {
                    lower[i] = pos;
                    init[i] = true;
                }
            }
        }
        for i in 0..groups {
            if !init[i] {
                lower[i] = fallback[i];
            }
        }
        lower
    }

    pub(crate) fn begin(&self) -> Txn<'_> {
        Txn {
            manifest: self,
            ops: BTreeMap::new(),
        }
    }

    pub(crate) fn stage_orphan_data_file(&self, file_id: u64) {
        self.stage_orphan_marker(orphan_data_marker_key(file_id), "data", file_id);
        self.opt
            .observer
            .counter(CounterMetric::FlushOrphanDataStaged, 1);
        self.opt.observer.event(ObserveEvent {
            kind: EventKind::FlushOrphanDataStaged,
            bucket_id: 0,
            txid: 0,
            file_id,
            value: 0,
        });
    }

    pub(crate) fn stage_orphan_blob_file(&self, file_id: u64) {
        self.stage_orphan_marker(orphan_blob_marker_key(file_id), "blob", file_id);
        self.opt
            .observer
            .counter(CounterMetric::FlushOrphanBlobStaged, 1);
        self.opt.observer.event(ObserveEvent {
            kind: EventKind::FlushOrphanBlobStaged,
            bucket_id: 0,
            txid: 0,
            file_id,
            value: 0,
        });
    }

    pub(crate) fn clear_orphan_data_file(&self, txn: &mut Txn<'_>, file_id: u64) {
        txn.ops_mut()
            .entry(BUCKET_NUMERICS.to_string())
            .or_default()
            .push(MetaOp::Del(orphan_data_marker_key(file_id)));
        self.opt
            .observer
            .counter(CounterMetric::FlushOrphanDataCleared, 1);
        self.opt.observer.event(ObserveEvent {
            kind: EventKind::FlushOrphanDataCleared,
            bucket_id: 0,
            txid: 0,
            file_id,
            value: 0,
        });
    }

    pub(crate) fn stage_unsynced_data_file(&self, file_id: u64) {
        self.data_unsync.write().insert(file_id);
    }

    pub(crate) fn stage_unsynced_blob_file(&self, file_id: u64) {
        self.blob_unsync.write().insert(file_id);
    }

    pub(crate) fn clear_synced_data(&self) {
        self.data_unsync.write().clear();
    }

    pub(crate) fn clear_synced_blob(&self) {
        self.blob_unsync.write().clear();
    }

    pub(crate) fn is_unsynced_data_file(&self, file_id: u64) -> bool {
        self.data_unsync.read().contains(&file_id)
    }

    pub(crate) fn is_unsynced_blob_file(&self, file_id: u64) -> bool {
        self.blob_unsync.read().contains(&file_id)
    }

    pub(crate) fn load_wal_recycle_state(&self, group_id: u8) -> WalRecycleState {
        let key = wal_recycle_key(group_id);
        match self.btree.view(BUCKET_NUMERICS, |txn| txn.get(&key)) {
            Ok(val) => WalRecycleState::decode(&val),
            Err(btree_store::Error::NotFound) => WalRecycleState::none(group_id),
            Err(err) => panic!("load wal recycle state failed for group {group_id}: {err:?}"),
        }
    }

    pub(crate) fn record_wal_recycle_state(&self, txn: &mut Txn<'_>, state: WalRecycleState) {
        let mut buf = vec![0u8; state.packed_size()];
        state.encode(&mut buf);
        txn.ops_mut()
            .entry(BUCKET_NUMERICS.to_string())
            .or_default()
            .push(MetaOp::Put(wal_recycle_key(state.group_id), buf));
    }

    pub(crate) fn commit_wal_recycle_intent(&self, intent: WalRecycleIntent) {
        let mut txn = self.begin();
        self.record_wal_recycle_state(
            &mut txn,
            WalRecycleState::intent(intent.group_id, intent.from_file_id, intent.to_file_id),
        );
        txn.commit();
    }

    pub(crate) fn commit_wal_recycle_done(&self, intent: WalRecycleIntent) {
        let mut txn = self.begin();
        self.record_wal_recycle_state(
            &mut txn,
            WalRecycleState::done(intent.group_id, intent.from_file_id, intent.to_file_id),
        );
        txn.commit();
    }

    pub(crate) fn clear_orphan_blob_file(&self, txn: &mut Txn<'_>, file_id: u64) {
        txn.ops_mut()
            .entry(BUCKET_NUMERICS.to_string())
            .or_default()
            .push(MetaOp::Del(orphan_blob_marker_key(file_id)));
        self.opt
            .observer
            .counter(CounterMetric::FlushOrphanBlobCleared, 1);
        self.opt.observer.event(ObserveEvent {
            kind: EventKind::FlushOrphanBlobCleared,
            bucket_id: 0,
            txid: 0,
            file_id,
            value: 0,
        });
    }

    fn stage_orphan_marker(&self, key: Vec<u8>, kind: &str, file_id: u64) {
        let v = [];
        self.btree
            .exec(BUCKET_NUMERICS, |txn| txn.put(&key, v))
            .unwrap_or_else(|e| {
                panic!(
                    "failed to stage {} orphan marker for file {}: {:?}",
                    kind, file_id, e
                )
            });
    }

    pub(crate) fn vacuum_meta(
        &self,
        target_bytes: u64,
    ) -> Result<btree_store::CompactStats, OpCode> {
        self.btree.compact(target_bytes).map_err(OpCode::from)
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
        C: Fn(BoxView),
    {
        let ctx = self.get_bucket_context_must_exist(bucket_id);
        match self.blob_stat.load(addr, &ctx) {
            Ok(b) => {
                cache(b.view());
                Ok(b)
            }
            e => e,
        }
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
            self.opt.sync_data_dir();
            #[cfg(feature = "failpoints")]
            crate::utils::failpoint::crash("mace_delete_files_after_dir_sync_before_meta_commit");

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

    pub(crate) fn recover_intervals(&self, bucket_id: u64, ctx: &BucketContext) {
        let mut max_addr = crate::utils::INIT_ADDR;

        let data_ivl_table = data_interval_name(bucket_id);
        let _ = self.btree.view(&data_ivl_table, |txn| {
            let mut iter = txn.iter();
            let mut k = Vec::new();
            let mut v = Vec::new();
            let mut map = ctx.data_intervals.write();
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
            let mut map = ctx.blob_intervals.write();
            while iter.next_ref(&mut k, &mut v) {
                let ivl = IntervalPair::decode(&v);
                max_addr = max_addr.max(ivl.hi_addr);
                map.upsert(ivl.lo_addr, ivl.hi_addr, ivl.file_id);
            }
            Ok(())
        });

        ctx.state.next_addr.fetch_max(max_addr + 1, Relaxed);
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
            if let Some(reader) = self.common.cache.get(file_id).map(|r| r.clone()) {
                return reader.read_at(addr);
            }

            let lk = self.common.cache.lock_shard(file_id);
            let decoders = self.common.decoders.clone();
            lk.add_if_missing(|| {
                new_reader::<K::Footer>(K::file_path(&self.common.opt, file_id), decoders.clone())
            })?;
        }
    }

    // it's possible that during multiple compaction the intermediate sibling/blob addr may be removed
    // from dirty page and never flush to disk, but meanwhile someone may still hold the old ref to
    // those addr, in this case we can't find reloc by addr
    fn try_get_reloc(&self, file_id: u64, pos: u64) -> Option<Reloc> {
        loop {
            if let Some(reader) = self.common.cache.get(file_id).map(|x| x.clone()) {
                return reader.find_reloc(pos);
            }

            let lk = self.common.cache.lock_shard(file_id);
            let decoders = self.common.decoders.clone();
            lk.add_if_missing(|| {
                new_reader::<K::Footer>(K::file_path(&self.common.opt, file_id), decoders.clone())
            })
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
    cache: ShardLru<Arc<FileReader>>,
    decoders: Arc<DecompressorPool>,
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
            decoders: DecompressorPool::new(),
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

    pub(crate) fn add_stat_mem(&self, stat: MemDataStat) {
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
                    fstat.active_size -= ls.active_len() as usize;
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

        self.add_stat_mem(fstat);
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
        is_retired: impl Fn(u64) -> bool,
    ) -> Vec<DataStat> {
        let grouped: BTreeMap<u64, Vec<u64>> = {
            let lk = ctx.data_intervals.read();
            let mut grouped = BTreeMap::<u64, Vec<u64>>::new();
            for &addr in junks {
                // race condition: gc might have already removed the interval containing this junk
                // 1. flush thread holds a junk addr
                // 2. gc thread rewrites the file containing addr, and since it is junk, it is not moved
                // 3. gc thread removes the interval from interval map if it becomes empty
                // 4. flush thread tries to find the file_id of the junk, but the interval is gone
                if let Some(file_id) = lk.find(addr) {
                    grouped.entry(file_id).or_default().push(addr);
                }
            }
            grouped
        };

        // Merge all updates on the same file_id into one DataStat record to avoid
        // generating many duplicate per-file meta puts in a single publish round.
        let mut v: Vec<DataStat> = Vec::with_capacity(grouped.len());
        for (file_id, addrs) in grouped {
            if is_retired(file_id) {
                continue;
            }
            if self.ensure_mask(file_id, btree).is_err() {
                continue;
            }
            if let Some(mut stat) = self.map.get_mut(&file_id) {
                let mut seqs = Vec::new();
                for addr in addrs {
                    // race condition: gc might have already removed the interval containing this junk
                    let Some(reloc) = self.try_get_reloc(file_id, addr) else {
                        // interval ranges can cover sparse logical addresses, so a junk addr may
                        // resolve to a file without having ever been written into its reloc table
                        continue;
                    };
                    if stat.mask.as_ref().expect("mask loaded").test(reloc.seq) {
                        continue;
                    }
                    self.update_stat(&mut stat, addr, &reloc, tick);
                    seqs.push(reloc.seq);
                }
                if !seqs.is_empty() {
                    v.push(DataStat {
                        inner: stat.inner,
                        inactive_elems: seqs,
                    });
                }
            }
        }
        v
    }

    pub(crate) fn update_stat(&self, stat: &mut MemDataStat, junk: u64, reloc: &Reloc, tick: u64) {
        self.active_size
            .fetch_sub(reloc.active_len() as u64, Release);
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
                    bstat.active_size -= ls.active_len() as usize;
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
        self.add_stat_mem(bstat);
        ret
    }

    fn apply_junks(
        &self,
        junks: &[u64],
        ctx: &BucketContext,
        btree: &BTree,
        is_retired: impl Fn(u64) -> bool,
    ) -> Vec<BlobStat> {
        let grouped: BTreeMap<u64, Vec<u64>> = {
            let lk = ctx.blob_intervals.read();
            let mut grouped = BTreeMap::<u64, Vec<u64>>::new();
            for &addr in junks {
                // race condition: gc might have already removed the interval containing this junk
                // 1. flush thread holds a junk addr
                // 2. gc thread rewrites the file containing addr, and since it is junk, it is not moved
                // 3. gc thread removes the interval from interval map if it becomes empty
                // 4. flush thread tries to find the file_id of the junk, but the interval is gone
                if let Some(file_id) = lk.find(addr) {
                    grouped.entry(file_id).or_default().push(addr);
                }
            }
            grouped
        };
        let mut v: Vec<BlobStat> = Vec::with_capacity(grouped.len());
        for (file_id, addrs) in grouped {
            if is_retired(file_id) {
                continue;
            }
            if self.ensure_mask(file_id, btree).is_err() {
                continue;
            }
            let mut map = self.map.write();
            if let Some(stat) = map.get_mut(&file_id) {
                let mut seqs = Vec::new();
                for addr in addrs {
                    let Some(reloc) = self.try_get_reloc(file_id, addr) else {
                        // interval ranges can cover sparse logical addresses, so a junk addr may
                        // resolve to a file without having ever been written into its reloc table
                        continue;
                    };
                    if stat.mask.as_ref().expect("mask loaded").test(reloc.seq) {
                        continue;
                    }
                    self.update_stat(stat, &reloc, addr);
                    seqs.push(reloc.seq);
                }
                if !seqs.is_empty() {
                    v.push(BlobStat {
                        inner: stat.inner,
                        inactive_elems: seqs,
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

    pub(crate) fn add_stat_mem(&self, stat: MemBlobStat) {
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
