#![cfg(feature = "failpoints")]

mod common;

use btree_store::{BTree, Error as BTreeError};
use common::child_test_command;
use mace::observe::{CounterMetric, InMemoryObserver};
use mace::{Bucket, BucketOptions, Mace, OpCode, Options, RandomPath};
use serde::Deserialize;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::ExitStatus;
use std::sync::Arc;
use std::time::{Duration, Instant};

const ENV_CHILD: &str = "MACE_PROD_FP_CHILD";
const ENV_CASE: &str = "MACE_PROD_FP_CASE";
const ENV_DB_ROOT: &str = "MACE_PROD_FP_DB_ROOT";
const BUCKET_METAS: &str = "bucket_metas";
const BUCKET_PENDING_DEL: &str = "pending_del";
const BUCKET_MISC: &str = "misc";
const WAL_RECYCLE_PREFIX: &str = "wrc_";
const OPTIONS_KEY: &str = "options";

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct PersistedGlobalOptions {
    concurrent_write: u8,
    sync_on_write: bool,
    gc_timeout: u64,
    checkpoint_nudge_ms: u64,
    data_garbage_ratio: u32,
    gc_eager: bool,
    blob_file_size: usize,
    blob_garbage_ratio: u32,
    lru_capacity: usize,
    stat_mask_cache_count: usize,
    data_handle_cache_capacity: usize,
    blob_handle_cache_capacity: usize,
    data_file_size: usize,
    wal_buffer_size: usize,
    max_ckpt_per_txn: usize,
    wal_file_size: u32,
    keep_stable_wal_file: bool,
    truncate_corrupted_wal: bool,
}

impl PersistedGlobalOptions {
    fn apply_to(&self, opt: &mut Options) {
        opt.concurrent_write = self.concurrent_write;
        opt.sync_on_write = self.sync_on_write;
        opt.gc_timeout = self.gc_timeout;
        opt.checkpoint_nudge_ms = self.checkpoint_nudge_ms;
        opt.data_garbage_ratio = self.data_garbage_ratio;
        opt.gc_eager = self.gc_eager;
        opt.blob_file_size = self.blob_file_size;
        opt.blob_garbage_ratio = self.blob_garbage_ratio;
        opt.lru_capacity = self.lru_capacity;
        opt.stat_mask_cache_count = self.stat_mask_cache_count;
        opt.data_handle_cache_capacity = self.data_handle_cache_capacity;
        opt.blob_handle_cache_capacity = self.blob_handle_cache_capacity;
        opt.data_file_size = self.data_file_size;
        opt.wal_buffer_size = self.wal_buffer_size;
        opt.max_ckpt_per_txn = self.max_ckpt_per_txn;
        opt.wal_file_size = self.wal_file_size;
        opt.keep_stable_wal_file = self.keep_stable_wal_file;
        opt.truncate_corrupted_wal = self.truncate_corrupted_wal;
    }
}

fn counter_value(observer: &InMemoryObserver, metric: CounterMetric) -> u64 {
    observer
        .snapshot()
        .counters
        .iter()
        .find(|(current, _)| *current == metric)
        .map(|(_, value)| *value)
        .unwrap_or(0)
}

fn page_table_name(bucket_id: u64) -> String {
    format!("page_table_{bucket_id}")
}

fn data_interval_name(bucket_id: u64) -> String {
    format!("data_interval_{bucket_id}")
}

fn blob_interval_name(bucket_id: u64) -> String {
    format!("blob_interval_{bucket_id}")
}

fn spawn_child(case: &str, db_root: &Path, failpoint: &str) -> ExitStatus {
    let exe = std::env::current_exe().expect("load current exe failed");
    child_test_command(&exe)
        .arg("--exact")
        .arg("failpoint_child")
        .arg("--nocapture")
        .env(ENV_CHILD, "1")
        .env(ENV_CASE, case)
        .env(ENV_DB_ROOT, db_root.as_os_str())
        .env("MACE_FAILPOINT", failpoint)
        .status()
        .expect("spawn failpoint child failed")
}

#[cfg(unix)]
fn assert_child_aborted(status: ExitStatus, msg: &str) {
    use std::os::unix::process::ExitStatusExt;

    assert_eq!(status.signal(), Some(6), "{msg}");
}

#[cfg(not(unix))]
fn assert_child_aborted(status: ExitStatus, msg: &str) {
    assert!(!status.success(), "{msg}");
}

fn open_with_tune<F>(db_root: &Path, tune: F) -> Mace
where
    F: FnOnce(&mut Options),
{
    let mut opt = Options::new(db_root);
    opt.tmp_store = false;
    if let Some(persisted) = load_persisted_global_options(db_root) {
        persisted.apply_to(&mut opt);
    }
    tune(&mut opt);
    Mace::new(opt.validate().expect("validate options failed")).expect("open mace failed")
}

fn open_manifest(db_root: &Path) -> BTree {
    let opt = Options::new(db_root);
    BTree::open(opt.manifest()).expect("open manifest failed")
}

fn load_persisted_global_options(db_root: &Path) -> Option<PersistedGlobalOptions> {
    let manifest_path = Options::new(db_root).manifest();
    match manifest_path.try_exists() {
        Ok(true) => {}
        Ok(false) => return None,
        Err(err) => panic!("check manifest path failed: {err:?}"),
    }

    let manifest = BTree::open(manifest_path).expect("open manifest failed");
    let raw = manifest
        .view(BUCKET_MISC, |txn| txn.get(OPTIONS_KEY))
        .expect("load persisted global options failed");
    Some(serde_json::from_slice(&raw).expect("decode persisted global options failed"))
}

fn manifest_bucket_exists(manifest: &BTree, bucket: &str) -> bool {
    match manifest.view(bucket, |_txn| Ok(())) {
        Ok(()) => true,
        Err(BTreeError::NotFound) => false,
        Err(err) => panic!("manifest bucket view failed for {bucket}: {err:?}"),
    }
}

fn manifest_bucket_keys(manifest: &BTree, bucket: &str) -> Option<Vec<Vec<u8>>> {
    match manifest.view(bucket, |txn| {
        let mut iter = txn.iter_uncached();
        let mut out = Vec::new();
        let mut k = Vec::new();
        let mut v = Vec::new();
        while iter.next_ref(&mut k, &mut v) {
            out.push(k.clone());
        }
        Ok(out)
    }) {
        Ok(keys) => Some(keys),
        Err(BTreeError::NotFound) => None,
        Err(err) => panic!("manifest bucket view failed for {bucket}: {err:?}"),
    }
}

fn manifest_bucket_has_key(manifest: &BTree, bucket: &str, key: &[u8]) -> bool {
    match manifest.view(bucket, |txn| txn.get(key)) {
        Ok(_) => true,
        Err(BTreeError::NotFound) => false,
        Err(err) => panic!("manifest key lookup failed for {bucket}: {err:?}"),
    }
}

fn wal_recycle_key(group_id: u8) -> Vec<u8> {
    format!("{WAL_RECYCLE_PREFIX}{group_id}").into_bytes()
}

fn wal_recycle_state_bytes(db_root: &Path, group_id: u8) -> Option<Vec<u8>> {
    let manifest = open_manifest(db_root);
    match manifest.view(BUCKET_MISC, |txn| txn.get(wal_recycle_key(group_id))) {
        Ok(val) => Some(val),
        Err(BTreeError::NotFound) => None,
        Err(err) => panic!("load wal recycle state failed: {err:?}"),
    }
}

fn wal_recycle_stage(db_root: &Path, group_id: u8) -> Option<u8> {
    wal_recycle_state_bytes(db_root, group_id).map(|bytes| {
        *bytes
            .get(1)
            .expect("wal recycle state must contain stage byte")
    })
}

fn pending_bucket_ids(db_root: &Path) -> BTreeSet<u64> {
    let manifest = open_manifest(db_root);
    let Some(keys) = manifest_bucket_keys(&manifest, BUCKET_PENDING_DEL) else {
        return BTreeSet::new();
    };
    keys.into_iter()
        .map(|key| {
            let raw: [u8; 8] = key[..8].try_into().expect("pending_del key must be u64");
            u64::from_le_bytes(raw)
        })
        .collect()
}

fn bucket_name_present(db_root: &Path, name: &str) -> bool {
    let manifest = open_manifest(db_root);
    manifest_bucket_has_key(&manifest, BUCKET_METAS, name.as_bytes())
}

fn aux_bucket_presence(db_root: &Path, bucket_id: u64) -> (bool, bool, bool) {
    let manifest = open_manifest(db_root);
    (
        manifest_bucket_exists(&manifest, &page_table_name(bucket_id)),
        manifest_bucket_exists(&manifest, &data_interval_name(bucket_id)),
        manifest_bucket_exists(&manifest, &blob_interval_name(bucket_id)),
    )
}

fn child_setup_common(db_root: &Path) -> (Mace, Bucket) {
    let mace = open_with_tune(db_root, |opt| {
        opt.sync_on_write = true;
        opt.data_file_size = 16 << 10;
        opt.wal_buffer_size = 32 << 10;
        opt.wal_file_size = 8 << 10;
        opt.gc_timeout = 20;
        opt.gc_eager = true;
        opt.data_garbage_ratio = 1;
    });

    let bucket = match mace.get_bucket("prod") {
        Ok(bucket) => bucket,
        Err(OpCode::NotFound) => mace
            .new_bucket(
                "prod",
                BucketOptions {
                    inline_size: 4096,
                    cache_evict_pct: 10,
                    checkpoint_size: 32 << 10,
                    pool_capacity: 64 << 10,
                    enable_backpressure: false,
                    ..BucketOptions::default()
                },
            )
            .expect("create prod bucket failed"),
        Err(err) => panic!("open prod bucket failed: {err:?}"),
    };

    (mace, bucket)
}

fn child_setup_gc(db_root: &Path) -> (Mace, Bucket) {
    let mace = open_with_tune(db_root, |opt| {
        opt.concurrent_write = 1;
        opt.sync_on_write = true;
        opt.data_file_size = 128 << 10;
        opt.wal_buffer_size = 1 << 20;
        opt.wal_file_size = 1 << 20;
        opt.gc_timeout = 20;
        opt.gc_eager = true;
        opt.data_garbage_ratio = 1;
        opt.blob_garbage_ratio = 1;
        opt.blob_file_size = 128 << 10;
    });

    let bucket = match mace.get_bucket("prod") {
        Ok(bucket) => bucket,
        Err(OpCode::NotFound) => mace
            .new_bucket(
                "prod",
                BucketOptions {
                    inline_size: 8192,
                    cache_evict_pct: 10,
                    checkpoint_size: 32 << 10,
                    pool_capacity: 64 << 10,
                    enable_backpressure: false,
                    ..BucketOptions::default()
                },
            )
            .expect("create prod bucket failed"),
        Err(err) => panic!("open prod bucket failed: {err:?}"),
    };

    (mace, bucket)
}

fn child_setup_data_gc(db_root: &Path) -> (Mace, Bucket) {
    let mace = open_with_tune(db_root, |opt| {
        opt.concurrent_write = 1;
        opt.sync_on_write = true;
        opt.data_file_size = 16 << 10;
        opt.wal_buffer_size = 1 << 20;
        opt.wal_file_size = 1 << 20;
        opt.gc_timeout = 20;
        opt.gc_eager = true;
        // keep data rewrite gate always open in this failpoint harness
        // the test target here is crash-window closure after entering rewrite path
        opt.data_garbage_ratio = 0;
        opt.blob_garbage_ratio = 1;
        opt.blob_file_size = 128 << 10;
    });

    let bucket = match mace.get_bucket("prod") {
        Ok(bucket) => bucket,
        Err(OpCode::NotFound) => mace
            .new_bucket(
                "prod",
                BucketOptions {
                    inline_size: 8192,
                    cache_evict_pct: 10,
                    checkpoint_size: 32 << 10,
                    pool_capacity: 64 << 10,
                    enable_backpressure: false,
                    ..BucketOptions::default()
                },
            )
            .expect("create prod bucket failed"),
        Err(err) => panic!("open prod bucket failed: {err:?}"),
    };

    (mace, bucket)
}

fn child_setup_retire(db_root: &Path) -> (Mace, Bucket) {
    let mace = open_with_tune(db_root, |opt| {
        opt.sync_on_write = true;
        opt.data_file_size = 16 << 10;
        opt.wal_buffer_size = 64 << 10;
        opt.wal_file_size = 8 << 10;
        opt.gc_timeout = 60_000;
        opt.gc_eager = false;
        opt.data_garbage_ratio = 1;
    });

    let bucket = match mace.get_bucket("prod") {
        Ok(bucket) => bucket,
        Err(OpCode::NotFound) => mace
            .new_bucket(
                "prod",
                BucketOptions {
                    inline_size: 512,
                    cache_evict_pct: 10,
                    checkpoint_size: 32 << 10,
                    pool_capacity: 64 << 10,
                    enable_backpressure: false,
                    ..BucketOptions::default()
                },
            )
            .expect("create prod bucket failed"),
        Err(err) => panic!("open prod bucket failed: {err:?}"),
    };

    (mace, bucket)
}

fn seed_bucket_delete_target(db_root: &Path) -> u64 {
    let mace = open_with_tune(db_root, |opt| {
        opt.sync_on_write = true;
        opt.data_file_size = 16 << 10;
        opt.wal_buffer_size = 32 << 10;
        opt.wal_file_size = 8 << 10;
        opt.gc_timeout = 60_000;
        opt.gc_eager = false;
    });
    let bucket = mace
        .new_bucket(
            "victim",
            BucketOptions {
                inline_size: 512,
                cache_evict_pct: 10,
                checkpoint_size: 32 << 10,
                pool_capacity: 64 << 10,
                enable_backpressure: false,
                ..BucketOptions::default()
            },
        )
        .expect("create victim bucket failed");
    let bucket_id = bucket.id();
    let payload = vec![b'v'; 2048];

    for round in 0..32 {
        let txn = bucket.begin().expect("begin victim seed txn failed");
        for idx in 0..16 {
            txn.upsert(format!("k_{round}_{idx}"), &payload)
                .expect("seed victim key failed");
        }
        txn.commit().expect("commit victim seed txn failed");
    }

    bucket.checkpoint();
    mace.sync().expect("sync victim bucket failed");
    drop(bucket);
    bucket_id
}

fn seed_pending_bucket_reap_target(db_root: &Path) -> u64 {
    let bucket_id = seed_bucket_delete_target(db_root);
    let mace = open_with_tune(db_root, |opt| {
        opt.sync_on_write = true;
        opt.data_file_size = 16 << 10;
        opt.wal_buffer_size = 32 << 10;
        opt.wal_file_size = 8 << 10;
        opt.gc_timeout = 60_000;
        opt.gc_eager = false;
    });
    while mace.del_bucket("victim") == Err(OpCode::Again) {
        std::thread::sleep(Duration::from_millis(10));
    }
    drop(mace);
    bucket_id
}

fn child_setup_wal_recycle(db_root: &Path) -> (Mace, Bucket) {
    let mace = open_with_tune(db_root, |opt| {
        opt.concurrent_write = 1;
        opt.sync_on_write = true;
        opt.data_file_size = 16 << 10;
        opt.wal_buffer_size = 8 << 10;
        opt.wal_file_size = 4 << 10;
        opt.gc_timeout = 60_000;
        opt.gc_eager = false;
    });

    let bucket = match mace.get_bucket("prod") {
        Ok(bucket) => bucket,
        Err(OpCode::NotFound) => mace
            .new_bucket(
                "prod",
                BucketOptions {
                    inline_size: 512,
                    cache_evict_pct: 10,
                    enable_backpressure: false,
                    ..BucketOptions::default()
                },
            )
            .expect("create prod bucket failed"),
        Err(err) => panic!("open prod bucket failed: {err:?}"),
    };

    (mace, bucket)
}

fn child_setup_wal_recycle_keep_stable(db_root: &Path) -> (Mace, Bucket) {
    let mace = open_with_tune(db_root, |opt| {
        opt.concurrent_write = 1;
        opt.sync_on_write = true;
        opt.data_file_size = 16 << 10;
        opt.wal_buffer_size = 8 << 10;
        opt.wal_file_size = 4 << 10;
        opt.gc_timeout = 60_000;
        opt.gc_eager = false;
        opt.keep_stable_wal_file = true;
    });

    let bucket = match mace.get_bucket("prod") {
        Ok(bucket) => bucket,
        Err(OpCode::NotFound) => mace
            .new_bucket(
                "prod",
                BucketOptions {
                    inline_size: 512,
                    cache_evict_pct: 10,
                    enable_backpressure: false,
                    ..BucketOptions::default()
                },
            )
            .expect("create prod bucket failed"),
        Err(err) => panic!("open prod bucket failed: {err:?}"),
    };

    (mace, bucket)
}

fn seed_committed_and_uncommitted(bucket: &Bucket, committed: usize, uncommitted: usize) {
    let txn = bucket.begin().expect("begin committed txn failed");
    for idx in 0..committed {
        txn.put(format!("k_{idx}"), format!("v_{idx}"))
            .expect("put committed key failed");
    }
    txn.commit().expect("commit committed txn failed");

    let txn = bucket.begin().expect("begin uncommitted txn failed");
    for idx in 0..uncommitted {
        txn.put(format!("u_{idx}"), format!("u_{idx}"))
            .expect("put uncommitted key failed");
    }
}

fn drive_flush_pressure(bucket: &Bucket, rounds: usize, value_size: usize) {
    let payload = vec![b'x'; value_size];
    for round in 0..rounds {
        let txn = bucket.begin().expect("begin flush txn failed");
        for idx in 0..64 {
            txn.upsert(format!("w_{round}_{idx}"), &payload)
                .expect("upsert flush key failed");
        }
        txn.commit().expect("commit flush txn failed");
    }
}

fn drive_gc_pressure(bucket: &Bucket, rounds: usize) {
    let txn = bucket.begin().expect("begin data gc seed txn failed");
    for idx in 0..2048 {
        txn.upsert(format!("k_{idx:05}"), format!("seed_{idx}"))
            .expect("seed data gc key failed");
    }
    txn.commit().expect("commit data gc seed txn failed");

    for round in 0..rounds {
        let txn = bucket.begin().expect("begin gc txn failed");
        for idx in 0..512 {
            let key_idx = (idx * 2) % 2048;
            txn.upsert(format!("k_{key_idx:05}"), format!("g_{round}_{idx}"))
                .expect("upsert gc key failed");
        }
        txn.commit().expect("commit gc txn failed");
    }
}

fn drive_blob_gc_pressure(bucket: &Bucket, rounds: usize, blob_size: usize) {
    let payload = vec![b'b'; blob_size];
    let txn = bucket.begin().expect("begin blob seed txn failed");
    for idx in 0..96 {
        txn.upsert(format!("blob_{idx}"), &payload)
            .expect("seed blob key failed");
        txn.upsert(format!("meta_{idx}"), format!("seed_{idx}"))
            .expect("seed meta key failed");
    }
    txn.commit().expect("commit blob seed txn failed");
    bucket.checkpoint();

    for round in 0..rounds {
        let txn = bucket.begin().expect("begin blob txn failed");
        for idx in 0..48 {
            let key_idx = idx * 2;
            txn.upsert(format!("blob_{key_idx}"), &payload)
                .expect("upsert blob key failed");
            txn.upsert(format!("meta_{key_idx}"), format!("m_{round}_{idx}"))
                .expect("upsert meta key failed");
        }
        txn.commit().expect("commit blob txn failed");
        if round % 4 == 3 {
            bucket.checkpoint();
        }
    }

    bucket.checkpoint();
}

fn assert_visibility_after_reopen(db_root: &Path, committed: usize, uncommitted: usize) {
    let mace = open_with_tune(db_root, |_opt| {});
    let bucket = mace.get_bucket("prod").expect("bucket prod should exist");
    let view = bucket.view().expect("open verify view failed");

    for idx in 0..committed {
        let key = format!("k_{idx}");
        let val = view.get(&key).expect("committed key missing");
        assert_eq!(val.slice(), format!("v_{idx}").as_bytes());
    }

    for idx in 0..uncommitted {
        let key = format!("u_{idx}");
        assert!(view.get(&key).is_err());
    }
}

fn assert_bucket_readable(db_root: &Path) {
    let mace = open_with_tune(db_root, |_opt| {});
    let bucket = mace.get_bucket("prod").expect("bucket prod should exist");
    let view = bucket.view().expect("open post-crash view failed");

    for idx in 0..16 {
        let key = format!("k_{idx}");
        let _ = view.get(&key);
    }
}

fn assert_bucket_exists_after_reopen(db_root: &Path, name: &str) {
    let mace = open_with_tune(db_root, |_opt| {});
    let bucket = mace
        .get_bucket(name)
        .expect("bucket should exist after reopen");
    let _view = bucket.view().expect("open bucket view after reopen failed");
}

fn assert_bucket_missing_after_reopen(db_root: &Path, name: &str) {
    let mace = open_with_tune(db_root, |_opt| {});
    match mace.get_bucket(name) {
        Err(OpCode::NotFound) => {}
        Err(err) => panic!("bucket reopen should return NotFound, got {err:?}"),
        Ok(_) => panic!("bucket should be missing after reopen"),
    }
}

fn assert_pending_bucket_survives_reopen(db_root: &Path, bucket_id: u64) {
    let pending = pending_bucket_ids(db_root);
    assert!(
        pending.contains(&bucket_id),
        "pending bucket id {bucket_id} should survive reopen"
    );
    let (has_page, has_data_ivl, has_blob_ivl) = aux_bucket_presence(db_root, bucket_id);
    assert!(
        has_page || has_data_ivl || has_blob_ivl,
        "pending bucket should keep at least one aux bucket before reap commit"
    );
}

fn assert_pending_bucket_survives_reopen_without_aux(db_root: &Path, bucket_id: u64) {
    let pending = pending_bucket_ids(db_root);
    assert!(
        pending.contains(&bucket_id),
        "pending bucket id {bucket_id} should survive reopen"
    );
    let (has_page, has_data_ivl, has_blob_ivl) = aux_bucket_presence(db_root, bucket_id);
    assert!(
        !has_page && !has_data_ivl && !has_blob_ivl,
        "pending bucket should keep no aux bucket after finalize-before-commit crash"
    );
}

fn assert_pending_bucket_reaped_after_reopen(db_root: &Path, bucket_id: u64) {
    let pending = pending_bucket_ids(db_root);
    assert!(
        !pending.contains(&bucket_id),
        "pending bucket id {bucket_id} should be cleared after reap commit"
    );
    let (has_page, has_data_ivl, has_blob_ivl) = aux_bucket_presence(db_root, bucket_id);
    assert!(
        !has_page && !has_data_ivl && !has_blob_ivl,
        "reaped bucket should not keep aux buckets after reopen"
    );
}

fn assert_rewrite_visibility_after_reopen(db_root: &Path) {
    let mace = open_with_tune(db_root, |opt| {
        opt.gc_timeout = 20;
        opt.gc_eager = true;
    });
    let bucket = mace.get_bucket("prod").expect("bucket prod should exist");
    let view = bucket.view().expect("open post-crash view failed");
    let payload = vec![b'r'; 1024];
    for idx in 0..16 {
        let key = format!("rk_{idx}");
        let val = view.get(&key).expect("rewrite key missing after reopen");
        assert_eq!(val.slice(), payload.as_slice());
    }
    for _ in 0..4 {
        mace.start_gc();
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn assert_rewrite_visibility_after_reopen_multi_bucket(db_root: &Path) {
    let mace = open_with_tune(db_root, |opt| {
        opt.gc_timeout = 20;
        opt.gc_eager = true;
    });
    let bucket1 = mace.get_bucket("prod").expect("bucket prod should exist");
    let bucket2 = mace.get_bucket("prod2").expect("bucket prod2 should exist");
    let view1 = bucket1.view().expect("open post-crash view1 failed");
    let view2 = bucket2.view().expect("open post-crash view2 failed");
    let payload = vec![b'r'; 1024];
    for idx in 0..16 {
        let key1 = format!("rk_a_{idx}");
        let key2 = format!("rk_b_{idx}");
        let val1 = view1
            .get(&key1)
            .expect("bucket1 rewrite key missing after reopen");
        let val2 = view2
            .get(&key2)
            .expect("bucket2 rewrite key missing after reopen");
        assert_eq!(val1.slice(), payload.as_slice());
        assert_eq!(val2.slice(), payload.as_slice());
    }
    for _ in 0..4 {
        mace.start_gc();
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn data_blob_files(db_root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let root = db_root.join("data");
    let entries = std::fs::read_dir(&root).expect("read data dir failed");
    for entry in entries {
        let entry = entry.expect("read data dir entry failed");
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|x| x.to_str()) else {
            continue;
        };
        if name.starts_with("data_") || name.starts_with("blob_") {
            files.push(path);
        }
    }
    files.sort();
    files
}

fn wal_files(db_root: &Path, group: u8) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let root = db_root.join("log");
    let prefix = format!("wal_{group}_");
    let entries = std::fs::read_dir(&root).expect("read log dir failed");
    for entry in entries {
        let entry = entry.expect("read log dir entry failed");
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|x| x.to_str()) else {
            continue;
        };
        if name.starts_with(&prefix) {
            files.push(path);
        }
    }
    files.sort();
    files
}

fn wait_for_data_dir_quiet(db_root: &Path, quiet: Duration, timeout: Duration) {
    let root = db_root.join("data");
    let fingerprint = || -> (u64, u64) {
        let mut files = 0u64;
        let mut bytes = 0u64;
        if let Ok(entries) = std::fs::read_dir(&root) {
            for entry in entries.flatten() {
                let path = entry.path();
                if !path.is_file() {
                    continue;
                }
                let Some(name) = path.file_name().and_then(|x| x.to_str()) else {
                    continue;
                };
                if !name.starts_with("data_") && !name.starts_with("blob_") {
                    continue;
                }
                files += 1;
                bytes = bytes.saturating_add(entry.metadata().map(|m| m.len()).unwrap_or(0));
            }
        }
        (files, bytes)
    };

    let deadline = Instant::now() + timeout;
    let mut last = fingerprint();
    let mut stable_since = Instant::now();
    while Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(50));
        let now = fingerprint();
        if now == last {
            if stable_since.elapsed() >= quiet {
                return;
            }
        } else {
            last = now;
            stable_since = Instant::now();
        }
    }
    panic!("data dir did not become quiet in expected window");
}

fn wait_for_crash(timeout: Duration) -> ! {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("failpoint did not fire in expected window")
}

fn child_case_flush_after_data_sync(db_root: &Path) -> ! {
    let (_mace, bucket) = child_setup_common(db_root);
    seed_committed_and_uncommitted(&bucket, 64, 24);
    drive_flush_pressure(&bucket, 128, 2048);
    wait_for_crash(Duration::from_secs(20))
}

fn child_case_flush_after_manifest_commit_with_retire(db_root: &Path) -> ! {
    let (_mace, bucket) = child_setup_retire(db_root);
    let payload = vec![b'r'; 1024];

    for idx in 0..128 {
        let txn = bucket.begin().expect("begin seed txn failed");
        txn.put(format!("rk_{idx}"), &payload)
            .expect("seed put failed");
        txn.commit().expect("seed commit failed");
    }

    for round in 0..512 {
        let txn = bucket.begin().expect("begin rewrite txn failed");
        for idx in 0..128 {
            txn.upsert(format!("rk_{idx}"), &payload)
                .expect("rewrite upsert failed");
        }
        txn.commit().expect("rewrite commit failed");
        if round % 16 == 0 {
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    wait_for_crash(Duration::from_secs(20))
}

fn child_case_flush_after_manifest_commit_with_retire_multi_bucket(db_root: &Path) -> ! {
    let (mace, bucket1) = child_setup_retire(db_root);
    let bucket2 = match mace.get_bucket("prod2") {
        Ok(bucket) => bucket,
        Err(OpCode::NotFound) => mace
            .new_bucket("prod2", BucketOptions::default())
            .expect("create prod2 bucket failed"),
        Err(err) => panic!("open prod2 bucket failed: {err:?}"),
    };
    let payload = vec![b'r'; 1024];

    for idx in 0..128 {
        let txn1 = bucket1.begin().expect("begin seed txn1 failed");
        txn1.put(format!("rk_a_{idx}"), &payload)
            .expect("seed put bucket1 failed");
        txn1.commit().expect("seed commit bucket1 failed");

        let txn2 = bucket2.begin().expect("begin seed txn2 failed");
        txn2.put(format!("rk_b_{idx}"), &payload)
            .expect("seed put bucket2 failed");
        txn2.commit().expect("seed commit bucket2 failed");
    }

    for round in 0..512 {
        let txn1 = bucket1.begin().expect("begin rewrite txn1 failed");
        let txn2 = bucket2.begin().expect("begin rewrite txn2 failed");
        for idx in 0..64 {
            txn1.upsert(format!("rk_a_{idx}"), &payload)
                .expect("rewrite upsert bucket1 failed");
            txn2.upsert(format!("rk_b_{idx}"), &payload)
                .expect("rewrite upsert bucket2 failed");
        }
        txn1.commit().expect("rewrite commit bucket1 failed");
        txn2.commit().expect("rewrite commit bucket2 failed");
        if round % 16 == 0 {
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    wait_for_crash(Duration::from_secs(20))
}

fn child_case_data_obsolete_reclaim(db_root: &Path) -> ! {
    let mace = open_with_tune(db_root, |opt| {
        opt.concurrent_write = 1;
        opt.sync_on_write = true;
        opt.data_file_size = 16 << 10;
        opt.wal_buffer_size = 1 << 20;
        opt.wal_file_size = 1 << 20;
        opt.gc_timeout = 60_000;
        opt.gc_eager = false;
        opt.data_garbage_ratio = 100;
        opt.blob_garbage_ratio = 100;
    });
    let bucket = match mace.get_bucket("prod") {
        Ok(bucket) => bucket,
        Err(OpCode::NotFound) => mace
            .new_bucket(
                "prod",
                BucketOptions {
                    inline_size: 8192,
                    cache_evict_pct: 10,
                    checkpoint_size: 32 << 10,
                    pool_capacity: 64 << 10,
                    enable_backpressure: false,
                    ..BucketOptions::default()
                },
            )
            .expect("create prod bucket failed"),
        Err(err) => panic!("open prod bucket failed: {err:?}"),
    };
    let payload = vec![b'd'; 1024];

    let seed = bucket.begin().expect("begin data obsolete seed failed");
    for idx in 0..192 {
        seed.put(format!("dok_{idx:04}"), &payload)
            .expect("seed data obsolete key failed");
    }
    seed.commit().expect("commit data obsolete seed failed");
    bucket.checkpoint();

    let delete = bucket.begin().expect("begin data obsolete delete failed");
    for idx in 0..192 {
        delete
            .del(format!("dok_{idx:04}"))
            .expect("delete data obsolete key failed");
    }
    delete.commit().expect("commit data obsolete delete failed");

    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        bucket.checkpoint();
        mace.start_gc();
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("data obsolete failpoint did not fire")
}

fn child_case_blob_obsolete_reclaim(db_root: &Path) -> ! {
    let observer = Arc::new(InMemoryObserver::new(64));
    let mace = open_with_tune(db_root, |opt| {
        opt.concurrent_write = 1;
        opt.sync_on_write = true;
        opt.data_file_size = 16 << 10;
        opt.blob_file_size = 64 << 10;
        opt.wal_buffer_size = 1 << 20;
        opt.wal_file_size = 1 << 20;
        opt.gc_timeout = 60_000;
        opt.gc_eager = false;
        opt.data_garbage_ratio = 100;
        opt.blob_garbage_ratio = 100;
        opt.observer = observer.clone();
    });
    let bucket = match mace.get_bucket("prod") {
        Ok(bucket) => bucket,
        Err(OpCode::NotFound) => mace
            .new_bucket(
                "prod",
                BucketOptions {
                    inline_size: 512,
                    split_elems: 64,
                    consolidate_threshold: 16,
                    cache_evict_pct: 10,
                    checkpoint_size: 32 << 10,
                    pool_capacity: 64 << 10,
                    enable_backpressure: false,
                    ..BucketOptions::default()
                },
            )
            .expect("create prod bucket failed"),
        Err(err) => panic!("open prod bucket failed: {err:?}"),
    };
    let payload = vec![b'b'; 8192];

    let seed = bucket.begin().expect("begin blob obsolete seed failed");
    for idx in 0..64 {
        seed.put(format!("bok_{idx:04}"), &payload)
            .expect("seed blob obsolete key failed");
    }
    seed.commit().expect("commit blob obsolete seed failed");
    bucket.checkpoint();

    let update = bucket.begin().expect("begin blob obsolete update failed");
    for idx in 0..64 {
        update
            .update(format!("bok_{idx:04}"), &payload)
            .expect("update blob obsolete key failed");
    }
    update.commit().expect("commit blob obsolete update failed");

    let before = counter_value(&observer, CounterMetric::TreeNodeConsolidate);
    for _ in 0..3 {
        let consolidate = bucket
            .begin()
            .expect("begin blob consolidate trigger failed");
        for _ in 0..32 {
            consolidate
                .update("bok_0000", &payload)
                .expect("update blob consolidate trigger failed");
        }
        consolidate
            .commit()
            .expect("commit blob consolidate trigger failed");
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        counter_value(&observer, CounterMetric::TreeNodeConsolidate) >= before + 3,
        "expected repeated foreground consolidation"
    );
    bucket.checkpoint();

    for idx in 0..128 {
        let barrier = bucket.begin().expect("begin blob obsolete barrier failed");
        barrier
            .put(format!("blob_obsolete_barrier_{idx:04}"), b"barrier")
            .expect("put blob obsolete barrier failed");
        barrier
            .commit()
            .expect("commit blob obsolete barrier failed");
    }

    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        bucket.checkpoint();
        mace.start_gc();
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("blob obsolete failpoint did not fire")
}

fn child_case_wal_after_checkpoint_write(db_root: &Path) -> ! {
    let (_mace, bucket) = child_setup_common(db_root);
    seed_committed_and_uncommitted(&bucket, 64, 24);

    for round in 0..512 {
        let txn = bucket.begin().expect("begin wal txn failed");
        txn.upsert(format!("wal_{round}"), format!("wal_v_{round}"))
            .expect("upsert wal key failed");
        txn.commit().expect("commit wal txn failed");
    }

    wait_for_crash(Duration::from_secs(20))
}

fn child_case_manifest_before_multi_commit(db_root: &Path) -> ! {
    let (_mace, bucket) = child_setup_common(db_root);
    seed_committed_and_uncommitted(&bucket, 64, 24);
    drive_flush_pressure(&bucket, 128, 1536);
    wait_for_crash(Duration::from_secs(20))
}

fn child_case_wal_recycle_before_dir_sync(db_root: &Path) -> ! {
    let (mace, bucket) = child_setup_wal_recycle(db_root);
    seed_committed_and_uncommitted(&bucket, 64, 24);
    let deadline = Instant::now() + Duration::from_secs(20);
    let payload = vec![b'w'; 1024];
    let mut round = 0usize;

    while Instant::now() < deadline {
        let txn = bucket.begin().expect("begin wal recycle txn failed");
        for idx in 0..64 {
            txn.upsert(format!("rw_{round}_{idx}"), &payload)
                .expect("upsert wal recycle key failed");
        }
        txn.commit().expect("commit wal recycle txn failed");
        if round.is_multiple_of(4) {
            bucket.checkpoint();
            mace.start_gc();
        }
        round += 1;
    }

    panic!("wal recycle failpoint did not fire")
}

fn child_case_wal_recycle_before_dir_sync_keep_stable(db_root: &Path) -> ! {
    let (mace, bucket) = child_setup_wal_recycle_keep_stable(db_root);
    seed_committed_and_uncommitted(&bucket, 64, 24);
    let deadline = Instant::now() + Duration::from_secs(20);
    let payload = vec![b'w'; 1024];
    let mut round = 0usize;

    while Instant::now() < deadline {
        let txn = bucket.begin().expect("begin wal recycle txn failed");
        for idx in 0..64 {
            txn.upsert(format!("rw_{round}_{idx}"), &payload)
                .expect("upsert wal recycle key failed");
        }
        txn.commit().expect("commit wal recycle txn failed");
        if round.is_multiple_of(4) {
            bucket.checkpoint();
            mace.start_gc();
        }
        round += 1;
    }

    panic!("wal recycle keep-stable failpoint did not fire")
}

fn child_case_wal_recycle_reopen(db_root: &Path) -> ! {
    let _ = child_setup_wal_recycle(db_root);
    panic!("recovery wal recycle failpoint did not fire")
}

fn child_case_wal_recycle_reopen_keep_stable(db_root: &Path) -> ! {
    let _ = child_setup_wal_recycle_keep_stable(db_root);
    panic!("recovery wal recycle keep-stable failpoint did not fire")
}

fn child_case_wal_recycle_reopen_expect_io(db_root: &Path, keep_stable: bool) {
    let mut opt = Options::new(db_root);
    opt.concurrent_write = 1;
    opt.sync_on_write = true;
    opt.data_file_size = 16 << 10;
    opt.wal_buffer_size = 8 << 10;
    opt.wal_file_size = 4 << 10;
    opt.gc_timeout = 60_000;
    opt.gc_eager = false;
    opt.keep_stable_wal_file = keep_stable;
    let res = Mace::new(opt.validate().expect("validate options failed"));
    let err = res.err().expect("recovery reopen must fail with io error");
    assert_eq!(err, OpCode::IoError);
}

fn child_case_reopen_common(db_root: &Path) -> ! {
    let _ = child_setup_common(db_root);
    panic!("recovery failpoint did not fire")
}

fn child_case_txn_commit_abort_window(db_root: &Path) -> ! {
    let (_mace, bucket) = child_setup_common(db_root);
    seed_committed_and_uncommitted(&bucket, 64, 24);

    let deadline = Instant::now() + Duration::from_secs(20);
    let mut round = 0usize;

    while Instant::now() < deadline {
        let txn = bucket.begin().expect("begin txn-window txn failed");
        txn.upsert(format!("txn_k_{round}"), format!("txn_v_{round}"))
            .expect("upsert txn-window key failed");
        let _ = txn.commit();
        round += 1;
    }

    panic!("txn commit failpoint did not fire")
}

fn child_case_gc_data_before_meta_commit(db_root: &Path) -> ! {
    let (mace, bucket) = child_setup_data_gc(db_root);
    seed_committed_and_uncommitted(&bucket, 64, 0);
    drive_gc_pressure(&bucket, 256);
    mace.sync().expect("sync before data gc failed");
    wait_for_data_dir_quiet(db_root, Duration::from_millis(300), Duration::from_secs(20));

    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        mace.start_gc();
        std::thread::sleep(Duration::from_millis(20));
    }

    panic!("gc data failpoint did not fire")
}

fn child_case_gc_blob_before_meta_commit(db_root: &Path) -> ! {
    let (mace, bucket) = child_setup_gc(db_root);

    seed_committed_and_uncommitted(&bucket, 64, 0);
    drive_blob_gc_pressure(&bucket, 24, 16 << 10);
    mace.sync().expect("sync before blob gc failed");
    wait_for_data_dir_quiet(db_root, Duration::from_millis(300), Duration::from_secs(20));

    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        mace.start_gc();
        std::thread::sleep(Duration::from_millis(20));
    }

    panic!("gc blob failpoint did not fire")
}

fn child_case_evictor_before_evict_once(db_root: &Path) -> ! {
    let mace = open_with_tune(db_root, |opt| {
        opt.sync_on_write = false;
        opt.data_file_size = 16 << 10;
        opt.wal_buffer_size = 32 << 10;
        opt.wal_file_size = 8 << 10;
    });
    let bucket = mace
        .new_bucket(
            "prod",
            BucketOptions {
                cache_capacity: 1 << 20,
                cache_evict_pct: 100,
                ..BucketOptions::default()
            },
        )
        .expect("create prod bucket failed");

    let deadline = Instant::now() + Duration::from_secs(30);
    let payload = vec![b'e'; 4 << 10];
    let mut round = 0usize;

    while Instant::now() < deadline {
        let txn = bucket.begin().expect("begin evictor txn failed");
        for idx in 0..256 {
            txn.upsert(format!("ev_{round}_{idx}"), &payload)
                .expect("upsert evictor key failed");
        }
        txn.commit().expect("commit evictor txn failed");

        round += 1;
        if round.is_multiple_of(8) {
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    panic!("evictor failpoint did not fire")
}

fn child_case_recovery_abort_clean_seed(db_root: &Path) {
    let mace = open_with_tune(db_root, |opt| {
        opt.tmp_store = false;
        opt.sync_on_write = true;
        opt.concurrent_write = 1;
        opt.gc_timeout = 60_000;
        opt.data_file_size = 16 << 10;
        opt.wal_buffer_size = 8 << 10;
        opt.wal_file_size = 4 << 10;
    });
    let bucket = match mace.get_bucket("prod") {
        Ok(bucket) => bucket,
        Err(OpCode::NotFound) => mace
            .new_bucket(
                "prod",
                BucketOptions {
                    inline_size: 512,
                    cache_evict_pct: 10,
                    checkpoint_size: 32 << 10,
                    pool_capacity: 64 << 10,
                    enable_backpressure: false,
                    ..BucketOptions::default()
                },
            )
            .expect("create prod bucket failed"),
        Err(err) => panic!("open prod bucket failed: {err:?}"),
    };

    let seed = bucket.begin().expect("begin seed txn failed");
    seed.put("seed", "base").expect("seed put failed");
    seed.commit().expect("seed commit failed");

    let tx = bucket.begin().expect("begin abort-clean txn failed");
    tx.update("seed", "v1").expect("abort-clean update failed");
    drop(tx);
}

fn child_case_recovery_abort_clean_after_drain_before_start(db_root: &Path) -> ! {
    let _mace = open_with_tune(db_root, |opt| {
        opt.tmp_store = false;
        opt.sync_on_write = true;
        opt.concurrent_write = 1;
        opt.gc_timeout = 60_000;
        opt.data_file_size = 16 << 10;
        opt.wal_buffer_size = 8 << 10;
        opt.wal_file_size = 4 << 10;
    });
    panic!("recovery abort-clean after-drain failpoint did not fire")
}

fn child_case_recovery_abort_clean_post_start_gc(db_root: &Path) {
    let mace = open_with_tune(db_root, |opt| {
        opt.tmp_store = false;
        opt.sync_on_write = true;
        opt.concurrent_write = 1;
        opt.gc_timeout = 60_000;
        opt.data_file_size = 16 << 10;
        opt.wal_buffer_size = 8 << 10;
        opt.wal_file_size = 4 << 10;
    });
    let bucket = mace.get_bucket("prod").expect("bucket prod should exist");
    let view = bucket.view().expect("open verify view failed");
    let val = view.get("seed").expect("seed key missing after reopen");
    assert_eq!(val.slice(), b"base");
    drop(view);
    drop(bucket);

    mace.start_gc();
}

fn child_case_bucket_create(db_root: &Path) -> ! {
    let mace = open_with_tune(db_root, |opt| {
        opt.sync_on_write = true;
        opt.data_file_size = 16 << 10;
        opt.wal_buffer_size = 32 << 10;
        opt.wal_file_size = 8 << 10;
        opt.gc_timeout = 60_000;
        opt.gc_eager = false;
    });
    let _bucket = mace
        .new_bucket(
            "create_fp",
            BucketOptions {
                inline_size: 512,
                cache_evict_pct: 10,
                checkpoint_size: 32 << 10,
                pool_capacity: 64 << 10,
                enable_backpressure: false,
                ..BucketOptions::default()
            },
        )
        .expect("create failpoint bucket failed");
    panic!("bucket create failpoint did not fire")
}

fn child_case_bucket_delete(db_root: &Path) -> ! {
    let mace = open_with_tune(db_root, |opt| {
        opt.sync_on_write = true;
        opt.data_file_size = 16 << 10;
        opt.wal_buffer_size = 32 << 10;
        opt.wal_file_size = 8 << 10;
        opt.gc_timeout = 60_000;
        opt.gc_eager = false;
    });
    mace.del_bucket("victim")
        .expect("delete failpoint bucket failed");
    panic!("bucket delete failpoint did not fire")
}

fn child_case_pending_bucket_reap(db_root: &Path) -> ! {
    let mace = open_with_tune(db_root, |opt| {
        opt.sync_on_write = true;
        opt.data_file_size = 16 << 10;
        opt.wal_buffer_size = 32 << 10;
        opt.wal_file_size = 8 << 10;
        opt.gc_timeout = 60_000;
        opt.gc_eager = false;
    });
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        mace.start_gc();
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("pending bucket reap failpoint did not fire")
}

#[test]
fn failpoint_child() {
    if std::env::var(ENV_CHILD).ok().as_deref() != Some("1") {
        return;
    }

    let case = std::env::var(ENV_CASE).expect("missing failpoint case");
    let db_root = PathBuf::from(std::env::var(ENV_DB_ROOT).expect("missing failpoint db root"));

    match case.as_str() {
        "flush_after_data_sync" => child_case_flush_after_data_sync(&db_root),
        "flush_after_data_dir_sync" => child_case_manifest_before_multi_commit(&db_root),
        "flush_before_manifest_commit" => child_case_manifest_before_multi_commit(&db_root),
        "flush_after_manifest_commit" => child_case_manifest_before_multi_commit(&db_root),
        "flush_after_manifest_commit_before_wal_checkpoint" => {
            child_case_manifest_before_multi_commit(&db_root)
        }
        "flush_after_manifest_commit_with_retire" => {
            child_case_flush_after_manifest_commit_with_retire(&db_root)
        }
        "flush_after_manifest_commit_with_retire_multi_bucket" => {
            child_case_flush_after_manifest_commit_with_retire_multi_bucket(&db_root)
        }
        "flush_after_old_stat_delta" => {
            child_case_flush_after_manifest_commit_with_retire(&db_root)
        }
        "data_obsolete_reclaim" => child_case_data_obsolete_reclaim(&db_root),
        "blob_obsolete_reclaim" => child_case_blob_obsolete_reclaim(&db_root),
        "wal_after_checkpoint_write" => child_case_wal_after_checkpoint_write(&db_root),
        "manifest_before_multi_commit" => child_case_manifest_before_multi_commit(&db_root),
        "wal_recycle_before_intent_commit" => child_case_wal_recycle_before_dir_sync(&db_root),
        "wal_recycle_after_remove_before_dir_sync" => {
            child_case_wal_recycle_before_dir_sync(&db_root)
        }
        "wal_recycle_done_windows" => child_case_wal_recycle_before_dir_sync(&db_root),
        "wal_recycle_done_windows_keep_stable" => {
            child_case_wal_recycle_before_dir_sync_keep_stable(&db_root)
        }
        "recovery_wal_recycle_done_windows" => child_case_wal_recycle_reopen(&db_root),
        "recovery_wal_recycle_done_windows_keep_stable" => {
            child_case_wal_recycle_reopen_keep_stable(&db_root)
        }
        "recovery_wal_recycle_expect_remove_io" => {
            child_case_wal_recycle_reopen_expect_io(&db_root, false)
        }
        "recovery_wal_recycle_expect_rename_io" => {
            child_case_wal_recycle_reopen_expect_io(&db_root, true)
        }
        "gc_data_rewrite_before_meta_commit" => child_case_gc_data_before_meta_commit(&db_root),
        "gc_data_rewrite_after_stage_marker" => child_case_gc_data_before_meta_commit(&db_root),
        "gc_data_rewrite_after_data_dir_sync" => child_case_gc_data_before_meta_commit(&db_root),
        "gc_data_rewrite_after_meta_commit" => child_case_gc_data_before_meta_commit(&db_root),
        "gc_blob_rewrite_before_meta_commit" => child_case_gc_blob_before_meta_commit(&db_root),
        "gc_blob_rewrite_after_stage_marker" => child_case_gc_blob_before_meta_commit(&db_root),
        "gc_blob_rewrite_after_data_dir_sync" => child_case_gc_blob_before_meta_commit(&db_root),
        "gc_blob_rewrite_after_meta_commit" => child_case_gc_blob_before_meta_commit(&db_root),
        "delete_files_after_dir_sync_before_meta_commit" => {
            child_case_gc_data_before_meta_commit(&db_root)
        }
        "recovery_orphan_cleanup_after_data_dir_sync_before_marker_clear" => {
            child_case_reopen_common(&db_root)
        }
        "recovery_abort_clean_seed" => child_case_recovery_abort_clean_seed(&db_root),
        "recovery_abort_clean_after_drain_before_start" => {
            child_case_recovery_abort_clean_after_drain_before_start(&db_root)
        }
        "recovery_abort_clean_post_start_gc" => {
            child_case_recovery_abort_clean_post_start_gc(&db_root)
        }
        "bucket_create" => child_case_bucket_create(&db_root),
        "bucket_delete" => child_case_bucket_delete(&db_root),
        "pending_bucket_reap" => child_case_pending_bucket_reap(&db_root),
        "txn_commit_after_record_commit" => child_case_txn_commit_abort_window(&db_root),
        "txn_commit_after_wal_sync" => child_case_txn_commit_abort_window(&db_root),
        "txn_commit_after_wal_file_sync_before_dir_sync" => {
            child_case_txn_commit_abort_window(&db_root)
        }
        "evictor_before_evict_once" => child_case_evictor_before_evict_once(&db_root),
        _ => panic!("unknown failpoint case: {case}"),
    }
}

#[test]
#[ignore]
fn chaos_failpoint_flush_after_data_sync() {
    let path = RandomPath::new();
    let status = spawn_child(
        "flush_after_data_sync",
        &path,
        "mace_flush_after_data_sync=abort@1",
    );
    assert_child_aborted(status, "flush failpoint child should abort");
    let crashed_files = data_blob_files(&path);
    assert!(
        !crashed_files.is_empty(),
        "expected flush crash to leave data/blob files before recovery"
    );
    assert_visibility_after_reopen(&path, 64, 24);
    for file in crashed_files {
        assert!(
            !file.exists(),
            "flush orphan file should be cleaned on reopen: {file:?}"
        );
    }
}

#[test]
#[ignore]
fn chaos_failpoint_flush_after_data_dir_sync() {
    let path = RandomPath::new();
    let status = spawn_child(
        "flush_after_data_dir_sync",
        &path,
        "mace_flush_after_data_dir_sync=abort@1",
    );
    assert_child_aborted(
        status,
        "flush-after-data-dir-sync failpoint child should abort",
    );
    let crashed_files = data_blob_files(&path);
    assert!(
        !crashed_files.is_empty(),
        "expected flush crash to leave data/blob files before recovery"
    );
    assert_visibility_after_reopen(&path, 64, 24);
    for file in crashed_files {
        assert!(
            !file.exists(),
            "flush orphan file should be cleaned on reopen: {file:?}"
        );
    }
}

#[test]
#[ignore]
fn chaos_failpoint_flush_before_manifest_commit() {
    let path = RandomPath::new();
    let status = spawn_child(
        "flush_before_manifest_commit",
        &path,
        "mace_flush_before_manifest_commit=abort@1",
    );
    assert_child_aborted(status, "flush-before-manifest failpoint child should abort");
    let crashed_files = data_blob_files(&path);
    assert!(
        !crashed_files.is_empty(),
        "expected flush crash to leave data/blob files before recovery"
    );
    assert_visibility_after_reopen(&path, 64, 24);
    for file in crashed_files {
        assert!(
            !file.exists(),
            "flush orphan file should be cleaned on reopen: {file:?}"
        );
    }
}

#[test]
#[ignore]
fn chaos_failpoint_flush_after_manifest_commit() {
    let path = RandomPath::new();
    let status = spawn_child(
        "flush_after_manifest_commit",
        &path,
        "mace_flush_after_manifest_commit=abort@1",
    );
    assert_child_aborted(status, "flush-after-manifest failpoint child should abort");
    let crashed_files = data_blob_files(&path);
    assert!(
        !crashed_files.is_empty(),
        "expected committed flush files before recovery"
    );
    assert_visibility_after_reopen(&path, 64, 24);
    assert!(
        crashed_files.iter().any(|file| file.exists()),
        "flush files committed before crash should survive recovery"
    );
}

#[test]
#[ignore]
fn chaos_failpoint_flush_after_manifest_commit_with_retire() {
    let path = RandomPath::new();
    let status = spawn_child(
        "flush_after_manifest_commit_with_retire",
        &path,
        "mace_flush_after_manifest_commit=abort@8",
    );
    assert_child_aborted(
        status,
        "flush-after-manifest-with-retire failpoint child should abort",
    );
    assert_rewrite_visibility_after_reopen(&path);
}

#[test]
#[ignore]
fn chaos_failpoint_flush_after_data_sync_with_retire() {
    let path = RandomPath::new();
    let status = spawn_child(
        "flush_after_manifest_commit_with_retire",
        &path,
        "mace_flush_after_data_sync=abort@8",
    );
    assert_child_aborted(
        status,
        "flush-after-data-sync-with-retire failpoint child should abort",
    );
    assert_rewrite_visibility_after_reopen(&path);
}

#[test]
#[ignore]
fn chaos_failpoint_flush_before_manifest_commit_with_retire() {
    let path = RandomPath::new();
    let status = spawn_child(
        "flush_after_manifest_commit_with_retire",
        &path,
        "mace_flush_before_manifest_commit=abort@8",
    );
    assert_child_aborted(
        status,
        "flush-before-manifest-with-retire failpoint child should abort",
    );
    assert_rewrite_visibility_after_reopen(&path);
}

#[test]
#[ignore]
fn chaos_failpoint_flush_after_manifest_commit_with_retire_multi_bucket() {
    let path = RandomPath::new();
    let status = spawn_child(
        "flush_after_manifest_commit_with_retire_multi_bucket",
        &path,
        "mace_flush_after_manifest_commit=abort@8",
    );
    assert_child_aborted(
        status,
        "flush-after-manifest-with-retire-multi-bucket failpoint child should abort",
    );
    assert_rewrite_visibility_after_reopen_multi_bucket(&path);
}

#[test]
#[ignore]
fn chaos_failpoint_flush_after_old_stat_delta() {
    let path = RandomPath::new();
    let status = spawn_child(
        "flush_after_old_stat_delta",
        &path,
        "mace_flush_after_old_stat_delta=abort@1",
    );
    assert_child_aborted(status, "flush-after-old-stat-delta child should abort");
    assert_rewrite_visibility_after_reopen(&path);
}

#[test]
#[ignore]
fn chaos_failpoint_gc_data_obsolete_after_meta_commit() {
    let path = RandomPath::new();
    let status = spawn_child(
        "data_obsolete_reclaim",
        &path,
        "mace_gc_data_obsolete_after_meta_commit=abort@1",
    );
    assert_child_aborted(status, "data-obsolete-after-meta child should abort");
    assert_bucket_readable(&path);
}

#[test]
#[ignore]
fn chaos_failpoint_gc_data_obsolete_after_retired_mark() {
    let path = RandomPath::new();
    let status = spawn_child(
        "data_obsolete_reclaim",
        &path,
        "mace_gc_data_obsolete_after_retired_mark=abort@1",
    );
    assert_child_aborted(
        status,
        "data-obsolete-after-retired-mark child should abort",
    );
    assert_bucket_readable(&path);
}

#[test]
#[ignore]
fn chaos_failpoint_gc_data_obsolete_after_remove_stat() {
    let path = RandomPath::new();
    let status = spawn_child(
        "data_obsolete_reclaim",
        &path,
        "mace_gc_data_obsolete_after_remove_stat=abort@1",
    );
    assert_child_aborted(status, "data-obsolete-after-remove-stat child should abort");
    assert_bucket_readable(&path);
}

#[test]
#[ignore]
fn chaos_failpoint_gc_blob_obsolete_after_meta_commit() {
    let path = RandomPath::new();
    let status = spawn_child(
        "blob_obsolete_reclaim",
        &path,
        "mace_gc_blob_obsolete_after_meta_commit=abort@1",
    );
    assert_child_aborted(status, "blob-obsolete-after-meta child should abort");
    assert_bucket_readable(&path);
}

#[test]
#[ignore]
fn chaos_failpoint_gc_blob_obsolete_after_retired_mark() {
    let path = RandomPath::new();
    let status = spawn_child(
        "blob_obsolete_reclaim",
        &path,
        "mace_gc_blob_obsolete_after_retired_mark=abort@1",
    );
    assert_child_aborted(
        status,
        "blob-obsolete-after-retired-mark child should abort",
    );
    assert_bucket_readable(&path);
}

#[test]
#[ignore]
fn chaos_failpoint_gc_blob_obsolete_after_remove_stat() {
    let path = RandomPath::new();
    let status = spawn_child(
        "blob_obsolete_reclaim",
        &path,
        "mace_gc_blob_obsolete_after_remove_stat=abort@1",
    );
    assert_child_aborted(status, "blob-obsolete-after-remove-stat child should abort");
    assert_bucket_readable(&path);
}

#[test]
#[ignore]
fn chaos_failpoint_wal_after_checkpoint_write() {
    let path = RandomPath::new();
    let status = spawn_child(
        "wal_after_checkpoint_write",
        &path,
        "mace_wal_after_checkpoint_write=abort@1",
    );
    assert_child_aborted(status, "wal failpoint child should abort");
    assert_visibility_after_reopen(&path, 64, 24);
}

#[test]
#[ignore]
fn chaos_failpoint_manifest_before_multi_commit() {
    let path = RandomPath::new();
    let status = spawn_child(
        "manifest_before_multi_commit",
        &path,
        "mace_manifest_before_multi_commit=abort@3",
    );
    assert_child_aborted(status, "manifest failpoint child should abort");
    assert_visibility_after_reopen(&path, 64, 24);
}

#[test]
#[ignore]
fn chaos_failpoint_wal_recycle_before_intent_commit() {
    let path = RandomPath::new();
    let status = spawn_child(
        "wal_recycle_before_intent_commit",
        &path,
        "mace_wal_recycle_before_intent_commit=abort@1",
    );
    assert_child_aborted(
        status,
        "wal-recycle-before-intent-commit failpoint child should abort",
    );
    assert_eq!(
        wal_recycle_stage(&path, 0),
        None,
        "before-intent crash must not leave a durable recycle record",
    );
    assert_visibility_after_reopen(&path, 64, 24);
}

#[test]
#[ignore]
fn chaos_failpoint_wal_recycle_after_remove_before_dir_sync() {
    let path = RandomPath::new();
    let status = spawn_child(
        "wal_recycle_after_remove_before_dir_sync",
        &path,
        "mace_wal_recycle_after_remove_before_dir_sync=abort@1",
    );
    assert_child_aborted(
        status,
        "wal-recycle-after-remove-before-dir-sync failpoint child should abort",
    );
    assert_eq!(
        wal_recycle_stage(&path, 0),
        Some(1),
        "partial-unlink crash must leave durable recycle intent",
    );
    assert_visibility_after_reopen(&path, 64, 24);
}

#[test]
#[ignore]
fn chaos_failpoint_wal_recycle_after_dir_sync_before_done_commit() {
    let path = RandomPath::new();
    let status = spawn_child(
        "wal_recycle_done_windows",
        &path,
        "mace_wal_recycle_after_dir_sync_before_done_commit=abort@1",
    );
    assert_child_aborted(
        status,
        "wal-recycle-after-dir-sync-before-done-commit child should abort",
    );
    assert_eq!(
        wal_recycle_stage(&path, 0),
        Some(1),
        "dir-sync-before-done crash must still expose intent stage",
    );
    assert_visibility_after_reopen(&path, 64, 24);
}

#[test]
#[ignore]
fn chaos_failpoint_wal_recycle_after_done_commit_before_publish() {
    let path = RandomPath::new();
    let status = spawn_child(
        "wal_recycle_done_windows",
        &path,
        "mace_wal_recycle_after_done_commit_before_publish=abort@1",
    );
    assert_child_aborted(
        status,
        "wal-recycle-after-done-commit-before-publish child should abort",
    );
    assert_eq!(
        wal_recycle_stage(&path, 0),
        Some(2),
        "done-commit crash must preserve durable recycle frontier",
    );
    assert_visibility_after_reopen(&path, 64, 24);
}

fn seed_wal_recycle_intent_after_dir_sync(db_root: &Path) {
    let status = spawn_child(
        "wal_recycle_done_windows",
        db_root,
        "mace_wal_recycle_after_dir_sync_before_done_commit=abort@1",
    );
    assert_child_aborted(
        status,
        "wal-recycle-after-dir-sync-before-done-commit child should abort",
    );
    assert_eq!(
        wal_recycle_stage(db_root, 0),
        Some(1),
        "seed crash must leave durable recycle intent",
    );
}

fn seed_wal_recycle_intent_after_first_remove(db_root: &Path) {
    let status = spawn_child(
        "wal_recycle_after_remove_before_dir_sync",
        db_root,
        "mace_wal_recycle_after_remove_before_dir_sync=abort@2",
    );
    assert_child_aborted(
        status,
        "wal-recycle-after-remove-before-dir-sync child should abort after partial recycle",
    );
    assert_eq!(
        wal_recycle_stage(db_root, 0),
        Some(1),
        "seed crash must leave durable recycle intent after a partial remove",
    );
}

fn seed_wal_recycle_intent_after_first_rename(db_root: &Path) {
    let status = spawn_child(
        "wal_recycle_done_windows_keep_stable",
        db_root,
        "mace_wal_recycle_after_remove_before_dir_sync=abort@2",
    );
    assert_child_aborted(
        status,
        "wal-recycle-after-remove-before-dir-sync child should abort after partial rename",
    );
    assert_eq!(
        wal_recycle_stage(db_root, 0),
        Some(1),
        "seed crash must leave durable recycle intent after a partial rename",
    );
}

#[test]
#[ignore]
fn chaos_failpoint_recovery_wal_recycle_after_dir_sync_before_done_commit() {
    let path = RandomPath::new();
    seed_wal_recycle_intent_after_dir_sync(&path);

    let status = spawn_child(
        "recovery_wal_recycle_done_windows",
        &path,
        "mace_wal_recycle_after_dir_sync_before_done_commit=abort@1",
    );
    assert_child_aborted(
        status,
        "recovery wal-recycle-after-dir-sync-before-done-commit child should abort",
    );
    assert_eq!(
        wal_recycle_stage(&path, 0),
        Some(1),
        "recovery dir-sync-before-done crash must keep intent stage durable",
    );

    assert_visibility_after_reopen(&path, 64, 24);
    assert_eq!(
        wal_recycle_stage(&path, 0),
        Some(2),
        "clean reopen should finish pending recycle after recovery crash",
    );
}

#[test]
#[ignore]
fn chaos_failpoint_recovery_wal_recycle_after_done_commit_before_publish() {
    let path = RandomPath::new();
    seed_wal_recycle_intent_after_dir_sync(&path);

    let status = spawn_child(
        "recovery_wal_recycle_done_windows",
        &path,
        "mace_wal_recycle_after_done_commit_before_publish=abort@1",
    );
    assert_child_aborted(
        status,
        "recovery wal-recycle-after-done-commit-before-publish child should abort",
    );
    assert_eq!(
        wal_recycle_stage(&path, 0),
        Some(2),
        "recovery done-commit crash must persist durable done frontier",
    );

    assert_visibility_after_reopen(&path, 64, 24);
    assert_eq!(
        wal_recycle_stage(&path, 0),
        Some(2),
        "clean reopen should keep durable done frontier after recovery crash",
    );
}

#[test]
#[ignore]
fn chaos_failpoint_recovery_fs_remove_file_io() {
    let path = RandomPath::new();
    seed_wal_recycle_intent_after_first_remove(&path);

    let status = spawn_child(
        "recovery_wal_recycle_expect_remove_io",
        &path,
        "mace_fs_remove_file=io(permission_denied)@1",
    );
    assert!(
        status.success(),
        "recovery remove_file child should report io error"
    );
    assert_eq!(
        wal_recycle_stage(&path, 0),
        Some(1),
        "failed recovery remove must keep recycle intent durable",
    );
}

#[test]
#[ignore]
fn chaos_failpoint_recovery_fs_rename_io() {
    let path = RandomPath::new();
    seed_wal_recycle_intent_after_first_rename(&path);

    let status = spawn_child(
        "recovery_wal_recycle_expect_rename_io",
        &path,
        "mace_fs_rename=io(permission_denied)@1",
    );
    assert!(
        status.success(),
        "recovery rename child should report io error"
    );
    assert_eq!(
        wal_recycle_stage(&path, 0),
        Some(1),
        "failed recovery rename must keep recycle intent durable",
    );
}

#[test]
#[ignore]
fn chaos_failpoint_txn_commit_after_wal_file_sync_before_dir_sync() {
    let path = RandomPath::new();
    let status = spawn_child(
        "txn_commit_after_wal_file_sync_before_dir_sync",
        &path,
        "mace_wal_after_file_sync_before_dir_sync=abort@2",
    );
    assert_child_aborted(
        status,
        "txn-after-wal-file-sync-before-dir-sync failpoint child should abort",
    );
    assert_visibility_after_reopen(&path, 64, 24);
}

#[test]
#[ignore]
fn chaos_failpoint_txn_commit_after_record_commit() {
    let path = RandomPath::new();
    let status = spawn_child(
        "txn_commit_after_record_commit",
        &path,
        "mace_txn_commit_after_record_commit=abort@2",
    );
    assert_child_aborted(
        status,
        "txn-after-record-commit failpoint child should abort",
    );
    assert_visibility_after_reopen(&path, 64, 24);
}

#[test]
#[ignore]
fn chaos_failpoint_txn_commit_after_wal_sync() {
    let path = RandomPath::new();
    let status = spawn_child(
        "txn_commit_after_wal_sync",
        &path,
        "mace_txn_commit_after_wal_sync=abort@2",
    );
    assert_child_aborted(status, "txn-after-wal-sync failpoint child should abort");
    assert_visibility_after_reopen(&path, 64, 24);
}

#[test]
#[ignore]
fn chaos_failpoint_bucket_create_before_manifest_commit() {
    let path = RandomPath::new();
    let status = spawn_child(
        "bucket_create",
        &path,
        "mace_bucket_create_before_manifest_commit=abort@1",
    );
    assert_child_aborted(status, "bucket-create-before-manifest child should abort");
    assert!(!bucket_name_present(&path, "create_fp"));
    assert_bucket_missing_after_reopen(&path, "create_fp");
}

#[test]
#[ignore]
fn chaos_failpoint_bucket_create_after_manifest_commit() {
    let path = RandomPath::new();
    let status = spawn_child(
        "bucket_create",
        &path,
        "mace_bucket_create_after_manifest_commit=abort@1",
    );
    assert_child_aborted(status, "bucket-create-after-manifest child should abort");
    assert!(bucket_name_present(&path, "create_fp"));
    assert_bucket_exists_after_reopen(&path, "create_fp");
}

#[test]
#[ignore]
fn chaos_failpoint_bucket_delete_before_manifest_commit() {
    let path = RandomPath::new();
    let bucket_id = seed_bucket_delete_target(&path);
    let status = spawn_child(
        "bucket_delete",
        &path,
        "mace_bucket_delete_before_manifest_commit=abort@1",
    );
    assert_child_aborted(status, "bucket-delete-before-manifest child should abort");
    assert!(bucket_name_present(&path, "victim"));
    assert!(!pending_bucket_ids(&path).contains(&bucket_id));
    assert_bucket_exists_after_reopen(&path, "victim");
}

#[test]
#[ignore]
fn chaos_failpoint_bucket_delete_after_manifest_commit() {
    let path = RandomPath::new();
    let bucket_id = seed_bucket_delete_target(&path);
    let status = spawn_child(
        "bucket_delete",
        &path,
        "mace_bucket_delete_after_manifest_commit=abort@1",
    );
    assert_child_aborted(status, "bucket-delete-after-manifest child should abort");
    assert!(!bucket_name_present(&path, "victim"));
    assert!(pending_bucket_ids(&path).contains(&bucket_id));
    assert_bucket_missing_after_reopen(&path, "victim");
}

#[test]
#[ignore]
fn chaos_failpoint_pending_bucket_reap_after_batch_before_finalize() {
    let path = RandomPath::new();
    let bucket_id = seed_pending_bucket_reap_target(&path);
    let status = spawn_child(
        "pending_bucket_reap",
        &path,
        "mace_pending_bucket_reap_after_batch_before_finalize=abort@1",
    );
    assert_child_aborted(
        status,
        "pending-bucket-reap-after-batch-before-finalize child should abort",
    );
    assert_pending_bucket_survives_reopen(&path, bucket_id);
}

#[test]
#[ignore]
fn chaos_failpoint_pending_bucket_reap_after_finalize_before_meta_commit() {
    let path = RandomPath::new();
    let bucket_id = seed_pending_bucket_reap_target(&path);
    let status = spawn_child(
        "pending_bucket_reap",
        &path,
        "mace_pending_bucket_reap_after_finalize_before_meta_commit=abort@1",
    );
    assert_child_aborted(
        status,
        "pending-bucket-reap-after-finalize-before-meta child should abort",
    );
    let (has_page, has_data_ivl, has_blob_ivl) = aux_bucket_presence(&path, bucket_id);
    assert!(
        !has_page && !has_data_ivl && !has_blob_ivl,
        "aux buckets should already be finalized before pending meta commit"
    );
    assert!(pending_bucket_ids(&path).contains(&bucket_id));
    assert_pending_bucket_survives_reopen_without_aux(&path, bucket_id);
}

#[test]
#[ignore]
fn chaos_failpoint_pending_bucket_reap_after_meta_commit() {
    let path = RandomPath::new();
    let bucket_id = seed_pending_bucket_reap_target(&path);
    let status = spawn_child(
        "pending_bucket_reap",
        &path,
        "mace_pending_bucket_reap_after_manifest_commit=abort@1",
    );
    assert_child_aborted(
        status,
        "pending-bucket-reap-after-manifest child should abort",
    );
    assert!(!pending_bucket_ids(&path).contains(&bucket_id));
    assert_pending_bucket_reaped_after_reopen(&path, bucket_id);
}

#[test]
#[ignore]
fn chaos_failpoint_gc_data_rewrite_before_meta_commit() {
    let path = RandomPath::new();
    let status = spawn_child(
        "gc_data_rewrite_before_meta_commit",
        &path,
        "mace_gc_data_rewrite_before_meta_commit=abort@1",
    );
    assert_child_aborted(status, "gc-data failpoint child should abort");
    assert_bucket_readable(&path);
}

#[test]
#[ignore]
fn chaos_failpoint_gc_data_rewrite_after_stage_marker() {
    let path = RandomPath::new();
    let status = spawn_child(
        "gc_data_rewrite_after_stage_marker",
        &path,
        "mace_gc_data_rewrite_after_stage_marker=abort@1",
    );
    assert_child_aborted(status, "gc-data-after-marker failpoint child should abort");
    assert_bucket_readable(&path);
}

#[test]
#[ignore]
fn chaos_failpoint_gc_data_rewrite_after_data_dir_sync() {
    let path = RandomPath::new();
    let status = spawn_child(
        "gc_data_rewrite_after_data_dir_sync",
        &path,
        "mace_gc_data_rewrite_after_data_dir_sync=abort@1",
    );
    assert_child_aborted(
        status,
        "gc-data-after-data-dir-sync failpoint child should abort",
    );
    assert_bucket_readable(&path);
}

#[test]
#[ignore]
fn chaos_failpoint_gc_data_rewrite_after_meta_commit() {
    let path = RandomPath::new();
    let status = spawn_child(
        "gc_data_rewrite_after_meta_commit",
        &path,
        "mace_gc_data_rewrite_after_meta_commit=abort@1",
    );
    assert_child_aborted(status, "gc-data-after-meta failpoint child should abort");
    assert_bucket_readable(&path);
}

#[test]
#[ignore]
fn chaos_failpoint_gc_blob_rewrite_before_meta_commit() {
    let path = RandomPath::new();
    let status = spawn_child(
        "gc_blob_rewrite_before_meta_commit",
        &path,
        "mace_gc_blob_rewrite_before_meta_commit=abort@1",
    );
    assert_child_aborted(status, "gc-blob failpoint child should abort");
    assert_bucket_readable(&path);
}

#[test]
#[ignore]
fn chaos_failpoint_gc_blob_rewrite_after_stage_marker() {
    let path = RandomPath::new();
    let status = spawn_child(
        "gc_blob_rewrite_after_stage_marker",
        &path,
        "mace_gc_blob_rewrite_after_stage_marker=abort@1",
    );
    assert_child_aborted(status, "gc-blob-after-marker failpoint child should abort");
    assert_bucket_readable(&path);
}

#[test]
#[ignore]
fn chaos_failpoint_gc_blob_rewrite_after_data_dir_sync() {
    let path = RandomPath::new();
    let status = spawn_child(
        "gc_blob_rewrite_after_data_dir_sync",
        &path,
        "mace_gc_blob_rewrite_after_data_dir_sync=abort@1",
    );
    assert_child_aborted(
        status,
        "gc-blob-after-data-dir-sync failpoint child should abort",
    );
    assert_bucket_readable(&path);
}

#[test]
#[ignore]
fn chaos_failpoint_gc_blob_rewrite_after_meta_commit() {
    let path = RandomPath::new();
    let status = spawn_child(
        "gc_blob_rewrite_after_meta_commit",
        &path,
        "mace_gc_blob_rewrite_after_meta_commit=abort@1",
    );
    assert_child_aborted(status, "gc-blob-after-meta failpoint child should abort");
    assert_bucket_readable(&path);
}

#[test]
#[ignore]
fn chaos_failpoint_delete_files_after_dir_sync_before_meta_commit() {
    let path = RandomPath::new();
    let status = spawn_child(
        "delete_files_after_dir_sync_before_meta_commit",
        &path,
        "mace_delete_files_after_dir_sync_before_meta_commit=abort@1",
    );
    assert_child_aborted(
        status,
        "delete-files-after-dir-sync-before-meta-commit failpoint child should abort",
    );
    assert_bucket_readable(&path);
}

#[test]
#[ignore]
fn chaos_failpoint_recovery_orphan_cleanup_after_data_dir_sync_before_marker_clear() {
    let path = RandomPath::new();
    let status = spawn_child(
        "flush_before_manifest_commit",
        &path,
        "mace_flush_before_manifest_commit=abort@1",
    );
    assert_child_aborted(status, "flush-before-manifest failpoint child should abort");
    let crashed_files = data_blob_files(&path);
    assert!(
        !crashed_files.is_empty(),
        "expected flush crash to leave orphan files before recovery"
    );

    let status = spawn_child(
        "recovery_orphan_cleanup_after_data_dir_sync_before_marker_clear",
        &path,
        "mace_recovery_orphan_cleanup_after_data_dir_sync_before_marker_clear=abort@1",
    );
    assert_child_aborted(
        status,
        "recovery-orphan-cleanup-after-data-dir-sync-before-marker-clear child should abort",
    );

    assert_visibility_after_reopen(&path, 64, 24);
    for file in crashed_files {
        assert!(
            !file.exists(),
            "orphan file should be cleaned after recovery retry: {file:?}"
        );
    }
}

#[test]
#[ignore]
fn chaos_failpoint_recovery_abort_clean_after_drain_before_start() {
    let path = RandomPath::new();
    let status = spawn_child("recovery_abort_clean_seed", &path, "");
    assert!(status.success(), "seed child should finish normally");

    let status = spawn_child(
        "recovery_abort_clean_after_drain_before_start",
        &path,
        "mace_recovery_abort_clean_after_drain_before_start=abort@1",
    );
    assert_child_aborted(
        status,
        "recovery-abort-clean-after-drain-before-start child should abort",
    );

    let mace = open_with_tune(&path, |opt| {
        opt.tmp_store = false;
        opt.sync_on_write = true;
        opt.concurrent_write = 1;
        opt.gc_timeout = 60_000;
        opt.data_file_size = 16 << 10;
        opt.wal_buffer_size = 8 << 10;
        opt.wal_file_size = 4 << 10;
    });
    let bucket = mace.get_bucket("prod").expect("bucket prod should exist");
    let view = bucket.view().expect("open verify view failed");
    let val = view.get("seed").expect("seed key missing after reopen");
    assert_eq!(val.slice(), b"base");
}

#[test]
#[ignore]
fn chaos_failpoint_recovery_abort_clean_does_not_recycle_wal_before_runtime_checkpoint() {
    let path = RandomPath::new();
    let status = spawn_child("recovery_abort_clean_seed", &path, "");
    assert!(status.success(), "seed child should finish normally");

    let before = wal_files(&path, 0);
    assert!(
        !before.is_empty(),
        "expected recovery-abort-clean seed to create wal files"
    );

    let status = spawn_child(
        "recovery_abort_clean_after_drain_before_start",
        &path,
        "mace_recovery_abort_clean_after_drain_before_start=abort@1",
    );
    assert_child_aborted(
        status,
        "recovery-abort-clean-after-drain-before-start child should abort",
    );

    let status = spawn_child(
        "recovery_abort_clean_post_start_gc",
        &path,
        "mace_wal_recycle_after_remove_before_dir_sync=abort@1",
    );
    assert!(
        status.success(),
        "post-start gc should not recycle wal before a runtime checkpoint exists: {status:?}"
    );

    let after = wal_files(&path, 0);
    assert_eq!(
        before, after,
        "wal inventory changed even though post-start gc should not have recycled any file"
    );
}

#[test]
fn recovery_rejects_sparse_wal_gap_after_checkpoint() {
    let path = RandomPath::new();
    {
        let mace = open_with_tune(&path, |opt| {
            opt.concurrent_write = 1;
            opt.sync_on_write = true;
            opt.data_file_size = 64 << 20;
            opt.wal_buffer_size = 8 << 10;
            opt.wal_file_size = 4 << 10;
        });
        let bucket = mace
            .new_bucket(
                "prod",
                BucketOptions {
                    inline_size: 512,
                    checkpoint_size: 64 << 20,
                    pool_capacity: 128 << 20,
                    ..BucketOptions::default()
                },
            )
            .expect("create prod bucket for sparse wal test failed");

        let txn = bucket.begin().expect("begin seed txn failed");
        for idx in 0..32 {
            txn.upsert(format!("k_{idx}"), format!("seed_{idx}"))
                .expect("seed put failed");
        }
        txn.commit().expect("seed commit failed");

        // create a durable checkpoint first
        bucket.checkpoint();

        // append many wal records after checkpoint without dirtying data pages
        // this keeps checkpoint position stable while still spanning multiple wal files
        for _ in 0..5000 {
            let txn = bucket
                .begin()
                .expect("begin post-checkpoint wal-only txn failed");
            txn.commit()
                .expect("commit post-checkpoint wal-only txn failed");
        }
    }

    let mut files = wal_files(&path, 0);
    assert!(
        files.len() >= 3,
        "need at least 3 wal files to form a sparse sequence, got {}",
        files.len()
    );
    let hole = files.remove(files.len() - 2);
    std::fs::remove_file(&hole).expect("remove middle wal file failed");

    let mut opt = Options::new(&*path);
    opt.tmp_store = false;
    opt.concurrent_write = 1;
    opt.sync_on_write = true;
    opt.data_file_size = 16 << 10;
    opt.wal_buffer_size = 8 << 10;
    opt.wal_file_size = 4 << 10;
    match Mace::new(opt.validate().expect("validate options failed")) {
        Err(err) => assert_eq!(err, OpCode::Corruption),
        Ok(_) => panic!("recovery should reject sparse wal gap after checkpoint"),
    }
}

#[test]
#[ignore]
fn wal_recycle_done_reopen_is_idempotent() {
    let path = RandomPath::new();
    let status = spawn_child(
        "wal_recycle_done_windows",
        &path,
        "mace_wal_recycle_after_done_commit_before_publish=abort@1",
    );
    assert_child_aborted(
        status,
        "wal-recycle-after-done-commit-before-publish child should abort",
    );
    assert_eq!(
        wal_recycle_stage(&path, 0),
        Some(2),
        "first crash should preserve durable done frontier",
    );

    assert_visibility_after_reopen(&path, 64, 24);
    assert_eq!(
        wal_recycle_stage(&path, 0),
        Some(2),
        "reopen must keep durable recycle frontier for later boots",
    );
    assert_visibility_after_reopen(&path, 64, 24);
}

#[test]
#[ignore]
fn wal_recycle_done_does_not_weaken_gap_detection_after_frontier() {
    let path = RandomPath::new();
    let status = spawn_child(
        "wal_recycle_done_windows",
        &path,
        "mace_wal_recycle_after_done_commit_before_publish=abort@1",
    );
    assert_child_aborted(
        status,
        "wal-recycle-after-done-commit-before-publish child should abort",
    );
    assert_visibility_after_reopen(&path, 64, 24);

    let mut files = wal_files(&path, 0);
    assert!(
        !files.is_empty(),
        "expected at least one wal file after durable recycle frontier"
    );
    let hole = files.remove(0);
    std::fs::remove_file(&hole).expect("remove post-frontier wal file failed");

    let mut opt = Options::new(&*path);
    opt.tmp_store = false;
    opt.concurrent_write = 1;
    opt.sync_on_write = true;
    opt.data_file_size = 16 << 10;
    opt.wal_buffer_size = 8 << 10;
    opt.wal_file_size = 4 << 10;
    match Mace::new(opt.validate().expect("validate options failed")) {
        Err(err) => assert_eq!(err, OpCode::Corruption),
        Ok(_) => panic!("recovery should reject wal gap beyond durable recycle frontier"),
    }
}

#[test]
#[ignore]
fn chaos_failpoint_evictor_before_evict_once() {
    let path = RandomPath::new();
    let status = spawn_child(
        "evictor_before_evict_once",
        &path,
        "mace_evictor_before_evict_once=abort@1",
    );
    assert_child_aborted(status, "evictor failpoint child should abort");
    assert_bucket_readable(&path);
}
