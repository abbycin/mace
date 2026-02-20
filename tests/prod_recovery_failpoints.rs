#![cfg(feature = "failpoints")]

mod common;

use common::child_test_command;
use mace::{Bucket, Mace, OpCode, Options, RandomPath};
use std::path::{Path, PathBuf};
use std::process::ExitStatus;
use std::time::{Duration, Instant};

const ENV_CHILD: &str = "MACE_PROD_FP_CHILD";
const ENV_CASE: &str = "MACE_PROD_FP_CASE";
const ENV_DB_ROOT: &str = "MACE_PROD_FP_DB_ROOT";

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

fn open_with_tune<F>(db_root: &Path, tune: F) -> Mace
where
    F: FnOnce(&mut Options),
{
    let mut opt = Options::new(db_root);
    opt.tmp_store = false;
    tune(&mut opt);
    Mace::new(opt.validate().expect("validate options failed")).expect("open mace failed")
}

fn child_setup_common(db_root: &Path) -> (Mace, Bucket) {
    let mace = open_with_tune(db_root, |opt| {
        opt.sync_on_write = true;
        opt.data_file_size = 16 << 10;
        opt.max_log_size = 32 << 10;
        opt.wal_file_size = 8 << 10;
        opt.gc_timeout = 20;
        opt.gc_eager = true;
        opt.data_garbage_ratio = 1;
        opt.gc_compacted_size = opt.data_file_size;
        opt.inline_size = 512;
    });

    let bucket = match mace.get_bucket("prod") {
        Ok(bucket) => bucket,
        Err(OpCode::NotFound) => mace.new_bucket("prod").expect("create prod bucket failed"),
        Err(err) => panic!("open prod bucket failed: {err:?}"),
    };

    (mace, bucket)
}

fn child_setup_gc(db_root: &Path) -> (Mace, Bucket) {
    let mace = open_with_tune(db_root, |opt| {
        opt.concurrent_write = 1;
        opt.sync_on_write = true;
        opt.data_file_size = 16 << 10;
        opt.max_log_size = 1 << 20;
        opt.wal_file_size = 1 << 20;
        opt.gc_timeout = 20;
        opt.gc_eager = true;
        opt.data_garbage_ratio = 1;
        opt.gc_compacted_size = opt.data_file_size;
        opt.inline_size = 256;
        opt.blob_garbage_ratio = 1;
        opt.blob_gc_ratio = 100;
        opt.blob_max_size = 128 << 10;
    });

    let bucket = match mace.get_bucket("prod") {
        Ok(bucket) => bucket,
        Err(OpCode::NotFound) => mace.new_bucket("prod").expect("create prod bucket failed"),
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
    for round in 0..rounds {
        let txn = bucket.begin().expect("begin gc txn failed");
        for idx in 0..128 {
            txn.upsert(format!("k_{idx}"), format!("g_{round}_{idx}"))
                .expect("upsert gc key failed");
        }
        txn.commit().expect("commit gc txn failed");
    }
}

fn drive_blob_gc_pressure(bucket: &Bucket, rounds: usize, blob_size: usize) {
    let payload = vec![b'b'; blob_size];
    for round in 0..rounds {
        let txn = bucket.begin().expect("begin blob txn failed");
        for idx in 0..96 {
            txn.upsert(format!("blob_{idx}"), &payload)
                .expect("upsert blob key failed");
            txn.upsert(format!("meta_{idx}"), format!("m_{round}_{idx}"))
                .expect("upsert meta key failed");
        }
        txn.commit().expect("commit blob txn failed");
    }
}

fn assert_visibility_after_reopen(db_root: &Path, committed: usize, uncommitted: usize) {
    let mace = open_with_tune(db_root, |_| {});
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
    let mace = open_with_tune(db_root, |_| {});
    let bucket = mace.get_bucket("prod").expect("bucket prod should exist");
    let view = bucket.view().expect("open post-crash view failed");

    for idx in 0..16 {
        let key = format!("k_{idx}");
        let _ = view.get(&key);
    }
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
    let (mace, bucket) = child_setup_gc(db_root);
    seed_committed_and_uncommitted(&bucket, 64, 0);
    drive_gc_pressure(&bucket, 64);
    mace.sync().expect("sync before data gc failed");

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
    drive_blob_gc_pressure(&bucket, 96, 16 << 10);
    mace.sync().expect("sync before blob gc failed");

    let deadline = Instant::now() + Duration::from_secs(40);
    while Instant::now() < deadline {
        mace.start_gc();
        std::thread::sleep(Duration::from_millis(20));
    }

    panic!("gc blob failpoint did not fire")
}

fn child_case_evictor_before_evict_once(db_root: &Path) -> ! {
    let mace = open_with_tune(db_root, |opt| {
        opt.sync_on_write = false;
        opt.cache_capacity = Options::MIN_CACHE_CAP;
        opt.cache_evict_pct = 100;
        opt.data_file_size = 16 << 10;
        opt.max_log_size = 32 << 10;
        opt.wal_file_size = 8 << 10;
    });
    let bucket = mace.new_bucket("prod").expect("create prod bucket failed");

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

#[test]
fn failpoint_child() {
    if std::env::var(ENV_CHILD).ok().as_deref() != Some("1") {
        return;
    }

    let case = std::env::var(ENV_CASE).expect("missing failpoint case");
    let db_root = PathBuf::from(std::env::var(ENV_DB_ROOT).expect("missing failpoint db root"));

    match case.as_str() {
        "flush_after_data_sync" => child_case_flush_after_data_sync(&db_root),
        "flush_before_manifest_commit" => child_case_manifest_before_multi_commit(&db_root),
        "flush_after_manifest_commit" => child_case_manifest_before_multi_commit(&db_root),
        "wal_after_checkpoint_write" => child_case_wal_after_checkpoint_write(&db_root),
        "manifest_before_multi_commit" => child_case_manifest_before_multi_commit(&db_root),
        "gc_data_rewrite_before_meta_commit" => child_case_gc_data_before_meta_commit(&db_root),
        "gc_data_rewrite_after_stage_marker" => child_case_gc_data_before_meta_commit(&db_root),
        "gc_data_rewrite_after_meta_commit" => child_case_gc_data_before_meta_commit(&db_root),
        "gc_blob_rewrite_before_meta_commit" => child_case_gc_blob_before_meta_commit(&db_root),
        "gc_blob_rewrite_after_stage_marker" => child_case_gc_blob_before_meta_commit(&db_root),
        "gc_blob_rewrite_after_meta_commit" => child_case_gc_blob_before_meta_commit(&db_root),
        "txn_commit_after_record_commit" => child_case_txn_commit_abort_window(&db_root),
        "txn_commit_after_wal_sync" => child_case_txn_commit_abort_window(&db_root),
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
    assert!(!status.success(), "flush failpoint child should abort");
    assert_visibility_after_reopen(&path, 64, 24);
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
    assert!(
        !status.success(),
        "flush-before-manifest failpoint child should abort"
    );
    assert_visibility_after_reopen(&path, 64, 24);
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
    assert!(
        !status.success(),
        "flush-after-manifest failpoint child should abort"
    );
    assert_visibility_after_reopen(&path, 64, 24);
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
    assert!(!status.success(), "wal failpoint child should abort");
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
    assert!(!status.success(), "manifest failpoint child should abort");
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
    assert!(
        !status.success(),
        "txn-after-record-commit failpoint child should abort"
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
    assert!(
        !status.success(),
        "txn-after-wal-sync failpoint child should abort"
    );
    assert_visibility_after_reopen(&path, 64, 24);
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
    assert!(!status.success(), "gc-data failpoint child should abort");
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
    assert!(
        !status.success(),
        "gc-data-after-marker failpoint child should abort"
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
    assert!(
        !status.success(),
        "gc-data-after-meta failpoint child should abort"
    );
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
    assert!(!status.success(), "gc-blob failpoint child should abort");
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
    assert!(
        !status.success(),
        "gc-blob-after-marker failpoint child should abort"
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
    assert!(
        !status.success(),
        "gc-blob-after-meta failpoint child should abort"
    );
    assert_bucket_readable(&path);
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
    assert!(!status.success(), "evictor failpoint child should abort");
    assert_bucket_readable(&path);
}
