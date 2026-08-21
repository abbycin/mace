#![cfg(feature = "extra_check")]

//! same-route physical layout migration: an older durable format wrote the
//! wal to per-group `wal_<group>_<seq>` files; the shared-stream format writes
//! `group_wal_<seq>`. reopening an old-format durable database with durable
//! options (same route) must rebuild the epoch: replay the legacy wal, force
//! a full-fsync checkpoint, wipe every old physical stream, and continue on
//! the shared namespace at H+1. the migration must be idempotent across crash
//! windows and must never leave legacy files behind.

mod common;

use common::child_test_command;
use mace::testing;
use mace::{BucketOptions, Mace, OpCode, Options, RandomPath};
use std::io::Write;
use std::path::{Path, PathBuf};

const ENV_CRASH_CHILD: &str = "MACE_MIGRATION_CRASH_CHILD";
const ENV_CRASH_DB: &str = "MACE_MIGRATION_CRASH_DB";
const ENV_LEGACY_CRASH_CHILD: &str = "MACE_LEGACY_MIGRATION_CRASH_CHILD";

#[cfg(unix)]
fn assert_child_aborted(status: std::process::ExitStatus, msg: &str) {
    use std::os::unix::process::ExitStatusExt;

    assert_eq!(status.signal(), Some(6), "{msg}");
}

#[cfg(not(unix))]
fn assert_child_aborted(status: std::process::ExitStatus, msg: &str) {
    assert!(!status.success(), "{msg}");
}

fn group_wal_file_ids(log_root: &Path) -> Vec<u64> {
    let mut ids = Vec::new();
    if let Ok(entries) = std::fs::read_dir(log_root) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if let Some(rest) = name.strip_prefix("group_wal_")
                && let Ok(file_id) = rest.parse::<u64>()
            {
                ids.push(file_id);
            }
        }
    }
    ids.sort_unstable();
    ids
}

fn legacy_wal_file_ids(log_root: &Path, group: u8) -> Vec<u64> {
    let prefix = format!("wal_{group}_");
    let mut ids = Vec::new();
    if let Ok(entries) = std::fs::read_dir(log_root) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if let Some(rest) = name.strip_prefix(&prefix)
                && let Ok(file_id) = rest.parse::<u64>()
            {
                ids.push(file_id);
            }
        }
    }
    ids.sort_unstable();
    ids
}

fn rename_stream(log_root: &Path, from_prefix: &str, to_prefix: &str) -> usize {
    let mut renamed = 0;
    for entry in std::fs::read_dir(log_root).expect("read log root") {
        let entry = entry.expect("entry");
        let name = entry.file_name().to_string_lossy().into_owned();
        if let Some(rest) = name.strip_prefix(from_prefix) {
            let dest = log_root.join(format!("{to_prefix}{rest}"));
            std::fs::rename(entry.path(), dest).expect("rename wal file");
            renamed += 1;
        }
    }
    renamed
}

fn durable_opt(path: &Path) -> Options {
    let mut opt = Options::new(path);
    opt.sync_on_write = true;
    opt.concurrent_write = 1;
    opt.wal_file_size = 4096;
    opt.gc_timeout = 60_000;
    opt.checkpoint_nudge_ms = 0;
    opt.data_file_size = 100 << 10;
    opt
}

fn legacy_opt(path: &Path) -> Options {
    let mut opt = durable_opt(path);
    opt.sync_on_write = false;
    opt.concurrent_write = 2;
    opt.data_file_size = 1 << 30;
    opt
}

fn assert_legacy_seeded(mace: &Mace, count: usize) {
    let db = mace.get_bucket("legacy").expect("legacy bucket");
    let view = db.view().expect("legacy view");
    for group in ["a", "b"] {
        for i in 0..count {
            let key = format!("{group}_{i}");
            assert_eq!(
                view.get(&key)
                    .unwrap_or_else(|_| panic!("missing {key}"))
                    .slice(),
                format!("v_{i}").as_bytes()
            );
        }
    }
}

fn spawn_legacy_crash_child(db_root: &Path) -> std::process::ExitStatus {
    let exe = std::env::current_exe().expect("load current exe failed");
    child_test_command(&exe)
        .arg("--exact")
        .arg("crash_child_true_legacy_layout")
        .arg("--nocapture")
        .env(ENV_LEGACY_CRASH_CHILD, "1")
        .env(ENV_CRASH_DB, db_root.as_os_str())
        .status()
        .expect("spawn true legacy crash child failed")
}

#[test]
fn crash_child_true_legacy_layout() {
    if std::env::var(ENV_LEGACY_CRASH_CHILD).ok().as_deref() != Some("1") {
        return;
    }
    let db_root = PathBuf::from(std::env::var(ENV_CRASH_DB).expect("missing crash db root"));
    let opt = legacy_opt(&db_root);
    let mace = Mace::new(opt.validate().expect("legacy child options")).expect("legacy child open");
    let db = mace
        .new_bucket("legacy", BucketOptions::default())
        .expect("legacy bucket");
    let tx_a = db.begin().expect("group 0 begin");
    let tx_b = db.begin().expect("group 1 begin");
    assert_eq!(
        testing::txn_group(&db, testing::txn_start_ts(&tx_a)),
        Some(0)
    );
    assert_eq!(
        testing::txn_group(&db, testing::txn_start_ts(&tx_b)),
        Some(1)
    );
    for i in 0..300 {
        tx_a.put(format!("a_{i}"), format!("v_{i}"))
            .expect("group 0 put");
        tx_b.put(format!("b_{i}"), format!("v_{i}"))
            .expect("group 1 put");
    }
    tx_a.commit().expect("group 0 commit");
    tx_b.commit().expect("group 1 commit");
    mace.sync().expect("legacy wal sync");
    std::process::abort();
}

#[test]
fn true_multi_group_legacy_wal_is_recovered_and_migrated() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let status = spawn_legacy_crash_child(&path);
    assert_child_aborted(status, "legacy child must abort");
    let legacy = legacy_opt(&path);
    let log_root = legacy.log_root();
    let ids0 = legacy_wal_file_ids(&log_root, 0);
    let ids1 = legacy_wal_file_ids(&log_root, 1);
    assert!(
        !ids0.is_empty() && !ids1.is_empty(),
        "fixture must have two real legacy streams"
    );
    let old_high = ids0.iter().chain(&ids1).copied().max().unwrap();
    testing::rewrite_persisted_sync_on_write(legacy.clone(), true)?;

    let mut durable = legacy;
    durable.sync_on_write = true;
    let mace = Mace::new(durable.validate()?)?;
    assert_legacy_seeded(&mace, 300);
    assert!(legacy_wal_file_ids(&log_root, 0).is_empty());
    assert!(legacy_wal_file_ids(&log_root, 1).is_empty());
    let new_ids = group_wal_file_ids(&log_root);
    assert!(!new_ids.is_empty());
    let new_start = new_ids.iter().copied().min().unwrap();
    assert!(
        new_start > old_high,
        "new shared epoch must start above every legacy stream: old={old_high}, new={new_ids:?}"
    );
    mace.start_gc();
    for physical_group in [0, 1, Options::SHARED_ID] {
        assert_eq!(
            testing::wal_recycle_boundary(&mace.get_bucket("legacy")?, physical_group),
            new_start,
            "an empty runtime GC pass must not regress layout migration's recycle boundary"
        );
    }
    Ok(())
}

#[test]
fn malformed_true_legacy_wal_obeys_truncation_option_during_migration() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let status = spawn_legacy_crash_child(&path);
    assert_child_aborted(status, "legacy child must abort");
    let legacy = legacy_opt(&path);
    let log_root = legacy.log_root();
    let corrupt_id = *legacy_wal_file_ids(&log_root, 1)
        .last()
        .expect("group 1 legacy stream");
    let corrupt_path = legacy.wal_file(1, corrupt_id);
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&corrupt_path)
        .expect("open legacy tail");
    file.write_all(&[0xff; 16]).expect("append malformed tail");
    file.sync_all().expect("sync malformed tail");
    let corrupt_len = file.metadata().expect("legacy metadata").len();
    drop(file);
    testing::rewrite_persisted_sync_on_write(legacy.clone(), true)?;

    let mut strict = legacy.clone();
    strict.sync_on_write = true;
    strict.truncate_corrupted_wal = false;
    assert_eq!(
        Mace::new(strict.validate()?).err(),
        Some(OpCode::Corruption)
    );
    assert_eq!(std::fs::metadata(&corrupt_path).unwrap().len(), corrupt_len);

    let mut truncate = legacy;
    truncate.sync_on_write = true;
    truncate.truncate_corrupted_wal = true;
    let mace = Mace::new(truncate.validate()?)?;
    assert_legacy_seeded(&mace, 300);
    assert!(legacy_wal_file_ids(&log_root, 0).is_empty());
    assert!(legacy_wal_file_ids(&log_root, 1).is_empty());
    Ok(())
}

fn seed(mace: &Mace, count: usize) {
    let db = mace
        .new_bucket("main", BucketOptions::default())
        .expect("new bucket");
    for i in 0..count {
        let tx = db.begin().expect("begin");
        tx.put(format!("k_{i}"), format!("v_{i}")).expect("put");
        tx.commit().expect("commit");
    }
}

fn assert_seeded(mace: &Mace, count: usize) {
    let db = mace.get_bucket("main").expect("bucket");
    let view = db.view().expect("view");
    for i in 0..count {
        let val = view
            .get(format!("k_{i}"))
            .unwrap_or_else(|_| panic!("key k_{i} missing after migration"));
        assert_eq!(val.slice(), format!("v_{i}").as_bytes());
    }
}

#[test]
fn legacy_durable_per_group_wal_is_migrated_on_same_route_reopen() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let opt = durable_opt(&path);
    let log_root = opt.log_root();
    {
        let mace = Mace::new(opt.clone().validate()?)?;
        seed(&mace, 200);
        // graceful exit: data lives in the data files, wal is all checkpointed
    }

    // simulate the old durable layout: every shared stream file becomes a
    // per-group wal_0_<seq> file
    let renamed = rename_stream(&log_root, "group_wal_", "wal_0_");
    assert!(
        renamed > 0,
        "fixture must produce shared wal files to rename"
    );
    assert!(legacy_wal_file_ids(&log_root, 0).len() == renamed);
    assert!(group_wal_file_ids(&log_root).is_empty());

    // same-route durable reopen must migrate: replay, checkpoint, wipe legacy
    let mace = Mace::new(opt.clone().validate()?)?;
    assert_seeded(&mace, 200);
    assert!(
        legacy_wal_file_ids(&log_root, 0).is_empty(),
        "legacy per-group wal files must be wiped by the migration"
    );
    let new_ids = group_wal_file_ids(&log_root);
    assert!(
        !new_ids.is_empty(),
        "the migrated database must continue on the shared namespace"
    );

    // the new era keeps working: a second reopen is a plain same-route reopen
    drop(mace);
    let mace = Mace::new(opt.clone().validate()?)?;
    let db = mace.get_bucket("main")?;
    let tx = db.begin()?;
    tx.put("post", b"v")?;
    tx.commit()?;
    db.checkpoint();
    let view = db.view()?;
    assert_eq!(view.get("post").expect("post").slice(), b"v");
    Ok(())
}

#[test]
fn crash_child_migration() {
    if std::env::var(ENV_CRASH_CHILD).ok().as_deref() != Some("1") {
        return;
    }
    let db_root = PathBuf::from(std::env::var(ENV_CRASH_DB).expect("missing crash db root"));
    let opt = durable_opt(&db_root);
    let mace = Mace::new(
        opt.clone()
            .validate()
            .expect("crash child options must validate"),
    )
    .expect("crash child open must succeed");
    let db = mace
        .new_bucket("main", BucketOptions::default())
        .expect("crash child bucket must succeed");
    for i in 0..201 {
        let tx = db.begin().expect("crash child begin");
        tx.put(format!("k_{i}"), format!("v_{i}"))
            .expect("crash child put");
        tx.commit().expect("crash child commit");
    }
    let tx = db.begin().expect("crash child tail begin");
    tx.put("tail", b"v").expect("crash child tail put");
    tx.commit().expect("crash child tail commit");
    // crash before any checkpoint: the tail is only in the wal
    std::process::abort();
}

#[test]
fn legacy_crash_wal_is_recovered_then_migrated_on_same_route_reopen() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let status = spawn_crash_child(&path);
    assert_child_aborted(status, "crash child must abort before checkpointing");

    let opt = durable_opt(&path);
    let log_root = opt.log_root();
    let renamed = rename_stream(&log_root, "group_wal_", "wal_0_");
    assert!(
        renamed > 0,
        "fixture must produce shared wal files to rename"
    );

    // the crash tail lives only in the legacy wal; the migration must redo it,
    // force-checkpoint it, wipe the legacy files and continue on group_wal
    let mace = Mace::new(opt.clone().validate()?)?;
    assert_seeded(&mace, 201);
    let db = mace.get_bucket("main")?;
    let view = db.view()?;
    assert_eq!(view.get("tail").expect("tail").slice(), b"v");
    drop(view);
    assert!(
        legacy_wal_file_ids(&log_root, 0).is_empty(),
        "legacy per-group wal files must be wiped after crash recovery migration"
    );
    let new_ids = group_wal_file_ids(&log_root);
    assert!(
        !new_ids.is_empty(),
        "the migrated database must continue on the shared namespace"
    );

    // the tail must be readable even with the wal gone: another same-route
    // reopen without any legacy files is a plain reopen, and gc recycles the
    // shared stream past the checkpoint boundary
    drop(mace);
    let mace = Mace::new(opt.clone().validate()?)?;
    assert_seeded(&mace, 201);
    let db = mace.get_bucket("main")?;
    let view = db.view()?;
    assert_eq!(view.get("tail").expect("tail after reopen").slice(), b"v");
    Ok(())
}

#[test]
fn mixed_namespace_is_fully_migrated() -> Result<(), OpCode> {
    // a partial migration (some files in the legacy layout, some still in the
    // shared namespace) must not enter steady state: both namespaces are
    // analyzed, both are wiped, and the new era starts above both
    let path = RandomPath::tmp();
    let status = spawn_crash_child(&path);
    assert_child_aborted(status, "crash child must abort before checkpointing");

    let opt = durable_opt(&path);
    let log_root = opt.log_root();
    let shared_ids = group_wal_file_ids(&log_root);
    assert!(
        shared_ids.len() >= 2,
        "fixture must span several wal files: {shared_ids:?}"
    );
    // rename the oldest half to the legacy layout, keep the rest shared
    let to_rename: Vec<u64> = shared_ids
        .iter()
        .copied()
        .take(shared_ids.len() / 2)
        .collect();
    for seq in to_rename {
        let from = log_root.join(format!("group_wal_{seq}"));
        let to = log_root.join(format!("wal_0_{seq}"));
        std::fs::rename(&from, &to).expect("rename to legacy layout");
    }
    assert!(!legacy_wal_file_ids(&log_root, 0).is_empty());
    assert!(!group_wal_file_ids(&log_root).is_empty());

    let mace = Mace::new(opt.clone().validate()?)?;
    assert_seeded(&mace, 201);
    let db = mace.get_bucket("main")?;
    let view = db.view()?;
    assert_eq!(view.get("tail").expect("tail").slice(), b"v");
    drop(view);
    assert!(
        legacy_wal_file_ids(&log_root, 0).is_empty(),
        "legacy files must be wiped in a mixed-namespace migration"
    );
    // after the migration exactly one namespace is live: the shared one
    assert!(
        !group_wal_file_ids(&log_root).is_empty(),
        "the mixed migration must reach a single active namespace"
    );
    Ok(())
}

fn spawn_crash_child(db_root: &Path) -> std::process::ExitStatus {
    let exe = std::env::current_exe().expect("load current exe failed");
    child_test_command(&exe)
        .arg("--exact")
        .arg("crash_child_migration")
        .arg("--nocapture")
        .env(ENV_CRASH_CHILD, "1")
        .env(ENV_CRASH_DB, db_root.as_os_str())
        .status()
        .expect("spawn migration crash child failed")
}
