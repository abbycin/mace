//! Durable checkpoint acceptance tests.
//!
//! Deletion suspension: while a checkpoint holds its copy window, runtime unlink sites
//! skip their deletion phase entirely instead of deferring it per file.

mod common;

#[cfg(feature = "extra_check")]
use mace::observe::{CounterMetric, InMemoryObserver};
#[cfg(feature = "extra_check")]
use mace::testing;
#[cfg(feature = "extra_check")]
use mace::{BucketOptions, Mace, OpCode, Options, RandomPath};
#[cfg(feature = "extra_check")]
use std::fs;
#[cfg(feature = "extra_check")]
use std::path::Path;
#[cfg(feature = "extra_check")]
use std::sync::Arc;
#[cfg(feature = "extra_check")]
use std::time::Duration;

#[cfg(feature = "extra_check")]
fn entries_under(root: &Path, prefix: &str) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(root)
        .expect("directory must be readable")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with(prefix))
        .collect();
    names.sort();
    names
}

#[cfg(feature = "extra_check")]
fn payload_files(data_root: &Path) -> Vec<String> {
    entries_under(data_root, "data_")
}

#[cfg(feature = "extra_check")]
fn wal_segments(log_root: &Path) -> Vec<String> {
    entries_under(log_root, "group_wal_")
}

#[cfg(feature = "extra_check")]
fn payload_ids(data_root: &Path) -> Vec<u64> {
    let mut ids: Vec<u64> = entries_under(data_root, "data_")
        .iter()
        .filter_map(|name| name.strip_prefix("data_").and_then(|raw| raw.parse().ok()))
        .collect();
    ids.sort_unstable();
    ids
}

/// A suspended deletion window must only delay deletion: the gc data path still
/// runs (including its metadata commit), no file that existed when the window
/// opened is unlinked, and closing the window releases exactly that batch.
#[cfg(feature = "extra_check")]
#[test]
fn suspended_deletions_leave_gc_work_running() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    common::deterministic_gc(&mut opt);
    opt.sync_on_write = true;
    opt.concurrent_write = 1;
    opt.data_file_size = 16 << 10;
    opt.data_garbage_ratio = 1;
    let data_root = opt.data_root();
    let mace = Mace::new(opt.validate()?)?;
    let db = mace.new_bucket(
        "x",
        BucketOptions {
            checkpoint_size: 1 << 30,
            pool_capacity: 1 << 30,
            enable_backpressure: false,
            ..BucketOptions::default()
        },
    )?;

    // overwrite the same keys every round so the published files become garbage and
    // give the gc data path reclaim work
    let write_round = |value: u8| -> Result<(), OpCode> {
        for index in 0..400u32 {
            let tx = db.begin()?;
            tx.upsert(format!("k_{index:04}"), vec![value; 256])?;
            tx.commit()?;
        }
        testing::checkpoint_and_wait(&db);
        Ok(())
    };
    write_round(b'a')?;
    for value in *b"bc" {
        write_round(value)?;
    }
    let retained = payload_ids(&data_root);
    assert!(
        !retained.is_empty(),
        "fixture must publish payload files, got {retained:?}"
    );

    let runs_before = mace.data_gc_count();
    let suspended = testing::suspend_deletions(&mace);
    assert!(
        common::gc_rounds_until_with_settle(&mace, 10, Duration::from_millis(50), || {
            mace.data_gc_count() > runs_before
        }),
        "a suspended deletion window must not block gc work or its metadata commit"
    );
    // note: `testing::assert_persisted_gc_stats` is a steady-state helper - it asserts
    // that no obsolete file awaits cleanup, which is exactly what a suspended window
    // leaves behind, so it is checked after the window closes instead
    let during = payload_ids(&data_root);
    for id in &retained {
        assert!(
            during.contains(id),
            "suspended deletion window dropped data_{id}: {retained:?} -> {during:?}"
        );
    }
    drop(suspended);

    assert!(
        common::gc_rounds_until_with_settle(&mace, 10, Duration::from_millis(50), || {
            let now = payload_ids(&data_root);
            retained.iter().any(|id| !now.contains(id))
        }),
        "closing the window must let a later round delete a retained file: {retained:?}"
    );
    common::gc_round(&mace, Duration::from_secs(30));
    testing::assert_persisted_gc_stats(&mace);
    Ok(())
}

#[cfg(feature = "extra_check")]
fn recycled_files(observer: &InMemoryObserver) -> u64 {
    observer
        .snapshot()
        .counters
        .iter()
        .find(|(metric, _)| *metric == CounterMetric::GcWalRecycleFile)
        .map(|(_, value)| *value)
        .unwrap_or(0)
}

#[cfg(feature = "extra_check")]
fn metric_value(observer: &InMemoryObserver, metric: CounterMetric) -> u64 {
    observer
        .snapshot()
        .counters
        .iter()
        .find(|(observed, _)| *observed == metric)
        .map(|(_, value)| *value)
        .unwrap_or(0)
}

/// The checkpoint reports fixed-cardinality start/complete/failure, copied bytes, the
/// number of cut WAL streams and the number of reported frontiers.
#[cfg(feature = "extra_check")]
#[test]
fn checkpoint_reports_fixed_cardinality_metrics() -> Result<(), OpCode> {
    let scratch = RandomPath::tmp();
    std::fs::create_dir_all(&*scratch).expect("scratch dir must be created");
    let observer = Arc::new(InMemoryObserver::new(1024));
    let mut opt = Options::new(scratch.join("db"));
    common::deterministic_gc(&mut opt);
    opt.observer = observer.clone();
    opt.sync_on_write = true;
    let mace = Mace::new(opt.validate()?)?;
    let db = mace.new_bucket("x", BucketOptions::default())?;
    for index in 0..64u32 {
        let tx = db.begin()?;
        tx.put(format!("k_{index:04}"), vec![b'v'; 128])?;
        tx.commit()?;
    }
    testing::checkpoint_and_wait(&db);

    let target = scratch.join("cp");
    mace.create_checkpoint(&target)?;
    assert_eq!(metric_value(&observer, CounterMetric::CheckpointStart), 1);
    assert_eq!(
        metric_value(&observer, CounterMetric::CheckpointComplete),
        1
    );
    assert_eq!(metric_value(&observer, CounterMetric::CheckpointFailure), 0);
    assert!(
        metric_value(&observer, CounterMetric::CheckpointCopiedBytes) > 0,
        "a copy with payload must report copied bytes"
    );
    assert!(
        metric_value(&observer, CounterMetric::CheckpointWalCut) >= 1,
        "the copy must report its cut streams"
    );
    assert!(
        metric_value(&observer, CounterMetric::CheckpointFrontier) >= 1,
        "the copy must report its frontiers"
    );

    // a refused destination is observable as a failure, not silence
    assert_eq!(mace.create_checkpoint(&target), Err(OpCode::Exist));
    assert_eq!(metric_value(&observer, CounterMetric::CheckpointStart), 2);
    assert_eq!(metric_value(&observer, CounterMetric::CheckpointFailure), 1);
    Ok(())
}

/// A suspended deletion window must keep the payload files of a bucket that is
/// deleted from the foreground thread, and the next round must delete exactly
/// that batch once the window closes.
#[cfg(feature = "extra_check")]
#[test]
fn suspended_deletions_keep_deleted_bucket_payloads() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    common::deterministic_gc(&mut opt);
    opt.sync_on_write = true;
    opt.data_file_size = 8 << 10;
    let data_root = opt.data_root();
    let mace = Mace::new(opt.validate()?)?;
    let db = mace.new_bucket("d", BucketOptions::default())?;
    for index in 0..200u32 {
        let tx = db.begin()?;
        tx.put(format!("k_{index:04}"), vec![b'v'; 256])?;
        tx.commit()?;
    }
    testing::checkpoint_and_wait(&db);
    drop(db);
    let published = payload_files(&data_root);
    assert!(
        published.len() > 1,
        "fixture must publish several payload files, got {published:?}"
    );

    let suspended = testing::suspend_deletions(&mace);
    mace.del_bucket("d")?;
    assert_eq!(
        payload_files(&data_root),
        published,
        "a suspended deletion window must keep the deleted bucket's payload files"
    );
    drop(suspended);

    common::gc_round(&mace, Duration::from_secs(30));
    assert!(
        payload_files(&data_root).is_empty(),
        "closing the window must let the next round delete the same batch, got {:?}",
        payload_files(&data_root)
    );
    Ok(())
}

/// A suspended deletion window must freeze wal recycling before its durable
/// intent, keep every segment that already existed, and let a later round
/// recycle the prefix once the window closes.
#[cfg(feature = "extra_check")]
#[test]
fn suspended_deletions_freeze_wal_recycle() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let observer = Arc::new(InMemoryObserver::new(1024));
    let mut opt = Options::new(&*path);
    common::deterministic_gc(&mut opt);
    opt.observer = observer.clone();
    opt.sync_on_write = true;
    opt.concurrent_write = 4;
    opt.wal_file_size = 4096;
    opt.checkpoint_nudge_ms = 0;
    let log_root = opt.log_root();
    let mace = Mace::new(opt.validate()?)?;
    let db = mace.new_bucket("x", BucketOptions::default())?;

    for _ in 0..3000 {
        db.begin()?.commit()?;
    }
    mace.sync()?;
    let initial = wal_segments(&log_root);
    assert!(
        initial.len() > 1,
        "unmodified transactions must rotate the shared wal: {initial:?}"
    );

    // liveness: this fixture really recycles segments in an unsuspended round,
    // so an unchanged counter inside the window below is evidence, not silence
    assert!(
        common::gc_rounds_until(&mace, 8, || recycled_files(&observer) > 0),
        "fixture precondition: an unsuspended round must recycle at least one segment"
    );
    let reclaimed = recycled_files(&observer);
    let retained_before_window = wal_segments(&log_root);

    let suspended = testing::suspend_deletions(&mace);
    // commits keep running inside the window; only deletion is suspended
    for _ in 0..3000 {
        db.begin()?.commit()?;
    }
    mace.sync()?;
    common::gc_round(&mace, Duration::from_secs(30));
    assert_eq!(
        recycled_files(&observer),
        reclaimed,
        "a suspended deletion window must not recycle wal segments"
    );
    let during_window = wal_segments(&log_root);
    for name in &retained_before_window {
        assert!(
            during_window.contains(name),
            "suspended deletion window dropped {name}: {retained_before_window:?} -> {during_window:?}"
        );
    }
    drop(suspended);

    assert!(
        common::gc_rounds_until(&mace, 8, || recycled_files(&observer) > reclaimed),
        "closing the window must let a later round recycle the prefix: {during_window:?}"
    );
    Ok(())
}

/// `create_checkpoint` refuses an existing destination, rejects a destination that
/// overlaps the source roots, never deletes a directory that already sits at the staging
/// path, and serializes concurrent calls.
#[cfg(feature = "extra_check")]
#[test]
fn checkpoint_destination_rules_hold() -> Result<(), OpCode> {
    let scratch = RandomPath::tmp();
    std::fs::create_dir_all(&*scratch).expect("scratch dir must be created");
    let mut opt = Options::new(scratch.join("db"));
    common::deterministic_gc(&mut opt);
    opt.sync_on_write = true;
    let db_root = opt.db_root();
    let mace = Mace::new(opt.validate()?)?;
    let db = mace.new_bucket("x", BucketOptions::default())?;
    let tx = db.begin()?;
    tx.put("k", b"v")?;
    tx.commit()?;

    // existing target
    let existing = db_root.join("existing-backup");
    std::fs::create_dir_all(&existing).expect("existing target must be created");
    assert_eq!(
        mace.create_checkpoint(&existing),
        Err(OpCode::Exist),
        "an existing destination must be refused"
    );

    // destination inside the source roots
    let nested = db_root.join("nested-backup");
    assert_eq!(
        mace.create_checkpoint(&nested),
        Err(OpCode::Invalid),
        "a destination inside the source roots must be refused"
    );
    assert!(
        !nested.exists(),
        "a rejected destination must not be created"
    );

    // a directory at the staging path is never deleted: it cannot be told apart from one
    // the caller owns, so the call is refused and its contents survive untouched
    let target = scratch.join("owned-staging-backup");
    let staging = std::path::PathBuf::from(format!("{}.tmp", target.display()));
    std::fs::create_dir_all(staging.join("data")).expect("owned staging data dir");
    std::fs::create_dir_all(staging.join("log")).expect("owned staging log dir");
    std::fs::write(staging.join("data").join("data_9999"), b"junk").expect("owned payload");
    std::fs::write(staging.join("log").join("manifest"), b"junk").expect("owned manifest");
    assert_eq!(
        mace.create_checkpoint(&target),
        Err(OpCode::Exist),
        "an existing staging directory must be refused"
    );
    assert!(
        !target.exists(),
        "a refused call must not publish a destination"
    );
    assert!(
        staging.join("data").join("data_9999").exists(),
        "a refused call must leave the staging path untouched"
    );
    assert!(
        staging.join("log").join("manifest").exists(),
        "a refused call must leave the staging path untouched"
    );

    // the caller removes its own staging directory, and then the copy succeeds
    std::fs::remove_dir_all(&staging).expect("caller must be able to remove its own dir");
    let info = mace.create_checkpoint(&target)?;
    assert!(target.exists(), "the copy must be published at the target");
    assert!(
        !staging.exists(),
        "staging must not survive a successful copy"
    );
    assert!(info.seq > 0, "the copy names its metadata generation");

    // concurrent calls with the same destination: exactly one wins
    let shared = scratch.join("raced-backup");
    let handles = (0..2)
        .map(|_| {
            let mace = mace.clone();
            let shared = shared.clone();
            std::thread::spawn(move || mace.create_checkpoint(&shared).is_ok())
        })
        .collect::<Vec<_>>();
    let winners = handles
        .into_iter()
        .map(|handle| handle.join().expect("checkpoint thread must finish"))
        .filter(|published| *published)
        .count();
    assert_eq!(
        winners, 1,
        "exactly one concurrent call may publish a destination"
    );
    Ok(())
}

/// The copy is a standalone database equal to the cut: it opens with strict WAL
/// tail handling, holds every commit the copied frontier folds in, and excludes the
/// commits made after `create_checkpoint` returned.
#[cfg(feature = "extra_check")]
#[test]
fn checkpoint_copy_opens_and_matches_the_cut() -> Result<(), OpCode> {
    let scratch = RandomPath::tmp();
    std::fs::create_dir_all(&*scratch).expect("scratch dir must be created");
    let mut opt = Options::new(scratch.join("db"));
    common::deterministic_gc(&mut opt);
    opt.sync_on_write = true;
    opt.wal_file_size = 4096;
    let mace = Mace::new(opt.validate()?)?;
    let db = mace.new_bucket("x", BucketOptions::default())?;
    for index in 0..200u32 {
        let tx = db.begin()?;
        tx.put(format!("k_{index:04}"), vec![b'a'; 64])?;
        tx.commit()?;
    }
    testing::checkpoint_and_wait(&db);

    let target = scratch.join("cut-backup");
    let info = mace.create_checkpoint(&target)?;
    assert!(
        !info.frontiers.is_empty(),
        "the copy must report per-bucket frontiers"
    );
    // the returned frontier is the copy's own durable frontier: compare it against the
    // source's persisted per-group positions for the same bucket
    let bucket_id = db.id();
    let reported = info
        .frontiers
        .iter()
        .find(|frontier| frontier.bucket_id == bucket_id)
        .expect("the copy must report the fixture bucket");
    // the durable frontier carries the fixed writer-group slot space, not just the
    // runtime width
    assert_eq!(
        reported.groups.len(),
        usize::from(Options::MAX_CONCURRENT_WRITE),
        "frontiers must carry one slot per durable group slot"
    );
    for (group, position) in reported.groups.iter().enumerate() {
        let durable = testing::durable_frontier(&db, group as u8);
        assert_eq!(
            (position.file_id, position.offset),
            durable,
            "group {group} frontier must match the source's durable position"
        );
    }

    // commits after the cut must stay out of the copy
    for index in 200..400u32 {
        let tx = db.begin()?;
        tx.put(format!("k_{index:04}"), vec![b'b'; 64])?;
        tx.commit()?;
    }

    let mut copy_opt = Options::new(&target);
    copy_opt.sync_on_write = true;
    copy_opt.truncate_corrupted_wal = false;
    let copy = Mace::new(copy_opt.validate()?)?;
    let copy_db = copy.open_bucket("x")?;
    let view = copy_db.view()?;
    for index in 0..200u32 {
        assert_eq!(
            view.get(format!("k_{index:04}"))?.slice().len(),
            64,
            "the copy must hold every key committed before the cut"
        );
    }
    assert!(
        view.get("k_0200").is_err(),
        "the copy must not hold commits newer than the cut"
    );
    Ok(())
}

/// The relaxed route keeps one WAL stream per logical group, so the copy must carry
/// every group's retained segments and still open with strict tail handling.
#[cfg(feature = "extra_check")]
#[test]
fn checkpoint_covers_relaxed_wal_streams() -> Result<(), OpCode> {
    let scratch = RandomPath::tmp();
    std::fs::create_dir_all(&*scratch).expect("scratch dir must be created");
    let mut opt = Options::new(scratch.join("db"));
    common::deterministic_gc(&mut opt);
    opt.sync_on_write = false;
    opt.concurrent_write = 2;
    opt.wal_file_size = 4096;
    let log_root = opt.log_root();
    let mace = Mace::new(opt.validate()?)?;
    let db = mace.new_bucket("x", BucketOptions::default())?;
    for index in 0..400u32 {
        let tx = db.begin()?;
        tx.put(format!("k_{index:04}"), vec![b'a'; 64])?;
        tx.commit()?;
    }
    testing::checkpoint_and_wait(&db);
    let retained = entries_under(&log_root, "wal_");
    assert!(
        retained.iter().any(|name| name.starts_with("wal_0_"))
            && retained.iter().any(|name| name.starts_with("wal_1_")),
        "the relaxed fixture must write one stream per group: {retained:?}"
    );

    // records committed after the last checkpoint exist only in the WAL, so the copy
    // must carry them and recovery must replay them
    let wal_only = 400..500u32;
    for index in wal_only.clone() {
        let tx = db.begin()?;
        tx.put(format!("k_{index:04}"), vec![b'b'; 64])?;
        tx.commit()?;
    }

    let target = scratch.join("cp");
    mace.create_checkpoint(&target)?;
    let copied = entries_under(&target.join("log"), "wal_");
    for group in ["wal_0_", "wal_1_"] {
        assert!(
            copied.iter().any(|name| name.starts_with(group)),
            "the copy must carry {group} segments: {copied:?}"
        );
    }
    for name in &copied {
        let staged = std::fs::read(target.join("log").join(name)).expect("staged segment");
        assert!(!staged.is_empty(), "{name} must not be an empty segment");
        let source = std::fs::read(log_root.join(name)).expect("source segment");
        let prefix = &source[..staged.len()];
        assert_eq!(
            staged, prefix,
            "{name} must be the source's prefix (the cut bounds the append segment)"
        );
    }

    let mut copy_opt = Options::new(&target);
    copy_opt.sync_on_write = false;
    copy_opt.concurrent_write = 2;
    copy_opt.truncate_corrupted_wal = false;
    let copy = Mace::new(copy_opt.validate()?)?;
    let copy_db = copy.open_bucket("x")?;
    let view = copy_db.view()?;
    for index in 0..500u32 {
        assert_eq!(
            view.get(format!("k_{index:04}"))?.slice().len(),
            64,
            "the copy must hold every key committed before the cut, including the              WAL-only ones"
        );
    }
    Ok(())
}

/// Reading the copied metadata must not modify the copy: enumeration is the payload
/// set and frontier source, so an accidental write there would corrupt the result.
#[cfg(feature = "extra_check")]
#[test]
fn checkpoint_enumeration_leaves_the_copy_untouched() -> Result<(), OpCode> {
    let scratch = RandomPath::tmp();
    std::fs::create_dir_all(&*scratch).expect("scratch dir must be created");
    let mut opt = Options::new(scratch.join("db"));
    common::deterministic_gc(&mut opt);
    opt.sync_on_write = true;
    let mace = Mace::new(opt.validate()?)?;
    let db = mace.new_bucket("x", BucketOptions::default())?;
    let tx = db.begin()?;
    tx.put("k", b"v")?;
    tx.commit()?;
    testing::checkpoint_and_wait(&db);

    let target = scratch.join("enumerate-backup");
    mace.create_checkpoint(&target)?;
    let manifest = target.join("log").join("manifest");
    let before = std::fs::read(&manifest).expect("copy manifest must be readable");

    let copy = btree_store::BTree::open(&manifest).map_err(|_| OpCode::IoError)?;
    let buckets = copy.buckets().map_err(|_| OpCode::IoError)?;
    assert!(buckets.iter().any(|name| name == "data_stat"));
    drop(copy);

    let after = std::fs::read(&manifest).expect("copy manifest must stay readable");
    assert_eq!(
        before, after,
        "opening and enumerating the copy must not write a byte"
    );
    Ok(())
}

/// Published payload files are immutable, so a same-filesystem copy links them
/// instead of duplicating bytes.
#[cfg(all(feature = "extra_check", unix))]
#[test]
fn checkpoint_links_payload_files_on_the_same_filesystem() -> Result<(), OpCode> {
    use std::os::unix::fs::MetadataExt;

    let scratch = RandomPath::tmp();
    std::fs::create_dir_all(&*scratch).expect("scratch dir must be created");
    let mut opt = Options::new(scratch.join("db"));
    common::deterministic_gc(&mut opt);
    opt.sync_on_write = true;
    opt.data_file_size = 8 << 10;
    let data_root = opt.data_root();
    let mace = Mace::new(opt.validate()?)?;
    let db = mace.new_bucket("x", BucketOptions::default())?;
    for index in 0..200u32 {
        let tx = db.begin()?;
        tx.put(format!("k_{index:04}"), vec![b'v'; 256])?;
        tx.commit()?;
    }
    testing::checkpoint_and_wait(&db);

    let target = scratch.join("link-backup");
    mace.create_checkpoint(&target)?;
    let source = payload_files(&data_root);
    assert!(!source.is_empty(), "fixture must publish payload files");
    let copied = payload_files(&target.join("data"));
    assert_eq!(
        copied, source,
        "every published payload file must appear in the copy"
    );
    for name in &source {
        let src = std::fs::metadata(data_root.join(name)).expect("source metadata");
        let dst = std::fs::metadata(target.join("data").join(name)).expect("copy metadata");
        assert_eq!(
            (src.dev(), src.ino()),
            (dst.dev(), dst.ino()),
            "{name} must be hard linked, not duplicated"
        );
    }
    Ok(())
}

/// A failing copy leaves the destination absent and removes its staging directory.
#[cfg(feature = "extra_check")]
#[test]
fn checkpoint_failure_leaves_no_destination() -> Result<(), OpCode> {
    let scratch = RandomPath::tmp();
    std::fs::create_dir_all(&*scratch).expect("scratch dir must be created");
    let mut opt = Options::new(scratch.join("db"));
    common::deterministic_gc(&mut opt);
    opt.sync_on_write = true;
    let mace = Mace::new(opt.validate()?)?;
    let db = mace.new_bucket("x", BucketOptions::default())?;
    let tx = db.begin()?;
    tx.put("k", b"v")?;
    tx.commit()?;

    // a regular file in the destination's parent path makes staging creation fail
    let blocker = scratch.join("not-a-directory");
    std::fs::write(&blocker, b"x").expect("blocker must be written");
    let target = blocker.join("backup");
    let staging = std::path::PathBuf::from(format!("{}.tmp", target.display()));
    assert!(
        mace.create_checkpoint(&target).is_err(),
        "a destination that cannot be created must fail"
    );
    assert!(
        !target.exists(),
        "a failed copy must not publish a destination"
    );
    assert!(
        !staging.exists(),
        "a failed copy must not leave staging behind"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// failpoints feature: crash windows, injected io failures, and the link fallback
// ---------------------------------------------------------------------------

#[cfg(feature = "failpoints")]
mod checkpoint_failpoints {
    use super::*;
    use std::path::PathBuf;
    use std::process::Command;

    const ENV_CHILD: &str = "MACE_CHECKPOINT_FAILPOINT_CHILD";
    const ENV_CASE: &str = "MACE_CHECKPOINT_FAILPOINT_CASE";
    const ENV_ROOT: &str = "MACE_CHECKPOINT_FAILPOINT_ROOT";
    const ENV_TARGET: &str = "MACE_CHECKPOINT_FAILPOINT_TARGET";

    fn prepare(scratch: &std::path::Path) -> Result<PathBuf, OpCode> {
        let root = scratch.join("db");
        let mut opt = Options::new(&root);
        opt.sync_on_write = true;
        opt.wal_file_size = 4096;
        opt.data_file_size = 8 << 10;
        let mace = Mace::new(opt.validate()?)?;
        let db = mace.new_bucket("x", BucketOptions::default())?;
        for index in 0..120u32 {
            let tx = db.begin()?;
            tx.put(format!("k_{index:04}"), vec![b'v'; 256])?;
            tx.commit()?;
        }
        testing::checkpoint_and_wait(&db);
        Ok(root)
    }

    fn spawn(root: &std::path::Path, target: &std::path::Path, case: &str, rule: &str) -> Command {
        let mut cmd = common::child_test_command(&std::env::current_exe().expect("current exe"));
        cmd.arg("--exact")
            .arg("checkpoint_failpoints::child")
            .arg("--nocapture")
            .env(ENV_CHILD, "1")
            .env(ENV_CASE, case)
            .env(ENV_ROOT, root.as_os_str())
            .env(ENV_TARGET, target.as_os_str())
            .env("MACE_FAILPOINT", rule);
        cmd
    }

    #[test]
    fn child() {
        if std::env::var(ENV_CHILD).ok().as_deref() != Some("1") {
            return;
        }
        let case = std::env::var(ENV_CASE).expect("case must be set");
        let root = PathBuf::from(std::env::var(ENV_ROOT).expect("root must be set"));
        let target = PathBuf::from(std::env::var(ENV_TARGET).expect("target must be set"));
        let mut opt = Options::new(&root);
        opt.sync_on_write = true;
        let mace = Mace::new(opt.validate().expect("child open must succeed")).expect("child open");
        let _ = mace.open_bucket("x").expect("child bucket");
        let result = mace.create_checkpoint(&target);
        match case.as_str() {
            // these windows abort inside the call
            "metadata_copy" | "payload_copy" | "write_before_rename" => {
                panic!("failpoint for {case} did not fire: {result:?}")
            }
            // these publish the copy before aborting
            "after_rename_before_dir_sync" | "after_dir_sync" => {
                assert!(
                    result.is_ok(),
                    "the copy must be published first: {result:?}"
                )
            }
            "rename_failure" | "parent_dir_sync_failure" => {
                assert_eq!(
                    result,
                    Err(OpCode::IoError),
                    "injected io failure must surface"
                )
            }
            "hard_link_fallback" => {
                assert!(result.is_ok(), "link fallback must still produce a copy");
                let data_root = Options::new(&root).data_root();
                let copied = super::payload_files(&target.join("data"));
                assert!(!copied.is_empty(), "the copy must hold payload files");
                for name in &copied {
                    let source = data_root.join(name);
                    let copied_file = target.join("data").join(name);
                    assert_eq!(
                        std::fs::read(&source).expect("source read"),
                        std::fs::read(&copied_file).expect("copy read"),
                        "{name} bytes must match"
                    );
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::MetadataExt;
                        let src = std::fs::metadata(&source).expect("source metadata");
                        let dst = std::fs::metadata(&copied_file).expect("copy metadata");
                        assert_ne!(
                            (src.dev(), src.ino()),
                            (dst.dev(), dst.ino()),
                            "{name} must be a real copy when linking fails"
                        );
                    }
                }
            }
            other => panic!("unknown child case {other}"),
        }
    }

    #[cfg(unix)]
    fn assert_aborted(status: std::process::ExitStatus, case: &str) {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(
            status.signal(),
            Some(6),
            "case {case} must abort: {status:?}"
        );
    }

    #[cfg(not(unix))]
    fn assert_aborted(status: std::process::ExitStatus, case: &str) {
        assert!(!status.success(), "case {case} must abort: {status:?}");
    }

    /// every pre-publish crash window leaves the destination absent, and every window
    /// at or after the rename leaves a complete, openable copy
    #[test]
    fn crash_windows_leave_absent_or_complete_targets() {
        for (case, name, published) in [
            (
                "metadata_copy",
                "mace_checkpoint_after_metadata_copy",
                false,
            ),
            (
                "payload_copy",
                "mace_checkpoint_after_payload_copy_before_wal_cut",
                false,
            ),
            (
                "write_before_rename",
                "mace_checkpoint_after_write_before_rename",
                false,
            ),
            (
                "after_rename_before_dir_sync",
                "mace_checkpoint_after_rename_before_dir_sync",
                true,
            ),
            ("after_dir_sync", "mace_checkpoint_after_dir_sync", true),
        ] {
            let scratch = RandomPath::tmp();
            std::fs::create_dir_all(&*scratch).expect("scratch dir");
            let root = prepare(&scratch).expect("fixture must build");
            let target = scratch.join("cp");
            let status = spawn(&root, &target, case, &format!("{name}=abort"))
                .status()
                .expect("child must run");
            assert_aborted(status, case);
            if published {
                assert!(target.exists(), "case {case} must leave a published copy");
                btree_store::BTree::open(target.join("log").join("manifest"))
                    .expect("a published copy must be openable");
            } else {
                assert!(
                    !target.exists(),
                    "case {case} must not publish a destination"
                );
            }
        }
    }

    /// injected rename and directory-sync failures follow the documented contract:
    /// before the rename the destination stays absent, after it the complete copy is
    /// already visible even though the call reports the error
    #[test]
    fn injected_io_failures_follow_the_documented_contract() {
        let scratch = RandomPath::tmp();
        std::fs::create_dir_all(&*scratch).expect("scratch dir");
        let root = prepare(&scratch).expect("fixture must build");
        let target = scratch.join("cp");
        let staging = PathBuf::from(format!("{}.tmp", target.display()));
        let status = spawn(
            &root,
            &target,
            "rename_failure",
            "mace_fs_rename=io(permission_denied)",
        )
        .status()
        .expect("child must run");
        assert!(
            status.success(),
            "the child's assertion must hold: {status:?}"
        );
        assert!(
            !target.exists(),
            "a failed rename must not publish a destination"
        );
        assert!(!staging.exists(), "a failed rename must remove staging");

        let scratch = RandomPath::tmp();
        std::fs::create_dir_all(&*scratch).expect("scratch dir");
        let root = prepare(&scratch).expect("fixture must build");
        let target = scratch.join("cp");
        // the parent-directory sync is the fourth sync_dir call of a successful copy
        let status = spawn(
            &root,
            &target,
            "parent_dir_sync_failure",
            "mace_fs_sync_dir=io(permission_denied)@4",
        )
        .status()
        .expect("child must run");
        assert!(
            status.success(),
            "the child's assertion must hold: {status:?}"
        );
        assert!(
            target.exists(),
            "a failure after the rename leaves the complete copy in place"
        );
        btree_store::BTree::open(target.join("log").join("manifest"))
            .expect("the published copy must be openable");
    }

    /// the cross-filesystem fallback is driven by failing every hard link
    #[test]
    fn linking_failure_falls_back_to_copying_payloads() {
        let scratch = RandomPath::tmp();
        std::fs::create_dir_all(&*scratch).expect("scratch dir");
        let root = prepare(&scratch).expect("fixture must build");
        let target = scratch.join("cp");
        let status = spawn(
            &root,
            &target,
            "hard_link_fallback",
            "mace_fs_hard_link=io(permission_denied)",
        )
        .status()
        .expect("child must run");
        assert!(
            status.success(),
            "the child's assertion must hold: {status:?}"
        );
        btree_store::BTree::open(target.join("log").join("manifest"))
            .expect("a copied destination must be openable");
    }
}

/// Inside a suspended deletion window the gc rewrite path must still publish
/// replacements: this fixture builds one oversized victim with half its records
/// garbage, so the selector rewrites instead of reclaiming.
#[cfg(feature = "extra_check")]
#[test]
fn suspended_deletions_leave_rewrite_running() -> Result<(), OpCode> {
    let source_target = 256usize << 10;
    let rewrite_target = 16usize << 10;
    let keys = (0..512)
        .map(|index| format!("data_{index:04}"))
        .collect::<Vec<_>>();
    let initial = vec![b'a'; 128];
    let updated = vec![b'b'; 128];

    let scratch = RandomPath::tmp();
    std::fs::create_dir_all(&*scratch).expect("scratch dir must be created");
    let mut opt = Options::new(scratch.join("db"));
    common::deterministic_gc(&mut opt);
    opt.concurrent_write = 1;
    opt.sync_on_write = true;
    opt.gc_eager = false;
    opt.data_garbage_ratio = 100;
    opt.data_file_size = source_target;
    let data_root = opt.data_root();

    {
        let mace = Mace::new(opt.clone().validate()?)?;
        let db = mace.new_bucket(
            "oversized_data",
            BucketOptions {
                split_elems: 64,
                consolidate_threshold: 16,
                checkpoint_size: source_target,
                pool_capacity: source_target * 2,
                enable_backpressure: false,
                ..BucketOptions::default()
            },
        )?;
        for key in &keys {
            let tx = db.begin()?;
            tx.put(key, &initial)?;
            tx.commit()?;
        }
        testing::checkpoint_and_wait(&db);
        for key in keys.iter().step_by(2) {
            let tx = db.begin()?;
            tx.upsert(key, &updated)?;
            tx.commit()?;
        }
    }

    // a fresh open with a rewrite-sized target turns the oversized victim into a
    // partial-rewrite candidate
    opt.gc_eager = true;
    opt.data_garbage_ratio = 1;
    opt.data_file_size = rewrite_target;
    let mace = Mace::new(opt.validate()?)?;
    let _db = mace.open_bucket("oversized_data")?;
    let retained = payload_ids(&data_root);
    assert!(!retained.is_empty(), "fixture must publish payload files");
    let runs_before = mace.data_gc_count();
    let max_before = retained.iter().copied().max().unwrap_or(0);

    let suspended = testing::suspend_deletions(&mace);
    assert!(
        common::gc_rounds_until_with_settle(&mace, 10, Duration::from_millis(50), || {
            let ids = payload_ids(&data_root);
            mace.data_gc_count() > runs_before
                && ids.iter().copied().max().unwrap_or(0) > max_before
        }),
        "a suspended deletion window must not block the gc rewrite path: ids {retained:?} -> {:?}",
        payload_ids(&data_root)
    );
    let during = payload_ids(&data_root);
    for id in &retained {
        assert!(
            during.contains(id),
            "suspended deletion window dropped data_{id}: {retained:?} -> {during:?}"
        );
    }
    drop(suspended);

    assert!(
        common::gc_rounds_until_with_settle(&mace, 10, Duration::from_millis(50), || {
            let now = payload_ids(&data_root);
            retained.iter().any(|id| !now.contains(id))
        }),
        "closing the window must let a later round delete a retained victim: {retained:?}"
    );
    common::gc_round(&mace, Duration::from_secs(30));
    testing::assert_persisted_gc_stats(&mace);
    Ok(())
}
