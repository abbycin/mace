#![cfg(feature = "extra_check")]

//! `concurrent_write` epoch migration: source-stream retirement and durable boundaries, the
//! target logger state (floors, oldest ids, group allocation), orphan era files of retired
//! streams, and the idempotence of an open that already recorded the migration
//!
//! beyond those, this file covers the design's non-crash matrix rows: grow on the same route
//! (source tails only in the wal), the grow boot boundary on both routes, shrink on both
//! routes (committed and aborted tails on a retired group, interleaved logical groups in the
//! shared stream), historical page records whose group id belongs to a retired width through
//! point read/scan/conflict/upsert/abort-retry/checkpoint/compaction, a mixed-generation page
//! rewritten by recovery abort-clean, degenerate widths, a wal-less reopen, chained
//! migrations, the post-migration gc round, a width change combined with a route change, and a
//! route-only switch that follows a completed migration. the reopen at the persisted source
//! width after a half-finished wipe is a crash row and lives in the failpoint file.
//!
//! every fixture is produced by the normal writer over real WAL state. crash rows read their
//! source tail out of a child that commits through the writer and then aborts; no test writes
//! wal or page bytes, and the only deletion is the design's wal-less reopen row

mod common;

use btree_store::BTree;
use common::child_test_command;
use mace::observe::{CounterMetric, InMemoryObserver};
use mace::testing;
use mace::{Bucket, BucketOptions, Mace, OpCode, Options, RandomPath, TxnKV};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

const WIDE: u8 = 8;
const FOUR: u8 = 4;
const NARROW: u8 = 2;
const SINGLE: u8 = 1;

// child-process producer: a table of scenario names, its db root and (for the recovery
// abort-clean row) the file where the child parks the aborted transaction id
const ENV_CHILD: &str = "MACE_CWM_CHILD";
const ENV_CHILD_DB: &str = "MACE_CWM_CHILD_DB";
const ENV_CHILD_TXID: &str = "MACE_CWM_CHILD_TXID";

fn wal_files(log_root: &Path) -> Vec<(u8, u64)> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(log_root).into_iter().flatten().flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(rest) = name.strip_prefix("wal_") else {
            continue;
        };
        let Some((physical, file_id)) = rest.split_once('_') else {
            continue;
        };
        let (Ok(physical), Ok(file_id)) = (physical.parse::<u8>(), file_id.parse::<u64>()) else {
            continue;
        };
        files.push((physical, file_id));
    }
    files.sort_unstable();
    files
}

fn shared_wal_files(log_root: &Path) -> Vec<u64> {
    let mut ids: Vec<u64> = std::fs::read_dir(log_root)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|entry| {
            let name = entry.file_name();
            let name = name.to_string_lossy().into_owned();
            name.strip_prefix("group_wal_")
                .and_then(|id| id.parse::<u64>().ok())
        })
        .collect();
    ids.sort_unstable();
    ids
}

fn wal_file_ids(log_root: &Path, physical: u8) -> Vec<u64> {
    if physical == Options::SHARED_ID {
        shared_wal_files(log_root)
    } else {
        wal_files(log_root)
            .into_iter()
            .filter(|(stream, _)| *stream == physical)
            .map(|(_, file_id)| file_id)
            .collect()
    }
}

fn group_wal_file_ids(log_root: &Path) -> Vec<u64> {
    shared_wal_files(log_root)
}

/// every physical stream id the log directory currently holds, in both namespaces
fn all_stream_ids(log_root: &Path) -> Vec<u64> {
    wal_files(log_root)
        .into_iter()
        .map(|(_, file_id)| file_id)
        .chain(shared_wal_files(log_root))
        .collect()
}

/// bytes of committed records a source stream still holds in the wal
fn wal_stream_bytes(log_root: &Path, physical: u8) -> u64 {
    let prefix = if physical == Options::SHARED_ID {
        "group_wal_".to_owned()
    } else {
        format!("wal_{physical}_")
    };
    std::fs::read_dir(log_root)
        .into_iter()
        .flatten()
        .flatten()
        .filter(|entry| {
            entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with(&prefix))
        })
        .filter_map(|entry| entry.metadata().ok().map(|meta| meta.len()))
        .sum()
}

/// physical streams a migration must have retired: the recycle/wipe bound is
/// `max(source, target)` and the shared slot is always covered
fn retired_streams(source: u8, target: u8) -> Vec<u8> {
    (0..source.max(target))
        .chain(std::iter::once(Options::SHARED_ID))
        .collect()
}

fn open(db_root: &Path, concurrent_write: u8, sync_on_write: bool) -> Mace {
    open_with_observer(db_root, concurrent_write, sync_on_write, None)
}

/// the historical-page row has to prove the retired group id really reaches the maintenance
/// path, so it opens with a counter observer; the engine only increments counters under
/// `metrics`, so its assertion is gated the same way
fn open_with_observer(
    db_root: &Path,
    concurrent_write: u8,
    sync_on_write: bool,
    observer: Option<Arc<InMemoryObserver>>,
) -> Mace {
    let mut opt = Options::new(db_root);
    common::deterministic_gc(&mut opt);
    opt.concurrent_write = concurrent_write;
    opt.sync_on_write = sync_on_write;
    if let Some(observer) = observer {
        opt.observer = observer;
    }
    Mace::new(opt.validate().expect("options must validate")).expect("open must succeed")
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

fn persisted_width(options: &Options) -> u8 {
    let tree = BTree::open(options.manifest()).expect("open manifest");
    let mut raw = None;
    tree.view("misc", |txn| {
        raw = Some(txn.get("options")?);
        Ok(())
    })
    .expect("view persisted options");
    let value: serde_json::Value =
        serde_json::from_slice(&raw.expect("persisted options record")).expect("options json");
    value["concurrent_write"]
        .as_u64()
        .expect("persisted width")
        .try_into()
        .expect("width fits a byte")
}

/// one transaction per key, so versions land on every writer group of the wide width
fn seed_keys(bucket: &Bucket, count: u32) {
    for i in 0..count {
        let tx = bucket.begin().expect("begin seed txn");
        tx.put(format!("k{i}"), format!("v{i}"))
            .expect("put seed key");
        tx.commit().expect("commit seed txn");
    }
}

fn assert_keys(bucket: &Bucket, count: u32) {
    let view = bucket.view().expect("open verify view");
    for i in 0..count {
        assert_eq!(
            view.get(format!("k{i}")).expect("seed key missing").slice(),
            format!("v{i}").as_bytes()
        );
    }
}

fn read_value(bucket: &Bucket, key: &str) -> Option<Vec<u8>> {
    let view = bucket.view().expect("open read view");
    match view.get(key) {
        Ok(val) => Some(val.to_vec()),
        Err(OpCode::NotFound) => None,
        Err(err) => panic!("reading {key} failed: {err:?}"),
    }
}

/// begin transactions until one lands on `group`; the misses carry no data, so only their
/// abort records reach the wal
fn txn_on_group<'a>(bucket: &'a Bucket, group: usize) -> TxnKV<'a> {
    loop {
        let tx = bucket.begin().expect("begin");
        let landed = testing::txn_group(bucket, testing::txn_start_ts(&tx)).expect("group");
        if landed == group {
            return tx;
        }
        drop(tx);
    }
}

fn put_on_group(bucket: &Bucket, group: usize, key: &str, value: &[u8]) {
    let tx = txn_on_group(bucket, group);
    tx.put(key, value).expect("put on group");
    tx.commit().expect("commit on group");
}

fn update_on_group(bucket: &Bucket, group: usize, key: &str, value: &[u8]) {
    let tx = txn_on_group(bucket, group);
    tx.update(key, value).expect("update on group");
    tx.commit().expect("commit on group");
}

/// the surviving writer groups of `count` sequential transactions
fn sequential_groups(bucket: &Bucket, count: u32) -> BTreeSet<usize> {
    let mut groups = BTreeSet::new();
    for i in 0..count {
        let tx = bucket.begin().expect("begin probe txn");
        groups.insert(
            testing::txn_group(bucket, testing::txn_start_ts(&tx))
                .expect("a begun txn has a group"),
        );
        tx.put(format!("probe_{i}"), b"v").expect("put probe");
        tx.commit().expect("commit probe");
    }
    groups
}

/// a completed epoch rebuild leaves a durable `done` boundary at the era start on every
/// retired stream: no `STAGE_NONE` residue, no intent left at file 0, no `u64::MAX` cut
fn assert_stream_retired(bucket: &Bucket, physical: u8, epoch_start: u64) {
    let (from, to, done) = testing::wal_recycle_state(bucket, physical);
    assert!(
        done,
        "stream {physical} must hold a completed wipe, got ({from}, {to}, done={done})"
    );
    assert_eq!(
        to, epoch_start,
        "stream {physical} done boundary must be the era start, got ({from}, {to})"
    );
    assert!(
        from < to,
        "stream {physical} must hold a real cut, got ({from}, {to})"
    );
}

/// the epoch start this migration consumed, read off the live target logger
fn epoch_start(bucket: &Bucket) -> u64 {
    testing::logger_oldest_wal_id(bucket, 0)
}

fn spawn_migration_child(db_root: &Path, scenario: &str) -> std::process::ExitStatus {
    let exe = std::env::current_exe().expect("load current exe failed");
    child_test_command(&exe)
        .arg("--exact")
        .arg("migration_child_producer")
        .arg("--nocapture")
        .env(ENV_CHILD, scenario)
        .env(ENV_CHILD_DB, db_root.as_os_str())
        .env(ENV_CHILD_TXID, aborted_txid_path(db_root).as_os_str())
        .status()
        .expect("spawn migration child failed")
}

fn aborted_txid_path(db_root: &Path) -> PathBuf {
    db_root.with_extension("aborted-txid")
}

#[cfg(unix)]
fn assert_child_aborted(status: std::process::ExitStatus, msg: &str) {
    use std::os::unix::process::ExitStatusExt;

    assert_eq!(status.signal(), Some(6), "{msg}");
}

#[cfg(not(unix))]
fn assert_child_aborted(status: std::process::ExitStatus, msg: &str) {
    assert!(!status.success(), "{msg}");
}

// the child fixtures below commit through the normal writer and then abort the process, so the
// committed tail is only in the wal (no exit checkpoint) and every abort-clean task is still
// pending. nothing here edits a wal or page byte.

/// abort with the engine still running: returning from a producer would drop `Mace` and run
/// its exit checkpoint, which materializes exactly the wal-only tail these fixtures need
fn abort_in_flight(mace: Mace) -> ! {
    std::mem::forget(mace);
    std::process::abort()
}

/// the producers pause the collector and disable write backpressure, so neither a collector
/// round nor a foreground flow-control checkpoint can materialize the committed tail into
/// pages before the abort
fn child_bucket_options() -> BucketOptions {
    BucketOptions {
        enable_backpressure: false,
        ..BucketOptions::default()
    }
}

fn child_narrow_relaxed(db_root: &Path) {
    let mace = open(db_root, NARROW, false);
    mace.disable_gc();
    let bucket = mace
        .new_bucket("mig", child_bucket_options())
        .expect("child bucket");
    seed_keys(&bucket, 24);
    abort_in_flight(mace)
}

fn child_narrow_durable(db_root: &Path) {
    let mace = open(db_root, NARROW, true);
    mace.disable_gc();
    let bucket = mace
        .new_bucket("mig", child_bucket_options())
        .expect("child bucket");
    seed_keys(&bucket, 24);
    abort_in_flight(mace)
}

fn child_wide_relaxed(db_root: &Path) {
    child_wide_relaxed_with(db_root, false)
}

/// the historical-page row needs the retired-group leaf to reach the consolidation gate, so its
/// producer lowers the maintenance thresholds; every other row keeps the stock thresholds so
/// its record layout stays what the row was written against
fn child_wide_relaxed_with(db_root: &Path, maintenance: bool) {
    let bucket_options = || {
        if maintenance {
            BucketOptions {
                split_elems: 64,
                consolidate_threshold: 16,
                ..child_bucket_options()
            }
        } else {
            child_bucket_options()
        }
    };
    let mace = open(db_root, FOUR, false);
    mace.disable_gc();
    let bucket = mace
        .new_bucket("mig", bucket_options())
        .expect("child bucket");
    seed_keys(&bucket, 12);
    put_on_group(&bucket, 3, "g3_head", b"from-group-3");
    put_on_group(&bucket, 3, "g3_only", b"retired-only");
    put_on_group(&bucket, 3, "g3_upsert", b"historical-upsert");
    put_on_group(&bucket, 3, "g3_retry", b"historical-retry");
    put_on_group(&bucket, 0, "g0_head", b"from-group-0");
    // a committed tail and an aborted tail on the same retired source group
    let tx = txn_on_group(&bucket, 3);
    tx.put("g3_aborted", b"never").expect("put aborted tail");
    drop(tx);
    // a merge chain whose base and operand both live on the retired group
    let counter = mace
        .new_bucket(
            "cnt",
            BucketOptions {
                merge_operator: mace::u64_add_operator(),
                ..bucket_options()
            },
        )
        .expect("child counter bucket");
    put_on_group(&counter, 3, "counter", 10u64.to_le_bytes().as_slice());
    let tx = txn_on_group(&counter, 3);
    tx.merge("counter", 5u64.to_le_bytes())
        .expect("merge operand");
    tx.commit().expect("commit operand");
    abort_in_flight(mace)
}

fn child_wide_durable(db_root: &Path) {
    let mace = open(db_root, FOUR, true);
    mace.disable_gc();
    let bucket = mace
        .new_bucket("mig", child_bucket_options())
        .expect("child bucket");
    seed_keys(&bucket, 8);
    put_on_group(&bucket, 0, "hot", b"from-g0");
    update_on_group(&bucket, 3, "hot", b"from-g3");
    update_on_group(&bucket, 0, "hot", b"from-g0-again");
    put_on_group(&bucket, 3, "only3", b"kept");
    abort_in_flight(mace)
}

/// a committed tail, a shrink so its pages carry the retired group id, then an in-flight
/// transaction whose records are forced out of the wal ring before the abort. the reopen at
/// the target width rebuilds the abort-clean chain over a page that still holds a retired gid
fn child_mixed_generation(db_root: &Path, txid_path: &Path) {
    {
        let mace = open(db_root, FOUR, false);
        mace.disable_gc();
        let bucket = mace
            .new_bucket("mig", child_bucket_options())
            .expect("child bucket");
        put_on_group(&bucket, 3, "hist", b"historical-from-3");
        put_on_group(&bucket, 3, "keep", b"kept");
        seed_keys(&bucket, 6);
    }
    {
        // the shrink only has to complete; reading a retired-group head here would move the
        // guard's coverage out of the parent's recovery abort-clean
        let mace = open(db_root, NARROW, false);
        drop(mace);
        assert_eq!(persisted_width(&Options::new(db_root)), NARROW);
    }
    // a small ring makes every record reach the wal file while the transaction is still open;
    // the process abort below then leaves a genuine in-flight chain behind
    let mut opt = Options::new(db_root);
    common::deterministic_gc(&mut opt);
    opt.concurrent_write = NARROW;
    opt.sync_on_write = false;
    opt.wal_buffer_size = 1024;
    let mace = Mace::new(opt.validate().expect("child options")).expect("child open");
    mace.disable_gc();
    let bucket = mace.open_bucket("mig").expect("child bucket");
    // the in-flight chain only writes new keys: its leaf is the one holding the retired
    // group's committed head, so recovery's abort-clean has to walk that head
    let tx = bucket.begin().expect("child begin");
    for i in 0..16 {
        tx.put(format!("inflight_{i:02}"), vec![b'x'; 96])
            .expect("child inflight put");
    }
    let txid = testing::txn_start_ts(&tx);
    std::fs::write(txid_path, txid.to_le_bytes()).expect("park aborted txid");
    std::mem::forget(tx);
    abort_in_flight(mace)
}

#[test]
fn migration_child_producer() {
    let Some(scenario) = std::env::var(ENV_CHILD).ok() else {
        return;
    };
    let db_root = PathBuf::from(std::env::var(ENV_CHILD_DB).expect("missing child db root"));
    let txid_path = PathBuf::from(std::env::var(ENV_CHILD_TXID).expect("missing child txid path"));
    match scenario.as_str() {
        "narrow-relaxed" => child_narrow_relaxed(&db_root),
        "narrow-durable" => child_narrow_durable(&db_root),
        "wide-relaxed" => child_wide_relaxed(&db_root),
        "wide-relaxed-maintenance" => child_wide_relaxed_with(&db_root, true),
        "wide-durable" => child_wide_durable(&db_root),
        "mixed-generation" => child_mixed_generation(&db_root, &txid_path),
        other => panic!("unknown migration child scenario {other}"),
    }
    // every producer diverges through `abort_in_flight`
    std::process::abort();
}

#[test]
fn migration_retires_every_source_stream_and_records_the_target_width() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    {
        let mace = open(&path, WIDE, false);
        let bucket = mace.new_bucket("mig", BucketOptions::default())?;
        seed_keys(&bucket, 64);
    }
    let log_root = Options::new(&*path).log_root();
    let source_max = *all_stream_ids(&log_root)
        .iter()
        .max()
        .expect("the wide open must own wal files");

    let mace = open(&path, NARROW, false);
    let bucket = mace.open_bucket("mig")?;
    assert_keys(&bucket, 64);

    // not one pre-migration file id survives, so a stale intent or a half wipe would be visible
    for id in all_stream_ids(&log_root) {
        assert!(
            id > source_max,
            "file id {id} survived the migration: the wipe must retire every source stream"
        );
    }
    let era_start = *all_stream_ids(&log_root)
        .iter()
        .min()
        .expect("the target streams must own era files");

    // every covered stream holds a durable boundary at or above the new era start; a stale
    // `STAGE_INTENT` would report its `from_file_id` (0) instead
    for physical in (0..WIDE).chain(std::iter::once(Options::SHARED_ID)) {
        assert!(
            testing::wal_recycle_boundary(&bucket, physical) >= era_start,
            "stream {physical} has no durable boundary above the era start"
        );
    }
    assert_eq!(
        persisted_width(&Options::new(&*path)),
        NARROW,
        "a completed migration must record the target width"
    );
    assert_eq!(mace.options().concurrent_write, NARROW);
    Ok(())
}

#[test]
fn migrated_runtime_seeds_target_loggers_and_allocates_only_target_groups() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    {
        let mace = open(&path, WIDE, false);
        let bucket = mace.new_bucket("mig", BucketOptions::default())?;
        seed_keys(&bucket, 64);
    }
    let log_root = Options::new(&*path).log_root();
    let mace = open(&path, NARROW, false);
    let bucket = mace.open_bucket("mig")?;
    let era_start = *all_stream_ids(&log_root)
        .iter()
        .min()
        .expect("the target streams must own era files");

    // a relaxed target keeps a real own-slot floor (the MAX sentinel would build a recycle
    // intent with `to_file_id = u64::MAX`) and an era-start oldest id
    for group in 0..NARROW as usize {
        let own = testing::group_checkpoint_floor(&bucket, group, group);
        assert_ne!(
            own,
            (u64::MAX, u64::MAX),
            "group {group} must keep a real own-slot floor"
        );
        assert!(
            own.0 >= era_start,
            "group {group} own floor must point into the new era, got {own:?}"
        );
        for other in 0..NARROW as usize {
            if other != group {
                assert_eq!(
                    testing::group_checkpoint_floor(&bucket, group, other),
                    (u64::MAX, u64::MAX),
                    "group {group} must leave slot {other} inactive"
                );
            }
        }
        // the logger's floor vector follows the target width: the slot right past it is absent
        assert_eq!(
            testing::group_checkpoint_floor(&bucket, group, NARROW as usize),
            (0, 0),
            "group {group} must not carry a source-width floor vector"
        );
        assert!(
            testing::logger_oldest_wal_id(&bucket, group) >= era_start,
            "group {group} logger must not read below the era start"
        );
    }

    // after the shrink, new transactions only reach target groups
    for i in 0..32 {
        let tx = bucket.begin()?;
        let group = testing::txn_group(&bucket, testing::txn_start_ts(&tx))
            .expect("a begun txn must publish its group");
        assert!(
            group < NARROW as usize,
            "a new transaction landed on retired group {group}"
        );
        tx.put(format!("new_{i}"), b"v")?;
        tx.commit()?;
    }
    Ok(())
}

#[test]
fn durable_target_migration_leaves_every_logical_floor_inactive() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    {
        let mace = open(&path, WIDE, true);
        let bucket = mace.new_bucket("mig", BucketOptions::default())?;
        seed_keys(&bucket, 32);
    }
    let log_root = Options::new(&*path).log_root();
    let mace = open(&path, NARROW, true);
    let bucket = mace.open_bucket("mig")?;
    assert_keys(&bucket, 32);

    let era_start = *all_stream_ids(&log_root)
        .iter()
        .min()
        .expect("the target streams must own era files");
    assert!(
        !group_wal_file_ids(&log_root).is_empty(),
        "a durable target must own a shared era file"
    );
    for slot in 0..NARROW as usize {
        assert_eq!(
            testing::shared_checkpoint_floor(&bucket, slot),
            (u64::MAX, u64::MAX),
            "shared slot {slot} must start inactive after the wipe"
        );
    }
    assert!(testing::logger_oldest_wal_id(&bucket, 0) >= era_start);
    Ok(())
}

/// the width was already recorded, so this open is a plain one: no wipe, no new era, and no
/// per-stream id set or durable boundary may move. comparing while the store is open catches a
/// delete-and-recreate that reuses the same ids
#[test]
fn reopen_after_a_completed_migration_starts_no_new_era() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    {
        let mace = open(&path, WIDE, false);
        let bucket = mace.new_bucket("mig", BucketOptions::default())?;
        seed_keys(&bucket, 32);
    }
    let log_root = Options::new(&*path).log_root();
    let mace = open(&path, NARROW, false);
    let bucket = mace.open_bucket("mig")?;
    assert_keys(&bucket, 32);

    // snapshot the settled namespace and the durable boundaries with the store still open
    let settled: Vec<(u8, Vec<u64>, (u64, u64, bool))> = (0..NARROW)
        .chain(std::iter::once(Options::SHARED_ID))
        .map(|physical| {
            (
                physical,
                wal_file_ids(&log_root, physical),
                testing::wal_recycle_state(&bucket, physical),
            )
        })
        .collect();
    let settled_oldest = testing::logger_oldest_wal_id(&bucket, 0);
    drop(bucket);
    drop(mace);

    let mace = open(&path, NARROW, false);
    let bucket = mace.open_bucket("mig")?;
    assert_keys(&bucket, 32);
    for (physical, ids, recycle) in settled {
        assert_eq!(
            wal_file_ids(&log_root, physical),
            ids,
            "stream {physical} file ids moved on a settled reopen"
        );
        assert_eq!(
            testing::wal_recycle_state(&bucket, physical),
            recycle,
            "stream {physical} durable boundary moved on a settled reopen"
        );
    }
    assert_eq!(
        testing::logger_oldest_wal_id(&bucket, 0),
        settled_oldest,
        "a settled reopen must keep the era start"
    );
    Ok(())
}

/// design row "grow same route": the source tail is only in the wal of both source groups, so
/// the target recovery must replay it before the new era starts above every old file
#[test]
fn grow_same_route_recovers_every_source_group_tail() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    assert_child_aborted(
        spawn_migration_child(&path, "narrow-relaxed"),
        "the wal-only source producer must abort, so no exit checkpoint hides the tail",
    );
    let log_root = Options::new(&*path).log_root();
    for physical in 0..NARROW {
        assert!(
            wal_stream_bytes(&log_root, physical) > 0,
            "source group {physical} must carry uncheckpointed committed wal"
        );
    }
    let source_max = *all_stream_ids(&log_root)
        .iter()
        .max()
        .expect("the source streams must own wal files");

    let mace = open(&path, FOUR, false);
    let bucket = mace.open_bucket("mig")?;
    assert_keys(&bucket, 24);
    assert_eq!(
        sequential_groups(&bucket, 8),
        BTreeSet::from([0, 1, 2, 3]),
        "a grown runtime must allocate every target group"
    );
    assert!(
        all_stream_ids(&log_root).iter().all(|id| *id > source_max),
        "the new epoch must start above every source file"
    );
    assert_eq!(persisted_width(&Options::new(&*path)), FOUR);
    Ok(())
}

/// design row "grow boot boundary", both routes: the target boot list is built from the post
/// wipe state, target-only streams carry `done == epoch_start` (never a `STAGE_NONE` file 0),
/// and the shared stream's boundary is its first retained era file
#[test]
fn grow_boot_boundary_holds_on_both_routes() -> Result<(), OpCode> {
    for sync_on_write in [false, true] {
        let path = RandomPath::tmp();
        let scenario = if sync_on_write {
            "narrow-durable"
        } else {
            "narrow-relaxed"
        };
        assert_child_aborted(
            spawn_migration_child(&path, scenario),
            "the boot-boundary producer must abort with its tail only in the wal",
        );
        let log_root = Options::new(&*path).log_root();
        let source_max = *all_stream_ids(&log_root)
            .iter()
            .max()
            .expect("the source streams must own wal files");

        let mace = open(&path, FOUR, sync_on_write);
        let bucket = mace.open_bucket("mig")?;
        assert_keys(&bucket, 24);
        let era_start = epoch_start(&bucket);
        assert!(
            era_start > source_max,
            "era start {era_start} must sit above the source high water {source_max}"
        );
        for physical in retired_streams(NARROW, FOUR) {
            assert_stream_retired(&bucket, physical, era_start);
        }

        // every stream that owns era files starts at or above the epoch start
        for physical in (0..FOUR).chain(std::iter::once(Options::SHARED_ID)) {
            let ids = wal_file_ids(&log_root, physical);
            if let Some(first) = ids.first() {
                assert!(
                    *first >= era_start,
                    "stream {physical} retained a pre-era file {first}"
                );
            }
        }
        if sync_on_write {
            // the shared stream's durable boundary is its first retained era file, never file 0
            let shared = shared_wal_files(&log_root);
            assert_eq!(
                shared.first().copied(),
                Some(era_start),
                "the shared era file must start at the epoch start"
            );
            assert_eq!(
                testing::wal_recycle_state(&bucket, Options::SHARED_ID).1,
                shared[0],
                "the shared done boundary must match its first retained file"
            );
        } else {
            // a relaxed target never writes the shared namespace
            assert!(
                shared_wal_files(&log_root).is_empty(),
                "a relaxed target must not own a shared stream"
            );
            for physical in 0..FOUR {
                assert_eq!(
                    wal_file_ids(&log_root, physical).first().copied(),
                    Some(era_start),
                    "relaxed stream {physical} must open its era at the epoch start"
                );
            }
        }
    }
    Ok(())
}

/// design row "shrink relaxed": group 3's committed tail exists only in `wal_3_*`, so a
/// retained scan shortened to the target width loses it; the aborted tail on the same stream
/// must not resurrect
#[test]
fn shrink_relaxed_scans_the_retired_stream_and_drops_its_aborted_tail() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    assert_child_aborted(
        spawn_migration_child(&path, "wide-relaxed"),
        "the shrink producer must abort with group 3's committed tail only in the wal",
    );
    let log_root = Options::new(&*path).log_root();
    assert!(
        wal_stream_bytes(&log_root, 3) > 0,
        "the retired source group must carry uncheckpointed committed wal"
    );

    let mace = open(&path, NARROW, false);
    let bucket = mace.open_bucket("mig")?;
    assert_keys(&bucket, 12);
    assert_eq!(
        read_value(&bucket, "g3_only").as_deref(),
        Some(&b"retired-only"[..])
    );
    assert_eq!(
        read_value(&bucket, "g3_head").as_deref(),
        Some(&b"from-group-3"[..])
    );
    assert_eq!(
        read_value(&bucket, "g0_head").as_deref(),
        Some(&b"from-group-0"[..])
    );
    assert!(
        read_value(&bucket, "g3_aborted").is_none(),
        "an aborted tail on the retired stream must not resurrect"
    );

    let era_start = epoch_start(&bucket);
    for physical in retired_streams(FOUR, NARROW) {
        assert_stream_retired(&bucket, physical, era_start);
    }
    // a stream the target width retires keeps no file of its own: its era start lives in
    // the durable done boundary asserted above, and an era file here would mean the retired
    // slot was opened again
    assert!(
        wal_file_ids(&log_root, 3).is_empty(),
        "the retired stream must hold no wal file: {:?}",
        wal_file_ids(&log_root, 3)
    );
    assert_eq!(
        sequential_groups(&bucket, 6),
        BTreeSet::from([0, 1]),
        "the shrunk runtime must only allocate target groups"
    );
    Ok(())
}

/// design row "shrink durable": logical groups 0 and 3 interleave on the shared stream, so the
/// per-logical frontier and the shared recycle cut must both keep group 3's history
#[test]
fn shrink_durable_keeps_interleaved_logical_group_history() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    assert_child_aborted(
        spawn_migration_child(&path, "wide-durable"),
        "the durable shrink producer must abort with its tail only in the shared wal",
    );
    let log_root = Options::new(&*path).log_root();
    assert!(
        wal_stream_bytes(&log_root, Options::SHARED_ID) > 0,
        "the shared stream must carry uncheckpointed committed wal"
    );

    let mace = open(&path, NARROW, true);
    let bucket = mace.open_bucket("mig")?;
    assert_keys(&bucket, 8);
    assert_eq!(
        read_value(&bucket, "hot").as_deref(),
        Some(&b"from-g0-again"[..]),
        "the interleaved logical updates must replay in order"
    );
    assert_eq!(
        read_value(&bucket, "only3").as_deref(),
        Some(&b"kept"[..]),
        "group 3's history must survive the shared recycle cut"
    );

    let era_start = epoch_start(&bucket);
    for physical in retired_streams(FOUR, NARROW) {
        assert_stream_retired(&bucket, physical, era_start);
    }
    for slot in 0..NARROW as usize {
        assert_eq!(
            testing::shared_checkpoint_floor(&bucket, slot),
            (u64::MAX, u64::MAX),
            "shared slot {slot} must start inactive after the wipe"
        );
    }
    // the first target write publishes its own logical slot inside the new era
    let tx = bucket.begin()?;
    tx.put("post", b"p")?;
    tx.commit()?;
    let (file_id, offset) = testing::shared_checkpoint_floor(&bucket, 0);
    assert_eq!(file_id, era_start, "slot 0 must point into the new era");
    assert!(offset < u64::MAX);
    assert_eq!(
        testing::shared_checkpoint_floor(&bucket, 1),
        (u64::MAX, u64::MAX),
        "an untouched logical slot must stay inactive"
    );
    Ok(())
}

/// design row "historical page paths": a page record whose visible head belongs to a retired
/// group must read, scan, refuse a first-writer-wins insert, accept an upsert, recover from an
/// aborted head, and survive checkpoint and both compaction paths
#[test]
fn historical_retired_group_records_survive_reads_and_maintenance() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    assert_child_aborted(
        spawn_migration_child(&path, "wide-relaxed-maintenance"),
        "the historical-page producer must abort with its tail only in the wal",
    );
    let merge_opts = BucketOptions {
        merge_operator: mace::u64_add_operator(),
        ..BucketOptions::default()
    };
    let observer = Arc::new(InMemoryObserver::new(256));
    let mace = open_with_observer(&path, NARROW, false, Some(observer.clone()));
    let bucket = mace.open_bucket("mig")?;
    assert_keys(&bucket, 12);

    // point read of a head that lives on the retired group
    assert_eq!(
        read_value(&bucket, "g3_only").as_deref(),
        Some(&b"retired-only"[..])
    );
    // prefix scan: exactly the committed retired-group keys, never the aborted one
    let view = bucket.view()?;
    let mut scanned = Vec::new();
    let mut iter = view.seek("g3");
    while let Some(item) = iter.try_next()? {
        scanned.push(String::from_utf8_lossy(item.key()).into_owned());
    }
    drop(iter);
    drop(view);
    assert_eq!(
        scanned,
        vec![
            "g3_head".to_owned(),
            "g3_only".to_owned(),
            "g3_retry".to_owned(),
            "g3_upsert".to_owned(),
        ]
    );
    // first-writer-wins: a plain insert must refuse the existing historical head
    let tx = bucket.begin()?;
    assert!(matches!(tx.put("g3_only", b"second"), Err(OpCode::Exist)));
    drop(tx);
    // upsert replaces the historical head
    let tx = bucket.begin()?;
    tx.upsert("g3_upsert", b"replaced")?;
    tx.commit()?;
    assert_eq!(
        read_value(&bucket, "g3_upsert").as_deref(),
        Some(&b"replaced"[..])
    );
    // an aborted head in front of the historical chain is cleaned and retried
    let tx = txn_on_group(&bucket, 0);
    tx.upsert("g3_retry", b"aborted-head")?;
    drop(tx);
    let tx = bucket.begin()?;
    tx.upsert("g3_retry", b"retried")?;
    tx.commit()?;
    assert_eq!(
        read_value(&bucket, "g3_retry").as_deref(),
        Some(&b"retried"[..])
    );
    // a checkpoint round persists the replaced heads
    testing::checkpoint_and_wait(&bucket);
    assert_eq!(
        read_value(&bucket, "g3_upsert").as_deref(),
        Some(&b"replaced"[..])
    );
    assert_eq!(
        read_value(&bucket, "g3_retry").as_deref(),
        Some(&b"retried"[..])
    );
    // plain compaction: one transaction holding many versions of the historical key
    let tx = bucket.begin()?;
    for i in 0..32 {
        tx.update("g3_only", format!("compact-{i}"))?;
    }
    tx.commit()?;
    assert_eq!(
        read_value(&bucket, "g3_only").as_deref(),
        Some(&b"compact-31"[..])
    );
    testing::checkpoint_and_wait(&bucket);
    assert_eq!(
        read_value(&bucket, "g3_only").as_deref(),
        Some(&b"compact-31"[..])
    );

    // merge compaction: the folded base and its first operand both come from the retired group
    let counter = mace.open_bucket_with_options("cnt", merge_opts.clone())?;
    let fold = |counter: &Bucket| -> u64 {
        let view = counter.view().expect("counter view");
        u64::from_le_bytes(
            view.get("counter")
                .expect("counter key")
                .slice()
                .try_into()
                .expect("u64"),
        )
    };
    assert_eq!(
        fold(&counter),
        15,
        "the retired group's merge chain must fold"
    );
    for operand in [1u64, 2, 4, 8] {
        let tx = counter.begin()?;
        tx.merge("counter", operand.to_le_bytes())?;
        tx.commit()?;
    }
    let tx = counter.begin()?;
    for _ in 0..40u64 {
        tx.merge("counter", 1u64.to_le_bytes())?;
    }
    tx.commit()?;
    // 15 + 1 + 2 + 4 + 8 + 40
    assert_eq!(
        fold(&counter),
        70,
        "merge compaction must preserve the fold"
    );
    testing::checkpoint_and_wait(&counter);
    assert_eq!(fold(&counter), 70);
    // the row exists to prove a retired group id survives the maintenance path, so the fixture
    // must really reach the consolidation gate: without it neither PlainLeafFilter::check nor
    // LeafFilter::drops_retained_abort would ever see the retired id
    if cfg!(feature = "metrics") {
        assert!(
            counter_value(&observer, CounterMetric::TreeNodeConsolidate) > 0,
            "the historical-page fixture must consolidate the retired-group leaf"
        );
    }
    drop(counter);
    drop(bucket);
    drop(mace);

    let mace = open(&path, NARROW, false);
    let bucket = mace.open_bucket("mig")?;
    assert_eq!(
        read_value(&bucket, "g3_only").as_deref(),
        Some(&b"compact-31"[..])
    );
    assert_keys(&bucket, 12);
    let counter = mace.open_bucket_with_options("cnt", merge_opts)?;
    assert_eq!(fold(&counter), 70);
    Ok(())
}

/// design row "mixed generation page": after the shrink a page holds a retired group id, and
/// the reopen at the target width rebuilds the abort-clean chain of the in-flight transaction
/// over that page. the historical group id must not index the target group table
#[test]
fn mixed_generation_page_survives_recovery_abort_clean() -> Result<(), OpCode> {
    // the aborting child parks its txid next to the db root, so the guard must own both:
    // an abort skips the child's `Drop`, and only the parent can remove this tree
    let scratch = RandomPath::tmp();
    let path = scratch.join("db");
    assert_child_aborted(
        spawn_migration_child(&path, "mixed-generation"),
        "the mixed-generation producer must abort with an in-flight transaction",
    );
    let raw = std::fs::read(aborted_txid_path(&path)).expect("parked aborted txid");
    let aborted_txid = u64::from_le_bytes(raw.try_into().expect("8 byte txid"));
    let log_root = Options::new(&*path).log_root();
    let source_max = *all_stream_ids(&log_root)
        .iter()
        .max()
        .expect("the era must own wal files");

    let mace = open(&path, NARROW, false);
    let bucket = mace.open_bucket("mig")?;
    // recovery scanned the in-flight chain and advanced the oracle past its txid; a scan that
    // skipped the era file would leave the persisted oracle at the aborted txid
    assert!(
        testing::safe_exclusive(&bucket) > aborted_txid,
        "recovery must have marked the in-flight transaction aborted \
         (safe_exclusive {}, aborted txid {aborted_txid})",
        testing::safe_exclusive(&bucket)
    );
    // the retired group's committed head and its page survive the abort-clean rewrite
    assert_eq!(
        read_value(&bucket, "hist").as_deref(),
        Some(&b"historical-from-3"[..])
    );
    assert_eq!(read_value(&bucket, "keep").as_deref(), Some(&b"kept"[..]));
    assert_keys(&bucket, 6);
    for i in 0..16 {
        assert!(
            read_value(&bucket, &format!("inflight_{i:02}")).is_none(),
            "an uncommitted key must never resurrect"
        );
    }
    // the same-width reopen must not have opened a new era
    assert!(
        all_stream_ids(&log_root).iter().all(|id| *id <= source_max),
        "a same-width recovery must not bump the epoch"
    );
    // writing on top of the historical head still works
    let tx = bucket.begin()?;
    tx.upsert("hist", b"after-recovery")?;
    tx.commit()?;
    assert_eq!(
        read_value(&bucket, "hist").as_deref(),
        Some(&b"after-recovery"[..])
    );
    Ok(())
}

/// design row "degenerate widths": the source and target boot slices must not assume at least
/// two writer groups
#[test]
fn degenerate_widths_migrate_with_a_single_group_slice() -> Result<(), OpCode> {
    for (source, target) in [(SINGLE, NARROW), (NARROW, SINGLE)] {
        let path = RandomPath::tmp();
        {
            let mace = open(&path, source, false);
            let bucket = mace.new_bucket("mig", BucketOptions::default())?;
            seed_keys(&bucket, 8);
        }
        let log_root = Options::new(&*path).log_root();
        let source_max = *all_stream_ids(&log_root)
            .iter()
            .max()
            .expect("the source streams must own wal files");

        let mace = open(&path, target, false);
        let bucket = mace.open_bucket("mig")?;
        assert_keys(&bucket, 8);
        let era_start = epoch_start(&bucket);
        assert!(era_start > source_max);
        for physical in retired_streams(source, target) {
            assert_stream_retired(&bucket, physical, era_start);
        }
        let expected: BTreeSet<usize> = (0..target as usize).collect();
        assert_eq!(
            sequential_groups(&bucket, 6),
            expected,
            "{source} -> {target} must allocate exactly the target groups"
        );
    }
    Ok(())
}

/// design row "clean exit without wal": the old width closes cleanly, every wal file is
/// deleted, and the shrink reopen must rebuild from the data pages alone (the only place this
/// file deletes a file, and it deletes wal files only)
#[test]
fn walless_clean_exit_keeps_only_pages_with_the_old_high_group() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    {
        let mace = open(&path, FOUR, false);
        let bucket = mace.new_bucket("mig", BucketOptions::default())?;
        seed_keys(&bucket, 12);
        put_on_group(&bucket, 3, "g3_only", b"retired-only");
        // the clean exit path persists every page, including the retired group's records
        testing::checkpoint_and_wait(&bucket);
    }
    let log_root = Options::new(&*path).log_root();
    let mut removed = 0;
    for entry in std::fs::read_dir(&log_root)
        .expect("read log dir")
        .flatten()
    {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with("wal_") || name.starts_with("group_wal_") {
            std::fs::remove_file(entry.path()).expect("remove wal file");
            removed += 1;
        }
    }
    assert!(removed > 0, "the wide open must have owned wal files");
    assert!(
        all_stream_ids(&log_root).is_empty(),
        "every wal file must be gone before the reopen"
    );

    let mace = open(&path, NARROW, false);
    let bucket = mace.open_bucket("mig")?;
    assert_keys(&bucket, 12);
    assert_eq!(
        read_value(&bucket, "g3_only").as_deref(),
        Some(&b"retired-only"[..]),
        "the retired group's page record must carry the store without any wal"
    );
    let era_start = epoch_start(&bucket);
    for physical in retired_streams(FOUR, NARROW) {
        assert_stream_retired(&bucket, physical, era_start);
        assert!(
            wal_file_ids(&log_root, physical)
                .iter()
                .all(|id| *id >= era_start),
            "stream {physical} must not re-create a pre-era file"
        );
    }
    assert_eq!(persisted_width(&Options::new(&*path)), NARROW);

    let tx = bucket.begin()?;
    tx.put("post", b"p")?;
    tx.commit()?;
    drop(bucket);
    drop(mace);
    let mace = open(&path, NARROW, false);
    let bucket = mace.open_bucket("mig")?;
    assert_keys(&bucket, 12);
    assert_eq!(read_value(&bucket, "post").as_deref(), Some(&b"p"[..]));
    assert_eq!(
        read_value(&bucket, "g3_only").as_deref(),
        Some(&b"retired-only"[..])
    );
    Ok(())
}

/// design row "chained migration high water": a shrink followed by a grow with no new era
/// writes must still place the second epoch start above the first migration's retired-stream
/// done boundaries
#[test]
fn chained_migrations_advance_the_epoch_high_water() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    assert_child_aborted(
        spawn_migration_child(&path, "wide-relaxed"),
        "the chained producer must abort with its tail only in the wal",
    );
    let log_root = Options::new(&*path).log_root();

    let mace = open(&path, NARROW, false);
    let bucket = mace.open_bucket("mig")?;
    assert_keys(&bucket, 12);
    let first_done: Vec<(u8, u64, u64)> = [2u8, 3]
        .into_iter()
        .map(|physical| {
            let (from, to, done) = testing::wal_recycle_state(&bucket, physical);
            assert!(done);
            (physical, from, to)
        })
        .collect();
    let first_start = epoch_start(&bucket);
    drop(bucket);
    drop(mace);

    // no write (and therefore no committed wal) separates the two migrations
    let mace = open(&path, FOUR, false);
    let bucket = mace.open_bucket("mig")?;
    assert_keys(&bucket, 12);
    let second_start = epoch_start(&bucket);
    for (physical, from, to) in first_done {
        assert!(
            to < second_start,
            "source group {physical} done {to} must stay below the second epoch start {second_start}"
        );
        assert!(from < to);
    }
    assert!(second_start > first_start);
    for physical in retired_streams(FOUR, FOUR) {
        assert_stream_retired(&bucket, physical, second_start);
        assert!(
            wal_file_ids(&log_root, physical)
                .iter()
                .all(|id| *id >= second_start),
            "stream {physical} must not retain a first-era file"
        );
    }
    Ok(())
}

/// design row "relaxed floor/recycle boundary": after a relaxed migration covering a
/// zero-write group and a begin+abort-only transaction, one gc round must leave real floors
/// and a real recycle cut (no `u64::MAX` intent, done or oldest id)
#[test]
fn relaxed_gc_round_after_a_migration_keeps_real_floors_and_boundaries() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    {
        let mace = open(&path, NARROW, false);
        let bucket = mace.new_bucket("mig", BucketOptions::default())?;
        seed_keys(&bucket, 8);
    }
    let mace = open(&path, FOUR, false);
    let bucket = mace.open_bucket("mig")?;
    let era_start = epoch_start(&bucket);
    // groups 2 and 3 receive no data; group 3 gets one begin+abort-only transaction
    let tx = txn_on_group(&bucket, (FOUR - 1) as usize);
    assert_eq!(
        testing::txn_group(&bucket, testing::txn_start_ts(&tx)),
        Some((FOUR - 1) as usize),
        "the abort-only transaction must sit on a zero-write group"
    );
    drop(tx);

    common::gc_round(&mace, Duration::from_secs(30));

    for group in 0..FOUR as usize {
        let own = testing::group_checkpoint_floor(&bucket, group, group);
        assert_ne!(
            own,
            (u64::MAX, u64::MAX),
            "group {group} must keep a real own-slot floor after gc"
        );
        assert!(
            own.0 >= era_start,
            "group {group} own floor {own:?} left the era"
        );
        assert!(own.1 < u64::MAX);
        assert_eq!(
            testing::group_checkpoint_floor(&bucket, group, FOUR as usize),
            (0, 0),
            "group {group} must not carry a wider floor vector"
        );
        let oldest = testing::logger_oldest_wal_id(&bucket, group);
        assert_ne!(oldest, u64::MAX, "group {group} logger oldest must be real");
        assert!(oldest >= era_start);
        let (file_id, offset) = testing::wal_durable_pos(&bucket, group);
        assert_ne!(file_id, u64::MAX);
        assert!(offset < u64::MAX);
    }
    for physical in retired_streams(NARROW, FOUR) {
        assert_stream_retired(&bucket, physical, era_start);
        let (from, to, done) = testing::wal_recycle_state(&bucket, physical);
        assert_ne!(from, u64::MAX);
        assert_ne!(to, u64::MAX);
        assert!(done);
    }
    assert_keys(&bucket, 8);
    Ok(())
}

/// design row "width change with a route change": recovery scans at the source width, the wipe
/// and recycle cover `max(source, target)`, and the new runtime owns the target route's stream
#[test]
fn width_and_route_change_scan_at_source_and_run_at_target() -> Result<(), OpCode> {
    // relaxed source -> durable target: the per-group source wals are replayed, the runtime
    // continues on the shared stream only
    let path = RandomPath::tmp();
    assert_child_aborted(
        spawn_migration_child(&path, "wide-relaxed"),
        "the relaxed source producer must abort with its tail only in the per-group wals",
    );
    let log_root = Options::new(&*path).log_root();
    let source_max = *all_stream_ids(&log_root)
        .iter()
        .max()
        .expect("the source streams must own wal files");
    assert!(wal_stream_bytes(&log_root, 3) > 0);

    let mace = open(&path, NARROW, true);
    let bucket = mace.open_bucket("mig")?;
    assert_keys(&bucket, 12);
    assert_eq!(
        read_value(&bucket, "g3_only").as_deref(),
        Some(&b"retired-only"[..])
    );
    assert!(testing::loggings_shared_across_groups(&bucket));
    let era_start = epoch_start(&bucket);
    assert!(era_start > source_max);
    for physical in retired_streams(FOUR, NARROW) {
        assert_stream_retired(&bucket, physical, era_start);
    }
    assert!(
        wal_files(&log_root).is_empty(),
        "a durable runtime must not keep per-group streams"
    );
    assert_eq!(shared_wal_files(&log_root), vec![era_start]);
    let tx = bucket.begin()?;
    tx.put("post", b"p")?;
    tx.commit()?;
    assert_eq!(
        shared_wal_files(&log_root),
        vec![era_start],
        "target writes must stay on the shared era file"
    );

    // durable source -> relaxed target: the shared source stream is replayed, the runtime
    // writes per-group era files
    let path = RandomPath::tmp();
    assert_child_aborted(
        spawn_migration_child(&path, "wide-durable"),
        "the durable source producer must abort with its tail only in the shared wal",
    );
    let log_root = Options::new(&*path).log_root();
    assert!(wal_stream_bytes(&log_root, Options::SHARED_ID) > 0);

    let mace = open(&path, NARROW, false);
    let bucket = mace.open_bucket("mig")?;
    assert_keys(&bucket, 8);
    assert_eq!(read_value(&bucket, "only3").as_deref(), Some(&b"kept"[..]));
    assert!(!testing::loggings_shared_across_groups(&bucket));
    let era_start = epoch_start(&bucket);
    for physical in retired_streams(FOUR, NARROW) {
        assert_stream_retired(&bucket, physical, era_start);
    }
    for physical in 0..NARROW {
        assert_eq!(
            wal_file_ids(&log_root, physical).first().copied(),
            Some(era_start),
            "relaxed target stream {physical} must open its era at the epoch start"
        );
    }
    // the target width retires the source width's remaining streams: they hold no wal
    // file of their own, and their era start is the durable done boundary asserted above
    for physical in NARROW..FOUR {
        assert!(
            wal_file_ids(&log_root, physical).is_empty(),
            "a target-retired stream must hold no wal file: stream {physical} kept {:?}",
            wal_file_ids(&log_root, physical)
        );
    }
    // the retired shared stream keeps a real cut into the new era
    let (from, to, done) = testing::wal_recycle_state(&bucket, Options::SHARED_ID);
    assert!(done && to == era_start && from < to);
    Ok(())
}

/// design row "route-only switch after a completed migration": the switch performs its own
/// epoch rebuild, so it must keep the historical retired-group records, must not reuse a
/// pre-era file id, and must advance the epoch instead of returning to an older one
#[test]
fn route_switch_after_a_completed_migration_keeps_the_era() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    assert_child_aborted(
        spawn_migration_child(&path, "wide-relaxed"),
        "the route-switch producer must abort with its tail only in the per-group wals",
    );
    let log_root = Options::new(&*path).log_root();

    // the shrink completes on the relaxed route and records the narrow width
    let mace = open(&path, NARROW, false);
    let bucket = mace.open_bucket("mig")?;
    assert_keys(&bucket, 12);
    assert_eq!(
        read_value(&bucket, "g3_only").as_deref(),
        Some(&b"retired-only"[..])
    );
    let migrated_era = epoch_start(&bucket);
    let migrated_high_water = *all_stream_ids(&log_root)
        .iter()
        .max()
        .expect("the migrated era must own wal files");
    drop(bucket);
    drop(mace);

    // a route-only switch rebuilds the epoch without changing the width: the records written
    // under the retired group must survive it
    let mace = open(&path, NARROW, true);
    let bucket = mace.open_bucket("mig")?;
    assert_keys(&bucket, 12);
    assert_eq!(
        read_value(&bucket, "g3_only").as_deref(),
        Some(&b"retired-only"[..]),
        "a route switch must keep a record whose group id the migration retired"
    );
    assert_eq!(
        persisted_width(&Options::new(&*path)),
        NARROW,
        "a route-only switch must not change the recorded width"
    );
    let switched_era = epoch_start(&bucket);
    assert!(
        switched_era > migrated_high_water,
        "the switched route must open a strictly newer era (got {switched_era}, high-water {migrated_high_water})"
    );
    assert!(
        !group_wal_file_ids(&log_root).is_empty(),
        "the durable route must own a shared era file"
    );
    drop(bucket);
    drop(mace);

    // switching back is another rebuild: the epoch keeps moving forward and no key is lost
    let mace = open(&path, NARROW, false);
    let bucket = mace.open_bucket("mig")?;
    assert_keys(&bucket, 12);
    assert_eq!(
        read_value(&bucket, "g3_only").as_deref(),
        Some(&b"retired-only"[..])
    );
    assert!(
        epoch_start(&bucket) > switched_era,
        "each route switch must advance the epoch (got {} after {switched_era})",
        epoch_start(&bucket)
    );
    assert!(
        all_stream_ids(&log_root)
            .iter()
            .all(|id| *id >= migrated_era),
        "no stream may fall back below the migration's era start {migrated_era}"
    );
    Ok(())
}
