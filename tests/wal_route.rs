#![cfg(feature = "extra_check")]

use mace::observe::{CounterMetric, InMemoryObserver};
use mace::testing::{self, WalRecordKind, WalUpdateProbe};
use mace::{BucketOptions, Mace, OpCode, Options, RandomPath};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn durable_route_sends_all_logical_groups_through_group_wal() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.sync_on_write = true;
    opt.concurrent_write = 2;
    let log_root = opt.log_root();
    let parsed = opt.validate()?;
    let mace = Mace::new(parsed)?;
    let db = mace.new_bucket("x", BucketOptions::default())?;

    let tx1 = db.begin()?;
    let txid1 = testing::txn_start_ts(&tx1);
    assert_eq!(testing::txn_group(&db, txid1), Some(0));
    tx1.put("a", b"1")?;
    tx1.commit()?;

    let tx2 = db.begin()?;
    let txid2 = testing::txn_start_ts(&tx2);
    assert_eq!(testing::txn_group(&db, txid2), Some(1));
    tx2.put("b", b"2")?;
    tx2.commit()?;

    let tx3 = db.begin()?;
    let txid3 = testing::txn_start_ts(&tx3);
    assert_eq!(testing::txn_group(&db, txid3), Some(0));
    tx3.put("c", b"3")?;
    drop(tx3);

    drop(db);
    drop(mace);

    let probes: Vec<WalUpdateProbe> = testing::wal_update_probes(&log_root, 2);
    assert!(
        !probes.is_empty(),
        "durable route must write wal update records"
    );
    assert!(
        probes
            .iter()
            .all(|p| p.physical_wal_id == Options::SHARED_ID),
        "durable route must write every logical group into the shared group_wal stream"
    );
    assert!(
        probes.iter().any(|p| p.logical_group_id == 0),
        "group_wal must contain logical group 0 updates"
    );
    assert!(
        probes.iter().any(|p| p.logical_group_id == 1),
        "group_wal must contain logical group 1 updates"
    );
    let records = testing::wal_record_probes(&log_root, 2);
    assert!(
        records
            .iter()
            .all(|r| r.physical_wal_id == Options::SHARED_ID),
        "durable route must write every txn record into group_wal"
    );
    for kind in [
        WalRecordKind::Begin,
        WalRecordKind::Update,
        WalRecordKind::Commit,
        WalRecordKind::Abort,
    ] {
        assert!(
            records.iter().any(|r| r.kind == kind),
            "durable route must write {kind:?} records into group_wal"
        );
    }
    // every record of a captured txn must carry that txn's logical group, so
    // a record-level group-label swap cannot pass unnoticed
    for (txid, expect_group) in [(txid1, 0u8), (txid2, 1u8), (txid3, 0u8)] {
        let group_probes: Vec<u8> = records
            .iter()
            .filter(|r| r.txid == txid)
            .filter_map(|r| r.logical_group_id)
            .collect();
        assert!(
            !group_probes.is_empty(),
            "txn {txid} must have wal records in the durable route"
        );
        assert!(
            group_probes.iter().all(|&g| g == expect_group),
            "txn {txid} must be tagged with logical group {expect_group}, got {group_probes:?}"
        );
    }
    Ok(())
}

#[test]
fn relaxed_route_keeps_per_group_wal_streams() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.sync_on_write = false;
    opt.concurrent_write = 2;
    let log_root = opt.log_root();
    let mace = Mace::new(opt.validate()?)?;
    let db = mace.new_bucket("x", BucketOptions::default())?;

    let tx1 = db.begin()?;
    let txid1 = testing::txn_start_ts(&tx1);
    assert_eq!(testing::txn_group(&db, txid1), Some(0));
    tx1.put("a", b"1")?;
    tx1.commit()?;

    let tx2 = db.begin()?;
    let txid2 = testing::txn_start_ts(&tx2);
    assert_eq!(testing::txn_group(&db, txid2), Some(1));
    tx2.put("b", b"2")?;
    tx2.commit()?;

    drop(db);
    drop(mace);

    let probes = testing::wal_update_probes(&log_root, 2);
    assert!(
        !probes.is_empty(),
        "relaxed route must write wal update records"
    );
    assert!(
        probes
            .iter()
            .all(|p| p.physical_wal_id == p.logical_group_id),
        "relaxed route must keep wal_id == logical group"
    );
    assert!(probes.iter().any(|p| p.logical_group_id == 0));
    assert!(probes.iter().any(|p| p.logical_group_id == 1));
    Ok(())
}

#[test]
fn durable_large_record_commit_performs_single_wal_sync() -> Result<(), OpCode> {
    let observer = Arc::new(InMemoryObserver::new(2));
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.sync_on_write = true;
    opt.concurrent_write = 1;
    opt.wal_buffer_size = 8 << 10;
    opt.wal_file_size = 4 << 10;
    opt.observer = observer.clone();
    let mace = Mace::new(opt.validate()?)?;
    let db = mace.new_bucket("x", BucketOptions::default())?;

    let payload = vec![b'x'; 64 << 10];
    let tx = db.begin()?;
    tx.put("big", &payload)?;
    tx.commit()?;

    let syncs = observer
        .snapshot()
        .counters
        .iter()
        .find(|(metric, _)| *metric == CounterMetric::WalSync)
        .map(|(_, value)| *value)
        .unwrap_or(0);
    assert_eq!(
        syncs, 1,
        "large-record durable commit must not fsync before the commit barrier"
    );
    Ok(())
}

#[test]
fn durable_large_record_across_rotation_survives_reopen() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.sync_on_write = true;
    opt.concurrent_write = 1;
    opt.wal_buffer_size = 8 << 10;
    opt.wal_file_size = 4 << 10;
    let mace = Mace::new(opt.validate()?)?;
    let db = mace.new_bucket("x", BucketOptions::default())?;

    let payload = vec![b'x'; 64 << 10];
    let tx = db.begin()?;
    tx.put("big", &payload)?;
    tx.commit()?;
    drop(db);
    drop(mace);

    let mut reopen = Options::new(&*path);
    reopen.concurrent_write = 1;
    reopen.sync_on_write = true;
    reopen.wal_buffer_size = 8 << 10;
    reopen.wal_file_size = 4 << 10;
    let mace = Mace::new(reopen.validate()?)?;
    let db = mace.get_bucket("x").expect("bucket must reopen");
    let view = db.view()?;
    let got = view.get("big")?;
    assert_eq!(got.slice(), payload.as_slice());
    Ok(())
}

#[test]
fn durable_mode_shares_one_logging_with_per_group_checkpoint_floors() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.sync_on_write = true;
    opt.concurrent_write = 2;
    let mace = Mace::new(opt.validate()?)?;
    let db = mace.new_bucket("x", BucketOptions::default())?;

    assert!(
        testing::loggings_shared_across_groups(&db),
        "durable mode must share one logging Arc across writer groups"
    );

    // commit to both logical groups
    let tx1 = db.begin()?;
    let txid1 = testing::txn_start_ts(&tx1);
    assert_eq!(testing::txn_group(&db, txid1), Some(0));
    tx1.put("a", b"1")?;
    tx1.commit()?;

    let tx2 = db.begin()?;
    let txid2 = testing::txn_start_ts(&tx2);
    assert_eq!(testing::txn_group(&db, txid2), Some(1));
    tx2.put("b", b"2")?;
    tx2.commit()?;

    db.checkpoint();
    // the floor slots advance with the checkpoint publish rounds (the publish
    // samples the bucket frontier only while flush data is pending); drive a
    // few rounds so both logical groups' floors separate
    for _ in 0..8 {
        db.checkpoint();
        mace.start_gc();
        std::thread::sleep(Duration::from_millis(10));
    }
    // the shared logger must hold one floor slot per logical group, not a
    // single scalar shared by every group
    let floor0 = testing::shared_checkpoint_floor(&db, 0);
    let floor1 = testing::shared_checkpoint_floor(&db, 1);
    assert_ne!(floor0, floor1, "per-group checkpoint floors must differ");
    Ok(())
}

#[test]
fn relaxed_mode_keeps_per_group_loggings() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.sync_on_write = false;
    opt.concurrent_write = 2;
    let mace = Mace::new(opt.validate()?)?;
    let db = mace.new_bucket("x", BucketOptions::default())?;
    assert!(
        !testing::loggings_shared_across_groups(&db),
        "relaxed mode must keep per-group loggings"
    );
    Ok(())
}
