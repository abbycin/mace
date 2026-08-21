#[cfg(feature = "extra_check")]
use mace::testing;
use mace::{BucketOptions, Mace, OpCode, Options, RandomPath};
use std::fs;
use std::path::Path;

fn wal_file_ids(log_root: &Path, physical: u8) -> Vec<u64> {
    let prefix = format!("wal_{physical}_");
    let mut ids = Vec::new();
    if let Ok(entries) = fs::read_dir(log_root) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with(&prefix)
                && let Ok(id) = name[prefix.len()..].parse::<u64>()
            {
                ids.push(id);
            }
        }
    }
    ids.sort_unstable();
    ids
}

fn group_wal_file_ids(log_root: &Path) -> Vec<u64> {
    let prefix = "group_wal_";
    let mut ids = Vec::new();
    if let Ok(entries) = fs::read_dir(log_root) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with(prefix)
                && let Ok(id) = name[prefix.len()..].parse::<u64>()
            {
                ids.push(id);
            }
        }
    }
    ids.sort_unstable();
    ids
}

#[test]
fn route_switch_reopen_recovers_across_epochs() -> Result<(), OpCode> {
    let path = RandomPath::tmp();

    {
        let mut opt = Options::new(&*path);
        opt.sync_on_write = false;
        opt.concurrent_write = 2;
        let mace = Mace::new(opt.validate()?)?;
        let db = mace.new_bucket("x", BucketOptions::default())?;
        for (k, v) in [("a", b"1"), ("b", b"2")] {
            let tx = db.begin()?;
            tx.put(k, v)?;
            tx.commit()?;
        }
    }

    {
        let mut opt = Options::new(&*path);
        opt.sync_on_write = true;
        opt.concurrent_write = 2;
        let mace = Mace::new(opt.validate()?)?;
        let db = mace.get_bucket("x")?;
        let view = db.view()?;
        assert_eq!(view.get("a")?.slice(), b"1");
        assert_eq!(view.get("b")?.slice(), b"2");
        drop(view);

        let tx = db.begin()?;
        tx.put("c", b"3")?;
        tx.commit()?;
    }

    {
        let mut opt = Options::new(&*path);
        opt.sync_on_write = false;
        opt.concurrent_write = 2;
        let mace = Mace::new(opt.validate()?)?;
        let db = mace.get_bucket("x")?;
        let view = db.view()?;
        assert_eq!(view.get("a")?.slice(), b"1");
        assert_eq!(view.get("b")?.slice(), b"2");
        assert_eq!(view.get("c")?.slice(), b"3");
    }
    Ok(())
}

#[test]
fn route_switch_starts_new_era_at_higher_contiguous_file_ids() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.sync_on_write = true;
    opt.concurrent_write = 2;
    let log_root = opt.log_root();
    let mace = Mace::new(opt.clone().validate()?)?;
    let db = mace.new_bucket("x", BucketOptions::default())?;
    let tx = db.begin()?;
    tx.put("k", b"v")?;
    tx.commit()?;
    drop(db);
    drop(mace);

    let first_epoch_max = group_wal_file_ids(&log_root).last().copied();
    for physical in 0..2 {
        let ids = wal_file_ids(&log_root, physical);
        if ids.len() > 1 {
            assert!(
                ids.windows(2).all(|w| w[1] == w[0] + 1),
                "physical stream {physical} must have a contiguous epoch prefix: {ids:?}"
            );
        }
    }

    let mut opt = Options::new(&*path);
    opt.sync_on_write = false;
    opt.concurrent_write = 2;
    let log_root = opt.log_root();
    let mace = Mace::new(opt.validate()?)?;
    let db = mace.get_bucket("x")?;
    let tx = db.begin()?;
    tx.put("k2", b"v2")?;
    tx.commit()?;
    drop(db);
    drop(mace);

    let second_epoch_ids = wal_file_ids(&log_root, 0);
    assert!(
        second_epoch_ids.last().copied() > first_epoch_max,
        "new route must start from a strictly higher file id than the previous epoch"
    );
    for physical in 0..2 {
        let ids = wal_file_ids(&log_root, physical);
        if ids.len() > 1 {
            assert!(
                ids.windows(2).all(|w| w[1] == w[0] + 1),
                "physical stream {physical} must keep a contiguous epoch prefix: {ids:?}"
            );
        }
    }
    Ok(())
}

#[cfg(feature = "extra_check")]
#[test]
fn route_switch_gc_keeps_recycle_boundary_at_new_epoch_start() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let mut durable = Options::new(&*path);
    durable.sync_on_write = true;
    durable.concurrent_write = 2;
    durable.wal_file_size = 4 << 10;
    let log_root = durable.log_root();
    let mace = Mace::new(durable.validate()?)?;
    let db = mace.new_bucket("x", BucketOptions::default())?;
    let payload = vec![b'x'; 1024];
    for round in 0..64 {
        let tx = db.begin()?;
        tx.put(format!("k_{round}"), &payload)?;
        tx.commit()?;
    }
    drop(db);
    drop(mace);

    let start = group_wal_file_ids(&log_root)
        .last()
        .copied()
        .expect("durable run must retain its current wal file")
        .checked_add(1)
        .expect("test wal id must not overflow");

    let mut relaxed = Options::new(&*path);
    relaxed.sync_on_write = false;
    relaxed.concurrent_write = 2;
    relaxed.wal_file_size = 4 << 10;
    let mace = Mace::new(relaxed.validate()?)?;
    let db = mace.get_bucket("x")?;
    mace.start_gc();

    assert_eq!(
        testing::wal_recycle_boundary(&db, 0),
        start,
        "an empty relaxed GC pass must not regress the switch wipe's done boundary"
    );
    Ok(())
}

#[cfg(feature = "extra_check")]
#[test]
fn relaxed_to_durable_gc_keeps_recycle_boundary_at_new_epoch_start() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let mut relaxed = Options::new(&*path);
    relaxed.sync_on_write = false;
    relaxed.concurrent_write = 2;
    relaxed.wal_file_size = 4 << 10;
    let log_root = relaxed.log_root();
    let mace = Mace::new(relaxed.validate()?)?;
    let db = mace.new_bucket("x", BucketOptions::default())?;
    let payload = vec![b'x'; 1024];
    for round in 0..64 {
        let tx = db.begin()?;
        tx.put(format!("k_{round}"), &payload)?;
        tx.commit()?;
    }
    drop(db);
    drop(mace);

    let start = (0..2)
        .flat_map(|group| wal_file_ids(&log_root, group))
        .max()
        .expect("relaxed run must retain a wal file")
        .checked_add(1)
        .expect("test wal id must not overflow");

    let mut durable = Options::new(&*path);
    durable.sync_on_write = true;
    durable.concurrent_write = 2;
    durable.wal_file_size = 4 << 10;
    let mace = Mace::new(durable.validate()?)?;
    let db = mace.get_bucket("x")?;
    mace.start_gc();

    assert_eq!(
        testing::wal_recycle_boundary(&db, Options::SHARED_ID),
        start,
        "an empty durable GC pass must not regress the switch wipe's done boundary"
    );
    Ok(())
}

#[test]
fn same_route_reopen_creates_no_files_and_continues_in_stream() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.sync_on_write = true;
    opt.concurrent_write = 2;
    let log_root = opt.log_root();
    let mace = Mace::new(opt.clone().validate()?)?;
    let db = mace.new_bucket("x", BucketOptions::default())?;
    let tx = db.begin()?;
    tx.put("k", b"v")?;
    tx.commit()?;
    drop(db);
    drop(mace);
    let before = group_wal_file_ids(&log_root);

    // same-route reopen: no epoch, no empty-file bootstrap, no wipe; the
    // stream continues in its existing latest file
    let mace = Mace::new(opt.clone().validate()?)?;
    let db = mace.get_bucket("x")?;
    let view = db.view()?;
    assert_eq!(view.get("k")?.slice(), b"v");
    drop(view);
    let tx = db.begin()?;
    tx.put("k2", b"v2")?;
    tx.commit()?;
    drop(db);
    drop(mace);
    let after = group_wal_file_ids(&log_root);
    assert_eq!(
        before, after,
        "same-route reopen must not create or delete wal files: before={before:?} after={after:?}"
    );
    Ok(())
}

#[test]
fn group_wal_gap_rejected_on_reopen() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.sync_on_write = true;
    opt.concurrent_write = 1;
    let missing = opt.group_wal_file(5);
    let tail = opt.group_wal_file(6);
    let mace = Mace::new(opt.validate()?)?;
    drop(mace);

    fs::write(&missing, b"")?;
    fs::write(&tail, b"")?;
    fs::remove_file(&missing)?;

    let mut reopen = Options::new(&*path);
    reopen.sync_on_write = true;
    reopen.concurrent_write = 1;
    let err = Mace::new(reopen.validate()?)
        .err()
        .expect("reopen with a real wal gap must fail");
    assert_eq!(err, OpCode::Corruption);
    Ok(())
}
