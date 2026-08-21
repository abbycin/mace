//! exit-flush experiment: after a graceful drop, all committed data must live
//! in the data files alone -- deleting every WAL file of both namespaces
//! (per-group `wal_<group>_<seq>` and shared `group_wal_<seq>`) and reopening
//! must still expose the full committed set. this proves the exit path flushed
//! all dirty pages into the data files (and exposed them through the
//! manifest).

use mace::{BucketOptions, Mace, Options};
use std::path::Path;

fn open(path: &Path, sync_on_write: bool) -> Mace {
    let mut opt = Options::new(path);
    opt.sync_on_write = sync_on_write;
    opt.concurrent_write = 1;
    opt.data_file_size = 32 << 10;
    opt.wal_file_size = 16 << 10;
    opt.max_ckpt_per_txn = 64;
    Mace::new(opt.validate().expect("options must validate")).expect("open failed")
}

fn seed(mace: &Mace, count: usize) {
    let bucket = mace
        .new_bucket("main", BucketOptions::default())
        .expect("new bucket");
    for i in 0..count {
        let txn = bucket.begin().expect("begin");
        txn.put(format!("k_{i:04}"), format!("v_{i:04}"))
            .expect("put");
        txn.commit().expect("commit");
    }
}

/// remove every wal file in both namespaces: legacy per-group `wal_<group>_<seq>`
/// and the durable shared `group_wal_<seq>` stream. a WAL-independent exit test
/// must prove the committed model survives with NO wal file of either namespace
fn remove_wal_files(dir: &Path) {
    // wal files live under the log root; with default options that is
    // <db_root>/log (Options::log_root), not the db root itself
    let log_root = dir.join("log");
    let log_dir = if log_root.is_dir() {
        log_root.as_path()
    } else {
        dir
    };
    for entry in std::fs::read_dir(log_dir).expect("read log dir") {
        let entry = entry.expect("entry");
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with("wal_") || name.starts_with("group_wal_") {
            std::fs::remove_file(entry.path()).expect("remove wal");
        }
    }
    // both namespaces must be empty now; otherwise the reopen below could
    // recover from a wal file this helper missed and the test would prove
    // nothing about the data files
    let leftover: Vec<String> = std::fs::read_dir(log_dir)
        .expect("read log dir")
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("wal_") || n.starts_with("group_wal_"))
        .collect();
    assert!(
        leftover.is_empty(),
        "WAL removal must empty both namespaces, leftover: {leftover:?}"
    );
}

fn assert_full(mace: &Mace, count: usize) {
    let bucket = mace.get_bucket("main").expect("bucket");
    let view = bucket.view().expect("view");
    for i in 0..count {
        let val = view
            .get(format!("k_{i:04}"))
            .unwrap_or_else(|_| panic!("key k_{i:04} missing after reopen"));
        assert_eq!(val.slice(), format!("v_{i:04}").as_bytes());
    }
}

#[test]
fn durable_exit_leaves_all_committed_in_data_files() {
    let path = mace::RandomPath::tmp();
    {
        let mace = open(&path, true);
        seed(&mace, 200);
        // graceful exit
    }
    remove_wal_files(&path);
    let mace = open(&path, true);
    assert_full(&mace, 200);
}

#[test]
fn relaxed_exit_leaves_all_committed_in_data_files() {
    let path = mace::RandomPath::tmp();
    {
        let mace = open(&path, false);
        seed(&mace, 200);
        // graceful exit
    }
    remove_wal_files(&path);
    let mace = open(&path, false);
    assert_full(&mace, 200);
}
#[test]
fn exit_flush_aborted_txn_stays_invisible_without_wal() {
    // graceful exit then delete-every-WAL (both namespaces) reopen: a
    // transaction that aborted
    // right before exit must not become
    // visible just because its abort-clean work was still pending in the GC
    // queue when the engine closed.
    let path = mace::RandomPath::tmp();
    {
        let mace = open(&path, true);
        let bucket = mace
            .new_bucket("main", BucketOptions::default())
            .expect("new bucket");
        let t = bucket.begin().expect("begin");
        t.put("committed", "v").expect("put");
        t.commit().expect("commit");
        // modified abort: version written, abort-clean task enqueued, GC
        // thread has not run it yet (immediate exit)
        let t = bucket.begin().expect("begin");
        t.put("aborted", "ghost").expect("put");
        drop(t);
        // many later committed txns raise the oracle far above the aborted
        // txid, so the fast positive-proof path must decide its visibility
        for i in 0..200 {
            let t = bucket.begin().expect("begin");
            t.put(format!("f_{i:03}"), format!("v_{i:03}"))
                .expect("put");
            t.commit().expect("commit");
        }
        // graceful exit
    }
    // the exit abort-clean drain must have removed the aborted version
    // physically: no "ghost" bytes may remain in any data file. this scan is
    // deterministic because no runtime checkpoint can fire during the 200-txn
    // loop (default checkpoint_size/pool thresholds are far above this test's
    // dirty set, checkpoint_nudge_ms is 60s, and the run is <1s), so every
    // data file present was written after the exit drain ran
    for entry in std::fs::read_dir(&*path.join("data")).expect("read data dir") {
        let p = entry.expect("entry").path();
        let name = p.file_name().unwrap().to_string_lossy().into_owned();
        if name.starts_with("data_") {
            let bytes = std::fs::read(&p).expect("read data file");
            assert!(
                !bytes.windows(5).any(|w| w == b"ghost"),
                "{name} still holds aborted bytes after exit drain"
            );
        }
    }
    remove_wal_files(&path);
    let mace = open(&path, true);
    let bucket = mace.get_bucket("main").expect("bucket");
    let view = bucket.view().expect("view");
    assert_eq!(view.get("committed").expect("committed").slice(), b"v");
    for i in 0..200 {
        assert_eq!(
            view.get(format!("f_{i:03}")).expect("f").slice(),
            format!("v_{i:03}").as_bytes()
        );
    }
    let res = view.get("aborted");
    assert!(
        matches!(res, Err(mace::OpCode::NotFound)),
        "aborted key leaked after WAL removal"
    );
}

#[test]
fn exit_flush_aborted_override_of_committed_stays_at_committed_value() {
    // delete-every-WAL (both namespaces) reopen must not lose a committed
    // value that an aborted
    // transaction overwrote: after graceful exit + WAL removal, the key must
    // still resolve to the last committed value, with the aborted version
    // invisible.
    let path = mace::RandomPath::tmp();
    {
        let mace = open(&path, true);
        let bucket = mace
            .new_bucket("main", BucketOptions::default())
            .expect("new bucket");
        let t = bucket.begin().expect("begin");
        t.put("k", "committed_v").expect("put");
        t.commit().expect("commit");
        let t = bucket.begin().expect("begin");
        t.update("k", "aborted_v").expect("update");
        drop(t); // abort: version chain now [committed, aborted]
        for i in 0..200 {
            let t = bucket.begin().expect("begin");
            t.put(format!("f_{i:03}"), format!("v_{i:03}"))
                .expect("put");
            t.commit().expect("commit");
        }
        // graceful exit
    }
    remove_wal_files(&path);
    let mace = open(&path, true);
    let bucket = mace.get_bucket("main").expect("bucket");
    let view = bucket.view().expect("view");
    assert_eq!(
        view.get("k").expect("k must resolve").slice(),
        b"committed_v",
        "committed value lost under aborted override after WAL removal"
    );
    for i in 0..200 {
        assert_eq!(
            view.get(format!("f_{i:03}")).expect("f").slice(),
            format!("v_{i:03}").as_bytes()
        );
    }
}
