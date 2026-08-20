#![no_main]

//! durable crash-reopen fuzz:
//! a child process replays a scripted transaction sequence, then simulates a
//! crash at a scripted transaction by _exit()ing without running destructors
//! (so pending WAL ring buffers are never flushed and no graceful shutdown
//! checkpoint runs). the parent reopens the same directory and asserts the
//! crash-consistency invariants:
//!   1. every transaction whose commit() returned is fully visible with the
//!      returned value (sync_on_write = true durability promise)
//!   2. nothing is visible that was never committed (no phantom keys): the
//!      visible set after reopen must be exactly the commit-returned model.
//!      a crash transaction never calls commit() in this target (windows 0/1/3
//!      die before the commit record exists, window 2 commits fully), so any
//!      in-flight write surfacing after reopen is an engine regression and
//!      must fail the target -- there is no tolerated override state.
//! four crash windows are scripted per transaction tag: right after begin
//! (nothing durable), after upsert before commit (no commit record exists),
//! after a full commit (must be durable), and after an abort (must stay
//! invisible). background checkpoint/gc threads keep running in the child, so
//! the crash lands on an arbitrary checkpoint window as well.
//! the child is spawned as the current executable with `-runs=1`; the fuzz
//! input is streamed over stdin and commit-returned transaction indices are
//! streamed back over stdout (`C\t<i>` lines); the parent replays the input to
//! rebuild the expected key/value model.

#[allow(dead_code)]
mod common;

use common::{FuzzDbRoot, get_or_create_bucket, key_name, open_engine, value_bytes};
use libfuzzer_sys::fuzz_target;
use mace::BucketOptions;
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::{Command, ExitStatus, Stdio};

const ENV_CHILD: &str = "MACE_FUZZ_CRASH_REOPEN_CHILD";
const ENV_DB_ROOT: &str = "MACE_FUZZ_CRASH_REOPEN_DB_ROOT";
const CRASH_CODE: i32 = 42;
const MAX_TXNS: usize = 128;

fn tune(opt: &mut mace::Options) {
    opt.concurrent_write = 1;
    opt.sync_on_write = true; // durable: committed txns must survive a crash
    opt.data_file_size = 32 << 10;
    opt.wal_file_size = 16 << 10;
    opt.max_ckpt_per_txn = 64;
}

// input layout: data[0..4] = crash transaction index (LE, mod 1024);
// data[4 + i] = tag for transaction i. every transaction touches exactly one
// key from a closed 8-key universe, so the parent can enumerate all visible
// keys after reopen.
fn txn_at(data: &[u8], index: usize) -> (String, Vec<u8>, bool) {
    let tag = data[4 + index];
    let key = key_name(tag as usize % 8);
    let value = value_bytes(tag, tag as usize);
    let abort = tag as usize % 32 == 15;
    (key, value, abort)
}

fn child_transaction_count(data: &[u8]) -> usize {
    if data.len() < 5 {
        return 0;
    }
    (data.len() - 4).min(MAX_TXNS)
}

fn crashat(data: &[u8]) -> usize {
    if data.len() < 5 {
        return usize::MAX;
    }
    u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize % 1024
}

fn report(line: String) {
    let mut out = std::io::stdout().lock();
    let _ = out.write_all(line.as_bytes());
    let _ = out.write_all(b"\n");
    let _ = out.flush();
}

fn child_die(code: i32) -> ! {
    // _exit bypasses atexit hooks and async-signal-unsafe cleanup; none of the
    // engine's pending buffers are flushed and no leak/destructor machinery
    // runs, which is exactly the crash the target wants to model.
    unsafe { libc::_exit(code) }
}

fn child_main() -> ! {
    // any engine fault (expect panic) must die like a crash, not unwind: an
    // unwind would run destructors (durable abort, final checkpoint) and turn
    // an engine failure into a consistent database the parent would pass.
    // the hook reports the fault over the stdout protocol and exits with a
    // code distinct from the scripted crash so the parent can diagnose.
    std::panic::set_hook(Box::new(|info| {
        let mut out = std::io::stdout().lock();
        let _ = writeln!(out, "E\t{}", info);
        let _ = out.flush();
        unsafe { libc::_exit(3) }
    }));

    let db_root = PathBuf::from(std::env::var(ENV_DB_ROOT).expect("missing child db root"));
    let mut data = Vec::new();
    std::io::stdin()
        .read_to_end(&mut data)
        .expect("child stdin read failed");
    let n = child_transaction_count(&data);
    let crash_at = crashat(&data);

    let mut opt = mace::Options::new(&db_root);
    tune(&mut opt);
    let mace = mace::Mace::new(opt.validate().expect("options must validate"))
        .expect("child engine open failed");
    let bucket = get_or_create_bucket(&mace, "main", BucketOptions::default())
        .expect("child bucket open failed");

    for i in 0..n {
        let (key, value, abort) = txn_at(&data, i);
        if i == crash_at {
            let mode = data[4 + i] % 4;
            match mode {
                // crash right after begin: only the begin record exists, the
                // transaction can never become visible
                0 => {
                    let txn = bucket.begin().expect("begin failed");
                    drop(txn);
                    child_die(CRASH_CODE);
                }
                // crash after upsert, before commit: the record may have hit
                // the file (abort-clean must erase it) or still sit in the
                // ring buffer
                1 => {
                    let txn = bucket.begin().expect("begin failed");
                    txn.upsert(&key, &value).expect("upsert failed");
                    child_die(CRASH_CODE);
                }
                // crash after a full commit: this transaction must be durable
                2 => {
                    let txn = bucket.begin().expect("begin failed");
                    txn.upsert(&key, &value).expect("upsert failed");
                    txn.commit().expect("commit failed");
                    report(format!("C\t{i}"));
                    child_die(CRASH_CODE);
                }
                // crash after an abort: an aborted transaction must stay
                // invisible
                _ => {
                    let txn = bucket.begin().expect("begin failed");
                    txn.upsert(&key, &value).expect("upsert failed");
                    drop(txn);
                    child_die(CRASH_CODE);
                }
            }
        }
        let txn = bucket.begin().expect("begin failed");
        txn.upsert(&key, &value).expect("upsert failed");
        if abort {
            drop(txn);
        } else {
            txn.commit().expect("commit failed");
            report(format!("C\t{i}"));
        }
    }
    child_die(0);
}

fn parse_reports(out: &str) -> BTreeSet<usize> {
    let mut committed = BTreeSet::new();
    for line in out.lines() {
        let mut it = line.split('\t');
        let tag = it.next().unwrap_or("");
        let idx = it.next().and_then(|s| s.parse::<usize>().ok());
        if let ("C", Some(i)) = (tag, idx) {
            committed.insert(i);
        }
    }
    committed
}

fn verify(
    db_root: &FuzzDbRoot,
    data: &[u8],
    status: ExitStatus,
    committed: &BTreeSet<usize>,
) {
    let n = child_transaction_count(data);
    // the child must die either naturally (0) or via the scripted crash
    // (CRASH_CODE); any other exit is an engine fault that the panic hook
    // already reported over stdout before _exit(3)
    assert!(
        matches!(status.code(), Some(0) | Some(CRASH_CODE)),
        "unexpected child exit {status}"
    );

    // committed model: last committed value per key
    let mut model = BTreeMap::<String, Vec<u8>>::new();
    for i in 0..n {
        if committed.contains(&i) {
            let (key, value, _) = txn_at(data, i);
            model.insert(key, value);
        }
    }

    let mace = open_engine(db_root.path(), tune);
    let bucket = get_or_create_bucket(&mace, "main", BucketOptions::default())
        .expect("reopen main bucket failed");
    let view = bucket.view().expect("reopen view failed");

    // 1. every commit-returned transaction must be visible with its value
    for (key, expected) in &model {
        let actual = view.get(key).expect("committed key missing after crash");
        assert_eq!(
            actual.slice(),
            expected.as_slice(),
            "key {key}: committed value lost after crash"
        );
    }

    // 2. no phantom keys: every visible key must be part of the committed
    // model. a crash transaction in this target never produces a commit
    // record (windows 0/1/3 die before commit, window 2 commits fully), so
    // any in-flight write surfacing after reopen is an engine regression
    for idx in 0..8usize {
        let key = key_name(idx);
        if view.get(&key).is_ok() {
            assert!(
                model.contains_key(&key),
                "phantom key {key} visible after crash"
            );
        }
    }
}

fuzz_target!(|data: &[u8]| {
    if std::env::var(ENV_CHILD).ok().as_deref() == Some("1") {
        // child mode: execute the scripted sequence against a fresh engine
        // and crash at the scripted transaction; fuzz_target! only runs once
        // under `-runs=1`, so this never returns
        child_main();
    }
    if data.len() < 5 {
        return;
    }

    let db_root = FuzzDbRoot::new();
    let exe = std::env::current_exe().expect("current exe unavailable");
    let mut child = Command::new(exe)
        .arg("-runs=1")
        .env(ENV_CHILD, "1")
        .env(ENV_DB_ROOT, db_root.path().as_path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn crash child failed");

    // stream the input; EOF on stdin makes the child proceed
    {
        let mut stdin = child.stdin.take().expect("child stdin");
        stdin
            .write_all(data)
            .expect("write child input failed");
    }
    // stdin dropped: EOF, child proceeds

    let mut reports = String::new();
    child
        .stdout
        .take()
        .expect("child stdout")
        .read_to_string(&mut reports)
        .expect("read child reports failed");
    let status = child.wait().expect("wait child failed");

    let committed = parse_reports(&reports);
    if !matches!(status.code(), Some(0) | Some(CRASH_CODE)) {
        // surface the child's panic report (E lines) before failing
        panic!(
            "crash_reopen child exited abnormally: {status}\nchild reports:\n{reports}"
        );
    }
    verify(&db_root, data, status, &committed);
});