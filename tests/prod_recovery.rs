mod common;

use common::{TestEnv, child_test_command, env_usize};
use mace::{Mace, OpCode, Options, RandomPath};
use std::path::Path;
use std::process::ExitStatus;

const MODE_FLAG: &str = "MACE_PROD_RECOVERY_MODE";
const DB_ROOT_FLAG: &str = "MACE_PROD_RECOVERY_DB_ROOT";

fn spawn_child(
    current_exe: &Path,
    mode: &str,
    db_root: &str,
    failpoint: Option<&str>,
) -> ExitStatus {
    let mut cmd = child_test_command(current_exe);
    cmd.arg("--exact")
        .arg("fast_process_crash_window")
        .arg("--nocapture")
        .env(MODE_FLAG, mode)
        .env(DB_ROOT_FLAG, db_root);

    if let Some(value) = failpoint {
        cmd.env("MACE_FAILPOINT", value);
    }

    cmd.status().expect("spawn child failed")
}

fn run_crash_verify_once(current_exe: &Path, db_root: &str) {
    let crash_status = spawn_child(current_exe, "crash", db_root, None);
    assert!(
        !crash_status.success(),
        "crash child should exit abnormally"
    );

    let verify_status = spawn_child(current_exe, "verify", db_root, None);
    assert!(verify_status.success(), "verify child should pass");
}

fn child_crash_path() -> ! {
    let db_root = std::env::var(DB_ROOT_FLAG).expect("missing child db root");
    let engine = Mace::new(Options::new(&db_root).validate().expect("bad options"))
        .expect("open engine failed");

    let bucket = match engine.get_bucket("prod_recovery") {
        Ok(bucket) => bucket,
        Err(OpCode::NotFound) => engine
            .new_bucket("prod_recovery")
            .expect("create bucket failed"),
        Err(err) => panic!("open bucket failed: {err:?}"),
    };

    let committed = bucket.begin().expect("begin committed txn failed");
    committed
        .put("committed", "ok")
        .expect("put committed failed");
    committed.commit().expect("commit failed");

    let pending = bucket.begin().expect("begin pending txn failed");
    pending
        .put("uncommitted", "lost")
        .expect("put pending failed");

    std::process::abort();
}

fn child_verify_path() {
    let db_root = std::env::var(DB_ROOT_FLAG).expect("missing verify db root");
    let engine = Mace::new(
        Options::new(&db_root)
            .validate()
            .expect("bad verify options"),
    )
    .expect("reopen engine failed");

    let bucket = engine
        .get_bucket("prod_recovery")
        .expect("bucket should exist");
    let view = bucket.view().expect("open view failed");

    assert_eq!(
        view.get("committed").expect("missing committed").slice(),
        b"ok"
    );
    assert!(view.get("uncommitted").is_err());
}

#[cfg(feature = "failpoints")]
fn child_failpoint_io_write_path() {
    let db_root = std::env::var(DB_ROOT_FLAG).expect("missing io db root");
    let engine = Mace::new(Options::new(&db_root).validate().expect("bad io options"))
        .expect("open io engine failed");
    let bucket = engine
        .new_bucket("prod_failpoint_io")
        .expect("create io bucket failed");

    let txn = bucket.begin().expect("begin io txn failed");
    txn.put("k", "v").expect("io put failed");
    assert_eq!(txn.commit(), Err(OpCode::IoError));

    let view = bucket.view().expect("open io view failed");
    assert!(view.get("k").is_err());
}

#[cfg(feature = "failpoints")]
fn child_failpoint_io_verify_path() {
    let db_root = std::env::var(DB_ROOT_FLAG).expect("missing io verify db root");
    let engine = Mace::new(
        Options::new(&db_root)
            .validate()
            .expect("bad io verify options"),
    )
    .expect("open io verify engine failed");
    let bucket = engine
        .get_bucket("prod_failpoint_io")
        .expect("io bucket should exist");
    let view = bucket.view().expect("open io verify view failed");
    assert!(view.get("k").is_err());
}

#[cfg(feature = "failpoints")]
fn child_failpoint_abort_write_path() -> ! {
    let db_root = std::env::var(DB_ROOT_FLAG).expect("missing abort db root");
    let engine = Mace::new(
        Options::new(&db_root)
            .validate()
            .expect("bad abort options"),
    )
    .expect("open abort engine failed");
    let bucket = engine
        .new_bucket("prod_failpoint_abort")
        .expect("create abort bucket failed");

    let txn = bucket.begin().expect("begin abort txn failed");
    txn.put("k", "v").expect("abort put failed");
    let _ = txn.commit();
    panic!("abort failpoint did not fire")
}

#[cfg(feature = "failpoints")]
fn child_failpoint_abort_verify_path() {
    let db_root = std::env::var(DB_ROOT_FLAG).expect("missing abort verify db root");
    let engine = Mace::new(
        Options::new(&db_root)
            .validate()
            .expect("bad abort verify options"),
    )
    .expect("open abort verify engine failed");

    match engine.get_bucket("prod_failpoint_abort") {
        Ok(bucket) => {
            let view = bucket.view().expect("open abort verify view failed");
            assert!(view.get("k").is_err());
        }
        Err(OpCode::NotFound) => {}
        Err(err) => panic!("open abort bucket failed: {err:?}"),
    }
}

#[test]
fn fast_reopen_visibility() -> Result<(), OpCode> {
    let env = TestEnv::new();

    {
        let engine = env.open_default()?;
        let bucket = engine.new_bucket("prod_reopen")?;

        let committed = bucket.begin()?;
        committed.put("k1", "v1")?;
        committed.commit()?;

        let pending = bucket.begin()?;
        pending.put("k2", "v2")?;
    }

    {
        let engine = env.open_default()?;
        let bucket = engine.get_bucket("prod_reopen")?;
        let view = bucket.view()?;

        assert_eq!(view.get("k1")?.slice(), b"v1");
        assert!(view.get("k2").is_err());
    }

    Ok(())
}

#[test]
fn fast_process_crash_window() {
    match std::env::var(MODE_FLAG).ok().as_deref() {
        Some("crash") => child_crash_path(),
        Some("verify") => {
            child_verify_path();
            return;
        }
        #[cfg(feature = "failpoints")]
        Some("failpoint_io_write") => {
            child_failpoint_io_write_path();
            return;
        }
        #[cfg(feature = "failpoints")]
        Some("failpoint_io_verify") => {
            child_failpoint_io_verify_path();
            return;
        }
        #[cfg(feature = "failpoints")]
        Some("failpoint_abort_write") => child_failpoint_abort_write_path(),
        #[cfg(feature = "failpoints")]
        Some("failpoint_abort_verify") => {
            child_failpoint_abort_verify_path();
            return;
        }
        _ => {}
    }

    let db_root = RandomPath::new();
    let db_root_text = db_root.to_string_lossy().to_string();
    let current_exe = std::env::current_exe().expect("load current exe failed");

    run_crash_verify_once(&current_exe, &db_root_text);

    std::fs::remove_dir_all(&db_root_text).expect("cleanup db root failed");
}

#[test]
#[ignore]
fn stress_crash_reopen_loop() {
    let rounds = env_usize("MACE_PROD_RECOVERY_STRESS_ROUNDS", 64);
    let current_exe = std::env::current_exe().expect("load current exe failed");

    for round in 0..rounds {
        let db_root = RandomPath::new();
        let db_root_text = db_root.to_string_lossy().to_string();

        run_crash_verify_once(&current_exe, &db_root_text);
        std::fs::remove_dir_all(&db_root_text).expect("cleanup stress db root failed");

        if round % 16 == 0 {
            std::thread::yield_now();
        }
    }
}

#[cfg(feature = "failpoints")]
#[test]
#[ignore]
fn chaos_failpoint_txn_commit_io() {
    let db_root = RandomPath::new();
    let db_root_text = db_root.to_string_lossy().to_string();
    let current_exe = std::env::current_exe().expect("load current exe failed");

    let write_status = spawn_child(
        &current_exe,
        "failpoint_io_write",
        &db_root_text,
        Some("mace_txn_commit_begin=io@1"),
    );
    assert!(write_status.success(), "io child should handle io error");

    let verify_status = spawn_child(&current_exe, "failpoint_io_verify", &db_root_text, None);
    assert!(verify_status.success(), "io verify child should pass");

    std::fs::remove_dir_all(&db_root_text).expect("cleanup io db root failed");
}

#[cfg(feature = "failpoints")]
#[test]
#[ignore]
fn chaos_failpoint_txn_commit_abort() {
    let db_root = RandomPath::new();
    let db_root_text = db_root.to_string_lossy().to_string();
    let current_exe = std::env::current_exe().expect("load current exe failed");

    let abort_status = spawn_child(
        &current_exe,
        "failpoint_abort_write",
        &db_root_text,
        Some("mace_txn_commit_begin=abort@1"),
    );
    assert!(
        !abort_status.success(),
        "abort child should exit abnormally"
    );

    let verify_status = spawn_child(&current_exe, "failpoint_abort_verify", &db_root_text, None);
    assert!(verify_status.success(), "abort verify child should pass");

    std::fs::remove_dir_all(&db_root_text).expect("cleanup abort db root failed");
}
