//! `mace-kv check` CLI acceptance tests.
//!
//! The CLI is the only user-visible surface of the checker, so these tests pin the
//! contract it documents: stable categories plus location on stdout, stable exit
//! codes, and a read-only run that never creates or repairs anything.

mod common;

use common::child_test_command;
use mace::{Mace, Options, RandomPath};
#[cfg(feature = "extra_check")]
use std::collections::BTreeMap;
use std::path::Path;
use std::process::Output;

fn cli() -> std::process::Command {
    child_test_command(Path::new(env!("CARGO_BIN_EXE_mace-kv")))
}

fn run(args: &[&str]) -> Output {
    cli().args(args).output().expect("spawn the mace-kv binary")
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[cfg(feature = "extra_check")]
fn fingerprint(root: &Path) -> BTreeMap<String, Vec<u8>> {
    fn walk(root: &Path, dir: &Path, out: &mut BTreeMap<String, Vec<u8>>) {
        for entry in std::fs::read_dir(dir).unwrap().flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(root, &path, out);
            } else {
                out.insert(
                    path.strip_prefix(root)
                        .unwrap()
                        .to_string_lossy()
                        .into_owned(),
                    std::fs::read(path).unwrap(),
                );
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(root, root, &mut out);
    out
}

/// a store with committed data, a published checkpoint and a flushed wal
fn seed(root: &Path) {
    let mut opt = Options::new(root);
    opt.sync_on_write = true;
    let mace = Mace::new(opt.validate().expect("validate")).expect("open store");
    let db = mace.new_bucket("x", Default::default()).expect("bucket");
    for index in 0..16u32 {
        let tx = db.begin().expect("txn");
        tx.put(format!("k_{index:04}"), vec![b'v'; 128])
            .expect("put");
        tx.commit().expect("commit");
    }
    db.checkpoint_and_wait();
}

/// a healthy directory is reported as clean with exit code 0
#[test]
fn a_healthy_directory_is_reported_clean() {
    let root = RandomPath::tmp();
    std::fs::create_dir_all(&*root).expect("scratch dir");
    seed(&root);

    let output = run(&["check", &root.to_string_lossy()]);
    assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
    let text = stdout(&output);
    assert!(text.contains(": ok"), "unexpected report: {text}");
}

/// a durable checkpoint copy is a standalone database: the CLI reports it clean
#[test]
fn a_checkpoint_copy_is_reported_clean_by_the_cli() {
    let scratch = RandomPath::tmp();
    std::fs::create_dir_all(&*scratch).expect("scratch dir");
    let root = scratch.join("db");
    let mut opt = Options::new(&root);
    opt.sync_on_write = true;
    let mace = Mace::new(opt.validate().expect("validate")).expect("open store");
    let db = mace.new_bucket("x", Default::default()).expect("bucket");
    for index in 0..16u32 {
        let tx = db.begin().expect("txn");
        tx.put(format!("k_{index:04}"), vec![b'v'; 128])
            .expect("put");
        tx.commit().expect("commit");
    }
    db.checkpoint_and_wait();
    // the copy must be a directory tree that never existed, so the checkpoint can build it
    let backup = scratch.join("backup");
    mace.create_checkpoint(&backup).expect("checkpoint copy");
    // release the source handle before spawning the CLI
    drop(db);
    drop(mace);

    let backup = backup.to_string_lossy().into_owned();
    let output = run(&["check", &backup]);
    assert_eq!(
        output.status.code(),
        Some(0),
        "a checkpoint copy must check clean, stdout: {} stderr: {}",
        stdout(&output),
        stderr(&output)
    );
    assert!(
        stdout(&output).contains(": ok"),
        "unexpected report: {}",
        stdout(&output)
    );
}

/// damage is reported with its category and location, and the exit code is non-zero
#[test]
fn damage_is_reported_with_a_stable_category() {
    let root = RandomPath::tmp();
    std::fs::create_dir_all(&*root).expect("scratch dir");
    seed(&root);
    // remove a payload file the metadata still references
    let data = std::fs::read_dir(Options::new(&*root).data_root())
        .expect("data dir")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .collect::<Vec<_>>();
    assert!(!data.is_empty(), "fixture must write a payload file");
    std::fs::remove_file(&data[0]).expect("remove payload file");

    let output = run(&["check", &root.to_string_lossy()]);
    assert_eq!(output.status.code(), Some(1), "{}", stderr(&output));
    let text = stdout(&output);
    assert!(
        text.contains("bad_payload_file:"),
        "damage must be reported under its category: {text}"
    );
    assert!(
        text.contains("file=") && !text.contains("file=-"),
        "the report must locate the file: {text}"
    );
}

/// the checker contract holds for the CLI too: a missing directory is not created
#[test]
fn a_missing_directory_is_not_created() {
    let root = RandomPath::tmp();
    let missing = root.join("absent");
    assert!(!missing.exists());

    let output = run(&["check", &missing.to_string_lossy()]);
    assert_eq!(output.status.code(), Some(1), "{}", stderr(&output));
    assert!(
        !missing.exists(),
        "checking a missing directory must not create it"
    );
}

/// usage errors are reported on stderr with the usage exit code
#[test]
fn usage_errors_exit_with_code_two() {
    for args in [
        vec![],
        vec!["check"],
        vec!["check", "a", "b"],
        vec!["verify"],
    ] {
        let output = run(&args);
        assert_eq!(
            output.status.code(),
            Some(2),
            "args {args:?} must be a usage error, stderr: {}",
            stderr(&output)
        );
        assert!(
            stderr(&output).contains("Usage: mace-kv"),
            "args {args:?} must print usage on stderr, got: {}",
            stderr(&output)
        );
        assert!(
            stdout(&output).trim().is_empty(),
            "args {args:?} must not report anything on stdout, got: {}",
            stdout(&output)
        );
    }
}

/// help and version are not errors, and they print to stdout
#[test]
fn help_and_version_exit_zero_on_stdout() {
    for args in [vec!["--help"], vec!["check", "--help"]] {
        let output = run(&args);
        assert_eq!(
            output.status.code(),
            Some(0),
            "args {args:?} must not be an error, stderr: {}",
            stderr(&output)
        );
        assert!(
            stdout(&output).contains("Usage: mace-kv"),
            "args {args:?} must print usage on stdout, got: {}",
            stdout(&output)
        );
        assert!(
            stderr(&output).trim().is_empty(),
            "args {args:?} must not write to stderr, got: {}",
            stderr(&output)
        );
    }

    let output = run(&["--version"]);
    assert_eq!(output.status.code(), Some(0));
    assert!(
        stdout(&output).starts_with("mace-kv "),
        "the version must name the program, got: {}",
        stdout(&output)
    );
}

/// a live database is reported as busy, not as damage: btree-store holds an exclusive
/// lock while a writer has the store open, so the checker cannot read it at all
#[test]
fn a_live_database_is_reported_as_busy_not_as_damage() {
    let root = RandomPath::tmp();
    std::fs::create_dir_all(&*root).expect("scratch dir");
    seed(&root);
    // reopen and keep the handle alive: the exclusive lock is what the checker hits
    let mut opt = Options::new(&*root);
    opt.sync_on_write = true;
    let live = Mace::new(opt.validate().expect("validate")).expect("live open");

    let output = run(&["check", &root.to_string_lossy()]);
    let text = stdout(&output);
    assert!(
        text.contains("metadata_busy:"),
        "a locked store must be reported under its own category: {text}"
    );
    assert!(
        !text.contains("unreadable_metadata:"),
        "lock contention is not corruption and must not claim unreadable metadata: {text}"
    );
    assert_eq!(output.status.code(), Some(1), "{}", stderr(&output));
    drop(live);
}

/// page-level metadata damage terminates the CLI by signal after the upstream
/// `BTREE_FATAL_*` line; the tool adds no isolation of its own
#[test]
fn page_level_metadata_damage_terminates_the_cli() {
    let root = RandomPath::tmp();
    std::fs::create_dir_all(&*root).expect("scratch dir");
    let mut opt = Options::new(&*root);
    opt.sync_on_write = true;
    let mace = Mace::new(opt.validate().expect("validate")).expect("open store");
    let mut buckets = Vec::new();
    for index in 0..4 {
        buckets.push(
            mace.new_bucket(format!("b{index}"), Default::default())
                .expect("bucket"),
        );
    }
    for index in 0..4000u32 {
        let db = &buckets[(index as usize) % buckets.len()];
        let tx = db.begin().expect("txn");
        tx.put(format!("k_{index:05}"), vec![b'v'; 256])
            .expect("put");
        tx.commit().expect("commit");
    }
    for db in &buckets {
        db.checkpoint_and_wait();
    }
    drop(buckets);
    drop(mace);

    let manifest = Options::new(&*root).manifest();
    let pages = std::fs::metadata(&manifest).expect("metadata").len() / 4096;
    assert!(
        pages > 20,
        "fixture must span many metadata pages, got {pages}"
    );
    // keep both meta slots and the open-time reads valid, cut the pages a validator
    // reaches afterwards
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&manifest)
        .expect("open manifest");
    file.set_len(8 * 4096).expect("truncate manifest");
    drop(file);

    let output = run(&["check", &root.to_string_lossy()]);
    let text = stderr(&output);
    assert!(
        text.contains("BTREE_FATAL"),
        "page damage must surface the upstream fatal line, got: {text}"
    );
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(
            output.status.signal(),
            Some(6),
            "page-level metadata damage is fatal by design"
        );
    }
    #[cfg(not(unix))]
    assert!(!output.status.success());
}

#[test]
fn leak_commands_preserve_missing_paths_and_report_busy_metadata() {
    let root = RandomPath::tmp();
    let missing = root.join("absent");
    for fix in [false, true] {
        let mut args = vec!["leak-check", missing.to_str().unwrap()];
        if fix {
            args.push("--fix");
        }
        let result = run(&args);
        assert_eq!(result.status.code(), Some(1));
        assert!(stdout(&result).contains("missing_metadata"));
        assert!(!missing.exists());
    }
    seed(&root);
    let live = Mace::new(Options::new(&*root).validate().unwrap()).unwrap();
    let result = run(&["leak-check", root.to_str().unwrap(), "--fix"]);
    assert_eq!(result.status.code(), Some(1));
    assert!(stdout(&result).contains("metadata_busy"));
    assert!(!stdout(&result).contains("repaired:"));
    drop(live);
    for args in [
        vec!["leak-check"],
        vec!["leak-check", "a", "b"],
        vec!["leak-check", "--unknown"],
    ] {
        let result = run(&args);
        assert_eq!(result.status.code(), Some(2));
        assert!(stdout(&result).is_empty());
        assert!(!stderr(&result).is_empty());
    }
    let result = run(&["leak-check", "--fix"]);
    assert_eq!(result.status.code(), Some(2));
    assert!(stdout(&result).is_empty());
    assert!(!stderr(&result).is_empty());
}

#[test]
fn leak_check_zero_manifest_is_read_only_unless_fix_was_requested() {
    let root = RandomPath::tmp();
    let opt = Options::new(&*root);
    std::fs::create_dir_all(opt.log_root()).unwrap();
    std::fs::write(opt.manifest(), []).unwrap();
    let result = run(&["leak-check", root.to_str().unwrap()]);
    assert_eq!(result.status.code(), Some(1));
    assert!(stdout(&result).contains("unreadable_metadata:"));
    assert_eq!(std::fs::metadata(opt.manifest()).unwrap().len(), 0);
    let result = run(&["leak-check", root.to_str().unwrap(), "--fix"]);
    assert_eq!(result.status.code(), Some(1));
    assert!(stdout(&result).contains("missing_metadata"));
    assert!(!stdout(&result).contains("repaired:"));
    let tree = btree_store::BTree::open_read_only(opt.manifest()).unwrap();
    assert!(tree.buckets().unwrap().is_empty());
}

#[cfg(feature = "extra_check")]
#[test]
fn leak_cli_prints_only_final_diagnostics_then_repairs_then_summary() {
    use mace::maintenance::{BUCKET_DATA_STAT, FileKind, IMetaCodec, PersistStat};
    let root = RandomPath::tmp();
    let mut opt = Options::new(&*root);
    opt.gc_timeout = 0;
    let mace = Mace::new(opt.clone().validate().unwrap()).unwrap();
    let db = mace
        .new_bucket(
            "x",
            mace::BucketOptions {
                consolidate_threshold: 2,
                ..Default::default()
            },
        )
        .unwrap();
    for round in 0..12u8 {
        let tx = db.begin().unwrap();
        for key in 0..128 {
            tx.upsert(format!("key_{key}"), vec![round; 128]).unwrap();
        }
        tx.commit().unwrap();
        db.checkpoint_and_wait();
    }
    drop(db);
    drop(mace);
    let tree = btree_store::BTree::open_read_only(opt.manifest()).unwrap();
    let mut file = None;
    tree.view(BUCKET_DATA_STAT, |tx| {
        let mut it = tx.iter();
        let (mut k, mut v) = (Vec::new(), Vec::new());
        while it.next_ref(&mut k, &mut v) {
            let s = PersistStat::decode(&v);
            if !s.inactive_elems.is_empty() {
                file = Some(s.file_id);
                break;
            }
        }
        Ok(())
    })
    .unwrap();
    drop(tree);
    let file = file.expect("junk fixture");
    mace::testing::drop_one_inactive_mark_for_leak_test(&opt, FileKind::Data, file);
    let result = run(&["leak-check", root.to_str().unwrap()]);
    assert_eq!(result.status.code(), Some(1));
    assert!(stdout(&result).contains("junk_leak:"));
    let result = run(&["leak-check", root.to_str().unwrap(), "--fix"]);
    assert_eq!(
        result.status.code(),
        Some(0),
        "{} {}",
        stdout(&result),
        stderr(&result)
    );
    let text = stdout(&result);
    let lines: Vec<_> = text.lines().collect();
    assert!(lines[0].starts_with("repaired: data "), "{text}");
    assert!(lines.last().unwrap().ends_with(": ok"));
    assert!(!text.contains("junk_leak:"));
    let result = run(&["leak-check", root.to_str().unwrap(), "--fix"]);
    assert_eq!(result.status.code(), Some(0));
    assert!(!stdout(&result).contains("repaired:"));
    let before = fingerprint(&root);
    let result = run(&["leak-check", root.to_str().unwrap()]);
    assert_eq!(result.status.code(), Some(0), "{}", stderr(&result));
    assert!(stdout(&result).trim_end().ends_with(": ok"));
    assert_eq!(before, fingerprint(&root));
    let tree = btree_store::BTree::open(opt.manifest()).unwrap();
    let mut bad = None;
    tree.view(BUCKET_DATA_STAT, |tx| {
        let mut it = tx.iter();
        let (mut k, mut v) = (Vec::new(), Vec::new());
        while it.next_ref(&mut k, &mut v) {
            let stat = PersistStat::decode(&v);
            if stat.file_id != file {
                bad = Some(stat);
                break;
            }
        }
        Ok(())
    })
    .unwrap();
    let mut bad = bad.expect("second stat");
    bad.total_elems += 1;
    let mut encoded = vec![0; bad.packed_size()];
    bad.encode(&mut encoded);
    tree.exec(BUCKET_DATA_STAT, |tx| {
        tx.put(bad.file_id.to_le_bytes(), encoded.clone())
    })
    .unwrap();
    drop(tree);
    mace::testing::drop_one_inactive_mark_for_leak_test(&opt, FileKind::Data, file);
    let result = run(&["leak-check", root.to_str().unwrap(), "--fix"]);
    assert_eq!(result.status.code(), Some(1));
    let text = stdout(&result);
    let diagnostic = text.find("bad_stat_accounting:").unwrap();
    let repair = text.find("repaired:").unwrap();
    let summary = text.rfind("diagnostic(s)").unwrap();
    assert!(diagnostic < repair && repair < summary, "{text}");
    assert!(!text.contains("junk_leak:"));
}
