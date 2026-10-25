//! Offline checker acceptance tests.

mod common;

use btree_store::BTree;
use mace::{Mace, OpCode, Options, RandomPath};
use mace_kv_cli::{CheckCategory, check_database};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// recursive fingerprint of every file under `root`: path -> (length, mtime, bytes hash)
fn fingerprint(root: &Path) -> BTreeMap<String, (u64, i64, u64)> {
    fn walk(root: &Path, dir: &Path, out: &mut BTreeMap<String, (u64, i64, u64)>) {
        let entries = match std::fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(_) => return,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(root, &path, out);
                continue;
            }
            let meta = std::fs::metadata(&path).expect("metadata");
            let bytes = std::fs::read(&path).expect("bytes");
            let hash = bytes.iter().fold(0xcbf29ce484222325u64, |acc, byte| {
                (acc ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
            });
            let mtime = meta
                .modified()
                .ok()
                .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_nanos() as i64)
                .unwrap_or(0);
            let relative = path
                .strip_prefix(root)
                .unwrap_or(&path)
                .to_string_lossy()
                .into_owned();
            out.insert(relative, (meta.len(), mtime, hash));
        }
    }
    let mut out = BTreeMap::new();
    walk(root, root, &mut out);
    out
}

fn seed_database(root: &Path) -> Result<(), OpCode> {
    let mut opt = Options::new(root);
    opt.sync_on_write = true;
    let mace = Mace::new(opt.clone().validate()?)?;
    let db = mace.new_bucket("x", Default::default())?;
    for index in 0..32u32 {
        let tx = db.begin()?;
        tx.put(format!("k_{index:04}"), vec![b'v'; 128])?;
        tx.commit()?;
    }
    db.checkpoint_and_wait();
    Ok(())
}

/// Seeds a store whose metadata tree spans many pages, so page reads after the meta
/// slots have something to hit.
fn seed_large_database(root: &Path) -> Result<(), OpCode> {
    let mut opt = Options::new(root);
    opt.sync_on_write = true;
    let mace = Mace::new(opt.clone().validate()?)?;
    let mut buckets = Vec::new();
    for index in 0..4 {
        buckets.push(mace.new_bucket(format!("b{index}"), Default::default())?);
    }
    for index in 0..4000u32 {
        let db = &buckets[(index as usize) % buckets.len()];
        let tx = db.begin()?;
        tx.put(format!("k_{index:05}"), vec![b'v'; 256])?;
        tx.commit()?;
    }
    for db in &buckets {
        db.checkpoint_and_wait();
    }
    Ok(())
}

fn seed_cross_file_chain(root: &Path) -> Result<(Options, u64), OpCode> {
    let mut opt = Options::new(root);
    opt.sync_on_write = true;
    let mace = Mace::new(opt.clone().validate()?)?;
    let db = mace.new_bucket("chain", Default::default())?;
    let tx = db.begin()?;
    tx.put("key", vec![b'a'; 128])?;
    tx.commit()?;
    db.checkpoint_and_wait();
    let tx = db.begin()?;
    tx.update("key", vec![b'b'; 128])?;
    tx.commit()?;
    db.checkpoint_and_wait();
    let bucket_id = db.id();
    drop(db);
    drop(mace);
    Ok((opt, bucket_id))
}

fn seed_sibling_pages(root: &Path) -> Result<u64, OpCode> {
    let mut opt = Options::new(root);
    opt.sync_on_write = true;
    let mace = Mace::new(opt.validate()?)?;
    let db = mace.new_bucket(
        "sibling",
        mace::BucketOptions {
            inline_size: 256,
            split_elems: 8,
            consolidate_threshold: 2,
            ..mace::BucketOptions::default()
        },
    )?;
    let keys = (0..64)
        .map(|index| format!("key_{index:04}"))
        .collect::<Vec<_>>();
    let mut lagging = Vec::new();
    for round in 0..40 {
        lagging.push(db.view()?);
        let value = vec![b'a' + (round % 26) as u8; 64];
        let tx = db.begin()?;
        for key in &keys {
            tx.upsert(key, &value)?;
        }
        tx.commit()?;
        db.checkpoint_and_wait();
    }
    drop(lagging);
    let bucket_id = db.id();
    drop(db);
    drop(mace);
    Ok(bucket_id)
}

fn interval_rows(root: &Path, bucket_id: u64) -> Vec<(Vec<u8>, (u64, u64, u64))> {
    let opt = Options::new(root);
    let btree = BTree::open(opt.manifest()).expect("open manifest");
    let mut rows = Vec::new();
    btree
        .view(&format!("data_interval_{bucket_id}"), |txn| {
            let mut iter = txn.iter();
            let (mut key, mut value) = (Vec::new(), Vec::new());
            while iter.next_ref(&mut key, &mut value) {
                if value.len() >= 32 {
                    let lo = u64::from_le_bytes(value[0..8].try_into().unwrap());
                    let hi = u64::from_le_bytes(value[8..16].try_into().unwrap());
                    let file = u64::from_le_bytes(value[16..24].try_into().unwrap());
                    rows.push((key.clone(), (lo, hi, file)));
                }
            }
            Ok(())
        })
        .expect("read intervals");
    rows
}

fn count_sibling_pages(root: &Path, bucket_id: u64) -> usize {
    let intervals = interval_rows(root, bucket_id);
    std::fs::read_dir(Options::new(root).data_root())
        .expect("read data directory")
        .flatten()
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("data_"))
        .map(|entry| {
            let bytes = std::fs::read(entry.path()).expect("read data file");
            let mut count = 0;
            let mut offset = 0;
            while offset + 64 <= bytes.len() {
                let header = &bytes[offset..offset + 64];
                let total_size = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
                if total_size < 64 || offset + total_size > bytes.len() {
                    break;
                }
                let address = u64::from_le_bytes(header[28..36].try_into().unwrap());
                let covered = intervals
                    .iter()
                    .any(|(_, (lo, hi, _))| address >= *lo && address <= *hi);
                if covered && header[0] == 2 && header[1] == 1 && header[2] == 2 {
                    count += 1;
                }
                offset += total_size;
            }
            count
        })
        .sum()
}

/// A healthy database checks clean and the run leaves the directory byte-identical.
#[test]
fn checking_a_healthy_database_changes_nothing() -> Result<(), OpCode> {
    let scratch = RandomPath::tmp();
    std::fs::create_dir_all(&*scratch).expect("scratch dir");
    let root = scratch.join("db");
    seed_database(&root)?;

    let before = fingerprint(&scratch);
    let report = check_database(&Options::new(&root));
    let after = fingerprint(&scratch);

    assert!(
        report.is_ok(),
        "a healthy database must check clean: {report:?}"
    );
    assert_eq!(
        before, after,
        "a check run must not create, remove or modify a single file"
    );
    Ok(())
}

#[test]
fn unresolved_page_chain_reference_is_reported_without_writing() -> Result<(), OpCode> {
    let scratch = RandomPath::tmp();
    std::fs::create_dir_all(&*scratch).expect("scratch dir");
    let root = scratch.join("db");
    let (opt, bucket_id) = seed_cross_file_chain(&root)?;
    let rows = interval_rows(&root, bucket_id);
    assert!(rows.len() >= 2, "fixture must produce two data intervals");

    let btree = BTree::open(opt.manifest()).expect("open manifest");
    let mut page_addr = None;
    btree
        .view(&format!("page_table_{bucket_id}"), |txn| {
            let mut iter = txn.iter();
            let (mut key, mut value) = (Vec::new(), Vec::new());
            if iter.next_ref(&mut key, &mut value) && value.len() >= 8 {
                page_addr = Some(u64::from_le_bytes(value[..8].try_into().unwrap()));
            }
            Ok(())
        })
        .expect("read page table");
    let page_addr = page_addr.expect("fixture must map a page");
    let root_file = rows
        .iter()
        .find(|(_, (lo, hi, _))| page_addr >= *lo && page_addr <= *hi)
        .map(|(_, (_, _, file))| *file)
        .expect("page root must be covered");
    let (_, (_, _, deleted_file)) = rows
        .iter()
        .find(|(_, (_, _, file))| *file != root_file)
        .expect("fixture must retain a prior chain file");
    let delete_key = rows
        .iter()
        .find(|(_, (_, _, file))| *file == *deleted_file)
        .map(|(key, _)| key.clone())
        .unwrap();
    btree
        .exec(&format!("data_interval_{bucket_id}"), |txn| {
            txn.del(delete_key)
        })
        .expect("remove chain-root interval");
    drop(btree);
    let before = fingerprint(&scratch);
    let report = check_database(&Options::new(&root));
    assert!(
        report
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.category == CheckCategory::BadPageChain),
        "missing chain interval must be reported: {report:?}"
    );
    assert_eq!(fingerprint(&scratch), before, "checker must stay read-only");
    Ok(())
}

#[test]
fn intact_cross_file_page_chain_checks_clean() -> Result<(), OpCode> {
    let scratch = RandomPath::tmp();
    std::fs::create_dir_all(&*scratch).expect("scratch dir");
    let root = scratch.join("db");
    let (_opt, _bucket_id) = seed_cross_file_chain(&root)?;
    let report = check_database(&Options::new(&root));
    assert!(
        report.is_ok(),
        "an intact page chain must check clean: {report:?}"
    );
    Ok(())
}

/// An internal page still references every child page id, so removing a non-root page-table
/// row must be reported even though every remaining row stays self-consistent.
#[test]
fn missing_child_page_mapping_is_reported() -> Result<(), OpCode> {
    let scratch = RandomPath::tmp();
    std::fs::create_dir_all(&*scratch).expect("scratch dir");
    let root = scratch.join("db");
    let mut opt = Options::new(&root);
    opt.sync_on_write = true;
    let mace = Mace::new(opt.validate()?)?;
    let db = mace.new_bucket("internal", Default::default())?;
    let tx = db.begin()?;
    for index in 0..1500u32 {
        tx.put(format!("k_{index:05}"), vec![b'v'; 256])?;
    }
    tx.commit()?;
    db.checkpoint_and_wait();
    let bucket_id = db.id();
    drop(db);
    drop(mace);

    let btree = BTree::open(Options::new(&root).manifest()).expect("open manifest");
    let mut victim = None;
    btree
        .view(&format!("page_table_{bucket_id}"), |txn| {
            let mut iter = txn.iter();
            let (mut key, mut value) = (Vec::new(), Vec::new());
            while iter.next_ref(&mut key, &mut value) {
                if key.len() < 8 || value.len() < 8 {
                    continue;
                }
                let pid = u64::from_le_bytes(key[..8].try_into().unwrap());
                if pid != 1 {
                    victim = Some(key.clone());
                }
            }
            Ok(())
        })
        .expect("read page table");
    let victim = victim.expect("the seeded tree must map a non-root page");
    btree
        .exec(&format!("page_table_{bucket_id}"), |txn| txn.del(victim))
        .expect("remove child mapping");
    drop(btree);

    let report = check_database(&Options::new(&root));
    assert!(
        report
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.category == CheckCategory::BadPageChain),
        "a deleted child mapping must be reported: {report:?}"
    );
    let before = fingerprint(&root);
    let result = mace_kv_cli::leak_check_database(&Options::new(&root), true);
    assert!(result.repairs.is_empty());
    assert!(
        result
            .report
            .diagnostics
            .iter()
            .any(|d| d.category == CheckCategory::LeakUndecidable)
    );
    assert!(
        !result
            .report
            .diagnostics
            .iter()
            .any(|d| d.category == CheckCategory::JunkLeak)
    );
    assert_eq!(before, fingerprint(&root));
    Ok(())
}

#[test]
fn checker_accepts_reachable_sibling_pages() -> Result<(), OpCode> {
    let scratch = RandomPath::tmp();
    std::fs::create_dir_all(&*scratch).expect("scratch dir");
    let root = scratch.join("db");
    let bucket_id = seed_sibling_pages(&root)?;
    assert!(
        count_sibling_pages(&root, bucket_id) > 0,
        "fixture must persist sibling pages in the target bucket address space"
    );
    let report = check_database(&Options::new(&root));
    assert!(
        report.is_ok(),
        "sibling pages must not be reported as damaged: {report:?}"
    );
    Ok(())
}

/// A directory without a metadata store is reported, not initialized.
#[test]
fn checking_an_empty_directory_does_not_create_metadata() -> Result<(), OpCode> {
    let scratch = RandomPath::tmp();
    std::fs::create_dir_all(&*scratch).expect("scratch dir");
    let root = scratch.join("db");
    std::fs::create_dir_all(&root).expect("empty db dir");
    let opt = Options::new(&root);

    let report = check_database(&opt);

    assert_eq!(report.diagnostics.len(), 1, "got {report:?}");
    assert_eq!(
        report.diagnostics[0].category,
        CheckCategory::MissingMetadata
    );
    assert!(
        !opt.manifest().exists(),
        "the checker must never create the metadata store"
    );
    Ok(())
}

/// A zero-length metadata store is unreadable, not an empty database.
#[test]
fn checking_a_zero_length_metadata_store_is_not_initialization() -> Result<(), OpCode> {
    let scratch = RandomPath::tmp();
    std::fs::create_dir_all(&*scratch).expect("scratch dir");
    let root = scratch.join("db");
    std::fs::create_dir_all(root.join("log")).expect("log dir");
    let opt = Options::new(&root);
    std::fs::write(opt.manifest(), b"").expect("seed empty manifest");
    let before = fingerprint(&scratch);

    let report = check_database(&opt);

    assert_eq!(report.diagnostics.len(), 1, "got {report:?}");
    assert_eq!(
        report.diagnostics[0].category,
        CheckCategory::UnreadableMetadata
    );
    assert_eq!(
        fingerprint(&scratch),
        before,
        "an empty metadata store must stay empty (no initialization)"
    );
    Ok(())
}

/// An unusable meta slot pair is reported as a category: this covers the open-time
/// class only. Page-read damage aborts inside btree-store by design, which is
/// pinned separately by `metadata_page_damage_aborts_by_design`.
#[test]
fn meta_slot_corruption_is_reported_as_a_category() -> Result<(), OpCode> {
    let scratch = RandomPath::tmp();
    std::fs::create_dir_all(&*scratch).expect("scratch dir");
    let root = scratch.join("db");
    seed_database(&root)?;
    let opt = Options::new(&root);
    // overwrite the meta pages with garbage of the same length
    let manifest = opt.manifest();
    let mut bytes = std::fs::read(&manifest).expect("read manifest");
    for byte in bytes.iter_mut().take(8192) {
        *byte = 0xA5;
    }
    std::fs::write(&manifest, &bytes).expect("corrupt manifest");

    let report = check_database(&opt);

    assert!(
        !report.is_ok(),
        "a corrupt metadata store must be reported, got {report:?}"
    );
    assert!(
        report.diagnostics.iter().all(|diagnostic| matches!(
            diagnostic.category,
            CheckCategory::UnreadableMetadata | CheckCategory::BadVersion
        )),
        "corruption must map to a stable category: {report:?}"
    );
    Ok(())
}

const ENV_CHILD: &str = "MACE_CHECKER_ABORT_CHILD";
const ENV_ROOT: &str = "MACE_CHECKER_ABORT_ROOT";

/// child body: run a check on a damaged store and report its exit code
#[test]
fn damaged_store_child() {
    if std::env::var(ENV_CHILD).ok().as_deref() != Some("1") {
        return;
    }
    let root = PathBuf::from(std::env::var(ENV_ROOT).expect("child root"));
    let report = check_database(&Options::new(&root));
    eprintln!("child report: {report:?}");
    std::process::exit(report.exit_code());
}

/// Pins the documented fatal behavior for *page-level* metadata damage: btree-store
/// aborts (`BTREE_FATAL_IO`) instead of returning a diagnostic, because an unusable
/// metadata store cannot be described meaningfully.
///
/// This test pins that behaviour: btree-store logs a `BTREE_FATAL_*` line (path,
/// generation, offset) and only then aborts. The CLI adopts the same shape: page-level
/// metadata damage terminates the tool with a non-zero signal after the log line, with
/// no extra process isolation.
#[test]
fn metadata_page_damage_aborts_by_design() -> Result<(), OpCode> {
    let scratch = RandomPath::tmp();
    std::fs::create_dir_all(&*scratch).expect("scratch dir");
    let root = scratch.join("db");
    seed_large_database(&root)?;
    let opt = Options::new(&root);
    let manifest = opt.manifest();
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
    file.set_len(8 * 4096).expect("truncate pages");
    drop(file);

    let exe = std::env::current_exe().expect("current exe");
    let status = common::child_test_command(&exe)
        .arg("--exact")
        .arg("damaged_store_child")
        .arg("--nocapture")
        .env(ENV_CHILD, "1")
        .env(ENV_ROOT, root.as_os_str())
        .status()
        .expect("child must run");

    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(
            status.signal(),
            Some(6),
            "page-level metadata damage is fatal by design: {status:?}"
        );
    }
    #[cfg(not(unix))]
    assert!(
        !status.success(),
        "page-level metadata damage is fatal by design: {status:?}"
    );
    Ok(())
}

/// A damaged payload file is reported with its file id and address, without panicking
/// and without touching the file.
#[test]
fn payload_damage_is_reported_with_localisation() -> Result<(), OpCode> {
    let scratch = RandomPath::tmp();
    std::fs::create_dir_all(&*scratch).expect("scratch dir");
    let root = scratch.join("db");
    seed_database(&root)?;
    let opt = Options::new(&root);
    let payload = std::fs::read_dir(opt.data_root())
        .expect("data root")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("data_"))
        })
        .expect("fixture must publish a data file");
    let file_id: u64 = payload
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_prefix("data_"))
        .and_then(|raw| raw.parse().ok())
        .expect("payload name carries its id");
    // flip a byte inside the relocation table region: the table checksum must fail
    let mut bytes = std::fs::read(&payload).expect("read payload");
    let len = bytes.len();
    let victim = len - 64;
    bytes[victim] ^= 0xFF;
    std::fs::write(&payload, &bytes).expect("corrupt payload");
    let before = fingerprint(&scratch);

    let report = check_database(&opt);

    assert!(!report.is_ok(), "corruption must be reported: {report:?}");
    let diagnostic = report
        .diagnostics
        .iter()
        .find(|diagnostic| diagnostic.category == CheckCategory::BadPayloadFile)
        .expect("a payload diagnostic must be produced");
    assert_eq!(diagnostic.file_id, Some(file_id), "{diagnostic:?}");
    assert_eq!(report.exit_code(), 1);
    assert_eq!(
        fingerprint(&scratch),
        before,
        "checking a damaged payload must not modify anything"
    );
    Ok(())
}

/// A relocation row can carry a valid table checksum while its record length is smaller than
/// the persisted box header. That must surface as payload damage, not an allocation underflow.
#[test]
fn undersized_relocation_length_is_reported() -> Result<(), OpCode> {
    use std::hash::Hasher;

    const FOOTER_LEN: usize = 24;
    const RELOC_LEN: usize = 32;

    let scratch = RandomPath::tmp();
    std::fs::create_dir_all(&*scratch).expect("scratch dir");
    let root = scratch.join("db");
    seed_database(&root)?;
    let opt = Options::new(&root);
    let payload = std::fs::read_dir(opt.data_root())
        .expect("data root")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("data_"))
        })
        .expect("fixture must publish a data file");
    let mut bytes = std::fs::read(&payload).expect("read payload");
    let len = bytes.len();
    let footer = len - FOOTER_LEN;
    let nr_reloc = u32::from_le_bytes(bytes[footer + 8..footer + 12].try_into().unwrap()) as usize;
    assert!(nr_reloc > 0, "fixture must publish relocation rows");
    let relocs = footer - nr_reloc * RELOC_LEN;
    for index in 0..nr_reloc {
        let raw_len = relocs + index * RELOC_LEN + 16;
        bytes[raw_len..raw_len + 4].copy_from_slice(&1u32.to_le_bytes());
    }
    let mut crc = crc32c::Crc32cHasher::default();
    crc.write(&bytes[relocs..footer]);
    bytes[footer + 16..footer + 20].copy_from_slice(&(crc.finish() as u32).to_le_bytes());
    std::fs::write(&payload, &bytes).expect("corrupt payload");
    let before = fingerprint(&scratch);

    let report = check_database(&opt);

    assert!(
        report
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.category == CheckCategory::BadPayloadFile),
        "an undersized relocation length must be reported: {report:?}"
    );
    assert_eq!(
        fingerprint(&scratch),
        before,
        "checking a damaged payload must not modify anything"
    );
    Ok(())
}

/// A truncated payload file is a diagnostic, not a panic.
#[test]
fn a_truncated_payload_file_is_reported() -> Result<(), OpCode> {
    let scratch = RandomPath::tmp();
    std::fs::create_dir_all(&*scratch).expect("scratch dir");
    let root = scratch.join("db");
    seed_database(&root)?;
    let opt = Options::new(&root);
    let payload = std::fs::read_dir(opt.data_root())
        .expect("data root")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("data_"))
        })
        .expect("fixture must publish a data file");
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&payload)
        .expect("open payload");
    file.set_len(16).expect("truncate payload");
    drop(file);

    let report = check_database(&opt);

    assert!(
        report
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.category == CheckCategory::BadPayloadFile),
        "a truncated payload must be reported: {report:?}"
    );
    Ok(())
}

/// A store another process holds open is lock contention, not damage: the runtime open
/// must return a retryable error instead of aborting the process.
#[test]
fn opening_a_store_another_process_holds_returns_again() -> Result<(), OpCode> {
    const ENV_CHILD: &str = "MACE_CHECKER_BUSY_CHILD";
    const ENV_ROOT: &str = "MACE_CHECKER_BUSY_ROOT";

    let root = RandomPath::tmp();
    std::fs::create_dir_all(&*root).expect("scratch dir");
    seed_database(&root)?;

    // the parent holds the exclusive lock for the whole child run
    let live = Mace::new(Options::new(&*root).validate()?)?;
    let output = common::child_test_command(&std::env::current_exe().expect("current exe"))
        .arg("--exact")
        .arg("a_live_store_open_child")
        .arg("--nocapture")
        .env(ENV_CHILD, "1")
        .env(ENV_ROOT, &*root)
        .output()
        .expect("child must run");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("open error: Some(Again)"),
        "a store another process holds must be a retryable error: {stdout}{}",
        String::from_utf8_lossy(&output.stderr)
    );
    drop(live);

    // the handle is released, so the same open now succeeds
    drop(Mace::new(Options::new(&*root).validate()?)?);
    Ok(())
}

/// the child half of the test above: it opens the store the parent holds and prints the
/// error it got, so the parent can assert on a real cross-process refusal
#[test]
fn a_live_store_open_child() {
    if std::env::var("MACE_CHECKER_BUSY_CHILD").ok().as_deref() != Some("1") {
        return;
    }
    let root = PathBuf::from(std::env::var("MACE_CHECKER_BUSY_ROOT").expect("child root"));
    let result = Mace::new(Options::new(&root).validate().expect("child options"));
    println!("open error: {:?}", result.err());
}

/// WAL records that do not frame or checksum are reported, and whether a bad tail is
/// tolerated follows the database's own persisted option rather than the caller's.
#[test]
fn wal_tail_tolerance_follows_the_persisted_option() -> Result<(), OpCode> {
    // a database that refuses to truncate: recovery rejects it, so the checker must too
    let strict_scratch = RandomPath::tmp();
    let strict_root = strict_scratch.join("db");
    std::fs::create_dir_all(strict_root.parent().expect("scratch")).expect("scratch dir");
    let mut strict_seed = Options::new(&strict_root);
    strict_seed.truncate_corrupted_wal = false;
    let mutator = Mace::new(strict_seed.validate()?)?;
    let bucket = mutator.new_bucket("x", Default::default())?;
    for index in 0..16u32 {
        let tx = bucket.begin()?;
        tx.put(format!("k_{index:04}"), vec![b'v'; 128])?;
        tx.commit()?;
    }
    bucket.checkpoint_and_wait();
    drop(bucket);
    drop(mutator);
    damage_shared_wal_tail(&strict_root)?;

    // the caller's options are permissive, the database's are not: the damage is found
    let report = check_database(&Options::new(&strict_root));
    assert!(
        report
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.category == CheckCategory::BadWal),
        "the database's persisted option is authoritative: {report:?}"
    );

    // a database that does truncate: the same tail damage is what recovery drops
    let tolerant_scratch = RandomPath::tmp();
    let tolerant_root = tolerant_scratch.join("db");
    std::fs::create_dir_all(tolerant_root.parent().expect("scratch")).expect("scratch dir");
    seed_database(&tolerant_root)?;
    damage_shared_wal_tail(&tolerant_root)?;
    let mut strict_caller = Options::new(&tolerant_root);
    strict_caller.truncate_corrupted_wal = false;
    let tolerated = check_database(&strict_caller);
    assert!(
        tolerated
            .diagnostics
            .iter()
            .all(|diagnostic| diagnostic.category != CheckCategory::BadWal),
        "the last segment's bad tail is what recovery truncates here: {tolerated:?}"
    );
    Ok(())
}

/// flips a byte in the middle of the only shared wal segment, leaving a damaged record
/// in the middle of a stream
fn damage_shared_wal_tail(root: &Path) -> std::io::Result<()> {
    let log_root = Options::new(root).log_root();
    let segment = std::fs::read_dir(&log_root)
        .expect("log root")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("group_wal_"))
        })
        .expect("fixture must write a shared wal segment");
    let mut bytes = std::fs::read(&segment)?;
    let victim = bytes.len() / 2;
    bytes[victim] ^= 0xFF;
    std::fs::write(&segment, &bytes)
}

/// A missing segment in the middle of a stream is reported as a gap.
#[test]
fn wal_stream_gaps_are_reported() -> Result<(), OpCode> {
    let scratch = RandomPath::tmp();
    std::fs::create_dir_all(&*scratch).expect("scratch dir");
    let root = scratch.join("db");
    seed_database(&root)?;
    let opt = Options::new(&root);
    let segment = std::fs::read_dir(opt.log_root())
        .expect("log root")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("group_wal_"))
        })
        .expect("fixture must write a shared wal segment");
    let file_id: u64 = segment
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_prefix("group_wal_"))
        .and_then(|raw| raw.parse().ok())
        .expect("segment name carries its id");
    // leave a hole: the stream now holds <id> and <id + 2>
    let hole = opt.group_wal_file(file_id + 2);
    std::fs::copy(&segment, &hole).expect("copy segment past a hole");

    let report = check_database(&Options::new(&root));

    assert!(
        report.diagnostics.iter().any(|diagnostic| {
            diagnostic.category == CheckCategory::BadWal && diagnostic.detail.contains("missing")
        }),
        "a stream gap must be reported: {report:?}"
    );
    Ok(())
}

/// A durable checkpoint copy is a standalone database: the checker reports it clean and the
/// copy opens and reads back the copied keys.
#[test]
fn a_checkpoint_copy_checks_clean_and_reads_back() -> Result<(), OpCode> {
    let scratch = RandomPath::tmp();
    std::fs::create_dir_all(&*scratch).expect("scratch dir");
    let root = scratch.join("db");
    seed_database(&root)?;

    let target = scratch.join("copy");
    let mace = Mace::new(Options::new(&root).validate()?)?;
    let copy = mace.create_checkpoint(&target)?;
    assert!(!copy.frontiers.is_empty(), "the copy must carry frontiers");
    drop(mace);

    let report = check_database(&Options::new(&target));
    assert!(
        report.is_ok(),
        "a checkpoint copy must check clean: {report:?}"
    );

    // the copy is a database in its own right
    let copied = Mace::new(Options::new(&target).validate()?)?;
    let bucket = copied.open_bucket("x")?;
    let tx = bucket.begin()?;
    for index in 0..32u32 {
        let value = tx.get(format!("k_{index:04}"))?;
        assert_eq!(
            value.slice().len(),
            128,
            "copied key {index} must read back"
        );
    }
    drop(tx);
    drop(copied);
    Ok(())
}

/// A store in the pending-delete window is clean both before and after the physical reap:
/// the logical delete unpublishes the bucket and records the pending entry in one commit.
#[test]
fn a_pending_delete_window_checks_clean() -> Result<(), OpCode> {
    let scratch = RandomPath::tmp();
    std::fs::create_dir_all(&*scratch).expect("scratch dir");
    let root = scratch.join("db");
    seed_database(&root)?;

    let mut opt = Options::new(&root);
    opt.sync_on_write = true;
    let mace = Mace::new(opt.validate()?)?;
    let extra = mace.new_bucket("doomed", Default::default())?;
    for index in 0..16u32 {
        let tx = extra.begin()?;
        tx.put(format!("d_{index:04}"), vec![b'v'; 64])?;
        tx.commit()?;
    }
    extra.checkpoint_and_wait();
    // a bucket handle keeps the bucket busy, so the delete needs it released first
    drop(extra);
    // the delete reports `Again` while a pending abort-clean is outstanding: drive the
    // cleanup synchronously instead of sleeping (the pattern the bucket tests use)
    let mut attempts = 0;
    loop {
        match mace.del_bucket("doomed") {
            Ok(()) => break,
            Err(OpCode::Again) if attempts < 64 => {
                attempts += 1;
                mace.start_gc();
            }
            Err(err) => panic!("the bucket delete did not complete: {err:?}"),
        }
    }
    drop(mace);

    let report = check_database(&Options::new(&root));
    assert!(
        report.is_ok(),
        "a store in the pending-delete window must check clean: {report:?}"
    );

    // reopening runs the pending reap; the store must still check clean
    let reopened = Mace::new(Options::new(&root).validate()?)?;
    drop(reopened);
    let report = check_database(&Options::new(&root));
    assert!(
        report.is_ok(),
        "a store after the pending reap must check clean: {report:?}"
    );
    Ok(())
}

#[cfg(feature = "extra_check")]
mod leaks {
    use super::*;
    use mace::maintenance::{
        BUCKET_BLOB_STAT, BUCKET_DATA_STAT, FileKind, IHeader, IMetaCodec, MetaReader, PersistStat,
        ROOT_PID, page_table_name,
    };
    use mace::testing::{
        add_inactive_mark_for_leak_test, drop_one_inactive_mark_for_leak_test,
        persisted_reachable_for_test,
    };
    use mace_kv_cli::{leak_check_database, leak_check_reachable_for_test};

    fn stats(opt: &Options, kind: FileKind) -> Vec<PersistStat> {
        let tree = BTree::open_read_only(opt.manifest()).unwrap();
        let mut out = Vec::new();
        tree.view(
            match kind {
                FileKind::Data => BUCKET_DATA_STAT,
                FileKind::Blob => BUCKET_BLOB_STAT,
            },
            |tx| {
                let mut it = tx.iter();
                let (mut k, mut v) = (Vec::new(), Vec::new());
                while it.next_ref(&mut k, &mut v) {
                    out.push(PersistStat::decode(&v));
                }
                Ok(())
            },
        )
        .unwrap();
        out
    }

    fn seed_junk(root: &Path) -> (Options, u64) {
        let mut opt = Options::new(root);
        opt.gc_timeout = 0;
        opt.sync_on_write = true;
        let mace = Mace::new(opt.clone().validate().unwrap()).unwrap();
        let db = mace
            .new_bucket(
                "leaks",
                mace::BucketOptions {
                    inline_size: 32,
                    consolidate_threshold: 2,
                    split_elems: 8,
                    ..Default::default()
                },
            )
            .unwrap();
        for round in 0..12u8 {
            let tx = db.begin().unwrap();
            for key in 0..128 {
                if round == 0 || key >= 64 {
                    tx.upsert(format!("key_{key:03}"), vec![round; 512])
                        .unwrap();
                }
            }
            tx.commit().unwrap();
            db.checkpoint_and_wait();
        }
        let tx = db.begin().unwrap();
        tx.upsert("key_127", vec![11; 512]).unwrap();
        tx.commit().unwrap();
        db.checkpoint_and_wait();
        let id = db.id();
        drop(db);
        drop(mace);
        (opt, id)
    }

    fn assert_closed_closures(opt: &Options, id: u64) {
        let before = fingerprint(&opt.db_root());
        let typed = persisted_reachable_for_test(opt, id);
        let raw = leak_check_reachable_for_test(opt, id);
        assert!(!typed.0.is_empty());
        assert_eq!(typed, raw);
        assert_eq!(before, fingerprint(&opt.db_root()));
    }

    #[test]
    fn fixed_root_collectors_agree_on_closed_history_and_cross_file_chains() -> Result<(), OpCode> {
        let scratch = RandomPath::tmp();
        let first = scratch.join("history");
        let id = seed_sibling_pages(&first)?;
        assert!(count_sibling_pages(&first, id) > 0);
        let history_opt = Options::new(&first);
        assert_closed_closures(&history_opt, id);
        let history_closure = leak_check_reachable_for_test(&history_opt, id);
        let (mut sibling, mut branch) = (None, None);
        for stat in stats(&history_opt, FileKind::Data) {
            let reader = mace::maintenance::try_new_reader(
                history_opt.data_file(stat.file_id),
                mace::maintenance::DecompressorPool::new(),
                history_opt.fs_arc(),
            )
            .unwrap();
            for reloc in MetaReader::try_new(history_opt.fs(), history_opt.data_file(stat.file_id))
                .unwrap()
                .try_get_reloc()
                .unwrap()
                .iter()
            {
                let addr = reloc.key;
                let page = reader.try_read_at(addr).unwrap();
                if page.header().flag == mace::maintenance::TagFlag::Sibling {
                    sibling = Some(addr);
                }
                if page.header().node_type == mace::maintenance::NodeType::Intl {
                    branch = Some(addr);
                }
            }
        }
        assert!(
            history_closure
                .0
                .contains(&sibling.expect("concrete history sibling"))
        );
        assert!(
            history_closure
                .0
                .contains(&branch.expect("concrete branch"))
        );
        let r = leak_check_database(&Options::new(&first), false);
        assert!(r.report.is_ok(), "{r:?}");
        let (opt, id) = seed_cross_file_chain(&scratch.join("chain"))?;
        assert_closed_closures(&opt, id);
        let closure = leak_check_reachable_for_test(&opt, id);
        let mut owners = std::collections::BTreeMap::new();
        for stat in stats(&opt, FileKind::Data) {
            for reloc in MetaReader::try_new(opt.fs(), opt.data_file(stat.file_id))
                .unwrap()
                .try_get_reloc()
                .unwrap()
                .iter()
            {
                owners.insert(reloc.key, stat.file_id);
            }
        }
        let mut cross_link = false;
        for (&addr, &file) in &owners {
            if !closure.0.contains(&addr) {
                continue;
            }
            let reader = mace::maintenance::try_new_reader(
                opt.data_file(file),
                mace::maintenance::DecompressorPool::new(),
                opt.fs_arc(),
            )
            .unwrap();
            let page = reader.try_read_at(addr).unwrap();
            let link = page.header().link;
            if link != 0 && owners[&link] != file {
                assert!(closure.0.contains(&link));
                assert_eq!(page.header().kind, mace::maintenance::TagKind::Delta);
                cross_link = true;
            }
        }
        assert!(
            cross_link,
            "fixture must retain a concrete cross-file page link"
        );
        assert!(leak_check_database(&opt, false).report.is_ok());
        Ok(())
    }

    #[test]
    fn missing_marks_are_detected_repaired_and_idempotent_for_both_kinds() {
        for kind in FileKind::ALL {
            let root = RandomPath::tmp();
            let (opt, id) = seed_junk(&root);
            assert_closed_closures(&opt, id);
            assert!(!persisted_reachable_for_test(&opt, id).1.is_empty());
            let clean = leak_check_database(&opt, false);
            assert!(clean.report.is_ok(), "{clean:?}");
            let stat = stats(&opt, kind)
                .into_iter()
                .find(|s| !s.inactive_elems.is_empty())
                .expect("inactive fixture");
            let (addr, _) = drop_one_inactive_mark_for_leak_test(&opt, kind, stat.file_id);
            assert!(check_database(&opt).is_ok());
            let before = fingerprint(&root);
            let detection = leak_check_database(&opt, false);
            assert_eq!(before, fingerprint(&root));
            let leaks: Vec<_> = detection
                .report
                .diagnostics
                .iter()
                .filter(|d| d.category == CheckCategory::JunkLeak)
                .collect();
            assert_eq!(leaks.len(), 1, "{detection:?}");
            assert_eq!(leaks[0].address, Some(addr));
            assert_eq!(leaks[0].bucket_id, Some(id));
            assert_eq!(leaks[0].file_id, Some(stat.file_id));
            let fixed = leak_check_database(&opt, true);
            assert!(fixed.report.is_ok(), "{fixed:?}");
            assert!(fixed.fault.is_none());
            assert_eq!(fixed.repairs.len(), 1);
            assert_eq!(fixed.repairs[0].added_inactive, 1);
            assert_eq!(fixed.repairs[0].bucket_id, id);
            assert_eq!(fixed.repairs[0].file_id, stat.file_id);
            assert_eq!(
                fixed.repairs[0].kind,
                match kind {
                    FileKind::Data => "data",
                    FileKind::Blob => "blob",
                }
            );
            let restored = stats(&opt, kind)
                .into_iter()
                .find(|s| s.file_id == stat.file_id)
                .unwrap();
            assert_eq!((restored.up1, restored.up2), (stat.up1, stat.up2));
            assert_eq!(restored.inactive_elems, stat.inactive_elems);
            assert_eq!(
                (
                    restored.active_elems,
                    restored.total_elems,
                    restored.active_size,
                    restored.total_size
                ),
                (
                    stat.active_elems,
                    stat.total_elems,
                    stat.active_size,
                    stat.total_size
                )
            );
            let before = fingerprint(&root);
            let again = leak_check_database(&opt, true);
            assert!(again.report.is_ok());
            assert!(again.repairs.is_empty());
            assert_eq!(before, fingerprint(&root));
            let mace = Mace::new(opt.validate().unwrap()).unwrap();
            let db = mace.open_bucket("leaks").unwrap();
            let tx = db.begin().unwrap();
            for key in 0..128 {
                assert_eq!(
                    tx.get(format!("key_{key:03}")).unwrap().slice(),
                    &[if key < 64 { 0 } else { 11 }; 512]
                );
            }
            drop(tx);
            drop(db);
            mace.start_gc();
            mace::testing::assert_persisted_gc_stats(&mace);
            if stat.active_elems == 0 {
                let path = if kind == FileKind::Data {
                    mace::Options::new(&*root).data_file(stat.file_id)
                } else {
                    mace::Options::new(&*root).blob_file(stat.file_id)
                };
                assert!(!path.exists(), "fully inactive victim was not reclaimed");
            }
        }
    }

    fn encoded_stat(opt: &Options, kind: FileKind, file: u64) -> Vec<u8> {
        let stat = stats(opt, kind)
            .into_iter()
            .find(|s| s.file_id == file)
            .unwrap();
        let mut bytes = vec![0; stat.packed_size()];
        stat.encode(&mut bytes);
        bytes
    }

    #[test]
    fn overmarked_files_are_never_repaired_even_with_missing_marks() {
        for kind in FileKind::ALL {
            let root = RandomPath::tmp();
            let (opt, id) = seed_junk(&root);
            let live = persisted_reachable_for_test(&opt, id);
            let set = if kind == FileKind::Data {
                live.0
            } else {
                live.1
            };
            let stat = stats(&opt, kind)
                .into_iter()
                .find(|s| s.active_elems > 0 && !s.inactive_elems.is_empty());
            let Some(stat) = stat else {
                panic!(
                    "mixed live/junk fixture required: {kind:?} {:?}",
                    stats(&opt, kind)
                        .iter()
                        .map(|s| (s.file_id, s.active_elems, s.total_elems))
                        .collect::<Vec<_>>()
                )
            };
            let path = if kind == FileKind::Data {
                opt.data_file(stat.file_id)
            } else {
                opt.blob_file(stat.file_id)
            };
            let mut reader = MetaReader::try_new(opt.fs(), &path).unwrap();
            let addr = reader
                .try_get_reloc()
                .unwrap()
                .iter()
                .find(|r| {
                    let addr = r.key;
                    set.contains(&addr)
                })
                .unwrap()
                .key;
            add_inactive_mark_for_leak_test(&opt, kind, stat.file_id, addr);
            let pure_before = encoded_stat(&opt, kind, stat.file_id);
            let pure = leak_check_database(&opt, true);
            assert!(pure.repairs.is_empty(), "{pure:?}");
            assert_eq!(pure_before, encoded_stat(&opt, kind, stat.file_id));
            assert!(
                pure.report
                    .diagnostics
                    .iter()
                    .any(|d| d.category == CheckCategory::BadStatAccounting)
            );
            let tree = BTree::open(opt.manifest()).unwrap();
            let mut encoded = vec![0; stat.packed_size()];
            stat.encode(&mut encoded);
            tree.exec(
                match kind {
                    FileKind::Data => BUCKET_DATA_STAT,
                    FileKind::Blob => BUCKET_BLOB_STAT,
                },
                |tx| tx.put(stat.file_id.to_le_bytes(), encoded.clone()),
            )
            .unwrap();
            drop(tree);
            drop_one_inactive_mark_for_leak_test(&opt, kind, stat.file_id);
            add_inactive_mark_for_leak_test(&opt, kind, stat.file_id, addr);
            let other = stats(&opt, kind)
                .into_iter()
                .find(|s| s.file_id != stat.file_id && !s.inactive_elems.is_empty())
                .unwrap();
            drop_one_inactive_mark_for_leak_test(&opt, kind, other.file_id);
            let before = encoded_stat(&opt, kind, stat.file_id);
            let result = leak_check_database(&opt, true);
            assert!(
                result.repairs.iter().any(|r| r.file_id == other.file_id),
                "{result:?}"
            );
            assert!(
                result
                    .report
                    .diagnostics
                    .iter()
                    .any(|d| d.category == CheckCategory::JunkLeak
                        && d.file_id == Some(stat.file_id)),
                "{result:?}"
            );
            assert!(result.repairs.iter().all(|r| r.file_id != stat.file_id));
            assert_eq!(before, encoded_stat(&opt, kind, stat.file_id));
        }
    }

    #[test]
    fn absent_or_null_fixed_root_refuses_all_repairs() {
        for null in [false, true] {
            let root = RandomPath::tmp();
            let (opt, id) = seed_junk(&root);
            let tree = BTree::open(opt.manifest()).unwrap();
            tree.exec(&page_table_name(id), |tx| {
                if null {
                    tx.put(ROOT_PID.to_le_bytes(), 0u64.to_le_bytes())
                } else {
                    tx.del(ROOT_PID.to_le_bytes()).map(|_| ())
                }
            })
            .unwrap();
            drop(tree);
            let before = fingerprint(&root);
            let result = leak_check_database(&opt, true);
            assert!(result.repairs.is_empty());
            assert!(
                result
                    .report
                    .diagnostics
                    .iter()
                    .any(|d| d.category == CheckCategory::LeakUndecidable),
                "{result:?}"
            );
            assert!(
                !result
                    .report
                    .diagnostics
                    .iter()
                    .any(|d| d.category == CheckCategory::JunkLeak)
            );
            assert_eq!(before, fingerprint(&root));
        }
    }
    #[test]
    fn disconnected_mapping_and_its_exclusive_blob_are_not_roots() {
        let root = RandomPath::tmp();
        let (opt, id) = seed_junk(&root);
        let original = persisted_reachable_for_test(&opt, id);
        let tree = BTree::open(opt.manifest()).unwrap();
        let mut rows = Vec::new();
        tree.view(&page_table_name(id), |tx| {
            let mut it = tx.iter();
            let (mut k, mut v) = (Vec::new(), Vec::new());
            while it.next_ref(&mut k, &mut v) {
                rows.push((
                    u64::from_le_bytes(k.as_slice().try_into().unwrap()),
                    u64::from_le_bytes(v.as_slice().try_into().unwrap()),
                ));
            }
            Ok(())
        })
        .unwrap();
        let old_root = rows.iter().find(|(pid, _)| *pid == ROOT_PID).unwrap().1;
        drop(tree);
        let mut candidates = Vec::new();
        for stat in stats(&opt, FileKind::Data) {
            for reloc in MetaReader::try_new(opt.fs(), opt.data_file(stat.file_id))
                .unwrap()
                .try_get_reloc()
                .unwrap()
                .iter()
            {
                let addr = reloc.key;
                if !original.0.contains(&addr) {
                    candidates.push(addr);
                }
            }
        }
        let (orphan_page, orphan_blob) = candidates
            .into_iter()
            .find_map(|addr| {
                let tree = BTree::open(opt.manifest()).unwrap();
                tree.exec(&page_table_name(id), |tx| {
                    tx.put(ROOT_PID.to_le_bytes(), addr.to_le_bytes())
                })
                .unwrap();
                drop(tree);
                let closure = leak_check_reachable_for_test(&opt, id);
                let tree = BTree::open(opt.manifest()).unwrap();
                tree.exec(&page_table_name(id), |tx| {
                    tx.put(ROOT_PID.to_le_bytes(), old_root.to_le_bytes())
                })
                .unwrap();
                closure
                    .1
                    .difference(&original.1)
                    .next()
                    .copied()
                    .map(|blob| (addr, blob))
            })
            .expect("fixture must retain an unreachable page with an exclusive blob");
        let orphan_pid = rows.iter().map(|(pid, _)| *pid).max().unwrap() + 1;
        let tree = BTree::open(opt.manifest()).unwrap();
        tree.exec(&page_table_name(id), |tx| {
            tx.put(orphan_pid.to_le_bytes(), orphan_page.to_le_bytes())
        })
        .unwrap();
        drop(tree);
        let typed = persisted_reachable_for_test(&opt, id);
        let raw = leak_check_reachable_for_test(&opt, id);
        assert_eq!(typed, raw);
        assert!(!typed.0.contains(&orphan_page));
        assert!(!raw.0.contains(&orphan_page));
        assert!(!typed.1.contains(&orphan_blob));
        assert!(!raw.1.contains(&orphan_blob));
        assert_ne!(orphan_pid, ROOT_PID);
        let before = fingerprint(&root);
        let result = leak_check_database(&opt, true);
        assert!(result.repairs.is_empty());
        assert!(
            !result
                .report
                .diagnostics
                .iter()
                .any(|d| d.category == CheckCategory::JunkLeak)
        );
        assert!(
            result
                .report
                .diagnostics
                .iter()
                .any(|d| d.category == CheckCategory::LeakUndecidable
                    && d.detail.contains(&format!("PID {orphan_pid}"))),
            "{result:?}"
        );
        assert_eq!(before, fingerprint(&root));
    }

    fn write_stat(opt: &Options, kind: FileKind, stat: &PersistStat) {
        let mut bytes = vec![0; stat.packed_size()];
        stat.encode(&mut bytes);
        let tree = BTree::open(opt.manifest()).unwrap();
        tree.exec(
            match kind {
                FileKind::Data => BUCKET_DATA_STAT,
                FileKind::Blob => BUCKET_BLOB_STAT,
            },
            |tx| tx.put(stat.file_id.to_le_bytes(), bytes.clone()),
        )
        .unwrap();
    }

    #[test]
    fn invalid_mask_and_count_exclude_only_the_file_and_keep_its_blob_edges() {
        for damage in ["mask", "count", "duplicate"] {
            let root = RandomPath::tmp();
            let (opt, id) = seed_junk(&root);
            let closure = persisted_reachable_for_test(&opt, id);
            let all = stats(&opt, FileKind::Data);
            let mut bad = all.iter().find(|s| s.active_elems > 0).unwrap().clone();
            let good = all
                .iter()
                .find(|s| s.file_id != bad.file_id && !s.inactive_elems.is_empty())
                .unwrap();
            drop_one_inactive_mark_for_leak_test(&opt, FileKind::Data, good.file_id);
            match damage {
                "mask" => bad.inactive_elems.push(bad.total_elems),
                "count" => bad.total_elems += 1,
                _ => {
                    let seq = *bad.inactive_elems.first().expect("mixed file");
                    bad.inactive_elems.push(seq);
                }
            }
            write_stat(&opt, FileKind::Data, &bad);
            assert_eq!(closure, leak_check_reachable_for_test(&opt, id));
            let result = leak_check_database(&opt, true);
            assert!(result.fault.is_none(), "{result:?}");
            assert!(
                result
                    .report
                    .diagnostics
                    .iter()
                    .any(|d| d.category == CheckCategory::BadStatAccounting
                        && d.file_id == Some(bad.file_id)),
                "{result:?}"
            );
            assert!(
                !result.report.diagnostics.iter().any(|d| matches!(
                    d.category,
                    CheckCategory::JunkLeak | CheckCategory::LeakUndecidable
                )),
                "{result:?}"
            );
            assert_eq!(result.repairs.len(), 1, "{result:?}");
            assert_eq!(result.repairs[0].file_id, good.file_id);
        }
    }

    #[test]
    fn unknown_stat_ownership_blocks_all_buckets_and_missing_payload_blocks_the_group() {
        for damage in ["header", "payload"] {
            let root = RandomPath::tmp();
            let (opt, _) = seed_junk(&root);
            let all = stats(&opt, FileKind::Data);
            let stat = &all[0];
            if damage == "header" {
                let tree = BTree::open(opt.manifest()).unwrap();
                tree.exec(BUCKET_DATA_STAT, |tx| {
                    tx.put(stat.file_id.to_le_bytes(), vec![0; 3])
                })
                .unwrap();
            } else {
                std::fs::remove_file(opt.data_file(stat.file_id)).unwrap();
            }
            let before = fingerprint(&root);
            let result = leak_check_database(&opt, true);
            assert!(result.repairs.is_empty(), "{result:?}");
            assert!(
                result
                    .report
                    .diagnostics
                    .iter()
                    .any(|d| d.category == CheckCategory::LeakUndecidable)
            );
            assert!(
                !result
                    .report
                    .diagnostics
                    .iter()
                    .any(|d| d.category == CheckCategory::JunkLeak)
            );
            assert_eq!(before, fingerprint(&root));
        }
    }

    #[test]
    fn crash_frontier_child() {
        let Ok(root) = std::env::var("MACE_LEAK_CRASH_ROOT") else {
            return;
        };
        let covered = std::env::var("MACE_LEAK_COVERED").unwrap() == "true";
        let mut opt = Options::new(root);
        opt.sync_on_write = true;
        opt.gc_timeout = 0;
        let mace = Mace::new(opt.validate().unwrap()).unwrap();
        let db = mace.open_bucket("leaks").unwrap();
        let _old = db.view().unwrap();
        let tx = db.begin().unwrap();
        tx.upsert("crash_key", vec![99; 512]).unwrap();
        tx.commit().unwrap();
        if covered {
            db.checkpoint_and_wait();
        }
        std::process::abort();
    }

    #[test]
    fn redo_beyond_frontier_is_rejected_but_covered_crash_is_decidable() {
        for covered in [false, true] {
            let root = RandomPath::tmp();
            let (opt, _) = seed_junk(&root);
            let status = common::child_test_command(&std::env::current_exe().unwrap())
                .args(["--exact", "leaks::crash_frontier_child", "--nocapture"])
                .env("MACE_LEAK_CRASH_ROOT", root.to_string_lossy().as_ref())
                .env("MACE_LEAK_COVERED", covered.to_string())
                .status()
                .unwrap();
            assert!(!status.success());
            let before = fingerprint(&root);
            let result = leak_check_database(&opt, true);
            assert!(result.repairs.is_empty(), "{result:?}");
            assert_eq!(before, fingerprint(&root));
            if covered {
                assert!(result.report.is_ok(), "{result:?}");
            } else {
                assert!(
                    result
                        .report
                        .diagnostics
                        .iter()
                        .any(|d| d.category == CheckCategory::LeakUndecidable
                            && d.detail.contains("frontier")),
                    "{result:?}"
                );
            }
            if covered {
                let stat = stats(&opt, FileKind::Data)
                    .into_iter()
                    .find(|s| !s.inactive_elems.is_empty())
                    .unwrap();
                drop_one_inactive_mark_for_leak_test(&opt, FileKind::Data, stat.file_id);
                let repaired = leak_check_database(&opt, true);
                assert!(repaired.report.is_ok(), "{repaired:?}");
                assert_eq!(repaired.repairs.len(), 1);
            }
            let mace = Mace::new(opt.clone().validate().unwrap()).unwrap();
            let db = mace.open_bucket("leaks").unwrap();
            let tx = db.begin().unwrap();
            for key in 0..128 {
                let round = if key < 64 { 0 } else { 11 };
                assert_eq!(
                    tx.get(format!("key_{key:03}")).unwrap().slice(),
                    &[round; 512]
                );
            }
            assert_eq!(tx.get("crash_key").unwrap().slice(), &[99; 512]);
            drop(tx);
            drop(db);
            drop(mace);
            assert!(leak_check_database(&opt, false).report.is_ok());
        }
    }

    fn edit_relocations(path: &Path, edit: impl FnOnce(&mut [u8], usize, usize)) {
        let mut bytes = std::fs::read(path).unwrap();
        let footer = bytes.len() - 24;
        let count = u32::from_le_bytes(bytes[footer + 8..footer + 12].try_into().unwrap()) as usize;
        let table = footer - count * 32;
        edit(&mut bytes, table, count);
        let crc = crc32c::crc32c(&bytes[table..footer]);
        bytes[footer + 16..footer + 20].copy_from_slice(&crc.to_le_bytes());
        std::fs::write(path, bytes).unwrap();
    }

    #[test]
    fn invalid_sequences_exclude_one_file_but_duplicate_addresses_block_the_group() {
        for damage in [
            "seq_duplicate",
            "seq_range",
            "addr_duplicate",
            "missing_ref",
        ] {
            let root = RandomPath::tmp();
            let (opt, _) = seed_junk(&root);
            let all = stats(&opt, FileKind::Data);
            let bad = all
                .iter()
                .find(|s| s.active_elems > 0 && s.total_elems > 1)
                .unwrap();
            let good = all
                .iter()
                .find(|s| s.file_id != bad.file_id && !s.inactive_elems.is_empty())
                .unwrap();
            drop_one_inactive_mark_for_leak_test(&opt, FileKind::Data, good.file_id);
            edit_relocations(&opt.data_file(bad.file_id), |bytes, table, count| {
                assert!(count > 1);
                if damage == "seq_duplicate" {
                    let seq = bytes[table + 24..table + 28].to_vec();
                    bytes[table + 56..table + 60].copy_from_slice(&seq);
                } else if damage == "seq_range" {
                    bytes[table + 24..table + 28].copy_from_slice(&(count as u32).to_le_bytes());
                } else if damage == "addr_duplicate" {
                    let addr = bytes[table..table + 8].to_vec();
                    bytes[table + 32..table + 40].copy_from_slice(&addr);
                } else {
                    for i in 0..count {
                        bytes[table + i * 32..table + i * 32 + 8]
                            .copy_from_slice(&(u64::MAX - i as u64).to_le_bytes());
                    }
                }
            });
            let result = leak_check_database(&opt, true);
            if damage.starts_with("seq") {
                assert_eq!(result.repairs.len(), 1, "{result:?}");
                assert_eq!(result.repairs[0].file_id, good.file_id);
                assert!(
                    result
                        .report
                        .diagnostics
                        .iter()
                        .any(|d| d.category == CheckCategory::BadStatAccounting
                            && d.file_id == Some(bad.file_id))
                );
            } else {
                assert!(result.repairs.is_empty(), "{result:?}");
                assert!(
                    result
                        .report
                        .diagnostics
                        .iter()
                        .any(|d| d.category == CheckCategory::LeakUndecidable)
                );
                assert!(
                    !result
                        .report
                        .diagnostics
                        .iter()
                        .any(|d| d.category == CheckCategory::JunkLeak)
                );
            }
        }
    }

    #[test]
    fn recycle_boundary_mismatch_blocks_leak_repairs() {
        use mace::maintenance::{BUCKET_MISC, IAsSlice, WalRecycleState, wal_recycle_key};

        let root = RandomPath::tmp();
        let (opt, bucket_id) = seed_junk(&root);
        let stat = stats(&opt, FileKind::Data)
            .into_iter()
            .find(|stat| !stat.inactive_elems.is_empty())
            .expect("inactive fixture");
        drop_one_inactive_mark_for_leak_test(&opt, FileKind::Data, stat.file_id);

        let segment_ids: Vec<_> = std::fs::read_dir(opt.log_root())
            .expect("log root")
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| {
                let path = entry.path();
                path.file_name()
                    .and_then(|name| name.to_str())
                    .and_then(|name| name.strip_prefix("group_wal_"))
                    .and_then(|raw| raw.parse::<u64>().ok())
                    .map(|id| (id, path))
            })
            .collect();
        let (oldest, oldest_path) = segment_ids
            .iter()
            .min_by_key(|(id, _)| id)
            .expect("fixture must write a shared segment");
        let newest = segment_ids.iter().map(|(id, _)| id).max().unwrap();
        let tree = BTree::open(opt.manifest()).unwrap();
        tree.exec(BUCKET_MISC, |tx| {
            tx.put(
                wal_recycle_key(Options::SHARED_ID),
                WalRecycleState::done(Options::SHARED_ID, *oldest, *oldest).as_slice(),
            )
        })
        .unwrap();
        drop(tree);
        std::fs::remove_file(oldest_path).expect("remove oldest segment");
        std::fs::write(opt.group_wal_file(*newest + 1), []).expect("leave a later empty segment");

        let before = fingerprint(&root);
        let result = leak_check_database(&opt, true);
        assert!(result.repairs.is_empty(), "{result:?}");
        assert!(
            result.report.diagnostics.iter().any(|diagnostic| {
                diagnostic.category == CheckCategory::LeakUndecidable
                    && diagnostic.detail.contains("lifecycle or WAL inventory")
                    && diagnostic.bucket_id == Some(bucket_id)
            }),
            "{result:?}"
        );
        assert_eq!(before, fingerprint(&root));
    }

    #[test]
    fn collectors_follow_sibling_and_merging_pids_without_branch_edges() {
        for field in [8usize, 16] {
            let root = RandomPath::tmp();
            let (opt, id) = seed_junk(&root);
            let tree = BTree::open(opt.manifest()).unwrap();
            let mut rows = Vec::new();
            tree.view(&page_table_name(id), |tx| {
                let mut it = tx.iter();
                let (mut k, mut v) = (Vec::new(), Vec::new());
                while it.next_ref(&mut k, &mut v) {
                    rows.push((
                        u64::from_le_bytes(k.as_slice().try_into().unwrap()),
                        u64::from_le_bytes(v.as_slice().try_into().unwrap()),
                    ));
                }
                Ok(())
            })
            .unwrap();
            drop(tree);
            let mut leaves = Vec::new();
            for stat in stats(&opt, FileKind::Data) {
                let path = opt.data_file(stat.file_id);
                let bytes = std::fs::read(&path).unwrap();
                let relocs = MetaReader::try_new(opt.fs(), &path)
                    .unwrap()
                    .try_get_reloc()
                    .unwrap();
                for row in rows.iter().filter(|(pid, _)| *pid != ROOT_PID) {
                    if let Some(r) = relocs.iter().find(|r| r.key == row.1) {
                        assert!(!r.val.is_compressed());
                        if bytes[r.val.off] == 2 && bytes[r.val.off + 1] == 1 {
                            leaves.push((*row, stat.file_id, r.val.off));
                        }
                    }
                }
            }
            assert!(leaves.len() >= 2, "two mapped base leaves required");
            let ((_, from), _, _) = leaves[0];
            let ((pid, to), _, _) = leaves[1];
            for (source, item) in [leaves[0], leaves[1]].into_iter().enumerate() {
                let ((_, addr), file, off) = item;
                edit_relocations(&opt.data_file(file), |bytes, table, count| {
                    bytes[off + 36..off + 44].fill(0);
                    let base = off + mace::maintenance::BoxRef::DUMP_HDR_LEN;
                    bytes[base + 8..base + 24].fill(0);
                    if source == 0 {
                        bytes[base + field..base + field + 8].copy_from_slice(&pid.to_le_bytes());
                    }
                    for i in 0..count {
                        let r = table + i * 32;
                        if u64::from_le_bytes(bytes[r..r + 8].try_into().unwrap()) == addr {
                            let len = u32::from_le_bytes(bytes[r + 16..r + 20].try_into().unwrap())
                                as usize;
                            let crc = crc32c::crc32c(&bytes[off..off + len]);
                            bytes[r + 28..r + 32].copy_from_slice(&crc.to_le_bytes());
                        }
                    }
                });
            }
            let tree = BTree::open(opt.manifest()).unwrap();
            tree.exec(&page_table_name(id), |tx| {
                for (other, _) in &rows {
                    if *other != ROOT_PID && *other != pid {
                        tx.del(other.to_le_bytes())?;
                    }
                }
                tx.put(ROOT_PID.to_le_bytes(), from.to_le_bytes())
            })
            .unwrap();
            drop(tree);
            let before = fingerprint(&root);
            let raw = leak_check_reachable_for_test(&opt, id);
            let typed = persisted_reachable_for_test(&opt, id);
            assert_eq!(raw, typed);
            assert!(raw.0.contains(&from));
            assert!(raw.0.contains(&to));
            assert!(!raw.1.is_empty());
            assert_eq!(before, fingerprint(&root));
            let fixed = leak_check_database(&opt, true);
            assert!(fixed.report.is_ok(), "{fixed:?}");
        }
    }

    #[test]
    fn repaired_partial_live_files_are_rewritten_without_losing_fixed_views() {
        for kind in FileKind::ALL {
            let root = RandomPath::tmp();
            let (mut opt, _) = seed_junk(&root);
            let target = stats(&opt, kind)
                .into_iter()
                .find(|s| s.active_elems > 0 && !s.inactive_elems.is_empty())
                .expect("partial live victim");
            let source_path = if kind == FileKind::Data {
                opt.data_file(target.file_id)
            } else {
                opt.blob_file(target.file_id)
            };
            let survivors: std::collections::BTreeSet<_> =
                MetaReader::try_new(opt.fs(), source_path)
                    .unwrap()
                    .try_get_reloc()
                    .unwrap()
                    .iter()
                    .filter(|r| {
                        let seq = r.val.seq;
                        !target.inactive_elems.contains(&seq)
                    })
                    .map(|r| r.key)
                    .collect();
            let (leaked, _) = drop_one_inactive_mark_for_leak_test(&opt, kind, target.file_id);
            let fixed = leak_check_database(&opt, true);
            assert!(fixed.report.is_ok(), "{fixed:?}");
            assert!(fixed.repairs[0].active_elems.1 > 0);
            opt.gc_eager = true;
            opt.data_garbage_ratio = 1;
            opt.blob_garbage_ratio = 1;
            if kind == FileKind::Data {
                opt.data_file_size = target.active_size;
            } else {
                opt.blob_file_size = target.active_size;
            }
            let mace = Mace::new(opt.clone().validate().unwrap()).unwrap();
            let db = mace.open_bucket("leaks").unwrap();
            let old = db.view().unwrap();
            assert_eq!(old.get("key_000").unwrap().slice(), &[0; 512]);
            let tx = db.begin().unwrap();
            tx.upsert("key_127", vec![42; 512]).unwrap();
            tx.commit().unwrap();
            db.checkpoint_and_wait();
            mace.start_gc();
            assert_eq!(old.get("key_127").unwrap().slice(), &[11; 512]);
            let tx = db.begin().unwrap();
            assert_eq!(tx.get("key_127").unwrap().slice(), &[42; 512]);
            drop(tx);
            drop(old);
            db.checkpoint_and_wait();
            mace.start_gc();
            mace::testing::assert_persisted_gc_stats(&mace);
            drop(db);
            drop(mace);
            let path = if kind == FileKind::Data {
                opt.data_file(target.file_id)
            } else {
                opt.blob_file(target.file_id)
            };
            assert!(
                !path.exists(),
                "target victim was not physically collected: {}",
                path.display()
            );
            let mut copied_survivors = std::collections::BTreeSet::new();
            for stat in stats(&opt, kind) {
                let path = if kind == FileKind::Data {
                    opt.data_file(stat.file_id)
                } else {
                    opt.blob_file(stat.file_id)
                };
                let relocs = MetaReader::try_new(opt.fs(), path)
                    .unwrap()
                    .try_get_reloc()
                    .unwrap();
                copied_survivors.extend(
                    relocs
                        .iter()
                        .map(|r| r.key)
                        .filter(|addr| survivors.contains(addr)),
                );
                assert!(
                    relocs.iter().all(|r| r.key != leaked),
                    "replacement copied leaked address"
                );
            }
            assert!(
                survivors.is_subset(&copied_survivors),
                "replacement lost target survivors: missing {:?}",
                survivors.difference(&copied_survivors).collect::<Vec<_>>()
            );
            assert!(leak_check_database(&opt, false).report.is_ok());
        }
    }
    #[test]
    fn unsupported_engine_version_prevents_repair_of_real_missing_marks() {
        let root = RandomPath::tmp();
        let (opt, _) = seed_junk(&root);
        let stat = stats(&opt, FileKind::Data)
            .into_iter()
            .find(|s| !s.inactive_elems.is_empty())
            .unwrap();
        drop_one_inactive_mark_for_leak_test(&opt, FileKind::Data, stat.file_id);
        let tree = BTree::open(opt.manifest()).unwrap();
        tree.exec(mace::maintenance::BUCKET_VERSION, |tx| {
            tx.put(mace::maintenance::VERSION_KEY, 0u64.to_le_bytes())
        })
        .unwrap();
        drop(tree);
        let before = fingerprint(&root);
        let report = leak_check_database(&opt, true);
        assert!(report.repairs.is_empty());
        assert!(
            report
                .report
                .diagnostics
                .iter()
                .any(|d| d.category == CheckCategory::BadVersion)
        );
        assert!(
            !report
                .report
                .diagnostics
                .iter()
                .any(|d| d.category == CheckCategory::JunkLeak)
        );
        assert_eq!(before, fingerprint(&root));
    }
}
