//! Offline checker acceptance tests.

mod common;

use btree_store::BTree;
use mace::{CheckCategory, Mace, OpCode, Options, RandomPath, check_database};
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
