//! Model-based fuzz for merge semantics: commutative counter operands folded
//! onto bases/absence across checkpoints and reopens.
//!
//! The model is the logical value (BTreeMap<String, u64>); every committed
//! mutation must be observable through point get and a full scan, and the fold
//! result must survive materializing compacts, checkpoints, and reopens
//! (frontier absorption vs WAL redo: no double-add, no lost operand).
#![no_main]

mod common;

use common::{ByteStream, FuzzDbRoot, key_name, open_engine};
use libfuzzer_sys::fuzz_target;
use mace::{Bucket, BucketOptions, Mace, MergeOperator, OpCode, Options};
use std::collections::BTreeMap;
use std::sync::Arc;

/// counter operator: operand is a little-endian u64 delta (same algebra as
/// tests/merge.rs AddOp); total, commutative, associative
#[derive(Default)]
struct AddOp;

impl MergeOperator for AddOp {
    fn combine_operands(&self, _key: &[u8], left: &[u8], right: &[u8]) -> Vec<u8> {
        decode_u64(left).wrapping_add(decode_u64(right)).to_le_bytes().to_vec()
    }

    fn apply(&self, _key: &[u8], base: Option<&[u8]>, operand: &[u8]) -> Option<Vec<u8>> {
        let o = decode_u64(operand);
        Some(match base {
            None => o.to_le_bytes().to_vec(),
            Some(b) => decode_u64(b).wrapping_add(o).to_le_bytes().to_vec(),
        })
    }
}

fn decode_u64(raw: &[u8]) -> u64 {
    u64::from_le_bytes(raw.try_into().expect("u64 value"))
}

/// opens or creates the merge bucket, always providing the runtime operator
/// (a fresh open without it cannot interpret operands); the same Arc is reused
/// across reopens so the frozen registration accepts it
fn open_merge_bucket(mace: &Mace, name: &str, operator: &Arc<dyn MergeOperator>) -> Bucket {
    let opt = BucketOptions {
        merge_operator: Some(operator.clone()),
        ..BucketOptions::default()
    };
    match mace.open_bucket_with_options(name, opt.clone()) {
        Ok(bucket) => bucket,
        Err(OpCode::NotFound) => mace.new_bucket(name, opt).expect("create bucket failed"),
        Err(err) => panic!("open bucket failed: {err:?}"),
    }
}

/// verifies every modeled key through point get AND a full scan, and that no
/// phantom key (e.g. a fold-deleted key) appears in the scan
fn assert_merge_model(bucket: &Bucket, model: &BTreeMap<String, u64>) {
    let view = bucket.view().expect("open view failed");
    for (key, expected) in model {
        let actual = view.get(key).expect("expected key visible");
        let got = decode_u64(actual.slice());
        assert_eq!(got, *expected, "point get fold mismatch for {key}");
    }
    let mut iter = view.seek("k");
    while let Some(item) = iter.try_next().expect("scan failed") {
        let key = String::from_utf8(item.key().to_vec()).expect("utf8 key");
        let expected = model.get(&key).expect("phantom key in scan");
        let got = decode_u64(item.val());
        assert_eq!(got, *expected, "scan fold mismatch for {key}");
    }
}

#[derive(Clone)]
enum PendingOp {
    Put(String, u64),
    Update(String, u64),
    Merge(String, u64),
    Del(String),
}

/// shadow application in commit order, mirroring the engine fold semantics:
/// put is a base barrier, merge accumulates onto the current value (or starts
/// from zero after a delete/absence), del removes the key
fn apply_pending_shadow(
    model: &BTreeMap<String, u64>,
    pending: &[PendingOp],
) -> BTreeMap<String, u64> {
    let mut shadow = model.clone();
    for op in pending {
        match op {
            PendingOp::Put(key, value) | PendingOp::Update(key, value) => {
                shadow.insert(key.clone(), *value);
            }
            PendingOp::Merge(key, delta) => {
                let entry = shadow.entry(key.clone()).or_insert(0);
                *entry = entry.wrapping_add(*delta);
            }
            PendingOp::Del(key) => {
                shadow.remove(key);
            }
        }
    }
    shadow
}

/// applies the batch inside one txn; returns the first non-transient error so
/// the caller can abort the batch (the merge admission budget can return
/// OpCode::Again under page relocation — a legal engine response, not a bug)
fn apply_batch(bucket: &Bucket, pending: &[PendingOp], commit: bool) -> Result<(), OpCode> {
    let txn = bucket.begin().expect("begin txn failed");
    for op in pending {
        let res = match op {
            PendingOp::Put(key, value) => txn.put(key, value.to_le_bytes()),
            PendingOp::Update(key, value) => txn.update(key, value.to_le_bytes()),
            PendingOp::Merge(key, delta) => txn.merge(key, delta.to_le_bytes()),
            PendingOp::Del(key) => match txn.del(key) {
                Ok(()) => Ok(()),
                Err(OpCode::NotFound) => Ok(()),
                Err(err) => Err(err),
            },
        };
        match res {
            Ok(()) => {}
            Err(OpCode::Again) => {
                drop(txn);
                return Err(OpCode::Again);
            }
            Err(err) => panic!("write failed: {err:?}"),
        }
    }
    if commit {
        txn.commit().expect("commit failed");
    } else {
        drop(txn);
    }
    Ok(())
}

fn run_pending(bucket: &Bucket, pending: &[PendingOp]) {
    let _ = apply_batch(bucket, pending, false);
}

fn commit_pending(bucket: &Bucket, model: &mut BTreeMap<String, u64>, pending: &mut Vec<PendingOp>) {
    if pending.is_empty() {
        return;
    }
    if let Err(OpCode::Again) = apply_batch(bucket, pending, true) {
        // the admission budget was exhausted: the batch never committed, the
        // model is unchanged
        pending.clear();
        return;
    }
    *model = apply_pending_shadow(model, pending);
    pending.clear();
}

fn drop_pending(bucket: &Bucket, pending: &mut Vec<PendingOp>) {
    if pending.is_empty() {
        return;
    }
    run_pending(bucket, pending);
    pending.clear();
}

fn tune(opts: &mut Options) {
    opts.concurrent_write = 1;
    opts.data_file_size = 32 << 10;
    opts.wal_file_size = 16 << 10;
    opts.max_ckpt_per_txn = 64;
}

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }

    let operator: Arc<dyn MergeOperator> = Arc::new(AddOp);
    let db_root = FuzzDbRoot::new();
    let mut mace = open_engine(db_root.path(), tune);
    let mut bucket = open_merge_bucket(&mace, "main", &operator);
    let mut model = BTreeMap::<String, u64>::new();
    let mut pending = Vec::<PendingOp>::new();
    let mut stream = ByteStream::new(data);
    let max_steps = 128usize;

    for _ in 0..max_steps {
        let Some(tag) = stream.next() else {
            break;
        };
        let key = key_name((tag as usize) % 8);
        let delta = tag as u64;
        match tag % 8 {
            // discard the pending batch without writing
            0 => pending.clear(),
            // accumulate one more op into the pending batch
            1 => {
                if pending.len() < 8 {
                    let shadow = apply_pending_shadow(&model, &pending);
                    let exists = shadow.contains_key(&key);
                    match (tag % 4, exists) {
                        // put is insert-only: only valid for absent keys
                        (0, false) => pending.push(PendingOp::Put(key, delta)),
                        // update overwrites an existing value
                        (1, true) => pending.push(PendingOp::Update(key, delta)),
                        // merge accumulates on the current value or from zero
                        (2, _) => pending.push(PendingOp::Merge(key, delta)),
                        // del removes an existing value
                        (_, true) => pending.push(PendingOp::Del(key)),
                        // nothing valid for this selector; try merge instead
                        (_, false) => pending.push(PendingOp::Merge(key, delta)),
                    }
                }
            }
            // commit the batch
            2 => commit_pending(&bucket, &mut model, &mut pending),
            // abort the batch
            3 => drop_pending(&bucket, &mut pending),
            // commit + checkpoint (drives materializing compacts)
            4 => {
                commit_pending(&bucket, &mut model, &mut pending);
                bucket.checkpoint();
            }
            // commit + sync (durable WAL cut)
            5 => {
                commit_pending(&bucket, &mut model, &mut pending);
                mace.sync().expect("sync failed");
            }
            // commit + shutdown/reopen: the fold result must survive the
            // frontier gate across a fresh open (no double-add from redo, no
            // lost operand); a clean shutdown exercises the durable closure,
            // while torn-WAL crash coverage lives in crash_reopen
            6 => {
                commit_pending(&bucket, &mut model, &mut pending);
                drop(bucket);
                drop(mace);
                mace = open_engine(db_root.path(), tune);
                bucket = open_merge_bucket(&mace, "main", &operator);
                assert_merge_model(&bucket, &model);
            }
            // commit + full verification
            _ => {
                commit_pending(&bucket, &mut model, &mut pending);
                assert_merge_model(&bucket, &model);
            }
        }
        if stream.is_empty() {
            break;
        }
    }

    drop_pending(&bucket, &mut pending);
    assert_merge_model(&bucket, &model);
    bucket.checkpoint();
    drop(bucket);
    drop(mace);

    let reopened = open_engine(db_root.path(), tune);
    let bucket = open_merge_bucket(&reopened, "main", &operator);
    assert_merge_model(&bucket, &model);
});
