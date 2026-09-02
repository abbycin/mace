//! Focused checks for the merge design:
//! per-bucket runtime operator binding, persistence boundary, and
//! read-surface gating of recovered merge records.
mod common;

use common::TestEnv;
use mace::{BucketOptions, MergeOperator, OpCode};
use std::sync::{Arc, Barrier};

/// genuine counter operator: operand is a little-endian u64 delta
#[derive(Default)]
struct AddOp;

impl MergeOperator for AddOp {
    fn combine_operands(&self, _key: &[u8], left: &[u8], right: &[u8]) -> Vec<u8> {
        let (l, r) = (decode_u64(left).unwrap(), decode_u64(right).unwrap());
        l.wrapping_add(r).to_le_bytes().to_vec()
    }

    fn apply(&self, _key: &[u8], base: Option<&[u8]>, operand: &[u8]) -> Option<Vec<u8>> {
        let o = decode_u64(operand).unwrap();
        Some(match base {
            None => o.to_le_bytes().to_vec(),
            Some(b) => decode_u64(b)
                .unwrap()
                .wrapping_add(o)
                .to_le_bytes()
                .to_vec(),
        })
    }
}

fn decode_u64(raw: &[u8]) -> Result<u64, OpCode> {
    raw.try_into()
        .map(u64::from_le_bytes)
        .map_err(|_| OpCode::Invalid)
}

fn op(_id: u8) -> Option<Arc<dyn MergeOperator>> {
    Some(Arc::new(AddOp))
}

/// the default public U64AddOperator is usable from the crate root
#[test]
fn default_u64_add_operator_is_public_and_usable() {
    use mace::U64AddOperator;
    let env = TestEnv::default();
    let mace = env.open_default().expect("open must succeed");
    let db = mace
        .new_bucket(
            "u",
            BucketOptions {
                merge_operator: Some(Arc::new(U64AddOperator)),
                ..BucketOptions::default()
            },
        )
        .unwrap();
    let kv = db.begin().unwrap();
    kv.put("k", 10u64.to_le_bytes()).unwrap();
    kv.commit().unwrap();
    let kv = db.begin().unwrap();
    kv.merge("k", 5u64.to_le_bytes()).unwrap();
    kv.commit().unwrap();
    assert_eq!(fold_value(&db, b"k"), 15);
}

/// counter operator that deletes the key when the accumulated value saturates:
/// legal, deterministic, commutative, associative, and exercises Ok(None) apply results
#[derive(Default)]
struct SatAddOp;

impl MergeOperator for SatAddOp {
    fn combine_operands(&self, _key: &[u8], left: &[u8], right: &[u8]) -> Vec<u8> {
        let (l, r) = (decode_u64(left).unwrap(), decode_u64(right).unwrap());
        l.wrapping_add(r).to_le_bytes().to_vec()
    }

    fn apply(&self, _key: &[u8], base: Option<&[u8]>, operand: &[u8]) -> Option<Vec<u8>> {
        let o = decode_u64(operand).unwrap();
        let v = match base {
            None => o,
            Some(b) => decode_u64(b).unwrap().wrapping_add(o),
        };
        if v == u64::MAX {
            None
        } else {
            Some(v.to_le_bytes().to_vec())
        }
    }
}

#[test]
fn buckets_keep_independent_merge_operators() {
    let env = TestEnv::default();
    let mace = env.open_default().expect("open must succeed");

    let a = BucketOptions {
        merge_operator: op(1),
        ..BucketOptions::default()
    };
    let b = BucketOptions {
        merge_operator: op(2),
        ..BucketOptions::default()
    };

    mace.new_bucket("a", a.clone()).expect("bucket a");
    mace.new_bucket("b", b.clone()).expect("bucket b");

    // the same Arc is compatible with the operator fixed at context load
    mace.open_bucket_with_options("a", a)
        .expect("same operator must be accepted");
    mace.open_bucket_with_options("b", b)
        .expect("same operator must be accepted");

    // a different implementation conflicts with the frozen registration
    let other = BucketOptions {
        merge_operator: op(3),
        ..BucketOptions::default()
    };

    assert!(
        matches!(
            mace.open_bucket_with_options("a", other),
            Err(OpCode::Invalid)
        ),
        "conflicting operator must be rejected while the context lives"
    );
    // plain open without options keeps the existing registration untouched
    mace.open_bucket("a").expect("plain open must succeed");
}
#[test]
fn concurrent_conflicting_loads_admit_exactly_one() {
    let env = TestEnv::default();
    let mace = env.open_default().expect("open must succeed");
    mace.new_bucket("m", BucketOptions::default())
        .expect("create bucket");
    mace.drop_bucket("m").expect("unload bucket");

    let outcomes: Vec<std::sync::atomic::AtomicUsize> = (0..2)
        .map(|_| std::sync::atomic::AtomicUsize::new(0))
        .collect();
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let mace_a = mace.clone();
    let mace_b = mace.clone();
    let barrier_a = barrier.clone();
    let barrier_b = barrier.clone();
    let o0 = &outcomes[0];
    let o1 = &outcomes[1];
    std::thread::scope(|s| {
        s.spawn(move || {
            let opts = BucketOptions {
                merge_operator: op(21),
                ..BucketOptions::default()
            };
            barrier_a.wait();
            o0.store(
                match mace_a.open_bucket_with_options("m", opts) {
                    Ok(_) => 1,
                    Err(OpCode::Invalid) => 2,
                    Err(_) => 3,
                },
                std::sync::atomic::Ordering::Relaxed,
            );
        });
        s.spawn(move || {
            let opts = BucketOptions {
                merge_operator: op(22),
                ..BucketOptions::default()
            };
            barrier_b.wait();
            o1.store(
                match mace_b.open_bucket_with_options("m", opts) {
                    Ok(_) => 1,
                    Err(OpCode::Invalid) => 2,
                    Err(_) => 3,
                },
                std::sync::atomic::Ordering::Relaxed,
            );
        });
    });
    // exactly one serialized load fixes its operator; the loser must see Invalid
    let mut results = [
        o0.load(std::sync::atomic::Ordering::Relaxed),
        o1.load(std::sync::atomic::Ordering::Relaxed),
    ];
    results.sort();
    assert_eq!(
        results,
        [1, 2],
        "conflicting concurrent loads must yield one success and one Invalid"
    );
}

#[test]
fn unloaded_bucket_requires_operator_again() {
    let env = TestEnv::default();
    let mace = env.open_default().expect("open must succeed");

    let opts = BucketOptions {
        merge_operator: op(7),
        ..BucketOptions::default()
    };
    mace.new_bucket("m", opts).expect("create bucket");
    mace.drop_bucket("m").expect("unload must succeed");

    // after unload the context is gone: any fresh operator is accepted,
    // and it is frozen again for the new context lifetime
    let fresh = BucketOptions {
        merge_operator: op(7),
        ..BucketOptions::default()
    };
    mace.open_bucket_with_options("m", fresh)
        .expect("re-register after unload must succeed");

    let conflicting = BucketOptions {
        merge_operator: op(8),
        ..BucketOptions::default()
    };
    assert!(
        matches!(
            mace.open_bucket_with_options("m", conflicting),
            Err(OpCode::Invalid)
        ),
        "the new context must be frozen to the first registered operator"
    );
}

fn open_counter_bucket(env: &TestEnv, name: &str, with_op: bool) -> mace::Bucket {
    let mace = env.open_default().expect("open must succeed");
    let mut opts = BucketOptions::default();
    if with_op {
        opts.merge_operator = op(1);
    }
    mace.new_bucket(name, opts).expect("create bucket")
}

#[test]
fn merge_folds_against_base_and_tombstone_barriers() {
    let env = TestEnv::default();
    let db = open_counter_bucket(&env, "m", true);

    // base 100 then +2 +3 => 105
    let kv = db.begin().unwrap();
    kv.put("k", 100u64.to_le_bytes()).unwrap();
    kv.commit().unwrap();
    let kv = db.begin().unwrap();
    kv.merge("k", 2u64.to_le_bytes()).unwrap();
    kv.merge("k", 3u64.to_le_bytes()).unwrap();
    kv.commit().unwrap();
    let v = db.view().unwrap().get("k").unwrap();
    assert_eq!(v.slice(), 105u64.to_le_bytes());

    // delete barrier: apply(None, +4) => 4
    let kv = db.begin().unwrap();
    kv.del("k").unwrap();
    kv.commit().unwrap();
    assert!(db.view().unwrap().get("k").is_err());
    let kv = db.begin().unwrap();
    kv.merge("k", 4u64.to_le_bytes()).unwrap();
    kv.commit().unwrap();
    let v = db.view().unwrap().get("k").unwrap();
    assert_eq!(v.slice(), 4u64.to_le_bytes());

    // fresh key without any base: apply(None, +7) => 7
    let kv = db.begin().unwrap();
    kv.merge("fresh", 7u64.to_le_bytes()).unwrap();
    kv.commit().unwrap();
    let v = db.view().unwrap().get("fresh").unwrap();
    assert_eq!(v.slice(), 7u64.to_le_bytes());
}

#[test]
fn uncommitted_merge_is_invisible_to_other_snapshots() {
    let env = TestEnv::default();
    let db = open_counter_bucket(&env, "m", true);
    let kv = db.begin().unwrap();
    kv.put("k", 10u64.to_le_bytes()).unwrap();
    kv.commit().unwrap();

    let kv = db.begin().unwrap();
    kv.merge("k", 5u64.to_le_bytes()).unwrap();
    // drop without commit aborts the txn: the operand must vanish
    drop(kv);
    let v = db.view().unwrap().get("k").unwrap();
    assert_eq!(v.slice(), 10u64.to_le_bytes());
}

#[test]
fn concurrent_merge_writers_both_commit_and_sum() {
    let env = TestEnv::default();
    let db = open_counter_bucket(&env, "m", true);
    let kv = db.begin().unwrap();
    kv.put("k", 0u64.to_le_bytes()).unwrap();
    kv.commit().unwrap();

    let barrier_a = Arc::new(Barrier::new(2));
    let barrier_b = Arc::new(Barrier::new(2));
    let (d1, d2) = (db.clone(), db.clone());
    let (ba1, bb1) = (barrier_a.clone(), barrier_b.clone());
    let h1 = std::thread::spawn(move || {
        let kv = d1.begin().unwrap();
        kv.merge("k", 1u64.to_le_bytes()).unwrap();
        ba1.wait(); // t2 begins after t1's uncommitted merge
        bb1.wait();
        kv.commit().expect("t1 commit");
    });
    let (ba2, bb2) = (barrier_a, barrier_b);
    let h2 = std::thread::spawn(move || {
        ba2.wait();
        let kv = d2.begin().unwrap();
        kv.merge("k", 2u64.to_le_bytes())
            .expect("t2 merge must coexist");
        bb2.wait();
        kv.commit().expect("t2 commit");
    });
    h1.join().unwrap();
    h2.join().unwrap();

    let v = db.view().unwrap().get("k").unwrap();
    assert_eq!(
        v.slice(),
        3u64.to_le_bytes(),
        "both concurrent operands must survive and sum"
    );
}

#[test]
fn normal_writer_conflicts_with_concurrent_merge_head() {
    let env = TestEnv::default();
    let db = open_counter_bucket(&env, "m", true);

    // t1 holds an uncommitted merge; t2's put over it must conflict
    let kv1 = db.begin().unwrap();
    kv1.merge("k", 1u64.to_le_bytes()).unwrap();
    let kv2 = db.begin().unwrap();
    assert!(
        matches!(
            kv2.put("k", 9u64.to_le_bytes()),
            Err(OpCode::AbortTx | OpCode::Again)
        ),
        "put over a concurrent uncommitted merge must first-writer-wins"
    );
    kv1.commit().unwrap();

    // reverse: t1 holds an uncommitted put; t2's merge over it must conflict
    let kv1 = db.begin().unwrap();
    kv1.put("j", 1u64.to_le_bytes()).unwrap();
    let kv2 = db.begin().unwrap();
    assert!(matches!(
        kv2.merge("j", 2u64.to_le_bytes()),
        Err(OpCode::AbortTx | OpCode::Again)
    ));
    kv1.commit().unwrap();
    let v = db.view().unwrap().get("j").unwrap();
    assert_eq!(v.slice(), 1u64.to_le_bytes(), "t2 operand must not leak");
}

#[test]
fn merge_requires_operator_and_allows_blob_operands() {
    let env = TestEnv::default();

    // no operator registered: every merge is rejected up front
    let db = open_counter_bucket(&env, "noop", false);
    let kv = db.begin().unwrap();
    assert!(matches!(
        kv.merge("k", 1u64.to_le_bytes()),
        Err(OpCode::Invalid)
    ));

    // empty operand would decode as tombstone: Invalid
    let db = open_counter_bucket(&env, "op", true);
    let kv = db.begin().unwrap();
    assert!(matches!(kv.merge("k", ""), Err(OpCode::Invalid)));

    // raw operands at the inline boundary use the normal remote/blob path
    let mace = env.open_default().unwrap();
    let opts = BucketOptions {
        inline_size: BucketOptions::MIN_INLINE_SIZE,
        merge_operator: Some(Arc::new(ConcatOp)),
        ..BucketOptions::default()
    };
    let db = mace.new_bucket("small", opts).unwrap();
    let kv = db.begin().unwrap();
    let big = vec![0u8; BucketOptions::MIN_INLINE_SIZE];
    kv.merge("k", &big).expect("blob operand must be admitted");
    kv.commit().unwrap();
    assert_eq!(db.view().unwrap().get("k").unwrap().slice(), big);
}

#[test]
fn same_transaction_merge_run_crossing_imtree_leaf_folds_completely() {
    let env = TestEnv::default();
    let db = open_counter_bucket(&env, "m", true);

    let kv = db.begin().unwrap();
    kv.put("k", 0u64.to_le_bytes()).unwrap();
    kv.commit().unwrap();

    // imtree leaves hold 64 entries. The base plus these operands force an internal
    // separator whose traversal must retain the full (txid, cmd) ordering.
    let kv = db.begin().unwrap();
    for _ in 0..65 {
        kv.merge("k", 1u64.to_le_bytes()).unwrap();
    }
    kv.commit().unwrap();

    assert_eq!(
        db.view().unwrap().get("k").unwrap().slice(),
        65u64.to_le_bytes(),
        "point read must fold every same-transaction operand"
    );

    let view = db.view().unwrap();
    let mut forward = view.seek("k");
    assert_eq!(
        forward.try_next().unwrap().unwrap().val(),
        65u64.to_le_bytes(),
        "forward iteration must fold every same-transaction operand"
    );

    let mut reverse = view.seek("k");
    assert_eq!(
        reverse.try_next_back().unwrap().unwrap().val(),
        65u64.to_le_bytes(),
        "reverse iteration must fold every same-transaction operand"
    );
}

#[test]
fn opening_without_operator_never_unregisters() {
    let env = TestEnv::default();
    let mace = env.open_default().expect("open must succeed");

    let handle = op(9);
    let opts = BucketOptions {
        merge_operator: handle.clone(),
        ..BucketOptions::default()
    };
    mace.new_bucket("m", opts).expect("create bucket");

    // plain open_bucket must not clear or replace the operator
    mace.open_bucket("m").expect("plain open");
    let same = BucketOptions {
        merge_operator: handle,
        ..BucketOptions::default()
    };
    mace.open_bucket_with_options("m", same)
        .expect("original operator must still be registered");
}

/// A merge marker lives in the record, not in the bucket's runtime hint. After
/// a checkpointed reopen without an operator, both scan directions must reject
/// the uninterpretable operand instead of returning its physical bytes.
#[test]
fn operatorless_context_requires_reload_before_binding_operator() {
    let env = TestEnv::default();
    let mace = env.open_default().expect("open");
    let db = mace
        .new_bucket(
            "m",
            BucketOptions {
                merge_operator: op(1),
                ..BucketOptions::default()
            },
        )
        .expect("create bucket");

    let kv = db.begin().expect("begin base");
    kv.put("k", 10u64.to_le_bytes()).expect("base put");
    kv.commit().expect("commit base");
    let kv = db.begin().expect("begin merge");
    kv.merge("k", 7u64.to_le_bytes()).expect("merge");
    kv.commit().expect("commit merge");
    db.checkpoint_and_wait();
    drop(db);
    drop(mace);

    let mace = env.open_default().expect("reopen");
    let db = mace.open_bucket("m").expect("open without operator");
    // Force the reopened page through raw-preserving consolidation. The bucket
    // no longer has an operator, so this must retain the operand chain rather
    // than treating it as an ordinary version run.
    for i in 0_u64..64 {
        let kv = db.begin().expect("begin padding write");
        kv.put(format!("pad{i:03}"), i.to_le_bytes())
            .expect("padding put");
        kv.commit().expect("padding commit");
    }
    let view = db.view().expect("view");
    assert!(matches!(view.seek("k").try_next(), Err(OpCode::Invalid)));
    assert!(matches!(
        view.seek("k").try_next_back(),
        Err(OpCode::Invalid)
    ));
    drop(view);
    assert!(matches!(
        mace.open_bucket_with_options(
            "m",
            BucketOptions {
                merge_operator: op(1),
                ..BucketOptions::default()
            }
        ),
        Err(OpCode::Invalid)
    ));
    drop(db);
    mace.drop_bucket("m").expect("unload operatorless context");
    let db = mace
        .open_bucket_with_options(
            "m",
            BucketOptions {
                merge_operator: op(1),
                ..BucketOptions::default()
            },
        )
        .expect("bind operator while loading a new context");
    assert_eq!(
        db.view().unwrap().get("k").unwrap().slice(),
        17u64.to_le_bytes()
    );
}

#[test]
fn scan_folds_merge_runs_forward_and_reverse() {
    let env = TestEnv::default();
    let mace = env.open_default().expect("open must succeed");
    let db = mace
        .new_bucket(
            "s",
            BucketOptions {
                merge_operator: Some(Arc::new(SatAddOp)),
                ..BucketOptions::default()
            },
        )
        .unwrap();

    let kv = db.begin().unwrap();
    kv.put("k1", 1u64.to_le_bytes()).unwrap();
    kv.put("k2", 2u64.to_le_bytes()).unwrap();
    kv.put("k3", 3u64.to_le_bytes()).unwrap();
    kv.commit().unwrap();
    let kv = db.begin().unwrap();
    kv.merge("k1", 10u64.to_le_bytes()).unwrap();
    kv.merge("k2", 20u64.to_le_bytes()).unwrap();
    kv.commit().unwrap();

    let view = db.view().unwrap();
    let forward: Vec<(Vec<u8>, u64)> = view
        .seek("k")
        .map(|item| {
            (
                item.key().to_vec(),
                u64::from_le_bytes(item.val().try_into().unwrap()),
            )
        })
        .collect();
    assert_eq!(
        forward,
        vec![
            (b"k1".to_vec(), 11),
            (b"k2".to_vec(), 22),
            (b"k3".to_vec(), 3),
        ],
        "forward scan must surface folded logical values"
    );

    let reverse: Vec<u64> = view
        .seek("k")
        .rev()
        .map(|item| u64::from_le_bytes(item.val().try_into().unwrap()))
        .collect();
    assert_eq!(reverse, vec![3, 22, 11], "reverse scan folds identically");
}

#[test]
fn scan_omits_keys_deleted_by_fold() {
    let env = TestEnv::default();
    let mace = env.open_default().expect("open must succeed");
    let db = mace
        .new_bucket(
            "s",
            BucketOptions {
                merge_operator: Some(Arc::new(SatAddOp)),
                ..BucketOptions::default()
            },
        )
        .unwrap();
    let kv = db.begin().unwrap();
    kv.put("gone", 0u64.to_le_bytes()).unwrap();
    kv.put("stay", 7u64.to_le_bytes()).unwrap();
    // 0 + MAX saturates => apply returns None => the key is logically deleted
    kv.merge("gone", u64::MAX.to_le_bytes()).unwrap();
    kv.commit().unwrap();

    let view = db.view().unwrap();
    assert!(view.get("gone").is_err());
    let keys: Vec<Vec<u8>> = view
        .range::<&[u8], _>(..)
        .map(|i| i.key().to_vec())
        .collect();
    assert_eq!(keys, vec![b"stay".to_vec()]);
}

#[test]
fn committed_operand_folds_over_uncommitted_consolidated_merge() {
    let env = TestEnv::default();
    let mace = env.open_default().expect("open must succeed");
    let db = mace
        .new_bucket(
            "s",
            BucketOptions {
                merge_operator: op(1),
                consolidate_threshold: 16,
                split_elems: 64,
                ..BucketOptions::default()
            },
        )
        .unwrap();

    // an uncommitted merge survives consolidation as an invisible SST head;
    // a later committed operand must still resolve through it by snapshot
    let kv2 = db.begin().unwrap();
    kv2.merge("k", 5u64.to_le_bytes()).unwrap();
    let padding = db.begin().unwrap();
    for i in 0..16u64 {
        padding.put(format!("pad_{i:02}"), i.to_le_bytes()).unwrap();
    }
    padding.commit().unwrap();
    let kv3 = db.begin().unwrap();
    kv3.merge("k", 7u64.to_le_bytes()).unwrap();
    kv3.commit().unwrap();

    // only the committed operand is visible: apply(None, 7) = 7. The
    // uncommitted +5 must never leak into the result.
    let v = db.view().unwrap().get("k").unwrap();
    assert_eq!(v.slice(), 7u64.to_le_bytes());
}

#[test]
fn val_ref_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<mace::ValRef>();
}

#[test]
fn checkpointed_invisible_head_resolves_visible_history_in_both_scan_directions() {
    let env = TestEnv::default();
    let mace = env.open_default().expect("open must succeed");
    let db = mace
        .new_bucket(
            "s",
            BucketOptions {
                merge_operator: op(1),
                ..BucketOptions::default()
            },
        )
        .unwrap();

    let kv = db.begin().unwrap();
    kv.put("put", 10u64.to_le_bytes()).unwrap();
    kv.put("merge", 20u64.to_le_bytes()).unwrap();
    kv.commit().unwrap();

    // both uncommitted heads are checkpointed into the base page. Their HistRef
    // still leads to the committed values, which must remain visible to a fresh view.
    let pending = db.begin().unwrap();
    pending.update("put", 11u64.to_le_bytes()).unwrap();
    pending.merge("merge", 1u64.to_le_bytes()).unwrap();
    db.checkpoint_and_wait();

    let view = db.view().unwrap();
    assert_eq!(view.get("put").unwrap().slice(), 10u64.to_le_bytes());
    assert_eq!(view.get("merge").unwrap().slice(), 20u64.to_le_bytes());

    let forward: Vec<(Vec<u8>, u64)> = view
        .range::<&[u8], _>(..)
        .map(|item| {
            (
                item.key().to_vec(),
                u64::from_le_bytes(item.val().try_into().unwrap()),
            )
        })
        .collect();
    assert_eq!(
        forward,
        vec![(b"merge".to_vec(), 20), (b"put".to_vec(), 10)]
    );

    let reverse: Vec<(Vec<u8>, u64)> = view
        .range::<&[u8], _>(..)
        .rev()
        .map(|item| {
            (
                item.key().to_vec(),
                u64::from_le_bytes(item.val().try_into().unwrap()),
            )
        })
        .collect();
    assert_eq!(
        reverse,
        vec![(b"put".to_vec(), 10), (b"merge".to_vec(), 20)]
    );

    drop(pending);
}

/// total operator that returns an empty combine output when an operand (or the
/// accumulated value) is zero: an engine-detected contract violation that keeps
/// that key raw-preserving while other keys fold normally
#[derive(Default)]
struct EmptyOnZeroOp;

impl MergeOperator for EmptyOnZeroOp {
    fn combine_operands(&self, _key: &[u8], left: &[u8], right: &[u8]) -> Vec<u8> {
        let (l, r) = (decode_u64(left).unwrap(), decode_u64(right).unwrap());
        if l == 0 || r == 0 {
            return Vec::new();
        }
        l.wrapping_add(r).to_le_bytes().to_vec()
    }

    fn apply(&self, _key: &[u8], base: Option<&[u8]>, operand: &[u8]) -> Option<Vec<u8>> {
        let o = decode_u64(operand).unwrap();
        if o == 0 {
            return None;
        }
        Some(match base {
            None => o.to_le_bytes().to_vec(),
            Some(b) => decode_u64(b)
                .unwrap()
                .wrapping_add(o)
                .to_le_bytes()
                .to_vec(),
        })
    }
}

fn fold_value(db: &mace::Bucket, key: &[u8]) -> u64 {
    let v = db.view().unwrap().get(key).unwrap();
    decode_u64(v.slice()).unwrap()
}

fn force_consolidation(db: &mace::Bucket) {
    let kv = db.begin().unwrap();
    for i in 0..120u64 {
        kv.put(format!("pad_{i:03}"), vec![b'x'; 8]).unwrap();
    }
    kv.commit().unwrap();
}

/// Phase C crash-safety core: a materializing compact absorbs operand LSNs into
/// the compacted page lineage, so recovery's bucket-frontier gate must skip them.
/// a missing frontier entry would redo the operands on top of the folded base
/// (double-add); a non-durable folded base would lose the increments (no-add).
#[test]
fn materializing_compact_survives_reopen_without_double_add() {
    let env = TestEnv::default();
    let mace = env.open_default().expect("open");
    let db = mace
        .new_bucket(
            "b",
            BucketOptions {
                merge_operator: op(1),
                ..BucketOptions::default()
            },
        )
        .unwrap();

    let kv = db.begin().unwrap();
    kv.put("k", 10u64.to_le_bytes()).unwrap();
    kv.commit().unwrap();
    for operand in [1u64, 2, 4, 8] {
        let kv = db.begin().unwrap();
        kv.merge("k", operand.to_le_bytes()).unwrap();
        kv.commit().unwrap();
    }
    assert_eq!(fold_value(&db, b"k"), 25);

    // materializing consolidation folds the chain into one base row
    force_consolidation(&db);
    assert_eq!(fold_value(&db, b"k"), 25, "fold survives consolidation");

    // shutdown checkpoint publishes the folded page; reopen replays the WAL tail
    // against the bucket frontier derived from page lineages
    drop(mace);
    let mace = env.open_default().expect("reopen");
    let db = mace
        .open_bucket_with_options(
            "b",
            BucketOptions {
                merge_operator: op(1),
                ..BucketOptions::default()
            },
        )
        .unwrap();
    assert_eq!(
        fold_value(&db, b"k"),
        25,
        "no double-add (frontier covers absorbed operand LSNs) and no loss"
    );
}

/// a snapshot pinned before a materializing compact still resolves every merge
/// key through its raw chain; a fresh snapshot reads the folded base. both must
/// agree with the pre-compact value.
#[test]
fn materializing_compact_keeps_long_view_stable() {
    let env = TestEnv::default();
    let mace = env.open_default().expect("open");
    let db = mace
        .new_bucket(
            "b",
            BucketOptions {
                merge_operator: op(1),
                ..BucketOptions::default()
            },
        )
        .unwrap();

    let kv = db.begin().unwrap();
    kv.put("k", 10u64.to_le_bytes()).unwrap();
    kv.commit().unwrap();
    for operand in [1u64, 2, 4, 8] {
        let kv = db.begin().unwrap();
        kv.merge("k", operand.to_le_bytes()).unwrap();
        kv.commit().unwrap();
    }
    let view_old = db.view().unwrap();
    assert_eq!(fold_value(&db, b"k"), 25);

    force_consolidation(&db);

    assert_eq!(fold_value(&db, b"k"), 25, "fresh snapshot folds the base");
    assert_eq!(
        view_old.get("k").unwrap().slice(),
        25u64.to_le_bytes(),
        "pinned snapshot still resolves the raw chain"
    );
}

/// operator failure during a materializing compact keeps that key's chain
/// verbatim (raw-preserving) while other keys on the same page still fold:
/// per-key error isolation from design 2.7.
#[test]
fn empty_fold_output_keeps_chain_and_isolates_the_key() {
    let env = TestEnv::default();
    let mace = env.open_default().expect("open");
    let db = mace
        .new_bucket(
            "b",
            BucketOptions {
                merge_operator: Some(Arc::new(EmptyOnZeroOp)),
                ..BucketOptions::default()
            },
        )
        .unwrap();

    // "bad": the zero operand makes every fold (compact and read) fail
    let kv = db.begin().unwrap();
    kv.put("bad", 10u64.to_le_bytes()).unwrap();
    kv.commit().unwrap();
    for operand in [1u64, 0, 4] {
        let kv = db.begin().unwrap();
        kv.merge("bad", operand.to_le_bytes()).unwrap();
        kv.commit().unwrap();
    }
    // "good": a clean chain on the same consolidation path folds normally
    let kv = db.begin().unwrap();
    kv.put("good", 1u64.to_le_bytes()).unwrap();
    kv.commit().unwrap();
    for operand in [2u64, 4, 8] {
        let kv = db.begin().unwrap();
        kv.merge("good", operand.to_le_bytes()).unwrap();
        kv.commit().unwrap();
    }

    force_consolidation(&db);

    // the violating key's chain survives: the read fold hits the same zero
    // operand, the engine detects the empty combine output as a contract
    // violation, and the key is neither deleted nor silently skipped
    assert!(
        matches!(
            db.view().unwrap().get("bad"),
            Err(OpCode::MergeContractViolation)
        ),
        "chain must survive the empty-output violation"
    );
    // the clean key folded normally on the same compact
    assert_eq!(fold_value(&db, b"good"), 15);
}

/// concatenating operator: fold results grow past small inline boundaries
#[derive(Default)]
struct ConcatOp;

impl MergeOperator for ConcatOp {
    fn combine_operands(&self, _key: &[u8], left: &[u8], right: &[u8]) -> Vec<u8> {
        let mut out = left.to_vec();
        out.extend_from_slice(right);
        out
    }

    fn apply(&self, _key: &[u8], base: Option<&[u8]>, operand: &[u8]) -> Option<Vec<u8>> {
        let mut out = match base {
            None => Vec::new(),
            Some(b) => b.to_vec(),
        };
        out.extend_from_slice(operand);
        Some(out)
    }
}

/// operator whose apply result always exceeds MAX_KV_SIZE: a contract violation
#[derive(Default)]
struct OversizeOp;

impl MergeOperator for OversizeOp {
    fn combine_operands(&self, _key: &[u8], _left: &[u8], right: &[u8]) -> Vec<u8> {
        right.to_vec()
    }

    fn apply(&self, _key: &[u8], base: Option<&[u8]>, operand: &[u8]) -> Option<Vec<u8>> {
        let _ = base;
        let _ = operand;
        // MAX_KV_SIZE = 1 GiB; one byte past the persisted limit
        Some(vec![0u8; (1usize << 30) + 1])
    }
}

/// a folded value past the bucket inline boundary materializes through the
/// remote/blob path and survives checkpoint/reopen (design 6.1)
#[test]
fn materialized_blob_fold_survives_reopen() {
    let env = TestEnv::default();
    let mace = env.open_default().expect("open");
    let db = mace
        .new_bucket(
            "b",
            BucketOptions {
                inline_size: 64,
                merge_operator: Some(Arc::new(ConcatOp)),
                ..BucketOptions::default()
            },
        )
        .unwrap();

    let kv = db.begin().unwrap();
    kv.put("k", vec![b'a'; 40]).unwrap();
    kv.commit().unwrap();
    let kv = db.begin().unwrap();
    kv.merge("k", vec![b'b'; 40]).unwrap();
    kv.commit().unwrap();
    // 40 + 40 = 80 bytes of folded value, past the 64-byte inline boundary
    let expect: Vec<u8> = vec![b'a'; 40].into_iter().chain(vec![b'b'; 40]).collect();
    assert_eq!(db.view().unwrap().get("k").unwrap().slice(), expect);

    force_consolidation(&db);
    assert_eq!(db.view().unwrap().get("k").unwrap().slice(), expect);

    drop(mace);
    let mace = env.open_default().expect("reopen");
    let db = mace
        .open_bucket_with_options(
            "b",
            BucketOptions {
                inline_size: 64,
                merge_operator: Some(Arc::new(ConcatOp)),
                ..BucketOptions::default()
            },
        )
        .unwrap();
    assert_eq!(
        db.view().unwrap().get("k").unwrap().slice(),
        expect,
        "blob-materialized fold survives reopen without loss or double-add"
    );
}

/// a folded value past MAX_KV_SIZE is a contract violation on every reliable read
/// path; the key becomes merge-blocked until a committed del/reset_merge clears
/// it, while the raw chain stays valid and does not block other keys' maintenance
/// (solve.md)
#[test]
fn oversized_fold_blocks_merge_until_committed_delete() {
    use std::sync::Arc;

    let env = TestEnv::default();
    let mace = env.open_default().expect("open");
    let db = mace
        .new_bucket(
            "b",
            BucketOptions {
                merge_operator: Some(Arc::new(OversizeOp)),
                ..BucketOptions::default()
            },
        )
        .unwrap();

    let kv = db.begin().unwrap();
    kv.put("bad", 10u64.to_le_bytes()).unwrap();
    kv.commit().unwrap();
    let kv = db.begin().unwrap();
    kv.merge("bad", 1u64.to_le_bytes()).unwrap();
    kv.commit().unwrap();
    // a plain key without merge operands is unaffected by the violating operator
    let kv = db.begin().unwrap();
    kv.put("plain", 7u64.to_le_bytes()).unwrap();
    kv.commit().unwrap();

    // point get reports the violation; the chain is not deleted or degraded
    assert!(matches!(
        db.view().unwrap().get("bad"),
        Err(mace::OpCode::MergeContractViolation)
    ));
    assert_eq!(
        db.view().unwrap().get("plain").unwrap().slice(),
        7u64.to_le_bytes()
    );

    // forward iterator reports the violation instead of skipping or truncating
    let view = db.view().unwrap();
    let mut iter = view.seek("b".as_bytes());
    let first = iter.try_next();
    assert!(matches!(first, Err(mace::OpCode::MergeContractViolation)));
    // reverse iterator follows the same convention
    let mut rev = view.seek("b".as_bytes());
    assert!(matches!(
        rev.try_next_back(),
        Err(mace::OpCode::MergeContractViolation)
    ));
    // the compatibility Iterator wrapper terminates at the violating key and
    // never yields a later key instead
    let mut compat = view.seek("b".as_bytes());
    assert!(
        compat.next().is_none(),
        "compat wrapper terminates, does not skip"
    );
    assert!(
        compat.next().is_none(),
        "compat wrapper stays terminated after the first resolver error"
    );

    // the violating key is merge-blocked: new operands are rejected without
    // touching WAL or the tree, and the plain key stays writable
    let kv = db.begin().unwrap();
    assert!(matches!(
        kv.merge("bad", 8u64.to_le_bytes()),
        Err(mace::OpCode::MergeContractViolation)
    ));
    let kv = db.begin().unwrap();
    kv.merge("plain", 3u64.to_le_bytes()).unwrap();
    kv.commit().unwrap();

    // an aborted del keeps the blocked state: the next merge is still rejected
    let kv = db.begin().unwrap();
    kv.del("bad").unwrap();
    drop(kv); // abort without commit
    let kv = db.begin().unwrap();
    assert!(matches!(
        kv.merge("bad", 9u64.to_le_bytes()),
        Err(mace::OpCode::MergeContractViolation)
    ));

    // a committed del and reset_merge both succeed (their commit-gated unblock
    // is a best-effort reset: a background reader pinned before the del may
    // still re-block the key from the old chain, which is designed behavior —
    // any observation of the violation re-blocks, solve.md point 3)
    let kv = db.begin().unwrap();
    kv.del("bad").unwrap();
    kv.commit().unwrap();
    let kv = db.begin().unwrap();
    kv.reset_merge("bad").unwrap();
    assert!(kv.commit().is_ok());

    // the oversized key does not block other keys' consolidation on the same page
    let kv = db.begin().unwrap();
    kv.put("plain2", 11u64.to_le_bytes()).unwrap();
    kv.commit().unwrap();
    let kv = db.begin().unwrap();
    for i in 0..120u64 {
        kv.put(format!("pad_{i:03}"), vec![b'x'; 8]).unwrap();
    }
    kv.commit().unwrap();
    assert_eq!(
        db.view().unwrap().get("plain2").unwrap().slice(),
        11u64.to_le_bytes()
    );
    // the raw chain is crash-safe: reopen replays it and reads still report the
    // violation; the runtime-only blocked set is gone, so merge() admits again
    // (the tombstone barrier from the committed del hides the key meanwhile,
    // which is the P1 fix: safe deletes shadow stale merges)
    assert!(matches!(
        db.view().unwrap().get("bad"),
        Err(mace::OpCode::NotFound)
    ));
    drop(mace);
    let mace = env.open_default().expect("reopen");
    let db = mace
        .open_bucket_with_options(
            "b",
            BucketOptions {
                merge_operator: Some(Arc::new(OversizeOp)),
                ..BucketOptions::default()
            },
        )
        .unwrap();
    // the tombstone barrier survives reopen: the key stays absent; the
    // runtime-only blocked set is gone in the fresh context, so merge admits
    assert!(matches!(
        db.view().unwrap().get("bad"),
        Err(mace::OpCode::NotFound)
    ));
    let kv = db.begin().unwrap();
    assert!(kv.merge("bad", 8u64.to_le_bytes()).is_ok());
}

/// operator dispatching algebra per key prefix: the engine must pass the stable
/// user raw key (no version encoding) to every combine/apply across reads,
/// materializing compact, and reopen (design 7.1/7.2)
#[derive(Default)]
struct PrefixDispatchOp;

impl MergeOperator for PrefixDispatchOp {
    fn combine_operands(&self, key: &[u8], left: &[u8], right: &[u8]) -> Vec<u8> {
        if key.starts_with(b"sum:") {
            let (l, r) = (decode_u64(left).unwrap(), decode_u64(right).unwrap());
            l.wrapping_add(r).to_le_bytes().to_vec()
        } else {
            let mut out = left.to_vec();
            out.extend_from_slice(right);
            out
        }
    }

    fn apply(&self, key: &[u8], base: Option<&[u8]>, operand: &[u8]) -> Option<Vec<u8>> {
        if key.starts_with(b"sum:") {
            let o = decode_u64(operand).unwrap();
            Some(
                match base {
                    None => o,
                    Some(b) => decode_u64(b).unwrap().wrapping_add(o),
                }
                .to_le_bytes()
                .to_vec(),
            )
        } else {
            let mut out = match base {
                None => Vec::new(),
                Some(b) => b.to_vec(),
            };
            out.extend_from_slice(operand);
            Some(out)
        }
    }
}

/// one bucket, two algebras dispatched by the stable user key: the key argument
/// must survive read-time fold, materializing compact, and checkpoint/reopen
#[test]
fn key_aware_operator_dispatch_survives_compact_and_reopen() {
    let env = TestEnv::default();
    let mace = env.open_default().expect("open");
    let db = mace
        .new_bucket(
            "b",
            BucketOptions {
                merge_operator: Some(Arc::new(PrefixDispatchOp)),
                ..BucketOptions::default()
            },
        )
        .unwrap();

    // sum algebra under "sum:" prefix
    let kv = db.begin().unwrap();
    kv.put("sum:k", 10u64.to_le_bytes()).unwrap();
    kv.commit().unwrap();
    for o in [1u64, 2, 4] {
        let kv = db.begin().unwrap();
        kv.merge("sum:k", o.to_le_bytes()).unwrap();
        kv.commit().unwrap();
    }
    // concat algebra under "cat:" prefix
    let kv = db.begin().unwrap();
    kv.put("cat:k", b"ab").unwrap();
    kv.commit().unwrap();
    let kv = db.begin().unwrap();
    kv.merge("cat:k", b"cd").unwrap();
    kv.commit().unwrap();

    assert_eq!(
        db.view().unwrap().get("sum:k").unwrap().slice(),
        17u64.to_le_bytes()
    );
    assert_eq!(db.view().unwrap().get("cat:k").unwrap().slice(), b"abcd");

    // materializing compact must pass the same raw key
    force_consolidation(&db);
    assert_eq!(
        db.view().unwrap().get("sum:k").unwrap().slice(),
        17u64.to_le_bytes(),
        "sum algebra intact after compact"
    );
    assert_eq!(
        db.view().unwrap().get("cat:k").unwrap().slice(),
        b"abcd",
        "concat algebra intact after compact"
    );

    // checkpoint/reopen keeps the key argument stable
    drop(mace);
    let mace = env.open_default().expect("reopen");
    let db = mace
        .open_bucket_with_options(
            "b",
            BucketOptions {
                merge_operator: Some(Arc::new(PrefixDispatchOp)),
                ..BucketOptions::default()
            },
        )
        .unwrap();
    assert_eq!(
        db.view().unwrap().get("sum:k").unwrap().slice(),
        17u64.to_le_bytes()
    );
    assert_eq!(db.view().unwrap().get("cat:k").unwrap().slice(), b"abcd");
}

/// a committed Delete at the safe boundary must keep the key NotFound across
/// materializing compact and reopen: stale merge operands below the tombstone
/// must never be folded back into a value (review P1)
#[test]
fn safe_delete_keeps_key_absent_across_compact_and_reopen() {
    let env = TestEnv::default();
    let mace = env.open_default().expect("open");
    let db = mace
        .new_bucket(
            "b",
            BucketOptions {
                merge_operator: op(1),
                ..BucketOptions::default()
            },
        )
        .unwrap();

    // Merge@tx, Merge@tx, Put, then Delete — the latest version is a tombstone
    let kv = db.begin().unwrap();
    kv.merge("k", 1u64.to_le_bytes()).unwrap();
    kv.commit().unwrap();
    let kv = db.begin().unwrap();
    kv.merge("k", 2u64.to_le_bytes()).unwrap();
    kv.commit().unwrap();
    let kv = db.begin().unwrap();
    kv.upsert("k", 30u64.to_le_bytes()).unwrap();
    kv.commit().unwrap();
    let kv = db.begin().unwrap();
    kv.del("k").unwrap();
    kv.commit().unwrap();
    assert!(matches!(
        db.view().unwrap().get("k"),
        Err(mace::OpCode::NotFound)
    ));

    // materializing compact must not resurrect the key from shadowed operands
    force_consolidation(&db);
    assert!(matches!(
        db.view().unwrap().get("k"),
        Err(mace::OpCode::NotFound)
    ));
    let view = db.view().unwrap();
    let mut iter = view.range("k".as_bytes()..="k".as_bytes());
    assert!(
        iter.next().is_none(),
        "scan must not surface the deleted key"
    );

    // recovery/reopen keeps the tombstone barrier
    drop(mace);
    let mace = env.open_default().expect("reopen");
    let db = mace
        .open_bucket_with_options(
            "b",
            BucketOptions {
                merge_operator: op(1),
                ..BucketOptions::default()
            },
        )
        .unwrap();
    assert!(matches!(
        db.view().unwrap().get("k"),
        Err(mace::OpCode::NotFound)
    ));
}
