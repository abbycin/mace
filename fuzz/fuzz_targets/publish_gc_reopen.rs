#![no_main]

mod common;

use common::{
    ByteStream, FuzzDbRoot, assert_bucket_matches_model, get_or_create_bucket, key_name,
    open_engine, value_bytes,
};
use libfuzzer_sys::fuzz_target;
use mace::{BucketOptions, OpCode, TxnView};
use std::collections::BTreeMap;

fn batch_upsert(
    bucket: &mace::Bucket,
    model: &mut BTreeMap<String, Option<Vec<u8>>>,
    parity: usize,
    payload: &[u8],
) {
    let txn = bucket.begin().expect("begin batch txn failed");
    for idx in 0..16usize {
        if idx % 2 != parity {
            continue;
        }
        let key = key_name(idx);
        txn.upsert(&key, payload).expect("batch upsert failed");
        model.insert(key, Some(payload.to_vec()));
    }
    txn.commit().expect("batch commit failed");
}

fn verify_lagging_view(lag_view: &TxnView<'_>, snapshot: &BTreeMap<String, Option<Vec<u8>>>) {
    for idx in (0..32usize).step_by(5) {
        let key = key_name(idx);
        match snapshot.get(&key).and_then(|x| x.as_ref()) {
            Some(expected) => {
                let actual = lag_view.get(&key).expect("lag key must exist");
                assert_eq!(
                    actual.slice(),
                    expected.as_slice(),
                    "lag key {key} mismatch"
                );
            }
            None => {
                let res = lag_view.get(&key);
                assert!(
                    matches!(res, Err(OpCode::NotFound)),
                    "lag key {key} should be absent"
                );
            }
        }
    }
}

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }

    let db_root = FuzzDbRoot::new();
    let bucket_opt = BucketOptions {
        inline_size: 128,
        split_elems: 16,
        consolidate_threshold: 4,
        checkpoint_size: 128 << 10,
        pool_capacity: 1 << 20,
        cache_capacity: 1 << 20,
        cache_evict_pct: 100,
        ..BucketOptions::default()
    };
    let mut mace = open_engine(db_root.path(), |opt| {
        opt.concurrent_write = 1;
        opt.data_file_size = 16 << 10;
        opt.blob_file_size = 16 << 10;
        opt.data_garbage_ratio = 0;
        opt.blob_garbage_ratio = 0;
        opt.gc_eager = true;
    });
    let mut bucket =
        get_or_create_bucket(&mace, "main", bucket_opt).expect("open gc bucket failed");
    let mut model = BTreeMap::<String, Option<Vec<u8>>>::new();
    let mut stream = ByteStream::new(data);
    let mut lag_expected: Option<BTreeMap<String, Option<Vec<u8>>>> = None;
    let mut lag_view: Option<TxnView<'_>> = None;
    let mut dirty_batches_since_lifecycle = 0usize;
    let mut progressed_since_verify = false;

    for _ in 0..96usize {
        let Some(tag) = stream.next() else {
            break;
        };
        match tag % 7 {
            0 => {
                let size_hint = if tag & 0x1f == 0 {
                    (tag as usize) + 515
                } else {
                    (tag as usize) + 513
                };
                let payload = value_bytes(tag, size_hint);
                batch_upsert(&bucket, &mut model, (tag as usize) & 1, &payload);
                dirty_batches_since_lifecycle += 1;
                progressed_since_verify = true;
            }
            1 => {
                let key = key_name((tag as usize) % 32);
                if let Some(value) = model.get(&key).and_then(|x| x.as_ref()) {
                    let txn = bucket.begin().expect("begin delete txn failed");
                    txn.del(&key).expect("delete failed");
                    txn.commit().expect("delete commit failed");
                    let _ = value;
                    model.insert(key, None);
                    progressed_since_verify = true;
                }
            }
            2 => {
                if lag_view.is_none() {
                    lag_expected = Some(model.clone());
                    lag_view = Some(bucket.view().expect("open lag view failed"));
                }
            }
            3 => {
                if let (Some(view), Some(expected)) = (lag_view.as_ref(), lag_expected.as_ref()) {
                    verify_lagging_view(view, expected);
                }
                assert_bucket_matches_model(&bucket, &model);
                progressed_since_verify = false;
            }
            4 => {
                if dirty_batches_since_lifecycle > 0 {
                    bucket.checkpoint();
                    progressed_since_verify = true;
                    dirty_batches_since_lifecycle = 0;
                }
            }
            5 => {
                if lag_view.is_some() && dirty_batches_since_lifecycle >= 2 {
                    bucket.checkpoint();
                    mace.start_gc();
                    std::thread::yield_now();
                    if let (Some(view), Some(expected)) = (lag_view.as_ref(), lag_expected.as_ref()) {
                        verify_lagging_view(view, expected);
                    }
                    assert_bucket_matches_model(&bucket, &model);
                    dirty_batches_since_lifecycle = 0;
                    progressed_since_verify = false;
                }
            }
            _ => {
                if progressed_since_verify || dirty_batches_since_lifecycle > 0 {
                    if let (Some(view), Some(expected)) = (lag_view.as_ref(), lag_expected.as_ref()) {
                        verify_lagging_view(view, expected);
                    }
                    lag_view = None;
                    lag_expected = None;
                    drop(bucket);
                    drop(mace);
                    mace = open_engine(db_root.path(), |opt| {
                        opt.concurrent_write = 1;
                        opt.data_file_size = 16 << 10;
                        opt.blob_file_size = 16 << 10;
                        opt.data_garbage_ratio = 0;
                        opt.blob_garbage_ratio = 0;
                        opt.gc_eager = true;
                    });
                    bucket = get_or_create_bucket(&mace, "main", bucket_opt).expect("reopen gc bucket");
                    assert_bucket_matches_model(&bucket, &model);
                    dirty_batches_since_lifecycle = 0;
                    progressed_since_verify = false;
                }
            }
        }
        if stream.is_empty() {
            break;
        }
    }

    if let (Some(view), Some(expected)) = (lag_view.as_ref(), lag_expected.as_ref()) {
        verify_lagging_view(view, expected);
    }
    assert_bucket_matches_model(&bucket, &model);
});
