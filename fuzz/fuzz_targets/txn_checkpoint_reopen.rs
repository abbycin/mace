#![no_main]

mod common;

use common::{
    ByteStream, FuzzDbRoot, assert_bucket_matches_model, get_or_create_bucket, key_name,
    open_engine, value_bytes,
};
use libfuzzer_sys::fuzz_target;
use mace::BucketOptions;
use std::collections::BTreeMap;

#[derive(Clone)]
enum PendingOp {
    Put(String, Vec<u8>),
    Update(String, Vec<u8>),
    Upsert(String, Vec<u8>),
    Del(String),
}

fn apply_pending_shadow(
    model: &BTreeMap<String, Option<Vec<u8>>>,
    pending: &[PendingOp],
) -> BTreeMap<String, Option<Vec<u8>>> {
    let mut shadow = model.clone();
    for op in pending {
        match op {
            PendingOp::Put(key, value) => {
                shadow.insert(key.clone(), Some(value.clone()));
            }
            PendingOp::Update(key, value) | PendingOp::Upsert(key, value) => {
                shadow.insert(key.clone(), Some(value.clone()));
            }
            PendingOp::Del(key) => {
                shadow.insert(key.clone(), None);
            }
        }
    }
    shadow
}

fn build_valid_pending_op(
    selector: u8,
    key: String,
    value: Vec<u8>,
    shadow: &BTreeMap<String, Option<Vec<u8>>>,
) -> PendingOp {
    let exists = shadow.get(&key).and_then(|x| x.as_ref()).is_some();
    match selector % 4 {
        0 if !exists => PendingOp::Put(key, value),
        1 if exists => PendingOp::Update(key, value),
        2 => PendingOp::Upsert(key, value),
        _ if exists => PendingOp::Del(key),
        _ => PendingOp::Upsert(key, value),
    }
}

fn commit_pending(
    bucket: &mace::Bucket,
    model: &mut BTreeMap<String, Option<Vec<u8>>>,
    pending: &mut Vec<PendingOp>,
) {
    if pending.is_empty() {
        return;
    }
    let txn = bucket.begin().expect("begin txn failed");
    for op in pending.iter() {
        match op {
            PendingOp::Put(key, value) => txn.put(key, value).expect("put failed"),
            PendingOp::Update(key, value) => txn.update(key, value).expect("update failed"),
            PendingOp::Upsert(key, value) => txn.upsert(key, value).expect("upsert failed"),
            PendingOp::Del(key) => txn.del(key).expect("delete failed"),
        }
    }
    txn.commit().expect("commit failed");
    *model = apply_pending_shadow(model, pending);
    pending.clear();
}

fn drop_pending(bucket: &mace::Bucket, pending: &mut Vec<PendingOp>) {
    if pending.is_empty() {
        return;
    }
    let txn = bucket.begin().expect("begin abort txn failed");
    for op in pending.iter() {
        match op {
            PendingOp::Put(key, value) => txn.put(key, value).expect("put failed"),
            PendingOp::Update(key, value) => txn.update(key, value).expect("update failed"),
            PendingOp::Upsert(key, value) => txn.upsert(key, value).expect("upsert failed"),
            PendingOp::Del(key) => txn.del(key).expect("delete failed"),
        }
    }
    drop(txn);
    pending.clear();
}

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }

    let db_root = FuzzDbRoot::new();
    let mut mace = open_engine(db_root.path(), |opt| {
        opt.concurrent_write = 1;
        opt.data_file_size = 32 << 10;
        opt.wal_file_size = 16 << 10;
        opt.max_ckpt_per_txn = 64;
    });
    let mut bucket = get_or_create_bucket(&mace, "main", BucketOptions::default())
        .expect("open main bucket failed");
    let mut model = BTreeMap::<String, Option<Vec<u8>>>::new();
    let mut pending = Vec::<PendingOp>::new();
    let mut stream = ByteStream::new(data);
    let max_steps = 128usize;

    for _ in 0..max_steps {
        let Some(tag) = stream.next() else {
            break;
        };
        let key = key_name((tag as usize) % 8);
        let value = value_bytes(tag, tag as usize);
        match tag % 8 {
            0 => {
                if pending.is_empty() {
                    pending = Vec::new();
                }
            }
            1 => {
                let shadow = apply_pending_shadow(&model, &pending);
                let op = build_valid_pending_op(tag, key, value, &shadow);
                if pending.len() < 8 {
                    pending.push(op);
                }
            }
            2 => commit_pending(&bucket, &mut model, &mut pending),
            3 => drop_pending(&bucket, &mut pending),
            4 => {
                commit_pending(&bucket, &mut model, &mut pending);
                bucket.checkpoint();
            }
            5 => {
                commit_pending(&bucket, &mut model, &mut pending);
                mace.sync().expect("sync failed");
            }
            6 => {
                commit_pending(&bucket, &mut model, &mut pending);
                drop(bucket);
                drop(mace);
                mace = open_engine(db_root.path(), |opt| {
                    opt.concurrent_write = 1;
                    opt.data_file_size = 32 << 10;
                    opt.wal_file_size = 16 << 10;
                    opt.max_ckpt_per_txn = 64;
                });
                bucket = get_or_create_bucket(&mace, "main", BucketOptions::default())
                    .expect("reopen main bucket failed");
                assert_bucket_matches_model(&bucket, &model);
            }
            _ => assert_bucket_matches_model(&bucket, &model),
        }
        if stream.is_empty() {
            break;
        }
    }

    drop_pending(&bucket, &mut pending);
    assert_bucket_matches_model(&bucket, &model);
    bucket.checkpoint();
    drop(bucket);
    drop(mace);

    let reopened = open_engine(db_root.path(), |opt| {
        opt.concurrent_write = 1;
        opt.data_file_size = 32 << 10;
        opt.wal_file_size = 16 << 10;
        opt.max_ckpt_per_txn = 64;
    });
    let bucket =
        get_or_create_bucket(&reopened, "main", BucketOptions::default()).expect("final reopen");
    assert_bucket_matches_model(&bucket, &model);
});
