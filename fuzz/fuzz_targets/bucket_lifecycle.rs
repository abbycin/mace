#![no_main]

mod common;

use common::{ByteStream, FuzzDbRoot, open_engine};
use libfuzzer_sys::fuzz_target;
use mace::{Bucket, BucketOptions, OpCode};
use std::collections::{BTreeMap, BTreeSet};

fn bucket_name(idx: usize) -> String {
    format!("b_{idx}")
}

fn compatible_update(seed: u8) -> BucketOptions {
    BucketOptions {
        checkpoint_size: (32 + ((seed as usize) % 8) * 8) << 10,
        pool_capacity: (64 + ((seed as usize) % 8) * 8) << 10,
        cache_capacity: (64 + ((seed as usize) % 8) * 8) << 10,
        cache_evict_pct: 10 + ((seed as usize) % 7) * 10,
        enable_backpressure: seed & 1 == 0,
        ..BucketOptions::default()
    }
}

fn incompatible_update(seed: u8) -> BucketOptions {
    BucketOptions {
        inline_size: if seed & 1 == 0 { 4096 } else { 8192 },
        split_elems: if seed & 2 == 0 { 128 } else { 256 },
        ..compatible_update(seed)
    }
}

fn assert_bucket_sets(
    mace: &mace::Mace,
    existing: &BTreeMap<String, BucketOptions>,
    pending_delete: usize,
) {
    let nr = mace.nr_buckets() as usize;
    assert!(
        nr >= existing.len(),
        "nr_buckets {} smaller than existing {}",
        nr,
        existing.len()
    );
    assert!(
        nr <= existing.len() + pending_delete,
        "nr_buckets {} exceeds upper bound {}",
        nr,
        existing.len() + pending_delete
    );
    for name in existing.keys() {
        assert!(mace.get_bucket(name).is_ok(), "bucket {name} should exist");
    }
}

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }

    let db_root = FuzzDbRoot::new();
    let mut mace = open_engine(db_root.path(), |opt| {
        opt.concurrent_write = 1;
    });
    let mut existing = BTreeMap::<String, BucketOptions>::new();
    let mut loaded = BTreeMap::<String, Bucket>::new();
    let mut pending_delete = 0usize;
    let mut stream = ByteStream::new(data);
    let mut known_names = BTreeSet::new();

    for _ in 0..96usize {
        let Some(tag) = stream.next() else {
            break;
        };
        let name = bucket_name((tag as usize) % 4);
        known_names.insert(name.clone());

        match tag % 8 {
            0 => {
                if existing.contains_key(&name) {
                    let res = mace.new_bucket(&name, BucketOptions::default());
                    assert!(
                        matches!(res, Err(OpCode::Exist)),
                        "existing bucket create should fail"
                    );
                } else {
                    let bucket = mace
                        .new_bucket(&name, BucketOptions::default())
                        .expect("create bucket failed");
                    existing.insert(name.clone(), BucketOptions::default());
                    loaded.insert(name.clone(), bucket);
                }
            }
            1 => {
                if existing.contains_key(&name) {
                    let bucket = mace.get_bucket(&name).expect("open bucket failed");
                    loaded.insert(name.clone(), bucket);
                } else {
                    let res = mace.get_bucket(&name);
                    assert!(
                        matches!(res, Err(OpCode::NotFound)),
                        "missing bucket get should fail"
                    );
                }
            }
            2 => {
                loaded.remove(&name);
                if existing.contains_key(&name) {
                    mace.drop_bucket(&name).expect("drop bucket failed");
                }
            }
            3 => {
                loaded.remove(&name);
                if existing.remove(&name).is_some() {
                    mace.del_bucket(&name).expect("delete bucket failed");
                    pending_delete += 1;
                }
            }
            4 => {
                let update = compatible_update(tag);
                if let Some(slot) = existing.get_mut(&name) {
                    let res = mace.update_bucket_opt(&name, update);
                    if loaded.contains_key(&name) {
                        assert!(
                            matches!(res, Err(OpCode::Again)),
                            "loaded bucket compatible update should block"
                        );
                    } else {
                        res.expect("compatible update failed");
                        *slot = update;
                    }
                } else {
                    let res = mace.update_bucket_opt(&name, update);
                    assert!(
                        matches!(res, Err(OpCode::NotFound)),
                        "missing bucket update should fail"
                    );
                }
            }
            5 => {
                let update = incompatible_update(tag);
                if existing.contains_key(&name) {
                    let res = mace.update_bucket_opt(&name, update);
                    if loaded.contains_key(&name) {
                        assert!(
                            matches!(res, Err(OpCode::Again)),
                            "loaded bucket incompatible update should block first"
                        );
                    } else {
                        assert!(
                            matches!(res, Err(OpCode::Invalid)),
                            "unloaded bucket incompatible update should fail"
                        );
                    }
                }
            }
            6 => {
                mace.start_gc();
                std::thread::yield_now();
            }
            _ => {
                loaded.clear();
                drop(mace);
                mace = open_engine(db_root.path(), |opt| {
                    opt.concurrent_write = 1;
                });
                for existing_name in existing.keys() {
                    let bucket = mace
                        .get_bucket(existing_name)
                        .expect("existing bucket missing after reopen");
                    loaded.insert(existing_name.clone(), bucket);
                }
                for missing_name in &known_names {
                    if !existing.contains_key(missing_name) {
                        let res = mace.get_bucket(missing_name);
                        assert!(
                            matches!(res, Err(OpCode::NotFound)),
                            "deleted bucket should stay absent"
                        );
                    }
                }
            }
        }

        assert_bucket_sets(&mace, &existing, pending_delete);
        if stream.is_empty() {
            break;
        }
    }
});
