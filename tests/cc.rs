use mace::observe::{CounterMetric, InMemoryObserver, ObserveSnapshot};
use mace::{Bucket, Mace, OpCode, Options, RandomPath};
use rand::seq::SliceRandom;
use std::{
    collections::HashSet,
    sync::{Arc, Barrier, RwLock},
    thread::{JoinHandle, sleep},
    time::Duration,
};

#[test]
fn put_get() -> Result<(), OpCode> {
    let path = RandomPath::new();
    let opt = Options::new(&*path);
    let mut saved = opt.clone();
    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let db = mace.new_bucket("default").unwrap();
    let n = 100000;
    let mut elems = Vec::with_capacity(n);
    for i in 0..n {
        elems.push(format!("elem_{i}"));
    }
    let del1: RwLock<HashSet<Vec<u8>>> = RwLock::new(HashSet::new());
    let del2: RwLock<HashSet<Vec<u8>>> = RwLock::new(HashSet::new());
    let put_ok: RwLock<HashSet<Vec<u8>>> = RwLock::new(HashSet::new());
    let barrier = Barrier::new(3);

    std::thread::scope(|s| {
        s.spawn(|| {
            barrier.wait();
            for i in &elems {
                let kv = db.begin().unwrap();
                if kv.put(i, i).is_ok() {
                    put_ok.write().unwrap().insert(i.as_bytes().to_vec());
                }
                kv.commit().unwrap();
            }
        });

        s.spawn(|| {
            barrier.wait();
            for i in &elems {
                let kv = db.begin().unwrap();
                if let Ok(x) = kv.del(i) {
                    assert_eq!(x.slice(), i.as_bytes());
                    del1.write().unwrap().insert(x.slice().to_vec());
                }
                kv.commit().unwrap();
            }
        });

        s.spawn(|| {
            barrier.wait();
            for i in &elems {
                let kv = db.begin().unwrap();
                if let Ok(x) = kv.del(i) {
                    assert_eq!(x.slice(), i.as_bytes());
                    del2.write().unwrap().insert(x.slice().to_vec());
                }
                kv.commit().unwrap();
            }
        });
    });
    drop(db);
    drop(mace);

    saved.tmp_store = true;
    let mace = Mace::new(saved.validate().unwrap()).unwrap();
    let db = mace.get_bucket("default").unwrap();

    check(&db, &elems, &put_ok, &del1, &del2);

    Ok(())
}

fn check(
    db: &Bucket,
    elems: &[String],
    put_ok: &RwLock<HashSet<Vec<u8>>>,
    del1: &RwLock<HashSet<Vec<u8>>>,
    del2: &RwLock<HashSet<Vec<u8>>>,
) {
    let put_lk = put_ok.read().unwrap();
    let lk1 = del1.read().unwrap();
    let lk2 = del2.read().unwrap();
    let view = db.view().unwrap();
    for i in elems.iter() {
        let tmp = i.as_bytes().to_vec();
        if !put_lk.contains(&tmp) {
            continue;
        }
        if lk1.contains(&tmp) || lk2.contains(&tmp) {
            continue;
        }
        let r = view.get(i).expect("must exist");
        if r.slice() != i.as_bytes() {
            assert!(!lk1.contains(r.slice()));
            assert_eq!(r.slice(), i.as_bytes());
        }
    }

    for i in lk1.iter() {
        let r = view.get(i.as_slice());
        assert!(r.is_err());
    }

    for i in lk2.iter() {
        let r = view.get(i.as_slice());
        assert!(r.is_err());
    }
}

#[test]
fn get_del() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let opt = Options::new(&*path);
    let mace = Mace::new(opt.validate().unwrap())?;
    let db = mace.new_bucket("default").unwrap();
    let n = 1000;
    let mut v = Vec::with_capacity(n);

    for i in 0..n {
        v.push(format!("elem_{i}"));
    }

    for i in &v {
        let kv = db.begin().unwrap();
        kv.put(i.as_bytes(), i.as_bytes()).expect("can't put");
        kv.commit()?
    }

    for i in &v {
        let view = db.view().unwrap();
        view.get(i.as_bytes()).expect("can't get");
    }

    let cnt = n / 3;
    let mut removed = Vec::with_capacity(cnt);
    let mut rng = rand::rng();

    v.shuffle(&mut rng);

    for (i, k) in v.iter().enumerate() {
        if i == cnt {
            break;
        }
        removed.push(k.clone());
        let kv = db.begin().unwrap();
        kv.del(k.as_bytes()).expect("can't del");
        kv.commit().unwrap();
    }

    for i in &removed {
        let view = db.view().unwrap();
        let r = view.get(i.as_bytes());
        assert!(r.is_err());
    }

    drop(db);
    drop(mace);
    Ok(())
}

#[test]
fn rollback() {
    let groups = 12;
    let mut opts = Options::new(&*RandomPath::new());
    opts.tmp_store = true;
    opts.sync_on_write = false;
    opts.concurrent_write = 16;
    let mace = Mace::new(opts.validate().unwrap()).unwrap();
    const N: usize = 10000;
    let db = mace.new_bucket("x").unwrap();
    // multiple threads are trying to update the same key, some of them will pass the visibility check
    // and start updating the key, but only one of them may succeed, those failure threads will check the
    // visibility again and find the new key is invisible to them, they will abort the transaction
    // and rollback what they have written to the log
    fn update(db: &Bucket, pairs: &Vec<Vec<u8>>) {
        for x in pairs {
            let kv = db.begin().unwrap();
            if kv.update(x, x).is_ok() {
                kv.commit().unwrap();
            }
            // automatically rollback
        }
    }

    for t in [2, 6, groups] {
        for len in [16, 64, 256] {
            let pairs: Vec<Vec<u8>> = (0..N)
                .map(|i| {
                    let mut tmp = vec![0; len];
                    let k = format!("{i}");
                    tmp[..k.len()].copy_from_slice(k.as_bytes());
                    tmp
                })
                .collect();
            let pairs = Arc::new(pairs);

            for x in pairs.iter() {
                let kv = db.begin().unwrap();
                kv.upsert(x, x).unwrap();
                kv.commit().unwrap();
            }
            let h: Vec<JoinHandle<()>> = (0..t)
                .map(|_| {
                    let pairs = pairs.clone();
                    let db = db.clone();
                    std::thread::spawn(move || update(&db, &pairs))
                })
                .collect();

            for x in h {
                x.join().unwrap();
            }
        }
    }
    drop(db);
    drop(mace);
}

#[test]
fn range_simple() -> Result<(), OpCode> {
    let mut opts = Options::new(&*RandomPath::new());
    opts.tmp_store = true;
    let mace = Mace::new(opts.validate().unwrap()).unwrap();
    let db = mace.new_bucket("x").unwrap();

    let kv = db.begin().unwrap();
    kv.put("foo", "1")?;
    kv.update("foo", "2")?;
    kv.put("mo", "1")?;
    kv.update("mo", "2")?;
    kv.commit()?;

    let kv = db.begin().unwrap();
    kv.del("mo")?;
    kv.put("mo", "3")?;
    kv.update("foo", "3")?;
    kv.put("fool", "1")?;
    kv.commit()?;

    {
        let view = db.view().unwrap();
        let mut iter = view.seek("foo");
        let item = iter.next().unwrap();
        assert_eq!(item.key(), "foo".as_bytes());
        assert_eq!(item.val(), "3".as_bytes());
        let item = iter.next().unwrap();
        assert_eq!(item.key(), "fool".as_bytes());
        assert_eq!(item.val(), "1".as_bytes());

        assert!(iter.next().is_none());
        drop(item);
        drop(iter);

        let mut iter = view.seek("mo");
        let item = iter.next().unwrap();
        assert_eq!(item.key(), "mo".as_bytes());
        assert_eq!(item.val(), "3".as_bytes());
        assert!(iter.next().is_none());
        drop(item);
        drop(iter);
    }

    let kv = db.begin().unwrap();
    kv.del("foo")?;
    {
        let mut iter = kv.seek("foo");
        let item = iter.next().unwrap();
        assert_eq!(item.key(), "fool".as_bytes());
        assert_eq!(item.val(), "1".as_bytes());
        assert!(iter.next().is_none());
    }
    drop(kv);
    drop(db);
    drop(mace);

    {
        let mut opts = Options::new(&*RandomPath::new());
        opts.tmp_store = true;
        let mace = Mace::new(opts.validate().unwrap()).unwrap();
        let db = mace.new_bucket("x").unwrap();

        let kv = db.begin().unwrap();
        kv.put([0], "bar")?;
        kv.put([0, 1], "bar")?;
        kv.put([0, 2], "bar")?;
        kv.commit()?;

        let view = db.view().unwrap();
        let mut iter = view.seek([0]);
        let item = iter.next().unwrap();
        assert_eq!(item.key(), [0].as_slice());
        assert_eq!(item.val(), "bar".as_bytes());
        let item = iter.next().unwrap();
        assert_eq!(item.key(), [0, 1].as_slice());
        assert_eq!(item.val(), "bar".as_bytes());
        let item = iter.next().unwrap();
        assert_eq!(item.key(), [0, 2].as_slice());
        assert_eq!(item.val(), "bar".as_bytes());

        drop(item);
        drop(iter);
        drop(view);
        drop(db);
        drop(mace);
    }
    Ok(())
}

#[test]
fn seek_stops_when_key_no_longer_matches_prefix() -> Result<(), OpCode> {
    let mut opts = Options::new(&*RandomPath::new());
    opts.tmp_store = true;
    let mace = Mace::new(opts.validate().unwrap()).unwrap();
    let db = mace.new_bucket("x").unwrap();

    let kv = db.begin().unwrap();
    for key in [
        vec![0x00],
        vec![0xFE, 0xFF],
        vec![0xFF],
        vec![0xFF, 0x00],
        vec![0xFF, 0x01],
        vec![0xFF, 0xFF],
        vec![0xFF, 0xFF, 0x00],
        vec![0xFF, 0xFF, 0x01],
    ] {
        kv.put(&key, "v")?;
    }
    kv.commit()?;

    let view = db.view().unwrap();
    let got: Vec<Vec<u8>> = view.seek([0xFF]).map(|item| item.key().to_vec()).collect();
    assert_eq!(
        got,
        vec![
            vec![0xFF],
            vec![0xFF, 0x00],
            vec![0xFF, 0x01],
            vec![0xFF, 0xFF],
            vec![0xFF, 0xFF, 0x00],
            vec![0xFF, 0xFF, 0x01]
        ]
    );

    let got: Vec<Vec<u8>> = view
        .seek([0xFF, 0xFF])
        .map(|item| item.key().to_vec())
        .collect();
    assert_eq!(
        got,
        vec![
            vec![0xFF, 0xFF],
            vec![0xFF, 0xFF, 0x00],
            vec![0xFF, 0xFF, 0x01]
        ]
    );

    Ok(())
}

#[test]
fn range_in_one_node() -> Result<(), OpCode> {
    let mut opts = Options::new(&*RandomPath::new());
    opts.tmp_store = true;
    let mace = Mace::new(opts.validate().unwrap()).unwrap();
    const N: usize = 10;
    let db = mace.new_bucket("x").unwrap();

    let check_app = || {
        let mut words = Vec::new();
        let view = db.view().unwrap();
        for item in view.seek("app") {
            words.push((item.key().to_vec(), item.val().to_vec()));
        }
        assert_eq!(words.len(), 4);
        assert!(words.contains(&("app".as_bytes().to_vec(), "bar".as_bytes().to_vec())));
        assert!(words.contains(&("apple".as_bytes().to_vec(), "+1s".as_bytes().to_vec())));
        assert!(words.contains(&("approve".as_bytes().to_vec(), "+1s".as_bytes().to_vec())));
        assert!(words.contains(&("apply".as_bytes().to_vec(), "bar".as_bytes().to_vec())));
    };

    let db2 = db.clone();
    let t = std::thread::spawn(move || {
        for i in 0..N {
            let k = format!("key_{i}");
            let kv = db2.begin().unwrap();
            kv.put(&k, &k).unwrap();
            kv.commit().unwrap();
            sleep(Duration::from_micros(1));
        }
    });

    let kv = db.begin().unwrap();
    kv.put("app", "bar")?;
    kv.put("ape", "aba")?;
    sleep(Duration::from_micros(1));
    kv.put("apple", "+1s")?;
    kv.put("foo", "bar")?;
    sleep(Duration::from_micros(1));
    kv.put("approve", "+1s")?;
    kv.put("mo", "ha")?;
    sleep(Duration::from_micros(1));
    kv.put("apply", "bar")?;
    kv.commit()?;

    check_app();

    t.join().unwrap();

    let mut keys = HashSet::with_capacity(N);
    for i in 0..N {
        let x = format!("key_{i}");
        keys.insert(x);
    }

    {
        let mut cnt = 0;
        let view = db.view().unwrap();
        for item in view.seek("key") {
            cnt += 1;
            let k = item.key();
            let v = item.val();
            assert_eq!(k, v);
            assert!(keys.contains(to_str(k)));
        }
        assert_eq!(cnt, N);
    }
    // check again
    check_app();

    drop(db);
    drop(mace);
    Ok(())
}

#[test]
fn range_cross_node() -> Result<(), OpCode> {
    let mut opts = Options::new(&*RandomPath::new());
    opts.split_elems = 128; // force split
    opts.sync_on_write = false;
    opts.tmp_store = true;
    let mace = Mace::new(opts.validate().unwrap()).unwrap();
    let db = mace.new_bucket("x").unwrap();
    const N: usize = 500;
    let mut h = HashSet::with_capacity(N);

    for i in 0..N {
        let x = format!("key_{i}");
        let kv = db.begin().unwrap();
        kv.put(&x, &x)?;
        kv.commit()?;
        h.insert(x);
    }

    let check_key = || {
        let mut cnt = 0;
        let view = db.view().unwrap();
        for item in view.seek("key") {
            cnt += 1;
            let k = item.key();
            let v = item.val();
            assert_eq!(k, v);
            assert!(h.contains(to_str(k)));
        }
        assert_eq!(cnt, h.len());
    };

    check_key();

    let kv = db.begin().unwrap();
    for i in 0..40 {
        let a = format!("aa_{i}");
        kv.put(&a, "bar")?;
    }
    kv.commit()?;

    // check again
    check_key();

    let kv = db.begin().unwrap();
    for i in 0..40 {
        let z = format!("zz_{i}");
        kv.put(&z, "bar")?;
    }
    kv.commit()?;

    // check again
    check_key();

    drop(db);
    drop(mace);
    Ok(())
}

#[test]
fn cross_txn() {
    let path = RandomPath::new();
    let mut opt = Options::new(&*path);
    opt.tmp_store = true;
    opt.concurrent_write = 4;
    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let db = mace.new_bucket("x").unwrap();

    let tx1 = db.begin().unwrap();
    let tx2 = db.begin().unwrap();

    let _ = tx1.put("foo", "1");
    let _ = tx1.put("fool", "2");

    let iter = tx2.seek("fo");
    assert_eq!(iter.count(), 0);

    let _ = tx1.commit();

    let iter = tx2.seek("fo");
    assert_eq!(iter.count(), 0);
    let _ = tx2.commit();

    {
        let tx2 = db.view().unwrap();
        let iter = tx2.seek("fo");
        assert_eq!(iter.count(), 2);
    }

    let tx1 = db.begin().unwrap();
    tx1.del("foo").unwrap();

    {
        let iter = tx1.seek("fo");
        assert_eq!(iter.count(), 1);
    }

    {
        let view = db.view().unwrap();
        let iter = view.seek("fo");
        assert_eq!(iter.count(), 2);

        let _ = tx1.commit();
        let iter = view.seek("fo");
        assert_eq!(iter.count(), 2);
    }

    {
        let view = db.view().unwrap();
        let iter = view.seek("fo");
        assert_eq!(iter.count(), 1);
        let r = view.get("fool").unwrap();
        assert_eq!(r.slice(), "2".as_bytes());
    }

    drop(db);
    drop(mace);
}

#[test]
fn smo_during_scan() -> Result<(), OpCode> {
    let path = RandomPath::new();
    let mut opt = Options::new(&*path);
    opt.tmp_store = true;
    opt.split_elems = 128;

    let mace = Mace::new(opt.validate().unwrap()).unwrap();
    let db = mace.new_bucket("x").unwrap();
    let data: Vec<String> = (0..512).map(|x| format!("key_{x}")).collect();
    let mut target = Vec::new();

    for (i, x) in data.iter().enumerate() {
        if i % 2 == 0 {
            let kv = db.begin().unwrap();
            kv.put(x, x)?;
            kv.commit()?;
            target.push(x.clone());
        }
    }

    target.sort();

    {
        let view = db.view().unwrap();
        let mut iter = view.seek("key");
        let mut idx = 0;
        for (i, x) in data.iter().enumerate() {
            if i % 2 != 0 {
                let kv = db.begin().unwrap();
                kv.put(x, x)?;
                kv.commit()?;
            } else if let Some(item) = iter.next() {
                assert_eq!(item.key(), target[idx].as_bytes());
                idx += 1;
            }
        }

        assert_eq!(idx, target.len());

        drop(iter);

        let mut iter = view.seek("key");
        idx = 0;
        // merge is hard to trigger, so we just do a simple test
        for x in data.iter() {
            let kv = db.begin().unwrap();
            kv.del(x)?;
            kv.commit()?;

            if let Some(item) = iter.next() {
                assert_eq!(item.key(), target[idx].as_bytes());
                idx += 1;
            }
        }
        assert_eq!(idx, target.len());
    }

    drop(db);
    drop(mace);
    Ok(())
}

fn upsert_retry(db: &Bucket, key: &str) {
    const RETRY_LIMIT: usize = 8192;
    for _ in 0..RETRY_LIMIT {
        let kv = db.begin().unwrap();
        match kv.upsert(key, key) {
            Ok(_) => match kv.commit() {
                Ok(_) => return,
                Err(OpCode::Again | OpCode::AbortTx) => std::thread::yield_now(),
                Err(e) => panic!("commit upsert fail: {e:?}"),
            },
            Err(OpCode::Again | OpCode::AbortTx) => std::thread::yield_now(),
            Err(e) => panic!("upsert fail: {e:?}"),
        }
    }
    panic!("upsert retry exhausted: {key}");
}

fn del_retry(db: &Bucket, key: &str) {
    const RETRY_LIMIT: usize = 8192;
    for _ in 0..RETRY_LIMIT {
        let kv = db.begin().unwrap();
        let ready = match kv.del(key) {
            Ok(_) | Err(OpCode::NotFound) => true,
            Err(OpCode::Again | OpCode::AbortTx) => false,
            Err(e) => panic!("del fail: {e:?}"),
        };
        if !ready {
            std::thread::yield_now();
            continue;
        }
        match kv.commit() {
            Ok(_) => return,
            Err(OpCode::Again | OpCode::AbortTx) => std::thread::yield_now(),
            Err(e) => panic!("commit del fail: {e:?}"),
        }
    }
    panic!("del retry exhausted: {key}");
}

fn assert_seek_sorted_unique(db: &Bucket, prefix: &str) {
    let view = db.view().unwrap();
    let mut last: Option<Vec<u8>> = None;
    for item in view.seek(prefix) {
        let key = item.key();
        if let Some(prev) = last.as_ref() {
            assert!(prev.as_slice() < key);
        }
        last = Some(key.to_vec());
    }
}

fn counter(snapshot: &ObserveSnapshot, metric: CounterMetric) -> u64 {
    snapshot
        .counters
        .iter()
        .find(|(m, _)| *m == metric)
        .map(|(_, v)| *v)
        .unwrap_or_default()
}

#[test]
fn smo_merge_preserves_final_state() -> Result<(), OpCode> {
    let mut opts = Options::new(&*RandomPath::new());
    let observer = Arc::new(InMemoryObserver::new(256));
    opts.tmp_store = true;
    opts.sync_on_write = false;
    opts.concurrent_write = 8;
    opts.split_elems = 20;
    opts.consolidate_threshold = 8;
    opts.observer = observer.clone();
    let mace = Mace::new(opts.validate().unwrap()).unwrap();
    let db = mace.new_bucket("x").unwrap();

    const WRITERS: usize = 4;
    const WINDOW: usize = 192;
    const WRITE_STEPS: usize = 1800;
    const READERS: usize = 2;
    const READ_STEPS: usize = 320;
    const EXTRA_STEPS: usize = 256;
    const MAX_EXTRA_ROUNDS: usize = 12;
    const MIN_SPLIT_COUNT: u64 = 16;
    const MIN_MERGE_COUNT: u64 = 8;
    const MIN_CONSOLIDATE_COUNT: u64 = 64;

    std::thread::scope(|s| {
        let mut writer_handles = Vec::with_capacity(WRITERS);
        for tid in 0..WRITERS {
            let db = db.clone();
            writer_handles.push(s.spawn(move || {
                for step in 0..WRITE_STEPS {
                    let key = format!("w{tid}_{step:06}");
                    upsert_retry(&db, &key);
                    if step >= WINDOW {
                        let old = format!("w{tid}_{:06}", step - WINDOW);
                        del_retry(&db, &old);
                    }
                }
            }));
        }

        let mut reader_handles = Vec::with_capacity(READERS);
        for _ in 0..READERS {
            let db = db.clone();
            reader_handles.push(s.spawn(move || {
                for _ in 0..READ_STEPS {
                    assert_seek_sorted_unique(&db, "w");
                }
            }));
        }

        for handle in writer_handles {
            handle.join().unwrap();
        }
        for handle in reader_handles {
            handle.join().unwrap();
        }
    });

    for _ in 0..96 {
        let view = db.view().unwrap();
        for tid in 0..WRITERS {
            for idx in (0..(WRITE_STEPS - WINDOW)).step_by(4) {
                let key = format!("w{tid}_{idx:06}");
                let _ = view.get(&key);
            }
        }
    }

    let mut end_step = WRITE_STEPS;
    let mut snapshot = observer.snapshot();
    for _ in 0..MAX_EXTRA_ROUNDS {
        let split_count = counter(&snapshot, CounterMetric::TreeNodeSplit);
        let merge_count = counter(&snapshot, CounterMetric::TreeNodeMerge);
        let consolidate_count = counter(&snapshot, CounterMetric::TreeNodeConsolidate);
        if split_count >= MIN_SPLIT_COUNT
            && merge_count >= MIN_MERGE_COUNT
            && consolidate_count >= MIN_CONSOLIDATE_COUNT
        {
            break;
        }
        for tid in 0..WRITERS {
            for step in end_step..(end_step + EXTRA_STEPS) {
                let key = format!("w{tid}_{step:06}");
                upsert_retry(&db, &key);
                let old = format!("w{tid}_{:06}", step - WINDOW);
                del_retry(&db, &old);
            }
        }
        end_step += EXTRA_STEPS;
        for _ in 0..64 {
            assert_seek_sorted_unique(&db, "w");
        }
        snapshot = observer.snapshot();
    }

    let view = db.view().unwrap();
    for tid in 0..WRITERS {
        for idx in 0..(end_step - WINDOW) {
            let key = format!("w{tid}_{idx:06}");
            assert!(view.get(&key).is_err());
        }
        for idx in (end_step - WINDOW)..end_step {
            let key = format!("w{tid}_{idx:06}");
            let r = view.get(&key).expect("must exist");
            assert_eq!(r.slice(), key.as_bytes());
        }
    }

    assert_eq!(view.seek("w").count(), WRITERS * WINDOW);

    let snapshot = observer.snapshot();
    let split_count = counter(&snapshot, CounterMetric::TreeNodeSplit);
    let merge_count = counter(&snapshot, CounterMetric::TreeNodeMerge);
    let consolidate_count = counter(&snapshot, CounterMetric::TreeNodeConsolidate);
    assert!(
        split_count >= MIN_SPLIT_COUNT,
        "split count too low: {split_count}"
    );
    assert!(
        merge_count >= MIN_MERGE_COUNT,
        "merge count too low: {merge_count}"
    );
    assert!(
        consolidate_count >= MIN_CONSOLIDATE_COUNT,
        "consolidate count too low: {consolidate_count}"
    );
    Ok(())
}

#[test]
fn smo_scan_remains_ordered_under_merge_churn() -> Result<(), OpCode> {
    let mut opts = Options::new(&*RandomPath::new());
    let observer = Arc::new(InMemoryObserver::new(256));
    opts.tmp_store = true;
    opts.sync_on_write = false;
    opts.concurrent_write = 8;
    opts.split_elems = 20;
    opts.consolidate_threshold = 8;
    opts.observer = observer.clone();
    let mace = Mace::new(opts.validate().unwrap()).unwrap();
    let db = mace.new_bucket("x").unwrap();

    const WRITERS: usize = 6;
    const WINDOW: usize = 128;
    const WRITE_STEPS: usize = 2000;
    const READERS: usize = 4;
    const READ_STEPS: usize = 420;
    const EXTRA_STEPS: usize = 256;
    const MAX_EXTRA_ROUNDS: usize = 12;
    const MIN_SPLIT_COUNT: u64 = 24;
    const MIN_MERGE_COUNT: u64 = 12;
    const MIN_CONSOLIDATE_COUNT: u64 = 96;

    std::thread::scope(|s| {
        let mut writer_handles = Vec::with_capacity(WRITERS);
        for tid in 0..WRITERS {
            let db = db.clone();
            writer_handles.push(s.spawn(move || {
                for step in 0..WRITE_STEPS {
                    let key = format!("k{tid}_{step:06}");
                    upsert_retry(&db, &key);
                    if step >= WINDOW {
                        let old = format!("k{tid}_{:06}", step - WINDOW);
                        del_retry(&db, &old);
                    }
                }
            }));
        }

        let mut reader_handles = Vec::with_capacity(READERS);
        for _ in 0..READERS {
            let db = db.clone();
            reader_handles.push(s.spawn(move || {
                for _ in 0..READ_STEPS {
                    assert_seek_sorted_unique(&db, "k");
                }
            }));
        }

        for handle in writer_handles {
            handle.join().unwrap();
        }
        for handle in reader_handles {
            handle.join().unwrap();
        }
    });

    for _ in 0..120 {
        let view = db.view().unwrap();
        for tid in 0..WRITERS {
            for idx in (0..(WRITE_STEPS - WINDOW)).step_by(4) {
                let key = format!("k{tid}_{idx:06}");
                let _ = view.get(&key);
            }
        }
    }

    let mut end_step = WRITE_STEPS;
    let mut snapshot = observer.snapshot();
    for _ in 0..MAX_EXTRA_ROUNDS {
        let split_count = counter(&snapshot, CounterMetric::TreeNodeSplit);
        let merge_count = counter(&snapshot, CounterMetric::TreeNodeMerge);
        let consolidate_count = counter(&snapshot, CounterMetric::TreeNodeConsolidate);
        if split_count >= MIN_SPLIT_COUNT
            && merge_count >= MIN_MERGE_COUNT
            && consolidate_count >= MIN_CONSOLIDATE_COUNT
        {
            break;
        }
        for tid in 0..WRITERS {
            for step in end_step..(end_step + EXTRA_STEPS) {
                let key = format!("k{tid}_{step:06}");
                upsert_retry(&db, &key);
                let old = format!("k{tid}_{:06}", step - WINDOW);
                del_retry(&db, &old);
            }
        }
        end_step += EXTRA_STEPS;
        for _ in 0..64 {
            assert_seek_sorted_unique(&db, "k");
        }
        snapshot = observer.snapshot();
    }

    assert_seek_sorted_unique(&db, "k");
    let view = db.view().unwrap();
    for tid in 0..WRITERS {
        for idx in 0..(end_step - WINDOW) {
            let key = format!("k{tid}_{idx:06}");
            assert!(view.get(&key).is_err());
        }
        for idx in (end_step - WINDOW)..end_step {
            let key = format!("k{tid}_{idx:06}");
            let r = view.get(&key).expect("must exist");
            assert_eq!(r.slice(), key.as_bytes());
        }
    }
    assert_eq!(view.seek("k").count(), WRITERS * WINDOW);

    let snapshot = observer.snapshot();
    let split_count = counter(&snapshot, CounterMetric::TreeNodeSplit);
    let merge_count = counter(&snapshot, CounterMetric::TreeNodeMerge);
    let consolidate_count = counter(&snapshot, CounterMetric::TreeNodeConsolidate);
    assert!(
        split_count >= MIN_SPLIT_COUNT,
        "split count too low: {split_count}"
    );
    assert!(
        merge_count >= MIN_MERGE_COUNT,
        "merge count too low: {merge_count}"
    );
    assert!(
        consolidate_count >= MIN_CONSOLIDATE_COUNT,
        "consolidate count too low: {consolidate_count}"
    );
    Ok(())
}

fn to_str(x: &[u8]) -> &str {
    unsafe { std::str::from_utf8_unchecked(x) }
}
