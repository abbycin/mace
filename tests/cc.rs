use mace::{Mace, OpCode, Options, RandomPath};
use rand::seq::SliceRandom;
use std::{
    collections::HashSet,
    sync::{Arc, Barrier, RwLock},
    thread::{JoinHandle, sleep},
    time::Duration,
};

#[test]
fn put_get() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let opt = Options::new(&*path);
    let db = Mace::new(opt.validate().unwrap())?;

    let n = 1000;
    let mut elems = Vec::with_capacity(n);
    for i in 0..n {
        elems.push(format!("elem_{i}"));
    }
    let del1: RwLock<HashSet<Vec<u8>>> = RwLock::new(HashSet::new());
    let del2: RwLock<HashSet<Vec<u8>>> = RwLock::new(HashSet::new());
    let barrier = Barrier::new(3);

    std::thread::scope(|s| {
        s.spawn(|| {
            barrier.wait();
            for i in &elems {
                let kv = db.begin().unwrap();
                let _ = kv.put(i, i);
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

    check(&db, &elems, &del1, &del2);

    let kv = db.begin().unwrap();
    kv.put("foo", "bar").unwrap();
    kv.commit()?;

    Ok(())
}

fn check(
    db: &Mace,
    elems: &[String],
    del1: &RwLock<HashSet<Vec<u8>>>,
    del2: &RwLock<HashSet<Vec<u8>>>,
) {
    let lk1 = del1.read().unwrap();
    let lk2 = del2.read().unwrap();
    let view = db.view().unwrap();
    for i in elems.iter() {
        let tmp = i.as_bytes().to_vec();
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
    let db = Mace::new(opt.validate().unwrap())?;

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

    Ok(())
}

#[test]
fn rollback() {
    let workers = 12;
    let mut opts = Options::new(&*RandomPath::new());
    opts.tmp_store = true;
    opts.sync_on_write = false;
    opts.workers = workers;
    let db = Mace::new(opts.validate().unwrap()).unwrap();
    const N: usize = 10000;

    // multiple threads trying to update the same key, some of them will pass the visibility check and
    // start update the key, but only one of them may success, those failure threads will check the
    // visibility again and find the new key is invisible to them, they will abort the transaction
    // and rollback what they have wrote to the log
    fn update(db: &Mace, pairs: &Vec<Vec<u8>>) {
        for x in pairs {
            let kv = db.begin().unwrap();
            if kv.update(x, x).is_ok() {
                kv.commit().unwrap();
            }
            // automatically rollback
        }
    }

    for t in [2, 6, workers] {
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
                    let db = db.clone();
                    let pairs = pairs.clone();
                    std::thread::spawn(move || update(&db, &pairs))
                })
                .collect();

            for x in h {
                x.join().unwrap();
            }
        }
    }
}

#[test]
fn range_simple() -> Result<(), OpCode> {
    let mut opts = Options::new(&*RandomPath::new());
    opts.tmp_store = true;
    let db = Mace::new(opts.validate().unwrap()).unwrap();

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

    let view = db.view().unwrap();
    let mut iter = view.seek("foo");
    let item = iter.next().unwrap();
    assert_eq!(item.key(), "foo".as_bytes());
    assert_eq!(item.val(), "3".as_bytes());
    let item = iter.next().unwrap();
    assert_eq!(item.key(), "fool".as_bytes());
    assert_eq!(item.val(), "1".as_bytes());

    assert!(iter.next().is_none());

    let mut iter = view.seek("mo");
    let item = iter.next().unwrap();
    assert_eq!(item.key(), "mo".as_bytes());
    assert_eq!(item.val(), "3".as_bytes());
    assert!(iter.next().is_none());

    let kv = db.begin().unwrap();
    kv.del("foo")?;
    let mut iter = kv.seek("foo");
    let item = iter.next().unwrap();
    assert_eq!(item.key(), "fool".as_bytes());
    assert_eq!(item.val(), "1".as_bytes());
    assert!(iter.next().is_none());
    drop(kv);

    {
        let mut opts = Options::new(&*RandomPath::new());
        opts.tmp_store = true;
        let db = Mace::new(opts.validate().unwrap()).unwrap();

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
    }
    Ok(())
}

#[test]
fn range_in_one_node() -> Result<(), OpCode> {
    let mut opts = Options::new(&*RandomPath::new());
    opts.tmp_store = true;
    let db = Mace::new(opts.validate().unwrap()).unwrap();
    const N: usize = 10;

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

    let mut cnt = 0;
    let view = db.view().unwrap();
    for item in view.seek("key") {
        cnt += 1;
        let k = item.key();
        let v = item.val();
        assert_eq!(k, v);
        assert!(keys.contains(to_str(&k)));
    }

    assert_eq!(cnt, N);
    // check again
    check_app();

    Ok(())
}

#[test]
fn range_cross_node() -> Result<(), OpCode> {
    let mut opts = Options::new(&*RandomPath::new());
    opts.split_elems = 128; // force split
    opts.sync_on_write = false;
    opts.tmp_store = true;
    let db = Mace::new(opts.validate().unwrap()).unwrap();
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
            assert!(h.contains(to_str(&k)));
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

    Ok(())
}

#[test]
fn cross_txn() {
    let path = RandomPath::new();
    let mut opt = Options::new(&*path);
    opt.tmp_store = true;
    opt.workers = 3;
    let db = Mace::new(opt.validate().unwrap()).unwrap();

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

    let tx2 = db.view().unwrap();
    let iter = tx2.seek("fo");
    assert_eq!(iter.count(), 2);

    let tx1 = db.begin().unwrap();
    tx1.del("foo").unwrap();

    let iter = tx1.seek("fo");
    assert_eq!(iter.count(), 1);

    let view = db.view().unwrap();
    let iter = view.seek("fo");
    assert_eq!(iter.count(), 2);

    let _ = tx1.commit();
    let iter = view.seek("fo");
    assert_eq!(iter.count(), 2);

    let view = db.view().unwrap();
    let iter = view.seek("fo");
    assert_eq!(iter.count(), 1);
    let r = view.get("fool").unwrap();
    assert_eq!(r.slice(), "2".as_bytes());
}

#[test]
fn smo_during_scan() -> Result<(), OpCode> {
    let path = RandomPath::new();
    let mut opt = Options::new(&*path);
    opt.tmp_store = true;
    opt.split_elems = 128;

    let db = Mace::new(opt.validate()?)?;
    let data: Vec<String> = (0..512).map(|x| format!("key_{x}")).collect();
    let mut target = Vec::new();

    for (i, x) in data.iter().enumerate() {
        if i % 2 == 0 {
            let kv = db.begin()?;
            kv.put(x, x)?;
            kv.commit()?;
            target.push(x.clone());
        }
    }

    target.sort();

    let view = db.view()?;
    let mut iter = view.seek("key");
    let mut idx = 0;
    for (i, x) in data.iter().enumerate() {
        if i % 2 != 0 {
            let kv = db.begin()?;
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
        let kv = db.begin()?;
        kv.del(x)?;
        kv.commit()?;

        if let Some(item) = iter.next() {
            assert_eq!(item.key(), target[idx].as_bytes());
            idx += 1;
        }
    }
    assert_eq!(idx, target.len());

    Ok(())
}

fn to_str(x: &[u8]) -> &str {
    unsafe { std::str::from_utf8_unchecked(x) }
}
