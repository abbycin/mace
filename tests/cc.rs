use mace::{Mace, OpCode, Options, RandomPath};
use rand::{seq::SliceRandom, thread_rng};
use std::thread::sleep;
use std::time::Duration;
use std::{
    collections::HashSet,
    sync::{Barrier, RwLock},
};

#[test]
fn put_get() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.buffer_count = 5;
    let db = Mace::new(opt.validate().unwrap())?;

    let n = 1000;
    let mut container = Vec::with_capacity(n);
    for i in 0..n {
        container.push(format!("elem_{i}"));
    }
    let elems: Vec<&[u8]> = container.iter().map(|x| x.as_bytes()).collect();
    let del1: RwLock<HashSet<Vec<u8>>> = RwLock::new(HashSet::new());
    let del2: RwLock<HashSet<Vec<u8>>> = RwLock::new(HashSet::new());
    let barrier = Barrier::new(3);

    std::thread::scope(|s| {
        s.spawn(|| {
            barrier.wait();
            for i in &elems {
                let kv = db.begin().unwrap();
                let _ = kv.put(*i, *i);
                kv.commit().unwrap();
            }
        });

        s.spawn(|| {
            barrier.wait();
            for i in &elems {
                let kv = db.begin().unwrap();
                if let Ok(x) = kv.del(*i) {
                    assert_eq!(x.slice(), *i);
                    del1.write().unwrap().insert(x.slice().to_vec());
                }
                kv.commit().unwrap();
            }
        });

        s.spawn(|| {
            barrier.wait();
            for i in &elems {
                let kv = db.begin().unwrap();
                if let Ok(x) = kv.del(*i) {
                    assert_eq!(x.slice(), *i);
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
    elems: &Vec<&[u8]>,
    del1: &RwLock<HashSet<Vec<u8>>>,
    del2: &RwLock<HashSet<Vec<u8>>>,
) {
    let lk1 = del1.read().unwrap();
    let lk2 = del2.read().unwrap();
    let view = db.view().unwrap();
    for i in elems.iter() {
        let tmp = i.to_vec();
        if lk1.contains(&tmp) || lk2.contains(&tmp) {
            continue;
        }
        let r = view.get(*i).expect("must exist");
        if r.slice() != *i {
            assert!(!lk1.contains(r.slice()));
            assert_eq!(r.slice(), *i);
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
    let mut rng = thread_rng();

    v.shuffle(&mut rng);

    for (i, k) in v.iter().enumerate() {
        if i == cnt {
            break;
        }
        removed.push(k.clone());
        let kv = db.begin().unwrap();
        kv.del(v[i].as_bytes()).expect("can't del");
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
    assert_eq!(iter.next(), Some(("foo".as_bytes(), "3".as_bytes())));
    assert_eq!(iter.next(), Some(("fool".as_bytes(), "1".as_bytes())));
    assert_eq!(iter.next(), None);

    let mut iter = view.seek("mo");
    assert_eq!(iter.next(), Some(("mo".as_bytes(), "3".as_bytes())));
    assert_eq!(iter.next(), None);

    let kv = db.begin().unwrap();
    kv.del("foo")?;
    let mut iter = kv.seek("foo");
    assert_eq!(iter.next(), Some(("fool".as_bytes(), "1".as_bytes())));
    assert_eq!(iter.next(), None);
    kv.rollback()?;

    {
        let mut opts = Options::new(&*RandomPath::new());
        opts.consolidate_threshold = 10;
        opts.tmp_store = true;
        let db = Mace::new(opts.validate().unwrap()).unwrap();

        let kv = db.begin().unwrap();
        kv.put([0], "bar")?;
        kv.put([0, 1], "bar")?;
        kv.put([0, 2], "bar")?;
        kv.commit()?;

        let view = db.view().unwrap();
        let mut iter = view.seek([0]);
        assert_eq!(iter.next(), Some(([0].as_slice(), "bar".as_bytes())));
        assert_eq!(iter.next(), Some(([0, 1].as_slice(), "bar".as_bytes())));
        assert_eq!(iter.next(), Some(([0, 2].as_slice(), "bar".as_bytes())));
    }
    Ok(())
}

#[test]
fn range_in_one_node() -> Result<(), OpCode> {
    let mut opts = Options::new(&*RandomPath::new());
    opts.consolidate_threshold = 10;
    opts.tmp_store = true;
    let db = Mace::new(opts.validate().unwrap()).unwrap();
    const N: usize = 10;

    let check_app = || {
        let mut words = Vec::new();
        let view = db.view().unwrap();
        for (k, v) in view.seek("app") {
            words.push((k.to_vec(), v.to_vec()));
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
            let k = format!("key_{}", i);
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
        let x = format!("key_{}", i);
        keys.insert(x);
    }

    let mut cnt = 0;
    let view = db.view().unwrap();
    for (k, v) in view.seek("key") {
        cnt += 1;
        assert_eq!(k, v);
        assert!(keys.contains(to_str(k)));
    }

    assert_eq!(cnt, N);
    // check again
    check_app();

    Ok(())
}

#[test]
fn range_cross_node() -> Result<(), OpCode> {
    let mut opts = Options::new(&*RandomPath::new());
    opts.consolidate_threshold = 8;
    opts.page_size = 1024; // force split
    opts.sync_on_write = false;
    opts.tmp_store = true;
    let db = Mace::new(opts.validate().unwrap()).unwrap();
    const N: usize = 500;
    let mut h = HashSet::with_capacity(N);

    for i in 0..N {
        let x = format!("key_{}", i);
        let kv = db.begin().unwrap();
        kv.put(&x, &x)?;
        kv.commit()?;
        h.insert(x);
    }

    let check_key = || {
        let mut cnt = 0;
        let view = db.view().unwrap();
        for (k, v) in view.seek("key") {
            cnt += 1;
            assert_eq!(k, v);
            assert!(h.contains(to_str(k)));
        }
        assert_eq!(cnt, h.len());
    };

    check_key();

    let kv = db.begin().unwrap();
    for i in 0..40 {
        let a = format!("aa_{}", i);
        kv.put(&a, "bar")?;
    }
    kv.commit()?;

    // check again
    check_key();

    let kv = db.begin().unwrap();
    for i in 0..40 {
        let z = format!("zz_{}", i);
        kv.put(&z, "bar")?;
    }
    kv.commit()?;

    // check again
    check_key();

    Ok(())
}

fn to_str(x: &[u8]) -> &str {
    std::str::from_utf8(x).unwrap()
}
