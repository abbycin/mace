use std::{
    collections::HashSet,
    sync::{Barrier, RwLock},
};

use coreid::bind_core;
use mace::{Db, IsolationLevel, OpCode, Options, RandomPath, Tx};
use rand::{seq::SliceRandom, thread_rng};

#[test]
fn put_get() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let opt = Options::new(&*path);
    let db = Db::open(opt)?;
    let tx = db.default();

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
            bind_core(1);
            barrier.wait();
            for i in &elems {
                let kv = tx.begin(IsolationLevel::SI);
                let _ = kv.put(*i, *i);
                tx.commit(kv);
            }
        });

        s.spawn(|| {
            bind_core(2);
            barrier.wait();
            for i in &elems {
                let kv = tx.begin(IsolationLevel::SI);
                if let Ok(x) = kv.del(*i) {
                    assert_eq!(x.data(), *i);
                    del1.write().unwrap().insert(x.data().to_vec());
                }
                tx.commit(kv);
            }
        });

        s.spawn(|| {
            bind_core(3);
            barrier.wait();
            for i in &elems {
                let kv = tx.begin(IsolationLevel::SI);
                if let Ok(x) = kv.del(*i) {
                    assert_eq!(x.data(), *i);
                    del2.write().unwrap().insert(x.data().to_vec());
                }
                tx.commit(kv);
            }
        });
    });

    check(&tx, &elems, &del1, &del2);

    let kv = tx.begin(IsolationLevel::SI);
    kv.put("foo", "bar").unwrap();
    tx.commit(kv);

    let r = db.remove(tx);
    assert!(r.is_ok());

    let tx = db.default();
    let kv = tx.begin(IsolationLevel::SI);
    let r = kv.get("foo");
    assert!(r.is_err());
    tx.commit(kv);

    Ok(())
}

fn check(
    tx: &Tx,
    elems: &Vec<&[u8]>,
    del1: &RwLock<HashSet<Vec<u8>>>,
    del2: &RwLock<HashSet<Vec<u8>>>,
) {
    let lk1 = del1.read().unwrap();
    let lk2 = del2.read().unwrap();
    let kv = tx.view(IsolationLevel::SI);
    for i in elems.iter() {
        let tmp = i.to_vec();
        if lk1.contains(&tmp) || lk2.contains(&tmp) {
            continue;
        }
        let r = kv.get(*i).expect("must exist");
        if r.data() != *i {
            assert!(!lk1.contains(r.data()));
            assert_eq!(r.data(), *i);
        }
    }

    for i in lk1.iter() {
        let r = kv.get(i.as_slice());
        assert!(r.is_err());
    }

    for i in lk2.iter() {
        let r = kv.get(i.as_slice());
        assert!(r.is_err());
    }
}

#[allow(dead_code)]
fn to_str(x: &[u8]) -> &str {
    unsafe { std::str::from_utf8_unchecked(x) }
}

#[test]
fn get_del() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let opt = Options::new(&*path);
    let db = Db::open(opt)?;
    let tx = db.default();

    let n = 1000;
    let mut v = Vec::with_capacity(n);

    for i in 0..n {
        v.push(format!("elem_{i}"));
    }

    for i in &v {
        let kv = tx.begin(IsolationLevel::SI);
        kv.put(i.as_bytes(), i.as_bytes()).expect("can't put");
        tx.commit(kv);
    }

    for i in &v {
        let kv = tx.view(IsolationLevel::SI);
        kv.get(i.as_bytes()).expect("can't get");
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
        let kv = tx.begin(IsolationLevel::SI);
        kv.del(v[i].as_bytes()).expect("can't del");
        tx.commit(kv);
    }

    for i in &removed {
        let kv = tx.view(IsolationLevel::SI);
        let r = kv.get(i.as_bytes());
        assert!(r.is_err());
    }

    Ok(())
}
