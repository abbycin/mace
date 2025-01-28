use std::{
    collections::HashSet,
    sync::{Barrier, RwLock},
};

use mace::{IsolationLevel, Mace, OpCode, Options, RandomPath, Tx};
use rand::{seq::SliceRandom, thread_rng};

#[test]
fn put_get() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let opt = Options::new(&*path);
    let db = Mace::open(opt)?;
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
            barrier.wait();
            for i in &elems {
                let _ = tx.begin(IsolationLevel::SI, |kv| {
                    let _ = kv.put(*i, *i);
                    kv.commit()
                });
            }
        });

        s.spawn(|| {
            barrier.wait();
            for i in &elems {
                let _ = tx.begin(IsolationLevel::SI, |kv| {
                    if let Ok(x) = kv.del(*i) {
                        assert_eq!(x.data(), *i);
                        del1.write().unwrap().insert(x.data().to_vec());
                    }
                    kv.commit()
                });
            }
        });

        s.spawn(|| {
            barrier.wait();
            for i in &elems {
                let _ = tx.begin(IsolationLevel::SI, |kv| {
                    if let Ok(x) = kv.del(*i) {
                        assert_eq!(x.data(), *i);
                        del2.write().unwrap().insert(x.data().to_vec());
                    }
                    kv.commit()
                });
            }
        });
    });

    check(&tx, &elems, &del1, &del2);

    let _ = tx.begin(IsolationLevel::SI, |kv| {
        kv.put("foo", "bar").unwrap();
        kv.commit()
    });

    let r = db.remove(tx);
    assert!(r.is_ok());

    let tx = db.default();
    let _ = tx.begin(IsolationLevel::SI, |kv| {
        let r = kv.get("foo");
        assert!(r.is_err());
        kv.commit()
    });

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
    let _ = tx.view(IsolationLevel::SI, |view| {
        for i in elems.iter() {
            let tmp = i.to_vec();
            if lk1.contains(&tmp) || lk2.contains(&tmp) {
                continue;
            }
            let r = view.get(*i).expect("must exist");
            if r.data() != *i {
                assert!(!lk1.contains(r.data()));
                assert_eq!(r.data(), *i);
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
        Ok(())
    });
}

#[test]
fn get_del() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let opt = Options::new(&*path);
    let db = Mace::open(opt)?;
    let tx = db.default();

    let n = 1000;
    let mut v = Vec::with_capacity(n);

    for i in 0..n {
        v.push(format!("elem_{i}"));
    }

    for i in &v {
        let _ = tx.begin(IsolationLevel::SI, |kv| {
            kv.put(i.as_bytes(), i.as_bytes()).expect("can't put");
            kv.commit()
        });
    }

    for i in &v {
        let _ = tx.view(IsolationLevel::SI, |view| {
            view.get(i.as_bytes()).expect("can't get");
            Ok(())
        });
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
        let _ = tx.begin(IsolationLevel::SI, |kv| {
            kv.del(v[i].as_bytes()).expect("can't del");
            kv.commit()
        });
    }

    for i in &removed {
        let _ = tx.view(IsolationLevel::SI, |view| {
            let r = view.get(i.as_bytes());
            assert!(r.is_err());
            Ok(())
        });
    }

    Ok(())
}
