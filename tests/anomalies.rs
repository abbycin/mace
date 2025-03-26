//! [Generalized Isolation Level Definitions](https://pmg.csail.mit.edu/papers/icde00.pdf)
use std::{
    collections::HashMap,
    sync::{
        atomic::{
            AtomicBool, AtomicU64,
            Ordering::{AcqRel, Acquire, Release},
        },
        Arc, Mutex,
    },
    thread::JoinHandle,
};

use mace::{Mace, OpCode, Options, RandomPath, TxnKV};

macro_rules! prelude {
    ($($core:expr),+) => {
        {
            let e = Executor::launch(&[$($core),+]);
            ($(e.session($core)),+, e)
        }
    };
    ($($core:expr),+;$tmp:expr) => {
        {
            let e = Executor::launch2(&[$($core),+], $tmp);
            ($(e.session($core)),+,e)
        }
    };
}

#[test]
fn no_p0_1() {
    let (mut s1, mut s2, _e) = prelude!(1, 2; true);

    s1.begin();
    s2.begin();

    let r1 = s1.replace("1", "11").unwrap();
    assert_eq!(r1.as_slice(), "10".as_bytes());
    let r2 = s2.replace("1", "12");
    assert!(r2.is_err() && r2.err().unwrap() == OpCode::AbortTx);

    s1.rollback();
    s2.rollback();

    s1.begin();
    let r = s1.get("1").unwrap();
    assert_eq!(r.as_slice(), "10".as_bytes());
    s1.commit();
}

#[test]
fn no_p0_2() {
    let (mut s1, mut s2, _e) = prelude!(1, 2);

    s1.begin();
    s2.begin();

    s1.replace("1", "11").unwrap();
    let r = s2.replace("1", "12");
    assert!(r.is_err() && r.err().unwrap() == OpCode::AbortTx);

    s1.rollback();
    s2.commit();

    s1.begin();
    let r = s1.get("1").unwrap();
    assert_eq!(r.as_slice(), "10".as_bytes());
    s1.commit();
}

// write cycles
#[test]
fn no_g0() {
    let (mut s1, mut s2, _e) = prelude!(1, 2);

    s1.begin();
    s2.begin();

    s1.replace("1", "11").unwrap();
    let r = s2.replace("1", "12");
    assert!(r.is_err() && r.err().unwrap() == OpCode::AbortTx);
    s1.replace("2", "21").unwrap();

    s1.commit();

    let r = s1.view("1").unwrap();
    assert_eq!(r.as_slice(), "11".as_bytes());

    let r = s1.view("2").unwrap();
    assert_eq!(r.as_slice(), "21".as_bytes());

    let r = s2.replace("2", "22");
    assert!(r.is_err() && r.err().unwrap() == OpCode::AbortTx);
    s2.commit();

    let r = s2.view("1").unwrap();
    assert_eq!(r.as_slice(), "11".as_bytes());

    let r = s2.view("2").unwrap();
    assert_eq!(r.as_slice(), "21".as_bytes());
}

// abort reads
#[test]
fn no_g1_a() {
    let (mut s1, mut s2, _e) = prelude!(1, 2);

    s1.begin();
    s2.begin();
    s1.replace("1", "11").unwrap();

    let r = s2.get("1").unwrap();
    assert_eq!(r.as_slice(), "10".as_bytes());

    s1.rollback();

    let r = s2.get("1").unwrap();
    assert_eq!(r.as_slice(), "10".as_bytes());
    s2.commit();
}

// intermediate reads
#[test]
fn no_g1_b() {
    let (mut s1, mut s2, _e) = prelude!(1, 2);

    s1.begin();
    s2.begin();

    s1.replace("1", "11").unwrap();
    let r = s2.get("1").unwrap();
    assert_eq!(r.as_slice(), "10".as_bytes());

    s1.commit();
    let r = s2.get("1").unwrap();
    assert_eq!(r.as_slice(), "10".as_bytes());

    s2.commit();
}

// cirular information flow
#[test]
fn no_g1_c() {
    let (mut s1, mut s2, _e) = prelude!(1, 2);

    s1.begin();
    s2.begin();
    s1.replace("1", "11").unwrap();
    s2.replace("2", "21").unwrap();

    // expect get old version
    let r = s1.get("2").unwrap();
    assert_eq!(r.as_slice(), "20".as_bytes());

    // expect get old version
    let r = s2.get("1").unwrap();
    assert_eq!(r.as_slice(), "10".as_bytes());

    s1.commit();
    s2.commit();
}

// observed transaction vanishes
#[test]
fn no_otv() {
    let (mut s1, mut s2, mut s3, _e) = prelude!(1, 2, 3);

    s1.begin();
    s2.begin();
    s3.begin();

    s1.replace("1", "11").unwrap();
    s1.replace("2", "21").unwrap();

    let r = s2.replace("1", "12");
    assert!(r.is_err() && r.err().unwrap() == OpCode::AbortTx);
    s1.commit();

    let r = s3.get("1").unwrap();
    assert_eq!(r.as_slice(), "10".as_bytes());

    let r = s2.replace("2", "22");
    assert!(r.is_err() && r.err().unwrap() == OpCode::AbortTx);

    let r = s3.get("2").unwrap();
    assert_eq!(r.as_slice(), "20".as_bytes());
    s2.commit();

    let r = s3.get("1").unwrap();
    assert_eq!(r.as_slice(), "10".as_bytes());

    let r = s3.get("2").unwrap();
    assert_eq!(r.as_slice(), "20".as_bytes());

    s3.commit();
}

// predicate many preceders
#[test]
fn no_pmp() {
    let (mut s1, mut s2, _e) = prelude!(1, 2);

    s1.begin();
    s2.begin();

    let r = s1.get("3");
    assert!(r.is_err() && r.err().unwrap() == OpCode::NotFound);

    s2.put("3", "30");
    s2.commit();

    let r = s1.get("3");
    assert!(r.is_err() && r.err().unwrap() == OpCode::NotFound);

    s1.commit();

    let (mut s1, mut s2, mut s3, _e) = prelude!(1, 2, 3);

    s1.begin();
    s1.put("foo", "bar");
    s1.commit();

    s2.begin();
    s3.begin();

    s2.replace("foo", "+1s").unwrap();
    let r = s3.get("foo");
    assert!(r.is_ok());
    assert_eq!(r.unwrap().as_slice(), "bar".as_bytes());

    s2.rollback();
    let r = s3.get("foo");
    assert!(r.is_ok());
    assert_eq!(r.unwrap().as_slice(), "bar".as_bytes());
}

// lost update
#[test]
fn no_p4() {
    let (mut s1, mut s2, mut s3, _e) = prelude!(1, 2, 3);

    s1.begin();
    s2.begin();

    let r = s1.get("1").unwrap();
    assert_eq!(r.as_slice(), "10".as_bytes());
    let r = s2.get("1").unwrap();
    assert_eq!(r.as_slice(), "10".as_bytes());

    s1.replace("1", "11").unwrap();
    let r = s2.replace("1", "12");
    assert!(r.is_err() && r.err().unwrap() == OpCode::AbortTx);

    s1.commit();
    s2.rollback();

    s3.begin();
    let r = s3.get("1").unwrap();
    assert_eq!(r.as_slice(), "11".as_bytes());
    s3.commit();
}

// single anti-dependency cycles
#[test]
fn no_g2() {
    let (mut s1, mut s2, _e) = prelude!(1, 2);

    s1.begin();
    s2.begin();

    let r = s1.get("1").unwrap();
    assert_eq!(r.as_slice(), "10".as_bytes());

    let r = s2.get("1").unwrap();
    assert_eq!(r.as_slice(), "10".as_bytes());

    let r = s2.get("2").unwrap();
    assert_eq!(r.as_slice(), "20".as_bytes());

    s2.replace("1", "11").unwrap();
    s2.replace("2", "21").unwrap();

    s2.commit();

    let r = s1.get("2").unwrap();
    assert_eq!(r.as_slice(), "20".as_bytes());
    s1.commit();
}

// item anti-dependency cycles
#[test]
#[should_panic]
fn write_skew() {
    let (mut s1, mut s2, e) = prelude!(1, 2);

    s1.begin();
    s2.begin();

    let s1_r1 = s1.get("1").unwrap();
    let s1_r2 = s1.get("2").unwrap();
    assert_eq!(s1_r1.as_slice(), "10".as_bytes());
    assert_eq!(s1_r2.as_slice(), "20".as_bytes());

    let s2_r1 = s2.get("1").unwrap();
    let s2_r2 = s2.get("2").unwrap();
    assert_eq!(s2_r1.as_slice(), "10".as_bytes());
    assert_eq!(s2_r2.as_slice(), "20".as_bytes());

    s1.replace("1", "21").unwrap();
    s2.replace("2", "11").unwrap();

    s1.commit();
    s2.commit();

    let mut s1 = e.session(1);
    let mut s2 = e.session(2);

    s1.begin();
    s2.begin();

    let r1 = s1.get("1").unwrap();
    let r2 = s2.get("2").unwrap();

    s1.commit();
    s2.commit();

    assert_eq!(r1.as_slice(), "21".as_bytes());
    assert_eq!(r2.as_slice(), "11".as_bytes());
    // SI can't prevent write skew, expect panic
    assert_eq!(r1, r2);
}

#[test]
fn history() -> Result<(), OpCode> {
    let (mut s1, mut s2, _e) = prelude!(2, 3);

    s1.begin();
    s1.replace("1", "10").unwrap();
    s1.replace("1", "11").unwrap();
    s1.commit();

    s2.begin();

    s1.begin();
    s1.replace("1", "13").unwrap();
    s1.replace("1", "14").unwrap();
    s1.commit();

    s1.begin();
    s1.replace("1", "15").unwrap();
    s1.replace("1", "16").unwrap();
    s1.commit();

    s1.begin();
    s1.replace("1", "17").unwrap();
    s1.commit();

    s1.begin();
    s1.replace("2", "20").unwrap();
    s1.replace("1", "18").unwrap();
    s1.commit();

    let r = s2.get("1").unwrap();

    s2.commit();

    assert_eq!(r.as_slice(), "11".as_bytes());
    Ok(())
}

//////////////////////// auxiliaries ////////////////////////

struct Executor {
    map: HashMap<usize, (Arc<SyncClosure>, Arc<AtomicBool>)>,
    db: Arc<Mace>,
    handle: Vec<Option<JoinHandle<()>>>,
}

impl Executor {
    fn execute(quit: Arc<AtomicBool>, cond: Arc<SyncClosure>) -> JoinHandle<()> {
        std::thread::spawn(move || {
            let p = cond.consumer();
            while !quit.load(Acquire) {
                p.wait();
                cond.call();
                p.wake();
            }
        })
    }

    fn launch(core: &[usize]) -> Self {
        Self::launch2(core, true)
    }

    fn launch2(workers: &[usize], tmp: bool) -> Self {
        let path = RandomPath::new();
        let mut opt = Options::new(&*path);

        opt.tmp_store = tmp;
        opt.workers = workers.len();
        let db = Arc::new(Mace::new(opt).unwrap());

        let mut map = HashMap::new();
        let mut handle = Vec::new();

        for c in workers {
            let local = Arc::new(AtomicU64::new(Notifier::WAIT));
            let peer = Arc::new(AtomicU64::new(Notifier::WAIT));
            let ln = Notifier {
                local: local.clone(),
                peer: peer.clone(),
            };
            let rn = Notifier {
                local: peer.clone(),
                peer: local.clone(),
            };

            let cond = Arc::new(SyncClosure::new(ln, rn));
            let quit = Arc::new(AtomicBool::new(false));
            handle.push(Some(Self::execute(quit.clone(), cond.clone())));
            map.insert(*c, (cond, quit));
        }

        let this = Self { map, db, handle };
        let mut s = this.session(workers[0]);
        s.begin();
        s.put("1", "10");
        s.put("2", "20");
        s.commit();
        this
    }

    fn session(&self, worker: usize) -> Session {
        let (cond, _) = self.map.get(&worker).expect("invalid core");
        let db = self.db.clone();
        Session {
            db,
            kv: None,
            cond: cond.clone(),
        }
    }
}

impl Drop for Executor {
    fn drop(&mut self) {
        for (_, (cond, quit)) in self.map.iter() {
            quit.store(true, Release);
            cond.producer().wake();
        }
        self.handle
            .iter_mut()
            .map(|x| {
                let j = x.take().unwrap();
                j.join().unwrap();
            })
            .count();
    }
}

struct Session {
    db: Arc<Mace>,
    kv: Option<TxnKV<'static>>,
    cond: Arc<SyncClosure>,
}

struct SyncPtr {
    data: *const u8,
    len: usize,
}

unsafe impl Send for SyncPtr {}
unsafe impl Sync for SyncPtr {}

impl SyncPtr {
    fn data(&self) -> &'static [u8] {
        unsafe { std::slice::from_raw_parts(self.data, self.len) }
    }

    fn from(value: &[u8]) -> Self {
        Self {
            data: value.as_ptr(),
            len: value.len(),
        }
    }
}

impl Clone for SyncPtr {
    fn clone(&self) -> Self {
        Self {
            data: self.data,
            len: self.len,
        }
    }
}

#[derive(Clone)]
struct Closure {
    data: *mut (),
}

impl Closure {
    fn new<F>(p: F) -> Self
    where
        F: FnMut(),
    {
        let cb: Box<dyn FnMut()> = Box::new(p);
        let wrapped = Box::new(cb);
        Self {
            data: Box::into_raw(wrapped) as *mut (),
        }
    }

    fn call(self) {
        unsafe {
            let tmp = self.data as *mut Box<dyn FnMut()>;
            let mut fp = Box::from_raw(tmp);
            fp();
        }
    }
}

unsafe impl Send for Closure {}
unsafe impl Sync for Closure {}

#[derive(Clone)]
struct Notifier {
    local: Arc<AtomicU64>,
    peer: Arc<AtomicU64>,
}

impl Notifier {
    const WAIT: u64 = 3;
    const WAKE: u64 = 7;

    fn wait(&self) {
        while self.local.swap(Self::WAIT, AcqRel) == Self::WAIT {
            std::hint::spin_loop();
        }
    }

    fn wake(&self) {
        self.peer.store(Self::WAKE, Release);
    }
}

struct SyncClosure {
    l: Notifier,
    r: Notifier,
    mtx: Arc<Mutex<Option<Closure>>>,
}

impl SyncClosure {
    fn new(l: Notifier, r: Notifier) -> Self {
        Self {
            l,
            r,
            mtx: Arc::new(Mutex::new(None)),
        }
    }

    fn set_fn(&self, c: Closure) {
        let mut lk = self.mtx.lock().unwrap();
        assert!(lk.is_none());
        *lk = Some(c);
    }

    fn call(&self) {
        let mut lk = self.mtx.lock().unwrap();
        let x = lk.take();
        if let Some(f) = x {
            f.call();
        }
    }

    fn producer(&self) -> &Notifier {
        &self.l
    }

    fn consumer(&self) -> &Notifier {
        &self.r
    }
}

impl Session {
    fn begin(&mut self) {
        self.cond.set_fn(Closure::new(|| {
            let kv = unsafe {
                std::mem::transmute::<mace::TxnKV<'_>, mace::TxnKV<'_>>(self.db.begin().unwrap())
            };
            self.kv = Some(kv);
        }));
        self.sync();
    }

    fn view(&mut self, k: impl AsRef<[u8]>) -> Result<Vec<u8>, OpCode> {
        let mut rv = None;
        self.cond.set_fn(Closure::new(|| {
            let view = self.db.view().unwrap();
            rv = Some(view.get(k.as_ref()).map(|x| x.data().to_vec()));
        }));
        self.sync();
        rv.unwrap()
    }

    fn commit(&mut self) {
        self.cond.set_fn(Closure::new(|| {
            let kv = self.kv.take().unwrap();
            let _ = kv.commit();
        }));
        self.sync();
    }

    fn rollback(&mut self) {
        self.cond.set_fn(Closure::new(|| {
            let kv = self.kv.take().unwrap();
            let _ = kv.rollback();
        }));
        self.sync();
    }

    fn put<K: AsRef<[u8]>>(&mut self, k: K, v: K) {
        let kv = self.kv.as_ref().unwrap();

        let (k, v) = (SyncPtr::from(k.as_ref()), SyncPtr::from(v.as_ref()));
        self.cond.set_fn(Closure::new(|| {
            kv.put(k.data(), v.data()).expect("can't put");
        }));

        self.sync();
    }

    fn replace<T: AsRef<[u8]>>(&mut self, k: T, v: T) -> Result<Vec<u8>, OpCode> {
        let kv = self.kv.as_ref().unwrap();
        let (k, v) = (SyncPtr::from(k.as_ref()), SyncPtr::from(v.as_ref()));
        let mut rv = None;
        self.cond.set_fn(Closure::new(|| {
            let r = kv.update(k.data(), v.data());
            rv = Some(r.map(|x| x.data().to_vec()));
        }));
        self.sync();
        rv.unwrap()
    }

    fn get<K: AsRef<[u8]>>(&mut self, k: K) -> Result<Vec<u8>, OpCode> {
        self.get_or_del(k, false)
    }

    #[allow(unused)]
    fn del<K: AsRef<[u8]>>(&mut self, k: K) -> Result<Vec<u8>, OpCode> {
        self.get_or_del(k, true)
    }

    fn get_or_del<K: AsRef<[u8]>>(&mut self, k: K, is_del: bool) -> Result<Vec<u8>, OpCode> {
        let kv = self.kv.as_ref().unwrap();

        let k = SyncPtr::from(k.as_ref());
        let mut v = None;

        self.cond.set_fn(Closure::new(|| {
            let r = if is_del {
                kv.del(k.data())
            } else {
                kv.get(k.data())
            };
            v = Some(r.map(|x| x.data().to_vec()));
        }));
        self.sync();
        v.unwrap()
    }

    fn sync(&self) {
        let p = self.cond.producer();
        p.wake();
        p.wait();
    }
}
