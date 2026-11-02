//! [A Critique of ANSI SQL Isolation Levels](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-95-51.pdf)
//! [Generalized Isolation Level Definitions](https://pmg.csail.mit.edu/papers/icde00.pdf)
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{
            AtomicBool, AtomicU64,
            Ordering::{AcqRel, Acquire, Release},
        },
    },
    thread::JoinHandle,
};

use mace::{Bucket, BucketOptions, Mace, OpCode, Options, RandomPath, TxnKV};

#[path = "anomalies/trace.rs"]
mod trace;

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
    let (mut s1, mut s2, e) = prelude!(1, 2; true);

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
    assert_eq!(e.trace_len(), 13, "no_p0_1 event count");
    e.finish(0);
}

#[test]
fn no_p0_2() {
    let (mut s1, mut s2, e) = prelude!(1, 2);

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
    assert_eq!(e.trace_len(), 13, "no_p0_2 event count");
    e.finish(0);
}

// write cycles
#[test]
fn no_g0() {
    let (mut s1, mut s2, e) = prelude!(1, 2);

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
    assert_eq!(e.trace_len(), 18, "no_g0 event count");
    e.finish(0);
}

// abort reads
#[test]
fn no_g1_a() {
    let (mut s1, mut s2, e) = prelude!(1, 2);

    s1.begin();
    s2.begin();
    s1.replace("1", "11").unwrap();

    let r = s2.get("1").unwrap();
    assert_eq!(r.as_slice(), "10".as_bytes());

    s1.rollback();

    let r = s2.get("1").unwrap();
    assert_eq!(r.as_slice(), "10".as_bytes());
    s2.commit();
    assert_eq!(e.trace_len(), 10, "no_g1_a event count");
    e.finish(0);
}

#[test]
fn aborted_head_allows_followup_update() {
    let (mut s1, mut s2, mut s3, e) = prelude!(1, 2, 3);

    s1.begin();
    let old = s1.replace("1", "11").unwrap();
    assert_eq!(old.as_slice(), "10".as_bytes());
    s1.rollback();

    s2.begin();
    let old = s2.replace("1", "12").unwrap();
    assert_eq!(old.as_slice(), "10".as_bytes());
    s2.commit();

    s3.begin();
    let now = s3.get("1").unwrap();
    assert_eq!(now.as_slice(), "12".as_bytes());
    s3.commit();
    assert_eq!(
        e.trace_len(),
        13,
        "aborted_head_allows_followup_update event count"
    );
    e.finish(0);
}

#[test]
fn stacked_aborted_versions_are_removed_before_followup_write() {
    let (mut s1, mut s2, mut s3, e) = prelude!(1, 2, 3);

    s1.begin();
    let old = s1.replace("1", "11").unwrap();
    assert_eq!(old.as_slice(), "10".as_bytes());
    let old = s1.replace("1", "12").unwrap();
    assert_eq!(old.as_slice(), "11".as_bytes());
    let old = s1.replace("1", "13").unwrap();
    assert_eq!(old.as_slice(), "12".as_bytes());
    s1.rollback();

    s2.begin();
    let old = s2.replace("1", "14").unwrap();
    assert_eq!(old.as_slice(), "10".as_bytes());
    s2.commit();

    s3.begin();
    let now = s3.get("1").unwrap();
    assert_eq!(now.as_slice(), "14".as_bytes());
    s3.commit();
    assert_eq!(
        e.trace_len(),
        17,
        "stacked_aborted_versions_are_removed_before_followup_write event count"
    );
    e.finish(0);
}

#[test]
fn aborted_delete_head_allows_followup_update() {
    let (mut s1, mut s2, mut s3, _e) = prelude!(1, 2, 3);

    s1.begin();
    s1.del("1").unwrap();
    s1.rollback();

    s2.begin();
    let old = s2.replace("1", "12").unwrap();
    assert_eq!(old.as_slice(), "10".as_bytes());
    s2.commit();

    s3.begin();
    let now = s3.get("1").unwrap();
    assert_eq!(now.as_slice(), "12".as_bytes());
    s3.commit();
}

#[test]
fn aborted_head_must_not_hide_newer_write_conflict() {
    let (mut s1, mut s2, mut s3, e) = prelude!(1, 2, 3);

    s1.begin();
    s2.begin();

    let old = s2.replace("1", "12").unwrap();
    assert_eq!(old.as_slice(), "10".as_bytes());
    s2.commit();

    s3.begin();
    let old = s3.replace("1", "13").unwrap();
    assert_eq!(old.as_slice(), "12".as_bytes());

    s3.rollback();

    let r = s1.replace("1", "11");
    assert_eq!(r, Err(OpCode::AbortTx));
    s1.rollback();
    assert_eq!(
        e.trace_len(),
        14,
        "aborted_head_must_not_hide_newer_write_conflict event count"
    );
    e.finish(0);
}

// intermediate reads
#[test]
fn no_g1_b() {
    let (mut s1, mut s2, e) = prelude!(1, 2);

    s1.begin();
    s2.begin();

    s1.replace("1", "11").unwrap();
    let r = s2.get("1").unwrap();
    assert_eq!(r.as_slice(), "10".as_bytes());

    s1.commit();
    let r = s2.get("1").unwrap();
    assert_eq!(r.as_slice(), "10".as_bytes());

    s2.commit();
    assert_eq!(e.trace_len(), 10, "no_g1_b event count");
    e.finish(0);
}

// circular information flow
#[test]
fn no_g1_c() {
    let (mut s1, mut s2, e) = prelude!(1, 2);

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
    assert_eq!(e.trace_len(), 12, "no_g1_c event count");
    e.finish(0);
}

// observed transaction vanishes
#[test]
fn no_otv() {
    let (mut s1, mut s2, mut s3, e) = prelude!(1, 2, 3);

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
    assert_eq!(e.trace_len(), 20, "no_otv event count");
    e.finish(0);
}

// predicate many preceders
#[test]
fn no_pmp() {
    let (mut s1, mut s2, e) = prelude!(1, 2);

    s1.begin();
    s2.begin();

    let r = s1.get("3");
    assert!(r.is_err() && r.err().unwrap() == OpCode::NotFound);

    s2.put("3", "30");
    s2.commit();

    let r = s1.get("3");
    assert!(r.is_err() && r.err().unwrap() == OpCode::NotFound);

    s1.commit();

    let (mut s1, mut s2, mut s3, e2) = prelude!(1, 2, 3);

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
    drop(s3);
    assert_eq!(e.trace_len(), 9, "first executor event count");
    e.finish(0);
    assert_eq!(e2.trace_len(), 13, "second executor event count");
    e2.finish(0);
}

// lost update
#[test]
fn no_p4() {
    let (mut s1, mut s2, mut s3, e) = prelude!(1, 2, 3);

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
    assert_eq!(e.trace_len(), 15, "no_p4 event count");
    e.finish(0);
}

// single anti-dependency cycles
#[test]
fn no_g2() {
    let (mut s1, mut s2, e) = prelude!(1, 2);

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
    assert_eq!(e.trace_len(), 14, "no_g2 event count");
    e.finish(0);
}

// item anti-dependency cycles
#[test]
fn write_skew_is_allowed_under_snapshot_isolation() {
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

    s1.replace("1", &s1_r2).unwrap();
    s2.replace("2", &s2_r1).unwrap();

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

    assert_eq!(r1, s1_r2);
    assert_eq!(r2, s2_r1);
    // a serializable schedule would leave "1" and "2" at the same value
    // ("10" or "20", depending on the order of s1 and s2); snapshot
    // isolation permits the skew, so both pre-snapshot values survive and
    // the pair ends inconsistent. pin that outcome explicitly — a bare
    // should_panic would also accept an unrelated engine panic as "skew"
    assert_ne!(
        r1, r2,
        "snapshot isolation permits write skew; the pair must end inconsistent"
    );
    assert_eq!(
        e.trace_len(),
        20,
        "write_skew_is_allowed_under_snapshot_isolation event count"
    );
    e.finish(0);
}

#[test]
fn history() -> Result<(), OpCode> {
    let (mut s1, mut s2, e) = prelude!(2, 3);

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
    assert_eq!(e.trace_len(), 33, "history event count");
    e.finish(1);
    Ok(())
}

//////////////////////// auxiliaries ////////////////////////

struct Executor {
    map: HashMap<usize, (Arc<SyncClosure>, Arc<AtomicBool>)>,
    db: Arc<Mace>,
    _path: Arc<RandomPath>,
    handle: Vec<Option<JoinHandle<()>>>,
    trace: Arc<trace::Recorder>,
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
        let path = RandomPath::tmp();
        let mut opt = Options::new(&*path);

        opt.tmp_store = tmp;
        opt.concurrent_write = workers.len().next_power_of_two() as u8;
        let db = Arc::new(Mace::new(opt.validate().unwrap()).unwrap());
        db.new_bucket("xx", BucketOptions::default()).unwrap();

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

        let this = Self {
            map,
            db,
            _path: Arc::new(path),
            handle,
            trace: Arc::new(trace::Recorder::new()),
        };
        let mut s = this.seed_session(workers[0]);
        s.begin();
        s.put("1", "10");
        s.put("2", "20");
        s.commit();
        this
    }

    fn seed_session(&self, worker: usize) -> Session {
        let mut session = self.session(worker);
        session.seed = true;
        session
    }

    fn finish(&self, expected_inconclusive: usize) {
        let verdict = self.trace.verdict();
        assert!(
            verdict.accepted(),
            "anomaly trace rejected:\n{}",
            verdict.describe()
        );
        assert_eq!(
            verdict.inconclusive,
            expected_inconclusive,
            "unexpected inconclusive count:\n{}",
            verdict.describe()
        );
    }

    fn trace_len(&self) -> usize {
        self.trace.len()
    }

    fn session(&self, worker: usize) -> Session {
        let (cond, _) = self.map.get(&worker).expect("invalid core");
        let db = Arc::new(self.db.open_bucket("xx").unwrap());
        Session {
            kv: None,
            db,
            cond: cond.clone(),
            _path: self._path.clone(),
            trace: self.trace.clone(),
            txn: None,
            seed: false,
        }
    }
}

impl Drop for Executor {
    fn drop(&mut self) {
        for (cond, quit) in self.map.values() {
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
    // NOTE: field order matters! kv must be dropped before db
    // TxnKV::drop accesses Context, which is held alive by db
    kv: Option<TxnKV<'static>>,
    db: Arc<Bucket>,
    cond: Arc<SyncClosure>,
    _path: Arc<RandomPath>,
    trace: Arc<trace::Recorder>,
    txn: Option<trace::TxnId>,
    seed: bool,
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
            std::thread::yield_now();
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

fn observed_bytes(result: &Result<Vec<u8>, OpCode>) -> Option<Vec<u8>> {
    match result {
        Ok(bytes) => Some(bytes.clone()),
        Err(OpCode::NotFound) => None,
        Err(other) => panic!("unexpected read error: {other:?}"),
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        if let Some(txn) = self.txn.take() {
            self.trace.drop_txn(txn);
        }
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
        if !self.seed {
            let txn = self.trace.alloc_txn();
            self.txn = Some(txn);
            self.trace.begin(txn);
        }
    }

    fn view(&mut self, k: impl AsRef<[u8]>) -> Result<Vec<u8>, OpCode> {
        let mut rv = None;
        self.cond.set_fn(Closure::new(|| {
            let view = self.db.view().unwrap();
            rv = Some(view.get(k.as_ref()).map(|x| x.slice().to_vec()));
        }));
        self.sync();
        let rv = rv.unwrap();
        let bytes = observed_bytes(&rv);
        self.trace
            .read(trace::Owner::View, k.as_ref(), bytes.as_deref());
        rv
    }

    fn commit(&mut self) {
        let mut outcome = None;
        self.cond.set_fn(Closure::new(|| {
            let kv = self.kv.take().unwrap();
            outcome = Some(kv.commit());
        }));
        self.sync();
        let txn = self.txn.take();
        if let (Some(txn), Some(result)) = (txn, outcome) {
            self.trace.commit(txn, result.is_ok());
        }
    }

    fn rollback(&mut self) {
        self.cond.set_fn(Closure::new(|| {
            let kv = self.kv.take().unwrap();
            drop(kv);
        }));
        self.sync();
        if let Some(txn) = self.txn.take() {
            self.trace.drop_txn(txn);
        }
    }

    fn put<K: AsRef<[u8]>>(&mut self, k: K, v: K) {
        let kv = self.kv.as_ref().unwrap();
        let key = k.as_ref().to_vec();
        let val = v.as_ref().to_vec();
        let (pk, pv) = (SyncPtr::from(&key), SyncPtr::from(&val));
        self.cond.set_fn(Closure::new(|| {
            kv.put(pk.data(), pv.data()).expect("can't put");
        }));

        self.sync();
        if self.seed {
            self.trace.seed(&key, &val);
        } else if let Some(txn) = self.txn {
            self.trace.write(txn, &key, true, Some(&val));
        }
    }

    fn upsert<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, k: K, v: V) -> Result<(), OpCode> {
        let kv = self.kv.as_ref().unwrap();
        let key = k.as_ref().to_vec();
        let val = v.as_ref().to_vec();
        let (pk, pv) = (SyncPtr::from(&key), SyncPtr::from(&val));
        let mut outcome = None;
        self.cond.set_fn(Closure::new(|| {
            outcome = Some(kv.upsert(pk.data(), pv.data()));
        }));
        self.sync();
        let outcome = outcome.unwrap();
        if let Some(txn) = self.txn {
            let bytes = if outcome.is_ok() {
                Some(val.as_slice())
            } else {
                None
            };
            self.trace.write(txn, &key, outcome.is_ok(), bytes);
        }
        outcome
    }

    fn replace<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, k: K, v: V) -> Result<Vec<u8>, OpCode> {
        let kv = self.kv.as_ref().unwrap();
        let key = k.as_ref().to_vec();
        let val = v.as_ref().to_vec();
        let (pk, pv) = (SyncPtr::from(&key), SyncPtr::from(&val));
        let mut got = None;
        let mut wrote = None;
        let mut rv = None;
        self.cond.set_fn(Closure::new(|| {
            let g = kv.get(pk.data()).map(|x| x.slice().to_vec());
            let r = match &g {
                Ok(old) => {
                    let w = kv.update(pk.data(), pv.data());
                    wrote = Some(w);
                    w.map(|_| old.clone())
                }
                Err(e) => Err(*e),
            };
            got = Some(g);
            rv = Some(r);
        }));
        self.sync();
        if let Some(g) = got {
            let bytes = observed_bytes(&g);
            if let Some(txn) = self.txn {
                self.trace
                    .read(trace::Owner::Txn(txn), &key, bytes.as_deref());
            }
        }
        if let (Some(txn), Some(w)) = (self.txn, wrote) {
            let bytes = if w.is_ok() {
                Some(val.as_slice())
            } else {
                None
            };
            self.trace.write(txn, &key, w.is_ok(), bytes);
        }
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
        let key = k.as_ref().to_vec();
        let pk = SyncPtr::from(&key);
        let mut got = None;
        let mut v = None;

        self.cond.set_fn(Closure::new(|| {
            let g = kv.get(pk.data()).map(|x| x.slice().to_vec());
            let r = if is_del {
                match &g {
                    Ok(old) => kv.del(pk.data()).map(|_| old.clone()),
                    Err(e) => Err(*e),
                }
            } else {
                g.clone()
            };
            got = Some(g);
            v = Some(r);
        }));
        self.sync();
        if let Some(g) = got {
            let bytes = observed_bytes(&g);
            let owner = self
                .txn
                .map(trace::Owner::Txn)
                .unwrap_or(trace::Owner::View);
            self.trace.read(owner, &key, bytes.as_deref());
        }
        v.unwrap()
    }

    fn sync(&self) {
        let p = self.cond.producer();
        p.wake();
        p.wait();
    }
}

mod checker {
    use super::trace::{Attr, Owner, Recorder, Rule};

    fn write(rec: &Recorder, txn: usize, key: &str, bytes: &str) {
        rec.write(txn, key.as_bytes(), true, Some(bytes.as_bytes()))
            .unwrap();
    }

    #[test]
    fn attribution_has_five_outcomes() {
        let rec = Recorder::new();
        rec.seed(b"k", b"10");
        assert_eq!(rec.read(Owner::View, b"k", Some(b"10")), Attr::Seed);
        assert_eq!(rec.read(Owner::View, b"k", Some(b"99")), Attr::Unknown);
        let t = rec.alloc_txn();
        rec.begin(t);
        write(&rec, t, "k", "10");
        assert_eq!(rec.read(Owner::Txn(t), b"k", Some(b"10")), Attr::Ambiguous);
        assert_eq!(rec.read(Owner::View, b"missing", None), Attr::Absent);
    }

    #[test]
    fn read_before_own_write_is_not_a_self_read() {
        let rec = Recorder::new();
        rec.seed(b"k", b"10");
        let t = rec.alloc_txn();
        rec.begin(t);
        assert_eq!(rec.read(Owner::Txn(t), b"k", Some(b"10")), Attr::Seed);
        write(&rec, t, "k", "20");
        rec.commit(t, true);
        let v = rec.verdict();
        assert!(v.accepted(), "{}", v.describe());
        assert_eq!(v.inconclusive, 0);
    }

    #[test]
    fn dirty_read_is_reported() {
        let rec = Recorder::new();
        let t = rec.alloc_txn();
        rec.begin(t);
        write(&rec, t, "k", "v1");
        let r = rec.alloc_txn();
        rec.begin(r);
        rec.read(Owner::Txn(r), b"k", Some(b"v1"));
        rec.drop_txn(t);
        rec.commit(r, true);
        let v = rec.verdict();
        assert!(
            v.violations.iter().any(|x| x.rule == Rule::DirtyRead),
            "{}",
            v.describe()
        );
    }

    #[test]
    fn stale_read_is_reported() {
        let rec = Recorder::new();
        rec.seed(b"k", b"10");
        let w = rec.alloc_txn();
        rec.begin(w);
        write(&rec, w, "k", "20");
        rec.commit(w, true);
        let r = rec.alloc_txn();
        rec.begin(r);
        rec.read(Owner::Txn(r), b"k", Some(b"10"));
        rec.commit(r, true);
        let v = rec.verdict();
        assert!(
            v.violations.iter().any(|x| x.rule == Rule::StaleRead),
            "{}",
            v.describe()
        );
    }

    #[test]
    fn future_read_is_reported() {
        let rec = Recorder::new();
        rec.seed(b"k", b"10");
        let r = rec.alloc_txn();
        rec.begin(r);
        let w = rec.alloc_txn();
        rec.begin(w);
        write(&rec, w, "k", "20");
        rec.commit(w, true);
        rec.read(Owner::Txn(r), b"k", Some(b"20"));
        rec.commit(r, true);
        let v = rec.verdict();
        assert!(
            v.violations.iter().any(|x| x.rule == Rule::FutureRead),
            "{}",
            v.describe()
        );
    }

    #[test]
    fn self_read_is_enforced() {
        let rec = Recorder::new();
        rec.seed(b"k", b"10");
        let t = rec.alloc_txn();
        rec.begin(t);
        write(&rec, t, "k", "20");
        rec.read(Owner::Txn(t), b"k", Some(b"10"));
        rec.commit(t, true);
        let v = rec.verdict();
        assert!(
            v.violations.iter().any(|x| x.rule == Rule::SelfRead),
            "{}",
            v.describe()
        );
    }

    #[test]
    fn write_skew_is_accepted() {
        let rec = Recorder::new();
        rec.seed(b"x", b"10");
        rec.seed(b"y", b"20");
        let a = rec.alloc_txn();
        let b = rec.alloc_txn();
        rec.begin(a);
        rec.begin(b);
        rec.read(Owner::Txn(a), b"x", Some(b"10"));
        rec.read(Owner::Txn(a), b"y", Some(b"20"));
        rec.read(Owner::Txn(b), b"x", Some(b"10"));
        rec.read(Owner::Txn(b), b"y", Some(b"20"));
        write(&rec, a, "x", "20");
        write(&rec, b, "y", "10");
        rec.commit(a, true);
        rec.commit(b, true);
        let v = rec.verdict();
        assert!(v.accepted(), "{}", v.describe());
        assert!(v.coverage.cycles >= 1, "{}", v.describe());
    }

    #[test]
    fn lost_update_is_reported() {
        let rec = Recorder::new();
        rec.seed(b"k", b"10");
        let a = rec.alloc_txn();
        let b = rec.alloc_txn();
        rec.begin(a);
        rec.begin(b);
        rec.read(Owner::Txn(a), b"k", Some(b"10"));
        rec.read(Owner::Txn(b), b"k", Some(b"10"));
        write(&rec, a, "k", "11");
        write(&rec, b, "k", "12");
        rec.commit(a, true);
        rec.commit(b, true);
        let v = rec.verdict();
        assert!(
            v.violations.iter().any(|x| x.rule == Rule::LostUpdate),
            "{}",
            v.describe()
        );
    }
    #[test]
    fn write_cycle_is_reported() {
        let rec = Recorder::new();
        rec.seed(b"k2", b"10");
        let one = rec.alloc_txn();
        let two = rec.alloc_txn();
        rec.begin(one);
        write(&rec, one, "k1", "a");
        rec.begin(two);
        rec.read(Owner::Txn(two), b"k2", Some(b"10"));
        write(&rec, two, "k1", "b");
        write(&rec, one, "k2", "c");
        rec.commit(one, true);
        rec.commit(two, true);
        let v = rec.verdict();
        assert!(
            v.violations.iter().any(|x| x.rule == Rule::WriteCycle),
            "{}",
            v.describe()
        );
    }

    #[test]
    fn fabricated_value_is_reported() {
        let rec = Recorder::new();
        rec.seed(b"k", b"10");
        let t = rec.alloc_txn();
        rec.begin(t);
        rec.read(Owner::Txn(t), b"k", Some(b"nobody wrote this"));
        rec.commit(t, true);
        let v = rec.verdict();
        assert!(
            v.violations.iter().any(|x| x.rule == Rule::FabricatedValue),
            "{}",
            v.describe()
        );
    }

    #[test]
    fn unstable_read_is_reported() {
        let rec = Recorder::new();
        rec.seed(b"k", b"10");
        let w = rec.alloc_txn();
        rec.begin(w);
        write(&rec, w, "k", "20");
        rec.commit(w, true);
        let r = rec.alloc_txn();
        rec.begin(r);
        rec.read(Owner::Txn(r), b"k", Some(b"20"));
        rec.read(Owner::Txn(r), b"k", Some(b"10"));
        rec.commit(r, true);
        let v = rec.verdict();
        assert!(
            v.violations.iter().any(|x| x.rule == Rule::UnstableRead),
            "{}",
            v.describe()
        );
    }

    #[test]
    fn blind_writes_are_accepted() {
        let rec = Recorder::new();
        let a = rec.alloc_txn();
        let b = rec.alloc_txn();
        rec.begin(a);
        rec.begin(b);
        write(&rec, a, "k", "a");
        write(&rec, b, "k", "b");
        rec.commit(a, true);
        rec.commit(b, true);
        let v = rec.verdict();
        assert!(v.accepted(), "{}", v.describe());
        assert_eq!(v.coverage.baselines, 0);
    }

    #[test]
    fn self_read_then_rollback_is_accepted() {
        let rec = Recorder::new();
        rec.seed(b"k", b"10");
        let t = rec.alloc_txn();
        rec.begin(t);
        write(&rec, t, "k", "20");
        rec.read(Owner::Txn(t), b"k", Some(b"20"));
        rec.drop_txn(t);
        let v = rec.verdict();
        assert!(v.accepted(), "{}", v.describe());
        assert_eq!(v.inconclusive, 0);
    }

    #[test]
    fn ambiguous_baselines_are_inconclusive() {
        let rec = Recorder::new();
        rec.seed(b"k", b"x");
        let t0 = rec.alloc_txn();
        rec.begin(t0);
        write(&rec, t0, "k", "x");
        rec.commit(t0, true);
        let a = rec.alloc_txn();
        let b = rec.alloc_txn();
        rec.begin(a);
        rec.read(Owner::Txn(a), b"k", Some(b"x"));
        rec.begin(b);
        rec.read(Owner::Txn(b), b"k", Some(b"x"));
        write(&rec, a, "k", "y");
        write(&rec, b, "k", "z");
        rec.commit(a, true);
        rec.commit(b, true);
        let v = rec.verdict();
        assert!(v.accepted(), "{}", v.describe());
        assert_eq!(v.inconclusive, 2, "{}", v.describe());
    }

    #[test]
    fn absent_read_against_seed_is_stale() {
        let rec = Recorder::new();
        rec.seed(b"k", b"10");
        let t = rec.alloc_txn();
        rec.begin(t);
        rec.read(Owner::Txn(t), b"k", None);
        rec.commit(t, true);
        let v = rec.verdict();
        assert!(
            v.violations.iter().any(|x| x.rule == Rule::StaleRead),
            "{}",
            v.describe()
        );
    }

    #[test]
    fn absent_read_builds_an_rw_edge() {
        let rec = Recorder::new();
        let one = rec.alloc_txn();
        let two = rec.alloc_txn();
        let three = rec.alloc_txn();
        rec.begin(one);
        rec.read(Owner::Txn(one), b"k", None);
        write(&rec, one, "n", "one");
        rec.begin(two);
        write(&rec, two, "k", "two");
        rec.commit(two, true);
        rec.begin(three);
        rec.read(Owner::Txn(three), b"n", None);
        write(&rec, three, "k", "three");
        rec.commit(three, true);
        rec.commit(one, true);
        let v = rec.verdict();
        assert!(
            v.violations.iter().any(|x| x.rule == Rule::WriteCycle),
            "{}",
            v.describe()
        );
    }
}

mod random_schedules {
    use super::trace::{Coverage, Verdict};

    const KEYS: [&[u8]; 6] = [b"1", b"2", b"k0", b"k1", b"k2", b"k3"];

    struct Rng(u64);

    impl Rng {
        fn new(seed: u64) -> Self {
            Self(seed ^ 0x9E37_79B9_7F4A_7C15)
        }

        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }

        fn below(&mut self, bound: usize) -> usize {
            (self.next() % bound as u64) as usize
        }
    }

    #[derive(Clone, Copy)]
    enum Op {
        Get(usize),
        Upsert(usize),
    }

    struct Script {
        ops: Vec<Op>,
        cursor: usize,
        begun: bool,
        done: bool,
    }

    fn build_script(rng: &mut Rng) -> Script {
        let len = 1 + rng.below(4);
        let mut ops = Vec::with_capacity(len);
        for _ in 0..len {
            let key = rng.below(KEYS.len());
            ops.push(if rng.next().is_multiple_of(2) {
                Op::Get(key)
            } else {
                Op::Upsert(key)
            });
        }
        Script {
            ops,
            cursor: 0,
            begun: false,
            done: false,
        }
    }

    fn run_schedule(seed: u64, schedule: usize, workers: usize) {
        let cores: Vec<usize> = (1..=workers).collect();
        let e = super::Executor::launch(&cores);
        let sessions: Vec<_> = cores.iter().map(|c| e.session(*c)).collect();

        let mut rng = Rng::new(seed.wrapping_mul(1_000_003).wrapping_add(schedule as u64));
        let mut scripts: Vec<Script> = (0..workers).map(|_| build_script(&mut rng)).collect();
        let mut sessions: Vec<_> = sessions.into_iter().map(Some).collect();
        let mut sequence = 0u64;

        loop {
            let live: Vec<usize> = (0..workers).filter(|i| !scripts[*i].done).collect();
            if live.is_empty() {
                break;
            }
            let i = live[rng.below(live.len())];
            let step = {
                let script = &mut scripts[i];
                if !script.begun {
                    script.begun = true;
                    None
                } else if script.cursor < script.ops.len() {
                    let op = script.ops[script.cursor];
                    script.cursor += 1;
                    Some(op)
                } else {
                    script.done = true;
                    Some(Op::Get(usize::MAX))
                }
            };
            let session = sessions[i].as_mut().expect("session");
            match step {
                None => session.begin(),
                Some(Op::Get(usize::MAX)) => {
                    if rng.next().is_multiple_of(2) {
                        session.commit();
                    } else {
                        session.rollback();
                    }
                }
                Some(Op::Get(key)) => {
                    let _ = session.get(KEYS[key]);
                }
                Some(Op::Upsert(key)) => {
                    sequence += 1;
                    let value = format!("w{i}-{sequence}");
                    let _ = session.upsert(KEYS[key], value.as_bytes());
                }
            }
        }

        // a conflicted writer: both read the same baseline, exactly one write may land
        let mut a = sessions[0].take().expect("a");
        let mut b = sessions[1].take().expect("b");
        a.begin();
        b.begin();
        let _ = a.get(b"1");
        let _ = a.get(b"1");
        let _ = b.get(b"1");
        sequence += 1;
        let va = format!("a-{sequence}");
        sequence += 1;
        let vb = format!("b-{sequence}");
        let first = a.upsert(b"1", va.as_bytes());
        let second = b.upsert(b"1", vb.as_bytes());
        assert!(
            first.is_ok() || second.is_ok(),
            "at least one concurrent writer must land"
        );
        a.commit();
        b.commit();

        // write skew: two rw edges and no ww edge, which snapshot isolation allows
        let mut c = sessions[2].take().expect("c");
        let mut d = sessions[3].take().expect("d");
        c.begin();
        d.begin();
        let _ = c.get(b"1");
        let _ = d.get(b"2");
        sequence += 1;
        let vc = format!("c-{sequence}");
        sequence += 1;
        let vd = format!("d-{sequence}");
        let _ = c.upsert(b"2", vc.as_bytes());
        let _ = d.upsert(b"1", vd.as_bytes());
        c.commit();
        d.commit();

        let random_steps: usize = scripts.iter().map(|script| script.ops.len() + 2).sum();
        let injected_steps = 17; // two begins, three reads, two writes, two commits; then 2+2+2+2
        assert_eq!(
            e.trace_len(),
            2 + random_steps + injected_steps,
            "seed {seed} schedule {schedule} lost or duplicated events"
        );

        let verdict: Verdict = e.trace.verdict();
        assert!(
            verdict.accepted(),
            "seed {seed} schedule {schedule} rejected:\n{}",
            verdict.describe()
        );
        let Coverage {
            boundary,
            stability,
            cycles,
            baselines,
            ..
        } = verdict.coverage;
        assert!(
            boundary >= 1 && stability >= 1 && cycles >= 1 && baselines >= 1,
            "seed {seed} schedule {schedule} did not exercise the checker: {}",
            verdict.describe()
        );
        assert_eq!(
            verdict.inconclusive,
            0,
            "unexpected ambiguity:\n{}",
            verdict.describe()
        );
    }

    #[test]
    fn random_schedules_are_anomaly_free() {
        let seed: u64 = std::env::var("MACE_MODEL_SEED")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1);
        let schedules: usize = std::env::var("MACE_MODEL_SCHEDULES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(128);
        let workers: usize = std::env::var("MACE_MODEL_WORKERS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(4);
        assert!(workers >= 4, "the injection blocks need four workers");
        for schedule in 0..schedules {
            run_schedule(seed, schedule, workers);
        }
    }
}
