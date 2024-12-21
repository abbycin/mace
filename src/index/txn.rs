use crate::{
    cc::{
        context::Context,
        data::Record,
        wal::{PayloadType, WalDel, WalFileReader, WalPut, WalReplace, WalUpdate},
        worker::SyncWorker,
    },
    index::{data::Value, tree::Tree, Key},
    utils::{
        traits::{IKey, IVal},
        IsolationLevel, INIT_CMD,
    },
    OpCode,
};
use std::{
    cell::RefCell,
    sync::{
        atomic::Ordering::{Relaxed, Release},
        Arc,
    },
};

use super::Val;

fn get_impl<'a, K: AsRef<[u8]>>(
    ctx: &Arc<Context>,
    tree: &Tree,
    w: SyncWorker,
    k: K,
    cmd: u32,
) -> Result<(u64, Val<Record<'a>>), OpCode> {
    let start_ts = w.txn.start_ts;
    let key = Key::new(k.as_ref(), start_ts, cmd);
    let mut mw = w;
    let r = tree.traverse::<Record, _>(key, |txid, t| {
        debug_assert!(t.worker_id() < coreid::cores_online() as u16);
        mw.cc.is_visible_to(ctx, w, t.worker_id(), start_ts, txid)
    })?;

    debug_assert!(!r.1.unwrap().is_tombstone());
    Ok(r)
}

pub struct TxnKV {
    ctx: Arc<Context>,
    tree: Tree,
    w: SyncWorker,
    seq: RefCell<u32>,
}

impl TxnKV {
    pub(crate) fn new(ctx: Arc<Context>, tree: Tree, w: SyncWorker) -> Self {
        let this = Self {
            ctx,
            tree,
            w,
            seq: RefCell::new(INIT_CMD),
        };
        this.bind_core();
        this
    }

    fn bind_core(&self) {
        if self.ctx.opt.bind_core {
            coreid::bind_core(self.w.id as usize);
        }
    }

    fn unbind_core(&self) {
        if self.ctx.opt.bind_core {
            coreid::unbind_core();
        }
    }

    fn cmd_id(&self) -> u32 {
        let mut o = self.seq.borrow_mut();
        let r = *o;
        *o += 1;
        r
    }

    fn modify<F>(&self, k: &[u8], v: &[u8], mut f: F) -> Result<Option<Val<Record>>, OpCode>
    where
        F: FnMut(&Option<(Key, Val<Record>)>, u64, u32, SyncWorker) -> Result<(), OpCode>,
    {
        let start_ts = self.w.txn.start_ts;
        let (wid, cmd_id) = (self.w.id, self.cmd_id());
        let key = Key::new(k, start_ts, cmd_id);
        let val = Value::Put(Record::normal(wid, unsafe {
            // shutup compiler
            std::slice::from_raw_parts(v.as_ptr(), v.len())
        }));

        self.tree
            .update(key, val, |opt| f(opt, start_ts, cmd_id, self.w))
    }

    fn put_impl(&self, k: &[u8], v: &[u8], logged: &mut bool) -> Result<(), OpCode> {
        self.modify(k, v, |opt, start_ts, cmd_id, mut w| {
            let r = match opt {
                None => Ok(()),
                Some((rk, rv)) => {
                    let t = rv.unwrap();
                    if rv.is_put()
                        || !w
                            .cc
                            .is_visible_to(&self.ctx, self.w, t.worker_id(), start_ts, rk.txid)
                    {
                        Err(OpCode::AbortTx)
                    } else {
                        Ok(())
                    }
                }
            };
            if r.is_ok() && !*logged {
                *logged = true;
                w.txn.modified = true;
                w.logging
                    .record_update(cmd_id, WalPut::new(k.len(), v.len()), k, [].as_slice(), v);
            }
            r
        })
        .map(|_| ())
    }

    fn update_impl(&self, k: &[u8], v: &[u8], logged: &mut bool) -> Result<Val<Record>, OpCode> {
        self.modify(k, v, |opt, start_ts, cmd_id, mut w| match opt {
            None => Err(OpCode::NotFound),
            Some((rk, rv)) => {
                if rv.is_del() {
                    return Err(OpCode::NotFound);
                }
                let t = rv.unwrap();
                if !w
                    .cc
                    .is_visible_to(&self.ctx, self.w, t.worker_id(), start_ts, rk.txid)
                {
                    return Err(OpCode::AbortTx);
                }

                if !*logged {
                    w.txn.modified = true;
                    *logged = true;
                    w.logging.record_update(
                        cmd_id,
                        WalReplace::new(
                            t.worker_id(),
                            rk.cmd,
                            rk.txid,
                            rk.raw.len(),
                            t.data().len(),
                            v.len(),
                        ),
                        rk.raw,
                        t.data(),
                        v,
                    );
                }
                Ok(())
            }
        })
        .map(|x| x.unwrap())
    }

    pub fn put<T>(&self, k: T, v: T) -> Result<(), OpCode>
    where
        T: AsRef<[u8]>,
    {
        let mut logged = false;
        self.put_impl(k.as_ref(), v.as_ref(), &mut logged)
    }

    pub fn update<T>(&self, k: T, v: T) -> Result<Val<Record>, OpCode>
    where
        T: AsRef<[u8]>,
    {
        let mut logged = false;
        self.update_impl(k.as_ref(), v.as_ref(), &mut logged)
    }

    pub fn upsert<T>(&self, k: T, v: T) -> Result<Option<Val<Record>>, OpCode>
    where
        T: AsRef<[u8]>,
    {
        let mut logged = false;
        let (k, v) = (k.as_ref(), v.as_ref());

        if self.put_impl(k, v, &mut logged).is_ok() {
            return Ok(None);
        }

        assert!(!logged);

        self.update_impl(k, v, &mut logged).map(Some)
    }

    pub fn del<T>(&self, k: T) -> Result<Val<Record>, OpCode>
    where
        T: AsRef<[u8]>,
    {
        let mut w = self.w;
        let (wid, start_ts) = (w.id, w.txn.start_ts);
        let key = Key::new(k.as_ref(), start_ts, self.cmd_id());
        let val = Value::Del(Record::remove(wid));
        let mut logged = false;

        self.tree
            .update(key, val, |opt| match opt {
                None => Err(OpCode::NotFound),
                Some((rk, rv)) => {
                    if rv.is_del() {
                        return Err(OpCode::NotFound);
                    }
                    let t = rv.unwrap();
                    if !w
                        .cc
                        .is_visible_to(&self.ctx, self.w, t.worker_id(), start_ts, rk.txid)
                    {
                        return Err(OpCode::AbortTx);
                    }

                    if !logged {
                        logged = true;
                        w.txn.modified = true;
                        w.logging.record_update(
                            key.cmd,
                            WalDel::new(
                                t.worker_id(),
                                rk.cmd,
                                rk.txid,
                                rk.raw.len(),
                                t.data().len(),
                            ),
                            rk.raw,
                            t.data(),
                            [].as_slice(),
                        );
                    }
                    Ok(())
                }
            })
            .map(|x| x.unwrap())
    }

    #[inline]
    pub fn get<K>(&self, k: K) -> Result<Val<Record>, OpCode>
    where
        K: AsRef<[u8]>,
    {
        get_impl(&self.ctx, &self.tree, self.w, k, self.cmd_id()).map(|x| x.1)
    }
}

pub struct TxnView {
    ctx: Arc<Context>,
    tree: Tree,
    w: SyncWorker,
}

impl TxnView {
    fn new(ctx: Arc<Context>, tree: Tree, w: SyncWorker) -> Self {
        Self { ctx, tree, w }
    }

    #[inline]
    pub fn get<K: AsRef<[u8]>>(&self, k: K) -> Result<Val<Record>, OpCode> {
        get_impl(&self.ctx, &self.tree, self.w, k, INIT_CMD).map(|x| x.1)
    }
}

pub struct Tx {
    pub(crate) ctx: Arc<Context>,
    pub(crate) tree: Tree,
}

impl Tx {
    fn init(&self, level: IsolationLevel, start_ts: u64) -> SyncWorker {
        assert!(
            level == IsolationLevel::SI,
            "only SI is supported at present"
        );

        let mut w = self.ctx.worker(coreid::current_core());
        let id = w.id;
        w.txn.reset(start_ts, level);
        w.tx_id.store(start_ts, Relaxed);
        w.cc.global_wmk_tx = self.ctx.wmk_oldest.load(Relaxed);
        w.cc.commit_tree.compact(&self.ctx, id);
        w
    }

    pub fn view(&self, level: IsolationLevel) -> TxnView {
        TxnView::new(
            self.ctx.clone(),
            self.tree.clone(),
            self.init(level, self.ctx.load_oracle()),
        )
    }

    pub fn begin(&self, level: IsolationLevel) -> TxnKV {
        let start_ts = self.ctx.alloc_oracle();
        let mut w = self.init(level, start_ts);
        w.logging.reset(self.tree.id() as u16, start_ts);
        w.logging.record_begin();
        TxnKV::new(self.ctx.clone(), self.tree.clone(), w)
    }

    pub fn commit(&self, kv: TxnKV) {
        debug_assert_eq!(kv.tree.id(), self.tree.id());
        let mut w = kv.w;
        let mut txn = w.txn;
        let txid = txn.start_ts;

        if !w.txn.modified {
            w.logging.record_commit();
            w.logging.record_end(txid);
            kv.unbind_core();
            return;
        }

        txn.commit_ts = self.ctx.alloc_oracle();
        w.cc.commit_tree.append(txid, txn.commit_ts);
        w.cc.latest_cts.store(txn.commit_ts, Relaxed);

        w.tx_id.store(0, Release); // sync with cc
        txn.max_gsn = w.logging.gsn();

        // we have no remote dependency, since we are append-only
        w.logging.append_txn(txn);

        w.logging.record_commit();
        w.cc.collect_wmk(&self.ctx);
        w.logging.wait_commit(txn.commit_ts);
        w.logging.record_end(txid);
        kv.unbind_core();
    }

    fn rollback_impl(&self, update: &WalUpdate, kv: &TxnKV, mut w: SyncWorker, txid: u64) {
        let payload_type = update.payload_type();
        match payload_type {
            PayloadType::Insert => {
                let ins = update.put();
                let k = Key::new(ins.key(), txid, kv.cmd_id());
                let v = Value::Del(Record::remove(w.id));
                log::debug!("rollback insert {} {}", k.to_string(), v.to_string());
                self.tree.put(k, v).expect("can't be wrong");
            }
            PayloadType::Update => {
                let upd = update.update();
                let k = Key::new(upd.key(), txid, kv.cmd_id());
                let v = Value::Put(Record::normal(upd.wid(), upd.old_val()));
                log::debug!("rollback update {} {}", k.to_string(), v.to_string());
                self.tree.put(k, v).expect("can't be wrong");
            }
            PayloadType::Delete => {
                let del = update.del();
                let k = Key::new(del.key(), txid, kv.cmd_id());
                let v = Value::Put(Record::normal(w.id, del.val()));
                log::debug!("rollback delete {} {}", k.to_string(), v.to_string());
                self.tree.put(k, v).expect("can't be wrong");
            }
        }
        // NOTE: changes are not required to be flushed, when crash occurred, crash recovery
        // will bring it to consistent
        w.logging.record_clr(txid, update.prev_gsn);
    }

    fn rollback_from_file(&self, kv: &TxnKV, w: SyncWorker, txid: u64) {
        let len = self.ctx.commiter.wal_len() as u64;
        let mut reader = WalFileReader::new(self.ctx.opt.wal_file(), len);

        reader.collect(txid);
        reader.apply(|update| {
            self.rollback_impl(update, kv, w, txid);
        });
    }

    pub fn rollback(&self, kv: TxnKV) {
        debug_assert_eq!(kv.tree.id(), self.tree.id());
        let mut w = kv.w;
        let txid = w.txn.start_ts;

        w.logging.record_abort();

        if w.txn.modified {
            w.logging.wait_flush();
            self.rollback_from_file(&kv, w, txid);
        }

        w.logging.record_end(txid);
        kv.unbind_core();
    }
}

#[cfg(test)]
mod test {
    use crate::{utils::IsolationLevel, Mace, OpCode, Options, RandomPath};

    #[test]
    fn txnkv() -> Result<(), OpCode> {
        let path = RandomPath::tmp();
        let _ = std::fs::remove_dir_all(&*path);
        let mut opt = Options::new(&*path);
        opt.bind_core = true;
        let db = Mace::open(opt)?;
        let (k1, k2) = ("beast".as_bytes(), "senpai".as_bytes());
        let (v1, v2) = ("114514".as_bytes(), "1919810".as_bytes());
        let tx = db.default();

        {
            let kv = tx.begin(IsolationLevel::SI);

            kv.put(k1, v1).expect("can't put");
            kv.put(k2, v2).expect("can't put");

            let r = kv.get(k1).expect("can't get");
            assert_eq!(r.data(), v1);
            let r = kv.get(k2).expect("can't get");
            assert_eq!(r.data(), v2);

            let r = kv.del(k1).expect("can't del");
            assert_eq!(r.data(), v1);
            tx.commit(kv);
        }

        {
            let kv = tx.begin(IsolationLevel::SI);

            let r = kv.get(k1);
            assert!(r.is_err());

            let r = kv.get(k2).expect("can't get");
            assert_eq!(r.data(), v2);

            let r = kv.del(k2).expect("can't del");
            assert_eq!(r.data(), v2);
            tx.rollback(kv);
        }

        {
            let kv = tx.begin(IsolationLevel::SI);

            let r = kv.get(k1);
            assert!(r.is_err());
            let r = kv.del(k2).expect("can't del");
            assert_eq!(r.data(), v2);
            let r = kv.del(k2);
            assert!(r.is_err());

            tx.commit(kv);
        }

        {
            let kv = tx.begin(IsolationLevel::SI);

            let r = kv.get(k1);
            assert!(r.is_err());
            let r = kv.get(k2);
            assert!(r.is_err());

            tx.commit(kv);
        }

        {
            let kv = tx.begin(IsolationLevel::SI);
            kv.put("1", "10")?;
            tx.commit(kv);

            let kv = tx.begin(IsolationLevel::SI);
            kv.update("1", "11").expect("can't replace");
            tx.rollback(kv);

            let kv = tx.view(IsolationLevel::SI);
            let x = kv.get("1").expect("can't get");
            assert_eq!(x.data(), "10".as_bytes());
        }

        {
            let kv = tx.begin(IsolationLevel::SI);
            kv.put("2", "20")?;
            kv.update("2", "21")?;
            let r = kv.get("2").unwrap();
            assert_eq!(r.data(), "21".as_bytes());
            kv.del("2")?;
            tx.rollback(kv);

            let kv = tx.view(IsolationLevel::SI);
            let x = kv.get("2");
            assert!(x.is_err());
        }

        let tx = db.get("upsert").unwrap();

        {
            let kv = tx.begin(IsolationLevel::SI);
            kv.put("1", "10")?;
            tx.commit(kv);

            let kv = tx.begin(IsolationLevel::SI);
            kv.upsert("1", "11").expect("can't replace");
            tx.rollback(kv);

            let kv = tx.view(IsolationLevel::SI);
            let x = kv.get("1").expect("can't get");
            assert_eq!(x.data(), "10".as_bytes());
        }

        {
            let kv = tx.begin(IsolationLevel::SI);
            kv.put("2", "20")?;
            kv.upsert("2", "21")?;
            let r = kv.get("2").unwrap();
            assert_eq!(r.data(), "21".as_bytes());
            kv.del("2")?;
            tx.rollback(kv);

            let kv = tx.view(IsolationLevel::SI);
            let x = kv.get("2");
            assert!(x.is_err());
        }
        Ok(())
    }
}
