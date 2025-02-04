use crate::{
    cc::{
        context::Context,
        data::{Record, Ver},
        wal::{WalDel, WalPut, WalReplace},
        worker::SyncWorker,
    },
    index::{data::Value, tree::Tree, Key},
    utils::{IsolationLevel, INIT_CMD, NULL_CMD},
    OpCode,
};
use std::sync::atomic::Ordering::Relaxed;
use std::{cell::Cell, sync::Arc};

use super::{registry::Registry, Val};

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
        mw.cc.is_visible_to(ctx, w, t.worker_id(), start_ts, txid)
    })?;

    debug_assert!(!r.1.unwrap().is_tombstone());
    Ok(r)
}

pub struct TxnKV {
    ctx: Arc<Context>,
    tree: Tree,
    w: SyncWorker,
    seq: Cell<u32>,
    is_end: Cell<bool>,
    limit: usize,
}

impl TxnKV {
    fn new(mgr: Registry, tree: Tree, w: SyncWorker) -> Self {
        let limit = mgr.store.opt.max_ckpt_per_txn;
        Self {
            ctx: mgr.store.context.clone(),
            tree,
            w,
            seq: Cell::new(INIT_CMD),
            is_end: Cell::new(false),
            limit,
        }
    }

    fn cmd_id(&self) -> u32 {
        let r = self.seq.get();
        self.seq.set(r + 1);
        r
    }

    fn should_abort(&self) -> Result<(), OpCode> {
        if self.is_end.get() {
            return Err(OpCode::Invalid);
        }
        if self.w.ckpt_cnt.load(Relaxed) >= self.limit {
            return Err(OpCode::AbortTx);
        }
        Ok(())
    }

    fn modify<F>(&self, k: &[u8], v: &[u8], mut f: F) -> Result<Option<Val<Record>>, OpCode>
    where
        F: FnMut(&Option<(Key, Val<Record>)>, Ver, SyncWorker) -> Result<(), OpCode>,
    {
        self.should_abort()?;
        let start_ts = self.w.txn.start_ts;
        let (wid, cmd_id) = (self.w.id, self.cmd_id());
        let key = Key::new(k, start_ts, cmd_id);
        let val = Value::Put(Record::normal(wid, v));

        self.tree.update(key, val, |opt| f(opt, *key.ver(), self.w))
    }

    fn put_impl(&self, k: &[u8], v: &[u8], logged: &mut bool) -> Result<(), OpCode> {
        self.modify(k, v, |opt, ver, mut w| {
            let r = match opt {
                None => Ok(()),
                Some((rk, rv)) => {
                    let t = rv.unwrap();
                    if rv.is_put()
                        || !w
                            .cc
                            .is_visible_to(&self.ctx, self.w, t.worker_id(), ver.txid, rk.txid)
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
                w.logging.record_update(
                    ver,
                    self.tree.id(),
                    WalPut::new(v.len()),
                    k,
                    [].as_slice(),
                    v,
                );
            }
            r
        })
        .map(|_| ())
    }

    fn update_impl(&self, k: &[u8], v: &[u8], logged: &mut bool) -> Result<Val<Record>, OpCode> {
        self.modify(k, v, |opt, ver, mut w| match opt {
            None => Err(OpCode::NotFound),
            Some((rk, rv)) => {
                if rv.is_del() {
                    return Err(OpCode::NotFound);
                }
                let t = rv.unwrap();
                if !w
                    .cc
                    .is_visible_to(&self.ctx, self.w, t.worker_id(), ver.txid, rk.txid)
                {
                    return Err(OpCode::AbortTx);
                }

                if !*logged {
                    w.txn.modified = true;
                    *logged = true;
                    w.logging.record_update(
                        ver,
                        self.tree.id(),
                        WalReplace::new(t.data().len(), v.len()),
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
        self.should_abort()?;
        let mut w = self.w;
        let (wid, start_ts) = (w.id, w.txn.start_ts);
        let key = Key::new(k.as_ref(), start_ts, self.cmd_id());
        let val = Value::Del(Record::remove(wid));
        let mut logged = false;

        self.tree
            .update::<Record, Record, _>(key, val, |opt| match opt {
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
                            *key.ver(),
                            self.tree.id(),
                            WalDel::new(t.data().len()),
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

    pub fn commit(&self) -> Result<(), OpCode> {
        self.should_abort()?;
        if !self.is_end.get() {
            self.w.commit(&self.ctx);
            self.is_end.set(true);
        }
        Ok(())
    }

    pub fn rollback(&self) -> Result<(), OpCode> {
        if !self.is_end.get() {
            self.w.rollback(&self.ctx, &self.tree);
            self.is_end.set(true);
        }
        Ok(())
    }

    #[inline]
    pub fn get<K>(&self, k: K) -> Result<Val<Record>, OpCode>
    where
        K: AsRef<[u8]>,
    {
        get_impl(&self.ctx, &self.tree, self.w, k, self.cmd_id()).map(|x| x.1)
    }
}

impl Drop for TxnKV {
    fn drop(&mut self) {
        let _ = self.rollback();
        self.ctx.free(self.w);
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
        get_impl(&self.ctx, &self.tree, self.w, k, NULL_CMD).map(|x| x.1)
    }
}

impl Drop for TxnView {
    fn drop(&mut self) {
        self.ctx.free(self.w);
    }
}

pub struct Tx {
    pub(crate) ctx: Arc<Context>,
    pub(crate) tree: Tree,
    pub(crate) mgr: Registry,
}

impl Tx {
    pub fn id(&self) -> u64 {
        self.tree.id()
    }
    /// read-only txn
    pub fn view<F>(&self, level: IsolationLevel, f: F) -> Result<(), OpCode>
    where
        F: Fn(TxnView) -> Result<(), OpCode>,
    {
        f(self._read(level))
    }

    /// the user must commit, otherwise, when leaving clouser, the transaction will be rolled back
    pub fn begin<F>(&self, level: IsolationLevel, mut f: F) -> Result<(), OpCode>
    where
        F: FnMut(TxnKV) -> Result<(), OpCode>,
    {
        f(self._write(level))
    }

    #[doc(hidden)]
    pub fn _write(&self, level: IsolationLevel) -> TxnKV {
        let mut w = self.ctx.alloc();
        w.begin(&self.ctx, level);
        TxnKV::new(self.mgr.clone(), self.tree.clone(), w)
    }

    #[doc(hidden)]
    pub fn _read(&self, level: IsolationLevel) -> TxnView {
        let mut w = self.ctx.alloc();
        w.view(&self.ctx, level);
        TxnView::new(self.ctx.clone(), self.tree.clone(), w)
    }
}

impl Clone for Tx {
    fn clone(&self) -> Self {
        self.mgr.inc_ref(self.tree.id());
        Self {
            ctx: self.ctx.clone(),
            tree: self.tree.clone(),
            mgr: self.mgr.clone(),
        }
    }
}

impl Drop for Tx {
    fn drop(&mut self) {
        self.mgr.dec_ref(self.tree.id());
    }
}

#[cfg(test)]
mod test {
    use crate::{utils::IsolationLevel, Mace, OpCode, Options, RandomPath};

    #[test]
    fn txnkv() -> Result<(), OpCode> {
        let path = RandomPath::tmp();
        let _ = std::fs::remove_dir_all(&*path);
        let opt = Options::new(&*path);
        let db = Mace::new(opt)?;
        let (k1, k2) = ("beast".as_bytes(), "senpai".as_bytes());
        let (v1, v2) = ("114514".as_bytes(), "1919810".as_bytes());
        let tx = db.default();

        tx.begin(IsolationLevel::SI, |kv| {
            kv.put(k1, v1).expect("can't put");
            kv.put(k2, v2).expect("can't put");

            let r = kv.get(k1).expect("can't get");
            assert_eq!(r.data(), v1);
            let r = kv.get(k2).expect("can't get");
            assert_eq!(r.data(), v2);

            let r = kv.del(k1).expect("can't del");
            assert_eq!(r.data(), v1);
            kv.commit()
        })?;

        tx.begin(IsolationLevel::SI, |kv| {
            let r = kv.get(k1);
            assert!(r.is_err());

            let r = kv.get(k2).expect("can't get");
            assert_eq!(r.data(), v2);

            let r = kv.del(k2).expect("can't del");
            assert_eq!(r.data(), v2);
            kv.rollback()
        })?;

        tx.begin(IsolationLevel::SI, |kv| {
            let r = kv.get(k1);
            assert!(r.is_err());
            let r = kv.del(k2).expect("can't del");
            assert_eq!(r.data(), v2);
            let r = kv.del(k2);
            assert!(r.is_err());

            kv.commit()
        })?;

        tx.begin(IsolationLevel::SI, |kv| {
            let r = kv.get(k1);
            assert!(r.is_err());
            let r = kv.get(k2);
            assert!(r.is_err());

            kv.commit()
        })?;

        {
            tx.begin(IsolationLevel::SI, |kv| {
                kv.put("1", "10")?;
                kv.commit()
            })?;

            tx.begin(IsolationLevel::SI, |kv| {
                kv.update("1", "11").expect("can't replace");
                kv.rollback()
            })?;

            tx.view(IsolationLevel::SI, |view| {
                let x = view.get("1").expect("can't get");
                assert_eq!(x.data(), "10".as_bytes());
                Ok(())
            })?;
        }

        {
            tx.begin(IsolationLevel::SI, |kv| {
                kv.put("2", "20")?;
                kv.update("2", "21")?;
                let r = kv.get("2").unwrap();
                assert_eq!(r.data(), "21".as_bytes());
                kv.del("2")?;
                kv.rollback()
            })?;

            tx.view(IsolationLevel::SI, |view| {
                let x = view.get("2");
                assert!(x.is_err());
                Ok(())
            })?;
        }

        let tx = db.alloc().unwrap();

        {
            tx.begin(IsolationLevel::SI, |kv| {
                kv.put("1", "10")?;
                kv.commit()
            })?;

            tx.begin(IsolationLevel::SI, |kv| {
                kv.upsert("1", "11").expect("can't replace");
                kv.rollback()
            })?;

            tx.view(IsolationLevel::SI, |view| {
                let x = view.get("1").expect("can't get");
                assert_eq!(x.data(), "10".as_bytes());
                Ok(())
            })?;
        }

        {
            tx.begin(IsolationLevel::SI, |kv| {
                kv.put("2", "20")?;
                kv.upsert("2", "21")?;
                let r = kv.get("2").unwrap();
                assert_eq!(r.data(), "21".as_bytes());
                kv.del("2")?;
                kv.rollback()
            })?;

            tx.view(IsolationLevel::SI, |view| {
                let x = view.get("2");
                assert!(x.is_err());
                Ok(())
            })?;
        }
        Ok(())
    }
}
