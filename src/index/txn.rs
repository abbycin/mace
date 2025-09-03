use super::ValRef;
use crate::{
    OpCode,
    cc::{
        context::Context,
        wal::{WalDel, WalPut, WalReplace},
        worker::SyncWorker,
    },
    index::tree::{Iter, Tree},
    types::data::{Key, Record, Value, Ver},
    utils::{INIT_CMD, NULL_CMD},
};
use crossbeam_epoch::Guard;
use std::sync::{
    Arc,
    atomic::Ordering::{Relaxed, Release},
};
use std::{cell::Cell, sync::atomic::AtomicU32};

struct Package<'a> {
    ctx: &'a Context,
    w: SyncWorker,
    refcnt: Arc<AtomicU32>,
}

impl Clone for Package<'_> {
    fn clone(&self) -> Self {
        self.refcnt.fetch_add(1, Relaxed);
        Self {
            ctx: self.ctx,
            w: self.w,
            refcnt: self.refcnt.clone(),
        }
    }
}

impl Package<'_> {
    fn destroy(&self) {
        if self.refcnt.fetch_sub(1, Release) == 1 {
            self.ctx.free_worker(self.w);
        }
    }
}

fn get_impl<K: AsRef<[u8]>>(
    ctx: &Context,
    g: &Guard,
    tree: &Tree,
    mut w: SyncWorker,
    k: K,
) -> Result<ValRef, OpCode> {
    #[cfg(feature = "extra_check")]
    assert!(!k.as_ref().is_empty(), "key must be non-empty");

    let wid = w.id;
    let start_ts = w.start_ts;
    let key = Key::new(k.as_ref(), Ver::new(start_ts, NULL_CMD));
    let r = tree.traverse(g, key, |txid, t| {
        w.cc.is_visible_to(ctx, wid, t.worker_id(), start_ts, txid)
    })?;

    Ok(r)
}

fn seek_impl<'a, K>(
    ctx: &'a Context,
    tree: &'a Tree,
    mut w: SyncWorker,
    prefix: K,
    p: Package<'a>,
) -> Iter<'a>
where
    K: AsRef<[u8]>,
{
    let b = prefix.as_ref();
    let mut e = b.to_vec();
    #[cfg(feature = "extra_check")]
    assert!(!e.is_empty(), "prefix can't be empty");

    if *e.last().unwrap() == u8::MAX {
        e.push(0);
    } else {
        *e.last_mut().unwrap() += 1;
    }

    let wid = w.id;
    let start_ts = w.start_ts;

    tree.range(
        b..e.as_slice(),
        move |txid, t| w.cc.is_visible_to(ctx, wid, t.worker_id(), start_ts, txid),
        move || {
            p.destroy();
        },
    )
}

pub struct TxnKV<'a> {
    p: Package<'a>,
    tree: &'a Tree,
    seq: Cell<u32>,
    is_end: Cell<bool>,
    limit: usize,
}

impl<'a> TxnKV<'a> {
    pub(crate) fn new(ctx: &'a Context, tree: &'a Tree) -> Result<Self, OpCode> {
        let mut w = ctx.alloc_worker()?;
        w.begin(ctx);
        let limit = tree.store.opt.max_ckpt_per_txn;
        Ok(Self {
            p: Package {
                ctx,
                w,
                refcnt: Arc::new(AtomicU32::new(1)),
            },
            tree,
            seq: Cell::new(INIT_CMD),
            is_end: Cell::new(false),
            limit,
        })
    }

    fn cmd_id(&self) -> u32 {
        let r = self.seq.get();
        self.seq.set(r + 1);
        r
    }

    fn should_abort(&self) -> Result<(), OpCode> {
        if self.is_end.get() || self.p.w.logging.ckpt_cnt() >= self.limit {
            return Err(OpCode::AbortTx);
        }
        Ok(())
    }

    fn modify<F>(&self, k: &[u8], v: &[u8], mut f: F) -> Result<Option<ValRef>, OpCode>
    where
        F: FnMut(&Option<(Key, ValRef)>, Ver, SyncWorker) -> Result<(u16, u64), OpCode>,
    {
        #[cfg(feature = "extra_check")]
        assert!(!k.as_ref().is_empty(), "key must be non-empty");

        let g = crossbeam_epoch::pin();
        self.should_abort()?;
        let start_ts = self.p.w.start_ts;
        let (wid, cmd_id) = (self.p.w.id, self.cmd_id());
        let key = Key::new(k, Ver::new(start_ts, cmd_id));
        let val = Value::Put(Record::normal(wid, v));

        self.tree
            .update(&g, key, val, |opt| f(opt, *key.ver(), self.p.w))
    }

    fn put_impl(&self, k: &[u8], v: &[u8], logged: &mut bool) -> Result<(), OpCode> {
        self.modify(k, v, |opt, ver, mut w| {
            let r = match opt {
                None => Ok(()),
                Some((rk, rv)) => {
                    let t = rv.unwrap();
                    if rv.is_put()
                        || !w.cc.is_visible_to(
                            self.p.ctx,
                            self.p.w.id,
                            t.worker_id(),
                            ver.txid,
                            rk.txid,
                        )
                    {
                        Err(OpCode::AbortTx)
                    } else {
                        Ok(())
                    }
                }
            };
            if r.is_ok() && !*logged {
                *logged = true;
                w.modified = true;
                w.logging
                    .record_update(ver, WalPut::new(v.len()), k, [].as_slice(), v);
            }
            r.map(|_| (w.id, w.logging.seq()))
        })
        .map(|_| ())
    }

    fn update_impl(&self, k: &[u8], v: &[u8], logged: &mut bool) -> Result<ValRef, OpCode> {
        self.modify(k, v, |opt, ver, mut w| match opt {
            None => Err(OpCode::NotFound),
            Some((rk, rv)) => {
                if rv.is_del() {
                    return Err(OpCode::NotFound);
                }
                let t = rv.unwrap();
                if !w
                    .cc
                    .is_visible_to(self.p.ctx, self.p.w.id, t.worker_id(), ver.txid, rk.txid)
                {
                    return Err(OpCode::AbortTx);
                }

                if !*logged {
                    w.modified = true;
                    *logged = true;
                    w.logging.record_update(
                        ver,
                        WalReplace::new(t.data().len(), v.len()),
                        rk.raw,
                        t.data(),
                        v,
                    );
                }
                Ok((w.id, w.logging.seq()))
            }
        })
        .map(|x| x.unwrap())
    }

    pub fn put<K, V>(&self, k: K, v: V) -> Result<(), OpCode>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let mut logged = false;
        self.put_impl(k.as_ref(), v.as_ref(), &mut logged)
    }

    pub fn update<K, V>(&self, k: K, v: V) -> Result<ValRef, OpCode>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let mut logged = false;
        self.update_impl(k.as_ref(), v.as_ref(), &mut logged)
    }

    pub fn upsert<K, V>(&self, k: K, v: V) -> Result<Option<ValRef>, OpCode>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let mut logged = false;
        let (k, v) = (k.as_ref(), v.as_ref());
        self.modify(k, v, |opt, ver, mut w| match opt {
            None => {
                if !logged {
                    w.modified = true;
                    logged = true;
                    w.logging
                        .record_update(ver, WalPut::new(v.len()), k, &[], v);
                }
                Ok((w.id, w.logging.seq()))
            }
            Some((rk, rv)) => {
                let t = rv.unwrap();
                if !w
                    .cc
                    .is_visible_to(self.p.ctx, self.p.w.id, t.worker_id(), ver.txid, rk.txid)
                {
                    return Err(OpCode::AbortTx);
                }

                if !logged {
                    w.modified = true;
                    logged = true;
                    w.logging.record_update(
                        ver,
                        WalReplace::new(t.data().len(), v.len()),
                        rk.raw,
                        t.data(),
                        v,
                    );
                }
                Ok((w.id, w.logging.seq()))
            }
        })
    }

    pub fn del<T>(&self, k: T) -> Result<ValRef, OpCode>
    where
        T: AsRef<[u8]>,
    {
        self.should_abort()?;
        let mut w = self.p.w;
        let (wid, start_ts) = (w.id, w.start_ts);
        let key = Key::new(k.as_ref(), Ver::new(start_ts, self.cmd_id()));
        let val = Value::Del(Record::remove(wid));
        let mut logged = false;
        let g = crossbeam_epoch::pin();

        self.tree
            .update::<Record, _>(&g, key, val, |opt| match opt {
                None => Err(OpCode::NotFound),
                Some((rk, rv)) => {
                    if rv.is_del() {
                        return Err(OpCode::NotFound);
                    }
                    let t = rv.unwrap();
                    if !w.cc.is_visible_to(
                        self.p.ctx,
                        self.p.w.id,
                        t.worker_id(),
                        start_ts,
                        rk.txid,
                    ) {
                        return Err(OpCode::AbortTx);
                    }

                    if !logged {
                        logged = true;
                        w.modified = true;
                        w.logging.record_update(
                            *key.ver(),
                            WalDel::new(t.data().len()),
                            rk.raw,
                            t.data(),
                            [].as_slice(),
                        );
                    }
                    Ok((w.id, w.logging.seq()))
                }
            })
            .map(|x| x.unwrap())
    }

    pub fn commit(&self) -> Result<(), OpCode> {
        self.should_abort()?;
        self.p.w.commit(self.p.ctx);
        self.p.destroy();
        self.is_end.set(true);
        Ok(())
    }

    pub fn rollback(&self) -> Result<(), OpCode> {
        if !self.is_end.get() {
            let g = crossbeam_epoch::pin();
            self.p.w.rollback(&g, self.p.ctx, self.tree);
            self.p.destroy();
            self.is_end.set(true);
            Ok(())
        } else {
            Err(OpCode::AbortTx)
        }
    }

    #[inline]
    pub fn get<K>(&self, k: K) -> Result<ValRef, OpCode>
    where
        K: AsRef<[u8]>,
    {
        let g = crossbeam_epoch::pin();
        get_impl(self.p.ctx, &g, self.tree, self.p.w, k)
    }

    /// prefix can't be empty and the [`Iter::Item`] is only valid in current iteration
    #[inline]
    pub fn seek<K>(&'a self, prefix: K) -> Iter<'a>
    where
        K: AsRef<[u8]>,
    {
        seek_impl(self.p.ctx, self.tree, self.p.w, prefix, self.p.clone())
    }
}

impl Drop for TxnKV<'_> {
    fn drop(&mut self) {
        let _ = self.rollback();
    }
}

pub struct TxnView<'a> {
    p: Package<'a>,
    tree: &'a Tree,
}

impl<'a> TxnView<'a> {
    pub(crate) fn new(ctx: &'a Context, tree: &'a Tree) -> Result<Self, OpCode> {
        let mut w = ctx.alloc_worker()?;
        w.view(ctx);
        Ok(Self {
            p: Package {
                ctx,
                w,
                refcnt: Arc::new(AtomicU32::new(1)),
            },
            tree,
        })
    }

    #[inline]
    pub fn get<K: AsRef<[u8]>>(&self, k: K) -> Result<ValRef, OpCode> {
        let g = crossbeam_epoch::pin();
        get_impl(self.p.ctx, &g, self.tree, self.p.w, k)
    }

    /// prefix can't be empty and the [`Iter::Item`] is only valid in current iteration
    #[inline]
    pub fn seek<K>(&'a self, prefix: K) -> Iter<'a>
    where
        K: AsRef<[u8]>,
    {
        seek_impl(self.p.ctx, self.tree, self.p.w, prefix, self.p.clone())
    }
}

impl Drop for TxnView<'_> {
    fn drop(&mut self) {
        self.p.destroy();
    }
}

#[cfg(test)]
mod test {
    use crate::{Mace, OpCode, Options, RandomPath};

    #[test]
    fn txnkv() -> Result<(), OpCode> {
        let path = RandomPath::tmp();
        let _ = std::fs::remove_dir_all(&*path);
        let opt = Options::new(&*path).validate().unwrap();
        let db = Mace::new(opt)?;
        let (k1, k2) = ("beast".as_bytes(), "senpai".as_bytes());
        let (v1, v2) = ("114514".as_bytes(), "1919810".as_bytes());

        let kv = db.begin()?;
        kv.put(k1, v1).expect("can't put");
        kv.put(k2, v2).expect("can't put");

        let r = kv.del(k1).expect("can't del");
        assert_eq!(r.slice(), v1);
        kv.commit()?;

        let kv = db.begin()?;
        let r = kv.get(k1);
        assert!(r.is_err());

        let r = kv.get(k2).expect("can't get");
        assert_eq!(r.slice(), v2);

        let r = kv.del(k2).expect("can't del");
        assert_eq!(r.slice(), v2);
        kv.rollback()?;

        let kv = db.begin()?;
        let r = kv.get(k1);
        assert!(r.is_err());
        let r = kv.del(k2).expect("can't del");
        assert_eq!(r.slice(), v2);
        let r = kv.del(k2);
        assert!(r.is_err());

        kv.commit()?;

        let kv = db.begin()?;
        let r = kv.get(k1);
        assert!(r.is_err());
        let r = kv.get(k2);
        assert!(r.is_err());

        kv.commit()?;

        {
            let kv = db.begin()?;
            kv.put("1", "10")?;
            kv.commit()?;

            let kv = db.begin()?;
            kv.update("1", "11").expect("can't replace");
            kv.rollback()?;

            let view = db.view()?;
            let x = view.get("1").expect("can't get");
            assert_eq!(x.slice(), "10".as_bytes());
        }

        {
            let kv = db.begin()?;
            kv.put("2", "20")?;
            kv.update("2", "21")?;
            let r = kv.get("2").unwrap();
            assert_eq!(r.slice(), "21".as_bytes());
            kv.del("2")?;
            kv.rollback()?;

            let view = db.view()?;
            let x = view.get("2");
            assert!(x.is_err());
        }

        {
            let kv = db.begin()?;
            kv.put("11", "10")?;
            kv.commit()?;

            let kv = db.begin()?;
            kv.upsert("11", "11").expect("can't replace");
            kv.rollback()?;

            let view = db.view()?;
            let x = view.get("11").expect("can't get");
            assert_eq!(x.slice(), "10".as_bytes());
        }

        {
            let kv = db.begin()?;
            kv.put("22", "20")?;
            kv.upsert("22", "21")?;
            let r = kv.get("22").unwrap();
            assert_eq!(r.slice(), "21".as_bytes());
            kv.del("22")?;
            kv.rollback()?;

            let view = db.view()?;
            let x = view.get("22");
            assert!(x.is_err());
        }

        {
            let kv = db.begin()?;
            kv.put("elder", "+1s")?;
            kv.del("elder")?;
            kv.commit()?;
            let kv = db.begin()?;
            let r = kv.update("elder", "mo");
            // a remove key can't be update again
            assert!(r.is_err());
            // but can be upsert
            kv.upsert("elder", "mo")?;
            kv.commit()?;
            let view = db.view()?;
            assert_eq!(view.get("elder").unwrap().slice(), "mo".as_bytes());
        }
        Ok(())
    }
}
