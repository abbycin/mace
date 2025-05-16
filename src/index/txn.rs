use crate::{
    OpCode,
    cc::{
        context::Context,
        data::{Record, Ver},
        wal::{WalDel, WalPut, WalReplace},
        worker::SyncWorker,
    },
    index::{Key, data::Value, tree::Tree},
    utils::{INIT_CMD, NULL_CMD},
};
use std::sync::{Arc, atomic::Ordering::Relaxed};
use std::{cell::Cell, sync::atomic::AtomicU32};

use super::{ValRef, tree::SeekIter};

struct Guard<'a> {
    ctx: &'a Context,
    w: SyncWorker,
    refcnt: Arc<AtomicU32>,
}

impl Clone for Guard<'_> {
    fn clone(&self) -> Self {
        self.refcnt.fetch_add(1, Relaxed);
        Self {
            ctx: self.ctx,
            w: self.w,
            refcnt: self.refcnt.clone(),
        }
    }
}

impl Guard<'_> {
    fn destroy(&self) {
        if self.refcnt.fetch_sub(1, Relaxed) == 1 {
            self.ctx.free_worker(self.w);
        }
    }
}

fn get_impl<'a, K: AsRef<[u8]>>(
    ctx: &Context,
    tree: &Tree,
    mut w: SyncWorker,
    k: K,
) -> Result<ValRef<Record<'a>>, OpCode> {
    #[cfg(feature = "extra_check")]
    assert!(k.as_ref().len() > 0, "key must be non-empty");

    let wid = w.id;
    let start_ts = w.start_ts;
    let key = Key::new(k.as_ref(), start_ts, NULL_CMD);
    let r = tree.traverse::<Record, _>(key, |txid, t| {
        w.cc.is_visible_to(ctx, wid, t.worker_id(), start_ts, txid)
    })?;

    debug_assert!(!r.unwrap().is_tombstone());
    Ok(r)
}

// NOTE: SeekIter lives at least as long as Mace, but the worker may be shared among several Txns
fn seek_impl<'a, 'b, K>(
    ctx: &'a Context,
    tree: &'a Tree,
    mut w: SyncWorker,
    prefix: K,
    g: Guard<'a>,
) -> SeekIter<'a, 'b, Record<'b>>
where
    K: AsRef<[u8]>,
{
    #[cfg(feature = "extra_check")]
    assert!(prefix.as_ref().len() > 0, "prefix must be non-empty");

    let wid = w.id;
    let start_ts = w.start_ts;
    SeekIter::<Record>::new(
        tree,
        prefix,
        move |txid, t| w.cc.is_visible_to(ctx, wid, t.worker_id(), start_ts, txid),
        move || {
            g.destroy();
        },
    )
}

pub struct TxnKV<'a> {
    g: Guard<'a>,
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
            g: Guard {
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
        if self.g.w.ckpt_cnt.load(Relaxed) >= self.limit {
            return Err(OpCode::AbortTx);
        }
        Ok(())
    }

    fn modify<F>(&self, k: &[u8], v: &[u8], mut f: F) -> Result<Option<ValRef<Record>>, OpCode>
    where
        F: FnMut(&Option<(Key, ValRef<Record>)>, Ver, SyncWorker) -> Result<(), OpCode>,
    {
        #[cfg(feature = "extra_check")]
        assert!(k.as_ref().len() > 0, "key must be non-empty");

        self.should_abort()?;
        let start_ts = self.g.w.start_ts;
        let (wid, cmd_id) = (self.g.w.id, self.cmd_id());
        let key = Key::new(k, start_ts, cmd_id);
        let val = Value::Put(Record::normal(wid, v));

        self.tree
            .update(key, val, |opt| f(opt, *key.ver(), self.g.w))
    }

    fn put_impl(&self, k: &[u8], v: &[u8], logged: &mut bool) -> Result<(), OpCode> {
        self.modify(k, v, |opt, ver, mut w| {
            let r = match opt {
                None => Ok(()),
                Some((rk, rv)) => {
                    let t = rv.unwrap();
                    if rv.is_put()
                        || !w.cc.is_visible_to(
                            self.g.ctx,
                            self.g.w.id,
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
            r
        })
        .map(|_| ())
    }

    fn update_impl(&self, k: &[u8], v: &[u8], logged: &mut bool) -> Result<ValRef<Record>, OpCode> {
        self.modify(k, v, |opt, ver, mut w| match opt {
            None => Err(OpCode::NotFound),
            Some((rk, rv)) => {
                if rv.is_del() {
                    return Err(OpCode::NotFound);
                }
                let t = rv.unwrap();
                if !w
                    .cc
                    .is_visible_to(self.g.ctx, self.g.w.id, t.worker_id(), ver.txid, rk.txid)
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
                Ok(())
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

    pub fn update<K, V>(&self, k: K, v: V) -> Result<ValRef<Record>, OpCode>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let mut logged = false;
        self.update_impl(k.as_ref(), v.as_ref(), &mut logged)
    }

    pub fn upsert<K, V>(&self, k: K, v: V) -> Result<Option<ValRef<Record>>, OpCode>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let mut logged = false;
        let (k, v) = (k.as_ref(), v.as_ref());

        if self.put_impl(k, v, &mut logged).is_ok() {
            return Ok(None);
        }

        assert!(!logged);

        self.update_impl(k, v, &mut logged).map(Some)
    }

    pub fn del<T>(&self, k: T) -> Result<ValRef<Record>, OpCode>
    where
        T: AsRef<[u8]>,
    {
        self.should_abort()?;
        let mut w = self.g.w;
        let (wid, start_ts) = (w.id, w.start_ts);
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
                    if !w.cc.is_visible_to(
                        self.g.ctx,
                        self.g.w.id,
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
                    Ok(())
                }
            })
            .map(|x| x.unwrap())
    }

    pub fn commit(self) -> Result<(), OpCode> {
        self.should_abort()?;
        self.g.w.commit(self.g.ctx);
        self.is_end.set(true);
        Ok(())
    }

    pub fn rollback(self) -> Result<(), OpCode> {
        self.rollback_impl()
    }

    fn rollback_impl(&self) -> Result<(), OpCode> {
        if !self.is_end.get() {
            self.g.w.rollback(self.g.ctx, self.tree);
            self.is_end.set(true);
        }
        Ok(())
    }

    #[inline]
    pub fn get<K>(&self, k: K) -> Result<ValRef<Record>, OpCode>
    where
        K: AsRef<[u8]>,
    {
        get_impl(self.g.ctx, self.tree, self.g.w, k)
    }

    #[inline]
    pub fn seek<'b, K>(&self, prefix: K) -> SeekIter<'a, 'b, Record<'b>>
    where
        K: AsRef<[u8]>,
    {
        seek_impl(self.g.ctx, self.tree, self.g.w, prefix, self.g.clone())
    }
}

impl Drop for TxnKV<'_> {
    fn drop(&mut self) {
        let _ = self.rollback_impl();
        self.g.destroy();
    }
}

pub struct TxnView<'a> {
    g: Guard<'a>,
    tree: &'a Tree,
}

impl<'a> TxnView<'a> {
    pub(crate) fn new(ctx: &'a Context, tree: &'a Tree) -> Result<Self, OpCode> {
        let mut w = ctx.alloc_worker()?;
        w.view(ctx);
        Ok(Self {
            g: Guard {
                ctx,
                w,
                refcnt: Arc::new(AtomicU32::new(1)),
            },
            tree,
        })
    }

    #[inline]
    pub fn get<K: AsRef<[u8]>>(&self, k: K) -> Result<ValRef<Record>, OpCode> {
        get_impl(self.g.ctx, self.tree, self.g.w, k)
    }

    #[inline]
    pub fn seek<'b, K>(&self, prefix: K) -> SeekIter<'a, 'b, Record<'b>>
    where
        K: AsRef<[u8]>,
    {
        seek_impl(self.g.ctx, self.tree, self.g.w, prefix, self.g.clone())
    }

    pub fn show(&self) {
        self.tree.show::<Record>();
    }
}

impl Drop for TxnView<'_> {
    fn drop(&mut self) {
        self.g.destroy();
    }
}

#[cfg(test)]
mod test {
    use crate::{Mace, OpCode, Options, RandomPath};

    #[test]
    fn txnkv() -> Result<(), OpCode> {
        let path = RandomPath::tmp();
        let _ = std::fs::remove_dir_all(&*path);
        let opt = Options::new(&*path);
        let db = Mace::new(opt)?;
        let (k1, k2) = ("beast".as_bytes(), "senpai".as_bytes());
        let (v1, v2) = ("114514".as_bytes(), "1919810".as_bytes());

        let kv = db.begin()?;
        kv.put(k1, v1).expect("can't put");
        kv.put(k2, v2).expect("can't put");

        let r = kv.get(k1).expect("can't get");
        assert_eq!(r.data(), v1);
        let r = kv.get(k2).expect("can't get");
        assert_eq!(r.data(), v2);

        let r = kv.del(k1).expect("can't del");
        assert_eq!(r.data(), v1);
        kv.commit()?;

        let kv = db.begin()?;
        let r = kv.get(k1);
        assert!(r.is_err());

        let r = kv.get(k2).expect("can't get");
        assert_eq!(r.data(), v2);

        let r = kv.del(k2).expect("can't del");
        assert_eq!(r.data(), v2);
        kv.rollback()?;

        let kv = db.begin()?;
        let r = kv.get(k1);
        assert!(r.is_err());
        let r = kv.del(k2).expect("can't del");
        assert_eq!(r.data(), v2);
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
            assert_eq!(x.data(), "10".as_bytes());
        }

        {
            let kv = db.begin()?;
            kv.put("2", "20")?;
            kv.update("2", "21")?;
            let r = kv.get("2").unwrap();
            assert_eq!(r.data(), "21".as_bytes());
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
            assert_eq!(x.data(), "10".as_bytes());
        }

        {
            let kv = db.begin()?;
            kv.put("22", "20")?;
            kv.upsert("22", "21")?;
            let r = kv.get("22").unwrap();
            assert_eq!(r.data(), "21".as_bytes());
            kv.del("22")?;
            kv.rollback()?;

            let view = db.view()?;
            let x = view.get("22");
            assert!(x.is_err());
        }
        Ok(())
    }
}
