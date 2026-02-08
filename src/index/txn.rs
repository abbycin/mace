use super::ValRef;
use crate::{
    OpCode,
    cc::{
        cc::ConcurrencyControl,
        context::{CCNode, Context},
        group::TxnState,
        wal::{WalDel, WalPut, WalReplace},
    },
    index::tree::{Iter, Tree},
    types::data::{Key, Record, Ver},
    utils::{Handle, NULL_CMD, data::Position},
};
use std::cell::{Cell, UnsafeCell};
use std::sync::atomic::Ordering::Relaxed;

fn get_impl<K: AsRef<[u8]>>(
    ctx: &Context,
    cc: &ConcurrencyControl,
    tree: &Tree,
    group_id: u8,
    start_ts: u64,
    k: K,
) -> Result<ValRef, OpCode> {
    #[cfg(feature = "extra_check")]
    assert!(!k.as_ref().is_empty(), "key must be non-empty");

    let g = crossbeam_epoch::pin();
    let key = Key::new(k.as_ref(), Ver::new(start_ts, NULL_CMD));
    let r = tree.traverse(&g, key, |txid, record_gid| {
        cc.is_visible_to(ctx, group_id, record_gid, start_ts, txid)
    })?;

    Ok(r)
}

fn seek_impl<'a, K>(
    cc: &'a ConcurrencyControl,
    tree: &'a Tree,
    group_id: u8,
    start_ts: u64,
    prefix: K,
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

    tree.range(b..e.as_slice(), move |ctx, txid, record_gid| {
        cc.is_visible_to(ctx, group_id, record_gid, start_ts, txid)
    })
}

/// A read-write transaction.
pub struct TxnKV<'a> {
    ctx: &'a Context,
    state: UnsafeCell<TxnState>,
    tree: &'a Tree,
    bucket_id: u64,
    is_end: Cell<bool>,
    limit: usize,
}

impl<'a> TxnKV<'a> {
    pub(crate) fn new(ctx: &'a Context, tree: &'a Tree) -> Result<Self, OpCode> {
        let start_ts = ctx.alloc_oracle();
        let gid = ctx.next_group_id();
        let g = ctx.group(gid);
        let start_ckpt = g.ckpt_cnt.load(Relaxed);
        let mut state = TxnState::new(gid, start_ts, start_ckpt);
        let bucket_id = tree.bucket_id();
        let max_ckpt_per_txn = tree.store.opt.max_ckpt_per_txn;

        tree.bucket.state.inc_txn_ref();

        {
            let mut log = g.logging.lock();
            state.prev_lsn = log.record_begin(start_ts)?;
            g.active_txns.insert(start_ts, state.prev_lsn);
        }

        g.cc.commit_tree.compact(ctx, gid as u8);

        Ok(Self {
            ctx,
            state: UnsafeCell::new(state),
            tree,
            bucket_id,
            is_end: Cell::new(false),
            limit: max_ckpt_per_txn,
        })
    }

    fn should_abort(&self) -> Result<(), OpCode> {
        let state = self.state_ref();
        let g = self.ctx.group(state.group_id);
        if self.is_end.get() || g.ckpt_cnt.load(Relaxed) - state.start_ckpt >= self.limit {
            return Err(OpCode::AbortTx);
        }
        Ok(())
    }

    #[inline]
    fn state_ref(&self) -> &TxnState {
        unsafe { &*self.state.get() }
    }

    #[inline]
    #[allow(clippy::mut_from_ref)]
    fn state_mut(&self) -> &mut TxnState {
        unsafe { &mut *self.state.get() }
    }

    fn modify<F>(&self, k: &[u8], v: &[u8], mut f: F) -> Result<Option<ValRef>, OpCode>
    where
        F: FnMut(&Option<(Key, ValRef)>, Ver, &mut TxnState) -> Result<(u8, Position), OpCode>,
    {
        #[cfg(feature = "extra_check")]
        assert!(!k.as_ref().is_empty(), "key must be non-empty");

        self.should_abort()?;
        let g = crossbeam_epoch::pin();
        let state = self.state_mut();
        let start_ts = state.start_ts;
        let gid = state.group_id;

        let cmd_id_val = state.cmd_id;
        state.cmd_id += 1;
        let key = Key::new(k, Ver::new(start_ts, cmd_id_val));
        let val = Record::normal(gid as u8, v);

        self.tree
            .update(&g, key, val, |opt| f(opt, *key.ver(), state))
    }

    fn put_impl(&self, k: &[u8], v: &[u8], logged: &mut bool) -> Result<(), OpCode> {
        self.modify(k, v, |opt, ver, state| {
            let gid = state.group_id;
            let g = self.ctx.group(gid);

            let r = match opt {
                None => Ok(()),
                Some((rk, rv)) => {
                    if rv.is_put()
                        || !g.cc.is_visible_to(
                            self.ctx,
                            gid as u8,
                            rv.unwrap().group_id(),
                            state.start_ts,
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
                state.modified = true;
                let mut log = g.logging.lock();
                let new_pos = log.record_update(
                    &Key::new(k, ver),
                    WalPut::new(v.len()),
                    [].as_slice(),
                    v,
                    state.prev_lsn,
                    self.bucket_id,
                )?;
                state.prev_lsn = new_pos;
            }
            r.map(|_| (gid as u8, state.prev_lsn))
        })
        .map(|_| ())
    }

    fn update_impl(&self, k: &[u8], v: &[u8], logged: &mut bool) -> Result<ValRef, OpCode> {
        self.modify(k, v, |opt, ver, state| {
            let gid = state.group_id;
            let g = self.ctx.group(gid);
            match opt {
                None => Err(OpCode::NotFound),
                Some((rk, rv)) => {
                    if rv.is_del() {
                        return Err(OpCode::NotFound);
                    }
                    let t = rv.unwrap();
                    if !g.cc.is_visible_to(
                        self.ctx,
                        gid as u8,
                        t.group_id(),
                        state.start_ts,
                        rk.txid,
                    ) {
                        return Err(OpCode::AbortTx);
                    }

                    if !*logged {
                        state.modified = true;
                        *logged = true;
                        let mut log = g.logging.lock();
                        let new_pos = log.record_update(
                            &Key::new(rk.raw, ver),
                            WalReplace::new(t.data().len(), v.len()),
                            t.data(),
                            v,
                            state.prev_lsn,
                            self.bucket_id,
                        )?;
                        state.prev_lsn = new_pos;
                    }
                    Ok((gid as u8, state.prev_lsn))
                }
            }
        })
        .map(|x| x.unwrap())
    }

    /// Puts a key-value pair into the bucket.
    pub fn put<K, V>(&self, k: K, v: V) -> Result<(), OpCode>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let mut logged = false;
        self.put_impl(k.as_ref(), v.as_ref(), &mut logged)
    }

    /// Updates existing key-value pair in the bucket.
    pub fn update<K, V>(&self, k: K, v: V) -> Result<ValRef, OpCode>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let mut logged = false;
        self.update_impl(k.as_ref(), v.as_ref(), &mut logged)
    }

    /// Upserts a key-value pair into the bucket.
    pub fn upsert<K, V>(&self, k: K, v: V) -> Result<Option<ValRef>, OpCode>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let mut logged = false;
        let (k, v) = (k.as_ref(), v.as_ref());
        self.modify(k, v, |opt, ver, state| {
            let gid = state.group_id;
            let g = self.ctx.group(gid);
            match opt {
                None => {
                    if !logged {
                        state.modified = true;
                        logged = true;
                        let mut log = g.logging.lock();
                        let new_pos = log.record_update(
                            &Key::new(k, ver),
                            WalPut::new(v.len()),
                            &[],
                            v,
                            state.prev_lsn,
                            self.bucket_id,
                        )?;
                        state.prev_lsn = new_pos;
                    }
                    Ok((gid as u8, state.prev_lsn))
                }
                Some((rk, rv)) => {
                    let t = rv.unwrap();
                    if !g.cc.is_visible_to(
                        self.ctx,
                        gid as u8,
                        t.group_id(),
                        state.start_ts,
                        rk.txid,
                    ) {
                        return Err(OpCode::AbortTx);
                    }

                    if !logged {
                        state.modified = true;
                        logged = true;
                        let mut log = g.logging.lock();
                        let new_pos = log.record_update(
                            &Key::new(rk.raw, ver),
                            WalReplace::new(t.data().len(), v.len()),
                            t.data(),
                            v,
                            state.prev_lsn,
                            self.bucket_id,
                        )?;
                        state.prev_lsn = new_pos;
                    }
                    Ok((gid as u8, state.prev_lsn))
                }
            }
        })
    }

    /// Deletes a key-value pair from the bucket.
    pub fn del<T>(&self, k: T) -> Result<ValRef, OpCode>
    where
        T: AsRef<[u8]>,
    {
        self.should_abort()?;
        let state = self.state_mut();
        let (gid, start_ts) = (state.group_id, state.start_ts);

        let cmd_id_val = state.cmd_id;
        state.cmd_id += 1;

        let key = Key::new(k.as_ref(), Ver::new(start_ts, cmd_id_val));
        let val = Record::remove(gid as u8);
        let mut logged = false;
        let guard = crossbeam_epoch::pin();

        let res = self.tree.update(&guard, key, val, |opt| {
            let g = self.ctx.group(gid);
            match opt {
                None => Err(OpCode::NotFound),
                Some((rk, rv)) => {
                    if rv.is_del() {
                        return Err(OpCode::NotFound);
                    }
                    let t = rv.unwrap();
                    if !g
                        .cc
                        .is_visible_to(self.ctx, gid as u8, t.group_id(), start_ts, rk.txid)
                    {
                        return Err(OpCode::AbortTx);
                    }

                    if !logged {
                        logged = true;
                        state.modified = true;
                        let mut log = g.logging.lock();
                        let new_pos = log.record_update(
                            &key,
                            WalDel::new(t.data().len()),
                            t.data(),
                            [].as_slice(),
                            state.prev_lsn,
                            self.bucket_id,
                        )?;
                        state.prev_lsn = new_pos;
                    }
                    Ok((gid as u8, state.prev_lsn))
                }
            }
        });

        res.map(|x| x.unwrap())
    }

    /// Commits the transaction.
    pub fn commit(self) -> Result<(), OpCode> {
        self.should_abort()?;
        let state = self.state_ref();
        let g = self.ctx.group(state.group_id);

        if !state.modified {
            g.logging.lock().record_commit(state.start_ts)?;
            g.active_txns.remove(&state.start_ts);
            self.is_end.set(true);
            return Ok(());
        }

        let commit_ts = self.ctx.alloc_oracle();

        {
            let mut log = g.logging.lock();
            log.record_commit(state.start_ts)?;
            log.stabilize()?;
        }

        g.cc.commit_tree.append(state.start_ts, commit_ts);
        g.cc.latest_cts.store(commit_ts, Relaxed);
        g.cc.collect_wmk(self.ctx);

        g.active_txns.remove(&state.start_ts);
        self.is_end.set(true);
        Ok(())
    }

    /// Gets the value associated with a key.
    #[inline]
    pub fn get<K>(&self, k: K) -> Result<ValRef, OpCode>
    where
        K: AsRef<[u8]>,
    {
        let state = self.state_ref();
        let group_id = state.group_id;
        get_impl(
            self.ctx,
            &self.ctx.group(group_id).cc,
            self.tree,
            group_id as u8,
            state.start_ts,
            k,
        )
    }

    /// Seeks an iterator to a key prefix.
    /// prefix can't be empty and the [`Iter::Item`] is only valid in current iteration.
    ///
    /// **NOTE:** [`Iter`] will save a clone of the resource, so do not save [`Iter`] to avoid
    /// resource shortage.
    #[inline]
    pub fn seek<K>(&self, prefix: K) -> Iter<'_>
    where
        K: AsRef<[u8]>,
    {
        let state = self.state_ref();
        let group_id = state.group_id;
        seek_impl(
            &self.ctx.group(group_id).cc,
            self.tree,
            group_id as u8,
            state.start_ts,
            prefix,
        )
    }
}

impl Drop for TxnKV<'_> {
    fn drop(&mut self) {
        if !self.is_end.get() {
            let g = crossbeam_epoch::pin();
            let state = self.state_ref();
            let grp = self.ctx.group(state.group_id);

            if !state.modified {
                grp.logging
                    .lock()
                    .record_abort(state.start_ts)
                    .inspect_err(|e| {
                        log::error!("can't record abort, {:?}", e);
                    })
                    .expect("can't fail");
            } else {
                grp.logging
                    .lock()
                    .stabilize()
                    .map_err(|e| {
                        log::error!("can't stabilize WAL, {:?}", e);
                    })
                    .expect("can't fail");

                use crate::cc::wal::{Location, WalReader};
                use crate::utils::block::Block;

                const SMALL_SIZE: usize = 256;
                let mut block = Block::alloc(SMALL_SIZE);
                let reader = WalReader::new(self.ctx, &g);
                let location = Location {
                    group_id: state.group_id as u32,
                    pos: state.prev_lsn,
                    len: 0,
                };

                reader
                    .rollback(&mut block, state.start_ts, location, |_| self.tree.clone())
                    .inspect_err(|e| {
                        log::error!("can't rollback, {:?}", e);
                    })
                    .expect("can't fail");

                let commit_ts = self.ctx.alloc_oracle();
                grp.cc.commit_tree.append(state.start_ts, commit_ts);
                grp.cc.latest_cts.store(commit_ts, Relaxed);
                grp.cc.collect_wmk(self.ctx);
            }
            grp.active_txns.remove(&state.start_ts);
            self.is_end.set(true);
        }
        self.tree.bucket.state.dec_txn_ref();
    }
}

/// A read-only transaction (consistent view).
pub struct TxnView<'a> {
    ctx: &'a Context,
    cc: Handle<CCNode>,
    group_id: u8,
    tree: &'a Tree,
}

impl<'a> TxnView<'a> {
    pub(crate) fn new(ctx: &'a Context, tree: &'a Tree) -> Result<Self, OpCode> {
        let cc = ctx.alloc_cc();
        Ok(Self {
            ctx,
            cc,
            group_id: u8::MAX,
            tree,
        })
    }

    /// Gets the value associated with a key in this view.
    #[inline]
    pub fn get<K: AsRef<[u8]>>(&self, k: K) -> Result<ValRef, OpCode> {
        get_impl(
            self.ctx,
            &self.cc,
            self.tree,
            self.group_id,
            self.cc.start_ts,
            k,
        )
    }

    /// Seeks an iterator to a key prefix in this view.
    /// prefix can't be empty and the [`Iter::Item`] is only valid in current iteration.
    ///
    /// **NOTE:** [`Iter`] will save a clone of the resource, so do not save [`Iter`] to avoid
    /// resource shortage.
    #[inline]
    pub fn seek<K>(&self, prefix: K) -> Iter<'_>
    where
        K: AsRef<[u8]>,
    {
        seek_impl(&self.cc, self.tree, self.group_id, self.cc.start_ts, prefix)
    }
}

impl Drop for TxnView<'_> {
    fn drop(&mut self) {
        self.ctx.free_cc(self.cc);
    }
}

#[cfg(test)]
mod test {
    use crate::{Mace, OpCode, Options, RandomPath};

    #[test]
    fn txnkv() {
        txnkv_impl().unwrap();
    }

    fn txnkv_impl() -> Result<(), OpCode> {
        let path = RandomPath::tmp();
        let _ = std::fs::remove_dir_all(&*path);
        let opt = Options::new(&*path).validate().unwrap();
        let mace = Mace::new(opt)?;
        let (k1, k2) = ("beast".as_bytes(), "senpai".as_bytes());
        let (v1, v2) = ("114514".as_bytes(), "1919810".as_bytes());
        let db = mace.new_bucket("default")?;

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
        drop(kv);

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
            drop(kv);

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
            drop(kv);

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
            drop(kv);

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
            drop(kv);

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
        drop(db);
        drop(mace);
        Ok(())
    }

    #[test]
    fn cross_long_txn() {
        cross_long_txn_impl().unwrap();
    }

    fn cross_long_txn_impl() -> Result<(), OpCode> {
        let path = RandomPath::new();
        let mut opt = Options::new(&*path);
        let consolidate_threshold = 256;
        opt.tmp_store = true;
        opt.split_elems = consolidate_threshold * 2;
        opt.consolidate_threshold = consolidate_threshold;
        let mace = Mace::new(opt.validate().unwrap())?;
        let db = mace.new_bucket("default")?;

        let kv = db.begin()?;
        kv.put("foo", "bar")?;
        kv.commit()?;

        let view = db.view()?;
        let kv = db.begin()?;

        kv.update("foo", "bar1")?;
        kv.update("foo", "bar2")?;

        // trigger consolidate
        for i in 0..consolidate_threshold {
            let x = format!("key_{i}");
            kv.put(&x, &x)?;
        }

        let r = kv.get("foo")?;
        assert_eq!(r.slice(), "bar2".as_bytes());
        kv.commit()?;

        let v = view.get("foo")?;
        assert_eq!(v.slice(), "bar".as_bytes());

        drop(view);
        drop(db);
        drop(mace);
        Ok(())
    }
}
