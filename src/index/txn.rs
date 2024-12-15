use crate::{
    cc::{
        context::Context,
        data::Record,
        wal::{IWalReader, PayloadType, WalDel, WalFileReader, WalPut, WalReplace, WalUpdate},
        worker::SyncWorker,
    },
    index::{data::Value, tree::Tree, Key},
    utils::{IsolationLevel, INIT_CMD},
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

    pub fn put<K, V>(&self, k: K, v: V) -> Result<(), OpCode>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let start_ts = self.w.txn.start_ts;
        let mut w = self.w;
        let (wid, cmd_id) = (w.id, self.cmd_id());
        let (key, v) = (Key::new(k.as_ref(), start_ts, cmd_id), v.as_ref());

        w.txn.modified = true;
        w.logging.record_update(
            self.tree.id(),
            start_ts,
            cmd_id,
            WalPut::new(key.raw.len(), v.len()),
            key.raw,
            v,
        );
        self.tree.put(key, Value::Put(Record::normal(wid, v)))
    }

    pub fn replace<T>(&self, k: T, v: T) -> Result<Val<Record>, OpCode>
    where
        T: AsRef<[u8]>,
    {
        let start_ts = self.w.txn.start_ts;
        let mut w = self.w;
        let (wid, cmd_id) = (w.id, self.cmd_id());
        let (k, v) = (Key::new(k.as_ref(), start_ts, cmd_id), v.as_ref());
        let (rk, rv) = self.tree.get::<Record>(k)?;
        let t = rv.unwrap();

        if rv.is_put()
            && w.cc
                .is_visible_to(&self.ctx, self.w, t.worker_id(), start_ts, rk.txid)
        {
            w.txn.modified = true;
            w.logging.record_update(
                self.tree.id(),
                start_ts,
                cmd_id,
                WalReplace::new(t.worker_id(), 0, rk.txid, k.raw.len(), v.len()),
                k.raw,
                t.data(),
            );
            self.tree.put(k, Value::Put(Record::normal(wid, v)))?;
            Ok(rv)
        } else {
            Err(OpCode::AbortTx)
        }
    }

    #[inline]
    pub fn get<K>(&self, k: K) -> Result<Val<Record>, OpCode>
    where
        K: AsRef<[u8]>,
    {
        get_impl(&self.ctx, &self.tree, self.w, k, self.cmd_id()).map(|x| x.1)
    }

    pub fn del<K>(&self, k: K) -> Result<Val<Record>, OpCode>
    where
        K: AsRef<[u8]>,
    {
        let mut w = self.w;
        let (wid, start_ts) = (w.id, w.txn.start_ts);
        let key = Key::new(k.as_ref(), self.w.txn.start_ts, self.cmd_id());
        let (rk, rv) = self.tree.get::<Record>(key)?;

        debug_assert_eq!(rk.raw, key.raw);
        let t = rv.unwrap();
        if rv.is_put()
            && w.cc
                .is_visible_to(&self.ctx, self.w, t.worker_id(), start_ts, rk.txid)
        {
            w.txn.modified = true;
            w.logging.record_update(
                self.tree.id(),
                start_ts,
                key.cmd,
                WalDel::new(t.worker_id(), rk.cmd, rk.txid, rk.raw.len(), t.data().len()),
                rk.raw,
                t.data(),
            );
            self.tree.del(key, Value::Del(Record::remove(wid)))?;
            Ok(rv)
        } else {
            Err(OpCode::AbortTx)
        }
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

        let mut w = self.ctx.get_worker();
        let id = w.id;
        w.txn.reset(start_ts, level);
        w.tx_id.store(start_ts, Relaxed);
        w.cc.global_wmk_tx = self.ctx.wmk_info.wmk_odlest.load(Relaxed);
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
        w.logging.record_begin(start_ts);
        TxnKV::new(self.ctx.clone(), self.tree.clone(), w)
    }

    pub fn commit(&self, kv: TxnKV) {
        debug_assert_eq!(kv.tree.id(), self.tree.id());
        let mut w = kv.w;
        let mut txn = w.txn;
        let txid = txn.start_ts;

        if !w.txn.modified {
            w.logging.record_commit(txid);
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

        w.logging.record_commit(txid);
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
                self.tree.del(k, v).expect("can't be wrong");
            }
            PayloadType::Update => {
                let upd = update.update();
                let k = Key::new(upd.key(), txid, kv.cmd_id());
                let v = Value::Put(Record::normal(upd.wid(), upd.val()));
                self.tree.put(k, v).expect("can't be wrong");
            }
            PayloadType::Delete => {
                let del = update.del();
                let k = Key::new(del.key(), txid, kv.cmd_id());
                let v = Value::Put(Record::normal(w.id, del.val()));
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

        reader.read_txn(txid, |update| {
            self.rollback_impl(update, kv, w, txid);
        });
    }

    pub fn rollback(&self, kv: TxnKV) {
        debug_assert_eq!(kv.tree.id(), self.tree.id());
        let mut w = kv.w;
        let txid = w.txn.start_ts;

        w.logging.record_abort(txid);

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
    use crate::{utils::IsolationLevel, Db, OpCode, Options, RandomPath};

    #[test]
    fn txnkv() -> Result<(), OpCode> {
        let path = RandomPath::tmp();
        let _ = std::fs::remove_dir_all(&*path);
        let mut opt = Options::new(&*path);
        opt.bind_core = true;
        let db = Db::open(opt)?;
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

        Ok(())
    }
}
