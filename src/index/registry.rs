use std::cmp::Ordering;
use std::sync::atomic::Ordering::Relaxed;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::cc::data::Ver;
use crate::utils::{NULL_CMD, NULL_ORACLE, ROOT_TREEID};
use crate::{
    cc::{
        wal::{WalMgrDel, WalMgrPut},
        worker::SyncWorker,
    },
    slice_to_number,
    utils::ROOT_PID,
    IsolationLevel, OpCode, Store,
};

use super::tree::{CachePadding, SharedLocks, NR_LOCKS};
use super::{data::Value, tree::Tree, Key};

#[derive(Clone)]
pub(crate) struct Registry {
    pub(crate) store: Arc<Store>,
    pub(crate) tree: Tree,
    open_map: Arc<Mutex<HashMap<u64, usize>>>,
    locks: SharedLocks,
}

impl Registry {
    fn begin(&self, mut w: SyncWorker) -> u64 {
        w.begin(&self.store.context, IsolationLevel::SI)
    }

    fn log<const PUT: bool>(&self, mut w: SyncWorker, txid: u64, pid: u64, name: &[u8]) {
        w.txn.modified = true;
        if PUT {
            let record = WalMgrPut::new(txid);
            w.logging.record_update(
                Ver::new(txid, NULL_CMD),
                ROOT_TREEID,
                record,
                name,
                [].as_slice(),
                &pid.to_le_bytes(),
            );
        } else {
            let record = WalMgrDel::new(txid);
            w.logging.record_update(
                Ver::new(txid, NULL_CMD),
                ROOT_TREEID,
                record,
                name,
                [].as_slice(),
                &pid.to_le_bytes(),
            );
        }
    }

    fn commit(&self, w: SyncWorker) {
        w.commit(&self.store.context);
    }

    pub(crate) fn new(store: Arc<Store>) -> Self {
        let name = "registry".into();
        let locks = Arc::new([const { Mutex::new(CachePadding) }; NR_LOCKS]);
        if !store.is_fresh(ROOT_PID) {
            Self {
                store: store.clone(),
                tree: Tree::load(store, locks.clone(), ROOT_PID, ROOT_TREEID, name),
                open_map: Arc::new(Mutex::new(HashMap::new())),
                locks,
            }
        } else {
            let pid = store.page.alloc().expect("no space");
            assert_eq!(pid, ROOT_PID);
            Self {
                store: store.clone(),
                tree: Tree::new(store, locks.clone(), ROOT_PID, ROOT_TREEID, name),
                open_map: Arc::new(Mutex::new(HashMap::new())),
                locks,
            }
        }
    }

    fn get_impl<F>(&self, key: Key, f: F) -> Result<Tree, OpCode>
    where
        F: Fn(&Key, &Key) -> Ordering,
    {
        match self.tree.get_by::<&[u8], _>(key, f) {
            Ok((rk, rv)) => {
                if rv.is_del() {
                    return Err(OpCode::NotFound);
                }
                let v = rv.data();
                let pid = slice_to_number!(v, u64);
                let name = std::str::from_utf8(rk.raw).unwrap();
                let r = Tree::load(
                    self.store.clone(),
                    self.locks.clone(),
                    pid,
                    rk.txid,
                    name.to_string(),
                );
                Ok(r)
            }
            Err(_) => Err(OpCode::NotFound),
        }
    }

    pub(crate) fn get_by_name(&self, name: &[u8]) -> Option<Tree> {
        let mut _map = self.open_map.lock().unwrap();
        let key = Key::new(name, NULL_ORACLE, NULL_CMD);
        self.get_impl(key, |x, y| x.raw.cmp(y.raw)).ok()
    }

    pub(crate) fn get_by_id(&self, id: u64) -> Option<Tree> {
        let mut _map = self.open_map.lock().unwrap();
        if id == ROOT_TREEID {
            return Some(self.tree.clone());
        }
        let key = Key::new([].as_slice(), id, NULL_CMD);
        self.get_impl(key, |x, y| x.ver().cmp(y.ver())).ok()
    }

    fn new_tree(&self, name: &String) -> Tree {
        let pid = self.tree.store.page.alloc().expect("no space");
        let w = self.store.context.alloc();
        let txid = self.begin(w);
        let k = Key::new(name.as_bytes(), txid, NULL_CMD);
        self.log::<true>(w, k.txid, pid, name.as_bytes());
        self.tree
            .put(k, Value::Put(&pid.to_le_bytes()[..]))
            .expect("can't go wrong");
        let (_, v) = self.tree.get::<&[u8]>(k).expect("impossible");
        let r = slice_to_number!(*v.data(), u64);
        assert_eq!(r, pid);
        let r = Tree::new(
            self.tree.store.clone(),
            self.locks.clone(),
            pid,
            k.txid,
            name.clone(),
        );
        self.commit(w);
        self.store.context.free(w);
        r
    }

    pub(crate) fn init_tree(&self, name: &[u8]) {
        let t = self.get_by_name(name).expect("impossible");
        t.init();
    }

    pub(crate) fn open(&self, name: impl AsRef<str>) -> Tree {
        let mut map = self.open_map.lock().unwrap();
        let name = name.as_ref().to_string();
        let r = match self.get_impl(Key::new(name.as_bytes(), NULL_ORACLE, NULL_CMD), |x, y| {
            x.raw.cmp(y.raw)
        }) {
            Err(_) => self.new_tree(&name),
            Ok(t) => t,
        };
        if let Some(v) = map.get_mut(&r.root_index.pid) {
            *v += 1;
        } else {
            map.insert(r.root_index.pid, 1);
        }
        r
    }

    pub(crate) fn remove(&self, tree: &Tree) -> Result<(), OpCode> {
        let mut map = self.open_map.lock().unwrap();
        let Some(v) = map.get_mut(&tree.root_index.pid) else {
            return Err(OpCode::NotFound);
        };
        *v -= 1;
        if *v != 0 {
            return Err(OpCode::Again);
        }
        let pid = tree.root_index.pid;
        let w = self.store.context.alloc();
        let txid = self.begin(w);
        let name = tree.name().as_bytes();
        self.log::<false>(w, tree.seq(), tree.root_index.pid, name);
        map.remove(&tree.root_index.pid);
        self.tree
            .put::<&[u8]>(
                Key::new(tree.name().as_bytes(), txid, NULL_CMD),
                Value::Del([].as_slice()),
            )
            .expect("can't go wrong");
        self.store
            .page
            .unmap(pid, self.store.page.index(pid).load(Relaxed));
        self.commit(w);
        self.store.context.free(w);
        Ok(())
    }
}
