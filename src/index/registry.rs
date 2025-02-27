use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{Arc, Mutex},
};

use crate::utils::{traits::IValCodec, DEFAULT_TREEID, NULL_CMD, NULL_ORACLE, ROOT_TREEID};
use crate::{utils::ROOT_PID, OpCode, Store};

use super::data::Id;
use super::Key;
use super::{data::Value, tree::Tree};

#[derive(Clone)]
pub(crate) struct Registry {
    pub(crate) store: Arc<Store>,
    pub(crate) tree: Tree,
    map: Arc<Mutex<HashMap<u64, usize>>>,
}

// functions for internal use
impl Registry {
    pub(crate) fn search(&self, id: u64) -> Option<Tree> {
        if id == ROOT_TREEID {
            return Some(self.tree.clone());
        }
        match self
            .tree
            .get::<_, Id>(Key::new(Id::new(id).data(), NULL_ORACLE, NULL_CMD))
        {
            Ok((_, v)) => {
                if v.is_del() {
                    return None;
                }
                let v = v.unwrap();
                let tree = Tree::load(self.store.clone(), v.id, id);
                Some(tree)
            }
            Err(_) => None,
        }
    }

    pub(crate) fn init_tree(&self, id: u64, pid: u64, txid: u64) -> Tree {
        self.tree
            .put(
                Key::new(Id::new(id).data(), txid, NULL_CMD),
                Value::Put(Id::new(pid)),
            )
            .expect("can't go wrong");
        Tree::new(self.store.clone(), pid, id)
    }

    // since the record was logged after all kv pairs were removed, we simply insert a Del record
    pub(crate) fn destroy_tree(&self, id: u64, pid: u64, txid: u64) {
        self.tree
            .put(
                Key::new(Id::new(id).data(), txid, NULL_CMD),
                Value::Del(Id::new(pid)),
            )
            .expect("can't go wrong");
    }
}

impl Registry {
    pub(crate) fn new(store: Arc<Store>) -> Self {
        let tree = if store.is_fresh(ROOT_PID) {
            let pid = store.page.alloc().expect("no space");
            assert_eq!(pid, ROOT_PID);
            Tree::new(store.clone(), ROOT_PID, ROOT_TREEID)
        } else {
            Tree::load(store.clone(), ROOT_PID, ROOT_TREEID)
        };

        Self {
            store,
            tree,
            map: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn create_tree_impl(&self, tree_id: u64, txid: u64) -> Tree {
        let pid = self.store.page.alloc().expect("no space");
        let w = self.store.context.alloc();
        w.logging.record_put(tree_id, txid, pid);
        w.logging.wait_flush(); // make sure the tree is flushed before txn starts
        self.store.context.free(w);

        self.init_tree(tree_id, pid, txid)
    }

    pub(crate) fn open_default(&self) -> Result<Tree, OpCode> {
        match self.get_tree(DEFAULT_TREEID) {
            Err(_) => {
                let txid = self.store.context.alloc_oracle();
                Ok(self.create_tree_impl(DEFAULT_TREEID, txid))
            }
            o => o,
        }
    }

    pub(crate) fn create_tree(&self) -> Result<Tree, OpCode> {
        let txid = self.store.context.alloc_oracle();
        let tree = self.create_tree_impl(txid, txid);
        self.inc_ref(tree.id());
        Ok(tree)
    }

    pub(crate) fn get_tree(&self, id: u64) -> Result<Tree, OpCode> {
        if id == ROOT_TREEID {
            return Err(OpCode::Invalid);
        }
        let Some(tree) = self.search(id) else {
            return Err(OpCode::NotFound);
        };

        self.inc_ref(id);
        Ok(tree)
    }

    // remove when the tree is not referenced or has only one reference, except root and the deafult
    pub(crate) fn remove_tree(&self, id: u64) -> Result<(), OpCode> {
        if id <= DEFAULT_TREEID {
            return Err(OpCode::Invalid);
        }
        let mut map = self.map.lock().unwrap();
        let Some(tree) = self.search(id) else {
            return Err(OpCode::NotFound);
        };

        if let Some(cnt) = map.get_mut(&id) {
            *cnt = cnt.saturating_sub(1);
            if *cnt > 1 {
                return Err(OpCode::Again);
            }
        }

        tree.remove_all();

        let pid = tree.root_pid();
        let txid = self.store.context.alloc_oracle();

        let w = self.store.context.alloc();
        w.logging.record_del(tree.id(), txid, pid);
        w.logging.wait_flush(); // make sure the tree is flushed before txn starts
        self.store.context.free(w);

        self.tree
            .put(
                Key::new(Id::new(tree.id()).data(), txid, NULL_CMD),
                Value::Del(Id::new(pid)),
            )
            .expect("can't go wrong");
        Ok(())
    }

    pub(crate) fn inc_ref(&self, id: u64) {
        let mut map = self.map.lock().unwrap();

        match map.entry(id) {
            Entry::Occupied(ref mut x) => {
                let cnt = x.get_mut();
                *cnt = cnt.saturating_add(1);
            }
            Entry::Vacant(x) => {
                x.insert(1);
            }
        }
    }

    pub(crate) fn dec_ref(&self, id: u64) {
        let mut map = self.map.lock().unwrap();

        if let Some(x) = map.get_mut(&id) {
            *x = x.saturating_sub(1);
        }
    }
}
