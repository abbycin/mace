use std::sync::{atomic::AtomicU64, Arc};

pub use cc::data::Record;
use cc::INIT_ORACLE;
pub use index::txn::{Tx, TxnKV, TxnView};
use index::{
    data::Value,
    tree::{Registry, Tree},
};
pub(crate) use store::store::Store;
pub use utils::{options::Options, IsolationLevel, OpCode, RandomPath};

mod cc;
mod index;
mod map;
mod store;
mod utils;
use crate::index::Key;
pub use index::Val;

pub struct Mace {
    store: Arc<Store>,
    tree: Tree,
    seq: AtomicU64,
}

impl Mace {
    fn txid(&self) -> u64 {
        self.seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    pub fn new(opt: Options) -> Result<Self, OpCode> {
        let store = Arc::new(Store::new(opt)?);
        let mgr = Registry::new(store.clone())?;
        let tree = mgr.open("mace")?;
        let this = Self {
            store: store.clone(),
            tree,
            seq: AtomicU64::new(INIT_ORACLE),
        };

        Ok(this)
    }

    pub fn put<K, V>(&self, key: K, val: V) -> Result<(), OpCode>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.tree.put(
            Key::new(key.as_ref(), self.txid(), 0),
            Value::Put(val.as_ref()),
        )
    }

    pub fn del(&self, key: impl AsRef<[u8]>) -> Result<(), OpCode> {
        self.tree.del(
            Key::new(key.as_ref(), self.txid(), 0),
            Value::Del("del".as_bytes()),
        )
    }

    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Val<&[u8]>, OpCode> {
        let (_, v) = self.tree.get(Key::new(key.as_ref(), self.txid(), 0))?;
        Ok(v)
    }

    #[allow(dead_code)]
    fn scan(&self, _from: &[u8], _to: &[u8]) {
        todo!()
    }

    pub fn options(&self) -> Arc<Options> {
        self.store.opt.clone()
    }
}

#[derive(Clone)]
pub struct Db {
    store: Arc<Store>,
    tree: Registry,
    default_tree: String,
}

impl Db {
    pub const DEFAULT_TREE: &'static str = "MACE";

    pub fn open(opt: Options) -> Result<Self, OpCode> {
        let store = Arc::new(Store::new(opt)?);
        let tree = Registry::new(store.clone())?;
        Ok(Self {
            store,
            tree,
            default_tree: Self::DEFAULT_TREE.into(),
        })
    }

    pub fn default(&self) -> Tx {
        let tree = self.tree.open(&self.default_tree).expect("can't open tree");
        Tx {
            ctx: self.store.context.clone(),
            tree,
        }
    }

    pub fn load_tree(&self, name: impl AsRef<str>) -> Result<Tx, OpCode> {
        let tree = self.tree.open(name)?;

        Ok(Tx {
            ctx: self.store.context.clone(),
            tree,
        })
    }

    pub fn set_default(&mut self, name: impl AsRef<str>) {
        let mut new = name.as_ref().into();
        std::mem::swap(&mut self.default_tree, &mut new);
    }

    pub fn remove(&self, tx: Tx) -> Result<(), OpCode> {
        self.tree.remove(&tx.tree)
    }
}

#[cfg(test)]
mod test {

    use crate::{Mace, OpCode, Options, RandomPath};

    #[test]
    fn test_lib() -> Result<(), OpCode> {
        let path = RandomPath::tmp();
        let _ = std::fs::remove_dir_all(&*path);
        let opt = Options::new(&*path);
        let m = Mace::new(opt.clone())?;
        let cnt = 10;
        let mut pairs = Vec::new();
        let wmk = 4;

        for i in 0..cnt {
            pairs.push((format!("key{}", i), format!("val{}", i)));
        }

        for (k, v) in &pairs {
            m.put(k, v)?;
        }

        for (k, v) in &pairs {
            let rv = m.get(k).expect("can't find key");
            assert_eq!(rv.put(), &v.as_bytes());
        }

        m.store.context.wmk_info.update_wmk(wmk);

        for (k, v) in &pairs {
            m.put(k, v)?;
        }

        for (i, (k, _)) in pairs.iter().enumerate() {
            if i % 2 == 0 {
                assert!(m.del(k).is_ok());
            }
        }

        for (i, (k, v)) in pairs.iter().enumerate() {
            let rv = m.get(k).expect("error occurred");

            if i % 2 == 0 {
                assert!(rv.is_del());
            } else {
                assert_eq!(rv.put(), &v.as_bytes());
            }
        }

        let txid = m.txid();

        drop(m);

        let m = Mace::new(opt)?;

        // a patch
        for _ in 0..txid {
            m.txid();
        }

        for (i, (k, v)) in pairs.iter().enumerate() {
            let rv = m.get(k).expect("error occurred");

            if i % 2 == 0 {
                assert!(rv.is_del());
            } else {
                assert_eq!(rv.put(), &v.as_bytes());
            }
        }

        Ok(())
    }
}
