use std::sync::Arc;

pub(crate) use store::store::Store;
use tree::{data::Value, tree::Tree};
pub use utils::{options::Options, OpCode};

mod cc;
mod map;
mod store;
mod tree;
mod utils;
pub use tree::Val;

pub struct Mace {
    store: Arc<Store>,
    tree: Tree,
}

impl Mace {
    pub fn new(opt: Options) -> Result<Self, OpCode> {
        let store = Arc::new(Store::new(opt)?);
        let this = Self {
            store: store.clone(),
            tree: Tree::new(store)?,
        };

        Ok(this)
    }

    pub fn put<'b, K, V>(&self, key: K, val: V) -> Result<(), OpCode>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let gsn = 0;
        self.tree.put(gsn, key.as_ref(), Value::Put(val.as_ref()))
    }

    pub fn del(&self, key: impl AsRef<[u8]>) -> Result<Vec<u8>, OpCode> {
        let gsn = 0;
        self.tree.del(gsn, key.as_ref()).map(|x| x.to_vec())
    }

    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Val, OpCode> {
        let gsn = 0;
        self.tree.get(gsn, key.as_ref())
    }

    #[allow(dead_code)]
    fn scan(&self, _from: &[u8], _to: &[u8]) {
        todo!()
    }

    pub fn options(&self) -> Arc<Options> {
        self.store.opt.clone()
    }
}

#[cfg(test)]
mod test {
    use crate::{Mace, OpCode, Options};

    #[test]
    fn test_lib() -> Result<(), OpCode> {
        let opt = Options::default();
        let _ = std::fs::remove_dir_all(&opt.db_path);
        let m = Mace::new(opt)?;
        let cnt = 10;
        let mut pairs = Vec::new();

        for i in 0..cnt {
            pairs.push((format!("key{}", i), format!("val{}", i)));
        }

        for i in 0..cnt {
            m.put(&pairs[i].0, &pairs[i].1)?;
        }

        for i in 0..cnt {
            let v = m.get(&pairs[i].0).expect("can't find key");
            assert_eq!(v.to_vec().as_slice(), pairs[i].1.as_bytes());
        }

        for i in 0..cnt {
            if i % 2 == 0 {
                assert!(m.del(&pairs[i].0).is_ok());
            }
        }

        for i in 0..cnt {
            let v = m.get(&pairs[i].0);

            if i % 2 == 0 {
                assert!(v.is_err());
            } else {
                assert_eq!(v.unwrap().to_vec().as_slice(), pairs[i].1.as_bytes());
            }
        }
        drop(m);

        let m = Mace::new(Options::default())?;

        for i in 0..cnt {
            let v = m.get(&pairs[i].0);

            if i % 2 == 0 {
                assert!(v.is_err());
            } else {
                assert_eq!(v.unwrap().to_vec().as_slice(), pairs[i].1.as_bytes());
            }
        }

        assert!(m.del("foo").is_err());

        Ok(())
    }
}
