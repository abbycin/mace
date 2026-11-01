use super::{ValRef, tree::LatestValMeta};
#[cfg(feature = "metrics")]
use crate::utils::observe::{
    CounterMetric, EventKind, HistogramMetric, LATENCY_SAMPLE_SHIFT, ObserveEvent, observe_elapsed,
    sampled_instant,
};
use crate::{
    OpCode, Options,
    cc::{
        SnapshotStamp,
        context::{CCNode, Context},
        group::{TxnState, WriterGroup},
        is_retained_abort, is_visible_to,
        wal::{WalDel, WalMerge, WalPut, WalReplace},
    },
    index::tree::{Iter, Tree},
    map::flow::ForegroundWritePermit,
    must_ok,
    types::data::{Key, Record, Ver},
    utils::{Handle, NULL_CMD},
};
use crossbeam_epoch::Guard;
use std::cell::{Cell, UnsafeCell};
use std::ops::RangeBounds;
use std::sync::atomic::Ordering::Relaxed;

mod kv;
mod view;
mod write;

#[cfg(test)]
use self::view::prefix_upper_exclusive;

pub struct TxnKV<'a> {
    ctx: &'a Context,
    state: UnsafeCell<TxnState>,
    tree: &'a Tree,
    bucket_id: u64,
    is_end: Cell<bool>,
    limit: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FailCause {
    Aborted,
    Conflict,
}

struct RegGuard<'a> {
    group: &'a WriterGroup,
    finished: bool,
}

impl<'a> RegGuard<'a> {
    fn new(group: &'a WriterGroup) -> Self {
        Self {
            group,
            finished: false,
        }
    }

    fn finish(&mut self) {
        self.finished = true;
    }
}

impl Drop for RegGuard<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.group.reg_abot();
        }
    }
}

/// A read-only transaction (consistent view).
pub struct TxnView<'a> {
    ctx: &'a Context,
    pin: Handle<CCNode>,
    tree: &'a Tree,
}

#[cfg(test)]
mod test {
    use super::prefix_upper_exclusive;
    use crate::{BucketOptions, Mace, OpCode, Options, RandomPath};

    #[test]
    fn txnkv() {
        txnkv_impl().unwrap();
    }

    #[test]
    fn prefix_upper_exclusive_handles_carry() {
        assert_eq!(
            prefix_upper_exclusive(&[0x61, 0x62, 0x63]),
            Some(vec![0x61, 0x62, 0x64])
        );
        assert_eq!(
            prefix_upper_exclusive(&[0x61, 0xff, 0xff]),
            Some(vec![0x62])
        );
        assert_eq!(prefix_upper_exclusive(&[0xff]), None);
        assert_eq!(prefix_upper_exclusive(&[0xff, 0xff]), None);
    }

    fn txnkv_impl() -> Result<(), OpCode> {
        let path = RandomPath::tmp();
        let _ = std::fs::remove_dir_all(&*path);
        let opt = Options::new(&*path).validate().unwrap();
        let mace = Mace::new(opt)?;
        let (k1, k2) = ("beast".as_bytes(), "senpai".as_bytes());
        let (v1, v2) = ("114514".as_bytes(), "1919810".as_bytes());
        let db = mace.new_bucket("default", BucketOptions::default())?;

        let kv = db.begin()?;
        kv.put(k1, v1).expect("can't put");
        kv.put(k2, v2).expect("can't put");

        kv.del(k1).expect("can't del");
        kv.commit()?;

        let kv = db.begin()?;
        let r = kv.get(k1);
        assert!(r.is_err());

        let r = kv.get(k2).expect("can't get");
        assert_eq!(r.slice(), v2);

        kv.del(k2).expect("can't del");
        drop(kv);

        let kv = db.begin()?;
        let r = kv.get(k1);
        assert!(r.is_err());
        kv.del(k2).expect("can't del");
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

        {
            let kv = db.begin()?;
            kv.put("fast", "v0")?;
            kv.commit()?;

            let kv = db.begin()?;
            kv.update("fast", "v1")?;
            kv.commit()?;

            let view = db.view()?;
            assert_eq!(view.get("fast")?.slice(), b"v1");
        }

        {
            let kv = db.begin()?;
            let r = kv.update("missing", "v1");
            assert!(matches!(r, Err(OpCode::NotFound)));
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
        let path = RandomPath::tmp();
        let mut opt = Options::new(&*path);
        let consolidate_threshold = 256;
        opt.tmp_store = true;
        let mace = Mace::new(opt.validate().unwrap())?;
        let db = mace.new_bucket(
            "default",
            BucketOptions {
                split_elems: consolidate_threshold * 2,
                consolidate_threshold,
                ..BucketOptions::default()
            },
        )?;

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
