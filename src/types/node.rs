use crate::hot_true;
use parking_lot::{Mutex, MutexGuard, RwLock};
use std::{
    cmp::Ordering::{self, Equal, Greater, Less},
    ops::{Bound, Deref, DerefMut},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering::Relaxed},
    },
};

use crate::{
    cc::{context::Context, is_retained_abort},
    must_exist,
    types::{
        base::{BaseIter, BaseRevIter},
        data::{
            Index, IntlKey, IntlSeg, IterItem, Key, LeafSeg, Record, Val, Ver,
            cmp_raw_with_prefixed_tail,
        },
        header::{BoxHeader, NodeType, RemoteHeader},
        refbox::{BaseView, BoxView, DeltaView, RemoteView},
        traits::{
            IAsBoxRef, IBoxHeader, ICodec, IDecode, IFrameAlloc, IHeader, IKey, ILoader, IVal,
        },
    },
    utils::{
        Handle, INIT_ORACLE, NULL_ADDR, NULL_CMD, NULL_ORACLE, NULL_PID, OpCode,
        data::Position,
        imtree::{ImTree, Iter, RangeIter},
        options::Options,
    },
};

use super::{header::TagKind, refbox::BoxRef};

mod compact;
mod core;
mod filter;
mod iter;

pub(crate) use compact::{CompactVal, LeafCompactSource, PlainValSource};
#[cfg(test)]
pub(crate) use compact::{FoldClass, classify_fold_size};
pub(crate) use core::{Junk, MergeOp, Node};
#[cfg(test)]
pub(crate) use filter::LeafFilter;
#[cfg(test)]
pub(crate) use iter::LeafIter;
pub(crate) use iter::{RawLeafIter, RawLeafRevIter};

#[cfg(test)]
mod test {
    use parking_lot::Mutex;
    use std::{
        collections::HashMap,
        rc::Rc,
        sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed},
    };

    use crate::{
        BucketOptions,
        types::{
            data::{Key, LeafSeg, Record, Val, Ver},
            node::{Junk, LeafFilter, NULL_ADDR, Node},
            refbox::{BoxRef, BoxView, DeltaView},
            traits::{ICodec, IFrameAlloc, IHeader, ILoader},
        },
        utils::data::Position,
    };

    struct AInner {
        map: Mutex<HashMap<u64, BoxRef>>,
        off: AtomicU64,
        remote_loads: AtomicUsize,
        inline: AtomicUsize,
    }

    #[derive(Clone)]
    struct A {
        inner: Rc<AInner>,
    }

    impl A {
        fn new() -> Self {
            Self {
                inner: Rc::new(AInner {
                    map: Mutex::new(HashMap::new()),
                    off: AtomicU64::new(0),
                    remote_loads: AtomicUsize::new(0),
                    inline: AtomicUsize::new(BucketOptions::MIN_INLINE_SIZE),
                }),
            }
        }

        /// test-only: change the inline boundary a rebuild is classified under,
        /// to model a persisted bucket threshold change on an existing tree
        fn set_inline(&self, inline: usize) {
            self.inner.inline.store(inline, Relaxed);
        }

        fn load(&self, addr: u64) -> BoxRef {
            let lk = self.inner.map.lock();
            lk.get(&addr).unwrap().clone()
        }
    }

    impl IFrameAlloc for A {
        fn alloc(&mut self, size: u32) -> BoxRef {
            let addr = self
                .inner
                .off
                .fetch_add(BoxRef::real_size(size) as u64, Relaxed);
            let p = BoxRef::alloc(size, addr);
            let mut lk = self.inner.map.lock();
            lk.insert(addr, p.clone());
            p
        }

        fn inline_size(&self) -> usize {
            self.inner.inline.load(Relaxed)
        }
    }

    impl ILoader for A {
        fn load_pinned(&self, addr: u64) -> BoxView {
            self.load(addr).view()
        }

        fn pin(&self, data: BoxRef) {
            let mut lk = self.inner.map.lock();
            lk.insert(data.header().addr, data);
        }

        fn copy(&self) -> Self {
            self.clone()
        }

        fn copy_detached(&self) -> Self {
            self.clone()
        }

        fn load_sibling(&self, addr: u64) -> BoxRef {
            self.load(addr)
        }

        fn load_blob(&self, addr: u64, _cache: bool) -> BoxRef {
            self.inner.remote_loads.fetch_add(1, Relaxed);
            self.load(addr)
        }
    }

    #[test]
    fn find_latest_meta_skips_remote_value_load() {
        let mut a = A::new();
        let l = a.clone();
        let node = Node::new_leaf(&mut a, l.clone(), 0, Position::MIN);
        let key = Key::new("blob".as_bytes(), Ver::new(1, 1));
        let value = vec![7u8; BucketOptions::MIN_INLINE_SIZE + 16];
        let record = Record::normal(1, &value);
        let (delta, remote) = DeltaView::from_key_val(&mut a, &key, &record, 0, Position::MIN);
        node.insert_inplace(
            delta.view().as_delta(),
            remote
                .as_ref()
                .map(|x| x.header().total_size as usize)
                .unwrap_or(0),
        );
        let _ = remote;
        node.save_delta(delta);
        let (node, _) = node.compact(&mut a, 1, None, &mut |_| {}, None);

        a.inner.remote_loads.store(0, Relaxed);
        let meta = node
            .find_latest_meta(&key)
            .expect("latest meta should exist");
        assert_eq!(meta.ver, *key.ver());
        assert_eq!(meta.group_id, 1);
        assert!(!meta.is_del);
        assert_eq!(a.inner.remote_loads.load(Relaxed), 0);

        let latest = node.find_latest(&key).expect("latest value should exist");
        assert_eq!(latest.1.data(), value.as_slice());
        assert_eq!(a.inner.remote_loads.load(Relaxed), 1);
    }

    #[test]
    fn find_latest_meta_delta_max_fast_path_keeps_full_equivalence() {
        let mut a = A::new();
        let l = a.clone();
        let node = Node::new_leaf(&mut a, l.clone(), 0, Position::MIN);

        // seed the base sst with a and c, then compact (delta becomes empty)
        for raw in ["a", "c"] {
            let key = Key::new(raw.as_bytes(), Ver::new(1, 1));
            let record = Record::normal(1, raw.as_bytes());
            let (delta, remote) = DeltaView::from_key_val(&mut a, &key, &record, 0, Position::MIN);
            node.insert_inplace(
                delta.view().as_delta(),
                remote
                    .as_ref()
                    .map(|x| x.header().total_size as usize)
                    .unwrap_or(0),
            );
            let _ = remote;
            node.save_delta(delta);
        }
        let (node, _) = node.compact(&mut a, 1, None, &mut |_| {}, None);

        // delta now holds only b; the base holds a and c
        let key_b = Key::new("b".as_bytes(), Ver::new(2, 1));
        let record = Record::normal(1, b"b");
        let (delta, remote) = DeltaView::from_key_val(&mut a, &key_b, &record, 0, Position::MIN);
        node.insert_inplace(
            delta.view().as_delta(),
            remote
                .as_ref()
                .map(|x| x.header().total_size as usize)
                .unwrap_or(0),
        );
        let _ = remote;
        node.save_delta(delta);

        // probe c: above the delta max (b), the fast path must still find the base copy
        let meta_c = node
            .find_latest_meta(&Key::new("c".as_bytes(), Ver::new(u64::MAX, u32::MAX)))
            .expect("base key above delta max must stay visible");
        assert_eq!(meta_c.ver.txid, 1);
        assert!(!meta_c.is_del);

        // probe b: inside the delta, must resolve through the delta (newest txid 2)
        let meta_b = node
            .find_latest_meta(&Key::new("b".as_bytes(), Ver::new(u64::MAX, u32::MAX)))
            .expect("delta key must resolve through the delta");
        assert_eq!(meta_b.ver.txid, 2);

        // probe a: below the delta max and present only in base
        let meta_a = node
            .find_latest_meta(&Key::new("a".as_bytes(), Ver::new(u64::MAX, u32::MAX)))
            .expect("base key below delta max must stay visible");
        assert_eq!(meta_a.ver.txid, 1);

        // probe z: above the delta max and absent everywhere
        assert!(
            node.find_latest_meta(&Key::new("z".as_bytes(), Ver::new(u64::MAX, u32::MAX)))
                .is_none()
        );
    }

    #[test]
    fn find_latest_meta_is_independent_of_probe_txid() {
        let mut a = A::new();
        let l = a.clone();
        let node = Node::new_leaf(&mut a, l.clone(), 0, Position::MIN);
        let raw = "race".as_bytes();

        for txid in [20u64, 10u64] {
            let key = Key::new(raw, Ver::new(txid, 1));
            let value = format!("v{txid}").into_bytes();
            let record = Record::normal(1, &value);
            let (delta, remote) = DeltaView::from_key_val(&mut a, &key, &record, 0, Position::MIN);
            node.insert_inplace(
                delta.view().as_delta(),
                remote
                    .as_ref()
                    .map(|x| x.header().total_size as usize)
                    .unwrap_or(0),
            );
            let _ = remote;
            node.save_delta(delta);
        }

        let probe = Key::new(raw, Ver::new(5, 1));
        let meta = node
            .find_latest_meta(&probe)
            .expect("latest meta should follow raw-key head");
        assert_eq!(meta.ver.txid, 20);
        assert!(!meta.is_del);
    }

    #[test]
    fn inline_size_boundary_is_remote() {
        let mut a = A::new();
        let l = a.clone();
        let node = Node::new_leaf(&mut a, l.clone(), 0, Position::MIN);
        let key = Key::new("blob".as_bytes(), Ver::new(1, 1));
        let value = vec![7u8; BucketOptions::MIN_INLINE_SIZE];
        let record = Record::normal(1, &value);
        let (delta, remote) = DeltaView::from_key_val(&mut a, &key, &record, 0, Position::MIN);
        let remote = remote.expect("equal inline boundary must allocate remote");

        node.insert_inplace(delta.view().as_delta(), remote.header().total_size as usize);
        node.save_delta(delta);
        let (node, _) = node.compact(&mut a, 1, None, &mut |_| {}, None);

        a.inner.remote_loads.store(0, Relaxed);
        let latest = node.find_latest(&key).expect("latest value should exist");
        assert_eq!(latest.1.data(), value.as_slice());
        assert_eq!(a.inner.remote_loads.load(Relaxed), 1);
    }

    #[test]
    fn leaf_iter() {
        let mut a = A::new();
        let txid = AtomicU64::new(1);
        const CONSOLIDATE_THRESHOLD: usize = 64;
        let lsn = Position::MIN;
        let mut j = Junk::new();

        {
            let l = a.clone();
            let mut node = Node::new_leaf(&mut a, l, 0, Position::MIN);

            let (d1, r1) = DeltaView::from_key_val(
                &mut a,
                &Key::new("foo".as_bytes(), Ver::new(txid.fetch_add(1, Relaxed), 1)),
                &Record::normal(1, "1".as_bytes()),
                0,
                lsn,
            );
            let (d2, r2) = DeltaView::from_key_val(
                &mut a,
                &Key::new("foo".as_bytes(), Ver::new(txid.load(Relaxed), 2)),
                &Record::normal(1, "2".as_bytes()),
                0,
                lsn,
            );

            let (d3, r3) = DeltaView::from_key_val(
                &mut a,
                &Key::new("foo".as_bytes(), Ver::new(txid.load(Relaxed), 3)),
                &Record::remove(1),
                0,
                lsn,
            );

            node.insert_inplace(
                d1.view().as_delta(),
                r1.as_ref()
                    .map(|x| x.header().total_size as usize)
                    .unwrap_or(0),
            );
            let _ = r1;
            node.save_delta(d1);
            node.insert_inplace(
                d2.view().as_delta(),
                r2.as_ref()
                    .map(|x| x.header().total_size as usize)
                    .unwrap_or(0),
            );
            let _ = r2;
            node.save_delta(d2);
            (node, _) = node.compact(&mut a, 1, None, &mut |_| {}, None);

            let mut iter = node.leaf_iter(&mut j, 1, BucketOptions::MIN_INLINE_SIZE, None, None);
            assert_eq!(drain_len(&mut iter), 2);

            node.insert_inplace(
                d3.view().as_delta(),
                r3.as_ref()
                    .map(|x| x.header().total_size as usize)
                    .unwrap_or(0),
            );
            let _ = r3;
            node.save_delta(d3);
            let mut iter = node.leaf_iter(&mut j, 3, BucketOptions::MIN_INLINE_SIZE, None, None);
            assert_eq!(drain_len(&mut iter), 0);
        }

        let l = a.clone();
        let mut node = Node::new_leaf(&mut a, l, 0, Position::MIN);

        for i in 0..30 {
            let raw = format!("key_{i}");
            let k = Key::new(raw.as_bytes(), Ver::new(txid.fetch_add(1, Relaxed), 1));
            let v = Record::normal(1, raw.as_bytes());
            let (delta, r) = DeltaView::from_key_val(&mut a, &k, &v, 0, lsn);
            node.insert_inplace(
                delta.view().as_delta(),
                r.as_ref()
                    .map(|x| x.header().total_size as usize)
                    .unwrap_or(0),
            );
            let _ = r;
            node.save_delta(delta);

            if node.delta_len() >= CONSOLIDATE_THRESHOLD {
                (node, _) = node.compact(&mut a, 3, None, &mut |_| {}, None);
            }
        }

        // this will create siblings
        for i in 0..20 {
            let raw = format!("key_{i}");
            let k = Key::new(raw.as_bytes(), Ver::new(txid.fetch_add(1, Relaxed), 0));
            let v = Record::remove(1);
            let (delta, r) = DeltaView::from_key_val(&mut a, &k, &v, 0, lsn);
            node.insert_inplace(
                delta.view().as_delta(),
                r.as_ref()
                    .map(|x| x.header().total_size as usize)
                    .unwrap_or(0),
            );
            let _ = r;
            node.save_delta(delta);

            if node.delta_len() >= CONSOLIDATE_THRESHOLD {
                (node, _) = node.compact(&mut a, 3, None, &mut |_| {}, None);
            }
        }

        // this will mix siblings and new keys
        for i in 30..31 {
            let raw = format!("key_{i}");
            let k = Key::new(raw.as_bytes(), Ver::new(txid.fetch_add(1, Relaxed), 0));
            let v = Record::normal(1, raw.as_bytes());
            let (delta, r) = DeltaView::from_key_val(&mut a, &k, &v, 0, lsn);
            node.insert_inplace(
                delta.view().as_delta(),
                r.as_ref()
                    .map(|x| x.header().total_size as usize)
                    .unwrap_or(0),
            );
            let _ = r;
            node.save_delta(delta);

            if node.delta_len() >= CONSOLIDATE_THRESHOLD {
                (node, _) = node.compact(&mut a, 3, None, &mut |_| {}, None);
            }
        }

        let mut last: Option<LeafSeg<'_>> = None;
        let mut iter = node.leaf_iter(&mut j, 3, BucketOptions::MIN_INLINE_SIZE, None, None);
        let mut scratch: Vec<u8> = Vec::new();
        use crate::types::node::LeafCompactSource;

        // make sure the iterator produce ascending sorted value
        while let Some((k, _)) = iter.next_compact(&mut scratch) {
            if let Some(old) = last {
                assert!(old.cmp(&k).is_lt());
            }
            last = Some(k);
        }
    }

    #[test]
    fn compact_drops_latest_tombstone_instead_of_revealing_older_value() {
        let mut a = A::new();
        let l = a.clone();
        let lsn = Position::MIN;
        let mut node = Node::new_leaf(&mut a, l, 0, lsn);
        let mut txid = 1_u64;
        let v1 = vec![b'x'; BucketOptions::MIN_INLINE_SIZE + 1024];
        let v2 = vec![b'y'; BucketOptions::MIN_INLINE_SIZE + 1024];

        for i in 0..64 {
            let raw = format!("blob_{i:04}");
            let key = Key::new(raw.as_bytes(), Ver::new(txid, 0));
            txid += 1;
            let value = Record::normal(1, &v1);
            let (delta, remote) = DeltaView::from_key_val(&mut a, &key, &value, 0, lsn);
            node.insert_inplace(
                delta.view().as_delta(),
                remote
                    .as_ref()
                    .map(|x| x.header().total_size as usize)
                    .unwrap_or(0),
            );
            let _ = remote;
            node.save_delta(delta);
        }
        (node, _) = node.compact(&mut a, txid, None, &mut |_| {}, None);

        for i in 0..64 {
            let raw = format!("blob_{i:04}");
            let key = Key::new(raw.as_bytes(), Ver::new(txid, 0));
            let value = Record::normal(1, &v2);
            let (delta, remote) = DeltaView::from_key_val(&mut a, &key, &value, 0, lsn);
            node.insert_inplace(
                delta.view().as_delta(),
                remote
                    .as_ref()
                    .map(|x| x.header().total_size as usize)
                    .unwrap_or(0),
            );
            let _ = remote;
            node.save_delta(delta);
            txid += 1;
        }

        for i in 0..64 {
            let raw = format!("blob_{i:04}");
            let key = Key::new(raw.as_bytes(), Ver::new(txid, 0));
            txid += 1;
            let value = Record::remove(1);
            let (delta, remote) = DeltaView::from_key_val(&mut a, &key, &value, 0, lsn);
            node.insert_inplace(
                delta.view().as_delta(),
                remote
                    .as_ref()
                    .map(|x| x.header().total_size as usize)
                    .unwrap_or(0),
            );
            let _ = remote;
            node.save_delta(delta);
        }

        let latest_txid = txid;
        (node, _) = node.compact(&mut a, latest_txid, None, &mut |_| {}, None);
        let mut junk = Junk::new();
        assert_eq!(
            drain_len(&mut node.leaf_iter(
                &mut junk,
                latest_txid,
                BucketOptions::MIN_INLINE_SIZE,
                None,
                None
            )),
            0
        );

        let probe = Key::new("blob_0000".as_bytes(), Ver::new(latest_txid, 0));
        assert!(node.find_latest(&probe).is_none());
    }

    fn drain_len<'a, L>(iter: &mut crate::types::node::LeafIter<'a, L>) -> usize
    where
        L: crate::types::traits::ILoader,
    {
        let mut scratch: Vec<u8> = Vec::new();
        use crate::types::node::LeafCompactSource;
        let mut n = 0;
        while iter.next_compact(&mut scratch).is_some() {
            n += 1;
        }
        n
    }

    /// a safe tombstone must shadow every older version, including merge operands
    #[test]
    fn safe_delete_shadows_stale_merges_across_compact() {
        struct AddOp;
        impl crate::MergeOperator for AddOp {
            fn combine_operands(&self, _key: &[u8], left: &[u8], right: &[u8]) -> Vec<u8> {
                let d = |b: &[u8]| u64::from_le_bytes(b.try_into().unwrap());
                (d(left) + d(right)).to_le_bytes().to_vec()
            }

            fn apply(&self, _key: &[u8], base: Option<&[u8]>, operand: &[u8]) -> Option<Vec<u8>> {
                let d = |b: &[u8]| u64::from_le_bytes(b.try_into().unwrap());
                let v = match base {
                    None => d(operand),
                    Some(b) => d(b) + d(operand),
                };
                Some(v.to_le_bytes().to_vec())
            }
        }

        fn push_version(a: &mut A, node: &Node<A>, txid: u64, gid: u8, data: &[u8], merge: bool) {
            let key = Key::new("k".as_bytes(), Ver::new(txid, 1));
            let rec = if merge {
                Record::merge(gid, data)
            } else {
                Record::normal(gid, data)
            };
            let (delta, remote) = DeltaView::from_key_val(a, &key, &rec, 0, Position::MIN);
            node.insert_inplace(
                delta.view().as_delta(),
                remote
                    .as_ref()
                    .map(|x| x.header().total_size as usize)
                    .unwrap_or(0),
            );
            node.save_delta(delta);
        }

        // chain: Merge@tx10, Merge@tx20, Put@tx30, Delete@tx40, safe = 40
        fn build(a: &mut A, node: &Node<A>) {
            push_version(a, node, 10, 1, &1u64.to_le_bytes(), true);
            push_version(a, node, 20, 1, &2u64.to_le_bytes(), true);
            push_version(a, node, 30, 0, &30u64.to_le_bytes(), false);
            push_version(a, node, 40, 0, b"", false);
        }

        fn rows(_a: &A, node: &Node<A>, op: Option<&dyn crate::MergeOperator>) -> usize {
            let mut junk = Junk::new();
            let mut iter = node.leaf_iter(&mut junk, 40, BucketOptions::MIN_INLINE_SIZE, op, None);
            let mut scratch: Vec<u8> = Vec::new();
            use crate::types::node::LeafCompactSource;
            let mut n = 0;
            while let Some((seg, _)) = iter.next_compact(&mut scratch) {
                if seg.raw() == b"k" {
                    n += 1;
                }
            }
            n
        }

        // without an operator the operands must not survive the tombstone
        let mut a = A::new();
        let l = a.clone();
        let node = Node::new_leaf(&mut a, l.clone(), 0, Position::MIN);
        build(&mut a, &node);
        let (node, _) = node.compact(&mut a, 40, None, &mut |_| {}, None);
        assert_eq!(
            rows(&l, &node, None),
            0,
            "no stale operand survives the tombstone"
        );

        // with an operator the fold must not resurrect a value from shadowed operands
        let mut a = A::new();
        let l = a.clone();
        let node = Node::new_leaf(&mut a, l.clone(), 0, Position::MIN);
        build(&mut a, &node);
        let (node, _) = node.compact(&mut a, 40, Some(&AddOp), &mut |_| {}, None);
        assert_eq!(
            rows(&l, &node, Some(&AddOp)),
            0,
            "no folded value resurrects the deleted key"
        );
    }

    /// compact with a materializing operator folds the contiguous merge run at the
    /// safe boundary into one base row carrying the boundary version's Ver and
    /// logical group (Base(10)@tx10, Merge(+1)@tx20, Merge(+2)@tx30, safe=25 -> Base(11)@tx20 + Merge(+2)@tx30)
    #[test]
    fn compact_fold_passes_full_prefixed_key_to_operator() {
        struct CaptureOp(Mutex<Option<Vec<u8>>>);
        impl crate::MergeOperator for CaptureOp {
            fn combine_operands(&self, key: &[u8], left: &[u8], right: &[u8]) -> Vec<u8> {
                *self.0.lock() = Some(key.to_vec());
                let d = |b: &[u8]| u64::from_le_bytes(b.try_into().unwrap());
                (d(left) + d(right)).to_le_bytes().to_vec()
            }

            fn apply(&self, key: &[u8], base: Option<&[u8]>, operand: &[u8]) -> Option<Vec<u8>> {
                *self.0.lock() = Some(key.to_vec());
                let d = |b: &[u8]| u64::from_le_bytes(b.try_into().unwrap());
                Some(
                    match base {
                        None => d(operand),
                        Some(b) => d(b) + d(operand),
                    }
                    .to_le_bytes()
                    .to_vec(),
                )
            }
        }

        fn gen_merge_val(a: &mut A, v: u64) -> Val<'static> {
            let data = v.to_le_bytes();
            let r = Record::merge(0, &data);
            let sz = Val::calc_size(false, a.inline_size(), r.packed_size());
            let mut b = a.alloc(sz as u32);
            Val::encode_inline(b.data_slice_mut::<u8>(), None, &r);
            Val::from_raw(unsafe {
                std::mem::transmute::<&[u8], &'static [u8]>(b.data_slice::<u8>())
            })
        }

        // a merge run on the FULL key "sum:a" inside a page whose prefix is
        // "sum:" (prefix_len = 4): the operator must receive the full key, not
        // the prefix-stripped tail "a"
        let mut a = A::new();
        let l = a.clone();
        let mut junks = Vec::new();
        let op = CaptureOp(Mutex::new(None));
        let mut filter = LeafFilter::new(25, &l, &mut junks, Some(&op), a.inline_size(), None);
        filter.run_ops.push((
            LeafSeg::new(b"sum:".as_slice(), b"a".as_slice(), Ver::new(20, 1)),
            gen_merge_val(&mut a, 1),
        ));
        filter.run_ops.push((
            LeafSeg::new(b"sum:".as_slice(), b"a".as_slice(), Ver::new(10, 1)),
            gen_merge_val(&mut a, 10),
        ));
        let mut synth = Vec::new();
        filter
            .fold_run(&op, None, &mut synth)
            .expect("fold must succeed");
        let seen = op.0.lock().clone().expect("operator must run");
        assert_eq!(seen, b"sum:a", "operator must receive the full user key");
    }

    #[test]
    fn compact_folds_merge_run_at_boundary_ver_and_group() {
        struct AddOp;
        impl crate::MergeOperator for AddOp {
            fn combine_operands(&self, _key: &[u8], left: &[u8], right: &[u8]) -> Vec<u8> {
                let d = |b: &[u8]| u64::from_le_bytes(b.try_into().unwrap());
                (d(left) + d(right)).to_le_bytes().to_vec()
            }

            fn apply(&self, _key: &[u8], base: Option<&[u8]>, operand: &[u8]) -> Option<Vec<u8>> {
                let d = |b: &[u8]| u64::from_le_bytes(b.try_into().unwrap());
                if d(operand) == 0 {
                    // empty apply value: an engine-detected contract violation
                    return Some(Vec::new());
                }
                let v = match base {
                    None => d(operand),
                    Some(b) => d(b) + d(operand),
                };
                Some(v.to_le_bytes().to_vec())
            }
        }

        fn push_version(a: &mut A, node: &Node<A>, txid: u64, gid: u8, data: &[u8], merge: bool) {
            let key = Key::new("k".as_bytes(), Ver::new(txid, 1));
            let rec = if merge {
                Record::merge(gid, data)
            } else {
                Record::normal(gid, data)
            };
            let (delta, remote) = DeltaView::from_key_val(a, &key, &rec, 0, Position::MIN);
            node.insert_inplace(
                delta.view().as_delta(),
                remote
                    .as_ref()
                    .map(|x| x.header().total_size as usize)
                    .unwrap_or(0),
            );
            node.save_delta(delta);
        }

        let mut a = A::new();
        let l = a.clone();
        let node = Node::new_leaf(&mut a, l.clone(), 0, Position::MIN);
        push_version(&mut a, &node, 10, 0, &10u64.to_le_bytes(), false);
        push_version(&mut a, &node, 20, 1, &1u64.to_le_bytes(), true);
        push_version(&mut a, &node, 30, 2, &2u64.to_le_bytes(), true);

        let (node, _) = node.compact(&mut a, 25, Some(&AddOp), &mut |_| {}, None);
        let mut junk = Junk::new();
        let mut iter = node.leaf_iter(
            &mut junk,
            25,
            BucketOptions::MIN_INLINE_SIZE,
            Some(&AddOp),
            None,
        );
        let mut scratch: Vec<u8> = Vec::new();
        use crate::types::node::LeafCompactSource;

        let first = iter
            .next_compact(&mut scratch)
            .expect("above-safe merge row");
        assert_eq!(first.0.ver.txid, 30, "above-safe merge keeps its own Ver");
        assert!(first.1.is_merge(), "above-safe merge stays an operand");

        let second = iter.next_compact(&mut scratch).expect("folded base row");
        assert_eq!(
            second.0.ver.txid, 20,
            "folded base carries the safe boundary version's Ver"
        );
        let (rec, _) = second.1.get_record(&l);
        assert!(!rec.is_merge(), "folded base is a plain value");
        assert_eq!(
            rec.group_id(),
            1,
            "folded base keeps the boundary version's group"
        );
        assert_eq!(rec.data(), 11u64.to_le_bytes(), "fold equals the chain sum");

        assert!(
            iter.next_compact(&mut scratch).is_none(),
            "the absorbed base is trimmed"
        );
    }

    /// a fold contract violation (empty/oversized output) or an undecodable
    /// barrier keeps every version of that key verbatim instead of dropping the
    /// absorbed operands
    #[test]
    fn compact_empty_fold_output_stays_raw_preserving() {
        struct EmptyOnZero;
        impl crate::MergeOperator for EmptyOnZero {
            fn combine_operands(&self, _key: &[u8], left: &[u8], right: &[u8]) -> Vec<u8> {
                let d = |b: &[u8]| u64::from_le_bytes(b.try_into().unwrap());
                if d(left) == 0 || d(right) == 0 {
                    // empty combine output: an engine-detected contract violation
                    return Vec::new();
                }
                (d(left) + d(right)).to_le_bytes().to_vec()
            }

            fn apply(&self, _key: &[u8], base: Option<&[u8]>, operand: &[u8]) -> Option<Vec<u8>> {
                let d = |b: &[u8]| u64::from_le_bytes(b.try_into().unwrap());
                if d(operand) == 0 {
                    // empty apply value: an engine-detected contract violation
                    return Some(Vec::new());
                }
                let v = match base {
                    None => d(operand),
                    Some(b) => d(b) + d(operand),
                };
                Some(v.to_le_bytes().to_vec())
            }
        }

        fn push_version(a: &mut A, node: &Node<A>, txid: u64, gid: u8, data: &[u8], merge: bool) {
            let key = Key::new("k".as_bytes(), Ver::new(txid, 1));
            let rec = if merge {
                Record::merge(gid, data)
            } else {
                Record::normal(gid, data)
            };
            let (delta, remote) = DeltaView::from_key_val(a, &key, &rec, 0, Position::MIN);
            node.insert_inplace(
                delta.view().as_delta(),
                remote
                    .as_ref()
                    .map(|x| x.header().total_size as usize)
                    .unwrap_or(0),
            );
            node.save_delta(delta);
        }

        let mut a = A::new();
        let l = a.clone();
        let node = Node::new_leaf(&mut a, l.clone(), 0, Position::MIN);
        // the zero operand makes the fold fail; every version must survive
        push_version(&mut a, &node, 10, 0, &10u64.to_le_bytes(), false);
        push_version(&mut a, &node, 20, 1, &0u64.to_le_bytes(), true);
        push_version(&mut a, &node, 30, 1, &2u64.to_le_bytes(), true);

        let (node, _) = node.compact(&mut a, 25, Some(&EmptyOnZero), &mut |_| {}, None);
        let mut junk = Junk::new();
        let mut iter = node.leaf_iter(
            &mut junk,
            25,
            BucketOptions::MIN_INLINE_SIZE,
            Some(&EmptyOnZero),
            None,
        );
        let mut scratch: Vec<u8> = Vec::new();
        use crate::types::node::LeafCompactSource;

        // all three versions survive the failed fold, newest first
        let first = iter
            .next_compact(&mut scratch)
            .expect("merge@tx30 survives");
        assert!(first.1.is_merge());
        let second = iter
            .next_compact(&mut scratch)
            .expect("merge@tx20 survives");
        assert!(second.1.is_merge());
        let third = iter.next_compact(&mut scratch).expect("base@tx10 survives");
        let (rec, _) = third.1.get_record(&l);
        assert!(!rec.is_merge());
        assert_eq!(rec.data(), 10u64.to_le_bytes());
        assert!(iter.next_compact(&mut scratch).is_none());
    }

    /// a tombstone closes the fold run: apply(None, combined) may produce a value
    /// or a tombstone-shaped row (DEL bit), never an empty plain value
    #[test]
    fn compact_folds_merge_run_onto_tombstone_barrier() {
        struct ApplyNoneAdds;
        impl crate::MergeOperator for ApplyNoneAdds {
            fn combine_operands(&self, _key: &[u8], left: &[u8], right: &[u8]) -> Vec<u8> {
                let d = |b: &[u8]| u64::from_le_bytes(b.try_into().unwrap());
                (d(left) + d(right)).to_le_bytes().to_vec()
            }

            fn apply(&self, _key: &[u8], base: Option<&[u8]>, operand: &[u8]) -> Option<Vec<u8>> {
                let d = |b: &[u8]| u64::from_le_bytes(b.try_into().unwrap());
                match base {
                    // deleting + operand keeps the key present
                    None => Some(d(operand).to_le_bytes().to_vec()),
                    Some(b) => Some((d(b) + d(operand)).to_le_bytes().to_vec()),
                }
            }
        }

        fn push_version(a: &mut A, node: &Node<A>, txid: u64, gid: u8, data: &[u8], merge: bool) {
            let key = Key::new("k".as_bytes(), Ver::new(txid, 1));
            let rec = if merge {
                Record::merge(gid, data)
            } else {
                Record::normal(gid, data)
            };
            let (delta, remote) = DeltaView::from_key_val(a, &key, &rec, 0, Position::MIN);
            node.insert_inplace(
                delta.view().as_delta(),
                remote
                    .as_ref()
                    .map(|x| x.header().total_size as usize)
                    .unwrap_or(0),
            );
            node.save_delta(delta);
        }

        let mut a = A::new();
        let l = a.clone();
        let node = Node::new_leaf(&mut a, l.clone(), 0, Position::MIN);
        // tombstone@tx10, Merge(+5)@tx20, Merge(+7)@tx30, safe=25
        push_version(&mut a, &node, 10, 0, b"", false);
        push_version(&mut a, &node, 20, 1, &5u64.to_le_bytes(), true);
        push_version(&mut a, &node, 30, 2, &7u64.to_le_bytes(), true);

        let (node, _) = node.compact(&mut a, 25, Some(&ApplyNoneAdds), &mut |_| {}, None);
        let mut junk = Junk::new();
        let mut iter = node.leaf_iter(
            &mut junk,
            25,
            BucketOptions::MIN_INLINE_SIZE,
            Some(&ApplyNoneAdds),
            None,
        );
        let mut scratch: Vec<u8> = Vec::new();
        use crate::types::node::LeafCompactSource;

        let first = iter.next_compact(&mut scratch).expect("above-safe merge");
        assert!(first.1.is_merge());
        let second = iter.next_compact(&mut scratch).expect("folded row");
        assert_eq!(second.0.ver.txid, 20, "boundary version's Ver");
        let (rec, _) = second.1.get_record(&l);
        assert_eq!(rec.group_id(), 1, "boundary version's group");
        assert_eq!(rec.data(), 5u64.to_le_bytes(), "apply(None, +5)");
        assert!(iter.next_compact(&mut scratch).is_none());
    }

    /// a raw-preserving chain behind a history pointer must be folded exactly once
    #[test]
    fn compact_folds_hist_backed_chain_exactly_once() {
        struct AddOp;
        impl crate::MergeOperator for AddOp {
            fn combine_operands(&self, _key: &[u8], left: &[u8], right: &[u8]) -> Vec<u8> {
                let d = |b: &[u8]| u64::from_le_bytes(b.try_into().unwrap());
                (d(left) + d(right)).to_le_bytes().to_vec()
            }

            fn apply(&self, _key: &[u8], base: Option<&[u8]>, operand: &[u8]) -> Option<Vec<u8>> {
                let d = |b: &[u8]| u64::from_le_bytes(b.try_into().unwrap());
                if d(operand) == 0 {
                    // empty apply value: an engine-detected contract violation
                    return Some(Vec::new());
                }
                let v = match base {
                    None => d(operand),
                    Some(b) => d(b) + d(operand),
                };
                Some(v.to_le_bytes().to_vec())
            }
        }

        fn push_version(a: &mut A, node: &Node<A>, txid: u64, gid: u8, data: &[u8], merge: bool) {
            let key = Key::new("k".as_bytes(), Ver::new(txid, 1));
            let rec = if merge {
                Record::merge(gid, data)
            } else {
                Record::normal(gid, data)
            };
            let (delta, remote) = DeltaView::from_key_val(a, &key, &rec, 0, Position::MIN);
            node.insert_inplace(
                delta.view().as_delta(),
                remote
                    .as_ref()
                    .map(|x| x.header().total_size as usize)
                    .unwrap_or(0),
            );
            node.save_delta(delta);
        }

        fn rows(a: &A, node: &Node<A>) -> Vec<(u64, bool, u64)> {
            let mut junk = Junk::new();
            let mut iter = node.leaf_iter(
                &mut junk,
                25,
                BucketOptions::MIN_INLINE_SIZE,
                Some(&AddOp),
                None,
            );
            let mut scratch: Vec<u8> = Vec::new();
            use crate::types::node::LeafCompactSource;
            let mut out = Vec::new();
            while let Some((seg, cv)) = iter.next_compact(&mut scratch) {
                let (rec, _) = cv.get_record(a);
                out.push((
                    seg.ver.txid,
                    rec.is_merge(),
                    u64::from_le_bytes(rec.data().try_into().unwrap()),
                ));
            }
            out
        }

        // probe 1: base10@tx10, m+1@tx15, m+2@tx20 -> region [m15, base10] behind m20
        let mut a = A::new();
        let l = a.clone();
        let node = Node::new_leaf(&mut a, l.clone(), 0, Position::MIN);
        push_version(&mut a, &node, 10, 0, &10u64.to_le_bytes(), false);
        push_version(&mut a, &node, 15, 1, &1u64.to_le_bytes(), true);
        push_version(&mut a, &node, 20, 1, &2u64.to_le_bytes(), true);
        // raw-preserving consolidation regions the chain
        let (node, _) = node.compact(&mut a, 25, None, &mut |_| {}, None);
        // materializing consolidation folds it exactly once
        let (node, _) = node.compact(&mut a, 25, Some(&AddOp), &mut |_| {}, None);
        assert_eq!(
            rows(&l, &node),
            vec![(20, false, 13)],
            "one folded row at the boundary Ver with the full chain sum"
        );

        // probe 2: a delta merge newer than the head joins the same fold run
        let mut a = A::new();
        let l = a.clone();
        let node = Node::new_leaf(&mut a, l.clone(), 0, Position::MIN);
        push_version(&mut a, &node, 10, 0, &10u64.to_le_bytes(), false);
        push_version(&mut a, &node, 15, 1, &1u64.to_le_bytes(), true);
        push_version(&mut a, &node, 20, 1, &2u64.to_le_bytes(), true);
        let (node, _) = node.compact(&mut a, 25, None, &mut |_| {}, None);
        push_version(&mut a, &node, 22, 2, &5u64.to_le_bytes(), true);
        let (node, _) = node.compact(&mut a, 25, Some(&AddOp), &mut |_| {}, None);
        assert_eq!(
            rows(&l, &node),
            vec![(22, false, 18)],
            "delta merge above the head folds in; boundary Ver is the newest run member"
        );
    }

    /// a blob-backed folded history row must not carry a dangling address
    #[test]
    fn blob_fold_as_history_sibling_materializes_remote() {
        struct ConcatOp;
        impl crate::MergeOperator for ConcatOp {
            fn combine_operands(&self, _key: &[u8], left: &[u8], right: &[u8]) -> Vec<u8> {
                let mut out = left.to_vec();
                out.extend_from_slice(right);
                out
            }

            fn apply(&self, _key: &[u8], base: Option<&[u8]>, operand: &[u8]) -> Option<Vec<u8>> {
                let mut out = match base {
                    None => Vec::new(),
                    Some(b) => b.to_vec(),
                };
                out.extend_from_slice(operand);
                Some(out)
            }
        }

        fn push_version(a: &mut A, node: &Node<A>, txid: u64, gid: u8, data: &[u8], merge: bool) {
            let key = Key::new("k".as_bytes(), Ver::new(txid, 1));
            let rec = if merge {
                Record::merge(gid, data)
            } else {
                Record::normal(gid, data)
            };
            let (delta, remote) = DeltaView::from_key_val(a, &key, &rec, 0, Position::MIN);
            node.insert_inplace(
                delta.view().as_delta(),
                remote
                    .as_ref()
                    .map(|x| x.header().total_size as usize)
                    .unwrap_or(0),
            );
            node.save_delta(delta);
        }

        let mut a = A::new();
        let l = a.clone();
        let node = Node::new_leaf(&mut a, l.clone(), 0, Position::MIN);
        // base 3000B @ tx10, merge 1500B @ tx20 -> fold 4500B > 4096 inline -> blob;
        // merge @ tx30 sits above safe=25 and stays as the head with the folded
        // blob row behind it as a history sibling
        push_version(&mut a, &node, 10, 0, &vec![b'a'; 3000], false);
        push_version(&mut a, &node, 20, 1, &vec![b'b'; 1500], true);
        push_version(&mut a, &node, 30, 1, &[b'c'; 100], true);

        let (node, _) = node.compact(&mut a, 25, Some(&ConcatOp), &mut |_| {}, None);
        let mut junk = Junk::new();
        let mut iter = node.leaf_iter(
            &mut junk,
            25,
            BucketOptions::MIN_INLINE_SIZE,
            Some(&ConcatOp),
            None,
        );
        let mut scratch: Vec<u8> = Vec::new();
        use crate::types::node::LeafCompactSource;

        let mut rows = Vec::new();
        while let Some((seg, cv)) = iter.next_compact(&mut scratch) {
            let (rec, _) = cv.get_record(&l);
            rows.push((
                seg.ver.txid,
                cv.is_merge(),
                cv.get_remote(),
                rec.data().len(),
            ));
        }
        // merge@tx30 head + the blob-folded sibling; no dangling address anywhere
        assert_eq!(rows.len(), 2, "head plus one folded blob sibling");
        assert!(rows[0].1, "head stays an above-safe merge");
        let folded = rows
            .iter()
            .find(|(_, is_merge, _, _)| !is_merge)
            .expect("folded row");
        assert!(
            folded.2 != NULL_ADDR,
            "the blob sibling must carry a real remote address, got 0x{:x}",
            folded.2
        );
        assert_eq!(folded.3, 4500, "folded value survives the blob round trip");
    }

    /// a raw chain written inline under a wide threshold re-encodes into
    /// blob-backed rows when the threshold drops and consolidation regions
    /// the chain again: every rebuilt row (main payload and history sibling)
    /// must carry a materialized remote address, never a null envelope
    #[test]
    fn inline_history_rows_materialize_remotes_after_threshold_drop() {
        fn push_version(a: &mut A, node: &Node<A>, txid: u64, gid: u8, data: &[u8], merge: bool) {
            let key = Key::new("k".as_bytes(), Ver::new(txid, 1));
            let rec = if merge {
                Record::merge(gid, data)
            } else {
                Record::normal(gid, data)
            };
            let (delta, remote) = DeltaView::from_key_val(a, &key, &rec, 0, Position::MIN);
            node.insert_inplace(
                delta.view().as_delta(),
                remote
                    .as_ref()
                    .map(|x| x.header().total_size as usize)
                    .unwrap_or(0),
            );
            node.save_delta(delta);
        }

        let base = vec![b'a'; 3000];
        let operand = vec![b'b'; 3000];
        let mut a = A::new();
        let l = a.clone();
        // build the chain under a wide threshold: both 3000B rows are inline
        a.set_inline(4096);
        let node = Node::new_leaf(&mut a, l.clone(), 0, Position::MIN);
        push_version(&mut a, &node, 10, 0, &base, false);
        push_version(&mut a, &node, 15, 1, &operand, true);
        // raw-preserving consolidation regions the chain while the wide
        // threshold still applies (rows stay inline in the history pages)
        let (node, _) = node.compact(&mut a, 25, None, &mut |_| {}, None);
        // lower the threshold (a persisted bucket option change) and rebuild
        // again: the page-backed chain rows now classify past the boundary
        // and must materialize real blob boxes in the main pass and in the
        // history-sibling pass
        a.set_inline(BucketOptions::MIN_INLINE_SIZE);
        let (node, _) = node.compact(&mut a, 25, None, &mut |_| {}, None);

        let mut junk = Junk::new();
        let mut iter = node.leaf_iter(&mut junk, 25, BucketOptions::MIN_INLINE_SIZE, None, None);
        let mut scratch: Vec<u8> = Vec::new();
        use crate::types::node::LeafCompactSource;
        let mut rows = Vec::new();
        while let Some((seg, cv)) = iter.next_compact(&mut scratch) {
            let (rec, _) = cv.get_record(&l);
            rows.push((
                seg.ver.txid,
                cv.is_merge(),
                cv.get_remote(),
                rec.data().len(),
            ));
        }
        assert_eq!(rows.len(), 2, "raw chain survives both consolidations");
        for (_, is_merge, remote, len) in rows {
            assert!(
                remote != NULL_ADDR,
                "rebuilt row must carry a real remote address after the threshold drop"
            );
            assert_eq!(len, 3000, "row data survives the blob round trip");
            let _ = is_merge;
        }
    }

    /// the shared folded-size classification pins the inline/blob/too-large
    /// boundaries exactly (record length = gid byte + data)
    #[test]
    fn classify_fold_size_boundaries() {
        use crate::Options;
        use crate::types::node::{FoldClass, classify_fold_size};
        let inline = 64usize;
        // record_len = 1 + len; inline strictly below inline_size
        assert!(matches!(classify_fold_size(62, inline), FoldClass::Inline));
        // record_len == inline_size is the blob boundary (mirrors writer is_remote)
        assert!(matches!(classify_fold_size(63, inline), FoldClass::Blob));
        assert!(matches!(
            classify_fold_size(inline * 4, inline),
            FoldClass::Blob
        ));
        // up to and including MAX_KV_SIZE total record length is blob-able
        assert!(matches!(
            classify_fold_size(Options::MAX_KV_SIZE - 1, inline),
            FoldClass::Blob
        ));
        // one byte past MAX_KV_SIZE is too large
        assert!(matches!(
            classify_fold_size(Options::MAX_KV_SIZE, inline),
            FoldClass::TooLarge
        ));
    }

    /// a merges-only chain (no plain/tombstone base) folds with base None at the
    /// end of its buffered key; both key-change and walk-end cases use the same
    /// per-key processing path
    #[test]
    fn compact_folds_merges_only_chain_without_base() {
        struct AddOp;
        impl crate::MergeOperator for AddOp {
            fn combine_operands(&self, _key: &[u8], left: &[u8], right: &[u8]) -> Vec<u8> {
                let d = |b: &[u8]| u64::from_le_bytes(b.try_into().unwrap());
                (d(left) + d(right)).to_le_bytes().to_vec()
            }

            fn apply(&self, _key: &[u8], base: Option<&[u8]>, operand: &[u8]) -> Option<Vec<u8>> {
                let d = |b: &[u8]| u64::from_le_bytes(b.try_into().unwrap());
                if d(operand) == 0 {
                    // empty apply value: an engine-detected contract violation
                    return Some(Vec::new());
                }
                let v = match base {
                    None => d(operand),
                    Some(b) => d(b) + d(operand),
                };
                Some(v.to_le_bytes().to_vec())
            }
        }

        fn push_version(a: &mut A, node: &Node<A>, key: &[u8], txid: u64, gid: u8, data: &[u8]) {
            let key = Key::new(key, Ver::new(txid, 1));
            let rec = Record::merge(gid, data);
            let (delta, remote) = DeltaView::from_key_val(a, &key, &rec, 0, Position::MIN);
            node.insert_inplace(
                delta.view().as_delta(),
                remote
                    .as_ref()
                    .map(|x| x.header().total_size as usize)
                    .unwrap_or(0),
            );
            node.save_delta(delta);
        }

        fn rows(a: &A, node: &Node<A>) -> Vec<(u64, u64)> {
            let mut junk = Junk::new();
            let mut iter = node.leaf_iter(
                &mut junk,
                25,
                BucketOptions::MIN_INLINE_SIZE,
                Some(&AddOp),
                None,
            );
            let mut scratch: Vec<u8> = Vec::new();
            use crate::types::node::LeafCompactSource;
            let mut out = Vec::new();
            while let Some((seg, cv)) = iter.next_compact(&mut scratch) {
                let (rec, _) = cv.get_record(a);
                out.push((
                    seg.ver.txid,
                    u64::from_le_bytes(rec.data().try_into().unwrap()),
                ));
            }
            out
        }

        // first key: m+1@tx15, m+2@tx20 then a second key; the key buffer closes
        // on the key change with base None -> apply(None, 1+2) = 3 @ tx20 (boundary)
        let mut a = A::new();
        let l = a.clone();
        let node = Node::new_leaf(&mut a, l.clone(), 0, Position::MIN);
        push_version(&mut a, &node, b"a", 15, 1, &1u64.to_le_bytes());
        push_version(&mut a, &node, b"a", 20, 1, &2u64.to_le_bytes());
        push_version(&mut a, &node, b"b", 21, 1, &9u64.to_le_bytes());
        let (node, _) = node.compact(&mut a, 25, Some(&AddOp), &mut |_| {}, None);
        assert_eq!(
            rows(&l, &node),
            vec![(20, 3), (21, 9)],
            "key-change close folds with base None at the boundary Ver"
        );

        // last key: the run reaches the end of the walk and folds in the same path
        let mut a = A::new();
        let l = a.clone();
        let node = Node::new_leaf(&mut a, l.clone(), 0, Position::MIN);
        push_version(&mut a, &node, b"a", 21, 1, &9u64.to_le_bytes());
        push_version(&mut a, &node, b"k", 15, 1, &1u64.to_le_bytes());
        push_version(&mut a, &node, b"k", 20, 1, &2u64.to_le_bytes());
        let (node, _) = node.compact(&mut a, 25, Some(&AddOp), &mut |_| {}, None);
        assert_eq!(
            rows(&l, &node),
            vec![(21, 9), (20, 3)],
            "walk-end close folds with base None at the boundary Ver"
        );
    }

    /// a fold result beyond the inline boundary uses the blob path; results beyond
    /// MAX_KV_SIZE remain raw
    #[test]
    fn compact_folds_oversized_result_into_blob() {
        struct BigFold;
        impl crate::MergeOperator for BigFold {
            fn combine_operands(&self, _key: &[u8], left: &[u8], right: &[u8]) -> Vec<u8> {
                let mut out = left.to_vec();
                out.extend_from_slice(right);
                out
            }

            fn apply(&self, _key: &[u8], base: Option<&[u8]>, operand: &[u8]) -> Option<Vec<u8>> {
                let mut out = match base {
                    None => Vec::new(),
                    Some(b) => b.to_vec(),
                };
                out.extend_from_slice(operand);
                Some(out)
            }
        }

        fn push_version(a: &mut A, node: &Node<A>, txid: u64, gid: u8, data: &[u8], merge: bool) {
            let key = Key::new("k".as_bytes(), Ver::new(txid, 1));
            let rec = if merge {
                Record::merge(gid, data)
            } else {
                Record::normal(gid, data)
            };
            let (delta, remote) = DeltaView::from_key_val(a, &key, &rec, 0, Position::MIN);
            node.insert_inplace(
                delta.view().as_delta(),
                remote
                    .as_ref()
                    .map(|x| x.header().total_size as usize)
                    .unwrap_or(0),
            );
            node.save_delta(delta);
        }

        let mut a = A::new();
        let l = a.clone();
        let node = Node::new_leaf(&mut a, l.clone(), 0, Position::MIN);
        let big = vec![b'x'; BucketOptions::MIN_INLINE_SIZE];
        push_version(&mut a, &node, 10, 0, &big, false);
        push_version(&mut a, &node, 20, 1, &1u64.to_le_bytes(), true);

        let (node, _) = node.compact(&mut a, 25, Some(&BigFold), &mut |_| {}, None);
        let mut junk = Junk::new();
        let mut iter = node.leaf_iter(
            &mut junk,
            25,
            BucketOptions::MIN_INLINE_SIZE,
            Some(&BigFold),
            None,
        );
        let mut scratch: Vec<u8> = Vec::new();
        use crate::types::node::LeafCompactSource;

        // the oversized fold result materializes through the remote/blob path:
        // one blob-backed base row whose record re-reads through the loader
        let row = iter.next_compact(&mut scratch).expect("folded blob row");
        assert_eq!(row.0.ver.txid, 20, "boundary version's Ver");
        assert!(!row.1.is_merge());
        assert!(
            row.1.is_remote(),
            "oversized fold must persist as a blob row"
        );
        let (rec, _) = row.1.get_record(&l);
        let mut expect = big.clone();
        expect.extend_from_slice(&1u64.to_le_bytes());
        assert_eq!(
            rec.data(),
            expect.as_slice(),
            "folded value survives blob round trip"
        );
        assert!(iter.next_compact(&mut scratch).is_none());
    }

    /// a fold whose apply returns None must persist a DEL-bit tombstone row
    #[test]
    fn compact_fold_result_can_be_tombstone_shaped() {
        struct DeleteOnFold;
        impl crate::MergeOperator for DeleteOnFold {
            fn combine_operands(&self, _key: &[u8], left: &[u8], right: &[u8]) -> Vec<u8> {
                let d = |b: &[u8]| u64::from_le_bytes(b.try_into().unwrap());
                (d(left) + d(right)).to_le_bytes().to_vec()
            }

            fn apply(&self, _key: &[u8], base: Option<&[u8]>, operand: &[u8]) -> Option<Vec<u8>> {
                let d = |b: &[u8]| u64::from_le_bytes(b.try_into().unwrap());
                match base {
                    // folding onto an existing base deletes the key
                    Some(_) => None,
                    None => Some(d(operand).to_le_bytes().to_vec()),
                }
            }
        }

        fn push_version(a: &mut A, node: &Node<A>, txid: u64, gid: u8, data: &[u8], merge: bool) {
            let key = Key::new("k".as_bytes(), Ver::new(txid, 1));
            let rec = if merge {
                Record::merge(gid, data)
            } else {
                Record::normal(gid, data)
            };
            let (delta, remote) = DeltaView::from_key_val(a, &key, &rec, 0, Position::MIN);
            node.insert_inplace(
                delta.view().as_delta(),
                remote
                    .as_ref()
                    .map(|x| x.header().total_size as usize)
                    .unwrap_or(0),
            );
            node.save_delta(delta);
        }

        let mut a = A::new();
        let l = a.clone();
        let node = Node::new_leaf(&mut a, l.clone(), 0, Position::MIN);
        push_version(&mut a, &node, 10, 0, &10u64.to_le_bytes(), false);
        push_version(&mut a, &node, 20, 1, &1u64.to_le_bytes(), true);

        let (node, _) = node.compact(&mut a, 25, Some(&DeleteOnFold), &mut |_| {}, None);
        let mut junk = Junk::new();
        // iterate below the boundary row's txid so the below-safe tombstone trim
        // (correct for reads) does not hide the row we want to inspect
        let mut iter = node.leaf_iter(
            &mut junk,
            19,
            BucketOptions::MIN_INLINE_SIZE,
            Some(&DeleteOnFold),
            None,
        );
        let mut scratch: Vec<u8> = Vec::new();
        use crate::types::node::LeafCompactSource;

        let row = iter.next_compact(&mut scratch).expect("folded row");
        assert_eq!(row.0.ver.txid, 20, "boundary version's Ver");
        assert!(
            row.1.is_tombstone(),
            "apply -> None must persist a DEL-bit tombstone row"
        );
        assert!(!row.1.is_merge());
        let (rec, _) = row.1.get_record(&l);
        assert!(rec.data().is_empty(), "the re-read record is a tombstone");
        assert!(rec.data().is_empty());
        assert!(iter.next_compact(&mut scratch).is_none());
    }
}
