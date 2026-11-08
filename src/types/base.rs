use crate::hot_true;
use std::{cell::Cell, collections::VecDeque, ptr::null_mut};

use crate::{
    BucketOptions, must_exist,
    types::{
        data::{HistRef, Index, IntlKey, IntlSeg, Key, LeafSeg, Record, Val, Ver},
        header::{BaseHeader, NodeType, SLOT_LEN, SlotType, TagFlag, TagKind},
        node::{CompactVal, LeafCompactSource, PlainValSource},
        refbox::{BaseView, BoxRef},
        sst::Sst,
        traits::{IBoxHeader, ICodec, IFrameAlloc, IHeader, IKey, IKeyCodec, ILoader},
    },
    utils::{NULL_ADDR, NULL_PID, OpCode, data::Position},
};

use self::iter::FuseBaseIter;

impl BaseView {
    pub(super) const HDR_LEN: usize = size_of::<BaseHeader>();

    pub(crate) fn null() -> Self {
        Self(null_mut())
    }

    pub(crate) fn is_null(&self) -> bool {
        self.0.is_null()
    }

    pub(crate) fn calc_prefix(lo: &[u8], hi: &Option<&[u8]>) -> usize {
        if let Some(hi) = hi {
            lo.iter()
                .zip(hi.iter())
                .take_while(|&(x, y)| x == y)
                .count()
        } else {
            0
        }
    }

    pub(crate) fn merge<A: IFrameAlloc, L: ILoader>(
        &self,
        a: &mut A,
        loader: &L,
        other: Self,
        txid: u64,
        group: u8,
        lsn: Position,
    ) -> BoxRef {
        let hdr1 = self.header();
        let hdr2 = other.header();
        hot_true!(eq hdr1.is_index, hdr2.is_index);

        let lo1 = self.lo();
        let lo2 = other.lo();
        let hi = other.hi();
        let sibling = hdr2.right_sibling;
        let elems1 = hdr1.elems as usize;
        let elems2 = hdr2.elems as usize;
        let pl1 = hdr1.prefix_len as usize;
        let pl2 = hdr2.prefix_len as usize;

        if hdr1.is_index {
            let f = || {
                let l = self.range_iter::<L, IntlKey>(loader, 0, elems1);
                let r = other.range_iter::<L, IntlKey>(loader, 0, elems2);
                FuseBaseIter::new(&lo1[..pl1], &lo2[..pl2], l, r)
            };
            Self::new_intl(a, lo1, hi, sibling, f, txid, group, lsn)
        } else {
            let l = self.range_iter::<L, Key>(loader, 0, elems1);
            let r = other.range_iter::<L, Key>(loader, 0, elems2);
            let mut source = PlainValSource::new(FuseBaseIter::new(&lo1[..pl1], &lo2[..pl2], l, r));
            Self::new_leaf(a, loader, lo1, hi, sibling, &mut source, txid, group, lsn)
        }
    }

    /// the data means slot + hi/lo + key-value, excluding header and remotes
    pub(super) fn data_mut(&mut self) -> &mut [u8] {
        let h = self.header();
        unsafe {
            std::slice::from_raw_parts_mut(
                self.0.add(1).cast::<u8>(),
                h.size as usize - Self::HDR_LEN,
            )
        }
    }

    pub(super) fn data(&self) -> &[u8] {
        let h = self.header();
        unsafe {
            std::slice::from_raw_parts(self.0.add(1).cast::<u8>(), h.size as usize - Self::HDR_LEN)
        }
    }

    pub(crate) fn lo(&self) -> &[u8] {
        let h = self.header();
        if h.lo_len == 0 {
            return &[];
        }
        let off = h.elems as usize * SLOT_LEN;
        &self.data()[off..off + h.lo_len as usize]
    }

    pub(crate) fn hi(&self) -> Option<&[u8]> {
        let h = self.header();
        if h.hi_len == 0 {
            return None;
        }
        let off = h.elems as usize * SLOT_LEN;
        let sz = h.lo_len + h.hi_len;
        Some(&self.data()[off + h.lo_len as usize..off + sz as usize])
    }

    pub(crate) fn range_iter<'a, L, K>(
        &'a self,
        l: &'a L,
        beg: usize,
        end: usize,
    ) -> BaseIter<'a, L, K>
    where
        L: ILoader,
        K: IKey,
    {
        if self.is_null() {
            BaseIter::<L, K>::new(l, self.sst::<K>(), 0, 0)
        } else {
            BaseIter::<L, K>::new(l, self.sst::<K>(), beg, end)
        }
    }

    pub(crate) fn sst<K>(&self) -> Sst<K> {
        Sst::<K>::new(self.0)
    }
}

mod builder;
mod history;
mod iter;
mod layout;

pub(crate) use iter::{BaseIter, BaseRevIter};

#[cfg(test)]
mod test {
    use super::builder::SeekableIter;
    use crate::{
        types::{
            data::{Index, IntlKey, IntlSeg, Key, LeafSeg, Record, Val, Ver},
            header::{SLOT_LEN, TagKind},
            node::{CompactVal, PlainValSource},
            refbox::{BaseView, BoxRef, BoxView},
            traits::{IBoxHeader, ICodec, IFrameAlloc, IHeader, ILoader},
        },
        utils::{MutRef, NULL_ADDR, NULL_ORACLE, NULL_PID, data::Position, options::BucketOptions},
    };
    use std::{
        cell::Cell,
        collections::{HashMap, VecDeque},
    };

    struct Inner {
        off: Cell<u64>,
        map: HashMap<u64, BoxRef>,
    }

    #[derive(Clone)]
    struct Allocator {
        inner: MutRef<Inner>,
        inline_size: usize,
    }

    impl Allocator {
        fn new() -> Self {
            Self::with_inline(BucketOptions::MIN_INLINE_SIZE)
        }

        fn with_inline(inline_size: usize) -> Self {
            Self {
                inner: MutRef::new(Inner {
                    off: Cell::new(0),
                    map: HashMap::new(),
                }),
                inline_size,
            }
        }
    }

    impl IFrameAlloc for Allocator {
        fn alloc(&mut self, size: u32) -> BoxRef {
            let r = &mut self.inner;
            let old = r.off.get();
            r.off.set(old + size as u64);
            let b = BoxRef::alloc(size, old);
            r.map.insert(old, b.clone());
            b
        }

        fn inline_size(&self) -> usize {
            self.inline_size
        }
    }

    impl ILoader for Allocator {
        fn load_pinned(&self, addr: u64) -> BoxView {
            self.inner.map.get(&addr).unwrap().view()
        }

        fn pin(&self, data: BoxRef) {
            self.inner.raw_ref().map.insert(data.header().addr, data);
        }

        fn copy(&self) -> Self {
            self.clone()
        }

        fn copy_detached(&self) -> Self {
            self.clone()
        }

        fn load_sibling(&self, addr: u64) -> BoxRef {
            self.inner.map.get(&addr).unwrap().clone()
        }

        fn load_blob(&self, addr: u64, _cache: bool) -> BoxRef {
            self.inner.map.get(&addr).unwrap().clone()
        }
    }

    #[derive(Clone, Copy)]
    struct LeafData<'a> {
        data: &'a [(LeafSeg<'a>, Val<'a>)],
        pos: usize,
    }

    impl<'a> Iterator for LeafData<'a> {
        type Item = (LeafSeg<'a>, Val<'a>);
        fn next(&mut self) -> Option<Self::Item> {
            if self.pos == self.data.len() {
                return None;
            }
            let index = self.pos;
            self.pos += 1;
            let (k, v) = self.data[index];
            Some((k, v))
        }
    }

    fn gen_history_val(a: &mut Allocator, data: &[u8]) -> Val<'static> {
        let r = Record::normal(1, data);
        let sz = Val::calc_size(false, a.inline_size(), r.packed_size());
        let mut b = a.alloc(sz as u32);
        Val::encode_inline(b.data_slice_mut::<u8>(), None, &r);
        Val::from_raw(unsafe { std::mem::transmute::<&[u8], &[u8]>(b.data_slice::<u8>()) })
    }

    #[test]
    fn box_base() {
        let mut a = Allocator::new();
        let kv = [(IntlKey::new("mo".as_bytes()), Index::new(233))];
        let b = BaseView::new_intl(
            &mut a,
            "a".as_bytes(),
            None,
            NULL_PID,
            || kv.iter().map(|&(k, v)| (IntlSeg::new(&[], k.raw), v)),
            NULL_ORACLE,
            0,
            Position::MIN,
        );

        let th = b.header();
        assert_eq!(th.kind, TagKind::Base);

        let empty = LeafData { data: &[], pos: 0 };
        let l = a.clone();
        let b = BaseView::new_leaf(
            &mut a,
            &l,
            [].as_slice(),
            None,
            NULL_PID,
            &mut PlainValSource::new(empty),
            NULL_ORACLE,
            0,
            Position::MIN,
        );
        let th = b.header();
        assert_eq!(th.kind, TagKind::Base);
    }

    #[test]
    fn builder_leaf() {
        #[allow(clippy::too_many_arguments)]
        fn check(k: &Key, v: &Val, l: &Allocator, ks: &str, txid: u64, cmd: u32, vs: &str, w: u8) {
            assert_eq!(k.raw, ks.as_bytes());
            assert_eq!(k.txid, txid);
            assert_eq!(k.cmd, cmd);
            let (r, _) = v.get_record(l);
            assert_eq!(r.data(), vs.as_bytes());
            assert_eq!(r.group_id(), w);
        }

        fn gen_val(a: &mut Allocator, r: Record) -> Val<'static> {
            let sz = Val::calc_size(false, a.inline_size(), r.packed_size());
            let mut b = a.alloc(sz as u32);
            Val::encode_inline(b.data_slice_mut::<u8>(), None, &r);
            Val::from_raw(unsafe { std::mem::transmute::<&[u8], &[u8]>(b.data_slice::<u8>()) })
        }

        let mut a = Allocator::new();
        // NOTE: the kv should be ordered
        let kv = LeafData {
            data: &[
                (
                    LeafSeg::new(&[], "foo".as_bytes(), Ver::new(2, 1)),
                    gen_val(&mut a, Record::normal(1, "bar".as_bytes())),
                ),
                (
                    LeafSeg::new(&[], "foo".as_bytes(), Ver::new(1, 1)),
                    gen_val(&mut a, Record::normal(1, "foo".as_bytes())),
                ),
                (
                    LeafSeg::new(&[], "mo".as_bytes(), Ver::new(3, 1)),
                    gen_val(&mut a, Record::normal(1, "ha".as_bytes())),
                ),
            ],
            pos: 0,
        };
        let l = a.clone();

        let b = BaseView::new_leaf(
            &mut a,
            &l,
            &[],
            None,
            NULL_PID,
            &mut PlainValSource::new(kv),
            NULL_ORACLE,
            0,
            Position::MIN,
        );
        let base = b.view().as_base();
        let mut iter = base.range_iter::<Allocator, Key>(&l, 0, base.header().elems as usize);

        let (k, v) = iter.next().unwrap();

        check(&k, &v, &l, "foo", 2, 1, "bar", 1);

        let hist = v.get_hist().expect("must have history");
        assert!(hist.page_addr < b.header().addr);
        assert_eq!(hist.slot, 0);
        assert_eq!(hist.count, 1);

        iter.next(); // skip the expanded sibling

        let (k, v) = iter.next().unwrap();
        check(&k, &v, &l, "mo", 3, 1, "ha", 1);

        assert!(iter.next().is_none());
    }

    #[test]
    fn builder_intl() {
        macro_rules! test {
            ($l:expr, $r: expr, $k: expr, $v: expr) => {
                assert_eq!($l.raw, $k.as_bytes());
                assert_eq!($r.pid, $v);
            };
        }
        let kv = [
            (IntlKey::new("foo".as_bytes()), Index::new(1)),
            (IntlKey::new("bar".as_bytes()), Index::new(2)),
            (IntlKey::new("baz".as_bytes()), Index::new(3)),
        ];
        let mut a = Allocator::new();
        let l = a.clone();

        let b = BaseView::new_intl(
            &mut a,
            &[],
            None,
            NULL_PID,
            || kv.iter().map(|&(k, v)| (IntlSeg::new(&[], k.raw), v)),
            NULL_ORACLE,
            0,
            Position::MIN,
        );
        let base = b.view().as_base();
        let mut iter = base.range_iter::<Allocator, IntlKey>(&l, 0, base.header().elems as usize);

        let (k, v) = iter.next().unwrap();
        test!(k, v, "foo", 1);

        let (k, v) = iter.next().unwrap();
        test!(k, v, "bar", 2);

        let (k, v) = iter.next().unwrap();
        test!(k, v, "baz", 3);

        assert!(iter.next().is_none());
    }

    #[test]
    fn merge_leaf_with_different_prefix_lengths_keeps_distinct_raw_keys() {
        fn gen_val(a: &mut Allocator, r: Record) -> Val<'static> {
            let sz = Val::calc_size(false, a.inline_size(), r.packed_size());
            let mut b = a.alloc(sz as u32);
            Val::encode_inline(b.data_slice_mut::<u8>(), None, &r);
            Val::from_raw(unsafe { std::mem::transmute::<&[u8], &[u8]>(b.data_slice::<u8>()) })
        }

        let mut a = Allocator::new();
        let l = a.clone();

        let left_data = LeafData {
            data: &[(
                LeafSeg::new(&[], "aa1".as_bytes(), Ver::new(3, 1)),
                gen_val(&mut a, Record::normal(1, "left".as_bytes())),
            )],
            pos: 0,
        };
        let left = BaseView::new_leaf(
            &mut a,
            &l,
            "aa0".as_bytes(),
            Some("ab0".as_bytes()),
            NULL_PID,
            &mut PlainValSource::new(left_data),
            NULL_ORACLE,
            0,
            Position::MIN,
        );

        let right_data = LeafData {
            data: &[(
                LeafSeg::new(&[], "aba1".as_bytes(), Ver::new(2, 1)),
                gen_val(&mut a, Record::normal(1, "right".as_bytes())),
            )],
            pos: 0,
        };
        let right = BaseView::new_leaf(
            &mut a,
            &l,
            "ab0".as_bytes(),
            Some("abz".as_bytes()),
            NULL_PID,
            &mut PlainValSource::new(right_data),
            NULL_ORACLE,
            0,
            Position::MIN,
        );

        let merged = left.view().as_base().merge(
            &mut a,
            &l,
            right.view().as_base(),
            NULL_ORACLE,
            0,
            Position::MIN,
        );
        let base = merged.view().as_base();
        let mut iter = base.range_iter::<Allocator, Key>(&l, 0, base.header().elems as usize);

        let (k1, _) = iter.next().expect("must have first key");
        assert_eq!(k1.raw, "a1".as_bytes());
        let (k2, _) = iter.next().expect("must have second key");
        assert_eq!(k2.raw, "ba1".as_bytes());
        assert!(iter.next().is_none());
    }

    #[test]
    fn builder_leaf_history_region_can_cross_page() {
        #[allow(clippy::too_many_arguments)]
        fn check(k: &Key, v: &Val, l: &Allocator, ks: &str, txid: u64, cmd: u32, vs: &str, w: u8) {
            assert_eq!(k.raw, ks.as_bytes());
            assert_eq!(k.txid, txid);
            assert_eq!(k.cmd, cmd);
            let (r, _) = v.get_record(l);
            assert_eq!(r.data(), vs.as_bytes());
            assert_eq!(r.group_id(), w);
        }

        fn gen_val(a: &mut Allocator, r: Record) -> Val<'static> {
            let sz = Val::calc_size(false, a.inline_size(), r.packed_size());
            let mut b = a.alloc(sz as u32);
            Val::encode_inline(b.data_slice_mut::<u8>(), None, &r);
            Val::from_raw(unsafe { std::mem::transmute::<&[u8], &[u8]>(b.data_slice::<u8>()) })
        }

        let mut a = Allocator::new();
        let mut rows = Vec::new();
        rows.push((
            LeafSeg::new(&[], "foo".as_bytes(), Ver::new(20, 1)),
            gen_val(&mut a, Record::normal(1, "v20".as_bytes())),
        ));
        for txid in (1..20).rev() {
            let val = format!("v{txid}");
            rows.push((
                LeafSeg::new(&[], "foo".as_bytes(), Ver::new(txid, 1)),
                gen_val(&mut a, Record::normal(1, val.as_bytes())),
            ));
        }

        rows.push((
            LeafSeg::new(&[], "z".as_bytes(), Ver::new(30, 1)),
            gen_val(&mut a, Record::normal(1, "vz".as_bytes())),
        ));

        let kv = LeafData {
            data: rows.as_slice(),
            pos: 0,
        };
        let l = a.clone();
        let b = BaseView::new_leaf(
            &mut a,
            &l,
            &[],
            None,
            NULL_PID,
            &mut PlainValSource::new(kv),
            NULL_ORACLE,
            0,
            Position::MIN,
        );
        let base = b.view().as_base();
        let mut iter = base.range_iter::<Allocator, Key>(&l, 0, base.header().elems as usize);

        let (head_k, head_v) = iter.next().expect("must have head");
        check(&head_k, &head_v, &l, "foo", 20, 1, "v20", 1);
        let hist = head_v.get_hist().expect("must have history");
        assert_eq!(hist.slot, 0);
        assert_eq!(hist.count, 19);
        let head_page = l.load_sibling(hist.page_addr).view().as_base();
        assert_ne!(head_page.box_header().link, NULL_ADDR);

        for txid in (1..20).rev() {
            let (k, v) = iter.next().expect("must have old version");
            let expected = format!("v{txid}");
            check(&k, &v, &l, "foo", txid, 1, expected.as_str(), 1);
        }

        let (tail_k, tail_v) = iter.next().expect("must have tail key");
        check(&tail_k, &tail_v, &l, "z", 30, 1, "vz", 1);
        assert!(tail_v.get_hist().is_none());
        assert!(iter.next().is_none());
    }

    #[test]
    fn history_pages_pack_regions_and_bytes() {
        let mut a = Allocator::new();
        let mut iter = SeekableIter::new();
        iter.add(
            LeafSeg::new(&[], b"skip", Ver::new(0, 0)),
            CompactVal::Page(gen_history_val(&mut a, b"skip")),
        );
        let keys: Vec<Vec<u8>> = (1..=6u64).map(|i| i.to_le_bytes().to_vec()).collect();
        let mut record_sizes = Vec::new();
        for (i, key) in (1..=6u64).zip(keys.iter()) {
            let val = gen_history_val(&mut a, &[b'x'; 40]);
            record_sizes.push(
                Ver::len() + Val::calc_size(false, a.inline_size(), val.data_size()) + SLOT_LEN,
            );
            iter.add(
                LeafSeg::new(&[], key, Ver::new(i, 1)),
                CompactVal::Page(val),
            );
        }
        let hints = VecDeque::from([(0, 3), (3, 2), (5, 1)]);
        let mut hint_addrs = Vec::new();
        let mut remote_hints = Vec::new();
        let l = a.clone();
        let refs = BaseView::save_hist_regions(
            &mut a,
            &l,
            &hints,
            &iter,
            100,
            1,
            Position::MIN,
            &mut hint_addrs,
            &mut remote_hints,
        );
        assert_eq!(refs.len(), 3);
        assert_eq!(hint_addrs.len(), 2);
        assert!(remote_hints.is_empty());

        let mut page_elems = Vec::new();
        let mut record_at = 0;
        for &addr in &hint_addrs {
            let page = a.load_sibling(addr).view().as_base();
            let elems = page.header().elems as usize;
            let expected = BaseView::HDR_LEN
                + record_sizes[record_at..record_at + elems]
                    .iter()
                    .sum::<usize>();
            assert_eq!(page.header().size as usize, expected);
            page_elems.push(elems);
            record_at += elems;
        }

        let mut starts = Vec::new();
        let mut acc = 0;
        for elems in &page_elems {
            starts.push(acc);
            acc += elems;
        }
        assert_eq!(acc, 6);
        assert_eq!(page_elems, vec![3, 3]);

        let region_starts = [0usize, 3, 5];
        for (i, r) in refs.iter().enumerate() {
            let start = region_starts[i];
            let pid = starts.iter().rposition(|&s| s <= start).unwrap();
            assert_eq!(r.page_addr, hint_addrs[pid]);
            assert_eq!(r.slot as usize, start - starts[pid]);
            assert_eq!(r.count as usize, hints[i].1);
        }
        assert_eq!(
            refs.iter().map(|r| r.slot).collect::<Vec<_>>(),
            vec![0, 0, 2]
        );
        assert_eq!(
            refs.iter().map(|r| r.count).collect::<Vec<_>>(),
            vec![3, 2, 1]
        );
    }

    #[test]
    fn history_oversized_record_gets_its_own_page() {
        let mut a = Allocator::with_inline(4096);
        let mut iter = SeekableIter::new();
        iter.add(
            LeafSeg::new(&[], b"skip", Ver::new(0, 0)),
            CompactVal::Page(gen_history_val(&mut a, b"skip")),
        );
        let oversized = gen_history_val(&mut a, &[b'v'; 300]);
        let oversized_size = oversized.data_size();
        iter.add(
            LeafSeg::new(&[], b"k", Ver::new(1, 1)),
            CompactVal::Page(oversized),
        );
        let hints = VecDeque::from([(0, 1)]);
        let mut hint_addrs = Vec::new();
        let mut remote_hints = Vec::new();
        let l = a.clone();
        let refs = BaseView::save_hist_regions(
            &mut a,
            &l,
            &hints,
            &iter,
            100,
            1,
            Position::MIN,
            &mut hint_addrs,
            &mut remote_hints,
        );
        assert_eq!(refs.len(), 1);
        assert_eq!(hint_addrs.len(), 1);
        assert_eq!(refs[0].page_addr, hint_addrs[0]);
        assert_eq!(refs[0].slot, 0);
        assert_eq!(refs[0].count, 1);
        let page = a.load_sibling(hint_addrs[0]).view().as_base();
        assert_eq!(page.header().elems, 1);
        let expected = BaseView::HDR_LEN
            + Ver::len()
            + Val::calc_size(false, a.inline_size(), oversized_size)
            + SLOT_LEN;
        assert_eq!(page.header().size as usize, expected);
        assert!(
            page.header().size as usize > 256,
            "an oversized history record must exceed the test page budget"
        );
    }
}
