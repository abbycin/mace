use std::collections::VecDeque;

use crate::{
    OpCode,
    cc::data::Ver,
    utils::{
        bytes::ByteArray,
        extend_slice_lifetime, pack_id,
        traits::{ICodec, IKey, IPageIter, IVal, IValCodec},
        unpack_id,
    },
};

use super::{
    FrameRef, IAlloc, Key,
    data::{Sibling, Slot, Value},
    iter::{ItemIter, SliceIter},
    page::{
        DeltaType, LeafMergeIter, MAIN_HDR_LEN, MAIN_SLOT_LEN, MAX_PREFIX_LEN, MainPage,
        MainPageInner, MainSlotType, Meta, NodeType, VER_HDR_LEN, VER_SLOT_LEN, VerPage,
        VerPageInner, VerSlotType,
    },
};

pub(crate) const HINTS_CNT: usize = 16;
pub(crate) const HINTS_LEN: usize = 64; // a cacheline size

#[inline(always)]
pub(crate) fn key_to_hints(key: &[u8]) -> u32 {
    match key.len() {
        0 => 0,
        1 => (key[0] as u32) << 24,
        2 => (unsafe { key.as_ptr().cast::<u16>().read_unaligned().to_be() } as u32) << 16,
        3 => {
            ((unsafe { key.as_ptr().cast::<u16>().read_unaligned().to_be() } as u32) << 16)
                | ((key[2] as u32) << 8)
        }
        _ => unsafe { key.as_ptr().cast::<u32>().read_unaligned().to_be() },
    }
}

#[cold]
fn add_remote<K, V, A>(
    k: &K,
    v: &V,
    a: &mut A,
    total: usize,
    offset: usize,
    payload: &ByteArray,
) -> usize
where
    K: IKey,
    V: IVal,
    A: IAlloc,
{
    let mut x = a.allocate(total).unwrap();
    x.set_normal();
    let s = Slot::from_remote(x.addr());
    let slot_sz = s.packed_size();
    let hdr = payload.as_mut_slice::<u8>(offset, slot_sz);
    s.encode_to(hdr);
    let b = x.payload();

    let (kdst, vdst) = b
        .as_mut_slice::<u8>(0, b.len())
        .split_at_mut(k.packed_size());
    k.encode_to(kdst);
    v.encode_to(vdst);
    slot_sz
}

fn add_kv<K, V, A>(
    k: &K,
    v: &V,
    a: &mut A,
    offset: usize,
    payload: &ByteArray,
    limit: usize,
) -> usize
where
    K: IKey,
    V: IVal,
    A: IAlloc,
{
    let (ksz, vsz) = (k.packed_size(), v.packed_size());
    let mut total_sz = ksz + vsz;

    if total_sz < limit {
        let s = Slot::inline();
        let slot_sz = s.packed_size();
        let hdr = payload.as_mut_slice::<u8>(offset, slot_sz);
        s.encode_to(hdr);

        // we allocated slot_sz in builder, and we are not plus slot_sz into total_sz, so the
        // count is total_sz
        let (kdst, vdst) = payload
            .as_mut_slice::<u8>(offset + slot_sz, total_sz)
            .split_at_mut(ksz);
        k.encode_to(kdst);
        v.encode_to(vdst);

        total_sz += slot_sz;
    } else {
        total_sz = add_remote(k, v, a, total_sz, offset, payload);
    }

    total_sz
}

pub(crate) struct MainPageBuilder {
    slots: &'static mut [MainSlotType],
    payload: ByteArray,
    hints: &'static mut [u32],
    prefix_len: usize,
    elems: usize,
    index: usize,
    offset: u32,
}

impl MainPageBuilder {
    const HDR_LEN: usize = size_of::<MainPageInner>();

    pub(crate) fn from(b: ByteArray, prefix: &[u8], elems: usize) -> Self {
        let slot_start = Self::HDR_LEN + HINTS_LEN;
        let data_start = elems * MAIN_SLOT_LEN + slot_start + prefix.len();
        if !prefix.is_empty() {
            let dist = b.as_mut_slice(data_start - prefix.len(), prefix.len());
            dist.copy_from_slice(prefix);
        }
        Self {
            slots: b.as_mut_slice(slot_start, elems),
            payload: b,
            hints: b.as_mut_slice(Self::HDR_LEN, HINTS_CNT),
            prefix_len: prefix.len(),
            elems,
            index: 0,
            offset: data_start as u32,
        }
    }

    pub(crate) fn add<K, V, A>(&mut self, k: &K, v: &V, limit: usize, a: &mut A)
    where
        K: IKey,
        V: IVal,
        A: IAlloc,
    {
        let size = add_kv(k, v, a, self.offset as usize, &self.payload, limit);

        self.slots[self.index] = pack_id(self.offset, key_to_hints(&k.raw()[self.prefix_len..]));
        self.index += 1;
        self.offset += size as u32;
    }

    pub(crate) fn finish(&mut self) {
        let step = self.elems / (HINTS_CNT + 1);
        let cnt = self.elems.min(HINTS_CNT);
        for i in 0..cnt {
            let (_, hint) = unpack_id(self.slots[(i + 1) * step]);
            self.hints[i] = hint;
        }
    }
}

pub(crate) struct VerPageBuilder {
    slots: &'static mut [VerSlotType],
    payload: ByteArray,
    index: usize,
    offset: u32,
}

impl VerPageBuilder {
    pub(crate) fn from(b: ByteArray, elems: usize) -> Self {
        Self {
            slots: b.as_mut_slice(VER_HDR_LEN, elems),
            payload: b,
            index: 0,
            offset: (VER_HDR_LEN + elems * VER_SLOT_LEN) as u32,
        }
    }

    pub(crate) fn add<K, V, A>(&mut self, k: &K, v: &V, limit: usize, a: &mut A)
    where
        K: IKey,
        V: IVal,
        A: IAlloc,
    {
        let size = add_kv(k, v, a, self.offset as usize, &self.payload, limit);

        self.slots[self.index] = self.offset;
        self.index += 1;
        self.offset += size as u32;
    }
}

fn calc_prefix_len(lhs: &[u8], rhs: &[u8]) -> usize {
    let len = lhs
        .iter()
        .zip(rhs.iter())
        .take_while(|(x, y)| x.eq(y))
        .count();
    if len > MAX_PREFIX_LEN { 0 } else { len }
}

pub(crate) struct Delta<T> {
    kind: DeltaType,
    class: NodeType,
    elems: u16,
    size: u32,
    prefix_len: usize,
    iter: Option<T>,
}

impl<T, K, V> Delta<T>
where
    T: IPageIter<Item = (K, V)>,
    K: IKey,
    V: IVal,
{
    pub(crate) fn new(kind: DeltaType, class: NodeType) -> Self {
        Self {
            kind,
            class,
            elems: 0,
            size: 0,
            prefix_len: 0,
            iter: None,
        }
    }

    /// NOTE: the iter must be ordered
    pub(crate) fn from(mut self, mut iter: T, limit_sz: u32) -> Self {
        let first_kv = iter.first();
        let mut last = None;
        for (k, v) in &mut iter {
            // it's safe here, the memory was held by at least one container so that it can't be
            // dangling
            last = Some(extend_slice_lifetime(k.raw()));
            self.elems += 1;
            let kv_sz = (k.packed_size() + v.packed_size()) as u32;
            if kv_sz > limit_sz {
                self.size += Slot::REMOTE_LEN as u32;
            } else {
                self.size += kv_sz + Slot::LOCAL_LEN as u32;
            }
        }

        // prefix is only used when there's more than 1 enrty in page, while the
        // hints is always used
        if self.elems > 1 {
            let (k, _) = first_kv.unwrap();
            self.prefix_len = calc_prefix_len(k.raw(), last.unwrap());
        }
        self.size += (self.elems as usize * MAIN_SLOT_LEN + HINTS_LEN + self.prefix_len) as u32;
        self.iter = Some(iter);
        self
    }

    pub(crate) fn size(&self) -> usize {
        MAIN_HDR_LEN + self.size as usize
    }

    fn build_header(&self) -> MainPageInner {
        let mut h = MainPageInner {
            meta: Meta::new(self.kind, self.class),
            link: 0,
            elems: self.elems as u32,
            size: self.size() as u32,
            prefix_len: self.prefix_len,
        };
        h.set_depth(1);

        h
    }

    pub(crate) fn build<A>(&mut self, page: ByteArray, a: &mut A) -> &mut MainPageInner
    where
        A: IAlloc,
    {
        let limit = a.limit_size() as usize;
        let mut pg = MainPage::<K, V>::new(page, self.build_header());
        let prefix_len = pg.prefix_len;

        if let Some(iter) = self.iter.as_mut() {
            iter.rewind();
            let prefix = if prefix_len != 0 {
                let (k, _) = iter.first().unwrap();
                extend_slice_lifetime(&k.raw()[..prefix_len])
            } else {
                [].as_slice()
            };
            let mut builder = MainPageBuilder::from(page, prefix, self.elems as usize);
            for (k, v) in iter {
                builder.add(&k, &v, limit, a);
            }
            builder.finish();
        }
        pg.inner_mut()
    }
}

impl<K, V> Delta<ItemIter<(K, V)>>
where
    K: IKey,
    V: IVal,
{
    pub fn with_item(self, pg_sz: u32, item: (K, V)) -> Self {
        self.from(ItemIter::new(item), pg_sz)
    }
}

impl<'a, K, V> Delta<SliceIter<'a, K, V>>
where
    K: IKey,
    V: IVal,
{
    pub fn with_slice(self, pg_sz: u32, s: &'a [(K, V)]) -> Self {
        self.from(SliceIter::new(s), pg_sz)
    }
}

pub(crate) struct FuseBuilder<'a, T: IValCodec> {
    header: MainPageInner,
    iter: Option<LeafMergeIter<'a, T>>,
    hints: VecDeque<(u32, u32)>,
}

impl<'a, T> FuseBuilder<'a, T>
where
    T: IValCodec + 'a,
{
    pub(crate) fn new(kind: DeltaType, class: NodeType) -> Self {
        let mut h = MainPageInner {
            meta: Meta::new(kind, class),
            link: 0,
            elems: 0,
            size: MAIN_HDR_LEN as u32,
            prefix_len: 0,
        };
        h.set_depth(1);

        Self {
            header: h,
            iter: None,
            hints: VecDeque::new(),
        }
    }

    pub(crate) fn prepare<A: IAlloc>(&mut self, mut iter: LeafMergeIter<'a, T>, a: &A) {
        let mut fixed = false;
        self.hints.reserve(iter.len() * 3 / 5);
        let mut last_raw = None;
        let mut pos = 0;
        let limit = a.limit_size();
        let mut first_raw = None;

        for (k, v) in &mut iter {
            if let Some(raw) = last_raw {
                if raw == k.raw {
                    if !fixed {
                        fixed = true;
                        self.header.size += Sibling::<T>::ADDR_LEN as u32; // plus extra addr size
                        self.hints.push_back((pos - 1, 0));
                    }
                    let (_, cnt) = self.hints.back_mut().unwrap();
                    *cnt += 1;
                    pos += 1;
                    continue;
                }
            }

            let kv_sz = (k.packed_size() + v.packed_size()) as u32;
            pos += 1;
            if first_raw.is_none() {
                first_raw = Some(k.raw);
            }
            last_raw = Some(k.raw);
            fixed = false;
            if kv_sz > limit {
                self.header.size += Slot::REMOTE_LEN as u32;
            } else {
                self.header.size += Slot::LOCAL_LEN as u32;
                self.header.size += kv_sz;
            }
            self.header.elems += 1;
        }

        if self.header.elems > 1 {
            self.header.prefix_len = calc_prefix_len(first_raw.unwrap(), last_raw.unwrap());
        }

        self.header.size += (self.header.elems as usize * MAIN_SLOT_LEN
            + HINTS_LEN
            + self.header.prefix_len) as u32;
        iter.rewind();
        self.iter = Some(iter);
    }

    pub(crate) fn build<A: IAlloc>(
        &mut self,
        a: &mut A,
    ) -> Result<(MainPage<Key, Value<T>>, FrameRef), OpCode> {
        let mut iter = self.iter.take().unwrap();
        let b = a.allocate(self.header.size as usize)?;
        let raw = b.payload();
        let page = MainPage::<Key, Value<T>>::new(raw, self.header);
        let prefix = if page.prefix_len != 0 {
            let (k, _) = iter.first().unwrap();
            extend_slice_lifetime(&k.raw()[..page.prefix_len])
        } else {
            [].as_slice()
        };
        let mut builder = MainPageBuilder::from(raw, prefix, self.header.elems as usize);

        let mut pos = 0;
        let limit = a.limit_size() as usize;

        loop {
            let Some((k, v)) = iter.next() else {
                break;
            };

            let Some((idx, _)) = self.hints.front() else {
                builder.add(k, v, limit, a);
                pos += 1;
                continue;
            };

            if pos == *idx {
                let (_, cnt) = self.hints.pop_front().unwrap();
                let addr = self.save_versions(a, cnt as usize, &mut pos, &mut iter)?;

                builder.add(k, &Value::Sib(Sibling::from(addr, v)), limit, a);
            } else {
                builder.add(k, v, limit, a);
            }
            pos += 1;
        }
        builder.finish();

        Ok((page, b))
    }

    fn save_versions<A: IAlloc>(
        &mut self,
        a: &mut A,
        mut cnt: usize,
        pos: &mut u32,
        iter: &mut LeafMergeIter<'a, T>,
    ) -> Result<u64, OpCode>
    where
        T: IValCodec,
    {
        let pg_sz = a.page_size() as usize;
        let limit = a.limit_size() as usize;
        let mut head = None;
        let mut tail: Option<VerPage<T>> = None;
        let mut beg = iter.curr_pos();

        while cnt > 0 {
            let saved = beg;
            let mut len = VER_HDR_LEN;
            while cnt > 0 {
                let (_, v) = iter.next().expect("it's always valid here");
                let vz = v.packed_size();
                let vlen = if vz > limit {
                    Slot::REMOTE_LEN
                } else {
                    vz + Slot::LOCAL_LEN
                };
                let tmp = len + (Ver::len() + vlen) + VER_SLOT_LEN;
                if tmp > pg_sz {
                    break;
                }
                len = tmp;
                beg += 1;
                cnt -= 1;
            }
            iter.seek_to(saved);

            let mut b = a.allocate(len)?;
            b.set_normal();

            let addr = b.addr();
            let data = b.payload();
            let pg = VerPage::<T>::new(
                data,
                VerPageInner {
                    link: 0,
                    elems: (beg - saved) as u32,
                    size: data.len() as u32,
                },
            );

            if head.is_none() {
                head = Some(addr);
            }

            let mut builder = VerPageBuilder::from(data, pg.elems as usize);

            if let Some(last) = tail.as_mut() {
                last.link = addr;
            }
            tail = Some(pg);

            for _ in saved..beg {
                let (k, v) = iter.next().expect("it's always valid here");
                builder.add(k.ver(), v, limit, a);
                *pos += 1;
            }
        }
        Ok(head.unwrap())
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use crate::{
        index::{
            FrameRef, IAlloc, Key,
            data::Value,
            page::{DeltaType, LeafMergeIter, MainPage, NodeType},
        },
        map::data::FrameOwner,
        utils::traits::IDataLoader,
    };

    use super::FuseBuilder;

    struct Arena {
        map: HashMap<u64, FrameOwner>,
        page_size: u32,
    }

    impl Arena {
        fn new(page_size: u32) -> Self {
            Self {
                map: HashMap::new(),
                page_size,
            }
        }

        fn alloc(&mut self, size: usize) -> FrameRef {
            let b = FrameOwner::alloc(size);
            let addr = b.data().data() as u64;
            let copy = b.as_ref();
            self.map.insert(addr, b);
            copy
        }
    }

    impl IAlloc for Arena {
        fn allocate(&mut self, size: usize) -> Result<FrameRef, crate::OpCode> {
            Ok(self.alloc(size))
        }

        fn page_size(&self) -> u32 {
            self.page_size
        }

        fn limit_size(&self) -> u32 {
            self.page_size / 2
        }
    }

    #[derive(Clone)]
    struct DummyLoader;

    impl IDataLoader for DummyLoader {
        type Out = FrameOwner;
        fn load_data(&self, _addr: u64) -> Self::Out {
            unimplemented!()
        }
    }

    #[test]
    fn fuse_builder() {
        let mut iter = LeafMergeIter::new(12);
        iter.add(Key::new("1".as_bytes(), 9, 0), Value::Put("11".as_bytes()));
        iter.add(Key::new("1".as_bytes(), 8, 0), Value::Put("12".as_bytes()));
        iter.add(Key::new("1".as_bytes(), 7, 0), Value::Put("13".as_bytes()));
        iter.add(Key::new("2".as_bytes(), 1, 0), Value::Put("21".as_bytes()));
        iter.add(Key::new("3".as_bytes(), 2, 0), Value::Put("31".as_bytes()));
        iter.add(Key::new("3".as_bytes(), 1, 0), Value::Put("31".as_bytes()));
        iter.add(Key::new("4".as_bytes(), 2, 0), Value::Put("41".as_bytes()));
        iter.add(Key::new("4".as_bytes(), 1, 0), Value::Put("42".as_bytes()));
        iter.add(Key::new("5".as_bytes(), 5, 0), Value::Put("51".as_bytes()));
        iter.add(Key::new("5".as_bytes(), 3, 0), Value::Put("52".as_bytes()));
        iter.add(Key::new("5".as_bytes(), 2, 0), Value::Put("53".as_bytes()));
        iter.add(Key::new("6".as_bytes(), 1, 0), Value::Put("61".as_bytes()));

        let mut b = FuseBuilder::new(DeltaType::Data, NodeType::Leaf);
        let mut arena = Arena::new(128);

        iter.sort();

        b.prepare(iter, &arena);
        let l = DummyLoader;

        let (_, f) = b.build(&mut arena).unwrap();

        let (addr, page) = (f.addr(), MainPage::<Key, Value<&[u8]>>::from(f.payload()));
        page.show(&l, addr);
    }
}
