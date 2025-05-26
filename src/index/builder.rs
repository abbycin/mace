use std::{collections::VecDeque, marker::PhantomData};

use crate::{
    OpCode,
    cc::data::Ver,
    index::data::SLOT_LEN,
    utils::{
        bytes::ByteArray,
        traits::{ICodec, IKey, IPageIter, IVal, IValCodec},
    },
};

use super::{
    FrameRef, IAlloc, Key,
    data::{Sibling, Slot, Value},
    iter::{ItemIter, SliceIter},
    page::{
        self, DeltaType, LeafMergeIter, MainPage, MainPageHdr, Meta, NodeType, SIBPG_HDR_LEN,
        SibPage, SibPageHdr,
    },
};

pub(crate) struct PageBuilder<H> {
    payload: ByteArray,
    slots: ByteArray,
    index: usize,
    offset: u32,
    _marker: PhantomData<H>,
}

impl<H> PageBuilder<H> {
    const HDR_LEN: usize = size_of::<H>();

    pub(crate) fn from(b: ByteArray, elems: usize) -> Self {
        let slot_size = elems * SLOT_LEN + Self::HDR_LEN;
        Self {
            slots: b.sub_array(Self::HDR_LEN, slot_size - Self::HDR_LEN),
            payload: b,
            index: 0,
            offset: slot_size as u32,
            _marker: PhantomData,
        }
    }

    #[cold]
    fn add_remote<K, V, A: IAlloc>(&mut self, total: usize, k: &K, v: &V, a: &mut A) -> usize
    where
        K: IKey,
        V: IVal,
    {
        let mut x = a.allocate(total).unwrap();
        x.set_normal();
        let s = Slot::from_remote(x.addr());
        let slot_sz = s.packed_size();
        let hdr = self
            .payload
            .as_mut_slice::<u8>(self.offset as usize, slot_sz);
        s.encode_to(hdr);
        let b = x.payload();

        let (kdst, vdst) = b
            .as_mut_slice::<u8>(0, b.len())
            .split_at_mut(k.packed_size());
        k.encode_to(kdst);
        v.encode_to(vdst);
        slot_sz
    }

    pub(crate) fn add<K, V, A: IAlloc>(&mut self, k: &K, v: &V, limit: usize, a: &mut A)
    where
        K: IKey,
        V: IVal,
    {
        let (ksz, vsz) = (k.packed_size(), v.packed_size());
        let mut total_sz = ksz + vsz;

        if total_sz < limit {
            let s = Slot::inline();
            let slot_sz = s.packed_size();
            let hdr = self
                .payload
                .as_mut_slice::<u8>(self.offset as usize, slot_sz);
            s.encode_to(hdr);

            // we allocated slot_sz in builder, and we are not plus slot_sz into total_sz, so the
            // count is total_sz
            let (kdst, vdst) = self
                .payload
                .as_mut_slice::<u8>(self.offset as usize + slot_sz, total_sz)
                .split_at_mut(ksz);
            k.encode_to(kdst);
            v.encode_to(vdst);

            total_sz += slot_sz;
        } else {
            total_sz = self.add_remote(total_sz, k, v, a);
        }

        self.slots.write::<u32>(self.index * SLOT_LEN, self.offset);
        self.index += 1;
        self.offset += total_sz as u32;
    }
}

pub(crate) struct Delta<T> {
    kind: DeltaType,
    class: NodeType,
    elems: u16,
    size: u32,
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
            iter: None,
        }
    }

    pub(crate) fn from(mut self, mut iter: T, limit_sz: u32) -> Self {
        for (k, v) in &mut iter {
            self.elems += 1;
            let kv_sz = (k.packed_size() + v.packed_size()) as u32;
            if kv_sz > limit_sz {
                self.size += Slot::REMOTE_LEN as u32;
            } else {
                self.size += kv_sz + Slot::LOCAL_LEN as u32;
            }
        }
        self.size += self.elems as u32 * SLOT_LEN as u32;
        self.iter = Some(iter);
        self
    }

    pub(crate) fn size(&self) -> usize {
        size_of::<MainPageHdr>() + self.size as usize
    }

    fn build_header(&self, h: &mut MainPageHdr) {
        h.meta = Meta::new(self.kind, self.class);
        h.set_depth(1);
        h.set_link(0);
        h.len = self.size() as u32;
        h.elems = self.elems as u32;
    }

    pub(crate) fn build<A>(&mut self, page: ByteArray, a: &mut A) -> &mut MainPageHdr
    where
        A: IAlloc,
    {
        let limit = a.limit_size() as usize;
        let h = unsafe { &mut *page.data().cast::<MainPageHdr>() };
        self.build_header(h);

        if let Some(iter) = self.iter.as_mut() {
            let mut buf = PageBuilder::<MainPageHdr>::from(page, self.elems as usize);
            iter.rewind();
            for (k, v) in iter {
                buf.add(&k, &v, limit, a);
            }
        }
        h
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
    header: MainPageHdr,
    iter: Option<LeafMergeIter<'a, T>>,
    hints: VecDeque<(u32, u32)>,
}

impl<'a, T> FuseBuilder<'a, T>
where
    T: IValCodec + 'a,
{
    pub(crate) fn new(kind: DeltaType, class: NodeType) -> Self {
        let mut h = MainPageHdr::default();
        h.meta = Meta::new(kind, class);
        h.set_depth(1);
        h.set_link(0);
        h.len = page::MAINPG_HDR_LEN as u32;

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

        for (k, v) in &mut iter {
            if let Some(raw) = last_raw {
                if raw == k.raw {
                    if !fixed {
                        fixed = true;
                        self.header.len += Sibling::<T>::ADDR_LEN as u32; // plus extra addr size
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
            last_raw = Some(k.raw);
            fixed = false;
            if kv_sz > limit {
                self.header.len += Slot::REMOTE_LEN as u32;
            } else {
                self.header.len += Slot::LOCAL_LEN as u32;
                self.header.len += kv_sz;
            }
            self.header.elems += 1;
        }

        self.header.len += self.header.elems * SLOT_LEN as u32;
        iter.rewind();
        self.iter = Some(iter);
    }

    pub(crate) fn build<A: IAlloc>(
        &mut self,
        a: &mut A,
    ) -> Result<(MainPage<Key, Value<T>>, FrameRef), OpCode> {
        let b = a.allocate(self.header.len as usize)?;
        let mut page = MainPage::<Key, Value<T>>::from(b.payload());
        *page.header_mut() = self.header;
        let mut builder = PageBuilder::<MainPageHdr>::from(page.raw(), self.header.elems as usize);

        let mut iter = self.iter.take().unwrap();
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

        Ok((page.clone(), b))
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
        let mut tail: Option<page::SibPage<Ver, Value<T>>> = None;
        let mut beg = iter.curr_pos();

        while cnt > 0 {
            let saved = beg;
            let mut len = SIBPG_HDR_LEN;
            while cnt > 0 {
                let (_, v) = iter.next().expect("it's always valid here");
                let vz = v.packed_size();
                let vlen = if vz > limit {
                    Slot::REMOTE_LEN
                } else {
                    vz + Slot::LOCAL_LEN
                };
                let tmp = len + (Ver::len() + vlen) + SLOT_LEN;
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
            let mut pg = SibPage::<Ver, Value<T>>::from(b.payload());

            if head.is_none() {
                head = Some(addr);
            }

            let hdr = pg.header_mut();
            hdr.init(b.payload().len() as u32, (beg - saved) as u32);

            let mut builder = PageBuilder::<SibPageHdr>::from(b.payload(), beg - saved);

            if let Some(mut last) = tail {
                last.header_mut().link = addr;
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
            page::{DeltaType, LeafMergeIter, NodeType},
        },
        map::data::FrameOwner,
        utils::traits::IDataLoader,
    };

    use super::{FuseBuilder, page::MainPage};

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
