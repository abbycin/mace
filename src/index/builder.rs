use std::{collections::VecDeque, iter::Peekable};

use crate::{
    index::data::SLOT_LEN,
    utils::traits::{ICodec, IKey, IPageIter, IVal, IValCodec},
    OpCode,
};

use super::{
    data::{Sibling, Value, Ver},
    iter::{ItemIter, SliceIter},
    page::{self, DeltaType, LeafMergeIter, Meta, NodeType, Page, PageHeader},
    slotted::{self, SlottedPage},
    IAlloc, Key,
};

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

    pub(crate) fn from(mut self, mut iter: T) -> Self {
        for (k, v) in &mut iter {
            self.elems += 1;
            self.size += (k.packed_size() + v.packed_size()) as u32;
        }
        self.size += self.elems as u32 * SLOT_LEN as u32;
        self.iter = Some(iter);
        self
    }

    pub(crate) fn size(&self) -> usize {
        size_of::<PageHeader>() + self.size as usize
    }

    fn build_header(&self, h: &mut PageHeader) {
        h.meta = Meta::new(self.kind, self.class);
        h.set_depth(1);
        h.set_link(0);
        h.len = self.size() as u32;
        h.elems = self.elems as u32;
    }

    pub(crate) fn build(&mut self, page: &mut Page<K, V>) {
        self.build_header(page.header_mut());

        if let Some(iter) = self.iter.as_mut() {
            let mut buf = page::PageBuilder::from(&page.raw(), self.elems as usize);
            iter.rewind();
            for (k, v) in iter {
                buf.add(&k, &v);
            }
        }
    }
}

impl<K, V> Delta<ItemIter<(K, V)>>
where
    K: IKey,
    V: IVal,
{
    pub fn with_item(self, item: (K, V)) -> Self {
        self.from(ItemIter::new(item))
    }
}

impl<'a, K, V> Delta<SliceIter<'a, K, V>>
where
    K: IKey,
    V: IVal,
{
    pub fn with_slice(self, s: &'a [(K, V)]) -> Self {
        self.from(SliceIter::new(s))
    }
}

pub(crate) struct FuseBuilder<'a, T: IValCodec> {
    header: PageHeader,
    iter: Option<LeafMergeIter<'a, T>>,
    hints: VecDeque<(u32, u32, u32)>,
}

impl<'a, T> FuseBuilder<'a, T>
where
    T: IValCodec + 'a,
{
    pub(crate) fn new(kind: DeltaType, class: NodeType) -> Self {
        let mut h = PageHeader::default();
        h.meta = Meta::new(kind, class);
        h.set_depth(1);
        h.set_link(0);
        h.len = page::PAGE_HEADER_SIZE as u32;

        Self {
            header: h,
            iter: None,
            hints: VecDeque::new(),
        }
    }

    pub(crate) fn prepare(&mut self, mut iter: LeafMergeIter<'a, T>) {
        let mut fixed = false;
        self.hints.reserve(iter.len() * 3 / 5);
        let mut last_raw = None;
        let mut pos = 0;

        for (k, v) in &mut iter {
            if let Some(raw) = last_raw {
                if raw == k.raw {
                    if !fixed {
                        fixed = true;
                        self.header.len += Sibling::<T>::ADDR_LEN as u32; // plus extra addr size
                        self.hints.push_back((pos - 1, 0, 0));
                    }
                    let (_, sz, cnt) = self.hints.back_mut().unwrap();
                    *sz += (Ver::len() + v.packed_size()) as u32;
                    *cnt += 1;
                    pos += 1;
                    continue;
                }
            }

            pos += 1;
            last_raw = Some(k.raw);
            fixed = false;
            self.header.len += (k.packed_size() + v.packed_size()) as u32;
            self.header.elems += 1;
        }

        self.header.len += self.header.elems * SLOT_LEN as u32;
        iter.rewind();
        self.iter = Some(iter);
    }

    pub(crate) fn build<A: IAlloc>(
        &mut self,
        a: &mut A,
    ) -> Result<(u64, Page<Key, Value<T>>), OpCode> {
        let (addr, b) = a.allocate(self.header.len as usize)?;
        let mut page = Page::<Key, Value<T>>::from(b.payload());

        *page.header_mut() = self.header;
        let mut builder = page::PageBuilder::from(&page.raw(), self.header.elems as usize);

        let iter = self.iter.take().unwrap();
        let mut iter = iter.peekable();
        let mut pos = 0;

        loop {
            let Some((k, v)) = iter.next() else {
                break;
            };

            let Some((idx, _, _)) = self.hints.front() else {
                builder.add(k, v);
                pos += 1;
                continue;
            };

            if pos == *idx {
                let (_, sz, cnt) = self.hints.pop_front().unwrap();
                let addr = self.save_versions(a, sz as usize, cnt as usize, &mut pos, &mut iter)?;

                builder.add(k, &Value::Sib(Sibling::from(addr, v)));
            } else {
                builder.add(k, v);
            }
            pos += 1;
        }

        Ok((addr, page))
    }

    fn save_versions<A: IAlloc>(
        &mut self,
        a: &mut A,
        mut sz: usize,
        cnt: usize,
        pos: &mut u32,
        iter: &mut Peekable<LeafMergeIter<'a, T>>,
    ) -> Result<u64, OpCode> {
        let pg_sz = a.page_size();
        let mut head = None;
        let mut tail: Option<SlottedPage<Ver, T>> = None;

        // sz can represent elems of same `raw``
        while sz > 0 {
            let n = if sz > pg_sz { sz - pg_sz } else { sz };
            let alloc_sz = n + cnt * SLOT_LEN + slotted::HEADER_LEN;
            let (addr, mut b) = a.allocate(alloc_sz)?;
            b.set_slotted();
            let mut pg = SlottedPage::<Ver, T>::new(b.payload());
            let mut real = 0;

            if head.is_none() {
                head = Some(addr);
            }

            if let Some(mut last) = tail {
                last.header_mut().link = addr;
            }
            tail = Some(pg);

            while let Some((k, v)) = iter.peek() {
                if pg.insert(*k.ver(), *v).is_err() {
                    break;
                }
                real += Ver::len() + v.packed_size();
                *pos += 1;
                iter.next();
            }
            debug_assert_eq!(n, real);
            sz -= real;
        }

        Ok(head.unwrap())
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use crate::{
        index::{
            data::Value,
            page::{DeltaType, LeafMergeIter, NodeType},
            FrameOwner, IAlloc, Key,
        },
        map::data::FrameFlag,
    };

    use super::FuseBuilder;

    struct Arena {
        map: HashMap<u64, FrameOwner>,
        page_size: usize,
    }

    impl Arena {
        fn new(page_size: usize) -> Self {
            Self {
                map: HashMap::new(),
                page_size,
            }
        }

        fn alloc(&mut self, size: usize) -> (u64, FrameOwner) {
            let mut b = FrameOwner::alloc(size);
            let addr = b.data().data() as u64;
            b.init(addr, FrameFlag::Unknown);
            let copy = b.shallow_copy();
            self.map.insert(addr, b);
            (addr, copy)
        }
    }

    impl IAlloc for Arena {
        fn allocate(&mut self, size: usize) -> Result<(u64, FrameOwner), crate::OpCode> {
            Ok(self.alloc(size))
        }

        fn page_size(&self) -> usize {
            self.page_size
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

        b.prepare(iter);

        let (addr, page) = b.build(&mut arena).unwrap();

        page.show(addr);
    }
}
