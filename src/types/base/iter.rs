use super::*;

#[repr(C)]
pub(crate) struct BaseIter<'a, L, K>
where
    L: ILoader,
    K: IKey,
{
    beg: usize,
    end: usize,
    sst: Sst<K>,
    pub loader: &'a L,
    hist: Option<HistIter<'a>>,
    keepalive: Vec<BoxRef>,
}

#[repr(C)]
pub(crate) struct BaseRevIter<'a, L, K>
where
    L: ILoader,
    K: IKey,
{
    cur: isize,
    end: isize,
    sst: Sst<K>,
    pub loader: &'a L,
    hist: Option<HistIter<'a>>,
    keepalive: Vec<BoxRef>,
}

struct HistIter<'a> {
    key_raw: &'a [u8],
    page: BoxRef,
    slot: usize,
    remaining: usize,
}

impl HistIter<'_> {
    #[inline]
    fn current_page(&self) -> BaseView {
        self.page.view().as_base()
    }
}

impl<'a, L, K> BaseIter<'a, L, K>
where
    L: ILoader,
    K: IKey,
{
    pub(super) fn new(loader: &'a L, sst: Sst<K>, beg: usize, end: usize) -> Self {
        Self {
            loader,
            sst,
            beg,
            end,
            hist: None,
            keepalive: Vec::new(),
        }
    }
}

impl<'a, L, K> BaseRevIter<'a, L, K>
where
    L: ILoader,
    K: IKey,
{
    pub(crate) fn new(loader: &'a L, sst: Sst<K>, start: isize, end: isize) -> Self {
        Self {
            loader,
            sst,
            cur: start,
            end,
            hist: None,
            keepalive: Vec::new(),
        }
    }
}

impl<'a, L> Iterator for BaseIter<'a, L, Key<'a>>
where
    L: ILoader,
{
    type Item = (Key<'a>, Val<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        self.next_with_sibling(|_| {})
    }
}

impl<'a, L> BaseIter<'a, L, Key<'a>>
where
    L: ILoader,
{
    pub(crate) fn try_next_with_sibling<F>(
        &mut self,
        mut on_sibling: F,
    ) -> Result<Option<(Key<'a>, Val<'a>)>, OpCode>
    where
        F: FnMut(u64),
    {
        while let Some(state) = self.hist.as_mut() {
            if state.remaining == 0 {
                self.hist = None;
                continue;
            }
            let page = state.current_page();

            let elems = page.header().elems as usize;
            if state.slot >= elems {
                let link = page.box_header().link;
                if link == NULL_ADDR {
                    self.hist = None;
                    continue;
                }
                on_sibling(link);
                let page = self.loader.load_sibling(link);
                self.keepalive.push(page.clone());
                state.page = page;
                state.slot = 0;
                continue;
            }

            let (ver, val) = page.sst::<Ver>().kv_at(state.slot);
            state.slot += 1;
            state.remaining -= 1;
            let k = Key::new(state.key_raw, ver);
            return Ok(Some((k, val)));
        }
        if self.beg < self.end {
            let (k, v) = self.sst.kv_at::<Val>(self.beg);
            self.beg += 1;
            if let Some(hist) = v.get_hist() {
                on_sibling(hist.page_addr);
                let page = self.loader.load_sibling(hist.page_addr);
                self.keepalive.push(page.clone());
                self.hist = Some(HistIter {
                    key_raw: k.raw,
                    page,
                    slot: hist.slot as usize,
                    remaining: hist.count as usize,
                });
                // the sibling is still in `v`
                Ok(Some((k, v)))
            } else {
                Ok(Some((k, v)))
            }
        } else {
            Ok(None)
        }
    }

    pub(crate) fn next_with_sibling<F>(&mut self, mut on_sibling: F) -> Option<(Key<'a>, Val<'a>)>
    where
        F: FnMut(u64),
    {
        self.try_next_with_sibling(&mut on_sibling)
            .expect("must exist")
    }
}

impl<'a, L> BaseRevIter<'a, L, Key<'a>>
where
    L: ILoader,
{
    pub(crate) fn try_next_back_with_sibling<F>(
        &mut self,
        mut on_sibling: F,
    ) -> Result<Option<(Key<'a>, Val<'a>)>, OpCode>
    where
        F: FnMut(u64),
    {
        while let Some(state) = self.hist.as_mut() {
            if state.remaining == 0 {
                self.hist = None;
                continue;
            }
            let page = state.current_page();

            let elems = page.header().elems as usize;
            if state.slot >= elems {
                let link = page.box_header().link;
                if link == NULL_ADDR {
                    self.hist = None;
                    continue;
                }
                on_sibling(link);
                let page = self.loader.load_sibling(link);
                self.keepalive.push(page.clone());
                state.page = page;
                state.slot = 0;
                continue;
            }

            let (ver, val) = page.sst::<Ver>().kv_at(state.slot);
            state.slot += 1;
            state.remaining -= 1;
            let k = Key::new(state.key_raw, ver);
            return Ok(Some((k, val)));
        }

        if self.cur >= self.end {
            let pos = self.cur as usize;
            let (k, v) = self.sst.kv_at::<Val>(pos);
            self.cur -= 1;
            if let Some(hist) = v.get_hist() {
                on_sibling(hist.page_addr);
                let page = self.loader.load_sibling(hist.page_addr);
                self.keepalive.push(page.clone());
                self.hist = Some(HistIter {
                    key_raw: k.raw,
                    page,
                    slot: hist.slot as usize,
                    remaining: hist.count as usize,
                });
            }
            Ok(Some((k, v)))
        } else {
            Ok(None)
        }
    }
}

impl<'a, L> Iterator for BaseIter<'a, L, IntlKey<'a>>
where
    L: ILoader,
{
    type Item = (IntlKey<'a>, Index);

    fn next(&mut self) -> Option<Self::Item> {
        if self.beg < self.end {
            let (k, v) = self.sst.kv_at(self.beg);
            self.beg += 1;
            Some((k, v))
        } else {
            None
        }
    }
}

pub(super) struct FuseBaseIter<'a, L, K>
where
    L: ILoader,
    K: IKey,
{
    prefix1: &'a [u8],
    prefix2: &'a [u8],
    iter1: BaseIter<'a, L, K>,
    iter2: BaseIter<'a, L, K>,
}

impl<'a, L, K> FuseBaseIter<'a, L, K>
where
    L: ILoader,
    K: IKey,
{
    pub(super) fn new(
        prefix1: &'a [u8],
        prefix2: &'a [u8],
        i1: BaseIter<'a, L, K>,
        i2: BaseIter<'a, L, K>,
    ) -> Self {
        Self {
            prefix1,
            prefix2,
            iter1: i1,
            iter2: i2,
        }
    }
}

impl<'a, L> Iterator for FuseBaseIter<'a, L, Key<'a>>
where
    L: ILoader,
{
    type Item = (LeafSeg<'a>, Val<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((k, v)) = self.iter1.next() {
            Some((LeafSeg::new(self.prefix1, k.raw, k.ver), v))
        } else {
            self.iter2
                .next()
                .map(|(k, v)| (LeafSeg::new(self.prefix2, k.raw, k.ver), v))
        }
    }
}

impl<'a, L> Iterator for FuseBaseIter<'a, L, IntlKey<'a>>
where
    L: ILoader,
{
    type Item = (IntlSeg<'a>, Index);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((k, v)) = self.iter1.next() {
            Some((IntlSeg::new(self.prefix1, k.raw), v))
        } else {
            self.iter2
                .next()
                .map(|(k, v)| (IntlSeg::new(self.prefix2, k.raw), v))
        }
    }
}
