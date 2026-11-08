use super::*;

/// the layout of sst:
/// ```text
/// +---------+-------------+---------+--------+----------+
/// | header  | index table | low key | hi key |key-value |
/// +---------+-------------+---------+--------+----------+
/// ```
pub(super) struct Builder<'a> {
    slots: &'a mut [SlotType],
    payload: &'a mut [u8],
    pos: usize,
    index: usize,
    offset: usize,
}

impl<'a> Builder<'a> {
    /// the `s` is starts from index table
    pub(super) fn from(s: &'a mut [u8], elems: usize) -> Self {
        let offset = elems * SLOT_LEN;
        let slots = unsafe {
            let p = s.as_mut_ptr().cast::<SlotType>();
            std::slice::from_raw_parts_mut(p, elems)
        };
        Self {
            slots,
            payload: s,
            pos: BaseView::HDR_LEN,
            index: 0,
            offset,
        }
    }

    pub(super) fn setup_boundary_keys(&mut self, lo: &[u8], hi: Option<&[u8]>) {
        let hi_len = hi.map_or(0, |x| x.len());
        let boundary_len = lo.len() + hi_len;

        if boundary_len > 0 {
            self.payload[self.offset..self.offset + lo.len()].copy_from_slice(lo);
            self.offset += lo.len();
            self.payload[self.offset..self.offset + hi_len].copy_from_slice(hi.map_or(&[], |x| x));
            self.offset += hi_len;
        }
    }

    #[inline]
    pub(super) fn slice(&mut self, b: usize, len: usize) -> &mut [u8] {
        &mut self.payload[b..b + len]
    }

    pub(super) fn add_intl<K>(&mut self, k: &K, v: &Index)
    where
        K: ICodec,
    {
        let (ksz, vsz) = (k.packed_size(), v.packed_size());
        k.encode_to(self.slice(self.offset, ksz));
        v.encode_to(self.slice(self.offset + ksz, vsz));

        self.update_slot(ksz + vsz);
    }

    pub(super) fn add_leaf<K>(
        &mut self,
        limit: usize,
        k: &K,
        v: &Record,
        hist: Option<HistRef>,
        remote: u64,
    ) where
        K: ICodec,
    {
        let ksz = k.packed_size();
        let vsz = v.packed_size();
        let val_sz = Val::calc_size(hist.is_some(), limit, vsz);

        k.encode_to(self.slice(self.offset, ksz));
        if vsz < limit {
            #[cfg(feature = "extra_check")]
            assert_eq!(remote, NULL_ADDR);
            Val::encode_inline(self.slice(self.offset + ksz, val_sz), hist, v);
        } else {
            Val::encode_remote(self.slice(self.offset + ksz, val_sz), hist, remote, v);
        }

        self.update_slot(ksz + val_sz);
    }

    #[inline(always)]
    pub(super) fn update_slot(&mut self, total_size: usize) {
        self.slots[self.index] = (self.pos + self.offset) as SlotType;
        self.index += 1;
        self.offset += total_size;
    }
}

pub(super) struct SeekableIter<'a> {
    data: Vec<(LeafSeg<'a>, CompactVal<'a>)>,
    index: Cell<usize>,
}

impl<'a> SeekableIter<'a> {
    pub(super) fn new() -> Self {
        Self {
            data: Vec::with_capacity(BucketOptions::MAX_SPLIT_ELEMS as usize * 2),
            index: Cell::new(0),
        }
    }

    pub(super) fn add(&mut self, k: LeafSeg<'a>, v: CompactVal<'a>) {
        self.data.push((k, v));
    }

    pub(super) fn seek_to(&self, pos: usize) {
        self.index.set(pos);
    }

    pub(super) fn next(&self) -> Option<&(LeafSeg<'a>, CompactVal<'a>)> {
        let idx = self.index.get();
        if idx < self.data.len() {
            self.index.set(idx + 1);
            Some(&self.data[idx])
        } else {
            None
        }
    }
}
