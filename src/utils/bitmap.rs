use std::fmt::{Debug, Formatter};

pub(crate) struct BitMap {
    cap: u32,
    data: Vec<u64>,
}

const POWER: u32 = 6;
const BITS: u32 = 64;
const MASK: u32 = BITS - 1;

pub type BitmapElemType = u64;
pub const BITMAP_ELEM_LEN: usize = size_of::<BitmapElemType>();

impl BitMap {
    /// cap is the target count
    pub(crate) fn new(cap: u32) -> Self {
        let cap = match cap % BITS {
            0 => cap / BITS,
            _ => cap / BITS + 1,
        };

        Self {
            cap,
            data: vec![0u64; cap as usize],
        }
    }

    pub(crate) fn from(data: &[BitmapElemType]) -> Self {
        let cap = data.len() as u32;
        let v = data.to_vec();

        Self { cap, data: v }
    }

    pub(crate) fn set(&mut self, bit: u32) {
        self.data[(bit >> POWER) as usize] |= 1 << (bit & MASK);
    }

    #[allow(unused)]
    pub(crate) fn del(&mut self, bit: u32) {
        self.data[(bit >> POWER) as usize] &= !(1 << (bit & MASK));
    }

    pub(crate) fn test(&self, bit: u32) -> bool {
        self.data[(bit >> POWER) as usize] & (1 << (bit & MASK)) != 0
    }

    pub(crate) fn slice(&self) -> &[u64] {
        self.data.as_slice()
    }

    pub(crate) fn len(&self) -> usize {
        self.data.len()
    }

    #[allow(unused)]
    pub(crate) fn clear(&mut self) {
        self.data.fill(0);
    }

    pub(crate) fn iter(&self) -> BitMapIter {
        BitMapIter {
            m: self,
            idx: 0,
            end: self.bits(),
        }
    }

    pub(crate) fn bits(&self) -> u32 {
        self.cap * size_of::<BitmapElemType>() as u32
    }
}

pub(crate) struct BitMapIter<'a> {
    m: &'a BitMap,
    idx: u32,
    end: u32,
}

impl Iterator for BitMapIter<'_> {
    type Item = (bool, u32);
    fn next(&mut self) -> Option<Self::Item> {
        if self.idx < self.end {
            let r = Some((self.m.test(self.idx), self.idx));
            self.idx += 1;
            return r;
        }
        None
    }
}

impl Debug for BitMap {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut bits = vec![0u8; self.bits() as usize];
        for i in 0..self.bits() {
            if self.test(i) {
                bits[i as usize] = 1;
            }
        }
        f.write_fmt(format_args!("{:?}", bits))
    }
}

#[cfg(test)]
mod test {
    use super::BitMap;

    #[test]
    fn bitmap() {
        let mut m = BitMap::new(32);

        assert_eq!(m.len(), 1);

        for i in 0..32 {
            m.set(i);
        }

        for i in 0..32 {
            assert!(m.test(i));
            m.del(i);
        }

        for i in 0..64 {
            assert!(!m.test(i));
        }
    }
}
