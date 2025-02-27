pub(crate) struct BitMap {
    cap: u32,
    size: u32,
    data: Vec<u64>,
}

const POWER: u32 = 6;
const BITS: u32 = 64;
const MASK: u32 = BITS - 1;

impl BitMap {
    /// cap is the target count
    pub(crate) fn new(cap: u32) -> Self {
        let cnt = match cap % BITS {
            0 => cap / BITS,
            _ => cap / BITS + 1,
        };

        Self {
            cap,
            size: 0,
            data: vec![0u64; cnt as usize],
        }
    }

    pub(crate) fn set(&mut self, bit: u32) {
        self.data[(bit >> POWER) as usize] |= 1 << (bit & MASK);
        self.size += 1;
    }

    #[allow(unused)]
    pub(crate) fn del(&mut self, bit: u32) {
        self.data[(bit >> POWER) as usize] &= !(1 << (bit & MASK));
        self.size -= 1;
    }

    pub(crate) fn test(&self, bit: u32) -> bool {
        self.data[(bit >> POWER) as usize] & (1 << (bit & MASK)) != 0
    }

    #[allow(unused)]
    pub(crate) fn free(&self) -> u32 {
        self.cap - self.size
    }

    pub(crate) fn full(&self) -> bool {
        self.size == self.cap
    }

    #[cfg(test)]
    fn real_cap(&self) -> usize {
        self.data.len()
    }
}

#[cfg(test)]
mod test {
    use super::BitMap;

    #[test]
    fn bitmap() {
        let mut m = BitMap::new(32);

        assert_eq!(m.real_cap(), 1);

        for i in 0..32 {
            if !m.full() {
                m.set(i);
            }
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
