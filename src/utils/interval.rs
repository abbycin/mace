use std::collections::{BTreeMap, btree_map::Entry};

#[derive(Default)]
pub struct IntervalMap {
    map: BTreeMap<u64, (u64, u64)>,
}

impl IntervalMap {
    pub fn new() -> Self {
        Self {
            map: Default::default(),
        }
    }

    /// the range must not overlap
    pub fn insert(&mut self, lo: u64, hi: u64, val: u64) {
        #[cfg(feature = "extra_check")]
        assert_eq!(self.find(lo), None);
        self.map.insert(lo, (hi, val));
    }

    /// used by recovery only, because it's possible that same interval point to another val
    pub fn upsert(&mut self, lo: u64, hi: u64, val: u64) {
        let e = self.map.entry(lo);
        match e {
            Entry::Vacant(v) => {
                v.insert((hi, val));
            }
            Entry::Occupied(mut o) => {
                let old = o.get_mut();
                #[cfg(feature = "extra_check")]
                assert_eq!(old.0, hi);
                old.1 = val;
            }
        }
    }

    pub fn update(&mut self, lo: u64, _hi: u64, val: u64) {
        let e = self.map.entry(lo);
        match e {
            Entry::Occupied(mut o) => {
                let v = o.get_mut();
                #[cfg(feature = "extra_check")]
                assert_eq!(v.0, _hi);
                v.1 = val;
            }
            _ => unreachable!("impossible for update"),
        }
    }

    pub fn remove(&mut self, lo: u64) -> Option<(u64, u64)> {
        self.map.remove(&lo)
    }

    pub fn find(&self, point: u64) -> Option<u64> {
        if let Some((lo, (hi, val))) = self.map.range(..=point).next_back()
            && point >= *lo
            && point <= *hi
        {
            return Some(*val);
        }
        None
    }
}

#[cfg(test)]
mod test {
    use crate::utils::interval::IntervalMap;

    #[test]
    fn basic_operations() {
        let mut b = IntervalMap::new();

        b.insert(0, 100, 10);
        b.insert(101, 200, 20);

        let x = b.find(0);
        assert_eq!(x, Some(10));

        let x = b.find(100);
        assert_eq!(x, Some(10));

        let x = b.find(50);
        assert_eq!(x, Some(10));

        let y = b.find(101);
        assert_eq!(y, Some(20));

        let y = b.find(200);
        assert_eq!(y, Some(20));

        let y = b.find(150);
        assert_eq!(y, Some(20));

        b.remove(0);
        let x = b.find(10);
        assert_eq!(x, None);

        let y = b.find(150);
        assert_eq!(y, Some(20));
    }

    #[test]
    #[should_panic]
    fn moha() {
        let mut b = IntervalMap::new();

        b.insert(1, 6854, 5); // 5 was compacted from 1 and 3
        b.insert(2367, 4739, 2); // it overlaps with 2
        b.insert(6855, 9218, 4);
        b.insert(9219, 11340, 6);

        let x = b.find(5076);
        assert_eq!(x, Some(5));

        let x = b.find(6436);
        assert_eq!(x, Some(5));
    }

    #[test]
    fn merge_split() {
        let mut b = IntervalMap::new();
        const I: u64 = 1000;
        const J: u64 = 100;
        fn gen_data(i: u64, j: u64) -> (u64, u64) {
            // make sure not overlap
            let lo = (i * 100 + j) * 2;
            (lo, lo + 1)
        }

        for i in 0..I {
            for j in 0..J {
                let (lo, hi) = gen_data(i, j);
                b.insert(lo, hi, lo);
                let x = b.find(lo);
                assert_eq!(x, Some(lo));
            }
        }

        for i in 0..I {
            for j in 0..J {
                // make sure not overlap
                let (lo, _) = gen_data(i, j);
                let x = b.find(lo);
                assert_eq!(x, Some(lo));
            }
        }

        for i in 0..I {
            for j in 0..J {
                if i & 1 == 0 && j & 1 == 0 {
                    let lo = (i * 100 + j) * 2;
                    b.remove(lo);
                }
            }
        }

        for i in 0..I {
            for j in 0..J {
                let (lo, _) = gen_data(i, j);
                let x = b.find(lo);
                if i & 1 == 0 && j & 1 == 0 {
                    assert_eq!(x, None);
                } else {
                    assert_eq!(x, Some(lo));
                }
            }
        }
    }
}
