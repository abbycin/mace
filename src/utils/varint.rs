pub struct Varint32;
#[cfg(test)]
pub struct Varint64;

macro_rules! impl_varint {
    ($name: ty, $bits: expr, $max_bytes: expr, $e: ty) => {

        impl $name {
            #[allow(unused)]
            pub fn size(x: usize) -> usize {
                if x < (1 << 7) {
                    1
                } else if x < (1 << 14) {
                    2
                } else if x < (1 << 21) {
                    3
                } else if x < (1 << 28) {
                    4
                } else if x < (1 << 35) {
                    5
                } else if x < (1 << 42) {
                    6
                } else if x < (1 << 49) {
                    7
                } else if x < (1 << 56) {
                    8
                } else if x < (1 << 63) {
                    9
                } else {
                    $max_bytes
                }
            }

            pub fn encode(data: &mut [u8], mut x: $e) -> usize {
                let mut i = 0;
                while x >= 128 && i < $max_bytes - 1 {
                    data[i] = (x as u8) | 128;
                    x >>= 7;
                    i += 1;
                }
                data[i] = x as u8;
                debug_assert!(data[i] < 128);
                i + 1
            }

            #[allow(unused)]
            pub fn decode(data: &[u8]) -> Option<($e, usize)> {
                let mut n: $e = 0;
                let mut shift = 0;
                for (i, &x) in data.iter().enumerate().take($max_bytes) {
                    if x < 128 {
                        return (<$e>::from(x)).checked_shl(shift).map(|x| (n | x, (i+1) as usize));
                    }
                    // extract lower 7 bits
                    if let Some(x) = ((<$e>::from(x)) & 127).checked_shl(shift) {
                        n |= x;
                    } else {
                        return None;
                    }
                    shift += 7;
                }
                None
            }
        }
    };
}

impl_varint!(Varint32, 32, 5, u32);
#[cfg(test)]
impl_varint!(Varint64, 64, 10, u64);

#[cfg(test)]
mod test {
    use super::{Varint32, Varint64};

    #[test]
    fn test_varint32() {
        let x = 114514;
        let mut data = [0u8; 4];

        assert_eq!(Varint32::size(127), 1);
        assert_eq!(Varint32::size(128), 2);
        assert_eq!(Varint32::size(255), 2);
        assert_eq!(Varint32::size(256), 2);

        let n = Varint32::encode(&mut data, x);
        assert_eq!(n, 3);

        let ans = Varint32::decode(&data);
        assert_ne!(ans, None);
        let (y, n) = ans.unwrap();
        assert_eq!(x, y);
        assert_eq!(n, 3);
    }

    #[test]
    fn test_varint64() {
        let x = 114514114514;
        let mut data = [0u8; 8];

        let n = Varint64::encode(&mut data, x);
        assert_eq!(n, 6);

        let ans = Varint64::decode(&data);
        assert_ne!(ans, None);
        let (y, n) = ans.unwrap();
        assert_eq!(x, y);
        assert_eq!(n, 6);
    }
}
