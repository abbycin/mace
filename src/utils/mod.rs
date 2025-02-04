use std::{
    cell::RefCell,
    ops::{Deref, Range},
    path::{Path, PathBuf},
};

use rand::{rngs::ThreadRng, Rng};

pub(crate) mod block;
pub(crate) mod bytes;
pub(crate) mod countblock;
pub(crate) mod data;
pub(crate) mod lru;
pub mod options;
pub(crate) mod queue;
pub(crate) mod traits;

pub(crate) const NULL_PID: u64 = 0;
pub(crate) const ROOT_PID: u64 = 1;
pub(crate) const ROOT_TREEID: u64 = 0;
pub(crate) const DEFAULT_TREEID: u64 = 1;
pub(crate) const NEXT_ID: u16 = 1;
pub(crate) const NULL_ID: u16 = u16::MAX;
pub(crate) const INIT_CMD: u32 = 1;
pub(crate) const NULL_CMD: u32 = u32::MAX;
/// NOTE: must larger than wmk_oldest_tx(which is 0 by default) and ROOT_TREEID and DEFAULT_TREEID
pub(crate) const INIT_ORACLE: u64 = 2;
pub(crate) const NULL_ORACLE: u64 = u64::MAX;

#[derive(Debug, PartialEq)]
pub enum OpCode {
    NotFound,
    TooLarge,
    NeedMore,
    Again,
    Invalid,
    NoSpace,
    IoError,
    AbortTx,
    Duplicated,
    DbFull,
    Unknown,
}

pub(crate) const fn align_up(n: usize, align: usize) -> usize {
    (n + (align - 1)) & !(align - 1)
}

#[allow(unused)]
pub(crate) const fn align_down(n: usize, align: usize) -> usize {
    n & !(align - 1)
}

#[repr(u8)]
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum IsolationLevel {
    SI,
    SSI,
}

const _: () = assert!(size_of::<IsolationLevel>() == 1);

pub(crate) const fn is_power_of_2(x: usize) -> bool {
    if x == 0 {
        false
    } else {
        x & (x - 1) == 0
    }
}

pub(crate) fn next_power_of_2(x: usize) -> usize {
    let mut r = 1;
    while r < x {
        r <<= 1;
    }
    r
}

pub(crate) fn raw_ptr_to_ref<'a, T>(x: *mut T) -> &'a T {
    unsafe { &*x }
}

pub(crate) fn raw_ptr_to_ref_mut<'a, T>(x: *mut T) -> &'a mut T {
    unsafe { &mut *x }
}

#[macro_export]
macro_rules! static_assert {
    ($cond:expr, $msg:expr) => {
        const _: () = assert!($cond, $msg);
    };

    ($cond:expr) => {
        const _: () = assert!($cond);
    };
}

#[macro_export]
macro_rules! number_to_slice {
    ($num: expr, $slice:expr) => {
        $slice.copy_from_slice(&$num.to_le_bytes());
    };
}
#[macro_export]
macro_rules! slice_to_number {
    ($slice:expr, $num:ty) => {{
        <$num>::from_le_bytes($slice.try_into().unwrap())
    }};
}

pub(crate) const SEG_BITS: u64 = 48;

pub(crate) const fn pack_id(hi: u16, lo: u64) -> u64 {
    ((hi as u64) << SEG_BITS) | lo
}

pub(crate) const fn unpack_id(x: u64) -> (u16, u64) {
    ((x >> SEG_BITS) as u16, (x & ((1u64 << SEG_BITS) - 1)))
}

thread_local! {
    pub static G_RAND: RefCell<ThreadRng> = RefCell::new(rand::thread_rng());
}

pub fn rand_range(range: Range<usize>) -> usize {
    G_RAND.with_borrow_mut(|x| x.gen_range(range))
}

static_assert!(size_of::<usize>() == 8, "exepct 64 bits pointer width");

static_assert!(size_of::<usize>() == 8, "exepct 64 bits pointer width");

pub struct RandomPath {
    path: PathBuf,
    del: bool,
}

impl Default for RandomPath {
    fn default() -> Self {
        Self::new()
    }
}

impl RandomPath {
    fn gen() -> PathBuf {
        let tmp = std::env::temp_dir();
        let path = Path::new(&tmp);
        loop {
            let r = rand_range(1000..1000000);
            let p = path.join(format!("mace_tmp_{}", r));
            if !p.exists() {
                return p;
            }
        }
    }

    pub fn tmp() -> Self {
        Self {
            path: Self::gen(),
            del: true,
        }
    }

    pub fn new() -> Self {
        Self {
            path: Self::gen(),
            del: false,
        }
    }

    pub fn unlink(&self) {
        if self.path.exists() {
            let _ = if self.path.is_file() {
                std::fs::remove_file(&self.path)
            } else {
                std::fs::remove_dir_all(&self.path)
            };
        }
    }
}

impl Deref for RandomPath {
    type Target = PathBuf;

    fn deref(&self) -> &Self::Target {
        &self.path
    }
}

impl Drop for RandomPath {
    fn drop(&mut self) {
        if self.del {
            self.unlink();
        }
    }
}

#[cfg(test)]
mod test {
    use crate::utils::{align_up, is_power_of_2, next_power_of_2};

    #[test]
    fn test_free_functions() {
        assert_eq!(align_up(4, 8), 8);
        assert_eq!(align_up(16, 8), 16);
        assert_eq!(align_up(23, 8), 24);
        assert_eq!(align_up(56, 8), 56);
        assert!(is_power_of_2(1));
        assert!(is_power_of_2(2));
        assert!(is_power_of_2(1 << 10));
        assert!(!is_power_of_2(0));
        assert!(!is_power_of_2(3));
        assert!(!is_power_of_2((1 << 10) - 1));

        assert_eq!(next_power_of_2(0), 1);
        assert_eq!(next_power_of_2(1), 1);
        assert_eq!(next_power_of_2(2), 2);
        assert_eq!(next_power_of_2(3), 4);

        static_assert!(true);
        static_assert!(true, "damn");

        let mut num = 233u64;
        let mut s = &mut num.to_le_bytes()[0..size_of::<u64>()];
        let new_num = slice_to_number!(s, u64);
        assert_eq!(new_num, num);

        s[0] = 1;
        num = 114514;
        number_to_slice!(num, &mut s);
        let new_num = u64::from_le_bytes(s.try_into().unwrap());

        assert_eq!(new_num, num);
    }
}
