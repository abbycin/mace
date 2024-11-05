pub mod byte_array;
pub(crate) mod data;
pub mod options;
pub(crate) mod queue;

pub(crate) const NAN_PID: u64 = 0;
pub(crate) const ROOT_PID: u64 = 1;
pub(crate) const NEXT_ID: u32 = 1;

#[derive(Debug)]
pub enum OpCode {
    Ok,
    Duplicate,
    NotFound,
    TooLarge,
    NeedMore,
    Again,
    Invalid,
    NoSpace,
    IoError,
    Unknown,
}

pub(crate) const fn align_up(n: usize, align: usize) -> usize {
    (n + (align - 1)) & !(align - 1)
}

#[repr(u8)]
#[derive(Copy, Clone)]
pub enum TxnState {
    Idle,
    Start,
    Commit,
    Abort,
}

#[repr(u8)]
#[derive(Copy, Clone)]
pub enum IsolationLevel {
    SI,
    SSI,
}

#[repr(u8)]
#[derive(Copy, Clone)]
pub enum TxnMode {
    Short,
    Long,
}

const _: () = assert!(size_of::<TxnState>() == 1);
const _: () = assert!(size_of::<IsolationLevel>() == 1);
const _: () = assert!(size_of::<TxnMode>() == 1);

// BwTree Related Stuffs

#[repr(u8)]
enum NodeType {
    Delta,
    Base,
}

#[repr(u8)]
enum OpType {
    Insert,
    Update,
    Remove,
    Flush,
    Merge,
    Split,
}

pub(crate) fn is_power_of_2(x: usize) -> bool {
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

pub(crate) const fn encode_u64(hi: u32, lo: u32) -> u64 {
    (hi as u64) << 32 | lo as u64
}

pub(crate) const fn decode_u64(x: u64) -> (u32, u32) {
    ((x >> 32) as u32, (x & ((1 << 32) - 1)) as u32)
}

static_assert!(size_of::<usize>() == 8, "exepct 64 bits pointer width");

static_assert!(size_of::<usize>() == 8, "exepct 64 bits pointer width");

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
