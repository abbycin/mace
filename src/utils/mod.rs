use std::{
    cell::RefCell,
    ops::{Deref, DerefMut, Range},
    path::{Path, PathBuf},
    sync::atomic::{AtomicI64, AtomicU32, Ordering::Relaxed},
};

use rand::{Rng, rngs::ThreadRng};

pub(crate) mod bitmap;
pub(crate) mod block;
pub(crate) mod bytes;
pub(crate) mod countblock;
pub(crate) mod data;
pub(crate) mod lru;
pub(crate) mod options;
pub(crate) mod queue;
mod spooky;
pub(crate) mod traits;
pub(crate) mod varint;

pub(crate) const NULL_PID: u64 = 0;
pub(crate) const ROOT_PID: u64 = 1;
pub(crate) const NEXT_ID: u32 = 1;
pub(crate) const INVALID_ID: u32 = 0;
pub(crate) const NULL_ID: u32 = u32::MAX;
pub(crate) const INIT_CMD: u32 = 1;
pub(crate) const NULL_CMD: u32 = u32::MAX;
/// NOTE: must larger than wmk_oldest_tx(which is 0 by default)
pub(crate) const INIT_ORACLE: u64 = 2;
pub(crate) const NULL_ORACLE: u64 = u64::MAX;

#[derive(Debug, PartialEq)]
pub enum OpCode {
    NotFound,
    BadData,
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

impl std::fmt::Display for OpCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{:?}", self))
    }
}

pub(crate) const fn align_up(n: usize, align: usize) -> usize {
    (n + (align - 1)) & !(align - 1)
}

#[allow(unused)]
pub(crate) const fn align_down(n: usize, align: usize) -> usize {
    n & !(align - 1)
}

pub(crate) const fn is_power_of_2(x: usize) -> bool {
    if x == 0 { false } else { x & (x - 1) == 0 }
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
    ($slice:expr, $num:ty) => {{ <$num>::from_le_bytes($slice.try_into().unwrap()) }};
}

pub(crate) const SEG_BITS: u64 = 32;

pub(crate) const fn pack_id(hi: u32, lo: u32) -> u64 {
    ((hi as u64) << SEG_BITS) | lo as u64
}

pub(crate) const fn unpack_id(x: u64) -> (u32, u32) {
    (
        (x >> SEG_BITS) as u32,
        (x & ((1u64 << SEG_BITS) - 1)) as u32,
    )
}

thread_local! {
    pub static G_RAND: RefCell<ThreadRng> = RefCell::new(rand::thread_rng());
}

pub fn rand_range(range: Range<usize>) -> usize {
    G_RAND.with_borrow_mut(|x| x.gen_range(range))
}

static_assert!(size_of::<usize>() == 8, "exepct 64 bits pointer width");

pub struct RandomPath {
    path: PathBuf,
    del: bool,
}

impl RandomPath {
    const PREFIX: &'static str = "mace_tmp_";

    fn gen_path(root: &PathBuf) -> PathBuf {
        static TID: AtomicI64 = AtomicI64::new(0);
        let path = Path::new(&root);
        loop {
            let r = rand_range(1000..1000000);
            let p = path.join(format!(
                "{}{}{}{}",
                Self::PREFIX,
                std::process::id(),
                TID.fetch_add(1, Relaxed),
                r
            ));
            if !p.exists() {
                return p;
            }
        }
    }

    pub fn tmp() -> Self {
        Self {
            path: Self::gen_path(&std::env::temp_dir()),
            del: true,
        }
    }

    pub fn new() -> Self {
        Self {
            path: Self::gen_path(&std::env::temp_dir()),
            del: false,
        }
    }

    pub fn from_root<P: AsRef<Path>>(root: P) -> Self {
        Self {
            path: Self::gen_path(&root.as_ref().to_path_buf()),
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

struct AMutRefInner<T: Send + Sync> {
    raw: T,
    refcnt: AtomicU32,
}

/// it's user's responsibility to make sure there's no reference cycle
pub struct AMutRef<T: Send + Sync> {
    inner: *mut AMutRefInner<T>,
}

unsafe impl<T: Send + Sync> Send for AMutRef<T> {}
unsafe impl<T: Send + Sync> Sync for AMutRef<T> {}

impl<T: Send + Sync> AMutRef<T> {
    pub fn new(x: T) -> Self {
        Self {
            inner: Box::into_raw(Box::new(AMutRefInner {
                raw: x,
                refcnt: AtomicU32::new(1),
            })),
        }
    }

    pub fn raw(&self) -> *mut T {
        unsafe { &mut (*self.inner).raw }
    }

    fn inc(&self) {
        unsafe { (*self.inner).refcnt.fetch_add(1, Relaxed) };
    }

    fn dec(&self) -> u32 {
        unsafe { (*self.inner).refcnt.fetch_sub(1, Relaxed) }
    }
}

impl<T> Clone for AMutRef<T>
where
    T: Send + Sync,
{
    fn clone(&self) -> Self {
        self.inc();
        Self { inner: self.inner }
    }
}

impl<T> Drop for AMutRef<T>
where
    T: Send + Sync,
{
    fn drop(&mut self) {
        if self.dec() == 1 {
            unsafe { drop(Box::from_raw(self.inner)) };
        }
    }
}

impl<T> Deref for AMutRef<T>
where
    T: Send + Sync,
{
    type Target = T;
    fn deref(&self) -> &Self::Target {
        unsafe { &(*self.inner).raw }
    }
}

impl<T> DerefMut for AMutRef<T>
where
    T: Send + Sync,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut (*self.inner).raw }
    }
}

struct MutRefInner<T> {
    raw: T,
    refcnt: u32,
}

impl<T> MutRefInner<T> {
    fn new(x: T) -> Self {
        Self { raw: x, refcnt: 1 }
    }
}

pub struct MutRef<T> {
    inner: *mut MutRefInner<T>,
}

impl<T> MutRef<T> {
    pub fn new(x: T) -> Self {
        Self {
            inner: Box::into_raw(Box::new(MutRefInner::new(x))),
        }
    }

    #[allow(clippy::mut_from_ref)]
    pub fn raw(&self) -> &mut T {
        unsafe { &mut (*self.inner).raw }
    }

    fn inc(&self) {
        unsafe { (*self.inner).refcnt += 1 };
    }

    fn dec(&self) -> u32 {
        unsafe {
            let old = (*self.inner).refcnt;
            (*self.inner).refcnt -= 1;
            old
        }
    }
}

impl<T> Clone for MutRef<T> {
    fn clone(&self) -> Self {
        self.inc();
        Self { inner: self.inner }
    }
}

impl<T> Drop for MutRef<T> {
    fn drop(&mut self) {
        unsafe {
            let old = self.dec();
            if old == 1 {
                drop(Box::from_raw(self.inner));
            }
        }
    }
}

impl<T> Deref for MutRef<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &(*self.inner).raw }
    }
}

impl<T> DerefMut for MutRef<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut (*self.inner).raw }
    }
}

#[cfg(test)]
mod test {
    use crate::utils::align_up;

    #[test]
    fn test_free_functions() {
        assert_eq!(align_up(4, 8), 8);
        assert_eq!(align_up(16, 8), 16);
        assert_eq!(align_up(23, 8), 24);
        assert_eq!(align_up(56, 8), 56);

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
