use crate::utils::{OpCode, is_power_of_2};
use std::alloc::{Layout, alloc, dealloc};
use std::sync::atomic::{AtomicU32, Ordering, fence};

/// we use two pair of prod-{head, tail} and cons-{head, tail} instead of single head-tail pair for
/// the following reason:
/// - when multiple thread content on head to produce and on tail to consume, the free space we are
///   trying to calculate is not safe, for example: thread 1 load the head value and then load the
///   tail value to calculate free space left for consume, at the same time thread 2 increased the
///   tail to consume, thread 1 think it still has space to consume, but actually there's no space
///   before thread 1 is trying to consume, since thread 2 already consumed the last one
///
/// here we use extra tail to sync thread, if thread 1 advance the head first then thread 2, thread
/// 2 will wait the thread 1 finish advance tail before thread 2 progress, so thread 2 can observe
/// thread finish the whole produce operation, this form a sequential order between two thread, who
/// advance head first will finish first
///
/// the free space for producer is: `cap - (prod_head - cons_tail)` where `prod_head - cons_tail`
/// is the maximum range for consumer, the rest space is safe to produce, while the free space for
/// consumer is: `prod_tail - cons_head`
pub struct Queue<T> {
    data: *mut T,
    cap: u32,
    /// require the [`cap`] is power of two for replacing `%` with `&`
    mask: u32,
    dtor: Option<Box<dyn Fn(T)>>,
    prod_head: AtomicU32,
    cons_head: AtomicU32,
    prod_tail: AtomicU32,
    cons_tail: AtomicU32,
}

impl<T> Queue<T> {
    const SPIN_COUNT: usize = 100;

    pub fn new(cap: u32, dtor: Option<Box<dyn Fn(T)>>) -> Self {
        assert!(is_power_of_2(cap as usize));
        let data = unsafe { alloc(Layout::array::<T>(cap as usize).unwrap()) as *mut T };
        Self {
            data,
            cap,
            mask: cap - 1,
            dtor,
            prod_head: AtomicU32::new(0),
            cons_head: AtomicU32::new(0),
            prod_tail: AtomicU32::new(0),
            cons_tail: AtomicU32::new(0),
        }
    }

    pub fn push(&self, x: T) -> Result<(), OpCode> {
        let mut prod_head;
        let mut prod_next;
        let mut cons_tail;
        let cap = self.cap;

        loop {
            prod_head = self.prod_head.load(Ordering::Relaxed);
            cons_tail = self.cons_tail.load(Ordering::Relaxed);

            fence(Ordering::Acquire); // forbid l/l reorder in weak model

            // no space for producer
            if cap.wrapping_add(cons_tail.wrapping_sub(prod_head)) == 0 {
                return Err(OpCode::NoSpace);
            }
            prod_next = prod_head.wrapping_add(1);
            if self
                .prod_head
                .compare_exchange(prod_head, prod_next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
        }

        let mut cnt = Self::SPIN_COUNT;
        // wait other thread finish store `prod_tail`
        while self.prod_tail.load(Ordering::Acquire) != prod_head {
            std::hint::spin_loop();
            cnt -= 1;
            if cnt == 0 {
                cnt = Self::SPIN_COUNT;
                std::thread::yield_now();
            }
        }

        let idx = prod_head & self.mask;
        unsafe {
            self.data.add(idx as usize).write(x);
        }

        // advance to next, equal to other thread's head
        self.prod_tail.store(prod_next, Ordering::Release);
        Ok(())
    }

    pub fn pop(&self) -> Result<T, OpCode> {
        let mut cons_head;
        let mut cons_next;
        let mut prod_tail;

        loop {
            cons_head = self.cons_head.load(Ordering::Relaxed);
            prod_tail = self.prod_tail.load(Ordering::Relaxed);

            fence(Ordering::Acquire); // forbid l/l reorder in weak model

            // no space for consumer
            if prod_tail.wrapping_sub(cons_head) == 0 {
                return Err(OpCode::NoSpace);
            }
            cons_next = cons_head.wrapping_add(1);
            if self
                .cons_head
                .compare_exchange(cons_head, cons_next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
        }

        let mut cnt = Self::SPIN_COUNT;
        while self.cons_tail.load(Ordering::Acquire) != cons_head {
            std::hint::spin_loop();
            cnt -= 1;
            if cnt == 0 {
                cnt = Self::SPIN_COUNT;
                std::thread::yield_now();
            }
        }

        let idx = cons_head & self.mask;
        let r = unsafe { Ok(self.data.add(idx as usize).read()) };

        self.cons_tail.store(cons_next, Ordering::Release);
        r
    }

    pub fn count(&self) -> u32 {
        let prod_tail = self.prod_tail.load(Ordering::Relaxed);
        let cons_tail = self.cons_tail.load(Ordering::Relaxed);

        prod_tail.wrapping_sub(cons_tail) & self.mask
    }

    #[allow(unused)]
    pub fn cap(&self) -> u32 {
        self.cap
    }

    #[allow(unused)]
    pub fn is_full(&self) -> bool {
        self.count() == self.cap
    }

    #[allow(unused)]
    pub fn is_empty(&self) -> bool {
        self.count() == 0
    }
}

impl<T> Drop for Queue<T> {
    fn drop(&mut self) {
        if let Some(f) = self.dtor.take() {
            while let Ok(x) = self.pop() {
                f(x);
            }
        }
        unsafe {
            dealloc(
                self.data as *mut u8,
                Layout::array::<T>(self.cap as usize).unwrap(),
            );
        }
    }
}

unsafe impl<T> Send for Queue<T> {}
unsafe impl<T> Sync for Queue<T> {}

#[cfg(test)]
mod test {

    use crate::utils::queue::Queue;
    use crate::utils::rand_range;
    use std::ops::{Deref, DerefMut};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    #[test]
    fn test_queue() {
        let q = Arc::new(Queue::new(16, None));
        let mut h = Vec::new();
        const COUNT: usize = 10;
        const LOOP: usize = 10000;
        let arr: Arc<[AtomicU64; COUNT]> = Arc::new(std::array::from_fn(|_| AtomicU64::new(0)));

        for i in 0..COUNT {
            let queue = q.clone();
            let a = arr.clone();
            h.push(std::thread::spawn(move || {
                for _ in 0..LOOP {
                    while queue.push(1).is_err() {}
                    while let Ok(x) = queue.pop() {
                        a[i].fetch_add(x, Ordering::Relaxed);
                    }
                }
            }));
        }

        for i in h {
            i.join().unwrap();
        }

        assert_eq!(q.count(), 0);

        let sum = arr.iter().map(|x| x.load(Ordering::Relaxed)).sum::<u64>() as usize;
        assert_eq!(sum, COUNT * LOOP);
    }

    struct Ctx {
        cur: usize,
        last: usize,
    }

    struct Handle {
        raw: *mut Ctx,
    }

    impl Deref for Handle {
        type Target = Ctx;
        fn deref(&self) -> &Self::Target {
            unsafe { &*self.raw }
        }
    }

    impl DerefMut for Handle {
        fn deref_mut(&mut self) -> &mut Self::Target {
            unsafe { &mut *self.raw }
        }
    }

    impl Handle {
        fn new() -> Self {
            Self {
                raw: Box::into_raw(Box::new(Ctx { cur: 0, last: 0 })),
            }
        }

        fn free(h: Self) {
            unsafe {
                let _ = Box::from_raw(h.raw);
            }
        }
    }

    fn delay() {
        for i in 0..rand_range(1..100) {
            std::hint::black_box(i);
        }
    }

    #[test]
    fn test_queue2() {
        let nr_th = std::thread::available_parallelism().unwrap().get();
        let n = nr_th.next_power_of_two();
        let q = Arc::new(Queue::new(n as u32, Some(Box::new(Handle::free))));
        for _ in 0..n {
            q.push(Handle::new()).unwrap();
        }

        let nr_loop = 100000;
        let nr_round = 10;
        let mut thrd = Vec::new();

        for _ in 0..nr_round {
            let num = Arc::new(AtomicUsize::new(1));
            for _ in 0..nr_th {
                let cq = q.clone();
                let cn = num.clone();
                let x = std::thread::spawn(move || {
                    for _ in 0..nr_loop {
                        if let Ok(mut x) = cq.pop() {
                            x.cur = cn.fetch_add(1, Ordering::Relaxed);
                            delay();
                            assert!(x.last < x.cur);
                            cq.push(x).unwrap();
                        }
                    }
                });
                thrd.push(x);
            }

            while let Some(x) = thrd.pop() {
                x.join().unwrap();
            }
        }
    }
}
