//! port from http://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue
use std::{
    cell::{Cell, UnsafeCell},
    mem::MaybeUninit,
    ops::Deref,
    sync::atomic::{
        AtomicUsize,
        Ordering::{Acquire, Relaxed, Release},
    },
};

#[repr(align(64))]
struct Atomic {
    data: AtomicUsize,
}

impl Deref for Atomic {
    type Target = AtomicUsize;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

struct Sched {
    count: Cell<u32>,
}

impl Sched {
    const SPIN_COUNT: u32 = 3;
    const YIELD_COUNT: u32 = 5;

    const fn new() -> Self {
        Self {
            count: Cell::new(0),
        }
    }

    #[inline]
    fn spin(&self) {
        for _ in 0..(1 << Self::SPIN_COUNT) {
            std::hint::spin_loop();
        }
        if self.count.get() <= Self::SPIN_COUNT {
            self.count.set(self.count.get() + 1);
        }
    }

    #[inline]
    fn sched(&self) {
        for _ in 0..(1 << self.count.get()) {
            std::hint::spin_loop();
        }
        if self.count.get() > Self::SPIN_COUNT {
            std::thread::yield_now();
        }
        if self.count.get() <= Self::YIELD_COUNT {
            self.count.set(self.count.get() + 1);
        }
    }
}

pub(crate) struct Queue<T> {
    prod: Atomic,
    cons: Atomic,
    mask: usize,
    slots: Box<[Slot<T>]>,
}

impl<T> Queue<T> {
    pub(crate) fn new(cap: usize) -> Self {
        assert!(cap >= 1);
        assert!(cap.is_power_of_two());
        let slots: Box<[Slot<T>]> = (0..cap)
            .map(|x| Slot {
                seq: AtomicUsize::new(x),
                data: UnsafeCell::new(MaybeUninit::uninit()),
            })
            .collect();

        Self {
            prod: Atomic {
                data: AtomicUsize::new(0),
            },
            cons: Atomic {
                data: AtomicUsize::new(0),
            },
            mask: cap - 1,
            slots,
        }
    }

    pub(crate) fn push(&self, data: T) -> Result<(), T> {
        let sched = Sched::new();
        let mut prod = self.prod.load(Relaxed);
        loop {
            let slot = unsafe { self.slots.get_unchecked(prod & self.mask) };
            let seq = slot.seq.load(Acquire);
            if seq == prod {
                match self
                    .prod
                    .compare_exchange_weak(prod, prod + 1, Relaxed, Relaxed)
                {
                    Ok(_) => {
                        unsafe { slot.data.get().write(MaybeUninit::new(data)) };
                        slot.seq.store(prod + 1, Release);
                        return Ok(());
                    }
                    Err(cur) => prod = cur,
                }
            } else if seq < prod {
                // another thread just update prod (but not update seq yet) before we load prod
                // and the consumer is one round behind the producer, so the queue is full
                if prod == self.cons.load(Relaxed) + self.cap() {
                    return Err(data);
                }
                sched.spin();
                prod = self.prod.load(Relaxed);
            } else {
                sched.sched();
                prod = self.prod.load(Relaxed);
            }
        }
    }

    pub(crate) fn pop(&self) -> Option<T> {
        let sched = Sched::new();
        let mut cons = self.cons.load(Relaxed);
        loop {
            let slot = unsafe { self.slots.get_unchecked(cons & self.mask) };
            let seq = slot.seq.load(Acquire);
            let tgt = cons + 1;
            if seq == tgt {
                match self
                    .cons
                    .compare_exchange_weak(cons, cons + 1, Relaxed, Relaxed)
                {
                    Ok(_) => {
                        let data = unsafe { slot.data.get().read().assume_init() };
                        // prepare seq for **next round** `push`
                        slot.seq.store(cons + self.cap(), Release);
                        return Some(data);
                    }
                    Err(cur) => cons = cur,
                }
            } else if seq < tgt {
                if self.prod.load(Relaxed) == cons {
                    return None;
                }
                sched.spin();
                cons = self.cons.load(Relaxed);
            } else {
                sched.sched();
                cons = self.cons.load(Relaxed);
            }
        }
    }

    #[inline(always)]
    pub(crate) fn cap(&self) -> usize {
        self.mask + 1
    }

    #[allow(unused)]
    pub(crate) fn count(&self) -> usize {
        self.prod.load(Relaxed) - self.cons.load(Relaxed)
    }

    #[allow(unused)]
    pub(crate) fn is_empty(&self) -> bool {
        self.prod.load(Relaxed) == self.cons.load(Relaxed)
    }

    #[allow(unused)]
    pub(crate) fn is_full(&self) -> bool {
        self.count() == self.cap()
    }
}

impl<T> Drop for Queue<T> {
    fn drop(&mut self) {
        if std::mem::needs_drop::<T>() {
            while !self.is_empty() {
                self.pop();
            }
        }
    }
}

struct Slot<T> {
    seq: AtomicUsize,
    data: UnsafeCell<MaybeUninit<T>>,
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
        let q = Arc::new(Queue::new(16));
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
                    while let Some(x) = queue.pop() {
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

    #[derive(Debug, Clone, Copy)]
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
        let q = Arc::new(Queue::new(n));
        let mut v = Vec::with_capacity(n);
        for _ in 0..n {
            let h = Handle::new();
            v.push(h);
            q.push(h).unwrap();
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
                        if let Some(mut x) = cq.pop() {
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
        while let Some(h) = v.pop() {
            Handle::free(h);
        }
    }
}
