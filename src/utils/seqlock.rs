use crate::utils::CachePad;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};

pub struct SeqLock {
    seq: CachePad<AtomicU64>,
}

pub struct SeqWriteGuard<'a> {
    lock: &'a SeqLock,
}

impl SeqLock {
    #[inline]
    pub fn new() -> Self {
        Self {
            seq: CachePad::default(),
        }
    }

    #[inline]
    pub fn write_lock(&self) -> SeqWriteGuard<'_> {
        let mut seq = self.seq.load(Relaxed);
        loop {
            if seq & 1 == 1 {
                std::hint::spin_loop();
                seq = self.seq.load(Relaxed);
                continue;
            }
            match self
                .seq
                .compare_exchange_weak(seq, seq + 1, AcqRel, Acquire)
            {
                Ok(_) => return SeqWriteGuard { lock: self },
                Err(actual) => seq = actual,
            }
        }
    }

    #[inline]
    pub fn read<T, F>(&self, mut f: F) -> T
    where
        F: FnMut() -> T,
    {
        loop {
            let seq1 = self.seq.load(Acquire);
            if seq1 & 1 == 1 {
                std::hint::spin_loop();
                continue;
            }
            let out = f();
            let seq2 = self.seq.load(Acquire);
            if seq1 == seq2 {
                return out;
            }
        }
    }
}

impl Drop for SeqWriteGuard<'_> {
    fn drop(&mut self) {
        self.lock.seq.fetch_add(1, Release);
    }
}
