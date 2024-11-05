const SWIP_EVICT_BIT: u64 = 1u64 << 63;
const SWIP_EVICT_MASK: u64 = SWIP_EVICT_BIT - 1;
const SWIP_COOL_BIT: u64 = 1u64 << 62;
const SWIP_COOL_MASK: u64 = SWIP_COOL_BIT - 1;
const SWIP_HOT_MASK: u64 = !(3u64 << 62);

/// the Swip is used as both `Page ID` and `BufferFrame`
/// the status transfer is: hot -> cool -> evict
/// when first allocated from buffer manager, the Swip is `hot`, then in page evictor which may mark
/// the Swip to `cool`, and when no one is using the Swip and buffers need to flush, the Swip then be
/// marked and `evict`
///
/// when recover from starting, the Swip is constructed by `Page ID` from WAL with status `evict` and
/// then read page file, then replace Swip::poly with page allocated from buffer manager, and finally
/// following above status change
///
/// NOTE: 这里对应的BufferFrame是Bw Tree中的逻辑节点，包括了delta和base，delta不由buffer manager分配，base才是page
pub struct Swip {
    poly: u64,
}

const _: () = assert!(size_of::<Swip>() == 8);

impl Swip {
    /// NOTE: a further `mark_evict` may required
    pub fn from_pid(pid: u64) -> Self {
        Self { poly: pid }
    }

    /// NOTE: the pointer maybe hot or not
    pub fn from_ptr<T>(ptr: *mut T) -> Self {
        let tmp = Self {
            poly: unsafe { std::mem::transmute(ptr) },
        };
        tmp
    }

    pub fn is_cool(&self) -> bool {
        self.poly & SWIP_COOL_BIT != 0
    }

    /// NOTE: this is the first status of Swip, when Page is load from disk or allocated from buffer
    /// manager, the HOT flag is set
    pub fn is_hot(&self) -> bool {
        self.poly & (SWIP_COOL_BIT | SWIP_EVICT_BIT) == 0
    }

    pub fn is_evict(&self) -> bool {
        self.poly & SWIP_EVICT_BIT != 0
    }

    /// NOTE: when construct from pointer, it already a `BufferFrame` and is hot
    pub fn mark_hot(&mut self) {
        self.poly &= SWIP_COOL_MASK;
    }

    pub fn replace_with_ptr<T>(&mut self, ptr: *mut T) {
        unsafe {
            self.poly = std::mem::transmute(ptr);
        }
        assert!(self.is_hot());
    }

    /// called in recovery procedure as a combination of [`Self::from_pid`] and [`Self::mark_evict`]
    pub fn replace_with_pid(&mut self, pid: u64) {
        self.poly = pid | SWIP_EVICT_BIT;
    }

    pub fn mark_cool(&mut self) {
        assert!(self.is_hot());
        self.poly |= SWIP_COOL_BIT;
    }

    pub fn mark_evict(&mut self) {
        self.poly |= SWIP_EVICT_BIT;
    }

    /// when load page from page file, we will set Swip to evict and Swip::poly is page id allocated
    /// by `Partition`
    pub fn as_pid(&self) -> u64 {
        #[cfg(not(debug_assertions))]
        assert!(self.is_evict());
        self.poly & SWIP_EVICT_MASK
    }

    pub fn as_ptr_masked<T>(&self) -> &mut T {
        unsafe { std::mem::transmute(self.poly & SWIP_HOT_MASK) }
    }

    pub fn as_ptr<T>(&self) -> &mut T {
        unsafe { std::mem::transmute(self.poly) }
    }
}

#[cfg(test)]
mod tests {
    use super::Swip;

    #[test]
    fn test_convertion() {
        let a = Box::into_raw(Box::new(233));
        let swip = Swip::from_ptr(a);

        let ptr: *mut i32 = swip.as_ptr();
        unsafe { *ptr = 666 };

        assert_eq!(unsafe { *a }, 666);

        let pid = swip.as_pid();
        let swip = Swip::from_pid(pid);

        let ptr: *mut i32 = swip.as_ptr();
        unsafe { *ptr = 777 };

        assert_eq!(unsafe { *a }, 777);

        unsafe {
            drop(Box::from_raw(a));
        }
    }

    #[test]
    fn test_mask() {
        let a = Box::into_raw(Box::new(233));
        let mut swip = Swip::from_ptr(a);

        assert!(swip.is_hot());

        swip.mark_cool();
        assert!(swip.is_cool());

        swip.mark_evict();
        assert!(swip.is_evict());

        let a: *mut i32 = swip.as_ptr_masked();

        unsafe {
            drop(Box::from_raw(a));
        }
    }
}
