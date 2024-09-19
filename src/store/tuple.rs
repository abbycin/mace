struct Tuple {
    tx_id: u64,
    format: u16,
    worker_id: u16,
    command_id: u32,
}

struct ChainedTuple {
    tuple: Tuple,
    oldest_tx: u32,
    deleted: u16,
    update_cnt: u16,
    // payload lives here
}

const CHAINED_TUPLE: u16 = 3;
const FLAT_TUPLE: u16 = 7;
const INVALID_COMMAND: u32 = (1u32 << 31) - 1;

impl ChainedTuple {
    pub fn from_raw<'a>(raw: *mut u8) -> &'a mut Self {
        unsafe { &mut *(raw as *mut ChainedTuple) }
    }

    pub fn build(&mut self, worker_id: u16, tx_id: u64, val: &[u8]) {
        self.tuple.format = CHAINED_TUPLE;
        self.tuple.command_id = INVALID_COMMAND;
        self.tuple.tx_id = tx_id;
        self.tuple.worker_id = worker_id;

        unsafe {
            std::ptr::copy(val.as_ptr(), self.payload(), val.len());
        }
    }

    pub fn write_payload(&self, dst: *mut u8, size: usize) {
        unsafe {
            std::ptr::copy(self.payload(), dst, size);
        }
    }
    fn payload(&self) -> *mut u8 {
        unsafe {
            let ptr: *mut u8 = std::mem::transmute(self);
            ptr.offset(size_of::<Self>() as isize)
        }
    }
}
