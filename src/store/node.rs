use crate::utils::byte_array::ByteArray;
use std::cmp::Ordering;

#[derive(Copy, Clone)]
#[repr(C)]
pub struct PageStatus {
    flag: u8,
}

impl PageStatus {
    fn cool() -> Self {
        Self { flag: 3 }
    }

    fn warm() -> Self {
        Self { flag: 7 }
    }

    fn hot() -> Self {
        Self { flag: 13 }
    }

    fn mark_cool(&mut self) {
        self.flag = Self::cool().flag
    }

    fn mark_wam(&mut self) {
        self.flag = Self::warm().flag
    }

    fn mark_hot(&mut self) {
        self.flag = Self::hot().flag
    }
}

#[derive(Copy, Clone)]
#[repr(C)]
pub struct NodeHeader {
    pub pid: u64,
    pub lsn: u64,
    pub rec_lsn: u64,
    pub status: PageStatus,
    pub dirty: bool,
    /// the following is page's metadata
    pub elems: u16,
    pub page_size: u32,
    pub slot_offset: u32,
    pub data_offset: u32,
}

const NODE_HEADER_SIZE: usize = size_of::<NodeHeader>();

impl NodeHeader {
    fn new(page_size: u32) -> Self {
        Self {
            pid: 0,
            lsn: 0,
            rec_lsn: 0,
            status: PageStatus::cool(),
            dirty: false,
            elems: 0,
            page_size,
            slot_offset: NODE_HEADER_SIZE as u32,
            data_offset: page_size,
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
#[repr(C)]
pub struct Slot {
    off: u32,
    sep: u16, // key value separate pos
    size: u16,
}

const SLOT_INFO_SIZE: usize = size_of::<Slot>();
const _: () = assert!(SLOT_INFO_SIZE == 8, "not equal");

impl Slot {
    fn value_len(&self) -> usize {
        self.size as usize - self.sep as usize
    }

    //                                 off
    // +---------+------+--------+-----+
    // | value   | key  | value  | key |
    // +---------+------+--------+-----+
    fn value_off(&self) -> isize {
        (self.off - self.size as u32) as isize
    }

    fn key_off(&self) -> isize {
        (self.off - self.sep as u32) as isize
    }

    fn key_len(&self) -> usize {
        self.sep as usize
    }

    fn offset(&self) -> isize {
        (self.off - self.size as u32) as isize
    }

    fn len(&self) -> usize {
        self.size as usize
    }
}

/// the node is `slotted page` representation, the `slot` is ordered by key and the real key is not
/// ordered in `page`, they are mapped by key's comparator   
/// page is dynamic sized with fixed size of elements, when a page can't hold new key-value pair, it
/// will split, when a page has no elements after remove, it will merge
pub struct Node {
    raw: ByteArray,
    /// new created Node has 0 cnt, when restore from log, it will increase in every insert
    cnt: i32,
    // data offset grow from tail to head, while slot offset grow from head to tail, when they are
    // equal, there's no free space left in page
    data_off: u32,
    slot_off: u32,
}

impl Node {
    pub fn from(raw: *mut u8, size: usize) -> Self {
        assert!(size > NODE_HEADER_SIZE);
        Self {
            raw: ByteArray::new(raw, size),
            cnt: 0,
            slot_off: NODE_HEADER_SIZE as u32,
            data_off: size as u32,
        }
    }

    pub fn header(&self) -> &mut NodeHeader {
        let slice: &mut [NodeHeader] = self.raw.to_mut_slice(0, 1);
        &mut slice[0]
        // unsafe { &mut *(self.raw.offset(NODE_HEADER_SIZE as isize) as *mut NodeHeader) }
    }

    fn raw_header(&self) -> &mut [u8] {
        self.raw.to_mut_slice(0, NODE_HEADER_SIZE)
    }

    pub fn elem_cnt(&self) -> usize {
        self.cnt as usize
    }

    pub fn raw_data(&mut self) -> ByteArray {
        self.raw
    }

    fn slot(&self, pos: usize) -> Slot {
        let offset = NODE_HEADER_SIZE + pos * SLOT_INFO_SIZE;
        unsafe {
            let slot = self.raw.offset(offset as isize) as *const Slot;
            *slot
        }
    }

    fn slot_array(&self, cnt: usize) -> &mut [Slot] {
        self.raw.to_mut_slice(NODE_HEADER_SIZE as isize, cnt)
    }

    fn key_at_mut(&self, pos: usize) -> &mut [u8] {
        let slot = self.slot(pos);
        self.raw.to_mut_slice(slot.key_off(), slot.key_len())
    }

    fn key_at(&self, pos: usize) -> &[u8] {
        let slot = self.slot(pos);
        self.raw.to_slice(slot.key_off(), slot.key_len())
    }

    fn value_at_mut(&self, pos: usize) -> &mut [u8] {
        let slot = self.slot(pos);
        self.raw.to_mut_slice(slot.value_off(), slot.value_len())
    }

    fn value_at(&self, pos: usize) -> &[u8] {
        let slot = self.slot(pos);
        self.raw.to_slice(slot.value_off(), slot.value_len())
    }

    fn key_value_at(&self, slot: Slot) -> &[u8] {
        self.raw.to_slice(slot.offset(), slot.len())
    }

    fn has_space(&self, size: usize) -> bool {
        let req_size = (SLOT_INFO_SIZE + size) as u32;
        self.data_off - self.slot_off >= req_size
    }

    /// a naive implementation
    /// TODO: replace to `memmove`
    fn compact(&mut self) {
        let mut v = vec![0u8; self.raw.len()];
        let tmp = v.as_mut_ptr();
        let tmp = ByteArray::new(tmp, self.raw.len());

        let hdr = tmp.to_mut_slice(0, NODE_HEADER_SIZE);
        hdr.copy_from_slice(self.raw_header());

        let slots = tmp.to_mut_slice(NODE_HEADER_SIZE as isize, self.cnt as usize);
        let old_slots = self.slot_array(self.cnt as usize);
        slots.copy_from_slice(old_slots);

        let mut offset = self.raw.len() as u32;

        for (i, s) in old_slots.iter().enumerate() {
            let beg = offset - s.size as u32;
            let kv = tmp.to_mut_slice(beg as isize, s.size as usize);

            kv.copy_from_slice(self.key_value_at(*s));
            // update meta
            slots[i].off = offset;
            offset = beg;
        }

        self.data_off = offset;
        self.raw.copy(v.as_slice());
    }

    /// NOTE: no duplication is allowed
    pub fn search(&self, key: &[u8]) -> (bool, usize) {
        let cnt = self.cnt as usize;
        let mut lo: usize = 0;
        let mut hi: usize = cnt;

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match self.key_at(mid).cmp(key) {
                Ordering::Less => {
                    lo = mid + 1;
                }
                Ordering::Greater => {
                    hi = mid;
                }
                Ordering::Equal => return (true, mid),
            }
        }

        (false, lo)
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        let (ok, _) = self.search(key);
        ok
    }

    /// NOTE: this occurs in `consolidate` procedure, we can hold a lock at present, and the lock is
    /// managed by caller
    pub fn insert(&mut self, key: &[u8], val: &[u8]) -> bool {
        let (exist, pos) = self.search(key);

        if exist {
            return false;
        }
        let size = (key.len() + val.len()) as u16;

        // reserve one slot for insert
        let slots = self.slot_array(self.cnt as usize + 1);
        // shift one slot right from pos
        slots.copy_within(pos..self.cnt as usize, pos + 1);

        slots[pos].off = self.data_off;
        slots[pos].size = size;
        slots[pos].sep = key.len() as u16;

        self.key_at_mut(pos).copy_from_slice(key);
        self.value_at_mut(pos).copy_from_slice(val);

        // update stats
        self.cnt += 1;
        self.data_off -= size as u32;
        self.slot_off += SLOT_INFO_SIZE as u32;

        true
    }

    /// NOTE: remove simply overwrite slot, and not clean key-value space, the obsoleted data will be
    /// compacted in insert procedure when space is not enough for insert new key-value pair
    pub fn remove(&mut self, key: &[u8]) -> bool {
        let (ok, pos) = self.search(key);

        if ok {
            let cnt = self.cnt as usize;
            let pos = pos as usize;
            let slots = self.slot_array(cnt);
            slots.copy_within(pos + 1..cnt, pos);

            self.cnt -= 1;
            self.slot_off -= SLOT_INFO_SIZE as u32;
        }
        ok
    }

    /// split always create a node right to current node
    fn split(&mut self) -> Node {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use crate::store::node::Node;

    #[test]
    fn test_node() {
        let size = 4096;
        let mut page = vec![0u8; size];
        let page = page.as_mut_ptr();
        let mut node = Node::from(page, size);

        let hdr = node.header();
        hdr.pid = 1;

        let new_hdr = node.header();
        assert_eq!(new_hdr.pid, 1);
    }

    #[test]
    fn test_node_kv() {
        let size = 4096;
        let mut v = vec![0u8; size];
        let page = v.as_mut_ptr();
        let mut node = Node::from(page, size);
        let (k1, v1, k2, v2) = (
            "mo".as_bytes(),
            "ha".as_bytes(),
            "elder".as_bytes(),
            "+1s".as_bytes(),
        );

        node.insert(k1, v1);

        let r = node.search(k1);
        assert!(r.0);

        assert_eq!(node.value_at_mut(r.1), v1);

        node.insert(k2, v2);

        let r = node.search(k2);
        assert!(r.0);

        assert_eq!(node.value_at_mut(r.1), v2);

        let arr = node.slot_array(node.elem_cnt());
        assert_eq!(node.slot(0), arr[0]);
        assert_eq!(node.slot(1), arr[1]);

        node.remove(k2);
        assert!(!node.contains(k2));
        node.remove(k1);
        assert!(!node.contains(k1));
    }

    #[test]
    fn test_compact() {
        let size = 4096;
        let mut v = vec![0u8; size];
        let page = v.as_mut_ptr();
        let mut node = Node::from(page, size);

        let (k1, v1, k2, v2, k3, v3) = (
            "mo".as_bytes(),
            "ha".as_bytes(),
            "elder".as_bytes(),
            "+1s".as_bytes(),
            "young".as_bytes(),
            "naive".as_bytes(),
        );

        node.insert(k1, v1);
        node.insert(k3, v3);
        node.insert(k2, v2);

        assert!(node.contains(k1));
        assert!(node.contains(k2));
        assert!(node.contains(k3));

        node.remove(k2);

        node.compact();

        let (s1, s3) = (node.search(k1), node.search(k3));

        assert!(s1.0);
        assert!(s3.0);
        assert_eq!(node.value_at(s1.1), v1);
        assert_eq!(node.value_at(s3.1), v3);
    }
}
