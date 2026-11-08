//! golden byte fixtures for the v1 persistent format
//!
//! `docs/format.md` specifies the bytes Mace writes. The fixtures below build those bytes by
//! hand from the specification and then hand them to the real readers, so a layout change fails
//! here before it can silently reinterpret an existing database. They are deliberately not
//! generated from the writer: a round trip through the same code that wrote the bytes would
//! agree with any layout the writer happens to use.
//!
//! The size and offset pins live next to the structs they protect; this module covers the byte
//! values, the reader branches and the rejection samples.

use crc32c::crc32c;

use crate::{
    BucketOptions, Mace, OpCode, Options, PersistedBucketOptions, RandomPath,
    cc::wal::{
        EntryType, PayloadType, WalAbort, WalBegin, WalCheckpoint, WalCommit, WalUpdate, ptr_to,
        wal_record_sz,
    },
    map::data::{FileBuilder, FileFooter, FileVersion, MetaReader, new_reader},
    meta::{
        BUCKET_BLOB_STAT, BUCKET_DATA_STAT, BUCKET_FRONTIER, BUCKET_METAS, BUCKET_MISC,
        BUCKET_OBSOLETE_BLOB, BUCKET_OBSOLETE_DATA, BUCKET_PENDING_DEL, BUCKET_VERSION,
        BucketDurableFrontier, BucketMeta, CURRENT_VERSION, Delete, FileKind, IMetaCodec,
        IntervalPair, MetaKind, MetaOp, MetaRecord, OPTIONS_KEY, ORPHAN_BLOB_MARKER_PREFIX,
        ORPHAN_DATA_MARKER_PREFIX, PageTable, PersistStat, SEQUENCES_KEY, Sequences, StatInner,
        VERSION_KEY, WAL_RECYCLE_PREFIX, WalRecycleState, blob_interval_name, data_interval_name,
        page_table_name,
    },
    types::{
        data::{HistRef, Index, IntlKey, Key, Val, Ver},
        header::{NodeType, TagFlag, TagKind},
        refbox::BoxRef,
        traits::{IAsSlice, IBoxHeader, IDecode, IHeader},
    },
    utils::{
        NULL_ADDR, NULL_PID,
        compress::{CompressorPool, DecompressorPool},
        data::{AddrPair, Interval, Position, Reloc},
        options::PersistedOptions,
    },
};

// ---------------------------------------------------------------------------
// independent encoders
// ---------------------------------------------------------------------------

fn put_u16(dst: &mut [u8], off: usize, x: u16) {
    dst[off..off + 2].copy_from_slice(&x.to_le_bytes());
}

fn put_u32(dst: &mut [u8], off: usize, x: u32) {
    dst[off..off + 4].copy_from_slice(&x.to_le_bytes());
}

fn put_u64(dst: &mut [u8], off: usize, x: u64) {
    dst[off..off + 8].copy_from_slice(&x.to_le_bytes());
}

/// the bytes of a plain struct, for the types that do not implement `IAsSlice`
fn bytes_of<T>(x: &T) -> &[u8] {
    unsafe { std::slice::from_raw_parts((x as *const T).cast(), size_of::<T>()) }
}

/// the documented unsigned LEB128-style varint: seven bits per byte, low group first
fn varint(mut x: usize) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let byte = (x & 0x7f) as u8;
        x >>= 7;
        if x == 0 {
            out.push(byte);
            return out;
        }
        out.push(byte | 0x80);
    }
}

/// `Varint32(len) | Ver | raw`
fn leaf_key(raw: &[u8], ver: Ver) -> Vec<u8> {
    let mut out = varint(raw.len() + Ver::len());
    out.extend_from_slice(&ver.txid.to_le_bytes());
    out.extend_from_slice(&ver.cmd.to_le_bytes());
    out.extend_from_slice(raw);
    out
}

/// `Varint32(len) | raw`, no `Ver`
fn intl_key(raw: &[u8]) -> Vec<u8> {
    let mut out = varint(raw.len());
    out.extend_from_slice(raw);
    out
}

fn ver_bytes(ver: Ver) -> [u8; 12] {
    let mut out = [0u8; 12];
    out[..8].copy_from_slice(&ver.txid.to_le_bytes());
    out[8..].copy_from_slice(&ver.cmd.to_le_bytes());
    out
}

/// the `Val` envelope with an optional history reference
fn val_envelope(
    flags: u8,
    group_id: u8,
    data_len: usize,
    hist: Option<HistRef>,
    body: &[u8],
) -> Vec<u8> {
    let mut out = vec![0u8; 6];
    out[0] = flags | if hist.is_some() { Val::HIST_BIT } else { 0 };
    out[1] = group_id;
    put_u32(&mut out, 2, data_len as u32);
    if let Some(hist) = hist {
        out.extend_from_slice(&hist.page_addr.to_le_bytes());
        out.extend_from_slice(&hist.slot.to_le_bytes());
        out.extend_from_slice(&hist.count.to_le_bytes());
    }
    out.extend_from_slice(body);
    out
}

fn inline_val(flags: u8, group_id: u8, record: &[u8], hist: Option<HistRef>) -> Vec<u8> {
    // `Record` = group byte | payload
    let mut body = vec![group_id];
    body.extend_from_slice(record);
    val_envelope(flags, group_id, body.len(), hist, &body)
}

fn remote_val(
    flags: u8,
    group_id: u8,
    record_len: usize,
    hist: Option<HistRef>,
    addr: u64,
) -> Vec<u8> {
    val_envelope(flags, group_id, record_len, hist, &addr.to_le_bytes())
}

/// build a base payload with hand-written header, slots, boundaries and entries
fn base_payload(
    lo: &[u8],
    hi: &[u8],
    prefix_len: u32,
    is_index: bool,
    has_multiple_versions: bool,
    entries: &[Vec<u8>],
) -> Vec<u8> {
    let elems = entries.len();
    let mut payload = vec![0u8; 40 + elems * 4];
    payload.extend_from_slice(lo);
    payload.extend_from_slice(hi);
    let mut slots = Vec::with_capacity(elems);
    for entry in entries {
        slots.push(payload.len() as u32);
        payload.extend_from_slice(entry);
    }
    let size = payload.len() as u32;
    put_u16(&mut payload, 0, elems as u16);
    put_u16(&mut payload, 2, 0); // split_elems
    put_u32(&mut payload, 4, size);
    put_u64(&mut payload, 8, 0); // right_sibling
    put_u64(&mut payload, 16, 0); // merging_child
    put_u32(&mut payload, 24, lo.len() as u32);
    put_u32(&mut payload, 28, hi.len() as u32);
    put_u32(&mut payload, 32, prefix_len);
    payload[36] = 0; // merging
    payload[37] = u8::from(is_index);
    payload[38] = u8::from(has_multiple_versions);
    payload[39] = 0; // writer-guaranteed zero, no reader validation
    for (i, off) in slots.into_iter().enumerate() {
        put_u32(&mut payload, 40 + i * 4, off);
    }
    payload
}

/// wrap a payload in a persisted box and decode it back through the production reader
fn boxed(
    kind: TagKind,
    node_type: NodeType,
    pid: u64,
    addr: u64,
    link: u64,
    payload: &[u8],
) -> BoxRef {
    let mut b = BoxRef::alloc(payload.len() as u32, addr);
    {
        let h = b.header_mut();
        h.kind = kind;
        h.node_type = node_type;
        h.pid = pid;
        h.link = link;
    }
    b.data_slice_mut::<u8>().copy_from_slice(payload);
    b
}

/// the stored bytes of a box, exactly as a file frame holds them
fn box_bytes(b: &BoxRef) -> Vec<u8> {
    let mut out = Vec::new();
    b.with_persisted_parts(|head, body| {
        out.extend_from_slice(head);
        if let Some(body) = body {
            out.extend_from_slice(body);
        }
    });
    out
}

/// decode stored frame bytes through the production v1 frame reader
fn frame_of(stored: &[u8], addr: u64) -> Result<BoxRef, crate::OpCode> {
    let alloc = stored.len() as u32 + 4;
    let mut b = BoxRef::alloc_exact(alloc, addr);
    b.load_slice(stored.len()).copy_from_slice(stored);
    match b.decode_persisted_v1_in_place(addr, stored.len()) {
        Ok(()) => Ok(b),
        Err(err) => {
            // the stored bytes overlay the in-memory header, so a rejected frame leaves
            // header fields that no longer describe the allocation; restore them before
            // drop, exactly as the production reader does
            b.header_mut().payload_size = alloc - BoxRef::HDR_LEN as u32;
            b.header_mut().total_size = alloc;
            Err(err)
        }
    }
}

// ---------------------------------------------------------------------------
// data/blob file tables
// ---------------------------------------------------------------------------

#[test]
fn file_footer_and_tables_have_golden_bytes() {
    let footer = FileFooter::new(2, 1, 0x1122_3344, 0x5566_7788);
    let bytes = footer.as_slice();
    assert_eq!(bytes.len(), 24);
    assert_eq!(
        bytes,
        &[
            1, 0, // version
            0, 0, 0, 0, 0, 0, // padding
            2, 0, 0, 0, // nr_reloc
            1, 0, 0, 0, // nr_intervals
            0x44, 0x33, 0x22, 0x11, // reloc_crc
            0x88, 0x77, 0x66, 0x55, // interval_crc
        ]
    );
    assert_eq!(&bytes[8..12], &2u32.to_le_bytes(), "nr_reloc offset");

    let reloc = Reloc {
        off: 0x0102,
        raw_len: 0x0304,
        compressed_len: 0,
        seq: 7,
        crc: 0xAABB_CCDD,
    };
    let bytes = bytes_of(&reloc);
    assert_eq!(bytes.len(), 24);
    assert_eq!(
        bytes,
        &[
            0x02, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // off (usize)
            0x04, 0x03, 0x00, 0x00, // raw_len
            0x00, 0x00, 0x00, 0x00, // compressed_len
            0x07, 0x00, 0x00, 0x00, // seq
            0xDD, 0xCC, 0xBB, 0xAA, // crc
        ]
    );

    let pair = AddrPair::new(42, 0x0102, 0x0304, 0, 7, 0xAABB_CCDD);
    let bytes = pair.as_slice();
    assert_eq!(bytes.len(), 32);
    assert_eq!(&bytes[0..8], &42u64.to_le_bytes(), "AddrPair.key offset");
    assert_eq!(&bytes[8..], bytes_of(&reloc));

    let interval = Interval::new(0x0102_0304_0506_0708, 0x1112_1314_1516_1718);
    assert_eq!(
        interval.as_slice(),
        &[
            0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, // lo
            0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11, // hi
        ]
    );

    let pos = Position::new(3, 4);
    assert_eq!(
        bytes_of(&pos),
        &[3, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0]
    );
}

// ---------------------------------------------------------------------------
// Bw-Tree headers and nodes
// ---------------------------------------------------------------------------

#[test]
fn record_headers_have_golden_bytes() {
    use crate::types::header::{DiskBaseHeader, DiskBoxHeader, DiskDeltaHeader, DiskRemoteHeader};

    let hdr = DiskBoxHeader {
        kind: TagKind::Base as u8,
        node_type: NodeType::Leaf as u8,
        flag: 1,
        group: 3,
        total_size: 0x1234,
        payload_size: 0x0F04,
        pid: 0x0102_0304_0506_0708,
        txid: 0x1112_1314_1516_1718,
        addr: 0x2122_2324_2526_2728,
        link: 0x3132_3334_3536_3738,
        lsn: Position::new(0x41, 0x42),
    };
    let bytes = hdr.as_slice();
    assert_eq!(bytes.len(), 60);
    assert_eq!(&bytes[0..4], &[2, 1, 1, 3]);
    assert_eq!(&bytes[4..8], &0x1234u32.to_le_bytes());
    assert_eq!(&bytes[8..12], &0x0F04u32.to_le_bytes());
    assert_eq!(&bytes[12..20], &hdr.pid.to_le_bytes(), "pid offset");
    assert_eq!(&bytes[44..52], &0x41u64.to_le_bytes(), "lsn offset");
    assert_eq!(&bytes[52..60], &0x42u64.to_le_bytes());

    let base = DiskBaseHeader {
        elems: 5,
        split_elems: 0,
        size: 249,
        right_sibling: 0x0102,
        merging_child: 0,
        lo_len: 2,
        hi_len: 0,
        prefix_len: 2,
        merging: 0,
        is_index: 0,
        has_multiple_versions: 1,
        padding: 0,
    };
    let bytes = base.as_slice();
    assert_eq!(bytes.len(), 40);
    assert_eq!(&bytes[0..2], &5u16.to_le_bytes());
    assert_eq!(&bytes[4..8], &249u32.to_le_bytes(), "size offset");
    assert_eq!(&bytes[24..28], &2u32.to_le_bytes(), "lo_len offset");
    assert_eq!(&bytes[32..36], &2u32.to_le_bytes(), "prefix_len offset");
    assert_eq!(&bytes[38..40], &[1, 0]);

    let delta = DiskDeltaHeader { klen: 7, vlen: 9 };
    assert_eq!(delta.as_slice(), &[7, 0, 0, 0, 9, 0, 0, 0]);
    let remote = DiskRemoteHeader { size: 11 };
    assert_eq!(remote.as_slice(), &[11, 0, 0, 0, 0, 0, 0, 0]);
}

#[test]
fn leaf_page_bytes_cover_every_val_branch() {
    let hist = HistRef::new(0x0202_0303_0404_0505, 1, 3);
    let entries = vec![
        // inline
        [
            leaf_key(b"er/alpha", Ver::new(7, 3)),
            inline_val(0, 4, b"hello", None),
        ]
        .concat(),
        // tombstone: empty `Record` payload
        [
            leaf_key(b"er/beta", Ver::new(8, 1)),
            inline_val(Val::DEL_BIT, 4, b"", None),
        ]
        .concat(),
        // remote, with a history reference
        [
            leaf_key(b"er/delta", Ver::new(9, 2)),
            remote_val(Val::REMOTE_BIT, 5, 10, Some(hist), 0x0A0B_0C0D_0E0F_1011),
        ]
        .concat(),
        // inline, with a history reference
        [
            leaf_key(b"er/epsilon", Ver::new(10, 4)),
            inline_val(Val::HIST_BIT, 6, b"hi", Some(hist)),
        ]
        .concat(),
        // merge operand
        [
            leaf_key(b"er/gamma", Ver::new(11, 5)),
            inline_val(Val::MERGE_BIT, 7, b"oper", None),
        ]
        .concat(),
    ];
    let payload = base_payload(b"us", b"", 2, false, true, &entries);
    assert_eq!(payload.len(), 249, "the leaf fixture is a fixed shape");
    let page = boxed(TagKind::Base, NodeType::Leaf, 77, 0x10, NULL_ADDR, &payload);
    let base = page.view().as_base();
    assert_eq!({ base.header().elems }, 5);
    assert_eq!({ base.header().size }, 249);
    assert_eq!({ base.header().prefix_len }, 2);
    assert_eq!(base.lo(), b"us");
    assert_eq!(base.hi(), None);

    // slots are absolute offsets from the base header start, not from the slot table
    let sst = base.sst::<Key>();
    assert_eq!(&payload[40..44], &62u32.to_le_bytes());
    assert_eq!(&payload[44..48], &95u32.to_le_bytes());

    let (ver, val) = sst.ver_val_at(0);
    assert_eq!(ver, Ver::new(7, 3));
    assert_eq!(val.data_size(), 6);
    assert_eq!(val.inline_data(), Some(&b"hello"[..]));
    assert!(!val.has_hist());
    assert!(!val.is_tombstone());
    assert!(!val.is_merge());
    assert_eq!(val.get_remote(), NULL_ADDR);

    let (ver, val) = sst.ver_val_at(1);
    assert_eq!(ver, Ver::new(8, 1));
    assert!(val.is_tombstone());
    assert_eq!(val.data_size(), 1);
    assert_eq!(val.inline_data(), Some(&b""[..]));

    let (ver, val) = sst.ver_val_at(2);
    assert_eq!(ver, Ver::new(9, 2));
    assert_eq!(val.data_size(), 10);
    assert_eq!(val.get_remote(), 0x0A0B_0C0D_0E0F_1011);
    assert_eq!(val.get_hist(), Some(hist));
    assert!(val.inline_data().is_none());

    let (ver, val) = sst.ver_val_at(3);
    assert_eq!(ver, Ver::new(10, 4));
    assert_eq!(val.get_hist(), Some(hist));
    assert_eq!(val.inline_data(), Some(&b"hi"[..]));

    let (ver, val) = sst.ver_val_at(4);
    assert_eq!(ver, Ver::new(11, 5));
    assert!(val.is_merge());
    assert_eq!(val.inline_data(), Some(&b"oper"[..]));
}

#[test]
fn internal_page_bytes_are_indexes_without_versions() {
    let entries = vec![
        [
            intl_key(b"er/a"),
            0x0102_0304_0506_0708u64.to_le_bytes().to_vec(),
        ]
        .concat(),
        [
            intl_key(b"er/b"),
            0x1112_1314_1516_1718u64.to_le_bytes().to_vec(),
        ]
        .concat(),
    ];
    let payload = base_payload(b"us", b"", 2, true, false, &entries);
    let page = boxed(TagKind::Base, NodeType::Intl, 88, 0x20, NULL_ADDR, &payload);
    let base = page.view().as_base();
    assert!({ base.header().is_index });
    let sst = base.sst::<IntlKey>();
    assert_eq!(sst.pid_at(0), 0x0102_0304_0506_0708);
    assert_eq!(sst.pid_at(1), 0x1112_1314_1516_1718);
    let (k, v) = sst.kv_at::<Index>(1);
    assert_eq!(k.raw, b"er/b");
    assert_eq!(v.pid, 0x1112_1314_1516_1718);
    assert_eq!(Index::len(), 8);
}

#[test]
fn deltas_and_remote_records_have_golden_bytes() {
    // leaf delta: `DiskDeltaHeader | leaf key | Val`
    let key = leaf_key(b"er/alpha", Ver::new(7, 3));
    let val = inline_val(0, 4, b"hello", None);
    let mut payload = Vec::new();
    payload.extend_from_slice(&(key.len() as u32).to_le_bytes());
    payload.extend_from_slice(&(val.len() as u32).to_le_bytes());
    payload.extend_from_slice(&key);
    payload.extend_from_slice(&val);
    let delta = boxed(TagKind::Delta, NodeType::Leaf, 77, 0x30, 0x29, &payload);
    let stored = box_bytes(&delta);
    let back = frame_of(&stored, 0x30).expect("leaf delta must decode");
    let view = back.view().as_delta();
    assert_eq!({ view.header().klen }, key.len() as u32);
    assert_eq!({ view.header().vlen }, val.len() as u32);
    assert_eq!(view.key(), &key[..]);
    assert_eq!(view.val().inline_data(), Some(&b"hello"[..]));

    // internal delta: `DiskDeltaHeader | internal key | Index`, the key has no `Ver`
    let key = intl_key(b"er/b");
    let mut payload = Vec::new();
    payload.extend_from_slice(&(key.len() as u32).to_le_bytes());
    payload.extend_from_slice(&8u32.to_le_bytes());
    payload.extend_from_slice(&key);
    payload.extend_from_slice(&0x1112_1314_1516_1718u64.to_le_bytes());
    let delta = boxed(
        TagKind::Delta,
        NodeType::Intl,
        88,
        0x31,
        NULL_ADDR,
        &payload,
    );
    let stored = box_bytes(&delta);
    let back = frame_of(&stored, 0x31).expect("internal delta must decode");
    let view = back.view().as_delta();
    let k = IntlKey::decode_from(view.key());
    let v = Index::decode_from(view.index());
    assert_eq!(k.raw, b"er/b");
    assert_eq!(v.pid, 0x1112_1314_1516_1718);

    // remote box: `DiskRemoteHeader.size | Record`
    let record = [vec![9u8], b"payload".to_vec()].concat();
    let mut payload = Vec::new();
    payload.extend_from_slice(&(record.len() as u64).to_le_bytes());
    payload.extend_from_slice(&record);
    let remote = boxed(
        TagKind::Remote,
        NodeType::Leaf,
        NULL_PID,
        0x40,
        NULL_ADDR,
        &payload,
    );
    let stored = box_bytes(&remote);
    let back = frame_of(&stored, 0x40).expect("remote box must decode");
    assert_eq!({ back.view().as_remote().header().size }, record.len());
    assert_eq!(back.view().as_remote().raw(), &record[..]);
}

#[test]
fn sibling_history_pages_hold_ver_and_val_without_a_length_prefix() {
    // `count` spans both pages: slot 1 on the first page, then slot 0 on the linked page
    let older = |txid: u64| inline_val(0, 4, &txid.to_le_bytes()[..1], None);
    let first = base_payload(
        b"",
        b"",
        0,
        false,
        true,
        &[
            [ver_bytes(Ver::new(5, 1)).to_vec(), older(5)].concat(),
            [ver_bytes(Ver::new(4, 2)).to_vec(), older(4)].concat(),
        ],
    );
    let second = base_payload(
        b"",
        b"",
        0,
        false,
        true,
        &[[ver_bytes(Ver::new(3, 1)).to_vec(), older(3)].concat()],
    );
    let mut tail = boxed(
        TagKind::Base,
        NodeType::Leaf,
        NULL_PID,
        0x51,
        NULL_ADDR,
        &second,
    );
    tail.header_mut().flag = TagFlag::Sibling;
    let mut head = boxed(
        TagKind::Base,
        NodeType::Leaf,
        NULL_PID,
        0x50,
        tail.header().addr,
        &first,
    );
    head.header_mut().flag = TagFlag::Sibling;
    // the persisted flag is 2 only for history pages
    let stored = box_bytes(&head);
    assert_eq!(stored[2], 2, "sibling flag");
    assert_eq!(stored[12..20], NULL_PID.to_le_bytes(), "pid is null");
    let head = frame_of(&stored, 0x50).expect("sibling page must decode");
    let base = head.view().as_base();
    assert_eq!({ base.header().elems }, 2);
    assert_eq!({ base.header().lo_len }, 0);
    assert_eq!({ base.box_header().link }, 0x51);
    let sst = base.sst::<Ver>();
    assert_eq!(sst.kv_at::<Val>(0).0, Ver::new(5, 1));
    assert_eq!(sst.kv_at::<Val>(1).0, Ver::new(4, 2));

    let tail = frame_of(&box_bytes(&tail), 0x51).expect("sibling tail must decode");
    let sst = tail.view().as_base().sst::<Ver>();
    assert_eq!(sst.kv_at::<Val>(0).0, Ver::new(3, 1));
}

// ---------------------------------------------------------------------------
// metadata values and keys
// ---------------------------------------------------------------------------

#[test]
fn metadata_value_layouts_have_golden_bytes() {
    let interval = IntervalPair::new(0x0102, 0x0304, 0x0506, 0x0708);
    let mut bytes = [0u8; 32];
    interval.encode(&mut bytes);
    assert_eq!(&bytes[0..8], &0x0102u64.to_le_bytes());
    assert_eq!(&bytes[8..16], &0x0304u64.to_le_bytes());
    assert_eq!(&bytes[16..24], &0x0506u64.to_le_bytes());
    assert_eq!(&bytes[24..32], &0x0708u64.to_le_bytes());

    let inner = StatInner {
        file_id: 0x0102_0304_0506_0708,
        up1: 2,
        up2: 3,
        active_elems: 4,
        total_elems: 5,
        active_size: 0x0A,
        total_size: 0x0B,
        bucket_id: 0x0C,
    };
    let stat = PersistStat::from_parts(inner, vec![1, 4]);
    let mut bytes = vec![0u8; stat.packed_size()];
    stat.encode(&mut bytes);
    assert_eq!(bytes.len(), 8 + 56 + 8);
    assert_eq!(&bytes[0..4], &2u32.to_le_bytes(), "StatHdr.elems");
    assert_eq!(&bytes[4..8], &64u32.to_le_bytes(), "StatHdr.size");
    assert_eq!(&bytes[8..16], &inner.file_id.to_le_bytes());
    assert_eq!(&bytes[16..24], &2u64.to_le_bytes(), "up1");
    assert_eq!(&bytes[24..32], &3u64.to_le_bytes(), "up2");
    assert_eq!(&bytes[32..36], &4u32.to_le_bytes(), "active_elems");
    assert_eq!(&bytes[36..40], &5u32.to_le_bytes(), "total_elems");
    assert_eq!(&bytes[40..48], &0x0Au64.to_le_bytes(), "active_size");
    assert_eq!(&bytes[48..56], &0x0Bu64.to_le_bytes(), "total_size");
    assert_eq!(
        &bytes[56..64],
        &inner.bucket_id.to_le_bytes(),
        "bucket_id at 48"
    );
    assert_eq!(
        &bytes[64..],
        &[1, 0, 0, 0, 4, 0, 0, 0],
        "inactive sequence mask"
    );

    let mut lsn = [Position::MIN; 128];
    lsn[0] = Position::new(1, 2);
    lsn[3] = Position::new(4, 5);
    let frontier = BucketDurableFrontier::new(9, lsn);
    let mut bytes = vec![0u8; frontier.packed_size()];
    frontier.encode(&mut bytes);
    assert_eq!(bytes.len(), 8 + 128 * 16);
    assert_eq!(&bytes[0..8], &9u64.to_le_bytes());
    assert_eq!(&bytes[8..24], bytes_of(&Position::new(1, 2)));
    assert_eq!(&bytes[8 + 48..8 + 64], bytes_of(&Position::new(4, 5)));
    assert_eq!(
        &bytes.as_slice()[8 + 16..8 + 24],
        &[0u8; 8],
        "unused group is MIN"
    );

    let sequences = Sequences::default();
    let mut bytes = vec![0u8; sequences.packed_size()];
    sequences.encode(&mut bytes);
    assert_eq!(
        bytes,
        [vec![0u8; 16], 1u64.to_le_bytes().to_vec()].concat(),
        "next_file_id | next_bucket_id | oracle"
    );

    let recycle = WalRecycleState::intent(3, 0x0A, 0x0B);
    let mut bytes = vec![0u8; recycle.packed_size()];
    recycle.encode(&mut bytes);
    assert_eq!(
        bytes,
        [
            vec![3u8, 1, 0, 0, 0, 0, 0, 0], // group | stage | pad
            0x0Au64.to_le_bytes().to_vec(),
            0x0Bu64.to_le_bytes().to_vec(),
        ]
        .concat()
    );

    let mut options = PersistedBucketOptions::from(&BucketOptions::new());
    options.enable_backpressure = true;
    let meta = BucketMeta { id: 7, options };
    let mut bytes = vec![0u8; meta.packed_size()];
    meta.encode(&mut bytes);
    assert_eq!(bytes.len(), 56);
    assert_eq!(&bytes[0..8], &7u64.to_le_bytes());
    assert_eq!(&bytes[8..], bytes_of(&options), "options offset");
}

#[test]
fn metadata_keys_use_the_documented_encoding() {
    assert_eq!(VERSION_KEY, "current_version");
    assert_eq!(OPTIONS_KEY, "options");
    assert_eq!(SEQUENCES_KEY, "sequences");
    assert_eq!(BUCKET_VERSION, "version");
    assert_eq!(BUCKET_MISC, "misc");
    assert_eq!(BUCKET_METAS, "bucket_metas");
    assert_eq!(BUCKET_FRONTIER, "bucket_frontier");
    assert_eq!(BUCKET_PENDING_DEL, "pending_del");
    assert_eq!(BUCKET_DATA_STAT, "data_stat");
    assert_eq!(BUCKET_BLOB_STAT, "blob_stat");
    assert_eq!(BUCKET_OBSOLETE_DATA, "obsolete_data");
    assert_eq!(BUCKET_OBSOLETE_BLOB, "obsolete_blob");
    assert_eq!(CURRENT_VERSION, 1);

    // orphan and recycle markers are decimal ASCII, not LE u64 keys
    assert_eq!(ORPHAN_DATA_MARKER_PREFIX, "odf_");
    assert_eq!(ORPHAN_BLOB_MARKER_PREFIX, "obf_");
    assert_eq!(WAL_RECYCLE_PREFIX, "wrc_");
    assert_eq!(crate::meta::orphan_data_marker_key(12), b"odf_12".to_vec());
    assert_eq!(crate::meta::orphan_blob_marker_key(25), b"obf_25".to_vec());
    assert_eq!(crate::meta::wal_recycle_key(3), b"wrc_3".to_vec());

    // interval and page-table bucket names carry the decimal bucket id
    assert_eq!(page_table_name(5), "page_table_5");
    assert_eq!(data_interval_name(5), "data_interval_5");
    assert_eq!(blob_interval_name(5), "blob_interval_5");

    // a page table publishes one entry per key: LE u64 pid -> LE u64 address
    let mut table = PageTable::default();
    table.bucket_id = 5;
    table.add(9, 0x1122_3344_5566_7788);
    let mut ops = std::collections::BTreeMap::new();
    table.record(MetaKind::Map, &mut ops);
    let bucket = ops.get("page_table_5").expect("page table bucket");
    assert_eq!(bucket.len(), 1);
    match &bucket[0] {
        MetaOp::Put(key, value) => {
            assert_eq!(key, &9u64.to_le_bytes());
            assert_eq!(value, &0x1122_3344_5566_7788u64.to_le_bytes());
        }
        _ => panic!("a page table entry must be a put"),
    }

    // an obsolete marker is one empty value per file id, keyed by LE u64
    let mut ops = std::collections::BTreeMap::new();
    Delete::from(vec![7u64]).record(MetaKind::DataDelete, &mut ops);
    let bucket = ops
        .get(BUCKET_OBSOLETE_DATA)
        .expect("obsolete marker bucket");
    assert_eq!(bucket.len(), 1);
    match &bucket[0] {
        MetaOp::Put(key, value) => {
            assert_eq!(key, &7u64.to_le_bytes(), "marker key is a LE u64 file id");
            assert!(value.is_empty(), "a marker value is empty");
        }
        _ => panic!("an obsolete marker must be a put"),
    }
}

#[test]
fn persisted_options_json_is_canonical() {
    let canonical = r#"{"concurrent_write":16,"sync_on_write":true,"gc_timeout":60000,"checkpoint_nudge_ms":60000,"data_garbage_ratio":20,"gc_eager":true,"blob_file_size":268435456,"blob_garbage_ratio":50,"lru_capacity":268435456,"stat_mask_cache_count":16384,"data_handle_cache_capacity":128,"blob_handle_cache_capacity":128,"data_file_size":67108864,"wal_buffer_size":16777216,"max_ckpt_per_txn":1000000,"wal_file_size":67108864,"truncate_corrupted_wal":true}"#;
    assert_eq!(
        PersistedOptions::default().to_json().unwrap(),
        canonical.as_bytes()
    );
    // the canonical text parses back to the same values
    assert_eq!(
        PersistedOptions::from_json(canonical.as_bytes()).unwrap(),
        PersistedOptions::default()
    );

    // a missing field takes its default and an unknown field is ignored
    let partial = br#"{"concurrent_write":4,"unknown_field":true}"#;
    let parsed = PersistedOptions::from_json(partial).unwrap();
    assert_eq!(parsed.concurrent_write, 4);
    assert_eq!(
        parsed.sync_on_write,
        PersistedOptions::default().sync_on_write
    );

    // malformed JSON is corruption, not a defaulted value
    assert_eq!(
        PersistedOptions::from_json(b"not json").err(),
        Some(crate::OpCode::Corruption)
    );
}

// ---------------------------------------------------------------------------
// WAL records
// ---------------------------------------------------------------------------

fn update_record(key: &[u8], value: &[u8], sub_type: PayloadType) -> Vec<u8> {
    let body = if matches!(sub_type, PayloadType::Delete) {
        Vec::new()
    } else {
        [(value.len() as u32).to_le_bytes().to_vec(), value.to_vec()].concat()
    };
    let size = key.len() + body.len();
    let mut record = vec![0u8; 51];
    record[0] = EntryType::Update as u8;
    record[1] = sub_type as u8;
    put_u64(&mut record, 2, 7); // bucket_id
    record[10] = 3; // group_id
    put_u32(&mut record, 11, size as u32);
    put_u32(&mut record, 15, 11); // cmd_id
    put_u32(&mut record, 19, key.len() as u32);
    put_u64(&mut record, 23, 13); // txid
    put_u64(&mut record, 31, 0); // prev_id
    put_u64(&mut record, 39, 0); // prev_off
    record.extend_from_slice(key);
    record.extend_from_slice(&body);
    let crc = crc32c(&[&record[0..47], &record[51..]].concat());
    put_u32(&mut record, 47, crc);
    record
}

#[test]
fn wal_update_record_has_golden_bytes() {
    let record = update_record(b"k1", b"abc", PayloadType::Insert);
    assert_eq!(record.len(), 51 + 2 + 4 + 3);
    assert_eq!(&record[0..2], &[0, 0], "wal_type | sub_type");
    assert_eq!(&record[2..10], &7u64.to_le_bytes(), "bucket_id offset");
    assert_eq!(&record[10..11], &[3], "group_id");
    assert_eq!(&record[11..15], &9u32.to_le_bytes(), "size offset");
    assert_eq!(&record[15..19], &11u32.to_le_bytes(), "cmd_id");
    assert_eq!(&record[19..23], &2u32.to_le_bytes(), "klen");
    assert_eq!(&record[23..31], &13u64.to_le_bytes(), "txid");
    assert_eq!(&record[31..47], &[0u8; 16], "previous LSN");
    assert_eq!(
        &record[47..51],
        &crc32c(&[&record[0..47], &record[51..]].concat()).to_le_bytes()
    );
    assert_eq!(&record[51..53], b"k1");
    assert_eq!(&record[53..57], &3u32.to_le_bytes(), "value_len");
    assert_eq!(&record[57..60], b"abc");

    let u = ptr_to::<WalUpdate>(record.as_ptr());
    assert_eq!({ u.bucket_id }, 7);
    assert_eq!({ u.size }, 9);
    assert_eq!({ u.klen }, 2);
    assert_eq!({ u.group_id }, 3);
    assert!(u.is_intact());
    assert_eq!(u.validate_record(&record), Ok(()));
    assert_eq!(wal_record_sz(EntryType::Update).unwrap(), 51);

    // the three value-bearing payload shapes
    let merge = update_record(b"k1", b"operand", PayloadType::Merge);
    assert_eq!(
        ptr_to::<WalUpdate>(merge.as_ptr()).validate_record(&merge),
        Ok(())
    );
    let delete = update_record(b"k1", b"", PayloadType::Delete);
    assert_eq!(
        ptr_to::<WalUpdate>(delete.as_ptr()).validate_record(&delete),
        Ok(())
    );
}

#[test]
fn wal_control_records_have_golden_bytes() {
    let mut begin = [0u8; 13];
    begin[0] = EntryType::Begin as u8;
    begin[1..9].copy_from_slice(&13u64.to_le_bytes());
    let crc = crc32c(&begin[0..9]);
    begin[9..13].copy_from_slice(&crc.to_le_bytes());
    assert_eq!(&begin[0..1], &[1]);
    assert_eq!(&begin[9..], &crc.to_le_bytes());
    let b = ptr_to::<WalBegin>(begin.as_ptr());
    assert!(b.is_intact());
    assert_eq!(wal_record_sz(EntryType::Begin).unwrap(), 13);

    let mut commit = begin;
    commit[0] = EntryType::Commit as u8;
    let crc = crc32c(&commit[0..9]);
    commit[9..13].copy_from_slice(&crc.to_le_bytes());
    assert!(ptr_to::<WalCommit>(commit.as_ptr()).is_intact());
    assert_eq!(wal_record_sz(EntryType::Commit).unwrap(), 13);

    let mut abort = begin;
    abort[0] = EntryType::Abort as u8;
    let crc = crc32c(&abort[0..9]);
    abort[9..13].copy_from_slice(&crc.to_le_bytes());
    assert!(ptr_to::<WalAbort>(abort.as_ptr()).is_intact());
    assert_eq!(wal_record_sz(EntryType::Abort).unwrap(), 13);

    let mut checkpoint = [0u8; 21];
    checkpoint[0] = EntryType::CheckPoint as u8;
    checkpoint[1..9].copy_from_slice(&0x0102u64.to_le_bytes());
    checkpoint[9..17].copy_from_slice(&0x0304u64.to_le_bytes());
    let crc = crc32c(&checkpoint[0..17]);
    checkpoint[17..21].copy_from_slice(&crc.to_le_bytes());
    let c = ptr_to::<WalCheckpoint>(checkpoint.as_ptr());
    assert!(c.is_intact());
    assert_eq!({ c.checkpoint }, Position::new(0x0102, 0x0304));
    assert_eq!(&checkpoint[1..9], &0x0102u64.to_le_bytes());
    assert_eq!(wal_record_sz(EntryType::CheckPoint).unwrap(), 21);
    assert_eq!(
        wal_record_sz(EntryType::Unknown),
        Err(crate::OpCode::Corruption)
    );
}

#[test]
fn wal_framing_stops_at_the_first_bad_record() {
    let first = update_record(b"k1", b"abc", PayloadType::Insert);
    let second = update_record(b"k2", b"def", PayloadType::Update);
    let mut stream = [first.clone(), second.clone()].concat();
    let boundary = stream.len();
    // an unknown type byte ends the walk at its own start
    stream.push(0xEE);

    let mut pos = 0;
    while pos < stream.len() {
        let Ok(kind) = EntryType::try_from(stream[pos]) else {
            break;
        };
        let Ok(hdr) = wal_record_sz(kind) else {
            break;
        };
        if pos + hdr > stream.len() {
            break;
        }
        let len = match kind {
            EntryType::Update => 51 + ptr_to::<WalUpdate>(stream[pos..].as_ptr()).size as usize,
            other => wal_record_sz(other).unwrap(),
        };
        if pos + len > stream.len() {
            break;
        }
        let record = &stream[pos..pos + len];
        let ok = match kind {
            EntryType::Update => {
                let u = ptr_to::<WalUpdate>(record.as_ptr());
                u.is_intact() && u.validate_record(record).is_ok()
            }
            EntryType::Begin | EntryType::Commit | EntryType::Abort => {
                ptr_to::<WalBegin>(record.as_ptr()).is_intact()
            }
            EntryType::CheckPoint => ptr_to::<WalCheckpoint>(record.as_ptr()).is_intact(),
            EntryType::Unknown => false,
        };
        if !ok {
            break;
        }
        pos += len;
    }
    assert_eq!(
        pos, boundary,
        "truncation point is the first bad record start"
    );
}

#[test]
fn wal_stream_file_names_are_the_documented_families() {
    let path = RandomPath::tmp();
    let opt = Options::new(&*path).validate().unwrap();
    assert_eq!(opt.log_root(), opt.db_root().join("log"));
    assert_eq!(
        opt.wal_file(3, 7),
        opt.log_root().join("wal_3_7"),
        "per-group stream"
    );
    assert_eq!(
        opt.group_wal_file(9),
        opt.log_root().join("group_wal_9"),
        "shared stream"
    );
    assert_eq!(opt.physical_wal_path(255, 9), opt.group_wal_file(9));
    assert_eq!(opt.physical_wal_path(3, 9), opt.wal_file(3, 9));
    assert_eq!(
        opt.data_file(4),
        opt.data_root().join("data_4"),
        "data family"
    );
    assert_eq!(
        opt.blob_file(4),
        opt.data_root().join("blob_4"),
        "blob family"
    );
}

// ---------------------------------------------------------------------------
// data/blob files
// ---------------------------------------------------------------------------

fn delta_page(addr: u64, key: &[u8], record: &[u8]) -> BoxRef {
    let k = leaf_key(key, Ver::new(1, 1));
    let v = inline_val(0, 1, record, None);
    let mut payload = Vec::new();
    payload.extend_from_slice(&(k.len() as u32).to_le_bytes());
    payload.extend_from_slice(&(v.len() as u32).to_le_bytes());
    payload.extend_from_slice(&k);
    payload.extend_from_slice(&v);
    boxed(
        TagKind::Delta,
        NodeType::Leaf,
        42,
        addr,
        NULL_ADDR,
        &payload,
    )
}

fn remote_page(addr: u64, record: &[u8]) -> BoxRef {
    let mut payload = Vec::with_capacity(8 + record.len());
    payload.extend_from_slice(&(record.len() as u64).to_le_bytes());
    payload.extend_from_slice(record);
    boxed(
        TagKind::Remote,
        NodeType::Leaf,
        NULL_PID,
        addr,
        NULL_ADDR,
        &payload,
    )
}

/// `frames | intervals | relocations | footer` built from the specification, not from the writer
fn payload_file(frames: &[(u64, Vec<u8>)], version: u16, padding: [u8; 6]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut relocs = Vec::new();
    let (mut lo, mut hi) = (u64::MAX, 0u64);
    for (seq, (addr, stored)) in frames.iter().enumerate() {
        let pair = AddrPair::new(
            *addr,
            out.len(),
            stored.len() as u32,
            0,
            seq as u32,
            crc32c(stored),
        );
        relocs.extend_from_slice(pair.as_slice());
        out.extend_from_slice(stored);
        lo = lo.min(*addr);
        hi = hi.max(*addr);
    }
    let intervals = Interval::new(lo, hi);
    let interval_crc = crc32c(intervals.as_slice());
    let reloc_crc = crc32c(&relocs);
    out.extend_from_slice(intervals.as_slice());
    out.extend_from_slice(&relocs);
    let mut footer = FileFooter::new(frames.len() as u32, 1, reloc_crc, interval_crc);
    footer.version = version;
    footer.padding = padding;
    out.extend_from_slice(footer.as_slice());
    out
}

#[test]
fn data_and_blob_files_pin_frames_tables_and_footer() {
    let root = RandomPath::tmp();
    let mut opt = Options::new(&*root);
    opt.tmp_store = true;
    let opt = opt.validate().unwrap();
    let _ = opt.create_dir();

    // a frame small enough to stay raw and one large enough for the writer to compress
    let small = delta_page(0x100, b"k", b"value");
    let big = delta_page(0x101, b"big", &vec![b'a'; 1500]);
    let remote = remote_page(0x200, b"blob-record");

    let mut builder = FileBuilder::new(0, true, CompressorPool::new(), opt.fs.clone());
    builder.add(small.clone());
    builder.add(big.clone());
    let data = builder.flush_files(
        FileKind::Data,
        opt.data_file_size,
        7,
        || (0, opt.data_file(0)),
        |_| {},
        |_| {},
    );
    assert_eq!(data.len(), 1, "both data frames belong to one file");
    builder.add(remote.clone());
    let blob = builder.flush_files(
        FileKind::Blob,
        opt.data_file_size,
        7,
        || (1, opt.blob_file(1)),
        |_| {},
        |_| {},
    );
    assert_eq!(blob.len(), 1);

    // region order and footer
    let raw = std::fs::read(opt.data_file(0)).expect("read data file");
    let footer_off = raw.len() - size_of::<FileFooter>();
    assert_eq!(
        u16::from_le_bytes([raw[footer_off], raw[footer_off + 1]]),
        1,
        "footer version"
    );
    assert_eq!(
        &raw[footer_off + 2..footer_off + 8],
        &[0u8; 6],
        "footer reserved bytes"
    );
    let nr_reloc =
        u32::from_le_bytes(raw[footer_off + 8..footer_off + 12].try_into().unwrap()) as usize;
    let nr_intervals =
        u32::from_le_bytes(raw[footer_off + 12..footer_off + 16].try_into().unwrap()) as usize;
    let reloc_crc = u32::from_le_bytes(raw[footer_off + 16..footer_off + 20].try_into().unwrap());
    let interval_crc =
        u32::from_le_bytes(raw[footer_off + 20..footer_off + 24].try_into().unwrap());
    assert_eq!((nr_reloc, nr_intervals), (2, 1));

    let reloc_off = footer_off - nr_reloc * AddrPair::LEN;
    let interval_off = reloc_off - nr_intervals * Interval::LEN;
    assert_eq!(
        reloc_crc,
        crc32c(&raw[reloc_off..footer_off]),
        "relocation table CRC"
    );
    assert_eq!(
        interval_crc,
        crc32c(&raw[interval_off..reloc_off]),
        "interval table CRC"
    );

    let r0 = AddrPair::from_slice(&raw[reloc_off..]);
    let r1 = AddrPair::from_slice(&raw[reloc_off + AddrPair::LEN..]);
    assert_eq!(
        { r0.key },
        0x100,
        "one relocation per frame, keyed by logical address"
    );
    assert_eq!({ r0.val.off }, 0, "the first frame starts the file");
    assert_eq!({ r0.val.raw_len } as usize, box_bytes(&small).len());
    assert_eq!({ r0.val.compressed_len }, 0, "a small frame stays raw");
    assert_eq!({ r0.val.crc }, crc32c(&raw[..box_bytes(&small).len()]));
    assert_eq!({ r1.key }, 0x101);
    assert_eq!(
        { r1.val.off } as usize,
        box_bytes(&small).len(),
        "frames are stored back to back"
    );
    assert_eq!({ r1.val.raw_len } as usize, box_bytes(&big).len());
    // 60 header + 8 delta header + (1 varint + 12 ver + 3 key) + (6 val header + 1 group + 1500)
    assert_eq!({ r1.val.raw_len } as usize, 60 + 8 + 16 + 1507);
    let stored_len = { r1.val.compressed_len } as usize;
    assert_ne!(stored_len, 0, "a compressible frame is stored compressed");
    assert_eq!(
        { r1.val.crc },
        crc32c(&raw[{ r1.val.off } as usize..{ r1.val.off } as usize + stored_len]),
        "a compressed frame's CRC covers the stored bytes"
    );
    assert_eq!(
        interval_off,
        { r0.val.raw_len } as usize + stored_len,
        "the interval table follows the stored frames"
    );

    let interval = Interval::from_slice(&raw[interval_off..]);
    assert_eq!({ interval.lo }, 0x100);
    assert_eq!({ interval.hi }, 0x101);

    // the same bytes through the fallible file reader
    let mut meta = MetaReader::try_new(opt.fs.as_ref(), opt.data_file(0)).expect("v1 file");
    assert_eq!(meta.version(), FileVersion::V1);
    assert_eq!(meta.try_get_reloc().expect("relocations").len(), 2);
    assert_eq!(meta.try_get_interval().expect("intervals").len(), 1);

    let reader = new_reader(opt.data_file(0), DecompressorPool::new(), opt.fs.clone());
    assert_eq!(reader.version(), FileVersion::V1);
    for page in [&small, &big] {
        let back = reader
            .try_read_at(page.header().addr)
            .expect("every frame must decode");
        assert_eq!(
            box_bytes(&back),
            box_bytes(page),
            "logical address {} must read back the stored frame",
            page.header().addr
        );
    }
    assert_eq!(
        reader.try_read_at(0x999).err(),
        Some(OpCode::NotFound),
        "an address outside the relocation table is not a frame"
    );

    // the blob file has the same layout with one remote frame
    let raw = std::fs::read(opt.blob_file(1)).expect("read blob file");
    let footer_off = raw.len() - size_of::<FileFooter>();
    let nr_reloc =
        u32::from_le_bytes(raw[footer_off + 8..footer_off + 12].try_into().unwrap()) as usize;
    assert_eq!(nr_reloc, 1);
    let reloc_off = footer_off - nr_reloc * AddrPair::LEN;
    let interval_off = reloc_off - Interval::LEN;
    let r = AddrPair::from_slice(&raw[reloc_off..]);
    assert_eq!({ r.key }, 0x200);
    assert_eq!({ r.val.off }, 0);
    assert_eq!({ r.val.raw_len } as usize, box_bytes(&remote).len());
    assert_eq!({ r.val.crc }, crc32c(&raw[..box_bytes(&remote).len()]));
    let interval = Interval::from_slice(&raw[interval_off..]);
    assert_eq!({ interval.lo }, 0x200);
    assert_eq!({ interval.hi }, 0x200);

    let reader = new_reader(opt.blob_file(1), DecompressorPool::new(), opt.fs.clone());
    let back = reader.try_read_at(0x200).expect("remote frame must decode");
    assert_eq!(
        { back.view().as_remote().header().size },
        b"blob-record".len()
    );
    assert_eq!(back.view().as_remote().raw(), b"blob-record");
}

#[test]
fn meta_reader_dispatches_on_the_per_file_footer_version() {
    let root = RandomPath::tmp();
    let opt = Options::new(&*root).validate().unwrap();
    let fs = opt.fs.clone();
    let frame = box_bytes(&delta_page(0x300, b"k", b"v"));
    let v1 = root.join("data_1");
    let forged = root.join("data_2");
    let bad_padding = root.join("data_3");
    std::fs::write(&v1, payload_file(&[(0x300, frame.clone())], 1, [0; 6])).unwrap();
    std::fs::write(&forged, payload_file(&[(0x300, frame.clone())], 2, [0; 6])).unwrap();
    std::fs::write(
        &bad_padding,
        payload_file(&[(0x300, frame)], 1, [0, 0, 0, 0, 0, 1]),
    )
    .unwrap();

    let reader = MetaReader::try_new(fs.as_ref(), &v1).expect("v1 must open");
    assert_eq!(reader.version(), FileVersion::V1);
    assert_eq!(
        MetaReader::try_new(fs.as_ref(), &forged).err(),
        Some(OpCode::BadVersion),
        "the version comes from each file's own footer"
    );
    assert_eq!(
        MetaReader::try_new(fs.as_ref(), &bad_padding).err(),
        Some(OpCode::BadVersion),
        "a v1 footer with non-zero reserved bytes is not v1"
    );
}

#[test]
fn payload_files_reject_truncated_tables_and_bad_table_crcs() {
    let root = RandomPath::tmp();
    let opt = Options::new(&*root).validate().unwrap();
    let fs = opt.fs.clone();
    let path = root.join("data_9");
    let good = payload_file(
        &[(0x400, box_bytes(&delta_page(0x400, b"k", b"v")))],
        1,
        [0; 6],
    );
    let footer_off = good.len() - size_of::<FileFooter>();
    let reloc_off = footer_off - AddrPair::LEN;
    let interval_off = reloc_off - Interval::LEN;

    // shorter than a footer
    std::fs::write(&path, &good[..10]).unwrap();
    assert_eq!(
        MetaReader::try_new(fs.as_ref(), &path).err(),
        Some(OpCode::Corruption)
    );

    // tables that claim more bytes than the file holds
    let mut truncated = good.clone();
    put_u32(&mut truncated, footer_off + 8, u32::MAX);
    std::fs::write(&path, &truncated).unwrap();
    assert_eq!(
        MetaReader::try_new(fs.as_ref(), &path).err(),
        Some(OpCode::Corruption)
    );

    // a damaged relocation table is rejected on read, not on open
    let mut damaged = good.clone();
    damaged[reloc_off + 8] ^= 0xff;
    std::fs::write(&path, &damaged).unwrap();
    let mut reader = MetaReader::try_new(fs.as_ref(), &path).expect("footer is intact");
    assert_eq!(reader.try_get_reloc().err(), Some(OpCode::Corruption));

    // and so is a damaged interval table
    let mut damaged = good.clone();
    damaged[interval_off] ^= 0xff;
    std::fs::write(&path, &damaged).unwrap();
    let mut reader = MetaReader::try_new(fs.as_ref(), &path).expect("footer is intact");
    assert_eq!(reader.try_get_interval().err(), Some(OpCode::Corruption));
}

#[test]
fn payload_frames_reject_out_of_spec_headers() {
    let page = delta_page(0x500, b"k", b"v");
    let good = box_bytes(&page);
    assert!(frame_of(&good, 0x500).is_ok());

    let mut bad = good.clone();
    bad[0] = 0;
    assert_eq!(
        frame_of(&bad, 0x500).err(),
        Some(OpCode::Corruption),
        "kind"
    );

    let mut bad = good.clone();
    bad[1] = 9;
    assert_eq!(
        frame_of(&bad, 0x500).err(),
        Some(OpCode::Corruption),
        "node_type"
    );

    let mut bad = good.clone();
    bad[2] = TagFlag::Sibling as u8;
    assert_eq!(
        frame_of(&bad, 0x500).err(),
        Some(OpCode::Corruption),
        "only base pages may be siblings"
    );

    let mut bad = good.clone();
    put_u32(&mut bad, 4, 0xDEAD);
    assert_eq!(
        frame_of(&bad, 0x500).err(),
        Some(OpCode::Corruption),
        "total_size must equal the stored frame length"
    );

    assert_eq!(
        frame_of(&good, 0x501).err(),
        Some(OpCode::Corruption),
        "the header address must match the relocation key"
    );

    // a base payload header carries three one-byte flags that must be 0 or 1
    let base = boxed(
        TagKind::Base,
        NodeType::Leaf,
        7,
        0x600,
        NULL_ADDR,
        &base_payload(b"", b"", 0, false, false, &[]),
    );
    let good = box_bytes(&base);
    assert!(frame_of(&good, 0x600).is_ok());
    for (byte, name) in [
        (36, "merging"),
        (37, "is_index"),
        (38, "has_multiple_versions"),
    ] {
        let mut bad = good.clone();
        bad[60 + byte] = 2;
        assert_eq!(
            frame_of(&bad, 0x600).err(),
            Some(OpCode::Corruption),
            "{name} above 1"
        );
    }
}

// ---------------------------------------------------------------------------
// metadata rejections
// ---------------------------------------------------------------------------

fn seeded_store(root: &RandomPath) -> std::path::PathBuf {
    // `tmp_store` deletes the whole root when the parsed options drop, which would remove
    // the store as soon as the seeded instance closes
    let opt = Options::new(&**root).validate().unwrap();
    let manifest = opt.manifest();
    drop(Mace::new(opt).expect("a fresh store must open"));
    manifest
}

fn reopen(root: &RandomPath) -> Result<Mace, OpCode> {
    Mace::new(Options::new(&**root).validate().unwrap())
}

#[test]
fn metadata_version_gate_rejects_an_unknown_current_version() {
    let root = RandomPath::tmp();
    let manifest = seeded_store(&root);
    let tree = btree_store::BTree::open(&manifest).expect("open manifest");
    tree.exec(BUCKET_VERSION, |txn| {
        txn.put(VERSION_KEY, 2u64.to_le_bytes())
    })
    .expect("forge the version record");
    drop(tree);

    assert_eq!(reopen(&root).err(), Some(OpCode::BadVersion));
}

#[test]
fn metadata_without_options_on_an_initialized_store_is_corruption() {
    let root = RandomPath::tmp();
    let manifest = seeded_store(&root);
    let tree = btree_store::BTree::open(&manifest).expect("open manifest");
    tree.exec(BUCKET_MISC, |txn| txn.del(OPTIONS_KEY))
        .expect("drop the options record");
    drop(tree);

    assert_eq!(reopen(&root).err(), Some(OpCode::Corruption));
}

#[test]
fn malformed_options_json_is_corruption() {
    let root = RandomPath::tmp();
    let manifest = seeded_store(&root);
    let tree = btree_store::BTree::open(&manifest).expect("open manifest");
    tree.exec(BUCKET_MISC, |txn| {
        txn.put(OPTIONS_KEY, b"not json".as_slice())
    })
    .expect("replace the options record");
    drop(tree);

    assert_eq!(reopen(&root).err(), Some(OpCode::Corruption));
}
