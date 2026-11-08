//! data/blob file construction and the page map built from a flush batch

use crc32c::Crc32cHasher;
use std::hash::Hasher;
use std::path::PathBuf;
use std::sync::Arc;

use crate::io::FileSystem;
use crate::meta::{FileKind, MemStat, PageTable, StatInner};
use crate::must_true;
use crate::types::header::{TagFlag, TagKind};
use crate::types::refbox::BoxRef;
use crate::types::traits::{IAsSlice, IHeader};
use crate::utils::bitmap::BitMap;
use crate::utils::compress::{COMPRESS_MIN_LEN, CompressorPool};
use crate::utils::data::{AddrPair, GatherWriter, Interval};
use crate::utils::{NULL_ADDR, NULL_PID};
use crate::{must_exist, must_ok, static_assert};

use super::checkpoint_task::PidSet;

/// the layout of a flushed batch is:
/// ```text
/// +-------------+-----------+-------------+---------+
/// | data frames | intervals | relocations | trailer |
/// +-------------+-----------+-------------+---------+
/// ```
/// write file from frames to trailer while read file from trailer to relocations

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FileVersion(u16);

impl FileVersion {
    pub(crate) const V1: Self = Self(1);
    pub(crate) const CURRENT: Self = Self::V1;

    pub(crate) const fn raw(self) -> u16 {
        self.0
    }

    pub(crate) const fn can_reuse_to(self, target: Self) -> bool {
        matches!((self.0, target.0), (1, 1))
    }

    pub(super) fn from_raw(raw: u16) -> Option<Self> {
        match raw {
            x if x == Self::V1.raw() => Some(Self::V1),
            _ => None,
        }
    }
}

#[repr(C, packed(1))]
#[derive(Default, Debug, Clone, Copy)]
pub(crate) struct FileFooter {
    pub(crate) version: u16,
    pub(crate) padding: [u8; 6],
    pub(crate) nr_reloc: u32,
    pub(crate) nr_intervals: u32,
    pub(crate) reloc_crc: u32,
    pub(crate) interval_crc: u32,
}

impl IAsSlice for FileFooter {}

static_assert!(size_of::<FileFooter>() == 24);
static_assert!(std::mem::offset_of!(FileFooter, nr_reloc) == 8);

impl FileFooter {
    const VERSION: u16 = FileVersion::CURRENT.raw();

    pub(crate) const fn new(
        nr_reloc: u32,
        nr_intervals: u32,
        reloc_crc: u32,
        interval_crc: u32,
    ) -> Self {
        Self {
            version: Self::VERSION,
            padding: [0; 6],
            nr_reloc,
            nr_intervals,
            reloc_crc,
            interval_crc,
        }
    }

    pub(super) fn is_supported_version(&self, version: FileVersion) -> bool {
        match version {
            FileVersion::V1 => self.padding.iter().all(|x| *x == 0),
            _ => false,
        }
    }
}

/// build both data and blob file
/// NOTE: the blob data may be not ordered by key especially after compaction, this is absolutely ok
/// for SSD, because it's good at random read
pub(crate) struct FileBuilder {
    bucket_id: u64,
    enable_compression: bool,
    compressors: Arc<CompressorPool>,
    fs: Arc<dyn FileSystem>,
    files: [FlushFrames; 2],
}

pub(crate) struct BuiltFile {
    pub(crate) file_id: u64,
    pub(crate) interval: Interval,
    pub(crate) writer: GatherWriter,
    pub(crate) stat: MemStat,
    pub(crate) addrs: Vec<u64>,
}

struct ChunkBuild {
    built: BuiltFile,
    consumed: usize,
    resident_size: usize,
}

struct RawRecordMeta {
    raw_len: u32,
    crc: u32,
}

#[derive(Clone)]
struct PersistedHead {
    len: usize,
    bytes: [u8; BoxRef::MAX_PERSISTED_HEAD_LEN],
}

impl PersistedHead {
    fn new(src: &[u8]) -> Self {
        let mut bytes = [0u8; BoxRef::MAX_PERSISTED_HEAD_LEN];
        bytes[..src.len()].copy_from_slice(src);
        Self {
            len: src.len(),
            bytes,
        }
    }

    fn as_slice(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}

struct ChunkRecordMeta {
    raw_len: u32,
    compressed_len: u32,
    dump_len: usize,
    crc: u32,
}

#[derive(Default)]
struct FlushFrames {
    frames: Vec<BoxRef>,
}

impl FileBuilder {
    fn make_stat(&self, file_id: u64, tick: u64, active_size: usize, nr_reloc: u32) -> MemStat {
        let inner = StatInner {
            file_id,
            up1: tick,
            up2: tick,
            active_elems: nr_reloc,
            total_elems: nr_reloc,
            active_size,
            total_size: active_size,
            bucket_id: self.bucket_id,
        };
        MemStat::from_parts(inner, Some(BitMap::new(nr_reloc)))
    }

    fn update_addr(ivl: &mut Interval, addr: u64) {
        ivl.lo = ivl.lo.min(addr);
        ivl.hi = ivl.hi.max(addr);
    }

    pub(crate) fn new(
        bucket_id: u64,
        enable_compression: bool,
        compressors: Arc<CompressorPool>,
        fs: Arc<dyn FileSystem>,
    ) -> Self {
        Self {
            bucket_id,
            enable_compression,
            compressors,
            fs,
            files: [FlushFrames::default(), FlushFrames::default()],
        }
    }

    pub(crate) fn add(&mut self, f: BoxRef) {
        let h = f.header();
        // NOTE: the pid maybe NULL_PID when it's a sibling or remote page
        match h.flag {
            TagFlag::Normal | TagFlag::Sibling => {
                // all blob were allocated by RemoteView
                let kind = if h.kind == TagKind::Remote {
                    FileKind::Blob
                } else {
                    FileKind::Data
                };
                let state = self.frames_mut(kind);
                state.frames.push(f);
            }
        }
    }

    pub(crate) fn has_kind(&self, kind: FileKind) -> bool {
        !self.frames(kind).frames.is_empty()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.files.iter().all(|x| x.frames.is_empty())
    }

    fn frames(&self, kind: FileKind) -> &FlushFrames {
        &self.files[kind.slot()]
    }

    fn frames_mut(&mut self, kind: FileKind) -> &mut FlushFrames {
        &mut self.files[kind.slot()]
    }

    fn ensure_iov_room(
        need: usize,
        queued_iov: &mut usize,
        hdr_batch: &mut Vec<PersistedHead>,
        w: &mut GatherWriter,
    ) {
        if *queued_iov + need > GatherWriter::DEFAULT_IOVCNT {
            w.flush();
            *queued_iov = 0;
            hdr_batch.clear();
        }
    }

    fn queue_raw_parts(
        head: &[u8],
        tail: Option<&[u8]>,
        w: &mut GatherWriter,
        queued_iov: &mut usize,
        hdr_batch: &mut Vec<PersistedHead>,
    ) {
        let need = 1 + usize::from(tail.is_some());
        Self::ensure_iov_room(need, queued_iov, hdr_batch, w);
        hdr_batch.push(PersistedHead::new(head));
        let hdr = must_exist!(hdr_batch.last(), "queued persisted head must exist");
        w.queue(hdr.as_slice());
        *queued_iov += 1;
        if let Some(body) = tail {
            w.queue(body);
            *queued_iov += 1;
        }
    }

    fn queue_raw_record(
        f: &BoxRef,
        w: &mut GatherWriter,
        queued_iov: &mut usize,
        hdr_batch: &mut Vec<PersistedHead>,
    ) {
        f.with_persisted_parts(|head, tail| {
            Self::queue_raw_parts(head, tail, w, queued_iov, hdr_batch)
        });
    }

    fn queue_raw_record_with_meta(
        f: &BoxRef,
        w: &mut GatherWriter,
        queued_iov: &mut usize,
        hdr_batch: &mut Vec<PersistedHead>,
    ) -> RawRecordMeta {
        f.with_persisted_parts(|head, tail| {
            let mut crc = Crc32cHasher::default();
            crc.write(head);
            if let Some(body) = tail {
                crc.write(body);
            }
            Self::queue_raw_parts(head, tail, w, queued_iov, hdr_batch);
            RawRecordMeta {
                raw_len: (head.len() + tail.map_or(0, <[u8]>::len)) as u32,
                crc: crc.finish() as u32,
            }
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn fill_head_chunk_records(
        &self,
        frames: &[BoxRef],
        cap: usize,
        w: &mut GatherWriter,
        relocs: &mut Vec<u8>,
        addrs: &mut Vec<u64>,
        queued_iov: &mut usize,
        hdr_batch: &mut Vec<PersistedHead>,
    ) -> (usize, usize, usize, Interval) {
        #[cfg(feature = "extra_check")]
        for w in frames.windows(2) {
            assert!(
                w[0].header().addr < w[1].header().addr,
                "flush frames must be strictly sorted by addr: {} then {}",
                w[0].header().addr,
                w[1].header().addr
            );
        }

        let mut pos = 0usize;
        let mut consumed = 0usize;
        let mut active_size = 0usize;
        let mut resident_size = 0usize;
        let mut interval = Interval::new(u64::MAX, 0);
        let mut compressor = None;
        let mut idx = 0;
        while idx < frames.len() {
            let f = &frames[idx];
            let raw_len = f.dump_len();

            let raw_path = !self.enable_compression || raw_len < COMPRESS_MIN_LEN;
            let mut encoded = None;
            let meta = if raw_path {
                ChunkRecordMeta {
                    raw_len: raw_len as u32,
                    compressed_len: 0,
                    dump_len: raw_len,
                    crc: 0,
                }
            } else {
                let codec = compressor.get_or_insert_with(|| {
                    must_ok!(self.compressors.borrow(), "record encode must succeed")
                });
                let data = must_ok!(codec.encode_box(f), "record encode must succeed");
                let meta = ChunkRecordMeta {
                    raw_len: data.raw_len,
                    compressed_len: data.compressed_len,
                    dump_len: data.active_len(),
                    crc: data.crc,
                };
                encoded = Some(data);
                meta
            };

            if consumed != 0 && active_size + meta.dump_len > cap {
                break;
            }

            let meta = if raw_path {
                let raw = Self::queue_raw_record_with_meta(f, w, queued_iov, hdr_batch);
                must_true!(eq raw.raw_len as usize, raw_len);
                ChunkRecordMeta {
                    raw_len: raw.raw_len,
                    compressed_len: 0,
                    dump_len: raw_len,
                    crc: raw.crc,
                }
            } else {
                let encoded = must_exist!(
                    encoded,
                    "encoded payload must exist after compressed planning"
                );
                let dump_len = encoded.active_len();
                let raw_len = encoded.raw_len;
                let stored_len = encoded.compressed_len;
                let crc = encoded.crc;
                if let Some(bytes) = encoded.bytes {
                    Self::ensure_iov_room(1, queued_iov, hdr_batch, w);
                    w.queue_owned(bytes);
                    *queued_iov += 1;
                } else {
                    Self::queue_raw_record(f, w, queued_iov, hdr_batch);
                }
                ChunkRecordMeta {
                    raw_len,
                    compressed_len: stored_len,
                    dump_len,
                    crc,
                }
            };
            let h = f.header();
            let addr = h.addr;
            if consumed == 0 {
                interval = Interval::new(addr, addr);
            } else {
                Self::update_addr(&mut interval, addr);
            }
            active_size += meta.dump_len;
            resident_size += h.total_size as usize;
            addrs.push(addr);
            let reloc = AddrPair::new(
                addr,
                pos,
                meta.raw_len,
                meta.compressed_len,
                consumed as u32,
                meta.crc,
            );
            relocs.extend_from_slice(reloc.as_slice());
            pos += meta.dump_len;
            consumed += 1;
            idx += 1;
        }

        must_true!(consumed > 0);
        (consumed, active_size, resident_size, interval)
    }

    fn build_head_chunk(
        &self,
        frames: &[BoxRef],
        max_file_size: usize,
        file_id: u64,
        tick: u64,
        path: PathBuf,
    ) -> ChunkBuild {
        must_true!(!frames.is_empty());
        let cap = max_file_size.max(1);
        let mut w = GatherWriter::trunc(self.fs.as_ref(), &path, 64);
        let mut relocs = Vec::new();
        let mut addrs = Vec::new();
        let mut queued_iov = 0usize;
        let mut hdr_batch: Vec<PersistedHead> =
            Vec::with_capacity(GatherWriter::DEFAULT_IOVCNT / 2);
        let (consumed, active_size, resident_size, interval) = self.fill_head_chunk_records(
            frames,
            cap,
            &mut w,
            &mut relocs,
            &mut addrs,
            &mut queued_iov,
            &mut hdr_batch,
        );

        let mut h = Crc32cHasher::default();
        let is = interval.as_slice();
        h.write(is);
        let interval_crc = h.finish() as u32;
        Self::ensure_iov_room(1, &mut queued_iov, &mut hdr_batch, &mut w);
        w.queue(is);
        queued_iov += 1;

        let mut reloc_crc = Crc32cHasher::default();
        let rs = relocs.as_slice();
        reloc_crc.write(rs);
        Self::ensure_iov_room(1, &mut queued_iov, &mut hdr_batch, &mut w);
        w.queue(rs);
        queued_iov += 1;

        let hdr = FileFooter::new(consumed as u32, 1, reloc_crc.finish() as u32, interval_crc);

        Self::ensure_iov_room(1, &mut queued_iov, &mut hdr_batch, &mut w);
        w.queue(hdr.as_slice());
        w.flush();

        let stat = self.make_stat(file_id, tick, active_size, consumed as u32);
        ChunkBuild {
            built: BuiltFile {
                file_id,
                interval,
                writer: w,
                stat,
                addrs,
            },
            consumed,
            resident_size,
        }
    }

    pub(crate) fn flush_files<F, P, O>(
        &mut self,
        kind: FileKind,
        max_file_size: usize,
        tick: u64,
        mut alloc: F,
        mut on_flushed: P,
        mut on_file: O,
    ) -> Vec<BuiltFile>
    where
        F: FnMut() -> (u64, PathBuf),
        P: FnMut(u64),
        O: FnMut(&BuiltFile),
    {
        let mut frames = {
            let state = self.frames_mut(kind);
            std::mem::take(&mut state.frames)
        };
        frames.sort_unstable_by_key(|x| x.header().addr);
        let mut out = Vec::new();
        while !frames.is_empty() {
            let (file_id, path) = alloc();
            let chunk = self.build_head_chunk(&frames, max_file_size, file_id, tick, path);
            on_flushed(chunk.resident_size as u64);
            frames.drain(..chunk.consumed);
            on_file(&chunk.built);
            out.push(chunk.built);
        }
        out
    }
}

pub(crate) struct MapBuilder {
    table: PageTable,
}

impl MapBuilder {
    pub(crate) fn new(bucket_id: u64, unmap_pid: &PidSet) -> Self {
        let mut table = PageTable::default();
        table.bucket_id = bucket_id;
        let mut this = Self { table };
        for x in unmap_pid.iter() {
            this.add_impl(*x, NULL_ADDR);
        }
        this
    }

    fn add_impl(&mut self, pid: u64, addr: u64) {
        must_true!(ne pid, NULL_PID);
        self.table.add(pid, addr);
    }

    pub(crate) fn add(&mut self, f: &BoxRef) {
        let h = f.header();
        match h.flag {
            TagFlag::Normal => {
                // ignore those failed in CAS
                if h.pid != NULL_PID {
                    self.add_impl(h.pid, h.addr);
                }
            }
            TagFlag::Sibling => {
                must_true!(eq h.pid, NULL_PID);
            }
        }
    }

    pub(crate) fn table(self) -> PageTable {
        self.table
    }
}
