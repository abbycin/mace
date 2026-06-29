use parking_lot::Mutex;
use std::hash::Hasher;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use zstd::stream::raw::{Decoder, Encoder, InBuffer, Operation, OutBuffer};
use zstd::zstd_safe::{self, CCtx, CParameter, DCtx, ResetDirective};

use crate::{OpCode, io::GatherIO, types::refbox::BoxRef, utils::data::GatherWriter};

pub(crate) const COMPRESS_MIN_LEN: usize = 1024;
const COMPRESS_LEVEL: i32 = 3;
const COMPRESS_GAIN_MARGIN: usize = 16;
const DECOMPRESS_IO_BUF: usize = 128 << 10;

pub(crate) struct EncodedRecord {
    pub(crate) bytes: Option<Vec<u8>>,
    pub(crate) raw_len: u32,
    pub(crate) compressed_len: u32,
    pub(crate) crc: u32,
}

impl EncodedRecord {
    pub(crate) fn active_len(&self) -> usize {
        if self.compressed_len == 0 {
            self.raw_len as usize
        } else {
            self.compressed_len as usize
        }
    }
}

pub(crate) struct RecordCompressor {
    cctx: CCtx<'static>,
}

impl RecordCompressor {
    pub(crate) fn new() -> Result<Self, OpCode> {
        let mut cctx = CCtx::create();
        cctx.set_parameter(CParameter::CompressionLevel(COMPRESS_LEVEL))
            .map_err(|_| OpCode::IoError)?;
        Ok(Self { cctx })
    }

    pub(crate) fn try_compress(&mut self, raw: &[u8]) -> Result<Option<Vec<u8>>, OpCode> {
        if raw.len() < COMPRESS_MIN_LEN {
            return Ok(None);
        }
        let compressed = self.compress_slices(&[raw])?;
        if compressed.len() + COMPRESS_GAIN_MARGIN < raw.len() {
            Ok(Some(compressed))
        } else {
            Ok(None)
        }
    }

    fn compress_slices(&mut self, parts: &[&[u8]]) -> Result<Vec<u8>, OpCode> {
        let raw_len = parts.iter().map(|x| x.len()).sum::<usize>();
        self.cctx
            .reset(ResetDirective::SessionOnly)
            .map_err(|_| OpCode::IoError)?;

        let mut encoder = Encoder::with_context(&mut self.cctx);
        encoder
            .set_pledged_src_size(Some(raw_len as u64))
            .map_err(|_| OpCode::IoError)?;

        let mut out = Vec::with_capacity(zstd_safe::compress_bound(raw_len));
        for part in parts {
            let mut input = InBuffer::around(part);
            while input.pos() < part.len() {
                let pos = out.len();
                let cap = out.capacity();
                if pos == cap {
                    out.reserve(COMPRESS_MIN_LEN);
                }
                let mut output = OutBuffer::around_pos(&mut out, pos);
                encoder
                    .run(&mut input, &mut output)
                    .map_err(|_| OpCode::IoError)?;
            }
        }

        loop {
            let pos = out.len();
            if pos == out.capacity() {
                out.reserve(COMPRESS_MIN_LEN);
            }
            let mut output = OutBuffer::around_pos(&mut out, pos);
            let remain = encoder
                .finish(&mut output, true)
                .map_err(|_| OpCode::IoError)?;
            if remain == 0 {
                break;
            }
        }

        Ok(out)
    }

    fn encode_raw_parts(head: &[u8], tail: Option<&[u8]>) -> EncodedRecord {
        let mut crc = crc32c::Crc32cHasher::default();
        crc.write(head);
        if let Some(body) = tail {
            crc.write(body);
        }
        let raw_len = head.len() + tail.map_or(0, <[u8]>::len);
        EncodedRecord {
            bytes: None,
            raw_len: raw_len as u32,
            compressed_len: 0,
            crc: crc.finish() as u32,
        }
    }

    pub(crate) fn encode_box(&mut self, b: &BoxRef) -> Result<EncodedRecord, OpCode> {
        b.with_dump_parts(|head, tail| {
            let raw_len = head.len() + tail.map_or(0, <[u8]>::len);
            debug_assert!(raw_len >= COMPRESS_MIN_LEN);

            let mut parts = [head, &[][..]];
            let parts = if let Some(body) = tail {
                parts[1] = body;
                &parts[..2]
            } else {
                &parts[..1]
            };

            let compressed = self.compress_slices(parts)?;
            if compressed.len() + COMPRESS_GAIN_MARGIN < raw_len {
                Ok(EncodedRecord {
                    crc: crc32c::crc32c(&compressed),
                    raw_len: raw_len as u32,
                    compressed_len: compressed.len() as u32,
                    bytes: Some(compressed),
                })
            } else {
                Ok(Self::encode_raw_parts(head, tail))
            }
        })
    }
}

pub(crate) struct CompressorGuard<'a> {
    pool: &'a CompressorPool,
    compressor: Option<RecordCompressor>,
}

impl Deref for CompressorGuard<'_> {
    type Target = RecordCompressor;

    fn deref(&self) -> &Self::Target {
        self.compressor
            .as_ref()
            .expect("borrowed compressor must exist")
    }
}

impl DerefMut for CompressorGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.compressor
            .as_mut()
            .expect("borrowed compressor must exist")
    }
}

impl Drop for CompressorGuard<'_> {
    fn drop(&mut self) {
        if let Some(compressor) = self.compressor.take() {
            self.pool.pool.lock().push(compressor);
        }
    }
}

pub(crate) struct CompressorPool {
    pool: Mutex<Vec<RecordCompressor>>,
}

impl CompressorPool {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            pool: Mutex::new(Vec::new()),
        })
    }

    pub(crate) fn borrow(&self) -> Result<CompressorGuard<'_>, OpCode> {
        let compressor = self
            .pool
            .lock()
            .pop()
            .map(Ok)
            .unwrap_or_else(RecordCompressor::new)?;
        Ok(CompressorGuard {
            pool: self,
            compressor: Some(compressor),
        })
    }
}

pub(crate) struct RecordDecompressor {
    dctx: DCtx<'static>,
    io: Vec<u8>,
    out: Vec<u8>,
}

pub(crate) struct DecodedRecordCrc {
    pub(crate) stored: u32,
    pub(crate) raw: u32,
}

impl RecordDecompressor {
    pub(crate) fn new() -> Result<Self, OpCode> {
        let mut dctx = DCtx::create();
        dctx.init().map_err(|_| OpCode::IoError)?;
        Ok(Self {
            dctx,
            io: vec![0u8; DECOMPRESS_IO_BUF],
            out: vec![0u8; DECOMPRESS_IO_BUF],
        })
    }

    pub(crate) fn decode_reader_into<R>(
        &mut self,
        reader: &R,
        off: u64,
        stored_len: usize,
        dst: &mut [u8],
    ) -> Result<u32, OpCode>
    where
        R: GatherIO,
    {
        let mut decoder = Decoder::with_context(&mut self.dctx);
        decoder.reinit().map_err(|_| OpCode::Corruption)?;

        let mut crc = crc32c::Crc32cHasher::default();
        let mut file_off = off;
        let mut remain = stored_len;
        let mut out_pos = 0usize;

        while remain != 0 {
            let read_len = remain.min(self.io.len());
            let buf = &mut self.io[..read_len];
            let got = reader.read(buf, file_off).map_err(|_| OpCode::IoError)?;
            if got != read_len {
                return Err(OpCode::Corruption);
            }
            crc.write(&buf[..got]);
            file_off += got as u64;
            remain -= got;

            let mut input = InBuffer::around(&buf[..got]);
            while input.pos() < got {
                let before_out = out_pos;
                let mut output = OutBuffer::around_pos(dst, out_pos);
                let _hint = decoder
                    .run(&mut input, &mut output)
                    .map_err(|_| OpCode::Corruption)?;
                out_pos = output.pos();

                if out_pos == dst.len() && (input.pos() < got || remain != 0) {
                    return Err(OpCode::Corruption);
                }
                if input.pos() == got && out_pos == before_out {
                    break;
                }
            }
        }

        let mut output = OutBuffer::around_pos(dst, out_pos);
        let tail = decoder
            .finish(&mut output, true)
            .map_err(|_| OpCode::Corruption)?;
        if tail != 0 || output.pos() != dst.len() {
            return Err(OpCode::Corruption);
        }

        Ok(crc.finish() as u32)
    }

    pub(crate) fn decode_to_writer<R>(
        &mut self,
        reader: &R,
        off: u64,
        raw_len: usize,
        stored_len: usize,
        writer: &mut GatherWriter,
    ) -> Result<DecodedRecordCrc, OpCode>
    where
        R: GatherIO,
    {
        let mut decoder = Decoder::with_context(&mut self.dctx);
        decoder.reinit().map_err(|_| OpCode::Corruption)?;

        let mut stored_crc = crc32c::Crc32cHasher::default();
        let mut raw_crc = crc32c::Crc32cHasher::default();
        let mut file_off = off;
        let mut remain = stored_len;
        let mut total_out = 0usize;

        while remain != 0 {
            let read_len = remain.min(self.io.len());
            let buf = &mut self.io[..read_len];
            let got = reader.read(buf, file_off).map_err(|_| OpCode::IoError)?;
            if got != read_len {
                return Err(OpCode::Corruption);
            }
            stored_crc.write(&buf[..got]);
            file_off += got as u64;
            remain -= got;

            let mut input = InBuffer::around(&buf[..got]);
            while input.pos() < got {
                let mut output = OutBuffer::around(&mut self.out);
                let _hint = decoder
                    .run(&mut input, &mut output)
                    .map_err(|_| OpCode::Corruption)?;
                let produced = output.pos();
                if produced != 0 {
                    total_out += produced;
                    if total_out > raw_len {
                        return Err(OpCode::Corruption);
                    }
                    raw_crc.write(&self.out[..produced]);
                    writer.write(&self.out[..produced]);
                }
                if input.pos() == got && produced == 0 {
                    break;
                }
            }
        }

        loop {
            let mut output = OutBuffer::around(&mut self.out);
            let tail = decoder
                .finish(&mut output, true)
                .map_err(|_| OpCode::Corruption)?;
            let produced = output.pos();
            if produced != 0 {
                total_out += produced;
                if total_out > raw_len {
                    return Err(OpCode::Corruption);
                }
                raw_crc.write(&self.out[..produced]);
                writer.write(&self.out[..produced]);
            }
            if tail == 0 {
                break;
            }
            if produced == 0 {
                return Err(OpCode::Corruption);
            }
        }

        if total_out != raw_len {
            return Err(OpCode::Corruption);
        }

        Ok(DecodedRecordCrc {
            stored: stored_crc.finish() as u32,
            raw: raw_crc.finish() as u32,
        })
    }
}

pub(crate) struct DecompressorPool {
    pool: Mutex<Vec<RecordDecompressor>>,
}

impl DecompressorPool {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            pool: Mutex::new(Vec::new()),
        })
    }

    pub(crate) fn with_decoder<T, F>(&self, f: F) -> Result<T, OpCode>
    where
        F: FnOnce(&mut RecordDecompressor) -> Result<T, OpCode>,
    {
        let mut decoder = self
            .pool
            .lock()
            .pop()
            .map(Ok)
            .unwrap_or_else(RecordDecompressor::new)?;
        let ret = f(&mut decoder);
        self.pool.lock().push(decoder);
        ret
    }
}

#[cfg(test)]
mod test {
    use super::{RecordCompressor, RecordDecompressor};
    use crate::{
        RandomPath,
        io::{File, GatherIO},
        utils::data::GatherWriter,
    };

    #[test]
    fn decode_to_writer() {
        let raw = vec![b'x'; super::COMPRESS_MIN_LEN * 2];
        let mut compressor = RecordCompressor::new().expect("compressor must initialize");
        let compressed = compressor
            .try_compress(&raw)
            .expect("compression must succeed")
            .expect("payload must compress");

        let path = RandomPath::tmp();
        let mut writer = GatherWriter::trunc(&path.to_path_buf(), 8);
        writer.write(&compressed);
        drop(writer);

        let reader = File::options()
            .read(true)
            .open(path.to_path_buf())
            .expect("compressed file must open");
        let out = RandomPath::tmp();
        let mut out_writer = GatherWriter::trunc(&out.to_path_buf(), 8);
        let mut decoder = RecordDecompressor::new().expect("decoder must initialize");
        let crc = decoder
            .decode_to_writer(&reader, 0, raw.len(), compressed.len(), &mut out_writer)
            .expect("decode to writer must succeed");
        drop(out_writer);

        assert_eq!(crc.stored, crc32c::crc32c(&compressed));
        assert_eq!(crc.raw, crc32c::crc32c(&raw));
        assert_ne!(crc.stored, crc.raw);

        let mut loaded = vec![0u8; raw.len()];
        let out_reader = File::options()
            .read(true)
            .open(out.to_path_buf())
            .expect("decoded file must open");
        let got = out_reader
            .read(&mut loaded, 0)
            .expect("decoded bytes must read");
        assert_eq!(got, raw.len());
        assert_eq!(loaded, raw);
    }
}
