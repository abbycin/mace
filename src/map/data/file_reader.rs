//! payload file readers: footer discovery, relocations, and record reads

use crc32c::Crc32cHasher;
use std::hash::Hasher;
use std::mem::{MaybeUninit, size_of};
use std::path::{Path, PathBuf};
use std::ptr::addr_of_mut;
use std::sync::Arc;

use crate::io::{File, FileSystem, GatherIO};
use crate::must_ok;
use crate::types::refbox::BoxRef;
use crate::types::traits::IHeader;
use crate::utils::OpCode;
use crate::utils::compress::DecompressorPool;
use crate::utils::data::{AddrPair, Interval, Reloc};

use super::file_builder::{FileFooter, FileVersion};

pub struct MetaReader {
    file: File,
    footer: ParsedFooter,
    end: u64,
}

#[derive(Clone, Copy, Debug)]
struct ParsedFooter {
    version: FileVersion,
    nr_reloc: u32,
    nr_intervals: u32,
    reloc_crc: u32,
    interval_crc: u32,
}

impl ParsedFooter {
    fn from_footer(version: FileVersion, footer: FileFooter) -> Self {
        Self {
            version,
            nr_reloc: footer.nr_reloc,
            nr_intervals: footer.nr_intervals,
            reloc_crc: footer.reloc_crc,
            interval_crc: footer.interval_crc,
        }
    }
}

impl MetaReader {
    pub(crate) fn new<P: AsRef<Path>>(fs: &dyn FileSystem, path: P) -> Self {
        must_ok!(Self::try_new(fs, path), "can't open payload metadata")
    }

    /// fallible variant of [`Self::new`], for readers that must report damage
    /// instead of aborting (the offline checker)
    pub fn try_new<P: AsRef<Path>>(fs: &dyn FileSystem, path: P) -> Result<Self, OpCode> {
        let path = path.as_ref();
        let file = File::options()
            .read(true)
            .trunc(false)
            .open(fs, path)
            .map_err(OpCode::from)?;
        let end = file.size().map_err(OpCode::from)?;
        if end < size_of::<FileFooter>() as u64 {
            return Err(OpCode::Corruption);
        }
        let footer =
            Self::try_read_tail::<FileFooter>(&file, end - size_of::<FileFooter>() as u64)?;
        let version_raw = footer.version;
        let version = FileVersion::from_raw(version_raw).ok_or(OpCode::BadVersion)?;
        if !footer.is_supported_version(version) {
            return Err(OpCode::BadVersion);
        }
        let footer = ParsedFooter::from_footer(version, footer);
        let interval_len = (footer.nr_intervals as usize)
            .checked_mul(Interval::LEN)
            .ok_or(OpCode::Corruption)?;
        let reloc_len = (footer.nr_reloc as usize)
            .checked_mul(AddrPair::LEN)
            .ok_or(OpCode::Corruption)?;
        let meta_len = interval_len
            .checked_add(reloc_len)
            .and_then(|x| x.checked_add(size_of::<FileFooter>()))
            .ok_or(OpCode::Corruption)?;
        if end < meta_len as u64 {
            return Err(OpCode::Corruption);
        }

        Ok(Self { file, footer, end })
    }

    pub(crate) fn version(&self) -> FileVersion {
        self.footer.version
    }

    pub(crate) fn get_reloc(&mut self) -> Box<[AddrPair]> {
        must_ok!(self.try_get_reloc(), "can't read relocations")
    }

    /// fallible variant of [`Self::get_reloc`]
    pub fn try_get_reloc(&mut self) -> Result<Box<[AddrPair]>, OpCode> {
        let len = self.footer.nr_reloc as usize * AddrPair::LEN;
        self.try_read_meta(
            self.end - (len + size_of::<FileFooter>()) as u64,
            self.footer.nr_reloc as usize,
            self.footer.reloc_crc,
        )
    }

    pub(crate) fn get_interval(&mut self) -> Box<[Interval]> {
        must_ok!(self.try_get_interval(), "can't read intervals")
    }

    /// fallible variant of [`Self::get_interval`]
    pub fn try_get_interval(&mut self) -> Result<Box<[Interval]>, OpCode> {
        let len = self.footer.nr_intervals as usize * Interval::LEN;
        self.try_read_meta(
            self.end
                - (len + self.footer.nr_reloc as usize * AddrPair::LEN + size_of::<FileFooter>())
                    as u64,
            self.footer.nr_intervals as usize,
            self.footer.interval_crc,
        )
    }

    fn try_read_meta<U>(&self, off: u64, count: usize, crc: u32) -> Result<Box<[U]>, OpCode> {
        let len = size_of::<U>()
            .checked_mul(count)
            .ok_or(OpCode::Corruption)?;
        let mut data = Box::<[MaybeUninit<U>]>::new_uninit_slice(count);
        let dst = unsafe { std::slice::from_raw_parts_mut(data.as_mut_ptr().cast::<u8>(), len) };
        Self::try_read_exact(&self.file, dst, off)?;
        let mut h = Crc32cHasher::default();
        h.write(dst);
        if h.finish() as u32 != crc {
            return Err(OpCode::Corruption);
        }
        let p = Box::into_raw(data) as *mut [U];
        unsafe { Ok(Box::from_raw(p)) }
    }

    fn try_read_tail<T: Default>(file: &File, off: u64) -> Result<T, OpCode> {
        let mut footer = T::default();
        let dst = unsafe {
            let p = addr_of_mut!(footer);
            std::slice::from_raw_parts_mut(p.cast::<u8>(), size_of::<T>())
        };
        Self::try_read_exact(file, dst, off)?;
        Ok(footer)
    }

    fn try_read_exact(file: &File, buf: &mut [u8], off: u64) -> Result<(), OpCode> {
        let got = file.read(buf, off).map_err(OpCode::from)?;
        if got != buf.len() {
            return Err(OpCode::Corruption);
        }
        Ok(())
    }

    pub(crate) fn take(self) -> File {
        self.file
    }
}
pub struct FileReader {
    file: File,
    version: FileVersion,
    relocs: Box<[AddrPair]>,
    decoders: Arc<DecompressorPool>,
}

pub(crate) fn new_reader(
    path: PathBuf,
    decoders: Arc<DecompressorPool>,
    fs: Arc<dyn FileSystem>,
) -> Arc<FileReader> {
    let mut loader = MetaReader::new(fs.as_ref(), &path);
    let version = loader.version();
    let relocs = loader.get_reloc();
    let relocs = {
        #[cfg(feature = "extra_check")]
        for w in relocs.windows(2) {
            let prev = w[0].key;
            let next = w[1].key;
            debug_assert!(prev <= next, "reloc table must be sorted by addr");
        }
        relocs
    };
    let file = loader.take();
    Arc::new(FileReader {
        file,
        version,
        relocs,
        decoders,
    })
}

#[cfg(feature = "maintenance")]
pub fn try_new_reader(
    path: PathBuf,
    decoders: Arc<DecompressorPool>,
    fs: Arc<dyn FileSystem>,
) -> Result<Arc<FileReader>, OpCode> {
    let mut loader = MetaReader::try_new(fs.as_ref(), &path)?;
    let version = loader.version();
    let relocs = loader.try_get_reloc()?;
    Ok(Arc::new(FileReader {
        file: loader.take(),
        version,
        relocs,
        decoders,
    }))
}

impl FileReader {
    fn try_read_exact(&self, buf: &mut [u8], mut off: u64) -> Result<(), OpCode> {
        let mut done = 0;
        while done < buf.len() {
            let got = self
                .file
                .read(&mut buf[done..], off)
                .map_err(OpCode::from)?;
            if got == 0 {
                return Err(OpCode::Corruption);
            }
            done += got;
            off += got as u64;
        }
        Ok(())
    }

    pub(crate) fn version(&self) -> FileVersion {
        self.version
    }

    pub(crate) fn file(&self) -> &File {
        &self.file
    }

    #[inline]
    pub(crate) fn find_reloc(&self, pos: u64) -> Option<Reloc> {
        let idx = self.relocs.binary_search_by_key(&pos, |x| x.key).ok()?;
        Some(self.relocs[idx].val)
    }

    pub(crate) fn read_at(&self, pos: u64) -> BoxRef {
        must_ok!(self.try_read_at(pos), "must read payload record")
    }

    pub fn try_read_at(&self, pos: u64) -> Result<BoxRef, OpCode> {
        let m = self.find_reloc(pos).ok_or(OpCode::NotFound)?;
        let raw_len = m.raw_len() as usize;
        if raw_len < BoxRef::DUMP_HDR_LEN {
            return Err(OpCode::Corruption);
        }
        let Some(alloc_size) = raw_len
            .checked_add(std::mem::size_of::<std::sync::atomic::AtomicU32>())
            .and_then(|size| u32::try_from(size).ok())
        else {
            return Err(OpCode::Corruption);
        };
        let mut page = match self.version {
            FileVersion::V1 => BoxRef::alloc_exact(alloc_size, pos),
            _ => return Err(OpCode::BadVersion),
        };
        let result = (|| {
            let dst = page.load_slice(raw_len);
            if !m.is_compressed() {
                let mut crc = Crc32cHasher::default();
                self.try_read_exact(dst, m.off as u64)?;
                crc.write(dst);
                let actual_crc = crc.finish() as u32;
                if actual_crc != m.crc {
                    return Err(OpCode::Corruption);
                }
            } else {
                let actual_crc = self.decoders.with_decoder(|decoder| {
                    decoder.decode_reader_into(
                        &self.file,
                        m.off as u64,
                        m.compressed_len() as usize,
                        dst,
                    )
                })?;
                if actual_crc != m.crc {
                    return Err(OpCode::Corruption);
                }
            }
            page.decode_persisted_v1_in_place(pos, raw_len)
        })();
        match result {
            Ok(()) => Ok(page),
            Err(err) => {
                // corrupted payload headers must not make BoxRef cleanup abort
                page.header_mut().payload_size = alloc_size - BoxRef::HDR_LEN as u32;
                page.header_mut().total_size = alloc_size;
                Err(err)
            }
        }
    }
}
