use std::{
    io,
    path::{Path, PathBuf},
};

#[cfg(feature = "failpoints")]
use crate::utils::failpoint::FsOp;

#[repr(C)]
pub struct IoVec {
    pub data: *const u8,
    pub len: usize,
}

impl From<&[u8]> for IoVec {
    fn from(value: &[u8]) -> Self {
        unsafe { std::mem::transmute(value) }
    }
}

pub trait GatherIO {
    fn read(&self, data: &mut [u8], pos: u64) -> Result<usize, io::Error>;

    fn write(&mut self, data: &[u8]) -> Result<usize, io::Error>;

    fn writev(&mut self, data: &mut [IoVec], total_len: usize) -> Result<(), io::Error>;

    fn sync(&mut self) -> Result<(), io::Error>;

    fn sync_data(&mut self) -> Result<(), io::Error>;

    fn truncate(&self, to: u64) -> Result<(), io::Error>;

    fn size(&self) -> Result<u64, io::Error>;
}

/// filesystem hook for namespace operations and runtime file opens
pub(crate) trait FileSystem: Send + Sync {
    fn open(&self, path: &Path, opt: &OpenOptions) -> Result<File, io::Error>;

    fn try_exists(&self, path: &Path) -> Result<bool, io::Error>;

    fn create_dir_all(&self, path: &Path) -> Result<(), io::Error>;

    fn read_dir(&self, path: &Path) -> Result<Vec<PathBuf>, io::Error>;

    fn rename(&self, from: &Path, to: &Path) -> Result<(), io::Error>;

    fn remove_file(&self, path: &Path) -> Result<(), io::Error>;

    fn sync_dir(&self, path: &Path) -> Result<(), io::Error>;

    fn remove_file_if_exists(&self, path: &Path) -> Result<(), io::Error> {
        if !self.try_exists(path)? {
            return Ok(());
        }
        match self.remove_file(path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err),
        }
    }
}

pub(crate) struct OsFileSystem;

pub struct OpenOptions {
    read: bool,
    write: bool,
    append: bool,
    create: bool,
    trunc: bool,
}

impl OpenOptions {
    fn new() -> Self {
        Self {
            read: false,
            write: false,
            append: false,
            create: false,
            trunc: false,
        }
    }

    pub fn read(&mut self, on: bool) -> &mut Self {
        self.read = on;
        self
    }

    pub fn write(&mut self, on: bool) -> &mut Self {
        self.write = on;
        self
    }

    // when trunc is enabled, append will be ignored
    pub fn append(&mut self, on: bool) -> &mut Self {
        if !self.trunc {
            self.append = on;
        }
        self
    }

    pub fn create(&mut self, on: bool) -> &mut Self {
        self.create = on;
        self
    }

    pub fn trunc(&mut self, on: bool) -> &mut Self {
        if on {
            self.append = false;
        }
        self.trunc = on;
        self
    }

    pub(crate) fn open<P: AsRef<Path>>(
        &self,
        fs: &dyn FileSystem,
        path: P,
    ) -> Result<File, io::Error> {
        fs.open(path.as_ref(), self)
    }
}

#[cfg(windows)]
pub mod win;
#[cfg(windows)]
pub use win::{File, sync_dir};

#[cfg(unix)]
pub mod unix;
#[cfg(unix)]
pub use unix::{File, sync_dir};

impl FileSystem for OsFileSystem {
    fn open(&self, path: &Path, opt: &OpenOptions) -> Result<File, io::Error> {
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::check_fs(FsOp::Open, path)?;
        opt.open_os(path)
    }

    fn try_exists(&self, path: &Path) -> Result<bool, io::Error> {
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::check_fs(FsOp::TryExists, path)?;
        path.try_exists()
    }

    fn create_dir_all(&self, path: &Path) -> Result<(), io::Error> {
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::check_fs(FsOp::CreateDirAll, path)?;
        std::fs::create_dir_all(path)
    }

    fn read_dir(&self, path: &Path) -> Result<Vec<PathBuf>, io::Error> {
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::check_fs(FsOp::ReadDir, path)?;
        std::fs::read_dir(path)?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect()
    }

    fn rename(&self, from: &Path, to: &Path) -> Result<(), io::Error> {
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::check_fs(FsOp::Rename, from)?;
        std::fs::rename(from, to)
    }

    fn remove_file(&self, path: &Path) -> Result<(), io::Error> {
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::check_fs(FsOp::RemoveFile, path)?;
        std::fs::remove_file(path)
    }

    fn sync_dir(&self, path: &Path) -> Result<(), io::Error> {
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::check_fs(FsOp::SyncDir, path)?;
        sync_dir(path)
    }
}

#[cfg(test)]
pub(crate) mod testfs {
    use std::{
        io,
        path::{Path, PathBuf},
        sync::Mutex,
    };

    use super::{File, FileSystem, OpenOptions, OsFileSystem};

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(crate) enum InjectOp {
        Open,
        TryExists,
        CreateDirAll,
        ReadDir,
        Rename,
        RemoveFile,
        SyncDir,
    }

    #[derive(Clone, Debug)]
    struct Rule {
        op: InjectOp,
        path: Option<PathBuf>,
        nth: usize,
        seen: usize,
        kind: io::ErrorKind,
    }

    #[derive(Default)]
    struct State {
        rules: Vec<Rule>,
        calls: Vec<(InjectOp, PathBuf)>,
    }

    pub(crate) struct InjectedFileSystem {
        state: Mutex<State>,
        os: OsFileSystem,
    }

    impl InjectedFileSystem {
        pub(crate) fn new() -> Self {
            Self {
                state: Mutex::new(State::default()),
                os: OsFileSystem,
            }
        }

        pub(crate) fn fail_once<P: AsRef<Path>>(&self, op: InjectOp, path: P, kind: io::ErrorKind) {
            self.fail_nth(op, Some(path.as_ref().to_path_buf()), 1, kind);
        }

        pub(crate) fn fail_nth_any(&self, op: InjectOp, nth: usize, kind: io::ErrorKind) {
            self.fail_nth(op, None, nth, kind);
        }

        pub(crate) fn calls(&self) -> Vec<(InjectOp, PathBuf)> {
            self.state
                .lock()
                .expect("fs state lock must work")
                .calls
                .clone()
        }

        fn fail_nth(&self, op: InjectOp, path: Option<PathBuf>, nth: usize, kind: io::ErrorKind) {
            self.state
                .lock()
                .expect("fs state lock must work")
                .rules
                .push(Rule {
                    op,
                    path,
                    nth,
                    seen: 0,
                    kind,
                });
        }

        fn check(&self, op: InjectOp, path: &Path) -> Result<(), io::Error> {
            let mut state = self.state.lock().expect("fs state lock must work");
            state.calls.push((op, path.to_path_buf()));
            for rule in &mut state.rules {
                let path_match = match &rule.path {
                    Some(expected) => expected == path,
                    None => true,
                };
                if rule.op == op && path_match {
                    rule.seen += 1;
                    if rule.seen == rule.nth {
                        return Err(io::Error::new(
                            rule.kind,
                            format!("injected {op:?} failure for {:?}", path),
                        ));
                    }
                }
            }
            Ok(())
        }
    }

    impl FileSystem for InjectedFileSystem {
        fn open(&self, path: &Path, opt: &OpenOptions) -> Result<File, io::Error> {
            self.check(InjectOp::Open, path)?;
            self.os.open(path, opt)
        }

        fn try_exists(&self, path: &Path) -> Result<bool, io::Error> {
            self.check(InjectOp::TryExists, path)?;
            self.os.try_exists(path)
        }

        fn create_dir_all(&self, path: &Path) -> Result<(), io::Error> {
            self.check(InjectOp::CreateDirAll, path)?;
            self.os.create_dir_all(path)
        }

        fn read_dir(&self, path: &Path) -> Result<Vec<PathBuf>, io::Error> {
            self.check(InjectOp::ReadDir, path)?;
            self.os.read_dir(path)
        }

        fn rename(&self, from: &Path, to: &Path) -> Result<(), io::Error> {
            self.check(InjectOp::Rename, from)?;
            self.os.rename(from, to)
        }

        fn remove_file(&self, path: &Path) -> Result<(), io::Error> {
            self.check(InjectOp::RemoveFile, path)?;
            self.os.remove_file(path)
        }

        fn sync_dir(&self, path: &Path) -> Result<(), io::Error> {
            self.check(InjectOp::SyncDir, path)?;
            self.os.sync_dir(path)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{io::ErrorKind, path::PathBuf};

    use crate::RandomPath;

    use super::{
        FileSystem,
        testfs::{InjectOp, InjectedFileSystem},
    };

    #[test]
    fn remove_file_if_exists_propagates_non_not_found_error() {
        let path = PathBuf::from(&*RandomPath::tmp());
        std::fs::write(&path, b"x").expect("seed file must be created");
        let fs = InjectedFileSystem::new();
        fs.fail_once(InjectOp::RemoveFile, &path, ErrorKind::PermissionDenied);

        let err = fs
            .remove_file_if_exists(&path)
            .expect_err("remove must fail");

        assert_eq!(err.kind(), ErrorKind::PermissionDenied);
        assert!(path.exists(), "failing remove must leave file intact");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn injected_fs_supports_nth_call_and_records_calls() {
        let path = PathBuf::from(&*RandomPath::tmp());
        std::fs::write(&path, b"x").expect("seed file must be created");
        let fs = InjectedFileSystem::new();
        fs.fail_nth_any(InjectOp::TryExists, 2, ErrorKind::PermissionDenied);

        assert!(super::FileSystem::try_exists(&fs, &path).expect("first stat must pass"));
        let err = super::FileSystem::try_exists(&fs, &path).expect_err("second stat must fail");

        assert_eq!(err.kind(), ErrorKind::PermissionDenied);
        let calls = fs.calls();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].0, InjectOp::TryExists);
        assert_eq!(calls[1].0, InjectOp::TryExists);
        let _ = std::fs::remove_file(path);
    }
}
