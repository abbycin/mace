use mace::{Bucket, BucketOptions, Mace, OpCode, Options, RandomPath};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

static ROOTS: OnceLock<Mutex<Vec<PathBuf>>> = OnceLock::new();

pub(crate) struct FuzzDbRoot {
    path: RandomPath,
}

impl FuzzDbRoot {
    pub(crate) fn new() -> Self {
        let path = RandomPath::tmp();
        register_cleanup();
        roots()
            .lock()
            .expect("fuzz root registry poisoned")
            .push((*path).clone());
        Self { path }
    }

    pub(crate) fn path(&self) -> &RandomPath {
        &self.path
    }
}

impl Drop for FuzzDbRoot {
    fn drop(&mut self) {
        remove_root(self.path.as_path());
        self.path.unlink();
    }
}

fn roots() -> &'static Mutex<Vec<PathBuf>> {
    ROOTS.get_or_init(|| Mutex::new(Vec::new()))
}

fn register_cleanup() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        #[cfg(unix)]
        unsafe extern "C" {
            fn atexit(cb: extern "C" fn()) -> i32;
        }

        #[cfg(unix)]
        extern "C" fn cleanup() {
            if let Some(roots) = ROOTS.get() {
                let roots = roots.lock().expect("fuzz root registry poisoned");
                for path in roots.iter() {
                    if path.exists() {
                        if path.is_file() {
                            let _ = std::fs::remove_file(path);
                        } else {
                            let _ = std::fs::remove_dir_all(path);
                        }
                    }
                }
            }
        }

        #[cfg(unix)]
        unsafe {
            let _ = atexit(cleanup);
        }
    });
}

fn remove_root(path: &Path) {
    if let Some(roots) = ROOTS.get() {
        let mut roots = roots.lock().expect("fuzz root registry poisoned");
        roots.retain(|p| p.as_path() != path);
    }
}

pub(crate) struct ByteStream<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> ByteStream<'a> {
    pub(crate) fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    pub(crate) fn next(&mut self) -> Option<u8> {
        let out = self.data.get(self.pos).copied();
        if out.is_some() {
            self.pos += 1;
        }
        out
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.pos >= self.data.len()
    }
}

pub(crate) fn key_name(id: usize) -> String {
    format!("k_{id:02}")
}

pub(crate) fn value_bytes(seed: u8, size_hint: usize) -> Vec<u8> {
    let len = match size_hint % 4 {
        0 => 8,
        1 => 64,
        2 => 512,
        _ => 2048,
    };
    let mut out = vec![seed.max(1); len];
    out[0] = seed.wrapping_add(1);
    out
}

pub(crate) fn open_engine(path: &RandomPath, tune: impl FnOnce(&mut Options)) -> Mace {
    let mut opt = Options::new(&**path);
    tune(&mut opt);
    Mace::new(opt.validate().expect("fuzz options must validate")).expect("open engine failed")
}

pub(crate) fn get_or_create_bucket(
    mace: &Mace,
    name: &str,
    opt: BucketOptions,
) -> Result<Bucket, OpCode> {
    match mace.get_bucket(name) {
        Ok(bucket) => Ok(bucket),
        Err(OpCode::NotFound) => mace.new_bucket(name, opt),
        Err(err) => Err(err),
    }
}

pub(crate) fn assert_bucket_matches_model(
    bucket: &Bucket,
    model: &BTreeMap<String, Option<Vec<u8>>>,
) {
    let view = bucket.view().expect("open view failed");
    for (key, expected) in model {
        match expected {
            Some(value) => {
                let actual = view.get(key).expect("expected visible key");
                assert_eq!(actual.slice(), value.as_slice(), "key {key} mismatch");
            }
            None => {
                let res = view.get(key);
                assert!(
                    matches!(res, Err(OpCode::NotFound)),
                    "key {key} should be absent"
                );
            }
        }
    }
}
