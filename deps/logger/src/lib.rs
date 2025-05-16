use log::{LevelFilter, Metadata, Record};
use std::cell::OnceCell;
use std::io::Write;
use std::ops::Deref;
use std::path::Path;
use std::ptr::addr_of_mut;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Mutex, MutexGuard};

thread_local! {
    static G_TID: OnceCell<i32> = OnceCell::new();
}
#[cfg(not(target_os = "linux"))]
static G_ID: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(1);

static mut G_LOGGER: Logger = Logger {
    mtx_shard: [const { Mutex::new(()) }; 8],
    sink: Vec::new(),
    abort_on_error: AtomicBool::new(false),
};
static G_INIIED: Mutex<bool> = Mutex::new(false);

const G_CONSOLE: &'static str = "console";
const G_FILE: &'static str = "file";

#[cfg(target_os = "linux")]
fn get_tid() -> i32 {
    G_TID.with(|x| *x.get_or_init(|| unsafe { libc::gettid() }))
}

#[cfg(not(target_os = "linux"))]
fn get_tid() -> i32 {
    use std::sync::atomic::Ordering::Relaxed;
    G_TID.with(|x| *x.get_or_init(|| G_ID.fetch_add(1, Relaxed)))
}

/// a simple sync logger which impl log::Log
pub struct Logger {
    mtx_shard: [Mutex<()>; 8],
    sink: Vec<SinkHandle>,
    abort_on_error: AtomicBool,
}

struct SinkHandle {
    raw: *mut dyn Sink,
}

unsafe impl Send for SinkHandle {}
unsafe impl Sync for SinkHandle {}

impl SinkHandle {
    fn new<T>(x: T) -> Self
    where
        T: Sink + 'static,
    {
        let x = Box::new(x);
        let raw = Box::into_raw(x);
        Self { raw }
    }

    fn as_mut(&self) -> &mut dyn Sink {
        unsafe { &mut *self.raw }
    }
}

impl Deref for SinkHandle {
    type Target = dyn Sink;
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.raw }
    }
}

impl Drop for SinkHandle {
    fn drop(&mut self) {
        unsafe {
            let _ = Box::from_raw(self.raw);
        }
    }
}

trait Sink: Send + Sync {
    fn sink(&mut self, str: &String);

    fn flush(&mut self);

    fn name(&self) -> &'static str;
}

impl log::Log for Logger {
    fn enabled(&self, _metadata: &Metadata) -> bool {
        return true;
    }

    fn log(&self, record: &Record) {
        let s = format!(
            "{} {} [{}] {}:{} {}\n",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S.%6f"),
            get_tid(),
            record.level().as_str(),
            record.file().unwrap(),
            record.line().unwrap(),
            record.args()
        );
        let _lk = self.lock();

        for p in &self.sink {
            p.as_mut().sink(&s);
        }

        if record.level() == log::LevelFilter::Error && self.should_abort() {
            let bt = std::backtrace::Backtrace::force_capture();
            let buf = format!("{}", bt);
            for p in &self.sink {
                p.as_mut().sink(&buf);
            }
            std::process::abort();
        }
    }

    fn flush(&self) {
        let _lk = self.lock();
        for p in &self.sink {
            p.as_mut().flush();
        }
    }
}

struct Console {}

impl Console {
    fn new() -> Self {
        Self {}
    }
}

/// NOTE: file rolling is not support at present
struct File {
    w: std::fs::File,
}

impl File {
    fn new(path: impl AsRef<Path>, trunc: bool) -> Result<Self, std::io::Error> {
        let mut ops = std::fs::File::options();
        ops.write(true).create(true);
        if trunc {
            ops.truncate(true);
        } else {
            ops.append(true);
        }
        match ops.open(path) {
            Err(e) => Err(e),
            Ok(f) => Ok(Self { w: f }),
        }
    }
}

impl Sink for Console {
    fn sink(&mut self, str: &String) {
        std::io::stdout().write(str.as_bytes()).unwrap();
    }

    fn flush(&mut self) {
        std::io::stdout().flush().unwrap();
    }

    fn name(&self) -> &'static str {
        G_CONSOLE
    }
}

impl Sink for File {
    fn sink(&mut self, str: &String) {
        self.w.write(str.as_bytes()).unwrap();
    }

    fn flush(&mut self) {
        self.w.flush().unwrap();
    }

    fn name(&self) -> &'static str {
        G_FILE
    }
}

impl Logger {
    fn is_set() -> bool {
        *G_INIIED.lock().unwrap()
    }
    pub fn init() -> &'static mut Self {
        if !Self::is_set() {
            *G_INIIED.lock().unwrap() = true;
            log::set_logger(Self::get()).unwrap();
            log::set_max_level(LevelFilter::Trace);
        }
        return Self::get();
    }

    pub fn get() -> &'static mut Self {
        unsafe {
            let a = addr_of_mut!(G_LOGGER);
            return &mut *a;
        }
    }

    fn exist(&self, sink: &'static str) -> Option<&mut Self> {
        let _lk = self.mtx_shard[0].lock().unwrap();
        for i in &self.sink {
            if i.name() == sink {
                return Some(Self::get());
            }
        }
        return None;
    }

    fn should_abort(&self) -> bool {
        self.abort_on_error.load(Relaxed)
    }

    fn lock(&self) -> MutexGuard<()> {
        self.mtx_shard[get_tid() as usize & (self.mtx_shard.len() - 1)]
            .lock()
            .unwrap()
    }

    pub fn abort_on_error(&mut self, flag: bool) -> &mut Self {
        self.abort_on_error.store(flag, Relaxed);
        self
    }

    pub fn add_console(&mut self) -> &mut Self {
        if self.exist(G_CONSOLE).is_none() {
            self.sink.push(SinkHandle::new(Console::new()));
        }
        self
    }

    pub fn add_file(&mut self, path: impl AsRef<Path>, trunc: bool) -> Option<&mut Self> {
        if self.exist(G_FILE).is_none() {
            match File::new(&path, trunc) {
                Err(e) => {
                    eprintln!(
                        "can't open {}, error {}",
                        path.as_ref().to_str().unwrap(),
                        e.to_string()
                    );
                    return None;
                }
                Ok(f) => {
                    self.sink.push(SinkHandle::new(f));
                    return Some(self);
                }
            }
        }
        Some(self)
    }

    fn remove_impl(&mut self, name: &'static str) {
        let _lk = self.mtx_shard[0].lock().unwrap();
        for (idx, s) in self.sink.iter().enumerate() {
            if s.name() == name {
                self.sink.remove(idx);
                break;
            }
        }
    }

    pub fn remove_file(&mut self) {
        self.remove_impl(G_FILE);
    }

    pub fn remove_console(&mut self) {
        self.remove_impl(G_CONSOLE);
    }
}

impl Drop for Logger {
    fn drop(&mut self) {
        let _lk = self.lock();
        for p in &self.sink {
            p.as_mut().flush();
        }
    }
}

#[cfg(test)]
mod test {
    use crate::Logger;

    #[test]
    fn test_console() {
        let l = Logger::init();

        let p = log::logger() as *const dyn log::Log;
        let q = &*l as *const dyn log::Log;
        assert!(std::ptr::addr_eq(p, q));
    }
}
