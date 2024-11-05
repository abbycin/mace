use log::{LevelFilter, Metadata, Record};
use std::cell::{Cell, RefCell};
use std::io::Write;
use std::path::Path;
use std::ptr::addr_of_mut;
use std::sync::Mutex;

thread_local! {
    static G_TID: Cell<i32> = Cell::new(0);
}
static mut G_LOGGER: Logger = Logger { sink: Vec::new() };

const G_CONSOLE: &'static str = "console";
const G_FILE: &'static str = "file";

fn get_tid() -> i32 {
    unsafe {
        if G_TID.get() == 0 {
            G_TID.set(libc::gettid());
        }
        return G_TID.get();
    }
}

/// a simple sync logger which impl log::Log
pub struct Logger {
    sink: Vec<Mutex<RefCell<Box<dyn Sink>>>>,
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

        for p in &self.sink {
            if let Ok(p) = p.lock() {
                p.borrow_mut().sink(&s);
            }
        }
    }

    fn flush(&self) {
        for p in &self.sink {
            if let Ok(p) = p.lock() {
                p.borrow_mut().flush();
            }
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
    pub fn init() -> &'static mut Self {
        log::set_logger(Self::get()).unwrap();
        log::set_max_level(LevelFilter::Trace);
        return Self::get();
    }

    pub fn get() -> &'static mut Self {
        unsafe {
            let a = addr_of_mut!(G_LOGGER);
            return &mut *a;
        }
    }

    fn exist(&self, sink: &'static str) -> Option<&mut Self> {
        for i in &self.sink {
            if i.lock().unwrap().borrow().name() == sink {
                return Some(Self::get());
            }
        }
        return None;
    }

    pub fn add_console(&mut self) -> &mut Self {
        if self.exist(G_CONSOLE).is_none() {
            self.sink
                .push(Mutex::new(RefCell::new(Box::new(Console::new()))));
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
                    self.sink.push(Mutex::new(RefCell::new(Box::new(f))));
                    return Some(self);
                }
            }
        }
        Some(self)
    }

    fn remove_impl(&mut self, name: &'static str) {
        for (idx, s) in self.sink.iter().enumerate() {
            if s.lock().unwrap().borrow().name() == name {
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
