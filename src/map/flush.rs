use crate::map::data::{DataBuilder, Frame};
use crate::Options;
use std::io::Seek;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::{Condvar, Mutex};
use std::{
    fs::File,
    sync::{
        atomic::Ordering::Relaxed,
        mpsc::{channel, Receiver, Sender},
        Arc,
    },
    thread::JoinHandle,
    time::Duration,
};

use super::data::FlushData;

fn flush_data(msg: &FlushData, opt: &Arc<Options>) {
    let mut builder = DataBuilder::new();

    let mut size = 0;
    if msg.still() {
        let flush_all = msg.is_force();
        for f in msg.iter {
            if f.set_state(Frame::STATE_INACTIVE, Frame::STATE_DEAD) == Frame::STATE_INACTIVE {
                size += f.size();
                builder.add(f, flush_all);
            }
        }
    }

    let path = opt.data_file(msg.id());
    let mut f = match File::options().append(true).create(true).open(&path) {
        Ok(f) => f,
        Err(e) => {
            log::error!("can't open {:?}, {:?}", path, e);
            panic!("fatal error, path {:?}, error {:?}", path, e);
        }
    };
    let off = f
        .seek(std::io::SeekFrom::End(0))
        .inspect_err(|e| {
            log::error!("can't seek file {:?}, error {}", path, e);
            panic!("fatal error, path {:?}, error {}", path, e);
        })
        .unwrap();
    let mut cur_pos = off;

    if !builder.is_empty() {
        cur_pos += builder.build(off, &mut f);
        let _ = f.sync_all().map_err(|x| {
            log::error!("can't sync {:?} {}", path, x);
            panic!("fatal error");
        });
        log::trace!(
            "flush dirty {} frames, size {} off {} end {}",
            builder.len(),
            size,
            off,
            cur_pos
        );
    }

    msg.mark_done(cur_pos);
}

fn flush_thread(rx: Receiver<FlushData>, opt: Arc<Options>, sync: Arc<Notifier>) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("flush".into())
        .spawn(move || {
            log::debug!("start flush thread");
            while !sync.is_quit() {
                match rx.recv_timeout(Duration::from_millis(1)) {
                    Ok(x) => flush_data(&x, &opt),
                    Err(RecvTimeoutError::Disconnected) => break,
                    _ => {}
                }
            }
            drop(rx);
            sync.notify_done();
            log::debug!("stop flush thread");
        })
        .expect("can't build flush thread")
}

struct Notifier {
    quit: AtomicBool,
    cond: Condvar,
    done: Mutex<bool>,
}

impl Notifier {
    fn new() -> Self {
        Self {
            quit: AtomicBool::new(false),
            cond: Condvar::new(),
            done: Mutex::new(false),
        }
    }

    fn is_quit(&self) -> bool {
        self.quit.load(Relaxed)
    }

    fn wait_done(&self) {
        let _guard = self.cond.wait_while(self.done.lock().unwrap(), |x| !(*x));
    }

    fn notify_quit(&self) {
        self.quit.store(true, Relaxed);
    }

    fn notify_done(&self) {
        let mut lk = self.done.lock().expect("can't lock");
        *lk = true;
        self.cond.notify_one();
    }
}

pub struct Flush {
    pub tx: Sender<FlushData>,
    sync: Arc<Notifier>,
}

impl Flush {
    pub fn new(opt: Arc<Options>) -> Self {
        let (tx, rx) = channel();
        let sync = Arc::new(Notifier::new());
        flush_thread(rx, opt.clone(), sync.clone());
        Self { tx, sync }
    }

    pub fn quit(&self) {
        self.sync.notify_quit();
        self.sync.wait_done();
    }
}
