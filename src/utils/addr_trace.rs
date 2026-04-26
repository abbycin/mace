#![allow(dead_code)]

use std::{
    collections::{HashSet, VecDeque},
    env,
    fmt::Write,
    sync::{Mutex, OnceLock},
};

enum TraceMode {
    Off,
    All,
    Selected(HashSet<u64>),
}

enum TraceScope {
    Addr(u64),
    Interval {
        bucket_id: u64,
        kind: &'static str,
        lo: u64,
        hi: u64,
        file_id: u64,
    },
}

struct TraceEvent {
    seq: u64,
    kind: &'static str,
    detail: String,
    scope: TraceScope,
}

struct TraceState {
    mode: TraceMode,
    cap: usize,
    seq: u64,
    events: VecDeque<TraceEvent>,
}

impl TraceState {
    fn new() -> Self {
        let mode = parse_mode();
        let cap = env::var("MACE_TRACE_EVENT_CAP")
            .ok()
            .and_then(|x| x.parse::<usize>().ok())
            .filter(|&x| x > 0)
            .unwrap_or(100_000);
        Self {
            mode,
            cap,
            seq: 0,
            events: VecDeque::new(),
        }
    }

    fn enabled(&self) -> bool {
        !matches!(self.mode, TraceMode::Off)
    }

    fn match_addr(&self, addr: u64) -> bool {
        match &self.mode {
            TraceMode::Off => false,
            TraceMode::All => true,
            TraceMode::Selected(set) => set.contains(&addr),
        }
    }

    fn match_interval(&self, lo: u64, hi: u64) -> bool {
        match &self.mode {
            TraceMode::Off => false,
            TraceMode::All => true,
            TraceMode::Selected(set) => set.iter().any(|&addr| addr >= lo && addr <= hi),
        }
    }

    fn push(&mut self, kind: &'static str, detail: String, scope: TraceScope) {
        self.seq += 1;
        if self.events.len() >= self.cap {
            self.events.pop_front();
        }
        self.events.push_back(TraceEvent {
            seq: self.seq,
            kind,
            detail,
            scope,
        });
    }
}

fn parse_mode() -> TraceMode {
    let Some(raw) = env::var("MACE_TRACE_ADDR").ok() else {
        return TraceMode::Off;
    };
    let raw = raw.trim();
    if raw.is_empty() {
        return TraceMode::Off;
    }
    if raw.eq_ignore_ascii_case("all") {
        return TraceMode::All;
    }
    let mut set = HashSet::new();
    for part in raw.split([',', ' ', ';']) {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let parsed = if let Some(hex) = part.strip_prefix("0x").or_else(|| part.strip_prefix("0X"))
        {
            u64::from_str_radix(hex, 16).ok()
        } else {
            part.parse::<u64>().ok()
        };
        if let Some(addr) = parsed {
            set.insert(addr);
        }
    }
    if set.is_empty() {
        TraceMode::Off
    } else {
        TraceMode::Selected(set)
    }
}

fn global() -> &'static Mutex<TraceState> {
    static TRACE: OnceLock<Mutex<TraceState>> = OnceLock::new();
    TRACE.get_or_init(|| Mutex::new(TraceState::new()))
}

fn configured() -> bool {
    static CONFIGURED: OnceLock<bool> = OnceLock::new();
    *CONFIGURED.get_or_init(|| {
        env::var("MACE_TRACE_ADDR")
            .ok()
            .is_some_and(|raw| !raw.trim().is_empty())
    })
}

pub(crate) fn record_addr<F>(addr: u64, kind: &'static str, detail: F)
where
    F: FnOnce() -> String,
{
    if !configured() {
        return;
    }
    let mut lk = global().lock().expect("addr trace lock poisoned");
    if !lk.match_addr(addr) {
        return;
    }
    lk.push(kind, detail(), TraceScope::Addr(addr));
}

pub(crate) fn record_interval<F>(
    bucket_id: u64,
    kind: &'static str,
    range_kind: &'static str,
    lo: u64,
    hi: u64,
    file_id: u64,
    detail: F,
) where
    F: FnOnce() -> String,
{
    if !configured() {
        return;
    }
    let mut lk = global().lock().expect("addr trace lock poisoned");
    if !lk.match_interval(lo, hi) {
        return;
    }
    lk.push(
        kind,
        detail(),
        TraceScope::Interval {
            bucket_id,
            kind: range_kind,
            lo,
            hi,
            file_id,
        },
    );
}

pub(crate) fn dump(addr: u64) -> Option<String> {
    if !configured() {
        return None;
    }
    let lk = global().lock().expect("addr trace lock poisoned");
    if !lk.enabled() {
        return None;
    }

    let mut out = String::new();
    let _ = writeln!(out, "addr trace dump for {}", addr);
    let _ = writeln!(out, "tracked events {}", lk.events.len());
    let mut matched = 0usize;
    for ev in lk.events.iter() {
        match &ev.scope {
            TraceScope::Addr(x) if *x == addr => {
                matched += 1;
                let _ = writeln!(out, "#{} addr {} {} {}", ev.seq, x, ev.kind, ev.detail);
            }
            TraceScope::Interval {
                bucket_id,
                kind,
                lo,
                hi,
                file_id,
            } if touches(*lo, *hi, addr) => {
                matched += 1;
                let _ = writeln!(
                    out,
                    "#{} interval {} bucket={} [{}, {}] file={} {} {}",
                    ev.seq, kind, bucket_id, lo, hi, file_id, ev.kind, ev.detail
                );
            }
            _ => {}
        }
    }
    let _ = writeln!(out, "matched events {}", matched);
    Some(out)
}

fn touches(lo: u64, hi: u64, addr: u64) -> bool {
    addr >= lo.saturating_sub(1) && addr <= hi.saturating_add(1)
}
