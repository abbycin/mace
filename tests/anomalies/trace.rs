//! black-box trace recording and snapshot-isolation checking for the anomaly suite.
//!
//! the recorder sees only public API results; version identity, read attribution and the
//! per-key version order are derived here, never read from engine internals

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Mutex;

pub type TxnId = usize;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WriteRef {
    pub txn: TxnId,
    pub op: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Owner {
    Txn(TxnId),
    View,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Attr {
    Unique(WriteRef),
    Seed,
    Absent,
    Ambiguous,
    Unknown,
}

#[derive(Clone, Debug)]
pub enum Ev {
    Seed {
        key: Vec<u8>,
    },
    Begin {
        txn: TxnId,
    },
    Read {
        owner: Owner,
        key: Vec<u8>,
        attr: Attr,
    },
    Write {
        txn: TxnId,
        key: Vec<u8>,
        ok: bool,
        wref: Option<WriteRef>,
    },
    Commit {
        txn: TxnId,
        ok: bool,
    },
    Drop {
        txn: TxnId,
    },
}

/// append-only recorder. one recorder per `Executor`; the harness thread is the only
/// writer, so the mutex never serializes engine work
pub struct Recorder {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    events: Vec<Ev>,
    next_txn: TxnId,
    next_op: BTreeMap<TxnId, u32>,
    seeds: Vec<(Vec<u8>, Vec<u8>)>,
    writes: Vec<(WriteRef, Vec<u8>, Vec<u8>)>,
}

impl Default for Recorder {
    fn default() -> Self {
        Self::new()
    }
}

impl Recorder {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(State::default()),
        }
    }

    pub fn alloc_txn(&self) -> TxnId {
        let mut st = self.state.lock().unwrap();
        let id = st.next_txn;
        st.next_txn += 1;
        id
    }

    pub fn seed(&self, key: &[u8], bytes: &[u8]) {
        let mut st = self.state.lock().unwrap();
        st.seeds.push((key.to_vec(), bytes.to_vec()));
        st.events.push(Ev::Seed { key: key.to_vec() });
    }

    pub fn begin(&self, txn: TxnId) {
        self.state.lock().unwrap().events.push(Ev::Begin { txn });
    }

    pub fn read(&self, owner: Owner, key: &[u8], bytes: Option<&[u8]>) -> Attr {
        let mut st = self.state.lock().unwrap();
        let attr = st.attribute(key, bytes);
        st.events.push(Ev::Read {
            owner,
            key: key.to_vec(),
            attr,
        });
        attr
    }

    pub fn write(
        &self,
        txn: TxnId,
        key: &[u8],
        ok: bool,
        bytes: Option<&[u8]>,
    ) -> Option<WriteRef> {
        let mut st = self.state.lock().unwrap();
        let wref = if ok {
            let op = st.next_op.entry(txn).or_insert(0);
            let wref = WriteRef { txn, op: *op };
            *op += 1;
            st.writes
                .push((wref, key.to_vec(), bytes.unwrap_or_default().to_vec()));
            Some(wref)
        } else {
            None
        };
        st.events.push(Ev::Write {
            txn,
            key: key.to_vec(),
            ok,
            wref,
        });
        wref
    }

    pub fn commit(&self, txn: TxnId, ok: bool) {
        self.state
            .lock()
            .unwrap()
            .events
            .push(Ev::Commit { txn, ok });
    }

    pub fn drop_txn(&self, txn: TxnId) {
        self.state.lock().unwrap().events.push(Ev::Drop { txn });
    }

    pub fn len(&self) -> usize {
        self.state.lock().unwrap().events.len()
    }

    pub fn trace(&self) -> Vec<Ev> {
        self.state.lock().unwrap().events.clone()
    }

    pub fn verdict(&self) -> Verdict {
        check(&self.trace())
    }
}

impl State {
    fn attribute(&self, key: &[u8], bytes: Option<&[u8]>) -> Attr {
        let Some(bytes) = bytes else {
            return Attr::Absent;
        };
        let mut candidates: Vec<Attr> = Vec::new();
        for (seed_key, seed_bytes) in &self.seeds {
            if seed_key == key && seed_bytes == bytes {
                candidates.push(Attr::Seed);
            }
        }
        for (wref, write_key, write_bytes) in &self.writes {
            if write_key == key && write_bytes == bytes {
                candidates.push(Attr::Unique(*wref));
            }
        }
        match candidates.len() {
            0 => Attr::Unknown,
            1 => candidates[0],
            _ => Attr::Ambiguous,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rule {
    DirtyRead,
    StaleRead,
    FutureRead,
    SelfRead,
    UnstableRead,
    WriteCycle,
    LostUpdate,
    FabricatedValue,
}

impl Rule {
    pub fn label(self) -> &'static str {
        match self {
            Self::DirtyRead => "DirtyRead",
            Self::StaleRead => "StaleRead",
            Self::FutureRead => "FutureRead",
            Self::SelfRead => "SelfRead",
            Self::UnstableRead => "UnstableRead",
            Self::WriteCycle => "WriteCycle",
            Self::LostUpdate => "LostUpdate",
            Self::FabricatedValue => "FabricatedValue",
        }
    }
}

#[derive(Clone, Debug)]
pub struct Violation {
    pub rule: Rule,
    pub detail: String,
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.rule.label(), self.detail)
    }
}

#[derive(Clone, Debug, Default)]
pub struct Coverage {
    pub dirty: usize,
    pub unknown: usize,
    pub boundary: usize,
    pub stability: usize,
    pub cycles: usize,
    pub baselines: usize,
    pub events: usize,
}

#[derive(Clone, Debug, Default)]
pub struct Verdict {
    pub violations: Vec<Violation>,
    pub inconclusive: usize,
    pub coverage: Coverage,
}

impl Verdict {
    pub fn accepted(&self) -> bool {
        self.violations.is_empty()
    }

    pub fn describe(&self) -> String {
        let mut out = String::new();
        for v in &self.violations {
            out.push_str(&format!("  {v}\n"));
        }
        out.push_str(&format!(
            "  inconclusive={} coverage(dirty={}, unknown={}, boundary={}, stability={}, cycles={}, baselines={}, events={})",
            self.inconclusive,
            self.coverage.dirty,
            self.coverage.unknown,
            self.coverage.boundary,
            self.coverage.stability,
            self.coverage.cycles,
            self.coverage.baselines,
            self.coverage.events
        ));
        out
    }
}

/// a per-key version: `order` is 0 for the initial (absent) state, 1 for a seed, and grows
/// with commit order for committed writes
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Version {
    order: u64,
    wref: Option<WriteRef>,
}

struct Index {
    terminal: BTreeMap<TxnId, bool>,
    commit_idx: BTreeMap<TxnId, usize>,
    versions: BTreeMap<Vec<u8>, Vec<Version>>,
    order_of: BTreeMap<WriteRef, u64>,
}

fn build_index(events: &[Ev]) -> Index {
    let mut terminal = BTreeMap::new();
    let mut commit_idx = BTreeMap::new();
    for (idx, ev) in events.iter().enumerate() {
        match ev {
            Ev::Commit { txn, ok } => {
                terminal.insert(*txn, *ok);
                commit_idx.insert(*txn, idx);
            }
            Ev::Drop { txn } => {
                terminal.entry(*txn).or_insert(false);
            }
            _ => {}
        }
    }

    let mut versions: BTreeMap<Vec<u8>, Vec<Version>> = BTreeMap::new();
    // seed order per key
    let mut seed_order: BTreeMap<Vec<u8>, u64> = BTreeMap::new();
    for ev in events {
        if let Ev::Seed { key, .. } = ev {
            let next = seed_order.entry(key.clone()).or_insert(0);
            *next += 1;
            versions.entry(key.clone()).or_default().push(Version {
                order: *next,
                wref: None,
            });
        }
    }

    // committed writes, ordered per key by (commit event, op)
    let mut pending: BTreeMap<Vec<u8>, Vec<(usize, u32, WriteRef)>> = BTreeMap::new();
    for ev in events {
        let Ev::Write {
            txn,
            key,
            ok: true,
            wref: Some(wref),
            ..
        } = ev
        else {
            continue;
        };
        if terminal.get(txn).copied() != Some(true) {
            continue;
        }
        let commit = commit_idx.get(txn).copied().unwrap_or(usize::MAX);
        pending
            .entry(key.clone())
            .or_default()
            .push((commit, wref.op, *wref));
    }

    let mut order_of = BTreeMap::new();
    for (key, mut writes) in pending {
        writes.sort();
        let list = versions.entry(key).or_default();
        for (_, _, wref) in writes {
            let next = list.len() as u64 + 1;
            list.push(Version {
                order: next,
                wref: Some(wref),
            });
            order_of.insert(wref, next);
        }
    }

    Index {
        terminal,
        commit_idx,
        versions,
        order_of,
    }
}

pub fn check(events: &[Ev]) -> Verdict {
    let index = build_index(events);
    let mut verdict = Verdict {
        coverage: Coverage {
            events: events.len(),
            ..Coverage::default()
        },
        ..Verdict::default()
    };

    let mut inconclusive = 0usize;
    let mut seen: BTreeMap<(Owner, Vec<u8>), u64> = BTreeMap::new();
    let mut edges: BTreeMap<TxnId, Vec<(TxnId, bool)>> = BTreeMap::new();

    for (idx, ev) in events.iter().enumerate() {
        match ev {
            Ev::Write {
                txn, key, ok: true, ..
            } => {
                seen.remove(&(Owner::Txn(*txn), key.clone()));
            }
            Ev::Read {
                owner, key, attr, ..
            } => {
                let owner_txn = match owner {
                    Owner::Txn(t) => Some(*t),
                    Owner::View => None,
                };

                if *attr == Attr::Unknown {
                    verdict.coverage.unknown += 1;
                    verdict.violations.push(Violation {
                        rule: Rule::FabricatedValue,
                        detail: format!("read of key {key:?} has no writer in the trace"),
                    });
                    continue;
                }

                if let Attr::Unique(wref) = attr {
                    if Some(wref.txn) != owner_txn {
                        verdict.coverage.dirty += 1;
                    }
                    if Some(wref.txn) != owner_txn
                        && index.terminal.get(&wref.txn).copied() != Some(true)
                    {
                        verdict.violations.push(Violation {
                            rule: Rule::DirtyRead,
                            detail: format!("read of key {key:?} observed aborted write {wref:?}"),
                        });
                    }
                }

                let own_prior = match owner_txn {
                    Some(t) => own_write_before(events, t, key, idx),
                    None => None,
                };

                if let Some(prior) = own_prior {
                    if *attr == Attr::Ambiguous {
                        inconclusive += 1;
                        continue;
                    }
                    if *attr != Attr::Unique(prior) {
                        verdict.violations.push(Violation {
                            rule: Rule::SelfRead,
                            detail: format!(
                                "txn {owner_txn:?} read key {key:?} as {attr:?}, expected own write {prior:?}"
                            ),
                        });
                    }
                } else if let Some(snapshot) = snapshot_event(events, *owner, idx) {
                    let newest = newest_before(&index, key, snapshot);
                    match observed_order(&index, *attr) {
                        Some(order) => {
                            verdict.coverage.boundary += 1;
                            if let Some(newest) = newest
                                && order < newest.order
                            {
                                verdict.violations.push(Violation {
                                    rule: Rule::StaleRead,
                                    detail: format!(
                                        "read of key {key:?} at event {idx} returned order {order}, newest visible is {}",
                                        newest.order
                                    ),
                                });
                            }
                        }
                        None if *attr == Attr::Ambiguous => inconclusive += 1,
                        None => {}
                    }
                    if let Attr::Unique(wref) = attr
                        && let Some(commit) = index.commit_idx.get(&wref.txn)
                        && *commit > snapshot
                    {
                        verdict.violations.push(Violation {
                            rule: Rule::FutureRead,
                            detail: format!(
                                "read of key {key:?} at event {idx} observed {wref:?} committed at {commit}"
                            ),
                        });
                    }
                }

                if let Some(order) = observed_order(&index, *attr) {
                    let slot = (*owner, key.clone());
                    if let Some(prev) = seen.get(&slot) {
                        verdict.coverage.stability += 1;
                        if order < *prev {
                            verdict.violations.push(Violation {
                                rule: Rule::UnstableRead,
                                detail: format!(
                                    "read of key {key:?} moved from order {prev} back to {order}"
                                ),
                            });
                        }
                    }
                    seen.insert(slot, order);
                }

                if let Some(t) = owner_txn {
                    if let Attr::Unique(wref) = attr
                        && wref.txn != t
                    {
                        edges.entry(wref.txn).or_default().push((t, false));
                    }
                    if let Some(order) = observed_order(&index, *attr)
                        && let Some(list) = index.versions.get(key)
                    {
                        for version in list {
                            if version.order > order
                                && let Some(w) = version.wref
                                && w.txn != t
                            {
                                edges.entry(t).or_default().push((w.txn, false));
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }

    for list in index.versions.values() {
        let writes: Vec<WriteRef> = list.iter().filter_map(|v| v.wref).collect();
        for pair in writes.windows(2) {
            let (a, b) = (pair[0], pair[1]);
            if a.txn != b.txn {
                edges.entry(a.txn).or_default().push((b.txn, true));
            }
        }
    }

    let cycles = enumerate_cycles(&edges, 6);
    verdict.coverage.cycles = cycles.len();
    for cycle in &cycles {
        if cycle.1 {
            verdict.violations.push(Violation {
                rule: Rule::WriteCycle,
                detail: format!("cycle with a write edge: {:?}", cycle.0),
            });
        }
    }

    // lost update: two attempted writers of one key share a baseline read. both writing
    // is a violation; a conflicted write is exactly what first-writer-wins should produce,
    // so it still counts as a decidable sample
    let mut writes: Vec<(usize, TxnId, Vec<u8>, bool)> = Vec::new();
    for (idx, ev) in events.iter().enumerate() {
        if let Ev::Write { txn, key, ok, .. } = ev {
            writes.push((idx, *txn, key.clone(), *ok));
        }
    }
    for i in 0..writes.len() {
        for j in (i + 1)..writes.len() {
            let (ia, ta, ref ka, oka) = writes[i];
            let (ib, tb, ref kb, okb) = writes[j];
            if ta == tb || ka != kb {
                continue;
            }
            let (Some(ba), Some(bb)) = (
                baseline_before(events, ta, ka, ia),
                baseline_before(events, tb, kb, ib),
            ) else {
                continue;
            };
            let decidable = |attr: Attr| match attr {
                Attr::Seed | Attr::Absent => true,
                Attr::Unique(w) => index.terminal.get(&w.txn) == Some(&true),
                Attr::Ambiguous | Attr::Unknown => false,
            };
            if !decidable(ba) || !decidable(bb) {
                continue;
            }
            if ba != bb {
                continue;
            }
            verdict.coverage.baselines += 1;
            if oka
                && okb
                && index.terminal.get(&ta) == Some(&true)
                && index.terminal.get(&tb) == Some(&true)
            {
                verdict.violations.push(Violation {
                    rule: Rule::LostUpdate,
                    detail: format!(
                        "txns {ta:?} and {tb:?} both committed key {ka:?} from the same baseline"
                    ),
                });
            }
        }
    }

    verdict.inconclusive = inconclusive;
    verdict
}

fn baseline_before(events: &[Ev], txn: TxnId, key: &[u8], write_index: usize) -> Option<Attr> {
    let mut found = None;
    for ev in events.iter().take(write_index) {
        if let Ev::Read {
            owner: Owner::Txn(t),
            key: k,
            attr,
            ..
        } = ev
            && *t == txn
            && k.as_slice() == key
        {
            found = Some(*attr);
        }
    }
    found
}

fn snapshot_event(events: &[Ev], owner: Owner, read_idx: usize) -> Option<usize> {
    match owner {
        Owner::View => Some(read_idx),
        Owner::Txn(txn) => events
            .iter()
            .position(|ev| matches!(ev, Ev::Begin { txn: t } if *t == txn)),
    }
}

fn own_write_before(events: &[Ev], txn: TxnId, key: &[u8], read_idx: usize) -> Option<WriteRef> {
    let mut found = None;
    for ev in events.iter().take(read_idx) {
        if let Ev::Write {
            txn: t,
            key: k,
            ok: true,
            wref: Some(wref),
            ..
        } = ev
            && *t == txn
            && k.as_slice() == key
        {
            found = Some(*wref);
        }
    }
    found
}

fn newest_before(index: &Index, key: &[u8], snapshot: usize) -> Option<Version> {
    let list = index.versions.get(key)?;
    let mut newest = None;
    for version in list {
        let visible = match version.wref {
            None => true,
            Some(wref) => index
                .commit_idx
                .get(&wref.txn)
                .map(|idx| *idx < snapshot)
                .unwrap_or(false),
        };
        if visible {
            newest = Some(*version);
        }
    }
    newest
}

fn observed_order(index: &Index, attr: Attr) -> Option<u64> {
    match attr {
        Attr::Absent => Some(0),
        Attr::Seed => Some(1),
        Attr::Unique(wref) => index.order_of.get(&wref).copied(),
        _ => None,
    }
}

/// every elementary cycle up to `max_len`, canonically rooted at its smallest node;
/// the flag reports whether the cycle contains a write-write edge
fn enumerate_cycles(
    edges: &BTreeMap<TxnId, Vec<(TxnId, bool)>>,
    max_len: usize,
) -> Vec<(Vec<TxnId>, bool)> {
    let nodes: BTreeSet<TxnId> = edges
        .keys()
        .copied()
        .chain(edges.values().flatten().map(|(to, _)| *to))
        .collect();
    let mut out = Vec::new();
    for start in nodes.iter().copied() {
        let mut path = vec![start];
        dfs(edges, start, start, &mut path, false, max_len, &mut out);
    }
    out
}

fn dfs(
    edges: &BTreeMap<TxnId, Vec<(TxnId, bool)>>,
    start: TxnId,
    node: TxnId,
    path: &mut Vec<TxnId>,
    ww: bool,
    max_len: usize,
    out: &mut Vec<(Vec<TxnId>, bool)>,
) {
    if path.len() > max_len {
        return;
    }
    let Some(nexts) = edges.get(&node) else {
        return;
    };
    for (to, is_ww) in nexts {
        if *to == start && path.len() >= 2 {
            out.push((path.clone(), ww || *is_ww));
            continue;
        }
        if path.contains(to) || *to < start {
            continue;
        }
        path.push(*to);
        dfs(edges, start, *to, path, ww || *is_ww, max_len, out);
        path.pop();
    }
}
