use crate::OpCode;
use std::{
    collections::HashMap,
    io,
    path::Path,
    sync::{Mutex, OnceLock},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FailAction {
    Panic,
    IoError,
    Abort,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FsOp {
    Open,
    TryExists,
    CreateDirAll,
    ReadDir,
    RemoveFile,
    SyncDir,
}

impl FsOp {
    fn parse(name: &str) -> Option<Self> {
        match name {
            "open" => Some(Self::Open),
            "try_exists" => Some(Self::TryExists),
            "create_dir_all" => Some(Self::CreateDirAll),
            "read_dir" => Some(Self::ReadDir),
            "remove_file" => Some(Self::RemoveFile),
            "sync_dir" => Some(Self::SyncDir),
            _ => None,
        }
    }

    fn rule_name(self) -> &'static str {
        match self {
            Self::Open => "mace_fs_open",
            Self::TryExists => "mace_fs_try_exists",
            Self::CreateDirAll => "mace_fs_create_dir_all",
            Self::ReadDir => "mace_fs_read_dir",
            Self::RemoveFile => "mace_fs_remove_file",
            Self::SyncDir => "mace_fs_sync_dir",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ActionSpec {
    action: FailAction,
    io_kind: io::ErrorKind,
}

#[derive(Clone, Copy, Debug)]
struct Rule {
    action: FailAction,
    nth: Option<u64>,
    hits: u64,
}

#[derive(Clone, Debug)]
struct FsRule {
    op: FsOp,
    matcher: Option<String>,
    action: ActionSpec,
    nth: Option<u64>,
    hits: u64,
}

impl Rule {
    fn hit(&mut self) -> bool {
        self.hits += 1;
        match self.nth {
            Some(nth) => self.hits == nth,
            None => true,
        }
    }
}

impl FsRule {
    fn matches(&self, op: FsOp, path: &str) -> bool {
        self.op == op
            && self
                .matcher
                .as_ref()
                .is_none_or(|matcher| path.contains(matcher))
    }

    fn hit(&mut self) -> bool {
        self.hits += 1;
        match self.nth {
            Some(nth) => self.hits == nth,
            None => true,
        }
    }
}

#[derive(Default)]
struct ParsedRules {
    named_rules: HashMap<String, Rule>,
    fs_rules: Vec<FsRule>,
}

struct State {
    raw: String,
    named_rules: HashMap<String, Rule>,
    fs_rules: Vec<FsRule>,
    /// in-process rules override environment rules
    override_rules: HashMap<String, Rule>,
    /// total consultations per named rule since process start, independent of
    /// nth semantics (a rule consulted but not yet acting still counts)
    named_hits: HashMap<String, u64>,
    /// same accounting for fs rules, keyed by op plus matcher
    fs_hits: HashMap<String, u64>,
}

impl State {
    fn new() -> Self {
        Self {
            raw: String::new(),
            named_rules: HashMap::new(),
            fs_rules: Vec::new(),
            override_rules: HashMap::new(),
            named_hits: HashMap::new(),
            fs_hits: HashMap::new(),
        }
    }

    fn refresh(&mut self) {
        let current = std::env::var("MACE_FAILPOINT").unwrap_or_default();
        if current == self.raw {
            return;
        }
        self.raw = current.clone();
        let parsed = parse_rules(&current);
        self.named_rules = parsed.named_rules;
        self.fs_rules = parsed.fs_rules;
        // preserve override hit counters across refreshes
        for (name, rule) in self.override_rules.iter() {
            self.named_rules.insert(name.clone(), *rule);
        }
    }

    fn hit_named(&mut self, name: &str) -> Option<FailAction> {
        let rule = self.named_rules.get_mut(name)?;
        *self.named_hits.entry(name.to_string()).or_insert(0) += 1;
        rule.hit().then_some(rule.action)
    }

    fn hit_fs(&mut self, op: FsOp, path: &str) -> Option<ActionSpec> {
        for rule in self.fs_rules.iter_mut().rev() {
            if !rule.matches(op, path) {
                continue;
            }
            let key = fs_hit_key(rule.op, rule.matcher.as_deref());
            *self.fs_hits.entry(key).or_insert(0) += 1;
            if !rule.hit() {
                return None;
            }
            return Some(rule.action);
        }
        None
    }

    /// whether any effective rule reached its acting consultation (the nth
    /// hit for nth rules, the first otherwise); raw hit totals cannot make
    /// this call because they also count benign pre-nth consultations
    fn any_actioned(&self) -> bool {
        self.named_rules.iter().any(|(name, rule)| {
            let hits = self.named_hits.get(name).copied().unwrap_or(0);
            hits >= rule.nth.unwrap_or(1)
        }) || self.fs_rules.iter().any(|rule| {
            let key = fs_hit_key(rule.op, rule.matcher.as_deref());
            let hits = self.fs_hits.get(&key).copied().unwrap_or(0);
            hits >= rule.nth.unwrap_or(1)
        })
    }
}

enum ParsedAction {
    Off,
    Active(ActionSpec),
}

fn normalize_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

/// canonical hit-accounting key for one fs rule
fn fs_hit_key(op: FsOp, matcher: Option<&str>) -> String {
    format!("{}[{}]", op.rule_name(), matcher.unwrap_or(""))
}

fn parse_rules(raw: &str) -> ParsedRules {
    let mut out = ParsedRules::default();

    for token in raw.split(',') {
        let token = token.trim();
        if token.is_empty() {
            continue;
        }

        let (name_raw, body) = token
            .split_once('=')
            .or_else(|| token.split_once(':'))
            .unwrap_or((token, "panic"));

        let name_raw = name_raw.trim();
        if name_raw.is_empty() {
            continue;
        }

        let (action_raw, nth_raw) = body.trim().split_once('@').unwrap_or((body.trim(), ""));
        let Some(action) = parse_action(action_raw.trim()) else {
            continue;
        };
        if matches!(action, ParsedAction::Off) {
            continue;
        }
        let ParsedAction::Active(action) = action else {
            unreachable!()
        };

        let nth = if nth_raw.is_empty() {
            None
        } else {
            nth_raw.trim().parse::<u64>().ok().filter(|x| *x > 0)
        };

        if let Some((op, matcher)) = parse_fs_name(name_raw) {
            out.fs_rules.push(FsRule {
                op,
                matcher,
                action,
                nth,
                hits: 0,
            });
            continue;
        }

        out.named_rules.insert(
            name_raw.to_string(),
            Rule {
                action: action.action,
                nth,
                hits: 0,
            },
        );
    }

    out
}

fn parse_fs_name(raw: &str) -> Option<(FsOp, Option<String>)> {
    let (name, matcher) = if let Some((prefix, suffix)) = raw.split_once('[') {
        let matcher = suffix.strip_suffix(']')?;
        let matcher = normalize_matcher(matcher);
        (prefix.trim(), matcher)
    } else {
        (raw.trim(), None)
    };

    let op = FsOp::parse(name.strip_prefix("mace_fs_")?)?;
    Some((op, matcher))
}

fn normalize_matcher(raw: &str) -> Option<String> {
    let normalized = raw.trim().replace('\\', "/");
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

fn parse_action(raw: &str) -> Option<ParsedAction> {
    match raw {
        "panic" => Some(ParsedAction::Active(ActionSpec {
            action: FailAction::Panic,
            io_kind: io::ErrorKind::Other,
        })),
        "io" => Some(ParsedAction::Active(ActionSpec {
            action: FailAction::IoError,
            io_kind: io::ErrorKind::Other,
        })),
        "abort" => Some(ParsedAction::Active(ActionSpec {
            action: FailAction::Abort,
            io_kind: io::ErrorKind::Other,
        })),
        "off" => Some(ParsedAction::Off),
        _ => {
            let kind_raw = raw.strip_prefix("io(")?.strip_suffix(')')?;
            let io_kind = parse_io_kind(kind_raw)?;
            Some(ParsedAction::Active(ActionSpec {
                action: FailAction::IoError,
                io_kind,
            }))
        }
    }
}

fn parse_io_kind(raw: &str) -> Option<io::ErrorKind> {
    let key = raw.trim().to_ascii_lowercase().replace(['-', ' '], "_");
    match key.as_str() {
        "other" => Some(io::ErrorKind::Other),
        "not_found" => Some(io::ErrorKind::NotFound),
        "permission_denied" => Some(io::ErrorKind::PermissionDenied),
        "connection_refused" => Some(io::ErrorKind::ConnectionRefused),
        "connection_reset" => Some(io::ErrorKind::ConnectionReset),
        "connection_aborted" => Some(io::ErrorKind::ConnectionAborted),
        "not_connected" => Some(io::ErrorKind::NotConnected),
        "addr_in_use" => Some(io::ErrorKind::AddrInUse),
        "addr_not_available" => Some(io::ErrorKind::AddrNotAvailable),
        "broken_pipe" => Some(io::ErrorKind::BrokenPipe),
        "already_exists" => Some(io::ErrorKind::AlreadyExists),
        "would_block" => Some(io::ErrorKind::WouldBlock),
        "invalid_input" => Some(io::ErrorKind::InvalidInput),
        "invalid_data" => Some(io::ErrorKind::InvalidData),
        "timed_out" => Some(io::ErrorKind::TimedOut),
        "write_zero" => Some(io::ErrorKind::WriteZero),
        "interrupted" => Some(io::ErrorKind::Interrupted),
        "unsupported" => Some(io::ErrorKind::Unsupported),
        "unexpected_eof" => Some(io::ErrorKind::UnexpectedEof),
        "out_of_memory" => Some(io::ErrorKind::OutOfMemory),
        _ => None,
    }
}

fn global_state() -> &'static Mutex<State> {
    static STATE: OnceLock<Mutex<State>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new(State::new()))
}

pub(crate) fn check(name: &str) -> Result<(), OpCode> {
    let mut lk = global_state().lock().expect("failpoint lock poisoned");
    lk.refresh();
    match lk.hit_named(name) {
        None => Ok(()),
        Some(FailAction::Panic) => panic!("failpoint panic: {name}"),
        Some(FailAction::Abort) => std::process::abort(),
        Some(FailAction::IoError) => Err(OpCode::IoError),
    }
}

/// arm in-process rules that override environment rules; consumers live in
/// testing (extra_check) and unit tests
#[cfg(any(test, feature = "extra_check"))]
pub(crate) fn arm_rules(raw: &str) {
    let mut lk = global_state().lock().expect("failpoint lock poisoned");
    let parsed = parse_rules(raw);
    for (name, rule) in parsed.named_rules {
        lk.named_rules.insert(name.clone(), rule);
        lk.override_rules.insert(name, rule);
    }
}

/// scope guard for `arm_rules_scoped`: on drop, every armed name restores the
/// rule it shadowed at arm time per table (strict LIFO), so nested same-name
/// scopes unwind correctly, same-process tests can neither leak overrides
/// into each other nor silence env-derived rules, and an env-derived rule is
/// never promoted into override_rules (which refresh would keep forever)
#[cfg(any(test, feature = "extra_check"))]
pub(crate) struct FailpointScope {
    /// (name, rule shadowed in override_rules, rule shadowed in named_rules)
    armed: Vec<(String, Option<Rule>, Option<Rule>)>,
}

#[cfg(any(test, feature = "extra_check"))]
fn disarm_scoped(lk: &mut State, armed: &[(String, Option<Rule>, Option<Rule>)]) {
    for (name, override_prev, named_prev) in armed {
        // restore each table to exactly what this scope covered over
        match override_prev {
            Some(rule) => {
                lk.override_rules.insert(name.clone(), *rule);
            }
            None => {
                lk.override_rules.remove(name);
            }
        }
        match named_prev {
            Some(rule) => {
                lk.named_rules.insert(name.clone(), *rule);
            }
            None => {
                // refresh only re-merges overrides; without removing the
                // effective copy the armed rule would stay active until the
                // next env change
                lk.named_rules.remove(name);
            }
        }
    }
}

#[cfg(any(test, feature = "extra_check"))]
impl Drop for FailpointScope {
    fn drop(&mut self) {
        let mut lk = global_state().lock().expect("failpoint lock poisoned");
        disarm_scoped(&mut lk, &self.armed);
    }
}

/// arm named rules for the lifetime of the returned scope; fs rules are not
/// supported (the scoped use case is in-process crash-site arming)
#[cfg(any(test, feature = "extra_check"))]
pub(crate) fn arm_rules_scoped(raw: &str) -> FailpointScope {
    let parsed = parse_rules(raw);
    assert!(
        parsed.fs_rules.is_empty(),
        "arm_rules_scoped does not support fs rules ({raw:?}): scoped arming targets in-process named sites only"
    );
    let mut lk = global_state().lock().expect("failpoint lock poisoned");
    let mut armed = Vec::with_capacity(parsed.named_rules.len());
    for (name, rule) in parsed.named_rules {
        let override_prev = lk.override_rules.get(&name).cloned();
        let named_prev = lk.named_rules.get(&name).cloned();
        lk.override_rules.insert(name.clone(), rule);
        lk.named_rules.insert(name.clone(), rule);
        armed.push((name, override_prev, named_prev));
    }
    FailpointScope { armed }
}

pub(crate) fn crash(name: &str) {
    let mut lk = global_state().lock().expect("failpoint lock poisoned");
    lk.refresh();
    match lk.hit_named(name) {
        None => {}
        Some(FailAction::Panic) => panic!("failpoint panic: {name}"),
        Some(FailAction::Abort) => std::process::abort(),
        Some(FailAction::IoError) => panic!("failpoint io translated to panic: {name}"),
    }
}

pub(crate) fn check_fs(op: FsOp, path: &Path) -> Result<(), io::Error> {
    let mut lk = global_state().lock().expect("failpoint lock poisoned");
    lk.refresh();
    if lk.fs_rules.is_empty() {
        return Ok(());
    }
    let path_text = normalize_path(path);
    match lk.hit_fs(op, &path_text) {
        None => Ok(()),
        Some(ActionSpec {
            action: FailAction::Panic,
            ..
        }) => panic!("failpoint panic: {} path={}", op.rule_name(), path_text),
        Some(ActionSpec {
            action: FailAction::Abort,
            ..
        }) => std::process::abort(),
        Some(ActionSpec {
            action: FailAction::IoError,
            io_kind,
        }) => Err(io::Error::new(
            io_kind,
            format!("failpoint io error: {} path={}", op.rule_name(), path_text),
        )),
    }
}

/// total consultations across every armed rule since process start; the
/// crash-window wait loops only need "did any injection fire" and must not
/// depend on knowing the rule name (env-derived and in-process armed rules
/// coexist in the crash children)
pub fn hits_total() -> u64 {
    let lk = global_state().lock().expect("failpoint lock poisoned");
    lk.named_hits.values().sum::<u64>() + lk.fs_hits.values().sum::<u64>()
}

/// total consultations of a named rule since process start, regardless of
/// whether the action fired (nth rules only act on their nth consultation)
pub fn hit_count(name: &str) -> u64 {
    let lk = global_state().lock().expect("failpoint lock poisoned");
    lk.named_hits.get(name).copied().unwrap_or(0)
}

/// whether any armed rule has been consulted enough times to act; crash-window
/// waits use this to attribute a timeout without mistaking benign pre-nth
/// consultations of nth>1 rules for a survived injection
pub fn any_rule_actioned() -> bool {
    let lk = global_state().lock().expect("failpoint lock poisoned");
    lk.any_actioned()
}

/// every active rule with action, nth, hit count and override marker; used by
/// tests to attribute a crash-window timeout to "site never reached" versus
/// "reached but did not abort"
pub fn snapshot() -> String {
    let lk = global_state().lock().expect("failpoint lock poisoned");
    let mut out = String::from("failpoint rules:");
    for (name, rule) in &lk.named_rules {
        let hits = lk.named_hits.get(name).copied().unwrap_or(0);
        let marker = if lk.override_rules.contains_key(name) {
            " [override]"
        } else {
            ""
        };
        out.push_str(&format!(
            "\n  {name} action={:?} nth={:?} hits={hits}{marker}",
            rule.action, rule.nth
        ));
    }
    for rule in &lk.fs_rules {
        let key = fs_hit_key(rule.op, rule.matcher.as_deref());
        let hits = lk.fs_hits.get(&key).copied().unwrap_or(0);
        out.push_str(&format!(
            "\n  {} matcher={:?} nth={:?} hits={hits}",
            rule.op.rule_name(),
            rule.matcher,
            rule.nth
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{
        ActionSpec, FailAction, FsOp, ParsedRules, State, check, disarm_scoped, fs_hit_key,
        normalize_path, parse_rules, snapshot,
    };
    use crate::{
        OpCode,
        utils::failpoint::{arm_rules, arm_rules_scoped, hit_count},
    };
    use std::{collections::HashMap, io::ErrorKind, path::Path};

    fn state_with(raw: &str) -> State {
        let ParsedRules {
            named_rules,
            fs_rules,
        } = parse_rules(raw);
        State {
            raw: raw.to_string(),
            named_rules,
            fs_rules,
            override_rules: HashMap::new(),
            named_hits: HashMap::new(),
            fs_hits: HashMap::new(),
        }
    }

    #[test]
    fn named_rule_keeps_nth_semantics() {
        let mut state = state_with("mace_txn_commit_begin=io@2");

        assert_eq!(state.hit_named("mace_txn_commit_begin"), None);
        assert_eq!(
            state.hit_named("mace_txn_commit_begin"),
            Some(FailAction::IoError)
        );
        assert_eq!(state.hit_named("mace_txn_commit_begin"), None);
    }

    #[test]
    fn fs_rule_matches_normalized_path_and_nth_hit() {
        let mut state = state_with(r"mace_fs_create_dir_all[tmp\mace]=io(permission_denied)@2");
        let path = normalize_path(Path::new("tmp/mace/db"));

        assert_eq!(state.hit_fs(FsOp::CreateDirAll, &path), None);
        assert_eq!(
            state.hit_fs(FsOp::CreateDirAll, &path),
            Some(ActionSpec {
                action: FailAction::IoError,
                io_kind: ErrorKind::PermissionDenied,
            })
        );
        assert_eq!(state.hit_fs(FsOp::CreateDirAll, &path), None);
    }

    #[test]
    fn fs_rule_ignores_non_matching_path() {
        let mut state = state_with("mace_fs_remove_file[/blob/]=io(not_found)");

        assert_eq!(state.hit_fs(FsOp::RemoveFile, "/data/file"), None);
        assert_eq!(
            state.hit_fs(FsOp::RemoveFile, "/blob/file"),
            Some(ActionSpec {
                action: FailAction::IoError,
                io_kind: ErrorKind::NotFound,
            })
        );
    }

    #[test]
    fn fs_rule_last_match_wins() {
        let mut state = state_with(
            "mace_fs_open[/data/]=io(not_found),mace_fs_open[/data/]=io(permission_denied)",
        );

        assert_eq!(
            state.hit_fs(FsOp::Open, "/data/001"),
            Some(ActionSpec {
                action: FailAction::IoError,
                io_kind: ErrorKind::PermissionDenied,
            })
        );
    }

    #[test]
    fn scoped_arm_removes_override_and_effective_rule_on_drop() {
        {
            let _scope = arm_rules_scoped("mace_unit_scoped_probe=io");
            assert_eq!(check("mace_unit_scoped_probe"), Err(OpCode::IoError));
        }

        assert_eq!(check("mace_unit_scoped_probe"), Ok(()));
        let snap = snapshot();
        assert!(!snap.contains("mace_unit_scoped_probe"));
    }

    #[test]
    fn actioned_predicate_requires_reaching_the_nth_consultation() {
        const NAME: &str = "mace_unit_actioned_probe";
        let mut state = state_with(&format!("{NAME}=io@3"));
        assert!(
            !state.any_actioned(),
            "zero consultations must not count as acted"
        );

        assert_eq!(state.hit_named(NAME), None, "nth=3 swallows hit 1");
        assert!(
            !state.any_actioned(),
            "pre-nth consultation must stay benign"
        );
        assert_eq!(state.hit_named(NAME), None, "nth=3 swallows hit 2");
        assert!(!state.any_actioned());
        assert_eq!(
            state.hit_named(NAME),
            Some(FailAction::IoError),
            "nth=3 acts on hit 3"
        );
        assert!(
            state.any_actioned(),
            "reaching the nth consultation must report acted"
        );
    }

    #[test]
    fn scoped_drop_restores_shadowed_rule_lifo() {
        // state-level simulation of the env interplay: the state carries an
        // env-derived NAME=io (no nth) as the effective rule, then a scope
        // arms NAME=io@3 over it (shadowing it), then the scope drops
        const NAME: &str = "mace_unit_scoped_env_probe";
        let mut state = state_with(&format!("{NAME}=io"));

        // arm exactly as arm_rules_scoped does: capture per-table shadows
        // the env rule lives only in named_rules, so override_prev is None —
        // disarm must NOT promote it into override_rules
        let scope_rule = parse_rules(&format!("{NAME}=io@3"))
            .named_rules
            .remove(NAME)
            .expect("scope rule must parse");
        let override_prev = state.override_rules.get(NAME).cloned();
        let named_prev = state.named_rules.get(NAME).cloned();
        assert!(
            override_prev.is_none(),
            "env source must not sit in overrides"
        );
        state.override_rules.insert(NAME.to_string(), scope_rule);
        state.named_rules.insert(NAME.to_string(), scope_rule);
        assert_eq!(state.hit_named(NAME), None, "nth=3 swallows hit 1");

        // disarm restores each table independently (LIFO)
        disarm_scoped(&mut state, &[(NAME.to_string(), override_prev, named_prev)]);
        assert!(
            !state.override_rules.contains_key(NAME),
            "env-derived rule must never be promoted into override_rules"
        );
        assert_eq!(
            state.hit_named(NAME),
            Some(FailAction::IoError),
            "the env-derived no-nth rule must be effective again"
        );
    }

    #[test]
    fn nested_same_name_scopes_unwind_lifo() {
        // inner drop must restore the outer override, not erase it
        const NAME: &str = "mace_unit_nested_probe";
        let mut state = state_with("unrelated=panic");

        // outer scope: io@2
        let outer = parse_rules(&format!("{NAME}=io@2"))
            .named_rules
            .remove(NAME)
            .unwrap();
        state.override_rules.insert(NAME.to_string(), outer);
        state.named_rules.insert(NAME.to_string(), outer);

        // inner scope: panic, shadowing the outer override
        let inner = parse_rules(&format!("{NAME}=panic"))
            .named_rules
            .remove(NAME)
            .unwrap();
        let inner_override_prev = state.override_rules.get(NAME).cloned();
        let inner_named_prev = state.named_rules.get(NAME).cloned();
        state.override_rules.insert(NAME.to_string(), inner);
        state.named_rules.insert(NAME.to_string(), inner);

        // inner drops first: the outer io@2 override comes back in BOTH tables
        disarm_scoped(
            &mut state,
            &[(NAME.to_string(), inner_override_prev, inner_named_prev)],
        );
        assert!(state.override_rules.contains_key(NAME));
        assert_eq!(state.hit_named(NAME), None, "outer nth=2 swallows hit 1");
        assert_eq!(
            state.hit_named(NAME),
            Some(FailAction::IoError),
            "outer override fires on its second consultation"
        );
    }

    #[test]
    fn normalize_path_replaces_backslashes() {
        assert_eq!(normalize_path(Path::new(r"foo\bar\baz")), "foo/bar/baz");
    }

    #[test]
    fn named_hit_count_tracks_consultations_independent_of_nth() {
        arm_rules("mace_unit_hit_probe=io@2");

        assert_eq!(hit_count("mace_unit_hit_probe"), 0);
        assert_eq!(check("mace_unit_hit_probe"), Ok(()));
        assert_eq!(hit_count("mace_unit_hit_probe"), 1);
        assert_eq!(check("mace_unit_hit_probe"), Err(OpCode::IoError));
        assert_eq!(hit_count("mace_unit_hit_probe"), 2);
        assert_eq!(check("mace_unit_hit_probe"), Ok(()));
        assert_eq!(hit_count("mace_unit_hit_probe"), 3);

        let snap = snapshot();
        assert!(snap.contains("mace_unit_hit_probe"));
        assert!(snap.contains("hits=3"));
        assert!(snap.contains("[override]"));
    }

    #[test]
    fn fs_hit_counts_track_consultations() {
        let mut state = state_with("mace_fs_sync_dir[/data/]=panic@2");

        assert!(state.hit_fs(FsOp::SyncDir, "/data/x").is_none());
        assert_eq!(
            state
                .fs_hits
                .get(&fs_hit_key(FsOp::SyncDir, Some("/data/"))),
            Some(&1)
        );
        assert!(state.hit_fs(FsOp::SyncDir, "/data/x").is_some());
        assert_eq!(
            state
                .fs_hits
                .get(&fs_hit_key(FsOp::SyncDir, Some("/data/"))),
            Some(&2)
        );
    }
}
