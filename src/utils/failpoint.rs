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
    Rename,
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
            "rename" => Some(Self::Rename),
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
            Self::Rename => "mace_fs_rename",
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
}

impl State {
    fn new() -> Self {
        Self {
            raw: String::new(),
            named_rules: HashMap::new(),
            fs_rules: Vec::new(),
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
    }

    fn hit_named(&mut self, name: &str) -> Option<FailAction> {
        let rule = self.named_rules.get_mut(name)?;
        rule.hit().then_some(rule.action)
    }

    fn hit_fs(&mut self, op: FsOp, path: &str) -> Option<ActionSpec> {
        for rule in self.fs_rules.iter_mut().rev() {
            if !rule.matches(op, path) {
                continue;
            }
            if !rule.hit() {
                return None;
            }
            return Some(rule.action);
        }
        None
    }
}

enum ParsedAction {
    Off,
    Active(ActionSpec),
}

fn normalize_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
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

#[cfg(test)]
mod tests {
    use super::{ActionSpec, FailAction, FsOp, ParsedRules, State, normalize_path, parse_rules};
    use std::{io::ErrorKind, path::Path};

    fn state_with(raw: &str) -> State {
        let ParsedRules {
            named_rules,
            fs_rules,
        } = parse_rules(raw);
        State {
            raw: raw.to_string(),
            named_rules,
            fs_rules,
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
    fn normalize_path_replaces_backslashes() {
        assert_eq!(normalize_path(Path::new(r"foo\bar\baz")), "foo/bar/baz");
    }
}
