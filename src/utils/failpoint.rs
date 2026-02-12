use crate::OpCode;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

#[derive(Clone, Copy)]
pub(crate) enum FailAction {
    Panic,
    IoError,
    Abort,
}

#[derive(Clone, Copy)]
struct Rule {
    action: FailAction,
    nth: Option<u64>,
    hits: u64,
}

struct State {
    raw: String,
    rules: HashMap<String, Rule>,
}

impl State {
    fn new() -> Self {
        Self {
            raw: String::new(),
            rules: HashMap::new(),
        }
    }

    fn refresh(&mut self) {
        let current = std::env::var("MACE_FAILPOINT").unwrap_or_default();
        if current == self.raw {
            return;
        }
        self.raw = current.clone();
        self.rules = parse_rules(&current);
    }

    fn hit(&mut self, name: &str) -> Option<FailAction> {
        self.refresh();
        let rule = self.rules.get_mut(name)?;
        rule.hits += 1;
        if let Some(nth) = rule.nth
            && rule.hits != nth
        {
            return None;
        }
        Some(rule.action)
    }
}

fn parse_rules(raw: &str) -> HashMap<String, Rule> {
    let mut out = HashMap::new();

    for token in raw.split(',') {
        let token = token.trim();
        if token.is_empty() {
            continue;
        }

        let (name, body) = token
            .split_once('=')
            .or_else(|| token.split_once(':'))
            .unwrap_or((token, "panic"));

        if name.is_empty() {
            continue;
        }

        let (action_raw, nth_raw) = body.split_once('@').unwrap_or((body, ""));
        let action = match action_raw {
            "panic" => FailAction::Panic,
            "io" => FailAction::IoError,
            "abort" => FailAction::Abort,
            "off" => continue,
            _ => continue,
        };

        let nth = if nth_raw.is_empty() {
            None
        } else {
            nth_raw.parse::<u64>().ok().filter(|x| *x > 0)
        };

        out.insert(
            name.to_string(),
            Rule {
                action,
                nth,
                hits: 0,
            },
        );
    }

    out
}

fn global_state() -> &'static Mutex<State> {
    static STATE: OnceLock<Mutex<State>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new(State::new()))
}

pub(crate) fn check(name: &str) -> Result<(), OpCode> {
    let mut lk = global_state().lock().expect("failpoint lock poisoned");
    match lk.hit(name) {
        None => Ok(()),
        Some(FailAction::Panic) => panic!("failpoint panic: {name}"),
        Some(FailAction::Abort) => std::process::abort(),
        Some(FailAction::IoError) => Err(OpCode::IoError),
    }
}

pub(crate) fn crash(name: &str) {
    let mut lk = global_state().lock().expect("failpoint lock poisoned");
    match lk.hit(name) {
        None => {}
        Some(FailAction::Panic) => panic!("failpoint panic: {name}"),
        Some(FailAction::Abort) => std::process::abort(),
        Some(FailAction::IoError) => panic!("failpoint io translated to panic: {name}"),
    }
}
