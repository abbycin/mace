//! `mace-kv` maintenance command line tool.
//!
//! offline consistency and inactive-ledger maintenance:
//!
//! ```text
//! mace-kv check <db-dir>
//! mace-kv leak-check <db-dir> [--fix]
//! ```
//!
//! exit codes are stable:
//!
//! - `0`: the directory was checked and no damage was found
//! - `1`: damage was reported (each diagnostic names a category and its location)
//! - `2`: usage error, or a subcommand that could not be run
//!
//! `--help`/`--version` exit `0` and print to stdout; a usage error exits `2` and prints
//! to stderr, which is clap's default and the documented contract above.
//!
//! inspection is read-only; leak-check --fix adds missing inactive marks in one
//! metadata transaction. neither mode runs engine writeback recovery.
//!
//! fatal metadata page damage is not isolated: `btree-store` intentionally logs a
//! `BTREE_FATAL_*` diagnostic and then aborts, and this process terminates the same way;
//! such damage already means the metadata is unusable, and the log carries the file,
//! offset and generation.

use clap::{Parser, Subcommand};
use mace::Options;
use mace_kv_cli::{CheckReport, Diagnostic, check_database, leak_check_database};
use std::path::Path;
use std::process::ExitCode;

/// exit code of a clean check
const EXIT_CLEAN: u8 = 0;
/// exit code of a check that reported damage
const EXIT_DAMAGE: u8 = 1;

#[derive(Parser)]
#[command(
    name = "mace-kv",
    version,
    about = "mace-kv maintenance tooling",
    long_about = "mace-kv maintenance tooling: offline inspection and stat-ledger repair.\n\
                  a usage error exits 2, a reported damage exits 1, a clean check exits 0."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// check a database directory and report damage without modifying anything
    Check {
        /// database directory to check
        dir: std::path::PathBuf,
    },
    /// detect unreachable records missing from the inactive ledger, optionally repairing marks
    LeakCheck {
        /// database directory to check
        dir: std::path::PathBuf,
        /// add missing inactive marks to stat records
        #[arg(long)]
        fix: bool,
    },
}

fn main() -> ExitCode {
    match Cli::parse().command {
        Command::Check { dir } => check(&dir),
        Command::LeakCheck { dir, fix } => {
            let result = leak_check_database(&Options::new(&dir), fix);
            if let Some(fault) = &result.fault {
                eprintln!("leak-check repair failed: {fault}");
            }
            print_diagnostics(&result.report);
            for repair in &result.repairs {
                println!(
                    "repaired: {} bucket={} file={} added_inactive={} active_elems={}->{} active_size={}->{}",
                    repair.kind,
                    repair.bucket_id,
                    repair.file_id,
                    repair.added_inactive,
                    repair.active_elems.0,
                    repair.active_elems.1,
                    repair.active_size.0,
                    repair.active_size.1
                );
            }
            print_summary(&dir, &result.report);
            ExitCode::from(if result.fault.is_some() {
                2
            } else {
                result.report.exit_code() as u8
            })
        }
    }
}

fn check(dir: &Path) -> ExitCode {
    let options = Options::new(dir);
    let report = check_database(&options);
    print_report(dir, &report);
    match report.exit_code() {
        0 => ExitCode::from(EXIT_CLEAN),
        _ => ExitCode::from(EXIT_DAMAGE),
    }
}

fn print_report(dir: &Path, report: &CheckReport) {
    print_diagnostics(report);
    print_summary(dir, report);
}

fn print_diagnostics(report: &CheckReport) {
    for diagnostic in &report.diagnostics {
        println!("{}", render(diagnostic));
    }
}

fn print_summary(dir: &Path, report: &CheckReport) {
    let counts: Vec<String> = report
        .summary()
        .into_iter()
        .map(|(category, count)| format!("{category}={count}"))
        .collect();
    let dir = dir.display();
    if counts.is_empty() {
        println!("{dir}: ok");
    } else {
        println!(
            "{dir}: {} diagnostic(s) [{}]",
            report.diagnostics.len(),
            counts.join(" ")
        );
    }
}

/// one line per diagnostic: stable category, then the location fields that are known
fn render(diagnostic: &Diagnostic) -> String {
    let mut location = String::new();
    match diagnostic.bucket_id {
        Some(bucket) => location.push_str(&format!(" bucket={bucket}")),
        None => location.push_str(" bucket=-"),
    }
    match diagnostic.file_id {
        Some(file) => location.push_str(&format!(" file={file}")),
        None => location.push_str(" file=-"),
    }
    match diagnostic.address {
        Some(address) => location.push_str(&format!(" addr=0x{address:x}")),
        None => location.push_str(" addr=-"),
    }
    format!(
        "{}:{}: {}",
        diagnostic.category.name(),
        location,
        diagnostic.detail
    )
}
