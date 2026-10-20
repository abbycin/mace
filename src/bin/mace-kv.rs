//! `mace-kv` maintenance command line tool.
//!
//! one read-only subcommand today:
//!
//! ```text
//! mace-kv check <db-dir>
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
//! the tool is read-only: it never creates, deletes or repairs a file, and it never
//! runs the writeback recovery that opening a [`mace::Mace`] instance would run.
//!
//! fatal metadata page damage is not isolated: `btree-store` intentionally logs a
//! `BTREE_FATAL_*` diagnostic and then aborts, and this process terminates the same way;
//! such damage already means the metadata is unusable, and the log carries the file,
//! offset and generation.

use clap::{Parser, Subcommand};
use mace::{CheckReport, Diagnostic, Options, check_database};
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
    long_about = "mace-kv maintenance tooling: read-only inspection of a mace database directory.\n\
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
}

fn main() -> ExitCode {
    match Cli::parse().command {
        Command::Check { dir } => check(&dir),
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
    for diagnostic in &report.diagnostics {
        println!("{}", render(diagnostic));
    }
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
