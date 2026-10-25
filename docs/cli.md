# mace-kv CLI

`mace-kv` is the offline maintenance tool for Mace databases. It provides two commands:

```text
mace-kv check <db-dir>
mace-kv leak-check <db-dir> [--fix]
```

Both commands inspect a closed database directory without opening a runtime `Mace` instance. They do
not run writeback recovery or start background services. `check` is read-only. `leak-check` is
read-only unless `--fix` is supplied; repair adds missing inactive marks to stat records in one
metadata transaction. It never deletes payload files.

## General conventions

### Command form

```text
mace-kv <COMMAND> [OPTIONS]
```

Help:

```bash
mace-kv --help
mace-kv check --help
mace-kv leak-check --help
```

### Exit codes

| Exit code | Meaning |
| --- | --- |
| `0` | The check completed without diagnostics, or `--help` / `--version` was requested |
| `1` | The check found one or more diagnostics |
| `2` | Usage error or repair/open failure |

### Output

Each diagnostic is printed to stdout as one line:

```text
<category>: bucket=<id or -> file=<id or -> addr=<hex address or ->: <detail>
```

The final line is either `<db-dir>: ok` or a diagnostic count and per-category totals. A successful
repair also prints one `repaired:` line per updated stat record before the final summary. Fatal
metadata page damage is not converted to a diagnostic: btree-store logs a `BTREE_FATAL_*` message and
aborts because the metadata store cannot be safely inspected.

## `check`

### Usage

```bash
mace-kv check <db-dir>
```

`check` validates the database metadata, bucket catalog, intervals, per-file stats, data/blob
relocation tables, payload files, lifecycle records and WAL framing. It checks that page-table and
page-chain references resolve within the published metadata and payload inventory. It also reports
payload files that are not named by a stat record, orphan marker or obsolete entry.

The checker opens metadata read-only and does not modify the database. It is intended for a closed,
stable database. Do not run it against a database being written by another process; direct external
writes that bypass Mace's locking protocol also invalidate the stable-file assumption.

## `leak-check`

### Usage

```bash
mace-kv leak-check <db-dir>
mace-kv leak-check <db-dir> --fix
```

`leak-check` runs the consistency checks and derives reachability independently for each bucket,
starting from that bucket's fixed root PID. It follows data-page links and references, including
history and remote blob references, then derives the corresponding blob reachability. It compares
the resulting reachable addresses with the relocation tables and inactive masks in the stat records.

An unreachable retained relocation that is not marked inactive is reported as `junk_leak`. This is
specifically a missing inactive-ledger mark; ordinary records that remain until normal GC cleanup are
not classified as leaks. If the checker cannot establish a complete and safe reference closure, it
reports `leak_undecidable` and does not repair that bucket or file.

Without `--fix`, metadata is opened read-only. With `--fix`, the CLI opens the metadata store for
writing under its exclusive lock, adds missing inactive marks for safely classified records, and
updates the corresponding stat counts and sizes atomically. It then reruns the check and prints the
final diagnostics. Payload files, page tables, lifecycle records and WAL are never rewritten or
deleted. Other diagnostics may remain after eligible stat records have been repaired, so a run can
print both `repaired:` lines and diagnostics and still exit `1`.

Do not use `--fix` while the database is open or changing. It does not perform engine recovery and
does not repair corruption or undecidable reachability.

## Diagnostic categories

| Category | Meaning |
| --- | --- |
| `missing_metadata` | The metadata store is absent or has not been initialized |
| `unreadable_metadata` | The metadata store cannot be read or opened |
| `metadata_busy` | Another process holds a conflicting metadata lock |
| `unsupported_metadata_format` | The metadata file format is not supported by this build |
| `bad_version` | The stored metadata organization version is absent or unsupported |
| `bad_options` | Persisted global options are missing or invalid |
| `bad_bucket_catalog` | A bucket catalog entry is malformed or inconsistent |
| `bad_payload_file` | A data/blob file is unreadable or internally inconsistent |
| `bad_wal` | WAL segments have a gap or malformed record framing/checksum |
| `bad_interval` | An interval disagrees with the stat record or payload file it names |
| `bad_stat_accounting` | Stat counts, sizes or inactive masks disagree with relocations |
| `bad_lifecycle_state` | Lifecycle records or payload-file ownership contradict one another |
| `bad_page_table` | A page-table mapping points outside the bucket's published intervals |
| `bad_page_chain` | A reachable page reference cannot be resolved consistently |
| `junk_leak` | An unreachable retained relocation is missing its inactive mark |
| `leak_undecidable` | The checker cannot prove the fixed-root closure needed for leak classification or repair |

The category names are stable machine-readable output. Details and the available bucket, file and
address fields depend on the detected problem.

## Building

From the Mace repository root:

```bash
cargo build -p mace-kv-cli --release
```

The binary is `target/release/mace-kv` (or `target/debug/mace-kv` for a debug build).
