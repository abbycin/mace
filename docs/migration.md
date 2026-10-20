<!--
Maintained as the index of on-disk format migrations; each hop has its own directory under
docs/migration/ and a README.md there holds that hop's runbook, expected output, diagnostics and
verified evidence.

To add a migration: create docs/migration/<from>-<to>/README.md from an existing hop, append one row
to "Migrations", and keep the shared sections (What is versioned, Tools, Procedure, Verifying, Using
the migrated database, Rolling back, Troubleshooting, Notes) free of version-specific steps and
messages. A hop that needs more than the shared procedure says so in its own README.
-->

# Migrating a database

An on-disk format change ships with a migration path instead of being applied silently: a database
written by an older build is refused (`Mace::new` returns `OpCode::Corruption`, `mace-kv check` reports
`unsupported_metadata_format`), and it is rebuilt once with an offline tool. Migrations run on a copy of
the database directory, so the original stays usable until the migrated copy passes its checks.

## Migrations

| from | to | metadata store format | rebuild with | guide |
| --- | --- | --- | --- | --- |
| 0.1.2 | 0.2.0 | 1 -> 2 | `btree-store migrate --to 2` (btree-store 2.x, which reads format 1) | [0.1.2 -> 0.2.0](./migration/0.1.2-0.2.0/README.md) |

Apply the hops in release order, one at a time: a database several formats behind is migrated to the
next format, then to the one after it, and so on. Each hop runs with the tool build that reads that
hop's source format - `btree-store` reports the versions it accepts
(`... uses format version <n>; this build reads <to> and migrates <list>`), and the hop's guide names
the build it was verified with. `mace-kv check` names the format a database is in when its build cannot
read it (`... is format <n> ...`), which selects the next hop.

## What is versioned

- `<db>/log/manifest` (or `<log_root>/manifest` when `Options::log_root` is set) is the only file whose
  metadata format a migration rewrites; there is one metadata store per database directory.
  (Data/blob files carry their own payload file version in the footer, which a metadata migration does
  not change.)
- Payload files (`<db>/data/*`) and WAL files (`<db>/log/wal_*`, `<db>/log/group_wal_*`) are not
  rewritten by a migration unless its own guide says so.
- A database that was not closed cleanly is migrated the same way, and the committed records still in
  its WAL are replayed on the first open by the newer build, exactly as that build's recovery would
  replay them without a migration.

## Tools

- `mace-kv`, the maintenance CLI (added in 0.2.0; 0.1.2 shipped no CLI): `cargo install mace-kv`, the
  binary is `mace-kv`. `mace-kv check` exits `0` on a clean check, `1` when it reports a finding
  (including a format this build cannot read), and `2` on a usage error.
- the `btree-store` CLI: `cargo install btree-store`. `btree-store --help` must list `migrate`, and
  `btree-store migrate --help` prints
  `Usage: btree-store migrate --output <destination> --to <version> <source>`.
- no process may have the database open. The migration opens the source file read-only under a shared
  lock and retries for about a second, so a database a running engine still holds (an engine takes the
  exclusive lock) is refused with `lock-busy` instead of being migrated while it moves; stopping the
  writer first is still the clean way to do it.
- room for one copy of the database directory (the recommended procedure migrates a copy).

## Procedure

`DB` is the database directory; `FROM` and `TO` are the metadata store format versions of the hop, and
the hop's guide carries the exact values plus its own rebuild command. The block assumes the default
`<db>/log`; when `Options::log_root` points elsewhere, use that directory in place of `log`:

```bash
DB=/srv/myapp/data/mydb
FROM=1
TO=2

# 0. stop every process that has the database open
# 1. work on a copy; the original stays untouched as the rollback artifact
cp -a "$DB" "$DB.migrate"

# 2. rebuild the metadata store (the source file is never modified)
btree-store migrate --output "$DB.migrate/log/manifest.v$TO" --to "$TO" "$DB.migrate/log/manifest"

# 3. keep the old file for rollback, then swap the rebuilt one in with one atomic rename
cp -a "$DB.migrate/log/manifest" "$DB.migrate/log/manifest.v$FROM"
mv "$DB.migrate/log/manifest.v$TO" "$DB.migrate/log/manifest"

# 4. check the copy offline: exit 0 and "<dir>: ok"
mace-kv check "$DB.migrate"

# 5. replace the live directory; the old one becomes the directory-level rollback
mv "$DB" "$DB.v$FROM"
mv "$DB.migrate" "$DB"
```

Rules the tool follows, whichever hop you are running:

- `--output` must differ from the source and must sit in an existing directory.
- an existing destination is replaced, and the replacement is a single `rename` of a private staging
  file (`<destination>.migrate-v<to>.<pid>.<n>.tmp`), so an interrupted run never leaves a partial
  destination.
- the tool reopens the file it published with a fresh instance and compares every key and value against
  the source; it prints `verified=true` only after that comparison passes. The source file is never
  modified.

## Verifying

`mace-kv check` opens the directory without constructing the engine: it is read-only and never creates,
repairs or deletes a file.

```console
$ mace-kv check /srv/myapp/data/mydb
/srv/myapp/data/mydb: ok
$ echo $?
0
```

Exit `0` means no finding; exit `1` means at least one diagnostic line above the summary. A database
whose metadata store a live engine still holds is not damaged - the check reports `metadata_busy` and
exits `1`, so stop the engine first to get a meaningful result.

Run it before a migration too: its `unsupported_metadata_format` diagnostic names the rebuild command
for that format, and the guide for the hop named by `... is format <n> ...` carries the exact text.

## Using the migrated database

The package name (`mace-kv`) and the library name (`mace`) are unchanged across versions, so upgrading
is a version bump in `Cargo.toml`:

```toml
[dependencies]
mace-kv = "0.2.0" # the release you are upgrading to
```

```rust
use mace::{Mace, OpCode, Options};

fn main() -> Result<(), OpCode> {
    let opt = Options::new("/srv/myapp/data/mydb");
    let mace = Mace::new(opt.validate()?)?; // a database that still needs a migration fails here

    let db = mace.open_bucket("alpha")?; // a bucket written by an older version
    {
        let view = db.view()?;
        let value = view.get("alpha:k0000")?;
        println!("{} bytes", value.slice().len());
    }

    let tx = db.begin()?; // the newer build writes as usual
    tx.put("alpha:new", vec![b'n'; 4096])?;
    tx.commit()?;
    db.checkpoint_and_wait();
    Ok(())
}
```

## Rolling back

- Before the newer build has written anything, the pre-migration directory kept as `$DB.v$FROM` by
  step 5 is the rollback: `mv "$DB" "$DB.failed" && mv "$DB.v$FROM" "$DB"`, then open it with the older
  build as it was.
- The older-format metadata store saved by step 3 is enough for a same-directory rollback too: move
  `log/manifest.v$FROM` back over `log/manifest` before the newer build has written. Both the engine
  and `mace-kv check` tolerate that extra file staying in the directory.
- After the newer build has written, do not roll back: the metadata store is the newer format and
  payload files may have been rewritten.

## Troubleshooting

Messages follow `error: <phase>: <class>: <detail>`; the text below is captured on Linux. `<to>` is the
`btree-store` build's own format version - the only value `--to` accepts. The OS-level description
after the last colon comes from the platform (`(os error 2)` is Linux's `ENOENT`), so other systems may
word it differently. Phases, classes and exit codes are stable across platforms.

| message | meaning | action |
| --- | --- | --- |
| `Mace::new` returns `OpCode::Corruption`, or `mace-kv check` reports `unsupported_metadata_format` | the database still needs its migration | run the hop that matches its format |
| `error: source: source-already-current: <path> already uses format version <to>; there is nothing to migrate` (exit 1) | already migrated | nothing to do |
| `error: arguments: arg: --to <n> is not this build's format version (<to>)` (exit 2) | wrong `--to` | use the value from "Migrations" |
| `error: arguments: arg: <path> is the source itself; the destination must be another file` (exit 2) | `--output` equals the source | write to the `.v<TO>` file, then rename it over |
| `error: target: io: resolve parent of <path>: No such file or directory (os error 2)` (exit 1; shown for Linux) | the destination's parent directory does not exist | create it first |
| `error: source: lock-busy: <path> is locked by another process` (exit 1) | a writer still holds the database | stop it and rerun |
| `error: source: source-version-unsupported: <path> uses format version <n>; this build reads <to> and migrates <list>` (exit 1) | this `btree-store` does not read that source format | install the build listed for that hop |
| `btree-store check`: `error: check: source-version-unsupported: unsupported format version <n>` (exit 1) | you checked a file whose format this build does not read | check the file after migrating it |
| `command not found: btree-store` | the CLI is not installed | `cargo install btree-store` |

## Notes

- Do this once per database directory: every database you own is a directory of its own and needs its
  own migration.
- Run the block once. `cp -a` into an existing `$DB.migrate` copies the database *inside* it, and
  re-running step 3 would overwrite the saved old manifest with the migrated one: if a step failed,
  delete `$DB.migrate` and start again (the original `$DB` is untouched).
- The rebuilt file inherits the source's permission bits narrowed to owner read/write
  (`source_mode & 0o600`, with owner read kept) and the migration run's ownership. Adjust them if
  another user reads the directory.
- A killed migration can leave a `<destination>.migrate-v<to>.<pid>.<n>.tmp` staging file behind. It is
  not a database file; delete it.
