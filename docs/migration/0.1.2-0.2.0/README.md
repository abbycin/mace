# 0.1.2 -> 0.2.0

[All migrations](../../migration.md) · metadata store format 1 -> 2 · release notes in
[CHANGELOG](../../../CHANGELOG.md)

The metadata store (`<db>/log/manifest`) moves from format 1 to format 2. Payload files (`data/*`) and
WAL files are not rewritten; a database left behind by an unclean abort is migrated the same way, and
its committed WAL records replay on the first 0.2.0 open.

Use the `btree-store` 2.x CLI (2.0 or later: it reads format 1 and writes format 2) and the `mace-kv`
0.2.0 CLI for the checks. `--to` must be `2`, the build's own format version; any other value is
refused as a usage error. Stop every process that has the database open first; the migration runs on a
copy, so the original stays usable until the copy passes `mace-kv check`.

## Runbook

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

The destination handling (atomic staging rename, `verified` re-read), the lock behaviour and the
rollback options are shared across hops and live in [the index](../../migration.md).

## Expected output

`btree-store migrate` reports the generations it read and wrote, what it copied, and `verified=true`
once the published file has been compared key for key with the source:

```text
source=.../log/manifest
source_version=1
source_generation=43
source_ignored_invalid_candidate=false
target=.../log/manifest.v2
target_version=2
target_generation=28
buckets=15
records=42
logical_bytes=7403
batches=11
physical_bytes=98304
verified=true
```

## Diagnostics

A database still in format 1 refuses to open on 0.2.0, and the check already names this hop:

```console
$ mace-kv check /srv/myapp/data/mydb
unsupported_metadata_format: bucket=- file=- addr=-: metadata store /srv/myapp/data/mydb/log/manifest is format 1, which this build cannot read; rebuild it offline with `btree-store migrate <source> --output <destination> --to 2`
/srv/myapp/data/mydb: 1 diagnostic(s) [unsupported_metadata_format=1]
$ echo $?
1
```

`btree-store check` on the format-1 file reports
`error: check: source-version-unsupported: unsupported format version 1` (exit 1); check the file after
migrating instead.

## Verified

With mace 0.1.2 + btree-store 1.1.1 as the source and mace 0.2.0 + btree-store 2.0.0 as the target:

- a 3.0 MB database with 2 buckets and 450 live keys (small inline values, 8 KiB blobs, overwrites and
  deletes) migrated with `verified=true`, checked `ok` afterwards, and read back key for key - same
  lengths and hashes, deleted keys still absent;
- writes through 0.2.0 (put, update, delete, a new bucket) survived a reopen on the migrated directory,
  and `mace-kv check` stayed `ok`;
- a second database left behind by an unclean abort (500 committed records in the WAL, no checkpoint)
  migrated the same way, checked `ok`, and all 500 committed records were visible after the first
  0.2.0 open.
