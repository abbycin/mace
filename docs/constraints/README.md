# Constraint Registry

This directory separates stable design invariants from the live constraint ledger.

- `../design.md`
  - stable system invariants that the design still intends to keep
- `registry.yaml`
  - live ledger for constraints that may be partial, suspect, evolving, or dependency-driven
- `retired.md`
  - constraints intentionally removed from the live ledger, with reasons

## Why this exists

Not every constraint has one owner function.
Many Mace constraints are cross-path protocols or system-level effects:

- write path + checkpoint + read path
- manifest publish + recovery + GC
- Mace logic + `btree-store` contract

For these constraints, a single `owner code` field is usually misleading.
This registry tracks the more realistic things:

- what the constraint says
- what changes should trigger re-review
- which code paths currently witness the constraint
- what symptoms show up when it breaks
- what verification exists today

## Entry model

Each entry should have:

- `id`
- `title`
- `kind`
- `status`
- `statement`
- `watch_points`
- `witnesses`
- `failure_symptoms`
- `verifiers`

Recommended fields:

- `confidence`
- `invalidation_triggers`
- `next_actions`
- `notes`

### `kind`

- `local`
  - closed inside one module or data structure
- `protocol`
  - spans a bounded sequence of steps or lifecycle phases
- `emergent`
  - only holds because several paths together preserve it
- `dependency`
  - depends on upstream or external contract semantics

### `status`

- `draft`
  - observed or inferred, but not yet strong enough to rely on
- `active`
  - currently relied on, with at least one meaningful verifier
- `suspect`
  - likely real, but must be re-reviewed because evidence is weak, stale, or a watch point changed
- `retired`
  - no longer intended to hold; move the full note to `retired.md`

### `confidence`

- `low`
  - symptom or witness exists, but proof is weak
- `medium`
  - partial verifier coverage exists
- `high`
  - direct verifier coverage plus stable source evidence

## Review rules

When a change touches an entry's `watch_points`, do one of these in the same patch:

1. rerun or update the listed verifiers and keep the entry as-is
2. downgrade the entry to `suspect`
3. move the entry to `retired.md` if the constraint is no longer intended

Do not leave a touched constraint marked `active` if its current evidence is no longer trustworthy.

## Practical rules

- keep `active` entries scarce
  - prefer only correctness, crash-safety, and externally visible semantic constraints
- if a constraint has no current verifier, prefer `draft` or `suspect`
- if a constraint is only a local implementation detail, keep it near code or tests instead of the global registry
- if a constraint becomes obsolete, retire it explicitly instead of silently deleting it

## Suggested workflow

1. Add a new entry as `draft` with statement, watch points, witnesses, and symptoms
2. Add or identify a verifier
3. Promote to `active` only after the verifier is real
4. Downgrade to `suspect` whenever a watch point or dependency changes
5. Move to `retired.md` once the constraint is intentionally removed or disproved
