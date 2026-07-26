# Retired Constraints

Move entries here instead of silently deleting them from `registry.yaml`.

Use this template:

## `<constraint-id>`

- retired_on:
- previous_status:
- reason:
- replacement:
- evidence:

Notes:

- `reason` should say why the constraint stopped being meaningful
- `replacement` should point to the new constraint if the old one was split or renamed
- `evidence` should name the code change, test, or design change that justified retirement

## `protocol.group_resolved_prefix_does_not_cross_active_hole`

- retired_on: 2026-07-20
- previous_status: active
- reason: visibility no longer has a group-local resolved-prefix proof, so there is no terminal queue or prefix boundary that can cross an active hole
- replacement: exact `TxnFact` lookup in `src/cc/mod.rs`; active-hole visibility remains covered by the live safe and exact-outcome constraints
- evidence: `src/cc/group.rs` removes `ResolvedPrefix` and terminal queue state; `tests/si.rs` retains same-group active-hole and late-commit schedules without prefix forcing
