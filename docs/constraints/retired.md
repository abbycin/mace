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

## `protocol.wal_format_upgrade_requires_wal_independent_exit`

- retired_on: 2026-09-01
- previous_status: active
- reason: the identifier described a WAL-independent graceful-exit invariant as a data-format upgrade, but the current data/blob format is V1-only and has no format-upgrade protocol
- replacement: `protocol.graceful_exit_wal_independent_visibility` in `registry.yaml`
- evidence: `docs/design.md` section 15.1 defines V1-only writers/readers and an explicit future migration boundary; `tests/exit_flush.rs` verifies the retained graceful-exit WAL-independent visibility invariant

## `protocol.persist_stat_decode_bounds` (clause: a failed validation returns `OpCode::Corruption` instead of terminating)

- retired_on: 2026-09-19
- previous_status: suspect (the constraint itself stays `active` in `registry.yaml` with the clause removed)
- reason: how a failed stat-record validation surfaces is owned by the fatal-failure contract, not by this decoding constraint. The production callers (`decode_inner_only`, `decode_mask_only`, reached from startup stat loading and lazy mask loading) go through `must_ok!`, and an invariant failure now prints its report and aborts the process instead of unwinding or returning a typed error, so the clause contradicted the code it was supposed to describe.
- replacement: `protocol.invariant_failure_is_fail_stop` in `registry.yaml` owns the termination shape; the bounds themselves remain owned by `protocol.persist_stat_decode_bounds`, and the offline checker reads the same records through the fallible variants (`try_decode_inner_only`, `try_decode_mask_only`) to report without aborting
- evidence: `src/error/mod.rs` replaces `panic!` with `abort_after_report` (report, flush, `std::process::abort`); `src/meta/entry.rs` keeps the panicking names as `must_ok!` wrappers over the validated fallible variants; `src/store/checker.rs` unit tests (`an_undecodable_stat_record_is_reported_once`, `a_declared_sequence_count_beyond_the_record_is_a_corruption`, `an_out_of_range_inactive_sequence_is_a_corruption`) cover the bounds, and `src/error/mod.rs::tests::a_fatal_failure_aborts_the_process` covers the termination

## 2026-09-20: entries removed by the five-concern restructuring

- retired_on: 2026-09-20
- previous_status: active
- reason: the ledger was restructured around four abstract concerns (see `README.md`). An
  entry now has to name the concern it protects; these entries were either folded into a
  survivor that states the same rule for several mechanisms, or moved to the place that
  actually documents them. No invariant was dropped: every folded rule is present in the
  survivor's statement, and every moved rule now lives in a code comment, the module docs
  or `docs/design.md`.
- replacement: the surviving entry named in each line, or the named file
- evidence: `docs/constraints/registry.yaml` (31 entries, each with `protects`) and the code
  comments added at the move destinations

### folded into a surviving entry
- `protocol.frontier_monotonic_multi_group_closure` -> `protocol.recovery_gate_uses_bucket_frontier`: the frontier rule (monotonic, covers every group folded into the closure, deferred pages do not advance it) is stated there together with the A10/B20 counterexample and the single-transaction publish it depends on; the two entries shared a reproducer, so keeping both stated the same rule twice.
- `protocol.snapshot_registration_cut_covers_every_live_reader` -> `protocol.positive_proof_rechecks_abort_state`: the cut that defines what a live reader may see is part of the same publication window.
- `protocol.snapshot_terminal_publication_order` -> `protocol.positive_proof_rechecks_abort_state`: commit must publish its terminal fact inside the window the cut can observe.
- `protocol.collector_start_waits_for_recovery_waterline` -> `protocol.positive_proof_rechecks_abort_state`: the watermark may only be published or raised after recovery's oracle.
- `protocol.safe_prefix_covers_every_live_snapshot` -> `protocol.positive_proof_rechecks_abort_state`: publishing before pruning is the same monotonicity rule.
- `protocol.snapshot_pin_covers_iterator_lifetime` -> `protocol.positive_proof_rechecks_abort_state`: the pin keeps that watermark alive for derived readers.
- `protocol.snapshot_first_writer_wins` -> `protocol.positive_proof_rechecks_abort_state`: conflict detection consumes the same verdict, so at most one writer commits.
- `protocol.history_region_key_locality` -> `protocol.snapshot_candidate_traversal_complete`: the same resolver, and the region a key's history may occupy.
- `protocol.merge_operand_fold_is_snapshot_complete` -> `protocol.snapshot_candidate_traversal_complete`: the one shared fallible resolver that folds the visible operand chain.
- `protocol.merge_contract_violation` -> `protocol.snapshot_candidate_traversal_complete`: a bad fold is a public error, never a wrong or missing value.
- `protocol.merge_val_tag_round_trip` -> `protocol.snapshot_candidate_traversal_complete`: the persisted kind bit that keeps a merge envelope from decoding as a tombstone.
- `protocol.merge_operator_runtime_contract` -> `protocol.snapshot_candidate_traversal_complete`: the operator never reaches manifest metadata, WAL or page bytes; its present/unregister lifecycle is a design.md and API-doc fact.
- `protocol.recovery_defers_merge_structural_maintenance` -> `protocol.merge_compact_preserves_snapshot_and_frontier`: the same rule, deferred while recovering.
- `protocol.inline_size_updatable_rebuilds_materialize_blobs` -> `protocol.merge_compact_preserves_snapshot_and_frontier`: a rebuild must not leave a reader with an unresolvable row.
- `protocol.checkpoint_epoch_cut_atomicity` -> `emergent.checkpoint_live_set_only`: the single cut that makes the live set well-defined.
- `protocol.flush_junk_atomic_with_owner_durability` -> `emergent.reachable_junk_lifecycle`: the owner's hard constraint 4, the retirement half of the same boundary.
- `protocol.map_and_frontier_publish_atomicity` -> `protocol.recovery_gate_uses_bucket_frontier`: the single manifest transaction that makes map and frontier agree.
- `protocol.abort_clean_wal_chain_bounded` -> `protocol.abort_clean_visibility_and_retirement`: corruption must retain the task, the abort fact and the WAL pin.
- `protocol.runtime_checkpoint_enable_waits_for_recovery_abort_clean` -> `protocol.abort_clean_visibility_and_retirement`: recycle must not run before the drain that still needs that WAL.
- `protocol.global_options_presence_and_compatibility_gate` -> `protocol.concurrent_write_epoch_migration`: the options presence gate and the writeback-after-completion order.

### moved out of the ledger
- `dependency.manifest_conditional_update_semantics`: docs/design.md, dependency contracts: the pinned btree-store update-if-exist semantics. An upstream contract with no reader-visible outcome of its own; the mace-side conditional stat update it backs is described in the stat-retirement entry.
- `dependency.manifest_multi_bucket_atomicity`: docs/design.md, dependency contracts: the pinned btree-store one-generation exec_multi publication. The mace-side rule it carries (publish map and frontier as one transaction) is folded into protocol.recovery_gate_uses_bucket_frontier.
- `dependency.metadata_page_damage_aborts_by_design`: docs/design.md and CHANGELOG: the upstream btree-store BTREE_FATAL abort behaviour that the offline checker and the maintenance CLI inherit.
- `protocol.foreground_admission_before_tree_update`: code comment in src/index/txn.rs and docs/design.md 5: a foreground admission budget before entering tree.update. Breaking it amplifies dirty memory; no reader observes a different value and no disk bytes are orphaned.
- `protocol.shutdown_admission_exclusion`: code comment in src/store/store.rs: the quit path is reachable only from Inner::drop, so no public API admits a transaction during shutdown. A topology fact, not a reader-visible outcome; the risk it guards (a commit lost to a racing shutdown) is covered by the data-loss entries.
- `relaxed_commit_bypasses_sync_coordination`: docs/design.md 8.1 and a code comment in src/cc/log.rs: with sync_on_write=false a commit flushes and never registers a generation or opens a shared logger. A coordination-elision contract, not a visibility outcome.
- `protocol.maintenance_checker_contract`: tooling contract, not one of the ledger's concerns: the offline checker is read-only, reports each damage once, and `mace-kv check` exits 0/1/2. Documented in the module docs of `src/store/checker.rs` and `src/bin/mace-kv.rs` and in CHANGELOG.
- `runtime.instance_local_slot_isolation_and_reclamation`: thread-local storage discipline with no reader-visible consequence on its own: module doc in `src/utils/instance_local.rs`.
- `protocol.dirty_ceiling_pool_plus_bounded_drain_burst`: a memory budget, not an invariant: exceeding it throttles the foreground and discards nothing. Module doc in `src/map/flow.rs`.
- `test.gc_round_completion_signal`: test-harness contract: a completed GC round must be observable. Lives with the harness in `src/testing.rs` and `tests/gc.rs`.
- `test.failpoint_hit_observable`: test-harness contract: failpoint consultations must be countable. Module doc in `src/utils/failpoint.rs`.
- `test.collector_cycle_completion_signal`: test-harness contract: a completed collector cycle must be observable. Lives with the harness in `src/testing.rs` and `tests/cc.rs`.
- `test.timeout_zero_disables_background_trigger`: extra_check harness contract for `gc_timeout = 0`. Lives with the harness in `src/testing.rs`.
