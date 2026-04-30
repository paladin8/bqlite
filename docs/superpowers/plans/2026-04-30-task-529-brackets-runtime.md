# TASK-529 — BRACKETS runtime emission in SequenceMatch operator

**Goal**: Implement per-bracket row emission so `MATCH … BRACKETS [..]` (and the
`RETENTION(…)` sugar that desugars to it) returns one row per
`(entity, binding track, bracket)` with real `bracket` / `bracket_end` values
and a `step_reached` that reflects which step completed inside each bracket.

This completes the RETENTION end-to-end path: the four un-ignored RETENTION
integration tests can then assert specific `retention_rate` values per bracket
instead of `row_count() > 0`.

References: `docs/design/sequence-matching.md` §5–§12, `docs/design/query-language.md` §4.12 / §6.3, `docs/design/planner-pipeline.md` §10.2, TASKS.md TASK-529.

## Decisions

1. **`bracket_end` semantics** = **relative duration in nanoseconds** (`bracket_durations[bracket_idx]`), not an absolute epoch. Anchor-independent so all entities in a cohort share identical `bracket_end` values, which is what the desugared `STATS … GROUP BY bracket` and explicit-form retention queries need. Update query-language.md §4.12 to make this explicit (audit finding R3).
2. **Bracket window semantics** = `[0, dur_0]` for bracket 0 and `(dur_{i-1}, dur_i]` for `i > 0` — left-open / right-closed for non-zero brackets, exactly as query-language.md §4.12 illustrates with `[0-1d], (1d-7d], …`. Step events at delta `D` fall in the smallest bracket `b` where `D ≤ dur_b`. (TASKS.md's "[prev, dur)" phrasing is reconciled to match the user-facing spec.)
3. **`brackets` lives on `CompiledNfa`**, not separately on `SequenceMatchPhysical`. The matcher already consumes `CompiledNfa`; carrying brackets there keeps the operator constructor surface unchanged and matches how `global_window`, `session_window`, and `emit_all` already live.
4. **Per-step timestamps for partials are not added** in this task. With a 2-step pattern (the only shape RETENTION uses) the data we already have — `(anchor_ts, final_ts)` for completions and `(anchor_ts, step_reached)` for partials — is sufficient. For longer patterns the partial path conservatively reports `step_reached=1` in bracket 0 and `0` elsewhere; tightening that requires per-step timestamps and is out of scope.
5. **Cumulative emission** is computed at `output.rs` time as a backward partial-sum: `cumulative_step_reached[k] = max(exclusive_step_reached[0..=k])`. The matcher core stays bracket-unaware.

## Per-bracket `step_reached` rule

For each `(entity, binding track)` and each bracket index `b`:

```
exclusive_step_reached[b] =
    completion present (anchor_ts, final_ts, num_steps):
        let delta_final = final_ts - anchor_ts
        let final_in_b  = delta_final falls in bracket b's window (right-closed)
        let anchor_in_b = (b == 0)         // anchor delta = 0 ∈ [0, dur_0]
        if final_in_b: num_steps
        else if anchor_in_b: 1
        else: 0
    partial only (step_reached_partial >= 1):
        if b == 0: 1
        else: 0

cumulative_step_reached[b] = max(exclusive_step_reached[0..=b])
```

A completion whose `delta_final > durations[N-1]` is treated as "the entity converted past the end of the bracket window" — `final_in_b` is false for every bracket, so the rule degenerates to the partial-only path: bracket 0 gets `1`, later brackets `0`. This matches the cohort semantics in query-language.md §4.12 (event past the last bracket is not retained in any bracket).

For 2-step retention this gives the standard cohort behaviour:
- exclusive: bracket where activity fell → `step_reached=2`; bracket 0 always `≥1`; later brackets `0` if the activity didn't fall there.
- cumulative: `step_reached=2` from the bracket where activity fell onward.

## Output-rule summary

Let `N` = number of brackets, `B = brackets present on CompiledNfa`.

| Mode | Per `(entity, track)` rows |
|---|---|
| no `B`                        | unchanged: 1 completion row (or 0/1 partial under EMIT ALL) |
| `B`, no EMIT ALL, completion  | 1 row, for the bracket where the completion's `delta_final` fell. With `cumulative`: rows for that bracket and every later bracket (one row each). Without completion (or with completion `delta_final > durations[N-1]`): 0 rows. |
| `B` + EMIT ALL                | exactly `N` rows per `(entity, track)`. `step_reached` per bracket per the rule above. |

## Checkpoints

### CP1 — Thread `BracketSpec` and tighten nullability

Touches shared types (`CompiledNfa`) — must merge first.

Files:
- `crates/bqlite-planner/src/compile.rs`
  - Add `pub brackets: Option<BracketSpec>` to `CompiledNfa`. Populate from `pattern.brackets` in `compile_pattern`.
  - Update every `CompiledNfa { … }` literal in tests / fixtures to include `brackets: None`.
  - Re-export `BracketSpec` from `bqlite_ast::pattern` if not already; `CompiledNfa` will surface the AST type.
- `crates/bqlite-planner/src/logical.rs`
  - Tighten the two `nullable: true` flags for `bracket` / `bracket_end` to `nullable: false` (reverses the 670b2d5 mitigation now that emission produces real values).
- `crates/bqlite-operators/src/matcher/{nfa.rs, step_counter.rs, mod.rs}` test fixtures: add `brackets: None` to local `CompiledNfa` literals.

Validation:
- `scripts/local-ci.sh` green.
- All existing tests still pass with `brackets: None` carrying through.
- The `bracket_columns_emit_null_arrays_under_nullable_contract` regression test in `output.rs` will be removed in CP2 (its premise — that the null path is acceptable — no longer holds once emission is real). For CP1 it stays green by virtue of `brackets: None` falling through the existing null path.

Subagent code review of staged changes before commit. Merge to `main` ff-only.

### CP2 — Per-bracket emission in `output.rs`

Files:
- `crates/bqlite-operators/src/matcher/output.rs`
  - Extend `build_output_batch` to take an `Option<&BracketSpec>` (or accept the entire `&CompiledNfa`).
  - When `brackets.is_some()`:
    - For EMIT ALL: emit `N` rows per `(completion or partial)` with `bracket = b`, `bracket_end = durations[b]`, `step_reached` per the rule above. Apply cumulative pass at the end.
    - Without EMIT ALL: emit only completed rows in their bracket(s) (single bracket for exclusive; that-and-later brackets for cumulative). Partials are skipped (current behaviour).
  - Existing branches (no brackets) unchanged.
  - Remove the `bracket_columns_emit_null_arrays_under_nullable_contract` test (replaced by per-bracket emission tests).
  - Add unit tests:
    - completion in bracket 0, exclusive
    - completion in last bracket, exclusive
    - completion outside all brackets (delta > max), exclusive — emits `step_reached=1` for bracket 0 only under EMIT ALL
    - same with cumulative (monotone nondecreasing across brackets)
    - partial-only (step 1 reached): under EMIT ALL bracket 0 emits 1, others 0; under cumulative they all stay at 1 from bracket 0 onward
    - bindings combined with brackets: per-bracket rows correctly carry the binding values
- `crates/bqlite-operators/src/matcher/mod.rs`
  - Pass the `BracketSpec` (from `compiled_nfa.brackets`) to `build_output_batch` in both `finish_entity` and `finish_entity_into`.
  - `build_match_output_schema` signature changes to `(emit_all: bool, _num_steps: u8, brackets: Option<&BracketSpec>)`. When `brackets.is_some()`, append non-null `bracket` and `bracket_end` Int columns. Variable-binding columns and step-property columns remain absent from the fused intermediate schema (they aren't required by the current `update_batch` aggregator path; widening that surface is out of scope and the BRACKETS × bindings test in CP3 exercises only the non-fused `finish_entity` path).
  - Update the two call sites at `mod.rs:148-150` and `mod.rs:208` (the `from_compiled_nfa` constructor's match_output_schema isn't built; only the descriptor path constructs it — so only one site needs the new arg, but the helper's signature must change).
  - **Demand pruning sanity**: when brackets is set, assert `output_schema.column("bracket").is_some()` at operator construction (debug_assert) so a future demand-pass regression that strips the column is caught fast.

Validation:
- All matcher unit tests green.
- The four formerly un-ignored RETENTION integration tests (still asserting only `row_count() > 0`) keep passing — CP3 strengthens those assertions.
- `scripts/local-ci.sh` green.

Subagent code review. Merge ff-only.

### CP3 — EXPLAIN, doc reconciliation, and tightened tests

Files:
- `crates/bqlite-planner/src/explain.rs`
  - Add `brackets: Option<Vec<i64>>` and `cumulative: bool` to `ExplainNode::SequenceMatch`.
  - Format them in the `write_node` pretty-printer (`brackets : [1d, 7d, 14d, 30d]`, `cumulative : true|false` lines, only when present).
  - Update existing EXPLAIN tests; add one new test that EXPLAIN of a `RETENTION(…)` query renders the bracket list and cumulative flag.
- `docs/design/query-language.md` §4.12
  - Clarify that `bracket_end` is the **relative bracket-upper-bound duration in nanoseconds** (anchor-independent), not an absolute epoch. Note the bracket-window convention (`[0, dur_0]` then `(dur_{i-1}, dur_i]`).
- `docs/design/planner-pipeline.md` §10.2
  - Reflect the new `brackets` / `cumulative` fields on `ExplainNode::SequenceMatch`. (The doc already lists richer fields than the code; this addition lands the bracket fields and is otherwise minimal.)
- `tests/tests/wave4_advanced_analytics_event_select.rs`
  - Update the stale `#[ignore]` preamble at `wave4_advanced_analytics_event_select.rs:487-503` — the four tests are no longer ignored and the panic the comment describes no longer reproduces.
  - `retention_standard_brackets_produces_expected_rates`: now assert exact rates per bracket. With three users converting at days 2 / 9 / 20 and brackets `[1d, 7d, 14d, 30d]`:
    - Exclusive `step_reached` per user is `2` only in the bracket where the activity falls (right-closed windows):
      - u_early (delta=2d) → bracket 1 `(1d, 7d]`
      - u_mid (delta=9d) → bracket 2 `(7d, 14d]`
      - u_late (delta=20d) → bracket 3 `(14d, 30d]`
    - Aggregated `retention_rate = AVG(step_reached >= 2)`:
      - bracket 0: 0/3 = 0.0
      - bracket 1: 1/3 ≈ 0.3333
      - bracket 2: 1/3 ≈ 0.3333
      - bracket 3: 1/3 ≈ 0.3333
  - `retention_cumulative_brackets_are_monotone`: assert exact monotone-non-decreasing values:
      - bracket 0: 0.0; bracket 1: 1/3; bracket 2: 2/3; bracket 3: 3/3 = 1.0.
- `tests/tests/wave4_acceptance.rs::retention_invariance_under_compaction` — already runs; tighten to also compare a specific bracket's rate pre/post-compaction.
- New proptest in `crates/bqlite-operators/src/matcher/output.rs` (under `#[cfg(test)]`):
  - For arbitrary completions/partials and arbitrary bracket lists: cumulative `step_reached` is monotone non-decreasing across `bracket = 0, 1, …, N-1` per `(entity, track)`.
- New test in `crates/bqlite-operators/src/matcher/mod.rs` for BRACKETS × variable-binding composition: a 2-step pattern with `$plan = signup.plan` binding and brackets, two distinct `plan` values per entity, asserts per-track per-bracket row count and step_reached values.

Validation:
- `scripts/local-ci.sh` green.
- Property test runs and passes (default 256 cases).
- New tests pass.

Subagent code review. Merge ff-only. Then move lock file to `tasks/completed/TASK-529.done` with `completed_at`, commit and push.

## Risks & open questions

- **Partials with longer patterns**: per-step timestamps would let us bucket intermediate steps correctly. Out of scope here. The retention path is unaffected.
- **`physical.rs:1368` (`brackets: _` discard)**: the discard is intentional once `CompiledNfa.brackets` is populated by `compile_pattern`. The data flows logical `LogicalPlan::SequenceMatch.brackets` → AST `MatchPattern.brackets` → `compile_pattern` → `CompiledNfa.brackets`. No physical.rs change needed; the discard simply documents that the physical lowering reads brackets indirectly via the compiled NFA.

No `[NEEDS INPUT]` blockers identified. The TASKS.md description (a) literally says "add `brackets` to `SequenceMatchPhysical`"; this plan deviates by routing through `CompiledNfa` because that matches how `global_window` / `session_window` / `emit_all` already live and keeps the matcher's input contract uniform. The completion message will note this deviation.
