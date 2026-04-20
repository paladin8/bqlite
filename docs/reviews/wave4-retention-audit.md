# Wave 4 RETENTION Semantic Audit

**Auditor**: TASK-443
**Date**: 2026-04-20
**Sources reviewed**:
- Design spec: `docs/design/query-language.md` §4.12 (BRACKETS), §6.3 (RETENTION sugar), §19.3 (FUNNEL/RETENTION inside JOINs), §30.6 (BRACKETS × variable bindings)
- Design spec: `docs/design/planner-pipeline.md` §4.3 (desugaring), §4.4 (scan-range extension), §10 (EXPLAIN)
- Parser: `crates/bqlite-parser/src/pattern.rs` (BRACKETS clause), `crates/bqlite-parser/src/pipeline.rs` (RETENTION sugar)
- AST: `crates/bqlite-ast/src/pattern.rs` (BracketSpec), `crates/bqlite-ast/src/operator.rs` (Retention, PipelineStage)
- Planner — desugaring: `crates/bqlite-planner/src/opt/desugar_retention.rs` (TASK-426)
- Planner — logical lowering: `crates/bqlite-planner/src/logical.rs` (lower_match, bracket schema construction, scan extension)
- Planner — physical: `crates/bqlite-planner/src/physical.rs` (SequenceMatch lowering arm)
- Operator output: `crates/bqlite-operators/src/matcher/output.rs` (RecordBatch assembly)
- Integration tests: `tests/tests/wave4_advanced_analytics_event_select.rs` (TASK-439 CP2)

**Methodology**: Walk each design-doc promise for the RETENTION / BRACKETS feature; locate primary evidence in code and tests; classify each as ✅ Covered, ⚠️ Partial, or ❌ Missing. Follow-up items for partial / missing rows are filed at the end. Nothing is fixed here — all drift and missing coverage are rolled up into TASK-455.

---

## Promise-vs-Evidence Matrix

### §4.12 — BRACKETS Parsing and AST

| Promise | Evidence | Status |
|---------|----------|--------|
| `BRACKETS [d1, d2, …]` recognized as a match modifier, mutually exclusive with `WITHIN` | `parse_brackets` at `pattern.rs:481`; `parse_match_modifiers` at `pattern.rs:120` enforces mutual exclusion with `WITHIN`; `WITHIN after BRACKETS` guard at `pattern.rs:142–148` | ✅ |
| `BRACKETS CUMULATIVE [d1, d2, …]` sets `BracketSpec.cumulative = true` | `p.try_kw(Keyword::Cumulative).is_some()` at `pattern.rs:489`; `modifiers_brackets_cumulative` test | ✅ |
| Modifier order enforced: `WITHIN`/`BRACKETS` must precede `EMIT ALL` | `parse_match_modifiers` checks for `EMIT ALL` before `BRACKETS` at `pattern.rs:540–562`; out-of-order parse error tested | ✅ |
| Empty bracket list is a parse error | `pattern.rs:493–498`: `RBracket` immediately after `[` triggers error `"BRACKETS requires at least one duration"` | ✅ |
| Bracket durations stored in user-declared order (required for correct slice intervals) | `parse_brackets` pushes durations in left-to-right order into `Vec<i64>`; no sort or reorder | ✅ |
| No validation that bracket durations are strictly ascending | Neither the parser nor the planner checks that `durations[i] < durations[i+1]`. A query `BRACKETS [30d, 7d, 1d]` is accepted silently, producing inverted time slices at runtime. The design does not explicitly call out an ascending-order requirement, but the spec semantics ("distinct time slices with no overlap") depend on ascending order. | ❌ |
| `BRACKETS` and `WITHIN` mutual exclusion enforced | `WITHIN_after_BRACKETS` guard in parser + planner rejects both present (planner: `MatchWindowSpec` is `None` for bracket patterns in `logical.rs`; parser guard at `pattern.rs:142`) | ✅ |

---

### §6.3 — RETENTION Sugar: Desugaring Pass (TASK-426)

| Promise | Evidence | Status |
|---------|----------|--------|
| `RETENTION(entry: signup, activity: purchase, brackets: [1d, 7d, 14d, 30d])` desugars to `MATCH FIRST SEQUENCE(signup THEN purchase) BRACKETS [1d, 7d, 14d, 30d] EMIT ALL` | `desugar_retention.rs:108–118`: `match_stage` has `mode=MatchMode::First`, `emit_all=true`, `window=None`, `brackets=Some(retention.brackets)`; `basic_retention_produces_match_and_stats` unit test | ✅ |
| Desugared STATS is `retention_rate = AVG(CAST(step_reached >= 2 AS INT)) GROUP BY bracket` | `desugar_retention.rs:136–170`: aggregate function "avg", alias "retention_rate", arg is `CAST(Compare(step_reached >= 2) AS INT)`, `group_by = [Expr::Column("bracket")]`; `step_reached_comparison_uses_value_two` and `aggregate_function_is_avg_not_sum` unit tests | ✅ |
| `emit_all: true` set in the desugared MATCH | `desugar_retention.rs:112`; `basic_retention_produces_match_and_stats` verifies `pattern.emit_all` | ✅ |
| `window: None` when brackets are present (BRACKETS/WITHIN mutually exclusive) | `desugar_retention.rs:115`: `window: None`; `no_window_on_match_when_brackets_present` unit test | ✅ |
| `cumulative: true` forwarded from RETENTION arg to `BracketSpec.cumulative` | `desugar_retention.rs:64–118`: `retention.brackets` (including `cumulative` flag) forwarded as-is to `BracketSpec`; `cumulative_brackets_flag_is_preserved` unit test | ✅ |
| Table-qualified event refs preserved through desugaring (§19.3) | `table_qualified_event_refs_are_preserved` unit test in `desugar_retention.rs:357–400` | ✅ |
| Empty brackets list → `BqliteError::Plan` | `desugar_retention.rs:67–70`; `empty_brackets_returns_error` unit test | ✅ |
| Limitation: RETENTION sugar accepts only bare event types (no step predicates, no variable bindings, no WITHOUT) | Per-step predicates impossible via the sugar — `Step.predicate = None` is hardcoded at `desugar_retention.rs:90,103`. Correctly scoped; escape hatch via MATCH + BRACKETS + STATS documented in `query-language.md §6.3`. | ✅ (by design) |
| Original RETENTION source span preserved on both desugared stages for error reporting | Both `match_stage` and `stats_stage` carry `span` from `retention.span` (`desugar_retention.rs:108,166`). Step-level spans use `entry_span` / `activity_span` (`desugar_retention.rs:85–86,96,105`) for fine-grained error pointing. | ✅ |

---

### §4.4 — Scan-Range Widening by Maximum Bracket

| Promise | Evidence | Status |
|---------|----------|--------|
| Scan upper bound extended by `max(brackets)` beyond user's stated range | `logical.rs:2080–2086`: `max_bracket = brackets.as_ref().and_then(|b| b.durations.iter().copied().max()).unwrap_or(0)`, `extension = window_ns.max(max_bracket)`, then `acc.extend_scan_reader_forward(extension)` | ✅ |
| When both WITHIN and BRACKETS present, extend by `max(window, max_bracket)` | `extension = window_ns.max(max_bracket)` at `logical.rs:2084`; since BRACKETS and WITHIN are mutually exclusive, exactly one of `window_ns` / `max_bracket` is non-zero in practice | ✅ |
| Extension applies to LAST ranges, historical BETWEEN ranges, and open-ended ranges | `extend_scan_reader_forward` modifies the accumulated scan plan regardless of range form; covered generically | ✅ |

---

### §5.1 / §4.12 — MATCH Output Schema with BRACKETS

| Promise | Evidence | Status |
|---------|----------|--------|
| `bracket` column present (Int, non-nullable) when BRACKETS specified | `logical.rs:1991–1997`: `if pattern.brackets.is_some()` adds `bracket: Int, nullable: false` | ✅ |
| `bracket_end` column present (Int, non-nullable) when BRACKETS specified | `logical.rs:1998–2003`: adds `bracket_end: Int, nullable: false` alongside `bracket` | ✅ |
| `step_reached` present for EMIT ALL bracket queries (required downstream for `AVG(CAST(step_reached >= 2 ...))`) | `logical.rs:1978–1986`: `step_reached` is added only when `emit_all=true` (`if emit_all { output_columns.push(step_reached) }`). RETENTION desugaring always sets `emit_all=true` at `desugar_retention.rs:112`, so `step_reached` is always present in the desugared RETENTION form. The guard is not unconditional — a `MATCH … BRACKETS` query without `EMIT ALL` would get `bracket`/`bracket_end` columns but no `step_reached`. | ✅ |
| `bracket_end` semantics: "upper bound in nanos" — but relative to anchor or to epoch? | `logical.rs:1990` comment says "upper bound in nanos"; `query-language.md §4.12` table says "Bracket upper bound (nanos) for display". Neither source specifies whether this is nanoseconds-from-epoch (absolute) or nanoseconds-from-anchor (relative). Ambiguity exists in the spec. | ⚠️ |

---

### Operator Runtime — Bracket Emission

This section audits whether the MATCH operator actually implements bracket-slot emission at runtime.

| Promise | Evidence | Status |
|---------|----------|--------|
| Physical planner carries `BracketSpec` into the compiled physical node | `physical.rs:1237`: `brackets: _` — the bracket spec is **discarded** during SequenceMatch physical lowering. No `bracket_spec` field exists on `SequenceMatchPhysical`. | ❌ |
| Compiled NFA / step counter implements one-row-per-bracket emission | Neither `compile.rs`, `step_counter.rs`, nor `nfa.rs` reference brackets in their computation. `CompiledNfa` has no `brackets` field. The step counter's `StepCounterState` has no per-bracket tracking state. | ❌ |
| `output.rs` builds `bracket` and `bracket_end` columns with correct values | `output.rs:87–91`: `bracket` and `bracket_end` fall through the default arm `_ => build_null_column(...)`, emitting a fully-null array. Since `bracket` is declared non-nullable, `RecordBatch::try_new` panics at `output.rs:96`: `"Column 'bracket' is declared as non-nullable but contains null values"`. | ❌ |
| Cumulative bracket partial-sum accumulation implemented | No runtime path. The `cumulative` flag is forwarded correctly through AST → BracketSpec → logical plan but has no effect at the operator level because bracket emission itself is unimplemented. | ❌ |
| MATCH FIRST with BRACKETS: only first entry per entity/binding track used as cohort entry | Per spec §4.12; `MatchMode::First` is set correctly, but since bracket emission is unimplemented the behavior cannot be verified end-to-end. | ❌ |
| One row per (entity, binding track, bracket) emitted with EMIT ALL | See above — operator produces at most one row per entity (non-bracket behavior). | ❌ |

---

### EXPLAIN Fidelity

| Promise | Evidence | Status |
|---------|----------|--------|
| EXPLAIN shows that the MATCH carries a BRACKETS spec | `physical.rs:1237`: `brackets: _` is discarded during physical lowering. The resulting `SequenceMatchPhysical` has no bracket information. EXPLAIN output would not show the bracket list, bracket count, or cumulative flag. | ❌ |
| EXPLAIN shows extended scan range (original + bracket extension) | Scan range is correctly extended by `extend_scan_reader_forward`; the extended range is carried on `ScanPhysical.time_range`, which EXPLAIN renders. This part is correct. | ✅ |
| EXPLAIN shows desugared form when original was RETENTION sugar | Both desugared stages (`Match` and `Stats`) carry the original `Retention` source span; EXPLAIN errors reference the right location. The desugared plan tree (MATCH + STATS) is what EXPLAIN renders — users see the primitive form, consistent with §6.5's design intent. | ✅ |

---

### Integration Test Coverage (TASK-439)

| Test | Coverage | Status |
|------|----------|--------|
| `retention_standard_brackets_produces_expected_rates` | Standard RETENTION sugar (`brackets: [1d, 7d, 14d, 30d]`) on a 3-entity fixture | `#[ignore]` — blocked by `bracket` null panic |
| `retention_cumulative_brackets_are_monotone` | Cumulative RETENTION asserts non-decreasing rates | `#[ignore]` — blocked by same null panic |
| Direct `MATCH … BRACKETS [..]` (desugared form, no sugar) | Not covered by any test in the suite | ❌ |
| MATCH FIRST with BRACKETS (only-first-entry semantics) | Not covered | ❌ |
| MATCH ALL with BRACKETS (every entry starts its own window) | Not covered | ❌ |
| Bracket × variable bindings (§30.6: each binding track gets its own bracket eval) | Not covered | ❌ |
| BRACKETS in a joined-source query (§19.3) | Not covered | ❌ |
| Cumulative bracket monotonicity property (property test) | Not covered | ❌ |
| Out-of-order bracket durations (e.g., `[30d, 7d, 1d]`) | Not covered | ❌ |

---

## Drift and Missing Coverage — Follow-up Items for TASK-455

### R1 — Bracket emission not implemented in the operator (Critical)

**Promise**: `query-language.md §4.12` specifies that a BRACKETS query produces one row per `(entity, binding track, bracket)`. With EMIT ALL (as RETENTION always uses), every bracket is emitted regardless of completion; `step_reached` distinguishes completed brackets from dropouts. `bracket` is 0-indexed, `bracket_end` is the upper bound for that slice.

**Evidence**: The physical planner discards `brackets` (`physical.rs:1237: brackets: _`). Neither `CompiledNfa` nor `StepCounterState` carry bracket state. `output.rs` produces a null array for any unknown column name (including `bracket` and `bracket_end`). Since both are declared non-nullable in the output schema, `RecordBatch::try_new` panics: `"Column 'bracket' is declared as non-nullable but contains null values"`. This surfaces on every RETENTION query (via the sugar or directly via `MATCH … BRACKETS`).

**Impact**: RETENTION is entirely non-functional at runtime. Both integration tests are `#[ignore]`. The Wave 4 acceptance test (TASK-442) also uses a retention query — it depends on RETENTION being fixable before the acceptance gate passes.

**Required work**: (a) Add `brackets: Option<BracketSpec>` to `SequenceMatchPhysical`; forward it from the logical plan instead of discarding it. (b) Extend `CompiledNfa` to carry the bracket durations and `cumulative` flag so the step counter / NFA execution loop can compute bracket assignments at match completion or window expiry. (c) Implement per-bracket row emission in `output.rs` (one row per bracket per entity): for completion events, set `bracket` to the index of the slice containing `final_ts - anchor_ts`; for EMIT ALL / partial entries, emit a row for every bracket with `step_reached` set appropriately. (d) Implement cumulative bracket partial-sum: after exclusive per-bracket emission, for `cumulative=true`, carry forward each bracket's `step_reached` max so bracket N reflects activity in any bracket 0..N.

---

### R2 — No validation that bracket durations are strictly ascending

**Promise**: The semantics described in §4.12 ("Each bracket is a distinct time slice with no overlap") implicitly require `durations[0] < durations[1] < … < durations[N-1]`. Without this ordering the slice boundaries would be inverted or overlapping, producing incorrect bracket assignments.

**Evidence**: Neither the parser (`pattern.rs:481–527`) nor the planner (`logical.rs:lower_match`) validates ordering. A query `BRACKETS [30d, 7d, 1d]` is accepted silently.

**Impact**: Silent semantic corruption on any query with out-of-order brackets. Unlikely in practice (users typically write them in ascending order), but the guard should exist.

**Required work**: Add a planner validation in `lower_match` (or optionally in the parser after `parse_brackets`) that checks `durations[i] < durations[i+1]` for all adjacent pairs. Reject with `BqliteError::Plan("BRACKETS durations must be strictly ascending")`.

---

### R3 — `bracket_end` semantic ambiguity: relative to anchor vs. absolute from epoch

**Promise**: `query-language.md §4.12` says `bracket_end` is "Bracket upper bound (nanos) for display". `logical.rs:1990` says "upper bound in nanos". Neither source clarifies whether the value is an absolute nanosecond epoch timestamp (`anchor_ts + duration`) or a relative duration in nanoseconds (`duration` directly).

**Evidence**: No runtime implementation exists to resolve this empirically (see R1). The downstream STATS desugaring uses `GROUP BY bracket` (the 0-indexed integer), not `bracket_end`, so the ambiguity does not affect the RETENTION aggregate result — but it matters for user display and for direct use of `bracket_end` in downstream expressions.

**Impact**: Medium. Any downstream use of `bracket_end` in a user query (e.g., `SELECT bracket_end / 86400000000000 AS days`) would produce wrong results if the semantics are misunderstood.

**Required work**: Clarify the spec in `query-language.md §4.12` to explicitly state whether `bracket_end` is an absolute epoch timestamp (anchor + bracket duration) or a relative duration. Update the implementation accordingly and add a test that verifies a specific numeric value.

---

### R4 — EXPLAIN doesn't show brackets

**Promise**: `planner-pipeline.md §10.1` specifies that EXPLAIN shows strategy selection and structural decisions. For a RETENTION query, a user should be able to see that the MATCH carries brackets and how many.

**Evidence**: `physical.rs:1237`: `brackets: _` — the bracket spec is silently dropped during physical lowering. The `ExplainNode::SequenceMatch` variant doesn't carry brackets info.

**Impact**: Low for current users (RETENTION doesn't work at all until R1 is fixed). Once R1 is fixed, misleading EXPLAIN output becomes a real UX problem — a RETENTION query would show as a plain MATCH with no indication of bracket structure.

**Required work**: (a) Preserve `bracket_spec` on `SequenceMatchPhysical`. (b) Add a `brackets` field to `ExplainNode::SequenceMatch` that shows the bracket count, durations summary, and cumulative flag. (c) Confirm that the extended scan range (which is already correct) is rendered alongside the bracket info.

Note: the bracket gap is one part of a broader `ExplainNode::SequenceMatch` under-specification. The spec at `planner-pipeline.md §10.2:1150–1162` lists nine fields (`strategy`, `pattern_class`, `steps`, `window`, `emit_all`, `fused_agg`, `fused_filter`, `step_properties`, `input`). The actual implementation at `crates/bqlite-planner/src/explain.rs:66–75` has only four: `strategy`, `step_count`, `emit_all`, and `input`. Missing: `pattern_class`, `window`, `fused_agg`, `fused_filter`, and `step_properties`. Adding `brackets` should be done as part of a single pass that brings the implementation into alignment with the full spec-defined shape.

---

### R5 — Both RETENTION integration tests are `#[ignore]` with no passing alternatives

**Promise**: TASK-443 depends on TASK-439, which claims to cover "RETENTION bracket semantics (including cumulative mode)". The test file (`wave4_advanced_analytics_event_select.rs`) does include two RETENTION tests but both are `#[ignore]`.

**Evidence**: `wave4_advanced_analytics_event_select.rs:528–600`: `retention_standard_brackets_produces_expected_rates` and `retention_cumulative_brackets_are_monotone`, both `#[ignore = "RETENTION desugars to MATCH … BRACKETS and panics at bracket column nullability"]`. No alternative RETENTION coverage exists in the test suite; the wave4 acceptance test (TASK-442) lists a sessionized retention query in its description but has not yet been implemented (TASK-442 is blocked on multiple predecessors).

**Impact**: RETENTION has zero passing end-to-end test coverage. All correctness evidence for the feature comes from the desugaring unit tests (`desugar_retention.rs`) and parser unit tests — both of which test pre-runtime layers only.

**Required work**: Once R1 is fixed, un-`#[ignore]` both tests and strengthen them: assert specific bracket-indexed `retention_rate` values (not just `row_count > 0`), assert cumulative monotonicity numerically, and add at least one test for the direct desugared form (`MATCH … BRACKETS`).

---

### R6 — No property test for cumulative bracket monotonicity

**Promise**: `docs/core-beliefs.md §11` and `CLAUDE.md` (Testing section) call for property tests on components with large input spaces and clear invariants. Cumulative bracket semantics have a clear invariant: for any fixture, `rate[bracket=N] >= rate[bracket=N-1]` for all N.

**Evidence**: No property test exists for bracket or cumulative semantics anywhere in the test suite.

**Impact**: Low until R1 is fixed. After R1, the invariant is easy to state and cheap to test with proptest.

**Required work**: Add a proptest in the integration test file that generates random fixtures (entity count, event timestamps, bracket list) and asserts the cumulative monotonicity invariant on the RETENTION output.

---

### R7 — No test for BRACKETS × variable bindings composition (§30.6)

**Promise**: `query-language.md §30.6` states "each (entity, binding track) gets its own bracket evaluation" when both BRACKETS and variable bindings are specified. Each track produces its own set of per-bracket rows.

**Evidence**: No test exists for this combination. The design note is in the "Resolved Design Questions" section of query-language.md and is not backed by any test coverage.

**Required work**: Once R1 is fixed, add an integration test that uses `MATCH FIRST SEQUENCE($plan: signup WHERE $plan IS NOT NULL THEN purchase) BRACKETS [7d, 30d] EMIT ALL` and asserts that each (entity, binding-track) combination produces two bracket rows with independent `step_reached` values.

---

## Summary

The RETENTION implementation is **structurally complete at the parser, AST, and planner levels**: parsing, desugaring, scan-range extension, output schema construction, and the STATS aggregate naming all correctly match the spec. These layers have good unit-test coverage via the desugaring tests and parser tests.

The feature is **entirely non-functional at the operator runtime level**. The physical planner silently discards the `BracketSpec` (R1), the compiled NFA and step counter have no bracket logic (R1), and the output builder emits a null array for the non-nullable `bracket` column (R1), causing a panic on every RETENTION query. Both integration tests are `#[ignore]`. Zero passing end-to-end tests exist.

| Item | Severity | Blocking? |
|------|----------|-----------|
| R1: Bracket emission not implemented at all — runtime panic on every RETENTION query | **Critical** | Yes — RETENTION is non-functional; blocks TASK-442 acceptance test |
| R2: No validation that bracket durations are strictly ascending | Medium | No (silent semantic error on malformed input) |
| R3: `bracket_end` relative-vs-absolute ambiguity in spec | Medium | No (doesn't affect the STATS aggregate, only direct column use) |
| R4: EXPLAIN doesn't show brackets | Low | No (UX issue after R1 is fixed) |
| R5: Both RETENTION integration tests `#[ignore]` | High | No (tests are correct; need R1 before they can run) |
| R6: No property test for cumulative monotonicity | Low | No |
| R7: No test for BRACKETS × variable bindings | Low | No |
