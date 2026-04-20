# Wave 4 ATTRIBUTE Semantic Audit

**Auditor**: TASK-446
**Date**: 2026-04-20
**Sources reviewed**:
- Design spec: `docs/design/operators/attribute.md` (TASK-406, Wave 4)
- Query-language spec: `docs/design/query-language.md` §14.3, §25.2
- Type-system spec: `docs/design/type-system.md` §6.14
- Planner spec: `docs/design/planner-pipeline.md` §8.4, §13
- Execution-model spec: `docs/design/execution-model.md` §2.1
- Parser: `crates/bqlite-parser/src/pipeline.rs` (TASK-422)
- AST: `crates/bqlite-ast/src/operator.rs` (`Attribute`)
- Planner — logical: `crates/bqlite-planner/src/logical.rs` (`lower_attribute`, `attribute_conversion_range`)
- Planner — physical: `crates/bqlite-planner/src/physical.rs` (`AttributePhysical`, physical lowering)
- Planner — explain: `crates/bqlite-planner/src/explain.rs` (`ExplainNode::Attribute`)
- Operator: `crates/bqlite-operators/src/attribute.rs` (`AttributeOperator`, TASK-431)
- Property tests: `tests/tests/prop_attribute.rs`
- Integration tests: `tests/tests/wave4_advanced_analytics_attribute_cohort_join.rs` (TASK-439 CP3)
- Benchmarks: `benches/wave4/attribute.rs` (TASK-431)

**Methodology**: Walk each design-doc promise for the ATTRIBUTE feature; locate primary evidence in code and tests; classify each as ✅ Covered, ⚠️ Partial, or ❌ Missing. Follow-up items for partial/missing rows are filed at the end. Nothing is fixed here — all drift and missing coverage are rolled up into TASK-455.

---

## Promise-vs-Evidence Matrix

### §3 — Parameters and List Extension

| Promise | Evidence | Status |
|---------|----------|--------|
| `ATTRIBUTE(conversion: …, touchpoints: …, window: …, touchpoint_key: …)` — all four parameters required, any order | `parse_attribute_stage` at `pipeline.rs:1576`; accumulates the four keys; missing-key error fired when closing `)` | ✅ |
| `event_ref_list := event_ref \| "(" event_ref ("," event_ref)* ")"` — single or parenthesized list | `parse_attr_event_ref_list` at `pipeline.rs`; single-ref accepted without parens; parenthesized list consumed with comma separation | ✅ |
| Duplicate event type within each list rejected at parse time | `parse_attr_event_ref_list` guards duplicates with error `"duplicate event type in ATTRIBUTE \`conversion:\` list"` / `\`touchpoints:\` list"` at `pipeline.rs:1617–1634` | ✅ |
| Lists may overlap across the two parameters (`conversion: E, touchpoints: E`) | Comment in parser: "Lists may overlap across the two parameters"; no cross-list dedup check; `same_event_type_no_self_attribution` unit test confirms valid overlap | ✅ |
| Duplicate parameter key rejected (e.g., `window:` twice) | Duplicate key guard inside the parameter loop at `pipeline.rs:1608–1629, 1637–1644`; `detail: Some("duplicate …")` error | ✅ |
| Unknown parameter key rejected | The `_ =>` catch-all arm at the end of the parameter dispatch produces a `ParseError` for unrecognised keys | ✅ |
| `window:` value must be a duration literal | `if let TokenKind::Duration(ns) = p.peek().kind` at `pipeline.rs:1648`; non-duration rejected with `Expected::Literal` error | ✅ |
| `touchpoint_key:` accepts any scalar expression | `parse_expr` called for the key — full expression surface available | ✅ |

---

### §4 / §4.1 / §4.2 — Output Schema and Row Shapes

| Promise | Evidence | Status |
|---------|----------|--------|
| Output: `entity_id NOT NULL, conversion_ts NOT NULL, touchpoint_ts NULL, touchpoint_key NULL` | `lower_attribute` at `logical.rs:2833–2843`: four `ColumnDef`s with correct nullability | ✅ |
| Demand-forwarded conversion properties inserted between `conversion_ts` and `touchpoint_ts` | Demand analysis (planner optimizer pass) splices forwarded columns by name; `forwarded_conversion_columns: Vec::new()` at construction time, populated downstream | ✅ |
| Row shape 1: normal attribution — non-null `touchpoint_ts`, non-null `touchpoint_key` | `append_row(… Some(entry.ts), entry.key.as_deref())` at `attribute.rs:783–790` when `key` is `Some` | ✅ |
| Row shape 2: qualifying touchpoint, null key — non-null `touchpoint_ts`, null `touchpoint_key` | `key: Option<CompactString>` allows `None`; `entry.key.as_deref()` passes `None` to builder; `null_touchpoint_key_emits_row_with_null_key` unit test | ✅ |
| Row shape 3: LEFT-UNNEST — null `touchpoint_ts`, null `touchpoint_key` | `append_row(…, None, None)` when `matched == 0` at `attribute.rs:793–805`; `only_conversions_emits_left_unnest_per_conversion` unit test | ✅ |
| Cardinality: K conversions × max(N, 1) rows | `cardinality_matches_spec` property test; `every_conversion_emits_at_least_one_row` property test | ✅ |
| `touchpoint_key` type must be `String` (or CAST to String) | Planner validation at `logical.rs:2821–2826`; operator-level check at `attribute.rs:163–168`; `rejects_non_string_touchpoint_key` unit test | ✅ |

---

### §5 — Window Boundary Rules

| Promise | Evidence | Status |
|---------|----------|--------|
| §5.1 Inclusion rule: `conversion_ts - window <= touchpoint_ts < conversion_ts` | `emit_conversion` at `attribute.rs:782`: `entry.ts >= lookback_edge && entry.ts < conversion_ts`; `inclusive_lookback_boundary_qualifies` and `strict_at_conversion_boundary_excludes` unit tests | ✅ |
| §5.1 Inclusive at lookback edge — touchpoint at exactly `conversion_ts - window` qualifies | `>=` in the qualification guard; `inclusive_lookback_boundary_qualifies` test uses `(0, "click", …), (1000, "purchase", …)` with `window_ns = 1000` and asserts 1 output row | ✅ |
| §5.1 Strict at `conversion_ts` — touchpoint at same timestamp excluded | `<` guard in emit loop; `strict_at_conversion_boundary_excludes` test: click at ts=500, purchase at ts=500 → LEFT-UNNEST row (touchpoint excluded) | ✅ |
| §5.2 Window rule is `ts`-space only — `__seq_id` not a window-boundary participant | Code uses only `entry.ts` and `conversion_ts` in the window check; no `__seq_id` references in `emit_conversion`; `window_boundaries_respected` property test validates; `no_self_attribution` property test confirms same-ts events don't attribute | ✅ |
| §16.1 `window: 0s` semantically valid — spec says "not rejected at plan time" | `lower_attribute` at `logical.rs:2778` **rejects** `args.window <= 0` with `BqliteError::Plan`. The design doc §16.1 explicitly says "This is semantically valid but useless — not rejected at plan time". **Spec/implementation drift.** The operator handles it correctly when constructed directly via `from_physical` (unit test `zero_window_always_left_unnests` passes), but the planner blocks it from the full pipeline. | ❌ |

---

### §6 — Per-Event Processing Order

| Promise | Evidence | Status |
|---------|----------|--------|
| §6.1 Deque pruning before conversion check | `process_sub_batch` order at `attribute.rs:679–695`: prune loop first, then `is_conversion` check | ✅ |
| §6.2 Conversion emission before touchpoint add (emit-before-add) | `if is_conversion … emit_conversion(…)` at `attribute.rs:699–701` runs before `if is_touchpoint … deque.push_back(…)` at `attribute.rs:703–733` | ✅ |
| §6 Self-attribution prevented by emit-before-add | `same_event_type_no_self_attribution` unit test; `no_self_attribution` property test (256 cases, `X` event is both conversion and touchpoint) | ✅ |
| §6 Overlap with multi-list: event matching both lists follows emit-before-add | `multi_type_conversion_and_touchpoint_lists` unit test; `operator_matches_reference` property test exercises overlap via `X` type | ✅ |
| §6 Pruning cutoff: drop entries with `entry.ts < event.ts - window_ns` (inclusive at `>=`) | `cutoff = ts.saturating_sub(self.window_ns)` at `attribute.rs:683`; `front.ts < cutoff` drops only entries strictly below the cutoff; `deque_pruning_drops_stale_entries` unit test | ✅ |

---

### §7 — Emission Order

| Promise | Evidence | Status |
|---------|----------|--------|
| Touchpoints emitted in ascending `touchpoint_ts` order (FIFO from deque) | `for entry in state.deque.iter()` at `attribute.rs:780` — iteration over `VecDeque` is FIFO (front to back); deque entries are added in ascending `ts` order due to storage invariant; `emission_order_is_ascending_touchpoint_ts` unit test | ✅ |

---

### §8 / §8.3 — Per-Entity State Layout

| Promise | Evidence | Status |
|---------|----------|--------|
| Deque element: `ts: i64, key: Option<CompactString>` | `TouchpointDequeEntry { ts: i64, key: Option<CompactString> }` at `attribute.rs:325–331` | ✅ |
| `CompactString` used for key to avoid per-entry heap for short strings | `compact_str::CompactString::from(…)` at `attribute.rs:729`; inline for `len <= 24`; design note and TASK-454 cross-reference in comment | ✅ |
| Deque cleared at entity boundary | `create_state` at `attribute.rs:609`: new empty `VecDeque`; `finish_entity` consumes `state` (drops it) | ✅ |
| `ConversionScratch` (forwarded values) held only for duration of emission, not stored in deque | `forwarded_src` slice built from `forwarded_conversion_columns` names per sub-batch at `attribute.rs:651–665`; passed through to `append_row` where it is read from the source batch row — no per-entity scratch buffer stored between events | ✅ |

---

### §9 — Per-Entity Deque Cap

| Promise | Evidence | Status |
|---------|----------|--------|
| Cap at 1,000,000 touchpoints | `DEFAULT_ATTRIBUTE_DEQUE_CAP: usize = 1_000_000` at `attribute.rs:46` | ✅ |
| On cap: flush in-flight conversion, skip remaining entity events, emit diagnostic | `state.capped = true` at `attribute.rs:714`; in-flight conversion rows already persisted in `state.builders` (they were emitted before the cap fired per the event order); `break` exits the row loop; `finish_entity` still returns the accumulated rows | ✅ |
| Diagnostic shape matches design: `entity_id, event_count, operator, cap` | `EntityCapDiagnostic` at `attribute.rs:64–74`; `operator: ATTRIBUTE_OP_NAME`; `deque_cap_flushes_and_skips_entity` unit test asserts `diag.cap == 2`, `diag.operator == ATTRIBUTE_OP_NAME`, `diag.event_count >= 4` | ✅ |
| Query succeeds — no error on cap | Operator returns `None` / partial batch from `finish_entity`; no `Result::Err` path from `process_sub_batch` | ✅ |
| Cap fires on _touchpoint_ add exceeding the cap, not on conversion events | Cap check at `attribute.rs:707–721` is inside the `if is_touchpoint { … }` block | ✅ |
| Configurable cap for tests via `with_deque_cap` | `AttributeOperator::with_deque_cap(n)` at `attribute.rs:241–244`; used in `deque_cap_flushes_and_skips_entity` unit test with cap=2 | ✅ |

---

### §10 — Demand-Driven Conversion Property Forwarding

| Promise | Evidence | Status |
|---------|----------|--------|
| Forwarded conversion properties sourced from the conversion event and attached to every row of that conversion (including LEFT-UNNEST rows) | `forwarded_src` built from `forwarded_conversion_columns` names; `append_row` receives `forwarded_src, row` where `row` is the conversion event index; LEFT-UNNEST path passes the same conversion row index | ✅ |
| Unit test for forwarded column on normal attributed row | `forwarded_conversion_column_flows_to_output`: click + purchase(amount=42) → `amount == 42` on output row | ✅ |
| Unit test for forwarded column on LEFT-UNNEST row | `forwarded_conversion_column_left_unnest_copies_value`: bare purchase with no touchpoint → `amount == 42` on the LEFT-UNNEST row | ✅ |
| §10.2 Multi-conversion-type forwarding: column NULL when that conversion type doesn't carry it | Forwarded column builder has `append_null` path triggered when source row `is_null(row)` at `attribute.rs:538–539` | ✅ |
| Forwarded columns not present in demand set are not read | Demand analysis sets `forwarded_conversion_columns`; `required_column_names` only includes demanded columns; scan respects projection | ✅ |

---

### §11 — `touchpoint_key` Expression Surface

| Promise | Evidence | Status |
|---------|----------|--------|
| Any scalar expression valid in BQL accepted for `touchpoint_key` | `parse_expr` used in the parser; full scalar expression surface available | ✅ |
| Result must type-check to `String` — non-string requires explicit `CAST` | Planner check at `logical.rs:2821–2826`; `rejects_non_string_touchpoint_key` unit test | ✅ |
| Aggregate and window functions rejected at plan time | Not explicitly guarded by a dedicated check — relies on `TypedExpr::from_ast` naturally failing for aggregate/window expressions since they require a group-context that is not present during scalar lowering | ⚠️ |
| Expression evaluated per touchpoint row, not per conversion row | `eval::evaluate(&self.touchpoint_key, batch)` is called once per sub-batch at `attribute.rs:643`; `key_view.value(row)` indexing is inside the per-row loop only in the `is_touchpoint` branch at `attribute.rs:724–729` | ✅ |
| Column validation is against source schema (runtime evaluation, not per-event-type) | `TypedExpr::from_ast(&args.touchpoint_key, &input_schema, registry)` at `logical.rs:2820` uses the source table schema; NULL on rows where the column is absent from the touchpoint event type (per §11 design rationale) | ✅ |

---

### §12 — Scan-Range Extension

| Promise | Evidence | Status |
|---------|----------|--------|
| Planner widens scan backward by `window` | `acc.extend_scan_reader_backward(args.window)` at `logical.rs:2852` | ✅ |
| `conversion_range` captures pristine query range for BETWEEN | `attribute_conversion_range(&acc)` at `logical.rs:2848`; walks to primary `Scan`, extracts `TimeRange::Between` and returns `Some((start_ns, end_ns))` | ✅ |
| LAST ranges resolved at physical layer (needs `now_ns`) | `resolve_last_range_from_scan(&input, now_ns)` at `physical.rs:1472–1473` when logical `conversion_range` is None | ✅ |
| Conversion emission filtered internally to `[outer_start, outer_end)` | `is_in_conversion_range(ts)` at `attribute.rs:759–764`: `None` → always emit; `Some((start, end))` → `ts >= start && ts < end` | ✅ |
| Touchpoints from extended zone are deque material only, never conversion triggers | Conversion emission guarded by `is_in_conversion_range` at `attribute.rs:699` | ✅ |
| Unit test for conversion-range filter | `conversion_range_excludes_conversions_outside` and `conversion_at_upper_bound_is_excluded` unit tests | ✅ |
| Integration test covering end-to-end scan extension (`events LAST 60d \| ATTRIBUTE(window: 30d, …)`) | No integration test exercises the LAST scan-extension path through the full engine. The integration tests use bare `events \| ATTRIBUTE(…)` without a time range. | ❌ |

---

### §13 — Fused Aggregate Shapes

| Promise | Evidence | Status |
|---------|----------|--------|
| v1 emits flat rows; no ATTRIBUTE → STATS fusion | `fused_aggregate: None` in every construction path; `from_physical` returns `Err` if `fused_aggregate.is_some()`; `rejects_fused_aggregate_in_v1` unit test | ✅ |
| Wave 5 fusion shapes documented but not implemented | `planner-pipeline.md §7.4.4` table preserved; `attribute.md §13` identifies the three fusion patterns as Wave 5 targets | ✅ |

---

### §14 — Composition Rules

| Promise | Evidence | Status |
|---------|----------|--------|
| §14.1 `SESSIONIZE \| ATTRIBUTE` allowed | `extend_scan_reader_backward` recurse arm at `logical.rs:920`: `LogicalPlan::Sessionize { input, .. }` passes through; no guard blocking SESSIONIZE upstream of ATTRIBUTE | ✅ |
| §14.1 Session boundaries not treated specially — deque spans sessions | `process_sub_batch` has no session-boundary awareness; deque accumulates touchpoints across all sub-batches regardless of `session_id` presence | ✅ |
| §14.1 SESSIONIZE output `session_id` / `session_duration` forwarded demand-driven | These are ordinary columns in the SESSIONIZE output schema; demand propagation treats them identically to any other column | ✅ |
| §14.1 No integration test for `SESSIONIZE \| ATTRIBUTE` composition | No test exercises this pipeline in either the integration suite or unit tests | ❌ |
| §14.2 `MATCH \| ATTRIBUTE` rejected with `TypeError` at plan time | No explicit guard in `lower_attribute` for `acc` being a `SequenceMatch` output. The MATCH output schema lacks `event_type`, `ts` (at their raw source positions), making the `touchpoint_key` expression resolution likely fail — but the error message would be a schema resolution error, not the documented `TypeError`. No test covers this case. | ❌ |
| §14.3 Valid downstreams of ATTRIBUTE (WHERE, SELECT, LET, STATS, ORDER BY, LIMIT) | Composition table at `query-language.md §25.2` line 1573 lists all six; integration tests cover WHERE and STATS; the remaining combinator paths are covered by the general pipeline lowering | ✅ |

---

### §15 — EntityOperator Integration

| Promise | Evidence | Status |
|---------|----------|--------|
| `EntityOperator::create_state` initializes fresh per-entity state | `create_state` at `attribute.rs:609–619` | ✅ |
| `process_sub_batch` implements §6 event loop | Correct ordering verified in earlier matrix rows | ✅ |
| `finish_entity` clears deque and returns batch (or None if empty) | `finish_entity` at `attribute.rs:739–743`: calls `finish_into_batch` if `builders.rows > 0`, else returns `None`; `finish_entity_returns_none_when_no_conversions_emitted` test | ✅ |
| `output_schema()` returns the planner-threaded schema | `&self.output_schema` — verbatim pass-through | ✅ |
| `required_columns()` includes entity_id, ts, event_type, forwarded, and expression column refs | `required_column_names` built at construction; `collect_expr_columns` walker; `required_columns_includes_forwarded_and_expression_refs` unit test | ✅ |
| `supported_demands()` matches `AttributePhysical::DEMAND_CAPS` | `supported_demands` returns `AttributePhysical::DEMAND_CAPS`; `exposes_capability_matches_physical_descriptor` unit test | ✅ |
| §15.2 Sub-batch streaming: deque persists across sub-batches | `deque` is on `AttributeState` which persists per entity; `single_conversion_single_touchpoint_across_sub_batches` unit test and `sub_batch_boundary_invariance` property test | ✅ |
| §15.3 Arrow builders pre-sized and respect output schema column order | `finish_into_batch` iterates `output_schema.columns()` to assemble output, not a hardcoded order | ✅ |
| `StringViewBuilder` used for `touchpoint_key` | `touchpoint_key: StringViewBuilder` at `attribute.rs:400`; `StringViewBuilder::new()` | ✅ |
| `TimestampNanosecondBuilder::with_timezone("UTC")` for both timestamp columns | `attribute.rs:396, 401`; UTC timezone applied | ✅ |
| `AttributeOperator: Send + Sync`, `AttributeState: Send` | `is_send_sync` and `state_is_send` unit tests | ✅ |

---

### §16.1 — Edge-Case Matrix

| Case | Evidence | Status |
|------|----------|--------|
| Empty entity | `empty_entity_yields_no_rows` unit test | ✅ |
| Entity with only touchpoints, no conversions | `only_touchpoints_yields_no_rows` unit test | ✅ |
| Entity with only conversions, no touchpoints | `only_conversions_emits_left_unnest_per_conversion` unit test | ✅ |
| Conversion with touchpoint at `conversion_ts - window` (inclusive edge) | `inclusive_lookback_boundary_qualifies` unit test | ✅ |
| Touchpoint at exactly `conversion_ts` (strict exclusion) | `strict_at_conversion_boundary_excludes` unit test | ✅ |
| Same event type for conversion and touchpoints | `same_event_type_no_self_attribution` unit test | ✅ |
| Multiple conversions sharing same touchpoints | `multiple_conversions_share_same_touchpoint` unit test | ✅ |
| Touchpoint with NULL `touchpoint_key` expression | `null_touchpoint_key_emits_row_with_null_key` unit test | ✅ |
| Deque cap exceeded | `deque_cap_flushes_and_skips_entity` unit test | ✅ |
| Single-event entity (one conversion, no touchpoints) | Covered by `only_conversions_emits_left_unnest_per_conversion` (degenerate case) | ✅ |
| Single-event entity (one touchpoint, no conversion) | `finish_entity_returns_none_when_no_conversions_emitted` unit test | ✅ |
| `window: 0s` | Operator handles it correctly when constructed via `from_physical` (`zero_window_always_left_unnests` unit test). Planner rejects it — spec says it should not be rejected. See A1. | ⚠️ |
| Conversion at boundary of scan extension zone | `conversion_range_excludes_conversions_outside` and `conversion_at_upper_bound_is_excluded` unit tests | ✅ |
| Touchpoints across sub-batch boundaries | `single_conversion_single_touchpoint_across_sub_batches` unit test; `sub_batch_boundary_invariance` property test | ✅ |
| Multi-type conversion and touchpoint lists | `multi_type_conversion_and_touchpoint_lists` unit test | ✅ |
| Forwarded conversion property missing on one conversion event type | `ForwardedBuilder::append_from_array` NULL path for absent values; no dedicated edge-case test for multi-type conversion with heterogeneous schemas | ⚠️ |

---

### §16.2 — Invariants

| Invariant | Evidence | Status |
|-----------|----------|--------|
| Invariant 1: entity ordering preserved — output rows grouped by entity in same order as input | `EntityOperatorAdapter` drives all `EntityOperator` implementations in entity-sorted input order; this guarantee is at the framework level (`execution-model.md §2.1`) rather than operator-level code; no dedicated ATTRIBUTE-specific test for multi-entity ordering | ✅ (by framework) |
| Invariant 2: per-entity conversion ordering preserved — conversions appear in ascending `conversion_ts` order within an entity | Events arrive in `(ts, __seq_id)` order per storage invariant; `process_sub_batch` processes events sequentially without reordering; conversions are emitted in the order they are encountered in the event stream, which is ascending `ts`; no dedicated unit test, but implied by `emission_order_is_ascending_touchpoint_ts` and the property tests | ✅ (by storage invariant) |
| Invariant 3: per-conversion touchpoint ordering — ascending `touchpoint_ts` within a conversion's rows | `§7 — Emission Order` row above; `emission_order_is_ascending_touchpoint_ts` unit test | ✅ |
| Invariant 4: LEFT-UNNEST exactly once per unattributed conversion | `emit_conversion`: `if matched == 0 { append_row(…, None, None) }` emits exactly one LEFT-UNNEST row; `every_conversion_emits_at_least_one_row` property test | ✅ |
| Invariant 5: no self-attribution — conversion+touchpoint overlap event does not attribute to itself | Emit-before-add; `no_self_attribution` property test (256 cases) | ✅ |
| Invariant 6: window rule is `ts`-space only — `__seq_id` never in qualification predicate | No `__seq_id` reference in `emit_conversion`; `window_boundaries_respected` property test | ✅ |
| Invariant 7: deque cap is per-entity — cap on entity A does not affect entity B | `capped` flag on `AttributeState`, which is created fresh per entity via `create_state`; `deque_cap_flushes_and_skips_entity` test verifies cap fires only for the affected entity | ✅ |
| Invariant 8: conversion-range filter is internal — scan-extended events visible to deque but never trigger emission outside `[outer_start, outer_end)` | `is_in_conversion_range` guards all `emit_conversion` calls; `conversion_range_excludes_conversions_outside` unit test | ✅ |

**Note on Invariants 1 and 2**: Neither has a dedicated ATTRIBUTE-specific test. Multi-entity ordering (Invariant 1) is enforced by the `EntityOperatorAdapter` dispatch loop driving all `EntityOperator` implementations in entity-sorted order — it is a cross-cutting framework guarantee. Per-entity conversion ordering (Invariant 2) depends on the storage invariant that events arrive in ascending `(ts, __seq_id)` order; no operator-level code reorders them. Both would benefit from a single integration test that inserts two entities with interleaved timestamps and asserts entity-grouped output ordering.

---

### §17 — Benchmarks and Property Tests

| Coverage | Evidence | Status |
|----------|----------|--------|
| Benchmark: single-entity many touchpoints | `single_entity_many_touchpoints` bench in `benches/wave4/attribute.rs` | ✅ |
| Benchmark: many entities sparse | `many_entities_sparse` bench | ✅ |
| Benchmark: high fan-out | `high_fan_out` bench | ✅ |
| Benchmark: LEFT-UNNEST dominant | `left_unnest_dominant` bench | ✅ |
| Benchmark: multi-type attribution | `multi_type_attribution` bench | ✅ |
| Property: operator matches reference evaluator row-for-row | `operator_matches_reference` (256 cases, over random streams and windows 0–500ns); reference evaluator is an independent reimplementation of §5.1/§6 semantics | ✅ |
| Property: every conversion emits ≥ 1 row (LEFT-UNNEST guarantee) | `every_conversion_emits_at_least_one_row` (256 cases) | ✅ |
| Property: window boundary predicates (`touchpoint_ts < conversion_ts`, `touchpoint_ts >= conversion_ts - window`) | `window_boundaries_respected` (256 cases) | ✅ |
| Property: no self-attribution | `no_self_attribution` (256 cases) | ✅ |
| Property: cardinality = Σ max(qualifiers, 1) | `cardinality_matches_spec` (256 cases) | ✅ |
| Property: `window: 0s` always LEFT-UNNESTs | `window_zero_always_left_unnests` (256 cases) — tests operator layer; planner rejects before reaching the operator in the full pipeline | ⚠️ |
| Property: sub-batch boundary invariance | `sub_batch_boundary_invariance` (256 cases, random split points) | ✅ |

---

### Integration Test Coverage (TASK-439 CP3)

| Test | Coverage | Status |
|------|----------|--------|
| `attribute_unattributed_conversion_emits_left_unnest_row` | LEFT-UNNEST shape via full engine pipeline | ✅ |
| `attribute_multiple_touchpoints_produces_n_rows` | N-row auto-unnest; touchpoint_ts / touchpoint_key values verified | ✅ |
| `attribute_null_touchpoint_key_is_distinct_from_unattributed` | Row-shape 2 vs LEFT-UNNEST distinction (§4.1) | ✅ |
| `attribute_downstream_stats_counts_per_channel` | ATTRIBUTE → WHERE → STATS composition; per-channel attribution counts | ✅ |
| ATTRIBUTE with `events LAST <d> \| ATTRIBUTE(window: …)` scan extension | Not covered — no integration test uses a time-range source before ATTRIBUTE | ❌ |
| ATTRIBUTE with multi-type `conversion: (A, B)` or `touchpoints: (C, D)` | Not covered by integration tests (operator-level coverage only) | ❌ |
| `SESSIONIZE \| ATTRIBUTE` composition | Not covered | ❌ |
| ATTRIBUTE + forwarded conversion property via full engine | Not covered — unit tests cover this at the operator level only | ❌ |
| `joined_source_stats_counts_entities_in_both_tables` | `#[ignore]` — `MergeSourcesOperator __seq_id` nullability bug | ⚠️ |
| `joined_source_sequence_match_spans_tables` | `#[ignore]` — same bug | ⚠️ |
| `MATCH \| ATTRIBUTE` rejected at plan time | Not covered | ❌ |

---

### Documentation Coverage

| Document | §14.3 ATTRIBUTE surface syntax | §25.2 composition table | §6.14 type-system schema | Status |
|----------|-------------------------------|------------------------|--------------------------|--------|
| `query-language.md §14.3` | Complete: list syntax, window boundary rule, three-way row shapes, scan-range widening, self-type-attribution rule, emission order, worked examples | ✅ | — | ✅ |
| `query-language.md §25.2` | — | ATTRIBUTE appears in valid-downstream and SESSIONIZE downstream rows; §1577 note about v1 session-scoping caveat | — | ✅ |
| `type-system.md §6.14` | — | — | Output schema columns, `touchpoint_key` typing, LEFT-UNNEST semantics, cardinality rule documented | ✅ |

---

## Drift and Missing Coverage — Follow-up Items for TASK-455

### A1 — `window: 0s` rejected by planner despite spec saying "not rejected at plan time" (Medium)

**Promise**: `attribute.md §16.1` row "`window: 0s`" explicitly states "This is semantically valid but useless — not rejected at plan time."

**Evidence**: `lower_attribute` at `logical.rs:2778` has:
```rust
if args.window <= 0 {
    return Err(BqliteError::Plan(format!(
        "ATTRIBUTE: window must be positive — got {}ns",
        args.window
    )));
}
```
This rejects both zero and negative windows at the planner level. The operator handles `window: 0` correctly (always LEFT-UNNESTs, `zero_window_always_left_unnests` unit test) but the planner blocks it. The property test `window_zero_always_left_unnests` covers the operator layer by constructing directly via `from_physical`, bypassing the planner.

**Impact**: A user writing `ATTRIBUTE(window: 0s, …)` gets a plan-time error instead of the documented "every conversion LEFT-UNNESTs" behavior. The spec says this is valid syntax (with a useless but well-defined result).

**Required work**: Two options — (a) Remove the `<= 0` guard for the zero case and keep only `< 0` to reject negative windows; update the spec to clarify negative-window behavior. (b) Update the design doc §16.1 to say "also rejected at plan time" and add a plan-error test. Decision should be made before TASK-455 closes.

---

### A2 — `MATCH | ATTRIBUTE` composition not explicitly rejected with `TypeError` (Medium)

**Promise**: `attribute.md §14.2` states: "`MATCH | ATTRIBUTE` is rejected — MATCH emits per-match rows, not raw event rows; there is no meaningful input shape for ATTRIBUTE to consume. The planner rejects this composition at plan time with a `TypeError`."

**Evidence**: No explicit guard in `lower_attribute` checking that `acc` (the upstream plan) is not a `SequenceMatch` output. The error a user would actually receive depends on whether the MATCH output schema happens to contain a column named `event_type` (it does not — MATCH output columns are named `step_1_ts`, `entity_id`, etc.). The `touchpoint_key` type-check would likely fail with a "column not found" error rather than a `TypeError`. No test covers this rejection path.

**Impact**: The composition may accidentally fail (wrong error shape, no clear user-facing message), or might silently pass if someone constructs a MATCH output schema that happens to look like an event schema. The documented `TypeError` is not produced.

**Required work**: Add an explicit check in `lower_attribute` (or in a composition-validator pass) that rejects `MATCH | ATTRIBUTE` with the message specified in §14.2. Add a planner unit test asserting the rejection.

---

### A3 — EXPLAIN output missing `touchpoint_key`, `conversion_range`, and forwarded columns (Low)

**Promise**: The general EXPLAIN contract (`planner-pipeline.md §10.1`) says EXPLAIN shows strategy selection and structural decisions. For ATTRIBUTE, users should be able to see the touchpoint_key expression, whether scan-extension is in effect (`conversion_range`), and which conversion properties are forwarded.

**Evidence**: `ExplainNode::Attribute` at `explain.rs:121–128` carries only `conversion_events`, `touchpoint_events`, `window`, and `input`. The `format_attribute` EXPLAIN test at `explain.rs:1313` confirms only these four fields are rendered. `touchpoint_key` (which might be a complex expression), `conversion_range`, and `forwarded_conversion_columns` are invisible in EXPLAIN output.

**Impact**: Low for operational use but notable for debugging attribution queries — a user who writes `ATTRIBUTE(… touchpoint_key: CONCAT(channel, ':', campaign))` with a typo gets no EXPLAIN evidence of what expression was compiled.

**Required work**: (a) Add `touchpoint_key: String` field to `ExplainNode::Attribute` (formatted as a human-readable expression). (b) Optionally add `conversion_range: Option<String>` to show the original query range and whether scan extension fired. (c) Add `forwarded_columns: Vec<String>` when non-empty. These are display-only; no correctness impact.

---

### A4 — No integration test for scan-range extension via LAST time range (Medium)

**Promise**: `attribute.md §12` specifies that `events LAST 30d | ATTRIBUTE(window: 30d, …)` "should just work" — the planner widens the scan backward by `window` so touchpoints from the lookback zone qualify for in-range conversions. This is a key feature promised to users.

**Evidence**: The unit tests `conversion_range_excludes_conversions_outside` and `conversion_at_upper_bound_is_excluded` cover the `conversion_range` filter at the operator layer. But no integration test constructs an actual `Database`, inserts events spanning two months, and runs `events LAST <d> | ATTRIBUTE(window: <d>, …)` to verify that pre-range touchpoints are actually credited to in-range conversions. The integration tests all use bare `events | ATTRIBUTE(…)` without any time range restriction.

**Impact**: The scan-extension path through the full engine (`physical::lower_physical`'s `resolve_last_range_from_scan` → `conversion_range` → `is_in_conversion_range`) has no end-to-end test coverage.

**Required work**: Add an integration test that inserts a touchpoint at `T0` and a conversion at `T0 + 35d`, then queries `events LAST 30d | ATTRIBUTE(window: 30d, …)` and asserts that the touchpoint (at `T0`, outside the LAST 30d window but inside the extended scan) appears as an attributed touchpoint.

---

### A5 — No integration test for multi-type conversion/touchpoint lists (Low)

**Promise**: `attribute.md §3` and `query-language.md §14.3` document `conversion: (purchase, subscription)` and `touchpoints: (ad_click, email_open)` as first-class syntax. The design note at §10.2 specifies multi-conversion-type forwarding behavior.

**Evidence**: Operator-level tests cover multi-type lists (`multi_type_conversion_and_touchpoint_lists`); property tests exercise the `C`/`T`/`X` mixed inventory. No integration test exercises the full multi-type list form through the parser, planner, and engine.

**Required work**: Add one integration test for `ATTRIBUTE(conversion: (purchase, subscription), touchpoints: (ad_click, email_open), …)` verifying that both event types in each list trigger/qualify correctly.

---

### A6 — No integration test for `SESSIONIZE | ATTRIBUTE` composition (Low)

**Promise**: `attribute.md §14.1` designates `SESSIONIZE | ATTRIBUTE` as allowed. The design note explicitly calls out `session_id` flowing through demand-driven forwarding when referenced downstream.

**Evidence**: No test in the integration suite or unit tests exercises this composition path end-to-end. The planner correctly handles it (extend_scan_reader_backward recurses through Sessionize), but the full-stack path has no coverage.

**Required work**: Add one integration test that chains `events | SESSIONIZE(gap: …) | ATTRIBUTE(…)` and verifies that (a) the output is non-empty, and (b) `session_id` is forwarded when referenced downstream.

---

### A7 — Joined-source tests `#[ignore]` due to MergeSourcesOperator `__seq_id` nullability bug (High, pre-existing)

**Promise**: `attribute.md §14.3` and `query-language.md §19` describe joined-source queries as valid upstream of ATTRIBUTE.

**Evidence**: Both `joined_source_stats_counts_entities_in_both_tables` and `joined_source_sequence_match_spans_tables` carry `#[ignore = "MergeSourcesOperator: failed to assemble output batch: Invalid argument error: Column '__seq_id' is declared as non-nullable but contains null values"]`. This is an independently filed bug in the `MergeSourcesOperator` output schema contract — not an ATTRIBUTE-specific issue.

**Impact**: All joined-source queries — not just ATTRIBUTE ones — fail with the same `__seq_id` nullability error. The ATTRIBUTE integration test suite documents the failure mode clearly in the file header.

**Required work**: This is tracked as a distinct issue (MergeSourcesOperator output schema drift). Once fixed, un-`#[ignore]` both tests.

---

## Summary

The ATTRIBUTE implementation is **structurally sound and functionally correct** across its most important surfaces: the sliding-window deque, three-way row-shape emission, emit-before-add ordering, window boundary rules, per-entity deque cap with diagnostics, demand-driven column forwarding, and sub-batch streaming all match the design doc. Property test coverage is comprehensive (six distinct invariants, 256 cases each, with a brute-force reference evaluator). Benchmark coverage matches the five workloads called out in §17.1. The parser handles list syntax, duplicate detection, and parameter ordering correctly.

**Spec/implementation divergences:**

| Item | Severity | Blocking? |
|------|----------|-----------|
| A1: `window: 0s` rejected by planner; spec says not rejected | Medium | No (useless case; choose which source to fix) |
| A2: `MATCH \| ATTRIBUTE` not explicitly rejected with `TypeError` — wrong error shape | Medium | No (likely fails; but not with the documented error) |
| A3: EXPLAIN missing `touchpoint_key`, `conversion_range`, forwarded columns | Low | No (display-only; no correctness impact) |
| A4: No integration test for scan-range extension via LAST | Medium | No (unit coverage exists; end-to-end path untested) |
| A5: No integration test for multi-type conversion/touchpoint lists | Low | No |
| A6: No integration test for `SESSIONIZE \| ATTRIBUTE` | Low | No |
| A7: Joined-source tests `#[ignore]` (MergeSourcesOperator bug) | High (pre-existing) | No (tracked separately) |
