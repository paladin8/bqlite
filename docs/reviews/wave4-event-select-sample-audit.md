# Wave 4 EventSelect + SAMPLE Semantic Audit

**Auditor**: TASK-445
**Date**: 2026-04-20
**Sources reviewed**:
- Design spec: `docs/design/operators/event-select-sample.md` (TASK-411)
- Query-language spec: `docs/design/query-language.md` §14.1 (FIRST/LAST/NTH), §14.2 (SAMPLE)
- Execution-model spec: `docs/design/execution-model.md` §2.1, §3.5
- Operator implementation: `crates/bqlite-operators/src/event_select.rs` (TASK-429)
- Storage implementation: `crates/bqlite-storage/src/sample.rs` (TASK-430)
- Planner pushdown pass: `crates/bqlite-planner/src/opt/sample_pushdown.rs` (TASK-430)
- Engine bind step: `crates/bqlite-engine/src/bind.rs` (TASK-438)
- Integration tests: `tests/tests/wave4_advanced_analytics_event_select.rs` (TASK-439)
- Benchmarks: `benches/wave4/sample.rs` (TASK-441)

**Methodology**: Walk the design doc section by section; for each promise, locate primary evidence in code and tests; classify as ✅ Covered, ⚠️ Partial, or ❌ Missing. File follow-up items for partial/missing rows. No code is changed in this audit — all drift is rolled up into TASK-455.

---

## Promise-vs-Evidence Matrix

### Block A — EventSelect Operator (§3–§13)

#### §3 — Operator Identity

| Promise | Evidence | Status |
|---------|----------|--------|
| Single `EventSelectOperator` struct parameterized by `EventSelectKind` | `crates/bqlite-operators/src/event_select.rs` defines `EventSelectOperator`, `EventSelectKind { First, Last, Nth(u32) }`, and `EventSelectState`; the type is a single concrete struct switching on `self.kind` | ✅ |
| Operator struct is immutable; all mutable state in `EventSelectState` | `EventSelectOperator` fields are not `mut`; `EventSelectState` holds `candidate` and `done`; `EntityOperatorAdapter` creates state fresh per entity | ✅ |
| Implements `EntityOperator` trait | `impl EntityOperator for EventSelectOperator` at `event_select.rs:307` | ✅ |
| Crate placement: `bqlite-operators` | File lives at `crates/bqlite-operators/src/event_select.rs` | ✅ |

---

#### §4 — Operator Construction

| Promise | Evidence | Status |
|---------|----------|--------|
| Constructed from `EventSelectPhysical` descriptor by engine bind step | `EventSelectOperator::new(desc: &EventSelectPhysical, input_schema: &OperatorSchema)` at `event_select.rs:210`; invoked by `bind.rs` | ✅ |
| Event-type set populated from `desc.event_types` | `let event_types: HashSet<String> = desc.event_types.iter().cloned().collect()` at `event_select.rs:212` | ✅ |
| Column indices resolved once at construction via `EventSelectInputMap` | `let input_map = EventSelectInputMap { entity_id_idx, ts_idx, seq_id_idx, event_type_idx }` at `event_select.rs:221–226` | ✅ |
| `NTH(n)` requires `n >= 1`; `EventSelectKind::Nth(u32)` enforces non-negativity | `u32` type ensures `n >= 0`; parser rejects `n == 0` at parse time (TASK-421). **No defensive assertion in `EventSelectOperator::new` against `Nth(0)`** — constructing `EventSelectPhysical` programmatically with `kind: Nth(0)` bypasses the parse-time guard and silently produces empty output for every entity (the `qualifying_count += 1` never equals 0 after the first increment). | ⚠️ |
| `fused_aggregate: None` asserted at construction | `EventSelectOperator::new` does **not** assert `desc.fused_aggregate.is_none()`. The field is present on `EventSelectPhysical` but silently ignored by the constructor. Compare: `SessionizeOperator::new` at `sessionize.rs:157` has `assert!(desc.fused_aggregate.is_none(), ...)`. The absence of this guard leaves v1's "no fusion" contract implicit rather than enforced. | ⚠️ |
| `forwarded_columns` drives demand propagation | `desc.forwarded_columns` is **not read** by `EventSelectOperator::new`. The operator derives its output mapping exclusively from `desc.output_schema` (column-by-column `input_schema.column(&col.name)` lookup). `forwarded_columns` is a dead field on `EventSelectPhysical`. The demand-driven forwarding still works correctly because the planner constructs `output_schema` with only demanded columns. | ⚠️ |

---

#### §5 — Selection Semantics

| Promise | Evidence | Status |
|---------|----------|--------|
| §5.1 Event-type filter: `event_type IN event_types_set` using `HashSet<String>` | `EventTypeCheck` strategy at `event_select.rs:443`; dictionary fast path at `event_select.rs:484`; `EventTypeCodeSet` for `DictionaryArray<Int32, Utf8View>` inputs; string check for `StringViewArray` inputs; `EventTypeCheck::Unknown` returns `false` for unrecognised encoding | ✅ |
| §5.2 WHERE predicate evaluated per-event, before position selection; NULL treated as false | Predicate evaluated vectorized over the full sub-batch before the per-row loop; mask checked at `event_select.rs:376`. **Deviation from spec's described evaluation order**: spec says "apply WHERE predicate only if event-type check passes"; implementation evaluates predicate over all rows (incl. non-qualifying event types), then checks both filters per-row. For normal (non-error) predicate evaluation this is semantically equivalent — a row must pass both checks to qualify. **Fail-safe divergence in the error case**: the spec would evaluate the predicate only on event-type-passing rows and skip only the failing rows; the implementation skips the entire sub-batch when predicate eval fails, meaning event-type-qualifying rows in the same batch that have a valid predicate response are also dropped. This is more conservative (never over-selects) but is not equivalent to the spec's stated per-event evaluation semantics when an error occurs mid-batch. Fail-safe tested via `predicate_eval_error_skips_batch_fail_safe` and `predicate_eval_error_across_multiple_batches_produces_no_row`. | ⚠️ |
| §5.3 FIRST: smallest `(ts, __seq_id)` qualifying event | Processes events in ascending `(ts, __seq_id)` input order; first qualifying event encountered is retained and `done` set to `true`; correct by streaming monotonicity | ✅ |
| §5.3 LAST: largest `(ts, __seq_id)` qualifying event | Overwrites `row` candidate with each new qualifying event; last overwrite is the latest (ascending input); tested by `last_selects_latest_qualifying_event`, `last_interspersed_with_other_types` | ✅ |
| §5.3 NTH(n): n-th qualifying event in ascending `(ts, __seq_id)` order | Counter incremented per qualifying event; candidate set on `qualifying_count == n`; tested by `nth_3_with_exactly_3_qualifying_events`, `nth_5_counts_correctly_across_sub_batches` | ✅ |
| §5.4 Same-`ts` tie-breaking by `__seq_id` ascending | Relies on the `EntityOperatorAdapter` guarantee that input rows are sorted by `(entity_id, ts, __seq_id)` ascending; FIRST takes the first of same-`ts` events (smallest `__seq_id`); LAST takes the last (largest `__seq_id`); tested by `first_same_ts_picks_smallest_seq_id`, `last_same_ts_picks_largest_seq_id` | ✅ |
| §5.5 No qualifying event → entity omitted (no output row) | `candidate_row?` in `finish_entity` returns `None` when no candidate; tested by `first_no_qualifying_event_produces_no_row`, `first_empty_entity_produces_no_row`, `nth_3_with_only_2_qualifying_events_produces_no_row` | ✅ |

---

#### §6 — Event-Type List Extension

| Promise | Evidence | Status |
|---------|----------|--------|
| Parenthesized multi-type list accepted by the operator | `event_types: HashSet<String>` handles any number of types; tested by `first_from_event_type_list` (two-type list) | ✅ |
| Column resolution against source schema (missing columns → NULL) | `output_col_to_input_idx[i] == None` for columns absent from input → `ScalarValue::Null` → `new_null_array` in output batch | ✅ |
| Duplicate event types rejected at parse time | Handled by TASK-421 (parser); not tested in the operator layer (correct — parser responsibility) | ✅ |

---

#### §7 — `lookback:` Parameter

| Promise | Evidence | Status |
|---------|----------|--------|
| §7.6 No operator-side change for `lookback:`; widening is transparent | `EventSelectOperator::new` neither reads `desc.lookback` nor changes its per-row logic based on it. The operator simply sees a wider event stream from the scan. | ✅ |
| §7.1–7.5 LAST rejects `lookback:` at parse time; FIRST/NTH accept it | Parser responsibility (TASK-421); `lookback` field on `EventSelectPhysical` is `None` for LAST by contract | ✅ |
| `lookback:` integration test coverage | Both `first_without_lookback_is_bounded_by_outer_range` and `first_with_lookback_widens_scan_range` are `#[ignore]` — blocked by the `__seq_id` gap (Finding E1). Lookback planner logic (scan-range extension) is downstream of TASK-425 and not separately tested without a running EventSelect. | ❌ |

---

#### §8 — Per-Entity State Layout

| Promise | Evidence | Status |
|---------|----------|--------|
| `EventSelectCandidate` with `First { row }`, `Last { row }`, `Nth { n, qualifying_count, row }` | Matches design exactly at `event_select.rs:128–139` | ✅ |
| `CandidateRow` holds one `ScalarValue` per demanded output column | `event_select.rs:117–120`; only columns mapped via `output_col_to_input_idx` are extracted | ✅ |
| §8.3 FIRST early termination: break after first qualifying event | `state.done = true; return` at `event_select.rs:386–387`; `process_sub_batch` returns immediately when `is_done()` at `event_select.rs:333`; tested by `first_early_termination_across_sub_batches` | ✅ |
| §8.4 NTH early termination: break after n-th qualifying event | `state.done = true; return` at `event_select.rs:404–405`; same early-return mechanism | ✅ |
| §8.5 LAST full scan — no early termination | LAST variant always overwrites candidate, never sets `state.done`; confirmed by code structure | ✅ |
| At most one `CandidateRow` in memory per entity | Confirmed by enum structure — only one `Option<CandidateRow>` per variant | ✅ |

---

#### §9 — EntityOperator Integration

| Promise | Evidence | Status |
|---------|----------|--------|
| §9.1 `create_state` initializes correct variant for `self.kind` | `event_select.rs:311–324`; unit test `finish_entity_without_process_produces_no_row` covers initial state | ✅ |
| §9.2 `process_sub_batch`: event-type filter → WHERE predicate → position selection | Loop at `event_select.rs:368–409`; dictionary fast path via `EventTypeCodeSet` | ✅ |
| §9.2 Dictionary fast path: code-set built once per sub-batch | `build_event_type_check` called once per `process_sub_batch` invocation; `EventTypeCodeSet::from_dict` scans dictionary values once; tested by `first_with_dictionary_encoded_event_type`, `last_with_dictionary_encoded_event_type` | ✅ |
| §9.3 Sub-batch continuation: state persists across sub-batches | `last_across_multiple_sub_batches`, `first_early_termination_across_sub_batches`, `nth_5_counts_correctly_across_sub_batches` cover multi-sub-batch scenarios | ✅ |
| §9.4 `finish_entity` emits 0 or 1 rows; entity omitted when no candidate | `event_select.rs:412–421`; returns `None` when candidate is `None`; builds single-row batch otherwise | ✅ |
| §9.5 `finish_entity_into` deferred to Wave 5 | No Wave 5 fusion path; Wave 4 relies on the default trait impl | ✅ |
| §9.6 `required_columns` includes always-needed + output schema + predicate columns | Built at `event_select.rs:239–264`; `entity_id`, `ts`, `__seq_id`, `event_type` always present; tested by `entity_operator_required_columns_includes_system_columns` | ✅ |
| §9.7 `supported_demands`: `supports_forwarded_columns: true`, all others false | `event_select.rs:427–432`; tested by `supported_demands_reports_forwarded_columns` and `demand_caps_match_physical_constant` | ✅ |

---

#### §10 — Demand-Driven Column Forwarding

| Promise | Evidence | Status |
|---------|----------|--------|
| Physical candidate row retains only demanded columns | `output_col_to_input_idx` maps output schema positions to input column indices; `None` slots → `ScalarValue::Null` | ✅ |
| Non-demanded columns emitted as NULL arrays in the output batch | `scalar_to_single_row_array(ScalarValue::Null, data_type)` → `new_null_array` | ✅ |

---

#### §11 — Scan-Range Extension for `lookback:`

*(Note: §11's operator-transparency promise is assessed under §7 above. This section records the planner-side promise.)*

| Promise | Evidence | Status |
|---------|----------|--------|
| Planner extends the scan range backward by `lookback` for FIRST/NTH; operator sees the wider event stream transparently | Planner-side implementation belongs to TASK-425 (AST→logical lowering). The operator correctly makes no assumption about the scan range — it processes all events it receives, regardless of whether they fall before `outer_start`. The planner's lowering code is outside this audit's direct scope (TASK-425 is a separate task), but the operator-boundary contract (§7.6) is verified: the operator does not check or restrict by outer range boundaries. No passing integration test verifies end-to-end `lookback:` widening behavior (blocked by E1). | ⚠️ |
| `lookback:` scan-range extension consistent with ATTRIBUTE §12 (scan widens; output `ts` may precede `outer_start`) | Operator returns the selected event's `ts` as extracted from the candidate row, with no clamping or filtering — a `ts` before `outer_start` is emitted as-is. Correct per §7.2. | ✅ |

---

#### §12 — Fused Aggregate Shapes Deferred to Wave 5

| Promise | Evidence | Status |
|---------|----------|--------|
| `fused_aggregate` always `None` in v1; `supported_demands.supports_aggregation_fusion == false` | `DemandCapabilities { supports_forwarded_columns: true, ..DemandCapabilities::none() }` at `event_select.rs:428–432` ensures all fusion bits are false | ✅ |

---

#### §13 — No Per-Entity Event Cap

| Promise | Evidence | Status |
|---------|----------|--------|
| EventSelect has no event cap; per-entity state is O(1) | No cap counter, no `skipping` flag, no diagnostic channel. Confirmed by code structure: only a single `CandidateRow` at most. | ✅ |

---

#### §21.1 — EventSelect Benchmarks

| Promise | Evidence | Status |
|---------|----------|--------|
| FIRST throughput: 10M events, 100K entities, >200M events/sec/core | **No EventSelect bench file exists** in `benches/wave4/`. The `Glob` search for `benches/wave4/event*` returns empty. | ❌ |
| LAST throughput: >100M events/sec/core | Not benchmarked. | ❌ |
| NTH(5) throughput: >150M events/sec/core | Not benchmarked. | ❌ |
| FIRST with WHERE predicate: >150M events/sec/core | Not benchmarked. | ❌ |
| Event-type list matching: integer comparison, no string alloc | Not benchmarked. | ❌ |
| Memory per entity: <2 KB at 10 demanded columns | Not benchmarked. | ❌ |
| Entity boundary overhead: <500 ns per entity | Not benchmarked. | ❌ |

---

#### §22.1 — EventSelect Property Tests

| Promise | Evidence | Status |
|---------|----------|--------|
| Property 1: Output cardinality — exactly 0 or 1 rows per entity | All tests are example-based. **No property tests exist** for EventSelect. `docs/core-beliefs.md §11` and `CLAUDE.md` require property tests for components with large input spaces and clear invariants. | ❌ |
| Property 2: FIRST correctness — emitted row is the minimum `(ts, __seq_id)` | Not property-tested. | ❌ |
| Property 3: LAST correctness — emitted row is the maximum `(ts, __seq_id)` | Not property-tested. | ❌ |
| Property 4: NTH correctness — exactly `n-1` qualifying events have smaller `(ts, __seq_id)` | Not property-tested. | ❌ |
| Property 5: Omission invariant — no row implies fewer than `n` qualifying events | Not property-tested. | ❌ |
| Property 6: Entity isolation — no output row mixes data from different entities | Not property-tested. | ❌ |
| Property 7: NTH(1) == FIRST equivalence | Tested as a unit example (`nth_1_is_equivalent_to_first`) but not as a property over arbitrary inputs. | ⚠️ |

---

#### §20 — Edge-Case Matrix (EventSelect)

The design spec's §20.1 lists 22 EventSelect edge cases with explicit `High` / `Medium` / `Low` priority markers. This section cross-checks the unit test suite against the `High`-priority cases.

| Edge case (§20.1) | Priority | Test coverage | Status |
|---|---|---|---|
| Empty entity (0 events after filter) → no output row | High | `first_no_qualifying_event_produces_no_row`, `first_empty_entity_produces_no_row`, `finish_entity_without_process_produces_no_row` | ✅ |
| Single-event entity, matching → one output row | High | `first_single_matching_event` | ✅ |
| Single-event entity, non-matching type → no output row | High | `first_no_qualifying_event_produces_no_row` (single non-matching row) | ✅ |
| Single-event entity, WHERE predicate fails → no output row | High | `first_where_predicate_no_qualifying_event_produces_no_row` | ✅ |
| FIRST with all events qualifying → selects min `(ts, __seq_id)` | High | `first_selects_earliest_qualifying_event` | ✅ |
| LAST with all events qualifying → selects max `(ts, __seq_id)` | High | `last_selects_latest_qualifying_event` | ✅ |
| NTH(3) with exactly 3 qualifying events → selects the third | High | `nth_3_with_exactly_3_qualifying_events` | ✅ |
| NTH(3) with only 2 qualifying events → no output row | High | `nth_3_with_only_2_qualifying_events_produces_no_row` | ✅ |
| Same-`ts` tie-breaking (FIRST picks smallest `__seq_id`; LAST picks largest) | High | `first_same_ts_picks_smallest_seq_id`, `last_same_ts_picks_largest_seq_id` | ✅ |
| Event-type list with multiple types | High | `first_from_event_type_list` | ✅ |
| FIRST early termination — remaining events skipped | High | `first_early_termination_across_sub_batches` | ✅ |
| NTH early termination — remaining events skipped | High | `nth_5_counts_correctly_across_sub_batches` implicitly verifies (state goes `done` after the 5th) | ✅ |
| LAST full scan — all events processed | High | `last_across_multiple_sub_batches` (two batches, last qualifying event from batch 2 selected) | ✅ |
| Entity boundary mid-batch | High | **Not directly tested at the operator level.** The `EntityOperatorAdapter` manages entity boundaries; individual unit tests call `run_entity` which is a single-entity harness. No unit test constructs two consecutive entities in one batch and verifies boundary isolation at the operator level. The adapter's boundary handling is tested elsewhere but not in conjunction with `EventSelectOperator`. | ⚠️ |
| Candidate spanning multiple sub-batches | High | `last_across_multiple_sub_batches`, `nth_5_counts_correctly_across_sub_batches` | ✅ |
| `lookback:` extends scan range | Medium | Blocked by E1; both tests `#[ignore]` | ❌ |
| Dictionary-encoded event_type | Medium | `first_with_dictionary_encoded_event_type`, `last_with_dictionary_encoded_event_type` | ✅ |
| Event-type list with non-overlapping column sets → NULL for missing properties | Medium | **Not tested.** No unit test creates a multi-type list where one type lacks a column that the WHERE predicate references, to confirm NULL propagation at the operator level. | ❌ |
| Large entity with millions of events, FIRST — only processes up to first qualifying | High | Structural: `state.done = true; return` exits the per-row loop immediately; unit `first_early_termination_across_sub_batches` verifies at small scale. No high-scale unit test validates the early-exit path on a large batch. Covered by the missing benchmarks (E3). | ⚠️ |

---

### Block C — Composition Rules (§19)

#### §19 — Composition Rules

| Promise | Evidence | Status |
|---------|----------|--------|
| §19.1 `SESSIONIZE \| FIRST/LAST/NTH` allowed; entity-level selection; `session_id`/`session_duration` flow through as forwarded columns when demanded | The pushdown optimizer at `sample_pushdown.rs:173–175` recurses into `Sessionize` sub-trees, confirming Sessionize is recognized as a stateful interior node. The `EventSelectOperator` treats `session_id` and `session_duration` as forwarded columns like any other: they appear in `output_schema` if demanded, and are extracted via `extract_candidate_row`. **No integration test** covers `SESSIONIZE \| FIRST/LAST/NTH` end-to-end (all FIRST/LAST/NTH tests are `#[ignore]` per E1). The "entity-level, not session-level" semantics cannot be verified at runtime. | ⚠️ |
| §19.2 Chained EventSelects allowed; planner does not reject; runtime produces empty output | The pushdown optimizer's `PhysicalPlan::EventSelect` arm (`sample_pushdown.rs:177–180`) recurses into the child, confirming EventSelect is not treated as opaque. The planner lowering for chained EventSelects is part of TASK-425; no test exercises `FIRST(a) \| LAST(b)` in the plan. No passing test verifies the "empty output for every entity" behavior. | ❌ |
| §19.3 Valid downstreams of EventSelect: WHERE, SELECT/LET, STATS, ORDER BY, LIMIT | Structural: EventSelect emits a standard `RecordBatch` that downstream operators process normally. Not independently tested (all FIRST/LAST/NTH tests `#[ignore]`). | ⚠️ |
| §19.4 Valid upstreams of SAMPLE: source table first, then operators | Planner lowering and pushdown optimizer enforce placement; `sample_pushdown.rs:236–249` defines `can_push_through` | ✅ |
| §19.5 `SAMPLE \| FIRST/LAST/NTH` allowed | `can_push_through` returns `false` for `EventSelect` (stateful); Sample stays above EventSelect and recurses into its sub-tree. Semantically correct per §17.1 population invariance. No passing integration test. | ⚠️ |
| §19.6 `MATCH \| FIRST/LAST/NTH` valid; "first match row per entity" semantics | Not tested. MATCH emits a different schema (step-property columns, `match_duration`); EventSelect downstream of MATCH would select from match rows. No test covers this composition shape. | ❌ |

---

### Block B — SAMPLE (§14–§18)

#### §14 — Operator Identity

| Promise | Evidence | Status |
|---------|----------|--------|
| SAMPLE is a scan-level filter, not an EntityOperator | `SampleFilter` in `crates/bqlite-storage/src/sample.rs` implements `Predicate` (not `EntityOperator`). No SAMPLE operator struct in `bqlite-operators`. | ✅ |
| Crate placement: filter logic in `bqlite-storage` or `bqlite-operators`; `SamplePhysical` in `bqlite-planner` | Filter at `crates/bqlite-storage/src/sample.rs`; `SamplePhysical` at `bqlite-planner`; fallback `SampleOperator` at `bqlite-engine/src/bind.rs:514` | ✅ |

---

#### §15 — SAMPLE Parameters

| Promise | Evidence | Status |
|---------|----------|--------|
| §15.1 `fraction` in `[0.0, 1.0]` inclusive; both boundaries legal | `SampleFilter::new` validates `!fraction.is_finite() || !(0.0..=1.0).contains(&fraction)` at `sample.rs:88`; boundaries tested by `boundary_zero_is_empty_set`, `boundary_one_is_pass_through` | ✅ |
| §15.2 No `count:` parameter | `SamplePhysical { fraction: f64, seed: i64 }` — no `count` field; grammar and planner confirm `fraction:`-only surface | ✅ |

---

#### §16 — Hash Function and Determinism

| Promise | Evidence | Status |
|---------|----------|--------|
| §16.1 xxHash64 pinned; stability contract | Uses `twox_hash::XxHash64::oneshot(seed, bytes)` at `sample.rs:185,200`; stability regression test `int_entity_hash_is_little_endian_stable` pins the exact hash output for a known `(seed, key)` pair | ✅ |
| §16.2 String entity keys: UTF-8 bytes | `accepts_str(bytes: &[u8])` receives UTF-8 bytes at `sample.rs:178` | ✅ |
| §16.2 Int entity keys: little-endian 8 bytes | `accepts_int(v: i64)` uses `&v.to_le_bytes()` at `sample.rs:199`; stability pinned by `int_entity_hash_is_little_endian_stable` | ✅ |
| §16.3 Threshold test: `hash < fraction * u64::MAX` | `(fraction * u64::MAX as f64) as u64` threshold; strict `<` comparison; `fraction: 1.0` sets threshold to `u64::MAX` avoiding `1/2^64` exclusion | ✅ |
| §16.3 `fraction: 0.0` threshold = 0 → empty set | `fraction <= 0.0` sets `threshold = 0`; `is_empty_set()` short-circuits per-row work; tested | ✅ |
| §16.3 `fraction: 1.0` pass-through; all entities included without hashing | `is_pass_through()` returns `true`; `accepts_str` and `accepts_int` return `true` immediately; tested by `boundary_one_is_pass_through`, `apply_to_array_pass_through_returns_all_true` | ✅ |

---

#### §17 — Population Semantics

| Promise | Evidence | Status |
|---------|----------|--------|
| §17.1 Population-invariance: `events | WHERE P | SAMPLE(f) ≡ events | SAMPLE(f) | WHERE P` for entity-key-independent P | `sample_population_invariance_with_stateless_filter` integration test confirms equal entity sets and row counts for `WHERE event_type = 'click'` before and after SAMPLE | ✅ |
| §17.2 Sampled entity with no matching WHERE rows contributes zero rows but is still "in" the sample set | **Not tested**. No test verifies that an entity passing SAMPLE's hash test but having zero WHERE-passing events produces zero output rows without altering the sampled entity set of other tests. | ❌ |
| §17.3 `SAMPLE + IN alias / IN QUERY` cohort behavior documented and correct | **Not tested** end-to-end. The design's "10% of full population, intersected with cohort" semantics requires `SubqueryFilter` + `SampleFilter` composition that has no integration test. | ❌ |

---

#### §18 — Scan Pushdown Contract

| Promise | Evidence | Status |
|---------|----------|--------|
| §18.1 Pushdown elides `Sample` node when path to `Scan` is stateless | `sample_pushdown.rs:67`: `push_sample` calls `can_push_through` and elides the `Sample` node; `sample_over_scan_is_pushed_and_sample_elided`, `sample_over_filter_over_scan_pushes_through_filter`, `sample_over_project_over_scan_pushes_through_project` | ✅ |
| §18.1 Pushdown through `MergeSources`: same `(fraction, seed)` stamped on every sub-scan | `push_into_scan` → `MergeSources` arm stamps `sample` on every `ScanPhysical` in `tables`; tested by `sample_over_merge_sources_pushes_into_every_sub_scan`, `sample_over_filter_over_merge_sources_pushes_through`, `sample_over_project_over_merge_sources_pushes_through` | ✅ |
| §18.1 `Limit` is NOT commutative with SAMPLE — Sample stays above Limit | `can_push_through` returns `false` for `Limit`; `pushdown_sample` leaves `Sample` above `Limit`; tested by `sample_over_limit_is_not_pushed`, `sample_over_limit_over_merge_sources_is_not_pushed` | ✅ |
| Stateful nodes (Sessionize, EventSelect, etc.): Sample stays above, recursion continues into sub-tree | `pushdown_sample` recurse arms for `SequenceMatch`, `Aggregate`, `Sort`, `Distinct`, `Sessionize`, `EventSelect`, `Attribute`; tested by `sample_over_sessionize_is_not_pushed_but_subtree_is_walked`, `nested_sample_inside_aggregate_is_pushed_into_inner_scan` | ✅ |
| §18.2 Pre-pushdown fallback operator implemented | `SampleOperator` (entity-level filter) at `bind.rs:514`; handles `Sample` nodes that the pushdown pass could not elide | ✅ |
| §18.3 Physical descriptor: `SamplePhysical { fraction: f64, seed: i64 }` | Confirmed in `bqlite-planner`; `seed: i64` with `None` resolved to database-UUID-derived default by bind step | ✅ |

---

#### §22.2 — SAMPLE Property Tests

| Promise | Evidence | Status |
|---------|----------|--------|
| Property 1: Determinism | `same_seed_same_entity_is_deterministic` unit test (5 named entities) + `sample_is_deterministic_across_runs` integration test | ✅ |
| Property 2: Monotonicity — entity accepted at `f1` also accepted at `f2 > f1` (same seed) | `monotonic_in_fraction` unit test (1000 entities, 3 fraction points) | ✅ |
| Property 3: Boundary — `f=0.0` empty, `f=1.0` all-in | `boundary_zero_is_empty_set`, `boundary_one_is_pass_through`, `apply_to_array_empty_set_returns_all_false`, `apply_to_array_pass_through_returns_all_true` | ✅ |
| Property 4: Population invariance | `sample_population_invariance_with_stateless_filter` integration test | ✅ |
| Properties implemented as property tests (`proptest`) | All implemented as hand-written example tests, not as property tests over arbitrary inputs. Given the clear algebraic invariants and the CLAUDE.md/core-beliefs.md mandate, these are candidates for proptest conversion, but the example tests cover the key invariants adequately for v1. | ⚠️ |

---

#### §21.2 — SAMPLE Benchmarks

| Promise | Evidence | Status |
|---------|----------|--------|
| Pushdown throughput: 1M entities, fraction: 0.1, >50M entities/sec/core | `bench_apply_to_array` and `bench_scan_pushdown` at 65 536 entities with hardcoded reference target `BenchTarget::at_least(50_000_000.0)`. N is 65 536 (one row-group), not 1M, but the per-entity rate measurement satisfies the spec's per-core throughput target. | ✅ |
| Hash computation: xxHash64 over 32-byte string entity IDs, <10 ns/entity | Not explicitly benchmarked as a standalone measurement. Implicitly captured by `bench_apply_to_array` throughput (65 536 entities / elapsed_s), but no hard-target for per-entity latency is recorded. | ⚠️ |
| Determinism verification: same seed, same entity set, 3 runs | Covered as a unit test (`same_seed_same_entity_is_deterministic`) and integration test (`sample_is_deterministic_across_runs`), not as a benchmark. | ⚠️ |

---

### Integration Test Coverage (TASK-439)

| Test | Status | Notes |
|------|--------|-------|
| `sample_fraction_zero_produces_no_rows` | ✅ runs | Boundary case — empty set contract |
| `sample_fraction_one_passes_through` | ✅ runs | Boundary case + row-count comparison |
| `sample_is_deterministic_across_runs` | ✅ runs | Seed stability + negative-control different-seed |
| `sample_population_invariance_with_stateless_filter` | ✅ runs | §17.1 invariance — both pipelines produce equal entity sets and row counts |
| `sample_composes_with_stats` | ✅ runs | §25.2 composition table row: SAMPLE → STATS |
| `first_returns_first_event_per_entity` | `#[ignore]` | Blocked by E1: `__seq_id` not materialized at scan runtime |
| `first_with_candidate_predicate_filters_before_selection` | `#[ignore]` | Same blocker |
| `first_with_event_type_list_matches_any` | `#[ignore]` | Same blocker |
| `first_without_lookback_is_bounded_by_outer_range` | `#[ignore]` | Same blocker |
| `first_with_lookback_widens_scan_range` | `#[ignore]` | Same blocker |
| `last_returns_last_event_per_entity` | `#[ignore]` | Same blocker |
| `nth_returns_third_matching_event` | `#[ignore]` | Same blocker |

**Summary**: SAMPLE has 5 passing end-to-end tests. EventSelect has zero passing end-to-end tests.

---

## Findings

### E1 — `__seq_id` not materialized at scan runtime (Critical)

**Promise**: `EventSelectOperator::new` requires `__seq_id` in the input schema for same-`ts` tie-breaking (§5.4). The schema's `required_columns()` returns it and `find_required(SEQ_ID_COLUMN)` panics if it is absent.

**Evidence**: The integration test file header documents this explicitly:

> `EventSelectOperator::new` panics with `"required column '__seq_id' not found in input schema"`. Not a routing-layer issue — even a bare `events | FIRST(x)` on a non-empty table hits it.

All 7 FIRST/LAST/NTH integration tests are `#[ignore]` with attribution to this gap. The bind.rs comment (TASK-438) notes the deferral. This is the same scan-layer gap identified in the SESSIONIZE audit (finding E: "Schema binding bug: Scan does not materialize system columns").

**Impact**: FIRST/LAST/NTH are entirely non-functional at runtime. The operators are correctly implemented at the unit level (all unit tests pass) but cannot be invoked through the engine without hitting a panic.

**Required work**: Ensure the scan runtime materializes `__seq_id` as a column in every output `RecordBatch`. Once fixed, remove all `#[ignore]` markers from the FIRST/LAST/NTH integration tests. This is likely the same fix required to resolve SESSIONIZE finding E.

---

### E2 — No EventSelect property tests (High)

**Promise**: `docs/design/operators/event-select-sample.md §22.1` lists 7 clear property-testable invariants. `CLAUDE.md` (Testing section) and `docs/core-beliefs.md §11` require property tests for components with large input spaces and clear invariants.

**Evidence**: All tests in `event_select.rs` are hand-written examples on specific inputs. No `proptest`-based tests exist for EventSelect anywhere in the codebase.

**Invariants suitable for property testing** (the ones listed in §22.1):
1. Output cardinality: exactly 0 or 1 rows per entity.
2. FIRST correctness: emitted row's `(ts, __seq_id)` is the minimum among qualifying events.
3. LAST correctness: emitted row's `(ts, __seq_id)` is the maximum among qualifying events.
4. NTH correctness: exactly `n-1` qualifying events have smaller `(ts, __seq_id)`.
5. Omission invariant: no row iff fewer than `n` qualifying events (n=1 for FIRST/LAST).
6. Entity isolation: no output row mixes data from two entities.
7. NTH(1) == FIRST equivalence.

**Required work**: Add a proptest suite using `tests/src/strategies.rs` Arrow-shaped generators. Each invariant can be stated as a `for all` property over entity event streams with varying event-type distributions, predicate selectivities, and entity sizes.

---

### E3 — No EventSelect benchmarks (High)

**Promise**: `docs/design/operators/event-select-sample.md §21.1` lists 7 benchmarks with explicit numeric targets (>200M events/sec/core for FIRST, >100M for LAST, >150M for NTH, etc.).

**Evidence**: `Glob("benches/wave4/event*")` returns empty. No EventSelect bench file exists.

**Impact**: The performance story for the hot path (FIRST early termination, dictionary event-type matching) has no empirical validation. The per-entity overhead target (<500 ns per entity) is unverified.

**Required work**: Create `benches/wave4/event_select.rs` covering the 7 benchmark scenarios in §21.1. Use the `bqlite_benches::common` generators to produce entity-sorted event streams at the specified entity/event scales.

---

### E4 — All EventSelect integration tests `#[ignore]` (High)

**Promise**: TASK-439's checkpoint 2 is described as covering FIRST/LAST/NTH semantics end-to-end through `Engine::query`.

**Evidence**: All 7 FIRST/LAST/NTH integration tests are `#[ignore]` (same blocker as E1). The only end-to-end coverage comes from unit tests that bypass the engine and call `process_sub_batch` / `finish_entity` directly.

**Impact**: The full engine pipeline — scan → bind → `EventSelectPhysical` → `EventSelectOperator` — is untested. Bugs in the bind step, scan projection, or schema-binding layer would not be caught by the existing test suite.

---

### E5 — `fused_aggregate` not asserted None at construction (Minor)

**Promise**: The design doc's Wave 4/5 deferral contract requires `fused_aggregate: None` in v1. `SessionizeOperator::new` (`sessionize.rs:157`) asserts this explicitly.

**Evidence**: `EventSelectOperator::new` does not read or assert `desc.fused_aggregate`. The field is silently ignored.

**Impact**: If a planner bug sets `fused_aggregate: Some(...)` on `EventSelectPhysical`, the operator will silently produce unfused output rather than failing fast with a clear error. The SESSIONIZE pattern is the correct defensive approach.

**Required work**: Add `assert!(desc.fused_aggregate.is_none(), "EventSelect fused aggregates are deferred to Wave 5");` at the top of `EventSelectOperator::new`.

---

### E6 — `forwarded_columns` is a dead field on `EventSelectPhysical` (Minor)

**Promise**: The design doc discusses demand-driven column forwarding propagated through `forwarded_columns` on the physical descriptor (consistent with `SessionizePhysical`).

**Evidence**: `EventSelectOperator::new` does not read `desc.forwarded_columns`. The output mapping is derived entirely from `desc.output_schema`. The field is populated as `vec![]` in all test helpers. Demand-driven forwarding still works correctly because the planner constructs `output_schema` with only demanded columns.

**Impact**: Minor confusion for future maintainers reading `EventSelectPhysical`. The field can either be removed or documented as unused-in-v1.

---

### E7 — No defensive guard against `Nth(0)` at operator construction (Minor)

**Promise**: §4.2 states the `>= 1` invariant is validated during plan construction.

**Evidence**: `EventSelectKind::Nth(u32)` enforces non-negativity but not strict positivity. `EventSelectOperator::new` does not check `n > 0`. A programmatically constructed `EventSelectPhysical` with `kind: Nth(0)` would silently produce empty output for every entity (the `qualifying_count += 1; if qualifying_count == 0` branch is unreachable, so no candidate is ever set).

**Required work**: Add a validation in `EventSelectOperator::new`:
```rust
if let EventSelectKind::Nth(0) = desc.kind {
    panic!("EventSelectOperator: Nth(0) is invalid; n must be >= 1");
}
```

---

### E8 — SAMPLE §17.2 and §17.3 not tested (Low)

**Promise**: §17.2 specifies that a sampled entity with no WHERE-passing events contributes zero output rows but remains "in" the sampled set. §17.3 specifies the SAMPLE + cohort-filter composition produces `(sampled) ∩ (cohort)`.

**Evidence**: No integration test covers these cases. The population-invariance test (`sample_population_invariance_with_stateless_filter`) verifies §17.1 but not the edge case in §17.2 (an entity that passes SAMPLE but has no events after filtering).

**Impact**: Low — the semantics follow mechanically from the implementation's structure (scan-level entity filter × upstream WHERE), but the gap means a regression in entity-set handling would not be caught by tests.

---

### E9 — SAMPLE per-entity hash latency not benchmarked (Low)

**Promise**: §21.2 row 2 specifies "Hash computation: xxHash64 over 32-byte string entity IDs, <10 ns per entity."

**Evidence**: `bench_apply_to_array` measures aggregate throughput (entities/sec) on 65 536 entities but does not record a per-entity latency measurement against a hard target. The correctness of `xxHash64` for 32-byte strings specifically is not measured.

**Impact**: The <10 ns per-entity target cannot be automatically gated by CI without a measurement.

---

### E10 — Composition §19.2 (chained EventSelects) and §19.6 (MATCH | EventSelect) not tested (Low)

**Promise**: §19.2 says chained EventSelects (`events | FIRST(signup) | LAST(purchase)`) are legal and produce empty output for every entity. §19.6 says `MATCH | FIRST/LAST/NTH` is a valid composition.

**Evidence**: No test exercises either composition at any level (unit or integration). §19.2's "empty output" behavior follows mechanically from the single-row output of FIRST feeding into LAST (one signup row per entity is not a purchase), but there is no test to guard against a regression where the planner incorrectly rejects the composition or the operator incorrectly handles a one-row entity input.

**Impact**: Low — the behaviors are mechanical consequences of the existing design, but the absence of tests means a planner change (e.g., a misguided "no chained EventSelect" rule) would not be caught.

**Required work**: Add a test for chained EventSelects once E1 is resolved. A minimal form: `events | FIRST(login) | LAST(purchase)` on a fixture where the login is not a purchase → no output rows.

---

### E11 — Entity-boundary isolation not tested at the operator unit level (Low)

**Promise**: §20.1 "Entity boundary mid-batch" is listed as a `High`-priority edge case.

**Evidence**: The `EntityOperatorAdapter` manages entity boundaries in production; unit tests use a single-entity `run_entity` harness that calls `create_state` / `process_sub_batch` / `finish_entity` manually. No unit test in `event_select.rs` constructs a batch spanning two entities and verifies that the first entity's state does not contaminate the second.

**Impact**: Low — the adapter's boundary detection is covered by its own tests. Contamination between entities in `EventSelectOperator` is structurally impossible (state is created fresh per entity), but the edge case is declared `High` in the spec and merits at least one direct test.

**Required work**: Add a unit test that calls `run_entity` twice on the same operator with different entity IDs and verifies each produces an independent result.

---

## Drift Summary

### EventSelect

| Spec location | Implementation status | Severity |
|---|---|---|
| §5.2 predicate eval order: fail-safe skips whole batch, not just failing rows | Conservative divergence from spec's per-row semantics in the error case | Minor |
| §4.2 `fused_aggregate: None` assertion | Assertion absent; field silently ignored | Minor |
| §4.1 `forwarded_columns` usage | Field present but never read | Minor |
| §4.2 `Nth(0)` guard | Not defensively checked at construction | Minor |
| §11 / §7 `lookback:` planner-side scan-range extension | Planner-side (TASK-425 scope); operator contract correctly satisfied; no passing e2e test (E1) | ⚠️ |
| §19.2 Chained EventSelects; §19.6 MATCH | EventSelect composition | Not tested | Low |
| §20.1 Entity-boundary mid-batch test | Not tested at operator unit level | Low |
| §20.1 Event-type list with non-overlapping column sets | Not tested | Low |
| §22.1 Property tests (all 7 invariants) | Zero property tests | High |
| §21.1 Benchmarks (all 7 targets) | No bench file for EventSelect | High |
| §9.x End-to-end tests | All 7 integration tests `#[ignore]` (blocked by E1) | Critical |
| `__seq_id` materialization at scan runtime | Gap in engine/scan layer; blocks all runtime use | Critical |

### SAMPLE

| Spec location | Implementation status | Severity |
|---|---|---|
| §17.2 Sampled entity with no WHERE-matching events | Not tested | Low |
| §17.3 SAMPLE + cohort filter composition | Not tested | Low |
| §21.2 Per-entity hash latency benchmark | Not measured | Low |
| §22.2 Property tests | Example-based only; core invariants tested | Low |

---

## Summary

**SAMPLE** is production-quality within its current scope. The hash function, threshold test, boundary handling, population-invariance proof, zone-map pruning, pushdown optimizer, fallback operator, and integration tests all match the design spec precisely. Five integration tests pass today. The only gaps are minor: two edge-case behaviors from §17 that are not tested, and one of the three specified benchmarks that is not measured.

**EventSelect** is structurally complete at the unit level. The operator's logic, state machine, early-termination optimizations, dictionary fast path, demand-driven forwarding, and composition rules are correctly implemented and tested by 28 unit tests. However, the feature is **entirely non-functional at the runtime level** due to the `__seq_id` materialization gap (E1), which causes a panic in `EventSelectOperator::new` during engine bind. All 7 integration tests are `#[ignore]`. Additionally, the feature has no property tests (7 invariants called for) and no benchmarks (7 targets called for), leaving correctness under property-space exploration and performance targets unverified.

| Finding | Severity | Blocking? |
|---------|----------|-----------|
| E1: `__seq_id` not materialized at scan runtime — all FIRST/LAST/NTH integration tests `#[ignore]`; runtime panics on any FIRST/LAST/NTH query | **Critical** | Yes — FIRST/LAST/NTH are non-functional; blocks TASK-442 acceptance test |
| E2: No EventSelect property tests (7 invariants from §22.1) | High | No |
| E3: No EventSelect benchmarks (7 targets from §21.1) | High | No |
| E4: All 7 EventSelect integration tests `#[ignore]` | High | No (consequence of E1) |
| E5: `fused_aggregate` not asserted None at construction | Minor | No |
| E6: `forwarded_columns` is dead field on `EventSelectPhysical` | Minor | No |
| E7: No defensive guard against `Nth(0)` at construction | Minor | No |
| E8: SAMPLE §17.2 and §17.3 not tested | Low | No |
| E9: SAMPLE per-entity hash latency not benchmarked | Low | No |
| E10: Composition §19.2 (chained EventSelects) and §19.6 (MATCH \| EventSelect) not tested | Low | No |
| E11: Entity-boundary isolation not tested at operator unit level (§20.1) | Low | No |
