# Wave 4 Cohort, Alias, and Joined-Source Semantic Audit

**Auditor**: TASK-447
**Date**: 2026-04-20
**Sources reviewed**:
- Design spec: `docs/design/language/cohorts-aliases-joins.md` §2 (Block A — aliases/cohorts), §3 (Block B — entity-aligned source JOIN), §4 (Block C — SubqueryFilter execution)
- Language spec: `docs/design/query-language.md` §17 (IN set membership), §18 (Aliases), §19 (Cross-table entity joins)
- Planner: `crates/bqlite-planner/src/logical.rs` (`AliasTable`, `resolve_alias`, `apply_subquery_filter`, `build_joined_scan`, `lower_query_pipeline`)
- Planner: `crates/bqlite-planner/src/physical.rs` (`SubqueryFilterPhysical`, `MergeSourcesPhysical`)
- Runtime — cohort: `crates/bqlite-operators/src/cohort.rs` (`CohortHashSet`, `SubqueryFilterOperator`)
- Runtime — join: `crates/bqlite-operators/src/scan.rs` (`MergeSourcesOperator`, `build_output_batch`)
- Engine bind: `crates/bqlite-engine/src/bind.rs` (`CohortCache`, `bind_subquery_filter`, `bind_merge_sources`)
- Parser — aliases: `crates/bqlite-parser/src/parser.rs`, `crates/bqlite-ast/src/expr.rs`
- Parser — JOIN: `crates/bqlite-parser/src/parser.rs`, `crates/bqlite-parser/src/error.rs`
- Parser — DELETE+JOIN: `crates/bqlite-parser/src/dml.rs`
- Integration tests: `tests/tests/wave4_advanced_analytics_attribute_cohort_join.rs`
- Acceptance test: `tests/tests/wave4_acceptance.rs`
- Benchmarks: `benches/wave4/cohort_join.rs`

**Methodology**: Walk each design-doc promise for cohort/alias (Block A), entity-aligned source JOIN (Block B), and SubqueryFilter execution (Block C); locate primary evidence in code and tests; classify each as ✅ Covered, ⚠️ Partial, or ❌ Missing. Follow-up items for partial/missing rows are filed at the end. Nothing is fixed here — drift and missing coverage are rolled up into TASK-455.

---

## Promise-vs-Evidence Matrix

### Block A — Aliases and Cohorts (Language)

#### §2.2 — A1: Alias Shadowing Rules

| Promise | Evidence | Status |
|---------|----------|--------|
| Aliases may not shadow reserved keywords | Parser emits `ParseError::ReservedKeyword` on 2-token lookahead `kw "="` (`parser.rs`) | ✅ |
| Aliases may not shadow table names | No catalog-lookup check exists in `lower_statements` or `lower_query_pipeline`; `AliasNameCollision` error type is absent from `bqlite-core/src/error.rs`; `push_definition` (`logical.rs:1076–1082`) unconditionally inserts without consulting the catalog | ❌ |
| Shadowing other aliases permitted (last-wins) | `AliasTable.definitions.insert(name, ...)` unconditionally overwrites on duplicate (`logical.rs:1081`); tested by `alias_last_wins_on_duplicate_definition` | ✅ |
| Event-type names and column names NOT forbidden as alias names (A1 rationale) | No restriction in parser or planner; design doc explains event types are runtime values, not grammar tokens | ✅ (by design) |

---

#### §2.3 — A2: Forward References Forbidden

| Promise | Evidence | Status |
|---------|----------|--------|
| Alias must be defined before it is referenced (top-down source order) | `resolve_alias` at `logical.rs:1596–1625` checks position-based order; error: `BqliteError::Plan("alias '...' is referenced from '...' but defined later")` | ✅ |
| Forward reference produces bind-time error with span-accurate diagnostic | Error produced at bind time; span from `AliasTable.order` preserves source order; tested by `alias_forward_reference_rejected` | ✅ |

---

#### §2.4 — A3: Submission Boundary

| Promise | Evidence | Status |
|---------|----------|--------|
| Engine `execute` call scopes aliases to that single submission | `lower_statements` creates a fresh `AliasTable` per call; engine holds no alias state across calls (`logical.rs:1111–1142`) | ✅ |
| Cross-submission alias persistence is a CLI-layer concern only | Consistent with module structure; no session alias state in engine or planner | ✅ (by design) |

---

#### §2.5 — A4: Per-Submission Alias Caching

| Promise | Evidence | Status |
|---------|----------|--------|
| Alias executed exactly once per submission even when referenced multiple times | `AliasTable.resolved` cache checked before lowering (`logical.rs:1647–1666`); engine-layer `CohortCache` deduplicates by structural `PhysicalPlan` equality (`bind.rs:945–961`) | ✅ |
| `Arc<CohortHashSet>` is shared across references — not duplicated | `cohort_cache_get_returns_arc_for_equal_plan` test (`bind.rs:2104–2138`) asserts `Arc::ptr_eq` between two gets of structurally equal plans | ✅ |

---

#### §2.6 — A5: No Cross-Submission Caching

| Promise | Evidence | Status |
|---------|----------|--------|
| Cohort materializations dropped when `execute` returns | `CohortCache` is created inside `bind_physical` (stack allocation) and dropped at call end | ✅ |

---

#### §2.7 — A6: Cohort Size Accounting

| Promise | Evidence | Status |
|---------|----------|--------|
| No dedicated cohort size cap in v1; memory budget layer (TASK-501) governs | No cohort-specific limit; correct for v1 scope | ✅ (by design) |
| Cohort exceeding memory budget errors the whole query (no silent truncation) | TASK-501 not implemented; no current out-of-budget path for cohort | ⚠️ |
| `query-language.md §17/§18` must document the memory-budget caveat (design §7.1 update requirement) | §17 and §18 do not mention memory budget or out-of-budget error for cohort materializations | ❌ |

---

#### §2.8 — A7: Alias Cycle Detection

| Promise | Evidence | Status |
|---------|----------|--------|
| Cycle detection at bind time via DFS stack check | `resolve_alias` maintains active path stack; cycle re-entry emits `BqliteError::Plan("alias cycle detected: a -> b -> a")` (`logical.rs:1635–1645`) | ✅ |
| Self-reference detected with dedicated diagnostic | Early `if current_name == name` check (`logical.rs:1610–1614`): `"alias '...' cannot reference itself — self-references are a degenerate cycle"`; tested by `alias_self_reference_reports_dedicated_error` | ✅ |
| Error type promised: `TypeError::AliasCycle { path: Vec<String> }` | Implementation uses `BqliteError::Plan(String)` with the path as a formatted string. No structured `AliasCycle` variant exists in `bqlite-core`; the path vec is not machine-readable | ⚠️ |

---

#### §2.9 — A8: Alias Result Shape Checked at Use Site

| Promise | Evidence | Status |
|---------|----------|--------|
| Shape check deferred to use site; definition time is shape-agnostic | `apply_subquery_filter` validates arity and types at binding (`logical.rs:1681–1731`); definition has no shape requirement | ✅ |

---

#### §2.10 — A9: Multi-Column IN Positional Binding

| Promise | Evidence | Status |
|---------|----------|--------|
| LHS tuple binds to subquery output positionally (column names ignored) | `apply_subquery_filter` type-checks columns in declaration order (`logical.rs:1714–1723`); `SubqueryFilterOperator.probe_batch` evaluates LHS expressions in order and constructs `CohortKey` positionally (`cohort.rs:370–392`) | ✅ |
| `query-language.md §17.4` documents positional rule with example showing differing LHS/subquery column names | §17.4 explicitly states: "Column names in the subquery output are ignored for matching purposes — only position and type matter" with a worked example where `user_id` binds to `entity_id` positionally | ✅ |
| `type-system.md §6.9` documents A8 and A9 | Not verified in this audit (type-system.md is out of this task's primary scope); filed as J5 | ⚠️ |
| Tuple positional tests | `two_column_tuple_probe_matches_positionally` (`cohort.rs:682`) covers the 2-column case; `multi_column_in_query_filters_by_tuple` end-to-end test passes | ✅ |

---

#### §2.11 — A10: `IN alias` vs `IN QUERY` Equivalence

| Promise | Evidence | Status |
|---------|----------|--------|
| Both forms normalize to the same internal `SubqueryFilter` representation | Parser produces `Expr::In { lhs, rhs: InRhs::Alias(name) }` and `Expr::In { lhs, rhs: InRhs::Subquery(pipeline) }`; planner lowers both through the same `apply_subquery_filter` path | ✅ |
| Caching applies to both: identical inner plans share one materialization | `CohortCache` keys on `PhysicalPlan` structural equality; alias and equivalent inline subquery produce the same plan key | ✅ |
| `cohort_alias_equals_inline_in_query` end-to-end test | Test at `wave4_advanced_analytics_attribute_cohort_join.rs:429`; passes | ✅ |
| `query-language.md §18` states explicit equivalence between the two forms | §18.1 covers per-submission caching and alias composition but does not explicitly state that `IN alias` and `IN QUERY (same pipeline)` are semantically identical. The equivalence is implicit but not stated as a guarantee | ⚠️ |

---

### Block B — Entity-Aligned Source JOIN

#### §3.2 — B1: Same-Timestamp Cross-Table Event Ordering

| Promise | Evidence | Status |
|---------|----------|--------|
| Merge order: `(ts, table_order_in_source_expression, __seq_id)` | `MergeSourcesOperator` heap uses `(entity_key_value, ts, scan_idx)` — `scan_idx` is the 0-indexed JOIN clause position, mapping directly to `table_order` per §3.2 | ✅ |
| `merge_sources_same_ts_tiebroken_by_scan_idx` passes | Test at `scan.rs:3974` verifies same-`(entity, ts)` tie-broken by scan_idx; passes | ✅ |
| End-to-end verification across real storage queries | All end-to-end joined-source tests are `#[ignore]` due to J1 (`__seq_id` nullability crash); cannot verify B1 end-to-end | ❌ |

---

#### §3.3 — B2: Uniform Scan-Range Widening Across Joined Tables

| Promise | Evidence | Status |
|---------|----------|--------|
| Planner's scan-extension applies uniformly to every joined table | `LogicalPlan::Scan { joined_tables }` carries the primary + joined tables together; `reader_backward_ns`/`reader_forward_ns` on the single logical scan node applies to all tables | ✅ |
| Simplest correct rule (per-operator-arg widening is a Wave 5 optimization) | Design doc §3.3 documents this as the conservative-correct approach; code matches | ✅ (by design) |

---

#### §3.4 — B3: JOIN + SAMPLE Interaction

| Promise | Evidence | Status |
|---------|----------|--------|
| SAMPLE hash is over entity-key value, not column name; atomic cross-table keep/drop | `MergeSourcesOperator` module doc at `scan.rs:1261–1270` confirms SAMPLE is applied inside each sub-scan's `ScanOperator` via `SampleFilter` pushdown (TASK-430 + TASK-436 CP1); identical entity-id value → identical xxHash64 → identical keep/drop across tables | ✅ |
| End-to-end SAMPLE + JOIN test | End-to-end tests for JOIN are `#[ignore]` (J1); cannot verify B3 end-to-end | ❌ |

---

#### §3.5 — B4: DELETE + JOIN Disallowed

| Promise | Evidence | Status |
|---------|----------|--------|
| `DELETE FROM events JOIN ...` is a parser error with targeted message | `dml.rs:388–397` rejects `JOIN` after `DELETE FROM <table>` with: `"DELETE FROM <table> does not support JOIN; filter via a WHERE predicate over entity or system columns instead"` | ✅ |
| Tested | `delete_join_after_table_errors` at `dml.rs:1262–1269` | ✅ |
| `query-language.md §19.2` should document B4 with `IN QUERY` workaround example | §19.2 covers self-joins only; the DELETE+JOIN prohibition and the `IN QUERY` workaround pattern described in design §3.5 are not present in §19.2 | ❌ |

---

#### §3.6 — B5: Entity-Key Type Mismatch

| Promise | Evidence | Status |
|---------|----------|--------|
| JOIN between tables with different entity-key `BqlType`s is a plan-time error | `lower_query_pipeline` checks entity-key types at `logical.rs:1217–1225`; error: `BqliteError::Plan("JOIN entity-key type mismatch: primary ... has ..., joined ... has ...")` | ✅ |
| Tested | `joined_pipeline_entity_key_type_mismatch_rejected` at `logical.rs:7874` | ✅ |
| `query-language.md §19.5` explicitly calls this a plan-time error | §19.5 states "Both must have the same entity-key type" but does not explicitly say this is detected at plan time vs. runtime | ⚠️ |

---

#### §3.7 — B6: N-ary MergeSources Operator

| Promise | Evidence | Status |
|---------|----------|--------|
| JOIN implemented as single n-ary `MergeSources` (not chained binary merge) | `MergeSourcesPhysical { tables: Vec<ScanPhysical>, ... }` (one entry per joined table, n-ary); `MergeSourcesOperator` takes `Vec<Box<dyn PhysicalOperator>>` | ✅ |
| k-way merge (not per-operator multi-stream awareness) | `MergeSourcesOperator` owns a min-heap of size n; per-shard k-way merge confirmed in unit tests (2-table, 3-table cases pass) | ✅ |
| 12 `MergeSourcesOperator` unit tests pass | `scan.rs:3906–4247` covers disjoint entities, ordering, tie-breaking, 3-way, empty cases, constructor validation, multi-batch reload; all pass | ✅ |

---

#### §3.8 — B7: Source-Table Discriminator Column

| Promise | Evidence | Status |
|---------|----------|--------|
| `__source_table_id: Int8` non-nullable discriminator injected by `MergeSources` | Discriminator injected at `logical.rs:1320`; non-nullable confirmed | ✅ |
| Type specified as `Int8` in design; implementation uses `Int` (i64) | `physical.rs:845–847` notes: "design doc specifies `Int8` for `__source_table_id`, but `BqlType` has no `Int8` variant. The planner uses `Int` (i64)". Arrow representation follows planner's `BqlType::Int` → `Int64`. | ⚠️ |
| Table-qualified references resolve through registry | Planner rewrites `events.signup` to `__source_table_id = 0 AND event_type = 'signup'` via `table_id_map` | ✅ |
| `joined_pipeline_lowers_with_combined_schema_and_discriminator` test | `logical.rs:7836` confirms discriminator in combined schema; passes | ✅ |

---

#### §3.9 — B8: `__source_table_id` Absent in Single-Table Queries

| Promise | Evidence | Status |
|---------|----------|--------|
| Single-table source produces no `__source_table_id` column | Plain `Scan` (not `MergeSources`) for single-table; discriminator only injected in `build_joined_scan`; `MergeSourcesOperator` also supports optional discriminator via `source_table_id_col = None` | ✅ |
| Tested | `merge_sources_without_source_table_id_column` at `scan.rs:4075` confirms absence is valid | ✅ |

---

#### §3.10 — B9: Aliases Referencing Joined-Source Pipelines

| Promise | Evidence | Status |
|---------|----------|--------|
| Alias built from a joined-source pipeline produces a cohort that outer queries consume normally | Alias expansion is pre-planning; the alias body is lowered independently with its own JOIN source; its output shape is what matters at use site | ✅ (by construction) |

---

#### §3.11 — B10: Step-Name Before Table Qualifier

| Promise | Evidence | Status |
|---------|----------|--------|
| In MATCH steps inside JOIN queries, `step_name: table.event_type` order is enforced | `pattern.rs` and `pipeline.rs` parser tests confirm `s: events.signup` and `purchase_step: purchases.purchase WHERE purchases.amount > 100` forms accepted; order is fixed by grammar production | ✅ |
| Restated explicitly in `query-language.md §19.1` | §19.1 includes: "Inside a MATCH step, the step-name prefix (`s:`) is written before the table-qualified event: `s: events.signup`. Both prefixes are optional individually but have fixed order when both are present." | ✅ |

---

### Block C — SubqueryFilter Physical Execution

#### §4.1 — C1: Hash-Set Probe Execution

| Promise | Evidence | Status |
|---------|----------|--------|
| Subquery materialized into `HashSet<Tuple>` (single-column and N-column) | `CohortHashSet` (`cohort.rs:136–229`) uses `HashSet<CohortKey, RandomState>`; single-element and N-element tuple keys both supported | ✅ |
| Outer stream probed row-by-row | `SubqueryFilterOperator.probe_batch` (`cohort.rs:346–395`); batch-level evaluation with per-row probe loop | ✅ |
| `NULL` on LHS drops the row (three-valued `IN` semantics) | `any_null` check at `cohort.rs:374–385`; NULL-bearing rows bypass probe and evaluate to false; tested by `null_lhs_row_is_dropped` | ✅ |
| Empty cohort short-circuits (all rows dropped) | Early `if self.cohort.is_empty()` at `cohort.rs:354`; tested by `empty_cohort_rejects_every_row` | ✅ |
| System columns (`__seq_id`, `__batch_id`) excluded from cohort key | `CohortHashSet::from_batches` filters `is_system()` columns when building keys (`cohort.rs:196–203`); tested by `cohort_from_batches_skips_system_columns` | ✅ |

---

#### §4.2 — C2: Cohort Materialization Timing (at Query Start)

| Promise | Evidence | Status |
|---------|----------|--------|
| All cohorts materialized before outer-query scan begins | `bind_subquery_filter` (`bind.rs:973–1008`) materializes the inner subquery fully, builds `CohortHashSet`, then wires it into `SubqueryFilterOperator`; the bind step is called before any `open()` or `next_batch()` | ✅ |
| Independent cohorts may materialize in parallel | CohortCache is single-threaded linear scan; parallel materialization not implemented in v1 | ⚠️ (v1 acceptable) |
| `cohort_cache_get_returns_arc_for_equal_plan` pins Arc-sharing invariant | Test at `bind.rs:2104–2138` asserts `Arc::ptr_eq` for two structurally equal plans; passes | ✅ |

---

#### §4.3 — C3: Entity-ID Component Pushdown

| Promise | Evidence | Status |
|---------|----------|--------|
| Entity-id component of multi-column cohorts pushed to scan as hash-set filter | Not implemented; post-scan probe is the only filtering path | ❌ |
| Deferral documented in design doc §6.3.1 | Design §6.3.1 explicitly documents the deferral with rationale ("purely a performance optimization") and the follow-up work required (extending `ScanPredicate` with `column IN <Arc<HashSet<EntityId>>>`); intentionally out of scope for TASK-437 | ✅ (intentional deferral) |

---

### Block D — Documentation Reconciliation (Design §7)

The design doc §7 specifies explicit `query-language.md`, `type-system.md`, and `planner-pipeline.md` updates required in the same checkpoint as dependent code changes.

#### §7.1 — `query-language.md` Updates

| Promised Update | Section | Status |
|----------------|---------|--------|
| A9 positional multi-column binding with example showing differing LHS/subquery column names | §17.4 | ✅ §17.4 includes explicit statement plus worked example |
| A6 memory-budget caveat: exceeding budget errors the whole query | §17 / §18 | ❌ Not present (tracked as J4) |
| A1: event-type and column names not reserved against alias shadowing | §18.1 | ⚠️ §18.1 says "Shadowing other aliases is permitted" but the event-type/column-name exclusion rationale is absent |
| A2: forward refs forbidden, top-down order | §18.1 | ✅ "Top-down order. An alias must be defined before it is referenced." |
| A3: submission = one `execute` call; engine is alias-stateless | §18.1 | ✅ "Submission-scoped in the engine. Aliases live for one query submission / execute call." |
| A4: always-cached within submission | §18.1 | ✅ "Lazy evaluation with per-submission caching." |
| A5: no cross-submission caching (corollary of A3) | §18.3 | ✅ "Persistent aliases — named views — v2 feature and explicitly out of scope." |
| A10: explicit equivalence between `IN alias` and `IN QUERY` | §18 | ⚠️ Not stated explicitly (tracked as J5) |
| B4: DELETE+JOIN disallowed with `IN QUERY` workaround example in §19.2 | §19.2 | ⚠️ Content exists in §28.16 as a code comment, not in §19.2 as required (tracked as J6) |

---

#### §7.2 — `type-system.md` Updates

| Promised Update | Section | Status |
|----------------|---------|--------|
| A8 (alias shape use-site enforced) and A9 (positional tuple binding) | §6.9 | ⚠️ §6.9 says "multi-column IN is supported for compound keys" and "column tuple on left must type-match subquery columns" but does not state A8 (definition-time shape-agnostic) or show A9 example with differing column names |
| New section: `__source_table_id: Int8 NOT NULL` column introduced by `MergeSources` | New section | ❌ No section for `__source_table_id` or system columns added by JOIN exists in `type-system.md` |

---

#### §7.3 — `planner-pipeline.md` Updates

| Promised Update | Section | Status |
|----------------|---------|--------|
| `SubqueryFilter` physical shape per C1/C2: carries `Arc<HashSetCohort>`, materialized at query start, entity-id pushdown per C3 | §5.1 | ⚠️ `planner-pipeline.md §5.1` and `§1092` show only `SubqueryFilterPhysical { /* ... */ }` as a stub; `Arc<HashSetCohort>` materialization-at-query-start semantics are not reflected |

---

#### §8.2 — JOIN Error Table Coverage

| Error Condition | Evidence | Status |
|----------------|---------|--------|
| Self-join (`events JOIN events`) → `ParseError::SelfJoin` | `error.rs:105` defines `ParseError::SelfJoin`; tested at parser level | ✅ |
| Unknown table in JOIN → `TypeError::UnknownTable` | `catalog.resolve_table(joined_name)?` at `logical.rs:1216` returns `BqliteError::Plan` via `unknown_table_error`; correct behavior but generic `Plan` string, not structured `TypeError::UnknownTable` | ⚠️ |
| Entity-key type mismatch → `TypeError::EntityKeyTypeMismatch` | Implemented as `BqliteError::Plan("JOIN entity-key type mismatch: ...")` | ⚠️ (see J3) |
| `DELETE FROM ... JOIN ...` → `ParseError::DeleteJoinNotSupported` | Parser produces `ParseError::Unexpected { expected: Keyword("WHERE"), detail: Some("DELETE FROM <table> does not support JOIN; ...") }` — structured named variant absent | ⚠️ (see J3) |
| Bare reference in JOIN context → `TypeError::UnqualifiedReferenceInJoin` | Planner rejects via "unknown column" error from schema lookup; no dedicated variant | ⚠️ (see J3) |

---

### Integration Test Coverage

| Test | Feature | Status |
|------|---------|--------|
| `cohort_in_query_restricts_downstream_scan` | `IN QUERY` probe | Passing |
| `cohort_alias_equals_inline_in_query` | A10 alias ≡ IN QUERY equivalence | Passing |
| `multi_column_in_query_filters_by_tuple` | A9 positional multi-column binding | Passing |
| `cohort_plus_filter_plus_aggregate_exact_count` | Cohort + filter + aggregate composition | Passing |
| `joined_source_stats_counts_entities_in_both_tables` | Two-table JOIN, basic stats | `#[ignore]` — J1 blocker |
| `joined_source_sequence_match_spans_tables` | Two-table JOIN + MATCH | `#[ignore]` — J1 blocker |
| `joined_source_funnel_invariance_under_compaction` (acceptance) | JOIN + FUNNEL + compaction | `#[ignore]` — J1 blocker |
| MergeSourcesOperator unit tests (12 total, `scan.rs`) | Merge ordering, tie-breaking, empty cases, constructor validation | All passing |
| `bench_cohort_semijoin` (Criterion) | SubqueryFilterOperator throughput (≥10M rows/sec at 10k cohort) | Benchmark present |
| `bench_merge_sources` (Criterion) | MergeSourcesOperator merge throughput (≥10M rows/sec at k=2) | Benchmark present |

---

## Drift and Missing Coverage — Follow-up Items for TASK-455

### J1 — `__seq_id` nullability crash blocks all end-to-end joined-source tests (Critical)

**Promise**: Entity-aligned source JOIN merges entity-sorted streams from N tables and produces a unified event stream that downstream operators (MATCH, SESSIONIZE, FUNNEL, etc.) consume normally.

**Evidence**: Three end-to-end joined-source tests are `#[ignore]` with the message "MergeSourcesOperator fails `__seq_id` nullability at assembly; any `events JOIN purchases` surfaces it."

**Root cause**: The ScanOperator module (`scan.rs:53–60`) explicitly states: "Implicit system columns (`__seq_id`, `__batch_id`) are **not** included [in the ScanOperator's output schema]; the Wave 2 segment reader does not yet expose them." When real ScanOperator children are wired to `MergeSourcesOperator`:
1. Each sub-scan's schema lacks `__seq_id`.
2. `MergeSourcesOperator.new()` builds `reverse_col_map`: for `__seq_id`, both sub-scans' descriptors have `None` (sub-scan has no `__seq_id` column to map).
3. `build_output_batch` falls through to `new_null_array(field_type, len)` for both sub-scans.
4. `interleave` picks from all-null arrays → all-null `__seq_id` column in output.
5. The combined schema (from `build_joined_scan`) declares `__seq_id: Int NOT NULL` → `RecordBatch::try_new` panics: "Column `__seq_id` is declared as non-nullable but contains null values."

**Impact**: All joined-source queries fail at runtime. The MergeSourcesOperator is functionally complete at the unit-test level (using synthetic `VecOp` children that omit `__seq_id`), but cannot be driven by real storage scans. Both integration tests and the Wave 4 acceptance test's joined-source case are `#[ignore]`.

**Required work**: One of three approaches:
1. **Preferred**: Extend the Wave 2 segment reader to materialize `__seq_id` per row and expose it in `ScanOperator`'s output schema. This makes the system consistent with the design intent and unblocks all JOIN-related tests.
2. **Stopgap**: Declare `__seq_id` (and `__batch_id`) as nullable in the combined join schema (`build_joined_scan`). This allows the null array to pass `RecordBatch::try_new`; downstream operators would need to handle nullable `__seq_id`. Simpler to implement but leaves the system in an inconsistent state.
3. **Alternative**: Remove `__seq_id`/`__batch_id` from the combined join schema entirely until system column materialization is implemented (TASK-501 scope).

---

### J2 — `__source_table_id` type drift: design says `Int8`, implementation uses `Int` (i64)

**Promise**: `cohorts-aliases-joins.md §3.8` specifies `__source_table_id: Int8 NOT NULL`, citing `i8` as the minimal representation for realistic JOIN widths (≤ 4 tables).

**Evidence**: `physical.rs:845–847` acknowledges the discrepancy: "the design doc specifies `Int8` for `__source_table_id`, but `BqlType` has no `Int8` variant. The planner uses `Int` (i64)."

**Impact**: Low. The discriminator is 8× larger than designed (8 bytes vs 1 byte per row). For large multi-table queries this is wasted memory and reduces Arrow batch density. No correctness issue. The Arrow schema emits `Int64` where the design promised `Int8`.

**Required work**: Either (a) add `BqlType::Int8` / `BqlType::SmallInt` to `bqlite-core`'s type system and update the planner, or (b) update the design doc to accept `Int` (i64) as the implementation type and retire the `Int8` goal. If (b), update `cohorts-aliases-joins.md §3.8` and `type-system.md §new section` (see design §7.2).

---

### J3 — Named error type variants missing; all errors use generic `BqliteError::Plan(String)`

**Promise**: Design doc §8.1–8.3 specifies structured error variants: `TypeError::AliasCycle { path: Vec<String> }`, `TypeError::UndefinedAlias`, `TypeError::AliasNameCollision`, `TypeError::IncompatibleCohortShape`, `TypeError::IncompatibleCohortType`, `TypeError::EntityKeyTypeMismatch`, `TypeError::UnqualifiedReferenceInJoin`, `ParseError::SelfJoin`, `ParseError::DeleteJoinNotSupported`.

**Evidence**: `bqlite-core/src/error.rs` has no `TypeError` enum. All planner errors use `BqliteError::Plan(String)` with human-readable messages. `ParseError::SelfJoin` exists (`error.rs:105`) — the only structured error from the promised set. For DELETE+JOIN: `dml.rs:388–397` produces `ParseError::Unexpected { expected: Keyword("WHERE"), detail: Some("DELETE FROM <table> does not support JOIN; ...") }` — the detail string carries a useful hint, but the named `ParseError::DeleteJoinNotSupported` variant is absent. The `AliasCycle.path` field is rendered as a formatted string, not a machine-readable `Vec<String>`. Unknown table in JOIN produces `BqliteError::Plan` via `catalog.resolve_table()`, not `TypeError::UnknownTable`.

**Impact**: Medium. Human-readable error messages are correct and informative; the DELETE+JOIN error in particular includes a clear hint about the workaround. The absence of structured variants prevents programmatic error-handling in CLI, FFI, and test assertions (current tests use `.contains("...")` on the string). The `AliasCycle.path` vec is non-inspectable without string parsing.

**Required work**: Introduce `TypeError` (or extend `BqliteError`) with the promised structured variants. Priority candidates: `AliasCycle { path: Vec<String> }` (path vec is most useful for tools), `IncompatibleCohortShape { lhs_arity, rhs_arity }`, and `EntityKeyTypeMismatch { primary, joined, primary_type, joined_type }`. `ParseError::DeleteJoinNotSupported` is the remaining missing parse-level named type (the current error message is already correct; it just needs a dedicated variant).

---

### J4 — A6 memory budget caveat missing from `query-language.md`

**Promise**: Design §2.7 requires: "`query-language.md §17 / §18` must document the A6 caveat: exceeding the memory budget errors the whole query (no silent truncation)."

**Evidence**: `query-language.md §17` and `§18` do not mention cohort memory limits, out-of-budget errors, or the "whole-query failure with no partial result" behavior. Section 18.1's "Lazy evaluation with per-submission caching" bullet is the closest, but it does not address failure mode.

**Impact**: Low (TASK-501 memory budget not yet implemented, so users cannot trigger this path). Once TASK-501 lands, users have no documentation guidance on what happens when a cohort exceeds budget.

**Required work**: Add a note to `query-language.md §17` and `§18.1`: "Cohort materialization draws from the query's memory budget (configured at engine initialization). If a cohort's hash set exceeds the budget, the entire query fails with an out-of-budget error — there is no partial-result mode and the cohort is never silently truncated." Cross-reference TASK-501.

---

### J5 — A10 equivalence not stated explicitly in `query-language.md §18`

**Promise**: Design §2.11 requires: "`query-language.md §18` should add an explicit A10 equivalence statement between `IN alias` and `IN QUERY (same pipeline)`."

**Evidence**: `query-language.md §18.2` explains that bare identifiers on the right of `IN` are alias references and describes alias resolution, but does not explicitly state that `entity_id IN my_alias` and `entity_id IN QUERY (same pipeline as my_alias)` are semantically identical. The caching guarantee (executed once, shared result) is also not made explicit for `IN QUERY` forms.

**Impact**: Low. Users who read §18.2 understand the mechanics but may not realize the two forms are fully interchangeable for refactoring purposes.

**Required work**: Add a paragraph to `query-language.md §18` (or §18.2): "The two forms `x IN alias` and `x IN QUERY (<alias body>)` are semantically identical. The planner normalizes both to the same internal representation before making materialization decisions, so refactoring between the forms does not change query semantics or caching behavior."

---

### J6 — `query-language.md §19.2` missing DELETE+JOIN prohibition — content misplaced in §28.16

**Promise**: Design §7.1 table requires: "`query-language.md §19.2`: Restate B4 disallowing `DELETE FROM ... JOIN ...` with `IN QUERY` workaround example."

**Evidence**: `query-language.md §19.2` covers only the self-join prohibition (`events JOIN events` is a parse error). However, the DELETE+JOIN prohibition and workaround pattern **do exist** in the document — they appear as a code comment at `query-language.md §28.16` (the DELETE examples section, lines 2163–2164): `"-- Cannot use DELETE FROM ... JOIN ...; express cross-table deletes as sequential single-table DELETEs using IN QUERY"` with a full worked example. The content was written in the DELETE context rather than promoted to §19.2 where the design doc requires it.

**Impact**: Low. The content exists and is discoverable by a reader scanning the DELETE section. But a reader looking up "JOIN limitations" in §19.2 would not find the DELETE restriction or workaround; they would have to encounter the parser error first or search for the DELETE context.

**Required work**: Promote the §28.16 inline comment into a dedicated subsection in `query-language.md §19.2` (e.g., "§19.2.1 — No DELETE FROM with JOIN") with a cross-reference back to §28.16 for worked examples. The content does not need to be written from scratch — it exists at §28.16 and in `cohorts-aliases-joins.md §3.5`; it needs placement, not authoring.

---

### J7 — `type-system.md` §6.9 and `planner-pipeline.md` §5.1 updates not applied (design §7.2–7.3)

**Promise**: Design §7.2 requires two `type-system.md` updates: (a) §6.9 should document A8 (alias shape is use-site-enforced, definition is shape-agnostic) and A9 (positional tuple binding with example); (b) a new section for `__source_table_id: Int8 NOT NULL`. Design §7.3 requires `planner-pipeline.md §5.1` to reflect the `SubqueryFilter` physical shape (carrying `Arc<HashSetCohort>`, materialized at query start, entity-id pushdown per C3).

**Evidence**: `type-system.md §6.9` covers `IN` subquery filtering at a high level ("multi-column IN is supported for compound keys") but does not document A8 or provide an A9 worked example with differing column names. No section covering `__source_table_id` or `MergeSources` system columns exists in `type-system.md`. `planner-pipeline.md §5.1` shows only `SubqueryFilterPhysical { /* ... */ }` as a stub at line 1092; the materialized-cohort-at-query-start semantics and entity-id pushdown deferral are not reflected.

**Impact**: Low. The working spec for these features is in `cohorts-aliases-joins.md`, which is the design anchor. The downstream docs are supplementary references. Missing updates reduce cross-reference discoverability for future contributors.

**Required work**: (a) Expand `type-system.md §6.9` to state explicitly that alias shape is shape-agnostic at definition time (A8) and that multi-column IN binds positionally by declaration order with a concrete example. (b) Add a new subsection to `type-system.md` for `__source_table_id: Int NOT NULL` (or `Int8` if J2 is resolved) introduced by `MergeSources`. (c) Expand `planner-pipeline.md §5.1` `SubqueryFilterPhysical` to show the materialized cohort field, query-start materialization guarantee, and note the C3 pushdown deferral.

---

### J8 — A1: Alias-name table-name collision check not implemented

**Promise**: Design §2.2 (A1) states: "Aliases may not shadow reserved keywords or table names." Design §8.1 specifies `TypeError::AliasNameCollision` fires at bind time when the alias name collides with a catalog table name.

**Evidence**: No check exists. `AliasTable.push_definition` (`logical.rs:1076–1082`) inserts the alias name unconditionally without consulting the catalog. `lower_statements` and `lower_query_pipeline` do not compare alias names against the catalog's table registry. `TypeError::AliasNameCollision` does not exist in `bqlite-core/src/error.rs`.

**Impact**: Medium. A user can define `events = events | WHERE event_type = 'x'`, and subsequent `events |` source references would be ambiguous — the planner might resolve to the alias rather than the storage table, or produce confusing errors. No test covers this failure mode.

**Required work**: In `lower_statement_with_aliases` (or `push_definition`), after parsing alias names, validate each name against `catalog.resolve_table(name)`. If the lookup succeeds, reject the submission with `TypeError::AliasNameCollision { name }` (or equivalent `BqliteError::Plan`). Add a test: `alias_name_matching_table_name_is_rejected`.

---

### J9 — Entity-id pushdown deferred (C3) — tracking item only

**Promise**: Design §4.3 specifies entity-id component pushdown for multi-column cohorts; §6.3.1 explicitly defers it.

**Evidence**: Deferral is documented in the design doc with detailed rationale and a follow-up work plan. No implementation exists; the post-scan probe is correct without it. Impact is performance, not correctness.

**Impact**: On large outer tables filtered by small cohorts, all rows are scanned and post-filtered rather than using shard/segment skipping. For v1 cohort sizes this is acceptable.

**Required work**: File a Wave 5 task that extends `ScanPredicate` taxonomy with `column IN <Arc<HashSet<EntityId>>>`, teaches shard/segment skip logic to use it, and wires it in the engine bind step after cohort materialization.

---

## Summary

The cohort and alias implementation (Block A) is **nearly complete** at all layers — parser, planner, runtime, and engine bind. Nine of ten A-promises are implemented and backed by passing tests. One gap remains: A1's table-name collision check (J8) — the planner does not validate that an alias name doesn't collide with a catalog table name, and the `AliasNameCollision` error type is absent. The cohort semi-join integration tests (`cohort_in_query_restricts_downstream_scan`, `cohort_alias_equals_inline_in_query`, `multi_column_in_query_filters_by_tuple`, `cohort_plus_filter_plus_aggregate_exact_count`) all pass. The `SubqueryFilterOperator` (Block C) is correct and benchmarked.

The entity-aligned source JOIN (Block B) is **structurally complete at the planner and operator-unit level** but **entirely non-functional end-to-end**. The MergeSourcesOperator has 12 passing unit tests covering ordering, tie-breaking, constructor validation, and multi-batch reload. However, three end-to-end joined-source tests are `#[ignore]` due to J1 (`__seq_id` nullability crash): the planner declares `__seq_id` non-nullable in the combined join schema, but the ScanOperator does not expose `__seq_id` in its schema (documented design gap), causing the merge operator to emit a null array that violates the non-null constraint.

| Item | Severity | Blocking? |
|------|----------|-----------|
| J1: `__seq_id` nullability crash — all end-to-end JOIN tests `#[ignore]` | **Critical** | Yes — entity-aligned JOIN is non-functional end-to-end; blocks TASK-442 acceptance test's joined-source case |
| J2: `__source_table_id` type drift: Int8 vs Int (i64) | Low | No (correctness unaffected; memory cost only) |
| J3: Structured error variants missing (using `BqliteError::Plan(String)`) | Medium | No (human messages correct; programmatic handling limited) |
| J4: A6 memory budget caveat not documented in query-language.md §17/§18 | Low | No (TASK-501 not landed yet) |
| J5: A10 alias ≡ IN QUERY equivalence not explicit in query-language.md §18 | Low | No |
| J6: DELETE+JOIN prohibition + workaround example not in query-language.md §19.2 | Low | No |
| J7: type-system.md §6.9 and planner-pipeline.md §5.1 doc updates not applied | Low | No |
| J8: A1 alias-name table-name collision check not implemented | Medium | No (can produce confusing behavior but does not crash) |
| J9: C3 entity-id pushdown deferred (intentional, documented in §6.3.1) | Low | No (performance optimization, not correctness) |
