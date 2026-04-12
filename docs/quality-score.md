# Quality Score

Per-crate quality grades, updated at the close of each wave. The most
recent pass is the **Wave 3 audit** (TASK-399, `2026-04-12`).

## Grading scale

| Grade | Meaning |
|-------|---------|
| **A** | Wave-scope complete, comprehensive tests (unit + property/integration), extensive docs with design-doc cross-references, bench coverage for perf-critical paths. |
| **B** | Wave-scope complete with minor gaps; good edge-case tests; thorough module + item docs; bench harness wired. |
| **C** | Stubs land correctly for the wave's scope; basic tests exercise happy path + a few edges; module-level docs present; bench harness at least scaffolded. |
| **D** | Minimal stubs, few tests, docs sparse, no benches. |
| **F** | Not started, empty stub only. |

Any crate grading below **C** on any dimension triggers a follow-up task,
filed no later than the next wave (`[AGENTS.md] §Behavioral Requirements`).

## Wave 3 grades

Per-cell grades carry a one-line justification. Evidence is collected
below the table. Grade changes from Wave 2 are annotated with arrows.

| Crate | Tests | API | Docs | Benchmarks | Overall |
|-------|-------|-----|------|------------|---------|
| bqlite | **C** · 0 unit, 0 doctest; re-export crate, transitive coverage via re-exported crates | **B-** · re-exports `ast`, `types`, `parser`, `engine`, `BqliteError`, `Result` — complete for Wave 3 | **C+** · crate-level doc with quick-start; 1 rustdoc collision warning persists | **C** · no per-crate benches; workspace harness available | **C+** |
| bqlite-core | **A** · 246 unit + 3 doctest; covers every type including Wave 3 additions (ScalarValue, AggFunction) *(↑ from 217)* | **A-** · foundational trait surface + types; Wave 3 additive extensions (ScalarValue, AggFunction shared types) preserve backward compat | **B+** · exhaustive module docs with design-doc refs; 4 rustdoc warnings persist (intra-doc links) | **C** · no per-crate benches (appropriate — pure types) | **A-** |
| bqlite-ast | **A-** · 49 unit across expr/operator/pattern/pipeline/span/statement | **A-** · AST node enums for every BQL construct through Wave 3 (STATS, ORDER BY/SORT additions) *(↑ from B+)* | **B** · per-module docs on every file | **C** · no per-crate benches | **A-** *(↑ from B+)* |
| bqlite-storage | **A** · 426 unit + 79 workspace property tests (encoding roundtrips, zone-map no-false-negatives) *(↑ from 417)* | **A** · complete v1 segment format; no Wave 3 API changes | **A-** · extensive module docs with design-doc §-refs; 18 rustdoc warnings (private-item links, +2 new) | **A-** · 4 Wave 2 bench groups covering all perf-gate metrics; bench CI wired | **A** |
| bqlite-parser | **A** · 415 unit + 1 doctest; new pattern module (49 tests) + MATCH/FUNNEL/STATS/ORDER BY pipeline productions (85+ new tests) + source time-range parsing (12 tests, TASK-328); every production has happy-path + error-case coverage *(↑ from 263)* | **A** · full Wave 3 grammar: MATCH (FIRST/ALL), FUNNEL, STATS (10 agg functions + GROUP BY), ORDER BY/SORT, SEQUENCE patterns with WITHIN/BRACKETS/EMIT ALL/WITHOUT/IMMEDIATELY/repetition, source time-range (LAST/BETWEEN) *(↑ from A-)* | **A-** · module docs with grammar-section refs and design-doc cross-references (pattern-grammar.md, query-language.md §4–§26); 3 rustdoc warnings persist *(↑ from B+)* | **C** · no per-crate benches (parser is not perf-critical path) | **A** *(↑ from A-)* |
| bqlite-planner | **A** · 272 unit + 1 doctest; pattern compiler (41 tests), FUNNEL desugaring (10), match-aggregate fusion (11), logical/physical lowering for 4 new plan variants, scan time-range extension + EXPLAIN formatting (13 tests, TASK-328), variable-binding validation (TASK-329) *(↑ from 173)* | **A** · AST → LogicalPlan → PhysicalPlan with 4 new plan nodes (SequenceMatch, Aggregate, Sort, Distinct), pattern compiler (CompiledNfa), 4 optimizer passes (pushdown, pruning, fusion, demand propagation), DemandSet backward analysis, scan time-range extension *(↑ from A-)* | **A-** · module docs with design-doc refs (wave3-lowering.md §2–§4, sequence-matching.md §14.2, aggregate-operator.md §9); 5 rustdoc warnings (+2 new: `desugar_funnel`/`fuse_match_aggregate` name collisions) *(↑ from B+)* | **C** · no per-crate benches (planner is not hot path) | **A** *(↑ from A-)* |
| bqlite-operators | **A** · 331 unit; MATCH operator (99 tests across 5 matcher submodules), hash aggregate (59 tests), DDSketch percentiles (30 tests), sort (15 tests), distinct (15 tests) + Wave 2 operators (113 tests) *(↑↑ from 113)* | **A** · complete Wave 3 operator set: SequenceMatchOperator (NFA + step-counter strategies, variable bindings, EMIT ALL), HashAggregateOperator (8 agg functions incl. DDSketch P50/P90/P95/P99), SortOperator, DistinctOperator — all with cancellation, memory caps, fused-aggregate protocol *(↑ from A-)* | **A-** · every module has section-level design-doc cross-references (match-operator.md, sequence-matching.md, matcher-strategy.md, aggregate-operator.md, sort-distinct.md); 3 rustdoc warnings (+2 new: AggregatePhysical, PatternClass links) *(↑ from B+)* | **A-** · 6 dedicated Wave 3 bench groups (matcher, aggregate, sort, distinct, funnel, percentile) covering step-counter vs NFA comparison, grouped aggregation scaling, DDSketch throughput, sort/distinct at multiple row counts *(↑↑ from B+)* | **A** *(↑ from A-)* |
| bqlite-engine | **B+** · 58 unit covering parse → plan → bind → drive + DDL/DML execution + Wave 3 bind step for SequenceMatch/Aggregate/Sort/Distinct *(↑ from 51)* | **A-** · `Engine::query` extended with Wave 3 plan-to-operator binding for all 4 new physical descriptors *(↑ from B+)* | **B** · module docs present; **6 rustdoc warnings** (+1 new: `SequenceMatchAdapter` private-item link) | **C+** · no per-crate benches; covered transitively by workspace funnel + acceptance benches | **B+** |
| bqlite-cli | **A-** · 84 unit covering 3 subcommands (init, query, ingest), auto-limit machinery, argument parsing *(↑ from 80)* | **A-** · unchanged API surface; Wave 3 features available via `bqlite query` | **B+** · extensive module docs; clean rustdoc (0 warnings) | **C** · no per-crate benches (CLI frontend not perf-critical) | **A-** |
| bqlite-ffi | **C** · 0 unit tests — appropriate; FFI is Wave 6 | **C** · module docs enumerate intended PyO3 surface; no implementation yet | **C** · crate-level doc explains intent and placement | **C** · no benches — out-of-scope | **C** |

**Workspace-level test artifacts** (`bqlite-tests` + `bqlite-benches` workspace crates):

| Target | Count | Purpose |
|--------|-------|---------|
| `tests/common_smoke.rs` | 13 | Integration fixture framework (TASK-120) — temp DB helper, assert_batches_eq, CSV loader |
| `tests/matcher_integration.rs` | 45 | **NEW** Matcher integration test suite (TASK-324, TASK-329) — linear patterns, MATCH FIRST/ALL, negation, repetition, time windows, EMIT ALL, alternation, IMMEDIATELY, sub-batch streaming, variable bindings (8 E2E tests added by TASK-329) |
| `tests/prop_property_value.rs` | 12 | Property-test harness (TASK-124) — `PropertyValue` round-trips via `proptest` |
| `tests/prop_arrow.rs` | 1 | Arrow ↔ BqlType round-trip property test |
| `tests/prop_encoding_plain.rs` | 13 | Plain encoding encode/decode round-trip property tests |
| `tests/prop_encoding_dictionary.rs` | 15 | Dictionary encoding encode/decode round-trip property tests |
| `tests/prop_encoding_delta.rs` | 10 | Delta encoding encode/decode round-trip property tests |
| `tests/prop_encoding_bitpacking.rs` | 11 | BitPacking encoding encode/decode round-trip property tests |
| `tests/prop_encoding_constant.rs` | 9 | Constant encoding encode/decode round-trip property tests |
| `tests/prop_nfa.rs` | 6 | **NEW** NFA simulator property tests — window expiry monotonicity, poison kill completeness, epsilon/wildcard matching |
| `tests/prop_bindings.rs` | 5 | **NEW** Variable binding property tests — track identity stability, NULL short-circuit, active-track cap, bind-once-then-check |
| `tests/prop_time.rs` | 7 | TimeRange intersection/shift property tests |
| `tests/smoke.rs` | 8 | Wave 1+2 acceptance gates (TASK-123) — CLI subprocess, `(0 rows)` footer, unknown-table error |
| `tests/wave2_acceptance.rs` | 8 (1 ign) | Wave 2 acceptance gate (TASK-235) — CREATE TABLE, INSERT FROM CSV, INSERT VALUES, DESCRIBE, ALTER, DROP, end-to-end query |
| `tests/wave3_acceptance.rs` | 6 | **NEW** Wave 3 acceptance gate (TASK-326, TASK-328) — FUNNEL sugar, desugared MATCH+STATS, FUNNEL-desugared equivalence, EXPLAIN time-range display, BETWEEN time-range, backward-compat without time-range |
| `benches/benches/smoke.rs` | 1 | Criterion harness smoke (TASK-121) — no-op bench to exercise wiring |
| `benches/wave2/scan.rs` | — | Columnar decode throughput: int64, string, float, with/without zone-map pruning |
| `benches/wave2/encoding.rs` | — | Per-encoding encode/decode microbenches (Plain, Dictionary, Delta, BitPacking, Constant, LZ4) |
| `benches/wave2/ingest.rs` | — | CSV ingest throughput end-to-end |
| `benches/wave2/acceptance.rs` | — | Full round-trip: ingest → write segments → read segments, compression ratio, zone-map pruning rate |
| `benches/wave3/matcher.rs` | — | **NEW** Step-counter vs NFA strategy comparison at 3 entity scales, MATCH ALL mode, windowed NFA |
| `benches/wave3/aggregate.rs` | — | **NEW** Hash aggregation throughput: COUNT/SUM/AVG at 10/1K/1M groups, ungrouped multi-agg |
| `benches/wave3/sort.rs` | — | **NEW** Sort operator: single-key (10K–500K rows), two-key (string+int), multi-batch |
| `benches/wave3/distinct.rs` | — | **NEW** Distinct operator: dedup ratio variance (0%–99%), row scaling, multi-batch |
| `benches/wave3/funnel.rs` | — | **NEW** End-to-end 3-step funnel: CI mode (50K events), reference mode (100M events, <10s target) |
| `benches/wave3/percentile.rs` | — | **NEW** DDSketch insert/quantile/merge throughput, AggState integration, grouped P50 |

**Evidence aggregate**: **2,076 passing tests** via `cargo test --workspace --all-targets` (1,881 per-crate unit + 181 workspace integration/property + 14 bench-crate unit), **1 ignored** (1 wave2 100M-row acceptance), **0 failing tests**. Separately, `cargo test --workspace --doc` adds **5 passing doctests** and **5 ignored doctests**. Of the 181 workspace tests, **89 are property tests** covering encoding roundtrips (5 encodings × all applicable types), PropertyValue coercion, TimeRange algebra, Arrow type mapping, NFA simulator invariants, and variable binding semantics. **10 Criterion bench groups** cover both Wave 2 and Wave 3 performance gate metrics.

## Evidence

Gathered from `cargo test -p <crate>`, `cargo test --workspace --all-targets`,
`cargo bench -p bqlite-benches --no-run`, `cargo doc --workspace --no-deps`,
and `find crates/<crate>/src -name '*.rs'`.

| Crate | Unit tests | Doctests | LOC (src) | `pub` items | Rustdoc warnings |
|-------|-----------:|---------:|----------:|------------:|-----------------:|
| bqlite            |   0 |  0 (1 ign) |     33 |    5 | 1 (output collision) |
| bqlite-core       | 246 |  3         |  7,318 |  157 | 4 (intra-doc links) |
| bqlite-ast        |  49 |  0         |  2,197 |   68 | 0 |
| bqlite-storage    | 426 |  0 (1 ign) | 21,995 |  155 | 18 (private-item links) |
| bqlite-parser     | 415 |  1         |  9,581 |    7 | 3 (private-item links) |
| bqlite-planner    | 272 |  1         | 15,208 |  108 | 5 (private-item + name collisions) |
| bqlite-operators  | 331 |  0         | 15,894 |  163 | 3 (unresolved links) |
| bqlite-engine     |  58 |  0         |  3,428 |   24 | 6 (1 link, 4 redundant, 1 private-item) |
| bqlite-cli        |  84 |  0         |  2,054 |    6 | 0 |
| bqlite-ffi        |   0 |  0         |     10 |    0 | 0 |
| bqlite-benches    |  14 |  0         |  5,204 |    — | 1 (unresolved link) |
| bqlite-tests      | 181 (1 ign) |  0 | —  |    — | 0 |

- **Bench harness** compiles cleanly (`cargo bench -p bqlite-benches --no-run` → `Finished bench profile`, builds `src/lib.rs` + `benches/smoke.rs` + 4 Wave 2 benches + 6 Wave 3 benches).
- **Bench CI** wired via `.github/workflows/bench.yml` (TASK-241): baseline capture on main push, regression gate on PRs (>10% on 3 consecutive samples), `bench-skip` label opt-out. All 6 Wave 3 benches registered in baseline/gate/reference jobs.
- **Doc build** succeeds with warnings only (`cargo doc --workspace --no-deps` → `Finished dev profile`).
- **Clippy** clean at `-D warnings` across the workspace (`scripts/local-ci.sh` passing).
- **Formatting** clean at `cargo fmt --all --check`.
- **Dep-direction** check clean (`scripts/check-dep-direction.sh`).
- **End-to-end acceptance**: Wave 3 acceptance test (`tests/wave3_acceptance.rs`, 6 tests) exercises FUNNEL sugar → desugared MATCH+STATS → equivalence check against deterministic 20-entity funnel fixture, canonical `LAST 30d | FUNNEL(...)` query, BETWEEN time-range, EXPLAIN time-range display, all passing.

## Findings

### 1 — Rustdoc warnings grew from 33 to 41 (+8 new, 48 including cargo summary lines)

Warnings by crate:

- **bqlite** (1): output filename collision (persists from Wave 1)
- **bqlite-core** (4): `BqliteError::Execution`, two `try_reserve`, `DESIGN` (persist from Wave 1)
- **bqlite-storage** (18, +2): private-item links in `writer`, `reader`, `merge`, `advise`, `delta`, `dictionary`, `plain`, `Partitioner`, `SegmentFileScan`, `ColumnChunkMeta` module docs — new links for `ScanPlan` and `ColumnChunkMeta`
- **bqlite-parser** (3): private-item links in `lex`, `parser`, `error` module docs (persist from Wave 2)
- **bqlite-planner** (5, +2): private-item links in `expr`, `from_ast` docs + 2 new name-collision warnings (`desugar_funnel`, `fuse_match_aggregate` are both function and module names)
- **bqlite-operators** (3, +2): `BqliteError` link (persists) + 2 new unresolved links (`AggregatePhysical`, `PatternClass`) — references to planner types not in scope
- **bqlite-engine** (6, +1): `ScanPhysical` unresolved + 4 redundant links + 1 new `SequenceMatchAdapter` private-item link
- **bqlite-benches** (1, +1): new unresolved `finish` link

Impact: rendered docs build fine, but broken intra-doc links erode navigation.
New warnings mostly come from (a) function/module name collisions in optimizer
passes (cosmetic — cargo doc resolves to module), (b) cross-crate type
references that rustdoc cannot resolve (planner types referenced in operator
docs). **Not filed as a follow-up task** because the Docs dimension sits at
**B** or above for every affected crate.

### 2 — `bqlite` top-level re-export collision persists

Same as Wave 1/2 Findings. `cargo doc --workspace --no-deps` emits
`warning: output filename collision at target/doc/bqlite/index.html`.
Same disposition — noted, not filed.

### 3 — Benchmark coverage expanded significantly with 6 Wave 3 groups

Wave 3 added 6 dedicated Criterion bench groups under `benches/wave3/`
(matcher, aggregate, sort, distinct, funnel, percentile). Combined with
Wave 2's 4 groups, there are now **10 Criterion bench groups** covering
all perf-critical paths. The bench CI regression gate covers all 10.

Operator-specific coverage:

- **bqlite-operators**: **A-** — all 6 Wave 3 bench groups exercise operator code
  directly (step-counter vs NFA, hash aggregation scaling, DDSketch throughput,
  sort/distinct at multiple row counts) + Wave 2 scan/acceptance benches
- **bqlite-storage**: **A-** — all 4 Wave 2 bench groups (encoding, scan, ingest,
  acceptance) continue to exercise storage code
- **bqlite-engine**: **C+** — funnel end-to-end bench exercises engine transitively
  but no per-crate bench targets

### 4 — Property test coverage expanded with NFA and binding suites

Wave 3 added 11 new workspace-level property tests (89 total, up from 78):
- `prop_nfa.rs` (6 tests): window expiry monotonicity, poison kill completeness,
  epsilon transition reachability, wildcard matching correctness
- `prop_bindings.rs` (5 tests): track identity stability, NULL short-circuit,
  active-track cap enforcement, bind-once-then-check semantics

Combined with the 79 inherited Wave 2 property tests, property-test coverage
continues to match `docs/core-beliefs.md` §11 for codecs, merge guarantees,
and now extends to matcher invariants.

### 5 — Variable-binding E2E integration gap closed (TASK-329)

TASK-329 resolved the variable-binding E2E gap identified in the pre-closure
audit. The planner's `TypedExpr` type-checking now recognizes `$var` references
in MATCH step predicates (via `validate_variable_usage()` in `compile.rs`),
and the binding values propagate from NFA tracks through the output builder
to typed Arrow columns.

The matcher integration test suite (`tests/matcher_integration.rs`, 45 tests)
now includes **8 E2E variable-binding tests**: single-binding, commuted form
(`$var = col`), multi-entity, multi-variable, NULL short-circuit, MATCH ALL
rebinding, mixed binding + negation, and non-equality rejection error. Combined
with the operator-level coverage (32 unit tests in `matcher/bindings.rs` +
5 property tests in `prop_bindings.rs`), variable-binding coverage is now
comprehensive at both the unit and integration levels.

**Impact on grades**: reinforces the **A** Tests grade for bqlite-operators
and strengthens integration confidence for bqlite-planner.

### 6 — Matcher benchmark scenario coverage is partial vs TASK-302 spec

`matcher-strategy.md` §8.1 specifies 9 benchmark scenarios across all
`PatternClass` variants. The current matcher bench (`benches/wave3/matcher.rs`)
covers 2 of 9: `LinearSimple` (step counter) and `GeneralNfa` (forced NFA),
plus MATCH ALL and windowed variants. Missing scenarios include
`LinearImmediate`, `LinearWithNegation`, `LinearWithBindings`, `LinearFull`,
and `GeneralNfa` with repetition.

Additionally, §8.5 requires explicit `PatternClass` assertions on compiled
patterns to prevent benchmark rot — the current benchmarks construct NFAs
with explicit class parameters but do not assert post-compilation.

**Impact on grades**: does not drop the Benchmarks dimension below B+ for
operators — the existing 4 matcher bench functions plus the funnel end-to-end
bench provide meaningful performance coverage. The missing scenarios would
strengthen confidence in per-strategy cost claims but are not required for
the overall Wave 3 performance gate. Worth a follow-up task.

### 7 — bqlite-ffi remains an intentional placeholder

FFI lands in Wave 6; its `C` across every dimension reflects that scope,
not a quality gap. Same disposition as Wave 1/2 findings.

### 8 — CompactString recommendation: conditional go for BindingValue only (TASK-332)

TASK-332 evaluated `compact_str::CompactString` (v0.9) for matcher hot paths.
The full evaluation is at `docs/design/operators/compactstring-evaluation.md`.

**Decision: CONDITIONAL GO** — adopt CompactString for `BindingValue::String`
only. For strings ≤ 24 bytes (the overwhelming majority of analytics event
properties), CompactString clone is ~10× faster than `Box<str>` because
clone is a 24-byte stack memcpy with zero allocator interaction. The remaining
surfaces (`Transition.event_type`, `PoisonTransition.event_type`,
`relevant_event_types`) should keep `String` — they are never cloned in hot
paths and comparison cost is identical across representations.

The recommendation would flip to NO-GO if binding values were routinely > 24
bytes (URLs, free-form text). The `compact_str` crate dependency is
lightweight (MIT licensed, well-maintained, compile-time-only transitive deps).
Migration is not yet applied — a follow-up implementation task should be filed
in Wave 4 if binding-heavy workloads materialize.

### 9 — No crate slipped vs Wave 2 grades

Every crate either maintained or improved its Overall grade from Wave 2.
The largest movements:

| Crate | Wave 2 | Wave 3 | Delta |
|-------|--------|--------|-------|
| bqlite-ast | B+ | **A-** | ↑ |
| bqlite-parser | A- | **A** | ↑ |
| bqlite-planner | A- | **A** | ↑ |
| bqlite-operators | A- | **A** | ↑ |

No crate is below **C** on any dimension. No follow-up tasks required
under the `below-C → file a follow-up` rule.

## Wave 3 status

**Wave 3 is complete.** All 32 tasks in TASK-3xx have `.done` markers in
`tasks/completed/` (TASK-301 through TASK-332, including 5 closure tasks
TASK-328–332); the Wave 3 acceptance gate (`tests/wave3_acceptance.rs`)
passes end-to-end with 6 tests (including the canonical `LAST 30d | FUNNEL(...)`
query); per-crate grades all sit at or above **C** on every dimension; no
crate slipped from its Wave 2 grade. The bench CI regression gate covers
all 10 benchmark groups (4 Wave 2 + 6 Wave 3). Wave 4 can begin.

---

## Wave 2 grades (historical)

*Captured by TASK-299 on 2026-04-12. Preserved for historical reference.*

| Crate | Tests | API | Docs | Benchmarks | Overall |
|-------|-------|-----|------|------------|---------|
| bqlite | **C** · 0 unit, 0 doctest; re-export crate, transitive coverage via re-exported crates | **B-** · re-exports `ast`, `types`, `parser`, `engine`, `BqliteError`, `Result` — complete for Wave 2 | **C+** · crate-level doc with quick-start; 1 rustdoc collision warning persists | **C** · no per-crate benches; workspace harness available | **C+** |
| bqlite-core | **A** · 217 unit + 3 doctest; covers every type including Wave 2 predicate IR (ScanPredicate, ScanConjunct, DictRewrite) *(↑ from 182)* | **A-** · foundational trait surface + types; Wave 2 additive extensions (Predicate zone-map/dict hooks) preserve backward compat | **B+** · exhaustive module docs with design-doc refs; 4 rustdoc warnings persist (intra-doc links) | **C** · no per-crate benches (appropriate — pure types) | **A-** |
| bqlite-ast | **A-** · 49 unit across expr/operator/pattern/pipeline/span/statement *(↑ from 46)* | **B+** · AST node enums for every BQL construct through Wave 2 (DDL, DML, pipeline stages) | **B** · per-module docs on every file | **C** · no per-crate benches | **B+** |
| bqlite-storage | **A** · 417 unit + 79 workspace property tests (encoding roundtrips, zone-map no-false-negatives); 6 encoding impls each with encode/decode roundtrip coverage *(↑ from B+, 33 → 417)* | **A** · complete v1 segment format: reader, writer, 5 encodings + LZ4, k-way merge, encoding selector, CSV ingest, partitioner, zone-map pushdown, orphan cleanup, posix_fadvise *(↑ from B+)* | **A-** · extensive module docs with design-doc §-refs (segment-format-v1.md, predicate-pushdown.md); 16 rustdoc warnings (private-item links in writer/reader docs) *(↑ from B+)* | **A-** · 4 dedicated Wave 2 bench groups (scan, encoding, ingest, acceptance) covering all perf-gate metrics; bench CI wired with regression gate *(↑ from C)* | **A** *(↑ from B+)* |
| bqlite-parser | **A** · 263 unit + 1 doctest; every production has happy-path + error-case coverage; 8 grammar modules fully tested *(↑ from B, 21 → 263)* | **A-** · full Wave 2 grammar: DDL (CREATE/ALTER/DROP/DESCRIBE/EXPLAIN), DML (INSERT FROM/VALUES), pipeline stages (WHERE/SELECT/LIMIT), expression ladder *(↑ from C+)* | **B+** · module docs with grammar-section refs (§26, §20); 3 rustdoc warnings (private-item links) | **C** · no per-crate benches (parser is not perf-critical path) | **A-** *(↑ from B-)* |
| bqlite-planner | **A-** · 173 unit + 1 doctest; type checker, kernel selection, pushdown, pruning, explain all covered *(↑ from B-, 16 → 173)* | **A-** · AST → LogicalPlan → PhysicalPlan pipeline with 2 optimizer passes (pushdown, pruning), expression compilation (Expr → TypedExpr → CompiledExpr), EXPLAIN formatter *(↑ from B-)* | **B+** · module docs with design-doc refs (logical-plan-nodes.md, expression-compilation.md); 3 rustdoc warnings (private-item links) | **C** · no per-crate benches (planner is not hot path; optimizer pass cost is negligible vs storage I/O) | **A-** *(↑ from B-)* |
| bqlite-operators | **A-** · 113 unit; scan/filter/project/limit operators + expression evaluator fully tested including lifecycle, cancellation, error propagation *(↑ from 48)* | **A-** · Wave 2 operator set (ScanOperator, FilterOperator, ProjectOperator, LimitOperator) with tile-based evaluation, zone-map pruning, k-way merge integration *(↑ from B+)* | **B+** · module docs with Wave 1 vs later-wave scoping; 1 rustdoc warning (BqliteError link) | **B+** · scan/filter/project covered by workspace bench groups (scan, acceptance); perf-gate metrics exercised *(↑ from C)* | **A-** *(↑ from B+)* |
| bqlite-engine | **B+** · 51 unit covering parse → plan → bind → drive + DDL execution + INSERT FROM/VALUES + rendering with truncation *(↑ from B, 16 → 51)* | **B+** · `Engine::query`, DDL execution (CREATE/DROP/ALTER), DML execution (INSERT FROM/VALUES), DESCRIBE/EXPLAIN, result rendering with auto-limit *(↑ from B)* | **B** · module docs present; **5 rustdoc warnings** persist (1 unresolved `ScanPhysical`, 4 redundant links) *(↑ from B-)* | **C+** · no per-crate benches; covered transitively by workspace acceptance bench | **B+** *(↑ from B-)* |
| bqlite-cli | **A-** · 80 unit covering 3 subcommands (init, query, ingest), auto-limit machinery, argument parsing, end-to-end flows *(↑ from B+, 16 → 80)* | **A-** · `bqlite init`, `bqlite query` with `--limit`/`--no-limit`, `bqlite ingest` with `--map`/`--format`; DDL bypass for auto-limit; exit codes 0/1/2 *(↑ from B+)* | **B+** · extensive module docs covering arg parsing, exit codes, architecture rule; clean rustdoc (0 warnings) | **C** · no per-crate benches (CLI frontend not perf-critical) | **A-** *(↑ from B+)* |
| bqlite-ffi | **C** · 0 unit tests — appropriate for Wave 1 scope; FFI is Wave 6 | **C** · module docs enumerate intended PyO3 surface; no implementation yet | **C** · crate-level doc explains intent and placement | **C** · no benches — out-of-scope | **C** |

Wave 2 evidence aggregate: **1,479 passing tests**, **4 ignored**, **0 failing**. **4 Criterion bench groups**.

### Wave 2 findings (historical)

1. Rustdoc warnings grew from 11 to 33 (minor)
2. `bqlite` top-level re-export collision persists
3. Benchmark dimension differentiates by coverage
4. Property test coverage expanded to 79 workspace tests
5. `bqlite-ffi` remains intentional placeholder
6. No crate slipped vs Wave 1

---

## Wave 1 grades (historical)

*Captured by TASK-199 on 2026-04-11. Preserved for historical reference.*

| Crate | Tests | API | Docs | Benchmarks | Overall |
|-------|-------|-----|------|------------|---------|
| bqlite | **C** · 0 unit, 0 doctest; re-export crate, transitive coverage via re-exported crates | **B-** · re-exports `ast`, `types`, `parser`, `engine`, `BqliteError`, `Result` — minimal but complete for Wave 1 | **C+** · crate-level doc with quick-start; 1 rustdoc collision warning (`target/doc/bqlite/index.html`) shared with `bqlite_core::bqlite` module | **C** · no per-crate benches; workspace harness available | **C+** |
| bqlite-core | **A** · 182 unit + 3 doctest; covers every type (Event, Schema, Timestamp, PropertyValue, Memory, Metrics, Demand, SegmentReader) | **A-** · foundational trait surface + types for every downstream crate; Wave 1 contracts frozen | **B+** · exhaustive module docs with design-doc refs; 4 rustdoc warnings (unresolved intra-doc links) | **C** · no per-crate benches (none needed for pure types yet) | **A-** |
| bqlite-ast | **A-** · 46 unit across expr/operator/pattern/pipeline/span/statement | **B+** · AST node enums for every BQL construct except advanced Wave 4+ features | **B** · per-module docs on every file | **C** · no per-crate benches | **B+** |
| bqlite-storage | **B+** · 33 unit across database/manifest/catalog/locking | **B+** · `Database::open_or_create` + atomic manifest + flock + ManifestCatalog + SegmentReader stub | **B+** · full module docs + design-doc refs; 1 ignored doctest (by design — `Database::catalog` sample is compile-only until Wave 2) | **C** · no per-crate benches | **B+** |
| bqlite-parser | **B** · 21 unit + 1 doctest | **C+** · Wave 1 grammar accepts a single bare identifier; full grammar is Wave 2 | **B** · module docs with grammar scope explained | **C** · no per-crate benches | **B-** |
| bqlite-planner | **B-** · 16 unit + 1 doctest | **B-** · `plan(stmt, &dyn Catalog)` → logical `Scan` → plain-data `PhysicalPlan`; no optimizer yet (by design, Wave 2) | **B-** · module docs cover Wave 1 scope and planner-pipeline.md §15 rationale | **C** · no per-crate benches | **B-** |
| bqlite-operators | **A-** · 48 unit (`PhysicalOperator`/`EntityOperator` traits + scan/filter/project stubs) including cancellation, error propagation, empty-segment skip, composition | **B+** · Wave 1 operator trait surface + stubs; real filter/project IR lands Wave 2 | **B+** · module docs with Wave 1 vs later-wave scoping; 1 rustdoc warning (unresolved `BqliteError` link) | **C** · no per-crate benches | **B+** |
| bqlite-engine | **B** · 16 unit covering parse → plan → bind → drive happy path and error shapes | **B** · `Engine::query`, `Database` re-export, `format_result_as_text`, bind step; single text-in, rows-out surface | **B-** · module docs present; **5 rustdoc warnings** (1 unresolved `ScanPhysical`, 4 redundant explicit link targets) | **C** · no per-crate benches | **B-** |
| bqlite-cli | **B+** · 16 unit (arg parsing, dispatcher, end-to-end query) + 3 workspace smoke tests (spawn binary, pin `(0 rows)` footer, negative path for unknown table) | **B+** · `bqlite query <bql> --db <path>` with `--db=path` long-form, exit codes `0`/`1`/`2` split by error kind | **B+** · extensive main.rs module doc covering arg parsing, exit codes, future-subcommand list, architecture rule | **C** · no per-crate benches (nothing perf-critical in a CLI frontend yet) | **B+** |
| bqlite-ffi | **C** · 0 unit tests — **appropriate for Wave 1 scope**; FFI is Wave 6 | **C** · module docs enumerate the intended PyO3 surface; no implementation yet (deliberately) | **C** · crate-level doc explains intent and placement in architecture | **C** · no benches — wave-out-of-scope | **C** |

Evidence aggregate (Wave 1): **398 passing unit + integration tests**, **5 passing doctests**, **2 ignored doctests**, **0 failing tests**.

### Wave 1 findings (historical)

1. Rustdoc intra-doc link warnings (10 total, minor)
2. `bqlite` top-level re-export collides with `bqlite_core::bqlite` module
3. Universal "C" on Benchmarks was a harness-scoping artifact
4. `bqlite-ffi` was an intentional placeholder
5. Smoke-test stale-binary foot-gun (environmental, not a grade issue)
