# Quality Score

Per-crate quality grades, updated at the close of each wave. The most
recent pass is the **Wave 5 audit** (TASK-599, `2026-05-02`).

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

## Wave 5 grades

Per-cell grades carry a one-line justification. Evidence is collected
below the table. Grade changes from Wave 4 are annotated with arrows.

| Crate | Tests | API | Docs | Benchmarks | Overall |
|-------|-------|-----|------|------------|---------|
| bqlite | **B** · 4 doctests (2 in `README.md`, 1 on `BqliteError` re-export, 1 on `Result` re-export); 0 unit tests (re-export crate — appropriate) *(↑ from C — TASK-544 doctest uplift)* | **B-** · re-exports `ast`, `types`, `parser`, `engine`, `BqliteError`, `Result` — complete for Wave 5 | **B** · `include_str!`-driven crate doc with re-export table and compilable usage examples; 1 rustdoc collision warning persists *(↑ from C+ — TASK-544 README uplift)* | **C** · no per-crate benches; workspace harness available; accepted per TASK-544(d) workspace-bench decision | **B** *(↑ from C+)* |
| bqlite-core | **A** · 331 unit + 3 doctest; adds Wave 5 runtime types — `QueryWarning`/`WarningSeverity`, `MemoryBudget`/`MemoryReservation`/`MemoryTracker`/`SpillNotification`, `CancellationToken`, `TempSpillFile`, `BqliteError::{Timeout, OperatorPanic, WarningsOverflow}` *(↑ from 267)* | **A-** · foundational trait surface + types; Wave 5 additive (memory budget, warning channel, cancellation, spill RAII, structured error variants) preserves backward compat | **B+** · exhaustive module docs with design-doc refs; 8 rustdoc warnings (6 carried + 2 new `try_reserve` / `TombstoneScanWrapper` cross-crate links) *(warnings 6 → 8; grade unchanged)* | **C** · no per-crate benches (appropriate — pure types) | **A-** |
| bqlite-ast | **A-** · 54 unit across expr/operator/pattern/pipeline/span/statement *(unchanged from Wave 4 — no AST surface changes in Wave 5)* | **A-** · AST node enums for every BQL construct through Wave 4; Wave 5 added no new node variants | **B** · per-module docs on every file; 0 rustdoc warnings | **C** · no per-crate benches | **A-** |
| bqlite-storage | **A** · 878 unit + workspace encoding/integration property suites *(↑ from 814)* — adds `tombstone_scan`, `encoded_tombstone`, sort spill-run files, partitioner spill, system-column projection paths | **A** · v2 segment format unchanged; Wave 5 adds `EntityIn` cohort-pushdown plumbing into scan, `__seq_id` / `__batch_id` materialization, late-materialization boundary helpers per `zero-copy-scan-filter.md`, `SpillRunFile` writer + sort-merge consumer surface | **B+** · extensive module docs with design-doc §-refs; **35 rustdoc warnings** (32 carried + 3 new private-item links in spill, partitioner, tombstone-with-index) *(warnings 32 → 35; grade unchanged)* | **A** · 9 Wave 4 + Wave 2 storage benches; Wave 4 follow-up `tombstone_scan` (TASK-534) + Wave 5 `zero_copy_scan` + `cohort_pushdown` provide storage-side perf evidence | **A** |
| bqlite-parser | **A** · 574 unit + 1 doctest *(unchanged from Wave 4 — no grammar additions in Wave 5)* | **A** · full Wave 4 grammar; Wave 5 added no new productions | **A-** · module docs with grammar-section refs and design-doc cross-references; 3 rustdoc warnings persist (private-item links, unchanged) | **C** · no per-crate benches (parser is not perf-critical path) | **A** |
| bqlite-planner | **A** · 530 unit + 1 doctest *(↑ from 429)* — adds optimizer framework + `RuleTrace` (TASK-521), `coalesce_scan_predicates` / `filter_order` / `sample_pushdown` / scan-adjacent rule pack (TASK-527), cohort entity pushdown (TASK-522), narrow heuristic gating per `optimizer-direction.md` | **A** · `Optimizer` framework + `OptimizerRule` registry + per-rule policy matrix per TASK-504; cohort-to-`EntityIn` pushdown into `ScanPhysical`; fused-segment lowering hooks; `PlannerStats` snapshot wired through bind | **B** · module docs with design-doc refs (optimizer-direction.md, demand-protocol.md, planner-pipeline.md); **23 rustdoc warnings** (10 carried + 13 new across the rule pack: `coalesce_scan_predicates` function/module collision, `filter_order`-private `rank`, registry private `finalize_physical`, `clamp_filter_tile_size`, `pre_fusion_output_schema`, `bind_cohorts`, `crate::opt::fuse_match_aggregate` collision) *(warnings 10 → 23; ↓ B+ → B)* | **C** · no per-crate benches (planner is not hot path) | **A-** *(Overall preserved; Docs sub-grade ↓ B+ → B)* |
| bqlite-operators | **A** · 603 unit *(↑ from 537)* — adds `FusedSegmentPhysical` driver (TASK-518), Filter/Project/Limit kernel refactor (TASK-519), stateful-aggregate fusion for Sessionize/EventSelect/Attribute (TASK-520), encoded-filter zero-copy path (TASK-516), `__seq_id` / `__batch_id` materialization (TASK-508), sort spill (TASK-513), partitioner spill (TASK-512), cohort entity pushdown (TASK-522) | **A** · complete Wave 5 fused/zero-copy surface: `FilteredBatch` + `SelectionVector` + `StatelessKernel` trait, `FusedSegmentPhysical` push-segment driver, `MaterializeTrigger` (sparsity / segment-boundary / aggregate-handoff), inline accumulator path on Sessionize/EventSelect/Attribute, `EntityInFilter` / cohort-pushdown predicate, `SortOperator::with_spill` + `TempSpillFile` lifecycle, system-column projection on `ScanOperator` + `MergeSourcesOperator` | **B+** · every module has design-doc cross-references (operator-fusion.md, system-columns.md, spill.md, cancellation.md); **10 rustdoc warnings** (6 carried + 4 new: `pre_fusion_output_schema`, `clamp_filter_tile_size`, `SortSpillHandler`, `SubqueryFilterOperator`) *(warnings 6 → 10; grade unchanged)* | **A** · 11 Wave 3+4 operator benches + Wave 5 `fused_segment`, `stateful_aggregate_fusion`, `morsel_skew`, `spill_overhead`, `cohort_pushdown`, `tombstone_scan` — 16 dedicated operator/storage bench groups exercise Wave 5 perf evidence | **A** |
| bqlite-engine | **A** · 205 unit *(↑ from 105)* — adds memory tracker scaffold (TASK-510), warning channel (TASK-511), spill cleanup wiring (TASK-502), morsel scheduler integration (TASK-506, TASK-523), CPU/skew metrics + `--explain-perf` (TASK-524), cancellation token wiring (TASK-505), fusion bind plumbing | **A** · `QueryContext` carries memory tracker + warning sink + spill root + cancellation token; `Engine::query` returns `peak_memory_bytes` + drained warnings; morsel scheduler with adaptive halving, lock-free MPMC queue, FIFO `CoreBudget` queuing; `--explain-perf` rendering | **B** · module docs present; **13 rustdoc warnings** (8 carried + 5 new: `SpillCleanup`, `CoreBudget`, `cycles_per_event`, `Mutex`, `finish` private-item links from scheduler / context / perf modules) *(warnings 8 → 13; grade unchanged)* | **C+** · no per-crate benches; covered transitively by workspace `wave5_acceptance` + `wave5_runtime_stress` + Wave 5 `morsel_skew` / `spill_overhead` benches *(unchanged)* | **A-** *(↑ from B+ — Tests A and API A pull Overall up)* |
| bqlite-cli | **A-** · 90 unit covering 3 subcommands + Wave 5 `--explain-perf` rendering + auto-limit machinery *(↑ from 84)* | **A-** · CLI surface stable; `bqlite query` exposes Wave 5 features (warnings, `--explain-perf`) via `Engine::query` return | **B+** · extensive module docs; clean rustdoc (0 warnings) | **C** · no per-crate benches (CLI frontend not perf-critical) | **A-** |
| bqlite-ffi | **C** · 0 unit tests — appropriate; FFI is Wave 6 | **C** · module docs enumerate intended PyO3 surface; no implementation yet | **C** · crate-level doc explains intent and placement | **C** · no benches — out-of-scope | **C** |

**Workspace-level test artifacts** (`bqlite-tests` + `bqlite-benches` workspace crates):

| Target | Count | Purpose |
|--------|------:|---------|
| `tests/src/` unit | 21 | Fixture framework (`common.rs`, `csv.rs`, `jsonl.rs`, `strategies.rs`) |
| `tests/common_smoke.rs` | 13 | Integration fixture framework (TASK-120) |
| `tests/demand_contract.rs` | 4 | DemandCapabilities protocol contract tests (TASK-409, TASK-427) |
| `tests/fused_segment_bind.rs` | 1 | **NEW** Fused-segment binding contract (TASK-518/519) |
| `tests/jsonl_ingest.rs` | 7 (1 ign) | JSONL ingest end-to-end tests (TASK-410) |
| `tests/matcher_integration.rs` | 56 | Matcher integration suite (TASK-324, TASK-329) |
| `tests/prop_arrow.rs` | 1 | Arrow ↔ BqlType round-trip property test |
| `tests/prop_attribute.rs` | 7 | ATTRIBUTE operator property tests (TASK-431) |
| `tests/prop_bindings.rs` | 5 | Variable-binding property tests |
| `tests/prop_encoding_alp.rs` | 8 | ALP encoding property tests (TASK-417) |
| `tests/prop_encoding_bitpacking.rs` | 11 | BitPacking encoding property tests |
| `tests/prop_encoding_constant.rs` | 9 | Constant encoding property tests |
| `tests/prop_encoding_delta.rs` | 10 | Delta encoding property tests |
| `tests/prop_encoding_dictionary.rs` | 15 | Dictionary encoding property tests |
| `tests/prop_encoding_double_delta.rs` | 14 | DoubleDelta encoding property tests (TASK-414) |
| `tests/prop_encoding_for.rs` | 20 | FOR encoding property tests (TASK-415) |
| `tests/prop_encoding_fsst.rs` | 18 | FSST encoding property tests (TASK-416) |
| `tests/prop_encoding_pfor.rs` | 20 | PFOR encoding property tests (TASK-450) |
| `tests/prop_encoding_plain.rs` | 13 | Plain encoding property tests |
| `tests/prop_encoding_rle.rs` | 28 | RLE encoding property tests (TASK-413) |
| `tests/prop_event_select.rs` | 8 | EventSelect candidate-row property tests (TASK-429, extended in TASK-531) |
| `tests/prop_nfa.rs` | 7 | NFA simulator property tests |
| `tests/prop_property_value.rs` | 12 | `PropertyValue` round-trip property tests |
| `tests/prop_time.rs` | 7 | TimeRange intersection/shift property tests |
| `tests/smoke.rs` | 8 | Wave 1+2 acceptance gates |
| `tests/warning_channel.rs` | 3 | **NEW** `QueryWarning` cap + drain + overflow contract (TASK-511) |
| `tests/wave2_acceptance.rs` | 9 (1 ign) | Wave 2 acceptance gate |
| `tests/wave3_acceptance.rs` | 6 | Wave 3 acceptance gate |
| `tests/wave4_acceptance.rs` | 5 (1 ign) | Wave 4 acceptance gate (TASK-442) |
| `tests/wave4_advanced_analytics_attribute.rs` | 5 | **NEW** ATTRIBUTE composition + WITHIN SESSION integration (TASK-532) |
| `tests/wave4_advanced_analytics_attribute_cohort_join.rs` | 9 (1 ign) | ATTRIBUTE + cohort + joined-source integration |
| `tests/wave4_advanced_analytics_event_select.rs` | 14 | FIRST/LAST/NTH + SAMPLE + RETENTION integration |
| `tests/wave4_advanced_analytics_sessionize.rs` | 12 | SESSIONIZE + `WITHIN SESSION` integration *(2 previously-ignored tests un-ignored after TASK-510 retirement; +4 new)* |
| `tests/wave4_delete_compaction.rs` | 17 | DELETE + tombstone + compaction integration |
| `tests/wave5_acceptance.rs` | 9 | **NEW** Wave 5 acceptance gate (TASK-528) — multi-shard analytical query under documented budget, cancellation/timeout cleanup, sort/cohort spill policy, fused/zero-copy result equivalence |
| `tests/wave5_cohort_pushdown.rs` | 7 | **NEW** cohort entity pushdown correctness (TASK-522) |
| `tests/wave5_runtime_stress.rs` | 19 | **NEW** runtime stress: budget exhaustion, cancellation cleanup, snapshot isolation, spill fallback, warning overflow (TASK-525) |
| `tests/wave5_system_columns.rs` | 13 | **NEW** `__seq_id` / `__batch_id` materialization + delete + compaction invariance (TASK-508, TASK-509) |
| `benches/benches/smoke.rs` | — | Criterion harness smoke |
| `benches/wave2/scan.rs` | — | Columnar decode throughput |
| `benches/wave2/scan_encoded.rs` | — | Encoded-batch scan throughput |
| `benches/wave2/encoding.rs` | — | Per-encoding encode/decode microbenches |
| `benches/wave2/ingest.rs` | — | CSV ingest throughput |
| `benches/wave2/acceptance.rs` | — | Full round-trip: ingest → write segments → read segments |
| `benches/wave2/fused_segment.rs` | — | **NEW** `FusedStatelessSegment` operator-level microbench (TASK-519) |
| `benches/wave3/matcher.rs` | — | Step-counter vs NFA strategy comparison |
| `benches/wave3/aggregate.rs` | — | Hash aggregation throughput |
| `benches/wave3/sort.rs` | — | Sort operator |
| `benches/wave3/distinct.rs` | — | Distinct operator |
| `benches/wave3/funnel.rs` | — | End-to-end 3-step funnel |
| `benches/wave3/percentile.rs` | — | DDSketch insert/quantile/merge |
| `benches/wave3/compactstring_eval.rs` | — | CompactString microbench (TASK-332) |
| `benches/wave4/sessionize.rs` | — | SessionizeOperator throughput *(extended in TASK-535 with multi-end-event-type matrix)* |
| `benches/wave4/attribute.rs` | — | AttributeOperator deque/ratio throughput |
| `benches/wave4/event_select.rs` | — | EventSelect FIRST/LAST/NTH throughput *(hard targets pinned in TASK-531)* |
| `benches/wave4/pfor.rs` | — | PFOR codec encode/decode throughput |
| `benches/wave4/encoding_matrix.rs` | — | Wave 4 encoding comparison matrix |
| `benches/wave4/ingest.rs` | — | JSONL + Parquet ingest throughput |
| `benches/wave4/compaction.rs` | — | L0-to-L1 compaction throughput |
| `benches/wave4/sample.rs` | — | SAMPLE pushdown determinism |
| `benches/wave4/cohort_join.rs` | — | `SubqueryFilterOperator` probe + `MergeSourcesOperator` k-way merge |
| `benches/wave4/tombstone_scan.rs` | — | **NEW** Query-time tombstone-filter throughput + compaction-time density overhead (TASK-534) |
| `benches/wave5/zero_copy_scan.rs` | — | **NEW** Copy-budget bench: `bytes_materialized_before_filter` / `bytes_decompressed` (TASK-526 CP1) |
| `benches/wave5/stateful_aggregate_fusion.rs` | — | **NEW** SESSIONIZE → STATS fusion vs fallback regression tripwire (TASK-526 CP2) |
| `benches/wave5/morsel_skew.rs` | — | **NEW** Morsel-scheduler wall-clock regression on skewed-vs-balanced entity distribution (TASK-526 CP3) |
| `benches/wave5/spill_overhead.rs` | — | **NEW** `SortOperator::with_spill` overhead vs in-memory baseline (TASK-526 CP4) |
| `benches/wave5/cohort_pushdown.rs` | — | **NEW** Bytes-scanned savings of `EntityIn` pushdown vs probe-only (TASK-526 CP5) |

**Evidence aggregate**: **3,730 passing tests** via `cargo test --workspace --all-targets` (3,265 per-crate library unit + 14 bench-crate unit + 451 workspace integration/property), **4 ignored**, **0 failing tests**. Separately, `cargo test --workspace --doc` adds **9 passing doctests** and **5 ignored doctests** (TASK-544 added 4 `bqlite` doctests: 2 in `README.md`, 1 on `BqliteError`, 1 on `Result`). Of the workspace suite, **213 are property tests** covering 11 encoding codecs (Plain, Dictionary, Delta, DoubleDelta, BitPacking, Constant, RLE, FOR, PFOR, FSST, ALP), PropertyValue coercion, TimeRange algebra, Arrow type mapping, NFA simulator invariants, variable binding semantics, ATTRIBUTE deque and window-boundary rules, and EventSelect candidate-row behavior. **29 Criterion bench groups** now cover Wave 2 (6, including new `fused_segment`), Wave 3 (7), Wave 4 (10, including new `tombstone_scan`), and Wave 5 (5) performance gates plus the Wave 1 smoke bench.

## Evidence

Gathered from `cargo test -p <crate>`, `cargo test --workspace --all-targets`,
`cargo bench -p bqlite-benches --no-run`, `cargo doc --workspace --no-deps`,
and `find crates/<crate>/src -name '*.rs'`. The `pub` items column counts
items matching `^\s*pub (fn|struct|enum|trait|type|const|static|mod) ` —
the same methodology used Wave-over-Wave; small per-crate drift versus
prior tables reflects internal `pub(crate)`-vs-`pub` rebalancing inside
otherwise-stable APIs, not surface removal.

| Crate | Unit tests | Doctests | LOC (src) | `pub` items | Rustdoc warnings |
|-------|-----------:|---------:|----------:|------------:|-----------------:|
| bqlite            |   0 |  4 (1 ign) |     35 |    5 | 1 (output collision) |
| bqlite-core       | 331 |  3         | 10,436 |  217 | 8 (intra-doc + new `try_reserve` / `TombstoneScanWrapper`) |
| bqlite-ast        |  54 |  0         |  2,374 |   66 | 0 |
| bqlite-storage    | 878 |  0 (1 ign) | 44,556 |  331 | 35 (private-item links across spill / partitioner / tombstone) |
| bqlite-parser     | 574 |  1         | 13,439 |    6 | 3 (private-item links, unchanged since Wave 2) |
| bqlite-planner    | 530 |  1         | 28,375 |  194 | 23 (+13 Wave 5: optimizer rule pack name collisions + private-item links) |
| bqlite-operators  | 603 |  0         | 32,425 |  273 | 10 (+4 Wave 5: `pre_fusion_output_schema`, `clamp_filter_tile_size`, `SortSpillHandler`, `SubqueryFilterOperator`) |
| bqlite-engine     | 205 |  0         | 11,865 |  165 | 13 (+5 Wave 5: `SpillCleanup`, `CoreBudget`, `cycles_per_event`, `Mutex`, `finish`) |
| bqlite-cli        |  90 |  0         |  2,232 |    6 | 0 |
| bqlite-ffi        |   0 |  0         |     10 |    0 | 0 |
| bqlite-benches    |  14 |  0         |  — |    — | 1 (unresolved link to `docs/design/engine/operator-fusion.md`) |
| bqlite-tests      | 451 (1 ign) |  0 | —  |    — | 0 |

- **Bench harness** compiles cleanly (`cargo bench -p bqlite-benches --no-run` → `Finished bench profile`, 29 bench targets registered in `benches/Cargo.toml`).
- **Bench CI** (TASK-241, updated by TASK-543) runs baseline capture on `main` push and the regression gate on PRs. All 29 wave-scoped bench groups are now invoked by `.github/workflows/bench.yml`: Wave 1 (`smoke`), Wave 2 (`scan`, `scan_encoded`, `encoding`, `ingest`, `acceptance`, `fused_segment`), Wave 3 (`matcher`, `aggregate`, `wave3_sort`, `wave3_distinct`, `funnel`, `percentile`, `compactstring_eval`), Wave 4 (`sessionize`, `attribute`, `event_select`, `pfor`, `encoding_matrix`, `wave4_ingest`, `compaction`, `sample`, `cohort_join`, `tombstone_scan`), and Wave 5 (`zero_copy_scan`, `stateful_aggregate_fusion`, `morsel_skew`, `spill_overhead`, `cohort_pushdown`). The `bench-compare.sh` 10% × 3-consecutive-sample threshold scales to 29 groups without changes — it walks `*/new/estimates.json` files independently, so adding groups does not affect per-metric logic. Timeout raised from 45 → 90 minutes for `bench-baseline` and `bench-gate` to accommodate the expanded suite.
- **Doc build** succeeds with warnings only (`cargo doc --workspace --no-deps` → `Finished dev profile`).
- **Clippy** clean at `-D warnings` across the workspace (`scripts/local-ci.sh` passing).
- **Formatting** clean at `cargo fmt --all --check`.
- **Dep-direction** check clean (`scripts/check-dep-direction.sh`).
- **End-to-end acceptance**: Wave 5 acceptance test (`tests/wave5_acceptance.rs`, 9 tests) exercises the four bands the task definition pins — multi-shard analytical query under the documented `MIN_QUERY_BUDGET_BYTES` floor, cancellation/timeout cleanup, sort/cohort spill policy with no spill artefacts after return, and fused/zero-copy answer-equivalence against hand-computed ground truth. All tests pass; no Wave 5 ignored tests added.
- **Wave 5 design-doc inputs**: TASK-501 (`engine/memory-budget.md`), TASK-502 (`engine/spill.md`), TASK-503 (`engine/operator-fusion.md`), TASK-504 (`planner/optimizer-direction.md`), TASK-505 (`engine/cancellation.md`), TASK-506 (`engine/morsel-scheduler.md`), TASK-508 (`storage/system-columns.md`) — all six steady-state-engine design notes landed before their dependent `[IMPL]` tasks and continue to be cited from operator/engine module docs.

## Findings

### 1 — Rustdoc warnings grew from 67 to 94 (+27 new; trajectory Wave 2: 33 → Wave 3: 41 → Wave 4: 67 → Wave 5: 94) — resolved by TASK-542

Warnings by crate:

- **bqlite** (1, unchanged): output filename collision with `bqlite_core::bqlite` module
- **bqlite-core** (8, +2): two new cross-crate references — `try_reserve` (×2) and `bqlite_storage::TombstoneScanWrapper` (×2) — added when memory-tracker prose started referencing reservation helpers and tombstone-scan integration
- **bqlite-storage** (35, +3): largest single contributor again; new private-item links in `Partitioner` (`estimated_event_size`), `with_spill_dir` (`SpillRunFile`), and `entity_delete_index` (`TombstoneFilter::apply_entity_deletes_with_index`) added by Wave 5 spill + cohort pushdown plumbing
- **bqlite-parser** (3, unchanged): private-item links in `lex`, `parser`, `error` module docs persist from Wave 2
- **bqlite-planner** (23, +13): the largest *delta* this wave; optimizer-framework rule pack added function/module collisions on `coalesce_scan_predicates` and `crate::opt::fuse_match_aggregate`, plus private-item links from `order_stateless_filters` (`rank`), `registry` (`crate::finalize_physical`), `expr` / `from_ast` (`type_error`), and unresolved cross-crate links to `clamp_filter_tile_size`, `pre_fusion_output_schema` (×3), `bind_cohorts` (×2), `DeleteFilter`, and `bqlite_storage::SampleFilter`
- **bqlite-operators** (10, +4): four new unresolved cross-crate or private-item references — `pre_fusion_output_schema`, `clamp_filter_tile_size`, `SortSpillHandler` (×2), `SubqueryFilterOperator` — introduced by the fused-segment scaffolding and sort-spill wiring
- **bqlite-engine** (13, +5): private-item links to `SpillCleanup`, `CoreBudget`, `cycles_per_event`, `Mutex`, `finish` from the new scheduler/perf/context surface
- **bqlite-benches** (1, unchanged)

Impact: rendered docs build fine, but the trajectory (33 → 41 → 67 → 94) continues to drift wrong. The Wave 4 audit warned this would happen if Wave 5 added another ~25 warnings without a cleanup pass — that prediction held. The bqlite-planner Docs grade slipped one sub-grade (B+ → B) at the Wave 4 audit's stated threshold; bqlite-operators stays at B+ but a Wave 6 cleanup pass that swaps private-item links for plain back-ticks and re-aliases the `coalesce_scan_predicates` / `fuse_match_aggregate` function/module collisions would reverse the drift cheaply. **Not filed as a follow-up task** under the *below-C → file a follow-up* rule because the Docs dimension still sits at **B** or above for every affected crate; flagged explicitly so Wave 6 can absorb the cleanup. If Wave 6 adds another ~25 warnings without a cleanup pass, bqlite-storage Docs will drop below B+ and bqlite-engine Docs will likely follow.

**TASK-542 resolution (Wave 6):** All 93 doc-comment warnings fixed — private-item links converted to plain backticks, `coalesce_scan_predicates` / `fuse_match_aggregate` / `desugar_funnel` / `desugar_retention` function/module collisions resolved with `mod@` disambiguation, redundant explicit link targets simplified. `cargo doc --workspace --no-deps` now emits 1 warning (the persistent bqlite/bqlite-cli filename collision per Cargo bug #6313, not a doc-comment issue). CI gate added to `scripts/local-ci.sh` and `.github/workflows/ci.yml` via `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`. Trajectory reset: Wave 6 baseline = 1.

### 2 — `bqlite` top-level re-export collision persists

Same as Wave 1/2/3/4 Findings. `cargo doc --workspace --no-deps` emits
`warning: output filename collision at target/doc/bqlite/index.html`.
This is a Cargo-level warning caused by the `bqlite` binary (in `bqlite-cli`) and the `bqlite` library sharing the same output filename — a known Cargo bug (#6313). `#[doc(no_inline)]` was added to the crate-level re-exports in `crates/bqlite/src/lib.rs` (TASK-542) but does not suppress this cargo-level warning (which is distinct from rustdoc-level warnings). The `RUSTDOCFLAGS="-D warnings"` CI gate introduced by TASK-542 does not fail on this warning because it is emitted by cargo, not rustdoc. Disposition: **expected and documented**; the CI gate treats it as the Wave 6 baseline (1 warning).

### 3 — Benchmark coverage expanded with 5 Wave 5 groups + 2 Wave 4 follow-ups

Wave 5 added 5 dedicated Criterion bench groups under `benches/wave5/`
(`zero_copy_scan`, `stateful_aggregate_fusion`, `morsel_skew`,
`spill_overhead`, `cohort_pushdown` — all under TASK-526), plus 2
Wave 4 follow-ups that completed in Wave 5: `tombstone_scan`
(TASK-534, registered as a Wave 4 bench) and `fused_segment`
(TASK-519, registered as a Wave 2 bench because it exercises the
filter/project/limit kernels). Combined with prior waves there are
now **29 Criterion bench groups** (including the Wave 1 `smoke`)
covering every perf-critical path that Wave 5 ships.

Coverage by perf surface:

- **Zero-copy / fusion**: `zero_copy_scan` reports
  `bytes_materialized_before_filter` and `bytes_decompressed` against
  the `zero-copy-scan-filter.md` § 3 targets;
  `stateful_aggregate_fusion` and `fused_segment` exercise the
  Sessionize/EventSelect/Attribute → STATS handoff and the stateless
  kernel chain respectively.
- **Spill**: `spill_overhead` measures `SortOperator::with_spill`
  against the in-memory baseline; ingest-partitioner spill is
  exercised by the `wave4_ingest` group at large input sizes.
- **Morsel scheduler**: `morsel_skew` is a wall-clock regression
  tripwire on skewed-vs-balanced entity distribution; per the
  bench's own README, the v1 morsel scheduler records all-zero
  per-worker snapshots so `entity_event_skew_p99` is not yet
  usefully assertable — the bench guards the wall-clock axis only.
- **Cohort pushdown**: `cohort_pushdown` compares `bytes_scanned`
  with and without the `EntityIn` conjunct.
- **Tombstones**: `tombstone_scan` covers query-time
  `TombstoneFilter::filter_batch_with_index` throughput and
  compaction-time `Database::compact_now` overhead at 1% / 5% / 10%
  density.

Operator-specific coverage:

- **bqlite-operators**: **A** — 16 dedicated operator benches across Waves 3–5 (matcher, aggregate, sort, distinct, funnel, percentile, sessionize, attribute, event_select, sample, cohort_join, fused_segment, stateful_aggregate_fusion, morsel_skew, spill_overhead, cohort_pushdown)
- **bqlite-storage**: **A** — 10 dedicated storage benches (scan, scan_encoded, encoding, ingest, acceptance, pfor, encoding_matrix, wave4_ingest, compaction, tombstone_scan)
- **bqlite-engine**: **C+** — still covered transitively; no per-crate bench targets

### 4 — Property-test count held flat at 213 while Wave 5 added structural integration coverage

Wave 5 did not add new property suites — the Wave 4 codec / operator /
deque-and-window proptest matrix already covers the testable
invariants of the steady-state engine, and Wave 5's additions are
predominantly stateful runtime behavior (memory budget, spill,
cancellation, morsel scheduling) that is poorly served by random
input and well-served by deterministic stress tests.

The structural integration coverage that landed instead:

- `tests/wave5_acceptance.rs` (9 tests, TASK-528) — four-band acceptance gate
- `tests/wave5_runtime_stress.rs` (19 tests, TASK-525) — budget exhaustion, cancellation cleanup, snapshot isolation, spill fallback, warning overflow
- `tests/wave5_system_columns.rs` (13 tests, TASK-508/509) — `__seq_id` / `__batch_id` end-to-end including delete + compaction invariance
- `tests/wave5_cohort_pushdown.rs` (7 tests, TASK-522) — cohort entity pushdown correctness under multi-column / aggregate-body / empty-cohort edge cases
- `tests/warning_channel.rs` (3 tests, TASK-511) — `QueryWarning` cap + drain + overflow contract
- `tests/fused_segment_bind.rs` (1 test, TASK-518/519) — fused-segment binding contract

Combined with the inherited 213-test property surface, this brings the
workspace integration/property total to **451** (vs Wave 4's **389**),
a +62 net gain entirely on the integration axis. The
`prop_event_select` suite was extended in TASK-531 (no count change —
generator strengthening, not new tests).

### 5 — `optimizer-direction.md` reconciliation lands under Wave 5 (TASK-504, TASK-521, TASK-527)

`planner-pipeline.md`'s Wave 0 "rule-based only" v1 promise is
preserved. TASK-504 froze the policy: rule-based architecture intact,
narrow heuristic gating admitted, five-source `PlannerStats`
snapshot (catalog aggregates, index registry booleans, cohort sizes),
explicit non-sources (no NDV sketches, no histograms, no selectivity
functions), per-rule policy matrix for Passes 1–10, plan-time vs
post-cohort phase split, EXPLAIN visibility contract.

The framework lands under TASK-521 (`Optimizer` driver + per-rule
policy + `RuleTrace`), the cohort-aware pushdown lands under TASK-522
(`EntityIn` conjunct propagation into scan), and TASK-527 strengthens
the scan-adjacent rule pack (Pass 6.5 / Pass 7 probe-registry
consultation, `coalesce_scan_predicates`, `filter_order`,
`sample_pushdown`). **Impact on grades**: bqlite-planner's Tests
dimension carries the +101 unit growth largely from the rule pack and
the `fuse_match_aggregate` rewrite, sustaining the **A** grade; API
remains **A** with the `PlannerStats` and `RuleTrace` additions
purely additive.

### 6 — Four integration tests ignored, all carryovers from Wave 4

The ignored test count fell from 6 (Wave 4) to **4** (Wave 5):

| File | Ignored | Attribution |
|------|--------:|-------------|
| `jsonl_ingest.rs` | 1 | JSONL batch-size boundary (non-blocking edge case) |
| `wave2_acceptance.rs` | 1 | 100M-row reference bench, unchanged since Wave 2 |
| `wave4_acceptance.rs` | 1 | Bracket-indexed RETENTION rate — *un-ignore deferred until TASK-529 lift documented in Wave 6 audit; per-bracket rows now emit, fixture assertion still pinned to the row-count contract* |
| `wave4_advanced_analytics_attribute_cohort_join.rs` | 1 | Joined-source `__seq_id` projection through alias rewrite — the broader `__seq_id` materialization landed in TASK-508, the per-alias surface remains pending |

The two `wave4_advanced_analytics_sessionize.rs` `WITHIN SESSION`
ignores closed because TASK-510's work was absorbed into TASK-499
audit P0 #1 (commit `031cdf5`, 2026-04-26) and TASK-510 retired. The
single `wave4_advanced_analytics_attribute_cohort_join.rs` `__seq_id`
ignore that was attributed to TASK-508 closed; the second ignore is a
distinct alias-rewrite path. No Wave 5 correctness regression is
hidden by an `#[ignore]`.

### 7 — bqlite-ffi remains an intentional placeholder

FFI lands in Wave 6; its `C` across every dimension reflects that scope,
not a quality gap. Same disposition as Wave 1/2/3/4 findings.

### 8 — Memory budget enforcement and spill protocols are live (TASK-501/502/510/512/513/525)

Wave 4 had no enforced query-time memory budget; Wave 5 ships the
contract. `MemoryBudget` / `MemoryReservation` / `MemoryTracker`
land in `bqlite-core` (TASK-510), `QueryContext` carries the tracker
through bind, `Engine::query` returns `peak_memory_bytes`, and
v1 spill is wired for two operators only — `SortOperator::with_spill`
(TASK-513) and the ingest partitioner (TASK-512) — with the cohort /
IN-subquery and aggregate paths failing fast per `engine/spill.md`
§ 3. `TempSpillFile` enforces RAII cleanup; `tests/wave5_runtime_stress.rs`
covers per-query subdir reclamation under the cancellation, timeout,
and panic exit paths.

`tests/wave5_acceptance.rs` § 1 / § 3 pin the contract end-to-end:
the multi-shard analytical query runs to completion at the documented
`MIN_QUERY_BUDGET_BYTES` floor with `peak_memory_bytes = Some(_)`, and
the sort-by-ts path produces answer-invariant output across the
default budget vs the floor (the spill path activating below the
floor). The Wave 5 acceptance gate's no-spill-artefacts contract
(`engine/spill.md` § 8.3) holds across all four bands.

### 9 — Operator fusion and zero-copy scan/filter land under Wave 5 (TASK-503/515/516/517/518/519/520)

Wave 5 turns `execution-model.md` § 3.8 from "documented target" into
implementation: `FilteredBatch`, `SelectionVector`, `StatelessKernel`
(`bqlite-operators::kernel`), `FusedSegmentPhysical` push-segment
driver (`bqlite-operators::fused_segment`), explicit
`MaterializeTrigger` (sparsity / segment-boundary / aggregate-handoff
in `bqlite-operators::materialize`). Filter, Project, and Limit are
refactored onto kernels (TASK-519); Sessionize, EventSelect, and
Attribute gain inline-accumulator paths via `finish_entity_into`
overrides (TASK-520).

`tests/wave5_acceptance.rs` § 4 pins the answer-equivalence contract:
the fused / zero-copy path produces byte-identical output against
hand-computed ground truth on the multi-shard reference fixture.
`benches/wave5/zero_copy_scan.rs` reports
`bytes_materialized_before_filter` and `bytes_decompressed` against
`zero-copy-scan-filter.md` § 3; `benches/wave5/stateful_aggregate_fusion.rs`
guards the SESSIONIZE → STATS handoff against regression.
**Impact on grades**: reinforces the **A** Tests + API on
bqlite-operators; lifts bqlite-engine's Overall from B+ to A- given
the bind-side surface that integrates the kernels.

### 10 — Two crates slipped one sub-grade vs Wave 4; one Overall climbed; no crate is below C

The *Any crate slipping vs. Wave 4 is flagged* rule surfaces:

| Crate | Wave 4 | Wave 5 | Dimension | Cause |
|-------|--------|--------|-----------|-------|
| bqlite-planner | Docs B+ | Docs **B** | Docs | 10 → 23 rustdoc warnings; optimizer-framework rule pack added function/module name collisions and private-item links |
| bqlite-engine  | Overall **B+** | Overall **A-** *(↑)* | Tests / API | +100 unit tests, full Wave 5 runtime surface (memory budget, warnings, spill, scheduler, fusion bind) |

bqlite-operators' Docs warning count rose from 6 to 10 but stayed
within the **B+** band; bqlite-storage's rose 32 → 35 and stayed
**B+** for the same reason. No crate slipped on Tests, API, or
Benchmarks; bqlite-engine specifically *gained* an Overall sub-grade
(B+ → A-) on the strength of the +100 tests and the Wave 5 runtime
API surface. No crate is below **C** anywhere.

**No new follow-up tasks required under the *below-C → file a follow-up* rule** for these grade movements — but Finding 1 calls out the rustdoc-warning trajectory explicitly so Wave 6 can absorb the cleanup. Without it, the Wave 5 audit projects bqlite-storage Docs slipping to **B** in Wave 6 if the +3-warnings-per-wave rate continues.

## Wave 5 status

All 34 numbered Wave 5 tasks have `.done` markers in
`tasks/completed/` (TASK-501 through TASK-535, with TASK-530 retired
before scheduling per the "numbers are never reused" rule); the
Wave 5 acceptance gate (`tests/wave5_acceptance.rs`) passes end-to-end
with 9 tests covering the four documented bands — multi-shard
analytical query under the `MIN_QUERY_BUDGET_BYTES` floor,
cancellation/timeout cleanup (contract-level only — see open gaps
below), sort/cohort spill policy with no spill artefacts after
return, and fused/zero-copy answer equivalence against hand-computed
ground truth. One crate slipped one Docs sub-grade (bqlite-planner
Docs B+ → B), one crate gained an Overall sub-grade (bqlite-engine
B+ → A-).

Bench CI now invokes all 29 wave-scoped bench groups (TASK-543);
every group registered in `benches/Cargo.toml` is in the CI gate's
invocation list.

TASK-544 (2026-05-09) uplifted `bqlite` Tests **C → B** and Docs **C+ → B**
by adding 4 doctests and an `include_str!`-driven crate README, and recorded
named owners + remediation plans for all remaining below-B cells
(workspace-bench model for Benchmarks C, Wave 6 deferral for `bqlite-ffi`).
Remaining below-B cells are `bqlite-ffi` (C across all, Wave 6 scope) and
the Benchmarks dimension on six crates (workspace-bench model proposed);
both await human sign-off per the TASK-599 gate before Wave 6 begins.

### TASK-599 hard-gate disposition (closure follow-ups filed)

The TASK-599 task definition pins a hard gate: *"Every crate is
expected to be at least B across all dimensions; anything below B
ships only with a named owner, a concrete remediation plan, and
human sign-off before Wave 6 begins."* TASK-544 (2026-05-09)
discharges the "named owner + remediation plan" halves: `bqlite`
Tests and Docs are uplifted to B (no remaining below-B), and every
remaining below-B cell has a recorded owner and concrete plan in the
table below. **Human sign-off on the accepted/deferred rows remains
the explicit gate that must clear before Wave 6 starts.**

#### Below-B grade remediation table

*Named owner and remediation plan filled by TASK-544 (2026-05-09). Human sign-off on rows marked "Pending human sign-off" is the remaining gate before Wave 6 begins.*

| Crate | Below-B cells | Owner | Decision | Sign-off |
|---|---|---|---|---|
| `bqlite` | Tests ~~C~~ → **B**, Docs ~~C+~~ → **B** | TASK-544 | **(a) Uplifted.** Added `#![doc = include_str!("../README.md")]`-driven crate doc + 4 doctests (2 in README, 1 on `BqliteError`, 1 on `Result`). Tests and Docs grades both reach B in this checkpoint. No further remediation required. | **Resolved in TASK-544** |
| `bqlite` | Benchmarks **C** | TASK-544(d) | **(d) Workspace-bench model proposed.** `bqlite` is a thin re-export crate with zero implementation; per-crate benches would bench nothing meaningful. Transitive coverage flows through `bqlite-benches`. Rationale recorded; no new bench target filed. | **Pending human sign-off** |
| `bqlite-ffi` | Tests **C**, API **C**, Docs **C**, Benchmarks **C** | TASK-544(b) → Wave 6 (TASK-603, TASK-604) | **(b) Deferral to Wave 6 proposed.** FFI implementation is Wave 6 scope by design. `TASK-603` (PyO3 integration) and `TASK-604` (C ABI surface) are the named remediation tasks. C grades hold until Wave 6 ships. | **Pending human sign-off** |
| `bqlite-core`, `bqlite-ast`, `bqlite-parser`, `bqlite-planner`, `bqlite-engine`, `bqlite-cli` | Benchmarks **C** / **C+** | TASK-544(d) | **(d) Workspace-bench model proposed for all six crates.** Per-crate Criterion bench targets were never planned for these crates. `bqlite-core` and `bqlite-ast` are pure-types crates with no hot paths; `bqlite-parser` and `bqlite-planner` are compile-time, not hot-path; `bqlite-engine` is covered transitively by workspace acceptance + `morsel_skew` / `spill_overhead`; `bqlite-cli` is a thin frontend. All six crates' perf-critical surfaces are covered by `bqlite-benches` workspace bench groups. No individual per-crate bench scaffolds filed. | **Pending human sign-off** |

#### Other open Wave 5 closure gaps (closure tasks filed)

Each gap below has a numbered closure task in `TASKS.md` § *Wave 5
closure follow-ups*. The original Wave 5 task remains `.done` because
it shipped the documented v1 scope; the follow-up task closes the
delta to the spec's stated payoff.

| Gap | Cited evidence | Closure task |
|---|---|---|
| TASK-523 multi-core dispatch is scaffold-only — engine still dispatches "one degenerate whole-database task per query" | `crates/bqlite-engine/src/query.rs:454-487` | **TASK-536** |
| TASK-524 worker idle/busy timing is zero; CPU counters are stubbed on every platform | `crates/bqlite-engine/src/perf.rs:21-31, 265-267` | **TASK-537** |
| TASK-525 + TASK-528 cancellation/timeout coverage is contract-level only because `Engine::query` has no per-query cancel/timeout knob | `tests/wave5_runtime_stress.rs:21-23`, `tests/wave5_acceptance.rs:14-21` | **TASK-538** |
| TASK-525 + TASK-528 ingest partitioner spill is out-of-scope of both the stress and acceptance suites | `tests/wave5_runtime_stress.rs:28`, `tests/wave5_acceptance.rs:32-34` | **TASK-539** |
| TASK-525 same-database concurrent DELETE/query under scheduler pressure not covered | `tests/wave5_runtime_stress.rs:434-486` (separate-DB only) | **TASK-540** |
| Finding 1 rustdoc warning trajectory (33 → 41 → 67 → 94) without a cleanup pass | this file, lines 145-160 | **TASK-542** |
| 14 wave-scoped Criterion bench groups still not in CI's invocation list | `.github/workflows/bench.yml` (15-of-29) | **TASK-543** ✅ Closed — all 29 groups now in CI |

**Wave 6 readiness.** TASK-544 (2026-05-09) satisfies the "named owner +
remediation plan" halves of the TASK-599 hard gate:

- `bqlite` Tests and Docs uplifted to **B** — those below-B cells are fully
  resolved; no sign-off needed for grades that no longer exist.
- Every remaining below-B cell has a recorded owner and concrete plan in the
  table above (workspace-bench model for Benchmarks C; Wave 6 deferral for
  `bqlite-ffi`).

**The TASK-599 human sign-off gate remains open.** The three "Pending human
sign-off" rows above require explicit human acknowledgement before Wave 6
begins: (a) the workspace-bench-model acceptance for `bqlite` and the six
implementation crates' Benchmarks C/C+ grades, and (b) the Wave 6 deferral
for `bqlite-ffi`. Update the Sign-off column in the table above when that
acknowledgement is received.

---

## Wave 4 grades (historical)

*Captured by TASK-499 on 2026-04-20. Preserved for historical reference.*

| Crate | Tests | API | Docs | Benchmarks | Overall |
|-------|-------|-----|------|------------|---------|
| bqlite | **C** · 0 unit, 0 doctest; re-export crate | **B-** · re-exports complete for Wave 4 | **C+** · crate-level doc; 1 rustdoc collision | **C** · workspace harness only | **C+** |
| bqlite-core | **A** · 267 unit + 3 doctest; encoded-column view types *(↑ from 246)* | **A-** · Wave 4 additive (encoded column views, sample/tombstone surfaces) | **B+** · 6 rustdoc warnings *(4 → 6)* | **C** · pure-types crate | **A-** |
| bqlite-ast | **A-** · 54 unit incl. Wave 4 stage nodes *(↑ from 49)* | **A-** · cohort/alias/source-JOIN/DELETE additions | **B** · 0 rustdoc warnings | **C** · no per-crate benches | **A-** |
| bqlite-storage | **A** · 814 unit + 143 encoding property tests *(↑↑ from 426)* | **A** · v2 segment format + 6 new encodings + tombstones + concurrent compaction + JSONL/Parquet ingest | **B+** · 32 rustdoc warnings *(↓ A- → B+)* | **A** · 9 Wave 4 bench groups *(↑ from A-)* | **A** |
| bqlite-parser | **A** · 574 unit + 1 doctest; Wave 4 productions *(↑ from 415)* | **A** · full Wave 4 grammar additive | **A-** · 3 rustdoc warnings | **C** · no per-crate benches | **A** |
| bqlite-planner | **A** · 429 unit + 1 doctest; Wave 4 lowering + DemandCapabilities *(↑ from 272)* | **A** · 7 new plan variants + DemandPropagation + retention desugar | **B+** · 10 rustdoc warnings *(↓ A- → B+)* | **C** · no per-crate benches | **A-** *(↓ A on Docs only)* |
| bqlite-operators | **A** · 537 unit + Wave 4 property/integration *(↑ from 331)* | **A** · SessionizeOperator + EventSelect + Attribute + SampleFilter + MergeSources + SubqueryFilter + tombstone-aware scan | **A-** · 6 rustdoc warnings | **A** · 11 dedicated operator benches | **A** |
| bqlite-engine | **A-** · 105 unit Wave 4 bind + DELETE + compact_now *(↑ from 58)* | **A-** · Wave 4 plan-to-operator bind + tombstone writer + cohort cache | **B** · 8 rustdoc warnings | **C+** · no per-crate benches | **B+** |
| bqlite-cli | **A-** · 84 unit | **A-** · unchanged Wave 4 surface | **B+** · 0 rustdoc warnings | **C** · no per-crate benches | **A-** |
| bqlite-ffi | **C** · placeholder; Wave 6 | **C** · placeholder | **C** · placeholder | **C** · out-of-scope | **C** |

Wave 4 evidence aggregate: **3,267 passing tests**, **6 ignored**, **0 failing**. **22 Criterion bench groups**.

### Wave 4 findings (historical)

1. Rustdoc warnings grew from 41 to 67 (+26)
2. `bqlite` top-level re-export collision persists
3. Benchmark coverage expanded with 9 Wave 4 groups
4. Property-test coverage expanded with 10 new encoding + operator suites (89 → 213)
5. DemandCapabilities protocol is live (TASK-409, TASK-427)
6. Six integration tests ignored, each attributed to a Wave 5 follow-up
7. `bqlite-ffi` remains an intentional placeholder
8. CompactString adoption is live (TASK-454)
9. Two crates slipped one sub-grade vs Wave 3 (bqlite-storage Docs A- → B+, bqlite-planner Overall A → A-); zero overall grades slipped below Wave 3
10. Wave 5 follow-up tasks filed (TASK-508 through TASK-513)

---

## Wave 3 grades (historical)

*Captured by TASK-399 on 2026-04-12. Preserved for historical reference.*

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

Wave 3 evidence aggregate: **2,076 passing tests**, **1 ignored**, **0 failing**. **10 Criterion bench groups**.

### Wave 3 findings (historical)

1. Rustdoc warnings grew from 33 to 41 (minor)
2. `bqlite` top-level re-export collision persists
3. Benchmark coverage expanded significantly with 6 Wave 3 groups
4. Property test coverage expanded with NFA and binding suites
5. Variable-binding E2E integration gap closed (TASK-329)
6. Matcher benchmark scenario coverage is partial vs TASK-302 spec
7. `bqlite-ffi` remains an intentional placeholder
8. CompactString recommendation: conditional go for `BindingValue` only (TASK-332)
9. No crate slipped vs Wave 2 grades

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
