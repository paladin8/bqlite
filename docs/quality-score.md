# Quality Score

Per-crate quality grades, updated at the close of each wave. The most
recent pass is the **Wave 2 audit** (TASK-299, `2026-04-12`).

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

## Wave 2 grades

Per-cell grades carry a one-line justification. Evidence is collected
below the table. Grade changes from Wave 1 are annotated with arrows.

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

**Workspace-level test artifacts** (`bqlite-tests` + `bqlite-benches` workspace crates):

| Target | Count | Purpose |
|--------|-------|---------|
| `tests/common_smoke.rs` | 13 | Integration fixture framework (TASK-120) — temp DB helper, assert_batches_eq, CSV loader |
| `tests/prop/property_value.rs` | 12 | Property-test harness (TASK-124) — `PropertyValue` round-trips via `proptest` |
| `tests/prop_arrow.rs` | 1 | Arrow ↔ BqlType round-trip property test |
| `tests/prop_encoding_plain.rs` | 14 | Plain encoding encode/decode round-trip property tests |
| `tests/prop_encoding_dictionary.rs` | 15 | Dictionary encoding encode/decode round-trip property tests |
| `tests/prop_encoding_delta.rs` | 10 | Delta encoding encode/decode round-trip property tests |
| `tests/prop_encoding_bitpacking.rs` | 11 | BitPacking encoding encode/decode round-trip property tests |
| `tests/prop_encoding_constant.rs` | 9 | Constant encoding encode/decode round-trip property tests |
| `tests/prop_time.rs` | 7 | TimeRange intersection/shift property tests |
| `tests/smoke.rs` | 4 | Wave 1+2 acceptance gates (TASK-123) — CLI subprocess, `(0 rows)` footer, unknown-table error |
| `tests/wave2_acceptance.rs` | 9 (1 ign) | Wave 2 acceptance gate (TASK-235) — CREATE TABLE, INSERT FROM CSV, INSERT VALUES, DESCRIBE, ALTER, DROP, end-to-end query |
| `benches/benches/smoke.rs` | 1 | Criterion harness smoke (TASK-121) — no-op bench to exercise wiring |
| `benches/wave2/scan.rs` | — | Columnar decode throughput: int64, string, float, with/without zone-map pruning |
| `benches/wave2/encoding.rs` | — | Per-encoding encode/decode microbenches (Plain, Dictionary, Delta, BitPacking, Constant, LZ4) |
| `benches/wave2/ingest.rs` | — | CSV ingest throughput end-to-end |
| `benches/wave2/acceptance.rs` | — | Full round-trip: ingest → write segments → read segments, compression ratio, zone-map pruning rate |

**Evidence aggregate**: **1,479 passing tests** via `cargo test --workspace --all-targets` (1,363 per-crate unit + 111 workspace integration/property + 5 bench-crate unit), **4 ignored** (1 wave2 100M-row acceptance + 3 workspace doctest stubs), **0 failing tests**. Separately, `cargo test --workspace --doc` adds **5 passing doctests** and **2 ignored doctests**. Of the 111 workspace tests, **79 are property tests** covering encoding roundtrips (5 encodings × all applicable types), PropertyValue coercion, TimeRange algebra, and Arrow type mapping. **4 Criterion bench groups** cover the Wave 2 performance gate metrics.

## Evidence

Gathered from `cargo test -p <crate>`, `cargo test --workspace --all-targets`,
`cargo bench -p bqlite-benches --no-run`, `cargo doc --workspace --no-deps`,
and `find crates/<crate>/src -name '*.rs'`.

| Crate | Unit tests | Doctests | LOC (src) | `pub` items | Rustdoc warnings |
|-------|-----------:|---------:|----------:|------------:|-----------------:|
| bqlite            |   0 |  0 (1 ign) |     33 |    5 | 1 (output collision) |
| bqlite-core       | 217 |  3         |  6,687 |   74 | 4 (intra-doc links) |
| bqlite-ast        |  49 |  0         |  2,191 |   65 | 0 |
| bqlite-storage    | 417 |  0 (1 ign) | 20,909 |  104 | 16 (private-item links) |
| bqlite-parser     | 263 |  1         |  6,374 |    7 | 3 (private-item links) |
| bqlite-planner    | 173 |  1         |  8,738 |   60 | 3 (private-item links) |
| bqlite-operators  | 113 |  0         |  4,946 |   21 | 1 (intra-doc link) |
| bqlite-engine     |  51 |  0         |  2,635 |   23 | 5 (1 link, 4 redundant) |
| bqlite-cli        |  80 |  0         |  1,922 |    6 | 0 |
| bqlite-ffi        |   0 |  0         |     10 |    0 | 0 |
| bqlite-benches    |   5 |  0         |  1,087 |    — | 0 |
| bqlite-tests      | 111 (4 ign) |  0 | —  |    — | 0 |

- **Bench harness** compiles cleanly (`cargo bench -p bqlite-benches --no-run` → `Finished bench profile`, builds `src/lib.rs` + `benches/smoke.rs` + 4 Wave 2 benches).
- **Bench CI** wired via `.github/workflows/bench.yml` (TASK-241): baseline capture on main push, regression gate on PRs (>10% on 3 consecutive samples), `bench-skip` label opt-out.
- **Doc build** succeeds with warnings only (`cargo doc --workspace --no-deps` → `Finished dev profile`).
- **Clippy** clean at `-D warnings` across the workspace (`scripts/local-ci.sh` passing).
- **Formatting** clean at `cargo fmt --all --check`.
- **Dep-direction** check clean (`scripts/check-dep-direction.sh`).
- **End-to-end acceptance**: Wave 2 acceptance test (`tests/wave2_acceptance.rs`) exercises `bqlite init` → `CREATE TABLE` → `INSERT FROM CSV` → `INSERT VALUES` → `SELECT ... WHERE ... LIMIT` → `DESCRIBE` → `ALTER TABLE ADD COLUMN` → `DROP TABLE`, all passing.

## Findings

### 1 — Rustdoc warnings grew from 11 to 33 (minor, not below-C)

Warnings by crate:

- **bqlite** (1): output filename collision (persists from Wave 1)
- **bqlite-core** (4): `BqliteError::Execution`, two `try_reserve`, `DESIGN` (persist from Wave 1)
- **bqlite-storage** (16): private-item links in `writer`, `reader`, `merge`, `advise`, `delta`, `dictionary`, `plain`, `Partitioner` module docs — new in Wave 2, consequence of extensive module docs referencing private helper functions
- **bqlite-parser** (3): private-item links in `lex`, `parser`, `error` module docs — new in Wave 2
- **bqlite-planner** (3): private-item links in `from_ast`, `type_error` docs — new in Wave 2
- **bqlite-operators** (1): `BqliteError` link (persists from Wave 1)
- **bqlite-engine** (5): `ScanPhysical` unresolved + 4 redundant links (persist from Wave 1, one new)

Impact: rendered docs build fine, but broken intra-doc links erode navigation.
Most new warnings come from module-level docs citing private helper functions
(e.g., `validate_request`, `plan_row_groups`, `hoist_dictionary_chunk`). These
are informational references, not broken public-API links. Recommended cleanup
fits within an early Wave 3 hygiene sweep. **Not filed as a follow-up task**
because the Docs dimension sits at **B** or above for every affected crate —
the `below-C → file a follow-up` rule is not tripped.

### 2 — `bqlite` top-level re-export collision persists

Same as Wave 1 Finding #2. `cargo doc --workspace --no-deps` emits
`warning: output filename collision at target/doc/bqlite/index.html`.
Worth a rename of the conflicting module in `bqlite-core` or a rustdoc
output path override. Same disposition — noted, not filed.

### 3 — Benchmark dimension now differentiates by coverage

Wave 2 introduced 4 dedicated Criterion bench groups under `benches/wave2/`
(scan, encoding, ingest, acceptance) plus the bench CI regression gate
(`.github/workflows/bench.yml`, `scripts/bench-compare.sh`). Crates with
perf-critical paths covered by these benches score higher on the Benchmarks
dimension:

- **bqlite-storage**: **A-** — all 4 bench groups exercise storage-layer code
  (encoding encode/decode, segment read/write, CSV ingest, zone-map pruning)
- **bqlite-operators**: **B+** — scan and acceptance benches exercise operator
  pipeline (ScanOperator → FilterOperator → ProjectOperator → LimitOperator)
- **bqlite-engine**: **C+** — acceptance bench exercises engine transitively
  but no per-crate bench targets

Crates without perf-critical paths (parser, planner, core types, CLI, FFI)
remain at **C** — appropriate for their role.

### 4 — Property test coverage expanded significantly

Wave 2 added 79 workspace-level property tests (up from 3 in Wave 1):
- 5 encoding roundtrip suites (plain, dictionary, delta, bitpacking, constant)
  covering all applicable BqlType variants
- Arrow ↔ BqlType bidirectional mapping
- TimeRange intersection/shift/containment algebra
- PropertyValue coercion roundtrips (expanded from Wave 1)

These live in `tests/tests/prop_*.rs` using the `tests/src/strategies.rs`
generator library. Combined with 4 inline zone-map no-false-negatives tests
in `bqlite-storage`, property-test coverage now matches the bar set by
`docs/core-beliefs.md` §11 for codecs and merge guarantees.

### 5 — bqlite-ffi remains an intentional placeholder

FFI lands in Wave 6; its `C` across every dimension reflects that scope,
not a quality gap. Same disposition as Wave 1 Finding #4.

### 6 — No crate slipped vs Wave 1 grades

Every crate either maintained or improved its Overall grade from Wave 1.
The largest movements:

| Crate | Wave 1 | Wave 2 | Delta |
|-------|--------|--------|-------|
| bqlite-storage | B+ | **A** | ↑ |
| bqlite-parser | B- | **A-** | ↑↑ |
| bqlite-planner | B- | **A-** | ↑↑ |
| bqlite-operators | B+ | **A-** | ↑ |
| bqlite-engine | B- | **B+** | ↑ |
| bqlite-cli | B+ | **A-** | ↑ |

No crate is below **C** on any dimension. No follow-up tasks required
under the `below-C → file a follow-up` rule.

## Wave 2 status

**Wave 2 is complete.** Every task in TASK-2xx has a `.done` marker in
`tasks/completed/` (TASK-201 through TASK-243, excluding retired TASK-242);
the Wave 2 acceptance gate (`tests/wave2_acceptance.rs`) passes end-to-end;
per-crate grades all sit at or above **C** on every dimension; no crate
slipped from its Wave 1 grade. The bench CI regression gate is wired and
active. Wave 3 can begin.

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
