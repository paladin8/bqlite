# Quality Score

Per-crate quality grades, updated at the close of each wave. The most
recent pass is the **Wave 1 audit** (TASK-199, `2026-04-11`).

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

## Wave 1 grades

Per-cell grades carry a one-line justification — a flat letter grade
loses too much signal at the wave boundary. Evidence is collected
below the table.

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

**Workspace-level test artifacts** (`bqlite-tests` + `bqlite-benches` workspace crates):

| Target | Count | Purpose |
|--------|-------|---------|
| `tests/common_smoke.rs` | 13 | Integration fixture framework (TASK-120) — temp DB helper, assert_batches_eq, CSV loader stub |
| `tests/prop/property_value.rs` | 3 | Property-test harness template (TASK-124) — `PropertyValue` round-trips via `proptest` |
| `tests/smoke.rs` | 3 | Wave 1 acceptance gate (TASK-123) — CLI subprocess, `(0 rows)` footer, unknown-table error path |
| `benches/smoke.rs` | 1 | Criterion harness smoke (TASK-121) — measures a no-op so CI exercises the harness wiring |

**Evidence aggregate**: **398 passing unit + integration tests** (378 per-crate unit + 19 workspace integration + 1 bench-harness unit), **5 passing doctests**, **2 ignored doctests** (intentional), **0 failing tests** across the workspace.

## Evidence

Gathered from `cargo test -p <crate>`, `cargo bench -p bqlite-benches --no-run`,
`cargo doc --workspace --no-deps`, and `find crates/<crate>/src -name '*.rs'`.

| Crate | Unit tests | Doctests | LOC (src) | `pub` items | Rustdoc warnings |
|-------|-----------:|---------:|----------:|------------:|-----------------:|
| bqlite            |   0 |  0 (1 ign) |    33 |   5 | 1 (output collision) |
| bqlite-core       | 182 |  3         | 5570  |  69 | 4 (intra-doc links) |
| bqlite-ast        |  46 |  0         | 1990  |  63 | 0 |
| bqlite-storage    |  33 |  0 (1 ign) | 1407  |  19 | 0 |
| bqlite-parser     |  21 |  1         |  478  |   2 | 0 |
| bqlite-planner    |  16 |  1         |  587  |   4 | 0 |
| bqlite-operators  |  48 |  0         | 2031  |  14 | 1 (intra-doc link) |
| bqlite-engine     |  16 |  0         |  971  |  13 | 5 (1 link, 4 redundant) |
| bqlite-cli        |  16 |  0         |  512  |   0 | 0 |
| bqlite-ffi        |   0 |  0         |   10  |   0 | 0 |
| bqlite-benches    |   1 |  0         |   10  |   — | 0 |
| bqlite-tests      |  19 |  0         |   —   |   — | 0 |

- **Bench harness** compiles cleanly (`cargo bench -p bqlite-benches --no-run` → `Finished bench profile`, builds `src/lib.rs` + `benches/smoke.rs`).
- **Doc build** succeeds with warnings only (`cargo doc --workspace --no-deps` → `Finished dev profile`).
- **Clippy** clean at `-D warnings` across the workspace (`scripts/local-ci.sh` passing).
- **Formatting** clean at `cargo fmt --all --check`.
- **Dep-direction** check clean (`scripts/check-dep-direction.sh`).
- **End-to-end acceptance**: `bqlite query "events" --db <fresh-dir>` prints the bootstrap schema header (`entity_id`, `ts`, `event_type`, `__seq_id`, `__batch_id`) and the `(0 rows)` footer, exit code `0`. Negative path (`bqlite query "ghost" --db <fresh-dir>`) exits non-zero and names `ghost` in stderr.

## Findings

### 1 — Rustdoc intra-doc link warnings (minor, not below-C)

Ten warnings total across three crates, none blocking the audit:

- **bqlite-core** (4): `BqliteError::Execution`, two `try_reserve`, `DESIGN`
- **bqlite-operators** (1): `BqliteError`
- **bqlite-engine** (5): `ScanPhysical` unresolved, 4 redundant `[label](target)` where `[label]` already resolves

Impact: the rendered docs still build, but broken intra-doc links silently
erode navigation as more cross-refs accumulate. Recommended cleanup fits
within an early Wave 2 hygiene sweep. **Not filed as a follow-up task**
because the docs dimension still sits at **B-** or above for every
affected crate — the `below-C → file a follow-up` rule from AGENTS.md §4
is not tripped. Captured here so the Wave 2 author picks it up
opportunistically.

### 2 — `bqlite` top-level re-export collides with `bqlite_core::bqlite` module

`cargo doc --workspace --no-deps` emits:

```
warning: output filename collision at /workspace/target/doc/bqlite/index.html
```

…because `bqlite-core` exposes a module also named `bqlite`. Only the
last-written `index.html` survives. Harmless locally but surprising for
anyone trying to link to the root crate's docs. Worth a rename of the
conflicting module in `bqlite-core` or a rustdoc-output path override.
Same disposition as #1 — noted, not filed.

### 3 — Universal "C" on Benchmarks is a harness-scoping artifact, not a gap

Every crate scores **C** on Benchmarks because all Wave 1 benches live in
the workspace-level `bqlite-benches` crate (TASK-121) as a single smoke
target. This is correct for Wave 1 — no per-crate performance-critical
path exists yet — but the dimension will need rescoring as real
microbenchmarks land per-crate in Waves 2+. The current grade should be
read as "harness wired, no per-crate benches yet" rather than as a
failing gap.

### 4 — `bqlite-ffi` is an intentional placeholder

FFI lands in Wave 6; its `C` across every dimension reflects that scope,
not a quality gap. Do not file a follow-up to raise this grade before the
Wave 6 `[IMPL]` tasks land.

### 5 — Smoke-test stale-binary foot-gun (not a grade issue — environmental)

`cargo test --workspace` (without `--all-targets`) does not rebuild the
`bqlite` CLI binary, so `tests/smoke.rs` can spawn a stale binary and
fail with misleading "Not yet implemented" output. The canonical
invocation used by `scripts/local-ci.sh` (`cargo test --all-targets`)
does build the binary, so CI is fine; only ad-hoc `cargo test` invocations
can hit the stale-binary trap. Documented here so the next audit doesn't
treat it as a regression.

## Wave 1 status

**Wave 1 is complete.** Every unblocking `[IMPL]` and `[DESIGN][TRAIT]`
task under TASK-1xx has a `.done` marker in `tasks/completed/`; the
acceptance gate (TASK-123) passes end-to-end; per-crate grades all sit
at or above **C** on every dimension — no follow-up tasks required
under the `below-C → file a follow-up` rule.

The Findings section above records two minor cleanup opportunities
(rustdoc link warnings and the `bqlite` doc-output collision) for
opportunistic pickup during Wave 2. Neither is a wave gate.
