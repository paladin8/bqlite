# TASK-521 — Optimizer framework + rule-trace surface

**Status:** draft
**Owner:** agent-3
**Branch:** `task/TASK-521`
**Depends on:** TASK-504 (`docs/design/planner/optimizer-direction.md` — already merged)
**Specs to satisfy:**
- `docs/design/planner/optimizer-direction.md` §6 (PlannerStats), §7 (per-rule policy matrix), §8 (determinism + EXPLAIN), §9 (planner-pipeline.md reconciliation), §10.1 (TASK-521 scope), §12 (crate placement).
- `docs/design/planner-pipeline.md` §6, §10, §14, §15 (the sections being reconciled).

## 1. Scope

This is the **framework + scaffold** task. Per `optimizer-direction.md` §10.1, TASK-521 owns:

1. The `PlannerStats` snapshot type and `StatsBudget` declaration mechanics.
2. The `PlannerStatsView<'a>` per-rule narrowed access surface.
3. The optimizer rule trait + registry + pipeline driver.
4. EXPLAIN-visible rule-trace output (plan-level summary + per-rule decisions).
5. Wiring the existing passes (`fuse_match_aggregate`, `pushdown_predicates`, `prune_columns`, `pushdown_sample`) onto the new framework so they participate in the rule trace.
6. Empty-but-correctly-positioned framework slots for Pass 6.5 (Tier-3 predicate-shape) and Pass 7 (MATCH anchor presence-bitmap), so TASK-527 only writes rule bodies.
7. Reconciling the four sections of `planner-pipeline.md` listed in `optimizer-direction.md` §9.

**Explicitly out of scope (owned by other tasks):**

- TASK-522 implements Pass 8 (cohort/entity pushdown into scan), the `bind_cohorts` call site, the post-cohort `OptimizerPipeline::run_post_cohort` invocation, and the `ScanPredicate::EntityIn` extension. We expose `PlannerStats::bind_cohorts` and `OptimizerPipeline::run_post_cohort` so TASK-522 has a hook, but no pass-8 rule body lands here.
- TASK-527 implements Pass 6.5, 7, 9, 10 rule logic. We provide empty registered slots; Pass 6.5 and Pass 7 ship as registered no-ops so the trace surface and budget enforcement are exercised. Pass 9 / Pass 10 slots are documented in §10.1 of optimizer-direction.md as TASK-527's scope and are **not** registered yet — adding their slots in this task would either (a) ship empty rules with no enforced budget assertion (no value over leaving them out), or (b) require the actual filter-ordering / coalescing structural transform which is TASK-527's body. They are added when TASK-527 implements them.
- `PlannerStats::from_manifest` is **not** shipped here. The framework only ships `PlannerStats::empty()` and `bind_cohorts`. Manifest-derived population is plumbed in TASK-522 alongside the `bind_cohorts` call site, where the engine has access to both the manifest and the planner snapshot — this avoids inventing a `StatsManifest` abstraction in `bqlite-core` that `optimizer-direction.md` §6.1 explicitly rules out ("plain struct with public fields rather than a trait. There are no alternative implementations"). Until that wiring lands, the framework runs against `PlannerStats::empty()`; the index-registry maps stay empty and the registered no-op rules remain no-ops, exactly the §10.3 sequencing caveat.

## 2. Constraints from the design doc

- §3, item 1: rules remain pure structural functions; **no plan-space search, no cost minimization, no fixpoint iteration**. The pipeline driver applies each rule once.
- §6.1: `PlannerStats` is a plain struct with public fields; no trait abstraction at the consumer side.
- §6.2: snapshot is built once at planner entry, immutable through phase 5.1; phase-5.2 binds cohort sizes via `bind_cohorts` exactly once.
- §6.3 crate placement: `PlannerStats`, `StatsBudget`, `PlannerStatsView` live in `bqlite-planner::stats`; rule registry lives in `bqlite-planner::opt`. **No new dep edges.** `bqlite-planner` must continue to depend only on `bqlite-ast` and `bqlite-core`.
- §6.4: rule registers a `StatsBudget`; `PlannerStatsView` panics in **debug** builds on out-of-budget access, no-op in release. The matrix in §7 is the spec.
- §7: every rule listed must be modeled, with declared phase, declared `StatsBudget`, and a name. Wave 5 rules that have no body yet (Pass 6.5, 7, 9, 10) register as no-op rules so the trace is complete.
- §8.1: pipeline must be deterministic — given equal `(AST, catalog, PlannerStats)` produce equal `PhysicalPlan`. The existing structural passes are already pure functions; the framework adds no nondeterminism.
- §8.2: EXPLAIN must show every rule that fired and every rule that was eligible-but-skipped, with the stat values it read. Format is freed by us; we keep it terse and grouped under a "rule trace:" section in the existing tree dump, so existing tests that match individual node content keep passing.
- §9: the matrix-described pass numbering is decimal-extended (6.5, 7, 8, 9, 10) so existing prose references to "Pass 4" stay valid. The doc reconciliation lands as part of this checkpoint.

## 3. High-level design

### 3.1 New module `bqlite-planner::stats`

```rust
pub struct PlannerStats {
    pub table_row_count: HashMap<String, u64>,
    pub table_segment_count: HashMap<String, u32>,
    pub table_byte_count: HashMap<String, u64>,
    pub table_time_extent: HashMap<String, (i64, i64)>,

    pub value_set_indexed: HashMap<(String, String), bool>,
    pub entity_presence_indexed: HashMap<(String, String), bool>,

    /// Empty during phase 5.1; bound by phase 5.2 before the post-cohort
    /// rule pass runs. Reads before binding panic in debug builds.
    pub cohort_size: HashMap<CohortId, u64>,
    cohorts_bound: bool,
}

#[derive(Default, Clone, Copy)]
pub struct StatsBudget {
    pub catalog_aggregates: bool,
    pub index_registry: bool,
    pub cohort_sizes: bool,
}

pub struct PlannerStatsView<'a> { stats: &'a PlannerStats, budget: StatsBudget }
```

Type aliases: `pub type CohortId = u32;` so post-cohort plumbing is unblocked.

`PlannerStats::empty()` constructs a snapshot with all maps empty. **No `StatsManifest` trait is added** — `optimizer-direction.md` §6.1 rules out alternative implementations of the surface. Manifest-driven population is wired in TASK-522 directly inside the engine's query coordinator, which already holds both the `bqlite-storage::Manifest` and the planner snapshot; that wiring writes into the public fields of `PlannerStats` (or constructs the snapshot inline using a free function in `bqlite-engine`). The trait-abstraction route — even with default-empty methods in `bqlite-core` — is exactly the forward-compat shim §6.1 prohibits, and is dropped from this plan.

`bind_cohorts(&mut self, sizes: &[(CohortId, u64)])` validates that:
1. `cohorts_bound` is currently `false` (panic with a clear message on second call — design-doc §6.2 says "bound exactly once").
2. After binding it sets `cohorts_bound = true` and populates `cohort_size`.

`PlannerStatsView` exposes accessor methods (`table_row_count(&self, table: &str)`, etc.) that:
- in **debug**: assert the corresponding budget bit is true, else panic with `"out-of-budget stats read: rule '<name>' read <field> without declaring it in StatsBudget"`.
- in **release**: skip the assertion (a release-mode bug here is at most a stale read; the matrix in `optimizer-direction.md` §7 is the spec).

The view also tracks reads (`Cell<Vec<StatRead>>`) so the trace surface can render them. We use `Cell` rather than `&mut` so the rule's `apply(plan, view)` signature stays `&self` on the view (rules don't need a mutable view to take notes; the trace is a side-channel).

Actually — to avoid interior mutability, we make trace recording happen at the registry level: the registry passes a `&mut TraceBuilder` separately. The view records reads into the `TraceBuilder`. So the rule's signature is:

```rust
fn apply(&self, plan: PhysicalPlan, ctx: &mut RuleContext<'_>) -> RuleOutcome;
```

`RuleContext` carries the view, the trace builder, and the snapshot phase. This avoids `Cell`/`RefCell` and keeps the type purely value-passing.

### 3.2 Rule trait + registry — `bqlite-planner::opt::registry`

```rust
pub trait OptimizerRule: Send + Sync {
    fn id(&self) -> &'static str;
    fn phase(&self) -> RulePhase;          // PlanTime | PostCohort
    fn budget(&self) -> StatsBudget;       // declared, enforced by view
    /// Transform the plan. Default outcome (recorded in the trace) is
    /// `Applied`; rules that wish to record themselves as skipped call
    /// `ctx.record_skipped(reason)` before returning the unchanged plan.
    fn apply(&self, plan: PhysicalPlan, ctx: &mut RuleContext<'_>) -> PhysicalPlan;
}

pub enum RulePhase { PlanTime, PostCohort }

pub struct RuleContext<'a> {
    view: PlannerStatsView<'a>,
    trace: &'a mut RuleTrace,
    current_rule: &'static str,
}

impl<'a> RuleContext<'a> {
    pub fn stats(&self) -> &PlannerStatsView<'a> { &self.view }
    pub fn record_skipped(&mut self, reason: &'static str) { ... }
}

pub struct OptimizerPipeline {
    rules: Vec<Box<dyn OptimizerRule>>,
}

#[derive(Default)]
pub struct RuleTrace {
    pub entries: Vec<RuleTraceEntry>,
}

pub struct RuleTraceEntry {
    pub rule: &'static str,
    pub phase: RulePhase,
    pub outcome: RuleTraceOutcome,
    pub stat_reads: Vec<StatRead>,
}

pub enum RuleTraceOutcome { Applied, Skipped(&'static str) }
pub enum StatRead { /* per-field tagged reads, formatted in EXPLAIN */ }
```

The trait method returns `PhysicalPlan` (the transformed plan, or the same plan back when the rule is a no-op). The trace outcome is recorded as a side effect via `RuleContext`. Default outcome is `Applied` — a rule that wants to mark itself "skipped" calls `ctx.record_skipped("reason")`. This avoids the awkward `RuleOutcome::NoOp { plan: PhysicalPlan }` shape from earlier drafts and matches the by-value-in/by-value-out pattern the existing pass functions already use.

The four existing wrapper rules (see §3.3) never call `record_skipped` — they always report `Applied` because their underlying functions are pure transforms that may leave the plan structurally unchanged but are still considered "applied". The Pass 6.5 / Pass 7 stub rules call `record_skipped("no value-set indexes registered")` etc. when the relevant `PlannerStats` registry map is empty, so the trace correctly reflects the spec's "every rule that could have fired but did not" requirement (`optimizer-direction.md` §8.2).

`OptimizerPipeline::run_plan_time(plan, &stats, &mut trace)` walks `PlanTime` rules in registration order, applies each, and returns the final plan. `OptimizerPipeline::run_post_cohort(...)` is the §5.2 hook — it only runs `PostCohort` rules (none registered in this task; TASK-522 registers Pass 8). It panics if `stats.cohorts_bound` is false, enforcing the snapshot discipline from §6.2.

`OptimizerPipeline::v1()` builds the canonical Wave 5 pipeline in the order specified by `optimizer-direction.md` §9. Pass 1–5 of `planner-pipeline.md` are AST-level and live in the lowering phase (not the physical-pipeline registry); we add module-level documentation calling this out explicitly so a reader of the trace doesn't ask "where's expression inlining?" The lowering helpers `desugar_funnel` / `desugar_retention` are likewise AST-level and intentionally not registered here — they run during `logical::fold_stage`, before the physical plan exists.

### 3.3 Wrapping existing passes as rules

Each existing pass becomes a thin `OptimizerRule` impl:

| RuleId | Existing function | Phase | Budget |
| --- | --- | --- | --- |
| `fuse_match_aggregate` | `opt::fuse_match_aggregate::fuse_match_aggregate` | PlanTime | none |
| `sample_pushdown` | `opt::sample_pushdown::pushdown_sample` | PlanTime | none |
| `predicate_pushdown` | `opt::pushdown::pushdown_predicates` | PlanTime | none |
| `projection_pruning` | `opt::prune::prune_columns` | PlanTime | none |

The rule wrappers are zero-cost — they call the existing function unchanged and report `Applied` always (the existing functions are idempotent and are pure functions of the plan, so we don't need a structural-change check; the trace just records "applied"). For finer "did this rule actually change the plan?" reporting, we compare a cheap signal: if the rule's function returns the same `PhysicalPlan` *shape* (same root variant, same scan pushdown count) we still mark `Applied` — distinguishing no-op vs. effective application requires a structural `==` we don't have, and the existing passes are designed to be idempotent. **Decision: trace records "ran" for all four rules; no false-positive "skipped" is possible.** This is consistent with the existing `pushdown` pass, which sometimes leaves a plan unchanged (e.g., a Filter sitting above a non-Scan child).

### 3.4 Stat-aware rule slots

Pass 6.5 (`tier3_predicate_shape_gating`) and Pass 7 (`match_anchor_presence_pushdown`) register with `StatsBudget { index_registry: true, ..Default::default() }`. Their bodies are stubs that return `NoOp { reason: "no value-set indexes in registry" }` (or the equivalent). They consult `view.value_set_indexed(...)` so the budget enforcement is exercised by tests; the registry maps are empty until TASK-435/447 populate them, so the rules degrade to no-ops.

### 3.5 EXPLAIN integration

The trace lives next to the plan. To avoid changing every consumer of `build_explain_node`, we extend `format_explain` to take an optional `&RuleTrace` and append a `rule_trace:` section **after** the tree dump. The new function is `format_explain_with_trace(&ExplainNode, Option<&RuleTrace>) -> String`; the existing `format_explain(node) -> String` becomes a thin shim that calls it with `None`. When the trace is `None`, the output is byte-identical to today's. When the trace is `Some`, the additional section appears after the existing tree (separated by a blank line), so existing tests that match substrings of the tree dump are unaffected.

**Existing EXPLAIN test impact (audited):**
- `crates/bqlite-planner/src/explain.rs` tests (lines 902+) call `format_explain(node)` directly with `None` trace — byte-identical, no impact.
- `tests/tests/wave2_acceptance.rs` line 207–237 and `tests/tests/wave3_acceptance.rs` line 280–298 use `.contains(...)` against the EXPLAIN text. Appending a `rule_trace:` section preserves every existing substring; no test changes required.
- Verified there are zero `assert_eq!` or exact-text comparisons against EXPLAIN output in the repo (grep result: only `assert_eq!(explain.row_count(), 1)` which is unaffected).

**Engine wiring change to `crates/bqlite-engine/src/ddl.rs`:**
- The plan-construction call site (find via `bind.rs:931`) needs to thread the trace through. Currently the planner returns just `PhysicalPlan`; we add `bqlite_planner::plan_with_trace` that returns `(PhysicalPlan, RuleTrace)`. The engine retains the trace alongside the bound plan and threads it into `build_explain_batch`. The batch builder switches from `format_explain(&node)` to `format_explain_with_trace(&node, Some(&trace))`. **Three lines** in the engine change.

Trace rendering format (one line per rule, indented; aligned columns):

```
rule_trace:
  fuse_match_aggregate     applied
  sample_pushdown          applied
  predicate_pushdown       applied
  projection_pruning       applied
  tier3_predicate_shape    skipped (no value-set indexes registered)
  match_anchor_presence    skipped (no entity-presence bitmaps registered)
```

When a rule has stat reads with non-empty results (post-TASK-522), they render on a follow-up indented line per `optimizer-direction.md` §8.2 example: `cohort_pushdown(c1) gate=true (size=1234 < 65536)`. For Wave 5 with empty registry maps, the format collapses to "skipped (no … registered)" lines. This satisfies §8.2: every gate that *could* fire reports its decision and the value it consulted.

### 3.6 Plan entry point

`bqlite_planner::plan` and `plan_script` keep their current signature (return `Result<PhysicalPlan>`) so engine callers don't break. Internally, `finalize_physical` becomes:

```rust
fn finalize_physical(logical: LogicalPlan, now_ns: i64) -> Result<PhysicalPlan> {
    let physical = lower_physical(logical, now_ns);
    let stats = PlannerStats::empty();
    let mut trace = RuleTrace::default();
    let pipeline = OptimizerPipeline::v1();
    let physical = pipeline.run_plan_time(physical, &stats, &mut trace);
    Ok(physical)
}
```

A new `plan_with_trace(stmt, catalog, now_ns, stats) -> Result<(PhysicalPlan, RuleTrace)>` is added so EXPLAIN consumers can request the trace. The engine's EXPLAIN batch builder switches to this entry point.

## 4. Checkpoint plan

### CP1 — `PlannerStats` + `StatsBudget` + view scaffolding (additive only)

Files added:
- `crates/bqlite-planner/src/stats.rs` — types listed in §3.1: `PlannerStats`, `StatsBudget`, `PlannerStatsView`, `CohortId`. Pre-sized `Vec`s where applicable.

Files edited:
- `crates/bqlite-planner/src/lib.rs` — `pub mod stats;` plus re-exports of `PlannerStats`, `StatsBudget`, `PlannerStatsView`, `CohortId`.

Tests (in `stats.rs` `#[cfg(test)] mod tests`):
- `PlannerStats::empty()` produces all-empty maps with `cohorts_bound = false`.
- View constructed against `StatsBudget { catalog_aggregates: true, .. }` reads `table_row_count` successfully.
- View constructed against `StatsBudget::default()` panics in debug when reading `value_set_indexed` (`#[should_panic]`).
- `bind_cohorts` succeeds once; calling it twice panics with a clear message (`#[should_panic]`).
- Reading `cohort_size` from a view with `cohort_sizes: true` budget but the snapshot's `cohorts_bound = false` panics in debug.

`scripts/local-ci.sh` must pass. No behavior change to `plan()`.

### CP2 — Rule trait + registry + driver wrapping existing passes + planner-pipeline.md reconciliation

This is the framework arrival checkpoint. Code and docs land together so the spec (`planner-pipeline.md`) and the implementation are consistent at every merge boundary.

Files added:
- `crates/bqlite-planner/src/opt/registry.rs` — `OptimizerRule`, `RuleContext`, `RulePhase`, `RuleTrace`, `RuleTraceEntry`, `RuleTraceOutcome`, `StatRead`, `OptimizerPipeline`. Plus the four wrapper rules (one struct each delegating to the existing pass functions).
- `crates/bqlite-planner/src/opt/rules.rs` (single file, not a subdirectory — keep small) — Pass 6.5 stub (`Tier3PredicateShapeGating`) and Pass 7 stub (`MatchAnchorPresencePushdown`). Each declares `StatsBudget { index_registry: true, .. }`, calls `ctx.stats().value_set_indexed(...)` (or `entity_presence_indexed(...)`) on the relevant columns, finds the registry empty, and calls `record_skipped`. Returns the plan unchanged.

Files edited:
- `crates/bqlite-planner/src/opt/mod.rs` — `pub mod registry; pub mod rules;` plus re-exports.
- `crates/bqlite-planner/src/lib.rs` — `finalize_physical` builds and runs `OptimizerPipeline::v1()` instead of calling each function inline. Add `plan_with_trace(stmt, catalog, now_ns) -> Result<(PhysicalPlan, RuleTrace)>`. `plan` keeps its current signature; under the hood it calls `plan_with_trace` and discards the trace.
- `docs/design/planner-pipeline.md` — reconciliation per `optimizer-direction.md` §9:
  - §1.2 row: refine cost-model non-goal text.
  - §6.1 table: replace "Cost model" / "Statistics from storage" rows with the narrow-heuristic-gating text from `optimizer-direction.md` §9.
  - §6.2: extend pass list with 6.5, 7, 8, 9, 10 in §9 order. Mark which run plan-time vs post-cohort.
  - §10 EXPLAIN section: add a paragraph noting the `rule_trace:` section is now part of EXPLAIN output, link to `optimizer-direction.md` §8.2.
  - §14 row "Statistics from storage?": replace with the new resolution.
  - §15: split optimizer-pass crate placement to note Pass 8 lives in `bqlite-engine`.

Tests:
- `OptimizerPipeline::v1()` registers exactly six rules in order: `fuse_match_aggregate`, `sample_pushdown`, `predicate_pushdown`, `projection_pruning`, `tier3_predicate_shape`, `match_anchor_presence`.
- Running the pipeline over a `bare events` query produces a trace with six entries; the first four are `Applied`, the last two are `Skipped`.
- Determinism property test: `plan_with_trace(stmt, catalog, now_ns)` called twice on the same input yields equal plans **and** equal traces (per §8.1, the trace is part of the deterministic surface).
- StatsBudget enforcement: a synthetic in-test rule that declares `StatsBudget::default()` but reads `value_set_indexed` panics in debug (`#[should_panic]`).
- All existing planner tests pass unchanged (the new pipeline is a pure refactor of the same passes; behavior identical).
- All existing wave2/wave3 acceptance tests pass unchanged (no change to plan output, only the optional trace is added).

### CP3 — EXPLAIN rule-trace surface (engine wiring)

Files edited:
- `crates/bqlite-planner/src/explain.rs` — `format_explain_with_trace(&ExplainNode, Option<&RuleTrace>) -> String` plus a `format_rule_trace` helper. Existing `format_explain(node) -> String` becomes `format_explain_with_trace(node, None)`. **No change** to `ExplainNode` enum.
- `crates/bqlite-engine/src/ddl.rs` — `build_explain_batch` accepts an optional `&RuleTrace` and renders it. Caller in `bind.rs:931` switches from `plan` to `plan_with_trace` and threads the trace through.
- `crates/bqlite-engine/src/bind.rs` — adjust the EXPLAIN call site to use the trace-returning entry point.

Tests:
- `format_explain_with_trace(node, None)` is byte-identical to `format_explain(node)` (regression test).
- A synthetic plan + trace renders the trace section after the tree, separated by a blank line, with one row per rule.
- Wave2 / wave3 acceptance tests: re-run, verify the existing `.contains(...)` assertions still match (their substrings are inside the tree dump, not the trace section).

### CP4 — Final reconciliation + code review + completion

Files edited (small):
- `docs/design/INDEX.md` if any cross-link is now wrong; otherwise none.

Run final review subagent on the cumulative diff. Move lock file to `tasks/completed/TASK-521.done`, add `completed_at`, commit, push.

## 5. Risk register

| Risk | Mitigation |
| --- | --- |
| Existing EXPLAIN tests break due to format drift | CP3 keeps `format_explain(node)` byte-identical when trace is `None`. |
| Engine `build_explain_batch` plumbing churns more than expected | We confirmed it's a single call site (`crates/bqlite-engine/src/ddl.rs:177–178`). Trace-threading is a 5-line change. |
| `StatsBudget` enforcement creates noisy debug panics in unrelated tests | Wrappers around the four existing passes declare `StatsBudget::default()`. None of them read stats, so debug-mode panics are impossible there. |
| `PlannerStats` shape needs to grow before TASK-522 lands | Field set is fixed by `optimizer-direction.md` §6.1; we copy it verbatim. New fields require a §10.3 amendment (out of scope here). |
| Pipeline driver becomes a hidden cost layer | Driver is a single `for rule in &self.rules { rule.apply(...) }` loop. No fixpoint, no dependency analysis, no candidate enumeration. |
| Trace allocation cost on the hot path | The trace is built per-query at plan time; `RuleTrace::entries` is `Vec::with_capacity(pipeline.rules.len())` (six today). Plan-time work is negligible relative to execution; bounded allocation. |

## 6. Decisions to highlight in code review

- The trace is rendered **inside** `format_explain_with_trace`, not as a sibling enum variant. This keeps `ExplainNode` stable.
- `PlannerStats::from_manifest` is **not** in this task; only `empty()` and `bind_cohorts` ship. No `StatsManifest` trait is added — `optimizer-direction.md` §6.1 explicitly rules out alternative implementations of the surface, and engine-side wiring in TASK-522 will populate the public fields directly without needing a trait.
- The rule trait returns `PhysicalPlan` (not `Result<PhysicalPlan, ...>`); rules cannot fail, matching the existing pure-function pass signatures. Outcome is reported via `RuleContext` side-channel.
- The four existing passes become rule wrappers but their function bodies do **not** move — we keep the `pub use` exports at the module root so any out-of-tree consumers (none, as of CP2) still work.

## 7. Outside this PR

- TASK-522: implements `bind_cohorts` callsite + Pass 8 rule body + `ScanPredicate::EntityIn`.
- TASK-527: implements Pass 6.5/7 rule bodies + Pass 9 (filter ordering) + Pass 10 (filter coalescing).
- TASK-435/TASK-447: populate `value_set_indexed` and `entity_presence_indexed` from the manifest.

When TASK-435/447 land, `PlannerStats::from_manifest` (or its trait-based equivalent) will read non-empty registry maps; the rule slots already exist and will start firing without further framework changes — **the explicit goal of this task**.
