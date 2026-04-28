# Optimizer Direction Reconciliation + Statistics Source

**Wave**: 5
**Task**: TASK-504
**Status**: draft
**Depends on**: planner-pipeline.md (TASK-006 frozen pipeline), storage-format.md §11–§12 (zone maps, manifest), execution-model.md §3.8 (stateless fusion materialization), language/cohorts-aliases-joins.md (cohort pushdown intent)
**Depended on by**: TASK-521 (optimizer framework + rule-trace surface), TASK-522 (cohort/entity pushdown into scan), TASK-527 (scan-adjacent optimizer rule pack)

---

## 1. Purpose

planner-pipeline.md §6.1 + §14 froze the v1 optimizer as **rule-based only, no statistics from storage at plan time**. Wave 4–5 design notes have since introduced rules whose value depends on data shape:

- **Cohort/entity pushdown into scan** (cohorts-aliases-joins.md §4.3, TASK-522): the scan-side filter is only worth pushing if the cohort is small enough to act as a useful pre-filter. A 50-entity cohort skips most shards; a 50-million-entity cohort wastes work.
- **Tier-2 entity-presence bitmap and Tier-3 value-set skip indexes** (storage-format.md §11.2.2 / §11.2.3): only earn their place when query history says the column is filtered often, and only fire when the bitmap is selective for *this* query's predicates.
- **Selection-vector materialization triggers** (execution-model.md §3.8.3): the "sparsity → materialize" decision needs at least a runtime-known selectivity measurement.
- **Stateful-to-aggregate fusion gating** (TASK-503/TASK-527): some downstream-aggregate fusions are unconditionally beneficial; a small minority are net-negative when the stateful operator's per-entity output is tiny *and* the aggregate is non-trivial.

Each of these wants to consult something the v1 optimizer was forbidden to look at. This document reconciles that drift: it decides the v1 (Wave 5) optimizer policy, names the exact statistics sources rules are allowed to consume, declares what is explicitly out of scope, and gives a per-rule policy matrix that downstream tasks (TASK-521, TASK-522, TASK-527) can implement against.

**What this document covers:**

- Three policy options considered (§2)
- Decision: narrow heuristic gating, not a cost layer (§3)
- Statistics sources catalog: manifest metadata, zone maps, runtime cohort sizes, query-history counters, the explicit "not-a-source" list (§4)
- Plan-time vs query-runtime split: where each consumer sits and why (§5)
- The `PlannerStats` typed access surface and the rule-stat allowlist (§6)
- Per-rule policy matrix for every Wave 5 rule (§7)
- Determinism, EXPLAIN visibility, reproducibility constraints (§8)
- Reconciliation against planner-pipeline.md (§9)
- Forward references to TASK-521 / TASK-522 / TASK-527 (§10)
- Resolved design questions (§11)

**What this document does NOT cover:**

- Concrete pass ordering (planner-pipeline.md §6.2 — unchanged).
- The optimizer framework / rule registry shape (TASK-521).
- The implementation of any single rule (TASK-522, TASK-527, etc.).
- Selectivity-driven join ordering. BQL has no general join planning surface; the only joins are entity-aligned source JOINs (cohorts-aliases-joins.md §3) which have a fixed driving-side rule.
- Cardinality estimation for arbitrary predicates. v1 deliberately does not maintain enough statistics to do that responsibly.

---

## 2. Three Policy Options Considered

| Option | What it means | Pros | Cons |
| ------ | ------------- | ---- | ---- |
| **A. Pure rule-based, no stats access** (planner-pipeline.md status quo) | Every rule is a pure structural transformation. The plan never reads anything beyond the AST and the catalog schema. | Simplest. Deterministic by construction. Trivial to test. | Forbids cohort pushdown gating, Tier-2/3 index usage, anything that needs to know "is this filter selective." Forces every new data-aware rule to be either always-on (and sometimes-wrong) or shelved. |
| **B. Narrow heuristic gating** (this document's choice) | Optimizer remains structurally rule-based — rules are still pure transformations, no plan-space search, no cost minimization. A small, named set of rules is allowed to read a small, named set of statistics through one typed interface. Each (rule, statistic) pairing is allowlisted. | Keeps the v1 architecture and its determinism. Unblocks the specific Wave 5 rules that need data-awareness. Bounded blast radius — a new rule cannot "discover" a new statistics source. | Requires designing the access interface, the per-rule allowlist, and the staleness/freshness contract. More moving parts than option A. |
| **C. True cost layer** | Plan space search with a cost model. Operators get cardinality estimates, statistics, selectivity functions, etc. The planner enumerates a small set of plan candidates and scores them. | Industry-standard for OLAP. Maximum expressive power for future rules. | Very large architectural commitment for a v1 that has linear pipelines and no general join planning. Cardinality estimation needs maintained statistics (NDV sketches, histograms) the storage layer does not yet emit. Determinism becomes harder — small stat changes can flip plans. Compilation time grows non-trivially. |

Option A is what the docs say today; it is also what the Wave 5 task list has already moved past. Option C is too large for v1 and is not justified by the workload — bqlite has no multi-way join planning, so the most expensive thing a cost model normally pays for is unavailable.

---

## 3. Decision: Narrow Heuristic Gating

**Wave 5 adopts option B.**

Specifically:

1. **The optimizer remains rule-based.** No plan enumeration, no cost minimization, no fixpoint iteration. Each pass is still a deterministic function from `LogicalPlan` to `LogicalPlan` (or `PhysicalPlan` for the late passes). planner-pipeline.md §6.2's six-pass sequence is preserved; Wave 5 adds new passes after Pass 6 (see §9), but every pass is still a pure structural function — *not* a cost-driven plan picker.

2. **A finite, allowlisted set of statistics sources is exposed at plan time** (§4). Rules can consult them only through the `PlannerStats` interface (§6). Adding a new statistics source requires a design-doc change.

3. **Each rule explicitly declares which statistics it reads** (§7). Rules that read nothing are tagged "pure structural"; rules that read a stat are tagged with the source name and the gate they apply.

4. **Most data-aware decisions are not plan-time decisions at all** (§5). Selection-vector materialization, Tier-2 bitmap intersection, and zone-map row-group skipping happen *during query execution*. Plan-time only decides whether the runtime is *allowed* to attempt them. This keeps the most volatile data-dependent decisions out of the plan tree and inside the operator, where freshness is automatic.

5. **No cardinality estimation, no selectivity function, no NDV sketches at plan time.** Where a rule needs a selectivity-like number, it uses one of:
   - A runtime-known exact value (cohort size after materialization).
   - A coarse boolean ("is the column a registered Tier-3 dimension?").
   - A static structural heuristic — "cheap-first by predicate *kind*, not per-column selectivity" — that orders equality predicates before range predicates inside a fused stateless segment (§7 row 10). DBMS textbooks would call this a canonical-form pass, not a selectivity-driven reorder; the heuristic is independent of the data.

6. **Determinism is preserved.** Given the same plan input, the same catalog snapshot, and the same statistics snapshot, the planner produces the same plan. Statistics snapshots are taken once per query at planner entry (§6.2) and are immutable for the rest of compilation.

The label is deliberately "narrow heuristic gating", not "lightweight cost-based": the optimizer still does not enumerate plans or score them. It applies a structural rewrite when its applicability gate (which may now read a statistic) returns true, and skips the rewrite otherwise.

---

## 4. Statistics Sources Catalog

Five sources are admitted. Anything not in this list is not a v1 plan-time statistics source.

### 4.1 Catalog and Manifest Metadata (plan-time, cheap)

The per-table `Manifest` (storage-format.md §12.3, `crates/bqlite-storage/src/manifest.rs::Manifest`) is already loaded into memory at database open. It carries:

- `shard_count` (database-wide, fixed at init).
- `tables[t].windows[w].shards[s]` — every active segment's `SegmentMeta`.
- Per-segment: `row_count`, `byte_size`, `ts_range`, `entity_range`, `level`, `created_at`, `column_stats`.

Aggregations of these are exposed:

| Statistic | Derivation | Cost | Use site |
| --------- | ---------- | ---- | -------- |
| `table_row_count(table)` | Sum of `SegmentMeta.row_count` across the table | O(segments) | Coarse "is this a big table" gate (§7 cohort pushdown row) |
| `table_segment_count(table)` | Length of the segment list | O(1) | "Is there enough data for a particular index to matter" gate |
| `table_byte_count(table)` | Sum of `SegmentMeta.byte_size` | O(segments) | Diagnostic only (informational in EXPLAIN); not a gate input |
| `table_time_extent(table)` | Min/max of `ts_range` across all segments | O(segments) | Used by Pass 5 / scan time-range extension reconciliation only |

These derivations run at most once per query, are cached on the `PlannerStats` snapshot (§6), and never mutate during compilation. They are **catalog-derived**, not "live" — a query that runs concurrently with ingest sees the manifest snapshot it loaded at query start (storage-format.md §7.6, `Arc::clone` semantics).

### 4.2 Per-Segment Zone Maps (runtime, not plan-time)

Per-segment min/max from `SegmentMeta.column_stats` is **not a plan-time statistics source**. It is a runtime pruning input owned by the scan layer (storage-format.md §11.1). The optimizer never reads zone maps directly. The plan-time decision is only whether a scan predicate is **shaped** so the scan can apply zone-map pruning to it (storage/predicate-pushdown.md §4 — pushable predicate taxonomy). That decision is structural and does not look at the zone map's contents.

This is a deliberate split: per-segment data lives in the segment, and the planner does not build a per-segment plan. Treating zone maps as plan-time stats would force a per-segment fanout in the optimizer that does not exist anywhere else in the architecture. The structural pushable-predicate taxonomy is in storage/predicate-pushdown.md §4.

### 4.3 Tier-1 Roaring Bitmap (event_type) (runtime)

The Tier-1 event-type bitmap (storage-format.md §11.2.1, Wave 4) is built per segment and is consumed by the scan at runtime to skip row-groups. **Plan-time policy:** the optimizer treats it the same way it treats zone maps — it may **request** that the scan apply it (by ensuring `event_type IN (…)` reaches the scan as a pushable predicate, which Pass 3 already does), but never reads bitmap contents. Plan-time has no per-segment loop.

### 4.4 Tier-2 / Tier-3 Indexes (runtime, with plan-time registration)

Tier-2 entity-presence bitmaps (storage-format.md §11.2.2) and Tier-3 value-set skip indexes (§11.2.3) require **plan-time registration**: the planner must know whether a column has the index registered before it can put a predicate in a shape the scan can exploit. This is one boolean per (table, column) pair stored in the manifest's index registry (added by TASK-435 / TASK-447 — out of TASK-504's scope; this document only declares the registry as the source of truth).

| Statistic | Source | Use site |
| --------- | ------ | -------- |
| `column_has_value_set_index(table, column)` | Manifest `index_registry` | Pass 3.5 (Tier-3 predicate-shape rewriting); §7 row 7 |
| `column_has_entity_presence_bitmap(table, anchor_event_type)` | Manifest `index_registry` | MATCH anchor pruning gate; §7 row 6 |

Both are booleans. The actual bitmap contents are read at scan time, not plan time.

### 4.5 Cohort Sizes (runtime, before outer scan)

Cohorts are materialized at query start, before the outer pipeline runs (cohorts-aliases-joins.md §4.2). At the moment the outer plan executes, every cohort it depends on has a known exact size. This is the only "live data" statistic the optimizer cares about for v1, and it is exact, not estimated.

| Statistic | Source | Use site |
| --------- | ------ | -------- |
| `cohort_size(cohort_id)` | `SubqueryFilter`'s materialized `Arc<HashSet<Tuple>>` row count | Cohort/entity pushdown gate (§7 row 9) |

Whether a cohort is *shaped* like a single entity-id column vs. a multi-column tuple is **structurally** known at plan time from the cohort's logical-plan output schema and is not a runtime statistic — it lives on the `LogicalPlan` node, not in `PlannerStats`. The cohort/entity pushdown rule (§7 row 9) is therefore a 2-input conjunctive gate: a *shape* check resolved during phase 5.1 and a *size* check resolved during phase 5.2.

Because cohorts are materialized before the outer plan runs, the optimizer must run **after** cohort materialization for the size half of that gate. v1 handles this by splitting the optimizer into two phases (§5.2): the structural plan-time phase (no cohort sizes available, but cohort shape *is* available from the plan tree), and a small post-materialization phase that runs after cohorts are resolved but before the outer scan starts. Both phases are still rule-based; the second phase's rules can consult cohort sizes through the same `PlannerStats` interface, with a flag that says "cohort sizes are now bound." Phase 5.1 emits a *conditional pushdown directive* (e.g. "if `size(c) < 65_536` at runtime, push entity-id set into scan") that phase 5.2 evaluates and either applies or discards.

### 4.6 Query-History Counters (background, not query-time)

Storage-format.md §11.2.3 says value-set skip indexes are added "when the column has been filtered on more than ~5% of recent queries". The query-history counter that drives that decision is a **background maintenance** input, not a query-time plan-time input. The optimizer never reads it; the index registrar (a separate background process, TASK-447 territory) reads it to decide which indexes to add. By the time the optimizer runs, the only thing it sees is the registry boolean from §4.4.

This separation keeps the optimizer's stats access read-only and snapshot-based, with no feedback loop from the optimizer back into the storage layer.

### 4.7 Runtime Counters (`QueryMetrics`)

Runtime counters in `QueryMetrics` (execution-model.md §14.1) — `rows_after_pushdown`, `selection_vector_materializations`, `entities_processed`, etc. — are **collected during execution**, not before. The optimizer cannot read them for the current query (they don't exist yet) and is forbidden to read them across queries (would re-introduce the query-history feedback loop §4.6 explicitly excludes). They feed observability and benchmarks; they are not a plan-time source.

The single point where runtime counters interact with planning is **selection-vector materialization triggering** inside a fused stateless segment (execution-model.md §3.8.3). That decision is made by the engine at runtime per batch, not by the planner. The planner's contribution is structural — it decides whether stateless operators fuse into a single push segment (§7 row 6, §7 row 10) and orders the filters within it; everything per-batch is engine-runtime.

The only feedback loop in the system is the index registrar's (§4.6), which is asynchronous to query planning and lives in the storage layer. Optimizer rules never observe its inputs directly; they observe its outputs only as registry booleans (§4.4). This keeps the optimizer's snapshot semantics intact: nothing the optimizer reads can change while a query compiles.

### 4.8 Explicit Non-Sources

The following are *not* v1 statistics sources, even though some of them would be common in a cost-based optimizer:

- **NDV (number of distinct values) sketches per column.** `ColumnStats.distinct_count_estimate` is an HLL sketch the writer optionally produces, but no v1 rule consults it. It is preserved on disk for forward compatibility (a future cost layer might use it) and may be surfaced in EXPLAIN diagnostics, but no rule's gate reads it. The reason: NDV is a per-segment quantity; aggregating it across segments without overcounting requires either MinHash (we don't compute it) or an opaque mergeable HLL state (we don't expose mergeability through the manifest API today). Adding either is a design change worth its own task.
- **Per-column histograms.** Not produced by the writer, not in the manifest schema, not on the roadmap.
- **Per-predicate selectivity estimates.** No mechanism to compute one. Where a heuristic looks like selectivity (e.g. equality vs. range), it is a structural rule (§7 row 9), not a statistic.
- **Cross-query result caches.** Out of scope (planner-pipeline.md §1.2).

---

## 5. Plan-Time vs Query-Runtime Statistics Access

The five admitted sources split cleanly across two access points.

### 5.1 Plan-Time Phase (single snapshot)

Inputs available at plan time:

- Catalog metadata (§4.1).
- Index registry booleans (§4.4).

These are read once, snapshotted, and frozen for the rest of compilation (§6.2). Rules in this phase produce a `PhysicalPlan` that may carry conditional structure — e.g. "if cohort `c` is small, push it; otherwise probe-only" — to be resolved by phase 5.2.

### 5.2 Post-Cohort Phase (small, structural)

Inputs available after cohort materialization but before outer-plan execution:

- Everything from phase 5.1 (still snapshotted).
- Cohort sizes (§4.5).

This phase is a small, fixed set of rules — currently just the size-half of the cohort/entity pushdown gate (§7 row 9). It is structurally identical to phase 5.1 (rules are still pure functions of plan + statistics) but runs in the engine's query coordinator after cohort materialization completes. Implementation lives in `bqlite-engine` (TASK-522) because the rule must run after cohort materialization, which only the engine can sequence — `bqlite-planner` cannot orchestrate that step without taking on a reverse dependency on the engine. The rules themselves are shaped as functions over the planner's existing `PhysicalPlan` types, so the dependency direction stays clean.

### 5.3 Engine-Runtime Triggers (not optimizer)

Inputs available only during execution:

- Per-batch selection-vector density (sparsity → materialize, execution-model.md §3.8.3).
- Per-segment zone-map and bitmap evaluation (storage-format.md §11.1, §11.2.1, §11.2.2).

These are not optimizer concerns. The optimizer only decides whether the corresponding code path is *enabled* on the operator (e.g. "this scan may apply Tier-2 anchor pruning"); the operator decides when to fire it. This split is what keeps the optimizer free of per-segment fanout and per-batch state.

---

## 6. The `PlannerStats` Interface

### 6.1 Surface

```rust
/// Snapshot of plan-time-readable statistics. Lives in `bqlite-planner::stats`.
///
/// One snapshot is constructed at planner entry and threaded through every
/// rule that declares a stat dependency. Rules that declare no stat
/// dependency are passed a `PlannerStats` that they ignore.
///
/// Phase 5.2 (post-cohort) extends an existing snapshot in place by binding
/// cohort sizes; it never constructs a fresh one, so plan-time-readable
/// values stay stable across phases.
pub struct PlannerStats {
    // §4.1 — derived from manifest at construction
    pub table_row_count: HashMap<TableId, u64>,
    pub table_segment_count: HashMap<TableId, u32>,
    pub table_byte_count: HashMap<TableId, u64>,
    pub table_time_extent: HashMap<TableId, (i64, i64)>,

    // §4.4 — index registry booleans
    pub value_set_indexed: HashMap<(TableId, ColumnId), bool>,
    pub entity_presence_indexed: HashMap<(TableId, EventTypeId), bool>,

    // §4.5 — bound by phase 5.2; empty during phase 5.1
    pub cohort_size: HashMap<CohortId, u64>,
}

impl PlannerStats {
    /// Construct from a manifest snapshot. `cohort_size` is empty.
    pub fn from_manifest(manifest: &Manifest) -> Self { /* ... */ }

    /// Bind cohort sizes. Called once by phase 5.2 after every cohort has
    /// been materialized and before the post-cohort rule pass starts.
    /// Rules that read `cohort_size` before binding panic — this is a
    /// programmer error, never a user-visible failure.
    pub fn bind_cohorts(&mut self, sizes: &[(CohortId, u64)]) { /* ... */ }
}
```

The interface is intentionally a plain struct with public fields rather than a trait. There are no alternative implementations: the manifest-derived snapshot is the only producer. A trait would invite forward-compatibility shims and mock-injection patterns we do not need.

### 6.2 Snapshot Discipline

- **One snapshot per query.** Constructed at the planner's entry point (`plan(stmt, catalog, now_ns, stats)`), passed by reference into every rule.
- **Immutable for plan-time phase.** No rule mutates a snapshot during phase 5.1.
- **Bound once at the phase boundary.** `bind_cohorts` is called exactly once between phases. After the call, cohort fields are immutable for the rest of the query's lifetime.
- **No background refresh.** A long compilation does not see a manifest change mid-flight. The snapshot is taken from the `Arc<Manifest>` the engine handed in, and that `Arc` is the snapshot.

This is the same discipline storage-format.md §7.6 already uses for query-vs-compaction concurrency: the query takes a manifest snapshot at start and runs against it. The optimizer extends the snapshot to include the derived statistics it actually consumes.

### 6.3 Crate Placement

- `PlannerStats` and the per-rule allowlist live in `bqlite-planner::stats`.
- The manifest-derivation helper (`PlannerStats::from_manifest`) is in `bqlite-planner` because `bqlite-planner` already depends on `bqlite-core`'s catalog trait, which in turn is implemented by `bqlite-storage::ManifestCatalog`. The helper takes a `&Manifest` (or a `&dyn StatsCatalog` if we eventually need to abstract — see open question §10.3) and produces the snapshot.
- The post-cohort phase (§5.2) lives in `bqlite-engine`, which already owns cohort materialization and the query-coordination loop. Engine constructs the `PlannerStats` for phase 5.1, hands it to the planner, then mutates it in-place via `bind_cohorts` before invoking the post-cohort rule pass.

### 6.4 Rule-to-Stats Allowlist

Every Wave 5 optimizer rule registers a `StatsBudget` declaring which `PlannerStats` fields it reads. The framework refuses to construct a rule that reads outside its declared budget. This is enforced via a small wrapper:

```rust
pub struct StatsBudget {
    pub catalog_aggregates: bool,
    pub index_registry: bool,
    pub cohort_sizes: bool, // implies post-cohort phase
}
```

Each rule's `apply` signature takes `&PlannerStatsView<'_>` rather than `&PlannerStats`; the view is constructed against the rule's declared budget and panics in debug builds on out-of-budget access. This is a debug-mode runtime check, not a compile-time guarantee — the goal is to surface accidental dependency creep during testing, not to make it a type error. The contract that *truly* enforces "a new rule cannot silently grow a new stat dependency" is human review of the `StatsBudget` declaration in code review against the matrix in §7. The runtime view is a tripwire that fires the next time the rule's tests run; the matrix is the spec. TASK-521 may upgrade the view to a stronger compile-time shape (per-rule view types like `IndexRegistryView<'a>` that only expose the fields they consume) if the runtime check proves insufficient in practice.

The TASK-521 framework owns the `StatsBudget` declaration and the rule registry. This document only fixes the shape; TASK-521 implements the registration plumbing.

---

## 7. Per-Rule Policy Matrix

The rules below are the v1 (Wave 5) optimizer's full plan-rewriting surface. "Phase" is 5.1 (plan-time) or 5.2 (post-cohort). "Stat" lists the `PlannerStats` fields the rule reads; "none" means a pure structural rule that never consults statistics.

| # | Rule | Phase | Stat | Policy summary |
| - | ---- | ----- | ---- | -------------- |
| 1 | Pass 1: Expression inlining (planner-pipeline.md §6.3) | 5.1 | none | Pure structural rewrite of `LET` bindings into use sites. Never reads a stat. Unchanged from Wave 0. |
| 2 | Pass 2: Predicate pushdown (planner-pipeline.md §6.4) | 5.1 | none | Filters move past stateless / qualifying-stateful operators by reference-set analysis. No stats input. |
| 3 | Pass 3: Scan predicate extraction from MATCH (planner-pipeline.md §6.5) | 5.1 | none | Derives `event_type IN (…)` and per-step property predicates from the pattern. No stats. |
| 4 | Pass 4: Projection pruning / demand collection (planner-pipeline.md §6.6) | 5.1 | none | Backward demand walk. No stats. |
| 5 | Pass 5: Constant folding (planner-pipeline.md §6.7) | 5.1 | none | Pure structural. |
| 6 | Pass 6: Stateful-to-aggregate fusion (planner-pipeline.md §6.8 / §7) | 5.1 | none | Eligibility is structural per planner-pipeline.md §7.2 (rules 1 adjacency, 2 incremental computability, 3 group-by key availability, 4 no ordering dependency). All aggregates are incrementally computable (rule 2), so there is no "is this aggregate cheap enough to fuse" gate. **Fusion is unconditionally applied when eligible.** This is the existing v1 contract; Wave 5 does not weaken it. |
| 7 | Pass 6.5: Tier-3 value-set predicate-shape gating (TASK-527 / storage-format.md §11.2.3) | 5.1 | `value_set_indexed` | If the column carries a registered value-set index, the planner ensures the predicate reaches the scan in a shape the scan can intersect against the index (i.e. an `IN` set or equality literal). No statistics-driven cardinality estimate; the gate is a single boolean. |
| 8 | Pass 7: MATCH anchor presence-bitmap pushdown (storage-format.md §11.2.2 / TASK-527) | 5.1 | `entity_presence_indexed` | If the anchor event type carries a registered entity-presence bitmap, mark the scan operator to apply Tier-2 row-group pruning at runtime. Single boolean gate; the bitmap itself is read at scan time, not plan time. |
| 9 | Pass 8: Cohort/entity pushdown into scan (TASK-522, cohorts-aliases-joins.md §4.3 / §5.2) | 5.1 (shape gate) + 5.2 (size gate) | `cohort_size` | The only data-aware rule. The gate is **conjunctive over two predicates**: a structural *shape* check (cohort tuple is a single entity-id column — known at plan time, resolved during phase 5.1, no statistic involved) and a *size* check (`cohort_size(c) < COHORT_PUSHDOWN_MAX_SIZE`, threshold initially **65,536** entities — see §10.4 for tuning, resolved during phase 5.2). Phase 5.1 emits a conditional pushdown directive when the shape check passes; phase 5.2 evaluates the size check and either pushes the entity-id set as a `ScanPredicate::EntityIn` into the outer scan or discards the directive and falls back to the post-scan `SubqueryFilter` probe. Correctness is identical regardless of which branch fires; the rule is pure performance. The threshold is a fixed planner constant, not a learned value; the only data-dependence is the boolean `size < threshold`. **This is a refinement of cohorts-aliases-joins.md §5.2 step 6 / §4.3, which currently document an unconditional pushdown for entity-shaped cohorts** — see §9 for the reconciliation note. |
| 10 | Pass 9: Stateless filter ordering inside fused segment (TASK-503/518/519) | 5.1 | none | When multiple stateless filters fuse into a single push segment (execution-model.md §3.8), the planner orders them by a static heuristic: equality literals first, then small `IN` sets, then range comparisons, then `LIKE`/regex, then arbitrary scalar functions. Tie-break by source order. **The heuristic is independent of the data** — it is the standard "cheap predicates first" rule and produces a stable plan even on synthetic inputs where the heuristic is wrong. Selection-vector sparsity-driven materialization is a runtime decision, not a plan-time one (execution-model.md §3.8.3). |
| 11 | Pass 10: Scan-pushdown filter coalescing (planner-pipeline.md §6.5 + storage/predicate-pushdown.md) | 5.1 | none | Reduces multiple equivalent `event_type IN (…)` clauses to one, dedupes property predicates after MATCH extraction, and unions equivalent zone-map-acceptable predicates. Pure structural. |

Every rule in this table is implementable as a deterministic function of `(LogicalPlan, &PlannerStatsView<'_>)`. None of them require enumerating plan candidates, scoring them, or backtracking. The only data-dependent rule is row 9, and even there the dependence is "exact size below a fixed threshold" — not a continuous selectivity estimate.

### 7.1 Why This Matrix Stops Where It Does

A real cost-based optimizer would also make decisions like:

- **Filter reorder by per-column selectivity** — covered by row 9 with a static heuristic; we explicitly do not consult column-level statistics.
- **Predicate-driven join-side flipping** — there is no general join planning surface (cohorts-aliases-joins.md §3 fixes the driving side).
- **Aggregate-vs-streaming aggregate choice** — we have one aggregate algorithm (operators/aggregate-operator.md), and fusion eligibility is structural.
- **Materialization location of `IN QUERY` cohorts** — already fixed by cohorts-aliases-joins.md §4.2 (always at query start).
- **Spill-vs-no-spill decisions** — runtime concern, not plan-time (engine/spill.md, TASK-502).

Every "data-aware decision" in v1 either reduces to a structural rule or is owned by the operator at runtime. The Wave 5 optimizer's data-awareness footprint is small on purpose: row 9 is the only place plan shape changes based on a statistic.

---

## 8. Determinism, Reproducibility, EXPLAIN Visibility

### 8.1 Determinism Contract

Given:

- The same parsed AST.
- The same catalog snapshot.
- The same `PlannerStats` snapshot.

The planner produces the same plan. The contract is testable: TASK-521 ships a property test that runs the optimizer over the same `(AST, PlannerStats)` pair twice and asserts plan equality. Because every rule is a pure function of those inputs, the test is a one-liner.

Phase 5.2 widens the contract: given the same `PlannerStats` *plus* the same cohort-size bindings, the post-cohort phase is also deterministic. Phase 5.2 determinism is therefore *conditional* on cohort-materialization producing the same row counts on identical input data — that property is owned by `cohorts-aliases-joins.md §4.2` (cohort materialization is a deterministic sub-pipeline), not by this document.

### 8.2 EXPLAIN Visibility

`EXPLAIN` (planner-pipeline.md §10) shows:

- Every rule that fired on this plan, in order.
- For each stat-reading rule, the value it read (e.g. `cohort_pushdown(c1) gate=true (size=1234 < 65536)`).
- For each rule that *could* have fired but did not, a one-line diagnostic explaining why (e.g. `cohort_pushdown(c2) gate=false (size=2_000_000 ≥ 65536)`).

This is what makes "narrow heuristic gating" debuggable. A user looking at a query that ran slower than expected can read the EXPLAIN and see exactly which gate's value differed from their expectation. The format is freely refined by TASK-521; this document only requires that **every stat read by a rule is named in the EXPLAIN output**, so a slow query can never have a hidden data-dependent decision.

### 8.3 Compilation Cost

The plan-time stats snapshot construction is `O(num_segments)` for the catalog aggregates and `O(num_indexed_columns)` for the index registry — both load-once, cache-on-snapshot operations. This is well below the cost of parsing for any reasonable database. The post-cohort phase adds `O(num_cohorts)` for `bind_cohorts`. Compilation cost as a fraction of total query time stays where it was — small.

---

## 9. Reconciliation Against planner-pipeline.md

This document supersedes the following lines in planner-pipeline.md. The reconciliation lands as part of the TASK-521 framework checkpoint (single commit), not in TASK-504, because TASK-504 is a design task and edits to planner-pipeline.md belong in the implementation checkpoint that introduces the new pass numbering.

**§1.2 Non-Goals — "Cost-based optimization. Rule-based only for v1."**

Refined to: "Rule-based only for v1. Wave 5 admits *narrow heuristic gating* (this document §3): rules remain pure structural functions, but a small allowlisted set may consult `PlannerStats` (this document §6) for go/no-go decisions on a single rule. No plan-space search, no cost minimization, no continuous selectivity estimation."

**§6.1 Design Decisions — "Cost model? Rule-based only" / "Statistics from storage? Not in v1"**

Refined to point at this document for the v1 (Wave 5) policy. The "rule-based only" answer remains correct in spirit; the "no statistics" answer is replaced by "narrow heuristic gating, statistics catalog in optimizer-direction.md §4."

**§6.2 Pass Order — "The six passes run in this exact order."**

Wave 5 adds passes 6.5, 7, 8, 9, 10 as the last five entries in the sequence. The numbering is deliberately decimal-extended rather than renumbering 1–6, so existing references to "Pass 4" in other docs stay valid. The full Wave 5 order is:

1. Expression inlining
2. Predicate pushdown
3. Scan predicate extraction from MATCH
4. Projection pruning / demand collection
5. Constant folding
6. Stateful-to-aggregate fusion
6.5. Tier-3 predicate-shape gating
7. MATCH anchor presence-bitmap pushdown
8. Cohort/entity pushdown into scan (post-cohort phase, runs in engine)
9. Stateless filter ordering inside fused segment
10. Scan-pushdown filter coalescing

Passes 6.5, 7, 9, 10 run in plan-time (phase 5.1). Pass 8 runs in the engine after cohort materialization (phase 5.2). Plan-time output is a `PhysicalPlan` that phase 5.2 may further mutate before execution starts.

**§14 Resolved Design Questions — "Statistics from storage? Not used for planning in v1"**

Replaced row: "Statistics from storage at plan time? Allowlisted catalog metadata (table-level row/segment counts) and index registry booleans, plus exact cohort sizes after materialization. No per-column histograms, no NDV sketches, no selectivity estimates. See optimizer-direction.md §4." Rationale column points at this document.

**§15 Crate Placement — "Optimizer passes 1–7 → `bqlite-planner`"**

Refined to: optimizer passes 1–6 + 6.5 + 7 + 9 + 10 live in `bqlite-planner::opt`; pass 8 (post-cohort cohort/entity pushdown) lives in `bqlite-engine` because it must run after cohort materialization, which the engine's query coordinator owns. The `bqlite-planner → ast, core` dependency rule is preserved — the engine consumes planner types and applies the rule itself, no reverse dependency is introduced. See §12 of this document for the updated crate placement table.

### 9.1 Reconciliation Against `cohorts-aliases-joins.md`

`cohorts-aliases-joins.md §5.2 step 6` and `§4.3` document an **unconditional** entity-id pushdown for cohorts that include the entity-key column. Pass 8 in §7 row 9 of this document refines that to a *size-gated* pushdown (`size < COHORT_PUSHDOWN_MAX_SIZE`). Correctness is preserved in both branches — the post-scan probe path remains the fallback — so this is a performance-only refinement.

The reconciliation lands in TASK-522's checkpoint, alongside the implementation: TASK-522 updates `cohorts-aliases-joins.md §4.3` and `§5.2 step 6` to point at this document for the gating policy, and adds the threshold constant to the doc. Until that checkpoint lands, implementers should follow the policy in *this* document, not the unconditional version in `cohorts-aliases-joins.md`.

---

## 10. Forward References and Open Questions

### 10.1 TASK-521 (Optimizer framework + rule-trace surface)

TASK-521 owns:

- The rule registry and `StatsBudget` declaration mechanics (this document §6.4).
- The EXPLAIN rule-trace format (this document §8.2).
- The integration of Pass 6.5 and Pass 7 into the existing pipeline.
- The `PlannerStats::from_manifest` implementation.
- The reconciliation edits to planner-pipeline.md (this document §9).

TASK-521's plan should adopt the matrix in §7 verbatim. New rules added later require a TASK-504 amendment, not a one-line code change.

### 10.2 TASK-522 (Cohort/entity pushdown into scan)

TASK-522 owns:

- The post-cohort phase plumbing in `bqlite-engine` (this document §5.2).
- The `bind_cohorts` call site.
- Pass 8 itself (the size-gated entity-id pushdown into the outer scan).
- The `ScanPredicate::EntityIn` extension on the scan side.

TASK-522 must not introduce a continuous selectivity function — the runtime portion of the gate is a single boolean (`size < COHORT_PUSHDOWN_MAX_SIZE`) against a fixed threshold, paired with the structural shape check from phase 5.1 (see §7 row 9). If real-world workloads show the threshold needs tuning, that is a follow-up tuning task; the *shape* of the gate stays a single threshold.

### 10.3 TASK-527 (Scan-adjacent optimizer rule pack)

TASK-527 owns:

- Pass 6.5 (Tier-3 predicate-shape gating).
- Pass 7 (Tier-2 anchor bitmap registration).
- Pass 9 (stateless filter ordering inside fused segment).
- Pass 10 (scan-pushdown filter coalescing).
- The static heuristic table for filter ordering (this document §7 row 10).

TASK-527 must not introduce per-column selectivity tables. If a future task wants them, that is a TASK-504 amendment.

**Sequencing caveat.** Passes 6.5 and 7 read `value_set_indexed` and `entity_presence_indexed` from the manifest's index registry (§4.4). The registry itself is delivered by separate Wave 5 tasks (TASK-435 / TASK-447) and is not in scope for TASK-504 or TASK-521. Until those tasks land, `PlannerStats::from_manifest` populates both maps as empty — passes 6.5 and 7 then fire on no columns and degrade to no-ops. This is intentional: TASK-527 can ship the rule logic before the registry exists, and the rules become effective the moment the registry starts being populated. No code change is needed in `bqlite-planner` to "switch on" the rules later.

### 10.4 Open Tuning Choices (deliberately deferred)

| Question | Initial value | Owner | When to revisit |
| -------- | ------------- | ----- | --------------- |
| `COHORT_PUSHDOWN_MAX_SIZE` | 65 536 entities | TASK-522 | After Wave 5 bench gate (TASK-526) measures the cohort-pushdown bench under realistic inputs. |
| Tier-3 registration threshold ("filtered on more than ~5% of recent queries") | 5% / 100-query window | TASK-447 (separate Wave 5 task) | Outside this document's scope; flagged here for cross-reference only. |
| Per-rule diagnostic verbosity in EXPLAIN | "show every gate read; suppress no-op stats reads" | TASK-521 | If EXPLAIN output becomes noisy in practice. |

These are tuning constants, not architectural decisions. They live in code, not in design docs, and changing them does not require a TASK-504 amendment.

### 10.5 Out-of-Scope Items Worth Documenting

A `[NEEDS INPUT]` would be appropriate before any of the following, but each is explicitly *not* attempted in v1:

- A continuous selectivity function over arbitrary predicates (would require histograms or maintained NDV sketches).
- Plan-space search with cost minimization (the option C path).
- Plan caching across queries / sessions (planner-pipeline.md §1.2).
- Optimizer feedback loops from runtime metrics (would re-introduce non-determinism across queries — §4.7 explicitly rejects this).
- Adaptive re-planning mid-query (no execution surface for it).

If a future wave wants any of these, it should start with a fresh design doc that supersedes this one, not extend §4. The five admitted sources are the v1 ceiling.

---

## 11. Resolved Design Questions

| Question | Decision | Rationale |
| -------- | -------- | --------- |
| Pure rule-based, narrow heuristic gating, or true cost layer for Wave 5? | Narrow heuristic gating (option B, §3) | Unblocks the Wave 5 rules that need data-awareness without taking on cost-model architecture v1 cannot justify. |
| Plan-time statistics sources? | Catalog aggregates (manifest-derived), index registry booleans, cohort sizes after materialization (§4) | Five sources cover every Wave 5 rule the matrix in §7 contains; explicit list keeps the optimizer's stats footprint auditable. |
| Per-column histograms, NDV sketches, selectivity functions? | Not in v1 (§4.8) | Storage layer does not maintain mergeable statistics for this; adding them is its own design task. |
| Are zone maps a plan-time stat? | No — runtime only (§4.2) | The optimizer has no per-segment fanout; zone-map use is a structural property of the predicate's *shape*, not its *content*. |
| Where do cohort-size-gated rules run? | Engine, after cohort materialization (phase 5.2, §5.2) | `bqlite-planner` cannot depend on the engine; cohort materialization happens in the engine's query coordinator anyway. |
| Single statistics surface or per-rule trait? | Single plain-struct `PlannerStats` (§6.1) | No alternative implementations exist; a trait would invite shims we do not need. |
| Snapshot discipline? | One snapshot per query, immutable per phase, bound at the phase boundary (§6.2) | Matches the manifest snapshot semantics already in use; preserves determinism. |
| Determinism guarantee? | `(AST, catalog, PlannerStats)` → unique plan; `(AST, catalog, PlannerStats, cohort-bindings)` → unique post-cohort plan (§8.1) | Guards against silent plan flips when statistics shift slightly; testable as a one-liner. |
| EXPLAIN visibility for stat-driven decisions? | Every stat read by a rule appears in the EXPLAIN trace, including reads that did not change the plan (§8.2) | A user diagnosing a slow query must be able to see the gate values; hiding them defeats the point of narrow heuristic gating. |
| Filter ordering inside fused stateless segment? | Static heuristic only (equality < small-IN < range < LIKE < scalar fn), no per-column selectivity (§7 row 10) | Independent of data; produces a stable plan; selectivity-driven reordering would require column statistics we do not maintain. |
| Stateful-to-aggregate fusion: gated or unconditional? | Unconditional when eligible (§7 row 6) | All aggregates are incrementally computable (planner-pipeline.md §7.2); fusion is always net-positive on linear pipelines. No data-dependent gate is required or admitted. |
| Cohort pushdown threshold mechanism? | Conjunctive 2-input gate: structural shape check (phase 5.1, no stat) AND `cohort_size < COHORT_PUSHDOWN_MAX_SIZE` (phase 5.2, initial threshold 65 536; §7 row 9, §10.4) | Avoids continuous selectivity estimation; threshold is a tuning constant, not a learned value; shape is structural and does not need to live in `PlannerStats`. |
| Adding a new statistics source in the future? | Requires a TASK-504 amendment, not a code-only change (§10.5) | Keeps the optimizer's data-awareness surface explicit and reviewable. |

---

## 12. Crate Placement Summary

| Module | Crate | Purpose |
| ------ | ----- | ------- |
| `PlannerStats`, `StatsBudget`, `PlannerStatsView` | `bqlite-planner::stats` | Plan-time statistics snapshot and per-rule access view |
| `PlannerStats::from_manifest` | `bqlite-planner::stats` | Manifest-to-snapshot derivation |
| Rule registry + `StatsBudget` enforcement | `bqlite-planner::opt` | Owned by TASK-521 |
| Phase 5.1 rules (Pass 1 through Pass 6, Pass 6.5, Pass 7, Pass 9, Pass 10) | `bqlite-planner::opt` | Plan-time optimizer passes |
| `bind_cohorts` invocation site + Phase 5.2 driver | `bqlite-engine` | Owned by TASK-522 (no reverse dependency on engine from planner) |
| Pass 8 (cohort/entity pushdown) | `bqlite-engine` | Phase 5.2 rule; reads cohort sizes already known to the engine |
| `ScanPredicate::EntityIn` | `bqlite-storage` | Scan-side surface for the pushed entity-id set; introduced by TASK-522 |
| EXPLAIN rule-trace formatter | `bqlite-planner::explain` | Renders the per-rule stat reads (§8.2) |

Dependency direction is preserved: `bqlite-planner` does not gain a dependency on `bqlite-engine` or `bqlite-operators`. The engine-side phase 5.2 driver owns its own rules and consumes planner types directly.

---

## 13. Summary

- v1 (Wave 5) optimizer policy is **narrow heuristic gating**: rule-based architecture, no plan-space search, no cost minimization. A small allowlisted set of rules may consult a small allowlisted set of statistics through a single typed interface (`PlannerStats`, §6).
- Five plan-time statistics sources: catalog aggregates, index registry booleans, cohort sizes (post-materialization). Zone maps, Tier-1/2/3 bitmaps, and runtime counters are not plan-time sources — they live where they belong (scan-time, engine-runtime).
- Per-column histograms, NDV sketches, continuous selectivity functions are explicitly out of scope for v1 (§4.8).
- One rule (cohort/entity pushdown, §7 row 9) is the only place plan shape changes based on a runtime statistic; the runtime portion of its gate is a single boolean against a fixed threshold, paired with a structural shape check resolved at plan time.
- The optimizer remains deterministic, EXPLAIN-visible, and snapshot-bound. Adding a new statistics source requires a design-doc amendment, not a code-only change.
- planner-pipeline.md §1.2 / §6.1 / §6.2 / §14 are reconciled in TASK-521's framework checkpoint with text that points at this document.
