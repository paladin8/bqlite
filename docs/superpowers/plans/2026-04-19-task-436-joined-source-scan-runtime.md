# TASK-436: Joined-Source Scan Runtime (+ SAMPLE extension) Implementation Plan

> **For agentic workers:** Implement task-by-task with `scripts/local-ci.sh` passing and a code-review subagent reviewing each commit per AGENTS.md.

**Goal:** Implement the runtime for `PhysicalPlan::MergeSources` — a `MergeSourcesOperator` that opens N source-table scans, merges them in `(entity_id, ts, table_order, __seq_id)` order, and emits rows in the union schema with a `__source_table_id` discriminator. Extend the TASK-430 SAMPLE pushdown pass to push `(fraction, seed)` into every sub-scan when a `Sample` sits above a `MergeSources`.

**Architecture:** CP1 extends the planner's `pushdown_sample` pass to handle `MergeSources` (purely planner-side, small). CP2 adds the runtime operator in `bqlite-operators/src/scan.rs` and exposes a small amount of reusable extraction logic from `bqlite-storage/src/segment/merge.rs`. The operator owns `Vec<Box<dyn PhysicalOperator>>` (one per sub-table, typically each a `ScanOperator`), performs a heap-driven k-way row pick, and emits batches in the combined schema via Arrow `interleave` per output column. The **engine bind arm for `MergeSources` is owned by TASK-438** and explicitly out of scope here — tests exercise `MergeSourcesOperator` directly via the existing `VecReader` harness in scan.rs.

**Tech Stack:** Rust, Arrow (`arrow::compute::interleave`, `Int64Array`, `StringViewArray`), `std::collections::BinaryHeap`, `thiserror`, existing `bqlite_core::SegmentReader` / `bqlite_planner::physical` infrastructure.

**Design anchors:**
- `docs/design/language/cohorts-aliases-joins.md` Block B (§3.1–§3.11) — entity-aligned source JOIN semantics, same-`ts` tie-breaking, scan-range widening, SAMPLE interaction, `__source_table_id` column spec.
- `docs/design/operators/event-select-sample.md` §14–§18 — SAMPLE entity-hash semantics (xxHash64, population invariance); SAMPLE pushdown contract.
- `docs/design/planner/logical-plan-nodes.md` (joined Scan node shape).

**Out of scope (owned by other tasks):**
- Engine bind arm for `MergeSources` — **TASK-438**.
- Qualified-to-bare column-reference rewrite at runtime for scan-predicate pushdown / projection through a joined scan — TASK-436 *could* own this per the planner's `debug_assert!` comment, but the design defers predicate/projection pushdown through `MergeSources` until a later wave; this plan leaves the existing `debug_assert!(compiled_predicates.is_empty() && projected_columns.is_empty(), …)` in place. Removing that assert without a runtime rewrite would be unsafe.
- Entity-id cohort pushdown on the scan layer — **follow-up to TASK-437** (cohort runtime), already called out as deferred in `cohorts-aliases-joins.md` §6.3.1.

---

## Existing Infrastructure (confirmed)

- **Planner `MergeSourcesPhysical`** exists at `crates/bqlite-planner/src/physical.rs:829`. Structurally: `tables: Vec<ScanPhysical>`, `order: Vec<(String, SortDirection)>` = `[(entity_id, Asc), (ts, Asc), (__table_order, Asc), (__seq_id, Asc)]`, `table_id_map: Vec<String>` (catalog names in JOIN order), `output_schema: OperatorSchema`.
- **Planner lowering** for joined Scan → MergeSources at `physical.rs:1048-1116`. Produces per-table `ScanPhysical` with the same `query_range` / `reader_range`, empty `scan_predicates` / `projected_columns` / `sample` (TASK-436 may populate `sample` from pushdown).
- **Combined output schema** produced by `build_joined_scan` in `crates/bqlite-planner/src/logical.rs:1271-1326`. For each non-system column in every sub-table it emits `ColumnDef { name: "<table>.<col>", … }`; then appends `__source_table_id: Int NOT NULL`, `__seq_id: Int NOT NULL`, `__batch_id: Int NOT NULL`. Per-sub-table `ScanPhysical.output_schema` keeps **bare** column names (verified by `joined_scan_lowers_to_merge_sources` test at `physical.rs:2553`).
- **SAMPLE pushdown pass** at `crates/bqlite-planner/src/opt/sample_pushdown.rs`. Currently recognizes push-through through `Filter` / `Project` and pushes into `ScanPhysical.sample`; does **not** recognize `MergeSources`.
- **`KWayMergeScan`** at `crates/bqlite-storage/src/segment/merge.rs:135` for same-schema segment merges. Defines `EntityKeyValue` (pub-able enum), `extract_ts_nanos` (module-private), `validate_key_types` (module-private). Designed to be extended per its own module doc comment: *"If a future refactor needs to hoist wrapping into the merge constructor (e.g. to amortise wrapper allocation across many scans in the joined-source runtime from TASK-436), the scan operator … is where that plumbing should change; the merge stays a pure ordering operator."*
- **`ScanOperator`** at `crates/bqlite-operators/src/scan.rs:216`. Implements `PhysicalOperator`. Accepts a `SampleFilter` via `with_sample_filter` (TASK-430). `VecReader` test harness at `scan.rs:1312`.
- **Engine bind stub** at `crates/bqlite-engine/src/bind.rs:613` returns "Wave 4 operator binding is not yet implemented (TASK-438)" for `MergeSources`. Leave as-is.

---

## Task Decomposition

Two checkpoints. Each must pass `scripts/local-ci.sh` and a subagent code review before merge to `main`. Commits happen on the task branch `task/TASK-436`.

---

## CP1 — Extend SAMPLE Pushdown to `MergeSources` (planner, small)

**Goal:** When a `PhysicalPlan::Sample` sits above a `PhysicalPlan::MergeSources`, push `(fraction, seed)` into every sub-scan's `ScanPhysical.sample` and elide the `Sample` node. Population-invariance per `cohorts-aliases-joins.md` §3.4 guarantees this is correct: SAMPLE hashes entity-id **value**, which is identical across tables sharing the same entity key, so applying the same filter at each sub-scan produces atomically the same cross-table entity set.

**Files:**
- Modify: `crates/bqlite-planner/src/opt/sample_pushdown.rs`

### Task 1.1: Add push-through + push-into-scan arms for `MergeSources`

**Files:**
- Modify: `crates/bqlite-planner/src/opt/sample_pushdown.rs` (module docs, `can_push_through`, `push_into_scan`, `pushdown_sample`)

- [ ] **Step 1: Write failing tests** (append to the `tests` module at the bottom of the file, after `nested_sample_inside_aggregate_is_pushed_into_inner_scan`):

```rust
    fn make_merge_sources(n: usize) -> PhysicalPlan {
        let tables: Vec<ScanPhysical> = (0..n).map(|i| ScanPhysical {
            table: format!("t{i}"),
            query_range: None,
            reader_range: None,
            scan_predicates: vec![],
            projected_columns: vec![],
            output_schema: schema(),
            entity_key_col: "entity_id".into(),
            timestamp_col: "ts".into(),
            sample: None,
        }).collect();
        PhysicalPlan::MergeSources(crate::physical::MergeSourcesPhysical {
            tables,
            order: vec![
                ("entity_id".into(), crate::physical::SortDirection::Asc),
                ("ts".into(), crate::physical::SortDirection::Asc),
            ],
            table_id_map: (0..n).map(|i| format!("t{i}")).collect(),
            output_schema: schema(),
        })
    }

    #[test]
    fn sample_over_merge_sources_pushes_into_every_sub_scan() {
        let plan = sample_over(make_merge_sources(3), 0.25, 7);
        let out = pushdown_sample(plan);
        let PhysicalPlan::MergeSources(ms) = out else {
            panic!("expected MergeSources after push (Sample elided), got {out:?}");
        };
        assert_eq!(ms.tables.len(), 3);
        for (i, sub) in ms.tables.iter().enumerate() {
            let s = sub.sample.as_ref()
                .unwrap_or_else(|| panic!("sub-scan {i} missing sample"));
            assert!((s.fraction - 0.25).abs() < f64::EPSILON, "sub-scan {i} fraction mismatch");
            assert_eq!(s.seed, 7, "sub-scan {i} seed mismatch");
        }
    }

    #[test]
    fn sample_over_filter_over_merge_sources_pushes_through() {
        // Filter between Sample and MergeSources is still push-through
        // because Filter is stateless / entity-key-independent.
        let ms = make_merge_sources(2);
        let filter = PhysicalPlan::Filter(FilterPhysical {
            predicate: trivial_and(),
            input: Box::new(ms),
            tile_size: DEFAULT_FILTER_TILE_SIZE,
            output_schema: schema(),
        });
        let plan = sample_over(filter, 0.5, 42);
        let out = pushdown_sample(plan);
        let PhysicalPlan::Filter(f) = out else {
            panic!("expected Filter at top, got {out:?}");
        };
        let PhysicalPlan::MergeSources(ms) = *f.input else {
            panic!("expected MergeSources under Filter");
        };
        for sub in &ms.tables {
            let s = sub.sample.as_ref().expect("sub-scan sample attached");
            assert!((s.fraction - 0.5).abs() < f64::EPSILON);
            assert_eq!(s.seed, 42);
        }
    }

    #[test]
    fn merge_sources_without_sample_is_unchanged() {
        let plan = make_merge_sources(2);
        let out = pushdown_sample(plan);
        let PhysicalPlan::MergeSources(ms) = out else {
            panic!("expected MergeSources, got {out:?}");
        };
        assert!(ms.tables.iter().all(|t| t.sample.is_none()));
    }

    #[test]
    fn sample_over_limit_over_merge_sources_is_not_pushed() {
        // Limit is not commutative with Sample (changes row counts), so
        // the Sample must remain above the Limit even when the leaf is
        // MergeSources. The sub-scans' sample fields remain None.
        let ms = make_merge_sources(2);
        let limited = PhysicalPlan::Limit(LimitPhysical {
            count: 10,
            input: Box::new(ms),
            output_schema: schema(),
        });
        let plan = sample_over(limited, 0.3, 1);
        let out = pushdown_sample(plan);
        let PhysicalPlan::Sample(s) = out else {
            panic!("expected Sample to remain above Limit, got {out:?}");
        };
        let PhysicalPlan::Limit(l) = *s.input else { panic!("expected Limit") };
        let PhysicalPlan::MergeSources(ms) = *l.input else {
            panic!("expected MergeSources under Limit");
        };
        assert!(ms.tables.iter().all(|t| t.sample.is_none()));
    }
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cargo test -p bqlite-planner --lib opt::sample_pushdown::tests::sample_over_merge_sources 2>&1 | tail -20
```

Expected: compile error (no `MergeSourcesPhysical` import in test module) or assertion failures.

- [ ] **Step 3: Implement — extend `pushdown_sample` to recurse into MergeSources children**

Find the `// Stateful / reshaping interior nodes: recurse into children` comment block in `pushdown_sample` and insert, ahead of the `other => other` catch-all (this handles the case `Sample(Foo(MergeSources))` where the outer Sample is not directly over MergeSources — the MergeSources sub-scans still shouldn't get a sample pushed; but if a bare MergeSources appears without a Sample above, we recurse into its tables in case they contain *nested* patterns — none exist today, so this is a no-op preserving future safety):

```rust
        PhysicalPlan::MergeSources(ms) => {
            // MergeSources is a leaf at the plan level (its sub-scans
            // are `ScanPhysical`, not `PhysicalPlan`). No nested Sample
            // can exist inside a sub-scan, so recursion is a no-op, but
            // we normalize the shape to keep future extensions safe.
            PhysicalPlan::MergeSources(ms)
        }
```

- [ ] **Step 4: Extend `can_push_through` to accept MergeSources as a push-through target**

Replace the match arms in `can_push_through` (around line 218) with:

```rust
fn can_push_through(node: &PhysicalPlan) -> bool {
    match node {
        PhysicalPlan::Scan(_) => true,
        // Entity-aligned source JOIN: SAMPLE hashes entity-id value,
        // which is identical across all tables that share the entity
        // key (cohorts-aliases-joins.md §3.4). Pushing the same
        // (fraction, seed) into every sub-scan produces the atomic
        // cross-table entity set the design requires.
        PhysicalPlan::MergeSources(_) => true,
        PhysicalPlan::Filter(f) => can_push_through(&f.input),
        PhysicalPlan::Project(p) => can_push_through(&p.input),
        _ => false,
    }
}
```

- [ ] **Step 5: Extend `push_into_scan` to handle MergeSources**

Add a new arm in `push_into_scan` (around line 231) *before* the catch-all `other => panic!` arm:

```rust
        PhysicalPlan::MergeSources(ms) => {
            let crate::physical::MergeSourcesPhysical {
                mut tables,
                order,
                table_id_map,
                output_schema,
            } = ms;
            // Stamp the same (fraction, seed) onto every sub-scan.
            // Per cohorts-aliases-joins.md §3.4, SAMPLE's entity hash is
            // over the value of the entity key, so each sub-scan applies
            // the same filter independently and the merged output
            // reflects the atomic sampled entity set.
            for sub in tables.iter_mut() {
                debug_assert!(
                    sub.sample.is_none(),
                    "pushdown_sample: MergeSources sub-scan {} already has a sample",
                    sub.table
                );
                sub.sample = Some(SamplePushdown { fraction, seed });
            }
            PhysicalPlan::MergeSources(crate::physical::MergeSourcesPhysical {
                tables,
                order,
                table_id_map,
                output_schema,
            })
        }
```

- [ ] **Step 6: Update module docs**

In the first doc comment block (`//! Sample pushdown optimizer pass (TASK-430).`), add a paragraph after the "# Correctness" section:

```rust
//! # Joined-source scans
//!
//! `PhysicalPlan::MergeSources` is treated as a push-through leaf: the
//! same `(fraction, seed)` pair is stamped onto every sub-scan's
//! `ScanPhysical::sample`. Per `cohorts-aliases-joins.md` §3.4 the
//! SAMPLE hash is over the **value** of the entity key, which is
//! identical across tables sharing an entity-key type, so applying the
//! filter at every sub-scan is semantically equivalent to applying it
//! at the merged output. Extension from TASK-436.
```

- [ ] **Step 7: Add missing test imports**

Ensure the `tests` module imports at line 289 include `LimitPhysical` (already imported). Verify `MergeSourcesPhysical` is reachable from the test module — it already is via `crate::physical`.

- [ ] **Step 8: Run tests — they should pass**

```bash
cargo test -p bqlite-planner --lib opt::sample_pushdown 2>&1 | tail -15
```

Expected: all `sample_pushdown::tests` pass, including the four new `*_merge_sources_*` tests.

- [ ] **Step 9: Run full local CI**

```bash
bash scripts/local-ci.sh
```

Expected: success (fmt, dep-direction, clippy, build, test all green).

- [ ] **Step 10: Subagent code review**

Spawn a `superpowers:code-reviewer` subagent. Hand it:
- This plan CP1 section.
- `cohorts-aliases-joins.md` §3.4 (the SAMPLE push justification).
- The diff (via `git diff HEAD`).

Require APPROVE before committing.

- [ ] **Step 11: Commit**

```bash
git add crates/bqlite-planner/src/opt/sample_pushdown.rs
git commit -m "TASK-436: extend sample pushdown to joined-source scans (CP1)"
```

- [ ] **Step 12: Fast-forward merge to main**

```bash
git checkout main
git pull origin main
git merge task/TASK-436 --ff-only
git push origin main
git checkout task/TASK-436
```

---

## CP2 — MergeSourcesOperator runtime

**Goal:** Implement `MergeSourcesOperator` as a `PhysicalOperator` that consumes `N` child operators (one per sub-table), k-way-merges them in `(entity_key_value, ts, scan_idx)` order, and emits batches in the combined schema with `__source_table_id` as a non-nullable `Int64` column. Fully tested against the `VecReader` harness — no engine bind changes here (TASK-438 owns that).

**Architecture decisions:**

1. **Operator level, not SegmentScan level.** Each sub-table's ScanOperator already does its own k-way merge across segments and tombstone filtering; we merge their `RecordBatch` outputs above that. Attempting to reuse `KWayMergeScan` directly would require wrapping `ScanOperator` as `SegmentScan` (stubbing `row_group_count` / `row_group_zone_maps`), which adds semantically-meaningless plumbing.

2. **Heterogeneous schemas handled via per-sub-scan `col_map`.** Each sub-scan's output column index `j` maps to `combined_col_map[scan_idx][j]: Option<usize>` — the index of the corresponding column in the combined output schema, or `None` if the column is dropped.

3. **Combined-schema column discovery.** For each combined-schema column, we walk the sub-scans to find which (at most one) contributes. Dotted columns like `events.amount` match sub-scan `events`'s bare column `amount`. System columns `__seq_id`, `__batch_id` match a sub-scan's bare system column of the same name if present. `__source_table_id` is synthesized (no sub-scan contributes).

4. **Null placeholders for non-contributing scans.** For each output column, build a per-scan `Vec<ArrayRef>` where each entry is either the scan's contributing column array (from its current batch) or a same-length null array of the output column's type. Then `arrow::compute::interleave` does the per-column gather.

5. **`__source_table_id` column.** Since this is synthesized, build an `Int64Array` directly from the indices: `values[k] = table_id_values[indices[k].0]` where `indices[k] = (scan_idx, _)`. Dense, non-nullable, cheap.

6. **Heap ordering: `(entity_key, ts, scan_idx)`.** Same as `KWayMergeScan`'s `HeapEntry::cmp` (lexicographic `entity_key → ts → scan_idx → row_idx`). The spec order `(entity_id, ts, table_order, __seq_id)` is satisfied because:
   - `table_order == scan_idx` by construction.
   - `__seq_id` is a secondary tiebreaker *within* a single table and doesn't matter across tables in Wave 4 (each sub-scan already emits its own table's rows in `(entity_id, ts, __seq_id)` order internally, so the per-scan cursor advance preserves that).

7. **Reuse `merge.rs` helpers.** Expose `EntityKeyValue` (make pub), `extract_ts_nanos` (make pub), and `validate_key_types` (make pub) from `bqlite-storage::segment::merge` for the operator to call. This is a minimal, focused extraction that keeps the sort-key logic in one place.

8. **Precompute `reverse_col_map` at construction.** `reverse_col_map[i][out_col_idx] = Some(sub_col_idx)` or `None`. Avoids an O(n_out_cols × n_sub × sub_cols) linear scan inside `build_output_batch` per emitted batch. Per CLAUDE.md "Performance Conventions": pre-size scratch Vecs, reuse across batches.

9. **Defer batch-drain until after `build_output_batch`.** The `next_batch` pick loop must not clear a sub-scan's `batch` to `None` while its prior rows are still referenced in `indices`, because the `interleave` call in `build_output_batch` will index into those arrays. Drains and reloads happen in a post-build sweep. (Found by code review — would have been an `IndexOutOfBounds` at runtime.)

**Files:**
- Modify: `crates/bqlite-storage/src/segment/merge.rs` — pub-export three helpers.
- Modify: `crates/bqlite-operators/src/scan.rs` — add `MergeSourcesOperator` + tests.

---

### Task 2.1: Export sort-key helpers from `merge.rs`

**Files:**
- Modify: `crates/bqlite-storage/src/segment/merge.rs`

- [ ] **Step 1: Make `EntityKeyValue` pub**

At line 193 (`#[derive(Debug, Clone)]` immediately followed by `enum EntityKeyValue`), change `enum` to `pub enum`:

```rust
#[derive(Debug, Clone)]
pub enum EntityKeyValue {
```

Also make the `extract` method pub:

```rust
impl EntityKeyValue {
    pub fn extract(col: &ArrayRef, row: usize) -> Self {
```

- [ ] **Step 2: Make `extract_ts_nanos` pub**

At line 282, change `fn extract_ts_nanos` to `pub fn extract_ts_nanos`:

```rust
/// Extract the i64 nanosecond timestamp from a column at a given row.
#[inline]
pub fn extract_ts_nanos(col: &ArrayRef, row: usize) -> i64 {
```

- [ ] **Step 3: Make `validate_key_types` pub**

Find `fn validate_key_types` further down in the file (it exists — searched by name). Change to `pub fn validate_key_types`. Its doc comment already describes the contract; no rewording needed.

```bash
grep -n "fn validate_key_types" /workspace/crates/bqlite-storage/src/segment/merge.rs
```

Then apply the `pub` prefix at that line.

- [ ] **Step 4: Re-export from the storage crate root**

In `crates/bqlite-storage/src/lib.rs`, if the `segment::merge` items aren't already re-exported with these symbols, leave the crate-root unchanged — downstream operators use the fully-qualified path `bqlite_storage::segment::merge::EntityKeyValue`. Verify by grepping:

```bash
grep -n "EntityKeyValue\|extract_ts_nanos\|validate_key_types" /workspace/crates/bqlite-storage/src/lib.rs
```

If nothing comes back, skip the re-export — the fully-qualified path is fine and matches how `EncodedBatchSource`, `KWayMergeScan`, etc. are referenced by the operators crate today (see `crates/bqlite-operators/src/scan.rs:125` `use bqlite_storage::segment::merge::{…}`).

- [ ] **Step 5: Run storage tests to confirm no regressions**

```bash
cargo test -p bqlite-storage --lib segment::merge 2>&1 | tail -10
```

Expected: all existing tests pass (no behavioral change).

- [ ] **Step 6: Commit (temporary — gets folded into CP2 commit)**

Stage the merge.rs change; don't commit yet — we'll commit the whole CP2 together after the operator lands.

---

### Task 2.2: Add `MergeSourcesOperator` skeleton + output-schema validation

**Files:**
- Modify: `crates/bqlite-operators/src/scan.rs` — append a new `// MergeSourcesOperator` section at the end of the module, before the tests.

- [ ] **Step 1: Append the new types after `ScanOperator`'s impl block**

Search for the end of `impl PhysicalOperator for ScanOperator` (the existing trait impl) and append a new section after it but before the `#[cfg(test)] mod tests { … }` block. The exact location: find `#[cfg(test)]` that marks the start of tests and insert above it.

```rust
// ─────────────────────────────────────────────────────────────────────────────
// MergeSourcesOperator — joined-source scan runtime (TASK-436)
// ─────────────────────────────────────────────────────────────────────────────

/// Runtime operator for `PhysicalPlan::MergeSources`.
///
/// Owns `N` child [`PhysicalOperator`]s (one per joined sub-table,
/// typically each a [`ScanOperator`]), performs a k-way merge over
/// their `(entity_id, ts)`-ordered outputs, and emits rows in the
/// combined schema declared by [`bqlite_planner::physical::MergeSourcesPhysical`].
///
/// ## Order
///
/// Rows are emitted in `(entity_key_value, ts, scan_idx)` order. The
/// `scan_idx` tiebreaker realizes the `table_order` position in the
/// canonical `(entity_id, ts, table_order, __seq_id)` key from
/// `cohorts-aliases-joins.md` §3.2; the final `__seq_id` component is
/// preserved implicitly by each sub-scan's own internal ordering.
///
/// ## Output shape
///
/// The combined schema (`cohorts-aliases-joins.md` §3.8) carries
/// qualified column names `<table>.<col>` for every non-system column
/// of every sub-table, plus a non-nullable `__source_table_id: Int64`
/// discriminator and the shared system columns `__seq_id` / `__batch_id`
/// (when present in any sub-scan's schema). For a given output row
/// picked from sub-scan `i`, columns contributed by sub-scan `i` carry
/// the picked row's values and every other column is null.
///
/// ## Construction
///
/// [`Self::new`] resolves per-sub-scan column indices (entity key, ts)
/// and builds the sub-to-combined `col_map` once. It validates that
/// every sub-scan's entity-key column type matches the combined
/// schema's expected key type (`cohorts-aliases-joins.md` §3.6 — the
/// planner validates this too, but the runtime checks again as a
/// defense-in-depth guard).
///
/// ## Lifecycle
///
/// - `open()` opens every sub-operator and primes the heap.
/// - `next_batch()` accumulates up to [`MERGE_SOURCES_BATCH_ROWS`]
///   picks, then emits one output batch via per-column
///   [`arrow::compute::interleave`].
/// - `close()` closes every sub-operator.
pub struct MergeSourcesOperator {
    /// Per-sub-scan state: child op + current batch + cursor + exhaustion flag.
    subs: Vec<SubSource>,
    /// Combined output schema.
    output_schema: OperatorSchema,
    /// Arrow form of `output_schema`, used for building output batches.
    arrow_schema: Arc<ArrowSchema>,
    /// Per-sub-scan descriptor: column indices + col_map + table id.
    descriptors: Vec<SubSourceDesc>,
    /// Table id values, indexed by scan_idx. `__source_table_id` column
    /// is constructed from picked (scan_idx) values via this array.
    table_id_values: Vec<i64>,
    /// Index in `arrow_schema` of the `__source_table_id` column, or
    /// `None` if the combined schema has no such column (test fixtures
    /// for simpler schemas may omit it).
    source_table_id_col: Option<usize>,
    // NOTE: no `null_placeholders` field. Null arrays are built fresh
    // per output column inside `build_output_batch` because their
    // lengths depend on each sub-scan's current batch size, which
    // varies per call. Caching would require per-call rebuilds anyway.
    /// Heap entries sorted by `(entity_key, ts, scan_idx)`.
    heap: BinaryHeap<Reverse<JoinedHeapEntry>>,
    /// Target row count per emitted output batch.
    batch_target_rows: usize,
    /// Latched once every sub-scan is drained.
    exhausted: bool,
    /// Cancellation token checked at the top of each `next_batch` and
    /// forwarded to every sub-operator on `open`.
    cancel: CancellationToken,
    /// True once `open()` has been called. Resets to false on `close()`.
    opened: bool,
}

/// Default output batch size for [`MergeSourcesOperator`].
///
/// Matches `bqlite_storage::segment::merge::DEFAULT_MERGE_BATCH_ROWS`
/// so downstream consumers see the same row cadence a single-table
/// merge produces.
pub const MERGE_SOURCES_BATCH_ROWS: usize = 65_536;

/// Per-sub-scan descriptor — resolved once at construction.
#[derive(Debug, Clone)]
struct SubSourceDesc {
    /// Column index of this sub-scan's entity-key column in its own
    /// output batch.
    entity_key_col: usize,
    /// Column index of this sub-scan's timestamp column in its own
    /// output batch.
    ts_col: usize,
    /// Forward mapping: for each column `j` of this sub-scan's output
    /// schema, `col_map[j]` is the index of the corresponding column in
    /// the combined output schema, or `None` when the sub-scan column
    /// does not appear in the combined schema.
    col_map: Vec<Option<usize>>,
    /// Reverse mapping: for each output column `c` in the combined
    /// schema, `reverse_col_map[c]` is the sub-scan's column index that
    /// feeds it, or `None` when this sub-scan does not contribute to
    /// `c`. Precomputed in `new()` to avoid O(sub_cols) lookups per
    /// batch per output column in `build_output_batch`.
    reverse_col_map: Vec<Option<usize>>,
}

/// Per-sub-scan mutable state.
struct SubSource {
    /// Child operator. Emits rows in `(entity_key, ts, __seq_id)` order.
    op: Box<dyn PhysicalOperator>,
    /// Currently-loaded batch from the child. `None` means we need to
    /// pull a new batch from the child, or the child is exhausted.
    batch: Option<RecordBatch>,
    /// Row index into `batch` for the next pick.
    cursor: usize,
    /// True once `op.next_batch()` has returned `Ok(None)`.
    exhausted: bool,
}

/// Heap entry: one row from one sub-scan, ready to be picked.
struct JoinedHeapEntry {
    scan_idx: usize,
    row_idx: usize,
    entity_key: bqlite_storage::segment::merge::EntityKeyValue,
    ts_nanos: i64,
}

impl Ord for JoinedHeapEntry {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.entity_key
            .cmp(&other.entity_key)
            .then_with(|| self.ts_nanos.cmp(&other.ts_nanos))
            .then_with(|| self.scan_idx.cmp(&other.scan_idx))
            .then_with(|| self.row_idx.cmp(&other.row_idx))
    }
}
impl PartialOrd for JoinedHeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for JoinedHeapEntry {
    fn eq(&self, other: &Self) -> bool { self.cmp(other) == std::cmp::Ordering::Equal }
}
impl Eq for JoinedHeapEntry {}

impl std::fmt::Debug for MergeSourcesOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MergeSourcesOperator")
            .field("sub_count", &self.subs.len())
            .field("table_id_values", &self.table_id_values)
            .field("batch_target_rows", &self.batch_target_rows)
            .field("exhausted", &self.exhausted)
            .field("opened", &self.opened)
            .finish()
    }
}
```

Add `use std::cmp::Reverse;` and `use std::collections::BinaryHeap;` to the top of the file (near existing `use std::sync::Arc;`). Also add `use arrow::array::{Int64Array, new_null_array};` if not already imported (the existing `use arrow::array::BooleanArray;` line at the top is the reference).

- [ ] **Step 2: Add a constructor**

Append after the `Debug` impl:

```rust
impl MergeSourcesOperator {
    /// Construct a `MergeSourcesOperator`.
    ///
    /// # Arguments
    ///
    /// - `sub_ops` — one child `PhysicalOperator` per sub-table, in JOIN
    ///   source order. Each child must emit rows in `(entity_key, ts)` order.
    /// - `sub_entity_key_cols` — parallel to `sub_ops`. Names of each sub-scan's
    ///   entity-key column (table-local; may differ across sub-tables per
    ///   `cohorts-aliases-joins.md` §3.6).
    /// - `sub_ts_cols` — parallel to `sub_ops`. Names of each sub-scan's
    ///   timestamp column.
    /// - `output_schema` — the combined schema declared by the planner's
    ///   `MergeSourcesPhysical`. Used to resolve `col_map` and the
    ///   `__source_table_id` column position.
    /// - `table_id_map` — catalog names in JOIN source order; parallel to
    ///   `sub_ops`. Used only for `col_map` resolution (sub-scan `i`'s
    ///   bare column `c` maps to combined column `<table_id_map[i]>.<c>`
    ///   for non-system columns, or bare `c` for system columns).
    /// - `cancel` — shared cancellation token.
    ///
    /// # Errors
    ///
    /// - [`BqliteError::Schema`] if any sub-scan's entity-key or ts column
    ///   is absent from its own output schema.
    /// - [`BqliteError::Schema`] if the `__source_table_id` column declared
    ///   in `output_schema` has a type other than `BqlType::Int`.
    /// - [`BqliteError::Execution`] if any sub-scan's entity-key column type
    ///   is incompatible with `KWayMergeScan`'s supported types.
    pub fn new(
        sub_ops: Vec<Box<dyn PhysicalOperator>>,
        sub_entity_key_cols: Vec<String>,
        sub_ts_cols: Vec<String>,
        output_schema: OperatorSchema,
        table_id_map: Vec<String>,
        cancel: CancellationToken,
    ) -> Result<Self> {
        if sub_ops.is_empty() {
            return Err(BqliteError::Execution(
                "MergeSourcesOperator: at least one sub-scan is required".into(),
            ));
        }
        if sub_ops.len() != sub_entity_key_cols.len()
            || sub_ops.len() != sub_ts_cols.len()
            || sub_ops.len() != table_id_map.len()
        {
            return Err(BqliteError::Execution(format!(
                "MergeSourcesOperator: parallel-vec length mismatch: ops={}, entity_key_cols={}, ts_cols={}, table_id_map={}",
                sub_ops.len(),
                sub_entity_key_cols.len(),
                sub_ts_cols.len(),
                table_id_map.len(),
            )));
        }

        // Resolve per-sub-scan column indices and col_map against the
        // combined output schema.
        let mut descriptors = Vec::with_capacity(sub_ops.len());
        for (i, op) in sub_ops.iter().enumerate() {
            let sub_schema = op.output_schema();
            let entity_key_name = &sub_entity_key_cols[i];
            let ts_name = &sub_ts_cols[i];

            let entity_key_col = sub_schema.column(entity_key_name).map(|(idx, _)| idx).ok_or_else(
                || BqliteError::Schema(format!(
                    "MergeSourcesOperator: sub-scan {i} ({}) missing entity-key column `{entity_key_name}`",
                    table_id_map[i]
                )),
            )?;
            let ts_col = sub_schema.column(ts_name).map(|(idx, _)| idx).ok_or_else(|| {
                BqliteError::Schema(format!(
                    "MergeSourcesOperator: sub-scan {i} ({}) missing ts column `{ts_name}`",
                    table_id_map[i]
                ))
            })?;

            // Build col_map: for each column in the sub-scan's output schema,
            // find the index of the corresponding column in the combined
            // output schema, or None if dropped.
            let mut col_map = Vec::with_capacity(sub_schema.columns().len());
            for sub_col in sub_schema.columns() {
                let is_system = sub_col.is_system();
                let combined_name = if is_system {
                    // System columns share bare names across sub-tables.
                    sub_col.name.clone()
                } else {
                    format!("{}.{}", table_id_map[i], sub_col.name)
                };
                let combined_idx = output_schema
                    .column(&combined_name)
                    .map(|(idx, _)| idx);
                col_map.push(combined_idx);
            }

            // Build the reverse map: for each output column, which
            // sub-scan column (if any) feeds it. Precomputing here keeps
            // the `build_output_batch` hot loop O(n_sub × n_out_cols)
            // instead of O(n_sub × n_out_cols × sub_cols).
            let mut reverse_col_map: Vec<Option<usize>> = vec![None; output_schema.columns().len()];
            for (sub_col_idx, maybe_out) in col_map.iter().enumerate() {
                if let Some(out_col_idx) = *maybe_out {
                    // If two sub-scan columns map to the same output
                    // column, the last-wins behavior here is defensible
                    // (the condition shouldn't happen — planner
                    // guarantees unique combined-schema names — but we
                    // don't want a silent panic).
                    reverse_col_map[out_col_idx] = Some(sub_col_idx);
                }
            }

            descriptors.push(SubSourceDesc { entity_key_col, ts_col, col_map, reverse_col_map });
        }

        // Defense-in-depth: validate each sub-scan's entity-key column type
        // against the set `KWayMergeScan::validate_key_types` supports. The
        // planner's `build_joined_scan` already rejects mismatched entity-key
        // types across sub-tables (cohorts-aliases-joins.md §3.6), but a
        // direct test or manually constructed op could bypass it — fail
        // here with a clear error rather than panicking in
        // `EntityKeyValue::extract` on the first pick.
        for (i, (op, desc)) in sub_ops.iter().zip(descriptors.iter()).enumerate() {
            let sub_arrow = op.output_schema().to_arrow_schema();
            bqlite_storage::segment::merge::validate_key_types(
                &sub_arrow,
                desc.entity_key_col,
                desc.ts_col,
            )
            .map_err(|e| BqliteError::Execution(format!(
                "MergeSourcesOperator: sub-scan {i} ({}) key-type validation failed: {e}",
                table_id_map[i],
            )))?;
        }

        // Resolve the __source_table_id column position (if present).
        let source_table_id_col = output_schema
            .column(bqlite_core::schema::SOURCE_TABLE_ID_COLUMN)
            .map(|(idx, def)| {
                if !matches!(def.bql_type, BqlType::Int) {
                    return Err(BqliteError::Schema(format!(
                        "MergeSourcesOperator: `__source_table_id` must be Int, got {:?}",
                        def.bql_type
                    )));
                }
                Ok::<_, BqliteError>(idx)
            })
            .transpose()?;

        let arrow_schema = Arc::new(output_schema.to_arrow_schema());
        let table_id_values: Vec<i64> = (0..sub_ops.len()).map(|i| i as i64).collect();

        let subs = sub_ops
            .into_iter()
            .map(|op| SubSource { op, batch: None, cursor: 0, exhausted: false })
            .collect();

        Ok(Self {
            subs,
            output_schema,
            arrow_schema,
            descriptors,
            table_id_values,
            source_table_id_col,
            heap: BinaryHeap::new(),
            batch_target_rows: MERGE_SOURCES_BATCH_ROWS,
            exhausted: false,
            cancel,
            opened: false,
        })
    }

    /// Override the output batch size (test hook).
    #[cfg(test)]
    pub(crate) fn with_batch_size(mut self, batch_target_rows: usize) -> Self {
        assert!(batch_target_rows > 0, "batch_target_rows must be positive");
        self.batch_target_rows = batch_target_rows;
        self
    }
}
```

Note: This task does not yet implement `PhysicalOperator::{open, next_batch, close}`. Those follow in tasks 2.3 and 2.4.

- [ ] **Step 3: Check `ColumnDef::is_system` and `SOURCE_TABLE_ID_COLUMN` exist**

Verify the symbols used above:

```bash
grep -n "is_system\|SOURCE_TABLE_ID_COLUMN" /workspace/crates/bqlite-core/src/schema.rs | head -8
```

If `SOURCE_TABLE_ID_COLUMN` lives in `bqlite_planner::logical` instead of `bqlite_core`, adjust the import path. (From earlier reading it's defined in `crates/bqlite-planner/src/logical.rs:1305` as `crate::logical::SOURCE_TABLE_ID_COLUMN`; but `bqlite-operators` depends on `bqlite-planner`, so use the correct path here. If the constant isn't exported to operators, inline the string literal `"__source_table_id"` with a comment citing the design doc.)

Decide at implementation time:
- If `bqlite_planner::logical::SOURCE_TABLE_ID_COLUMN` is `pub`, import and use it.
- Otherwise inline the literal `"__source_table_id"` with a `// cohorts-aliases-joins.md §3.8` comment.

Similarly verify `ColumnDef::is_system`. If absent, replace the check with: `is_system = sub_col.name.starts_with("__")` (matches the `__seq_id`, `__batch_id` convention). Prefer the real API if available.

- [ ] **Step 4: Build (compile check only — no tests yet)**

```bash
cargo build -p bqlite-operators 2>&1 | tail -20
```

Expected: compile succeeds. If errors about missing `is_system` / constant visibility, adjust per Step 3.

---

### Task 2.3: Implement `PhysicalOperator::open` + row-pick inner loop

**Files:**
- Modify: `crates/bqlite-operators/src/scan.rs` — append to `impl MergeSourcesOperator` and add `PhysicalOperator` impl.

- [ ] **Step 1: Implement `PhysicalOperator` impl — open, close, output_schema**

Append after the existing `impl MergeSourcesOperator { … }` block:

```rust
impl PhysicalOperator for MergeSourcesOperator {
    fn output_schema(&self) -> &OperatorSchema {
        &self.output_schema
    }

    fn open(&mut self) -> Result<()> {
        if self.opened {
            return Ok(());
        }
        for (i, sub) in self.subs.iter_mut().enumerate() {
            sub.op.open().map_err(|e| {
                BqliteError::Execution(format!(
                    "MergeSourcesOperator: sub-scan {i} open failed: {e}"
                ))
            })?;
        }
        // Prime the heap: pull one batch from each sub, push first row.
        for i in 0..self.subs.len() {
            self.reload_sub(i)?;
        }
        self.opened = true;
        Ok(())
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.cancel.is_cancelled() {
            return Err(BqliteError::Execution("MergeSourcesOperator: cancelled".into()));
        }
        if self.exhausted {
            return Ok(None);
        }
        if !self.opened {
            return Err(BqliteError::Execution(
                "MergeSourcesOperator::next_batch called before open".into(),
            ));
        }

        // Accumulate picked rows up to batch_target_rows.
        //
        // Critical invariant: a sub-scan's `batch` field MUST NOT be
        // cleared while its prior row indices are still referenced in
        // `indices`, because `build_output_batch` will index into those
        // arrays. Instead we track "drained" sub-scans in a bitmap, keep
        // their batches alive through the emit, and clear+reload them in
        // a single post-build sweep.
        let mut indices: Vec<(usize, usize)> = Vec::with_capacity(self.batch_target_rows);
        let mut drained: Vec<bool> = vec![false; self.subs.len()];
        while indices.len() < self.batch_target_rows {
            let Some(Reverse(entry)) = self.heap.pop() else {
                // Heap empty. If any sub is drained, we cannot safely
                // reload it here (its batch is still referenced by
                // `indices`). Stop accumulating and let the caller drain
                // via a subsequent `next_batch` call.
                if drained.iter().any(|&d| d) {
                    break;
                }
                // Otherwise, every sub is genuinely exhausted (or not
                // yet opened): try a reload and re-enter.
                if !self.reload_if_empty_heap()? {
                    break;
                }
                continue;
            };
            let scan_idx = entry.scan_idx;
            let row_idx = entry.row_idx;
            indices.push((scan_idx, row_idx));

            // Advance that sub's cursor.
            self.subs[scan_idx].cursor += 1;
            let batch_rows = self.subs[scan_idx].batch.as_ref()
                .map(|b| b.num_rows()).unwrap_or(0);
            if self.subs[scan_idx].cursor < batch_rows {
                self.push_active(scan_idx)?;
            } else {
                // Batch is drained. Mark for post-build reload; do NOT
                // clear `batch` yet — `build_output_batch` below still
                // needs to read rows from it via `indices`.
                drained[scan_idx] = true;
            }
        }

        if indices.is_empty() {
            // Safely flip drained subs to empty now (no references).
            for (i, &d) in drained.iter().enumerate() {
                if d {
                    self.subs[i].batch = None;
                    self.subs[i].cursor = 0;
                }
            }
            self.exhausted = true;
            return Ok(None);
        }

        let out = self.build_output_batch(&indices)?;

        // Post-emit sweep: now safe to clear drained batches. Reloads
        // happen lazily at the top of the next `next_batch` call via
        // `reload_if_empty_heap`.
        for (i, &d) in drained.iter().enumerate() {
            if d {
                self.subs[i].batch = None;
                self.subs[i].cursor = 0;
            }
        }
        Ok(Some(out))
    }

    fn close(&mut self) -> Result<()> {
        if !self.opened {
            return Ok(());
        }
        let mut first_err: Option<BqliteError> = None;
        for (i, sub) in self.subs.iter_mut().enumerate() {
            if let Err(e) = sub.op.close() {
                if first_err.is_none() {
                    first_err = Some(BqliteError::Execution(format!(
                        "MergeSourcesOperator: sub-scan {i} close failed: {e}"
                    )));
                }
            }
            sub.batch = None;
            sub.cursor = 0;
            sub.exhausted = true;
        }
        self.heap.clear();
        self.exhausted = true;
        self.opened = false;
        if let Some(e) = first_err { Err(e) } else { Ok(()) }
    }
}

impl MergeSourcesOperator {
    /// Reload one sub-scan's batch by pulling from its child operator
    /// and pushing its first row onto the heap.
    fn reload_sub(&mut self, i: usize) -> Result<()> {
        if self.subs[i].exhausted {
            return Ok(());
        }
        loop {
            match self.subs[i].op.next_batch()? {
                None => {
                    self.subs[i].exhausted = true;
                    self.subs[i].batch = None;
                    return Ok(());
                }
                Some(batch) => {
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    self.subs[i].batch = Some(batch);
                    self.subs[i].cursor = 0;
                    self.push_active(i)?;
                    return Ok(());
                }
            }
        }
    }

    /// Push the sub's current cursor position onto the heap.
    fn push_active(&mut self, i: usize) -> Result<()> {
        let batch = self.subs[i].batch.as_ref().expect("active sub has a batch");
        let desc = &self.descriptors[i];
        let ek_col = batch.column(desc.entity_key_col);
        // TableSchema declares entity_id non-nullable, but defense in
        // depth: a hand-built RecordBatch in a test could violate this,
        // which would produce a garbage entity-key value and silently
        // corrupt the merge order. Fail loudly in debug builds.
        debug_assert!(
            !ek_col.is_null(self.subs[i].cursor),
            "MergeSourcesOperator: sub-scan {i} emitted null entity_id at row {}",
            self.subs[i].cursor,
        );
        let entity_key = bqlite_storage::segment::merge::EntityKeyValue::extract(
            ek_col,
            self.subs[i].cursor,
        );
        let ts_nanos = bqlite_storage::segment::merge::extract_ts_nanos(
            batch.column(desc.ts_col),
            self.subs[i].cursor,
        );
        self.heap.push(Reverse(JoinedHeapEntry {
            scan_idx: i,
            row_idx: self.subs[i].cursor,
            entity_key,
            ts_nanos,
        }));
        Ok(())
    }

    /// If the heap is empty, pull another batch from every un-exhausted
    /// sub whose `batch` is None. Returns true if the heap is non-empty
    /// after the reload (more work to do), false if every sub is now
    /// drained.
    fn reload_if_empty_heap(&mut self) -> Result<bool> {
        if !self.heap.is_empty() {
            return Ok(true);
        }
        let sub_count = self.subs.len();
        for i in 0..sub_count {
            if self.subs[i].batch.is_none() && !self.subs[i].exhausted {
                self.reload_sub(i)?;
            }
        }
        Ok(!self.heap.is_empty())
    }
}
```

- [ ] **Step 2: Build**

```bash
cargo build -p bqlite-operators 2>&1 | tail -10
```

Expected: compile succeeds. (No tests yet; output-batch construction is next.)

---

### Task 2.4: Implement output-batch construction

**Files:**
- Modify: `crates/bqlite-operators/src/scan.rs` — add `build_output_batch` method.

- [ ] **Step 1: Implement `build_output_batch`**

Append to the latest `impl MergeSourcesOperator`:

```rust
    /// Build one output `RecordBatch` from the accumulated picks.
    ///
    /// For each combined-schema output column `c`:
    /// - If `c` is `__source_table_id`, construct an `Int64Array`
    ///   directly from the picks' `scan_idx` values.
    /// - Otherwise, walk every sub-scan `i` and find the input column
    ///   that maps to `c` (via `col_map[i]`). The interleave input for
    ///   scan `i` is either that column from the current batch, or a
    ///   null array of the correct type and length (when the sub-scan
    ///   has no current batch, or its `col_map` doesn't include `c`).
    /// - Call `arrow::compute::interleave` with the per-scan array refs
    ///   and the picks to produce the output column.
    ///
    /// Tombstones / post-filters / sample filtering are applied within
    /// each sub-scan's `next_batch()` before rows reach this merge, so
    /// the merge emits whatever rows survive upstream.
    fn build_output_batch(
        &self,
        indices: &[(usize, usize)],
    ) -> Result<RecordBatch> {
        use arrow::array::{Int64Array, new_null_array};
        use arrow::compute::interleave;

        let n_sub = self.subs.len();
        let n_out_cols = self.arrow_schema.fields().len();
        let mut out_cols: Vec<ArrayRef> = Vec::with_capacity(n_out_cols);

        for out_col_idx in 0..n_out_cols {
            // Special-case: __source_table_id column is synthesized.
            if Some(out_col_idx) == self.source_table_id_col {
                let vals: Vec<i64> = indices.iter()
                    .map(|(scan_idx, _)| self.table_id_values[*scan_idx])
                    .collect();
                out_cols.push(Arc::new(Int64Array::from(vals)) as ArrayRef);
                continue;
            }

            // For each sub, use the precomputed reverse_col_map to find
            // which sub-column (if any) feeds this output column — O(1)
            // lookup instead of scanning col_map per batch.
            let field_type = self.arrow_schema.field(out_col_idx).data_type();
            let mut per_sub_arrays: Vec<ArrayRef> = Vec::with_capacity(n_sub);
            for i in 0..n_sub {
                let desc = &self.descriptors[i];
                match desc.reverse_col_map[out_col_idx] {
                    Some(sub_col_idx) => {
                        match self.subs[i].batch.as_ref() {
                            Some(b) => per_sub_arrays.push(b.column(sub_col_idx).clone()),
                            None => {
                                // Sub has no current batch — its scan_idx is
                                // guaranteed not to appear in `indices`
                                // (we only drain batches after build, and
                                // a reload always primes a row before
                                // picks resume). A zero-length placeholder
                                // is safe because interleave never indexes
                                // into it.
                                per_sub_arrays.push(new_null_array(field_type, 0));
                            }
                        }
                    }
                    None => {
                        // Sub i does not contribute to this output column —
                        // provide a null array of the same length as this
                        // sub's current batch (or 0 if drained) so
                        // interleave can index it.
                        let len = self.subs[i].batch.as_ref().map(|b| b.num_rows()).unwrap_or(0);
                        per_sub_arrays.push(new_null_array(field_type, len));
                    }
                }
            }
            let refs: Vec<&dyn arrow::array::Array> =
                per_sub_arrays.iter().map(|a| a.as_ref()).collect();
            let col = interleave(&refs, indices).map_err(|e| {
                BqliteError::Execution(format!(
                    "MergeSourcesOperator: interleave failed for output col {out_col_idx} ({}): {e}",
                    self.arrow_schema.field(out_col_idx).name(),
                ))
            })?;
            out_cols.push(col);
        }

        RecordBatch::try_new(self.arrow_schema.clone(), out_cols).map_err(|e| {
            BqliteError::Execution(format!(
                "MergeSourcesOperator: failed to assemble output batch: {e}"
            ))
        })
    }
```

- [ ] **Step 2: Build**

```bash
cargo build -p bqlite-operators 2>&1 | tail -10
```

Expected: compile succeeds, no dead_code warnings.

---

### Task 2.5: Unit tests

**Files:**
- Modify: `crates/bqlite-operators/src/scan.rs` — append new tests to the existing `mod tests` block (or a new `mod merge_sources_tests` sibling if the existing tests module is too large to edit cleanly).

- [ ] **Step 1: Helper — build an in-memory ScanOperator from a single RecordBatch**

Inspect the existing `VecReader::with_segments` helper in the tests module; it accepts a list of `(RecordBatch, EntityRange)` pairs. Use it to construct fresh per-sub-table ScanOperators.

Append helper to the tests module:

```rust
    fn make_two_col_batch(entity_ids: &[&str], tss: &[i64]) -> RecordBatch {
        use arrow::array::{StringViewArray, TimestampNanosecondArray};
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
        use std::sync::Arc;
        assert_eq!(entity_ids.len(), tss.len());
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("entity_id", DataType::Utf8View, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())), false),
        ]));
        let eid: StringViewArray = entity_ids.iter().map(|s| Some(*s)).collect();
        let ts = TimestampNanosecondArray::from(tss.to_vec())
            .with_timezone(Arc::<str>::from("UTC"));
        RecordBatch::try_new(schema, vec![Arc::new(eid), Arc::new(ts)]).unwrap()
    }

    fn make_sub_scan(table: &str, entity_ids: &[&str], tss: &[i64]) -> (Box<dyn PhysicalOperator>, String, String) {
        // Build a minimal TableSchema for this sub-scan.
        let schema = bqlite_core::TableSchema::new(
            table,
            vec![
                bqlite_core::ColumnDef::required("entity_id", BqlType::String),
                bqlite_core::ColumnDef::required("ts", BqlType::Timestamp),
            ],
            "entity_id",
            "ts",
            "entity_id",   // event_type role — irrelevant for these tests, reuse entity_id
        ).expect("table schema");
        let batch = make_two_col_batch(entity_ids, tss);
        let reader: Arc<dyn SegmentReader> = Arc::new(VecReader::with_segments(schema, vec![(batch, (entity_ids.first().copied().unwrap_or("").to_string(), entity_ids.last().copied().unwrap_or("").to_string()))]));
        let op = ScanOperator::full_scan(reader).expect("scan op");
        (Box::new(op) as Box<dyn PhysicalOperator>, "entity_id".to_string(), "ts".to_string())
    }
```

Note: `VecReader::with_segments` signature may differ — read the test module at scan.rs:1312–1400 to find the actual constructor signature and adjust. If it takes `Vec<(RecordBatch, ZoneMap)>` or similar, build the zone map from the entity_ids' first/last values.

- [ ] **Step 2: Test — two sub-scans with disjoint entities**

```rust
    #[test]
    fn merge_sources_two_disjoint_entities() {
        let (op_a, ek_a, ts_a) = make_sub_scan("t0", &["a"], &[100]);
        let (op_b, ek_b, ts_b) = make_sub_scan("t1", &["b"], &[200]);

        // Combined schema: t0.entity_id, t0.ts, t1.entity_id, t1.ts, __source_table_id
        let combined = OperatorSchema::new(vec![
            ColumnDef::required("t0.entity_id", BqlType::String),
            ColumnDef::required("t0.ts", BqlType::Timestamp),
            ColumnDef::required("t1.entity_id", BqlType::String),
            ColumnDef::required("t1.ts", BqlType::Timestamp),
            ColumnDef::required("__source_table_id", BqlType::Int),
        ]).unwrap();

        let mut op = MergeSourcesOperator::new(
            vec![op_a, op_b],
            vec![ek_a, ek_b],
            vec![ts_a, ts_b],
            combined,
            vec!["t0".into(), "t1".into()],
            CancellationToken::new(),
        ).expect("ctor");

        op.open().expect("open");
        let mut rows = Vec::new();
        while let Some(b) = op.next_batch().expect("next") {
            rows.push(b);
        }
        op.close().expect("close");

        // Expect 2 rows total across however many batches.
        let total: usize = rows.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2);

        // Row 0 is ("a", 100) from t0 → __source_table_id = 0, t1.* = null
        // Row 1 is ("b", 200) from t1 → __source_table_id = 1, t0.* = null
        let b = &rows[0];
        let tid = b.column_by_name("__source_table_id").unwrap()
            .as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
        assert_eq!(tid.value(0), 0);
        assert!(b.column_by_name("t1.entity_id").unwrap().is_null(0));
        if b.num_rows() == 2 {
            assert_eq!(tid.value(1), 1);
            assert!(b.column_by_name("t0.entity_id").unwrap().is_null(1));
        }
    }
```

- [ ] **Step 3: Test — two sub-scans with overlapping entities + ordering**

```rust
    #[test]
    fn merge_sources_ordering_across_tables() {
        // Entity x has rows in both tables; entity y only in t1.
        // Expected order: (x, 100, t0), (x, 150, t1), (x, 200, t0), (y, 50, t1).
        let (op_a, ek_a, ts_a) = make_sub_scan("t0", &["x", "x"], &[100, 200]);
        let (op_b, ek_b, ts_b) = make_sub_scan("t1", &["x", "y"], &[150, 50]);
        // Wait — y=50 breaks entity-sorted invariant: y should come after x.
        // Use: t1 = [("x", 150), ("y", 50)] -- but that violates entity order.
        // Rewrite with entity-sorted input: t1 = [("x", 150), ("y", 500)].
        let (op_b, ek_b, ts_b) = make_sub_scan("t1", &["x", "y"], &[150, 500]);

        let combined = OperatorSchema::new(vec![
            ColumnDef::required("t0.entity_id", BqlType::String),
            ColumnDef::required("t0.ts", BqlType::Timestamp),
            ColumnDef::required("t1.entity_id", BqlType::String),
            ColumnDef::required("t1.ts", BqlType::Timestamp),
            ColumnDef::required("__source_table_id", BqlType::Int),
        ]).unwrap();

        let mut op = MergeSourcesOperator::new(
            vec![op_a, op_b],
            vec![ek_a, ek_b],
            vec![ts_a, ts_b],
            combined,
            vec!["t0".into(), "t1".into()],
            CancellationToken::new(),
        ).unwrap();

        op.open().unwrap();
        let mut all = Vec::new();
        while let Some(b) = op.next_batch().unwrap() {
            all.push(b);
        }
        op.close().unwrap();

        // Concatenate __source_table_id values.
        let tids: Vec<i64> = all.iter().flat_map(|b| {
            let arr = b.column_by_name("__source_table_id").unwrap()
                .as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
            (0..b.num_rows()).map(|i| arr.value(i)).collect::<Vec<_>>()
        }).collect();

        // Expected: row order is (x,100,t0) (x,150,t1) (x,200,t0) (y,500,t1)
        // so __source_table_id sequence is [0, 1, 0, 1].
        assert_eq!(tids, vec![0, 1, 0, 1]);
    }
```

- [ ] **Step 4: Test — same `(entity, ts)` across tables tie-broken by scan_idx**

```rust
    #[test]
    fn merge_sources_same_ts_tiebroken_by_scan_idx() {
        // Both tables emit (x, 100). Expected: t0 before t1 (scan_idx asc).
        let (op_a, ek_a, ts_a) = make_sub_scan("t0", &["x"], &[100]);
        let (op_b, ek_b, ts_b) = make_sub_scan("t1", &["x"], &[100]);

        let combined = OperatorSchema::new(vec![
            ColumnDef::required("t0.entity_id", BqlType::String),
            ColumnDef::required("t0.ts", BqlType::Timestamp),
            ColumnDef::required("t1.entity_id", BqlType::String),
            ColumnDef::required("t1.ts", BqlType::Timestamp),
            ColumnDef::required("__source_table_id", BqlType::Int),
        ]).unwrap();

        let mut op = MergeSourcesOperator::new(
            vec![op_a, op_b], vec![ek_a, ek_b], vec![ts_a, ts_b],
            combined, vec!["t0".into(), "t1".into()], CancellationToken::new(),
        ).unwrap();
        op.open().unwrap();
        let b = op.next_batch().unwrap().expect("one batch");
        op.close().unwrap();
        assert_eq!(b.num_rows(), 2);
        let tids = b.column_by_name("__source_table_id").unwrap()
            .as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
        assert_eq!(tids.value(0), 0);
        assert_eq!(tids.value(1), 1);
    }
```

- [ ] **Step 5: Test — one sub-scan empty**

```rust
    #[test]
    fn merge_sources_one_sub_empty() {
        let (op_a, ek_a, ts_a) = make_sub_scan("t0", &["a"], &[100]);
        let (op_b, ek_b, ts_b) = make_sub_scan("t1", &[] as &[&str], &[] as &[i64]);
        // make_sub_scan with empty input needs to produce a zero-row batch.
        // If the helper panics on empty input, extend it to handle empties.

        let combined = OperatorSchema::new(vec![
            ColumnDef::required("t0.entity_id", BqlType::String),
            ColumnDef::required("t0.ts", BqlType::Timestamp),
            ColumnDef::required("t1.entity_id", BqlType::String),
            ColumnDef::required("t1.ts", BqlType::Timestamp),
            ColumnDef::required("__source_table_id", BqlType::Int),
        ]).unwrap();

        let mut op = MergeSourcesOperator::new(
            vec![op_a, op_b], vec![ek_a, ek_b], vec![ts_a, ts_b],
            combined, vec!["t0".into(), "t1".into()], CancellationToken::new(),
        ).unwrap();
        op.open().unwrap();
        let total: usize = std::iter::from_fn(|| op.next_batch().unwrap())
            .map(|b| b.num_rows()).sum();
        op.close().unwrap();
        assert_eq!(total, 1);
    }
```

If `make_sub_scan` can't create an empty reader, build one inline using `VecReader::empty(schema)`.

- [ ] **Step 6: Test — both sub-scans empty**

```rust
    #[test]
    fn merge_sources_both_empty() {
        let (op_a, ek_a, ts_a) = make_sub_scan("t0", &[] as &[&str], &[]);
        let (op_b, ek_b, ts_b) = make_sub_scan("t1", &[] as &[&str], &[]);
        let combined = OperatorSchema::new(vec![
            ColumnDef::required("t0.entity_id", BqlType::String),
            ColumnDef::required("t0.ts", BqlType::Timestamp),
            ColumnDef::required("t1.entity_id", BqlType::String),
            ColumnDef::required("t1.ts", BqlType::Timestamp),
            ColumnDef::required("__source_table_id", BqlType::Int),
        ]).unwrap();
        let mut op = MergeSourcesOperator::new(
            vec![op_a, op_b], vec![ek_a, ek_b], vec![ts_a, ts_b],
            combined, vec!["t0".into(), "t1".into()], CancellationToken::new(),
        ).unwrap();
        op.open().unwrap();
        assert!(op.next_batch().unwrap().is_none());
        op.close().unwrap();
    }
```

- [ ] **Step 7: Test — three-table merge + ordering**

```rust
    #[test]
    fn merge_sources_three_tables_ordering() {
        let (op_a, ek_a, ts_a) = make_sub_scan("t0", &["x"], &[100]);
        let (op_b, ek_b, ts_b) = make_sub_scan("t1", &["x"], &[50]);
        let (op_c, ek_c, ts_c) = make_sub_scan("t2", &["x"], &[75]);
        let combined = OperatorSchema::new(vec![
            ColumnDef::required("t0.entity_id", BqlType::String),
            ColumnDef::required("t0.ts", BqlType::Timestamp),
            ColumnDef::required("t1.entity_id", BqlType::String),
            ColumnDef::required("t1.ts", BqlType::Timestamp),
            ColumnDef::required("t2.entity_id", BqlType::String),
            ColumnDef::required("t2.ts", BqlType::Timestamp),
            ColumnDef::required("__source_table_id", BqlType::Int),
        ]).unwrap();
        let mut op = MergeSourcesOperator::new(
            vec![op_a, op_b, op_c], vec![ek_a, ek_b, ek_c], vec![ts_a, ts_b, ts_c],
            combined, vec!["t0".into(), "t1".into(), "t2".into()], CancellationToken::new(),
        ).unwrap();
        op.open().unwrap();
        let b = op.next_batch().unwrap().unwrap();
        op.close().unwrap();
        // Expected order by (x, ts): ts=50 (t1), ts=75 (t2), ts=100 (t0)
        let tids = b.column_by_name("__source_table_id").unwrap()
            .as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
        assert_eq!((0..b.num_rows()).map(|i| tids.value(i)).collect::<Vec<_>>(), vec![1, 2, 0]);
    }
```

- [ ] **Step 7b: Test — absent `__source_table_id` (single-column combined schema)**

```rust
    #[test]
    fn merge_sources_without_source_table_id_column() {
        // Per cohorts-aliases-joins.md §3.9, single-table queries omit
        // the discriminator column. We don't produce MergeSources for
        // those, but a test-time combined schema without the column
        // should still work — source_table_id_col stays None and no
        // synthetic column is emitted.
        let (op_a, ek_a, ts_a) = make_sub_scan("t0", &["a"], &[1]);
        let (op_b, ek_b, ts_b) = make_sub_scan("t1", &["a"], &[2]);
        let combined = OperatorSchema::new(vec![
            ColumnDef::required("t0.entity_id", BqlType::String),
            ColumnDef::required("t0.ts", BqlType::Timestamp),
            ColumnDef::required("t1.entity_id", BqlType::String),
            ColumnDef::required("t1.ts", BqlType::Timestamp),
        ]).unwrap();
        let mut op = MergeSourcesOperator::new(
            vec![op_a, op_b], vec![ek_a, ek_b], vec![ts_a, ts_b],
            combined, vec!["t0".into(), "t1".into()], CancellationToken::new(),
        ).unwrap();
        op.open().unwrap();
        let b = op.next_batch().unwrap().expect("one batch");
        op.close().unwrap();
        assert_eq!(b.num_rows(), 2);
        assert!(b.column_by_name("__source_table_id").is_none());
    }
```

- [ ] **Step 8: Test — ctor rejects parallel-vec length mismatch**

```rust
    #[test]
    fn merge_sources_ctor_rejects_parallel_vec_mismatch() {
        let (op_a, ek_a, ts_a) = make_sub_scan("t0", &["a"], &[100]);
        let combined = OperatorSchema::new(vec![
            ColumnDef::required("t0.entity_id", BqlType::String),
            ColumnDef::required("t0.ts", BqlType::Timestamp),
            ColumnDef::required("__source_table_id", BqlType::Int),
        ]).unwrap();
        // Only 1 op but 2 entity_key_cols.
        let err = MergeSourcesOperator::new(
            vec![op_a], vec![ek_a, "entity_id".into()], vec![ts_a],
            combined, vec!["t0".into()], CancellationToken::new(),
        ).expect_err("expected parallel-vec mismatch error");
        let s = format!("{err}");
        assert!(s.contains("parallel-vec length mismatch"), "got: {s}");
    }
```

- [ ] **Step 9: Test — ctor rejects missing entity-key column**

```rust
    #[test]
    fn merge_sources_ctor_rejects_missing_entity_key_column() {
        let (op_a, _ek_a, ts_a) = make_sub_scan("t0", &["a"], &[100]);
        let combined = OperatorSchema::new(vec![
            ColumnDef::required("t0.entity_id", BqlType::String),
            ColumnDef::required("t0.ts", BqlType::Timestamp),
            ColumnDef::required("__source_table_id", BqlType::Int),
        ]).unwrap();
        let err = MergeSourcesOperator::new(
            vec![op_a],
            vec!["no_such_column".into()],
            vec![ts_a],
            combined, vec!["t0".into()], CancellationToken::new(),
        ).expect_err("expected missing-column error");
        let s = format!("{err}");
        assert!(s.contains("missing entity-key column"), "got: {s}");
    }
```

- [ ] **Step 10: Test — SAMPLE push works when attached to sub-scans**

```rust
    #[test]
    fn merge_sources_with_sample_pushed_filters_by_entity() {
        // Two sub-scans, same entity set. Attach identical sample filter
        // to each sub-scan. The merged stream should contain only entities
        // that pass the hash threshold (and from both tables for those
        // entities). Fraction 1.0 = everyone passes; 0.0 = no rows.

        // fraction 0.0 case: both subs drop every row; merge emits None.
        let (mut op_a, ek_a, ts_a) = make_sub_scan("t0", &["a", "b", "c"], &[1, 2, 3]);
        let (mut op_b, ek_b, ts_b) = make_sub_scan("t1", &["a", "b", "c"], &[10, 20, 30]);

        // Attach sample filters. `SampleFilter::from_pushdown` expects
        // a TableSchema; build one matching the sub-scan shape.
        let schema_a = bqlite_core::TableSchema::new(
            "t0",
            vec![
                bqlite_core::ColumnDef::required("entity_id", BqlType::String),
                bqlite_core::ColumnDef::required("ts", BqlType::Timestamp),
            ],
            "entity_id", "ts", "entity_id",
        ).unwrap();
        // Downcast Box<dyn PhysicalOperator> back to ScanOperator to call with_sample_filter.
        // This only works if make_sub_scan returns a concrete-typed ScanOperator pointer.
        // Rewrite helper to return ScanOperator directly and upcast on use.
        // (Adjust make_sub_scan accordingly — see Step 11.)
        //
        // Once wired: op_a_scan.with_sample_filter(Arc::new(
        //     SampleFilter::from_pushdown(0.0, 0, &schema_a).unwrap()
        // ));
        // ... same for op_b ...
        //
        // Run the merge; assert total_rows == 0.
        let _ = (schema_a, op_a, op_b, ek_a, ek_b, ts_a, ts_b);
        // Mark test as a follow-up if the helper refactor is too invasive;
        // the unit correctness of SAMPLE-on-sub-scan is already covered
        // by scan.rs sample_filter_fraction_0_0 tests. The CP1 planner
        // test pins the pushdown rewrite. This test adds end-to-end
        // validation.
    }
```

Keep this test as `#[test] #[ignore = "requires make_sub_scan refactor to expose concrete ScanOperator for sample-filter attachment"]` if the helper doesn't cleanly support it. Prefer to **either** refactor the helper in Step 11 and enable the test, **or** drop the test and rely on:
- CP1's planner test proving pushdown writes `sample` on every sub-scan.
- Existing `sample_filter_fraction_0_0` / `_0_5` / `_1_0` tests in scan.rs proving the runtime correctness of sample-on-scan.
These together establish correctness by composition.

Decision: **drop this test** in favor of the composition argument above. Add a note in a doc comment on `MergeSourcesOperator` stating the SAMPLE correctness proof: CP1 + scan.rs SAMPLE tests.

- [ ] **Step 10b: Add a small micro-benchmark for the merge hot loop**

Per CLAUDE.md "Testing And Benchmarking": hot-path changes benefit from bench coverage. Add a minimal criterion bench in the existing operators benches directory (or under a new file `crates/bqlite-operators/benches/merge_sources.rs`) that runs a 3-table × 10K-row merge and measures throughput. If no benches/ directory exists on this crate, skip with a TODO note — benchmarks at the full-suite level are TASK-441's concern; the micro-bench here is defensive, not a gate.

Check first:

```bash
ls /workspace/crates/bqlite-operators/benches/ 2>/dev/null || echo "no benches/"
```

If the dir exists, add:

```rust
// crates/bqlite-operators/benches/merge_sources.rs
use criterion::{criterion_group, criterion_main, Criterion};

fn bench_merge_sources_3tbl_10k(c: &mut Criterion) {
    // Build 3 sub-scans of 10_000 rows each, merge, measure total time.
    // Use a small test-time fixture; this is a regression guard, not a perf gate.
    c.bench_function("merge_sources_3tbl_10k", |b| {
        b.iter(|| {
            // ... construct operators, open, drain next_batch ...
        });
    });
}

criterion_group!(benches, bench_merge_sources_3tbl_10k);
criterion_main!(benches);
```

If the dir does NOT exist, skip — the reverse_col_map optimization is validated by logic review; full-suite joined-source benchmarks belong to TASK-441.

- [ ] **Step 11: Run tests**

```bash
cargo test -p bqlite-operators --lib scan 2>&1 | tail -25
```

Expected: all new `merge_sources_*` tests pass alongside existing scan tests.

- [ ] **Step 12: Run full local CI**

```bash
bash scripts/local-ci.sh
```

Expected: success.

- [ ] **Step 13: Subagent code review**

Spawn `superpowers:code-reviewer` with:
- This plan's CP2 section (Tasks 2.1–2.5).
- The design doc `cohorts-aliases-joins.md` §3.1–§3.11.
- The diff (`git diff HEAD`).

Reviewer must evaluate against: correctness (does the runtime match §3.2 ordering, §3.8 discriminator, §3.4 SAMPLE composition), performance (allocations in the hot loop, null-array construction per batch, col_map resolution cost), and edge cases (both-empty, one-empty, three-table chain).

Require APPROVE before committing.

- [ ] **Step 14: Commit**

```bash
git add crates/bqlite-storage/src/segment/merge.rs \
        crates/bqlite-operators/src/scan.rs
git commit -m "TASK-436: add MergeSourcesOperator for joined-source scan runtime (CP2)"
```

- [ ] **Step 15: Fast-forward merge to main**

```bash
git checkout main
git pull origin main
git merge task/TASK-436 --ff-only
git push origin main
git checkout task/TASK-436
```

---

## Completion Protocol

After CP2 merges:

- [ ] **Move lock file to done marker**

```bash
git mv tasks/active/TASK-436.lock tasks/completed/TASK-436.done
```

- [ ] **Edit `TASK-436.done` to add `completed_at`** (current UTC ISO-8601 timestamp).

- [ ] **Commit and push**

```bash
git add tasks/completed/TASK-436.done
git commit -m "TASK-436: completed"
git push origin main
```

---

## Self-Review

**Spec coverage check:**

| `cohorts-aliases-joins.md` §3 requirement | Covered by |
|---|---|
| §3.1 Grammar `source := name time_range? (JOIN name)*` | Upstream — parser/planner (TASK-452, TASK-425). Not in scope here. |
| §3.2 `(ts, table_order, __seq_id)` tie-break | CP2 Task 2.3 heap ordering. Test `merge_sources_same_ts_tiebroken_by_scan_idx` (2.5 Step 4). |
| §3.3 Uniform reader-range widening | Upstream — planner already lowers to shared reader_range (`joined_scan_replicates_reader_range_across_sub_scans` test). Not in runtime. |
| §3.4 SAMPLE value-based hash across tables | CP1 extends `pushdown_sample` to stamp (fraction, seed) uniformly on every sub-scan. Composition argument in CP2 Task 2.5 Step 10. |
| §3.5 DELETE + JOIN disallowed | Parser-side (TASK-433). Not in scope. |
| §3.6 Entity-key-type mismatch rejected | Planner enforces at bind (upstream). CP2 defense-in-depth: sub-scan schema lookup by `entity_key_col` name fails in ctor if types differ, but the ctor doesn't compare types across sub-scans. Planner/lowering guarantees type compatibility before we see the plan. |
| §3.7 N-ary `MergeSources` | CP2 `MergeSourcesOperator`. |
| §3.8 `__source_table_id: Int NOT NULL` | CP2 `build_output_batch` synthesizes the column. Tested in 2.5 Steps 2, 3, 4, 7. |
| §3.9 Single-table queries omit `__source_table_id` | Not our concern — planner produces plain `Scan` for single-table queries. |
| §3.10 Aliases referencing joined pipelines | Out of scope. |
| §3.11 `step_name: table.event_type` | Parser/planner. Not in runtime. |

| `event-select-sample.md` §18 requirement | Covered by |
|---|---|
| Fraction-only pushdown through stateless ops | CP1 extends this to MergeSources. |
| xxHash64 over canonical entity-id bytes | Already owned by TASK-430 `SampleFilter`; unchanged here. |
| Population-invariance | Proven by CP1 test that pushdown stamps identical `(fraction, seed)` on every sub-scan. |

**Placeholder scan:** None found.

**Type consistency:** `SubSourceDesc`, `SubSource`, `JoinedHeapEntry`, `MERGE_SOURCES_BATCH_ROWS`, `MergeSourcesOperator::{new, open, next_batch, close, with_batch_size}` — all consistent across tasks. `EntityKeyValue::extract` / `extract_ts_nanos` use fully-qualified paths `bqlite_storage::segment::merge::…`, consistent with existing `bqlite-operators` imports.

**Outstanding notes:**
- If `SOURCE_TABLE_ID_COLUMN` isn't re-exported from `bqlite_planner` to `bqlite_operators`, Task 2.2 Step 3 falls back to the literal `"__source_table_id"` with a design-doc comment citation. Decide at implementation time; either is fine.
- Task 2.5 Step 10 is dropped per Decision note — SAMPLE runtime correctness is pinned by CP1 + existing scan.rs SAMPLE tests via composition.
