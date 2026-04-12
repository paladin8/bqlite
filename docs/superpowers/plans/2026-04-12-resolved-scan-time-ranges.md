# Resolved Scan Time Ranges — Implementation Plan

**Date:** 2026-04-12

## Goal

Remove the `bqlite_ast::TimeRange` dependency from `bqlite-engine`. The planner resolves both
`query_range` and `reader_range` to `bqlite_core::TimeRange` before the engine sees them. The engine
reads two pre-resolved fields from `ScanPhysical` and never imports AST types for this purpose.

## Semantic clarification

MATCH WITHIN is a **forward** extension: the entry event (first step) falls within the query range;
subsequent steps may occur up to `window_ns` later, so the segment reader must look forward past the
query end. Both `Last` and `Between` behave identically — the reader extends the **end** of the
resolved range, not the start. The current `Last` backward-extension behaviour is a bug fixed here.

- `query_range` — user-stated range; drives row-level timestamp predicates.
- `reader_range` — query range extended forward by accumulated `reader_forward_ns` (and backward by
  `reader_backward_ns` for future operators like ATTRIBUTE); drives segment reader selection.

## Steps

### Step 1 — Add directional extension fields to `LogicalPlan::Scan` (`logical.rs`)

Add two fields to the `LogicalPlan::Scan` variant:

```rust
reader_backward_ns: i64,  // default 0
reader_forward_ns:  i64,  // default 0
```

Add constructor `LogicalPlan::scan_full` initialises both to `0`. All existing `Scan` construction
sites set them to `0`.

Replace `extend_scan_time_range(&mut self, extension_ns: i64)` with two methods:

```rust
pub(crate) fn extend_scan_reader_backward(&mut self, ns: i64) -> Result<()>
pub(crate) fn extend_scan_reader_forward(&mut self, ns: i64)  -> Result<()>
```

Each walks `Filter`/`Project`/`Limit` wrappers to the innermost `Scan` and accumulates into the
respective field. No-op when `time_range` is `None`.

Update the MATCH lowering call site (line ~1154 in `logical.rs`):
```rust
// Before:
acc.extend_scan_time_range(extension)?;
// After:
acc.extend_scan_reader_forward(extension)?;
```

Update `logical.rs` tests:
- `scan_time_range_extended_by_match_within_window`: `Last(30d) + WITHIN 7d` → `reader_forward_ns = 7d`, `time_range` stays `Last(30d)`. (Previously this test expected `Last(37d)` — that was the bug.)
- `scan_between_range_extended_by_match_within_window`: unchanged semantics, but now `reader_forward_ns` carries the extension instead of mutating the end string.
- `scan_time_range_not_extended_when_no_window`: add assertions that both extension fields remain `0`.

### Step 2 — Move resolution helpers into `physical.rs`, add `now_ns` to `lower_physical`

Move `parse_time_range_timestamp` and `extend_between_end` from `logical.rs` to `physical.rs` as
private helpers (or delete them from `logical.rs` if no longer needed there).

Add a private resolution function in `physical.rs`:

```rust
fn resolve_ast_time_range(
    tr: Option<&bqlite_ast::pipeline::TimeRange>,
    now_ns: i64,
) -> Option<bqlite_core::TimeRange>
```

- `None` → `None`
- `Last(ns)` → `Some([now_ns - ns, now_ns))`
- `Between { start, end }` → `Some([parse(start), parse(end) + 1))`

Add a private extension function:

```rust
fn apply_reader_extension(
    base: Option<bqlite_core::TimeRange>,
    backward_ns: i64,
    forward_ns: i64,
) -> Option<bqlite_core::TimeRange>
```

- `None` → `None`
- `Some(r)` → `Some(TimeRange::new(r.start.saturating_sub(backward_ns), r.end.saturating_add(forward_ns)))`
  where saturation clamps to `Timestamp::MIN` / `Timestamp::MAX`.

Change signature:

```rust
pub fn lower_physical(plan: LogicalPlan, now_ns: i64) -> PhysicalPlan
```

The recursive calls inside `lower_physical` all forward `now_ns` unchanged.

### Step 3 — Replace `ScanPhysical.time_range` with two resolved fields (`physical.rs`)

Remove:
```rust
pub time_range: Option<bqlite_ast::pipeline::TimeRange>,
```

Add:
```rust
pub query_range:  Option<bqlite_core::TimeRange>,
pub reader_range: Option<bqlite_core::TimeRange>,
```

In the `LogicalPlan::Scan` arm of `lower_physical`:

```rust
let query_range = resolve_ast_time_range(time_range.as_ref(), now_ns);
let reader_range = apply_reader_extension(query_range, reader_backward_ns, reader_forward_ns);
PhysicalPlan::Scan(ScanPhysical {
    table: table.name().to_string(),
    query_range,
    reader_range,
    scan_predicates: compiled_predicates,
    projected_columns,
    output_schema,
    entity_key_col: table.entity_key_column().name.clone(),
    timestamp_col:  table.timestamp_column().name.clone(),
})
```

Fix all `ScanPhysical` construction sites in `physical.rs` tests and in optimizer passes
(`opt/prune.rs`, `opt/pushdown.rs`, `opt/fuse_match_aggregate.rs`):
- Replace `time_range: None` with `query_range: None, reader_range: None`.

Update `physical.rs` tests:
- All `lower_physical(logical)` calls → `lower_physical(logical, 0)`.
- All `crate::plan(stmt, &catalog)` calls → `crate::plan(stmt, &catalog, 0)` (see Step 4).
- The `lower_bare_query_produces_scan_with_empty_optimizer_fields` test: assert `query_range` and
  `reader_range` are both `None`.

### Step 4 — Add `now_ns` to `plan()` (`lib.rs`)

```rust
pub fn plan(statement: Statement, catalog: &dyn Catalog, now_ns: i64) -> Result<PhysicalPlan> {
    let logical = lower_statement(statement, catalog)?;
    let physical = lower_physical(logical, now_ns);
    // ... existing optimizer passes unchanged ...
}
```

Update all `plan()` call sites in `lib.rs` tests: add `0` as the third argument.

### Step 5 — Simplify engine bind (`bind.rs`, `query.rs`)

In `bind.rs`:
- Remove `use bqlite_ast::TimeRange as AstTimeRange`.
- Delete `resolve_scan_time_range`, `resolve_scan_time_range_at`, `parse_query_timestamp`.
- Rewrite `bind_scan`:

```rust
fn bind_scan(scan: &ScanPhysical, db: &Database) -> Result<Box<dyn PhysicalOperator>> {
    let reader_range = scan.reader_range.unwrap_or_else(TimeRange::unbounded);
    let reader_box = db.segment_reader_for_time_range(&scan.table, reader_range)?;
    let reader: Arc<dyn SegmentReader> = Arc::from(reader_box);
    let mut scan_predicates = scan.scan_predicates.clone();
    scan_predicates.extend(build_time_range_predicates(scan, reader_range)?);
    // ... rest unchanged ...
}
```

- `build_time_range_predicates` already takes `ScanPhysical` + `TimeRange`; no signature change needed.

Update `bind.rs` tests:
- `ScanPhysical` constructions using `time_range: Some(AstTimeRange::...)` → set `query_range` and
  `reader_range` to the equivalent resolved `core::TimeRange`.
- `resolve_last_time_range_uses_closed_open_bounds` and `resolve_between_time_range_makes_end_exclusive`:
  these tested the deleted `resolve_scan_time_range_at`. Delete them (behaviour is now tested in
  `physical.rs` via the resolution helpers).
- The `plan(stmt, &catalog)` call at line 888 → `plan(stmt, &catalog, 0)`.

In `query.rs`:
- Read the system clock once before calling `plan()`:

```rust
let now_ns = std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH)
    .map_err(|e| BqliteError::Execution(format!("system clock error: {e}")))?
    .as_nanos()
    .try_into()
    .unwrap_or(i64::MAX);
let physical = bqlite_planner::plan(statement, &catalog, now_ns)?;
```

### Step 6 — Verify

```bash
cargo build
cargo test
cargo clippy --all-targets --all-features -- -D warnings
```

## Files changed

| File | Change summary |
|------|----------------|
| `crates/bqlite-planner/src/logical.rs` | Add extension fields; replace `extend_scan_time_range` with two directional methods; update MATCH call site; update tests |
| `crates/bqlite-planner/src/physical.rs` | Replace `time_range` with `query_range`+`reader_range`; add resolution helpers; `lower_physical` gains `now_ns`; update tests |
| `crates/bqlite-planner/src/lib.rs` | `plan()` gains `now_ns`; update test call sites |
| `crates/bqlite-planner/src/opt/prune.rs` | `ScanPhysical` construction: `time_range: None` → two fields |
| `crates/bqlite-planner/src/opt/pushdown.rs` | Same |
| `crates/bqlite-planner/src/opt/fuse_match_aggregate.rs` | Same |
| `crates/bqlite-engine/src/bind.rs` | Remove AstTimeRange import + resolution fns; use resolved fields; update tests |
| `crates/bqlite-engine/src/query.rs` | Read clock once; pass `now_ns` to `plan()` |
