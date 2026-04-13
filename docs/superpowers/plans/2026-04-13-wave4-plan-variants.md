# Wave 4 Logical + Physical Plan Variants Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Wave 4 query-side plan variants to `LogicalPlan` and `PhysicalPlan` enums, plus physical descriptors, logical→physical lowering, and EXPLAIN rendering.

**Architecture:** Extends the existing planner enum pattern (logical node + physical descriptor + explain rendering) for 5 new logical variants (Sessionize, EventSelect, Attribute, SubqueryFilter, Sample) and 6 physical variants (those 5 + MergeSources for entity-aligned JOINs). The `__source_table_id` discriminator column and table-id map are plumbed through MergeSources.

**Tech Stack:** Rust, bqlite-planner crate, no new external dependencies.

---

## Checkpoint 1: LogicalPlan + PhysicalPlan Variants

### Files
- Modify: `crates/bqlite-planner/src/logical.rs` — add 5 LogicalPlan variants, EventSelectKind enum, constructors, match arm updates
- Modify: `crates/bqlite-planner/src/physical.rs` — add 6 PhysicalPlan variants, 6 descriptor structs, lower_physical arms
- Modify: `crates/bqlite-planner/src/lib.rs` — re-export new public types

### Logical Plan Variants (from logical-plan-nodes.md §5.2)

```rust
Sessionize {
    gap: i64,
    end_events: Vec<String>,
    forwarded_columns: Vec<ColumnId>,
    fused_downstream: Option<FusedDownstream>,
    input: Box<LogicalPlan>,
    output_schema: OperatorSchema,
}

EventSelect {
    kind: EventSelectKind,
    event_types: Vec<String>,
    predicate: Option<TypedExpr>,
    lookback: Option<i64>,
    forwarded_columns: Vec<ColumnId>,
    fused_downstream: Option<FusedDownstream>,
    input: Box<LogicalPlan>,
    output_schema: OperatorSchema,
}

Attribute {
    conversion_events: Vec<String>,
    touchpoint_events: Vec<String>,
    window: i64,
    touchpoint_key: TypedExpr,
    forwarded_conversion_columns: Vec<ColumnId>,
    fused_downstream: Option<FusedDownstream>,
    input: Box<LogicalPlan>,
    output_schema: OperatorSchema,
}

SubqueryFilter {
    columns: Vec<TypedExpr>,
    subquery: Box<LogicalPlan>,
    input: Box<LogicalPlan>,
    output_schema: OperatorSchema,
}

Sample {
    fraction: f64,
    seed: Option<i64>,
    input: Box<LogicalPlan>,
    output_schema: OperatorSchema,
}
```

### Physical Plan Descriptors

SessionizePhysical, EventSelectPhysical, AttributePhysical, SubqueryFilterPhysical, SamplePhysical per respective design docs; MergeSourcesPhysical per cohorts-aliases-joins.md §3.7–3.8 with `table_id_map: Vec<String>`.

### Match Arms to Update
- `LogicalPlan::output_schema()` — add 5 arms
- `LogicalPlan::extend_scan_reader_backward/forward` — pass through stateful nodes (Sessionize, EventSelect, Attribute recurse into input; SubqueryFilter, Sample recurse into input)
- `PhysicalPlan::output_schema()` — add 6 arms
- `lower_physical()` — add 5 arms (MergeSources is physical-only, no logical counterpart)

## Checkpoint 2: EXPLAIN Rendering

### Files
- Modify: `crates/bqlite-planner/src/explain.rs` — add ExplainNode variants + build/format arms

### ExplainNode Variants
Sessionize, EventSelect, Attribute, SubqueryFilter, Sample, MergeSources — each with human-readable summary fields mirroring the Wave 3 pattern.
