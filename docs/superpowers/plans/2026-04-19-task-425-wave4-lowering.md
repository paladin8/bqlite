# TASK-425: Wave 4 AST→Logical + Logical→Physical Lowering

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` or `superpowers:executing-plans` to implement this plan checkpoint-by-checkpoint. Each checkpoint must pass `scripts/local-ci.sh`, get a code-review subagent approval, and be fast-forward-merged to `main` before the next checkpoint begins.

**Goal:** Implement AST→logical lowering for the Wave 4 pipeline stages (SESSIONIZE / FIRST/LAST/NTH / SAMPLE / ATTRIBUTE), the cohort-subquery expression forms (`IN QUERY (...)`, `IN alias`), top-level alias definitions, and entity-aligned source JOINs. The six physical descriptors and logical→physical `lower_physical` arms already exist (TASK-424); TASK-425 fills in the lowering paths that currently return "not yet supported" errors plus the source-resolution and alias-binding passes.

**Architecture.** The existing `fold_stage` function in `crates/bqlite-planner/src/logical.rs` is the single extension point — each new stage gets a `lower_<stage>` helper that produces the corresponding `LogicalPlan` variant, with scan-range extension applied through `extend_scan_reader_{backward,forward}`. Alias binding is a *new* pre-fold pass on `Pipeline` that threads a resolved-alias table into lowering. Joined-source support grows `lower_query_pipeline` into a source-resolution step that chooses between a single `LogicalPlan::Scan` (current behavior) and a new joined-scan shape that later maps to `MergeSourcesPhysical`.

**Tech Stack:** Rust 2021, existing `bqlite-planner` crate. No new external dependencies.

**Out of scope (other tasks):**
- `RETENTION` desugaring: TASK-426.
- `DELETE` logical/planner: TASK-453.
- Actual operator execution (SessionizeOperator, AttributeOperator, etc.): TASK-428/429/430/431/437.
- Bind step materialization: TASK-438.

---

## Checkpoint map

| CP | Scope | Files | Merge size | Depends on |
|----|-------|-------|------------|------------|
| **CP1** | SESSIONIZE + SAMPLE lowering | `logical.rs` | ~400 LOC | nothing new |
| **CP2** | EventSelect + Attribute lowering, scan-range extension (single-scan path), `LogicalPlan::Attribute.conversion_range` capture at lowering time + threading to `AttributePhysical` | `logical.rs`, `physical.rs` | ~650 LOC | CP1 |
| **CP3** | `IN QUERY (...)` inline subquery → `LogicalPlan::SubqueryFilter`; multi-column tuple cohort keys | `logical.rs`, `expr.rs` | ~500 LOC | CP2 |
| **CP4** | Top-level `Statement::DefineAlias` + alias resolution table + `IN alias` + cycle detection + alias deduplication; `lower_statements(Vec<Statement>)` entrypoint + engine caller update | `logical.rs`, `expr.rs`, `bqlite-engine/src/query.rs` | ~600 LOC | CP3 |
| **CP5a** | Entity-aligned source `JOIN` logical path: `lower_query_pipeline` accepts joins, validates entity-key compatibility, builds combined schema with `__source_table_id` and qualified-reference side-table; scan-range extension stays uniform (single `reader_{backward,forward}_ns` field applies to all joined tables) | `logical.rs`, `expr.rs` | ~500 LOC | CP4 |
| **CP5b** | `LogicalPlan::Scan { joined_tables: non-empty }` → `PhysicalPlan::MergeSources` in `lower_physical`; table-qualified `Expr::Qualified { table, column }` / `QualifiedWildcard(table)` resolution in `TypedExpr::from_ast`; bare unqualified `Expr::Column` inside joined context → `Plan` error | `physical.rs`, `expr.rs` | ~400 LOC | CP5a |

Checkpoints 1–4 are largely additive (new functions). CP5 touches the `Scan` arm of `lower_physical` (dropping the `debug_assert!(joined_tables.is_empty())`), so it is intentionally last to minimize conflict risk.

**Every checkpoint must:**
1. Pass `scripts/local-ci.sh` (fmt, clippy, build, full test suite, dep-direction).
2. Be reviewed by a subagent (code-reviewer) with no blocking findings.
3. Be reconciled with the owning design doc and reflect any doc drift in the same commit.
4. Be fast-forward-merged to `main` before the next CP starts.

---

## Global conventions

**Validation ordering.** Within each lowering helper, perform all structural checks (non-empty event-type lists, lookup of event-type names, unknown column errors) *before* extending the scan-range window. An early return must not leave `reader_*_ns` mutated.

**Error surface.** All lowering errors use `BqliteError::Plan(...)` (string detail) to match existing `fold_stage` conventions. No new error variants — the design docs' `TypeError::X` names are the conceptual variant; in this codebase they are spelled `BqliteError::Plan` with the rule name in the string.

**Testing style.** Every logical-lowering helper gets unit tests under `mod tests { ... }` at the bottom of `logical.rs`, using the existing `InMemoryCatalog` helper. Every arm of the `lower_physical` change gets a matching test in `physical.rs` (look at the existing Wave 3 tests for the pattern). No property tests are required for pure lowering — the input surface is structurally constrained by the parser.

**Scan-range extension.** `LogicalPlan::extend_scan_reader_forward` / `_backward` already exist and walk Filter/Project/Limit wrappers to reach the `Scan`. When a joined-scan shape lands in CP5, these helpers must learn to fan out across joined tables too (see CP5 detail).

**Design doc reconciliation.** Each CP has an entry in the "Doc reconciliation" section below that names the doc file and the specific assertion to re-read after the code lands.

---

## CP1 — SESSIONIZE + SAMPLE lowering

### Scope
Implement `lower_sessionize(...)` and `lower_sample(...)`, called from `fold_stage`. Remove the "not yet supported" arms for `PipelineStage::Sessionize(_)` and `PipelineStage::Sample(_)`. Produce `LogicalPlan::Sessionize { .. }` and `LogicalPlan::Sample { .. }` respectively.

### Files

- Modify: `crates/bqlite-planner/src/logical.rs`
  - Add: `fn lower_sessionize(args: bqlite_ast::Sessionize, acc, source_table) -> Result<LogicalPlan>`
  - Add: `fn lower_sample(args: bqlite_ast::Sample, acc) -> Result<LogicalPlan>`
  - Add: constructor helpers `LogicalPlan::sessionize(..)` and `LogicalPlan::sample(..)` (mirroring existing `LogicalPlan::filter`/`LogicalPlan::project` shape — build output schema, return `Ok(LogicalPlan::Sessionize { .. })`).
  - Modify: `fold_stage` — replace the `PipelineStage::Sessionize(args) => Err(...)` and `PipelineStage::Sample(args) => Err(...)` fall-throughs with `lower_sessionize(args, acc, source_table)` and `lower_sample(args, acc)` calls.
- Add tests in `mod tests` for each lowering helper.

### SESSIONIZE rules (from `docs/design/operators/sessionize.md` §4–§6)

1. **`gap` must be `> 0`.** `gap <= 0` → `BqliteError::Plan("SESSIONIZE: gap must be positive — got <N>ns")`.
2. **`end_events` list validation.** For each `EventRef`, its `.event.text` is the event-type name. Duplicate names within the list → `Plan` error (`"SESSIONIZE: duplicate end-event type `<name>`"`). Order is preserved; no catalog cross-check is required (event types are open-string values).
3. **Input must expose the event-type column** (the `TableSchema.event_type_column().name`). If the input schema has dropped it (not possible through any current Wave 4 stage, but guard anyway), return `Plan("SESSIONIZE requires the input to expose event type column `<name>`")`.
4. **Output schema.** Input columns in order, followed by `session_id: Int NOT NULL` and `session_duration: Int NOT NULL` (per §6.1). Reuse `ColumnDef { name, bql_type: BqlType::Int, nullable: false, default_value: None }`.
5. **`forwarded_columns`** is empty at construction (demand analysis populates it later, per TASK-427 wiring).
6. **`fused_downstream`** is always `None` in v1 (Wave 5 populates it).

### SAMPLE rules (from `docs/design/operators/event-select-sample.md` §15–§17)

1. **`fraction` must be finite and in `[0.0, 1.0]`.** Outside → `Plan("SAMPLE: fraction must be in [0.0, 1.0] — got <f>")`. NaN → same error text; infinity → same.
2. **`seed`** carries through as-is. `None` means "engine picks a DB-UUID-derived seed". No type-coercion needed.
3. **Output schema** is identical to the input schema (SAMPLE never reshapes).

### Pseudo-code

```rust
fn lower_sessionize(
    args: bqlite_ast::Sessionize,
    acc: LogicalPlan,
    source_table: &TableSchema,
) -> Result<LogicalPlan> {
    if args.gap <= 0 {
        return Err(BqliteError::Plan(format!(
            "SESSIONIZE: gap must be positive — got {}ns", args.gap
        )));
    }
    let input_schema = acc.output_schema();
    let event_type_col = &source_table.event_type_column().name;
    if input_schema.column(event_type_col).is_none() {
        return Err(BqliteError::Plan(format!(
            "SESSIONIZE requires the input to expose event type column `{event_type_col}`"
        )));
    }
    let end_events: Vec<String> = match args.end {
        None => Vec::new(),
        Some(refs) => {
            let mut seen = HashSet::new();
            let mut out = Vec::with_capacity(refs.len());
            for r in refs {
                let t = r.event.text;
                if !seen.insert(t.clone()) {
                    return Err(BqliteError::Plan(format!(
                        "SESSIONIZE: duplicate end-event type `{t}`"
                    )));
                }
                out.push(t);
            }
            out
        }
    };
    let mut cols = input_schema.columns().to_vec();
    cols.push(ColumnDef::required("session_id", BqlType::Int));
    cols.push(ColumnDef::required("session_duration", BqlType::Int));
    let output_schema = OperatorSchema::new(cols)?;
    Ok(LogicalPlan::Sessionize {
        gap: args.gap,
        end_events,
        forwarded_columns: Vec::new(),
        fused_downstream: None,
        input: Box::new(acc),
        output_schema,
    })
}

fn lower_sample(args: bqlite_ast::Sample, acc: LogicalPlan) -> Result<LogicalPlan> {
    if !args.fraction.is_finite() || !(0.0..=1.0).contains(&args.fraction) {
        return Err(BqliteError::Plan(format!(
            "SAMPLE: fraction must be in [0.0, 1.0] — got {}", args.fraction
        )));
    }
    let output_schema = acc.output_schema().clone();
    Ok(LogicalPlan::Sample {
        fraction: args.fraction,
        seed: args.seed,
        input: Box::new(acc),
        output_schema,
    })
}
```

### Tests (CP1)

- `sessionize_lowers_with_default_end_events` — no `end:` clause → empty `end_events`, schema includes `session_id` and `session_duration`.
- `sessionize_with_end_events_keeps_order` — `end: (logout, tab_close)` preserves order.
- `sessionize_duplicate_end_event_rejected` — `end: (logout, logout)` → `Plan` error naming "duplicate".
- `sessionize_zero_gap_rejected` — `gap: 0ns` → `Plan` error.
- `sessionize_negative_gap_rejected` — `gap: -1` (construct manually) → `Plan` error.
- `sample_fraction_out_of_range_rejected` — `fraction: 1.5` → `Plan` error; `-0.1` → `Plan` error.
- `sample_nan_fraction_rejected`.
- `sample_keeps_input_schema` — output schema identity-compared via `OperatorSchema::columns`.
- `sample_without_seed_stores_none`.
- `sample_with_seed_preserves_seed`.
- `sessionize_then_sample_composes` — fold two stages, assert nested structure.

### Validation

- `cargo test -p bqlite-planner sessionize` and `sample` pass.
- `scripts/local-ci.sh` clean.
- Code-review subagent approves.
- Doc reconciliation: re-read `docs/design/operators/sessionize.md` §4 + `event-select-sample.md` §15–§17 — match exactly.

### Commit

`TASK-425: SESSIONIZE + SAMPLE AST→logical lowering`

---

## CP2 — EventSelect + Attribute lowering + scan-range extension

### Scope
Implement `lower_event_select` and `lower_attribute`. Extend the scan backward by `lookback` for EventSelect (FIRST/NTH only — LAST was parser-rejected already) and by `window` for Attribute. Capture the pristine query time range at logical-lowering time into a new `LogicalPlan::Attribute.conversion_range: Option<(i64, i64)>` field, then copy it straight into `AttributePhysical.conversion_range` during `lower_physical`. (Per plan review B2: capturing at logical-lowering time is strictly simpler than walking the logical tree during physical lowering — `lower_attribute` has direct access to the primary Scan via `&acc`, and a trivial field copy downstream avoids a variant-list maintenance burden.)

### Files

- Modify: `crates/bqlite-planner/src/logical.rs`
  - Add field: `LogicalPlan::Attribute` gains a `conversion_range: Option<(i64, i64)>` field. This carries the resolved-at-logical-time query range `(start_ns, end_ns)`. `None` when the source has no time range (unbounded scan).
    - **Resolution note:** `Attribute` is evaluated relative to `LAST <d>` / `BETWEEN <a> AND <b>` scan time. `LAST` requires `now_ns`; since logical lowering does not carry a clock, only `BETWEEN` can be resolved at logical time. For `LAST`, we store the **unresolved** AST time range indirectly: set `conversion_range = None` at logical-lowering time when the scan's `time_range` is `Some(TimeRange::Last(_))`, and have `lower_physical` (which does carry `now_ns`) resolve it there via a small helper. This is a tiny, localized walk — two lines — and avoids any new clock-source plumbing into logical lowering. `BETWEEN` results are captured directly at logical time.
    - The prior design ("walker through 12 variants") is rejected per plan review B2.
  - Add: `lower_event_select(args, acc, registry, source_table) -> Result<LogicalPlan>`
  - Add: `lower_attribute(args, acc, registry, source_table) -> Result<LogicalPlan>` — captures `conversion_range` by inspecting `acc`'s primary Scan *before* extending it via `extend_scan_reader_backward`. Uses a small `fn attribute_conversion_range(acc: &LogicalPlan) -> Option<(i64, i64)>` helper that matches only `LogicalPlan::Scan { time_range: Some(TimeRange::Between { start, end }), .. }` (the `LAST` case falls through to `None` at logical time and is resolved later).
  - Add: helper that converts `bqlite_ast::EventSelectKind` → `crate::logical::EventSelectKind` (Nth narrowing u64→u32 is already done in the AST per Explore report; re-validate `n >= 1` here to be defensive).
- Modify: `crates/bqlite-planner/src/physical.rs`
  - `LogicalPlan::Attribute` arm of `lower_physical`: if `conversion_range` is already `Some`, copy it straight into `AttributePhysical.conversion_range`. If `None`, call a new `resolve_last_range_from_scan(&input, now_ns) -> Option<(i64, i64)>` that shallow-walks `input` (Filter/Project/Limit/Sample/SubqueryFilter/Sessionize/EventSelect/Sort/Distinct/Aggregate/SequenceMatch/Attribute/Scan) looking for a Scan with `TimeRange::Last(_)`, and resolves it via `resolve_ast_time_range`. The walker returns `None` if no `LAST` scan is found (i.e., unbounded scan), which is correct.
- Modify: `fold_stage` — replace the `EventSelect(args) => Err(...)` and `Attribute(args) => Err(...)` arms with calls to the new helpers.

### EventSelect rules (from `docs/design/operators/event-select-sample.md` §4–§11)

1. **Non-empty `event_types`** (parser already guarantees `≥ 1`; guard as `debug_assert!`).
2. **No duplicate event types** (parser guarantees; same pattern).
3. **`predicate`** is type-checked against the input schema via `TypedExpr::from_ast`. Result type must be `BqlType::Bool`. Reuse existing `LogicalPlan::filter`-style check.
4. **`lookback`**: only permissible for FIRST/NTH (parser already rejects on LAST). If somehow present on LAST → `Plan` error (defensive).
5. **`Nth(n)` must have `n >= 1`** (parser enforces; defensive check).
6. **Scan-range extension for lookback.** FIRST/NTH with `Some(lookback_ns)` must call `acc.extend_scan_reader_backward(lookback_ns)` *after* all validation succeeds. (Rationale: if validation fails after extending, the caller's `acc` state is mutated.)
7. **Output schema** equals the input schema restricted to non-system columns of the source table + entity_id? **Per design doc §5.5: "omitted entities" (entities with no qualifying event) produce no output rows**, but the schema is the input schema verbatim (one row per surviving entity). Therefore `output_schema = input.output_schema().clone()`.
8. **`forwarded_columns`** starts empty — populated by demand analysis.

### Attribute rules (from `docs/design/operators/attribute.md` §4–§12)

1. **Non-empty `conversion_events` and `touchpoint_events`** (parser guarantees).
2. **`window > 0`** → else `Plan("ATTRIBUTE: window must be positive — got <N>ns")`.
3. **`touchpoint_key`** is type-checked against the input schema via `TypedExpr::from_ast`. Its `result_type` must be `BqlType::String` → else `Plan("ATTRIBUTE: touchpoint_key must evaluate to String, got <type>")`. Nullability is preserved in the output (the key column is nullable).
4. **Output schema (§4.1):**
   - `entity_id: <entity_key_type> NOT NULL` (use the source table's entity_key column type).
   - `conversion_ts: Timestamp NOT NULL`.
   - Forwarded conversion columns (empty at construction — demand analysis adds them later).
   - `touchpoint_ts: Timestamp NULL`.
   - `touchpoint_key: String NULL`.
5. **Scan-range extension for window.** Call `acc.extend_scan_reader_backward(window_ns)` after validation.
6. **`forwarded_conversion_columns`** starts empty.
7. **`fused_downstream` = None**.

### `AttributePhysical.conversion_range` threading

Captured at logical-lowering time (for `BETWEEN` ranges) or resolved at physical-lowering time (for `LAST` ranges, which need `now_ns`). The logical `Attribute` node carries `conversion_range: Option<(i64, i64)>`. In `lower_physical`, the arm uses:

```rust
let final_conversion_range = conversion_range
    .or_else(|| resolve_last_range_from_scan(&input, now_ns));
```

where `resolve_last_range_from_scan` walks the input tree looking for a `Scan` with `TimeRange::Last(ns)` and resolves via `resolve_ast_time_range(Some(tr), now_ns)`.

### Tests (CP2)

- `event_select_first_lowers_with_lookback_extends_scan` — construct a pipeline `purchases LAST 1d | FIRST(purchase, lookback: 2h)` and assert the resulting `Scan.reader_backward_ns == 2h_ns`.
- `event_select_last_without_lookback` — `LAST(logout)` → `lookback: None`, scan unchanged.
- `event_select_nth_n_zero_rejected` — construct `Nth(0)` manually → `Plan` error.
- `event_select_predicate_type_mismatch` — `FIRST(purchase WHERE amount)` (amount is Float, not Bool) → `Plan` error.
- `event_select_output_schema_equals_input`.
- `attribute_lowers_with_window_extends_scan` — `ATTRIBUTE ... window: 7d` → scan extended 7d backward.
- `attribute_touchpoint_key_non_string_rejected` — `touchpoint_key: amount` (Float) → `Plan` error.
- `attribute_non_positive_window_rejected`.
- `attribute_output_schema_shape` — assert exact column layout (entity_id, conversion_ts, touchpoint_ts, touchpoint_key).
- `attribute_conversion_range_threaded_through_physical` — build a logical plan with a scan time range, lower to physical, assert `AttributePhysical.conversion_range` equals the pristine resolved range (before reader extension).

### Validation

- `cargo test -p bqlite-planner` passes.
- `scripts/local-ci.sh` clean.
- Code-review subagent approves.
- Doc reconciliation: re-read `event-select-sample.md` §4–§11 and `attribute.md` §4–§12.

### Commit

`TASK-425: EventSelect + Attribute lowering with scan-range extension`

---

## CP3 — IN QUERY (inline subquery) → SubqueryFilter

### Scope
Lift the "IN QUERY is deferred" error in `expr.rs`. Add a compile-time signal: `TypedExpr::from_ast` currently only returns expression trees, but a subquery cannot be collapsed into `TypedExpr` — the cohort needs its own logical subtree. Solution: pass a mutable "subquery accumulator" through the WHERE-stage lowering path only, then wrap the filter in `LogicalPlan::SubqueryFilter`.

### Files

- Modify: `crates/bqlite-planner/src/logical.rs`
  - Add: `fn lower_where(predicate, acc, registry, catalog, source_table) -> Result<LogicalPlan>` — a wrapper that replaces the direct `TypedExpr::from_ast + LogicalPlan::filter` call in `fold_stage`. Extracts top-level conjuncts that are `Expr::In { rhs: InRhs::Query(_), .. }` and builds one `LogicalPlan::SubqueryFilter` per matched conjunct, composing around the accumulated plan. Remaining conjuncts fold into a regular `LogicalPlan::Filter`.
- Modify: `crates/bqlite-planner/src/expr.rs`
  - Allow multi-column LHS: the `if lhs.len() != 1` rejection moves into the caller when the RHS is a `List`. For `InRhs::Query`/`InRhs::Alias` paths we route entirely through `lower_where` so the expression compiler never sees them.
  - Keep `InRhs::Query(_) => Err(...)` and `InRhs::Alias(_) => Err(...)` *inside* `TypedExpr::from_ast` as a guard — they should never arrive here because the caller routes them to `lower_where`. Update error text to reflect: "IN subquery/alias must appear as a top-level WHERE conjunct; nested use is not supported" (matches cohorts-aliases-joins.md restriction that cohorts filter entities).

### Algorithm for `lower_where`

1. Parse the predicate into top-level conjuncts. `Expr::And(vec)` → flatten (recursively for nested ANDs). Any other shape is a single conjunct.
2. For each conjunct:
    - If it is `Expr::In { lhs, rhs: InRhs::Query(subq), negated }`: build a `LogicalPlan::SubqueryFilter`:
      - Lower `*subq` via `lower_query_pipeline(subq, catalog)`.
      - Validate arity: `subq.output_schema().columns()` (non-system) must have length equal to `lhs.len()`.
      - Validate per-column type compatibility (positional).
      - Type-check each `lhs[i]` expression against the *outer* `acc.output_schema()` via `TypedExpr::from_ast`.
      - If `negated`, reject in v1 — `Plan("NOT IN (subquery) is not yet supported")`.
      - Wrap `acc = LogicalPlan::SubqueryFilter { columns: typed_lhs, subquery: Box::new(subq_plan), input: Box::new(acc), output_schema: acc.output_schema().clone() }`.
    - If it is `Expr::In { rhs: InRhs::Alias(_), .. }`: defer to CP4.
    - Otherwise: accumulate into `residual_conjuncts`.
3. If `residual_conjuncts` is non-empty, re-combine via `Expr::And(...)` (or the sole element if `len() == 1`) and wrap `acc` in `LogicalPlan::Filter { predicate: TypedExpr::from_ast(combined, acc.output_schema(), registry)?, ... }`.
4. Return `acc`.

### Multi-column cohort arity/type rules (cohorts-aliases-joins.md §4.1)

- Arity mismatch: `Plan("IN QUERY arity mismatch: LHS has N columns, subquery produces M")`.
- Positional type incompatibility (no widening/coercion): `Plan("IN QUERY column N type mismatch: LHS <type-a>, subquery <type-b>")`.
- Types follow the BQL equality rules from `type-system.md`: compatible iff the underlying scalar classes match (Int/Int, String/String, Bool/Bool, Timestamp/Timestamp, Float/Float). Reuse `BqlType::PartialEq` (they are equal iff identical).

### Tests (CP3)

- `where_in_query_single_column_lowers_to_subquery_filter` — `WHERE user_id IN QUERY (vip_users | SELECT user_id)`.
- `where_in_query_tuple_lowers_multi_column` — `WHERE (country, device) IN QUERY (promoted | SELECT country, device)`.
- `where_in_query_arity_mismatch_rejected`.
- `where_in_query_type_mismatch_rejected`.
- `where_in_query_combined_with_other_conjuncts` — `WHERE a = 1 AND b IN QUERY (...) AND c > 2` → residual filter `a = 1 AND c > 2`, SubqueryFilter on `b`.
- `where_negated_in_query_rejected`.
- `where_nested_in_query_in_or_rejected` — `WHERE foo OR (x IN QUERY (...))` → error; `Expr::Or` can't host subqueries in v1.

### Validation, commit message as usual.

`TASK-425: IN QUERY (inline subquery) SubqueryFilter lowering`

---

## CP4 — DefineAlias + IN alias + cycle detection

### Scope
Lift the "alias definitions are deferred" error for `Statement::DefineAlias`. Introduce an **alias table** threaded through lowering that maps alias name → lazily-planned `LogicalPlan`. Resolve `InRhs::Alias(name)` through it. Detect cycles via DFS.

### Wire protocol

The parser already returns `Vec<Statement>` from `bqlite_parser::parse` (see `/workspace/crates/bqlite-parser/src/lib.rs:109`). Today, engine callers receive this `Vec` and pick the terminal `Statement::Query` (or iterate DDL statements); alias definitions are currently rejected in `lower_statement`. The fix is to expose a `lower_statements` entrypoint that accepts the full script so alias definitions can be threaded into the terminal's lowering.

**New entrypoint:** `pub fn lower_statements(statements: Vec<Statement>, catalog: &dyn Catalog) -> Result<LogicalPlan>`
  - Split the input into `(alias_defs, terminal)` where `alias_defs` is a `Vec<(Name, Pipeline)>` preserving source order (last-wins on duplicates per cohorts-aliases-joins.md §2.2 — the last definition for a given name wins when resolving) and `terminal` is the final non-`DefineAlias` statement. If more than one non-alias statement appears, or if none does, return a `Plan` error.
  - Build an `AliasTable { definitions: BTreeMap<String, (Pipeline, usize)>, resolved: BTreeMap<String, LogicalPlan>, path: Vec<String>, order: Vec<String> }` where `order[i]` is the source-order name at position `i` (used to enforce "forward references are illegal" — a reference to `name` from alias body at position `j` is valid iff `order.iter().take(j).any(|n| n == name)`).
  - Lower `terminal` via `lower_statement_with_aliases(terminal, catalog, &mut aliases)`.

**Storage form for `resolved`.** Use `BTreeMap<String, LogicalPlan>` with `LogicalPlan: Clone` — per plan review S3, the `Rc` affordance buys nothing (runtime dedup happens in TASK-437/438; logical-plan cloning is cheap and `SubqueryFilter.subquery` already owns a `Box<LogicalPlan>` child). Cache semantics: once a name is resolved, subsequent `IN alias name` references clone the resolved plan.

**Backward compat:** `lower_statement(statement, catalog)` becomes `lower_statements(vec![statement], catalog)` — existing callers keep working.

### Engine caller update

- Modify: `crates/bqlite-engine/src/query.rs` — wherever `lower_statement` is called today in a loop over a parsed `Vec<Statement>`, replace the loop-of-`lower_statement` with a single `lower_statements(parsed, catalog)` call (for the terminal query path) and keep per-statement dispatch for DDL sequences. Add a test in `bqlite-engine` that a BQL script of form `vip = events | WHERE ...; events | WHERE user_id IN alias vip` lowers and executes end-to-end in test harness mode (no actual query execution needed — just planning, since operator execution is TASK-437/438 scope).
- Add: `lower_statements` backward-compat regression test that `lower_statements(vec![Statement::Query(...)])` produces an identical `LogicalPlan` to `lower_statement(Statement::Query(...))`.

### Alias resolution algorithm

`fn resolve_alias(name: &str, caller_order_index: usize, aliases: &mut AliasTable, catalog: &dyn Catalog) -> Result<LogicalPlan>`
1. **Forward-reference check:** if `name` is not defined at any position ≤ `caller_order_index` in `aliases.order` → `Plan("alias `<name>` is undefined or defined after this reference — alias references must resolve in source order")`.
2. **Cycle check:** if `aliases.path.contains(name)` → `Plan("alias cycle detected: <joined path>")` with the path joined by ` -> `, followed by `-> <name>` to close the cycle visually.
3. If `aliases.resolved.get(name).is_some()` → return `aliases.resolved[name].clone()`.
4. Else: look up the most recent definition at position ≤ `caller_order_index` via `aliases.definitions[name]` (last-wins semantics are baked into the BTreeMap by virtue of the loader overwriting on duplicate names — OK because forward-reference check enforces "defined earlier"). Push `name` onto `aliases.path`, call `lower_query_pipeline_with_aliases(pipeline.clone(), catalog, aliases)?`, pop.
5. Insert into `resolved`. Return `.clone()`.

### Changes to `lower_where`

In CP3 we rejected `InRhs::Alias(_)`. Now:
- Resolve via `resolve_alias(name, ...)` → `LogicalPlan`.
- Use it as the `SubqueryFilter.subquery` child.
- Same arity/type checks as IN QUERY (single-column and tuple).

### Tests (CP4)

- `alias_def_and_reference` — `vip = purchases | WHERE amount > 100 | SELECT user_id`, then `events | WHERE user_id IN alias vip` → lowers to `SubqueryFilter` wrapping the alias plan.
- `alias_forward_reference_rejected`.
- `alias_cycle_detected` — `a = events | WHERE user_id IN alias b`, `b = events | WHERE user_id IN alias a` → `Plan` error naming the cycle path.
- `alias_last_wins_on_duplicate_name` — two definitions of `vip`, later one wins (per cohorts-aliases-joins.md §2.2).
- `alias_referenced_twice_is_deduped` — single resolution call produces a cached plan; assert `aliases.resolved` contains one entry after lowering.
- `alias_tuple_arity_mismatch_rejected`.

### Commit

`TASK-425: DefineAlias + IN alias resolution with cycle detection`

---

## CP5a — Entity-aligned source JOIN: logical path

### Scope
Lift the `JOIN clauses are deferred to Wave 4` rejection in `lower_query_pipeline`. Resolve the primary + joined tables, validate entity-key type compatibility, build the combined output schema with `__source_table_id: Int NOT NULL` and **all user columns qualified** (via a side-table, **not** by name-munging the column list). Per plan review B3 and per `cohorts-aliases-joins.md` §3.11 and §5 error table: table-qualified references are **mandatory** inside a joined pipeline; bare `Expr::Column` is a bind-time `UnqualifiedReferenceInJoin` error.

### Files

- Modify: `crates/bqlite-planner/src/logical.rs`
  - Drop the `joins is non-empty → Err` block at the top of `lower_query_pipeline`.
  - Resolve primary + each joined table name against the catalog. Reject self-joins at the logical layer too (parser already rejects, but guard defensively).
  - Validate **entity-key type compatibility**: every joined table's `entity_key_column().bql_type` must equal the primary's. Mismatch → `Plan("JOIN entity-key type mismatch: primary `<t1>` has <T1>, joined `<t2>` has <T2>")`.
  - Build combined output schema. **Schema shape (joined case):**
    - Non-system columns from the primary, each retaining its bare name (`"amount"`, etc.). This preserves `output_schema().column("amount")` behavior for schemas that don't collide.
    - Inject `__source_table_id: Int NOT NULL`.
    - Implicit qualified aliases are carried in a **side-table** (see below); *do not* duplicate columns in the column list under dotted names.
  - Add a new field to `LogicalPlan::Scan`: `qualified_lookup: Option<Arc<QualifiedLookup>>` where `QualifiedLookup` is a plain data struct mapping `(table_name, column_name)` → `(column_index_in_output_schema, ColumnDef)`. `None` when the scan is single-table. Populated when joined. `Arc` so `Scan` remains `Clone` cheaply.
    - Entries include: every non-system column of every joined table (primary + joined), keyed by `(table_name, column_name)`. Primary columns are also reachable through `qualified_lookup[("primary_table_name", column_name)]` — same underlying column position.
    - Collision handling: two joined tables exposing the same bare name (e.g., both tables have a `country` column) is **not** an error at schema-build time — because qualified refs are mandatory inside a joined pipeline, the bare name is never looked up via `schema.column(name)`. The side-table makes both accessible via `(t1, country)` and `(t2, country)`. (This diverges from the earlier draft's "collision is an error" rule — the mandatory-qualification rule subsumes it.)
- Modify: `crates/bqlite-planner/src/expr.rs`
  - In `TypedExpr::from_ast`, when resolving an `Expr::Column(name)` reference, the caller passes both `schema: &OperatorSchema` and an optional `qualified_lookup: Option<&QualifiedLookup>`. If `qualified_lookup.is_some()`: a bare `Expr::Column` is an error `Plan("unqualified column reference `<name>` inside joined pipeline — qualify with `<table>.<column>`")`. This matches `TypeError::UnqualifiedReferenceInJoin` per cohorts-aliases-joins.md §5.
  - `Expr::Qualified { table, column }`: if `qualified_lookup.is_some()`, look up `(table.text, column.text)` in the map. Miss → `Plan("unknown column reference `<t>.<c>` — known tables are <list>")`. Hit → produce a `TypedExpr` whose `column_index` points at the combined schema.
  - If `qualified_lookup.is_none()` (single-table pipeline), `Expr::Qualified` continues to be rejected as today (or resolved against the single table name — keep current behavior, which is rejection per the `operator.rs` survey).
  - `QualifiedWildcard(table)`: with `qualified_lookup.is_some()`, expand to one ProjectItem per column registered under that table. Without, reject as today.
  - Thread the optional `qualified_lookup` through every `from_ast` recursive call.

### Tests (CP5a)

- `bare_pipeline_without_joins_still_lowers_to_plain_scan` — regression: single-table path unchanged, `qualified_lookup` is `None`.
- `joined_pipeline_lowers_to_scan_with_joined_tables` — `purchases JOIN logins` → single logical Scan with `joined_tables.len() == 1`, `qualified_lookup.is_some()`, and `__source_table_id` injected as the final (or near-final) column.
- `joined_pipeline_entity_key_mismatch_rejected`.
- `joined_pipeline_self_join_rejected_at_logical`.
- `joined_pipeline_unqualified_column_rejected` — `WHERE amount > 100` (bare) in a joined pipeline → `Plan` error naming "unqualified".
- `joined_pipeline_qualified_column_resolves` — `WHERE purchases.amount > 100`.
- `joined_pipeline_qualified_wildcard_expands`.
- `joined_pipeline_column_collision_disambiguated_by_qualification` — both tables have a `country` column; `SELECT purchases.country, logins.country` lowers cleanly.
- `joined_pipeline_attribute_uniform_scan_extension` — `purchases JOIN logins LAST 1d | ATTRIBUTE ... window: 7d` → a single logical Scan whose `reader_backward_ns == 7d`. The physical lowering in CP5b fans this out across both sub-scans; at the logical layer the single shared field is sufficient.

### Commit

`TASK-425: Entity-aligned source JOIN logical path + qualified-reference validation`

---

## CP5b — MergeSources physical lowering + table-qualified resolution wired end-to-end

### Scope
Produce `PhysicalPlan::MergeSources(MergeSourcesPhysical { ... })` when `LogicalPlan::Scan.joined_tables` is non-empty. Replicate `reader_range` across all sub-scans. Populate `table_id_map`. Carry the canonical merge `order`. Drop the `debug_assert!` in `lower_physical`. End-to-end integration test that a joined pipeline lowers through all three stages (AST → logical → physical) cleanly.

### Files

- Modify: `crates/bqlite-planner/src/physical.rs`
  - Drop `debug_assert!(joined_tables.is_empty(), ...)` in the `LogicalPlan::Scan` arm.
  - If `joined_tables.is_empty()`: preserve today's single-`ScanPhysical` output.
  - Else: build one `ScanPhysical` per table (primary + each joined), each sharing the same `query_range`/`reader_range` (computed once from the logical Scan's `time_range + reader_backward_ns + reader_forward_ns`), each carrying its **own** `output_schema` (that table's declared + system columns). Wrap the vector in `PhysicalPlan::MergeSources(MergeSourcesPhysical { tables, order, table_id_map, output_schema })` where `output_schema` is the combined schema from the logical Scan.
  - `order`: the canonical `(entity_id ASC, ts ASC, __table_order ASC, __seq_id ASC)` per cohorts-aliases-joins.md §3.2. `__table_order` is a synthetic name the MergeSources operator interprets by table index; the planner stores it explicitly for EXPLAIN rendering.
  - `table_id_map`: `vec![primary.name().to_string()]` followed by each joined table's name in JOIN order.

### Tests (CP5b)

- `joined_pipeline_physical_lowers_to_merge_sources` — assert `PhysicalPlan::MergeSources { tables: 2, table_id_map: ["purchases", "logins"] }`.
- `merge_sources_replicates_reader_range_across_tables` — both `ScanPhysical.reader_range` values equal.
- `merge_sources_order_is_canonical` — exact equality with the canonical `[(entity_id,Asc), (ts,Asc), (__table_order,Asc), (__seq_id,Asc)]`.
- `joined_pipeline_end_to_end` — `parse("purchases JOIN logins LAST 1d | WHERE purchases.amount > 100")` → `lower_statements` → `lower_physical` produces the expected nested physical tree.

### Commit

`TASK-425: MergeSources physical lowering + joined-pipeline end-to-end`

---

## Doc reconciliation map

| CP | Design doc(s) to re-read | Key assertions to verify |
|----|-------------------------|--------------------------|
| CP1 | `operators/sessionize.md` §4–§6; `event-select-sample.md` §15–§17 | gap > 0; end-event duplicate rejection; SAMPLE fraction inclusive range |
| CP2 | `operators/event-select-sample.md` §4–§11; `operators/attribute.md` §4–§12 | lookback FIRST/NTH only; window > 0; touchpoint_key String; scan-range extension rules |
| CP3 | `language/cohorts-aliases-joins.md` §4 | arity/type equivalence; NOT-IN deferral |
| CP4 | `language/cohorts-aliases-joins.md` §2.1–§2.3 | last-wins; source-order forward-ref rejection; cycle error path form |
| CP5a | `language/cohorts-aliases-joins.md` §3, §5, `planner-pipeline.md` §4.4 | mandatory qualified refs in JOINs; `__source_table_id` semantics; uniform extension across joined tables |
| CP5b | `language/cohorts-aliases-joins.md` §3.2, §3.7, §3.8 | MergeSources n-ary; canonical `(entity_id, ts, table_order, __seq_id)` order; `table_id_map` shape |

After each CP lands, if the code implements something the doc doesn't state, edit the doc in the same commit and note it in the commit message.

---

## Completion

After CP5 merges cleanly to `main`:

1. `git mv tasks/active/TASK-425.lock tasks/completed/TASK-425.done`.
2. Edit `.done` file to set `completed_at` (UTC ISO-8601).
3. `git commit -m "TASK-425: completed"` directly on `main`.
4. `git push origin main`.
5. End turn. Do not claim another task.
