# Planner Pipeline Design

**Status**: draft
**Covers**: TASK-006 — the end-to-end compiler pipeline from BQL text to an executable physical plan.

This document is the source of truth for how BQL queries become physical plans. It specifies AST structure, logical plan nodes, the optimizer, demand propagation, fusion, physical planning, and schema validation. Other docs referenced throughout:

- **[query-language.md](query-language.md)** — surface syntax and grammar, referenced by section number.
- **[type-system.md](type-system.md)** — `BqlType`, `TableSchema`, `OperatorSchema`, `TypeError`.
- **[execution-model.md](execution-model.md)** — `PhysicalOperator`, `EntityOperator`, demand propagation, memory budgets.
- **[sequence-matching.md](sequence-matching.md)** — pattern classification, strategy selection, filter pushdown levels.
- **[storage-format.md](storage-format.md)** — zone maps, scan-level predicate evaluation.

---

## 1. Goals and Scope

### 1.1 Goals

1. **Deterministic compilation.** A given BQL query + catalog produces one and only one physical plan. No randomness, no time-dependent rules.
2. **Schema validity by construction.** If a `LogicalPlan` value exists, every expression inside it has passed type checking. There is no separate validation pass.
3. **Clean separation of concerns.** Each stage has one job: the parser knows syntax, the planner knows schemas, the optimizer does structural rewrites, the physical planner binds strategies. None of them overlap.
4. **Fusion as a first-class concern.** The optimizer is designed around enabling stateful operators (MATCH, SESSIONIZE, ATTRIBUTE) to directly feed downstream aggregates without materializing intermediate per-entity rows.
5. **Explainability.** Every optimizer decision is visible through `EXPLAIN` (query-language.md §20.6). A user can inspect what the planner did and why.
6. **Predictable cost.** No cost-based optimization. All transformations are structural and always beneficial. Query compilation is fast and reproducible.

### 1.2 Non-Goals

- **Cost-based optimization.** Rule-based only for v1. Structural rewrites are sufficient for linear BQL pipelines.
- **Multi-query optimization.** Each query is planned independently. Aliases provide manual sharing of intermediate results (query-language.md §18).
- **Plan caching across sessions.** Compilation is cheap relative to execution; caching adds complexity with little benefit.
- **Iterative optimizer passes.** Passes run in a fixed order with no fixpoint iteration.
- **Cross-query dependency tracking.** Session-scoped aliases are resolved lazily at use time, not tracked as a DAG across the session.

### 1.3 Scope Boundaries

The planner stops at producing a physical plan tree. Actual execution — driving iterators, managing memory, handling cancellation — belongs to `bqlite-engine` as specified in execution-model.md.

---

## 2. Pipeline Overview

```
BQL text
  │
  ▼
Parser (bqlite-parser)           ── ast::Statement
  │
  ▼
Planner (bqlite-planner)         ── logical::LogicalPlan
  │   - catalog resolution
  │   - desugaring (FUNNEL, RETENTION, LET, BETWEEN)
  │   - type checking, schema validation
  │   - pipeline-to-tree lowering
  │
  ▼
Optimizer (bqlite-planner)       ── logical::LogicalPlan (rewritten)
  │   - 6 fixed passes
  │
  ▼
Physical Planner (bqlite-planner) ── physical::PhysicalPlan
  │   - strategy selection
  │   - demand propagation
  │
  ▼
Execution (bqlite-engine)
```

| Stage             | Input                      | Output                         | What it knows                            |
| ----------------- | -------------------------- | ------------------------------ | ---------------------------------------- |
| Parser            | BQL text                   | untyped `Statement`            | Syntax only                              |
| Planner           | `Statement`, catalog       | typed `LogicalPlan`            | Schemas, types, desugaring               |
| Optimizer         | `LogicalPlan`              | rewritten `LogicalPlan`        | Structural transformations               |
| Physical Planner  | optimized `LogicalPlan`    | `PhysicalPlan` (plain data)    | Execution strategies, demand             |
| Engine binding    | `PhysicalPlan`             | `Box<dyn PhysicalOperator>`    | Concrete operator instantiation          |
| Execution         | `Box<dyn PhysicalOperator>`| Arrow `RecordBatch` stream     | Memory, I/O, cancellation                |

`PhysicalPlan` is a plain-data tree of structs (e.g., `ScanPhysical`, `SequenceMatchPhysical`). The planner never holds `Box<dyn PhysicalOperator>` — that type lives in a crate above `bqlite-planner` in the dependency order. The engine binds the plain-data tree to concrete operators in a separate step, which is outside the scope of TASK-006. See §15 for crate placement.

Every stage boundary is a value. There are no mutable shared structures threaded through compilation. This makes the pipeline easy to test — each stage can be fed synthetic input and its output diffed.

---

## 3. Parser → AST

### 3.1 Responsibilities

- Parse BQL text into an untyped, unvalidated AST.
- Understand syntax but nothing about schemas, types, or table contents.
- Report parse errors with line/column information and halt on the first error (query-language.md §27).
- Preserve source positions on every node for downstream error reporting.

The parser does not have access to a catalog. It cannot tell whether a table exists, whether a column is the right type, or whether an expression is well-formed beyond surface syntax. All of that is the planner's job.

### 3.2 Statement Types

```rust
pub enum Statement {
    Query(Pipeline),
    Explain(Pipeline),           // EXPLAIN <pipeline> — pipelines only per query-language.md §20.6
    CreateTable(CreateTableStmt),
    AlterTable(AlterTableStmt),  // v1 supports ADD COLUMN only per type-system.md §5.3
    DropTable(DropTableStmt),
    Insert(InsertStmt),
    Delete(DeleteStmt),
    DefineAlias { name: String, body: Pipeline },
}
```

Notes:
- `AlterTable` in v1 is limited to `ADD COLUMN` (type-system.md §5.3 and query-language.md §20.4). There is no column removal, rename, or type change.
- `EXPLAIN` wraps a pipeline only, never a DDL or DML statement.
- `DefineAlias` binds a name to a pipeline in the session scope. Aliases are lazily evaluated at use time (query-language.md §18.1).

### 3.3 Pipeline as Linear Sequence

A `Pipeline` is a **flat list** of operators against a source, not a tree:

```rust
pub struct Pipeline {
    pub source: Source,
    pub operators: Vec<AstOperator>,
    pub span: Span,
}

pub struct Source {
    pub primary: TableRef,
    pub time_range: Option<TimeRange>,
    pub joins: Vec<TableRef>,     // from JOIN <table> (JOIN <table>)* — query-language.md §19
    pub span: Span,
}

pub struct TableRef {
    pub name: Name,               // identifier or backtick-quoted name
    pub span: Span,
}
```

Pipe syntax (`A | B | C`) serializes a composition tree in linear order. The planner converts the list into a tree in §4.2. Making the AST flat keeps the parser simple and makes pipeline rewriting operations (insertion, deletion) trivial.

### 3.4 AST Operator Variants

```rust
pub enum AstOperator {
    Where(Expr),
    Select { distinct: bool, items: Vec<SelectItem> },
    Let { name: String, expr: Expr, span: Span },
    Match(MatchPattern),
    Funnel(FunnelArgs),                 // sugar — desugared in planner
    Retention(RetentionArgs),           // sugar — desugared in planner
    Sessionize(SessionizeParams),
    Stats {
        aggregates: Vec<AggItem>,       // every item carries an explicit output name
        group_by: Vec<GroupItem>,       // every item carries an explicit output name
    },
    OrderBy(Vec<OrderItem>),
    Limit(u64),
    Pivot { column: String, on: String },
    FirstLastNth(EventSelector),
    Sample(SampleParams),
    Attribute(AttributeParams),         // query-language.md §14.3, type-system.md §6.14
}
```

Notes:
- `Funnel` and `Retention` are carried as AST-level sugar. The planner desugars them during logical plan construction because desugaring requires schema access (to fill in step counts and the aggregation list). See §4.3.
- `Let` is sugar for `Project(*, expr AS name)` and is also desugared in the planner.
- `Stats.aggregates` carries explicit output names — query-language.md §7.1 forbids anonymous aggregates. The parser enforces this by rejecting bare `COUNT(*)` without `name =`.
- `Attribute` is specified in query-language.md §14.3 and type-system.md §6.14. It auto-unnests its touchpoints into flat rows with a single `touchpoint_key` column, so there is no separate `Unnest` AST variant — BQL has no general UNNEST operator.

### 3.5 Grammar Enforcement vs. Planner Checks

The parser rejects:
- Unrecognized keywords and structural errors.
- Missing output names in `STATS` / `SELECT` / `LET` / `GROUP BY`.
- Use of `$` followed by anything other than a bare identifier (`$` naming grammar is fixed in query-language.md §4.11).
- `LIMIT` in positions where it is not grammatically allowed (it is not terminal per query-language.md §15).

The parser does **not** reject (these are **planner errors**):
- References to columns that don't exist (the parser has no catalog).
- Type mismatches.
- Self-joins (`events JOIN events`) — rejected by the planner per query-language.md §19.2 because reliably detecting table identity requires catalog resolution.
- Invalid cross-table qualifiers (e.g., `unrelated.col` where `unrelated` is not in the join).
- Variable references whose binding step is never reached.
- Use of `$var` outside of a MATCH expression (query-language.md §4.11).

Everything in the second list becomes a `TypeError` raised by the planner in §4.

### 3.6 Crate Placement

- `bqlite-ast` — `Statement`, `Pipeline`, `AstOperator`, `Expr`, `Name`, `Span`, and all parser output types. No dependencies except `bqlite-core`.
- `bqlite-parser` — hand-written recursive-descent or parser-combinator implementation (`winnow` is a strong candidate) that produces `bqlite-ast` values. Depends only on `bqlite-ast` and `bqlite-core`.

---

## 4. Planner → Logical Plan

### 4.1 Responsibilities

Given a `Statement` and a `Catalog` handle, produce a typed `LogicalPlan`:

1. **Resolve tables and columns** against the catalog.
2. **Resolve aliases** referenced as subqueries or in `IN` clauses.
3. **Desugar** `FUNNEL`, `RETENTION`, `LET`, and `BETWEEN`.
4. **Type check** every expression, propagating types bottom-up and validating operand compatibility.
5. **Validate pipe composition** — each operator's output schema must satisfy the next operator's input requirements.
6. **Extend scan time ranges** for `MATCH` windows and `RETENTION` brackets.
7. **Raise `TypeError`** at the first failure, with source position context.

If planning succeeds, the returned `LogicalPlan` is guaranteed well-typed. There is no separate validation pass. This is the central invariant — see §4.5.

### 4.2 Pipeline-to-Tree Lowering

The linear pipeline `source | op1 | op2 | op3` becomes a tree:

```
op3
└── op2
    └── op1
        └── Scan(source)
```

This is standard relational algebra representation. The pipe symbol is just the reader-friendly serialization of function composition. Each logical node owns its input as `Box<LogicalPlan>`, so the tree is trivially traversable top-down (from output toward scan) or bottom-up (via recursive walks).

Lowering is mechanical: walk `operators` left-to-right, fold each into the accumulated tree. The only complication is desugaring, which can expand one AST operator into multiple logical nodes (see §4.3).

### 4.3 Desugaring

Desugaring happens during lowering, not in the parser, because it requires schema access (for example, to know that `FUNNEL` with three steps needs three `step_reached` comparisons). Canonical desugaring rules live in query-language.md; this table summarizes what the planner applies:

| AST form                                          | Canonical spec         | What the planner produces                                                                 |
| ------------------------------------------------- | ---------------------- | ------------------------------------------------------------------------------------------ |
| `FUNNEL(A THEN B THEN C) WITHIN 7d`               | query-language.md §6.1 | `Scan → SequenceMatch(FIRST, A→B→C, window=7d, emit_all=true) → Aggregate(one `SUM(CAST(step_reached >= N AS INT))` per step, named after the step's event type — e.g. `signup = …`, `add_to_cart = …`, `purchase = …`)` |
| `RETENTION(entry: A, activity: B, brackets: […])` | query-language.md §6.3 | `Scan → SequenceMatch(FIRST, A→B, brackets=[…], emit_all=true) → Aggregate(retention_rate = AVG(CAST(step_reached >= 2 AS INT)) GROUP BY bracket)` |
| `LET x = expr`                                    | query-language.md §11  | `Project(*, expr AS x)`                                                                    |
| `x BETWEEN a AND b`                                | query-language.md §9   | `(x >= a) AND (x <= b)`                                                                    |

The step-named FUNNEL aggregate output follows query-language.md §6.1: each aggregate is named after the corresponding step's event type (or the step name if the step is named). For named steps (`s: signup THEN p: purchase`), the planner uses the step name as the output column name.

**Note on the aggregation idiom.** `COUNT(col)` counts non-null values of `col`. `step_reached` is non-nullable, so `COUNT(step_reached >= N)` would count every row regardless of whether the predicate held. The correct pattern for "count rows where predicate holds" is `SUM(CAST(predicate AS INT))`. This is the idiom used throughout FUNNEL and RETENTION desugaring and is discussed in detail in query-language.md §6.1.

Desugaring must run before type checking the downstream operators, so that the types the downstream operators see match what the physical plan will actually produce.

### 4.4 Scan Time Range Extension

When a pipeline contains a `MATCH` with a `WITHIN` window or `BRACKETS`, the planner extends the scan's upper time bound to allow matches that start near the end of the user's stated range to complete. The user's stated range filters *entry-qualifying events* (the first step of the pattern); the extended range feeds the NFA all events needed to reach the final step.

Rule:

- `events LAST 30d | MATCH ... WITHIN 7d` → scan range becomes `[now-30d, now+7d]` (or symmetric widening if the range is historical).
- `events LAST 90d | MATCH ... BRACKETS [1d, 7d, 14d, 30d]` → scan extends by the maximum bracket (30d).
- Both → extend by `max(window, max_bracket)`.

Entry-step predicates additionally filter entities whose anchor falls outside the user's stated range, so no matches are counted from anchors beyond what the user asked for.

This extension is a planner responsibility, not a scan-layer one. The scan layer's job is to honor the time range it's given; the planner's job is to compute the correct range.

### 4.5 Integrated Schema Validation

Every `LogicalPlan` node exposes an `output_schema()` method that returns an `OperatorSchema` (type-system.md §5.2). When the planner constructs a node, it validates that:

1. The input's output schema contains every column the node references.
2. Every expression in the node is well-typed with respect to that schema.
3. The resulting output schema is itself consistent.

If any check fails, the planner raises a `TypeError` (type-system.md §12) and refuses to construct the node. Because nodes are constructed bottom-up during lowering, the first failure terminates planning at the offending point.

```rust
impl LogicalPlan {
    pub fn output_schema(&self) -> &OperatorSchema {
        // cached per node; computed once at construction time
    }
}
```

The schema is computed once and cached on the node itself, so repeated lookups during optimization are free.

**Invariant**: If you hold a `LogicalPlan` value, it is well-typed. There is no way to construct an ill-typed plan. This eliminates an entire class of bugs — runtime type errors are impossible.

### 4.6 TypedExpr

Expressions in the logical plan are carried as `TypedExpr` rather than the raw `Expr` from the AST:

```rust
pub struct TypedExpr {
    pub expr: Expr,
    pub bql_type: BqlType,
    pub nullable: bool,
    pub span: Span,
}
```

The `Expr` sub-tree is identical to the AST's, but each node has been resolved against a schema. Column references now point to resolved `ColumnId` values (a newtype over `String` — the fully-qualified column name within the current operator's scope, e.g. `events.amount` in a joined query). Function calls have been matched against signatures, and literals have been coerced to their final types.

`TypedExpr` is produced by a single function in the planner (`type_check`) that walks an AST expression tree given a schema and returns either `Ok(TypedExpr)` or a `TypeError`. This is the sole entry point for expression typing — there is no ad-hoc type inference scattered across the planner.

Aggregate expressions use a parallel `TypedAggExpr` type because their input/output shape is different (they reference an aggregation function, a possibly-empty argument expression, and a group-by context).

### 4.7 Validation Sequence

Construction of a logical plan from a pipeline proceeds in this order:

1. **Resolve source.** Look up primary table and every `JOIN` table in the catalog. Reject unknown tables. Produce the initial `Scan` node with the combined schema (for joins, the combined schema tags each column with its source table — see query-language.md §19).
2. **Resolve aliases** referenced in the pipeline body. Planning recurses into the alias body, producing a sub-plan that is inlined when the alias is referenced. Alias outputs are typed at the use site, not at definition (query-language.md §18.1 on lazy evaluation).
3. **Fold operators left to right.** For each `AstOperator`:
   - Retrieve the current accumulated plan's `output_schema()`.
   - Type-check every expression in the operator against that schema.
   - For `MATCH`: resolve variable bindings (first binding site determines type; subsequent uses checked for equality per query-language.md §4.11).
   - Construct the corresponding `LogicalPlan` node wrapping the previous plan.
   - Cache the new node's `output_schema()` for the next iteration.
4. **Report the first error** encountered with the originating source span.

Alias bodies and subqueries inside `IN QUERY (...)` are planned via recursive entry into the same validation sequence.

### 4.8 Alias Resolution

When an alias is used as a subquery (`entity_id IN alias_name`) or inlined into a pipeline, the planner retrieves the alias body and plans it once per use site. There is no planner-level deduplication. Aliases are a readability mechanism, not a caching one.

The planner detects alias cycles (`alias A references alias B references A`) during resolution and raises `TypeError::AliasCycle { path: "A -> B -> A" }` (type-system.md §12). Depth is bounded to prevent pathological nesting.

---

## 5. Logical Plan Node Catalog

This section enumerates every node. Each node's input type, output schema rules, and planner-set fields are documented. See type-system.md §6 for canonical output column names.

### 5.1 Core Relational Nodes

```rust
pub enum LogicalPlan {
    Scan {
        table: TableRef,
        time_range: Option<TimeRange>,
        joined_tables: Vec<TableRef>,       // empty for single-table sources
        // Populated by optimizer Pass 3 and Pass 4:
        scan_predicates: Vec<TypedExpr>,
        projected_columns: Vec<ColumnId>,
    },
    Filter {
        predicate: TypedExpr,
        input: Box<LogicalPlan>,
    },
    Project {
        expressions: Vec<(TypedExpr, String)>,  // each (expr, output_name)
        input: Box<LogicalPlan>,
    },
    Aggregate {
        aggregates: Vec<TypedAggExpr>,
        group_by: Vec<TypedExpr>,
        input: Box<LogicalPlan>,
    },
    Sort {
        keys: Vec<(TypedExpr, SortDir)>,
        input: Box<LogicalPlan>,
    },
    Limit {
        count: u64,
        input: Box<LogicalPlan>,
    },
    Window {
        function: WindowFunction,
        partition_by: Vec<TypedExpr>,
        order_by: Vec<(TypedExpr, SortDir)>,
        input: Box<LogicalPlan>,
    },
    Pivot {
        column: String,
        on: String,
        input: Box<LogicalPlan>,
    },
    SubqueryFilter {
        column: ColumnId,
        subquery: Box<LogicalPlan>,
        input: Box<LogicalPlan>,
    },
}
```

### 5.2 Stateful (Entity-Streaming) Nodes

These operators hold per-entity state during execution and are the primary fusion targets.

```rust
pub enum LogicalPlan {
    // ... core nodes above ...

    SequenceMatch {
        pattern: SequencePattern,
        mode: MatchMode,                    // FIRST or ALL
        emit_all: bool,
        window: Option<i64>,                // nanoseconds
        brackets: Option<BracketSpec>,
        step_properties: Vec<StepPropertyRef>,  // filled by demand analysis
        fused_downstream: Option<FusedDownstream>, // set by optimizer Pass 6
        input: Box<LogicalPlan>,
    },
    Sessionize {
        gap: i64,                           // nanoseconds
        end_event: Option<String>,
        forwarded_columns: Vec<ColumnId>,   // filled by demand analysis — see §8.3
        fused_downstream: Option<FusedDownstream>,
        input: Box<LogicalPlan>,
    },
    EventSelect {
        kind: EventSelectKind,              // FIRST | LAST | NTH(n)
        event_type: String,
        predicate: Option<TypedExpr>,
        forwarded_columns: Vec<ColumnId>,
        fused_downstream: Option<FusedDownstream>,
        input: Box<LogicalPlan>,
    },
    Sample {
        spec: SampleSpec,
        input: Box<LogicalPlan>,
    },
    Attribute {
        conversion_event: String,
        touchpoint_event: String,
        window: i64,
        /// Type-checked touchpoint-key expression. Resolves against the
        /// touchpoint event's schema; must produce String.
        touchpoint_key: TypedExpr,
        /// Conversion-side forwarded properties, populated by demand analysis.
        /// Accessed downstream as `<conversion_event>.<column>`.
        forwarded_conversion_columns: Vec<ColumnId>,
        fused_downstream: Option<FusedDownstream>,
        input: Box<LogicalPlan>,
    },
}
```

### 5.3 FusedDownstream

The optimizer communicates to the physical planner that a stateful operator should absorb its downstream consumer:

```rust
pub enum FusedDownstream {
    Aggregate(FusableAggregate),
    FilterThenAggregate {
        filter: TypedExpr,
        aggregate: FusableAggregate,
    },
}

pub struct FusableAggregate {
    pub functions: Vec<AggFunction>,        // COUNT, SUM, AVG, etc.
    pub arguments: Vec<TypedExpr>,
    pub group_by: Vec<TypedExpr>,
    pub output_names: Vec<String>,
}
```

When `fused_downstream` is `Some`, the optimizer has already removed the corresponding `Filter` and `Aggregate` nodes from the plan tree. The physical planner emits a single fused physical operator rather than a chain.

### 5.4 Schema Computation Rules

Each logical node computes its output schema from its input:

| Node              | Output schema                                                                               |
| ----------------- | ------------------------------------------------------------------------------------------- |
| `Scan`            | Catalog-resolved table schema, possibly with join-combined columns                          |
| `Filter`          | Identical to input                                                                          |
| `Project`         | Defined entirely by the projection list; starred projections prepend the input's columns    |
| `SequenceMatch`   | `entity_id`, `$var` columns, named step properties (demand-driven), plus `step_reached`/`match_duration`/`match_events` (demand) |
| `Sessionize`      | Input columns plus `session_id`, `session_duration` (when demanded)                         |
| `EventSelect`     | `entity_id`, event `ts`, demanded forwarded properties                                      |
| `Aggregate`       | Group-by keys plus named aggregate results                                                  |
| `Sort`            | Identical to input                                                                          |
| `Limit`           | Identical to input                                                                          |
| `Window`          | Input columns plus the window function's output name                                        |
| `Pivot`           | One group-by column plus one column per distinct pivot value                                |
| `SubqueryFilter`  | Identical to outer input                                                                    |
| `Attribute`       | `entity_id`, `conversion_ts`, forwarded conversion properties, `touchpoint_ts` (nullable), `touchpoint_key: String` (nullable) — one row per (entity, conversion, matched-touchpoint) |

Exact column names, types, and nullability are defined in type-system.md §6.

---

## 6. Optimizer

### 6.1 Design Decisions

| Question                          | Decision                                | Rationale                                                                 |
| --------------------------------- | --------------------------------------- | ------------------------------------------------------------------------- |
| Cost model?                       | Rule-based only                         | Transformations are always beneficial for linear pipelines                |
| Pass ordering?                    | Fixed, single pass                      | No circular interactions between rules                                    |
| Fixpoint iteration?               | Not needed                              | One pass per rule is provably sufficient                                  |
| Multi-query optimization?         | Not in v1                               | Aliases provide manual sharing                                            |
| Plan caching across queries?      | Not in v1                               | Compilation is cheap relative to execution                                |
| Statistics from storage?          | Not in v1                               | No cardinality estimates; zone maps used only at scan runtime             |

The optimizer is a sequence of small rewriters. Each rewriter is a function from `LogicalPlan` to `LogicalPlan` that visits the tree and performs a specific transformation. They are composed in a fixed order; running them twice is always a no-op on a stable plan.

### 6.2 Pass Order

The six passes run in this exact order. Each pass depends on the output form of its predecessors:

1. **Expression inlining** — resolve `Let` / computed columns to their defining expressions
2. **Predicate pushdown** — move `Filter` nodes closer to scans
3. **Scan predicate extraction from MATCH** — derive event-type and property filters from the pattern
4. **Projection pruning / demand collection** — determine which columns the scan must produce
5. **Constant folding** — evaluate constant subexpressions
6. **General fusion detection** — set `fused_downstream` on stateful operators

### 6.3 Pass 1: Expression Inlining

`LET x = f(a, b)` desugars to `Project(*, f(a, b) AS x)`, and downstream references to `x` become references to a projected column. Expression inlining walks the plan and substitutes the defining expression wherever `x` is referenced in a context that can be absorbed by a later pass (a filter predicate or aggregate argument).

Example:

```
Match(emit_all) → Project(*, (step_reached >= 2) AS converted) → Aggregate(rate = AVG(CAST(converted AS INT)))
```

becomes

```
Match(emit_all) → Aggregate(rate = AVG(CAST(step_reached >= 2 AS INT)))
```

The `Project` that only added the `converted` column can be dropped because its sole consumer was the aggregate, and the inlined expression produces the same result.

**Why first?** Inlining exposes the true expression being aggregated, which Pass 6 (fusion) needs to reason about fusability.

### 6.4 Pass 2: Predicate Pushdown

Move `Filter` nodes upward through the tree (toward the scan) whenever the predicate references only columns produced by the scan (or by nodes above the destination).

Rules (walking downward from a `Filter`):

| Predicate references                                  | Can push past?                      |
| ------------------------------------------------------ | ----------------------------------- |
| Event-stream columns only (`event_type`, `ts`, props) | Push past `SequenceMatch`, `Sessionize`, `EventSelect` |
| `step_reached`, `$var`, named step properties         | **Cannot** push past `SequenceMatch` |
| `session_id`, `session_duration`                      | **Cannot** push past `Sessionize`    |
| Aggregate result columns                              | **Cannot** push past `Aggregate`    |
| Forwarded columns from `Attribute`                    | **Cannot** push past `Attribute`    |

Predicate pushdown is correct only when all referenced columns exist upstream of the destination. The optimizer verifies this before each move.

Pass 2 is the sole mechanism for moving filters past stateful operators. There is no separate "filter-before-match reordering" pass — correctness of MATCH ordering follows from the pushdown rules in the table above (event-stream filters can cross MATCH; match-output filters cannot).

### 6.5 Pass 3: Scan Predicate Extraction from MATCH

Even without a user-written `WHERE`, a `MATCH` pattern implies constraints on the events the scan must produce:

1. **Event-type filter.** Collect the set of event types mentioned in every step (including negation target types, which the NFA needs to see to poison transitions). Add `event_type IN (…)` to the scan predicates.
2. **Property predicate pushdown.** For each step's `WHERE`, extract clauses that reference only the step's event type and lift them to the scan as `(event_type != 'X' OR <predicate>)`. This is Level 1 / Level 2 pushdown from sequence-matching.md §9.

Example:

```
-- Input plan:
Scan(events) → SequenceMatch(s: signup THEN p: purchase WHERE p.amount > 100)

-- After Pass 3:
Scan(events,
     predicates = [event_type IN ('signup', 'purchase'),
                   event_type != 'purchase' OR amount > 100])
  → SequenceMatch(s: signup THEN p: purchase WHERE p.amount > 100)
```

The MATCH operator still evaluates the full predicate itself — the scan is an early filter, not a replacement. This keeps pushdown decisions independent of the NFA semantics.

**Interaction with negation.** Negation target types must be in the scan filter; otherwise the NFA wouldn't see the events that trigger poison transitions. The extraction logic explicitly unions negation types into the event-type set before generating the `IN` clause.

### 6.6 Pass 4: Projection Pruning (Demand Collection)

Walk the tree bottom-up from the output (top) node, computing which columns each node actually uses. Each node contributes its own requirements (predicate columns, group-by keys, aggregate arguments, variable bindings) and forwards everything downstream asks for (minus columns the node produces itself).

At the scan, the accumulated set becomes `projected_columns`. The scan only reads those columns from disk.

**Example.** Consider a query with real column demand on every operator:

```bql
events LAST 30d
| MATCH FIRST SEQUENCE(
    s: signup WHERE country = 'US'
    THEN p: purchase WHERE amount > 100
  ) WITHIN 7d EMIT ALL
| WHERE s.plan = 'pro'
| STATS
    entered   = COUNT(*),
    converted = SUM(CAST(step_reached >= 2 AS INT)),
    avg_price = AVG(p.amount)
  GROUP BY QUANTIZE(s.ts, 1d) AS day
```

Walking bottom-up from the Aggregate node, the demand accumulates like this:

| Node              | Demand contributed                                                     | Demand passed upstream                                                                                   |
| ----------------- | ---------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| `Aggregate`       | `step_reached`, `p.amount`, `s.ts` (for `QUANTIZE` group key)          | same                                                                                                     |
| `Filter (s.plan)` | `s.plan`                                                               | `step_reached`, `p.amount`, `s.ts`, `s.plan`                                                             |
| `SequenceMatch`   | Produces `step_reached`, step props; strips match-output columns; adds its own needs (`country`, `amount` for predicates) | `entity_id`, `ts`, `event_type`, `country`, `amount`, `plan` (scan-side columns; step props resolved to these) |
| `Scan`            | Accumulated demand becomes `projected_columns`                         | —                                                                                                        |

So the scan reads **six** columns from disk (`entity_id`, `ts`, `event_type`, `country`, `amount`, `plan`) instead of the entire events schema. Columns like `device`, `referrer`, `session_id`, etc. are never decoded.

This pass is also where step-property demand is computed. Downstream references `s.plan`, `p.amount`, and `s.ts` are all named-step-property references; the planner records them as `StepPropertyRef` entries in `SequenceMatch.step_properties` and resolves each one to a `(step_index, column_name, bql_type)` triple at physical planning time (see §8.2 and §9.3).

**Contrast: no-demand query.** The trivial case `events | MATCH(signup THEN purchase) | STATS total = COUNT(*)` demands only `entity_id`, `ts`, and `event_type` — no property columns at all, no step properties. The gap between this and the example above is exactly what pruning saves.

### 6.7 Pass 5: Constant Folding

Evaluate constant subexpressions at plan time:

- Duration literals (`7d`, `30m`, `2h15m`) parsed to nanosecond integers (actually done earlier during typing, but remaining cases are caught here).
- `'2025-01-01T00:00:00Z'` literals parsed to nanosecond epoch integers.
- Arithmetic on literals (`3 * 1_000_000_000`) reduced to a single value.
- Redundant CASTs (`CAST(x AS SAME_TYPE_AS_X)`) removed.
- Identity operations (`x AND true`, `x OR false`, `x + 0`) simplified.

Constant folding runs after predicate pushdown so that predicates that become constants (`true` / `false`) can be eliminated before the remaining passes.

### 6.8 Pass 6: General Fusion Detection

The largest and most impactful pass. Detailed in §7.

---

## 7. General Fusion Framework

### 7.1 Principle

Any stateful entity operator that produces per-entity output can fuse with a downstream aggregate consumer that only needs a reduction of that output. Fusion avoids materializing per-entity rows that get immediately collapsed.

The prototypical case is a funnel:

```
Scan → Match(emit_all) → Aggregate(step counts)
```

Without fusion, the MATCH operator emits one row per entity with a `step_reached` column, then the Aggregate hashes those rows into counts. With fusion, the MATCH operator directly increments a `[u64; num_steps]` array as matches complete. The intermediate rows never exist.

### 7.2 Fusion Eligibility Rules

An `Aggregate` (optionally preceded by a `Filter`) fuses into an upstream stateful operator if all of the following hold:

1. **Adjacency**. The stateful operator and the aggregate are immediately adjacent in the plan, or separated only by a `Filter` that itself becomes part of the fusion (the filter is absorbed as `FusedDownstream::FilterThenAggregate`).
2. **Incremental computability**. Every aggregate function must be updatable one entity (or one match, or one session) at a time.
   - `COUNT`, `COUNT_DISTINCT`, `SUM`, `MIN`, `MAX` — trivially incremental.
   - `AVG` — incremental as `sum + count`.
   - `P50`, `P90`, `P95`, `P99` — incremental via **DDSketch** (bounded relative error, ~1–2 KB per group). DDSketch's relative-error guarantee and constant-time merge make it ideal for fused accumulators in stateful operators. The planner relies on this sketch being incremental so percentile aggregates never block fusion. The v1 aggregate list is fixed in query-language.md §7.1.
3. **Group-by key availability**. Every group-by expression must reference columns available inside the stateful operator's output schema:
   - `SequenceMatch`: `entity_id`, bound `$var`s, named step properties (`s.ts`, `p.amount`), `step_reached`, `match_duration`, or pattern-intrinsic columns like `bracket`.
   - `Sessionize`: `entity_id`, `session_id`, `session_duration`.
   - `EventSelect`: `entity_id`, event timestamp, forwarded properties.
   - `Attribute`: `entity_id`, `conversion_ts`, forwarded conversion properties.
4. **No ordering dependency** between the stateful operator and the aggregate. `ORDER BY` in between blocks fusion because the aggregate needs to see rows in a specific order that the stateful operator doesn't guarantee.

All aggregation functions are incrementally computable, so **fusion eligibility never fails due to an unsupported aggregate function**. This is an explicit design decision — it eliminates the combinatorial complexity of partial fusion (some aggregates fused, some not).

### 7.3 Fusion Detection Algorithm

For each stateful operator node in the plan (walked top-down):

1. Look at the immediate downstream consumer.
2. If it's a `Filter`, look past it at the next consumer.
3. If the consumer is an `Aggregate` and the eligibility rules pass:
   - Extract the aggregate's functions, arguments, group-by, and output names into a `FusableAggregate`.
   - Extract any intermediate filter's predicate.
   - Set `fused_downstream` on the stateful operator.
   - Remove the intermediate `Filter` and the `Aggregate` nodes from the plan.
4. If eligibility fails, leave the chain intact.

Fusion is never attempted beyond the immediate aggregate. A chain of three aggregates (unusual) would fuse only the first into the upstream operator.

### 7.4 Per-Operator Fusion Strategies

#### 7.4.1 MATCH Fusion

| Downstream pattern                                    | Fused strategy                                             | What's avoided                                |
| ----------------------------------------------------- | ---------------------------------------------------------- | --------------------------------------------- |
| `STATS total = COUNT(*)`                              | Boolean per-entity match flag, single counter              | No `match_events`, no `match_duration`        |
| `STATS count = COUNT(*) GROUP BY $var`                | Boolean per binding track, hash counter on binding value   | Per-track flag only                            |
| `STATS steps = SUM(CAST(step_reached >= N AS INT))`  | Direct step counter `[u64; num_steps]`                     | No per-entity rows                            |
| `STATS avg = AVG(match_duration)`                     | Track anchor + last-step timestamp, running sum+count      | No `match_events` extraction                  |
| `STATS avg = AVG(s.amount)`                           | Extract one property at one step (step-property demand)   | No full match trace                           |
| `WHERE step_reached >= 2 \| STATS total = COUNT(*)`  | Step counter, increment only on qualifying entities        | Filter fused into the counter update          |
| `STATS total = COUNT(*) GROUP BY QUANTIZE(s.ts, 1d) AS day` | Track anchor timestamp, bucket inline at commit time       | No intermediate rows, no separate projection  |

Strategy selection inside MATCH proceeds through the matrix in sequence-matching.md §10.2, which is orthogonal to fusion: a fused MATCH can still be a `StepCounter` or a full `NFA` depending on pattern class (sequence-matching.md §10.1).

#### 7.4.2 SESSIONIZE Fusion

| Downstream pattern                                                        | Fused strategy                                                    | What's avoided                     |
| ------------------------------------------------------------------------- | ----------------------------------------------------------------- | ---------------------------------- |
| `STATS sessions = COUNT_DISTINCT(session_id)`                             | Per-entity session counter                                        | No `session_id` on every event     |
| `STATS avg = AVG(session_duration)`                                       | Track start/end timestamps per session, running sum+count         | No per-event annotation            |
| `STATS events = COUNT(*) GROUP BY session_id`                             | Events-per-session counter                                        | No column materialization          |
| `STATS first_page = FIRST_VALUE(page), dur = MAX(ts) - MIN(ts) GROUP BY session_id` | Per-session accumulator with column forwarding (see §8.3)        | Full `session_id` materialization  |

#### 7.4.3 FIRST / LAST / NTH Fusion

| Downstream pattern                      | Fused strategy                            | What's avoided             |
| --------------------------------------- | ----------------------------------------- | -------------------------- |
| `STATS total = COUNT(*)`                | Boolean existence check per entity        | No event extraction        |
| `STATS avg = AVG(amount)`               | Extract single property, accumulate       | No full row materialization |
| `STATS total = COUNT(*) GROUP BY device` | Extract group key, update hash counter    | Minimal extraction         |

#### 7.4.4 ATTRIBUTE Fusion

ATTRIBUTE already emits flat per-touchpoint rows (query-language.md §14.3), so the downstream is a plain `STATS … GROUP BY touchpoint_key` — there is no list to materialize and no UNNEST to fuse away.

| Downstream pattern                                                                 | Fused strategy                                               | What's avoided                            |
| ---------------------------------------------------------------------------------- | ------------------------------------------------------------ | ----------------------------------------- |
| `STATS total = COUNT(*) GROUP BY touchpoint_key`                                   | Per-touchpoint accumulation directly in the attribution loop | No intermediate row emission              |
| `STATS revenue = SUM(purchase.amount) GROUP BY touchpoint_key`                     | Per-touchpoint accumulator keyed by `touchpoint_key`, updated at each emission with the demanded conversion property | Zero per-row materialization |
| `WHERE touchpoint_ts IS NOT NULL \| STATS COUNT(*) GROUP BY touchpoint_key`        | Un-attributed rows filtered at emission time; fused counter  | LEFT-UNNEST rows never leave the operator |

Last-touch / first-touch / time-decay attribution requires a window function between ATTRIBUTE and STATS and is not a fusion target — the window function is a pipeline breaker per §7.2 rule 4 (ordering dependency), so the planner materializes the flat rows for the window phase and aggregates them downstream.

### 7.5 Layered Extraction

A single stateful operator can fuse with many different downstream shapes. Rather than implementing a separate code path per shape, stateful operators use **layered extraction**: a fixed inner loop with independently toggled optional hooks that run at match/session/event completion.

```rust
pub struct MatchExecutionConfig {
    // Core (always runs): NFA / step counter transitions + step_reached tracking.
    pub track_match_duration: bool,
    pub track_match_events: bool,
    pub step_properties: Vec<StepPropertyExtraction>,
    pub fused_accumulator: Option<Box<dyn Accumulator>>,
}

pub struct StepPropertyExtraction {
    pub step_index: u8,
    pub column_name: String,
    pub bql_type: BqlType,
}
```

**Relationship to `StepPropertyRef`.** `StepPropertyRef` (see §9.3) identifies a step by its user-facing name (`s`, `p`) as it appears in the query. `StepPropertyExtraction` identifies a step by its resolved index (0, 1, …) inside the compiled pattern. The physical planner resolves every `StepPropertyRef` from the `DemandSet` into a `StepPropertyExtraction` during physical planning, using the pattern's step name table.

The NFA / step-counter inner loop has no demand-related branches. At match completion (which is infrequent relative to per-event transitions), the operator executes the enabled hooks:

```rust
fn on_match_complete(&mut self) {
    let duration = if self.config.track_match_duration {
        Some(self.last_step_ts - self.anchor_ts)
    } else { None };

    let events = if self.config.track_match_events {
        Some(self.build_match_events_map())
    } else { None };

    for extraction in &self.config.step_properties {
        // extract value from retained event reference
    }

    if let Some(acc) = &mut self.config.fused_accumulator {
        // Accumulator::update takes (group_key, values) — values are laid out
        // in FusableAggregate::functions order. See execution-model.md §9.4
        // for the trait and sequence-matching.md §13.4 for the MATCH call site.
        acc.update(group_key.as_deref(), &reduced_values);
    } else {
        self.output_batch.push(/* ... */);
    }
}
```

`Sessionize`, `EventSelect`, and `Attribute` use the same pattern. The set of hooks differs per operator but the principle is identical: branch only at completion, never in the per-event hot loop.

### 7.6 Multi-Level Fusion Through Filters

After Pass 1 (expression inlining), a chain like:

```
Match → LET converted = step_reached >= 2 → WHERE converted → STATS total = COUNT(*)
```

collapses to:

```
Match → WHERE step_reached >= 2 → STATS total = COUNT(*)
```

Pass 6 then detects the filter-then-aggregate shape and fuses both into the match as `FusedDownstream::FilterThenAggregate`. The entire chain collapses into a single physical operator.

### 7.7 Chains of Stateful Operators

For v1, fusion does not cross stateful operator boundaries. A chain like:

```
Sessionize → Match → STATS
```

fuses MATCH with STATS, but SESSIONIZE remains a separate physical operator that materializes its output (`session_id` annotations) for MATCH to consume.

The special case `MATCH ... WITHIN SESSION` semantically fuses session boundaries into the NFA — MATCH observes the sequential `session_id` column (guaranteed sequential per query-language.md §30.2) and resets its state on session change. This is a MATCH-internal optimization, not a cross-operator fusion.

Cross-stateful-operator fusion (e.g., sessionize-into-match) is a potential v2 enhancement and is not pursued now.

---

## 8. Column Forwarding

### 8.1 Concept

Column forwarding is the ability for stateful operators to carry specific column values from their input events through to their output, **driven by downstream demand**. Instead of "always emit all columns" or "always emit a fixed set", the operator extracts only the columns that the downstream actually uses.

Column forwarding is computed during Pass 4 (projection pruning) and recorded on the stateful operator node. The physical planner then configures the operator's layered extraction to retain precisely those columns.

### 8.2 MATCH Column Forwarding

`MATCH` with named steps (`s: signup THEN p: purchase`) permits downstream references to per-step columns (`s.device`, `p.amount`). Demand analysis walks downstream from the MATCH and collects every such reference:

```
Match(s THEN p) → Select s.device, p.amount | ...
   step_properties: [
     (step=s, column=device, type=String),
     (step=p, column=amount, type=Float),
   ]
```

The step property resolution is:

1. Find the step in the pattern with the given name.
2. Look up the column in the step's event type's schema.
3. Record the (step_index, column_name, bql_type) triple.

If the referenced step name is not defined in the pattern, the planner raises `TypeError::StepNotFound { step_name, available }` (type-system.md §12). If the column doesn't exist on the step's event type, it raises `TypeError::ColumnNotFound` with a `context` field identifying the referenced step.

At execution, the MATCH operator retains a reference (row index within the sub-batch or an extracted value) for each demanded step-property at the moment that step's event is consumed. Only the demanded properties are retained; everything else is discarded.

Per-(step, property) demand is finer-grained than per-column demand — a query that references `s.ts` but not `p.ts` only retains step `s`'s timestamp.

Named step property types are added to the MATCH output schema during plan construction, so downstream type checking sees them exactly as if they were first-class columns.

### 8.3 SESSIONIZE Column Forwarding

A fused SESSIONIZE + per-session aggregate needs to retain specific column values at session boundaries:

```
events | SESSIONIZE(gap: 30m)
       | STATS
           first_page = FIRST_VALUE(page),
           dur = MAX(ts) - MIN(ts),
           event_count = COUNT(*)
         GROUP BY session_id
```

The fused operator's per-session accumulator retains:

```rust
struct SessionAccumulator {
    session_start_ts: i64,
    session_latest_ts: i64,
    event_count: u64,
    first_page: Option<String>,   // from the first event in the session
    min_ts: i64,
    max_ts: i64,
}
```

When a gap is detected, the accumulator is finalized and emitted (or fed into a downstream accumulator, if any). Then it resets for the new session.

Without fusion, SESSIONIZE would materialize `session_id` on every input event and a separate HashAggregate would group by it. With fusion, the work is a single streaming pass with `O(active_sessions_per_entity)` state instead of `O(events)` intermediate data.

**Special case: `GROUP BY session_id` enables eager emission.** When the fused aggregate groups by `session_id` (and only `session_id`), the SESSIONIZE operator knows exactly when each group is complete: the moment a gap closes out the current session, no further events will ever land in that `session_id`. The accumulator for the closed session can be **finalized and emitted immediately**, freeing its state before the next session begins. This is structurally the same guarantee entity boundaries provide for the rest of the plan — the operator knows a group's boundary is final and does not need to keep the group open waiting for late arrivals.

No other group-by key has this property. For `GROUP BY device` or `GROUP BY session_id, country`, the fused operator must hold accumulators open until the end of the entity's event stream (or even the end of the entire scan, if the group key spans entities) because a future event could always add to any existing group. The `session_id`-alone special case is the only one where the upstream operator can exploit the group's closure point as a streaming emission point.

This has two implications:

1. **Memory.** Fused `SESSIONIZE | STATS … GROUP BY session_id` holds only the currently-active session's state (`O(1)` per entity), not all sessions seen so far. For entities with thousands of sessions, this is a large saving.
2. **Latency / pipeline-ability.** Downstream operators receive per-session rows as sessions close, rather than waiting for entity boundaries. This does not change the per-query result (results are order-independent under the fused aggregate) but it smooths the memory profile.

The same principle applies in MATCH's `WITHIN SESSION` mode: MATCH observes `session_id` transitions and treats them exactly like entity-end signals for the purpose of flushing active candidates (sequence-matching.md §16 safety valves).

### 8.4 ATTRIBUTE Column Forwarding

`ATTRIBUTE` handles touchpoint data inline via its `touchpoint_key` expression — there is no "touchpoint forwarding" in the usual sense because ATTRIBUTE never carries touchpoint rows as structured data. The expression is evaluated once per qualifying touchpoint and its String result becomes the `touchpoint_key` output column. The only columns ATTRIBUTE forwards are **conversion properties**, which are demand-driven.

```bql
events
| ATTRIBUTE(
    conversion: purchase,
    touchpoints: ad_click,
    window: 30d,
    touchpoint_key: channel
)
| STATS revenue = SUM(purchase.amount) GROUP BY touchpoint_key
```

Demand analysis yields:

- **Conversion forwarding**: `amount` from `purchase` (referenced downstream as `purchase.amount`).
- **Touchpoint-key expression**: needs `channel` from `ad_click` (the expression's column references become part of `required_columns()`).

Only those columns are read from the scan. `amount` is retained on the `AttributePhysical`'s per-entity state at the moment each `purchase` event is consumed (so it can be attached to every touchpoint row emitted for that conversion). `channel` is read only when evaluating the `touchpoint_key` expression on each `ad_click` event; no retention is needed because the expression's String result is stored directly in the deque entry.

**Deque element shape** (`AttributePhysical` per-entity state):

```rust
struct TouchpointDequeEntry {
    ts: i64,               // always — needed for window check and output
    key: String,           // always — pre-computed touchpoint_key expression result
}
```

The deque entry does not carry raw touchpoint row data. That's the point of the auto-unnest design: by collapsing the "per-touchpoint structured payload" into a single pre-computed String, the state per touchpoint is minimal and has no type-system dependency on `List(Map)` / `List(Struct)`.

---

## 9. Physical Planning

### 9.1 Responsibilities

Convert the optimized logical plan into an executable physical plan:

1. **Strategy selection.** Pick a concrete implementation strategy for each logical node. For MATCH, this means choosing between `StepCounter`, `DedicatedConsecutive`, or `FullNfa` per sequence-matching.md §10.2.
2. **Demand propagation.** Compute the `DemandSet` at each node using a backward pass from the root. This is the protocol discussed informally in execution-model.md §8.2; this document is the formal definition.
3. **Fused-operator emission.** When a logical node has `fused_downstream` set, emit a single fused physical plan node instead of the original chain.
4. **Compile expressions.** Turn `TypedExpr` into `CompiledExpr` — a form optimized for batch evaluation. `CompiledExpr` is a planner-owned type that holds pre-resolved column indices, pre-coerced literals, and a small bytecode tree suitable for per-batch evaluation by Arrow compute kernels.
5. **Assemble the physical plan tree** — a plain-data tree of structs like `ScanPhysical`, `SequenceMatchPhysical`, `AggregatePhysical`. The planner does not produce `Box<dyn PhysicalOperator>` values; that binding step happens in `bqlite-engine` (see §15). Stateful nodes carry enough information for the engine to choose between the `PhysicalOperator` and `EntityOperator` traits (execution-model.md §3.2 and §4) and apply the `EntityOperatorAdapter` (execution-model.md §4.1) as needed.

### 9.2 Demand Propagation Protocol

A backward pass through the physical plan tree:

1. Start at the output (root) node. The root's demand is the user-requested projection plus any implicit system columns (`__seq_id` for stable ordering, etc.).
2. Walk backward (toward the scan).
3. Each node receives a `DemandSet` value from its downstream consumer.
4. Each node transforms the demand:
   - **Strips** columns it produces itself (downstream asked for `session_id`, but sessionize produces it — so scan doesn't need it).
   - **Adds** columns it needs internally (predicate columns, group-by keys, variable binding columns).
   - **Passes the transformed demand upstream.**
5. When demand reaches the scan, the accumulated column set becomes `projected_columns`.

This backward pass is the ultimate source of truth for which columns are read from disk.

### 9.3 DemandSet

`DemandSet` is the downstream-needs struct propagated backward through the plan. It is the planner's source of truth for what each node must produce.

```rust
pub struct DemandSet {
    /// Columns the downstream needs to see.
    pub columns: HashSet<ColumnId>,
    /// Whether `match_events` and `match_duration` are needed.
    pub needs_match_detail: bool,
    /// Whether `step_reached` is needed.
    pub needs_step_reached: bool,
    /// Named step properties needed (per step, per column).
    pub step_properties: Vec<StepPropertyRef>,
    /// Forwarded columns needed from sessionize / attribute.
    pub forwarded: Vec<ColumnId>,
    /// Fused aggregate specification, if fusion is active.
    pub fused_aggregate: Option<FusableAggregate>,
    /// Fused filter predicate, if fusion is active.
    pub fused_filter: Option<TypedExpr>,
}
```

`StepPropertyRef` is the finer-grained per-(step, column) demand bit introduced in §8.2:

```rust
pub struct StepPropertyRef {
    pub step_name: String,
    pub column_name: String,
    pub bql_type: BqlType,
}
```

**Relationship to `DemandCapabilities` in sequence-matching.md §13.5.** Sequence-matching.md uses a distinct type named `DemandCapabilities` returned by `supported_demands()` — that is the **operator side** of the protocol, advertising which demand shapes the operator supports (step counts, match details, aggregation fusion, step property forwarding). `DemandSet` in this document is the **planner side** — what the downstream needs. The two are dual: the planner constructs a `DemandSet` during the backward pass, then matches it against the operator's `DemandCapabilities` to select a strategy. Sequence-matching.md §13.5 documents this distinction explicitly.

### 9.4 Strategy Selection for MATCH

The physical planner examines, in order:

1. **Pattern classification** (sequence-matching.md §10.1) — compute `PatternClass` by inspecting the logical `SequencePattern`. Variants: `LinearSimple`, `LinearImmediate`, `LinearWithNegation`, `LinearWithBindings`, `LinearFull`, `GeneralNfa`.
2. **Demand set** — what downstream actually wants to see.
3. **Fusion** — is `fused_downstream` set?

Then it selects a strategy from the matrix in sequence-matching.md §10.2:

| `PatternClass`        | Strategy                                                             |
| --------------------- | -------------------------------------------------------------------- |
| `LinearSimple`        | `StepCounter` (with fused accumulator when `fused_downstream` is set) |
| `LinearImmediate`     | `DedicatedConsecutive` (consecutive-event matcher)                   |
| `LinearWithNegation`  | `StepCounter` + poison flags                                         |
| `LinearWithBindings`  | `StepCounter` per binding track + candidate deque                    |
| `LinearFull`          | `StepCounter` + poison + bindings                                    |
| `GeneralNfa`          | `FullNfa` with candidate propagation                                 |

When the `DemandSet` requires full match details (`needs_match_detail = true`), any pattern class falls back to `FullNfa` with path tracking, regardless of the table above (matching sequence-matching.md §10.2's "Match details" row).

When `fused_aggregate` is set, the Aggregate physical operator is **not** emitted separately — MATCH directly produces aggregated output through its layered extraction hooks (§7.5). Fusion is orthogonal to strategy selection: a fused MATCH can still be a `StepCounter` or a `FullNfa` depending on the pattern class.

### 9.5 Physical Operator Types

```rust
pub struct ScanPhysical {
    pub table: TableRef,
    pub time_range: TimeRange,
    pub scan_predicates: Vec<CompiledExpr>,
    pub projected_columns: Vec<ColumnId>,
}

pub struct FilterPhysical {
    pub predicate: CompiledExpr,
}

pub struct ProjectPhysical {
    pub expressions: Vec<(CompiledExpr, String)>,
}

pub struct SequenceMatchPhysical {
    pub compiled_nfa: CompiledNfa,
    pub strategy: MatchStrategy,
    pub demand: DemandSet,
    pub execution_config: MatchExecutionConfig,  // layered extraction
    pub fused_aggregate: Option<FusableAggregate>,
    pub fused_filter: Option<CompiledExpr>,
}

pub struct SessionizePhysical {
    pub gap: i64,
    pub end_event: Option<String>,
    pub demand: DemandSet,
    pub forwarded_columns: Vec<ColumnId>,
    pub fused_aggregate: Option<FusableAggregate>,
}

pub struct EventSelectPhysical {
    pub kind: EventSelectKind,
    pub event_type: String,
    pub predicate: Option<CompiledExpr>,
    pub forwarded_columns: Vec<ColumnId>,
    pub fused_aggregate: Option<FusableAggregate>,
}

pub struct AttributePhysical {
    pub conversion_event: String,
    pub touchpoint_event: String,
    pub window: i64,
    /// Compiled touchpoint_key expression — evaluated per qualifying touchpoint,
    /// result is a String stored in the deque entry.
    pub touchpoint_key: CompiledExpr,
    /// Conversion-side demanded properties (forwarded onto every emitted row).
    pub forwarded_conversion_columns: Vec<ColumnId>,
    pub demand: DemandSet,
    pub fused_aggregate: Option<FusableAggregate>,
}

pub struct AggregatePhysical {
    pub aggregates: Vec<CompiledAgg>,
    pub group_by: Vec<CompiledExpr>,
}

pub struct SortPhysical { /* ... */ }
pub struct LimitPhysical { pub count: u64 }
pub struct PivotPhysical { /* ... */ }
pub struct WindowPhysical { /* ... */ }
pub struct SubqueryFilterPhysical { /* ... */ }
pub struct SamplePhysical { /* ... */ }
```

Each struct above is a **plain-data description**, not an executable operator. When `bqlite-engine` binds the plan, each description becomes an instance of either `PhysicalOperator` (stateless or holdable-in-memory) or `EntityOperator` wrapped in `EntityOperatorAdapter` (stateful across entity boundaries), per execution-model.md §3.2 and §4.

---

## 10. EXPLAIN Output

### 10.1 Format

`EXPLAIN <pipeline>` compiles the pipeline through every stage and prints the resulting physical plan without executing it. The format is a compact, indented tree with per-node fields:

```
EXPLAIN events LAST 30d
| MATCH FIRST SEQUENCE(s: signup THEN p: purchase WHERE p.amount > 100) WITHIN 7d EMIT ALL
| STATS entered = COUNT(*), converted = SUM(CAST(step_reached >= 2 AS INT))
```

```
FusedSequenceMatchAggregate
  strategy      : StepCounter (LinearSimple)
  pattern       : 2 steps, window: 7d, emit_all: true
  fused_agg     : [entered = COUNT(*), converted = SUM(CAST(step_reached >= 2 AS INT))]
  fused_filter  : none
  step_properties: none
  └── Scan(events)
        time_range : [now-30d, now+7d]   (extended by 7d window)
        predicates : event_type IN ('signup', 'purchase')
                     AND (event_type != 'purchase' OR amount > 100)
        columns    : [entity_id, ts, event_type, amount]
```

The format surfaces:

- **Extended time range** (with a parenthetical noting the extension)
- **Scan predicate pushdown** (derived + user predicates)
- **Projection pruning** (only the columns actually read)
- **Fusion** (as `FusedXxxAggregate` operator names)
- **Strategy selection** (the `StepCounter`/`FullNfa` name and the inferred `PatternClass`)

The exact rendering format is deferred per query-language.md §30.7 — this section describes the required information, not the precise character layout.

### 10.2 ExplainNode Representation

EXPLAIN is produced by walking the physical plan into an `ExplainNode` tree and pretty-printing:

```rust
pub enum ExplainNode {
    Scan {
        table: String,
        time_range: String,
        predicates: Vec<String>,
        columns: Vec<String>,
    },
    Filter { predicate: String, input: Box<ExplainNode> },
    Project { columns: Vec<String>, input: Box<ExplainNode> },
    SequenceMatch {
        strategy: String,
        pattern_class: String,
        steps: usize,
        window: Option<String>,
        emit_all: bool,
        fused_agg: Option<Vec<String>>,
        fused_filter: Option<String>,
        step_properties: Vec<String>,
        input: Box<ExplainNode>,
    },
    Sessionize {
        gap: String,
        forwarded: Vec<String>,
        fused_agg: Option<Vec<String>>,
        input: Box<ExplainNode>,
    },
    EventSelect { /* ... */ },
    Attribute { /* ... */ },
    Aggregate {
        functions: Vec<String>,
        group_by: Vec<String>,
        input: Box<ExplainNode>,
    },
    Sort { keys: Vec<String>, input: Box<ExplainNode> },
    Limit { count: u64, input: Box<ExplainNode> },
    Pivot { /* ... */ },
    Window { /* ... */ },
}
```

`ExplainNode` is a simple string-based mirror of the physical plan — it exists so that EXPLAIN output is stable across executor changes.

### 10.3 What EXPLAIN Does Not Show

- Runtime statistics (row counts, timings). `EXPLAIN ANALYZE` is a potential v2 addition.
- Arrow buffer layouts.
- Compaction state of scanned segments.

EXPLAIN is a planner introspection tool. Runtime metrics belong in execution-model.md §14 and flow through a separate metrics channel.

---

## 11. Worked Example: End-to-End Plan Construction

To tie the pipeline together, here is the full compilation trace for a realistic query.

### 11.1 Input Query

```
events LAST 30d
| MATCH FIRST SEQUENCE(
    s: signup WHERE s.country = 'US'
    THEN p: purchase WHERE p.amount > 50
  ) WITHIN 7d EMIT ALL
| WHERE s.plan = 'pro'
| STATS converted = SUM(CAST(step_reached >= 2 AS INT)),
        total    = COUNT(*)
```

### 11.2 After Parsing

```
Statement::Query(Pipeline {
  source: Source {
    primary: "events",
    time_range: Some(LAST 30d),
    joins: [],
  },
  operators: [
    Match(pattern, mode=FIRST, emit_all=true, window=7d),
    Where(s.plan = 'pro'),
    Stats {
      aggregates: [
        (converted, SUM(CAST(step_reached >= 2 AS INT))),
        (total,     COUNT(*)),
      ],
      group_by: [],
    },
  ],
})
```

### 11.3 After Planner Lowering (Pre-Optimization)

```
Aggregate
  [converted = SUM(CAST(step_reached >= 2 AS INT)),
   total     = COUNT(*)]
└── Filter(s.plan = 'pro')
    └── SequenceMatch
          pattern: s: signup WHERE s.country = 'US'
                   THEN
                   p: purchase WHERE p.amount > 50
          mode: FIRST
          emit_all: true
          window: 7d
        └── Scan(events, time_range: LAST 30d)
```

At this point the plan is well-typed. `s.plan`, `s.country`, `p.amount`, and `step_reached` are all valid references in their respective scopes. Scan time range is still the user-stated `LAST 30d`.

### 11.4 After Optimizer Passes

**Pass 1** (expression inlining) — no-op, no `LET`.

**Pass 2** (predicate pushdown) — `s.plan = 'pro'` references `plan` from step `s`. This is a **match output column**, not an event-stream column, so it **cannot** be pushed past the MATCH. The filter stays where it is.

**Pass 3** (scan predicate extraction from MATCH):

- Event types: `{'signup', 'purchase'}`
- Step `s` predicate `country = 'US'` lifts to `(event_type != 'signup' OR country = 'US')`.
- Step `p` predicate `amount > 50` lifts to `(event_type != 'purchase' OR amount > 50)`.

Scan predicates become:
```
event_type IN ('signup', 'purchase')
AND (event_type != 'signup' OR country = 'US')
AND (event_type != 'purchase' OR amount > 50)
```

**Pass 4** (projection pruning) — demand walk from top:

- Aggregate needs: `step_reached`
- Filter needs: `s.plan` (step-property demand: step=`s`, column=`plan`)
- MATCH consumes both of those (it produces `step_reached` and forwards the step property)
- MATCH needs from scan: `entity_id`, `ts`, `event_type`, `country` (for predicate), `amount` (for predicate), `plan` (for step-property forwarding)

Scan projected columns: `[entity_id, ts, event_type, country, amount, plan]`.

`SequenceMatch.step_properties` is set to `[(s, plan, String)]`.

**Pass 5** (constant folding) — `'US'`, `50`, `7d`, `30d` all parsed to their nanosecond / typed forms. Scan time range becomes `[now-30d, now+7d]` (extended by 7d window).

**Pass 6** (general fusion detection):

- `SequenceMatch` downstream consumer is `Filter`.
- Past the filter is `Aggregate`.
- Filter references match output columns (`s.plan`) — fusable.
- All aggregates (`SUM(CAST(... AS INT))`, `COUNT(*)`) are incrementally computable.
- Group-by is empty — trivially available.
- No ordering dependencies.

Fusion succeeds. `SequenceMatch.fused_downstream = FilterThenAggregate { filter, aggregate }`. The `Filter` and `Aggregate` nodes are removed from the tree.

Plan after Pass 6:

```
SequenceMatch (fused)
  pattern: s: signup WHERE country = 'US' THEN p: purchase WHERE amount > 50
  mode: FIRST, emit_all: true, window: 7d
  step_properties: [(s, plan, String)]
  fused_downstream: FilterThenAggregate(
    filter: (s.plan = 'pro'),
    aggregate: [converted = SUM(CAST(step_reached >= 2 AS INT)),
                total     = COUNT(*)]
  )
└── Scan(events)
      time_range: [now-30d, now+7d]
      predicates: event_type IN ('signup', 'purchase')
                  AND (event_type != 'signup' OR country = 'US')
                  AND (event_type != 'purchase' OR amount > 50)
      projected_columns: [entity_id, ts, event_type, country, amount, plan]
```

### 11.5 Physical Planning

Demand propagation walks backward from the fused MATCH. Scan demand is already correct from Pass 4.

Strategy selection for MATCH:

- Pattern class: `LinearSimple` (two ordered steps, no negation, no repetition, no `$var` bindings; step names `s:` and `p:` are labels for downstream property forwarding, not variable bindings — per sequence-matching.md §10.1)
- `emit_all = true`
- Fused aggregate is incremental

Strategy: `StepCounter` with layered extraction enabled for step-property forwarding. The physical operator is `SequenceMatchPhysical` with:

```rust
execution_config = MatchExecutionConfig {
    track_match_duration: false,
    track_match_events: false,
    step_properties: [StepPropertyExtraction { step_index: 0, column_name: "plan", bql_type: String }],
    fused_accumulator: Some(Box::new(FilterThenAggAccumulator { .. })),
}
```

The physical tree is:

```
SequenceMatchPhysical {
  strategy: StepCounter,
  fused_aggregate: Some(...),
  fused_filter: Some(...),
}
└── ScanPhysical {
      time_range: [now-30d, now+7d],
      scan_predicates: [...],
      projected_columns: [entity_id, ts, event_type, country, amount, plan],
    }
```

Two physical operators total. No separate Filter or Aggregate operator is emitted.

### 11.6 Execution Summary

At runtime this plan reads six columns from disk (not the full events table), evaluates three scan predicates per row, feeds qualifying events into a step counter, retains the `plan` value at step 1, checks the fused filter at match completion, and increments two counters. No per-entity intermediate rows, no hash aggregation. That is the payoff of fusion and column forwarding.

---

## 12. Error Handling and TypeErrors

All planner errors are variants of `TypeError` (type-system.md §12). The relevant variants:

| Variant                    | When raised                                                    |
| -------------------------- | -------------------------------------------------------------- |
| `ColumnNotFound`           | Referenced column does not exist in the current schema        |
| `TypeMismatch`             | Expression operand types are not compatible                   |
| `SchemaMismatch`           | Operator input schema does not satisfy its requirements       |
| `IncompatibleOperands`     | Operator applied to incompatible operand types                |
| `InvalidAggregateInput`    | Aggregate function applied to an unsupported type             |
| `RegexOnNonString`         | `LIKE` / `MATCH` regex applied to a non-string column         |
| `InvalidSchema`            | `CREATE TABLE` schema fails validation                        |
| `VariableTypeConflict`     | Same `$var` bound to different types across steps              |

Planner errors are **fatal** — the planner returns `Err(TypeError)` to the caller with the source span, and the query does not execute. There is no partial planning, no "warning" mode, and no runtime fallback. Parsers halt on the first syntactic error; planners halt on the first type error.

Error formatting uses the source span to extract the offending fragment and render a diagnostic in the style described in query-language.md §27.

---

## 13. ATTRIBUTE in the Physical Plan

ATTRIBUTE is specified in query-language.md §14.3 (surface syntax), type-system.md §6.14 (output schema), and execution-model.md §2 (execution category). This section records only what the planner needs to know about it.

`ATTRIBUTE` is a stateful entity operator that finds touchpoint events preceding each conversion event within a time window and auto-unnests them into flat rows. The logical plan node is `LogicalPlan::Attribute` (see §5.2); the physical node is `AttributePhysical` (§9.5).

The planner's responsibilities for ATTRIBUTE:

- **Type-check the `touchpoint_key` expression** against the touchpoint event type's schema. The expression must evaluate to `String`; any other type is a `TypeError::TypeMismatch`. The expression cannot reference conversion properties — the planner raises `TypeError::ColumnNotFound` if the expression references a column not on the touchpoint event's schema.
- **Conversion-side demand collection.** Walk downstream and record which conversion properties are referenced (as `<conversion_event>.<column>` expressions). Store them in `forwarded_conversion_columns`. These are retained on the operator's per-entity state at the moment each conversion is consumed and attached to every emitted row for that conversion.
- **Scan predicate extraction.** ATTRIBUTE implies the scan must produce events of types `conversion_event` and `touchpoint_event`. Add `event_type IN (conversion_event, touchpoint_event)` to the scan predicates during Pass 3.
- **Scan column extraction.** Add every column referenced by the `touchpoint_key` expression to the scan's projected columns. Do the same for each forwarded conversion property.
- **Scan time range extension.** Extend the scan's upper bound by the ATTRIBUTE `window` duration so touchpoints preceding a conversion near the user's stated range end are visible.
- **Fusion with downstream aggregate.** The fusion detection pass (§6.9) looks for `Attribute → Aggregate` (or `Attribute → Filter → Aggregate`) chains and fuses them into a single `AttributePhysical` with per-touchpoint accumulation. Because ATTRIBUTE's output is flat and `GROUP BY touchpoint_key` is a common pattern, this is the primary fusion target. See §7.4.4.
- **No UNNEST.** BQL has no separate UNNEST operator. ATTRIBUTE is the only operator that emits "one row per sub-element", and it does so intrinsically — there is no list column to explode downstream. This eliminates an entire class of plan shapes the optimizer would otherwise need to recognize, and removes the `List(Struct)` / `List(Map)` type-system workaround the earlier design required.

---

## 14. Resolved Design Questions

The following open questions from TASK-006 and the design notes are resolved:

| Question                                               | Decision                                                                                           | Rationale                                                                                              |
| ------------------------------------------------------ | -------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| Cost model: rule-based or cost-based?                  | Rule-based only                                                                                    | Structural transformations always beneficial; BQL pipelines are linear, not multi-way join planning   |
| Optimizer pass ordering: fixed or iterative?           | Fixed, single ordered pass                                                                        | No circular interactions between rules; fixpoint iteration not required                               |
| Multi-query optimization?                              | Not in v1                                                                                          | Queries planned independently; aliases provide manual sharing                                         |
| Plan caching across repeated queries?                  | Not in v1                                                                                          | Compilation cost is small relative to execution; BQL queries are typically ad-hoc                     |
| Where does desugaring happen?                          | Planner, not parser                                                                                | Desugaring requires schema access (step counts, column types)                                         |
| Schema validation — separate pass or integrated?       | Integrated into plan construction                                                                  | `LogicalPlan` existence implies validity; eliminates runtime type errors                              |
| Parser output shape: linear or tree?                   | Flat pipeline list                                                                                 | Matches surface syntax; planner converts to tree during lowering                                      |
| All aggregates fusable?                                | Yes — `COUNT`/`SUM`/`MIN`/`MAX`/`AVG` trivially, percentiles via DDSketch (execution-model.md §8.4) | Eliminates partial-fusion combinatorial complexity                                                    |
| Fusion scope?                                          | Stateful operator → (filter →)? aggregate, adjacent only                                           | Simple, predictable; handles the high-value cases                                                     |
| Chains of stateful operators?                          | v1 does not fuse across stateful-operator boundaries; `WITHIN SESSION` is a MATCH-internal special | Keeps fusion detection tractable                                                                      |
| Demand propagation direction?                          | Backward (root → scan)                                                                             | Standard approach; matches execution-model.md §8.2                                                    |
| Statistics from storage?                               | Not used for planning in v1                                                                        | Zone maps are consulted at scan runtime, not at plan time                                             |
| Scan time range extension when MATCH has WINDOW?       | Planner extends upper bound by `max(window, max_bracket)`; entry-step predicates still filter the user's stated range | Ensures matches can complete without changing reported anchor ranges                                  |
| Error surfacing model?                                 | Planner halts on the first `TypeError`; no partial planning                                        | Matches parser-halt-on-first-error in query-language.md §27                                           |
| ATTRIBUTE output shape?                                | Auto-unnest: emit one flat row per `(entity, conversion, matched-touchpoint)` with a single String `touchpoint_key` column | Avoids BQL's type-system gap around `List(Struct)`/`List(Map)` and removes the need for a separate UNNEST operator |
| Expression typing model?                               | Single `type_check` entry point producing `TypedExpr`                                             | Single source of truth; no ad-hoc type inference scattered across the planner                         |

---

## 15. Crate Placement

| Module                              | Crate            | Purpose                                                   |
| ----------------------------------- | ---------------- | --------------------------------------------------------- |
| `Statement`, `Pipeline`, `Expr`     | `bqlite-ast`     | Untyped AST, shared by parser and planner                 |
| Parser implementation               | `bqlite-parser`  | Produces `bqlite-ast` values                              |
| `LogicalPlan`, `TypedExpr`          | `bqlite-planner` | Typed logical plan                                        |
| `type_check(expr, schema)`          | `bqlite-planner` | Expression typing (sole entry point)                      |
| Desugaring (FUNNEL, RETENTION, LET) | `bqlite-planner` | Runs during lowering                                      |
| Optimizer passes 1–7                | `bqlite-planner` | Rule-based structural rewrites                            |
| `DemandSet`                          | `bqlite-planner` | Downstream-needs value carried through backward pass      |
| Physical planner                    | `bqlite-planner` | Strategy selection, fused operator emission               |
| `PhysicalOperator` trait             | `bqlite-operators` | Operators implement this; see execution-model.md §15     |
| Physical operator implementations    | `bqlite-operators` | Per execution-model.md §4                                 |
| `Accumulator` / `HashAccumulator` / `AggState` | `bqlite-operators` | Fused + non-fused aggregation — see execution-model.md §9.4 |
| `MatchExecutionConfig`               | `bqlite-operators` | Layered extraction configuration for MATCH              |
| `ExplainNode` and formatter         | `bqlite-planner` | Stable EXPLAIN output                                     |

`bqlite-planner` depends on `bqlite-ast` and `bqlite-core` only. It does **not** depend on `bqlite-operators` or `bqlite-engine` (see CLAUDE.md "Dependency Direction"). This constrains the shape of the physical plan: the planner emits a **plain-data description** (structs like `SequenceMatchPhysical`, `ScanPhysical`, etc.) with no trait objects. `bqlite-engine` (or `bqlite-operators`, which is below `bqlite-engine` in the dependency order) consumes that description and materializes a tree of `Box<dyn PhysicalOperator>` using concrete operator implementations. The planner never holds a `PhysicalOperator` value directly.

In this model, "physical planning" ends at producing the plain-data tree. The engine's binding step (translating the description into executable operators) is not part of TASK-006.

---

## 16. Summary of Key Design Decisions

| Decision                                    | Rationale                                                                                             |
| ------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| Parser emits a flat pipeline, not a tree   | Linear syntax → linear AST; planner does the tree conversion                                         |
| Desugaring lives in the planner            | Requires schema access; parser knows only syntax                                                      |
| Schema validation is integrated into construction | Eliminates a whole class of runtime type errors; if you have a `LogicalPlan`, it's valid    |
| Rule-based optimizer, fixed 6-pass order   | Sufficient for linear BQL pipelines; no circular rule interactions                                    |
| Fusion targets any stateful operator       | General framework beats pattern-specific special cases                                                |
| All aggregates are fusable                 | DDSketch for percentiles (execution-model.md §8.4); eliminates partial-fusion complexity             |
| Layered extraction for stateful operators  | Avoids combinatorial code paths; branches only at match/session completion, never in per-event loops |
| Column forwarding is demand-driven         | Only retain what downstream actually references; fine-grained per-(step, column) bits                |
| Demand propagation is a backward pass      | Standard, well-understood protocol; aligned with execution-model.md §8.2                             |
| `EXPLAIN pipeline` shows structural decisions | Inspectable optimizer output with no runtime state                                                    |
| ATTRIBUTE auto-unnests touchpoints         | Flat row output; no `List(Struct)` type workaround; no separate UNNEST operator in the language      |
| Credit distribution is user-driven         | Window functions + standard aggregates express last-touch, first-touch, equal, time-decay, position-based attribution |
| No multi-query optimization, no plan caching, no cross-stateful-operator fusion in v1 | Keep the v1 compiler small; revisit based on real-world friction                       |
