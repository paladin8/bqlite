# Logical Plan Node Catalog

**Wave**: 2
**Task**: TASK-204
**Status**: draft — authoritative catalog for the full project; Wave 2
implements the subset marked **[Wave 2 depth]**, later waves implement the
stubs in §5 without renumbering.

## 1. Scope

This note is the authoritative enumeration of every logical plan node the
bqlite planner will ever emit — across every wave — together with each
node's AST source, input / output schema, lowering rules, and the
optimizer rewrites that apply to it. It exists so that:

1. **Wave 2 has an unambiguous contract.** TASK-224 (`LogicalPlan` enum
   + AST → logical lowering) and TASK-226 (physical-plan descriptors +
   logical → physical) can implement directly against this doc without
   re-relitigating node boundaries or schema shapes.
2. **The catalog does not churn across waves.** Later-wave stubs (§5)
   are documented with their eventual signatures so that when
   Wave 3/4/5 agents flesh them out they are extending an existing
   entry, not renaming and renumbering live variants.
3. **Every node has a single source of truth.** planner-pipeline.md §5
   already enumerates the *core relational* and *stateful* variants.
   This doc does not duplicate those definitions — it cross-references
   them and **adds** the DDL / DML / explain variants Wave 2 introduces,
   plus the full AST-to-node lowering map, plus the status-by-wave
   axis that planner-pipeline.md does not track.

What this doc **does not** cover:

- Physical plan descriptors. Those are plain-data mirrors of the logical
  nodes and are specified in planner-pipeline.md §9 and §15; TASK-226 is
  the implementation task. The logical → physical lowering is mechanical
  once the logical catalog is fixed.
- The six optimizer passes. Those live in planner-pipeline.md §6. This
  doc only lists which passes *may rewrite* each node, as a
  cross-reference.
- Expression-level compilation (`TypedExpr` / `CompiledExpr`). That is
  TASK-205 / TASK-225. Per-node fields that carry expressions are typed
  as `TypedExpr` here without further elaboration.

## 2. Relationship to other design docs

| Topic | Authoritative doc | Why it lives there |
|---|---|---|
| Core relational node list (`Scan`, `Filter`, `Project`, `Aggregate`, `Sort`, `Limit`, `Window`, `Pivot`, `SubqueryFilter`) | planner-pipeline.md §5.1 | Wave 0 direction doc; this catalog cross-references it. |
| Stateful/entity-streaming nodes (`SequenceMatch`, `Sessionize`, `EventSelect`, `Sample`, `Attribute`) | planner-pipeline.md §5.2 | Same. |
| `FusedDownstream` and fused-accumulator plumbing | planner-pipeline.md §5.3, §7, §8 | Optimizer pass 6 owns this. |
| Output schema rules per operator | type-system.md §6 | Canonical column names, types, nullability. |
| Desugaring rules (FUNNEL, RETENTION, LET, BETWEEN) | planner-pipeline.md §4.3 | Same table appears there; we reference it. |
| DDL / DML output semantics | query-language.md §20 | CREATE, DROP, ALTER, DESCRIBE, INSERT, DELETE, EXPLAIN surface. |
| AST statement shapes | crates/bqlite-ast/src/statement.rs | `Statement`, `InsertBody`, `AlterAction`, etc. |
| AST pipeline-stage shapes | crates/bqlite-ast/src/operator.rs | `PipelineStage` variants the Wave 2 parser produces. |

Where this doc and planner-pipeline.md overlap, planner-pipeline.md is the
canonical spec for node **fields**. This doc is canonical for **status**
(which wave implements which node), **AST source** (exactly which AST
construct lowers to each node), and **DDL / DML** (nodes
planner-pipeline.md §5 does not yet enumerate).

### 2.1 Corrections to planner-pipeline.md §5

One field-level drift that the reader must know about:

- **`Scan.table` and `Scan.joined_tables` are `TableSchema`, not
  `TableRef`.** planner-pipeline.md §5.1 sketched these fields as
  `table: TableRef` and `joined_tables: Vec<TableRef>` in pseudo-code,
  but `TableRef` is the parser-side AST shape — a bare name with a
  span — and the planner only keeps unresolved `TableRef` values
  *before* the first catalog lookup. Post-lowering, every plan node
  holds *resolved* `TableSchema` values so downstream passes never
  need to re-hit the catalog to recover column types. The Wave 1 stub
  at `crates/bqlite-planner/src/lib.rs:77` already uses `TableSchema`
  and this doc formalizes that shape. The same rule applies to
  `Insert.table` (§4.9). Treat this doc as authoritative on this
  point; a follow-up edit to planner-pipeline.md §5.1 aligning the
  wording is filed as a Wave 2 doc-drift fix (a one-paragraph edit;
  no task number needed).

## 3. Catalog overview

Status markers:

- **[Wave 2 depth]** — full shape locked; TASK-224 / TASK-226 implement now.
- **[Wave 3]** — shape locked per planner-pipeline.md §5; Wave 3 tasks implement (MATCH, aggregate, sort).
- **[Wave 4]** — shape locked per planner-pipeline.md §5; Wave 4 tasks implement (sessionize, retention, attribution, cohorts, deletes).
- **[later]** — shape still in flux; placeholder entry.

| Node | Kind | Wave | AST source | §  |
|---|---|---|---|---|
| `Scan` | relational | **[Wave 2 depth]** | `Pipeline.source` (or bare `Statement::Query(Pipeline { source, stages: [] })`) | §4.1 |
| `Filter` | relational | **[Wave 2 depth]** | `PipelineStage::Where` | §4.2 |
| `Project` | relational | **[Wave 2 depth]** | `PipelineStage::Select` | §4.3 |
| `Limit` | relational | **[Wave 2 depth]** | `PipelineStage::Limit` | §4.4 |
| `CreateTable` | DDL | **[Wave 2 depth]** | `Statement::CreateTable` | §4.5 |
| `DropTable` | DDL | **[Wave 2 depth]** | `Statement::DropTable` | §4.6 |
| `AlterTableAddColumn` | DDL | **[Wave 2 depth]** | `Statement::AlterTable` with `AlterAction::AddColumn` | §4.7 |
| `Describe` | DDL | **[Wave 2 depth]** | `Statement::Describe` | §4.8 |
| `Insert` | DML | **[Wave 2 depth]** | `Statement::Insert` (both `Values` and `From` bodies) | §4.9 |
| `Explain` | meta | **[Wave 2 depth]** | `Statement::Explain(Pipeline)` | §4.10 |
| `SequenceMatch` | stateful | **[Wave 3]** | `PipelineStage::Match` (+ desugared `Funnel` / `Retention`) | §5.1 |
| `Aggregate` | relational | **[Wave 3]** | `PipelineStage::Stats` (+ desugared funnel aggregates) | §5.1 |
| `Sort` | relational | **[Wave 3]** | `PipelineStage::OrderBy` | §5.1 |
| `Distinct` | relational | **[Wave 3]** | `PipelineStage::Select { distinct: true }` | §5.1 |
| `Sessionize` | stateful | **[Wave 4]** | `PipelineStage::Sessionize` | §5.2 |
| `EventSelect` | stateful | **[Wave 4]** | `PipelineStage::FirstLastNth` | §5.2 |
| `Attribute` | stateful | **[Wave 4]** | `PipelineStage::Attribute` | §5.2 |
| `SubqueryFilter` (Cohort) | relational | **[Wave 4]** | `IN QUERY <alias>` / `IN (subquery)` inside WHERE | §5.2 |
| `Delete` | DML | **[Wave 4]** | `Statement::Delete` — rejected in Wave 2 at both parser and lowering; requires tombstones (TASK-404) | §5.2 |
| `Sample` | stateful | **[Wave 4]** | `PipelineStage::Sample` | §5.2 |
| `Window` | relational | **[later]** | window `OVER(...)` inside `SELECT` / `LET` | §5.3 |
| `Pivot` | relational | **[later]** | `PipelineStage::Pivot` | §5.3 |
| `FusedDownstream` annotation | not a node | **[Wave 5]** | optimizer pass 6; see planner-pipeline.md §5.3 | §5.3 |

The shape of every **[Wave 2 depth]** row is locked by this doc.
Everything else defers its Rust-level definition to the wave that
implements it; this table exists so agents can see the full target
surface and avoid building Wave-2 abstractions that won't scale.

## 4. Wave 2 depth nodes

Every Wave 2 node carries a cached `output_schema: OperatorSchema`
computed once at construction time per planner-pipeline.md §4.5. The
Rust shape below shows the schema field explicitly, but node builders
(see §7) are responsible for populating it — *construction of an
ill-schemed `LogicalPlan` value is impossible*.

### 4.1 Scan

```rust
LogicalPlan::Scan {
    table: TableSchema,                     // catalog-resolved
    time_range: Option<TimeRange>,          // from `table LAST 30d` etc.
    joined_tables: Vec<TableSchema>,        // empty in Wave 2; Wave 4 joins fill this
    scan_predicates: Vec<TypedExpr>,        // populated by optimizer pass 3/4
    projected_columns: Vec<ColumnId>,       // populated by optimizer pass 4
    output_schema: OperatorSchema,
}
```

**AST source.** Every statement that reads rows — including
`Statement::Query(Pipeline)`, `Statement::Explain(Pipeline)`, and the
inner pipeline of later-wave features — lowers its `Pipeline.source` to
a `Scan`. A bare `Statement::Query(Pipeline { source, stages: [] })`
(the Wave 1 smoke-test shape) lowers to a single `Scan` with no parent
nodes. Unknown `source.primary.name` is a `TypeError` raised at lowering
time (planner-pipeline.md §4.1). `Scan.time_range` is populated from
`Pipeline.source.time_range` (the `LAST <duration>` / `BETWEEN … AND …`
form declared on the `Source`, not on the `Pipeline` directly —
`crates/bqlite-ast/src/pipeline.rs`).

**Output schema.** `OperatorSchema::from_table(&table)` — i.e. the
table's declared columns followed by the implicit `__seq_id` /
`__batch_id` system columns. This is exactly the shape TASK-115 already
materializes in the Wave 1 stub.

**Optimizer rewrites.** Passes 3 and 4 populate `scan_predicates` and
`projected_columns`; neither mutates `output_schema`. Pass 4 may
reorder declared columns in `projected_columns` so the scan can skip
column-chunk decode for unused columns.

**Wave 2 surface.** `time_range` and `joined_tables` exist in the
struct but are always `None` / empty in Wave 2 — the parser does not yet
produce `table LAST <duration>` or `JOIN <table>`. They live here so
Wave 4's time-range extension (planner-pipeline.md §4.4) and join work
extend an existing field rather than adding a variant.

### 4.2 Filter

```rust
LogicalPlan::Filter {
    predicate: TypedExpr,                    // Boolean, three-valued
    input: Box<LogicalPlan>,
    output_schema: OperatorSchema,           // identical to input's
}
```

**AST source.** `PipelineStage::Where { predicate, .. }`. Lowering
wraps the accumulated plan tree so far in `Filter` and type-checks
`predicate` against the input's output schema.

**Output schema.** Identical to `input.output_schema()`. Filter never
changes the column shape, only row count.

**Optimizer rewrites.** Pass 2 (predicate pushdown) may move conjuncts
of `predicate` down into the scan's `scan_predicates`, possibly leaving
an empty residual. Pass 5 (constant folding) may simplify `predicate`
to `true` (elided entirely) or `false` (plan collapses to an empty
result). A filter with an empty residual is removed by TASK-227's
pushdown pass before physical lowering.

**Null semantics.** The predicate is evaluated under three-valued
logic per type-system.md §4: rows whose predicate evaluates to `NULL`
are dropped (same as `FALSE`). TASK-225's `CompiledExpr` is responsible
for implementing this.

### 4.3 Project

```rust
LogicalPlan::Project {
    expressions: Vec<ProjectItem>,           // ordered, each carries output name
    input: Box<LogicalPlan>,
    output_schema: OperatorSchema,
}

pub struct ProjectItem {
    pub expr: TypedExpr,                     // schema-resolved
    pub output_name: String,                 // planner-assigned if not user-supplied
}
```

**AST source.** `PipelineStage::Select { distinct, items, .. }` — the
`distinct: true` case in Wave 3 grows into `Distinct(Project(...))`
(§5.1). Wave 2 rejects `distinct: true` at lowering.

Each `items` entry is a
`SelectItem { kind: SelectItemKind, alias: Option<Name>, span }` from
`crates/bqlite-ast/src/operator.rs`. Lowering dispatches on `kind`:

- `SelectItemKind::Expr(Spanned<Expr>)` with an `alias` →
  `ProjectItem { expr: TypedExpr::from(expr), output_name: alias.text }`.
  The parser has already enforced "explicit alias required when the
  expression is anything other than a bare column reference"
  (query-language.md §10), so a missing alias on a non-column
  expression never reaches the planner.
- `SelectItemKind::Expr(Spanned<Expr>)` with `alias = None` and the
  expression is a bare column reference →
  `ProjectItem { expr, output_name: <column name> }`.
- `SelectItemKind::Wildcard` → expand to one `ProjectItem` per column
  in the input's schema, preserving order. The wildcard MUST be the
  sole item in `items` (query-language.md §10); mixing `*` with other
  items is a planner `TypeError`.
- `SelectItemKind::QualifiedWildcard(Name)` → deferred to Wave 4
  joins; Wave 2 lowering rejects this with `TypeError::Unsupported`.

**Output schema.** Built from `expressions` in order. Each item's
output type comes from `TypedExpr::bql_type()`. Duplicate output names
are a `TypeError` (matching `OperatorSchema::new()`'s existing check).

**Optimizer rewrites.** Pass 4 (projection pruning) walks demand
bottom-up and may trim unused `ProjectItem`s whose outputs are not
referenced downstream. Pass 1 (expression inlining) applies when a
later stage references an alias introduced by this `Project` — e.g.
`SELECT amount * 1.1 AS total | WHERE total > 100` lowers to
`Filter(total > 100) └── Project(total = amount*1.1) └── Scan`, and
Pass 1 inlines `amount*1.1` into the filter's predicate so the
optimizer can then try to push the rewritten filter through the
project into the scan (Pass 2). Note that in the common pipeline
shape `WHERE ... | SELECT ...` the fold order produces
`Project └── Filter └── Scan` and Pass 1 has nothing to do — Pass 2
still does the heavy lifting by pushing the already-below filter
further into the scan.

### 4.4 Limit

```rust
LogicalPlan::Limit {
    count: u64,
    input: Box<LogicalPlan>,
    output_schema: OperatorSchema,           // identical to input's
}
```

**AST source.** `PipelineStage::Limit { count, .. }`. The parser
already enforces that `count` is a non-negative integer literal and
that `LIMIT` is not the first stage in a pipeline (query-language.md
§15).

**Output schema.** Identical to `input.output_schema()`.

**Optimizer rewrites.** No Wave 2 pass mutates `Limit`. Wave 5 may
push `Limit` into `Project` and (where legal) further into sorted
scans. Wave 2 preserves the node verbatim.

**CLI interaction.** The `bqlite query` CLI may auto-inject a
`LIMIT 1000` at the CLI boundary when the user's query has no explicit
limit (TASK-234). The injection happens in the CLI, not in the
planner, so the planner never sees an injected limit distinct from a
user-supplied one.

### 4.5 CreateTable

```rust
LogicalPlan::CreateTable {
    name: String,
    columns: Vec<ColumnDef>,                 // from bqlite-core, ordered
    entity_key: String,                      // column name
    event_time: String,                      // column name
    event_type: String,                      // column name
    output_schema: OperatorSchema,           // empty (0 columns)
}
```

**AST source.** `Statement::CreateTable(CreateTableStmt)`. Lowering
validates the §5.1 schema-creation rules — exactly one column per
required role, all NOT NULL where required, no system-column-prefixed
names, unique names — **at plan time**, not at engine bind time. This
means `TableSchema::new()` is the authoritative validator; the logical
node holds the validated pieces (`columns`, `entity_key`, `event_time`,
`event_type`) as a destructured record rather than a `TableSchema`
value, because the table does not yet exist in the catalog and
`TableSchema` requires a catalog identity. TASK-232's engine bind
reconstructs the `TableSchema` by calling `TableSchema::new(name,
columns, &entity_key, &event_time, &event_type)` and atomically
registers it via the manifest API (TASK-217).

**Output schema.** Empty — zero columns. DDL statements produce no
rows. The engine returns an empty result batch; the CLI prints a
one-line status message.

**Errors raised at plan time.**
- `TypeError::DuplicateTable` — a table with `name` already exists in
  the catalog. (The catalog lookup uses the same `&dyn Catalog` handle
  TASK-125 resolved `events` through.)
- `TypeError::Schema` — any §5.1 validation failure from
  `TableSchema::new`. The error surface is narrow because v0 DDL has
  no expressions in column defaults (only literals).

### 4.6 DropTable

```rust
LogicalPlan::DropTable {
    name: String,
    output_schema: OperatorSchema,           // empty
}
```

**AST source.** `Statement::DropTable(DropTableStmt)`. The grammar has
no `IF EXISTS` modifier (query-language.md §20.4); dropping a
non-existent table is a plan-time error.

**Output schema.** Empty.

**Errors raised at plan time.** `TypeError::UnknownTable { name }` if
the catalog has no entry for `name`. The Wave 1 bootstrap `events`
table (TASK-125) is retired by TASK-240 before Wave 2 acceptance, so
dropping it is permitted in Wave 2.

**Segment reaping.** The logical node carries only the name; the
engine-side DROP implementation (TASK-232) removes the table entry and
the associated segment inventory from the manifest atomically. The
on-disk segment files are reaped by TASK-239's startup orphan-cleanup
pass on the next database open — the logical node does not need to
track file paths.

### 4.7 AlterTableAddColumn

```rust
LogicalPlan::AlterTableAddColumn {
    name: String,
    column: ColumnDef,                       // the new column, already validated
    output_schema: OperatorSchema,           // empty
}
```

**AST source.** `Statement::AlterTable(AlterTableStmt { action:
AlterAction::AddColumn(ColumnDef), .. })`. `AlterAction` is an enum to
leave room for future ALTER variants without churning `Statement`.

**Output schema.** Empty.

**Errors raised at plan time.**
- `TypeError::UnknownTable { name }`.
- `TypeError::Schema` — duplicate column name against the existing
  schema.
- `TypeError::Schema` — the new column declares a role
  (`ENTITY KEY` / `EVENT TIME` / `EVENT TYPE`). Roles are frozen at
  `CREATE TABLE`; only `Regular` columns can be added.
- `TypeError::Schema` — the new column is `NOT NULL` without a
  `DEFAULT`. Existing rows would read as NULL for the added column,
  which violates the constraint. This matches type-system.md §5.3's
  schema-evolution rules.

**Runtime behavior.** The engine (TASK-232) appends the new `ColumnDef`
to the existing `TableSchema`, bumps `TableSchema::version`, and
atomically writes the manifest. No segment rewrite is needed because
reads project against the per-segment schema snapshot and new columns
read as NULL (or the column's `DEFAULT`) for existing rows. This is a
deliberately metadata-only operation — the logical node captures that
guarantee by holding only the new `ColumnDef`, never the full table.

### 4.8 Describe

```rust
LogicalPlan::Describe {
    name: String,
    output_schema: OperatorSchema,           // fixed four-column schema below
}
```

**AST source.** `Statement::Describe(DescribeStmt)`.

**Output schema.** Fixed, four columns, matching query-language.md
§20.5:

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `name` | `String` | no | Column name, in DDL order. |
| `type` | `String` | no | `BqlType` formatted via `Display` (`"string"`, `"int"`, …). |
| `nullable` | `Bool` | no | `true` if the column is nullable. |
| `role` | `String` | no | One of `entity_key`, `event_time`, `event_type`, `property`, `system`. |

System columns (`__seq_id`, `__batch_id`) are included in the output
rows — users need to discover them for `DELETE` (query-language.md
§20.2) and explicit `SELECT` targeting. The `role` column distinguishes
them from user-declared property columns.

**Errors raised at plan time.** `TypeError::UnknownTable { name }`.

**Runtime behavior.** Metadata-only; no segment read. TASK-232's
`DescribePhysical` implementation looks up the resolved `TableSchema`
and formats its `logical_columns()` iterator as the four-column batch.

### 4.9 Insert

```rust
LogicalPlan::Insert {
    table: TableSchema,                      // catalog-resolved
    body: InsertLogicalBody,
    output_schema: OperatorSchema,           // empty
}

pub enum InsertLogicalBody {
    /// Literal `VALUES (...)` tuples. Each row is arity-checked and
    /// type-coerced against `table` at plan time (TASK-238).
    Values(Vec<Vec<PropertyValue>>),
    /// `FROM <path> WITH (...)` bulk load.
    ///
    /// The resolved ingest descriptor carries:
    /// - the format (explicit `format: 'csv'` or inferred from path),
    /// - the parsed option list (delimiter, header, ...),
    /// - the column-rename map after type-checking against `table` —
    ///   every `target` name in `map` must exist in `table`, and
    ///   duplicate targets within a single `map` are rejected.
    /// - the list of unmapped source-column passthroughs implied by
    ///   the map (TASK-226 does not resolve actual source columns;
    ///   the engine (TASK-233) does that once it opens the file
    ///   against the live CSV schema).
    From(InsertFromDescriptor),
}

pub struct InsertFromDescriptor {
    pub path: String,
    pub format: IngestFormat,                // Csv, JsonL, Parquet
    pub options: Vec<(String, PropertyValue)>, // flat literal options
    pub column_map: Vec<(String, String)>,   // (source, target); empty if no map
}

pub enum IngestFormat { Csv, JsonL, Parquet }
```

**AST source.** `Statement::Insert(InsertStmt)`. The two `InsertBody`
variants lower to the two `InsertLogicalBody` variants one-for-one.

**Why the logical body is distinct from the AST body.** The AST
carries `map: Option<Vec<ColumnMapping>>` (TASK-237) because the
parser can't validate target names without a catalog. Lowering is the
first place a catalog exists, so it is where type-checking happens.
The logical body stores:

- `Values` as `Vec<Vec<PropertyValue>>` — each literal is coerced to
  the target column's `BqlType` using type-system.md §3 coercion
  rules. Row arity is checked against `table.columns().len()`. NOT
  NULL violations and type mismatches are plan-time errors naming the
  offending row index.
- `From` as `InsertFromDescriptor` — the flat option list is
  normalized (unknown keys → error, `format` key resolved, `delimiter`
  validated as single-character string), and `column_map` is resolved
  against the target table schema (duplicate targets → error;
  unknown targets → error). *Source* column resolution is deferred to
  the engine because the source file is not read at plan time.

**Output schema.** Empty.

**Errors raised at plan time.**
- `TypeError::UnknownTable { name }`.
- `TypeError::Arity` — `VALUES` row arity mismatch.
- `TypeError::CoercionFailed` — a literal in `VALUES` cannot be
  coerced to the target column's type.
- `TypeError::NotNullViolation` — a `NULL` literal for a NOT NULL
  column.
- `TypeError::UnknownOption` — unknown key inside the `WITH (...)`
  list for `FROM`.
- `TypeError::UnknownColumn` — the `map` clause names a target column
  that does not exist on `table`.
- `TypeError::DuplicateMapping` — two entries in the `map` clause
  share a target.

**What is *not* checked at plan time.** Source-file existence, CSV
header matching, source-column types, and row-level type mismatches
coming out of CSV parsing are all engine-time errors (TASK-233),
because they need file I/O that the planner cannot do.

**Wave 2 format support.** `IngestFormat` enumerates all three
formats for forward-compat, but Wave 2 only implements `Csv`
(TASK-233). Wave 2 lowering rejects `JsonL` and `Parquet` with
`TypeError::Unsupported` naming the `format: '...'` option's span.
`IngestFormat::JsonL` and `IngestFormat::Parquet` land in Wave 4
(TASK-410); the logical enum is deliberately full-surface so the
Wave 4 work is a pure engine / ingest extension rather than another
planner change.

### 4.10 Explain

```rust
LogicalPlan::Explain {
    plan: Box<LogicalPlan>,                  // the pipeline being explained
    output_schema: OperatorSchema,           // fixed one-column schema below
}
```

**AST source.** `Statement::Explain(Pipeline)`. The wrapper only
accepts pipelines, not DDL / DML (query-language.md §20.6). Lowering
recurses into the pipeline, invokes the full lowering path (so
`EXPLAIN` of an ill-typed query still raises the underlying
`TypeError` — EXPLAIN does not hide errors), and stores the resulting
logical plan as `plan`.

**Output schema.** Fixed, one column:

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `plan` | `String` | no | One line per `ExplainNode`, indented to show tree depth. |

**Wave 2 narrowing of query-language.md §20.6.** The language doc
says EXPLAIN "shows the logical plan, optimized plan, and physical
plan" and that "the planner emits structured plan data that the REPL
formats". Wave 2 **narrows** this to a single-column text batch: one
row per `ExplainNode` in the final optimized plan, indented by depth,
with optimizer-pass annotations inlined (e.g. `Filter [pushed]`). The
structured-data EXPLAIN surface — where the REPL receives a tree of
plan snapshots and formats them with Unicode tree characters — is
deferred to Wave 6's REPL work (TASK-602). Wave 2's one-column text
shape is the minimum viable contract the acceptance test
(`EXPLAIN purchases | where …`) needs to assert on, and it avoids
building a second output format that would immediately be replaced.

**Optimizer interaction.** EXPLAIN is the only node where the
optimizer runs *through* the child plan but also *captures*
intermediate state: TASK-229's `ExplainNode` builder snapshots the
logical plan after each optimizer pass so the output can show "before
pushdown → after pushdown → after pruning → …". The `Explain` logical
node holds the final optimized plan; the intermediate snapshots are
attached via a side-channel map populated during optimization (see
TASK-229 for the exact shape). Wave 2 renders a linearized form of
this structure as a single-column batch; later waves can add a
structured output mode without changing the `Explain` node itself.

**Output format.** Text only in Wave 2 — no Unicode polish, no JSON
mode. The formatter is TASK-229's responsibility.

## 5. Future-wave stubs

Each subsection lists the nodes whose shape is already frozen (usually
in planner-pipeline.md §5) but whose implementation lands in a later
wave. The entries are deliberately terse — full detail lives in the
owning wave's design notes.

### 5.1 Wave 3 — Pattern Matching MVP

- `SequenceMatch { pattern, mode, emit_all, window, brackets, step_properties, fused_downstream, input, output_schema }`
  — shape per planner-pipeline.md §5.2. AST source:
  `PipelineStage::Match`, plus `PipelineStage::Funnel` and
  `PipelineStage::Retention` after desugaring (§6). Wave 3 design
  detail: TASK-301 / TASK-302.
- `Aggregate { aggregates, group_by, input, output_schema }` — shape
  per planner-pipeline.md §5.1. AST source: `PipelineStage::Stats`,
  plus desugared funnel / retention aggregates. Wave 3 implementation:
  TASK-307.
- `Sort { keys, input, output_schema }` — shape per planner-pipeline.md
  §5.1. AST source: `PipelineStage::OrderBy`. Implemented alongside
  `Limit` in Wave 3 so `ORDER BY … LIMIT N` fits the expected
  pattern.
- `Distinct { input, output_schema }` — new relational node that
  collapses duplicate rows. AST source: `PipelineStage::Select {
  distinct: true, .. }` after the `Select` has been lowered to
  `Project`. Introduced alongside Wave 3's aggregate work because
  `DISTINCT` shares most of its implementation with a group-by-all
  aggregate.

### 5.2 Wave 4 — Advanced Analytics

- `Sessionize { gap, end_event, forwarded_columns, fused_downstream,
  input, output_schema }` — planner-pipeline.md §5.2. AST source:
  `PipelineStage::Sessionize`. Design: TASK-405.
- `EventSelect { kind, event_type, predicate, forwarded_columns,
  fused_downstream, input, output_schema }` — planner-pipeline.md
  §5.2. AST source: `PipelineStage::FirstLastNth`.
- `Attribute { conversion_event, touchpoint_event, window,
  touchpoint_key, forwarded_conversion_columns, fused_downstream,
  input, output_schema }` — planner-pipeline.md §5.2. AST source:
  `PipelineStage::Attribute`. Design: TASK-406.
- `SubqueryFilter { column, subquery, input, output_schema }` —
  planner-pipeline.md §5.1. AST source: `WHERE col IN QUERY <alias>`
  and `WHERE col IN (<subquery>)`. Design: TASK-407 (cohorts).
- `Delete { table, predicate, output_schema (empty) }` — AST source:
  `Statement::Delete`. Pairs with Wave 4's tombstone work
  (TASK-404). Wave 2 rejects `DELETE` at the parser boundary, not at
  lowering — query-language.md §20.2 defers it to Wave 4.
- `Sample { spec, input, output_schema }` — planner-pipeline.md §5.2.
  AST source: `PipelineStage::Sample`.

### 5.3 Later waves

- `Window { function, partition_by, order_by, input, output_schema }`
  — planner-pipeline.md §5.1. AST source: `OVER(...)` clauses inside
  `SELECT` / `LET`. No dedicated pipeline stage; Window enters the
  plan as a child of `Project` during Pass 1 expression inlining.
- `Pivot { column, on, input, output_schema }` — planner-pipeline.md
  §5.1. AST source: `PipelineStage::Pivot`. Pivot's output schema is
  data-dependent (one column per distinct pivot value), so it is the
  only relational node whose `output_schema` cannot be computed at
  construction time without reading the catalog for the declared
  `IN (...)` list.
- `FusedDownstream` **annotation** — planner-pipeline.md §5.3 and §7.
  Not a node in its own right; it is an optional field on
  `SequenceMatch`, `Sessionize`, `EventSelect`, and `Attribute` that
  the Wave 5 fusion pass populates. Wave 2 carries the field as
  `None` on every stateful node it creates — which in Wave 2 is
  none, because stateful nodes are Wave 3+.

## 6. Desugaring rules

The desugaring table is authoritatively maintained in
planner-pipeline.md §4.3; this section only records the **landing
surface** each sugar rewrites into, so consumers of this catalog can
see which logical nodes receive desugared traffic:

| Sugar AST form | Rewrites to | Wave |
|---|---|---|
| `PipelineStage::Funnel(FunnelArgs)` | `SequenceMatch(FIRST) → Aggregate` | Wave 3 |
| `PipelineStage::Retention(RetentionArgs)` | `SequenceMatch(FIRST, brackets) → Aggregate` | Wave 4 |
| `PipelineStage::Let { name, expr }` | `Project(*, expr AS name)` | Wave 3 (Wave 2 parser does not emit `Let`; Wave 3 adds the production and this desugaring rule) |
| `x BETWEEN a AND b` (inside an `Expr`) | `(x >= a) AND (x <= b)` | Wave 2 — expression-level sugar, not a node rewrite |

Desugaring runs **during** lowering, not in a separate pass, because
it needs schema access (to fill in per-step aggregate counts, for
example). Wave 2's only desugaring is the expression-level `BETWEEN`
rewrite, handled inside TASK-225's expression compiler.

## 7. AST → LogicalPlan lowering map

This map is the single source of truth for TASK-224's lowering
function. Every `Statement` variant produces exactly one root
`LogicalPlan` node; pipeline stages are folded left-to-right into the
accumulated tree per planner-pipeline.md §4.2.

### 7.1 Statement-level

| AST statement | Root logical node | Notes |
|---|---|---|
| `Statement::Query(Pipeline)` | innermost: `Scan` built from `Pipeline.source`; outer: stages folded in order | see §7.2 |
| `Statement::Explain(Pipeline)` | `Explain { plan: <lowered pipeline>, .. }` | lowered child inherits all Query rules |
| `Statement::CreateTable(..)` | `CreateTable` | |
| `Statement::DropTable(..)` | `DropTable` | |
| `Statement::AlterTable(AlterTableStmt { action: AlterAction::AddColumn(col), .. })` | `AlterTableAddColumn { column: col, .. }` | |
| `Statement::Describe(..)` | `Describe` | |
| `Statement::Insert(InsertStmt { body: InsertBody::Values(..), .. })` | `Insert { body: InsertLogicalBody::Values(..), .. }` | literals coerced here |
| `Statement::Insert(InsertStmt { body: InsertBody::From { .. }, .. })` | `Insert { body: InsertLogicalBody::From(InsertFromDescriptor { .. }), .. }` | `map` resolved against target schema |
| `Statement::Delete(..)` | **rejected at lowering in Wave 2** | Wave 2 parser (TASK-221) does not emit the production per the wave's Scope Exclusions; kept as a rejection arm for forward-compat. Wave 4 adds the `Delete` logical node alongside tombstones (TASK-404, TASK-410 territory). |
| `Statement::DefineAlias { .. }` | **rejected at lowering in Wave 2** | Wave 2 parser does not emit this production either; Wave 4 alias resolution is owned by TASK-407. |

### 7.2 Pipeline-stage fold (Wave 2 subset)

Starting from `acc = Scan(source)`, each stage folds into `acc`:

| AST `PipelineStage` | Wave 2 fold | Wave |
|---|---|---|
| `Where { predicate, .. }` | `acc = Filter { predicate, input: acc, output_schema: acc.output_schema }` | 2 |
| `Select { distinct: false, items, .. }` | `acc = Project { expressions: items.map(typed), input: acc, output_schema: … }` | 2 |
| `Select { distinct: true, .. }` | **rejected** — Wave 3 adds `Distinct` | 3 |
| `Limit { count, .. }` | `acc = Limit { count, input: acc, output_schema: acc.output_schema }` | 2 |
| `Match { .. }` | **rejected** — Wave 3 adds `SequenceMatch` | 3 |
| `Funnel(..)` / `Retention(..)` | **rejected** — Wave 3/4 desugar | 3/4 |
| `Stats { .. }` | **rejected** — Wave 3 adds `Aggregate` | 3 |
| `OrderBy { .. }` | **rejected** — Wave 3 adds `Sort` | 3 |
| `Sessionize(..)` / `Sample(..)` / `Pivot { .. }` / `FirstLastNth(..)` / `Attribute(..)` / `Let { .. }` | **rejected** — later waves | 3-4 |

**Rejection shape.** Every rejection emits a `TypeError::Unsupported`
that names the source span of the offending stage and says *"X is not
yet supported in Wave 2"* so the parser / planner stay forward-friendly
without accidentally hiding real bugs. The error text is extended in
later waves as stages come online; the error **variant** stays
constant, so code that matches on `Unsupported` keeps compiling as
Wave 3/4 tasks land.

## 8. Schema computation summary

For the Wave 2 depth subset, every `output_schema` is fixed or trivial
to compute. This table mirrors planner-pipeline.md §5.4 and is
repeated here so TASK-224's test matrix has a single list to cover:

| Node | Output schema rule |
|---|---|
| `Scan` | `OperatorSchema::from_table(&table)` |
| `Filter` | identical to `input.output_schema()` |
| `Project` | built from `expressions`; names come from `ProjectItem::output_name`, types from `TypedExpr::bql_type()` |
| `Limit` | identical to `input.output_schema()` |
| `CreateTable` | empty — `OperatorSchema::new(vec![]).unwrap()` |
| `DropTable` | empty |
| `AlterTableAddColumn` | empty |
| `Describe` | fixed `(name, type, nullable, role)` — see §4.8 |
| `Insert` | empty |
| `Explain` | fixed single-column `(plan: String)` — see §4.10 |

The "empty" schema is a single canonical value — the planner
constructs one `EMPTY_OPERATOR_SCHEMA: OperatorSchema` lazily and
clones it into each empty-output node so every DDL / DML variant
shares object identity where possible. (This is a minor perf /
ergonomics choice; TASK-224 is free to materialize it lazily.)

## 9. Naming and casing decisions

Several small naming decisions that would otherwise leak into the
TASK-224 review:

- **`LogicalPlan` is the enum name.** Not `LogicalNode`, not `Plan`.
  This matches the existing Wave 1 stub in
  `crates/bqlite-planner/src/lib.rs` and planner-pipeline.md §5.
- **DDL variants use `PascalCase` with no redundant suffix.**
  `CreateTable`, not `CreateTableNode`. Matches `Scan`, `Filter`, etc.
- **`AlterTableAddColumn` over `Alter { action: AddColumn }`.**
  Flattening to one variant per concrete action keeps pattern matches
  unambiguous in Wave 2. If Wave 3+ introduces additional ALTER
  actions (`DropColumn`, `RenameColumn`), each gets its own variant
  rather than a nested enum. The AST's `AlterAction` enum absorbs the
  parser-side flexibility; the logical plan is concrete.
- **`Insert` carries a `body: InsertLogicalBody` enum, not two
  distinct variants (`InsertValues`, `InsertFrom`).** This preserves
  the "one plan root per statement" invariant while letting the engine
  bind step dispatch on the body variant. The logical enum *could*
  flatten these the way `AlterTable` flattens, but `Insert` has more
  shared fields (the resolved `table: TableSchema`) and the
  sub-variants are more likely to grow additional shapes in Wave 4
  (e.g. `Insert … SELECT`).
- **`Explain` takes a `Box<LogicalPlan>` child, not a raw `Pipeline`.**
  Storing the *lowered* plan lets EXPLAIN run optimizer passes on the
  child and reflect the optimized shape in its output. An alternative
  considered — storing the raw `Pipeline` and re-lowering when the
  engine binds — would produce different error behavior for ill-typed
  pipelines (a bare `Query` raises the error immediately; an
  `EXPLAIN` wrapper would defer it to bind time). Storing the lowered
  plan makes `EXPLAIN` fail-fast on type errors, which matches the
  user expectation that `EXPLAIN bad-query` is just as loud as
  `bad-query`.

## 10. Open questions deferred to later waves

Recorded here so they don't get lost between this doc and the tasks
that will resolve them:

1. **How does `Describe` handle bootstrap vs real tables?** Wave 1's
   bootstrap `events` table (TASK-125) carries
   `bootstrap_events_table: true` in the manifest. TASK-240 retires
   the bootstrap shortcut, so after Wave 2 every describable table is
   user-declared. The Wave 2 `Describe` node does not need a
   bootstrap-specific code path.
2. **Should `Insert { body: Values(...) }` lower to a `Scan(VALUES)`
   source-side node?** Some SQL systems model `INSERT ... VALUES` as
   `Insert { source: Scan(VALUES list) }` to unify the two insert
   bodies under a common "table of rows" child. Wave 2 does not —
   the literal list is carried directly in `InsertLogicalBody::Values`.
   If Wave 4 adds `INSERT … SELECT <query>`, the right refactor at
   that point is to add a third `InsertLogicalBody::Query(Box<LogicalPlan>)`
   variant rather than unify everything under a `VALUES` scan. This
   is recorded so the Wave 4 planner change is contained.
3. **How does `Explain` capture per-pass snapshots?** TASK-229 owns
   the `ExplainNode` builder and the capture protocol. This doc
   reserves the `plan` field for the *final* optimized plan; the
   intermediate snapshots live on a side-channel map owned by the
   optimizer driver. The shape of that side channel is TASK-229's
   call, not this doc's.

---

This catalog is the contract the Wave 2 planner work implements against.
If TASK-224 or TASK-226 uncovers a case where this doc is wrong or
insufficient, the fix is a one-paragraph edit here (in the same
checkpoint as the code change), not a fresh design task.
