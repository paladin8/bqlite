# Wave 3 Logical Lowering + Demand Propagation

**Wave**: 3
**Task**: TASK-309
**Status**: draft
**Depends on**: TASK-301 (match-operator.md), TASK-308 (aggregate-operator.md)
**Depended on by**: TASK-317 (plan variants), TASK-318 (implementation)

---

## 1. Purpose

This document is the spec-level design for the AST-to-logical-plan lowering
that TASK-318 implements, plus the formal `DemandSet` type and the backward
demand propagation algorithm that drives column pruning, step-property
forwarding, and fusion setup across Wave 3 operators.

Wave 2 lowering handles `WHERE`, `SELECT`, `LIMIT`, `EXPLAIN`, and all
DDL/DML forms. This document extends lowering to the four new pipeline
stages introduced in Wave 3:

1. `PipelineStage::Match` -> `LogicalPlan::SequenceMatch`
2. `PipelineStage::Stats` -> `LogicalPlan::Aggregate`
3. `PipelineStage::OrderBy` -> `LogicalPlan::Sort`
4. `PipelineStage::Select { distinct: true }` -> `LogicalPlan::Distinct(Project(...))`

It also specifies the `DemandSet` struct (planner-pipeline.md SS9.3),
the backward-propagation algorithm, and how TASK-320's fusion pass
populates `fused_downstream` after demand resolves.

**What this document does not cover:**

- Pattern compilation (`SequencePattern` -> `CompiledNfa`) -- TASK-311.
- Optimizer passes 1-6 -- planner-pipeline.md SS6.
- Physical plan descriptor shapes -- planner-pipeline.md SS9.5.
- Operator runtime behavior -- match-operator.md, aggregate-operator.md.

---

## 2. Lowering Rules

### 2.1 Match -> SequenceMatch

**AST source**: `PipelineStage::Match { pattern: MatchPattern, span }`

**Logical node**:

```rust
LogicalPlan::SequenceMatch {
    pattern: SequencePattern,        // converted from MatchPattern
    mode: MatchMode,                 // FIRST | ALL (from pattern.mode)
    emit_all: bool,                  // copied from pattern.emit_all
    window: Option<MatchWindowSpec>,  // lowered from MatchWindow; see SS2.1.1
    brackets: Option<BracketSpec>,   // always None in Wave 3; populated by RETENTION desugaring in Wave 4
    step_properties: Vec<StepPropertyRef>, // initially empty; filled by demand analysis (Pass 4)
    fused_downstream: Option<FusedDownstream>, // initially None; set by Pass 6 (TASK-320)
    input: Box<LogicalPlan>,
    output_schema: OperatorSchema,   // computed per SS2.1.3
}
```

#### 2.1.1 MatchPattern -> SequencePattern Conversion

The planner converts the AST `MatchPattern` into a planner-owned
`SequencePattern` during lowering. The conversion is structural:

| AST field | Planner field | Transformation |
|---|---|---|
| `pattern.steps: Vec<Step>` | `SequencePattern.steps` | Each step's event type validated against the catalog; predicates type-checked via `type_check(expr, step_event_schema)` |
| `pattern.mode: MatchMode`, `pattern.emit_all: bool` | `mode`, `emit_all` | `mode` lowers directly; `emit_all` is copied unchanged |
| `pattern.window` | `window` | `MatchWindow::Within(ns)` -> `Some(MatchWindowSpec::Duration(ns))`, `MatchWindow::WithinSession` -> `Some(MatchWindowSpec::Session)`, `None` -> `None` |
| `pattern.brackets` | `brackets` | `None` in Wave 3. RETENTION desugaring (Wave 4) will populate this; the field is carried as `Option<BracketSpec>` for forward compatibility. When populated, durations are validated for monotonically increasing order. |
| `step.name: Option<Name>` | step name table | Recorded for step-property resolution in SS2.1.3 |

**MatchWindowSpec.** The AST `MatchWindow` type distinguishes between
duration-based windows and session-based windows. The planner-side
`MatchWindowSpec` preserves this distinction:

```rust
pub enum MatchWindowSpec {
    /// WITHIN <duration> -- nanoseconds.
    Duration(i64),
    /// WITHIN SESSION -- NFA resets on session_id transitions.
    /// Requires an upstream SESSIONIZE operator (Wave 4).
    Session,
}
```

In Wave 3, `MatchWindow::WithinSession` is accepted at lowering time
(producing `MatchWindowSpec::Session`) but execution requires an upstream
`Sessionize` operator that annotates events with `session_id`. Since
SESSIONIZE is a Wave 4 operator, a Wave 3 query using `WITHIN SESSION`
without an upstream SESSIONIZE will fail at the schema-validation step
(the `session_id` column will not be present in the input schema).
planner-pipeline.md SS7.7 describes the `WITHIN SESSION` semantics.

**Event type validation.** For each step, the planner resolves the event
type name against the catalog's table schema. The event type column must
exist and the step's predicate expressions must type-check against the
step's event type's property columns. If a step references a column not
present on its event type, the planner raises `TypeError::ColumnNotFound`
with context identifying the step name and event type.

**Variable binding validation.** For each `$var` reference in step
predicates or in downstream expressions:

1. The first binding site determines the variable's type.
2. Subsequent uses are checked for type equality against the first.
3. If types conflict, the planner raises `TypeError::VariableTypeConflict {
   variable, first_type, first_step, second_type, second_step }`.

Variable bindings in step predicates are recorded in the `SequencePattern`
for the pattern compiler to resolve during TASK-311.

#### 2.1.2 MatchMode Mapping

The logical plan carries match mode and `emit_all` as two orthogonal fields:

| AST fields | Logical `mode` | Logical `emit_all` | Semantics |
|---|---|---|---|
| `mode = First`, `emit_all = false` | `MatchMode::First` | `false` | One match per entity (earliest) |
| `mode = All`, `emit_all = false` | `MatchMode::All` | `false` | All non-overlapping matches per entity |
| `mode = First`, `emit_all = true` | `MatchMode::First` | `true` | One row per entity regardless of completion; `step_reached` column present |
| `mode = All`, `emit_all = true` | `MatchMode::All` | `true` | One row per step-1 entry; `step_reached` column present |

FUNNEL and RETENTION desugaring use `mode = First, emit_all = true`
(planner-pipeline.md SS4.3). When `emit_all` is true, MATCH emits partial
progress rows with `step_reached` indicating how far through the pattern the
entity or entry progressed (1-indexed).

#### 2.1.3 Output Schema Computation

The MATCH output schema is demand-driven (type-system.md SS6.1). At
lowering time, the planner constructs the **maximum** schema; demand
analysis (Pass 4) prunes unreferenced columns later.

The maximum schema contains:

| Column | Type | Nullable | Condition |
|---|---|---|---|
| `entity_id` | same as table entity key | no | always |
| `$var` (per binding) | binding type | no | when pattern has `$var` bindings |
| `step_reached` | `Int` | no | when `emit_all == true` |
| `match_duration` | `Int` | yes | demand-driven (needs_match_detail) |
| `match_events` | `Map(Timestamp)` | yes | demand-driven (needs_match_detail) |
| `<step>.<column>` | source column type | yes | demand-driven (step_properties) |

At construction time:

1. `entity_id` is always added.
2. Variable binding columns are added from the pattern's binding declarations.
3. If `emit_all` is true, `step_reached` is added unconditionally.
4. `match_duration` and `match_events` are added to the maximum schema but
   flagged as demand-dependent -- they survive into the final schema only
   if downstream references them.
5. Step-property columns (`s.plan`, `p.amount`) are **not** added at
   construction time. They are resolved and added during demand analysis
   (Pass 4) when the planner encounters qualified column references in
   downstream operators. See SS3.3.

**Scan time range extension.** When the pattern has a `WITHIN` window, the
planner extends the scan's upper time bound by the window duration. When
brackets are present, the extension is `max(window, max_bracket)`. This is
computed during lowering and applied to the `Scan` node's `time_range`.
See planner-pipeline.md SS4.4.

### 2.2 Stats -> Aggregate

**AST source**: `PipelineStage::Stats { aggregates: Vec<AggItem>, group_by: Vec<GroupItem>, span }`

**Logical node**:

```rust
LogicalPlan::Aggregate {
    aggregates: Vec<TypedAggExpr>,
    group_by: Vec<(TypedExpr, String)>,  // (expr, output_name)
    input: Box<LogicalPlan>,
    output_schema: OperatorSchema,       // computed per SS2.2.2
}
```

**Refinement note.** planner-pipeline.md SS5.1 defines `group_by` as
`Vec<TypedExpr>`. This document refines the shape to
`Vec<(TypedExpr, String)>` -- a (expression, output_name) pair -- because
BQL requires every group-by key to have an output name in the aggregate's
result schema (query-language.md SS7.1). The output name comes from the
`GroupItem.alias` (explicit) or is generated from the expression text
(implicit). This refinement is additive and does not invalidate the
parent spec's intent.

#### 2.2.1 AggItem -> TypedAggExpr Conversion

Each `AggItem` is validated and type-checked:

```rust
pub struct TypedAggExpr {
    pub function: AggFunction,     // COUNT, SUM, AVG, MIN, MAX, COUNT_DISTINCT
    pub args: Vec<TypedExpr>,      // type-checked against input schema
    pub distinct: bool,            // reserved, always false in v1
    pub output_name: String,       // from AggItem.alias
    pub output_type: BqlType,      // inferred from function + arg types
    pub nullable: bool,            // inferred from function
    pub span: Span,
}
```

**Function resolution.** The `AggItem.function` name is matched
case-insensitively against the known aggregate function set:

| Function name | `AggFunction` variant | Arg count | Arg type constraint | Output type | Nullable |
|---|---|---|---|---|---|
| `count` | `Count` | 0 (`*`) or 1 | any | `Int` | no |
| `sum` | `Sum` | 1 | `Int` or `Float` | same as arg | yes |
| `avg` | `Avg` | 1 | `Int` or `Float` | `Float` | yes |
| `min` | `Min` | 1 | any orderable | same as arg | yes |
| `max` | `Max` | 1 | any orderable | same as arg | yes |
| `count_distinct` | `CountDistinct` | 1 | any | `Int` | no |

If the function name is unrecognized, the planner raises
`TypeError::ColumnNotFound` with a context message indicating the
function is unknown (reusing the existing error variant with a clear
diagnostic). If the argument type violates the constraint, the planner
raises `TypeError::InvalidAggregateInput { function, column, actual_type }`.

**COUNT(*) handling.** When `AggItem.args` is empty, the function is
`COUNT(*)` -- a row count with no column dependency. The `TypedAggExpr`
has an empty `args` vector. `COUNT(col)` with one argument counts
non-null values of that column.

**Expression arguments.** Aggregate arguments may be arbitrary expressions,
not just column references. For example, `SUM(CAST(step_reached >= 2 AS INT))`
is a valid aggregate argument. The inner expression is type-checked
against the input schema using the standard `type_check` entry point.

#### 2.2.2 Output Schema

The output schema of `Aggregate` is:

1. Group-by columns, in declaration order. Each has the type and
   nullability of its source expression.
2. Aggregate result columns, in declaration order. Each has the
   output type and nullability from the table above.

The input schema is **not** passed through -- only group-by keys and
aggregate results are visible downstream. This is the standard SQL
aggregate contract.

**Group-by alias handling.** Each `GroupItem` may have an explicit alias
(`GROUP BY QUANTIZE(s.ts, 1d) AS day`). If present, the alias becomes
the output column name. If absent, the planner generates a name from the
expression text (column reference -> column name; function call ->
`fn_arg1_arg2`; complex expression -> `group_N` where N is the 0-based
position).

**Schema validation of group-by expressions.** Every group-by expression
must type-check against the input schema. `step_name.column` references
are resolved through the MATCH operator's step-property mechanism if the
input is a `SequenceMatch` node (see SS3.3).

#### 2.2.3 Variable Reference Rules Through Aggregates

`$var` references that originate in a MATCH pattern survive into the
aggregate **only** as group-by keys or aggregate arguments. The planner
enforces:

1. A `$var` column in a group-by expression is valid -- it groups matches
   by the binding value.
2. A `$var` column in an aggregate argument is valid -- e.g.,
   `COUNT_DISTINCT($product)`.
3. A bare `$var` reference **outside** a group-by key or aggregate argument
   in a downstream SELECT after an AGGREGATE is a `TypeError::ColumnNotFound`
   -- the aggregate boundary collapses the per-entity match rows, so
   ungrouped binding values are no longer available.

This is standard SQL scoping: after GROUP BY, only grouped columns and
aggregates are accessible.

### 2.3 OrderBy -> Sort

**AST source**: `PipelineStage::OrderBy { items: Vec<OrderItem>, span }`

**Logical node**:

```rust
LogicalPlan::Sort {
    keys: Vec<(TypedExpr, SortDir)>,
    input: Box<LogicalPlan>,
    output_schema: OperatorSchema,  // identical to input
}
```

#### 2.3.1 Lowering Rules

Each `OrderItem` is converted:

1. Type-check `item.expr` against the input's output schema.
2. Verify the expression's type is orderable (all BQL types except `Map`
   are orderable).
3. Preserve the `SortDir` (`Asc` or `Desc`). Default is `Asc` per
   query-language.md SS13.

**Output schema**: identical to the input schema. Sort does not add,
remove, or rename columns.

**Interaction with LIMIT.** `ORDER BY ... LIMIT N` is a common pattern.
The planner lowers these as adjacent `Sort -> Limit` nodes. The optimizer
does not fuse them in v1 -- they execute as separate operators. A future
top-N optimization may fuse them into a single heap-based operator.

**Interaction with fusion.** Sort between a stateful operator and an
aggregate **blocks fusion** (planner-pipeline.md SS7.2 rule 4). The
planner does not attempt to reorder Sort past Aggregate. Sort is a
pipeline breaker -- it must see all input rows before producing output.

### 2.4 Select { distinct: true } -> Distinct(Project(...))

**AST source**: `PipelineStage::Select { distinct: true, items, span }`

**Logical nodes** (two-node expansion):

```rust
LogicalPlan::Distinct {
    input: Box<LogicalPlan::Project {
        expressions: Vec<ProjectItem>,
        input: Box<LogicalPlan>,   // upstream
    }>,
    output_schema: OperatorSchema, // identical to inner Project's output
}
```

#### 2.4.1 Lowering Rules

1. Lower the `SELECT` items into a `Project` node exactly as Wave 2's
   `SELECT` lowering does (type-check each expression, resolve wildcards,
   compute output schema).
2. Wrap the `Project` in a `Distinct` node.
3. The `Distinct` output schema is identical to its input (the inner
   `Project`'s output).

`Distinct` deduplicates rows based on **all** output columns. There is no
partial-column distinct in BQL v1. The implementation shares its hashing
kernel with `Aggregate`'s `GROUP BY` (aggregate-operator.md SS4.1 --
`GroupKey` hashing).

**Non-distinct SELECT.** `PipelineStage::Select { distinct: false }` lowers
to `Project` alone, exactly as in Wave 2.

---

## 3. Demand Propagation

### 3.1 DemandSet

`DemandSet` is the downstream-needs struct propagated backward through the
plan tree from root toward scan. It is the planner's source of truth for
what each operator must produce.

```rust
pub struct DemandSet {
    /// Flat column names the downstream needs to see.
    pub columns: HashSet<ColumnId>,

    /// Whether `match_events` and `match_duration` are needed.
    pub needs_match_detail: bool,

    /// Whether `step_reached` is needed.
    pub needs_step_reached: bool,

    /// Named step properties needed (per step, per column).
    pub step_properties: Vec<StepPropertyRef>,

    /// Forwarded columns needed from SESSIONIZE / ATTRIBUTE (Wave 4).
    pub forwarded: Vec<ColumnId>,

    /// Fused aggregate specification, if fusion is active.
    /// Set by TASK-320 (Pass 6) after demand resolves.
    pub fused_aggregate: Option<FusableAggregate>,

    /// Fused filter predicate, if fusion is active.
    /// Set by TASK-320 (Pass 6) after demand resolves.
    /// NOTE: This field extends planner-pipeline.md SS9.3's DemandSet
    /// definition, which includes fused_aggregate but not fused_filter.
    /// The addition is driven by the FilterThenAggregate fusion pattern
    /// (planner-pipeline.md SS5.3) -- the filter predicate must travel
    /// alongside the fused aggregate so the physical planner can emit
    /// a single fused operator.
    pub fused_filter: Option<TypedExpr>,
}
```

`StepPropertyRef` identifies a per-(step, column) demand:

```rust
pub struct StepPropertyRef {
    pub step_name: String,     // user-facing step label (e.g. "s", "p")
    pub column_name: String,   // property column on the step's event type
    pub bql_type: BqlType,     // resolved type from the catalog
}
```

**Crate placement.** `DemandSet` and `StepPropertyRef` live in
`bqlite-planner`. They are planner-internal types, not shared with
operators. The operator-side dual is `DemandCapabilities` (bqlite-core),
which advertises what demands an operator can satisfy (sequence-matching.md
SS13.5).

### 3.2 Backward Propagation Algorithm

The demand propagation pass runs as optimizer Pass 4 (projection pruning)
in planner-pipeline.md SS6.6. It walks the logical plan tree from the root
(output) toward the scan, accumulating a `DemandSet` at each node.

**Algorithm:**

```
fn propagate_demand(node: &mut LogicalPlan, downstream_demand: DemandSet) {
    match node {
        // -- Terminal: Scan absorbs demand --
        Scan { projected_columns, .. } => {
            *projected_columns = downstream_demand.columns
                .union(always_required)  // entity_id, ts, event_type
                .collect();
        }

        // -- Transparent nodes: pass demand through unchanged --
        Filter { predicate, input, .. } => {
            let mut demand = downstream_demand;
            demand.columns.extend(predicate.referenced_columns());
            propagate_demand(input, demand);
        }

        Limit { input, .. } => {
            propagate_demand(input, downstream_demand);
        }

        // -- Schema-reshaping nodes: transform demand --
        Project { expressions, input, .. } => {
            let mut child_demand = DemandSet::empty();
            // For each output column the downstream wants,
            // find the Project expression that produces it and
            // add that expression's referenced input columns.
            for (expr, name) in expressions {
                if downstream_demand.columns.contains(name) {
                    child_demand.columns.extend(expr.referenced_columns());
                }
            }
            // Step properties and match detail flags pass through
            // Projects unchanged -- they reference MATCH output, not
            // the Project's input.
            child_demand.needs_match_detail = downstream_demand.needs_match_detail;
            child_demand.needs_step_reached = downstream_demand.needs_step_reached;
            child_demand.step_properties = downstream_demand.step_properties;
            propagate_demand(input, child_demand);
        }

        Sort { keys, input, .. } => {
            let mut demand = downstream_demand;
            for (expr, _dir) in keys {
                demand.columns.extend(expr.referenced_columns());
            }
            propagate_demand(input, demand);
        }

        Distinct { input, .. } => {
            // Distinct needs all its output columns to compute
            // uniqueness, so it demands all of its own output schema.
            let mut demand = downstream_demand;
            for col in input.output_schema().column_names() {
                demand.columns.insert(col);
            }
            propagate_demand(input, demand);
        }

        // -- Aggregate: demand boundary --
        Aggregate { aggregates, group_by, input, .. } => {
            let mut child_demand = DemandSet::empty();
            // Group-by expressions reference the input schema.
            for (expr, _name) in group_by {
                child_demand.columns.extend(expr.referenced_columns());
                // Qualified references (step_name.column) become
                // step-property demand.
                child_demand.step_properties.extend(
                    extract_step_property_refs(expr)
                );
            }
            // Aggregate argument expressions reference the input schema.
            for agg in aggregates {
                for arg in &agg.args {
                    child_demand.columns.extend(arg.referenced_columns());
                    child_demand.step_properties.extend(
                        extract_step_property_refs(arg)
                    );
                }
            }
            // step_reached detection: if any expression references
            // "step_reached", propagate the flag.
            if child_demand.columns.contains("step_reached") {
                child_demand.needs_step_reached = true;
            }
            // match_duration / match_events detection.
            if child_demand.columns.contains("match_duration")
                || child_demand.columns.contains("match_events")
            {
                child_demand.needs_match_detail = true;
            }
            propagate_demand(input, child_demand);
        }

        // -- SequenceMatch: demand consumer and producer --
        SequenceMatch {
            step_properties,
            input,
            output_schema,
            pattern,
            ..
        } => {
            // 1. Record step-property demand on the node itself.
            *step_properties = downstream_demand.step_properties.clone();

            // 2. Refine output schema based on demand.
            prune_match_output_schema(
                output_schema,
                &downstream_demand,
            );

            // 3. Compute scan-side demand from the pattern.
            let mut child_demand = DemandSet::empty();
            // Always need entity_id, ts, event_type.
            child_demand.columns.insert("entity_id".into());
            child_demand.columns.insert("ts".into());
            child_demand.columns.insert("event_type".into());

            // Columns from step predicates.
            for step in &pattern.steps {
                if let Some(pred) = &step.predicate {
                    child_demand.columns.extend(
                        pred.referenced_columns()
                    );
                }
            }
            // Columns from variable bindings.
            for binding in pattern.variable_bindings() {
                child_demand.columns.insert(binding.column.clone());
            }
            // Columns from step-property forwarding.
            for sp in &downstream_demand.step_properties {
                child_demand.columns.insert(sp.column_name.clone());
            }

            propagate_demand(input, child_demand);
        }
    }
}
```

### 3.3 Step-Property Resolution

When the planner encounters a qualified column reference of the form
`step_name.column_name` in any expression downstream of a
`SequenceMatch` node:

1. **Find the step.** Look up `step_name` in the MATCH pattern's step
   name table. If not found, raise `TypeError::StepNotFound { step_name,
   available }` where `available` lists the declared step names.

2. **Find the column.** Look up `column_name` in the catalog schema for
   the step's event type. If not found, raise `TypeError::ColumnNotFound`
   with context `"step <step_name> (event type <event_type>)"`.

3. **Record the demand.** Add `StepPropertyRef { step_name, column_name,
   bql_type }` to the demand set being propagated. The resolved type
   comes from the catalog column definition.

4. **Add to output schema.** During demand analysis, each resolved
   step-property reference is added to the `SequenceMatch` node's
   `output_schema` as a column named `<step_name>.<column_name>` with
   the resolved type. Nullability is `true` -- when `emit_all` is
   enabled, entities that did not reach the referenced step have NULL
   for that step's properties.

Step-property resolution happens at type-check time during lowering for
expressions that appear directly after a MATCH (e.g., `WHERE s.plan = 'pro'`),
and during demand propagation (Pass 4) for expressions further downstream
(e.g., aggregate arguments that reference step properties).

### 3.4 step_reached Synthetic Column

The `step_reached` column is synthetic -- it is produced by the
`SequenceMatch` operator, not read from the scan.

**When present.** `step_reached` is in the MATCH output schema when
`emit_all == true`. It is an `Int` column, non-nullable, with values
from 1 to `num_steps` (inclusive, 1-indexed).

**Demand propagation.** When downstream expressions reference
`step_reached`, the demand propagation pass sets
`DemandSet.needs_step_reached = true`. This flag does **not** propagate
past the `SequenceMatch` node -- the column is produced there, not
upstream. The `SequenceMatch` node's demand transform strips
`step_reached` from the child demand (it does not ask the scan for it).

**Usage pattern.** The typical use is in FUNNEL desugaring:
`SUM(CAST(step_reached >= N AS INT))` counts entities that reached step N.
The expression `step_reached >= N` type-checks as `Bool` because
`step_reached` is `Int` and `N` is an `Int` literal.

### 3.5 match_detail Columns

`match_duration` and `match_events` are demand-driven columns that
appear only when explicitly referenced downstream.

**`match_duration`**: `Int`, nullable (NULL when `step_reached == 1`).
Nanoseconds between the first and last matched step's timestamps.

**`match_events`**: `Map(Timestamp)`, nullable. Step-name to timestamp
mapping for each matched step. Partial when `emit_all` is true and
`step_reached < num_steps`.

**Demand flag.** When either column is referenced, the demand pass sets
`needs_match_detail = true`. This forces the physical planner to select
a strategy with path tracking (typically `FullNfa`), overriding the
default strategy selection from sequence-matching.md SS10.2.

### 3.6 DemandSet Shape Stability

The `DemandSet` shape defined in SS3.1 persists into Waves 4 and 5. The
`forwarded` field (empty in Wave 3) is reserved for SESSIONIZE and
ATTRIBUTE column forwarding in Wave 4. The `fused_aggregate` and
`fused_filter` fields are populated by TASK-320 (Pass 6 -- fusion
detection) and are not set during the backward propagation pass itself.

Future extensions (e.g., window function demand) should add new fields
to `DemandSet` rather than repurposing existing ones, to maintain
backward compatibility with existing demand transforms.

---

## 4. Fusion Setup

### 4.1 Relationship to Demand Propagation

Demand propagation (Pass 4) and fusion detection (Pass 6) are separate
passes that run in sequence per planner-pipeline.md SS6.2:

1. **Pass 4 (demand propagation)** resolves which columns each node
   needs and records step-property demand on `SequenceMatch` nodes.
2. **Pass 6 (fusion detection -- TASK-320)** examines
   `SequenceMatch -> Aggregate` chains and, if eligible, populates
   `fused_aggregate` on the `SequenceMatchPhysical` descriptor and
   removes the `Aggregate` node.

The separation ensures that demand is fully resolved before fusion
decisions are made. Fusion eligibility depends on knowing which columns
the aggregate references, which is only available after Pass 4 has
collected demand.

**Implementation note (TASK-320 Wave 3 scope).** Pass 6 operates on
the physical plan after logical→physical lowering, directly setting
`SequenceMatchPhysical.fused_aggregate`. The logical plan's
`SequenceMatch.fused_downstream` field is not set by this pass —
it remains `None` and the physical lowering's `fused_downstream.map(...)`
arm is a forward-compat path for a possible future logical-level fusion
optimizer.

### 4.2 Fusion Eligibility Summary

Per planner-pipeline.md SS7.2, full fusion requires:

1. **Adjacency.** `SequenceMatch` and `Aggregate` are adjacent (or
   separated by a single `Filter`).
2. **Incremental computability.** All aggregate functions are
   incrementally computable (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`,
   `COUNT_DISTINCT`, and DDSketch percentiles are all incremental).
3. **Group-by key availability.** Every group-by expression references
   only columns in the `SequenceMatch` output schema (including
   `entity_id`, `$var` bindings, step properties, `step_reached`).
4. **No ordering dependency.** No `Sort` between the `SequenceMatch`
   and the `Aggregate`.

**TASK-320 Wave 3 implements direct-adjacency fusion only.**
When `Aggregate` is immediately above `SequenceMatch` (no intermediate
node) and conditions 2–4 hold, the pass constructs a
`CompiledFusableAggregate` from the `AggregatePhysical` descriptor and
sets it on `SequenceMatchPhysical.fused_aggregate`. The `Aggregate` node
is removed.

The `Filter`-separated pattern (`Aggregate(Filter(SequenceMatch(...)))`),
which planner-pipeline.md §7.2 calls `FilterThenAggregate`, is not fused
in Wave 3. A `Filter` between the `Aggregate` and `SequenceMatch` blocks
fusion — the plan tree is left unchanged and both nodes remain. This is
a deliberate conservative choice: the `CompiledFusableAggregate` type
would need a `fused_filter: Option<CompiledExpr>` field and the
`SequenceMatchOperator` (TASK-321) would need to evaluate it at entity
boundary. This complexity is deferred to a follow-up task. The
`DemandSet.fused_filter` field is reserved for this purpose.

The `Aggregate` node is removed from the plan tree when fusion occurs.

### 4.3 Post-Fusion Demand Update

After fusion, the `SequenceMatch` node's effective output schema changes
from its own output to the fused aggregate's output. The physical planner
uses the `fused_downstream` field to determine the final output schema
and configures the operator's layered extraction accordingly
(match-operator.md SS3, planner-pipeline.md SS7.5).

---

## 5. Schema Validation Rules

### 5.1 step_name.column References

A qualified reference `step_name.column_name` is valid only when:

1. There is a `SequenceMatch` node in the ancestry chain (walking
   input pointers from the referencing expression toward the scan).
2. The `SequenceMatch` pattern declares a step with the given name.
3. The step's event type has a column with the given name in the
   catalog schema.

**Error cases:**

| Condition | TypeError variant |
|---|---|
| No MATCH in ancestry | `ColumnNotFound { column: "step_name.column_name", context: "no MATCH operator in scope" }` |
| Step name not declared | `StepNotFound { step_name, available: [declared step names] }` |
| Column not on event type | `ColumnNotFound { column: column_name, context: "step <step_name> (event type <event_type>)" }` |

### 5.2 $var References Through Aggregate GROUP BY

After an `Aggregate` boundary, only group-by keys and aggregate results
are accessible. `$var` columns from a MATCH survive past an aggregate
**only** if they appear in the aggregate's `group_by` list or as an
aggregate argument.

**Valid:**
```bql
events | MATCH ... | STATS total = COUNT(*) GROUP BY $product
-- downstream can reference: $product (group-by key), total (aggregate)
```

**Invalid:**
```bql
events | MATCH ... | STATS total = COUNT(*)
       | SELECT $product
-- TypeError: $product not found in aggregate output schema
```

The planner enforces this through standard schema validation: the
`SELECT` operator's type-check sees only the `Aggregate`'s output schema,
which does not include `$product` unless it is a group-by key.

### 5.3 step_reached Under EMIT ALL

`step_reached` is present in the `SequenceMatch` output schema **only**
when `emit_all == true`. Referencing `step_reached` downstream of a
non-EMIT-ALL MATCH raises `TypeError::ColumnNotFound`.

When `emit_all` is false, only completed matches are emitted, so
`step_reached` is implicitly `num_steps` for every row. The column is
omitted to avoid carrying redundant data.

### 5.4 Aggregate Output Name Uniqueness

Every aggregate result and every group-by key must have a distinct output
name. If two items produce the same output name, the planner raises
`TypeError::NameCollision { name, context: "STATS output" }`.

### 5.5 Sort Key Type Validation

Sort keys must be orderable types. `Map` is not orderable. If a sort key
expression evaluates to `Map`, the planner raises
`TypeError::TypeMismatch` with context indicating that `Map` values
cannot be compared.

All other BQL types (`Bool`, `Int`, `Float`, `String`, `Timestamp`,
`List`) are orderable. `List` ordering is lexicographic by element.

---

## 6. Lowering Sequence

The full lowering sequence for a Wave 3 pipeline integrates with the
Wave 2 fold-left lowering loop (planner-pipeline.md SS4.7):

```
For each PipelineStage in pipeline.operators:
    match stage {
        // Wave 2 (existing)
        Where { .. }  => Filter(type_check(predicate), accumulated)
        Select { distinct: false, .. } => Project(type_check(items), accumulated)
        Limit { .. }  => Limit(count, accumulated)

        // Wave 3 (new)
        Match { pattern, .. } => {
            validate_pattern(pattern, catalog)
            SequenceMatch(convert_pattern(pattern), accumulated)
        }
        Stats { aggregates, group_by, .. } => {
            Aggregate(
                type_check_aggs(aggregates, accumulated.output_schema()),
                type_check_group_by(group_by, accumulated.output_schema()),
                accumulated,
            )
        }
        OrderBy { items, .. } => {
            Sort(
                type_check_sort_keys(items, accumulated.output_schema()),
                accumulated,
            )
        }
        Select { distinct: true, items, .. } => {
            let project = Project(type_check(items), accumulated);
            Distinct(project)
        }

        // Desugaring (Wave 3)
        Funnel(args) => {
            // Per planner-pipeline.md SS4.3:
            // 1. Build SequenceMatch from funnel steps
            // 2. Build Aggregate with SUM(CAST(step_reached >= N AS INT))
            //    per step, named after the step's event type
            let match_node = SequenceMatch(
                desugar_funnel_pattern(args),
                accumulated,
            );
            Aggregate(desugar_funnel_aggregates(args), [], match_node)
        }
        Let { name, expr, .. } => {
            // LET x = expr  =>  Project(*, expr AS x)
            Project(
                extend_with(accumulated.output_schema(), name, type_check(expr)),
                accumulated,
            )
        }

        // Deferred
        Retention(..) => TypeError (Wave 4)
        Sessionize(..) => TypeError (Wave 4)
        _ => TypeError (unsupported stage)
    }
```

**Error on unsupported stages.** Pipeline stages that Wave 3 does not
implement (`Retention`, `Sessionize`, `Attribute`, `Pivot`, `Sample`,
`EventSelect`) produce a planner error at lowering time. The error
message identifies the stage and the wave that will implement it.

---

## 7. Worked Example

### 7.1 Input

```bql
events LAST 30d
| MATCH FIRST SEQUENCE(
    s: signup WHERE country = 'US'
    THEN p: purchase WHERE amount > 50
  ) WITHIN 7d EMIT ALL
| WHERE s.plan = 'pro'
| STATS
    converted = SUM(CAST(step_reached >= 2 AS INT)),
    total = COUNT(*)
  GROUP BY QUANTIZE(s.ts, 1d) AS day
```

### 7.2 After Lowering (Pre-Optimization)

```
Aggregate
  aggregates: [
    converted = SUM(CAST(step_reached >= 2 AS INT)),
    total     = COUNT(*)
  ]
  group_by: [QUANTIZE(s.ts, 1d) AS day]
  output_schema: [day: Timestamp, converted: Int, total: Int]
  |
  Filter(s.plan = 'pro')
    output_schema: [entity_id, step_reached, s.plan(*), s.ts(*), ...]
    |
    SequenceMatch
      pattern: s: signup WHERE country = 'US'
               THEN p: purchase WHERE amount > 50
      mode: FIRST, emit_all: true
      window: 7d (604800000000000 ns)
      step_properties: []  (not yet resolved)
      fused_downstream: None
      output_schema: [entity_id, step_reached, match_duration(?), ...]
      |
      Scan(events)
        time_range: [now-30d, now+7d]  (extended by 7d window)
```

(*) Step-property columns are demand-dependent; shown here as
present in the maximum schema before pruning.

### 7.3 After Demand Propagation (Pass 4)

Walking backward from the Aggregate:

1. **Aggregate demands**: `step_reached`, `s.ts` (from QUANTIZE group-by),
   no other columns from the Match output.
2. **Filter demands**: `s.plan` (step-property reference).
3. **SequenceMatch consumes**: `step_reached` (produced here),
   `s.plan` and `s.ts` (step-property demands).
   Records: `step_properties = [(s, plan, String), (s, ts, Timestamp)]`.
   Passes upstream: `entity_id`, `ts`, `event_type`, `country`, `amount`, `plan`.

Result:
- `SequenceMatch.step_properties = [(s, plan, String), (s, ts, Timestamp)]`
- `SequenceMatch.output_schema` pruned to:
  `[entity_id, step_reached, s.plan, s.ts]`
- `Scan.projected_columns = [entity_id, ts, event_type, country, amount, plan]`
- `needs_step_reached = true`, `needs_match_detail = false`

### 7.4 After Fusion (Pass 6)

Pass 6 detects `SequenceMatch -> Filter -> Aggregate` chain:
- All aggregates are incremental (SUM, COUNT).
- Group-by key `QUANTIZE(s.ts, 1d)` references `s.ts`, which is a
  step property available in the SequenceMatch output.
- No Sort in between.

Fusion succeeds:

```
SequenceMatch
  fused_downstream: FilterThenAggregate {
    filter: s.plan = 'pro',
    aggregate: FusableAggregate {
      functions: [SUM, COUNT],
      arguments: [CAST(step_reached >= 2 AS INT), *],
      group_by: [QUANTIZE(s.ts, 1d)],
      output_names: [converted, total],
    }
  }
  step_properties: [(s, plan, String), (s, ts, Timestamp)]
  output_schema: [day: Timestamp, converted: Int, total: Int]
  |
  Scan(events)
    time_range: [now-30d, now+7d]
    projected_columns: [entity_id, ts, event_type, country, amount, plan]
```

Two plan nodes remain. Filter and Aggregate have been absorbed.

---

## 8. Interaction with Physical Planning

The physical planner (planner-pipeline.md SS9) consumes the optimized
logical plan produced by the pipeline above. For each Wave 3 node:

### 8.1 SequenceMatch -> SequenceMatchPhysical

1. **Classify the pattern** using sequence-matching.md SS10.1 to obtain a
   `PatternClass`.
2. **Select strategy** from the matrix in sequence-matching.md SS10.2,
   considering the `DemandSet` (especially `needs_match_detail`, which
   forces `FullNfa` with path tracking).
3. **Compile the pattern** via TASK-311 to obtain a `CompiledNfa`.
4. **Build `MatchExecutionConfig`** from the demand:
   - `track_match_duration = needs_match_detail`
   - `track_match_events = needs_match_detail`
   - `step_properties` = resolved `StepPropertyRef` entries mapped to
     `StepPropertyExtraction { step_index, column_name, bql_type }`
   - `fused_accumulator` = constructed from `fused_downstream` if present
5. **Compile expressions** (`TypedExpr` -> `CompiledExpr`) in fused
   filter and aggregate arguments.

### 8.2 Aggregate -> AggregatePhysical

When not fused (i.e., the Aggregate node survived Pass 6):

1. Compile group-by expressions to `CompiledExpr`.
2. Compile aggregate argument expressions to `CompiledExpr`.
3. Build `AggregatePhysical` with compiled expressions and the
   `AggFunction` list.

### 8.3 Sort -> SortPhysical

1. Compile sort-key expressions to `CompiledExpr`.
2. Record sort directions.

### 8.4 Distinct -> DistinctPhysical

1. Record the output schema (all columns are dedup keys).

---

## 9. Resolved Design Decisions

| Decision | Rationale |
|---|---|
| `DemandSet` lives in `bqlite-planner`, not `bqlite-core` | It is a planner-internal optimization artifact; operators see `DemandCapabilities` (the dual) |
| Step properties are resolved lazily during demand analysis, not eagerly during lowering | Keeps the maximum-schema construction simple; demand analysis is the authoritative point for column pruning |
| `window` uses `MatchWindowSpec` enum, not bare `Option<i64>` | Preserves the `WITHIN SESSION` distinction from the AST; `Session` variant requires upstream `Sessionize` (Wave 4) at execution |
| `brackets` is always `None` in Wave 3 | RETENTION (the primary bracket consumer) is Wave 4; the field exists for forward compatibility |
| `Aggregate.group_by` is `Vec<(TypedExpr, String)>`, refining planner-pipeline.md's `Vec<TypedExpr>` | BQL requires named group-by outputs; the output name is needed for schema construction |
| `emit_all` is a separate bool, not a third `MatchMode` variant | Clean separation: `mode` controls match semantics (FIRST vs ALL), `emit_all` controls output completeness |
| `Distinct` is its own node, not `Aggregate(group_by=all_columns)` | Semantically cleaner; shares hash kernel but avoids aggregation overhead |
| Fusion is a separate pass from demand propagation | Demand must be fully resolved before fusion eligibility can be assessed |
| `step_reached` is non-nullable even under EMIT ALL | Every entity emits exactly one row with a valid step count (minimum 1) |
| `DemandSet.forwarded` is present but empty in Wave 3 | Forward-compatible slot for Wave 4 SESSIONIZE/ATTRIBUTE column forwarding |
| Sort blocks fusion (rule 4 in planner-pipeline.md SS7.2) | Sort is a pipeline breaker; cannot incrementally accumulate through a sort boundary |
