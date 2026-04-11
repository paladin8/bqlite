# Type System Design

> **Status**: DRAFT
> **Task**: TASK-005
> **Depends on**: (none — this is foundational)
> **Depended on by**: TASK-001 (storage format), TASK-002 (query language), TASK-003 (execution model), TASK-004 (sequence matching)

---

## 1. Design Goals

The type system serves three constraints from [core-beliefs.md](../core-beliefs.md):

**Performance (Belief 1).** Types must map 1:1 onto Arrow physical types with zero conversion overhead in the hot path. The BQL type enum must be small and cheap to compare — no heap allocation in the enum discriminant itself. Type dispatch in vectorized operators should compile to jump tables, not dynamic trait dispatch.

**Strongly-typed pipelines (Belief 8).** Every operator input and output is schema-typed. The planner validates schema compatibility at plan construction time. There are no runtime type errors during execution. The type system must be expressive enough to describe every operator's output schema precisely.

**Powerful primitives (Belief 2).** A small set of types that compose cleanly is better than a large set of specialized types. Each type must earn its place by being necessary for correctness or performance.

---

## 2. The BqlType Enum

```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BqlType {
    Bool,
    Int,
    Float,
    String,
    Timestamp,
    List(Box<BqlType>),
    Map(Box<BqlType>),
}
```

### 2.1 Type Inventory

| BqlType | Rust representation | Arrow DataType | Domain examples |
|---------|-------------------|----------------|-----------------|
| `Bool` | `bool` | `Boolean` | conversion flags, retention periods |
| `Int` | `i64` | `Int64` | counts, durations (nanos), `step_reached`, `session_id` |
| `Float` | `f64` | `Float64` | `amount`, `latency`, percentiles |
| `String` | — | `Utf8View` | `user_id`, `event_type`, `device` |
| `Timestamp` | `i64` (epoch nanos) | `Timestamp(Nanosecond, Some("UTC"))` | `ts`, step timestamps |
| `List(T)` | — | `List(T.to_arrow())` | `match_events`, `tags` |
| `Map(V)` | — | `Map(Utf8View, V.to_arrow())` | flexible property bags |

### 2.2 Design Rationale

**Int is i64 only.** A single integer width eliminates promotion ambiguity and simplifies the type system. Counts, cardinalities, and session IDs all fit in i64. Storage-layer compression (delta encoding, FastLanes) recovers any space savings that narrower types would provide. Multiple int widths would create a combinatorial explosion in coercion rules for negligible query-level benefit.

**Float is f64 only.** Behavioral analytics involves amounts, latencies, scores, and percentiles — all need f64 precision. f32 would introduce precision loss in aggregations (summing millions of amounts) and require widening coercion rules. A single float width eliminates float promotion entirely.

**String uses Utf8View.** Arrow v54's `Utf8View` stores small strings inline (up to 12 bytes) and uses buffer references for larger ones. This eliminates the i32 offset limitation of `Utf8` (2 GB total per array) and provides better cache locality for short strings — event types and entity IDs are typically short. All strings are UTF-8. Binary data is out of scope for v1.

**Timestamp is always UTC nanoseconds.** The bootstrap spec requires nanosecond precision and i64 epoch nanos. Storing as UTC avoids timezone ambiguity in temporal comparisons, which is critical for correct pattern matching. Arrow mapping uses `Timestamp(Nanosecond, Some("UTC"))`. Display-time timezone conversion is a formatting concern, not a type concern.

**`Timestamp::MAX` is a reserved exclusive-bound sentinel, not a valid event time.** `bqlite` uses half-open `[start, end)` intervals for every `TimeRange`, including the "unbounded" range `[MIN, MAX)` returned by `TimeRange::unbounded`. To keep the exclusive upper bound representable without widening the stored integer, the maximum `i64` nanosecond value (`Timestamp::MAX`) is reserved: no event, ingest path, or test fixture may produce it. The maximum *valid* event timestamp is `Timestamp::MAX_VALID = Timestamp(i64::MAX - 1)`. Construction APIs whose semantics break at the sentinel — for example `TimeRange::instant(ts)`, which would need `ts + 1` as the exclusive end — return `Option<TimeRange>` and refuse the boundary input rather than silently collapsing to an empty range. In practice, `i64::MAX` nanoseconds is approximately the year 2262, so reserving this value has no effect on real workloads; the rule exists so the half-open math elsewhere in the engine stays clean.

**No Duration type.** Durations (e.g., `match_duration`, `session_duration`, timestamp differences) are represented as `Int` — nanoseconds as i64. In a domain-specific temporal query engine, the context makes durations unambiguous. A separate Duration type would add a variant to every type-dispatch site, complicate coercion and aggregate return-type rules, and provide marginal safety in a domain where every i64 from timestamp arithmetic is obviously a duration. Duration literals like `7d` and `30m` parse to i64 nanosecond values at plan time. Duration-specific display formatting (e.g., "2h 15m") belongs in the presentation layer, not the type system.

**List is homogeneously typed.** `List(BqlType)` requires all elements to share a type. This is mandated by Arrow's List type and is sufficient for the domain: a user-declared column like `tags` is `List(String)`, `page_views_per_session` could be `List(Int)`, and so on. **BQL has no `Struct` type**, so there is no `List(Struct)`. This is intentional — adding Struct would multiply every coercion, schema-validation, and Arrow-mapping rule to handle nested-field access. The operators that would otherwise need heterogeneously-typed list elements (notably ATTRIBUTE — see Section 6.14) auto-unnest into flat rows instead, which keeps the type system small without losing expressiveness.

**Map has String keys.** Event properties are accessed by name. Restricting keys to String simplifies key comparison and hashing, and matches Arrow's Map ergonomics. The value type is parameterized: `Map(Float)` for numeric property bags, `Map(String)` for string property bags.

**No ENTITY_KEY or EVENT_TYPE as distinct types.** These are semantically special columns but structurally `String` (or `Int` for entity keys). Semantic meaning is captured in `TableSchema` metadata — which columns are entity key, timestamp, event type — not in the type system. This keeps the enum small. The `TableSchema` records the `BqlType` of the entity key column, which can be `String` or `Int`.

---

## 3. Null Handling

### 3.1 Nullability Model

Every column has an explicit `nullable: bool` flag. When true, the column's Arrow array uses the null bitmap. When false, the bitmap is guaranteed empty.

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub bql_type: BqlType,
    pub nullable: bool,
    /// Default value for this column, used when reading segments written
    /// before the column was added. None means NULL (requires nullable=true).
    pub default_value: Option<PropertyValue>,
}
```

**System columns are never nullable.** The entity key, timestamp, and event type columns are implicitly `NOT NULL`. The parser rejects explicit `NULL` annotations on these. This is enforced at ingest time and provides guarantees to temporal operators that can skip null checks on these critical columns.

**Property columns are nullable by default.** `amount FLOAT` is equivalent to `amount FLOAT NULL`. Use `amount FLOAT NOT NULL` to require non-null values. This handles the common case where not every event has every property.

### 3.2 Null Propagation: SQL Three-Valued Logic

bqlite uses SQL three-valued logic (TRUE, FALSE, NULL). This is the established standard, well-understood, and Arrow's compute kernels already implement it.

| Expression | Result when operand is NULL |
|---|---|
| `x + NULL`, `x * NULL`, etc. | `NULL` |
| `x = NULL` | `NULL` (not `FALSE`) |
| `x != NULL` | `NULL` (not `TRUE`) |
| `NULL AND TRUE` | `NULL` |
| `NULL AND FALSE` | `FALSE` |
| `NULL OR TRUE` | `TRUE` |
| `NULL OR FALSE` | `NULL` |
| `NOT NULL` | `NULL` |
| `IS NULL` / `IS NOT NULL` | `TRUE` / `FALSE` (never `NULL`) |
| `COALESCE(NULL, x)` | `x` |

### 3.3 Nulls in Context

**Comparisons.** `WHERE amount > 100` excludes rows where `amount` is NULL. Standard SQL behavior, matching Arrow's comparison kernels.

**Aggregations.** NULL values are skipped by aggregate functions. `count(*)` counts all rows; `count(amount)` counts non-null values. `avg`, `sum`, `min`, `max`, and percentile functions all skip NULLs.

**Pattern matching.** A predicate like `WHERE amount > 50` does not match events where `amount` is NULL. A null property value is equivalent to "this event does not have this property" for predicate evaluation.

### 3.4 Explicit Null Testing

```sql
-- IS NULL / IS NOT NULL always produce non-null Bool
events | MATCH FIRST SEQUENCE(checkout WHERE discount IS NOT NULL THEN purchase) WITHIN 1h
```

### 3.5 COALESCE

```sql
... | STATS AVG(COALESCE(amount, 0.0))
```

`COALESCE(expr1, expr2, ...)` returns the first non-NULL argument. All arguments must be type-compatible (same type or implicitly coercible to a common type). The result type is the common type of all arguments. Follows standard SQL variadic semantics.

---

## 4. Type Coercion

### 4.1 Implicit Coercions

Minimal, safe, lossless. No surprises.

| From | To | Context | Rationale |
|---|---|---|---|
| `Int` | `Float` | Mixed arithmetic, comparison | Lossless widening for values in [-2^53, 2^53]. Values outside this range produce a planner warning. |
| String literal | `Timestamp` | Comparison (`ts > '2024-01-01'`) | ISO-8601 literals are parsed at plan time, not runtime. |
| Duration literal | `Int` | Time window (`WITHIN 7d`, `gap: 30m`) | Literals like `7d`, `30m`, `1h` are parsed to i64 nanoseconds at plan time. |

**No other implicit coercions.** Specifically:

- `Float` -> `Int`: not implicit (lossy). Use `CAST(x AS INT)` or `FLOOR(x)`.
- `String` -> `Int`/`Float`: not implicit. Use `CAST`.
- `Bool` -> `Int`: not implicit. Avoids `TRUE = 1` ambiguity.
- `Timestamp` -> `Int`: not implicit. Use `EPOCH_NANOS(ts)`.

### 4.2 Explicit Casts

```sql
CAST(expression AS type)
```

| From | To | Behavior |
|---|---|---|
| `Int` | `Float` | Exact if in [-2^53, 2^53]; planner warning otherwise |
| `Float` | `Int` | Truncates toward zero |
| `Bool` | `Int` | `TRUE -> 1`, `FALSE -> 0`, `NULL -> NULL` |
| `String` | `Int` | Parses decimal integer; `NULL` on failure |
| `String` | `Float` | Parses decimal float; `NULL` on failure |
| `String` | `Timestamp` | Parses RFC 3339 / ISO-8601 via `chrono::DateTime::parse_from_rfc3339`. Surrounding whitespace is trimmed. Non-UTC offsets (e.g. `+05:30`) are accepted and converted to UTC nanoseconds. Returns `NULL` on parse failure *or* if the parsed instant falls outside the nanosecond-representable range (~1677-09-21 .. 2262-04-11). |
| `String` | `Bool` | `"true"`/`"false"` case-insensitive; `NULL` otherwise |
| `Int` | `String` | Decimal string representation |
| `Float` | `String` | Standard float formatting |
| `Bool` | `String` | `"true"` / `"false"` |
| `Timestamp` | `String` | RFC 3339 / ISO-8601 UTC with trailing `Z`, formatted via `chrono::DateTime::<Utc>::to_rfc3339_opts(SecondsFormat::AutoSi, true)` — e.g. `2023-11-14T22:13:20Z` for whole seconds or `2023-11-14T22:13:20.123456789Z` with nanosecond precision. Output is round-trip stable: `CAST(CAST(ts AS STRING) AS TIMESTAMP) = ts` for every valid `Timestamp`. |
| `Timestamp` | `Int` | Epoch nanoseconds as i64 |
| `Int` | `Timestamp` | Interprets as epoch nanoseconds |

**Failed casts produce NULL, not errors.** Queries operate over large datasets where a few unparseable values should not halt execution. This follows TRY_CAST semantics by default.

The `Bool -> Int` cast makes `SUM(CAST(predicate AS INT))` the standard idiom for counting rows where a predicate is true.

### 4.3 Coercion in Comparisons

1. If one side is a literal and the other a column, coerce the literal to the column's type at plan time.
2. If both sides are columns, apply implicit coercion rules (`Int` -> `Float` only). If no implicit coercion exists, the planner rejects the query with a type error.
3. Regex matching (`~=`) requires the left operand to be `String`. The planner rejects `42 ~= "pattern"`.

### 4.4 Arithmetic Type Rules

| Left | Op | Right | Result |
|---|---|---|---|
| `Int` | `+ - * / %` | `Int` | `Int` |
| `Float` | `+ - * /` | `Float` | `Float` |
| `Int` | `+ - * /` | `Float` | `Float` (Int promoted) |
| `Timestamp` | `-` | `Timestamp` | `Int` (nanoseconds) |
| `Timestamp` | `+` | `Int` | `Timestamp` (Int interpreted as nanos) |
| `Timestamp` | `-` | `Int` | `Timestamp` (Int interpreted as nanos) |

Integer division (`Int / Int`) produces `Int`, truncated toward zero, consistent with SQL semantics. Use `CAST(x AS FLOAT) / y` for float division.

**Accepted tradeoff: Timestamp +/- Int is unguarded.** Because durations are plain `Int`, the planner cannot distinguish `ts + duration_nanos` from `ts + session_id`. Both type-check as `Timestamp + Int`. This is an accepted consequence of not having a separate Duration type — the column name and query context make the intent clear, and the alternative (a Duration type) would add complexity across the entire type system for marginal safety in this domain.

---

## 5. Schema System

### 5.1 TableSchema

The schema for a declared event table. Encodes column definitions plus metadata identifying the three mandatory column roles.

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSchema {
    /// Table name.
    pub name: String,

    /// Ordered list of column definitions.
    pub columns: Vec<ColumnDef>,

    /// Index into `columns` for the entity key column.
    pub entity_key_index: usize,

    /// Index into `columns` for the canonical event-time column.
    pub timestamp_index: usize,

    /// Index into `columns` for the event type column.
    pub event_type_index: usize,

    /// Monotonically increasing schema version. Incremented on each
    /// ALTER TABLE ADD COLUMN. Segments record the version they were
    /// written with so the scan layer can fill missing columns.
    pub version: u32,
}
```

**Declared vs logical schema.** `TableSchema.columns` stores the user-declared columns in DDL order. The logical table schema seen by planning and schema introspection also includes two implicit system columns:

| Column | Type | Nullable | Meaning |
|---|---|---|---|
| `__seq_id` | Int | no | Unique row identity assigned at ingest |
| `__batch_id` | Int | no | Ingest-batch identity assigned at ingest |

These names, and the full `__` prefix, are reserved for system use. System columns are selectable explicitly, appear in `DESCRIBE`, and may be used in predicates and `DELETE`, but they are excluded from `SELECT *` expansion and are never accepted as `INSERT` inputs.

**Validation rules enforced at schema creation time:**

1. Entity key column must be `String` or `Int`, non-nullable.
2. The canonical event-time column must be `Timestamp`, non-nullable.
3. Event type column must be `String`, non-nullable.
4. Column names must be unique (case-sensitive).
5. Column names must be valid identifiers (alphanumeric + underscore, not starting with digit).
6. A table must have at least the three mandatory columns.
7. `List` and `Map` types are allowed for property columns but not for the three mandatory columns.
8. User-declared column names may not start with `__`.

### 5.2 OperatorSchema

The output schema of a plan node — the contract between piped operators.

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorSchema {
    /// Ordered list of output columns.
    pub columns: Vec<ColumnDef>,
}
```

Key methods:

- `column(name) -> Option<(usize, &ColumnDef)>` — look up by name.
- `to_arrow_schema() -> arrow::datatypes::Schema` — convert for execution.
- `validate_against(required) -> Result<(), TypeError>` — check compatibility.

When an `OperatorSchema` contains system columns, wildcard expansion in `SELECT *` still excludes them. This is a query-language rule, not a schema omission.

### 5.3 Schema Evolution

**v1 supports adding columns.** `ALTER TABLE ADD COLUMN` appends a nullable column to an existing table. This is the only schema mutation supported in v1 — no column removal, no type changes, no renaming.

```sql
ALTER TABLE events ADD COLUMN category STRING
ALTER TABLE events ADD COLUMN score FLOAT NOT NULL DEFAULT 0.0
```

**Rules:**

1. Added columns must be nullable, OR must specify a `DEFAULT` value. Existing segments cannot retroactively populate a non-null column without a default.
2. The new column cannot be `ENTITY KEY`, `EVENT TYPE`, or `EVENT TIME` — the three mandatory column roles are immutable after table creation.
3. Column names must not conflict with existing columns.
4. `List` and `Map` columns can be added.

**Storage impact:** Existing segments are not rewritten. When reading a segment written before the column was added, the storage layer returns all-NULL (or the default value) for that column. The table metadata tracks a monotonically increasing **schema version**. Each segment records the schema version it was written with. The scan layer uses the current schema as the output schema, filling missing columns from older segments with NULL/default.

**Why not defer:** Adding a property column to an event table is a routine operation in analytics — new features ship, new events get new properties. Requiring DROP + recreate + re-ingest for a column addition is operationally painful and would block adoption for any non-trivial use case.

**Not supported in v1:** `DROP COLUMN`, `ALTER COLUMN TYPE`, `RENAME COLUMN`. These require segment rewriting or complex compatibility logic. They can be added later if needed.

---

## 6. Operator Output Schemas

Every operator produces a precisely defined output schema. These definitions are the contract that enables type-safe pipe composition.

**Naming convention:** All BQL keywords, operators, and functions are **ALL-CAPS** (MATCH, SESSIONIZE, STATS, WHERE, SELECT, WITHIN, BY, CAST, COALESCE, etc.). Event names, column names, and table names are user-defined and typically lowercase. This makes it easy to distinguish language constructs from data in a query. The parser accepts keywords case-insensitively.

### 6.1 MATCH / sequence

Without EMIT ALL, only entities that complete the full sequence appear in the output. With EMIT ALL, all entities that enter the NFA (match step 1) appear, including incomplete sequences. See sequence-matching.md Section 5.3 and Section 12 for full semantics.

| Column | Type | Nullable | Present | Description |
|---|---|---|---|---|
| `entity_id` | String or Int (matches entity key) | no | Always | Entity identifier |
| `$var` | (per variable type) | no | When variables are bound | One column per bound variable (e.g., `$plan`), named by the variable |
| *step-property columns* | (resolved from source schema) | follows source column | When downstream references `step_name.column` | One column per referenced named-step property — see below |
| `step_reached` | Int | no | When EMIT ALL is enabled | 1-indexed step number of the farthest step matched |
| `match_duration` | Int | yes | When demanded | First-to-last matched event time in nanoseconds (NULL if `step_reached == 1`) |
| `match_events` | Map(Timestamp) | yes | When demanded | Step name → timestamp of the matched event at that step (partial if incomplete) |

The `match_events` map keys are the event type names from the pattern (e.g., `"signup"`, `"purchase"`). When a pattern contains repeated event types, keys are disambiguated with a numeric suffix (e.g., `"page_view_0"`, `"page_view_1"`).

**Variable binding columns.** Each `$variable` in the pattern produces an output column with the bound value. The column type matches the source column's type (validated at plan time). Variable columns are non-nullable — only events with non-NULL binding values match the step predicate (sequence-matching.md Section 6.2).

**Named step property columns.** When a MATCH step is named (`s: signup THEN p: purchase`) and a downstream operator references a per-step property (`s.plan`, `p.amount`, `s.ts`), that property becomes a **first-class column** in the MATCH output schema at plan time. The column's type is resolved from the source table's schema for the step's event type (e.g., `s.plan` has the type of `signup.plan`). Nullability follows the source column. The column is only present when demanded — if nothing downstream references `s.plan`, it does not appear in the output schema.

Step property access is **per-(step, column)**, not per-(step, everything). Referencing `s.plan` adds only `s.plan` to the output schema; `s.country` remains absent unless separately referenced. This fine granularity is the basis for MATCH's column forwarding (planner-pipeline.md §8.2). The `match_events` map is not materialized by named step property access — only by explicit references to `match_events` itself or to `match_duration`.

If a referenced step name is not defined in the pattern, or the referenced column does not exist on the step's event type, the planner raises `TypeError::ColumnNotFound` with a context identifying the step (e.g., `"step 's' of MATCH pattern"`).

**Without EMIT ALL,** downstream pipelines do not need a `matched: Bool` column — every row is a completed match. A `STATS COUNT(*)` after MATCH counts matched entities directly. **With EMIT ALL,** the `step_reached` column distinguishes completed (`step_reached = num_steps`) from incomplete sequences.

### 6.2 FUNNEL and RETENTION

Funnels and retention are **convenience wrappers**, not operators with their own output schemas. They desugar into compositions of MATCH, STATS, and other primitives during logical planning. Their output schemas are determined by the primitives they expand into.

For example, a FUNNEL desugars into a series of progressively longer MATCH patterns with step-wise aggregation. RETENTION desugars into repeated MATCH queries across time brackets. The exact desugaring is specified in the query language design (TASK-002).

Because these are syntactic sugar, they do not appear in the operator output schema catalog. The planner expands them before schema validation occurs.

### 6.3 SESSIONIZE

Passes through all input columns, plus:

| Column | Type | Nullable | Description |
|---|---|---|---|
| `session_id` | Int | no | Monotonically increasing per entity |
| `session_duration` | Int | no | Session duration in nanoseconds |

### 6.4 STATS / aggregate

Output depends on GROUP BY and aggregate expressions:

- Group-by columns retain their input type and nullability.
- `COUNT(*)` -> `Int`, non-nullable.
- `COUNT(col)` -> `Int`, non-nullable (counts non-nulls).
- `COUNT_DISTINCT(col)` -> `Int`, non-nullable.
- `SUM(Int)` -> `Int`, nullable (NULL if all inputs NULL).
- `SUM(Float)` -> `Float`, nullable.
- `AVG(Int)` / `AVG(Float)` -> `Float`, nullable.
- `MIN(T)` / `MAX(T)` -> `T`, nullable.
- `P50(T)` / `P90(T)` / `P95(T)` / `P99(T)` -> `Float`, nullable.

**Type restrictions on aggregates:**

| Function | Accepts | Rejects |
|---|---|---|
| `COUNT`, `COUNT_DISTINCT` | any type | — |
| `SUM`, `AVG` | `Int`, `Float` | `Bool`, `String`, `Timestamp`, `List`, `Map` |
| `MIN`, `MAX` | `Int`, `Float`, `String`, `Timestamp` | `Bool`, `List`, `Map` |
| `P50`, `P90`, `P95`, `P99` | `Int`, `Float` | `Bool`, `String`, `Timestamp`, `List`, `Map` |

**Percentile implementation.** `P50`/`P90`/`P95`/`P99` are computed using **DDSketch** (bounded relative error, ~1–2 KB sketch per group, constant-time merge). DDSketch's merge operator makes percentile aggregates incrementally computable, which is load-bearing for fusion into upstream stateful operators (planner-pipeline.md §7.2, execution-model.md §8.4).

### 6.5 WHERE / filter

Passes through input schema unchanged. The filter predicate must evaluate to `Bool`.

### 6.6 SELECT / project

Projects to requested columns, preserving their types and nullability. Computed expressions get types inferred from the expression.

### 6.7 Event sub-selection (FIRST, LAST, NTH)

Per-entity operators that extract a specific event from the entity's event stream. The output schema matches the source table's columns — each row is a single event.

```sql
-- First purchase per entity
events | FIRST(purchase)

-- Last event from a filtered set
events | LAST(page_view WHERE url LIKE '/checkout%')

-- Nth occurrence
events | NTH(page_view, 3)
```

| Column | Type | Nullable | Description |
|---|---|---|---|
| `entity_id` | String or Int | no | Entity identifier |
| `ts` | Timestamp | no | Timestamp of the selected event |
| `event_type` | String | no | Event type of the selected event |
| *(property columns)* | *(from table schema)* | *(from table schema)* | All property columns from the source table |

The output has exactly one row per entity (entities with no matching event are omitted). This means sub-selection results compose naturally with other operators: pipe into STATS for aggregation, into WHERE for filtering on properties of the selected event, or use in an IN clause as a cohort.

### 6.8 Window functions (OVER)

Window functions compute values across the entity's ordered event stream without collapsing rows. They pass through all input columns and add computed columns.

```sql
-- Time since previous event per entity
events
  | SESSIONIZE(gap: 30m)
  | SELECT *, LAG(ts, 1) OVER (ORDER BY ts) AS prev_ts

-- Running purchase count per entity
events
  | WHERE event_type = 'purchase'
  | SELECT *, ROW_NUMBER() OVER (ORDER BY ts) AS purchase_num
```

Output: all input columns, plus the window function result column. The added column's type depends on the function:

| Function | Result type | Description |
|---|---|---|
| `LAG(col, n)` | same as `col`, nullable | Value of `col` from `n` rows prior (NULL at stream start) |
| `LEAD(col, n)` | same as `col`, nullable | Value of `col` from `n` rows ahead (NULL at stream end) |
| `ROW_NUMBER()` | Int, non-nullable | 1-based position within the entity's stream |
| `RANK()` | Int, non-nullable | Rank with gaps for ties |
| `SUM(col) OVER ...` | same as `SUM(col)` | Running sum |
| `AVG(col) OVER ...` | Float, nullable | Running average |
| `COUNT(*) OVER ...` | Int, non-nullable | Running count |
| `MIN(col) OVER ...` / `MAX(col) OVER ...` | same as `col`, nullable | Running min/max |

Window functions always partition by the entity key implicitly (BQL is an entity-first query language). The `OVER` clause accepts an optional `PARTITION BY` to subdivide further and an optional `ORDER BY` (defaults to the timestamp column).

### 6.9 IN (subquery filtering)

Filters rows where a tuple of columns matches results from a subquery. This is the primary mechanism for cohort-style composition — cohorts are just queries that produce entity IDs, not special objects.

```sql
-- Purchases by entities who signed up in January
events
  | WHERE event_type = 'purchase'
  | WHERE entity_id IN (
      events
      | WHERE event_type = 'signup' AND ts >= '2024-01-01' AND ts < '2024-02-01'
      | SELECT entity_id
    )

-- Events from entities who completed a funnel, using an alias
converted_users = events
  | MATCH FIRST SEQUENCE(signup THEN add_to_cart THEN purchase) WITHIN 7d
  | SELECT entity_id

events
  | WHERE event_type = 'support_ticket'
  | WHERE entity_id IN converted_users
```

Output schema: passes through input schema unchanged. The IN clause is a filter — it reduces rows but does not alter columns.

**Type rules:** The column tuple on the left must type-match the corresponding columns from the subquery. Typically this is a single entity ID column (`String` or `Int`), but multi-column IN is supported for compound keys.

### 6.10 PIVOT

Reshapes long-form results into wide-form by turning values of a pivot column into separate output columns.

```sql
-- Retention as wide-form: one row per entity, one column per bracket
events
  | MATCH FIRST SEQUENCE(signup THEN purchase) BRACKETS [1d, 7d, 30d] EMIT ALL
  | STATS retained = SUM(CAST(step_reached >= 2 AS INT)) GROUP BY bracket
  | PIVOT bracket ON retained
```

Output schema: group-by columns, plus one new column per distinct value in the pivot column. The new column types match the value column's type. The set of distinct values must be known at plan time (provided as a literal list, or inferred from the query structure for operators like RETENTION that produce a fixed set of values).

| Column | Type | Nullable | Description |
|---|---|---|---|
| *(group-by columns)* | *(from input)* | *(from input)* | Retained as-is |
| *(pivot_value_1)* | same as value column | yes | Value for first pivot category |
| *(pivot_value_2)* | same as value column | yes | Value for second pivot category |
| ... | ... | ... | ... |

Pivot columns are nullable because not every group may have a value for every pivot category.

### 6.11 SAMPLE

Random sampling of entities. Reduces the entity set to a fraction or fixed count before processing.

```sql
-- 10% random sample of entities
events
  | SAMPLE(fraction: 0.1)
  | MATCH FIRST SEQUENCE(signup THEN purchase) WITHIN 7d

-- Fixed sample size
events | SAMPLE(count: 10000)
```

Output schema: passes through input schema unchanged. SAMPLE is a scan-level operator — it filters entities early to avoid processing the full dataset.

### 6.12 ORDER BY

Passes through input schema unchanged. The sort column must exist in the input schema and its type must support ordering — all scalar types (`Bool`, `Int`, `Float`, `String`, `Timestamp`) are orderable; `List` and `Map` are not.

### 6.13 LIMIT

Passes through input schema unchanged.

### 6.14 ATTRIBUTE

Finds touchpoint events preceding each conversion within a time window. See query-language.md Section 14.3 for surface syntax and planner-pipeline.md Section 13 for execution semantics.

**Output schema**: one row per `(entity_id, conversion, matched-touchpoint)`. ATTRIBUTE auto-unnests — it emits flat rows, not a list column.

| Column | Type | Nullable | Present | Description |
|---|---|---|---|---|
| `entity_id` | String or Int (matches entity key) | no | Always | Entity identifier |
| `conversion_ts` | Timestamp | no | Always | Conversion event's timestamp |
| *conversion properties* | (resolved from source schema) | follows source | When downstream references `<conversion_event_type>.<column>` | Demand-driven forwarded conversion properties |
| `touchpoint_ts` | Timestamp | **yes** | Always | Touchpoint timestamp; `NULL` when no touchpoint qualified for this conversion |
| `touchpoint_key` | String | **yes** | Always | Result of the `touchpoint_key` expression; `NULL` when no touchpoint qualified |

**`touchpoint_key` typing.** The `touchpoint_key` expression in the ATTRIBUTE parameters must evaluate to `String`. Any other type is a plan-time error; use `CAST(expr AS STRING)` to forward non-string columns. The expression is type-checked against the touchpoint event type's schema only — it cannot reference conversion properties or columns from other event types.

**Conversion property forwarding.** Properties of the conversion event are accessed downstream using the conversion event type as a prefix — for `conversion: purchase`, downstream writes `purchase.amount`. This mirrors MATCH's bare-step property access (Section 6.1). Only referenced properties are materialized. If the conversion event type's name collides with a column name on the source table, the planner raises `TypeError::NameCollision` (Section 12).

**LEFT-UNNEST semantics.** Conversions with no qualifying touchpoints still emit one row, with `touchpoint_ts = NULL` and `touchpoint_key = NULL`. This preserves un-attributed conversions so they can be counted. Use `WHERE touchpoint_ts IS NOT NULL` downstream to drop them for INNER-join semantics.

**Row cardinality.** For an entity with K conversions and, on average, N qualifying touchpoints per conversion (within `window`), the operator emits `K * max(N, 1)` rows. The `max(N, 1)` accounts for the LEFT-UNNEST row emitted for un-attributed conversions.

**Why no separate UNNEST operator in BQL.** Earlier designs of ATTRIBUTE emitted a `List(Struct)` of touchpoints per conversion and relied on a generic UNNEST operator to flatten it. BQL's type system doesn't have `Struct`, and `Map(V)` has a single value type, so a list of heterogeneously-typed touchpoint fields has no natural representation. Auto-unnesting sidesteps the type-system gap: ATTRIBUTE itself is the "unnest", the output is flat rows with a single typed `touchpoint_key` column, and the language needs no `Struct` type or `UNNEST` operator. All common attribution models (last-touch, first-touch, equal-weight, time-decay, position-based) are expressible with window functions and standard aggregates on the flat row form.

---

## 7. Arrow Type Mapping

### 7.1 BqlType to Arrow

```rust
impl BqlType {
    pub fn to_arrow(&self) -> arrow::datatypes::DataType {
        match self {
            BqlType::Bool      => DataType::Boolean,
            BqlType::Int       => DataType::Int64,
            BqlType::Float     => DataType::Float64,
            BqlType::String    => DataType::Utf8View,
            BqlType::Timestamp => DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            BqlType::List(elem) => DataType::List(
                Arc::new(Field::new("item", elem.to_arrow(), true))
            ),
            BqlType::Map(value_type) => {
                let entries = DataType::Struct(vec![
                    Field::new("key", DataType::Utf8View, false),
                    Field::new("value", value_type.to_arrow(), true),
                ].into());
                DataType::Map(Arc::new(Field::new("entries", entries, false)), false)
            }
        }
    }
}
```

### 7.2 Arrow to BqlType

Used during ingest from Parquet/CSV and when interfacing with external Arrow data.

| Arrow DataType | BqlType | Notes |
|---|---|---|
| `Boolean` | `Bool` | |
| `Int8`, `Int16`, `Int32`, `Int64`, `UInt8`, `UInt16`, `UInt32` | `Int` | Widened to i64 at ingest |
| `UInt64` | `Int` | Accepted; flag at ingest if value > i64::MAX |
| `Float16`, `Float32`, `Float64` | `Float` | Widened to f64 at ingest |
| `Utf8`, `LargeUtf8`, `Utf8View` | `String` | |
| `Dictionary(_, Utf8*)` | `String` | Dictionary-encoded strings |
| `Timestamp(_, _)` | `Timestamp` | Converted to UTC nanos at ingest |
| `Date32`, `Date64` | `Timestamp` | Converted to nanos at ingest |
| `Duration(_)` | `Int` | Converted to nanos at ingest |
| `List(field)`, `LargeList(field)` | `List(from_arrow(field))` | Recursive |
| `Map(field, _)` | `Map(from_arrow(value_field))` | Expects Struct{key, value} |
| Everything else | — | Rejected at ingest with error |

**Integer width consolidation on ingest.** Narrower Arrow integer types are widened to Int64. This costs negligible storage (compression recovers it) and eliminates width-related complexity throughout the query engine.

**Timestamp normalization on ingest.** Timestamps in any Arrow TimeUnit are converted to nanoseconds. Timezone-naive timestamps are treated as UTC (with a warning). Timestamps with a non-UTC timezone are converted to UTC.

### 7.3 Round-Trip Guarantee

For all BqlType variants, `BqlType::from_arrow(&bql_type.to_arrow()) == Some(bql_type)`. This is a testable invariant.

---

## 8. PropertyValue (Dynamic Typing)

For the ingest path and memtable — before data reaches columnar Arrow arrays — a dynamic value type is needed.

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PropertyValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Timestamp(i64),        // epoch nanos
    List(Vec<PropertyValue>),
    Map(Vec<(String, PropertyValue)>),
}
```

**Where it appears:**

1. **Ingest.** Raw data (CSV, JSON, Parquet) is parsed into `PropertyValue`, validated against `TableSchema`, then written to columnar storage.
2. **Test fixtures.** `input.json` and `expected.json` map naturally to `PropertyValue`.

**Where it does NOT appear:** the query execution hot path. Once data is in columnar storage or flowing through the query engine, everything is Arrow arrays. `PropertyValue` is a boundary type, not a runtime type.

Key methods:

- `bql_type() -> Option<BqlType>` — returns `None` for `Null`.
- `coerce_to(target: &BqlType) -> Option<PropertyValue>` — attempts type conversion; returns `None` if impossible.

---

## 9. BQL Schema Declaration Syntax

### 9.1 CREATE TABLE

```sql
CREATE TABLE events (
    user_id STRING ENTITY KEY,
    ts TIMESTAMP EVENT TIME,
    event_type STRING EVENT TYPE,
    amount FLOAT,
    query STRING,
    device STRING,
    tags LIST(STRING),
    metadata MAP(STRING)
)
```

### 9.2 Grammar

```
create_table     := "CREATE" "TABLE" identifier "(" column_def ("," column_def)* ")"
alter_table      := "ALTER" "TABLE" identifier "ADD" "COLUMN" identifier type_expr alter_modifier*
column_def       := identifier type_expr column_modifier*
type_expr        := scalar_type | composite_type
scalar_type      := "BOOL" | "INT" | "FLOAT" | "STRING" | "TIMESTAMP"
composite_type   := "LIST" "(" type_expr ")" | "MAP" "(" type_expr ")"
column_modifier  := "ENTITY" "KEY" | "EVENT" "TYPE" | "EVENT" "TIME" | "NOT" "NULL" | "NULL"
alter_modifier   := "NOT" "NULL" | "NULL" | "DEFAULT" literal
```

### 9.3 Rules

- Exactly one column must have `ENTITY KEY`. Must be `STRING` or `INT`.
- Exactly one column must have `EVENT TYPE`. Must be `STRING`.
- If exactly one `TIMESTAMP` column exists and none is annotated, it is inferred as the `EVENT TIME` column.
- If multiple `TIMESTAMP` columns exist, exactly one must be annotated `EVENT TIME`.
- `ENTITY KEY`, `EVENT TYPE`, and the `EVENT TIME` column are implicitly `NOT NULL`.
- Property columns are `NULL` by default. `NOT NULL` overrides.
- User-declared column names starting with `__` are rejected because the prefix is reserved for implicit system columns.
- Type names are case-insensitive (`string`, `STRING`, `String` all valid).

### 9.4 DESCRIBE TABLE

```sql
DESCRIBE events
```

Output columns: name, type, nullable, role (entity_key / event_time / event_type / property / system).

---

## 10. Scalar Functions

Scalar functions operate on individual values and can appear anywhere an expression is expected — in predicates, projections, and aggregate arguments. Each scalar function has a typed signature: input types and a return type, validated at plan time.

### 10.1 Type Signature Model

```rust
pub struct ScalarFunctionSig {
    pub name: String,
    /// Expected argument types. `None` means any type is accepted for that position.
    pub arg_types: Vec<Option<BqlType>>,
    /// The return type, possibly derived from the input types.
    pub return_type: BqlType,
    pub nullable: bool,
}
```

For functions where the return type depends on the input (e.g., `COALESCE`), the planner computes the return type during type checking rather than using a static signature.

### 10.2 Initial Scalar Functions

This is the initial set. Additional functions will be specified in the query language design (TASK-002).

**Temporal:**

| Function | Signature | Description |
|---|---|---|
| `QUANTIZE(Timestamp, Int)` | `Timestamp` | Truncate timestamp to bucket boundaries (Int is bucket width in nanos, buckets aligned to UTC) |
| `QUANTIZE(Timestamp, Int, String)` | `Timestamp` | Timezone-aware bucketing — third argument is IANA timezone (e.g., `'America/Los_Angeles'`). Buckets align to local midnight/boundaries in the given timezone, result remains UTC. |
| `EPOCH_NANOS(Timestamp)` | `Int` | Extract epoch nanoseconds |
| `EPOCH_SECONDS(Timestamp)` | `Float` | Extract epoch seconds (float for sub-second precision) |

Timezone-aware `QUANTIZE` is essential for analytics: "by day" means different boundaries in `America/New_York` vs `Asia/Tokyo`. Without it, daily/weekly aggregations silently produce wrong results for non-UTC users. The two-argument form defaults to UTC alignment. The timezone string is validated at plan time — invalid IANA names produce a planner error.

**Numeric bucketing:**

| Function | Signature | Description |
|---|---|---|
| `QUANTIZE(Int, Int)` | `Int` | Floor-divide numeric value by bucket width, multiply back: `(v / w) * w`. Bucket width must be positive. |
| `QUANTIZE(Float, Float)` | `Float` | Same semantics for floating-point values. |
| `QUANTIZE(Int, Float)` | `Float` | Int is implicitly promoted to Float. |

The numeric overload of `QUANTIZE` shares a name with the temporal overload but uses the scalar bucket-width type to disambiguate at plan time. `QUANTIZE(amount, 100)` produces buckets `0, 100, 200, ...`; `QUANTIZE(price, 0.05)` produces 5-cent price buckets.

**String:**

| Function | Signature | Description |
|---|---|---|
| `LENGTH(String)` | `Int` | UTF-8 character count |
| `SUBSTRING(String, Int, Int)` | `String` | Substring by start position and length |
| `CONCAT(String, String, ...)` | `String` | Variadic string concatenation |
| `LOWER(String)` | `String` | Lowercase |
| `UPPER(String)` | `String` | Uppercase |
| `TRIM(String)` | `String` | Strip leading/trailing whitespace |

**Numeric:**

| Function | Signature | Description |
|---|---|---|
| `ABS(Int) -> Int`, `ABS(Float) -> Float` | — | Absolute value (return type matches input) |
| `FLOOR(Float)` | `Int` | Floor to integer |
| `CEIL(Float)` | `Int` | Ceiling to integer |
| `ROUND(Float, Int)` | `Float` | Round to N decimal places |
| `EXP(Float)` | `Float` | Natural exponential |
| `LN(Float)` | `Float` | Natural logarithm |
| `POW(Float, Float)` | `Float` | Exponentiation |

**Null handling:**

| Function | Signature | Description |
|---|---|---|
| `COALESCE(T, T, ...)` | `T` | First non-NULL argument (variadic) |
| `IF(Bool, T, T)` | `T` | Conditional expression |

Scalar functions are registered in a function registry. The planner resolves function calls against this registry during type checking. Unrecognized function names produce a clear error. The registry is extensible — new functions can be added without modifying the type system.

---

## 11. Variable Binding Types

In pattern matching, variables bind a value from one step and enforce equality in subsequent steps:

```sql
events | MATCH FIRST SEQUENCE(
    view WHERE category = $c THEN purchase WHERE category = $c
  ) WITHIN 7d
```

### 11.1 Type Inference Rules

1. The first occurrence of `$c` in `category = $c` binds `$c` to the type of `category` (looked up in the table schema).
2. Subsequent uses must appear in equality predicates against columns of the same type.
3. If `$c` first binds to `String` (from `category`) and later appears in `WHERE price = $c` where `price` is `Float`, the planner reports a `VariableTypeConflict` error.
4. Variable names: `$` followed by one or more alphanumeric characters or underscores.
5. Variables are scoped to a single MATCH / FUNNEL expression. They do not leak across pipe stages.

### 11.2 Planner Implementation

The planner maintains a `HashMap<String, BqlType>` as the variable binding environment during type checking of a pattern expression. On first encounter, the variable is inserted. On subsequent encounters, the type is checked for equality.

At execution time, the sequence matcher stores the bound value and checks equality for subsequent steps. The type system guarantees this comparison is always well-typed.

---

## 12. Type Errors

All type errors are detected at plan construction time and reported with enough context to locate the problem in the query.

```rust
#[derive(Debug, thiserror::Error)]
pub enum TypeError {
    #[error("column '{column}' not found in {context}; available columns: {available}")]
    ColumnNotFound {
        column: String,
        context: String,
        available: String,
    },

    #[error("type mismatch: '{column}' is {actual}, but {operation} requires {expected}")]
    TypeMismatch {
        column: String,
        actual: BqlType,
        expected: BqlType,
        operation: String,
    },

    #[error("schema mismatch: {operator} expects {expected}, but received {actual}")]
    SchemaMismatch {
        operator: String,
        expected: String,
        actual: String,
    },

    #[error("cannot apply {operation} to {left_type} and {right_type}")]
    IncompatibleOperands {
        operation: String,
        left_type: BqlType,
        right_type: BqlType,
    },

    #[error("{function} requires numeric input, got {actual_type} for '{column}'")]
    InvalidAggregateInput {
        function: String,
        column: String,
        actual_type: BqlType,
    },

    #[error("regex match requires STRING operand, got {actual_type} for '{column}'")]
    RegexOnNonString {
        column: String,
        actual_type: BqlType,
    },

    #[error("invalid table schema: {reason}")]
    InvalidSchema {
        reason: String,
    },

    #[error("variable '${variable}' bound to {first_type} in {first_step}, \
             but used with {second_type} in {second_step}")]
    VariableTypeConflict {
        variable: String,
        first_type: BqlType,
        first_step: String,
        second_type: BqlType,
        second_step: String,
    },

    #[error("alias cycle detected: {path}")]
    AliasCycle {
        /// Dot-separated alias path showing the cycle, e.g. "A -> B -> C -> A".
        path: String,
    },

    #[error("step '{step_name}' not found in MATCH pattern (available: {available})")]
    StepNotFound {
        step_name: String,
        available: String,
    },

    #[error("name collision in {context}: '{name}' would be defined twice")]
    NameCollision {
        /// The colliding name.
        name: String,
        /// Where the collision occurred, e.g. "FUNNEL step outputs",
        /// "SELECT aliases", "STATS output names", "JOIN result columns".
        context: String,
    },
}
```

**`NameCollision` usage.** This variant covers all cases where the planner would need to produce two output columns with the same name and cannot silently pick one:

- **FUNNEL step outputs.** A FUNNEL with repeated event types and no step names (`signup THEN signup`) would produce two `signup = SUM(...)` aggregates. The planner raises `NameCollision { name: "signup", context: "FUNNEL step outputs" }`; the user must add step names to disambiguate (`s1: signup THEN s2: signup`).
- **SELECT aliases.** `| SELECT a AS x, b AS x` is caught at the planner level when both aliases are user-written. Bare column references (`| SELECT x, x`) are a parser-level duplicate which the parser also rejects.
- **STATS output names.** `| STATS total = COUNT(*), total = SUM(x)` raises the same collision.
- **JOIN result columns.** A cross-table JOIN whose schema-combining step would produce two columns with the same name (despite the required table qualifier rule in query-language.md §19.1) — this is a defensive check.
- **LET rebinding.** `| LET x = a | LET x = b` also raises this error (query-language.md §11 forbids rebinding).

### 12.1 Validation Sequence

When the planner constructs a plan from an AST:

1. **Table resolution.** Verify the table exists, retrieve `TableSchema`.
2. **Predicate type checking.** For each predicate, resolve column references against the schema, verify the operation is valid for the column type, apply literal coercion.
3. **Variable binding.** For held-property variables, the first binding site determines the type. Subsequent uses are checked for type equality.
4. **Pipe composition.** When operator A pipes into operator B, validate that A's output schema satisfies B's input requirements:
   - WHERE: referenced columns must exist, predicate must produce `Bool`.
   - STATS: group-by columns must exist; aggregate functions must accept their input types.
   - SELECT: columns must exist or be computable expressions.
   - ORDER BY: column must exist and its type must support ordering (all scalar types; `List` and `Map` are not orderable).
5. **Error reporting.** All errors include column name, expected vs. actual type, and enough context to pinpoint the problem.

---

## 13. Crate Placement

All types defined in this document belong in `bqlite-core`:

- `BqlType`
- `ColumnDef`
- `TableSchema`
- `OperatorSchema`
- `PropertyValue`
- `TypeError`
- Arrow conversion methods

This follows the dependency rule: `bqlite-core` has no internal dependencies and is imported by all other crates. The planner uses `OperatorSchema` and `TypeError` for validation. Storage uses `TableSchema` and `PropertyValue` for ingest. The AST uses `BqlType` for type annotations.

---

## 14. Decision Summary

| Question | Decision | Rationale |
|---|---|---|
| Int precision | i64 only | Simplicity; compression handles storage |
| Float precision | f64 only | Aggregation precision; no widening rules |
| String encoding | UTF-8 via Utf8View | Arrow v54 perf for short strings |
| Timestamp timezone | Always UTC, nanosecond | Correct comparisons; convert on ingest |
| List element types | Homogeneous | Arrow requirement; sufficient for domain |
| Map key/value types | String keys, typed values | Properties are name-accessed |
| Duration | No separate type; Int (nanos) | Context is unambiguous in temporal domain; avoids type proliferation |
| Null semantics | SQL three-valued logic | Standard; Arrow kernels implement it |
| Implicit coercion | Minimal (Int->Float, literal->typed) | No surprises; explicit CAST for lossy |
| Failed casts | Produce NULL | TRY_CAST semantics for large datasets |
| Schema evolution | ADD COLUMN only in v1 | Column addition is routine in analytics; no segment rewrite needed |
| PropertyValue scope | Ingest/memtable only | Off the hot path |
| Cohorts | Not a special type; entity ID sets via IN subqueries | Composable without new abstractions |
| BqlType enum size | 7 variants | Small, composable, each earns its place |

---

## 15. Open Questions for Other Design Docs

These questions are intentionally deferred to the design docs that own them:

- **Storage (TASK-001):** How does type information drive per-column compression codec selection? How are `List` and `Map` columns encoded in segments? How does schema versioning interact with segment metadata — what is the format for recording which schema version a segment was written with, and how does the scan layer efficiently fill missing columns?
- **Query Language (TASK-002):** Exact CAST syntax. Interaction between type coercion and cohort/alias composition rules. Type annotations in result aliases. Complete scalar function catalog beyond the initial set defined here.
- **Execution Model (TASK-003):** How does type dispatch work in vectorized operator kernels? Does the engine monomorphize per-type or use enum dispatch?
- **Sequence Matching (TASK-004):** How are held-property variable values stored during NFA evaluation? What is the memory cost per bound variable per active entity?
