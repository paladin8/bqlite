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
| `List(T)` | — | `List(T.to_arrow())` | `match_events`, `path` |
| `Map(V)` | — | `Map(Utf8View, V.to_arrow())` | flexible property bags |

### 2.2 Design Rationale

**Int is i64 only.** A single integer width eliminates promotion ambiguity and simplifies the type system. Counts, cardinalities, and session IDs all fit in i64. Storage-layer compression (delta encoding, FastLanes) recovers any space savings that narrower types would provide. Multiple int widths would create a combinatorial explosion in coercion rules for negligible query-level benefit.

**Float is f64 only.** Behavioral analytics involves amounts, latencies, scores, and percentiles — all need f64 precision. f32 would introduce precision loss in aggregations (summing millions of amounts) and require widening coercion rules. A single float width eliminates float promotion entirely.

**String uses Utf8View.** Arrow v54's `Utf8View` stores small strings inline (up to 12 bytes) and uses buffer references for larger ones. This eliminates the i32 offset limitation of `Utf8` (2 GB total per array) and provides better cache locality for short strings — event types and entity IDs are typically short. All strings are UTF-8. Binary data is out of scope for v1.

**Timestamp is always UTC nanoseconds.** The bootstrap spec requires nanosecond precision and i64 epoch nanos. Storing as UTC avoids timezone ambiguity in temporal comparisons, which is critical for correct pattern matching. Arrow mapping uses `Timestamp(Nanosecond, Some("UTC"))`. Display-time timezone conversion is a formatting concern, not a type concern.

**No Duration type.** Durations (e.g., `match_duration`, `session_duration`, timestamp differences) are represented as `Int` — nanoseconds as i64. In a domain-specific temporal query engine, the context makes durations unambiguous. A separate Duration type would add a variant to every type-dispatch site, complicate coercion and aggregate return-type rules, and provide marginal safety in a domain where every i64 from timestamp arithmetic is obviously a duration. Duration literals like `7d` and `30m` parse to i64 nanosecond values at plan time. Duration-specific display formatting (e.g., "2h 15m") belongs in the presentation layer, not the type system.

**List is homogeneously typed.** `List(BqlType)` requires all elements to share a type. This is mandated by Arrow's List type and is sufficient for the domain: `match_events` is `List(Struct)`, `step_timestamps` is `List(Timestamp)`, `path` is `List(String)`.

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
MATCH(checkout WHERE discount IS NOT NULL -> purchase) WITHIN 1h BY user_id
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
| `String` | `Int` | Parses decimal integer; `NULL` on failure |
| `String` | `Float` | Parses decimal float; `NULL` on failure |
| `String` | `Timestamp` | Parses ISO-8601; `NULL` on failure |
| `String` | `Bool` | `"true"`/`"false"` case-insensitive; `NULL` otherwise |
| `Int` | `String` | Decimal string representation |
| `Float` | `String` | Standard float formatting |
| `Bool` | `String` | `"true"` / `"false"` |
| `Timestamp` | `String` | ISO-8601 UTC format |
| `Timestamp` | `Int` | Epoch nanoseconds as i64 |
| `Int` | `Timestamp` | Interprets as epoch nanoseconds |

**Failed casts produce NULL, not errors.** Queries operate over large datasets where a few unparseable values should not halt execution. This follows TRY_CAST semantics by default.

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

    /// Index into `columns` for the timestamp column.
    pub timestamp_index: usize,

    /// Index into `columns` for the event type column.
    pub event_type_index: usize,

    /// Monotonically increasing schema version. Incremented on each
    /// ALTER TABLE ADD COLUMN. Segments record the version they were
    /// written with so the scan layer can fill missing columns.
    pub version: u32,
}
```

**Validation rules enforced at schema creation time:**

1. Entity key column must be `String` or `Int`, non-nullable.
2. Timestamp column must be `Timestamp`, non-nullable.
3. Event type column must be `String`, non-nullable.
4. Column names must be unique (case-sensitive).
5. Column names must be valid identifiers (alphanumeric + underscore, not starting with digit).
6. A table must have at least the three mandatory columns.
7. `List` and `Map` types are allowed for property columns but not for the three mandatory columns.

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

### 5.3 Schema Evolution

**v1 supports adding columns.** `ALTER TABLE ADD COLUMN` appends a nullable column to an existing table. This is the only schema mutation supported in v1 — no column removal, no type changes, no renaming.

```sql
ALTER TABLE events ADD COLUMN category STRING
ALTER TABLE events ADD COLUMN score FLOAT NOT NULL DEFAULT 0.0
```

**Rules:**

1. Added columns must be nullable, OR must specify a `DEFAULT` value. Existing segments cannot retroactively populate a non-null column without a default.
2. The new column cannot be `ENTITY KEY`, `EVENT TYPE`, or `TIMESTAMP` — the three mandatory column roles are immutable after table creation.
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

Only entities that match the pattern appear in the output. Unmatched entities are not emitted — there is no `matched: Bool` column. Downstream operators that need a boolean "did this entity match?" signal use the presence/absence of the entity in the result set.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `entity_id` | String or Int (matches entity key) | no | Entity identifier |
| `match_duration` | Int | no | First-to-last matched event time in nanoseconds |
| `match_events` | Map(Timestamp) | no | Step name -> timestamp of the matched event at that step |

The `match_events` map keys are the event type names from the pattern (e.g., `"signup"`, `"purchase"`). When a pattern contains repeated event types, keys are disambiguated with a numeric suffix (e.g., `"page_view_0"`, `"page_view_1"`).

**Note:** Since MATCH only emits matched entities, downstream pipelines do not need a `matched: Bool` column. The bootstrap example `| WHERE matched = true` is unnecessary — every row in the MATCH output is a match. A `STATS COUNT(*)` after MATCH counts matched entities directly.

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

### 6.5 WHERE / filter

Passes through input schema unchanged. The filter predicate must evaluate to `Bool`.

### 6.6 SELECT / project

Projects to requested columns, preserving their types and nullability. Computed expressions get types inferred from the expression.

### 6.7 PATHS

| Column | Type | Nullable | Description |
|---|---|---|---|
| `entity_id` | String or Int | no | |
| `path` | List(String) | no | Sequence of event types traversed |
| `path_length` | Int | no | Number of steps |

### 6.8 Event sub-selection (FIRST, LAST, NTH)

Per-entity operators that extract a specific event from the entity's event stream. The output schema matches the source table's columns — each row is a single event.

```sql
-- First purchase per user
FIRST(purchase) BY user_id

-- Last event before churn (within a MATCH pipeline)
MATCH(signup -> purchase -> churn) WITHIN 30d BY user_id
  | LAST(purchase)

-- Nth occurrence
NTH(page_view, 3) BY user_id
```

| Column | Type | Nullable | Description |
|---|---|---|---|
| `entity_id` | String or Int | no | Entity identifier |
| `ts` | Timestamp | no | Timestamp of the selected event |
| `event_type` | String | no | Event type of the selected event |
| *(property columns)* | *(from table schema)* | *(from table schema)* | All property columns from the source table |

The output has exactly one row per entity (entities with no matching event are omitted). This means sub-selection results compose naturally with other operators: pipe into STATS for aggregation, into WHERE for filtering on properties of the selected event, or use in an IN clause as a cohort.

### 6.9 Window functions (OVER)

Window functions compute values across the entity's ordered event stream without collapsing rows. They pass through all input columns and add computed columns.

```sql
-- Time since previous event per entity
SESSIONIZE(gap: 30m) BY user_id
  | SELECT *, LAG(ts, 1) OVER (BY user_id ORDER BY ts) AS prev_ts

-- Running purchase count per user
WHERE event_type = 'purchase'
  | SELECT *, ROW_NUMBER() OVER (BY user_id ORDER BY ts) AS purchase_num
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

Window functions require `OVER (BY entity_key ORDER BY col)`. The `BY` clause is implicit (always the entity key) and can be omitted. The `ORDER BY` clause defaults to the timestamp column.

### 6.10 IN (subquery filtering)

Filters rows where a tuple of columns matches results from a subquery. This is the primary mechanism for cohort-style composition — cohorts are just queries that produce entity IDs, not special objects.

```sql
-- Purchases by users who signed up in January
WHERE event_type = 'purchase' BY user_id
  | WHERE (user_id) IN (
      WHERE event_type = 'signup' AND ts >= '2024-01-01' AND ts < '2024-02-01'
        BY user_id
      | SELECT entity_id
    )

-- Events from users who completed a funnel
MATCH(signup -> add_to_cart -> purchase) WITHIN 7d BY user_id
  | SELECT entity_id AS converted_users

WHERE event_type = 'support_ticket' BY user_id
  | WHERE (user_id) IN (converted_users)
```

Output schema: passes through input schema unchanged. The IN clause is a filter — it reduces rows but does not alter columns.

**Type rules:** The column tuple on the left must type-match the corresponding columns from the subquery. Typically this is a single entity ID column (`String` or `Int`), but multi-column IN is supported for compound keys.

### 6.11 PIVOT

Reshapes long-form results into wide-form by turning values of a pivot column into separate output columns.

```sql
-- Retention as wide-form: one row per entity, one column per period
RETENTION(entry: signup, returning: any, brackets: [1d, 7d, 30d]) BY user_id
  | PIVOT period_name ON period_active
```

Output schema: group-by columns, plus one new column per distinct value in the pivot column. The new column types match the value column's type. The set of distinct values must be known at plan time (provided as a literal list, or inferred from the query structure for operators like RETENTION that produce a fixed set of values).

| Column | Type | Nullable | Description |
|---|---|---|---|
| *(group-by columns)* | *(from input)* | *(from input)* | Retained as-is |
| *(pivot_value_1)* | same as value column | yes | Value for first pivot category |
| *(pivot_value_2)* | same as value column | yes | Value for second pivot category |
| ... | ... | ... | ... |

Pivot columns are nullable because not every group may have a value for every pivot category.

### 6.12 SAMPLE

Random sampling of entities. Reduces the entity set to a fraction or fixed count before processing.

```sql
-- 10% random sample of entities
SAMPLE(fraction: 0.1) BY user_id
  | MATCH(signup -> purchase) WITHIN 7d

-- Fixed sample size
SAMPLE(count: 10000) BY user_id
```

Output schema: passes through input schema unchanged. SAMPLE is a scan-level operator — it filters entities early to avoid processing the full dataset.

### 6.13 ORDER BY

Passes through input schema unchanged. The sort column must exist in the input schema and its type must support ordering — all scalar types (`Bool`, `Int`, `Float`, `String`, `Timestamp`) are orderable; `List` and `Map` are not.

### 6.14 LIMIT

Passes through input schema unchanged.

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
    ts TIMESTAMP,
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
column_modifier  := "ENTITY" "KEY" | "EVENT" "TYPE" | "NOT" "NULL" | "NULL"
alter_modifier   := "NOT" "NULL" | "NULL" | "DEFAULT" literal
```

### 9.3 Rules

- Exactly one column must have `ENTITY KEY`. Must be `STRING` or `INT`.
- Exactly one column must have `EVENT TYPE`. Must be `STRING`.
- Exactly one column must be `TIMESTAMP` type. If multiple `TIMESTAMP` columns exist, one must be annotated (syntax TBD in query-language.md).
- `ENTITY KEY`, `EVENT TYPE`, and the `TIMESTAMP` column are implicitly `NOT NULL`.
- Property columns are `NULL` by default. `NOT NULL` overrides.
- Type names are case-insensitive (`string`, `STRING`, `String` all valid).

### 9.4 DESCRIBE TABLE

```sql
DESCRIBE events
```

Output columns: name, type, nullable, role (entity_key / timestamp / event_type / property).

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
MATCH(view WHERE category = $c -> purchase WHERE category = $c)
    WITHIN 7d BY user_id
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
}
```

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
