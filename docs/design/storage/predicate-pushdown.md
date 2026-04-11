# Scan Interface and Predicate Pushdown Protocol

**Wave**: 2
**Task**: TASK-202
**Status**: draft — Wave 2 v1 protocol; extension points called out in §11.

## 1. Scope

This note fixes the wire protocol by which the **scan operator** hands
a filter predicate to the **storage layer** so the storage layer can:

1. **Skip whole row-groups** using zone maps (coarse pruning).
2. **Skip whole rows** inside a retained row-group using
   dictionary-aware filtering (fine pruning before decode).
3. **Fall back to post-filter** on any predicate shape the scan
   cannot evaluate, without the planner needing to know in advance.

It is the cross-cutting contract between:

- **Planner** (TASK-227 predicate-pushdown pass) — decides which
  `CompiledExpr` conjuncts can be pushed and extracts them from a
  `FilterPhysical` into `ScanPhysical.scan_predicates`.
- **Scan operator** (TASK-230 entity-sorted scan) — receives the
  pushed predicates on its physical descriptor, wraps them into a
  `ScanPredicate` value, and hands them to the `SegmentReader`.
- **Storage layer** (TASK-215 segment reader, TASK-216 zone-map
  pruning) — evaluates the pushed predicate at zone-map and
  row-group granularity, rewrites equality conjuncts against
  dictionary codes before decoding, and returns one row-group at a
  time to the scan operator.

What this note does **not** cover:

- The full expression compiler. TASK-205 owns `TypedExpr` and
  `CompiledExpr`; this doc only references the shapes it consumes.
- How `FilterPhysical` is rewritten after pushdown. TASK-227's
  optimizer pass owns that mechanics; this doc only specifies the
  contract the pass lowers into.
- Row-level evaluation of residual predicates. That is the filter
  operator's job (TASK-231); the scan hands back a `RecordBatch`
  with the rows that survived pushdown, and the filter operator
  evaluates whatever predicates were not pushed.
- Bloom filters, range indexes, or any pruning structure beyond
  zone maps. v1 is zone-map-only per storage-format.md §11.1.

## 2. Relationship to existing docs

| Topic | Authoritative doc | What this doc does |
|---|---|---|
| `SegmentReader` / `SegmentScan` traits | storage/reader-trait.md §4-§5 | **Extends** `open_segment`'s `predicate: Option<Arc<dyn Predicate>>` parameter with the richer predicate shape specified here. |
| `Predicate` trait (v0) | storage/reader-trait.md §6.4 | **Supersedes** the Wave 1 one-method placeholder with the full Wave 2 interface. The Wave 1 method (`accepts_zone`) becomes one of several trait methods; existing callers that honour `accepts_zone` conservatively (always-true) stay valid. |
| `ZoneMap` type | storage/reader-trait.md §6.2 | Reused unchanged. The `(min, max, null_count, row_count)` shape covers every v1 pruning case this doc needs. |
| Zone-map semantics | storage-format.md §11.1 | Unchanged. This doc specifies *how* zone maps are consulted, not *what* they contain. |
| Dictionary-rewritten equality | storage-format.md §10.4, segment-format-v1.md §9.2 | Reused. This doc specifies when the scan asks the reader to perform the rewrite and what the rewrite result looks like. |
| `CompiledExpr` shape | planner-pipeline.md §9.5 (line 1022), TASK-205 design | Referenced as the input to the pushdown protocol. This doc does not spec it; it specs which subset of `CompiledExpr` values can be pushed. |
| Predicate-pushdown optimizer pass | planner-pipeline.md §6.4 | Unchanged. This doc specs the interface the pass emits into; the pass itself is TASK-227. |

### 2.1 Terminology

- **Predicate.** A `CompiledExpr` whose evaluation produces a
  `Boolean` column. The planner's pushdown pass only considers
  predicates at `FilterPhysical` boundaries.
- **Conjunct.** A single `AND`-joined term inside a predicate. A
  top-level `WHERE a = 1 AND b > 10 AND c LIKE '%x%'` has three
  conjuncts. The pushdown pass considers conjuncts independently —
  each conjunct either pushes or stays.
- **Pushable.** A conjunct is *pushable* if the storage layer can
  evaluate it with one of its three pruning mechanisms (§6-§8). A
  non-pushable conjunct stays in the parent `Filter`.
- **Residual.** The set of conjuncts that did not push. If every
  conjunct pushed, the residual is empty and `FilterPhysical` is
  elided.
- **Zone-map acceptance.** A row-group *accepts* a pushed conjunct
  when the conjunct *might* match at least one row in the row-group
  per its zone map. The decision is conservative: when in doubt,
  accept (never prune a row-group that might contain a match).

## 3. The pipeline

```
planner                         scan operator                  storage
─────────                       ─────────────                  ───────
FilterPhysical(pred)
  └── ScanPhysical
           │
           │ TASK-227:
           │ extract pushable conjuncts
           │ into scan_predicates
           ▼
ScanPhysical {
  scan_predicates: Vec<CompiledExpr>,
  projected_columns: Vec<ColumnId>,
  ...
}
           │
           │ TASK-232: engine bind
           ▼
ScanOperator::new(cfg, scan_predicates)
           │
           │ wraps the vec into
           │ a ScanPredicate
           ▼
SegmentReader::open_segment(
  handle,
  projection,
  Some(Arc::new(scan_predicate)),
)
           │
           │ SegmentScan.next_row_group loop
           │   1. row_group_zone_maps
           │   2. scan_predicate.accepts_zone
           │       (every column in the AND set)
           │   3. if accepted: dict rewrite +
           │      row-level pruning (§7)
           │   4. decode surviving rows
           │   5. return RecordBatch (possibly empty)
           ▼
RecordBatch flows back to scan
           │
           │ Post-filter: residual predicates
           │ (TASK-231 FilterOperator on top)
           ▼
caller of FilterPhysical
```

The key idea: the planner does not have to *know* whether a conjunct
prunes well or not. It only has to decide whether the conjunct is
*pushable in principle* (§4). The storage layer then does its best
with the conjuncts it receives, and the scan operator returns
whatever rows survived; residual conjuncts are re-evaluated at the
filter operator above.

This keeps pushdown decisions a purely *syntactic* check at plan
time, postponing any actual pruning to runtime. TASK-227's pass
never needs to consult a cost model or histogram — if a conjunct is
syntactically pushable, it pushes; if not, it stays.

## 4. Pushable conjunct taxonomy

A conjunct is pushable iff it matches one of these shapes, where
`col` is a reference to a column already present in the scan's
table schema and `lit`, `lit_a`, `lit_b` are compile-time constant
literals of a type compatible with `col`:

| Shape | Example | Notes |
|---|---|---|
| `col = lit` | `event = 'checkout'` | Equality. Dictionary-rewritable when `col` is dictionary-encoded. |
| `col != lit` | `country != 'US'` | Inequality. Zone-map prunable only when `[min, max] == [lit, lit]`. |
| `col < lit`, `col <= lit`, `col > lit`, `col >= lit` | `amount > 100` | Range. Zone-map prunable via ordered comparison against `(min, max)`. |
| `col BETWEEN lit_a AND lit_b` (desugared to `col >= lit_a AND col <= lit_b`) | `ts BETWEEN '2026-01-01' AND '2026-03-01'` | Each side desugars to a range; pushed as two conjuncts. |
| `col IN (lit_1, lit_2, ..., lit_k)` | `event IN ('signup', 'purchase')` | Set. Each element is a dictionary-rewritable equality; the set becomes a code-set if the column is dictionary-encoded. |
| `col IS NULL`, `col IS NOT NULL` | `referrer IS NULL` | Nullability. Zone-map prunable via `null_count == 0` / `null_count == row_count`. |
| `pushable AND pushable` | `event = 'x' AND amount > 10` | Conjunction of pushable shapes is pushable; evaluated as the intersection of per-conjunct acceptance. |
| `NOT pushable` | `NOT (event = 'x')` | **Not directly pushable** in v1; the planner rewrites `NOT (col = lit)` to `col != lit` (which is pushable) and `NOT (col IS NULL)` to `col IS NOT NULL`, but generic negation stays in the filter. |

Any conjunct that does not match one of these shapes is
non-pushable and remains in the parent `FilterPhysical`. Explicitly
non-pushable in Wave 2:

- `OR` combinations — disjunction across different columns is
  non-prunable without an expensive row-level evaluation pass.
  (Disjunction between two `col = lit` shapes on the *same* column
  is just an `IN` set and rewrites as such — TASK-227 performs
  that rewrite in pass 1 before checking pushability.)
- `col LIKE '%pattern%'` — requires full decode and a regex engine
  in the storage layer; Wave 4 may add a bloom-filter hook here.
- `col1 op col2` — predicates comparing two columns cannot be zone
  mapped (the per-column zone maps don't interact).
- `fn(col)` — any scalar function on a column. Pushdown across
  functions requires an inverse that the selector has not yet
  designed.
- Anything inside a nested expression that is not a literal (e.g.
  `col > other_expression_involving_a_column`).

**Why this list is small.** Wave 2's goal is correctness of
pushdown plus the *acceptance-test-critical* cases — the Wave 2
acceptance query is `where event = 'checkout' AND amount > 100`,
which hits exactly equality + range on dictionary and non-dictionary
columns. Growing the list beyond this is explicitly a Wave 4/5
concern.

## 5. `ScanPredicate` — the runtime value handed to storage

The scan operator wraps its `scan_predicates: Vec<CompiledExpr>`
into a single `ScanPredicate` value at `open_segment` time.
`ScanPredicate` is the concrete type that implements the
(extended) `Predicate` trait from reader-trait.md §6.4, and it
lives in **`bqlite-core`** next to the existing `Predicate` trait
and `ZoneMap` type. Placement matters: both the scan operator
(`bqlite-operators`) and the storage reader (`bqlite-storage`)
need to name `ScanConjunct` — the scan operator builds a
`ScanPredicate` from its `scan_predicates: Vec<CompiledExpr>`, and
the storage reader iterates its conjuncts to build dictionary
masks (§7). Both crates already depend on `bqlite-core`, so
placing the shape there respects the dependency direction
(`operators → core`, `storage → core`) without either crate
needing to import the other.

```rust
// bqlite-core::storage — alongside the existing Predicate trait.

/// A conjunctive (AND) set of pushable scan predicates, in the
/// shape the storage layer can evaluate directly.
///
/// Each conjunct is a `ScanConjunct`. A row (or row-group) satisfies
/// the `ScanPredicate` iff it satisfies every conjunct. An empty
/// `conjuncts` vec means "no pushdown" — the scan returns every row
/// the projection produces and leaves all filtering to the filter
/// operator above. This is the pre-pushdown baseline.
#[derive(Debug, Clone)]
pub struct ScanPredicate {
    pub conjuncts: Vec<ScanConjunct>,
    /// Cached referenced columns (see §10's `referenced_columns`).
    /// Populated at construction so the trait method can hand back
    /// a slice without re-walking `conjuncts` on every call.
    pub(crate) referenced: Vec<String>,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ScanConjunct {
    /// `col = lit` — equality, dictionary-rewritable.
    Equal { column: String, value: PropertyValue },
    /// `col != lit` — inequality.
    NotEqual { column: String, value: PropertyValue },
    /// `col op lit` for `<`, `<=`, `>`, `>=`.
    Range {
        column: String,
        op: RangeOp,
        value: PropertyValue,
    },
    /// `col IN (lit_1, ..., lit_k)`. The set is always non-empty —
    /// TASK-227 elides empty IN lists to a `false` residual.
    InSet {
        column: String,
        values: Vec<PropertyValue>,
    },
    /// `col IS NULL`.
    IsNull { column: String },
    /// `col IS NOT NULL`.
    IsNotNull { column: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeOp {
    Lt,
    Le,
    Gt,
    Ge,
}
```

**Design notes.**

- **No nested expression tree.** `ScanConjunct` is flat — the
  planner has already collapsed nested `AND`s by the time it builds
  this value. The storage layer never sees an `Expr`-shaped tree.
- **`PropertyValue`, not `CompiledExpr`.** Each conjunct carries
  pre-resolved literal values of the boundary scalar type from
  `bqlite-core`. This keeps the storage layer free of any planner
  dependency on `CompiledExpr` internals — the entire storage
  crate sees `PropertyValue` only, consistent with the `ZoneMap`
  surface in reader-trait.md §6.2.
- **Column names, not `ColumnId`s.** The scan operator translates
  `CompiledExpr`'s column indices into names before building the
  `ScanPredicate`, because segments referenced through the
  `SegmentReader` use name-keyed projection (`ColumnProjection` in
  reader-trait.md §6.3). Name resolution happens at this boundary;
  §5.1 calls it out explicitly. Name-based lookup also handles
  schema evolution correctly: a conjunct on a column added after
  the segment was written refers to a column the segment does not
  contain, and the storage layer applies the NULL-backfill rule
  (§9) instead of failing.
- **`#[non_exhaustive]` on `ScanConjunct`.** Adding a new variant in
  a later wave (`ColumnCompare`, a bloom-filter hook, a pattern
  matcher — §11) is a breaking change for intra-crate consumers
  otherwise. Marking the enum non-exhaustive forces every `match`
  to have a catch-all arm from day one, so Wave 4+ additions do
  not cause compile-time churn across the reader, the scan
  operator, and the optimizer pass simultaneously. The tradeoff —
  consumers must always handle the catch-all — is cheap in
  practice because the catch-all can short-circuit with "treat as
  non-pushable" and return a safe default.

### 5.1 Building a `ScanPredicate` from `Vec<CompiledExpr>`

TASK-230's scan operator owns the conversion. Roughly:

1. For each `CompiledExpr` in `scan_predicates`, match on the
   outermost structure. Planner-pipeline.md §9.5 guarantees these
   are already vetted by TASK-227, so the match is exhaustive for
   the seven variants in §4 and unreachable for everything else.
2. Extract the column name, operator, and literal.
3. Append the resulting `ScanConjunct` to `ScanPredicate.conjuncts`.
4. Pass `Some(Arc::new(scan_predicate))` to `open_segment`.

The conversion is pure syntactic; no type coercion happens here
because `CompiledExpr` construction (TASK-225) has already coerced
every literal to the column's declared type (`TypedExpr`'s
invariant — planner-pipeline.md §4.5).

## 6. Zone-map acceptance

The storage layer calls `ScanPredicate::accepts_zone_group` once
per row-group. This is the single entry point the reader uses for
coarse pruning:

```rust
impl ScanPredicate {
    /// Returns `true` iff *every* conjunct accepts the row-group
    /// described by `zones`. A row-group where at least one
    /// conjunct rejects is pruned entirely — the row-group is
    /// never decoded.
    ///
    /// `zones` is the per-column zone-map map from
    /// `SegmentScan::row_group_zone_maps(idx)`. A conjunct on a
    /// column missing from `zones` is **conservatively accepted**
    /// — the storage layer must not prune row-groups just because
    /// a column had no zone map (reader-trait.md §5.1 explicitly
    /// allows `row_group_zone_maps` to return empty or partial
    /// maps).
    pub fn accepts_zone_group(
        &self,
        zones: &HashMap<String, ZoneMap>,
    ) -> bool {
        self.conjuncts
            .iter()
            .all(|c| c.accepts_zone_group(zones))
    }
}
```

Per-conjunct acceptance rules. In each row below, let
`zone = zones.get(column)`. If `zone` is `None`, the conjunct
**accepts unconditionally** — absent zones are the conservative
default required by reader-trait.md §5.1. If `zone` is `Some`, let
`min = zone.min` and `max = zone.max` (both `Option<PropertyValue>`
per reader-trait.md §6.2 — `None` means the row-group is all-null
for this column), `nulls = zone.null_count`, `rows = zone.row_count`.

| Conjunct | Acceptance rule (given `zone` is `Some`) |
|---|---|
| `Equal { column, value }` | `nulls < rows` **and** `min.as_ref().is_none_or(\|m\| m <= value)` **and** `max.as_ref().is_none_or(\|x\| value <= x)`. The `is_none_or` arms handle "the column has some non-nulls but the boundary value is unknown", which never happens in v1 zone maps (writer always fills both bounds when `nulls < rows`) but is specified so a conservative reader that produces `None` bounds stays correct. |
| `NotEqual { column, value }` | **Reject** iff `nulls == 0 && min == Some(value) && max == Some(value)`. Otherwise accept. The degenerate rejection is "every non-null row equals `value` and there are no nulls"; any row-group that contains at least one NULL row may still satisfy the predicate through the non-null survivors that the filter operator will re-check. |
| `Range { column, Lt, value }` | `nulls < rows` **and** `min.as_ref().is_some_and(\|m\| m < value)`. Row-groups with only NULLs (`nulls == rows`) reject immediately — every non-null value is needed. |
| `Range { column, Le, value }` | `nulls < rows` **and** `min.as_ref().is_some_and(\|m\| m <= value)`. |
| `Range { column, Gt, value }` | `nulls < rows` **and** `max.as_ref().is_some_and(\|x\| value < x)`. |
| `Range { column, Ge, value }` | `nulls < rows` **and** `max.as_ref().is_some_and(\|x\| value <= x)`. |
| `InSet { column, values }` | `nulls < rows` **and** there exists `v` in `values` with `min.as_ref().is_none_or(\|m\| m <= v)` **and** `max.as_ref().is_none_or(\|x\| v <= x)`. Linear scan over `values` is fine — `values` is bounded by the parsed `IN (...)` list size. |
| `IsNull { column }` | `nulls > 0`. |
| `IsNotNull { column }` | `nulls < rows`. |

**Null semantics rationale.** A `Range`, `Equal`, or `InSet`
predicate evaluated against a NULL row returns `UNKNOWN` under the
three-valued logic in type-system.md §4, and a row that evaluates
to `UNKNOWN` is filtered out by the filter operator. Every row in
a row-group where `nulls == rows` is NULL, so no row can satisfy
the predicate — the reader prunes the row-group. The leading
`nulls < rows` check encodes this invariant once per rule.

`NotEqual` is the one asymmetry: a row-group where every non-null
row equals `value` **and** `nulls == 0` contains no survivors
(every row fails), but a row-group with `min == max == value`
**and** `nulls > 0` still has NULL rows. Those NULL rows produce
`UNKNOWN` under `!=` and the filter operator drops them — so the
decision is identical either way (reject), but stating the rule as
"reject only when `nulls == 0 && min == max == value`" makes the
min/max comparisons well-typed against `Option<PropertyValue>`
bounds.

The `is_none_or` / `is_some_and` dance is load-bearing: a
`zone.min == None` under `nulls < rows` means the writer did not
populate a minimum for a partial-null row-group, which v1 writers
do not produce (segment-format-v1.md §10.2's `min_value` field is
always populated for non-empty non-null row-groups). The rule
nevertheless accepts that case so a hypothetical future reader
that chooses not to populate one side of the bound — e.g. for
columns with very expensive `PartialOrd` — keeps correctness at
the cost of pruning effectiveness.

### 6.1 Evaluation order

The reader evaluates conjuncts in the declaration order supplied
by the scan operator. Because acceptance is short-circuit AND, the
first conjunct that rejects terminates the row-group evaluation —
this gives the planner a handle on prune ordering without
introducing a cost model: if TASK-227's pass sees a conjunct
likely to prune harder (e.g. a range on a sorted column), it can
emit that conjunct first. Wave 2 does not exploit this; conjuncts
go out in source order. Wave 5 may add an ordering heuristic.

## 7. Dictionary-aware filtering

For row-groups that pass zone-map acceptance, the reader may
perform *fine* pruning before decoding full column chunks. The only
fine-pruning mechanism in v1 is dictionary-aware equality rewriting
per storage-format.md §10.4. The protocol:

1. For each `Equal` or `InSet` conjunct whose `column` is
   dictionary-encoded (the reader inspects the column chunk's
   encoding descriptor in the segment footer — segment-format-v1.md
   §9.2), the reader resolves the literal(s) against the
   segment-level dictionary to produce a *code set*:

   ```
   resolve(column, values) → Option<Vec<u32>>
   ```

   - If every literal resolves, the reader obtains a `Vec<u32>` of
     codes to match.
   - If *no* literal resolves (e.g. `event = 'never_seen'`), the
     row-group produces zero rows for that conjunct — the reader
     can skip the remaining column decodes and return an empty
     batch.
   - If *some* but not all literals resolve (only possible in
     `InSet`), the reader filters against the resolved subset; the
     missing literals would never match anyway.

2. For every conjunct whose column is dictionary-encoded and whose
   resolution succeeded, the reader computes a boolean mask over
   the row-group's bit-packed codes *before* decoding any other
   column. This is the "dictionary pushdown" storage-format.md
   §10.4 calls out.

3. If any conjunct produced an empty code set, the reader
   short-circuits to an empty `RecordBatch` and returns without
   decoding.

4. Otherwise, the reader decodes the projected columns for the
   surviving rows only — either by applying the mask during
   decode (Plain, BitPacking, Delta) or by re-indexing into a
   pre-decoded column and taking a gather. The exact strategy is
   per-encoding and is TASK-215's responsibility; this doc only
   specifies the mask's shape and how it composes across conjuncts.

### 7.1 Mask composition

When multiple conjuncts produce masks, the reader ANDs them:

```rust
let combined = dictionary_masks
    .into_iter()
    .reduce(|a, b| and_bitmaps(&a, &b))
    .unwrap_or_else(|| all_true(row_count));
```

Non-dictionary conjuncts (`Range`, `NotEqual`, `IsNull`,
`IsNotNull`, or an `Equal`/`InSet` on a non-dict column) do **not**
produce masks at this stage. They are evaluated at the
row/value level *after* decode, either by the reader (TASK-215 may
implement a fast path) or by the filter operator above (TASK-231)
on whatever rows survived dictionary filtering.

### 7.2 Why only dictionary rewriting is Wave 2

Wave 2 scope frozen by storage-format.md §10.4 ("must be in v1").
Advanced fine-pruning — range-aware bit-packed filters, ALP/FSST
predicate kernels, FOR/PFOR pushdown — is Wave 4 territory
(TASK-401). The protocol in this section is intentionally a hook
point: the scan hands the reader a `ScanPredicate`; what the
reader does with each conjunct beyond zone maps is entirely the
reader's internal decision. Wave 4 can add more fine-pruning paths
without changing the `ScanPredicate` shape.

## 8. Post-filter fallback

Any conjunct that the reader cannot evaluate fully at pruning time
(range on a non-sorted column, `IS NULL` on a column whose
null-bitmap must be scanned, or a conjunct whose column is missing
from the segment due to schema evolution) **survives into the
returned `RecordBatch`**. The row-group is returned with the rows
that passed whatever pruning was possible; the filter operator
above the scan (TASK-231) evaluates the full filter on the
returned rows.

The key invariant:

> **No false negatives.** The storage layer may return rows that
> the predicate will ultimately reject, but it must never drop a
> row the predicate would have kept.

This one-sided contract lets the reader be as conservative or as
aggressive as it likes without correctness consequences. The
filter operator on top is the single source of final truth.

### 8.1 What the scan operator does with the residual

TASK-227's pushdown pass extracts pushable conjuncts from the
parent `FilterPhysical` and leaves the residual in place:

- All conjuncts pushable → `FilterPhysical` is elided; the scan
  returns the already-filtered rows (pruned by zone maps +
  dictionary masks) and the next operator is whatever was above
  `Filter`.
- Some pushable → `FilterPhysical` remains with the residual
  conjuncts only, directly above `ScanPhysical`. The filter
  operator re-evaluates the whole residual on the scan's output.
- None pushable → `ScanPhysical.scan_predicates` is empty, the
  scan runs unpruned, and the filter operator does all the work.

This three-way split is the only optimizer behavior the protocol
demands; everything else is inside the scan and filter operators.

**Contract on TASK-227: pushed conjuncts are *removed* from the
filter, not *copied*.** The optimizer pass must delete every
pushed conjunct from `FilterPhysical.predicate` so that the filter
operator never re-evaluates them on rows the reader already
pruned. This is what makes §8.2 correct — the filter operator
cannot double-evaluate pushed conjuncts *because they are not in
its `predicate` at all*. Copying instead of moving would still be
semantically correct (pushed conjuncts are idempotent under
three-valued logic), but wastes CPU on hot-path rows. State this
as an invariant the pass must uphold, not a suggested
optimization.

### 8.2 Double evaluation of pushable conjuncts

Because the reader's evaluation of pushed conjuncts is
*conservative*, the filter operator above does **not** re-evaluate
pushed conjuncts. This is safe because pushdown is one-sided
("may return extras, never drop") but would be inefficient — the
operator would redo work the reader already did. The one-directional
contract lets the planner confidently elide `FilterPhysical`
whenever the residual is empty and trust the reader's pruning.

If a future reader chose to *return* a row that it was supposed to
have pruned (e.g. because a bug or a conservative shortcut), the
filter operator would produce a wrong result — which is exactly
why §8's invariant is non-negotiable. Readers must not shortcut
toward "return too few"; they may only shortcut toward "return
too many".

## 9. Schema evolution and missing columns

reader-trait.md §4.1 requires `SegmentReader::schema` to return the
*current* schema — older segments are backfilled with NULL (or the
column's DEFAULT) before the reader produces a `RecordBatch`.
Pushdown honours the same rule at conjunct level:

- A conjunct on a column **absent** from the segment's write-time
  schema is treated as if the column were all-NULL:
  - `Equal { column, value }`, `NotEqual { column, value }`,
    `Range { column, .. }`, `InSet { column, .. }` all reject
    the row-group if the segment has the column as
    "added-later-backfilled-NULL" and the conjunct has no
    DEFAULT-aware path (Wave 4 may teach the reader about defaults;
    Wave 2 treats a backfilled-NULL column as unconditionally NULL).
  - `IsNull { column }` accepts the row-group.
  - `IsNotNull { column }` rejects the row-group.
- A conjunct on a column present in the segment's write-time
  schema but **not** present in the *current* manifest schema (a
  column removed by some hypothetical future `ALTER TABLE DROP
  COLUMN` — not supported in Wave 2, but recorded here for
  forward-compat) is a planner bug. The conjunct's column was
  resolved against the current schema at plan time, so it cannot
  reference a dropped column.

Wave 2 only implements the "absent from old segment" case because
`ALTER TABLE ADD COLUMN` is the only schema-evolution DDL in scope
(TASK-221). The "present but dropped" case is called out only to
freeze the rule for later waves.

## 10. Trait extension — the Wave 2 `Predicate` trait

reader-trait.md §6.4's one-method `Predicate` trait grows to the
following Wave 2 shape. This extension is **additive**: the Wave 1
default behaviour (treat every unknown conjunct as accepted) is
preserved when the trait is implemented by `ScanPredicate`
specifically, and the trait stays object-safe for
`Arc<dyn Predicate>`:

```rust
pub trait Predicate: Send + Sync + std::fmt::Debug {
    /// Single-column zone-map acceptance — the Wave 1 surface,
    /// retained unchanged so Wave 1 `Predicate` impls stay valid.
    ///
    /// For a multi-column predicate like `ScanPredicate`, this
    /// method is defined as: *return true unless at least one
    /// conjunct that references `column` rejects `zone`*.
    /// Conjuncts on other columns are not checked — the caller is
    /// asking about a single column in isolation. For a
    /// full-row-group decision, call `accepts_zone_group` instead.
    ///
    /// Because this method only consults conjuncts on `column`, a
    /// `ScanPredicate` that returns `true` here may still reject
    /// the whole row-group via conjuncts on other columns. Callers
    /// that have access to the complete `column → zone_map` map
    /// should prefer `accepts_zone_group`; the single-column form
    /// is kept so Wave 1 code that iterates its own column
    /// dictionary and calls `accepts_zone` per column remains
    /// correct (each per-column call sees its own conjuncts
    /// applied).
    fn accepts_zone(&self, column: &str, zone: &ZoneMap) -> bool;

    /// Full-row-group zone-map acceptance.
    ///
    /// The caller supplies the complete `column → zone_map`
    /// dictionary from `SegmentScan::row_group_zone_maps(idx)`.
    /// The implementer returns `true` iff at least one row in the
    /// row-group *might* satisfy every conjunct. Conjuncts that
    /// reference a column absent from `zones` are treated
    /// **conservatively**: they accept (the reader must not prune
    /// a row-group just because a zone was not recorded, per
    /// reader-trait.md §5.1).
    ///
    /// **`ScanPredicate` overrides this** — the default body below
    /// is a correct but suboptimal fallback for Wave 1 impls. The
    /// default iterates the *zones map* and calls `accepts_zone`
    /// per entry, which misses conjuncts on columns absent from
    /// the map. For Wave 1 `Predicate` impls that only meaningfully
    /// answer per-column, this is the best we can do. For
    /// `ScanPredicate`, the correct loop is over its own
    /// `conjuncts`, using §6's acceptance rules — see the
    /// `ScanPredicate` impl section below.
    fn accepts_zone_group(
        &self,
        zones: &HashMap<String, ZoneMap>,
    ) -> bool {
        zones
            .iter()
            .all(|(col, zone)| self.accepts_zone(col, zone))
    }

    /// Column names this predicate refers to.
    ///
    /// Lets the reader decide which dictionaries to resolve before
    /// decoding, and which null bitmaps it *might* need to scan. An
    /// implementer that does not know its columns returns an empty
    /// slice — the reader then pessimistically handles every
    /// column. `ScanPredicate` caches this list in its
    /// `referenced` field, populated at construction from a walk
    /// over its conjuncts, so this accessor is a plain slice return.
    fn referenced_columns(&self) -> &[String] {
        &[]
    }

    /// Dictionary rewrite for `Equal` / `InSet` conjuncts on a
    /// dictionary-encoded column.
    ///
    /// The reader calls this method for each projected column whose
    /// column chunk uses Dictionary encoding (segment-format-v1.md
    /// §9.2) and hands in a view of the segment-level dictionary.
    /// The trait implementer inspects its own conjuncts that
    /// reference `column` and returns one of the three outcomes in
    /// [`DictRewrite`]:
    ///
    /// - `NoRewrite` — the implementer has no `Equal` or `InSet`
    ///   conjunct on `column` (or is a Wave 1 impl that doesn't
    ///   know how to rewrite). The reader proceeds as if no
    ///   rewrite were requested — range conjuncts and post-filter
    ///   still apply.
    /// - `EmptySet` — there is at least one dict-rewritable
    ///   conjunct on `column`, and *none* of its literals resolved
    ///   to codes in the segment's dictionary. The row-group
    ///   cannot possibly contain a match on that conjunct; the
    ///   reader short-circuits to an empty `RecordBatch` for the
    ///   row-group without decoding any other column.
    /// - `Codes(codes)` — the literal(s) resolved to at least one
    ///   code. The reader constructs a bitmask by matching the
    ///   row-group's bit-packed codes against `codes` and uses
    ///   that mask to filter (or gather) the other projected
    ///   columns during decode.
    ///
    /// The default body returns [`DictRewrite::NoRewrite`], so
    /// Wave 1 `Predicate` impls are unaffected. `ScanPredicate`
    /// overrides this to walk its `Equal` and `InSet` conjuncts
    /// on `column`, resolve each literal via `dict.code_of`, and
    /// return `EmptySet` if all resolutions fail, `Codes` with
    /// the resolved subset otherwise. `InSet` conjuncts where
    /// *some* literals fail to resolve keep the surviving codes —
    /// the missing literals would never match anyway, so dropping
    /// them preserves the "no false negatives" invariant.
    fn resolve_dictionary_codes(
        &self,
        _column: &str,
        _dict: &DictionaryIndex,
    ) -> DictRewrite {
        DictRewrite::NoRewrite
    }
}

/// Outcome of a dictionary rewrite for a single column.
///
/// See `Predicate::resolve_dictionary_codes` for the full semantics.
#[derive(Debug, Clone)]
pub enum DictRewrite {
    /// No dict-rewritable conjunct on this column — proceed as
    /// if the method had not been called.
    NoRewrite,
    /// At least one dict-rewritable conjunct exists, but none of
    /// its literals resolved to codes — the row-group yields zero
    /// matching rows for this conjunct.
    EmptySet,
    /// The resolved code set. Non-empty by construction;
    /// duplicate codes are allowed (the reader's mask
    /// construction handles them).
    Codes(Vec<u32>),
}
```

**`DictionaryIndex`** is the segment-level dictionary surface the
reader exposes to its own predicate implementers. It is not a new
public type — segment-format-v1.md §11.1 already specifies the
on-disk shape. The `Predicate` trait just borrows a view of it:

```rust
pub struct DictionaryIndex<'a> {
    /// Sorted distinct values (one entry per code).
    pub values: &'a [PropertyValue],
}

impl DictionaryIndex<'_> {
    pub fn code_of(&self, value: &PropertyValue) -> Option<u32> {
        self.values.binary_search(value).ok().map(|i| i as u32)
    }
}
```

A trait method is the right layer for `resolve_dictionary_codes`
because the reader walks its own dictionary inventory and does not
know (and should not know) whether the caller is a `ScanPredicate`
or some other `Predicate` impl. The default body is
`DictRewrite::NoRewrite`, which matches the Wave 1 behaviour of
"no rewrite possible", so existing readers keep working.

### 10.2 `ScanPredicate` implementation of the `Predicate` trait

`ScanPredicate` overrides three of the four methods (every method
except the Wave 1 required `accepts_zone`):

```rust
impl Predicate for ScanPredicate {
    fn accepts_zone(&self, column: &str, zone: &ZoneMap) -> bool {
        // Walk only the conjuncts that reference `column`. A
        // conjunct on any *other* column is not checked here — by
        // contract, this single-column form only answers about
        // `column` in isolation. Callers that want the full
        // row-group decision use `accepts_zone_group`.
        self.conjuncts
            .iter()
            .filter(|c| c.column() == column)
            .all(|c| c.accepts_zone(zone))
    }

    fn accepts_zone_group(
        &self,
        zones: &HashMap<String, ZoneMap>,
    ) -> bool {
        // Iterate *our conjuncts*, not the zones map, so conjuncts
        // on columns absent from `zones` are conservatively
        // accepted per §6. This overrides the default body, which
        // would miss them.
        self.conjuncts
            .iter()
            .all(|c| match zones.get(c.column()) {
                Some(zone) => c.accepts_zone(zone),
                None => true,
            })
    }

    fn referenced_columns(&self) -> &[String] {
        &self.referenced
    }

    fn resolve_dictionary_codes(
        &self,
        column: &str,
        dict: &DictionaryIndex<'_>,
    ) -> DictRewrite {
        // Walk conjuncts on `column`, collect resolved codes from
        // every Equal / InSet, return per §7's "no rewrite / empty
        // / codes" contract.
        let mut any_rewritable = false;
        let mut codes: Vec<u32> = Vec::new();
        for conj in &self.conjuncts {
            if conj.column() != column {
                continue;
            }
            match conj {
                ScanConjunct::Equal { value, .. } => {
                    any_rewritable = true;
                    if let Some(code) = dict.code_of(value) {
                        codes.push(code);
                    }
                }
                ScanConjunct::InSet { values, .. } => {
                    any_rewritable = true;
                    for v in values {
                        if let Some(code) = dict.code_of(v) {
                            codes.push(code);
                        }
                    }
                }
                _ => {} // Range, NotEqual, IsNull, IsNotNull — not
                        // dict-rewritable; leave for post-filter.
            }
        }
        match (any_rewritable, codes.is_empty()) {
            (false, _) => DictRewrite::NoRewrite,
            (true, true) => DictRewrite::EmptySet,
            (true, false) => DictRewrite::Codes(codes),
        }
    }
}
```

`ScanConjunct` exposes a private `column(&self) -> &str` helper and
a private `accepts_zone(&self, &ZoneMap) -> bool` helper that
encodes §6's per-conjunct rule set. These helpers are not part of
the public trait — they are implementation detail of
`bqlite-core::storage` that the reader never calls directly.

### 10.1 `open_segment` signature

Unchanged from reader-trait.md §4.1. The `predicate: Option<Arc<dyn
Predicate>>` parameter already exists; Wave 2 only tightens how
the parameter is produced and consumed. No `open_segment` change
means TASK-215's reader impl and TASK-117's Wave 1 operator stub
keep compiling without a rebase.

## 11. Open questions and extension points

Called out explicitly so we do not accidentally close them:

1. **Conjunct ordering heuristic.** §6.1 leaves conjuncts in
   declaration order. A real cost model — count of distinct
   values, null ratio, encoding type — would let the reader
   evaluate the most-selective conjunct first. Tracked for Wave 5;
   not an interface change, only an internal reordering.
2. **Range-aware bit-packed predicate kernels.** §7 only specs
   dictionary-mask rewriting. Wave 4 (TASK-401) may add
   bit-packed range kernels so `amount > 100` prunes rows before
   decoding the `amount` column. This would be a new `Predicate`
   trait method (`evaluate_range_kernel`) with a `None` default
   body, again additive.
3. **Bloom filters.** storage-format.md §11 defers bloom filters
   to v2. The protocol already accommodates them: a later wave can
   add a `resolve_bloom` default method returning a bit that says
   "definitely absent" or "maybe present", and an `accepts_zone`-
   style hook for checking. No `ScanConjunct` shape change
   required.
4. **Residual reuse across row-groups.** A conservative reader
   might return the same post-filter mask for every row-group in
   a segment. Nothing in the protocol forbids this, but also
   nothing encourages it — Wave 2 lets each row-group be a fresh
   evaluation. Revisit if profiling shows mask construction is hot.
5. **Multi-column comparisons.** `col1 > col2` is explicitly
   non-pushable in v1 (§4). Wave 5 may add a limited form
   (`col = other_col`) when both columns live on the same
   row-group; the protocol has room for this as a new
   `ScanConjunct::ColumnCompare { lhs, rhs, op }` variant.
6. **`LIKE` with anchored prefixes.** `col LIKE 'prefix%'` could
   push down to a range `col >= 'prefix' AND col < 'prefiy'`. The
   grammar landing point is Wave 4; the protocol welcomes it as
   a planner rewrite into two `Range` conjuncts, requiring no
   storage-side change.

## 12. Wave 2 implementation task mapping

| Concern | Implementing task | Notes |
|---|---|---|
| Extend the `Predicate` trait with the Wave 2 method set (incl. `accepts_zone_group`, `referenced_columns`, `resolve_dictionary_codes`, `DictRewrite`) | TASK-109 follow-up (small doc-driven patch) | reader-trait.md §6.4 can grow in place; no new trait. |
| Define `ScanPredicate` + `ScanConjunct` + `RangeOp` + `DictRewrite` + `DictionaryIndex` | TASK-216 or a small follow-up land | Lives in **`bqlite-core::storage`** next to the existing `Predicate` trait and `ZoneMap` type. Both `bqlite-operators` (via TASK-230's scan operator building `ScanPredicate`) and `bqlite-storage` (via TASK-215 / TASK-216's reader iterating conjuncts) import from `bqlite-core`, which each already depends on. This placement is deliberate (see §5) and is the one structural decision this doc makes outside its protocol scope. |
| TASK-227 predicate-pushdown optimizer pass | TASK-227 | Uses §4's taxonomy as its pushability check. |
| TASK-215 segment reader's zone-map consult loop | TASK-215 / TASK-216 | Calls `accepts_zone_group` and handles dictionary rewrites via the new trait methods. |
| Row-level post-filter of residual conjuncts | TASK-231 (filter operator) | Existing work — this task just confirms the residual shape the filter operator receives. |
| Tests for each `ScanConjunct` shape | TASK-230 + TASK-216 each carry their own | §4's taxonomy is the test matrix spine. Every row in §6's acceptance table becomes a test case. |

### 12.1 Cross-cutting invariant tests

Two invariants need tests at the crate boundary, above any single
Wave 2 task's scope:

- **Pushdown preservation.** For any query, the result set of
  `Query(Pipeline)` equals the result set of the same `Pipeline`
  run with pushdown disabled. Asserted by a property test that
  runs each query twice — once with `scan_predicates: Vec::new()`
  (post-filter only) and once with the post-TASK-227 plan (full
  pushdown) — and diffs the rows. This proves the "no false
  negatives" invariant at the workload level. Filed as a follow-up
  integration test after TASK-230 lands; not blocking for Wave 2
  acceptance.
- **Acceptance query pruning rate.** The Wave 2 perf gate requires
  ≥80% row-group pruning on the acceptance query
  (`where event = 'checkout' AND amount > 100`). This is a
  benchmark-level assertion; TASK-236 owns the measurement. The
  protocol's correctness is a prerequisite for the benchmark to
  make sense at all.

---

The protocol is intentionally small: one flat `ScanPredicate`
value, one entry point (`accepts_zone_group`), one optional
rewrite hook (`resolve_dictionary_codes`), and one non-negotiable
invariant ("never drop a row the filter would have kept"). Wave 2
ships this surface; Wave 4/5 extend it additively.
