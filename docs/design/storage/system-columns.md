# System Column Materialization

> Reconciles `storage-format.md` §6.2 (semantics), `execution-model.md` §3.7
> (pipeline schema), `planner/logical-plan-nodes.md` §Scan (logical-plan
> output shape), and `language/cohorts-aliases-joins.md` §3.8 (joined-source
> combined schema). Where this doc and the older docs disagree, this doc
> wins; the older docs are edited in lockstep with TASK-508.

## 1. The Two Columns

| Column | Arrow type | Nullable | Origin | `SELECT *` |
|---|---|---|---|---|
| `__seq_id` | `Int64` | NO | per-row, segment-local | excluded |
| `__batch_id` | `Int64` | NO | per-segment constant | excluded |

`storage-format.md` §6.2 is the authoritative semantics doc — `__seq_id` is
a per-row monotonically-increasing identifier and `__batch_id` is a
per-segment constant identifying the ingest call (or compaction-output
operation) that produced the segment. This doc covers materialization
rules only.

## 2. Per-Segment Derivation

Every segment footer (`SegmentFooter::seq_id_range`, `SegmentFooter::batch_id`
in `crates/bqlite-storage/src/segment/layout.rs`) carries:

- `seq_id_range: (u64, u64)` — closed inclusive range. `seq_id_range.0` is
  the segment's first row's `__seq_id`. Compaction allocates a fresh
  contiguous range via `Database::allocate_sequence_id_range`, so the rule
  below holds for both ingest-written and compaction-written segments.
- `batch_id: u64` — the ingest batch (or compaction-output id) the segment
  belongs to.

For row `n` (0-indexed) in cumulative segment-storage order:

```
__seq_id(n) = seq_id_range.0 + n
__batch_id  = batch_id   (constant)
```

The `seq_id_range.0 + n` form is identical to what `CompactionTombstoneScan`
(`crates/bqlite-storage/src/tombstone_scan.rs`) already uses to derive
per-row `__seq_id` for compaction-time tombstone filtering. Both readers
and the compaction wrapper share the same convention.

**Rationale.** Storing `__seq_id` and `__batch_id` as on-disk column
chunks would waste roughly 8–16 bytes/row that are recoverable from
metadata at zero space cost. The `seq_id_first + offset` derivation is
correct because rows are written to a segment in seq_id-allocation order
(both ingest in `bqlite-storage::writer::write_bucket` and compaction in
`bqlite-storage::compaction` allocate a contiguous range and stamp rows
sequentially as they emit them).

## 3. Reader Contract

`SegmentFileReader::scan(projection, predicate)` (in
`crates/bqlite-storage/src/segment/reader.rs`) resolves the projection as
follows:

- **Empty / `ColumnProjection::all()`**: emit every declared column (in
  `current_schema.columns()` order) followed by `__seq_id` and
  `__batch_id` in that order. This matches the doc-comment on
  `ColumnProjection::all()` in `bqlite-core/src/storage.rs`.
- **Explicit projection**: each requested name is resolved against
  `TableSchema::logical_columns()` (declared + system). System column
  names are recognised by exact match against
  `bqlite_core::schema::SEQ_ID_COLUMN` and `BATCH_ID_COLUMN`. Unknown
  names error as before.

Synthesised arrays are `Int64Array`s with no null bitmap attached and a
declared `Field::is_nullable() == false`, matching `execution-model.md`
§3.7 (the non-nullable column rule). The reader does NOT touch
column-chunk bytes for these names — there is no on-disk chunk for them
in the v1/v2 segment format.

The `ScanPlan` carries two `PlannedColumnSource` variants
(`SystemSeqId`, `SystemBatchId`, introduced by TASK-508 CP2) so the
per-row-group decoder can build them inline. A prefix-sum
`row_group_start: Vec<u64>` on `SegmentFileScan` tracks the cumulative
offset of each row group's first row so synthesised `__seq_id` is
correct even when zone-map pruning skips earlier row groups.

For the encoded path (`next_encoded_row_group`), system columns are
emitted as `EncodedColumn::Materialized` wrapping the same `Int64Array` —
the encoded merge / kernel layer treats them as opaque materialized
columns. This keeps the encoded-batch contract intact without requiring
a synthetic dictionary or RLE encoding for what is structurally a
counter / constant.

## 4. Operator Contract

### 4.1 ScanOperator

`build_output_schema` (in `crates/bqlite-operators/src/scan.rs`) mirrors
the reader:

- Empty `projected_columns` slice → output schema =
  `OperatorSchema::from_table(reader.schema())` (declared + `__seq_id` +
  `__batch_id`).
- Explicit projection → resolved against `TableSchema::logical_columns()`;
  system column names are first-class.

This contract was previously documented as an explicit carve-out — the
scan operator's module docs stated that `__seq_id` / `__batch_id` were
NOT exposed because the Wave 2 reader did not yet materialise them. As
of TASK-508 they are exposed; the carve-out comment is removed.

### 4.2 MergeSourcesOperator

Per `cohorts-aliases-joins.md` §3.8, the combined schema for a joined
source today carries:

- `<table>.<col>` for every non-system column of every sub-table (nullable).
- `__source_table_id` non-nullable. The spec defines this as `Int8`; the
  current planner implementation widens it to `Int` (Int64) because
  `BqlType` has no `Int8` variant — see the comment at
  `crates/bqlite-planner/src/logical.rs:1190-1193`. TASK-508 does not
  change this column's type; the `Int8` narrowing is a separate
  follow-up tracked against `BqlType`'s narrow-int support.

**TASK-508 extends `cohorts-aliases-joins.md` §3.8 by adding the two
system columns to the combined schema:**

- `__seq_id: Int` (Int64) non-nullable.
- `__batch_id: Int` (Int64) non-nullable.

The §3.8 spec must be updated in lockstep with the CP4 code change so
the spec and implementation continue to agree (see §6 below).

The system columns are bare-named (no `<table>.` qualifier) because they
have identical semantics across every sub-table. The merge picks one row
from one sub-scan at a time, and the picked sub-scan's `__seq_id` /
`__batch_id` populate the output — never null — because every sub-scan
emits them by the contract above.

`build_joined_scan` (`crates/bqlite-planner/src/logical.rs`) adds the
two system columns to the combined schema. The previous comment about
omitting them (added when the scan operator could not materialise them)
is removed.

Sub-scan projections must include the system columns. The simplest and
current-default form — pass an empty `projected_columns` slice — already
gives the full set after CP2/CP3 (see §3 above and §4.1). Joined-source
lowering relies on this default.

### 4.3 Same-`ts` Tiebreaking

`MergeSourcesOperator`'s heap orders by `(entity_key, ts, scan_idx,
row_idx)` (per `cohorts-aliases-joins.md` §3.2 / TASK-407 note B1). The
documented canonical tiebreaker is `__seq_id`, which is preserved
implicitly by each sub-scan's own `(entity_key, ts, __seq_id)` ordering —
within a sub-scan, `row_idx` advances in `__seq_id` order because each
segment's rows are stored in seq_id-allocation order (§2 above). The
`scan_idx` term realises the cross-table `table_order` position; the
`row_idx` term defers to the within-sub-scan `__seq_id` order. Therefore
declaring `__seq_id` in the combined schema does not change the merge's
ordering; it only makes the column visible to downstream operators.

## 5. SELECT *

`SELECT *` excludes system columns (per `storage-format.md` §6.2 and
`query-language.md`); the planner's project-expansion step is
responsible for filtering them out of the star expansion. TASK-508 does
not change that behaviour: the *operator* schemas carry the system
columns, but the user-visible `SELECT *` projection drops them.

## 6. Reconciliation With Prior Docs

- `bqlite-operators::scan` module docs previously stated that `__seq_id`
  / `__batch_id` are NOT in the scan's output schema. TASK-508 rewrites
  that paragraph to point here.
- `bqlite-planner::logical::build_joined_scan` previously documented an
  explicit omission of the two system columns. TASK-508 rewrites that
  comment to point here.
- `crates/bqlite-storage/src/segment/reader.rs::SegmentFileReader::scan`
  doc-comment previously made no mention of system-column synthesis.
  TASK-508 adds a "## System columns" subsection.
- `bqlite-core/src/storage.rs::ColumnProjection::all()` already
  documents "all declared columns plus the implicit `__seq_id` and
  `__batch_id` system columns". After TASK-508 the implementation
  finally matches that doc.
- `docs/design/language/cohorts-aliases-joins.md` §3.8 currently
  documents only `__source_table_id` in the combined schema. TASK-508
  CP4 extends §3.8 by adding `__seq_id` and `__batch_id`; the spec
  and combined-schema table at line 235 must be updated in the same
  checkpoint so the two stay in lockstep.

## 7. Future Optimisations

The current synthesis allocates a fresh `Int64Array` of length
`row_count` per row group for each system column. `__seq_id` is a
near-monotonic Int and `__batch_id` is a constant; both compress
trivially under existing v1 encodings (`Delta + BitPacking` for
`__seq_id`, `RLE` or a dictionary-of-one for `__batch_id`). A future
optimization can preserve these encoded forms through the encoded scan
path so kernels can skip decoding system columns when they're only
needed for tombstone filtering or `DELETE` materialization. This is a
follow-on; today's allocation is bounded by `row_group_size` so the
worst-case overhead per row-group is small.
