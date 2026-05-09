//! `SegmentReader` — the v0 storage API consumed by scan operators.
//!
//! This module defines the contract between the storage layer
//! (`bqlite-storage`, which implements `SegmentReader` against real
//! segment files) and the scan operator (`bqlite-operators`, which
//! drives the reader through its trait). Both crates sit above
//! `bqlite-core` in the dependency graph — placing the trait here
//! avoids a cycle and lets in-memory fakes in test code implement it
//! without depending on the real storage crate.
//!
//! See [`docs/design/storage/reader-trait.md`](../../../docs/design/storage/reader-trait.md)
//! for the full design note — scope, crate placement, lifecycle,
//! error semantics, and how the Wave 1 surface projects onto the
//! richer storage-format.md §8–§11 model.
//!
//! ## Five pushdown hooks
//!
//! The task spec calls out five capabilities the trait must cover;
//! each maps to one of the types or methods in this module:
//!
//! 1. **Segment enumeration** — [`SegmentReader::segments`] yields
//!    lazy [`SegmentHandle`]s against the query snapshot.
//! 2. **Column projection** — [`ColumnProjection`] passed to
//!    [`SegmentReader::open_segment`] names the columns to decode.
//! 3. **Row-group iteration** — [`SegmentScan::next_row_group`]
//!    streams one `RecordBatch` per row-group.
//! 4. **Zone-map access** — [`SegmentScan::row_group_zone_maps`]
//!    exposes per-column min/max metadata for pruning.
//! 5. **Predicate pushdown** — an optional `Arc<dyn Predicate>` the
//!    reader may consult for zone-map filtering. The real predicate
//!    IR is a Wave 2 DESIGN task; [`Predicate`] here is a narrow
//!    one-method hook that extension is additive on.
//!
//! ## v0 scope exclusions
//!
//! - **Dictionary filter bitsets** (storage-format.md §8.2) — land
//!   with the encoding layer in a later wave, exposed as a
//!   segment-local API alongside `SegmentScan`, not as a trait
//!   method.
//! - **K-way merge across shards/windows** — sits above the
//!   per-segment reader and is a scan-operator concern.
//! - **Async I/O** — the trait is synchronous; an async variant may
//!   land later if single-core overlap becomes load-bearing.

use std::collections::HashMap;
use std::sync::Arc;

use ::arrow::record_batch::RecordBatch;

use crate::error::Result;
use crate::property::PropertyValue;
use crate::schema::TableSchema;

// ─────────────────────────────────────────────────────────────────────────────
// Supporting types
// ─────────────────────────────────────────────────────────────────────────────

/// A handle to a segment visible in the reader's query snapshot.
///
/// Cheap to clone — holds only metadata (five integer fields), no
/// pointers into the segment file. Scan operators may keep many
/// handles alive at once (e.g. one per shard per window when a
/// later-wave k-way merge lands) without worrying about allocation
/// cost. A handle that outlives its [`SegmentReader`] is useless but
/// not unsafe: passing it to [`SegmentReader::open_segment`] on a
/// different reader returns `BqliteError::Execution`.
///
/// # What this handle does *not* carry
///
/// The full `SegmentMeta` in the manifest (storage-format.md §12.3)
/// carries additional fields that are deliberately omitted from the
/// Wave 1 handle:
///
/// - **`level`** — compaction tier. Scan operators do not need it;
///   the reader decides pruning order internally.
/// - **`ts_range` / `entity_range`** — segment-level zone maps.
///   Wave 1 scan operators consult zone maps at the *row-group*
///   granularity through [`SegmentScan::row_group_zone_maps`], so
///   hoisting a segment-level version onto the handle is premature.
///   Adding them later is additive.
/// - **`batch_id` / `created_at` / `byte_size`** — useful for
///   compaction and debugging, not for the read path.
///
/// Keeping the handle minimal means scan operators never accidentally
/// branch on metadata that was not needed for the query.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SegmentHandle {
    /// Monotonically increasing segment identifier assigned from the
    /// manifest counter (storage-format.md §5.2 filename derivation,
    /// §12.3 `SegmentMeta.segment_id`). Unique across the database.
    pub segment_id: u64,
    /// Shard index within the database's fixed shard count
    /// (storage-format.md §5.1).
    pub shard_id: u32,
    /// Identifier of the time window this segment belongs to
    /// (storage-format.md §4.1 for the partition model,
    /// §5.2 `w_<days-since-epoch>` for the naming). `0` is legal and
    /// is the value the Wave 1 stub uses — window partitioning lands
    /// in a later wave.
    pub window_id: u64,
    /// Total rows in the segment across all row-groups.
    pub row_count: u64,
    /// Schema version the segment was written against
    /// (type-system.md §5, storage-format.md §6.4). Used by the
    /// reader to backfill columns added after the segment was
    /// written.
    pub schema_version: u32,
    /// First `__seq_id` in this segment (`SegmentMeta::seq_id_range.0`).
    /// Row at segment-relative offset `n` has `__seq_id = seq_id_first + n`.
    /// Carried here so `bqlite_storage::TombstoneScanWrapper` can
    /// synthesise per-row `__seq_id` values when the scan projection omits
    /// the system column but tombstone filtering still needs it.
    /// `0` is the safe default for callers that do not populate system-column
    /// metadata (e.g. unit-test stub readers).
    pub seq_id_first: u64,
    /// Ingest batch that produced this segment (`SegmentMeta::batch_id`).
    /// Used by `bqlite_storage::TombstoneScanWrapper` to apply batch-level
    /// tombstones when `__batch_id` is absent from the scan output.
    /// `0` is the safe default for callers that do not populate this field.
    pub batch_id: u64,
}

/// Per-row-group, per-column zone map (storage-format.md §11.1).
///
/// Stored inline in the segment footer per storage-format.md §9.4.
/// Readers load these once when a segment is opened and expose them
/// via [`SegmentScan::row_group_zone_maps`] for predicate pruning.
///
/// `min` and `max` use [`PropertyValue`], not Arrow scalars, so the
/// predicate's `accepts_zone` check can run without touching Arrow
/// — the readers convert from Arrow once at load time.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ZoneMap {
    /// Column minimum, or `None` if the row-group is all-null for
    /// this column.
    pub min: Option<PropertyValue>,
    /// Column maximum, or `None` if the row-group is all-null.
    pub max: Option<PropertyValue>,
    /// Number of null values in the row-group for this column.
    pub null_count: u64,
    /// Number of rows in the row-group (the same value is also
    /// available from the scan; duplicated here so a `ZoneMap` is
    /// self-contained for pruning loops).
    pub row_count: u64,
}

impl ZoneMap {
    /// Construct a zone map for a row-group whose column has every
    /// row null. Both `min` and `max` are `None`.
    pub fn all_null(row_count: u64) -> Self {
        Self {
            min: None,
            max: None,
            null_count: row_count,
            row_count,
        }
    }

    /// True when no rows in this column survive a pruning filter —
    /// i.e. there are zero non-null rows (so no value can satisfy
    /// any non-null predicate).
    pub fn is_all_null(&self) -> bool {
        self.row_count != 0 && self.null_count == self.row_count
    }
}

/// Column projection hint passed to [`SegmentReader::open_segment`].
///
/// An empty projection is interpreted as "all declared columns plus
/// the `__seq_id` / `__batch_id` system columns, in table-schema
/// order". This keeps the common no-projection-pruning case a
/// single allocation cheaper than carrying an explicit `::All`
/// variant.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColumnProjection {
    names: Vec<String>,
}

impl ColumnProjection {
    /// Projection naming every column in table-schema order —
    /// declared columns followed by the implicit `__seq_id` and
    /// `__batch_id` system columns. Matches the shape of
    /// `OperatorSchema::from_table(&table)`.
    ///
    /// Equivalent to `ColumnProjection::default()` — provided for
    /// intent clarity at call sites. Callers do **not** need to
    /// spell the system columns explicitly when they want them; use
    /// `all()` (or the default) and the reader includes them.
    pub fn all() -> Self {
        Self::default()
    }

    /// Projection naming an explicit column list in the desired
    /// output order. Column names are copied; duplicates are
    /// preserved verbatim (validation of the projection against the
    /// reader's schema happens inside `open_segment`).
    pub fn with_columns<I, S>(columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            names: columns.into_iter().map(Into::into).collect(),
        }
    }

    /// True if this projection means "every column".
    ///
    /// Readers check this first and fall through to a fast-path when
    /// no projection pruning is requested.
    pub fn is_all(&self) -> bool {
        self.names.is_empty()
    }

    /// Column names in projection order. Empty when [`Self::is_all`].
    pub fn columns(&self) -> &[String] {
        &self.names
    }

    /// Number of explicitly named columns. Zero when
    /// [`Self::is_all`].
    pub fn len(&self) -> usize {
        self.names.len()
    }

    /// True when no columns are explicitly named — which in this
    /// type means "project every column", not "no columns".
    /// Equivalent to [`Self::is_all`]; provided to satisfy Clippy's
    /// `len_without_is_empty` lint. Callers checking whether a
    /// projection selects zero columns should use `len() == 0`
    /// against a projection built with [`Self::with_columns`]; that
    /// shape is legal but is an application-level error for scan.
    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Scan predicate IR
//
// The concrete pushdown IR consumed by the storage layer. `ScanPredicate`
// is a flat conjunction of `ScanConjunct`s — each conjunct is one
// column/operator/literal triple the planner has already validated as
// pushable. See `docs/design/storage/predicate-pushdown.md` §5 for the
// placement rationale (why these types live in `bqlite-core` instead of
// in the optimizer crate) and §6 for the per-conjunct zone-map
// acceptance rules implemented in `ScanConjunct::accepts_zone` below.
// ─────────────────────────────────────────────────────────────────────────────

/// Comparison operator for a [`ScanConjunct::Range`] predicate.
///
/// The planner normalises every range comparison into one of these
/// four forms; `=` and `!=` are separate variants so dictionary
/// rewriting (predicate-pushdown.md §7) can dispatch on the variant
/// rather than parsing an operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeOp {
    /// `col < value`.
    Lt,
    /// `col <= value`.
    Le,
    /// `col > value`.
    Gt,
    /// `col >= value`.
    Ge,
}

/// One conjunct in a [`ScanPredicate`] — a single pushable predicate
/// of the shape `column <op> literal(s)`.
///
/// The flat shape — no nested boolean tree — is deliberate:
/// predicate-pushdown.md §5 pins this representation, and the
/// predicate pushdown optimizer pass (TASK-227) is responsible for
/// collapsing nested `AND`s before building a `ScanPredicate`. The
/// storage layer never sees a boolean expression tree.
///
/// Marked `#[non_exhaustive]` so Wave 4+ can add new variants (bloom
/// filter hooks, pattern matchers, compare-column-to-column) without
/// a breaking change to any crate that matches on a `ScanConjunct`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ScanConjunct {
    /// `column = value`. Dictionary-rewritable (predicate-pushdown.md §7).
    Equal {
        /// Column this conjunct filters on.
        column: String,
        /// Pre-resolved literal the column is compared to.
        value: PropertyValue,
    },
    /// `column != value`.
    NotEqual {
        /// Column this conjunct filters on.
        column: String,
        /// Pre-resolved literal the column is compared to.
        value: PropertyValue,
    },
    /// `column <op> value` for `<`, `<=`, `>`, `>=`.
    Range {
        /// Column this conjunct filters on.
        column: String,
        /// Range comparison operator.
        op: RangeOp,
        /// Pre-resolved literal the column is compared to.
        value: PropertyValue,
    },
    /// `column IN (v1, v2, ..., vk)`. `values` is always non-empty —
    /// empty `IN` lists are collapsed to a constant `false` by the
    /// optimizer (TASK-227) and never reach storage.
    /// Dictionary-rewritable (predicate-pushdown.md §7).
    InSet {
        /// Column this conjunct filters on.
        column: String,
        /// Non-empty set of literals to match against.
        values: Vec<PropertyValue>,
    },
    /// `column IS NULL`.
    IsNull {
        /// Column this conjunct filters on.
        column: String,
    },
    /// `column IS NOT NULL`.
    IsNotNull {
        /// Column this conjunct filters on.
        column: String,
    },
    /// `entity_id ∈ <materialised cohort>` — the runtime form of cohort
    /// entity-id pushdown described in
    /// `docs/design/language/cohorts-aliases-joins.md` §4.3 / §5.2 step 6
    /// and gated per `docs/design/planner/optimizer-direction.md` §7 row 9.
    ///
    /// Constructed by the engine's query coordinator (TASK-522) after a
    /// `SubqueryFilter` materialises its cohort. The `values` set is the
    /// cohort's entity-id column, deduplicated; `set_min` / `set_max`
    /// are the precomputed bounds used for O(1) row-group zone-map
    /// acceptance.
    ///
    /// Functionally similar to a giant `InSet` on the entity-id column
    /// but kept structurally distinct for two reasons:
    ///
    /// 1. The post-scan `SubqueryFilterOperator` is the source of truth
    ///    for the row-level probe; this conjunct exists *only* to drive
    ///    zone-map row-group skipping and must not duplicate per-row
    ///    work.
    /// 2. The values live behind an `Arc<Vec<...>>` so the same
    ///    materialised cohort can back several `EntityIn` instances
    ///    (one per scan that the cohort filters) without a deep clone.
    ///    `InSet`'s un-shared `Vec<PropertyValue>` would force a deep
    ///    clone per scan.
    ///
    /// Construct via [`ScanConjunct::entity_in`] — struct-literal
    /// construction must preserve `values` sorted and deduplicated with
    /// `set_min == values[0]` and `set_max == values[len - 1]`.
    EntityIn {
        /// Outer scan's entity-key column name (typically `"entity_id"`).
        column: String,
        /// Sorted, deduplicated cohort entity-id values. Held behind
        /// `Arc` so the engine can share one set across multiple scans
        /// without cloning. The storage layer only consults
        /// `[set_min, set_max]` for row-group acceptance; this vector
        /// is preserved so future per-row dictionary or bloom kernels
        /// can binary-search it without re-deduplicating, and so the
        /// pushdown is stably equality-comparable for plan-tree diffs.
        ///
        /// `PropertyValue` is `Eq + Ord` but not `Hash`, so a sorted
        /// `Vec` is the right shape — `HashSet` would require an
        /// additional trait surface that is not load-bearing for the
        /// pushdown path.
        values: Arc<Vec<PropertyValue>>,
        /// Pre-computed minimum entity-id (`values[0]` after sort).
        /// Carried with the conjunct so `accepts_zone` does not
        /// re-walk the vector per row-group.
        set_min: PropertyValue,
        /// Pre-computed maximum entity-id (`values[len - 1]`).
        set_max: PropertyValue,
    },
}

impl ScanConjunct {
    /// Column name this conjunct filters on. Used by
    /// [`ScanPredicate::accepts_zone_group`], the dictionary
    /// rewriter, and inline zone-map evaluation in the storage reader
    /// (TASK-247) to group conjuncts by their target column.
    pub fn column(&self) -> &str {
        match self {
            ScanConjunct::Equal { column, .. }
            | ScanConjunct::NotEqual { column, .. }
            | ScanConjunct::Range { column, .. }
            | ScanConjunct::InSet { column, .. }
            | ScanConjunct::IsNull { column }
            | ScanConjunct::IsNotNull { column }
            | ScanConjunct::EntityIn { column, .. } => column,
        }
    }

    /// Build an [`ScanConjunct::EntityIn`] from a `Vec` of cohort
    /// entity-id values. The constructor sorts and deduplicates the
    /// vector, capturing `set_min` / `set_max` from the resulting
    /// boundary entries so per-row-group acceptance stays O(1)
    /// regardless of cohort size. Returns `None` when `values` is
    /// empty — an empty cohort filters every outer row, but this case
    /// is already handled by the post-scan `SubqueryFilterOperator`
    /// (which probes against an empty set and rejects every row);
    /// the pushdown adds nothing in that degenerate case.
    pub fn entity_in(column: String, mut values: Vec<PropertyValue>) -> Option<Self> {
        values.sort_unstable();
        values.dedup();
        let set_min = values.first()?.clone();
        let set_max = values.last()?.clone();
        Some(ScanConjunct::EntityIn {
            column,
            values: Arc::new(values),
            set_min,
            set_max,
        })
    }

    /// Per-conjunct zone-map acceptance — implements the Wave 2 rule
    /// table from `docs/design/storage/predicate-pushdown.md` §6.
    ///
    /// Returns `true` iff the row-group described by `zone` **might**
    /// contain at least one row that satisfies this conjunct. The
    /// "might" is load-bearing: zone maps are a coarse summary, so a
    /// row-group that passes this check is still re-evaluated row-by-
    /// row by the filter operator above the scan. A row-group that
    /// **fails** this check is unconditionally skipped — the no-false-
    /// negatives invariant (no matching row is ever pruned) is what
    /// makes the pruning correct. Every per-variant branch below is
    /// conservative: when in doubt, it accepts.
    pub fn accepts_zone(&self, zone: &ZoneMap) -> bool {
        let nulls = zone.null_count;
        let rows = zone.row_count;
        match self {
            // `Equal`: need at least one non-null row, and the literal
            // must lie within [min, max] when the bounds are populated.
            // `is_none_or` accepts conservatively when the writer did
            // not populate one of the bounds (v1 writers always do, but
            // the rule stays correct for a future partial-bound writer).
            ScanConjunct::Equal { value, .. } => {
                nulls < rows
                    && zone.min.as_ref().is_none_or(|m| m <= value)
                    && zone.max.as_ref().is_none_or(|x| value <= x)
            }
            // `NotEqual`: reject only when every non-null row equals
            // `value` AND there are zero nulls. A row-group with
            // `min == max == value && nulls > 0` still has null rows,
            // but those rows produce `UNKNOWN` under `!=` three-valued
            // logic and are dropped by the filter operator — so the
            // correct answer is still "reject", but we only know the
            // exact count from the writer when `nulls == 0`.
            ScanConjunct::NotEqual { value, .. } => {
                !(nulls == 0
                    && zone.min.as_ref() == Some(value)
                    && zone.max.as_ref() == Some(value))
            }
            // `Range`: the bound check differs per operator. A
            // row-group with only nulls rejects immediately because
            // every non-null value is needed to satisfy the range.
            ScanConjunct::Range { op, value, .. } => match op {
                RangeOp::Lt => nulls < rows && zone.min.as_ref().is_some_and(|m| m < value),
                RangeOp::Le => nulls < rows && zone.min.as_ref().is_some_and(|m| m <= value),
                RangeOp::Gt => nulls < rows && zone.max.as_ref().is_some_and(|x| value < x),
                RangeOp::Ge => nulls < rows && zone.max.as_ref().is_some_and(|x| value <= x),
            },
            // `InSet`: accept if any literal could fall within the
            // row-group's [min, max] range. Linear over `values` —
            // parsed `IN (...)` lists are bounded and small.
            ScanConjunct::InSet { values, .. } => {
                nulls < rows
                    && values.iter().any(|v| {
                        zone.min.as_ref().is_none_or(|m| m <= v)
                            && zone.max.as_ref().is_none_or(|x| v <= x)
                    })
            }
            // `IS NULL`: accept if the row-group has at least one null.
            ScanConjunct::IsNull { .. } => nulls > 0,
            // `IS NOT NULL`: accept if the row-group has at least one
            // non-null row.
            ScanConjunct::IsNotNull { .. } => nulls < rows,
            // `EntityIn`: accept iff the cohort's value range overlaps
            // the row-group's [min, max]. Pre-computed bounds make this
            // O(1) per row-group regardless of cohort size. NULL rows
            // produce UNKNOWN under set membership and the filter
            // operator drops them; reject row-groups that are all-null.
            ScanConjunct::EntityIn {
                set_min, set_max, ..
            } => {
                nulls < rows
                    && zone.min.as_ref().is_none_or(|m| m <= set_max)
                    && zone.max.as_ref().is_none_or(|x| set_min <= x)
            }
        }
    }
}

/// A conjunctive (AND) set of pushable scan predicates in the flat
/// shape the storage layer can evaluate directly.
///
/// Constructed by the scan operator (TASK-230) from its
/// `scan_predicates: Vec<CompiledExpr>` and handed to storage via the
/// `Option<Arc<dyn Predicate>>` parameter on `open_segment`. The
/// storage reader loops this predicate's `conjuncts` at row-group
/// boundaries to decide which row groups to decode.
///
/// An empty `conjuncts` vec is legal and means "no pushdown" — the
/// scan returns every row the projection produces and leaves all
/// filtering to the filter operator above. This is the pre-pushdown
/// baseline.
///
/// `ScanPredicate` is `Clone`-cheap because the struct is a single
/// heap allocation for each vector — the intention is that scan
/// operators construct exactly one per query and share it across
/// every segment they open via `Arc<dyn Predicate>`.
#[derive(Debug, Clone, Default)]
pub struct ScanPredicate {
    /// Conjuncts, in the order the storage layer will evaluate them.
    /// Acceptance is short-circuit `AND`, so the first conjunct that
    /// rejects a row-group terminates evaluation — the optimizer can
    /// hint pruning order by sorting this vec (not exploited in
    /// Wave 2; see predicate-pushdown.md §6.1).
    pub conjuncts: Vec<ScanConjunct>,
    /// Cached set of columns referenced by any conjunct, in first-
    /// occurrence order, with duplicates removed. Populated at
    /// construction so [`Predicate::referenced_columns`] can return a
    /// borrowed slice without re-walking `conjuncts` on every call.
    referenced: Vec<String>,
}

impl ScanPredicate {
    /// Construct a `ScanPredicate` from a flat conjunct list.
    ///
    /// Populates the internal `referenced_columns` cache by walking
    /// the conjuncts once. Callers building from a `Vec<CompiledExpr>`
    /// (TASK-230 scan operator) build the conjunct list first and
    /// hand it off in one call — no conjunct is ever added or removed
    /// after construction.
    pub fn new(conjuncts: Vec<ScanConjunct>) -> Self {
        let mut referenced: Vec<String> = Vec::new();
        for c in &conjuncts {
            let col = c.column();
            if !referenced.iter().any(|r| r == col) {
                referenced.push(col.to_string());
            }
        }
        Self {
            conjuncts,
            referenced,
        }
    }

    /// True when this predicate has no conjuncts — equivalent to "no
    /// pushdown" on the storage side. Scan operators may short-circuit
    /// the `Option<Arc<dyn Predicate>>` argument to `None` in this
    /// case to avoid an extra `Arc` clone; either form is correct.
    pub fn is_empty(&self) -> bool {
        self.conjuncts.is_empty()
    }

    /// Number of conjuncts — exposed primarily for benches and
    /// `Debug`-time assertions.
    pub fn len(&self) -> usize {
        self.conjuncts.len()
    }
}

/// Outcome of a dictionary rewrite for a single column
/// (predicate-pushdown.md §7 / §10). Returned from
/// [`Predicate::resolve_dictionary_codes`].
///
/// The reader calls the hook once per dictionary-encoded projected
/// column and dispatches on the returned shape:
///
/// - `NoRewrite`: no dict-rewritable conjunct on this column; proceed
///   as if the method had not been called (range conjuncts and post-
///   filter still apply).
/// - `EmptySet`: at least one dict-rewritable conjunct exists, and
///   **none** of its literals resolved. The row-group yields zero
///   rows for that conjunct — the reader short-circuits to an empty
///   batch without decoding the remaining columns.
/// - `Codes(codes)`: at least one literal resolved. The reader builds
///   a bitmask against the row-group's bit-packed code stream and
///   uses it to filter during decode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DictRewrite {
    /// No dict-rewritable conjunct on this column — proceed as if the
    /// method had not been called.
    NoRewrite,
    /// At least one dict-rewritable conjunct exists, but none of its
    /// literals resolved to codes in the segment dictionary — the
    /// row-group yields zero matching rows for this conjunct.
    EmptySet,
    /// Resolved code set. Non-empty by construction; duplicate codes
    /// are legal and handled by the reader's mask-construction loop.
    Codes(Vec<u32>),
}

/// Read-only view over a segment-level dictionary, handed to
/// [`Predicate::resolve_dictionary_codes`] so the trait implementer
/// can rewrite `Equal`/`InSet` literals into the segment's code space
/// without depending on the storage crate's dictionary internals.
///
/// The shape — a slice of sorted distinct values — matches the
/// on-disk layout specified by storage-format.md §11.1 (Wave 2
/// dictionary footer: sorted distinct values, code is the position).
#[derive(Debug)]
pub struct DictionaryIndex<'a> {
    /// Sorted distinct dictionary values. The value at index `i` is
    /// assigned code `i` (0-based, `u32`).
    pub values: &'a [PropertyValue],
}

impl DictionaryIndex<'_> {
    /// Resolve a literal to its dictionary code via binary search.
    ///
    /// Returns `None` when the literal is absent from the dictionary —
    /// the storage reader treats such a literal as "never matches"
    /// for the purposes of the bitmask rewrite.
    pub fn code_of(&self, value: &PropertyValue) -> Option<u32> {
        self.values.binary_search(value).ok().map(|i| i as u32)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Predicate
// ─────────────────────────────────────────────────────────────────────────────

/// Predicate pushdown hook consumed by [`SegmentReader::open_segment`].
///
/// The Wave 2 surface is four methods wide:
///
/// 1. [`accepts_zone`](Self::accepts_zone) — single-column zone-map
///    acceptance, retained from Wave 1 for back-compat.
/// 2. [`accepts_zone_group`](Self::accepts_zone_group) — whole-row-
///    group acceptance taking the complete `column → ZoneMap` map.
///    `ScanPredicate` overrides the default body to respect conjuncts
///    on columns absent from `zones`.
/// 3. [`referenced_columns`](Self::referenced_columns) — columns the
///    pruning loop should feed into the acceptance methods.
/// 4. [`resolve_dictionary_codes`](Self::resolve_dictionary_codes) —
///    dictionary rewrite hook for `Equal`/`InSet` conjuncts on
///    dictionary-encoded columns, per predicate-pushdown.md §7.
///
/// The trait stays object-safe (`Arc<dyn Predicate>`) — none of the
/// methods return `Self` or use generics. The Wave 2 extension is
/// additive: every new method has a default body that preserves the
/// Wave 1 "conservative accept" behaviour, so existing `Predicate`
/// impls keep compiling without touching them.
///
/// Implementations are held behind `Arc` so many `SegmentScan`s can
/// share a single predicate without per-scan cloning.
pub trait Predicate: Send + Sync + std::fmt::Debug + 'static {
    /// Downcast hook for the inline zone-map evaluation fast path
    /// (TASK-247). Implementors that override this method with
    /// `self` enable the storage reader to downcast `Arc<dyn
    /// Predicate>` to a concrete type without changing the trait's
    /// object-safety story.
    fn as_any(&self) -> &dyn std::any::Any {
        // Default: return nothing useful. ScanPredicate overrides.
        &()
    }

    /// True if the predicate **might** accept at least one row in a
    /// column range described by `zone`.
    ///
    /// Returning `false` tells the reader to skip the range entirely
    /// — the scan operator never sees those rows. Conservative
    /// implementations may always return `true`; this disables
    /// zone-map pruning but is always safe.
    ///
    /// `column` is the fully-qualified column name the caller is
    /// asking about, not a position. Readers call this method once
    /// per row-group per [`referenced_columns`](Self::referenced_columns)
    /// entry; implementations must be cheap.
    ///
    /// For multi-column predicates, this method answers **about
    /// `column` in isolation** — conjuncts on other columns are not
    /// consulted. Callers that have access to the complete
    /// `column → ZoneMap` map should prefer
    /// [`accepts_zone_group`](Self::accepts_zone_group).
    fn accepts_zone(&self, column: &str, zone: &ZoneMap) -> bool;

    /// Whole-row-group zone-map acceptance.
    ///
    /// The caller supplies the complete `column → ZoneMap` dictionary
    /// from [`SegmentScan::row_group_zone_maps`]. The implementer
    /// returns `true` iff at least one row in the row-group **might**
    /// satisfy every conjunct. Conjuncts that reference a column
    /// absent from `zones` are treated **conservatively** — they
    /// accept unconditionally (the reader must not prune a row-group
    /// just because a zone was not recorded, per
    /// `docs/design/storage/reader-trait.md` §5.1).
    ///
    /// The default body iterates `zones` and calls `accepts_zone`
    /// per entry — a correct fallback for Wave 1 impls that only
    /// meaningfully answer per-column, but one that misses conjuncts
    /// on columns absent from the map. Multi-column predicates
    /// ([`ScanPredicate`] in particular) override this to walk their
    /// own conjuncts instead.
    fn accepts_zone_group(&self, zones: &HashMap<String, ZoneMap>) -> bool {
        zones.iter().all(|(col, zone)| self.accepts_zone(col, zone))
    }

    /// Columns whose zone maps the reader should feed into
    /// [`accepts_zone`](Self::accepts_zone) or
    /// [`accepts_zone_group`](Self::accepts_zone_group).
    ///
    /// Pruning is driven from this list, **not** the scan's
    /// projection — a predicate on a column that the scan does not
    /// otherwise read (e.g. `WHERE amount > 100 | SELECT user_id`)
    /// must still get the chance to prune row groups. The default
    /// implementation returns an empty slice, which disables zone-map
    /// pruning for the predicate (always safe, just slow).
    fn referenced_columns(&self) -> &[String] {
        const EMPTY: &[String] = &[];
        EMPTY
    }

    /// Dictionary rewrite hook for `Equal` / `InSet` conjuncts on a
    /// dictionary-encoded column (predicate-pushdown.md §7).
    ///
    /// The reader calls this once per projected column whose column
    /// chunk uses Dictionary encoding and hands over a view of the
    /// segment-level dictionary. The implementer inspects its own
    /// conjuncts on `column` and returns a [`DictRewrite`] describing
    /// how the reader should use the bit-packed code stream.
    ///
    /// The default body returns [`DictRewrite::NoRewrite`], so Wave 1
    /// `Predicate` impls (and any future impl that does not care
    /// about dictionary rewriting) are unaffected.
    fn resolve_dictionary_codes(&self, _column: &str, _dict: &DictionaryIndex<'_>) -> DictRewrite {
        DictRewrite::NoRewrite
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ScanPredicate: Predicate impl
// ─────────────────────────────────────────────────────────────────────────────

impl Predicate for ScanPredicate {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn accepts_zone(&self, column: &str, zone: &ZoneMap) -> bool {
        // Only the conjuncts that reference `column` have anything to
        // say about this zone — conjuncts on other columns are not
        // consulted here, per the single-column contract. Callers
        // that need the full row-group decision (the reader's
        // pruning loop is one) call `accepts_zone_group` instead.
        self.conjuncts
            .iter()
            .filter(|c| c.column() == column)
            .all(|c| c.accepts_zone(zone))
    }

    fn accepts_zone_group(&self, zones: &HashMap<String, ZoneMap>) -> bool {
        // Walk our conjuncts (not `zones`) so conjuncts on columns
        // absent from `zones` conservatively accept — the default
        // body iterates `zones` and would silently miss them.
        self.conjuncts.iter().all(|c| match zones.get(c.column()) {
            Some(zone) => c.accepts_zone(zone),
            None => true,
        })
    }

    fn referenced_columns(&self) -> &[String] {
        &self.referenced
    }

    fn resolve_dictionary_codes(&self, column: &str, dict: &DictionaryIndex<'_>) -> DictRewrite {
        // Walk conjuncts on `column`, collecting resolved codes from
        // every Equal / InSet. Range, NotEqual, IsNull, IsNotNull,
        // EntityIn are never dictionary-rewritable — they fall through
        // to zone-map pruning plus post-filter. If every literal fails
        // to resolve, the conjunct definitely-rejects every row in the
        // segment.
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
                _ => {
                    // Range / NotEqual / IsNull / IsNotNull — not
                    // dict-rewritable; post-filter handles them.
                }
            }
        }
        match (any_rewritable, codes.is_empty()) {
            (false, _) => DictRewrite::NoRewrite,
            (true, true) => DictRewrite::EmptySet,
            (true, false) => DictRewrite::Codes(codes),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SegmentScan
// ─────────────────────────────────────────────────────────────────────────────

/// Streaming read over a single segment's row-groups.
///
/// Obtained from [`SegmentReader::open_segment`] and iterated to
/// completion (or dropped early). Dropping releases any OS resources
/// the scan holds — memory maps, file handles, decompression state.
///
/// One `SegmentScan` corresponds to one segment file. A scan
/// operator reads many segments by calling
/// [`SegmentReader::open_segment`] repeatedly; each call returns a
/// fresh `SegmentScan`.
pub trait SegmentScan: Send {
    /// Number of row-groups in this segment.
    ///
    /// Known before iteration starts (row-group count is in the
    /// segment footer per storage-format.md §9.2). Scan operators
    /// use it to size per-row-group scratch buffers and to drive
    /// zone-map pruning loops.
    fn row_group_count(&self) -> usize;

    /// Per-column zone maps for row-group `idx`.
    ///
    /// Column presence is not guaranteed — zone maps are absent
    /// when a row-group is all-null for a column, when the column
    /// was added after the segment was written (schema evolution),
    /// or when the storage layer chose not to maintain zone maps
    /// for the column. Callers check presence per column.
    ///
    /// Returning `Ok(HashMap::new())` is legal and means "no zone
    /// maps for this row-group" — the scan operator must then
    /// assume the row-group is not prunable and read it.
    ///
    /// Implementations should not load the row-group's data to
    /// answer this call; it is a metadata-only read off the footer.
    ///
    /// # Return type
    ///
    /// Returns an owned `HashMap` rather than `&HashMap`. The scan
    /// operator calls this once per row-group before deciding
    /// whether to decode it — the clone is amortized against the
    /// row-group decode cost (64K-row decompress + decode) and keeps
    /// stubs that compute zone maps lazily from having to cache a
    /// materialized map internally.
    fn row_group_zone_maps(&self, idx: usize) -> Result<HashMap<String, ZoneMap>>;

    /// Yield the next row-group as a `RecordBatch`, or `Ok(None)`
    /// when the segment is exhausted.
    ///
    /// Batch shape:
    ///
    /// - Column order matches the [`ColumnProjection`] passed to
    ///   [`SegmentReader::open_segment`] — or table-schema order
    ///   when the projection was [`ColumnProjection::all`].
    /// - Each returned batch contains exactly one row-group worth of
    ///   rows (no concatenation, no splitting).
    /// - Nullable columns carry Arrow null bitmaps; non-nullable
    ///   columns do not (execution-model.md §3.7).
    /// - An empty batch is legal for row-groups the predicate fully
    ///   pruned. Consumers must tolerate zero-row batches.
    /// - Implementations may return `Ok(None)` early if they know
    ///   every remaining row-group is pruned.
    ///
    /// After the first `Ok(None)`, subsequent calls must continue
    /// returning `Ok(None)` without side effects.
    fn next_row_group(&mut self) -> Result<Option<RecordBatch>>;

    /// Yield the next row-group as an encoded-preserving
    /// `EncodedBatch`, or `Ok(None)` when the segment is exhausted.
    ///
    /// This is the encoded-path read hook described in
    /// `docs/design/storage/zero-copy-scan-filter.md` and
    /// `docs/design/storage/reader-trait.md`. It lets scan operators
    /// consume encoded column chunks directly and run selection-first
    /// predicate kernels over them without eagerly materializing every
    /// row-group into an Arrow array.
    ///
    /// # Default implementation
    ///
    /// The default delegates to [`SegmentScan::next_row_group`] and
    /// wraps each resulting Arrow column into
    /// `EncodedColumn::Materialized`. This means every existing
    /// `SegmentScan` implementor works on the encoded-path surface
    /// with no behavior change — they just stay on the materialized
    /// fallback until they override this method with a real encoded
    /// reader.
    ///
    /// # Contract
    ///
    /// - Column order matches the [`ColumnProjection`] passed to
    ///   [`SegmentReader::open_segment`] — identical to
    ///   `next_row_group`.
    /// - Returned `EncodedBatch::row_count` equals the row count of
    ///   the equivalent `next_row_group` call. Every column in the
    ///   batch has the same `rows()` value.
    /// - Implementors must not mix encoded and materialized-fallback
    ///   iteration within one scan instance — pick one path for the
    ///   lifetime of the scan.
    /// - After the first `Ok(None)`, subsequent calls must continue
    ///   returning `Ok(None)` without side effects.
    fn next_encoded_row_group(&mut self) -> Result<Option<crate::encoded::EncodedBatch>> {
        match self.next_row_group()? {
            None => Ok(None),
            Some(batch) => {
                let row_count = batch.num_rows() as u32;
                let columns = batch
                    .columns()
                    .iter()
                    .map(|array| crate::encoded::EncodedColumn::Materialized {
                        array: array.clone(),
                        rows: array.len() as u32,
                    })
                    .collect();
                Ok(Some(crate::encoded::EncodedBatch::new(row_count, columns)))
            }
        }
    }

    /// Attach a copy-budget `Metrics` handle.
    ///
    /// Implementations that record the
    /// `docs/design/storage/zero-copy-scan-filter.md` §3 counters
    /// (`bytes_scanned`, `bytes_decompressed`,
    /// `bytes_materialized_before_filter`) override this to swap the
    /// handle in. The default body is a no-op so existing
    /// implementations stay compiling without behavior change.
    ///
    /// Wrapper scans (e.g. the tombstone wrapper) override this to
    /// forward the handle to the inner scan(s) so the counters
    /// reach the production `SegmentFileScan` underneath. Future
    /// wrappers (k-way merge) follow the same pattern.
    ///
    /// May be called more than once; the latest call wins. Typically
    /// invoked by the scan operator immediately after
    /// [`SegmentReader::open_segment`].
    fn attach_metrics(&mut self, _metrics: std::sync::Arc<dyn crate::metrics::Metrics>) {}
}

// ─────────────────────────────────────────────────────────────────────────────
// SegmentReader
// ─────────────────────────────────────────────────────────────────────────────

/// Read-side API for a table's segments — the entry point scan
/// operators use to read data out of the storage layer.
///
/// A `SegmentReader` is a per-query, per-table snapshot of the
/// manifest's live segment inventory (storage-format.md §7.6 query
/// snapshots). The engine obtains one from `Database` at query
/// start, hands it to the scan operator, and drops it when the
/// query completes.
///
/// Iteration pattern:
///
/// ```text
/// let reader: Box<dyn SegmentReader> = db.segment_reader("events")?;
/// for handle in reader.segments() {
///     let handle = handle?;
///     let mut scan = reader.open_segment(
///         &handle,
///         &ColumnProjection::all(),
///         None, // no predicate pushdown
///     )?;
///     while let Some(batch) = scan.next_row_group()? {
///         // process batch
///     }
/// }
/// ```
///
/// `Send + Sync` so a single reader can be shared across scan
/// threads in a later-wave parallel execution model. Implementations
/// that hold non-thread-safe state must serialize access internally.
pub trait SegmentReader: Send + Sync {
    /// The table schema this reader produces rows against.
    ///
    /// This is the *current* schema from the manifest
    /// (storage-format.md §12.2), not any individual segment's
    /// write-time schema. Implementations fill columns missing from
    /// older segments with NULL or the column's default value
    /// before returning a `RecordBatch`.
    fn schema(&self) -> &TableSchema;

    /// Enumerate segments visible to this reader's snapshot.
    ///
    /// Iteration order is implementation-defined but stable for the
    /// lifetime of the reader. Returning an iterator (rather than a
    /// `Vec`) lets large manifests stream lazily and keeps the
    /// Wave 1 stub trivial: the empty-iterator case is
    /// `Box::new(std::iter::empty())`.
    ///
    /// Errors yielded mid-iteration abort the query — the scan
    /// operator propagates them up without opening further
    /// segments.
    fn segments(&self) -> Box<dyn Iterator<Item = Result<SegmentHandle>> + Send + '_>;

    /// Open a streaming scan over a segment.
    ///
    /// # Arguments
    ///
    /// - `handle` must be a value returned by [`Self::segments`] on
    ///   the same reader; opening a stale, unknown, or foreign
    ///   handle returns `BqliteError::Execution`.
    /// - `projection` names the columns to decode. `is_all()` means
    ///   "every declared column plus the `__seq_id` / `__batch_id`
    ///   system columns, in table-schema order". Any named column
    ///   that is not present in [`Self::schema`] returns
    ///   `BqliteError::Schema`.
    /// - `predicate` is an optional pushdown hint. Passing `None`
    ///   disables zone-map pruning for this scan; passing
    ///   `Some(pred)` lets the reader call `pred.accepts_zone(…)`
    ///   to skip row-groups.
    ///
    /// The returned `SegmentScan` owns the per-segment OS resources
    /// (a memory map, a file handle, or an in-memory buffer in the
    /// Wave 1 stub). Dropping it releases those resources; there is
    /// no explicit `close()`.
    fn open_segment(
        &self,
        handle: &SegmentHandle,
        projection: &ColumnProjection,
        predicate: Option<Arc<dyn Predicate>>,
    ) -> Result<Box<dyn SegmentScan>>;
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicUsize, Ordering};

    use ::arrow::array::{Int64Array, RecordBatch as ArrowRecordBatch, StringArray};
    use ::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

    use crate::error::BqliteError;
    use crate::property::BqlType;
    use crate::schema::ColumnDef;

    // ── SegmentHandle ────────────────────────────────────────────────────────

    #[test]
    fn segment_handle_is_clone_eq_hash() {
        let a = SegmentHandle {
            segment_id: 1,
            shard_id: 2,
            window_id: 3,
            row_count: 4,
            schema_version: 5,
            seq_id_first: 0,
            batch_id: 0,
        };
        let b = a.clone();
        assert_eq!(a, b);
        // Hashable — used as map keys by scan operators.
        let mut set = std::collections::HashSet::new();
        set.insert(a.clone());
        assert!(set.contains(&b));
    }

    // ── ZoneMap ──────────────────────────────────────────────────────────────

    #[test]
    fn zone_map_default_is_empty() {
        let z = ZoneMap::default();
        assert_eq!(z.min, None);
        assert_eq!(z.max, None);
        assert_eq!(z.null_count, 0);
        assert_eq!(z.row_count, 0);
        assert!(!z.is_all_null()); // zero-row is not "all null"
    }

    #[test]
    fn zone_map_all_null_constructor() {
        let z = ZoneMap::all_null(64);
        assert_eq!(z.min, None);
        assert_eq!(z.max, None);
        assert_eq!(z.null_count, 64);
        assert_eq!(z.row_count, 64);
        assert!(z.is_all_null());
    }

    #[test]
    fn zone_map_is_all_null_requires_matching_counts() {
        let mostly = ZoneMap {
            min: Some(PropertyValue::Int(1)),
            max: Some(PropertyValue::Int(10)),
            null_count: 5,
            row_count: 10,
        };
        assert!(!mostly.is_all_null());
    }

    // ── ColumnProjection ─────────────────────────────────────────────────────

    #[test]
    fn column_projection_default_is_all() {
        let p = ColumnProjection::default();
        assert!(p.is_all());
        assert!(p.is_empty());
        assert_eq!(p.len(), 0);
        assert!(p.columns().is_empty());
    }

    #[test]
    fn column_projection_all_matches_default() {
        assert_eq!(ColumnProjection::all(), ColumnProjection::default());
    }

    #[test]
    fn column_projection_with_columns_preserves_order_and_duplicates() {
        let p = ColumnProjection::with_columns(["amount", "ts", "amount"]);
        assert!(!p.is_all());
        assert_eq!(p.len(), 3);
        assert_eq!(p.columns(), &["amount", "ts", "amount"]);
    }

    #[test]
    fn column_projection_with_columns_accepts_owned_and_borrowed_strings() {
        let _owned: ColumnProjection =
            ColumnProjection::with_columns(vec![String::from("a"), String::from("b")]);
        let _borrowed: ColumnProjection = ColumnProjection::with_columns(["a", "b"]);
    }

    // ── Predicate (object-safety + zone filter) ──────────────────────────────

    #[derive(Debug)]
    struct EventTypePrefixPredicate {
        prefix: &'static str,
    }

    impl Predicate for EventTypePrefixPredicate {
        fn accepts_zone(&self, column: &str, zone: &ZoneMap) -> bool {
            if column != "event_type" {
                // Predicate only talks about event_type; every other
                // column passes through.
                return true;
            }
            // Accept only if *some* string in [min, max] could start
            // with our prefix. A naive lexicographic check: the
            // maximum must be >= prefix.
            match &zone.max {
                Some(PropertyValue::String(s)) => s.as_str() >= self.prefix,
                _ => true,
            }
        }
    }

    #[test]
    fn predicate_is_object_safe() {
        let p: Arc<dyn Predicate> = Arc::new(EventTypePrefixPredicate { prefix: "p" });
        let yes = ZoneMap {
            min: Some(PropertyValue::String("apple".into())),
            max: Some(PropertyValue::String("zebra".into())),
            null_count: 0,
            row_count: 64,
        };
        let no = ZoneMap {
            min: Some(PropertyValue::String("apple".into())),
            max: Some(PropertyValue::String("orange".into())),
            null_count: 0,
            row_count: 64,
        };
        assert!(p.accepts_zone("event_type", &yes));
        assert!(!p.accepts_zone("event_type", &no));
        // Non-target columns pass through unconditionally.
        assert!(p.accepts_zone("other", &no));
    }

    // ── In-memory fake reader ────────────────────────────────────────────────
    //
    // Exercises the trait objects end-to-end to prove the contracts
    // compose: object safety, Box<dyn Iterator>, Arc<dyn Predicate>,
    // Box<dyn SegmentScan>. Later waves add real readers in
    // bqlite-storage; Wave 1 relies on this fake to validate the
    // trait surface before TASK-116 / TASK-117 land.

    struct FakeReader {
        schema: TableSchema,
        handles: Vec<SegmentHandle>,
    }

    struct FakeScan {
        batches: Vec<RecordBatch>,
        zone_maps: Vec<HashMap<String, ZoneMap>>,
        position: usize,
    }

    impl SegmentReader for FakeReader {
        fn schema(&self) -> &TableSchema {
            &self.schema
        }

        fn segments(&self) -> Box<dyn Iterator<Item = Result<SegmentHandle>> + Send + '_> {
            Box::new(self.handles.iter().cloned().map(Ok))
        }

        fn open_segment(
            &self,
            handle: &SegmentHandle,
            _projection: &ColumnProjection,
            _predicate: Option<Arc<dyn Predicate>>,
        ) -> Result<Box<dyn SegmentScan>> {
            if !self.handles.iter().any(|h| h == handle) {
                return Err(BqliteError::Execution(format!(
                    "unknown segment handle {handle:?}"
                )));
            }
            // Build one row-group with a tiny RecordBatch matching
            // the minimal schema columns (entity_id, ts, event_type).
            let arrow_schema = Arc::new(ArrowSchema::new(vec![
                Field::new("entity_id", DataType::Utf8, false),
                Field::new("ts", DataType::Int64, false),
                Field::new("event_type", DataType::Utf8, false),
            ]));
            let batch = ArrowRecordBatch::try_new(
                arrow_schema,
                vec![
                    Arc::new(StringArray::from(vec!["u1", "u1", "u2"])),
                    Arc::new(Int64Array::from(vec![100_i64, 200, 300])),
                    Arc::new(StringArray::from(vec!["signup", "purchase", "signup"])),
                ],
            )
            .expect("valid RecordBatch");
            let mut zone_maps = HashMap::new();
            zone_maps.insert(
                "entity_id".to_string(),
                ZoneMap {
                    min: Some(PropertyValue::String("u1".into())),
                    max: Some(PropertyValue::String("u2".into())),
                    null_count: 0,
                    row_count: 3,
                },
            );
            zone_maps.insert(
                "ts".to_string(),
                ZoneMap {
                    min: Some(PropertyValue::Timestamp(100)),
                    max: Some(PropertyValue::Timestamp(300)),
                    null_count: 0,
                    row_count: 3,
                },
            );
            Ok(Box::new(FakeScan {
                batches: vec![batch],
                zone_maps: vec![zone_maps],
                position: 0,
            }))
        }
    }

    impl SegmentScan for FakeScan {
        fn row_group_count(&self) -> usize {
            self.batches.len()
        }

        fn row_group_zone_maps(&self, idx: usize) -> Result<HashMap<String, ZoneMap>> {
            self.zone_maps
                .get(idx)
                .cloned()
                .ok_or_else(|| BqliteError::Execution(format!("row-group {idx} out of range")))
        }

        fn next_row_group(&mut self) -> Result<Option<RecordBatch>> {
            if self.position >= self.batches.len() {
                return Ok(None);
            }
            let batch = self.batches[self.position].clone();
            self.position += 1;
            Ok(Some(batch))
        }
    }

    fn minimal_schema() -> TableSchema {
        TableSchema::new(
            "events",
            vec![
                ColumnDef::required("entity_id", BqlType::String),
                ColumnDef::required("ts", BqlType::Timestamp),
                ColumnDef::required("event_type", BqlType::String),
            ],
            "entity_id",
            "ts",
            "event_type",
        )
        .unwrap()
    }

    #[test]
    fn fake_reader_iterates_segments() {
        let reader = FakeReader {
            schema: minimal_schema(),
            handles: vec![
                SegmentHandle {
                    segment_id: 1,
                    shard_id: 0,
                    window_id: 0,
                    row_count: 3,
                    schema_version: 0,
                    seq_id_first: 0,
                    batch_id: 0,
                },
                SegmentHandle {
                    segment_id: 2,
                    shard_id: 0,
                    window_id: 0,
                    row_count: 3,
                    schema_version: 0,
                    seq_id_first: 0,
                    batch_id: 0,
                },
            ],
        };
        let segs: Vec<SegmentHandle> = reader.segments().map(|r| r.unwrap()).collect();
        assert_eq!(segs.len(), 2);
        assert_eq!(segs[0].segment_id, 1);
        assert_eq!(segs[1].segment_id, 2);
    }

    #[test]
    fn fake_reader_opens_and_drives_scan_to_exhaustion() {
        let reader = FakeReader {
            schema: minimal_schema(),
            handles: vec![SegmentHandle {
                segment_id: 1,
                shard_id: 0,
                window_id: 0,
                row_count: 3,
                schema_version: 0,
                seq_id_first: 0,
                batch_id: 0,
            }],
        };
        let handle = reader.segments().next().unwrap().unwrap();
        let mut scan = reader
            .open_segment(&handle, &ColumnProjection::all(), None)
            .unwrap();
        assert_eq!(scan.row_group_count(), 1);
        let zmaps = scan.row_group_zone_maps(0).unwrap();
        assert!(zmaps.contains_key("entity_id"));
        assert!(zmaps.contains_key("ts"));
        // First row-group has data.
        let batch = scan.next_row_group().unwrap().expect("one batch");
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 3);
        // Exhausted.
        assert!(scan.next_row_group().unwrap().is_none());
        // Idempotent after exhaustion.
        assert!(scan.next_row_group().unwrap().is_none());
    }

    #[test]
    fn fake_reader_rejects_unknown_handle() {
        let reader = FakeReader {
            schema: minimal_schema(),
            handles: vec![SegmentHandle {
                segment_id: 1,
                shard_id: 0,
                window_id: 0,
                row_count: 3,
                schema_version: 0,
                seq_id_first: 0,
                batch_id: 0,
            }],
        };
        let stale = SegmentHandle {
            segment_id: 999,
            shard_id: 0,
            window_id: 0,
            row_count: 0,
            schema_version: 0,
            seq_id_first: 0,
            batch_id: 0,
        };
        let err = match reader.open_segment(&stale, &ColumnProjection::all(), None) {
            Err(e) => e,
            Ok(_) => panic!("expected stale handle to be rejected"),
        };
        assert!(matches!(err, BqliteError::Execution(_)), "{err}");
    }

    #[test]
    fn reader_rejects_projection_with_unknown_column() {
        // A reader that validates the projection against its schema
        // and returns `Schema` on mismatch. Exercises the trait
        // contract in §4.3 / §5 of the design doc that real readers
        // return `BqliteError::Schema` when the projection names a
        // column that does not exist.
        struct ValidatingReader {
            schema: TableSchema,
            handles: Vec<SegmentHandle>,
        }
        impl SegmentReader for ValidatingReader {
            fn schema(&self) -> &TableSchema {
                &self.schema
            }
            fn segments(&self) -> Box<dyn Iterator<Item = Result<SegmentHandle>> + Send + '_> {
                Box::new(self.handles.iter().cloned().map(Ok))
            }
            fn open_segment(
                &self,
                handle: &SegmentHandle,
                projection: &ColumnProjection,
                _predicate: Option<Arc<dyn Predicate>>,
            ) -> Result<Box<dyn SegmentScan>> {
                if !self.handles.iter().any(|h| h == handle) {
                    return Err(BqliteError::Execution(format!(
                        "unknown segment handle {handle:?}"
                    )));
                }
                for name in projection.columns() {
                    if self.schema.column(name).is_none() {
                        return Err(BqliteError::Schema(format!(
                            "projection column `{name}` not in table `{}`",
                            self.schema.name()
                        )));
                    }
                }
                // Empty scan — we only need to prove the rejection
                // path fires before any row-group work.
                Ok(Box::new(FakeScan {
                    batches: Vec::new(),
                    zone_maps: Vec::new(),
                    position: 0,
                }))
            }
        }

        let reader = ValidatingReader {
            schema: minimal_schema(),
            handles: vec![SegmentHandle {
                segment_id: 1,
                shard_id: 0,
                window_id: 0,
                row_count: 0,
                schema_version: 0,
                seq_id_first: 0,
                batch_id: 0,
            }],
        };
        let handle = reader.segments().next().unwrap().unwrap();
        // Valid projection passes through.
        reader
            .open_segment(
                &handle,
                &ColumnProjection::with_columns(["entity_id", "ts"]),
                None,
            )
            .expect("valid projection should succeed");
        // Unknown column is rejected with Schema error.
        let bad = ColumnProjection::with_columns(["not_a_column"]);
        let err = match reader.open_segment(&handle, &bad, None) {
            Err(e) => e,
            Ok(_) => panic!("expected projection rejection"),
        };
        let msg = format!("{err}");
        assert!(
            matches!(err, BqliteError::Schema(_)) && msg.contains("not_a_column"),
            "{msg}"
        );
    }

    #[test]
    fn empty_reader_yields_no_segments() {
        // The shape TASK-116's storage stub returns: a reader with
        // no segments. Validates the trait works end-to-end in the
        // empty case that the Wave 1 smoke test relies on.
        struct EmptyReader {
            schema: TableSchema,
        }
        impl SegmentReader for EmptyReader {
            fn schema(&self) -> &TableSchema {
                &self.schema
            }
            fn segments(&self) -> Box<dyn Iterator<Item = Result<SegmentHandle>> + Send + '_> {
                Box::new(std::iter::empty())
            }
            fn open_segment(
                &self,
                _handle: &SegmentHandle,
                _projection: &ColumnProjection,
                _predicate: Option<Arc<dyn Predicate>>,
            ) -> Result<Box<dyn SegmentScan>> {
                Err(BqliteError::Execution(
                    "empty reader has no segments".into(),
                ))
            }
        }
        let reader: Box<dyn SegmentReader> = Box::new(EmptyReader {
            schema: minimal_schema(),
        });
        assert_eq!(reader.schema().name(), "events");
        let segs: Vec<_> = reader.segments().collect();
        assert_eq!(segs.len(), 0);
    }

    // ── Trait object bounds ──────────────────────────────────────────────────

    #[test]
    fn trait_objects_are_send_sync() {
        // These asserts pin the `Send + Sync` bounds at compile time —
        // removing them from the trait definition fails this test.
        fn assert_send_sync<T: Send + Sync + ?Sized>() {}
        assert_send_sync::<dyn SegmentReader>();
        assert_send_sync::<dyn Predicate>();
        // SegmentScan is Send-only (not Sync) — scan state is
        // single-threaded. Pin just Send to catch regressions.
        fn assert_send<T: Send + ?Sized>() {}
        assert_send::<dyn SegmentScan>();
    }

    // ── Interior reference for predicate-shared readers ──────────────────────

    #[test]
    fn predicate_can_be_arced_and_shared() {
        // Validates that a single predicate can be passed to many
        // open_segment calls without cloning the inner value —
        // important for zero-alloc hot paths.
        #[derive(Debug)]
        struct CountingPredicate(AtomicUsize);
        impl Predicate for CountingPredicate {
            fn accepts_zone(&self, _column: &str, _zone: &ZoneMap) -> bool {
                self.0.fetch_add(1, Ordering::Relaxed);
                true
            }
        }
        let p: Arc<dyn Predicate> = Arc::new(CountingPredicate(AtomicUsize::new(0)));
        let z = ZoneMap::default();
        assert!(p.accepts_zone("a", &z));
        assert!(p.accepts_zone("b", &z));
        let other: Arc<dyn Predicate> = Arc::clone(&p);
        assert!(other.accepts_zone("c", &z));
    }

    // ─────────────────────────────────────────────────────────────────────
    // Scan predicate IR — ScanConjunct / ScanPredicate / DictRewrite tests
    //
    // Every row of the acceptance table in
    // `docs/design/storage/predicate-pushdown.md` §6 is covered below:
    // a rejecting case, an accepting case, and the all-null /
    // null-present edges where they matter. The tests double as the
    // executable form of the rule table.
    // ─────────────────────────────────────────────────────────────────────

    /// Build a zone map with integer bounds — the shape used by most
    /// rule-table tests. `nulls = 0` unless a test overrides it.
    fn int_zone(min: i64, max: i64, rows: u64) -> ZoneMap {
        ZoneMap {
            min: Some(PropertyValue::Int(min)),
            max: Some(PropertyValue::Int(max)),
            null_count: 0,
            row_count: rows,
        }
    }

    fn int_zone_with_nulls(min: i64, max: i64, nulls: u64, rows: u64) -> ZoneMap {
        ZoneMap {
            min: Some(PropertyValue::Int(min)),
            max: Some(PropertyValue::Int(max)),
            null_count: nulls,
            row_count: rows,
        }
    }

    // ── Equal ────────────────────────────────────────────────────────

    #[test]
    fn scan_conjunct_equal_accepts_when_value_in_range() {
        let conj = ScanConjunct::Equal {
            column: "amount".into(),
            value: PropertyValue::Int(50),
        };
        assert!(conj.accepts_zone(&int_zone(1, 100, 64)));
        // Boundary values accept on both sides.
        assert!(conj.accepts_zone(&int_zone(50, 50, 64)));
        assert!(conj.accepts_zone(&int_zone(50, 100, 64)));
        assert!(conj.accepts_zone(&int_zone(1, 50, 64)));
    }

    #[test]
    fn scan_conjunct_equal_rejects_when_value_out_of_range() {
        let conj = ScanConjunct::Equal {
            column: "amount".into(),
            value: PropertyValue::Int(200),
        };
        assert!(!conj.accepts_zone(&int_zone(1, 100, 64)));
        let below = ScanConjunct::Equal {
            column: "amount".into(),
            value: PropertyValue::Int(-5),
        };
        assert!(!below.accepts_zone(&int_zone(1, 100, 64)));
    }

    #[test]
    fn scan_conjunct_equal_rejects_all_null_row_group() {
        let conj = ScanConjunct::Equal {
            column: "amount".into(),
            value: PropertyValue::Int(5),
        };
        assert!(!conj.accepts_zone(&ZoneMap::all_null(64)));
    }

    // ── NotEqual ─────────────────────────────────────────────────────

    #[test]
    fn scan_conjunct_not_equal_rejects_only_degenerate_case() {
        let conj = ScanConjunct::NotEqual {
            column: "event".into(),
            value: PropertyValue::Int(5),
        };
        // Every non-null row equals 5, no nulls — definitely empty.
        assert!(!conj.accepts_zone(&int_zone(5, 5, 64)));
        // Mixed range — some rows may not equal 5.
        assert!(conj.accepts_zone(&int_zone(1, 10, 64)));
        // Single-value range with nulls — writer's "exact" info is
        // the (min, max) pair, which is still == value; the rule's
        // `nulls == 0` guard keeps us conservative when nulls exist.
        assert!(conj.accepts_zone(&int_zone_with_nulls(5, 5, 10, 64)));
        // All-null row-group: min == max == None, nulls == rows > 0.
        // Conservatively accept — every row is NULL, every row
        // produces UNKNOWN under `!=`, and the filter operator above
        // the scan drops them. Accept keeps the rule well-typed
        // against `Option<PropertyValue>` bounds.
        assert!(conj.accepts_zone(&ZoneMap::all_null(64)));
    }

    // ── Range ────────────────────────────────────────────────────────

    #[test]
    fn scan_conjunct_range_lt_accepts_when_min_below_value() {
        let conj = ScanConjunct::Range {
            column: "amount".into(),
            op: RangeOp::Lt,
            value: PropertyValue::Int(100),
        };
        assert!(conj.accepts_zone(&int_zone(1, 99, 64)));
        assert!(conj.accepts_zone(&int_zone(50, 150, 64))); // some rows < 100
                                                            // min == value fails Lt (needs strict <)
        assert!(!conj.accepts_zone(&int_zone(100, 200, 64)));
        assert!(!conj.accepts_zone(&int_zone(101, 200, 64)));
    }

    #[test]
    fn scan_conjunct_range_le_accepts_on_equality() {
        let conj = ScanConjunct::Range {
            column: "amount".into(),
            op: RangeOp::Le,
            value: PropertyValue::Int(100),
        };
        assert!(conj.accepts_zone(&int_zone(100, 200, 64))); // min == value
        assert!(!conj.accepts_zone(&int_zone(101, 200, 64)));
    }

    #[test]
    fn scan_conjunct_range_gt_accepts_when_max_above_value() {
        let conj = ScanConjunct::Range {
            column: "amount".into(),
            op: RangeOp::Gt,
            value: PropertyValue::Int(100),
        };
        assert!(conj.accepts_zone(&int_zone(1, 101, 64)));
        assert!(conj.accepts_zone(&int_zone(50, 200, 64)));
        // max == value fails Gt (needs strict >)
        assert!(!conj.accepts_zone(&int_zone(1, 100, 64)));
    }

    #[test]
    fn scan_conjunct_range_ge_accepts_on_equality() {
        let conj = ScanConjunct::Range {
            column: "amount".into(),
            op: RangeOp::Ge,
            value: PropertyValue::Int(100),
        };
        assert!(conj.accepts_zone(&int_zone(1, 100, 64))); // max == value
        assert!(!conj.accepts_zone(&int_zone(1, 99, 64)));
    }

    #[test]
    fn scan_conjunct_range_rejects_all_null_row_group() {
        let conj = ScanConjunct::Range {
            column: "amount".into(),
            op: RangeOp::Gt,
            value: PropertyValue::Int(0),
        };
        assert!(!conj.accepts_zone(&ZoneMap::all_null(64)));
    }

    // ── InSet ────────────────────────────────────────────────────────

    #[test]
    fn scan_conjunct_in_set_accepts_when_any_value_in_range() {
        let conj = ScanConjunct::InSet {
            column: "status".into(),
            values: vec![
                PropertyValue::Int(1),
                PropertyValue::Int(50),
                PropertyValue::Int(9999),
            ],
        };
        // 50 falls in [1, 100].
        assert!(conj.accepts_zone(&int_zone(1, 100, 64)));
    }

    #[test]
    fn scan_conjunct_in_set_rejects_when_all_values_out_of_range() {
        let conj = ScanConjunct::InSet {
            column: "status".into(),
            values: vec![PropertyValue::Int(200), PropertyValue::Int(300)],
        };
        assert!(!conj.accepts_zone(&int_zone(1, 100, 64)));
    }

    #[test]
    fn scan_conjunct_in_set_rejects_all_null_row_group() {
        let conj = ScanConjunct::InSet {
            column: "status".into(),
            values: vec![PropertyValue::Int(50)],
        };
        assert!(!conj.accepts_zone(&ZoneMap::all_null(64)));
    }

    // ── IsNull / IsNotNull ───────────────────────────────────────────

    #[test]
    fn scan_conjunct_is_null_requires_any_null() {
        let conj = ScanConjunct::IsNull {
            column: "amount".into(),
        };
        assert!(!conj.accepts_zone(&int_zone(1, 100, 64))); // nulls == 0
        assert!(conj.accepts_zone(&int_zone_with_nulls(1, 100, 5, 64)));
        assert!(conj.accepts_zone(&ZoneMap::all_null(64)));
    }

    #[test]
    fn scan_conjunct_is_not_null_requires_any_non_null() {
        let conj = ScanConjunct::IsNotNull {
            column: "amount".into(),
        };
        assert!(conj.accepts_zone(&int_zone(1, 100, 64)));
        assert!(conj.accepts_zone(&int_zone_with_nulls(1, 100, 5, 64)));
        assert!(!conj.accepts_zone(&ZoneMap::all_null(64)));
    }

    // ── ScanConjunct::EntityIn (TASK-522) ────────────────────────────

    #[test]
    fn entity_in_accepts_overlapping_zone() {
        let conj = ScanConjunct::entity_in(
            "entity_id".into(),
            vec![
                PropertyValue::Int(10),
                PropertyValue::Int(20),
                PropertyValue::Int(30),
            ],
        )
        .expect("non-empty");
        assert!(conj.accepts_zone(&int_zone(15, 25, 100)));
    }

    #[test]
    fn entity_in_rejects_disjoint_zone_above() {
        let conj = ScanConjunct::entity_in(
            "entity_id".into(),
            vec![PropertyValue::Int(10), PropertyValue::Int(20)],
        )
        .expect("non-empty");
        assert!(!conj.accepts_zone(&int_zone(50, 60, 100)));
    }

    #[test]
    fn entity_in_rejects_disjoint_zone_below() {
        let conj = ScanConjunct::entity_in(
            "entity_id".into(),
            vec![PropertyValue::Int(50), PropertyValue::Int(60)],
        )
        .expect("non-empty");
        assert!(!conj.accepts_zone(&int_zone(10, 20, 100)));
    }

    #[test]
    fn entity_in_rejects_all_null_zone() {
        let conj = ScanConjunct::entity_in("entity_id".into(), vec![PropertyValue::Int(10)])
            .expect("non-empty");
        assert!(!conj.accepts_zone(&ZoneMap::all_null(100)));
    }

    #[test]
    fn entity_in_accepts_when_zone_bounds_missing_with_some_nonnulls() {
        // Conservative-accept: writers always populate bounds when
        // nulls < rows in v1, but a hypothetical partial-bound writer
        // must not be pruned away.
        let conj = ScanConjunct::entity_in("entity_id".into(), vec![PropertyValue::Int(10)])
            .expect("non-empty");
        let zone = ZoneMap {
            min: None,
            max: None,
            null_count: 5,
            row_count: 100,
        };
        assert!(conj.accepts_zone(&zone));
    }

    #[test]
    fn entity_in_string_values_accept_string_zone() {
        let conj = ScanConjunct::entity_in(
            "entity_id".into(),
            vec![
                PropertyValue::String("u3".into()),
                PropertyValue::String("u7".into()),
            ],
        )
        .expect("non-empty");
        let zone = ZoneMap {
            min: Some(PropertyValue::String("u1".into())),
            max: Some(PropertyValue::String("u9".into())),
            null_count: 0,
            row_count: 100,
        };
        assert!(conj.accepts_zone(&zone));
    }

    #[test]
    fn entity_in_constructor_rejects_empty_vec() {
        assert!(ScanConjunct::entity_in("entity_id".into(), Vec::new()).is_none());
    }

    #[test]
    fn entity_in_constructor_sorts_and_dedups() {
        let conj = ScanConjunct::entity_in(
            "entity_id".into(),
            vec![
                PropertyValue::Int(30),
                PropertyValue::Int(10),
                PropertyValue::Int(20),
                PropertyValue::Int(10), // duplicate
            ],
        )
        .expect("non-empty");
        match conj {
            ScanConjunct::EntityIn {
                ref values,
                ref set_min,
                ref set_max,
                ..
            } => {
                assert_eq!(set_min, &PropertyValue::Int(10));
                assert_eq!(set_max, &PropertyValue::Int(30));
                assert_eq!(
                    values.as_slice(),
                    &[
                        PropertyValue::Int(10),
                        PropertyValue::Int(20),
                        PropertyValue::Int(30),
                    ]
                );
            }
            _ => panic!("expected EntityIn"),
        }
    }

    #[test]
    fn entity_in_referenced_column_propagates_into_predicate() {
        let conj = ScanConjunct::entity_in("entity_id".into(), vec![PropertyValue::Int(1)])
            .expect("non-empty");
        let predicate = ScanPredicate::new(vec![conj]);
        assert_eq!(predicate.referenced_columns(), &["entity_id"]);
    }

    // ── ScanPredicate ────────────────────────────────────────────────

    #[test]
    fn scan_predicate_new_populates_referenced_dedup_ordered() {
        let pred = ScanPredicate::new(vec![
            ScanConjunct::Range {
                column: "amount".into(),
                op: RangeOp::Gt,
                value: PropertyValue::Int(100),
            },
            ScanConjunct::Equal {
                column: "event".into(),
                value: PropertyValue::String("checkout".into()),
            },
            // Second reference to "amount" — must not duplicate.
            ScanConjunct::Range {
                column: "amount".into(),
                op: RangeOp::Le,
                value: PropertyValue::Int(1000),
            },
        ]);
        assert_eq!(pred.referenced_columns(), &["amount", "event"]);
    }

    #[test]
    fn scan_predicate_is_empty_accepts_everything() {
        let pred = ScanPredicate::default();
        assert!(pred.is_empty());
        assert_eq!(pred.len(), 0);
        let zones: HashMap<String, ZoneMap> = HashMap::new();
        assert!(pred.accepts_zone_group(&zones));
    }

    #[test]
    fn scan_predicate_accepts_zone_group_rejects_when_any_conjunct_rejects() {
        let pred = ScanPredicate::new(vec![
            ScanConjunct::Range {
                column: "amount".into(),
                op: RangeOp::Gt,
                value: PropertyValue::Int(100),
            },
            ScanConjunct::Equal {
                column: "event".into(),
                value: PropertyValue::Int(5),
            },
        ]);
        let mut zones: HashMap<String, ZoneMap> = HashMap::new();
        zones.insert("amount".into(), int_zone(1, 50, 64)); // rejects Gt 100
        zones.insert("event".into(), int_zone(1, 10, 64)); // would accept Equal 5
        assert!(!pred.accepts_zone_group(&zones));
    }

    #[test]
    fn scan_predicate_accepts_zone_group_missing_column_is_conservative() {
        // A conjunct on a column absent from `zones` must NOT cause
        // the row-group to be pruned. This is the core invariant
        // from predicate-pushdown.md §6.
        let pred = ScanPredicate::new(vec![ScanConjunct::Range {
            column: "amount".into(),
            op: RangeOp::Gt,
            value: PropertyValue::Int(100),
        }]);
        let zones: HashMap<String, ZoneMap> = HashMap::new(); // empty
        assert!(pred.accepts_zone_group(&zones));
    }

    #[test]
    fn scan_predicate_single_column_accepts_zone_only_checks_that_column() {
        // The single-column form answers in isolation — a conjunct on
        // a different column must not affect the answer.
        let pred = ScanPredicate::new(vec![
            ScanConjunct::Equal {
                column: "event".into(),
                value: PropertyValue::Int(5),
            },
            // `amount` conjunct that would reject its zone, but we're
            // asking about `event`.
            ScanConjunct::Range {
                column: "amount".into(),
                op: RangeOp::Gt,
                value: PropertyValue::Int(1000),
            },
        ]);
        assert!(pred.accepts_zone("event", &int_zone(1, 10, 64)));
    }

    #[test]
    fn scan_predicate_accepts_zone_group_all_accept() {
        let pred = ScanPredicate::new(vec![ScanConjunct::Range {
            column: "amount".into(),
            op: RangeOp::Gt,
            value: PropertyValue::Int(50),
        }]);
        let mut zones: HashMap<String, ZoneMap> = HashMap::new();
        zones.insert("amount".into(), int_zone(1, 100, 64));
        assert!(pred.accepts_zone_group(&zones));
    }

    // ── DictionaryIndex / resolve_dictionary_codes ───────────────────

    #[test]
    fn dictionary_index_code_of_binary_searches_sorted_values() {
        let values = vec![
            PropertyValue::String("apple".into()),
            PropertyValue::String("banana".into()),
            PropertyValue::String("cherry".into()),
        ];
        let dict = DictionaryIndex { values: &values };
        assert_eq!(
            dict.code_of(&PropertyValue::String("apple".into())),
            Some(0)
        );
        assert_eq!(
            dict.code_of(&PropertyValue::String("banana".into())),
            Some(1)
        );
        assert_eq!(
            dict.code_of(&PropertyValue::String("cherry".into())),
            Some(2)
        );
        assert_eq!(dict.code_of(&PropertyValue::String("durian".into())), None);
    }

    #[test]
    fn scan_predicate_resolve_dictionary_codes_no_rewrite_when_no_equal_in_set() {
        let values = vec![PropertyValue::String("apple".into())];
        let dict = DictionaryIndex { values: &values };
        let pred = ScanPredicate::new(vec![ScanConjunct::Range {
            column: "fruit".into(),
            op: RangeOp::Gt,
            value: PropertyValue::String("apple".into()),
        }]);
        assert_eq!(
            pred.resolve_dictionary_codes("fruit", &dict),
            DictRewrite::NoRewrite
        );
    }

    #[test]
    fn scan_predicate_resolve_dictionary_codes_empty_set_when_nothing_resolves() {
        let values = vec![
            PropertyValue::String("apple".into()),
            PropertyValue::String("banana".into()),
        ];
        let dict = DictionaryIndex { values: &values };
        let pred = ScanPredicate::new(vec![ScanConjunct::Equal {
            column: "fruit".into(),
            value: PropertyValue::String("durian".into()), // not in dict
        }]);
        assert_eq!(
            pred.resolve_dictionary_codes("fruit", &dict),
            DictRewrite::EmptySet
        );
    }

    #[test]
    fn scan_predicate_resolve_dictionary_codes_returns_codes_from_equal() {
        let values = vec![
            PropertyValue::String("apple".into()),
            PropertyValue::String("banana".into()),
        ];
        let dict = DictionaryIndex { values: &values };
        let pred = ScanPredicate::new(vec![ScanConjunct::Equal {
            column: "fruit".into(),
            value: PropertyValue::String("banana".into()),
        }]);
        assert_eq!(
            pred.resolve_dictionary_codes("fruit", &dict),
            DictRewrite::Codes(vec![1])
        );
    }

    #[test]
    fn scan_predicate_resolve_dictionary_codes_keeps_partial_resolutions_in_set() {
        // `InSet` with some literals missing from the dict: the missing
        // literals can't match anyway, so dropping them preserves the
        // no-false-negatives invariant.
        let values = vec![
            PropertyValue::String("apple".into()),
            PropertyValue::String("banana".into()),
            PropertyValue::String("cherry".into()),
        ];
        let dict = DictionaryIndex { values: &values };
        let pred = ScanPredicate::new(vec![ScanConjunct::InSet {
            column: "fruit".into(),
            values: vec![
                PropertyValue::String("banana".into()),
                PropertyValue::String("durian".into()), // missing
                PropertyValue::String("cherry".into()),
            ],
        }]);
        let rewrite = pred.resolve_dictionary_codes("fruit", &dict);
        match rewrite {
            DictRewrite::Codes(mut codes) => {
                codes.sort_unstable();
                assert_eq!(codes, vec![1, 2]);
            }
            other => panic!("expected Codes, got {other:?}"),
        }
    }

    #[test]
    fn scan_predicate_resolve_dictionary_codes_wrong_column_is_no_rewrite() {
        let values = vec![PropertyValue::String("apple".into())];
        let dict = DictionaryIndex { values: &values };
        let pred = ScanPredicate::new(vec![ScanConjunct::Equal {
            column: "other".into(),
            value: PropertyValue::String("apple".into()),
        }]);
        // Asking about "fruit" — the conjunct is on "other", so no
        // rewrite applies to "fruit".
        assert_eq!(
            pred.resolve_dictionary_codes("fruit", &dict),
            DictRewrite::NoRewrite
        );
    }

    // ── Trait-level back-compat: Wave 1 impls keep working ──────────

    // ── Encoded-path default fallback ───────────────────────────────

    #[test]
    fn next_encoded_row_group_default_wraps_record_batch_as_materialized() {
        // A scan that only implements `next_row_group` must still
        // produce an `EncodedBatch` via the default body — every
        // column becomes an `EncodedColumn::Materialized` fallback.
        let mut scan = FakeScan {
            batches: vec![ArrowRecordBatch::try_new(
                Arc::new(ArrowSchema::new(vec![
                    Field::new("a", DataType::Int64, false),
                    Field::new("b", DataType::Utf8, false),
                ])),
                vec![
                    Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4])),
                    Arc::new(StringArray::from(vec!["x", "y", "z", "w"])),
                ],
            )
            .unwrap()],
            zone_maps: vec![HashMap::new()],
            position: 0,
        };

        let encoded = scan.next_encoded_row_group().unwrap().unwrap();
        assert_eq!(encoded.row_count, 4);
        assert_eq!(encoded.columns.len(), 2);
        for col in &encoded.columns {
            assert_eq!(col.rows(), 4);
            assert!(matches!(
                col,
                crate::encoded::EncodedColumn::Materialized { .. }
            ));
        }

        // Second call exhausts; third must stay None.
        assert!(scan.next_encoded_row_group().unwrap().is_none());
        assert!(scan.next_encoded_row_group().unwrap().is_none());
    }

    #[test]
    fn wave1_predicate_impls_keep_compiling_via_default_bodies() {
        // A Wave 1 `Predicate` impl that only implements `accepts_zone`
        // (no `referenced_columns`, no `accepts_zone_group`, no
        // `resolve_dictionary_codes`) must still compile and be
        // usable. This test is the executable proof of the "additive
        // extension" promise in the trait docs.
        #[derive(Debug)]
        struct Wave1Only;
        impl Predicate for Wave1Only {
            fn accepts_zone(&self, _column: &str, _zone: &ZoneMap) -> bool {
                true
            }
        }
        let p: Arc<dyn Predicate> = Arc::new(Wave1Only);
        let zones: HashMap<String, ZoneMap> = HashMap::new();
        assert!(p.accepts_zone_group(&zones));
        assert!(p.referenced_columns().is_empty());
        let values: Vec<PropertyValue> = Vec::new();
        let dict = DictionaryIndex { values: &values };
        assert_eq!(
            p.resolve_dictionary_codes("any", &dict),
            DictRewrite::NoRewrite
        );
    }
}
