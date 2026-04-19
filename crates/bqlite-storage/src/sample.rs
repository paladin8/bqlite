//! Entity-level deterministic sampling filter (TASK-430).
//!
//! Implements the scan-layer side of the SAMPLE operator per
//! `docs/design/operators/event-select-sample.md` Block B, §16–§18:
//!
//! - xxHash64 over the canonical entity-id byte serialization
//!   (UTF-8 for String entity keys, little-endian 8 bytes for Int).
//! - Threshold test: `hash(id, seed) < fraction * u64::MAX`.
//! - `fraction == 0.0` — empty set (no entity passes).
//! - `fraction == 1.0` — pass-through (short-circuit, no hashing).
//! - Row-group zone-map pruning is opportunistic: a row group is
//!   rejected only when its entity-id zone has `min == max` *and* the
//!   threshold test rejects that single entity. This is the only
//!   shape that lets us decide the whole group from its zone; any
//!   wider min/max must conservatively accept and fall back to
//!   row-level filtering at the scan operator.
//!
//! The filter is constructed once per query and shared by `Arc` across
//! every segment scan in that query.

use std::sync::Arc;

use arrow::array::{Array, BooleanArray, Int64Array, StringViewArray};
use arrow::datatypes::DataType;
use bqlite_core::{BqlType, BqliteError, PropertyValue, Result, ZoneMap};
use twox_hash::XxHash64;

/// Entity-ID hash filter for the SAMPLE operator.
///
/// Holds a precomputed `u64` threshold, a query seed, and the entity
/// column's type. The `entity_type` is carried so callers can dispatch
/// byte-serialization (UTF-8 vs little-endian 8 bytes) without
/// re-deriving it from the table schema.
///
/// `SampleFilter` is deliberately cheap to clone — every field is
/// either a scalar or a short `String`, so wrapping in `Arc` is
/// optional. Callers that want zero extra work across fan-out still
/// get it by sharing via `Arc<SampleFilter>`.
#[derive(Debug, Clone, PartialEq)]
pub struct SampleFilter {
    /// `fraction * u64::MAX`, saturated to `u64::MAX` for `fraction ==
    /// 1.0`. An entity is accepted iff its hash is strictly less than
    /// this value. `0` means "no entity passes" (empty set).
    threshold: u64,
    /// Original fraction, retained for debug dumps and for the
    /// pass-through short-circuit in [`SampleFilter::is_pass_through`].
    /// Validated to be in `[0.0, 1.0]` at construction.
    fraction: f64,
    /// Per-query seed for xxHash64. The `None` case from the logical
    /// plan is resolved to the database-UUID-derived default by the
    /// planner bind step before it reaches this constructor.
    seed: i64,
    /// Name of the entity-id column this filter targets. Used by the
    /// scan/reader code to look up the right Arrow array or zone map.
    entity_col: String,
    /// BQL type of the entity-id column. Used to dispatch between the
    /// string and integer byte-serialization paths (design §16.2).
    entity_type: BqlType,
}

impl SampleFilter {
    /// Construct a filter from a validated fraction, resolved seed,
    /// entity-id column name, and entity-id type.
    ///
    /// # Errors
    ///
    /// - [`BqliteError::Plan`] if `fraction` is outside `[0.0, 1.0]`
    ///   or not finite. The planner re-checks this so the scan
    ///   operator never has to; the defensive re-check here keeps
    ///   the storage-layer primitive independently testable.
    /// - [`BqliteError::Plan`] if `entity_type` is neither
    ///   [`BqlType::String`] nor [`BqlType::Int`]. The table schema
    ///   constructor already rejects other key types
    ///   (see `TableSchema::new`), so this is unreachable in the
    ///   normal pipeline and surfaces only if a caller wires the
    ///   filter against a synthetic schema.
    pub fn new(
        fraction: f64,
        seed: i64,
        entity_col: impl Into<String>,
        entity_type: BqlType,
    ) -> Result<Self> {
        if !fraction.is_finite() || !(0.0..=1.0).contains(&fraction) {
            return Err(BqliteError::Plan(format!(
                "SAMPLE: fraction must be in [0.0, 1.0] — got {fraction}"
            )));
        }
        if !matches!(entity_type, BqlType::String | BqlType::Int) {
            return Err(BqliteError::Plan(format!(
                "SAMPLE: entity-id column must be String or Int — got {entity_type:?}"
            )));
        }
        // Compute the threshold. `fraction == 1.0` saturates to
        // `u64::MAX`; any other value converts without surprise
        // because `u64::MAX as f64 * fraction` fits inside `u64`.
        let threshold = if fraction >= 1.0 {
            u64::MAX
        } else if fraction <= 0.0 {
            0
        } else {
            (fraction * u64::MAX as f64) as u64
        };
        Ok(Self {
            threshold,
            fraction,
            seed,
            entity_col: entity_col.into(),
            entity_type,
        })
    }

    /// True when the filter rejects every entity. Equivalent to
    /// `fraction == 0.0`. Callers may short-circuit a scan to an
    /// empty stream in this case.
    pub fn is_empty_set(&self) -> bool {
        self.threshold == 0 && self.fraction == 0.0
    }

    /// True when the filter accepts every entity. Equivalent to
    /// `fraction == 1.0`. Callers may skip hashing entirely.
    pub fn is_pass_through(&self) -> bool {
        self.fraction >= 1.0
    }

    /// Name of the entity-id column this filter targets.
    pub fn entity_column(&self) -> &str {
        &self.entity_col
    }

    /// BQL type of the entity-id column.
    pub fn entity_type(&self) -> &BqlType {
        &self.entity_type
    }

    /// Original fraction as supplied to [`SampleFilter::new`]. Exposed
    /// for EXPLAIN / test assertions; the hot filter path uses the
    /// precomputed `threshold`.
    pub fn fraction(&self) -> f64 {
        self.fraction
    }

    /// Resolved query seed used for xxHash64.
    pub fn seed(&self) -> i64 {
        self.seed
    }

    /// Test a string entity-id against the threshold.
    ///
    /// `fraction == 1.0` short-circuits to `true` without hashing —
    /// the design doc §16.3 pins this as a guarantee so `1.0` is a
    /// true pass-through (no `1/2^64` excluded entity).
    #[inline]
    pub fn accepts_str(&self, bytes: &[u8]) -> bool {
        if self.is_pass_through() {
            return true;
        }
        if self.is_empty_set() {
            return false;
        }
        let h = XxHash64::oneshot(self.seed as u64, bytes);
        h < self.threshold
    }

    /// Test an integer entity-id against the threshold. The integer
    /// is serialized to 8 little-endian bytes per design §16.2.
    #[inline]
    pub fn accepts_int(&self, v: i64) -> bool {
        if self.is_pass_through() {
            return true;
        }
        if self.is_empty_set() {
            return false;
        }
        let h = XxHash64::oneshot(self.seed as u64, &v.to_le_bytes());
        h < self.threshold
    }

    /// Opportunistic row-group pruning from the entity-id column's
    /// zone map. Returns `false` only when the zone describes a
    /// single distinct entity that the filter rejects.
    ///
    /// A zone with `min != max`, null-only, or with the filter in
    /// pass-through / empty-set mode delegates the decision to per-
    /// row filtering at the scan operator. The rationale is coverage:
    /// the common shape is a row group with many entities, where
    /// zone-map pruning cannot speak to the threshold test; the
    /// min==max case falls out of entity-major writer layout when a
    /// large entity straddles multiple row groups, which is exactly
    /// where the I/O savings are worth the hash evaluation.
    pub fn accepts_zone(&self, zone: &ZoneMap) -> bool {
        if self.is_pass_through() {
            return true;
        }
        if self.is_empty_set() {
            return false;
        }
        // All-null zones cannot be pruned by the entity-id filter
        // alone — the rows are still candidates for other conjuncts.
        let (Some(min), Some(max)) = (zone.min.as_ref(), zone.max.as_ref()) else {
            return true;
        };
        if min != max {
            return true;
        }
        match (&self.entity_type, min) {
            (BqlType::String, PropertyValue::String(s)) => self.accepts_str(s.as_bytes()),
            (BqlType::Int, PropertyValue::Int(v)) => self.accepts_int(*v),
            // Type mismatch or unsupported shape — be conservative
            // and accept. Schema validation has already caught wrong
            // column types; this only fires on a corrupted zone map.
            _ => true,
        }
    }

    /// Build a boolean mask selecting rows whose entity-id passes the
    /// threshold test. One bit per row of `entity_col_array`, `true`
    /// for accept.
    ///
    /// Null entity-id rows are rejected (`false`). The table schema
    /// guarantees the entity-id column is non-null on ingested rows,
    /// so nulls arrive only through synthetic fixtures. Rejection
    /// keeps the invariant that a sampled entity has a stable hash —
    /// `null.hash` would be unstable under any definition we pick.
    pub fn apply_to_array(&self, entity_col_array: &dyn Array) -> Result<BooleanArray> {
        let n = entity_col_array.len();
        // Dispatch on the filter's declared entity type, not the
        // array's Arrow type. This makes a type mismatch an explicit
        // `BqliteError` rather than silently hashing the wrong bytes.
        match (&self.entity_type, entity_col_array.data_type()) {
            (BqlType::String, DataType::Utf8View) => {
                let arr = entity_col_array
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .ok_or_else(|| {
                        BqliteError::Execution(
                            "SAMPLE: expected StringViewArray for Utf8View column".to_string(),
                        )
                    })?;
                let mut mask = Vec::with_capacity(n);
                for i in 0..n {
                    if arr.is_null(i) {
                        // Null entity-id rows reject unconditionally.
                        // Table schema forbids null entity keys, so
                        // this only fires on synthetic fixtures, but
                        // the invariant "sampled entities have stable
                        // hashes" forces a deterministic answer here.
                        mask.push(false);
                    } else if self.is_pass_through() {
                        mask.push(true);
                    } else if self.is_empty_set() {
                        mask.push(false);
                    } else {
                        mask.push(self.accepts_str(arr.value(i).as_bytes()));
                    }
                }
                Ok(BooleanArray::from(mask))
            }
            (BqlType::Int, DataType::Int64) => {
                let arr = entity_col_array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        BqliteError::Execution(
                            "SAMPLE: expected Int64Array for Int column".to_string(),
                        )
                    })?;
                let mut mask = Vec::with_capacity(n);
                for i in 0..n {
                    if arr.is_null(i) {
                        mask.push(false);
                    } else if self.is_pass_through() {
                        mask.push(true);
                    } else if self.is_empty_set() {
                        mask.push(false);
                    } else {
                        mask.push(self.accepts_int(arr.value(i)));
                    }
                }
                Ok(BooleanArray::from(mask))
            }
            (expected, actual) => Err(BqliteError::Execution(format!(
                "SAMPLE: entity column type mismatch — filter expects {expected:?} but array is {actual:?}"
            ))),
        }
    }
}

/// Shared reference type that the scan operator, reader, and engine
/// bind step can all hand around without cloning the filter body.
pub type SharedSampleFilter = Arc<SampleFilter>;

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringViewArray};
    use bqlite_core::{BqlType, PropertyValue, ZoneMap};

    fn some_string(s: &str) -> PropertyValue {
        PropertyValue::String(s.into())
    }

    fn some_int(v: i64) -> PropertyValue {
        PropertyValue::Int(v)
    }

    // ── construction validation ──────────────────────────────────────────────

    #[test]
    fn rejects_out_of_range_fraction() {
        assert!(SampleFilter::new(-0.1, 0, "entity_id", BqlType::String).is_err());
        assert!(SampleFilter::new(1.1, 0, "entity_id", BqlType::String).is_err());
        assert!(SampleFilter::new(f64::NAN, 0, "entity_id", BqlType::String).is_err());
        assert!(SampleFilter::new(f64::INFINITY, 0, "entity_id", BqlType::String).is_err());
    }

    #[test]
    fn rejects_unsupported_entity_type() {
        assert!(SampleFilter::new(0.5, 0, "ts", BqlType::Timestamp).is_err());
        assert!(SampleFilter::new(0.5, 0, "flag", BqlType::Bool).is_err());
    }

    #[test]
    fn boundary_zero_is_empty_set() {
        let f = SampleFilter::new(0.0, 42, "entity_id", BqlType::String).unwrap();
        assert!(f.is_empty_set());
        assert!(!f.is_pass_through());
        assert!(!f.accepts_str(b"anything"));
        assert!(!f.accepts_int(123));
    }

    #[test]
    fn boundary_one_is_pass_through() {
        let f = SampleFilter::new(1.0, 42, "entity_id", BqlType::String).unwrap();
        assert!(f.is_pass_through());
        assert!(!f.is_empty_set());
        assert!(f.accepts_str(b"anything"));
        assert!(f.accepts_int(123));
    }

    // ── determinism + seed behavior ──────────────────────────────────────────

    #[test]
    fn same_seed_same_entity_is_deterministic() {
        let f1 = SampleFilter::new(0.5, 7, "entity_id", BqlType::String).unwrap();
        let f2 = SampleFilter::new(0.5, 7, "entity_id", BqlType::String).unwrap();
        for name in ["alice", "bob", "charlie", "dave", "erin"] {
            assert_eq!(
                f1.accepts_str(name.as_bytes()),
                f2.accepts_str(name.as_bytes())
            );
        }
    }

    #[test]
    fn different_seeds_yield_different_sets() {
        // With 1M entities and fraction 0.5 the probability of exactly
        // the same set across two seeds is astronomically small.
        let f1 = SampleFilter::new(0.5, 1, "entity_id", BqlType::String).unwrap();
        let f2 = SampleFilter::new(0.5, 2, "entity_id", BqlType::String).unwrap();
        let mut diffs = 0;
        for i in 0..1_000 {
            let key = format!("entity_{i}");
            if f1.accepts_str(key.as_bytes()) != f2.accepts_str(key.as_bytes()) {
                diffs += 1;
            }
        }
        assert!(diffs > 100, "two seeds should disagree on many entities");
    }

    #[test]
    fn fraction_half_approximates_half_the_entities() {
        // With 10k entities, fraction 0.5, binomial standard deviation
        // is sqrt(10_000 * 0.25) = 50, so |accepted - 5_000| > 500 is
        // a 10-sigma event.
        let f = SampleFilter::new(0.5, 42, "entity_id", BqlType::String).unwrap();
        let mut accepted = 0;
        for i in 0..10_000 {
            let key = format!("u-{i}");
            if f.accepts_str(key.as_bytes()) {
                accepted += 1;
            }
        }
        assert!(
            (4_500..=5_500).contains(&accepted),
            "fraction 0.5 should accept ~5000 of 10000 entities, got {accepted}"
        );
    }

    #[test]
    fn monotonic_in_fraction() {
        // Any entity accepted at f=0.25 must also be accepted at
        // f=0.5 and f=0.75 with the same seed.
        let fa = SampleFilter::new(0.25, 99, "entity_id", BqlType::String).unwrap();
        let fb = SampleFilter::new(0.5, 99, "entity_id", BqlType::String).unwrap();
        let fc = SampleFilter::new(0.75, 99, "entity_id", BqlType::String).unwrap();
        for i in 0..1_000 {
            let key = format!("monotonic-{i}");
            let b = key.as_bytes();
            if fa.accepts_str(b) {
                assert!(fb.accepts_str(b));
                assert!(fc.accepts_str(b));
            }
            if fb.accepts_str(b) {
                assert!(fc.accepts_str(b));
            }
        }
    }

    // ── int entity IDs ───────────────────────────────────────────────────────

    #[test]
    fn int_entity_hash_is_little_endian_stable() {
        // Pin the exact xxhash64 output for a known `(seed, key)` pair.
        // §16.2 of the design doc makes this contractual: the hash must
        // consume the entity-id as little-endian 8 bytes and combine
        // with the seed exactly this way, forever. If `twox_hash` bumps
        // a major version or reinterprets seed bytes, this regression
        // catches the break before a database-hash-seed change surprises
        // users with a different sampled set.
        let h_le = XxHash64::oneshot(0, &42_i64.to_le_bytes());
        assert_eq!(h_le, 13_066_772_586_158_965_587_u64);

        // A non-trivial fraction: every entity whose hash is below
        // u64::MAX/2 is accepted at fraction 0.5 exactly when
        // h < u64::MAX/2. This ties the `accepts_int` path to the raw
        // hash above.
        let f_half = SampleFilter::new(0.5, 0, "entity_id", BqlType::Int).unwrap();
        let expected = h_le < (u64::MAX as f64 * 0.5) as u64;
        assert_eq!(f_half.accepts_int(42), expected);
    }

    // ── zone-map pruning ─────────────────────────────────────────────────────

    #[test]
    fn zone_with_min_equals_max_prunes_rejected_entity() {
        // Find an entity the filter rejects at fraction 0.01 and verify
        // a zone with that entity as min == max is pruned.
        let f = SampleFilter::new(0.01, 17, "entity_id", BqlType::String).unwrap();
        let rejected = (0..10_000)
            .map(|i| format!("z-{i}"))
            .find(|k| !f.accepts_str(k.as_bytes()))
            .expect("fraction 0.01 rejects most entities");
        let zone = ZoneMap {
            min: Some(some_string(&rejected)),
            max: Some(some_string(&rejected)),
            null_count: 0,
            row_count: 10,
        };
        assert!(!f.accepts_zone(&zone));
    }

    #[test]
    fn zone_with_different_min_max_is_conservative_accept() {
        let f = SampleFilter::new(0.01, 17, "entity_id", BqlType::String).unwrap();
        let zone = ZoneMap {
            min: Some(some_string("a")),
            max: Some(some_string("z")),
            null_count: 0,
            row_count: 100,
        };
        // Even though most single entities reject at 0.01, a wide
        // zone must accept because we can't decide from the bounds.
        assert!(f.accepts_zone(&zone));
    }

    #[test]
    fn zone_all_null_is_conservative_accept() {
        let f = SampleFilter::new(0.01, 17, "entity_id", BqlType::String).unwrap();
        let zone = ZoneMap::all_null(10);
        assert!(f.accepts_zone(&zone));
    }

    #[test]
    fn zone_prune_respects_pass_through_and_empty_set() {
        let pass = SampleFilter::new(1.0, 0, "entity_id", BqlType::String).unwrap();
        let empty = SampleFilter::new(0.0, 0, "entity_id", BqlType::String).unwrap();
        let any_zone = ZoneMap {
            min: Some(some_string("alice")),
            max: Some(some_string("alice")),
            null_count: 0,
            row_count: 10,
        };
        assert!(pass.accepts_zone(&any_zone));
        assert!(!empty.accepts_zone(&any_zone));
    }

    #[test]
    fn zone_with_int_entity_prunes() {
        let f = SampleFilter::new(0.01, 3, "entity_id", BqlType::Int).unwrap();
        let rejected = (0..10_000_i64)
            .find(|&v| !f.accepts_int(v))
            .expect("some int must reject at fraction 0.01");
        let zone = ZoneMap {
            min: Some(some_int(rejected)),
            max: Some(some_int(rejected)),
            null_count: 0,
            row_count: 42,
        };
        assert!(!f.accepts_zone(&zone));
    }

    // ── apply_to_array ───────────────────────────────────────────────────────

    #[test]
    fn apply_to_array_filters_string_entities() {
        let f = SampleFilter::new(0.5, 42, "entity_id", BqlType::String).unwrap();
        let entities = vec!["alice", "bob", "charlie", "dave"];
        let array = StringViewArray::from(entities.clone());
        let mask = f.apply_to_array(&array).unwrap();
        assert_eq!(mask.len(), 4);
        for (i, entity) in entities.iter().enumerate() {
            let expected = f.accepts_str(entity.as_bytes());
            assert_eq!(mask.value(i), expected, "mask[{i}] for {entity}");
        }
    }

    #[test]
    fn apply_to_array_filters_int_entities() {
        let f = SampleFilter::new(0.5, 42, "entity_id", BqlType::Int).unwrap();
        let entities: Vec<i64> = (1..=6).collect();
        let array = Int64Array::from(entities.clone());
        let mask = f.apply_to_array(&array).unwrap();
        assert_eq!(mask.len(), 6);
        for (i, &v) in entities.iter().enumerate() {
            let expected = f.accepts_int(v);
            assert_eq!(mask.value(i), expected);
        }
    }

    #[test]
    fn apply_to_array_empty_set_returns_all_false() {
        let f = SampleFilter::new(0.0, 1, "entity_id", BqlType::String).unwrap();
        let array = StringViewArray::from(vec!["a", "b", "c"]);
        let mask = f.apply_to_array(&array).unwrap();
        assert_eq!(mask.len(), 3);
        assert!(mask.iter().all(|b| b == Some(false)));
    }

    #[test]
    fn apply_to_array_pass_through_returns_all_true() {
        let f = SampleFilter::new(1.0, 1, "entity_id", BqlType::String).unwrap();
        let array = StringViewArray::from(vec!["a", "b", "c"]);
        let mask = f.apply_to_array(&array).unwrap();
        assert!(mask.iter().all(|b| b == Some(true)));
    }

    #[test]
    fn apply_to_array_rejects_null_entities() {
        let f = SampleFilter::new(1.0, 1, "entity_id", BqlType::String).unwrap();
        let array: StringViewArray = StringViewArray::from(vec![Some("a"), None, Some("c")]);
        let mask = f.apply_to_array(&array).unwrap();
        // Even in pass-through mode, nulls reject — the invariant is
        // "sampled entities have stable hashes"; nulls don't.
        assert!(mask.value(0));
        assert!(!mask.value(1));
        assert!(mask.value(2));
    }

    #[test]
    fn apply_to_array_rejects_type_mismatch() {
        let f = SampleFilter::new(0.5, 1, "entity_id", BqlType::String).unwrap();
        let array = Int64Array::from(vec![1, 2, 3]);
        assert!(f.apply_to_array(&array).is_err());
    }

    // ── misc accessors ───────────────────────────────────────────────────────

    #[test]
    fn accessors_return_constructed_values() {
        let f = SampleFilter::new(0.25, 99, "my_col", BqlType::Int).unwrap();
        assert_eq!(f.entity_column(), "my_col");
        assert_eq!(f.entity_type(), &BqlType::Int);
        assert!((f.fraction() - 0.25).abs() < f64::EPSILON);
        assert_eq!(f.seed(), 99);
    }
}
