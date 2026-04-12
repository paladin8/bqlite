//! Zone-map-driven row-group pruning for the segment reader.
//!
//! ## Role in the pushdown pipeline
//!
//! See [`docs/design/storage/predicate-pushdown.md`](../../../docs/design/storage/predicate-pushdown.md) §3
//! for the full pipeline. At row-group boundaries, the scan consults
//! this module to decide whether to decode the row group (accept) or
//! skip it (prune). Every surviving row group is then decoded and
//! filtered row-by-row by the filter operator above the scan, so
//! pruning is only an optimization — it must never change the result
//! set, only the amount of work done to produce it.
//!
//! ## The no-false-negatives invariant
//!
//! This module exists to honour one invariant:
//!
//! > **For any row that would pass the filter operator, the row
//! > group containing that row must never be pruned.**
//!
//! Stated as a one-sided contract: pruning is allowed to be imprecise
//! in one direction only — a row group with no matching rows may
//! still be decoded (wasted work), but a row group with at least one
//! matching row must never be skipped. The per-conjunct decision
//! rules in `bqlite_core::storage::ScanConjunct::accepts_zone` encode
//! this as conservative boundary checks: every ambiguity accepts.
//! The property tests in this module's `tests` section are an
//! executable form of the invariant — randomized row data is
//! compared against the pruning decision and the true filter
//! outcome, and the invariant is asserted for every row.
//!
//! ## Public surface
//!
//! - [`accepts_row_group`] — the atomic decision: given a row
//!   group's zone maps and a predicate, return `true` to decode,
//!   `false` to prune. Thin forwarding wrapper over
//!   [`Predicate::accepts_zone_group`] that lives here so the
//!   reader's hot path does not name the trait method directly.
//! - [`should_decode_row_group`] — full-loop helper combining the
//!   `referenced_columns().is_empty()` fast path with the zone-map
//!   load and the predicate call. This is what the segment reader
//!   calls from `next_row_group`.
//! - [`surviving_row_groups`] — eager batch helper that walks every
//!   row group in a scan and returns the indices the predicate
//!   accepts. Useful for tests, benchmarks, and the pruning-rate
//!   metric that the Wave 2 benchmark suite reports.

use std::collections::HashMap;

use bqlite_core::storage::ScanPredicate;
use bqlite_core::{Predicate, Result, SegmentScan, ZoneMap};

use crate::segment::layout::RowGroupIndex;

/// Decide whether a single row group should be decoded given its
/// zone maps and a predicate.
///
/// Returns `true` when the row group **might** contain at least one
/// row satisfying the predicate — the caller must decode it and
/// hand the rows to the filter operator for exact evaluation.
/// Returns `false` when the predicate rejects every possible row in
/// the row group, and the reader may skip the decode entirely.
///
/// Forwards directly to [`Predicate::accepts_zone_group`]. The thin
/// wrapper exists so this module owns the documentation link back
/// to `docs/design/storage/predicate-pushdown.md` §6 and so callers
/// that already have the row group's zone-map dictionary in hand
/// (tests, benchmarks, `surviving_row_groups`,
/// `should_decode_row_group`) can make the decision without paying
/// the short-circuit check up front.
#[inline]
pub fn accepts_row_group(predicate: &dyn Predicate, zones: &HashMap<String, ZoneMap>) -> bool {
    predicate.accepts_zone_group(zones)
}

/// Decide whether row group `idx` of `scan` should be decoded,
/// encapsulating the full zone-map pruning sequence.
///
/// The three-step decision:
///
/// 1. **Short-circuit**: if the predicate has no referenced columns
///    (its default or a predicate that opts out of pruning), skip
///    the zone-map build entirely and return `true`. This matches
///    the perf-critical path in the reader — the Wave 1 default
///    `Predicate::referenced_columns() -> &[]` must have zero cost
///    and must not allocate a [`HashMap`].
/// 2. **Load zone maps**: ask the scan for row group `idx`'s zone
///    maps via [`SegmentScan::row_group_zone_maps`]. This is a
///    metadata-only read off the footer; no row data is decoded.
/// 3. **Evaluate**: forward to [`accepts_row_group`], which calls
///    [`Predicate::accepts_zone_group`].
///
/// Returns `Ok(true)` to decode the row group, `Ok(false)` to prune
/// it. Errors bubble up from the zone-map load.
pub fn should_decode_row_group(
    scan: &dyn SegmentScan,
    predicate: &dyn Predicate,
    idx: usize,
) -> Result<bool> {
    if predicate.referenced_columns().is_empty() {
        return Ok(true);
    }
    let zones = scan.row_group_zone_maps(idx)?;
    Ok(accepts_row_group(predicate, &zones))
}

/// Walk every row group in `scan` and return the indices of those
/// the predicate does not prune.
///
/// Eager batch helper. The primary consumers:
///
/// - **Tests** — assert which row groups survive a given predicate
///   without decoding any row data.
/// - **Benchmarks** — measure the false-positive rate (surviving
///   row groups / total row groups) on synthetic workloads. The
///   Wave 2 perf gate requires ≥80% pruning on the acceptance query
///   (see `predicate-pushdown.md` §12.1).
/// - **Future batch planners** — any caller that wants a
///   materialized pruning plan before opening a decode pipeline.
///
/// A `None` predicate (no pushdown) returns every row group index.
pub fn surviving_row_groups(
    scan: &dyn SegmentScan,
    predicate: Option<&dyn Predicate>,
) -> Result<Vec<usize>> {
    let count = scan.row_group_count();
    let mut survivors = Vec::with_capacity(count);
    for idx in 0..count {
        let accept = match predicate {
            None => true,
            Some(p) => should_decode_row_group(scan, p, idx)?,
        };
        if accept {
            survivors.push(idx);
        }
    }
    Ok(survivors)
}

/// Evaluate a [`ScanPredicate`] directly against a row-group's footer
/// metadata, avoiding the [`HashMap`] allocation that the
/// trait-based [`should_decode_row_group`] path requires.
///
/// This is the performance-critical pruning entry point used by
/// `SegmentFileScan::next_row_group` (TASK-247). For every conjunct
/// in the predicate, it finds the matching column's
/// [`ColumnChunkMeta`] by name (using the write-time schema to map
/// `column_ordinal` → name), constructs an ephemeral [`ZoneMap`] on
/// the stack, and calls `accepts_zone`. A conjunct whose column is
/// absent from the row-group's metadata is conservatively accepted
/// (per `predicate-pushdown.md` §6).
///
/// Returns `true` iff every conjunct accepts the row-group — same
/// semantics as [`accepts_row_group`], but without allocating.
#[inline]
pub fn accepts_row_group_inline(
    predicate: &ScanPredicate,
    rg: &RowGroupIndex,
    schema_columns: &[bqlite_core::ColumnDef],
) -> bool {
    if predicate.conjuncts.is_empty() {
        return true;
    }
    predicate.conjuncts.iter().all(|conj| {
        let col_name = conj.column();
        // Find the column chunk whose write-time schema name matches.
        let meta = rg.columns.iter().find(|cc| {
            schema_columns
                .get(cc.column_ordinal as usize)
                .is_some_and(|c| c.name == col_name)
        });
        match meta {
            Some(cc) => {
                // Build an ephemeral ZoneMap on the stack. The min/max
                // Option<PropertyValue> clones are cheap for Int/Float
                // (Copy-sized) and slightly less so for String (heap
                // alloc). Even for String columns, this is far cheaper
                // than the old path which allocated a full HashMap plus
                // cloned *every* column's zone bounds.
                conj.accepts_zone(&ZoneMap {
                    min: cc.zone_min.as_ref().cloned(),
                    max: cc.zone_max.as_ref().cloned(),
                    null_count: cc.null_count,
                    row_count: rg.row_count,
                })
            }
            // Column absent from this row-group's metadata →
            // conservative accept (schema evolution, §9).
            None => true,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use ::arrow::record_batch::RecordBatch;
    use bqlite_core::storage::{RangeOp, ScanConjunct, ScanPredicate};
    use bqlite_core::PropertyValue;

    // ── Fake SegmentScan driving the batch helpers ──────────────────
    //
    // Real segment integration lives in `segment::reader`'s tests;
    // this module's tests exercise the pruning decision logic in
    // isolation, using a hand-crafted scan whose row-group zone maps
    // are spelled out up front.

    struct FakeScan {
        zones_per_rg: Vec<HashMap<String, ZoneMap>>,
    }

    impl SegmentScan for FakeScan {
        fn row_group_count(&self) -> usize {
            self.zones_per_rg.len()
        }

        fn row_group_zone_maps(&self, idx: usize) -> Result<HashMap<String, ZoneMap>> {
            Ok(self.zones_per_rg[idx].clone())
        }

        fn next_row_group(&mut self) -> Result<Option<RecordBatch>> {
            // Decoder is out of scope for the pruning-decision tests.
            Ok(None)
        }
    }

    fn int_zone(min: i64, max: i64, nulls: u64, rows: u64) -> ZoneMap {
        ZoneMap {
            min: Some(PropertyValue::Int(min)),
            max: Some(PropertyValue::Int(max)),
            null_count: nulls,
            row_count: rows,
        }
    }

    fn zones_for(col: &str, zm: ZoneMap) -> HashMap<String, ZoneMap> {
        let mut m = HashMap::new();
        m.insert(col.to_string(), zm);
        m
    }

    // ── accepts_row_group ────────────────────────────────────────────

    #[test]
    fn accepts_row_group_forwards_to_predicate_accepts_zone_group() {
        let pred = ScanPredicate::new(vec![ScanConjunct::Range {
            column: "amount".into(),
            op: RangeOp::Gt,
            value: PropertyValue::Int(100),
        }]);
        // Row group has values 1..=50 — below the threshold.
        assert!(!accepts_row_group(
            &pred,
            &zones_for("amount", int_zone(1, 50, 0, 64))
        ));
        // Row group has values 1..=200 — some may exceed the threshold.
        assert!(accepts_row_group(
            &pred,
            &zones_for("amount", int_zone(1, 200, 0, 64))
        ));
    }

    // ── should_decode_row_group ──────────────────────────────────────

    #[test]
    fn should_decode_short_circuits_when_no_referenced_columns() {
        // A predicate that claims no referenced columns must cause
        // the helper to skip the zone-map build — the FakeScan's
        // `row_group_zone_maps` is still wired so the answer would
        // be the same either way, but the optimization is what makes
        // the Wave 1 default-impl path zero-alloc.
        #[derive(Debug)]
        struct AlwaysAccept;
        impl Predicate for AlwaysAccept {
            fn accepts_zone(&self, _column: &str, _zone: &ZoneMap) -> bool {
                true
            }
            // Inherits `referenced_columns() -> &[]` default.
        }
        let scan = FakeScan {
            zones_per_rg: vec![zones_for("amount", int_zone(1, 10, 0, 64))],
        };
        let pred = AlwaysAccept;
        assert!(should_decode_row_group(&scan, &pred, 0).unwrap());
    }

    #[test]
    fn should_decode_loads_zones_when_referenced_non_empty() {
        let scan = FakeScan {
            zones_per_rg: vec![
                zones_for("amount", int_zone(1, 10, 0, 64)),
                zones_for("amount", int_zone(200, 300, 0, 64)),
            ],
        };
        let pred = ScanPredicate::new(vec![ScanConjunct::Range {
            column: "amount".into(),
            op: RangeOp::Gt,
            value: PropertyValue::Int(100),
        }]);
        // rg 0 is below the threshold → prune (decode=false)
        assert!(!should_decode_row_group(&scan, &pred, 0).unwrap());
        // rg 1 is above the threshold → decode (decode=true)
        assert!(should_decode_row_group(&scan, &pred, 1).unwrap());
    }

    // ── surviving_row_groups ────────────────────────────────────────

    #[test]
    fn surviving_row_groups_returns_every_idx_when_no_predicate() {
        let scan = FakeScan {
            zones_per_rg: vec![
                zones_for("amount", int_zone(1, 10, 0, 64)),
                zones_for("amount", int_zone(11, 20, 0, 64)),
                zones_for("amount", int_zone(21, 30, 0, 64)),
            ],
        };
        assert_eq!(surviving_row_groups(&scan, None).unwrap(), vec![0, 1, 2]);
    }

    #[test]
    fn surviving_row_groups_prunes_to_the_matching_subset() {
        let scan = FakeScan {
            zones_per_rg: vec![
                zones_for("amount", int_zone(1, 10, 0, 64)),
                zones_for("amount", int_zone(50, 150, 0, 64)),
                zones_for("amount", int_zone(200, 300, 0, 64)),
                zones_for("amount", int_zone(5, 8, 0, 64)),
            ],
        };
        let pred = ScanPredicate::new(vec![ScanConjunct::Range {
            column: "amount".into(),
            op: RangeOp::Gt,
            value: PropertyValue::Int(100),
        }]);
        // rg 0: max=10 ≤ 100 → prune
        // rg 1: max=150 > 100 → survive
        // rg 2: max=300 > 100 → survive
        // rg 3: max=8 ≤ 100 → prune
        assert_eq!(
            surviving_row_groups(&scan, Some(&pred)).unwrap(),
            vec![1, 2]
        );
    }

    #[test]
    fn surviving_row_groups_multi_column_predicate() {
        // Predicate: amount > 100 AND event = 'checkout' — only
        // row groups where BOTH zones survive should pass.
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
        ]);
        let mut rg0 = HashMap::new();
        rg0.insert("amount".to_string(), int_zone(1, 50, 0, 64));
        rg0.insert(
            "event".to_string(),
            ZoneMap {
                min: Some(PropertyValue::String("checkout".into())),
                max: Some(PropertyValue::String("checkout".into())),
                null_count: 0,
                row_count: 64,
            },
        );
        // rg0 rejects on amount.
        let mut rg1 = HashMap::new();
        rg1.insert("amount".to_string(), int_zone(200, 500, 0, 64));
        rg1.insert(
            "event".to_string(),
            ZoneMap {
                min: Some(PropertyValue::String("view".into())),
                max: Some(PropertyValue::String("view".into())),
                null_count: 0,
                row_count: 64,
            },
        );
        // rg1 passes amount but rejects event ("checkout" is outside ["view","view"]).
        let mut rg2 = HashMap::new();
        rg2.insert("amount".to_string(), int_zone(200, 500, 0, 64));
        rg2.insert(
            "event".to_string(),
            ZoneMap {
                min: Some(PropertyValue::String("add_to_cart".into())),
                max: Some(PropertyValue::String("view".into())),
                null_count: 0,
                row_count: 64,
            },
        );
        // rg2 passes both.
        let scan = FakeScan {
            zones_per_rg: vec![rg0, rg1, rg2],
        };
        assert_eq!(surviving_row_groups(&scan, Some(&pred)).unwrap(), vec![2]);
    }

    #[test]
    fn surviving_row_groups_missing_column_is_conservative() {
        // Conjunct on a column absent from every row group's zone
        // map must accept all row groups — the scan operator relies
        // on this to read columns added after the segment was
        // written (schema evolution, §9 of the design doc).
        let pred = ScanPredicate::new(vec![ScanConjunct::Range {
            column: "referrer".into(),
            op: RangeOp::Gt,
            value: PropertyValue::Int(0),
        }]);
        let scan = FakeScan {
            zones_per_rg: vec![
                zones_for("amount", int_zone(1, 10, 0, 64)),
                zones_for("amount", int_zone(11, 20, 0, 64)),
            ],
        };
        assert_eq!(
            surviving_row_groups(&scan, Some(&pred)).unwrap(),
            vec![0, 1]
        );
    }

    // ── Arc<dyn Predicate> interop ──────────────────────────────────

    #[test]
    fn helpers_accept_arc_dyn_predicate() {
        // The reader holds its predicate as `Option<Arc<dyn
        // Predicate>>`. Smoke-test that the helpers work with that
        // shape directly — no inference gymnastics at the call site.
        let pred: Arc<dyn Predicate> = Arc::new(ScanPredicate::new(vec![ScanConjunct::Equal {
            column: "x".into(),
            value: PropertyValue::Int(5),
        }]));
        let scan = FakeScan {
            zones_per_rg: vec![zones_for("x", int_zone(1, 10, 0, 64))],
        };
        assert!(should_decode_row_group(&scan, &*pred, 0).unwrap());
        assert_eq!(surviving_row_groups(&scan, Some(&*pred)).unwrap(), vec![0]);
    }

    // ─────────────────────────────────────────────────────────────────
    // Property test: no-false-negatives invariant
    //
    // Generates random row data and random predicates, then asserts
    // that a row group with any matching row is NEVER pruned. This
    // is the core correctness contract of zone-map pruning — see the
    // module docs above. The deterministic driver below uses a
    // small pseudo-RNG so the test is fast and reproducible without
    // pulling in a property-test framework.
    //
    // Coverage: Int columns under all four range operators, plus
    // the three-valued null case. Extending coverage to String /
    // Float types is a follow-up once those types are exercised
    // by a real end-to-end pipeline.
    // ─────────────────────────────────────────────────────────────────

    /// Splittable-PRNG-free seeded RNG. Uses xorshift64 for speed —
    /// quality is more than sufficient for invariant testing.
    struct Xorshift64(u64);

    impl Xorshift64 {
        fn next_u64(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }

        fn next_i64(&mut self) -> i64 {
            self.next_u64() as i64
        }

        fn next_bool(&mut self, true_odds: u32) -> bool {
            (self.next_u64() % 100) < (true_odds as u64)
        }
    }

    fn compute_int_zone(rows: &[Option<i64>]) -> ZoneMap {
        let mut min: Option<i64> = None;
        let mut max: Option<i64> = None;
        let mut nulls: u64 = 0;
        for r in rows {
            match r {
                Some(v) => {
                    min = Some(min.map_or(*v, |m| m.min(*v)));
                    max = Some(max.map_or(*v, |m| m.max(*v)));
                }
                None => nulls += 1,
            }
        }
        ZoneMap {
            min: min.map(PropertyValue::Int),
            max: max.map(PropertyValue::Int),
            null_count: nulls,
            row_count: rows.len() as u64,
        }
    }

    fn row_matches_range(row: Option<i64>, op: RangeOp, threshold: i64) -> bool {
        // NULL rows produce UNKNOWN under three-valued logic and are
        // dropped by the filter operator — we treat them as "does not
        // match" for the purposes of computing the true-match set.
        match row {
            None => false,
            Some(v) => match op {
                RangeOp::Lt => v < threshold,
                RangeOp::Le => v <= threshold,
                RangeOp::Gt => v > threshold,
                RangeOp::Ge => v >= threshold,
            },
        }
    }

    fn row_matches_equal(row: Option<i64>, target: i64) -> bool {
        matches!(row, Some(v) if v == target)
    }

    fn row_matches_not_equal(row: Option<i64>, target: i64) -> bool {
        matches!(row, Some(v) if v != target)
    }

    fn row_matches_in_set(row: Option<i64>, set: &[i64]) -> bool {
        match row {
            None => false,
            Some(v) => set.contains(&v),
        }
    }

    #[test]
    fn no_false_negatives_range_int_over_random_workload() {
        let mut rng = Xorshift64(0xDEAD_BEEF_CAFE_F00D);
        // 500 random row groups, 100 rows each. The state space is
        // large enough that an off-by-one in `accepts_zone` would
        // surface within a few iterations.
        for _ in 0..500 {
            let rows: Vec<Option<i64>> = (0..100)
                .map(|_| {
                    if rng.next_bool(15) {
                        None
                    } else {
                        // Bias values into a narrow range so the
                        // acceptance/rejection boundary gets exercised.
                        Some(rng.next_i64() % 1000)
                    }
                })
                .collect();
            let zone = compute_int_zone(&rows);
            let zones = zones_for("x", zone);

            for &op in &[RangeOp::Lt, RangeOp::Le, RangeOp::Gt, RangeOp::Ge] {
                let threshold = rng.next_i64() % 1200 - 100;
                let pred = ScanPredicate::new(vec![ScanConjunct::Range {
                    column: "x".into(),
                    op,
                    value: PropertyValue::Int(threshold),
                }]);

                let accepted = accepts_row_group(&pred, &zones);
                let any_row_matches = rows.iter().any(|r| row_matches_range(*r, op, threshold));

                // The invariant: if any row matches, the predicate
                // must accept the row group. The converse — accept
                // despite no matching rows — is a false positive and
                // allowed (wasted decode work, not a correctness bug).
                assert!(
                    !any_row_matches || accepted,
                    "no-false-negatives violated: op={op:?} threshold={threshold} rows={rows:?}"
                );
            }
        }
    }

    #[test]
    fn no_false_negatives_equal_and_not_equal_over_random_workload() {
        let mut rng = Xorshift64(0x1234_5678_9ABC_DEF0);
        for _ in 0..500 {
            let rows: Vec<Option<i64>> = (0..100)
                .map(|_| {
                    if rng.next_bool(10) {
                        None
                    } else {
                        Some(rng.next_i64() % 50)
                    }
                })
                .collect();
            let zone = compute_int_zone(&rows);
            let zones = zones_for("x", zone);

            let target = rng.next_i64() % 60 - 5;

            // Equal.
            let eq_pred = ScanPredicate::new(vec![ScanConjunct::Equal {
                column: "x".into(),
                value: PropertyValue::Int(target),
            }]);
            let eq_accepted = accepts_row_group(&eq_pred, &zones);
            let eq_any_match = rows.iter().any(|r| row_matches_equal(*r, target));
            assert!(
                !eq_any_match || eq_accepted,
                "equal invariant violated: target={target} rows={rows:?}"
            );

            // NotEqual.
            let ne_pred = ScanPredicate::new(vec![ScanConjunct::NotEqual {
                column: "x".into(),
                value: PropertyValue::Int(target),
            }]);
            let ne_accepted = accepts_row_group(&ne_pred, &zones);
            let ne_any_match = rows.iter().any(|r| row_matches_not_equal(*r, target));
            assert!(
                !ne_any_match || ne_accepted,
                "not-equal invariant violated: target={target} rows={rows:?}"
            );
        }
    }

    #[test]
    fn no_false_negatives_in_set_over_random_workload() {
        let mut rng = Xorshift64(0x0BAD_C0DE_CAFE_BABE);
        for _ in 0..500 {
            let rows: Vec<Option<i64>> = (0..100)
                .map(|_| {
                    if rng.next_bool(10) {
                        None
                    } else {
                        Some(rng.next_i64() % 100)
                    }
                })
                .collect();
            let zone = compute_int_zone(&rows);
            let zones = zones_for("x", zone);

            // Random IN set, size 1..=4.
            let set_size = 1 + (rng.next_u64() % 4) as usize;
            let raw: Vec<i64> = (0..set_size).map(|_| rng.next_i64() % 120 - 10).collect();
            let values: Vec<PropertyValue> = raw.iter().map(|v| PropertyValue::Int(*v)).collect();

            let pred = ScanPredicate::new(vec![ScanConjunct::InSet {
                column: "x".into(),
                values,
            }]);
            let accepted = accepts_row_group(&pred, &zones);
            let any_match = rows.iter().any(|r| row_matches_in_set(*r, &raw));
            assert!(
                !any_match || accepted,
                "in-set invariant violated: set={raw:?} rows={rows:?}"
            );
        }
    }

    #[test]
    fn no_false_negatives_null_predicates_over_random_workload() {
        let mut rng = Xorshift64(0xFEED_FACE_DEAD_BEEF);
        for _ in 0..500 {
            let rows: Vec<Option<i64>> = (0..100)
                .map(|_| {
                    if rng.next_bool(25) {
                        None
                    } else {
                        Some(rng.next_i64() % 100)
                    }
                })
                .collect();
            let zone = compute_int_zone(&rows);
            let zones = zones_for("x", zone);

            let is_null = ScanPredicate::new(vec![ScanConjunct::IsNull { column: "x".into() }]);
            let is_null_accepted = accepts_row_group(&is_null, &zones);
            let has_null = rows.iter().any(|r| r.is_none());
            assert!(
                !has_null || is_null_accepted,
                "is-null invariant violated: rows={rows:?}"
            );

            let is_not_null =
                ScanPredicate::new(vec![ScanConjunct::IsNotNull { column: "x".into() }]);
            let is_not_null_accepted = accepts_row_group(&is_not_null, &zones);
            let has_non_null = rows.iter().any(|r| r.is_some());
            assert!(
                !has_non_null || is_not_null_accepted,
                "is-not-null invariant violated: rows={rows:?}"
            );
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // False-positive rate sanity — synthetic partitioned workloads
    //
    // The no-false-negatives tests above prove correctness: any
    // matching row is preserved. These tests prove *effectiveness*:
    // on realistic synthetic workloads, the pruning decision actually
    // eliminates most non-matching row groups (so the reader isn't
    // paying I/O to decode row groups it can't use).
    //
    // The Wave 2 perf gate requires ≥80% row-group pruning on the
    // acceptance query (`event = 'checkout' AND amount > 100`) —
    // that's a benchmark-level assertion owned by TASK-236/TASK-241
    // on the 100M-row reference dataset. These unit tests pin the
    // same property on tiny synthetic workloads so regressions in
    // the decision rules trip immediately, without waiting for the
    // bench suite.
    // ─────────────────────────────────────────────────────────────────

    #[test]
    fn pruning_rate_exceeds_threshold_on_partitioned_range_workload() {
        // 100 row groups, each holding 100 values in a disjoint
        // window: rg k has values in [k*100, k*100+99]. Predicate
        // `x > 9000` can match only rg 90..=99 — 10 row groups.
        // Every other row group has `max < threshold` and must be
        // pruned. Expected prune rate: 90%.
        let scan = FakeScan {
            zones_per_rg: (0..100)
                .map(|k: i64| zones_for("x", int_zone(k * 100, k * 100 + 99, 0, 100)))
                .collect(),
        };
        let pred = ScanPredicate::new(vec![ScanConjunct::Range {
            column: "x".into(),
            op: RangeOp::Gt,
            value: PropertyValue::Int(9000),
        }]);
        let survivors = surviving_row_groups(&scan, Some(&pred)).unwrap();
        // Exact count: rg 90..=99 survive (rg 90 has max=9099 > 9000,
        // rg 89 has max=8999 < 9000 → prune). 10 survivors, 90 pruned.
        assert_eq!(
            survivors.len(),
            10,
            "expected 10 survivors, got {survivors:?}"
        );
        let prune_rate = 1.0 - (survivors.len() as f64 / 100.0);
        assert!(
            prune_rate >= 0.80,
            "prune rate {prune_rate:.2} below 80% threshold on partitioned range workload"
        );
    }

    #[test]
    fn pruning_rate_exceeds_threshold_on_equality_workload() {
        // 100 row groups, each holding values in a disjoint integer
        // window (rg k spans [k*10, k*10+9]). Predicate `x = 503`
        // can match only rg 50 (values 500..509). Expected prune
        // rate: 99%.
        let scan = FakeScan {
            zones_per_rg: (0..100)
                .map(|k: i64| zones_for("x", int_zone(k * 10, k * 10 + 9, 0, 10)))
                .collect(),
        };
        let pred = ScanPredicate::new(vec![ScanConjunct::Equal {
            column: "x".into(),
            value: PropertyValue::Int(503),
        }]);
        let survivors = surviving_row_groups(&scan, Some(&pred)).unwrap();
        assert_eq!(survivors, vec![50]);
        let prune_rate = 1.0 - (survivors.len() as f64 / 100.0);
        assert!(
            prune_rate >= 0.95,
            "prune rate {prune_rate:.2} below 95% threshold on equality workload"
        );
    }

    #[test]
    fn pruning_rate_on_multi_conjunct_predicate_is_stronger_than_either_alone() {
        // Multi-column workload: each row group has a (category,
        // amount) zone map pair. The conjunction `category = 'B'
        // AND amount > 100` should prune strictly more than either
        // conjunct alone — this is the claim the Wave 2 acceptance
        // query relies on.
        //
        // Workload shape:
        //   rg 0..25:  category in {A}, amount in [0, 50]       ← both reject
        //   rg 25..50: category in {A}, amount in [150, 200]    ← category rejects
        //   rg 50..75: category in {B}, amount in [0, 50]       ← amount rejects
        //   rg 75..100: category in {B}, amount in [150, 200]   ← both accept
        let mut zones_per_rg: Vec<HashMap<String, ZoneMap>> = Vec::with_capacity(100);
        for k in 0..100 {
            let category = if k < 50 { "A" } else { "B" };
            let (amt_min, amt_max) = if k % 50 < 25 { (0, 50) } else { (150, 200) };
            let mut m = HashMap::new();
            m.insert(
                "category".to_string(),
                ZoneMap {
                    min: Some(PropertyValue::String(category.into())),
                    max: Some(PropertyValue::String(category.into())),
                    null_count: 0,
                    row_count: 100,
                },
            );
            m.insert("amount".to_string(), int_zone(amt_min, amt_max, 0, 100));
            zones_per_rg.push(m);
        }
        let scan = FakeScan { zones_per_rg };

        let category_only = ScanPredicate::new(vec![ScanConjunct::Equal {
            column: "category".into(),
            value: PropertyValue::String("B".into()),
        }]);
        let amount_only = ScanPredicate::new(vec![ScanConjunct::Range {
            column: "amount".into(),
            op: RangeOp::Gt,
            value: PropertyValue::Int(100),
        }]);
        let both = ScanPredicate::new(vec![
            ScanConjunct::Equal {
                column: "category".into(),
                value: PropertyValue::String("B".into()),
            },
            ScanConjunct::Range {
                column: "amount".into(),
                op: RangeOp::Gt,
                value: PropertyValue::Int(100),
            },
        ]);

        let category_surv = surviving_row_groups(&scan, Some(&category_only)).unwrap();
        let amount_surv = surviving_row_groups(&scan, Some(&amount_only)).unwrap();
        let both_surv = surviving_row_groups(&scan, Some(&both)).unwrap();

        assert_eq!(category_surv.len(), 50); // rg 50..100
        assert_eq!(amount_surv.len(), 50); // rg 25..50 + rg 75..100
        assert_eq!(both_surv.len(), 25); // rg 75..100 only
                                         // Conjunction must be a subset of either conjunct alone.
        assert!(both_surv.iter().all(|i| category_surv.contains(i)));
        assert!(both_surv.iter().all(|i| amount_surv.contains(i)));
    }

    // ── accepts_row_group_inline (TASK-247) ────────────────────────

    use crate::segment::layout::{ColumnChunkMeta, RowGroupIndex};
    use bqlite_core::ColumnDef;

    fn make_rg(columns: Vec<ColumnChunkMeta>) -> RowGroupIndex {
        RowGroupIndex {
            byte_offset: 0,
            byte_length: 0,
            row_count: 100,
            columns,
        }
    }

    fn make_ccm(
        ordinal: u32,
        min: Option<PropertyValue>,
        max: Option<PropertyValue>,
    ) -> ColumnChunkMeta {
        ColumnChunkMeta {
            column_ordinal: ordinal,
            byte_offset: 0,
            byte_length: 0,
            encoding: 0,
            compression: 0,
            row_count: 100,
            null_count: 0,
            zone_min: min,
            zone_max: max,
        }
    }

    fn schema_cols() -> Vec<ColumnDef> {
        vec![
            ColumnDef {
                name: "amount".into(),
                bql_type: bqlite_core::BqlType::Int,
                nullable: false,
                default_value: None,
            },
            ColumnDef {
                name: "event".into(),
                bql_type: bqlite_core::BqlType::String,
                nullable: false,
                default_value: None,
            },
        ]
    }

    #[test]
    fn inline_prunes_row_group_with_range_predicate() {
        let pred = ScanPredicate::new(vec![ScanConjunct::Range {
            column: "amount".into(),
            op: RangeOp::Gt,
            value: PropertyValue::Int(100),
        }]);
        let rg_below = make_rg(vec![make_ccm(
            0,
            Some(PropertyValue::Int(1)),
            Some(PropertyValue::Int(50)),
        )]);
        let rg_above = make_rg(vec![make_ccm(
            0,
            Some(PropertyValue::Int(50)),
            Some(PropertyValue::Int(200)),
        )]);
        let cols = schema_cols();
        assert!(!accepts_row_group_inline(&pred, &rg_below, &cols));
        assert!(accepts_row_group_inline(&pred, &rg_above, &cols));
    }

    #[test]
    fn inline_multi_conjunct_acceptance_query() {
        // The Wave 2 acceptance predicate: event = 'checkout' AND amount > 100.
        let pred = ScanPredicate::new(vec![
            ScanConjunct::Equal {
                column: "event".into(),
                value: PropertyValue::String("checkout".into()),
            },
            ScanConjunct::Range {
                column: "amount".into(),
                op: RangeOp::Gt,
                value: PropertyValue::Int(100),
            },
        ]);
        let cols = schema_cols();

        // RG with event='view' and amount=1..50 → both reject.
        let rg_both_reject = make_rg(vec![
            make_ccm(0, Some(PropertyValue::Int(1)), Some(PropertyValue::Int(50))),
            make_ccm(
                1,
                Some(PropertyValue::String("view".into())),
                Some(PropertyValue::String("view".into())),
            ),
        ]);
        assert!(!accepts_row_group_inline(&pred, &rg_both_reject, &cols));

        // RG with event='checkout' and amount=200..500 → both accept.
        let rg_both_accept = make_rg(vec![
            make_ccm(
                0,
                Some(PropertyValue::Int(200)),
                Some(PropertyValue::Int(500)),
            ),
            make_ccm(
                1,
                Some(PropertyValue::String("checkout".into())),
                Some(PropertyValue::String("checkout".into())),
            ),
        ]);
        assert!(accepts_row_group_inline(&pred, &rg_both_accept, &cols));

        // RG with event='checkout' but amount=1..50 → amount rejects.
        let rg_amount_rejects = make_rg(vec![
            make_ccm(0, Some(PropertyValue::Int(1)), Some(PropertyValue::Int(50))),
            make_ccm(
                1,
                Some(PropertyValue::String("checkout".into())),
                Some(PropertyValue::String("checkout".into())),
            ),
        ]);
        assert!(!accepts_row_group_inline(&pred, &rg_amount_rejects, &cols));
    }

    #[test]
    fn inline_missing_column_accepts_conservatively() {
        let pred = ScanPredicate::new(vec![ScanConjunct::Range {
            column: "missing".into(),
            op: RangeOp::Gt,
            value: PropertyValue::Int(0),
        }]);
        let rg = make_rg(vec![make_ccm(
            0,
            Some(PropertyValue::Int(1)),
            Some(PropertyValue::Int(10)),
        )]);
        let cols = schema_cols();
        // Column "missing" not in the row-group → conservative accept.
        assert!(accepts_row_group_inline(&pred, &rg, &cols));
    }
}
