//! Engine-side gate for the cohort entity-id pushdown (TASK-522).
//!
//! Implements the post-cohort decision described in
//! `docs/design/planner/optimizer-direction.md` §7 row 9 and
//! `docs/design/language/cohorts-aliases-joins.md` §4.3 / §6.3.1:
//! after a `SubqueryFilter` materialises its cohort, decide whether
//! the cohort's entity-id column qualifies for scan-side pushdown.
//!
//! ## The gate (two predicates, AND-combined)
//!
//! 1. **Shape**: the LHS of the cohort `IN` is a single column
//!    expression that references the outer scan's entity-key column.
//!    Multi-column tuples (`(entity_id, day) IN ...`) and computed
//!    LHS expressions (`QUANTIZE(ts, 1d) IN ...`) do **not** qualify
//!    in v1 — `cohorts-aliases-joins.md §4.3` defers full multi-key
//!    pushdown.
//! 2. **Size**: the cohort's row count is strictly less than
//!    [`COHORT_PUSHDOWN_MAX_SIZE`]. Larger cohorts skip the pushdown
//!    — an entity-id set with millions of values offers almost no
//!    row-group skipping (every zone overlaps) while paying the
//!    per-row hash-set construction cost.
//!
//! Correctness is preserved regardless of which branch fires: the
//! post-scan `SubqueryFilterOperator` probes the full cohort row by
//! row, so a missed pushdown only loses the pruning optimisation.
//!
//! ## Placement
//!
//! Per `optimizer-direction.md §12` Pass 8 lives in `bqlite-engine`
//! because it must run after cohort materialisation, which only the
//! engine's query coordinator sequences. TASK-521's optimizer rule
//! registry exposes `OptimizerPipeline::run_post_cohort` as the
//! eventual landing site for this rule, but the runtime cohort
//! artifact (the materialised hash set) is engine-owned and not
//! visible to the planner — so v1 implements the gate as a direct
//! function call from `bind_subquery_filter`. Moving the gate body
//! into a registered `PostCohort` rule is a future mechanical
//! refactor.

use bqlite_core::storage::ScanConjunct;
use bqlite_core::{PropertyValue, ScalarValue};
use bqlite_operators::cohort::CohortHashSet;
use bqlite_planner::compiled::{CompiledExpr, CompiledNode};

/// Exclusive upper bound on cohort size for entity-id pushdown.
/// Cohorts with `len() < COHORT_PUSHDOWN_MAX_SIZE` qualify; cohorts at
/// or above the threshold are not pushed down.
///
/// Per `optimizer-direction.md` §7 row 9 the threshold is a fixed
/// planner constant. Beyond this size the cohort's
/// `[set_min, set_max]` interval almost certainly covers every
/// row-group's entity-id zone, so the pushdown loses its skip benefit
/// while still paying the per-row construction cost. Tuning is
/// `optimizer-direction.md §10.4` future work.
pub const COHORT_PUSHDOWN_MAX_SIZE: u64 = 65_536;

/// Try to extract a [`ScanConjunct::EntityIn`] from a materialised
/// cohort. Returns `None` when either gate predicate fails.
///
/// `lhs_columns` is the [`bqlite_planner::SubqueryFilterPhysical::lhs_columns`]
/// from the planner — the per-LHS-position compiled expressions whose
/// values the cohort tuple is matched against. `entity_key_col` is
/// the outer scan's entity-key column name (the storage-side conjunct
/// targets this column).
pub fn try_extract_entity_pushdown(
    lhs_columns: &[CompiledExpr],
    cohort: &CohortHashSet,
    entity_key_col: &str,
) -> Option<ScanConjunct> {
    // ── Shape gate ────────────────────────────────────────────────
    if lhs_columns.len() != 1 {
        return None;
    }
    let CompiledNode::Column { ref name, .. } = lhs_columns[0].node else {
        return None;
    };
    if name != entity_key_col {
        return None;
    }
    if cohort.arity() != 1 {
        return None;
    }

    // ── Size gate ────────────────────────────────────────────────
    let size = cohort.len() as u64;
    if size == 0 {
        return None;
    }
    if size >= COHORT_PUSHDOWN_MAX_SIZE {
        return None;
    }

    // Convert the cohort's first-position scalars to PropertyValue.
    // NULL keys are skipped (not aborted) — `IN` against NULL is
    // UNKNOWN under three-valued logic and the post-scan probe drops
    // those outer rows anyway, so removing them from the entity-id
    // set preserves the no-false-negatives invariant.
    let mut values: Vec<PropertyValue> = Vec::with_capacity(cohort.len());
    for key in cohort.iter_keys() {
        let Some(scalar) = key.0.first() else {
            continue;
        };
        let Some(pv) = scalar_to_property_value(scalar) else {
            continue;
        };
        values.push(pv);
    }
    if values.is_empty() {
        return None;
    }

    ScanConjunct::entity_in(entity_key_col.to_string(), values)
}

/// Lossless conversion from runtime [`ScalarValue`] (used by
/// `CohortKey`) to the boundary [`PropertyValue`] (used by
/// `ScanConjunct`). NULL keys do not push down — the cohort's
/// post-scan probe drops outer rows whose LHS is NULL anyway.
///
/// `ScalarValue::Float` columns return `None`. Float entity-key
/// columns are not a documented use case in v1 (entity keys are
/// conventionally `Int` / `String` / `Timestamp`), and `f64` values
/// would interact subtly with NaN semantics inside the sorted Vec
/// backing `EntityIn`. Returning `None` here means a Float entity-key
/// cohort silently falls back to the post-scan probe.
fn scalar_to_property_value(scalar: &ScalarValue) -> Option<PropertyValue> {
    match scalar {
        ScalarValue::Null => None,
        ScalarValue::Bool(b) => Some(PropertyValue::Bool(*b)),
        ScalarValue::Int(i) => Some(PropertyValue::Int(*i)),
        ScalarValue::Float(_) => None,
        ScalarValue::String(s) => Some(PropertyValue::String(s.clone())),
        ScalarValue::Timestamp(t) => Some(PropertyValue::Timestamp(*t)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bqlite_core::BqlType;
    use bqlite_operators::cohort::CohortKey;

    fn col_expr(name: &str) -> CompiledExpr {
        CompiledExpr {
            node: CompiledNode::Column {
                index: 0,
                name: name.into(),
            },
            result_type: BqlType::Int,
            nullable: false,
        }
    }

    fn cohort_with_int_keys(values: &[i64]) -> CohortHashSet {
        let mut set = CohortHashSet::empty(1);
        for &v in values {
            set.insert_for_test(CohortKey(vec![ScalarValue::Int(v)]));
        }
        set
    }

    #[test]
    fn shape_gate_rejects_multi_column_lhs() {
        let lhs = vec![col_expr("entity_id"), col_expr("day")];
        let cohort = cohort_with_int_keys(&[1, 2, 3]);
        assert!(try_extract_entity_pushdown(&lhs, &cohort, "entity_id").is_none());
    }

    #[test]
    fn shape_gate_rejects_non_column_lhs() {
        let lhs = vec![CompiledExpr {
            node: CompiledNode::Literal(PropertyValue::Int(1)),
            result_type: BqlType::Int,
            nullable: false,
        }];
        let cohort = cohort_with_int_keys(&[1]);
        assert!(try_extract_entity_pushdown(&lhs, &cohort, "entity_id").is_none());
    }

    #[test]
    fn shape_gate_rejects_mismatched_column_name() {
        let lhs = vec![col_expr("user_id")];
        let cohort = cohort_with_int_keys(&[1]);
        assert!(try_extract_entity_pushdown(&lhs, &cohort, "entity_id").is_none());
    }

    #[test]
    fn shape_gate_rejects_cohort_arity_mismatch() {
        // LHS arity 1 but cohort declared 2-column — defensive check
        // against planner producing inconsistent shapes.
        let lhs = vec![col_expr("entity_id")];
        let mut cohort = CohortHashSet::empty(2);
        cohort.insert_for_test(CohortKey(vec![ScalarValue::Int(1), ScalarValue::Int(2)]));
        assert!(try_extract_entity_pushdown(&lhs, &cohort, "entity_id").is_none());
    }

    #[test]
    fn size_gate_rejects_at_threshold() {
        let lhs = vec![col_expr("entity_id")];
        let values: Vec<i64> = (0..COHORT_PUSHDOWN_MAX_SIZE as i64).collect();
        let cohort = cohort_with_int_keys(&values);
        assert!(try_extract_entity_pushdown(&lhs, &cohort, "entity_id").is_none());
    }

    #[test]
    fn size_gate_rejects_empty_cohort() {
        let lhs = vec![col_expr("entity_id")];
        let cohort = CohortHashSet::empty(1);
        assert!(try_extract_entity_pushdown(&lhs, &cohort, "entity_id").is_none());
    }

    #[test]
    fn happy_path_emits_entity_in_with_correct_bounds() {
        let lhs = vec![col_expr("entity_id")];
        let cohort = cohort_with_int_keys(&[10, 20, 5, 15]);
        let conj = try_extract_entity_pushdown(&lhs, &cohort, "entity_id").expect("gate accepts");
        match conj {
            ScanConjunct::EntityIn {
                column,
                set_min,
                set_max,
                values,
            } => {
                assert_eq!(column, "entity_id");
                assert_eq!(set_min, PropertyValue::Int(5));
                assert_eq!(set_max, PropertyValue::Int(20));
                assert_eq!(values.len(), 4);
            }
            other => panic!("unexpected conjunct: {other:?}"),
        }
    }

    #[test]
    fn null_keys_in_cohort_are_skipped_not_aborted() {
        let lhs = vec![col_expr("entity_id")];
        let mut cohort = CohortHashSet::empty(1);
        cohort.insert_for_test(CohortKey(vec![ScalarValue::Null]));
        cohort.insert_for_test(CohortKey(vec![ScalarValue::Int(7)]));
        cohort.insert_for_test(CohortKey(vec![ScalarValue::Int(13)]));
        let conj = try_extract_entity_pushdown(&lhs, &cohort, "entity_id")
            .expect("two non-null keys produce a pushdown");
        match conj {
            ScanConjunct::EntityIn { values, .. } => assert_eq!(values.len(), 2),
            other => panic!("unexpected conjunct: {other:?}"),
        }
    }

    #[test]
    fn float_entity_key_cohort_produces_no_pushdown() {
        // Float entity keys aren't a documented use case; the
        // conversion returns None and the gate falls back to the
        // post-scan probe.
        let lhs = vec![col_expr("entity_id")];
        let mut cohort = CohortHashSet::empty(1);
        cohort.insert_for_test(CohortKey(vec![ScalarValue::Float(1.0)]));
        cohort.insert_for_test(CohortKey(vec![ScalarValue::Float(2.0)]));
        assert!(try_extract_entity_pushdown(&lhs, &cohort, "entity_id").is_none());
    }
}
