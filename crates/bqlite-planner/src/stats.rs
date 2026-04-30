//! Plan-time statistics surface for the optimizer (TASK-521 / TASK-504).
//!
//! `PlannerStats` is a frozen snapshot of the small allowlisted set of
//! statistics the Wave 5 optimizer is permitted to consult, per the policy
//! defined in `docs/design/planner/optimizer-direction.md` §4 and §6.
//!
//! ## Contract
//!
//! - One snapshot per query, constructed at planner entry.
//! - Phase 5.1 (plan-time) reads only the manifest-derived fields and the
//!   index-registry booleans. `cohort_size` is empty during phase 5.1 and
//!   reads against it panic in debug builds (programmer error).
//! - Phase 5.2 (post-cohort) is opened by exactly one call to
//!   [`PlannerStats::bind_cohorts`]; subsequent calls panic.
//! - The struct's public fields are the **complete** plan-time stats
//!   surface. `optimizer-direction.md` §6.1 explicitly rules out trait
//!   abstractions: there is one producer (manifest-derived snapshot) and
//!   one consumer (`PlannerStatsView`).
//!
//! ## Per-rule budget enforcement
//!
//! Every optimizer rule declares a [`StatsBudget`] enumerating which
//! fields it may read. The pipeline driver narrows `PlannerStats` into a
//! [`PlannerStatsView`] for that rule; reads against fields the rule did
//! not declare panic in debug builds. The matrix in
//! `optimizer-direction.md` §7 is the spec; this enforcement is a
//! tripwire that surfaces accidental dependency creep during testing.
//!
//! No `from_manifest` constructor ships in this task — the storage-side
//! wiring lands with TASK-522 (which already holds the `Manifest` at the
//! engine call site that calls `bind_cohorts`). Until then,
//! [`PlannerStats::empty`] is the only constructor; the registered rule
//! slots that consult `value_set_indexed` / `entity_presence_indexed`
//! find empty maps and degrade to no-ops, which is the explicit
//! sequencing caveat in `optimizer-direction.md` §10.3.

use std::collections::HashMap;

/// Identifier for a materialized cohort. Issued by the engine's query
/// coordinator (TASK-522) when it materializes a cohort prior to the
/// outer scan. Wave 5 does not need a richer identity than a small
/// integer — cohorts live for the duration of one query.
pub type CohortId = u32;

/// Snapshot of plan-time-readable statistics for one query.
///
/// See `docs/design/planner/optimizer-direction.md` §4 for the catalog
/// of admitted sources and §6 for the access discipline. Fields are
/// public so the engine-side `bind_cohorts` and (future) manifest
/// derivation can populate them without a trait abstraction.
#[derive(Debug, Clone, Default)]
pub struct PlannerStats {
    /// §4.1 — sum of `SegmentMeta.row_count` per table.
    pub table_row_count: HashMap<String, u64>,
    /// §4.1 — number of active segments per table.
    pub table_segment_count: HashMap<String, u32>,
    /// §4.1 — sum of `SegmentMeta.byte_size` per table (informational).
    pub table_byte_count: HashMap<String, u64>,
    /// §4.1 — min/max of `ts_range` across all segments per table.
    pub table_time_extent: HashMap<String, (i64, i64)>,

    /// §4.4 — per-(table, column) Tier-3 value-set index registry.
    pub value_set_indexed: HashMap<(String, String), bool>,
    /// §4.4 — per-(table, anchor_event_type) Tier-2 entity-presence
    /// bitmap registry.
    pub entity_presence_indexed: HashMap<(String, String), bool>,

    /// §4.5 — exact cohort sizes after materialization. Empty during
    /// phase 5.1; populated exactly once by [`bind_cohorts`] before the
    /// post-cohort rule pass runs. Reading this map before binding is a
    /// programmer error and panics in debug builds (see
    /// [`PlannerStatsView::cohort_size`]).
    pub cohort_size: HashMap<CohortId, u64>,

    /// True after [`bind_cohorts`] has been called. Used by
    /// [`PlannerStatsView`] to enforce the snapshot discipline in
    /// `optimizer-direction.md` §6.2.
    cohorts_bound: bool,
}

impl PlannerStats {
    /// Construct an empty snapshot — every map is empty,
    /// `cohorts_bound` is false. This is the entry-point used by the
    /// planner today; manifest-derived population lands with TASK-522.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Returns true if [`bind_cohorts`] has been called on this
    /// snapshot.
    pub fn cohorts_bound(&self) -> bool {
        self.cohorts_bound
    }

    /// Bind cohort sizes for phase 5.2.
    ///
    /// Per `optimizer-direction.md` §6.2 this is the one place phase
    /// 5.1's snapshot is mutated, and it must be called exactly once.
    /// `sizes` lists every materialized cohort's exact row count.
    ///
    /// # Panics
    ///
    /// Panics if called twice on the same snapshot. A second call
    /// indicates a bug in the engine's query-coordinator sequencing
    /// (the planner does not invoke this directly).
    pub fn bind_cohorts(&mut self, sizes: &[(CohortId, u64)]) {
        assert!(
            !self.cohorts_bound,
            "PlannerStats::bind_cohorts called twice — cohort sizes must be \
             bound exactly once per query (optimizer-direction.md §6.2)",
        );
        self.cohort_size.reserve(sizes.len());
        for (id, count) in sizes {
            self.cohort_size.insert(*id, *count);
        }
        self.cohorts_bound = true;
    }
}

/// Per-rule budget declaring which `PlannerStats` fields a rule is
/// permitted to read. Default: all-false (a pure structural rule).
///
/// The budget is enforced by [`PlannerStatsView`]: reads against a
/// field whose budget bit is false panic in debug builds. See
/// `optimizer-direction.md` §6.4 for the rationale (tripwire, not
/// hard guarantee).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StatsBudget {
    /// `table_row_count`, `table_segment_count`, `table_byte_count`,
    /// `table_time_extent` — manifest-derived aggregates (§4.1).
    pub catalog_aggregates: bool,
    /// `value_set_indexed`, `entity_presence_indexed` — per-column
    /// index registry booleans (§4.4).
    pub index_registry: bool,
    /// `cohort_size` — exact post-materialization cohort sizes (§4.5).
    /// Implies the rule runs in phase 5.2; the pipeline driver checks
    /// `PlannerStats::cohorts_bound` before invoking such rules.
    pub cohort_sizes: bool,
}

impl StatsBudget {
    /// Const constructor for use in `const`-context rule declarations.
    pub const fn none() -> Self {
        Self {
            catalog_aggregates: false,
            index_registry: false,
            cohort_sizes: false,
        }
    }
}

/// Narrowed view of [`PlannerStats`] for one rule, enforcing the
/// rule's [`StatsBudget`] on every read.
///
/// Construct via [`PlannerStats::view_for`]. Reads against fields the
/// rule did not declare panic in debug builds and silently succeed in
/// release builds (matching the design-doc tripwire policy:
/// `optimizer-direction.md` §6.4).
pub struct PlannerStatsView<'a> {
    stats: &'a PlannerStats,
    budget: StatsBudget,
    /// Identifies the rule for panic messages.
    rule_id: &'static str,
}

impl PlannerStats {
    /// Construct a [`PlannerStatsView`] for a rule with the given
    /// budget. The `rule_id` is used in panic messages when a read
    /// violates the budget.
    pub fn view_for<'a>(
        &'a self,
        rule_id: &'static str,
        budget: StatsBudget,
    ) -> PlannerStatsView<'a> {
        PlannerStatsView {
            stats: self,
            budget,
            rule_id,
        }
    }
}

impl<'a> PlannerStatsView<'a> {
    /// Returns the underlying [`StatsBudget`] this view enforces.
    pub fn budget(&self) -> StatsBudget {
        self.budget
    }

    /// §4.1 — sum of segment row counts for `table`.
    pub fn table_row_count(&self, table: &str) -> Option<u64> {
        debug_assert!(
            self.budget.catalog_aggregates,
            "rule '{}' read table_row_count without declaring \
             StatsBudget::catalog_aggregates",
            self.rule_id,
        );
        self.stats.table_row_count.get(table).copied()
    }

    /// §4.1 — number of active segments for `table`.
    pub fn table_segment_count(&self, table: &str) -> Option<u32> {
        debug_assert!(
            self.budget.catalog_aggregates,
            "rule '{}' read table_segment_count without declaring \
             StatsBudget::catalog_aggregates",
            self.rule_id,
        );
        self.stats.table_segment_count.get(table).copied()
    }

    /// §4.1 — sum of segment byte sizes for `table`.
    pub fn table_byte_count(&self, table: &str) -> Option<u64> {
        debug_assert!(
            self.budget.catalog_aggregates,
            "rule '{}' read table_byte_count without declaring \
             StatsBudget::catalog_aggregates",
            self.rule_id,
        );
        self.stats.table_byte_count.get(table).copied()
    }

    /// §4.1 — min/max ts_range across segments for `table`.
    pub fn table_time_extent(&self, table: &str) -> Option<(i64, i64)> {
        debug_assert!(
            self.budget.catalog_aggregates,
            "rule '{}' read table_time_extent without declaring \
             StatsBudget::catalog_aggregates",
            self.rule_id,
        );
        self.stats.table_time_extent.get(table).copied()
    }

    /// §4.4 — does the (table, column) pair carry a registered Tier-3
    /// value-set index? Returns `false` if the registry has no entry.
    pub fn value_set_indexed(&self, table: &str, column: &str) -> bool {
        debug_assert!(
            self.budget.index_registry,
            "rule '{}' read value_set_indexed without declaring \
             StatsBudget::index_registry",
            self.rule_id,
        );
        self.stats
            .value_set_indexed
            .get(&(table.to_string(), column.to_string()))
            .copied()
            .unwrap_or(false)
    }

    /// §4.4 — does the (table, anchor_event_type) pair carry a
    /// registered Tier-2 entity-presence bitmap?
    pub fn entity_presence_indexed(&self, table: &str, anchor_event: &str) -> bool {
        debug_assert!(
            self.budget.index_registry,
            "rule '{}' read entity_presence_indexed without declaring \
             StatsBudget::index_registry",
            self.rule_id,
        );
        self.stats
            .entity_presence_indexed
            .get(&(table.to_string(), anchor_event.to_string()))
            .copied()
            .unwrap_or(false)
    }

    /// §4.5 — exact materialized size for `cohort`. Returns `None` if
    /// the cohort was not materialized.
    ///
    /// # Panics
    ///
    /// Panics in debug builds if the snapshot's cohorts have not yet
    /// been bound (phase 5.1 reading a phase-5.2-only field). Callers
    /// should declare `StatsBudget::cohort_sizes` and only run in the
    /// post-cohort phase.
    pub fn cohort_size(&self, cohort: CohortId) -> Option<u64> {
        debug_assert!(
            self.budget.cohort_sizes,
            "rule '{}' read cohort_size without declaring \
             StatsBudget::cohort_sizes",
            self.rule_id,
        );
        debug_assert!(
            self.stats.cohorts_bound,
            "rule '{}' read cohort_size before bind_cohorts (phase 5.1 \
             reading a phase-5.2 field)",
            self.rule_id,
        );
        self.stats.cohort_size.get(&cohort).copied()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_snapshot_has_no_data_and_unbound_cohorts() {
        let stats = PlannerStats::empty();
        assert!(stats.table_row_count.is_empty());
        assert!(stats.table_segment_count.is_empty());
        assert!(stats.table_byte_count.is_empty());
        assert!(stats.table_time_extent.is_empty());
        assert!(stats.value_set_indexed.is_empty());
        assert!(stats.entity_presence_indexed.is_empty());
        assert!(stats.cohort_size.is_empty());
        assert!(!stats.cohorts_bound());
    }

    #[test]
    fn budget_default_reads_no_fields() {
        let b = StatsBudget::default();
        assert!(!b.catalog_aggregates);
        assert!(!b.index_registry);
        assert!(!b.cohort_sizes);
        // const-constructor matches default.
        assert_eq!(b, StatsBudget::none());
    }

    #[test]
    fn view_with_catalog_aggregates_budget_reads_table_row_count() {
        let mut stats = PlannerStats::empty();
        stats.table_row_count.insert("events".into(), 12_345);
        let view = stats.view_for(
            "test",
            StatsBudget {
                catalog_aggregates: true,
                ..Default::default()
            },
        );
        assert_eq!(view.table_row_count("events"), Some(12_345));
        assert_eq!(view.table_row_count("missing"), None);
    }

    #[test]
    fn view_with_index_registry_budget_returns_false_for_missing_keys() {
        let stats = PlannerStats::empty();
        let view = stats.view_for(
            "test",
            StatsBudget {
                index_registry: true,
                ..Default::default()
            },
        );
        assert!(!view.value_set_indexed("events", "user_id"));
        assert!(!view.entity_presence_indexed("events", "purchase"));
    }

    #[test]
    fn view_with_catalog_aggregates_budget_reads_all_four_fields() {
        // All four catalog-aggregate fields share the same budget bit;
        // exercise each accessor so the budget-enforcement path is
        // covered for every field, not just `table_row_count`.
        let mut stats = PlannerStats::empty();
        stats.table_row_count.insert("events".into(), 1);
        stats.table_segment_count.insert("events".into(), 2);
        stats.table_byte_count.insert("events".into(), 3);
        stats.table_time_extent.insert("events".into(), (10, 20));
        let view = stats.view_for(
            "test",
            StatsBudget {
                catalog_aggregates: true,
                ..Default::default()
            },
        );
        assert_eq!(view.table_row_count("events"), Some(1));
        assert_eq!(view.table_segment_count("events"), Some(2));
        assert_eq!(view.table_byte_count("events"), Some(3));
        assert_eq!(view.table_time_extent("events"), Some((10, 20)));
    }

    #[test]
    #[cfg_attr(
        not(debug_assertions),
        ignore = "budget enforcement is debug-only per optimizer-direction.md §6.4"
    )]
    #[should_panic(expected = "without declaring StatsBudget::index_registry")]
    fn view_with_default_budget_panics_on_entity_presence_read_in_debug() {
        let stats = PlannerStats::empty();
        let view = stats.view_for("test", StatsBudget::default());
        let _ = view.entity_presence_indexed("events", "purchase");
    }

    #[test]
    #[cfg_attr(
        not(debug_assertions),
        ignore = "budget enforcement is debug-only per optimizer-direction.md §6.4"
    )]
    #[should_panic(expected = "without declaring StatsBudget::index_registry")]
    fn view_with_default_budget_panics_on_index_registry_read_in_debug() {
        let stats = PlannerStats::empty();
        let view = stats.view_for("test", StatsBudget::default());
        let _ = view.value_set_indexed("events", "user_id");
    }

    #[test]
    #[cfg_attr(
        not(debug_assertions),
        ignore = "budget enforcement is debug-only per optimizer-direction.md §6.4"
    )]
    #[should_panic(expected = "without declaring StatsBudget::catalog_aggregates")]
    fn view_with_default_budget_panics_on_catalog_aggregate_read_in_debug() {
        let stats = PlannerStats::empty();
        let view = stats.view_for("test", StatsBudget::default());
        let _ = view.table_row_count("events");
    }

    #[test]
    fn bind_cohorts_marks_snapshot_bound_and_populates_sizes() {
        let mut stats = PlannerStats::empty();
        stats.bind_cohorts(&[(7, 100), (9, 250)]);
        assert!(stats.cohorts_bound());
        assert_eq!(stats.cohort_size.get(&7), Some(&100));
        assert_eq!(stats.cohort_size.get(&9), Some(&250));
    }

    #[test]
    fn bind_cohorts_with_empty_input_still_marks_bound() {
        // A query with no cohorts must still transition the snapshot
        // into phase 5.2 — the post-cohort rule pass must be able to
        // run even when no cohort sizes exist to consult.
        let mut stats = PlannerStats::empty();
        stats.bind_cohorts(&[]);
        assert!(stats.cohorts_bound());
        assert!(stats.cohort_size.is_empty());
    }

    #[test]
    #[should_panic(expected = "bind_cohorts called twice")]
    fn bind_cohorts_twice_panics() {
        let mut stats = PlannerStats::empty();
        stats.bind_cohorts(&[(1, 10)]);
        stats.bind_cohorts(&[(2, 20)]);
    }

    #[test]
    fn view_with_cohort_sizes_budget_reads_after_binding() {
        let mut stats = PlannerStats::empty();
        stats.bind_cohorts(&[(3, 42)]);
        let view = stats.view_for(
            "test",
            StatsBudget {
                cohort_sizes: true,
                ..Default::default()
            },
        );
        assert_eq!(view.cohort_size(3), Some(42));
        assert_eq!(view.cohort_size(99), None);
    }

    #[test]
    #[cfg_attr(
        not(debug_assertions),
        ignore = "budget enforcement is debug-only per optimizer-direction.md §6.4"
    )]
    #[should_panic(expected = "before bind_cohorts")]
    fn view_panics_in_debug_when_reading_cohort_before_binding() {
        let stats = PlannerStats::empty();
        let view = stats.view_for(
            "test",
            StatsBudget {
                cohort_sizes: true,
                ..Default::default()
            },
        );
        let _ = view.cohort_size(0);
    }

    #[test]
    fn budget_method_returns_declared_budget() {
        let stats = PlannerStats::empty();
        let budget = StatsBudget {
            catalog_aggregates: true,
            ..Default::default()
        };
        let view = stats.view_for("test", budget);
        assert_eq!(view.budget(), budget);
    }
}
