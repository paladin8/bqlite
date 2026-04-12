//! Planner-side demand propagation types (TASK-317 / TASK-309).
//!
//! `DemandSet` is the planner's "downstream needs" struct that flows
//! backward through the plan tree (root → scan) to determine which
//! columns, match details, step properties, and aggregate fusions each
//! operator must produce. The backward propagation algorithm itself lives
//! in Wave 3's logical → physical lowering (TASK-318); these types are the
//! shared vocabulary it builds on.
//!
//! See `docs/design/planner/wave3-lowering.md` §3.1 for the authoritative
//! field-level specification.
//!
//! ## Crate placement
//!
//! These types live in `bqlite-planner` (not `bqlite-core`) because they
//! are planner-internal. The operator-side dual is `DemandCapabilities` in
//! `bqlite-core`, which advertises what demands an operator can satisfy.

use std::collections::HashSet;

use bqlite_core::{AggFunction, BqlType, OperatorSchema};

use crate::compiled::CompiledExpr;
use crate::expr::TypedExpr;

// ─────────────────────────────────────────────────────────────────────────────
// ColumnId
// ─────────────────────────────────────────────────────────────────────────────

/// A column name used in demand sets.
///
/// A plain `String` alias rather than a newtype so demand sets compose
/// naturally with `OperatorSchema::column_names()` iterators without
/// conversion overhead.
pub type ColumnId = String;

// ─────────────────────────────────────────────────────────────────────────────
// StepPropertyRef
// ─────────────────────────────────────────────────────────────────────────────

/// Identifies a per-step, per-column demand in a `DemandSet`.
///
/// When downstream expressions reference `step_name.column_name`
/// (e.g. `s.plan` in `| WHERE s.plan = 'pro'`), the planner resolves
/// the reference against the MATCH pattern's step table and records a
/// `StepPropertyRef` in the demand set. The `SequenceMatch` node then
/// adds `<step_name>.<column_name>` as a nullable column to its output
/// schema.
///
/// See `wave3-lowering.md` §3.3 for the step-property resolution algorithm.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StepPropertyRef {
    /// User-facing step label (e.g. `"s"` for `s: signup`).
    pub step_name: String,
    /// Property column on the step's event type (e.g. `"plan"`).
    pub column_name: String,
    /// Resolved BQL type from the catalog column definition.
    pub bql_type: BqlType,
}

// ─────────────────────────────────────────────────────────────────────────────
// FusableAggregate
// ─────────────────────────────────────────────────────────────────────────────

/// A single aggregate expression in a fusable aggregate descriptor.
///
/// The logical form of a compiled aggregate — carries `TypedExpr`
/// arguments. TASK-318's physical lowering compiles these into
/// `CompiledAgg` values on the physical descriptor.
#[derive(Debug, Clone, PartialEq)]
pub struct FusableAggExpr {
    /// The resolved aggregate function.
    pub function: AggFunction,
    /// Argument expression (type-checked). `None` for `COUNT(*)`.
    pub arg: Option<TypedExpr>,
    /// Output column name from the BQL alias.
    pub output_name: String,
    /// Output BQL type derived from function + arg type.
    pub output_type: BqlType,
    /// Whether this aggregate output is nullable.
    pub nullable: bool,
}

/// Optimizer's aggregate fusion descriptor (planner-pipeline.md §5.3).
///
/// When the match-aggregate fusion optimizer (TASK-320) detects that an
/// `Aggregate` is immediately downstream of a `SequenceMatch` (optionally
/// separated by a `Filter`), it extracts a `FusableAggregate` and sets it
/// on the `SequenceMatch` node's `fused_downstream` field and propagates
/// it through the `DemandSet`.
///
/// The physical planner (TASK-318) compiles the typed expressions in this
/// struct into `CompiledFusableAggregate` on the `SequenceMatchPhysical`
/// descriptor.
///
/// Lives in `bqlite-planner` per `docs/design/operators/aggregate-operator.md`
/// §11.
#[derive(Debug, Clone, PartialEq)]
pub struct FusableAggregate {
    /// The aggregate expressions to compute inline with the match.
    pub aggregates: Vec<FusableAggExpr>,
    /// Group-by key expressions (expression, output name), in declaration order.
    pub group_by: Vec<(TypedExpr, String)>,
    /// Output schema of the fused aggregate node (group-by + aggregate cols).
    pub output_schema: OperatorSchema,
}

// ─────────────────────────────────────────────────────────────────────────────
// CompiledFusableAggregate
// ─────────────────────────────────────────────────────────────────────────────

/// A single aggregate expression in its compiled physical form.
#[derive(Debug, Clone, PartialEq)]
pub struct CompiledAggExpr {
    /// The resolved aggregate function.
    pub function: AggFunction,
    /// Compiled argument expression. `None` for `COUNT(*)`.
    pub arg: Option<CompiledExpr>,
    /// Output column name.
    pub output_name: String,
}

/// Compiled form of [`FusableAggregate`] carried on the physical descriptor.
///
/// Produced by TASK-318's physical lowering from `FusableAggregate`, or
/// directly by TASK-320's match-aggregate fusion pass.
/// Consumed by the `SequenceMatchOperator` (TASK-321) to update
/// accumulators inline via `finish_entity_into`.
#[derive(Debug, Clone, PartialEq)]
pub struct CompiledFusableAggregate {
    /// Compiled aggregate expressions.
    pub aggregates: Vec<CompiledAggExpr>,
    /// Compiled group-by key expressions (expression, output name).
    pub group_by: Vec<(CompiledExpr, String)>,
    /// Output schema of the fused aggregate.
    pub output_schema: OperatorSchema,
    /// Hard cap on group cardinality. Propagated from the originating
    /// `AggregatePhysical.max_groups`. Default: `DEFAULT_MAX_GROUPS`.
    ///
    /// The `SequenceMatchOperator` constructs a `HashAccumulator` with this
    /// cap, matching the behaviour of the non-fused `HashAggregateOperator`.
    pub max_groups: usize,
}

// ─────────────────────────────────────────────────────────────────────────────
// DemandSet
// ─────────────────────────────────────────────────────────────────────────────

/// The planner's "downstream needs" struct, propagated backward from root
/// toward scan during Pass 4 (demand propagation / projection pruning).
///
/// Each field represents a category of demand that downstream operators
/// impose on their inputs. The propagation algorithm in TASK-318 starts
/// with a full-demand set at the root and progressively narrows it as it
/// walks toward the scan.
///
/// See `docs/design/planner/wave3-lowering.md` §3.1 for the authoritative
/// field specification and §3.2 for the propagation algorithm.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct DemandSet {
    /// Flat column names the downstream needs to see.
    pub columns: HashSet<ColumnId>,

    /// Whether `match_events` and `match_duration` are needed by downstream.
    ///
    /// When `true`, the `SequenceMatch` operator must carry full per-match
    /// event lists and durations in its output, and the execution strategy
    /// is forced to `MatchStrategy::FullNfa` regardless of pattern class.
    pub needs_match_detail: bool,

    /// Whether `step_reached` is needed by downstream.
    ///
    /// `step_reached` is a synthetic column emitted only when `emit_all`
    /// is true. This flag tells the SequenceMatch operator to include it.
    pub needs_step_reached: bool,

    /// Named step properties needed (per step, per column).
    ///
    /// Resolved by the step-property resolution pass (wave3-lowering.md §3.3)
    /// when downstream expressions reference `step_name.column_name`.
    pub step_properties: Vec<StepPropertyRef>,

    /// Forwarded columns needed from SESSIONIZE / ATTRIBUTE (Wave 4).
    ///
    /// Populated by Wave 4 operators; always empty in Wave 3.
    pub forwarded: Vec<ColumnId>,

    /// Fused aggregate specification, if fusion is active.
    ///
    /// Set by TASK-320 (Pass 6, match-aggregate fusion) after demand
    /// resolves. `None` until the fusion optimizer detects a fusable
    /// SequenceMatch + Aggregate adjacency.
    pub fused_aggregate: Option<FusableAggregate>,

    /// Fused filter predicate, if fusion is active.
    ///
    /// Set by TASK-320 when a `Filter` sits between the `SequenceMatch`
    /// and the `Aggregate` in the FilterThenAggregate fusion pattern.
    /// See planner-pipeline.md §5.3 and wave3-lowering.md §3.1.
    pub fused_filter: Option<TypedExpr>,
}

impl DemandSet {
    /// Construct an empty demand set (no column requirements).
    ///
    /// Used as the starting point for child-side demand accumulation in
    /// the demand propagation algorithm.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Construct a full-demand set that requests all columns by name.
    ///
    /// Used at the root of the plan tree (no downstream to restrict demand).
    pub fn full(columns: impl IntoIterator<Item = ColumnId>) -> Self {
        Self {
            columns: columns.into_iter().collect(),
            ..Self::default()
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_demand_set_has_no_columns() {
        let d = DemandSet::empty();
        assert!(d.columns.is_empty());
        assert!(!d.needs_match_detail);
        assert!(!d.needs_step_reached);
        assert!(d.step_properties.is_empty());
        assert!(d.fused_aggregate.is_none());
        assert!(d.fused_filter.is_none());
    }

    #[test]
    fn full_demand_set_contains_requested_columns() {
        let d = DemandSet::full(["entity_id".to_string(), "ts".to_string()]);
        assert!(d.columns.contains("entity_id"));
        assert!(d.columns.contains("ts"));
        assert_eq!(d.columns.len(), 2);
    }

    #[test]
    fn step_property_ref_equality() {
        let a = StepPropertyRef {
            step_name: "s".to_string(),
            column_name: "plan".to_string(),
            bql_type: BqlType::String,
        };
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn demand_set_default_is_empty() {
        let d: DemandSet = Default::default();
        assert!(d.columns.is_empty());
    }
}
