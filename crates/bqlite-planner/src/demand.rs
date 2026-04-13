//! Demand propagation types (TASK-317 / TASK-309 / TASK-409 / TASK-427).
//!
//! This module is the single home for both sides of the demand protocol:
//!
//! - **`DemandSet`** — the planner's "downstream needs" struct that flows
//!   backward through the plan tree (root → scan) to determine which
//!   columns, match details, step properties, and aggregate fusions each
//!   operator must produce.
//! - **`DemandCapabilities`** — the operator-side capability advertisement
//!   struct. Each bool field advertises a single, orthogonal capability that
//!   a stateful operator may or may not support.
//! - **`DemandPropagation`** — object-safe trait for querying an operator's
//!   demand capabilities at plan time.
//!
//! See `docs/design/planner/wave3-lowering.md` §3.1 for `DemandSet` field
//! specification, and `docs/design/planner/demand-protocol.md` for the full
//! `DemandCapabilities` protocol.
//!
//! ## Crate placement
//!
//! All demand types live in `bqlite-planner::demand` per
//! `docs/design/planner/demand-protocol.md` §4. `bqlite-operators` depends
//! on `bqlite-planner`, so operators import these types directly.

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
// DemandCapabilities
// ─────────────────────────────────────────────────────────────────────────────

/// Operator-side capability advertisement for demand-driven output shaping.
///
/// The planner constructs a [`DemandSet`] during its backward pass (root → scan),
/// then matches it against each operator's `DemandCapabilities` during physical
/// planning to select a strategy and validate that every demand can be satisfied.
///
/// This is the dual of [`DemandSet`]: `DemandSet` says "what the downstream
/// needs"; `DemandCapabilities` says "what this operator can provide."
///
/// See `docs/design/planner/demand-protocol.md` §2–§3 for the authoritative
/// field specification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DemandCapabilities {
    /// Emits the `step_reached` synthetic column on `emit_all` MATCH paths.
    pub supports_step_reached: bool,
    /// Count-only strategy (step-counter / dedicated-consecutive, no path tracking).
    pub supports_match_count: bool,
    /// FullNFA strategy — emits `match_events` + `match_duration`.
    pub supports_full_detail: bool,
    /// `finish_entity_into` hot path; operator absorbs downstream `FusableAggregate`.
    pub supports_aggregation_fusion: bool,
    /// Per-(step, column) retention for `s.plan`-style references.
    pub supports_step_property_forwarding: bool,
    /// Generic per-column forwarding from stateful ops (Sessionize, Attribute,
    /// EventSelect).
    pub supports_forwarded_columns: bool,
    /// Reserved for Wave 5: advertises eager per-group emission mid-stream.
    pub supports_eager_group_emit: bool,
}

impl DemandCapabilities {
    /// All capabilities disabled. Use for stateless operators that implement
    /// `DemandPropagation` purely for uniformity.
    pub const fn none() -> Self {
        DemandCapabilities {
            supports_step_reached: false,
            supports_match_count: false,
            supports_full_detail: false,
            supports_aggregation_fusion: false,
            supports_step_property_forwarding: false,
            supports_forwarded_columns: false,
            supports_eager_group_emit: false,
        }
    }
}

impl Default for DemandCapabilities {
    fn default() -> Self {
        Self::none()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// DemandPropagation
// ─────────────────────────────────────────────────────────────────────────────

/// Object-safe trait for querying an operator's demand capabilities.
///
/// Implementable by any operator — stateful or stateless. Stateless
/// operators that don't override any capability bit can use the default
/// implementation (returns [`DemandCapabilities::default()`], all `false`).
///
/// The planner's physical planning pass uses `&dyn DemandPropagation`
/// to reason about demand at plan time without depending on
/// `bqlite-operators`.
///
/// See `docs/design/planner/demand-protocol.md` §5 for the dual-surface
/// design rationale.
pub trait DemandPropagation {
    /// Advertise the demand capabilities this operator supports.
    fn supported_demands(&self) -> DemandCapabilities {
        DemandCapabilities::default()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Demand matching
// ─────────────────────────────────────────────────────────────────────────────

/// Check that an operator's capabilities satisfy the downstream demand.
///
/// Returns `Ok(())` when every demanded shape is covered by a matching
/// capability bit. Returns `Err(BqliteError::Plan)` listing the missing
/// capability names when the operator cannot satisfy the demand.
///
/// `supports_match_count` and `supports_eager_group_emit` are not
/// demand-checked — they influence strategy selection, not demand
/// satisfaction. See `docs/design/planner/demand-protocol.md` §8.
pub fn check_demand_satisfied(
    demand: &DemandSet,
    caps: &DemandCapabilities,
    node_label: &str,
) -> bqlite_core::Result<()> {
    let mut missing = Vec::new();

    if demand.needs_step_reached && !caps.supports_step_reached {
        missing.push("supports_step_reached");
    }
    if demand.needs_match_detail && !caps.supports_full_detail {
        missing.push("supports_full_detail");
    }
    if demand.fused_aggregate.is_some() && !caps.supports_aggregation_fusion {
        missing.push("supports_aggregation_fusion");
    }
    if !demand.step_properties.is_empty() && !caps.supports_step_property_forwarding {
        missing.push("supports_step_property_forwarding");
    }
    if !demand.forwarded.is_empty() && !caps.supports_forwarded_columns {
        missing.push("supports_forwarded_columns");
    }

    if missing.is_empty() {
        Ok(())
    } else {
        Err(bqlite_core::BqliteError::Plan(format!(
            "unsupported demand on {node_label}: missing capabilities: {}",
            missing.join(", "),
        )))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── DemandSet ───────────────────────────────────────────────────────

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

    // ── DemandCapabilities ──────────────────────────────────────────────

    #[test]
    fn default_is_all_false() {
        let caps = DemandCapabilities::default();
        assert!(!caps.supports_step_reached);
        assert!(!caps.supports_match_count);
        assert!(!caps.supports_full_detail);
        assert!(!caps.supports_aggregation_fusion);
        assert!(!caps.supports_step_property_forwarding);
        assert!(!caps.supports_forwarded_columns);
        assert!(!caps.supports_eager_group_emit);
    }

    #[test]
    fn const_none_matches_default() {
        assert_eq!(DemandCapabilities::none(), DemandCapabilities::default());
    }

    #[test]
    fn none_usable_as_const_item() {
        const NONE: DemandCapabilities = DemandCapabilities::none();
        assert_eq!(NONE, DemandCapabilities::default());
    }

    #[test]
    fn is_copy_and_clone() {
        fn assert_copy<T: Copy + Clone>() {}
        assert_copy::<DemandCapabilities>();
    }

    #[test]
    fn is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<DemandCapabilities>();
    }

    #[test]
    fn equality_and_hash_are_consistent() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(DemandCapabilities::none());
        set.insert(DemandCapabilities::none());
        assert_eq!(set.len(), 1, "duplicate inserts must collapse");
        assert!(set.contains(&DemandCapabilities::none()));
    }

    #[test]
    fn struct_literal_with_some_true() {
        let caps = DemandCapabilities {
            supports_forwarded_columns: true,
            ..DemandCapabilities::none()
        };
        assert!(caps.supports_forwarded_columns);
        assert!(!caps.supports_step_reached);
    }

    // ── DemandPropagation ───────────────────────────────────────────────

    struct DefaultingOperator;
    impl DemandPropagation for DefaultingOperator {}

    #[test]
    fn propagation_default_returns_none() {
        let op = DefaultingOperator;
        assert_eq!(op.supported_demands(), DemandCapabilities::none());
    }

    struct OverridingOperator;
    impl DemandPropagation for OverridingOperator {
        fn supported_demands(&self) -> DemandCapabilities {
            DemandCapabilities {
                supports_forwarded_columns: true,
                ..DemandCapabilities::none()
            }
        }
    }

    #[test]
    fn propagation_override_is_observed() {
        let op = OverridingOperator;
        assert!(op.supported_demands().supports_forwarded_columns);
    }

    #[test]
    fn propagation_is_object_safe() {
        let op: Box<dyn DemandPropagation> = Box::new(DefaultingOperator);
        assert_eq!(op.supported_demands(), DemandCapabilities::none());
    }

    // ── check_demand_satisfied ──────────────────────────────────────────

    #[test]
    fn satisfied_when_no_demand() {
        let demand = DemandSet::empty();
        let caps = DemandCapabilities::none();
        assert!(check_demand_satisfied(&demand, &caps, "test").is_ok());
    }

    #[test]
    fn satisfied_when_caps_cover_demand() {
        let demand = DemandSet {
            needs_step_reached: true,
            needs_match_detail: true,
            step_properties: vec![StepPropertyRef {
                step_name: "s".into(),
                column_name: "plan".into(),
                bql_type: BqlType::String,
            }],
            ..DemandSet::empty()
        };
        let caps = DemandCapabilities {
            supports_step_reached: true,
            supports_full_detail: true,
            supports_step_property_forwarding: true,
            ..DemandCapabilities::none()
        };
        assert!(check_demand_satisfied(&demand, &caps, "test").is_ok());
    }

    #[test]
    fn unsatisfied_reports_missing_capabilities() {
        let demand = DemandSet {
            needs_step_reached: true,
            needs_match_detail: true,
            ..DemandSet::empty()
        };
        let caps = DemandCapabilities::none();
        let err = check_demand_satisfied(&demand, &caps, "SequenceMatch").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("supports_step_reached"), "got: {msg}");
        assert!(msg.contains("supports_full_detail"), "got: {msg}");
        assert!(msg.contains("SequenceMatch"), "got: {msg}");
    }

    #[test]
    fn match_count_and_eager_emit_not_demand_checked() {
        // These bits influence strategy selection, not demand satisfaction.
        let demand = DemandSet::empty();
        let caps = DemandCapabilities::none();
        // Even with a demand set that doesn't need match detail, the
        // absence of match_count and eager_group_emit doesn't cause failure.
        assert!(check_demand_satisfied(&demand, &caps, "test").is_ok());
    }

    #[test]
    fn forwarded_columns_demand_requires_capability() {
        let demand = DemandSet {
            forwarded: vec!["session_id".into()],
            ..DemandSet::empty()
        };
        let caps = DemandCapabilities::none();
        assert!(check_demand_satisfied(&demand, &caps, "Sessionize").is_err());

        let caps_with = DemandCapabilities {
            supports_forwarded_columns: true,
            ..DemandCapabilities::none()
        };
        assert!(check_demand_satisfied(&demand, &caps_with, "Sessionize").is_ok());
    }

    #[test]
    fn fused_aggregate_demand_requires_capability() {
        let demand = DemandSet {
            fused_aggregate: Some(FusableAggregate {
                aggregates: vec![],
                group_by: vec![],
                output_schema: bqlite_core::OperatorSchema::new(vec![]).unwrap(),
            }),
            ..DemandSet::empty()
        };
        let caps = DemandCapabilities::none();
        assert!(check_demand_satisfied(&demand, &caps, "test").is_err());

        let caps_with = DemandCapabilities {
            supports_aggregation_fusion: true,
            ..DemandCapabilities::none()
        };
        assert!(check_demand_satisfied(&demand, &caps_with, "test").is_ok());
    }
}
