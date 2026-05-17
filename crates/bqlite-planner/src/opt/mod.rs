//! Optimizer passes for the physical plan.
//!
//! Each submodule implements one pass. Passes are independent and
//! compose by applying them in order after [`crate::physical::lower_physical`].
//!
//! ## Wave 2 passes
//!
//! | Pass | Module | Task |
//! |------|--------|------|
//! | Predicate pushdown | [`pushdown`] | TASK-227 |
//! | Projection pruning | [`prune`] | TASK-228 |
//!
//! ## Wave 3 lowering helpers
//!
//! These modules assist the lowering phase (not the post-`lower_physical`
//! optimizer passes). They operate on AST nodes during `logical::fold_stage`,
//! before any `LogicalPlan` node is constructed.
//!
//! | Helper | Module | Task |
//! |--------|--------|------|
//! | FUNNEL desugaring | [`mod@desugar_funnel`] | TASK-319 |
//!
//! ## Wave 3 passes
//!
//! | Pass | Module | Task |
//! |------|--------|------|
//! | Match-aggregate fusion | [`mod@fuse_match_aggregate`] | TASK-320 |
//!
//! ## Wave 4 lowering helpers
//!
//! | Helper | Module | Task |
//! |--------|--------|------|
//! | RETENTION desugaring | [`mod@desugar_retention`] | TASK-426 |
//!
//! ## Wave 4 passes
//!
//! | Pass | Module | Task |
//! |------|--------|------|
//! | Sample pushdown | [`sample_pushdown`] | TASK-430 |
//!
//! ## Wave 5 framework
//!
//! | Module | Purpose | Task |
//! |--------|---------|------|
//! | [`registry`] | `OptimizerRule` trait, `OptimizerPipeline` driver, `RuleTrace` for EXPLAIN | TASK-521 |
//! | [`rules`] | Pass 6.5 / Pass 7 stub rules (TASK-527 fills in the bodies) | TASK-521 / TASK-527 |
//! | [`filter_order`] | Pass 9 — stateless filter conjunct ordering inside fused segment | TASK-527 |
//! | [`mod@coalesce_scan_predicates`] | Pass 10 — scan-pushdown filter coalescing | TASK-527 |
//!
//! The Wave 0 / Wave 2 / Wave 3 / Wave 4 passes above are wrapped as
//! [`registry::OptimizerRule`] implementations in
//! [`registry`] and registered into the canonical pipeline via
//! [`OptimizerPipeline::v1`](registry::OptimizerPipeline::v1). The
//! pipeline driver runs them in registration order and records the
//! per-rule outcome into [`registry::RuleTrace`] for
//! EXPLAIN visibility per `optimizer-direction.md` §8.2.

pub mod coalesce_scan_predicates;
pub mod desugar_funnel;
pub mod desugar_retention;
pub mod filter_order;
pub mod fuse_match_aggregate;
pub mod prune;
pub mod pushdown;
pub mod registry;
pub mod remap;
pub mod rules;
pub mod sample_pushdown;

pub use coalesce_scan_predicates::coalesce_scan_predicates;
pub use desugar_funnel::desugar_funnel;
pub use desugar_retention::desugar_retention;
pub use filter_order::order_stateless_filters;
pub use fuse_match_aggregate::fuse_match_aggregate;
pub use prune::prune_columns;
pub use pushdown::pushdown_predicates;
pub use registry::{
    ColumnIndexRemapRule, FuseMatchAggregateRule, OptimizerPipeline, OptimizerRule,
    PredicatePushdownRule, ProjectionPruningRule, RuleContext, RulePhase, RuleTrace,
    RuleTraceEntry, RuleTraceOutcome, SamplePushdownRule, ScanPredicateCoalesceRule,
    StatelessFilterOrderingRule,
};
pub use remap::remap_column_indices;
pub use rules::{MatchAnchorPresenceRule, Tier3PredicateShapeRule};
pub use sample_pushdown::pushdown_sample;
