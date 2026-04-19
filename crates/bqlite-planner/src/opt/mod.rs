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
//! | FUNNEL desugaring | [`desugar_funnel`] | TASK-319 |
//!
//! ## Wave 3 passes
//!
//! | Pass | Module | Task |
//! |------|--------|------|
//! | Match-aggregate fusion | [`fuse_match_aggregate`] | TASK-320 |
//!
//! ## Wave 4 lowering helpers
//!
//! | Helper | Module | Task |
//! |--------|--------|------|
//! | RETENTION desugaring | [`desugar_retention`] | TASK-426 |
//!
//! ## Wave 4 passes
//!
//! | Pass | Module | Task |
//! |------|--------|------|
//! | Sample pushdown | [`sample_pushdown`] | TASK-430 |

pub mod desugar_funnel;
pub mod desugar_retention;
pub mod fuse_match_aggregate;
pub mod prune;
pub mod pushdown;
pub mod sample_pushdown;

pub use desugar_funnel::desugar_funnel;
pub use desugar_retention::desugar_retention;
pub use fuse_match_aggregate::fuse_match_aggregate;
pub use prune::prune_columns;
pub use pushdown::pushdown_predicates;
pub use sample_pushdown::pushdown_sample;
