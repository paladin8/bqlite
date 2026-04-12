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

pub mod desugar_funnel;
pub mod prune;
pub mod pushdown;

pub use desugar_funnel::desugar_funnel;
pub use prune::prune_columns;
pub use pushdown::pushdown_predicates;
