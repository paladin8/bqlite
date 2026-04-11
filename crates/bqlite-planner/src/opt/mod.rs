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

pub mod prune;
pub mod pushdown;

pub use prune::prune_columns;
pub use pushdown::pushdown_predicates;
