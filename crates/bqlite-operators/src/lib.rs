//! # bqlite-operators
//!
//! Physical operator implementations for bqlite.
//!
//! Each operator implements a common trait and processes entity event streams:
//! - **scan**: entity-partitioned scan with merge support across segments
//! - **filter**: predicate evaluation including regex matching
//! - **sequence**: general NFA-based temporal pattern matcher
//! - **funnel**: optimized funnel evaluator (stepwise conversion)
//! - **retention**: retention matrix computer over configurable intervals
//! - **sessionize**: session segmentation (inactivity gap + end events)
//! - **aggregate**: hash/sort aggregation (count, sum, avg, min, max, percentiles)
//! - **cohort**: behavioral cohort materializer
//! - **paths**: Sankey-style path aggregation
//! - **limit**: row-count cutoff with early child termination
//! - **sort**: in-memory pipeline sort (materializes all input, applies Arrow lexsort)
//! - **distinct**: streaming hash-set deduplication (first-occurrence rows only)
//!
//! ## Trait surface
//!
//! The v0 execution trait surface lives in [`operator`]. Every stateless
//! operator implements [`PhysicalOperator`]; stateful per-entity operators
//! implement [`EntityOperator`] and are wrapped by an
//! `EntityOperatorAdapter` (landing in a later wave). See
//! `docs/design/operators/operator-traits.md` for the frozen design.
//!
//! ## Wave 1 operator stubs
//!
//! The Wave 1 stubs shipped by TASK-117 live in sibling modules:
//!
//! - [`scan`] — pull-based reader that drives a
//!   [`bqlite_core::SegmentReader`] to completion, one row-group at a
//!   time.
//! - [`filter`] — wrapper around a child operator; the Wave 1 stub
//!   forwards every row unchanged.
//! - [`project`] — wrapper around a child operator; the Wave 1 stub
//!   forwards every row unchanged.
//!
//! These give the planner (TASK-115) and engine bind step (TASK-118)
//! real `Box<dyn PhysicalOperator>` implementors to materialize from
//! the plain-data physical descriptor. Real filtering, projection
//! pruning, and k-way merge land in later waves.
//!
//! ## Wave 2 operators
//!
//! TASK-231 lands the real stateless surface alongside the Wave 1
//! stubs. The first addition is the [`limit`] operator, which is
//! purely additive (no Wave 1 stub existed). TASK-231's follow-up
//! checkpoints replace [`filter`] and [`project`] with real
//! implementations driven by compiled expressions from
//! [`bqlite_planner::compiled::CompiledExpr`].

pub mod aggregate;
pub mod distinct;
pub mod encoded_filter;
pub mod eval;
pub mod filter;
pub mod filtered_batch;
pub mod limit;
pub mod matcher;
pub mod operator;
pub mod project;
pub mod scan;
pub mod selection;
pub mod sort;

pub use aggregate::{
    Accumulator, AggState, GroupKey, HashAccumulator, HashAggregateOperator, SumState,
    DEFAULT_MAX_GROUPS,
};
pub use distinct::DistinctOperator;
pub use encoded_filter::{ConstantEqKernel, EncodedPredicateKernel};
pub use filter::FilterOperator;
pub use filtered_batch::FilteredBatch;
pub use limit::LimitOperator;
pub use matcher::SequenceMatchOperator;
pub use operator::{
    CancellationToken, EntityOperator, PhysicalOperator, DEFAULT_OUTPUT_BATCH_SIZE,
};
pub use project::ProjectOperator;
pub use scan::{ScanOperator, ScanPath};
pub use selection::{is_dense, selection_as_vector, selection_to_bool_array};
pub use sort::SortOperator;
