//! # bqlite-operators
//!
//! Physical operator implementations for bqlite.
//!
//! Each operator implements a common trait and processes entity event streams:
//! - **scan**: entity-partitioned scan with merge support across segments
//! - **fused_segment + kernel**: stateless filter / project / limit chain
//!   driven by `FusedStatelessSegment` over `StatelessKernel`s. The
//!   legacy stand-alone `FilterOperator` / `ProjectOperator` /
//!   `LimitOperator` types were retired in TASK-519; lowering now
//!   emits a single `FusedSegmentPhysical` per stateless run per
//!   `docs/design/engine/operator-fusion.md` §6.4.
//! - **matcher**: general NFA-based temporal pattern matcher
//! - **sessionize**: session segmentation (inactivity gap + end events)
//! - **event_select**: per-entity FIRST/LAST/NTH event selection
//! - **attribute**: multi-touch attribution
//! - **aggregate**: hash/sort aggregation (count, sum, avg, min, max, percentiles)
//! - **cohort**: behavioral cohort materializer
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
//! ## Stateless segment
//!
//! Stateless work (filter, project, limit) is implemented as a chain
//! of [`StatelessKernel`]s ([`FilterKernel`], [`ProjectKernel`], plus
//! the in-driver `LIMIT` step) driven by [`FusedStatelessSegment`].
//! See `docs/design/engine/operator-fusion.md` for the full
//! contract.

pub mod aggregate;
pub mod attribute;
pub mod cohort;
pub mod distinct;
pub mod encoded_filter;
pub mod eval;
pub mod event_select;
pub mod filtered_batch;
pub mod fused_segment;
pub mod kernel;
pub mod matcher;
pub mod materialize;
pub mod operator;
pub mod scan;
pub mod selection;
pub mod sessionize;
pub mod sort;
pub mod string_column;

pub use aggregate::{
    Accumulator, AggState, GroupKey, HashAccumulator, HashAggregateOperator, SumState,
    DEFAULT_MAX_GROUPS,
};
pub use attribute::{
    AttributeOperator, AttributeState, EntityCapDiagnostic, ATTRIBUTE_OP_NAME,
    DEFAULT_ATTRIBUTE_DEQUE_CAP,
};
pub use cohort::{CohortHashSet, CohortKey, SubqueryFilterOperator};
pub use distinct::DistinctOperator;
pub use encoded_filter::{
    apply_encoded_eq, apply_materialized_mask, partition_encoded_eq, recognize_encoded_eq,
    ConstantEqKernel, DictionaryEqKernel, EncodedEqShape, EncodedPredicateKernel, RleIntEqKernel,
};
pub use event_select::EventSelectOperator;
pub use filtered_batch::FilteredBatch;
pub use fused_segment::{FusedStatelessSegment, KernelStep, SPARSITY_FACTOR_DEFAULT};
pub use kernel::{FilterKernel, ProjectKernel, ProjectionExpr, StatelessKernel};
pub use matcher::SequenceMatchOperator;
pub use materialize::{
    materialize_filtered_batch, materialize_selected, materialize_selected_dict_pushthrough,
    materialize_selected_with_metrics, materialize_stitched, materialize_stitched_dict_pushthrough,
};
pub use operator::{
    CancellationToken, EntityOperator, PhysicalOperator, DEFAULT_OUTPUT_BATCH_SIZE,
};
pub use scan::{ScanOperator, ScanPath};
pub use selection::{is_dense, selection_as_vector, selection_to_bool_array};
pub use sessionize::{SessionizeOperator, SessionizeState, DEFAULT_SESSION_EVENT_CAP};
pub use sort::SortOperator;
