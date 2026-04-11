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
//! - **limit**: per-entity event count enforcement
//!
//! ## Trait surface
//!
//! The v0 execution trait surface lives in [`operator`]. Every stateless
//! operator implements [`PhysicalOperator`]; stateful per-entity operators
//! implement [`EntityOperator`] and are wrapped by an
//! `EntityOperatorAdapter` (landing in a later wave). See
//! `docs/design/operators/operator-traits.md` for the frozen design.

pub mod operator;

pub use operator::{CancellationToken, EntityOperator, PhysicalOperator};
