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
