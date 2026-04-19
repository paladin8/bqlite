//! Ingest pipeline — converts row-oriented events into sorted,
//! per-`(window, shard)` streams ready for the segment writer.
//!
//! # Crate position
//!
//! `bqlite-storage::ingest` sits between higher-level input readers
//! (CSV in TASK-233, INSERT VALUES in TASK-238) and the segment
//! writer (TASK-214). Its only output is a collection of
//! `(window_id, shard_id)` buckets, each containing events sorted by
//! `(entity_id, timestamp)` — the exact invariant storage-format.md
//! §7.2 asks the writer to preserve inside each row group.
//!
//! # Wave 2 scope
//!
//! [`partitioner`] — ingest partitioner (TASK-218). Hashes entities
//! into shards, derives window ids from event timestamps, buffers
//! each bucket in memory, and errors loudly when the buffer exceeds
//! a configurable budget. Real spill-to-disk is a Wave 5 concern.
//!
//! Downstream Wave 2 tasks layer on top without reshaping this
//! module: TASK-214 drains the partitioner into the writer, and
//! TASK-233 wires a streaming CSV reader into the partitioner's
//! `push_event` loop.

pub mod csv_reader;
pub mod json;
pub mod parquet;
pub mod partitioner;
