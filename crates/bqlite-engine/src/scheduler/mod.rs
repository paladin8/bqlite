//! Engine-side morsel scheduler scaffold (TASK-523, Wave 5).
//!
//! Implements the data structures and worker-handoff protocol from
//! `docs/design/engine/morsel-scheduler.md` (TASK-506). The pieces in
//! this module are wired into [`crate::Engine`] in CP3; CP2 lands them
//! as a self-contained scaffold so the type contracts and queue
//! mechanics can be unit-tested in isolation.
//!
//! ## Subsystem layout
//!
//! - [`morsel`] — the [`Morsel`] descriptor and the [`MorselGenerator`]
//!   metadata-only producer (design §3). The v1 generator emits a
//!   single "whole-shard" morsel per shard; per-entity-range generation
//!   is deferred to a follow-on task per design §11.
//! - [`policy`] — [`MorselSizePolicy`] and the per-query
//!   [`MorselSizeState`] sticky-halving counter (design §3.4).
//! - [`queue`] — [`MorselQueue`], the lock-free MPMC fronted by a
//!   condvar pair for producer/consumer wakeups (design §4.1).
//! - [`accumulator`] — [`AccumulatorHandle`], the per-shard
//!   `Mutex<Box<dyn Accumulator>>` wrapper plus shard-done signaling
//!   (design §6.2 / §4.3).
//! - [`worker`] — [`WorkerContext`] and [`WorkerMorselGuard`] with the
//!   §4.3 RAII drop hook.
//!
//! ## What this scaffold does NOT do (deferred to CP3 and follow-ons)
//!
//! - Wire the scheduler into [`crate::Engine::query`] — that is CP3 of
//!   this task.
//! - Make individual physical operators (scan, filter, aggregate, …)
//!   take a `(shard_id, [entity_lo, entity_hi))` parameter so each
//!   morsel only sees one entity range. The v1 path runs the existing
//!   operator tree end-to-end inside one degenerate whole-query
//!   morsel; per-operator morsel awareness is the follow-on work
//!   covered by the task list in design §11.
//! - Surface the §8 metrics counters — owned by TASK-524.

pub mod accumulator;
pub mod engine_pool;
pub mod morsel;
pub mod policy;
pub mod queue;
pub mod shard_plan;
pub mod worker;

pub use accumulator::{AccumulatorHandle, ShardDoneSignal};
pub use engine_pool::{
    build_from_config, BuildError, MorselScheduler, PerShardOutcome, PerWorkerCtx,
};
pub use morsel::{EntityRange, Morsel, MorselGenerator, ShardSnapshot, WindowSegments};
pub use policy::{MorselSizePolicy, MorselSizeState};
pub use queue::{MorselQueue, MorselQueueDrained, MorselQueueFull};
pub use shard_plan::enumerate_shard_snapshots;
pub use worker::{WorkerContext, WorkerMorselGuard};
