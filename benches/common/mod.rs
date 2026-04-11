//! Shared helpers for bqlite Criterion benchmarks.
//!
//! Every bench file under `benches/benches/` imports from here via
//! `use bqlite_benches::common::*`. This module is deliberately small
//! in Wave 1 — it just needs to prove the harness compiles and runs in
//! CI. Later waves extend it with:
//!
//! - Dataset generators (random events, monotonic-within-entity
//!   timestamps, low-cardinality string columns).
//! - The reference `purchases` fixture from the Wave 2 performance gate
//!   in `TASKS.md` §"Wave 2".
//! - Cold-cache harness helpers (page-cache eviction on Linux, pagers
//!   on macOS) for the cold-cache measurements the Wave 2 performance
//!   gate requires.
//! - Criterion configuration helpers that apply the workspace-standard
//!   sample size, warm-up time, and measurement time so individual
//!   benches only spell out what is actually bench-specific.
//!
//! Wave 1 keeps the surface to a single no-op identity helper so the
//! smoke bench has *something* real to measure through `black_box`
//! without accidentally measuring unrelated machinery. The helper is
//! marked `#[inline(never)]` so the optimizer cannot collapse the call
//! site and reduce the bench to a no-op across Criterion sample points.

/// Return `x` unchanged, through a function call the optimizer is not
/// permitted to inline.
///
/// Wave 1's smoke bench measures
/// `identity(black_box(value))` in a tight loop. The `#[inline(never)]`
/// attribute ensures the function boundary survives optimization so
/// Criterion measures a stable call-and-return overhead rather than a
/// fully elided loop.
#[inline(never)]
pub fn identity<T>(x: T) -> T {
    x
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_returns_input() {
        assert_eq!(identity(42u64), 42u64);
        assert_eq!(identity("hello"), "hello");
    }
}
