//! Wave 1 smoke benchmark.
//!
//! Runs under `cargo bench -p bqlite-benches --bench smoke` and exists
//! purely to exercise the Criterion harness end-to-end — it proves that
//! benches compile, that `bqlite_benches::common` is importable, and
//! that CI can run a bench without erroring.
//!
//! As real operators land the smoke bench is *kept* alongside them as
//! a cheap canary: if it ever fails to compile or regresses in wall
//! time, the harness (not any real operator) is broken.

use bqlite_benches::common::identity;
use criterion::{black_box, criterion_group, criterion_main, Criterion};

/// Measure a single `identity(u64) -> u64` call through `black_box` so
/// the optimizer cannot fold the loop body away. This is the cheapest
/// honest measurement the harness can perform.
fn smoke_noop_identity(c: &mut Criterion) {
    c.bench_function("smoke/noop_identity_u64", |b| {
        b.iter(|| identity(black_box(42u64)))
    });
}

criterion_group!(smoke, smoke_noop_identity);
criterion_main!(smoke);
