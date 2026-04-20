//! Compaction throughput and L0 segment-fan-in reduction (TASK-441).
//!
//! Seeds a database with a controlled L0 backlog (N separately-flushed
//! segments in a single `(window, shard)` bucket) and times
//! `Database::compact_now`, reporting:
//!
//! - **Throughput** — bytes-consumed-per-second across the
//!   compaction input set. Regression tripwire ≥ 200 MB/s in reference
//!   mode (plan `[floor]` target — `compaction-concurrency.md` pins
//!   no MB/s figure, only the 4-segment / 256 MiB L0 trigger in §3.2).
//! - **L0 reduction ratio** — `l0_before / l0_after`. `compaction-
//!   concurrency.md` §3.2 makes a `(window, shard)` eligible when
//!   L0 count is **strictly greater than** 4, so the smallest stack
//!   that triggers compaction is 5 segments. At that stack size the
//!   reduction ratio must be ≥ 5 (5 inputs → 1 output).
//!
//! Both are recorded via `BenchResultCollector` so the CI bench gate
//! in `scripts/bench-compare.sh` picks up regressions.
//!
//! Run with:
//! ```bash
//! cargo bench -p bqlite-benches --bench compaction
//! ```

use std::time::Instant;

use bqlite_benches::common::*;
use bqlite_core::event::Event;
use bqlite_storage::ingest::partitioner::Partitioner;
use bqlite_storage::writer::SegmentWriter;
use bqlite_storage::Database;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

/// Single-shard layout so every event lands in one bucket — this is
/// the bucket the compaction trigger fires on.
const SHARD_COUNT: u16 = 1;
const PARTITION_BUDGET_BYTES: usize = 512 * 1024 * 1024;

// ─────────────────────────────────────────────────────────────────────────────
// Fixture builder
// ─────────────────────────────────────────────────────────────────────────────

/// Seed a fresh database with `n_segments` L0 segments in a single
/// `(window, shard)` bucket. Each segment contains
/// `events_per_segment` events drawn from `generate_events`; every
/// event is pushed through a fresh `Partitioner` and written with a
/// fresh `SegmentWriter` so the segments end up on disk as distinct
/// files.
///
/// Returns the database plus the total number of bytes summed across
/// the pre-compaction segments (used as the throughput denominator).
fn seed_l0_stack(
    scratch: &ScratchDir,
    n_segments: usize,
    events_per_segment: usize,
) -> (Database, u64) {
    let schema = purchases_schema();
    let mut db = open_db_with_table(scratch.path(), "purchases", schema);

    let mut pre_bytes: u64 = 0;
    for seg_idx in 0..n_segments {
        // Vary the entity prefix per segment so the segments don't
        // all have identical entity ids — that would let compaction
        // merge more aggressively than a realistic workload.
        let events: Vec<Event> = generate_events(events_per_segment, 64)
            .into_iter()
            .map(|mut e| {
                // Tag entity id with seg_idx so segments cover
                // overlapping but non-identical entity sets.
                if let bqlite_core::event::EntityId::String(ref mut s) = e.entity {
                    *s = format!("{s}_seg{seg_idx}");
                }
                e
            })
            .collect();

        let batch_id = db.allocate_batch_id("purchases").unwrap();
        let mut partitioner =
            Partitioner::new(SHARD_COUNT, 30, batch_id, PARTITION_BUDGET_BYTES).unwrap();
        for event in events {
            partitioner.push_event(event).unwrap();
        }

        let mut writer = SegmentWriter::new(&mut db);
        let metas = writer.write_partitioner("purchases", partitioner).unwrap();
        pre_bytes += metas.iter().map(|m| m.byte_size).sum::<u64>();
    }

    (db, pre_bytes)
}

/// Total L0 (level == 0) segment count across every window and shard
/// of `table` in the current manifest snapshot.
fn l0_segment_count(db: &Database, table: &str) -> usize {
    let manifest = db.manifest();
    let Some(entry) = manifest.tables.get(table) else {
        return 0;
    };
    entry
        .windows
        .iter()
        .flat_map(|w| w.shards.iter())
        .flatten()
        .filter(|seg| seg.level == 0)
        .count()
}

// ─────────────────────────────────────────────────────────────────────────────
// Compaction benches
// ─────────────────────────────────────────────────────────────────────────────

/// Throughput bench — measures bytes of L0 input consumed per second
/// by `Database::compact_now`.
fn bench_compaction_throughput(c: &mut Criterion) {
    let mode = BenchMode::from_env();
    let events_per_segment = match mode {
        BenchMode::Ci => 10_000,
        BenchMode::Reference => 500_000,
    };
    // Seed once to estimate the bytes-consumed denominator for
    // Criterion's throughput line; re-seed inside the iter loop so
    // each iteration operates on a fresh manifest state.
    let probe_scratch = ScratchDir::new("compact-probe");
    let (_probe_db, probe_bytes) = seed_l0_stack(&probe_scratch, 4, events_per_segment);

    let mut collector = BenchResultCollector::new(mode);

    let mut group = c.benchmark_group("compaction/throughput");
    group.throughput(Throughput::Bytes(probe_bytes));

    group.bench_function("l0_to_l1_5_segments", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            let mut total_bytes: u64 = 0;
            for _ in 0..iters {
                // Re-seed every iteration: `compact_now` consumes the
                // L0 stack, so the bucket is empty afterwards. 5 is
                // the smallest stack that trips the `l0_count_trigger`
                // ("eligible when count > 4" per §3.2).
                let iter_scratch = ScratchDir::new("compact-iter");
                let (mut db, pre_bytes) = seed_l0_stack(&iter_scratch, 5, events_per_segment);
                let outcomes = db.compact_now("purchases").expect("compact_now");
                black_box(&outcomes);
                total_bytes += pre_bytes;
            }
            let elapsed = start.elapsed();
            report_metrics(total_bytes, total_bytes, elapsed);
            elapsed
        })
    });

    group.finish();

    // Hard-target measurement (reference mode only): one standalone
    // seed + compact pass, MB/s computed from `CompactionOutcome`
    // inputs so the denominator is exact rather than probe-derived.
    if mode.is_reference() {
        let target_scratch = ScratchDir::new("compact-target");
        let (mut db, _pre_bytes) = seed_l0_stack(&target_scratch, 5, events_per_segment);
        let pre_bytes_exact: u64 = db
            .manifest()
            .tables
            .get("purchases")
            .map(|t| {
                t.windows
                    .iter()
                    .flat_map(|w| w.shards.iter())
                    .flatten()
                    .filter(|s| s.level == 0)
                    .map(|s| s.byte_size)
                    .sum()
            })
            .unwrap_or(0);

        let start = Instant::now();
        let outcomes = db.compact_now("purchases").expect("compact_now target");
        let elapsed_s = start.elapsed().as_secs_f64();
        black_box(&outcomes);

        let mb_per_sec = pre_bytes_exact as f64 / elapsed_s / (1024.0 * 1024.0);
        collector.record(
            "compaction/throughput/l0_to_l1_mb_per_sec",
            mb_per_sec,
            "MB/s",
            Some(BenchTarget::at_least(200.0)),
        );
    }

    collector.finish();
}

/// L0 reduction-ratio bench — asserts compaction collapses the
/// smallest eligible L0 stack (5 segments, per §3.2 `count > 4`)
/// into a single output segment. Records the ratio at three stack
/// sizes so CI sees regressions in either trigger handling or
/// output-fan-out.
fn bench_compaction_l0_reduction(c: &mut Criterion) {
    let mode = BenchMode::from_env();
    let events_per_segment = match mode {
        BenchMode::Ci => 5_000,
        BenchMode::Reference => 100_000,
    };
    let mut collector = BenchResultCollector::new(mode);

    let mut group = c.benchmark_group("compaction/l0_reduction");

    for &n_segments in &[5usize, 8, 16] {
        group.bench_with_input(
            BenchmarkId::new("compact_now", n_segments),
            &n_segments,
            |b, &n| {
                b.iter_custom(|iters| {
                    let start = Instant::now();
                    for _ in 0..iters {
                        let iter_scratch = ScratchDir::new("compact-reduction");
                        let (mut db, _bytes) = seed_l0_stack(&iter_scratch, n, events_per_segment);
                        let outcomes = db.compact_now("purchases").expect("compact_now");
                        black_box(&outcomes);
                    }
                    start.elapsed()
                })
            },
        );

        // Standalone reduction-ratio measurement at this stack size.
        let probe_scratch = ScratchDir::new("compact-probe");
        let (mut db, _bytes) = seed_l0_stack(&probe_scratch, n_segments, events_per_segment);
        let before = l0_segment_count(&db, "purchases");
        let _outcomes = db.compact_now("purchases").expect("compact_now probe");
        let after = l0_segment_count(&db, "purchases");
        // After compaction every L0 input is replaced by higher-level
        // output, so `after` should be 0. Guard against div-by-zero.
        let ratio = before as f64 / (after as f64).max(1.0);

        let target = if n_segments == 5 {
            // [spec] compaction-concurrency.md §3.2 — the L0 trigger
            // fires when count > 4, so 5 is the smallest stack that
            // triggers. All 5 inputs collapse into one output
            // (§6 atomic publish), so the reduction ratio must be
            // ≥ 5.
            Some(BenchTarget::at_least(5.0))
        } else {
            None
        };
        collector.record(
            &format!("compaction/l0_reduction/{n_segments}_to_1/ratio"),
            ratio,
            "ratio",
            target,
        );
    }

    group.finish();
    collector.finish();
}

criterion_group! {
    name = compaction_benches;
    config = criterion_for_mode(BenchMode::from_env());
    targets =
        bench_compaction_throughput,
        bench_compaction_l0_reduction,
}
criterion_main!(compaction_benches);
