//! `AttributeOperator` throughput benchmarks (TASK-431, §17.1).
//!
//! Measures the operator under the five workload shapes called out by
//! `docs/design/operators/attribute.md` §17.1:
//!
//! - **single_entity_many_touchpoints** — deque-walk cost under a
//!   large per-entity touchpoint history with periodic conversions.
//! - **many_entities_sparse** — per-entity setup/teardown overhead
//!   when most entities have just a handful of events.
//! - **high_fan_out** — batch-builder cost when a single conversion
//!   emits many rows.
//! - **left_unnest_dominant** — LEFT-UNNEST fast path when nothing
//!   qualifies.
//! - **multi_type_attribution** — event-type dispatch cost when the
//!   conversion and touchpoint lists have multiple entries.
//!
//! Run with:
//! ```bash
//! cargo bench -p bqlite-benches --bench attribute
//! ```
//!
//! The CI mode scales these workloads down so the runner finishes in a
//! few seconds; reference mode (see `benches/common/mod.rs` for
//! `BQLITE_BENCH_MODE=reference`) runs the full workloads called out
//! by §17.1.

use std::sync::Arc;

use arrow::array::{StringViewArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use arrow::record_batch::RecordBatch;

use bqlite_ast::Span;
use bqlite_benches::common::{criterion_for_mode, BenchMode};
use bqlite_core::{BqlType, ColumnDef, EntityId, OperatorSchema};
use bqlite_operators::attribute::AttributeOperator;
use bqlite_operators::operator::EntityOperator;
use bqlite_planner::compiled::CompiledExpr;
use bqlite_planner::expr::{TypedExpr, TypedExprKind};
use bqlite_planner::physical::{AttributePhysical, ScanPhysical};
use bqlite_planner::PhysicalPlan;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

// ─────────────────────────────────────────────────────────────────────────────
// Scale knobs
// ─────────────────────────────────────────────────────────────────────────────

/// Workload sizes for each bench scenario, keyed by bench mode.
struct Scale {
    single_entity_touchpoints: usize,
    single_entity_conversions: usize,
    many_entities_count: usize,
    high_fan_out_touchpoints: usize,
    left_unnest_conversions: usize,
    multi_type_events: usize,
}

impl Scale {
    fn for_mode(mode: BenchMode) -> Self {
        match mode {
            BenchMode::Ci => Self {
                single_entity_touchpoints: 10_000,
                single_entity_conversions: 10,
                many_entities_count: 1_000,
                high_fan_out_touchpoints: 1_000,
                left_unnest_conversions: 1_000,
                multi_type_events: 10_000,
            },
            BenchMode::Reference => Self {
                // §17.1 targets.
                single_entity_touchpoints: 100_000,
                single_entity_conversions: 100,
                many_entities_count: 10_000,
                high_fan_out_touchpoints: 10_000,
                left_unnest_conversions: 100_000,
                multi_type_events: 1_000_000,
            },
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Fixtures
// ─────────────────────────────────────────────────────────────────────────────

fn events_schema() -> OperatorSchema {
    OperatorSchema::new(vec![
        ColumnDef::required("entity_id", BqlType::String),
        ColumnDef::required("ts", BqlType::Timestamp),
        ColumnDef::required("event_type", BqlType::String),
        ColumnDef::nullable("channel", BqlType::String),
    ])
    .unwrap()
}

fn output_schema() -> OperatorSchema {
    OperatorSchema::new(vec![
        ColumnDef::required("entity_id", BqlType::String),
        ColumnDef::required("conversion_ts", BqlType::Timestamp),
        ColumnDef::nullable("touchpoint_ts", BqlType::Timestamp),
        ColumnDef::nullable("touchpoint_key", BqlType::String),
    ])
    .unwrap()
}

fn channel_key_expr() -> CompiledExpr {
    CompiledExpr::from_typed(&TypedExpr {
        kind: TypedExprKind::Column {
            column_index: 3,
            name: "channel".into(),
        },
        result_type: BqlType::String,
        nullable: true,
        span: Span::EMPTY,
    })
}

fn arrow_schema() -> ArrowSchema {
    ArrowSchema::new(vec![
        Field::new("entity_id", DataType::Utf8View, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("event_type", DataType::Utf8View, false),
        Field::new("channel", DataType::Utf8View, true),
    ])
}

fn build_operator(conversion: &[&str], touchpoints: &[&str], window_ns: i64) -> AttributeOperator {
    let desc = AttributePhysical {
        conversion_events: conversion.iter().map(|s| s.to_string()).collect(),
        touchpoint_events: touchpoints.iter().map(|s| s.to_string()).collect(),
        window_ns,
        touchpoint_key: channel_key_expr(),
        forwarded_conversion_columns: vec![],
        fused_aggregate: None,
        conversion_range: None,
        input: Box::new(PhysicalPlan::Scan(ScanPhysical {
            table: "events".into(),
            query_range: None,
            reader_range: None,
            scan_predicates: vec![],
            projected_columns: vec![],
            output_schema: events_schema(),
            entity_key_col: "entity_id".into(),
            timestamp_col: "ts".into(),
            sample: None,
        })),
        output_schema: output_schema(),
    };
    AttributeOperator::from_physical(&desc).expect("valid descriptor")
}

fn build_batch<'a>(entity: &str, rows: &'a [(i64, &'a str, Option<&'a str>)]) -> RecordBatch {
    let entity_ids: Vec<&str> = rows.iter().map(|_| entity).collect();
    let ts: Vec<i64> = rows.iter().map(|r| r.0).collect();
    let ev: Vec<&str> = rows.iter().map(|r| r.1).collect();
    let channel: Vec<Option<&str>> = rows.iter().map(|r| r.2).collect();
    RecordBatch::try_new(
        Arc::new(arrow_schema()),
        vec![
            Arc::new(StringViewArray::from(entity_ids)),
            Arc::new(TimestampNanosecondArray::from(ts).with_timezone("UTC")),
            Arc::new(StringViewArray::from(ev)),
            Arc::new(StringViewArray::from(channel)),
        ],
    )
    .unwrap()
}

// ─────────────────────────────────────────────────────────────────────────────
// Workload builders
// ─────────────────────────────────────────────────────────────────────────────

/// Interleaved touchpoints and conversions for a single entity:
/// `N_TP` touchpoints with `N_CONV` conversion events spaced evenly.
fn single_entity_events(
    n_tp: usize,
    n_conv: usize,
) -> Vec<(i64, &'static str, Option<&'static str>)> {
    let mut out = Vec::with_capacity(n_tp + n_conv);
    let step = if n_conv == 0 {
        0
    } else {
        n_tp.max(1) / n_conv.max(1)
    };
    for i in 0..n_tp {
        out.push((i as i64 * 10, "click", Some("ads")));
    }
    // Interleave conversions every `step` touchpoints by inserting at
    // post-touchpoint timestamps.
    for j in 0..n_conv {
        let ts = ((j + 1) * step.max(1)) as i64 * 10 + 5;
        out.push((ts, "purchase", None));
    }
    out.sort_by_key(|r| r.0);
    out
}

/// Small entity: 1 conversion preceded by 4 touchpoints.
fn small_entity_events(entity_idx: usize) -> Vec<(i64, &'static str, Option<&'static str>)> {
    // Offset the base ts so no two entities share a timeline (purely
    // cosmetic; the operator treats each entity independently).
    let base = (entity_idx as i64) * 1000;
    vec![
        (base + 10, "click", Some("ads")),
        (base + 20, "click", Some("email")),
        (base + 30, "click", Some("search")),
        (base + 40, "click", Some("social")),
        (base + 50, "purchase", None),
    ]
}

/// 1 conversion, N touchpoints — single high-fan-out emission.
fn high_fan_out_events(n_tp: usize) -> Vec<(i64, &'static str, Option<&'static str>)> {
    let mut out = Vec::with_capacity(n_tp + 1);
    for i in 0..n_tp {
        out.push((i as i64, "click", Some("ads")));
    }
    out.push((n_tp as i64, "purchase", None));
    out
}

/// N conversions, no touchpoints — pure LEFT-UNNEST rows.
fn left_unnest_events(n: usize) -> Vec<(i64, &'static str, Option<&'static str>)> {
    (0..n).map(|i| (i as i64, "purchase", None)).collect()
}

/// Multi-type event stream: rotation across A/B/C/D/E to exercise
/// event-type dispatch in the hot loop. Conversions: A, B. Touchpoints: C, D, E.
fn multi_type_events(n: usize) -> Vec<(i64, &'static str, Option<&'static str>)> {
    const TYPES: &[(&str, Option<&str>)] = &[
        ("C", Some("c1")),
        ("D", Some("d1")),
        ("E", Some("e1")),
        ("A", None),
        ("B", None),
    ];
    (0..n)
        .map(|i| {
            let (ev, channel) = TYPES[i % TYPES.len()];
            (i as i64, ev, channel)
        })
        .collect()
}

// ─────────────────────────────────────────────────────────────────────────────
// Bench drivers
// ─────────────────────────────────────────────────────────────────────────────

fn bench_single_entity_many_touchpoints(c: &mut Criterion, scale: &Scale) {
    let events = single_entity_events(
        scale.single_entity_touchpoints,
        scale.single_entity_conversions,
    );
    let total_events = events.len() as u64;
    let batch = build_batch("user-1", &events);
    let window_ns = 10 * scale.single_entity_touchpoints as i64 * 2; // wide window

    let mut group = c.benchmark_group("attribute/single_entity_many_touchpoints");
    group.throughput(Throughput::Elements(total_events));
    group.bench_with_input(BenchmarkId::from_parameter(total_events), &(), |b, _| {
        let op = build_operator(&["purchase"], &["click"], window_ns);
        b.iter(|| {
            let mut state = op.create_state(&EntityId::String("user-1".into()));
            op.process_sub_batch(&mut state, &batch);
            black_box(op.finish_entity(state));
        });
    });
    group.finish();
}

fn bench_many_entities_sparse(c: &mut Criterion, scale: &Scale) {
    // Pre-build one batch per entity so the bench measures the
    // create/process/finish cycle, not batch construction.
    let batches: Vec<RecordBatch> = (0..scale.many_entities_count)
        .map(|i| {
            let events = small_entity_events(i);
            build_batch(&format!("user-{i}"), &events)
        })
        .collect();
    let total_events = (batches.len() * 5) as u64;

    let mut group = c.benchmark_group("attribute/many_entities_sparse");
    group.throughput(Throughput::Elements(total_events));
    group.bench_with_input(
        BenchmarkId::from_parameter(scale.many_entities_count),
        &(),
        |b, _| {
            let op = build_operator(&["purchase"], &["click"], 10_000);
            b.iter(|| {
                for (i, batch) in batches.iter().enumerate() {
                    let mut state = op.create_state(&EntityId::String(format!("user-{i}")));
                    op.process_sub_batch(&mut state, batch);
                    black_box(op.finish_entity(state));
                }
            });
        },
    );
    group.finish();
}

fn bench_high_fan_out(c: &mut Criterion, scale: &Scale) {
    let events = high_fan_out_events(scale.high_fan_out_touchpoints);
    let total = events.len() as u64;
    let batch = build_batch("user-1", &events);
    let window_ns = scale.high_fan_out_touchpoints as i64 * 2;

    let mut group = c.benchmark_group("attribute/high_fan_out");
    group.throughput(Throughput::Elements(total));
    group.bench_with_input(BenchmarkId::from_parameter(total), &(), |b, _| {
        let op = build_operator(&["purchase"], &["click"], window_ns);
        b.iter(|| {
            let mut state = op.create_state(&EntityId::String("user-1".into()));
            op.process_sub_batch(&mut state, &batch);
            black_box(op.finish_entity(state));
        });
    });
    group.finish();
}

fn bench_left_unnest_dominant(c: &mut Criterion, scale: &Scale) {
    let events = left_unnest_events(scale.left_unnest_conversions);
    let total = events.len() as u64;
    let batch = build_batch("user-1", &events);

    let mut group = c.benchmark_group("attribute/left_unnest_dominant");
    group.throughput(Throughput::Elements(total));
    group.bench_with_input(BenchmarkId::from_parameter(total), &(), |b, _| {
        let op = build_operator(&["purchase"], &["click"], 10_000);
        b.iter(|| {
            let mut state = op.create_state(&EntityId::String("user-1".into()));
            op.process_sub_batch(&mut state, &batch);
            black_box(op.finish_entity(state));
        });
    });
    group.finish();
}

fn bench_multi_type_attribution(c: &mut Criterion, scale: &Scale) {
    let events = multi_type_events(scale.multi_type_events);
    let total = events.len() as u64;
    let batch = build_batch("user-1", &events);
    // Narrow window relative to stream length so the deque stays small
    // — this bench targets event-type dispatch, not fan-out.
    let window_ns = 50i64;

    let mut group = c.benchmark_group("attribute/multi_type_attribution");
    group.throughput(Throughput::Elements(total));
    group.bench_with_input(BenchmarkId::from_parameter(total), &(), |b, _| {
        let op = build_operator(&["A", "B"], &["C", "D", "E"], window_ns);
        b.iter(|| {
            let mut state = op.create_state(&EntityId::String("user-1".into()));
            op.process_sub_batch(&mut state, &batch);
            black_box(op.finish_entity(state));
        });
    });
    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// Entry point
// ─────────────────────────────────────────────────────────────────────────────

fn all_benches(c: &mut Criterion) {
    let mode = BenchMode::from_env();
    let scale = Scale::for_mode(mode);
    bench_single_entity_many_touchpoints(c, &scale);
    bench_many_entities_sparse(c, &scale);
    bench_high_fan_out(c, &scale);
    bench_left_unnest_dominant(c, &scale);
    bench_multi_type_attribution(c, &scale);
}

criterion_group! {
    name = benches;
    config = criterion_for_mode(BenchMode::from_env());
    targets = all_benches
}
criterion_main!(benches);
