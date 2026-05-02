//! SESSIONIZE throughput benchmark (TASK-428, TASK-535).
//!
//! Feeds synthetic `(entity_id, ts, event_type)` streams into a
//! `SessionizeOperator` and measures events-per-second throughput across:
//!
//! - **gap-only**: gap boundaries only; no end-event matching.
//! - **gap + single end-event**: one end-event type, `StringViewArray`
//!   layout (scan decodes to the canonical Utf8View).
//! - **gap + dict end-event**: the same single end-event, but the
//!   `event_type` column arrives as a
//!   `DictionaryArray<Int32, Utf8View>` — exercises the code-set
//!   fast path (sessionize.md §8.2).
//! - **multi-end-event (1 / 3 / 5 types, StringView and Dict)**: exercises
//!   the `EndEventCodeSet` membership path across multiple end-event type
//!   cardinalities (TASK-535). A `[floor]` tripwire asserts that the
//!   3-type dictionary case is no more than 1.5× slower than the 1-type
//!   dictionary baseline (sessionize.md §14.2).
//!
//! Run with:
//! ```bash
//! cargo bench -p bqlite-benches --bench sessionize
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{
    ArrayRef, DictionaryArray, Int32Array, RecordBatch, StringViewArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema as ArrowSchema, TimeUnit};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use bqlite_benches::common::{BenchMode, BenchResultCollector, BenchTarget};
use bqlite_core::{BqlType, ColumnDef, EntityId, OperatorSchema, TableSchema};
use bqlite_operators::{EntityOperator, SessionizeOperator};
use bqlite_planner::logical::LogicalPlan;
use bqlite_planner::physical::{lower_physical, PhysicalPlan, SessionizePhysical};

// ─────────────────────────────────────────────────────────────────────────────
// Fixture shapes
// ─────────────────────────────────────────────────────────────────────────────

fn input_schema() -> OperatorSchema {
    OperatorSchema::new(vec![
        ColumnDef::required("entity_id", BqlType::String),
        ColumnDef::required("ts", BqlType::Timestamp),
        ColumnDef::required("event_type", BqlType::String),
    ])
    .unwrap()
}

fn output_schema(base: &OperatorSchema) -> OperatorSchema {
    let mut cols = base.columns().to_vec();
    cols.push(ColumnDef::required("session_id", BqlType::Int));
    cols.push(ColumnDef::required("session_duration", BqlType::Int));
    OperatorSchema::new(cols).unwrap()
}

fn build_op(gap_ns: i64, end_events: Vec<String>) -> SessionizeOperator {
    let input = input_schema();
    let scan = LogicalPlan::scan(
        TableSchema::new(
            "events",
            input.columns().to_vec(),
            "entity_id",
            "ts",
            "event_type",
        )
        .unwrap(),
    );
    let os = output_schema(&input);
    let node = LogicalPlan::Sessionize {
        gap: gap_ns,
        end_events,
        forwarded_columns: vec![],
        fused_downstream: None,
        entity_key_col: "entity_id".into(),
        input: Box::new(scan),
        output_schema: os,
    };
    let PhysicalPlan::Sessionize(desc): PhysicalPlan = lower_physical(node, 0) else {
        unreachable!()
    };
    let desc: SessionizePhysical = desc;
    SessionizeOperator::new(&desc)
}

/// Build a synthetic batch for a single entity with `num_events` events,
/// gap_ns=1_800_000_000_000 (30 min) and deterministic per-event delta so
/// sessions close every ~50 events.
fn build_events(num_events: usize, dictionary: bool, with_end_events: bool) -> RecordBatch {
    let num = num_events;
    let mut ts: Vec<i64> = Vec::with_capacity(num);
    let mut events: Vec<&str> = Vec::with_capacity(num);

    // Deterministic ts pattern: +10ns most of the time, with a >gap jump
    // every 50 events. Gap is 100ns for these benches (contrived to keep
    // timestamps compact while still exercising boundaries).
    let gap_ns_test: i64 = 100;
    let mut t: i64 = 1_000;
    for i in 0..num {
        if i > 0 && i % 50 == 0 {
            t += gap_ns_test + 1;
        } else {
            t += 10;
        }
        ts.push(t);
        // Emit a `logout` every 100 events so gap+end interleave.
        let et = if with_end_events && i % 100 == 99 {
            "logout"
        } else if i % 3 == 0 {
            "click"
        } else if i % 3 == 1 {
            "view"
        } else {
            "search"
        };
        events.push(et);
    }

    let fields = vec![
        Field::new("entity_id", DataType::Utf8View, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new(
            "event_type",
            if dictionary {
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8View))
            } else {
                DataType::Utf8View
            },
            false,
        ),
    ];
    let schema = Arc::new(ArrowSchema::new(fields));

    let entity_col: ArrayRef = Arc::new(StringViewArray::from(vec!["e1"; num]));
    let ts_col: ArrayRef = Arc::new(TimestampNanosecondArray::from(ts).with_timezone("UTC"));
    let et_col: ArrayRef = if dictionary {
        let values = Arc::new(StringViewArray::from(vec![
            "click", "view", "search", "logout",
        ]));
        let keys = Int32Array::from(
            events
                .iter()
                .map(|&e| match e {
                    "click" => 0,
                    "view" => 1,
                    "search" => 2,
                    "logout" => 3,
                    _ => 0,
                })
                .collect::<Vec<i32>>(),
        );
        Arc::new(DictionaryArray::<Int32Type>::try_new(keys, values).unwrap())
    } else {
        Arc::new(StringViewArray::from(events))
    };
    RecordBatch::try_new(schema, vec![entity_col, ts_col, et_col]).unwrap()
}

fn run_sessionize(op: &SessionizeOperator, batch: &RecordBatch) -> Option<RecordBatch> {
    let mut state = op.create_state(&EntityId::from("e1"));
    op.process_sub_batch(&mut state, batch);
    op.finish_entity(state)
}

// ─────────────────────────────────────────────────────────────────────────────
// Benchmark groups
// ─────────────────────────────────────────────────────────────────────────────

fn bench_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("sessionize/throughput");
    for &num in &[10_000usize, 100_000usize] {
        group.throughput(Throughput::Elements(num as u64));

        let op_gap = build_op(100, vec![]);
        let batch_gap = build_events(num, false, false);
        group.bench_with_input(
            BenchmarkId::new("gap_only", num),
            &(&op_gap, &batch_gap),
            |b, (op, batch)| {
                b.iter(|| {
                    let out = run_sessionize(op, batch);
                    black_box(out);
                });
            },
        );

        let op_end = build_op(100, vec!["logout".into()]);
        let batch_end = build_events(num, false, true);
        group.bench_with_input(
            BenchmarkId::new("gap_plus_end_stringview", num),
            &(&op_end, &batch_end),
            |b, (op, batch)| {
                b.iter(|| {
                    let out = run_sessionize(op, batch);
                    black_box(out);
                });
            },
        );

        let op_dict = build_op(100, vec!["logout".into()]);
        let batch_dict = build_events(num, true, true);
        group.bench_with_input(
            BenchmarkId::new("gap_plus_end_dict", num),
            &(&op_dict, &batch_dict),
            |b, (op, batch)| {
                b.iter(|| {
                    let out = run_sessionize(op, batch);
                    black_box(out);
                });
            },
        );
    }
    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// Multi-end-event helpers (TASK-535)
// ─────────────────────────────────────────────────────────────────────────────

/// The full ordered list of end-event type names available for multi-end-event
/// benchmarks. Slices of this array provide the 1 / 3 / 5-type sub-lists.
const MULTI_END_TYPES: &[&str] = &["logout", "timeout", "session_end", "app_close", "error"];

/// Return the first `n` end-event type names as owned strings.
fn multi_end_event_list(n: usize) -> Vec<String> {
    MULTI_END_TYPES[..n].iter().map(|s| s.to_string()).collect()
}

/// Build a synthetic batch for multi-end-event benchmarks.
///
/// Regular event types: `click`, `view`, `search` (cycling, 3-way).
/// End-event types: the first `n_end_types` entries of `MULTI_END_TYPES`.
/// End events fire every 100 rows, cycling across the `n_end_types` types so
/// that each configured end-event type appears in the data. Gap boundaries
/// occur every 50 events (same cadence as `build_events`).
fn build_multi_end_events(num_events: usize, dictionary: bool, n_end_types: usize) -> RecordBatch {
    const REGULAR: &[&str] = &["click", "view", "search"];
    let active_end = &MULTI_END_TYPES[..n_end_types];

    // All distinct event types that will appear in this batch — regular first,
    // then active end-event types.
    let mut all_types: Vec<&str> = REGULAR.to_vec();
    all_types.extend_from_slice(active_end);

    // Build a reverse map for dictionary key construction (used only when
    // `dictionary = true`; cheap at bench-setup time, not on the hot path).
    let type_to_idx: HashMap<&str, i32> = all_types
        .iter()
        .enumerate()
        .map(|(i, &s)| (s, i as i32))
        .collect();

    let gap_ns_test: i64 = 100;
    let mut ts_vals: Vec<i64> = Vec::with_capacity(num_events);
    let mut events: Vec<&str> = Vec::with_capacity(num_events);
    let mut t: i64 = 1_000;

    for i in 0..num_events {
        if i > 0 && i % 50 == 0 {
            t += gap_ns_test + 1;
        } else {
            t += 10;
        }
        ts_vals.push(t);
        // End event every 100 rows, cycling through the active end-event types.
        let et = if i % 100 == 99 {
            active_end[(i / 100) % n_end_types]
        } else {
            REGULAR[i % REGULAR.len()]
        };
        events.push(et);
    }

    let fields = vec![
        Field::new("entity_id", DataType::Utf8View, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new(
            "event_type",
            if dictionary {
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8View))
            } else {
                DataType::Utf8View
            },
            false,
        ),
    ];
    let schema = Arc::new(ArrowSchema::new(fields));

    let entity_col: ArrayRef = Arc::new(StringViewArray::from(vec!["e1"; num_events]));
    let ts_col: ArrayRef = Arc::new(TimestampNanosecondArray::from(ts_vals).with_timezone("UTC"));
    let et_col: ArrayRef = if dictionary {
        let values = Arc::new(StringViewArray::from(all_types));
        let keys = Int32Array::from(
            events
                .iter()
                .map(|&e| *type_to_idx.get(e).unwrap())
                .collect::<Vec<i32>>(),
        );
        Arc::new(DictionaryArray::<Int32Type>::try_new(keys, values).unwrap())
    } else {
        Arc::new(StringViewArray::from(events))
    };
    RecordBatch::try_new(schema, vec![entity_col, ts_col, et_col]).unwrap()
}

// ─────────────────────────────────────────────────────────────────────────────
// Benchmark: multi-end-event-type cardinality sweep (TASK-535, sessionize.md §14.2)
// ─────────────────────────────────────────────────────────────────────────────

fn bench_multi_end_event(c: &mut Criterion) {
    let mode = BenchMode::from_env();
    let mut collector = BenchResultCollector::new(mode);
    let mut group = c.benchmark_group("sessionize/multi_end_event");

    for &num in &[10_000usize, 100_000usize] {
        group.throughput(Throughput::Elements(num as u64));

        for &n_end in &[1usize, 3, 5] {
            let end_events = multi_end_event_list(n_end);

            // StringView variant — direct string comparisons.
            let op_sv = build_op(100, end_events.clone());
            let batch_sv = build_multi_end_events(num, false, n_end);
            group.bench_with_input(
                BenchmarkId::new(format!("stringview_{n_end}types"), num),
                &(&op_sv, &batch_sv),
                |b, (op, batch)| {
                    b.iter(|| black_box(run_sessionize(op, batch)));
                },
            );

            // Dictionary variant — EndEventCodeSet integer fast path.
            let op_dict = build_op(100, end_events.clone());
            let batch_dict = build_multi_end_events(num, true, n_end);
            group.bench_with_input(
                BenchmarkId::new(format!("dict_{n_end}types"), num),
                &(&op_dict, &batch_dict),
                |b, (op, batch)| {
                    b.iter(|| black_box(run_sessionize(op, batch)));
                },
            );
        }
    }
    group.finish();

    // [floor] tripwire (sessionize.md §14.2, TASK-535): the 3-type dictionary
    // case must be no more than 1.5× slower than the 1-type dictionary
    // baseline. EndEventCodeSet.matching_codes is a HashSet<i32>; O(1) probe
    // cost for ≤5 entries should be near-free. Measured at 100 000 events for
    // stable timing.
    if mode.is_reference() {
        const SCALE: usize = 100_000;
        const RUNS: u64 = 10;

        let op_1 = build_op(100, multi_end_event_list(1));
        let batch_1 = build_multi_end_events(SCALE, true, 1);
        black_box(run_sessionize(&op_1, &batch_1)); // warm-up
        let t0 = Instant::now();
        for _ in 0..RUNS {
            black_box(run_sessionize(&op_1, &batch_1));
        }
        let eps_1type = (SCALE as f64 * RUNS as f64) / t0.elapsed().as_secs_f64();
        collector.record(
            "sessionize/multi_end_event/dict_1type/events_per_sec",
            eps_1type,
            "events/sec",
            None,
        );

        let op_3 = build_op(100, multi_end_event_list(3));
        let batch_3 = build_multi_end_events(SCALE, true, 3);
        black_box(run_sessionize(&op_3, &batch_3)); // warm-up
        let t0 = Instant::now();
        for _ in 0..RUNS {
            black_box(run_sessionize(&op_3, &batch_3));
        }
        let eps_3type = (SCALE as f64 * RUNS as f64) / t0.elapsed().as_secs_f64();
        collector.record(
            "sessionize/multi_end_event/dict_3type/events_per_sec",
            eps_3type,
            "events/sec",
            None,
        );

        // Ratio = 1-type throughput / 3-type throughput. Value ≤ 1.5 means
        // 3-type is at most 1.5× slower than 1-type — the floor.
        let ratio = eps_1type / eps_3type.max(1.0);
        collector.record(
            "sessionize/multi_end_event/dict_3_vs_1_slowdown_ratio",
            ratio,
            "ratio",
            Some(BenchTarget::at_most(1.5)),
        );
    }
    collector.finish();
}

criterion_group!(benches, bench_throughput, bench_multi_end_event);
criterion_main!(benches);
