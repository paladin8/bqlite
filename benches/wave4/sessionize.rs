//! SESSIONIZE throughput benchmark (TASK-428).
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
//!
//! Run with:
//! ```bash
//! cargo bench -p bqlite-benches --bench sessionize
//! ```

use std::sync::Arc;

use arrow::array::{
    ArrayRef, DictionaryArray, Int32Array, RecordBatch, StringViewArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema as ArrowSchema, TimeUnit};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

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

criterion_group!(benches, bench_throughput);
criterion_main!(benches);
