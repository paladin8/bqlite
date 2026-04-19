//! EventSelectOperator throughput benchmarks (TASK-429, Wave 4).
//!
//! Measures `EventSelectOperator` entity-processing throughput for FIRST,
//! LAST, and NTH(5). Additional variants cover WHERE predicates,
//! event-type list matching (plain + dictionary-encoded), and varying
//! entity/event density.
//!
//! ## CI vs reference scale
//!
//! The CI fixture is intentionally small (1 000 entities × 100 events =
//! 100 000 total events) to keep wall time under 5 s on shared runners.
//! The relative ordering (FIRST ≈ early-termination fast path, LAST =
//! full scan, NTH(5) = intermediate) is observable at this scale and is
//! what matters for regression detection.
//!
//! For the full §21.1 reference targets (10M events / 100K entities),
//! run with `BQLITE_BENCH_MODE=reference` on pinned reference hardware.
//!
//! Run with:
//! ```bash
//! cargo bench -p bqlite-benches --bench event_select
//! ```

use std::sync::Arc;

use arrow::array::{
    DictionaryArray, Int32Array, Int64Array, StringViewBuilder, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema as ArrowSchema, TimeUnit};
use arrow::record_batch::RecordBatch;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use bqlite_core::{BqlType, ColumnDef, EntityId, OperatorSchema, PropertyValue, SEQ_ID_COLUMN};
use bqlite_operators::{EntityOperator, EventSelectOperator};
use bqlite_planner::compiled::{
    ArrowKernelId, CompareKernel, CompareOp, CompiledExpr, CompiledNode,
};
use bqlite_planner::logical::EventSelectKind;
use bqlite_planner::physical::{EventSelectPhysical, PhysicalPlan, ScanPhysical};

// ─────────────────────────────────────────────────────────────────────────────
// Fixture parameters
// ─────────────────────────────────────────────────────────────────────────────

const NUM_ENTITIES: usize = 1_000;
const EVENTS_PER_ENTITY: usize = 100;

// ─────────────────────────────────────────────────────────────────────────────
// Schema helpers
// ─────────────────────────────────────────────────────────────────────────────

fn input_schema() -> OperatorSchema {
    OperatorSchema::new(vec![
        ColumnDef::required("entity_id", BqlType::String),
        ColumnDef::required("ts", BqlType::Timestamp),
        ColumnDef::required(SEQ_ID_COLUMN, BqlType::Int),
        ColumnDef::required("event_type", BqlType::String),
        ColumnDef::required("amount", BqlType::Int),
    ])
    .unwrap()
}

fn output_schema() -> OperatorSchema {
    OperatorSchema::new(vec![
        ColumnDef::required("entity_id", BqlType::String),
        ColumnDef::required("ts", BqlType::Timestamp),
        ColumnDef::required("event_type", BqlType::String),
        ColumnDef::required("amount", BqlType::Int),
    ])
    .unwrap()
}

fn scan_physical(schema: OperatorSchema) -> ScanPhysical {
    ScanPhysical {
        table: "events".to_string(),
        query_range: None,
        reader_range: None,
        scan_predicates: vec![],
        projected_columns: vec![],
        output_schema: schema,
        entity_key_col: "entity_id".to_string(),
        timestamp_col: "ts".to_string(),
        sample: None,
    }
}

fn make_desc(
    kind: EventSelectKind,
    event_types: Vec<&str>,
    predicate: Option<CompiledExpr>,
) -> EventSelectPhysical {
    let in_schema = input_schema();
    EventSelectPhysical {
        kind,
        event_types: event_types.iter().map(|s| s.to_string()).collect(),
        predicate,
        lookback: None,
        forwarded_columns: vec![],
        fused_aggregate: None,
        input: Box::new(PhysicalPlan::Scan(scan_physical(in_schema))),
        output_schema: output_schema(),
    }
}

/// `amount > 50` predicate against column index 4 in the input schema.
fn predicate_amount_gt_50() -> CompiledExpr {
    CompiledExpr {
        node: CompiledNode::Compare {
            op: CompareOp::Greater,
            left: Box::new(CompiledExpr {
                node: CompiledNode::Column {
                    index: 4,
                    name: "amount".into(),
                },
                result_type: BqlType::Int,
                nullable: false,
            }),
            right: Box::new(CompiledExpr {
                node: CompiledNode::Literal(PropertyValue::Int(50)),
                result_type: BqlType::Int,
                nullable: false,
            }),
            kernel: CompareKernel::ArrowKernel(ArrowKernelId::GtInt),
        },
        result_type: BqlType::Bool,
        nullable: false,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Fixture generation
// ─────────────────────────────────────────────────────────────────────────────

/// Arrow schema for the input batch.
fn arrow_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("entity_id", DataType::Utf8View, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new(SEQ_ID_COLUMN, DataType::Int64, false),
        Field::new("event_type", DataType::Utf8View, false),
        Field::new("amount", DataType::Int64, false),
    ]))
}

/// Build a single-entity RecordBatch.
///
/// - `n_events` rows total
/// - Half are `"purchase"`, half `"login"` (interleaved)
/// - `amount` cycles through 0–99 (50% qualify for `amount > 50`)
fn make_entity_batch(entity_id: &str, n_events: usize) -> RecordBatch {
    let schema = arrow_schema();

    let mut eid_b = StringViewBuilder::with_capacity(n_events);
    let mut ts_b = TimestampNanosecondArray::builder(n_events);
    let mut seq_b = Int64Array::builder(n_events);
    let mut et_b = StringViewBuilder::with_capacity(n_events);
    let mut amt_b = Int64Array::builder(n_events);

    for i in 0..n_events {
        eid_b.append_value(entity_id);
        ts_b.append_value(i as i64 * 1_000_000); // 1ms apart
        seq_b.append_value(i as i64);
        let event_type = if i % 2 == 0 { "purchase" } else { "login" };
        et_b.append_value(event_type);
        amt_b.append_value((i % 100) as i64); // 0..99 → ~50% > 50
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(eid_b.finish()),
            Arc::new(ts_b.finish().with_timezone("UTC")),
            Arc::new(seq_b.finish()),
            Arc::new(et_b.finish()),
            Arc::new(amt_b.finish()),
        ],
    )
    .unwrap()
}

/// Build a single-entity RecordBatch with dictionary-encoded event_type
/// (`Dictionary<Int32, Utf8View>`). Same data distribution as
/// [`make_entity_batch`] but exercises the `EventTypeCodeSet` fast path.
fn make_entity_batch_dict(entity_id: &str, n_events: usize) -> RecordBatch {
    // Two-value dictionary: code 0 = "purchase", code 1 = "login".
    let dict_values = {
        let mut b = StringViewBuilder::with_capacity(2);
        b.append_value("purchase");
        b.append_value("login");
        Arc::new(b.finish())
    };

    let mut eid_b = StringViewBuilder::with_capacity(n_events);
    let mut ts_b = TimestampNanosecondArray::builder(n_events);
    let mut seq_b = Int64Array::builder(n_events);
    let mut keys: Vec<i32> = Vec::with_capacity(n_events);
    let mut amt_b = Int64Array::builder(n_events);

    for i in 0..n_events {
        eid_b.append_value(entity_id);
        ts_b.append_value(i as i64 * 1_000_000);
        seq_b.append_value(i as i64);
        keys.push((i % 2) as i32); // 0 = "purchase", 1 = "login"
        amt_b.append_value((i % 100) as i64);
    }

    let dict_encoded: Arc<dyn arrow::array::Array> = Arc::new(
        DictionaryArray::<Int32Type>::try_new(Int32Array::from(keys), dict_values).unwrap(),
    );

    let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8View));
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("entity_id", DataType::Utf8View, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new(SEQ_ID_COLUMN, DataType::Int64, false),
        Field::new("event_type", dict_type, false),
        Field::new("amount", DataType::Int64, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(eid_b.finish()),
            Arc::new(ts_b.finish().with_timezone("UTC")),
            Arc::new(seq_b.finish()),
            dict_encoded,
            Arc::new(amt_b.finish()),
        ],
    )
    .unwrap()
}

/// Pre-built fixture: one RecordBatch per entity (plain StringView event_type).
fn build_fixture(num_entities: usize, events_per_entity: usize) -> Vec<(EntityId, RecordBatch)> {
    (0..num_entities)
        .map(|i| {
            let eid = EntityId::from(format!("entity_{i}").as_str());
            let batch = make_entity_batch(&format!("entity_{i}"), events_per_entity);
            (eid, batch)
        })
        .collect()
}

/// Pre-built fixture with dictionary-encoded event_type column.
fn build_fixture_dict(
    num_entities: usize,
    events_per_entity: usize,
) -> Vec<(EntityId, RecordBatch)> {
    (0..num_entities)
        .map(|i| {
            let eid = EntityId::from(format!("entity_{i}").as_str());
            let batch = make_entity_batch_dict(&format!("entity_{i}"), events_per_entity);
            (eid, batch)
        })
        .collect()
}

// ─────────────────────────────────────────────────────────────────────────────
// Core driver
// ─────────────────────────────────────────────────────────────────────────────

/// Drive an EventSelectOperator over `fixture` and return the count of
/// entities that produced output rows (prevents dead-code elimination).
fn run_event_select(op: &EventSelectOperator, fixture: &[(EntityId, RecordBatch)]) -> usize {
    let mut hits = 0usize;
    for (eid, batch) in fixture {
        let mut state = op.create_state(eid);
        op.process_sub_batch(&mut state, batch);
        if op.finish_entity(state).is_some() {
            hits += 1;
        }
    }
    hits
}

// ─────────────────────────────────────────────────────────────────────────────
// Benchmark: FIRST / LAST / NTH(5) baseline
// ─────────────────────────────────────────────────────────────────────────────

fn bench_first_last_nth(c: &mut Criterion) {
    let fixture = build_fixture(NUM_ENTITIES, EVENTS_PER_ENTITY);
    let total_events = (NUM_ENTITIES * EVENTS_PER_ENTITY) as u64;

    let mut group = c.benchmark_group("event_select/kind");
    group.throughput(Throughput::Elements(total_events));

    let cases: Vec<(&str, EventSelectKind)> = vec![
        ("first", EventSelectKind::First),
        ("last", EventSelectKind::Last),
        ("nth_5", EventSelectKind::Nth(5)),
    ];

    for (label, kind) in &cases {
        let desc = make_desc(*kind, vec!["purchase"], None);
        let op = EventSelectOperator::new(&desc, &input_schema());

        group.bench_with_input(BenchmarkId::new("kind", label), label, |b, _| {
            b.iter(|| black_box(run_event_select(&op, &fixture)))
        });
    }
    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// Benchmark: FIRST with WHERE predicate
// ─────────────────────────────────────────────────────────────────────────────

fn bench_first_with_predicate(c: &mut Criterion) {
    let fixture = build_fixture(NUM_ENTITIES, EVENTS_PER_ENTITY);
    let total_events = (NUM_ENTITIES * EVENTS_PER_ENTITY) as u64;

    let mut group = c.benchmark_group("event_select/predicate");
    group.throughput(Throughput::Elements(total_events));

    let cases: Vec<(&str, Option<CompiledExpr>)> = vec![
        ("no_predicate", None),
        ("amount_gt_50", Some(predicate_amount_gt_50())),
    ];

    for (label, predicate) in cases {
        let desc = make_desc(EventSelectKind::First, vec!["purchase"], predicate);
        let op = EventSelectOperator::new(&desc, &input_schema());

        group.bench_with_input(BenchmarkId::new("first", label), &0usize, |b, _| {
            b.iter(|| black_box(run_event_select(&op, &fixture)))
        });
    }
    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// Benchmark: multi-type event list
// ─────────────────────────────────────────────────────────────────────────────

fn bench_event_type_list(c: &mut Criterion) {
    let fixture = build_fixture(NUM_ENTITIES, EVENTS_PER_ENTITY);
    let total_events = (NUM_ENTITIES * EVENTS_PER_ENTITY) as u64;

    let mut group = c.benchmark_group("event_select/event_type_list");
    group.throughput(Throughput::Elements(total_events));

    let cases: Vec<(&str, Vec<&str>)> = vec![
        ("single_type", vec!["purchase"]),
        ("two_types", vec!["purchase", "checkout"]),
        (
            "four_types",
            vec!["purchase", "checkout", "add_to_cart", "view_item"],
        ),
    ];

    for (label, types) in &cases {
        let desc = make_desc(EventSelectKind::First, types.clone(), None);
        let op = EventSelectOperator::new(&desc, &input_schema());

        group.bench_with_input(BenchmarkId::new("first", label), &types.len(), |b, _| {
            b.iter(|| black_box(run_event_select(&op, &fixture)))
        });
    }
    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// Benchmark: varying entity/event density
// ─────────────────────────────────────────────────────────────────────────────

fn bench_entity_density(c: &mut Criterion) {
    let mut group = c.benchmark_group("event_select/density");

    // (entities, events_per_entity) — total ≈ 100k each.
    let cases: Vec<(&str, usize, usize)> = vec![
        ("100e_1000ev", 100, 1_000), // few entities, many events each
        ("1000e_100ev", 1_000, 100), // balanced
        ("10000e_10ev", 10_000, 10), // many entities, few events each
    ];

    for (label, n_entities, n_events) in &cases {
        let total = (n_entities * n_events) as u64;
        let fixture = build_fixture(*n_entities, *n_events);
        let desc = make_desc(EventSelectKind::First, vec!["purchase"], None);
        let op = EventSelectOperator::new(&desc, &input_schema());

        group.throughput(Throughput::Elements(total));
        group.bench_with_input(BenchmarkId::new("first", label), &total, |b, _| {
            b.iter(|| black_box(run_event_select(&op, &fixture)))
        });
    }
    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// Benchmark: dictionary-encoded event_type (§21.1 dict fast path)
// ─────────────────────────────────────────────────────────────────────────────

fn bench_dict_event_type(c: &mut Criterion) {
    let fixture_plain = build_fixture(NUM_ENTITIES, EVENTS_PER_ENTITY);
    let fixture_dict = build_fixture_dict(NUM_ENTITIES, EVENTS_PER_ENTITY);
    let total_events = (NUM_ENTITIES * EVENTS_PER_ENTITY) as u64;

    let mut group = c.benchmark_group("event_select/encoding");
    group.throughput(Throughput::Elements(total_events));

    let desc_plain = make_desc(EventSelectKind::First, vec!["purchase"], None);
    let op_plain = EventSelectOperator::new(&desc_plain, &input_schema());
    group.bench_with_input(
        BenchmarkId::new("first_plain_stringview", "1000e_100ev"),
        &0usize,
        |b, _| b.iter(|| black_box(run_event_select(&op_plain, &fixture_plain))),
    );

    // Same operator descriptor works for dictionary input — the operator
    // handles both via `build_event_type_check` dispatch.
    let desc_dict = make_desc(EventSelectKind::First, vec!["purchase"], None);
    let op_dict = EventSelectOperator::new(&desc_dict, &input_schema());
    group.bench_with_input(
        BenchmarkId::new("first_dict_int32", "1000e_100ev"),
        &0usize,
        |b, _| b.iter(|| black_box(run_event_select(&op_dict, &fixture_dict))),
    );

    group.finish();
}

criterion_group!(
    event_select,
    bench_first_last_nth,
    bench_first_with_predicate,
    bench_event_type_list,
    bench_entity_density,
    bench_dict_event_type,
);
criterion_main!(event_select);
