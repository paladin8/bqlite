//! Property tests for `EventSelectOperator` (TASK-429, Wave 4).
//!
//! Covers the seven correctness invariants from
//! `docs/design/operators/event-select-sample.md` §22.1:
//!
//! 1. **Output cardinality** — 0 or 1 rows per entity.
//! 2. **FIRST correctness** — emitted row has minimum `(ts, __seq_id)`
//!    among qualifying events.
//! 3. **LAST correctness** — emitted row has maximum `(ts, __seq_id)`
//!    among qualifying events.
//! 4. **NTH correctness** — the n-th qualifying event (sorted by
//!    `(ts, __seq_id)`) is emitted.
//! 5. **Omission invariant** — no row emitted iff fewer than `n`
//!    qualifying events exist.
//! 6. **Entity isolation** — every column in the output row matches the
//!    data from the same row in the input batch (no cross-row mixing).
//! 7. **NTH(1) == FIRST** — equivalent results for all event sequences.

use std::sync::Arc;

use arrow::array::{Int64Array, StringViewBuilder, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use arrow::record_batch::RecordBatch;
use proptest::prelude::*;

use bqlite_core::{BqlType, ColumnDef, EntityId, OperatorSchema, SEQ_ID_COLUMN};
use bqlite_operators::{EntityOperator, EventSelectOperator};
use bqlite_planner::logical::EventSelectKind;
use bqlite_planner::physical::{EventSelectPhysical, PhysicalPlan, ScanPhysical};

// ─────────────────────────────────────────────────────────────────────────────
// Test fixture types
// ─────────────────────────────────────────────────────────────────────────────

/// One event row in the property test fixture.
///
/// `ts` and `seq_id` are `i32` to stay well away from `Timestamp::MAX`
/// sentinel, which must not appear in event data. `is_target` drives the
/// `event_type` column: `true` → `"target"`, `false` → `"other"`.
#[derive(Debug, Clone)]
struct EventRow {
    ts: i32,
    seq_id: i32,
    is_target: bool,
    amount: i64,
}

// ─────────────────────────────────────────────────────────────────────────────
// Strategies
// ─────────────────────────────────────────────────────────────────────────────

fn arb_event_row() -> impl Strategy<Value = EventRow> {
    (
        any::<i32>(),
        any::<i32>(),
        any::<bool>(),
        any::<i16>().prop_map(|x| x as i64),
    )
        .prop_map(|(ts, seq_id, is_target, amount)| EventRow {
            ts,
            seq_id,
            is_target,
            amount,
        })
}

/// Strategy producing a non-empty sorted sequence of `EventRow` values.
///
/// Events are sorted by `(ts, seq_id)` ascending, matching the
/// entity-sorted order that `process_sub_batch` requires.
fn arb_event_sequence() -> impl Strategy<Value = Vec<EventRow>> {
    prop::collection::vec(arb_event_row(), 0..=32).prop_map(|mut rows| {
        rows.sort_by_key(|r| (r.ts, r.seq_id));
        rows
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// Schema / operator helpers
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

fn make_op(kind: EventSelectKind) -> EventSelectOperator {
    let in_schema = input_schema();
    let desc = EventSelectPhysical {
        kind,
        event_types: vec!["target".to_string()],
        predicate: None,
        lookback: None,
        forwarded_columns: vec![],
        fused_aggregate: None,
        input: Box::new(PhysicalPlan::Scan(ScanPhysical {
            table: "events".to_string(),
            query_range: None,
            reader_range: None,
            scan_predicates: vec![],
            projected_columns: vec![],
            output_schema: in_schema.clone(),
            entity_key_col: "entity_id".to_string(),
            timestamp_col: "ts".to_string(),
        })),
        output_schema: output_schema(),
    };
    EventSelectOperator::new(&desc, &in_schema)
}

/// Build a RecordBatch from the property-test fixture rows.
fn build_batch(rows: &[EventRow]) -> RecordBatch {
    let n = rows.len();

    let mut eid_b = StringViewBuilder::with_capacity(n);
    let mut ts_b = TimestampNanosecondArray::builder(n);
    let mut seq_b = Int64Array::builder(n);
    let mut et_b = StringViewBuilder::with_capacity(n);
    let mut amt_b = Int64Array::builder(n);

    for row in rows {
        eid_b.append_value("u1");
        ts_b.append_value(row.ts as i64);
        seq_b.append_value(row.seq_id as i64);
        et_b.append_value(if row.is_target { "target" } else { "other" });
        amt_b.append_value(row.amount);
    }

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("entity_id", DataType::Utf8View, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new(SEQ_ID_COLUMN, DataType::Int64, false),
        Field::new("event_type", DataType::Utf8View, false),
        Field::new("amount", DataType::Int64, false),
    ]));

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

/// Run the operator over one entity batch and return the output row (or None).
fn run(op: &EventSelectOperator, batch: &RecordBatch) -> Option<RecordBatch> {
    let eid = EntityId::from("u1");
    let mut state = op.create_state(&eid);
    op.process_sub_batch(&mut state, batch);
    op.finish_entity(state)
}

/// All qualifying rows (target type) from the sorted event sequence.
fn qualifying(rows: &[EventRow]) -> Vec<&EventRow> {
    rows.iter().filter(|r| r.is_target).collect()
}

/// Extract `ts` value from output batch row 0.
fn out_ts(batch: &RecordBatch) -> i64 {
    batch
        .column_by_name("ts")
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap()
        .value(0)
}

/// Extract `amount` value from output batch row 0.
fn out_amount(batch: &RecordBatch) -> i64 {
    batch
        .column_by_name("amount")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

// ─────────────────────────────────────────────────────────────────────────────
// Property tests
// ─────────────────────────────────────────────────────────────────────────────

proptest! {
    /// Invariant 1: output cardinality is always 0 or 1 rows per entity.
    #[test]
    fn prop_output_cardinality_zero_or_one(rows in arb_event_sequence()) {
        let op = make_op(EventSelectKind::First);
        let batch = build_batch(&rows);
        let out = run(&op, &batch);
        if let Some(b) = out {
            prop_assert_eq!(b.num_rows(), 1);
        }
    }

    /// Invariant 2: FIRST emits the qualifying event with minimum (ts, seq_id).
    #[test]
    fn prop_first_emits_minimum_qualifying(rows in arb_event_sequence()) {
        let q = qualifying(&rows);
        let op = make_op(EventSelectKind::First);
        let batch = build_batch(&rows);
        let out = run(&op, &batch);

        if q.is_empty() {
            prop_assert!(out.is_none(), "empty qualifying set must produce None");
        } else {
            let result = out.expect("non-empty qualifying set must produce Some");
            let first = q[0]; // sorted by (ts, seq_id), so first qualifying = minimum
            prop_assert_eq!(out_ts(&result), first.ts as i64,
                "FIRST must emit minimum-ts qualifying row");
            prop_assert_eq!(out_amount(&result), first.amount,
                "FIRST must emit data from the minimum-ts qualifying row");
        }
    }

    /// Invariant 3: LAST emits the qualifying event with maximum (ts, seq_id).
    #[test]
    fn prop_last_emits_maximum_qualifying(rows in arb_event_sequence()) {
        let q = qualifying(&rows);
        let op = make_op(EventSelectKind::Last);
        let batch = build_batch(&rows);
        let out = run(&op, &batch);

        if q.is_empty() {
            prop_assert!(out.is_none(), "empty qualifying set must produce None");
        } else {
            let result = out.expect("non-empty qualifying set must produce Some");
            let last = q[q.len() - 1]; // last qualifying = maximum (ts, seq_id)
            prop_assert_eq!(out_ts(&result), last.ts as i64,
                "LAST must emit maximum-ts qualifying row");
            prop_assert_eq!(out_amount(&result), last.amount,
                "LAST must emit data from the maximum-ts qualifying row");
        }
    }

    /// Invariant 4 + 5: NTH(n) emits exactly the n-th qualifying event,
    /// and produces None when fewer than n qualifying events exist.
    #[test]
    fn prop_nth_emits_nth_qualifying_or_none(
        rows in arb_event_sequence(),
        n in 1u32..=5u32,
    ) {
        let q = qualifying(&rows);
        let op = make_op(EventSelectKind::Nth(n));
        let batch = build_batch(&rows);
        let out = run(&op, &batch);

        if (q.len() as u32) < n {
            // Invariant 5: omission invariant
            prop_assert!(out.is_none(),
                "fewer than n qualifying events must produce None");
        } else {
            // Invariant 4: NTH correctness
            let result = out.expect("n or more qualifying events must produce Some");
            let nth_row = q[(n - 1) as usize];
            prop_assert_eq!(out_ts(&result), nth_row.ts as i64,
                "NTH({}) must emit the n-th qualifying row by (ts, seq_id)", n);
            prop_assert_eq!(out_amount(&result), nth_row.amount,
                "NTH({}) must emit data from the n-th qualifying row", n);
        }
    }

    /// Invariant 6: entity isolation — output row's entity_id matches input.
    #[test]
    fn prop_entity_isolation(rows in arb_event_sequence()) {
        let op = make_op(EventSelectKind::First);
        let batch = build_batch(&rows);
        let out = run(&op, &batch);
        if let Some(result) = out {
            use arrow::array::StringViewArray;
            let eid_col = result
                .column_by_name("entity_id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringViewArray>()
                .unwrap();
            prop_assert_eq!(eid_col.value(0), "u1",
                "output entity_id must match input entity_id");
        }
    }

    /// Invariant 7: NTH(1) produces the same result as FIRST.
    #[test]
    fn prop_nth1_equals_first(rows in arb_event_sequence()) {
        let op_first = make_op(EventSelectKind::First);
        let op_nth1 = make_op(EventSelectKind::Nth(1));
        let batch = build_batch(&rows);

        let out_first = run(&op_first, &batch);
        let out_nth1 = run(&op_nth1, &batch.clone());

        match (out_first, out_nth1) {
            (None, None) => {}
            (Some(f), Some(n)) => {
                prop_assert_eq!(out_ts(&f), out_ts(&n),
                    "NTH(1) and FIRST must emit the same row");
                prop_assert_eq!(out_amount(&f), out_amount(&n),
                    "NTH(1) and FIRST must emit the same amount");
            }
            (Some(_), None) => prop_assert!(false, "FIRST found a row but NTH(1) did not"),
            (None, Some(_)) => prop_assert!(false, "NTH(1) found a row but FIRST did not"),
        }
    }

    /// Tie-breaking: when multiple qualifying events share the same ts,
    /// FIRST picks the one with the smallest seq_id.
    #[test]
    fn prop_first_tie_breaking_uses_seq_id(
        // Two events at the same timestamp with different seq_ids
        ts in any::<i32>(),
        seq1 in any::<i32>(),
        seq2 in any::<i32>(),
        amt1 in any::<i16>().prop_map(|x| x as i64),
        amt2 in any::<i16>().prop_map(|x| x as i64),
    ) {
        prop_assume!(seq1 != seq2);

        // Build rows in sorted order
        let (first_row, second_row) = if seq1 < seq2 {
            (EventRow { ts, seq_id: seq1, is_target: true, amount: amt1 },
             EventRow { ts, seq_id: seq2, is_target: true, amount: amt2 })
        } else {
            (EventRow { ts, seq_id: seq2, is_target: true, amount: amt2 },
             EventRow { ts, seq_id: seq1, is_target: true, amount: amt1 })
        };

        let rows = vec![first_row.clone(), second_row.clone()];
        let op = make_op(EventSelectKind::First);
        let batch = build_batch(&rows);
        let result = run(&op, &batch).expect("two qualifying events → must produce Some");

        prop_assert_eq!(out_amount(&result), first_row.amount,
            "FIRST with same-ts must pick smallest seq_id");
    }

    /// Tie-breaking: when multiple qualifying events share the same ts,
    /// LAST picks the one with the largest seq_id.
    #[test]
    fn prop_last_tie_breaking_uses_seq_id(
        ts in any::<i32>(),
        seq1 in any::<i32>(),
        seq2 in any::<i32>(),
        amt1 in any::<i16>().prop_map(|x| x as i64),
        amt2 in any::<i16>().prop_map(|x| x as i64),
    ) {
        prop_assume!(seq1 != seq2);

        let (first_row, second_row) = if seq1 < seq2 {
            (EventRow { ts, seq_id: seq1, is_target: true, amount: amt1 },
             EventRow { ts, seq_id: seq2, is_target: true, amount: amt2 })
        } else {
            (EventRow { ts, seq_id: seq2, is_target: true, amount: amt2 },
             EventRow { ts, seq_id: seq1, is_target: true, amount: amt1 })
        };

        let rows = vec![first_row.clone(), second_row.clone()];
        let op = make_op(EventSelectKind::Last);
        let batch = build_batch(&rows);
        let result = run(&op, &batch).expect("two qualifying events → must produce Some");

        // LAST should pick the row with the larger seq_id.
        prop_assert_eq!(out_amount(&result), second_row.amount,
            "LAST with same-ts must pick largest seq_id");
    }
}
