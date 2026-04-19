//! Property tests for `AttributeOperator` (TASK-431).
//!
//! Covers the §17.2 invariants from `docs/design/operators/attribute.md`:
//!
//! 1. **Every in-range conversion emits at least one row** (LEFT-UNNEST
//!    guarantee — §4.1 row shape 3).
//! 2. **`touchpoint_ts < conversion_ts`** — strict-at-conversion
//!    boundary (§5.1).
//! 3. **`touchpoint_ts >= conversion_ts - window`** — inclusive-at-
//!    lookback boundary (§5.1).
//! 4. **Same-event-type no self-attribution** — when `conversion == touchpoints`,
//!    no emitted row can have `touchpoint_ts` equal to the conversion row's
//!    own `ts`, because emission runs before the self-add step (§6).
//! 5. **Cardinality** — total output rows = `Σ max(qualifying_touchpoints, 1)`
//!    over all in-range conversions.
//!
//! Each property is checked against a brute-force reference evaluator
//! that re-implements the spec directly over the generated event
//! stream, independent of the operator's incremental deque.

use std::sync::Arc;

use arrow::array::{Array, StringViewArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use arrow::record_batch::RecordBatch;

use bqlite_ast::Span;
use bqlite_core::{BqlType, ColumnDef, EntityId, OperatorSchema};
use bqlite_operators::attribute::AttributeOperator;
use bqlite_operators::operator::EntityOperator;
use bqlite_planner::compiled::CompiledExpr;
use bqlite_planner::expr::{TypedExpr, TypedExprKind};
use bqlite_planner::physical::{AttributePhysical, ScanPhysical};
use bqlite_planner::PhysicalPlan;

use proptest::prelude::*;

// ─────────────────────────────────────────────────────────────────────────────
// Fixtures
// ─────────────────────────────────────────────────────────────────────────────

fn input_schema() -> OperatorSchema {
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

fn make_operator(conversion: &[&str], touchpoints: &[&str], window_ns: i64) -> AttributeOperator {
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
            output_schema: input_schema(),
            entity_key_col: "entity_id".into(),
            timestamp_col: "ts".into(),
        })),
        output_schema: output_schema(),
    };
    AttributeOperator::from_physical(&desc).expect("valid descriptor")
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

/// A single event in the property-test stream. `event_type` is a small
/// fixed inventory so conversion/touchpoint membership is easy to
/// control.
#[derive(Debug, Clone)]
struct Event {
    ts: i64,
    event_type: String,
    channel: Option<String>,
}

fn events_to_batch(events: &[Event]) -> RecordBatch {
    let entity_ids: Vec<&str> = events.iter().map(|_| "e").collect();
    let ts: Vec<i64> = events.iter().map(|e| e.ts).collect();
    let ev: Vec<&str> = events.iter().map(|e| e.event_type.as_str()).collect();
    let channel: Vec<Option<&str>> = events.iter().map(|e| e.channel.as_deref()).collect();
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
// Brute-force reference evaluator
// ─────────────────────────────────────────────────────────────────────────────

/// Reference implementation that re-applies §5.1/§6 semantics directly
/// over the full event stream. Returns one `(conversion_ts, touchpoint_ts)`
/// pair per emitted row (with `None` for LEFT-UNNEST rows), using the
/// same ascending-`touchpoint_ts` order the operator uses.
fn reference_emit(
    events: &[Event],
    conversion_set: &[&str],
    touchpoint_set: &[&str],
    window_ns: i64,
) -> Vec<(i64, Option<i64>)> {
    let mut out = Vec::new();
    for (i, e) in events.iter().enumerate() {
        if !conversion_set.contains(&e.event_type.as_str()) {
            continue;
        }
        // Walk all *earlier* events (by index so same-ts, same-event
        // cases follow the emit-before-add rule automatically).
        let mut qualifiers: Vec<i64> = Vec::new();
        for (j, candidate) in events.iter().enumerate().take(i) {
            if !touchpoint_set.contains(&candidate.event_type.as_str()) {
                continue;
            }
            let tp_ts = candidate.ts;
            let lookback = e.ts.saturating_sub(window_ns);
            // Strict at conversion, inclusive at lookback.
            if tp_ts >= lookback && tp_ts < e.ts {
                // `.take(i)` guarantees `j < i`; kept as a
                // `debug_assert!` so a future refactor of the walk
                // can't silently reintroduce self-attribution. §5.1's
                // strict-at-conversion rule (`tp_ts < e.ts`) is what
                // actually excludes the same-`ts` event.
                debug_assert!(j < i);
                qualifiers.push(tp_ts);
            }
        }
        qualifiers.sort_unstable();
        if qualifiers.is_empty() {
            out.push((e.ts, None));
        } else {
            for q in qualifiers {
                out.push((e.ts, Some(q)));
            }
        }
    }
    out
}

// ─────────────────────────────────────────────────────────────────────────────
// Strategy
// ─────────────────────────────────────────────────────────────────────────────

/// Event type inventory used by the strategy. The overlap of
/// conversion/touchpoint sets is chosen so the self-attribution
/// invariant is frequently exercised.
const TYPES: &[&str] = &["C", "T", "X"]; // C=conversion, T=touchpoint, X=both

fn event_strategy() -> impl Strategy<Value = Event> {
    (
        0i64..1000i64,
        0usize..TYPES.len(),
        prop::option::of("[a-z]{0,4}"),
    )
        .prop_map(|(ts, idx, channel)| Event {
            ts,
            event_type: TYPES[idx].to_string(),
            channel,
        })
}

fn stream_strategy() -> impl Strategy<Value = Vec<Event>> {
    prop::collection::vec(event_strategy(), 0..40).prop_map(|mut evs| {
        // Sort by ts to satisfy the EntityOperator adapter contract.
        // Ties remain in insertion order, which is deterministic.
        evs.sort_by_key(|e| e.ts);
        evs
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// Operator run helper
// ─────────────────────────────────────────────────────────────────────────────

fn run_operator(op: &AttributeOperator, events: &[Event]) -> Vec<(i64, Option<i64>)> {
    let mut state = op.create_state(&EntityId::String("e".into()));
    if !events.is_empty() {
        op.process_sub_batch(&mut state, &events_to_batch(events));
    }
    let Some(batch) = op.finish_entity(state) else {
        return Vec::new();
    };
    let conv = batch
        .column_by_name("conversion_ts")
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    let tp = batch
        .column_by_name("touchpoint_ts")
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    (0..batch.num_rows())
        .map(|i| {
            (
                conv.value(i),
                if tp.is_null(i) {
                    None
                } else {
                    Some(tp.value(i))
                },
            )
        })
        .collect()
}

// ─────────────────────────────────────────────────────────────────────────────
// Properties
// ─────────────────────────────────────────────────────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, .. ProptestConfig::default() })]

    /// Operator output matches the reference evaluator row-for-row
    /// (including ordering). Implies properties 1–5 of §17.2.
    #[test]
    fn operator_matches_reference(events in stream_strategy(), window in 0i64..500i64) {
        // Both C and X belong to the conversion set; both T and X belong
        // to the touchpoint set. This gives overlap between the lists so
        // the self-attribution invariant gets exercised.
        let conversion = &["C", "X"];
        let touchpoints = &["T", "X"];
        let op = make_operator(conversion, touchpoints, window);
        let actual = run_operator(&op, &events);
        let expected = reference_emit(&events, conversion, touchpoints, window);
        prop_assert_eq!(actual, expected);
    }

    /// Property 1 — every conversion emits at least one row.
    #[test]
    fn every_conversion_emits_at_least_one_row(
        events in stream_strategy(),
        window in 0i64..500i64,
    ) {
        let conversion = &["C", "X"];
        let touchpoints = &["T", "X"];
        let op = make_operator(conversion, touchpoints, window);
        let actual = run_operator(&op, &events);
        let conversion_count = events
            .iter()
            .filter(|e| conversion.contains(&e.event_type.as_str()))
            .count();
        let emitted_distinct_conversions = {
            let mut set = std::collections::BTreeSet::new();
            for (conv_ts, _) in &actual {
                set.insert(*conv_ts);
            }
            // We count distinct conversion timestamps that were emitted;
            // two conversions at the exact same ts are not
            // distinguishable downstream, so the LEFT-UNNEST guarantee
            // is stated per distinct conversion-ts.
            set.len()
        };
        let distinct_conversion_ts = {
            let mut set = std::collections::BTreeSet::new();
            for e in &events {
                if conversion.contains(&e.event_type.as_str()) {
                    set.insert(e.ts);
                }
            }
            set.len()
        };
        prop_assert_eq!(emitted_distinct_conversions, distinct_conversion_ts);
        // Also: total row count >= conversion event count.
        prop_assert!(actual.len() >= conversion_count);
    }

    /// Property 2/3 — window boundary predicates.
    #[test]
    fn window_boundaries_respected(
        events in stream_strategy(),
        window in 1i64..500i64,
    ) {
        let conversion = &["C", "X"];
        let touchpoints = &["T", "X"];
        let op = make_operator(conversion, touchpoints, window);
        let actual = run_operator(&op, &events);
        for (conv_ts, tp_ts) in actual {
            if let Some(tp) = tp_ts {
                prop_assert!(tp < conv_ts, "strict-at-conversion violated: {tp} !< {conv_ts}");
                prop_assert!(
                    tp >= conv_ts - window,
                    "inclusive-at-lookback violated: {tp} < {} (conv_ts={conv_ts}, window={window})",
                    conv_ts - window,
                );
            }
        }
    }

    /// Property 4 — no self-attribution when conversion and touchpoints
    /// share an event type.
    #[test]
    fn no_self_attribution(events in stream_strategy(), window in 1i64..500i64) {
        // Use the overlap type `X` exclusively so every event is both a
        // potential conversion and a potential touchpoint — the
        // strictest test of §6.
        let both = &["X"];
        let op = make_operator(both, both, window);
        let filtered: Vec<Event> = events
            .iter()
            .filter(|e| e.event_type == "X")
            .cloned()
            .collect();
        let actual = run_operator(&op, &filtered);
        // Count distinct conversion ts values.
        let distinct_conv_ts: std::collections::BTreeSet<i64> =
            filtered.iter().map(|e| e.ts).collect();
        // Each conversion ts must appear at least once in output.
        for ts in &distinct_conv_ts {
            prop_assert!(actual.iter().any(|(c, _)| c == ts));
        }
        // No row may attribute to a touchpoint at the same ts as its
        // conversion — that's the self-attribution we forbid.
        for (conv_ts, tp_ts) in actual {
            if let Some(tp) = tp_ts {
                prop_assert!(tp != conv_ts);
            }
        }
    }

    /// Property 5 — cardinality. Total output = Σ max(qualifiers, 1).
    #[test]
    fn cardinality_matches_spec(
        events in stream_strategy(),
        window in 0i64..500i64,
    ) {
        let conversion = &["C", "X"];
        let touchpoints = &["T", "X"];
        let op = make_operator(conversion, touchpoints, window);
        let actual = run_operator(&op, &events);
        let expected = reference_emit(&events, conversion, touchpoints, window);
        prop_assert_eq!(actual.len(), expected.len());
    }

    /// `window: 0s` forces every conversion to LEFT-UNNEST — the
    /// window interval `[conversion_ts, conversion_ts)` is empty, so no
    /// touchpoint can qualify. Explicit property on top of
    /// `operator_matches_reference` because this is the tightest
    /// boundary-condition regression an operator-side bug would hit
    /// first (§16.1 `window: 0s` row).
    #[test]
    fn window_zero_always_left_unnests(events in stream_strategy()) {
        let conversion = &["C", "X"];
        let touchpoints = &["T", "X"];
        let op = make_operator(conversion, touchpoints, 0);
        let actual = run_operator(&op, &events);
        for (_, tp) in actual {
            prop_assert!(tp.is_none(), "window=0 must never produce a qualifying touchpoint");
        }
    }

    /// Sub-batch boundary invariance — processing the stream in a
    /// single sub-batch must produce the same output as processing it
    /// split across arbitrary sub-batch boundaries (the deque state
    /// must persist across sub-batches per §15.2).
    #[test]
    fn sub_batch_boundary_invariance(
        events in stream_strategy(),
        window in 0i64..500i64,
        split_point in 0usize..40usize,
    ) {
        let conversion = &["C", "X"];
        let touchpoints = &["T", "X"];
        let op = make_operator(conversion, touchpoints, window);

        // Single batch.
        let single = run_operator(&op, &events);

        // Split batch.
        let effective_split = split_point.min(events.len());
        let mut state = op.create_state(&EntityId::String("e".into()));
        if effective_split > 0 {
            op.process_sub_batch(&mut state, &events_to_batch(&events[..effective_split]));
        }
        if effective_split < events.len() {
            op.process_sub_batch(&mut state, &events_to_batch(&events[effective_split..]));
        }
        let split = match op.finish_entity(state) {
            Some(batch) => {
                let conv = batch
                    .column_by_name("conversion_ts")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap();
                let tp = batch
                    .column_by_name("touchpoint_ts")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap();
                (0..batch.num_rows())
                    .map(|i| {
                        (
                            conv.value(i),
                            if tp.is_null(i) { None } else { Some(tp.value(i)) },
                        )
                    })
                    .collect::<Vec<_>>()
            }
            None => Vec::new(),
        };
        prop_assert_eq!(single, split);
    }
}
