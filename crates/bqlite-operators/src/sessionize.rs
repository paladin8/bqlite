//! SESSIONIZE operator: annotates events with per-entity `session_id` and
//! `session_duration` columns based on an inactivity gap and (optionally)
//! explicit end-event types.
//!
//! See `docs/design/operators/sessionize.md` for the authoritative
//! specification and `tasks/notes/TASK-405.md` for the pinned semantics.
//!
//! The operator is entity-streaming (implements [`EntityOperator`]) and is
//! wrapped by an `EntityOperatorAdapter` (landing with the first Wave 4
//! engine bind step) to surface as a [`PhysicalOperator`](crate::operator::PhysicalOperator).
//!
//! ## Semantics summary
//!
//! - Gap boundary is **exclusive** (strict `>`): `delta == gap_ns` keeps both
//!   events in the same session (sessionize.md §5.1).
//! - An end event **belongs to the session it closes** (§5.2).
//! - When a gap and an end-event coincide on the same row the **gap closes
//!   first** — the new event starts a fresh session which then closes
//!   immediately because it is an end event (§5.3).
//! - `session_id` resets to `1` at every entity boundary; it is **not**
//!   globally unique (§6.2).
//! - `session_duration == max_ts - min_ts` in nanoseconds; single-event
//!   sessions therefore have `session_duration == 0` (§6.3).
//! - The physical buffer holds only downstream-demanded columns; non-demanded
//!   input columns are still advertised in `output_schema` and are
//!   synthesised as null arrays at emission time to honour the schema
//!   contract without blowing up per-session memory (§9).
//! - Per-entity open-session event cap defaults to 1 M events. When the cap
//!   fires, the partial session is flushed and the remainder of the entity
//!   is skipped; the fact is recorded on the state for the adapter to
//!   surface as a warning (§11). The warning-channel plumbing itself is
//!   deferred (§11.4) — v1 exposes the flag on state.
//! - Fused aggregate shapes are deferred to Wave 5 (§10); `fused_aggregate`
//!   is always `None` on the physical descriptor.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, DictionaryArray, Int64Array, RecordBatch, StringViewArray};
use arrow::compute::{concat_batches, kernels::cast::cast};
use arrow::datatypes::{Int32Type, Schema as ArrowSchema, SchemaRef};

use bqlite_core::arrow::bql_type_to_arrow;
use bqlite_core::{EntityId, OperatorSchema};
use bqlite_planner::demand::DemandCapabilities;
use bqlite_planner::physical::SessionizePhysical;

use crate::operator::EntityOperator;

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────

/// Default per-entity open-session event cap (`docs/design/operators/sessionize.md` §11).
///
/// When `session_event_count` exceeds this value SESSIONIZE flushes the
/// partial session, sets the skip flag, and discards every remaining event
/// for the current entity.
pub const DEFAULT_SESSION_EVENT_CAP: usize = 1_000_000;

// ─────────────────────────────────────────────────────────────────────────────
// SessionizeInputMap
// ─────────────────────────────────────────────────────────────────────────────

/// Resolved input-batch column indices. Computed once at operator construction
/// from the child's advertised `OperatorSchema` (sessionize.md §4.1).
#[derive(Debug, Clone)]
struct SessionizeInputMap {
    /// Index of the `ts` column inside the input batch.
    ts_idx: usize,
    /// Index of the `event_type` column — only resolved when the operator
    /// has at least one end-event type. In gap-only mode this is `None`
    /// and the operator never inspects event types.
    event_type_idx: Option<usize>,
    /// Indices of columns that must be physically buffered in the session
    /// buffer (§9). These are the downstream-demanded forwarded columns.
    buffered_indices: Vec<usize>,
}

// ─────────────────────────────────────────────────────────────────────────────
// SessionizeOperator
// ─────────────────────────────────────────────────────────────────────────────

/// `EntityOperator` that annotates each event with `session_id` and
/// `session_duration` columns derived from an inactivity gap and an optional
/// set of explicit end-event types.
#[derive(Debug)]
pub struct SessionizeOperator {
    /// Minimum inactivity gap (nanoseconds). Boundary is exclusive: a new
    /// session starts iff `delta_ns(event_i, event_{i-1}) > gap_ns`.
    gap_ns: i64,

    /// Event types that explicitly end a session. Empty ⇒ gap-only mode.
    /// Stored in a `HashSet<String>` for O(1) membership.
    end_events: HashSet<String>,

    /// Cached sorted list of end-event names. Retained for diagnostics /
    /// debug formatting and as a deterministic audit surface; runtime
    /// membership testing uses `end_events` (HashSet) or the
    /// per-sub-batch dictionary code set.
    #[allow(dead_code)]
    end_events_list: Vec<String>,

    /// Output schema — `input schema ∪ {session_id, session_duration}`.
    output_schema: OperatorSchema,

    /// Child input-batch Arrow schema, used when slicing buffered sub-batches
    /// down to the forwarded-column subset.
    buffered_arrow_schema: SchemaRef,

    /// Full advertised output Arrow schema (used when assembling emitted
    /// session batches).
    output_arrow_schema: SchemaRef,

    /// Resolved column indices inside the input batch.
    input_columns: SessionizeInputMap,

    /// Names of the columns that must be physically forwarded (same set as
    /// `input_columns.buffered_indices`, in input-batch order). Drives
    /// `required_columns()`.
    required_column_names: Vec<String>,

    /// Output-schema column indices that are **not** physically buffered and
    /// must be emitted as typed null arrays (§9). Retained as an audit
    /// surface for tests and diagnostics; runtime uses
    /// `output_to_buffer_slot`.
    #[allow(dead_code)]
    null_padded_output_indices: Vec<usize>,

    /// Output-schema index of `session_id` — always in the advertised schema.
    session_id_out_idx: usize,
    /// Output-schema index of `session_duration`.
    session_duration_out_idx: usize,

    /// For every column in the output schema other than `session_id` and
    /// `session_duration`: the index within the physically-buffered column
    /// set, or `None` if the column must be null-padded at emission time.
    output_to_buffer_slot: Vec<Option<usize>>,

    /// Per-entity event cap (§11).
    session_event_cap: usize,
}

impl SessionizeOperator {
    /// Build from a [`SessionizePhysical`] descriptor. The engine bind step
    /// (landing with TASK-438) calls this.
    ///
    /// The child's advertised schema is taken from `desc.input.output_schema()`
    /// — this is the schema whose columns the operator resolves via name.
    pub fn new(desc: &SessionizePhysical) -> Self {
        Self::new_with_cap(desc, DEFAULT_SESSION_EVENT_CAP)
    }

    /// Variant of [`Self::new`] that lets tests override the per-entity cap.
    pub fn new_with_cap(desc: &SessionizePhysical, session_event_cap: usize) -> Self {
        assert!(
            desc.fused_aggregate.is_none(),
            "SESSIONIZE fused aggregates are deferred to Wave 5 (sessionize.md §10)"
        );

        let input_schema = desc.input.output_schema();

        // ── Resolve timestamp and event_type indices ─────────────────────
        let ts_idx = input_schema
            .column("ts")
            .map(|(i, _)| i)
            .expect("SESSIONIZE input schema must expose a `ts` column");

        let event_type_idx = if desc.end_events.is_empty() {
            None
        } else {
            Some(
                input_schema
                    .column("event_type")
                    .map(|(i, _)| i)
                    .expect(
                        "SESSIONIZE input schema must expose an `event_type` column when `end:` is configured",
                    ),
            )
        };

        // ── Determine physically-buffered columns ────────────────────────
        //
        // Policy (sessionize.md §9.2):
        //   buffered = {entity_id, ts} ∪ forwarded ∪ {event_type if end-events set}
        //
        // `entity_id` is always buffered — every downstream consumer
        // (MATCH, STATS, routing) needs it and it is part of the output
        // schema advertised as non-nullable. `ts` is always buffered
        // because SESSIONIZE itself uses it for gap computation and
        // duration. `event_type` is buffered only when end-events are
        // configured.
        let mut buffered_names: Vec<String> = Vec::new();
        let mut seen = HashSet::new();
        let push_name =
            |name: &str, buffered_names: &mut Vec<String>, seen: &mut HashSet<String>| {
                if seen.insert(name.to_string()) {
                    buffered_names.push(name.to_string());
                }
            };
        push_name("entity_id", &mut buffered_names, &mut seen);
        push_name("ts", &mut buffered_names, &mut seen);
        if event_type_idx.is_some() {
            push_name("event_type", &mut buffered_names, &mut seen);
        }
        for name in &desc.forwarded_columns {
            push_name(name, &mut buffered_names, &mut seen);
        }
        // Any NOT NULL column that appears in both the input schema and
        // the advertised output schema must be buffered: we can't emit a
        // null value for a non-nullable output column and a real pipeline
        // would never prune such a column without also dropping it from
        // the output schema. This is a planner-safety net, not a hot path.
        for col in desc.output_schema.columns() {
            if col.nullable {
                continue;
            }
            if col.name == "session_id" || col.name == "session_duration" {
                continue;
            }
            if input_schema.column(&col.name).is_some() {
                push_name(&col.name, &mut buffered_names, &mut seen);
            }
        }

        // Resolve indices for each buffered name against the input schema.
        let buffered_indices: Vec<usize> = buffered_names
            .iter()
            .map(|n| {
                input_schema.column(n).map(|(i, _)| i).unwrap_or_else(|| {
                    panic!("SESSIONIZE buffered column `{n}` missing from input schema")
                })
            })
            .collect();

        // ── Build buffered Arrow schema for concat operations ────────────
        let buffered_arrow_fields: Vec<_> = buffered_names
            .iter()
            .zip(buffered_indices.iter())
            .map(|(name, _idx)| {
                let (_, coldef) = input_schema.column(name).expect("column resolved above");
                arrow::datatypes::Field::new(
                    &coldef.name,
                    bql_type_to_arrow(&coldef.bql_type),
                    coldef.nullable,
                )
            })
            .collect();
        let buffered_arrow_schema: SchemaRef = Arc::new(ArrowSchema::new(buffered_arrow_fields));

        // ── Build full output Arrow schema ───────────────────────────────
        let output_arrow_fields: Vec<_> = desc
            .output_schema
            .columns()
            .iter()
            .map(|c| {
                arrow::datatypes::Field::new(&c.name, bql_type_to_arrow(&c.bql_type), c.nullable)
            })
            .collect();
        let output_arrow_schema: SchemaRef = Arc::new(ArrowSchema::new(output_arrow_fields));

        // ── Map each output-schema column back to its buffered slot ──────
        let (session_id_out_idx, _) = desc
            .output_schema
            .column("session_id")
            .expect("SESSIONIZE output schema must contain `session_id`");
        let (session_duration_out_idx, _) = desc
            .output_schema
            .column("session_duration")
            .expect("SESSIONIZE output schema must contain `session_duration`");

        let mut output_to_buffer_slot: Vec<Option<usize>> =
            Vec::with_capacity(desc.output_schema.columns().len());
        let mut null_padded_output_indices: Vec<usize> = Vec::new();
        for (out_idx, col) in desc.output_schema.columns().iter().enumerate() {
            if out_idx == session_id_out_idx || out_idx == session_duration_out_idx {
                output_to_buffer_slot.push(None);
                continue;
            }
            let slot = buffered_names.iter().position(|n| n == &col.name);
            if slot.is_none() {
                null_padded_output_indices.push(out_idx);
            }
            output_to_buffer_slot.push(slot);
        }

        // Sanity: operator declares `supports_forwarded_columns` so a
        // downstream demand set can include columns that SESSIONIZE
        // advertises logically but does not physically buffer. They will
        // be null-padded at emission time.

        let end_events: HashSet<String> = desc.end_events.iter().cloned().collect();
        let mut end_events_list: Vec<String> = end_events.iter().cloned().collect();
        end_events_list.sort();

        Self {
            gap_ns: desc.gap_ns,
            end_events,
            end_events_list,
            output_schema: desc.output_schema.clone(),
            buffered_arrow_schema,
            output_arrow_schema,
            input_columns: SessionizeInputMap {
                ts_idx,
                event_type_idx,
                buffered_indices,
            },
            required_column_names: buffered_names,
            null_padded_output_indices,
            session_id_out_idx,
            session_duration_out_idx,
            output_to_buffer_slot,
            session_event_cap,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SessionizeState
// ─────────────────────────────────────────────────────────────────────────────

/// Per-entity mutable state (sessionize.md §7.1).
#[derive(Debug)]
pub struct SessionizeState {
    /// Current session ID (starts at 1; increments at each session boundary).
    current_session_id: i64,

    /// Timestamp of the first event in the currently-open session. `i64::MIN`
    /// is the sentinel "no events seen yet for this entity".
    session_start_ts: i64,

    /// Timestamp of the most recent event in the currently-open session.
    session_last_ts: i64,

    /// Buffered `RecordBatch` slices for the currently-open session. Each
    /// entry carries only the buffered-column projection of the original
    /// input batch, already sliced down to the rows that belong to the
    /// still-open session.
    open_buffer: Vec<RecordBatch>,

    /// Number of events in the currently-open session (for cap enforcement).
    session_event_count: usize,

    /// Completed, fully-annotated per-session output batches that have not
    /// yet been flushed to the adapter. `finish_entity` consumes them.
    completed: Vec<RecordBatch>,

    /// Whether the per-entity event cap was exceeded — surfaces into a
    /// QueryWarning once the per-query warning channel lands (§11.4).
    cap_exceeded: bool,

    /// Once the cap fires we skip to the entity boundary.
    skipping: bool,
}

impl SessionizeState {
    /// Public accessor for the per-entity cap flag. Used by the
    /// `EntityOperatorAdapter` (landing later) to surface a `QueryWarning`.
    pub fn cap_exceeded(&self) -> bool {
        self.cap_exceeded
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EndEventCodeSet (dictionary fast path)
// ─────────────────────────────────────────────────────────────────────────────

/// Precomputed set of dictionary codes in a batch whose string value is an
/// end-event type. Resolved once per sub-batch (§8.2).
struct EndEventCodeSet {
    matching_codes: HashSet<i32>,
}

impl EndEventCodeSet {
    fn build(dict: &DictionaryArray<Int32Type>, end_events: &HashSet<String>) -> Self {
        let mut matching_codes = HashSet::new();
        // Try Utf8View first (canonical bqlite layout), fall back to Utf8.
        if let Some(values) = dict.values().as_any().downcast_ref::<StringViewArray>() {
            for code in 0..values.len() {
                if values.is_valid(code) && end_events.contains(values.value(code)) {
                    matching_codes.insert(code as i32);
                }
            }
        } else if let Some(values) = dict
            .values()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
        {
            for code in 0..values.len() {
                if values.is_valid(code) && end_events.contains(values.value(code)) {
                    matching_codes.insert(code as i32);
                }
            }
        }
        Self { matching_codes }
    }

    #[inline]
    fn contains_code(&self, code: i32) -> bool {
        self.matching_codes.contains(&code)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EventTypeView
// ─────────────────────────────────────────────────────────────────────────────

/// Per-sub-batch view of the `event_type` column. Supports the two layouts
/// that show up in practice: `StringViewArray` (canonical) and
/// `DictionaryArray<Int32, Utf8View>` (scan-side code-preserving layout).
enum EventTypeView<'a> {
    Strings(&'a StringViewArray),
    Dictionary {
        keys: &'a arrow::array::PrimitiveArray<Int32Type>,
        codes: EndEventCodeSet,
    },
}

impl<'a> EventTypeView<'a> {
    fn resolve(col: &'a ArrayRef, end_events: &HashSet<String>) -> Option<Self> {
        if let Some(s) = col.as_any().downcast_ref::<StringViewArray>() {
            return Some(EventTypeView::Strings(s));
        }
        if let Some(d) = col.as_any().downcast_ref::<DictionaryArray<Int32Type>>() {
            let codes = EndEventCodeSet::build(d, end_events);
            return Some(EventTypeView::Dictionary {
                keys: d.keys(),
                codes,
            });
        }
        None
    }

    #[inline]
    fn is_end_event(&self, row: usize, end_events: &HashSet<String>) -> bool {
        match self {
            EventTypeView::Strings(s) => {
                if s.is_null(row) {
                    false
                } else {
                    end_events.contains(s.value(row))
                }
            }
            EventTypeView::Dictionary { keys, codes } => {
                if keys.is_null(row) {
                    false
                } else {
                    codes.contains_code(keys.value(row))
                }
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EntityOperator implementation
// ─────────────────────────────────────────────────────────────────────────────

impl EntityOperator for SessionizeOperator {
    type State = SessionizeState;

    fn create_state(&self, _entity_id: &EntityId) -> Self::State {
        SessionizeState {
            current_session_id: 1,
            session_start_ts: i64::MIN,
            session_last_ts: i64::MIN,
            open_buffer: Vec::new(),
            session_event_count: 0,
            completed: Vec::new(),
            cap_exceeded: false,
            skipping: false,
        }
    }

    fn output_schema(&self) -> &OperatorSchema {
        &self.output_schema
    }

    fn process_sub_batch(&self, state: &mut SessionizeState, batch: &RecordBatch) {
        if state.skipping {
            return;
        }
        if batch.num_rows() == 0 {
            return;
        }

        // Resolve timestamps — prefer `TimestampNanosecondArray`, fall back
        // to `Int64Array` for ergonomic test construction. The indices are
        // fixed at construction time, so downcast errors here are
        // programming errors (schema drift in the child operator).
        let ts_col = batch.column(self.input_columns.ts_idx);
        let timestamps: &[i64] = if let Some(arr) = ts_col
            .as_any()
            .downcast_ref::<arrow::array::TimestampNanosecondArray>(
        ) {
            arr.values().as_ref()
        } else if let Some(arr) = ts_col.as_any().downcast_ref::<Int64Array>() {
            arr.values().as_ref()
        } else {
            panic!(
                "SESSIONIZE: `ts` column has unexpected Arrow type {:?}",
                ts_col.data_type()
            );
        };

        // Resolve event_type view when end-events are configured.
        let event_type_view: Option<EventTypeView> = self.input_columns.event_type_idx.map(|idx| {
            EventTypeView::resolve(batch.column(idx), &self.end_events).unwrap_or_else(|| {
                panic!(
                    "SESSIONIZE: unsupported event_type layout {:?}",
                    batch.column(idx).data_type()
                )
            })
        });

        // Scratch: the row range inside this sub-batch that belongs to the
        // currently-open session. We emit it into `state.open_buffer` once
        // the session either stays open past the last row of the batch or
        // closes at some row `r` (at which point we push the [slice_start..=r]
        // slice, flush, and reset `slice_start = r+1`).
        let num_rows = batch.num_rows();
        let mut slice_start: usize = 0;

        // `row` indexes into both `timestamps` and `event_type_view`; the
        // needless-range-loop lint's enumerate() rewrite would need two
        // parallel iterators, which is less readable than the index form.
        #[allow(clippy::needless_range_loop)]
        for row in 0..num_rows {
            let ts = timestamps[row];

            // ── Initialization for the first event of the entity ─────
            if state.session_start_ts == i64::MIN {
                state.session_start_ts = ts;
                state.session_last_ts = ts;
                state.session_event_count = 1;

                // If the first event is itself an end event it closes
                // a 1-event session right away (§5.2 applied at start).
                if let Some(view) = &event_type_view {
                    if view.is_end_event(row, &self.end_events) {
                        // Slice [slice_start..=row] into open buffer,
                        // then flush.
                        self.push_slice(state, batch, slice_start, row + 1);
                        self.flush_session(state);
                        slice_start = row + 1;
                    }
                }
                continue;
            }

            // ── Gap boundary check ──────────────────────────────────
            let delta = ts.saturating_sub(state.session_last_ts);
            if delta > self.gap_ns {
                // Close the currently-open session up to (but excluding)
                // the current row, then start a fresh session at `row`.
                if row > slice_start {
                    self.push_slice(state, batch, slice_start, row);
                }
                self.flush_session(state);
                slice_start = row;
                // Initialize the new session with this event.
                state.session_start_ts = ts;
                state.session_last_ts = ts;
                state.session_event_count = 1;

                // If this event is an end event the fresh session closes
                // immediately — 1-event session (§5.3).
                if let Some(view) = &event_type_view {
                    if view.is_end_event(row, &self.end_events) {
                        self.push_slice(state, batch, slice_start, row + 1);
                        self.flush_session(state);
                        slice_start = row + 1;
                    }
                }
                continue;
            }

            // ── Event stays in the current open session ─────────────
            state.session_last_ts = ts;
            state.session_event_count += 1;

            // Per-entity event cap (§11). When exceeded we flush what
            // we have (including the offending row) and skip forward.
            if state.session_event_count > self.session_event_cap {
                if row >= slice_start {
                    self.push_slice(state, batch, slice_start, row + 1);
                }
                self.flush_session(state);
                state.cap_exceeded = true;
                state.skipping = true;
                return;
            }

            // End-event membership (§5.2).
            if let Some(view) = &event_type_view {
                if view.is_end_event(row, &self.end_events) {
                    self.push_slice(state, batch, slice_start, row + 1);
                    self.flush_session(state);
                    slice_start = row + 1;
                }
            }
        }

        // Trailing open-session rows carry into the next sub-batch.
        if slice_start < num_rows {
            self.push_slice(state, batch, slice_start, num_rows);
        }
    }

    fn finish_entity(&self, mut state: SessionizeState) -> Option<RecordBatch> {
        // Flush the final open session if any rows remain. `skipping` means
        // the final session was already flushed at cap time, so skip this
        // step.
        if !state.skipping && state.session_start_ts != i64::MIN {
            self.flush_session(&mut state);
        }

        if state.completed.is_empty() {
            return None;
        }

        // Concatenate all per-session output batches into a single
        // multi-row RecordBatch (sessionize.md §8.4).
        let concatenated =
            concat_batches(&self.output_arrow_schema, state.completed.iter()).ok()?;
        Some(concatenated)
    }

    fn required_columns(&self) -> &[String] {
        &self.required_column_names
    }

    fn supported_demands(&self) -> DemandCapabilities {
        DemandCapabilities {
            supports_forwarded_columns: true,
            ..DemandCapabilities::none()
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

impl SessionizeOperator {
    /// Append `batch.slice(offset, len)` projected down to the buffered
    /// column set into `state.open_buffer`. No-op when `len == 0`.
    ///
    /// Columns are slice-referenced (zero-copy) when their Arrow type
    /// matches the buffered schema exactly. When a column arrives in a
    /// compressed shape (e.g. `DictionaryArray<Int32, Utf8View>` for an
    /// input-schema `String` column) we decode it to the canonical shape
    /// so that `concat_batches` at session-close time can stitch slices
    /// across sub-batches without a schema mismatch. Decoding happens
    /// once per sub-batch, not per row.
    fn push_slice(
        &self,
        state: &mut SessionizeState,
        batch: &RecordBatch,
        row_start: usize,
        row_end: usize,
    ) {
        if row_end <= row_start {
            return;
        }
        let len = row_end - row_start;
        let columns: Vec<ArrayRef> = self
            .input_columns
            .buffered_indices
            .iter()
            .enumerate()
            .map(|(buf_idx, &i)| {
                let src = batch.column(i).slice(row_start, len);
                let expected_dt = self.buffered_arrow_schema.field(buf_idx).data_type();
                if src.data_type() == expected_dt {
                    src
                } else {
                    cast(src.as_ref(), expected_dt).expect(
                        "input column must be castable to the canonical buffered Arrow type",
                    )
                }
            })
            .collect();
        let sliced = RecordBatch::try_new(self.buffered_arrow_schema.clone(), columns)
            .expect("buffered schema matches the sliced columns by construction");
        state.open_buffer.push(sliced);
    }

    /// Finalise the currently-open session: compute `session_duration`,
    /// materialise a full output batch matching `output_arrow_schema` with
    /// `session_id` / `session_duration` as constant columns and
    /// un-forwarded columns as typed null arrays.
    fn flush_session(&self, state: &mut SessionizeState) {
        if state.open_buffer.is_empty() {
            // Nothing buffered — either cap already flushed at this row
            // or a pathological empty session. Reset bookkeeping below.
            state.session_start_ts = i64::MIN;
            state.session_last_ts = i64::MIN;
            state.session_event_count = 0;
            state.current_session_id += 1;
            return;
        }

        // Concatenate buffered sub-batch slices.
        let buffered = concat_batches(&self.buffered_arrow_schema, state.open_buffer.iter())
            .expect("all slices share buffered_arrow_schema by construction");
        let num_rows = buffered.num_rows();
        let session_id = state.current_session_id;
        let duration_ns = state.session_last_ts.saturating_sub(state.session_start_ts);

        // Assemble the output RecordBatch column-by-column in
        // `output_schema` order.
        let mut out_columns: Vec<ArrayRef> =
            Vec::with_capacity(self.output_arrow_schema.fields().len());
        for (out_idx, coldef) in self.output_schema.columns().iter().enumerate() {
            if out_idx == self.session_id_out_idx {
                out_columns.push(Arc::new(Int64Array::from(vec![session_id; num_rows])));
                continue;
            }
            if out_idx == self.session_duration_out_idx {
                out_columns.push(Arc::new(Int64Array::from(vec![duration_ns; num_rows])));
                continue;
            }
            match self.output_to_buffer_slot[out_idx] {
                Some(buf_idx) => {
                    // Buffered column — forward as-is, but adapt types if
                    // the buffered array does not already match the output
                    // Arrow field (e.g. Int64 → Timestamp). This can happen
                    // in test fixtures that hand us a plain `Int64Array`
                    // for `ts`; production pipelines always see the
                    // canonical Arrow type.
                    let src = buffered.column(buf_idx);
                    let target_dt = self.output_arrow_schema.field(out_idx).data_type();
                    if src.data_type() == target_dt {
                        out_columns.push(src.clone());
                    } else {
                        let adapted = cast(src.as_ref(), target_dt)
                            .expect("buffered column must be castable to the output Arrow type");
                        out_columns.push(adapted);
                    }
                }
                None => {
                    // Not buffered — emit a typed null array (§9.1).
                    out_columns.push(arrow::array::new_null_array(
                        self.output_arrow_schema.field(out_idx).data_type(),
                        num_rows,
                    ));
                }
            }
            let _ = coldef; // silence unused warning; the `coldef` exists for doc intent.
        }

        let annotated = RecordBatch::try_new(self.output_arrow_schema.clone(), out_columns)
            .expect("constructed columns match output_arrow_schema by construction");
        state.completed.push(annotated);

        // Reset for the next session on this entity.
        state.open_buffer.clear();
        state.current_session_id += 1;
        state.session_event_count = 0;
        state.session_start_ts = i64::MIN;
        state.session_last_ts = i64::MIN;
    }

    /// Test helper: access `null_padded_output_indices` (used by integration-style
    /// tests to assert that un-demanded columns are emitted as null arrays).
    #[cfg(test)]
    fn null_padded_output_indices(&self) -> &[usize] {
        &self.null_padded_output_indices
    }

    /// Test helper: access the buffered Arrow schema.
    #[cfg(test)]
    fn buffered_arrow_schema(&self) -> &SchemaRef {
        &self.buffered_arrow_schema
    }

    /// Test helper: access the sorted end-events list.
    #[cfg(test)]
    fn end_events_list(&self) -> &[String] {
        &self.end_events_list
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{
        ArrayRef, DictionaryArray, Int32Array, Int64Array, StringViewArray,
        TimestampNanosecondArray,
    };
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
    use arrow::record_batch::RecordBatch;

    use bqlite_core::{BqlType, ColumnDef, EntityId, OperatorSchema};
    use bqlite_planner::logical::LogicalPlan;
    use bqlite_planner::physical::{lower_physical, PhysicalPlan};

    // ── Fixtures ─────────────────────────────────────────────────────────

    fn events_schema_no_amount() -> OperatorSchema {
        OperatorSchema::new(vec![
            ColumnDef::required("entity_id", BqlType::String),
            ColumnDef::required("ts", BqlType::Timestamp),
            ColumnDef::required("event_type", BqlType::String),
        ])
        .unwrap()
    }

    fn events_schema_with_amount() -> OperatorSchema {
        OperatorSchema::new(vec![
            ColumnDef::required("entity_id", BqlType::String),
            ColumnDef::required("ts", BqlType::Timestamp),
            ColumnDef::required("event_type", BqlType::String),
            ColumnDef::nullable("amount", BqlType::Int),
        ])
        .unwrap()
    }

    fn sess_output_schema(base: &OperatorSchema) -> OperatorSchema {
        let mut cols = base.columns().to_vec();
        cols.push(ColumnDef::required("session_id", BqlType::Int));
        cols.push(ColumnDef::required("session_duration", BqlType::Int));
        OperatorSchema::new(cols).unwrap()
    }

    /// Build a `SessionizePhysical` rooted at a scan over the given input
    /// schema.
    fn build_physical(
        input_schema: OperatorSchema,
        gap_ns: i64,
        end_events: Vec<String>,
        forwarded: Vec<String>,
    ) -> SessionizePhysical {
        let scan = LogicalPlan::scan(
            bqlite_core::TableSchema::new(
                "events",
                input_schema.columns().to_vec(),
                "entity_id",
                "ts",
                "event_type",
            )
            .unwrap(),
        );
        let os = sess_output_schema(&input_schema);
        let node = LogicalPlan::Sessionize {
            gap: gap_ns,
            end_events,
            forwarded_columns: forwarded,
            fused_downstream: None,
            input: Box::new(scan),
            output_schema: os,
        };
        let physical = lower_physical(node, 0);
        match physical {
            PhysicalPlan::Sessionize(s) => *{
                // unbox
                Box::new(s)
            },
            _ => panic!("expected Sessionize"),
        }
    }

    fn make_batch(
        input_schema: &ArrowSchema,
        entity: &str,
        ts: Vec<i64>,
        event_type: Vec<&str>,
        amount: Option<Vec<Option<i64>>>,
    ) -> RecordBatch {
        let num = ts.len();
        let mut cols: Vec<ArrayRef> = Vec::new();
        cols.push(Arc::new(StringViewArray::from(vec![entity; num])));
        cols.push(Arc::new(
            TimestampNanosecondArray::from(ts).with_timezone("UTC"),
        ));
        cols.push(Arc::new(StringViewArray::from(event_type)));
        if let Some(a) = amount {
            cols.push(Arc::new(Int64Array::from(a)));
        }
        RecordBatch::try_new(Arc::new(input_schema.clone()), cols).unwrap()
    }

    fn input_arrow(schema: &OperatorSchema) -> ArrowSchema {
        let fields: Vec<Field> = schema
            .columns()
            .iter()
            .map(|c| Field::new(&c.name, bql_type_to_arrow(&c.bql_type), c.nullable))
            .collect();
        ArrowSchema::new(fields)
    }

    fn read_session_columns(out: &RecordBatch) -> (Vec<i64>, Vec<i64>) {
        let sid = out
            .column_by_name("session_id")
            .expect("session_id present")
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let sdur = out
            .column_by_name("session_duration")
            .expect("session_duration present")
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let sids = (0..sid.len()).map(|i| sid.value(i)).collect();
        let sdurs = (0..sdur.len()).map(|i| sdur.value(i)).collect();
        (sids, sdurs)
    }

    fn read_ts(out: &RecordBatch) -> Vec<i64> {
        let col = out.column_by_name("ts").unwrap();
        let arr = col
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        (0..arr.len()).map(|i| arr.value(i)).collect()
    }

    // ── Empty / trivial cases ────────────────────────────────────────────

    #[test]
    fn empty_entity_produces_no_output() {
        let schema = events_schema_no_amount();
        let phys = build_physical(schema, 1_000, vec![], vec![]);
        let op = SessionizeOperator::new(&phys);
        let state = op.create_state(&EntityId::from("e1"));
        assert!(op.finish_entity(state).is_none());
    }

    #[test]
    fn single_event_is_single_session_with_duration_zero() {
        let schema = events_schema_no_amount();
        let phys = build_physical(schema.clone(), 1_000, vec![], vec![]);
        let op = SessionizeOperator::new(&phys);
        let arrow_schema = input_arrow(&schema);

        let mut state = op.create_state(&EntityId::from("e1"));
        let batch = make_batch(&arrow_schema, "e1", vec![100], vec!["click"], None);
        op.process_sub_batch(&mut state, &batch);
        let out = op.finish_entity(state).expect("one row");
        let (sids, sdurs) = read_session_columns(&out);
        assert_eq!(sids, vec![1]);
        assert_eq!(sdurs, vec![0]);
        assert_eq!(out.num_rows(), 1);
    }

    #[test]
    fn all_within_gap_becomes_one_session() {
        let schema = events_schema_no_amount();
        let phys = build_physical(schema.clone(), 100, vec![], vec![]);
        let op = SessionizeOperator::new(&phys);
        let arrow_schema = input_arrow(&schema);

        let mut state = op.create_state(&EntityId::from("e1"));
        // deltas: 50, 50, 50 — all <= gap → one session.
        let batch = make_batch(
            &arrow_schema,
            "e1",
            vec![0, 50, 100, 150],
            vec!["a", "b", "c", "d"],
            None,
        );
        op.process_sub_batch(&mut state, &batch);
        let out = op.finish_entity(state).unwrap();
        let (sids, sdurs) = read_session_columns(&out);
        assert_eq!(sids, vec![1, 1, 1, 1]);
        assert_eq!(sdurs, vec![150, 150, 150, 150]);
    }

    #[test]
    fn delta_equal_to_gap_stays_same_session() {
        // sessionize.md §5.1: boundary is strict `>`. delta == gap ⇒ same.
        let schema = events_schema_no_amount();
        let phys = build_physical(schema.clone(), 100, vec![], vec![]);
        let op = SessionizeOperator::new(&phys);
        let arrow_schema = input_arrow(&schema);

        let mut state = op.create_state(&EntityId::from("e1"));
        let batch = make_batch(
            &arrow_schema,
            "e1",
            vec![0, 100, 200],
            vec!["a", "b", "c"],
            None,
        );
        op.process_sub_batch(&mut state, &batch);
        let out = op.finish_entity(state).unwrap();
        let (sids, _) = read_session_columns(&out);
        assert_eq!(sids, vec![1, 1, 1]);
    }

    #[test]
    fn delta_one_ns_over_gap_starts_new_session() {
        let schema = events_schema_no_amount();
        let phys = build_physical(schema.clone(), 100, vec![], vec![]);
        let op = SessionizeOperator::new(&phys);
        let arrow_schema = input_arrow(&schema);

        let mut state = op.create_state(&EntityId::from("e1"));
        let batch = make_batch(&arrow_schema, "e1", vec![0, 101], vec!["a", "b"], None);
        op.process_sub_batch(&mut state, &batch);
        let out = op.finish_entity(state).unwrap();
        let (sids, sdurs) = read_session_columns(&out);
        assert_eq!(sids, vec![1, 2]);
        assert_eq!(sdurs, vec![0, 0]);
    }

    #[test]
    fn all_events_separated_produces_singleton_sessions() {
        let schema = events_schema_no_amount();
        let phys = build_physical(schema.clone(), 10, vec![], vec![]);
        let op = SessionizeOperator::new(&phys);
        let arrow_schema = input_arrow(&schema);
        let mut state = op.create_state(&EntityId::from("e1"));
        let batch = make_batch(
            &arrow_schema,
            "e1",
            vec![0, 100, 200, 300],
            vec!["a", "b", "c", "d"],
            None,
        );
        op.process_sub_batch(&mut state, &batch);
        let out = op.finish_entity(state).unwrap();
        let (sids, sdurs) = read_session_columns(&out);
        assert_eq!(sids, vec![1, 2, 3, 4]);
        assert_eq!(sdurs, vec![0, 0, 0, 0]);
    }

    // ── End-event handling ───────────────────────────────────────────────

    #[test]
    fn end_event_belongs_to_closed_session() {
        // sessionize.md §5.2 — `search THEN logout` resolves to a single
        // session including the logout.
        let schema = events_schema_no_amount();
        let phys = build_physical(
            schema.clone(),
            1_000_000_000, // effectively infinite gap
            vec!["logout".into()],
            vec![],
        );
        let op = SessionizeOperator::new(&phys);
        let arrow_schema = input_arrow(&schema);
        let mut state = op.create_state(&EntityId::from("e1"));
        let batch = make_batch(
            &arrow_schema,
            "e1",
            vec![0, 5, 10, 15],
            vec!["search", "logout", "login", "click"],
            None,
        );
        op.process_sub_batch(&mut state, &batch);
        let out = op.finish_entity(state).unwrap();
        let (sids, sdurs) = read_session_columns(&out);
        // rows 0-1 in session 1, rows 2-3 in session 2.
        assert_eq!(sids, vec![1, 1, 2, 2]);
        assert_eq!(sdurs, vec![5, 5, 5, 5]);
    }

    #[test]
    fn end_event_as_first_event_of_entity_is_singleton_session() {
        let schema = events_schema_no_amount();
        let phys = build_physical(schema.clone(), 1_000, vec!["logout".into()], vec![]);
        let op = SessionizeOperator::new(&phys);
        let arrow_schema = input_arrow(&schema);
        let mut state = op.create_state(&EntityId::from("e1"));
        let batch = make_batch(
            &arrow_schema,
            "e1",
            vec![0, 10, 20],
            vec!["logout", "click", "click"],
            None,
        );
        op.process_sub_batch(&mut state, &batch);
        let out = op.finish_entity(state).unwrap();
        let (sids, _) = read_session_columns(&out);
        assert_eq!(sids, vec![1, 2, 2]);
    }

    #[test]
    fn end_event_as_last_event_closes_session_cleanly() {
        let schema = events_schema_no_amount();
        let phys = build_physical(schema.clone(), 1_000, vec!["logout".into()], vec![]);
        let op = SessionizeOperator::new(&phys);
        let arrow_schema = input_arrow(&schema);
        let mut state = op.create_state(&EntityId::from("e1"));
        let batch = make_batch(
            &arrow_schema,
            "e1",
            vec![0, 10, 20],
            vec!["click", "click", "logout"],
            None,
        );
        op.process_sub_batch(&mut state, &batch);
        let out = op.finish_entity(state).unwrap();
        let (sids, sdurs) = read_session_columns(&out);
        assert_eq!(sids, vec![1, 1, 1]);
        assert_eq!(sdurs, vec![20, 20, 20]);
    }

    #[test]
    fn gap_and_end_event_on_same_row_gap_closes_first() {
        // sessionize.md §5.3 — the prior session closes due to gap; the
        // event starts a fresh 1-event session that closes via end-event.
        let schema = events_schema_no_amount();
        let phys = build_physical(schema.clone(), 10, vec!["logout".into()], vec![]);
        let op = SessionizeOperator::new(&phys);
        let arrow_schema = input_arrow(&schema);
        let mut state = op.create_state(&EntityId::from("e1"));
        let batch = make_batch(
            &arrow_schema,
            "e1",
            // delta between row1 (ts=5) and row2 (ts=100) is 95 > gap=10.
            vec![0, 5, 100],
            vec!["click", "click", "logout"],
            None,
        );
        op.process_sub_batch(&mut state, &batch);
        let out = op.finish_entity(state).unwrap();
        let (sids, sdurs) = read_session_columns(&out);
        assert_eq!(sids, vec![1, 1, 2]);
        assert_eq!(sdurs, vec![5, 5, 0]);
    }

    #[test]
    fn multiple_end_events_in_list_all_close() {
        let schema = events_schema_no_amount();
        let phys = build_physical(
            schema.clone(),
            1_000_000_000,
            vec!["logout".into(), "timeout".into()],
            vec![],
        );
        let op = SessionizeOperator::new(&phys);
        let arrow_schema = input_arrow(&schema);
        let mut state = op.create_state(&EntityId::from("e1"));
        let batch = make_batch(
            &arrow_schema,
            "e1",
            vec![0, 1, 2, 3],
            vec!["a", "timeout", "b", "logout"],
            None,
        );
        op.process_sub_batch(&mut state, &batch);
        let out = op.finish_entity(state).unwrap();
        let (sids, _) = read_session_columns(&out);
        assert_eq!(sids, vec![1, 1, 2, 2]);
    }

    // ── Dictionary fast path ─────────────────────────────────────────────

    #[test]
    fn dictionary_event_type_resolves_end_events_via_codes() {
        // Build a batch whose event_type column is a
        // DictionaryArray<Int32, Utf8View>. Same semantics as the
        // StringViewArray tests — any dictionary code whose resolved
        // string is in `end_events` must close the session.
        let schema = events_schema_no_amount();
        let phys = build_physical(schema.clone(), 1_000_000_000, vec!["logout".into()], vec![]);
        let op = SessionizeOperator::new(&phys);

        let values = Arc::new(StringViewArray::from(vec!["click", "logout", "other"]));
        let keys = Int32Array::from(vec![0, 0, 1, 2]);
        let dict = DictionaryArray::<Int32Type>::try_new(keys, values).unwrap();

        let fields = vec![
            Field::new("entity_id", DataType::Utf8View, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new(
                "event_type",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8View)),
                false,
            ),
        ];
        let arrow_schema = Arc::new(ArrowSchema::new(fields));
        let cols: Vec<ArrayRef> = vec![
            Arc::new(StringViewArray::from(vec!["e1"; 4])),
            Arc::new(TimestampNanosecondArray::from(vec![0, 5, 10, 15]).with_timezone("UTC")),
            Arc::new(dict),
        ];
        let batch = RecordBatch::try_new(arrow_schema, cols).unwrap();

        let mut state = op.create_state(&EntityId::from("e1"));
        op.process_sub_batch(&mut state, &batch);
        let out = op.finish_entity(state).unwrap();
        let (sids, _) = read_session_columns(&out);
        // rows 0,1 (click, click) then row 2 is logout (end), row 3 (other)
        // starts session 2 and is the lone row in it.
        assert_eq!(sids, vec![1, 1, 1, 2]);
    }

    // ── Entity boundary and cross-sub-batch behaviour ────────────────────

    #[test]
    fn entity_state_resets_to_session_one() {
        let schema = events_schema_no_amount();
        let phys = build_physical(schema.clone(), 5, vec![], vec![]);
        let op = SessionizeOperator::new(&phys);
        let arrow_schema = input_arrow(&schema);

        // Entity e1 produces 2 sessions; the next entity e2 must start at 1.
        let mut s1 = op.create_state(&EntityId::from("e1"));
        op.process_sub_batch(
            &mut s1,
            &make_batch(&arrow_schema, "e1", vec![0, 100], vec!["a", "b"], None),
        );
        let out1 = op.finish_entity(s1).unwrap();
        let (sids1, _) = read_session_columns(&out1);
        assert_eq!(sids1, vec![1, 2]);

        let mut s2 = op.create_state(&EntityId::from("e2"));
        op.process_sub_batch(
            &mut s2,
            &make_batch(&arrow_schema, "e2", vec![0, 1], vec!["a", "b"], None),
        );
        let out2 = op.finish_entity(s2).unwrap();
        let (sids2, _) = read_session_columns(&out2);
        assert_eq!(sids2, vec![1, 1]);
    }

    #[test]
    fn session_spans_multiple_sub_batches() {
        let schema = events_schema_no_amount();
        let phys = build_physical(schema.clone(), 100, vec![], vec![]);
        let op = SessionizeOperator::new(&phys);
        let arrow_schema = input_arrow(&schema);

        let mut state = op.create_state(&EntityId::from("e1"));
        // Sub-batch 1: rows at ts 0, 50, 100 — all one session (deltas 50).
        op.process_sub_batch(
            &mut state,
            &make_batch(
                &arrow_schema,
                "e1",
                vec![0, 50, 100],
                vec!["a", "b", "c"],
                None,
            ),
        );
        // Sub-batch 2: rows at ts 150, 200 — still same session (deltas 50).
        op.process_sub_batch(
            &mut state,
            &make_batch(&arrow_schema, "e1", vec![150, 200], vec!["d", "e"], None),
        );
        let out = op.finish_entity(state).unwrap();
        let (sids, sdurs) = read_session_columns(&out);
        assert_eq!(sids, vec![1, 1, 1, 1, 1]);
        assert_eq!(sdurs, vec![200; 5]);
        assert_eq!(read_ts(&out), vec![0, 50, 100, 150, 200]);
    }

    #[test]
    fn session_boundary_across_sub_batches() {
        let schema = events_schema_no_amount();
        let phys = build_physical(schema.clone(), 10, vec![], vec![]);
        let op = SessionizeOperator::new(&phys);
        let arrow_schema = input_arrow(&schema);

        let mut state = op.create_state(&EntityId::from("e1"));
        op.process_sub_batch(
            &mut state,
            &make_batch(&arrow_schema, "e1", vec![0, 5], vec!["a", "b"], None),
        );
        // First event of the next sub-batch arrives with delta=100 > gap.
        op.process_sub_batch(
            &mut state,
            &make_batch(&arrow_schema, "e1", vec![105, 110], vec!["c", "d"], None),
        );
        let out = op.finish_entity(state).unwrap();
        let (sids, _) = read_session_columns(&out);
        assert_eq!(sids, vec![1, 1, 2, 2]);
    }

    // ── Per-entity event cap ─────────────────────────────────────────────

    #[test]
    fn per_entity_event_cap_flushes_partial_and_skips_rest() {
        let schema = events_schema_no_amount();
        let phys = build_physical(schema.clone(), 1_000_000, vec![], vec![]);
        let mut op = SessionizeOperator::new(&phys);
        // Force the cap down for tests.
        op.session_event_cap = 3;
        let arrow_schema = input_arrow(&schema);

        let mut state = op.create_state(&EntityId::from("e1"));
        op.process_sub_batch(
            &mut state,
            &make_batch(
                &arrow_schema,
                "e1",
                vec![0, 10, 20, 30, 40, 50],
                vec!["a", "b", "c", "d", "e", "f"],
                None,
            ),
        );
        assert!(state.cap_exceeded());
        let out = op.finish_entity(state).expect("partial session emitted");
        let (sids, sdurs) = read_session_columns(&out);
        // Cap fires at the 4th event (count goes to 4 after accumulation,
        // which exceeds cap=3). Partial flush includes rows 0..=3.
        assert_eq!(sids, vec![1, 1, 1, 1]);
        assert!(sdurs.iter().all(|&d| d == 30));
        assert_eq!(out.num_rows(), 4);
    }

    // ── Schema / column forwarding ───────────────────────────────────────

    #[test]
    fn forwarded_column_flows_through_session_buffer() {
        let schema = events_schema_with_amount();
        let phys = build_physical(schema.clone(), 100, vec![], vec!["amount".into()]);
        let op = SessionizeOperator::new(&phys);
        let arrow_schema = input_arrow(&schema);

        let mut state = op.create_state(&EntityId::from("e1"));
        let batch = make_batch(
            &arrow_schema,
            "e1",
            vec![0, 50, 200],
            vec!["a", "b", "c"],
            Some(vec![Some(10), Some(20), Some(30)]),
        );
        op.process_sub_batch(&mut state, &batch);
        let out = op.finish_entity(state).unwrap();
        let amount = out
            .column_by_name("amount")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(amount.value(0), 10);
        assert_eq!(amount.value(1), 20);
        assert_eq!(amount.value(2), 30);
    }

    #[test]
    fn non_forwarded_column_is_null_padded() {
        let schema = events_schema_with_amount();
        // `amount` is in the logical output schema but NOT forwarded.
        let phys = build_physical(schema.clone(), 100, vec![], vec![]);
        let op = SessionizeOperator::new(&phys);
        assert!(
            !op.null_padded_output_indices().is_empty(),
            "at least `amount` is expected to be null-padded"
        );
        let arrow_schema = input_arrow(&schema);

        let mut state = op.create_state(&EntityId::from("e1"));
        op.process_sub_batch(
            &mut state,
            &make_batch(
                &arrow_schema,
                "e1",
                vec![0, 50],
                vec!["a", "b"],
                Some(vec![Some(10), Some(20)]),
            ),
        );
        let out = op.finish_entity(state).unwrap();
        let amount = out.column_by_name("amount").unwrap();
        assert_eq!(amount.null_count(), 2);
    }

    #[test]
    fn required_columns_include_ts_and_forwarded() {
        let schema = events_schema_with_amount();
        let phys = build_physical(schema, 100, vec![], vec!["amount".into()]);
        let op = SessionizeOperator::new(&phys);
        let req = op.required_columns();
        assert!(req.iter().any(|n| n == "ts"));
        assert!(req.iter().any(|n| n == "amount"));
        assert!(req.iter().any(|n| n == "entity_id"));
        // `event_type` is also buffered even in gap-only mode because the
        // advertised output schema declares it NOT NULL — see
        // sessionize.rs `SessionizeOperator::new` buffered-set derivation.
        assert!(req.iter().any(|n| n == "event_type"));
    }

    #[test]
    fn required_columns_include_event_type_when_end_events_set() {
        let schema = events_schema_no_amount();
        let phys = build_physical(schema, 100, vec!["logout".into()], vec![]);
        let op = SessionizeOperator::new(&phys);
        let req = op.required_columns();
        assert!(req.iter().any(|n| n == "event_type"));
    }

    #[test]
    fn supported_demands_advertises_forwarded_columns() {
        let schema = events_schema_no_amount();
        let phys = build_physical(schema, 100, vec![], vec![]);
        let op = SessionizeOperator::new(&phys);
        assert!(op.supported_demands().supports_forwarded_columns);
        // Matches the planner-side declaration on the physical descriptor.
        assert_eq!(op.supported_demands(), SessionizePhysical::DEMAND_CAPS);
    }

    #[test]
    fn end_events_list_is_sorted() {
        let schema = events_schema_no_amount();
        let phys = build_physical(schema, 100, vec!["timeout".into(), "logout".into()], vec![]);
        let op = SessionizeOperator::new(&phys);
        assert_eq!(
            op.end_events_list(),
            &["logout".to_string(), "timeout".to_string()]
        );
    }

    #[test]
    fn buffered_schema_always_includes_entity_id_and_ts() {
        let schema = events_schema_with_amount();
        let phys = build_physical(schema, 100, vec![], vec!["amount".into()]);
        let op = SessionizeOperator::new(&phys);
        let names: Vec<_> = op
            .buffered_arrow_schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        // `entity_id` is implicitly buffered even when not in the
        // caller's `forwarded_columns` list — the advertised output
        // schema carries it as NOT NULL (sessionize.md §9.2 buffered set).
        assert!(names.contains(&"entity_id".to_string()));
        assert!(names.contains(&"ts".to_string()));
        assert!(names.contains(&"amount".to_string()));
    }

    // ── Multi-session output in a single entity ──────────────────────────

    #[test]
    fn output_row_count_preserves_input_row_count() {
        let schema = events_schema_no_amount();
        let phys = build_physical(schema.clone(), 10, vec![], vec![]);
        let op = SessionizeOperator::new(&phys);
        let arrow_schema = input_arrow(&schema);
        let mut state = op.create_state(&EntityId::from("e1"));
        op.process_sub_batch(
            &mut state,
            &make_batch(
                &arrow_schema,
                "e1",
                vec![0, 100, 200, 300, 400],
                vec!["a", "b", "c", "d", "e"],
                None,
            ),
        );
        let out = op.finish_entity(state).unwrap();
        assert_eq!(out.num_rows(), 5);
        let (sids, _) = read_session_columns(&out);
        assert_eq!(sids, vec![1, 2, 3, 4, 5]);
    }
}
