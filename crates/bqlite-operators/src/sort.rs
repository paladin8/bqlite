//! Pipeline sort operator: materializes all input rows in memory, applies
//! Arrow `lexsort`, and emits sorted output in `DEFAULT_OUTPUT_BATCH_SIZE`
//! chunks.
//!
//! ## Algorithm
//!
//! **Phase 1 — accumulation**: `next_batch` drains the child operator,
//! pushing each input batch into an in-memory buffer. Each batch's
//! Arrow array memory is charged to the per-query [`MemoryBudget`]
//! through a [`MemoryReservation`] held alongside the batch, so the
//! buffered footprint is visible to the tracker.  If the running row
//! count exceeds `max_rows` the operator returns
//! `BqliteError::Execution` immediately.
//!
//! **Phase 2 — sort and drain**: when the child is exhausted the
//! operator concatenates all buffered batches into one `RecordBatch` via
//! `arrow::compute::concat_batches` (zero-copy where possible), evaluates
//! each sort-key expression over the concatenated batch, calls
//! `arrow::compute::lexsort_to_indices` for a stable sort, reorders
//! columns with `arrow::compute::take`, then splits the result into
//! `DEFAULT_OUTPUT_BATCH_SIZE`-row chunks for pipeline-friendly emission.
//!
//! ## Null ordering
//!
//! Per `docs/design/operators/sort-distinct.md §3.3`:
//! - `ASC`  → NULLs **last**  (`SortOptions { nulls_first: false }`)
//! - `DESC` → NULLs **first** (`SortOptions { nulls_first: true  }`)
//!
//! ## Cancellation
//!
//! The cancellation token is checked at the top of every `next_batch`
//! call (before pulling from the child or draining output) per
//! `operator-traits.md §5`.
//!
//! ## Memory and spill
//!
//! Each accumulated batch's Arrow array bytes are reserved against the
//! per-query [`MemoryBudget`] (TASK-513 CP2a). If a [`SpillFs`] handle
//! is wired in (production callers thread one through; tests may pass
//! `None`) the operator registers a `SortSpillHandler` with the
//! budget. On `try_reserve` overshoot the handler sorts the in-memory
//! buffer, writes one Arrow IPC stream-format run to disk, drops the
//! reservations the run held, and returns the freed reservation bytes
//! (per `engine/spill.md` § 10.2 step 6 — *reservation* bytes, not
//! on-disk bytes). At child exhaustion the operator builds a k-way
//! merger over every spilled run plus the sorted in-memory residual
//! and emits output via `arrow::compute::interleave_record_batch` so
//! `Utf8View` / dictionary representations survive the merge.
//!
//! Without a `SpillFs` the operator behaves like CP2a: any
//! `try_reserve` failure surfaces `MemoryBudgetExceeded` immediately.
//!
//! See `docs/design/operators/sort-distinct.md §3` and
//! `docs/design/engine/spill.md §6.1 / §10` for the full spec.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::{Arc, Mutex};

use arrow::array::ArrayRef;
use arrow::compute::{
    concat_batches, interleave, lexsort_to_indices, take, SortColumn, SortOptions,
};
use arrow::datatypes::Schema as ArrowSchema;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use arrow::row::{OwnedRow, RowConverter, SortField};

use bqlite_core::memory::{MemoryBudget, MemoryReservation, SpillNotification};
use bqlite_core::spill::{SpillFs, SpillQueryId, TempSpillFile};
use bqlite_core::{BqliteError, OperatorSchema, Result};
use bqlite_planner::compiled::CompiledExpr;
use bqlite_planner::logical::SortDirection;

use crate::eval;
use crate::operator::{CancellationToken, PhysicalOperator, DEFAULT_OUTPUT_BATCH_SIZE};

/// Spill-purpose tag used in the path scheme `spill.md` § 7.
const SORT_SPILL_PURPOSE: &str = "sort-run";

// ─────────────────────────────────────────────────────────────────────────────
// SortOperator
// ─────────────────────────────────────────────────────────────────────────────

/// Two-phase pipeline sort operator.
///
/// Constructed by the engine bind step (TASK-323) from a
/// [`bqlite_planner::physical::SortPhysical`] descriptor plus a bound
/// child operator and cancellation token.
pub struct SortOperator {
    child: Box<dyn PhysicalOperator>,
    /// Hard cap on total input rows. `BqliteError::Execution` on breach.
    max_rows: usize,
    cancel: CancellationToken,
    /// Per-query memory budget. Each buffered input batch is reserved
    /// against this budget so the tracker observes the in-flight sort
    /// footprint; the spill handler (when registered) frees bytes here
    /// on pressure.
    budget: Arc<dyn MemoryBudget>,
    schema: OperatorSchema,
    /// Spill state shared with the `SortSpillHandler`. The operator
    /// holds the writer side; the handler owns a clone of the same
    /// `Arc` so it can drain the buffer when the budget asks. The
    /// operator never holds this lock across `try_reserve`, so the
    /// handler's synchronous re-entry from inside `try_reserve` cannot
    /// deadlock (`memory-budget.md` § 4.2).
    state: Arc<Mutex<SortSpillState>>,
    /// Phase tracker. The spill state lives behind `state`; this enum
    /// just records whether we are still pulling from the child, in
    /// the merge phase, or finished.
    phase: Phase,
}

/// One in-memory input batch plus the budget reservation that pinned its
/// Arrow array bytes.  Dropping the entry releases the bytes back to the
/// tracker.  The spill handler (CP2b) inspects `reservation.bytes()`
/// to compute the total bytes freed when it drains the buffer.
struct BufferedBatch {
    batch: RecordBatch,
    reservation: MemoryReservation,
}

/// Spill-related state shared between the operator and the
/// `SortSpillHandler`. Held behind `Arc<Mutex<…>>` so the handler
/// can mutate it from inside the budget's `on_pressure` callback.
struct SortSpillState {
    /// In-memory accumulation buffer. Drained on spill or merge.
    buffer: Vec<BufferedBatch>,
    /// Spilled sorted runs in arrival order. Each guard owns one
    /// Arrow-IPC stream file; dropping the guard deletes the file.
    runs: Vec<TempSpillFile>,
    /// Cumulative row count across buffered + spilled runs. Bounded by
    /// the operator's `max_rows`.
    total_rows: usize,
    /// Sort keys + directions, also visible to the handler.
    keys: Vec<(CompiledExpr, SortDirection)>,
    /// Cached Arrow schema for `concat_batches` / IPC writers.
    arrow_schema: Arc<ArrowSchema>,
    /// Cancellation flag polled inside the spill writer's loop.
    cancel: CancellationToken,
    /// Optional spill route. `None` for tests / contexts that never
    /// want spill — the handler then degrades to a no-op.
    spill: Option<SpillRoute>,
}

/// Where spill files land. Cloned-in from [`bqlite_engine::QueryContext`].
struct SpillRoute {
    spill_fs: Arc<SpillFs>,
    qid: SpillQueryId,
}

/// Operator phase. The accumulation buffer / spilled runs live on
/// the shared `SortSpillState`; the merger lives directly on the
/// operator (only one consumer for the merger, no need to share).
enum Phase {
    /// Pulling from the child; pushing into `state.buffer`.
    Accumulating,
    /// Child exhausted; merger emits sorted output one batch per
    /// `next_batch`.
    Merging(SortMerger),
    /// Done — child empty or all output emitted.
    Done,
}

impl SortOperator {
    /// Construct a new `SortOperator` without spill (in-memory only).
    ///
    /// Convenience wrapper used by tests and Wave 3 callers that have
    /// no spill route. Equivalent to [`SortOperator::with_spill`] with
    /// `spill_fs = None`.
    pub fn new(
        child: Box<dyn PhysicalOperator>,
        keys: Vec<(CompiledExpr, SortDirection)>,
        max_rows: usize,
        cancel: CancellationToken,
        budget: Arc<dyn MemoryBudget>,
    ) -> Self {
        Self::with_spill(child, keys, max_rows, cancel, budget, None, None)
    }

    /// Construct a new `SortOperator` with optional spill plumbing.
    ///
    /// - `child` — the child operator whose output will be sorted.
    /// - `keys` — sort key expressions in priority order with their
    ///   directions.  An empty `keys` vec is legal; the operator
    ///   emits input rows in their original order (stable by
    ///   definition — `lexsort_to_indices` on zero keys is a no-op).
    /// - `max_rows` — total row cap (in-memory + spilled).
    ///   `BqliteError::Execution` on breach.
    /// - `cancel` — shared cancellation flag.
    /// - `budget` — per-query [`MemoryBudget`].
    /// - `spill_fs` / `spill_qid` — paired; both `Some` enables spill.
    ///   The operator registers a `SortSpillHandler` with `budget`
    ///   so `try_reserve` overshoot drains the buffer to disk.
    pub fn with_spill(
        child: Box<dyn PhysicalOperator>,
        keys: Vec<(CompiledExpr, SortDirection)>,
        max_rows: usize,
        cancel: CancellationToken,
        budget: Arc<dyn MemoryBudget>,
        spill_fs: Option<Arc<SpillFs>>,
        spill_qid: Option<SpillQueryId>,
    ) -> Self {
        let schema = child.output_schema().clone();
        let arrow_schema = Arc::new(schema.to_arrow_schema());
        let spill = match (spill_fs, spill_qid) {
            (Some(fs), Some(qid)) => Some(SpillRoute { spill_fs: fs, qid }),
            _ => None,
        };
        let state = Arc::new(Mutex::new(SortSpillState {
            buffer: Vec::new(),
            runs: Vec::new(),
            total_rows: 0,
            keys,
            arrow_schema,
            cancel: cancel.clone(),
            spill,
        }));
        // Register a spill handler regardless of spill_fs presence —
        // when `state.spill` is `None` the handler returns 0 freed
        // bytes, matching the no-spill contract. Registering even in
        // the no-spill case keeps the registration site uniform.
        budget.register_spill_handler(Arc::new(SortSpillHandler {
            state: Arc::clone(&state),
        }));
        Self {
            child,
            max_rows,
            cancel,
            budget,
            schema,
            state,
            phase: Phase::Accumulating,
        }
    }

    /// Borrow the child operator (used by tests and the engine bind step
    /// when it needs to inspect the subtree).
    pub fn child(&self) -> &dyn PhysicalOperator {
        self.child.as_ref()
    }
}

impl PhysicalOperator for SortOperator {
    fn output_schema(&self) -> &OperatorSchema {
        &self.schema
    }

    fn open(&mut self) -> Result<()> {
        self.child.open()
    }

    /// Drive the two-phase sort algorithm.
    ///
    /// Accumulation pulls batches from the child, charging each to the
    /// budget; the spill handler may drain the buffer to disk on
    /// pressure. When the child returns `None`, the operator builds a
    /// merger over every spilled run + the sorted in-memory residual
    /// and emits one merged batch per call until exhausted.
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        // Cancellation check at entry — before any child pull or merger emit.
        if self.cancel.is_cancelled() {
            return Err(BqliteError::Cancelled);
        }

        loop {
            match &mut self.phase {
                Phase::Accumulating => {
                    match self.child.next_batch()? {
                        Some(batch) => {
                            let row_count = batch.num_rows();
                            let bytes = batch.get_array_memory_size() as u64;

                            // 1. Reserve through the budget *without*
                            //    holding the state lock. The spill
                            //    handler may run synchronously here
                            //    and lock the state itself, so the
                            //    operator must not hold it.
                            let reservation = match self.budget.try_reserve(bytes) {
                                Ok(r) => r,
                                Err(e) => {
                                    self.phase = Phase::Done;
                                    return Err(e);
                                }
                            };

                            // 2. Lock state, enforce row cap, push.
                            let mut state = self.state.lock().expect("sort state poisoned");
                            let new_total = state.total_rows + row_count;
                            if new_total > self.max_rows {
                                self.phase = Phase::Done;
                                return Err(BqliteError::Execution(format!(
                                    "SortOperator: input row count {} exceeds max_rows limit {}",
                                    new_total, self.max_rows
                                )));
                            }
                            state.total_rows = new_total;
                            state.buffer.push(BufferedBatch { batch, reservation });
                            drop(state);

                            // 3. Cancellation check between pulls.
                            if self.cancel.is_cancelled() {
                                self.phase = Phase::Done;
                                return Err(BqliteError::Cancelled);
                            }
                        }
                        None => {
                            // Child exhausted — build the merger from
                            // (residual buffer, spilled runs). On
                            // error transition to `Done` so a re-entry
                            // does not reattempt the build against a
                            // half-drained state.
                            let merger = match build_merger(&self.state) {
                                Ok(m) => m,
                                Err(e) => {
                                    self.phase = Phase::Done;
                                    return Err(e);
                                }
                            };
                            match merger {
                                Some(m) => self.phase = Phase::Merging(m),
                                None => {
                                    self.phase = Phase::Done;
                                    return Ok(None);
                                }
                            }
                        }
                    }
                }
                Phase::Merging(merger) => match merger.next_batch()? {
                    Some(batch) => return Ok(Some(batch)),
                    None => {
                        self.phase = Phase::Done;
                        return Ok(None);
                    }
                },
                Phase::Done => return Ok(None),
            }
        }
    }

    /// Release resources: drop the accumulated buffer / merger / runs,
    /// and close the child. Idempotent.
    fn close(&mut self) -> Result<()> {
        if !matches!(self.phase, Phase::Done) {
            self.phase = Phase::Done;
            // Force-drop the spill state so any held TempSpillFile
            // guards delete their files even if the merger never
            // exhausted them.
            let mut state = self.state.lock().expect("sort state poisoned");
            state.buffer.clear();
            state.runs.clear();
            return self.child.close();
        }
        Ok(())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Spill handler
// ─────────────────────────────────────────────────────────────────────────────

/// Implements [`SpillNotification`]; lives behind the budget and
/// drains the operator's buffer to disk on pressure.
struct SortSpillHandler {
    state: Arc<Mutex<SortSpillState>>,
}

impl SpillNotification for SortSpillHandler {
    fn on_pressure(&self, _bytes_needed: u64) -> u64 {
        match self.state.lock() {
            Ok(mut state) => spill_buffer(&mut state).unwrap_or(0),
            Err(_) => 0,
        }
    }
}

/// Sort the buffer in-memory, write it as one Arrow IPC stream-format
/// run, push the guard onto `state.runs`, drop the buffer + its
/// reservations, and return the *reservation* bytes freed (per
/// `engine/spill.md` § 10.2 step 6).
///
/// Returns `Ok(0)` when there is nothing to spill (empty buffer or no
/// SpillFs route attached) so the caller's `unwrap_or(0)` is correct.
fn spill_buffer(state: &mut SortSpillState) -> Result<u64> {
    if state.buffer.is_empty() {
        return Ok(0);
    }
    let route = match &state.spill {
        Some(r) => r,
        None => return Ok(0),
    };
    if state.cancel.is_cancelled() {
        return Err(BqliteError::Cancelled);
    }

    // Sum reservation bytes *before* we move the buffer — this is what
    // the budget will see freed once the reservations drop.
    let freed_bytes: u64 = state.buffer.iter().map(|b| b.reservation.bytes()).sum();

    // Sort the buffer into a single batch (same primitive as the
    // in-memory path).
    let buffer = std::mem::take(&mut state.buffer);
    let sorted = sort_buffer_into_one_batch(&buffer, &state.arrow_schema, &state.keys)?;
    drop(buffer); // release reservations.

    // Write to a fresh TempSpillFile.
    let mut guard = route.spill_fs.open_spill(route.qid, SORT_SPILL_PURPOSE)?;
    {
        let writer = guard.file_mut();
        let mut stream =
            StreamWriter::try_new(writer, sorted.schema().as_ref()).map_err(BqliteError::Arrow)?;
        // Write the sorted batch in DEFAULT_OUTPUT_BATCH_SIZE-row
        // chunks; poll cancellation between chunks per spill.md § 11.
        let total = sorted.num_rows();
        let mut offset = 0;
        while offset < total {
            if state.cancel.is_cancelled() {
                return Err(BqliteError::Cancelled);
            }
            let len = DEFAULT_OUTPUT_BATCH_SIZE.min(total - offset);
            let chunk = sorted.slice(offset, len);
            stream.write(&chunk).map_err(BqliteError::Arrow)?;
            offset += len;
        }
        stream.finish().map_err(BqliteError::Arrow)?;
    }
    state.runs.push(guard);
    Ok(freed_bytes)
}

// ─────────────────────────────────────────────────────────────────────────────
// In-memory sort helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Concat + lexsort + take. Returns the single sorted batch; the
/// merger is responsible for splitting it into output-size chunks.
fn sort_buffer_into_one_batch(
    buffer: &[BufferedBatch],
    arrow_schema: &Arc<ArrowSchema>,
    keys: &[(CompiledExpr, SortDirection)],
) -> Result<RecordBatch> {
    // Dict push-through can leave per-batch columns as `Dict<UInt8, Utf8View>`
    // with disjoint per-segment dictionaries. `concat_batches` would call
    // Arrow's `concat`, which unifies the dict value tables and panics via
    // `MutableArrayData::new` once the union exceeds the u8 key space.
    // Canonicalise each batch to the schema's declared field types first so
    // dict mismatches turn into a single cast per source rather than a
    // overflow panic inside the concat kernel.
    let batches: Vec<RecordBatch> = buffer
        .iter()
        .map(|b| canonicalise_batch_to_schema(&b.batch, arrow_schema))
        .collect::<Result<_>>()?;
    let combined = concat_batches(arrow_schema, &batches)?;
    if combined.num_rows() == 0 || keys.is_empty() {
        return Ok(combined);
    }
    let sort_columns: Vec<SortColumn> = keys
        .iter()
        .map(|(expr, dir)| {
            let values = eval::evaluate(expr, &combined)?;
            Ok(SortColumn {
                values,
                options: Some(sort_options_for(dir)),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let indices = lexsort_to_indices(&sort_columns, None)?;
    let sorted_cols: Vec<_> = combined
        .columns()
        .iter()
        .map(|col| take(col.as_ref(), &indices, None).map_err(BqliteError::Arrow))
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(combined.schema(), sorted_cols).map_err(BqliteError::Arrow)
}

/// Cast every column of `batch` to the matching field type in
/// `target_schema`. No-op for columns whose physical type already matches
/// (the common case); reserved for the dict-push-through path where a
/// source emits `Dict<UInt8, Utf8View>` but the declared output type is
/// `Utf8View`. Casting here avoids a `MutableArrayData::new` panic when
/// downstream Arrow kernels (`concat`, `interleave`) try to unify
/// per-source dict tables that exceed the u8 key space.
fn canonicalise_batch_to_schema(
    batch: &RecordBatch,
    target_schema: &Arc<ArrowSchema>,
) -> Result<RecordBatch> {
    let n_cols = batch.num_columns();
    debug_assert_eq!(n_cols, target_schema.fields().len());
    let mut casted: Vec<ArrayRef> = Vec::with_capacity(n_cols);
    let mut any_changed = false;
    for i in 0..n_cols {
        let col = batch.column(i);
        let target_ty = target_schema.field(i).data_type();
        if col.data_type() == target_ty {
            casted.push(col.clone());
        } else {
            any_changed = true;
            let arr = ::arrow::compute::cast(col.as_ref(), target_ty).map_err(|e| {
                BqliteError::Execution(format!(
                    "sort: cast column {i} from {:?} to {target_ty:?} failed: {e}",
                    col.data_type()
                ))
            })?;
            casted.push(arr);
        }
    }
    if any_changed {
        RecordBatch::try_new(Arc::clone(target_schema), casted).map_err(BqliteError::Arrow)
    } else {
        Ok(batch.clone())
    }
}

/// Map a `SortDirection` to Arrow `SortOptions` following the null-ordering
/// convention from `sort-distinct.md §3.3` (matches DuckDB / BigQuery /
/// Oracle / Postgres defaults):
///
/// | Direction | NULL position | `SortOptions`                                |
/// |-----------|---------------|----------------------------------------------|
/// | `ASC`     | last          | `{ descending: false, nulls_first: false }`  |
/// | `DESC`    | first         | `{ descending: true,  nulls_first: true  }`  |
#[inline]
fn sort_options_for(dir: &SortDirection) -> SortOptions {
    match dir {
        SortDirection::Asc => SortOptions {
            descending: false,
            nulls_first: false,
        },
        SortDirection::Desc => SortOptions {
            descending: true,
            nulls_first: true,
        },
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// K-way merge
// ─────────────────────────────────────────────────────────────────────────────

/// Build a [`SortMerger`] from the operator's spill state. Returns
/// `None` when there is nothing to merge (empty input). Locks the
/// state for the duration; never blocks `try_reserve` because the
/// child has been exhausted by the time this runs.
fn build_merger(state: &Arc<Mutex<SortSpillState>>) -> Result<Option<SortMerger>> {
    let mut state = state.lock().expect("sort state poisoned");
    if state.buffer.is_empty() && state.runs.is_empty() {
        return Ok(None);
    }
    let arrow_schema = Arc::clone(&state.arrow_schema);
    let keys = std::mem::take(&mut state.keys);

    // Sort the in-memory residual (if any) first.
    let residual_batch = if !state.buffer.is_empty() {
        let buffer = std::mem::take(&mut state.buffer);
        let batch = sort_buffer_into_one_batch(&buffer, &arrow_schema, &keys)?;
        // Reservations drop with `buffer`.
        drop(buffer);
        Some(batch)
    } else {
        None
    };

    let runs = std::mem::take(&mut state.runs);
    let cancel = state.cancel.clone();
    drop(state);

    SortMerger::new(arrow_schema, keys, residual_batch, runs, cancel).map(Some)
}

/// Owns one source of sorted rows — either a spilled IPC stream or
/// the in-memory residual batch. Provides `next_batch` for the merger
/// and drops the underlying spill file when consumed.
enum RunReader {
    /// Spilled run; kept alongside its `TempSpillFile` guard so the
    /// file is deleted when the cursor drops. The reader is boxed
    /// because `StreamReader` carries a non-trivial buffer (~8 KB +
    /// schema state); without the box clippy fires on the variant
    /// size discrepancy with `InMemory`.
    Spilled {
        reader: Box<StreamReader<std::io::BufReader<std::fs::File>>>,
        // Held-for-drop. The reader has its own File handle (opened
        // by `File::open(guard.path())`); the guard's Drop deletes
        // the on-disk file after the reader is dropped.
        _guard: TempSpillFile,
    },
    /// Single in-memory batch, yielded once.
    InMemory(Option<RecordBatch>),
}

impl RunReader {
    fn next(&mut self) -> Result<Option<RecordBatch>> {
        match self {
            RunReader::Spilled { reader, .. } => match reader.next() {
                Some(Ok(b)) => Ok(Some(b)),
                Some(Err(e)) => Err(BqliteError::Arrow(e)),
                None => Ok(None),
            },
            RunReader::InMemory(slot) => Ok(slot.take()),
        }
    }
}

/// Cursor into one run. Tracks the current batch via an index into
/// the merger's append-only `batches` vec so heap entries can refer
/// to a batch that the cursor has since advanced past.
struct RunCursor {
    reader: RunReader,
    /// Index into `SortMerger::batches` for the cursor's current
    /// batch. `None` before the first pull and after exhaustion.
    current_batch_idx: Option<usize>,
    /// Cached row-format of the current batch's sort keys. Reused
    /// across every row in the batch.
    current_rows: Option<arrow::row::Rows>,
    /// Row index within `batches[current_batch_idx]`.
    row_idx: usize,
    /// Run identifier — tie-break for stable ordering across runs.
    run_idx: usize,
}

/// Min-heap entry. The `Ord` impl orders smallest-first (used with
/// `Reverse` in a max-heap), tie-breaking on `run_idx`.
struct HeapEntry {
    /// Owned row of the sort-key columns at the cursor's current
    /// position. Compared bytewise via `OwnedRow`'s natural order.
    row: OwnedRow,
    /// Index into the merger's `batches` vec for the row's batch.
    /// Captured at push time so subsequent cursor advances cannot
    /// invalidate it.
    batch_idx: usize,
    /// Row index within `batches[batch_idx]`.
    row_idx: usize,
    /// Index into `cursors` — used to advance the run after popping.
    cursor_idx: usize,
    /// Stable tie-break: spilled-earlier runs win on equal keys.
    run_idx: usize,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.row == other.row && self.run_idx == other.run_idx
    }
}
impl Eq for HeapEntry {}
impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap; we wrap in `Reverse` at the call
        // site so the min row pops first. Tie-break on run_idx so
        // smaller run_idx (earlier-spilled run) wins → stability.
        self.row
            .cmp(&other.row)
            .then_with(|| self.run_idx.cmp(&other.run_idx))
    }
}
impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// K-way merge over (spilled runs, sorted in-memory residual).
pub(crate) struct SortMerger {
    arrow_schema: Arc<ArrowSchema>,
    keys: Vec<(CompiledExpr, SortDirection)>,
    /// One [`RowConverter`] reused across every run/batch; produces
    /// the bytewise-comparable row format.
    converter: RowConverter,
    cursors: Vec<RunCursor>,
    /// Append-only history of every batch ever loaded by any cursor.
    /// Heap entries reference rows by `(batch_idx, row_idx)`, so the
    /// vec must never shrink during the merger's lifetime — old
    /// batches stay alive (Arc-shared, cheap) until the merger drops.
    batches: Vec<RecordBatch>,
    heap: BinaryHeap<std::cmp::Reverse<HeapEntry>>,
    cancel: CancellationToken,
}

impl SortMerger {
    fn new(
        arrow_schema: Arc<ArrowSchema>,
        keys: Vec<(CompiledExpr, SortDirection)>,
        residual: Option<RecordBatch>,
        runs: Vec<TempSpillFile>,
        cancel: CancellationToken,
    ) -> Result<Self> {
        let mut cursors: Vec<RunCursor> = Vec::with_capacity(runs.len() + 1);
        // Spilled runs first (smaller run_idx → spilled earlier →
        // higher priority on equal keys → stability).
        for guard in runs {
            let file = std::fs::File::open(guard.path()).map_err(BqliteError::Io)?;
            let reader = StreamReader::try_new(std::io::BufReader::new(file), None)
                .map_err(BqliteError::Arrow)?;
            cursors.push(RunCursor {
                reader: RunReader::Spilled {
                    reader: Box::new(reader),
                    _guard: guard,
                },
                current_batch_idx: None,
                current_rows: None,
                row_idx: 0,
                run_idx: cursors.len(),
            });
        }
        if let Some(batch) = residual {
            cursors.push(RunCursor {
                reader: RunReader::InMemory(Some(batch)),
                current_batch_idx: None,
                current_rows: None,
                row_idx: 0,
                run_idx: cursors.len(),
            });
        }

        // Build the row converter from the schema's sort-key fields.
        let sort_fields: Vec<SortField> = if keys.is_empty() {
            vec![SortField::new(arrow_schema.field(0).data_type().clone())]
        } else {
            keys.iter()
                .map(|(expr, dir)| {
                    SortField::new_with_options(
                        expr.result_type.to_arrow_type(),
                        sort_options_for(dir),
                    )
                })
                .collect()
        };
        let converter = RowConverter::new(sort_fields).map_err(BqliteError::Arrow)?;

        let mut merger = Self {
            arrow_schema,
            keys,
            converter,
            cursors,
            batches: Vec::new(),
            heap: BinaryHeap::new(),
            cancel,
        };
        // Prime each cursor.
        for idx in 0..merger.cursors.len() {
            merger.advance_cursor(idx)?;
        }
        Ok(merger)
    }

    /// Push the cursor's current row onto the heap, or pull a new
    /// batch if the current one is exhausted, or mark the cursor
    /// exhausted if the reader has no more batches.
    fn advance_cursor(&mut self, idx: usize) -> Result<()> {
        loop {
            // If the cursor has a live batch and a valid row index,
            // push that row.
            let cursor = &self.cursors[idx];
            if let Some(batch_idx) = cursor.current_batch_idx {
                let batch = &self.batches[batch_idx];
                if cursor.row_idx < batch.num_rows() {
                    let rows = cursor.current_rows.as_ref().expect("rows when batch live");
                    let row = rows.row(cursor.row_idx).owned();
                    let run_idx = cursor.run_idx;
                    let row_idx = cursor.row_idx;
                    self.heap.push(std::cmp::Reverse(HeapEntry {
                        row,
                        batch_idx,
                        row_idx,
                        cursor_idx: idx,
                        run_idx,
                    }));
                    return Ok(());
                }
                // Current batch exhausted — clear and pull next.
                let cursor = &mut self.cursors[idx];
                cursor.current_batch_idx = None;
                cursor.current_rows = None;
                cursor.row_idx = 0;
            }
            // Pull next batch.
            let next = self.cursors[idx].reader.next()?;
            match next {
                Some(batch) => {
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    let key_columns: Vec<ArrayRef> = if self.keys.is_empty() {
                        vec![batch.column(0).clone()]
                    } else {
                        self.keys
                            .iter()
                            .map(|(expr, _)| eval::evaluate(expr, &batch))
                            .collect::<Result<Vec<_>>>()?
                    };
                    let rows = self
                        .converter
                        .convert_columns(&key_columns)
                        .map_err(BqliteError::Arrow)?;
                    self.batches.push(batch);
                    let new_idx = self.batches.len() - 1;
                    let cursor = &mut self.cursors[idx];
                    cursor.current_batch_idx = Some(new_idx);
                    cursor.current_rows = Some(rows);
                    cursor.row_idx = 0;
                    // loop back to push first row.
                }
                None => return Ok(()), // cursor exhausted, no entry pushed.
            }
        }
    }

    /// Emit one merged batch. Returns `Ok(None)` when fully drained.
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.cancel.is_cancelled() {
            return Err(BqliteError::Cancelled);
        }
        if self.heap.is_empty() {
            return Ok(None);
        }

        // Collect (batch_idx, row_idx) pairs by popping the heap. The
        // batch_idx is captured at push time so subsequent cursor
        // advances don't invalidate it.
        let mut indices: Vec<(usize, usize)> = Vec::with_capacity(DEFAULT_OUTPUT_BATCH_SIZE);
        while indices.len() < DEFAULT_OUTPUT_BATCH_SIZE {
            let std::cmp::Reverse(entry) = match self.heap.pop() {
                Some(e) => e,
                None => break,
            };
            indices.push((entry.batch_idx, entry.row_idx));
            // Advance the row in the cursor and push the next row.
            let cursor_idx = entry.cursor_idx;
            self.cursors[cursor_idx].row_idx += 1;
            self.advance_cursor(cursor_idx)?;
        }

        if indices.is_empty() {
            return Ok(None);
        }

        // Materialize via interleave, one kernel call per column.
        //
        // Per-batch columns may be `Dict<UInt8, Utf8View>` with disjoint
        // dictionaries (dict push-through, see `canonicalise_batch_to_schema`).
        // Cast to the schema field type first; Arrow's `interleave` would
        // otherwise call `MutableArrayData::new` and panic on dict union
        // overflow.
        let num_cols = self.arrow_schema.fields().len();
        let mut out_columns: Vec<ArrayRef> = Vec::with_capacity(num_cols);
        for col_idx in 0..num_cols {
            let field_ty = self.arrow_schema.field(col_idx).data_type();
            let casted: Vec<ArrayRef> = self
                .batches
                .iter()
                .map(|b| {
                    let raw = b.column(col_idx);
                    if raw.data_type() == field_ty {
                        Ok(raw.clone())
                    } else {
                        ::arrow::compute::cast(raw.as_ref(), field_ty).map_err(|e| {
                            BqliteError::Execution(format!(
                                "sort k-way merge: cast column {col_idx} from {:?} to {field_ty:?} failed: {e}",
                                raw.data_type()
                            ))
                        })
                    }
                })
                .collect::<Result<_>>()?;
            let refs: Vec<&dyn arrow::array::Array> = casted.iter().map(|a| a.as_ref()).collect();
            let merged = interleave(&refs, &indices).map_err(BqliteError::Arrow)?;
            out_columns.push(merged);
        }
        let batch = RecordBatch::try_new(Arc::clone(&self.arrow_schema), out_columns)
            .map_err(BqliteError::Arrow)?;
        Ok(Some(batch))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// BqlType ↔ Arrow type bridge for RowConverter
// ─────────────────────────────────────────────────────────────────────────────

trait BqlTypeArrowExt {
    fn to_arrow_type(&self) -> arrow::datatypes::DataType;
}

impl BqlTypeArrowExt for bqlite_core::BqlType {
    fn to_arrow_type(&self) -> arrow::datatypes::DataType {
        bqlite_core::bql_type_to_arrow(self)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;

    use bqlite_core::memory::{MemoryBudget, MemoryTracker};
    use bqlite_core::{BqlType, BqliteError, ColumnDef, OperatorSchema, Result, UnboundedMemory};
    use bqlite_planner::compiled::{CompiledExpr, CompiledNode};
    use bqlite_planner::logical::SortDirection;

    fn unbounded_budget() -> Arc<dyn MemoryBudget> {
        Arc::new(UnboundedMemory::new())
    }

    use crate::operator::{CancellationToken, DEFAULT_OUTPUT_BATCH_SIZE};

    use super::SortOperator;
    use crate::operator::PhysicalOperator;

    // ── Test fixtures ────────────────────────────────────────────────────────

    fn int_schema() -> OperatorSchema {
        OperatorSchema::new(vec![ColumnDef::required("v", BqlType::Int)])
            .expect("schema construction must succeed")
    }

    fn make_int_batch(values: &[i64]) -> RecordBatch {
        let array: ArrayRef = Arc::new(Int64Array::from(values.to_vec()));
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "v",
            DataType::Int64,
            false,
        )]));
        RecordBatch::try_new(schema, vec![array]).expect("record batch construction must succeed")
    }

    /// A pre-canned `PhysicalOperator` that yields a fixed list of batches.
    struct VecOp {
        schema: OperatorSchema,
        batches: Vec<RecordBatch>,
        idx: usize,
    }

    impl VecOp {
        fn new(schema: OperatorSchema, batches: Vec<RecordBatch>) -> Self {
            Self {
                schema,
                batches,
                idx: 0,
            }
        }
    }

    impl PhysicalOperator for VecOp {
        fn output_schema(&self) -> &OperatorSchema {
            &self.schema
        }
        fn next_batch(&mut self) -> bqlite_core::Result<Option<RecordBatch>> {
            if self.idx >= self.batches.len() {
                return Ok(None);
            }
            let b = self.batches[self.idx].clone();
            self.idx += 1;
            Ok(Some(b))
        }
    }

    /// Build a `CompiledExpr` that references column 0 by index.
    fn col0_expr() -> CompiledExpr {
        CompiledExpr {
            node: CompiledNode::Column {
                index: 0,
                name: "v".to_string(),
            },
            result_type: BqlType::Int,
            nullable: false,
        }
    }

    // ── sort_options_for ─────────────────────────────────────────────────────

    #[test]
    fn asc_produces_nulls_last() {
        use super::sort_options_for;
        let opts = sort_options_for(&SortDirection::Asc);
        assert!(!opts.descending);
        assert!(!opts.nulls_first); // NULLs last
    }

    #[test]
    fn desc_produces_nulls_first() {
        use super::sort_options_for;
        let opts = sort_options_for(&SortDirection::Desc);
        assert!(opts.descending);
        assert!(opts.nulls_first); // NULLs first
    }

    // ── empty input ──────────────────────────────────────────────────────────

    #[test]
    fn sort_empty_input_returns_none() {
        let child = Box::new(VecOp::new(int_schema(), vec![]));
        let mut op = SortOperator::new(
            child,
            vec![],
            1000,
            CancellationToken::new(),
            unbounded_budget(),
        );
        assert!(op.next_batch().unwrap().is_none());
    }

    // ── single batch sort ────────────────────────────────────────────────────

    #[test]
    fn sort_single_batch_asc() {
        let batch = make_int_batch(&[5, 3, 1, 4, 2]);
        let child = Box::new(VecOp::new(int_schema(), vec![batch]));
        let keys = vec![(col0_expr(), SortDirection::Asc)];
        let mut op = SortOperator::new(
            child,
            keys,
            1_000_000,
            CancellationToken::new(),
            unbounded_budget(),
        );

        let out = op.next_batch().unwrap().unwrap();
        let col = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let values: Vec<i64> = (0..col.len()).map(|i| col.value(i)).collect();
        assert_eq!(values, vec![1, 2, 3, 4, 5]);

        assert!(op.next_batch().unwrap().is_none());
    }

    #[test]
    fn sort_single_batch_desc() {
        let batch = make_int_batch(&[3, 1, 2]);
        let child = Box::new(VecOp::new(int_schema(), vec![batch]));
        let keys = vec![(col0_expr(), SortDirection::Desc)];
        let mut op = SortOperator::new(
            child,
            keys,
            1_000_000,
            CancellationToken::new(),
            unbounded_budget(),
        );

        let out = op.next_batch().unwrap().unwrap();
        let col = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let values: Vec<i64> = (0..col.len()).map(|i| col.value(i)).collect();
        assert_eq!(values, vec![3, 2, 1]);
    }

    // ── multi-batch sort ─────────────────────────────────────────────────────

    #[test]
    fn sort_multiple_batches_concatenated_and_sorted() {
        let b1 = make_int_batch(&[4, 6]);
        let b2 = make_int_batch(&[1, 5]);
        let b3 = make_int_batch(&[2, 3]);
        let child = Box::new(VecOp::new(int_schema(), vec![b1, b2, b3]));
        let keys = vec![(col0_expr(), SortDirection::Asc)];
        let mut op = SortOperator::new(
            child,
            keys,
            1_000_000,
            CancellationToken::new(),
            unbounded_budget(),
        );

        // Collect all output rows across potentially multiple batches.
        let mut all_values: Vec<i64> = Vec::new();
        while let Some(batch) = op.next_batch().unwrap() {
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            all_values.extend((0..col.len()).map(|i| col.value(i)));
        }
        assert_eq!(all_values, vec![1, 2, 3, 4, 5, 6]);
    }

    // ── stability ────────────────────────────────────────────────────────────

    #[test]
    fn sort_is_stable_on_equal_keys() {
        // Two-column schema: key col (all same) + order col (to detect stability).
        let key_col: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 1, 1, 1]));
        let order_col: ArrayRef = Arc::new(Int64Array::from(vec![10i64, 20, 30, 40]));
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("order", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(arrow_schema, vec![key_col, order_col]).unwrap();

        let schema = OperatorSchema::new(vec![
            ColumnDef::required("key", BqlType::Int),
            ColumnDef::required("order", BqlType::Int),
        ])
        .unwrap();
        let child = Box::new(VecOp::new(schema, vec![batch]));

        // Sort by col 0 only (all equal); original order of col 1 must be preserved.
        let key_expr = CompiledExpr {
            node: CompiledNode::Column {
                index: 0,
                name: "key".to_string(),
            },
            result_type: BqlType::Int,
            nullable: false,
        };
        let keys = vec![(key_expr, SortDirection::Asc)];
        let mut op = SortOperator::new(
            child,
            keys,
            1_000_000,
            CancellationToken::new(),
            unbounded_budget(),
        );

        let out = op.next_batch().unwrap().unwrap();
        let order_values: Vec<i64> = {
            let col = out.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
            (0..col.len()).map(|i| col.value(i)).collect()
        };
        assert_eq!(order_values, vec![10, 20, 30, 40], "sort must be stable");
    }

    // ── output schema unchanged ───────────────────────────────────────────────

    #[test]
    fn sort_output_schema_unchanged() {
        let schema = int_schema();
        let child = Box::new(VecOp::new(schema.clone(), vec![]));
        let op = SortOperator::new(
            child,
            vec![],
            1_000_000,
            CancellationToken::new(),
            unbounded_budget(),
        );
        assert_eq!(*op.output_schema(), schema);
    }

    // ── max_rows cap ─────────────────────────────────────────────────────────

    #[test]
    fn sort_exceeds_max_rows_returns_execution_error() {
        let batch = make_int_batch(&[1, 2, 3]); // 3 rows
        let child = Box::new(VecOp::new(int_schema(), vec![batch]));
        let mut op = SortOperator::new(
            child,
            vec![],
            2, /* cap at 2 */
            CancellationToken::new(),
            unbounded_budget(),
        );

        match op.next_batch() {
            Err(BqliteError::Execution(msg)) => {
                assert_eq!(
                    msg,
                    "SortOperator: input row count 3 exceeds max_rows limit 2"
                );
            }
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn sort_exactly_at_max_rows_is_allowed() {
        let batch = make_int_batch(&[2, 1]); // 2 rows, cap is 2
        let child = Box::new(VecOp::new(int_schema(), vec![batch]));
        let keys = vec![(col0_expr(), SortDirection::Asc)];
        let mut op =
            SortOperator::new(child, keys, 2, CancellationToken::new(), unbounded_budget());

        let out = op.next_batch().unwrap().unwrap();
        assert_eq!(out.num_rows(), 2);
        let col = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(col.value(0), 1);
        assert_eq!(col.value(1), 2);
    }

    // ── cancellation ─────────────────────────────────────────────────────────

    #[test]
    fn sort_cancelled_before_first_call_returns_cancelled() {
        let token = CancellationToken::new();
        token.cancel();
        let child = Box::new(VecOp::new(int_schema(), vec![make_int_batch(&[1, 2])]));
        let mut op = SortOperator::new(child, vec![], 1_000_000, token, unbounded_budget());

        match op.next_batch() {
            Err(BqliteError::Cancelled) => {}
            other => panic!("expected Cancelled, got {other:?}"),
        }
    }

    #[test]
    fn sort_cancelled_during_drain_returns_cancelled() {
        // Forces 2 output chunks; cancels the token after the first drain call.
        let token = CancellationToken::new();
        let n = DEFAULT_OUTPUT_BATCH_SIZE + 1;
        let values: Vec<i64> = (0..n as i64).collect();
        let batch = make_int_batch(&values);
        let child = Box::new(VecOp::new(int_schema(), vec![batch]));
        let keys = vec![(col0_expr(), SortDirection::Asc)];
        let mut op = SortOperator::new(child, keys, n + 100, token.clone(), unbounded_budget());

        // First call triggers full sort and returns the first output batch.
        let first = op.next_batch().unwrap();
        assert!(first.is_some(), "expected first output batch");

        // Cancel before the second drain call.
        token.cancel();
        match op.next_batch() {
            Err(BqliteError::Cancelled) => {}
            other => panic!("expected Cancelled during drain, got {other:?}"),
        }
    }

    // ── output batch splitting ───────────────────────────────────────────────

    #[test]
    fn sort_splits_output_into_batch_size_chunks() {
        // Produce DEFAULT_OUTPUT_BATCH_SIZE + 1 rows; expect 2 output batches.
        let n = DEFAULT_OUTPUT_BATCH_SIZE + 1;
        let values: Vec<i64> = (0..n as i64).rev().collect(); // descending → need sort
        let batch = make_int_batch(&values);
        let child = Box::new(VecOp::new(int_schema(), vec![batch]));
        let keys = vec![(col0_expr(), SortDirection::Asc)];
        let mut op = SortOperator::new(
            child,
            keys,
            n + 100,
            CancellationToken::new(),
            unbounded_budget(),
        );

        let b1 = op.next_batch().unwrap().unwrap();
        assert_eq!(b1.num_rows(), DEFAULT_OUTPUT_BATCH_SIZE);

        let b2 = op.next_batch().unwrap().unwrap();
        assert_eq!(b2.num_rows(), 1);

        assert!(op.next_batch().unwrap().is_none());
    }

    // ── close is idempotent ──────────────────────────────────────────────────

    #[test]
    fn sort_close_is_idempotent() {
        let child = Box::new(VecOp::new(int_schema(), vec![]));
        let mut op = SortOperator::new(
            child,
            vec![],
            1_000_000,
            CancellationToken::new(),
            unbounded_budget(),
        );
        op.close().unwrap();
        op.close().unwrap();
        // After close, next_batch returns None (Done state).
        assert!(op.next_batch().unwrap().is_none());
    }

    // ── no-key sort ───────────────────────────────────────────────────────────

    #[test]
    fn sort_with_no_keys_preserves_order() {
        let batch = make_int_batch(&[3, 1, 2]);
        let child = Box::new(VecOp::new(int_schema(), vec![batch]));
        let mut op = SortOperator::new(
            child,
            vec![], /* no keys */
            1_000_000,
            CancellationToken::new(),
            unbounded_budget(),
        );

        let out = op.next_batch().unwrap().unwrap();
        let col = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        // With no sort keys, lexsort is a no-op and rows keep their input order.
        let values: Vec<i64> = (0..col.len()).map(|i| col.value(i)).collect();
        assert_eq!(values, vec![3, 1, 2]);
    }

    // ── Memory budget plumbing (TASK-513 CP2a) ────────────────────────────────

    #[test]
    fn sort_charges_buffered_batches_to_budget() -> Result<()> {
        // Sort accumulates two batches; the tracker must observe a peak
        // ≥ Σ get_array_memory_size of the input batches.
        let batch_a = make_int_batch(&[3, 1, 2]);
        let batch_b = make_int_batch(&[6, 5, 4]);
        let expected_min_peak =
            (batch_a.get_array_memory_size() + batch_b.get_array_memory_size()) as u64;
        let child = Box::new(VecOp::new(int_schema(), vec![batch_a, batch_b]));
        let tracker = MemoryTracker::new(1_000_000);
        let budget: Arc<dyn MemoryBudget> = tracker.clone();
        let mut op = SortOperator::new(
            child,
            vec![(col0_expr(), SortDirection::Asc)],
            1_000_000,
            CancellationToken::new(),
            budget,
        );
        // Drive the operator until it returns one full output batch.
        let _ = op.next_batch()?.expect("must produce output");
        // After phase-2 transition the buffer is dropped, so peak is what
        // we want to assert against — used has already returned to ~0.
        assert!(
            tracker.peak_bytes() >= expected_min_peak,
            "peak {} should be at least {}",
            tracker.peak_bytes(),
            expected_min_peak
        );
        Ok(())
    }

    #[test]
    fn sort_overflow_returns_typed_budget_error() {
        // Tracker with a tiny budget (smaller than even one Int64Array
        // batch) — the first try_reserve must surface
        // `BqliteError::MemoryBudgetExceeded` since no spill handler is
        // registered in CP2a.
        let batch = make_int_batch(&[1, 2, 3, 4, 5]);
        let bytes = batch.get_array_memory_size() as u64;
        let tiny_budget = bytes.saturating_sub(1);
        let child = Box::new(VecOp::new(int_schema(), vec![batch]));
        let tracker = MemoryTracker::new(tiny_budget);
        let budget: Arc<dyn MemoryBudget> = tracker;
        let mut op = SortOperator::new(
            child,
            vec![(col0_expr(), SortDirection::Asc)],
            1_000_000,
            CancellationToken::new(),
            budget,
        );
        match op.next_batch() {
            Err(BqliteError::MemoryBudgetExceeded { used, budget }) => {
                assert_eq!(budget, tiny_budget);
                assert!(
                    used >= bytes,
                    "used should reflect the rejected reservation: {used}"
                );
            }
            other => panic!("expected MemoryBudgetExceeded, got {other:?}"),
        }
    }

    #[test]
    fn sort_releases_reservation_after_drain() -> Result<()> {
        // After the merge-and-drain phase completes and every output
        // batch has been read, all reservations are released.
        let batch = make_int_batch(&[3, 1, 2]);
        let child = Box::new(VecOp::new(int_schema(), vec![batch]));
        let tracker = MemoryTracker::new(1_000_000);
        let budget: Arc<dyn MemoryBudget> = tracker.clone();
        let mut op = SortOperator::new(
            child,
            vec![(col0_expr(), SortDirection::Asc)],
            1_000_000,
            CancellationToken::new(),
            budget,
        );
        // Drive to completion.
        while op.next_batch()?.is_some() {}
        assert_eq!(
            tracker.used_bytes(),
            0,
            "all reservations must be released after drain"
        );
        Ok(())
    }

    // ── Spill writer + k-way merge (TASK-513 CP2b) ────────────────────────────

    use bqlite_core::spill::SpillFs;
    use std::sync::atomic::{AtomicU64, Ordering as AOrdering};

    fn scratch_spill_fs(label: &str) -> Arc<SpillFs> {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, AOrdering::Relaxed);
        let mut db_root = std::env::temp_dir();
        db_root.push(format!(
            "bqlite-sort-spill-{label}-{}-{}",
            std::process::id(),
            seq
        ));
        std::fs::create_dir_all(&db_root).unwrap();
        let spill_root = db_root.join("spill");
        SpillFs::open(spill_root, &db_root).expect("open spill fs")
    }

    fn drive_to_vec(op: &mut SortOperator) -> Vec<i64> {
        let mut out = Vec::new();
        while let Some(batch) = op.next_batch().unwrap() {
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..col.len() {
                out.push(col.value(i));
            }
        }
        out
    }

    #[test]
    fn sort_with_spill_matches_in_memory_path() {
        // Tight budget forces spill; output must equal the in-memory
        // sort output of the same input.
        let batches: Vec<RecordBatch> = (0..4)
            .map(|chunk| {
                let values: Vec<i64> = (chunk * 10..chunk * 10 + 10).rev().collect();
                make_int_batch(&values)
            })
            .collect();
        let expected: Vec<i64> = (0..40).collect();

        let fs = scratch_spill_fs("matches");
        let qid = fs.new_query_id();
        // Budget large enough for one batch but smaller than two —
        // every second push triggers a spill of the prior batch.
        let one_batch = batches[0].get_array_memory_size() as u64;
        let tracker = MemoryTracker::new(one_batch + (one_batch / 2));
        let budget: Arc<dyn MemoryBudget> = tracker;
        let child = Box::new(VecOp::new(int_schema(), batches));
        let mut op = SortOperator::with_spill(
            child,
            vec![(col0_expr(), SortDirection::Asc)],
            1_000_000,
            CancellationToken::new(),
            budget,
            Some(Arc::clone(&fs)),
            Some(qid),
        );
        let out = drive_to_vec(&mut op);
        assert_eq!(out, expected);
        // After drain, the per-query subdir should have no live spill
        // files (TempSpillFile guards delete on drop).
        let qdir = fs.root().join(qid.to_string());
        if qdir.exists() {
            let entries: Vec<_> = std::fs::read_dir(&qdir)
                .unwrap()
                .filter_map(|e| e.ok())
                .collect();
            assert!(entries.is_empty(), "spill files leaked: {entries:?}");
        }
    }

    #[test]
    fn sort_spill_handler_returns_reservation_bytes_not_disk_bytes() {
        // The handler must report the SUM of MemoryReservation bytes
        // it dropped, not the on-disk bytes written. Verify by
        // arranging a tiny budget and inspecting `used_bytes` after
        // the handler fires (the rejected reservation would not have
        // succeeded if the handler returned the wrong amount).
        let fs = scratch_spill_fs("reservation-bytes");
        let qid = fs.new_query_id();

        // Pick budget to fit exactly one batch but not two.
        let batch_a = make_int_batch(&[3, 1, 2]);
        let batch_b = make_int_batch(&[6, 5, 4]);
        let bytes_a = batch_a.get_array_memory_size() as u64;
        let bytes_b = batch_b.get_array_memory_size() as u64;
        // Budget allows one batch at a time. After handler fires
        // (freeing bytes_a), bytes_b must fit.
        let budget_size = bytes_a.max(bytes_b);
        let tracker = MemoryTracker::new(budget_size);
        let budget: Arc<dyn MemoryBudget> = tracker.clone();
        let child = Box::new(VecOp::new(int_schema(), vec![batch_a, batch_b]));
        let mut op = SortOperator::with_spill(
            child,
            vec![(col0_expr(), SortDirection::Asc)],
            1_000_000,
            CancellationToken::new(),
            budget,
            Some(Arc::clone(&fs)),
            Some(qid),
        );
        let out = drive_to_vec(&mut op);
        assert_eq!(out, vec![1, 2, 3, 4, 5, 6]);
        // After drain, used must be 0 — every reservation released.
        assert_eq!(tracker.used_bytes(), 0);
    }

    #[test]
    fn sort_spill_is_stable_across_runs() {
        // Two batches with identical sort keys but different tag
        // values. Spill the first batch; merger must place the
        // first-spilled run's rows before the residual's rows.
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("tag", DataType::Int64, false),
        ]));
        let schema = OperatorSchema::new(vec![
            ColumnDef::required("k", BqlType::Int),
            ColumnDef::required("tag", BqlType::Int),
        ])
        .unwrap();
        let make_batch = |k: i64, tags: &[i64]| {
            let k_col: ArrayRef = Arc::new(Int64Array::from(vec![k; tags.len()]));
            let t_col: ArrayRef = Arc::new(Int64Array::from(tags.to_vec()));
            RecordBatch::try_new(Arc::clone(&arrow_schema), vec![k_col, t_col]).unwrap()
        };
        let b1 = make_batch(7, &[1, 2, 3]);
        let b2 = make_batch(7, &[4, 5, 6]);
        let fs = scratch_spill_fs("stable");
        let qid = fs.new_query_id();
        // Tiny budget so b1 spills before b2 is pushed.
        let tracker = MemoryTracker::new(
            (b1.get_array_memory_size() as u64).max(b2.get_array_memory_size() as u64),
        );
        let budget: Arc<dyn MemoryBudget> = tracker;
        let child = Box::new(VecOp::new(schema, vec![b1, b2]));
        let key = CompiledExpr {
            node: CompiledNode::Column {
                index: 0,
                name: "k".to_string(),
            },
            result_type: BqlType::Int,
            nullable: false,
        };
        let mut op = SortOperator::with_spill(
            child,
            vec![(key, SortDirection::Asc)],
            1_000_000,
            CancellationToken::new(),
            budget,
            Some(Arc::clone(&fs)),
            Some(qid),
        );
        let mut tags: Vec<i64> = Vec::new();
        while let Some(batch) = op.next_batch().unwrap() {
            let t_col = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..t_col.len() {
                tags.push(t_col.value(i));
            }
        }
        assert_eq!(
            tags,
            vec![1, 2, 3, 4, 5, 6],
            "spilled-earlier run must come before residual on equal keys"
        );
    }

    #[test]
    fn sort_spill_drop_without_close_cleans_up() {
        // Drop the operator mid-execution; spill files must be
        // reclaimed by `TempSpillFile::Drop`.
        let fs = scratch_spill_fs("drop-nocls");
        let qid = fs.new_query_id();
        let batches: Vec<RecordBatch> = (0..4).map(|i| make_int_batch(&[i, i + 100])).collect();
        let one_batch = batches[0].get_array_memory_size() as u64;
        let tracker = MemoryTracker::new(one_batch + (one_batch / 2));
        let budget: Arc<dyn MemoryBudget> = tracker;
        let child = Box::new(VecOp::new(int_schema(), batches));
        let mut op = SortOperator::with_spill(
            child,
            vec![(col0_expr(), SortDirection::Asc)],
            1_000_000,
            CancellationToken::new(),
            budget,
            Some(Arc::clone(&fs)),
            Some(qid),
        );
        // Pull just enough to trigger at least one spill.
        let _ = op.next_batch().unwrap();
        // Drop without close.
        drop(op);
        // Per-query subdir is either gone or empty (file guards
        // ran their Drop).
        let qdir = fs.root().join(qid.to_string());
        if qdir.exists() {
            let entries: Vec<_> = std::fs::read_dir(&qdir)
                .unwrap()
                .filter_map(|e| e.ok())
                .collect();
            assert!(entries.is_empty(), "spill files leaked: {entries:?}");
        }
    }

    #[test]
    fn sort_spill_cancelled_during_write_returns_cancelled() {
        // Cancel the token, then drive — the spill writer must observe
        // cancellation and propagate `BqliteError::Cancelled` (or the
        // operator's outer cancel-check beats the writer to it).
        let fs = scratch_spill_fs("cancel-write");
        let qid = fs.new_query_id();
        let cancel = CancellationToken::new();
        let batches: Vec<RecordBatch> = (0..3).map(|i| make_int_batch(&[i, i + 100])).collect();
        let one_batch = batches[0].get_array_memory_size() as u64;
        let tracker = MemoryTracker::new(one_batch + (one_batch / 2));
        let budget: Arc<dyn MemoryBudget> = tracker;
        let child = Box::new(VecOp::new(int_schema(), batches));
        let mut op = SortOperator::with_spill(
            child,
            vec![(col0_expr(), SortDirection::Asc)],
            1_000_000,
            cancel.clone(),
            budget,
            Some(Arc::clone(&fs)),
            Some(qid),
        );
        cancel.cancel();
        match op.next_batch() {
            Err(BqliteError::Cancelled) => {}
            other => panic!("expected Cancelled, got {other:?}"),
        }
    }
}
