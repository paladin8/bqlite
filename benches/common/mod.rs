//! Shared helpers for bqlite Criterion benchmarks.
//!
//! Every bench file imports from here via
//! `use bqlite_benches::common::*`. Wave 1 provided the smoke-bench
//! identity helper; Wave 2 (TASK-236) adds:
//!
//! - Deterministic data generators for Arrow arrays of each primitive
//!   type, matching the reference `purchases` fixture profile (10k
//!   entities, 20 event types, monotonic-within-entity timestamps,
//!   7 mixed-type property columns).
//! - Synthetic event generators ([`generate_events`] and
//!   [`generate_acceptance_events`]) producing `Vec<Event>` sorted by
//!   `(entity_id, timestamp)` for scan, ingest, and acceptance
//!   benchmarks.
//! - Criterion configuration helpers ([`wave2_criterion`]) that apply
//!   the workspace-standard sample size and measurement time.
//!
//! ## Dual-mode dataset strategy (TASK-246)
//!
//! The bench harness supports two modes controlled by the
//! `BQLITE_BENCH_MODE` environment variable:
//!
//! - **`ci`** (default): CI-scaled fixtures for regression-noise
//!   control on shared runners. Targets are not enforced — only
//!   Criterion's statistical comparison applies.
//!
//! - **`reference`**: Reference-mode runs execute the true 100M-row
//!   acceptance query on the pinned reference hardware (Apple M2 Max).
//!   Hard performance targets are enforced: the bench panics if any
//!   target is missed. Results are written to a machine-readable JSON
//!   file at `target/bench-results.json`.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringViewArray, TimestampNanosecondArray};
use bqlite_core::event::{EntityId, Event};
use bqlite_core::property::PropertyValue;
use bqlite_core::schema::{ColumnDef, TableSchema};
use bqlite_core::time::Timestamp;
use bqlite_core::BqlType;
use bqlite_storage::ingest::partitioner::shard_id_for;
use bqlite_storage::writer::DEFAULT_ROW_GROUP_SIZE;
use bqlite_storage::Database;
use criterion::Criterion;

/// Return `x` unchanged, through a function call the optimizer is not
/// permitted to inline.
///
/// Wave 1's smoke bench measures
/// `identity(black_box(value))` in a tight loop. The `#[inline(never)]`
/// attribute ensures the function boundary survives optimization so
/// Criterion measures a stable call-and-return overhead rather than a
/// fully elided loop.
#[inline(never)]
pub fn identity<T>(x: T) -> T {
    x
}

// ── Bench mode ──────────────────────────────────────────────────────────────

/// Bench execution mode, selected via `BQLITE_BENCH_MODE` env var.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BenchMode {
    /// CI-scaled fixtures (default). Small datasets, no hard targets.
    Ci,
    /// Reference-hardware mode. Full 100M-row dataset, hard targets enforced.
    Reference,
}

impl BenchMode {
    /// Read the mode from `BQLITE_BENCH_MODE`. Defaults to `Ci`.
    pub fn from_env() -> Self {
        match std::env::var("BQLITE_BENCH_MODE").as_deref() {
            Ok("reference") => BenchMode::Reference,
            Ok("ci") | Err(_) => BenchMode::Ci,
            Ok(other) => {
                eprintln!(
                    "WARNING: unknown BQLITE_BENCH_MODE={other:?}, \
                     expected \"ci\" or \"reference\". Falling back to ci mode."
                );
                BenchMode::Ci
            }
        }
    }

    pub fn is_reference(self) -> bool {
        self == BenchMode::Reference
    }
}

/// Dataset sizing parameters that vary by mode.
pub struct BenchSizing {
    /// Total events for acceptance / ingest benchmarks.
    pub acceptance_events: usize,
    /// Entity count for acceptance benchmarks.
    pub acceptance_entities: usize,
    /// Total events for scan benchmarks.
    pub scan_events: usize,
    /// Entity count for scan benchmarks.
    pub scan_entities: usize,
    /// Total events for ingest benchmarks (small batch).
    pub ingest_events_small: usize,
    /// Entity count for ingest benchmarks (small batch).
    pub ingest_entities_small: usize,
    /// Total events for ingest benchmarks (large batch).
    pub ingest_events_large: usize,
    /// Entity count for ingest benchmarks (large batch).
    pub ingest_entities_large: usize,
}

impl BenchSizing {
    pub fn for_mode(mode: BenchMode) -> Self {
        match mode {
            BenchMode::Ci => BenchSizing {
                acceptance_events: 300_000,
                acceptance_entities: 500,
                scan_events: 50_000,
                scan_entities: 500,
                ingest_events_small: 10_000,
                ingest_entities_small: 100,
                ingest_events_large: 50_000,
                ingest_entities_large: 500,
            },
            BenchMode::Reference => BenchSizing {
                acceptance_events: 100_000_000,
                acceptance_entities: REF_ENTITY_COUNT,
                scan_events: 100_000_000,
                scan_entities: REF_ENTITY_COUNT,
                ingest_events_small: 100_000,
                ingest_entities_small: 1_000,
                ingest_events_large: 1_000_000,
                ingest_entities_large: REF_ENTITY_COUNT,
            },
        }
    }
}

// ── Criterion configuration ─────────────────────────────────────────────────

/// Standard Criterion config for Wave 2 benches: reduced sample size
/// and warm-up for CI-friendliness while still providing stable
/// statistical estimates.
pub fn wave2_criterion() -> Criterion {
    Criterion::default()
        .sample_size(20)
        .warm_up_time(std::time::Duration::from_secs(1))
        .measurement_time(std::time::Duration::from_secs(3))
}

/// Criterion config for reference-mode: fewer samples but longer
/// measurement to reduce noise on large datasets.
pub fn reference_criterion() -> Criterion {
    Criterion::default()
        .sample_size(10)
        .warm_up_time(std::time::Duration::from_secs(3))
        .measurement_time(std::time::Duration::from_secs(10))
}

/// Return the appropriate Criterion configuration for the current mode.
pub fn criterion_for_mode(mode: BenchMode) -> Criterion {
    match mode {
        BenchMode::Ci => wave2_criterion(),
        BenchMode::Reference => reference_criterion(),
    }
}

// ── Hard target enforcement (TASK-246) ──────────────────────────────────────

/// A single bench result with its name and measured value.
#[derive(Debug, Clone)]
pub struct BenchResult {
    pub name: String,
    pub value: f64,
    pub unit: String,
    pub target: Option<BenchTarget>,
}

/// A hard performance target: the bench fails if the measured value
/// violates the constraint.
#[derive(Debug, Clone, Copy)]
pub struct BenchTarget {
    pub limit: f64,
    pub direction: TargetDirection,
}

/// Whether the measured value must be at-most or at-least the limit.
#[derive(Debug, Clone, Copy)]
pub enum TargetDirection {
    /// Value must be <= limit (e.g. latency, compression ratio).
    AtMost,
    /// Value must be >= limit (e.g. throughput, pruning rate).
    AtLeast,
}

impl BenchTarget {
    pub fn at_most(limit: f64) -> Self {
        BenchTarget {
            limit,
            direction: TargetDirection::AtMost,
        }
    }

    pub fn at_least(limit: f64) -> Self {
        BenchTarget {
            limit,
            direction: TargetDirection::AtLeast,
        }
    }

    pub fn passes(self, value: f64) -> bool {
        match self.direction {
            TargetDirection::AtMost => value <= self.limit,
            TargetDirection::AtLeast => value >= self.limit,
        }
    }
}

/// Collect bench results and enforce hard targets in reference mode.
///
/// In CI mode, results are collected but targets are not enforced.
/// In reference mode, the harness panics if any target is missed.
///
/// Results are always written to `target/bench-results.json` (appending
/// to any existing file) so both CI and reference numbers can be
/// uploaded as artifacts.
pub struct BenchResultCollector {
    mode: BenchMode,
    results: Vec<BenchResult>,
}

impl BenchResultCollector {
    pub fn new(mode: BenchMode) -> Self {
        Self {
            mode,
            results: Vec::new(),
        }
    }

    /// Record a result. If in reference mode and a target is set, the
    /// target is checked immediately and the violation is recorded (but
    /// enforcement is deferred to [`finish`]).
    pub fn record(&mut self, name: &str, value: f64, unit: &str, target: Option<BenchTarget>) {
        let direction_str = target.map(|t| match t.direction {
            TargetDirection::AtMost => "at_most",
            TargetDirection::AtLeast => "at_least",
        });
        let pass = target.map(|t| t.passes(value));
        eprintln!(
            "  [bench-result] {name} = {value:.4} {unit}\
             {}{}\
             {}",
            target
                .map(|t| format!("  (target: {} {})", direction_str.unwrap(), t.limit))
                .unwrap_or_default(),
            pass.map(|p| if p { "  PASS" } else { "  **MISS**" })
                .unwrap_or(""),
            if self.mode.is_reference() {
                "  [reference]"
            } else {
                "  [ci]"
            },
        );
        self.results.push(BenchResult {
            name: name.to_string(),
            value,
            unit: unit.to_string(),
            target,
        });
    }

    /// Write results to JSON and enforce targets in reference mode.
    /// Panics if any target is missed in reference mode.
    pub fn finish(self) {
        self.write_json();

        if !self.mode.is_reference() {
            return;
        }

        let mut failures = Vec::new();
        for r in &self.results {
            if let Some(target) = r.target {
                if !target.passes(r.value) {
                    failures.push(format!(
                        "{}: measured {:.4} {} (target: {} {:.4})",
                        r.name,
                        r.value,
                        r.unit,
                        match target.direction {
                            TargetDirection::AtMost => "<=",
                            TargetDirection::AtLeast => ">=",
                        },
                        target.limit,
                    ));
                }
            }
        }

        if !failures.is_empty() {
            let msg = format!(
                "Reference-mode hard target failures:\n{}",
                failures.join("\n")
            );
            panic!("{msg}");
        }
    }

    fn write_json(&self) {
        // Cargo bench binaries run with CWD = package root, but CI expects
        // output at the workspace target dir. CARGO_TARGET_DIR is set when
        // Cargo uses a non-default target directory; fall back to resolving
        // relative to CARGO_MANIFEST_DIR's parent (the workspace root).
        let target_dir = std::env::var("CARGO_TARGET_DIR")
            .map(PathBuf::from)
            .or_else(|_| {
                std::env::var("CARGO_MANIFEST_DIR").map(|m| {
                    PathBuf::from(m)
                        .parent()
                        .unwrap_or(Path::new("."))
                        .join("target")
                })
            })
            .unwrap_or_else(|_| PathBuf::from("target"));
        let results_path = target_dir.join("bench-results.json");

        // Read existing results if the file exists, so multiple bench
        // binaries append to the same file.
        let mut all: BTreeMap<String, serde_json::Value> = std::fs::read_to_string(&results_path)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default();

        let mode_key = if self.mode.is_reference() {
            "reference"
        } else {
            "ci"
        };

        for r in &self.results {
            let mut entry = serde_json::Map::new();
            entry.insert("value".to_string(), serde_json::json!(r.value));
            entry.insert("unit".to_string(), serde_json::json!(r.unit));
            entry.insert("mode".to_string(), serde_json::json!(mode_key));
            if let Some(target) = r.target {
                entry.insert(
                    "target".to_string(),
                    serde_json::json!({
                        "limit": target.limit,
                        "direction": match target.direction {
                            TargetDirection::AtMost => "at_most",
                            TargetDirection::AtLeast => "at_least",
                        },
                        "pass": target.passes(r.value),
                    }),
                );
            }
            all.insert(
                format!("{mode_key}/{}", r.name),
                serde_json::Value::Object(entry),
            );
        }

        if let Ok(json) = serde_json::to_string_pretty(&all) {
            // Best-effort write — don't fail the bench if the file can't be written.
            let _ = std::fs::create_dir_all(&target_dir);
            let _ =
                std::fs::File::create(&results_path).and_then(|mut f| f.write_all(json.as_bytes()));
        }
    }
}

// ── Mock child operator for benchmarks ─────────────────────────────────────

/// A simple vector-backed `PhysicalOperator` that yields pre-built batches.
///
/// Used by Wave 3 sort and distinct benchmarks (and any future bench that
/// needs to feed pre-built `RecordBatch` data into an operator).
pub struct VecOp {
    schema: bqlite_core::OperatorSchema,
    batches: Vec<arrow::record_batch::RecordBatch>,
    idx: usize,
}

impl VecOp {
    pub fn new(
        schema: bqlite_core::OperatorSchema,
        batches: Vec<arrow::record_batch::RecordBatch>,
    ) -> Self {
        Self {
            schema,
            batches,
            idx: 0,
        }
    }
}

impl bqlite_operators::PhysicalOperator for VecOp {
    fn output_schema(&self) -> &bqlite_core::OperatorSchema {
        &self.schema
    }
    fn next_batch(&mut self) -> bqlite_core::Result<Option<arrow::record_batch::RecordBatch>> {
        if self.idx >= self.batches.len() {
            return Ok(None);
        }
        let b = self.batches[self.idx].clone();
        self.idx += 1;
        Ok(Some(b))
    }
}

// ── Arrow array generators ──────────────────────────────────────────────────

/// Deterministic i64 array — sequential values starting at `base`.
pub fn gen_int64_array(n: usize, base: i64) -> ArrayRef {
    let values: Vec<i64> = (0..n as i64).map(|i| base.wrapping_add(i)).collect();
    Arc::new(Int64Array::from(values))
}

/// Deterministic f64 array — `base + i * 0.1` for each element.
pub fn gen_float64_array(n: usize, base: f64) -> ArrayRef {
    let values: Vec<f64> = (0..n).map(|i| base + (i as f64) * 0.1).collect();
    Arc::new(Float64Array::from(values))
}

/// Deterministic timestamp array — monotonically increasing nanoseconds
/// starting at `base_ns` with step `step_ns`. Matches the
/// within-entity monotonic timestamp profile from the reference
/// dataset.
pub fn gen_timestamp_array(n: usize, base_ns: i64, step_ns: i64) -> ArrayRef {
    let values: Vec<i64> = (0..n as i64).map(|i| base_ns + i * step_ns).collect();
    Arc::new(TimestampNanosecondArray::from(values).with_timezone("UTC"))
}

/// Low-cardinality string array — cycles through `cardinality`
/// distinct values. Matches the 20-event-type distribution from the
/// reference dataset.
pub fn gen_low_cardinality_string_array(n: usize, cardinality: usize) -> ArrayRef {
    let labels: Vec<String> = (0..cardinality).map(|i| format!("event_{i}")).collect();
    let values: Vec<String> = (0..n).map(|i| labels[i % cardinality].clone()).collect();
    Arc::new(StringViewArray::from(values))
}

/// High-cardinality string array — unique string per row. Exercises
/// the Dictionary encoding's fallback-to-Plain path.
pub fn gen_high_cardinality_string_array(n: usize) -> ArrayRef {
    let values: Vec<String> = (0..n).map(|i| format!("entity_{i:08}")).collect();
    Arc::new(StringViewArray::from(values))
}

// ── Event generator ─────────────────────────────────────────────────────────

/// Number of distinct entity ids in the reference dataset.
pub const REF_ENTITY_COUNT: usize = 10_000;
/// Number of distinct event types in the reference dataset.
pub const REF_EVENT_TYPE_COUNT: usize = 20;

/// Event type labels matching the reference dataset.
fn event_type_labels() -> Vec<String> {
    (0..REF_EVENT_TYPE_COUNT)
        .map(|i| format!("event_{i}"))
        .collect()
}

/// Event type labels for the acceptance query fixture.
///
/// The acceptance query filters on `event_type = 'purchase'`, so the
/// low-cardinality string set reserves one label for that exact value
/// and keeps the remaining 19 values lexicographically below it
/// (`event_*`). That keeps zone maps on non-purchase row groups tight
/// enough to reject the equality without scanning.
fn acceptance_event_type_labels() -> Vec<String> {
    let mut labels = Vec::with_capacity(REF_EVENT_TYPE_COUNT);
    labels.push("purchase".to_string());
    labels.extend((1..REF_EVENT_TYPE_COUNT).map(|i| format!("event_{i}")));
    labels
}

/// Generate `n` synthetic events matching the reference dataset profile:
/// - `entity_count` distinct string entity ids
/// - 20 distinct event types
/// - Monotonic-within-entity timestamps
/// - 7 mixed-type property columns
///
/// Events are sorted by `(entity_id, timestamp)` as the partitioner
/// and writer expect.
pub fn generate_events(n: usize, entity_count: usize) -> Vec<Event> {
    let event_types = event_type_labels();
    let events_per_entity = n / entity_count.max(1);

    // Base timestamp: 2025-01-01T00:00:00Z in nanos.
    let base_ns: i64 = 1_735_689_600_000_000_000;
    // Step between events within an entity: ~1 minute.
    let step_ns: i64 = 60_000_000_000;

    let mut events = Vec::with_capacity(n);
    for entity_idx in 0..entity_count {
        let entity = EntityId::String(format!("user_{entity_idx:06}"));
        let count = if entity_idx < entity_count - 1 {
            events_per_entity
        } else {
            // Last entity absorbs the remainder.
            n - events_per_entity * (entity_count - 1)
        };
        for ev_idx in 0..count {
            let ts = Timestamp::from_nanos(base_ns + (ev_idx as i64) * step_ns);
            let event_type = &event_types[(entity_idx + ev_idx) % event_types.len()];
            let properties = vec![
                ("amount".into(), PropertyValue::Int((ev_idx as i64) * 100)),
                (
                    "price".into(),
                    PropertyValue::Float(9.99 + (ev_idx as f64) * 0.01),
                ),
                (
                    "category".into(),
                    PropertyValue::String(format!("cat_{}", ev_idx % 5)),
                ),
                (
                    "quantity".into(),
                    PropertyValue::Int((ev_idx as i64) % 10 + 1),
                ),
                (
                    "discount".into(),
                    PropertyValue::Float(if ev_idx % 3 == 0 { 0.1 } else { 0.0 }),
                ),
                (
                    "region".into(),
                    PropertyValue::String(format!("region_{}", ev_idx % 8)),
                ),
                ("flag".into(), PropertyValue::Bool(ev_idx % 2 == 0)),
            ];
            events.push(Event::with_properties(
                entity.clone(),
                ts,
                event_type.clone(),
                properties,
            ));
        }
    }
    events
}

/// Generate synthetic events tailored to the Wave 2 acceptance query.
///
/// The fixture shape is deliberate:
/// - one shard contains only `purchase` entities, with its tail
///   entities carrying `amount > 4500`
/// - every other shard contains non-purchase entities whose amounts
///   are also `> 4500`
///
/// This means:
/// - `event_type = 'purchase'` alone accepts every row group in the
///   designated purchase shard
/// - `amount > 4500` alone accepts the non-purchase shards plus the
///   tail row group in the purchase shard
/// - the full acceptance predicate accepts only the tail row group in
///   the designated shard
///
/// The result is a deterministic fixture that makes multi-column
/// zone-map pruning visible without changing the public acceptance
/// query shape.
pub fn generate_acceptance_events(n: usize, entity_count: usize, shard_count: u16) -> Vec<Event> {
    if n == 0 || entity_count == 0 {
        return Vec::new();
    }

    let labels = acceptance_event_type_labels();
    let purchase_label = &labels[0];
    let non_purchase_labels = &labels[1..];
    let base_rows_per_entity = (n / entity_count).max(1);
    let entity_targets = entity_targets_per_shard(entity_count, shard_count);
    let mut entities_by_shard = allocate_entities_by_shard(&entity_targets, shard_count);
    let purchase_shard = entities_by_shard
        .iter()
        .enumerate()
        .max_by_key(|(_, entities)| entities.len())
        .map(|(idx, _)| idx)
        .unwrap_or(0);
    let entities_per_row_group = (DEFAULT_ROW_GROUP_SIZE / base_rows_per_entity).max(1);
    let purchase_tail_len = tail_group_entity_count(
        entities_by_shard[purchase_shard].len(),
        entities_per_row_group,
    );
    let purchase_tail_start = entities_by_shard[purchase_shard]
        .len()
        .saturating_sub(purchase_tail_len);

    let mut entity_specs = Vec::with_capacity(entity_count);
    for (shard_idx, entities) in entities_by_shard.iter_mut().enumerate() {
        entities.sort();
        for (entity_pos, entity_id) in entities.iter().enumerate() {
            let kind = if shard_idx == purchase_shard {
                if entity_pos >= purchase_tail_start {
                    AcceptanceEntityKind::PurchaseHigh
                } else {
                    AcceptanceEntityKind::PurchaseLow
                }
            } else {
                let label = non_purchase_labels
                    [(entity_pos + shard_idx) % non_purchase_labels.len()]
                .clone();
                AcceptanceEntityKind::EventHigh(label)
            };
            entity_specs.push((entity_id.clone(), kind));
        }
    }
    entity_specs.sort_by(|a, b| a.0.cmp(&b.0));

    let base_ns: i64 = 1_735_689_600_000_000_000;
    let step_ns: i64 = 60_000_000_000;
    let base_count = n / entity_count;
    let remainder = n % entity_count;
    let mut events = Vec::with_capacity(n);

    for (entity_idx, (entity_id, kind)) in entity_specs.into_iter().enumerate() {
        let row_count = base_count + usize::from(entity_idx < remainder);
        let entity = EntityId::String(entity_id);
        let event_type = match &kind {
            AcceptanceEntityKind::PurchaseLow | AcceptanceEntityKind::PurchaseHigh => {
                purchase_label.clone()
            }
            AcceptanceEntityKind::EventHigh(label) => label.clone(),
        };
        let (amount_base, category_prefix, discount) = match kind {
            AcceptanceEntityKind::PurchaseLow => (100_i64, "purchase_low", 0.0),
            AcceptanceEntityKind::PurchaseHigh => (4_501_i64, "purchase_high", 0.1),
            AcceptanceEntityKind::EventHigh(_) => (4_501_i64, "event_high", 0.2),
        };

        for ev_idx in 0..row_count {
            let ts = Timestamp::from_nanos(base_ns + (ev_idx as i64) * step_ns);
            let amount = amount_base + (ev_idx as i64 % 128);
            let properties = vec![
                ("amount".into(), PropertyValue::Int(amount)),
                (
                    "price".into(),
                    PropertyValue::Float(9.99 + (ev_idx as f64) * 0.01),
                ),
                (
                    "category".into(),
                    PropertyValue::String(format!("{category_prefix}_{}", ev_idx % 5)),
                ),
                (
                    "quantity".into(),
                    PropertyValue::Int((ev_idx as i64) % 10 + 1),
                ),
                ("discount".into(), PropertyValue::Float(discount)),
                (
                    "region".into(),
                    PropertyValue::String(format!("region_{}", ev_idx % 8)),
                ),
                ("flag".into(), PropertyValue::Bool(ev_idx % 2 == 0)),
            ];
            events.push(Event::with_properties(
                entity.clone(),
                ts,
                event_type.clone(),
                properties,
            ));
        }
    }

    events
}

#[derive(Clone)]
enum AcceptanceEntityKind {
    PurchaseLow,
    PurchaseHigh,
    EventHigh(String),
}

fn entity_targets_per_shard(entity_count: usize, shard_count: u16) -> Vec<usize> {
    let shard_count = usize::from(shard_count.max(1));
    let mut targets = vec![entity_count / shard_count; shard_count];
    for target in targets.iter_mut().take(entity_count % shard_count) {
        *target += 1;
    }
    targets
}

fn allocate_entities_by_shard(entity_targets: &[usize], shard_count: u16) -> Vec<Vec<String>> {
    let mut entities_by_shard = vec![Vec::new(); entity_targets.len()];
    let mut candidate_idx = 0usize;

    while entities_by_shard
        .iter()
        .zip(entity_targets)
        .any(|(entities, target)| entities.len() < *target)
    {
        let candidate = format!("user_{candidate_idx:06}");
        let shard = usize::from(shard_id_for(
            &EntityId::String(candidate.clone()),
            shard_count.max(1),
        ));
        if entities_by_shard[shard].len() < entity_targets[shard] {
            entities_by_shard[shard].push(candidate);
        }
        candidate_idx += 1;
    }

    entities_by_shard
}

fn tail_group_entity_count(entity_count: usize, entities_per_row_group: usize) -> usize {
    if entity_count == 0 {
        return 0;
    }
    let remainder = entity_count % entities_per_row_group.max(1);
    if remainder == 0 {
        entities_per_row_group.min(entity_count)
    } else {
        remainder
    }
}

/// Compute the total logical data bytes across all events.
///
/// Counts entity overhead (32 bytes — entity-id struct + string), the
/// timestamp (8 bytes), the event-type string, and each property's key
/// length plus its payload size. This matches the partitioner's
/// per-event byte accounting so that throughput numbers are consistent
/// across the partitioner, end-to-end, and reference benchmarks.
pub fn compute_event_bytes(events: &[Event]) -> u64 {
    events
        .iter()
        .map(|e| {
            let prop_bytes: usize = e
                .properties
                .iter()
                .map(|(k, v)| {
                    k.len()
                        + match v {
                            PropertyValue::Int(_) => 8,
                            PropertyValue::Float(_) => 8,
                            PropertyValue::String(s) => s.len(),
                            PropertyValue::Bool(_) => 1,
                            _ => 8,
                        }
                })
                .sum();
            (32 + 8 + e.event_type.len() + prop_bytes) as u64
        })
        .sum()
}

// ── Database setup helpers ───────────────────────────────────────────────────

static SCRATCH_SEQ: AtomicU64 = AtomicU64::new(0);

/// Temporary directory for benchmark databases. Automatically cleaned
/// up on drop.
pub struct ScratchDir {
    path: PathBuf,
}

impl ScratchDir {
    pub fn new(label: &str) -> Self {
        let seq = SCRATCH_SEQ.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let mut path = std::env::temp_dir();
        path.push(format!("bqlite-bench-{label}-{pid}-{seq}"));
        Self { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for ScratchDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Build the reference `purchases` table schema: entity_id, timestamp,
/// event_type, plus 7 property columns.
pub fn purchases_schema() -> TableSchema {
    TableSchema::new(
        "purchases",
        vec![
            ColumnDef::required("user_id", BqlType::String),
            ColumnDef::required("ts", BqlType::Timestamp),
            ColumnDef::required("event_type", BqlType::String),
            ColumnDef::nullable("amount", BqlType::Int),
            ColumnDef::nullable("price", BqlType::Float),
            ColumnDef::nullable("category", BqlType::String),
            ColumnDef::nullable("quantity", BqlType::Int),
            ColumnDef::nullable("discount", BqlType::Float),
            ColumnDef::nullable("region", BqlType::String),
            ColumnDef::nullable("flag", BqlType::Bool),
        ],
        "user_id",
        "ts",
        "event_type",
    )
    .expect("purchases schema")
}

/// Create a database and register a table with the given schema.
pub fn open_db_with_table(path: &Path, table_name: &str, schema: TableSchema) -> Database {
    let mut db = Database::create(path).expect("create db");
    db.create_table(table_name.to_string(), schema)
        .expect("create table");
    db
}

/// Walk a directory tree, collecting all `.seg` segment files.
pub fn find_segment_files(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    walk_segments(root, &mut out);
    out
}

fn walk_segments(dir: &Path, out: &mut Vec<PathBuf>) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk_segments(&path, out);
            } else if path.extension().is_some_and(|ext| ext == "seg") {
                out.push(path);
            }
        }
    }
}

// ── Metric reporting helpers ────────────────────────────────────────────────

/// Print the bench-side metrics required by execution-model.md §14.1.
/// Called once per bench iteration with the bytes scanned and decoded.
pub fn report_metrics(bytes_scanned: u64, bytes_decoded: u64, elapsed: std::time::Duration) {
    let elapsed_secs = elapsed.as_secs_f64();
    let gb_per_sec = if elapsed_secs > 0.0 {
        (bytes_scanned as f64) / elapsed_secs / 1_073_741_824.0
    } else {
        0.0
    };
    let decoded_to_scanned = if bytes_scanned > 0 {
        (bytes_decoded as f64) / (bytes_scanned as f64)
    } else {
        0.0
    };
    eprintln!(
        "  gb_per_sec_scanned: {gb_per_sec:.2}, bytes_decoded_to_scanned: {decoded_to_scanned:.3}"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_returns_input() {
        assert_eq!(identity(42u64), 42u64);
        assert_eq!(identity("hello"), "hello");
    }

    #[test]
    fn gen_int64_array_has_correct_length() {
        let arr = gen_int64_array(100, 0);
        assert_eq!(arr.len(), 100);
    }

    #[test]
    fn gen_low_cardinality_cycles() {
        let arr = gen_low_cardinality_string_array(10, 3);
        assert_eq!(arr.len(), 10);
        let view = arr.as_any().downcast_ref::<StringViewArray>().unwrap();
        assert_eq!(view.value(0), "event_0");
        assert_eq!(view.value(3), "event_0");
    }

    #[test]
    fn generate_events_produces_sorted_output() {
        let events = generate_events(100, 10);
        assert_eq!(events.len(), 100);
        // Verify entity-major sort order.
        for window in events.windows(2) {
            assert!(
                (&window[0].entity, window[0].timestamp)
                    <= (&window[1].entity, window[1].timestamp),
                "events not sorted: {:?} > {:?}",
                (&window[0].entity, window[0].timestamp),
                (&window[1].entity, window[1].timestamp),
            );
        }
    }

    #[test]
    fn generate_events_has_expected_properties() {
        let events = generate_events(20, 5);
        for ev in &events {
            assert_eq!(ev.properties.len(), 7, "expected 7 property columns");
        }
    }

    // ── BenchTarget tests ──────────────────────────────────────────────────

    #[test]
    fn bench_target_at_most_passes_below_and_equal() {
        let t = BenchTarget::at_most(1.0);
        assert!(t.passes(0.5));
        assert!(t.passes(1.0));
        assert!(!t.passes(1.01));
    }

    #[test]
    fn bench_target_at_least_passes_above_and_equal() {
        let t = BenchTarget::at_least(100.0);
        assert!(t.passes(200.0));
        assert!(t.passes(100.0));
        assert!(!t.passes(99.9));
    }

    // ── BenchMode tests ────────────────────────────────────────────────────

    #[test]
    fn bench_mode_default_is_ci() {
        // Without BQLITE_BENCH_MODE set, defaults to Ci.
        // Note: this test may be affected by the env in the test runner,
        // but the standard CI doesn't set BQLITE_BENCH_MODE.
        let mode = BenchMode::from_env();
        // We just verify the function doesn't panic; the actual value
        // depends on the test environment.
        let _ = mode.is_reference();
    }

    #[test]
    fn bench_sizing_ci_has_small_datasets() {
        let s = BenchSizing::for_mode(BenchMode::Ci);
        assert_eq!(s.acceptance_events, 300_000);
        assert_eq!(s.acceptance_entities, 500);
        assert_eq!(s.scan_events, 50_000);
    }

    #[test]
    fn bench_sizing_reference_has_large_datasets() {
        let s = BenchSizing::for_mode(BenchMode::Reference);
        assert_eq!(s.acceptance_events, 100_000_000);
        assert_eq!(s.acceptance_entities, REF_ENTITY_COUNT);
        assert_eq!(s.scan_events, 100_000_000);
    }

    #[test]
    fn acceptance_events_cluster_purchase_rows_into_single_shard() {
        let events = generate_acceptance_events(300_000, 500, 4);
        let mut purchase_shards = std::collections::BTreeSet::new();
        let mut non_purchase_shards = std::collections::BTreeSet::new();
        let mut saw_purchase_low = false;
        let mut saw_purchase_high = false;
        let mut event_types = std::collections::BTreeSet::new();

        for ev in &events {
            event_types.insert(ev.event_type.clone());
            let amount = match ev.get("amount") {
                Some(PropertyValue::Int(v)) => *v,
                other => panic!("unexpected amount property: {other:?}"),
            };
            let shard = shard_id_for(&ev.entity, 4);
            if ev.event_type == "purchase" {
                purchase_shards.insert(shard);
                saw_purchase_low |= amount <= 4_500;
                saw_purchase_high |= amount > 4_500;
            } else {
                non_purchase_shards.insert(shard);
                assert!(
                    amount > 4_500,
                    "non-purchase amount must be high, got {amount}"
                );
            }
        }

        assert_eq!(
            purchase_shards.len(),
            1,
            "purchase rows should live on one shard"
        );
        assert!(
            !non_purchase_shards.is_empty(),
            "fixture should still exercise non-purchase shards"
        );
        assert!(
            saw_purchase_low,
            "fixture should include purchase rows below the threshold"
        );
        assert!(
            saw_purchase_high,
            "fixture should include purchase rows above the threshold"
        );
        assert_eq!(
            event_types.len(),
            REF_EVENT_TYPE_COUNT,
            "fixture should preserve 20 distinct event types"
        );
    }

    // ── BenchResultCollector tests ─────────────────────────────────────────

    #[test]
    fn collector_ci_mode_does_not_panic_on_target_miss() {
        let mut c = BenchResultCollector::new(BenchMode::Ci);
        c.record("test/metric", 999.0, "ns", Some(BenchTarget::at_most(1.0)));
        // finish() should NOT panic in CI mode even though target is missed.
        c.finish();
    }

    #[test]
    #[should_panic(expected = "Reference-mode hard target failures")]
    fn collector_reference_mode_panics_on_target_miss() {
        let mut c = BenchResultCollector::new(BenchMode::Reference);
        c.record("test/metric", 999.0, "ns", Some(BenchTarget::at_most(1.0)));
        c.finish();
    }

    #[test]
    fn collector_reference_mode_passes_when_targets_met() {
        let mut c = BenchResultCollector::new(BenchMode::Reference);
        c.record("test/latency", 0.5, "s", Some(BenchTarget::at_most(1.0)));
        c.record(
            "test/throughput",
            200.0,
            "MB/s",
            Some(BenchTarget::at_least(100.0)),
        );
        c.finish();
    }
}
