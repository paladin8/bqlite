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

// ── Bench scale (TASK-546) ──────────────────────────────────────────────────

/// Headline-throughput dataset size selector for the `bench-perf` suite.
///
/// Orthogonal to [`BenchMode`]: `Mode` controls hard-target enforcement;
/// `BenchScale` controls fixture size. The same bench file runs at any
/// scale.
///
/// See `docs/design/perf-suite.md` §3.1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BenchScale {
    /// 1M rows. Sub-second iteration; validates wiring.
    Small,
    /// 100M rows. Wave 5 reference-mode acceptance gate.
    Medium,
    /// 1B rows. Stress.
    Large,
    /// 10B rows. Headline production run.
    XLarge,
}

impl BenchScale {
    /// Read the scale from `BQLITE_BENCH_SCALE`. Defaults to `Small`.
    pub fn from_env() -> Self {
        match std::env::var("BQLITE_BENCH_SCALE").as_deref() {
            Ok("small") | Err(_) => BenchScale::Small,
            Ok("medium") => BenchScale::Medium,
            Ok("large") => BenchScale::Large,
            Ok("xlarge") => BenchScale::XLarge,
            Ok(other) => {
                eprintln!(
                    "WARNING: unknown BQLITE_BENCH_SCALE={other:?}, \
                     expected \"small\" / \"medium\" / \"large\" / \"xlarge\". \
                     Falling back to small."
                );
                BenchScale::Small
            }
        }
    }

    /// Total number of events the scale's fixture contains.
    pub fn rows(self) -> u64 {
        match self {
            BenchScale::Small => 1_000_000,
            BenchScale::Medium => 100_000_000,
            BenchScale::Large => 1_000_000_000,
            BenchScale::XLarge => 10_000_000_000,
        }
    }

    /// Distinct entity-id count: scales sub-linearly with rows so that
    /// average events-per-entity grows with scale (matches real-world
    /// workloads where bigger datasets cover longer histories per
    /// entity rather than only adding new entities).
    pub fn entity_count(self) -> u64 {
        match self {
            BenchScale::Small => 10_000,
            BenchScale::Medium => 100_000,
            BenchScale::Large => 1_000_000,
            BenchScale::XLarge => 10_000_000,
        }
    }

    /// Short, lower-case label used in cache paths and report tables.
    pub fn label(self) -> &'static str {
        match self {
            BenchScale::Small => "small",
            BenchScale::Medium => "medium",
            BenchScale::Large => "large",
            BenchScale::XLarge => "xlarge",
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

/// Criterion configuration scaled to fixture size for the `bench-perf`
/// suite. Larger fixtures get fewer samples but much longer measurement
/// time, on the assumption that variance per sample is small at scale
/// but per-iteration cost is large.
///
/// See `docs/design/perf-suite.md` §3.6.
pub fn criterion_for_scale(scale: BenchScale) -> Criterion {
    use std::time::Duration;
    match scale {
        BenchScale::Small => Criterion::default()
            .sample_size(50)
            .warm_up_time(Duration::from_secs(1))
            .measurement_time(Duration::from_secs(3)),
        BenchScale::Medium => Criterion::default()
            .sample_size(20)
            .warm_up_time(Duration::from_secs(3))
            .measurement_time(Duration::from_secs(10)),
        BenchScale::Large => Criterion::default()
            .sample_size(10)
            .warm_up_time(Duration::from_secs(5))
            .measurement_time(Duration::from_secs(30)),
        BenchScale::XLarge => Criterion::default()
            .sample_size(10) // Criterion-enforced minimum
            .warm_up_time(Duration::from_secs(10))
            .measurement_time(Duration::from_secs(60)),
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
    /// enforcement is deferred to `finish`).
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

// ── Streaming event generator (TASK-546) ────────────────────────────────────

/// Default seed for [`StreamingEventGenerator`]. Recorded in the
/// `bench-perf` report's methodology section so runs are reproducible.
pub const STREAMING_DEFAULT_SEED: u64 = 0xBEACA15E;
/// Default chunk size in events. At ~150 B/event this is ~150 MiB per
/// chunk, comfortably below per-process memory budgets at every scale.
pub const STREAMING_DEFAULT_CHUNK: usize = 1_000_000;
/// Default Zipf-like skew exponent. Higher = more concentrated head.
///
/// With `alpha = 1.0` (true Zipf) and 10K entities the top 1% own
/// roughly half of all events; with 1M entities the top 1% own about
/// 30% — a realistic behavioural-workload shape. Higher values
/// (e.g. 1.5) over-concentrate the head to the point a single entity
/// owns tens of percent of all events, which inflates fixture build
/// memory and dominates bench results with one hot entity.
pub const STREAMING_DEFAULT_SKEW: f64 = 1.0;

/// Configuration for [`StreamingEventGenerator`]. Defaults derive from
/// the chosen [`BenchScale`].
#[derive(Debug, Clone)]
pub struct StreamingConfig {
    pub total_events: u64,
    pub entity_count: u64,
    pub event_type_count: usize,
    pub entity_skew: f64,
    pub seed: u64,
    pub chunk_size: usize,
}

impl StreamingConfig {
    pub fn for_scale(scale: BenchScale) -> Self {
        Self {
            total_events: scale.rows(),
            entity_count: scale.entity_count(),
            event_type_count: REF_EVENT_TYPE_COUNT,
            entity_skew: STREAMING_DEFAULT_SKEW,
            seed: STREAMING_DEFAULT_SEED,
            chunk_size: STREAMING_DEFAULT_CHUNK,
        }
    }
}

/// Streaming, deterministic event generator with power-law entity
/// skew. Yields events in fixed-size chunks via a callback so total
/// memory stays bounded regardless of fixture size.
///
/// Output ordering: entities are emitted in monotonically-increasing
/// entity-id-string order; within each entity timestamps are monotonic.
/// This matches the partitioner's expectation.
///
/// See `docs/design/perf-suite.md` §3.2.
pub struct StreamingEventGenerator {
    cfg: StreamingConfig,
    per_entity_counts: Vec<u64>,
}

impl StreamingEventGenerator {
    pub fn for_scale(scale: BenchScale) -> Self {
        Self::with_config(StreamingConfig::for_scale(scale))
    }

    pub fn with_config(cfg: StreamingConfig) -> Self {
        let per_entity_counts =
            zipf_event_allocation(cfg.total_events, cfg.entity_count, cfg.entity_skew);
        Self {
            cfg,
            per_entity_counts,
        }
    }

    pub fn config(&self) -> &StreamingConfig {
        &self.cfg
    }

    /// Exact total event count this generator will yield. Equal to
    /// `cfg.total_events` modulo rounding from the allocation step.
    pub fn total_events(&self) -> u64 {
        self.per_entity_counts.iter().sum()
    }

    /// Walk the entire fixture, invoking `f` on each chunk of at most
    /// `cfg.chunk_size` events. The slice passed to `f` is reused
    /// across calls — copy out what you need before returning.
    pub fn for_each_chunk<F>(&self, mut f: F)
    where
        F: FnMut(&[Event]),
    {
        let labels: Vec<String> = (0..self.cfg.event_type_count.max(1))
            .map(|i| format!("event_{i}"))
            .collect();
        let mut buf: Vec<Event> = Vec::with_capacity(self.cfg.chunk_size);
        let base_ns: i64 = 1_735_689_600_000_000_000; // 2025-01-01T00:00:00Z
        let step_ns: i64 = 60_000_000_000; // 1 minute per event

        for (entity_idx, &count) in self.per_entity_counts.iter().enumerate() {
            if count == 0 {
                continue;
            }
            let entity = EntityId::String(format!("user_{entity_idx:08}"));
            let entity_offset_ns =
                splitmix64(self.cfg.seed.wrapping_add(entity_idx as u64)) as i64 % step_ns;

            for ev_idx in 0..count {
                let ts =
                    Timestamp::from_nanos(base_ns + entity_offset_ns + (ev_idx as i64) * step_ns);
                let event_type =
                    labels[((entity_idx as u64 + ev_idx) as usize) % labels.len()].clone();
                let amount = ((ev_idx as i64) % 5000) + 1;
                let price = 9.99 + ((ev_idx % 1024) as f64) * 0.01;
                let category = format!("cat_{}", ev_idx % 16);
                let quantity = (ev_idx as i64 % 10) + 1;
                let discount = if ev_idx % 3 == 0 { 0.1 } else { 0.0 };
                let region = format!("region_{}", entity_idx % 8);
                let flag = ev_idx % 2 == 0;

                buf.push(Event::with_properties(
                    entity.clone(),
                    ts,
                    event_type,
                    vec![
                        ("amount".into(), PropertyValue::Int(amount)),
                        ("price".into(), PropertyValue::Float(price)),
                        ("category".into(), PropertyValue::String(category)),
                        ("quantity".into(), PropertyValue::Int(quantity)),
                        ("discount".into(), PropertyValue::Float(discount)),
                        ("region".into(), PropertyValue::String(region)),
                        ("flag".into(), PropertyValue::Bool(flag)),
                    ],
                ));

                if buf.len() >= self.cfg.chunk_size {
                    f(&buf);
                    buf.clear();
                }
            }
        }

        if !buf.is_empty() {
            f(&buf);
        }
    }
}

/// Zipf-like event-count allocation across entities.
///
/// Entity at index `i` (0-based) receives `floor(N * w_i / W)` events
/// where `w_i = 1 / (i + 1)^alpha`. Rounding shortfall is added to
/// entity 0 so the total matches `total` exactly. Higher `alpha`
/// concentrates more events on the lowest-index entities; with
/// `alpha = 1.0` (true Zipf) and 10K entities the top 1% own roughly
/// half of all events. `alpha = 1.5` is *more* concentrated still and
/// pushes a single entity past 30% of events at moderate entity counts.
/// `alpha = 0` degenerates to uniform.
fn zipf_event_allocation(total: u64, entity_count: u64, alpha: f64) -> Vec<u64> {
    if entity_count == 0 || total == 0 {
        return Vec::new();
    }
    let n = entity_count as usize;
    let mut weights: Vec<f64> = (0..n).map(|i| 1.0 / ((i + 1) as f64).powf(alpha)).collect();
    let total_weight: f64 = weights.iter().sum();
    for w in &mut weights {
        *w /= total_weight;
    }
    let mut counts: Vec<u64> = weights
        .iter()
        .map(|w| ((*w) * (total as f64)).floor() as u64)
        .collect();
    let assigned: u64 = counts.iter().sum();
    let mut leftover = total.saturating_sub(assigned);
    // Distribute the remainder to the heaviest entities so the head
    // stays heavy even after rounding.
    let mut i = 0usize;
    while leftover > 0 && i < counts.len() {
        counts[i] += 1;
        leftover -= 1;
        i += 1;
    }
    counts
}

#[inline]
fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D049BB133111EB);
    x ^ (x >> 31)
}

// ── Persistent fixture cache (TASK-546) ─────────────────────────────────────

/// Manifest written next to each cached fixture database. Stored as
/// pretty JSON for easy human inspection.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FixtureManifest {
    pub scale: String,
    pub seed: u64,
    pub schema_version: u32,
    pub rows: u64,
    pub bytes_logical: u64,
    /// Wall-clock build time in seconds since the UNIX epoch.
    pub built_at_secs: u64,
}

/// A fully-built `Database` materialized on disk and reusable across
/// bench iterations and runs. Keyed by `(scale, seed, schema_version)`.
///
/// See `docs/design/perf-suite.md` §3.3.
pub struct PersistentFixture {
    pub root: PathBuf,
    pub db_path: PathBuf,
    pub manifest: FixtureManifest,
}

impl PersistentFixture {
    /// Bump whenever the streaming generator's event shape or the
    /// fixture's table schema changes — forces existing caches to
    /// rebuild.
    pub const SCHEMA_VERSION: u32 = 1;
    pub const DEFAULT_TABLE: &'static str = "purchases";
    /// Maximum age, in seconds, before a cached fixture is rebuilt
    /// (90 days). Generator changes are caught by `SCHEMA_VERSION`;
    /// this guard catches subtler bit-rot (filesystem corruption,
    /// retroactive Database format changes).
    pub const MAX_AGE_SECS: u64 = 90 * 24 * 60 * 60;
    /// Shard count for fixture databases.
    pub const SHARD_COUNT: u16 = 32;
    /// Partitioner buffer budget during fixture build. The spill
    /// directory beneath the fixture root absorbs the rest.
    pub const PARTITIONER_BUDGET_BYTES: usize = 1 << 30; // 1 GiB

    /// Open the cached fixture for `scale` with the default seed, or
    /// build it if absent / stale. Panics on build failure — bench
    /// harnesses cannot recover from a fixture that won't materialize.
    pub fn load_or_build(scale: BenchScale) -> Self {
        Self::load_or_build_with(scale, STREAMING_DEFAULT_SEED)
    }

    pub fn load_or_build_with(scale: BenchScale, seed: u64) -> Self {
        let cache_dir = resolve_fixture_cache_dir();
        let fixture_root = cache_dir.join(format!(
            "fixture-{}-seed{:#018x}-v{}",
            scale.label(),
            seed,
            Self::SCHEMA_VERSION,
        ));
        let manifest_path = fixture_root.join("manifest.json");
        let db_path = fixture_root.join("db");

        let force_rebuild = matches!(
            std::env::var("BQLITE_BENCH_REGEN").as_deref(),
            Ok("1") | Ok("true") | Ok("yes")
        );

        if !force_rebuild {
            if let Some(existing) =
                load_existing_fixture(&manifest_path, &db_path, scale, seed, Self::SCHEMA_VERSION)
            {
                eprintln!(
                    "  [fixture] reusing cached fixture at {} ({} rows, {} bytes_logical)",
                    fixture_root.display(),
                    existing.manifest.rows,
                    existing.manifest.bytes_logical,
                );
                return existing;
            }
        }

        if fixture_root.exists() {
            eprintln!(
                "  [fixture] removing stale fixture at {}",
                fixture_root.display()
            );
            let _ = std::fs::remove_dir_all(&fixture_root);
        }
        std::fs::create_dir_all(&fixture_root).expect("create fixture root");

        eprintln!(
            "  [fixture] building scale={} seed={:#018x} rows={} entities={} at {}",
            scale.label(),
            seed,
            scale.rows(),
            scale.entity_count(),
            fixture_root.display(),
        );

        let cfg = StreamingConfig {
            total_events: scale.rows(),
            entity_count: scale.entity_count(),
            event_type_count: REF_EVENT_TYPE_COUNT,
            entity_skew: STREAMING_DEFAULT_SKEW,
            seed,
            chunk_size: STREAMING_DEFAULT_CHUNK,
        };
        let generator = StreamingEventGenerator::with_config(cfg);

        let start = std::time::Instant::now();
        let (rows, bytes_logical) = build_fixture_database(&db_path, &generator);
        let elapsed = start.elapsed();
        eprintln!(
            "  [fixture] built {} rows ({:.2} GiB logical) in {:.1}s ({:.2}M rows/s)",
            rows,
            (bytes_logical as f64) / (1u64 << 30) as f64,
            elapsed.as_secs_f64(),
            (rows as f64) / elapsed.as_secs_f64() / 1_000_000.0,
        );

        let manifest = FixtureManifest {
            scale: scale.label().to_string(),
            seed,
            schema_version: Self::SCHEMA_VERSION,
            rows,
            bytes_logical,
            built_at_secs: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        };
        write_manifest_atomic(&manifest_path, &manifest);

        Self {
            root: fixture_root,
            db_path,
            manifest,
        }
    }

    /// Path to the materialized `Database` directory. Open with
    /// [`bqlite_storage::Database::open`].
    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    /// Convenience: open the database. Bench files should typically
    /// open once and reuse the handle across iterations.
    pub fn open_db(&self) -> bqlite_storage::Database {
        bqlite_storage::Database::open(self.db_path()).expect("open fixture database")
    }
}

fn resolve_fixture_cache_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("BQLITE_BENCH_CACHE_DIR") {
        return PathBuf::from(dir);
    }
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
    target_dir.join("bench-fixtures")
}

fn load_existing_fixture(
    manifest_path: &Path,
    db_path: &Path,
    scale: BenchScale,
    seed: u64,
    schema_version: u32,
) -> Option<PersistentFixture> {
    let raw = std::fs::read_to_string(manifest_path).ok()?;
    let manifest: FixtureManifest = serde_json::from_str(&raw).ok()?;
    if manifest.scale != scale.label()
        || manifest.seed != seed
        || manifest.schema_version != schema_version
    {
        return None;
    }
    if !db_path.is_dir() {
        return None;
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    if now.saturating_sub(manifest.built_at_secs) > PersistentFixture::MAX_AGE_SECS {
        return None;
    }
    let root = manifest_path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    Some(PersistentFixture {
        db_path: root.join("db"),
        root,
        manifest,
    })
}

fn write_manifest_atomic(path: &Path, manifest: &FixtureManifest) {
    let tmp = path.with_extension("json.tmp");
    let json = serde_json::to_string_pretty(manifest).expect("serialize manifest");
    let mut f = std::fs::File::create(&tmp).expect("create manifest tmp");
    f.write_all(json.as_bytes()).expect("write manifest tmp");
    f.sync_all().expect("sync manifest tmp");
    std::fs::rename(&tmp, path).expect("rename manifest");
}

fn build_fixture_database(db_path: &Path, generator: &StreamingEventGenerator) -> (u64, u64) {
    use bqlite_storage::ingest::partitioner::Partitioner;
    use bqlite_storage::writer::SegmentWriter;

    std::fs::create_dir_all(db_path).expect("create fixture db dir");
    let schema = purchases_schema();
    let mut db =
        bqlite_storage::Database::create_with_shards(db_path, PersistentFixture::SHARD_COUNT)
            .expect("create fixture db");
    db.create_table(PersistentFixture::DEFAULT_TABLE.to_string(), schema)
        .expect("create fixture table");

    let batch_id = db
        .allocate_batch_id(PersistentFixture::DEFAULT_TABLE)
        .expect("allocate batch id");
    let spill_dir = db_path.parent().unwrap_or(db_path).join("ingest-spill");
    std::fs::create_dir_all(&spill_dir).expect("create ingest spill dir");

    let mut partitioner = Partitioner::with_spill_dir(
        PersistentFixture::SHARD_COUNT,
        30,
        batch_id,
        PersistentFixture::PARTITIONER_BUDGET_BYTES,
        spill_dir.clone(),
    )
    .expect("partitioner init");

    let mut rows: u64 = 0;
    let mut bytes_logical: u64 = 0;
    let mut last_logged_rows: u64 = 0;
    let target = generator.cfg.total_events;
    let log_every = (target / 20).max(1_000_000);
    generator.for_each_chunk(|chunk| {
        bytes_logical += compute_event_bytes(chunk);
        for event in chunk {
            partitioner
                .push_event(event.clone())
                .expect("partitioner push_event");
            rows += 1;
        }
        if rows.saturating_sub(last_logged_rows) >= log_every {
            eprintln!(
                "  [fixture] ingest progress: {rows} / {target} rows ({:.1}%)",
                (rows as f64) / (target as f64) * 100.0
            );
            last_logged_rows = rows;
        }
    });

    let mut writer = SegmentWriter::new(&mut db);
    let _metas = writer
        .write_partitioner(PersistentFixture::DEFAULT_TABLE, partitioner)
        .expect("write_partitioner");

    // Best-effort cleanup of any leftover spill files. The Partitioner
    // is consumed; remaining files (if any) are spill scratch we
    // created.
    let _ = std::fs::remove_dir_all(&spill_dir);

    (rows, bytes_logical)
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

    // ── BenchScale tests (TASK-546) ────────────────────────────────────────

    #[test]
    fn bench_scale_rows_grow_monotonically() {
        assert!(BenchScale::Small.rows() < BenchScale::Medium.rows());
        assert!(BenchScale::Medium.rows() < BenchScale::Large.rows());
        assert!(BenchScale::Large.rows() < BenchScale::XLarge.rows());
    }

    #[test]
    fn bench_scale_labels_round_trip() {
        for scale in [
            BenchScale::Small,
            BenchScale::Medium,
            BenchScale::Large,
            BenchScale::XLarge,
        ] {
            assert!(!scale.label().is_empty());
            assert!(scale.rows() > 0);
            assert!(scale.entity_count() > 0);
        }
    }

    // ── Streaming generator tests (TASK-546) ──────────────────────────────

    #[test]
    fn zipf_allocation_sums_to_total() {
        let counts = zipf_event_allocation(1_000, 100, 1.5);
        let sum: u64 = counts.iter().sum();
        assert_eq!(sum, 1_000);
    }

    #[test]
    fn zipf_allocation_is_monotonically_non_increasing() {
        // Higher-rank entities should not receive more events than
        // lower-rank ones when alpha > 0.
        let counts = zipf_event_allocation(100_000, 1_000, 1.0);
        for w in counts.windows(2) {
            assert!(
                w[0] >= w[1],
                "non-monotone: counts[i]={}, counts[i+1]={}",
                w[0],
                w[1]
            );
        }
    }

    #[test]
    fn zipf_uniform_when_alpha_zero() {
        let counts = zipf_event_allocation(100, 10, 0.0);
        // With alpha=0 every entity gets weight 1, so counts differ by
        // at most 1 (rounding remainder distributed to the head).
        let max = *counts.iter().max().unwrap();
        let min = *counts.iter().min().unwrap();
        assert!(max - min <= 1, "uniform should differ by ≤ 1: {counts:?}");
    }

    #[test]
    fn streaming_generator_yields_total_events() {
        let cfg = StreamingConfig {
            total_events: 1_000,
            entity_count: 100,
            event_type_count: REF_EVENT_TYPE_COUNT,
            entity_skew: 1.0,
            seed: 42,
            chunk_size: 128,
        };
        let gen = StreamingEventGenerator::with_config(cfg);
        let mut total = 0u64;
        gen.for_each_chunk(|chunk| total += chunk.len() as u64);
        assert_eq!(total, gen.total_events());
        assert_eq!(total, 1_000);
    }

    #[test]
    fn streaming_generator_yields_sorted_chunks() {
        let cfg = StreamingConfig {
            total_events: 5_000,
            entity_count: 200,
            event_type_count: REF_EVENT_TYPE_COUNT,
            entity_skew: 1.5,
            seed: 7,
            chunk_size: 256,
        };
        let gen = StreamingEventGenerator::with_config(cfg);
        let mut last: Option<(EntityId, Timestamp)> = None;
        gen.for_each_chunk(|chunk| {
            for ev in chunk {
                let key = (ev.entity.clone(), ev.timestamp);
                if let Some(prev) = &last {
                    assert!(prev <= &key, "events not sorted: {:?} > {:?}", prev, key,);
                }
                last = Some(key);
            }
        });
    }

    #[test]
    #[ignore = "builds a 100K-event database; run with --ignored"]
    fn fixture_load_or_build_round_trip() {
        // Use a temp dir so we don't leave artefacts in target/.
        let tmp = ScratchDir::new("fixture-smoke");
        std::fs::create_dir_all(tmp.path()).expect("create tmp");
        std::env::set_var("BQLITE_BENCH_CACHE_DIR", tmp.path());

        // Override scale to a CI-friendly size by going through the
        // config directly. We don't expose a public Small-override on
        // PersistentFixture, so the test uses BenchScale::Small (1M
        // rows) — this is still a few-second build.
        let fixture = PersistentFixture::load_or_build(BenchScale::Small);
        assert_eq!(fixture.manifest.scale, "small");
        assert_eq!(fixture.manifest.seed, STREAMING_DEFAULT_SEED);
        assert_eq!(
            fixture.manifest.schema_version,
            PersistentFixture::SCHEMA_VERSION
        );
        assert_eq!(fixture.manifest.rows, BenchScale::Small.rows());
        assert!(fixture.manifest.bytes_logical > 0);
        assert!(fixture.db_path().is_dir());
        // segment files must have been written.
        let segs = find_segment_files(fixture.db_path());
        assert!(!segs.is_empty(), "fixture build wrote no segments");

        // Second load_or_build for the same scale must reuse the cache
        // without rebuilding.
        let again = PersistentFixture::load_or_build(BenchScale::Small);
        assert_eq!(again.manifest.built_at_secs, fixture.manifest.built_at_secs);

        std::env::remove_var("BQLITE_BENCH_CACHE_DIR");
    }

    #[test]
    fn streaming_generator_honors_event_type_count() {
        for k in [1usize, 3, 20, 50] {
            let cfg = StreamingConfig {
                total_events: 500,
                entity_count: 25,
                event_type_count: k,
                entity_skew: 1.0,
                seed: 0,
                chunk_size: 128,
            };
            let gen = StreamingEventGenerator::with_config(cfg);
            let mut seen = std::collections::BTreeSet::new();
            gen.for_each_chunk(|chunk| {
                for ev in chunk {
                    seen.insert(ev.event_type.clone());
                }
            });
            assert!(
                seen.len() <= k,
                "event_type_count={k} but observed {} distinct types: {seen:?}",
                seen.len()
            );
            // For sufficient rows we should hit every label.
            if 500 >= k * 4 {
                assert_eq!(
                    seen.len(),
                    k,
                    "expected all {k} labels to appear, got {}",
                    seen.len()
                );
            }
        }
    }

    #[test]
    fn streaming_generator_has_seven_properties_per_event() {
        let cfg = StreamingConfig {
            total_events: 100,
            entity_count: 5,
            event_type_count: REF_EVENT_TYPE_COUNT,
            entity_skew: 1.0,
            seed: 1,
            chunk_size: 64,
        };
        let gen = StreamingEventGenerator::with_config(cfg);
        gen.for_each_chunk(|chunk| {
            for ev in chunk {
                assert_eq!(ev.properties.len(), 7);
            }
        });
    }
}
