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
//! - A synthetic event generator ([`generate_events`]) producing
//!   `Vec<Event>` sorted by `(entity_id, timestamp)` for ingest and
//!   acceptance benchmarks.
//! - Criterion configuration helpers ([`wave2_criterion`]) that apply
//!   the workspace-standard sample size and measurement time.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringViewArray, TimestampNanosecondArray};
use bqlite_core::event::{EntityId, Event};
use bqlite_core::property::PropertyValue;
use bqlite_core::schema::{ColumnDef, TableSchema};
use bqlite_core::time::Timestamp;
use bqlite_core::BqlType;
use bqlite_storage::manifest::TableEntry;
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

/// Open a database and register a table with the given schema.
///
/// Uses the same manifest-edit approach as the writer tests since the
/// DDL surface (`CREATE TABLE`) is not yet wired to `Database`.
pub fn open_db_with_table(path: &Path, table_name: &str, schema: TableSchema) -> Database {
    let db = Database::open_or_create(path).expect("open db");
    let manifest_path = path.join("manifest.json");
    drop(db);
    let bytes = std::fs::read(&manifest_path).unwrap();
    let mut manifest: bqlite_storage::manifest::Manifest = serde_json::from_slice(&bytes).unwrap();
    manifest.tables.insert(
        table_name.to_string(),
        TableEntry {
            schema,
            next_sequence_id: 0,
            next_batch_id: 0,
            next_segment_id: 0,
            bootstrap_events_table: false,
            windows: Vec::new(),
        },
    );
    std::fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();
    Database::open_or_create(path).expect("reopen db")
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
}
