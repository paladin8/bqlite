//! Deterministic JSONL fixture generator for integration and acceptance tests.
//!
//! Mirrors the `csv` module (TASK-235) but emits newline-delimited JSON
//! instead of CSV, providing an equivalent dataset for exercising the Wave 4
//! JSONL ingest path (TASK-410).
//!
//! ## Determinism
//!
//! Every field is derived from the row index alone (no PRNG, no system clock).
//! Two calls with the same `row_count` produce byte-identical JSONL output.
//!
//! ## Schema: `purchases`
//!
//! Identical to `csv::PURCHASES_CREATE_TABLE`:
//!
//! | Column       | BQL Type    | Role       | Nullable |
//! |-------------|-------------|------------|----------|
//! | `user_id`   | STRING      | ENTITY KEY | NOT NULL |
//! | `ts`        | TIMESTAMP   | EVENT TIME | NOT NULL |
//! | `event_type`| STRING      | EVENT TYPE | NOT NULL |
//! | `amount`    | INT         |  Regular   | yes      |
//! | `category`  | STRING      |  Regular   | yes      |
//!
//! Entity ids cycle through `user_0` .. `user_{entity_count-1}`.
//! Timestamps are monotonically increasing starting at a fixed epoch.
//! `event_type` follows the same 2:1 purchase/refund pattern as the CSV
//! fixture.
//! `amount` is `row_index * 100 + 1` for purchase rows; absent (omitted
//! from the JSON object) for refund rows, which produces a `NULL` on
//! nullable columns.
//! `category` cycles through `["electronics", "books", "clothing", "food"]`.
//!
//! ## Flat and nested-properties variants
//!
//! The generator ships two output modes:
//!
//! - **Flat** (default, [`write_fixture`]): every field is a top-level key.
//! - **Nested** ([`write_fixture_nested`]): `amount` and `category` are moved
//!   inside a `properties` sub-object, exercising the JSONL reader's
//!   flattening logic.

use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// Re-export the DDL constant from the CSV module so callers can use
/// a single import path for the schema definition.
pub use crate::csv::PURCHASES_CREATE_TABLE;

/// Fixed base timestamp (2023-11-14T22:13:20Z in epoch nanos).
const BASE_TS_NANOS: i64 = 1_700_000_000_000_000_000;

/// Nanoseconds between consecutive rows (1 millisecond).
const TS_STEP_NANOS: i64 = 1_000_000;

/// Default entity count.
const DEFAULT_ENTITY_COUNT: u64 = 100;

/// Categories that cycle across rows.
const CATEGORIES: &[&str] = &["electronics", "books", "clothing", "food"];

/// Configuration for the JSONL fixture generator.
///
/// Identical shape to `csv::FixtureConfig` so the two can be swapped
/// transparently in acceptance tests.
pub struct FixtureConfig {
    /// Number of data rows to generate.
    pub row_count: u64,
    /// Number of distinct entity ids.
    pub entity_count: u64,
}

impl FixtureConfig {
    /// Construct with the given row count and the default entity count.
    pub fn new(row_count: u64) -> Self {
        Self {
            row_count,
            entity_count: DEFAULT_ENTITY_COUNT,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Flat variant
// ─────────────────────────────────────────────────────────────────────────────

/// Write the flat JSONL fixture to an arbitrary [`Write`] sink.
///
/// Every field is a top-level JSON key. Nullable fields (`amount`,
/// `category`) are omitted from the JSON object for refund rows so the
/// JSONL reader maps them to `NULL`.
pub fn write_fixture<W: Write>(config: &FixtureConfig, mut out: W) -> io::Result<()> {
    for i in 0..config.row_count {
        let user_id = i % config.entity_count;
        let ts = BASE_TS_NANOS + (i as i64) * TS_STEP_NANOS;
        let is_purchase = i % 3 != 2;
        let event_type = if is_purchase { "purchase" } else { "refund" };

        if is_purchase {
            let amount = (i as i64) * 100 + 1;
            let category = CATEGORIES[(i as usize) % CATEGORIES.len()];
            writeln!(
                out,
                r#"{{"user_id":"user_{user_id}","ts":{ts},"event_type":"{event_type}","amount":{amount},"category":"{category}"}}"#
            )?;
        } else {
            // Refund rows omit `amount` and `category` → NULL on ingest.
            writeln!(
                out,
                r#"{{"user_id":"user_{user_id}","ts":{ts},"event_type":"{event_type}"}}"#
            )?;
        }
    }
    out.flush()
}

/// Write the flat JSONL fixture to a file at `dir/<filename>` and
/// return the full path.
pub fn write_fixture_file(
    config: &FixtureConfig,
    dir: &Path,
    filename: &str,
) -> io::Result<PathBuf> {
    std::fs::create_dir_all(dir)?;
    let path = dir.join(filename);
    let file = std::fs::File::create(&path)?;
    let buf = io::BufWriter::new(file);
    write_fixture(config, buf)?;
    Ok(path)
}

// ─────────────────────────────────────────────────────────────────────────────
// Nested-properties variant
// ─────────────────────────────────────────────────────────────────────────────

/// Write the nested-properties JSONL fixture to an arbitrary [`Write`] sink.
///
/// The `amount` and `category` fields are nested inside a `properties`
/// sub-object for purchase rows, exercising the JSONL reader's
/// properties-flattening path:
///
/// ```text
/// {"user_id":"user_0","ts":...,"event_type":"purchase",
///  "properties":{"amount":1,"category":"electronics"}}
/// ```
pub fn write_fixture_nested<W: Write>(config: &FixtureConfig, mut out: W) -> io::Result<()> {
    for i in 0..config.row_count {
        let user_id = i % config.entity_count;
        let ts = BASE_TS_NANOS + (i as i64) * TS_STEP_NANOS;
        let is_purchase = i % 3 != 2;
        let event_type = if is_purchase { "purchase" } else { "refund" };

        if is_purchase {
            let amount = (i as i64) * 100 + 1;
            let category = CATEGORIES[(i as usize) % CATEGORIES.len()];
            writeln!(
                out,
                r#"{{"user_id":"user_{user_id}","ts":{ts},"event_type":"{event_type}","properties":{{"amount":{amount},"category":"{category}"}}}}"#
            )?;
        } else {
            writeln!(
                out,
                r#"{{"user_id":"user_{user_id}","ts":{ts},"event_type":"{event_type}"}}"#
            )?;
        }
    }
    out.flush()
}

/// Write the nested-properties JSONL fixture to a file and return the path.
pub fn write_fixture_nested_file(
    config: &FixtureConfig,
    dir: &Path,
    filename: &str,
) -> io::Result<PathBuf> {
    std::fs::create_dir_all(dir)?;
    let path = dir.join(filename);
    let file = std::fs::File::create(&path)?;
    let buf = io::BufWriter::new(file);
    write_fixture_nested(config, buf)?;
    Ok(path)
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared counting helpers (same semantics as csv module)
// ─────────────────────────────────────────────────────────────────────────────

/// Return the expected purchase-row count for a fixture with `row_count` rows.
pub fn expected_purchase_count(row_count: u64) -> u64 {
    row_count - row_count / 3
}

/// Return the expected refund-row count.
pub fn expected_refund_count(row_count: u64) -> u64 {
    row_count / 3
}

/// Return the timestamp (epoch nanos) for row index `i`.
pub fn row_timestamp(i: u64) -> i64 {
    BASE_TS_NANOS + (i as i64) * TS_STEP_NANOS
}

// ─────────────────────────────────────────────────────────────────────────────
// Unit tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flat_fixture_is_deterministic() {
        let config = FixtureConfig::new(50);
        let mut buf1 = Vec::new();
        write_fixture(&config, &mut buf1).unwrap();
        let mut buf2 = Vec::new();
        write_fixture(&config, &mut buf2).unwrap();
        assert_eq!(buf1, buf2);
    }

    #[test]
    fn flat_fixture_row_count_matches_config() {
        let config = FixtureConfig::new(30);
        let mut buf = Vec::new();
        write_fixture(&config, &mut buf).unwrap();
        let lines: Vec<&str> = std::str::from_utf8(&buf)
            .unwrap()
            .lines()
            .filter(|l| !l.trim().is_empty())
            .collect();
        assert_eq!(lines.len(), 30);
    }

    #[test]
    fn flat_fixture_each_line_is_valid_json_object() {
        let config = FixtureConfig::new(9);
        let mut buf = Vec::new();
        write_fixture(&config, &mut buf).unwrap();
        let output = std::str::from_utf8(&buf).unwrap();
        for line in output.lines() {
            let v: serde_json::Value = serde_json::from_str(line)
                .unwrap_or_else(|e| panic!("invalid JSON on line '{line}': {e}"));
            assert!(v.is_object(), "expected object, got {v:?}");
        }
    }

    #[test]
    fn flat_fixture_refund_rows_omit_amount_and_category() {
        // Row index 2 is a refund (i % 3 == 2); it should lack amount/category.
        let config = FixtureConfig::new(3);
        let mut buf = Vec::new();
        write_fixture(&config, &mut buf).unwrap();
        let output = std::str::from_utf8(&buf).unwrap();
        let lines: Vec<&str> = output.lines().collect();
        // Line index 2 (0-based) is row index 2 — a refund.
        let refund: serde_json::Value = serde_json::from_str(lines[2]).unwrap();
        assert_eq!(refund["event_type"], "refund");
        assert!(refund.get("amount").is_none(), "refund must omit amount");
        assert!(
            refund.get("category").is_none(),
            "refund must omit category"
        );
    }

    #[test]
    fn nested_fixture_is_deterministic() {
        let config = FixtureConfig::new(20);
        let mut buf1 = Vec::new();
        write_fixture_nested(&config, &mut buf1).unwrap();
        let mut buf2 = Vec::new();
        write_fixture_nested(&config, &mut buf2).unwrap();
        assert_eq!(buf1, buf2);
    }

    #[test]
    fn nested_fixture_purchase_rows_have_properties_sub_object() {
        let config = FixtureConfig::new(3);
        let mut buf = Vec::new();
        write_fixture_nested(&config, &mut buf).unwrap();
        let output = std::str::from_utf8(&buf).unwrap();
        let lines: Vec<&str> = output.lines().collect();
        // Row 0 is a purchase.
        let purchase: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(purchase["event_type"], "purchase");
        let props = purchase["properties"]
            .as_object()
            .expect("properties must be object");
        assert!(
            props.contains_key("amount"),
            "properties must contain amount"
        );
        assert!(
            props.contains_key("category"),
            "properties must contain category"
        );
        // amount and category must NOT be top-level.
        assert!(
            purchase.get("amount").is_none(),
            "amount must not be top-level"
        );
        assert!(
            purchase.get("category").is_none(),
            "category must not be top-level"
        );
    }

    #[test]
    fn purchase_refund_counts_match_csv_module() {
        assert_eq!(expected_purchase_count(9), 6);
        assert_eq!(expected_refund_count(9), 3);
    }

    #[test]
    fn row_timestamp_is_deterministic() {
        assert_eq!(row_timestamp(0), BASE_TS_NANOS);
        assert_eq!(row_timestamp(1), BASE_TS_NANOS + TS_STEP_NANOS);
    }
}
