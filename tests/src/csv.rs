//! Deterministic CSV fixture generator for integration and acceptance tests.
//!
//! The primary consumer is `tests/tests/wave2_acceptance.rs` (TASK-235),
//! which exercises the full Wave 2 pipeline at configurable scale. The
//! generator produces the **`purchases`** schema — a realistic
//! three-role-column table with two regular columns — and writes it to a
//! temporary CSV file that the acceptance test feeds into `INSERT FROM`.
//!
//! ## Determinism
//!
//! Every field is derived from the row index alone (no PRNG, no
//! system clock). Two calls with the same `row_count` produce
//! byte-identical CSV output, which makes assertion values trivially
//! reproducible without golden files.
//!
//! ## Schema: `purchases`
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
//! `event_type` cycles in a 2:1 purchase/refund pattern (rows where
//! `index % 3 == 2` are refunds).
//! `amount` is `row_index * 100 + 1` for purchase rows, empty for
//! refunds. `category` cycles through `["electronics", "books",
//! "clothing", "food"]`.

use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// The CREATE TABLE DDL for the `purchases` schema.
///
/// Matches the column definitions the CSV generator produces.
pub const PURCHASES_CREATE_TABLE: &str = "\
    CREATE TABLE purchases (\
        user_id STRING NOT NULL ENTITY KEY, \
        ts TIMESTAMP NOT NULL EVENT TIME, \
        event_type STRING NOT NULL EVENT TYPE, \
        amount INT, \
        category STRING\
    )";

/// Fixed base timestamp (2023-11-14T22:13:20Z in epoch nanos).
///
/// Chosen to be a round number in seconds (`1_700_000_000`) while
/// staying far from epoch zero and from overflow boundaries.
const BASE_TS_NANOS: i64 = 1_700_000_000_000_000_000;

/// Nanoseconds between consecutive rows (1 millisecond).
///
/// Small enough that 1M rows span only ~16 minutes of wall time,
/// keeping all rows in the same 30-day partitioner window.
const TS_STEP_NANOS: i64 = 1_000_000;

/// Default entity count for the generator.
const DEFAULT_ENTITY_COUNT: u64 = 100;

/// Categories that cycle across rows.
const CATEGORIES: &[&str] = &["electronics", "books", "clothing", "food"];

/// Configuration for the CSV fixture generator.
pub struct FixtureConfig {
    /// Number of data rows to generate (excludes the CSV header).
    pub row_count: u64,
    /// Number of distinct entity ids (`user_0` .. `user_{n-1}`).
    pub entity_count: u64,
}

impl FixtureConfig {
    /// Construct a config with the given row count and default entity
    /// count (100).
    pub fn new(row_count: u64) -> Self {
        Self {
            row_count,
            entity_count: DEFAULT_ENTITY_COUNT,
        }
    }
}

/// Write the CSV fixture to an arbitrary [`Write`] sink.
///
/// This is the low-level entry point — callers that want a temp file
/// should use [`write_fixture_file`] instead.
pub fn write_fixture<W: Write>(config: &FixtureConfig, mut out: W) -> io::Result<()> {
    // Header
    writeln!(out, "user_id,ts,event_type,amount,category")?;

    for i in 0..config.row_count {
        let user_id = i % config.entity_count;
        let ts = BASE_TS_NANOS + (i as i64) * TS_STEP_NANOS;
        let is_purchase = i % 3 != 2; // 2/3 purchases, 1/3 refunds
        let event_type = if is_purchase { "purchase" } else { "refund" };

        if is_purchase {
            let amount = (i as i64) * 100 + 1;
            let category = CATEGORIES[(i as usize) % CATEGORIES.len()];
            writeln!(out, "user_{user_id},{ts},{event_type},{amount},{category}")?;
        } else {
            // Refund rows have NULL amount and NULL category (empty CSV fields).
            writeln!(out, "user_{user_id},{ts},{event_type},,")?;
        }
    }

    out.flush()
}

/// Write the CSV fixture to a file at `dir/<filename>` and return the
/// full path.
///
/// Creates the parent directory if it does not exist.
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

/// Return the expected row count for a WHERE query that filters
/// `event_type = 'purchase'` on a fixture with `row_count` rows.
///
/// The generator makes 2 out of every 3 rows a purchase (rows where
/// `index % 3 != 2`), so the count is `row_count - row_count / 3`.
pub fn expected_purchase_count(row_count: u64) -> u64 {
    row_count - row_count / 3
}

/// Return the expected row count for refund rows (`event_type = 'refund'`).
pub fn expected_refund_count(row_count: u64) -> u64 {
    row_count / 3
}

/// Return the timestamp (epoch nanos) for the row at index `i`.
pub fn row_timestamp(i: u64) -> i64 {
    BASE_TS_NANOS + (i as i64) * TS_STEP_NANOS
}

// ─────────────────────────────────────────────────────────────────────────────
// Funnel fixture generator (Wave 3 acceptance test — TASK-326)
// ─────────────────────────────────────────────────────────────────────────────

/// The CREATE TABLE DDL for the funnel `events` schema.
///
/// Minimal 3-column table: entity key, event time, event type.
pub const FUNNEL_EVENTS_CREATE_TABLE: &str = "\
    CREATE TABLE events (\
        user_id STRING NOT NULL ENTITY KEY, \
        ts TIMESTAMP NOT NULL EVENT TIME, \
        event_type STRING NOT NULL EVENT TYPE\
    )";

/// Number of distinct entities in the funnel fixture.
const FUNNEL_ENTITY_COUNT: u64 = 20;

/// Number of entities that reach each funnel step.
///
/// - `FUNNEL_SIGNUP_COUNT` entities receive a `signup` event (step 1).
/// - `FUNNEL_ACTIVATION_COUNT` of those also receive an `activation` event (step 2).
/// - `FUNNEL_PURCHASE_COUNT` of those also receive a `purchase` event (step 3).
/// - The remaining entities receive only a `browse` event (no funnel entry).
const FUNNEL_SIGNUP_COUNT: u64 = 15;
const FUNNEL_ACTIVATION_COUNT: u64 = 10;
const FUNNEL_PURCHASE_COUNT: u64 = 5;

/// Nanoseconds between events for the same entity (1 hour).
///
/// Well within the 7d WITHIN window constraint used in the acceptance test.
const FUNNEL_EVENT_SPACING_NS: i64 = 3_600_000_000_000;

/// Write the funnel CSV fixture to an arbitrary [`Write`] sink using an
/// explicit base timestamp.
///
/// Produces a deterministic event stream where:
///
/// - Entities `user_0` .. `user_{FUNNEL_PURCHASE_COUNT - 1}` complete all 3
///   steps: signup → activation → purchase.
/// - Entities `user_{FUNNEL_PURCHASE_COUNT}` ..
///   `user_{FUNNEL_ACTIVATION_COUNT - 1}` complete 2 steps: signup → activation.
/// - Entities `user_{FUNNEL_ACTIVATION_COUNT}` ..
///   `user_{FUNNEL_SIGNUP_COUNT - 1}` complete 1 step: signup only.
/// - Entities `user_{FUNNEL_SIGNUP_COUNT}` ..
///   `user_{FUNNEL_ENTITY_COUNT - 1}` have only a `browse` event
///   (do not enter the funnel).
///
/// Events are written per-entity (all of entity 0's events, then entity 1's,
/// etc.) so the ingested data is entity-sorted — matching the storage layer's
/// requirement for entity-locality.
pub fn write_funnel_fixture_with_base_ts<W: Write>(
    mut out: W,
    base_ts_nanos: i64,
) -> io::Result<()> {
    writeln!(out, "user_id,ts,event_type")?;

    for user in 0..FUNNEL_ENTITY_COUNT {
        let mut ts = base_ts_nanos + (user as i64) * FUNNEL_EVENT_SPACING_NS * 4;

        if user < FUNNEL_SIGNUP_COUNT {
            writeln!(out, "user_{user},{ts},signup")?;
            ts += FUNNEL_EVENT_SPACING_NS;
        }

        if user < FUNNEL_ACTIVATION_COUNT {
            writeln!(out, "user_{user},{ts},activation")?;
            ts += FUNNEL_EVENT_SPACING_NS;
        }

        if user < FUNNEL_PURCHASE_COUNT {
            writeln!(out, "user_{user},{ts},purchase")?;
        }

        if user >= FUNNEL_SIGNUP_COUNT {
            // Non-funnel entities get a browse event.
            writeln!(out, "user_{user},{ts},browse")?;
        }
    }

    out.flush()
}

/// Write the canonical deterministic funnel CSV fixture to an arbitrary
/// [`Write`] sink.
pub fn write_funnel_fixture<W: Write>(out: W) -> io::Result<()> {
    write_funnel_fixture_with_base_ts(out, BASE_TS_NANOS)
}

/// Write the funnel CSV fixture to a file at `dir/<filename>` and return
/// the full path.
pub fn write_funnel_fixture_file(dir: &Path, filename: &str) -> io::Result<PathBuf> {
    std::fs::create_dir_all(dir)?;
    let path = dir.join(filename);
    let file = std::fs::File::create(&path)?;
    let buf = io::BufWriter::new(file);
    write_funnel_fixture(buf)?;
    Ok(path)
}

/// Write the funnel fixture to a file at `dir/<filename>` using an explicit
/// base timestamp and return the full path.
pub fn write_funnel_fixture_file_with_base_ts(
    dir: &Path,
    filename: &str,
    base_ts_nanos: i64,
) -> io::Result<PathBuf> {
    std::fs::create_dir_all(dir)?;
    let path = dir.join(filename);
    let file = std::fs::File::create(&path)?;
    let buf = io::BufWriter::new(file);
    write_funnel_fixture_with_base_ts(buf, base_ts_nanos)?;
    Ok(path)
}

/// Expected funnel step counts for the deterministic fixture.
///
/// Returns `(signup_count, activation_count, purchase_count)`.
pub fn expected_funnel_counts() -> (i64, i64, i64) {
    (
        FUNNEL_SIGNUP_COUNT as i64,
        FUNNEL_ACTIVATION_COUNT as i64,
        FUNNEL_PURCHASE_COUNT as i64,
    )
}

/// Return the entity id string for the row at index `i` given
/// `entity_count` entities.
pub fn row_entity_id(i: u64, entity_count: u64) -> String {
    format!("user_{}", i % entity_count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_output_is_identical_across_calls() {
        let config = FixtureConfig::new(100);
        let mut buf1 = Vec::new();
        write_fixture(&config, &mut buf1).unwrap();
        let mut buf2 = Vec::new();
        write_fixture(&config, &mut buf2).unwrap();
        assert_eq!(buf1, buf2);
    }

    #[test]
    fn header_has_five_columns() {
        let config = FixtureConfig::new(0);
        let mut buf = Vec::new();
        write_fixture(&config, &mut buf).unwrap();
        let output = String::from_utf8(buf).unwrap();
        let header = output.lines().next().unwrap();
        assert_eq!(header, "user_id,ts,event_type,amount,category");
    }

    #[test]
    fn row_count_matches_config() {
        let config = FixtureConfig::new(50);
        let mut buf = Vec::new();
        write_fixture(&config, &mut buf).unwrap();
        let output = String::from_utf8(buf).unwrap();
        // Subtract 1 for the header line, ignore trailing empty line.
        let data_lines = output.lines().count() - 1;
        assert_eq!(data_lines, 50);
    }

    #[test]
    fn purchase_refund_ratio() {
        // With 9 rows: indices 0..=8
        // refunds at i%3==2: indices 2, 5, 8 → 3 refunds, 6 purchases
        assert_eq!(expected_purchase_count(9), 6);
        assert_eq!(expected_refund_count(9), 3);
    }

    #[test]
    fn refund_rows_have_empty_amount_and_category() {
        let config = FixtureConfig::new(3);
        let mut buf = Vec::new();
        write_fixture(&config, &mut buf).unwrap();
        let output = String::from_utf8(buf).unwrap();
        let lines: Vec<&str> = output.lines().collect();
        // Row index 2 (line index 3) should be a refund with trailing ",,"
        assert!(
            lines[3].ends_with("refund,,"),
            "expected refund with empty fields, got: {}",
            lines[3]
        );
    }

    #[test]
    fn entity_ids_cycle() {
        let config = FixtureConfig {
            row_count: 5,
            entity_count: 3,
        };
        let mut buf = Vec::new();
        write_fixture(&config, &mut buf).unwrap();
        let output = String::from_utf8(buf).unwrap();
        let lines: Vec<&str> = output.lines().skip(1).collect();
        assert!(lines[0].starts_with("user_0,"));
        assert!(lines[1].starts_with("user_1,"));
        assert!(lines[2].starts_with("user_2,"));
        assert!(lines[3].starts_with("user_0,"));
        assert!(lines[4].starts_with("user_1,"));
    }

    #[test]
    fn funnel_fixture_deterministic() {
        let mut buf1 = Vec::new();
        write_funnel_fixture(&mut buf1).unwrap();
        let mut buf2 = Vec::new();
        write_funnel_fixture(&mut buf2).unwrap();
        assert_eq!(buf1, buf2, "funnel fixture must be deterministic");
    }

    #[test]
    fn funnel_fixture_has_correct_row_count() {
        let mut buf = Vec::new();
        write_funnel_fixture(&mut buf).unwrap();
        let output = String::from_utf8(buf).unwrap();
        // Header + data rows:
        // - 5 entities × 3 events (signup + activation + purchase) = 15
        // - 5 entities × 2 events (signup + activation) = 10
        // - 5 entities × 1 event (signup) = 5
        // - 5 entities × 1 event (browse) = 5
        // Total = 35 data rows + 1 header = 36 lines
        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(lines.len(), 36, "expected 36 lines (1 header + 35 data)");
        assert_eq!(lines[0], "user_id,ts,event_type");
    }

    #[test]
    fn funnel_fixture_entity_sorted() {
        let mut buf = Vec::new();
        write_funnel_fixture(&mut buf).unwrap();
        let output = String::from_utf8(buf).unwrap();
        let lines: Vec<&str> = output.lines().skip(1).collect();

        // Verify events are entity-sorted: all events for user_0 come
        // before user_1, etc.
        let mut prev_user = String::new();
        let mut seen_users: Vec<String> = Vec::new();
        for line in &lines {
            let user = line.split(',').next().unwrap().to_string();
            if user != prev_user {
                assert!(
                    !seen_users.contains(&user),
                    "entity {user} events must be contiguous (entity-sorted)"
                );
                seen_users.push(user.clone());
                prev_user = user;
            }
        }
    }

    #[test]
    fn funnel_fixture_allows_custom_base_timestamp() {
        let custom_base = 1_900_000_000_000_000_000;
        let mut buf = Vec::new();
        write_funnel_fixture_with_base_ts(&mut buf, custom_base).unwrap();
        let output = String::from_utf8(buf).unwrap();
        let first_row = output.lines().nth(1).unwrap();
        assert!(
            first_row.contains(&format!(",{custom_base},signup")),
            "first signup row must use the caller-provided base timestamp"
        );
    }

    #[test]
    fn funnel_expected_counts() {
        let (signup, activation, purchase) = expected_funnel_counts();
        assert_eq!(signup, 15);
        assert_eq!(activation, 10);
        assert_eq!(purchase, 5);
    }

    #[test]
    fn write_fixture_file_creates_file() {
        let dir = std::env::temp_dir().join("bqlite-csv-test");
        let _ = std::fs::remove_dir_all(&dir);
        let config = FixtureConfig::new(10);
        let path = write_fixture_file(&config, &dir, "test.csv").unwrap();
        assert!(path.exists());
        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(content.lines().count(), 11); // header + 10 rows
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn row_timestamp_is_deterministic() {
        assert_eq!(row_timestamp(0), BASE_TS_NANOS);
        assert_eq!(row_timestamp(1), BASE_TS_NANOS + TS_STEP_NANOS);
        assert_eq!(
            row_timestamp(1_000_000),
            BASE_TS_NANOS + 1_000_000 * TS_STEP_NANOS
        );
    }
}
