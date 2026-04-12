//! Wave 2 end-to-end smoke test — rewritten by TASK-240.
//!
//! This is the acceptance gate. Its assertions:
//!
//! > Running `bqlite init <path>` followed by
//! > `bqlite query "CREATE TABLE events (...)" --db <path>` and then
//! > `bqlite query "events" --db <path>` exits successfully, prints the
//! > schema header, and reports an empty `(0 rows)` result set.
//!
//! The bootstrap events table is no longer auto-seeded (TASK-240);
//! callers must use `bqlite init` + `CREATE TABLE` explicitly.

use std::path::PathBuf;
use std::process::Command;

use bqlite_tests::common::TempDb;

/// Platform-specific name of the compiled bqlite CLI binary.
#[cfg(windows)]
const BQLITE_BIN_NAME: &str = "bqlite.exe";
#[cfg(not(windows))]
const BQLITE_BIN_NAME: &str = "bqlite";

/// Resolve the absolute path of the compiled `bqlite` CLI binary.
fn bqlite_bin() -> PathBuf {
    let target_dir = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            let workspace_root = manifest_dir
                .parent()
                .expect("tests/ must live directly under the workspace root")
                .to_path_buf();
            workspace_root.join("target")
        });

    for profile in ["debug", "release"] {
        let candidate = target_dir.join(profile).join(BQLITE_BIN_NAME);
        if candidate.exists() {
            return candidate;
        }
    }

    panic!(
        "bqlite binary not found under {} — run `cargo build -p bqlite-cli` first",
        target_dir.display()
    );
}

/// Helper: initialize a database and create the events table.
fn init_db_with_events(db: &TempDb) {
    // Initialize the database.
    let output = Command::new(bqlite_bin())
        .arg("init")
        .arg(db.path())
        .output()
        .expect("failed to spawn bqlite init");
    assert!(
        output.status.success(),
        "bqlite init failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Create the events table.
    let output = Command::new(bqlite_bin())
        .arg("query")
        .arg("CREATE TABLE events (entity_id STRING NOT NULL ENTITY KEY, ts TIMESTAMP NOT NULL EVENT TIME, event_type STRING NOT NULL EVENT TYPE)")
        .arg("--db")
        .arg(db.path())
        .output()
        .expect("failed to spawn bqlite query");
    assert!(
        output.status.success(),
        "CREATE TABLE failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// The acceptance gate
// ─────────────────────────────────────────────────────────────────────────────

/// **The acceptance gate.**
///
/// Initializes a database via `bqlite init`, creates the events table
/// via `CREATE TABLE`, then queries it and asserts the schema header
/// and empty result.
#[test]
fn acceptance_gate_query_events_on_initialized_db_is_empty() {
    let db = TempDb::new();
    init_db_with_events(&db);

    let output = Command::new(bqlite_bin())
        .arg("query")
        .arg("events")
        .arg("--db")
        .arg(db.path())
        .output()
        .expect("failed to spawn bqlite binary");

    assert!(
        output.status.success(),
        "bqlite exited with {status}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        status = output.status,
        stdout = String::from_utf8_lossy(&output.stdout),
        stderr = String::from_utf8_lossy(&output.stderr),
    );

    let stdout = String::from_utf8(output.stdout).expect("bqlite stdout must be valid UTF-8");

    for column in ["entity_id", "ts", "event_type", "__seq_id", "__batch_id"] {
        assert!(
            stdout.contains(column),
            "expected column `{column}` in CLI stdout, got:\n{stdout}"
        );
    }

    assert!(
        stdout.contains("(0 rows)"),
        "expected '(0 rows)' footer in CLI stdout, got:\n{stdout}"
    );
}

/// Same test with the `--db=<path>` syntax.
#[test]
fn acceptance_gate_also_accepts_db_equals_syntax() {
    let db = TempDb::new();
    init_db_with_events(&db);
    let db_flag = format!("--db={}", db.path().display());

    let output = Command::new(bqlite_bin())
        .arg("query")
        .arg("events")
        .arg(&db_flag)
        .output()
        .expect("failed to spawn bqlite binary");

    assert!(
        output.status.success(),
        "bqlite exited with {status}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        status = output.status,
        stdout = String::from_utf8_lossy(&output.stdout),
        stderr = String::from_utf8_lossy(&output.stderr),
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout utf8");
    assert!(
        stdout.contains("(0 rows)"),
        "expected '(0 rows)' footer, got:\n{stdout}"
    );
}

/// A query against an unknown table must exit non-zero.
#[test]
fn query_against_unknown_table_exits_non_zero_and_names_the_table() {
    let db = TempDb::new();
    init_db_with_events(&db);

    let output = Command::new(bqlite_bin())
        .arg("query")
        .arg("ghost")
        .arg("--db")
        .arg(db.path())
        .output()
        .expect("failed to spawn bqlite binary");

    assert!(
        !output.status.success(),
        "expected non-zero exit for unknown-table query, got 0.\nstdout:\n{stdout}\nstderr:\n{stderr}",
        stdout = String::from_utf8_lossy(&output.stdout),
        stderr = String::from_utf8_lossy(&output.stderr),
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("ghost"),
        "expected stderr to name the offending table `ghost`, got:\n{stderr}"
    );
}

/// Query against an uninitialized directory must suggest `bqlite init`.
#[test]
fn query_against_uninitialized_dir_suggests_init() {
    let db = TempDb::new();

    let output = Command::new(bqlite_bin())
        .arg("query")
        .arg("events")
        .arg("--db")
        .arg(db.path())
        .output()
        .expect("failed to spawn bqlite binary");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("bqlite init"),
        "expected stderr to suggest `bqlite init`, got:\n{stderr}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Real-row CLI tests (TASK-245)
// ─────────────────────────────────────────────────────────────────────────────

/// Helper: run `bqlite query <bql> --db <path>` and return the output.
fn run_query(db: &TempDb, bql: &str) -> std::process::Output {
    Command::new(bqlite_bin())
        .arg("query")
        .arg(bql)
        .arg("--db")
        .arg(db.path())
        .output()
        .expect("failed to spawn bqlite query")
}

/// Helper: run `bqlite query <bql> --db <path> [extra_args...]` and return output.
fn run_query_with_args(db: &TempDb, bql: &str, extra_args: &[&str]) -> std::process::Output {
    let mut cmd = Command::new(bqlite_bin());
    cmd.arg("query").arg(bql).arg("--db").arg(db.path());
    for arg in extra_args {
        cmd.arg(arg);
    }
    cmd.output().expect("failed to spawn bqlite query")
}

/// After INSERT VALUES, a bare table scan must return the inserted rows.
#[test]
fn query_returns_real_rows_after_insert_values() {
    let db = TempDb::new();
    init_db_with_events(&db);

    // Insert two rows.
    let insert = run_query(
        &db,
        "INSERT INTO events VALUES \
         ('alice', 1700000000000000000, 'login'), \
         ('bob',   1700000001000000000, 'logout')",
    );
    assert!(
        insert.status.success(),
        "INSERT must succeed: {}",
        String::from_utf8_lossy(&insert.stderr)
    );

    // Query: the result must return both rows from disk.
    // Note: Arrow's pretty-printer may emit a render error for the
    // Timestamp(UTC) column (requires chrono-tz feature), but the
    // row-count footer always lands so we can assert the count.
    let output = run_query(&db, "events");
    assert!(
        output.status.success(),
        "query must succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("utf8");
    assert!(
        stdout.contains("(2 rows)"),
        "expected '(2 rows)' footer confirming both rows were scanned, got:\n{stdout}"
    );
}

/// The auto-limit injects `| limit DEFAULT+1` so the CLI can detect
/// truncation. With enough rows and a small explicit `--limit`, the
/// truncation footer must appear.
#[test]
fn truncation_footer_appears_with_explicit_limit() {
    let db = TempDb::new();
    init_db_with_events(&db);

    // Insert 5 rows.
    let insert = run_query(
        &db,
        "INSERT INTO events VALUES \
         ('u1', 1700000000000000000, 'e'), \
         ('u2', 1700000001000000000, 'e'), \
         ('u3', 1700000002000000000, 'e'), \
         ('u4', 1700000003000000000, 'e'), \
         ('u5', 1700000004000000000, 'e')",
    );
    assert!(
        insert.status.success(),
        "INSERT must succeed: {}",
        String::from_utf8_lossy(&insert.stderr)
    );

    // Query with --limit 3: engine runs | limit 4, returns 4 rows,
    // renderer truncates to 3 and appends the footer.
    let output = run_query_with_args(&db, "events", &["--limit", "3"]);
    assert!(
        output.status.success(),
        "query must succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("utf8");
    assert!(
        stdout.contains("showing 3 rows"),
        "expected truncation footer 'showing 3 rows', got:\n{stdout}"
    );
    assert!(
        stdout.contains("--no-limit"),
        "truncation footer must mention --no-limit, got:\n{stdout}"
    );
}

/// With `--no-limit`, all rows are returned and no truncation footer
/// is appended even when the row count exceeds DEFAULT_LIMIT.
#[test]
fn no_limit_flag_returns_all_rows_without_footer() {
    let db = TempDb::new();
    init_db_with_events(&db);

    // Insert 3 rows.
    let insert = run_query(
        &db,
        "INSERT INTO events VALUES \
         ('x1', 1700000000000000000, 'click'), \
         ('x2', 1700000001000000000, 'click'), \
         ('x3', 1700000002000000000, 'click')",
    );
    assert!(
        insert.status.success(),
        "INSERT must succeed: {}",
        String::from_utf8_lossy(&insert.stderr)
    );

    // Query with --no-limit: all 3 rows, no truncation footer.
    let output = run_query_with_args(&db, "events", &["--no-limit"]);
    assert!(
        output.status.success(),
        "query must succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("utf8");
    assert!(
        stdout.contains("(3 rows)"),
        "expected '(3 rows)' footer, got:\n{stdout}"
    );
    assert!(
        !stdout.contains("showing"),
        "no truncation footer expected with --no-limit, got:\n{stdout}"
    );
}

/// Default auto-limit of 1000: inserting exactly 1000 rows must NOT
/// show the truncation footer (1000 ≤ 1000).
#[test]
fn default_auto_limit_1000_no_footer_when_exactly_at_cap() {
    let db = TempDb::new();
    init_db_with_events(&db);

    // Build a single INSERT with exactly 1000 rows.
    // Each row: ('eNNN', ts, 'e')
    let values: Vec<String> = (0u32..1000)
        .map(|i| {
            format!(
                "('e{i:04}', {}, 'e')",
                1_700_000_000_000_000_000_i64 + i as i64
            )
        })
        .collect();
    let bql = format!("INSERT INTO events VALUES {}", values.join(", "));
    let insert = run_query(&db, &bql);
    assert!(
        insert.status.success(),
        "INSERT 1000 rows must succeed: {}",
        String::from_utf8_lossy(&insert.stderr)
    );

    // Query without explicit limit: engine injects | limit 1001.
    // With exactly 1000 rows the engine returns 1000 (≤ 1001), so no
    // truncation occurs and the footer must not appear.
    let output = run_query(&db, "events");
    assert!(
        output.status.success(),
        "query must succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("utf8");
    // format_result_as_text uses format_count (comma-separated thousands).
    assert!(
        stdout.contains("(1,000 rows)"),
        "expected '(1,000 rows)' footer, got:\n{stdout}"
    );
    assert!(
        !stdout.contains("showing"),
        "no truncation footer expected when rows ≤ DEFAULT_LIMIT, got:\n{stdout}"
    );
}
