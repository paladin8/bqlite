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
