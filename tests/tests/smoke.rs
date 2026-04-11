//! Wave 1 end-to-end smoke test — TASK-123.
//!
//! This is the Wave 1 **acceptance gate**. Its single assertion:
//!
//! > Running `bqlite query "events" --db <fresh-dir>` against a
//! > freshly-created database directory exits successfully, prints the
//! > bootstrap `events` table's column header, and reports an empty
//! > `(0 rows)` result set.
//!
//! If this test passes, Wave 1's capability claim — "`bqlite query
//! \"events\"` parses, plans, executes against an auto-bootstrapped
//! `events` table in a freshly created database directory, and
//! returns an empty result set" — is satisfied end-to-end. Every
//! subsystem touched by that claim (parser, planner, engine bind,
//! operator trait, segment reader, storage bootstrap, manifest
//! catalog, tracing init, CLI arg parsing, text renderer) is
//! exercised by this one test as a black box.
//!
//! ## Why a subprocess, not an in-process engine call
//!
//! The Wave 1 acceptance gate is phrased in terms of the *CLI
//! invocation* (`bqlite query "events" --db <path>`), not the
//! `Engine::query` API. Running the actual compiled binary as a
//! subprocess is the only way to prove that:
//!
//! 1. `main.rs` wires the binary together correctly (tracing init,
//!    `Database::open_or_create`, `Engine::query`, `format_result_as_text`,
//!    and the dispatcher happy-path all run without a missing edge).
//! 2. The exit code is `0` on success (the CLI unit tests return a
//!    `Result` from `run()` — they don't observe the final
//!    `ExitCode`).
//! 3. Text rendering lands on stdout (not stderr) and carries both
//!    the schema header and the `(0 rows)` footer exactly as
//!    specified.
//!
//! Engine-level happy-path tests already exist in
//! `crates/bqlite-engine/src/query.rs`. This test is the layer above.
//!
//! ## Binary discovery
//!
//! Cargo's built-in `env!("CARGO_BIN_EXE_<name>")` macro is only
//! defined for integration tests that live **inside** the package
//! that declares the `[[bin]]`. This test lives in the workspace-
//! level `bqlite-tests` package — a separate crate — so the macro
//! is not available and we have to locate the binary ourselves.
//!
//! The strategy, in priority order:
//!
//! 1. Honour `$CARGO_TARGET_DIR` if set (CI sometimes pins it to a
//!    shared cache directory).
//! 2. Otherwise walk from `$CARGO_MANIFEST_DIR` up to the workspace
//!    root and look at `target/`.
//! 3. Prefer the `debug` profile (plain `cargo test`) over `release`
//!    (`cargo test --release`), but fall back to whichever exists.
//!
//! If no binary is found the test panics with a message instructing
//! the caller to `cargo build -p bqlite-cli` first. In practice the
//! local-ci harness runs `cargo test --all-targets`, which builds
//! every binary as a prerequisite, so this fallback message only
//! matters for developers running the test crate in isolation.

use std::path::PathBuf;
use std::process::Command;

use bqlite_tests::common::TempDb;

/// Platform-specific name of the compiled bqlite CLI binary.
#[cfg(windows)]
const BQLITE_BIN_NAME: &str = "bqlite.exe";
#[cfg(not(windows))]
const BQLITE_BIN_NAME: &str = "bqlite";

/// Resolve the absolute path of the compiled `bqlite` CLI binary.
///
/// See the module-level docs for the discovery rules. Panics with a
/// build hint if the binary is not found — the panic message is the
/// only diagnostic surface this helper has.
fn bqlite_bin() -> PathBuf {
    // Step 1: honour $CARGO_TARGET_DIR. This is the env var Cargo
    // itself reads, so any caller who has customised the target
    // directory (e.g. a CI cache) will have set it already.
    let target_dir = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            // Step 2: walk up from the test crate's manifest directory
            // to the workspace root. `CARGO_MANIFEST_DIR` is set by
            // Cargo when compiling the test crate and points at
            // `<workspace>/tests`, so the workspace root is its
            // parent.
            let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            let workspace_root = manifest_dir
                .parent()
                .expect("tests/ must live directly under the workspace root")
                .to_path_buf();
            workspace_root.join("target")
        });

    // Step 3: prefer debug over release so the common `cargo test`
    // invocation (no `--release`) finds its own binary. If only the
    // release binary exists (e.g. `cargo test --release`), pick that
    // instead.
    for profile in ["debug", "release"] {
        let candidate = target_dir.join(profile).join(BQLITE_BIN_NAME);
        if candidate.exists() {
            return candidate;
        }
    }

    panic!(
        "bqlite binary not found under {} — run `cargo build -p bqlite-cli` first (or \
         `cargo test --all-targets`, which builds every binary as a prerequisite)",
        target_dir.display()
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// The Wave 1 acceptance gate
// ─────────────────────────────────────────────────────────────────────────────

/// **The Wave 1 acceptance gate.**
///
/// Spawns the compiled `bqlite` binary with `query events --db <temp>`
/// against a freshly-created empty database directory. Asserts:
///
/// - The process exits with status 0.
/// - Stdout contains the bootstrap events table's column names
///   (`entity_id`, `ts`, `event_type`) plus the two system columns
///   (`__seq_id`, `__batch_id`) — the exact shape pinned by the
///   bootstrap rule in TASK-125 and `type-system.md` §5.1.
/// - Stdout contains the `(0 rows)` footer specified by the task
///   description.
///
/// Any failure here blocks Wave 1 from closing out.
#[test]
fn wave1_acceptance_gate_query_events_on_fresh_db_is_empty() {
    // `TempDb::new()` creates the directory for us, but
    // `Database::open_or_create` expects either a missing or
    // already-initialised path — a pre-existing empty directory is
    // fine per storage-format.md §5 + §14 (the open contract treats
    // an empty directory as "initialise in place").
    let db = TempDb::new();

    let output = Command::new(bqlite_bin())
        .arg("query")
        .arg("events")
        .arg("--db")
        .arg(db.path())
        .output()
        .expect("failed to spawn bqlite binary");

    // Assert exit success with a dump of stderr on failure so CI
    // reports include the actual error message instead of just
    // `assertion failed`.
    assert!(
        output.status.success(),
        "bqlite exited with {status}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        status = output.status,
        stdout = String::from_utf8_lossy(&output.stdout),
        stderr = String::from_utf8_lossy(&output.stderr),
    );

    let stdout = String::from_utf8(output.stdout).expect("bqlite stdout must be valid UTF-8");

    // Schema header row — one "name:type" token per column of the
    // bootstrap `events` table. We assert on individual substrings
    // rather than the full joined line because the renderer reserves
    // the right to change whitespace / delimiters within the line
    // without breaking the contract.
    for column in ["entity_id", "ts", "event_type", "__seq_id", "__batch_id"] {
        assert!(
            stdout.contains(column),
            "expected bootstrap column `{column}` in CLI stdout, got:\n{stdout}"
        );
    }

    // Empty-result footer — pinned verbatim by the TASK-123 task
    // description. A missing footer means the renderer regressed, a
    // wrong number means the Wave 1 operator pipeline started
    // producing phantom rows, either of which should block the wave.
    assert!(
        stdout.contains("(0 rows)"),
        "expected '(0 rows)' footer in CLI stdout, got:\n{stdout}"
    );
}

/// Defensive companion test: same shape but with the `--db=<path>`
/// syntax. The CLI supports both `--db <path>` and `--db=<path>`
/// (see TASK-119), so the acceptance gate holds for both.
///
/// This test exists because `--db=<path>` is a separate arg-parsing
/// code path in `parse_query_args` and I want the Wave 1 gate to
/// catch a regression in either branch.
#[test]
fn wave1_acceptance_gate_also_accepts_db_equals_syntax() {
    let db = TempDb::new();
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

/// Negative companion: a query against an unknown table must exit
/// non-zero and include the offending name in the error message. This
/// pins the plan-error path all the way through the CLI surface so a
/// regression in error propagation (engine → CLI → exit code) surfaces
/// at the acceptance gate, not silently downgraded to a zero exit.
#[test]
fn wave1_query_against_unknown_table_exits_non_zero_and_names_the_table() {
    let db = TempDb::new();

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
