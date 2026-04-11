//! Workspace-level integration-test helpers shared across every
//! `bqlite` test binary.
//!
//! This module is the Wave 1 foundation for fixture-driven tests.
//! The Wave 1 smoke test (TASK-123) is its first real consumer; every
//! Wave 2+ suite under `tests/suite/` is expected to include it via
//! the same `#[path]` idiom and extend it as new assertion shapes are
//! needed.
//!
//! ## Inclusion pattern
//!
//! Like [`tests/prop/mod.rs`](../prop/mod.rs), this file is **not** a
//! standalone Cargo target — it has no `[[test]]` entry and no
//! integration test file lives alongside it. Individual test files
//! include the module inline:
//!
//! ```ignore
//! // tests/<your_test>.rs
//! #[path = "common/mod.rs"]
//! mod common;
//!
//! use common::{TempDb, assert_batches_empty};
//!
//! #[test]
//! fn my_integration_test() {
//!     let db = TempDb::new();
//!     // ... run a query against db.path() ...
//!     // assert_batches_empty(&result);
//! }
//! ```
//!
//! The `#[path]` attribute is load-bearing: each Cargo `[[test]]`
//! target is compiled as its own crate root, so the helpers cannot be
//! reached through a normal module tree. This is the same trick
//! `tests/prop/mod.rs` uses. The companion test `tests/common_smoke.rs`
//! exercises every helper below and doubles as a copy-paste template
//! for the next test file that lands.
//!
//! When a new test file is added:
//!
//! 1. Create `tests/<name>.rs` and include `common/mod.rs` via `#[path]`.
//! 2. Register the target in `tests/Cargo.toml` with a `[[test]]` entry.
//! 3. Run `scripts/local-ci.sh` — `cargo test --all-targets` picks it up.
//!
//! ## Scope at Wave 1
//!
//! - [`TempDb`] — owns a fresh temporary directory for a single test
//!   and cleans it up on drop. Intended as the argument source for the
//!   eventual `Database::open_or_create(path)` entry point (TASK-116).
//! - [`load_fixture`] — a deliberately unimplemented hook for loading
//!   CSV/JSON/Parquet event fixtures. The signature is frozen now so
//!   that fixture-driven test files can be written against a stable
//!   surface; Wave 2 lands a real CSV loader (TASK-233) and Wave 4
//!   adds JSON + Parquet (TASK-410).
//! - [`assert_batches_empty`] / [`assert_batches_eq`] — value
//!   comparison over `RecordBatch` slices. These are the two assertion
//!   shapes Wave 1 actually needs: "empty result" (the smoke test) and
//!   "expected rows" (the forward-looking contract that suite fixture
//!   tests will rely on). Any assertion weaker than exact equality
//!   (row-reorder tolerance, numeric epsilons, null-equivalence rules)
//!   should land as its own named helper rather than widening
//!   `assert_batches_eq`.
//!
//! ## Dead code
//!
//! The module attribute below is required: Cargo compiles each
//! `[[test]]` target in its own crate and the helpers used by one
//! target look dead to the compiler of every other target. Do not
//! delete "unused" helpers without grepping every sibling file, and
//! remember that later waves will grow this module.

#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::record_batch::RecordBatch;
use thiserror::Error;

// ─────────────────────────────────────────────────────────────────────────────
// TempDb
// ─────────────────────────────────────────────────────────────────────────────

/// Monotonic counter appended to temp-dir names so concurrent tests
/// inside one process never collide. `std::process::id()` is folded
/// into the path so parallel `cargo test` runners (different PIDs)
/// cannot collide either.
///
/// Uniqueness across arbitrarily many processes is only probabilistic
/// — an extremely long-lived machine with PID reuse could in
/// principle collide with a still-living leaked directory. This has
/// never mattered in CI and the [`Drop`] cleanup makes a collision
/// self-healing, but the caveat is worth naming.
static TEMP_DB_SEQ: AtomicU64 = AtomicU64::new(0);

/// A temporary database directory that is removed when the value is
/// dropped.
///
/// Integration tests that need an on-disk database path call
/// [`TempDb::new`], hand [`TempDb::path`] to the bqlite API under
/// test, and let `Drop` clean the directory up at the end of scope.
///
/// # Cleanup semantics
///
/// `Drop` removes the directory recursively and **ignores errors**.
/// Tests should never fail in teardown — if the directory was already
/// removed by the test itself, that is fine; if the OS refuses the
/// removal for any reason, the platform temp sweeper reclaims it
/// later. The guarantee is "tests do not leak on the happy path", not
/// "tests produce zero residue under adversarial conditions".
///
/// # Thread safety
///
/// Multiple `TempDb` instances created concurrently in the same
/// process are guaranteed to have distinct paths. `TEMP_DB_SEQ` uses
/// `Relaxed` ordering because the only invariant we need is
/// uniqueness, not happens-before ordering with any other memory.
pub struct TempDb {
    path: PathBuf,
}

impl TempDb {
    /// Create a fresh empty directory under the system temp folder
    /// with a name of the form `bqlite-test-<pid>-<seq>/`.
    ///
    /// # Panics
    ///
    /// Panics if `std::fs::create_dir_all` fails. This is a test-only
    /// helper; a temp-dir creation failure indicates a real environment
    /// problem (disk full, permissions, read-only tmpfs) that the test
    /// author must see surface loudly.
    pub fn new() -> Self {
        let seq = TEMP_DB_SEQ.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let mut path = std::env::temp_dir();
        path.push(format!("bqlite-test-{pid}-{seq}"));
        std::fs::create_dir_all(&path)
            .unwrap_or_else(|e| panic!("failed to create temp dir {}: {e}", path.display()));
        Self { path }
    }

    /// Absolute path of the owned temporary directory.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Default for TempDb {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        // Ignore cleanup errors — see the type-level documentation on
        // `TempDb` for the rationale.
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Fixture loader (stub)
// ─────────────────────────────────────────────────────────────────────────────

/// Errors returned by [`load_fixture`].
///
/// Wave 1 only has the `NotYetSupported` variant. The
/// `non_exhaustive` attribute lets Wave 2 and later waves add
/// parse/schema/I-O variants without breaking exhaustive pattern
/// matches in test code.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum FixtureError {
    /// The Wave 1 stub does not implement any fixture format yet.
    /// Wave 2 (TASK-233) replaces the stub body with a real CSV
    /// loader; Wave 4 (TASK-410) adds JSON and Parquet.
    #[error("fixture loader stub: {format} support lands in Wave 2 (TASK-233)")]
    NotYetSupported { format: &'static str },
}

/// Convenience alias for fixture-loader results.
pub type FixtureResult<T> = std::result::Result<T, FixtureError>;

/// Load an integration-test fixture file as a sequence of Arrow
/// `RecordBatch` values.
///
/// # Wave 1 stub
///
/// The Wave 1 implementation always returns
/// `Err(FixtureError::NotYetSupported)` — ingest and schema-declared
/// tables do not land until Wave 2 (TASK-233). The signature is
/// frozen now so that the Wave 2 suite fixture tests can be authored
/// against a stable API while the real loader is in flight.
///
/// Wave 1 integration tests that only need an empty database should
/// use [`TempDb`] directly instead of going through this function.
pub fn load_fixture(_path: &Path) -> FixtureResult<Vec<RecordBatch>> {
    Err(FixtureError::NotYetSupported { format: "csv/json" })
}

// ─────────────────────────────────────────────────────────────────────────────
// RecordBatch assertions
// ─────────────────────────────────────────────────────────────────────────────

/// Assert that a slice of `RecordBatch`es contains zero rows.
///
/// Accepts both an empty slice and any number of zero-row batches —
/// the engine is free to emit either shape for an empty result.
///
/// This is the Wave 1 smoke-test assertion (TASK-123): running
/// `bqlite query "events"` against a freshly-bootstrapped empty
/// database must return an empty result set.
///
/// # Panics
///
/// Panics with a message naming the total row count and batch count
/// if any batch contains one or more rows.
pub fn assert_batches_empty(batches: &[RecordBatch]) {
    let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(
        total,
        0,
        "expected an empty result set, got {total} row(s) across {n} batch(es)",
        n = batches.len(),
    );
}

/// Assert that two `RecordBatch` slices are equal by value.
///
/// The comparison is strict: same number of batches, matching schemas
/// (field names, data types, nullability, metadata), and identical
/// column data via `RecordBatch`'s `PartialEq` impl.
///
/// # Panics
///
/// Panics with a descriptive message identifying the first mismatch.
/// The total row count is checked first so size mismatches surface as
/// "row count mismatch" rather than dumping arbitrary batch contents.
///
/// # Looser comparisons
///
/// Tests that need row-reorder tolerance, null-equivalence, or
/// floating-point epsilons should add a dedicated helper (for example
/// `assert_batches_eq_unordered`) rather than relaxing this one —
/// exact equality is the default, and weakening it silently would
/// make Wave 2+ regressions harder to catch.
pub fn assert_batches_eq(actual: &[RecordBatch], expected: &[RecordBatch]) {
    let actual_rows: usize = actual.iter().map(RecordBatch::num_rows).sum();
    let expected_rows: usize = expected.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(
        actual_rows, expected_rows,
        "row count mismatch: actual={actual_rows}, expected={expected_rows}",
    );
    assert_eq!(
        actual.len(),
        expected.len(),
        "batch count mismatch: actual={a}, expected={e}",
        a = actual.len(),
        e = expected.len(),
    );
    for (i, (a, e)) in actual.iter().zip(expected.iter()).enumerate() {
        assert_eq!(a.schema(), e.schema(), "schema mismatch at batch index {i}");
        assert_eq!(a, e, "value mismatch at batch index {i}");
    }
}
