//! # bqlite CLI
//!
//! Command-line interface for the bqlite behavioral query engine. This
//! is the Wave 1 stub (TASK-119): it implements the single subcommand
//! required by the Wave 1 acceptance gate (TASK-123):
//!
//! ```text
//! bqlite query "<bql>" --db <path>
//! ```
//!
//! and prints every other subcommand as "not yet implemented". Later
//! waves extend the surface via their own tasks — see the command list
//! below for the intended shape.
//!
//! ## Subcommand list (future)
//!
//! - `bqlite init <path>` — initialize a new database
//! - `bqlite schema <path> <ddl>` — create a table with a schema
//! - `bqlite ingest <path> <table> --from <file>` — ingest data
//! - `bqlite query <bql> --db <path>` — run a BQL query **(Wave 1)**
//! - `bqlite inspect <path> [table]` — inspect database/table metadata
//! - `bqlite compact <path>` — compact storage segments
//! - `bqlite repl <path>` — interactive BQL shell
//!
//! ## Architecture
//!
//! `bqlite-cli` only depends on `bqlite-engine` (see
//! `docs/architecture.md` §"Dependency Direction"). Every crossing
//! into the query pipeline goes through the engine's re-exports:
//!
//! - [`bqlite_engine::Database::open_or_create`] — opens/initializes a
//!   database directory (storage-format.md §5 + §14).
//! - [`bqlite_engine::Engine::query`] — parse → plan → bind → drive.
//! - [`bqlite_engine::format_result_as_text`] — human-readable output.
//! - [`bqlite_engine::init_tracing`] — installs the global tracing
//!   subscriber from TASK-122.
//!
//! This indirection is deliberate: the CLI never imports `bqlite-parser`,
//! `bqlite-planner`, `bqlite-storage`, `bqlite-operators`, or `arrow`
//! directly. That matches the architecture rule and keeps the CLI
//! insulated from internal refactors.
//!
//! ## Argument parsing
//!
//! Wave 1 parses arguments by hand rather than pulling in `clap`. The
//! grammar is tiny (one subcommand, one positional, one flag) and the
//! workspace has no `clap` dependency yet; adding one for a throwaway
//! Wave 1 stub is not worth the churn. When the real CLI (Wave 2+)
//! needs `init` / `ingest` / `inspect` / etc. with their own flag
//! surfaces, migrating to `clap` is straightforward.
//!
//! ## Exit codes
//!
//! - **0** — success.
//! - **1** — runtime error (database open failure, query failure).
//! - **2** — usage error (missing subcommand, missing `--db`, unknown
//!   flag). This matches the convention Unix CLIs use for bad
//!   invocation vs. bad data.

use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;

use bqlite_engine::{format_result_as_text, init_tracing, Database, Engine};

/// Entry point. Delegates to [`run`] so error handling stays
/// centralised and the tests (see the module below) can exercise the
/// arg-parsing logic without spawning a subprocess.
fn main() -> ExitCode {
    // Install the global tracing subscriber before any other work so
    // every log line from the engine / planner / operators lands on
    // stderr with the format configured by `BQLITE_LOG`. `init_tracing`
    // is idempotent, so calling it from tests is also safe.
    init_tracing();

    let args: Vec<String> = std::env::args().skip(1).collect();

    match run(&args, &mut std::io::stdout(), &mut std::io::stderr()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(CliError::Usage(msg)) => {
            // Usage errors print to stderr followed by the top-level
            // help, so users get both the specific failure and a hint
            // at the correct invocation in one go.
            let _ = writeln!(std::io::stderr(), "error: {msg}");
            let _ = writeln!(std::io::stderr(), "{USAGE}");
            ExitCode::from(2)
        }
        Err(CliError::Runtime(msg)) => {
            let _ = writeln!(std::io::stderr(), "error: {msg}");
            ExitCode::from(1)
        }
    }
}

/// Top-level usage string. Printed on usage errors and `--help`.
const USAGE: &str = "\
Usage:
  bqlite query <bql> --db <path>

Commands:
  query    Run a BQL query against a database directory.

Options:
  -h, --help    Show this message.

Environment:
  BQLITE_LOG    tracing filter directive (default: warn).
";

/// Errors the CLI surfaces.
///
/// Split into `Usage` (user invoked the CLI incorrectly — bad
/// arguments, unknown subcommand) and `Runtime` (the invocation was
/// well-formed but the underlying operation failed — database open,
/// query execution, I/O). The two map to different exit codes so
/// scripts can distinguish "I called this wrong" from "this actually
/// failed".
#[derive(Debug)]
enum CliError {
    /// Bad invocation. Exits with status 2.
    Usage(String),
    /// The invocation was valid but the underlying operation failed.
    /// Exits with status 1.
    Runtime(String),
}

/// Top-level command dispatcher.
///
/// Separated from `main` so tests can drive it with a fabricated
/// argv / writer pair. The `out` and `err` sinks take `&mut dyn Write`
/// so the tests can capture output into an in-memory buffer.
fn run(args: &[String], out: &mut dyn Write, _err: &mut dyn Write) -> Result<(), CliError> {
    // `_err` is kept in the signature (rather than dropped) because
    // later-wave subcommands (`ingest`, `inspect`, ...) will want to
    // stream progress / warnings to stderr while the `query` path
    // writes tabular output to stdout. Leaving the parameter in now
    // means those additions don't need a signature change.
    let (subcommand, rest) = match args.split_first() {
        Some((head, tail)) => (head.as_str(), tail),
        // Usage hint for the no-subcommand case is printed by `main`
        // when it translates this error into an exit code — we don't
        // print it here to avoid double-emitting the usage text.
        None => return Err(CliError::Usage("no subcommand given".to_string())),
    };

    match subcommand {
        "-h" | "--help" | "help" => {
            let _ = writeln!(out, "{USAGE}");
            Ok(())
        }
        "query" => run_query(rest, out),
        other => Err(CliError::Usage(format!("unknown subcommand: {other}"))),
    }
}

/// Parsed shape of `bqlite query <bql> --db <path>`.
///
/// A dedicated struct (instead of a tuple) keeps the tests readable
/// and makes it obvious which field is which when we extend with
/// additional flags (`--no-limit`, `--limit N`, `--format json`, etc.)
/// in Wave 2+.
#[derive(Debug, PartialEq, Eq)]
struct QueryArgs {
    /// The BQL text. Wave 1's parser only accepts a bare identifier,
    /// but the CLI does not enforce that — the engine returns a typed
    /// parse error we surface verbatim.
    bql: String,
    /// Database directory path.
    db_path: PathBuf,
}

/// Parse the argv tail of `bqlite query ...`.
///
/// Accepts the BQL text and `--db <path>` in either order. Wave 1 has
/// exactly one positional and one flag, so there's no ambiguity — the
/// first non-flag token is the BQL text, the `--db` flag consumes the
/// next token as the path.
fn parse_query_args(rest: &[String]) -> Result<QueryArgs, CliError> {
    let mut bql: Option<String> = None;
    let mut db_path: Option<PathBuf> = None;

    let mut i = 0;
    while i < rest.len() {
        let arg = &rest[i];
        match arg.as_str() {
            "--db" => {
                // `--db` requires a following token. If it's missing,
                // error immediately with a specific message rather
                // than falling through to an obscure downstream
                // failure.
                let value = rest
                    .get(i + 1)
                    .ok_or_else(|| CliError::Usage("--db requires a path argument".to_string()))?;
                if db_path.is_some() {
                    return Err(CliError::Usage("--db specified more than once".to_string()));
                }
                db_path = Some(PathBuf::from(value));
                i += 2;
            }
            // `--db=<path>` — accept the GNU long-option convention
            // too. It's a small ergonomic win and costs almost nothing
            // to support.
            arg_str if arg_str.starts_with("--db=") => {
                let value = &arg_str["--db=".len()..];
                if db_path.is_some() {
                    return Err(CliError::Usage("--db specified more than once".to_string()));
                }
                db_path = Some(PathBuf::from(value));
                i += 1;
            }
            "-h" | "--help" => {
                return Err(CliError::Usage(
                    "help for 'query' not implemented yet — use `bqlite --help`".to_string(),
                ));
            }
            // Any other `--flag` is a usage error so typos don't
            // silently get interpreted as the BQL text.
            arg_str if arg_str.starts_with("--") => {
                return Err(CliError::Usage(format!("unknown flag: {arg_str}")));
            }
            _ => {
                // Positional argument = the BQL text. Only the first
                // positional is accepted; a second positional is a
                // usage error to avoid accidentally eating a trailing
                // shell-split fragment.
                if bql.is_some() {
                    return Err(CliError::Usage(
                        "query accepts exactly one positional BQL argument".to_string(),
                    ));
                }
                bql = Some(arg.clone());
                i += 1;
            }
        }
    }

    let bql = bql.ok_or_else(|| CliError::Usage("missing BQL text".to_string()))?;
    let db_path = db_path.ok_or_else(|| CliError::Usage("missing --db <path>".to_string()))?;

    Ok(QueryArgs { bql, db_path })
}

/// Implementation of `bqlite query <bql> --db <path>`.
///
/// Parses the arguments, opens the database, runs the query through
/// the engine, and writes the rendered result to `out`. Any failure
/// along the way turns into a `CliError::Runtime` with the underlying
/// error message preserved — we do not wrap with extra context because
/// `BqliteError` already includes enough information.
fn run_query(rest: &[String], out: &mut dyn Write) -> Result<(), CliError> {
    let parsed = parse_query_args(rest)?;

    let mut db = Database::open_or_create(&parsed.db_path)
        .map_err(|e| CliError::Runtime(format!("failed to open database: {e}")))?;

    let engine = Engine::new();
    let result = engine
        .query(&parsed.bql, &mut db)
        .map_err(|e| CliError::Runtime(format!("query failed: {e}")))?;

    let rendered = format_result_as_text(&result);
    // `format_result_as_text` always ends with a newline, so we use
    // `write!` (not `writeln!`) to avoid doubling it.
    out.write_all(rendered.as_bytes())
        .map_err(|e| CliError::Runtime(format!("failed to write output: {e}")))?;

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    // ── Argument parsing ────────────────────────────────────────────

    fn sv(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn parse_query_args_accepts_bql_then_db() {
        let parsed = parse_query_args(&sv(&["events", "--db", "/tmp/db1"])).expect("should parse");
        assert_eq!(parsed.bql, "events");
        assert_eq!(parsed.db_path, PathBuf::from("/tmp/db1"));
    }

    #[test]
    fn parse_query_args_accepts_db_then_bql() {
        // Flag-first ordering must also work — users move --db around.
        let parsed = parse_query_args(&sv(&["--db", "/tmp/db2", "events"])).expect("should parse");
        assert_eq!(parsed.bql, "events");
        assert_eq!(parsed.db_path, PathBuf::from("/tmp/db2"));
    }

    #[test]
    fn parse_query_args_accepts_db_equals_syntax() {
        let parsed = parse_query_args(&sv(&["events", "--db=/tmp/db3"])).expect("should parse");
        assert_eq!(parsed.db_path, PathBuf::from("/tmp/db3"));
    }

    #[test]
    fn parse_query_args_missing_db_is_usage_error() {
        match parse_query_args(&sv(&["events"])) {
            Err(CliError::Usage(msg)) => assert!(msg.contains("--db")),
            other => panic!("expected usage error for missing --db, got {other:?}"),
        }
    }

    #[test]
    fn parse_query_args_missing_bql_is_usage_error() {
        match parse_query_args(&sv(&["--db", "/tmp/dbx"])) {
            Err(CliError::Usage(msg)) => assert!(msg.contains("BQL")),
            other => panic!("expected usage error for missing BQL, got {other:?}"),
        }
    }

    #[test]
    fn parse_query_args_db_without_value_is_usage_error() {
        // Trailing `--db` with no following token is a classic
        // fat-finger case that should fail fast with a clear message.
        match parse_query_args(&sv(&["events", "--db"])) {
            Err(CliError::Usage(msg)) => {
                assert!(
                    msg.contains("--db"),
                    "message should mention --db, got: {msg}"
                );
            }
            other => panic!("expected usage error, got {other:?}"),
        }
    }

    #[test]
    fn parse_query_args_rejects_unknown_flag() {
        match parse_query_args(&sv(&["events", "--db", "/tmp/db", "--nope"])) {
            Err(CliError::Usage(msg)) => assert!(msg.contains("--nope")),
            other => panic!("expected usage error for unknown flag, got {other:?}"),
        }
    }

    #[test]
    fn parse_query_args_rejects_duplicate_positional() {
        // Shell splitting bugs can produce two positionals by accident;
        // error loudly instead of quietly using the first.
        match parse_query_args(&sv(&["events", "extra", "--db", "/tmp/db"])) {
            Err(CliError::Usage(msg)) => assert!(msg.contains("positional")),
            other => panic!("expected usage error for extra positional, got {other:?}"),
        }
    }

    #[test]
    fn parse_query_args_rejects_duplicate_db() {
        // `--db foo --db bar` is ambiguous; reject it instead of
        // picking one arbitrarily.
        match parse_query_args(&sv(&["events", "--db", "/tmp/a", "--db", "/tmp/b"])) {
            Err(CliError::Usage(msg)) => assert!(msg.contains("--db")),
            other => panic!("expected usage error for duplicate --db, got {other:?}"),
        }
    }

    // ── run() dispatcher ────────────────────────────────────────────

    #[test]
    fn run_with_no_args_is_usage_error() {
        let mut out = Vec::new();
        let mut err = Vec::new();
        let result = run(&[], &mut out, &mut err);
        match result {
            Err(CliError::Usage(msg)) => assert!(msg.contains("subcommand")),
            other => panic!("expected usage error, got {other:?}"),
        }
        // `run` does not write the usage hint itself — `main` prints
        // it when it translates the Usage error into an exit code.
        // Asserting `err.is_empty()` here pins that contract so that
        // if we ever move the printing back into `run`, the double
        // emission is caught by a test rather than by eyeball.
        assert!(
            err.is_empty(),
            "run() must not print the usage hint — main() does it"
        );
    }

    #[test]
    fn run_with_help_flag_prints_usage_on_stdout() {
        let mut out = Vec::new();
        let mut err = Vec::new();
        run(&sv(&["--help"]), &mut out, &mut err).expect("help should succeed");
        let out_text = String::from_utf8(out).unwrap();
        assert!(out_text.contains("bqlite query"));
    }

    #[test]
    fn run_with_unknown_subcommand_is_usage_error() {
        let mut out = Vec::new();
        let mut err = Vec::new();
        match run(&sv(&["nope"]), &mut out, &mut err) {
            Err(CliError::Usage(msg)) => assert!(msg.contains("nope")),
            other => panic!("expected usage error, got {other:?}"),
        }
    }

    // ── End-to-end query against a real temp database ──────────────

    static SEQ: AtomicU64 = AtomicU64::new(0);

    struct Scratch {
        path: PathBuf,
    }

    impl Scratch {
        fn new(label: &str) -> Self {
            let seq = SEQ.fetch_add(1, Ordering::Relaxed);
            let pid = std::process::id();
            let mut path = std::env::temp_dir();
            path.push(format!("bqlite-cli-{label}-{pid}-{seq}"));
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn query_against_fresh_database_prints_schema_header_and_zero_rows() {
        // This is the Wave 1 smoke-test shape (TASK-123) exercised
        // through the CLI's `run` entry point: a fresh database
        // directory plus the bare identifier `events` must print the
        // bootstrap schema header and the `(0 rows)` footer.
        let scratch = Scratch::new("smoke");
        let db_path_str = scratch.path().to_string_lossy().to_string();
        let args = sv(&["query", "events", "--db", &db_path_str]);

        let mut out = Vec::new();
        let mut err = Vec::new();
        run(&args, &mut out, &mut err).expect("query must succeed");

        let out_text = String::from_utf8(out).unwrap();
        assert!(
            out_text.contains("entity_id"),
            "expected bootstrap schema header, got:\n{out_text}"
        );
        assert!(
            out_text.contains("(0 rows)"),
            "expected '(0 rows)' footer, got:\n{out_text}"
        );
    }

    #[test]
    fn query_against_unknown_table_reports_plan_error() {
        // Runtime errors (as opposed to usage errors) map to
        // CliError::Runtime and the message should preserve the
        // engine's typed error text.
        let scratch = Scratch::new("unknown-table");
        let db_path_str = scratch.path().to_string_lossy().to_string();
        let args = sv(&["query", "ghost", "--db", &db_path_str]);

        let mut out = Vec::new();
        let mut err = Vec::new();
        match run(&args, &mut out, &mut err) {
            Err(CliError::Runtime(msg)) => {
                assert!(msg.contains("ghost"), "error should name the table: {msg}");
            }
            other => panic!("expected runtime error for unknown table, got {other:?}"),
        }
    }

    #[test]
    fn query_against_missing_db_flag_is_usage_error() {
        let mut out = Vec::new();
        let mut err = Vec::new();
        match run(&sv(&["query", "events"]), &mut out, &mut err) {
            Err(CliError::Usage(msg)) => assert!(msg.contains("--db")),
            other => panic!("expected usage error, got {other:?}"),
        }
    }

    #[test]
    fn query_with_empty_bql_surfaces_parse_error_from_engine() {
        // The parser rejects the empty string. The CLI must turn that
        // into a Runtime error (not a Usage error — the user invoked
        // the CLI correctly; the BQL itself is bad).
        let scratch = Scratch::new("empty-bql");
        let db_path_str = scratch.path().to_string_lossy().to_string();
        let args = sv(&["query", "", "--db", &db_path_str]);

        let mut out = Vec::new();
        let mut err = Vec::new();
        match run(&args, &mut out, &mut err) {
            Err(CliError::Runtime(msg)) => {
                assert!(
                    msg.contains("query failed"),
                    "error should be labelled as a query failure: {msg}"
                );
            }
            other => panic!("expected runtime error, got {other:?}"),
        }
    }
}
