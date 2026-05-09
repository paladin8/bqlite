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
//! - `bqlite ingest <file> --table <name> --db <path> [--format csv] [--map src=dst,...]` — ingest data
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
//! - [`bqlite_engine::Database::create`] — initializes a new database
//!   directory (`bqlite init`).
//! - [`bqlite_engine::Database::open`] — opens an existing database
//!   (`bqlite query`).
//! - [`bqlite_engine::Engine::query`] — parse → plan → bind → drive.
//! - [`bqlite_engine::format_result_as_text_limited`] — human-readable
//!   output with optional auto-limit truncation.
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

mod format;
mod ingest;

use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;

use bqlite_engine::{
    format_perf_explain, format_result_as_text_limited, init_tracing, Database, Engine,
    QueryOptions,
};

use format::{has_explicit_limit, maybe_inject_limit, DEFAULT_LIMIT};

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
  bqlite init   <path> [--shards N]
  bqlite query  <bql> --db <path> [--limit N | --no-limit] [--explain-perf]
  bqlite ingest <file> --table <name> --db <path> [--format csv] [--map src=dst,...]

Commands:
  init     Initialize a new database directory.
  query    Run a BQL query against a database directory.
  ingest   Load a data file into a table.

Options:
  -h, --help      Show this message.

Query options:
  --limit N       Cap output at N rows (default: 1000). Overrides auto-limit.
  --no-limit      Disable row cap; return all rows.
  --explain-perf  Run the query but print the per-query metrics
                  footer (`docs/design/execution-model.md` §14.1)
                  in place of the row table. Opts the query into
                  CPU-cost sampling.

Ingest options:
  --table <name>  Target table name (required).
  --format <fmt>  Source format: csv (default).
  --map <map>     Column rename map: src1=dst1,src2=dst2,...

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
        "init" => run_init(rest, out),
        "query" => run_query(rest, out, _err),
        "ingest" => ingest::run_ingest(rest, out, _err),
        other => Err(CliError::Usage(format!("unknown subcommand: {other}"))),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// init subcommand
// ─────────────────────────────────────────────────────────────────────────────

/// Parsed shape of `bqlite init <path> [--shards N]`.
#[derive(Debug, PartialEq, Eq)]
struct InitArgs {
    path: PathBuf,
    shards: Option<u16>,
}

/// Parse the argv tail of `bqlite init ...`.
fn parse_init_args(rest: &[String]) -> Result<InitArgs, CliError> {
    let mut path: Option<PathBuf> = None;
    let mut shards: Option<u16> = None;

    let mut i = 0;
    while i < rest.len() {
        let arg = &rest[i];
        match arg.as_str() {
            "--shards" => {
                let value = rest
                    .get(i + 1)
                    .ok_or_else(|| CliError::Usage("--shards requires a number".to_string()))?;
                let n: u16 = value.parse().map_err(|_| {
                    CliError::Usage(format!(
                        "--shards value must be a positive integer: {value}"
                    ))
                })?;
                if shards.is_some() {
                    return Err(CliError::Usage(
                        "--shards specified more than once".to_string(),
                    ));
                }
                shards = Some(n);
                i += 2;
            }
            arg_str if arg_str.starts_with("--shards=") => {
                let value = &arg_str["--shards=".len()..];
                let n: u16 = value.parse().map_err(|_| {
                    CliError::Usage(format!(
                        "--shards value must be a positive integer: {value}"
                    ))
                })?;
                if shards.is_some() {
                    return Err(CliError::Usage(
                        "--shards specified more than once".to_string(),
                    ));
                }
                shards = Some(n);
                i += 1;
            }
            "-h" | "--help" => {
                return Err(CliError::Usage(
                    "help for 'init' not implemented yet — use `bqlite --help`".to_string(),
                ));
            }
            arg_str if arg_str.starts_with("--") => {
                return Err(CliError::Usage(format!("unknown flag: {arg_str}")));
            }
            _ => {
                if path.is_some() {
                    return Err(CliError::Usage(
                        "init accepts exactly one positional path argument".to_string(),
                    ));
                }
                path = Some(PathBuf::from(arg));
                i += 1;
            }
        }
    }

    let path = path.ok_or_else(|| CliError::Usage("missing database path".to_string()))?;
    Ok(InitArgs { path, shards })
}

/// Implementation of `bqlite init <path> [--shards N]`.
fn run_init(rest: &[String], out: &mut dyn Write) -> Result<(), CliError> {
    let parsed = parse_init_args(rest)?;

    let db = if let Some(shards) = parsed.shards {
        Database::create_with_shards(&parsed.path, shards)
    } else {
        Database::create(&parsed.path)
    };

    db.map_err(|e| CliError::Runtime(format!("failed to initialize database: {e}")))?;

    let _ = writeln!(out, "initialized database at {}", parsed.path.display());
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// query subcommand
// ─────────────────────────────────────────────────────────────────────────────

/// Parsed shape of `bqlite query <bql> --db <path> [--limit N | --no-limit] [--explain-perf]`.
///
/// A dedicated struct (instead of a tuple) keeps the tests readable
/// and makes it obvious which field is which.
#[derive(Debug, PartialEq, Eq)]
struct QueryArgs {
    /// The BQL text.
    bql: String,
    /// Database directory path.
    db_path: PathBuf,
    /// Custom row cap from `--limit N`. `None` means use `DEFAULT_LIMIT`.
    limit: Option<usize>,
    /// `--no-limit` flag: disables the auto-limit row cap entirely.
    no_limit: bool,
    /// `--explain-perf` flag: discard row output and render the
    /// per-query metrics footer instead. Implies opt-in CPU-cost
    /// sampling via `QueryOptions::collect_cpu_metrics`.
    explain_perf: bool,
}

/// Parse the argv tail of `bqlite query ...`.
///
/// Accepts the BQL text, `--db <path>`, and the optional `--limit N` /
/// `--no-limit` flags in any order. The first non-flag token is the BQL
/// text; a second positional is a usage error.
fn parse_query_args(rest: &[String]) -> Result<QueryArgs, CliError> {
    let mut bql: Option<String> = None;
    let mut db_path: Option<PathBuf> = None;
    let mut limit: Option<usize> = None;
    let mut no_limit = false;
    let mut explain_perf = false;

    let mut i = 0;
    while i < rest.len() {
        let arg = &rest[i];
        match arg.as_str() {
            "--db" => {
                let value = rest
                    .get(i + 1)
                    .ok_or_else(|| CliError::Usage("--db requires a path argument".to_string()))?;
                if db_path.is_some() {
                    return Err(CliError::Usage("--db specified more than once".to_string()));
                }
                db_path = Some(PathBuf::from(value));
                i += 2;
            }
            arg_str if arg_str.starts_with("--db=") => {
                let value = &arg_str["--db=".len()..];
                if db_path.is_some() {
                    return Err(CliError::Usage("--db specified more than once".to_string()));
                }
                db_path = Some(PathBuf::from(value));
                i += 1;
            }
            "--limit" => {
                let value = rest.get(i + 1).ok_or_else(|| {
                    CliError::Usage("--limit requires a numeric argument".to_string())
                })?;
                if limit.is_some() {
                    return Err(CliError::Usage(
                        "--limit specified more than once".to_string(),
                    ));
                }
                let n: usize = value.parse().map_err(|_| {
                    CliError::Usage(format!(
                        "--limit value must be a non-negative integer, got {value:?}"
                    ))
                })?;
                limit = Some(n);
                i += 2;
            }
            arg_str if arg_str.starts_with("--limit=") => {
                let value = &arg_str["--limit=".len()..];
                if limit.is_some() {
                    return Err(CliError::Usage(
                        "--limit specified more than once".to_string(),
                    ));
                }
                let n: usize = value.parse().map_err(|_| {
                    CliError::Usage(format!(
                        "--limit value must be a non-negative integer, got {value:?}"
                    ))
                })?;
                limit = Some(n);
                i += 1;
            }
            "--no-limit" => {
                if no_limit {
                    return Err(CliError::Usage(
                        "--no-limit specified more than once".to_string(),
                    ));
                }
                no_limit = true;
                i += 1;
            }
            "--explain-perf" => {
                if explain_perf {
                    return Err(CliError::Usage(
                        "--explain-perf specified more than once".to_string(),
                    ));
                }
                explain_perf = true;
                i += 1;
            }
            "-h" | "--help" => {
                return Err(CliError::Usage(
                    "help for 'query' not implemented yet — use `bqlite --help`".to_string(),
                ));
            }
            arg_str if arg_str.starts_with("--") => {
                return Err(CliError::Usage(format!("unknown flag: {arg_str}")));
            }
            _ => {
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

    if limit.is_some() && no_limit {
        return Err(CliError::Usage(
            "--limit and --no-limit are mutually exclusive".to_string(),
        ));
    }
    if limit.is_some() && explain_perf {
        return Err(CliError::Usage(
            "--limit and --explain-perf are mutually exclusive: \
             --explain-perf needs the un-truncated pipeline so the \
             metrics reflect the full query"
                .to_string(),
        ));
    }

    let bql = bql.ok_or_else(|| CliError::Usage("missing BQL text".to_string()))?;
    let db_path = db_path.ok_or_else(|| CliError::Usage("missing --db <path>".to_string()))?;

    Ok(QueryArgs {
        bql,
        db_path,
        limit,
        no_limit,
        explain_perf,
    })
}

/// Implementation of `bqlite query <bql> --db <path> [--limit N | --no-limit]`.
///
/// Parses the arguments, opens the database, runs the query through
/// the engine, and writes the rendered result to `out`.
///
/// Auto-limit behaviour (TASK-234):
/// - When the BQL already has a `| limit` stage, or `--no-limit` was
///   given, no auto-injection happens and all rows are shown.
/// - Otherwise the CLI injects `| limit N+1` (where N is
///   `DEFAULT_LIMIT` or the `--limit N` value) before handing the
///   query to the engine. If the engine returns N+1 rows, the result
///   is truncated to N and a footer is appended.
fn run_query(rest: &[String], out: &mut dyn Write, _err: &mut dyn Write) -> Result<(), CliError> {
    let parsed = parse_query_args(rest)?;

    let mut db = Database::open(&parsed.db_path).map_err(|e| CliError::Runtime(format!("{e}")))?;

    // Determine whether to inject an auto-limit and what the display cap
    // should be passed to the renderer.
    //
    // `--explain-perf` discards row output but still drives the query
    // to completion, so auto-limit injection would silently hide
    // upstream work. Skip injection on the perf path.
    let (query_to_run, display_limit): (String, Option<usize>) =
        if parsed.explain_perf || parsed.no_limit || has_explicit_limit(&parsed.bql) {
            (parsed.bql.clone(), None)
        } else {
            let cap = parsed.limit.unwrap_or(DEFAULT_LIMIT);
            // Inject N+1 so the renderer can detect truncation.
            let q = maybe_inject_limit(&parsed.bql, cap);
            (q, Some(cap))
        };

    let engine = Engine::new();
    let options = QueryOptions {
        collect_cpu_metrics: parsed.explain_perf,
        ..Default::default()
    };
    let result = engine
        .query_with_options(&query_to_run, &mut db, &options)
        .map_err(|e| CliError::Runtime(format!("query failed: {e}")))?;

    if parsed.explain_perf {
        // Render the per-query metrics footer instead of the row
        // table. `format_perf_explain` returns a multi-section text
        // block already terminated with newlines.
        let rendered = format_perf_explain(&result.metrics);
        out.write_all(rendered.as_bytes())
            .map_err(|e| CliError::Runtime(format!("failed to write output: {e}")))?;
        return Ok(());
    }

    // `format_result_as_text_limited` always ends with a newline; use
    // `write!` (not `writeln!`) to avoid doubling it.
    let rendered = format_result_as_text_limited(&result, display_limit);
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

    /// Helper: init a database and create the events table via CLI.
    fn init_db_with_events(scratch: &Scratch) {
        let db_path_str = scratch.path().to_string_lossy().to_string();
        let mut out = Vec::new();
        let mut err = Vec::new();
        run(&sv(&["init", &db_path_str]), &mut out, &mut err).expect("init must succeed");
        run(
            &sv(&[
                "query",
                "CREATE TABLE events (entity_id STRING NOT NULL ENTITY KEY, ts TIMESTAMP NOT NULL EVENT TIME, event_type STRING NOT NULL EVENT TYPE)",
                "--db",
                &db_path_str,
            ]),
            &mut out,
            &mut err,
        )
        .expect("create table must succeed");
    }

    #[test]
    fn init_creates_database_directory() {
        let scratch = Scratch::new("init");
        let db_path_str = scratch.path().to_string_lossy().to_string();
        let mut out = Vec::new();
        let mut err = Vec::new();
        run(&sv(&["init", &db_path_str]), &mut out, &mut err).expect("init must succeed");

        let out_text = String::from_utf8(out).unwrap();
        assert!(
            out_text.contains("initialized"),
            "expected confirmation message, got:\n{out_text}"
        );
        assert!(
            scratch.path().join("manifest.json").is_file(),
            "manifest.json should exist after init"
        );
    }

    #[test]
    fn init_with_shards() {
        let scratch = Scratch::new("init-shards");
        let db_path_str = scratch.path().to_string_lossy().to_string();
        let mut out = Vec::new();
        let mut err = Vec::new();
        run(
            &sv(&["init", &db_path_str, "--shards", "8"]),
            &mut out,
            &mut err,
        )
        .expect("init must succeed");
        // Verify the shard count by opening the DB.
        let db = Database::open(scratch.path()).expect("open");
        assert_eq!(db.manifest().shard_count, 8);
    }

    #[test]
    fn init_twice_is_runtime_error() {
        let scratch = Scratch::new("init-twice");
        let db_path_str = scratch.path().to_string_lossy().to_string();
        let mut out = Vec::new();
        let mut err = Vec::new();
        run(&sv(&["init", &db_path_str]), &mut out, &mut err).expect("first init");
        match run(&sv(&["init", &db_path_str]), &mut out, &mut err) {
            Err(CliError::Runtime(msg)) => {
                assert!(
                    msg.contains("manifest already exists"),
                    "error should mention existing manifest: {msg}"
                );
            }
            other => panic!("expected runtime error for double init, got {other:?}"),
        }
    }

    #[test]
    fn query_against_initialized_database_prints_schema_header_and_zero_rows() {
        let scratch = Scratch::new("smoke");
        init_db_with_events(&scratch);

        let db_path_str = scratch.path().to_string_lossy().to_string();
        let args = sv(&["query", "events", "--db", &db_path_str]);

        let mut out = Vec::new();
        let mut err = Vec::new();
        run(&args, &mut out, &mut err).expect("query must succeed");

        let out_text = String::from_utf8(out).unwrap();
        assert!(
            out_text.contains("entity_id"),
            "expected schema header, got:\n{out_text}"
        );
        assert!(
            out_text.contains("(0 rows)"),
            "expected '(0 rows)' footer, got:\n{out_text}"
        );
    }

    #[test]
    fn query_against_uninitialized_directory_reports_error() {
        let scratch = Scratch::new("uninit");
        fs::create_dir_all(scratch.path()).unwrap();
        let db_path_str = scratch.path().to_string_lossy().to_string();
        let args = sv(&["query", "events", "--db", &db_path_str]);

        let mut out = Vec::new();
        let mut err = Vec::new();
        match run(&args, &mut out, &mut err) {
            Err(CliError::Runtime(msg)) => {
                assert!(
                    msg.contains("bqlite init"),
                    "error should suggest bqlite init: {msg}"
                );
            }
            other => panic!("expected runtime error, got {other:?}"),
        }
    }

    #[test]
    fn query_against_unknown_table_reports_plan_error() {
        let scratch = Scratch::new("unknown-table");
        init_db_with_events(&scratch);
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

    // ── --limit / --no-limit argument parsing ───────────────────────

    #[test]
    fn parse_query_args_accepts_limit_flag() {
        let parsed =
            parse_query_args(&sv(&["events", "--db", "/tmp/db", "--limit", "50"])).unwrap();
        assert_eq!(parsed.limit, Some(50));
        assert!(!parsed.no_limit);
    }

    #[test]
    fn parse_query_args_accepts_limit_equals_syntax() {
        let parsed = parse_query_args(&sv(&["events", "--db", "/tmp/db", "--limit=200"])).unwrap();
        assert_eq!(parsed.limit, Some(200));
    }

    #[test]
    fn parse_query_args_accepts_no_limit_flag() {
        let parsed = parse_query_args(&sv(&["events", "--db", "/tmp/db", "--no-limit"])).unwrap();
        assert!(parsed.no_limit);
        assert_eq!(parsed.limit, None);
    }

    #[test]
    fn parse_query_args_accepts_explain_perf_flag() {
        let parsed =
            parse_query_args(&sv(&["events", "--db", "/tmp/db", "--explain-perf"])).unwrap();
        assert!(parsed.explain_perf);
    }

    #[test]
    fn parse_query_args_explain_perf_default_is_off() {
        let parsed = parse_query_args(&sv(&["events", "--db", "/tmp/db"])).unwrap();
        assert!(!parsed.explain_perf);
    }

    #[test]
    fn parse_query_args_rejects_limit_with_explain_perf() {
        match parse_query_args(&sv(&[
            "events",
            "--db",
            "/db",
            "--limit",
            "10",
            "--explain-perf",
        ])) {
            Err(CliError::Usage(msg)) => assert!(msg.contains("--explain-perf")),
            other => panic!("expected usage error for --limit + --explain-perf, got {other:?}"),
        }
    }

    #[test]
    fn parse_query_args_rejects_duplicate_explain_perf() {
        match parse_query_args(&sv(&[
            "events",
            "--db",
            "/db",
            "--explain-perf",
            "--explain-perf",
        ])) {
            Err(CliError::Usage(msg)) => assert!(msg.contains("--explain-perf")),
            other => panic!("expected usage error for duplicate --explain-perf, got {other:?}"),
        }
    }

    #[test]
    fn parse_query_args_limit_and_no_limit_mutually_exclusive() {
        match parse_query_args(&sv(&[
            "events",
            "--db",
            "/db",
            "--limit",
            "10",
            "--no-limit",
        ])) {
            Err(CliError::Usage(msg)) => assert!(msg.contains("mutually exclusive")),
            other => panic!("expected usage error, got {other:?}"),
        }
    }

    #[test]
    fn parse_query_args_non_numeric_limit_is_usage_error() {
        match parse_query_args(&sv(&["events", "--db", "/db", "--limit", "foo"])) {
            Err(CliError::Usage(msg)) => assert!(msg.contains("--limit")),
            other => panic!("expected usage error, got {other:?}"),
        }
    }

    #[test]
    fn parse_query_args_limit_without_value_is_usage_error() {
        match parse_query_args(&sv(&["events", "--db", "/db", "--limit"])) {
            Err(CliError::Usage(msg)) => assert!(msg.contains("--limit")),
            other => panic!("expected usage error, got {other:?}"),
        }
    }

    #[test]
    fn parse_query_args_limit_zero_is_accepted() {
        let parsed = parse_query_args(&sv(&["events", "--db", "/tmp/db", "--limit", "0"])).unwrap();
        assert_eq!(parsed.limit, Some(0));
    }

    // ── Auto-limit end-to-end ────────────────────────────────────────

    #[test]
    fn query_limit_flag_accepted_and_query_completes() {
        // Verify that --limit N is accepted by the CLI, the auto-limit
        // machinery runs (injecting | limit N+1 into the BQL), and the
        // query completes without error against an empty table.
        // The truncation footer is tested by
        // `truncation_footer_shown_when_rows_exceed_display_cap` below.
        let scratch = Scratch::new("auto-limit");
        init_db_with_events(&scratch);
        let db_path_str = scratch.path().to_string_lossy().to_string();

        // Query with --limit 1 against an empty DB. Zero rows returned,
        // which is within the cap, so no truncation footer.
        let args = sv(&["query", "events", "--db", &db_path_str, "--limit", "1"]);
        let mut out = Vec::new();
        let mut err = Vec::new();
        run(&args, &mut out, &mut err).expect("query must succeed");
        let out_text = String::from_utf8(out).unwrap();
        assert!(
            out_text.contains("entity_id"),
            "schema header expected, got:\n{out_text}"
        );
        assert!(
            out_text.contains("(0 rows)"),
            "zero-rows footer expected (empty table), got:\n{out_text}"
        );
        assert!(
            !out_text.contains("showing"),
            "no truncation footer expected when rows ≤ limit, got:\n{out_text}"
        );
    }

    #[test]
    fn query_no_limit_flag_no_footer_when_rows_within_limit() {
        // With --limit 100 and only 0 rows, no footer should appear.
        let scratch = Scratch::new("auto-limit-none");
        init_db_with_events(&scratch);
        let db_path_str = scratch.path().to_string_lossy().to_string();
        let args = sv(&["query", "events", "--db", &db_path_str, "--limit", "100"]);
        let mut out = Vec::new();
        let mut err = Vec::new();
        run(&args, &mut out, &mut err).expect("query must succeed");
        let out_text = String::from_utf8(out).unwrap();
        assert!(
            !out_text.contains("showing"),
            "no truncation footer expected for empty result:\n{out_text}"
        );
    }

    #[test]
    fn query_with_no_limit_flag_does_not_add_footer() {
        let scratch = Scratch::new("no-limit-flag");
        init_db_with_events(&scratch);
        let db_path_str = scratch.path().to_string_lossy().to_string();
        let args = sv(&["query", "events", "--db", &db_path_str, "--no-limit"]);
        let mut out = Vec::new();
        let mut err = Vec::new();
        run(&args, &mut out, &mut err).expect("query must succeed");
        let out_text = String::from_utf8(out).unwrap();
        assert!(
            !out_text.contains("showing"),
            "no footer expected with --no-limit:\n{out_text}"
        );
    }

    #[test]
    fn query_with_explicit_limit_in_bql_suppresses_injection() {
        // If the BQL already has `| limit`, no auto-injection should happen
        // and no truncation footer should appear.
        let scratch = Scratch::new("explicit-limit");
        init_db_with_events(&scratch);
        let db_path_str = scratch.path().to_string_lossy().to_string();
        // Use LIMIT 5 in the BQL — even with 0 rows, no footer expected.
        let args = sv(&["query", "events | limit 5", "--db", &db_path_str]);
        let mut out = Vec::new();
        let mut err = Vec::new();
        run(&args, &mut out, &mut err).expect("query must succeed");
        let out_text = String::from_utf8(out).unwrap();
        assert!(
            !out_text.contains("showing"),
            "no auto-limit footer expected when query has explicit LIMIT:\n{out_text}"
        );
    }

    // ── Real-row integration (TASK-245) ─────────────────────────────

    /// Helper: insert rows into `events` and return the db path string.
    fn insert_events(scratch: &Scratch, values: &str) -> String {
        let db_path_str = scratch.path().to_string_lossy().to_string();
        let bql = format!("INSERT INTO events VALUES {values}");
        let args = sv(&["query", &bql, "--db", &db_path_str]);
        let mut out = Vec::new();
        let mut err = Vec::new();
        run(&args, &mut out, &mut err).expect("INSERT must succeed");
        db_path_str
    }

    #[test]
    fn query_returns_real_rows_after_insert_values() {
        // After INSERT VALUES the scan must return the real rows from disk.
        let scratch = Scratch::new("real-rows");
        init_db_with_events(&scratch);
        let db = insert_events(
            &scratch,
            "('alice', 1700000000000000000, 'login'), \
             ('bob',   1700000001000000000, 'logout')",
        );

        let args = sv(&["query", "events", "--db", &db]);
        let mut out = Vec::new();
        let mut err = Vec::new();
        run(&args, &mut out, &mut err).expect("query must succeed");
        let out_text = String::from_utf8(out).unwrap();
        // Two rows must have been scanned from disk.
        assert!(
            out_text.contains("(2 rows)"),
            "expected '(2 rows)' footer, got:\n{out_text}"
        );
    }

    // ── --explain-perf footer (TASK-524) ────────────────────────────

    #[test]
    fn query_with_explain_perf_emits_perf_footer_and_no_row_table() {
        let scratch = Scratch::new("explain-perf-footer");
        init_db_with_events(&scratch);
        let db = insert_events(
            &scratch,
            "('alice', 1700000000000000000, 'login'), \
             ('bob',   1700000001000000000, 'logout')",
        );

        let args = sv(&["query", "events", "--db", &db, "--explain-perf"]);
        let mut out = Vec::new();
        let mut err = Vec::new();
        run(&args, &mut out, &mut err).expect("query --explain-perf must succeed");
        let out_text = String::from_utf8(out).unwrap();

        // Every section header must be present so the footer is
        // self-evidently "the perf surface" and not something else.
        for header in [
            "bqlite query perf metrics",
            "Throughput",
            "CPU",
            "Skew",
            "Spill",
        ] {
            assert!(
                out_text.contains(header),
                "perf footer missing '{header}': {out_text}"
            );
        }

        // The row-table renderer always emits a `(N rows)` footer; the
        // perf path must not render one.
        assert!(
            !out_text.contains("(2 rows)"),
            "perf-only path must not render the row table: {out_text}"
        );
        assert!(
            !out_text.contains("(0 rows)"),
            "perf-only path must not render the row table: {out_text}"
        );

        // Wall-clock duration must surface (non-zero on any sane clock).
        assert!(
            out_text.contains("wall_clock_ns"),
            "perf footer missing wall_clock_ns row: {out_text}"
        );
        // CPU sampling flag must reflect that --explain-perf opted in.
        assert!(
            out_text.contains("cpu_metrics_enabled        : true"),
            "expected cpu_metrics_enabled: true on --explain-perf path: {out_text}"
        );
    }

    #[test]
    fn explain_perf_against_empty_table_still_renders_footer() {
        let scratch = Scratch::new("explain-perf-empty");
        init_db_with_events(&scratch);
        let db_path_str = scratch.path().to_string_lossy().to_string();

        let args = sv(&["query", "events", "--db", &db_path_str, "--explain-perf"]);
        let mut out = Vec::new();
        let mut err = Vec::new();
        run(&args, &mut out, &mut err).expect("query --explain-perf must succeed");
        let out_text = String::from_utf8(out).unwrap();

        // Section headers always render even when every row is zero.
        for header in ["Throughput", "CPU", "Skew", "Spill"] {
            assert!(out_text.contains(header), "missing {header}: {out_text}");
        }
        // CPU sampling is opt-in via --explain-perf, so the flag is true.
        assert!(
            out_text.contains("cpu_metrics_enabled        : true"),
            "{out_text}"
        );
        // The CPU rows are labelled when collection was enabled but
        // perf counters could not be sampled — TASK-537 scope (f). The
        // empty-table SELECT dispatches via the single-task path (no
        // per-shard workers ever open perf counters), so the runner
        // observes `cpu_counters_available == false` regardless of
        // whether `CAP_PERFMON` is granted. Accept either the
        // "not collected (no CAP_PERFMON)" label (the expected path)
        // or a concrete numeric render (forward-compatible: if a
        // future change opens perf counters even on the single-task
        // path, the assertion still holds). What we never want to see
        // is a bare zero with no label — that would be the pre-fix
        // ambiguous render.
        let cycles_line = out_text
            .lines()
            .find(|l| l.trim_start().starts_with("total_cpu_cycles"))
            .unwrap_or("");
        assert!(
            cycles_line.contains("not collected") || cycles_line.contains(": "),
            "total_cpu_cycles row must carry either a 'not collected' label or a \
             concrete value, never a bare zero: {out_text}"
        );
        // The same assertion at the cycles_per_event derived row.
        let cpe_line = out_text
            .lines()
            .find(|l| l.trim_start().starts_with("cycles_per_event"))
            .unwrap_or("");
        assert!(
            cpe_line.contains("not collected") || cpe_line.contains("—") || cpe_line.contains("."),
            "cycles_per_event row must be either labelled, an em-dash, or a numeric ratio: {out_text}"
        );
    }

    #[test]
    fn truncation_footer_shown_when_rows_exceed_display_cap() {
        // Insert 5 rows. Query with --limit 3: engine runs | limit 4,
        // returns 4 rows, renderer truncates to 3 and appends the footer.
        let scratch = Scratch::new("truncation-footer");
        init_db_with_events(&scratch);
        let db = insert_events(
            &scratch,
            "('u1', 1700000000000000000, 'e'), \
             ('u2', 1700000001000000000, 'e'), \
             ('u3', 1700000002000000000, 'e'), \
             ('u4', 1700000003000000000, 'e'), \
             ('u5', 1700000004000000000, 'e')",
        );

        let args = sv(&["query", "events", "--db", &db, "--limit", "3"]);
        let mut out = Vec::new();
        let mut err = Vec::new();
        run(&args, &mut out, &mut err).expect("query must succeed");
        let out_text = String::from_utf8(out).unwrap();
        assert!(
            out_text.contains("showing 3 rows"),
            "expected truncation footer 'showing 3 rows', got:\n{out_text}"
        );
        assert!(
            out_text.contains("--no-limit"),
            "truncation footer must mention --no-limit, got:\n{out_text}"
        );
    }

    #[test]
    fn no_limit_with_real_rows_returns_all_without_footer() {
        // Insert 3 rows. --no-limit must return all 3 with no truncation footer.
        let scratch = Scratch::new("no-limit-real");
        init_db_with_events(&scratch);
        let db = insert_events(
            &scratch,
            "('x1', 1700000000000000000, 'click'), \
             ('x2', 1700000001000000000, 'click'), \
             ('x3', 1700000002000000000, 'click')",
        );

        let args = sv(&["query", "events", "--db", &db, "--no-limit"]);
        let mut out = Vec::new();
        let mut err = Vec::new();
        run(&args, &mut out, &mut err).expect("query must succeed");
        let out_text = String::from_utf8(out).unwrap();
        assert!(
            out_text.contains("(3 rows)"),
            "expected '(3 rows)' footer, got:\n{out_text}"
        );
        assert!(
            !out_text.contains("showing"),
            "no truncation footer expected with --no-limit, got:\n{out_text}"
        );
    }

    #[test]
    fn default_limit_exceeded_shows_truncation_footer() {
        // Insert DEFAULT_LIMIT+1 (1001) rows without an explicit --limit.
        // The CLI auto-injects | limit 1002, gets 1001 rows back, truncates
        // to 1000 and appends the truncation footer.
        let scratch = Scratch::new("default-limit-exceeded");
        init_db_with_events(&scratch);
        let db_path_str = scratch.path().to_string_lossy().to_string();

        // Build a 1001-row INSERT VALUES in one shot.
        let values: String = (0usize..1001)
            .map(|i| {
                format!(
                    "('e{i:05}', {}, 'e')",
                    1_700_000_000_000_000_000_i64 + i as i64
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let insert_bql = format!("INSERT INTO events VALUES {values}");
        let insert_args = sv(&["query", &insert_bql, "--db", &db_path_str]);
        let mut out = Vec::new();
        let mut err = Vec::new();
        run(&insert_args, &mut out, &mut err).expect("INSERT 1001 rows must succeed");

        // Query with no explicit limit: auto-injection fires.
        let args = sv(&["query", "events", "--db", &db_path_str]);
        let mut out = Vec::new();
        let mut err = Vec::new();
        run(&args, &mut out, &mut err).expect("query must succeed");
        let out_text = String::from_utf8(out).unwrap();

        // Truncation footer: "showing 1,000 rows; use --limit N or --no-limit".
        assert!(
            out_text.contains("showing 1,000 rows"),
            "expected 'showing 1,000 rows' truncation footer, got:\n{out_text}"
        );
        assert!(
            out_text.contains("--no-limit"),
            "truncation footer must mention --no-limit, got:\n{out_text}"
        );
    }

    // ── ingest subcommand dispatcher ─────────────────────────────────

    #[test]
    fn run_dispatches_to_ingest_subcommand() {
        let scratch = Scratch::new("ingest-dispatch");
        let db_path_str = scratch.path().to_string_lossy().to_string();
        // Missing --table → usage error propagated from ingest module.
        let args = sv(&["ingest", "data.csv", "--db", &db_path_str]);
        let mut out = Vec::new();
        let mut err = Vec::new();
        match run(&args, &mut out, &mut err) {
            Err(CliError::Usage(msg)) => assert!(msg.contains("--table")),
            other => panic!("expected usage error from ingest dispatch, got {other:?}"),
        }
    }

    #[test]
    fn query_with_empty_bql_surfaces_parse_error_from_engine() {
        let scratch = Scratch::new("empty-bql");
        init_db_with_events(&scratch);
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
