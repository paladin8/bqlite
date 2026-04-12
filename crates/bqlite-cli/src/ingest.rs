//! `bqlite ingest` subcommand.
//!
//! Syntax:
//! ```text
//! bqlite ingest <file-path> --table <name> --db <db-path>
//!              [--format csv|jsonl|parquet]
//!              [--map "src=dst[,src2=dst2,...]"]
//! ```
//!
//! The command is a thin CLI wrapper: it constructs the equivalent
//! `INSERT INTO <table> FROM '<path>' WITH (...)` BQL statement and hands
//! it to `Engine::query`. The CLI never touches the parser or planner
//! directly — all crossing into the query pipeline goes through the
//! engine, matching the `bqlite-cli → bqlite-engine` dependency rule in
//! `docs/architecture.md`.
//!
//! ## Argument parsing
//!
//! The same hand-rolled flag parser as the `query` subcommand. There are
//! no optional values that require shell quoting beyond `--map`, which the
//! user must quote at the shell level themselves.
//!
//! ## SQL construction
//!
//! The generated statement follows the grammar for `INSERT INTO … FROM`:
//!
//! ```sql
//! INSERT INTO `<table>` FROM '<path>' WITH (format: '<fmt>'[, map: (<src> AS <dst>, ...)])
//! ```
//!
//! The file path is single-quote-escaped (a `'` in the path is replaced
//! with `''` per the BQL string-literal convention). The table name is
//! backtick-quoted to tolerate names with spaces or other special chars
//! (backtick-quoting allows any character except a literal backtick itself
//! — paths with backticks in the table name are rejected with a usage
//! error). Column names in the `--map` clause must be bare BQL identifiers;
//! names containing non-identifier characters are passed through as-is and
//! will produce a parse error from the engine, which surfaces as a
//! `CliError::Runtime`.

use std::io::Write;
use std::path::PathBuf;

use bqlite_engine::{Database, Engine};

use crate::CliError;

/// Parsed arguments for `bqlite ingest`.
#[derive(Debug, PartialEq)]
pub struct IngestArgs {
    /// Path to the source data file (CSV etc.).
    pub file_path: PathBuf,
    /// Target table name.
    pub table: String,
    /// Database directory path.
    pub db_path: PathBuf,
    /// Optional ingest format. Defaults to `"csv"` when absent.
    pub format: Option<String>,
    /// Column rename pairs: `(csv_col, table_col)`.
    pub column_map: Vec<(String, String)>,
}

/// Parse the argv tail of `bqlite ingest …`.
///
/// Accepts the file path as the first positional argument. All flags
/// (`--table`, `--db`, `--format`, `--map`) can appear in any order
/// relative to the positional.
pub fn parse_ingest_args(rest: &[String]) -> Result<IngestArgs, CliError> {
    let mut file_path: Option<PathBuf> = None;
    let mut table: Option<String> = None;
    let mut db_path: Option<PathBuf> = None;
    let mut format: Option<String> = None;
    let mut column_map: Vec<(String, String)> = Vec::new();

    let mut i = 0;
    while i < rest.len() {
        let arg = &rest[i];
        match arg.as_str() {
            "--table" => {
                let v = rest.get(i + 1).ok_or_else(|| {
                    CliError::Usage("--table requires a table name argument".to_string())
                })?;
                if table.is_some() {
                    return Err(CliError::Usage(
                        "--table specified more than once".to_string(),
                    ));
                }
                table = Some(v.clone());
                i += 2;
            }
            arg_str if arg_str.starts_with("--table=") => {
                let v = &arg_str["--table=".len()..];
                if table.is_some() {
                    return Err(CliError::Usage(
                        "--table specified more than once".to_string(),
                    ));
                }
                table = Some(v.to_string());
                i += 1;
            }
            "--db" => {
                let v = rest
                    .get(i + 1)
                    .ok_or_else(|| CliError::Usage("--db requires a path argument".to_string()))?;
                if db_path.is_some() {
                    return Err(CliError::Usage("--db specified more than once".to_string()));
                }
                db_path = Some(PathBuf::from(v));
                i += 2;
            }
            arg_str if arg_str.starts_with("--db=") => {
                let v = &arg_str["--db=".len()..];
                if db_path.is_some() {
                    return Err(CliError::Usage("--db specified more than once".to_string()));
                }
                db_path = Some(PathBuf::from(v));
                i += 1;
            }
            "--format" => {
                let v = rest.get(i + 1).ok_or_else(|| {
                    CliError::Usage("--format requires a format argument".to_string())
                })?;
                if format.is_some() {
                    return Err(CliError::Usage(
                        "--format specified more than once".to_string(),
                    ));
                }
                format = Some(v.clone());
                i += 2;
            }
            arg_str if arg_str.starts_with("--format=") => {
                let v = &arg_str["--format=".len()..];
                if format.is_some() {
                    return Err(CliError::Usage(
                        "--format specified more than once".to_string(),
                    ));
                }
                format = Some(v.to_string());
                i += 1;
            }
            "--map" => {
                let v = rest.get(i + 1).ok_or_else(|| {
                    CliError::Usage("--map requires a mapping argument".to_string())
                })?;
                if !column_map.is_empty() {
                    return Err(CliError::Usage(
                        "--map specified more than once".to_string(),
                    ));
                }
                column_map = parse_column_map(v)?;
                i += 2;
            }
            arg_str if arg_str.starts_with("--map=") => {
                let v = &arg_str["--map=".len()..];
                if !column_map.is_empty() {
                    return Err(CliError::Usage(
                        "--map specified more than once".to_string(),
                    ));
                }
                column_map = parse_column_map(v)?;
                i += 1;
            }
            "-h" | "--help" => {
                return Err(CliError::Usage(
                    "help for 'ingest' not implemented yet — use `bqlite --help`".to_string(),
                ));
            }
            arg_str if arg_str.starts_with("--") => {
                return Err(CliError::Usage(format!("unknown flag: {arg_str}")));
            }
            _ => {
                // First non-flag token is the file path.
                if file_path.is_some() {
                    return Err(CliError::Usage(
                        "ingest accepts exactly one positional file-path argument".to_string(),
                    ));
                }
                file_path = Some(PathBuf::from(arg));
                i += 1;
            }
        }
    }

    let file_path =
        file_path.ok_or_else(|| CliError::Usage("missing file path argument".to_string()))?;
    let table = table.ok_or_else(|| CliError::Usage("missing --table <name>".to_string()))?;
    let db_path = db_path.ok_or_else(|| CliError::Usage("missing --db <path>".to_string()))?;

    Ok(IngestArgs {
        file_path,
        table,
        db_path,
        format,
        column_map,
    })
}

/// Parse `"src1=dst1,src2=dst2,..."` into a `Vec<(String, String)>`.
fn parse_column_map(map_str: &str) -> Result<Vec<(String, String)>, CliError> {
    if map_str.is_empty() {
        return Err(CliError::Usage("--map value must not be empty".to_string()));
    }
    map_str
        .split(',')
        .map(|pair| {
            let (src, dst) = pair.split_once('=').ok_or_else(|| {
                CliError::Usage(format!("invalid --map entry {pair:?}: expected `src=dst`"))
            })?;
            let src = src.trim();
            let dst = dst.trim();
            if src.is_empty() || dst.is_empty() {
                return Err(CliError::Usage(format!(
                    "invalid --map entry {pair:?}: source and target must be non-empty"
                )));
            }
            Ok((src.to_string(), dst.to_string()))
        })
        .collect()
}

/// Build the `INSERT INTO … FROM … WITH (…)` BQL statement.
///
/// The file path is embedded as a single-quoted BQL string literal with
/// any embedded single-quotes doubled. The table name is backtick-quoted.
///
/// # Errors
///
/// Returns a `CliError::Usage` if the table name contains a backtick (which
/// cannot be backtick-quoted in BQL).
fn build_insert_stmt(args: &IngestArgs) -> Result<String, CliError> {
    // Backtick-quote the table name. Backticks inside the name are not
    // escapeable in BQL, so we reject them upfront.
    let table_name = &args.table;
    if table_name.contains('`') {
        return Err(CliError::Usage(format!(
            "table name {table_name:?} contains a backtick, which cannot be used in BQL identifiers"
        )));
    }
    let quoted_table = format!("`{table_name}`");

    // Single-quote-escape the file path (a `'` in the path is doubled).
    let path_str = args.file_path.to_string_lossy();
    let escaped_path = path_str.replace('\'', "''");

    // Build the WITH clause. The format option is always present (defaults
    // to `csv`). The map clause is appended when --map was given.
    // Apply the same single-quote escaping to the format value as the path,
    // for consistency (valid format names never contain quotes, but being
    // consistent prevents injection if the parser ever relaxes its syntax).
    let fmt_raw = args.format.as_deref().unwrap_or("csv");
    let fmt = fmt_raw.replace('\'', "''");
    let mut with_parts: Vec<String> = vec![format!("format: '{fmt}'")];

    if !args.column_map.is_empty() {
        let mappings: Vec<String> = args
            .column_map
            .iter()
            .map(|(src, dst)| format!("{src} AS {dst}"))
            .collect();
        with_parts.push(format!("map: ({})", mappings.join(", ")));
    }

    Ok(format!(
        "INSERT INTO {quoted_table} FROM '{escaped_path}' WITH ({})",
        with_parts.join(", ")
    ))
}

/// Implementation of `bqlite ingest <file> --table <name> --db <path> …`.
///
/// Parses the arguments, builds the INSERT statement, opens the database,
/// and runs the statement through the engine. On success a confirmation
/// message is printed to `out`.
pub fn run_ingest(
    rest: &[String],
    out: &mut dyn Write,
    _err: &mut dyn Write,
) -> Result<(), CliError> {
    let args = parse_ingest_args(rest)?;
    let stmt = build_insert_stmt(&args)?;

    let mut db = Database::open(&args.db_path)
        .map_err(|e| CliError::Runtime(format!("failed to open database: {e}")))?;

    let engine = Engine::new();
    engine
        .query(&stmt, &mut db)
        .map_err(|e| CliError::Runtime(format!("ingest failed: {e}")))?;

    // INSERT produces no rows; print a short confirmation to stdout so the
    // user knows the command succeeded (unlike `query`, there is no result
    // table to print).
    let table = &args.table;
    let path = args.file_path.display();
    writeln!(out, "ingested '{path}' → table '{table}'")
        .map_err(|e| CliError::Runtime(format!("failed to write output: {e}")))?;

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Write as IoWrite;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    fn sv(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|s| s.to_string()).collect()
    }

    // ── parse_column_map ────────────────────────────────────────────

    #[test]
    fn parse_column_map_single_entry() {
        let m = parse_column_map("uid=user_id").unwrap();
        assert_eq!(m, vec![("uid".to_string(), "user_id".to_string())]);
    }

    #[test]
    fn parse_column_map_multiple_entries() {
        let m = parse_column_map("uid=user_id,event_ts=ts,evt=event_type").unwrap();
        assert_eq!(m.len(), 3);
        assert_eq!(m[0], ("uid".into(), "user_id".into()));
        assert_eq!(m[1], ("event_ts".into(), "ts".into()));
        assert_eq!(m[2], ("evt".into(), "event_type".into()));
    }

    #[test]
    fn parse_column_map_trims_whitespace() {
        let m = parse_column_map(" uid = user_id ").unwrap();
        assert_eq!(m, vec![("uid".into(), "user_id".into())]);
    }

    #[test]
    fn parse_column_map_missing_equals_errors() {
        match parse_column_map("uid-user_id") {
            Err(CliError::Usage(msg)) => assert!(msg.contains("src=dst")),
            other => panic!("expected usage error, got {other:?}"),
        }
    }

    #[test]
    fn parse_column_map_empty_string_errors() {
        match parse_column_map("") {
            Err(CliError::Usage(msg)) => assert!(msg.contains("empty")),
            other => panic!("expected usage error, got {other:?}"),
        }
    }

    #[test]
    fn parse_column_map_empty_source_errors() {
        match parse_column_map("=user_id") {
            Err(CliError::Usage(msg)) => {
                assert!(msg.contains("non-empty"), "got: {msg}");
            }
            other => panic!("expected usage error, got {other:?}"),
        }
    }

    #[test]
    fn parse_column_map_empty_target_errors() {
        match parse_column_map("uid=") {
            Err(CliError::Usage(msg)) => {
                assert!(msg.contains("non-empty"), "got: {msg}");
            }
            other => panic!("expected usage error, got {other:?}"),
        }
    }

    // ── parse_ingest_args ───────────────────────────────────────────

    #[test]
    fn parse_ingest_args_minimal() {
        let args =
            parse_ingest_args(&sv(&["data.csv", "--table", "events", "--db", "/tmp/db"])).unwrap();
        assert_eq!(args.file_path, PathBuf::from("data.csv"));
        assert_eq!(args.table, "events");
        assert_eq!(args.db_path, PathBuf::from("/tmp/db"));
        assert!(args.format.is_none());
        assert!(args.column_map.is_empty());
    }

    #[test]
    fn parse_ingest_args_flags_before_positional() {
        let args =
            parse_ingest_args(&sv(&["--table", "events", "--db", "/tmp/db", "data.csv"])).unwrap();
        assert_eq!(args.file_path, PathBuf::from("data.csv"));
    }

    #[test]
    fn parse_ingest_args_format_flag() {
        let args = parse_ingest_args(&sv(&[
            "data.csv", "--table", "t", "--db", "/db", "--format", "csv",
        ]))
        .unwrap();
        assert_eq!(args.format, Some("csv".to_string()));
    }

    #[test]
    fn parse_ingest_args_map_flag() {
        let args = parse_ingest_args(&sv(&[
            "data.csv",
            "--table",
            "t",
            "--db",
            "/db",
            "--map",
            "uid=user_id",
        ]))
        .unwrap();
        assert_eq!(args.column_map, vec![("uid".into(), "user_id".into())]);
    }

    #[test]
    fn parse_ingest_args_equals_syntax() {
        let args = parse_ingest_args(&sv(&[
            "data.csv",
            "--table=events",
            "--db=/tmp/db",
            "--format=csv",
        ]))
        .unwrap();
        assert_eq!(args.table, "events");
        assert_eq!(args.db_path, PathBuf::from("/tmp/db"));
        assert_eq!(args.format, Some("csv".into()));
    }

    #[test]
    fn parse_ingest_args_missing_file_path_errors() {
        match parse_ingest_args(&sv(&["--table", "t", "--db", "/db"])) {
            Err(CliError::Usage(msg)) => assert!(msg.contains("file path")),
            other => panic!("expected usage error, got {other:?}"),
        }
    }

    #[test]
    fn parse_ingest_args_missing_table_errors() {
        match parse_ingest_args(&sv(&["data.csv", "--db", "/db"])) {
            Err(CliError::Usage(msg)) => assert!(msg.contains("--table")),
            other => panic!("expected usage error, got {other:?}"),
        }
    }

    #[test]
    fn parse_ingest_args_missing_db_errors() {
        match parse_ingest_args(&sv(&["data.csv", "--table", "t"])) {
            Err(CliError::Usage(msg)) => assert!(msg.contains("--db")),
            other => panic!("expected usage error, got {other:?}"),
        }
    }

    #[test]
    fn parse_ingest_args_duplicate_table_errors() {
        match parse_ingest_args(&sv(&[
            "data.csv", "--table", "a", "--table", "b", "--db", "/db",
        ])) {
            Err(CliError::Usage(msg)) => assert!(msg.contains("--table")),
            other => panic!("expected usage error, got {other:?}"),
        }
    }

    #[test]
    fn parse_ingest_args_unknown_flag_errors() {
        match parse_ingest_args(&sv(&["data.csv", "--table", "t", "--db", "/db", "--nope"])) {
            Err(CliError::Usage(msg)) => assert!(msg.contains("--nope")),
            other => panic!("expected usage error, got {other:?}"),
        }
    }

    // ── build_insert_stmt ───────────────────────────────────────────

    fn minimal_args(file: &str, table: &str) -> IngestArgs {
        IngestArgs {
            file_path: PathBuf::from(file),
            table: table.to_string(),
            db_path: PathBuf::from("/db"),
            format: None,
            column_map: Vec::new(),
        }
    }

    #[test]
    fn build_minimal_statement_defaults_to_csv() {
        let args = minimal_args("data.csv", "events");
        let sql = build_insert_stmt(&args).unwrap();
        assert_eq!(
            sql,
            "INSERT INTO `events` FROM 'data.csv' WITH (format: 'csv')"
        );
    }

    #[test]
    fn build_with_explicit_format() {
        let mut args = minimal_args("data.csv", "events");
        args.format = Some("csv".into());
        let sql = build_insert_stmt(&args).unwrap();
        assert!(sql.contains("format: 'csv'"));
    }

    #[test]
    fn build_with_column_map() {
        let mut args = minimal_args("data.csv", "events");
        args.column_map = vec![
            ("uid".into(), "user_id".into()),
            ("evt".into(), "event_type".into()),
        ];
        let sql = build_insert_stmt(&args).unwrap();
        assert!(
            sql.contains("map: (uid AS user_id, evt AS event_type)"),
            "got: {sql}"
        );
    }

    #[test]
    fn build_escapes_single_quote_in_path() {
        let args = minimal_args("da'ta.csv", "events");
        let sql = build_insert_stmt(&args).unwrap();
        // Single quote in path must be doubled.
        assert!(sql.contains("FROM 'da''ta.csv'"), "got: {sql}");
    }

    #[test]
    fn build_backtick_in_table_name_errors() {
        let args = minimal_args("data.csv", "bad`name");
        match build_insert_stmt(&args) {
            Err(CliError::Usage(msg)) => assert!(msg.contains("backtick")),
            other => panic!("expected usage error, got {other:?}"),
        }
    }

    #[test]
    fn build_table_name_with_spaces_is_backtick_quoted() {
        let args = minimal_args("data.csv", "my events");
        let sql = build_insert_stmt(&args).unwrap();
        assert!(sql.starts_with("INSERT INTO `my events`"), "got: {sql}");
    }

    // ── End-to-end: run_ingest against a real temp database ─────────

    static SEQ: AtomicU64 = AtomicU64::new(0);

    struct Scratch {
        path: PathBuf,
    }

    impl Scratch {
        fn new(label: &str) -> Self {
            let seq = SEQ.fetch_add(1, Ordering::Relaxed);
            let pid = std::process::id();
            let mut path = std::env::temp_dir();
            path.push(format!("bqlite-cli-ingest-{label}-{pid}-{seq}"));
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

    /// Create a database at `path` and create the events table.
    ///
    /// Since `Database::create` no longer seeds a bootstrap events table
    /// (TASK-240), end-to-end tests that need to ingest into `events` must
    /// set up the table schema explicitly. This helper mirrors the setup in
    /// `bqlite-engine/src/ingest.rs` tests.
    fn setup_db_with_events(db_path: &Path) {
        let mut db = bqlite_engine::Database::create(db_path).expect("create db");
        let engine = bqlite_engine::Engine::new();
        engine
            .query(
                "CREATE TABLE events (\
                     entity_id STRING NOT NULL ENTITY KEY, \
                     ts TIMESTAMP NOT NULL EVENT TIME, \
                     event_type STRING NOT NULL EVENT TYPE\
                 )",
                &mut db,
            )
            .expect("create events table");
    }

    /// Write `content` to a fresh temp file and return its path.
    ///
    /// The CSV is placed in the system temp dir (not the scratch database
    /// dir) because the database directory must be created via `setup_db_with_events`
    /// before the CSV file is ingested.
    fn write_csv(name: &str, content: &str) -> PathBuf {
        let csv_path = std::env::temp_dir().join(format!(
            "bqlite-cli-ingest-csv-{}-{}-{}",
            name,
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let mut f = fs::File::create(&csv_path).unwrap();
        f.write_all(content.as_bytes()).unwrap();
        csv_path
    }

    #[test]
    fn run_ingest_end_to_end_happy_path() {
        let scratch = Scratch::new("e2e");
        setup_db_with_events(scratch.path());
        let csv = write_csv(
            "events",
            "entity_id,ts,event_type\nalice,1700000000000000000,click\n",
        );
        let db_path = scratch.path().to_string_lossy().to_string();
        let csv_path = csv.to_string_lossy().to_string();

        let args = sv(&[&csv_path, "--table", "events", "--db", &db_path]);

        let mut out = Vec::new();
        let mut err = Vec::new();
        run_ingest(&args, &mut out, &mut err).expect("ingest must succeed");

        let stdout = String::from_utf8(out).unwrap();
        assert!(
            stdout.contains("ingested"),
            "success message expected, got: {stdout}"
        );
        assert!(
            stdout.contains("events"),
            "table name expected in output, got: {stdout}"
        );
        // Note: verifying ingested row count via re-query is deferred until the
        // scan path returns real rows (Wave 3+). The engine-level ingest tests
        // in bqlite-engine/src/ingest.rs verify manifest state via db.manifest().

        let _ = fs::remove_file(&csv);
    }

    #[test]
    fn run_ingest_with_column_map() {
        let scratch = Scratch::new("map");
        setup_db_with_events(scratch.path());
        let csv = write_csv(
            "events_map",
            "uid,event_ts,evt\nalice,1700000000000000000,click\n",
        );
        let db_path = scratch.path().to_string_lossy().to_string();
        let csv_path = csv.to_string_lossy().to_string();

        let args = sv(&[
            &csv_path,
            "--table",
            "events",
            "--db",
            &db_path,
            "--map",
            "uid=entity_id,event_ts=ts,evt=event_type",
        ]);

        let mut out = Vec::new();
        let mut err = Vec::new();
        run_ingest(&args, &mut out, &mut err).expect("ingest with map must succeed");

        // Verify the confirmation message mentions the table.
        let stdout = String::from_utf8(out).unwrap();
        assert!(
            stdout.contains("ingested"),
            "success message expected, got: {stdout}"
        );
        // Note: verifying ingested row count via re-query is deferred until the
        // scan path returns real rows (Wave 3+). The engine-level ingest tests
        // in bqlite-engine/src/ingest.rs verify manifest state via db.manifest().

        let _ = fs::remove_file(&csv);
    }

    #[test]
    fn run_ingest_missing_file_is_runtime_error() {
        let scratch = Scratch::new("missing");
        setup_db_with_events(scratch.path());
        let db_path = scratch.path().to_string_lossy().to_string();

        let args = sv(&[
            "/nonexistent/data.csv",
            "--table",
            "events",
            "--db",
            &db_path,
        ]);
        let mut out = Vec::new();
        let mut err = Vec::new();
        match run_ingest(&args, &mut out, &mut err) {
            Err(CliError::Runtime(msg)) => {
                assert!(msg.contains("ingest failed"), "got: {msg}");
            }
            other => panic!("expected runtime error, got {other:?}"),
        }
    }
}
