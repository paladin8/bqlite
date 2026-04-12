//! Auto-limit machinery for the bqlite CLI.
//!
//! This module handles the CLI-boundary row-cap that shields users from
//! accidentally materialising enormous result sets. The strategy:
//!
//! 1. Before execution, call [`maybe_inject_limit`] to append
//!    `| limit N+1` to the BQL text when the query has no explicit
//!    `| limit` stage and the user has not passed `--no-limit`.
//!    Injecting *N+1* (rather than *N*) lets the CLI distinguish
//!    "exactly N rows returned" from "at least N+1 rows exist but we
//!    cut off the stream".
//!
//! 2. After execution, call `bqlite_engine::format_result_as_text_limited`
//!    with the same `N` to render the result, truncating to the first *N*
//!    rows and appending the truncation footer when the engine returned
//!    the full *N+1*.
//!
//! ## Why a text scan rather than a full parse?
//!
//! The CLI cannot call the parser directly (dependency-direction rule in
//! `docs/architecture.md`). A simple byte-level scan for `| limit`
//! (case-insensitive) is sufficient for the heuristic: false positives
//! (a `| limit` string inside a BQL string literal) are benign — they
//! only suppress auto-injection, which the user can override with
//! `--limit N`.
//!
//! ## Default limit
//!
//! [`DEFAULT_LIMIT`] is 1 000 rows. Override with `--limit N` or disable
//! entirely with `--no-limit`.

/// Default row cap injected at the CLI boundary when the query has no
/// explicit `| limit` stage and the user has not passed `--no-limit`.
pub const DEFAULT_LIMIT: usize = 1_000;

/// Return `true` when `query` already contains a `| limit` pipeline stage.
///
/// Uses a byte-level scan rather than a full parse: looks for a `|` token
/// followed (after optional ASCII whitespace) by the five-letter keyword
/// `limit` (case-insensitive) on a word boundary (i.e., not immediately
/// followed by a letter, digit, or `_`).
///
/// This can be fooled by `| limit` embedded inside a BQL string literal,
/// but that is a benign false positive — auto-injection is simply
/// suppressed. Users who genuinely want auto-injection in such a query
/// (unlikely) can pass `--limit N` explicitly.
pub fn has_explicit_limit(query: &str) -> bool {
    let bytes = query.as_bytes();
    let len = bytes.len();
    let mut i = 0;
    while i < len {
        if bytes[i] == b'|' {
            // Skip whitespace after `|`.
            let mut j = i + 1;
            while j < len && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            // Match "limit" case-insensitively.
            if j + 5 <= len && bytes[j..j + 5].eq_ignore_ascii_case(b"limit") {
                // Word-boundary check: must not be followed by `[A-Za-z0-9_]`.
                let after = j + 5;
                let is_boundary =
                    after >= len || !(bytes[after].is_ascii_alphanumeric() || bytes[after] == b'_');
                if is_boundary {
                    return true;
                }
            }
        }
        i += 1;
    }
    false
}

/// Return `true` if `query` starts with a DDL, DML, or metadata keyword
/// that does not support pipeline stages and therefore must never have
/// `| limit` appended.
///
/// Checks the first non-whitespace word case-insensitively against
/// the set `{CREATE, DROP, ALTER, INSERT, DESCRIBE, EXPLAIN}`.
fn starts_with_non_pipeline_keyword(query: &str) -> bool {
    let first = query
        .trim_start()
        .split_ascii_whitespace()
        .next()
        .unwrap_or("");
    matches!(
        first.to_ascii_uppercase().as_str(),
        "CREATE" | "DROP" | "ALTER" | "INSERT" | "DESCRIBE" | "EXPLAIN"
    )
}

/// Return the query with `| limit N+1` appended when it has no explicit
/// limit stage.
///
/// Passing *N+1* (rather than *N*) to the engine lets the CLI detect
/// whether the result was truncated: if the engine returns exactly *N+1*
/// rows, more rows existed beyond the cap.
///
/// If the query already contains a `| limit` stage (detected by
/// [`has_explicit_limit`]), or if it begins with a DDL, DML, or metadata
/// keyword (detected by [`starts_with_non_pipeline_keyword`]), the query
/// is returned unchanged. DDL/DML/metadata statements do not support
/// pipeline stages and must not be modified.
///
/// A trailing `;` is stripped before appending the stage, because the
/// BQL parser rejects `events; | limit 10`.
pub fn maybe_inject_limit(query: &str, limit: usize) -> String {
    if has_explicit_limit(query) || starts_with_non_pipeline_keyword(query) {
        return query.to_string();
    }
    // Strip a single trailing semicolon (plus surrounding whitespace) so
    // the appended stage parses cleanly. Only one `;` is stripped: BQL
    // does not allow statement sequences, so `events;;` is already a
    // parse error before auto-injection is relevant.
    let base = query.trim_end();
    let base = base.strip_suffix(';').unwrap_or(base).trim_end();
    // Inject N+1 so the CLI can tell whether truncation occurred.
    format!("{base} | limit {}", limit.saturating_add(1))
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── has_explicit_limit ──────────────────────────────────────────

    #[test]
    fn no_limit_stage_returns_false() {
        assert!(!has_explicit_limit("events"));
        assert!(!has_explicit_limit("events | WHERE amount > 0"));
        assert!(!has_explicit_limit("events | SELECT user_id"));
    }

    #[test]
    fn limit_stage_detected_uppercase() {
        assert!(has_explicit_limit("events | LIMIT 100"));
    }

    #[test]
    fn limit_stage_detected_lowercase() {
        assert!(has_explicit_limit("events | limit 100"));
    }

    #[test]
    fn limit_stage_detected_mixed_case() {
        assert!(has_explicit_limit("events | Limit 100"));
    }

    #[test]
    fn limit_stage_detected_with_extra_whitespace() {
        assert!(has_explicit_limit("events  |   LIMIT   100"));
    }

    #[test]
    fn limit_stage_not_confused_with_word_limited() {
        // "limited" starts with "limit" but has more characters — must
        // not trigger a false positive.
        assert!(!has_explicit_limit("events | limited_view"));
        assert!(!has_explicit_limit("events | limiter 100"));
    }

    #[test]
    fn limit_in_the_middle_of_pipeline_detected() {
        assert!(has_explicit_limit(
            "events | WHERE x > 0 | limit 50 | SELECT user_id"
        ));
    }

    #[test]
    fn bare_word_limit_not_after_pipe_returns_false() {
        // "limit" as a column name (no preceding `|`) must not trigger.
        assert!(!has_explicit_limit("events | WHERE limit > 0"));
    }

    // ── maybe_inject_limit ──────────────────────────────────────────

    #[test]
    fn inject_appends_limit_plus_one() {
        let q = maybe_inject_limit("events", 1000);
        assert_eq!(q, "events | limit 1001");
    }

    #[test]
    fn inject_strips_trailing_semicolon() {
        let q = maybe_inject_limit("events;", 10);
        assert_eq!(q, "events | limit 11");
    }

    #[test]
    fn inject_strips_trailing_whitespace_and_semicolon() {
        let q = maybe_inject_limit("events ;  ", 5);
        assert_eq!(q, "events | limit 6");
    }

    #[test]
    fn inject_skips_when_limit_already_present() {
        let original = "events | LIMIT 500";
        let q = maybe_inject_limit(original, 1000);
        assert_eq!(q, original);
    }

    #[test]
    fn inject_with_zero_limit_yields_one() {
        // limit=0 → inject "| limit 1" (0.saturating_add(1) == 1).
        let q = maybe_inject_limit("events", 0);
        assert_eq!(q, "events | limit 1");
    }

    #[test]
    fn inject_with_usize_max_does_not_overflow() {
        // saturating_add should prevent overflow when limit == usize::MAX.
        let q = maybe_inject_limit("events", usize::MAX);
        assert!(q.contains("| limit"), "must still have limit stage: {q}");
    }

    #[test]
    fn inject_after_multi_stage_pipeline() {
        let q = maybe_inject_limit("events | WHERE x > 0 | SELECT user_id", 50);
        assert_eq!(q, "events | WHERE x > 0 | SELECT user_id | limit 51");
    }

    // ── DDL / DML / metadata bypass ─────────────────────────────────

    #[test]
    fn inject_skips_create_table() {
        let ddl = "CREATE TABLE foo (id STRING NOT NULL ENTITY KEY)";
        let q = maybe_inject_limit(ddl, 1000);
        assert_eq!(q, ddl, "CREATE TABLE must not be modified");
    }

    #[test]
    fn inject_skips_drop_table() {
        let ddl = "DROP TABLE foo";
        let q = maybe_inject_limit(ddl, 1000);
        assert_eq!(q, ddl);
    }

    #[test]
    fn inject_skips_alter_table() {
        let ddl = "ALTER TABLE foo ADD COLUMN bar STRING";
        let q = maybe_inject_limit(ddl, 1000);
        assert_eq!(q, ddl);
    }

    #[test]
    fn inject_skips_insert() {
        let dml = "INSERT INTO foo FROM 'data.csv' WITH (format: 'csv')";
        let q = maybe_inject_limit(dml, 1000);
        assert_eq!(q, dml);
    }

    #[test]
    fn inject_skips_describe() {
        let meta = "DESCRIBE events";
        let q = maybe_inject_limit(meta, 1000);
        assert_eq!(q, meta);
    }

    #[test]
    fn inject_skips_explain() {
        let meta = "EXPLAIN events | WHERE x > 0";
        let q = maybe_inject_limit(meta, 1000);
        assert_eq!(q, meta);
    }

    #[test]
    fn inject_skips_ddl_case_insensitive() {
        let ddl = "create table foo (id string not null entity key)";
        let q = maybe_inject_limit(ddl, 1000);
        assert_eq!(q, ddl);
    }
}
