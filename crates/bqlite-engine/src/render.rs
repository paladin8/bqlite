//! Text rendering for [`ExecutionResult`].
//!
//! This module is the single place the CLI (TASK-119) and any future
//! text-output caller go through to turn an [`ExecutionResult`] into a
//! string a human can read. It lives in `bqlite-engine` rather than in
//! `bqlite-cli` for two reasons:
//!
//! 1. The dependency graph in `docs/architecture.md` forbids `bqlite-cli`
//!    from importing `arrow` or any other bqlite crate besides
//!    `bqlite-engine`. Arrow's `pretty_format_batches` is the natural
//!    tool for rendering tabular data, and it is an `arrow` API — so it
//!    has to be called from an engine-level helper that can re-export
//!    the rendered string across the crate boundary.
//! 2. Putting rendering next to the execution result lets tests
//!    round-trip `Engine::query(...)` → `format_result_as_text(...)`
//!    without a process boundary, which is valuable for the Wave 1
//!    smoke-test shape (empty result) and the Wave 2 shapes (non-empty
//!    results with assorted types) both.
//!
//! ## Format
//!
//! The rendered string has two distinct shapes:
//!
//! - **Empty result.** A single schema header line of the form
//!   `name:type | name:type | ...`, followed by `(0 rows)`. `arrow`'s
//!   pretty printer collapses an empty-batch input to the empty string,
//!   which would hide the column set entirely — unacceptable for the
//!   Wave 1 smoke test, which exists specifically to assert that the
//!   engine reaches the schema for an empty `events` table. So we build
//!   the header manually from the `OperatorSchema` instead of relying
//!   on Arrow for the empty path.
//!
//! - **Non-empty result.** Arrow's `pretty_format_batches` output (an
//!   ASCII-ruled table with column headers) followed by a
//!   `(N row[s])` footer. Arrow's formatter already includes the
//!   column names as the table header, so we don't print our own
//!   schema line in this case — doing so would duplicate the header.
//!
//! The trailing newline is always included so the CLI can print the
//! string with a plain `print!` and end up with terminal output that
//! ends on its own line.
//!
//! ## Why arrow's `pretty_format_batches` rather than a hand-rolled
//! formatter
//!
//! A hand-rolled formatter would have to re-implement per-Arrow-type
//! display logic, alignment, and null handling — all of which Arrow
//! already ships. Wave 1 is a throwaway stub intended to make the
//! smoke test work; a ~10-line wrapper over Arrow's formatter beats a
//! 100-line reinvention. If Wave 2 needs fancier rendering (wrapping,
//! per-column width limits, ANSI color), we revisit this decision.

use crate::query::ExecutionResult;

/// Format an [`ExecutionResult`] as a human-readable text table.
///
/// For an empty result, returns a schema header of the form
/// `name:type | name:type | ...\n(0 rows)\n`. For a non-empty result,
/// returns the Arrow pretty-printed table followed by a `(N rows)`
/// footer.
///
/// This function never panics. A rendering failure inside Arrow
/// (extremely unlikely — pretty printing is infallible for schemas
/// Arrow itself produced) is reported inline as `(render error: ...)`
/// and the footer still lands so row counts stay visible.
///
/// Always ends with a newline.
pub fn format_result_as_text(result: &ExecutionResult) -> String {
    // Fast path: empty result. Arrow's pretty printer produces the
    // empty string when handed zero batches, which would erase the
    // schema row the Wave 1 smoke test relies on. Emit the header
    // ourselves from the `OperatorSchema` so the caller always sees
    // which columns the query would have produced.
    if result.is_empty() {
        let header = result
            .schema
            .columns()
            .iter()
            .map(|c| format!("{}:{}", c.name, c.bql_type))
            .collect::<Vec<_>>()
            .join(" | ");
        let mut out = String::with_capacity(header.len() + 16);
        out.push_str(&header);
        out.push('\n');
        out.push_str("(0 rows)\n");
        return out;
    }

    // Non-empty: defer to Arrow's pretty printer, then append the
    // row-count footer. We concatenate instead of using `format!` to
    // avoid an intermediate `String` for the table body.
    let mut out = String::new();
    match arrow::util::pretty::pretty_format_batches(&result.rows) {
        Ok(formatted) => {
            out.push_str(&formatted.to_string());
            // `pretty_format_batches` does not terminate its output
            // with a newline; add one so the footer starts on its own
            // line regardless of what Arrow emits.
            if !out.ends_with('\n') {
                out.push('\n');
            }
        }
        Err(e) => {
            // Render failures should never lose the row count — the
            // CLI's truncation footer is the user's only hint that
            // rows were actually produced. Emit a diagnostic line and
            // fall through to the footer.
            out.push_str(&format!("(render error: {e})\n"));
        }
    }

    let count = result.row_count();
    let unit = if count == 1 { "row" } else { "rows" };
    out.push_str(&format!("({count} {unit})\n"));
    out
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;

    use bqlite_core::property::BqlType;
    use bqlite_core::schema::{ColumnDef, OperatorSchema};

    use super::*;
    use crate::query::ExecutionResult;

    fn schema_of_table_shape() -> OperatorSchema {
        // Mimics the bootstrap `events` table plus the `__seq_id` /
        // `__batch_id` system columns — the shape the Wave 1 smoke
        // test exercises.
        OperatorSchema::new(vec![
            ColumnDef::required("entity_id", BqlType::String),
            ColumnDef::required("ts", BqlType::Timestamp),
            ColumnDef::required("event_type", BqlType::String),
            ColumnDef::required("__seq_id", BqlType::Int),
            ColumnDef::required("__batch_id", BqlType::Int),
        ])
        .expect("schema is unique by construction")
    }

    #[test]
    fn empty_result_emits_header_and_zero_rows_footer() {
        let result = ExecutionResult {
            schema: schema_of_table_shape(),
            rows: Vec::new(),
        };

        let rendered = format_result_as_text(&result);

        // Header: every column's "name:type" token, joined by " | ".
        assert!(
            rendered.contains("entity_id:STRING"),
            "header should include entity_id:STRING, got:\n{rendered}"
        );
        assert!(rendered.contains("ts:TIMESTAMP"));
        assert!(rendered.contains("event_type:STRING"));
        assert!(rendered.contains("__seq_id:INT"));
        assert!(rendered.contains("__batch_id:INT"));
        assert!(
            rendered.contains(" | "),
            "header columns should be delimited by ' | ', got:\n{rendered}"
        );
        // Footer: literal "(0 rows)" — the Wave 1 task description
        // pins this exact phrasing.
        assert!(
            rendered.contains("(0 rows)"),
            "empty result footer must be '(0 rows)', got:\n{rendered}"
        );
        // Always ends with a newline so the CLI can print! instead of
        // println! and still land on its own line.
        assert!(rendered.ends_with('\n'));
    }

    #[test]
    fn empty_result_with_zero_column_schema_still_emits_footer() {
        // Defensive edge case: an empty schema shouldn't crash the
        // header builder or leave the footer missing. A zero-column
        // operator schema is unusual but legal (a degenerate SELECT
        // with no projection), and we want the renderer to handle it.
        let result = ExecutionResult {
            schema: OperatorSchema::new(Vec::new()).expect("empty schema is valid"),
            rows: Vec::new(),
        };

        let rendered = format_result_as_text(&result);

        // Header is empty but the line still exists — just a bare "\n".
        assert!(rendered.starts_with('\n'), "expected empty header line");
        assert!(rendered.contains("(0 rows)"));
    }

    fn two_row_batch() -> (OperatorSchema, RecordBatch) {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2])),
                Arc::new(StringArray::from(vec!["alice", "bob"])),
            ],
        )
        .expect("build batch");
        let schema = OperatorSchema::new(vec![
            ColumnDef::required("id", BqlType::Int),
            ColumnDef::required("name", BqlType::String),
        ])
        .expect("schema is unique by construction");
        (schema, batch)
    }

    #[test]
    fn non_empty_result_uses_arrow_pretty_printer_and_plural_footer() {
        let (schema, batch) = two_row_batch();
        let result = ExecutionResult {
            schema,
            rows: vec![batch],
        };

        let rendered = format_result_as_text(&result);

        // Arrow's pretty printer draws ASCII rules. Don't pin the
        // exact drawing chars — just assert the values landed and the
        // column headers are present (Arrow emits its own header row).
        assert!(rendered.contains("alice"));
        assert!(rendered.contains("bob"));
        assert!(rendered.contains("id"));
        assert!(rendered.contains("name"));
        // Plural footer because row_count > 1.
        assert!(
            rendered.contains("(2 rows)"),
            "footer must report plural row count, got:\n{rendered}"
        );
        assert!(rendered.ends_with('\n'));
    }

    #[test]
    fn single_row_result_uses_singular_row_footer() {
        // Singular / plural wording is a classic off-by-one to get
        // wrong. A single-row result must say "row", not "rows".
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(arrow_schema, vec![Arc::new(Int64Array::from(vec![7_i64]))])
                .expect("build batch");

        let result = ExecutionResult {
            schema: OperatorSchema::new(vec![ColumnDef::required("id", BqlType::Int)])
                .expect("schema is unique by construction"),
            rows: vec![batch],
        };

        let rendered = format_result_as_text(&result);
        assert!(
            rendered.contains("(1 row)"),
            "singular footer expected, got:\n{rendered}"
        );
        assert!(
            !rendered.contains("(1 rows)"),
            "must not use plural 'rows' for single-row result, got:\n{rendered}"
        );
    }

    #[test]
    fn multiple_batches_are_all_rendered_and_row_count_sums_them() {
        // A query result can comprise several record batches; the
        // renderer must concatenate them through Arrow's formatter and
        // report the total row count in the footer.
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));
        let a = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2]))],
        )
        .unwrap();
        let b = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![3_i64, 4, 5]))],
        )
        .unwrap();
        let result = ExecutionResult {
            schema: OperatorSchema::new(vec![ColumnDef::required("id", BqlType::Int)])
                .expect("schema is unique by construction"),
            rows: vec![a, b],
        };

        let rendered = format_result_as_text(&result);
        for val in ["1", "2", "3", "4", "5"] {
            assert!(
                rendered.contains(val),
                "every row value should appear, missing {val} in:\n{rendered}"
            );
        }
        assert!(rendered.contains("(5 rows)"));
    }
}
