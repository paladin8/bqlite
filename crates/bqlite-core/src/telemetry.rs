//! Tracing and logging setup for bqlite.
//!
//! This module wires the `tracing` ecosystem for bqlite. It exposes:
//!
//! - [`init_tracing`] — call once at process startup to install the global
//!   subscriber. Reads the `BQLITE_LOG` environment variable for log-level
//!   control (e.g. `BQLITE_LOG=debug`). Output goes to stderr.
//! - [`query_span`] — creates a query-level `tracing::Span` with structured
//!   `query_id` and `query_text` fields.
//! - [`operator_span`] — creates an operator-level child span with structured
//!   `operator` and `label` fields.
//!
//! Later waves will extend this module with the metrics-to-span bridge
//! (wiring `bqlite-core`'s `Metrics` trait into span attributes).

use tracing::Span;
use tracing_subscriber::EnvFilter;

/// Initializes the global tracing subscriber.
///
/// Reads the `BQLITE_LOG` environment variable to determine the log level.
/// Acceptable values are the standard `tracing` filter directives, e.g.:
///
/// ```text
/// BQLITE_LOG=debug          # debug and above for all targets
/// BQLITE_LOG=bqlite=trace   # trace-level for bqlite crates only
/// BQLITE_LOG=warn           # default
/// ```
///
/// If `BQLITE_LOG` is not set the effective level is `warn`. All output
/// goes to stderr. Calling this function more than once is safe — subsequent
/// calls are silently ignored by the subscriber registry.
///
/// # Intended caller
///
/// `bqlite-cli` calls this once at the start of `main`. Library users who
/// embed bqlite in their own process can call it too, or configure their own
/// subscriber before calling any bqlite API.
pub fn init_tracing() {
    let directive = std::env::var("BQLITE_LOG").unwrap_or_else(|_| "warn".to_string());
    let filter = EnvFilter::new(directive);
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
}

/// Creates a query-level tracing span with structured fields.
///
/// The returned span should be entered for the duration of query execution.
/// All operator spans created inside the query should be nested within it.
///
/// # Fields
/// - `query_id`: a caller-assigned identifier for this query invocation
/// - `query_text`: the raw BQL query string
///
/// # Example
/// ```rust
/// # use bqlite_core::telemetry::query_span;
/// let span = query_span("q-0001", "events | filter event_type = 'click'");
/// let _guard = span.enter();
/// // execute query …
/// ```
pub fn query_span(query_id: &str, query_text: &str) -> Span {
    tracing::info_span!("query", query_id = %query_id, query_text = %query_text)
}

/// Creates an operator-level child span.
///
/// Intended to be used within a [`query_span`] for per-operator
/// instrumentation. The `operator_name` identifies the operator type
/// (e.g. `"scan"`, `"filter"`, `"match"`), and `label` provides any
/// additional context useful for debugging (e.g. the table name for a scan).
///
/// # Fields
/// - `operator`: the name of the operator type
/// - `label`: additional context label (e.g. table name, column name)
///
/// # Example
/// ```rust
/// # use bqlite_core::telemetry::operator_span;
/// let span = operator_span("scan", "events");
/// let _guard = span.enter();
/// // drive operator …
/// ```
pub fn operator_span(operator_name: &str, label: &str) -> Span {
    tracing::debug_span!("operator", operator = %operator_name, label = %label)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_span_is_created() {
        let span = query_span("q-0001", "events");
        // Verify the span is not disabled (i.e., it was constructed without panic).
        // We cannot assert on the subscriber here since tests share a global, but
        // we can verify the span value is valid.
        drop(span);
    }

    #[test]
    fn operator_span_is_created() {
        let span = operator_span("scan", "events");
        drop(span);
    }

    #[test]
    fn init_tracing_is_idempotent() {
        // Calling init_tracing twice must not panic.
        init_tracing();
        init_tracing();
    }
}
