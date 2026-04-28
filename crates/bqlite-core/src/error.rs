use thiserror::Error;

/// The unified error type for all bqlite operations.
///
/// All fallible bqlite APIs return `Result<T, BqliteError>`.
///
/// Conversion impls are provided for `std::io::Error` and
/// `arrow::error::ArrowError` so callers can use `?` in storage and
/// Arrow-heavy code paths.
#[derive(Debug, Error)]
pub enum BqliteError {
    /// An I/O error from the storage layer.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// An Arrow operation failed.
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// Schema validation failed or two schemas are incompatible.
    #[error("Schema error: {0}")]
    Schema(String),

    /// Query text could not be parsed.
    #[error("Parse error: {0}")]
    Parse(String),

    /// The query plan is invalid: unknown table, type mismatch, etc.
    ///
    /// This variant is used wherever a `TypeError` would be raised in the
    /// planner layer — callers pattern-match on `BqliteError::Plan` for
    /// structured plan-time errors.
    #[error("Plan error: {0}")]
    Plan(String),

    /// An error occurred during query execution.
    #[error("Execution error: {0}")]
    Execution(String),

    /// An on-disk structure (segment file, manifest, ...) failed
    /// validation because its bytes are malformed, truncated, or
    /// fail a checksum.
    ///
    /// Raised by the segment reader (TASK-215) when any of the
    /// `segment-format-v1.md` §15 validation rules trip, and by any
    /// future storage-layer surface that deserializes persistent
    /// bytes. Distinct from [`BqliteError::Execution`] because a
    /// corruption error is a file-integrity signal: callers can
    /// react by quarantining the affected segment and continuing
    /// with the rest of the manifest inventory (storage-format.md
    /// §9.5), rather than aborting the query outright.
    #[error("Corruption error: {0}")]
    Corruption(String),

    /// The query was cancelled by the caller.
    #[error("Query cancelled")]
    Cancelled,

    /// The query exceeded its configured timeout. Carries the elapsed
    /// time in milliseconds for diagnostics.
    ///
    /// Surfaced by the engine driver after a per-query timer fires
    /// `CancelReason::Timeout`. See `docs/design/engine/cancellation.md`
    /// §3.1 / §4.3 for the precedence rules and §6.1 for the variant's
    /// place in the reconciled error catalogue.
    #[error("query timed out after {elapsed_ms}ms")]
    Timeout { elapsed_ms: u64 },

    /// A worker (or the single-threaded driver) panicked while
    /// executing the query.
    ///
    /// `message` is the panic payload rendered as a `String`
    /// (best-effort `Display` of the `Box<dyn Any>` payload). `location`
    /// is the `file:line:column` of the panic site when a project-local
    /// panic hook captured it; `None` if the standard panic hook
    /// discarded the location before `catch_unwind` returned.
    /// See `docs/design/engine/cancellation.md` §4.
    #[error("worker panicked{}: {message}",
        location.as_deref().map(|l| format!(" at {l}")).unwrap_or_default())]
    OperatorPanic {
        message: String,
        location: Option<String>,
    },

    /// An operator's memory reservation could not be satisfied because
    /// the per-query budget would have been exceeded.
    ///
    /// `used` is the bytes already reserved against the budget at the
    /// moment the reservation failed; `budget` is the configured
    /// maximum. Shape matches `docs/design/engine/cancellation.md`
    /// §4.3 / §6.1 (frozen) — replaces the stringly-typed
    /// `BqliteError::Execution` previously returned by
    /// `bqlite_core::memory::budget_exceeded_error`.
    #[error("memory budget exceeded: {used} bytes used of {budget} byte budget")]
    MemoryBudgetExceeded { used: u64, budget: u64 },

    /// A grouping operator would have exceeded its hard cap on group
    /// cardinality.
    ///
    /// Raised by `HashAccumulator` (HashAggregate group cap) and
    /// `DistinctOperator` (distinct-row cap). See
    /// `docs/design/engine/cancellation.md` §6.1.
    #[error("group cardinality limit exceeded: {limit} groups")]
    MaxGroupsExceeded { limit: usize },
}

/// Convenience alias: `bqlite_core::Result<T>` is `Result<T, BqliteError>`.
pub type Result<T> = std::result::Result<T, BqliteError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeout_display_includes_elapsed() {
        let err = BqliteError::Timeout { elapsed_ms: 12_345 };
        let s = err.to_string();
        assert!(s.contains("12345"), "{s}");
        assert!(s.contains("timed out"), "{s}");
    }

    #[test]
    fn operator_panic_display_with_location() {
        let err = BqliteError::OperatorPanic {
            message: "boom".into(),
            location: Some("src/foo.rs:10:5".into()),
        };
        let s = err.to_string();
        assert!(s.contains("boom"), "{s}");
        assert!(s.contains("src/foo.rs:10:5"), "{s}");
    }

    #[test]
    fn operator_panic_display_without_location() {
        let err = BqliteError::OperatorPanic {
            message: "boom".into(),
            location: None,
        };
        let s = err.to_string();
        assert!(s.contains("boom"), "{s}");
        assert!(!s.contains(" at "), "{s}");
    }

    #[test]
    fn memory_budget_exceeded_display_has_used_and_budget() {
        let err = BqliteError::MemoryBudgetExceeded {
            used: 200,
            budget: 250,
        };
        let s = err.to_string();
        assert!(s.contains("200"), "{s}");
        assert!(s.contains("250"), "{s}");
    }

    #[test]
    fn max_groups_exceeded_display_has_limit() {
        let err = BqliteError::MaxGroupsExceeded { limit: 1_000_000 };
        assert!(err.to_string().contains("1000000"));
    }
}
