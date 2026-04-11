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
}

/// Convenience alias: `bqlite_core::Result<T>` is `Result<T, BqliteError>`.
pub type Result<T> = std::result::Result<T, BqliteError>;
