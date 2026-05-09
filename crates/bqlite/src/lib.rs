#![doc = include_str!("../README.md")]

pub use bqlite_ast as ast;
pub use bqlite_core as types;
pub use bqlite_engine as engine;
pub use bqlite_parser as parser;

/// The unified error type returned by all fallible bqlite APIs.
///
/// Variants span every layer: I/O, Arrow, schema validation, parse, plan,
/// execution, memory budget, cancellation, and worker panics.
///
/// # Example
///
/// ```
/// use bqlite::BqliteError;
///
/// let err = BqliteError::Execution("row limit exceeded".to_string());
/// assert!(err.to_string().contains("Execution error"));
/// ```
pub use bqlite_core::BqliteError;

/// Convenience `Result` alias: `bqlite::Result<T>` is `std::result::Result<T, BqliteError>`.
///
/// # Example
///
/// ```
/// fn check(ok: bool) -> bqlite::Result<u32> {
///     if ok { Ok(42) } else { Err(bqlite::BqliteError::Parse("bad".to_string())) }
/// }
///
/// assert_eq!(check(true).unwrap(), 42);
/// assert!(check(false).is_err());
/// ```
pub use bqlite_core::Result;
