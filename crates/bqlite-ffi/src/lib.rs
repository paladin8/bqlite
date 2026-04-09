//! # bqlite-ffi
//!
//! C ABI / FFI surface for bqlite, used by the PyO3 Python bindings.
//!
//! This crate exposes the bqlite engine through a C-compatible interface
//! that the Python package (`python/bqlite`) calls via PyO3. It handles:
//! - Database open/close lifecycle
//! - Query execution and result marshaling
//! - Arrow RecordBatch → PyArrow zero-copy conversion
//! - Error translation to Python exceptions
