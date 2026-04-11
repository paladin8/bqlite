//! Unit-style smoke tests for [`bqlite_tests::common`].
//!
//! This target is deliberately small: it exists to compile-check the
//! helpers in `bqlite_tests::common`, verify they behave as documented,
//! and double as the canonical template every future test binary under
//! `tests/tests/` copies. The real Wave 1 smoke test
//! (`bqlite query "events"` end-to-end) lands as
//! `tests/tests/smoke.rs` under TASK-123.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bqlite_tests::common::{
    assert_batches_empty, assert_batches_eq, load_fixture, FixtureError, TempDb,
};

/// A one-column `Int64` batch with the given values. Tiny helper so
/// the assertion tests below read as assertions, not setup.
fn int_batch(values: Vec<i64>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
    let column: ArrayRef = Arc::new(Int64Array::from(values));
    RecordBatch::try_new(schema, vec![column]).expect("constructing an int batch must not fail")
}

#[test]
fn temp_db_creates_directory_and_removes_on_drop() {
    let owned_path;
    {
        let db = TempDb::new();
        owned_path = db.path().to_path_buf();
        assert!(
            owned_path.exists(),
            "TempDb path {} should exist immediately after new()",
            owned_path.display(),
        );
        assert!(
            owned_path.is_dir(),
            "TempDb path {} should be a directory",
            owned_path.display(),
        );
    }
    assert!(
        !owned_path.exists(),
        "TempDb should remove {} on drop",
        owned_path.display(),
    );
}

#[test]
fn temp_db_paths_are_unique_per_instance() {
    let a = TempDb::new();
    let b = TempDb::new();
    assert_ne!(
        a.path(),
        b.path(),
        "two concurrent TempDb instances must have distinct paths"
    );
}

#[test]
fn temp_db_path_is_absolute_under_system_temp() {
    let db = TempDb::new();
    let p = db.path();
    assert!(
        p.is_absolute(),
        "temp db path must be absolute: {}",
        p.display()
    );
    // The name itself should start with the agreed-upon prefix. We
    // compare only the final component because `std::env::temp_dir()`
    // can resolve through symlinks on some platforms.
    let file_name = p
        .file_name()
        .and_then(|n| n.to_str())
        .expect("temp db path must have a valid utf-8 file name");
    assert!(
        file_name.starts_with("bqlite-test-"),
        "temp db name should start with 'bqlite-test-', got {file_name}",
    );
}

#[test]
fn load_fixture_returns_not_yet_supported_in_wave1() {
    // The argument is ignored by the Wave 1 stub — any path works.
    // This test exists to freeze the "always errors in Wave 1" contract
    // so that Wave 2 (TASK-233) explicitly replaces it when the real
    // CSV loader lands.
    let err =
        load_fixture(std::path::Path::new("/dev/null")).expect_err("wave 1 loader must be a stub");
    assert!(
        matches!(err, FixtureError::NotYetSupported { .. }),
        "unexpected fixture error variant: {err:?}",
    );
}

#[test]
fn assert_batches_empty_accepts_zero_batches() {
    assert_batches_empty(&[]);
}

#[test]
fn assert_batches_empty_accepts_zero_row_batch() {
    assert_batches_empty(&[int_batch(vec![])]);
}

#[test]
#[should_panic(expected = "expected an empty result set")]
fn assert_batches_empty_panics_on_non_empty_batch() {
    assert_batches_empty(&[int_batch(vec![1, 2, 3])]);
}

#[test]
fn assert_batches_eq_same_values() {
    let a = int_batch(vec![1, 2, 3]);
    let b = int_batch(vec![1, 2, 3]);
    assert_batches_eq(&[a], &[b]);
}

#[test]
fn assert_batches_eq_empty_vs_empty() {
    let actual: Vec<RecordBatch> = vec![];
    let expected: Vec<RecordBatch> = vec![];
    assert_batches_eq(&actual, &expected);
}

#[test]
#[should_panic(expected = "row count mismatch")]
fn assert_batches_eq_row_count_mismatch() {
    let a = int_batch(vec![1, 2]);
    let b = int_batch(vec![1, 2, 3]);
    assert_batches_eq(&[a], &[b]);
}

#[test]
#[should_panic(expected = "value mismatch")]
fn assert_batches_eq_value_mismatch() {
    let a = int_batch(vec![1, 2, 3]);
    let b = int_batch(vec![1, 2, 4]);
    assert_batches_eq(&[a], &[b]);
}

#[test]
#[should_panic(expected = "batch count mismatch")]
fn assert_batches_eq_batch_count_mismatch() {
    // Same total row count, but split into different numbers of batches.
    let actual = vec![int_batch(vec![1, 2, 3])];
    let expected = vec![int_batch(vec![1]), int_batch(vec![2, 3])];
    assert_batches_eq(&actual, &expected);
}

#[test]
#[should_panic(expected = "schema mismatch")]
fn assert_batches_eq_schema_mismatch() {
    // Same values but different field names.
    let schema_a = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
    let schema_b = Arc::new(Schema::new(vec![Field::new("y", DataType::Int64, false)]));
    let col: ArrayRef = Arc::new(Int64Array::from(vec![1_i64]));
    let a = RecordBatch::try_new(schema_a, vec![col.clone()]).unwrap();
    let b = RecordBatch::try_new(schema_b, vec![col]).unwrap();
    assert_batches_eq(&[a], &[b]);
}
