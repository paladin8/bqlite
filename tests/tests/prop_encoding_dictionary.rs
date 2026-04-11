//! Property tests for `bqlite_storage::encoding::Dictionary`.
//!
//! Follows the Plain / Delta template: round-trip is the load-bearing
//! invariant, plus a handful of encoding-specific edges.
//!
//! Dictionary claims `BqlType::Int` and `BqlType::String`, so this
//! module tests both code paths using the shared dense-array
//! strategies from `bqlite_tests::strategies`. Unlike Delta, Dictionary
//! accepts empty arrays (cardinality 0, width 1, zero-byte payload),
//! so no bespoke non-empty strategy is needed.
//!
//! See `docs/design/storage/segment-format-v1.md` §9.2 for the
//! byte-level contract this test guards.

use arrow::array::{Array, ArrayRef, Int64Array, StringViewArray};
use bqlite_core::BqlType;
use bqlite_storage::{Dictionary, Encoding};
use bqlite_tests::strategies::{arb_int64_array, arb_string_array};
use proptest::prelude::*;
use std::collections::HashSet;
use std::sync::Arc;

/// Helper that runs one value through `encode → decode` and returns
/// the decoded array. Keeps the encode/decode calls consistent across
/// properties.
fn round_trip(array: &dyn Array, ty: &BqlType) -> ArrayRef {
    let chunk = Dictionary
        .encode(array)
        .expect("Dictionary::encode must accept every dense Int/String array");
    Dictionary
        .decode(&chunk, ty)
        .expect("Dictionary::decode must succeed for a chunk Dictionary produced")
}

proptest! {
    /// Round-trip invariant on `BqlType::Int`. Covers the full `i64`
    /// domain — Dictionary's code stream is agnostic to the dict's
    /// value range, so this property exercises every bit-width path
    /// from 1 (single-value arrays) up to the largest the shared
    /// strategy produces (32 distinct values → 5 bits).
    #[test]
    fn dictionary_round_trip_int(array in arb_int64_array()) {
        let decoded = round_trip(array.as_ref(), &BqlType::Int);
        prop_assert_eq!(decoded.as_ref(), array.as_ref());
    }

    /// Round-trip invariant on `BqlType::String`. Uses the shared
    /// `StringViewArray` strategy so Arrow's `PartialEq` can assert
    /// equality without a cross-type gotcha — `Dictionary::decode`
    /// always produces `StringViewArray`, matching the schema-canonical
    /// type for `BqlType::String`.
    #[test]
    fn dictionary_round_trip_string(array in arb_string_array()) {
        let decoded = round_trip(array.as_ref(), &BqlType::String);
        prop_assert_eq!(decoded.as_ref(), array.as_ref());
    }

    /// `estimate_size` is an exact match against `encode().payload.len()`
    /// for Dictionary. The bit-packed code stream byte count is fully
    /// determined by `row_count` and the chosen `code_bit_width`, so
    /// there is no approximation slack. If a future optimization
    /// widens the estimator to a conservative upper bound, switch
    /// this to `estimate >= actual`.
    #[test]
    fn dictionary_estimate_size_is_exact_int(array in arb_int64_array()) {
        let estimated = Dictionary.estimate_size(array.as_ref()).unwrap();
        let actual = Dictionary.encode(array.as_ref()).unwrap().payload.len();
        prop_assert_eq!(estimated, actual);
    }

    #[test]
    fn dictionary_estimate_size_is_exact_string(array in arb_string_array()) {
        let estimated = Dictionary.estimate_size(array.as_ref()).unwrap();
        let actual = Dictionary.encode(array.as_ref()).unwrap().payload.len();
        prop_assert_eq!(estimated, actual);
    }

    /// The encoded payload is always a multiple of 8 bytes (one SIMD
    /// lane) — §9.2 pads the code stream so a FastLanes unpacker can
    /// read one full lane past the last code. Empty-array chunks have
    /// a zero-byte payload, which still satisfies "multiple of 8".
    #[test]
    fn dictionary_payload_is_simd_aligned(array in arb_int64_array()) {
        let chunk = Dictionary.encode(array.as_ref()).unwrap();
        prop_assert_eq!(chunk.payload.len() % 8, 0);
    }

    /// `chunk.row_count == array.len()` — Dictionary preserves the
    /// row count unchanged, same as every other v1 encoding.
    #[test]
    fn dictionary_row_count_preserved_int(array in arb_int64_array()) {
        let chunk = Dictionary.encode(array.as_ref()).unwrap();
        prop_assert_eq!(chunk.row_count, array.len());
    }

    #[test]
    fn dictionary_row_count_preserved_string(array in arb_string_array()) {
        let chunk = Dictionary.encode(array.as_ref()).unwrap();
        prop_assert_eq!(chunk.row_count, array.len());
    }

    /// Dictionary-specific invariant: every value in the input array
    /// maps to a code in `0..cardinality` where `cardinality` equals
    /// the input's distinct-value count. The round-trip property
    /// covers this implicitly (an OOB code would fail decode), but
    /// this explicit assertion shrinks faster on mis-ordering bugs
    /// in the binary-search lookup. The test inspects chunk.params to
    /// read the cardinality the encoder wrote.
    #[test]
    fn dictionary_codes_dense_in_cardinality_int(array in arb_int64_array()) {
        let chunk = Dictionary.encode(array.as_ref()).unwrap();
        // params layout: [type_tag: u8, code_bit_width: u8, cardinality: u32 LE, ...]
        let cardinality = u32::from_le_bytes(chunk.params[2..6].try_into().unwrap()) as usize;
        // Decode distinct set from input and cross-check cardinality.
        let int_array = array
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("arb_int64_array returns Int64Array");
        let input_distinct: HashSet<i64> = (0..int_array.len())
            .map(|i| int_array.value(i))
            .collect();
        prop_assert_eq!(cardinality, input_distinct.len());
    }

    /// Dictionary-specific invariant: the decoded array's distinct
    /// value set equals the input's distinct value set. This catches
    /// a class of bugs the round-trip property already covers, but
    /// shrinks faster on "one value got mis-mapped" failures because
    /// the counter-example is a set diff rather than a full array.
    #[test]
    fn dictionary_preserves_distinct_int_set(array in arb_int64_array()) {
        let int_array = array
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("arb_int64_array returns Int64Array");
        let input_set: HashSet<i64> = (0..int_array.len())
            .map(|i| int_array.value(i))
            .collect();
        let decoded = round_trip(array.as_ref(), &BqlType::Int);
        let decoded_int = decoded
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Dictionary::decode(Int) returns Int64Array");
        let decoded_set: HashSet<i64> = (0..decoded_int.len())
            .map(|i| decoded_int.value(i))
            .collect();
        prop_assert_eq!(decoded_set, input_set);
    }

    #[test]
    fn dictionary_preserves_distinct_string_set(array in arb_string_array()) {
        let view_array = array
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("arb_string_array returns StringViewArray");
        let input_set: HashSet<String> = (0..view_array.len())
            .map(|i| view_array.value(i).to_string())
            .collect();
        let decoded = round_trip(array.as_ref(), &BqlType::String);
        let decoded_view = decoded
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("Dictionary::decode(String) returns StringViewArray");
        let decoded_set: HashSet<String> = (0..decoded_view.len())
            .map(|i| decoded_view.value(i).to_string())
            .collect();
        prop_assert_eq!(decoded_set, input_set);
    }
}

// ── Example tests covering edge cases proptest might miss ───────────────

/// Empty arrays round-trip as zero-length Int chunks — the cardinality
/// is 0, the payload is 0 bytes, and the dictionary block is empty.
/// Unlike Delta, Dictionary never errors on an empty input.
#[test]
fn dictionary_round_trip_empty_int_array() {
    let array: ArrayRef = Arc::new(Int64Array::from(Vec::<i64>::new()));
    let decoded = round_trip(array.as_ref(), &BqlType::Int);
    assert_eq!(decoded.len(), 0);
}

/// Empty string arrays are symmetric with empty int arrays.
#[test]
fn dictionary_round_trip_empty_string_array() {
    let array: ArrayRef = Arc::new(StringViewArray::from(Vec::<String>::new()));
    let decoded = round_trip(array.as_ref(), &BqlType::String);
    assert_eq!(decoded.len(), 0);
}

/// A single distinct value repeated many times is the canonical
/// "high-compression" Dictionary workload. All codes collapse to 0 and
/// `code_bit_width` floors at 1.
#[test]
fn dictionary_round_trip_constant_int() {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![42_i64; 100]));
    let decoded = round_trip(array.as_ref(), &BqlType::Int);
    assert_eq!(decoded.as_ref(), array.as_ref());
}

/// A single distinct string repeated many times.
#[test]
fn dictionary_round_trip_constant_string() {
    let array: ArrayRef = Arc::new(StringViewArray::from(vec!["same"; 100]));
    let decoded = round_trip(array.as_ref(), &BqlType::String);
    assert_eq!(decoded.as_ref(), array.as_ref());
}

/// A larger cardinality sample exercises a wider `code_bit_width`:
/// 50 distinct values needs 6 bits per code.
#[test]
fn dictionary_round_trip_many_distinct_strings() {
    let values: Vec<String> = (0..50).map(|i| format!("event_{i}")).collect();
    let array: ArrayRef = Arc::new(StringViewArray::from(values.clone()));
    let decoded = round_trip(array.as_ref(), &BqlType::String);
    let decoded_view = decoded
        .as_any()
        .downcast_ref::<StringViewArray>()
        .expect("Dictionary::decode(String) returns StringViewArray");
    let decoded_values: Vec<String> = (0..decoded_view.len())
        .map(|i| decoded_view.value(i).to_string())
        .collect();
    assert_eq!(decoded_values, values);
}
