//! Property tests for `bqlite_storage::encoding::DoubleDelta`.
//!
//! Follows the Delta template: round-trip is the load-bearing invariant,
//! plus encoding-specific properties for second-order delta behavior.
//!
//! DoubleDelta only applies to `BqlType::Int` and `BqlType::Timestamp`,
//! identical to Delta. Inputs are generated with bounded consecutive deltas
//! (so both first-order and second-order deltas fit in `i64`) and with
//! near-constant-interval structure (DoubleDelta's sweet spot).
//!
//! See `docs/design/storage/advanced-encodings.md` §4 and
//! `docs/design/storage/segment-format-v2.md` §4 for the spec.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, TimestampNanosecondArray};
use bqlite_core::BqlType;
use bqlite_storage::{DoubleDelta, Encoding};
use proptest::prelude::*;

/// Non-empty dense `Int64Array` strategy whose consecutive first-order
/// and second-order deltas are guaranteed to fit in `i64`.
///
/// Uses the same bounded-delta construction as Delta's strategy, which
/// ensures both first-order deltas and second-order deltas remain in the
/// valid range. With `|delta| ≤ i64::MAX / 64`, any run of up to 64
/// elements has cumulative values that fit in i64, and second-order
/// deltas (differences of consecutive first-order deltas) are bounded
/// by `2 × DELTA_BOUND / 64` which fits well within i64.
fn arb_int64_nonempty_array() -> impl Strategy<Value = ArrayRef> {
    const DELTA_BOUND: i64 = i64::MAX / 64;
    (
        any::<i64>(),
        prop::collection::vec(-DELTA_BOUND..=DELTA_BOUND, 0..=31),
    )
        .prop_map(|(base, deltas)| {
            let mut values = Vec::with_capacity(deltas.len() + 1);
            values.push(base);
            let mut current = base;
            for d in deltas {
                current = current.saturating_add(d);
                values.push(current);
            }
            Arc::new(Int64Array::from(values)) as ArrayRef
        })
}

/// Non-empty dense `TimestampNanosecondArray` strategy with the canonical
/// `"UTC"` timezone stamp. Same bounded-delta construction as
/// [`arb_int64_nonempty_array`].
fn arb_timestamp_nonempty_array() -> impl Strategy<Value = ArrayRef> {
    const DELTA_BOUND: i64 = i64::MAX / 64;
    (
        (i64::MIN + 1)..i64::MAX,
        prop::collection::vec(-DELTA_BOUND..=DELTA_BOUND, 0..=31),
    )
        .prop_map(|(base, deltas)| {
            let mut values = Vec::with_capacity(deltas.len() + 1);
            values.push(base);
            let mut current = base;
            for d in deltas {
                current = current.saturating_add(d);
                values.push(current);
            }
            Arc::new(TimestampNanosecondArray::from(values).with_timezone("UTC")) as ArrayRef
        })
}

/// Near-constant-interval timestamp strategy — DoubleDelta's sweet spot.
///
/// Generates `len` timestamps at a constant `step` with small `jitter`
/// on each value. The first-order deltas are approximately equal to
/// `step`, so second-order deltas (jitter differences) are tiny and
/// DoubleDelta compresses them to a narrow bit width.
fn arb_near_constant_interval_timestamp_array() -> impl Strategy<Value = ArrayRef> {
    (
        // Base in a range with headroom for the full sequence.
        (1_700_000_000_000_000_000_i64 - 1_000_000_000_000)
            ..(1_700_000_000_000_000_000_i64 + 1_000_000_000_000),
        // Constant step: 1 µs to 1 s in nanoseconds.
        1_000_i64..=1_000_000_000_i64,
        // Per-step jitter: at most ±(step / 100), so jitter << step.
        1_usize..=32,
        any::<i64>(),
    )
        .prop_flat_map(|(base, step, len, seed)| {
            // Generate `len` jitter values bounded by step/100 each.
            let jitter_bound = (step / 100).max(1);
            let jitter_strat = prop::collection::vec(-jitter_bound..=jitter_bound, len..=len);
            Just((base, step, seed % 1000)).prop_flat_map(move |(base, step, _)| {
                jitter_strat.clone().prop_map(move |jitters| {
                    let values: Vec<i64> = jitters
                        .iter()
                        .enumerate()
                        .map(|(i, &j)| base + i as i64 * step + j)
                        .collect();
                    Arc::new(TimestampNanosecondArray::from(values).with_timezone("UTC"))
                        as ArrayRef
                })
            })
        })
}

/// Non-empty `Int64Array` strategy for the "strictly monotonic" case
/// (constant Δ, all double-deltas == 0). This exercises the
/// `dd_bit_width == 1` (floor) path.
fn arb_strictly_monotonic_int_array() -> impl Strategy<Value = ArrayRef> {
    (any::<i64>(), 1_i64..=1_000_000_i64, 1_usize..=32_usize).prop_map(|(base, step, len)| {
        let values: Vec<i64> = (0..len as i64)
            .map(|i| base.saturating_add(step.saturating_mul(i)))
            .collect();
        Arc::new(Int64Array::from(values)) as ArrayRef
    })
}

/// Helper: run one value through `encode → decode` and return the decoded
/// array.
fn round_trip(array: &dyn arrow::array::Array, ty: &BqlType) -> ArrayRef {
    let chunk = DoubleDelta
        .encode(array)
        .expect("DoubleDelta::encode must accept every dense Int/Timestamp array");
    DoubleDelta
        .decode(&chunk, ty)
        .expect("DoubleDelta::decode must succeed for a chunk DoubleDelta produced")
}

proptest! {
    /// Round-trip invariant on `BqlType::Int` under DoubleDelta-safe inputs.
    ///
    /// The bounded-delta strategy guarantees every consecutive first-order
    /// and second-order delta fits in `i64`, so the property runs on every
    /// generated case with no rejection filter.
    #[test]
    fn double_delta_round_trip_int(array in arb_int64_nonempty_array()) {
        let decoded = round_trip(array.as_ref(), &BqlType::Int);
        prop_assert_eq!(decoded.as_ref(), array.as_ref());
    }

    /// Round-trip invariant on `BqlType::Timestamp`. Also asserts the
    /// decoded array carries the canonical `"UTC"` timezone stamp.
    #[test]
    fn double_delta_round_trip_timestamp(array in arb_timestamp_nonempty_array()) {
        let decoded = round_trip(array.as_ref(), &BqlType::Timestamp);
        prop_assert_eq!(decoded.as_ref(), array.as_ref());
    }

    /// Round-trip on near-constant-interval timestamps — DoubleDelta's
    /// primary use case. The tiny jitter values produce a narrow dd_bit_width
    /// and a small payload.
    #[test]
    fn double_delta_round_trip_near_constant_interval(
        array in arb_near_constant_interval_timestamp_array(),
    ) {
        let decoded = round_trip(array.as_ref(), &BqlType::Timestamp);
        prop_assert_eq!(decoded.as_ref(), array.as_ref());
    }

    /// Strictly monotonic sequences (constant Δ, all dd == 0) exercise
    /// the floor-width path. The payload is all zeros bit-packed at
    /// `dd_bit_width == 1`.
    #[test]
    fn double_delta_round_trip_strictly_monotonic(array in arb_strictly_monotonic_int_array()) {
        let chunk = DoubleDelta.encode(array.as_ref()).unwrap();
        // All double-deltas are zero → width floors to 1.
        if array.len() >= 3 {
            prop_assert_eq!(chunk.params[16], 1_u8,
                "strictly monotonic input must use dd_bit_width == 1 (all dd == 0)"
            );
        }
        let decoded = DoubleDelta.decode(&chunk, &BqlType::Int).unwrap();
        prop_assert_eq!(decoded.as_ref(), array.as_ref());
    }

    /// `estimate_size` is an exact match against `encode().payload.len()` —
    /// DoubleDelta's payload size is deterministic from the width alone.
    #[test]
    fn double_delta_estimate_size_is_exact(array in arb_int64_nonempty_array()) {
        let estimated = DoubleDelta.estimate_size(array.as_ref()).unwrap();
        let actual = DoubleDelta.encode(array.as_ref()).unwrap().payload.len();
        prop_assert_eq!(estimated, actual);
    }

    /// Encoded payload is always a multiple of 8 bytes (one SIMD lane).
    /// Single-element and two-element arrays have a zero-byte payload,
    /// which still satisfies the "multiple of 8" invariant.
    #[test]
    fn double_delta_payload_is_simd_aligned(array in arb_int64_nonempty_array()) {
        let chunk = DoubleDelta.encode(array.as_ref()).unwrap();
        prop_assert_eq!(chunk.payload.len() % 8, 0);
    }

    /// `chunk.row_count == array.len()` — DoubleDelta preserves the row
    /// count unchanged.
    #[test]
    fn double_delta_row_count_preserved(array in arb_int64_nonempty_array()) {
        let chunk = DoubleDelta.encode(array.as_ref()).unwrap();
        prop_assert_eq!(chunk.row_count, array.len());
    }

    /// For arrays of 0 or 1 or 2 elements, the payload is empty because
    /// there are no double-delta values to pack. For row_count >= 3, the
    /// payload is non-empty (at least one dd value at minimum 1-bit width,
    /// padded to 8 bytes).
    #[test]
    fn double_delta_payload_empty_iff_row_count_leq_2(array in arb_int64_nonempty_array()) {
        let chunk = DoubleDelta.encode(array.as_ref()).unwrap();
        if array.len() <= 2 {
            prop_assert_eq!(chunk.payload.len(), 0,
                "row_count <= 2 must produce empty payload");
        } else {
            prop_assert!(
                !chunk.payload.is_empty(),
                "row_count > 2 must produce non-empty payload"
            );
        }
    }

    /// DoubleDelta's payload is always ≤ Delta's payload on the same input —
    /// it must never expand data relative to first-order delta. This is a
    /// regression guard for the claim in `advanced-encodings.md` §4.7.
    ///
    /// The property holds specifically for near-constant-interval inputs
    /// where DoubleDelta's core advantage is non-zero. For completely random
    /// inputs DoubleDelta may tie (not beat) Delta.
    #[test]
    fn double_delta_payload_not_larger_than_delta_on_near_constant_intervals(
        array in arb_near_constant_interval_timestamp_array(),
    ) {
        use bqlite_storage::Delta;
        let dd_size = DoubleDelta.estimate_size(array.as_ref()).unwrap_or(usize::MAX);
        let delta_size = Delta.estimate_size(array.as_ref()).unwrap_or(usize::MAX);
        prop_assert!(
            dd_size <= delta_size,
            "DoubleDelta ({} bytes) must not exceed Delta ({} bytes) on near-constant intervals",
            dd_size,
            delta_size,
        );
    }
}

// ── Example tests covering edge cases proptest might miss ───────────────

/// Single-element array: `base_value` in params, `first_delta = 0`,
/// empty payload.
#[test]
fn double_delta_round_trip_single_element() {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![42_i64]));
    let decoded = round_trip(array.as_ref(), &BqlType::Int);
    assert_eq!(decoded.as_ref(), array.as_ref());
}

/// Two-element array: `base_value` + `first_delta` in params, empty payload.
#[test]
fn double_delta_round_trip_two_elements() {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![0_i64, 100]));
    let chunk = DoubleDelta.encode(array.as_ref()).unwrap();
    assert_eq!(
        chunk.payload.len(),
        0,
        "two elements must produce no payload"
    );
    let decoded = DoubleDelta.decode(&chunk, &BqlType::Int).unwrap();
    assert_eq!(decoded.as_ref(), array.as_ref());
}

/// Constant array: all-zero double-deltas, `dd_bit_width` floors to 1.
#[test]
fn double_delta_round_trip_constant_sequence() {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![7_i64; 16]));
    let decoded = round_trip(array.as_ref(), &BqlType::Int);
    assert_eq!(decoded.as_ref(), array.as_ref());
}

/// Empty arrays are explicitly rejected.
#[test]
fn double_delta_rejects_empty_array() {
    let array: ArrayRef = Arc::new(Int64Array::from(Vec::<i64>::new()));
    let err = DoubleDelta.encode(array.as_ref()).unwrap_err();
    assert!(matches!(err, bqlite_core::BqliteError::Execution(_)));
}

/// Overflow in the first_delta (values[1] - values[0] > i64::MAX) is
/// detected and returned as a typed error.
#[test]
fn double_delta_rejects_overflow_first_delta() {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![i64::MIN, i64::MAX]));
    let err = DoubleDelta.encode(array.as_ref()).unwrap_err();
    assert!(matches!(err, bqlite_core::BqliteError::Execution(_)));
}
