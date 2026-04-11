//! Property tests for `bqlite_storage::encoding::Delta`.
//!
//! Follows the Plain template: round-trip is the load-bearing
//! invariant, plus a handful of encoding-specific edges.
//!
//! Delta only claims `BqlType::Int` and `BqlType::Timestamp`, so the
//! universe is narrow compared to Plain's "every primitive" strategy.
//! Inputs are generated via the shared strategies from
//! `bqlite_tests::strategies`, plus a small local strategy for
//! non-empty arrays — the spec rules out `row_count == 0` for Delta
//! so the strategy must not ever produce an empty array.
//!
//! See `docs/design/storage/segment-format-v1.md` §9.3 for the
//! byte-level contract this test guards.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, TimestampNanosecondArray};
use bqlite_core::BqlType;
use bqlite_storage::{Delta, Encoding};
use proptest::prelude::*;

/// Non-empty dense `Int64Array` strategy whose consecutive residuals
/// are guaranteed to fit in `i64`.
///
/// A naive `vec(any::<i64>())` strategy rejects most draws — a random
/// pair of `i64` values has a `~50%` chance of a residual outside
/// `i64` range (the residual is computed in `i128` but must round-trip
/// through `i64` per spec §9.3). Proptest's global-reject ceiling
/// (1024) is hit quickly and the test fails as "too many rejects"
/// rather than as a real counter-example.
///
/// Instead, build an array from a base value and a sequence of
/// bounded `i64` deltas (each `|delta| ≤ i64::MAX / 32`, which keeps
/// any 32-element series within the Delta-safe domain). This covers
/// the same domain Delta actually sees in practice: monotonic-ish
/// runs with bounded deltas.
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
                // Saturating keeps the base near i64 extrema from
                // wrapping — the resulting values are still covered
                // by Delta's domain, and residuals remain bounded.
                current = current.saturating_add(d);
                values.push(current);
            }
            Arc::new(Int64Array::from(values)) as ArrayRef
        })
}

/// Non-empty dense `TimestampNanosecondArray` strategy with the
/// canonical `"UTC"` timezone stamp. Same bounded-delta construction
/// as [`arb_int64_nonempty_array`] — Timestamps are i64 at the Arrow
/// level, so the Delta-safe constraints are identical.
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

/// Happy-case strategy for Delta: a monotonic-ish run of timestamps
/// whose consecutive residuals stay small. This is the workload
/// Delta is optimised for (nanosecond timestamps within an entity
/// advance by millisecond-ish deltas), and exercising it here
/// documents the common case alongside the "full i64 domain"
/// property.
///
/// The generator picks a `base` in the Timestamp-safe range, a
/// constant positive `step` in `[1, 1_000_000_000]` (up to one
/// second), and builds `len` values as `base + i * step`. Because
/// the residuals are identical, every width-selection path sees a
/// tight packing and the test shrinks to the smallest width that
/// still holds `step`.
fn arb_monotonic_timestamp_array() -> impl Strategy<Value = ArrayRef> {
    (
        // Leave headroom at the top so `base + len * step` never
        // overflows even for the largest generated step.
        (i64::MIN + 1_000_000_000_000)..(i64::MAX - 1_000_000_000_000),
        1_i64..=1_000_000_000,
        1_usize..=32,
    )
        .prop_map(|(base, step, len)| {
            let values: Vec<i64> = (0..len as i64).map(|i| base + i * step).collect();
            Arc::new(TimestampNanosecondArray::from(values).with_timezone("UTC")) as ArrayRef
        })
}

/// Helper that runs one value through `encode → decode` and returns
/// the decoded array. Keeps the encode/decode calls consistent
/// across properties.
fn round_trip(array: &dyn arrow::array::Array, ty: &BqlType) -> ArrayRef {
    let chunk = Delta
        .encode(array)
        .expect("Delta::encode must accept every dense Int/Timestamp array");
    Delta
        .decode(&chunk, ty)
        .expect("Delta::decode must succeed for a chunk Delta produced")
}

proptest! {
    /// Round-trip invariant on `BqlType::Int` under Delta-safe inputs.
    ///
    /// The bounded-delta strategy guarantees every consecutive
    /// residual fits in `i64`, so the property runs on every
    /// generated case with no rejection filter. Shrinks on this
    /// property reliably point at bit-pack / zigzag bugs because
    /// both paths touch every residual.
    #[test]
    fn delta_round_trip_int(array in arb_int64_nonempty_array()) {
        let decoded = round_trip(array.as_ref(), &BqlType::Int);
        prop_assert_eq!(decoded.as_ref(), array.as_ref());
    }

    /// Round-trip invariant on `BqlType::Timestamp`. Also asserts
    /// that the decoded array carries the canonical `"UTC"` timezone
    /// stamp — Arrow's `PartialEq` on `TimestampNanosecondArray`
    /// includes the timezone, so a missing `with_timezone("UTC")` on
    /// the decode side fails this test.
    #[test]
    fn delta_round_trip_timestamp(array in arb_timestamp_nonempty_array()) {
        let decoded = round_trip(array.as_ref(), &BqlType::Timestamp);
        prop_assert_eq!(decoded.as_ref(), array.as_ref());
    }

    /// Happy-case round-trip on a strictly monotonic timestamp run
    /// — the workload Delta is tuned for. Every generated case has
    /// an identical step so the bit-width selection collapses to the
    /// minimum that holds `step`.
    #[test]
    fn delta_round_trip_monotonic_timestamp_run(
        array in arb_monotonic_timestamp_array(),
    ) {
        let decoded = round_trip(array.as_ref(), &BqlType::Timestamp);
        prop_assert_eq!(decoded.as_ref(), array.as_ref());
    }

    /// `estimate_size` is an exact match against `encode().payload.len()`
    /// for Delta — the bit-pack output size is deterministic once the
    /// residual bit width is chosen. Later encodings whose estimator
    /// returns an upper bound can switch this to
    /// `prop_assert!(estimate >= actual)`.
    #[test]
    fn delta_estimate_size_is_exact(array in arb_int64_nonempty_array()) {
        let estimated = Delta.estimate_size(array.as_ref()).unwrap();
        let actual = Delta.encode(array.as_ref()).unwrap().payload.len();
        prop_assert_eq!(estimated, actual);
    }

    /// Encoded payload is always a multiple of 8 bytes (one SIMD
    /// lane) — the spec pads the residual stream so a FastLanes
    /// unpacker can read one full lane past the last residual.
    /// Single-element arrays have an empty (zero-byte) payload,
    /// which still satisfies the "multiple of 8" invariant.
    #[test]
    fn delta_payload_is_simd_aligned(array in arb_int64_nonempty_array()) {
        let chunk = Delta.encode(array.as_ref()).unwrap();
        prop_assert_eq!(chunk.payload.len() % 8, 0);
    }

    /// `chunk.row_count == array.len()` — Delta preserves the row
    /// count unchanged, same as every other v1 encoding.
    #[test]
    fn delta_row_count_preserved(array in arb_int64_nonempty_array()) {
        let chunk = Delta.encode(array.as_ref()).unwrap();
        prop_assert_eq!(chunk.row_count, array.len());
    }
}

// ── Example tests covering edge cases proptest might miss ───────────────

/// A single-element `Int64Array` round-trips via `base_value` only —
/// the residual stream is zero bytes plus SIMD padding. Proptest hits
/// this often but an explicit regression test documents the edge.
#[test]
fn delta_round_trip_int_single_element() {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![42_i64]));
    let decoded = round_trip(array.as_ref(), &BqlType::Int);
    assert_eq!(decoded.as_ref(), array.as_ref());
}

/// A constant-valued series produces all-zero residuals.
#[test]
fn delta_round_trip_int_constant() {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![-7_i64; 10]));
    let decoded = round_trip(array.as_ref(), &BqlType::Int);
    assert_eq!(decoded.as_ref(), array.as_ref());
}

/// A strictly descending run produces uniformly negative residuals;
/// zigzag maps them to small positives and the width collapses.
#[test]
fn delta_round_trip_int_descending() {
    let values: Vec<i64> = (0..16).rev().collect();
    let array: ArrayRef = Arc::new(Int64Array::from(values));
    let decoded = round_trip(array.as_ref(), &BqlType::Int);
    assert_eq!(decoded.as_ref(), array.as_ref());
}

/// Empty arrays are explicitly rejected — Delta's `row_count == 0`
/// case is illegal per the spec.
#[test]
fn delta_rejects_empty_array() {
    let array: ArrayRef = Arc::new(Int64Array::from(Vec::<i64>::new()));
    let err = Delta.encode(array.as_ref()).unwrap_err();
    assert!(matches!(err, bqlite_core::BqliteError::Execution(_)));
}
