//! Property tests for `bqlite_storage::encoding::Fsst`.
//!
//! Follows the RLE / Dictionary template: round-trip is the
//! load-bearing invariant, plus FSST-specific invariants
//! (params serialization round-trip, compression never overflows u16).
//!
//! FSST is String-only, so this module tests the `StringViewArray`
//! code path using the shared dense-array strategy from
//! `bqlite_tests::strategies`, plus a custom strategy for longer,
//! URL-like strings that exercise FSST's substring compression more
//! realistically.
//!
//! See `docs/design/storage/advanced-encodings.md` §7 for the
//! byte-level contract this test guards, and `segment-format-v2.md`
//! §5.3 for the on-disk parameter block layout.

use arrow::array::{Array, ArrayRef, StringViewArray};
use bqlite_core::BqlType;
use bqlite_storage::{Encoding, Fsst};
use bqlite_tests::strategies::arb_string_array;
use proptest::prelude::*;
use std::sync::Arc;

// ── Custom strategies ────────────────────────────────────────────────────────

/// Strategy for URL-like strings that exercise FSST's substring
/// compression. These have shared prefixes and common substrings
/// (`https://`, `.com/`, `/page`), which is the target use case.
fn arb_url_like_array() -> impl Strategy<Value = ArrayRef> {
    let prefixes = prop_oneof![
        Just("https://example.com/"),
        Just("https://api.example.org/v2/"),
        Just("http://cdn.test.io/assets/"),
        Just("https://www.example.net/"),
    ];
    let suffixes = prop::collection::vec("[a-z0-9/._-]{0,40}", 1..=256);
    (prefixes, suffixes).prop_map(|(prefix, suffixes)| {
        let strings: Vec<String> = suffixes
            .into_iter()
            .map(|suffix| format!("{prefix}{suffix}"))
            .collect();
        Arc::new(StringViewArray::from(
            strings.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        )) as ArrayRef
    })
}

/// Strategy for arrays of empty strings — exercises the degenerate
/// case where the symbol table is empty.
fn arb_empty_string_array() -> impl Strategy<Value = ArrayRef> {
    (1..=128_usize).prop_map(|n| {
        let strings: Vec<&str> = vec![""; n];
        Arc::new(StringViewArray::from(strings)) as ArrayRef
    })
}

/// Strategy for arrays with a mix of short and medium strings.
fn arb_mixed_string_array() -> impl Strategy<Value = ArrayRef> {
    prop::collection::vec(
        prop_oneof![
            "[a-z]{0,3}",                                  // short
            "[a-zA-Z0-9_]{5,30}",                          // medium
            "user_agent: Mozilla/5\\.0 \\([a-z]{3,10}\\)", // structured
        ],
        1..=256,
    )
    .prop_map(|v| Arc::new(StringViewArray::from(v)) as ArrayRef)
}

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Train on the array, encode, then decode and return the decoded array.
fn round_trip(array: &dyn Array) -> ArrayRef {
    let fsst = Fsst::train(array).expect("Fsst::train must succeed on any String array");
    let chunk = fsst
        .encode(array)
        .expect("Fsst::encode must accept every dense String array");
    fsst.decode(&chunk, &BqlType::String)
        .expect("Fsst::decode must succeed for a chunk Fsst produced")
}

/// Train, encode, then decode from params only (cross-decode).
fn round_trip_from_params(array: &dyn Array) -> ArrayRef {
    let fsst = Fsst::train(array).expect("Fsst::train must succeed on any String array");
    let chunk = fsst
        .encode(array)
        .expect("Fsst::encode must accept every dense String array");
    // Reconstruct decoder from params only.
    let decoder =
        Fsst::from_params(&chunk.params).expect("from_params must succeed for valid params");
    decoder
        .decode(&chunk, &BqlType::String)
        .expect("cross-decode must succeed")
}

proptest! {
    // ── Round-trip invariants ─────────────────────────────────────────

    /// Round-trip invariant on short ASCII strings.
    #[test]
    fn fsst_round_trip_short_strings(array in arb_string_array()) {
        let decoded = round_trip(array.as_ref());
        prop_assert_eq!(decoded.as_ref(), array.as_ref());
    }

    /// Round-trip invariant on URL-like strings — the primary use case.
    #[test]
    fn fsst_round_trip_url_like(array in arb_url_like_array()) {
        let decoded = round_trip(array.as_ref());
        prop_assert_eq!(decoded.as_ref(), array.as_ref());
    }

    /// Round-trip invariant on empty string arrays.
    #[test]
    fn fsst_round_trip_empty_strings(array in arb_empty_string_array()) {
        let decoded = round_trip(array.as_ref());
        prop_assert_eq!(decoded.as_ref(), array.as_ref());
    }

    /// Round-trip invariant on mixed-length strings.
    #[test]
    fn fsst_round_trip_mixed_strings(array in arb_mixed_string_array()) {
        let decoded = round_trip(array.as_ref());
        prop_assert_eq!(decoded.as_ref(), array.as_ref());
    }

    // ── Cross-decode from params ─────────────────────────────────────
    //
    // The self-contained params encode the symbol table. This invariant
    // verifies that a decoder reconstructed from params produces the
    // same output as the original encoder — simulating what the segment
    // reader does.

    /// Cross-decode from params on short strings.
    #[test]
    fn fsst_cross_decode_short_strings(array in arb_string_array()) {
        let decoded = round_trip_from_params(array.as_ref());
        prop_assert_eq!(decoded.as_ref(), array.as_ref());
    }

    /// Cross-decode from params on URL-like strings.
    #[test]
    fn fsst_cross_decode_url_like(array in arb_url_like_array()) {
        let decoded = round_trip_from_params(array.as_ref());
        prop_assert_eq!(decoded.as_ref(), array.as_ref());
    }

    // ── row_count preservation ────────────────────────────────────────

    /// `chunk.row_count == array.len()` for short strings.
    #[test]
    fn fsst_row_count_preserved_short(array in arb_string_array()) {
        let fsst = Fsst::train(array.as_ref()).unwrap();
        let chunk = fsst.encode(array.as_ref()).unwrap();
        prop_assert_eq!(chunk.row_count, array.len());
    }

    /// `chunk.row_count == array.len()` for URL-like strings.
    #[test]
    fn fsst_row_count_preserved_url(array in arb_url_like_array()) {
        let fsst = Fsst::train(array.as_ref()).unwrap();
        let chunk = fsst.encode(array.as_ref()).unwrap();
        prop_assert_eq!(chunk.row_count, array.len());
    }

    // ── Compression ratio sanity ─────────────────────────────────────
    //
    // FSST's worst case is 2× expansion (every byte escaped). This
    // invariant ensures the payload never exceeds that bound. The
    // per-string u16 compressed-length prefix is counted.

    /// Payload size never exceeds 2 * (2 + original_len) per string.
    #[test]
    fn fsst_payload_within_worst_case_bound(array in arb_string_array()) {
        let fsst = Fsst::train(array.as_ref()).unwrap();
        let chunk = fsst.encode(array.as_ref()).unwrap();

        let view = array.as_any().downcast_ref::<StringViewArray>().unwrap();
        let total_input_bytes: usize = (0..view.len()).map(|i| view.value(i).len()).sum();
        // Worst case: 2 bytes per string (u16 prefix) + 2× each byte (escape + literal)
        let worst_case = view.len() * 2 + total_input_bytes * 2;
        prop_assert!(
            chunk.payload.len() <= worst_case,
            "payload {} exceeds worst-case bound {} for {} strings totaling {} bytes",
            chunk.payload.len(), worst_case, view.len(), total_input_bytes
        );
    }

    // ── estimate_size is an upper bound ──────────────────────────────

    /// `estimate_size >= encode().payload.len()` — the estimate must be
    /// a conservative upper bound.
    #[test]
    fn fsst_estimate_is_upper_bound(array in arb_string_array()) {
        let fsst = Fsst::train(array.as_ref()).unwrap();
        let estimated = fsst.estimate_size(array.as_ref()).unwrap();
        let actual = fsst.encode(array.as_ref()).unwrap().payload.len();
        prop_assert!(
            estimated >= actual,
            "estimate {} < actual {} — estimate_size must be a conservative upper bound",
            estimated, actual
        );
    }

    /// estimate_size upper bound on URL-like strings.
    #[test]
    fn fsst_estimate_is_upper_bound_url(array in arb_url_like_array()) {
        let fsst = Fsst::train(array.as_ref()).unwrap();
        let estimated = fsst.estimate_size(array.as_ref()).unwrap();
        let actual = fsst.encode(array.as_ref()).unwrap().payload.len();
        prop_assert!(
            estimated >= actual,
            "estimate {} < actual {} — estimate_size must be a conservative upper bound",
            estimated, actual
        );
    }
}

// ── Example tests covering edge cases proptest might miss ────────────────────

/// Empty array round-trips as zero-length.
#[test]
fn fsst_round_trip_empty_array() {
    let array: ArrayRef = Arc::new(StringViewArray::from(Vec::<&str>::new()));
    let decoded = round_trip(array.as_ref());
    assert_eq!(decoded.len(), 0);
}

/// Single empty string.
#[test]
fn fsst_round_trip_single_empty_string() {
    let array: ArrayRef = Arc::new(StringViewArray::from(vec![""]));
    let decoded = round_trip(array.as_ref());
    assert_eq!(decoded.as_ref(), array.as_ref());
}

/// Strings shorter than any possible symbol (single characters).
#[test]
fn fsst_round_trip_single_char_strings() {
    let strings: Vec<String> = (b'a'..=b'z').map(|c| String::from(c as char)).collect();
    let array: ArrayRef = Arc::new(StringViewArray::from(
        strings.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
    ));
    let decoded = round_trip(array.as_ref());
    assert_eq!(decoded.as_ref(), array.as_ref());
}

/// Strings with repeated substrings (high compression opportunity).
#[test]
fn fsst_round_trip_highly_compressible() {
    let strings: Vec<String> = (0..100)
        .map(|i| format!("https://example.com/page/{i}/details?ref=home&lang=en"))
        .collect();
    let array: ArrayRef = Arc::new(StringViewArray::from(
        strings.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
    ));
    let fsst = Fsst::train(array.as_ref()).unwrap();
    let chunk = fsst.encode(array.as_ref()).unwrap();
    let decoded = fsst.decode(&chunk, &BqlType::String).unwrap();
    assert_eq!(decoded.as_ref(), array.as_ref());

    // Verify actual compression happened (not just 2× expansion).
    let total_input_bytes: usize = strings.iter().map(|s| s.len()).sum();
    let payload_overhead = strings.len() * 2; // u16 per string
    let compressed_data_bytes = chunk.payload.len() - payload_overhead;
    assert!(
        compressed_data_bytes < total_input_bytes,
        "FSST should compress URL-like data: {} compressed vs {} original",
        compressed_data_bytes,
        total_input_bytes
    );
}

/// Strings with no beneficial symbols (random bytes) — FSST should
/// still round-trip correctly even though it doesn't compress well.
#[test]
fn fsst_round_trip_no_beneficial_symbols() {
    // Each string is unique random hex — virtually no shared substrings.
    let strings: Vec<String> = (0..50)
        .map(|i| format!("{:016x}{:016x}", i * 7919 + 1, i * 6271 + 3))
        .collect();
    let array: ArrayRef = Arc::new(StringViewArray::from(
        strings.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
    ));
    let decoded = round_trip(array.as_ref());
    assert_eq!(decoded.as_ref(), array.as_ref());
}

/// Multi-byte UTF-8 characters round-trip correctly.
#[test]
fn fsst_round_trip_utf8_multibyte() {
    let strings = vec![
        "こんにちは世界",
        "Ünïcödé",
        "emoji: 🎉🎊",
        "mixed: abc日本語def",
        "",
        "ASCII only",
    ];
    let array: ArrayRef = Arc::new(StringViewArray::from(strings));
    let decoded = round_trip(array.as_ref());
    assert_eq!(decoded.as_ref(), array.as_ref());
}

/// Cross-decode from params with multi-byte UTF-8.
#[test]
fn fsst_cross_decode_utf8_multibyte() {
    let strings = vec!["café", "naïve", "résumé", "über", "日本語"];
    let array: ArrayRef = Arc::new(StringViewArray::from(strings));
    let decoded = round_trip_from_params(array.as_ref());
    assert_eq!(decoded.as_ref(), array.as_ref());
}
