//! Encoding selector heuristic for the v1 segment format.
//!
//! Given a dense Arrow column chunk and its [`BqlType`], the selector
//! picks the v1 encoding that produces the smallest payload, then
//! decides whether to wrap that payload in LZ4. The decision flow is:
//!
//! 1. **Empty arrays** → [`Plain`]. The other v1 encodings either
//!    error on empty input ([`Constant`], [`Delta`]) or have nothing
//!    to encode and gain nothing over Plain.
//! 2. **Uniform arrays** (all values bit-exactly equal) → [`Constant`].
//!    Constant emits a zero-byte payload regardless of `row_count`,
//!    which is unbeatable on size and decode cost. The uniformity check
//!    is done first so the heuristic doesn't waste estimator calls on
//!    chunks that have a clear winner.
//! 3. **Otherwise**, score every applicable encoding's
//!    [`Encoding::estimate_size`] and pick the smallest. Ties are
//!    broken by [`decode_cost`], so equal-bytes encodings collapse
//!    to whichever is fastest to decode.
//! 4. **LZ4 wrapper** is applied iff `compressed.len() ≤ 0.9 *
//!    payload.len()` per `storage-format.md` §10.7. The selector runs
//!    one [`compress_lz4`] call on the chosen encoding's payload to
//!    measure the ratio exactly — sampling-based selection is a
//!    Wave 4+ optimization (see §10.3 closing paragraph).
//!
//! Note: `storage-format.md` §10.3 sketches a phase-based "first
//! matching rule" pipeline using statistics like sortedness and
//! cardinality ratio. v1 implements the score-all-applicable
//! variant the closing paragraph of §10.3 describes ("a future
//! version can adopt BtrBlocks-style sampling selection… pick the
//! winner by estimated compression ratio"). The two are functionally
//! equivalent over the v1 encoding set; the score-based form is
//! easier to extend in Wave 4 when new encodings land.
//!
//! # Plain is the universal fallback
//!
//! Every primitive [`BqlType`] (`Bool`, `Int`, `Float`, `String`,
//! `Timestamp`) has [`Plain::applicable_to`] returning `true`, so the
//! candidate set is never empty for primitives. Nested types
//! (`List`, `Map`) have no v1 encoding at all — `segment-format-v1.md`
//! §9.5 documents that the selector "falls through to Plain" for
//! them, which in v1 surfaces as a `Plain::encode` error since the
//! Plain implementation hasn't yet wired up nested encoding. This
//! matches the deferred status of nested column ingest in Wave 2.
//!
//! # Self-contained Dictionary chunks
//!
//! `Dictionary::encode` produces a self-contained [`EncodedChunk`]
//! whose `params` block inlines the dictionary values, **not** the
//! `dict_id + code_bit_width` shape that ends up on disk per
//! §9.2. The selector treats Dictionary chunks the same as any other
//! encoding's output for size comparison; the writer orchestration
//! (TASK-214) is responsible for hoisting the inlined dictionary to
//! the segment dictionaries region and rewriting the params before
//! the chunk hits disk. See `dictionary.rs` module docs for the
//! hoist contract.

use arrow::array::{
    Array, BooleanArray, Float64Array, Int64Array, StringArray, StringViewArray,
    TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use bqlite_core::{BqlType, BqliteError, Result};

use super::{
    compress_lz4, require_dense, BitPacking, CompressionType, Constant, Delta, Dictionary,
    DoubleDelta, EncodedChunk, Encoding, EncodingType, Plain, Rle,
};

/// LZ4 minimum compression threshold per `storage-format.md` §10.7
/// and `segment-format-v1.md` §8: LZ4 wraps the payload only if
/// the compressed bytes are at most 90% of the uncompressed bytes
/// (i.e. ≥10% savings). Below the threshold, the writer emits the
/// payload uncompressed and records `compression = None`.
pub const LZ4_RATIO_THRESHOLD: f64 = 0.9;

/// Outcome of [`select_encoding`].
///
/// Carries the chosen encoding's full output so the writer
/// (TASK-214) does not have to re-encode after the selection
/// decision is made:
///
/// - [`Self::chunk`] is the encoded chunk produced by the chosen
///   encoding's `encode` method. The chunk's `payload` field is the
///   **uncompressed** payload bytes — what the encoding's decoder
///   needs to reconstruct the original array.
/// - [`Self::compression`] is the LZ4 decision the selector made.
/// - [`Self::on_disk_payload`] is the bytes the writer should emit
///   into the column chunk's payload region: equal to
///   `chunk.payload` when `compression == None`, or the LZ4-compressed
///   form of it when `compression == Lz4`. Either way, the column
///   chunk header records `chunk.payload.len()` as
///   `uncompressed_payload_length` so the reader can size its
///   decompression buffer.
#[derive(Debug, Clone)]
pub struct SelectedEncoding {
    /// The chosen encoding's full output. `chunk.encoding` identifies
    /// which encoding won the selection, and `chunk.payload` is the
    /// uncompressed payload bytes.
    pub chunk: EncodedChunk,
    /// LZ4 decision: `Lz4` if the threshold check fired, `None` otherwise.
    pub compression: CompressionType,
    /// Bytes to serialize into the column chunk's payload region.
    /// Equal to `chunk.payload` if `compression == None`, otherwise
    /// the LZ4-compressed form of `chunk.payload`.
    pub on_disk_payload: Vec<u8>,
}

impl SelectedEncoding {
    /// Convenience: which encoding the selector picked.
    pub fn encoding(&self) -> EncodingType {
        self.chunk.encoding
    }
}

/// Pick the best v1 encoding for `array` and decide whether to apply
/// the LZ4 wrapper.
///
/// `array` must be **dense** — the selector only operates on the
/// non-null values; null handling is the writer orchestration's job
/// per the [`Encoding`] trait contract. A nullable input is rejected
/// with [`BqliteError::Execution`] before any encoding work happens.
///
/// Returns a [`SelectedEncoding`] carrying the encoded chunk, the
/// compression decision, and the on-disk payload bytes ready for
/// the writer to serialize. The writer's only remaining job for the
/// payload region is to emit `on_disk_payload` verbatim.
pub fn select_encoding(array: &dyn Array, ty: &BqlType) -> Result<SelectedEncoding> {
    require_dense(array, "select_encoding")?;
    let chosen = pick_encoding(array, ty)?;
    let chunk = encode_with(chosen, array)?;
    let (compression, on_disk_payload) = pick_compression(&chunk);
    Ok(SelectedEncoding {
        chunk,
        compression,
        on_disk_payload,
    })
}

/// Pick the encoding type without running the full encode. Useful for
/// tests that want to verify the decision without paying for the
/// encode + compress passes; the writer should use [`select_encoding`]
/// to avoid double work.
pub fn select_encoding_type(array: &dyn Array, ty: &BqlType) -> Result<EncodingType> {
    require_dense(array, "select_encoding_type")?;
    pick_encoding(array, ty)
}

// ── core heuristic ───────────────────────────────────────────────────────────

fn pick_encoding(array: &dyn Array, ty: &BqlType) -> Result<EncodingType> {
    let row_count = array.len();

    // Phase 1: empty arrays. Plain handles them; Constant errors on
    // empty input; Delta errors on row_count == 0; Dictionary handles
    // empty but Plain is the universal fallback the spec calls out.
    if row_count == 0 {
        return Ok(EncodingType::Plain);
    }

    // Phase 2: uniformity check → Constant.
    //
    // Constant's `applicable_to` covers every primitive Plain handles,
    // and Constant's payload is always 0 bytes regardless of cardinality
    // — it cannot be beaten on size or decode cost when the chunk is
    // truly uniform. Doing this check first short-circuits the
    // estimator pass for trivial chunks.
    if Constant.applicable_to(ty) && is_uniform(array) {
        return Ok(EncodingType::Constant);
    }

    // Phase 3: rank applicable encodings by estimated payload size.
    // Tie-break by decode cost (lower is better).
    //
    // The candidate list is the v1 encoding set plus Wave 4 DoubleDelta
    // minus Constant (already resolved in Phase 2). Plain is included so
    // it serves as the universal fallback — every primitive type has at
    // least Plain in the candidate set, so `best` is always populated for
    // primitives.
    //
    // Note: Rle is intentionally omitted here. Its registration —
    // including the §3.7 average-run-length guard from
    // `docs/design/storage/advanced-encodings.md` (only select RLE when
    // `run_count < row_count / 2`) — is owned by TASK-419, which wires
    // all Wave 4 surviving codecs into the selector together. Rle is
    // already wired into `decode_cost`, `encode_with`,
    // `parse_encoding_params_len`, and `dispatch_decode` so TASK-419
    // only needs to append `&Rle` here and implement the guard.
    let candidates: &[&dyn Encoding] = &[&Plain, &Dictionary, &Delta, &DoubleDelta, &BitPacking];
    let mut best: Option<(EncodingType, usize, u8)> = None;
    for enc in candidates {
        if !enc.applicable_to(ty) {
            continue;
        }
        // `estimate_size` may return an error for legitimate reasons
        // (e.g. Delta on a chunk whose i128 residuals overflow i64).
        // Skip the encoding when that happens — Plain remains in the
        // pool as the universal fallback.
        let size = match enc.estimate_size(array) {
            Ok(s) => s,
            Err(_) => continue,
        };
        let cost = decode_cost(enc.encoding_type());
        let candidate = (enc.encoding_type(), size, cost);
        match best {
            None => best = Some(candidate),
            Some((_, best_size, best_cost)) => {
                if size < best_size || (size == best_size && cost < best_cost) {
                    best = Some(candidate);
                }
            }
        }
    }

    // For primitive types, Plain is always applicable so `best` is
    // always populated. For nested types (`List`, `Map`) Plain is *not*
    // applicable in v1 — the selector returns Plain anyway, matching
    // §9.5's "selector falls through to Plain" rule, and the
    // subsequent `encode_with(Plain, ...)` call will surface the
    // deferred-nested-encoding error. This keeps every BqlType in the
    // selector's domain without a special case.
    Ok(best.map(|(e, _, _)| e).unwrap_or(EncodingType::Plain))
}

/// Decode-cost tiebreaker. Lower is faster.
///
/// Approximate ordering by hot-path complexity
/// (per `segment-format-v2.md` §10.2):
///
/// | Encoding    | Cost | Decode hot path                                |
/// |-------------|------|------------------------------------------------|
/// | Constant    | 0    | broadcast single value                         |
/// | Plain       | 1    | memcpy / fixed-stride read                     |
/// | Rle         | 2    | broadcast per run (or zero-copy RunEndEncoded)  |
/// | BitPacking  | 3    | bit-unpack into i64                            |
/// | For         | 4    | per-block bit-unpack + base add                |
/// | Delta       | 5    | bit-unpack + cumulative sum                    |
/// | DoubleDelta | 6    | bit-unpack + 2x cumulative sum                 |
/// | PFor        | 7    | per-block bit-unpack + base add + patch scatter |
/// | Dictionary  | 8    | bit-unpack + per-row dict lookup               |
/// | Alp         | 9    | FOR-unpack mantissas + f64 mul + patch scatter  |
/// | Fsst        | 10   | per-byte symbol lookup                         |
///
/// These are *relative* costs used only as tiebreakers — the absolute
/// values have no meaning beyond the ordering. The selector ranks by
/// payload size first, so this only applies to candidates whose
/// estimated size is identical.
///
/// DoubleDelta (cost 6) is slower than Delta (cost 3) due to the
/// extra prefix-sum pass, per `advanced-encodings.md` §10.3, but is
/// still preferred over Delta when its payload is smaller.
pub const fn decode_cost(enc: EncodingType) -> u8 {
    match enc {
        EncodingType::Constant => 0,
        EncodingType::Plain => 1,
        EncodingType::Rle => 2,
        EncodingType::BitPacking => 3,
        EncodingType::For => 4,
        EncodingType::Delta => 5,
        EncodingType::DoubleDelta => 6,
        EncodingType::PFor => 7,
        EncodingType::Dictionary => 8,
        EncodingType::Alp => 9,
        EncodingType::Fsst => 10,
    }
}

// ── compression decision ─────────────────────────────────────────────────────

/// Compute the LZ4 wrapper decision and the bytes to emit on disk.
///
/// Returns `(compression, on_disk_payload)`. When `compression` is
/// `None`, `on_disk_payload` is a clone of `chunk.payload`. When
/// `compression` is `Lz4`, it is the LZ4-compressed form. Empty
/// payloads always return `None` since there is nothing to compress.
fn pick_compression(chunk: &EncodedChunk) -> (CompressionType, Vec<u8>) {
    if chunk.payload.is_empty() {
        // No bytes to compress — Constant lands here, as does Delta
        // on a single-row chunk.
        return (CompressionType::None, Vec::new());
    }
    let compressed = compress_lz4(&chunk.payload);
    let ratio = compressed.len() as f64 / chunk.payload.len() as f64;
    if ratio <= LZ4_RATIO_THRESHOLD {
        (CompressionType::Lz4, compressed)
    } else {
        (CompressionType::None, chunk.payload.clone())
    }
}

// ── shared encode dispatcher ─────────────────────────────────────────────────

/// Dispatch to a specific encoding's `encode` method by discriminant.
///
/// Used by `select_encoding` after the heuristic picks a winner. The
/// match is exhaustive over the known encoding set so adding a new
/// encoding (Wave 4+) requires updating this function and the
/// `decode_cost` table together.
fn encode_with(encoding: EncodingType, array: &dyn Array) -> Result<EncodedChunk> {
    match encoding {
        EncodingType::Plain => Plain.encode(array),
        EncodingType::Constant => Constant.encode(array),
        EncodingType::Dictionary => Dictionary.encode(array),
        EncodingType::Delta => Delta.encode(array),
        EncodingType::BitPacking => BitPacking.encode(array),
        EncodingType::Rle => Rle.encode(array),
        EncodingType::DoubleDelta => DoubleDelta.encode(array),
        // v2 encodings — implementations land in TASK-415 through TASK-418.
        // The selector never picks these; this arm exists only for exhaustiveness.
        EncodingType::Fsst
        | EncodingType::For
        | EncodingType::PFor
        | EncodingType::Alp => Err(BqliteError::Execution(format!(
            "v2 encoding {encoding:?} encode not yet implemented"
        ))),
    }
}

// ── uniformity check (Constant fast path) ────────────────────────────────────

/// Check whether every value in `array` is bit-exactly equal to the
/// first value. Empty arrays are not uniform (Constant errors on them
/// at encode time, so the selector should not pick Constant).
///
/// The bit-exact comparison matters for floats: `0.0` and `-0.0`
/// compare equal under `f64::PartialEq` but have different bit
/// patterns, and a Constant chunk would lose the sign bit on decode.
/// `f64::to_bits` comparison preserves both — matching the rule
/// `Constant::encode` enforces internally.
///
/// Unsupported Arrow types — anything that is not one of the
/// primitive shapes Constant accepts — return `false` so the selector
/// skips the Constant fast path and falls through to the estimator
/// pass. The Constant `applicable_to` guard at the call site usually
/// prevents this branch from firing.
fn is_uniform(array: &dyn Array) -> bool {
    let row_count = array.len();
    if row_count == 0 {
        return false;
    }
    if row_count == 1 {
        return true;
    }
    match array.data_type() {
        DataType::Boolean => {
            let arr = match array.as_any().downcast_ref::<BooleanArray>() {
                Some(a) => a,
                None => return false,
            };
            let first = arr.value(0);
            (1..row_count).all(|i| arr.value(i) == first)
        }
        DataType::Int64 => {
            let arr = match array.as_any().downcast_ref::<Int64Array>() {
                Some(a) => a,
                None => return false,
            };
            let values = arr.values();
            let first = values[0];
            values.iter().all(|v| *v == first)
        }
        DataType::Float64 => {
            let arr = match array.as_any().downcast_ref::<Float64Array>() {
                Some(a) => a,
                None => return false,
            };
            let values = arr.values();
            let first_bits = values[0].to_bits();
            values.iter().all(|v| v.to_bits() == first_bits)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let arr = match array.as_any().downcast_ref::<TimestampNanosecondArray>() {
                Some(a) => a,
                None => return false,
            };
            let values = arr.values();
            let first = values[0];
            values.iter().all(|v| *v == first)
        }
        DataType::Utf8 => {
            let arr = match array.as_any().downcast_ref::<StringArray>() {
                Some(a) => a,
                None => return false,
            };
            let first = arr.value(0);
            (1..row_count).all(|i| arr.value(i) == first)
        }
        DataType::Utf8View => {
            let arr = match array.as_any().downcast_ref::<StringViewArray>() {
                Some(a) => a,
                None => return false,
            };
            let first = arr.value(0);
            (1..row_count).all(|i| arr.value(i) == first)
        }
        // Nested or unrecognized types: skip the Constant fast path.
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayRef, BooleanArray, Float64Array, Int64Array, StringViewArray, TimestampNanosecondArray,
    };
    use bqlite_core::BqliteError;
    use std::sync::Arc;

    use super::super::{decompress_lz4, EncodingType, Rle};

    /// Drive `select_encoding` and verify the chosen encoding's
    /// decode of the (possibly LZ4-decompressed) on-disk payload
    /// reproduces the input array. Returns the chosen encoding so
    /// callers can assert on it.
    fn select_and_round_trip(array: ArrayRef, ty: BqlType) -> EncodingType {
        let selected = select_encoding(array.as_ref(), &ty).expect("selector must succeed");
        // Verify the on-disk payload matches the chunk payload after
        // optional LZ4 decompression.
        let payload = match selected.compression {
            CompressionType::None => selected.on_disk_payload.clone(),
            CompressionType::Lz4 => {
                decompress_lz4(&selected.on_disk_payload, selected.chunk.payload.len())
                    .expect("LZ4 decompress must succeed")
            }
        };
        assert_eq!(
            payload, selected.chunk.payload,
            "decompressed on-disk payload must match the encoder's payload"
        );
        // Decode and compare to the input.
        let decoded = match selected.encoding() {
            EncodingType::Plain => Plain.decode(&selected.chunk, &ty),
            EncodingType::Constant => Constant.decode(&selected.chunk, &ty),
            EncodingType::Dictionary => Dictionary.decode(&selected.chunk, &ty),
            EncodingType::Delta => Delta.decode(&selected.chunk, &ty),
            EncodingType::BitPacking => BitPacking.decode(&selected.chunk, &ty),
            EncodingType::Rle => Rle.decode(&selected.chunk, &ty),
            EncodingType::DoubleDelta => DoubleDelta.decode(&selected.chunk, &ty),
            other => panic!("selector should never pick unimplemented encoding {other:?}"),
        }
        .expect("decode must succeed");
        assert_eq!(
            decoded.as_ref(),
            array.as_ref(),
            "round-trip via the chosen encoding must match the input"
        );
        selected.encoding()
    }

    // ── Phase 1: empty arrays ──

    #[test]
    fn empty_int_array_picks_plain() {
        let array: ArrayRef = Arc::new(Int64Array::from(Vec::<i64>::new()));
        let chosen = select_and_round_trip(array, BqlType::Int);
        assert_eq!(chosen, EncodingType::Plain);
    }

    #[test]
    fn empty_string_array_picks_plain() {
        let array: ArrayRef = Arc::new(StringViewArray::from(Vec::<String>::new()));
        let chosen = select_and_round_trip(array, BqlType::String);
        assert_eq!(chosen, EncodingType::Plain);
    }

    #[test]
    fn empty_bool_array_picks_plain() {
        let array: ArrayRef = Arc::new(BooleanArray::from(Vec::<bool>::new()));
        let chosen = select_and_round_trip(array, BqlType::Bool);
        assert_eq!(chosen, EncodingType::Plain);
    }

    // ── Phase 2: uniformity → Constant ──

    #[test]
    fn uniform_int_array_picks_constant() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![42_i64; 100]));
        let chosen = select_and_round_trip(array, BqlType::Int);
        assert_eq!(chosen, EncodingType::Constant);
    }

    #[test]
    fn uniform_string_array_picks_constant() {
        let array: ArrayRef = Arc::new(StringViewArray::from(vec!["click"; 100]));
        let chosen = select_and_round_trip(array, BqlType::String);
        assert_eq!(chosen, EncodingType::Constant);
    }

    #[test]
    fn uniform_bool_array_picks_constant() {
        let array: ArrayRef = Arc::new(BooleanArray::from(vec![true; 100]));
        let chosen = select_and_round_trip(array, BqlType::Bool);
        assert_eq!(chosen, EncodingType::Constant);
    }

    #[test]
    fn uniform_float_picks_constant_via_bit_exact_check() {
        let array: ArrayRef = Arc::new(Float64Array::from(vec![1.5_f64; 50]));
        let chosen = select_and_round_trip(array, BqlType::Float);
        assert_eq!(chosen, EncodingType::Constant);
    }

    #[test]
    fn uniform_negative_zero_float_is_distinct_from_positive_zero() {
        // -0.0 and 0.0 compare equal under PartialEq but have
        // different bit patterns. The bit-exact uniformity check
        // means a mixed array of -0.0 and 0.0 is *not* uniform — the
        // selector falls through to Plain (or another non-Constant
        // encoding), preserving the sign on round-trip.
        let array: ArrayRef = Arc::new(Float64Array::from(vec![0.0_f64, -0.0, 0.0, -0.0]));
        let chosen = select_and_round_trip(array, BqlType::Float);
        assert_ne!(chosen, EncodingType::Constant);
    }

    #[test]
    fn uniform_timestamp_picks_constant() {
        let array: ArrayRef = Arc::new(
            TimestampNanosecondArray::from(vec![1_700_000_000_000_000_000_i64; 50])
                .with_timezone("UTC"),
        );
        let chosen = select_and_round_trip(array, BqlType::Timestamp);
        assert_eq!(chosen, EncodingType::Constant);
    }

    // ── Phase 3: encoding-rank ──

    #[test]
    fn low_cardinality_string_picks_dictionary() {
        // 100 rows, 3 distinct values — Dictionary's bit-packed code
        // stream is much smaller than Plain's length-prefixed strings.
        let mut values = Vec::with_capacity(100);
        for i in 0..100 {
            values.push(match i % 3 {
                0 => "click",
                1 => "view",
                _ => "purchase",
            });
        }
        let array: ArrayRef = Arc::new(StringViewArray::from(values));
        let chosen = select_and_round_trip(array, BqlType::String);
        assert_eq!(chosen, EncodingType::Dictionary);
    }

    #[test]
    fn narrow_range_int_picks_bitpacking_or_dictionary() {
        // Unsorted ints in a narrow range — BitPacking is the natural
        // winner over Plain. Dictionary may also win for very low
        // cardinality. Either is acceptable; both are smaller than
        // Plain. The test asserts "smaller than Plain" rather than
        // pinning a specific choice so the selector heuristic can
        // refine its tiebreaker without churning the test.
        let values: Vec<i64> = (0..200).map(|i| (i * 7) % 50).collect();
        let array: ArrayRef = Arc::new(Int64Array::from(values));
        let chosen = select_and_round_trip(array, BqlType::Int);
        assert_ne!(
            chosen,
            EncodingType::Plain,
            "narrow-range ints should beat Plain"
        );
    }

    #[test]
    fn monotonic_timestamp_run_picks_compact_encoding() {
        // Monotonic 1ms-spaced timestamps — Delta and BitPacking both
        // beat Plain. Either is acceptable.
        let values: Vec<i64> = (0..256)
            .map(|i| 1_700_000_000_000_000_000_i64 + i * 1_000_000)
            .collect();
        let array: ArrayRef = Arc::new(TimestampNanosecondArray::from(values).with_timezone("UTC"));
        let chosen = select_and_round_trip(array, BqlType::Timestamp);
        assert_ne!(chosen, EncodingType::Plain);
    }

    #[test]
    fn random_floats_pick_plain() {
        // Float has no other applicable v1 encoding (no Dictionary,
        // no Delta, no BitPacking). Plain is the only candidate.
        let values: Vec<f64> = (0..50).map(|i| (i as f64) * std::f64::consts::PI).collect();
        let array: ArrayRef = Arc::new(Float64Array::from(values));
        let chosen = select_and_round_trip(array, BqlType::Float);
        assert_eq!(chosen, EncodingType::Plain);
    }

    #[test]
    fn alternating_bool_picks_plain() {
        // Bool has Plain + Constant in the current candidate set (Rle
        // is deferred to TASK-419). Alternating bools are not uniform,
        // so Constant is out, and Plain is the only remaining
        // registered candidate.
        let values: Vec<bool> = (0..50).map(|i| i % 2 == 0).collect();
        let array: ArrayRef = Arc::new(BooleanArray::from(values));
        let chosen = select_and_round_trip(array, BqlType::Bool);
        assert_eq!(chosen, EncodingType::Plain);
    }

    // ── LZ4 compression decision ──

    #[test]
    fn highly_compressible_payload_gets_lz4() {
        // Float has no Dictionary / BitPacking / Delta applicability,
        // so the selector always picks Plain for Float (unless the
        // chunk is uniform, in which case Constant wins). This makes
        // Float the cleanest way to test the LZ4 wrapper on a Plain
        // payload — Int / String / Timestamp would route through
        // Dictionary or Constant on a highly compressible chunk.
        //
        // Construct a Float64 chunk that Constant rejects (one element
        // differs) but whose payload is mostly zero bytes. The Plain
        // payload is `8 * row_count` little-endian doubles, which LZ4
        // compresses by ~99%.
        let values: Vec<f64> = (0..10_000)
            .map(|i| if i == 0 { 1.0 } else { 0.0 })
            .collect();
        let array: ArrayRef = Arc::new(Float64Array::from(values));
        let selected =
            select_encoding(array.as_ref(), &BqlType::Float).expect("selector must succeed");
        assert_eq!(selected.encoding(), EncodingType::Plain);
        assert_eq!(
            selected.compression,
            CompressionType::Lz4,
            "highly compressible Plain payload must trigger LZ4"
        );
        // Sanity: the on_disk_payload is the compressed form and is
        // smaller than the chunk payload.
        assert!(selected.on_disk_payload.len() < selected.chunk.payload.len());
    }

    #[test]
    fn random_payload_skips_lz4() {
        // Random-looking floats. The payload is incompressible.
        let values: Vec<f64> = (0..200)
            .map(|i| ((i as f64) * 1.234567).sin() * 1e9 + (i as f64))
            .collect();
        let array: ArrayRef = Arc::new(Float64Array::from(values));
        let selected =
            select_encoding(array.as_ref(), &BqlType::Float).expect("selector must succeed");
        assert_eq!(selected.encoding(), EncodingType::Plain);
        assert_eq!(
            selected.compression,
            CompressionType::None,
            "incompressible Plain payload must not trigger LZ4"
        );
        assert_eq!(selected.on_disk_payload, selected.chunk.payload);
    }

    #[test]
    fn constant_chunks_have_no_compression() {
        // Constant emits zero payload bytes — LZ4 has nothing to wrap.
        let array: ArrayRef = Arc::new(Int64Array::from(vec![42_i64; 100]));
        let selected =
            select_encoding(array.as_ref(), &BqlType::Int).expect("selector must succeed");
        assert_eq!(selected.encoding(), EncodingType::Constant);
        assert_eq!(selected.compression, CompressionType::None);
        assert!(selected.on_disk_payload.is_empty());
    }

    // ── Plain is the universal fallback ──

    #[test]
    fn plain_is_legal_fallback_for_every_primitive() {
        // For every primitive type, the selector must produce a
        // chunk that round-trips, even when no specialized encoding
        // is applicable.
        let cases: Vec<(BqlType, ArrayRef)> = vec![
            (
                BqlType::Bool,
                Arc::new(BooleanArray::from(vec![true, false, true])),
            ),
            (BqlType::Int, Arc::new(Int64Array::from(vec![1_i64, 2, 3]))),
            (
                BqlType::Float,
                Arc::new(Float64Array::from(vec![1.0_f64, 2.0, 3.0])),
            ),
            (
                BqlType::String,
                Arc::new(StringViewArray::from(vec!["a", "b", "c"])),
            ),
            (
                BqlType::Timestamp,
                Arc::new(TimestampNanosecondArray::from(vec![1_i64, 2, 3]).with_timezone("UTC")),
            ),
        ];
        for (ty, array) in cases {
            // Just verify round-trip works; the chosen encoding may
            // be Plain or another applicable encoding.
            let _chosen = select_and_round_trip(array, ty);
        }
    }

    #[test]
    fn selector_rejects_nullable_input() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![Some(1_i64), None, Some(3)]));
        let err = select_encoding(array.as_ref(), &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("null_count")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    // ── decode_cost ordering ──

    #[test]
    fn decode_cost_orders_encodings_correctly() {
        // The cost table is the tiebreaker; assert the relative order
        // matches the documented "fastest decode wins" rule.
        // Full ordering: Constant < Plain < Rle < BitPacking < Delta < DoubleDelta < Dictionary.
        assert!(decode_cost(EncodingType::Constant) < decode_cost(EncodingType::Plain));
        assert!(decode_cost(EncodingType::Plain) < decode_cost(EncodingType::Rle));
        assert!(decode_cost(EncodingType::Rle) < decode_cost(EncodingType::BitPacking));
        assert!(decode_cost(EncodingType::BitPacking) < decode_cost(EncodingType::Delta));
        assert!(decode_cost(EncodingType::Delta) < decode_cost(EncodingType::DoubleDelta));
        assert!(decode_cost(EncodingType::DoubleDelta) < decode_cost(EncodingType::Dictionary));
    }

    #[test]
    fn tiebreaker_picks_lower_decode_cost_at_equal_size() {
        // A three-element ascending int run where three encodings
        // collide on payload size (each padded to 8 bytes):
        //
        // - Plain:       24 bytes (3 × i64 LE)
        // - BitPacking:  8 bytes padded (2 bits/value × 3 values padded)
        // - Delta:       8 bytes padded (2-bit residuals × 2 residuals padded)
        // - DoubleDelta: 8 bytes padded (1 dd value at 1-bit width padded)
        //
        // All three compact encodings produce 8-byte payloads. The
        // selector picks BitPacking (decode cost 3), beating Delta
        // (cost 5) and DoubleDelta (cost 6).
        let array: ArrayRef = Arc::new(Int64Array::from(vec![0_i64, 1, 2]));
        let chosen = select_encoding_type(array.as_ref(), &BqlType::Int).unwrap();
        assert_eq!(
            chosen,
            EncodingType::BitPacking,
            "BitPacking, Delta, and DoubleDelta tie on size; BitPacking \
             must win via lowest decode cost"
        );
    }

    #[test]
    fn double_delta_wins_two_element_input() {
        // For a 2-element array, DoubleDelta stores both values in params
        // (base_value + first_delta) with a 0-byte payload. This beats
        // Delta (8-byte padded payload for 1 residual) and BitPacking
        // (8-byte padded payload for 2 values at 1 bit each).
        let array: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2]));
        let chosen = select_encoding_type(array.as_ref(), &BqlType::Int).unwrap();
        assert_eq!(
            chosen,
            EncodingType::DoubleDelta,
            "DoubleDelta must win on 2-element input: 0-byte payload beats \
             Delta and BitPacking which both produce 8-byte padded payloads"
        );
    }

    #[test]
    fn delta_overflow_is_skipped_in_favor_of_other_encodings() {
        // Delta::estimate_size errors when consecutive residuals
        // overflow i64 (e.g. [i64::MIN, i64::MAX]). The selector must
        // silently skip Delta and pick another applicable encoding.
        // BitPacking handles the same input via min/max framing.
        let array: ArrayRef = Arc::new(Int64Array::from(vec![i64::MIN, i64::MAX]));
        let chosen = select_and_round_trip(array, BqlType::Int);
        assert_ne!(
            chosen,
            EncodingType::Delta,
            "Delta must be skipped when residuals overflow i64"
        );
    }

    #[test]
    fn nested_type_falls_through_to_plain_and_surfaces_encode_error() {
        // Per segment-format-v1.md §9.5, the v1 selector "falls through
        // to Plain" for nested types. Plain itself doesn't yet implement
        // List/Map encoding, so the surface is: select_encoding returns
        // the Plain choice but the subsequent encode call inside the
        // selector errors with a typed "nested type deferred" message.
        // This test pins that contract — when nested encoding lands in
        // a later task, the test should be updated to assert success.
        use arrow::array::{Int64Builder, ListBuilder};
        let mut builder = ListBuilder::new(Int64Builder::new());
        builder.values().append_value(1);
        builder.values().append_value(2);
        builder.append(true);
        let array: ArrayRef = Arc::new(builder.finish());
        let err = select_encoding(array.as_ref(), &BqlType::List(Box::new(BqlType::Int)))
            .expect_err("nested type encoding is deferred in v1");
        match err {
            BqliteError::Execution(msg) => {
                assert!(
                    msg.contains("nested") || msg.contains("List") || msg.contains("Map"),
                    "expected a deferred-nested-encoding error, got: {msg}"
                );
            }
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    // ── decision-only entry point ──

    #[test]
    fn select_encoding_type_matches_full_select() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![5_i64, 3, 5, 1, 3]));
        let from_type = select_encoding_type(array.as_ref(), &BqlType::Int).unwrap();
        let from_full = select_encoding(array.as_ref(), &BqlType::Int)
            .unwrap()
            .encoding();
        assert_eq!(from_type, from_full);
    }

    #[test]
    fn select_encoding_type_rejects_nullable_input() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![Some(1_i64), None]));
        let err = select_encoding_type(array.as_ref(), &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("null_count")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }
}
