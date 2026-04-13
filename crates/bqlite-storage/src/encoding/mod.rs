//! Column encoding layer for segment files.
//!
//! The byte-level contract defined here implements the per-encoding
//! parameter blocks and payload formats specified in
//! `docs/design/storage/segment-format-v1.md` §9. Every encoding in the
//! v1 set is a submodule of this module; TASK-206 lands the [`Encoding`]
//! trait and the [`Plain`] reference implementation, and subsequent
//! Wave 2 tasks (TASK-207 – TASK-211) add `Dictionary`, `Delta`,
//! `BitPacking`, `Constant`, and the LZ4 post-encoding wrapper without
//! changing the trait surface.
//!
//! # Contract
//!
//! An [`Encoding`] impl is a pure, stateless transform between an Arrow
//! array of a single column and a self-contained [`EncodedChunk`] — a
//! `(params, payload)` byte pair plus a row count. Null handling lives
//! **above** the encoding layer:
//!
//! - The writer (TASK-213 / TASK-214) extracts nulls into a separate
//!   bitmap stored as a prefix of the column chunk, then feeds only
//!   the dense non-null values into `Encoding::encode`.
//! - The reader (TASK-215) parses the null bitmap, decodes the dense
//!   values via `Encoding::decode`, and splices the two back into a
//!   nullable Arrow array.
//!
//! Encodings therefore never see null values. Passing a nullable array
//! with `array.null_count() > 0` to `encode` is rejected with
//! [`BqliteError::Execution`]; this is a contract violation, not a
//! recoverable condition.
//!
//! # Round-trip property-test pattern
//!
//! Every encoding ships with a `tests/tests/prop_encoding_<name>.rs`
//! file that asserts `decode(encode(x)) == x` on a proptest-generated
//! Arrow array. The Plain impl's test is the template — see
//! `tests/tests/prop_encoding_plain.rs`. Adding a new encoding means
//! copying the template file, swapping the encoding under test, and
//! extending the property set with encoding-specific invariants (e.g.
//! "Dictionary codes are dense in `[0, cardinality)`").
//!
//! See `docs/design/storage/segment-format-v1.md` §7 for how
//! `EncodedChunk` bytes compose into the on-disk column-chunk layout.

use arrow::array::{Array, ArrayRef};
use bqlite_core::{BqlType, BqliteError, Result};

pub mod bitpacking;
pub mod constant;
pub mod delta;
pub mod dictionary;
pub mod double_delta;
pub mod for_encoding;
pub mod frequency;
pub mod fsst;
pub mod lz4;
pub mod plain;
pub mod rle;
pub mod selector;

pub use bitpacking::BitPacking;
pub use constant::Constant;
pub use delta::Delta;
pub use dictionary::Dictionary;
pub use double_delta::DoubleDelta;
pub use for_encoding::ForEncoding;
pub use fsst::Fsst;
pub use lz4::{compress_lz4, decompress_lz4, CompressionType};
pub use plain::Plain;
pub use rle::Rle;
pub use selector::{decode_cost, select_encoding, select_encoding_type, SelectedEncoding};

/// On-disk encoding discriminant per `segment-format-v1.md` §9 and
/// `segment-format-v2.md` §4.
///
/// Values match the `EncodingType` enum documented in
/// `storage-format.md` §10.2 and `advanced-encodings.md` §1.2.
/// v1 discriminants (Plain, Dictionary, Delta, BitPacking, Constant)
/// are unchanged. Wave 4 adds Rle (TASK-413), DoubleDelta (TASK-414),
/// For (TASK-415), Fsst (TASK-416), and further variants in later
/// tasks. Existing discriminants never change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum EncodingType {
    // v1 (unchanged)
    /// Uncompressed primitive layout. §9.1.
    Plain = 0,
    /// Sorted distinct values + bit-packed ordinal codes. §9.2.
    Dictionary = 1,
    /// First-value + zigzag bit-packed residuals. §9.3.
    Delta = 2,
    /// Bit-packed offsets from a min-value frame of reference. §9.4.
    BitPacking = 4,
    /// Run-length encoding: (run_ends, values) pairs. Wave 4 / TASK-413.
    Rle = 5,
    /// Zero-payload single-value encoding. §9.5.
    Constant = 6,

    // v2 (new in Wave 4, per segment-format-v2.md §4)
    /// Second-order delta encoding for near-constant-interval columns.
    DoubleDelta = 3,
    /// Fast Static Symbol Table compression for high-cardinality strings.
    Fsst = 7,
    /// Frame-of-Reference encoding with per-block minimums.
    For = 8,
    /// Patched Frame-of-Reference with outlier exception list.
    PFor = 9,
    /// Adaptive Lossless floating-Point compression.
    Alp = 10,
    // Reserved: FreqEncoding = 11 (no-go per TASK-401 §9, discriminant
    //           reserved for future variable-width coding scheme)
}

impl EncodingType {
    /// The byte that identifies this encoding on disk.
    pub fn discriminant(self) -> u8 {
        self as u8
    }

    /// Parse a byte read out of a segment file back into an
    /// [`EncodingType`]. Unknown discriminants — including any
    /// reserved-for-later-waves value (`9`, `10`, ..) — produce
    /// [`BqliteError::Execution`] so the reader can surface a
    /// corruption error without panicking.
    ///
    /// This method enforces the v1 encoding set only. For
    /// version-aware parsing that also accepts v2 discriminants,
    /// use [`Self::from_discriminant_versioned`].
    pub fn from_discriminant(byte: u8) -> Result<Self> {
        match byte {
            0 => Ok(Self::Plain),
            1 => Ok(Self::Dictionary),
            2 => Ok(Self::Delta),
            3 => Ok(Self::DoubleDelta),
            4 => Ok(Self::BitPacking),
            5 => Ok(Self::Rle),
            6 => Ok(Self::Constant),
            8 => Ok(Self::For),
            other => Err(BqliteError::Execution(format!(
                "unknown encoding discriminant {other} — segment written by an incompatible version"
            ))),
        }
    }

    /// Parse a byte into an [`EncodingType`], gated by format version.
    ///
    /// v1 segments accept `{0, 1, 2, 3, 4, 5, 6}` (Rle added by
    /// TASK-413, DoubleDelta added by TASK-414). v2 segments
    /// additionally accept `{7, 8, 9, 10}`. Discriminant 11
    /// (`FreqEncoding`) is reserved but rejected in both versions
    /// per `segment-format-v2.md` §4.
    pub fn from_discriminant_versioned(byte: u8, format_version: u16) -> Result<Self> {
        match byte {
            // v1 set — always accepted (includes Rle since TASK-413,
            // DoubleDelta since TASK-414)
            0 => Ok(Self::Plain),
            1 => Ok(Self::Dictionary),
            2 => Ok(Self::Delta),
            3 => Ok(Self::DoubleDelta),
            4 => Ok(Self::BitPacking),
            5 => Ok(Self::Rle),
            6 => Ok(Self::Constant),
            // v2 set — accepted only when format_version >= 2
            7 if format_version >= 2 => Ok(Self::Fsst),
            8 if format_version >= 2 => Ok(Self::For),
            9 if format_version >= 2 => Ok(Self::PFor),
            10 if format_version >= 2 => Ok(Self::Alp),
            other => Err(BqliteError::Execution(format!(
                "unknown encoding discriminant {other} for format version {format_version} \
                 — segment written by an incompatible version"
            ))),
        }
    }
}

/// A self-contained encoded column chunk.
///
/// Corresponds to the "encoding header + payload" bytes for a single
/// column in a single row group, per `segment-format-v1.md` §7. The
/// null bitmap and optional LZ4 compression wrap live above this type
/// — `EncodedChunk` is the raw, uncompressed form the encoding trait
/// operates on.
///
/// The byte layout written to disk is:
///
/// ```text
/// [encoding discriminant: u8]
/// [params: Vec<u8>]                        // variable, encoding-specific
/// [uncompressed_payload_length: u32 LE]    // equals payload.len()
/// [payload: Vec<u8>]                       // variable, encoding-specific
/// ```
///
/// Writers emit the discriminant, then the params bytes verbatim, then
/// the u32 LE length, then the payload; readers reverse the order. The
/// chunk is self-describing only in conjunction with the column's
/// [`BqlType`] from the segment schema — some encoding params (e.g.
/// `Constant`) are type-dependent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedChunk {
    /// The encoding that produced this chunk.
    pub encoding: EncodingType,
    /// Encoding-specific parameter bytes written to the segment file
    /// immediately after the discriminant. An empty vector is legal
    /// and is what Plain returns (Plain has no parameters —
    /// `segment-format-v1.md` §9.1).
    pub params: Vec<u8>,
    /// Uncompressed payload bytes — the "native byte stream" the
    /// encoding decoder consumes. This is what
    /// `uncompressed_payload_length` counts on disk. LZ4 compression,
    /// when applied, wraps these bytes at a layer above the encoding
    /// trait.
    pub payload: Vec<u8>,
    /// Number of non-null values the payload represents. Carried on
    /// the chunk so round-trip tests do not need to thread row counts
    /// separately; readers in production get the same value from the
    /// footer's `ColumnChunkMeta.row_count`.
    pub row_count: usize,
}

/// Borrowed view over an encoded column chunk.
///
/// This is the read-path counterpart to [`EncodedChunk`]. Writers and
/// round-trip tests still traffic in owned chunks, but the segment
/// reader can point decoders directly at the already-parsed params and
/// payload bytes it holds in memory instead of cloning them into fresh
/// `Vec<u8>` buffers first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BorrowedEncodedChunk<'a> {
    /// The encoding that produced this chunk.
    pub encoding: EncodingType,
    /// Borrowed encoding-specific parameter bytes.
    pub params: &'a [u8],
    /// Borrowed uncompressed payload bytes.
    pub payload: &'a [u8],
    /// Number of non-null values represented by `payload`.
    pub row_count: usize,
}

/// Column encoding trait — the runtime entry point for every v1
/// encoding.
///
/// Trait objects (`Box<dyn Encoding>`) are how the writer orchestrator
/// (TASK-214) dispatches between the handful of concrete encodings
/// without a trait-parameter stew. The trait is intentionally narrow:
///
/// - No lifetime parameters on the trait itself — owned chunks remain
///   the canonical writer/test form, and read-time borrowing is an
///   opt-in method-level fast path.
/// - No generics (dispatch by discriminant, not by type).
/// - No state — every impl is a zero-sized marker type (see [`Plain`]).
///
/// # Invariants
///
/// - `encode` receives a dense (null-free) array. Passing an array
///   with nulls is a contract violation, reported as
///   [`BqliteError::Execution`].
/// - `decode` requires the same `BqlType` the array had at encode
///   time. Mismatches produce [`BqliteError::Execution`].
/// - `encode` followed by `decode` reproduces the original array
///   modulo Arrow's equality semantics. This is the round-trip
///   property every impl's test file asserts.
pub trait Encoding: Send + Sync {
    /// The on-disk discriminant this encoding writes into the segment
    /// file. Every impl returns a `'static` constant.
    fn encoding_type(&self) -> EncodingType;

    /// Whether this encoding can handle a column of the given
    /// [`BqlType`]. The selector (TASK-212) consults this predicate
    /// to prune the candidate set before ranking by estimated size.
    /// Nested types (`List`, `Map`) are intentionally handled only by
    /// [`Plain`] in v1 per `segment-format-v1.md` §9.1.
    fn applicable_to(&self, ty: &BqlType) -> bool;

    /// Upper-bound estimate of the encoded byte count without
    /// actually running the encode. The value is the size of the
    /// chunk's `payload` only — encoding-header and null-bitmap bytes
    /// are not included. Used by the selector to rank candidates; a
    /// conservative over-estimate is always safe, an under-estimate
    /// breaks the selector's ranking and is not allowed.
    fn estimate_size(&self, array: &dyn Array) -> Result<usize>;

    /// Encode a dense Arrow array into an [`EncodedChunk`]. The
    /// caller guarantees `array.null_count() == 0`; impls may return
    /// [`BqliteError::Execution`] if this invariant is violated.
    fn encode(&self, array: &dyn Array) -> Result<EncodedChunk>;

    /// Decode an [`EncodedChunk`] back into a dense Arrow array of the
    /// given [`BqlType`]. The returned array has exactly
    /// `chunk.row_count` elements and no nulls.
    fn decode(&self, chunk: &EncodedChunk, ty: &BqlType) -> Result<ArrayRef>;

    /// Decode a borrowed chunk view back into a dense Arrow array.
    ///
    /// The default implementation preserves the original behavior by
    /// materializing an owned [`EncodedChunk`] and delegating to
    /// [`Self::decode`]. Performance-sensitive decoders can override
    /// this to consume the borrowed slices directly and avoid the copy.
    fn decode_borrowed(&self, chunk: BorrowedEncodedChunk<'_>, ty: &BqlType) -> Result<ArrayRef> {
        self.decode(
            &EncodedChunk {
                encoding: chunk.encoding,
                params: chunk.params.to_vec(),
                payload: chunk.payload.to_vec(),
                row_count: chunk.row_count,
            },
            ty,
        )
    }
}

/// Shared helper: reject a nullable array at the `encode` boundary.
///
/// The encoding layer's invariant is that callers pre-extract nulls
/// before invoking any impl — see the module docs. This helper
/// centralises the error message so every impl surfaces the same
/// `BqliteError::Execution` when the invariant is violated.
pub(crate) fn require_dense(array: &dyn Array, encoding: &'static str) -> Result<()> {
    if array.null_count() != 0 {
        return Err(BqliteError::Execution(format!(
            "{encoding}::encode called with a nullable array \
             (null_count = {}); the writer must extract nulls \
             into a bitmap before feeding dense values into the \
             encoding layer",
            array.null_count()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoding_type_round_trip_discriminants() {
        for variant in [
            EncodingType::Plain,
            EncodingType::Dictionary,
            EncodingType::Delta,
            EncodingType::DoubleDelta,
            EncodingType::BitPacking,
            EncodingType::Rle,
            EncodingType::Constant,
            EncodingType::Fsst,
            EncodingType::For,
            EncodingType::PFor,
            EncodingType::Alp,
        ] {
            let byte = variant.discriminant();
            let parsed = EncodingType::from_discriminant_versioned(byte, 2).unwrap();
            assert_eq!(parsed, variant);
        }
    }

    #[test]
    fn encoding_type_discriminants_match_segment_format_spec() {
        // Values pinned to `segment-format-v1.md` §9 and
        // `segment-format-v2.md` §4 and `advanced-encodings.md` §1.2
        // so that later-wave encodings (Fsst = 7, ...) can be added
        // without renumbering.
        assert_eq!(EncodingType::Plain.discriminant(), 0);
        assert_eq!(EncodingType::Dictionary.discriminant(), 1);
        assert_eq!(EncodingType::Delta.discriminant(), 2);
        assert_eq!(EncodingType::DoubleDelta.discriminant(), 3);
        assert_eq!(EncodingType::BitPacking.discriminant(), 4);
        assert_eq!(EncodingType::Rle.discriminant(), 5);
        assert_eq!(EncodingType::Constant.discriminant(), 6);
        assert_eq!(EncodingType::Fsst.discriminant(), 7);
        assert_eq!(EncodingType::For.discriminant(), 8);
    }

    #[test]
    fn encoding_type_discriminants_match_segment_format_v2_spec() {
        // Values pinned to `segment-format-v2.md` §4.
        assert_eq!(EncodingType::DoubleDelta.discriminant(), 3);
        assert_eq!(EncodingType::Rle.discriminant(), 5);
        assert_eq!(EncodingType::Fsst.discriminant(), 7);
        assert_eq!(EncodingType::For.discriminant(), 8);
        assert_eq!(EncodingType::PFor.discriminant(), 9);
        assert_eq!(EncodingType::Alp.discriminant(), 10);
    }

    #[test]
    fn unknown_discriminant_is_a_typed_execution_error() {
        // v2-only discriminants (3, 7, 8, 9, 10) are rejected by
        // from_discriminant. Byte 5 (Rle) is accepted since TASK-413.
        for byte in [7u8, 8, 9, 10, 11, 255] {
            let err = EncodingType::from_discriminant(byte).unwrap_err();
            match err {
                BqliteError::Execution(msg) => {
                    assert!(
                        msg.contains(&byte.to_string()),
                        "error message should name the offending byte; got: {msg}"
                    );
                }
                other => panic!("expected Execution error, got {other:?}"),
            }
        }
    }

    #[test]
    fn from_discriminant_versioned_v1_rejects_v2_encodings() {
        // Rle (5) accepted in v1 since TASK-413, DoubleDelta (3) since TASK-414
        for byte in [7u8, 8, 9, 10] {
            assert!(
                EncodingType::from_discriminant_versioned(byte, 1).is_err(),
                "v1 should reject discriminant {byte}"
            );
        }
    }

    #[test]
    fn from_discriminant_versioned_v2_accepts_all_valid() {
        let v2_pairs = [
            (0, EncodingType::Plain),
            (1, EncodingType::Dictionary),
            (2, EncodingType::Delta),
            (3, EncodingType::DoubleDelta),
            (4, EncodingType::BitPacking),
            (5, EncodingType::Rle),
            (6, EncodingType::Constant),
            (7, EncodingType::Fsst),
            (8, EncodingType::For),
            (9, EncodingType::PFor),
            (10, EncodingType::Alp),
        ];
        for (byte, expected) in v2_pairs {
            assert_eq!(
                EncodingType::from_discriminant_versioned(byte, 2).unwrap(),
                expected,
            );
        }
    }

    #[test]
    fn from_discriminant_versioned_rejects_reserved_11() {
        assert!(EncodingType::from_discriminant_versioned(11, 1).is_err());
        assert!(EncodingType::from_discriminant_versioned(11, 2).is_err());
    }
}
