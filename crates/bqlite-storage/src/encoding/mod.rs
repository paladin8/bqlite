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

pub mod constant;
pub mod plain;

pub use constant::Constant;
pub use plain::Plain;

/// On-disk encoding discriminant per `segment-format-v1.md` §9.
///
/// Values match the `EncodingType` enum documented in
/// `storage-format.md` §10.2. v1 readers recognize exactly the five
/// variants below; any other discriminant in a segment file is
/// treated as corruption. Later waves extend this enum by adding
/// variants (and bumping the segment format version); existing
/// discriminants never change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum EncodingType {
    /// Uncompressed primitive layout. §9.1.
    Plain = 0,
    /// Reserved for TASK-207.
    Dictionary = 1,
    /// Reserved for TASK-208.
    Delta = 2,
    /// Reserved for TASK-209.
    BitPacking = 4,
    /// Reserved for TASK-210.
    Constant = 6,
}

impl EncodingType {
    /// The byte that identifies this encoding on disk.
    pub fn discriminant(self) -> u8 {
        self as u8
    }

    /// Parse a byte read out of a segment file back into an
    /// [`EncodingType`]. Unknown discriminants — including any
    /// reserved-for-later-waves value (`3`, `5`, `7` ..) — produce
    /// [`BqliteError::Execution`] so the reader can surface a
    /// corruption error without panicking.
    pub fn from_discriminant(byte: u8) -> Result<Self> {
        match byte {
            0 => Ok(Self::Plain),
            1 => Ok(Self::Dictionary),
            2 => Ok(Self::Delta),
            4 => Ok(Self::BitPacking),
            6 => Ok(Self::Constant),
            other => Err(BqliteError::Execution(format!(
                "unknown encoding discriminant {other} — segment written by an incompatible version"
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

/// Column encoding trait — the runtime entry point for every v1
/// encoding.
///
/// Trait objects (`Box<dyn Encoding>`) are how the writer orchestrator
/// (TASK-214) dispatches between the handful of concrete encodings
/// without a trait-parameter stew. The trait is intentionally narrow:
///
/// - No lifetime parameters (chunks own their bytes).
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
            EncodingType::BitPacking,
            EncodingType::Constant,
        ] {
            let byte = variant.discriminant();
            let parsed = EncodingType::from_discriminant(byte).unwrap();
            assert_eq!(parsed, variant);
        }
    }

    #[test]
    fn encoding_type_discriminants_match_segment_format_v1_spec() {
        // Values pinned to `segment-format-v1.md` §9 and
        // `storage-format.md` §10.2 so that later-wave encodings
        // (Rle = 5, Fsst = 7, ...) can be added without renumbering.
        assert_eq!(EncodingType::Plain.discriminant(), 0);
        assert_eq!(EncodingType::Dictionary.discriminant(), 1);
        assert_eq!(EncodingType::Delta.discriminant(), 2);
        assert_eq!(EncodingType::BitPacking.discriminant(), 4);
        assert_eq!(EncodingType::Constant.discriminant(), 6);
    }

    #[test]
    fn unknown_discriminant_is_a_typed_execution_error() {
        for byte in [3u8, 5, 7, 8, 255] {
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
}
