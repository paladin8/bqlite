//! FSST (Fast Static Symbol Table) string compression.
//!
//! FSST builds a 256-entry symbol table of common substrings (1–8
//! bytes each) from a sample of the column's strings, then re-encodes
//! each string by replacing substring occurrences with single-byte
//! codes. The symbol table is stored once per segment and shared by
//! all row groups.
//!
//! See `docs/design/storage/advanced-encodings.md` §7 for the go
//! decision evidence and `segment-format-v2.md` §5.3 for the on-disk
//! parameter block layout.
//!
//! # Applicable types
//!
//! `String` only. FSST targets high-cardinality string columns (URLs,
//! user agents, query strings) that fall through Dictionary encoding.
//!
//! # Self-contained `EncodedChunk` vs. segment-level layout
//!
//! The on-disk segment layout stores each column's FSST symbol table
//! once at the **segment** level (in the FSST symbol tables region,
//! `segment-format-v2.md` §6) and the per-row-group column chunk
//! stores only `symbol_table_id: u32 LE` params plus the compressed
//! strings payload. That layout is what TASK-419 writes to disk.
//!
//! The [`Encoding`] trait operates on **self-contained**
//! [`EncodedChunk`]s so that round-trip property tests hold without a
//! separate segment-level store. To honor both constraints, the
//! trait-level chunk produced by `Fsst` inlines the symbol table into
//! the `params` block — following the same pattern as Dictionary, which
//! inlines dictionary values into its params. The segment writer
//! (TASK-419) will parse each FSST chunk's params, hoist the symbol
//! table to the segment-level region, and rewrite the on-disk params to
//! the `symbol_table_id` shape §5.3 mandates.
//!
//! # Params layout (self-contained form)
//!
//! ```text
//! [symbol_count:    u16 LE]        // number of symbols (0..=255)
//! for each symbol:
//!   [sym_len:       u8]            // 1..=8
//!   [sym_bytes:     [u8; sym_len]] // the symbol's bytes
//! ```
//!
//! # Payload layout
//!
//! ```text
//! for each string:
//!   [compressed_len: u16 LE]       // length of compressed_bytes
//!   [compressed_bytes: [u8; compressed_len]]
//! ```
//!
//! Each `compressed_bytes` sequence is the FSST-encoded form of the
//! original string. Byte 0xFF is the escape byte: the following byte
//! is a literal. All other bytes are symbol table indices.
//!
//! # Symbol table construction
//!
//! This module uses the `fsst-rs` crate (a pure-Rust FSST
//! implementation following the VLDB 2020 paper) for symbol table
//! construction and string compression/decompression. The crate
//! provides:
//!
//! - `CompressorBuilder` → builds a `Compressor` from training data
//! - `Compressor::compress` → compresses individual strings
//! - `Decompressor::decompress` → decompresses individual strings
//! - `Compressor::rebuild_from(symbols, lens)` → reconstructs from
//!   serialized symbol table
//!
//! # Null handling
//!
//! FSST operates on dense arrays — the writer strips nulls into a
//! separate bitmap before encoding. A nullable input is a contract
//! violation caught by [`super::require_dense`].
//!
//! # Edge cases
//!
//! - `row_count == 0`: legal. `encode` returns an empty chunk (params =
//!   `[0, 0]` for symbol_count=0, payload = `[]`). `decode` returns
//!   an empty array.
//! - All strings empty: legal. The symbol table will be empty or
//!   near-empty; each compressed string is 0 bytes.
//! - Strings shorter than any symbol: encoded as escape-preceded
//!   literal bytes. The compressed form is at most 2× the original.
//! - No beneficial symbols (FSST payload ≥ Plain payload): the
//!   selector guard in TASK-419 catches this; `Fsst::encode` itself
//!   always produces valid output regardless.
//!
//! # Selector guard
//!
//! Per `advanced-encodings.md` §7.7: choose FSST when
//! `cardinality / row_count ≥ 0.3` (high cardinality) and the column
//! is String-typed. The guard lives in the selector (TASK-419), not
//! here. [`Fsst::estimate_size`] returns a conservative upper bound
//! (2× input size) so the selector can compare accurately.

use arrow::array::{Array, ArrayRef, StringArray, StringViewArray, StringViewBuilder};
use arrow::datatypes::DataType;
use bqlite_core::{BqlType, BqliteError, Result};
use std::sync::Arc;

use super::{require_dense, BorrowedEncodedChunk, EncodedChunk, Encoding, EncodingType};

/// Maximum number of strings sampled for symbol table construction.
/// Per `advanced-encodings.md` §7.5: sample up to 16,384 strings.
const SAMPLE_SIZE: usize = 16_384;

/// FSST encoding — string compression via a trained symbol table.
///
/// Unlike the other encodings (which are zero-sized marker types),
/// `Fsst` holds a trained `fsst::Compressor` that provides the symbol
/// table and compression logic. The compressor is built once from a
/// sample of the column's strings and reused for all row groups in the
/// segment.
///
/// For decode-only paths (reader), construct via
/// [`Fsst::from_symbol_table`] using the serialized symbol table
/// loaded from the segment file.
#[derive(Clone)]
pub struct Fsst {
    compressor: fsst::Compressor,
}

impl std::fmt::Debug for Fsst {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Fsst")
            .field("symbol_count", &self.compressor.symbol_table().len())
            .finish()
    }
}

impl Fsst {
    /// Build an FSST encoder by training a symbol table on a sample of
    /// strings from the given array.
    ///
    /// The array must be a `StringViewArray` or `StringArray` (the two
    /// Arrow string representations bqlite uses). Up to [`SAMPLE_SIZE`]
    /// strings are sampled for training.
    pub fn train(array: &dyn Array) -> Result<Self> {
        let strings = collect_string_bytes(array)?;

        // Sample up to SAMPLE_SIZE strings for training.
        let sample: Vec<&[u8]> = if strings.len() <= SAMPLE_SIZE {
            strings.iter().map(|s| s.as_slice()).collect()
        } else {
            // Deterministic evenly-spaced sampling — no RNG needed.
            let step = strings.len() as f64 / SAMPLE_SIZE as f64;
            (0..SAMPLE_SIZE)
                .map(|i| {
                    let idx = (i as f64 * step) as usize;
                    strings[idx].as_slice()
                })
                .collect()
        };

        let compressor = fsst::Compressor::train(&sample);

        Ok(Self { compressor })
    }

    /// Reconstruct an FSST encoder/decoder from a serialized symbol
    /// table (as stored in the segment file's FSST symbol tables
    /// region).
    ///
    /// `symbols` and `symbol_lens` are the parallel arrays from the
    /// on-disk format: each symbol is a `fsst::Symbol` (8-byte packed
    /// value) and its length in bytes (1–8).
    pub fn from_symbol_table(symbols: &[fsst::Symbol], symbol_lens: &[u8]) -> Self {
        let compressor = fsst::Compressor::rebuild_from(symbols, symbol_lens);
        Self { compressor }
    }

    /// Deserialize an FSST encoder/decoder from the self-contained
    /// params format used in `EncodedChunk`.
    pub fn from_params(params: &[u8]) -> Result<Self> {
        let (symbols, symbol_lens) = parse_params(params)?;
        Ok(Self::from_symbol_table(&symbols, &symbol_lens))
    }

    /// Serialize the symbol table into the self-contained params format
    /// used in `EncodedChunk`.
    fn serialize_params(&self) -> Vec<u8> {
        let symbols = self.compressor.symbol_table();
        let lengths = self.compressor.symbol_lengths();
        let n = symbols.len();

        // Calculate total size: 2 (symbol_count) + sum(1 + sym_len)
        let total_sym_bytes: usize = lengths.iter().map(|&l| 1 + l as usize).sum();
        let mut params = Vec::with_capacity(2 + total_sym_bytes);

        params.extend_from_slice(&(n as u16).to_le_bytes());
        for i in 0..n {
            let len = lengths[i];
            params.push(len);
            let sym_bytes = &symbols[i].to_u64().to_le_bytes()[..len as usize];
            params.extend_from_slice(sym_bytes);
        }
        params
    }
}

impl Encoding for Fsst {
    fn encoding_type(&self) -> EncodingType {
        EncodingType::Fsst
    }

    fn applicable_to(&self, ty: &BqlType) -> bool {
        matches!(ty, BqlType::String)
    }

    fn estimate_size(&self, array: &dyn Array) -> Result<usize> {
        // Conservative upper bound: each string contributes at most
        // 2 bytes (compressed_len) + 2 * original_len (worst case:
        // every byte escaped). In practice FSST compresses 3–5×, so
        // this is intentionally loose — the selector uses the estimate
        // only for ranking, not for allocation.
        let total_string_bytes = total_string_byte_len(array)?;
        let row_count = array.len();
        // 2 bytes per string for the compressed_len prefix, plus
        // worst-case 2× expansion of string data.
        Ok(row_count * 2 + total_string_bytes * 2)
    }

    fn encode(&self, array: &dyn Array) -> Result<EncodedChunk> {
        require_dense(array, "Fsst")?;
        let row_count = array.len();

        let params = self.serialize_params();

        // Compress each string directly from the array — no intermediate
        // Vec<Vec<u8>> materialization needed for the encode path.
        let mut payload = Vec::new();
        encode_strings(array, &self.compressor, &mut payload)?;

        Ok(EncodedChunk {
            encoding: EncodingType::Fsst,
            params,
            payload,
            row_count,
        })
    }

    fn decode(&self, chunk: &EncodedChunk, ty: &BqlType) -> Result<ArrayRef> {
        // Use the already-built compressor instead of re-parsing params.
        decode_with_decompressor(
            &self.compressor.decompressor(),
            &chunk.payload,
            chunk.row_count,
            ty,
        )
    }

    fn decode_borrowed(&self, chunk: BorrowedEncodedChunk<'_>, ty: &BqlType) -> Result<ArrayRef> {
        decode_with_decompressor(
            &self.compressor.decompressor(),
            chunk.payload,
            chunk.row_count,
            ty,
        )
    }
}

// ── decode implementation ───────────────────────────────────────────────────

/// Decode an FSST-encoded payload using an already-constructed
/// decompressor. This is the fast path used when the `Fsst` struct
/// already holds a trained compressor.
fn decode_with_decompressor(
    decompressor: &fsst::Decompressor<'_>,
    payload: &[u8],
    row_count: usize,
    ty: &BqlType,
) -> Result<ArrayRef> {
    if !matches!(ty, BqlType::String) {
        return Err(BqliteError::Execution(format!(
            "Fsst::decode called with {ty:?} — FSST only supports String"
        )));
    }

    if row_count == 0 {
        return Ok(Arc::new(StringViewArray::from(Vec::<&str>::new())) as ArrayRef);
    }

    let mut builder = StringViewBuilder::with_capacity(row_count);
    let mut offset = 0;

    for _ in 0..row_count {
        if offset + 2 > payload.len() {
            return Err(BqliteError::Corruption(
                "Fsst::decode: payload too short — missing compressed_len".into(),
            ));
        }
        let compressed_len =
            u16::from_le_bytes(payload[offset..offset + 2].try_into().unwrap()) as usize;
        offset += 2;

        if offset + compressed_len > payload.len() {
            return Err(BqliteError::Corruption(format!(
                "Fsst::decode: payload too short — need {compressed_len} compressed \
                 bytes at offset {offset} but only {} remain",
                payload.len() - offset
            )));
        }

        let compressed = &payload[offset..offset + compressed_len];
        offset += compressed_len;

        let decompressed = decompressor.decompress(compressed);
        // SAFETY: FSST preserves the original bytes — if the input was
        // valid UTF-8, the output is valid UTF-8.
        let s = std::str::from_utf8(&decompressed).map_err(|e| {
            BqliteError::Corruption(format!(
                "Fsst::decode: decompressed bytes are not valid UTF-8: {e}"
            ))
        })?;
        builder.append_value(s);
    }

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

/// Decode by reconstructing the decompressor from the self-contained
/// params. Used by external callers that only have a chunk (e.g. the
/// segment reader in TASK-419).
pub fn decode_from_params(
    params: &[u8],
    payload: &[u8],
    row_count: usize,
    ty: &BqlType,
) -> Result<ArrayRef> {
    let fsst = Fsst::from_params(params)?;
    let decompressor = fsst.compressor.decompressor();
    decode_with_decompressor(&decompressor, payload, row_count, ty)
}

// ── helpers ─────────────────────────────────────────────────────────────────

/// Compress each string in the array directly into the payload buffer,
/// avoiding per-row heap allocations. Each string is written as:
/// `[compressed_len: u16 LE][compressed_bytes]`.
///
/// Returns `BqliteError::Execution` if any compressed string exceeds
/// the `u16::MAX` (65,535 byte) limit — per `segment-format-v2.md` §5.3
/// the writer must fall back to Plain for the entire column in that case.
fn encode_strings(
    array: &dyn Array,
    compressor: &fsst::Compressor,
    payload: &mut Vec<u8>,
) -> Result<()> {
    match array.data_type() {
        DataType::Utf8View => {
            let a = array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| {
                    BqliteError::Execution("Fsst: expected StringViewArray, downcast failed".into())
                })?;
            for i in 0..a.len() {
                compress_one(a.value(i).as_bytes(), compressor, payload)?;
            }
            Ok(())
        }
        DataType::Utf8 => {
            let a = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    BqliteError::Execution("Fsst: expected StringArray, downcast failed".into())
                })?;
            for i in 0..a.len() {
                compress_one(a.value(i).as_bytes(), compressor, payload)?;
            }
            Ok(())
        }
        other => Err(BqliteError::Execution(format!(
            "Fsst: unsupported Arrow type {other:?} — FSST covers Utf8/Utf8View only"
        ))),
    }
}

/// Compress a single string and append `[u16 LE len][compressed bytes]`
/// to the payload. Returns an error if the compressed output overflows
/// `u16::MAX`.
fn compress_one(input: &[u8], compressor: &fsst::Compressor, payload: &mut Vec<u8>) -> Result<()> {
    let compressed = compressor.compress(input);
    let len = compressed.len();
    if len > u16::MAX as usize {
        return Err(BqliteError::Execution(format!(
            "Fsst: compressed string is {len} bytes, exceeding the u16 \
             (65,535 byte) limit — the column must fall back to Plain"
        )));
    }
    payload.extend_from_slice(&(len as u16).to_le_bytes());
    payload.extend_from_slice(&compressed);
    Ok(())
}

/// Collect all strings from a String-typed Arrow array into byte slices.
/// Used only for the training path (which needs all strings up front).
fn collect_string_bytes(array: &dyn Array) -> Result<Vec<Vec<u8>>> {
    match array.data_type() {
        DataType::Utf8View => {
            let a = array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| {
                    BqliteError::Execution("Fsst: expected StringViewArray, downcast failed".into())
                })?;
            Ok((0..a.len())
                .map(|i| a.value(i).as_bytes().to_vec())
                .collect())
        }
        DataType::Utf8 => {
            let a = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    BqliteError::Execution("Fsst: expected StringArray, downcast failed".into())
                })?;
            Ok((0..a.len())
                .map(|i| a.value(i).as_bytes().to_vec())
                .collect())
        }
        other => Err(BqliteError::Execution(format!(
            "Fsst: unsupported Arrow type {other:?} — FSST covers Utf8/Utf8View only"
        ))),
    }
}

/// Total byte length of all strings in an array (for estimate_size).
fn total_string_byte_len(array: &dyn Array) -> Result<usize> {
    match array.data_type() {
        DataType::Utf8View => {
            let a = array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| {
                    BqliteError::Execution("Fsst: expected StringViewArray, downcast failed".into())
                })?;
            Ok((0..a.len()).map(|i| a.value(i).len()).sum())
        }
        DataType::Utf8 => {
            let a = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    BqliteError::Execution("Fsst: expected StringArray, downcast failed".into())
                })?;
            Ok((0..a.len()).map(|i| a.value(i).len()).sum())
        }
        other => Err(BqliteError::Execution(format!(
            "Fsst: unsupported Arrow type {other:?}"
        ))),
    }
}

/// Parse the self-contained params into (symbols, symbol_lengths).
fn parse_params(params: &[u8]) -> Result<(Vec<fsst::Symbol>, Vec<u8>)> {
    if params.len() < 2 {
        return Err(BqliteError::Corruption(
            "Fsst params too short — missing symbol_count".into(),
        ));
    }
    let symbol_count = u16::from_le_bytes(params[..2].try_into().unwrap()) as usize;
    let mut symbols = Vec::with_capacity(symbol_count);
    let mut lengths = Vec::with_capacity(symbol_count);

    let mut offset = 2;
    for i in 0..symbol_count {
        if offset >= params.len() {
            return Err(BqliteError::Corruption(format!(
                "Fsst params truncated at symbol {i}/{symbol_count}"
            )));
        }
        let sym_len = params[offset] as usize;
        offset += 1;
        if sym_len == 0 || sym_len > 8 {
            return Err(BqliteError::Corruption(format!(
                "Fsst symbol {i} has invalid length {sym_len} (must be 1..=8)"
            )));
        }
        if offset + sym_len > params.len() {
            return Err(BqliteError::Corruption(format!(
                "Fsst params truncated at symbol {i} bytes"
            )));
        }
        let mut sym_buf = [0u8; 8];
        sym_buf[..sym_len].copy_from_slice(&params[offset..offset + sym_len]);
        symbols.push(fsst::Symbol::from_slice(&sym_buf));
        lengths.push(sym_len as u8);
        offset += sym_len;
    }

    Ok((symbols, lengths))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fsst_round_trip_basic() {
        let strings = vec!["hello world", "hello there", "hello again"];
        let array: ArrayRef = Arc::new(StringViewArray::from(strings.clone()));
        let fsst = Fsst::train(array.as_ref()).unwrap();
        let chunk = fsst.encode(array.as_ref()).unwrap();
        assert_eq!(chunk.encoding, EncodingType::Fsst);
        assert_eq!(chunk.row_count, 3);
        let decoded = fsst.decode(&chunk, &BqlType::String).unwrap();
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn fsst_round_trip_empty() {
        let array: ArrayRef = Arc::new(StringViewArray::from(Vec::<&str>::new()));
        let fsst = Fsst::train(array.as_ref()).unwrap();
        let chunk = fsst.encode(array.as_ref()).unwrap();
        assert_eq!(chunk.row_count, 0);
        let decoded = fsst.decode(&chunk, &BqlType::String).unwrap();
        assert_eq!(decoded.len(), 0);
    }

    #[test]
    fn fsst_round_trip_empty_strings() {
        let strings = vec!["", "", ""];
        let array: ArrayRef = Arc::new(StringViewArray::from(strings));
        let fsst = Fsst::train(array.as_ref()).unwrap();
        let chunk = fsst.encode(array.as_ref()).unwrap();
        let decoded = fsst.decode(&chunk, &BqlType::String).unwrap();
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn fsst_round_trip_single_string() {
        let strings = vec!["a single string"];
        let array: ArrayRef = Arc::new(StringViewArray::from(strings));
        let fsst = Fsst::train(array.as_ref()).unwrap();
        let chunk = fsst.encode(array.as_ref()).unwrap();
        let decoded = fsst.decode(&chunk, &BqlType::String).unwrap();
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn fsst_round_trip_utf8_array() {
        let strings = vec!["foo", "bar", "baz"];
        let array: ArrayRef = Arc::new(StringArray::from(strings));
        let fsst = Fsst::train(array.as_ref()).unwrap();
        let chunk = fsst.encode(array.as_ref()).unwrap();
        // Decode always produces StringViewArray per bqlite convention.
        let decoded = fsst.decode(&chunk, &BqlType::String).unwrap();
        let decoded_view = decoded.as_any().downcast_ref::<StringViewArray>().unwrap();
        assert_eq!(decoded_view.value(0), "foo");
        assert_eq!(decoded_view.value(1), "bar");
        assert_eq!(decoded_view.value(2), "baz");
    }

    #[test]
    fn fsst_params_serialization_round_trip() {
        let strings = vec!["https://example.com/page1", "https://example.com/page2"];
        let array: ArrayRef = Arc::new(StringViewArray::from(strings));
        let fsst = Fsst::train(array.as_ref()).unwrap();
        let params = fsst.serialize_params();
        let reconstructed = Fsst::from_params(&params).unwrap();

        // Verify the reconstructed encoder produces the same output.
        let chunk1 = fsst.encode(array.as_ref()).unwrap();
        let chunk2 = reconstructed.encode(array.as_ref()).unwrap();
        assert_eq!(chunk1.payload, chunk2.payload);
    }

    #[test]
    fn fsst_applicable_to_string_only() {
        let strings = vec!["a"];
        let array: ArrayRef = Arc::new(StringViewArray::from(strings));
        let fsst = Fsst::train(array.as_ref()).unwrap();
        assert!(fsst.applicable_to(&BqlType::String));
        assert!(!fsst.applicable_to(&BqlType::Int));
        assert!(!fsst.applicable_to(&BqlType::Float));
        assert!(!fsst.applicable_to(&BqlType::Bool));
        assert!(!fsst.applicable_to(&BqlType::Timestamp));
    }

    #[test]
    fn fsst_decode_rejects_non_string_type() {
        let strings = vec!["a"];
        let array: ArrayRef = Arc::new(StringViewArray::from(strings));
        let fsst = Fsst::train(array.as_ref()).unwrap();
        let chunk = fsst.encode(array.as_ref()).unwrap();
        let err = fsst.decode(&chunk, &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => {
                assert!(msg.contains("FSST only supports String"));
            }
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn fsst_cross_decode_from_params() {
        // Encode with one Fsst instance, decode by reconstructing from
        // the chunk's params — simulates what the segment reader does.
        let strings = vec![
            "user_agent: Mozilla/5.0",
            "user_agent: Chrome/120",
            "user_agent: Safari/17",
        ];
        let array: ArrayRef = Arc::new(StringViewArray::from(strings));
        let encoder = Fsst::train(array.as_ref()).unwrap();
        let chunk = encoder.encode(array.as_ref()).unwrap();

        // Reconstruct decoder from params only (no access to original Fsst).
        let decoder = Fsst::from_params(&chunk.params).unwrap();
        let decoded = decoder.decode(&chunk, &BqlType::String).unwrap();
        assert_eq!(decoded.as_ref(), array.as_ref());
    }
}
