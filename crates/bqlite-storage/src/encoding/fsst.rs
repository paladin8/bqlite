//! FSST (Fast Static Symbol Table) string compression.
//!
//! FSST builds a symbol table of common substrings and re-encodes each
//! string by replacing substring occurrences with single-byte codes.
//! The symbol table is stored once per segment and shared by all row
//! groups in the future v2 segment layout.
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
//! strings payload. That layout is what TASK-419 will eventually write
//! to disk.
//!
//! The [`Encoding`] trait operates on **self-contained**
//! [`EncodedChunk`]s so that round-trip property tests hold without a
//! separate segment-level store. To honor both constraints, the
//! trait-level chunk produced here inlines the serialized FSST symbol
//! table directly into `params`. The segment writer can later hoist
//! those bytes to the segment-level FSST region and rewrite the
//! per-chunk params to the `symbol_table_id` shape the v2 spec
//! mandates.
//!
//! # Params layout (self-contained form)
//!
//! `params` is the exact fixed-size symbol table blob exported by the
//! `fsst` crate. It is always `FSST_SYMBOL_TABLE_SIZE` bytes and
//! carries:
//!
//! - the crate's `FSST` magic/version header
//! - whether the crate elected to enable compression or pass the input
//!   through unchanged for very small inputs
//! - the symbol bytes and their lengths
//!
//! Keeping the crate-native blob intact avoids losing the
//! "encoder-switch" flag, which the crate uses to fall back to raw
//! bytes for small inputs.
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
//! original string. When the underlying crate chooses its small-input
//! passthrough path, `compressed_bytes` is simply the raw UTF-8 bytes
//! for that string; the serialized symbol-table blob in `params`
//! preserves that choice so decode remains correct.
//!
//! # State model
//!
//! The `fsst` crate exposes a one-shot `compress` / `decompress` API
//! keyed by a serialized symbol-table blob. It does **not** currently
//! expose a public "train once, then reuse this symbol table for many
//! encode calls" surface. Because bqlite's selector does not yet pick
//! FSST in the production writer path, this wrapper treats [`Fsst`] as
//! a lightweight adapter: `encode` builds a fresh self-contained chunk
//! for the provided array, and `decode` uses the authoritative symbol
//! table bytes carried in that chunk's `params`.
//!
//! `train` and `from_params` remain as convenience constructors for
//! tests and future read-path integration, but decode correctness is
//! driven by the chunk params, not hidden encoder state.
//!
//! # Null handling
//!
//! FSST operates on dense arrays — the writer strips nulls into a
//! separate bitmap before encoding. A nullable input is a contract
//! violation caught by [`super::require_dense`].
//!
//! # Edge cases
//!
//! - `row_count == 0`: legal. `encode` returns an empty payload and a
//!   valid serialized symbol-table blob with the crate's passthrough
//!   flag disabled.
//! - Very small inputs: legal. The crate falls back to raw bytes below
//!   its threshold; decode remains correct because the serialized
//!   symbol-table blob in `params` records that choice.
//! - All strings empty: legal. Each `compressed_len` is zero.
//! - Strings shorter than any useful symbol: legal. FSST may emit
//!   escape-prefixed literals or choose the passthrough path.
//! - No beneficial symbols: legal. The selector guard in TASK-419
//!   catches this; `Fsst::encode` itself always produces valid output.
//!
//! # Selector guard
//!
//! Per `advanced-encodings.md` §7.7: choose FSST when
//! `cardinality / row_count ≥ 0.3` (high cardinality) and the column
//! is String-typed. The guard lives in the selector (TASK-419), not
//! here. [`Fsst::estimate_size`] returns a conservative upper bound
//! (2× input size) so the selector can compare accurately.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringArray, StringViewArray, StringViewBuilder};
use arrow::datatypes::DataType;
use bqlite_core::{BqlType, BqliteError, Result};
use fsst::fsst::{
    compress as fsst_compress, decompress as fsst_decompress, FSST_SYMBOL_TABLE_SIZE,
};

use super::{require_dense, BorrowedEncodedChunk, EncodedChunk, Encoding, EncodingType};

/// FSST encoding adapter.
///
/// The `fsst` crate's public API is one-shot and chunk-oriented, so
/// this wrapper carries no mutable compression state of its own.
#[derive(Debug, Clone, Copy, Default)]
pub struct Fsst;

impl Fsst {
    /// Validate that `array` is a supported String array and that the
    /// underlying crate can compress it successfully.
    pub fn train(array: &dyn Array) -> Result<Self> {
        let _ = encode_chunk(array)?;
        Ok(Self)
    }

    /// Validate a serialized FSST symbol-table blob.
    pub fn from_symbol_table(symbol_table: &[u8]) -> Result<Self> {
        validate_params(symbol_table)?;
        Ok(Self)
    }

    /// Validate a self-contained params block.
    pub fn from_params(params: &[u8]) -> Result<Self> {
        Self::from_symbol_table(params)
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
        // every byte escaped). The `fsst` crate also has a small-input
        // passthrough mode, which still fits inside this bound.
        let total_string_bytes = total_string_byte_len(array)?;
        let row_count = array.len();
        Ok(row_count * 2 + total_string_bytes * 2)
    }

    fn encode(&self, array: &dyn Array) -> Result<EncodedChunk> {
        require_dense(array, "Fsst")?;
        encode_chunk(array)
    }

    fn decode(&self, chunk: &EncodedChunk, ty: &BqlType) -> Result<ArrayRef> {
        decode_from_params(&chunk.params, &chunk.payload, chunk.row_count, ty)
    }

    fn decode_borrowed(&self, chunk: BorrowedEncodedChunk<'_>, ty: &BqlType) -> Result<ArrayRef> {
        decode_from_params(chunk.params, chunk.payload, chunk.row_count, ty)
    }
}

/// Number of FSST symbols recorded in a serialized symbol-table blob.
///
/// The `fsst` crate encodes `(header & 0xFF)` as the symbol count in
/// the first byte of the blob. When the encoder took the small-input
/// passthrough path the blob still carries this count so decoders can
/// reconstruct the table; TASK-419 uses this value to populate the
/// `symbol_count` field of a segment-level [`FsstSymbolTableRef`].
///
/// bqlite's on-disk segment format is little-endian throughout (see
/// `segment-format-v1.md` §4), so the header is read with
/// [`u64::from_le_bytes`] — matching the byte order the `fsst` crate
/// produces on the little-endian hosts bqlite targets.
///
/// Returns `0` when the blob is shorter than the 8-byte header —
/// callers should first validate the blob with [`Fsst::from_params`].
pub fn fsst_symbol_count_from_bytes(bytes: &[u8]) -> u16 {
    if bytes.len() < 8 {
        return 0;
    }
    let header = u64::from_le_bytes(bytes[..8].try_into().expect("8 bytes"));
    (header & 0xFF) as u16
}

/// Decode from a self-contained params block plus payload.
pub fn decode_from_params(
    params: &[u8],
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

    validate_params(params)?;

    let (compressed_values, compressed_offsets) = unpack_payload(payload, row_count)?;
    let mut decoded_values = vec![0u8; decoded_size_from_symbol_table(params, &compressed_values)?];
    let mut decoded_offsets = vec![0i32; compressed_offsets.len()];
    fsst_decompress(
        params,
        &compressed_values,
        &compressed_offsets,
        &mut decoded_values,
        &mut decoded_offsets,
    )
    .map_err(|e| BqliteError::Corruption(format!("Fsst::decode: {e}")))?;

    let mut builder = StringViewBuilder::with_capacity(row_count);
    for window in decoded_offsets.windows(2) {
        let start = usize::try_from(window[0]).map_err(|_| {
            BqliteError::Corruption("Fsst::decode: decoded offsets must be non-negative".into())
        })?;
        let end = usize::try_from(window[1]).map_err(|_| {
            BqliteError::Corruption("Fsst::decode: decoded offsets must be non-negative".into())
        })?;
        let s = std::str::from_utf8(&decoded_values[start..end]).map_err(|e| {
            BqliteError::Corruption(format!(
                "Fsst::decode: decoded bytes are not valid UTF-8: {e}"
            ))
        })?;
        builder.append_value(s);
    }

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn encode_chunk(array: &dyn Array) -> Result<EncodedChunk> {
    let row_count = array.len();
    let (values, offsets) = flatten_string_values(array)?;
    let (params, compressed_values, compressed_offsets) = compress_values(&values, &offsets)?;
    let payload = pack_payload(&compressed_values, &compressed_offsets)?;

    Ok(EncodedChunk {
        encoding: EncodingType::Fsst,
        params,
        payload,
        row_count,
    })
}

fn compress_values(values: &[u8], offsets: &[i32]) -> Result<(Vec<u8>, Vec<u8>, Vec<i32>)> {
    let mut symbol_table = vec![0u8; FSST_SYMBOL_TABLE_SIZE];
    let mut compressed_values = vec![0u8; values.len()];
    let mut compressed_offsets = vec![0i32; offsets.len()];

    fsst_compress(
        &mut symbol_table,
        values,
        offsets,
        &mut compressed_values,
        &mut compressed_offsets,
    )
    .map_err(|e| BqliteError::Execution(format!("Fsst::encode: {e}")))?;

    Ok((symbol_table, compressed_values, compressed_offsets))
}

fn flatten_string_values(array: &dyn Array) -> Result<(Vec<u8>, Vec<i32>)> {
    match array.data_type() {
        DataType::Utf8View => {
            let a = array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| {
                    BqliteError::Execution("Fsst: expected StringViewArray, downcast failed".into())
                })?;

            let mut offsets = Vec::with_capacity(a.len() + 1);
            let mut values = Vec::new();
            offsets.push(0);
            for i in 0..a.len() {
                values.extend_from_slice(a.value(i).as_bytes());
                offsets.push(i32::try_from(values.len()).map_err(|_| {
                    BqliteError::Execution("Fsst: value buffer exceeds i32 offset space".into())
                })?);
            }
            Ok((values, offsets))
        }
        DataType::Utf8 => {
            let a = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    BqliteError::Execution("Fsst: expected StringArray, downcast failed".into())
                })?;

            let mut offsets = Vec::with_capacity(a.len() + 1);
            let mut values = Vec::new();
            offsets.push(0);
            for i in 0..a.len() {
                values.extend_from_slice(a.value(i).as_bytes());
                offsets.push(i32::try_from(values.len()).map_err(|_| {
                    BqliteError::Execution("Fsst: value buffer exceeds i32 offset space".into())
                })?);
            }
            Ok((values, offsets))
        }
        other => Err(BqliteError::Execution(format!(
            "Fsst: unsupported Arrow type {other:?} — FSST covers Utf8/Utf8View only"
        ))),
    }
}

fn pack_payload(compressed_values: &[u8], compressed_offsets: &[i32]) -> Result<Vec<u8>> {
    let mut payload = Vec::with_capacity(compressed_values.len() + compressed_offsets.len() * 2);
    for window in compressed_offsets.windows(2) {
        let start = usize::try_from(window[0]).map_err(|_| {
            BqliteError::Execution("Fsst: compressed offsets must be non-negative".into())
        })?;
        let end = usize::try_from(window[1]).map_err(|_| {
            BqliteError::Execution("Fsst: compressed offsets must be non-negative".into())
        })?;
        let len = end - start;
        if len > u16::MAX as usize {
            return Err(BqliteError::Execution(format!(
                "Fsst: compressed string is {len} bytes, exceeding the u16 \
                 (65,535 byte) limit — the column must fall back to Plain"
            )));
        }
        payload.extend_from_slice(&(len as u16).to_le_bytes());
        payload.extend_from_slice(&compressed_values[start..end]);
    }
    Ok(payload)
}

fn unpack_payload(payload: &[u8], row_count: usize) -> Result<(Vec<u8>, Vec<i32>)> {
    let mut compressed_values = Vec::new();
    let mut compressed_offsets = Vec::with_capacity(row_count + 1);
    compressed_offsets.push(0);

    let mut offset = 0usize;
    for row in 0..row_count {
        if offset + 2 > payload.len() {
            return Err(BqliteError::Corruption(format!(
                "Fsst::decode: payload too short — missing compressed_len for row {row}"
            )));
        }
        let len = u16::from_le_bytes(payload[offset..offset + 2].try_into().unwrap()) as usize;
        offset += 2;

        if offset + len > payload.len() {
            return Err(BqliteError::Corruption(format!(
                "Fsst::decode: payload too short — row {row} needs {len} bytes at offset \
                 {offset} but only {} remain",
                payload.len() - offset
            )));
        }

        compressed_values.extend_from_slice(&payload[offset..offset + len]);
        compressed_offsets.push(i32::try_from(compressed_values.len()).map_err(|_| {
            BqliteError::Corruption("Fsst::decode: compressed payload exceeds i32 offsets".into())
        })?);
        offset += len;
    }

    if offset != payload.len() {
        return Err(BqliteError::Corruption(format!(
            "Fsst::decode: payload has {} trailing bytes",
            payload.len() - offset
        )));
    }

    Ok((compressed_values, compressed_offsets))
}

fn validate_params(params: &[u8]) -> Result<()> {
    if params.len() != FSST_SYMBOL_TABLE_SIZE {
        return Err(BqliteError::Corruption(format!(
            "Fsst params must be exactly {FSST_SYMBOL_TABLE_SIZE} bytes, got {}",
            params.len()
        )));
    }
    Ok(())
}

fn decoded_size_from_symbol_table(params: &[u8], compressed_values: &[u8]) -> Result<usize> {
    let header = u64::from_ne_bytes(params[..8].try_into().unwrap());
    let encoder_switch = (header & (1 << 24)) != 0;
    if !encoder_switch {
        return Ok(compressed_values.len());
    }

    let symbol_count = (header & 0xFF) as usize;
    let lens_start = 8 + symbol_count * 8;
    if lens_start + symbol_count > params.len() {
        return Err(BqliteError::Corruption(format!(
            "Fsst params truncated: symbol_count {symbol_count} exceeds params length {}",
            params.len()
        )));
    }

    let mut lens = [0u8; 256];
    lens[..symbol_count].copy_from_slice(&params[lens_start..lens_start + symbol_count]);

    let mut decoded_size = 0usize;
    let mut i = 0usize;
    while i < compressed_values.len() {
        let code = compressed_values[i];
        if code == 255 {
            if i + 1 >= compressed_values.len() {
                return Err(BqliteError::Corruption(
                    "Fsst::decode: payload ends with dangling escape byte".into(),
                ));
            }
            decoded_size += 1;
            i += 2;
            continue;
        }

        let len = lens[code as usize] as usize;
        if len == 0 {
            return Err(BqliteError::Corruption(format!(
                "Fsst::decode: code {code} has no symbol length in params"
            )));
        }
        decoded_size += len;
        i += 1;
    }

    Ok(decoded_size)
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
        assert_eq!(chunk.params.len(), FSST_SYMBOL_TABLE_SIZE);
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
    fn fsst_round_trip_utf8_array() {
        let strings = vec!["foo", "bar", "baz"];
        let array: ArrayRef = Arc::new(StringArray::from(strings));
        let fsst = Fsst::train(array.as_ref()).unwrap();
        let chunk = fsst.encode(array.as_ref()).unwrap();
        let decoded = fsst.decode(&chunk, &BqlType::String).unwrap();
        let decoded_view = decoded.as_any().downcast_ref::<StringViewArray>().unwrap();
        assert_eq!(decoded_view.value(0), "foo");
        assert_eq!(decoded_view.value(1), "bar");
        assert_eq!(decoded_view.value(2), "baz");
    }

    #[test]
    fn fsst_cross_decode_from_params() {
        let strings = vec![
            "user_agent: Mozilla/5.0",
            "user_agent: Chrome/120",
            "user_agent: Safari/17",
        ];
        let array: ArrayRef = Arc::new(StringViewArray::from(strings));
        let encoder = Fsst::train(array.as_ref()).unwrap();
        let chunk = encoder.encode(array.as_ref()).unwrap();

        let decoder = Fsst::from_params(&chunk.params).unwrap();
        let decoded = decoder.decode(&chunk, &BqlType::String).unwrap();
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn fsst_small_input_round_trips() {
        let strings = vec!["a", "bb", "ccc", "dddd"];
        let array: ArrayRef = Arc::new(StringViewArray::from(strings));
        let fsst = Fsst::train(array.as_ref()).unwrap();
        let chunk = fsst.encode(array.as_ref()).unwrap();
        let decoded = fsst.decode(&chunk, &BqlType::String).unwrap();
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn fsst_applicable_to_string_only() {
        let fsst = Fsst;
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
    fn fsst_from_params_rejects_bad_length() {
        let err = Fsst::from_params(&[1, 2, 3]).unwrap_err();
        match err {
            BqliteError::Corruption(msg) => {
                assert!(msg.contains("exactly"));
            }
            other => panic!("expected Corruption error, got {other:?}"),
        }
    }
}
