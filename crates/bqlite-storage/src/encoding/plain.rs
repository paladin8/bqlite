//! Plain encoding — the zero-overhead reference implementation.
//!
//! "Plain" is the identity encoding: Arrow buffers go straight to
//! little-endian bytes on disk with no compression, dictionary, or
//! delta trickery. Every other v1 encoding falls back to Plain when
//! its own heuristics rule it out, so Plain is the **correctness
//! baseline** against which all other encodings are property-tested.
//!
//! The byte layout for each [`BqlType`] is pinned in
//! `docs/design/storage/segment-format-v1.md` §9.1. This module is
//! the runtime counterpart to that spec; any divergence between the
//! two is a design-doc bug that must be fixed in lock-step with the
//! code.
//!
//! # Type support
//!
//! Plain covers every primitive type the v1 segment format supports:
//!
//! | [`BqlType`] | Arrow array on encode input | Arrow array on decode output | On-disk payload |
//! |---|---|---|---|
//! | `Bool` | [`BooleanArray`] | [`BooleanArray`] | LSB-first bit-packed bitmap, `ceil(row_count / 8)` bytes |
//! | `Int` | [`Int64Array`] | [`Int64Array`] | `8 * row_count` bytes, i64 little-endian |
//! | `Float` | [`Float64Array`] | [`Float64Array`] | `8 * row_count` bytes, IEEE 754 f64 little-endian |
//! | `Timestamp` | [`TimestampNanosecondArray`] (any timezone) | [`TimestampNanosecondArray`] with `"UTC"` stamp | `8 * row_count` bytes, i64 little-endian nanoseconds |
//! | `String` | [`StringArray`] (`Utf8`) **or** [`StringViewArray`] (`Utf8View`) | [`StringViewArray`] (`Utf8View`) | `[u32 LE length][utf8 bytes]` per value |
//!
//! The type system's canonical Arrow representation for `BqlType::String`
//! is `Utf8View` (see `bqlite_core::arrow::bql_type_to_arrow` and
//! `property.rs` line 35). `Plain::encode` accepts both `Utf8` and
//! `Utf8View` on input — intermediate code in test fixtures and
//! early-Wave-2 experiments may hand in either — but `Plain::decode`
//! always produces `StringViewArray` so downstream operators see the
//! schema-canonical type. The on-disk byte layout is identical between
//! the two input shapes; the difference is a cheap in-memory reshape
//! at the decode boundary.
//!
//! Nested types (`List`, `Map`) are deferred — the selector routes
//! nested columns through Plain, but v1 does not yet implement their
//! byte layout. TASK-206 scopes to primitive types; later Wave 2+
//! tasks revisit nested.
//!
//! # Null handling
//!
//! Plain encodes only non-null values; the null bitmap is a separate
//! prefix of the column chunk emitted by the writer. Every method on
//! [`Plain`] treats a nullable input as a contract violation — see
//! `super::require_dense` and the module-level docs on
//! [`super::Encoding`].

use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBufferBuilder, Float64Array, Int64Array, StringArray,
    StringViewArray, StringViewBuilder, TimestampNanosecondArray,
};
use arrow::buffer::BooleanBuffer;
use arrow::datatypes::{DataType, TimeUnit};
use bqlite_core::{BqlType, BqliteError, Result};
use std::sync::Arc;

use super::{require_dense, BorrowedEncodedChunk, EncodedChunk, Encoding, EncodingType};

/// Zero-sized marker for the Plain encoding.
///
/// Stateless; can be freely copied and stored behind a
/// `Box<dyn Encoding>`. See the module-level documentation for the
/// byte layouts this impl produces.
#[derive(Debug, Clone, Copy, Default)]
pub struct Plain;

impl Plain {
    /// Construct a new Plain encoder. Plain has no configuration —
    /// this is sugar for `Plain` the unit struct.
    pub const fn new() -> Self {
        Plain
    }
}

const I64_BYTE_WIDTH: usize = 8;
const F64_BYTE_WIDTH: usize = 8;
const TIMESTAMP_BYTE_WIDTH: usize = 8;
const STRING_LENGTH_PREFIX: usize = 4;

impl Encoding for Plain {
    fn encoding_type(&self) -> EncodingType {
        EncodingType::Plain
    }

    fn applicable_to(&self, ty: &BqlType) -> bool {
        // v1 Plain covers every primitive scalar. Nested types are
        // deferred; see module docs.
        matches!(
            ty,
            BqlType::Bool | BqlType::Int | BqlType::Float | BqlType::String | BqlType::Timestamp
        )
    }

    fn estimate_size(&self, array: &dyn Array) -> Result<usize> {
        let len = array.len();
        match array.data_type() {
            DataType::Boolean => Ok(bitmap_byte_count(len)),
            DataType::Int64 => Ok(I64_BYTE_WIDTH * len),
            DataType::Float64 => Ok(F64_BYTE_WIDTH * len),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => Ok(TIMESTAMP_BYTE_WIDTH * len),
            DataType::Utf8 => {
                // Variable-width: sum the UTF-8 byte counts of every
                // value and add a 4-byte length prefix per value. An
                // over-estimate is legal (module docs), but this is
                // exact — the selector benefits from exact strings.
                let string_array = downcast::<StringArray>(array, "StringArray")?;
                let payload: usize = (0..string_array.len())
                    .map(|i| STRING_LENGTH_PREFIX + string_array.value(i).len())
                    .sum();
                Ok(payload)
            }
            DataType::Utf8View => {
                let string_array = downcast::<StringViewArray>(array, "StringViewArray")?;
                let payload: usize = (0..string_array.len())
                    .map(|i| STRING_LENGTH_PREFIX + string_array.value(i).len())
                    .sum();
                Ok(payload)
            }
            DataType::List(_) | DataType::Map(_, _) => Err(BqliteError::Execution(format!(
                "Plain::estimate_size: nested type {:?} is deferred — v1 primitive \
                 Plain encoding covers Bool, Int64, Float64, Timestamp(Nanosecond), \
                 Utf8, and Utf8View only; List and Map land in a later Wave 2 task",
                array.data_type()
            ))),
            other => Err(BqliteError::Execution(format!(
                "Plain::estimate_size does not support Arrow type {other:?} — \
                 v1 Plain encoding covers Bool, Int64, Float64, \
                 Timestamp(Nanosecond), Utf8, and Utf8View only"
            ))),
        }
    }

    fn encode(&self, array: &dyn Array) -> Result<EncodedChunk> {
        require_dense(array, "Plain")?;
        let row_count = array.len();
        let payload = match array.data_type() {
            DataType::Boolean => encode_bool(downcast::<BooleanArray>(array, "BooleanArray")?),
            DataType::Int64 => {
                encode_i64_le(downcast::<Int64Array>(array, "Int64Array")?.values().iter())
            }
            DataType::Float64 => encode_f64_le(
                downcast::<Float64Array>(array, "Float64Array")?
                    .values()
                    .iter(),
            ),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => encode_i64_le(
                downcast::<TimestampNanosecondArray>(array, "TimestampNanosecondArray")?
                    .values()
                    .iter(),
            ),
            DataType::Utf8 => encode_utf8(downcast::<StringArray>(array, "StringArray")?)?,
            DataType::Utf8View => {
                encode_utf8_view(downcast::<StringViewArray>(array, "StringViewArray")?)?
            }
            DataType::List(_) | DataType::Map(_, _) => {
                return Err(BqliteError::Execution(format!(
                    "Plain::encode: nested type {:?} is deferred — v1 primitive \
                     Plain encoding covers Bool, Int64, Float64, \
                     Timestamp(Nanosecond), Utf8, and Utf8View only; List and \
                     Map land in a later Wave 2 task",
                    array.data_type()
                )));
            }
            other => {
                return Err(BqliteError::Execution(format!(
                    "Plain::encode does not support Arrow type {other:?} — \
                     v1 Plain encoding covers Bool, Int64, Float64, \
                     Timestamp(Nanosecond), Utf8, and Utf8View only"
                )));
            }
        };
        Ok(EncodedChunk {
            encoding: EncodingType::Plain,
            params: Vec::new(),
            payload,
            row_count,
        })
    }

    fn decode(&self, chunk: &EncodedChunk, ty: &BqlType) -> Result<ArrayRef> {
        decode_impl(
            chunk.encoding,
            &chunk.params,
            &chunk.payload,
            chunk.row_count,
            ty,
        )
    }

    fn decode_borrowed(&self, chunk: BorrowedEncodedChunk<'_>, ty: &BqlType) -> Result<ArrayRef> {
        decode_impl(
            chunk.encoding,
            chunk.params,
            chunk.payload,
            chunk.row_count,
            ty,
        )
    }
}

fn decode_impl(
    encoding: EncodingType,
    params: &[u8],
    payload: &[u8],
    row_count: usize,
    ty: &BqlType,
) -> Result<ArrayRef> {
    if encoding != EncodingType::Plain {
        return Err(BqliteError::Execution(format!(
            "Plain::decode called on a {:?} chunk — dispatch must \
             route each chunk to its declared encoding's decoder",
            encoding
        )));
    }
    if !params.is_empty() {
        return Err(BqliteError::Execution(format!(
            "Plain::decode expects an empty params block per \
             segment-format-v1.md §9.1, got {} bytes",
            params.len()
        )));
    }
    match ty {
        BqlType::Bool => Ok(Arc::new(decode_bool(payload, row_count)?) as ArrayRef),
        BqlType::Int => Ok(Arc::new(decode_i64(payload, row_count)?) as ArrayRef),
        BqlType::Float => Ok(Arc::new(decode_f64(payload, row_count)?) as ArrayRef),
        BqlType::Timestamp => Ok(Arc::new(decode_timestamp(payload, row_count)?) as ArrayRef),
        BqlType::String => {
            // Produce the schema-canonical `StringViewArray`
            // (`Utf8View`) so downstream operators see the same
            // Arrow type `bqlite_core::arrow::bql_type_to_arrow`
            // declares for `BqlType::String`.
            Ok(Arc::new(decode_utf8_view(payload, row_count)?) as ArrayRef)
        }
        BqlType::List(_) | BqlType::Map(_) => Err(BqliteError::Execution(format!(
            "Plain::decode does not yet support nested type {ty} — \
             List and Map decode lands in a later Wave 2 task"
        ))),
    }
}

// ── encode helpers ───────────────────────────────────────────────────────────

fn bitmap_byte_count(row_count: usize) -> usize {
    row_count.div_ceil(8)
}

fn encode_bool(array: &BooleanArray) -> Vec<u8> {
    let row_count = array.len();
    let byte_count = bitmap_byte_count(row_count);
    let mut bytes = vec![0u8; byte_count];
    for i in 0..row_count {
        if array.value(i) {
            bytes[i / 8] |= 1 << (i % 8);
        }
    }
    bytes
}

fn encode_i64_le<'a, I>(values: I) -> Vec<u8>
where
    I: ExactSizeIterator<Item = &'a i64>,
{
    let mut bytes = Vec::with_capacity(I64_BYTE_WIDTH * values.len());
    for v in values {
        bytes.extend_from_slice(&v.to_le_bytes());
    }
    bytes
}

fn encode_f64_le<'a, I>(values: I) -> Vec<u8>
where
    I: ExactSizeIterator<Item = &'a f64>,
{
    let mut bytes = Vec::with_capacity(F64_BYTE_WIDTH * values.len());
    for v in values {
        bytes.extend_from_slice(&v.to_le_bytes());
    }
    bytes
}

fn encode_utf8(array: &StringArray) -> Result<Vec<u8>> {
    let total_bytes: usize = (0..array.len())
        .map(|i| STRING_LENGTH_PREFIX + array.value(i).len())
        .sum();
    let mut bytes = Vec::with_capacity(total_bytes);
    for i in 0..array.len() {
        let s = array.value(i);
        push_length_prefixed_utf8(&mut bytes, i, s)?;
    }
    Ok(bytes)
}

fn encode_utf8_view(array: &StringViewArray) -> Result<Vec<u8>> {
    let total_bytes: usize = (0..array.len())
        .map(|i| STRING_LENGTH_PREFIX + array.value(i).len())
        .sum();
    let mut bytes = Vec::with_capacity(total_bytes);
    for i in 0..array.len() {
        let s = array.value(i);
        push_length_prefixed_utf8(&mut bytes, i, s)?;
    }
    Ok(bytes)
}

/// Shared length-prefixed UTF-8 writer. Enforces the v1 per-value
/// length cap (u32::MAX bytes — see segment-format-v1.md §9.1
/// non-blocking note) as a typed error rather than a panic, matching
/// the "all errors must be typed and recoverable" convention from
/// `CLAUDE.md`.
fn push_length_prefixed_utf8(out: &mut Vec<u8>, idx: usize, s: &str) -> Result<()> {
    if s.len() > u32::MAX as usize {
        return Err(BqliteError::Execution(format!(
            "Plain::encode: string value at index {idx} is {} bytes, \
             exceeding the v1 per-value cap of u32::MAX ({} bytes)",
            s.len(),
            u32::MAX
        )));
    }
    out.extend_from_slice(&(s.len() as u32).to_le_bytes());
    out.extend_from_slice(s.as_bytes());
    Ok(())
}

// ── decode helpers ───────────────────────────────────────────────────────────

fn decode_bool(payload: &[u8], row_count: usize) -> Result<BooleanArray> {
    let expected = bitmap_byte_count(row_count);
    if payload.len() != expected {
        return Err(BqliteError::Execution(format!(
            "Plain::decode Bool: expected {expected} payload bytes for {row_count} rows, got {}",
            payload.len()
        )));
    }
    let mut builder = BooleanBufferBuilder::new(row_count);
    for i in 0..row_count {
        let bit = (payload[i / 8] >> (i % 8)) & 1 != 0;
        builder.append(bit);
    }
    let buffer: BooleanBuffer = builder.finish();
    Ok(BooleanArray::new(buffer, None))
}

fn decode_i64(payload: &[u8], row_count: usize) -> Result<Int64Array> {
    let expected = I64_BYTE_WIDTH * row_count;
    if payload.len() != expected {
        return Err(BqliteError::Execution(format!(
            "Plain::decode Int: expected {expected} payload bytes for {row_count} rows, got {}",
            payload.len()
        )));
    }
    let mut values = Vec::with_capacity(row_count);
    for chunk in payload.chunks_exact(I64_BYTE_WIDTH) {
        // chunk length is guaranteed to be 8 by the size check above,
        // so the unwrap is infallible.
        values.push(i64::from_le_bytes(chunk.try_into().unwrap()));
    }
    Ok(Int64Array::from(values))
}

fn decode_f64(payload: &[u8], row_count: usize) -> Result<Float64Array> {
    let expected = F64_BYTE_WIDTH * row_count;
    if payload.len() != expected {
        return Err(BqliteError::Execution(format!(
            "Plain::decode Float: expected {expected} payload bytes for {row_count} rows, got {}",
            payload.len()
        )));
    }
    let mut values = Vec::with_capacity(row_count);
    for chunk in payload.chunks_exact(F64_BYTE_WIDTH) {
        values.push(f64::from_le_bytes(chunk.try_into().unwrap()));
    }
    Ok(Float64Array::from(values))
}

fn decode_timestamp(payload: &[u8], row_count: usize) -> Result<TimestampNanosecondArray> {
    let expected = TIMESTAMP_BYTE_WIDTH * row_count;
    if payload.len() != expected {
        return Err(BqliteError::Execution(format!(
            "Plain::decode Timestamp: expected {expected} payload bytes for {row_count} rows, got {}",
            payload.len()
        )));
    }
    let mut values = Vec::with_capacity(row_count);
    for chunk in payload.chunks_exact(TIMESTAMP_BYTE_WIDTH) {
        values.push(i64::from_le_bytes(chunk.try_into().unwrap()));
    }
    // Stamp the resulting array with the canonical UTC timezone per
    // property.rs BqlType::Timestamp → Arrow Timestamp(Nanosecond, "UTC").
    let array = TimestampNanosecondArray::from(values).with_timezone("UTC");
    Ok(array)
}

fn decode_utf8_view(payload: &[u8], row_count: usize) -> Result<StringViewArray> {
    let mut builder = StringViewBuilder::with_capacity(row_count);
    let mut offset = 0usize;
    for i in 0..row_count {
        // Length prefix.
        if offset + STRING_LENGTH_PREFIX > payload.len() {
            return Err(BqliteError::Execution(format!(
                "Plain::decode String: payload truncated while reading length \
                 prefix for value {i}/{row_count} at offset {offset}"
            )));
        }
        let len = u32::from_le_bytes(
            payload[offset..offset + STRING_LENGTH_PREFIX]
                .try_into()
                .unwrap(),
        ) as usize;
        offset += STRING_LENGTH_PREFIX;
        // UTF-8 bytes.
        if offset + len > payload.len() {
            return Err(BqliteError::Execution(format!(
                "Plain::decode String: payload truncated while reading {len}-byte \
                 value {i}/{row_count} at offset {offset}"
            )));
        }
        let bytes = &payload[offset..offset + len];
        offset += len;
        let s = std::str::from_utf8(bytes).map_err(|e| {
            BqliteError::Execution(format!(
                "Plain::decode String: invalid UTF-8 at value {i}/{row_count}: {e}"
            ))
        })?;
        builder.append_value(s);
    }
    if offset != payload.len() {
        return Err(BqliteError::Execution(format!(
            "Plain::decode String: {} trailing bytes after reading {row_count} values",
            payload.len() - offset
        )));
    }
    Ok(builder.finish())
}

// ── shared downcast helper ──────────────────────────────────────────────────

fn downcast<'a, T: 'static>(array: &'a dyn Array, expected: &'static str) -> Result<&'a T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        BqliteError::Execution(format!(
            "Plain encoding: expected Arrow array of type {expected}, \
             got data_type = {:?}",
            array.data_type()
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        BooleanArray, Float64Array, Int64Array, StringArray, TimestampNanosecondArray,
    };

    fn round_trip(array: ArrayRef, ty: BqlType) -> ArrayRef {
        let plain = Plain;
        let chunk = plain.encode(array.as_ref()).unwrap();
        plain.decode(&chunk, &ty).unwrap()
    }

    #[test]
    fn applicable_to_covers_every_primitive() {
        let p = Plain;
        assert!(p.applicable_to(&BqlType::Bool));
        assert!(p.applicable_to(&BqlType::Int));
        assert!(p.applicable_to(&BqlType::Float));
        assert!(p.applicable_to(&BqlType::String));
        assert!(p.applicable_to(&BqlType::Timestamp));
        assert!(!p.applicable_to(&BqlType::List(Box::new(BqlType::Int))));
        assert!(!p.applicable_to(&BqlType::Map(Box::new(BqlType::String))));
    }

    #[test]
    fn encoding_type_is_plain_discriminant_zero() {
        assert_eq!(Plain.encoding_type(), EncodingType::Plain);
        assert_eq!(Plain.encoding_type().discriminant(), 0);
    }

    #[test]
    fn int_round_trip_example() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![0, 1, -1, i64::MIN, i64::MAX, 42]));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn float_round_trip_example() {
        let array: ArrayRef = Arc::new(Float64Array::from(vec![
            0.0_f64,
            -0.0,
            1.0,
            std::f64::consts::PI,
            f64::MIN_POSITIVE,
            f64::MAX,
        ]));
        let decoded = round_trip(array.clone(), BqlType::Float);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn bool_round_trip_covers_byte_boundary() {
        // 17 values exercise: within-byte packing, the byte boundary
        // at index 8, and the trailing partial byte (9th onwards).
        let pattern: Vec<bool> = (0..17).map(|i| i % 3 != 0).collect();
        let array: ArrayRef = Arc::new(BooleanArray::from(pattern.clone()));
        let decoded = round_trip(array, BqlType::Bool);
        let decoded_bool = decoded.as_any().downcast_ref::<BooleanArray>().unwrap();
        let decoded_pattern: Vec<bool> = (0..decoded_bool.len())
            .map(|i| decoded_bool.value(i))
            .collect();
        assert_eq!(decoded_pattern, pattern);
    }

    #[test]
    fn bool_round_trip_empty() {
        let array: ArrayRef = Arc::new(BooleanArray::from(Vec::<bool>::new()));
        let decoded = round_trip(array.clone(), BqlType::Bool);
        assert_eq!(decoded.len(), 0);
    }

    #[test]
    fn string_round_trip_accepts_utf8_input_and_produces_utf8_view() {
        // StringArray (Utf8) input path — early Wave 2 fixtures may
        // hand in either array shape; Plain::encode handles both.
        let values = vec![
            String::new(),
            "ascii".to_string(),
            "héllo, wörld 🌍".to_string(),
            "lorem ipsum dolor sit amet".to_string(),
        ];
        let array: ArrayRef = Arc::new(StringArray::from(values.clone()));
        let decoded = round_trip(array, BqlType::String);
        let decoded_view = decoded
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("Plain::decode(String) must produce StringViewArray");
        let decoded_values: Vec<String> = (0..decoded_view.len())
            .map(|i| decoded_view.value(i).to_string())
            .collect();
        assert_eq!(decoded_values, values);
    }

    #[test]
    fn string_round_trip_accepts_utf8_view_input() {
        // StringViewArray (Utf8View) is the schema-canonical type.
        // Feeding it through encode/decode must round-trip identically
        // — StringViewArray == StringViewArray under Arrow's PartialEq.
        let values = vec![
            String::new(),
            "view-input".to_string(),
            "multi\nline\nvalue".to_string(),
        ];
        let array: ArrayRef = Arc::new(StringViewArray::from(values.clone()));
        let decoded = round_trip(array.clone(), BqlType::String);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn timestamp_round_trip_has_utc_timezone() {
        let nanos = vec![0_i64, 1_700_000_000_000_000_000, -1, i64::MIN, i64::MAX - 1];
        let array: ArrayRef =
            Arc::new(TimestampNanosecondArray::from(nanos.clone()).with_timezone("UTC"));
        let decoded = round_trip(array.clone(), BqlType::Timestamp);
        // Equality on TimestampNanosecondArray includes the timezone
        // metadata, so this assert also proves the decode path
        // re-applies the canonical "UTC" timezone.
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn estimate_size_matches_encoded_length() {
        let p = Plain;

        let int_array: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4]));
        assert_eq!(
            p.estimate_size(int_array.as_ref()).unwrap(),
            p.encode(int_array.as_ref()).unwrap().payload.len()
        );

        let bool_array: ArrayRef = Arc::new(BooleanArray::from(vec![true; 17]));
        assert_eq!(
            p.estimate_size(bool_array.as_ref()).unwrap(),
            p.encode(bool_array.as_ref()).unwrap().payload.len()
        );

        let string_array: ArrayRef = Arc::new(StringArray::from(vec!["", "foo", "bar baz"]));
        assert_eq!(
            p.estimate_size(string_array.as_ref()).unwrap(),
            p.encode(string_array.as_ref()).unwrap().payload.len()
        );

        let view_array: ArrayRef = Arc::new(StringViewArray::from(vec!["", "foo", "bar baz"]));
        assert_eq!(
            p.estimate_size(view_array.as_ref()).unwrap(),
            p.encode(view_array.as_ref()).unwrap().payload.len()
        );
    }

    #[test]
    fn encode_rejects_nullable_input() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![Some(1_i64), None, Some(3)]));
        let err = Plain.encode(array.as_ref()).unwrap_err();
        match err {
            BqliteError::Execution(msg) => {
                assert!(
                    msg.contains("null_count"),
                    "error should mention null_count, got: {msg}"
                );
            }
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_wrong_encoding_discriminant() {
        let chunk = EncodedChunk {
            encoding: EncodingType::Dictionary,
            params: Vec::new(),
            payload: vec![0u8; 8],
            row_count: 1,
        };
        let err = Plain.decode(&chunk, &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("Dictionary")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_non_empty_params_for_plain() {
        let chunk = EncodedChunk {
            encoding: EncodingType::Plain,
            params: vec![0xAA],
            payload: vec![1u8; 8],
            row_count: 1,
        };
        let err = Plain.decode(&chunk, &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("empty params")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_truncated_payload() {
        let chunk = EncodedChunk {
            encoding: EncodingType::Plain,
            params: Vec::new(),
            payload: vec![0u8; 7], // short by 1 byte for 1 i64
            row_count: 1,
        };
        let err = Plain.decode(&chunk, &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("expected 8")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }
}
