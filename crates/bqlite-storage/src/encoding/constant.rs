//! Constant encoding — zero-data representation for uniform chunks.
//!
//! When every non-null value in a column chunk is identical, the
//! "payload" is pure redundancy. Constant encoding stores the single
//! literal value in the encoding parameter block and emits **zero
//! payload bytes**. Decode broadcasts the literal into an Arrow array
//! of the requested row count.
//!
//! The byte layout is pinned by
//! `docs/design/storage/segment-format-v1.md` §9.5:
//!
//! ```text
//! encoding_params:
//!     value_kind: u8               // 0 = literal, 1 = all-null (v1 reserved)
//!     value:      type-dependent   // present iff value_kind == 0
//! payload:
//!     (empty)
//! ```
//!
//! # All-null chunks
//!
//! §9.5 reserves `value_kind = 1` for row groups whose column is
//! entirely null (`null_count == row_count`). The TASK-206
//! `Encoding::encode` contract guarantees the caller strips nulls
//! before invoking an encoding, so a Constant impl on that trait
//! cannot observe an all-null input. v1 handles the all-null case
//! above the encoding layer: the writer orchestration (TASK-214)
//! detects a column chunk with `null_count == row_count` and emits
//! a null bitmap plus an empty payload of any applicable encoding,
//! bypassing `Encoding::encode` entirely.
//!
//! `Constant::encode` therefore only emits `value_kind = 0` — the
//! literal case. `Constant::decode` accepts only `value_kind = 0`
//! in v1. A later task can lift `value_kind = 1` to the trait
//! surface without changing the byte format.
//!
//! # Type support
//!
//! Every primitive type [`Plain`](super::plain::Plain) handles is
//! also supported by Constant: `Bool`, `Int`, `Float`, `Timestamp`,
//! `String`. Nested types are deferred — the selector routes nested
//! columns through Plain in v1.
//!
//! # Uniformity check
//!
//! `Constant::encode` verifies that every value in the input array
//! is **bit-exactly** equal to the first value. The bit-level
//! comparison matters for floats: `0.0` and `-0.0` compare as equal
//! under `f64::PartialEq` but have different bit patterns, and
//! encoding a Constant chunk with value `0.0` would lose the
//! `-0.0` sign bit on decode. `f64::to_bits()` comparison avoids
//! this lossy round-trip. Arrays that are *not* uniform return
//! [`BqliteError::Execution`] — the selector (TASK-212) is
//! responsible for invoking Constant only when the uniformity
//! check has already been satisfied at its own inspection pass.

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, StringViewArray,
    TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use bqlite_core::{BqlType, BqliteError, Result};
use std::sync::Arc;

use super::{require_dense, BorrowedEncodedChunk, EncodedChunk, Encoding, EncodingType};

/// Zero-sized marker for the Constant encoding.
#[derive(Debug, Clone, Copy, Default)]
pub struct Constant;

impl Constant {
    /// Construct a new Constant encoder. Constant has no
    /// configuration — this is sugar for `Constant` the unit struct.
    pub const fn new() -> Self {
        Constant
    }
}

/// Literal discriminant (`value_kind = 0`): the encoding header
/// carries a single type-dependent `value` field.
const VALUE_KIND_LITERAL: u8 = 0;

/// All-null discriminant (`value_kind = 1`): reserved by
/// `segment-format-v1.md` §9.5 for all-null row groups. v1 does not
/// emit or decode this variant at the encoding-trait layer — see the
/// module documentation for the writer-orchestration bypass.
const VALUE_KIND_ALL_NULL: u8 = 1;

impl Encoding for Constant {
    fn encoding_type(&self) -> EncodingType {
        EncodingType::Constant
    }

    fn applicable_to(&self, ty: &BqlType) -> bool {
        matches!(
            ty,
            BqlType::Bool | BqlType::Int | BqlType::Float | BqlType::String | BqlType::Timestamp
        )
    }

    fn estimate_size(&self, _array: &dyn Array) -> Result<usize> {
        // The payload is always empty — `estimate_size` is specified
        // as the upper bound on the chunk's `payload` bytes only
        // (trait doc at `encoding/mod.rs`). The selector uses this
        // to compare candidates; for Constant this is always 0, and
        // the selector breaks ties using the encoding's decode cost.
        Ok(0)
    }

    fn encode(&self, array: &dyn Array) -> Result<EncodedChunk> {
        require_dense(array, "Constant")?;
        let row_count = array.len();
        if row_count == 0 {
            return Err(BqliteError::Execution(
                "Constant::encode: input array is empty — Constant requires at \
                 least one value to materialise the literal. Empty chunks are \
                 a writer-orchestration concern handled above the encoding layer."
                    .to_owned(),
            ));
        }

        // Verify uniformity and extract the value bytes. Bit-exact
        // comparison for floats keeps `-0.0` and `0.0` distinct.
        let value_bytes = match array.data_type() {
            DataType::Boolean => {
                encode_bool_value(downcast::<BooleanArray>(array, "BooleanArray")?)?
            }
            DataType::Int64 => encode_int_value(downcast::<Int64Array>(array, "Int64Array")?)?,
            DataType::Float64 => {
                encode_float_value(downcast::<Float64Array>(array, "Float64Array")?)?
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                encode_timestamp_value(downcast::<TimestampNanosecondArray>(
                    array,
                    "TimestampNanosecondArray",
                )?)?
            }
            DataType::Utf8 => encode_utf8_value(downcast::<StringArray>(array, "StringArray")?)?,
            DataType::Utf8View => {
                encode_utf8_view_value(downcast::<StringViewArray>(array, "StringViewArray")?)?
            }
            DataType::List(_) | DataType::Map(_, _) => {
                return Err(BqliteError::Execution(format!(
                    "Constant::encode: nested type {:?} is deferred — v1 \
                     Constant encoding covers Bool, Int64, Float64, \
                     Timestamp(Nanosecond), Utf8, and Utf8View only; List and \
                     Map land in a later Wave 2 task",
                    array.data_type()
                )));
            }
            other => {
                return Err(BqliteError::Execution(format!(
                    "Constant::encode does not support Arrow type {other:?} — \
                     v1 Constant encoding covers Bool, Int64, Float64, \
                     Timestamp(Nanosecond), Utf8, and Utf8View only"
                )));
            }
        };

        let mut params = Vec::with_capacity(1 + value_bytes.len());
        params.push(VALUE_KIND_LITERAL);
        params.extend_from_slice(&value_bytes);

        Ok(EncodedChunk {
            encoding: EncodingType::Constant,
            params,
            payload: Vec::new(),
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
    if encoding != EncodingType::Constant {
        return Err(BqliteError::Execution(format!(
            "Constant::decode called on a {:?} chunk — dispatch must \
             route each chunk to its declared encoding's decoder",
            encoding
        )));
    }
    if !payload.is_empty() {
        return Err(BqliteError::Execution(format!(
            "Constant::decode expects an empty payload per \
             segment-format-v1.md §9.5, got {} bytes",
            payload.len()
        )));
    }
    if params.is_empty() {
        return Err(BqliteError::Execution(
            "Constant::decode: encoding header is missing the value_kind byte".to_owned(),
        ));
    }

    let value_kind = params[0];
    let rest = &params[1..];

    match value_kind {
        VALUE_KIND_LITERAL => decode_literal(rest, ty, row_count),
        VALUE_KIND_ALL_NULL => Err(BqliteError::Execution(
            "Constant::decode: value_kind = 1 (all-null) is reserved by \
             segment-format-v1.md §9.5 but not yet supported by v1 \
             readers. The writer orchestration handles all-null column \
             chunks via the null bitmap + an empty payload of another \
             encoding; any Constant chunk in a v1 segment should use \
             value_kind = 0. Seeing value_kind = 1 is a corruption \
             signal in v1 and will be reconsidered when a later wave \
             lifts the all-null case to the encoding trait."
                .to_owned(),
        )),
        other => Err(BqliteError::Execution(format!(
            "Constant::decode: unknown value_kind {other} — only 0 \
             (literal) and 1 (all-null, reserved) are defined by \
             segment-format-v1.md §9.5"
        ))),
    }
}

// ── encode helpers ───────────────────────────────────────────────────────────

fn encode_bool_value(array: &BooleanArray) -> Result<Vec<u8>> {
    let first = array.value(0);
    for i in 1..array.len() {
        if array.value(i) != first {
            return Err(not_uniform(i, "Bool"));
        }
    }
    Ok(vec![u8::from(first)])
}

fn encode_int_value(array: &Int64Array) -> Result<Vec<u8>> {
    let values = array.values();
    let first = values[0];
    for (i, &v) in values.iter().enumerate().skip(1) {
        if v != first {
            return Err(not_uniform(i, "Int"));
        }
    }
    Ok(first.to_le_bytes().to_vec())
}

fn encode_float_value(array: &Float64Array) -> Result<Vec<u8>> {
    // Bit-level comparison: `0.0` and `-0.0` are NOT considered the
    // same Constant value because decoding a `Constant 0.0` chunk
    // would flip the sign bit on every row that was originally
    // `-0.0`. NaN equality is also resolved by the bit-level check:
    // two NaNs with the same bit pattern are "equal" here, which is
    // the only robust definition for encoding.
    let values = array.values();
    let first_bits = values[0].to_bits();
    for (i, &v) in values.iter().enumerate().skip(1) {
        if v.to_bits() != first_bits {
            return Err(not_uniform(i, "Float"));
        }
    }
    Ok(values[0].to_le_bytes().to_vec())
}

fn encode_timestamp_value(array: &TimestampNanosecondArray) -> Result<Vec<u8>> {
    let values = array.values();
    let first = values[0];
    for (i, &v) in values.iter().enumerate().skip(1) {
        if v != first {
            return Err(not_uniform(i, "Timestamp"));
        }
    }
    Ok(first.to_le_bytes().to_vec())
}

fn encode_utf8_value(array: &StringArray) -> Result<Vec<u8>> {
    let first = array.value(0);
    for i in 1..array.len() {
        if array.value(i) != first {
            return Err(not_uniform(i, "String (Utf8)"));
        }
    }
    push_utf8_literal(first)
}

fn encode_utf8_view_value(array: &StringViewArray) -> Result<Vec<u8>> {
    let first = array.value(0);
    for i in 1..array.len() {
        if array.value(i) != first {
            return Err(not_uniform(i, "String (Utf8View)"));
        }
    }
    push_utf8_literal(first)
}

fn push_utf8_literal(s: &str) -> Result<Vec<u8>> {
    if s.len() > u32::MAX as usize {
        return Err(BqliteError::Execution(format!(
            "Constant::encode: string literal is {} bytes, exceeding the v1 \
             per-value cap of u32::MAX ({} bytes)",
            s.len(),
            u32::MAX
        )));
    }
    let mut bytes = Vec::with_capacity(4 + s.len());
    bytes.extend_from_slice(&(s.len() as u32).to_le_bytes());
    bytes.extend_from_slice(s.as_bytes());
    Ok(bytes)
}

fn not_uniform(index: usize, type_name: &str) -> BqliteError {
    BqliteError::Execution(format!(
        "Constant::encode: {type_name} array is not uniform — value at index \
         {index} differs from the first value. The selector (TASK-212) must \
         only dispatch Constant when uniformity has been confirmed."
    ))
}

// ── decode helpers ───────────────────────────────────────────────────────────

fn decode_literal(params: &[u8], ty: &BqlType, row_count: usize) -> Result<ArrayRef> {
    match ty {
        BqlType::Bool => {
            require_params_len(params, 1, "Bool")?;
            let value = match params[0] {
                0 => false,
                1 => true,
                other => {
                    return Err(BqliteError::Execution(format!(
                        "Constant::decode Bool: invalid literal byte {other} — \
                         v1 segment-format-v1.md §9.5 defines only 0x00 and 0x01"
                    )));
                }
            };
            Ok(Arc::new(BooleanArray::from(vec![value; row_count])) as ArrayRef)
        }
        BqlType::Int => {
            require_params_len(params, 8, "Int")?;
            let value = i64::from_le_bytes(params.try_into().unwrap());
            Ok(Arc::new(Int64Array::from(vec![value; row_count])) as ArrayRef)
        }
        BqlType::Float => {
            require_params_len(params, 8, "Float")?;
            let value = f64::from_le_bytes(params.try_into().unwrap());
            Ok(Arc::new(Float64Array::from(vec![value; row_count])) as ArrayRef)
        }
        BqlType::Timestamp => {
            require_params_len(params, 8, "Timestamp")?;
            let value = i64::from_le_bytes(params.try_into().unwrap());
            Ok(
                Arc::new(
                    TimestampNanosecondArray::from(vec![value; row_count]).with_timezone("UTC"),
                ) as ArrayRef,
            )
        }
        BqlType::String => {
            if params.len() < 4 {
                return Err(BqliteError::Execution(format!(
                    "Constant::decode String: encoding header truncated at \
                     length prefix — got {} bytes, need at least 4",
                    params.len()
                )));
            }
            let len = u32::from_le_bytes(params[..4].try_into().unwrap()) as usize;
            if params.len() != 4 + len {
                return Err(BqliteError::Execution(format!(
                    "Constant::decode String: expected {} bytes for u32 length \
                     prefix + {len}-byte UTF-8 value, got {}",
                    4 + len,
                    params.len()
                )));
            }
            let s = std::str::from_utf8(&params[4..]).map_err(|e| {
                BqliteError::Execution(format!(
                    "Constant::decode String: invalid UTF-8 in literal value: {e}"
                ))
            })?;
            // Produce `StringViewArray` (schema-canonical for
            // `BqlType::String`) so every Plain/Constant decode
            // returns the same Arrow type, matching TASK-206.
            let owned = s.to_owned();
            Ok(Arc::new(StringViewArray::from(vec![owned; row_count])) as ArrayRef)
        }
        BqlType::List(_) | BqlType::Map(_) => Err(BqliteError::Execution(format!(
            "Constant::decode does not yet support nested type {ty} — \
             List and Map decode lands in a later Wave 2 task"
        ))),
    }
}

fn require_params_len(params: &[u8], expected: usize, type_name: &str) -> Result<()> {
    if params.len() != expected {
        return Err(BqliteError::Execution(format!(
            "Constant::decode {type_name}: expected a {expected}-byte literal \
             after the value_kind byte, got {}",
            params.len()
        )));
    }
    Ok(())
}

// ── shared downcast helper ──────────────────────────────────────────────────

fn downcast<'a, T: 'static>(array: &'a dyn Array, expected: &'static str) -> Result<&'a T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        BqliteError::Execution(format!(
            "Constant encoding: expected Arrow array of type {expected}, \
             got data_type = {:?}",
            array.data_type()
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(array: ArrayRef, ty: BqlType) -> ArrayRef {
        let chunk = Constant.encode(array.as_ref()).unwrap();
        Constant.decode(&chunk, &ty).unwrap()
    }

    #[test]
    fn applicable_to_covers_every_primitive() {
        let c = Constant;
        assert!(c.applicable_to(&BqlType::Bool));
        assert!(c.applicable_to(&BqlType::Int));
        assert!(c.applicable_to(&BqlType::Float));
        assert!(c.applicable_to(&BqlType::String));
        assert!(c.applicable_to(&BqlType::Timestamp));
        assert!(!c.applicable_to(&BqlType::List(Box::new(BqlType::Int))));
        assert!(!c.applicable_to(&BqlType::Map(Box::new(BqlType::String))));
    }

    #[test]
    fn encoding_type_is_constant_discriminant_six() {
        assert_eq!(Constant.encoding_type(), EncodingType::Constant);
        assert_eq!(Constant.encoding_type().discriminant(), 6);
    }

    #[test]
    fn estimate_size_is_zero_regardless_of_input() {
        let c = Constant;
        let array: ArrayRef = Arc::new(Int64Array::from(vec![7_i64; 100]));
        assert_eq!(c.estimate_size(array.as_ref()).unwrap(), 0);
        let array: ArrayRef = Arc::new(StringArray::from(vec!["x"; 10]));
        assert_eq!(c.estimate_size(array.as_ref()).unwrap(), 0);
    }

    #[test]
    fn int_round_trip_repeated_value() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![42_i64; 16]));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn bool_round_trip_true() {
        let array: ArrayRef = Arc::new(BooleanArray::from(vec![true; 17]));
        let decoded = round_trip(array.clone(), BqlType::Bool);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn bool_round_trip_false_single_value() {
        let array: ArrayRef = Arc::new(BooleanArray::from(vec![false]));
        let decoded = round_trip(array.clone(), BqlType::Bool);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn float_round_trip_preserves_sign_zero() {
        // Encoding a Constant -0.0 chunk must round-trip to -0.0,
        // not to +0.0. The bit-level comparison in `encode_float_value`
        // is what makes this safe.
        let array: ArrayRef = Arc::new(Float64Array::from(vec![-0.0_f64; 4]));
        let decoded = round_trip(array, BqlType::Float);
        let decoded_float = decoded.as_any().downcast_ref::<Float64Array>().unwrap();
        for i in 0..decoded_float.len() {
            assert_eq!(decoded_float.value(i).to_bits(), (-0.0_f64).to_bits());
        }
    }

    #[test]
    fn float_mixed_zero_and_negative_zero_is_not_uniform() {
        // `0.0 == -0.0` under `f64::PartialEq`, but Constant's bit-level
        // check rejects the chunk so we don't lose the sign bit.
        let array: ArrayRef = Arc::new(Float64Array::from(vec![0.0_f64, -0.0_f64]));
        let err = Constant.encode(array.as_ref()).unwrap_err();
        match err {
            BqliteError::Execution(msg) => {
                assert!(
                    msg.contains("not uniform"),
                    "expected 'not uniform' error, got: {msg}"
                );
            }
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn timestamp_round_trip_has_utc_timezone() {
        let array: ArrayRef = Arc::new(
            TimestampNanosecondArray::from(vec![1_700_000_000_000_000_000_i64; 5])
                .with_timezone("UTC"),
        );
        let decoded = round_trip(array.clone(), BqlType::Timestamp);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn string_round_trip_accepts_utf8_input_and_produces_utf8_view() {
        let array: ArrayRef = Arc::new(StringArray::from(vec!["purchase"; 8]));
        let decoded = round_trip(array, BqlType::String);
        let decoded_view = decoded
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("Constant::decode(String) must produce StringViewArray");
        assert_eq!(decoded_view.len(), 8);
        for i in 0..8 {
            assert_eq!(decoded_view.value(i), "purchase");
        }
    }

    #[test]
    fn string_round_trip_accepts_utf8_view_input() {
        let array: ArrayRef = Arc::new(StringViewArray::from(vec!["héllo 🌍"; 3]));
        let decoded = round_trip(array.clone(), BqlType::String);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn encode_rejects_non_uniform_int_array() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 1, 2, 1]));
        let err = Constant.encode(array.as_ref()).unwrap_err();
        match err {
            BqliteError::Execution(msg) => {
                assert!(msg.contains("not uniform") && msg.contains("index 2"));
            }
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn encode_rejects_non_uniform_string_array() {
        let array: ArrayRef = Arc::new(StringArray::from(vec!["a", "a", "b"]));
        let err = Constant.encode(array.as_ref()).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("not uniform")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn encode_rejects_empty_array() {
        let array: ArrayRef = Arc::new(Int64Array::from(Vec::<i64>::new()));
        let err = Constant.encode(array.as_ref()).unwrap_err();
        match err {
            BqliteError::Execution(msg) => {
                assert!(
                    msg.contains("empty"),
                    "expected message to mention empty, got: {msg}"
                );
            }
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn encode_rejects_nullable_input() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![Some(1_i64), None, Some(1)]));
        let err = Constant.encode(array.as_ref()).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("null_count")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_wrong_encoding_discriminant() {
        let chunk = EncodedChunk {
            encoding: EncodingType::Plain,
            params: vec![0u8, 0, 0, 0, 0, 0, 0, 0, 0],
            payload: Vec::new(),
            row_count: 1,
        };
        let err = Constant.decode(&chunk, &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("Plain")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_non_empty_payload() {
        let chunk = EncodedChunk {
            encoding: EncodingType::Constant,
            params: vec![0u8, 42, 0, 0, 0, 0, 0, 0, 0],
            payload: vec![0xAA],
            row_count: 1,
        };
        let err = Constant.decode(&chunk, &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("empty payload")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_missing_value_kind_byte() {
        let chunk = EncodedChunk {
            encoding: EncodingType::Constant,
            params: Vec::new(),
            payload: Vec::new(),
            row_count: 1,
        };
        let err = Constant.decode(&chunk, &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("value_kind")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_all_null_value_kind_in_v1() {
        let chunk = EncodedChunk {
            encoding: EncodingType::Constant,
            params: vec![VALUE_KIND_ALL_NULL],
            payload: Vec::new(),
            row_count: 5,
        };
        let err = Constant.decode(&chunk, &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => {
                assert!(
                    msg.contains("all-null") || msg.contains("value_kind = 1"),
                    "expected 'all-null' or 'value_kind = 1' in error, got: {msg}"
                );
            }
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_unknown_value_kind() {
        let chunk = EncodedChunk {
            encoding: EncodingType::Constant,
            params: vec![42u8],
            payload: Vec::new(),
            row_count: 1,
        };
        let err = Constant.decode(&chunk, &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("value_kind 42")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_truncated_int_literal() {
        let chunk = EncodedChunk {
            encoding: EncodingType::Constant,
            params: vec![VALUE_KIND_LITERAL, 0, 0, 0], // only 3 bytes after discriminant
            payload: Vec::new(),
            row_count: 1,
        };
        let err = Constant.decode(&chunk, &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("8-byte literal")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }
}
