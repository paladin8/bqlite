//! Frame-of-Reference (FOR) encoding — per-block integer packing.
//!
//! FOR divides the column into fixed-size blocks (128 values each) and
//! encodes each block with its own per-block minimum value and bit-width.
//! This is a refinement of v1 BitPacking, which uses a single global
//! minimum across the entire column chunk. Per-block framing reduces the
//! bit-width requirement for columns where numeric properties cluster
//! locally (e.g. `amount` values that shift between ranges across
//! entity groups within a row group).
//!
//! The on-disk layout is pinned by
//! `docs/design/storage/segment-format-v2.md` §5.4:
//!
//! ```text
//! encoding_params:
//!     block_size:   u16 LE   // always 128 in v2 (SIMD-aligned)
//!     block_count:  u32 LE   // ceil(non_null_count / block_size)
//! payload (repeated block_count times):
//!     block_min:    i64 LE   // minimum value in this block
//!     bit_width:    u8       // bits per offset (1..=64)
//!     packed:       ceil(block_len × bit_width / 8) bytes, padded to
//!                   the next multiple of 8 bytes (SIMD tail safety)
//! ```
//!
//! Where `block_len` is `block_size` for all blocks except the last, which
//! has `block_len = non_null_count - (block_count - 1) × block_size`.
//! Offsets are unsigned: `offset[i] = value[i] - block_min`.
//!
//! # Predicate pushdown
//!
//! The per-block structure naturally exposes a per-block minimum (`block_min`).
//! A range predicate can derive `block_max = block_min + (1 << bit_width) - 1`
//! and skip entire blocks — intra-row-group pruning that global BitPacking
//! cannot offer. The reader and planner integration for block-level skipping
//! is wired in TASK-419.
//!
//! # Type support
//!
//! FOR applies to `Int` (`Int64Array`) and `Timestamp`
//! (`TimestampNanosecondArray`). All other types are inapplicable.
//!
//! # Null handling
//!
//! FOR operates on dense arrays — the writer strips nulls before encoding.
//! A nullable input is a contract violation caught by `super::require_dense`.
//!
//! # Edge cases
//!
//! - `row_count == 0`: legal. Params carry `block_count = 0`, payload is empty.
//! - Single block (`row_count ≤ 128`): `block_count = 1`, `block_len = row_count`.
//! - Short final block: `block_len < 128`, packed section still padded to 8 bytes.
//! - All identical in a block: `bit_width = 1` (1-bit floor per v2 §5.4 convention,
//!   matching v1 BitPacking).
//! - `bit_width` in the packed header is always in `1..=64`.
//!
//! # Selector guard
//!
//! Per `advanced-encodings.md` §5.7: choose FOR over BitPacking when the
//! sum of per-block bit widths is less than `block_count × global_bit_width`
//! by more than 10%. The guard lives in the selector (TASK-419), not here.
//! [`ForEncoding::estimate_size`] returns the exact payload size so the
//! selector can compare accurately.
//!
//! # Hot path
//!
//! Full 128-value blocks with `bit_width ≤ 32` use `bitpacking::BitPacker4x`
//! for SSE3/NEON-accelerated packing and unpacking. Short final blocks and
//! `bit_width > 32` fall back to a scalar bit-operation loop.

use arrow::array::{Array, ArrayRef, Int64Array, TimestampNanosecondArray};
use arrow::datatypes::{DataType, TimeUnit};
use bitpacking::{BitPacker, BitPacker4x};
use bqlite_core::{BqlType, BqliteError, Result};
use std::sync::Arc;

use super::{require_dense, BorrowedEncodedChunk, EncodedChunk, Encoding, EncodingType};

/// Zero-sized marker for the FOR encoding.
///
/// Stateless; freely clonable and stored behind a `Box<dyn Encoding>`.
/// See the module-level documentation for the byte layout this impl produces.
#[derive(Debug, Clone, Copy, Default)]
pub struct ForEncoding;

impl ForEncoding {
    /// Construct a new ForEncoding encoder. ForEncoding has no configuration.
    pub const fn new() -> Self {
        ForEncoding
    }
}

/// Block size (number of values per FOR block). Fixed at 128 in v2 to match
/// `BitPacker4x::BLOCK_LEN` for SIMD-accelerated packing on full blocks.
const BLOCK_SIZE: usize = <BitPacker4x as BitPacker>::BLOCK_LEN; // = 128

/// Encoding-params block size: `block_size: u16 LE` (2 bytes) + `block_count: u32 LE` (4 bytes).
const PARAMS_LEN: usize = 6;

/// Per-block header size: `block_min: i64 LE` (8 bytes) + `bit_width: u8` (1 byte).
const BLOCK_HEADER_LEN: usize = 9;

/// Payload padding granularity per v2 §5.4 — packed offsets are always
/// padded to the next multiple of 8 bytes for SIMD tail safety.
const PADDING_GRANULARITY: usize = 8;

/// `BitPacker4x` only accepts `u32`, so widths above 32 fall to scalar.
const BITPACKER4X_MAX_BIT_WIDTH: u8 = 32;

impl Encoding for ForEncoding {
    fn encoding_type(&self) -> EncodingType {
        EncodingType::For
    }

    fn applicable_to(&self, ty: &BqlType) -> bool {
        matches!(ty, BqlType::Int | BqlType::Timestamp)
    }

    /// Returns the exact encoded payload byte count (not an over-estimate).
    ///
    /// FOR's estimate is exact because `block_packed_byte_len` is a deterministic
    /// function of `(block_len, bit_width)` and `select_block_frame` is a
    /// deterministic function of the block values. The selector (TASK-419)
    /// compares this against BitPacking's estimate to decide which codec wins.
    ///
    /// Note: `require_dense` is intentionally *not* called here. The caller
    /// (the encoding selector) may invoke `estimate_size` on any column array
    /// to score candidates. Null rejection is enforced by `encode`, which is
    /// only called after a codec is selected. This matches the pattern used by
    /// `BitPacking`, `Delta`, and all other v1 / Wave 4 encoding impls.
    fn estimate_size(&self, array: &dyn Array) -> Result<usize> {
        let values = values_as_i64(array)?;
        Ok(compute_payload_size(values))
    }

    fn encode(&self, array: &dyn Array) -> Result<EncodedChunk> {
        require_dense(array, "ForEncoding")?;
        let values = values_as_i64(array)?;
        let row_count = array.len();

        let block_count = row_count.div_ceil(BLOCK_SIZE);

        // Encoding params: block_size (u16) + block_count (u32) = 6 bytes.
        let mut params = Vec::with_capacity(PARAMS_LEN);
        params.extend_from_slice(&(BLOCK_SIZE as u16).to_le_bytes());
        params.extend_from_slice(&(block_count as u32).to_le_bytes());

        // Payload: per-block header + packed offsets.
        let mut payload: Vec<u8> = Vec::with_capacity(compute_payload_size(values));
        for block_values in values.chunks(BLOCK_SIZE) {
            let (block_min, bit_width) = select_block_frame(block_values);
            payload.extend_from_slice(&block_min.to_le_bytes());
            payload.push(bit_width);
            let packed = pack_block_offsets(block_values, block_min, bit_width);
            payload.extend_from_slice(&packed);
        }

        Ok(EncodedChunk {
            encoding: EncodingType::For,
            params,
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
    if encoding != EncodingType::For {
        return Err(BqliteError::Execution(format!(
            "ForEncoding::decode called on a {:?} chunk — dispatch must \
             route each chunk to its declared encoding's decoder",
            encoding
        )));
    }
    if params.len() != PARAMS_LEN {
        return Err(BqliteError::Execution(format!(
            "ForEncoding::decode: encoding params must be exactly {} bytes \
             (u16 block_size + u32 block_count), got {}",
            PARAMS_LEN,
            params.len()
        )));
    }

    let block_size = u16::from_le_bytes(params[..2].try_into().unwrap()) as usize;
    let block_count = u32::from_le_bytes(params[2..6].try_into().unwrap()) as usize;

    if block_size != BLOCK_SIZE {
        return Err(BqliteError::Execution(format!(
            "ForEncoding::decode: block_size must be {} per \
             segment-format-v2.md §5.4, got {block_size}",
            BLOCK_SIZE
        )));
    }

    let expected_block_count = row_count.div_ceil(BLOCK_SIZE);
    if block_count != expected_block_count {
        return Err(BqliteError::Execution(format!(
            "ForEncoding::decode: block_count {block_count} is inconsistent \
             with row_count {row_count} (expected {expected_block_count})"
        )));
    }

    let mut values: Vec<i64> = Vec::with_capacity(row_count);
    let mut payload_cursor = 0usize;

    // Cache BitPacker4x across blocks — `new()` runs CPU feature detection.
    let bitpacker = BitPacker4x::new();
    let mut block_u32 = [0u32; BLOCK_SIZE];

    for block_idx in 0..block_count {
        let block_start = block_idx * BLOCK_SIZE;
        let block_len = (row_count - block_start).min(BLOCK_SIZE);

        // Read block header: block_min (i64 LE) + bit_width (u8).
        if payload_cursor + BLOCK_HEADER_LEN > payload.len() {
            return Err(BqliteError::Execution(format!(
                "ForEncoding::decode: payload truncated at block {block_idx}: \
                 expected {BLOCK_HEADER_LEN}-byte header at offset {payload_cursor}, \
                 payload is {} bytes",
                payload.len()
            )));
        }
        let block_min = i64::from_le_bytes(
            payload[payload_cursor..payload_cursor + 8]
                .try_into()
                .unwrap(),
        );
        let bit_width = payload[payload_cursor + 8];
        payload_cursor += BLOCK_HEADER_LEN;

        if bit_width == 0 || bit_width > 64 {
            return Err(BqliteError::Execution(format!(
                "ForEncoding::decode: bit_width must be in 1..=64 per \
                 segment-format-v2.md §5.4, got {bit_width} in block {block_idx}"
            )));
        }

        let packed_len = block_packed_byte_len(block_len, bit_width);
        if payload_cursor + packed_len > payload.len() {
            return Err(BqliteError::Execution(format!(
                "ForEncoding::decode: payload truncated at block {block_idx}: \
                 expected {packed_len} packed bytes at offset {payload_cursor}, \
                 payload is {} bytes",
                payload.len()
            )));
        }
        let packed = &payload[payload_cursor..payload_cursor + packed_len];
        payload_cursor += packed_len;

        // Hot path: full 128-value block with bit_width ≤ 32. Decompress
        // directly into a stack buffer and reconstitute via `wrapping_add`
        // — equivalent to the i128 cast for in-range values (offset fits in
        // u32, block_min + offset is a valid i64 by construction), but skips
        // both the per-block `Vec<u64>` allocation and the i128 arithmetic.
        //
        // The post-process loop writes through `spare_capacity_mut` so LLVM
        // can autovectorize the u32→i64 widen + scalar add (NEON `vmovl_u32`
        // + `vaddq_s64` on aarch64, AVX2 `vpmovzxdq` + `vpaddq` on x86_64)
        // instead of paying a per-element `Vec::push` length check.
        if block_len == BLOCK_SIZE && bit_width <= BITPACKER4X_MAX_BIT_WIDTH {
            let block_bytes = bitpacker4x_block_bytes(bit_width);
            bitpacker.decompress(&packed[..block_bytes], &mut block_u32, bit_width);
            let len_before = values.len();
            debug_assert!(values.capacity() - len_before >= BLOCK_SIZE);
            // SAFETY: `Vec::with_capacity(row_count)` above reserved enough
            // capacity for every block, so `len_before + BLOCK_SIZE` is in
            // bounds. We initialise all `BLOCK_SIZE` slots before setting
            // the new length.
            //
            // The index loop (rather than `enumerate()`) keeps the pointer
            // arithmetic explicit; LLVM autovectorizes it on aarch64 to
            // `uaddw.2d` widen-and-add across 8 u32 lanes per iteration.
            #[allow(clippy::needless_range_loop)]
            unsafe {
                let dst = values.as_mut_ptr().add(len_before);
                for i in 0..BLOCK_SIZE {
                    std::ptr::write(
                        dst.add(i),
                        block_min.wrapping_add(block_u32[i] as i64),
                    );
                }
                values.set_len(len_before + BLOCK_SIZE);
            }
        } else {
            // Scalar fallback for short final blocks or bit_width > 32.
            // `wrapping_add` works for the full range because offset is
            // computed as (value as i128 - block_min as i128) as u64 at
            // encode time, so the round-trip in two's-complement always
            // recovers the original i64.
            let width = bit_width as usize;
            for i in 0..block_len {
                let offset = read_bits(packed, i * width, width);
                values.push(block_min.wrapping_add(offset as i64));
            }
        }
    }

    if payload_cursor != payload.len() {
        return Err(BqliteError::Execution(format!(
            "ForEncoding::decode: {} trailing bytes in payload after decoding \
             {block_count} blocks — segment may be corrupt",
            payload.len() - payload_cursor
        )));
    }

    match ty {
        BqlType::Int => Ok(Arc::new(Int64Array::from(values)) as ArrayRef),
        BqlType::Timestamp => {
            Ok(Arc::new(TimestampNanosecondArray::from(values).with_timezone("UTC")) as ArrayRef)
        }
        other => Err(BqliteError::Execution(format!(
            "ForEncoding::decode does not support type {other} — FOR decodes \
             to Int or Timestamp only (segment-format-v2.md §5.4)"
        ))),
    }
}

// ── i64 accessor ─────────────────────────────────────────────────────────────

/// Extract an `&[i64]` view over an Int64Array or TimestampNanosecondArray.
/// FOR treats both as signed 64-bit integers.
fn values_as_i64(array: &dyn Array) -> Result<&[i64]> {
    match array.data_type() {
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                BqliteError::Execution("ForEncoding: Int64 downcast failed".into())
            })?;
            Ok(arr.values())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| {
                    BqliteError::Execution(
                        "ForEncoding: TimestampNanosecondArray downcast failed".into(),
                    )
                })?;
            Ok(arr.values())
        }
        DataType::List(_) | DataType::Map(_, _) => Err(BqliteError::Execution(format!(
            "ForEncoding: nested type {:?} is not supported — FOR encoding \
             covers Int64 and Timestamp(Nanosecond) only; nested types are deferred",
            array.data_type()
        ))),
        other => Err(BqliteError::Execution(format!(
            "ForEncoding: unsupported Arrow type {other:?} — FOR encoding \
             covers Int64 and Timestamp(Nanosecond) only"
        ))),
    }
}

// ── block frame selection ─────────────────────────────────────────────────────

/// Pick `(block_min, bit_width)` for a block of i64 values.
///
/// For an empty block returns `(0, 1)`. Per v2 §5.4, `bit_width` is in
/// `1..=64` — the `max(1, …)` floor ensures all-identical blocks
/// (which would otherwise need 0 bits) encode with `bit_width = 1`.
fn select_block_frame(block: &[i64]) -> (i64, u8) {
    if block.is_empty() {
        return (0, 1);
    }
    let mut min = block[0];
    let mut max = block[0];
    for &v in &block[1..] {
        if v < min {
            min = v;
        }
        if v > max {
            max = v;
        }
    }
    let max_offset = ((max as i128) - (min as i128)) as u64;
    let bit_width = if max_offset == 0 {
        1
    } else {
        (64 - max_offset.leading_zeros()) as u8
    };
    (min, bit_width)
}

// ── block payload sizing ──────────────────────────────────────────────────────

/// Number of packed-payload bytes for a block of `block_len` values at
/// `bit_width` bits each, padded to the next multiple of [`PADDING_GRANULARITY`].
fn block_packed_byte_len(block_len: usize, bit_width: u8) -> usize {
    if block_len == 0 {
        return 0;
    }
    let total_bits = block_len.saturating_mul(bit_width as usize);
    let packed_bytes = total_bits.div_ceil(8);
    packed_bytes.div_ceil(PADDING_GRANULARITY) * PADDING_GRANULARITY
}

/// Total payload byte count for encoding `values` with the FOR block layout.
/// Each block contributes [`BLOCK_HEADER_LEN`] + padded packed bytes.
/// Returns 0 for an empty slice.
fn compute_payload_size(values: &[i64]) -> usize {
    if values.is_empty() {
        return 0;
    }
    let mut total = 0usize;
    for block in values.chunks(BLOCK_SIZE) {
        let (_, bit_width) = select_block_frame(block);
        total += BLOCK_HEADER_LEN + block_packed_byte_len(block.len(), bit_width);
    }
    total
}

// ── block bit packing / unpacking ─────────────────────────────────────────────

/// Number of bytes `BitPacker4x::compress` writes for a full 128-value block
/// at the given bit width.
fn bitpacker4x_block_bytes(bit_width: u8) -> usize {
    // 128 * bit_width is always divisible by 8 for any bit_width in 1..=32,
    // so no rounding needed here.
    (BLOCK_SIZE * bit_width as usize) / 8
}

/// Pack `block` values into a padded bit stream given `block_min` and
/// `bit_width`. Returns exactly `block_packed_byte_len(block.len(), bit_width)`
/// bytes, with zero-padding at the end.
///
/// Fast path: full 128-value blocks with `bit_width ≤ 32` use `BitPacker4x`.
/// Scalar path: short final blocks and `bit_width > 32`.
fn pack_block_offsets(block: &[i64], block_min: i64, bit_width: u8) -> Vec<u8> {
    let block_len = block.len();
    let mut bytes = vec![0u8; block_packed_byte_len(block_len, bit_width)];
    if block_len == 0 {
        return bytes;
    }
    let width = bit_width as usize;

    // Fast path: full 128-value block with bit_width ≤ 32 → BitPacker4x.
    if block_len == BLOCK_SIZE && bit_width <= BITPACKER4X_MAX_BIT_WIDTH {
        let bitpacker = BitPacker4x::new();
        let mut block_u32 = [0u32; BLOCK_SIZE];
        for (slot, &value) in block_u32.iter_mut().zip(block) {
            let offset = ((value as i128) - (block_min as i128)) as u64;
            *slot = u32::try_from(offset)
                .expect("bit_width ≤ 32 guarantees BitPacker4x offsets fit in u32");
        }
        let written = bitpacker.compress(&block_u32, &mut bytes, bit_width);
        debug_assert_eq!(written, bitpacker4x_block_bytes(bit_width));
        return bytes;
    }

    // Scalar path: short final block or bit_width > 32.
    for (i, &value) in block.iter().enumerate() {
        let offset = ((value as i128) - (block_min as i128)) as u64;
        write_bits(&mut bytes, i * width, width, offset);
    }
    bytes
}

/// Write `width` bits of `value` at bit offset `start` into `out`,
/// LSB-first within each byte. `width` must be ≤ 64 and
/// `value >> width` must be zero.
///
/// Mirrors the identical helper in `bitpacking.rs` for the decode-side fallback
/// path. Kept local so this module has no private-item dependency on
/// `bitpacking.rs`.
fn write_bits(out: &mut [u8], start: usize, width: usize, value: u64) {
    let mut bits_written = 0usize;
    while bits_written < width {
        let byte_idx = (start + bits_written) / 8;
        let bit_in_byte = (start + bits_written) % 8;
        let chunk_bits = (width - bits_written).min(8 - bit_in_byte);
        let chunk_mask: u8 = if chunk_bits == 8 {
            u8::MAX
        } else {
            (1u8 << chunk_bits) - 1
        };
        let chunk = ((value >> bits_written) as u8) & chunk_mask;
        out[byte_idx] |= chunk << bit_in_byte;
        bits_written += chunk_bits;
    }
}

/// Read `width` bits at bit offset `start` from `bytes`, LSB-first
/// within each byte. `width` must be in 1..=64.
///
/// Mirrors the identical helper in `bitpacking.rs`.
fn read_bits(bytes: &[u8], start: usize, width: usize) -> u64 {
    let mut value = 0u64;
    let mut bits_read = 0usize;
    while bits_read < width {
        let byte_idx = (start + bits_read) / 8;
        let bit_in_byte = (start + bits_read) % 8;
        let chunk_bits = (width - bits_read).min(8 - bit_in_byte);
        let chunk_mask: u8 = if chunk_bits == 8 {
            u8::MAX
        } else {
            (1u8 << chunk_bits) - 1
        };
        let chunk = (bytes[byte_idx] >> bit_in_byte) & chunk_mask;
        value |= (chunk as u64) << bits_read;
        bits_read += chunk_bits;
    }
    value
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(array: ArrayRef, ty: BqlType) -> ArrayRef {
        let chunk = ForEncoding.encode(array.as_ref()).unwrap();
        ForEncoding.decode(&chunk, &ty).unwrap()
    }

    #[test]
    fn applicable_to_covers_int_and_timestamp_only() {
        let enc = ForEncoding;
        assert!(enc.applicable_to(&BqlType::Int));
        assert!(enc.applicable_to(&BqlType::Timestamp));
        assert!(!enc.applicable_to(&BqlType::Bool));
        assert!(!enc.applicable_to(&BqlType::Float));
        assert!(!enc.applicable_to(&BqlType::String));
    }

    #[test]
    fn encoding_type_is_for_discriminant_eight() {
        assert_eq!(ForEncoding.encoding_type(), EncodingType::For);
        assert_eq!(ForEncoding.encoding_type().discriminant(), 8);
    }

    #[test]
    fn params_are_exactly_six_bytes() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2, 3]));
        let chunk = ForEncoding.encode(array.as_ref()).unwrap();
        assert_eq!(chunk.params.len(), PARAMS_LEN);
    }

    #[test]
    fn params_block_size_is_128() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2, 3]));
        let chunk = ForEncoding.encode(array.as_ref()).unwrap();
        let block_size = u16::from_le_bytes(chunk.params[..2].try_into().unwrap());
        assert_eq!(block_size, 128);
    }

    #[test]
    fn int_round_trip_empty() {
        let array: ArrayRef = Arc::new(Int64Array::from(Vec::<i64>::new()));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.len(), 0);
        // Empty → block_count = 0, payload = empty.
        let chunk = ForEncoding.encode(array.as_ref()).unwrap();
        assert_eq!(chunk.payload.len(), 0);
        let bc = u32::from_le_bytes(chunk.params[2..6].try_into().unwrap());
        assert_eq!(bc, 0);
    }

    #[test]
    fn int_round_trip_single_value() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![42_i64]));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn int_round_trip_all_identical() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![7_i64; 256]));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.as_ref(), array.as_ref());
        // Every block should encode with bit_width = 1.
        let chunk = ForEncoding.encode(array.as_ref()).unwrap();
        // Two full blocks (256 / 128 = 2).
        let bc = u32::from_le_bytes(chunk.params[2..6].try_into().unwrap()) as usize;
        assert_eq!(bc, 2);
        for i in 0..bc {
            let off = i * (BLOCK_HEADER_LEN + block_packed_byte_len(BLOCK_SIZE, 1));
            let bw = chunk.payload[off + 8]; // bit_width byte
            assert_eq!(
                bw, 1,
                "block {i} should use bit_width = 1 for identical values"
            );
        }
    }

    #[test]
    fn int_round_trip_narrow_range_within_block() {
        // Values cluster in [100, 115] → 4-bit offsets.
        let values: Vec<i64> = (0..128).map(|i| 100 + (i % 16) as i64).collect();
        let array: ArrayRef = Arc::new(Int64Array::from(values));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn int_round_trip_multiple_blocks() {
        // 300 values → 3 blocks (128, 128, 44).
        let values: Vec<i64> = (0..300_i64).collect();
        let array: ArrayRef = Arc::new(Int64Array::from(values));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.as_ref(), array.as_ref());
        let chunk = ForEncoding.encode(array.as_ref()).unwrap();
        let bc = u32::from_le_bytes(chunk.params[2..6].try_into().unwrap());
        assert_eq!(bc, 3);
    }

    #[test]
    fn int_round_trip_full_i64_range() {
        // Forces bit_width = 64 — the widest scalar fallback path.
        let array: ArrayRef = Arc::new(Int64Array::from(vec![i64::MIN, i64::MAX, 0, -1, 1]));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn int_round_trip_block_min_near_i64_min() {
        // block_min = i64::MIN, value spread = u32::MAX → bit_width = 32.
        let max_offset = i64::from(u32::MAX);
        let values: Vec<i64> = (0..BLOCK_SIZE)
            .map(|i| {
                if i % 2 == 0 {
                    i64::MIN
                } else {
                    i64::MIN + max_offset
                }
            })
            .collect();
        let array: ArrayRef = Arc::new(Int64Array::from(values));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.as_ref(), array.as_ref());
        let chunk = ForEncoding.encode(array.as_ref()).unwrap();
        let bw = chunk.payload[8]; // bit_width of first block
        assert_eq!(bw, 32);
    }

    #[test]
    fn int_round_trip_short_final_block() {
        // 129 values → 1 full block (128) + 1 short block (1 value).
        let values: Vec<i64> = (0..129_i64).collect();
        let array: ArrayRef = Arc::new(Int64Array::from(values));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn timestamp_round_trip_has_utc_timezone() {
        let nanos = vec![
            1_700_000_000_000_000_000_i64,
            1_700_000_000_000_000_001,
            1_700_000_000_000_000_002,
        ];
        let array: ArrayRef =
            Arc::new(TimestampNanosecondArray::from(nanos.clone()).with_timezone("UTC"));
        let decoded = round_trip(array.clone(), BqlType::Timestamp);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn estimate_size_matches_encoded_payload() {
        let enc = ForEncoding;
        for values in [
            vec![10_i64, 12, 11, 15, 10],
            vec![42_i64; 256],
            vec![i64::MIN, 0, i64::MAX],
            vec![],
            (0..300_i64).collect::<Vec<_>>(),
        ] {
            let array: ArrayRef = Arc::new(Int64Array::from(values.clone()));
            let estimated = enc.estimate_size(array.as_ref()).unwrap();
            let actual = enc.encode(array.as_ref()).unwrap().payload.len();
            assert_eq!(
                estimated,
                actual,
                "estimate mismatch for values with len {}",
                values.len()
            );
        }
    }

    #[test]
    fn payload_is_padded_to_multiple_of_eight_per_block() {
        // Use 5 values → 1 block with block_len = 5. Payload per block:
        // 9 bytes header + padded(ceil(5 * bit_width / 8)).
        let values: Vec<i64> = vec![0, 1, 2, 3, 4];
        let array: ArrayRef = Arc::new(Int64Array::from(values));
        let chunk = ForEncoding.encode(array.as_ref()).unwrap();
        // Total payload = BLOCK_HEADER_LEN + block_packed_byte_len(5, 3)
        // block_packed_byte_len(5, 3) = ceil(15/8) = 2 bytes → padded to 8.
        // Expected payload = 9 + 8 = 17 bytes.
        // But we only care that the block packed section is padded:
        let packed_offset = BLOCK_HEADER_LEN;
        let packed_len = chunk.payload.len() - packed_offset;
        assert_eq!(
            packed_len % PADDING_GRANULARITY,
            0,
            "packed section must be padded to 8-byte boundary"
        );
    }

    #[test]
    fn encode_rejects_nullable_input() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![Some(1_i64), None, Some(3)]));
        let err = ForEncoding.encode(array.as_ref()).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("null_count")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_wrong_encoding_discriminant() {
        let chunk = EncodedChunk {
            encoding: EncodingType::Plain,
            params: vec![0u8; PARAMS_LEN],
            payload: Vec::new(),
            row_count: 0,
        };
        let err = ForEncoding.decode(&chunk, &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("Plain")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_malformed_params_length() {
        let chunk = EncodedChunk {
            encoding: EncodingType::For,
            params: vec![0u8; 4], // too short (need 6)
            payload: Vec::new(),
            row_count: 0,
        };
        let err = ForEncoding.decode(&chunk, &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(
                msg.contains("6 bytes"),
                "error should mention expected param size; got: {msg}"
            ),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_bad_block_size() {
        let mut params = vec![0u8; PARAMS_LEN];
        // Write block_size = 64 (not 128) into the first 2 bytes.
        params[..2].copy_from_slice(&64_u16.to_le_bytes());
        params[2..6].copy_from_slice(&0_u32.to_le_bytes()); // block_count = 0
        let chunk = EncodedChunk {
            encoding: EncodingType::For,
            params,
            payload: Vec::new(),
            row_count: 0,
        };
        let err = ForEncoding.decode(&chunk, &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(
                msg.contains("128"),
                "error should name the expected block_size; got: {msg}"
            ),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_bit_width_zero() {
        // Encode a chunk manually with bit_width = 0 in the block header.
        let mut params = vec![0u8; PARAMS_LEN];
        params[..2].copy_from_slice(&(BLOCK_SIZE as u16).to_le_bytes());
        params[2..6].copy_from_slice(&1_u32.to_le_bytes()); // block_count = 1
                                                            // Build a minimal payload: block_min (i64) + bit_width (0) + 8 zero bytes.
        let mut payload = 0_i64.to_le_bytes().to_vec();
        payload.push(0u8); // bit_width = 0 — illegal
        payload.extend_from_slice(&[0u8; 8]);
        let chunk = EncodedChunk {
            encoding: EncodingType::For,
            params,
            payload,
            row_count: 1,
        };
        let err = ForEncoding.decode(&chunk, &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(
                msg.contains("1..=64"),
                "error should mention the valid range; got: {msg}"
            ),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn select_block_frame_all_identical_yields_width_one() {
        let (min, width) = select_block_frame(&[42_i64; 8]);
        assert_eq!(min, 42);
        assert_eq!(width, 1);
    }

    #[test]
    fn select_block_frame_full_i64_range_yields_width_sixtyfour() {
        let (min, width) = select_block_frame(&[i64::MIN, i64::MAX]);
        assert_eq!(min, i64::MIN);
        assert_eq!(width, 64);
    }

    #[test]
    fn block_packed_byte_len_pads_to_multiple_of_eight() {
        assert_eq!(block_packed_byte_len(1, 3), 8); // 3 bits → padded to 8
        assert_eq!(block_packed_byte_len(128, 1), 16); // 128 bits = 16 bytes (already multiple)
        assert_eq!(block_packed_byte_len(128, 3), 48); // 384 bits = 48 bytes (already multiple)
        assert_eq!(block_packed_byte_len(5, 3), 8); // 15 bits → 2 bytes → padded to 8
        assert_eq!(block_packed_byte_len(0, 1), 0); // empty
    }

    #[test]
    fn write_read_bits_round_trip_every_width() {
        for width in 1..=64usize {
            let mask = if width == 64 {
                u64::MAX
            } else {
                (1u64 << width) - 1
            };
            let test_values: Vec<u64> = (0..5)
                .map(|i| (0x0123_4567_89AB_CDEF_u64.wrapping_mul(i + 1)) & mask)
                .collect();
            let byte_count = (5 * width).div_ceil(8).div_ceil(8) * 8;
            let mut bytes = vec![0u8; byte_count];
            for (i, &v) in test_values.iter().enumerate() {
                write_bits(&mut bytes, i * width, width, v);
            }
            let decoded: Vec<u64> = (0..5)
                .map(|i| read_bits(&bytes, i * width, width))
                .collect();
            assert_eq!(decoded, test_values, "mismatch at width {width}");
        }
    }

    #[test]
    fn for_beats_global_bitpacking_on_clustered_data() {
        // Simulate a column with two clusters: [100..116] for rows 0-127
        // and [10000..10016] for rows 128-255. Global BitPacking needs
        // ~14 bits (range 10015 - 100 ≈ 9915 → 14 bits). FOR uses 4 bits
        // per block.
        let mut values = Vec::with_capacity(256);
        for i in 0..128_i64 {
            values.push(100 + (i % 16));
        }
        for i in 0..128_i64 {
            values.push(10000 + (i % 16));
        }
        let array: ArrayRef = Arc::new(Int64Array::from(values));
        let for_chunk = ForEncoding.encode(array.as_ref()).unwrap();
        let decoded = ForEncoding.decode(&for_chunk, &BqlType::Int).unwrap();
        assert_eq!(decoded.as_ref(), array.as_ref());

        // Verify FOR payload is smaller than global BitPacking would need.
        // Global BP: range ≈ 9915 → 14 bits → 256*14/8 = 448 bytes payload.
        // FOR: 2 blocks × (9 header + 4-bit packed: 128*4/8=64 bytes) = 2*(9+64) = 146 bytes.
        assert!(
            for_chunk.payload.len() < 200,
            "FOR payload for clustered data should be well under 200 bytes, got {}",
            for_chunk.payload.len()
        );
    }
}
