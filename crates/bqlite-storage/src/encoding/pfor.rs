//! Patched Frame-of-Reference (PFOR) encoding — per-block integer packing
//! with an outlier "patch list".
//!
//! PFOR extends [`super::ForEncoding`] (TASK-415) by letting each block
//! pick a `main_width` narrower than the full block range and store the
//! values that exceed that width in a per-block patch list. For columns
//! where most values cluster in a narrow range but a small fraction are
//! wide-range outliers (e.g. "amount" columns with a long tail), PFOR
//! compresses ~2.5× better than plain FOR and ~5× better than global
//! BitPacking — see `docs/design/storage/advanced-encodings.md` §6.2.
//!
//! The on-disk layout is pinned by
//! `docs/design/storage/segment-format-v2.md` §5.5:
//!
//! ```text
//! encoding_params:
//!     block_size:    u16 LE   // always 128 in v2
//!     block_count:   u32 LE   // ceil(non_null_count / block_size)
//! payload (repeated block_count times):
//!     block_min:     i64 LE   // minimum non-outlier value
//!     main_width:    u8       // narrow bit width for non-outliers (1..=64)
//!     patch_count:   u16 LE   // number of outliers in this block
//!     packed_main:   ceil(block_len × main_width / 8) bytes, padded
//!                    to the next multiple of 8 bytes (SIMD tail safety)
//!     patch_indices: [u16 LE; patch_count]  // ascending positions within
//!                                           // the block (strictly monotonic)
//!     patch_values:  [i64 LE; patch_count]  // actual outlier values
//!                                           // (not offsets — decode scatters
//!                                           // them directly into output)
//! ```
//!
//! Outlier slots in `packed_main` are filled with a zero offset; the
//! decoder first reconstructs `block_min + offset[i]` for every
//! position, then overwrites outlier positions with `patch_values`.
//!
//! # Type support
//!
//! PFOR applies to `Int` (`Int64Array`) and `Timestamp`
//! (`TimestampNanosecondArray`). All other types are inapplicable.
//!
//! # Null handling
//!
//! Same as FOR: the writer strips nulls before encoding; passing a
//! nullable array is a contract violation caught by [`super::require_dense`].
//!
//! # Edge cases
//!
//! - `row_count == 0`: legal. Params carry `block_count = 0`, payload is empty.
//! - Single block (`row_count ≤ 128`): `block_count = 1`, `block_len = row_count`.
//! - Short final block: `block_len < 128`, packed section still padded to 8 bytes.
//! - All identical in a block: `main_width = 1`, `patch_count = 0` — degenerates
//!   to a FOR block.
//! - Zero patches: `patch_count == 0` — PFOR matches FOR block layout except
//!   for the 2-byte `patch_count` field.
//! - All outliers (`patch_count == block_len`): `main_width = 1`, packed_main
//!   is zero-filled, every value lives in the patch list. This is worse than
//!   `Plain` — the selector guard (TASK-419) must prevent selection at high
//!   outlier fractions per §6.7.
//!
//! # Decode scatter validation
//!
//! The decoder rejects `patch_indices` that are not strictly monotonic
//! ascending (catches corrupt segments where duplicates would silently
//! resolve as last-write-wins) and that fall outside `[0, block_len)`.
//!
//! # Selector guard
//!
//! Per `advanced-encodings.md` §6.7: choose PFOR when 1% < outlier
//! fraction < 10% of a block's values exceed FOR's bit width. The
//! guard lives in the selector (TASK-419), not here.
//! [`Pfor::estimate_size`] returns the exact payload size so the
//! selector can compare accurately against FOR / BitPacking.
//!
//! # Hot path
//!
//! Full 128-value blocks with `main_width ≤ 32` use `bitpacking::BitPacker4x`
//! for SSE3/NEON-accelerated packing and unpacking of the main stream.
//! Short final blocks and `main_width > 32` fall back to a scalar bit
//! loop. The patch scatter is a short sequential loop; at the ≤10%
//! outlier cap it touches at most 10% of cache lines.
//!
//! # Implementation note: the `fastpfor` hint in TASKS.md
//!
//! TASKS.md says "Use the fastpfor crate instead of implementing
//! manually." The Rust `fastpfor` crate implements FastPFOR
//! (Lemire 2015), a different block/exception scheme than the Zukowski
//! 2006 PFOR the design doc fixes. The on-disk byte format in
//! `segment-format-v2.md` §5.5 is the authoritative contract — it
//! pins the patch-list layout, field sizes, and padding rules. Using
//! `fastpfor` would force a format-version bump and a full
//! segment-reader rewrite. The design doc wins; we reuse the
//! `bitpacking` crate's `BitPacker4x` fast path exactly the way FOR
//! does.

use arrow::array::{Array, ArrayRef, Int64Array, TimestampNanosecondArray};
use arrow::datatypes::{DataType, TimeUnit};
use bitpacking::{BitPacker, BitPacker4x};
use bqlite_core::{BqlType, BqliteError, Result};
use std::sync::Arc;

use super::{require_dense, BorrowedEncodedChunk, EncodedChunk, Encoding, EncodingType};

/// Zero-sized marker for the PFOR encoding.
///
/// Stateless; freely clonable and stored behind a `Box<dyn Encoding>`.
/// See the module-level documentation for the byte layout this impl produces.
#[derive(Debug, Clone, Copy, Default)]
pub struct Pfor;

impl Pfor {
    /// Construct a new Pfor encoder. PFOR has no configuration.
    pub const fn new() -> Self {
        Pfor
    }
}

/// Block size (number of values per PFOR block). Fixed at 128 in v2 to match
/// `BitPacker4x::BLOCK_LEN` for SIMD-accelerated packing on full blocks.
const BLOCK_SIZE: usize = <BitPacker4x as BitPacker>::BLOCK_LEN; // = 128

/// Encoding-params block size: `block_size: u16 LE` (2 bytes) + `block_count: u32 LE` (4 bytes).
const PARAMS_LEN: usize = 6;

/// Per-block header size: `block_min: i64 LE` (8) + `main_width: u8` (1) + `patch_count: u16 LE` (2).
const BLOCK_HEADER_LEN: usize = 11;

/// Per-patch entry size: `patch_index: u16 LE` (2) + `patch_value: i64 LE` (8).
const PATCH_ENTRY_LEN: usize = 10;

/// Payload padding granularity per v2 §5.5 — packed_main is always
/// padded to the next multiple of 8 bytes for SIMD tail safety.
const PADDING_GRANULARITY: usize = 8;

/// `BitPacker4x` only accepts `u32`, so widths above 32 fall to scalar.
const BITPACKER4X_MAX_BIT_WIDTH: u8 = 32;

impl Encoding for Pfor {
    fn encoding_type(&self) -> EncodingType {
        EncodingType::PFor
    }

    fn applicable_to(&self, ty: &BqlType) -> bool {
        matches!(ty, BqlType::Int | BqlType::Timestamp)
    }

    /// Returns the exact encoded payload byte count (not an over-estimate).
    ///
    /// PFOR's estimate is exact because `select_block_frame_pfor` is a
    /// deterministic function of the block values and every byte the
    /// encoder writes is accounted for block-by-block. The selector
    /// (TASK-419) compares this against FOR's / BitPacking's estimates
    /// to decide which codec wins.
    fn estimate_size(&self, array: &dyn Array) -> Result<usize> {
        let values = values_as_i64(array)?;
        Ok(compute_payload_size(values))
    }

    fn encode(&self, array: &dyn Array) -> Result<EncodedChunk> {
        require_dense(array, "Pfor")?;
        let values = values_as_i64(array)?;
        let row_count = array.len();

        let block_count = row_count.div_ceil(BLOCK_SIZE);

        // Encoding params: block_size (u16) + block_count (u32) = 6 bytes.
        let mut params = Vec::with_capacity(PARAMS_LEN);
        params.extend_from_slice(&(BLOCK_SIZE as u16).to_le_bytes());
        params.extend_from_slice(&(block_count as u32).to_le_bytes());

        // Payload: per-block header + packed_main + patch lists.
        let mut payload: Vec<u8> = Vec::with_capacity(compute_payload_size(values));
        for block_values in values.chunks(BLOCK_SIZE) {
            let (block_min, main_width, patch_mask) = select_block_frame_pfor(block_values);
            let patch_count = patch_mask.iter().filter(|&&b| b).count();
            debug_assert!(patch_count <= u16::MAX as usize);

            payload.extend_from_slice(&block_min.to_le_bytes());
            payload.push(main_width);
            payload.extend_from_slice(&(patch_count as u16).to_le_bytes());

            let packed = pack_block_main(block_values, block_min, main_width, &patch_mask);
            payload.extend_from_slice(&packed);

            // Patch list: indices first (u16 LE), then values (i64 LE),
            // both in ascending-index order.
            for (i, &is_patch) in patch_mask.iter().enumerate() {
                if is_patch {
                    payload.extend_from_slice(&(i as u16).to_le_bytes());
                }
            }
            for (i, &is_patch) in patch_mask.iter().enumerate() {
                if is_patch {
                    payload.extend_from_slice(&block_values[i].to_le_bytes());
                }
            }
        }

        Ok(EncodedChunk {
            encoding: EncodingType::PFor,
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
    if encoding != EncodingType::PFor {
        return Err(BqliteError::Execution(format!(
            "Pfor::decode called on a {:?} chunk — dispatch must \
             route each chunk to its declared encoding's decoder",
            encoding
        )));
    }
    if params.len() != PARAMS_LEN {
        return Err(BqliteError::Execution(format!(
            "Pfor::decode: encoding params must be exactly {} bytes \
             (u16 block_size + u32 block_count), got {}",
            PARAMS_LEN,
            params.len()
        )));
    }

    let block_size = u16::from_le_bytes(params[..2].try_into().unwrap()) as usize;
    let block_count = u32::from_le_bytes(params[2..6].try_into().unwrap()) as usize;

    if block_size != BLOCK_SIZE {
        return Err(BqliteError::Execution(format!(
            "Pfor::decode: block_size must be {} per \
             segment-format-v2.md §5.5, got {block_size}",
            BLOCK_SIZE
        )));
    }

    let expected_block_count = row_count.div_ceil(BLOCK_SIZE);
    if block_count != expected_block_count {
        return Err(BqliteError::Execution(format!(
            "Pfor::decode: block_count {block_count} is inconsistent \
             with row_count {row_count} (expected {expected_block_count})"
        )));
    }

    let mut values: Vec<i64> = Vec::with_capacity(row_count);
    let mut payload_cursor = 0usize;

    for block_idx in 0..block_count {
        let block_start = block_idx * BLOCK_SIZE;
        let block_len = (row_count - block_start).min(BLOCK_SIZE);

        // Read block header: block_min (i64 LE) + main_width (u8) + patch_count (u16 LE).
        if payload_cursor + BLOCK_HEADER_LEN > payload.len() {
            return Err(BqliteError::Execution(format!(
                "Pfor::decode: payload truncated at block {block_idx}: \
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
        let main_width = payload[payload_cursor + 8];
        let patch_count = u16::from_le_bytes(
            payload[payload_cursor + 9..payload_cursor + 11]
                .try_into()
                .unwrap(),
        ) as usize;
        payload_cursor += BLOCK_HEADER_LEN;

        if main_width == 0 || main_width > 64 {
            return Err(BqliteError::Execution(format!(
                "Pfor::decode: main_width must be in 1..=64 per \
                 segment-format-v2.md §5.5, got {main_width} in block {block_idx}"
            )));
        }
        if patch_count > block_len {
            return Err(BqliteError::Execution(format!(
                "Pfor::decode: patch_count {patch_count} exceeds block_len \
                 {block_len} in block {block_idx}"
            )));
        }

        // Unpack packed_main.
        let packed_len = block_packed_byte_len(block_len, main_width);
        if payload_cursor + packed_len > payload.len() {
            return Err(BqliteError::Execution(format!(
                "Pfor::decode: payload truncated at block {block_idx}: \
                 expected {packed_len} packed_main bytes at offset {payload_cursor}, \
                 payload is {} bytes",
                payload.len()
            )));
        }
        let packed = &payload[payload_cursor..payload_cursor + packed_len];
        payload_cursor += packed_len;

        let offsets = unpack_block_offsets(packed, block_len, main_width);

        // Assemble output[i] = block_min + offset[i]. Patched positions have
        // offset = 0 here; the scatter overwrites them next.
        let values_start = values.len();
        for offset in offsets {
            values.push(((block_min as i128) + (offset as i128)) as i64);
        }

        // Parse patch list: all indices first, then all values.
        let patch_bytes_len = patch_count * PATCH_ENTRY_LEN;
        if payload_cursor + patch_bytes_len > payload.len() {
            return Err(BqliteError::Execution(format!(
                "Pfor::decode: payload truncated in patch list at block \
                 {block_idx}: expected {patch_bytes_len} patch bytes at \
                 offset {payload_cursor}, payload is {} bytes",
                payload.len()
            )));
        }
        let indices_start = payload_cursor;
        let values_section_start = indices_start + patch_count * 2;
        payload_cursor += patch_bytes_len;

        let mut prev_idx: Option<usize> = None;
        for k in 0..patch_count {
            let idx_bytes: [u8; 2] = payload[indices_start + k * 2..indices_start + k * 2 + 2]
                .try_into()
                .unwrap();
            let idx = u16::from_le_bytes(idx_bytes) as usize;
            if idx >= block_len {
                return Err(BqliteError::Execution(format!(
                    "Pfor::decode: patch_index {idx} out of range for \
                     block_len {block_len} in block {block_idx}"
                )));
            }
            if let Some(prev) = prev_idx {
                if idx <= prev {
                    return Err(BqliteError::Execution(format!(
                        "Pfor::decode: patch_indices must be strictly \
                         monotonic ascending; got {idx} after {prev} in \
                         block {block_idx}"
                    )));
                }
            }
            prev_idx = Some(idx);

            let value_bytes: [u8; 8] = payload
                [values_section_start + k * 8..values_section_start + k * 8 + 8]
                .try_into()
                .unwrap();
            let patch_value = i64::from_le_bytes(value_bytes);
            values[values_start + idx] = patch_value;
        }
    }

    if payload_cursor != payload.len() {
        return Err(BqliteError::Execution(format!(
            "Pfor::decode: {} trailing bytes in payload after decoding \
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
            "Pfor::decode does not support type {other} — PFOR decodes \
             to Int or Timestamp only (segment-format-v2.md §5.5)"
        ))),
    }
}

// ── i64 accessor ─────────────────────────────────────────────────────────────

/// Extract an `&[i64]` view over an Int64Array or TimestampNanosecondArray.
/// PFOR treats both as signed 64-bit integers, same as FOR.
fn values_as_i64(array: &dyn Array) -> Result<&[i64]> {
    match array.data_type() {
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| BqliteError::Execution("Pfor: Int64 downcast failed".into()))?;
            Ok(arr.values())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| {
                    BqliteError::Execution("Pfor: TimestampNanosecondArray downcast failed".into())
                })?;
            Ok(arr.values())
        }
        DataType::List(_) | DataType::Map(_, _) => Err(BqliteError::Execution(format!(
            "Pfor: nested type {:?} is not supported — PFOR encoding \
             covers Int64 and Timestamp(Nanosecond) only; nested types are deferred",
            array.data_type()
        ))),
        other => Err(BqliteError::Execution(format!(
            "Pfor: unsupported Arrow type {other:?} — PFOR encoding \
             covers Int64 and Timestamp(Nanosecond) only"
        ))),
    }
}

// ── block frame selection ─────────────────────────────────────────────────────

/// Pick `(block_min, main_width, patch_mask)` for a PFOR block.
///
/// Strategy: for each candidate width in `1..=full_width`, count how
/// many offsets exceed `(1 << w) - 1` (outliers) and compute total block
/// bytes. Return the width that minimises bytes; tie-break prefers the
/// smallest width (fewer bits per main slot = faster bit-unpack).
///
/// `patch_mask[i]` is true iff position `i` goes into the patch list.
///
/// An empty block returns `(0, 1, vec![])`.
fn select_block_frame_pfor(block: &[i64]) -> (i64, u8, Vec<bool>) {
    if block.is_empty() {
        return (0, 1, Vec::new());
    }

    let mut min_val = block[0];
    let mut max_val = block[0];
    for &v in &block[1..] {
        if v < min_val {
            min_val = v;
        }
        if v > max_val {
            max_val = v;
        }
    }

    // Compute unsigned offsets in u64. The difference is computed in i128
    // to avoid overflow when `block_min` is near `i64::MIN`; the result is
    // always in `0 ..= (i64::MAX - i64::MIN) == 2^64 - 1`, so the `as u64`
    // cast is safe.
    let block_min = min_val;
    let block_min_i128 = block_min as i128;
    let offsets: Vec<u64> = block
        .iter()
        .map(|&v| ((v as i128) - block_min_i128) as u64)
        .collect();
    let max_offset = *offsets.iter().max().unwrap();

    // full_width: FOR's choice. 1-bit floor matches v2 §5.5.
    let full_width = if max_offset == 0 {
        1u8
    } else {
        (64 - max_offset.leading_zeros()) as u8
    };

    // If full_width is 1 there's nothing narrower to try — this is the
    // all-identical fast path. No patches possible.
    if full_width == 1 {
        return (block_min, 1, vec![false; block.len()]);
    }

    // Search 1..=full_width for the width minimizing total block bytes.
    let block_len = block.len();
    let mut best_width = full_width;
    let mut best_mask: Vec<bool> = offsets.iter().map(|_| false).collect();
    let mut best_size = BLOCK_HEADER_LEN + block_packed_byte_len(block_len, full_width);

    // Precompute a per-width patch mask threshold.
    for w in 1u8..=full_width {
        // Threshold for "fits in w bits": offset < 2^w. `1u64 << 64` is
        // undefined behavior in Rust, so we special-case w == 64 to "no
        // threshold" (every u64 fits, patch_count = 0 — identical to FOR).
        let threshold: Option<u64> = if w == 64 { None } else { Some(1u64 << w) };
        let mut mask = vec![false; block_len];
        let mut patch_count = 0usize;
        for (i, &off) in offsets.iter().enumerate() {
            let is_patch = match threshold {
                Some(t) => off >= t,
                None => false,
            };
            if is_patch {
                mask[i] = true;
                patch_count += 1;
            }
        }
        let size =
            BLOCK_HEADER_LEN + block_packed_byte_len(block_len, w) + patch_count * PATCH_ENTRY_LEN;
        if size < best_size {
            best_size = size;
            best_width = w;
            best_mask = mask;
        }
    }

    (block_min, best_width, best_mask)
}

// ── block payload sizing ──────────────────────────────────────────────────────

/// Number of packed-payload bytes for a block of `block_len` values at
/// `bit_width` bits each, padded to the next multiple of [`PADDING_GRANULARITY`].
///
/// Mirrors FOR's `block_packed_byte_len` so the two encodings share the
/// same padding conventions.
fn block_packed_byte_len(block_len: usize, bit_width: u8) -> usize {
    if block_len == 0 {
        return 0;
    }
    let total_bits = block_len.saturating_mul(bit_width as usize);
    let packed_bytes = total_bits.div_ceil(8);
    packed_bytes.div_ceil(PADDING_GRANULARITY) * PADDING_GRANULARITY
}

/// Total payload byte count for encoding `values` with the PFOR block layout.
/// Each block contributes `BLOCK_HEADER_LEN + packed_main + patch_count * PATCH_ENTRY_LEN`.
/// Returns 0 for an empty slice.
fn compute_payload_size(values: &[i64]) -> usize {
    if values.is_empty() {
        return 0;
    }
    let mut total = 0usize;
    for block in values.chunks(BLOCK_SIZE) {
        let (_, main_width, patch_mask) = select_block_frame_pfor(block);
        let patch_count = patch_mask.iter().filter(|&&b| b).count();
        total += BLOCK_HEADER_LEN
            + block_packed_byte_len(block.len(), main_width)
            + patch_count * PATCH_ENTRY_LEN;
    }
    total
}

// ── block bit packing / unpacking ─────────────────────────────────────────────

/// Number of bytes `BitPacker4x::compress` writes for a full 128-value block
/// at the given bit width.
fn bitpacker4x_block_bytes(bit_width: u8) -> usize {
    (BLOCK_SIZE * bit_width as usize) / 8
}

/// Pack `block`'s main stream into a padded bit stream. Outlier positions
/// (where `patch_mask[i]` is true) are encoded as zero offsets — the
/// decoder will overwrite them during the patch scatter.
///
/// Returns exactly `block_packed_byte_len(block.len(), main_width)` bytes,
/// with zero-padding at the end.
///
/// Fast path: full 128-value blocks with `main_width ≤ 32` use `BitPacker4x`.
/// Scalar path: short final blocks and `main_width > 32`.
fn pack_block_main(block: &[i64], block_min: i64, main_width: u8, patch_mask: &[bool]) -> Vec<u8> {
    let block_len = block.len();
    let mut bytes = vec![0u8; block_packed_byte_len(block_len, main_width)];
    if block_len == 0 {
        return bytes;
    }
    let width = main_width as usize;

    // Effective offsets: 0 at patched positions, actual offset otherwise.
    // (The patched positions will be overwritten on decode by the scatter.)
    let block_min_i128 = block_min as i128;

    // Fast path: full 128-value block with main_width ≤ 32 → BitPacker4x.
    if block_len == BLOCK_SIZE && main_width <= BITPACKER4X_MAX_BIT_WIDTH {
        let bitpacker = BitPacker4x::new();
        let mut block_u32 = [0u32; BLOCK_SIZE];
        for (i, slot) in block_u32.iter_mut().enumerate() {
            if patch_mask[i] {
                *slot = 0;
                continue;
            }
            let offset = ((block[i] as i128) - block_min_i128) as u64;
            *slot = u32::try_from(offset).expect(
                "non-patched offset must fit in main_width ≤ 32 bits; select_block_frame_pfor bug",
            );
        }
        let written = bitpacker.compress(&block_u32, &mut bytes, main_width);
        debug_assert_eq!(written, bitpacker4x_block_bytes(main_width));
        return bytes;
    }

    // Scalar path: short final block or main_width > 32.
    for i in 0..block_len {
        let value_bits = if patch_mask[i] {
            0u64
        } else {
            let offset = ((block[i] as i128) - block_min_i128) as u64;
            // For non-patched positions, offset must fit in `width` bits by
            // construction. In the width == 64 case mask any value is fine.
            if width < 64 {
                debug_assert!(
                    offset < (1u64 << width),
                    "non-patched offset {offset} must fit in width {width}"
                );
            }
            offset
        };
        write_bits(&mut bytes, i * width, width, value_bits);
    }
    bytes
}

/// Unpack `block_len` bit-packed offsets out of `bytes` at `bit_width` bits each.
///
/// Fast path: full 128-value blocks with `bit_width ≤ 32` use `BitPacker4x`.
/// Scalar path: short final blocks and `bit_width > 32`.
///
/// Duplicated from FOR's decode helper to keep this module self-contained
/// (FOR's module comment explicitly sanctions this duplication).
fn unpack_block_offsets(bytes: &[u8], block_len: usize, bit_width: u8) -> Vec<u64> {
    let mut offsets = Vec::with_capacity(block_len);
    if block_len == 0 {
        return offsets;
    }
    let width = bit_width as usize;

    if block_len == BLOCK_SIZE && bit_width <= BITPACKER4X_MAX_BIT_WIDTH {
        let bitpacker = BitPacker4x::new();
        let block_bytes = bitpacker4x_block_bytes(bit_width);
        let mut block_u32 = [0u32; BLOCK_SIZE];
        bitpacker.decompress(&bytes[..block_bytes], &mut block_u32, bit_width);
        offsets.extend(block_u32.iter().copied().map(u64::from));
        return offsets;
    }

    for i in 0..block_len {
        offsets.push(read_bits(bytes, i * width, width));
    }
    offsets
}

/// Write `width` bits of `value` at bit offset `start` into `out`,
/// LSB-first within each byte. `width` must be ≤ 64 and
/// `value >> width` must be zero.
///
/// Mirrors the identical helper in `for_encoding.rs`.
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
/// Mirrors the identical helper in `for_encoding.rs`.
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
        let chunk = Pfor.encode(array.as_ref()).unwrap();
        Pfor.decode(&chunk, &ty).unwrap()
    }

    #[test]
    fn applicable_to_covers_int_and_timestamp_only() {
        let enc = Pfor;
        assert!(enc.applicable_to(&BqlType::Int));
        assert!(enc.applicable_to(&BqlType::Timestamp));
        assert!(!enc.applicable_to(&BqlType::Bool));
        assert!(!enc.applicable_to(&BqlType::Float));
        assert!(!enc.applicable_to(&BqlType::String));
    }

    #[test]
    fn encoding_type_is_pfor_discriminant_nine() {
        assert_eq!(Pfor.encoding_type(), EncodingType::PFor);
        assert_eq!(Pfor.encoding_type().discriminant(), 9);
    }

    #[test]
    fn params_are_exactly_six_bytes() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2, 3]));
        let chunk = Pfor.encode(array.as_ref()).unwrap();
        assert_eq!(chunk.params.len(), PARAMS_LEN);
    }

    #[test]
    fn params_block_size_is_128() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2, 3]));
        let chunk = Pfor.encode(array.as_ref()).unwrap();
        let block_size = u16::from_le_bytes(chunk.params[..2].try_into().unwrap());
        assert_eq!(block_size, 128);
    }

    #[test]
    fn round_trip_empty() {
        let array: ArrayRef = Arc::new(Int64Array::from(Vec::<i64>::new()));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.len(), 0);
        let chunk = Pfor.encode(array.as_ref()).unwrap();
        assert_eq!(chunk.payload.len(), 0);
        let bc = u32::from_le_bytes(chunk.params[2..6].try_into().unwrap());
        assert_eq!(bc, 0);
    }

    #[test]
    fn round_trip_single_value() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![42_i64]));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn round_trip_all_identical() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![7_i64; 256]));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.as_ref(), array.as_ref());
        let chunk = Pfor.encode(array.as_ref()).unwrap();
        // All-identical: main_width = 1, patch_count = 0 for every block.
        // First block header is at offset 0: skip 8 bytes of block_min.
        let main_width = chunk.payload[8];
        let patch_count = u16::from_le_bytes(chunk.payload[9..11].try_into().unwrap());
        assert_eq!(main_width, 1);
        assert_eq!(patch_count, 0);
    }

    #[test]
    fn round_trip_narrow_range_no_patches() {
        // 128 values in [100, 115] → 4 bits, no patches.
        let values: Vec<i64> = (0..128).map(|i| 100 + (i % 16) as i64).collect();
        let array: ArrayRef = Arc::new(Int64Array::from(values));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.as_ref(), array.as_ref());
        let chunk = Pfor.encode(array.as_ref()).unwrap();
        let patch_count = u16::from_le_bytes(chunk.payload[9..11].try_into().unwrap());
        assert_eq!(patch_count, 0, "narrow-range block should have no patches");
    }

    #[test]
    fn round_trip_short_final_block() {
        // 129 values → 1 full block (128) + 1 short block (1 value).
        let values: Vec<i64> = (0..129_i64).collect();
        let array: ArrayRef = Arc::new(Int64Array::from(values));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn round_trip_multiple_blocks() {
        // 300 values → 3 blocks (128, 128, 44).
        let values: Vec<i64> = (0..300_i64).collect();
        let array: ArrayRef = Arc::new(Int64Array::from(values));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.as_ref(), array.as_ref());
        let chunk = Pfor.encode(array.as_ref()).unwrap();
        let bc = u32::from_le_bytes(chunk.params[2..6].try_into().unwrap());
        assert_eq!(bc, 3);
    }

    #[test]
    fn round_trip_full_i64_range() {
        // Forces main_width = 64 — the widest scalar fallback path, no patches
        // (since every value is within range, patches wouldn't save anything).
        let array: ArrayRef = Arc::new(Int64Array::from(vec![i64::MIN, i64::MAX, 0, -1, 1]));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn round_trip_block_min_near_i64_min() {
        // block_min = i64::MIN, value spread = u32::MAX → main_width = 32.
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
    }

    #[test]
    fn round_trip_one_outlier_per_block() {
        // 127 small values + 1 outlier. Expected: main_width ≈ 8 (for the
        // small values), 1 patch (for the outlier).
        let mut values: Vec<i64> = (0..127).map(|i| 100 + i as i64).collect();
        values.push(i64::MAX - 1);
        assert_eq!(values.len(), 128);
        let array: ArrayRef = Arc::new(Int64Array::from(values));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.as_ref(), array.as_ref());

        let chunk = Pfor.encode(array.as_ref()).unwrap();
        let main_width = chunk.payload[8];
        let patch_count = u16::from_le_bytes(chunk.payload[9..11].try_into().unwrap());
        assert!(
            main_width < 40,
            "PFOR should pick a narrow main_width for the 127 small values, got {main_width}"
        );
        assert_eq!(patch_count, 1, "exactly one value is an outlier");
    }

    #[test]
    fn round_trip_five_percent_outliers() {
        // Approx the §6.2 worked example at one block: 95% of values in
        // [0,255], 5% in [0, 2^31]. Seed-deterministic for test stability.
        let mut values: Vec<i64> = Vec::with_capacity(128);
        for i in 0..128 {
            // Simple LCG for determinism without pulling in rand.
            let pseudo = (i as u64).wrapping_mul(2_654_435_761);
            if pseudo.is_multiple_of(20) {
                // ~5% outliers.
                values.push((pseudo % (1u64 << 31)) as i64);
            } else {
                values.push((pseudo % 256) as i64);
            }
        }
        let array: ArrayRef = Arc::new(Int64Array::from(values.clone()));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn round_trip_worst_case_wide_block() {
        // Round-trip on the worst-case wide block: alternating 0 and
        // i64::MAX → full_width = 64. The frame selector picks
        // main_width = 64 with 0 patches here (11 + 1024 = 1035 bytes),
        // because the all-patched alternative (1-bit main + 128 patches
        // = 11 + 16 + 128*10 = 1307 bytes) is larger. The all-patched
        // degenerate decode path is exercised by
        // `encoder_can_produce_all_patched_block_manually` below.
        let mut values: Vec<i64> = Vec::with_capacity(128);
        for i in 0..128 {
            values.push(if i % 2 == 0 { 0 } else { i64::MAX });
        }
        let array: ArrayRef = Arc::new(Int64Array::from(values));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn encoder_can_produce_all_patched_block_manually() {
        // Verify the decoder handles an all-patched block even though the
        // encoder's selector would never pick it. Build the chunk bytes by
        // hand: 2-value block, main_width = 1, patch_count = 2.
        //
        // packed_main: 2 bits → padded to 8 bytes (all zero).
        // patches: indices [0, 1], values [5, 9].
        let mut payload = Vec::<u8>::new();
        payload.extend_from_slice(&0_i64.to_le_bytes()); // block_min
        payload.push(1); // main_width = 1
        payload.extend_from_slice(&2_u16.to_le_bytes()); // patch_count = 2
        payload.extend_from_slice(&[0u8; 8]); // packed_main, zero-filled
        payload.extend_from_slice(&0_u16.to_le_bytes()); // index 0
        payload.extend_from_slice(&1_u16.to_le_bytes()); // index 1
        payload.extend_from_slice(&5_i64.to_le_bytes()); // value 5
        payload.extend_from_slice(&9_i64.to_le_bytes()); // value 9

        let mut params = Vec::<u8>::new();
        params.extend_from_slice(&(BLOCK_SIZE as u16).to_le_bytes());
        params.extend_from_slice(&1_u32.to_le_bytes()); // 1 block

        let chunk = EncodedChunk {
            encoding: EncodingType::PFor,
            params,
            payload,
            row_count: 2,
        };
        let decoded = Pfor.decode(&chunk, &BqlType::Int).unwrap();
        let expected: ArrayRef = Arc::new(Int64Array::from(vec![5_i64, 9]));
        assert_eq!(decoded.as_ref(), expected.as_ref());
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
        let enc = Pfor;
        let cases: Vec<Vec<i64>> = vec![
            vec![10_i64, 12, 11, 15, 10],
            vec![42_i64; 256],
            vec![i64::MIN, 0, i64::MAX],
            vec![],
            (0..300_i64).collect(),
            // Five-percent-outlier pattern at 2 blocks.
            (0..256)
                .map(|i| {
                    let p = (i as u64).wrapping_mul(2_654_435_761);
                    if p.is_multiple_of(20) {
                        (p % (1u64 << 31)) as i64
                    } else {
                        (p % 256) as i64
                    }
                })
                .collect(),
        ];
        for values in cases {
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
        // 5 values → 1 block with block_len = 5, no outliers.
        // Payload per block: 11 bytes header + padded(ceil(5 * main_width / 8)) + 0 patches.
        let values: Vec<i64> = vec![0, 1, 2, 3, 4];
        let array: ArrayRef = Arc::new(Int64Array::from(values));
        let chunk = Pfor.encode(array.as_ref()).unwrap();
        let patch_count = u16::from_le_bytes(chunk.payload[9..11].try_into().unwrap());
        assert_eq!(patch_count, 0);
        let packed_offset = BLOCK_HEADER_LEN;
        let packed_len = chunk.payload.len() - packed_offset;
        assert_eq!(
            packed_len % PADDING_GRANULARITY,
            0,
            "packed_main section must be padded to 8-byte boundary"
        );
    }

    #[test]
    fn encode_rejects_nullable_input() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![Some(1_i64), None, Some(3)]));
        let err = Pfor.encode(array.as_ref()).unwrap_err();
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
        let err = Pfor.decode(&chunk, &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("Plain")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_malformed_params_length() {
        let chunk = EncodedChunk {
            encoding: EncodingType::PFor,
            params: vec![0u8; 4], // too short (need 6)
            payload: Vec::new(),
            row_count: 0,
        };
        let err = Pfor.decode(&chunk, &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("6 bytes")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_bad_block_size() {
        let mut params = vec![0u8; PARAMS_LEN];
        params[..2].copy_from_slice(&64_u16.to_le_bytes());
        params[2..6].copy_from_slice(&0_u32.to_le_bytes());
        let chunk = EncodedChunk {
            encoding: EncodingType::PFor,
            params,
            payload: Vec::new(),
            row_count: 0,
        };
        let err = Pfor.decode(&chunk, &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("128")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_main_width_zero() {
        // Build a chunk with main_width = 0.
        let mut params = vec![0u8; PARAMS_LEN];
        params[..2].copy_from_slice(&(BLOCK_SIZE as u16).to_le_bytes());
        params[2..6].copy_from_slice(&1_u32.to_le_bytes());
        let mut payload = 0_i64.to_le_bytes().to_vec();
        payload.push(0u8); // main_width = 0 — illegal
        payload.extend_from_slice(&0_u16.to_le_bytes()); // patch_count = 0
        payload.extend_from_slice(&[0u8; 8]);
        let chunk = EncodedChunk {
            encoding: EncodingType::PFor,
            params,
            payload,
            row_count: 1,
        };
        let err = Pfor.decode(&chunk, &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("1..=64")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_patch_count_exceeding_block_len() {
        // block_len = 1 but patch_count = 2.
        let mut params = vec![0u8; PARAMS_LEN];
        params[..2].copy_from_slice(&(BLOCK_SIZE as u16).to_le_bytes());
        params[2..6].copy_from_slice(&1_u32.to_le_bytes());
        let mut payload = 0_i64.to_le_bytes().to_vec();
        payload.push(1u8); // main_width
        payload.extend_from_slice(&2_u16.to_le_bytes()); // patch_count = 2 > 1
        payload.extend_from_slice(&[0u8; 8]); // packed_main
        payload.extend_from_slice(&0_u16.to_le_bytes()); // index 0
        payload.extend_from_slice(&0_u16.to_le_bytes()); // index 0 (bogus)
        payload.extend_from_slice(&0_i64.to_le_bytes());
        payload.extend_from_slice(&0_i64.to_le_bytes());
        let chunk = EncodedChunk {
            encoding: EncodingType::PFor,
            params,
            payload,
            row_count: 1,
        };
        let err = Pfor.decode(&chunk, &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("patch_count")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_patch_index_out_of_range() {
        // block_len = 2, but patch_index = 5.
        let mut params = vec![0u8; PARAMS_LEN];
        params[..2].copy_from_slice(&(BLOCK_SIZE as u16).to_le_bytes());
        params[2..6].copy_from_slice(&1_u32.to_le_bytes());
        let mut payload = 0_i64.to_le_bytes().to_vec();
        payload.push(1u8);
        payload.extend_from_slice(&1_u16.to_le_bytes());
        payload.extend_from_slice(&[0u8; 8]);
        payload.extend_from_slice(&5_u16.to_le_bytes()); // out of range
        payload.extend_from_slice(&42_i64.to_le_bytes());
        let chunk = EncodedChunk {
            encoding: EncodingType::PFor,
            params,
            payload,
            row_count: 2,
        };
        let err = Pfor.decode(&chunk, &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("out of range")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_non_monotonic_patch_indices() {
        // block_len = 4, patches at [0, 0] — duplicate, not strictly ascending.
        let mut params = vec![0u8; PARAMS_LEN];
        params[..2].copy_from_slice(&(BLOCK_SIZE as u16).to_le_bytes());
        params[2..6].copy_from_slice(&1_u32.to_le_bytes());
        let mut payload = 0_i64.to_le_bytes().to_vec();
        payload.push(1u8);
        payload.extend_from_slice(&2_u16.to_le_bytes()); // patch_count = 2
        payload.extend_from_slice(&[0u8; 8]); // packed_main
        payload.extend_from_slice(&0_u16.to_le_bytes()); // index 0
        payload.extend_from_slice(&0_u16.to_le_bytes()); // index 0 again — illegal
        payload.extend_from_slice(&1_i64.to_le_bytes());
        payload.extend_from_slice(&2_i64.to_le_bytes());
        let chunk = EncodedChunk {
            encoding: EncodingType::PFor,
            params,
            payload,
            row_count: 4,
        };
        let err = Pfor.decode(&chunk, &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("monotonic")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_trailing_bytes() {
        // Valid 1-value chunk with one extra byte appended.
        let array: ArrayRef = Arc::new(Int64Array::from(vec![42_i64]));
        let mut chunk = Pfor.encode(array.as_ref()).unwrap();
        chunk.payload.push(0xFF);
        let err = Pfor.decode(&chunk, &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("trailing bytes")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn select_block_frame_pfor_all_identical_yields_width_one() {
        let (min, w, mask) = select_block_frame_pfor(&[42_i64; 8]);
        assert_eq!(min, 42);
        assert_eq!(w, 1);
        assert!(mask.iter().all(|&b| !b));
    }

    #[test]
    fn select_block_frame_pfor_converges_to_for_when_no_outliers() {
        // 128 values in [0, 15] → full_width = 4, no single width saves
        // bytes by switching to a narrower main + patches.
        let block: Vec<i64> = (0..128).map(|i| i as i64 % 16).collect();
        let (_, w, mask) = select_block_frame_pfor(&block);
        assert_eq!(w, 4);
        assert!(mask.iter().all(|&b| !b));
    }

    #[test]
    fn select_block_frame_pfor_prefers_narrow_with_one_outlier() {
        // 127 values in [0, 15] + 1 outlier needing 32 bits. FOR would
        // pick w = 32. PFOR should pick a much smaller w (~4–8) with 1 patch.
        let mut block: Vec<i64> = (0..127).map(|i| i as i64 % 16).collect();
        block.push(i64::from(u32::MAX) - 1); // big outlier
        assert_eq!(block.len(), 128);
        let (_, w, mask) = select_block_frame_pfor(&block);
        assert!(w < 32, "PFOR should pick narrow w, got {w}");
        assert_eq!(mask.iter().filter(|&&b| b).count(), 1);
        assert!(mask[127], "outlier position must be patched");
    }

    #[test]
    fn select_block_frame_pfor_empty_block() {
        let (min, w, mask) = select_block_frame_pfor(&[]);
        assert_eq!(min, 0);
        assert_eq!(w, 1);
        assert!(mask.is_empty());
    }

    #[test]
    fn block_packed_byte_len_pads_to_multiple_of_eight() {
        assert_eq!(block_packed_byte_len(1, 3), 8);
        assert_eq!(block_packed_byte_len(128, 1), 16);
        assert_eq!(block_packed_byte_len(128, 3), 48);
        assert_eq!(block_packed_byte_len(5, 3), 8);
        assert_eq!(block_packed_byte_len(0, 1), 0);
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
    fn pfor_beats_for_on_outlier_heavy_block() {
        // 127 small values (4-bit range) + 1 big outlier (32-bit range).
        // FOR payload: 9 header + padded(128 * 32 / 8) = 9 + 512 = 521 bytes.
        // PFOR payload: 11 header + padded(128 * 4 / 8) + 1 * 10
        //            = 11 + 64 + 10 = 85 bytes.
        let mut values: Vec<i64> = (0..127).map(|i| 100 + (i % 16) as i64).collect();
        values.push(i64::from(u32::MAX));
        let array: ArrayRef = Arc::new(Int64Array::from(values));
        let pfor_chunk = Pfor.encode(array.as_ref()).unwrap();
        let decoded = Pfor.decode(&pfor_chunk, &BqlType::Int).unwrap();
        assert_eq!(decoded.as_ref(), array.as_ref());
        assert!(
            pfor_chunk.payload.len() < 200,
            "PFOR payload should be well under 200 bytes for one-outlier block, got {}",
            pfor_chunk.payload.len()
        );
    }
}
