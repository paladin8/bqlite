//! LZ4 post-encoding compression wrapper.
//!
//! v1's only post-encoding codec, per
//! `docs/design/storage/segment-format-v1.md` §8 and
//! `docs/design/storage-format.md` §10.7. Operates on the **encoded
//! value bytes only** — null bitmaps, encoding parameters,
//! dictionaries, and the segment footer all stay uncompressed so the
//! reader can parse zone maps and headers without a decompression
//! step. LZ4 is therefore not an [`Encoding`](super::Encoding); it
//! layers above one.
//!
//! # Codec
//!
//! `lz4_flex`'s block API (not the frame format). The block's
//! decompressed length is carried on disk in the encoding header's
//! `uncompressed_payload_length: u32 LE` field; the compressed
//! length is derived arithmetically by the reader from the rest of
//! the column-chunk header (see segment-format-v1.md §7). Keeping
//! the on-disk bytes free of LZ4 frame metadata lets v1 readers
//! validate integrity through the segment footer's checksum without
//! a second independent length field.
//!
//! # Ownership of the selection decision
//!
//! This module just round-trips bytes. The **whether** decision
//! — "does applying LZ4 shrink this payload by at least 10%?" per
//! storage-format.md §10.7 — lives in the encoding selector
//! (TASK-212). Callers that want the selector's guard should run
//! the threshold check before calling [`compress_lz4`] and record
//! [`CompressionType::None`] in the on-disk header when the
//! threshold fails.
//!
//! # Configurable acceleration
//!
//! `lz4_flex`'s public safe API does not expose an acceleration
//! parameter — block-format compression runs with the crate's
//! default hash-chain strategy, which is tuned for small chunks and
//! matches what the task spec calls for. If a future task wires a
//! tunable compressor into the selector, it lands as an extended
//! variant here (e.g. `compress_lz4_accel(acc: u32)`) without
//! reshaping the on-disk discriminants. See
//! `docs/design/storage/segment-format-v1.md` §8 for the
//! forward-compat story.

use bqlite_core::error::{BqliteError, Result};

/// On-disk compression discriminant per `segment-format-v1.md` §8.
///
/// Two variants ship in v1; later waves extend this enum with new
/// codecs (ZSTD, etc.) alongside a format-version bump. Readers
/// reject any discriminant outside the v1 set with
/// [`BqliteError::Execution`], following the same contract the
/// [`super::EncodingType`] enum uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum CompressionType {
    /// Payload bytes are written verbatim. §8 discriminant `0`.
    None = 0,
    /// Payload bytes are wrapped in an LZ4 block. §8 discriminant `1`.
    Lz4 = 1,
}

impl CompressionType {
    /// The byte that identifies this compression on disk.
    #[inline]
    pub fn discriminant(self) -> u8 {
        self as u8
    }

    /// Parse a byte from a segment file back into a
    /// [`CompressionType`]. Unknown values — including any
    /// reserved-for-later-waves byte — produce
    /// [`BqliteError::Execution`] so the reader surfaces
    /// corruption/version-mismatch without panicking.
    pub fn from_discriminant(byte: u8) -> Result<Self> {
        match byte {
            0 => Ok(Self::None),
            1 => Ok(Self::Lz4),
            other => Err(BqliteError::Execution(format!(
                "unknown compression discriminant {other} — segment written by an \
                 incompatible version"
            ))),
        }
    }
}

/// Compress `input` into an LZ4 block.
///
/// Returns the compressed byte stream. The caller persists this
/// verbatim as the column chunk's payload and remembers
/// `input.len()` as the encoding header's
/// `uncompressed_payload_length` — the reader relies on that field
/// to size the destination buffer at decode time.
///
/// Empty inputs are legal: LZ4 handles them by emitting a small
/// marker block whose decompression produces a zero-length buffer.
/// Callers that want to skip the LZ4 layer entirely for short
/// payloads should do so by recording [`CompressionType::None`] in
/// the column-chunk header; this function itself never refuses an
/// input on size grounds.
#[inline]
pub fn compress_lz4(input: &[u8]) -> Vec<u8> {
    ::lz4_flex::block::compress(input)
}

/// Decompress an LZ4 block back into exactly `uncompressed_len`
/// bytes.
///
/// `input` must be the exact compressed byte stream produced by
/// [`compress_lz4`]; `uncompressed_len` must equal the original
/// `input.len()` passed to that call — readers get both values from
/// the column-chunk header in the segment footer, so the pairing is
/// always available.
///
/// `lz4_flex::block::decompress` treats the `uncompressed_size`
/// argument as a capacity hint (it sizes the output `Vec` but does
/// not refuse a shorter actual decompression). This wrapper layers
/// an exact-length check on top so a header byte flip that inflates
/// or deflates the recorded `uncompressed_payload_length` is always
/// caught as corruption rather than silently returning a wrong
/// number of bytes to the column decoder.
///
/// # Errors
///
/// - [`BqliteError::Execution`] if `lz4_flex` rejects the block
///   (corrupted bytes, truncated input, or inflation beyond the
///   caller's expectation).
/// - [`BqliteError::Execution`] if the decoded block length is not
///   exactly `uncompressed_len`. The error message carries both
///   the expected and actual lengths so an operator debugging a
///   corrupted segment can see the mismatch without re-running
///   the decode.
pub fn decompress_lz4(input: &[u8], uncompressed_len: usize) -> Result<Vec<u8>> {
    let decoded = ::lz4_flex::block::decompress(input, uncompressed_len).map_err(|e| {
        BqliteError::Execution(format!(
            "lz4 decompress failed (expected uncompressed_len = {uncompressed_len}, \
             input_len = {}): {e}",
            input.len()
        ))
    })?;
    if decoded.len() != uncompressed_len {
        return Err(BqliteError::Execution(format!(
            "lz4 decompress produced {} bytes but the column-chunk header recorded \
             uncompressed_payload_length = {uncompressed_len}",
            decoded.len()
        )));
    }
    Ok(decoded)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── CompressionType discriminants ──────────────────────────────────────

    #[test]
    fn compression_type_round_trip_discriminants() {
        for variant in [CompressionType::None, CompressionType::Lz4] {
            let byte = variant.discriminant();
            let parsed = CompressionType::from_discriminant(byte).unwrap();
            assert_eq!(parsed, variant);
        }
    }

    #[test]
    fn compression_type_matches_segment_format_v1_spec() {
        // §8 pins these discriminants so that later-wave codecs
        // (Zstd = 2, ...) can be added without renumbering.
        assert_eq!(CompressionType::None.discriminant(), 0);
        assert_eq!(CompressionType::Lz4.discriminant(), 1);
    }

    #[test]
    fn unknown_compression_discriminant_is_typed_execution_error() {
        for byte in [2_u8, 3, 10, 255] {
            let err = CompressionType::from_discriminant(byte).unwrap_err();
            match err {
                BqliteError::Execution(msg) => {
                    assert!(
                        msg.contains(&byte.to_string()),
                        "error should name offending byte; got: {msg}"
                    );
                }
                other => panic!("expected Execution, got {other:?}"),
            }
        }
    }

    // ── compress / decompress round trip ───────────────────────────────────

    #[test]
    fn round_trip_empty_input() {
        let input: &[u8] = &[];
        let compressed = compress_lz4(input);
        let back = decompress_lz4(&compressed, 0).expect("decompress empty");
        assert_eq!(back, input);
    }

    #[test]
    fn round_trip_single_byte() {
        let input = b"x";
        let compressed = compress_lz4(input);
        let back = decompress_lz4(&compressed, input.len()).expect("decompress");
        assert_eq!(back, input);
    }

    #[test]
    fn round_trip_highly_repetitive_payload_compresses_well() {
        // A pathological best-case input. We do not require a
        // specific compression ratio — that's a moving target across
        // lz4_flex versions — but a 64KB block of one byte must
        // shrink by at least 10x, which is the weakest bound any
        // reasonable LZ4 impl satisfies.
        let input = vec![0xAB_u8; 65_536];
        let compressed = compress_lz4(&input);
        assert!(
            compressed.len() * 10 < input.len(),
            "repetitive payload should compress by at least 10x, got {} vs {}",
            compressed.len(),
            input.len()
        );
        let back = decompress_lz4(&compressed, input.len()).expect("decompress");
        assert_eq!(back, input);
    }

    #[test]
    fn round_trip_incompressible_random_looking_payload() {
        // 4 KiB of hash-derived bytes — poor compressibility.
        // The round trip must still succeed even when LZ4 emits
        // mostly literals with no meaningful ratio. We also take
        // this opportunity to assert the round trip is byte-exact,
        // not just length-equal.
        let input: Vec<u8> = (0..4096_u32)
            .map(|i| {
                let mut x = i.wrapping_mul(2_654_435_761);
                x ^= x >> 16;
                (x & 0xFF) as u8
            })
            .collect();
        let compressed = compress_lz4(&input);
        let back = decompress_lz4(&compressed, input.len()).expect("decompress");
        assert_eq!(back, input);
    }

    #[test]
    fn round_trip_at_various_sizes_covers_block_boundary_math() {
        // Exercise sizes on either side of typical block-format
        // boundaries to guard against off-by-one bugs in whatever
        // bookkeeping lz4_flex does internally. The actual
        // boundaries are implementation-detail; the round trip
        // itself is the contract we care about.
        for &size in &[1, 15, 16, 17, 63, 64, 65, 255, 256, 257, 4095, 4096, 4097] {
            let input: Vec<u8> = (0..size).map(|i| (i * 31 + 7) as u8).collect();
            let compressed = compress_lz4(&input);
            let back = decompress_lz4(&compressed, input.len())
                .unwrap_or_else(|e| panic!("decompress at size {size}: {e}"));
            assert_eq!(back, input, "byte mismatch at size {size}");
        }
    }

    // ── decompress failure modes ───────────────────────────────────────────

    #[test]
    fn decompress_with_wrong_expected_length_errors_cleanly() {
        // Lying about `uncompressed_len` is the classic segment
        // corruption scenario — a header byte flip. lz4_flex
        // surfaces this as a decompression error; we must not
        // panic, and the wrapped error must mention the expected
        // length so an operator can correlate it with the header.
        let input = b"a fairly compressible payload aaaaaaaa".as_slice();
        let compressed = compress_lz4(input);

        let err =
            decompress_lz4(&compressed, input.len() + 1).expect_err("wrong length must error");
        match err {
            BqliteError::Execution(msg) => {
                assert!(
                    msg.contains("lz4 decompress"),
                    "error should name lz4; got: {msg}"
                );
                assert!(
                    msg.contains(&(input.len() + 1).to_string()),
                    "error should name the expected length; got: {msg}"
                );
            }
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    #[test]
    fn decompress_truncated_input_errors_cleanly() {
        let input = b"bqlite round-trip test payload 123456789".as_slice();
        let compressed = compress_lz4(input);
        // Chop off the last few bytes to simulate on-disk truncation.
        let truncated = &compressed[..compressed.len() / 2];
        let err = decompress_lz4(truncated, input.len()).expect_err("truncated input must error");
        assert!(
            matches!(err, BqliteError::Execution(_)),
            "expected Execution, got {err:?}"
        );
    }

    #[test]
    fn decompress_corrupted_bytes_errors_cleanly() {
        let input = b"some ordinary text that LZ4 can handle easily".as_slice();
        let mut compressed = compress_lz4(input);
        // Flip bytes inside the compressed block. lz4_flex may or
        // may not detect every corruption (it's a lossless codec,
        // not a checksum), but when it does it must return a clean
        // error rather than panicking. If the decode happens to
        // succeed with garbage, the round-trip will simply fail
        // byte-for-byte — catch that case too.
        for b in compressed.iter_mut() {
            *b = b.wrapping_add(0x5A);
        }
        match decompress_lz4(&compressed, input.len()) {
            Err(BqliteError::Execution(_)) => { /* expected */ }
            Ok(bytes) => {
                assert_ne!(
                    bytes, input,
                    "corrupted bytes decoded into the original — lz4_flex would have had to \
                     be magical"
                );
            }
            Err(other) => panic!("expected Execution, got {other:?}"),
        }
    }
}
