//! Frequency encoding — RETIRED (TASK-418).
//!
//! ## Status
//!
//! This encoding was evaluated as part of the Wave 4 advanced-encoding
//! research (TASK-401) and **rejected**. The module is present to record
//! the retirement decision and its rationale; it contains no runnable
//! code. On-disk discriminant 11 (`FreqEncoding`) is **reserved but not
//! assigned** — v1 and v2 readers both reject it as unknown.
//!
//! ## What frequency encoding is
//!
//! Frequency encoding is an optimization pass applied *on top of*
//! Dictionary encoding. After the dictionary is built (sorted distinct
//! values), the ordinal codes are remapped so that the most frequent
//! values receive the smallest codes. The idea is that a bit-packed code
//! stream uses fewer bits when common codes have smaller values —
//! reducing the *average* bit width.
//!
//! ## Why it does not help bqlite
//!
//! In bqlite's fixed-width bit-packing model (used in v1 and v2), every
//! code in a column chunk is packed with the same bit width:
//!
//! ```text
//! code_bit_width = ceil(log2(cardinality))
//! ```
//!
//! The bit width is determined by the *cardinality* of the dictionary,
//! not by the frequency distribution. Remapping codes so that common
//! values get smaller ordinals does not change the cardinality and
//! therefore does not change the code bit width. The codes are merely
//! permuted; the packed payload is exactly the same size.
//!
//! **Example** (100 distinct string values, 8 192 rows):
//!
//! | Encoding | bits/code | payload bytes |
//! |---|---|---|
//! | Dictionary + BitPacking | 7 | 57 344 |
//! | Dictionary + FreqEncoding + BitPacking | 7 | 57 344 |
//!
//! **No improvement.** See `docs/design/storage/advanced-encodings.md`
//! §9.2 for the full analysis.
//!
//! ## When frequency encoding *would* help
//!
//! The compression benefit materialises only when a **variable-width**
//! coding scheme is used instead of fixed-width bit-packing:
//!
//! - Huffman coding of dictionary codes (complex; rarely used in OLAP
//!   engines).
//! - Run-length encoding applied *after* frequency reordering (sorted
//!   data clusters common codes, which RLE can then compress well — but
//!   bqlite already has RLE as a standalone v2 encoding).
//! - Byte-aligned variable-width codes (1 byte for top-256, 2 bytes for
//!   the rest — incompatible with SIMD bit-unpacking).
//!
//! None of these alternatives are worth the added complexity for the
//! gains they would provide; the existing Dictionary + BitPacking + RLE
//! cascade already handles skewed categorical columns near-optimally.
//!
//! ## On-disk discriminant
//!
//! Discriminant 11 is **reserved** in the `EncodingType` enum and in the
//! v2 segment format spec, but is never written by any encoder. Both
//! `EncodingType::from_discriminant` and
//! `EncodingType::from_discriminant_versioned` reject byte 11 with a
//! corruption error, which is the correct behaviour: no bqlite version
//! has ever emitted discriminant 11, so any segment file containing it
//! is corrupt or was produced by an unknown tool.
//!
//! ## Future direction
//!
//! If future profiling reveals that a variable-width coding scheme would
//! provide material compression or I/O benefit on specific workloads,
//! frequency encoding should be revisited as part of that effort — not
//! as a standalone encoding, and not without first benchmarking it
//! against the Dictionary + RLE cascade.
//!
//! ## References
//!
//! - `docs/design/storage/advanced-encodings.md` §9 — full research
//!   analysis and no-go decision.
//! - `docs/design/storage/advanced-encodings.md` §11.6 — TASK-418
//!   retirement note.
//! - `docs/design/storage/segment-format-v2.md` §4 — discriminant
//!   reservation table.
