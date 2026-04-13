# Segment Format v2

**Wave**: 4
**Task**: TASK-402
**Status**: draft
**Depends on**: TASK-401 (advanced encoding research), TASK-201 (segment format v1)

## 1. Scope

This note freezes the **byte-level v2 layout** that extends the
Wave 2 v1 segment format with the six encodings approved by
TASK-401: **RLE**, **DoubleDelta**, **FOR**, **PFOR**, **FSST**,
and **ALP**. Concretely:

- The expanded `EncodingType` enum — six new discriminants, one
  reserved (Frequency, no-go).
- Per-encoding on-disk parameter blocks for the six new encodings,
  following the same `(encoding_params, payload)` split established
  in `segment-format-v1.md` §7.
- A new **FSST symbol tables region** between the segment
  dictionaries region and the footer body — the only encoding
  that requires segment-level auxiliary state.
- The **FooterV2** struct — a strict superset of FooterV1 with one
  new field (`fsst_symbol_tables: Vec<FsstSymbolTableRef>`).
- **Reader compatibility rules** for mixed v1/v2 databases: version
  dispatch at segment open, per-segment version tracking in the
  manifest, and no rewrite obligation.
- **Compaction rewrite policy** — when and whether compaction
  upgrades v1 segments to v2.
- The **encoding selection policy** the v2 writer uses to choose
  among the expanded codec set, incorporating the selector guards
  from TASK-401.

It does **not** design:

- The `Encoding` trait implementations for the six new encodings —
  those are TASK-413 (RLE), TASK-414 (DoubleDelta), TASK-415 (FOR),
  TASK-416 (FSST), TASK-417 (ALP), and TASK-450 (PFOR).
- The encoding selector integration — TASK-419 owns the runtime
  selector code. This note specifies the heuristic; TASK-419
  implements it.
- Per-row-group checksums — deferred from v1 (§17 item 1) and
  still deferred in v2. The segment-level xxHash64 checksum is sufficient
  for Wave 4; per-row-group granularity is a Wave 5+ addition if
  partial-segment recovery becomes a goal.
- Variable-width coding schemes (Huffman, ANS) — these are research
  items for Wave 5+, per TASK-401 §9.5.

### 1.1 Relationship to existing design docs

This document extends `segment-format-v1.md` (TASK-201). Every
aspect of the v1 layout that is not explicitly changed below
remains in effect for v2 segments. The v1 document is **not
modified** — it remains the authoritative reference for
`format_version == 1` segments. v2 is a strict superset: every v1
byte-level rule applies to v2, with the additions described here.

Cross-references:

| Document | Relationship |
|---|---|
| `segment-format-v1.md` (TASK-201) | v2 extends; all v1 rules apply unless overridden |
| `advanced-encodings.md` (TASK-401) | Source of go/no-go decisions and per-encoding analysis |
| `storage-format.md` (TASK-001) | Parent design; v2 implements §3.4 FSST symbol tables and §10.2 extended encoding set |
| `reader-trait.md` (TASK-109) | `SegmentReader` trait surface unchanged; v2 reader is a new impl behind the same trait |
| `compaction-concurrency.md` (TASK-403) | Compaction rewrite policy in §9 coordinates with the concurrency protocol |

### 1.2 Design principles

1. **Minimal delta from v1.** v2 changes exactly two things: the
   encoding set and the FSST symbol table region. The file header,
   row-group layout, column-chunk framing, null-bitmap placement,
   compression wrapper, segment-dictionary region, checksum, and
   trailer are **identical** to v1. This minimizes the reader
   surface that must be duplicated.

2. **Forward-compatible rejection.** v1 readers already reject
   unknown encoding discriminants and unknown format versions with
   `BqliteError::Corruption`. A v1 reader opening a v2 segment
   file fails cleanly at version check — no silent data corruption.

3. **No migration obligation.** Upgrading bqlite to a Wave 4
   release does not require rewriting existing v1 segments. v1
   segments remain readable indefinitely. Compaction *may* produce
   v2 output from v1 input (§9), but this is opportunistic, not
   mandatory.

---

## 2. Crate placement

All v2 concerns are internal to `bqlite-storage`, matching the
v1 crate boundary:

| Item | Crate | Notes |
|---|---|---|
| `SEGMENT_FORMAT_VERSION_V2` constant | `bqlite-storage::segment::layout` | New constant alongside the existing `SEGMENT_FORMAT_VERSION` (renamed to `SEGMENT_FORMAT_VERSION_V1` for clarity, with the old name kept as an alias). |
| `FooterV2` struct | `bqlite-storage::segment::layout` | New struct alongside `FooterV1`. |
| `FsstSymbolTableRef` struct | `bqlite-storage::segment::layout` | New struct for the FSST symbol table index. |
| Extended `EncodingType` enum | `bqlite-storage::encoding` | Six new variants added to the existing enum. |
| v2 `SegmentWriter` | `bqlite-storage::segment::writer` | Extended to emit v2 framing when any v2 encoding is selected. |
| v2 `SegmentFileReader` | `bqlite-storage::segment::reader` | Extended with version dispatch: v1 path unchanged, v2 path parses the new footer and FSST region. |
| New `Encoding` impls | `bqlite-storage::encoding::{rle,double_delta,for_encoding,pfor,fsst,alp}` | One module per encoding (TASK-413–TASK-417, TASK-450). |

The Wave 1 trait surface in `bqlite-core::storage` does not change.

---

## 3. File layout

v2 adds a single new region — the **FSST symbol tables region** —
between the segment dictionaries region and the footer body.
Everything else is byte-identical to v1.

```
┌──────────────────────────────────────────┐  offset 0
│  File header (6 bytes, fixed)            │
│    magic[4]    = "BQLT"                  │
│    version[2]  = u16 LE = 2              │
├──────────────────────────────────────────┤  offset 6
│  Row group 0  (variable)                 │
│  ┌────────────────────────────────────┐  │
│  │ Column chunk 0 (col_ordinal 0)     │  │
│  │ Column chunk 1                     │  │
│  │ ...                                │  │
│  │ Column chunk N−1                   │  │
│  └────────────────────────────────────┘  │
│  Row group 1 ... Row group K−1           │
├──────────────────────────────────────────┤
│  Segment dictionaries region (variable,  │
│  possibly empty) — identical to v1 §11   │
├──────────────────────────────────────────┤
│  FSST symbol tables region (variable,    │
│  possibly empty) — NEW in v2             │
│    Symbol table 0 (contiguous bytes)     │
│    Symbol table 1                        │
│    ...                                   │
├──────────────────────────────────────────┤
│  Footer body (variable, postcard-encoded)│
│    FooterV2 (§7)                         │
├──────────────────────────────────────────┤
│  Checksum (8 bytes)                      │
│    xxHash64 over [0, end_of_footer_body) │
│    little-endian u64                     │
├──────────────────────────────────────────┤
│  Trailer (8 bytes, fixed)                │
│    footer_body_length: u32 LE            │
│    magic[4] = "BQLT"                     │
└──────────────────────────────────────────┘  offset file_size
```

Constants (v2):

| Constant | Value | Change from v1 |
|---|---|---|
| `MAGIC` | ASCII `BQLT` → `[0x42, 0x51, 0x4C, 0x54]` | Unchanged |
| `SEGMENT_FORMAT_VERSION_V2` | `2` (u16) | **New** (v1 = `1`) |
| `ROW_GROUP_SIZE_DEFAULT` | `65_536` rows | Unchanged |
| `CHECKSUM_ALGORITHM` | xxHash64 (seed `0`) | Unchanged |
| Byte order | Little-endian | Unchanged |
| `FILE_HEADER_LEN` | `6` bytes | Unchanged |
| `CHECKSUM_LEN` | `8` bytes | Unchanged |
| `TRAILER_LEN` | `8` bytes | Unchanged |
| `FOOTER_SUFFIX_LEN` | `16` bytes | Unchanged |

**Single-pass open.** The three-I/O open protocol from v1 §4
applies unchanged. After reading the trailer and footer body, the
reader dispatches on `format_version` to parse the footer as
`FooterV1` or `FooterV2`. The FSST symbol table region is loaded
lazily on first access to an FSST-encoded column, or eagerly at
open time if any column in the schema uses FSST encoding — the
reader uses the footer's `fsst_symbol_tables` entries to locate the
bytes.

---

## 4. New encoding discriminants

v2 extends the `EncodingType` enum with six new variants. The v1
discriminants are unchanged. Discriminant 11 (`FreqEncoding`) is
**reserved but not assigned** per TASK-401's no-go decision.

```rust
#[repr(u8)]
pub enum EncodingType {
    // v1 (unchanged)
    Plain       = 0,
    Dictionary  = 1,
    Delta       = 2,
    BitPacking  = 4,
    Constant    = 6,
    // v2 (new in Wave 4)
    DoubleDelta = 3,
    Rle         = 5,
    Fsst        = 7,
    For         = 8,
    PFor        = 9,
    Alp         = 10,
    // Reserved: FreqEncoding = 11 (no-go per TASK-401 §9, discriminant
    //           reserved for future variable-width coding scheme)
}
```

**`from_discriminant` dispatch.** The `EncodingType::from_discriminant`
method is extended to accept `3`, `5`, `7`, `8`, `9`, `10` when
the segment's `format_version >= 2`. When `format_version == 1`,
these discriminants remain rejected as corruption — a v1 segment
must never contain v2 encodings. The reader passes the
`format_version` into the discriminant parser so the version gate
is enforced at parse time.

**Applicable-type matrix (v2 additions):**

| Encoding | Bool | Int | Float | String | Timestamp | List/Map |
|---|---|---|---|---|---|---|
| DoubleDelta | - | yes | - | - | yes | - |
| Rle | yes | yes | - | yes | yes | - |
| Fsst | - | - | - | yes | - | - |
| For | - | yes | - | - | yes | - |
| PFor | - | yes | - | - | yes | - |
| Alp | - | - | yes | - | - | - |

Combined with the v1 matrix (Plain: all primitives; Dictionary:
String, Int; Delta: Int, Timestamp; BitPacking: Int, Timestamp;
Constant: all primitives), the full v2 matrix covers every
primitive type with at least one specialized encoding.

---

## 5. Per-encoding on-disk parameter blocks

Each new encoding follows the same column-chunk framing as v1
(`segment-format-v1.md` §7): the null bitmap (if nullable),
followed by the encoding header (discriminant `u8` +
`encoding_params` + `uncompressed_payload_length: u32 LE`),
followed by the (optionally LZ4-wrapped) payload.

### 5.1 DoubleDelta (`encoding = 3`)

Second-order delta encoding for near-constant-interval integer and
timestamp columns. Extends the v1 Delta codec with an additional
prefix-sum pass.

**encoding_params.**

```
base_value:      i64 LE    // values[0]
first_delta:     i64 LE    // values[1] - values[0]
dd_bit_width:    u8        // 1..=64, bit width of zigzag-encoded
                           // second-order deltas
```

Total encoding_params size: **17 bytes** (fixed).

**Payload.** `max(0, non_null_count - 2)` zigzag-encoded
second-order deltas, bit-packed at `dd_bit_width` bits per value,
where `non_null_count = row_count - null_count` (matching the v1
convention that null rows are stripped by the null bitmap before
encoding — see `segment-format-v1.md` §9.1). Byte count
`ceil(dd_bit_width * max(0, non_null_count - 2) / 8)` rounded up
to the next multiple of 8 for SIMD-friendly unpacking. Trailing
padding is counted in `uncompressed_payload_length`.

`dd[i] = delta[i] - delta[i-1]` where `delta[i] = values[i] -
values[i-1]`. Zigzag encoding: `zigzag(x) = (x << 1) ^ (x >> 63)`.

**Edge cases.**

- `non_null_count == 0`: illegal. A column chunk with zero non-null
  values picks `Constant` with `value = NULL` (same as v1 §9.5) or
  `Plain` with an empty payload. The selector never picks
  DoubleDelta here.
- `non_null_count == 1`: `base_value` carries the lone value.
  `first_delta` is `0`. Payload is 0 bytes.
- `non_null_count == 2`: `base_value` and `first_delta` carry both
  values. Payload is 0 bytes (zero double-deltas).
- Overflow: the writer computes deltas and double-deltas in `i128`.
  If any value overflows `i64` after zigzag encoding, the writer
  falls back to Delta or another encoding. The on-disk format never
  carries unrepresentable values.

**Decode.** Bit-unpack → zigzag-decode → prefix-sum (reconstruct
first-order deltas from `first_delta`) → prefix-sum (reconstruct
values from `base_value`).

### 5.2 RLE (`encoding = 5`)

Run-length encoding for highly repetitive columns. Stores
`(value, run_length)` pairs.

**encoding_params.**

```
run_count:  u32 LE    // number of runs (>= 1)
```

Total encoding_params size: **4 bytes** (fixed).

**Payload.** Two contiguous arrays:

```
run_ends:  [u32 LE; run_count]    // cumulative end positions
values:    type-dependent payload  // one value per run
```

Run ends are **cumulative** (1-based): `run_ends[i]` is the number
of rows up to and including run `i`. `run_ends[run_count - 1]`
equals `non_null_count` (the chunk's non-null row count, per the
v1 convention that null rows are stripped before encoding). This
matches Arrow's `RunEndEncodedArray` convention for zero-copy
decode.

Values are encoded per the column's `BqlType`:

| Type | Per-value encoding | Total value-bytes |
|---|---|---|
| `Bool` | 1 byte per run (`0x00` or `0x01`) | `run_count` bytes |
| `Int` | 8 bytes, i64 LE | `8 * run_count` bytes |
| `Timestamp` | 8 bytes, i64 LE (nanoseconds) | `8 * run_count` bytes |
| `String` | `u32 LE length` + UTF-8 bytes | variable |

**Edge cases.**

- Single run: legal. `run_count == 1`,
  `run_ends[0] == non_null_count`. Values section contains one
  value.
- Empty after null stripping: `row_count == 0` is illegal (same as
  v1).
- String runs: each string in the values section is
  length-prefixed. The reader parses them sequentially using the
  `run_count` from the params block.

**Decode.** For each run, broadcast the run's value into
`run_ends[i] - run_ends[i-1]` output slots (with `run_ends[-1]`
defined as `0`). For fixed-width types this is a memset-style fill.
The Arrow `RunEndEncodedArray` zero-copy path is an optimization:
the on-disk `run_ends` and `values` arrays can be wrapped directly
into an Arrow run-end-encoded array without any per-row work, if
the downstream operator supports that representation.

### 5.3 FSST (`encoding = 7`)

Fast Static Symbol Table compression for high-cardinality string
columns. Requires a segment-level symbol table (§6).

**encoding_params.**

```
symbol_table_id:  u32 LE    // index into footer.fsst_symbol_tables
```

Total encoding_params size: **4 bytes** (fixed).

**Payload.** Variable-length compressed strings, one per non-null
row:

```
For i in 0..(row_count - null_count):
    compressed_len:   u16 LE
    compressed_bytes: [u8; compressed_len]
```

Each `compressed_bytes` sequence is the FSST-encoded form of the
original string. Each byte is either:
- A symbol table index (0–255): replaced by the corresponding
  symbol's 1–8 bytes during decode.
- An escape byte (`0xFF`): the next byte is a literal (passed
  through unchanged).

The escape mechanism means FSST-compressed output is never longer
than `2 * original_len` in the worst case (every byte escaped).
The `u16` compressed length field limits individual compressed
strings to 65,535 bytes. With 2x worst-case expansion, strings
longer than ~32 KB uncompressed could overflow; the writer must
fall back to Plain for the entire column if any string exceeds this
limit after FSST encoding. In practice, behavioral analytics
strings (event types, URLs, user agents) are well below this
threshold. The selector guard (§10) ensures FSST is only chosen
when it actually compresses.

**Reader invariant.** `symbol_table_id <
footer.fsst_symbol_tables.len()` and
`footer.fsst_symbol_tables[symbol_table_id].column_ordinal ==
this_chunk.column_ordinal`.

**Decode.** For each compressed string: walk the compressed bytes,
replacing symbol indices with their symbol table entries and
passing escape-preceded literal bytes through. The output is the
original UTF-8 string. The decode loop is a tight byte-by-byte
scan with no inter-string dependencies.

### 5.4 FOR (`encoding = 8`)

Frame-of-Reference encoding with per-block minimum values. A
refinement of v1 BitPacking that uses block-local rather than
global framing.

**encoding_params.**

```
block_size:   u16 LE    // 128 (SIMD-aligned); must be 128 in v2
block_count:  u32 LE    // ceil(non_null_count / block_size)
```

Total encoding_params size: **6 bytes** (fixed).

v2 fixes `block_size` at 128 to match AVX2 register files (16
lanes of 8 bytes). A future format version may allow 256 if
benchmarks justify it. `non_null_count = row_count - null_count`
per the v1 convention that null rows are stripped before encoding.

**Payload.** Contiguous blocks:

```
For each block (block_count blocks):
    block_min:  i64 LE     // minimum value in this block
    bit_width:  u8         // bits per offset (1..=64)
    packed:     ceil(block_len * bit_width / 8) bytes,
                padded to 8-byte boundary
```

Where `block_len` is `block_size` for all blocks except the last,
which may be short: `block_len = non_null_count - (block_count - 1)
* block_size`. Offsets are unsigned: `offset[i] = value[i] -
block_min`.

**Edge cases.**

- Single block: `block_count == 1`,
  `block_len == non_null_count`.
- All values identical within a block: `bit_width` is clamped to a
  minimum of `1` (the offsets are all zero, but zero-width packing
  is not supported — matching the v1 BitPacking convention). The
  Constant encoding should be preferred at the chunk level if the
  entire chunk is uniform, but per-block uniformity is handled by
  FOR naturally.
- Short final block: the packed section contains fewer offsets but
  is still padded to 8 bytes for SIMD tail safety.

**Decode.** Per block: read `block_min` and `bit_width`, unpack
`block_len` offsets, add `block_min` to each.

### 5.5 PFOR (`encoding = 9`)

Patched Frame-of-Reference. Extends FOR with an exception list for
outlier values that would otherwise force a wider bit width.

**encoding_params.**

```
block_size:   u16 LE    // 128 (same constraint as FOR)
block_count:  u32 LE    // ceil(non_null_count / block_size)
```

Total encoding_params size: **6 bytes** (fixed).

`non_null_count = row_count - null_count` per the v1 convention.

**Payload.** Contiguous blocks:

```
For each block (block_count blocks):
    block_min:     i64 LE     // minimum non-outlier value
    main_width:    u8         // narrow bit width for non-outliers
                              // (clamped to minimum 1, same as FOR)
    patch_count:   u16 LE     // number of outliers in this block
    packed_main:   ceil(block_len * main_width / 8) bytes,
                   padded to 8-byte boundary
    patch_indices: [u16 LE; patch_count]  // positions of outliers
                                          // within the block
    patch_values:  [i64 LE; patch_count]  // full-width outlier values
```

Outlier positions in `packed_main` are filled with zero offsets
(the decoder overwrites them with the patch values).

**Edge cases.**

- Zero patches: `patch_count == 0`. The block degrades to a FOR
  block with 2 bytes of overhead (the `patch_count` field).
- All patches: `patch_count == block_len`. The main stream is all
  zeros at `main_width == 1`; all values are in the patch list.
  This is worse than Plain — the selector must cap the patch
  fraction (§10).
- Short final block: same handling as FOR §5.4.

**Decode.** Per block: unpack the main stream (same as FOR), then
scatter `patch_values` at `patch_indices` positions into the output
buffer.

### 5.6 ALP (`encoding = 10`)

Adaptive Lossless floating-Point compression. Decomposes float
values into integer mantissas via a decimal exponent, then encodes
the mantissas with FOR.

**encoding_params.**

```
exponent:      u8        // decimal exponent (0..=18)
factor:        f64 LE    // 10^exponent, precomputed for decode
patch_count:   u32 LE    // number of exception values
for_block_size:  u16 LE  // FOR block size for mantissa stream (128)
for_block_count: u32 LE  // FOR block count for mantissa stream
```

Total encoding_params size: **19 bytes** (fixed).

**Payload.** Three contiguous sections:

```
1. Mantissa stream — FOR-encoded i64 array:
   For each FOR block (for_block_count blocks):
       block_min:  i64 LE
       bit_width:  u8
       packed offsets (padded to 8-byte boundary)

2. Patch indices — [u32 LE; patch_count]
   Positions of exception values in the original row order.

3. Patch values — [f64 LE; patch_count]
   IEEE 754 exact values for exceptions.
```

The mantissa stream contains `non_null_count - patch_count` values
(where `non_null_count = row_count - null_count` per the v1
convention). Mantissa `m[i]` is computed as
`round(value[i] * factor)` where the round-trip check
`(m[i] as f64) / factor == value[i]` passes (strict IEEE 754
equality). Values that fail the round-trip check are exceptions.

**Edge cases.**

- All values decompose cleanly: `patch_count == 0`. Sections 2 and
  3 are empty.
- No values decompose: `patch_count == non_null_count`. The
  mantissa FOR stream is empty (`for_block_count == 0`). All
  values are in the patch list. This is Plain with overhead — the
  selector prevents this (§10).
- NaN, ±Inf, subnormals: always treated as exceptions
  (stored in the patch list at full f64 precision).
- ±0.0: `+0.0` and `-0.0` are both valid ALP decomposition
  targets (`0 * factor == 0.0`). They compare as equal under
  IEEE 754 `==`, so the round-trip check succeeds for both.

**Decode.** FOR-unpack the mantissa stream → multiply each mantissa
by `1.0 / factor` → scatter exceptions from the patch list at the
indicated positions.

---

## 6. FSST symbol tables region

FSST is the only v2 encoding that requires segment-level auxiliary
state. The symbol table is built from a sample of the full column's
strings (not per row group), so it lives at the segment level —
parallel to how Dictionary values live in the segment dictionaries
region.

### 6.1 Region placement

The FSST symbol tables region occupies the bytes between the end of
the segment dictionaries region and the start of the footer body:

```
[last row group end] → [segment dictionaries region]
                     → [FSST symbol tables region]   ← NEW
                     → [footer body]
                     → [checksum]
                     → [trailer]
```

A v2 segment with no FSST-encoded columns has a zero-length FSST
symbol tables region. The footer's
`fsst_symbol_tables.len() == 0` and the region occupies zero bytes.

### 6.2 Symbol table on-disk format

Each symbol table is a contiguous byte sequence:

```
symbol_count: u16 LE                    // number of symbols (1..=256)
For each symbol (symbol_count symbols):
    sym_len:  u8                        // 1..=8
    sym_bytes: [u8; sym_len]            // the symbol's bytes
```

The maximum size of a single symbol table is:
`2 + 256 * (1 + 8) = 2306` bytes.

Symbol tables are stored contiguously in the region with no padding
between them. The footer's `FsstSymbolTableRef` entries provide the
byte offset and length for each table.

### 6.3 Symbol table construction

The writer builds one symbol table per FSST-encoded column per
segment. The construction algorithm (per Boncz et al., VLDB 2020):

1. Sample up to 16,384 strings from the column (or all strings if
   fewer).
2. Iteratively select the 256 substrings (1–8 bytes) that provide
   the greatest compression gain on the sample.
3. Encode the sample with the selected symbol table and verify that
   the compressed output is smaller than the Plain output. If not,
   fall back to Plain+LZ4.

The construction is performed once per segment during the write
phase. All row groups in the segment share the same symbol table
for a given column.

### 6.4 Footer reference struct

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FsstSymbolTableRef {
    /// The column this symbol table is for. A column has at most
    /// one FSST symbol table per segment.
    pub column_ordinal: u32,

    /// Absolute byte offset of the symbol table in the segment
    /// file. Points into the FSST symbol tables region.
    pub byte_offset: u64,
    pub byte_length: u64,

    /// Number of symbols in the table (1..=256).
    pub symbol_count: u16,
}
```

---

## 7. Footer body (v2)

The v2 footer body is a strict superset of v1. It adds one field:
`fsst_symbol_tables`. The field ordering and serialization format
(postcard) are unchanged.

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FooterV2 {
    /// Must equal `SEGMENT_FORMAT_VERSION_V2` (2).
    pub format_version: u16,

    /// Full segment schema at write time — identical semantics
    /// to FooterV1.schema.
    pub schema: TableSchema,

    /// Schema version — identical semantics to
    /// FooterV1.schema_version.
    pub schema_version: u32,

    /// Total rows across all row groups.
    pub row_count: u64,

    /// Number of row groups.
    pub row_group_count: u32,

    /// Row-group size hint. v2 writers continue to use
    /// ROW_GROUP_SIZE_DEFAULT (65,536).
    pub row_group_size_hint: u32,

    /// Creation timestamp in nanoseconds since epoch UTC.
    pub creation_timestamp_ns: i64,

    /// Sequence-ID range (min_inclusive, max_inclusive).
    pub seq_id_range: (u64, u64),

    /// Batch ID.
    pub batch_id: u64,

    /// Compaction tier.
    pub compaction_level: u8,

    /// Segment-level dictionaries — identical to FooterV1.
    pub dictionaries: Vec<SegmentDictRef>,

    /// Segment-level FSST symbol tables — NEW in v2.
    /// One entry per FSST-encoded column. Empty when no column
    /// uses FSST encoding.
    pub fsst_symbol_tables: Vec<FsstSymbolTableRef>,

    /// Per-row-group index — identical structure to FooterV1.
    /// ColumnChunkMeta.encoding may now contain v2 discriminants.
    pub row_groups: Vec<RowGroupIndex>,
}
```

**Why a new struct rather than extending FooterV1.** FooterV1 is
frozen — its postcard serialization is the v1 on-disk contract.
Adding a field to FooterV1 would change the serialized bytes,
breaking v1 segment reads. FooterV2 is a separate struct that
serializes independently. The reader dispatches on the
`format_version` from the file header to choose which struct to
deserialize.

**RowGroupIndex and ColumnChunkMeta.** These structs are shared
between FooterV1 and FooterV2. They do not change in v2 — the
only difference is that `ColumnChunkMeta.encoding` may now contain
v2 discriminants (`3`, `5`, `7`, `8`, `9`, `10`). The reader's
`EncodingType::from_discriminant` method gates these on
`format_version >= 2`.

**Postcard version pinning.** v2 continues to pin
`postcard = "=1.0"`. FooterV2 is a distinct type, so there is no
risk of postcard wire-format confusion between v1 and v2 footers.

---

## 8. Reader compatibility

### 8.1 Version dispatch at segment open

The reader's open path is:

1. Read trailer → validate magic → extract `footer_body_length`.
2. Read footer body bytes + checksum.
3. Read `format_version` from the file header (offset 4, u16 LE).
4. **Dispatch:**
   - `format_version == 1` → deserialize footer body as `FooterV1`.
     Validate `footer.format_version == 1`. Use v1 encoding set
     for `EncodingType::from_discriminant`.
   - `format_version == 2` → deserialize footer body as `FooterV2`.
     Validate `footer.format_version == 2`. Load FSST symbol
     tables if any. Use full v2 encoding set for
     `EncodingType::from_discriminant`.
   - Any other value → `BqliteError::Corruption`.

The rest of the read path (row-group iteration, column-chunk
decoding, zone-map lookup, predicate pushdown) is version-agnostic
— it operates on `RowGroupIndex` and `ColumnChunkMeta`, which are
shared types. The only version-sensitive code path is the encoding
decode dispatch, which already dispatches on
`ColumnChunkMeta.encoding`.

### 8.2 Mixed v1/v2 databases

A bqlite database may contain both v1 and v2 segments
simultaneously. This is the normal state during and after a Wave 4
upgrade:

- **Existing segments** written before the upgrade remain v1.
- **New segments** produced by ingest after the upgrade are v2
  (the v2 writer uses the expanded encoding set).
- **Compacted segments** may be v1 or v2 depending on the
  compaction rewrite policy (§9).

The manifest does not track per-segment format versions — it
doesn't need to. The reader discovers the version by reading each
segment's file header at open time. The version dispatch at open
(§8.1) handles both transparently.

### 8.3 Downgrade safety

A Wave 2/3 bqlite binary (v1-only reader) opening a database that
contains v2 segments will fail with `BqliteError::Corruption` on
the first v2 segment it tries to open. This is the intended
behavior — the v1 reader's version check at file-header parse time
rejects `format_version == 2` cleanly.

**Recommendation.** Applications that need to support downgrade
should avoid producing v2 segments until they are committed to the
Wave 4 binary. This is an operational concern, not a format concern
— the format itself is forward-compatible (unknown versions are
rejected, not silently misinterpreted).

### 8.4 Internal representation

After open, both v1 and v2 segments present the same interface to
the scan layer: `SegmentReader::open_segment` returns a
`SegmentScan` implementation. The scan layer does not know or care
which format version produced the scan — it operates on Arrow
arrays produced by the encoding decoders.

The reader internally holds either a `FooterV1` or `FooterV2`
behind an enum:

```rust
enum SegmentFooter {
    V1(FooterV1),
    V2(FooterV2),
}
```

Accessor methods on `SegmentFooter` delegate to the appropriate
variant. Common fields (`schema`, `row_groups`, `dictionaries`,
etc.) are accessed uniformly. The `fsst_symbol_tables` accessor
returns an empty slice for `V1` and the actual vec for `V2`.

---

## 9. Compaction rewrite policy

### 9.1 Compaction output format version

When the compaction scheduler (TASK-403) merges segments, the
output segment is **always v2**. This means:

- Merging two v1 segments produces a v2 segment. The new segment's
  columns are re-encoded using the v2 selector, which may choose
  v2 encodings (RLE, DoubleDelta, etc.) if they provide better
  compression.
- Merging two v2 segments produces a v2 segment (re-encoded with
  the current selector).
- Merging a mix of v1 and v2 segments produces a v2 segment.

This is the simplest policy: compaction always uses the current
writer, and the current writer produces v2. There is no
"preserve original encoding" mode — re-encoding is part of
compaction's value proposition (better encoding choices with more
data context).

### 9.2 No proactive v1→v2 rewrite

Compaction does **not** proactively rewrite v1 segments that are
not otherwise scheduled for compaction. A v1 segment that is not
in the compaction window remains v1 indefinitely. This avoids:

- Write amplification from gratuitous rewrites.
- I/O contention from a background "upgrade" pass.
- Complexity in the compaction scheduler.

v1 segments are perfectly readable by the v2 reader. The only
cost of keeping them is suboptimal encoding (the v1 encoding set
may produce larger segments than v2 would). This cost is amortized
as natural compaction cycles eventually replace v1 segments with
v2 output.

### 9.3 Compaction-level interaction

The `compaction_level` field in the footer is orthogonal to the
format version. A v1 segment at compaction level 0 (L0 ingest
output) may be merged with another v1 L0 segment to produce a v2
segment at compaction level 1. The format version and compaction
level are independently tracked.

---

## 10. Encoding selection policy (v2)

The v2 selector extends the v1 score-all-applicable heuristic with
the six new encodings and their selector guards from TASK-401.

### 10.1 Selection flow

```
Phase 1 — Trivial cases (unchanged from v1):
  1. If row_count == 0 → Plain (degenerate, no encoding needed)
  2. If all non-null values identical → Constant

Phase 2 — Type-specialized candidates:

  String columns:
    3. If cardinality / row_count < 0.3 → candidate: Dictionary
    4. If cardinality / row_count >= 0.3 → candidate: FSST
       Guard: FSST payload < Plain payload, else skip
    5. If average run length > 2 (sorted data) → candidate: RLE

  Integer / Timestamp columns:
    6. If sorted, monotonic, and dd_bit_width < 0.5 * delta_bit_width
       → candidate: DoubleDelta
    7. If sorted, monotonic → candidate: Delta
    8. If sum(per-block widths) < 0.9 * block_count * global_width
       → candidate: FOR
       - If > 1% and < 10% outliers → candidate: PFOR (instead of FOR)
    9. If cardinality / row_count < 0.3 → candidate: Dictionary
   10. Otherwise → candidate: BitPacking (global frame-of-reference)
   11. If average run length > 2 (sorted data) → candidate: RLE

  Float columns:
   12. If >= 70% of sample values ALP-decompose cleanly
       → candidate: ALP
   13. Otherwise → candidate: Plain

  Boolean columns:
   14. If average run length > 2 → candidate: RLE
   15. Otherwise → candidate: Plain (bitpacked by Arrow)

Phase 3 — Score and select:
  16. Plain is always a candidate (universal fallback).
  17. Score all candidates by estimated compressed size
      (Encoding::estimate_size).
  18. Ties broken by decode cost (lower is better, per §10.2).
  19. Winner is the candidate with the smallest estimated size.

Phase 4 — LZ4 post-compression (unchanged from v1):
  20. Compress the winner's payload with LZ4.
  21. If compressed / uncompressed <= 0.9 → use LZ4.
  22. Otherwise → emit uncompressed.
```

### 10.2 Decode cost tiebreaker

When two encodings produce estimated payloads within 5% of each
other, the selector prefers the one with the lower decode cost.
This avoids choosing a complex encoding for marginal size savings.

| Encoding | Cost | Decode hot path |
|---|---|---|
| Constant | 0 | broadcast single value |
| Plain | 1 | memcpy / fixed-stride read |
| RLE | 2 | broadcast per run (or zero-copy RunEndEncoded) |
| BitPacking | 3 | bit-unpack into i64 |
| FOR | 4 | per-block bit-unpack + base add |
| Delta | 5 | bit-unpack + cumulative sum |
| DoubleDelta | 6 | bit-unpack + 2x cumulative sum |
| PFOR | 7 | per-block bit-unpack + base add + patch scatter |
| Dictionary | 8 | bit-unpack + per-row dict lookup |
| ALP | 9 | FOR-unpack mantissas + f64 multiply + patch scatter |
| FSST | 10 | per-byte symbol lookup |

### 10.3 Selector guards (summary from TASK-401)

| Encoding | Guard condition | Rationale |
|---|---|---|
| RLE | avg run length > 2 | Prevents pathological expansion on alternating data (TASK-401 §3.2) |
| DoubleDelta | dd_bit_width < 0.5 * delta_bit_width | Only wins when data is approximately linear; marginal gains don't justify extra prefix-sum (TASK-401 §4.7) |
| FOR | sum(per-block widths) < 0.9 * block_count * global_width | Only wins when local ranges are tighter than the global range (TASK-401 §5.7) |
| PFOR | 1% < outlier fraction < 10% | Below 1%, FOR is sufficient. Above 10%, patch overhead dominates (TASK-401 §6.7) |
| FSST | cardinality / row_count >= 0.3 AND FSST payload < Plain payload | Low cardinality → Dictionary wins. FSST must actually compress (TASK-401 §7.7) |
| ALP | >= 70% of sample values decompose cleanly | Below 70%, exception overhead may exceed Plain+LZ4 (TASK-401 §8.7) |

### 10.4 Expected encodings by column role (v2)

| Column role | v1 encoding | v2 encoding | Improvement |
|---|---|---|---|
| `entity_id` (String, sorted) | Dictionary | **RLE** | ~25x better compression |
| `timestamp` (near-constant delta) | Delta | **DoubleDelta** | ~2x better compression |
| `timestamp` (variable delta) | Delta | Delta (unchanged) | -- |
| `event_type` (low cardinality) | Dictionary | Dictionary (unchanged) | -- |
| `__seq_id` (monotonic) | Delta | **DoubleDelta** | ~2x better |
| Boolean with runs | Plain | **RLE** | 10-100x better |
| Boolean alternating | Plain | Plain (unchanged) | -- |
| URLs, user agents | Plain+LZ4 | **FSST** | ~3x better with random access |
| `amount` (clustered integers) | BitPacking | **FOR** or **PFOR** | 1.5-5x better |
| `price` (round floats) | Plain+LZ4 | **ALP** | ~4x better |
| Random floats | Plain+LZ4 | Plain+LZ4 (unchanged) | -- |

---

## 11. Validation rules (v2 additions)

v2 inherits all v1 validation rules (`segment-format-v1.md` §15).
The following rules are added or modified:

1. **Rule 3 (modified).** Bytes `[4, 6)` are a recognized format
   version. v2 readers accept `version == 1` or `version == 2`.
   Unknown values produce `BqliteError::Corruption`.

2. **Rule 7 (modified).** `footer.format_version` matches the file
   header version. For v2: `footer.format_version == 2`.

3. **Rule 10 (modified).** `ColumnChunkMeta.encoding` is in the
   encoding set for the segment's format version:
   - v1: `{0, 1, 2, 4, 6}`
   - v2: `{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}`

4. **Rule 13 (new).** For each FSST symbol table reference in
   `footer.fsst_symbol_tables`:
   - `byte_offset + byte_length` is inside the FSST symbol tables
     region (between the end of the dictionaries region and the
     start of the footer body).
   - `column_ordinal < footer.schema.columns.len()`.
   - `symbol_count` is in `[1, 256]`.

5. **Rule 14 (new).** For each column chunk with `encoding == 7`
   (FSST), the `symbol_table_id` in the encoding params references
   a valid entry in `footer.fsst_symbol_tables`, and that entry's
   `column_ordinal` matches the chunk's `column_ordinal`.

6. **Rule 15 (new).** A v2 segment must not contain encoding
   discriminant 11 (`FreqEncoding`, retired). The reader rejects it
   as corruption even though discriminant 11 is "reserved" — only
   discriminants 0–10 are legal in v2.

---

## 12. Implementation task mapping

| v2 layout concern | Implementation task |
|---|---|
| `SEGMENT_FORMAT_VERSION_V2` constant, `FooterV2`, `FsstSymbolTableRef`, `SegmentFooter` enum, version-dispatch reader/writer scaffolding | TASK-412 |
| RLE encoding (`Encoding` trait impl) | TASK-413 |
| DoubleDelta encoding | TASK-414 |
| FOR encoding | TASK-415 |
| FSST encoding + symbol table construction + segment-level region integration | TASK-416 |
| ALP encoding | TASK-417 |
| PFOR encoding | TASK-450 |
| v2 encoding selector integration | TASK-419 |

TASK-412 is the merge-first scaffolding task that all encoding
implementations build on. It must land before TASK-413–TASK-418,
TASK-450. TASK-419 (selector) depends on having at least some v2
encodings implemented.

---

## 13. Open questions

None blocking. All decisions in this document are grounded in the
TASK-401 research output and the v1 format contract.

Deferred without blocking v2 merges:

1. **Per-row-group checksums.** Still deferred. v1 §17 item 1
   deferred them to v2, but the segment-level checksum is sufficient
   for Wave 4. If partial-segment recovery becomes a goal in Wave 5,
   extending `RowGroupIndex` with an optional `xxhash64: u64` field
   is an additive change that requires a v3 format bump.

2. **FOR block size 256.** v2 fixes `block_size` at 128. If
   TASK-415 benchmarks show 256 is meaningfully better, the
   encoding can support both values without a format change (the
   `block_size` field is already in the params block). The selector
   would need to choose between 128 and 256 based on the data.

3. **FSST crate integration.** TASK-416 uses the `fsst` crate for the
   core algorithm. The remaining work is writer/read-path
   integration: hoisting the self-contained symbol-table bytes out of
   trait-level chunks into the segment-level FSST region while
   preserving the v2 on-disk contract.

4. **Nested type encodings.** v1 supports `List` and `Map` only via
   Plain encoding. v2 does not add specialized encodings for nested
   types. This is a Wave 5+ consideration.

5. **ZSTD compression codec.** v2 retains LZ4 as the only
   post-encoding compression codec. A heavier codec like ZSTD would
   require extending `CompressionType` and is not justified by
   current workload analysis (bqlite is decode-speed-first per
   `storage-format.md` §10.7).
