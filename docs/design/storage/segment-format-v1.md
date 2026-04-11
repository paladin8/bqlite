# Segment Format v1

**Wave**: 2
**Task**: TASK-201
**Status**: draft — frozen for Wave 2, extended by later waves

## 1. Scope

This note finalizes the **byte-level v1 layout** every Wave 2 segment
file adheres to. Concretely:

- The file-level framing — header, row groups, segment dictionaries,
  footer body, checksum, and trailer — including every fixed-length
  field's width and endianness.
- The row-group and column-chunk layout — how nullable prefixes,
  encoded data, and compression wrappers compose.
- The v1 **encoding set** (`Plain`, `Dictionary`, `Delta`,
  `BitPacking`, `Constant`) and the single v1 compression codec
  (`LZ4`), along with each encoding's on-disk parameter block.
- The footer body's structured fields — schema, row-group index,
  segment-level dictionaries, per-column-chunk metadata with inline
  zone maps.
- Versioning and validation rules that Wave 2 readers trust.

It does **not** design:

- The `SegmentReader` / `SegmentScan` trait surface — that is
  [reader-trait.md](reader-trait.md) (TASK-109, Wave 1). This note is
  the on-disk contract the Wave 2 reader implementation (TASK-215)
  honours behind that trait.
- The runtime encoding trait (`Encoding::encode` / `decode`) — TASK-206
  owns it. This note only pins the on-disk representation each encoding
  produces.
- The encoding selector heuristic — TASK-212. This note defines what
  each encoding looks like on disk; the selector chooses between them.
- The scan interface and predicate pushdown protocol — TASK-202. This
  note provides the zone-map bytes pushdown consumes.
- Manifest format, ingest partitioning, k-way merge, or compaction —
  those are TASK-217, TASK-218, TASK-219, and later waves.
- Wave 4+ encodings (`FSST`, `ALP`, `PFOR`, `FOR`, `DoubleDelta`,
  `RLE`, `Frequency`) and their associated segment-level state (e.g.
  FSST symbol tables). v1 has none of these, and Wave 2 readers are
  not expected to parse them.

Background. The authoritative data-layout story lives in
[storage-format.md](../storage-format.md) §9, §10, §11. This note
pins the v1 subset to concrete bytes so that every Wave 2 storage
task can compile against a fixed contract. After Wave 2 the
`version: 1` layout is frozen; later waves bump the format version
and extend the encoding enum rather than mutating v1 readers.

## 2. Relationship to the existing design docs

The layout defined here is a **minimal v1 compatible projection** of
the richer container described in storage-format.md §9–§11.

| storage-format.md feature | v1 (this note) | Rationale |
|---|---|---|
| Segment container (§9.1, §9.2) | Header → row groups → segment dictionaries → footer body → checksum → trailer. | Matches the §9.2 diagram. The only concrete changes are (a) dropping FSST symbol tables from the footer because FSST is Wave 4, and (b) calling out an explicit "segment dictionaries region" between row groups and footer body so the writer can flush dictionaries without interleaving them with column chunks. |
| Row-group size (§3.3) | Fixed at **65,536 rows** for v1. Writers MAY emit a short final row group (last row group in a segment) when input rows don't divide evenly. | Row-group size is a wave-level knob. Locking it in v1 means readers never have to ask. |
| Per-column-chunk metadata (§9.4) | `byte_offset`, `byte_length`, `encoding`, `encoding_params`, `compression`, `null_count`, `row_count`, inline zone map (`min` / `max`). One record per (row-group, column) pair. | Stored inside the footer body per §9.4 — v1 does **not** add a separate "zone-map block" section even though TASK-201's description mentions one. Zone maps live alongside the column-chunk metadata they belong to, keeping the footer a single serialized record. §10 below explains the terminology split. |
| Encoding set (§10.3, §10.4) | Frozen at `Plain`, `Dictionary`, `Delta`, `BitPacking`, `Constant`. Each has a fixed on-disk parameter block. | TASK-201 description. Wave 4 unlocks `RLE`, `DoubleDelta`, `FOR`, `PFOR`, `FSST`, `ALP`, `Frequency` by bumping the format version. |
| Post-encoding compression (§10.7) | `CompressionType::None` (0) or `CompressionType::Lz4` (1). LZ4 wraps **only** the encoded value bytes — the null bitmap and encoding params stay uncompressed. | LZ4 is the only v1 codec. Keeping the wrapper narrow lets the reader parse null bitmaps and encoding headers without first decompressing, which is what zone-map pruning wants. |
| Segment-level dictionaries (§3.4) | One dictionary per `(segment, column_ordinal)` pair, stored in a "segment dictionaries" region between the last row group and the footer body. Referenced from per-column-chunk metadata by `dict_id: u32`. | §3.4's segment-level reuse direction. The v1 rule is narrow: a column that is `Dictionary`-encoded in any row group has exactly one dictionary for the whole segment, shared by every row group that uses `Dictionary` on that column. Row groups that pick a different encoding for the same column (e.g. `Constant` for an all-identical row group) do not reference the dictionary. |
| Segment-level FSST symbol tables (§3.4, §9.2) | **Deferred.** Not present in v1 footers. | FSST is Wave 4. |
| Zone maps on all columns (§11.1) | Per-column-chunk inline `min` / `max` covering **every** column in the row group, not just role columns. Readers look them up by column ordinal. | §11.1 direction ("zone maps on all columns are stored in the segment footer metadata"). Cheap to maintain; the pruning cost is paid at footer load time, not per row group. |
| Checksum placement (§9.5) | Fixed 8-byte xxHash64 immediately before the 8-byte trailer. Covers the file from offset 0 through the end of the footer body — i.e. every byte except the checksum itself and the trailer. | §9.5 says the segment-level checksum excludes its own bytes. Placing it just before the trailer is the only layout that lets the reader validate the entire file while keeping the trailer the "where is the footer" locator. |
| Validation on open (§9.5) | Magic at offset 0 and at `file_size - 4`, footer length within file bounds, footer body parses, checksum matches. | Matches §9.5 "Segment validation on open". |

### 2.1 Cross-doc consistency

This note does not require edits to `storage-format.md` —
storage-format.md §9 and §10 are the authoritative high-level story
and v1 is a subset of what they describe. One small clarification
lands here so the v1 implementation has a single place to cite:
storage-format.md §9.2 lists "zone-map block" as a conceptual element
of the footer without saying whether it is a distinct physical
section; v1 commits to the **inline** interpretation (zone maps live
in the per-column-chunk metadata records, not in a separate block) to
keep the footer a single serialized record. See §10 below.

## 3. Crate placement

Every v1 implementation concern is internal to `bqlite-storage`. The
byte-level format does not appear in any public API beyond what the
Wave 1 `SegmentReader` trait (`bqlite-core`) already exposes:

| Item | Crate | Notes |
|---|---|---|
| `FormatVersion`, `EncodingType`, `CompressionType` enums | `bqlite-storage` | Internal to the crate. Wave 2 code never exposes them across the crate boundary. |
| `SegmentWriter` (TASK-213) | `bqlite-storage::segment::writer` | Consumes encoded column chunks; emits the v1 byte layout. |
| `SegmentFileReader` (TASK-215) | `bqlite-storage::segment::reader` | Parses the v1 byte layout into a `SegmentScan` implementation. |
| `Encoding` trait + concrete encodings | `bqlite-storage::encoding` | TASK-206 – TASK-211. Decode emits Arrow arrays; encode accepts Arrow arrays. |
| `SegmentDictionary` / `RowGroupIndex` / `ColumnChunkMeta` | `bqlite-storage::segment::layout` | Internal plain-data structs; used by writer and reader. |

The Wave 1 trait surface in `bqlite-core::storage` does not change —
`SegmentHandle`, `ZoneMap`, `ColumnProjection`, and `Predicate`
remain the only public types Wave 2 scan work depends on.

## 4. File layout

```
┌──────────────────────────────────────────┐  offset 0
│  File header (6 bytes, fixed)            │
│    magic[4]    = "BQLT"                  │
│    version[2]  = u16 little-endian       │
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
│  possibly empty)                         │
│    Dictionary 0  (contiguous bytes)      │
│    Dictionary 1                          │
│    ...                                   │
├──────────────────────────────────────────┤
│  Footer body (variable, postcard-encoded)│
│    TableSchema (serialized)              │
│    schema_version: u32                   │
│    format_version: u16                   │
│    row_count: u64                        │
│    row_group_count: u32                  │
│    row_group_size_hint: u32              │
│    creation_timestamp_ns: i64            │
│    seq_id_range: (u64, u64)              │
│    batch_id: u64                         │
│    compaction_level: u8                  │
│    dictionaries: Vec<SegmentDictRef>     │
│    row_groups: Vec<RowGroupIndex>        │
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

Constants (v1):

| Constant | Value |
|---|---|
| `MAGIC` | ASCII `BQLT` → `[0x42, 0x51, 0x4C, 0x54]` |
| `FORMAT_VERSION` | `1` (u16) |
| `ROW_GROUP_SIZE_DEFAULT` | `65_536` rows |
| `CHECKSUM_ALGORITHM` | xxHash64 (seed `0`) |
| Byte order | Little-endian, matching storage-format.md §2 |

**Alignment.** No field inside the file is required to be naturally
aligned. Writers emit bytes contiguously with no padding. Readers
copy values via `u64::from_le_bytes` / `u32::from_le_bytes` rather
than unaligned pointer casts. This keeps the file format
architecture-neutral at a trivial cost (the alternative — aligning
row-group starts to 64 bytes — saves nothing measurable and
complicates writers).

**Single-pass open.** Opening a segment is three I/Os (pread-style):

1. Read last 8 bytes → trailer. Validate magic. Read
   `footer_body_length`.
2. Read `[file_size − 16 − footer_body_length, file_size − 16)` —
   the footer body + checksum — in one I/O.
3. When checksum verification is enabled, read
   `[0, file_size − 16 − footer_body_length)` and rehash. Verification
   mode is per §9.5 (default-on for first read after ingest /
   compaction, configurable off).

Every row-group decode after open is a random-access read over the
mmap or pread-backed buffer; no further footer lookups are needed.

## 5. File header (fixed, 6 bytes)

```
offset 0      magic[4]    = { 'B', 'Q', 'L', 'T' }    // 0x42 0x51 0x4C 0x54
offset 4      version[2]  = u16 LE                    // 1 for v1
```

Readers reject files whose first four bytes are not `BQLT` with
`BqliteError::Corruption` (new variant introduced alongside TASK-215
if not already present). Readers reject a `version` they do not
recognize with the same error — v1 readers only understand
`version == 1`. Later waves may accept multiple versions.

## 6. Row groups

A row group is a self-contained sequence of column chunks, one chunk
per declared column in the segment's schema. Column chunks appear
in **table-schema ordinal order** — ordinal `0` first, then `1`,
etc. Implicit system columns (`__seq_id`, `__batch_id`) are stored
as regular columns with ordinals equal to their position in the
segment-internal schema snapshot (writer convention: system columns
come after user columns in the stored schema).

Column chunks inside a row group are packed back to back with no
padding. The footer's `RowGroupIndex` tracks each chunk's absolute
byte offset and length, so the reader reads only the columns named
by the `ColumnProjection` passed to `SegmentReader::open_segment`.

**Row count.** Each row group's row count is stored in its
`RowGroupIndex` entry. Writers emit exactly
`ROW_GROUP_SIZE_DEFAULT = 65_536` rows per row group with one
exception: the **last row group in the segment** may be short when
the input does not divide evenly. Readers never assume every row
group is the default size.

**Entity-boundary invariant.** Per storage-format.md §7.2: entity
boundaries do not align with row-group boundaries, but an entity
that fits in a single row group is never split across two row
groups unnecessarily. Writers enforce this by deciding row-group
boundaries at entity breakpoints when the next entity would fit
within the remaining row-group budget plus a small slack. Entities
larger than `ROW_GROUP_SIZE_DEFAULT` occupy consecutive row groups
within the same segment (never split across segments). The writer
orchestration (TASK-214) owns this policy; the format itself only
requires that row groups are independently decodable.

**Zero rows.** An empty row group (0 rows, 0 column chunks, 0 bytes)
is illegal in v1. Writers must not emit them. Readers that encounter
one return `BqliteError::Corruption`.

## 7. Column chunk

```
┌───────────────────────────────────────────┐  byte_offset
│ Null bitmap (optional, variable)          │
│   Present iff the column is declared      │
│   nullable in the segment schema.         │
│   Length = ceil(row_count / 8) bytes.     │
│   Arrow LSB-first convention: bit i == 1  │
│   means row i is non-null.                │
├───────────────────────────────────────────┤
│ Encoding header (variable)                │
│   encoding: u8                            │
│   encoding_params: per §9                 │
│   uncompressed_payload_length: u32 LE     │
├───────────────────────────────────────────┤
│ Encoded payload (variable)                │
│   Optionally LZ4-wrapped (see §8).        │
│   On-disk length derived from byte_length.│
└───────────────────────────────────────────┘  byte_offset + byte_length
```

Column chunk metadata in the footer (`ColumnChunkMeta`, §10.2) lists
`byte_offset`, `byte_length`, `encoding`, `compression`, `row_count`,
`null_count`, and the zone map. `byte_length` spans everything from
the start of the null bitmap (or the encoding header, when the column
is non-nullable) through the end of the encoded payload.

**`uncompressed_payload_length` semantics.** The u32 LE stored
immediately before the payload is always the **decoder-side** (i.e.
post-decompression) byte count of the encoding's native payload —
including any SIMD tail padding the encoding requires (§9.2–§9.4).
Writers use it to size decompression buffers; readers use it to know
how many bytes to feed into the encoding decoder after optional
LZ4 decompression. When `compression == None`, it also happens to
equal the on-disk payload byte count. When `compression == Lz4`, the
on-disk compressed payload byte count is
`compressed_payload_length = byte_length − (null_bitmap_length +
encoding_header_length)`, where `encoding_header_length` is the
number of bytes the reader consumed while parsing the encoding
discriminant, the encoding's param block, and the
`uncompressed_payload_length` field itself. Both writer and reader
compute `encoding_header_length` via the same per-encoding parsing
routine in §9 — it is *not* a stored field, and it is not
necessarily a constant-per-discriminant value (see §9.5 Constant).

**Finding the payload start without decompression.** The null bitmap
and the full encoding header (discriminant + params +
`uncompressed_payload_length`) are always uncompressed. A reader
that wants to locate the payload start does so by:

1. Reading the null bitmap when the column is nullable
   (`ceil(row_count / 8)` bytes).
2. Dispatching on the `encoding` discriminant and parsing the
   encoding-specific `encoding_params` per §9.
3. Reading the trailing `uncompressed_payload_length: u32 LE` that
   closes the encoding header.

Only then does the reader read `compressed_payload_length` bytes and
(if `compression == Lz4`) hand them to `lz4_flex`. The encoding
header parsing is a pure byte walk with no decompression; it works
for every v1 encoding including Constant, whose param block includes
a type-dependent `value` field.

**Null bitmap presence.** v1 uses a simple rule: the null bitmap is
present if and only if the segment schema declares the column
nullable. A nullable column whose row group happens to contain zero
nulls **still writes a bitmap** (all ones). This keeps readers
branch-free — "is there a bitmap?" is answered by schema lookup,
not by reading a flag. The small fixed cost
(`ceil(row_count / 8) = 8192` bytes per row group for the default
size) is dwarfed by encoded data.

**Encoding header.** The 1-byte `encoding` discriminant and its
variable-length `encoding_params` block are not compressed — they
are metadata the reader needs before it can even find the payload.
The trailing `uncompressed_payload_length: u32 LE` is also
uncompressed, so the reader can size a decompression buffer and
compute the compressed byte count without peeking at the footer
again (the redundancy is cheap and catches footer/payload desyncs).

**Compression interaction.** When `compression == Lz4`, the
compressed bytes that follow the encoding header decompress to
exactly `uncompressed_payload_length` bytes of the encoding's
native byte stream. The null bitmap and encoding header themselves
stay uncompressed.

## 8. Compression (LZ4, `CompressionType::Lz4`)

v1's only post-encoding compression codec. Applied to the encoded
payload only — never to null bitmaps, encoding headers, dictionaries,
or the footer body.

**Codec.** `lz4_flex` (no_std-compatible, pure Rust) using the
**LZ4 block** format — a single compressed block, not the LZ4 frame
format. Per §7, the block's decompressed length is carried in the
encoding header's `uncompressed_payload_length` field; the
compressed length is derived by the reader as
`byte_length − (null_bitmap_length + encoding_header_length)`,
where every term on the right is known before any decompression
happens. This keeps the on-disk representation free of LZ4 frame
metadata we would have to parse and validate ourselves.

**Minimum compression threshold.** Per storage-format.md §10.7, LZ4
is only used when the compressed output is at least 10% smaller than
the input (`compressed / uncompressed ≤ 0.9`). Below that threshold
the writer emits the uncompressed bytes with `compression = None`.
The selector (TASK-212) owns the decision — this section only
specifies what "compressed" means on disk.

**Decode.** The reader computes `compressed_payload_length` as
described above, reads exactly that many bytes from the payload
region, sizes a destination buffer to
`uncompressed_payload_length`, and calls
`lz4_flex::block::decompress_into(&mut buf, compressed_bytes,
uncompressed_payload_length)`. The decompressed `buf` is then handed
to the encoding decoder. `decompress_into` is zero-allocation after
the destination buffer is sized.

**Versioning.** `CompressionType::Lz4 = 1`. `CompressionType::None = 0`
is the only other v1 value. Future codecs (ZSTD, etc.) extend the
enum and require a format-version bump.

## 9. Encoding set

v1's five primary encodings cover every Wave 2 column type. Later
waves add encodings by extending `EncodingType` and bumping the
format version.

```rust
#[repr(u8)]
pub enum EncodingType {
    Plain      = 0,
    Dictionary = 1,
    Delta      = 2,
    BitPacking = 4,
    Constant   = 6,
}
```

The numeric values match storage-format.md §10.2 so later waves can
add `Rle = 5`, `Fsst = 7`, etc. without renumbering. Readers reject
any discriminant outside the v1 set with `BqliteError::Corruption`.

Every encoding is defined as a pair of on-disk blocks:

- **`encoding_params`** — fixed-layout metadata the decoder needs
  before touching the payload. Placed in the column chunk's encoding
  header (§7). Written uncompressed.
- **Payload** — the value bytes. Optionally LZ4-wrapped.

For each encoding below, the parameter block is spelled out
byte-by-byte and the payload format is described in decode order.

### 9.1 Plain (`encoding = 0`)

Zero-overhead encoding. Fixed-width types are stored as contiguous
little-endian values; variable-width types use a 32-bit length
prefix per value.

**encoding_params.** Empty (0 bytes). The decoder reads the column's
`BqlType` from the segment schema and dispatches accordingly.

**Payload — fixed-width types.**

| Type | Bytes per value | Encoding |
|---|---|---|
| `Bool` | `ceil(row_count / 8)` bytes total | Packed LSB-first bitmap; bit `i` == 1 means row `i` is true. Shares the Arrow convention used for null bitmaps. |
| `Int` | `8 * row_count` | Little-endian i64. |
| `Float` | `8 * row_count` | Little-endian IEEE 754 f64. |
| `Timestamp` | `8 * row_count` | Little-endian i64 nanoseconds-since-epoch UTC. |

**Payload — `String`.**

```
For i in 0..row_count:
    length[i]: u32 LE
    bytes[i]:  UTF-8, length[i] bytes
```

Null rows are already filtered out by the null bitmap — the Plain
payload stores only the non-null values in row order, and the decoder
uses the bitmap to re-expand them into an Arrow `StringArray`. The
payload therefore contains `row_count − null_count` entries, not
`row_count`. Same rule applies to every variable-width encoding in
v1.

**Payload — `List(T)` and `Map(T)`.** v1 supports list and map
columns only via the Plain encoding. The payload layout follows
storage-format.md §15: for lists,
`[offsets: (row_count+1) × u32 LE] [values: Plain(T) payload]`;
for maps, `[offsets: (row_count+1) × u32 LE] [keys: Plain(String)
payload] [values: Plain(T) payload]`. Advanced encodings for nested
types are deferred to Wave 4.

**Decode cost.** Zero for fixed-width types (the on-disk bytes are
valid Arrow buffers after an alignment-preserving copy). One
allocation + memcpy per row for `String`, per storage-format.md §10.1.

### 9.2 Dictionary (`encoding = 1`)

Maps distinct values to integer codes. The dictionary is stored
once per column at the segment level (§11); each row group's column
chunk stores only the codes.

**encoding_params.**

```
dict_id:        u32 LE   // index into footer.dictionaries
code_bit_width: u8       // 1..=24, capped by the 16M dictionary-size limit (§10.3)
```

Reader invariant: `dict_id < footer.dictionaries.len()` and
`footer.dictionaries[dict_id].column_ordinal == this_chunk.column_ordinal`.

**Payload.** Bit-packed u32 codes, `code_bit_width` bits per code,
`row_count − null_count` codes. Bit packing is little-endian within
each 8-byte lane (FastLanes-compatible). The byte count is
`ceil(code_bit_width × (row_count − null_count) / 8)` rounded up to
the next multiple of 8 so the SIMD bit-packing routines can read
one full lane past the last code without a bounds check. The rounded
count — including the trailing SIMD padding — is the value written
into `uncompressed_payload_length` and accounted for in the
column chunk's `byte_length`. The reader never has to reason about
padding separately: it reads `uncompressed_payload_length` bytes of
code stream and hands them to the SIMD decoder.

**Dictionary reference.** The segment-level dictionary entry
(§11.1) carries the sorted distinct values. Codes are **ordinal
into the sorted dictionary**, so `codes[i] == dict_values.index_of(row_value_i)`.
Readers that evaluate equality predicates can resolve the predicate
against the dictionary once (producing a code value) and then compare
against the bit-packed codes with no string work — the optimization
storage-format.md §10.4 calls out as "must be in v1".

**Applicable types.** `String`, `Int`. v1 does not dictionary-encode
`Float`, `Bool`, `Timestamp`, `List`, or `Map` (the selector chooses
other encodings for those). Readers nevertheless decode
dictionary-encoded `String` and `Int` unconditionally.

### 9.3 Delta (`encoding = 2`)

Stores differences between consecutive values. Suitable for
monotonic-ish integer / timestamp columns within an entity.

**encoding_params.**

```
base_value:          i64 LE   // first non-null row's value
residual_bit_width:  u8       // 1..=64
```

**Payload.** `row_count − null_count − 1` zigzag-encoded i64
residuals, bit-packed at `residual_bit_width` bits per residual. The
zigzag encoding preserves signed deltas in an unsigned bit packer:
`zigzag(x) = (x << 1) ^ (x >> 63)`. The first value (`base_value`)
lives in the parameter block, not the payload.

The byte count is
`ceil(residual_bit_width × max(0, row_count − null_count − 1) / 8)`
rounded up to the next multiple of 8 for SIMD-friendly unpacking.
As in §9.2, the rounded count — including trailing padding — is
what `uncompressed_payload_length` reports and what `byte_length`
counts.

**Applicable types.** `Int`, `Timestamp`. Not applicable to `Float`,
`String`, `Bool`, `List`, `Map`.

**Edge cases.**

- `row_count − null_count == 0`: illegal. A column chunk with zero
  non-null values picks `Constant` with `value = NULL` (see §9.5)
  or `Plain` with an empty payload. The selector never picks Delta
  here.
- `row_count − null_count == 1`: legal. `base_value` carries the
  lone value; the residual payload is 0 bytes.
- Overflow: the writer computes residuals in `i128` and rejects a
  chunk if any residual overflows `i64`, falling back to another
  encoding. The on-disk format therefore never carries values the
  decoder cannot reconstruct.

### 9.4 BitPacking (`encoding = 4`)

Fixed-width bit-packed integers offset from a minimum value. In v1
BitPacking is usable as a **primary** encoding for narrow-range
integer columns (Frame-of-Reference-style usage) and is also the
internal payload format for Dictionary codes (§9.2) and Delta
residuals (§9.3). Only the primary-encoding case lands as its own
`ColumnChunkMeta.encoding` discriminant; the other two usages are
hidden inside their parent encoding's payload.

**encoding_params.**

```
min_value:  i64 LE   // subtracted from each value before packing
bit_width:  u8       // 1..=64
```

**Payload.** `row_count − null_count` unsigned offsets `value − min_value`,
bit-packed at `bit_width` bits per offset. Byte count
`ceil(bit_width × (row_count − null_count) / 8)` rounded up to the
next multiple of 8. Trailing padding is counted in both
`uncompressed_payload_length` and `byte_length`, same rule as §9.2
and §9.3.

**Applicable types.** `Int`, `Timestamp`. Readers decode into
Arrow `Int64Array` or `TimestampArray` respectively.

**SIMD.** The decoder targets the `bitpacking` crate's FastLanes
layout (storage-format.md §10.1). The writer guarantees the payload
is padded to the FastLanes lane size so the SIMD decoder never reads
out of bounds.

### 9.5 Constant (`encoding = 6`)

Zero-data encoding for chunks whose non-null values are all identical.
Common in Wave 2 because the `entity_id` column within a single row
group is frequently a single value (long entity runs).

**encoding_params.**

```
value_kind: u8           // 0 = literal, 1 = all-null
value:      variable     // present iff value_kind == 0
```

`value_kind = 1` signals a chunk with `null_count == row_count` —
every row is null and no literal is stored. This is the canonical
encoding for all-null row groups; it is never combined with LZ4.

`value_kind = 0` carries the single non-null value, serialized per
its `BqlType`:

| Type | `value` bytes |
|---|---|
| `Bool` | 1 byte, `0x00` or `0x01` |
| `Int` | 8 bytes, i64 LE |
| `Float` | 8 bytes, IEEE 754 f64 LE |
| `Timestamp` | 8 bytes, i64 LE nanoseconds |
| `String` | `u32 LE length` + UTF-8 bytes |
| `List`, `Map` | not permitted in v1 (selector falls through to `Plain`) |

**Payload.** 0 bytes. `uncompressed_payload_length` is `0`, and
no bytes follow the encoding header. Because the payload is empty,
LZ4 is never applied to a `Constant` chunk — the writer emits
`compression = None` unconditionally.

**Variable-length encoding header.** Unlike the four other v1
encodings, `Constant`'s encoding header size depends on the column's
`BqlType` (via the trailing `value` field when `value_kind == 0`).
This is safe because readers always know the column's type — they
look it up in `FooterV1.schema.columns[column_ordinal].ty` before
parsing the chunk — and the type determines `value`'s width:
1 byte for `Bool`, 8 bytes for `Int`/`Float`/`Timestamp`, and a
`u32 LE length` + UTF-8 bytes for `String`. When
`value_kind == 1` (all-null), the header is exactly 2 bytes:
`value_kind: u8` and the trailing `uncompressed_payload_length: u32 LE`
(2 + 4 = 6 bytes total including the discriminant). The
`encoding_header_length` term used in §7 and §8 is therefore computed
by parsing — not by looking up a per-discriminant constant — and the
Constant case is the only v1 encoding where that distinction matters.

**Decode.** The reader allocates an Arrow array of `row_count`
rows, uses the null bitmap to place nulls, and broadcasts the
constant value into every non-null slot.

## 10. Footer body

Everything the reader needs after framing validation lives in the
footer body. The footer body is serialized with **`postcard`** —
a compact, no_std, deterministic serde format. The Cargo dependency
is pinned (`postcard = "=1.0"` at the time of writing) so that
v1 segment bytes remain stable across bqlite versions; any future
`postcard` format break forces a format-version bump, which is
exactly the policy we want.

### 10.1 Top-level struct

```rust
#[derive(Serialize, Deserialize)]
struct FooterV1 {
    /// Must equal `FORMAT_VERSION` (1). Duplicated from the header
    /// so that a stray footer (e.g. in a hex dump) is self-identifying.
    format_version: u16,

    /// Full segment schema at write time (type-system.md §5 snapshot).
    /// Readers use this for both decoding and backfilling columns
    /// added after the segment was written — see reader-trait.md
    /// §2 "Schema evolution".
    schema: TableSchema,

    /// Schema version the segment was written against.
    schema_version: u32,

    /// Total rows in the segment across all row groups.
    row_count: u64,

    /// Number of row groups in `row_groups`. Redundant with
    /// `row_groups.len()` but stored explicitly so the reader can
    /// preallocate before deserializing the vec.
    row_group_count: u32,

    /// Row-group size the writer used. v1 writers always emit
    /// `ROW_GROUP_SIZE_DEFAULT = 65_536`; recording it here lets
    /// later waves vary it without bumping the format version.
    row_group_size_hint: u32,

    /// Creation timestamp in nanoseconds since epoch UTC.
    /// (storage-format.md §12.3 `SegmentMeta.created_at`.)
    creation_timestamp_ns: i64,

    /// Sequence-ID range covered by this segment as
    /// `(min_inclusive, max_inclusive)` — both endpoints are
    /// actual `__seq_id` values present in the segment, not a
    /// half-open interval. Empty segments are illegal (§6), so
    /// `min_inclusive ≤ max_inclusive` always holds.
    /// (§6.2, §12.3.)
    seq_id_range: (u64, u64),

    /// The batch ID this segment was produced from (§6.2).
    batch_id: u64,

    /// Compaction tier — 0 for L0 ingest output in Wave 2.
    compaction_level: u8,

    /// Segment-level dictionaries, one entry per dictionary-encoded
    /// column. Order is arbitrary; entries are referenced by their
    /// index in this vector via `dict_id`.
    dictionaries: Vec<SegmentDictRef>,

    /// Per-row-group index. `row_groups[i]` describes row group `i`.
    row_groups: Vec<RowGroupIndex>,
}
```

### 10.2 Row-group and column-chunk records

```rust
#[derive(Serialize, Deserialize)]
struct RowGroupIndex {
    /// Offset of the row group's first column chunk in the segment
    /// file. Every column chunk in this row group lives in
    /// `[byte_offset, byte_offset + byte_length)`.
    byte_offset: u64,
    byte_length: u64,

    /// Row count for this row group. For every row group except
    /// (possibly) the last, this equals `row_group_size_hint`.
    /// Stored as u64 to match storage-format.md §9.4; postcard's
    /// varint encoding makes the width choice cost-neutral on disk.
    row_count: u64,

    /// Per-column metadata in column-ordinal order. Length equals
    /// `schema.columns.len()` — every column has an entry even if
    /// its payload is 0 bytes (e.g. `Constant` all-null).
    columns: Vec<ColumnChunkMeta>,
}

#[derive(Serialize, Deserialize)]
struct ColumnChunkMeta {
    /// Index into `FooterV1.schema.columns`.
    column_ordinal: u32,

    /// Absolute offset of this column chunk in the segment file
    /// (start of the null bitmap if nullable, otherwise the
    /// encoding header).
    byte_offset: u64,

    /// Total bytes — null bitmap (if present) + encoding header
    /// + on-disk payload (compressed bytes when `compression == Lz4`,
    /// `uncompressed_payload_length` bytes otherwise).
    byte_length: u64,

    /// `EncodingType` discriminant from §9.
    encoding: u8,

    /// `CompressionType` discriminant from §8 (None | Lz4).
    compression: u8,

    /// Non-null row count. u64 to match storage-format.md §9.4.
    row_count: u64,
    /// Null row count. `row_count + null_count == row-group row_count`.
    null_count: u64,

    /// Inline zone map. Populated even when the encoding is
    /// `Constant` — in that case `min == max == value`. For
    /// all-null chunks (`null_count == row_count`), both are `None`.
    zone_min: Option<PropertyValue>,
    zone_max: Option<PropertyValue>,
}
```

`PropertyValue` is the `bqlite-core` scalar boundary type — using it
here means zone maps serialize the same way in the segment footer
and in the `ZoneMap` values the Wave 1 trait surface hands to
`Predicate::accepts_zone` (reader-trait.md §6.2). The reader
materializes a `ZoneMap` struct by copying `zone_min`, `zone_max`,
`null_count`, and `row_count` out of the `ColumnChunkMeta` at
segment open — no per-query footer re-read.

**"Zone-map block" terminology clarification.** TASK-201's task
description lists "zone-map block" as one of the v1 layout sections.
v1 does not emit a separate on-disk block for zone maps: every
per-column-chunk record already carries its min/max inline, so a
second structure would duplicate the same data with a different
index shape. Wave 2 readers materialize the
`row_group_zone_maps(idx)` result by walking `row_groups[idx].columns`
and copying each `ColumnChunkMeta` into a `HashMap<String, ZoneMap>`.
Treating "zone-map block" as an alias for "the subset of footer bytes
devoted to zone maps" — not as a distinct container — is the v1
decision.

### 10.3 Segment-dictionary references

```rust
#[derive(Serialize, Deserialize)]
struct SegmentDictRef {
    /// The column this dictionary is for. A column has at most one
    /// dictionary per segment; multiple row groups that pick
    /// `Dictionary` encoding for this column all reference it.
    column_ordinal: u32,

    /// Absolute byte offset of the dictionary values in the segment
    /// file. Points into the "segment dictionaries region" between
    /// the last row group and the footer body.
    byte_offset: u64,
    byte_length: u64,

    /// Number of distinct values in the dictionary.
    cardinality: u32,

    /// Dictionary values are serialized as a Plain payload of the
    /// column's `BqlType` (§9.1) — same format a Plain-encoded
    /// column chunk would produce. No null bitmap: dictionaries
    /// never contain null entries (null is represented at the
    /// row-group level by the null bitmap, not by a dictionary
    /// entry). Values are **sorted** ascending; codes are ordinals
    /// into the sorted vector.
    value_type: BqlType,
}
```

**Sorted values.** Storing dictionary values in ascending order
makes range predicates (`col < 'foo'`) tractable at pushdown time —
the predicate resolves to a code range, and the bit-packed code
stream is scanned against that range. Equality predicates are
already cheap regardless of order, so the sort cost is paid once
per segment at write time.

**Dictionary size limit.** v1 caps a single dictionary at
`2^24 = 16_777_216` entries. Beyond that, the selector falls through
to `Plain + LZ4` for the column. This keeps `code_bit_width ≤ 24`,
which fits comfortably in the FastLanes SIMD pipeline.

### 10.4 Why postcard and not a hand-rolled format

The footer body is a richly-typed record (nested vecs, enums, option
types) whose performance cost is paid once per segment open — not
per row. A hand-rolled binary layout buys nothing observable and
costs a significant maintenance burden. `postcard` gives us:

- Deterministic bytes for a given `FooterV1` value (reproducible
  segments for tests).
- Compact encoding (variable-length integers, no field tags).
- Pure-Rust no_std dep already compatible with the rest of the
  workspace.
- Compile-time schema via serde — field rename / reorder is caught
  by the compiler, not by corruption at runtime.

The same argument does **not** apply to column-chunk payloads. Those
are hot-path decoded once per row group read, so they are defined at
the byte level (§9) with no serde involvement.

**Version pinning.** The storage crate's `Cargo.toml` pins postcard
to an exact version (`postcard = "=1.0"`). Any postcard bump that
changes the wire format forces a bqlite format-version bump — the
v1 footer bytes stay readable because v1 readers link the v1
postcard version, and v2 readers handle both.

## 11. Segment-dictionaries region

Located between the last row group and the footer body. Contains the
concatenated bytes of every segment-level dictionary, one after
another, with no framing headers — framing lives entirely in the
footer's `dictionaries` vec.

```
byte_offset                   = row_groups_end
byte_offset + byte_length     = start_of_footer_body

For each dict in footer.dictionaries (arbitrary order):
    bytes at [dict.byte_offset, dict.byte_offset + dict.byte_length)
```

A segment with no dictionary-encoded columns has a zero-length
dictionary region; `footer.dictionaries.len() == 0` and
`row_groups_end == start_of_footer_body`. Writers must not emit
padding bytes in the dictionary region.

**Why not interleaved with row groups.** §3.4 of storage-format.md
motivates segment-level dictionaries by cross-row-group reuse. If
dictionaries were interleaved inside row groups, a reader that skips
a row group would still have to read its dictionary bytes. Grouping
them between row groups and the footer lets the reader load every
dictionary in one I/O at segment open and keep them resident for
the lifetime of the `SegmentScan`.

**Memory cost.** Dictionaries are small by construction (capped at
16M entries, typical workloads land in the hundreds-to-thousands
range). Loading every dictionary eagerly at open is the v1 strategy;
if a later benchmark shows the memory footprint matters, lazy
dictionary loading is an additive extension.

## 12. Checksum (8 bytes)

```
offset = file_size − 16
bytes  = xxHash64(file[0 .. file_size − 16]).to_le_bytes()
```

That is: the checksum covers the header, every row group, the
segment-dictionaries region, and the footer body — in other words,
every byte in the file except the checksum itself and the trailer.
The seed is `0` (xxHash64 default).

Verification is per storage-format.md §9.5:

- **Default.** First read after ingest / first read after compaction.
- **Paranoid mode.** Every read.
- **Off.** Skip entirely (benchmarking / trusted environments).

A mismatch produces `BqliteError::Corruption` and the segment is
treated as unreadable — the reader falls back to the rest of the
matching segments in the manifest inventory (the k-way merge layer
from TASK-219 handles "one segment is corrupt" gracefully).

## 13. Trailer (8 bytes, fixed)

```
offset = file_size − 8
bytes  = footer_body_length: u32 LE || magic[4] = "BQLT"
```

`footer_body_length` is the byte count of the **footer body only** —
not including the checksum, not including the trailer itself. The
reader computes `footer_start = file_size − 16 − footer_body_length`
and reads `[footer_start, file_size − 16)` to deserialize the footer.

Trailer magic identifies the file as a bqlite segment even in a
truncated or mis-named context. Magic mismatches produce
`BqliteError::Corruption`.

## 14. Schema evolution

The footer's `schema` field is the segment's **write-time** schema.
Older segments read under a newer manifest schema rely on the reader
to backfill missing columns with NULL or the column's default value,
per the Wave 1 reader-trait.md §2 rule. v1 does not require segment
rewriting on `ALTER TABLE ADD COLUMN` — the new column simply reads
as NULL from every segment whose `schema_version` predates the
column.

Projection into the current schema is **column-name based**, not
ordinal based. The contract, also captured in TASK-232's handling
of `AlterTableAddColumnPhysical`, is:

1. The reader receives a `ColumnProjection` naming current-schema
   column names.
2. For each requested column, the reader looks the name up in the
   segment's write-time `FooterV1.schema`:
   - If the column exists in the write-time schema, the reader
     decodes its column chunks and emits them as the corresponding
     Arrow array.
   - If the column is absent (added after the segment was written),
     the reader fabricates an all-null Arrow array of the requested
     type (or materializes the column's DEFAULT value if the
     current schema declares one) and emits it alongside the
     decoded columns.
3. The output `RecordBatch` carries columns in the order requested
   by the projection, not in the write-time schema's ordinal order.

This keeps the v1 format free of a per-column `added_in_version`
field and free of any ordinal-mapping table: the segment footer
stores a self-contained snapshot of the schema that produced it,
and the reader handles mismatches by matching names against the
current manifest schema (which the engine passes in when the
`SegmentReader` is constructed). Column **renames** are explicitly
out of scope for Wave 2 — query-language.md §20 does not define
an `ALTER TABLE RENAME COLUMN` verb — so name-based projection is
unambiguous for every schema change v1 supports.

## 15. Validation rules

On segment open, readers validate:

1. File size ≥ `6 (header) + 16 (checksum + trailer)`.
2. Bytes `[0, 4)` equal `"BQLT"`.
3. Bytes `[4, 6)` are a recognized format version (v1 accepts only
   `version == 1`).
4. Trailer bytes `[file_size − 4, file_size)` equal `"BQLT"`.
5. `footer_body_length` fits in a `usize`, and
   `16 + footer_body_length + 6 ≤ file_size`.
6. Footer body deserializes cleanly with postcard into a `FooterV1`.
7. `footer.format_version == 1`.
8. `footer.row_group_count == footer.row_groups.len()`, and the
   `row_count` of each row group sums to `footer.row_count`.
9. For each row group, `row_groups[i].byte_offset +
   row_groups[i].byte_length ≤ row_groups_end`.
10. For each column-chunk meta, `byte_offset + byte_length` is inside
    the parent row-group's byte range, `encoding` is in the v1 set
    `{0, 1, 2, 4, 6}`, and `compression` is in `{0, 1}`.
11. For each dictionary reference, `byte_offset + byte_length` is
    inside `[row_groups_end, start_of_footer_body)`, and
    `column_ordinal < footer.schema.columns.len()`.
12. Checksum matches (when verification is enabled).

Any violation produces `BqliteError::Corruption` with a variant-level
reason code. Writers emit an atomic rename from `*.tmp` only after
flushing a fully valid file — so a segment that fails validation is
either a partial write (cleaned up by TASK-239's startup orphan
sweep) or an externally-corrupted file.

## 16. Wave 2 implementation task mapping

| v1 layout concern | Implementation task |
|---|---|
| Header / row groups / trailer framing | TASK-213 (segment writer) + TASK-215 (segment reader) |
| Encoding trait + Plain reference impl | TASK-206 |
| Dictionary encoding + `SegmentDictRef` writer + reader | TASK-207 |
| Delta encoding | TASK-208 |
| BitPacking encoding (standalone + Dictionary / Delta internal use) | TASK-209 |
| Constant encoding | TASK-210 |
| LZ4 post-encoding wrapper | TASK-211 |
| Encoding selection heuristic across the v1 set | TASK-212 |
| Footer `dictionaries` region placement + writer orchestration | TASK-214 |
| Inline zone-map writer / reader / pruning | TASK-216 |
| Manifest-level `SegmentMeta` integration (batch id, seq range, etc.) | TASK-217 |

Every task above can build its slice without a second look at this
document — the byte-level layout and per-encoding parameter blocks
are the contract.

## 17. Open questions

Deferred without blocking v1 merges.

1. **Per-row-group checksums.** §9.5 explicitly defers these to v2;
   v1 has only the segment-level checksum. If partial-segment recovery
   becomes a goal, extending the row-group index with an optional
   `xxhash64: u64` field is additive and requires only a version
   bump.

2. **Row-group-aligned reads.** Wave 2 uses buffered pread; a future
   wave may switch to `io_uring` / `SetFileIoOverlappedRange` /
   `O_DIRECT` for aggregate scans over cold data. The format does not
   currently mandate row-group offsets aligned to any boundary. If
   direct I/O turns out to need 4 KiB alignment, writers can start
   padding row groups without bumping the format version — the
   footer's `byte_offset` values already cover arbitrary positions.

3. **Segment-level FSST symbol tables.** Wave 4 will add these as a
   second segment-level region, parallel to the dictionaries region.
   The v1 footer does not reserve space for them; their addition
   bumps the format version.

4. **Dictionary ordering beyond ASCII.** v1 dictionaries are sorted
   by the `BqlType`'s native comparison (byte-wise for `String`,
   numeric for `Int`). Locale-aware collation for string predicates
   is a Wave 5 language decision; v1 commits only to the byte-wise
   order because it matches the Arrow default.

5. **Per-column compression choice.** The v1 selector (TASK-212)
   may choose different compression codecs per column-chunk in a
   future wave (e.g. ZSTD for cold columns). The format already
   tracks `compression` per chunk, so this is a selector change,
   not a format change.
