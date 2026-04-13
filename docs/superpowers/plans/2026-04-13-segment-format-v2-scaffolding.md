# Segment Format v2 Reader/Writer Scaffolding (TASK-412)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add structural support for segment format v2 — new version constants, encoding discriminants, FooterV2/FsstSymbolTableRef types, SegmentFooter dispatch enum, v1/v2 reader coexistence, v2 writer plumbing — without landing individual v2 codec implementations.

**Architecture:** Extend `bqlite-storage` with v2 format awareness. The `EncodingType` enum gains 6 new discriminants (gated by format_version at parse time). A `SegmentFooter` enum wraps `FooterV1`/`FooterV2` and presents uniform accessor methods. The reader dispatches on the file header version to parse the correct footer type. The writer emits v2 framing (FSST symbol table region + FooterV2) when requested.

**Tech Stack:** Rust 2021, postcard (footer serialization), twox-hash (checksum), arrow (column arrays)

**Design doc:** `docs/design/storage/segment-format-v2.md`

---

## Checkpoint 1: Encoding discriminants + layout types (shared-file changes)

All changes in this checkpoint are additive — no existing behavior changes. This is the merge-first shared-file checkpoint that TASK-413–TASK-418 and TASK-450 depend on.

**Files:**
- Modify: `crates/bqlite-storage/src/encoding/mod.rs` — add 6 new `EncodingType` variants, add `from_discriminant_versioned` method
- Modify: `crates/bqlite-storage/src/segment/layout.rs` — add `SEGMENT_FORMAT_VERSION_V1`/`V2` constants, `FsstSymbolTableRef`, `FooterV2`, `SegmentFooter` enum with accessor methods

### Changes to `encoding/mod.rs`

- [ ] **Step 1: Add 6 new EncodingType variants**

Add after `Constant = 6`:

```rust
// v2 (new in Wave 4, per segment-format-v2.md §4)
DoubleDelta = 3,
Rle         = 5,
Fsst        = 7,
For         = 8,
PFor        = 9,
Alp         = 10,
```

The existing `from_discriminant` method matches on `byte: u8` literals (not enum variants), so it continues to reject 3/5/7/8/9/10 as unknown — correct v1 behavior. No match-exhaustiveness issue.

- [ ] **Step 2: Add `from_discriminant_versioned` method**

```rust
/// Parse a byte into an [`EncodingType`], gated by format version.
///
/// v1 segments accept only `{0, 1, 2, 4, 6}`. v2 segments additionally
/// accept `{3, 5, 7, 8, 9, 10}`. Discriminant 11 (FreqEncoding) is
/// reserved but rejected in both versions per segment-format-v2.md §4.
pub fn from_discriminant_versioned(byte: u8, format_version: u16) -> Result<Self> {
    match byte {
        // v1 set — always accepted
        0 => Ok(Self::Plain),
        1 => Ok(Self::Dictionary),
        2 => Ok(Self::Delta),
        4 => Ok(Self::BitPacking),
        6 => Ok(Self::Constant),
        // v2 set — accepted only when format_version >= 2
        3 if format_version >= 2 => Ok(Self::DoubleDelta),
        5 if format_version >= 2 => Ok(Self::Rle),
        7 if format_version >= 2 => Ok(Self::Fsst),
        8 if format_version >= 2 => Ok(Self::For),
        9 if format_version >= 2 => Ok(Self::PFor),
        10 if format_version >= 2 => Ok(Self::Alp),
        other => Err(BqliteError::Execution(format!(
            "unknown encoding discriminant {other} for format version {format_version} \
             — segment written by an incompatible version"
        ))),
    }
}
```

- [ ] **Step 3: Update `encoding_type_round_trip_discriminants` test**

Add the 6 new variants to the round-trip loop:

```rust
EncodingType::DoubleDelta,
EncodingType::Rle,
EncodingType::Fsst,
EncodingType::For,
EncodingType::PFor,
EncodingType::Alp,
```

- [ ] **Step 4: Add test for `from_discriminant_versioned`**

```rust
#[test]
fn from_discriminant_versioned_v1_rejects_v2_encodings() {
    for byte in [3u8, 5, 7, 8, 9, 10] {
        assert!(EncodingType::from_discriminant_versioned(byte, 1).is_err());
    }
}

#[test]
fn from_discriminant_versioned_v2_accepts_all_valid() {
    let v2_pairs = [
        (0, EncodingType::Plain),
        (1, EncodingType::Dictionary),
        (2, EncodingType::Delta),
        (3, EncodingType::DoubleDelta),
        (4, EncodingType::BitPacking),
        (5, EncodingType::Rle),
        (6, EncodingType::Constant),
        (7, EncodingType::Fsst),
        (8, EncodingType::For),
        (9, EncodingType::PFor),
        (10, EncodingType::Alp),
    ];
    for (byte, expected) in v2_pairs {
        assert_eq!(
            EncodingType::from_discriminant_versioned(byte, 2).unwrap(),
            expected,
        );
    }
}

#[test]
fn from_discriminant_versioned_rejects_reserved_11() {
    assert!(EncodingType::from_discriminant_versioned(11, 1).is_err());
    assert!(EncodingType::from_discriminant_versioned(11, 2).is_err());
}
```

- [ ] **Step 5: Add v2 discriminant spec-pinning test**

```rust
#[test]
fn encoding_type_discriminants_match_segment_format_v2_spec() {
    assert_eq!(EncodingType::DoubleDelta.discriminant(), 3);
    assert_eq!(EncodingType::Rle.discriminant(), 5);
    assert_eq!(EncodingType::Fsst.discriminant(), 7);
    assert_eq!(EncodingType::For.discriminant(), 8);
    assert_eq!(EncodingType::PFor.discriminant(), 9);
    assert_eq!(EncodingType::Alp.discriminant(), 10);
}
```

- [ ] **Step 6: Update `dispatch_decode` and `parse_encoding_params_len` in reader.rs for exhaustiveness**

The `dispatch_decode` and `parse_encoding_params_len` functions in `reader.rs` match on `EncodingType` variants. Adding new variants makes these matches non-exhaustive. Add stub arms that return errors indicating the encoding is not yet implemented:

In `dispatch_decode`:
```rust
EncodingType::DoubleDelta
| EncodingType::Rle
| EncodingType::Fsst
| EncodingType::For
| EncodingType::PFor
| EncodingType::Alp => Err(BqliteError::Execution(format!(
    "v2 encoding {encoding:?} decode not yet implemented (TASK-413–TASK-418)"
))),
```

In `parse_encoding_params_len`:
```rust
EncodingType::DoubleDelta => Ok(17),   // base_value(8) + first_delta(8) + dd_bit_width(1)
EncodingType::Rle => Ok(4),           // run_count(4)
EncodingType::Fsst => Ok(4),          // symbol_table_id(4)
EncodingType::For => Ok(6),           // block_size(2) + block_count(4)
EncodingType::PFor => Ok(6),          // block_size(2) + block_count(4)
EncodingType::Alp => Ok(19),          // exponent(1) + factor(8) + patch_count(4) + for_block_size(2) + for_block_count(4)
```

### Changes to `layout.rs`

- [ ] **Step 7: Add version constants**

```rust
/// Alias for the v1 format version, for clarity when both versions
/// are in scope. The original [`SEGMENT_FORMAT_VERSION`] name is
/// retained as an alias so existing code that refers to it (which
/// always means "the v1 constant") is unambiguous.
pub const SEGMENT_FORMAT_VERSION_V1: u16 = 1;

/// On-disk format version for v2 segments. v2 extends v1 with six
/// new encoding discriminants and the FSST symbol tables region.
/// See `docs/design/storage/segment-format-v2.md` §3.
pub const SEGMENT_FORMAT_VERSION_V2: u16 = 2;
```

- [ ] **Step 8: Add FsstSymbolTableRef struct**

```rust
/// Reference to a segment-level FSST symbol table (segment-format-v2.md §6.4).
///
/// One entry per FSST-encoded column. Each entry locates the symbol
/// table bytes inside the FSST symbol tables region (between the
/// segment dictionaries region and the footer body).
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

- [ ] **Step 9: Add FooterV2 struct**

```rust
/// The v2 segment footer body — a strict superset of [`FooterV1`]
/// with one new field (`fsst_symbol_tables`). See
/// `segment-format-v2.md` §7.
///
/// FooterV2 is a separate struct rather than an extension of FooterV1
/// because FooterV1's postcard serialization is the v1 on-disk
/// contract — adding a field would change serialized bytes and break
/// v1 reads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FooterV2 {
    /// Must equal [`SEGMENT_FORMAT_VERSION_V2`] (2).
    pub format_version: u16,
    pub schema: TableSchema,
    pub schema_version: u32,
    pub row_count: u64,
    pub row_group_count: u32,
    pub row_group_size_hint: u32,
    pub creation_timestamp_ns: i64,
    pub seq_id_range: (u64, u64),
    pub batch_id: u64,
    pub compaction_level: u8,
    pub dictionaries: Vec<SegmentDictRef>,
    /// Segment-level FSST symbol tables — NEW in v2.
    /// One entry per FSST-encoded column. Empty when no column
    /// uses FSST encoding.
    pub fsst_symbol_tables: Vec<FsstSymbolTableRef>,
    pub row_groups: Vec<RowGroupIndex>,
}
```

- [ ] **Step 10: Add SegmentFooter enum with accessor methods**

```rust
/// Version-dispatched segment footer. The reader parses the file
/// header version and deserializes the footer body as the
/// corresponding variant. Accessor methods delegate to the active
/// variant so callers do not need to match.
///
/// See `segment-format-v2.md` §8.4.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SegmentFooter {
    V1(FooterV1),
    V2(FooterV2),
}

impl SegmentFooter {
    pub fn format_version(&self) -> u16 { ... }
    pub fn schema(&self) -> &TableSchema { ... }
    pub fn schema_version(&self) -> u32 { ... }
    pub fn row_count(&self) -> u64 { ... }
    pub fn row_group_count(&self) -> u32 { ... }
    pub fn row_group_size_hint(&self) -> u32 { ... }
    pub fn creation_timestamp_ns(&self) -> i64 { ... }
    pub fn seq_id_range(&self) -> (u64, u64) { ... }
    pub fn batch_id(&self) -> u64 { ... }
    pub fn compaction_level(&self) -> u8 { ... }
    pub fn dictionaries(&self) -> &[SegmentDictRef] { ... }
    pub fn row_groups(&self) -> &[RowGroupIndex] { ... }

    /// FSST symbol table refs. Returns empty slice for V1.
    pub fn fsst_symbol_tables(&self) -> &[FsstSymbolTableRef] { ... }
}
```

- [ ] **Step 11: Add layout tests**

Test FooterV2 postcard round-trip, SegmentFooter accessor delegation, constants match spec.

- [ ] **Step 12: Run local-ci, code review, commit, merge to main**

---

## Checkpoint 2: Reader v2 dispatch

The reader gains version dispatch: v2 segments are opened and validated alongside v1. No new encoding decode logic — v2 encodings are rejected at decode time with "not yet implemented" until TASK-413+.

**Files:**
- Modify: `crates/bqlite-storage/src/segment/reader.rs` — version-dispatch open path, SegmentFooter storage, v2 validation, FSST symbol table loading
- Modify: `crates/bqlite-storage/src/segment.rs` — update module doc

### Changes to `reader.rs`

- [ ] **Step 1: Change `SegmentFileReader` internal footer type**

Replace `footer: Arc<FooterV1>` with `footer: Arc<SegmentFooter>` in both `SegmentFileReader` and `SegmentFileScan`. Update the `footer()` accessor to return `&SegmentFooter`.

- [ ] **Step 2: Update `validate_header` to accept v1 and v2**

Change the version check from `version != SEGMENT_FORMAT_VERSION` to `version != SEGMENT_FORMAT_VERSION_V1 && version != SEGMENT_FORMAT_VERSION_V2`, and return the parsed version.

Change signature: `fn validate_header(bytes: &[u8]) -> Result<u16>` (returns format_version).

- [ ] **Step 3: Update `from_bytes_shared` for version dispatch**

After parsing trailer and validating framing:
1. Read `format_version` from file header (already validated).
2. Dispatch on version:
   - v1: deserialize as `FooterV1`, wrap in `SegmentFooter::V1`
   - v2: deserialize as `FooterV2`, wrap in `SegmentFooter::V2`
   - other: corruption error (already caught by validate_header)
3. Call versioned footer validation.
4. Load dictionaries from footer (common path via SegmentFooter accessors).
5. For v2: validate FSST symbol table refs (rule 13 from §11).

- [ ] **Step 4: Update `validate_footer` for SegmentFooter**

Split into version-specific validation or use accessor methods. Key difference: v2 encoding discriminant rule accepts `{0,1,2,3,4,5,6,7,8,9,10}` instead of `{0,1,2,4,6}`. Also validates FSST symbol table refs (rule 13, rule 14, rule 15).

- [ ] **Step 5: Update `decode_column_chunk` and helpers**

Pass `format_version` through to `EncodingType::from_discriminant_versioned` instead of `from_discriminant`.

- [ ] **Step 6: Update all internal `self.footer.field` accesses to use accessor methods**

Replace direct field access like `self.footer.row_groups` with `self.footer.row_groups()`, `self.footer.schema` with `self.footer.schema()`, etc. throughout the reader and scan implementations.

- [ ] **Step 7: Add v2 reader tests**

Test: open a hand-crafted v2 segment (using v1 encodings in v2 framing), mixed version tests, v1 reader rejects v2 segments, v2 reader accepts v1 segments.

- [ ] **Step 8: Update `segment.rs` module doc**

Add mention of v2 support.

- [ ] **Step 9: Run local-ci, code review, commit, merge to main**

---

## Checkpoint 3: Writer v2 support + round-trip tests

The writer gains the ability to emit v2 segments with the FSST symbol table region and FooterV2. Round-trip tests prove v2 write → v2 read works.

**Files:**
- Modify: `crates/bqlite-storage/src/segment/writer.rs` — v2 write request fields, v2 encode path

### Changes to `writer.rs`

- [ ] **Step 1: Add PreparedFsstSymbolTable type**

```rust
/// Pre-built FSST symbol table bytes for one column, ready for the
/// writer to place in the FSST symbol tables region.
pub struct PreparedFsstSymbolTable {
    pub column_ordinal: u32,
    pub bytes: Vec<u8>,    // Serialized symbol table (§6.2 format)
    pub symbol_count: u16,
}
```

- [ ] **Step 2: Add v2 fields to SegmentWriteRequest**

Add `format_version: u16` (default 1) and `fsst_symbol_tables: Vec<PreparedFsstSymbolTable>` to `SegmentWriteRequest`.

- [ ] **Step 3: Extend `encode_segment` for v2**

After the dictionaries region and before the footer body, write the FSST symbol tables region (if format_version == 2 or if fsst_symbol_tables is non-empty). Build `FsstSymbolTableRef` entries for the footer. Construct `FooterV2` when format_version == 2, otherwise `FooterV1`. Write the correct version in the file header.

- [ ] **Step 4: Update `validate_request` for v2**

Validate that `format_version` is 1 or 2. When v1, reject any v2 encoding discriminants in column chunks. When v2, validate FSST symbol table column_ordinals.

- [ ] **Step 5: Add round-trip tests**

Test: write v2 segment with v1 encodings → read back with v2 reader → verify footer fields match. Test: write v2 segment with empty FSST tables → verify FSST region is zero-length. Test: write v2 segment with FSST symbol table data → verify FsstSymbolTableRef offsets in footer are correct.

- [ ] **Step 6: Add mixed-version round-trip test**

Write both a v1 and v2 segment, open both with the v2-aware reader, verify both produce correct scan results.

- [ ] **Step 7: Run local-ci, code review, commit, merge to main**

---

## File Change Summary

| File | Checkpoint | Change Type |
|------|-----------|-------------|
| `crates/bqlite-storage/src/encoding/mod.rs` | 1 | Modify: 6 new enum variants, new method, stub arms in reader |
| `crates/bqlite-storage/src/segment/layout.rs` | 1 | Modify: constants, FsstSymbolTableRef, FooterV2, SegmentFooter |
| `crates/bqlite-storage/src/segment/reader.rs` | 1, 2 | Modify: stub arms (CP1), version dispatch + SegmentFooter (CP2) |
| `crates/bqlite-storage/src/segment/writer.rs` | 3 | Modify: v2 write path, PreparedFsstSymbolTable |
| `crates/bqlite-storage/src/segment.rs` | 2 | Modify: module doc |
