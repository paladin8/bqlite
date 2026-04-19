# TASK-419 — Advanced encoding selector integration + reader/writer compatibility

**Goal:** Register the surviving Wave 4 codecs (RLE, DoubleDelta, FOR, PFOR, FSST, ALP) with the selector, wire FSST's segment-level symbol tables through the writer/reader paths, and demonstrate end-to-end compatibility across v1 and v2 segments.

**Architecture:**

1. The selector (`bqlite-storage/src/encoding/selector.rs`) gains Rle, ForEncoding, Pfor, Alp, Fsst as candidates alongside Plain/Dictionary/Delta/DoubleDelta/BitPacking/Constant. Each codec gets its selector guard from `docs/design/storage/advanced-encodings.md`. The selector returns the chosen encoding's `SelectedEncoding`; FSST chunks continue carrying their self-contained symbol-table blob in `params` (the writer hoists it).
2. The writer (`bqlite-storage/src/writer.rs`) auto-detects `format_version` based on the encodings it produced — emit v2 whenever *any* column chunk used a non-v1 codec (Rle, DoubleDelta, FOR, PFOR, FSST, ALP). When an FSST chunk appears, hoist its symbol-table bytes into `PreparedFsstSymbolTable`, assign a `symbol_table_id`, and rewrite the chunk's `params` to the 4-byte `symbol_table_id: u32 LE` v2 form.
3. The reader (`bqlite-storage/src/segment/reader.rs`) grows a dedicated FSST match arm that looks up `symbol_table_id` in the segment-level `fsst_symbol_tables`, grabs the symbol-table bytes out of the segment buffer, and calls `fsst::decode_from_params` with the reconstructed `(params, payload)` pair.
4. End-to-end integration test drives the ingest path against a table designed to exercise each new codec + FSST's segment-level region, opens the written segment via `SegmentFileReader`, and round-trips the rows back. Mixed-version test writes both a v1 and a v2 segment into the same directory tree and reads both.

**Checkpoint layout** (each checkpoint merges to main before the next starts; each must pass `scripts/local-ci.sh` and a subagent code review):

---

## Checkpoint 1 — Register RLE / FOR / PFOR / ALP in the selector + writer format-version auto-select

**Scope:** Selector wires Rle, ForEncoding, Pfor, Alp (not FSST — that needs symbol-table hoist in CP2). Writer's `prepare_segment` derives `format_version` from the winning encodings across the segment. No on-disk schema churn. Unit tests cover both.

**Files:**
- Modify: `crates/bqlite-storage/src/encoding/selector.rs` (add candidates + guards, extend `encode_with` already covers these codecs, update module docs and the `Note: Rle is intentionally omitted…` comment)
- Modify: `crates/bqlite-storage/src/writer.rs` (derive `format_version` from the encodings produced in `prepare_segment`)

**Selector guards to apply (per `advanced-encodings.md`):**
- RLE: only propose when `estimated_run_count * 2 < non_null_count` (≡ avg run length > 2). Guard lives in a small helper `rle_run_length_worth_it(array)` — operates on the dense array values to estimate the number of runs.
- FOR: only propose when `sum(per-block bit widths) < 0.9 * block_count * global_bit_width`. Guard is `for_block_framing_worth_it(values: &[i64])`, where the global width is `zigzag(max-min).bit_width()`, matching the existing `BitPacking` internals.
- PFOR: only propose when 1% < outlier fraction < 10% for the best-choice main width. Guard is `pfor_outlier_fraction_in_range(values: &[i64])` — picks the narrowest main width where outliers account for <10% and >1%.
- ALP: only propose when ≥70% of a sample decompose cleanly. Reuse `Alp::estimate_size` which already fails when the codec can't decompose; a new `alp_sample_decomposes_cleanly(values: &[f64])` does a dry-run through the codec's decomposition at the best candidate exponent and returns `clean_count >= ceil(0.7 * sample_size)`.

**Guards that already exist elsewhere:**
- DoubleDelta is already in `candidates` with its own `estimate_size` carrying the `< 0.5 * delta_bit_width` semantics inside the codec (existing test `double_delta_wins_two_element_input` covers it).
- Dictionary already has its cardinality guard inside `Dictionary::estimate_size`.

**Guard behavior in selector:** Each guard short-circuits by *skipping* the candidate when the guard fails. When the guard passes, `estimate_size` decides the winner. Decode cost tiebreaker already in place.

**Writer format_version derivation:**
Add a small helper:
```rust
fn derived_format_version(groups: &[PreparedRowGroup]) -> u16 {
    for rg in groups {
        for c in &rg.columns {
            if requires_v2(c.encoded.encoding) {
                return SEGMENT_FORMAT_VERSION_V2;
            }
        }
    }
    SEGMENT_FORMAT_VERSION
}

fn requires_v2(e: EncodingType) -> bool {
    matches!(e, EncodingType::Rle
        | EncodingType::DoubleDelta
        | EncodingType::For
        | EncodingType::PFor
        | EncodingType::Fsst
        | EncodingType::Alp)
}
```
Call `derived_format_version(&prepared_groups)` in `prepare_segment` and assign it to `request.format_version`. This is the reconciliation with `segment-format-v2.md` §9.1 — v2 output follows v2 codecs. Leave the writer's existing tolerance of Rle/DoubleDelta in v1 (the segment `from_discriminant_versioned` still accepts them there) so old segments keep loading; forward-written segments auto-upgrade to v2 when the selector picks a v2 codec.

- [ ] **CP1.1 — Write helper + selector tests first (TDD)**

Add unit tests in `selector.rs`:
```rust
#[test]
fn rle_wins_on_long_runs() {
    // 100 rows, 2 runs → avg run length 50 > 2, RLE wins over Dictionary.
    let mut v = Vec::with_capacity(100);
    v.extend(std::iter::repeat_n("alpha", 50));
    v.extend(std::iter::repeat_n("beta", 50));
    let array: ArrayRef = Arc::new(StringViewArray::from(v));
    let chosen = select_encoding_type(array.as_ref(), &BqlType::String).unwrap();
    assert_eq!(chosen, EncodingType::Rle);
}

#[test]
fn rle_skipped_on_alternating_bool() {
    let values: Vec<bool> = (0..64).map(|i| i % 2 == 0).collect();
    let array: ArrayRef = Arc::new(BooleanArray::from(values));
    let chosen = select_encoding_type(array.as_ref(), &BqlType::Bool).unwrap();
    assert_ne!(chosen, EncodingType::Rle);
}

#[test]
fn for_wins_when_local_ranges_are_tight() {
    // Two clusters of 128 values each: [100..110] and [4500..4510].
    // Global width ~13 bits, per-block width ~4 bits → FOR wins.
    let mut vals = Vec::with_capacity(256);
    for i in 0..128 { vals.push(100 + (i % 10) as i64); }
    for i in 0..128 { vals.push(4500 + (i % 10) as i64); }
    let array: ArrayRef = Arc::new(Int64Array::from(vals));
    let chosen = select_encoding_type(array.as_ref(), &BqlType::Int).unwrap();
    assert_eq!(chosen, EncodingType::For);
}

#[test]
fn alp_wins_on_round_floats() {
    let vals: Vec<f64> = (0..256).map(|i| (i as f64) * 0.01).collect();
    let array: ArrayRef = Arc::new(Float64Array::from(vals));
    let chosen = select_encoding_type(array.as_ref(), &BqlType::Float).unwrap();
    assert_eq!(chosen, EncodingType::Alp);
}

#[test]
fn alp_skipped_on_random_floats() {
    let vals: Vec<f64> = (0..256).map(|i| (i as f64).sqrt() * std::f64::consts::PI).collect();
    let array: ArrayRef = Arc::new(Float64Array::from(vals));
    let chosen = select_encoding_type(array.as_ref(), &BqlType::Float).unwrap();
    assert_ne!(chosen, EncodingType::Alp);
}
```

Run `cargo test -p bqlite-storage selector` — expect failures because candidates aren't registered yet.

- [ ] **CP1.2 — Implement guards + add candidates to `pick_encoding`**

Change the candidate list:
```rust
let candidates: &[&dyn Encoding] = &[
    &Plain, &Dictionary, &Delta, &DoubleDelta, &BitPacking,
    // v2 additions with inline guards handled via per-candidate skip below
    &Rle, &ForEncoding, &Pfor, &Alp,
];
```
Replace the body of the loop to also skip each candidate when its specific guard fails:
```rust
if let Some(skip) = selector_guard_skip(enc.encoding_type(), array, ty) {
    if skip { continue; }
}
```
Where `selector_guard_skip` returns `Some(true)` to skip, `Some(false)` to keep, `None` for "no guard". The guards live in small `pub(super) fn` helpers at the bottom of the file, each taking the dense array and returning bool. They do not allocate beyond the single pass they need.

Run `cargo test -p bqlite-storage selector` — expect all to pass.

- [ ] **CP1.3 — Writer auto-selects format_version**

Add `derived_format_version` helper next to `prepare_segment`. Change
```rust
format_version: 1,
```
to
```rust
format_version: derived_format_version(&prepared_groups),
```

Add a writer test:
```rust
#[test]
fn writer_emits_v2_when_selector_picks_rle() {
    // feed a bucket that encourages RLE → assert the resulting segment file
    // header carries version == 2.
    ...
}
```

Run `cargo test -p bqlite-storage` — expect pass.

- [ ] **CP1.4 — Reconcile docs**

Update `selector.rs` module docs: strike the `Note: Rle is intentionally omitted here.` paragraph, replace with a short paragraph describing the v2 candidate set and pointing to `advanced-encodings.md` §§3.7, 5.7, 6.7, 8.7.

Update `crates/bqlite-storage/src/writer.rs` header comment (around "Encoding selection + LZ4 wrapping") to mention that v2 format is auto-selected when any v2-only codec is used.

- [ ] **CP1.5 — Local CI + subagent review + merge**

Run `scripts/local-ci.sh`. Spawn a subagent code-review pass. Commit (`TASK-419: ...`) and fast-forward merge.

---

## Checkpoint 2 — FSST symbol-table hoist (writer + reader) and selector registration

**Scope:** Selector gains FSST. Writer hoists FSST symbol tables to the segment-level FSST region and rewrites chunk params. Reader dispatches FSST decode using those segment-level symbol tables. End-to-end round trip for an FSST-heavy column.

**Files:**
- Modify: `crates/bqlite-storage/src/encoding/selector.rs` (FSST candidate + guard)
- Modify: `crates/bqlite-storage/src/writer.rs` (`hoist_fsst_chunk` analogous to `hoist_dictionary_chunk`, wire into `build_column_chunk`)
- Modify: `crates/bqlite-storage/src/segment/reader.rs` (FSST match arm that looks up `symbol_table_id`, updated `parse_encoding_params_len` already returns `4`; wire dispatch)
- Test: a new integration test `tests/tests/prop_segment_v2_roundtrip.rs` (if it fits scope) or a focused unit test in `writer.rs` that writes a segment containing an FSST chunk, reads it back via `SegmentFileReader`, and compares the decoded column to the input.

**On-disk FSST hoist contract:**
- Trait-level FSST chunk carries `params = symbol_table_bytes (FSST_SYMBOL_TABLE_SIZE)`.
- v2 on-disk FSST chunk carries `params = symbol_table_id: u32 LE` (4 bytes). The segment-level symbol tables region holds the `symbol_table_bytes` verbatim (same byte layout the crate reads), referenced by `FsstSymbolTableRef { column_ordinal, byte_offset, byte_length, symbol_count }`.
- `symbol_count` in the ref can be computed by reading the crate's serialized blob (`symbol_count = (header & 0xFF) as u16` when the encoder_switch bit is set, 0 otherwise — mirroring `decoded_size_from_symbol_table`). A helper `fsst_symbol_count_from_bytes(&[u8]) -> u16` lives next to the hoist so the writer doesn't care about the blob's internal shape.

**FSST selector guard:** per §7.7 — only propose FSST for `String` columns when `cardinality / row_count >= 0.3` AND the FSST-encoded payload is smaller than Plain. The second clause requires encoding speculatively — keep the guard cheap: use `Fsst.estimate_size` (already a 2× upper bound) as a pre-filter; if the upper bound still loses to Plain, skip. This matches `advanced-encodings.md` §10 step 4 "Guard: FSST payload < Plain payload, else skip".

**FSST reader flow:**
```rust
EncodingType::Fsst => {
    if on_disk_params.len() != 4 {
        return Err(BqliteError::Corruption(...));
    }
    let symbol_table_id = u32::from_le_bytes(on_disk_params.try_into().unwrap()) as usize;
    let symbol_tables = footer.fsst_symbol_tables();  // already exposed by SegmentFooter
    let st = symbol_tables.get(symbol_table_id).ok_or_else(|| Corruption(...))?;
    if st.column_ordinal != meta.column_ordinal {
        return Err(Corruption(...));
    }
    let start = st.byte_offset as usize;
    let end = start + st.byte_length as usize;
    let symbol_bytes = &segment_bytes[start..end];
    crate::encoding::fsst::decode_from_params(
        symbol_bytes,
        uncompressed_payload.as_ref(),
        meta.row_count as usize,
        &write_time_col.bql_type,
    )?
}
```
(Need to thread `segment_bytes: &[u8]` into the decode site. It's already available via the reader's `bytes: Arc<[u8]>` field — the existing `decode_column_chunk` function takes it implicitly via its callers. Inspect line 800-940 context to confirm before modifying.)

**Checkpoint steps:**

- [ ] **CP2.1 — Write failing writer/reader round-trip test**

Add a unit test under `tests/tests/prop_segment_v2_roundtrip.rs` (or an in-crate `#[test]` in `writer.rs`) that:
1. Builds a bucket of ~200 events where one String column is a mix of distinct URLs (high cardinality), forcing FSST.
2. Ingests them via `SegmentWriter::write_bucket` or the equivalent low-level `prepare_segment` + `write_segment` flow.
3. Opens the segment via `SegmentFileReader::open`.
4. Reads all row groups, compares columns row-by-row to the input.

Expected: this test fails today because FSST is not selected, or because the reader errors on FSST.

- [ ] **CP2.2 — Extend selector with FSST**

Add `&Fsst` to candidates. Add `fsst_worth_it(array, ty)` guard that checks cardinality ratio + Plain comparison.

- [ ] **CP2.3 — Implement `hoist_fsst_chunk`**

Mirrors `hoist_dictionary_chunk`. Takes `&EncodedChunk`, column ordinal, `&mut Vec<PreparedFsstSymbolTable>`. Returns a new `EncodedChunk` with `params` replaced by 4-byte `symbol_table_id`. Call it from `build_column_chunk` when `chunk.encoding == EncodingType::Fsst`.

Plumb a `&mut Vec<PreparedFsstSymbolTable>` through `prepare_segment` → `build_column_chunk` alongside the existing `segment_dicts`, and drop it into the `SegmentWriteRequest.fsst_symbol_tables` field.

- [ ] **CP2.4 — Reader FSST dispatch**

In `decode_column_chunk`, split FSST out of the bulk-dispatch match arm. Grab the segment bytes + footer's `fsst_symbol_tables` slice, look up the symbol table, call `fsst::decode_from_params`.

Remove the stale TODO on line 919 and the TASK-416 placeholder arm in `dispatch_decode` — the decode now goes through the new FSST arm in the caller, so `dispatch_decode` can simply drop `EncodingType::Fsst` (or keep an `unreachable!` panic with a clear message).

- [ ] **CP2.5 — Run tests, local CI, subagent review, merge**

Run `cargo test -p bqlite-storage`, then `scripts/local-ci.sh`, then subagent review.

---

## Checkpoint 3 — Mixed-version end-to-end compatibility test

**Scope:** Integration test written at the `bqlite-storage::database` level (or `tests/tests/` crate-wide) that writes a v1 segment (forced by a workload that only triggers v1 codecs) and a v2 segment (forced by a workload that triggers v2 codecs) into the same database, then reads both back via a single open. Covers the read-side version dispatch end-to-end.

**Files:**
- Test: `crates/bqlite-storage/tests/` — add a `#[test]` in an existing file or a new focused test file.

**Checkpoint steps:**

- [ ] **CP3.1 — Write mixed-version integration test**

```rust
#[test]
fn mixed_v1_and_v2_segments_readable() {
    // 1. Open a temp Database.
    // 2. Ingest a small batch whose only codecs fall inside the v1 set
    //    (low-cardinality strings, small ints). Assert the written segment's
    //    format_version is 1.
    // 3. Ingest a second batch whose columns force a v2 codec
    //    (e.g. high-cardinality URL column → FSST, or many long runs → RLE).
    //    Assert the written segment's format_version is 2.
    // 4. Open the database fresh and scan both tables. Every row must
    //    round-trip identically.
}
```

- [ ] **CP3.2 — Run tests + CI + review + merge**

`cargo test -p bqlite-storage`, `scripts/local-ci.sh`, subagent review, fast-forward merge.

---

## Completion

After CP3 merges, move `tasks/active/TASK-419.lock` → `tasks/completed/TASK-419.done`, add `completed_at`, commit `TASK-419: completed`, push.

---

## Risk / decisions

1. **v1 tolerance of Rle/DoubleDelta discriminants is preserved.** We do not retighten `from_discriminant_versioned` because earlier tasks deliberately accepted them there (see comment in `encoding/mod.rs:156-165`). New writes produce v2 when those codecs are selected, so we drift toward the spec naturally without breaking existing artefacts. Noted in CP1 commit message as a deliberate non-change.

2. **FSST estimate_size is a 2× upper bound.** The selector uses it for the Plain-vs-FSST guard, which is conservative: real FSST will usually compress 3–5×, so the guard may let FSST through even when the pre-filter says "maybe". That's fine — the selector then picks by `estimate_size`, which will choose Plain if FSST's speculative payload is larger. If this turns out to be a pessimization, future work can add a sampling encode; `Encoding::estimate_size` stays an upper bound by contract.

3. **No property tests added for the selector itself.** The selector is deterministic over a small decision tree and each guard has a focused unit test. A proptest over all column shapes is a good follow-up but not part of this task's surface.
