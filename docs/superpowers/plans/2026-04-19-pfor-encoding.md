# TASK-450: PFOR Encoding Implementation Plan

> **For agentic workers:** implement this task-by-task. Each checkpoint
> must pass `scripts/local-ci.sh`, be reviewed by a code-review
> subagent, and be fast-forward merged to `main` before the next
> checkpoint begins.

**Goal:** Implement the Patched Frame-of-Reference (PFOR) integer
encoding on top of the FOR scaffolding from TASK-415, following the
byte layout pinned in `docs/design/storage/segment-format-v2.md` §5.5.

**Architecture:** PFOR reuses FOR's 128-value block frame plus
`BitPacker4x` fast-path. Each block adds an outlier ("patch") list
at a narrower `main_width` than FOR would pick, storing the outlier
positions as `u16 LE` indices and the outlier values as full-width
`i64 LE`. The selector picks `main_width` by minimizing total block
bytes over every candidate width.

**Tech Stack:** Rust 2021, `bitpacking = 0.9.3` (reused via
`BitPacker4x`), `arrow`, `proptest`, `criterion`. No new crate
dependencies — the `fastpfor` suggestion in `TASKS.md` conflicts with
the exact byte layout already pinned in §5.5, so we follow the design
doc (see §CP1 decision log).

---

## Scope and constraints

- **Byte format is pinned.** `segment-format-v2.md` §5.5 defines the
  exact layout: 6-byte `encoding_params` (u16 block_size + u32
  block_count), then per-block `block_min: i64 LE`,
  `main_width: u8`, `patch_count: u16 LE`,
  `packed_main: padded-to-8-bytes`,
  `patch_indices: [u16 LE; patch_count]`,
  `patch_values: [i64 LE; patch_count]`. The payload length is exact
  (no trailing padding beyond each block's packed-main section).
- **PFOR is not wired into the selector in this task.** The task
  wires the `encode`/`decode` paths into the read/write dispatch
  (since the reader's `dispatch_decode` already has a `PFor` arm that
  returns "not implemented"). Selector-guard integration with the 1%
  / 10% outlier-fraction rule is **TASK-419** per
  `advanced-encodings.md` §5.7; we do not add `&Pfor` to the
  selector's candidate list here.
- **The `fastpfor` hint in TASKS.md is superseded.** The Rust
  `fastpfor` crate implements FastPFOR (Lemire 2015), a different
  block/exception scheme than the Zukowski 2006 PFOR the design doc
  fixes. The design doc wins per AGENTS.md task-note precedence
  rules. The decision is captured in the CP1 module doc-comment.

---

## File structure

**Create:**
- `crates/bqlite-storage/src/encoding/pfor.rs` — the `Pfor`
  `Encoding` impl, helper functions, and per-function unit tests.
- `tests/tests/prop_encoding_pfor.rs` — proptest file mirroring
  `prop_encoding_for.rs`: round-trip plus PFOR-specific invariants
  (params layout, per-block header, `estimate_size` exactness,
  patch_count is always reachable as a function of the block).
- `benches/wave4/pfor.rs` — criterion benches: outlier-heavy vs
  outlier-free int64, plus a comparison against BitPacking and FOR on
  the same data.

**Modify:**
- `crates/bqlite-storage/src/encoding/mod.rs` — add `pub mod pfor;`
  and `pub use pfor::Pfor;` alongside the existing encodings. Update
  the doc-comment listing to mention TASK-450 next to `For`.
- `crates/bqlite-storage/src/encoding/selector.rs` — replace the
  `EncodingType::Fsst | EncodingType::PFor => Err(...)` arm in
  `encode_with` with a split arm so `PFor => Pfor.encode(array)` and
  `Fsst => Err(...)` stays as is. Do **not** add `&Pfor` to the
  `candidates` list (TASK-419 owns that).
- `crates/bqlite-storage/src/segment/reader.rs` — same split on the
  `EncodingType::Fsst | EncodingType::PFor` arm inside
  `dispatch_decode`: `PFor => Pfor.decode_borrowed(chunk, ty)`.
- `benches/Cargo.toml` — add `[[bench]] name = "pfor" path =
  "wave4/pfor.rs" harness = false`.

---

## Algorithm

### Encode per block

1. Compute `block_min = min(block)`, then `offsets[i] = (v[i] as i128
   - block_min as i128) as u64`. Safe: offsets always fit in u64 by
   construction.
2. Compute `full_width = max(1, bits(max(offsets)))` — the FOR choice.
3. Choose `main_width` ∈ `1..=full_width` that minimizes block bytes:
   ```
   size(w) = BLOCK_HEADER_LEN                            // 11 bytes
           + padded_8(ceil_div(block_len * w, 8))        // packed_main
           + patch_count(w) * PATCH_ENTRY_LEN            // 10 bytes each
   ```
   where `padded_8(n) = n.div_ceil(8) * 8` and
   `patch_count(w) = count(i : offsets[i] >= (1 << w))` when
   `w < 64`, else `0`. The `ceil_div(block_len * w, 8)` matches FOR's
   `block_packed_byte_len` exactly; using integer-truncating division
   would under-estimate on short final blocks and make `estimate_size`
   non-exact. Tie-break: smallest `w` wins (fewer bits ⇒ faster
   bit-unpack). `main_width` is clamped to ≥ 1 because v2 §5.5
   requires `bit_width ∈ 1..=64` (same 1-bit floor as FOR §5.4), not
   because of any offset constraint — an all-identical block spends
   `padded_8(ceil_div(block_len, 8))` bytes on a zeroed packed_main
   and 0 patches; this is the spec-sanctioned cost.
4. Packed-main stream: for each position `i`, if `offsets[i] < (1 <<
   main_width)` write `offsets[i]`, else write 0. Decode reconstructs
   `block_min + 0 = block_min` there, then the patch scatter overwrites.
5. Patch list: for each `i` with `offsets[i] >= (1 << main_width)`,
   emit `i as u16` and the **actual i64 value** `v[i]` (literal
   copy, not an offset or a cast from a computed offset). Sorted by
   index ascending for deterministic output.

**Why "actual value" for patches:** §5.5 says "full-width outlier
values" and the decode pseudocode says "scatter `patch_values` at
`patch_indices` positions" into the output buffer directly (no
`block_min` add). Advanced-encodings.md §6.3 reinforces this:
"scatter patches at the indicated positions … random writes into the
output buffer at `patch_indices` positions" — describing a direct
write, not an add. Storing the value (not an offset) also avoids a
u64-to-i64 sign-reconstruction subtlety when outliers have wide
negative values.

### Decode per block

1. Parse header: `block_min: i64`, `main_width: u8` (must be 1..=64),
   `patch_count: u16` (must be ≤ block_len).
2. Unpack `packed_main` (same fast-path split as FOR: BitPacker4x when
   `block_len == 128 && main_width ≤ 32`, scalar otherwise).
3. Write `output[i] = ((block_min as i128) + (offset[i] as i128)) as
   i64` for every `i` in the block.
4. Parse `patch_indices` and `patch_values`. For `k` in `0..patch_count`:
   validate `patch_indices[k] < block_len` **and**
   `patch_indices[k] > patch_indices[k-1]` for `k >= 1` (strict
   monotonic ascending — matches the encode-side "sorted by index"
   guarantee and makes a corrupt segment with duplicate or
   out-of-order indices fail loudly instead of silently letting the
   last write win). Then `output[idx] = patch_values[k]`.

### Size estimation

`estimate_size(array)` is **exact**: it iterates blocks, runs
`select_block_frame_pfor` to get `(block_min, main_width,
patch_count)`, and sums
`BLOCK_HEADER_LEN + padded_8(ceil_div(block_len * main_width, 8)) +
patch_count * PATCH_ENTRY_LEN` over every block. This matches what
`encode` writes byte-for-byte. Exactness is required so that the
TASK-419 selector can compare PFOR against FOR / BitPacking with
byte-accurate rankings (the selector guard at 1-10% outlier fraction
is a heuristic that kicks in only when PFOR's exact estimate is
already below FOR's).

---

## Decomposition into checkpoints

Every file change is purely additive except for the two dispatch
arms in `selector.rs` and `reader.rs`, which are small and already
carry a "not implemented" stub. Risk of merge conflict is minimal, so
the implementation fits one checkpoint. Property tests, benches, and
design-doc reconciliation ride in the same checkpoint.

Single checkpoint:

**CP1** — core impl + unit tests + property tests + bench + dispatch
wiring + design-doc reconciliation.

---

## Checkpoint CP1 — PFOR encoding end-to-end

**Files:**

- Create: `crates/bqlite-storage/src/encoding/pfor.rs`
- Create: `tests/tests/prop_encoding_pfor.rs`
- Create: `benches/wave4/pfor.rs`
- Modify: `crates/bqlite-storage/src/encoding/mod.rs`
- Modify: `crates/bqlite-storage/src/encoding/selector.rs`
- Modify: `crates/bqlite-storage/src/segment/reader.rs`
- Modify: `benches/Cargo.toml`

- [ ] **Step 1: Implement `pfor.rs` (encode side)**

Write the module skeleton — `Pfor` zero-sized type, `Encoding` impl
with `encoding_type`, `applicable_to`, `estimate_size`, `encode`.
Match FOR's structure: 6-byte params (u16 block_size = 128, u32
block_count), per-block emit of
`block_min (i64 LE) | main_width (u8) | patch_count (u16 LE) |
packed_main (padded 8) | patch_indices (u16 × patch_count) |
patch_values (i64 × patch_count)`.

Key helpers:
- `const BLOCK_HEADER_LEN: usize = 11;` (i64 + u8 + u16)
- `const PATCH_ENTRY_LEN: usize = 10;` (u16 + i64)
- `select_block_frame_pfor(block: &[i64]) -> (i64, u8, u16, [u8 helper: patch-mask or patch_count])` — returns `(block_min, main_width, patch_count)` for a block.
- `block_packed_byte_len(block_len, main_width)` — identical to FOR's helper (padded to multiple of 8).
- `compute_payload_size(values)` — block-by-block sum matching §5.5.
- `pack_block_main(block, block_min, main_width)` — like FOR's
  `pack_block_offsets`, but writes `0` where `offsets[i] >= (1 <<
  main_width)`. Reuses `BitPacker4x` fast path for full blocks with
  `main_width ≤ 32`.
- `write_bits` / `read_bits` — duplicated from FOR (private here;
  keep the module self-contained, matching FOR's precedent in
  commit `b595412` and its comment "kept local so this module has
  no private-item dependency on `bitpacking.rs`").

Encode flow:
1. `require_dense(array, "Pfor")`.
2. `values_as_i64(array)` (same signature as FOR).
3. Build 6-byte params.
4. For each 128-value chunk, call `select_block_frame_pfor`, write
   the per-block header, append packed main, then append patches
   (sorted by index for deterministic output).
5. Return `EncodedChunk { encoding: PFor, params, payload, row_count }`.

- [ ] **Step 2: Implement `pfor.rs` (decode side)**

Add `decode`, `decode_borrowed` (both delegate to `decode_impl`).
`decode_impl` mirrors FOR's: validate encoding tag, params length,
block_size = 128, block_count = ceil(row_count / 128). For each
block:
- Read header; validate `main_width ∈ 1..=64` and
  `patch_count <= block_len`.
- Compute packed length; validate payload has enough bytes.
- Unpack main stream (fast path + scalar fallback identical to FOR).
- Build `output[i] = ((block_min as i128) + (offset[i] as i128)) as
  i64` for i in 0..block_len.
- Read `patch_count × u16 LE` indices; validate each `idx <
  block_len`. Read `patch_count × i64 LE` values.
- Scatter: `output[indices[k]] = values[k]`.

At the end, assert the payload was fully consumed (trailing bytes =
corruption, same pattern as FOR).

Return `Int64Array` / `TimestampNanosecondArray::with_timezone("UTC")`
depending on `BqlType`.

- [ ] **Step 3: Unit tests in `pfor.rs`**

Cover the FOR-parallel surface plus PFOR-specific edges:
- `applicable_to_covers_int_and_timestamp_only`
- `encoding_type_is_pfor_discriminant_nine`
- `params_are_exactly_six_bytes`, `params_block_size_is_128`
- `round_trip_empty`, `round_trip_single_value`,
  `round_trip_all_identical`, `round_trip_narrow_range_no_patches`,
  `round_trip_short_final_block`, `round_trip_multiple_blocks`,
  `round_trip_full_i64_range`, `round_trip_block_min_near_i64_min`
- `round_trip_with_outliers_below_one_percent` — 128 small values +
  one large value (patch_count = 1, main_width small)
- `round_trip_with_outliers_five_percent` — the §6.2 worked example
  (6-7 outliers per 128)
- `round_trip_all_patched` — degenerate case: every value is an
  outlier, main_width = 1, patch_count = block_len
- `round_trip_timestamp_utc`
- `estimate_size_matches_encoded_payload` (over a variety of inputs)
- `payload_is_padded_to_multiple_of_eight_per_block`
- `encode_rejects_nullable_input`
- `decode_rejects_wrong_encoding_discriminant`
- `decode_rejects_malformed_params_length`
- `decode_rejects_bad_block_size`
- `decode_rejects_main_width_zero`, `decode_rejects_main_width_over_64`
- `decode_rejects_patch_count_exceeding_block_len`
- `decode_rejects_patch_index_out_of_range`
- `decode_rejects_patch_indices_not_strictly_monotonic` — build a
  chunk whose patch_indices are `[3, 3]` and confirm decode errors
- `decode_rejects_payload_truncated_in_patches`
- `decode_rejects_trailing_bytes_after_last_block`
- `select_block_frame_pfor_beats_for_on_one_outlier` — direct
  assertion that for 127 values at 8-bit range + 1 outlier needing
  40 bits, PFOR chooses `main_width ≈ 8` with 1 patch, not
  `main_width = 40` with 0 patches
- `select_block_frame_pfor_converges_to_for_when_no_outliers` — when
  no outlier saves bytes, PFOR matches FOR's `main_width` with 0
  patches
- Tests for `write_bits`/`read_bits` round-trip analogous to FOR's.

- [ ] **Step 4: Wire `Pfor` into `encoding/mod.rs`**

Add module declaration and re-export:

```rust
pub mod pfor;
pub use pfor::Pfor;
```

Update the Wave 4 note in the header doc-comment to include TASK-450
alongside the existing TASK-415 (FOR) entry.

- [ ] **Step 5: Wire `Pfor` into `selector.rs::encode_with`**

Split the combined `Fsst | PFor` arm. New code:

```rust
EncodingType::PFor => Pfor.encode(array),
EncodingType::Fsst => Err(BqliteError::Execution(format!(
    "v2 encoding {encoding:?} encode not yet implemented"
))),
```

Update imports at the top of the file to include `Pfor`. The
`candidates` candidate list is **unchanged** — TASK-419 owns the
selector guard.

- [ ] **Step 6: Wire `Pfor` into `segment/reader.rs::dispatch_decode`**

Same split:

```rust
EncodingType::PFor => Pfor.decode_borrowed(chunk, ty),
EncodingType::Fsst => Err(BqliteError::Execution(format!(
    "v2 encoding {encoding:?} decode not yet implemented (TASK-416)"
))),
```

Update imports. Make sure `parse_encoding_params_len` already returns
`Ok(6)` for PFor (it does — line 1038) so no change there.

- [ ] **Step 7: Add `prop_encoding_pfor.rs`**

Mirror `prop_encoding_for.rs`. Property tests that are load-bearing:
- `pfor_round_trip_int(array in arb_int64_array())`
- `pfor_round_trip_timestamp(array in arb_timestamp_array())`
- `pfor_estimate_size_is_exact` — under PFOR, estimate is exact
  (needed so a future TASK-419 selector can rank by estimated size).
- `pfor_params_are_exactly_six_bytes`
- `pfor_params_block_size_is_128`
- `pfor_block_count_is_ceil_of_row_count_over_128`
- `pfor_row_count_preserved`
- `pfor_encoding_type_is_pfor` (discriminant = 9)
- `pfor_every_block_header_structure_valid` — walk the payload,
  confirm each block header decodes cleanly and the total bytes add
  up (`HEADER + padded_8(bl * w / 8) + pc*10` summed).

Example tests (proptest may not hit these reliably):
- `pfor_round_trip_empty`
- `pfor_round_trip_exactly_one_full_block`
- `pfor_round_trip_short_final_block_one_value`
- `pfor_round_trip_three_blocks`
- `pfor_round_trip_one_outlier_per_block` — 127 small + 1 big
- `pfor_round_trip_all_patched_degenerate`
- `pfor_round_trip_alternating_small_and_i64_max`
- `pfor_round_trip_timestamp_utc`
- `pfor_round_trip_i64_extrema`
- `pfor_round_trip_single_value`

- [ ] **Step 8: Add `benches/wave4/pfor.rs`**

One criterion bench group `encoding/pfor`:
- `pfor_encode_outlier_free_int64` — sequential values, FOR-equivalent
  output, measures the "no patches" encode path.
- `pfor_decode_outlier_free_int64` — same chunk, measures the "no
  patches" decode path.
- `pfor_encode_five_percent_outliers_int64` — the §6.2 worked
  example, 65536 rows, 95% in [0,255], 5% in [0, 2³¹].
- `pfor_decode_five_percent_outliers_int64` — decode on the above.
- `pfor_vs_for_payload_bytes_five_percent_outliers` — logs the
  payload-size comparison (as a `iter_with_setup` with a
  `Throughput::Bytes` marker) so `cargo bench` captures the size
  delta the design doc promises (~2.5× better than FOR).

Register in `benches/Cargo.toml` under the Wave 4 section:

```toml
[[bench]]
name = "pfor"
path = "wave4/pfor.rs"
harness = false
```

- [ ] **Step 9: Design-doc reconciliation**

Two small notes, in the same checkpoint:
- `docs/design/storage/advanced-encodings.md` §11.7 currently says
  "Builds on FOR (TASK-415). The patch list mechanism is the new
  code; the block structure is inherited." After implementation,
  append one line confirming the implementation lives at
  `crates/bqlite-storage/src/encoding/pfor.rs` and notes that
  selector integration is deferred to TASK-419 (per §5.7).
- Leave the TASKS.md entry unchanged — the "Use the fastpfor crate"
  line is part of the task description and the commit message
  records the decision. (If a reviewer objects, we can update
  TASKS.md in the same checkpoint; it's a single sentence.)

No change to `segment-format-v2.md` — its §5.5 layout is what we
implemented.

- [ ] **Step 10: Run `scripts/local-ci.sh` and fix any failures**

Must be green. Anything that fails indicates either a real bug or a
doc/dep-direction issue and must be addressed before review.

- [ ] **Step 11: Subagent code review**

Spawn a `superpowers:code-reviewer` subagent on the staged diff.
Feed it:
- The full plan (this file).
- `TASKS.md` line 1180 (the TASK-450 entry).
- `docs/design/storage/segment-format-v2.md` §5.5.
- `docs/design/storage/advanced-encodings.md` §6 and §11.7.
- `crates/bqlite-storage/src/encoding/for_encoding.rs` as the
  scaffolding reference.

Address every blocking issue before commit.

- [ ] **Step 12: Commit and merge**

```bash
git add -A
git commit -m "TASK-450: Implement PFOR encoding with patch list"
# local-ci already green — fast-forward merge
git checkout main
git pull origin main
git merge task/TASK-450 --ff-only
git push origin main
git checkout task/TASK-450
```

- [ ] **Step 13: Completion protocol**

```bash
git mv tasks/active/TASK-450.lock tasks/completed/TASK-450.done
# edit the .done file to add completed_at ISO-8601 UTC timestamp
git add tasks/completed/TASK-450.done
git commit -m "TASK-450: completed"
git push origin main
```

End the turn.

---

## Self-review checklist

- [x] Covers every §5.5 byte-layout field (block_min, main_width,
      patch_count, packed_main, patch_indices, patch_values) and the
      6-byte params header.
- [x] Covers §6 design-doc requirements: zero-patch degenerate case,
      all-patched degenerate case, selector-guard deferral to
      TASK-419, 1–10% outlier target band.
- [x] Covers the TASKS.md description's required output path
      (`crates/bqlite-storage/src/encoding/pfor.rs`).
- [x] Covers the "property tests for overflow and the worst-case
      all-patched fallback" requirement (Step 7 explicitly lists
      both properties).
- [x] Benchmarks in Step 8 cover the hot path (decode throughput) and
      the size ratio the design doc promises.
- [x] Selector integration is explicitly **not** in scope — avoids
      stepping on TASK-419.
- [x] Reconciles the design doc in the same checkpoint as the
      implementation.
- [x] Reader's `dispatch_decode` arm is updated — otherwise a
      round-trip through a segment file would still return
      "not yet implemented".
- [x] All type signatures are consistent: `u16` for patch_count /
      patch_indices, `i64` for patch_values, `u64` for internal
      offsets, `i64` for block_min.
