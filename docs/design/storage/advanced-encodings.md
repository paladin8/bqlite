# Advanced Encoding Research

**Wave**: 4
**Task**: TASK-401
**Status**: draft
**Depends on**: none (builds on the Wave 2 v1 encoding set)

## 1. Scope

This document evaluates seven candidate encodings for the Wave 4
segment format (v2): **RLE**, **DoubleDelta**, **FOR**, **PFOR**,
**FSST**, **ALP**, and **Frequency**. For each candidate the
evaluation records:

- Compression ratio against the Wave 2 baseline on representative
  column profiles.
- Decode throughput — the metric that gates adoption in a
  query-performance engine.
- Predicate-pushdown implications — whether the encoding preserves,
  enables, or blocks pushdown paths.
- Segment-format impact — new metadata blocks, footer extensions,
  reader compatibility.
- Implementation complexity — lines of code, new dependencies,
  testing surface.

The deliverable is a **go / no-go recommendation per encoding** with
evidence, plus the **exact codec set Wave 4 carries forward** into
the v2 segment format work (TASK-402).

### 1.1 Out of scope

- Full `Encoding` trait implementations — those are the downstream
  IMPL tasks (TASK-413 through TASK-418, TASK-450).
- Encoding selector heuristic updates — TASK-419 owns integration.
- Segment format v2 layout (header, footer, version negotiation) —
  TASK-402.
- Deferred-research encodings from `storage-format.md` §10.6
  (PCodec, Gorilla, prefix suppression, Corra, ANS/FSE, Roaring
  bitmaps, BtrBlocks sampling) — these remain on the research
  backlog unless this document explicitly promotes one.

### 1.2 Relationship to existing design docs

This document refines `storage-format.md` §10.2–§10.4 by grounding
the encoding descriptions there in benchmark evidence. The
`EncodingType` enum values assigned in §10.2 are carried forward
unchanged:

| Encoding | Discriminant | §10.2 name |
|---|---|---|
| RLE | 5 | `Rle` |
| DoubleDelta | 3 | `DoubleDelta` |
| FOR | 8 | `For` |
| PFOR | 9 | `PFor` |
| FSST | 7 | `Fsst` |
| ALP | 10 | `Alp` |
| Frequency | 11 | `FreqEncoding` |

`segment-format-v1.md` §9 freezes the v1 encoding set at
`{Plain=0, Dictionary=1, Delta=2, BitPacking=4, Constant=6}`. The
seven candidates above extend the enum in v2 without renumbering
existing discriminants. v1 readers reject unknown discriminants with
`BqliteError::Corruption`, so mixed-version safety is preserved.

---

## 2. Methodology

### 2.1 Representative column profiles

Every candidate is evaluated against column profiles drawn from the
reference `purchases` dataset (10 000 entities, 20 event types,
monotonic-within-entity timestamps, 7 mixed-type property columns)
at the default row-group size of 65 536 rows. The profiles are:

| Profile | Column example | Data characteristics |
|---|---|---|
| **Monotonic timestamps** | `timestamp` | Sorted within entity, 1 ms spacing, base ~1.7×10¹⁸ ns |
| **Near-constant-interval timestamps** | `timestamp` (heartbeat entities) | Sorted, Δ ≈ 1 000 000 ns ± 100 ns |
| **Strongly monotonic integers** | `__seq_id` | Strictly increasing, constant Δ = 1 |
| **Narrow-range integers** | `quantity` (1–10), `amount` (clustered) | Unsorted, small range, occasional outliers |
| **Low-cardinality strings** | `event_type` (20 distinct) | Highly repetitive, dictionary-friendly |
| **Skewed categorical strings** | `event_type` where top-3 types account for 70% | Zipfian-distributed dictionary codes |
| **Repeated values (sorted)** | `entity_id` within a row group | Long runs of the same value, ~655 rows/run |
| **High-cardinality strings** | URL, user-agent, query strings | 10 000+ distinct values, common substrings |
| **Round floats** | `price` (9.99, 29.95, …), `discount` (0.0, 0.1, 0.2) | Few significant decimal digits |
| **Random floats** | Sensor data, computed scores | No decimal structure, high entropy |
| **Booleans** | `flag` column | 50/50 mix or long true/false runs |

### 2.2 Baseline

The baseline is the Wave 2 v1 encoding set with the existing
selector heuristic:

- **Plain** — universal fallback, zero overhead for fixed-width.
- **Dictionary** — sorted distinct values + bit-packed codes.
- **Delta** — first-value + zigzag bit-packed residuals.
- **BitPacking** — global min-value frame-of-reference + packed
  offsets.
- **Constant** — zero-payload for uniform chunks.
- **LZ4** — post-encoding compression wrapper (threshold ≤ 0.9).

### 2.3 Evaluation criteria

| Criterion | Weight | Rationale |
|---|---|---|
| Decode throughput | Highest | bqlite is a query-performance engine; `storage-format.md` §10.7 locks LZ4-speed as the floor |
| Compression ratio | High | Smaller data → less I/O → faster scans, but never at the cost of decode speed |
| Predicate pushdown | Medium | Encodings that preserve or enable code-based filtering earn a bonus |
| Implementation complexity | Medium | Each encoding becomes a maintenance surface; complexity must be justified by measurable wins |
| Segment-format impact | Low | New metadata is cheap if the reader can skip it for v1 segments |

### 2.4 Evidence sources

- Published benchmark numbers from peer-reviewed papers (cited per
  encoding).
- Analytical estimates grounded in the byte-level layout of each
  encoding and the reference dataset characteristics.
- Comparison against v1 implementations by computing compression
  ratios from the known v1 payload formulas (bit-width selection,
  dictionary code sizes, delta residual widths).

---

## 3. RLE (Run-Length Encoding)

**Discriminant**: `Rle = 5`
**Applicable types**: `Bool`, `Int`, `String`, `Timestamp`
**Reference**: Standard technique; used in Parquet, ORC, DuckDB.

### 3.1 Description

Stores `(value, run_length)` pairs. A run is a maximal consecutive
subsequence of identical values. The on-disk layout is:

```
run_count:   u32 LE                     // number of runs
run_ends:    [u32 LE; run_count]        // cumulative end positions
values:      encoding-specific payload  // one value per run
```

Run ends are cumulative (run_ends[i] = number of rows up to and
including run i), matching Arrow's `RunEndEncodedArray` convention
for zero-copy decode. Values are encoded per the column's `BqlType`:
i64 LE for Int/Timestamp, length-prefixed UTF-8 for String, packed
bitmap for Bool.

### 3.2 Compression ratio analysis

**entity_id column (sorted, ~655 rows/run at 65 536 rows with 100
entities):**

| Encoding | Payload bytes (65 536 rows) | Ratio vs Plain |
|---|---|---|
| Plain (length-prefixed strings, ~15 bytes/value) | ~983 040 | 1.00× |
| Dictionary (5-bit codes, 100 entries) | ~40 960 + dict | 0.05× |
| RLE (~100 runs × (4 + 15) bytes) | ~1 900 | **0.002×** |

RLE achieves ~500× compression on entity_id — far better than
Dictionary's ~20×. This is because the sort order guarantees long
runs.

**Boolean flag column (alternating true/false):**

| Encoding | Payload bytes | Ratio vs Plain |
|---|---|---|
| Plain (bit-packed booleans) | 8 192 | 1.00× |
| RLE (32 768 runs × 5 bytes) | 163 840 | **20×** (worse) |

RLE is catastrophically bad on alternating data — it expands rather
than compresses. The selector must check average run length before
choosing RLE.

**Boolean flag column (90% true in sorted runs of ~6 553):**

| Encoding | Payload bytes | Ratio vs Plain |
|---|---|---|
| Plain | 8 192 | 1.00× |
| RLE (~10 runs × 5 bytes) | ~50 | **0.006×** |

**Break-even point**: RLE beats Plain when the average run length
exceeds ~2 for fixed-width types. For strings, the threshold is
lower because Plain's per-value overhead is higher.

### 3.3 Decode throughput

RLE decode is a broadcast operation: for each run, fill
`run_length` slots with the run's value. This is:

- **Cache-friendly**: sequential writes into the output buffer.
- **Branch-free** on the inner loop (memset-style fill for
  fixed-width types).
- **Expected throughput**: 2–4 GB/s on modern hardware (comparable
  to Plain memcpy for the output buffer, limited by write bandwidth
  rather than compute).

For the Arrow `RunEndEncodedArray` path (zero-copy decode), the
decode cost is effectively zero — the on-disk bytes are already in
the Arrow layout. Operators that support run-end-encoded input
(counting, grouping) can skip the broadcast entirely and operate
in O(runs) time.

### 3.4 Predicate pushdown

RLE preserves value identity per run, so equality and range
predicates can be evaluated per-run rather than per-row. For
entity_id filtering, the predicate resolves to a run range — the
scan skips all rows outside that range with no per-row work.

### 3.5 Segment-format impact

No new segment-level metadata. RLE is self-contained within a
column chunk: the encoding header carries the run count, and the
payload carries the run ends and values. No footer extension needed.

### 3.6 Implementation complexity

**Low.** The encode path is a single pass over the input array
collecting runs. The decode path is a single pass broadcasting
values. The Arrow `RunEndEncodedArray` zero-copy path is an
optimization but not required for correctness. Estimated
implementation: ~200–300 lines of Rust plus property tests.

### 3.7 Recommendation

**GO.** RLE provides dramatic compression wins on sorted
low-cardinality columns (entity_id, boolean flags with runs) that
no v1 encoding matches. The implementation is simple, decode
throughput is excellent, and the encoding is self-contained with
no segment-format impact.

**Selector guard**: Only choose RLE when estimated average run
length > 2. This prevents the pathological expansion on
high-entropy data.

---

## 4. DoubleDelta

**Discriminant**: `DoubleDelta = 3`
**Applicable types**: `Int`, `Timestamp`
**Reference**: Pelkonen et al., "Gorilla: A Fast, Scalable,
In-Memory Time Series Database," VLDB 2015.

### 4.1 Description

Stores second-order deltas: `dd[i] = delta[i] - delta[i-1]` where
`delta[i] = values[i] - values[i-1]`. The first two values are
stored as-is (base_value and first_delta in the params block). The
remaining `row_count - 2` double-deltas are zigzag-encoded and
bit-packed at a uniform width.

```
encoding_params:
    base_value:   i64 LE    // values[0]
    first_delta:  i64 LE    // values[1] - values[0]
    dd_bit_width: u8        // 1..=64
payload:
    bit-packed zigzag(dd[i]) stream, (row_count - 2) values
```

### 4.2 Compression ratio analysis

**Near-constant-interval timestamps (Δ ≈ 1 000 000 ns ± 250 ns):**

| Encoding | Bit width | Payload bytes (65 536 rows) | Ratio vs Plain |
|---|---|---|---|
| Plain (8 bytes/value) | 64 | 524 288 | 1.00× |
| Delta (zigzag residuals) | ~21 bits (Δ range ~1M) | ~171 672 | 0.33× |
| DoubleDelta (dd range ~500) | ~9 bits | ~73 728 | **0.14×** |

DoubleDelta achieves ~7× compression vs Plain and ~2.3× vs Delta
on near-constant-interval data.

**Strictly monotonic seq_id (Δ = 1, dd = 0):**

| Encoding | Bit width | Payload bytes | Ratio vs Plain |
|---|---|---|---|
| Delta | 2 bits (zigzag(1)=2, width=2) | ~16 384 | 0.03× |
| DoubleDelta | 1 bit (dd=0, floor width=1) | ~8 192 | **0.016×** |

Both compress well, but DoubleDelta saves one additional bit per
value because the double-deltas are all zero.

**Random integers (no monotonic structure):**

DoubleDelta offers no benefit over Delta. The second-order deltas
have the same entropy as first-order deltas when the input has no
temporal structure. The selector should only choose DoubleDelta
when the variance of first-order deltas is significantly lower
than the variance of the values themselves — i.e., when the data
is approximately linear.

### 4.3 Decode throughput

DoubleDelta decode is: bit-unpack → zigzag-decode → cumulative sum
(first-order deltas) → cumulative sum (original values). This is
two prefix-sum passes over the output buffer — each is a simple
sequential scan.

**Expected throughput**: ~1.5–2.5 GB/s. The extra prefix-sum pass
compared to Delta costs roughly 15–25% of decode time, but the
smaller payload compensates via reduced I/O.

### 4.4 Predicate pushdown

DoubleDelta does not preserve individual values in a
directly-testable form. Range predicates cannot be evaluated without
full decode. However, zone maps at the row-group level still apply
(they are computed from the decoded values and stored in the
footer), so pushdown at the row-group granularity is unaffected.

### 4.5 Segment-format impact

No new segment-level metadata. The encoding params block grows by
8 bytes (the `first_delta` field) compared to Delta. This is an
inline change to the column chunk header — no footer extension.

### 4.6 Implementation complexity

**Low.** Very similar to the existing Delta implementation. The
encode path computes second-order differences instead of first-order
differences. The decode path runs two cumulative-sum passes instead
of one. The overflow check extends to the second-order delta
computation (three consecutive i128 subtractions).

Estimated implementation: ~250–350 lines of Rust (much of it
shared with Delta's bit-packing infrastructure). Property tests
can reuse the Delta test strategies with the additional invariant
that `decode(encode(x)) == x` for near-constant-delta sequences.

### 4.7 Recommendation

**GO.** DoubleDelta provides a meaningful ~2× improvement over
Delta on the timestamp columns that dominate bqlite's workload
(event timestamps with near-constant intervals). The
implementation is a straightforward extension of the existing Delta
codec, and the decode throughput is well above the LZ4-speed floor.

**Selector guard**: Choose DoubleDelta over Delta when the variance
of first-order deltas is below a threshold (e.g., the bit width of
zigzag-encoded first-order deltas exceeds 2× the bit width of
zigzag-encoded second-order deltas). When the improvement is
marginal (<10% payload reduction), prefer Delta for its simpler
decode path.

---

## 5. FOR (Frame-of-Reference)

**Discriminant**: `For = 8`
**Applicable types**: `Int`, `Timestamp`
**Reference**: Zukowski et al., "Super-Scalar RAM-CPU Cache
Compression," ICDE 2006.

### 5.1 Description

FOR divides the column into fixed-size **blocks** (128 or 256
values) and encodes each block with its own per-block minimum value
and bit width. This is a refinement of the v1 BitPacking encoding,
which uses a single global minimum across the entire column chunk.

```
encoding_params:
    block_size:  u16 LE    // 128 or 256 (SIMD-aligned)
    block_count: u32 LE    // ceil(row_count / block_size)
payload:
    for each block:
        block_min:  i64 LE
        bit_width:  u8
        packed offsets: ceil(block_size × bit_width / 8) bytes
                        (padded to 8-byte boundary)
```

### 5.2 Compression ratio analysis

**Narrow-range integers with local clustering (amount column,
clusters of 128 values in ranges [100,200], [4500,4600], etc.):**

| Encoding | Bits/value | Payload bytes (65 536 rows) | Ratio vs Plain |
|---|---|---|---|
| Plain | 64 | 524 288 | 1.00× |
| BitPacking (global min/max) | ~14 bits (range spans 0–4600) | ~114 688 | 0.22× |
| FOR (128-value blocks, ~7 bits/block avg) | ~7 bits + per-block overhead | ~65 536 | **0.13×** |

FOR wins because each block has a tighter range than the global
range. The per-block overhead (9 bytes: min + width) is amortized
across 128 values (0.07 bytes/value) — negligible.

**Uniformly distributed integers (no local clustering):**

When the range is uniform across blocks, FOR converges to
BitPacking with extra overhead (the per-block min/width headers).
In this case FOR is ~1–3% worse than BitPacking — the selector
should prefer the simpler encoding when FOR provides no benefit.

### 5.3 Decode throughput

FOR decode is: for each block, read min + width, then unpack
offsets and add min. The per-block structure is SIMD-friendly — a
128-value block fits in 16 SIMD registers (AVX2 lane = 8 × i32).
The block boundary overhead is minimal.

**Expected throughput**: ~2–3 GB/s. Comparable to BitPacking (same
bit-unpack hot loop), with a small overhead for per-block base
addition.

### 5.4 Predicate pushdown

FOR preserves per-block min/max naturally (the block_min and
block_min + max_offset are known from the header). A range
predicate can skip entire blocks by checking the block's range
against the predicate — a form of intra-row-group pruning that
BitPacking does not offer.

### 5.5 Segment-format impact

No new segment-level metadata. The block structure is encoded
entirely within the column chunk payload. The encoding params block
carries the block size and count; per-block headers are inline.

### 5.6 Implementation complexity

**Low–Medium.** The core bit-pack/unpack is shared with v1
BitPacking. The new code is the block iteration loop and per-block
header parsing. The block_size parameter requires validation (must
be 128 or 256). The last block may be short (fewer than block_size
values), which needs a dedicated code path.

Estimated implementation: ~300–400 lines of Rust. Property tests
should cover: round-trip for every block size, short final blocks,
single-block chunks, and the degenerate case where every block has
the same min (converges to BitPacking).

### 5.7 Recommendation

**GO.** FOR provides a meaningful improvement over global
BitPacking for columns with local clustering (common in event
data where numeric properties are correlated with entity or time).
The per-block structure also enables intra-row-group predicate
skipping — a new pushdown capability not available in v1. The
implementation reuses existing bit-packing infrastructure.

**Selector guard**: Choose FOR over BitPacking when the sum of
per-block bit widths is less than `block_count × global_bit_width`
by more than 10%. Below that threshold, the per-block overhead
makes FOR slightly worse than BitPacking.

---

## 6. PFOR (Patched Frame-of-Reference)

**Discriminant**: `PFor = 9`
**Applicable types**: `Int`, `Timestamp`
**Reference**: Zukowski et al., "Super-Scalar RAM-CPU Cache
Compression," ICDE 2006.

### 6.1 Description

PFOR extends FOR with an exception list for outlier values. Values
that fit in the "narrow" bit width are stored inline; outliers that
would force a wider bit width for the entire block are stored in a
separate patch list.

```
encoding_params:
    block_size:     u16 LE
    block_count:    u32 LE
payload:
    for each block:
        block_min:     i64 LE
        main_width:    u8           // narrow bit width for non-outliers
        patch_count:   u16 LE       // number of outliers in this block
        packed main:   ceil(block_size × main_width / 8) bytes
        patch_indices: [u16 LE; patch_count]  // positions of outliers
        patch_values:  [i64 LE; patch_count]  // full-width outlier values
```

### 6.2 Compression ratio analysis

**Integer column with 5% outliers (95% in [0,255], 5% in
[0, 2³¹]):**

| Encoding | Bits/value | Payload bytes (65 536 rows) | Ratio vs Plain |
|---|---|---|---|
| Plain | 64 | 524 288 | 1.00× |
| BitPacking (global, 32-bit width) | 32 | 262 144 | 0.50× |
| FOR (per-block, still needs 32 bits in blocks with outliers) | ~24 avg | ~196 608 | 0.38× |
| PFOR (8-bit main + patches) | ~8 + 0.05×80 = ~12 avg | ~98 304 | **0.19×** |

PFOR achieves ~2.5× better compression than plain FOR and ~5× vs
Plain. The savings come from encoding 95% of values at 8 bits
instead of 32 bits.

**No outliers (values all in [0,255]):**

PFOR converges to FOR with zero-length patch lists — the patch
overhead per block is just 2 bytes (patch_count = 0). Negligible
cost.

**All outliers (every value is an outlier):**

PFOR degrades to storing every value at full width plus per-value
overhead (2-byte index + 8-byte value). This is worse than Plain.
The selector must cap the patch fraction — PFOR should only be
chosen when the outlier fraction is below ~10%.

### 6.3 Decode throughput

PFOR decode is: unpack the main bit stream (same as FOR), then
apply patches at the indicated positions. The patch application is
a scatter operation — random writes into the output buffer at
`patch_indices` positions.

**Expected throughput**: ~1.5–2.5 GB/s. The scatter step causes
cache-line conflicts for very high patch counts, but at the
recommended ≤10% outlier cap, the overhead is small (the scatter
touches at most 10% of cache lines).

### 6.4 Predicate pushdown

Same as FOR: per-block min/max enables block-level predicate
skipping. Additionally, the main_width implies a tighter value
range for non-outlier values, which could enable more aggressive
pruning in future optimizations.

### 6.5 Segment-format impact

No new segment-level metadata. The block structure and patch lists
are fully inline in the column chunk payload.

### 6.6 Implementation complexity

**Medium.** PFOR builds on FOR's block structure but adds the
patch list encode/decode and the outlier detection heuristic. The
encode path must:
1. Choose the main bit width (e.g., the 90th percentile of
   offsets).
2. Identify outliers (values exceeding the main width).
3. Encode the main stream with outlier positions set to zero.
4. Encode the patch list.

The decode path reverses this: unpack main, then scatter patches.

Estimated implementation: ~400–500 lines of Rust (plus FOR as a
dependency — TASK-450 depends on TASK-415). Property tests must
cover: no outliers, all outliers, single outlier at each position,
and the worst-case all-patched fallback.

### 6.7 Recommendation

**GO.** PFOR is the standard solution for integer columns with
outliers — a common pattern in event data (most amounts are small,
but a few are large). The compression improvement over FOR/
BitPacking is substantial (2–5×) for the target workload. The
implementation complexity is moderate but well-understood.

**Selector guard**: Choose PFOR when more than 1% but fewer than
10% of values in a block exceed the FOR bit width at the
block_size-scaled threshold. Below 1% outliers, FOR is sufficient.
Above 10%, the patch overhead dominates — fall back to
BitPacking or Plain+LZ4.

---

## 7. FSST (Fast Static Symbol Table)

**Discriminant**: `Fsst = 7`
**Applicable types**: `String`
**Reference**: Boncz et al., "FSST: Fast Random Access String
Compression," VLDB 2020.

### 7.1 Description

FSST builds a 256-entry symbol table of common substrings (1–8
bytes each) from a sample of the column's strings, then re-encodes
each string by replacing substring occurrences with single-byte
codes. The symbol table is stored once per segment (not per row
group) and shared by all row groups.

```
segment-level:
    fsst_symbol_table:
        table_id:     u32
        column_ordinal: u32
        symbols:      [FsstSymbol; 256]    // each: len u8 + bytes[1..8]
        byte_offset:  u64
        byte_length:  u64

per-chunk encoding_params:
    symbol_table_id:  u32     // index into footer.fsst_tables
    
payload:
    for each string (row_count - null_count entries):
        compressed_len: u16 LE
        compressed_bytes: [u8; compressed_len]
```

### 7.2 Compression ratio analysis

**High-cardinality strings (URLs, ~80 bytes average, 10 000
distinct):**

| Encoding | Bytes/value | Payload bytes (65 536 rows) | Ratio vs Plain |
|---|---|---|---|
| Plain (length-prefixed) | ~84 | ~5 505 024 | 1.00× |
| Plain + LZ4 | ~84 → LZ4 block | ~1 376 256 (4× compression) | 0.25× |
| Dictionary (infeasible: 10 000 entries × 80 bytes = 800 KB dict + 14-bit codes) | ~1.75 + dict overhead | ~914 688 | 0.17× |
| FSST (3–5× compression per string) | ~20–28 | ~1 638 400 | **0.30×** |

FSST compresses ~3–4× on typical URL/user-agent columns. It is
slightly worse than LZ4 block compression on aggregate ratio, but
critically:

- **FSST preserves random access.** Each string is independently
  decodable. LZ4 requires decompressing the entire block to read a
  single string.
- **FSST decode is faster.** The decode loop is a byte-by-byte
  symbol lookup — no state, no dependencies between strings. Boncz
  et al. report >3 GB/s decode throughput. LZ4 block decode is
  ~2–4 GB/s but requires a larger working set.
- **FSST + LZ4 stacks.** The FSST-compressed payload can be further
  wrapped in LZ4 for an additional ~20–30% reduction. The combined
  ratio approaches Dictionary levels on high-cardinality data.

**Low-cardinality strings (20 distinct event types):**

Dictionary encoding is strictly superior. FSST's symbol table
cannot exploit the fact that only 20 strings exist — it operates
at the substring level. The selector should prefer Dictionary when
cardinality is low (< 30% of row count, per the v1 threshold).

### 7.3 Decode throughput

The FSST decode loop is:

```
for each byte in compressed_bytes:
    if byte < 256 and is_symbol[byte]:
        emit symbol_table[byte].bytes
    else:
        emit byte (literal escape)
```

This is a tight loop with no branches beyond the symbol/literal
check. Published benchmarks (Boncz et al., VLDB 2020) report:

- **Decode**: 3.5 GB/s on a single core (Intel Skylake, their
  benchmark hardware).
- **Encode**: 1.5 GB/s (symbol table construction is the expensive
  part, but it's amortized across the whole segment).

On modern Apple Silicon or AMD Zen 4, expect 4–6 GB/s decode
throughput.

### 7.4 Predicate pushdown

FSST does **not** preserve string identity — a predicate like
`event_type = 'purchase'` cannot be evaluated against the
compressed bytes without decoding. However:

- **Zone maps still apply.** Per-row-group min/max are computed from
  decoded values and stored in the footer.
- **Dictionary + FSST stacking.** For low-cardinality columns,
  Dictionary is preferred anyway. FSST targets the high-cardinality
  case where pushdown is less effective (the predicate matches a
  small fraction of a large value space).

### 7.5 Segment-format impact

**New segment-level metadata block.** FSST requires a segment-level
symbol table region, analogous to the existing segment-level
dictionary region. `segment-format-v1.md` §17.3 already defers FSST
symbol tables to Wave 4. The v2 footer gains:

- A `Vec<FsstSymbolTableRef>` field parallel to the existing
  `Vec<SegmentDictRef>`.
- A new region between the dictionaries region and the footer body
  (or appended to the dictionaries region) containing the
  concatenated symbol table bytes.

This is the **only** encoding in this evaluation that requires a
segment-level metadata extension.

### 7.6 Implementation complexity

**High.** FSST is the most complex encoding in this evaluation:

- **Symbol table construction**: An iterative greedy algorithm that
  selects the 256 best substrings from a sample of the column's
  data. The algorithm is well-specified in the paper but non-trivial
  to implement correctly (multiple passes, substring counting,
  gain-based selection).
- **Encode/decode**: The core encode/decode loops are straightforward
  (byte-by-byte lookup), but the escape mechanism and variable-
  length symbols require careful handling.
- **Segment-level storage**: The symbol table must be hoisted from
  the per-chunk encode output to the segment-level region, similar
  to how Dictionary values are hoisted today.

**Crate option**: The `fsst` crate from Lance exists in the Rust
ecosystem and is the chosen implementation path for TASK-416.
Microbenchmarks on bqlite's representative URL / user-agent string
workloads showed materially better encode, decode, and payload-size
results than the previous `fsst-rs`-based adapter, while preserving
the serialized symbol-table model bqlite needs for segment-level
storage.

Estimated implementation: ~600–800 lines of Rust if using an
existing crate for the core algorithm, ~1200–1500 lines if
implementing from scratch. Property tests must cover: round-trip
over diverse string distributions, empty strings, strings shorter
than the minimum symbol length, and the degenerate case where no
symbols are beneficial (FSST payload ≥ Plain payload).

### 7.7 Recommendation

**GO.** FSST fills a critical gap in bqlite's encoding portfolio:
high-cardinality string columns (URLs, user agents, query strings)
that fall through Dictionary encoding. These columns are common in
behavioral analytics data and currently encode as Plain+LZ4, which
sacrifices random access and prevents per-string predicate
evaluation. FSST provides comparable compression with superior
decode speed and random access preservation.

The segment-level metadata extension is the main cost, but it is
already anticipated by the design docs (`storage-format.md` §3.4,
`segment-format-v1.md` §17.3) and is a one-time addition to the
v2 format.

**Selector guard**: Choose FSST when `cardinality / row_count ≥ 0.3`
(high cardinality) and the column is String-typed. For low-
cardinality strings, Dictionary remains the winner.

---

## 8. ALP (Adaptive Lossless floating-Point)

**Discriminant**: `Alp = 10`
**Applicable types**: `Float`
**Reference**: Afroozeh & Leis, "ALP: Adaptive Lossless
floating-Point Compression," SIGMOD 2023.

### 8.1 Description

ALP exploits the observation that many real-world float columns
contain "round" values (prices, percentages, scores) that can be
losslessly represented as `mantissa × 10^exponent` where the
mantissa is a small integer. ALP finds per-chunk (exponent, factor)
pairs such that `round(value × factor) / factor == value` for most
values. The integer mantissas are then encoded with FOR+BitPacking,
achieving near-integer compression ratios.

For values that don't decompose cleanly ("exceptions"), ALP stores
them in a separate patch list (similar to PFOR's outlier mechanism).

```
encoding_params:
    exponent:      u8       // decimal exponent
    factor:        f64 LE   // 10^exponent, precomputed
    patch_count:   u32 LE   // number of exceptions
payload:
    mantissa stream: FOR-encoded i64 array (row_count - patch_count values)
    patch_indices:   [u32 LE; patch_count]
    patch_values:    [f64 LE; patch_count]   // exact IEEE 754 for exceptions
```

### 8.2 Compression ratio analysis

**Round prices ($9.99, $29.95, etc., 2 decimal places):**

| Encoding | Bytes/value | Payload bytes (65 536 rows) | Ratio vs Plain |
|---|---|---|---|
| Plain (8 bytes/value) | 8 | 524 288 | 1.00× |
| Plain + LZ4 | 8 → ~6.4 (20% LZ4 savings on structured floats) | ~419 430 | 0.80× |
| ALP (mantissa range ~[999, 9999], 14-bit FOR) | ~1.75 | ~114 688 | **0.22×** |

ALP achieves ~4.5× compression on round prices — dramatically
better than Plain+LZ4.

**Percentages (0.00–1.00, 2 decimal places):**

| Encoding | Bytes/value | Payload bytes | Ratio vs Plain |
|---|---|---|---|
| Plain | 8 | 524 288 | 1.00× |
| ALP (mantissa range [0, 100], 7-bit FOR) | ~0.875 | ~57 344 | **0.11×** |

~9× compression on percentage data.

**Random/high-entropy floats (sensor data, computed scores):**

ALP falls back to storing exceptions for every value — effectively
Plain with overhead. The selector must sample the column to check
ALP decomposability before committing.

Afroozeh & Leis (SIGMOD 2023) report that ALP achieves near-
integer compression ratios on ~80% of real-world float columns from
the Public BI Benchmark, with a graceful fallback for the remaining
~20%.

### 8.3 Decode throughput

ALP decode is: FOR-unpack mantissas → multiply by `1.0 / factor`
→ scatter exceptions. The mantissa unpacking reuses the FOR decode
path. The division by factor is a single f64 multiply per value.

**Expected throughput**: ~1.5–3 GB/s. The paper reports decode
speeds comparable to integer FOR (the f64 multiply adds minimal
overhead). The exception scatter is the same cost as PFOR's patch
application.

### 8.4 Predicate pushdown

ALP preserves numeric order in the mantissa domain: `value_a <
value_b` implies `mantissa_a < mantissa_b` (for same exponent).
Range predicates can be transformed into mantissa-domain predicates
and evaluated against the FOR-encoded mantissa stream without
decoding to f64. This is a **new pushdown capability** not available
in v1 for float columns.

### 8.5 Segment-format impact

No new segment-level metadata. The exponent and factor are stored
in the per-chunk encoding params. The FOR-encoded mantissa stream
and patch list are inline in the payload.

### 8.6 Implementation complexity

**Medium.** The core algorithm is:
1. **Exponent selection**: Try exponents 0–18 and pick the one that
   maximizes the fraction of values that decompose cleanly. This is
   a loop over the column sampling ~1–10% of values.
2. **Mantissa extraction**: `mantissa = round(value × factor)`,
   check `mantissa / factor == value` (exact equality). Values that
   don't round-trip are exceptions.
3. **FOR encoding of mantissas**: Reuses the FOR implementation
   (TASK-415).
4. **Exception list**: Same pattern as PFOR patches.

Estimated implementation: ~400–500 lines of Rust. The main
subtlety is floating-point exact comparison — the `round(value ×
factor) / factor == value` check must use strict IEEE 754 equality,
not approximate comparison. Property tests should verify lossless
round-trip for every value, including edge cases (NaN, ±Inf,
subnormals, ±0.0).

**Crate option**: No mature, well-maintained Rust ALP crate exists
at the time of writing. The algorithm is straightforward enough
(exponent search + FOR on mantissas) that a from-scratch
implementation is the expected path. The FOR dependency (TASK-415)
provides the mantissa encoding layer.

### 8.7 Recommendation

**GO.** ALP fills the major gap in bqlite's float encoding
strategy. Currently, float columns have no encoding better than
Plain+LZ4 (the v1 selector routes all non-uniform, non-constant
floats to Plain). For the common case of "round" float data
(prices, percentages, scores), ALP achieves 4–9× compression with
decode throughput comparable to integer encodings. The
implementation builds on FOR (TASK-415) and follows the established
PFOR exception pattern.

**Selector guard**: Choose ALP when a sample of the column shows
≥70% of values decompose cleanly at some exponent. Below that
threshold, Plain+LZ4 is safer (no exception overhead).

---

## 9. Frequency Encoding

**Discriminant**: `FreqEncoding = 11`
**Applicable types**: `String`, `Int` (via Dictionary codes)
**Reference**: Used in BtrBlocks (Kuschewski et al., SIGMOD 2023)
and DuckDB's dictionary-bitpacking cascade.

### 9.1 Description

Frequency encoding is an optimization pass applied **on top of
Dictionary encoding**. After building a sorted dictionary, the
codes are remapped so that the most frequent values get the smallest
codes. The bit-packed code stream then uses fewer bits on average
because common values have small codes. This is a form of implicit
Huffman-like encoding without the per-symbol variable-length
complexity.

```
encoding_params:
    dict_id:           u32 LE    // index into footer.dictionaries
    code_bit_width:    u8        // max bit width (for the rarest value)
    freq_map_length:   u32 LE    // number of entries in the remapping table
payload:
    freq_to_dict:  [u32 LE; freq_map_length]   // freq_code → dict_ordinal
    packed codes:  bit-packed frequency-ordered codes
```

Wait — actually, frequency encoding's compression benefit comes from
reducing the **average** bit width, not the **maximum** bit width.
With uniform-width bit-packing (the v1 approach), every code uses
the same number of bits regardless of frequency. The benefit only
materializes when the encoding uses **variable-width** codes (like
Huffman) or when the frequency reordering allows a **smaller fixed
width** by making the common values fit in fewer bits.

Let me re-evaluate the actual mechanism:

**Revised analysis**: In a fixed-width bit-packing scheme (which is
what v1 uses), the code width is determined by `ceil(log2(cardinality))`.
Frequency reordering does not change the cardinality, so it does
not change the bit width. The codes are just permuted — the same
number of bits per code.

The real benefit of frequency encoding in columnar databases comes
from **combining it with a variable-length scheme** like:
- Huffman coding of the codes (complex, rarely used in OLAP)
- Run-length encoding of the frequency-ordered codes (sorted data
  with frequent values clusters better after reordering)
- Byte-aligned variable-width codes (1-byte for top-256, 2-byte
  for the rest)

For bqlite's fixed-width bit-packing model, frequency encoding
provides **no compression benefit** unless paired with a
variable-width scheme.

### 9.2 Compression ratio analysis (revised)

**Skewed categorical column (Zipfian, 100 categories, top-3 = 70%
of rows):**

| Encoding | Bit width | Payload bytes (65 536 rows) | Ratio vs Plain |
|---|---|---|---|
| Dictionary + BitPacking (7-bit codes) | 7 | ~57 344 | 0.11× |
| Dictionary + FreqEncoding + BitPacking (still 7-bit codes) | 7 | ~57 344 | 0.11× |

**No improvement.** The bit width is determined by cardinality
(100 → 7 bits), not by frequency distribution. Frequency reordering
is a no-op for fixed-width bit-packing.

**Potential benefit with byte-aligned variable-width codes:**

If the top-N values (where N < 256) are encoded as 1-byte codes and
the rest as 2-byte codes, then for a Zipfian distribution where 70%
of values are in the top-3:

| Encoding | Avg bytes/code | Payload bytes | Ratio vs fixed-width |
|---|---|---|---|
| Fixed-width (7 bits) | 0.875 | ~57 344 | 1.00× |
| Byte-variable (1 byte for top-256, 2 for rest) | 0.7 × 1 + 0.3 × 2 = 1.3 | ~85 197 | **1.49× (worse)** |

The byte-aligned scheme is actually **worse** because the
fixed-width bit-packing is already very efficient. Variable-width
byte codes waste the high bits of each byte.

### 9.3 Segment-format impact

The frequency map adds a per-chunk overhead of
`4 × cardinality` bytes (the `freq_to_dict` remapping table). For
100 categories this is 400 bytes — negligible. But since the
compression benefit is zero for fixed-width bit-packing, even this
small overhead is unjustified.

### 9.4 Implementation complexity

**Low** if implemented, but the engineering investment is wasted
without a corresponding compression benefit.

### 9.5 Recommendation

**NO-GO.** Frequency encoding provides no measurable compression
benefit in bqlite's fixed-width bit-packing model. The theoretical
benefit requires variable-width coding, which introduces
significant complexity (Huffman trees, variable-length decode loops)
and is incompatible with SIMD bit-unpacking.

If future profiling reveals that a variable-width coding scheme
would provide material benefit, frequency encoding should be
revisited as part of that effort — not as a standalone encoding.

**Alternative**: For skewed categorical columns, the existing
Dictionary + BitPacking cascade is already near-optimal. If further
compression is needed, RLE on the dictionary codes (after sorting
the data) is a more effective approach.

---

## 10. Summary: Wave 4 Codec Set

### 10.1 Go / No-Go Matrix

| Encoding | Discriminant | Decision | Primary win | Key evidence |
|---|---|---|---|---|
| **RLE** | 5 | **GO** | 100–500× on sorted repetitive columns | entity_id: ~0.002× vs Plain |
| **DoubleDelta** | 3 | **GO** | 2× over Delta on near-constant-interval timestamps | ~0.14× vs Plain on 1ms-spaced timestamps |
| **FOR** | 8 | **GO** | 1.5–2× over BitPacking on locally-clustered integers | Per-block framing + intra-row-group pruning |
| **PFOR** | 9 | **GO** | 2.5–5× over FOR/BitPacking on columns with outliers | 8-bit main + patches vs 32-bit global width |
| **FSST** | 7 | **GO** | 3–5× on high-cardinality strings with random access | Published: >3 GB/s decode (VLDB 2020) |
| **ALP** | 10 | **GO** | 4–9× on round floats | Published: near-integer compression (SIGMOD 2023) |
| **Frequency** | 11 | **NO-GO** | None under fixed-width bit-packing | Zero compression benefit; requires variable-width coding |

### 10.2 Exact v2 encoding set

The v2 segment format carries forward the v1 set plus six new
encodings:

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
    // Retired: FreqEncoding = 11 (no-go, discriminant reserved)
}
```

Discriminant 11 is reserved but not assigned — a future
variable-width coding scheme could claim it.

### 10.3 Updated encoding selection policy

The v2 selector extends the v1 score-all-applicable heuristic with
new candidates and guards:

```
Phase 1 — Trivial cases (unchanged):
  1. If all values identical → Constant

Phase 2 — String columns:
  2. If cardinality / row_count < 0.3 → Dictionary + BitPacking on codes
  3. If cardinality / row_count ≥ 0.3 → FSST
     (guard: FSST payload < Plain payload, else fall through)

Phase 3 — Integer / Timestamp columns:
  4. If sorted, monotonic, and dd_bit_width < 0.5 × delta_bit_width → DoubleDelta
  5. If sorted, monotonically increasing → Delta + BitPacking
  6. If unsorted, sum(per-block widths) < 0.9 × block_count × global_width → FOR
     - If > 1% and < 10% of values are outliers → PFOR
  7. If cardinality / row_count < 0.3 → Dictionary + BitPacking on codes
  8. Otherwise → BitPacking (global frame-of-reference)

Phase 4 — Float columns:
  9. If ≥ 70% of sample values ALP-decompose cleanly → ALP
  10. Otherwise → Plain + LZ4

Phase 5 — Boolean columns:
  11. If average run length > 2 → RLE
  12. Otherwise → Plain (bitpacked by Arrow, already compact)

Phase 6 — RLE override (any applicable type):
  13. If average run length > threshold and RLE estimate <
      current best → RLE (overrides the type-specific choice)

Phase 7 — Fallback:
  14. Plain + LZ4 post-compression
```

### 10.4 Decode cost tiebreaker (v2 extension)

| Encoding | Cost | Decode hot path |
|---|---|---|
| Constant | 0 | broadcast single value |
| Plain | 1 | memcpy / fixed-stride read |
| RLE | 2 | broadcast per run (or zero-copy RunEndEncoded) |
| BitPacking | 3 | bit-unpack into i64 |
| FOR | 4 | per-block bit-unpack + base add |
| Delta | 5 | bit-unpack + cumulative sum |
| DoubleDelta | 6 | bit-unpack + 2× cumulative sum |
| PFOR | 7 | per-block bit-unpack + base add + patch scatter |
| Dictionary | 8 | bit-unpack + per-row dict lookup |
| ALP | 9 | FOR-unpack mantissas + f64 multiply + patch scatter |
| FSST | 10 | per-byte symbol lookup |

### 10.5 Expected encodings by column role (v2)

| Column role | v1 encoding | v2 encoding | Improvement |
|---|---|---|---|
| `entity_id` (String, sorted) | Dictionary | **RLE** | ~25× better compression |
| `timestamp` (near-constant Δ) | Delta | **DoubleDelta** | ~2× better compression |
| `timestamp` (variable Δ) | Delta | Delta (unchanged) | — |
| `event_type` (low cardinality) | Dictionary | Dictionary (unchanged) | — |
| `__seq_id` (monotonic) | Delta | **DoubleDelta** | ~2× better |
| Boolean with runs | Plain | **RLE** | 10–100× better |
| Boolean alternating | Plain | Plain (unchanged) | — |
| URLs, user agents | Plain+LZ4 | **FSST** | ~3× better with random access |
| `amount` (clustered integers) | BitPacking | **FOR** or **PFOR** | 1.5–5× better |
| `price` (round floats) | Plain+LZ4 | **ALP** | ~4× better |
| Random floats | Plain+LZ4 | Plain+LZ4 (unchanged) | — |

---

## 11. Downstream task implications

### 11.1 TASK-413 (RLE): Proceed

Implement per the `Encoding` trait pattern. Property tests should
verify round-trip and the run-length expansion guard (average run
length > 2).

### 11.2 TASK-414 (DoubleDelta): Proceed

Implement as an extension of the Delta codec. Share bit-packing
infrastructure. Property tests should include near-constant-delta
sequences and the overflow edge case (three consecutive i128
subtractions).

### 11.3 TASK-415 (FOR): Proceed

Implement with 128-value blocks. PFOR (TASK-450) builds on this.
Property tests should cover short final blocks and the degenerate
single-block case.

### 11.4 TASK-416 (FSST): Proceed

Use the `fsst` crate for the core algorithm. Coordinate with
TASK-402 (segment format v2) for the segment-level symbol table
region and keep bqlite's self-contained trait-level chunk params as
the serialized symbol-table blob until the writer hoists those bytes
to the segment-level FSST region.

### 11.5 TASK-417 (ALP): Proceed

Implement with FOR dependency (TASK-415). The exponent selection
loop and mantissa extraction are the key subtleties. Property tests
must verify lossless round-trip for all representable f64 values,
including edge cases.

### 11.6 TASK-418 (Frequency): Retire

This task should be retired with a note linking to this document's
§9 analysis. The discriminant 11 is reserved but not assigned.

### 11.7 TASK-450 (PFOR): Proceed

Builds on FOR (TASK-415). The patch list mechanism is the new code;
the block structure is inherited.

**Implementation (landed):**
`crates/bqlite-storage/src/encoding/pfor.rs` implements the `Pfor`
`Encoding` impl against the §5.5 byte layout, reusing the
`bitpacking` crate's `BitPacker4x` fast path that FOR already uses.
The TASKS.md guideline ("Use the fastpfor crate instead of implementing
manually") is superseded by the pinned on-disk format in
segment-format-v2.md §5.5 — the Rust `fastpfor` crate implements
FastPFOR (Lemire 2015), a different block/exception scheme than the
Zukowski 2006 PFOR this design doc specifies. Selector integration
(including the 1–10% outlier-fraction guard from §6.7) is deferred to
TASK-419 per the selector-ownership convention; `Pfor::estimate_size`
returns the exact payload size so TASK-419 can rank PFOR against FOR
and BitPacking with byte-accurate comparisons.

---

## 12. References

1. Boncz, P., Neumann, T., & Raducanu, O. (2020). "FSST: Fast
   Random Access String Compression." VLDB Endowment, 13(12).
2. Afroozeh, A., & Leis, V. (2023). "ALP: Adaptive Lossless
   floating-Point Compression." SIGMOD 2023.
3. Zukowski, M., Heman, S., Nes, N., & Boncz, P. (2006).
   "Super-Scalar RAM-CPU Cache Compression." ICDE 2006.
4. Pelkonen, T., et al. (2015). "Gorilla: A Fast, Scalable,
   In-Memory Time Series Database." VLDB Endowment, 8(12).
5. Kuschewski, M., et al. (2023). "BtrBlocks: Efficient Columnar
   Compression for Data Lakes." SIGMOD 2023.
6. Afroozeh, A., & Leis, V. (2023). "FastLanes: Accelerating
   Encodings for Fun and Profit." VLDB 2023.
7. Lemire, D., & Boytsov, L. (2015). "Decoding billions of integers
   per second through vectorization." Software: Practice and
   Experience.

---

## 13. Open Questions

None blocking. All decisions in this document are self-contained
and grounded in published evidence and analytical estimates.

**Note**: `storage-format.md` §10.3 and §10.4 currently describe
Frequency encoding as reducing bit width ("After frequency
reordering, BitPacking uses fewer bits because common values have
small codes"). This claim is incorrect for fixed-width bit-packing
(see §9.2 above). TASK-419 (selector integration) should update
`storage-format.md` to remove FreqEncoding from the selection
cascade and note that discriminant 11 is reserved but retired.

The following are deferred to downstream tasks:

1. **FSST crate integration model** (TASK-416): The crate choice is
   resolved (`fsst`), but the writer still needs to hoist the
   self-contained symbol-table bytes to the segment-level FSST region
   when TASK-419 wires v2 segment emission end-to-end.
2. **FOR block size** (TASK-415): 128 vs 256. Both are valid; 128
   aligns with AVX2 register files, 256 amortizes per-block overhead
   better. The implementation task should benchmark both and pick the
   winner.
3. **ALP exponent search range** (TASK-417): The paper uses
   exponents 0–18. The implementation may narrow this based on
   profiling the reference dataset.
4. **PFOR outlier threshold** (TASK-450): The 1%–10% range is a
   guideline. The implementation should validate this against the
   reference dataset and adjust if needed.
