# Microbenchmark Coverage Audit — Waves 2–4

**Auditor**: TASK-507  
**Date**: 2026-04-28

Audit of every hot path introduced in Waves 2–4 for benchmark coverage.
Each hot path is rated:

- **COVERED** — load-bearing bench exists with measurable throughput or
  regression tripwire.
- **PARTIAL** — bench exists but the documented or design-specified
  variant space is incomplete.
- **MISSING** — no bench exists for the path.

Gaps marked PARTIAL or MISSING become concrete Wave 5 tasks; see §Gaps and
§Wave 5 Tasks below.

---

## Wave 2 Hot Paths

| Hot Path | Bench File | Status |
|---|---|---|
| Segment full scan + all-column decode | `benches/wave2/scan.rs` — `bench_scan_full` | COVERED |
| Projected scan (column subset) | `benches/wave2/scan.rs` — `bench_scan_projected` | COVERED |
| Zone-map pruning (range + decision-only) | `benches/wave2/scan.rs` — `bench_scan_with_zone_map_pruning` | COVERED |
| Int64 column decode throughput | `benches/wave2/scan.rs` — `bench_scan_int64_column` | COVERED |
| String column decode throughput | `benches/wave2/scan.rs` — `bench_scan_string_column` | COVERED |
| Float64 column decode throughput | `benches/wave2/scan.rs` — `bench_scan_float64_column` | COVERED |
| Pushed-down equality (dictionary column) | `benches/wave2/scan.rs` — `bench_scan_pushed_equality` | COVERED |
| Encoding codec round-trips (v1 set) | `benches/wave2/encoding.rs` | COVERED |
| CSV ingest throughput | `benches/wave2/ingest.rs` | COVERED |
| Encoded scan (v1 encodings) | `benches/wave2/scan_encoded.rs` | COVERED |
| End-to-end acceptance query | `benches/wave2/acceptance.rs` | COVERED |

All Wave 2 hot paths have load-bearing benches. The Wave 2 reference targets
(Int64 decode ≥ 200 M rows/s, pushed-down equality ≥ 500 M rows/s, ingest
≥ 100 MB/s, acceptance query < 1 s) are enforced by `BenchResultCollector`
in reference mode and guarded by the Criterion regression gate in CI mode.

---

## Wave 3 Hot Paths

| Hot Path | Bench File | Status |
|---|---|---|
| Matcher StepCounter — LinearSimple/Immediate/WithNeg/WithBind/Full (9 scenarios) | `benches/wave3/matcher.rs` | COVERED |
| Matcher NFA — GeneralNfa alternation + repetition | `benches/wave3/matcher.rs` | COVERED |
| HashAccumulator COUNT/SUM/AVG by group count (10 / 1k / 1M groups) | `benches/wave3/aggregate.rs` | COVERED |
| SortOperator (multi-key, 10k / 100k / 500k rows) | `benches/wave3/sort.rs` | COVERED |
| DistinctOperator (0 / 50 / 90 / 99 % dedup ratios) | `benches/wave3/distinct.rs` | COVERED |
| End-to-end 3-step funnel (ingest + parse + plan + execute) | `benches/wave3/funnel.rs` | COVERED |
| DDSketch insert / quantile / merge; P99 grouped aggregate | `benches/wave3/percentile.rs` | COVERED |
| CompactString vs Arc\<str\> in matcher hot loop | `benches/wave3/compactstring_eval.rs` | COVERED |

All Wave 3 hot paths have load-bearing benches with explicit `PatternClass`
assertions (matcher-strategy.md §8.5) so classifier drift cannot silently
invalidate measurements.

---

## Wave 4 Hot Paths

| Hot Path | Bench File | Status |
|---|---|---|
| EventSelectOperator FIRST / LAST / NTH(5) | `benches/benches/event_select.rs` | COVERED |
| EventSelect FIRST with WHERE predicate | `benches/benches/event_select.rs` | COVERED |
| EventSelect event-type list (1 / 2 / 4 types, StringView) | `benches/benches/event_select.rs` | COVERED |
| EventSelect dict event-type fast path (Dictionary<Int32, Utf8View>) | `benches/benches/event_select.rs` | COVERED |
| EventSelect entity-density sweep (100e×1000ev / 1000e×100ev / 10000e×10ev) | `benches/benches/event_select.rs` | COVERED |
| SessionizeOperator gap-only | `benches/wave4/sessionize.rs` | COVERED |
| SessionizeOperator gap + 1 end-event (StringView) | `benches/wave4/sessionize.rs` | COVERED |
| SessionizeOperator gap + 1 end-event (Dictionary<Int32, Utf8View>) | `benches/wave4/sessionize.rs` | COVERED |
| SessionizeOperator gap + multi-type end-event list (3–5 types) | — | **PARTIAL** |
| AttributeOperator single-entity many touchpoints | `benches/wave4/attribute.rs` | COVERED |
| AttributeOperator many entities sparse | `benches/wave4/attribute.rs` | COVERED |
| AttributeOperator high fan-out emission | `benches/wave4/attribute.rs` | COVERED |
| AttributeOperator LEFT-UNNEST dominant | `benches/wave4/attribute.rs` | COVERED |
| AttributeOperator multi-type event dispatch | `benches/wave4/attribute.rs` | COVERED |
| AttributeOperator touchpoint:conversion ratio sweep (10:1 / 100:1 / 1000:1) | `benches/wave4/attribute.rs` | COVERED |
| SubqueryFilterOperator cohort semi-join probe (100 / 1k / 10k entity cohorts) | `benches/wave4/cohort_join.rs` | COVERED |
| MergeSourcesOperator k-way merge (k=2, k=4) | `benches/wave4/cohort_join.rs` | COVERED |
| SampleFilter::apply_to_array per-row xxHash64 + threshold | `benches/wave4/sample.rs` | COVERED |
| Advanced encodings ALP, DoubleDelta, FOR, RLE, Dictionary, BitPacking | `benches/wave4/encoding_matrix.rs` | COVERED |
| PFOR codec encode + decode throughput | `benches/wave4/pfor.rs` | COVERED |
| FSST string compression round-trip | `benches/wave4/encoding_matrix.rs` | COVERED |
| Compaction L0→L1 throughput (clean segments, 5 inputs) | `benches/wave4/compaction.rs` | COVERED |
| Compaction L0 reduction ratio (5 / 8 / 16 inputs) | `benches/wave4/compaction.rs` | COVERED |
| Compaction with active tombstones (`CompactionTombstoneScan`) | — | **MISSING** |
| JSONL ingest throughput | `benches/wave4/ingest.rs` | COVERED |
| Parquet ingest throughput | `benches/wave4/ingest.rs` | COVERED |
| TombstoneScanWrapper query-time filtering | — | **MISSING** |

---

## Gaps and Rationale

### Gap 1 — TombstoneScanWrapper query-time filtering  (MISSING)

**Operator**: `crates/bqlite-storage/src/tombstone_scan.rs::TombstoneScanWrapper`  
**Hot path**: `next_row_group()` → `TombstoneFilter::filter_batch_with_index()` —
the per-row-group entity-delete hash lookup, time-range compare loop, and
mixed-granularity path. The `EntityDeleteIndex` is pre-built at `new()` time
but the per-batch scan cost has no performance baseline.

**Why it matters**: `deletes.md` §7 routes every query through this wrapper
when any tombstones are active. Entity-delete scans are the most common
granularity (GDPR erasure). A large entity-delete set (1k–100k entries)
combined with a high-density row group hits the hash-lookup loop at every
event row — that cost is invisible in the existing compaction bench, which
uses clean segments.

**New task**: TASK-534 (see §Wave 5 Tasks).

---

### Gap 2 — CompactionTombstoneScan with active tombstones  (MISSING)

**Operator**: `crates/bqlite-storage/src/tombstone_scan.rs::CompactionTombstoneScan`  
**Hot path**: `next_row_group()` — the `seq_id_first + row_offset` derivation
loop for row-level tombstones (per-row arithmetic + HashSet probe), plus the
entity and time-range passes. The current `compaction.rs` bench seeds clean
segments and never exercises this code path.

**Why it matters**: `deletes.md` §12 designates compaction as the sole site
of physical row reclamation. The throughput of a compaction pass over a
segment with 1–10% tombstoned rows is the true steady-state cost (not the
clean-segment baseline). Without a baseline, a regression in tombstone-aware
compaction is invisible to CI.

**New task**: TASK-534 shares this gap (covered by the same bench file).

---

### Gap 3 — Sessionize multi-end-event-type list  (PARTIAL)

**Operator**: `crates/bqlite-operators/src/sessionize.rs`  
**Hot path**: `process_sub_batch()` → end-event matching when `end_events`
contains more than one type. `sessionize.md §8.2` documents the
`EndEventCodeSet` fast path for dictionary-encoded inputs; the existing bench
exercises only 1 end-event type (the minimal code-set case). With 3–5
end-event types the code-set membership test runs against a larger
`HashSet<i32>` (the backing type of `EndEventCodeSet.matching_codes`), and
the interaction between gap boundaries and end-event matching changes
character (an end event can close a session that was already at a gap
boundary).

**New task**: TASK-535 (see §Wave 5 Tasks).

---

### Non-gap notes (intentional omissions and existing coverage)

- **FilterOperator tile loop** — `filter.rs` comments explicitly state this
  operator is replaced by the `FilteredBatch` / `SelectionVector` path from
  `operator-fusion.md` (TASK-503). TASK-526 (Wave 5 benchmark suite) is
  the appropriate home for a fusion-vs-pre-fusion comparison bench; a
  standalone `FilterOperator` micro-bench would measure a path being retired.

- **MergeSourcesOperator at k > 4** — the k=2 and k=4 cases cover the
  dominant production shape. k=8 / k=16 heap-cost scaling is a Wave 5
  concern if the morsel scheduler (TASK-523) exposes many parallel sub-scans;
  TASK-526 can capture it at that time.

- **EventSelect event-type list ≥ 10 types / miss-all scenario** — TASK-531
  (EventSelect property tests and benchmarks) explicitly targets extended
  EventSelect bench coverage including the `lookback:` path and large
  event-type lists. These gaps are captured by that task.

- **Frequency encoding** — `advanced-encodings.md §9.5` is NO-GO; no
  shipping implementation to bench.

---

## Wave 5 Tasks

### TASK-534: `[EASY][IMPL]` Tombstone scan filter microbenchmark

**Output**: `benches/wave4/tombstone_scan.rs`  
**Depends on**: TASK-507  
**Description**: Add Criterion benches for both tombstone scan paths in
`crates/bqlite-storage/src/tombstone_scan.rs`:

1. **TombstoneScanWrapper** (query-time) — measure
   `TombstoneFilter::filter_batch_with_index` throughput on a sequence of
   65 536-row batches with entity-delete sets of 0 / 100 / 10 000 entries,
   a time-range delete covering 10% of rows, and a mixed-granularity case
   (entity + time-range + row-level simultaneously). Report surviving
   rows/second and regression-tripwire each granularity combination.

2. **CompactionTombstoneScan** (compaction-time) — extend `compaction.rs`
   (or add a sibling bench) to seed segments with 1% / 5% / 10%
   entity-tombstoned rows and measure `Database::compact_now` throughput
   relative to the clean-segment baseline already established by
   `bench_compaction_throughput`. The ratio `clean_mb_per_sec /
   tombstoned_mb_per_sec` should not exceed 2× for a 10%-tombstoned
   segment; record as a `[floor]` tripwire.

Both groups must run in CI mode (scaled-down fixtures) and reference mode
(full scale). Register a `[[bench]]` entry for `tombstone_scan` in
`benches/Cargo.toml` per bench-crate conventions, and add the new metrics
and floor targets to `benches/wave4/README.md` so the CI bench gate
(`scripts/bench-compare.sh`) picks them up.

---

### TASK-535: `[EASY][IMPL]` Sessionize multi-end-event-type benchmark

**Output**: `benches/wave4/sessionize.rs` (extend existing file)  
**Depends on**: TASK-507  
**Description**: Extend `benches/wave4/sessionize.rs` to add a
`bench_multi_end_event` group covering end-event lists of 1 / 3 / 5 types in
both StringView and Dictionary<Int32, Utf8View> variants — the
`EndEventCodeSet` fast path from `sessionize.md §8.2` is only exercised today
with a single end-event type. The multi-type cases should use the same
10 000 and 100 000-event scale points as the existing
`bench_throughput` group so regressions in the code-set membership path are
directly comparable. Add a `[floor]` tripwire asserting the 3-type
dictionary case is no more than 1.5× slower than the 1-type dictionary
baseline (the `EndEventCodeSet.matching_codes` field is a `HashSet<i32>`;
O(1) probe cost with small constant means cardinality within 5 entries should
be near-free).
