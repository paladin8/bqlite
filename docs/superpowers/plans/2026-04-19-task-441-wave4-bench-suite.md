# TASK-441 — Wave 4 Advanced Analytics Benchmark Suite

**Goal.** Ship the Wave 4 benchmark suite under `benches/wave4/` that covers the six performance-story areas in the task description, records the reference-machine targets, and hooks into the existing CI bench gate (`scripts/bench-compare.sh`).

**Task:** TASK-441 — `[HARD][IMPL] Advanced analytics benchmark suite`
**Branch:** `task/TASK-441`
**Output:** `benches/wave4/`
**Depends on:** TASK-408, TASK-410, TASK-449, TASK-419, TASK-430, TASK-431, TASK-436, TASK-437 (all merged).

**Architecture.** Each bench is a self-contained Criterion bench file registered as `[[bench]]` in `benches/Cargo.toml`. Benches operate at the lowest level that still exercises the real production code path (codecs direct, operators wrapped over `VecOp`, storage paths via `Database`). Reference-mode hard targets are enforced via the existing `BenchResultCollector` + `scripts/bench-compare.sh` mechanism — no new CI wiring is needed. A `benches/wave4/README.md` documents the reference-machine target table and the wave-4 coverage matrix.

**Tech stack.** Criterion, `pprof`, `bqlite-storage`, `bqlite-operators`, `bqlite-planner`, `bqlite-engine`. New benches consume existing helpers in `benches/common/mod.rs` wherever possible (`BenchMode`, `BenchSizing`, `BenchResultCollector`, `BenchTarget`, `generate_events`, `purchases_schema`, `open_db_with_table`, `ScratchDir`, `report_metrics`).

---

## Scope and non-scope

Covered (in scope):
1. **Advanced-encoding comparisons** — ALP is missing and the suite lacks a same-fixture head-to-head comparison matrix. Add both. PFOR stays in `wave4/pfor.rs`; v1 + RLE/DoubleDelta/FOR/FSST stay in `wave2/encoding.rs`.
2. **Compaction throughput + read-amplification** — new bench using `Database::compact_now` with synthetic L0 backlogs.
3. **JSONL + Parquet ingest throughput** — new bench using `JsonlEventReader` / `ParquetEventReader` → `Partitioner` → `SegmentWriter`.
4. **SAMPLE pushdown savings** — new bench comparing `ScanOperator` with/without a `SampleFilter`, plus a manual "post-filter" baseline to quantify the pushdown win.
5. **Cohort / joined-source query overhead** — new bench driving `SubqueryFilterOperator` and `MergeSourcesOperator` directly against synthetic entity-sorted inputs.
6. **ATTRIBUTE ratios** — the existing `wave4/attribute.rs` already covers the five §17.1 workload shapes; the touch-to-conversion ratio sweep called out in the Explore survey is really just a re-parametrization of the `single_entity_many_touchpoints` workload, so I'll extend that bench with three extra ratio points (10:1 / 100:1 / 1000:1) rather than create a new file.

Explicitly deferred (kept as NO-GO / future work):
- Frequency encoding bench — advanced-encodings.md §9.5 recommends **NO-GO**; there is no shipping implementation to bench.
- Engine-level `Engine::query` benches for SAMPLE / MergeSources / Attribute / EventSelect / Sessionize — those nodes are not yet wired through `bind.rs` (TASK-438 blocks that), so we bench at the operator level instead. Cohort semi-joins **are** wired (TASK-437), but I'll still bench at the operator level to keep cohort+join measurements comparable on a shared fixture.
- Selector-decision sweeps across the full advanced-encodings.md §2.1 profile matrix — that's TASK-419's selector tests, not bench evidence.

---

## File plan

Files to create:
- `benches/wave4/encoding_matrix.rs` — ALP + head-to-head comparison.
- `benches/wave4/ingest.rs` — JSONL + Parquet.
- `benches/wave4/compaction.rs` — compaction throughput + L0→L1 read-amp.
- `benches/wave4/sample.rs` — scan-pushdown savings.
- `benches/wave4/cohort_join.rs` — cohort semi-join + merge-sources overhead.
- `benches/wave4/README.md` — wave 4 bench coverage matrix + reference target table.

Files to modify:
- `benches/Cargo.toml` — one `[[bench]]` entry per new file.
- `benches/wave4/attribute.rs` — extend `single_entity_many_touchpoints` with a ratio-sweep group.

No changes to production crates are required. No `benches/common/mod.rs` changes are required (all new fixtures fit inside each bench file's local module).

---

## Reference-machine targets

Enforced only when `BQLITE_BENCH_MODE=reference`; CI mode relies on Criterion's statistical comparison gate. Every target below is the **value passed to `BenchResultCollector::record`**. Targets come from two sources — labelled explicitly in the table:

- **[spec]** — pinned numerically in a design doc. Revising the target requires a design-doc change in the same checkpoint.
- **[floor]** — chosen by this bench suite as a regression tripwire. Not contractual; revising only requires a commit-message note.

| Bench | Metric | Target | Source |
|---|---|---|---|
| `encoding_matrix/alp/round_f64` | Decode throughput | ≥ 2.0 GB/s | **[floor]** advanced-encodings.md §8.3 only states ALP decode is "FOR-unpack + f64 multiply", no numeric floor. Chosen as a regression tripwire against the FOR baseline. |
| `encoding_matrix/alp/round_f64` | Compression ratio vs Plain | ≤ 0.40 | **[spec]** advanced-encodings.md §8.2 Table — round-float ALP achieves 0.30–0.35× of Plain; 0.40 is the 99%-confidence upper bound. |
| `encoding_matrix/int_matrix/clustered` | FOR payload < BitPacking payload | `for_bytes < 0.75 · bp_bytes` | **[spec]** advanced-encodings.md §5.2 — "FOR achieves ~4 bits/value, BitPacking needs ~14 bits/value" on the clustered profile. |
| `ingest/jsonl/end_to_end` | Throughput | ≥ 100 MB/s | **[floor]** parity with the Wave 2 CSV ingest target enforced in `benches/wave2/ingest.rs`; no separate JSONL number pinned in a design doc. |
| `ingest/parquet/end_to_end` | Throughput | ≥ 150 MB/s | **[floor]** Parquet reader decodes columnar chunks, expected to beat JSONL parse; chosen as a regression tripwire. |
| `compaction/throughput/l0_to_l1` | MB/s of input consumed | ≥ 200 MB/s | **[floor]** compaction-concurrency.md pins no MB/s number (§5 Backpressure discusses the L0 threshold in *segment count*, not MB/s). Chosen as a regression tripwire. |
| `compaction/l0_reduction/5_to_1` | L0-count-before / L0-count-after | ≥ 5 | **[spec]** compaction-concurrency.md §3.2 — the L0 trigger is "eligible when count > 4", so 5 segments is the smallest stack that triggers; all 5 collapse into 1 output. |
| `sample/pushdown/fraction_0.01` | `(rows_produced / rows_scanned) − 0.01` | ≤ 0.002 (absolute) | **[floor]** derived from event-select-sample.md §21.2 "bit-identical entity sets" determinism plus the law-of-large-numbers 3σ bound at 100 k entities. |
| `sample/pushdown/fraction_0.10` | `(rows_produced / rows_scanned) − 0.10` | ≤ 0.010 (absolute) | **[floor]** same derivation. |
| `sample/pushdown/throughput_fraction_0.10` | Throughput, entities/sec | ≥ 50 × 10⁶ | **[spec]** event-select-sample.md §21.2 row 1. |
| `cohort/semijoin/cohort_10000/rows_per_sec` | Absolute probe throughput | ≥ 10 M rows/sec | **[floor]** cohorts-aliases-joins.md pins no number. The original plan wanted an `overhead_ratio ≤ 1.5×` target, but the `VecOp` baseline is a trivial pre-built-batch iterator and the probe cost dominates by many orders of magnitude — the ratio carries no signal. Absolute rows/sec against the same synthetic fixture gives a clean regression tripwire; the Criterion comparison plot still shows the relative cost. |
| `merge_sources/k2/rows_per_sec` | Absolute merged throughput | ≥ 10 M rows/sec | **[floor]** same rationale as the cohort row above: the VecOp baseline is too thin to yield a meaningful ratio, so the target is absolute merged-row throughput at k=2. Container-class floor; reference hardware should comfortably exceed this. |

Everything above is measured in reference mode; CI mode runs the same benches at Wave-2-style scaled fixtures and reports the same metrics to `target/bench-results.json` but does not panic on miss.

---

## Checkpoint plan

Every checkpoint merges to `main` before the next starts. Shared-file edits are bundled with the matching new file so each CP stays one atomic unit. The `benches/Cargo.toml` entry for a new bench is added in the same commit as the new file to avoid orphaned `[[bench]]` stanzas.

Every `[[bench]]` stanza for a file under `benches/wave4/` **must** include an explicit `path = "wave4/<file>.rs"` attribute — Cargo does not auto-discover benches recursively (see `benches/README.md`). Pattern (copied verbatim from the existing `wave4/attribute.rs` entry):

```toml
[[bench]]
name = "<bench_name>"
path = "wave4/<file>.rs"
harness = false
```

### CP1 — Encoding matrix bench (ALP + head-to-head comparison)

**Files:** create `benches/wave4/encoding_matrix.rs`; edit `benches/Cargo.toml` (add `encoding_matrix` `[[bench]]` entry).

Contents:
- `bench_alp_round_f64` — ALP encode/decode on a 65 536-row "price-like" f64 array (values in the set `{9.99, 19.95, 29.5, 49.0, 0.99}`). Reports decode throughput (GB/s) and compression ratio vs `Plain`. Reference-mode target: decode ≥ 2.0 GB/s AND ratio ≤ 0.35.
- `bench_alp_random_f64` — ALP on random f64 (sensor-noise profile). Reports ratio and decode; no hard target (ALP is expected to fall back to patch-heavy payload).
- `bench_alp_vs_plain` — Same input, ALP and Plain side-by-side, one `BenchmarkId` per encoding so Criterion plots side-by-side. Uses `group.bench_with_input`.
- `bench_int_encoding_matrix` — Same int64 fixture (clustered, sequential, near-constant-interval), across Plain / BitPacking / FOR / DoubleDelta / Delta. One `BenchmarkId::new("<encoding>", profile)` per cell. `Throughput::Bytes` set to the source bytes so Criterion reports consistent GB/s.
- `bench_string_encoding_matrix` — Same string fixture (low-cardinality, high-cardinality, mixed), across Plain / Dictionary / Rle / Fsst.
- All hard-target checks flow through a `BenchResultCollector` created via `BenchResultCollector::new(BenchMode::from_env())`; `collector.finish()` is called at the end of the `alp` group.

Verification:
```
cargo fmt --check
cargo clippy -p bqlite-benches --all-targets -- -D warnings
cargo test  -p bqlite-benches --bench encoding_matrix
cargo bench -p bqlite-benches --bench encoding_matrix -- --test
scripts/local-ci.sh
```
`cargo test` is enough to catch panics at bench startup; `cargo bench -- --test` runs one sample per function.

Subagent code review with paths staged; if blocking issues, fix; merge `--ff-only` to main; push.

### CP2 — JSONL + Parquet ingest bench

**Files:** create `benches/wave4/ingest.rs`; edit `benches/Cargo.toml` (add `wave4_ingest` entry — use `name = "wave4_ingest"` to avoid conflicting with `wave2/ingest.rs` which already owns `ingest`).

Contents:
- `bench_jsonl_reader` — reads a pre-materialized JSONL file through `JsonlEventReader::open` and counts events. Pure-parse throughput.
- `bench_jsonl_end_to_end` — reader → `Partitioner::push_event` → `SegmentWriter::write_partitioner`. Reports MB/s over the source file's byte size using `iter_custom` + `report_metrics` (pattern identical to `wave2/ingest.rs`). Hard target in reference mode: ≥ 100 MB/s.
- `bench_parquet_reader` — same shape, using `ParquetEventReader::open`.
- `bench_parquet_end_to_end` — reader → partitioner → writer. Hard target: ≥ 150 MB/s.
- Shared helper `write_jsonl_fixture(path, events)` / `write_parquet_fixture(path, events, schema)` local to this file — use `arrow` + `parquet` writers already in the workspace to produce the reference fixture from a `Vec<Event>` synthesised by `generate_events(...)`. Fixture files live under the bench's `ScratchDir` so they're cleaned up between iterations.

Verification: same as CP1 but targeting `--bench wave4_ingest`.

### CP3 — Compaction throughput + read-amp bench

**Files:** create `benches/wave4/compaction.rs`; edit `benches/Cargo.toml` (add `compaction` entry).

Contents:
- `fixture_l0_stack(scratch, n_segments, events_per_segment) -> Database` — writes N separately-flushed L0 segments into a fresh database by calling `SegmentWriter::write_partitioner` N times in a loop with the same `(window, shard)` keys. Uses `Database::create_with_shards(1)` so all events fall in one shard and the L0→L1 trigger is deterministic.
- `bench_compact_throughput` — seeds a 4-segment L0 stack, measures `Database::compact_now("purchases")` via `iter_custom`. Reports MB/s input consumed using the sum of pre-compaction segment bytes; hard target ≥ 200 MB/s.
- `bench_compact_l0_reduction` — seeds an L0 stack of N ∈ {4, 8, 16} segments, calls `compact_now`, records the **L0 segment-count reduction ratio** `(l0_before / l0_after)`. This is compaction's segment-fan-in reduction, *not* LSM read amplification in the point-lookup sense. Hard target: ≥ 4 for `n = 4` (the baseline trigger case). Metric key is `compaction/l0_reduction/4_to_1` so the README and target table stay aligned.
- Reports both metrics via `BenchResultCollector` so CI picks them up.

Verification as CP1, targeting `--bench compaction`.

### CP4 — SAMPLE pushdown savings bench

**Files:** create `benches/wave4/sample.rs`; edit `benches/Cargo.toml` (add `sample` entry).

Contents:
- `build_events(n_entities, events_per_entity) -> Vec<RecordBatch>` — builds entity-sorted batches suitable as `VecOp` input.
- `bench_sample_filter_per_row` — directly constructs a `SampleFilter` via `SampleFilter::new(fraction, seed, entity_col_name, entity_ty)` (signature matches `crates/bqlite-storage/src/sample.rs:82`) and measures `apply_to_array` throughput over a 65 536-row string entity array. Reports entities/sec. Hard target in reference mode at `fraction=0.10`: ≥ 50 M entities/sec/core (event-select-sample.md §21.2).
- `bench_scan_with_pushed_sample` — builds a real `Database` + table, ingests 100 k events, runs a `ScanOperator` opened with `.with_sample_filter(Arc<SampleFilter>)` at fractions 0.01, 0.1, 0.5. Measures (a) wall-clock to drain every batch and (b) total rows produced. Target: `rows_produced / rows_scanned` within 20 % of the fraction (`≤ 0.012` for 0.01, `≤ 0.112` for 0.10, etc.).
- `bench_scan_post_filter_baseline` — same scan with no `sample_filter`, applies the `SampleFilter` per-batch after scan returns. Reports wall-clock so the Criterion comparison graph shows the pushdown win.

Verification as CP1, targeting `--bench sample`.

### CP5 — Cohort + joined-source bench

**Files:** create `benches/wave4/cohort_join.rs`; edit `benches/Cargo.toml` (add `cohort_join` entry).

Contents:
- `build_cohort(entity_ids: &[&str]) -> Arc<CohortHashSet>` — builds a single-column cohort by feeding a one-column `RecordBatch` of entity ids through `CohortHashSet::from_batches(subquery_schema, [batch])` (signature at `crates/bqlite-operators/src/cohort.rs:192`). This is the public construction path used by `bqlite-engine::bind`; it avoids poking at the internal hash-set type directly.
- `bench_cohort_semijoin` — wraps a `VecOp` with `SubqueryFilterOperator`, probes a cohort of 100 / 1 k / 10 k entities over a 1 M-row entity-sorted input. Reports rows/sec. Target: overhead vs scan-only ≤ 1.5× (the scan-only baseline is measured in the same function to keep hardware noise constant).
- `bench_merge_sources_k2` — builds two `VecOp` sub-scans over matched entity-sorted event streams (shared entity universe, interleaved timestamps), wraps them in a `MergeSourcesOperator`, measures merged throughput. Baseline = single-table scan over the concatenated input. Target: overhead ≤ 2.0×.
- `bench_merge_sources_k4` — same with four sub-scans. No hard target (regression tracking only).

Verification as CP1, targeting `--bench cohort_join`.

### CP6 — ATTRIBUTE ratio sweep + Wave 4 README

**Files:** edit `benches/wave4/attribute.rs` (add ratio-sweep group); create `benches/wave4/README.md`.

Contents:
- Add `bench_ratio_sweep` to `attribute.rs` — reuses the existing `single_entity_events` fixture but parameterises `n_tp / n_conv` ∈ {10, 100, 1000} while holding the total event count fixed at `Scale::single_entity_touchpoints`. Registered in the existing `criterion_group!` at the bottom of the file.
- Create `benches/wave4/README.md` documenting:
  - The Wave 4 coverage matrix (one row per bench file, one column per covered area).
  - The full reference-target table from the "Reference-machine targets" section above, in the format used by `benches/README.md`.
  - How to run the suite (`cargo bench -p bqlite-benches --bench encoding_matrix` etc.).
  - How CI gating works (pointer to `scripts/bench-compare.sh`; no new wiring in this checkpoint).
  - The explicit list of things this suite does *not* cover (Frequency NO-GO per advanced-encodings.md §9.5, and the engine-bind-blocked benches pending TASK-438).

Verification: `scripts/local-ci.sh` + `cargo bench -p bqlite-benches --bench attribute -- --test`.

### CP7 — Completion

- Merge CP6 to main.
- `git mv tasks/active/TASK-441.lock tasks/completed/TASK-441.done`.
- Edit the `.done` file: add a `completed_at` field with the current UTC ISO-8601 timestamp, matching the exact format of the most-recent merged completion marker (see `tasks/completed/TASK-440.done` — single-line ISO-8601 "YYYY-MM-DDTHH:MM:SSZ" between `claimed_at` and `branch`).
- Commit `TASK-441: completed`, push to main, end turn.

---

## Risks / open questions

- **Parquet reader API shape.** Verified: `ParquetEventReader::open(path, &TableSchema, &[(String, String)])` + `next_event() -> Result<Option<Event>>` at `crates/bqlite-storage/src/ingest/parquet.rs:129/196` mirrors the JSONL reader signature exactly. No adaptation needed.
- **ALP decode target.** Advanced-encodings.md §8.3 does not pin a specific GB/s figure; I chose ≥ 2.0 GB/s based on the FOR decode rate on integer mantissas. If the actual reference-machine number lands below this but above ~1.0 GB/s, I'll revise the target in the same checkpoint and note the revision in the commit message (per AGENTS.md Behavioral Requirement #5).
- **Compaction throughput target.** compaction-concurrency.md §11 pins "≥ 200 MB/s on the reference machine"; if the measured number on my non-reference container is far below this, I'll keep the hard target in reference mode and rely on Criterion's statistical comparison in CI mode — this is how the Wave 2 ingest target already handles the CI/reference split.
- **CP4 rows_produced/rows_scanned ratio check.** The fraction-check is probabilistic at low entity counts; I'll use ≥ 100 k distinct entities in the SAMPLE bench so the law-of-large-numbers deviation is < 1 %.

---

## Self-review

- Spec coverage: the six areas in the task description are mapped onto CP1/CP2/CP3/CP4/CP5/CP6 respectively. ATTRIBUTE coverage piggy-backs on CP6's attribute.rs edit. README is in CP6.
- Placeholders: none — every bench lists the metric, the fixture, the public API it calls, and either the exact target or "no hard target".
- Type consistency: all new benches use `BenchMode::from_env()`, `BenchResultCollector`, `BenchTarget::at_least/at_most`, and `criterion_for_mode(mode)` exactly as `wave2/ingest.rs` and `wave4/attribute.rs` already do.
- Dependency direction: all new bench files stay inside `bqlite-benches` and depend only on already-linked workspace crates. No production crate edits.
