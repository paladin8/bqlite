# bqlite benchmarks

Criterion benchmarks for the bqlite query engine live in a single
workspace member crate at `benches/` (package name `bqlite-benches`).
This crate is intentionally separate from the production crates so the
Criterion dependency and any heavyweight dataset fixtures never leak
into library code.

## Layout

```
benches/
├── Cargo.toml              # bqlite-benches package manifest
├── README.md               # this file
├── src/
│   └── lib.rs              # anchors the `common` module via #[path]
├── common/
│   └── mod.rs              # shared helpers (re-exported as
│                           #     bqlite_benches::common)
└── benches/
    └── smoke.rs            # Wave 1 harness-smoke benchmark
```

The `common/` directory sits at the crate root, *not* inside `src/`,
to match the output path named in `TASKS.md` (TASK-121). `src/lib.rs`
uses `#[path = "../common/mod.rs"] pub mod common;` to expose it as
`bqlite_benches::common` for every bench file.

## Adding a new benchmark

1. Create `benches/benches/<name>.rs` — one file per logical benchmark
   group. Import shared helpers via `use bqlite_benches::common::*;`.
2. Register the bench in `benches/Cargo.toml`:

   ```toml
   [[bench]]
   name = "<name>"          # matches the filename without the .rs
   harness = false          # always — Criterion drives execution
   ```

3. Use the standard `criterion_group! { ... } criterion_main! { ... }`
   pair at the bottom of the file so `cargo bench` picks it up.
4. Keep individual bench functions narrow — one measured operator or
   code path per `c.bench_function` call. When a bench needs many
   related measurements, prefer `c.benchmark_group(...)` over stuffing
   everything into one function.
5. Always wrap measured values in `criterion::black_box(...)` to stop
   the optimizer from folding loop bodies away between samples.

Bench files must sit directly under `benches/benches/`. If a wave adds
sub-directories (e.g. `benches/benches/wave2/scan.rs`), each file
needs an explicit `path = "wave2/scan.rs"` on its `[[bench]]` entry —
Cargo does **not** auto-discover benches recursively.

## Running benches locally

```bash
# Run every bench in the suite (CI mode — scaled-down datasets).
cargo bench -p bqlite-benches

# Run a single bench group by name.
cargo bench -p bqlite-benches --bench smoke

# Run every bench matching a Criterion regex filter.
cargo bench -p bqlite-benches -- 'smoke/noop'

# Run in reference mode (100M rows, hard targets enforced).
# Only meaningful on the pinned reference hardware (Apple M2 Max).
BQLITE_BENCH_MODE=reference cargo bench -p bqlite-benches \
    --bench scan --bench encoding --bench ingest --bench acceptance
```

Criterion writes HTML reports and historical baselines under
`target/criterion/` — those files are gitignored and are not part of
CI artifacts.

## Dual-mode dataset strategy (TASK-246)

The bench harness supports two modes controlled by the
`BQLITE_BENCH_MODE` environment variable:

- **`ci`** (default): CI-scaled fixtures for regression-noise control
  on shared runners. Targets are not enforced — only Criterion's
  statistical regression gate applies.

- **`reference`**: Full 100M-row acceptance query on the pinned
  reference hardware. Hard performance targets are enforced — the
  bench panics if any target is missed:

  | Metric | Target |
  |--------|--------|
  | Acceptance query (cold-cache full scan) | < 1 s |
  | Compression ratio (segment / raw CSV) | ≤ 10% |
  | Zone-map pruning effectiveness | ≥ 80% |
  | Int64 decode throughput | ≥ 200M rows/s |
  | Pushed-down equality (dictionary column) | ≥ 500M rows/s effective |
  | Ingest throughput (parse → sort → encode → write) | ≥ 100 MB/s |

Both modes write machine-readable results to `target/bench-results.json`
so CI regression gating and manual release sign-off compare the same
metrics. The CI workflow uploads separate artifacts for each mode.

## CI contract

`scripts/local-ci.sh` and `.github/workflows/ci.yml` build the bench
crate with `cargo build --all-targets` and run `cargo test --all-targets`,
which is enough to catch:

- Compilation failures in bench files (a broken bench fails CI even if
  no one runs `cargo bench`).
- `#[test]`-annotated unit tests inside `common/mod.rs` — the intended
  way to sanity-check shared helpers without pulling Criterion into
  the unit-test path.
- **Criterion bench execution in `--test` mode.** Because every bench
  declares `harness = false`, each bench file is a plain binary that
  Criterion drives. Under `cargo test --all-targets` Cargo invokes
  those binaries with `--test`, which makes Criterion run one sample
  per function and exit with an error on failure — without this, a
  bench that panics at startup would silently pass CI.

The bench CI workflow (`.github/workflows/bench.yml`) provides:

- **Baseline capture** (`bench-baseline`): On every push to main,
  captures Criterion estimates as the `bench-baseline-main` artifact.
- **Regression gate** (`bench-gate`): On non-draft PRs, downloads the
  latest baseline and runs `scripts/bench-compare.sh`. Fails if any
  metric regresses >10% on 3+ consecutive Criterion samples.
- **Reference benchmark** (`bench-reference`): Manual dispatch with
  `BQLITE_BENCH_MODE=reference` for full 100M-row runs with hard
  target enforcement.

## Wave 1 status

Wave 1 ships only a single **smoke** benchmark (`benches/smoke.rs`)
that measures a trivial `identity(u64) -> u64` call. Its purpose is to
prove the harness itself compiles and runs — it is *not* a real
measurement. Real benchmarks arrive in the waves that build out each
measured subsystem:

- **Wave 2.** Scan/filter microbenchmarks, encoding round-trip throughput,
  end-to-end acceptance query (see `TASKS.md` §"Wave 2" performance gate).
- **Wave 3.** Pattern-matching (NFA, step-counter, anchor propagation).
- **Wave 4.** Advanced-encoding comparisons (FSST, ALP, PFOR, FOR,
  DoubleDelta, RLE, Frequency) against the v1 set.

Contributors should not delete the smoke benchmark — it stays as a
cheap canary so any Criterion-harness regression (e.g. a mis-configured
group, a broken shared helper, a dependency bump that breaks
compilation) fails CI on its own before any real measurement is taken.
