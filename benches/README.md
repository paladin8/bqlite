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
# Run every bench in the suite.
cargo bench -p bqlite-benches

# Run a single bench group by name.
cargo bench -p bqlite-benches --bench smoke

# Run every bench matching a Criterion regex filter.
cargo bench -p bqlite-benches -- 'smoke/noop'
```

Criterion writes HTML reports and historical baselines under
`target/criterion/` — those files are gitignored and are not part of
CI artifacts.

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

Actual wall-time regression gating is *not* wired up yet. There is no
dedicated `cargo bench` job in CI today. The Wave 2 work referenced by
`TASKS.md` §"Wave 2" performance gate is what introduces per-metric
baselines (≥10% slip blocks merges); until then, benches are collected
and compiled but their numbers are advisory only.

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
