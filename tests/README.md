# `bqlite-tests`

Workspace-level integration and property tests for bqlite. This is a normal
Rust library crate whose only purpose is to host shared test fixtures and
test binaries; nothing else in the workspace depends on it.

## Layout

```
tests/
├── Cargo.toml
├── README.md           ← this file
├── src/
│   ├── lib.rs          ← re-exports `common` and `strategies`
│   ├── common.rs       ← integration helpers (TempDb, assert_batches_eq, …)
│   └── strategies.rs   ← shared `proptest` strategies (arb_*)
├── tests/              ← Cargo's auto-discovered test binaries
│   ├── prop_property_value.rs
│   ├── prop_time.rs
│   ├── prop_arrow.rs
│   └── common_smoke.rs
└── suite/              ← README-only stubs for Wave 2+ test suites
    ├── retention/
    ├── funnel/
    └── …
```

`tests/tests/*.rs` is the conventional Cargo integration-test directory:
each `.rs` file becomes one `[[test]]` target via auto-discovery, with
zero `Cargo.toml` edits required when a new file lands.

## Adding a new test file

1. Create `tests/tests/<topic>.rs`. Property test names start with
   `prop_`; integration tests use plain topic names.
2. Import what you need from `bqlite_tests`:
   ```rust
   use bqlite_tests::common::{assert_batches_eq, TempDb};
   use bqlite_tests::strategies::{arb_property_value, arb_time_range};
   ```
3. Run `scripts/local-ci.sh` — `cargo test --all-targets` picks up the
   new binary automatically.

If a new test introduces data types worth reusing, add a `pub fn arb_<type>()`
strategy to `src/strategies.rs` so the next test can pull it directly.

## Property-test guidelines

- **One invariant per `#[test]` function.** Shrinking reports one failing
  property — a multi-invariant test makes the counter-example ambiguous.
- **Prefer round-trip invariants** (`encode → decode == identity`) and
  **differential invariants** (`fast_path == reference_impl`). They are
  the cheapest invariants to write and catch the most bugs per line.
- **Bound recursion and collection sizes.** `DEFAULT_RECURSION_DEPTH` and
  `DEFAULT_COLLECTION_SIZE` in `src/strategies.rs` exist to keep shrinking
  fast. Override per-test only when the invariant genuinely needs deeper
  structure.
- **Filter out values outside the domain** (e.g. `NaN`, non-finite floats
  for JSON tests) rather than special-casing them inside the test body.
  A filtered strategy produces legible counter-examples.
- **Keep strings short and alphanumeric by default.** Random arbitrary
  Unicode makes shrink output unreadable without catching more bugs for
  most properties. Widen the alphabet only when the test specifically
  covers encoding edge cases.
- **Tests that need the full `f64` domain** (NaN, ±∞, subnormals) must
  define a local strategy rather than widening the shared `arb_finite_f64`
  — the shared strategy stays finite so JSON-routed tests can't flake.

## Running

```bash
# Everything in the workspace:
cargo test --all-targets

# Just the bqlite-tests package:
cargo test -p bqlite-tests

# A single binary, optionally filtered to one invariant:
cargo test -p bqlite-tests --test prop_time
cargo test -p bqlite-tests --test prop_property_value cmp_is_transitive
```

Set `PROPTEST_CASES=<n>` to crank up (or down) the per-invariant case
count when reproducing a flake or stress-testing a change.

## Dep-direction note

`scripts/check-dep-direction.sh` walks `crates/*/` only, so this package
lives outside the enforced internal-dependency graph by design. Test code
is allowed to reach into any crate; adding deps here is not a rule bypass.
