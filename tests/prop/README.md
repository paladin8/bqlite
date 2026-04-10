# `bqlite-tests/prop` — Property-Test Harness

Workspace-level property tests for bqlite, powered by
[`proptest`](https://docs.rs/proptest). This directory is the landing
pad for randomized invariants that cut across crates: storage encoding
round-trips (Wave 2), parser / pretty-printer round-trips (Wave 2–3),
sequence-matcher equivalence between the NFA and step-counter paths
(Wave 3), and so on.

## Layout

```
tests/                   ← workspace member `bqlite-tests`
├── Cargo.toml           ← declares every test target explicitly
├── prop/
│   ├── README.md        ← this file
│   ├── mod.rs           ← shared strategies (arb_property_value, …)
│   └── property_value.rs← template test — PropertyValue round-trips
└── suite/               ← fixture-based integration tests (Wave 2+)
```

`mod.rs` is **not** a Cargo target. Each sibling `.rs` file (e.g.
`property_value.rs`) is registered as its own `[[test]]` entry in
`tests/Cargo.toml`, and includes `mod.rs` inline via
`#[path = "mod.rs"] mod strategies;`. The `#[path]` attribute is
needed — not a normal `mod strategies;` — because each `[[test]]`
target is compiled as a separate crate root, so they cannot share a
conventional module tree. This idiom keeps strategy definitions
reusable without turning the prop directory into a second library
crate.

> **Dep-direction note.** `scripts/check-dep-direction.sh` only walks
> `crates/*/` — the `bqlite-tests` package lives at the workspace
> root and is intentionally outside the enforced dependency graph.
> Test code is allowed to reach into any crate. When adding
> `dev-dependencies` to `tests/Cargo.toml`, that is by design, not a
> rule bypass.

## Adding a new property-test file

1. Create `tests/prop/<topic>.rs` modeled on
   [`property_value.rs`](property_value.rs):
   ```rust
   #[path = "mod.rs"]
   mod strategies;

   use proptest::prelude::*;
   use strategies::/* whatever strategy you need */;

   proptest! {
       #[test]
       fn <invariant_name>(/* inputs from strategies */) {
           // transform
           // prop_assert_eq! or prop_assert!
       }
   }
   ```
2. Register the file in `tests/Cargo.toml`:
   ```toml
   [[test]]
   name = "prop_<topic>"
   path = "prop/<topic>.rs"
   ```
3. If the topic introduces new data types, add a `pub fn arb_<type>()`
   strategy to [`mod.rs`](mod.rs) so it can be reused by later tests.
4. Run `scripts/local-ci.sh` — the new target is picked up by
   `cargo test --all-targets` automatically.

## Guidelines

- **One invariant per `#[test]` function.** Shrinking reports one
  failing property — a multi-invariant test makes the counter-example
  ambiguous.
- **Prefer round-trip invariants** (`encode → decode == identity`) and
  **differential invariants** (`fast_path == reference_impl`). They are
  the cheapest invariants to write and catch the most bugs per line.
- **Bound recursion and collection sizes.** `DEFAULT_RECURSION_DEPTH`
  and `DEFAULT_COLLECTION_SIZE` in `mod.rs` exist to keep shrinking
  fast. Override per-test only when the invariant genuinely needs
  deeper structure.
- **Filter out values outside the domain** (e.g. `NaN`, non-finite
  floats for JSON tests) rather than special-casing them inside the
  test body. A filtered strategy produces legible counter-examples.
- **Keep strings short and alphanumeric by default.** Random arbitrary
  Unicode makes shrink output unreadable without catching more bugs for
  most properties. Widen the alphabet only when the test specifically
  covers encoding edge cases.

## Running

```bash
# Everything:
cargo test --all-targets

# Just the workspace-level property tests:
cargo test -p bqlite-tests

# A single file / invariant:
cargo test -p bqlite-tests --test prop_property_value json_roundtrip_is_identity
```

Set `PROPTEST_CASES=<n>` to crank up (or down) the per-invariant case
count when reproducing a flake or stress-testing a change.
