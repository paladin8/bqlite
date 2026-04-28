# TASK-516 Plan: Dictionary/RLE Selection-First Scan Path Completion

**Goal:** Lower pre-filter materialization on the two remaining classes of common predicates that still fall through to the materialized fallback when `ScanPath::Encoded` is requested:

1. `column IN (literals…)` — dispatched today only as `Compare { Equal }`. The DictionaryEqKernel and ConstantEqKernel both already accept `Vec<ScalarValue>`; only the recognizer + shape + dispatcher are missing.
2. `column = literal` (or IN-list) on **RLE-encoded String columns** — RLE on String is a real outcome of the encoding selector for low-cardinality, long-run strings, but `apply_encoded_eq` falls back to the materialized arrow-compute path because no `RleStringEqKernel` exists.

This is the "Dictionary/RLE selection-first scan path" task description's payoff completion: the remaining common shapes that should not materialize before filter.

## Existing infrastructure (from TASK-515 and earlier CP1–CP5 commits)

- `ConstantEqKernel`, `RleIntEqKernel`, `DictionaryEqKernel` — selection-first kernels in `crates/bqlite-operators/src/encoded_filter.rs`.
- `EncodedEqShape { col_index, literal: PropertyValue }` — recognized only from `Compare { Equal, Column, Literal }` shapes.
- `apply_encoded_eq` — dispatches Constant → ConstantEq, Rle (Int/Timestamp only) → RleIntEq, Dictionary → DictionaryEq, else fallback.
- `ScanOperator` with `ScanPath::{Materialized, Encoded, Auto}`; encoded modes use `next_encoded_row_group` + kernel application, single- and multi-segment.
- `materialize_encoded_column_selected` (TASK-515) — selection-aware materialization with separated nulls.

## Out of scope

- Range kernels (`<`, `>`, etc.) on dict/RLE — different encoding mechanics, separate CP.
- IS NULL / IS NOT NULL kernels — no recognizer exists for the encoded path; deferred.
- Plain-fixed kernels — different encoding family, deferred.
- `ScanPath::Auto` default behavior change — `Auto` is already wired to pick `Encoded` when shapes match; no policy change needed.

## File touch surface

| File | Action | CP |
|------|--------|----|
| `crates/bqlite-operators/src/encoded_filter.rs` | Modify | CP1, CP2 |
| `crates/bqlite-operators/src/scan.rs` | Modify (struct-field break, not import) | CP1 |
| `benches/wave2/scan_encoded.rs` | Modify (required) | CP2 |
| `docs/design/storage/zero-copy-scan-filter.md` | Read-only — verify §6.2 + §8.2 still match | — |

---

## Checkpoint 1: IN-list recognition + multi-literal dispatch

**Files:** `crates/bqlite-operators/src/encoded_filter.rs`, `crates/bqlite-operators/src/scan.rs`

### Steps

- **Struct-field break, not import break.** Change `EncodedEqShape { col_index, literal: PropertyValue }` → `EncodedEqShape { col_index, literals: Vec<PropertyValue> }`. Every read site (currently `apply_encoded_eq`'s match on `&shape.literal` and the existing `recognize_encoded_eq`'s `EncodedEqShape { col_index, literal }` constructor) is rewritten in the same diff. `scan.rs` keeps `Vec<EncodedEqShape>` and `Arc<[EncodedEqShape]>` typed the same way — no callsite there reads the inner literal field, so the break is contained.
- Extend the recognizer (`recognize_encoded_eq` retained, returns `Some(EncodedEqShape)` for both) to match:
  - `CompiledNode::Compare { op: Equal, Column, Literal }` (commutative) — `literals = vec![v]` after rejecting `PropertyValue::Null`.
  - `CompiledNode::InLiteralSet { input: Column, values, negated: false }` with **non-empty** `values` — strip any `PropertyValue::Null` from `values` (NULL is never IN-equal under 2VL — Arrow `is_in` returns `unknown`; the materialized post-filter still drops nulls so the encoded path is consistent), then verify the survivors are type-homogeneous (all share one of Int/Timestamp/Float/String/Bool — heterogeneous shapes are dispatcher bugs and fall through to residual). `negated=true` is not pushable; empty `values` (after stripping) is not pushable — the recognizer returns `None` and the residual stays authoritative. The shape never carries an empty literal set; the kernel-side "empty literals returns empty selection" branch remains as defense-in-depth but the dispatcher never reaches it through the recognizer.
- Update `apply_encoded_eq`:
  - Convert `Vec<PropertyValue>` to the kernel's `Vec<ScalarValue>` (Null entries already stripped by the recognizer; assert via `debug_assert!` no `Null` survives).
  - Constant → `ConstantEqKernel::new(scalars)` (already accepts a `Vec`).
  - Rle + Int/Timestamp → drop literals not representable as i64 (e.g. a stray Float); only when the survivor list is empty does the conjunct fall back to materialized. Symmetric with `DictionaryEqKernel`'s "drop dict misses" rule.
  - Dictionary → `DictionaryEqKernel::new(scalars, col_type)` (already accepts a `Vec`).
  - Anything else (other encodings, materialized fallback) → `apply_fallback_eq` extended to take a `&[PropertyValue]` slice. Build the arrow mask via per-literal `cmp::eq` + `or_kleene` rather than `is_in` so the type-coercion path mirrors the existing single-literal version. (Keep the existing single-literal arrow path when `literals.len() == 1` to avoid a regression; the IN path uses the multi-literal fold.)
- Tests added in `encoded_filter.rs::tests`:
  - `recognize_encoded_in_dictionary_lowers_to_shape` — lowers `column IN (a, b)` from a compiled tree.
  - `recognize_encoded_in_negated_falls_through` — `NOT IN` is not pushable; returns None.
  - `recognize_encoded_in_empty_returns_none` — empty IN-set is not pushable (post-filter handles).
  - `recognize_encoded_in_strips_null_literal` — `IN (10, NULL, 20)` → `literals = [10, 20]`.
  - `recognize_encoded_in_heterogeneous_types_returns_none` — `IN (10, "foo")` is a dispatcher bug; recognizer rejects.
  - `apply_encoded_eq_in_dispatches_dictionary_multi_literal` — end-to-end dispatch + result equivalence.
  - `apply_encoded_eq_in_dispatches_rle_int_multi_literal` — same for RLE int.
  - `apply_encoded_eq_in_dispatches_constant_multi_literal` — same for Constant.
  - `apply_encoded_eq_in_partial_fallback_drops_unrepresentable` — RLE-Int `IN (1, 2, 1.5)` keeps {1, 2} and applies the kernel; only an all-bad list falls back.
- Verify every `EncodedEqShape` read site still compiles. The grep target is `\.literal[^s]` inside `crates/bqlite-operators/src/`.

### Verification

- `scripts/local-ci.sh` passes.
- Subagent code review of staged diff returns no blocking issues.
- Reconcile against `docs/design/storage/zero-copy-scan-filter.md` §6.2 and §8.2 — design already names dictionary IN as the run-of-the-mill case; no spec edit needed.

### Commit message

`TASK-516: Recognize IN-list shapes on the encoded filter path (CP1)`

---

## Checkpoint 2: RleStringEqKernel + dispatcher wiring

**Files:** `crates/bqlite-operators/src/encoded_filter.rs`

### Steps

- Add `RleStringEqKernel { literals: Arc<[Box<str>]> }`. `Arc<[Box<str>]>` keeps the literal vector cheap to clone across kernels and avoids per-row heap traffic; the kernel hot loop compares `&str` against the boxed slice. Construction is per-conjunct — cost is negligible vs. the per-row scan cost it saves.
- Add `parse_rle_string_runs(chunk) -> Option<Vec<(u32, &str)>>` mirroring `parse_rle_int_runs`. Read `run_count` from `chunk.params[..4]` exactly like the int parser does — string values are variable-length, so `run_count` cannot be inferred from payload length. Layout: `params = run_count u32 LE`; `payload = [run_ends u32 × run_count] || [(u32 LE length, utf8 bytes) × run_count]`. Defensive: bounds check, UTF-8 validation, `None` on malformation.
- Implement `EncodedPredicateKernel for RleStringEqKernel`:
  - Encoding mismatch → pass through input (defense-in-depth, matches RleIntEq behavior).
  - Empty literals → `RowSelection::empty()`.
  - Non-nullable: walk runs, emit `RowRun { start: prev_end, len: end - prev_end }` for each run whose value is in the literal set. Adjacent matching runs merge. Output `RowSelection::Runs`, intersected with input via `RowSelection::intersect`.
  - Nullable: walk validity bitmap in parallel with runs; emit logical row indices for each non-null position whose run value is in the literal set. Output `RowSelection::Indices`, intersected with input.
- Add an `EncodedKind::Rle` arm in `apply_encoded_eq` that picks `RleStringEqKernel` when `col_type == BqlType::String`. The Int/Timestamp arm stays `RleIntEqKernel`. Unsupported types fall back as before.
- Tests added:
  - `rle_string_eq_preserves_runs_through_filter` — single literal, non-null, asserts output is `Runs` and run shape matches expectation.
  - `rle_string_eq_merges_adjacent_matching_runs` — explicit merge-invariant test mirroring `select_runs_matching_i64`'s `last.start + last.len == prev_end` branch (e.g. runs `[A,B,A,A]` filtered for `A` produce two non-adjacent runs; `[A,A,B,A]` filtered for `A` produces a merged single run after the kernel sees a contiguous match across the gap induced by intersecting input).
  - `rle_string_eq_in_list_with_multiple_runs` — multi-literal IN.
  - `rle_string_eq_no_match_empty` — none-of literals match.
  - `rle_string_eq_narrows_with_input_runs` — input run intersection.
  - `rle_string_eq_with_nulls_translates_to_logical_indices` — null-bitmap path.
  - `rle_string_eq_real_rle_bytes` — differential test driving the kernel through real RLE bytes produced by `bqlite_storage::Rle.encode` over a `StringViewArray`.
  - `apply_encoded_eq_dispatches_rle_string_to_run_kernel` — end-to-end dispatch test asserting we no longer materialize.
  - `rle_string_eq_wrong_encoding_passes_through` — defense-in-depth.
- **Bench coverage (required, not optional).** Add a `wave2/scan_encoded.rs` group that exercises both new shapes — `IN (lit_a, lit_b)` on a Dictionary column and `=` / IN on an RLE-encoded string column — alongside the existing `constant_rg` / `realistic_rg` cases. Reusing `run_materialized` / `run_encoded` keeps the bench small. This satisfies the `CLAUDE.md` "scan/filter hot-path changes need bench coverage" rule and gives the regression gate a measurable signal for the payoff this CP claims.

### Verification

- `scripts/local-ci.sh` passes.
- Subagent code review.
- Re-verify §6.2 of the design doc — RLE row says "Preserve `RunEndEncoded` when possible; otherwise materialize selected rows only." Run-preserving filter is exactly what the kernel produces.

### Commit message

`TASK-516: RLE string equality kernel preserves run shape (CP2)`

---

## Risks and watchpoints

1. **Empty IN sets must remain residual.** TASK-227 already elides them to a constant `false` residual; the encoded path must not silently produce empty selections that diverge from materialized semantics. The recognizer returns `None` on empty values to keep the residual path authoritative.
2. **3VL semantics for IN with a NULL literal.** SQL `x IN (NULL, …)` returns NULL when no match; current materialized path drops null-matching rows via `is_in` returning unknown. The encoded path strips `PropertyValue::Null` from the literal vector before kernel dispatch — equivalent because a value cannot equal NULL under 2VL, and the residual / Arrow-compute path still applies on the materialized boundary in the rare case where a NULL literal made it past pushdown.
3. **Run merging in RleStringEqKernel.** Adjacent matching runs must merge to keep the output `RowSelection::Runs` invariant tight (sorted ascending, non-overlapping, no zero-length runs in the canonical form). Mirror the merge logic in `select_runs_matching_i64`.
4. **Defense-in-depth on malformed RLE bytes.** Both the existing `parse_rle_int_runs` and the new `parse_rle_string_runs` return `Option` on malformed input — the kernel returns empty selection rather than panicking. Adversarial-input regression: tested via `rle_string_eq_real_rle_bytes`.
5. **No widening of the live row set.** Both CPs preserve the kernel-trait contract: the returned selection is a subset of `input`. Tested in every IN test by passing an input narrower than the full row range.

## Final verification

- `scripts/local-ci.sh` passes after CP2.
- Re-read `docs/design/storage/zero-copy-scan-filter.md` §6.2, §8.2, §11; confirm no spec drift.
- Move `tasks/active/TASK-516.lock` → `tasks/completed/TASK-516.done`, add `completed_at`, push to `origin/main`.
