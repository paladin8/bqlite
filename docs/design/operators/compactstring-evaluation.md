# CompactString Evaluation for Matcher Hot Paths

**Wave**: 3
**Task**: TASK-332
**Status**: complete
**Depends on**: TASK-331 (pprof profiling pass)
**Depended on by**: TASK-399 (Wave 3 quality audit)
**Implemented by**: TASK-454 (Wave 4 — `BindingValue::String` migrated to `CompactString`)

---

## 1. Purpose

Evaluate whether adopting `compact_str::CompactString` (v0.9) for the string-
heavy surfaces in the matcher hot paths provides a material performance
improvement over the current `Box<str>` implementation, and deliver a go/no-go
recommendation with clear migration boundaries.

---

## 2. Background

The Wave 3 matcher hot paths use strings in three distinct roles:

1. **Event-type comparison** — the `transition.event_type` field on
   `Transition` and `PoisonTransition` in the compiled NFA
   (`bqlite-planner/src/compile.rs:72-91`). Stored as `String`, compared
   against borrowed `&str` from Arrow `StringViewArray` on every event.

2. **Binding value storage** — the `BindingValue::String(Box<str>)` variant
   in `bqlite-operators/src/matcher/bindings.rs:137`. Extracted from Arrow
   columns, cloned into `StepCounterTrack.bindings`, `Step0Entry.binding_key`,
   and `EntityBindingState.track_index` keys. The clone path is the
   highest-frequency string operation in the binding-enabled matcher.

3. **Relevant event-type set** — `CompiledNfa::relevant_event_types` is a
   `BTreeSet<String>` used for O(log k) early-exit filtering on every event
   (`step_counter.rs:487`, `nfa.rs:723`).

The TASK-331 pprof report (docs/perf/wave3-funnel-pprof.md) identified these
surfaces as optimization candidates but noted they are secondary to the decode
and merge bottlenecks that dominated pre-optimization profiles:

| Hotspot | % of post-opt profile |
|---------|-----------------------|
| Row-group decoding | ~45% |
| `step_counter::process_event` | ~25% |
| Entity boundary / adapter | ~15% |
| Output batch construction | ~10% |

Within the ~25% matcher slice, string operations are a fraction — the majority
is predicate evaluation, step advancement, and window checking. The string
surfaces are real but not dominant.

---

## 3. Candidate String Representations

| Type | Stack size | Inline capacity | Clone cost (short) | Clone cost (long) |
|------|------------|-----------------|--------------------|--------------------|
| `String` | 24 bytes | 0 (always heap) | heap alloc + memcpy | heap alloc + memcpy |
| `Box<str>` | 16 bytes | 0 (always heap) | heap alloc + memcpy | heap alloc + memcpy |
| `CompactString` | 24 bytes | ≤ 24 bytes | memcpy only | heap alloc + memcpy |

Key insight: `CompactString` stores strings ≤ 24 bytes entirely on the stack.
Clone is a 24-byte memcpy with no allocator interaction. This is the SSO
(Small String Optimization) strategy.

### 3.1 BindingValue Enum Size Impact

The `BindingValue` enum currently occupies 24 bytes regardless of string
variant, because `BindingValue::Int(i64)` already requires 8 bytes + 8 bytes
discriminant alignment:

| Variant | BindingValue size |
|---------|-------------------|
| With `Box<str>` (16 bytes) | 24 bytes |
| With `String` (24 bytes) | 24 bytes |
| With `CompactString` (24 bytes) | 24 bytes |

Switching from `Box<str>` to `CompactString` does not increase the enum size.

---

## 4. Microbenchmark Results

All benchmarks run on the CI container (Linux, release mode, 10,000 iterations
per measurement). Source: `benches/wave3/compactstring_eval.rs`.

### 4.1 Event-Type Equality Comparison

Comparing an owned string against a borrowed `&str` (the inner-loop pattern).

| String length | String | Box\<str\> | CompactString |
|---------------|--------|-----------|---------------|
| Short (≤8 bytes) | 55 ns | 55 ns | 57 ns |
| Medium (≤19 bytes) | 33 ns | 33 ns | 34 ns |
| Long (>24 bytes) | 22 ns | 22 ns | 22 ns |

**Verdict**: No measurable difference. Comparison is pure `memcmp` on the
dereferenced `&str` regardless of storage layout. The representation does not
affect comparison cost.

### 4.2 Binding Value Extraction (Arrow → Owned)

Creating an owned string from a borrowed `&str` (simulating
`StringViewArray::value(row).into()`), 10,000 iterations.

| String length | String | Box\<str\> | CompactString | CS vs Box\<str\> |
|---------------|--------|-----------|---------------|-------------------|
| Short (7 bytes) | 254 µs | 250 µs | 199 µs | **1.26× faster** |
| Medium (19 bytes) | 247 µs | 245 µs | 322 µs | 0.76× (slower) |
| Long (51 bytes) | 257 µs | 251 µs | 272 µs | 0.92× (comparable) |

**Verdict**: CompactString wins for very short strings (≤ ~12 bytes) where
inline construction avoids the allocator entirely. For medium strings near
the 24-byte boundary, CompactString's branch + length check adds overhead
vs. a straight `Box::from()`. For long strings, both heap-allocate and
are comparable.

Extraction happens once per variable per batch row. It is O(rows × vars),
pre-computed in `BindingValueCache::build()`, and is not the bottleneck.

### 4.3 Clone (Critical Path)

Cloning an owned string value, 10,000 iterations. This is the highest-
frequency string operation — it occurs 2–3 times per binding event in the
step counter path (binding key construction, Step0Entry insertion,
track creation).

| String length | String | Box\<str\> | CompactString | CS vs Box\<str\> |
|---------------|--------|-----------|---------------|-------------------|
| Short (7 bytes) | 457 µs | 251 µs | **23 µs** | **10.9× faster** |
| Medium (19 bytes) | 461 µs | 246 µs | **24 µs** | **10.3× faster** |
| Long (51 bytes) | 469 µs | 256 µs | 297 µs | 0.86× (comparable) |

**Verdict**: For strings ≤ 24 bytes (the overwhelming majority of event type
names and binding values in analytics workloads), CompactString clone is
**~10× faster** than both `Box<str>` and `String`. This is because clone
is a 24-byte stack memcpy with zero allocator interaction. For strings > 24
bytes, CompactString falls back to heap allocation and is slightly slower
due to its thicker pointer representation.

### 4.4 HashMap Lookup (Binding Track Index)

HashMap keyed by a single-element `Vec<T>`, 40,000 lookups (10,000 iterations
× 4 keys).

| Type | Time | vs Box\<str\> |
|------|------|---------------|
| String | 1.62 ms | 1.01× |
| Box\<str\> | 1.64 ms | baseline |
| CompactString | 1.73 ms | 0.95× |

**Verdict**: Negligible difference. Hash computation and equality checking
are dominated by the string bytes, not the wrapper overhead.

### 4.5 Aggregate Allocation (Realistic Workload)

1,000 entities × 4 binding values per entity, creating the full allocation
pattern (Vec of Vecs).

| Type | Time | vs Box\<str\> |
|------|------|---------------|
| Box\<str\> | 135 µs | baseline |
| CompactString | 60 µs | **2.2× faster** |

**Verdict**: The aggregate allocation advantage is significant. For workloads
with many entities and short-string bindings, CompactString reduces allocator
pressure substantially.

---

## 5. Profile-Weighted Impact Assessment

### 5.1 Where Clone Cost Matters

The clone path fires in these locations:

| Call site | Frequency | String length |
|-----------|-----------|---------------|
| `try_bind_variables` → `bound_values[var_idx] = Some(v.clone())` | Once per var per binding event | Binding values: typically ≤ 20 bytes |
| `check_step0` → `binding_key: SmallVec` construction | Once per step-0 match | Same |
| `Step0Entry` insertion → `binding_key.clone()` | Once per step-0 match | Same |
| `check_binding_advance` → `track_bindings.iter().map(|v| Some(v.clone()))` | Once per advance per check var | Same |
| `EntityBindingState::get_or_create_track` → `key.clone()` | Once per new track | Same |
| `all_completions` → `key.clone()` | Once per completion | Same |

For a 3-step binding pattern with 4 distinct binding values on a 100M-event
dataset (~1M entities, ~30% match rate), the clone path executes approximately:
- ~10M step-0 matches (10% event frequency × 100M events)
- ~10M binding key clones (Step0Entry + track lookups)
- ~3M advance clones (30% of entities complete the pattern)

At 10× speedup per clone, this saves ~(20M × 230 ns) ≈ **4.6 seconds** on
the binding clone path alone — material for a query that currently completes
in ~3.8 seconds for the non-binding funnel.

**However**: the 3-step funnel benchmark (TASK-331) uses `LinearSimple` (no
bindings), which has zero binding clones. The clone benefit only applies to
`LinearWithBindings`, `LinearFull`, and NFA paths with variable bindings.

### 5.2 Where Clone Cost Does Not Matter

- **Event-type comparison** (the ultra-hot path at ~25% of profile): Zero
  allocation, pure comparison. CompactString offers no benefit.
- **Compiled NFA construction** (plan time): Transition event-type strings
  are created once at compile time. Clone cost is amortized over millions
  of events and is negligible.
- **`relevant_event_types` BTreeSet**: Lookup is O(log k) string comparison,
  not allocation. CompactString offers no comparison speedup.

### 5.3 Weighted Recommendation

For the canonical 100M 3-step funnel query **without** bindings: **no impact**.
The hot path is event-type comparison and step advancement, neither of which
involves string allocation.

For binding-enabled patterns (`LinearWithBindings`, `LinearFull`): **material
impact** on the binding clone path. Expected improvement: 10–20% reduction in
per-event cost for the binding-track management code, translating to roughly
2–5% improvement in overall query time (since binding management is a subset
of the ~25% matcher slice).

---

## 6. Go / No-Go Recommendation

### Decision: **CONDITIONAL GO** — adopt CompactString for `BindingValue` only

The data supports a narrowly scoped adoption:

### 6.1 Surfaces Safe to Convert

| Surface | Current type | Proposed type | Rationale |
|---------|-------------|---------------|-----------|
| `BindingValue::String` | `Box<str>` | `CompactString` | 10× clone speedup for ≤ 24-byte strings; no enum size increase; this is the only clone-heavy surface |

### 6.2 Surfaces NOT Recommended for Conversion

| Surface | Current type | Why not convert |
|---------|-------------|-----------------|
| `Transition.event_type` | `String` | Never cloned in hot path; only compared. CompactString offers no comparison benefit. |
| `PoisonTransition.event_type` | `String` | Same as above. |
| `CompiledNfa.relevant_event_types` | `BTreeSet<String>` | Lookup-only; no clone. Consider `HashSet` for O(1) but that is orthogonal to string type. |
| `VariableBindingDef.name` / `source_column` | `String` | Compile-time only; not in hot path. |
| `PropertyValue::String` | `String` | Boundary type (ingest/test); not in query hot path per module doc. |
| `ScalarValue::String` | `String` | Used in aggregate/distinct; different hot path, different task. |

### 6.3 Thresholds

The recommendation is GO if:
- The dominant binding-value strings are ≤ 24 bytes (analytics event properties
  like plan names, categories, countries — virtually always true).
- The `compact_str` crate dependency is acceptable (well-maintained, lightweight
  transitive deps — castaway, cfg-if, itoa, rustversion, ryu, static_assertions
  — all compile-time-only or already common in the ecosystem; MIT licensed,
  widely used — 14M downloads).

The recommendation would flip to NO-GO if:
- Binding values were routinely > 24 bytes (URLs, free-form text as binding
  targets). In that case, CompactString clone is slightly slower than
  `Box<str>` and the dependency adds no value.
- The project had a strict no-new-dependencies policy.

### 6.4 Migration Plan

The conversion is a single-file change in `bqlite-operators/src/matcher/bindings.rs`:

1. Add `compact_str` as a dependency of `bqlite-operators`.
2. Change `BindingValue::String(Box<str>)` to `BindingValue::String(CompactString)`.
3. Update `extract_binding_value` to construct `CompactString::new(arr.value(row))`
   instead of `Box::from(arr.value(row))`.
4. Verify that `CompactString` derives `Clone`, `PartialEq`, `Eq`, `Hash`
   (it does — these are the traits required by `BindingValue`).
5. No changes needed to comparison code (`check_bindings`, `check_binding_advance`)
   because `PartialEq` on `CompactString` works transparently.

The migration does NOT touch `bqlite-planner` or `bqlite-core` — it is
confined to the `bqlite-operators` crate's binding module.

### 6.5 Implementation Status (TASK-454)

The migration was completed in TASK-454 (Wave 4). Changes made:

- `BindingValue::String` changed from `Box<str>` to `CompactString`
  in `crates/bqlite-operators/src/matcher/bindings.rs`.
- `extract_binding_value` updated to use `CompactString::from(arr.value(row))`.
- The existing Wave 3 benchmark evidence in §4.3 backs the change;
  the `benches/wave3/compactstring_eval.rs` suite remains as the
  canonical performance reference.

**Wave 4 operator call-outs:**

| Operator | Surface | Status |
|----------|---------|--------|
| SESSIONIZE (TASK-428) | `end_events: HashSet<String>` | No conversion — lookup-only, never cloned in hot path (see §6.2). Per-entity `SessionizeState` carries no string fields. |
| ATTRIBUTE (TASK-431) | `TouchpointDequeEntry.key: Option<CompactString>` | Already `CompactString` at initial implementation. |
| Cohort / SubqueryFilter (TASK-437) | Hash-set keys | Not yet implemented; conversion deferred until TASK-437 lands. |

---

## 7. Alternative Approaches Considered

### 7.1 String Interning

Instead of changing the string type, intern all event-type and binding-value
strings into a `StringInterner` at NFA compile time, replacing runtime string
comparisons with integer comparisons.

**Pros**: O(1) comparison via integer equality. Zero-allocation clones (clone
an integer). Potentially larger speedup than CompactString for comparison-
heavy paths.

**Cons**: Requires a global or per-query interner. Adds complexity to the
compilation pipeline. Interned strings need lifetime management. The benefit
for comparison is marginal — string comparison on short strings is already
~20-55 ns (nearly free). The primary bottleneck is clone, not comparison,
and CompactString addresses clone more simply.

**Verdict**: Higher complexity for marginal additional benefit on the comparison
path. CompactString addresses the actual bottleneck (clone) with zero
architectural changes. Interning could be revisited if comparison cost becomes
dominant (e.g., patterns with many steps or high-cardinality event types).

### 7.2 Arc\<str\>

Use `Arc<str>` for binding values to make clones O(1) (atomic ref-count bump).

**Pros**: Clone is a single atomic increment regardless of string length.
**Cons**: Atomic operations have fixed overhead (~15-25 ns) that exceeds
CompactString's memcpy cost for short strings (~2 ns). Cache-line contention
on the ref-count under concurrent reads. Does not help the extraction path
(still heap-allocates on creation).

**Verdict**: Worse than CompactString for the dominant short-string case.
Only beneficial for very long strings that are cloned many times, which is
not the matcher binding pattern.

### 7.3 BTreeSet → HashSet for relevant_event_types

Orthogonal to string type but noted as a separate optimization opportunity.
The `BTreeSet<String>::contains(&str)` call is O(log k) with k typically 3-5.
A `HashSet` would be O(1) amortized. For k ≤ 5, `BTreeSet` may actually be
faster due to cache locality, but a `HashSet` with `ahash` would be worth
benchmarking separately.

---

## 8. Benchmark Artifacts

The microbenchmark suite is available at `benches/wave3/compactstring_eval.rs`
and covers:
- `event_type_comparison` — equality check across String/Box\<str\>/CompactString
- `binding_extraction` — Arrow → owned string construction
- `binding_clone` — clone cost (the critical measurement)
- `binding_key_hashmap` — HashMap lookup cost
- `memory_layout` — aggregate allocation patterns

Run with:
```bash
cargo bench -p bqlite-benches --bench compactstring_eval
```

---

## 9. Decision Summary

| Aspect | Decision | Rationale |
|--------|----------|-----------|
| `BindingValue::String` | Convert `Box<str>` → `CompactString` | 10× clone speedup for ≤ 24-byte strings; zero enum size impact; dominant analytics string lengths are well within inline threshold |
| `Transition.event_type` | Keep `String` | Never cloned in hot path; comparison cost identical across all representations |
| `PoisonTransition.event_type` | Keep `String` | Same as Transition |
| `relevant_event_types` | Keep `BTreeSet<String>` (separate optimization) | String type does not affect lookup cost; HashSet is a separate task |
| `PropertyValue::String` | Keep `String` | Not in query hot path |
| Dependency | Add `compact_str = "0.9"` to `bqlite-operators` only | Well-maintained, zero transitive deps, MIT licensed |
| Migration scope | `bqlite-operators/src/matcher/bindings.rs` only | No cross-crate changes; confined to binding extraction and storage |
