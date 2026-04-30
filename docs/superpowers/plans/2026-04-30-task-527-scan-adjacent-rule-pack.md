# TASK-527: Scan-adjacent optimizer rule pack — Implementation Plan

**Owner**: agent-1
**Branch**: `task/TASK-527`
**Authoritative spec**: `docs/design/planner/optimizer-direction.md` (esp. §7 rows 7/8/10/11 and §10.3)
**Reconciled spec**: `docs/design/planner-pipeline.md` §6.2 (Pass numbering)
**Framework**: TASK-521 (`crates/bqlite-planner/src/opt/registry.rs`, `rules.rs`, `stats.rs`)

## 1. Scope

Per `optimizer-direction.md` §10.3, TASK-527 owns four Wave 5 plan-time optimizer passes:

| Pass | Owner | Status today |
|------|-------|--------------|
| 6.5 — Tier-3 value-set predicate-shape gating | TASK-527 | Stub (`Tier3PredicateShapeRule` in `rules.rs`) — probes empty key, always `Skipped`. |
| 7 — MATCH anchor presence-bitmap pushdown | TASK-527 | Stub (`MatchAnchorPresenceRule` in `rules.rs`) — same. |
| 9 — Stateless filter ordering inside fused segment | TASK-527 | Not implemented. |
| 10 — Scan-pushdown filter coalescing | TASK-527 | Not implemented. |

The framework that drives these (rule trait, registry, `RuleTrace`, `PlannerStatsView`, budget enforcement, EXPLAIN integration) is already merged. This plan delivers the **rule bodies** plus tests; it does not modify the framework.

### Out of scope

- Cohort/entity pushdown (Pass 8) — owned by TASK-522.
- Wider EXPLAIN refactor — TASK-527 only needs to register one new line per new rule, the format is fixed.
- Selection-vector materialization decisions — runtime, not plan-time (`optimizer-direction.md` §5.3).
- Per-column selectivity / NDV / histograms — explicitly forbidden (§4.8, §10.3).
- Adding new fields to `ScanPhysical` for index-registry markers. Tier-2 / Tier-3 plan-time work is "ensure the predicate is in pushable shape and the column is registered"; the runtime decides whether to consult the bitmap. No new physical-plan fields needed for this task.

## 2. Design notes

### 2.1 Pass 9 — stateless filter conjunct ordering

Per `optimizer-direction.md` §7 row 10:

> Equality literals first, then small `IN` sets, then range comparisons, then `LIKE`/regex, then arbitrary scalar functions. Tie-break by source order.

A `FusedSegmentStep::Filter` carries one `CompiledExpr`. When it is a top-level `CompiledNode::And`, its operands are the conjuncts to order. When it is a single conjunct, there is nothing to do.

**Predicate categories** (lower = cheaper, runs first):

| Rank | CompiledNode shape |
|------|--------------------|
| 0 | `Compare { op: Equal/NotEqual, Column vs Literal }` (incl. `Literal vs Column`) |
| 1 | `IsNull { Column, .. }` (cheap null mask) |
| 2 | `InLiteralSet { Column, .. }` with ≤ 8 literals ("small IN") |
| 3 | `Compare { op: Less*/Greater*, Column vs Literal }` |
| 4 | `InLiteralSet { Column, .. }` with > 8 literals |
| 5 | `FunctionCall { signature == "like" }` and other regex-shaped function calls |
| 6 | Everything else (function calls, column-vs-column compares, arithmetic-bearing compares, nested logical ops) |

The `IN` size threshold of 8 is a rule of thumb mirroring storage's dictionary-bitset path (`storage/predicate-pushdown.md` — small `IN`s are linear scans of the literal list per row, cheap; big `IN`s require a hash lookup that pays off only when the filter is selective). It is hard-coded inside the rule, not exposed as a tuning knob — `optimizer-direction.md` §10.4 keeps tuning constants in code.

**Sort stability matters.** Sort by `(rank, original_index)` to preserve source order on ties. Use `sort_by_key` (stable since Rust 1.x) on the original-index pair.

**Where to apply.** Any `FusedSegmentStep::Filter` in any `FusedSegmentPhysical`, regardless of position in the `steps` Vec. Order within a single `And` predicate; do not move conjuncts between Filter steps (those would have been collapsed already by upstream lowering and have semantically distinct positions if they are split apart).

**Scope of recursion.** The rule reorders only the **top-level** conjuncts of a **top-level `CompiledNode::And`** inside a `Filter` step's predicate. It does **not** recurse into `Or` / `Not` / nested `And`s. A `Filter { predicate: Or(a, b) }` is left alone (no top-level `And`). A `Filter { predicate: And(a, Or(b, c), d) }` reorders the three top-level conjuncts but does not look inside the `Or`. This matches `optimizer-direction.md` §7 row 10's "equality literals first…tie-break by source order" — about top-level conjunct order — and is the spec.

**Determinism.** The ordering is data-independent: it reads only the `CompiledNode` discriminant and operand shapes. Two compilations of the same plan produce the same order. No `PlannerStats` access — `StatsBudget::none()`.

### 2.2 Pass 10 — scan-pushdown filter coalescing

Per `optimizer-direction.md` §7 row 11:

> Reduces multiple equivalent `event_type IN (…)` clauses to one, dedupes property predicates after MATCH extraction, and unions equivalent zone-map-acceptable predicates. Pure structural.

The input is `ScanPhysical.scan_predicates: Vec<CompiledExpr>` populated by Pass 2 (predicate pushdown) and Pass 3 (MATCH extraction). After both passes run there can be:

1. Two structurally equal `CompiledExpr` conjuncts (e.g. `event_type = 'signup'` from both the user `WHERE` and MATCH extraction). Dedup by `==` equality on `CompiledExpr` (already derives `PartialEq` per the existing pushdown tests).
2. Two `InLiteralSet { Column { col: X }, negated: false, values: A }` and `... values: B }` over the same column — union into one conjunct with `A ∪ B`.
3. (Out of scope for v1) `event_type = 'a'` plus `event_type IN ('b', 'c')` could be coalesced into `event_type IN ('a', 'b', 'c')`. Skip for now; `optimizer-direction.md` lists "equivalent" merges only, and Pass 3 already emits MATCH event types as a single `IN` clause. If a real query produces this shape it stays correct (just not coalesced).

**Algorithm.**

```
walk plan; for every ScanPhysical:
  let mut seen: Vec<CompiledExpr> = Vec::new();
  let mut in_sets: HashMap<ColumnIndex, (negated, BTreeSet<PropertyValue>, kernel)> = …;
  for conjunct in scan.scan_predicates.drain(..):
    if let Some(InLiteralSet { input: Column{i}, negated: false, values, kernel }) = conjunct.node:
      merge values into in_sets[i] (only when kernel matches; otherwise keep both)
    else if seen.iter().any(|s| s == &conjunct):
      drop
    else:
      seen.push(conjunct)
  rebuild seen from {seen + flushed in_sets}
  scan.scan_predicates = seen
```

**Subtlety.** The dedup is structural — two `CompiledExpr` are duplicates iff their `CompiledNode` trees are equal. The Wave 0 `CompiledExpr` derives `PartialEq` already (verified via the predicate-pushdown tests at `crates/bqlite-planner/src/opt/pushdown.rs:419` and elsewhere). No deep canonicalization (no commutativity-aware sort) — that would be a bigger task and the spec says "equivalent", not "semantically equivalent".

**InLiteralSet merging.** Only union sets that share `(column_index: usize, negated: bool, kernel: InSetKernel)` — the grouping key contains no `PropertyValue` (`PropertyValue` is `Eq + Ord` but **not `Hash`**). `PropertyValue` ordering is total for every literal type. The merged `values` Vec is built from a `BTreeSet<PropertyValue>` for deterministic ordering.

**Float / NaN behavior** *(amended during CP2 implementation, 2026-04-30)*: an earlier draft of this plan special-cased `Float` to skip both dedup and union under the assumption that `NaN ≠ NaN` would corrupt the BTreeSet. Verification of `crates/bqlite-core/src/property.rs:237` and `:287` shows that bqlite intentionally uses `f64::total_cmp` for both `PartialEq` and `Ord` on `PropertyValue::Float` — `Float(NaN) == Float(NaN)` is true under bqlite's total ordering, and `BTreeSet<PropertyValue>` is well-defined for any Float value. The implementation therefore performs Float merge / dedup like every other type. Tests `float_nan_equality_predicates_are_deduplicated` and `float_in_set_with_nan_is_unioned_under_total_ordering` lock in the behavior.

**Determinism.** Output ordering: dedup-survivors in first-seen order, with merged `InLiteralSet` conjuncts placed at the position of their first occurrence. No `PlannerStats` access — `StatsBudget::none()`.

### 2.3 Pass 6.5 — Tier-3 predicate-shape probe

Per `optimizer-direction.md` §7 row 7, the rule "ensures the predicate reaches the scan in a shape the scan can intersect against the index (i.e. an `IN` set or equality literal)". Predicate pushdown (Pass 2) and Pass 10 (above) already produce that shape. The rule's runtime work is therefore:

1. Walk the plan, find every `ScanPhysical`.
2. For every conjunct in `scan.scan_predicates` whose shape is `Compare { Column = Literal }` or `InLiteralSet { Column, … }`, look up `value_set_indexed(scan.table, column_name)` from the registry.
3. If any conjunct matches a registered (table, column) pair, the rule reports `Applied` and (for now) produces an unchanged plan — the runtime already consults the registry per `optimizer-direction.md` §4.4.
4. If no match, report `Skipped("no value-set indexes registered for any pushed column")`.

This converts the existing "always-skipped, no probe" stub into an actual structural probe. Until TASK-435/447 populate the registry, the registry is empty and behavior is unchanged. Once it is populated, EXPLAIN starts showing `Applied` lines for the gate, which is what `optimizer-direction.md` §8.2 requires.

**No new ScanPhysical field.** The runtime scan operator independently consults the manifest's index registry per `storage-format.md` §11.2.3. The plan-time job is to verify the shape is right (which Passes 2 + 10 already deliver) and to make the gate visible in EXPLAIN.

### 2.4 Pass 7 — MATCH anchor presence-bitmap probe

Per `optimizer-direction.md` §7 row 8, the rule "marks `ScanPhysical` to apply Tier-2 row-group pruning at runtime". Same shape as Pass 6.5:

1. Walk the plan, find every `SequenceMatch(input = Scan)` (or `SequenceMatch(input = FusedSegment(_, input = Scan))` — the segment may sit between).
2. Read `compiled_nfa.relevant_event_types` (already a deduplicated `BTreeSet<String>` per `crates/bqlite-planner/src/compile.rs:45`). For Wave 5 v1, the **anchor** is the entire set — `entity_presence_indexed` is per `(table, anchor_event_type)`, so consult each event type and report `Applied` if any match.
3. If any anchor event type matches a registered `(scan.table, anchor)` pair, report `Applied`.
4. Otherwise report `Skipped("no entity-presence bitmaps registered for any MATCH anchor type")`.

**Why "any" rather than "first step"?** The MATCH operator can prune row-groups that have *none* of the events the NFA cares about. The Tier-2 bitmap is keyed on `(table, anchor_event_type)`, and storage-format.md §11.2.2 envisions one bitmap per event type so the scan can intersect them. Using the relevant-event-types set keeps the rule's gate aligned with the runtime's actual pruning capability. This also matches `optimizer-direction.md` §4.4, which calls the second key `anchor_event_type` (singular per registry entry) — the rule iterates over each event the MATCH cares about.

**No new ScanPhysical field.** Same reasoning as Pass 6.5: the runtime already knows it is a MATCH scan (it has the `CompiledNfa`), so it can intersect bitmaps without a planner-side flag. The plan-time job is the visibility/gate.

## 3. Checkpoints

Each checkpoint is mergeable independently, passes `scripts/local-ci.sh`, and is reviewed by a subagent before commit. Each checkpoint is a "new file only" addition (or a strict edit inside `rules.rs`); no shared-file changes.

### CP1 — Pass 9: stateless filter conjunct ordering

**Files**:

- `crates/bqlite-planner/src/opt/filter_order.rs` (new)
- `crates/bqlite-planner/src/opt/mod.rs` (add `pub mod filter_order;` and re-export)
- `crates/bqlite-planner/src/opt/registry.rs` (add `StatelessFilterOrderingRule`, register in `OptimizerPipeline::v1`)
- `crates/bqlite-planner/src/opt/registry.rs` tests: extend `v1_registers_…` count (now 7 rules) and add the new rule id to the assertion vec
- `crates/bqlite-planner/src/lib.rs` and `crates/bqlite-planner/src/explain.rs` if either references the rule list literal (verify; the v1 pipeline already covers it)

**Pre-CP1 audit**: search the workspace for assertions on conjunct order in EXPLAIN snapshot tests and predicate-pushdown tests. Document anything that breaks before writing the rule. The risk register flagged this; promoted here to a CP1 sub-task.

**Algorithm**:

1. Recursive walk over `PhysicalPlan` (mirror the structure of `pushdown.rs::pushdown_predicates`). Recurse into every node that has child plans (`FusedSegment`, `SequenceMatch`, `Aggregate`, `Sort`, `Distinct`, `Explain`).
2. At a `FusedSegment`, rebuild `steps` by mapping over each `Filter` step and replacing its predicate with `reorder_conjuncts(predicate)`.
3. `reorder_conjuncts(expr)`:
   - If `expr.node == CompiledNode::And { operands, kernel }`, sort `operands` by `(rank(operand), original_index)` using stable sort, return rebuilt `And`.
   - Otherwise return unchanged.
4. `rank(expr)` returns one of 0..7 by structural pattern match on `CompiledNode`.

**Tests** (in-file `#[cfg(test)] mod tests`):

- `pure_equality_conjuncts_are_stable` — three `col = literal` conjuncts in a given order remain in that order.
- `range_after_equality` — `[col > 0, col = 1]` reorders to `[col = 1, col > 0]`.
- `is_null_between_equality_and_range` — `[col > 0, col IS NULL, col = 1]` → `[col = 1, col IS NULL, col > 0]`.
- `small_in_before_range` — `IN(a,b)` orders before `col > 0`.
- `large_in_after_range` — `IN(1..16)` orders after `col > 0` but before `LIKE`.
- `like_after_range` — `LIKE 'foo%'` orders after `col > 0`.
- `function_call_last` — arbitrary `FunctionCall` orders last.
- `single_conjunct_is_unchanged` — a non-`And` predicate is returned by-value.
- `non_filter_steps_are_untouched` — `Project` and `Limit` survive.
- `recurses_into_sequence_match_input` — fused segment under a SequenceMatch also gets reordered.
- `recurses_into_explain` — Explain wrapper preserved, inner reordering applied.
- `idempotent` — running twice produces the same plan.
- `single_operand_and_is_a_noop` — defense against an `And { operands: [x] }` reaching the rule (shouldn't happen per `pushdown.rs:230` debug_assert, but still tested).
- `nullable_flag_preserved_on_reorder` — assert `result.nullable == input.nullable` and the `And.kernel` is unchanged.

**Determinism property test** (proptest): generate a random vector of conjuncts (each from a small enum of shapes), reorder, assert that running the rule twice is a fixpoint and that the output ranks are weakly increasing.

### CP2 — Pass 10: scan-pushdown filter coalescing

**Files**:

- `crates/bqlite-planner/src/opt/coalesce_scan_predicates.rs` (new)
- `crates/bqlite-planner/src/opt/mod.rs` (add module + re-export)
- `crates/bqlite-planner/src/opt/registry.rs` (add `ScanPredicateCoalesceRule`, register in `OptimizerPipeline::v1` *after* `tier3_predicate_shape` and `match_anchor_presence` — Pass 10 is the final plan-time pass per `optimizer-direction.md` §9). Update the rule-count test and the order assertion.

Wait — re-reading §9, the order is: 6.5, 7, 9, 10. Pass 9 is filter ordering inside fused segment, Pass 10 is scan-pushdown coalescing. Pass 9 (CP1) registers between `match_anchor_presence` and `coalesce_scan_predicates`. **Final v1 order**:

1. `fuse_match_aggregate` (existing)
2. `sample_pushdown` (existing)
3. `predicate_pushdown` (existing)
4. `projection_pruning` (existing)
5. `tier3_predicate_shape` (existing stub)
6. `match_anchor_presence` (existing stub)
7. `stateless_filter_order` (CP1 — new)
8. `coalesce_scan_predicates` (CP2 — new)

**Algorithm**:

1. Recursive walk over `PhysicalPlan`, same skeleton as `pushdown.rs`.
2. At a `ScanPhysical`, replace `scan_predicates` with `coalesce(scan_predicates)`.
3. `coalesce(preds)`:
   - First pass: dedupe by `==` equality, preserving first-seen order. Track which conjuncts are `InLiteralSet { Column, negated: false, .. }` separately.
   - For surviving `InLiteralSet`s grouped by `(column_index, negated, kernel)`, union their `values` into a `BTreeSet<PropertyValue>` (skip if any value is `Float` — NaN bites us). Replace the first occurrence in the survivor list with the merged version, drop the rest.
   - Return the rebuilt list.

**Tests**:

- `duplicate_equality_predicates_are_deduplicated`
- `duplicate_in_literal_sets_are_unioned` — `[col IN (a, b), col IN (b, c)]` → `[col IN (a, b, c)]`.
- `in_literal_sets_with_different_columns_are_not_merged`
- `in_literal_sets_with_different_negation_are_not_merged`
- `negated_in_literal_sets_are_left_alone` — only `negated: false` qualifies.
- `unrelated_conjuncts_pass_through_unchanged`
- `mixed_dedup_and_union` — full case.
- `float_nan_equality_predicates_are_deduplicated` — `[col = NaN, col = NaN]` collapses (bqlite uses `total_cmp` for `PartialEq` on Float, so `NaN == NaN` is true). See the §2.2 Float / NaN amendment.
- `float_in_set_with_nan_is_unioned_under_total_ordering` — `[col IN (NaN), col IN (NaN)]` merges into one set with a single NaN entry under `BTreeSet<PropertyValue>` total ordering.
- `bare_scan_with_no_predicates_is_unchanged`
- `walks_into_fused_segment_input`, `walks_into_sequence_match_input`, `walks_into_explain`
- `idempotent`

**Equivalence proptest** (proptest): generate random conjunct vectors restricted to dedup-eligible / union-eligible / pass-through, run coalesce twice, assert idempotence and that the surviving set is structurally a subset of the input.

### CP3 — Strengthen Pass 6.5 / Pass 7 stubs

**Files**:

- `crates/bqlite-planner/src/opt/rules.rs` (replace the body of `Tier3PredicateShapeRule::apply` and `MatchAnchorPresenceRule::apply`)

**Algorithm — Tier3PredicateShapeRule::apply**:

```
let mut applied_any = false;
walk(&plan, |node| {
    if let PhysicalPlan::Scan(scan) = node {
        for conjunct in &scan.scan_predicates {
            if let Some((col_name)) = extract_pushable_column(conjunct) {
                if ctx.stats().value_set_indexed(&scan.table, col_name) {
                    applied_any = true;
                }
            }
        }
    }
});
if !applied_any { ctx.record_skipped("no value-set indexes registered"); }
plan  // unchanged — runtime consults the index
```

`extract_pushable_column`: returns `Some(col_name)` for `Compare { Column = Literal }` and `InLiteralSet { Column, .. }` shapes; `None` otherwise. The function is local to the rule.

**Algorithm — MatchAnchorPresenceRule::apply**:

```
let mut applied_any = false;
walk(&plan, |node| {
    if let PhysicalPlan::SequenceMatch(seq_match) = node {
        let scan = unwrap_scan_under_segment(&seq_match.input)?;
        for anchor in &seq_match.compiled_nfa.relevant_event_types {
            if ctx.stats().entity_presence_indexed(&scan.table, anchor) {
                applied_any = true;
            }
        }
    }
});
if !applied_any { ctx.record_skipped("no entity-presence bitmaps registered"); }
plan  // unchanged — runtime consults the bitmap
```

`unwrap_scan_under_segment`: walks through an optional `FusedSegment` to find an underlying `ScanPhysical`. Returns `None` if the SequenceMatch's input is not anchored on a scan (e.g. nested under a Sort — not currently produced by lowering).

**Tests** (extend `rules.rs` `#[cfg(test)] mod tests`):

- `tier3_skips_with_empty_registry` (existing behavior preserved).
- `tier3_applied_when_registered_column_has_pushable_predicate` — populate `stats.value_set_indexed`, build a plan with `Scan(scan_predicates = [col = 'x'])`, run the rule, assert outcome `Applied`.
- `tier3_skips_when_registered_column_has_no_pushable_predicate` — registry has `(events, country)` but scan has only `amount > 100`; outcome `Skipped`.
- `tier3_skips_when_pushable_predicate_column_unregistered`.
- `match_anchor_skips_with_empty_registry` (preserved).
- `match_anchor_applied_when_anchor_event_type_registered` — populate `stats.entity_presence_indexed = {(events, signup): true}`, plan = `SequenceMatch(NFA touches signup) over Scan(events)`, outcome `Applied`.
- `match_anchor_skips_when_anchor_unregistered`.
- `match_anchor_handles_segment_between_scan_and_match` — `SequenceMatch over FusedSegment([Filter] over Scan)`.

**Test infrastructure**: factor a small helper `fn make_compiled_nfa_with_event_types(types: &[&str]) -> CompiledNfa` to build minimal NFAs for the anchor tests. The simpler path is to use `compile_pattern` on a tiny AST, but that pulls in parser dependencies; a hand-built `CompiledNfa { relevant_event_types: …, … }` with default everything else suffices.

**Note**: the integration test in `registry.rs` (`v1_stub_rules_report_skipped_with_empty_registry`) keeps its current expectation since the empty registry still produces `Skipped`. The trace `reason` strings stay the same.

### CP4 — Reconciliation pass + final completion

After CP3 merges, sweep:

- Re-read `docs/design/planner/optimizer-direction.md` §7 rows 7/8/10/11 and confirm every line is reflected in code.
- Re-read `docs/design/planner-pipeline.md` §6.2 and confirm the pass order in `OptimizerPipeline::v1` matches.
- Update `docs/design/planner/optimizer-direction.md` only if the implementation reveals a doc bug. The spec is mature enough that no doc changes are expected.
- Run `scripts/local-ci.sh` end-to-end one final time.
- Move the lock to `tasks/completed/TASK-527.done`, fill in `completed_at`, push.

This pass is folded into the CP3 commit if no doc updates are needed (the merge protocol still requires its own subagent review either way).

## 4. Risk register

| Risk | Mitigation |
|------|------------|
| Pass 9 reorders conjuncts in a way that changes nullability semantics. | Kleene `And` is commutative for nullability — `optimizer-direction.md` §7 row 10 specifically allows reordering. The output `And.kernel` and `nullable` flag are unchanged; only `operands` order changes. Add a unit test asserting nullability flags are preserved. |
| Pass 10 dedup uses `==` on `CompiledExpr` and the type carries floating-point literals that compare unequal under NaN. | `PropertyValue::Float(NaN)` would always compare unequal — dedup leaves NaN duplicates alone, which is correct (preserving the worst-case). Document this in the rule body. |
| Pass 9 destabilizes downstream tests that assert specific conjunct order. | Search the workspace for assertions on conjunct order before CP1. The conservative answer: existing tests construct predicates explicitly and assert by `==`; reordering only happens inside fused-segment Filter steps, and most existing tests use single conjuncts. Verified: the predicate-pushdown tests use either a single conjunct or a 2-element `And` whose order matches the post-Pass-9 order (equality conjuncts) — no breakage expected. If a test breaks, the test is the authoritative description of the expected order; fix the test. |
| `compiled_nfa.relevant_event_types` set is empty for some pattern shapes. | The set is populated for every step/negation per `compile.rs:45`. Empty-set MATCH is unreachable in Wave 5 (would require an empty pattern, which the AST forbids). If somehow empty, the iteration is a no-op and the rule reports `Skipped` — correct behavior. |
| Adding rules to `OptimizerPipeline::v1` breaks downstream EXPLAIN snapshot tests. | The EXPLAIN integration in `crates/bqlite-planner/src/explain.rs:1754` references `tier3_predicate_shape` literally; new rule ids appear there too. Audit `explain.rs` before CP1, update if it has any "expected number of rules" assertion. |

## 5. Acceptance criteria

- [ ] Four checkpoints merged to `main` in order, each passing `scripts/local-ci.sh`.
- [ ] `OptimizerPipeline::v1` registers 8 rules in the documented order.
- [ ] EXPLAIN trace shows `Applied` / `Skipped` for every new rule on at least one example query.
- [ ] Rule-trace property test (existing `v1_stub_rules_report_skipped_with_empty_registry`) updated to cover the new rules.
- [ ] No `docs/design/` drift — every line of `optimizer-direction.md` §7 rows 7/8/10/11 is reflected in code.
- [ ] `tasks/completed/TASK-527.done` exists on `origin/main` with a `completed_at` field.
