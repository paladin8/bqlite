# Sequence Matcher Strategy Selection

**Wave**: 3
**Task**: TASK-302
**Status**: draft
**Depends on**: match-operator.md (TASK-301), sequence-matching.md §10
**Depended on by**: TASK-305 (step counter fast path), TASK-311 (pattern compiler), TASK-325 (benchmark suite)

---

## 1. Purpose

This document specifies the compile-time classifier that selects the optimal execution strategy for a MATCH operator based on the pattern shape and downstream demand. It is the bridge between the NFA compiled by TASK-311 and the per-entity state variants defined in match-operator.md §4.

The classifier answers a single question at plan time: **given this pattern and the demand propagated from downstream operators, which execution strategy minimizes per-event cost?**

The output of classification — a `PatternClass` variant carried on the `CompiledNfa` — is a compile-time decision. There is no runtime branching between strategies. All entities within a single query use the same strategy.

---

## 2. Pattern Classification

### 2.1 PatternClass Enum

The `PatternClass` enum lives in `bqlite-planner` (on `CompiledNfa`, see sequence-matching.md §3.2). It encodes the structural shape of the pattern independently of match mode and demand:

```rust
/// Compile-time classification of a MATCH pattern's structural shape.
///
/// Determined by [`classify_pattern`] during NFA compilation (TASK-311).
/// Carried on [`CompiledNfa::pattern_class`] and read by the physical
/// planner to select the execution strategy (§3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PatternClass {
    /// Ordered steps, no negation, no repetition, no variable bindings,
    /// no alternation within steps. The simplest and fastest path.
    LinearSimple,

    /// Linear with one or more `IMMEDIATELY` transitions between steps.
    /// Compiles to a dedicated consecutive matcher instead of the
    /// general non-consecutive NFA or step counter.
    LinearImmediate,

    /// Linear with poison transitions (WITHOUT clauses) but no
    /// repetition, no variable bindings, no alternation.
    LinearWithNegation,

    /// Linear with variable bindings ($var) but no negation, no
    /// repetition, no alternation.
    LinearWithBindings,

    /// Linear with both negation and variable bindings. The most
    /// complex linear variant that still qualifies for the step counter.
    LinearFull,

    /// Contains branching (alternation within a step), repetition
    /// (`+`/`*`), or any structural feature that prevents linearization.
    /// Requires the general NFA simulator.
    GeneralNfa,
}
```

### 2.2 Classification Predicates

The classifier inspects the `MatchPattern` AST and the compiled NFA graph to determine the pattern class. Classification runs as step 8 of the compilation pipeline (sequence-matching.md §14.2).

The predicates are evaluated in order — the first match wins:

| Predicate | PatternClass |
|---|---|
| Any step has `immediately_next: true` | `LinearImmediate` |
| Any step has `repetition: Some(_)` | `GeneralNfa` |
| Any step has `StepEvent::Alternation(_)` | `GeneralNfa` |
| NFA graph has any state with >1 forward transition to distinct target states | `GeneralNfa` |
| Has negation AND has variable bindings | `LinearFull` |
| Has negation (any step has `without_next: Some(_)`) | `LinearWithNegation` |
| Has variable bindings (any step predicate references `Expr::Variable`) | `LinearWithBindings` |
| None of the above | `LinearSimple` |

**"Linear" means**: the NFA graph is a straight chain of states — each state has exactly one forward transition to a unique next state (plus optional poison transitions). There are no self-loops, no branching, and no epsilon transitions remaining after compilation.

**Single-event pattern bypass** (sequence-matching.md §10.4): A pattern with exactly one step and no negation, repetition, or bindings is classified as `LinearSimple`. However, the physical planner detects single-step `LinearSimple` patterns and rewrites them to a `FilterPhysical` descriptor instead of instantiating the MATCH operator at all. This bypass is a planner optimization, not a `PatternClass` variant — the classifier does not need to distinguish single-step patterns.

### 2.3 Classification Algorithm

```rust
/// Classify a compiled pattern for strategy selection.
///
/// Called during NFA compilation (TASK-311 step 8) after the NFA graph
/// is fully constructed, epsilon-free, and optimized.
pub fn classify_pattern(
    pattern: &MatchPattern,
    nfa: &NfaGraph,
) -> PatternClass {
    // 1. Check for IMMEDIATELY transitions.
    let has_immediately = pattern.steps.iter().any(|s| s.immediately_next);
    if has_immediately {
        return PatternClass::LinearImmediate;
    }

    // 2. Check for repetition (forces NFA).
    let has_repetition = pattern.steps.iter().any(|s| s.repetition.is_some());
    if has_repetition {
        return PatternClass::GeneralNfa;
    }

    // 3. Check for alternation within steps (forces NFA).
    let has_alternation = pattern.steps.iter().any(|s| {
        matches!(s.event, StepEvent::Alternation(_))
    });
    if has_alternation {
        return PatternClass::GeneralNfa;
    }

    // 4. Check NFA graph for structural branching (safety net).
    //    Even if the AST looks linear, confirm the compiled graph is
    //    a straight chain. This catches edge cases where Thompson's
    //    construction introduces branching not visible in the AST.
    if nfa_has_branching(nfa) {
        return PatternClass::GeneralNfa;
    }

    // 5. Classify among linear variants.
    let has_negation = pattern.steps.iter().any(|s| s.without_next.is_some());
    let has_bindings = pattern_has_variable_bindings(pattern);

    match (has_negation, has_bindings) {
        (true, true) => PatternClass::LinearFull,
        (true, false) => PatternClass::LinearWithNegation,
        (false, true) => PatternClass::LinearWithBindings,
        (false, false) => PatternClass::LinearSimple,
    }
}

/// Returns true if any NFA state has forward transitions to more than
/// one distinct target state (branching) or has self-loops (repetition).
///
/// **Important**: only forward (matching) transitions are inspected.
/// Poison transitions (from WITHOUT/negation clauses) are excluded —
/// they do not create structural branching in the NFA graph. A state
/// with one forward transition and one poison transition is linear.
fn nfa_has_branching(nfa: &NfaGraph) -> bool {
    for state in &nfa.states {
        // Only consider forward transitions, NOT poison_transitions.
        let distinct_targets: HashSet<u16> = state
            .transitions  // forward transitions only
            .iter()
            .map(|t| t.target)
            .collect();
        if distinct_targets.len() > 1 {
            return true;
        }
        // Self-loops (target == current state) indicate repetition.
        if state.transitions.iter().any(|t| t.target == state.id) {
            return true;
        }
    }
    false
}
```

### 2.4 Variable Binding Detection

Variable bindings are detected by walking the step predicates for `Expr::Variable` references. This is a syntactic check on the AST, not a semantic one — the planner has already validated that all variable references are correctly scoped (sequence-matching.md §14.2, step 3).

```rust
fn pattern_has_variable_bindings(pattern: &MatchPattern) -> bool {
    pattern.steps.iter().any(|s| {
        s.predicate
            .as_ref()
            .map_or(false, |p| expr_contains_variable(&p.node))
    })
}

/// Recursively check whether an expression tree contains any
/// `Expr::Variable` reference.
///
/// Must cover all recursive `Expr` variants from `bqlite-ast`.
/// The `_ => false` fallback catches leaf variants that cannot
/// contain variables (Literal, Column, Qualified).
fn expr_contains_variable(expr: &Expr) -> bool {
    match expr {
        Expr::Variable(_) => true,
        Expr::Binary { left, right, .. }
        | Expr::Compare { left, right, .. } => {
            expr_contains_variable(&left.node)
                || expr_contains_variable(&right.node)
        }
        Expr::Unary { operand, .. } => expr_contains_variable(&operand.node),
        Expr::Not(inner) => expr_contains_variable(&inner.node),
        Expr::And(exprs) | Expr::Or(exprs) => {
            exprs.iter().any(|e| expr_contains_variable(&e.node))
        }
        Expr::FunctionCall { args, .. } => {
            args.iter().any(|a| expr_contains_variable(&a.node))
        }
        Expr::IsNull { expr, .. }
        | Expr::Like { expr, .. }
        | Expr::Regex { expr, .. }
        | Expr::Contains { expr, .. }
        | Expr::Cast { expr, .. } => {
            expr_contains_variable(&expr.node)
        }
        Expr::Between { expr, low, high, .. } => {
            expr_contains_variable(&expr.node)
                || expr_contains_variable(&low.node)
                || expr_contains_variable(&high.node)
        }
        Expr::In { lhs, .. } => {
            lhs.iter().any(|e| expr_contains_variable(&e.node))
        }
        Expr::Case { arms, else_expr } => {
            arms.iter().any(|arm| {
                expr_contains_variable(&arm.when.node)
                    || expr_contains_variable(&arm.then.node)
            }) || else_expr
                .as_ref()
                .map_or(false, |e| expr_contains_variable(&e.node))
        }
        // Leaf variants: Literal, Column, Qualified — cannot contain variables.
        _ => false,
    }
}
```

### 2.5 LinearImmediate Special Case

`LinearImmediate` is checked first and is mutually exclusive with the other linear variants. A pattern with *any* `IMMEDIATELY` transition compiles to the dedicated consecutive matcher regardless of other features (negation, bindings). This is because the consecutive matcher's previous-event-slot model is fundamentally different from the step counter's candidate-deque model — mixing the two is not supported.

If a pattern combines `IMMEDIATELY` with features like negation or variable bindings, those features are handled within the consecutive matcher implementation (TASK-305). The classifier does not need to distinguish sub-variants of `LinearImmediate`.

**IMMEDIATELY + repetition/alternation**: The BQL grammar (query-language.md §4.9) does not permit `IMMEDIATELY` on a step that also carries a repetition quantifier (`+`/`*`), and alternation within a step (`(A OR B)`) combined with `IMMEDIATELY` is grammatically valid but semantically rejected by the planner (the "immediately following" constraint is ill-defined when the step can match multiple event types). If a future grammar change allows these combinations, the classifier must be updated to check for repetition and alternation before `IMMEDIATELY` — but for v1, the grammar enforcement makes this moot.

---

## 3. Strategy Selection Matrix

The `PatternClass` determines the per-entity state variant and execution kernel. The physical planner combines `PatternClass` with the downstream `DemandSet` to make the final strategy decision.

### 3.1 Full Strategy Matrix

| PatternClass | DemandSet | State Variant | Execution Kernel | Per-Event Cost |
|---|---|---|---|---|
| `LinearSimple` | Any except `match_events` | `StepCounter` | Step counter with candidate deque at step 0 | ~1–3 ns |
| `LinearSimple` | `match_events` demanded | `Nfa` | Full NFA with path tracking | ~10–30 ns |
| `LinearImmediate` | Any except `match_events` | `StepCounter` | Consecutive matcher with previous-event slot | ~1–3 ns |
| `LinearImmediate` | `match_events` demanded | `Nfa` | Full NFA with path tracking | ~10–30 ns |
| `LinearWithNegation` | Any except `match_events` | `StepCounter` | Step counter + poison flag check | ~2–5 ns |
| `LinearWithNegation` | `match_events` demanded | `Nfa` | Full NFA with path tracking | ~10–30 ns |
| `LinearWithBindings` | Any except `match_events` | `StepCounter` | Step counter per binding track | ~3–7 ns |
| `LinearWithBindings` | `match_events` demanded | `Nfa` | Full NFA with path tracking | ~10–30 ns |
| `LinearFull` | Any except `match_events` | `StepCounter` | Step counter + poison + bindings | ~3–7 ns |
| `LinearFull` | `match_events` demanded | `Nfa` | Full NFA with path tracking | ~10–30 ns |
| `GeneralNfa` | Any | `Nfa` | Full NFA with candidate propagation | ~5–15 ns |

### 3.2 Demand Override Rules

The demand set can force a more expensive strategy than the pattern class alone would require:

1. **`track_match_events: true`** forces the NFA path for all pattern classes. Match-event tracking requires per-candidate `EventRef` storage (sequence-matching.md §11.3), which the step counter does not support. This is the only demand that overrides a linear pattern class to the NFA path.

2. **`track_match_duration: true`** does NOT force the NFA path. For linear patterns, duration is `last_step_ts - anchor_ts` — both values are already tracked by `StepCounterTrack`. No additional state is needed.

3. **Step-property demand** (`step_properties` non-empty) does NOT force the NFA path. The `StepCounterTrack.retained_properties` field (match-operator.md §4.2) stores per-step property values lazily, populated when the corresponding step fires. This is compatible with the step counter.

4. **Aggregation fusion** (`fused_accumulator: Some(_)`) is orthogonal to strategy selection (sequence-matching.md §10.2). Fusion only affects what happens at match completion — it does not change the per-event inner loop. Any strategy can be fused.

### 3.3 Strategy Selection Function

The physical planner calls this function after NFA compilation and demand propagation to determine the final execution strategy:

```rust
/// The execution strategy selected by the physical planner.
///
/// Determines which state variant and execution kernel the
/// `SequenceMatchOperator` uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchStrategy {
    /// Step counter fast path for linear non-consecutive patterns.
    StepCounter,
    /// Dedicated consecutive matcher for IMMEDIATELY patterns.
    ConsecutiveMatcher,
    /// Full NFA simulator for general patterns or match-detail demand.
    FullNfa,
}

/// Select the execution strategy for a MATCH operator.
///
/// Called by the physical planner after NFA compilation and demand
/// propagation. The result determines which `SequenceMatchState` variant
/// is used for all entities.
pub fn select_strategy(
    pattern_class: PatternClass,
    exec_config: &MatchExecutionConfig,
) -> MatchStrategy {
    // Rule 1: match_events demand forces NFA for all pattern classes.
    if exec_config.track_match_events {
        return MatchStrategy::FullNfa;
    }

    // Rule 2: GeneralNfa always uses the NFA path.
    if pattern_class == PatternClass::GeneralNfa {
        return MatchStrategy::FullNfa;
    }

    // Rule 3: LinearImmediate uses the consecutive matcher.
    if pattern_class == PatternClass::LinearImmediate {
        return MatchStrategy::ConsecutiveMatcher;
    }

    // Rule 4: All other linear variants use the step counter.
    MatchStrategy::StepCounter
}
```

### 3.4 State Variant Mapping

The `MatchStrategy` maps to the `SequenceMatchState` variants defined in match-operator.md §4.1:

| MatchStrategy | SequenceMatchState Variant |
|---|---|
| `StepCounter` | `StepCounter(StepCounterState)` |
| `ConsecutiveMatcher` | `StepCounter(StepCounterState)` (using the consecutive sub-variant) |
| `FullNfa` | `Nfa(NfaEntityState)` |

The `ConsecutiveMatcher` strategy reuses the `StepCounterState` type but with a different execution kernel in `process_sub_batch`. The consecutive matcher keeps only the previous filtered event (per binding track) and advances the pattern iff the current event immediately follows the prior matched step. The `step0_candidates` deque is not used — consecutive matching has no window-rebinding problem since there is no gap between steps.

---

## 4. Variable-Binding Interaction with Step Counter

### 4.1 Binding Support in StepCounterTrack

The step counter supports variable bindings natively via `StepCounterTrack.bindings` (match-operator.md §4.2). Each distinct binding-value combination creates an independent `StepCounterTrack`. The step counter state manages multiple tracks in `SmallVec<[StepCounterTrack; 4]>`.

**Binding workflow in the step counter path:**

1. **Step 1 match**: Extract binding values from the event. Look up existing tracks by binding values. If no track exists for this binding combination, create a new `StepCounterTrack` with `bindings` populated and `current_step = 1`.
2. **Subsequent steps**: For each event matching the current step's type, check the event's property values against the track's bound values. Only advance the step counter if bindings match (equality check).
3. **Track isolation**: Each track advances independently. An event that advances track A does not affect track B.

**Binding lookup cost**: Track lookup is O(T) where T is the number of active tracks per entity. For the common case (1–4 tracks), `SmallVec` linear scan is faster than a HashMap due to cache locality. For entities with many distinct binding values (>16 tracks), the linear scan degrades. The active-state cap (match-operator.md §8, default 10,000 candidates) bounds the total candidate count across all tracks, which indirectly limits T for most workloads.

### 4.2 Binding Equivalence

The step counter uses `BindingValue` (match-operator.md §4.4) for binding comparisons. Two tracks are considered to have the same binding if all their `BindingValue` entries are equal (using the `PartialEq` impl). `BindingValue::Float` uses `FloatOrd<f64>` for deterministic NaN handling.

### 4.3 When Bindings Force NFA

Variable bindings alone never force the NFA path. The step counter handles bindings via per-track isolation. The NFA path is only required when:
- The pattern has structural features that prevent linearization (repetition, alternation), OR
- Downstream demand requires `match_events` tracking.

---

## 5. Fallback Behavior

### 5.1 Unsupported Predicate Fallback

Step predicates (`WHERE` clauses on individual steps) are evaluated by the execution kernel, not by the classifier. The classifier does not inspect predicate complexity — it only looks at pattern structure (negation, bindings, repetition, alternation, IMMEDIATELY).

If a step-counter-eligible pattern has predicates that the step counter kernel cannot evaluate at runtime (a scenario not expected in v1, since all `CompiledExpr` predicates are supported by both kernels), the fallback is:

1. The step counter kernel encounters an unsupported predicate during `process_sub_batch`.
2. It returns a typed error: `MatchError::UnsupportedPredicate { step_index, predicate_desc }`.
3. The engine surfaces this as a query error. There is no silent degradation or automatic re-planning.

This is a defensive fallback, not an expected code path. Both the step counter and NFA kernels support the same set of `CompiledExpr` predicates. The fallback exists to prevent silent correctness bugs if a future predicate type is added to the NFA kernel but not the step counter.

### 5.2 Classification Conservatism

The classifier is deliberately conservative — it will classify a pattern as `GeneralNfa` when in doubt rather than risk an incorrect step-counter execution. The NFA graph inspection (§2.3, step 4) acts as a safety net: even if the AST-level checks pass, a graph with unexpected branching is caught.

If a pattern is misclassified as `GeneralNfa` when it could have been linear, the result is a slower but correct execution. If a pattern were misclassified as linear when it requires the NFA, the result would be incorrect matches. The classifier always errs toward correctness.

### 5.3 Single-Step Pattern Bypass

When the physical planner detects a `LinearSimple` pattern with exactly one step and no time window, it rewrites the plan node from `SequenceMatchPhysical` to `FilterPhysical`. This avoids constructing the `SequenceMatchOperator` entirely. The bypass is implemented in the physical planner (TASK-309), not in the classifier.

Detection criteria:
- `pattern_class == PatternClass::LinearSimple`
- `pattern.steps.len() == 1`
- `pattern.window.is_none()`
- No variable bindings (guaranteed by `LinearSimple`)
- No EMIT ALL (EMIT ALL requires step-reached tracking, which a filter cannot provide)

---

## 6. Match Mode Interaction

Match mode (`MatchMode::First`, `MatchMode::All`) and the separate
`emit_all` flag do not affect pattern classification — the `PatternClass` is
determined purely by pattern structure. These flags affect the execution
kernel's behavior within the selected strategy:

| Mode / Flag | Step Counter Behavior | NFA Behavior |
|---|---|---|
| `First` | Stop track on first accept. Entity-level early-exit if single track. | Stop track on first accept. |
| `All` | On accept, consume the participating entry, set `scan_from = anchor_ts`, and keep later unmatched anchors alive. | On accept, prune candidates that share the consumed events and keep later unmatched entries alive. |
| `emit_all = true` | On window expiry / entity end, emit the farthest partial for the track/entry. In the fused path, increment `step_counts[current_step]`. | On window expiry / entity end, emit the farthest partial for each surviving track/entry. |

EMIT ALL is orthogonal to pattern classification. A `LinearSimple` pattern with EMIT ALL still uses the step counter — the additional work (tracking `max_step_reached`, emitting partial results at entity end) is handled within the step counter kernel without requiring the NFA.

---

## 7. Demand Propagation Integration

### 7.1 DemandSet to Strategy

The physical planner constructs a `MatchExecutionConfig` from the backward-propagated `DemandSet` (planner-pipeline.md §9.3). The relevant fields for strategy selection:

```rust
pub struct MatchExecutionConfig {
    pub track_match_duration: bool,   // does NOT force NFA
    pub track_match_events: bool,     // forces NFA for all linear classes
    pub step_properties: Vec<StepPropertyExtraction>,  // does NOT force NFA
    pub fused_accumulator: Option<Box<dyn Accumulator>>,  // orthogonal
}
```

Only `track_match_events` affects strategy selection. All other demand fields are satisfied by both the step counter and NFA paths.

### 7.2 Demand Escalation Path

When a downstream operator demands `match_events`, the strategy escalates from step counter to full NFA. The escalation path:

1. Physical planner propagates `DemandSet` backward through the plan tree.
2. `DemandSet` reaches the MATCH operator node.
3. `select_strategy(pattern_class, &exec_config)` returns `MatchStrategy::FullNfa` because `exec_config.track_match_events == true`.
4. `SequenceMatchOperator` is constructed with the NFA state variant.
5. Per-entity state includes `FullNfaInstance` with `match_trace: Vec<EventRef>` (sequence-matching.md §11.3).

The escalation is transparent — the operator's output schema is the same regardless of strategy. Only the internal execution cost changes.

### 7.3 No Demand De-escalation

There is no mechanism to de-escalate from NFA to step counter at runtime. If the planner selects the NFA path, all entities use the NFA path for the entire query. This is by design — mixed strategies within a query would complicate the operator's `process_sub_batch` hot loop with runtime branches.

---

## 8. Microbenchmark Methodology

TASK-325 (Wave 3 benchmark suite) validates the per-strategy performance expectations from the strategy matrix (§3.1). This section specifies the benchmark methodology.

### 8.1 Benchmark Scenarios

Each scenario isolates one strategy and measures per-event throughput:

| Scenario | PatternClass | Steps | Features | Expected Cost |
|---|---|---|---|---|
| `bench_linear_simple_3step` | `LinearSimple` | 3 | None | ~1–3 ns/event |
| `bench_linear_simple_5step` | `LinearSimple` | 5 | None | ~1–3 ns/event |
| `bench_linear_immediate_3step` | `LinearImmediate` | 3 | IMMEDIATELY | ~1–3 ns/event |
| `bench_linear_negation_3step` | `LinearWithNegation` | 3 | 1 WITHOUT clause | ~2–5 ns/event |
| `bench_linear_bindings_3step` | `LinearWithBindings` | 3 | 1 variable, 4 distinct values | ~3–7 ns/event |
| `bench_linear_full_3step` | `LinearFull` | 3 | negation + bindings | ~3–7 ns/event |
| `bench_general_nfa_3step` | `GeneralNfa` | 3 | 1 alternation | ~5–15 ns/event |
| `bench_general_nfa_repetition` | `GeneralNfa` | 3 | 1 `B+` repetition | ~5–15 ns/event |
| `bench_nfa_match_events` | `LinearSimple` (escalated) | 3 | `match_events` demanded | ~10–30 ns/event |

### 8.2 Data Generation

Each benchmark generates a synthetic entity-sorted event stream using the property-test generators from `tests/src/strategies.rs`:

- **Entity count**: 10,000 entities.
- **Events per entity**: 100–1,000 (uniformly distributed).
- **Event types**: 5–10 distinct types, with the pattern's target types appearing at configurable frequency (default 10% each).
- **Match rate**: ~30% of entities should complete the full pattern (tuned via event-type frequency and window size). This avoids benchmarking only the fast-path (no match) or only the slow path (every entity matches).
- **Binding cardinality**: For binding benchmarks, 4 distinct binding values per entity (uniformly distributed).
- **Window**: 80% of entity event span (ensures some window expirations).

### 8.3 Measurement Protocol

1. **Warm-up**: Criterion.rs default (3 seconds).
2. **Metric**: Wall-clock time per event = total benchmark time / (entity_count × avg_events_per_entity).
3. **Baseline comparison**: Each strategy benchmark reports absolute ns/event and relative throughput vs. the `bench_linear_simple_3step` baseline.
4. **Regression threshold**: Any strategy exceeding 2× its expected cost range (from §3.1) is flagged as a regression. TASK-325 should encode these thresholds as assertion comments in the benchmark harness.

### 8.4 Wave 3 Acceptance Criterion

From the Wave 3 acceptance criteria: "The step-counter fast path benchmarks within 2× of the Wave 2 scan-only baseline on the same dataset." This is measured by:

1. Run the Wave 2 scan-only benchmark on the same synthetic dataset (scan + filter, no MATCH).
2. Run `bench_linear_simple_3step` on the same dataset.
3. Assert that the step-counter benchmark's per-event cost is ≤ 2× the scan-only baseline.

This validates that the step counter adds minimal overhead on top of the scan path — the dominant cost should be I/O and column decoding, not pattern matching.

### 8.5 Strategy Isolation

Each benchmark must verify that the expected strategy was selected by asserting the `PatternClass` of the compiled pattern:

```rust
let compiled = compile_pattern(&pattern, &schema);
assert_eq!(compiled.pattern_class, PatternClass::LinearSimple);
// ... run benchmark ...
```

This prevents benchmark rot — if a future change to the classifier reclassifies a benchmark pattern, the assertion catches it immediately rather than silently benchmarking the wrong strategy.

---

## 9. Crate Placement

| Type | Crate | Rationale |
|---|---|---|
| `PatternClass` | `bqlite-planner` | Plan-time classification; carried on `CompiledNfa` |
| `MatchStrategy` | `bqlite-planner` | Plan-time decision; used by physical planner |
| `classify_pattern()` | `bqlite-planner` | Called during NFA compilation (TASK-311) |
| `select_strategy()` | `bqlite-planner` | Called by physical planner (TASK-309) |
| `StepCounterState` | `bqlite-operators` | Execution-time state; implements `EntityOperator` |
| `NfaEntityState` | `bqlite-operators` | Execution-time state; implements `EntityOperator` |

The classifier and strategy selector live in `bqlite-planner` because they are compile-time / plan-time decisions. The execution state types live in `bqlite-operators` because they are used only at runtime. This respects the dependency direction: `bqlite-operators → bqlite-planner`.

---

## 10. Decision Summary

| Aspect | Decision | Rationale |
|---|---|---|
| Classification granularity | 6 variants (5 linear + 1 general) | Each linear variant maps to a distinct step-counter configuration; collapsing them would lose optimization opportunities |
| IMMEDIATELY handling | Separate `LinearImmediate` class, checked first | Consecutive matching is architecturally distinct from non-consecutive step counting |
| Variable bindings in step counter | Supported via per-track isolation | Bindings are the most common "complex" feature in funnel queries; falling back to NFA for bindings would forfeit the fast path for the majority of production queries |
| Demand override | Only `match_events` forces NFA | `match_duration` and step properties are computable from step-counter state; no need to escalate |
| Classification conservatism | Prefer `GeneralNfa` over incorrect linear classification | Correctness over performance; the NFA path is always correct, the step counter requires linear structure |
| Fallback on unsupported predicates | Typed error, no silent degradation | Both kernels support the same predicates today; the fallback is a safety net for future extensibility |
| Benchmark methodology | Per-strategy isolation with classification assertions | Prevents benchmark rot and ensures each strategy is measured independently |
| Single-step bypass | Planner rewrite to filter, not a PatternClass variant | The bypass is a plan-tree optimization, not a pattern property |
