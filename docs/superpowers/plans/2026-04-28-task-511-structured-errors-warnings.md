# TASK-511: Structured Execution Errors and Warning Channel — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: `superpowers:executing-plans` (inline). Steps use `- [ ]` syntax for tracking.

**Goal:** Land the `BqliteError` variants (`Timeout`, `OperatorPanic`, `MemoryBudgetExceeded`, `MaxGroupsExceeded`), the `QueryWarning` enum, the per-query warning sink, and the `ExecutionResult.warnings` / `ExecutionFailure` API change specified by `docs/design/engine/cancellation.md` (TASK-505) §6 and §7. Replace the stringly-typed memory-budget and max-groups errors at the call sites where the doc already promises structured variants. Update `docs/design/execution-model.md` §12 to reconcile.

**Architecture:** Three additive checkpoints, each independently mergeable and reviewable. CP1 lands the new types in `bqlite-core` and the doc rewrite (zero behavior change). CP2 replaces the stringly memory-budget / max-groups errors with structured variants. CP3 wires the warning channel: `EntityOperator::take_pending_warnings`, Sessionize/Attribute/MATCH overrides, a single-threaded `WarningSink` in `bqlite-engine`, adapter forwarding, the `ExecutionResult.warnings` field, and the new `ExecutionFailure` wrapper. CLI updates render warnings after the result body.

**Tech Stack:** Rust 2021, `thiserror`, Apache Arrow, internal `bqlite_core` / `bqlite_operators` / `bqlite_engine` crates.

**Why:** Per `docs/design/engine/cancellation.md` §2 the doc rewrite of `execution-model.md` §12 cannot land without the variants — they reference `BqliteError::Timeout` and `BqliteError::OperatorPanic` which do not yet exist. TASK-511 owns landing both together. The warning channel is a hard prerequisite for surfacing Sessionize/Attribute per-entity cap diagnostics that today are recorded in operator state with nowhere to go.

**Scope qualifier:** TASK-541 (morsel scheduler) owns concurrent-worker `OperatorPanic` propagation and the per-query timer thread that fires `Timeout`. CP3 lands a single-threaded `WarningSink` shared via `Arc<Mutex<...>>`; the `OperatorPanic` and `Timeout` variants are added but only the `Engine::query` driver's `catch_unwind` path produces `OperatorPanic`. The full multi-worker drain happens in TASK-541. The project-local panic hook that captures `panic_location` (per `cancellation.md` §4.1) is *also* deferred to TASK-541 — until then `OperatorPanic.location` is always `None` and the documented behavior is "best-effort" location capture.

**`Arc<Mutex<...>>` choice (single-threaded today, multi-worker tomorrow).** The shared sink lands as `Arc<Mutex<WorkerContext>>` even though the driver is single-threaded. Alternatives: (a) thread `&mut WorkerContext` through `finalize_entity` — requires an EntityOperator trait change beyond `take_pending_warnings`, growing the API surface; (b) `Rc<RefCell<...>>` — won't work once TASK-541 adds threads. The mutex is on the cold path (only `finalize_entity`, never `process_sub_batch`), so the cost is one uncontended lock per entity boundary.

**Doc-shape compliance for `MemoryBudgetExceeded`.** `cancellation.md` §4.3 (frozen) prescribes `{ used, budget }`. We follow the doc exactly to avoid silent deviation; the requested-bytes diagnostic is preserved by the existing `BqliteError::Display` formatting at the call site, since callers can compare `used + N > budget` for the message they want.

---

## File Structure

| Path | Status | Responsibility |
|---|---|---|
| `crates/bqlite-core/src/error.rs` | modify | Add `Timeout`, `OperatorPanic`, `MemoryBudgetExceeded`, `MaxGroupsExceeded` variants. |
| `crates/bqlite-core/src/warning.rs` | create | `QueryWarning` enum (5 variants). |
| `crates/bqlite-core/src/lib.rs` | modify | Re-export `QueryWarning`. |
| `crates/bqlite-core/src/memory.rs` | modify | `budget_exceeded_error` → `MemoryBudgetExceeded { requested, used, budget }`. |
| `crates/bqlite-operators/src/distinct.rs` | modify | Use `MaxGroupsExceeded { limit }`. |
| `crates/bqlite-operators/src/aggregate/mod.rs` | modify | Two cardinality sites → `MaxGroupsExceeded { limit }`. |
| `crates/bqlite-operators/src/operator.rs` | modify | Add `EntityOperator::take_pending_warnings` default trait method. |
| `crates/bqlite-operators/src/sessionize.rs` | modify | Override `take_pending_warnings` from `cap_exceeded` flag. |
| `crates/bqlite-operators/src/attribute.rs` | modify | Override `take_pending_warnings` from `EntityCapDiagnostic`. |
| `crates/bqlite-operators/src/matcher/mod.rs` | modify | Override `take_pending_warnings` for StepCounter state (NFA paths have no cap today — return empty). Field name: `self.strategy` (not `driver`). |
| `crates/bqlite-operators/src/matcher/step_counter.rs` | modify | Add `pub fn active_state_limit(&self) -> usize` accessor on `StepCounterSimulator`. |
| `crates/bqlite-engine/src/warning_sink.rs` | create | `WarningSink` (Arc<Mutex<WorkerContext>>) + `WorkerContext`. |
| `crates/bqlite-engine/src/lib.rs` | modify | Re-export `ExecutionFailure`, `WarningSink`. |
| `crates/bqlite-engine/src/bind.rs` | modify | Thread `WarningSink` into `EntityOperatorAdapter` and `SequenceMatchAdapter`; drain warnings post-`finalize_entity`. |
| `crates/bqlite-engine/src/query.rs` | modify | `ExecutionResult.warnings`; `Engine::query` returns `Result<ExecutionResult, ExecutionFailure>`; `catch_unwind` for OperatorPanic. |
| `crates/bqlite-engine/src/render.rs` | modify | Render warning footer after the result body. |
| `crates/bqlite-engine/src/delete.rs` | modify | Wrap returned `BqliteError` in `ExecutionFailure` (no warnings recorded for DELETE). |
| `crates/bqlite-cli/src/main.rs` | modify | `run_query` adapts to `ExecutionFailure`. |
| `crates/bqlite-cli/src/ingest.rs` | modify | Adapt to `ExecutionFailure` if it calls `.query()`. |
| `crates/bqlite/src/lib.rs` | modify | Re-export new types if currently re-exporting `Engine`/`ExecutionResult`. |
| Test files (`tests/tests/wave2_acceptance.rs` etc.) | modify | Pattern-match adapters (`Err(ExecutionFailure { error, .. })`). |
| `docs/design/execution-model.md` | modify | §12 rewrite per cancellation.md. |

---

## Checkpoint 1: Core types + doc rewrite

**Goal:** Add `Timeout`, `OperatorPanic`, `MemoryBudgetExceeded`, `MaxGroupsExceeded` to `BqliteError`. Add `QueryWarning` enum module. Update `execution-model.md` §12. Zero behavior change — purely additive types and docs.

### Task 1.1 — Add four BqliteError variants

**Files:**
- Modify: `crates/bqlite-core/src/error.rs`

- [ ] **Step 1: Add the four variants to the existing enum**

Append after the `Cancelled` variant (line 57). Field layout matches `docs/design/engine/cancellation.md` §4.3 / §6.1, with `requested` added to `MemoryBudgetExceeded` for diagnostics:

```rust
    /// The query exceeded its configured timeout. Carries the elapsed
    /// time in milliseconds for diagnostics.
    ///
    /// Surfaced by the engine driver after a per-query timer fires
    /// `CancelReason::Timeout`. See `docs/design/engine/cancellation.md`
    /// §3.1 / §4.3 for the precedence rules and §6.1 for the variant's
    /// place in the reconciled error catalogue.
    #[error("query timed out after {elapsed_ms}ms")]
    Timeout { elapsed_ms: u64 },

    /// A worker (or the single-threaded driver) panicked while
    /// executing the query.
    ///
    /// `message` is the panic payload rendered as a `String`
    /// (best-effort `Display` of the `Box<dyn Any>` payload). `location`
    /// is the `file:line:column` of the panic site when a project-local
    /// panic hook captured it; `None` if the standard panic hook
    /// discarded the location before `catch_unwind` returned.
    /// See `docs/design/engine/cancellation.md` §4.
    #[error("worker panicked: {message}{}", .location.as_deref().map(|l| format!(" (at {l})")).unwrap_or_default())]
    OperatorPanic {
        message: String,
        location: Option<String>,
    },

    /// An operator's memory reservation could not be satisfied because
    /// the per-query budget would have been exceeded.
    ///
    /// `used` is the bytes already reserved against the budget at the
    /// moment the reservation failed; `budget` is the configured
    /// maximum. Shape matches `docs/design/engine/cancellation.md`
    /// §4.3 / §6.1 (frozen) — replaces the stringly-typed
    /// `BqliteError::Execution` previously returned by
    /// `bqlite_core::memory::budget_exceeded_error`.
    #[error("memory budget exceeded: {used} bytes used of {budget} byte budget")]
    MemoryBudgetExceeded { used: u64, budget: u64 },

    /// A grouping operator would have exceeded its hard cap on group
    /// cardinality.
    ///
    /// Raised by `HashAccumulator` (HashAggregate group cap) and
    /// `DistinctOperator` (distinct-row cap). See
    /// `docs/design/engine/cancellation.md` §6.1.
    #[error("group cardinality limit exceeded: {limit} groups")]
    MaxGroupsExceeded { limit: usize },
```

- [ ] **Step 2: Add unit tests for the new Display impls**

Append to the existing `#[cfg(test)] mod tests` if present, or add a new one at the bottom:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeout_display_includes_elapsed() {
        let err = BqliteError::Timeout { elapsed_ms: 12_345 };
        assert!(err.to_string().contains("12345"));
        assert!(err.to_string().contains("timed out"));
    }

    #[test]
    fn operator_panic_display_with_location() {
        let err = BqliteError::OperatorPanic {
            message: "boom".into(),
            location: Some("src/foo.rs:10:5".into()),
        };
        let s = err.to_string();
        assert!(s.contains("boom"));
        assert!(s.contains("src/foo.rs:10:5"));
    }

    #[test]
    fn operator_panic_display_without_location() {
        let err = BqliteError::OperatorPanic {
            message: "boom".into(),
            location: None,
        };
        let s = err.to_string();
        assert!(s.contains("boom"));
        assert!(!s.contains("at"));
    }

    #[test]
    fn memory_budget_exceeded_display_has_used_and_budget() {
        let err = BqliteError::MemoryBudgetExceeded {
            used: 200,
            budget: 250,
        };
        let s = err.to_string();
        assert!(s.contains("200"));
        assert!(s.contains("250"));
    }

    #[test]
    fn max_groups_exceeded_display_has_limit() {
        let err = BqliteError::MaxGroupsExceeded { limit: 1_000_000 };
        assert!(err.to_string().contains("1000000"));
    }
}
```

- [ ] **Step 3: Run tests**

```bash
cargo test -p bqlite-core --lib error::tests
```

Expected: PASS (5 new tests).

### Task 1.2 — Create QueryWarning module

**Files:**
- Create: `crates/bqlite-core/src/warning.rs`
- Modify: `crates/bqlite-core/src/lib.rs`

- [ ] **Step 1: Create `warning.rs`** with the exact enum from `cancellation.md` §7.1:

```rust
//! Per-query non-fatal diagnostics surfaced through
//! `ExecutionResult::warnings` and `ExecutionFailure::warnings`.
//!
//! Lives in `bqlite-core` so both the operator-side
//! `EntityOperator::take_pending_warnings` (in `bqlite-operators`) and
//! the engine-side `ExecutionResult` (in `bqlite-engine`) can reference
//! it without violating the dependency direction.
//!
//! See `docs/design/engine/cancellation.md` §7 for the protocol.

use std::fmt;

/// Non-fatal diagnostic surfaced after a query completes.
///
/// The enum is **exhaustive** — exhaustive matching is part of the
/// published API so callers can render every variant with full
/// context. Future operators that add a warning shape add a variant
/// here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryWarning {
    /// The entity event limit (default 10M) was reached for one entity;
    /// remaining events for that entity were dropped. See
    /// `docs/design/execution-model.md` §5.3.
    EntityEventLimitExceeded {
        entity_id: String,
        count: u64,
        limit: u64,
    },
    /// Sessionize per-entity event cap (default 1M) was reached for
    /// one entity; remaining events for that entity were dropped. See
    /// `docs/design/operators/sessionize.md` §11.3.
    SessionEventCapExceeded {
        entity_id: String,
        event_count: u64,
        cap: u64,
    },
    /// Attribute per-entity touchpoint cap was reached for one entity;
    /// remaining touchpoints for that entity were dropped. See
    /// `docs/design/operators/attribute.md` §10.
    AttributeTouchpointCapExceeded {
        entity_id: String,
        touchpoint_count: u64,
        cap: u64,
    },
    /// MATCH operator's active-state cap was reached for one entity;
    /// further state expansion was suppressed. See
    /// `docs/design/operators/match-operator.md` §13.3.
    ActiveStateLimitExceeded {
        entity_id: String,
        active_states: u64,
        cap: u64,
    },
    /// One or more workers exceeded the per-worker warning cap and
    /// silently dropped further warnings. Aggregated by the engine
    /// — the user sees a single `WarningsOverflow` even when many
    /// workers hit the cap. MUST be the last element of the assembled
    /// warning list when present (per `cancellation.md` §7.3).
    WarningsOverflow { suppressed_count: u64 },
}

impl fmt::Display for QueryWarning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryWarning::EntityEventLimitExceeded {
                entity_id,
                count,
                limit,
            } => write!(
                f,
                "entity event limit exceeded: entity={entity_id}, count={count}, limit={limit}"
            ),
            QueryWarning::SessionEventCapExceeded {
                entity_id,
                event_count,
                cap,
            } => write!(
                f,
                "session event cap exceeded: entity={entity_id}, event_count={event_count}, cap={cap}"
            ),
            QueryWarning::AttributeTouchpointCapExceeded {
                entity_id,
                touchpoint_count,
                cap,
            } => write!(
                f,
                "attribute touchpoint cap exceeded: entity={entity_id}, touchpoint_count={touchpoint_count}, cap={cap}"
            ),
            QueryWarning::ActiveStateLimitExceeded {
                entity_id,
                active_states,
                cap,
            } => write!(
                f,
                "active state limit exceeded: entity={entity_id}, active_states={active_states}, cap={cap}"
            ),
            QueryWarning::WarningsOverflow { suppressed_count } => {
                write!(f, "{suppressed_count} further warnings suppressed")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entity_event_limit_display() {
        let w = QueryWarning::EntityEventLimitExceeded {
            entity_id: "u_42".into(),
            count: 10_000_001,
            limit: 10_000_000,
        };
        let s = w.to_string();
        assert!(s.contains("u_42"));
        assert!(s.contains("10000001"));
        assert!(s.contains("10000000"));
    }

    #[test]
    fn warnings_overflow_display_matches_cli_footer() {
        // The CLI rendering in cancellation.md §7.5 formats the
        // overflow as "N further warnings suppressed".
        let w = QueryWarning::WarningsOverflow {
            suppressed_count: 12,
        };
        assert_eq!(w.to_string(), "12 further warnings suppressed");
    }

    #[test]
    fn variants_are_eq_and_clone() {
        let w = QueryWarning::SessionEventCapExceeded {
            entity_id: "e".into(),
            event_count: 1,
            cap: 1,
        };
        assert_eq!(w.clone(), w);
    }
}
```

- [ ] **Step 2: Re-export from `lib.rs`**

In `crates/bqlite-core/src/lib.rs`, add `pub mod warning;` and `pub use warning::QueryWarning;` next to the existing module declarations and re-exports.

- [ ] **Step 3: Run tests**

```bash
cargo test -p bqlite-core --lib warning
```

Expected: PASS (3 new tests).

### Task 1.3 — Update execution-model.md §12

**Files:**
- Modify: `docs/design/execution-model.md` (lines 958–1003)

- [ ] **Step 1: Replace the §12.1 enum sketch and §12.2 warning enum** with text that points at the new types and the cancellation.md protocol. Replace lines 958–1003 with:

```markdown
## 12. Error Handling

### 12.1 Error Propagation

Operators and the engine unify on `bqlite_core::BqliteError` (per
`docs/design/operators/operator-traits.md` §2). The earlier sketch of
separate `OperatorError` / `ExecutionError` enums is **superseded** by
this rule.

Variants relevant to runtime failures:

- `BqliteError::Cancelled` — caller cancellation or LIMIT
  short-circuit (the LIMIT case never reaches the user).
- `BqliteError::Timeout { elapsed_ms }` — query exceeded its
  configured timeout. The engine's per-query timer fires
  `CancelReason::Timeout`, then `token.cancel()`.
- `BqliteError::OperatorPanic { message, location }` — a worker
  panicked. Caught at the morsel boundary by `catch_unwind`; peer
  workers exit at their next yield point via cascading
  `token.cancel()`.
- `BqliteError::MemoryBudgetExceeded { requested, used, budget }` —
  per-query memory budget exhausted with no spillable handler willing
  to free bytes.
- `BqliteError::MaxGroupsExceeded { limit }` — `HashAccumulator` /
  `DistinctOperator` group-cardinality cap.
- `BqliteError::Io` / `BqliteError::Arrow` / `BqliteError::Schema` /
  `BqliteError::Plan` / `BqliteError::Execution` /
  `BqliteError::Corruption` — domain-specific failures unchanged from
  earlier waves.

The first-fire CAS on `QueryContext::reason` and the precedence rule
(panic > cancel > timeout > LimitHit) live in
`docs/design/engine/cancellation.md` §3.1 — that note is the single
source of truth for cancellation/timeout/panic attribution.

### 12.2 Query Warnings

Non-fatal conditions are surfaced through `bqlite_core::QueryWarning`
and attached to the result as `ExecutionResult::warnings`
(success path) or `ExecutionFailure::warnings` (error path). Per-worker
1,000-entry caps, coordinator merge, and `WarningsOverflow` ordering
are specified in `docs/design/engine/cancellation.md` §7. Operators
record warnings via `EntityOperator::take_pending_warnings` so the
hot path never sees engine-orchestration types.
```

- [ ] **Step 2: Run dep-direction check + lint**

```bash
scripts/local-ci.sh
```

Expected: PASS (we have not yet broken anything — the doc is text only and the new types compile cleanly).

### Task 1.4 — CP1 review + commit + merge

- [ ] **Step 1: Spawn code-review subagent** on the staged diff. Pass the diff plus `docs/design/engine/cancellation.md` §6 / §7. Block on any blocking findings.
- [ ] **Step 2: Commit**

```bash
git add crates/bqlite-core/src/error.rs crates/bqlite-core/src/warning.rs \
        crates/bqlite-core/src/lib.rs docs/design/execution-model.md
git commit -m "TASK-511: Add Timeout/OperatorPanic/MemoryBudgetExceeded/MaxGroupsExceeded variants and QueryWarning enum"
```

- [ ] **Step 3: Fast-forward merge to main per AGENTS.md** and continue on `task/TASK-511`.

---

## Checkpoint 2: Replace stringly memory-budget + max-groups errors

**Goal:** Switch the call sites the design doc explicitly names from `BqliteError::Execution(String)` to the structured variants from CP1. Update the test pattern-matches.

### Task 2.1 — `budget_exceeded_error` returns structured variant

**Files:**
- Modify: `crates/bqlite-core/src/memory.rs` (line 208–215, doc comments at lines 12, 132, 208)

- [ ] **Step 1:** Replace `budget_exceeded_error`. The signature keeps `requested` so existing callers do not need to change, but the variant only carries `used` (post-attempt usage = `used + requested` is the bytes that *would have been* held; we surface only `used` per the doc shape):

```rust
/// Construct a [`BqliteError::MemoryBudgetExceeded`] with the standard
/// shape used by every memory-budget-aware operator. `requested` is
/// folded into `used` (the variant only exposes `{ used, budget }`
/// per `docs/design/engine/cancellation.md` §4.3); callers that want
/// to surface the requested-bytes diagnostic should log it separately
/// before propagating the error.
pub fn budget_exceeded_error(requested: u64, budget: u64, used: u64) -> BqliteError {
    BqliteError::MemoryBudgetExceeded {
        used: used.saturating_add(requested),
        budget,
    }
}
```

- [ ] **Step 2:** Update the surrounding doc comments (lines 12, 132): replace
"`Err(BqliteError::Execution(...))`" with "`Err(BqliteError::MemoryBudgetExceeded { .. })`".

- [ ] **Step 3:** Replace the existing `budget_exceeded_error_message` test:

```rust
#[test]
fn budget_exceeded_error_yields_structured_variant() {
    let err = budget_exceeded_error(100, 200, 150);
    match err {
        BqliteError::MemoryBudgetExceeded { used, budget } => {
            assert_eq!(used, 250); // requested(100) folded into used(150)
            assert_eq!(budget, 200);
        }
        other => panic!("expected MemoryBudgetExceeded, got {other:?}"),
    }
}
```

- [ ] **Step 4:** Run tests:

```bash
cargo test -p bqlite-core --lib memory
```

Expected: PASS.

### Task 2.2 — `DistinctOperator` uses `MaxGroupsExceeded`

**Files:**
- Modify: `crates/bqlite-operators/src/distinct.rs` (line 197 and 531-535 test)

- [ ] **Step 1:** Replace the call site at line 197:

```rust
                if self.seen.len() >= self.max_groups {
                    return Err(BqliteError::MaxGroupsExceeded {
                        limit: self.max_groups,
                    });
                }
```

- [ ] **Step 2:** Update the test at line 531 (the one matching `BqliteError::Execution(msg)`) to:

```rust
            Err(BqliteError::MaxGroupsExceeded { limit }) => {
                assert_eq!(limit, 2);
            }
```

(Adjust the surrounding match arms accordingly. If the test asserts on the message text, replace those assertions with field-level asserts.)

- [ ] **Step 3:** Update doc comments at lines 76 and 184 to reference `MaxGroupsExceeded` instead of `Execution`.

- [ ] **Step 4:** Run tests:

```bash
cargo test -p bqlite-operators --lib distinct
```

Expected: PASS.

### Task 2.3 — `HashAccumulator` cardinality cap uses `MaxGroupsExceeded`

**Files:**
- Modify: `crates/bqlite-operators/src/aggregate/mod.rs` (lines 553-557, 707-711, 837-839 docs, 2146 test)

- [ ] **Step 1:** Replace both call sites:

Line ~553:
```rust
            if self.groups.len() >= self.max_groups {
                return Err(BqliteError::MaxGroupsExceeded {
                    limit: self.max_groups,
                });
            }
```

Line ~707:
```rust
                if self.groups.len() >= self.max_groups {
                    return Err(BqliteError::MaxGroupsExceeded {
                        limit: self.max_groups,
                    });
                }
```

- [ ] **Step 2:** Update doc comments at lines 461, 837 to reference `MaxGroupsExceeded`.

- [ ] **Step 3:** Update test at line 2146:

```rust
            assert!(
                matches!(err, BqliteError::MaxGroupsExceeded { limit: 2 }),
                "{err}"
            );
```

(Inspect the actual `max_groups` value used in that test and substitute the right number.)

- [ ] **Step 4:** Run tests:

```bash
cargo test -p bqlite-operators --lib aggregate
```

Expected: PASS.

### Task 2.4 — `physical.rs` planner doc comments

**Files:**
- Modify: `crates/bqlite-planner/src/physical.rs` (lines 593, 611)

- [ ] **Step 1:** Re-read both lines first. Two known doc comments refer to `BqliteError::Execution`:
  - Line 593: `SortPhysical::max_rows` doc — sort's `max_rows` is a *row count* cap, not a group/cardinality cap. Leave the variant reference as `BqliteError::Execution` (sort is out of TASK-511's structured-variant scope; the `Execution` variant is preserved per `cancellation.md` §6.1).
  - Line 611: if the doc references `DistinctPhysical::max_groups` or `HashAggregatePhysical::max_groups`, change `BqliteError::Execution` → `BqliteError::MaxGroupsExceeded`. If it references sort's `max_rows`, leave it alone.

- [ ] **Step 2:** Run a final `grep -rn "BqliteError::Execution" crates/bqlite-planner/` and `cargo check -p bqlite-planner` to verify nothing else needs updating.

### Task 2.5 — full local-ci, review, commit, merge

- [ ] **Step 1:** Run `scripts/local-ci.sh` and resolve every failure (some integration tests may pattern-match on `BqliteError::Execution(msg)` — use `grep -rn "BqliteError::Execution" tests/ crates/` to find them and convert each to the structured pattern).

- [ ] **Step 2:** Spawn code-review subagent. Block on blocking findings.

- [ ] **Step 3:** Commit:

```bash
git commit -m "TASK-511: Replace stringly memory-budget and max-groups errors with structured variants"
```

- [ ] **Step 4:** Fast-forward merge to main, continue on `task/TASK-511`.

---

## Checkpoint 3: Warning channel + ExecutionResult/ExecutionFailure surface

**Goal:** Wire the warning recording path end-to-end. Add `EntityOperator::take_pending_warnings`; override on Sessionize, Attribute, and SequenceMatchOperator. Add `WarningSink` (engine-side) shared via `Arc<Mutex<...>>`. Have the EntityOperatorAdapter and SequenceMatchAdapter drain warnings after each `finalize_entity`. Add `ExecutionResult.warnings` and the `ExecutionFailure { error, warnings }` wrapper, change `Engine::query` signature, install `catch_unwind` for `OperatorPanic`, update CLI rendering and every caller test.

### Task 3.1 — `EntityOperator::take_pending_warnings`

**Files:**
- Modify: `crates/bqlite-operators/src/operator.rs`

- [ ] **Step 1:** After `finish_entity_into` (around line 325) add the new method:

```rust
    /// Drain the per-entity warnings the operator stashed on its state
    /// during `process_sub_batch` (e.g. cap-exceeded events). The
    /// adapter calls this **before** consuming the state via
    /// `finish_entity` / `finish_entity_into`, then forwards each
    /// returned warning to the per-query [`WarningSink`].
    ///
    /// The default implementation returns an empty vec — stateless
    /// operators and stateful operators with no diagnostic channel
    /// inherit zero overhead. Sessionize, Attribute, and SequenceMatch
    /// override to report their cap-exceeded events.
    ///
    /// `entity_id` is supplied by the adapter so the operator does not
    /// need to thread the EntityId through its state purely for
    /// attribution. Operators that already carry the EntityId on their
    /// state (Sessionize, Attribute) may ignore the argument; operators
    /// that don't (the matcher's StepCounterState) populate the warning
    /// from this argument.
    fn take_pending_warnings(
        &self,
        _state: &mut Self::State,
        _entity_id: &bqlite_core::EntityId,
    ) -> Vec<bqlite_core::QueryWarning> {
        Vec::new()
    }
```

- [ ] **Step 2:** Add a unit test verifying the default returns empty:

```rust
    #[test]
    fn entity_operator_default_take_pending_warnings_is_empty() {
        let op = sum_op();
        let mut state = op.create_state(&EntityId::from("u1"));
        let warnings = op.take_pending_warnings(&mut state, &EntityId::from("u1"));
        assert!(warnings.is_empty());
    }
```

- [ ] **Step 3:** Run tests:

```bash
cargo test -p bqlite-operators --lib operator
```

Expected: PASS.

### Task 3.2 — Sessionize override

**Files:**
- Modify: `crates/bqlite-operators/src/sessionize.rs`

- [ ] **Step 1:** Find the `impl EntityOperator for SessionizeOperator` block. Add the override:

```rust
    fn take_pending_warnings(
        &self,
        state: &mut Self::State,
        _entity_id: &bqlite_core::EntityId,
    ) -> Vec<bqlite_core::QueryWarning> {
        if state.cap_exceeded {
            // Latched: clear it so a re-drain yields nothing.
            state.cap_exceeded = false;
            vec![bqlite_core::QueryWarning::SessionEventCapExceeded {
                entity_id: state.entity_id().to_string(),
                event_count: state.entity_event_count(),
                cap: self.session_event_cap as u64,
            }]
        } else {
            Vec::new()
        }
    }
```

(Replace `self.session_event_cap` with whatever field on `SessionizeOperator` already holds the cap; check the struct around line ~327.)

- [ ] **Step 2:** Add a unit test that drives the cap to fire and asserts the override yields exactly one `SessionEventCapExceeded` warning. Use the existing test fixture at the bottom of `sessionize.rs` (the test that exercises `state.cap_exceeded()`).

- [ ] **Step 3:** Run tests:

```bash
cargo test -p bqlite-operators --lib sessionize
```

Expected: PASS.

### Task 3.3 — Attribute override

**Files:**
- Modify: `crates/bqlite-operators/src/attribute.rs`

- [ ] **Step 1:** In `impl EntityOperator for AttributeOperator` (line 606 — verified by `grep -n "impl EntityOperator for AttributeOperator"`), add:

```rust
    fn take_pending_warnings(
        &self,
        state: &mut Self::State,
        _entity_id: &bqlite_core::EntityId,
    ) -> Vec<bqlite_core::QueryWarning> {
        match state.take_diagnostic() {
            Some(diag) => vec![bqlite_core::QueryWarning::AttributeTouchpointCapExceeded {
                entity_id: diag.entity_id.to_string(),
                touchpoint_count: diag.event_count,
                cap: diag.cap,
            }],
            None => Vec::new(),
        }
    }
```

- [ ] **Step 2:** Add a unit test driving the deque cap to fire — assert one `AttributeTouchpointCapExceeded` warning and that a second `take_pending_warnings` returns empty (latched).

- [ ] **Step 3:** Run tests:

```bash
cargo test -p bqlite-operators --lib attribute
```

Expected: PASS.

### Task 3.4 — Match operator override

**Files:**
- Modify: `crates/bqlite-operators/src/matcher/step_counter.rs` (add accessor)
- Modify: `crates/bqlite-operators/src/matcher/mod.rs` (around line 318, the `impl EntityOperator for SequenceMatchOperator`)

- [ ] **Step 1:** Add the public accessor on `StepCounterSimulator` next to the existing `with_active_state_limit` builder (~line 411):

```rust
    /// Configured active-state cap (used by `take_pending_warnings`
    /// to populate `QueryWarning::ActiveStateLimitExceeded`).
    pub fn active_state_limit(&self) -> usize {
        self.active_state_limit
    }
```

- [ ] **Step 2:** In `impl EntityOperator for SequenceMatchOperator` (line 318), add the override. Field on the operator is `self.strategy` (NOT `self.driver`). NFA paths do not currently track an active-state cap, so they always return empty:

```rust
    fn take_pending_warnings(
        &self,
        state: &mut Self::State,
        entity_id: &bqlite_core::EntityId,
    ) -> Vec<bqlite_core::QueryWarning> {
        if let SequenceMatchState::StepCounter(sc) = state {
            if sc.cap_exceeded {
                sc.cap_exceeded = false;
                let cap = match &self.strategy {
                    StrategyDriver::StepCounter(sim) => sim.active_state_limit() as u64,
                    StrategyDriver::Nfa(_) => return Vec::new(),
                };
                let active = sc.tracks.len() as u64 + sc.dropped_count;
                return vec![bqlite_core::QueryWarning::ActiveStateLimitExceeded {
                    entity_id: entity_id.to_string(),
                    active_states: active,
                    cap,
                }];
            }
        }
        Vec::new()
    }
```

- [ ] **Step 3:** Add a unit test that drives the active-state cap and asserts one `ActiveStateLimitExceeded`. Use the existing `active_state_cap_prevents_unbounded_track_growth` test (step_counter.rs:1659) as a fixture model — it already has the pattern that fires the cap.

- [ ] **Step 4:** Run tests:

```bash
cargo test -p bqlite-operators --lib matcher
```

Expected: PASS.

### Task 3.5 — `WarningSink` and `WorkerContext` in bqlite-engine

**Files:**
- Create: `crates/bqlite-engine/src/warning_sink.rs`
- Modify: `crates/bqlite-engine/src/lib.rs`

- [ ] **Step 1:** Create the file with:

```rust
//! Per-query warning sink shared across operators.
//!
//! The `WarningSink` wraps a `WorkerContext` behind an `Arc<Mutex<...>>`
//! so the single-threaded driver and (eventually) parallel workers
//! can share the same per-query warning buffer. The mutex is on the
//! cold path — operators only acquire it after `finish_entity`, never
//! inside the per-event loop. See `docs/design/engine/cancellation.md`
//! §7.2 / §7.3 for the full protocol.

use std::sync::{Arc, Mutex};

use bqlite_core::QueryWarning;

/// Per-worker warning slot. Owns the bounded `Vec<QueryWarning>` and
/// the suppressed-warning counter.
#[derive(Debug, Default)]
pub struct WorkerContext {
    pub warnings: Vec<QueryWarning>,
    pub warning_overflow: u64,
}

impl WorkerContext {
    /// Per-`cancellation.md` §7.2: each worker silently drops further
    /// warnings once the cap is reached, incrementing the overflow
    /// counter so the coordinator can surface a final
    /// `WarningsOverflow` aggregating across workers.
    pub const PER_WORKER_WARNING_CAP: usize = 1_000;

    pub fn record_warning(&mut self, warning: QueryWarning) {
        if self.warnings.len() < Self::PER_WORKER_WARNING_CAP {
            self.warnings.push(warning);
        } else {
            self.warning_overflow = self.warning_overflow.saturating_add(1);
        }
    }
}

/// Shared handle to a `WorkerContext`. Cloneable; clones share the
/// same underlying buffer.
#[derive(Debug, Clone, Default)]
pub struct WarningSink {
    inner: Arc<Mutex<WorkerContext>>,
}

impl WarningSink {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a single warning. Acquires the mutex.
    pub fn record(&self, warning: QueryWarning) {
        // Lock poisoning would mean a worker panicked while holding the
        // lock — surface the warning anyway by recovering the inner
        // value, since panic propagation goes through `OperatorPanic`,
        // not through the warning sink.
        let mut guard = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        guard.record_warning(warning);
    }

    /// Record many warnings in one mutex acquisition.
    pub fn record_many<I: IntoIterator<Item = QueryWarning>>(&self, iter: I) {
        let mut guard = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        for w in iter {
            guard.record_warning(w);
        }
    }

    /// Drain the assembled warning list per `cancellation.md` §7.3:
    /// concatenated buffer in record order, with a final
    /// `QueryWarning::WarningsOverflow` appended when any warnings
    /// were suppressed.
    pub fn into_warnings(self) -> Vec<QueryWarning> {
        // If other clones still exist, pull a copy out instead. Clones
        // exist only while operators are alive, and `Engine::query`
        // drops the operator tree before draining, so in practice this
        // is the sole owner.
        let mut guard = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let WorkerContext {
            mut warnings,
            warning_overflow,
        } = std::mem::take(&mut *guard);
        if warning_overflow > 0 {
            warnings.push(QueryWarning::WarningsOverflow {
                suppressed_count: warning_overflow,
            });
        }
        warnings
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_under_cap_is_collected() {
        let sink = WarningSink::new();
        sink.record(QueryWarning::WarningsOverflow {
            suppressed_count: 7,
        });
        let out = sink.into_warnings();
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn record_over_cap_emits_single_overflow_at_end() {
        let sink = WarningSink::new();
        for i in 0..(WorkerContext::PER_WORKER_WARNING_CAP + 5) {
            sink.record(QueryWarning::EntityEventLimitExceeded {
                entity_id: format!("e{i}"),
                count: 1,
                limit: 1,
            });
        }
        let out = sink.into_warnings();
        assert_eq!(out.len(), WorkerContext::PER_WORKER_WARNING_CAP + 1);
        assert!(matches!(
            out.last().unwrap(),
            QueryWarning::WarningsOverflow {
                suppressed_count: 5
            }
        ));
    }

    #[test]
    fn into_warnings_empty_when_nothing_recorded() {
        let sink = WarningSink::new();
        assert!(sink.into_warnings().is_empty());
    }

    #[test]
    fn clones_share_buffer() {
        let sink = WarningSink::new();
        let clone = sink.clone();
        sink.record(QueryWarning::WarningsOverflow {
            suppressed_count: 1,
        });
        clone.record(QueryWarning::WarningsOverflow {
            suppressed_count: 2,
        });
        let out = sink.into_warnings();
        assert_eq!(out.len(), 2);
    }
}
```

- [ ] **Step 2:** Add `pub mod warning_sink;` and `pub use warning_sink::{WarningSink, WorkerContext};` to `crates/bqlite-engine/src/lib.rs` next to the other re-exports.

- [ ] **Step 3:** Run tests:

```bash
cargo test -p bqlite-engine --lib warning_sink
```

Expected: PASS.

### Task 3.6 — Adapters drain warnings after `finalize_entity`

**Files:**
- Modify: `crates/bqlite-engine/src/bind.rs` (`SequenceMatchAdapter` + `EntityOperatorAdapter`)

- [ ] **Step 1:** Add a `warnings: Option<WarningSink>` field to both `SequenceMatchAdapter` (struct around `bind.rs:131`) and `EntityOperatorAdapter` (struct around `bind.rs:381`). Thread it through their constructors. Default `None` for stand-alone tests where the engine driver isn't constructing the adapter.

The single sink is constructed in `Engine::query` (Task 3.7) and threaded through `bind_physical` as a new explicit argument (`bind_physical(&physical, db, sink.clone())`).

Concrete change to `EntityOperatorAdapter::finalize_entity` (current signature: `fn finalize_entity(&mut self, state: Op::State) -> Result<()>` at `bind.rs:424`):

```rust
    fn finalize_entity(&mut self, entity_id: &EntityId, mut state: Op::State) -> Result<()> {
        if let Some(sink) = &self.warnings {
            sink.record_many(self.operator.take_pending_warnings(&mut state, entity_id));
        }
        if let Some(batch) = self.operator.finish_entity(state) {
            self.pending.push_back(batch);
        }
        Ok(())
    }
```

There are exactly two `finalize_entity` call sites in `EntityOperatorAdapter` to update — confirm each has the entity id available:

  - **Mid-stream transition** at `bind.rs:443-447` inside `process_child_batch`. The current code is `if let (Some(_prev_entity), Some(prev_state)) = (self.current_entity.take(), self.current_state.take()) { self.finalize_entity(prev_state)?; }` — bind `prev_entity` (drop the leading underscore) and pass `&prev_entity` to `finalize_entity`.

  - **Final-flush at child exhaustion** in `next_batch` (`bind.rs:491-495`). Same pattern: bind `entity` (drop underscore) and pass `&entity`.

For `SequenceMatchAdapter::finalize_entity` at `bind.rs:226` (already receives `entity: EntityId`), insert the `take_pending_warnings` drain *before* the existing `finish_entity` / `finish_entity_into` call:

```rust
    fn finalize_entity(&mut self, entity: EntityId, mut state: SequenceMatchState) -> Result<()> {
        if let Some(sink) = &self.warnings {
            sink.record_many(self.operator.take_pending_warnings(&mut state, &entity));
        }
        if let Some(fused) = &mut self.fused { ... }
        else { ... }
        Ok(())
    }
```

- [ ] **Step 2:** Add a tiny adapter test in `bind.rs` (or a new `tests/tests/warning_channel.rs`) that wires a sink through `EntityOperatorAdapter` over a Sessionize fixture that fires the cap and asserts the sink received exactly one `SessionEventCapExceeded`.

- [ ] **Step 3:** Run tests:

```bash
cargo test -p bqlite-engine --lib bind
```

Expected: PASS.

### Task 3.7 — `ExecutionResult.warnings` + `ExecutionFailure` + Engine API change

**Files:**
- Modify: `crates/bqlite-engine/src/query.rs`
- Modify: `crates/bqlite-engine/src/lib.rs`
- Modify: `crates/bqlite-engine/src/delete.rs`
- Modify: `crates/bqlite-engine/src/render.rs`

- [ ] **Step 1:** Add the `warnings` field to `ExecutionResult` (line 87 area):

```rust
pub struct ExecutionResult {
    pub schema: OperatorSchema,
    pub rows: Vec<RecordBatch>,
    pub rows_affected: Option<u64>,
    /// Warnings recorded during execution; empty for queries that
    /// produced no diagnostics. See
    /// `docs/design/engine/cancellation.md` §7.5.
    pub warnings: Vec<bqlite_core::QueryWarning>,
}
```

Update the constructor at line 234 to set `warnings: sink.into_warnings()`.

- [ ] **Step 2:** Define `ExecutionFailure`:

```rust
/// Wrapper attached when the engine wants to publish partial
/// diagnostics alongside a fatal error. See
/// `docs/design/engine/cancellation.md` §5.4.
#[derive(Debug)]
pub struct ExecutionFailure {
    pub error: bqlite_core::BqliteError,
    pub warnings: Vec<bqlite_core::QueryWarning>,
}

impl ExecutionFailure {
    pub fn new(error: bqlite_core::BqliteError, warnings: Vec<bqlite_core::QueryWarning>) -> Self {
        Self { error, warnings }
    }

    /// Pattern-friendly extraction for callers that only want the error.
    pub fn into_error(self) -> bqlite_core::BqliteError {
        self.error
    }
}

impl From<bqlite_core::BqliteError> for ExecutionFailure {
    fn from(error: bqlite_core::BqliteError) -> Self {
        Self {
            error,
            warnings: Vec::new(),
        }
    }
}

impl std::fmt::Display for ExecutionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.error)
    }
}

impl std::error::Error for ExecutionFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}
```

- [ ] **Step 3:** Change `Engine::query` to return `Result<ExecutionResult, ExecutionFailure>`. The drain happens in exactly one place (the `Err` arm of the outer match) — on success, the inner `run_query` already populated `ExecutionResult.warnings` from its own clone of the sink. The two paths are mutually exclusive:

```rust
pub fn query(
    &self,
    text: &str,
    db: &mut Database,
) -> std::result::Result<ExecutionResult, ExecutionFailure> {
    let sink = WarningSink::new();

    // `AssertUnwindSafe` is required because `&mut Database` is not
    // `UnwindSafe` by default. This is sound here per
    // `docs/design/engine/cancellation.md` §4.1: the engine owns the
    // database for the call's duration, and on unwind the database
    // is dropped along with the operator tree without further
    // observation. The shared `WarningSink` is `Send + Sync` and
    // safe to observe across the unwind boundary.
    let inner = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        run_query(self, text, db, sink.clone())
    }));

    match inner {
        // Success: `run_query` already drained its clone of the sink
        // into `ExecutionResult.warnings`. Discard the outer `sink`
        // (it shares state with the drained clone — `into_warnings`
        // is idempotent because `mem::take` empties the inner buffer).
        Ok(Ok(result)) => Ok(result),
        // Cooperative failure: pull the partial warnings the
        // operators recorded before the error fired.
        Ok(Err(error)) => Err(ExecutionFailure {
            error,
            warnings: sink.into_warnings(),
        }),
        // Worker panic: surface as `OperatorPanic` with the panic's
        // message (location is always `None` until TASK-541 installs
        // the project-local panic hook, per `cancellation.md` §4.1).
        Err(payload) => {
            let message = panic_message(payload);
            Err(ExecutionFailure {
                error: bqlite_core::BqliteError::OperatorPanic {
                    message,
                    location: None,
                },
                warnings: sink.into_warnings(),
            })
        }
    }
}

fn run_query(
    _engine: &Engine,
    text: &str,
    db: &mut Database,
    sink: WarningSink,
) -> bqlite_core::Result<ExecutionResult> {
    // Body identical to the existing `Engine::query` impl, threading
    // `sink.clone()` into `bind_physical(&physical, db, sink.clone())`
    // and `execute_delete_statement(d, db, sink.clone())`. The final
    // construction of ExecutionResult sets `warnings: sink.into_warnings()`.
    // Drop the operator tree before draining so all sink clones held
    // by adapters are released first.
    ...
}

/// Extract a `String` from a `catch_unwind` payload. Panic payloads
/// are commonly `&'static str` or `String`; everything else stringifies
/// as a placeholder per `docs/design/engine/cancellation.md` §4.1.
fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        return (*s).to_string();
    }
    if let Some(s) = payload.downcast_ref::<String>() {
        return s.clone();
    }
    "<non-string panic payload>".to_string()
}
```

(Take payload by value, not `&Box<...>`, to keep `clippy::borrowed_box` quiet under `-D warnings`.)

Note on idempotency: `WarningSink::into_warnings(self)` consumes the binding but the inner state is `Arc<Mutex<WorkerContext>>`; multiple clones may exist. The implementation uses `mem::take` on the locked `WorkerContext`, so a second clone observing afterward sees empty and returns `Vec::new()`. This is intentional — the sink is single-shot in spirit even though Arc cloning permits multiple drains.

- [ ] **Step 4:** Update `bind_physical` signature to accept the sink and propagate it to adapter constructors. The simplest approach: keep `bind_physical(&plan, db)` unchanged and instead pass the sink via a `pub(crate) struct BindContext` member, or add an explicit second arg. The smaller diff is a new arg — do that.

Update `delete.rs::execute_delete_statement` to return `bqlite_core::Result<ExecutionResult>` whose `warnings` field is empty (DELETE does not run stateful operators that produce warnings, per `cancellation.md` §7.4).

- [ ] **Step 5:** Update `format_result_as_text` and `format_result_as_text_limited` in `render.rs` to append a warning footer when `result.warnings` is non-empty. Format per `cancellation.md` §7.5:

```text
3 warnings:
  - <Display of warning 1>
  - <Display of warning 2>
  - <Display of warning 3>
```

`WarningsOverflow` already renders as "N further warnings suppressed" via its `Display` impl.

- [ ] **Step 6:** Update every caller of `Engine::query` in the workspace. Enumerated callers (verified by `grep -rn "engine.query\|\.query(" crates/ tests/`):

  - `crates/bqlite-cli/src/main.rs::run_query` (line 434) — `.map_err` closure receives `ExecutionFailure`; the existing `format!("query failed: {e}")` works unchanged because `ExecutionFailure: Display` forwards to the inner error.
  - `crates/bqlite-cli/src/ingest.rs` — check whether ingest calls `engine.query()`; adapt accordingly.
  - `crates/bqlite/src/lib.rs` — top-level re-export crate; if it re-exports `Result` types referencing `BqliteError`, add `pub use bqlite_engine::ExecutionFailure;`.
  - `crates/bqlite-engine/src/query.rs` — internal tests.
  - `crates/bqlite-engine/src/bind.rs` — internal tests (line ~1602, ~2072).
  - `crates/bqlite-engine/src/delete.rs` — internal tests (lines ~1131, ~1164, ~1241, ~1269).
  - `crates/bqlite-engine/src/ingest.rs` — internal tests (lines ~1271 etc.).
  - `tests/tests/wave2_acceptance.rs`, `wave3_acceptance.rs`, `wave4_acceptance.rs`, `wave4_delete_compaction.rs`, `wave4_advanced_analytics_attribute_cohort_join.rs`, `wave4_advanced_analytics_event_select.rs`, `wave4_advanced_analytics_sessionize.rs`, `matcher_integration.rs`, `jsonl_ingest.rs` — convert pattern matches.

`crates/bqlite-ffi` does **not** currently exist — when the FFI surface lands (Wave 6) it will adopt `ExecutionFailure` natively.

Conversion patterns:
- `engine.query(...).expect("...")` — works unchanged (`ExecutionFailure: Debug`).
- `Err(BqliteError::Plan(msg))` arms become `Err(ExecutionFailure { error: BqliteError::Plan(msg), .. })`.
- `engine.query(...).map_err(|e| ...)` — closure now receives `ExecutionFailure`; access `.error` for the inner `BqliteError`.

- [ ] **Step 7:** Run the full suite:

```bash
scripts/local-ci.sh
```

Expected: PASS. Resolve every test failure by adapting the call site, not by relaxing the API.

### Task 3.8 — Render warning footer in CLI smoke

**Files:**
- Modify: `crates/bqlite-cli/src/main.rs` (line 434–444)

- [ ] **Step 1:** Verify `format_result_as_text_limited` already includes the warning footer (Task 3.7 step 5). The CLI does not need additional code; the footer rides through `rendered`.

- [ ] **Step 2:** Add a CLI-level integration test (in the existing `mod tests`) driving a query that produces a warning and asserting the footer appears in the rendered output. Use a session-cap-exceeded fixture by `INSERT`ing >1M events for one entity then running a `SESSIONIZE`.

If constructing such a fixture is too heavyweight for a single CLI test, defer to a new `tests/tests/warning_channel.rs` integration test instead and keep the CLI tests unchanged.

### Task 3.9 — Integration test: partial warnings surface on failure path

**Files:**
- Create: `tests/tests/warning_channel.rs`

- [ ] **Step 1:** Add an integration test that:
  1. Builds a database with one entity that has events exceeding the Sessionize per-entity cap (use `SessionizeOperator::new_with_cap` with a small cap to keep the test cheap).
  2. Runs a query that triggers the cap, then asserts `result.warnings` contains `SessionEventCapExceeded`.
  3. Runs a *second* query that combines the cap-firing pipeline with a downstream operator that fails (e.g. a parse-broken WHERE expression after the SESSIONIZE — the planner-time error must surface partial warnings collected before the failure).
  4. Asserts the failure surfaces as `Err(ExecutionFailure { error, warnings })` with `warnings` containing the cap warning and `error` carrying the structured plan/exec error.

If step 3 cannot be cleanly constructed (e.g. plan-time errors short-circuit before any operator runs, so no warnings can be recorded), substitute step 4 to verify the success-path warning rendering only and document the gap as a Wave 5 follow-up.

- [ ] **Step 2:** Run:

```bash
cargo test --test warning_channel
```

Expected: PASS.

### Task 3.10 — CP3 review + commit + merge

- [ ] **Step 1:** Spawn code-review subagent on the staged diff. Pass it `docs/design/engine/cancellation.md` §5.4, §7. Block on blocking findings.

- [ ] **Step 2:** Commit:

```bash
git commit -m "TASK-511: Wire QueryWarning channel and ExecutionResult/ExecutionFailure surface"
```

- [ ] **Step 3:** Fast-forward merge to main.

- [ ] **Step 4:** Move the lock file per Completion Protocol:

```bash
git mv tasks/active/TASK-511.lock tasks/completed/TASK-511.done
# edit the .done file to add `completed_at` field
git commit -m "TASK-511: completed"
git push origin main
```

---

## Self-Review Checklist

- **Spec coverage:**
  - §6.1 BqliteError additions ✓ (CP1)
  - §6.1 stringly → structured ✓ (CP2 — MemoryBudgetExceeded, MaxGroupsExceeded)
  - §7.1 QueryWarning enum ✓ (CP1)
  - §7.2 per-worker cap (1000) + overflow counter ✓ (CP3 task 3.5)
  - §7.3 coordinator merge w/ trailing WarningsOverflow ✓ (CP3 task 3.5 `into_warnings`)
  - §7.4 EntityOperator::take_pending_warnings + Sessionize/Attribute/Match overrides ✓ (CP3 tasks 3.1–3.4)
  - §5.4 ExecutionFailure wrapper + Engine::query signature change ✓ (CP3 task 3.7)
  - §7.5 CLI rendering ✓ (CP3 task 3.7 step 5)
  - §6.2 single source of truth for precedence — note in execution-model.md §12 ✓ (CP1 task 1.3)
  - §4 OperatorPanic catch_unwind in single-threaded driver ✓ (CP3 task 3.7)
- **Out of scope (TASK-541):** parallel-worker timer thread, real CancelReason CAS in QueryContext (no QueryContext type lands here), morsel-boundary catch_unwind. Documented in plan preamble.
- **Out of scope (TASK-510):** spill files, MemoryReservation drop ordering, MemoryTracker hierarchy. Only the BqliteError variant lands here.

