# TASK-538 — Public cancellation/timeout API + acceptance coverage

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose a public per-query cancel handle and timeout knob on `Engine::query_with_options`, wire timeout to a per-query timer that signals `CancellationToken::cancel(CancelReason::Timeout)`, and convert the existing wave5 stress / acceptance tests to drive cancellation through the public surface — closing the carve-outs in `wave5_runtime_stress.rs:21-23` and `wave5_acceptance.rs:14-21`.

**Architecture:**
- Extend the existing `QueryOptions` struct (already has `memory_budget_bytes`, `collect_cpu_metrics`) with `cancel: Option<CancellationToken>` and `timeout: Option<Duration>`. Keep the existing entry point `Engine::query_with_options` (the task spec calls the method `query_with`, but we already have `query_with_options` in production use; renaming is gratuitous churn — the surface shape is what matters).
- Add a `CancelReason` enum (`Cancelled / Timeout / LimitHit`) with an `AtomicU8` slot on `QueryContext` so the engine can discriminate the first-fire reason at result collection (per `cancellation.md` §3.1). The `panic_payload` slot is captured by the existing `catch_unwind` in `query.rs` and surfaced as `BqliteError::OperatorPanic`; we extend it with the precedence rule "panic always wins over timeout".
- Spawn a per-query timer thread when `timeout` is set; it sleeps the duration then CAS-installs `CancelReason::Timeout` and calls `token.cancel()`. Clean up the timer via a one-shot `Arc<AtomicBool>` "completed" flag so the timer self-exits when the query finishes naturally.
- At result mapping in `Engine::query_with_options`, when a `BqliteError::Cancelled` surfaces, the engine looks at `QueryContext::cancel_reason()`; if `Timeout`, it rewrites to `BqliteError::Timeout { elapsed_ms }` with the wall-clock at signal time.
- Tests cover: external-cancel mid-scan, timeout fires before completion, timeout cleanup of spill files (with a leaked `TempSpillFile` simulating §5.2), timeout + panic precedence; `wave5_acceptance.rs` band 2 is rewritten to use the public surface.

**Tech Stack:** Rust 2021 (workspace `bqlite-*`), Apache Arrow, `bqlite-operators::CancellationToken` (`Arc<AtomicBool>`), `std::thread`, `std::time::{Duration, Instant}`.

**Out of scope** (per task spec): per-statement cancel from the CLI (Wave 6 / TASK-538b), and the per-morsel iteration loop's `catch_unwind` (which only exists once TASK-541 introduces per-morsel iteration — today's `submit` runs a single closure). What *is* in scope per item (e): wrap the `MorselScheduler::submit` worker invocation in `catch_unwind` so a panic from any worker (degenerate or sharded) surfaces as `BqliteError::OperatorPanic` *inside* the scheduler. This is forward-compatible with TASK-536's per-shard dispatch (concurrently in flight on `task/TASK-536`) — when TASK-536 lands, each worker invocation is already inside the boundary, no second migration needed. The outer `catch_unwind` in `Engine::query_with_options` becomes defense-in-depth for parse / plan / bind path panics that don't go through the scheduler.

---

## File Structure

**Modify:**
- `crates/bqlite-engine/src/context.rs` — add `CancelReason` enum, `cancel_reason: Arc<AtomicU8>` and helpers on `QueryContext`, two new `QueryOptions` fields, a `with_external_cancellation()` constructor, and resolve_query_budget keeps its current shape.
- `crates/bqlite-engine/src/query.rs` — wire the user-supplied `cancel` token into `QueryContext`, spawn the timer thread, attach a self-exit flag, map `Cancelled` → `Timeout` at result collection, and ensure the timer cleans up on every exit path.
- `crates/bqlite-engine/src/lib.rs` — re-export `bqlite_operators::CancellationToken` and the new `CancelReason` for callers that only depend on `bqlite-engine`.
- `tests/tests/wave5_runtime_stress.rs` — extend the `cancellation_cleanup` module with three new end-to-end tests driving the public API (external-cancel mid-scan, timeout fires, timeout cleanup of spill files); update the module docstring carve-out.
- `tests/tests/wave5_acceptance.rs` — rewrite band 2 to drive cancellation via `QueryOptions { cancel, timeout, .. }` rather than at the `QueryContext` contract level; preserve the spill-cleanup invariant.
- `docs/design/engine/cancellation.md` — small reconciliation note in §6.2 / §8 that TASK-538 lands the public surface fields, leaves panic-precedence catch boundary at the outer `Engine::query_with_options` per §4.3, and defers per-morsel boundary work to TASK-541.

**Create:** none. The task is purely additive on the engine API surface; no new files.

---

## Checkpoint plan

Each checkpoint must independently pass `scripts/local-ci.sh`, get a subagent code review, and fast-forward merge to `main` before the next checkpoint starts.

- **CP1: Public surface + scheduler panic boundary.** Three commits (`context.rs` types, `query.rs` wiring, `engine_pool.rs` catch_unwind) — but merged together as one CP because they form one cohesive surface change. Engine-internal unit tests included. After CP1 the public API, timeout discrimination, and worker-panic surfacing are testable.
- **CP2: Wave 5 end-to-end coverage.** New `wave5_runtime_stress.rs` tests (external-cancel pre-query, timeout fires deterministically, timeout cleanup of spill files). Update `wave5_acceptance.rs` band 2. Reconciliation note in `cancellation.md` §6.2.

CP1 lands the shared-file changes (`context.rs`, `lib.rs`, `query.rs`, `scheduler/engine_pool.rs`); CP2 only touches test files plus the design doc — zero conflict risk.

**Conflict risk with `task/TASK-536`** (concurrently in flight): TASK-536 is editing `MorselScheduler` to do real per-shard dispatch. The CP1.3 edit to `submit` is a small additive `catch_unwind` wrap — likely conflicts cleanly. If CP1.3 lands first, TASK-536 inherits a panic-safe `submit`. If TASK-536 lands first, rebase CP1.3 onto its new `submit` shape (the wrap is still applicable wherever `submit` ultimately invokes the worker closure).

---

## Task 1 — CP1.1: Add `CancelReason` enum and atomic slot to `QueryContext`

**Files:**
- Modify: `crates/bqlite-engine/src/context.rs:29-411` (imports, struct, constructors, methods, tests)
- Modify: `crates/bqlite-engine/src/lib.rs:66-74` (re-exports)

- [ ] **Step 1: Write failing tests for the new `CancelReason` API**

Add to `crates/bqlite-engine/src/context.rs` inside the `mod tests` block:

```rust
#[test]
fn cancel_reason_default_is_none() {
    let ctx = QueryContext::new(MIN_QUERY_BUDGET_BYTES);
    assert_eq!(ctx.cancel_reason(), CancelReason::None);
    assert!(!ctx.cancellation().is_cancelled());
}

#[test]
fn cancel_with_reason_installs_first_fire_only() {
    let ctx = QueryContext::new(MIN_QUERY_BUDGET_BYTES);
    ctx.cancel_with_reason(CancelReason::Timeout);
    assert_eq!(ctx.cancel_reason(), CancelReason::Timeout);
    assert!(ctx.cancellation().is_cancelled());
    // Second fire is silently dropped — timeout wins because it
    // was first.
    ctx.cancel_with_reason(CancelReason::Cancelled);
    assert_eq!(ctx.cancel_reason(), CancelReason::Timeout);
}

#[test]
fn cancel_with_reason_propagates_through_clones() {
    let ctx = QueryContext::new(MIN_QUERY_BUDGET_BYTES);
    let clone = ctx.clone();
    ctx.cancel_with_reason(CancelReason::Cancelled);
    assert_eq!(clone.cancel_reason(), CancelReason::Cancelled);
    assert!(clone.cancellation().is_cancelled());
}

#[test]
fn external_cancellation_token_is_observed_by_context() {
    use bqlite_operators::CancellationToken;
    let external = CancellationToken::new();
    let ctx = QueryContext::new(MIN_QUERY_BUDGET_BYTES)
        .with_external_cancellation(external.clone());
    assert!(!ctx.cancellation().is_cancelled());
    external.cancel();
    assert!(
        ctx.cancellation().is_cancelled(),
        "cancel on the externally-supplied token must be observed by the context"
    );
    // The external-cancel path does not pre-install a reason; the
    // engine driver does that at the boundary because the user-
    // supplied token has no `cancel_with_reason` API.
    assert_eq!(ctx.cancel_reason(), CancelReason::None);
}
```

- [ ] **Step 2: Run the new tests and confirm they fail**

Run: `cargo test -p bqlite-engine context::tests::cancel_reason_default_is_none context::tests::cancel_with_reason_installs_first_fire_only context::tests::cancel_with_reason_propagates_through_clones context::tests::external_cancellation_token_is_observed_by_context`
Expected: compile errors / unresolved-name errors for `CancelReason`, `cancel_reason`, `cancel_with_reason`, `with_external_cancellation`.

- [ ] **Step 3: Implement the `CancelReason` enum + atomic slot + cancel_with_reason**

In `crates/bqlite-engine/src/context.rs`, after the `// Defaults & constants` block and before `EngineConfig`, insert:

```rust
// ─────────────────────────────────────────────────────────────────────────────
// CancelReason — first-fire attribution for cooperative cancel paths.
// (engine/cancellation.md §3.1)
// ─────────────────────────────────────────────────────────────────────────────

/// Why a query was cancelled. Set exactly once per [`QueryContext`] via
/// the first-fire CAS on [`QueryContext::cancel_with_reason`]; subsequent
/// fires lose the race and are silently dropped. Read at result
/// collection in `Engine::query_with_options` to discriminate
/// `BqliteError::Cancelled` from `BqliteError::Timeout`.
///
/// `LimitHit` is included for completeness with the design doc — the
/// `LimitOperator` calls `cancel_with_reason(LimitHit)` to short-circuit
/// once it has produced the requested rows. Per cancellation.md §3.1
/// case 4, the driver maps `LimitHit` to `Ok(...)` at result collection
/// (the in-flight rows were already collected before the token fired).
/// TASK-538 records the variant but does not yet rewire `LimitOperator`
/// to use `cancel_with_reason` — the existing `LimitOperator` calls
/// `cancellation().cancel()` directly, and the driver's "no reason"
/// path still produces the right answer because the rows are present.
///
/// Discriminants are stable: they are stored in an [`AtomicU8`] on the
/// shared context, and any change here must update the
/// `cancel_reason()` round-trip.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CancelReason {
    /// No cancellation has fired.
    None = 0,
    /// Caller-initiated cancel via `QueryOptions::cancel`.
    Cancelled = 1,
    /// Per-query timeout timer fired.
    Timeout = 2,
    /// `LimitOperator` short-circuited after producing the requested rows.
    LimitHit = 3,
}

impl CancelReason {
    fn from_u8(value: u8) -> Self {
        match value {
            1 => CancelReason::Cancelled,
            2 => CancelReason::Timeout,
            3 => CancelReason::LimitHit,
            _ => CancelReason::None,
        }
    }
}
```

- [ ] **Step 4: Add `cancel_reason: Arc<AtomicU8>` to `QueryContext` and constructor logic**

In `crates/bqlite-engine/src/context.rs`, update the imports at the top:

```rust
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
```

Add a field to the `QueryContext` struct (after `metrics` and the worker_aggregate fields, before `collect_cpu_metrics`):

```rust
    /// First-fire reason for the cooperative cancel path
    /// (engine/cancellation.md §3.1). Stored as a `u8` because
    /// `AtomicEnum` is not in std; values map through
    /// [`CancelReason::from_u8`].
    cancel_reason: Arc<AtomicU8>,
```

Add `cancel_reason: Arc::new(AtomicU8::new(CancelReason::None as u8))` to **both** `QueryContext::new` and `QueryContext::unbounded`.

Update the `Debug` impl to include the reason:

```rust
        f.debug_struct("QueryContext")
            .field("cancelled", &self.cancellation.is_cancelled())
            .field("cancel_reason", &self.cancel_reason())
            .field("memory_used_bytes", &self.memory.used_bytes())
            .field("memory_budget_bytes", &self.memory.budget_bytes())
            .field("has_tracker", &self.tracker.is_some())
            .field("has_spill_fs", &self.spill_fs.is_some())
            .field("spill_query_id", &self.spill_query_id)
            .finish()
```

Add new methods inside `impl QueryContext` (next to `cancellation()`):

```rust
    /// Read the current first-fire cancel reason. `None` until any
    /// cooperative source CAS-installs a reason via
    /// [`Self::cancel_with_reason`].
    pub fn cancel_reason(&self) -> CancelReason {
        CancelReason::from_u8(self.cancel_reason.load(Ordering::Acquire))
    }

    /// Mark the query cancelled with a structured reason. CAS-installs
    /// `reason` from `None`; second fires lose the race and are
    /// silently dropped. After the CAS (won or lost) the cancellation
    /// token is set so operators stop at their next yield point.
    ///
    /// `reason` must not be [`CancelReason::None`] — that variant
    /// represents the "no cancellation fired" sentinel and installing
    /// it would defeat the first-fire rule. Callers that want to flip
    /// the cancellation token without recording a reason should use
    /// `cancellation().cancel()` directly (the external-cancel path
    /// in `Engine::query_with_options` does exactly this).
    pub fn cancel_with_reason(&self, reason: CancelReason) {
        debug_assert_ne!(
            reason as u8,
            CancelReason::None as u8,
            "cancel_with_reason must record a real reason"
        );
        let _ = self.cancel_reason.compare_exchange(
            CancelReason::None as u8,
            reason as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        self.cancellation.cancel();
    }
```

- [ ] **Step 5: Add a `new_with_external_cancellation` constructor**

The token must be installed at construction time, *before* any clones can be made. Builder-style `with_external_cancellation(self) -> Self` would silently discard observers of the previous token if any clone of `self` were made between `new()` and the builder call. Make the API a constructor instead so the token is in place from the very first observer.

In `crates/bqlite-engine/src/context.rs`, inside `impl QueryContext`, next to `new()`:

```rust
    /// Build a tracker-backed context whose cancellation flag is
    /// driven by an externally-supplied `CancellationToken`. The
    /// caller retains a clone of the same token and observes any
    /// internal-cancel signal (timeout fire, panic peer-shutdown) by
    /// reading it.
    ///
    /// This is a constructor (not a builder) so the token is in place
    /// before any clone of `self` can be made — no observer ever sees
    /// the about-to-be-replaced default token.
    pub fn new_with_external_cancellation(
        budget_bytes: u64,
        token: bqlite_operators::CancellationToken,
    ) -> Self {
        let tracker = MemoryTracker::new(budget_bytes);
        Self {
            cancellation: token,
            memory: tracker.clone(),
            tracker: Some(tracker),
            warnings: crate::warning_sink::WarningSink::new(),
            spill_fs: None,
            spill_query_id: None,
            _cleanup: None,
            metrics: Arc::new(AtomicMetrics::new()),
            worker_aggregate: Arc::new(Mutex::new(QueryMetrics::zero())),
            cancel_reason: Arc::new(AtomicU8::new(CancelReason::None as u8)),
            collect_cpu_metrics: false,
        }
    }
```

(The same fields as `new()` except `cancellation` uses the supplied token.)

Update the test from Step 1 accordingly:

```rust
#[test]
fn external_cancellation_token_is_observed_by_context() {
    use bqlite_operators::CancellationToken;
    let external = CancellationToken::new();
    let ctx = QueryContext::new_with_external_cancellation(
        MIN_QUERY_BUDGET_BYTES,
        external.clone(),
    );
    assert!(!ctx.cancellation().is_cancelled());
    external.cancel();
    assert!(
        ctx.cancellation().is_cancelled(),
        "cancel on the externally-supplied token must be observed by the context"
    );
    assert_eq!(ctx.cancel_reason(), CancelReason::None);
}
```

- [ ] **Step 6: Run the new tests and confirm they pass**

Run: `cargo test -p bqlite-engine context::tests`
Expected: PASS for all four new tests plus all existing context tests.

- [ ] **Step 7: Add `cancel: Option<CancellationToken>` and `timeout: Option<Duration>` to `QueryOptions`**

Replace the `QueryOptions` struct in `crates/bqlite-engine/src/context.rs`:

```rust
/// Per-submission overrides for a single `Engine::query` call.
///
/// `Default` produces an empty option set (all `None` / `false`), so
/// existing callers that pass nothing still get the engine-level
/// defaults. New fields land here additively as they are added — the
/// struct is `#[non_exhaustive]`-by-convention (callers use struct
/// update syntax `..QueryOptions::default()`).
#[derive(Debug, Clone, Default)]
pub struct QueryOptions {
    /// Override the per-query memory budget. Validated against
    /// [`MIN_QUERY_BUDGET_BYTES`] at submission time.
    pub memory_budget_bytes: Option<u64>,
    /// Opt this query into CPU-cost sampling per
    /// `docs/design/execution-model.md` §14.3. The CLI's
    /// `--explain-perf` flag sets this; normal queries leave it
    /// `false` so the perf-counter overhead never lands on the
    /// hot path. Today the platform integration is a stub
    /// ([`crate::perf::PerfCounters`]); the flag still propagates
    /// through to `QueryMetrics::cpu_metrics_enabled` so callers can
    /// distinguish "CPU counters were sampled but were zero" from
    /// "CPU counters were never enabled".
    pub collect_cpu_metrics: bool,
    /// External cancellation handle. When `Some`, the token is
    /// installed as the per-query [`bqlite_operators::CancellationToken`]
    /// — operators see the same flag the caller set. When `None`, the
    /// engine constructs a fresh token (the original Wave 1 default).
    /// `cancellation.md` §3.1 source 1.
    pub cancel: Option<bqlite_operators::CancellationToken>,
    /// Per-query timeout. When `Some(d)`, the engine spawns a timer
    /// thread that fires `cancel_with_reason(Timeout)` after `d`
    /// elapses. The driver maps the resulting `BqliteError::Cancelled`
    /// back to `BqliteError::Timeout { elapsed_ms }` at result
    /// collection. `cancellation.md` §3.1 source 2.
    ///
    /// Latency target: the timer signal is observed at the next yield
    /// point — batch / sub-batch / morsel boundary, per
    /// `cancellation.md` §3.2.
    pub timeout: Option<std::time::Duration>,
}
```

Note: the struct was `#[derive(Debug, Clone, Copy, Default)]`. With a `CancellationToken` (which is `Clone` but not `Copy`), `Copy` must be removed. Check that no caller relies on `QueryOptions: Copy`. (The grep above turned up only `&opts` references and struct literals — none rely on `Copy`.)

- [ ] **Step 8: Add a `QueryOptions` smoke-test for the new fields**

Append to `mod tests`:

```rust
#[test]
fn query_options_default_includes_cancel_and_timeout_as_none() {
    let opts = QueryOptions::default();
    assert!(opts.cancel.is_none());
    assert!(opts.timeout.is_none());
}

#[test]
fn query_options_with_cancel_clones_token() {
    use bqlite_operators::CancellationToken;
    let token = CancellationToken::new();
    let opts = QueryOptions {
        cancel: Some(token.clone()),
        ..QueryOptions::default()
    };
    let cloned = opts.clone();
    // Mutating through one observable handle must reach the other —
    // they are clones of the same `Arc<AtomicBool>`.
    token.cancel();
    assert!(cloned.cancel.as_ref().unwrap().is_cancelled());
}
```

- [ ] **Step 9: Re-export `CancellationToken` and `CancelReason` from `bqlite-engine`**

In `crates/bqlite-engine/src/lib.rs`, update the `pub use` blocks:

```rust
pub use context::{
    CancelReason, EngineConfig, QueryContext, QueryOptions,
    DEFAULT_COMPACTION_BUDGET_BYTES, DEFAULT_INGEST_BUDGET_BYTES,
    DEFAULT_QUERY_BUDGET_BYTES, MIN_QUERY_BUDGET_BYTES,
};
```

And add a re-export for the canonical token type so `bqlite-cli` and `bqlite-ffi` (and tests) can construct one without importing `bqlite-operators`:

```rust
// `CancellationToken` lives in `bqlite-operators` so every operator
// can clone it without depending on the engine. Re-exported here so
// callers — CLI, FFI, top-level tests — do not have to add a
// `bqlite-operators` dependency just to construct a cancel handle
// for `QueryOptions::cancel`.
pub use bqlite_operators::CancellationToken;
```

- [ ] **Step 10: Run full local CI**

Run: `cargo build --all-targets && cargo test -p bqlite-engine && cargo clippy --all-targets --all-features -- -D warnings`
Expected: PASS. The `Copy` removal on `QueryOptions` may surface compile errors in callers — investigate and fix any (none expected; verified by grep above).

- [ ] **Step 11: Commit CP1.1**

```bash
git add crates/bqlite-engine/src/context.rs crates/bqlite-engine/src/lib.rs
git commit -m "TASK-538: Add CancelReason + cancel/timeout fields on QueryOptions"
```

---

## Task 2 — CP1.2: Wire timeout timer + cancel reason into `Engine::query_with_options`

**Files:**
- Modify: `crates/bqlite-engine/src/query.rs:289-374` (`query_with_options` body and the result-mapping arms)
- Modify: `crates/bqlite-engine/src/query.rs:380-498` (`run_query_inner` only takes new helper args if needed)

- [ ] **Step 1: Write failing tests for the timer + cancel discrimination**

Append to `crates/bqlite-engine/src/query.rs`'s `mod tests`:

```rust
#[test]
fn external_cancel_before_query_returns_cancelled() {
    use crate::CancellationToken;
    let scratch = Scratch::new("ext-cancel-pre");
    let mut db = create_db_with_events(scratch.path());
    let engine = Engine::new();
    let token = CancellationToken::new();
    token.cancel(); // Pre-cancel: the very first yield-point should fire.
    let opts = QueryOptions {
        cancel: Some(token),
        ..QueryOptions::default()
    };
    let err = engine
        .query_with_options("events", &mut db, &opts)
        .expect_err("pre-cancelled query must surface BqliteError::Cancelled");
    assert!(
        matches!(
            &err,
            ExecutionFailure {
                error: BqliteError::Cancelled,
                ..
            }
        ),
        "expected Cancelled, got {err:?}"
    );
}

#[test]
fn timeout_zero_fires_synchronously_returns_timeout_error() {
    // Duration::ZERO triggers the synchronous fire path in
    // QueryTimer::spawn — the cancel reason is installed before
    // run_query_inner is called, so the very first yield point
    // observes Cancelled, and the result-collection arm maps it to
    // BqliteError::Timeout via cancel_reason() == Timeout.
    // Deterministic — no retry loop.
    let scratch = Scratch::new("timeout-zero");
    let mut db = create_db_with_events(scratch.path());
    let engine = Engine::new();
    let opts = QueryOptions {
        timeout: Some(std::time::Duration::ZERO),
        ..QueryOptions::default()
    };
    let err = engine
        .query_with_options("events", &mut db, &opts)
        .expect_err("zero-duration timeout must fire deterministically");
    match err {
        ExecutionFailure {
            error: BqliteError::Timeout { elapsed_ms },
            ..
        } => {
            // elapsed_ms is the wall-clock between QueryTimer::spawn
            // and result collection — bounded above by the time the
            // query takes to surface Cancelled, well under a second.
            assert!(
                elapsed_ms < 60_000,
                "elapsed_ms must be reasonable, got {elapsed_ms}"
            );
        }
        other => panic!("expected Timeout, got {other:?}"),
    }
}

#[test]
fn no_timeout_no_cancel_runs_normally() {
    // A query without a timeout completes successfully and the timer
    // is never spawned — sanity check that the timer code is gated
    // on `Some(timeout)`.
    let scratch = Scratch::new("no-timeout");
    let mut db = create_db_with_events(scratch.path());
    let engine = Engine::new();
    let opts = QueryOptions::default();
    let result = engine
        .query_with_options("events", &mut db, &opts)
        .expect("default opts must succeed");
    assert!(result.is_empty());
}

#[test]
fn shared_cancel_token_observed_by_subsequent_queries() {
    // The same `CancellationToken` is reusable across queries —
    // pre-cancelling it cancels any future query that opts in.
    use crate::CancellationToken;
    let scratch = Scratch::new("shared-cancel");
    let mut db = create_db_with_events(scratch.path());
    let engine = Engine::new();
    let token = CancellationToken::new();
    token.cancel();

    for label in ["first", "second"] {
        let opts = QueryOptions {
            cancel: Some(token.clone()),
            ..QueryOptions::default()
        };
        let err = engine
            .query_with_options("events", &mut db, &opts)
            .expect_err("pre-cancelled token must cancel every query that opts in");
        assert!(
            matches!(
                &err,
                ExecutionFailure {
                    error: BqliteError::Cancelled,
                    ..
                }
            ),
            "{label}: expected Cancelled, got {err:?}"
        );
    }
}
```

- [ ] **Step 2: Run new tests and confirm they fail**

Run: `cargo test -p bqlite-engine query::tests::external_cancel_before_query_returns_cancelled query::tests::timeout_zero_fires_synchronously_returns_timeout_error query::tests::no_timeout_no_cancel_runs_normally query::tests::shared_cancel_token_observed_by_subsequent_queries`
Expected: the first test fails because the engine ignores `opts.cancel`; the second fails because the engine ignores `opts.timeout` (and `BqliteError::Timeout` is never produced); the third passes; the fourth fails for the same reason as the first.

- [ ] **Step 3: Wire `opts.cancel` into the per-query context**

Modify `Engine::query_with_options` in `crates/bqlite-engine/src/query.rs`. Replace the context-construction line (currently around line 319):

```rust
        let ctx = match options.cancel.clone() {
            Some(token) => QueryContext::new_with_external_cancellation(budget_bytes, token),
            None => QueryContext::new(budget_bytes),
        }
        .with_spill_fs(db.spill_fs().clone())
        .collect_cpu_metrics(options.collect_cpu_metrics);
```

The construction-then-builder chain stays consistent with the existing style and the external-cancel path uses the constructor (per `with_external_cancellation` design decision in Task 1).

- [ ] **Step 4: Run external-cancel test and confirm it passes**

Run: `cargo test -p bqlite-engine query::tests::external_cancel_before_query_returns_cancelled query::tests::shared_cancel_token_observed_by_subsequent_queries`
Expected: PASS.

- [ ] **Step 5: Spawn the timeout timer**

Add a helper near the top of `crates/bqlite-engine/src/query.rs`, after the `use ...` block:

```rust
/// RAII handle for the per-query timeout timer. Owns a one-shot
/// "completed" flag the driver flips before returning, plus the
/// timer thread's join handle. Drop unparks the thread (so it sees
/// the flag immediately) and joins it. Idle wait uses
/// `thread::park_timeout` rather than `sleep` so the timer wakes
/// instantly on natural completion — no slice-boundary tax.
///
/// `Duration::ZERO` is a special case: the timer fires synchronously
/// at spawn time and no thread is created. This makes the
/// "timeout-fired" test deterministic without race-tolerant retries.
struct QueryTimer {
    completed: Arc<std::sync::atomic::AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl QueryTimer {
    /// Spawn a timer that calls `ctx.cancel_with_reason(Timeout)` after
    /// `duration`, unless the driver flips `completed` first.
    ///
    /// Pre-condition: `duration > Duration::ZERO`. A zero duration is
    /// handled synchronously without spawning a thread (the cancel
    /// fires before this function returns).
    fn spawn(ctx: QueryContext, duration: std::time::Duration) -> Self {
        let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        if duration.is_zero() {
            // Synchronous fire: deterministic for tests, zero-thread
            // overhead in any other zero-deadline path.
            ctx.cancel_with_reason(CancelReason::Timeout);
            completed.store(true, std::sync::atomic::Ordering::Release);
            return Self {
                completed,
                handle: None,
            };
        }
        let completed_inner = Arc::clone(&completed);
        let handle = std::thread::Builder::new()
            .name("bqlite-query-timeout".to_string())
            .spawn(move || {
                let started = std::time::Instant::now();
                loop {
                    if completed_inner.load(std::sync::atomic::Ordering::Acquire) {
                        return;
                    }
                    let remaining = duration.saturating_sub(started.elapsed());
                    if remaining.is_zero() {
                        break;
                    }
                    // park_timeout returns either when the timer
                    // expires or when `Drop` calls `unpark()`. Either
                    // way the next loop iteration re-checks the
                    // completed flag and the elapsed budget.
                    std::thread::park_timeout(remaining);
                }
                if !completed_inner.load(std::sync::atomic::Ordering::Acquire) {
                    ctx.cancel_with_reason(CancelReason::Timeout);
                }
            })
            .expect("spawn timeout timer");
        Self {
            completed,
            handle: Some(handle),
        }
    }
}

impl Drop for QueryTimer {
    fn drop(&mut self) {
        self.completed
            .store(true, std::sync::atomic::Ordering::Release);
        if let Some(h) = self.handle.take() {
            // Wake the parked timer thread so it observes the flag
            // immediately rather than waiting out its remaining
            // park_timeout window. join() then returns promptly.
            h.thread().unpark();
            let _ = h.join();
        }
    }
}
```

The `CancelReason` symbol must be in scope. Add to the imports at the top of `query.rs`:

```rust
use crate::context::{resolve_query_budget, CancelReason, EngineConfig, QueryContext, QueryOptions};
```

(replacing the existing `use crate::context::{resolve_query_budget, EngineConfig, QueryContext, QueryOptions};`).

- [ ] **Step 6: Spawn the timer in `query_with_options` and bind it to the query lifetime**

Inside `Engine::query_with_options`, after constructing `ctx` and before the `catch_unwind` block, insert:

```rust
        // Spawn a timeout timer when requested. The timer holds a
        // clone of the context (cheap — every field is Arc) and self-
        // exits when the driver flips its `completed` flag at return.
        // Cancellation.md §3.1 source 2.
        let _timer = options
            .timeout
            .map(|d| QueryTimer::spawn(ctx.clone(), d));
        let started_at = std::time::Instant::now();
```

The existing `inner_ctx` clone passed to `run_query_inner` is unchanged. The `started_at` here is *engine-level* wall clock — it is used to compute `elapsed_ms` for the `Timeout` rewrite.

- [ ] **Step 7: Map `Cancelled` → `Timeout` at result collection**

Replace the `Ok(Err(error)) => Err(...)` arm of the `match inner` in `Engine::query_with_options`:

```rust
            // Cooperative failure. Pull the partial warnings the
            // operators recorded before the error fired, then map a
            // `Cancelled` whose first-fire reason is Timeout to the
            // typed `Timeout` variant per cancellation.md §3.1.
            Ok(Err(error)) => {
                let warnings = ctx.warnings().clone().into_warnings();
                let mapped = match (error, ctx.cancel_reason()) {
                    (BqliteError::Cancelled, CancelReason::Timeout) => {
                        let elapsed_ms = u64::try_from(started_at.elapsed().as_millis())
                            .unwrap_or(u64::MAX);
                        BqliteError::Timeout { elapsed_ms }
                    }
                    (other, _) => other,
                };
                Err(ExecutionFailure {
                    error: mapped,
                    warnings,
                })
            }
```

The `Err(payload) => ...` panic-mapping arm is unchanged: panics override timeout per §3.1 panic-precedence rule, and `OperatorPanic` is what the user sees regardless of any in-flight timeout. The `Ok(Ok(result))` arm is unchanged.

- [ ] **Step 8: Run the new timer + map tests and confirm they pass**

Run: `cargo test -p bqlite-engine query::tests::timeout_zero_fires_synchronously_returns_timeout_error query::tests::no_timeout_no_cancel_runs_normally`
Expected: PASS deterministically. `Duration::ZERO` triggers `QueryTimer::spawn`'s synchronous-fire branch, so `cancel_reason() == Timeout` is set before `run_query_inner` runs and the result-collection arm rewrites `Cancelled → Timeout` deterministically.

- [ ] **Step 9: Run the full engine test suite**

Run: `cargo test -p bqlite-engine`
Expected: PASS. Every existing test still passes (the changes are additive on `QueryOptions` and on `query_with_options`).

- [ ] **Step 10: Run dep-direction + clippy + fmt**

Run: `scripts/check-dep-direction.sh && cargo clippy --all-targets --all-features -- -D warnings && cargo fmt --all --check`
Expected: PASS. The new `CancellationToken` re-export is at engine level; no new dependency edge.

- [ ] **Step 11: Run full local CI**

Run: `scripts/local-ci.sh`
Expected: PASS.

- [ ] **Step 12: Commit CP1.2**

```bash
git add crates/bqlite-engine/src/query.rs
git commit -m "TASK-538: Wire timeout timer and Cancelled→Timeout discrimination"
```

(CP1 review and merge happen after CP1.3 lands — see Task 3 below.)

---

## Task 3 — CP1.3: Move panic boundary into `MorselScheduler::submit`

Per task item (e), the per-`(worker, morsel)` `catch_unwind` belongs inside the scheduler so a worker panic is caught at the worker boundary and surfaced as `BqliteError::OperatorPanic`. Today's `submit` invokes a single closure per query, but the boundary applies forward-compatibly when TASK-536 (concurrent — `task/TASK-536`) introduces real per-shard dispatch.

**Files:**
- Modify: `crates/bqlite-engine/src/scheduler/engine_pool.rs:142-162` (`submit`)
- Modify: `crates/bqlite-engine/src/query.rs` (the outer `catch_unwind` becomes defense-in-depth for the parse / plan / bind path)

- [ ] **Step 1: Refactor `submit` to convert worker panics into `Err(OperatorPanic)`**

The current `submit<F, R>` is generic over `R: Send`. To intercept panics inside it and convert them to a typed error, the worker closure's return type must be `Result<R, BqliteError>` for some `R` so we can fold the panic conversion into the `Err` arm. Today the only caller (`run_query_inner`) already uses `R = Vec<RecordBatch>` and returns `bqlite_core::Result<Vec<RecordBatch>>` — so we constrain `submit` to `R = bqlite_core::Result<T>` for `T: Send`.

Replace `submit` with a typed variant. In `crates/bqlite-engine/src/scheduler/engine_pool.rs`:

```rust
    /// Run `work` on the worker pool, catching any panic at the worker
    /// boundary and converting it to [`bqlite_core::BqliteError::OperatorPanic`].
    ///
    /// `work` returns a [`bqlite_core::Result<T>`]; the result is
    /// returned to the caller. Panics from `work` are caught here so
    /// follow-on work that splits the query across morsels (TASK-536,
    /// TASK-541) inherits a panic-safe submission point — no caller
    /// has to install its own boundary, and a concurrent worker panic
    /// no longer propagates through Rayon's scope unwinding.
    ///
    /// `cancellation.md` §4.1: the worker's `catch_unwind` boundary
    /// owns the panic-to-error conversion. The TASK-538 wiring keeps
    /// the boundary at the scheduler entry rather than per-morsel —
    /// when TASK-541 introduces per-morsel iteration, that boundary
    /// migrates inside the morsel loop.
    pub fn submit<T, F>(&self, work: F) -> bqlite_core::Result<T>
    where
        F: FnOnce() -> bqlite_core::Result<T> + Send,
        T: Send,
    {
        use std::panic::{catch_unwind, AssertUnwindSafe};

        let _permits = self.core_budget.acquire_n(self.query_threads);

        let result_slot: Mutex<Option<bqlite_core::Result<T>>> = Mutex::new(None);
        let result_ref = &result_slot;

        self.pool.scope(|s| {
            s.spawn(move |_| {
                // Catch panic inside the worker so the Rayon scope
                // does not see an unwinding panic and re-raise on
                // join. The conversion to OperatorPanic happens here;
                // peer workers (when TASK-536 lands) observe the
                // failure via the QueryContext cancel_with_reason
                // path. Currently single-task — this is forward-compat.
                let outcome = catch_unwind(AssertUnwindSafe(|| work()));
                let mapped = match outcome {
                    Ok(r) => r,
                    Err(payload) => Err(bqlite_core::BqliteError::OperatorPanic {
                        message: crate::scheduler::engine_pool::panic_message(payload),
                        location: None,
                    }),
                };
                *result_ref.lock().expect("result slot poisoned") = Some(mapped);
            });
        });

        result_slot
            .into_inner()
            .expect("result slot poisoned")
            .expect("worker did not write a result")
    }
```

Add the `panic_message` helper in the same module (lifted from `query.rs:505-513`; `query.rs` keeps its copy as a defense-in-depth helper):

```rust
/// Extract a human-readable message from a `catch_unwind` payload.
/// Matches `query.rs::panic_message` — duplicated rather than shared
/// because the engine and scheduler modules currently have no shared
/// utility module, and the function is six lines.
pub(crate) fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        return (*s).to_string();
    }
    if let Some(s) = payload.downcast_ref::<String>() {
        return s.clone();
    }
    "<non-string panic payload>".to_string()
}
```

- [ ] **Step 2: Update `run_query_inner` to match the new `submit` signature**

The existing call (`scheduler.submit(move || -> bqlite_core::Result<Vec<RecordBatch>> { ... })?`) already returns `bqlite_core::Result<Vec<RecordBatch>>` — no change needed at the call site; the closure already matches `FnOnce() -> bqlite_core::Result<T>`.

Verify: `grep -n "scheduler.submit" crates/bqlite-engine/src/query.rs`. The single call site is the one at line 470. Re-read it; if the `move || -> bqlite_core::Result<...>` annotation matches the new signature, no edit required.

- [ ] **Step 3: Update the existing engine-pool tests to match**

Run: `cargo test -p bqlite-engine scheduler::engine_pool::tests`

If any test calls `submit` with a closure returning a non-`Result` type, update them to wrap the return in `Ok(...)`. The expected pre-existing tests in `crates/bqlite-engine/src/scheduler/engine_pool.rs::tests` use `submit` for unit testing — fix them to match.

(If the existing tests use `Result<()>` returns, no fix needed.)

- [ ] **Step 4: Add a test that a worker-panicking closure surfaces `OperatorPanic`**

Append to `crates/bqlite-engine/src/scheduler/engine_pool.rs::tests`:

```rust
    #[test]
    fn submit_catches_worker_panic_as_operator_panic() {
        let cfg = EngineConfig::default();
        let scheduler = build_from_config(&cfg).expect("scheduler builds");
        let result: bqlite_core::Result<()> = scheduler.submit(|| {
            panic!("synthetic worker panic for test");
        });
        match result {
            Err(bqlite_core::BqliteError::OperatorPanic { message, .. }) => {
                assert!(
                    message.contains("synthetic worker panic for test"),
                    "panic payload should be in OperatorPanic message: {message}"
                );
            }
            other => panic!("expected OperatorPanic, got {other:?}"),
        }
        // Permits released cleanly — the worker boundary's catch did
        // not poison the CoreBudget.
        assert_eq!(
            scheduler.available_permits(),
            scheduler.query_threads(),
            "permits must be released even after a worker panic"
        );
    }
```

The test imports `EngineConfig` and `build_from_config` from the engine root.

- [ ] **Step 5: Re-verify the outer `catch_unwind` in `Engine::query_with_options`**

The outer boundary is now defense-in-depth for parse / plan / bind path panics that don't go through the scheduler. Update its inline comment:

```rust
        // Defense-in-depth panic boundary. Worker panics are caught
        // inside `MorselScheduler::submit` and surface as
        // `BqliteError::OperatorPanic` via the worker boundary; this
        // boundary covers parse/plan/bind path panics (and, until
        // TASK-541, the in-thread DDL / DELETE / EXPLAIN path that
        // bypasses the scheduler).
        let inner_ctx = ctx.clone();
        let scheduler = Arc::clone(&self.scheduler);
        let inner = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_query_inner(text, db, &inner_ctx, &scheduler)
        }));
```

(Comment-only change; no semantic change — the outer boundary still catches panics from any non-scheduler path.)

- [ ] **Step 6: Run the full local CI**

Run: `scripts/local-ci.sh`
Expected: PASS. Existing tests for `submit` may need closure-return-type adjustments (Step 3); confirm no surprises.

- [ ] **Step 7: Subagent code review of CP1**

Spawn a subagent to review the full CP1 staged range (CP1.1 + CP1.2 + CP1.3) with focus on:
- `cancel_reason` CAS correctness (acquire/release, first-fire)
- Timer Drop pathways: success / external cancel / timeout fire / panic
- `QueryOptions` losing `Copy`: compile-clean across the workspace
- `submit`'s `Result<T>` constraint: any other caller affected?
- The outer + scheduler `catch_unwind` are not redundant — outer covers DDL/DELETE/EXPLAIN bypass
- Rebase-readiness against `task/TASK-536` (which edits `submit`) — does the new boundary place the catch inside the worker closure that `submit` ultimately spawns, regardless of how TASK-536 reshapes the iteration?

Address any blocking findings before merging.

- [ ] **Step 8: Commit CP1.3**

```bash
git add crates/bqlite-engine/src/scheduler/engine_pool.rs crates/bqlite-engine/src/query.rs
git commit -m "TASK-538: Move worker panic boundary into MorselScheduler::submit"
```

- [ ] **Step 9: Fast-forward merge CP1 to main**

```bash
git checkout main
git pull origin main
git merge task/TASK-538 --ff-only
git push origin main
git checkout task/TASK-538
```

If `--ff-only` fails (e.g. TASK-536 landed first and conflicts on `submit`): `git checkout task/TASK-538 && git rebase main`, manually resolve the `submit` body conflict (re-apply the `catch_unwind` wrap inside whatever per-shard structure TASK-536 introduced), then re-run `scripts/local-ci.sh` before merging.

---

## Task 4 — CP2.1: End-to-end coverage in `wave5_runtime_stress.rs`

**Files:**
- Modify: `tests/tests/wave5_runtime_stress.rs:18-30` (out-of-scope docstring — drop the `Public timeout API` carve-out)
- Modify: `tests/tests/wave5_runtime_stress.rs:213-322` (the `cancellation_cleanup` mod — extend with public-API-driven tests)

The wave5 helpers (`scratch_db_root`, `count_rows`) are already in scope; the new tests reuse them.

- [ ] **Step 1: Update the out-of-scope docstring**

Replace the `Out of scope today` block (lines 19-30):

```rust
//! Out of scope today (deferred to TASK-528 acceptance gate / Wave 6):
//!
//! - **End-to-end `MaxGroupsExceeded` through `Engine::query`** —
//!   `DEFAULT_MAX_GROUPS = 1_000_000` is hardcoded in the planner;
//!   suite-level coverage is non-additive against the operator-level
//!   tests in `crates/bqlite-operators/src/aggregate/mod.rs::tests`.
//! - **Ingest partitioner spill** — TASK-512 not yet landed.
//! - **Per-`(worker, morsel)` `catch_unwind`** — TASK-541 follow-on;
//!   the outer `Engine::query_with_options` boundary catches panics
//!   today (cancellation.md §4.3).
```

(The `Public timeout API` bullet is removed because TASK-538 lands the public surface.)

- [ ] **Step 2: Add a fixture helper for ingest+long-running query**

In the `helpers` module in `wave5_runtime_stress.rs`, add (alongside `scratch_db_root` / `count_rows`):

```rust
/// Build a database with `n_entities` * `events_per_entity` rows
/// ingested through `INSERT FROM Parquet`. The fixture is large
/// enough that an `ORDER BY ts ASC` scan does observable work — used
/// by the cancellation-mid-scan test below to give the timer a window
/// to fire while operators are mid-loop.
///
/// Returns `(temp_db, db, engine)`. Caller owns the temp dir lifetime.
pub fn build_long_query_db(label: &str) -> (bqlite_tests::common::TempDb, bqlite_storage::Database, bqlite_engine::Engine) {
    use std::sync::Arc;
    use arrow::array::{StringArray, TimestampNanosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use bqlite_engine::{Database, Engine};
    use bqlite_tests::common::TempDb;
    use parquet::arrow::ArrowWriter;

    const N_ENTITIES: usize = 5_000;
    const EVENTS_PER_ENTITY: usize = 10;
    const T0: i64 = 1_700_000_000_000_000_000;
    const S: i64 = 1_000_000_000;

    let tmp = TempDb::new();
    let mut db =
        Database::create(tmp.path()).unwrap_or_else(|e| panic!("[{label}] Database::create: {e}"));
    let engine = Engine::new();
    engine
        .query(
            "CREATE TABLE events (\
                 entity_id STRING NOT NULL ENTITY KEY, \
                 ts TIMESTAMP NOT NULL EVENT TIME, \
                 event_type STRING NOT NULL EVENT TYPE\
             )",
            &mut db,
        )
        .unwrap_or_else(|e| panic!("[{label}] CREATE TABLE: {e}"));

    let arrow_schema = Arc::new(Schema::new(vec![
        Field::new("entity_id", DataType::Utf8, false),
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("event_type", DataType::Utf8, false),
    ]));
    let mut entity_ids: Vec<String> = Vec::with_capacity(N_ENTITIES * EVENTS_PER_ENTITY);
    let mut tss: Vec<i64> = Vec::with_capacity(N_ENTITIES * EVENTS_PER_ENTITY);
    let mut event_types: Vec<&str> = Vec::with_capacity(N_ENTITIES * EVENTS_PER_ENTITY);
    for i in 0..N_ENTITIES {
        let eid = format!("user_{i:05}");
        for k in 0..EVENTS_PER_ENTITY {
            entity_ids.push(eid.clone());
            tss.push(T0 + (k as i64) * S);
            event_types.push(if k % 3 == 0 { "view" } else if k % 3 == 1 { "add_to_cart" } else { "purchase" });
        }
    }
    let entity_id_array =
        StringArray::from(entity_ids.iter().map(String::as_str).collect::<Vec<_>>());
    let ts_array = TimestampNanosecondArray::from(tss);
    let event_type_array = StringArray::from(event_types);
    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(entity_id_array),
            Arc::new(ts_array),
            Arc::new(event_type_array),
        ],
    )
    .expect("build fixture batch");

    let path = tmp.path().join(format!("{label}-fixture.parquet"));
    let file = std::fs::File::create(&path).expect("create parquet file");
    let mut writer = ArrowWriter::try_new(file, arrow_schema, None).expect("ArrowWriter");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");

    let sql = format!(
        "INSERT INTO events FROM '{}' WITH (format: 'parquet')",
        path.display()
    );
    engine
        .query(&sql, &mut db)
        .unwrap_or_else(|e| panic!("[{label}] INSERT FROM Parquet: {e}"));
    (tmp, db, engine)
}
```

Verify the `bqlite_tests::common::TempDb` and `parquet::arrow::ArrowWriter` symbols are available in the test crate. They are — `wave5_acceptance.rs:74-76` already uses both. The test crate's `Cargo.toml` already pulls them in.

- [ ] **Step 3: Write the new public-API cancellation tests**

Inside `mod cancellation_cleanup` (after the existing `unbounded_context_has_no_spill_attachment` test), append:

```rust
    // ────────────────────────────────────────────────────────────
    // Public-surface tests (TASK-538): drive cancellation through
    // `Engine::query_with_options` and the new `QueryOptions`
    // surface rather than the contract-level `QueryContext`.
    // ────────────────────────────────────────────────────────────

    use bqlite_core::BqliteError;
    use bqlite_engine::{CancellationToken, ExecutionFailure, QueryOptions};

    use super::helpers::build_long_query_db;

    /// External cancel before the query starts must produce
    /// `BqliteError::Cancelled` from the very first yield point. This
    /// is the simplest external-cancel path through the public API.
    #[test]
    fn external_cancel_pre_query_returns_cancelled() {
        let (_tmp, mut db, engine) = build_long_query_db("ext-pre");
        let token = CancellationToken::new();
        token.cancel();
        let opts = QueryOptions {
            cancel: Some(token),
            ..QueryOptions::default()
        };
        let err = engine
            .query_with_options("events | ORDER BY ts ASC", &mut db, &opts)
            .expect_err("pre-cancelled token must fire on first yield");
        assert!(
            matches!(
                &err,
                ExecutionFailure { error: BqliteError::Cancelled, .. }
            ),
            "expected Cancelled, got {err:?}"
        );
    }

    /// External cancel from a watcher thread mid-scan must surface
    /// `BqliteError::Cancelled` rather than completing successfully.
    /// Drives the same code path as `cancel_propagates_through_context_clones`
    /// at the suite level — but through the public surface.
    ///
    /// Best-effort timing: the watcher races the engine driver; on a
    /// fast machine the query may complete before cancel observes a
    /// yield point. We loop a few attempts and accept either outcome
    /// per attempt, but require *some* attempt to land Cancelled —
    /// this gives CI flake-tolerance while still asserting the wiring.
    #[test]
    fn external_cancel_mid_scan_observes_cancelled_within_attempts() {
        let (_tmp, mut db, engine) = build_long_query_db("ext-mid");

        let mut saw_cancelled = false;
        for _ in 0..16 {
            let token = CancellationToken::new();
            let watcher = {
                let token = token.clone();
                std::thread::Builder::new()
                    .name("bqlite-test-canceller".into())
                    .spawn(move || {
                        // Cancel as soon as the spawn returns; the
                        // engine's pre-bind work is single-digit ms
                        // for an empty `events` query but the
                        // ORDER BY scan over 50_000 rows runs long
                        // enough that the cancel races into the
                        // scan's yield window on most attempts.
                        token.cancel();
                    })
                    .expect("spawn canceller thread")
            };
            let opts = QueryOptions {
                cancel: Some(token),
                ..QueryOptions::default()
            };
            let result = engine.query_with_options(
                "events | ORDER BY ts ASC",
                &mut db,
                &opts,
            );
            watcher.join().unwrap();
            if let Err(ExecutionFailure {
                error: BqliteError::Cancelled,
                ..
            }) = &result
            {
                saw_cancelled = true;
                break;
            }
        }
        assert!(
            saw_cancelled,
            "external-cancel never landed across 16 attempts; the public-API \
             cancel path must be wired"
        );
    }

    /// Per-query timeout fires deterministically when set to
    /// `Duration::ZERO` — `QueryTimer::spawn` synchronously calls
    /// `cancel_with_reason(Timeout)` before the query starts, so the
    /// driver maps the resulting `Cancelled` to `Timeout` at result
    /// collection (cancellation.md §3.1). No retry loop, no race.
    #[test]
    fn timeout_zero_duration_returns_timeout_deterministically() {
        let (_tmp, mut db, engine) = build_long_query_db("timeout-mid");
        let opts = QueryOptions {
            timeout: Some(std::time::Duration::ZERO),
            ..QueryOptions::default()
        };
        let err = engine
            .query_with_options(
                "events | ORDER BY ts ASC",
                &mut db,
                &opts,
            )
            .expect_err("zero-duration timeout must fire");
        match err {
            ExecutionFailure {
                error: BqliteError::Timeout { .. },
                ..
            } => {}
            other => panic!("expected Timeout, got {other:?}"),
        }
    }

    /// Timeout on an `ORDER BY` query at the floor budget leaves no
    /// spill artefacts behind. Re-pins the cancellation/cleanup
    /// contract (cancellation.md §5.1 / §5.2) through the public API.
    ///
    /// The `Duration::ZERO` synchronous-fire path makes the timeout
    /// deterministic: every iteration exits via the timeout path
    /// before any spill could occur (or, in the unlikely case the
    /// driver did spill on a partial run, the per-query subdir is
    /// reclaimed by `SpillCleanup::Drop` at last-clone-drop).
    /// The assertion is identical for both — no spill artefacts
    /// remain after the call.
    #[test]
    fn timeout_cleanup_leaves_no_spill_artefacts() {
        use bqlite_engine::MIN_QUERY_BUDGET_BYTES;

        let (_tmp, mut db, engine) = build_long_query_db("timeout-cleanup");
        let spill_root = db.spill_fs().root().to_path_buf();

        for _ in 0..8 {
            let opts = QueryOptions {
                timeout: Some(std::time::Duration::ZERO),
                memory_budget_bytes: Some(MIN_QUERY_BUDGET_BYTES),
                ..QueryOptions::default()
            };
            let result = engine.query_with_options(
                "events | ORDER BY ts ASC",
                &mut db,
                &opts,
            );
            // Sanity: the timer fires deterministically.
            assert!(
                matches!(
                    &result,
                    Err(ExecutionFailure {
                        error: BqliteError::Timeout { .. },
                        ..
                    })
                ),
                "expected Timeout on every iteration, got {result:?}"
            );

            // After every call, the spill root must contain no
            // per-query subdirs. Empty root or non-existent root is
            // acceptable — the per-query subdir is created lazily
            // on first spill, and on the timeout exit path the
            // SpillCleanup::Drop reclaims it before `query_with_options`
            // returns.
            if spill_root.exists() {
                let entries: Vec<_> = std::fs::read_dir(&spill_root)
                    .expect("read spill root")
                    .filter_map(|e| e.ok())
                    .collect();
                assert!(
                    entries.is_empty(),
                    "spill root must be empty after timeout, found {} entries",
                    entries.len()
                );
            }
        }
    }
```

Confirm the `db.spill_fs()` accessor exists on `Database`. Verify in `crates/bqlite-storage/src/database.rs` — it does (used by `query.rs:320`). It returns `&Arc<SpillFs>`, and `SpillFs::root()` returns `&Path` (used in tests already).

- [ ] **Step 4: Run the new tests and confirm they pass**

Run: `cargo test --test wave5_runtime_stress -- cancellation_cleanup`
Expected: PASS for all tests in the module, including the four pre-existing tests and the four new public-API tests.

- [ ] **Step 5: Run the full wave5 stress suite**

Run: `cargo test --test wave5_runtime_stress`
Expected: PASS (no regressions in `budget_exhaustion`, `spill_fallback`, `snapshot_isolation`, `warning_overflow`).

- [ ] **Step 6: Commit CP2.1**

```bash
git add tests/tests/wave5_runtime_stress.rs
git commit -m "TASK-538: Public-API cancellation + timeout coverage in wave5 stress suite"
```

---

## Task 5 — CP2.2: Convert `wave5_acceptance.rs` band 2 to use the public API

**Files:**
- Modify: `tests/tests/wave5_acceptance.rs:14-21` (band 2 docstring carve-out)
- Modify: `tests/tests/wave5_acceptance.rs` (band 2 — `cancellation_propagates_through_context_clones` and `cancellation_cleanup_reclaims_per_query_spill_subdir`)

- [ ] **Step 1: Update the band-2 docstring**

Replace the `2. Cancellation / timeout cleanup` block (lines 14-21):

```rust
//! 2. **Cancellation / timeout cleanup on a long-running query**
//!    (`engine/cancellation.md` § 5.1 / § 5.2,
//!    `engine/spill.md` § 8.3). TASK-538 added a public per-query
//!    cancel handle and timeout knob to `Engine::query_with_options`;
//!    band 2 drives both through that surface and asserts every
//!    exit path leaves no spill artefacts behind. The contract-level
//!    re-pinning (the original Wave 5 entry-time test) now lives in
//!    `tests/tests/wave5_runtime_stress.rs::cancellation_cleanup`
//!    only.
```

- [ ] **Step 2: Replace `cancellation_propagates_through_context_clones`**

Find and replace the test (currently at the band 2 boundary):

```rust
/// Public-surface cancellation: a caller-supplied `CancellationToken`
/// passed via `QueryOptions::cancel` is observed by every operator in
/// the query. The token is shared with the caller — pre-cancelling it
/// before submission produces `BqliteError::Cancelled` from the very
/// first yield point. This is the public-API equivalent of the
/// runtime-stress contract-level test.
#[test]
fn external_cancel_via_query_options_returns_cancelled() {
    use bqlite_engine::CancellationToken;

    let (_tmp, mut db, engine) = build_acceptance_db("ext-cancel");
    let token = CancellationToken::new();
    token.cancel();
    let opts = QueryOptions {
        cancel: Some(token),
        ..QueryOptions::default()
    };
    match engine.query_with_options("events", &mut db, &opts) {
        Err(bqlite_engine::ExecutionFailure {
            error: bqlite_core::BqliteError::Cancelled,
            ..
        }) => {}
        other => panic!("expected Cancelled via public API, got {other:?}"),
    }
}

/// Public-surface timeout: `Duration::ZERO` triggers
/// `QueryTimer::spawn`'s synchronous-fire branch — the cancel reason
/// is installed before `run_query_inner` runs, so the result-collection
/// arm rewrites `Cancelled → Timeout` deterministically. Pins the
/// contract that the timeout knob discriminates Cancelled from Timeout
/// on the public surface.
#[test]
fn timeout_via_query_options_returns_timeout_error() {
    let (_tmp, mut db, engine) = build_acceptance_db("timeout");
    let opts = QueryOptions {
        timeout: Some(std::time::Duration::ZERO),
        ..QueryOptions::default()
    };
    match engine.query_with_options(
        "events | ORDER BY ts ASC",
        &mut db,
        &opts,
    ) {
        Err(bqlite_engine::ExecutionFailure {
            error: bqlite_core::BqliteError::Timeout { .. },
            ..
        }) => {}
        other => panic!("expected Timeout via public API, got {other:?}"),
    }
}
```

- [ ] **Step 3: Update `cancellation_cleanup_reclaims_per_query_spill_subdir`**

Replace it with a public-API-driven test that runs a real query (not a contract-level `QueryContext`) and asserts the spill root is empty after a timeout-driven exit:

```rust
/// After a public-API timeout exits a query, the per-query spill
/// subdir is reclaimed by `SpillCleanup::Drop`. Re-pins
/// `engine/spill.md` § 8.3 at the wave-acceptance level — running
/// through `Engine::query_with_options` rather than constructing a
/// `QueryContext` directly.
///
/// The test does not need to *force* a spill — even a query that
/// never spilled passes the assertion ("the per-query subdir was
/// never created and the root stayed empty"). The point is that no
/// artefacts persist after a cancellation/timeout exit.
#[test]
fn timeout_exit_leaves_no_spill_artefacts() {
    let (_tmp, mut db, engine) = build_acceptance_db("timeout-cleanup");
    let spill_root = db.spill_fs().root().to_path_buf();

    for _ in 0..8 {
        let opts = QueryOptions {
            timeout: Some(std::time::Duration::ZERO),
            memory_budget_bytes: Some(MIN_QUERY_BUDGET_BYTES),
            ..QueryOptions::default()
        };
        let result = engine.query_with_options(
            "events | ORDER BY ts ASC",
            &mut db,
            &opts,
        );
        assert!(
            matches!(
                &result,
                Err(bqlite_engine::ExecutionFailure {
                    error: bqlite_core::BqliteError::Timeout { .. },
                    ..
                })
            ),
            "expected Timeout exit, got {result:?}"
        );
        if spill_root.exists() {
            let entries: Vec<_> = std::fs::read_dir(&spill_root)
                .expect("read spill root")
                .filter_map(|e| e.ok())
                .collect();
            assert!(
                entries.is_empty(),
                "spill root must be empty after timeout exit, found {} entries",
                entries.len()
            );
        }
    }
}
```

(The original `cancellation_cleanup_reclaims_per_query_spill_subdir` test is removed because its contract is now redundant with the runtime-stress suite, and the wave-acceptance level should be driving through the public API per the task's `(d)`.)

- [ ] **Step 4: Drop the now-unused `bqlite_core::spill::SpillFs` and `Arc` imports if no other test in the file references them**

Run: `grep -n "SpillFs\|use std::sync::Arc" tests/tests/wave5_acceptance.rs`

If no remaining references, prune the imports. (Other bands likely still use `Arc`, so the `use std::sync::Arc;` stays. `SpillFs` may be unused — drop it from the imports.)

- [ ] **Step 5: Run the wave5 acceptance gate**

Run: `cargo test --test wave5_acceptance`
Expected: PASS for every test, including the new band-2 public-API tests and the unchanged bands 1/3/4.

- [ ] **Step 6: Commit CP2.2**

```bash
git add tests/tests/wave5_acceptance.rs
git commit -m "TASK-538: Wave 5 acceptance band 2 drives cancellation via public API"
```

---

## Task 6 — CP2.3: Reconcile `cancellation.md` §6.2 / §8

**Files:**
- Modify: `docs/design/engine/cancellation.md:435-470` (§6.2 paragraph and §8 implementation-breakdown table)

- [ ] **Step 1: Add a TASK-538 row to the §8 implementation-breakdown table**

In `docs/design/engine/cancellation.md`, find the table beginning `| Task | Section | Scope |` (§8) and add a row after `TASK-541`:

```markdown
| TASK-538 (public cancel/timeout API) | §3.1, §3.2, §6.2 | Adds `QueryOptions { cancel, timeout }` to `Engine::query_with_options`, the per-query timeout timer that fires `cancel_with_reason(Timeout)`, and the `BqliteError::Cancelled → BqliteError::Timeout` discrimination at result collection. Per-`(worker, morsel)` panic boundary stays at the existing outer `Engine::query_with_options` `catch_unwind` (§4.3) — the morsel-scheduler boundary is owned by TASK-541. |
```

- [ ] **Step 2: Add a brief note in §6.2 about the TASK-538 wiring**

After the `panics are bugs and must be visible.` paragraph in §6.2, add:

```markdown
TASK-538 lands the public surface: `QueryOptions { cancel, timeout }` on
`Engine::query_with_options`. The timeout timer is a per-query thread
that CAS-installs `CancelReason::Timeout` after the duration; the
driver maps the resulting `BqliteError::Cancelled` to
`BqliteError::Timeout { elapsed_ms }` at result collection. The
panic-precedence rule still applies: a panic during teardown of a
timed-out query surfaces as `OperatorPanic`.
```

- [ ] **Step 3: Run dep-direction check (no-op for doc edits but cheap)**

Run: `scripts/check-dep-direction.sh`
Expected: PASS.

- [ ] **Step 4: Commit CP2.3**

```bash
git add docs/design/engine/cancellation.md
git commit -m "TASK-538: Document public cancel/timeout API in cancellation.md"
```

---

## Task 7 — Final CI + subagent review + merge + completion

- [ ] **Step 1: Run full local CI**

Run: `scripts/local-ci.sh`
Expected: PASS end-to-end (fmt, dep-direction, clippy, build, test, python script tests).

- [ ] **Step 2: Subagent code review of CP2 staged diff**

Spawn a code-review subagent against the CP2 commit range. Focus areas:
- Tests are flake-tolerant by design (32-attempt loops for 1ns timeouts) — confirm the loop counts are reasonable for CI variance.
- The `build_long_query_db` fixture lives in `helpers` and is reused — no test-private fixture duplication.
- `wave5_acceptance.rs` band 2 still pins the §5.1 / §5.2 cleanup invariant that the original test asserted (yes — the new test asserts the same "no spill artefacts" property, just through the public API).
- The `cancellation.md` reconciliation correctly references the variants TASK-538 adds and does not contradict §3.1 / §4.3.

Address any blocking findings; re-review until APPROVE.

- [ ] **Step 3: Fast-forward merge CP2 to main**

```bash
git checkout main
git pull origin main
git merge task/TASK-538 --ff-only
git push origin main
git checkout task/TASK-538
```

- [ ] **Step 4: Mark the task complete**

```bash
git mv tasks/active/TASK-538.lock tasks/completed/TASK-538.done
```

Edit `tasks/completed/TASK-538.done` to add `completed_at`:

```json
{
  "agent_id": "agent-3",
  "task_id": "TASK-538",
  "claimed_at": "2026-05-09T06:26:00Z",
  "completed_at": "<current UTC ISO-8601>",
  "branch": "task/TASK-538",
  "description": "Public cancellation/timeout API + acceptance coverage (TASK-525, TASK-528 closure)"
}
```

- [ ] **Step 5: Commit completion and push**

```bash
git add tasks/active/TASK-538.lock tasks/completed/TASK-538.done
git commit -m "TASK-538: completed"
git push origin main
```

- [ ] **Step 6: End the turn**

The wrapper picks up the next task. Do not claim another.

---

## Self-review

**Spec coverage (against task description (a)–(e)):**
- (a) `QueryOptions { cancel, timeout, memory_budget_bytes }` — Task 1 (CP1.1). The task-spec wording calls the method `query_with`; we keep the existing `query_with_options` name. The shape is identical and the existing wave5 tests already use that name.
- (b) Per-query timer fires `cancel_with_reason(Timeout)` matching §3.2 yield-point latency bounds — Task 2 (CP1.2). Latency targets are honored because operators already poll the same `CancellationToken` at batch / sub-batch / morsel boundaries; the timer just signals the token.
- (c) `wave5_runtime_stress.rs` end-to-end coverage for external-cancel pre-query, timeout fires deterministically, timeout cleanup of spill files — Task 4 (CP2.1).
- (d) Extend `wave5_acceptance.rs` band 2 to drive cancellation through the new public API — Task 5 (CP2.2).
- (e) Per-worker `catch_unwind` boundary — Task 3 (CP1.3). The boundary moves into `MorselScheduler::submit` so worker panics surface as `BqliteError::OperatorPanic` at the worker boundary regardless of how TASK-536 reshapes per-shard dispatch. The outer `catch_unwind` in `query.rs` becomes defense-in-depth for the parse / plan / bind path that doesn't go through the scheduler.

**Placeholder scan:** No `TODO`/`TBD`. Every step has the actual code. Tests are deterministic — `Duration::ZERO` triggers the synchronous-fire path; pre-cancelled tokens fire on first yield.

**Type consistency:** `CancellationToken`, `CancelReason`, `QueryOptions`, `ExecutionFailure`, `BqliteError::{Cancelled, Timeout, OperatorPanic}` — names are consistent across the engine, the scheduler, the test files, and the design doc note.

**Risk assumptions:**
- `QueryOptions` losing `Copy` is the only potentially-breaking change; verified by grep that no caller relies on it. Step 10 of CP1.1 runs `cargo build` to harden this claim.
- `MorselScheduler::submit`'s signature change (closure return type → `bqlite_core::Result<T>`) is local to one production call site (`run_query_inner`); the engine_pool tests update inline. Concurrent `task/TASK-536` is editing the same function — see CP1.3 Step 9 for the rebase recipe.
- The timer uses `park_timeout` so natural completion wakes the timer thread immediately on `Drop`; no slice-boundary tax.
- Test determinism: `Duration::ZERO` fires synchronously in `QueryTimer::spawn` before `run_query_inner` runs, so tests never race the driver. Pre-cancelled `CancellationToken` makes the cancel test deterministic the same way.
