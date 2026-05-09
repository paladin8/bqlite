# Wave 5 Hard Task Completion Audit

**Date**: 2026-05-08  
**Auditor**: Codex  
**Scope**: every Wave 5 `[HARD]` task in `TASKS.md` (`TASK-501` through `TASK-529`, plus `TASK-599`)

**Status legend**

- `SUCCESS`: the task's intended output appears landed and materially evidenced in code/tests/docs.
- `PARTIAL`: the core scaffold exists, but an acceptance gap or a self-admitted placeholder remains.
- `FAIL`: the current tree contradicts the task's own acceptance bar.

## High-level conclusion

Most of the Wave 5 design work and most of the storage/operator/planner implementation work appear to have landed successfully. The biggest problems are concentrated in the runtime/quality gates rather than in the core feature code:

- `TASK-523` / `TASK-524` are still scaffold-level in important ways.
- `TASK-525` / `TASK-528` pass locally, but they do not cover every scenario their task text requires.
- `TASK-526` added the Wave 5 benches, but the CI regression gate was not actually refreshed to run them.
- `TASK-599` does not satisfy its own hard-gate criteria.

## Task-by-task status

### Design tasks

| Task | Status | Notes |
|------|--------|-------|
| `TASK-501` | `SUCCESS` | `docs/design/engine/memory-budget.md` exists and is referenced by downstream memory/spill/runtime work. |
| `TASK-502` | `SUCCESS` | `docs/design/engine/spill.md` exists and the spill-vs-fail policy is reflected in sort/cohort/ingest code and tests. |
| `TASK-503` | `SUCCESS` | `docs/design/engine/operator-fusion.md` exists and the fused stateless segment/stateful fusion work cites and follows it. |
| `TASK-504` | `SUCCESS` | `docs/design/planner/optimizer-direction.md` exists and the optimizer framework/rule pack align with it. |
| `TASK-505` | `SUCCESS` | `docs/design/engine/cancellation.md` exists and its structured-error/warning cleanup model is reflected in `TASK-511` code/tests. |
| `TASK-506` | `SUCCESS` | `docs/design/engine/morsel-scheduler.md` exists and clearly drove the scheduler scaffold, even though some downstream runtime work remains partial. |

### Implementation tasks

| Task | Status | Notes |
|------|--------|-------|
| `TASK-508` | `SUCCESS` | System-column materialization is present end-to-end and `wave5_system_columns` passes locally. The remaining ignored joined-source `MATCH` test is a separate planner/matcher issue, not a system-column regression. |
| `TASK-510` | `SUCCESS` | Query-scoped memory tracking, reservations, and typed `MemoryBudgetExceeded` surfacing appear implemented and tested. |
| `TASK-511` | `SUCCESS` | Structured execution errors and the warning channel are present; `tests/warning_channel.rs` and runtime stress coverage back it. |
| `TASK-512` | `SUCCESS` | External ingest spill code exists in `crates/bqlite-storage/src/ingest/partitioner.rs`. The implementation looks landed, although later gates under-cover it. |
| `TASK-513` | `SUCCESS` | Sort spill/run merge support exists and Wave 5 tests treat it as working behavior. |
| `TASK-514` | `SUCCESS` | Cohort materialization follows the documented fail-fast policy under low budgets; acceptance coverage exercises that policy. |
| `TASK-515` | `SUCCESS` | Copy-budget instrumentation and null-mask preservation appear landed; they are referenced by the zero-copy bench suite and downstream scan path work. |
| `TASK-516` | `SUCCESS` | Dictionary/RLE selection-first filtering appears implemented and is part of the compiled Wave 5 bench set. |
| `TASK-517` | `SUCCESS` | Late materialization / no-`interleave` merge path appears landed and is exercised by Wave 5 equivalence coverage. |
| `TASK-518` | `SUCCESS` | Fused stateless segment scaffold is present and backed by `tests/fused_segment_bind.rs`. |
| `TASK-519` | `SUCCESS` | Filter / Project / Limit were moved onto the fused path and operator-level fused bench coverage exists. |
| `TASK-520` | `SUCCESS` | Stateful-to-aggregate fusion for Sessionize / EventSelect / Attribute appears landed and has dedicated bench/test evidence. |
| `TASK-521` | `SUCCESS` | Optimizer framework, rule registry, and trace surface are present. |
| `TASK-522` | `SUCCESS` | Cohort/entity pushdown is present and `wave5_cohort_pushdown` coverage exists. |
| `TASK-523` | `PARTIAL` | The scheduler scaffold exists, but `Engine::query` still dispatches one degenerate whole-database task per query rather than real per-shard/per-morsel data-plane work. |
| `TASK-524` | `PARTIAL` | `--explain-perf` exists, but skew/worker rows are explicitly still all-zero placeholders and CPU counters are still stubbed. |
| `TASK-525` | `PARTIAL` | `wave5_runtime_stress` passes locally, but the file itself carves out public timeout coverage and ingest spill, and its snapshot-isolation band does not exercise concurrent DELETE/query on the same database under real scheduling. |
| `TASK-526` | `FAIL` | The Wave 5 bench targets exist and compile, but `.github/workflows/bench.yml` still runs only Wave 2/3 benches in baseline, PR gate, and reference jobs. |
| `TASK-527` | `SUCCESS` | The scan-adjacent rule pack appears landed in planner code and is reflected in the audit/optimizer docs. |
| `TASK-529` | `SUCCESS` | BRACKETS runtime emission now looks materially complete: matcher output has bracket logic, EXPLAIN carries bracket fields, and RETENTION tests assert real per-bracket rates. |
| `TASK-528` | `PARTIAL` | `wave5_acceptance` passes locally, but the file explicitly says ingest partitioner spill is not covered directly and the cancellation/timeout band is contract-level rather than end-to-end through the public query API. |
| `TASK-599` | `FAIL` | The audit output assigns multiple sub-`B` grades without named owners, remediation plans, or sign-off, and it overstates Wave 5 bench CI coverage. |

## Findings

### 1. `TASK-523` is still a scheduler scaffold, not the Wave 5 multicore checkpoint

`TASKS.md` says `TASK-523` is "the main multi-core execution checkpoint for Wave 5" and calls for engine-side morsel generation, work queueing, worker handoff, and per-shard partial-aggregate ownership (`TASKS.md:1329-1331`).

The current engine still says otherwise:

- `crates/bqlite-engine/src/query.rs:454-487` explicitly says v1 dispatches every query as "one degenerate whole-database task" and records one default worker snapshot.
- `benches/wave5/morsel_skew.rs:3-18` repeats that the scheduler currently dispatches one whole-database task per query and that real per-worker snapshots are future work.

Assessment: the scaffolding landed, but the task's intended runtime payoff did not. I would treat `TASK-523` as partially complete, not successfully complete.

### 2. `TASK-524` surfaces perf rows, but key Wave 5 metrics are still placeholders

`TASKS.md` says `TASK-524` should implement selection-vector, morsel-skew, worker-spread, spill, and sampled CPU-cost metrics (`TASKS.md:1333-1335`).

The perf module documents a narrower reality:

- `crates/bqlite-engine/src/perf.rs:21-31` says morsel/skew/worker rows are "present as fields, all-zero today" and CPU-cost rows remain stubbed until real platform counters are plugged in.
- `benches/wave5/morsel_skew.rs:15-19` says the bench cannot assert `entity_event_skew_p99` yet because the scheduler does not populate real per-worker snapshots.

Assessment: `--explain-perf` is present, and spill/selection metrics are useful, but the task is still only partially realized.

### 3. `TASK-525` passes, but misses required end-to-end scenarios

`TASKS.md` requires `TASK-525` to stress hard budget exhaustion, spill fallback, concurrent DELETE/query snapshot isolation under real runtime scheduling, timeout cleanup, and warning overflow (`TASKS.md:1337-1340`).

The test file is more limited than that requirement:

- `tests/tests/wave5_runtime_stress.rs:12-29` explicitly marks public timeout API and ingest partitioner spill out of scope.
- `tests/tests/wave5_runtime_stress.rs:434-486` covers delete-between-queries and concurrent queries on two separate database paths, but not concurrent DELETE/query on the same database under scheduler pressure.

Assessment: useful suite, green locally, but not a full closure of the task text.

### 4. `TASK-526` did not refresh the actual bench gate

`TASKS.md` requires new Wave 5 benchmark groups and CI baselines, explicitly extending the existing regression gate (`TASKS.md:1342-1345`).

The local bench assets are there:

- The Wave 5 bench binaries compile locally via `cargo bench -p bqlite-benches --no-run --bench zero_copy_scan --bench stateful_aggregate_fusion --bench morsel_skew --bench spill_overhead --bench cohort_pushdown`.

But the workflow is still stale:

- `.github/workflows/bench.yml:63-75` runs only Wave 2/3 benches on `main`.
- `.github/workflows/bench.yml:121-133` runs only Wave 2/3 benches in the PR regression gate.
- `.github/workflows/bench.yml:216-228` runs only Wave 2/3 benches in the reference job.

Assessment: this task is not successfully completed. The new benches exist, but the required regression-gate refresh is missing.

### 5. `TASK-528` is green locally, but not fully end-to-end

`TASKS.md` says `TASK-528` must verify sort, ingest, and cohort behavior under the chosen spill policy, prove cancellation/timeout cleanup on a long-running query, and validate fused/zero-copy correctness (`TASKS.md:1385-1388`).

The test file explicitly narrows that scope:

- `tests/tests/wave5_acceptance.rs:14-21` says cancellation/timeout cleanup is only checked at the contract level because the public query API lacks per-query cancellation/timeout control.
- `tests/tests/wave5_acceptance.rs:23-34` says ingest partitioner spill is not directly covered.

Assessment: the acceptance gate is valuable and passes locally, but it is not the full wave gate the task text describes.

### 6. `TASK-599` fails its own hard-gate standard

`TASKS.md` makes the Wave 5 audit a blocker: every crate should be at least `B` across all dimensions, and any below-`B` grade requires a named owner, concrete remediation plan, and human sign-off (`TASKS.md:1392-1395`).

The output file does not meet that bar:

- `docs/quality-score.md:26-35` assigns sub-`B` grades to multiple crates/dimensions, including `bqlite` (`C`, `C+`, `C`), several benchmark columns at `C`/`C+`, and `bqlite-ffi` (`C` across the board).
- `docs/quality-score.md:136-143` also claims Bench CI covers the new Wave 5 groups, but `.github/workflows/bench.yml:63-75`, `121-133`, and `216-228` show the workflow still runs only Wave 2/3 benches.

Assessment: this is not a successful quality gate. It is a useful audit document, but it records blocker-grade outcomes without the blocker-handling the task required.

## Suggested changes

- Re-open or replace `TASK-523` with a concrete "real morsel dispatch" follow-up that actually splits data-plane work across shards/morsels and records non-default worker snapshots.
- Finish `TASK-524` by wiring real skew/worker metrics into query teardown, then upgrade `benches/wave5/morsel_skew.rs` from a wall-clock-only tripwire to metric assertions.
- Extend `tests/tests/wave5_runtime_stress.rs` with a same-database concurrent DELETE/query isolation test and with ingest-spill coverage, or explicitly split those scenarios into new follow-up tasks instead of claiming `TASK-525` complete.
- Extend `tests/tests/wave5_acceptance.rs` so the acceptance gate directly exercises ingest partitioner spill and an end-to-end cancellation/timeout path through the public API once that API exists.
- Update `.github/workflows/bench.yml` so baseline, PR gate, and reference jobs execute the Wave 4/5 bench groups that `TASK-526` added.
- Rework `docs/quality-score.md` so it either meets `TASK-599`'s acceptance bar or includes the required named owners, remediation plans, and sign-off entries for every sub-`B` grade.

## Verification performed

- `cargo test --test wave5_acceptance --quiet` -> `9 passed`
- `cargo test --test wave5_runtime_stress --quiet` -> `19 passed`
- `cargo test --test wave5_system_columns --quiet` -> `13 passed`
- `cargo bench -p bqlite-benches --no-run --bench zero_copy_scan --bench stateful_aggregate_fusion --bench morsel_skew --bench spill_overhead --bench cohort_pushdown` -> compiled all 5 Wave 5 bench binaries

## Bottom line

If the question is "did the hard Wave 5 work mostly land?", the answer is yes.

If the question is "can we treat every hard Wave 5 task as successfully completed?", the answer is no. The codebase still has open closure gaps around real multicore scheduling, real skew metrics, Wave 5 bench CI coverage, full end-to-end acceptance coverage, and the Wave 5 quality gate itself.
