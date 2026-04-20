# TASK-439: Advanced Analytics Integration Tests — Implementation Plan

> **For agentic workers:** This plan is for the autonomous agent executing TASK-439. Each top-level section is one checkpoint. Each checkpoint must pass `scripts/local-ci.sh`, undergo subagent code review, and be fast-forward merged to `main` before the next checkpoint starts (per AGENTS.md).

**Goal.** Build the end-to-end integration-test matrix for the Wave 4 query primitives (SESSIONIZE / `WITHIN SESSION`, RETENTION brackets, FIRST/LAST/NTH, SAMPLE, joined-source scans, cohort semi-joins via `IN QUERY` / alias, ATTRIBUTE left-unnest) against the real `Engine::query` text surface, with exact downstream-aggregate assertions on realistic fixtures.

**Architecture.** One Rust test binary per feature cluster, all under `tests/tests/`. Cargo auto-discovers every `tests/tests/*.rs` — no manifest edits. Fixture data is built in-line via `INSERT` statements (matching the `wave4_delete_compaction.rs` precedent) so each test file is self-contained and reasons about one feature surface. Shared helpers (fixture DB bootstrap, row-to-vec collectors) live inside each file for locality; no new additions to `tests/src/common.rs` are required.

**Tech.** `bqlite_engine::Engine` + `bqlite_storage::Database` as the text-in/rows-out surface; `bqlite_tests::common::TempDb` for isolation; `arrow::array` for column downcasts; `#[test]` only (no property tests — the property surfaces for these operators already live in the per-crate test suites). The `bqlite-tests` crate already depends on everything we need; no `Cargo.toml` edits.

**Task description (TASKS.md §439).** "End-to-end integration matrix for the new query primitives: session boundary edge cases, `WITHIN SESSION`, RETENTION bracket semantics (including cumulative mode), FIRST/LAST/NTH with candidate predicates, event-type lists, and `lookback:` widening, deterministic fraction-only SAMPLE behavior, joined-source queries, cohort semi-joins, ATTRIBUTE left-unnest semantics, and exact downstream aggregate results on realistic fixtures."

**Design anchors.**
- `docs/design/operators/sessionize.md` §5 (boundary rules), §14.1 (edge-case matrix)
- `docs/design/operators/event-select-sample.md` — FIRST/LAST/NTH candidate predicates, `lookback:`, SAMPLE fraction-only contract
- `docs/design/operators/attribute.md` — sliding-window deque, three-way row shape, scan widening
- `docs/design/language/cohorts-aliases-joins.md` — `IN QUERY` / alias equivalence, joined-source JOIN, multi-column IN
- `docs/design/query-language.md` §6.3 (RETENTION sugar), §8 (SESSIONIZE surface), §14 (FIRST/LAST/NTH/SAMPLE), §14.3 (ATTRIBUTE), §17 (IN forms), §19 (JOIN)

**Output convention.** TASKS.md lists the output as `tests/integration/advanced_analytics/`. The established convention in this repo (see `tests/tests/wave4_delete_compaction.rs` for the TASK-440 companion) is a flat `tests/tests/*.rs` layout; Cargo's auto-discovery picks up new files with zero manifest churn. I will follow that convention — files named `wave4_advanced_analytics_*.rs` — and note the deviation from the literal TASKS.md path in each commit message for the auditor.

---

## Checkpoint 1 — SESSIONIZE + `WITHIN SESSION`

**File created:** `tests/tests/wave4_advanced_analytics_sessionize.rs`

**What it asserts.** The user-visible contract of SESSIONIZE through the engine plus its composition with MATCH `WITHIN SESSION` and downstream STATS. Covers the §14.1 edge-case matrix rows that are observable at the integration boundary.

**Test matrix (one `#[test]` each).**

1. `gap_exclusive_boundary_keeps_adjacent_events_in_same_session` — three events at `T0`, `T0 + gap`, `T0 + gap + 1ns`. Assert `session_id` = `[1, 1, 2]` and `session_duration` values match `max_ts - min_ts` per §6.3 (sessionize.md).
2. `single_event_entity_is_single_zero_duration_session` — one event → `session_id = 1`, `session_duration = 0` (§6.4).
3. `all_events_separate_sessions_yield_singletons` — events all separated by `> gap` → N one-event sessions, each `session_duration = 0`.
4. `entity_boundary_resets_session_id_to_one` — two entities interleaved in source order; assert each entity's `session_id` sequence starts at 1 (§5.5).
5. `end_event_closes_current_session` — `end: logout` with events `[click, view, logout, click]` at deltas well under the gap. Assert `session_id = [1, 1, 1, 2]`; the logout belongs to session 1 (sessionize.md §5.2: "end event belongs to the session it closes" — the logout is the **last event of the current session**, and the following event starts a new session).
6. `end_event_list_closes_on_any_listed_type` — `end: (logout, timeout)`. Mixed sequence with both end types. Assert membership rule is OR (§5.4).
7. `gap_plus_end_event_on_same_event_produces_singleton` — §5.3: gap strictly greater than threshold AND the arriving event is in `end:`. Assert the gap closes the prior session; the arriving event becomes its own 1-event session with `session_duration = 0`.
8. `within_session_match_expires_across_boundary` — `SESSIONIZE(gap: 1h) | MATCH FIRST SEQUENCE(search THEN checkout) WITHIN SESSION`. Two entities: entity A has `search` and `checkout` inside the same session (matches); entity B has `search`, a gap > 1h, then `checkout` (does not match — the gap closes the session and the candidate expires per query-language.md §30.2).
9. `within_session_match_composes_with_downstream_stats` — extends (8) with `| STATS matched = COUNT(*)` and asserts the exact matched count equals the number of entities where the search→checkout pair fits in one session.
10. `sessionize_stats_per_entity_session_count_uses_group_by` — the §6.2 user-facing caveat: `STATS sessions = COUNT_DISTINCT(session_id) GROUP BY entity_id`. Assert the per-entity session counts match the per-entity ground truth.

**Fixture pattern.** Entities with interleaved event types and deterministic timestamps relative to a `T0` anchor (mirroring `wave4_delete_compaction.rs`).

**Exit criteria for CP1.**
- `cargo test -p bqlite-tests --test wave4_advanced_analytics_sessionize`
- `scripts/local-ci.sh`
- Subagent review approves.
- `git checkout main && git pull && git merge task/TASK-439 --ff-only && git push origin main`

---

## Checkpoint 2 — RETENTION, FIRST/LAST/NTH, SAMPLE

**File created:** `tests/tests/wave4_advanced_analytics_event_select.rs`

**What it asserts.** RETENTION sugar (standard + cumulative), FIRST/LAST/NTH candidate-predicate + event-list + `lookback:` semantics, deterministic fraction-only SAMPLE. All through the engine text surface.

**Test matrix.**

1. `retention_standard_brackets_produces_expected_rates` — three entities, signup on day 0, purchases landing on days 2, 9, 20 respectively. `RETENTION(entry: signup, activity: purchase, brackets: [1d, 7d, 14d, 30d])`. Assert the per-bracket `retention_rate` column against the hand-computed expected values using the desugaring rule (query-language.md §6.3). Output column names are literally `bracket` and `retention_rate`, pinned by the desugarer at `crates/bqlite-planner/src/opt/desugar_retention.rs:155`.
2. `retention_cumulative_brackets_monotone` — same fixture with `cumulative: true`. Assert rates are non-decreasing across brackets (cumulative invariant) and match the hand-computed expected values.
3. `retention_no_activity_entity_still_in_denominator` — entity with signup but no purchase contributes to the denominator but not the numerator. Assert the per-bracket rate reflects this.
4. `first_returns_first_event_per_entity` — `FIRST(login)`. Assert one output row per entity whose login events exist, with the earliest `ts`.
5. `first_with_candidate_predicate_filters_before_selection` — `FIRST(purchase WHERE amount > 100)`. Entity has purchases 50, 150, 200; assert the `amount = 150` row is selected (not the overall first purchase).
6. `first_with_event_type_list_matches_any` — `FIRST((login, sso_login, mobile_login))`. Assert the first event whose `event_type` is in the list is returned per entity.
7. `first_without_lookback_bounded_by_outer_range` — outer range `LAST 7d`; entity's earliest signup is 30d ago. Assert `FIRST(signup)` with no `lookback:` does NOT return the pre-range signup (per §14.1 "no hidden default lookback").
8. `first_with_lookback_widens_scan_range` — same fixture as (7) but `FIRST(signup, lookback: 60d)`. Assert the pre-range signup is now returned (per §14.1).
9. `last_returns_last_event_per_entity` — `LAST(page_view)` with multiple page_views per entity; assert the latest one is selected.
10. `nth_returns_third_matching_event` — `NTH(purchase WHERE amount > 0, 3)`. Build fixtures where some entities have fewer than three matching purchases (→ no output row) and some have more. Assert the correct rows.
11. `sample_fraction_zero_produces_no_rows` — `SAMPLE(fraction: 0.0)`. Assert zero output rows (empty-set contract).
12. `sample_fraction_one_passes_through` — `SAMPLE(fraction: 1.0)`. Assert the row set equals the un-sampled scan's row set (entity-for-entity).
13. `sample_is_deterministic_across_runs` — two successive `SAMPLE(fraction: 0.5)` queries return the same entity set. This is the stability contract (event-select-sample.md §XXX / query-language.md §14.2 "Determinism").
14. `sample_population_invariance_with_filter` — `SAMPLE(fraction: 0.5) | WHERE event_type = 'purchase'` vs `WHERE event_type = 'purchase' | SAMPLE(fraction: 0.5)`. Assert the resulting entity sets are identical (query-language.md §14.2 "Population invariance").

**Exit criteria for CP2.**
- `cargo test -p bqlite-tests --test wave4_advanced_analytics_event_select`
- `scripts/local-ci.sh`
- Subagent review approves.
- FF-merge to main.

---

## Checkpoint 3 — ATTRIBUTE, joined-source, cohorts, downstream aggregates

**File created:** `tests/tests/wave4_advanced_analytics_attribute_cohort_join.rs`

**What it asserts.**
- ATTRIBUTE left-unnest semantics (1 row per conversion with no touchpoints; N rows for N touchpoints).
- ATTRIBUTE scan widening via `window:`.
- Joined-source query surface (`events JOIN purchases`), with table-qualified references.
- Cohort semi-joins through `IN QUERY (...)` and `IN <alias>`, and multi-column IN.
- Exact downstream aggregate results across the above.

**Test matrix.**

1. `attribute_unattributed_conversion_emits_left_unnest_row` — one entity with a conversion (`purchase`) but zero qualifying touchpoints in the window. Assert one output row with `touchpoint_ts IS NULL` and `touchpoint_key IS NULL` (§14.3 LEFT-UNNEST).
2. `attribute_multiple_touchpoints_produces_n_rows` — one entity, one conversion, three `ad_click`s inside the window. Assert three rows, each with the respective `touchpoint_ts` and `touchpoint_key` from the `channel` column. Sort the observed rows by `touchpoint_ts` before comparison — ATTRIBUTE's deque does not guarantee a downstream row order absent an explicit `ORDER BY`.
3. `attribute_window_widens_scan_backward` — outer range `LAST 10d`, conversion at day 1 (close to range start), qualifying touchpoint at day -5 (outside outer range). With `window: 30d`, the planner widens the scan backward so the pre-range touchpoint is visible (query-language.md §14.3 "Scan widening"). Assert the pre-range touchpoint appears in the output. **Control assertion (same test):** run a parallel query with the same outer range and `window: 1d` (too narrow to reach the pre-range touchpoint) and assert the touchpoint is absent / the row emits LEFT-UNNEST shape. This distinguishes scan-widening from the outer range being accidentally wider than expected.
4. `attribute_null_touchpoint_key_is_distinct_from_unattributed` — conversion with a qualifying touchpoint whose `channel` is NULL. Assert the emitted row has `touchpoint_ts IS NOT NULL` and `touchpoint_key IS NULL`, distinct from the LEFT-UNNEST shape (§14.3 final paragraph).
5. `attribute_downstream_stats_count_per_channel` — multiple conversions with mixed channels. `| WHERE touchpoint_ts IS NOT NULL | STATS attributions = COUNT(*) GROUP BY touchpoint_key`. Assert the exact per-channel counts.
6. `joined_source_table_qualified_select_returns_entity_aligned_rows` — two tables (`events`, `purchases`) sharing entity key. `events JOIN purchases | WHERE events.event_type = 'signup' | SELECT events.entity_id AS uid, purchases.amount AS amt`. Assert the row set matches the hand-computed cross-table merge.
7. `joined_source_sequence_match_across_tables` — `events JOIN purchases | MATCH FIRST SEQUENCE(events.signup THEN purchases.purchase) WITHIN 7d | STATS matched = COUNT(*)`. Assert the exact count of entities that converted from signup to purchase within 7d.
8. `cohort_in_query_filters_downstream_scan` — `events | WHERE entity_id IN QUERY (events | WHERE event_type = 'premium_signup' | SELECT entity_id)`. Assert only entities with a `premium_signup` event appear in the output.
9. `cohort_alias_reference_matches_in_query` — the same cohort defined via a leading `premium = events | WHERE event_type = 'premium_signup' | SELECT entity_id` alias plus `events | WHERE entity_id IN premium`. Assert the alias form's result equals the inline `IN QUERY` form's result (the §2.5 equivalence claim from cohorts-aliases-joins.md).
10. `cohort_multi_column_in_query` — `WHERE (user_id, day) IN QUERY (events | STATS first_active = MIN(ts) GROUP BY user_id, QUANTIZE(ts, 1d) AS day | SELECT user_id, day)`. Assert the row set equals the hand-computed first-active-day restriction (query-language.md §17.4).
11. `joined_source_plus_cohort_plus_aggregate_exact_row_count` — the full stack: joined source, cohort filter, and a terminal STATS. Serves as the Wave 4 correctness gate stand-in for this task (TASK-442 owns the wave-level one).

**Exit criteria for CP3.**
- `cargo test -p bqlite-tests --test wave4_advanced_analytics_attribute_cohort_join`
- `scripts/local-ci.sh`
- Subagent review approves.
- FF-merge to main.
- Move `tasks/active/TASK-439.lock` → `tasks/completed/TASK-439.done` with `completed_at`, commit, push. End turn.

---

## Shared Implementation Notes

- **Fixture idiom.** Each test file opens with `CREATE_EVENTS: &str`, `CREATE_PURCHASES: &str` (only in CP3), `T0: i64 = 1_700_000_000_000_000_000`, and a `fresh_db(label)` helper that creates the temp dir, creates the tables, and returns `(TempDb, Database, Engine)`. Mirrors `wave4_delete_compaction.rs` for consistency.
- **Asserting rows.** For exact-row comparisons, downcast columns via `StringViewArray` / `Int64Array` / `TimestampNanosecondArray` and collect into `Vec<_>` for comparison. Existing precedent: `collect_user_ids` in `wave4_delete_compaction.rs`.
- **No new deps / no shared-file churn.** CP1–CP3 add only new test binaries. No edits to `tests/Cargo.toml`, `tests/src/lib.rs`, or any crate source. This is the ideal checkpoint shape (AGENTS.md §Checkpoint Discipline).
- **No design-doc drift.** These tests exercise the existing specs literally. If a test reveals a spec/implementation discrepancy, surface it via `[NEEDS INPUT]` rather than editing the test to accommodate divergence (AGENTS.md behavioral requirement #4 + `docs/core-beliefs.md` on fixing code, not tests).
- **Spec-to-test reconciliation.** Before each checkpoint's commit, re-read the relevant design-doc section and confirm the test table matches the spec literally. Record any deviations in the commit message.

## Self-Review

1. **Coverage vs. TASKS.md §439.**
   - "session boundary edge cases" → CP1 tests 1–7.
   - "`WITHIN SESSION`" → CP1 tests 8–9.
   - "RETENTION bracket semantics (including cumulative mode)" → CP2 tests 1–3.
   - "FIRST/LAST/NTH with candidate predicates, event-type lists, and `lookback:` widening" → CP2 tests 4–10.
   - "deterministic fraction-only SAMPLE behavior" → CP2 tests 11–14.
   - "joined-source queries" → CP3 tests 6–7 + 11.
   - "cohort semi-joins" → CP3 tests 8–10.
   - "ATTRIBUTE left-unnest semantics" → CP3 tests 1–5.
   - "exact downstream aggregate results on realistic fixtures" → CP1 test 9–10, CP2 test 1–3, CP3 tests 5, 7, 11.
2. **Placeholder scan.** No TBDs, no "add appropriate error handling," every test spells out its input shape and expected assertion.
3. **Type consistency.** All tests go through `Engine::query`; output is `ExecutionResult`. Column downcasts use the Arrow arrays the schema actually carries (`StringViewArray` for strings per §Performance Conventions in CLAUDE.md; `Int64Array` for integers and sessions; `TimestampNanosecondArray` for `ts` / `conversion_ts` / `touchpoint_ts`).
