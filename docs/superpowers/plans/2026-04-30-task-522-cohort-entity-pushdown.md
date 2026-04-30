# TASK-522 — Cohort/entity pushdown into scan

> **For agentic workers:** the wrapper assigns ownership of TASK-522 via `tasks/active/TASK-522.lock`. Each checkpoint must pass `scripts/local-ci.sh`, be reviewed by a code-review subagent, and be fast-forward merged to `main` before the next checkpoint starts (see AGENTS.md).

**Goal:** When a `SubqueryFilter` materializes a cohort whose tuple shape is a single entity-id column, push the cohort's entity-id set into the outer scan as a `ScanConjunct::EntityIn` so storage can skip row groups whose entity-id zone-map doesn't overlap the cohort. Correctness is unchanged — the post-scan `SubqueryFilterOperator` probe stays in place — and the pushdown only runs when a small-cohort gate passes.

**Architecture:**
1. Extend `bqlite_core::storage::ScanConjunct` with a non-exhaustive `EntityIn { column, values, set_min, set_max }` variant. `accepts_zone` rejects when `[set_min, set_max] ∩ [zone.min, zone.max] = ∅`. Construction precomputes set bounds so per-row-group acceptance stays O(1).
2. Engine-side, after `bind_subquery_filter` materializes a cohort, decide if pushdown applies via the `optimizer-direction.md §7` row-9 gate: shape (single-column LHS that is a direct ref to the outer scan's entity-key column) AND size (`< COHORT_PUSHDOWN_MAX_SIZE = 65_536`). If yes, build the `ScanConjunct::EntityIn` and thread it through `bind_physical_with_pushdowns(...)` to the inner `Scan`.
3. The pushdown propagates only through entity-locality-preserving passthrough plan nodes (`Filter`, `Project`, `Limit`, nested `SubqueryFilter`); other nodes drop pending pushdowns. The `ScanOperator` exposes a builder method `with_extra_conjuncts(...)` so the engine can inject conjuncts that don't go through `CompiledExpr`.

**Tech Stack:** Rust 2021, Apache Arrow, `bqlite-core`, `bqlite-storage`, `bqlite-operators`, `bqlite-engine`. Shared types live in `bqlite-core` (no new crate edges).

---

## Constraints from the design docs

- `cohorts-aliases-joins.md §4.3` and `§6.3.1` — entity-id pushdown deferral becomes the work in this task. The post-scan probe path is the correctness baseline; the pushdown is purely a performance optimization (`§4.3` last paragraph: "Performance-only if the pushdown cannot apply; correctness must remain identical to the post-scan probe path." — quoted in TASK-522 description).
- `optimizer-direction.md §7 row 9` — Pass 8 is *size-gated*: push only when `cohort_size < 65_536`. The threshold is a fixed planner constant. Per `§9.1`, this refines `cohorts-aliases-joins.md §4.3`'s unconditional pushdown; both branches stay correct (post-scan probe survives).
- `optimizer-direction.md §12` — Pass 8 lives in `bqlite-engine` (because it must run after cohort materialization, which only the engine sequences). No reverse dependency on `bqlite-engine` from `bqlite-planner`.
- `predicate-pushdown.md §4–§6` — `ScanConjunct` extension is additive (`#[non_exhaustive]` already), and the new variant must implement zone-map acceptance per the existing rule shape. No false negatives: the no-prune-on-ambiguity invariant is non-negotiable.
- `predicate-pushdown.md §11` — explicitly lists "bloom filters / set membership" as additive extension points. `EntityIn` is one such addition.
- TASK-521's optimizer-framework rule registry has only landed CP1 (PlannerStats scaffolding); CP2 (rule registry) is not on `main`. We therefore do not implement Pass 8 as a registered rule today; we implement it as a direct post-cohort decision inside `bind_subquery_filter`. When TASK-521 CP2 lands, a follow-up task can move the gate logic into a `PostCohort` rule without changing semantics. The threshold constant and gate predicates live in one place so the move is a pure refactor.

---

## File structure

| File | Responsibility |
|------|----------------|
| `crates/bqlite-core/src/storage.rs` | Add `ScanConjunct::EntityIn { column, values: Arc<HashSet<PropertyValue>>, set_min, set_max }`. Add the matching `column()` arm and `accepts_zone()` arm. |
| `crates/bqlite-storage/src/zone_map.rs` | Property-style tests covering `EntityIn` acceptance (overlap / disjoint / null-only / partial-null). The hot path (`accepts_row_group_inline`) needs no change — it dispatches via `ScanConjunct::accepts_zone` already. |
| `crates/bqlite-operators/src/scan.rs` | Add `ScanOperator::with_extra_conjuncts(Vec<ScanConjunct>)` builder method. Extend `build_scan_predicate` so an extra-conjunct list can be appended after `CompiledExpr` lowering, producing a single `Arc<dyn Predicate>`. Add unit tests. |
| `crates/bqlite-engine/src/cohort_pushdown.rs` (new) | `try_extract_entity_pushdown(sqf, cohort, entity_key_col)` — gate logic + conversion `ScalarValue → PropertyValue` + bound computation. Lives in engine because the cohort runtime artifact is engine-scoped. Unit tests cover shape/size gating. |
| `crates/bqlite-engine/src/bind.rs` | Thread `pending_pushdowns: Vec<ScanConjunct>` through `bind_physical_with_cache`. `bind_subquery_filter` invokes `try_extract_entity_pushdown` and pushes the conjunct onto the pending list when the gate accepts. `bind_scan` consumes the list. Pass-through dispatch on `Filter`/`Project`/`Limit`/`SubqueryFilter` propagates; every other variant clears the list before recursing. |
| `crates/bqlite-engine/src/lib.rs` | `mod cohort_pushdown;` |
| `tests/tests/wave5_cohort_pushdown.rs` (new) | End-to-end integration tests: result equivalence between pushdown-on and pushdown-off, gate acceptance/rejection by size, multi-segment skip rate (assert at least one row group is pruned when the cohort is much smaller than the table). |
| `docs/design/language/cohorts-aliases-joins.md` | Update `§4.3` and `§5.2 step 6` to reference the size-gated policy from `optimizer-direction.md §7 row 9`; mark `§6.3.1`'s deferral resolved by TASK-522. |
| `docs/design/storage/predicate-pushdown.md` | Add a brief paragraph in `§4` (pushable taxonomy) noting the `EntityIn` extension landed for cohort pushdown; update `§11` to mark the bloom/set hook partially realised. |

---

## Checkpoint plan

The task ships in three checkpoints, each independently mergeable:

- **CP1:** Storage-side `ScanConjunct::EntityIn` shape + zone-map acceptance + tests. No call sites yet — pure additive type change.
- **CP2:** Engine-side gate, conjunct construction, bind threading, scan operator wiring. Includes unit tests for the gate and bind threading. End-to-end behaviour observable via integration tests.
- **CP3:** Integration tests + design-doc reconciliation. Optional benchmark hook deferred to TASK-526 per `TASKS.md` (the Wave 5 bench gate explicitly lists "cohort pushdown savings"); no benchmark code lands in this task.

---

## CP1: `ScanConjunct::EntityIn` + zone-map acceptance

### Task 1.1: Add the variant to `bqlite-core::storage`

**Files:**
- Modify: `crates/bqlite-core/src/storage.rs` — extend `ScanConjunct` enum, `column()`, `accepts_zone()`.

- [ ] **Step 1: Re-read `predicate-pushdown.md §6` so the new variant's acceptance rule mirrors the existing `InSet` pattern (uses [min, max] only; conservative on ambiguity).**

- [ ] **Step 2: Add the `EntityIn` variant. Edit `crates/bqlite-core/src/storage.rs` near the existing `ScanConjunct` enum (currently line 282–326).**

Add a `use std::collections::HashSet;` and `use std::sync::Arc;` if not already present at the top of the file (verify with grep — `Arc` is already used elsewhere in the crate; `HashSet` is added if missing).

Insert the new variant at the bottom of the enum, before the closing brace, and outside the existing `IsNotNull` arm:

```rust
    /// `entity_id ∈ <materialized cohort>` — the runtime form of cohort
    /// entity-id pushdown described in
    /// `docs/design/language/cohorts-aliases-joins.md` §4.3 / §5.2 step 6
    /// and gated per `docs/design/planner/optimizer-direction.md` §7 row 9.
    ///
    /// Constructed by the engine's query coordinator after a
    /// `SubqueryFilter` materializes its cohort. The `values` set is the
    /// cohort's entity-id column, deduplicated; `set_min` / `set_max`
    /// are the precomputed bounds used for O(1) row-group zone-map
    /// acceptance.
    ///
    /// Functionally equivalent to a giant `InSet` on the entity-id
    /// column, but kept structurally distinct for two reasons:
    ///
    /// 1. The post-scan `SubqueryFilterOperator` is the source of truth
    ///    for the row-level probe; this conjunct exists *only* to drive
    ///    zone-map row-group skipping and must not duplicate per-row
    ///    work.
    /// 2. The set lives behind an `Arc<HashSet<...>>` so the same
    ///    materialized cohort can back several `ScanConjunct::EntityIn`
    ///    instances (one per scan that the cohort filters) without a
    ///    deep clone. `InSet` carries `Vec<PropertyValue>` by value,
    ///    which would force a deep clone per scan.
    EntityIn {
        /// Outer scan's entity-key column name (e.g. `"entity_id"`).
        column: String,
        /// Cohort entity-id values (deduplicated). Held behind `Arc`
        /// so the engine can share one set across multiple scans
        /// produced by a `MergeSources` join without cloning.
        values: Arc<std::collections::HashSet<PropertyValue>>,
        /// Pre-computed minimum entity-id in `values`. Carried with
        /// the conjunct so `accepts_zone` does not re-scan the set per
        /// row-group. Required by construction; constructors must
        /// reject empty `values`.
        set_min: PropertyValue,
        /// Pre-computed maximum entity-id in `values`.
        set_max: PropertyValue,
    },
```

- [ ] **Step 3: Extend the `column()` accessor (around line 333).**

```rust
    pub fn column(&self) -> &str {
        match self {
            ScanConjunct::Equal { column, .. }
            | ScanConjunct::NotEqual { column, .. }
            | ScanConjunct::Range { column, .. }
            | ScanConjunct::InSet { column, .. }
            | ScanConjunct::IsNull { column }
            | ScanConjunct::IsNotNull { column }
            | ScanConjunct::EntityIn { column, .. } => column,
        }
    }
```

- [ ] **Step 4: Extend `accepts_zone()` (around line 356) with the new arm. Insert before the final `}` closing the inner `match self`.**

```rust
            // `EntityIn`: accept iff the cohort's value range overlaps
            // the row-group's [min, max]. Pre-computed bounds make this
            // O(1) per row-group regardless of cohort size. NULL rows
            // produce UNKNOWN under set membership and the filter
            // operator drops them; reject row-groups that are all-null.
            ScanConjunct::EntityIn {
                set_min, set_max, ..
            } => {
                nulls < rows
                    && zone.min.as_ref().is_none_or(|m| m <= set_max)
                    && zone.max.as_ref().is_none_or(|x| set_min <= x)
            }
```

- [ ] **Step 5: Add a constructor that enforces non-emptiness and computes bounds. Place after the enum, alongside the existing `accepts_zone` impl block.**

```rust
impl ScanConjunct {
    /// Build an `EntityIn` conjunct, computing `set_min` / `set_max`
    /// from `values`. Returns `None` when `values` is empty — an empty
    /// cohort filters every outer row, but this is handled by the
    /// post-scan `SubqueryFilterOperator` (which probes against an
    /// empty set and rejects every row); pushdown adds nothing in that
    /// degenerate case.
    pub fn entity_in(
        column: String,
        values: Arc<std::collections::HashSet<PropertyValue>>,
    ) -> Option<Self> {
        let mut iter = values.iter();
        let first = iter.next()?.clone();
        let mut min = first.clone();
        let mut max = first;
        for v in iter {
            if v < &min {
                min = v.clone();
            } else if v > &max {
                max = v.clone();
            }
        }
        Some(ScanConjunct::EntityIn {
            column,
            values,
            set_min: min,
            set_max: max,
        })
    }
}
```

- [ ] **Step 6: Build incrementally to confirm the type compiles.**

Run: `cargo build -p bqlite-core`
Expected: clean build with no warnings. Address any clippy lint specific to the new arm.

- [ ] **Step 7: Add unit tests directly in `bqlite-core::storage` (in the existing `#[cfg(test)] mod tests` block).**

Find the existing tests around `ScanConjunct::accepts_zone`. Add a new test fn:

```rust
    #[test]
    fn entity_in_accepts_overlapping_zone() {
        let mut set = std::collections::HashSet::new();
        set.insert(PropertyValue::Int(10));
        set.insert(PropertyValue::Int(20));
        set.insert(PropertyValue::Int(30));
        let conj = ScanConjunct::entity_in("entity_id".into(), Arc::new(set))
            .expect("non-empty");
        let zone = ZoneMap {
            min: Some(PropertyValue::Int(15)),
            max: Some(PropertyValue::Int(25)),
            null_count: 0,
            row_count: 100,
        };
        assert!(conj.accepts_zone(&zone));
    }

    #[test]
    fn entity_in_rejects_disjoint_zone_above() {
        let mut set = std::collections::HashSet::new();
        set.insert(PropertyValue::Int(10));
        set.insert(PropertyValue::Int(20));
        let conj = ScanConjunct::entity_in("entity_id".into(), Arc::new(set))
            .expect("non-empty");
        let zone = ZoneMap {
            min: Some(PropertyValue::Int(50)),
            max: Some(PropertyValue::Int(60)),
            null_count: 0,
            row_count: 100,
        };
        assert!(!conj.accepts_zone(&zone));
    }

    #[test]
    fn entity_in_rejects_disjoint_zone_below() {
        let mut set = std::collections::HashSet::new();
        set.insert(PropertyValue::Int(50));
        set.insert(PropertyValue::Int(60));
        let conj = ScanConjunct::entity_in("entity_id".into(), Arc::new(set))
            .expect("non-empty");
        let zone = ZoneMap {
            min: Some(PropertyValue::Int(10)),
            max: Some(PropertyValue::Int(20)),
            null_count: 0,
            row_count: 100,
        };
        assert!(!conj.accepts_zone(&zone));
    }

    #[test]
    fn entity_in_rejects_all_null_zone() {
        let mut set = std::collections::HashSet::new();
        set.insert(PropertyValue::Int(10));
        let conj = ScanConjunct::entity_in("entity_id".into(), Arc::new(set))
            .expect("non-empty");
        let zone = ZoneMap {
            min: None,
            max: None,
            null_count: 100,
            row_count: 100,
        };
        assert!(!conj.accepts_zone(&zone));
    }

    #[test]
    fn entity_in_accepts_when_zone_bounds_missing_with_some_nonnulls() {
        // Conservative accept: writers always populate bounds when
        // nulls < rows in v1, but a hypothetical partial-bound writer
        // must not be pruned away.
        let mut set = std::collections::HashSet::new();
        set.insert(PropertyValue::Int(10));
        let conj = ScanConjunct::entity_in("entity_id".into(), Arc::new(set))
            .expect("non-empty");
        let zone = ZoneMap {
            min: None,
            max: None,
            null_count: 5,
            row_count: 100,
        };
        assert!(conj.accepts_zone(&zone));
    }

    #[test]
    fn entity_in_string_values_accept_string_zone() {
        // Entity-key columns may be String; verify the rule fires on
        // String PropertyValues, not just Int.
        let mut set = std::collections::HashSet::new();
        set.insert(PropertyValue::String("u3".into()));
        set.insert(PropertyValue::String("u7".into()));
        let conj = ScanConjunct::entity_in("entity_id".into(), Arc::new(set))
            .expect("non-empty");
        let zone = ZoneMap {
            min: Some(PropertyValue::String("u1".into())),
            max: Some(PropertyValue::String("u9".into())),
            null_count: 0,
            row_count: 100,
        };
        assert!(conj.accepts_zone(&zone));
    }

    #[test]
    fn entity_in_constructor_rejects_empty_set() {
        let set = Arc::new(std::collections::HashSet::<PropertyValue>::new());
        assert!(ScanConjunct::entity_in("entity_id".into(), set).is_none());
    }

    #[test]
    fn entity_in_referenced_column_propagates_into_predicate() {
        let mut set = std::collections::HashSet::new();
        set.insert(PropertyValue::Int(1));
        let conj = ScanConjunct::entity_in("entity_id".into(), Arc::new(set))
            .expect("non-empty");
        let predicate = ScanPredicate::new(vec![conj]);
        assert_eq!(
            predicate.referenced.iter().map(String::as_str).collect::<Vec<_>>(),
            vec!["entity_id"]
        );
    }
```

- [ ] **Step 8: Run the new tests.**

Run: `cargo test -p bqlite-core storage::tests::entity_in_`
Expected: all 8 tests pass.

- [ ] **Step 9: Add a property-test smoke-check in `bqlite-storage::zone_map::tests`.** This anchors the new variant to the existing storage zone-map property-test bench:

Locate the existing `#[cfg(test)] mod tests` block in `crates/bqlite-storage/src/zone_map.rs` and add:

```rust
    #[test]
    fn accepts_row_group_inline_with_entity_in_prunes_disjoint_row_group() {
        use std::sync::Arc;
        let mut set = std::collections::HashSet::new();
        set.insert(PropertyValue::Int(1000));
        set.insert(PropertyValue::Int(2000));
        let conj = ScanConjunct::entity_in("entity_id".into(), Arc::new(set))
            .expect("non-empty");
        let predicate = ScanPredicate::new(vec![conj]);

        let schema_columns = vec![
            bqlite_core::ColumnDef::new("entity_id", bqlite_core::BqlType::Int, false),
        ];
        let rg = crate::segment::layout::RowGroupIndex {
            row_count: 100,
            byte_offset: 0,
            byte_length: 1,
            columns: vec![bqlite_core::storage::ColumnChunkMeta {
                column_ordinal: 0,
                encoding: bqlite_core::storage::EncodingDescriptor::Plain,
                compressed: false,
                byte_offset: 0,
                byte_length: 1,
                value_count: 100,
                null_count: 0,
                zone_min: Some(PropertyValue::Int(0)),
                zone_max: Some(PropertyValue::Int(100)),
                dictionary_offset: None,
                dictionary_length: None,
            }],
        };
        assert!(!accepts_row_group_inline(&predicate, &rg, &schema_columns));
    }
```

(Adjust `ColumnChunkMeta { ... }` field set if the struct shape differs — re-check with `grep -n "pub struct ColumnChunkMeta" crates/bqlite-core/src/storage.rs`.)

Also verify `bqlite_core::PropertyValue` is in scope for the test module; if not, add `use bqlite_core::PropertyValue;`.

- [ ] **Step 10: Run the storage-side test.**

Run: `cargo test -p bqlite-storage zone_map::tests::accepts_row_group_inline_with_entity_in`
Expected: PASS.

- [ ] **Step 11: Run the local CI script.**

Run: `scripts/local-ci.sh`
Expected: fmt, dep-direction, clippy, build, test all pass.

- [ ] **Step 12: Subagent code review.**

Dispatch a `superpowers:code-reviewer` subagent on the staged diff. The reviewer evaluates:
- Correctness of the `accepts_zone` rule (no false negatives — every row that could match must keep its row-group alive).
- Whether `entity_in` is a hot-path allocation hazard. Confirm it runs once per cohort/scan, not per row-group.
- Whether `Arc<HashSet<PropertyValue>>` is the right shape (vs `Vec`). Justify per ergonomics in the variant doc-comment.

If the reviewer raises blocking issues, address them before committing.

- [ ] **Step 13: Commit and merge to main.**

```bash
git add crates/bqlite-core/src/storage.rs crates/bqlite-storage/src/zone_map.rs
git commit -m "TASK-522: Add ScanConjunct::EntityIn for cohort pushdown (CP1)

Extends the storage-layer pushdown taxonomy with EntityIn — the runtime
form of cohort entity-id pushdown described in cohorts-aliases-joins.md
§4.3. Constructor pre-computes set bounds so accepts_zone is O(1) per
row group regardless of cohort size. Engine call sites land in CP2."
git push origin task/TASK-522
```

Then:

```bash
git checkout main
git pull origin main
git merge task/TASK-522 --ff-only
git push origin main
git checkout task/TASK-522
```

If `--ff-only` fails, rebase per AGENTS.md.

---

## CP2: Engine bind threading + gate + scan wiring

### Task 2.1: Scan operator extra-conjunct setter

**Files:**
- Modify: `crates/bqlite-operators/src/scan.rs` — add `with_extra_conjuncts(...)` builder; thread its values into `scan_predicate`.

- [ ] **Step 1: Find `build_scan_predicate` (around line 1256). Extract the set of conjuncts into a separate helper so the new builder can append to it.**

Replace the body of `build_scan_predicate` so it returns `Vec<ScanConjunct>` (not the wrapped predicate). Introduce a new wrapper that creates the `Arc<dyn Predicate>`:

```rust
/// Lower a slice of `CompiledExpr` predicates into the conjunct list
/// that backs a `ScanPredicate`. Conjuncts that don't match any
/// pushable shape are silently dropped — they remain as
/// `post_filters` for the scan operator's row-level filter pass.
fn lower_compiled_predicates(predicates: &[CompiledExpr]) -> Vec<ScanConjunct> {
    let mut conjuncts: Vec<ScanConjunct> = Vec::with_capacity(predicates.len());
    for pred in predicates {
        if let Some(conj) = lower_to_conjunct(pred) {
            conjuncts.push(conj);
        }
    }
    conjuncts
}

fn build_scan_predicate(predicates: &[CompiledExpr]) -> Option<Arc<dyn Predicate>> {
    let conjuncts = lower_compiled_predicates(predicates);
    if conjuncts.is_empty() {
        None
    } else {
        Some(Arc::new(ScanPredicate::new(conjuncts)) as Arc<dyn Predicate>)
    }
}
```

- [ ] **Step 2: Update the constructor body (around line 499) to consume the conjunct list directly, so the extra-conjunct builder can append before predicate construction. The cleanest route is to defer `scan_predicate` construction until after `with_extra_conjuncts` runs — which means we hold the conjuncts on the operator and assemble the predicate at `open()` time.**

In the `ScanOperator` struct, add a new field next to `scan_predicate`:

```rust
    /// Engine-injected extra conjuncts (cohort entity-id pushdown,
    /// future bloom hooks). Combined under AND with the conjuncts
    /// derived from `scan_predicates` to form `scan_predicate` at
    /// `open` time.
    extra_conjuncts: Vec<ScanConjunct>,
```

In `with_tombstones_and_scan_path` (the constructor path), replace the existing `let scan_predicate = build_scan_predicate(&scan_predicates);` line with:

```rust
        // Defer predicate assembly to `open()` so callers can use
        // `with_extra_conjuncts` between construction and open.
        let scan_predicate = None::<Arc<dyn Predicate>>;
```

In the struct initializer, add `extra_conjuncts: Vec::new(),`.

- [ ] **Step 3: Add the builder method on `ScanOperator` (next to `with_sample_filter`):**

```rust
    /// Append engine-injected scan conjuncts that cannot be derived
    /// from `CompiledExpr` (e.g. cohort entity-id pushdown — see
    /// `docs/design/language/cohorts-aliases-joins.md` §4.3 and
    /// TASK-522). Combined under AND with the conjuncts the operator
    /// derives from `scan_predicates` at `open()` time.
    ///
    /// Must be called before [`ScanOperator::open`].
    pub fn with_extra_conjuncts(&mut self, extra: Vec<ScanConjunct>) -> &mut Self {
        self.extra_conjuncts.extend(extra);
        self
    }
```

- [ ] **Step 4: Build the actual `scan_predicate` lazily inside `open()`. Find `ScanOperator::open` (the existing predicate-composition site is around line 752 per the explore report).**

Just before the existing `AndPredicate::new` composition with the sample filter, insert:

```rust
        // Assemble the runtime ScanPredicate now that any
        // engine-injected extra conjuncts (cohort pushdown, etc.) have
        // been added. Equivalent to the previous always-compute-at-
        // construction path when `extra_conjuncts` is empty.
        if self.scan_predicate.is_none() {
            let mut conjuncts = lower_compiled_predicates(&self.post_filters);
            conjuncts.extend(std::mem::take(&mut self.extra_conjuncts));
            self.scan_predicate = if conjuncts.is_empty() {
                None
            } else {
                Some(Arc::new(ScanPredicate::new(conjuncts)) as Arc<dyn Predicate>)
            };
        }
```

(Re-read the open function carefully to insert at the exact right spot; the existing `scan_predicate` field is referenced when composing with the sample filter — the assembly must happen before that reference.)

- [ ] **Step 5: Build incrementally to confirm.**

Run: `cargo build -p bqlite-operators`
Expected: clean build.

- [ ] **Step 6: Unit-test the extra-conjunct path.**

Locate the existing `#[cfg(test)] mod tests` block and add:

```rust
    #[test]
    fn with_extra_conjuncts_appends_to_runtime_predicate() {
        use std::sync::Arc;
        let reader = test_reader_with_one_row();
        let mut op = ScanOperator::full_scan(reader).expect("scan");
        let mut set = std::collections::HashSet::new();
        set.insert(bqlite_core::PropertyValue::Int(1));
        let conj = bqlite_core::storage::ScanConjunct::entity_in(
            "entity_id".into(), Arc::new(set),
        )
        .expect("non-empty");
        op.with_extra_conjuncts(vec![conj]);
        op.open().expect("open");
        // Predicate is assembled at open time; assert it's now Some.
        assert!(op.scan_predicate.is_some());
    }
```

(`test_reader_with_one_row` should be the existing helper used by other tests in this module; if a helper with a different name exists, use it.)

- [ ] **Step 7: Run the test.**

Run: `cargo test -p bqlite-operators scan::tests::with_extra_conjuncts_appends_to_runtime_predicate`
Expected: PASS.

- [ ] **Step 8: Run all scan tests.**

Run: `cargo test -p bqlite-operators scan::`
Expected: every existing test still passes (the lazy-assembly path is a refactor that preserves behaviour when `extra_conjuncts.is_empty()`).

### Task 2.2: Cohort-pushdown gate module

**Files:**
- Create: `crates/bqlite-engine/src/cohort_pushdown.rs`
- Modify: `crates/bqlite-engine/src/lib.rs` — `mod cohort_pushdown;`

- [ ] **Step 1: Create the new file.**

```rust
//! Engine-side gate for the cohort entity-id pushdown (TASK-522).
//!
//! Implements the post-cohort decision described in
//! `docs/design/planner/optimizer-direction.md` §7 row 9 and
//! `docs/design/language/cohorts-aliases-joins.md` §4.3 / §6.3.1:
//! after a `SubqueryFilter` materialises its cohort, decide whether
//! the cohort's entity-id column qualifies for scan-side pushdown.
//!
//! ## The gate (two predicates, AND-combined)
//!
//! 1. **Shape**: the LHS of the cohort `IN` is a single column
//!    expression that references the outer scan's entity-key column.
//!    Multi-column tuples (`(entity_id, day) IN ...`) and computed
//!    LHS expressions (`QUANTIZE(ts, 1d) IN ...`) do **not** qualify
//!    in v1 — `cohorts-aliases-joins.md §4.3` defers full multi-key
//!    pushdown.
//! 2. **Size**: the cohort's row count is strictly less than
//!    [`COHORT_PUSHDOWN_MAX_SIZE`]. Larger cohorts skip the
//!    pushdown — an entity-id set with millions of values offers
//!    almost no row-group skipping (every zone overlaps) while
//!    paying the per-row hash-set construction cost.
//!
//! Correctness is preserved regardless of which branch fires: the
//! post-scan `SubqueryFilterOperator` probes the full cohort row by
//! row, so a missed pushdown only loses the pruning optimisation.

use std::sync::Arc;

use bqlite_core::storage::ScanConjunct;
use bqlite_core::{PropertyValue, ScalarValue};
use bqlite_operators::cohort::{CohortHashSet, CohortKey};
use bqlite_planner::compiled::{CompiledExpr, CompiledNode};

/// Exclusive upper bound on cohort size for entity-id pushdown.
/// Cohorts with `size < COHORT_PUSHDOWN_MAX_SIZE` qualify; cohorts at
/// or above the threshold are not pushed.
///
/// Beyond this size the cohort's `[set_min, set_max]` interval almost
/// certainly covers every row-group's entity-id zone, so the pushdown
/// loses its skip benefit while still paying the per-row hashing cost
/// at storage time. Per `optimizer-direction.md` §7 row 9 the
/// threshold is a fixed planner constant; tuning is `§10.4` future
/// work.
pub const COHORT_PUSHDOWN_MAX_SIZE: u64 = 65_536;

/// Try to extract a `ScanConjunct::EntityIn` from a materialised
/// cohort. Returns `None` when either gate predicate is false.
///
/// `lhs_columns` is the [`SubqueryFilterPhysical::lhs_columns`] from
/// the planner — the per-LHS-position compiled expressions whose
/// values the cohort tuple is matched against. `entity_key_col` is
/// the outer scan's entity-key column name (the storage-side conjunct
/// targets this column).
pub fn try_extract_entity_pushdown(
    lhs_columns: &[CompiledExpr],
    cohort: &CohortHashSet,
    entity_key_col: &str,
) -> Option<ScanConjunct> {
    // Shape gate.
    if lhs_columns.len() != 1 {
        return None;
    }
    let lhs = &lhs_columns[0];
    let CompiledNode::Column { name, .. } = &lhs.node else {
        return None;
    };
    if name != entity_key_col {
        return None;
    }
    if cohort.arity() != 1 {
        return None;
    }

    // Size gate.
    let size = cohort.len() as u64;
    if size == 0 {
        return None;
    }
    if size >= COHORT_PUSHDOWN_MAX_SIZE {
        return None;
    }

    // Convert the cohort's first-position scalars to PropertyValue.
    // NULL keys are skipped (not aborted) — `IN` against NULL is
    // UNKNOWN under three-valued logic and the post-scan probe drops
    // those outer rows anyway, so removing them from the entity-id
    // set preserves the no-false-negatives invariant.
    let mut values: std::collections::HashSet<PropertyValue> =
        std::collections::HashSet::with_capacity(cohort.len());
    for key in cohort.iter_keys() {
        let Some(scalar) = key.0.first() else { continue };
        let Some(pv) = scalar_to_property_value(scalar) else {
            continue;
        };
        values.insert(pv);
    }
    if values.is_empty() {
        return None;
    }

    ScanConjunct::entity_in(entity_key_col.to_string(), Arc::new(values))
}

/// Lossless conversion from runtime [`ScalarValue`] (used by
/// `CohortKey`) to the boundary [`PropertyValue`] (used by
/// `ScanConjunct`). Every entity-key type the engine supports is
/// representable in both. NULL keys do not push down — the cohort's
/// post-scan probe drops outer rows whose LHS is NULL anyway.
fn scalar_to_property_value(scalar: &ScalarValue) -> Option<PropertyValue> {
    match scalar {
        ScalarValue::Null => None,
        ScalarValue::Bool(b) => Some(PropertyValue::Bool(*b)),
        ScalarValue::Int(i) => Some(PropertyValue::Int(*i)),
        ScalarValue::Float(f) => Some(PropertyValue::Float(*f)),
        ScalarValue::String(s) => Some(PropertyValue::String(s.clone())),
        ScalarValue::Timestamp(t) => Some(PropertyValue::Timestamp(*t)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bqlite_core::BqlType;

    fn col_expr(name: &str) -> CompiledExpr {
        CompiledExpr {
            node: CompiledNode::Column {
                index: 0,
                name: name.into(),
            },
            result_type: BqlType::Int,
            nullable: false,
        }
    }

    fn cohort_with_int_keys(values: &[i64]) -> Arc<CohortHashSet> {
        let mut set = CohortHashSet::empty(1);
        for &v in values {
            set.insert_for_test(CohortKey(vec![ScalarValue::Int(v)]));
        }
        Arc::new(set)
    }

    #[test]
    fn shape_gate_rejects_multi_column_lhs() {
        let lhs = vec![col_expr("entity_id"), col_expr("day")];
        let cohort = cohort_with_int_keys(&[1, 2, 3]);
        assert!(try_extract_entity_pushdown(&lhs, &cohort, "entity_id").is_none());
    }

    #[test]
    fn shape_gate_rejects_non_column_lhs() {
        let lhs = vec![CompiledExpr {
            node: CompiledNode::Literal(PropertyValue::Int(1)),
            result_type: BqlType::Int,
            nullable: false,
        }];
        let cohort = cohort_with_int_keys(&[1]);
        assert!(try_extract_entity_pushdown(&lhs, &cohort, "entity_id").is_none());
    }

    #[test]
    fn shape_gate_rejects_mismatched_column_name() {
        let lhs = vec![col_expr("user_id")];
        let cohort = cohort_with_int_keys(&[1]);
        assert!(try_extract_entity_pushdown(&lhs, &cohort, "entity_id").is_none());
    }

    #[test]
    fn size_gate_rejects_at_threshold() {
        let lhs = vec![col_expr("entity_id")];
        let values: Vec<i64> = (0..COHORT_PUSHDOWN_MAX_SIZE as i64).collect();
        let cohort = cohort_with_int_keys(&values);
        assert!(try_extract_entity_pushdown(&lhs, &cohort, "entity_id").is_none());
    }

    #[test]
    fn size_gate_rejects_empty_cohort() {
        let lhs = vec![col_expr("entity_id")];
        let cohort = Arc::new(CohortHashSet::empty(1));
        assert!(try_extract_entity_pushdown(&lhs, &cohort, "entity_id").is_none());
    }

    #[test]
    fn happy_path_emits_entity_in_with_correct_bounds() {
        let lhs = vec![col_expr("entity_id")];
        let cohort = cohort_with_int_keys(&[10, 20, 5, 15]);
        let conj = try_extract_entity_pushdown(&lhs, &cohort, "entity_id")
            .expect("gate accepts");
        match conj {
            ScanConjunct::EntityIn {
                column,
                set_min,
                set_max,
                values,
            } => {
                assert_eq!(column, "entity_id");
                assert_eq!(set_min, PropertyValue::Int(5));
                assert_eq!(set_max, PropertyValue::Int(20));
                assert_eq!(values.len(), 4);
            }
            other => panic!("unexpected conjunct: {other:?}"),
        }
    }
}
```

- [ ] **Step 2: Wire the new module.**

Modify `crates/bqlite-engine/src/lib.rs` — locate the existing `mod` declarations and add `mod cohort_pushdown;` (placement matches alphabetical ordering with neighboring `mod` lines).

- [ ] **Step 3: Expose `CohortHashSet::iter_keys` and an `insert_for_test` helper. Add to `crates/bqlite-operators/src/cohort.rs`:**

In the `impl CohortHashSet` block, add:

```rust
    /// Iterate the cohort's keys. Order is unspecified — the cohort
    /// is a hash set. Used by the engine's pushdown extraction
    /// (TASK-522) to copy entity-id values into a `PropertyValue` set.
    pub fn iter_keys(&self) -> impl Iterator<Item = &CohortKey> {
        self.set.iter()
    }

    /// Insert a key directly without going through `from_batches`.
    ///
    /// Test-only — bypasses memory-budget reservation accounting, so
    /// it must not be used in production code paths.
    #[doc(hidden)]
    pub fn insert_for_test(&mut self, key: CohortKey) {
        self.set.insert(key);
    }
```

- [ ] **Step 4: Build incrementally.**

Run: `cargo build -p bqlite-engine`
Expected: clean build.

- [ ] **Step 5: Run the new module's tests.**

Run: `cargo test -p bqlite-engine cohort_pushdown::tests`
Expected: all 6 tests pass.

### Task 2.3: Bind threading

**Files:**
- Modify: `crates/bqlite-engine/src/bind.rs` — thread `pending_pushdowns: Vec<ScanConjunct>` through `bind_physical_with_cache`.

- [ ] **Step 1: Re-read the current `bind_physical_with_cache` (lines 826–969) and `bind_subquery_filter` (1019–1056) so the propagation rules are clear.**

- [ ] **Step 2: Change the signature of `bind_physical_with_cache` to take an additional `pending_pushdowns: &mut Vec<ScanConjunct>` parameter.**

The simplest and safest pattern: callers that recurse pass the vec down; transformations that *break* the entity-id passthrough invariant clear the vec before recursing into their child. The engine's top-level `bind_physical` entry passes a fresh empty vec.

In `crates/bqlite-engine/src/bind.rs`:

```rust
fn bind_physical_with_cache(
    plan: &PhysicalPlan,
    db: &mut Database,
    ctx: &QueryContext,
    cohorts: &mut CohortCache,
    pending_pushdowns: &mut Vec<ScanConjunct>,
) -> Result<Box<dyn PhysicalOperator>> {
    match plan {
        // Scan: drain pushdowns into the operator.
        PhysicalPlan::Scan(scan) => bind_scan(scan, db, ctx, std::mem::take(pending_pushdowns)),

        // Pass-through: the operator preserves entity-id semantics.
        PhysicalPlan::Filter(filter) => {
            let child = bind_physical_with_cache(
                &filter.input, db, ctx, cohorts, pending_pushdowns,
            )?;
            Ok(Box::new(FilterOperator::new(
                child,
                filter.predicate.clone(),
                filter.tile_size,
            )))
        }

        PhysicalPlan::Project(project) => {
            let child = bind_physical_with_cache(
                &project.input, db, ctx, cohorts, pending_pushdowns,
            )?;
            Ok(Box::new(ProjectOperator::from_physical_items(
                child,
                project.expressions.clone(),
                project.output_schema.clone(),
            )))
        }

        PhysicalPlan::Limit(limit) => {
            let child = bind_physical_with_cache(
                &limit.input, db, ctx, cohorts, pending_pushdowns,
            )?;
            Ok(Box::new(LimitOperator::new(child, limit.count)))
        }

        // SubqueryFilter contributes its own pushdown, then propagates.
        PhysicalPlan::SubqueryFilter(sqf) => {
            bind_subquery_filter(sqf, db, ctx, cohorts, pending_pushdowns)
        }

        // All other variants break the passthrough invariant.
        // Drain `pending_pushdowns` before recursing so a wrong
        // pushdown can never reach a non-passthrough child.
        _ => {
            let _ = std::mem::take(pending_pushdowns);
            bind_physical_other(plan, db, ctx, cohorts)
        }
    }
}
```

`bind_physical_other` is the residue of the old `match`: every arm not listed above moves into it. To avoid duplicating arms, restructure:

```rust
fn bind_physical_other(
    plan: &PhysicalPlan,
    db: &mut Database,
    ctx: &QueryContext,
    cohorts: &mut CohortCache,
) -> Result<Box<dyn PhysicalOperator>> {
    // The pushdowns have been cleared by the caller; recursive
    // children of these nodes start with a fresh empty vec.
    let mut empty = Vec::new();
    match plan {
        PhysicalPlan::Scan(_)
        | PhysicalPlan::Filter(_)
        | PhysicalPlan::Project(_)
        | PhysicalPlan::Limit(_)
        | PhysicalPlan::SubqueryFilter(_) => unreachable!(
            "bind_physical_other called with a pass-through variant — \
             use bind_physical_with_cache instead"
        ),

        PhysicalPlan::Sort(sort) => {
            let child = bind_physical_with_cache(
                &sort.input, db, ctx, cohorts, &mut empty,
            )?;
            Ok(Box::new(SortOperator::with_spill(
                child,
                sort.keys.clone(),
                sort.max_rows,
                ctx.cancellation().clone(),
                ctx.memory().clone(),
                ctx.spill_fs().cloned(),
                ctx.spill_query_id(),
            )))
        }

        PhysicalPlan::Distinct(distinct) => { /* same pattern */ }
        PhysicalPlan::Aggregate(agg) => { /* same pattern */ }
        PhysicalPlan::SequenceMatch(seq) => { /* same pattern */ }
        // ... every other arm copy-paste from the old function, but
        // recursive calls use `&mut empty` so child subtrees start
        // pushdown-free.
    }
}
```

(Mechanical rewrite — preserve the exact existing semantics for every non-pushdown arm.)

- [ ] **Step 3: Update the public `bind_physical` entry that callers invoke.** Find the existing wrapper that creates a fresh `CohortCache`; it should now also create a fresh empty `Vec<ScanConjunct>`:

```rust
pub fn bind_physical(
    plan: &PhysicalPlan,
    db: &mut Database,
    ctx: &QueryContext,
) -> Result<Box<dyn PhysicalOperator>> {
    let mut cohorts = CohortCache::default();
    let mut pushdowns: Vec<ScanConjunct> = Vec::new();
    bind_physical_with_cache(plan, db, ctx, &mut cohorts, &mut pushdowns)
}
```

(Locate the existing `pub fn bind_physical` and edit in place — do not duplicate.)

- [ ] **Step 4: Update `bind_subquery_filter` to extract a pushdown after materialisation and add it to `pending_pushdowns`.**

Replace the current body (1019–1056) with:

```rust
fn bind_subquery_filter(
    sqf: &SubqueryFilterPhysical,
    db: &mut Database,
    ctx: &QueryContext,
    cohorts: &mut CohortCache,
    pending_pushdowns: &mut Vec<ScanConjunct>,
) -> Result<Box<dyn PhysicalOperator>> {
    let cohort = match cohorts.get(&sqf.subquery) {
        Some(existing) => existing,
        None => {
            // (existing materialisation code — unchanged)
            let mut op = bind_physical(&sqf.subquery, db, ctx)?;
            let drive_result = drive_cohort_subquery(op.as_mut());
            let close_result = op.close();
            let batches = drive_result?;
            close_result?;
            let cohort = Arc::new(CohortHashSet::from_batches(
                sqf.subquery.output_schema(),
                batches,
                ctx.memory().as_ref(),
            )?);
            cohorts.insert((*sqf.subquery).clone(), Arc::clone(&cohort));
            cohort
        }
    };

    // TASK-522: try to push the cohort's entity-id set into the outer
    // scan. The shape/size gate runs once per cohort/scan pair; if it
    // fires, the resulting conjunct travels with `pending_pushdowns`
    // until it reaches a Scan node.
    let entity_key_col = entity_key_col_name(&sqf.input);
    if let Some(conj) = crate::cohort_pushdown::try_extract_entity_pushdown(
        &sqf.lhs_columns,
        cohort.as_ref(),
        entity_key_col,
    ) {
        pending_pushdowns.push(conj);
    }

    let child = bind_physical_with_cache(
        &sqf.input, db, ctx, cohorts, pending_pushdowns,
    )?;
    Ok(Box::new(SubqueryFilterOperator::new(
        child,
        sqf.lhs_columns.clone(),
        cohort,
    )?))
}
```

(Note: nested materialisation still uses a fresh entry to `bind_physical`, not `bind_physical_with_cache`, because the inner subquery is its own pipeline.)

- [ ] **Step 5: Update `bind_scan` to consume the extra conjuncts.**

Change the signature:

```rust
fn bind_scan(
    scan: &ScanPhysical,
    db: &Database,
    ctx: &QueryContext,
    extra_conjuncts: Vec<ScanConjunct>,
) -> Result<Box<dyn PhysicalOperator>> {
    // ... (existing body unchanged through the with_tombstones call) ...

    let mut op = ScanOperator::with_tombstones(
        reader.clone(),
        &scan.projected_columns,
        scan_predicates,
        ctx.cancellation().clone(),
        tombstones,
    )?;

    if !extra_conjuncts.is_empty() {
        op.with_extra_conjuncts(extra_conjuncts);
    }

    if let Some(sample) = &scan.sample {
        // ... existing sample wiring ...
    }

    Ok(Box::new(op))
}
```

Update existing test fixtures that call `bind_scan` directly to pass `Vec::new()` as the new argument.

- [ ] **Step 6: Build incrementally.**

Run: `cargo build -p bqlite-engine`
Expected: clean build. Address compile errors arm-by-arm — each `bind_physical_with_cache` call site now needs the new `pending_pushdowns` arg.

- [ ] **Step 7: Run the engine's tests.**

Run: `cargo test -p bqlite-engine`
Expected: all existing tests pass. The bind threading is a refactor that preserves behaviour when no SubqueryFilter qualifies for pushdown.

- [ ] **Step 8: Add a bind-threading unit test in `bind.rs`'s test module.**

```rust
    #[test]
    fn pending_pushdowns_propagate_through_filter_to_scan() {
        // Construct a synthetic plan: Scan -> Filter -> SubqueryFilter.
        // Bind it with a known cohort and assert the resulting Scan
        // operator carries an EntityIn extra conjunct.
        // (Full setup reuses existing test helpers — copy from the
        // closest existing SubqueryFilter test in this module.)
        // ...
    }
```

(The test body's exact shape depends on the helpers already in `bind.rs` — copy from the closest existing cohort-binding test. The intent is: assert that after binding a SubqueryFilter-over-Filter-over-Scan plan, the Scan operator's `extra_conjuncts` is non-empty when the gate accepts.)

- [ ] **Step 9: Run all engine tests + the integration suite to catch regressions.**

Run: `scripts/local-ci.sh`
Expected: fmt, dep-direction, clippy, build, test all pass.

- [ ] **Step 10: Subagent code review.**

Dispatch `superpowers:code-reviewer` on the staged diff. Reviewer evaluates:
- Does every non-passthrough plan node clear the pending-pushdowns vec? (No silent leak of a pushdown into a non-entity-preserving child.)
- Is `bind_physical` (the public entry) and every external caller updated?
- Is the gate logic in `cohort_pushdown.rs` exhaustively tested for shape and size?
- Does `with_extra_conjuncts` interact correctly with `with_sample_filter` (i.e. both can be applied in any order)?

Address blockers; re-review.

- [ ] **Step 11: Commit and merge.**

```bash
git add crates/bqlite-engine/src/cohort_pushdown.rs \
        crates/bqlite-engine/src/bind.rs \
        crates/bqlite-engine/src/lib.rs \
        crates/bqlite-operators/src/cohort.rs \
        crates/bqlite-operators/src/scan.rs
git commit -m "TASK-522: Wire cohort entity-id pushdown into scan (CP2)

Adds the engine-side gate (shape + size, threshold 65,536 from
optimizer-direction.md §7 row 9) and threads a pending-pushdowns vec
through bind_physical_with_cache. SubqueryFilter contributes its own
ScanConjunct::EntityIn after materialisation; bind_scan drains the vec
into the operator via with_extra_conjuncts. Pass-through variants
(Filter / Project / Limit) propagate; everything else clears.

Correctness path unchanged — SubqueryFilterOperator still probes the
full cohort row by row. The pushdown only drives row-group skipping
via accepts_zone."
git push origin task/TASK-522
```

Then merge to main per the AGENTS.md fast-forward protocol.

---

## CP3: Integration tests + design-doc reconciliation

### Task 3.1: End-to-end correctness test

**Files:**
- Create: `tests/tests/wave5_cohort_pushdown.rs`
- Modify: `tests/Cargo.toml` (if test files are auto-discovered, no edit needed; verify with the existing wave4 test files).

- [ ] **Step 1: Look at one existing wave4 cohort integration test (e.g. `tests/tests/wave4_advanced_analytics_attribute_cohort_join.rs`) so the new file follows the same setup pattern (`bqlite_engine::Engine`, `bqlite_storage::Database`, fixtures from `bqlite_benches::common`).**

- [ ] **Step 2: Create the new file with three tests.**

> **Implementer note:** the body of each test depends on conventions in the existing wave4 cohort tests. Before writing, open `tests/tests/wave4_advanced_analytics_attribute_cohort_join.rs` and **copy the imports + setup pattern verbatim**. The `Engine::query` API returns `Result<Vec<RecordBatch>>` (or similar); collect rows by iterating batches and reading the `entity_id` column. Replace every `todo!()` and the `()` result types with concrete code from the closest wave4 test before running `cargo test`.

```rust
//! TASK-522: Cohort entity-id pushdown — correctness and gating.
//!
//! These tests assert two invariants from
//! `docs/design/language/cohorts-aliases-joins.md` §4.3:
//!
//! 1. **Correctness**: enabling pushdown produces the exact same row
//!    set as the post-scan probe path.
//! 2. **Gating**: the pushdown only fires for single-column cohorts
//!    on the entity-key column whose size is below the threshold.
//!
//! Skip-rate measurement is owned by TASK-526's bench suite.

// (Setup boilerplate copy-pasted from the closest wave4 test; it's
// always: build a Database, ingest events, run a query via Engine.)

use bqlite_engine::Engine;
// ... other imports as in the closest wave4 file ...

fn setup_engine_with_events() -> Engine {
    // 100K events across 1K entities, sorted by (entity_id, ts).
    // Use the same generator helper the wave4 tests use.
    todo!("copy from closest wave4 fixture builder")
}

#[test]
fn cohort_pushdown_matches_probe_only_path() {
    // Run the same query twice — once with a cohort tiny enough to
    // qualify for pushdown, once with a cohort big enough to fail
    // the size gate. Both must produce identical result sets.
    let mut engine = setup_engine_with_events();
    let small = engine
        .query(
            "buyers = events | WHERE event_type = 'purchase' | SELECT entity_id\n\
             events | WHERE entity_id IN buyers | STATS n = COUNT(*) GROUP BY entity_id",
        )
        .expect("small cohort query ok");

    // The "big" form just inverts the cohort to a different shape that
    // fails the size gate (multi-column LHS) but is logically the
    // same filter.
    let big = engine
        .query(
            "buyers = events | WHERE event_type = 'purchase' \
                  | SELECT entity_id, QUANTIZE(ts, 1d) AS day\n\
             events | WHERE (entity_id, QUANTIZE(ts, 1d)) IN buyers \
                    | STATS n = COUNT(*) GROUP BY entity_id",
        )
        .expect("multi-column cohort query ok");

    // Both queries scan the same outer table. The single-column
    // cohort takes the pushdown path; the multi-column cohort does
    // not. Result sets must agree on entity-id matches.
    let small_entities: std::collections::HashSet<_> = collect_entity_ids(&small);
    let big_entities: std::collections::HashSet<_> = collect_entity_ids(&big);
    assert_eq!(small_entities, big_entities);
}

#[test]
fn cohort_pushdown_skips_disjoint_segments() {
    // Build a database where the cohort's entity-id range is disjoint
    // from one segment's entity-id zone-map. After binding, decode
    // exactly one segment and assert the disjoint segment's row-groups
    // were pruned via the EntityIn zone-map rule.
    //
    // Implementation: ingest two segments — one with entity_ids 1..50,
    // one with entity_ids 100..150. Issue a cohort over entity_ids
    // 1..10. Run the query and assert via metrics (or a probe-row
    // count introspection helper) that fewer rows were decoded than
    // would be without pushdown.
    // ...
}

#[test]
fn cohort_pushdown_gate_rejects_large_cohort() {
    // Construct a cohort with > COHORT_PUSHDOWN_MAX_SIZE entries.
    // Run a query through it and assert the result is correct (i.e.
    // the post-scan probe still produces every matching row). This
    // is correctness coverage for the size-gate fallback.
    // ...
}

fn collect_entity_ids(_result: &/* result type from Engine */ ()) -> std::collections::HashSet<i64> {
    // Pull `entity_id` out of every result row. Use the same approach
    // the wave4 tests use; depending on the API this might be via
    // `Engine::query` returning batches that the test iterates.
    todo!("match the existing test pattern")
}
```

(Replace the `todo!()`s and `()` placeholders by reading the closest existing wave4 cohort test — the test infrastructure is mature; we only need to match conventions.)

- [ ] **Step 3: Run the new integration tests.**

Run: `cargo test --test wave5_cohort_pushdown`
Expected: all three tests pass.

- [ ] **Step 4: Run the full test suite.**

Run: `scripts/local-ci.sh`
Expected: green.

### Task 3.2: Design-doc reconciliation

**Files:**
- Modify: `docs/design/language/cohorts-aliases-joins.md`
- Modify: `docs/design/storage/predicate-pushdown.md`

- [ ] **Step 1: Update `cohorts-aliases-joins.md §4.3` with a paragraph noting that the pushdown is now implemented as `ScanConjunct::EntityIn` and is size-gated per `optimizer-direction.md §7 row 9`. Keep the existing prose; append a "Implementation notes (TASK-522)" subsection.**

```markdown
**Implementation notes (TASK-522).** The pushdown is implemented as
`bqlite_core::storage::ScanConjunct::EntityIn { column, values,
set_min, set_max }`. The engine extracts the entity-id set from a
materialised cohort after the inner subquery completes, computes the
set's min/max once for O(1) row-group zone-map acceptance, and
threads the conjunct through pass-through plan nodes (Filter /
Project / Limit / nested SubqueryFilter) until it reaches the inner
`Scan`. Per `docs/design/planner/optimizer-direction.md` §7 row 9
the pushdown is size-gated: cohorts with `size >=
COHORT_PUSHDOWN_MAX_SIZE` (65,536 in v1) are not pushed and rely on
the post-scan probe alone. Multi-column cohorts and cohorts whose
LHS is not a direct entity-key column reference are likewise not
pushed in v1 — `cohorts-aliases-joins.md` §4.3 explicitly defers
those.
```

- [ ] **Step 2: In §6.3.1, append a "Resolved by TASK-522" line at the top of the section so future readers see the deferral has been resolved.**

```markdown
> **Status:** Resolved by TASK-522 (2026-04-30). The
> `ScanPredicate` taxonomy now includes `ScanConjunct::EntityIn`,
> the engine bind step extracts entity-id sets from materialised
> cohorts and threads them through pass-through plan nodes into the
> outer `Scan`, and the pushdown is size-gated per
> `optimizer-direction.md §7 row 9`. The historical text below is
> preserved for context; no further work is owed against the
> deferral.
```

- [ ] **Step 3: Update `predicate-pushdown.md §4` (pushable conjunct taxonomy) with a one-row addition to the table:**

| Shape | Example | Notes |
|---|---|---|
| `entity_id ∈ <materialised cohort>` (engine-injected) | (TASK-522) | Set-membership against a deduplicated `Arc<HashSet<PropertyValue>>`; precomputed `[set_min, set_max]` drives O(1) zone-map acceptance. Not lowered from `CompiledExpr` — the engine constructs it post-cohort-materialisation. |

- [ ] **Step 4: In `predicate-pushdown.md §11`, update bullet 3 (Bloom filters) to note that set-membership now exists; revise the wording so set-membership is no longer a pure future hook.**

- [ ] **Step 5: Re-run `scripts/local-ci.sh` (no code changes; this catches markdown-link or formatting drift if any tests cover doc validity).**

Run: `scripts/local-ci.sh`
Expected: green.

- [ ] **Step 6: Subagent code review on the cumulative diff.**

Dispatch `superpowers:code-reviewer`. Focus areas:
- Are the design-doc updates accurate w.r.t. the implementation?
- Are the integration tests sufficient for correctness coverage given that benchmark-style skip-rate measurement is deferred to TASK-526?
- Any TASKS.md-listed dependents (TASK-526) that need a status note?

- [ ] **Step 7: Commit and merge.**

```bash
git add tests/tests/wave5_cohort_pushdown.rs \
        docs/design/language/cohorts-aliases-joins.md \
        docs/design/storage/predicate-pushdown.md
git commit -m "TASK-522: Integration tests + spec reconciliation (CP3)

Adds wave5 cohort-pushdown integration tests (correctness equivalence,
disjoint-segment skipping, large-cohort fallback) and reconciles the
two design docs that previously deferred the work:

- cohorts-aliases-joins.md §4.3 / §6.3.1: deferral marked resolved
- storage/predicate-pushdown.md §4 / §11: EntityIn entry added

Skip-rate / throughput measurement is owned by TASK-526's bench gate."
git push origin task/TASK-522
```

Merge to main per the AGENTS.md protocol.

---

## CP4: Completion

- [ ] **Step 1:** `git mv tasks/active/TASK-522.lock tasks/completed/TASK-522.done`
- [ ] **Step 2:** Edit the `.done` file to add `completed_at` (current UTC ISO-8601 timestamp). Keep all other fields unchanged.
- [ ] **Step 3:** Commit and push:

```bash
git add tasks/active/TASK-522.lock tasks/completed/TASK-522.done
git commit -m "TASK-522: completed"
git push origin main
```

End the turn — do not claim another task.

---

## Risk register

| Risk | Mitigation |
|------|------------|
| Pushdown leaks into a non-passthrough operator and produces a wrong result | Default-clear in the `_` arm of `bind_physical_with_cache`; only an explicit allowlist (Filter/Project/Limit/SubqueryFilter) propagates. |
| `ScanConjunct::EntityIn` constructor on a 65K-entity cohort is hot enough to regress query startup | Constructor walks the set once to compute bounds — O(N) at construction, but bounded by the gate (`< 65_536`). Outer scans without a qualifying cohort pay zero cost (gate fails). |
| Cohort with mixed `ScalarValue` types (e.g. cohort produces `Int` but entity column is `String`) | Catalog-level type checking happens before TASK-522's gate (`apply_subquery_filter` validates positional type compatibility per cohorts-aliases-joins.md §2.10). Mismatch is a planner error, not a runtime panic. The conversion in `scalar_to_property_value` is total for the entity-key types we accept. |
| The TASK-521 framework (rule registry) lands and replaces `bind_subquery_filter`'s direct gate call | The gate is a single function (`try_extract_entity_pushdown`) — moving its invocation from `bind_subquery_filter` into a `PostCohort` rule body is a mechanical refactor that doesn't change semantics. The threshold constant lives in one place (`cohort_pushdown::COHORT_PUSHDOWN_MAX_SIZE`). |
| Existing engine tests that construct `bind_physical_with_cache` directly need updating to pass the new `pending_pushdowns` parameter | All call sites are inside the engine crate; the compiler will list them. The test fixtures should pass `&mut Vec::new()` and accept the existing behaviour. |
| Multi-segment skip-rate test is fragile (depends on exact zone-map composition) | Use `assert_at_least_one_row_group_pruned` rather than asserting an exact count. The bench gate (TASK-526) owns precise skip-rate measurement. |
| `MergeSources`-rooted joined-source queries (`events JOIN purchases | WHERE entity_id IN cohort`) hit the `_ =>` arm and lose the pushdown silently | Acceptable v1 behaviour: correctness is preserved by the post-scan `SubqueryFilterOperator`. The `Decisions to highlight` section's mention of "Arc clone per scan" is forward-looking — it documents that the conjunct shape *would support* multi-scan use without a deep clone, not that v1 propagates into `MergeSources`. CP2 Task 2.3 Step 2's `_ =>` arm comment must explicitly call out `MergeSources` as a deliberate v1 drop, not an oversight. |
| `lower_compiled_predicates` becomes dead code if all callers move to lazy assembly inside `open` | Keep `build_scan_predicate` for the existing test module call sites (line 2810+) so the helper is still referenced; the lazy path inside `open` re-uses `lower_compiled_predicates` directly. |
| Clippy `let_unit_value` or "useless type annotation" on `let scan_predicate = None::<Arc<dyn Predicate>>;` | Use `let scan_predicate: Option<Arc<dyn Predicate>> = None;` with the explicit type annotation, matching the field's declared type. |
| Float entity-key column hashes inconsistently in `HashSet<PropertyValue>` due to `NaN` semantics | Add `if matches!(scalar, ScalarValue::Float(_)) { return None; }` early-out in `scalar_to_property_value`'s caller, or document explicitly that entity-key columns are by convention `Int` / `String` / `Timestamp` (not `Float`). |

---

## Decisions to highlight in code review

- **Conjunct shape**: `Arc<HashSet<PropertyValue>>` is the in-memory shape rather than `Vec<PropertyValue>` (matching `InSet`) because cohorts can be tens of thousands of entries and may back several scans (joined source). One materialised cohort, one `Arc` clone per scan.
- **Engine vs planner placement**: Pass 8 lives in the engine per `optimizer-direction.md §12`. The gate is implemented as a direct function call in `bind_subquery_filter` rather than a registered rule because TASK-521's rule registry hasn't landed; when it does, this becomes a mechanical refactor (single function relocation).
- **Bind threading via mutable vec**: a `&mut Vec<ScanConjunct>` is more ergonomic than passing a `Vec` by value through every recursive arm. The drained-on-leaf semantics ensures no leak.
- **Why no row-level dictionary rewrite for `EntityIn`**: the post-scan `SubqueryFilterOperator` already does the row-level probe. The pushdown is purely a zone-map skip mechanism; doing a redundant row-level filter inside the storage layer would duplicate work the operator above already performs.

---

## Outside this PR

- **TASK-526 (Wave 5 bench suite)**: skip-rate / throughput measurement for cohort pushdown.
- **Future**: TASK-521 CP2 + Pass 8 registration. The gate and constant in `cohort_pushdown.rs` move into a registered `PostCohort` rule with no semantic change; `OptimizerPipeline::run_post_cohort` becomes the single dispatch point.
- **Future (cohorts-aliases-joins.md §4.3)**: full multi-column tuple-bloom or multi-key predicate pushdown for `(entity_id, day) IN ...` style cohorts.
