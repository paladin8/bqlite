# TASK-409 — `DemandCapabilities` protocol

Human-assisted semantics decisions for `docs/design/planner/demand-protocol.md`. These decisions are authoritative and override conflicting sketches in `docs/design/sequence-matching.md §13.5`, `docs/design/operators/operator-traits.md §7`, `docs/design/planner-pipeline.md §9.2–§9.3 / §15`, and the Wave 1 scaffold in `crates/bqlite-core/src/demand.rs`. Reconcile those docs in the same checkpoint as TASK-427 (the implementation task that lands the real protocol).

## Already pinned by existing docs (not re-litigated here)

- `DemandSet` (planner side) and `DemandCapabilities` (operator side) are **dual** types: planner builds a `DemandSet` via a backward pass from the root; physical planner matches it against each node's `DemandCapabilities`. `sequence-matching.md §13.5`, `planner-pipeline.md §9.3`.
- `DemandSet` shape is already fixed by `planner-pipeline.md §9.3` / `crates/bqlite-planner/src/demand.rs`: `columns`, `needs_match_detail`, `needs_step_reached`, `step_properties: Vec<StepPropertyRef>`, `forwarded: Vec<ColumnId>`, `fused_aggregate`, `fused_filter`. No changes.
- Backward propagation protocol: start at root with full demand, each node strips columns it produces, adds columns it reads, passes upstream. `planner-pipeline.md §9.2`.
- The scaffold in `crates/bqlite-core/src/demand.rs` is explicitly a placeholder per its own doc comment — this note's job is to freeze the real shape and retire the scaffold.

## Decisions

### 1. Plain struct with `bool` fields — **not** enum, bitflags, or level-enum

```rust
pub struct DemandCapabilities {
    pub supports_step_reached: bool,
    pub supports_match_count: bool,
    pub supports_full_detail: bool,
    pub supports_aggregation_fusion: bool,
    pub supports_step_property_forwarding: bool,
    pub supports_forwarded_columns: bool,
    pub supports_eager_group_emit: bool,
}
```

**Why:** struct-of-bools reads trivially at call sites (`caps.supports_full_detail`), composes with `DemandSet` matching without an adapter layer, and lets Rust warn on exhaustive matches we never wanted in the first place. `bitflags!` buys nothing at seven bits; a level-enum (e.g. `DetailLevel::{None, Count, Full}`) encodes mutually-exclusive invariants the planner will sometimes want to violate (a Wave 5 fusion path can legitimately need `Count` + `Full` simultaneously for a fused-plus-non-fused sibling).

The field list is pinned in §3 below.

### 2. Crate home — `bqlite-planner::demand`, alongside `DemandSet`

Relocate from `bqlite-core::demand` to `bqlite-planner::demand`. The two dual types live in the same module.

**Why:** both types are planner-internal vocabulary per `planner-pipeline.md §15`. The Wave 1 placement in `bqlite-core` was a scaffold convenience (`bqlite-operators` could import it before `bqlite-planner` existed). Now that planner is stable and the dep graph allows `bqlite-operators → bqlite-planner`, the types belong together.

**Dep graph impact.** `bqlite-operators` already depends on `bqlite-planner` (CLAUDE.md "Dependency Direction"). No new edges.

### 3. Capability bit list — seven bits covering v1 + Wave 5 reserved

| Bit | Meaning | Demand-side source | v1 consumer |
|---|---|---|---|
| `supports_step_reached` | Emits the `step_reached` synthetic column on `emit_all` MATCH paths. | `DemandSet.needs_step_reached` | SequenceMatch |
| `supports_match_count` | Count-only strategy (step-counter / dedicated-consecutive, no path tracking). | `DemandSet.needs_match_detail == false` in combination with non-detail demands | SequenceMatch |
| `supports_full_detail` | FullNFA strategy — emits `match_events` + `match_duration`. | `DemandSet.needs_match_detail` | SequenceMatch |
| `supports_aggregation_fusion` | `finish_entity_into` hot path; operator absorbs downstream `FusableAggregate`. | `DemandSet.fused_aggregate.is_some()` | SequenceMatch (Wave 3 fusion); reserved for Wave 5 for SESSIONIZE / EventSelect / ATTRIBUTE |
| `supports_step_property_forwarding` | Per-(step, column) retention for `s.plan`-style references. | `DemandSet.step_properties` non-empty | SequenceMatch |
| `supports_forwarded_columns` | Generic per-column forwarding from stateful ops (SESSIONIZE `session_id`/`session_duration` passthrough of upstream columns, ATTRIBUTE conversion-property forwarding, EventSelect column forwarding). | `DemandSet.forwarded` non-empty | Sessionize, Attribute, EventSelect (Wave 4) |
| `supports_eager_group_emit` | Reserved for Wave 5. Advertises that the operator can emit per-group output mid-stream when the group key closes (e.g. `SESSIONIZE | STATS … GROUP BY session_id` per `planner-pipeline.md §8.3`). No v1 setter; bit exists so Wave 5 doesn't need a coordinated capability surface rebase. | No v1 matching — reserved. | None (Wave 5) |

**Why the two forwarding bits are separate (§B2=(a)).** `DemandSet.step_properties` (per-(step, column), MATCH-specific) and `DemandSet.forwarded` (flat per-column, everyone else) come from different planner passes and target different retention paths inside the operator. Collapsing to one bit would require an operator impl to explain "I only support one of these" by overriding the trait method with conditional logic on the `DemandSet` — cleaner to have two bits and let the planner match each independently.

**Why `supports_aggregation_fusion` stays in v1 (§B3=(a)).** SequenceMatch already uses it on the Wave 3 match-aggregate fusion path (`opt/fuse_match_aggregate.rs`). The bit is narrowly used in v1 — SESSIONIZE / EventSelect / ATTRIBUTE all return `false` per their Tier 1 notes (TASK-405 §10, TASK-406 §10, TASK-411 A7) — but it is a real capability with a real consumer, not dead code.

**Why `supports_eager_group_emit` is reserved (§B4=(b)).** Wave 5 fusion for SESSIONIZE per `planner-pipeline.md §8.3` needs this bit. Carrying it from the start — even with no v1 setter and no v1 matcher — means Wave 5 lands as an additive field update rather than a cross-crate capability-list expansion. Cost is near-zero; benefit is a cleaner Wave 5 merge.

### 4. `PhysicalOperator` vs `EntityOperator` — two orthogonal demand channels

`DemandCapabilities` covers only **flexible-output-shape** operators. Stateless operators (Scan, Filter, Project, Sort, Limit) don't need capability bits — their demand interaction is either trivial pass-through or a flat column list.

**Channel 1: `DemandCapabilities` (this task's protocol).** Advertised by stateful operators with variable output shapes — `SequenceMatch`, `Sessionize`, `EventSelect`, `Attribute`. Matched by the physical planner against the `DemandSet` at each plan node.

**Channel 2: `ScanPhysical.projected_columns: Vec<ColumnId>` (pre-existing).** Populated by the backward pass from the accumulated `DemandSet.columns` at the scan boundary. Scan has no capability bits to advertise — every scan supports every flat column projection.

Filter / Sort / Limit are neither: they modify the `DemandSet` (filter adds predicate columns, others pass through) but produce the same schema as their input. No capability bits needed; no physical-descriptor field needed.

**Trait implementation rule:**

- `DemandPropagation` is object-safe and *implementable* by any operator — an `impl DemandPropagation for MyStatelessOp {}` (empty body, default all-false) is legal and costs nothing.
- Only operators that override at least one capability bit to `true` are meaningfully matched by the planner. Everyone else can skip the impl; the planner's matcher treats a missing impl as all-false via the trait's default method.
- Scan's concrete demand handling is its `projected_columns` field, not a `DemandCapabilities` bit. This is documented in `demand-protocol.md` so later work doesn't try to retrofit scan into the capability protocol.

**Why:** the `DemandCapabilities` protocol is inherently a capability negotiation — it only has meaning where the operator has *choices* about output shape. Scans, filters, sorts, and limits don't; their demand is a direct read-through of the `DemandSet` fields. Conflating the two channels would force every operator to implement a trait whose bits they'd all set to `false`, adding friction without providing signal.

### 5. `#[non_exhaustive]` on the struct — **not** applied

The struct is **not** `#[non_exhaustive]`. External operator impls use struct-literal syntax:

```rust
impl DemandPropagation for SequenceMatchOperator {
    fn supported_demands(&self) -> DemandCapabilities {
        DemandCapabilities {
            supports_step_reached: true,
            supports_match_count: true,
            supports_full_detail: true,
            supports_aggregation_fusion: true,
            supports_step_property_forwarding: true,
            supports_forwarded_columns: false,
            supports_eager_group_emit: false,
        }
    }
}
```

**Why:** adding a capability bit is intentionally a breaking change to every operator impl — that's the point. Every existing operator must re-answer "do I support this new capability?" explicitly. Silently defaulting new bits to `false` via `#[non_exhaustive]` + `..Default::default()` would let new capabilities ship with operators that should support them but quietly don't, and we'd never notice at compile time. All operator impls live in this workspace (no external users of the capability struct); the blast radius of a capability addition is bounded by `cargo build`.

**Helper constructors provided:**

- `DemandCapabilities::default()` — all `false`. Use for stateless operators that implement `DemandPropagation` purely for uniformity.
- `DemandCapabilities::none()` — `const fn`, all `false`, identical to `default()` but usable in `const` contexts.

No `all()` constructor — "all true" is not a meaningful operator answer; it would mix MATCH-specific bits (`supports_step_property_forwarding`) with everyone-else bits (`supports_forwarded_columns`) that no single operator ever implements together.

### 6. Dual-surface trait: object-safe `DemandPropagation` + mirrored method on `EntityOperator`

Keep both surfaces (Wave 1 shape, promoted to real types):

```rust
// bqlite-planner::demand
pub trait DemandPropagation {
    fn supported_demands(&self) -> DemandCapabilities {
        DemandCapabilities::default()
    }
}
```

```rust
// bqlite-operators::operator (additive on top of existing EntityOperator trait)
pub trait EntityOperator: /* ... existing bounds ... */ {
    // ... existing methods ...

    fn supported_demands(&self) -> DemandCapabilities {
        DemandCapabilities::default()
    }
}
```

**Why:**

- Planner code reasoning about demand at plan time can take `&dyn DemandPropagation` without reaching into `bqlite-operators` (trait-object dispatch, no `EntityOperator` bound).
- `EntityOperator` impls get the method "for free" via the mirrored default — no second `impl` block needed.
- Operator kinds that aren't `EntityOperator` (e.g. a future push-mode streaming sink) still have a uniform advertisement channel.

Implementors override the `EntityOperator::supported_demands` method directly when they have capabilities. A separate `impl DemandPropagation for MyOp {}` block is only needed when the operator isn't an `EntityOperator`.

Wave 1's comment about "a later wave will unify the two surfaces" is retired — this note confirms both surfaces survive.

### 7. Capability matching happens in physical planning — **not** engine bind

The physical planner matches `DemandSet` against `DemandCapabilities` during strategy selection (`planner-pipeline.md §9.4`). Engine bind (TASK-438) does not re-check.

**Resolution for the dep-graph constraint (planner can't depend on operators):** each physical operator descriptor struct carries a `const DEMAND_CAPS: DemandCapabilities` declared alongside its data:

```rust
// bqlite-planner::physical
pub struct SequenceMatchPhysical {
    // ... existing fields ...
}

impl SequenceMatchPhysical {
    pub const DEMAND_CAPS: DemandCapabilities = DemandCapabilities {
        supports_step_reached: true,
        supports_match_count: true,
        supports_full_detail: true,
        supports_aggregation_fusion: true,
        supports_step_property_forwarding: true,
        supports_forwarded_columns: false,
        supports_eager_group_emit: false,
    };
}
```

The planner reads the `const` without depending on the operator implementation crate. The operator crate (`bqlite-operators`) is required to return the same `DemandCapabilities` from its trait impl — this is a contract enforced by a test in TASK-427 that asserts, for each operator kind, that `<Op>::supported_demands()` equals `<OpPhysical>::DEMAND_CAPS`.

**Why planner-only matching:** demand matching is a plan-time decision that influences strategy selection, lowering, and EXPLAIN output. Moving it to engine bind would mean plans could exist that cannot execute — an error class we'd rather rule out structurally. Defense-in-depth re-checking at bind time is not worth the duplicated surface.

### 8. Unmet-demand policy — plan-time error

If the physical planner computes a `DemandSet` that the matched node's `DEMAND_CAPS` cannot satisfy (e.g. `DemandSet.needs_match_detail = true` but `supports_full_detail = false`), the planner emits:

```rust
TypeError::UnsupportedDemand {
    node: PlanNodeKind,
    missing: Vec<&'static str>,  // names of the unsupported demand fields
}
```

**Why:** a plan that can't execute is a planner bug, not a user error. Graceful downgrade (§C1=(b)) would hide planner bugs behind silently-suboptimal queries. Operator-side fallback with NULL padding (§C1=(c)) would break the non-nullability guarantees on `match_events` / `match_duration` / forwarded columns documented in `type-system.md`.

In v1 the error is unreachable by construction — every v1 operator either sets every relevant bit to `true` or isn't ever demanded that shape (EventSelect never gets `needs_match_detail`, etc.). The error exists as a guardrail against future refactors where a capability is dropped from an operator without the matching planner update.

### 9. Scaffold retirement — `bqlite-core::demand` deleted in the same commit

TASK-427 is merge-first. It lands as one atomic commit that:

1. Adds `bqlite-planner::demand::{DemandCapabilities, DemandPropagation}` with the real shape (this note §1–§6).
2. Deletes `bqlite-core::demand` entirely. No re-export, no deprecation alias. Pre-1.0; no external consumers.
3. Updates every Wave 1 operator scaffold call site:
   - `DemandCapabilities::None` → `DemandCapabilities::none()` (const constructor from §5).
   - `impl DemandPropagation for Foo {}` (empty body) stays legal — default method body returns `DemandCapabilities::none()`, semantics unchanged.
4. Updates `bqlite-core`'s `lib.rs` re-export list to remove the `demand` module.
5. Adds the `const DEMAND_CAPS` to each physical operator descriptor per §7.
6. Adds the per-kind `<Op>::supported_demands() == <OpPhysical>::DEMAND_CAPS` contract test per §7.

**Why all-at-once:** this is a `[TRAIT]` task, and `[TRAIT]` tasks are merge-first by TASKS.md §2 rule. Staging the move (re-export alias, later cleanup) would leave a window where both homes exist and the doc can't be authoritative about which one to import.

## Follow-on implications to propagate

- **TASK-427 (relocation + wiring)** — lands the atomic commit described in §9. Adds the `const DEMAND_CAPS` to `SequenceMatchPhysical`, `SessionizePhysical`, `EventSelectPhysical`, `AttributePhysical`. Wires the planner's strategy-selection pass to check capabilities per §7. Implements `TypeError::UnsupportedDemand` per §8.
- **TASK-428 (SessionizeOperator)** — returns `DemandCapabilities { supports_forwarded_columns: true, ..default() }`. All other bits false. Matches TASK-405 §10 (no v1 fusion).
- **TASK-429 (EventSelectOperator)** — returns `DemandCapabilities { supports_forwarded_columns: true, ..default() }`. All other bits false. Matches TASK-411 A6 / A7.
- **TASK-431 (AttributeOperator)** — returns `DemandCapabilities { supports_forwarded_columns: true, ..default() }`. All other bits false. Matches TASK-406 §9 / §10.
- **Existing `SequenceMatchOperator`** — updates from the Wave 1 default impl to the full capability set per §3 table. This is the one existing consumer that had a non-trivial capability story; the bit set captures what its current physical descriptor already declares implicitly.
- **`docs/design/sequence-matching.md §13.5`** — reconcile the sketch with the final field list from §3 (adds `supports_forwarded_columns` and `supports_eager_group_emit`; confirms the existing five bits verbatim).
- **`docs/design/operators/operator-traits.md §7`** — retire the "Deferred to TASK-110" note for `supported_demands`; replace with a forward reference to `demand-protocol.md`.
- **`docs/design/planner-pipeline.md §9.3`** — add a cross-reference paragraph pointing to `demand-protocol.md` for the operator-side dual (the existing "Relationship to `DemandCapabilities`" paragraph already exists; update it to confirm both types now live in `bqlite-planner::demand`).
- **`docs/design/planner-pipeline.md §15`** — crate placement table gets a new `DemandCapabilities` row pointing to `bqlite-planner`. Remove any implication that it lives in `bqlite-core`.
- **`docs/design/execution-model.md §13.2`** — sentence about `DemandCapabilities` crate home is already correct (it says `bqlite-planner`); confirm unchanged.
- **`crates/bqlite-core/src/lib.rs`** — `pub mod demand;` and any `pub use demand::*;` re-exports are removed by TASK-427.
- **Wave 5 fusion work (TASK-503 ecosystem)** — `supports_aggregation_fusion` becomes the main capability bit for fused SESSIONIZE / EventSelect / ATTRIBUTE. `supports_eager_group_emit` becomes the bit for the `GROUP BY session_id` eager-emit path from `planner-pipeline.md §8.3`. Both are already reserved in v1; Wave 5 flips the operator-side setters to `true` and adds the planner-side matchers.
