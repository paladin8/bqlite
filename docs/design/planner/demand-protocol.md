# DemandCapabilities Protocol

**Wave**: 4
**Task**: TASK-409
**Status**: draft
**Depends on**: TASK-301 (match-operator.md), TASK-302 (matcher-strategy.md), TASK-309 (wave3-lowering.md)
**Depended on by**: TASK-427 (relocation + planner/operator wiring)

---

## 1. Purpose

This document is the authoritative specification for the operator-side
`DemandCapabilities` protocol — the dual of the planner-side `DemandSet`
(planner-pipeline.md §9.3, wave3-lowering.md §3.1). It replaces the Wave 1
scaffold in `bqlite-core::demand` with the real capability shape, defines
crate placement, forwarding/fusion capability bits, the matching protocol
used during physical planning, and the migration path away from the
placeholder enum.

**What this document covers:**

- `DemandCapabilities` struct shape and field semantics (§2)
- Capability bit semantics (§3)
- Crate placement and dependency graph impact (§4)
- `DemandPropagation` trait surface and dual-surface design (§5)
- Capability matching during physical planning (§6)
- Unmet-demand error policy (§7)
- Matching algorithm (§8)
- Channel separation: capability bits vs. scan projection (§9)
- `#[non_exhaustive]` rationale (§10)
- Scaffold retirement protocol (§11)
- Follow-on implications for downstream tasks (§12)
- Resolved design questions (§13)

**What this document does NOT cover:**

- `DemandSet` shape — already frozen by planner-pipeline.md §9.3 and
  wave3-lowering.md §3.1. No changes.
- Backward propagation algorithm — wave3-lowering.md §3.2.
- Pattern compilation — TASK-311.
- Operator runtime behavior — match-operator.md, aggregate-operator.md,
  sessionize.md, attribute.md.

---

## 2. DemandCapabilities Struct

`DemandCapabilities` is a plain struct with boolean fields. Each field
advertises a single, orthogonal capability that a stateful operator may
or may not support.

```rust
/// Operator-side capability advertisement for demand-driven output shaping.
///
/// The planner constructs a `DemandSet` during its backward pass (root → scan),
/// then matches it against each operator's `DemandCapabilities` during physical
/// planning to select a strategy and validate that every demand can be satisfied.
///
/// This is the dual of `DemandSet`: `DemandSet` says "what the downstream needs";
/// `DemandCapabilities` says "what this operator can provide."
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

### 2.1 Why a Plain Struct

A struct of bools was chosen over three alternatives:

- **Enum (the Wave 1 scaffold shape):** An enum forces mutually exclusive
  variants. Demand capabilities are not mutually exclusive — an operator can
  support both `supports_match_count` and `supports_full_detail`
  simultaneously (SequenceMatch does). The Wave 1 `DemandCapabilities::None`
  enum was explicitly a placeholder; this document retires it.

- **`bitflags!` macro:** At seven bits, the bitflags macro provides no
  ergonomic advantage over named bools. Named fields read better at call
  sites (`caps.supports_full_detail` vs `caps.contains(FULL_DETAIL)`) and
  compose with `DemandSet` matching without an adapter layer.

- **Level enum (e.g. `DetailLevel::{None, Count, Full}`):** A level enum
  encodes mutually-exclusive invariants that the planner will sometimes
  want to violate. A Wave 5 fusion path can legitimately need `Count` +
  `Full` simultaneously for a fused-plus-non-fused sibling. Named bools
  impose no such constraint.

### 2.2 Constructors

```rust
impl DemandCapabilities {
    /// All capabilities disabled. Use for stateless operators that implement
    /// `DemandPropagation` purely for uniformity.
    pub const fn none() -> Self {
        DemandCapabilities {
            supports_step_reached: false,
            supports_match_count: false,
            supports_full_detail: false,
            supports_aggregation_fusion: false,
            supports_step_property_forwarding: false,
            supports_forwarded_columns: false,
            supports_eager_group_emit: false,
        }
    }
}

impl Default for DemandCapabilities {
    fn default() -> Self {
        Self::none()
    }
}
```

`none()` is `const fn` for use in `const DEMAND_CAPS` declarations on
physical operator descriptors (§6). `default()` delegates to `none()`.

No `all()` constructor is provided. "All true" is not a meaningful operator
answer — it would mix MATCH-specific bits (`supports_step_property_forwarding`)
with non-MATCH bits (`supports_forwarded_columns`) that no single operator
ever sets together.

---

## 3. Capability Bit Semantics

Seven bits covering v1 operators plus one reserved for Wave 5:

| Bit | Meaning | `DemandSet` source | v1 consumer |
|---|---|---|---|
| `supports_step_reached` | Emits the `step_reached` synthetic column on `emit_all` MATCH paths. | `DemandSet.needs_step_reached` | SequenceMatch |
| `supports_match_count` | Count-only strategy (step-counter / dedicated-consecutive, no path tracking). | `DemandSet.needs_match_detail == false` in combination with non-detail demands | SequenceMatch |
| `supports_full_detail` | FullNFA strategy — emits `match_events` + `match_duration`. | `DemandSet.needs_match_detail` | SequenceMatch |
| `supports_aggregation_fusion` | `finish_entity_into` hot path; operator absorbs downstream `FusableAggregate`. | `DemandSet.fused_aggregate.is_some()` | SequenceMatch (Wave 3 fusion); reserved for Wave 5 for Sessionize / EventSelect / Attribute |
| `supports_step_property_forwarding` | Per-(step, column) retention for `s.plan`-style references. | `DemandSet.step_properties` non-empty | SequenceMatch |
| `supports_forwarded_columns` | Generic per-column forwarding from stateful ops (Sessionize `session_id`/`session_duration` passthrough, Attribute conversion-property forwarding, EventSelect column forwarding). | `DemandSet.forwarded` non-empty | Sessionize, Attribute, EventSelect (Wave 4) |
| `supports_eager_group_emit` | **Reserved for Wave 5.** Advertises that the operator can emit per-group output mid-stream when the group key closes (e.g. `SESSIONIZE \| STATS ... GROUP BY session_id` per planner-pipeline.md §8.3). No v1 setter; bit exists so Wave 5 lands as an additive field update. | No v1 matching — reserved. | None (Wave 5) |

### 3.1 Why Two Separate Forwarding Bits

`supports_step_property_forwarding` and `supports_forwarded_columns` target
different demand shapes and different retention paths:

- `DemandSet.step_properties` is per-(step, column) and MATCH-specific.
  It drives the SequenceMatch operator's per-step property retention in
  the NFA state or step-counter bookkeeping.

- `DemandSet.forwarded` is a flat per-column list used by Sessionize,
  EventSelect, and Attribute. It drives generic column pass-through where
  the operator copies upstream columns onto its output rows.

Collapsing to one bit would require operator impls to inspect the `DemandSet`
to distinguish "step-property forwarding" from "generic column forwarding" —
cleaner to have two bits and let the planner match each independently.

### 3.2 Why `supports_aggregation_fusion` Stays in v1

SequenceMatch already uses this bit on the Wave 3 match-aggregate fusion
path (`opt/fuse_match_aggregate.rs`). The bit is narrowly used in v1 —
Sessionize, EventSelect, and Attribute all return `false` per their
respective Tier 1 notes — but it is a real capability with a real consumer,
not dead code.

### 3.3 Why `supports_eager_group_emit` Is Reserved

Wave 5 fusion for Sessionize per planner-pipeline.md §8.3 needs this bit.
Carrying it from the start — even with no v1 setter and no v1 matcher —
means Wave 5 lands as an additive field update rather than a cross-crate
capability-list expansion. Cost is near-zero (one `bool` field); benefit
is a cleaner Wave 5 merge.

---

## 4. Crate Placement

`DemandCapabilities` and `DemandPropagation` relocate from
`bqlite-core::demand` to **`bqlite-planner::demand`**, alongside `DemandSet`.

### 4.1 Rationale

Both `DemandCapabilities` (operator-side) and `DemandSet` (planner-side)
are planner-internal vocabulary per planner-pipeline.md §15. The Wave 1
placement in `bqlite-core` was a scaffold convenience: `bqlite-operators`
could import `DemandCapabilities` before `bqlite-planner` existed in the
dependency graph. Now that the planner crate is stable and
`bqlite-operators → bqlite-planner` is an established dependency edge
(CLAUDE.md "Dependency Direction"), the types belong together.

### 4.2 Dependency Graph Impact

`bqlite-operators` already depends on `bqlite-planner`. No new dependency
edges are introduced by this relocation.

Updated crate placement (extending planner-pipeline.md §15):

| Module | Crate | Purpose |
|---|---|---|
| `DemandSet` | `bqlite-planner` | Downstream-needs value carried through backward pass |
| `DemandCapabilities` | `bqlite-planner` | Operator-side capability advertisement |
| `DemandPropagation` | `bqlite-planner` | Object-safe trait for capability queries |
| `StepPropertyRef` | `bqlite-planner` | Per-(step, column) demand tracking |
| `FusableAggregate` | `bqlite-planner` | Aggregate fusion specification |

All demand types live in the `bqlite_planner::demand` module.

---

## 5. DemandPropagation Trait

```rust
/// Object-safe trait for querying an operator's demand capabilities.
///
/// Implementable by any operator — stateful or stateless. Stateless
/// operators that don't override any capability bit can use the default
/// implementation (returns `DemandCapabilities::default()`, all `false`).
///
/// The planner's physical planning pass uses `&dyn DemandPropagation`
/// to reason about demand at plan time without depending on
/// `bqlite-operators`.
pub trait DemandPropagation {
    fn supported_demands(&self) -> DemandCapabilities {
        DemandCapabilities::default()
    }
}
```

The trait is object-safe: the method takes `&self` and returns a `Copy`
type. This allows planner code to work with `&dyn DemandPropagation`
without knowing the concrete operator type.

### 5.1 Dual-Surface Design

Both surfaces survive (promoted from the Wave 1 shape to real types):

**Surface 1: `DemandPropagation` trait** in `bqlite-planner::demand`.
Object-safe, usable by planner code via `&dyn DemandPropagation`. Any
operator type can implement this.

**Surface 2: `EntityOperator::supported_demands()` method** in
`bqlite-operators::operator`. Mirrored method on the existing
`EntityOperator` trait with the same default body.

```rust
// bqlite-operators::operator (additive on existing EntityOperator trait)
pub trait EntityOperator: /* ... existing bounds ... */ {
    // ... existing methods ...

    fn supported_demands(&self) -> DemandCapabilities {
        DemandCapabilities::default()
    }
}
```

### 5.2 Why Two Surfaces

- Planner code reasoning about demand at plan time can take
  `&dyn DemandPropagation` without reaching into `bqlite-operators`
  (trait-object dispatch, no `EntityOperator` bound).

- `EntityOperator` impls get the method "for free" via the mirrored
  default — no second `impl` block needed.

- Operator kinds that aren't `EntityOperator` (e.g. a future push-mode
  streaming sink) still have a uniform advertisement channel via
  `DemandPropagation`.

Implementors override the `EntityOperator::supported_demands()` method
directly when they have capabilities. A separate
`impl DemandPropagation for MyOp {}` block is only needed when the
operator isn't an `EntityOperator`.

Wave 1's comment about "a later wave will unify the two surfaces" is
retired — this document confirms both surfaces survive as the permanent
design.

---

## 6. Capability Matching in Physical Planning

Capability matching happens during physical planning's strategy selection
(planner-pipeline.md §9.4). Engine bind (TASK-438) does **not** re-check.

### 6.1 `const DEMAND_CAPS` on Physical Descriptors

The planner cannot depend on `bqlite-operators` (CLAUDE.md "Dependency
Direction"). To match capabilities at plan time without that dependency,
each physical operator descriptor struct carries a
`const DEMAND_CAPS: DemandCapabilities`:

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

The planner reads the `const` without depending on the operator
implementation crate. The operator crate (`bqlite-operators`) is
**required** to return the same `DemandCapabilities` from its trait
impl — this contract is enforced by a test (see §6.2).

### 6.2 Contract Test

TASK-427 adds a test for each operator kind that asserts:

```rust
assert_eq!(
    <OperatorType>::supported_demands(&operator_instance),
    <OpPhysical>::DEMAND_CAPS,
);
```

This ensures the planner's compile-time capability declaration stays
in sync with the runtime trait implementation. If an operator's
capabilities change, both sites must be updated in the same commit.

### 6.3 Per-Operator Capability Sets

| Operator | Capability bits set to `true` | Source |
|---|---|---|
| SequenceMatch | `step_reached`, `match_count`, `full_detail`, `aggregation_fusion`, `step_property_forwarding` | §3 table |
| Sessionize | `forwarded_columns` | TASK-405 §9 |
| EventSelect | `forwarded_columns` | TASK-411 A6/A7 |
| Attribute | `forwarded_columns` | TASK-406 §10 |

All unlisted bits are `false`. In particular:

- Sessionize, EventSelect, and Attribute do **not** set
  `supports_aggregation_fusion` in v1. Wave 5 will flip these to `true`
  when fusion support is added.
- No operator sets `supports_eager_group_emit` in v1.

### 6.4 Why Planner-Only Matching

Demand matching is a plan-time decision that influences strategy selection,
lowering, and EXPLAIN output. Moving it to engine bind would mean plans
could exist that cannot execute — an error class that is better ruled out
structurally. Defense-in-depth re-checking at bind time is not worth the
duplicated surface.

---

## 7. Unmet-Demand Policy

If the physical planner computes a `DemandSet` that the matched node's
`DEMAND_CAPS` cannot satisfy (e.g. `DemandSet.needs_match_detail = true`
but `supports_full_detail = false`), the planner emits a plan-time error:

```rust
TypeError::UnsupportedDemand {
    node: PlanNodeKind,
    missing: Vec<&'static str>,  // names of the unsupported demand fields
}
```

### 7.1 Why a Hard Error

Three alternatives were considered:

1. **(a) Hard error (chosen).** A plan that can't execute is a planner bug,
   not a user error. Fail fast and loud.

2. **(b) Graceful downgrade.** The planner silently downgrades to a less
   capable strategy. This would hide planner bugs behind
   silently-suboptimal queries.

3. **(c) Operator-side fallback with NULL padding.** The operator fills
   unsupported columns with NULLs. This would break non-nullability
   guarantees on `match_events`, `match_duration`, and forwarded columns
   documented in type-system.md.

### 7.2 Reachability in v1

In v1 the error is unreachable by construction — every v1 operator either
sets every relevant bit to `true` or is never demanded that shape
(EventSelect never gets `needs_match_detail`, etc.). The error exists as
a guardrail against future refactors where a capability is dropped from
an operator without the matching planner update.

---

## 8. Matching Algorithm

The physical planner's demand-capability matching is a straightforward
field-by-field check. For each plan node that advertises capabilities:

```rust
fn check_demand_satisfied(
    demand: &DemandSet,
    caps: &DemandCapabilities,
    node_kind: PlanNodeKind,
) -> Result<(), TypeError> {
    let mut missing = Vec::new();

    if demand.needs_step_reached && !caps.supports_step_reached {
        missing.push("supports_step_reached");
    }
    if demand.needs_match_detail && !caps.supports_full_detail {
        missing.push("supports_full_detail");
    }
    if demand.fused_aggregate.is_some() && !caps.supports_aggregation_fusion {
        missing.push("supports_aggregation_fusion");
    }
    if !demand.step_properties.is_empty() && !caps.supports_step_property_forwarding {
        missing.push("supports_step_property_forwarding");
    }
    if !demand.forwarded.is_empty() && !caps.supports_forwarded_columns {
        missing.push("supports_forwarded_columns");
    }
    // supports_match_count and supports_eager_group_emit are not
    // demand-checked -- they influence strategy selection, not
    // demand satisfaction.

    if missing.is_empty() {
        Ok(())
    } else {
        Err(TypeError::UnsupportedDemand {
            node: node_kind,
            missing,
        })
    }
}
```

### 8.1 Match-Count and Strategy Selection

`supports_match_count` is not a demand-checked bit — it influences
strategy selection rather than demand satisfaction. When
`DemandSet.needs_match_detail` is `false` and `supports_match_count` is
`true`, the physical planner may select a count-only strategy
(StepCounter / DedicatedConsecutive) instead of FullNFA. The absence
of `supports_match_count` does not cause a demand error; it simply means
the planner must use a detail-capable strategy regardless.

### 8.2 Eager-Group-Emit and Wave 5

`supports_eager_group_emit` has no v1 matching logic. The bit is carried
structurally so Wave 5 can add a planner matcher without modifying the
capability struct. When Wave 5 adds eager-emit fusion for
`SESSIONIZE | STATS ... GROUP BY session_id`, the planner will check this
bit during strategy selection for that specific fusion pattern.

---

## 9. Channel Separation

`DemandCapabilities` covers only **flexible-output-shape** operators.
Stateless operators and scan have different demand interactions that do
not use capability bits.

### 9.1 Channel 1: DemandCapabilities (This Protocol)

Advertised by stateful operators with variable output shapes:
SequenceMatch, Sessionize, EventSelect, Attribute. Matched by the
physical planner against the `DemandSet` at each plan node.

### 9.2 Channel 2: Scan Projection

`ScanPhysical.projected_columns: Vec<ColumnId>` is populated by the
backward pass from the accumulated `DemandSet.columns` at the scan
boundary. Scan has no capability bits to advertise — every scan supports
every flat column projection. Scan's demand handling is entirely through
its `projected_columns` field.

### 9.3 Stateless Pass-Through

Filter, Sort, and Limit are neither: they modify the `DemandSet` (filter
adds predicate columns; sort adds sort-key columns; limit passes through)
but produce the same schema as their input. No capability bits needed;
no physical-descriptor field needed.

### 9.4 Why Not a Unified Protocol

The `DemandCapabilities` protocol is inherently a capability negotiation —
it only has meaning where the operator has *choices* about output shape.
Scans, filters, sorts, and limits don't; their demand is a direct
read-through of the `DemandSet` fields. Conflating the two channels
would force every operator to implement a trait whose bits they'd all set
to `false`, adding friction without providing signal.

---

## 10. Non-Exhaustive Decision

The struct is **not** `#[non_exhaustive]`. Operator impls use struct-literal
syntax:

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

### 10.1 Rationale

Adding a capability bit is intentionally a breaking change to every operator
impl — that is the point. Every existing operator must re-answer "do I
support this new capability?" explicitly. Silently defaulting new bits to
`false` via `#[non_exhaustive]` + `..Default::default()` would let new
capabilities ship with operators that should support them but quietly don't,
and the gap would never surface at compile time.

All operator impls live in this workspace (no external users of the
capability struct); the blast radius of a capability addition is bounded
by `cargo build`.

---

## 11. Scaffold Retirement

TASK-427 lands the scaffold retirement as one atomic, merge-first commit:

1. Adds `bqlite_planner::demand::{DemandCapabilities, DemandPropagation}`
   with the real shape defined in this document.

2. Deletes `bqlite_core::demand` entirely. No re-export, no deprecation
   alias. Pre-1.0; no external consumers.

3. Updates every Wave 1 operator scaffold call site:
   - `DemandCapabilities::None` → `DemandCapabilities::none()`
     (const constructor from §2.2).
   - `impl DemandPropagation for Foo {}` (empty body) stays legal —
     default method body returns `DemandCapabilities::none()`, semantics
     unchanged.

4. Updates `bqlite_core::lib.rs` re-export list to remove the `demand`
   module.

5. Adds `const DEMAND_CAPS` to each physical operator descriptor per §6.

6. Adds the per-kind `<Op>::supported_demands() == <OpPhysical>::DEMAND_CAPS`
   contract test per §6.2.

### 11.1 Why All-at-Once

This is a `[TRAIT]` task, and `[TRAIT]` tasks are merge-first. Staging the
move (re-export alias, later cleanup) would leave a window where both
homes exist and the doc can't be authoritative about which one to import.

---

## 12. Follow-On Implications

This section lists the tasks and documents affected by this protocol.
The actual changes are made by the listed tasks, not by this document.

### 12.1 Implementation Tasks

- **TASK-427 (relocation + wiring)** — Lands the atomic commit described
  in §11. Adds `const DEMAND_CAPS` to `SequenceMatchPhysical`,
  `SessionizePhysical`, `EventSelectPhysical`, `AttributePhysical`.
  Wires the planner's strategy-selection pass to check capabilities per §5.
  Implements `TypeError::UnsupportedDemand` per §7.

- **TASK-428 (SessionizeOperator)** — Returns
  `DemandCapabilities { supports_forwarded_columns: true, ..default() }`.
  All other bits `false`. Matches TASK-405 §9 (no v1 fusion).

- **TASK-429 (EventSelectOperator)** — Returns
  `DemandCapabilities { supports_forwarded_columns: true, ..default() }`.
  All other bits `false`. Matches TASK-411 A6/A7.

- **TASK-431 (AttributeOperator)** — Returns
  `DemandCapabilities { supports_forwarded_columns: true, ..default() }`.
  All other bits `false`. Matches TASK-406 §10.

- **Existing SequenceMatchOperator** — Updates from the Wave 1 default
  impl to the full capability set per §3 table.

### 12.2 Documentation Reconciliation (TASK-427)

These document updates are made by TASK-427 in the same checkpoint as
the code change:

- **sequence-matching.md §13.5** — Reconcile the sketch with the final
  field list from §3 (adds `supports_forwarded_columns` and
  `supports_eager_group_emit`; confirms the existing five bits verbatim).

- **operators/sessionize.md §8.7, §14** — Update field name from
  `supports_column_forwarding` to `supports_forwarded_columns` to match
  the canonical name in this document's §3 table.

- **operators/operator-traits.md §7** — Retire the "Deferred to TASK-110"
  note for `supported_demands`; replace with a forward reference to this
  document.

- **planner-pipeline.md §9.3** — Add a cross-reference paragraph pointing
  to this document for the operator-side dual. Update the existing
  "Relationship to `DemandCapabilities`" paragraph to confirm both types
  now live in `bqlite-planner::demand`.

- **planner-pipeline.md §15** — Crate placement table gets a new
  `DemandCapabilities` row pointing to `bqlite-planner`. Remove any
  implication that it lives in `bqlite-core`.

- **execution-model.md §13.2** — Confirm the sentence about
  `DemandCapabilities` crate home is correct (it says `bqlite-planner`).

- **bqlite-core::lib.rs** — `pub mod demand;` and any
  `pub use demand::*;` re-exports are removed.

### 12.3 Wave 5 Implications

- `supports_aggregation_fusion` becomes the main capability bit for fused
  Sessionize / EventSelect / Attribute. Wave 5 flips the operator-side
  setters to `true` and adds the planner-side matchers.

- `supports_eager_group_emit` becomes the bit for the
  `GROUP BY session_id` eager-emit path from planner-pipeline.md §8.3.
  Wave 5 adds the planner-side matcher for the specific
  `SESSIONIZE | STATS ... GROUP BY session_id` fusion pattern.

---

## 13. Resolved Design Questions

| Question | Decision | Rationale |
|---|---|---|
| Enum vs struct vs bitflags? | Plain struct with `bool` fields | Struct-of-bools reads trivially, composes without adapters, and doesn't impose false mutual-exclusion (§2.1) |
| Crate home? | `bqlite-planner::demand` | Both dual types together; `bqlite-operators → bqlite-planner` already exists (§4) |
| `#[non_exhaustive]`? | Not applied | Adding a bit is intentionally breaking; all impls are in-workspace (§10) |
| Where does matching happen? | Physical planning only | Plans that can't execute are ruled out structurally; no engine bind re-check (§6.4) |
| Unmet-demand behavior? | Plan-time `TypeError` | A plan that can't execute is a planner bug, not a user error (§7) |
| Two forwarding bits or one? | Two: step-property + generic column | Different demand shapes, different retention paths, cleaner planner matching (§3.1) |
| Single trait or dual surfaces? | Dual: `DemandPropagation` + `EntityOperator` | Decouples planner from operator crate; non-`EntityOperator` types still advertisable (§5.2) |
| `all()` constructor? | Not provided | No operator legitimately sets all bits to `true` (§2.2) |
| Wave 5 reserved bits? | `supports_eager_group_emit` carried now | Near-zero cost; cleaner Wave 5 merge (§3.3) |
