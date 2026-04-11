# Expression Compilation Model

**Wave**: 2
**Task**: TASK-205
**Status**: draft — fixes the `TypedExpr` / `CompiledExpr` contract
for Wave 2; additive extensions in Wave 3+.

## 1. Scope

This note fixes the two-stage pipeline that turns raw AST expressions
into the runtime form every Wave 2 operator carries:

```
bqlite_ast::Expr  ──type-check──▶  TypedExpr  ──compile──▶  CompiledExpr
  (untyped,        (schema-        (plan-time    (runtime-
   unresolved)      resolved,       cache)       evaluable over
                    typed)                        Arrow batches)
```

It defines:

1. **`TypedExpr`** — the plan-tree form of an expression. Schema-
   resolved, type-checked per type-system.md §4 + §10, and carries a
   cached result `BqlType` so every enclosing node's `output_schema`
   can be computed in O(1). This is what `LogicalPlan` nodes hold.
2. **`CompiledExpr`** — the physical form handed to runtime
   operators. Pre-resolved column indices, pre-coerced literals, a
   small bytecode tree that dispatches to Arrow compute kernels or
   to monomorphized hot paths. This is what
   `FilterPhysical` / `ProjectPhysical` / `ScanPhysical` carry in
   their descriptor fields.
3. **The compilation-target selector** — the rule by which the
   compiler decides, per sub-expression, between the Arrow kernel
   dispatch path and a monomorphized fast path.
4. **Null propagation** — how three-valued logic is encoded so that
   every operator sees the same semantics.
5. **Pushdown surface** — how a `CompiledExpr` answers "can this
   node be pushed into the scan?" and becomes a `ScanConjunct` per
   the predicate-pushdown protocol (storage/predicate-pushdown.md
   §4).
6. **Error taxonomy** — the set of `TypeError` variants raised at
   `TypedExpr` construction time.

What this doc does **not** cover:

- AST grammar or parser internals (bqlite-ast / bqlite-parser —
  TASK-113, TASK-220).
- Logical plan node catalog. That is TASK-204 /
  planner/logical-plan-nodes.md; this doc only specifies the
  expression field each node carries.
- Physical plan descriptors. TASK-226 lowers `TypedExpr` to
  `CompiledExpr` inside the physical-lowering pass; this doc
  specifies both shapes but not the pass mechanics.
- Aggregate expressions (`SUM`, `COUNT`, `AVG`, ...). Those are
  `TypedAggExpr` (planner-pipeline.md §5.2) and live alongside
  `TypedExpr` but have their own type rules. Aggregate compilation
  is Wave 3 territory (TASK-307); this doc calls it out at §10
  only for forward-compat.
- The scalar-function registry population. type-system.md §10.2
  lists the initial functions; TASK-225's impl registers them.

## 2. Relationship to other docs

| Topic | Authoritative doc | Role here |
|---|---|---|
| `BqlType` / `PropertyValue` / coercion rules | type-system.md §3, §4 | The type lattice `TypedExpr` validates against. |
| Scalar function signatures | type-system.md §10 | Function-call type-check rules live here. |
| Null propagation (three-valued logic) | type-system.md §4.5 (implicit) | This doc formalizes how nulls flow through `CompiledExpr`. |
| `OperatorSchema` / column resolution | core/schema.rs, type-system.md §5.2 | The schema a `TypedExpr` resolves column references against. |
| Logical plan node catalog | planner/logical-plan-nodes.md §4 | Every node whose field is `TypedExpr` is listed there. |
| Physical descriptor catalog | planner-pipeline.md §9.5 | Every descriptor whose field is `CompiledExpr` is listed there. |
| Predicate pushdown protocol | storage/predicate-pushdown.md | `CompiledExpr` exposes a `to_scan_conjunct` method that returns `Some` when the expression matches a pushable shape. |
| AST `Expr` / `Literal` / `BinaryOp` | crates/bqlite-ast/src/expr.rs | The input to stage 1. |

## 3. Two-stage pipeline — why not one stage

A single-stage compiler (AST `Expr` directly to runtime form) would
be simpler but misses two concerns:

1. **Schema access is plan-time, not parse-time.** The parser
   produces `Expr` without a catalog; column references are just
   names. Resolving them requires a schema, which the planner
   builds during lowering. Doing the resolution twice (once for
   type-checking at lowering, once again at bind time) would
   duplicate the work the planner already does.
2. **Runtime representation is different from plan-time
   representation.** At plan time we want a small, inspectable
   tree the optimizer can rewrite (constant folding, expression
   inlining, pushdown matching). At runtime we want a flat,
   cache-friendly dispatch table — ideally with pre-resolved
   column indices, pre-coerced literals, and kernel pointers. The
   two representations have genuinely different requirements; a
   single form would compromise one or the other.

The two-stage approach resolves both. `TypedExpr` is the plan-time
form the optimizer touches; `CompiledExpr` is the runtime form
execution operates on. TASK-225 owns both conversions — stage 1
(`Expr → TypedExpr`) runs at logical-plan construction time during
TASK-224's lowering; stage 2 (`TypedExpr → CompiledExpr`) runs at
physical-plan construction time during TASK-226's descriptor
lowering.

## 4. `TypedExpr`

### 4.1 Shape

```rust
// crates/bqlite-planner::expr

use bqlite_ast::{BinaryOp, CompareOp, UnaryOp, Span};
use bqlite_core::{BqlType, PropertyValue};
use std::sync::Arc;

/// An AST expression that has been resolved against an
/// `OperatorSchema`, type-checked, and annotated with its result
/// type. Holding a `TypedExpr` value is proof that the expression
/// is well-typed — ill-typed expressions are rejected at
/// construction.
#[derive(Debug, Clone, PartialEq)]
pub struct TypedExpr {
    pub kind: TypedExprKind,
    /// Result type of this sub-expression under the schema it was
    /// typed against. Cached once at construction so
    /// `OperatorSchema` builders can read it in O(1).
    pub result_type: BqlType,
    /// Whether this sub-expression is nullable. Derived per the
    /// rules in §4.4.
    pub nullable: bool,
    /// Source span for diagnostics, inherited from the AST node
    /// (`bqlite_ast::Span`).
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum TypedExprKind {
    /// A literal that has been coerced to the context type if
    /// necessary. `Literal(PropertyValue)` — not the AST's
    /// `Literal`, because coercion has already collapsed
    /// source-level distinctions (Duration → Int nanos, ISO-8601
    /// string → Timestamp).
    Literal(PropertyValue),
    /// A column reference resolved against the enclosing
    /// `OperatorSchema`. Stores **both** the logical column index
    /// (into `OperatorSchema::columns`) *and* the column name, so
    /// `CompiledExpr` can be lowered without a schema handle and
    /// `to_scan_conjunct` (§8) can produce `ScanConjunct` values
    /// that carry the name directly.
    Column {
        column_index: usize,
        name: String,
    },
    /// Arithmetic binary operation — `a + b`, `x * 2`, `ts - ts`.
    /// Mirrors `bqlite_ast::Expr::Binary` with the same
    /// `bqlite_ast::BinaryOp` — the planner reuses the AST op
    /// enum rather than re-declaring it. Result type follows
    /// type-system.md §4.4.
    Arith {
        op: BinaryOp,
        left: Box<TypedExpr>,
        right: Box<TypedExpr>,
    },
    /// Comparison — `a = b`, `x < 10`. Mirrors
    /// `bqlite_ast::Expr::Compare` with
    /// `bqlite_ast::CompareOp`. Result type is always `Bool`.
    Compare {
        op: CompareOp,
        left: Box<TypedExpr>,
        right: Box<TypedExpr>,
    },
    /// Unary — negation or unary plus — from
    /// `bqlite_ast::Expr::Unary`. Operand must be `Int` or
    /// `Float`; result type matches.
    Unary {
        op: UnaryOp,
        operand: Box<TypedExpr>,
    },
    /// Variadic logical AND — mirrors `bqlite_ast::Expr::And`.
    /// All operands must produce `Bool`. An empty vec is
    /// structurally impossible (the AST never produces one; the
    /// planner rejects a synthetic empty `And` with
    /// `TypeError::Unsupported`).
    And(Vec<TypedExpr>),
    /// Variadic logical OR — mirrors `bqlite_ast::Expr::Or`.
    /// Same constraints as `And`.
    Or(Vec<TypedExpr>),
    /// Logical NOT — mirrors `bqlite_ast::Expr::Not`. Operand
    /// must be `Bool`; result is `Bool`.
    Not(Box<TypedExpr>),
    /// `expr IS NULL` (`negated = false`) or `expr IS NOT NULL`
    /// (`negated = true`). Mirrors `bqlite_ast::Expr::IsNull` —
    /// the `negated` flag is preserved rather than rewritten as
    /// `Not(IsNull(...))` so type-system.md §3's "IS NOT NULL is
    /// a primitive" phrasing is honored and the pushdown
    /// pattern-match in §8 can recognize `IsNotNull` directly
    /// without peeking through a `Not` wrapper.
    IsNull { input: Box<TypedExpr>, negated: bool },
    /// Scalar function call, resolved against the function
    /// registry. `signature` is the resolved entry, not a string
    /// — the lookup happens once at type-check time.
    FunctionCall {
        signature: Arc<ScalarFunctionSig>,
        args: Vec<TypedExpr>,
    },
    /// `CAST(expr AS type)` — **explicit** cast. The target type
    /// is also in `TypedExpr::result_type`; this variant records
    /// the user wrote a cast, so the optimizer knows it must not
    /// elide the node during rewrites.
    Cast {
        input: Box<TypedExpr>,
        target_type: BqlType,
    },
    /// An **implicit** coercion inserted by type-check time — the
    /// Int→Float promotion from type-system.md §4.1 is the only
    /// Wave 2 source. Distinct from `Cast` so the optimizer can
    /// elide implicit coercions during rewrites (per §4.3) while
    /// preserving explicit ones.
    ImplicitCoerce {
        input: Box<TypedExpr>,
        target_type: BqlType,
    },
    /// `expr IN (lit_1, lit_2, ..., lit_k)` — fixed literal set.
    /// `expr IN (subquery)` is a separate logical node
    /// (`SubqueryFilter` — planner-pipeline.md §5.1); this
    /// variant only handles literal sets, which are the Wave 2
    /// form. The AST's `Expr::In` form with `InRhs::List` maps
    /// here; `InRhs::Query` is Wave 4.
    InLiteralSet {
        input: Box<TypedExpr>,
        /// Coerced against `input.result_type` at type-check time.
        values: Vec<PropertyValue>,
        /// `expr NOT IN (...)` sets this true, mirroring the
        /// AST's `In { negated }` field.
        negated: bool,
    },
}
```

### 4.2 Construction — the type-check rules

`TypedExpr::from_ast(expr: &Spanned<Expr>, schema: &OperatorSchema,
registry: &FunctionRegistry)` is the single entry point. It walks
the AST bottom-up, computing the result type at each node according
to type-system.md §4. Every `bqlite_ast::Expr` variant appears in
the table below; Wave 2 either lowers it to a `TypedExprKind` or
rejects it with `TypeError::Unsupported`.

| AST `Expr` variant | Rule | Wave |
|---|---|---|
| `Literal(lit)` | Convert `lit` to a `PropertyValue` (Duration → Int nanos; ISO-8601 string literal stays a string until a comparison coerces it in §4.3). Result type is the literal's `BqlType`. Nullable iff `Literal::Null`. → `TypedExprKind::Literal(pv)`. | 2 |
| `Column(name)` | `schema.column(name).ok_or(TypeError::UnknownColumn { .. })`. Records `column_index` and `name`. Result type and nullability come from the resolved `ColumnDef`. → `TypedExprKind::Column { .. }`. | 2 |
| `Qualified { table, column }` | Deferred to Wave 4 joins; Wave 2 rejects with `TypeError::Unsupported` (Wave 2 sources are single-table only, so a qualifier is either redundant or references a non-joined table). | 2 rejects |
| `Variable($name)` | `TypeError::Unsupported` — `$`-bindings are Wave 3 MATCH territory only. The planner rejects them outside a `MATCH` pattern per query-language.md §4.11. | 2 rejects |
| `Binary { op, left, right }` | Recurse on both sides. Apply the type-system.md §4.4 arithmetic rules to pick the result type (Int+Int=Int, Int+Float=Float with the Int wrapped in `ImplicitCoerce { target_type: Float }`, Timestamp-Timestamp=Int, Timestamp±Int=Timestamp). → `TypedExprKind::Arith { op, left, right }`. | 2 |
| `Unary { op, operand }` | Recurse. Operand must be `Int` or `Float`; result type matches operand. → `TypedExprKind::Unary { op, operand }`. | 2 |
| `Compare { op, left, right }` | Recurse on both sides. If one side is a literal and the other a column, coerce the literal to the column's type at plan time (type-system.md §4.3 step 1). Otherwise both sides must be the same type modulo Int→Float implicit coercion. Result type is `Bool`. → `TypedExprKind::Compare { op, left, right }`. | 2 |
| `And(exprs)` | Recurse on every operand; each must produce `Bool`. Empty or single-element vecs are a parser bug (`TypeError::Unsupported` with an internal-error note). → `TypedExprKind::And(typed)`. | 2 |
| `Or(exprs)` | Same as `And`. → `TypedExprKind::Or(typed)`. | 2 |
| `Not(operand)` | Recurse; operand must be `Bool`. → `TypedExprKind::Not(typed)`. | 2 |
| `IsNull { expr, negated }` | Recurse. Accepts any operand type. Result type is `Bool`. → `TypedExprKind::IsNull { input, negated }` — the `negated` flag is preserved so the pushdown match (§8) can read it directly. | 2 |
| `Between { expr, low, high, negated }` | Desugared to `(expr >= low) AND (expr <= high)` (or its `NOT` for `negated`). Recurse on the desugared form — the `Between` variant never appears in `TypedExprKind`. | 2 |
| `Like { expr, pattern, negated }` | Not pushable per storage/predicate-pushdown.md §4; Wave 2 still lowers it so filter evaluation works above the scan. Planner implements `LIKE` via a compiled regex in `CompiledExpr`. → `TypedExprKind::FunctionCall { signature: like_sig, args: [input, literal(pattern)] }` — i.e. `LIKE` is treated as a built-in function `like(String, String) -> Bool`. `negated` wraps the call in `Not`. | 2 |
| `Regex { expr, pattern, negated }` | Same shape as `Like` but using a `regex` built-in. Pattern parse errors are `TypeError::CoercionFailed` at plan time. | 2 |
| `Contains { expr, list, negated }` | Wave 2 models list-membership predicates over `List<T>` columns, but Wave 2 ships no `List<T>` operators. Reject with `TypeError::Unsupported` and spell out Wave 4 as the landing wave. | 2 rejects |
| `FunctionCall { name, args, over: None }` | Look up `(name, arg_types)` in `registry` with coercion-aware matching (§9.1). Recurse over `args` first so arg types are known. Return type from the matched signature. → `TypedExprKind::FunctionCall { signature, args }`. | 2 |
| `FunctionCall { over: Some(_) }` | Window function. Wave 2 rejects with `TypeError::Unsupported`; Wave 3 grows a separate `WindowCall` variant per planner-pipeline.md §5.1. | 2 rejects |
| `Cast { expr, target }` | Recurse, verify the explicit cast is legal per type-system.md §4.2. Result type is `target`. → `TypedExprKind::Cast { input, target_type: target }`. | 2 |
| `In { input, rhs: InRhs::List(literals), negated }` | Recurse on `input`. Coerce every literal against `input.result_type`; a failure is `TypeError::CoercionFailed`. Result type is `Bool`. → `TypedExprKind::InLiteralSet { input, values, negated }`. | 2 |
| `In { rhs: InRhs::Query(_), .. }` | Cohort subquery form; Wave 4 (TASK-407). Reject with `TypeError::Unsupported`. | 2 rejects |
| `Case { .. }` | Deferred to Wave 3. Reject with `TypeError::Unsupported`. | 2 rejects |
| `Paren(inner)` | Transparent — recurse, return the inner `TypedExpr` unchanged (spans pick up the outer parens for diagnostics). | 2 |

### 4.3 Coercion at type-check time

Follows type-system.md §4.1 exactly. The relevant cases for Wave 2:

- **Literal vs column in comparison.** `col op literal` coerces
  `literal` to `col.bql_type` at plan time. This is where an ISO
  string literal becomes a `Timestamp` value
  (`'2026-03-01' → Timestamp(...)`, §4.3 step 1) and where a
  duration literal becomes an Int nanos value
  (`7d → 604_800_000_000_000 Int`). The coerced literal is stored
  directly as `TypedExprKind::Literal(PropertyValue)`; no
  `ImplicitCoerce` wrapper is needed because the literal never
  existed as its pre-coercion form in `TypedExpr`.
- **Mixed Int / Float arithmetic.** The `Int` operand is promoted
  to `Float` by wrapping it in `TypedExprKind::ImplicitCoerce {
  target_type: Float }`. The wrapper is structurally distinct
  from an explicit `TypedExprKind::Cast`, which lets the
  optimizer elide the implicit coercion during constant folding
  or expression inlining while preserving explicit `CAST(x AS
  FLOAT)` calls — see §4.2's "explicit casts must not be elided"
  rule. `CompiledExpr` lowering (§5.3) treats both the same way
  and just emits the right Arrow cast kernel.
- **Everything else.** No implicit coercion. Every other type
  change is an explicit `CAST`.

### 4.4 Nullability propagation

`TypedExpr::nullable` is computed at construction. Wave 2 uses the
**conservative over-approximation**: any kind whose operand is
nullable is itself nullable. Kleene AND/OR short-circuit rules
(`T OR N = T`, `F AND N = F`) are *precise* rather than
conservative — a conservative reading of them would say the
result is nullable, but that is wrong when the short-circuit
fires. The conservative rule below is safe for Wave 2 because it
is only used to populate `OperatorSchema` nullability hints;
runtime evaluation honors the precise Kleene semantics through
the Arrow kernels (§7). Wave 5 may tighten this — replacing the
conservative `|| .any(...)` rules with precise Kleene — without
breaking any consumer, because the rule moves only from "maybe
null" to "definitely not null".

| Kind | Nullable iff |
|---|---|
| `Literal(PropertyValue::Null)` | always |
| `Literal(other)` | never |
| `Column { column_index, .. }` | the resolved column's `nullable` flag is true |
| `Arith { op, left, right }` | `left.nullable || right.nullable` |
| `Compare { .. }` | `left.nullable || right.nullable` |
| `Unary { op, operand }` | `operand.nullable` |
| `And(exprs)` / `Or(exprs)` | any element is nullable (conservative; see above) |
| `Not(operand)` | `operand.nullable` — three-valued NOT preserves UNKNOWN |
| `IsNull { .. }` | **never** — both `IS NULL` and `IS NOT NULL` are guaranteed TRUE or FALSE per type-system.md §3 |
| `FunctionCall { signature, args }` | `signature.nullable || args.iter().any(|a| a.nullable)` |
| `Cast { input, .. }` | `input.nullable` — explicit casts additionally emit NULL on parse failure per type-system.md §4.2, making this nullable in practice |
| `ImplicitCoerce { input, .. }` | `input.nullable` — lossless widening, no new nulls |
| `InLiteralSet { input, .. }` | `input.nullable` |

### 4.5 The "holding a `TypedExpr` means it's well-typed" invariant

Per planner-pipeline.md §4.5, every `LogicalPlan` node validates
schemas at construction. `TypedExpr` is half of that validation —
the other half is operator-level schema matching (e.g. Filter's
predicate must be Bool). A `TypedExpr` whose `result_type` does not
match the enclosing node's requirement is rejected at the *enclosing
node's* construction, not at `TypedExpr`'s own construction. This
keeps `TypedExpr` reusable: the same expression shape can be a
Filter predicate (requires Bool), a Project item (any type), or an
Aggregate argument (type per function signature).

## 5. `CompiledExpr`

### 5.1 Shape

```rust
// crates/bqlite-planner::compiled

/// Runtime-evaluable expression carried by physical descriptors.
///
/// Unlike `TypedExpr`, `CompiledExpr` is a flat dispatch tree:
/// column indices are pre-resolved to `usize` offsets into the
/// runtime `RecordBatch`'s columns, literals are pre-coerced to
/// `PropertyValue`, and every node names the Arrow kernel or
/// monomorphized hot-path function it dispatches to.
#[derive(Debug, Clone)]
pub struct CompiledExpr {
    pub node: CompiledNode,
    /// Cached result type, identical to the originating
    /// `TypedExpr::result_type`. Carried here so runtime operators
    /// can ask for it without re-typing.
    pub result_type: BqlType,
    /// Whether this node may emit NULL at runtime.
    pub nullable: bool,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum CompiledNode {
    /// A broadcast literal. The runtime evaluator materializes
    /// this as an Arrow scalar array at call time (once per
    /// batch), then relies on Arrow kernel broadcasting.
    Literal(PropertyValue),
    /// A column read, carrying both the runtime batch index and
    /// the column's name.
    ///
    /// `index` is the position in the runtime `RecordBatch` built
    /// from the scan's `projected_columns` — so it may differ
    /// from the `TypedExprKind::Column::column_index` (which was
    /// against the node's `OperatorSchema`, pre-pruning).
    /// TASK-226's physical lowering remaps the index during
    /// `CompiledExpr::from_typed` (§5.3).
    ///
    /// `name` is carried alongside the index so
    /// `to_scan_conjunct` (§8) can produce `ScanConjunct` values
    /// — which are name-keyed per storage/predicate-pushdown.md
    /// §5 — without needing a schema handle or a name-lookup
    /// closure. This costs one extra `String` per column
    /// reference in the compiled tree, which is negligible:
    /// compiled trees hold a dozen expressions at most, not
    /// millions. The alternative (stateful `to_scan_conjunct`
    /// that takes a schema) puts more burden on TASK-227's pass
    /// for no measurable benefit.
    Column { index: usize, name: String },
    /// Arithmetic binary op — mirrors `TypedExprKind::Arith`.
    Arith {
        op: BinaryOp,
        left: Box<CompiledExpr>,
        right: Box<CompiledExpr>,
        kernel: ArithKernel,
    },
    /// Comparison — mirrors `TypedExprKind::Compare`.
    Compare {
        op: CompareOp,
        left: Box<CompiledExpr>,
        right: Box<CompiledExpr>,
        kernel: CompareKernel,
    },
    /// Unary arithmetic — mirrors `TypedExprKind::Unary`.
    Unary {
        op: UnaryOp,
        operand: Box<CompiledExpr>,
        kernel: UnaryKernel,
    },
    /// Variadic logical AND — mirrors `TypedExprKind::And`.
    /// Evaluated via Arrow's `and_kleene` reduced across the
    /// operand list, or via the monomorphized `FusedEqAnd` fast
    /// path when every operand is a dict-equality comparison
    /// (§6).
    And {
        operands: Vec<CompiledExpr>,
        kernel: LogicalKernel,
    },
    /// Variadic logical OR — mirrors `TypedExprKind::Or`.
    /// Evaluated via Arrow's `or_kleene`. Wave 2 has no OR fast
    /// path; the Wave 2 acceptance query is pure-AND shape.
    Or {
        operands: Vec<CompiledExpr>,
        kernel: LogicalKernel,
    },
    /// Logical NOT — mirrors `TypedExprKind::Not`. Dispatches to
    /// Arrow's `not` kernel.
    Not(Box<CompiledExpr>),
    /// `input IS NULL` (`negated = false`) or `IS NOT NULL`
    /// (`negated = true`).
    IsNull { input: Box<CompiledExpr>, negated: bool },
    /// Scalar function call. The signature is resolved; the
    /// kernel pointer is the chosen evaluator.
    FunctionCall {
        signature: Arc<ScalarFunctionSig>,
        args: Vec<CompiledExpr>,
        kernel: FunctionKernel,
    },
    /// Explicit `CAST`. `target_type` is also in the enclosing
    /// `CompiledExpr::result_type`; keep it here for readability.
    Cast {
        input: Box<CompiledExpr>,
        target_type: BqlType,
        kernel: CastKernel,
    },
    /// Implicit coercion inserted at type-check time (Int→Float).
    /// Lowered to an Arrow cast kernel at runtime, identical to
    /// `Cast`, but carried separately so optimizer passes can
    /// tell them apart (the explicit-cast-must-not-be-elided rule
    /// from §4.3).
    ImplicitCoerce {
        input: Box<CompiledExpr>,
        target_type: BqlType,
        kernel: CastKernel,
    },
    /// `input IN (lit_1, ..., lit_k)` (negated or not). Compiled
    /// into a single kernel call that builds the literal set
    /// once per batch.
    InLiteralSet {
        input: Box<CompiledExpr>,
        values: Vec<PropertyValue>,
        negated: bool,
        kernel: InSetKernel,
    },
}
```

### 5.2 The `*Kernel` enums

Each `*Kernel` is a tagged union naming the chosen compilation
target for the node. §6 specifies the selection rule. A minimal
Wave 2 shape:

```rust
/// Kernel selection for arithmetic binary operators.
#[derive(Debug, Clone, Copy)]
pub enum ArithKernel {
    /// Arrow compute kernel (e.g.
    /// `arrow::compute::kernels::numeric::add`). Covers every
    /// (Int|Float, Int|Float, Timestamp±Int, Timestamp-Timestamp)
    /// combination.
    ArrowKernel(ArrowKernelId),
}

/// Kernel selection for comparison operators.
#[derive(Debug, Clone, Copy)]
pub enum CompareKernel {
    /// Arrow compute kernel (e.g.
    /// `arrow::compute::kernels::cmp::eq`). Covers every
    /// comparison shape except those folded into `LogicalKernel::FusedEqAnd`.
    ArrowKernel(ArrowKernelId),
}

/// Kernel selection for variadic logical operators (`And`, `Or`).
#[derive(Debug, Clone, Copy)]
pub enum LogicalKernel {
    /// Generic path: fold `arrow::compute::and_kleene` /
    /// `or_kleene` across the operand list. Covers every shape
    /// not explicitly monomorphized.
    ArrowKleene,
    /// Monomorphized hot path for the *only* Wave 2 fast-path
    /// shape: an `And` whose every operand is a `Compare { Eq,
    /// Column, Literal }` on a dictionary-encoded column. The
    /// Wave 2 acceptance query's first conjunct
    /// (`event = 'checkout'`) is this shape, and combining it
    /// with the `amount > 100` range conjunct still uses the
    /// Arrow path for the range and `FusedEqAnd` for the dict
    /// equality, composed with a final `arrow::compute::and`
    /// over the two resulting bitmaps. Wave 5 may add more fast
    /// paths; each is a new variant here.
    FusedEqAnd,
}

#[derive(Debug, Clone, Copy)]
pub enum UnaryKernel { ArrowKernel(ArrowKernelId) }

#[derive(Debug, Clone, Copy)]
pub enum FunctionKernel { Registered(FunctionId) }

#[derive(Debug, Clone, Copy)]
pub enum CastKernel { ArrowKernel(ArrowKernelId) }

#[derive(Debug, Clone, Copy)]
pub enum InSetKernel {
    /// Generic path: build an Arrow set kernel per batch.
    ArrowIsIn,
    /// Fast path: if `input` is a dictionary-encoded column and
    /// every literal resolves to a code, the kernel walks the
    /// bit-packed codes without materializing a dict-decoded
    /// column. This is the runtime twin of the dictionary
    /// pushdown specified in storage/predicate-pushdown.md §7.
    DictionaryCodes,
}
```

`ArrowKernelId` and `FunctionId` are opaque identifiers — the
runtime resolves them via a lookup table owned by `bqlite-planner`
(for the built-in kernels) or `bqlite-operators` (for function
implementations). Keeping them as enum IDs rather than function
pointers means `CompiledExpr` stays `Clone + Debug` without any
`unsafe`, at a negligible per-call cost (one lookup indirection
per kernel dispatch per batch, amortized across 64K rows).

### 5.3 Lowering `TypedExpr → CompiledExpr`

TASK-226's physical-lowering pass calls
`CompiledExpr::from_typed(typed: &TypedExpr, runtime_schema:
&OperatorSchema)` once per expression in each physical descriptor.
The conversion is mechanical:

1. `Literal(pv) → CompiledNode::Literal(pv)`.
2. `Column { column_index: _, name } → CompiledNode::Column { index: runtime_schema.column(&name).expect("column survives to runtime").0, name: name.clone() }`.
   The name-based re-lookup handles projection pruning: if the
   planner pruned the scan's column set, the logical
   `column_index` may no longer be valid, but the column name is.
   The name is also preserved in `CompiledNode::Column` so
   `to_scan_conjunct` (§8) can read it without a schema handle.
3. `Arith { op, left, right } → CompiledNode::Arith { op,
   left: recurse, right: recurse, kernel:
   select_arith_kernel(op, &left, &right) }`.
4. `Compare { op, left, right } → CompiledNode::Compare { op,
   left: recurse, right: recurse, kernel:
   select_compare_kernel(op, &left, &right) }`.
5. `And(exprs) → CompiledNode::And { operands: exprs.map(recurse),
   kernel: select_logical_kernel_for_and(&operands) }` — the
   selector inspects the operand list for the `FusedEqAnd` shape
   per §6.
6. `Or(exprs) → CompiledNode::Or { operands, kernel:
   LogicalKernel::ArrowKleene }` (Wave 2 has no OR fast path).
7. `Not(operand) → CompiledNode::Not(recurse)`.
8. `IsNull { input, negated } → CompiledNode::IsNull { input:
   recurse, negated }`.
9. `Unary { op, operand } → CompiledNode::Unary { op, operand:
   recurse, kernel: select_unary_kernel(op, &operand) }`.
10. `FunctionCall { signature, args } → CompiledNode::FunctionCall
    { signature, args: args.map(recurse), kernel:
    FunctionKernel::Registered(signature.function_id) }`.
11. `Cast { input, target_type } → CompiledNode::Cast { input:
    recurse, target_type, kernel:
    CastKernel::ArrowKernel(lookup_cast(input.result_type,
    target_type)) }`.
12. `ImplicitCoerce { input, target_type } → CompiledNode::ImplicitCoerce
    { ... }` — same shape as `Cast` lowering.
13. `InLiteralSet { input, values, negated } → CompiledNode::InLiteralSet
    { input: recurse, values, negated, kernel:
    select_inset_kernel(&input, &values) }`.

The only interesting work is kernel selection; the tree walk is
pure structure.

## 6. Compilation-target selection

Wave 2 has exactly two compilation targets:

1. **Arrow compute kernels** — the generic path. Every operator,
   every type combination, every function. Slower per call than
   a monomorphized loop but covers the entire type lattice
   without any work at compile time. The kernels are picked by
   looking up `(op, lhs_type, rhs_type)` in a static table the
   compiler owns.
2. **Monomorphized fast paths** — hand-written hot-path loops for
   the expression shapes the benchmark gate (§12) measures. Wave
   2 ships exactly one: `FusedEqAnd` for "AND of one or more
   equality comparisons on dictionary-encoded string columns",
   which is what the acceptance query
   `where event = 'checkout' AND amount > 100` (the first
   conjunct) evaluates to after dictionary pushdown.

The selectors are three thin functions. Only the `And` selector
implements the monomorphized fast path; the arithmetic / compare
/ unary / cast / inset selectors all dispatch unconditionally to
Arrow.

```rust
fn select_arith_kernel(
    op: BinaryOp,
    left: &CompiledExpr,
    right: &CompiledExpr,
) -> ArithKernel {
    ArithKernel::ArrowKernel(
        lookup_arrow_arith_kernel(op, left.result_type, right.result_type),
    )
}

fn select_compare_kernel(
    op: CompareOp,
    left: &CompiledExpr,
    right: &CompiledExpr,
) -> CompareKernel {
    CompareKernel::ArrowKernel(
        lookup_arrow_compare_kernel(op, left.result_type, right.result_type),
    )
}

fn select_logical_kernel_for_and(operands: &[CompiledExpr]) -> LogicalKernel {
    // Wave 2 monomorphization set — exactly one shape:
    // every operand is an Eq between a dictionary-encoded column
    // and a literal.
    if !operands.is_empty() && operands.iter().all(is_dict_eq) {
        LogicalKernel::FusedEqAnd
    } else {
        LogicalKernel::ArrowKleene
    }
}
```

The `is_dict_eq` helper recognizes the one fast-path shape:

```rust
/// True if `expr` is exactly `Compare { Eq, Column(c), Literal(lit) }`
/// (or the flipped form `Literal == Column`) where column `c` is
/// dictionary-encoded in the runtime schema.
///
/// The "is dictionary-encoded" check consults a small hint
/// map populated by the physical-lowering pass (TASK-226) when
/// it inspects the segment reader's column encodings. Scans
/// whose segments are all dictionary-encoded set the hint;
/// scans over a mix of encodings set it only for the columns
/// that are dict-encoded in every segment. The hint map lives
/// on the `OperatorSchema` as an optional sidecar —
/// `schema.dictionary_columns() -> &HashSet<String>` — and
/// defaults to empty for non-scan-descended operators.
fn is_dict_eq(expr: &CompiledExpr) -> bool {
    let (col, _lit) = match &expr.node {
        CompiledNode::Compare { op: CompareOp::Eq, left, right, .. } => {
            match (&left.node, &right.node) {
                (CompiledNode::Column { name, .. }, CompiledNode::Literal(l)) => {
                    (name.as_str(), l)
                }
                (CompiledNode::Literal(l), CompiledNode::Column { name, .. }) => {
                    (name.as_str(), l)
                }
                _ => return false,
            }
        }
        _ => return false,
    };
    // Caller context owns the dict-columns set. In practice the
    // selector is called with the hint map in scope as a closure
    // environment; see the sketch in TASK-225's impl notes.
    col_is_dict_encoded(col)
}
```

Widening the fast-path set is a Wave 5 task — profiling will
identify hot shapes, and each new monomorphization is a new
variant in `LogicalKernel` (or a sibling kernel enum) plus a new
branch in the selector. A couple dozen lines each.

### 6.1 Why Arrow kernels are the default

- **Correctness.** Arrow compute kernels are well-tested, handle
  three-valued logic correctly, and compose cleanly over every
  Arrow array type we use. Monomorphizing shape by shape risks
  subtle semantic drift from the Arrow reference.
- **Coverage.** The Arrow kernel surface covers every arithmetic
  and comparison operator we need, including the null-propagation
  semantics from §7 for free. Monomorphizing would mean
  re-implementing null-aware arithmetic by hand for each shape.
- **Evolution.** As Arrow releases improve kernel performance, we
  inherit the speedup without any change to `CompiledExpr`.
- **Wave 2 acceptance gate is met by one fast path.** The
  benchmark target of 500M rows/sec on the filter step is hit by
  the `FusedEqAnd` path on dictionary columns; the rest of the
  Arrow-kernel path does not need to match that throughput to
  satisfy Wave 2's gate.

### 6.2 What a monomorphized path actually looks like

Informational — TASK-225 writes the first one. The `FusedEqAnd`
kernel takes a list of `(column_index, dict_code)` pairs and
a `RecordBatch` and emits a BooleanArray using bit-packed code
comparison:

```rust
fn fused_eq_and_on_dict_codes(
    batch: &RecordBatch,
    terms: &[(usize, u32)],
) -> BooleanArray {
    // For each term: downcast the column to a DictionaryArray,
    // get its code buffer, compare against the term's code,
    // AND the resulting bitmap into the accumulator.
    // Null codes propagate UNKNOWN per §7.
    ...
}
```

The kernel is tight, allocation-free past the per-term bitmap
build, and trivial to test against an Arrow-kernel reference.

## 7. Null propagation and three-valued logic

Every `CompiledExpr` kernel honours type-system.md §4's three-
valued logic:

- **Any arithmetic or comparison on a NULL operand produces
  NULL.** Arrow compute kernels already implement this.
- **Logical AND is `{T&T=T, T&F=F, T&N=N, F&F=F, F&N=F,
  N&N=N}`.** Arrow's `and_kleene` kernel handles this. The
  monomorphized `FusedEqAnd` kernel must match this semantics: a
  row where any conjunct's column is NULL has an UNKNOWN result
  for that conjunct, and the overall AND result is UNKNOWN unless
  some other conjunct explicitly evaluates to FALSE.
- **Logical OR is `{T|T=T, T|F=T, T|N=T, F|F=F, F|N=N, N|N=N}`.**
  Arrow's `or_kleene` kernel.
- **`NOT NULL = NULL`.** Arrow's negation kernel.
- **`col IS NULL` is always TRUE or FALSE — never NULL.** This is
  the only null-test that doesn't propagate.
- **Filter operator semantics.** The Filter operator treats an
  UNKNOWN row the same as FALSE — the row is dropped. This is
  the standard SQL semantics (type-system.md §4.5 implicit) and
  is implemented once, inside the Filter operator, not inside
  `CompiledExpr`. `CompiledExpr` returns a `BooleanArray` with
  null bitmap; the filter operator ANDs the validity bitmap with
  the value bitmap before dropping rows.

The compiler does not emit any special-case logic for null
propagation — it relies on the underlying Arrow kernels and the
filter operator's null-aware row-drop. This is exactly why Arrow
kernels are the default target: they are null-aware for free.

## 8. Pushdown integration

TASK-227's predicate-pushdown pass (planner-pipeline.md §6.4)
examines each `CompiledExpr` inside a `FilterPhysical` and asks
"can this be pushed into the parent `ScanPhysical`?" The answer is
a method on `CompiledExpr`:

```rust
impl CompiledExpr {
    /// If this expression matches one of the pushable shapes
    /// from storage/predicate-pushdown.md §4, return the
    /// equivalent `ScanConjunct`. Otherwise return `None`.
    ///
    /// `CompiledNode::Column` carries the column name inline, so
    /// no schema handle is needed. The method is a pure pattern
    /// match over `self.node`; the returned `ScanConjunct` has
    /// the same ownership as cloned sub-values inside `self`.
    ///
    /// The caller (TASK-227) moves the returned `ScanConjunct`
    /// into `ScanPhysical.scan_predicates` **and deletes** the
    /// matching `CompiledExpr` from
    /// `FilterPhysical.predicate` — the residual contract
    /// specified in storage/predicate-pushdown.md §8.1.
    pub fn to_scan_conjunct(&self) -> Option<ScanConjunct> {
        match &self.node {
            // -----  Equality / Inequality  -----
            CompiledNode::Compare { op, left, right, .. } => {
                // Try column op literal, then literal op column.
                let (col_name, value, op) = match_col_lit(left, right, *op)?;
                match op {
                    CompareOp::Eq => Some(ScanConjunct::Equal {
                        column: col_name.to_string(),
                        value,
                    }),
                    CompareOp::Ne => Some(ScanConjunct::NotEqual {
                        column: col_name.to_string(),
                        value,
                    }),
                    CompareOp::Lt => Some(ScanConjunct::Range {
                        column: col_name.to_string(),
                        op: RangeOp::Lt,
                        value,
                    }),
                    CompareOp::Le => Some(ScanConjunct::Range {
                        column: col_name.to_string(),
                        op: RangeOp::Le,
                        value,
                    }),
                    CompareOp::Gt => Some(ScanConjunct::Range {
                        column: col_name.to_string(),
                        op: RangeOp::Gt,
                        value,
                    }),
                    CompareOp::Ge => Some(ScanConjunct::Range {
                        column: col_name.to_string(),
                        op: RangeOp::Ge,
                        value,
                    }),
                }
            }

            // -----  InSet  -----
            CompiledNode::InLiteralSet { input, values, negated: false, .. } => {
                let CompiledNode::Column { name, .. } = &input.node else {
                    return None;
                };
                Some(ScanConjunct::InSet {
                    column: name.clone(),
                    values: values.clone(),
                })
            }
            // `NOT IN` is explicitly non-pushable in Wave 2 —
            // storage/predicate-pushdown.md §4 has no
            // `ScanConjunct::NotInSet` variant. TASK-227 leaves
            // it in the residual filter.

            // -----  IsNull / IsNotNull  -----
            CompiledNode::IsNull { input, negated } => {
                let CompiledNode::Column { name, .. } = &input.node else {
                    return None;
                };
                if *negated {
                    Some(ScanConjunct::IsNotNull { column: name.clone() })
                } else {
                    Some(ScanConjunct::IsNull { column: name.clone() })
                }
            }

            // -----  Everything else is non-pushable  -----
            // Arith, Unary, And, Or, Not, FunctionCall, Cast,
            // ImplicitCoerce all stay in the residual filter per
            // predicate-pushdown.md §4.
            _ => None,
        }
    }
}

/// Helper: if `(left, right)` is `(Column, Literal)` or
/// `(Literal, Column)`, return the column name, the literal
/// value, and an `op` flipped so the resulting predicate is in
/// canonical "column op literal" order.
///
/// The flip is necessary for range ops: `5 < col` is equivalent
/// to `col > 5`, so the caller must receive `CompareOp::Gt`, not
/// `Lt`, when the literal is on the left.
fn match_col_lit<'a>(
    left: &'a CompiledExpr,
    right: &'a CompiledExpr,
    op: CompareOp,
) -> Option<(&'a str, PropertyValue, CompareOp)> {
    if let (CompiledNode::Column { name, .. }, CompiledNode::Literal(v)) =
        (&left.node, &right.node)
    {
        return Some((name.as_str(), v.clone(), op));
    }
    if let (CompiledNode::Literal(v), CompiledNode::Column { name, .. }) =
        (&left.node, &right.node)
    {
        let flipped = match op {
            CompareOp::Eq => CompareOp::Eq,
            CompareOp::Ne => CompareOp::Ne,
            CompareOp::Lt => CompareOp::Gt,
            CompareOp::Le => CompareOp::Ge,
            CompareOp::Gt => CompareOp::Lt,
            CompareOp::Ge => CompareOp::Le,
        };
        return Some((name.as_str(), v.clone(), flipped));
    }
    None
}
```

**Variadic And/Or decomposition is the caller's job.** The Wave 2
taxonomy only pushes individual conjuncts, not `And` or `Or`
nodes. TASK-227's pass owns the decomposition: it walks a
top-level `CompiledNode::And { operands, .. }` at `FilterPhysical`
root and calls `to_scan_conjunct` on each operand. Operands that
return `Some` are moved into `ScanPhysical.scan_predicates`;
operands that return `None` are collected into a residual
`And { operands }` that replaces the original in `FilterPhysical`.
If every operand is pushed, the residual is empty and the
optimizer elides `FilterPhysical` entirely (three-way split per
storage/predicate-pushdown.md §8.1).

`Or` is never decomposed — an OR is non-pushable as a whole and
stays in the residual filter. Disjunction is pushable only when
both sides are equalities on the **same** column, which
TASK-227's pass 1 rewrites into an `InLiteralSet` *before*
calling `to_scan_conjunct`.

`Not` wrapping: the only `Not`-shape that pushes is
`Not(IsNull(col))`, and even that is *not* routed through
`to_scan_conjunct` because `TypedExpr::from_ast` already
preserves the AST's `IsNull { negated }` primitive. A Wave 2
optimizer pass 5 (constant folding / `Not` normalization) may
push `Not` into child shapes before pushdown runs, but Wave 2
does not implement that pass. `to_scan_conjunct` simply rejects
`CompiledNode::Not` — it stays in the residual filter.

## 9. Function registry integration

The scalar functions in type-system.md §10.2 need:

1. A registry keyed by `(name, arg_types)` returning a
   `ScalarFunctionSig`. Lookups happen at type-check time (stage
   1 of the pipeline).
2. A kernel table keyed by `FunctionId` returning the actual
   runtime evaluator. Lookups happen at compile time (stage 2).

Both tables are owned by `bqlite-planner` and populated at the
start of the first call to `TypedExpr::from_ast` via a `once_cell`
initializer. TASK-225's impl includes a helper macro
`register_function!(name, (arg_types), return_type, nullable,
kernel)` that registers both in one call.

The two-table split (signature vs kernel) matches the two-stage
pipeline: signatures are enough for type-checking, kernels are
only needed at compile time. This means a planner change that
rewrites expressions (optimizer pass) never touches the kernel
table.

### 9.1 Overloaded functions

`QUANTIZE` (type-system.md §10.2) has four overloads:
- `QUANTIZE(Timestamp, Int) → Timestamp`
- `QUANTIZE(Timestamp, Int, String) → Timestamp`
- `QUANTIZE(Int, Int) → Int`
- `QUANTIZE(Float, Float) → Float`

The registry keys on `(name, arg_types)`, so the four overloads
are four registry entries with the same `name` and different
`arg_types`. `TypedExpr::from_ast` looks up by matching the actual
argument types; a failure is a `TypeError::NoMatchingOverload`.
Coercion-compatible matches are allowed (the third `QUANTIZE(Int,
Float) → Float` row in §10.2 is such a case — the Int is coerced
to Float and the `QUANTIZE(Float, Float)` overload is chosen).

## 10. Aggregate expressions — forward compat

Aggregate expressions (`SUM`, `COUNT`, `AVG`, `MIN`, `MAX`,
`DISTINCT_COUNT`) are **not** `TypedExpr`. They live in a separate
`TypedAggExpr` enum (planner-pipeline.md §5.2) because:

1. **Stateful evaluation.** Scalar expressions evaluate per-row;
   aggregates maintain per-group state across rows. The runtime
   shapes are fundamentally different.
2. **Planner-side typing.** The return type of
   `COUNT(col) → Int` does not depend on `col.bql_type`; this
   shape is captured naturally by a distinct enum per aggregate
   function.
3. **Optimizer interactions.** Fusion (planner-pipeline.md §7)
   targets aggregates specifically, and keeping them in a
   separate enum makes the pass easier to write.

`TypedExpr` appears *inside* `TypedAggExpr::args` — so a
`TypedAggExpr::Sum { arg: TypedExpr }` carries a scalar expression
as its argument, and the scalar typing rules from §4 apply. The
aggregate layer adds its own type rules on top (e.g. `SUM` requires
a numeric argument, `COUNT` accepts any argument, `MIN`/`MAX`
require an ordered type).

Wave 2 does not ship aggregates. `TypedExpr::from_ast` returns
`TypeError::Unsupported` for any `Expr::Call` whose name resolves
to an aggregate in the function registry. The registry entries
themselves can land in Wave 2 (so the resolution target exists);
only the *use* of an aggregate is rejected. This keeps Wave 3's
aggregate work a pure additive change.

## 11. Error taxonomy

`TypedExpr::from_ast` raises exactly these error variants (added
to the existing `TypeError` from planner-pipeline.md §12):

| Variant | When raised | Wave |
|---|---|---|
| `TypeError::UnknownColumn { name, span }` | `Expr::Column(name)` did not resolve in the enclosing schema | 2 |
| `TypeError::UnknownFunction { name, span }` | `Expr::Call { name, .. }` did not resolve in the registry | 2 |
| `TypeError::NoMatchingOverload { name, arg_types, span }` | Function name resolved but no overload matched the argument types | 2 |
| `TypeError::TypeMismatch { expected, actual, context, span }` | Binary/unary operand types don't match their rule, or a context requires a type the expression doesn't produce (Filter wants Bool, got Int) | 2 |
| `TypeError::CoercionFailed { from, to, span }` | An implicit coercion the rule-table expects failed (e.g. ISO string literal that does not parse) | 2 |
| `TypeError::Unsupported { construct, span }` | An AST variant that is valid grammar but not yet supported (aggregates, CASE, window functions) | 2 |

All variants carry a `span` so the error renderer can quote the
offending fragment. This mirrors the error policy from
query-language.md §27 ("halt on first error, with source span").

`TypedExpr::from_ast` returns `Result<TypedExpr, TypeError>` — no
warning channel, no partial results. A failure aborts the
enclosing logical node's construction, which aborts the whole
plan. This is the strict planner-pipeline.md §4.5 invariant.

## 12. Wave 2 implementation task mapping

| Concern | Owner | Notes |
|---|---|---|
| `TypedExpr`, `TypedExprKind`, `TypedBinaryOp`, `TypedUnaryOp` | TASK-225 | Lives in `crates/bqlite-planner/src/expr.rs`. |
| `TypedExpr::from_ast` (type-checker) | TASK-225 | Follows §4.2 rule table. |
| `CompiledExpr`, `CompiledNode`, `*Kernel` enums | TASK-225 | Lives in `crates/bqlite-planner/src/expr.rs` alongside `TypedExpr`. |
| `CompiledExpr::from_typed` (lowering) | TASK-225 | Called by TASK-226 during physical-plan lowering. |
| Kernel selector | TASK-225 | `select_binary_kernel` etc. per §6. |
| `FusedEqAnd` monomorphized kernel | TASK-225 | The one Wave 2 fast path. Benchmark evidence is TASK-236. |
| Arrow kernel lookup table | TASK-225 | Static table of `(op, lhs_type, rhs_type) → ArrowKernelId`. |
| Function registry population (type-system.md §10.2 functions) | TASK-225 | `once_cell`-initialized; extensible. |
| `CompiledExpr::to_scan_conjunct` | TASK-225 | §8 method; consumed by TASK-227. |
| TASK-226 uses `CompiledExpr` in `FilterPhysical`, `ProjectPhysical`, `ScanPhysical` | TASK-226 | No change here — §5's shape is what TASK-226 will carry. |
| TASK-227 pushdown pass walks `CompiledExpr::to_scan_conjunct` | TASK-227 | §8. |
| TASK-224 `LogicalPlan` uses `TypedExpr` in Filter / Project / Insert | TASK-224 | §4. |
| Property tests: `Expr → TypedExpr → CompiledExpr → Arrow batch evaluation` round-trip is consistent | TASK-225 | One property test per type combo in the acceptance-query shape. |
| Benchmark: `FusedEqAnd` vs Arrow kernel on 1M dictionary-encoded strings | TASK-236 | Wave 2 perf-gate row "Filter with pushed-down equality on dictionary-encoded column ≥ 500M rows/sec effective". |

### 12.1 Unblocking chain

TASK-224 (logical plan) and TASK-225 (expression compiler) are
listed as parallel in the TASKS.md dep graph: TASK-224 depends on
TASK-204 only, TASK-225 depends on TASK-205 only. This is
intentional — TASK-224's `LogicalPlan::Filter` / `Project` fields
reference `TypedExpr` by name only; the actual type can be a
stub until TASK-225 lands, at which point TASK-224 rebases onto
the real `TypedExpr` import and its tests exercise real type
checking. TASK-226 combines them in the physical lowering step.

In practice this means TASK-224 ships with `TypedExpr` imported
from `crates/bqlite-planner/src/expr.rs`, where TASK-225 has (or
will have) placed the real type. The two tasks are adjacent in
the same crate; if TASK-224 lands first, it introduces a stub
`TypedExpr` module that TASK-225 then replaces. If TASK-225 lands
first, TASK-224 uses the finished module directly. Either order
works because the external shape (`TypedExpr { kind, result_type,
nullable, span }`) is fixed by §4.1.

## 13. Open questions

Deferred to later waves, recorded here to keep them from getting
lost:

1. **Constant folding at compile time.** Wave 2 does not fold
   `2 + 3` into `5` at compile time — the planner pass 5
   (planner-pipeline.md §6.7) runs on `TypedExpr`, *before*
   compilation, and rewrites the `TypedExpr` tree in place. The
   `CompiledExpr` then sees the folded form. This split keeps
   the compiler a pure structural walk.
2. **Partial evaluation across batches.** A `Literal` that
   appears on both sides of a binary operator (e.g.
   `col * 1.1` across every row in 1M batches) currently
   re-materializes the Arrow scalar every batch. Wave 5 may
   cache per-operator `CompiledExpr` state to avoid this. The
   cache would be keyed on the `CompiledExpr`'s address, so the
   `Clone` impl's semantics matter; §5.1's current derivation
   is address-preserving for cloned subtrees (via `Arc`), which
   is forward-compatible with a cache.
3. **`Vec<CompiledExpr>` vs `CompiledPlan` bytecode.** A single
   flat bytecode stream with indexed operand references would
   outperform the current tree for deep expressions. Tree is
   fine for Wave 2's acceptance query (depth 3-4); the bytecode
   form is a Wave 5 rewrite when profiling indicates.
4. **Column index remapping through projection pruning.**
   §5.3's "name-based re-lookup" handles this correctly but
   costs a hash lookup per column per compile. Wave 5 may
   pre-compute an index mapping once per descriptor and pass it
   to `from_typed`; the shape shown here accommodates that
   without any change.
5. **`BOOLEAN_AND_THEN_COUNT` monomorphization.** The filter +
   count pattern in the acceptance test is an obvious fusion
   target but cannot land until Wave 3 introduces aggregates.
   Noted for TASK-307 / TASK-503.

---

The shape is small on purpose: two types (`TypedExpr`,
`CompiledExpr`), two lowering functions, one kernel selector, one
fast path, one pushdown pattern-match. Wave 2 ships exactly this;
every extension point (more kernels, bytecode, caching, broader
pushdown shapes) is additive from here.
