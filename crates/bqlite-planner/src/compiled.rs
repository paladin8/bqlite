//! Compiled expressions — the runtime-evaluable form carried by
//! physical-plan descriptors per expression-compilation.md §5.
//!
//! # Two-stage pipeline
//!
//! ```text
//! bqlite_ast::Expr  ──type-check──▶  TypedExpr  ──compile──▶  CompiledExpr
//!                                    (crate::expr)        (this module)
//! ```
//!
//! [`TypedExpr`] is the plan-tree form carried by
//! [`crate::logical::LogicalPlan`] nodes; [`CompiledExpr`] is the
//! runtime form carried by physical-plan descriptors. TASK-226's
//! physical lowering invokes [`CompiledExpr::from_typed`] for every
//! expression field; TASK-231's filter / project / limit operators
//! consume the resulting tree and dispatch to Arrow kernels.
//!
//! # What this checkpoint ships
//!
//! - [`CompiledExpr`] + [`CompiledNode`] — the runtime tree shape.
//!   Mirrors `TypedExprKind` one-for-one with pre-resolved column
//!   indices and per-node kernel tags.
//! - `*Kernel` enums ([`ArithKernel`], [`CompareKernel`], [`LogicalKernel`],
//!   [`UnaryKernel`], [`FunctionKernel`], [`CastKernel`], [`InSetKernel`])
//!   naming the runtime evaluator. Wave 2 ships only the Arrow
//!   compute dispatch path; the `LogicalKernel::FusedEqAnd`
//!   monomorphized fast path is a follow-up.
//! - [`ArrowKernelId`] and [`FunctionId`] — opaque identifiers the
//!   runtime resolves via a lookup table. Keeping them as enums
//!   rather than function pointers means `CompiledExpr` stays
//!   `Clone + Debug` without any `unsafe`.
//! - [`CompiledExpr::from_typed`] — the `TypedExpr → CompiledExpr`
//!   walker. Structurally preserves the tree, inserts kernel
//!   selections based on the node's typed operand types, and
//!   forwards `column_index` verbatim (TASK-226 / TASK-228's
//!   projection pruning may later remap indices against a pruned
//!   runtime schema, but that lives above this module).
//!
//! # What this checkpoint does NOT ship
//!
//! - **Runtime kernel evaluation.** Calling into
//!   `arrow::compute::kernels::numeric::add` etc. happens in the
//!   `bqlite-operators` crate (TASK-231). This module holds the
//!   kernel ID; the operator crate holds the dispatch table that
//!   maps ID → function pointer.
//! - **Pushdown surface (`to_scan_conjunct`).** Lands alongside
//!   TASK-227's predicate pushdown pass, which is the only
//!   consumer.
//! - **Monomorphized fast paths.** The design doc's sole Wave 2
//!   fast path (`LogicalKernel::FusedEqAnd`) is deferred — every
//!   `LogicalKernel` in this checkpoint is `ArrowKleene`.
//! - **Projection remapping.** The design doc §5.3 says TASK-226
//!   remaps `column_index` during physical lowering so the runtime
//!   index reflects the pruned column set. This checkpoint's
//!   `from_typed` preserves the typed index verbatim; the remap
//!   lands when TASK-226 uses this module.

use std::sync::Arc;

// Re-export these operator enums from `bqlite_planner::compiled` so
// downstream crates (notably `bqlite-operators`, which is forbidden
// from taking a runtime dependency on `bqlite-ast`) can match on the
// variants stored inside [`CompiledNode::Arith`], [`CompiledNode::Compare`],
// and [`CompiledNode::Unary`] without adding a new dep. These are pure
// path re-exports — they do not introduce any behavior.
pub use bqlite_ast::expr::{BinaryOp, CompareOp, UnaryOp};
use bqlite_core::{BqlType, PropertyValue};

use crate::expr::{ScalarFunctionSig, TypedExpr, TypedExprKind};

// ─────────────────────────────────────────────────────────────────────────────
// CompiledExpr
// ─────────────────────────────────────────────────────────────────────────────

/// A runtime-evaluable expression tree carried by physical plan
/// descriptors (`FusedSegmentStep::Filter`,
/// `FusedSegmentStep::Project`, `ScanPhysical::scan_predicates`, …).
///
/// Constructed exclusively from a [`TypedExpr`] via
/// [`CompiledExpr::from_typed`]. Holding a `CompiledExpr` is proof
/// that type-checking already succeeded (by construction through
/// `TypedExpr`) and that each node carries a resolvable kernel
/// identifier.
#[derive(Debug, Clone, PartialEq)]
pub struct CompiledExpr {
    pub node: CompiledNode,
    /// Cached result type, identical to the originating
    /// `TypedExpr::result_type`. Carried here so runtime operators
    /// can ask for it without re-walking the tree.
    pub result_type: BqlType,
    /// Whether this node may emit NULL at runtime — carried over
    /// from the typed form's conservative over-approximation.
    pub nullable: bool,
}

/// Variant payload of a [`CompiledExpr`]. Mirrors
/// [`TypedExprKind`] one-for-one with pre-resolved runtime
/// information (column indices, kernel tags).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum CompiledNode {
    /// A broadcast literal. The runtime evaluator materializes this
    /// as an Arrow scalar array at call time and relies on Arrow
    /// kernel broadcasting.
    Literal(PropertyValue),

    /// A column read. `index` is the full table-schema ordinal of the
    /// column (0-based position in `TableSchema::columns()`). The scan
    /// and storage reader always emit columns in **table-schema order**
    /// so this ordinal maps directly to the batch column position,
    /// provided no column with a lower ordinal has been pruned away
    /// (which would create a gap). The pruning pass (TASK-228) avoids
    /// gaps for the expressions it tracks by including every column
    /// referenced by any predicate or projection expression.
    /// `name` is kept alongside for pushdown / diagnostics.
    ///
    /// **Limitation**: index remapping for pruned batches is not yet
    /// implemented (deferred per TASK-228 doc comment §5.3). A future
    /// pass will rewrite indices to batch-position ordinals at bind
    /// time, eliminating the gap constraint entirely.
    Column { index: usize, name: String },

    /// Arithmetic binary op.
    Arith {
        op: BinaryOp,
        left: Box<CompiledExpr>,
        right: Box<CompiledExpr>,
        kernel: ArithKernel,
    },

    /// Comparison binary op — result is always Bool.
    Compare {
        op: CompareOp,
        left: Box<CompiledExpr>,
        right: Box<CompiledExpr>,
        kernel: CompareKernel,
    },

    /// Unary arithmetic op.
    Unary {
        op: UnaryOp,
        operand: Box<CompiledExpr>,
        kernel: UnaryKernel,
    },

    /// Variadic logical AND. Wave 2 always uses
    /// [`LogicalKernel::ArrowKleene`]; the `FusedEqAnd`
    /// monomorphized fast path is a follow-up.
    And {
        operands: Vec<CompiledExpr>,
        kernel: LogicalKernel,
    },

    /// Variadic logical OR.
    Or {
        operands: Vec<CompiledExpr>,
        kernel: LogicalKernel,
    },

    /// Logical NOT — dispatches to Arrow's `not` kernel.
    Not(Box<CompiledExpr>),

    /// `input IS NULL` (or `IS NOT NULL` when `negated = true`).
    IsNull {
        input: Box<CompiledExpr>,
        negated: bool,
    },

    /// Scalar function call. The signature is preserved from the
    /// typed form; the kernel tag names the runtime dispatcher.
    FunctionCall {
        signature: Arc<ScalarFunctionSig>,
        args: Vec<CompiledExpr>,
        kernel: FunctionKernel,
    },

    /// Explicit `CAST(expr AS type)`.
    Cast {
        input: Box<CompiledExpr>,
        target_type: BqlType,
        kernel: CastKernel,
    },

    /// Implicit coercion inserted by the type checker (Int → Float).
    /// Lowered to the same Arrow cast kernel as `Cast` but kept
    /// structurally distinct so optimizer rewrites can elide
    /// implicit coercions while preserving explicit ones.
    ImplicitCoerce {
        input: Box<CompiledExpr>,
        target_type: BqlType,
        kernel: CastKernel,
    },

    /// `input IN (lit_1, ..., lit_k)` — literal set membership.
    InLiteralSet {
        input: Box<CompiledExpr>,
        values: Vec<PropertyValue>,
        negated: bool,
        kernel: InSetKernel,
    },
}

// ─────────────────────────────────────────────────────────────────────────────
// Kernel enums (per §5.2)
// ─────────────────────────────────────────────────────────────────────────────

/// Kernel selection for an arithmetic binary operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ArithKernel {
    /// Dispatch to an Arrow compute kernel (e.g.
    /// `arrow::compute::kernels::numeric::add`). Covers every
    /// `(Int|Float, Int|Float)`, `Timestamp±Int`, and
    /// `Timestamp-Timestamp` combination Wave 2 produces.
    ArrowKernel(ArrowKernelId),
}

/// Kernel selection for a comparison binary operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum CompareKernel {
    ArrowKernel(ArrowKernelId),
}

/// Kernel selection for a unary arithmetic operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum UnaryKernel {
    ArrowKernel(ArrowKernelId),
}

/// Kernel selection for variadic logical operators (`AND`, `OR`).
///
/// Wave 2 ships only the generic Arrow Kleene path; the
/// `FusedEqAnd` monomorphized fast path from expression-
/// compilation.md §6 is a follow-up checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum LogicalKernel {
    /// Fold `arrow::compute::and_kleene` / `or_kleene` across the
    /// operand list.
    ArrowKleene,
}

/// Kernel selection for a scalar function call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum FunctionKernel {
    /// Resolve via the runtime scalar-function registry using the
    /// opaque [`FunctionId`]. The registry mapping lives in
    /// `bqlite-operators`.
    Registered(FunctionId),
}

/// Kernel selection for an explicit or implicit cast.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum CastKernel {
    ArrowKernel(ArrowKernelId),
}

/// Kernel selection for `IN (literal set)`. Wave 2 ships only the
/// generic Arrow `is_in` dispatch; the `DictionaryCodes` fast path
/// (§5.2) is a follow-up.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum InSetKernel {
    /// Generic Arrow `is_in` dispatch — builds the Arrow set kernel
    /// once per batch.
    ArrowIsIn,
}

// ─────────────────────────────────────────────────────────────────────────────
// Opaque kernel identifiers
// ─────────────────────────────────────────────────────────────────────────────

/// Opaque identifier for a built-in Arrow kernel the runtime
/// dispatches to.
///
/// The variant names match the Arrow compute kernels they resolve
/// to at dispatch time. Runtime operators (TASK-231) own the
/// mapping to actual function pointers; this module only names
/// *which* kernel a given `(op, type)` pair picks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ArrowKernelId {
    // Arithmetic — numeric
    AddInt,
    AddFloat,
    SubInt,
    SubFloat,
    MulInt,
    MulFloat,
    DivInt,
    DivFloat,
    ModInt,
    ModFloat,

    // Arithmetic — timestamp
    SubTimestamp,    // ts - ts → Int
    AddTimestampInt, // ts + Int → ts
    SubTimestampInt, // ts - Int → ts
    AddIntTimestamp, // Int + ts → ts

    // Unary
    NegateInt,
    NegateFloat,
    PlusInt,
    PlusFloat,

    // Comparison
    EqInt,
    EqFloat,
    EqString,
    EqBool,
    EqTimestamp,
    NeInt,
    NeFloat,
    NeString,
    NeBool,
    NeTimestamp,
    LtInt,
    LtFloat,
    LtString,
    LtTimestamp,
    LeInt,
    LeFloat,
    LeString,
    LeTimestamp,
    GtInt,
    GtFloat,
    GtString,
    GtTimestamp,
    GeInt,
    GeFloat,
    GeString,
    GeTimestamp,

    // Cast
    CastIntToFloat,
    CastFloatToInt,
    CastIntToString,
    CastFloatToString,
    CastStringToInt,
    CastStringToFloat,
    CastStringToTimestamp,
    CastTimestampToInt,
    CastIntToTimestamp,
    CastBoolToInt,
    CastIntToBool,
    /// No-op cast — `CAST(x AS T)` where `x` already has type
    /// `T`. The type checker permits identity casts (they carry
    /// the "explicit cast" flag the optimizer must not elide);
    /// this identifier lets the runtime resolve them without a
    /// wrong-kernel hazard.
    CastIdentity,
}

/// Opaque identifier for a registered scalar function.
///
/// The actual index into the runtime function table lives in
/// `bqlite-operators`. This module carries a newtype so the
/// `CompiledExpr` tree is `Clone + Debug` and so compile-time
/// selection can store identifiers without reaching into operator
/// internals.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FunctionId(pub u32);

// ─────────────────────────────────────────────────────────────────────────────
// TypedExpr → CompiledExpr lowering
// ─────────────────────────────────────────────────────────────────────────────

impl CompiledExpr {
    /// Lower a [`TypedExpr`] into a runtime-evaluable
    /// [`CompiledExpr`] tree.
    ///
    /// The walk preserves the structure of the typed form, picks a
    /// kernel per node via the per-op selectors in §5.2, and
    /// forwards column indices verbatim. TASK-226 wraps this call
    /// with a projection-remap pass when the physical plan prunes
    /// columns via TASK-228; Wave 2 callers who have not yet pruned
    /// can feed the typed form directly.
    pub fn from_typed(typed: &TypedExpr) -> Self {
        let result_type = typed.result_type.clone();
        let nullable = typed.nullable;
        let node = match &typed.kind {
            TypedExprKind::Literal(value) => CompiledNode::Literal(value.clone()),
            TypedExprKind::Column { column_index, name } => CompiledNode::Column {
                index: *column_index,
                name: name.clone(),
            },
            TypedExprKind::Arith { op, left, right } => {
                let left = Box::new(Self::from_typed(left));
                let right = Box::new(Self::from_typed(right));
                let kernel = select_arith_kernel(*op, &left.result_type, &right.result_type);
                CompiledNode::Arith {
                    op: *op,
                    left,
                    right,
                    kernel,
                }
            }
            TypedExprKind::Compare { op, left, right } => {
                let left = Box::new(Self::from_typed(left));
                let right = Box::new(Self::from_typed(right));
                // Post type-check both sides share the same
                // coerced type (via ImplicitCoerce wrappers and the
                // literal→column coercion in §4.3), so we select
                // the kernel off the left side.
                let kernel = select_compare_kernel(*op, &left.result_type);
                CompiledNode::Compare {
                    op: *op,
                    left,
                    right,
                    kernel,
                }
            }
            TypedExprKind::Unary { op, operand } => {
                let operand = Box::new(Self::from_typed(operand));
                let kernel = select_unary_kernel(*op, &operand.result_type);
                CompiledNode::Unary {
                    op: *op,
                    operand,
                    kernel,
                }
            }
            TypedExprKind::And(operands) => CompiledNode::And {
                operands: operands.iter().map(Self::from_typed).collect(),
                kernel: LogicalKernel::ArrowKleene,
            },
            TypedExprKind::Or(operands) => CompiledNode::Or {
                operands: operands.iter().map(Self::from_typed).collect(),
                kernel: LogicalKernel::ArrowKleene,
            },
            TypedExprKind::Not(inner) => CompiledNode::Not(Box::new(Self::from_typed(inner))),
            TypedExprKind::IsNull { input, negated } => CompiledNode::IsNull {
                input: Box::new(Self::from_typed(input)),
                negated: *negated,
            },
            TypedExprKind::FunctionCall { signature, args } => CompiledNode::FunctionCall {
                signature: Arc::clone(signature),
                args: args.iter().map(Self::from_typed).collect(),
                // Wave 2 assigns a placeholder FunctionId(0); the
                // runtime registry resolves it via the function's
                // (name, params) tuple. A dedicated ID-allocation
                // pass lands when TASK-231 wires the operator-side
                // dispatch table.
                kernel: FunctionKernel::Registered(FunctionId(0)),
            },
            TypedExprKind::Cast { input, target_type } => {
                let input = Box::new(Self::from_typed(input));
                let kernel = select_cast_kernel(&input.result_type, target_type);
                CompiledNode::Cast {
                    input,
                    target_type: target_type.clone(),
                    kernel,
                }
            }
            TypedExprKind::ImplicitCoerce { input, target_type } => {
                let input = Box::new(Self::from_typed(input));
                let kernel = select_cast_kernel(&input.result_type, target_type);
                CompiledNode::ImplicitCoerce {
                    input,
                    target_type: target_type.clone(),
                    kernel,
                }
            }
            TypedExprKind::InLiteralSet {
                input,
                values,
                negated,
            } => CompiledNode::InLiteralSet {
                input: Box::new(Self::from_typed(input)),
                values: values.clone(),
                negated: *negated,
                kernel: InSetKernel::ArrowIsIn,
            },
        };
        CompiledExpr {
            node,
            result_type,
            nullable,
        }
    }

    /// Whether this compiled expression matches a shape the
    /// storage layer can evaluate directly — the "can you push
    /// this into the scan?" query that TASK-227's predicate
    /// pushdown pass asks per expression-compilation.md §8.
    ///
    /// Wave 2 pushable shapes, per the §8 table:
    ///
    /// - `Compare { col, op, lit }` / `Compare { lit, op, col }`
    ///   for any comparison op (`=`, `!=`, `<`, `<=`, `>`, `>=`).
    /// - `IsNull { Column, .. }` with either `negated` value.
    /// - `InLiteralSet { Column, negated: false, .. }`. `NOT IN`
    ///   is not pushable because storage would need to materialize
    ///   a row mask and the §8 design explicitly defers it.
    ///
    /// Everything else — including `And`/`Or`/`Not` at the top,
    /// arithmetic sub-expressions, column-vs-column comparisons,
    /// and function calls — returns `false`. TASK-227's caller is
    /// responsible for decomposing a variadic `And` into conjuncts
    /// and asking each conjunct individually.
    ///
    /// This method is pure structural pattern matching — it does
    /// not consult storage capabilities, which is TASK-202's
    /// runtime-side concern. The scan operator's
    /// `Predicate::accepts_zone` / `Predicate::filter_batch`
    /// implementations decide what to actually do with a pushed
    /// conjunct; this method decides *whether* to hand one over
    /// in the first place.
    pub fn supported_pushdown_shape(&self) -> bool {
        match &self.node {
            CompiledNode::Compare { left, right, .. } => {
                is_column(left) && is_literal(right) || is_literal(left) && is_column(right)
            }
            CompiledNode::IsNull { input, .. } => is_column(input),
            CompiledNode::InLiteralSet {
                input,
                negated: false,
                ..
            } => is_column(input),
            _ => false,
        }
    }
}

/// True if a compiled node is a direct column reference.
fn is_column(expr: &CompiledExpr) -> bool {
    matches!(expr.node, CompiledNode::Column { .. })
}

/// True if a compiled node is a direct literal value.
fn is_literal(expr: &CompiledExpr) -> bool {
    matches!(expr.node, CompiledNode::Literal(_))
}

// ─────────────────────────────────────────────────────────────────────────────
// Kernel selection helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Pick an [`ArithKernel`] for a binary arithmetic op against its
/// operand types.
///
/// Post type-check, the operand types are already coerced to a
/// compatible pair via [`TypedExprKind::ImplicitCoerce`]; this
/// selector just pattern-matches the final shape.
fn select_arith_kernel(op: BinaryOp, left: &BqlType, right: &BqlType) -> ArithKernel {
    use BinaryOp::*;
    use BqlType::*;
    let id = match (op, left, right) {
        // Int arithmetic
        (Add, Int, Int) => ArrowKernelId::AddInt,
        (Subtract, Int, Int) => ArrowKernelId::SubInt,
        (Multiply, Int, Int) => ArrowKernelId::MulInt,
        (Divide, Int, Int) => ArrowKernelId::DivInt,
        (Modulo, Int, Int) => ArrowKernelId::ModInt,

        // Float arithmetic
        (Add, Float, Float) => ArrowKernelId::AddFloat,
        (Subtract, Float, Float) => ArrowKernelId::SubFloat,
        (Multiply, Float, Float) => ArrowKernelId::MulFloat,
        (Divide, Float, Float) => ArrowKernelId::DivFloat,
        (Modulo, Float, Float) => ArrowKernelId::ModFloat,

        // Timestamp arithmetic
        (Subtract, Timestamp, Timestamp) => ArrowKernelId::SubTimestamp,
        (Add, Timestamp, Int) => ArrowKernelId::AddTimestampInt,
        (Subtract, Timestamp, Int) => ArrowKernelId::SubTimestampInt,
        (Add, Int, Timestamp) => ArrowKernelId::AddIntTimestamp,

        // Any other combination is a type-check bug — the type
        // checker should have either promoted the operands or
        // rejected the expression. We panic with an internal-error
        // message rather than guessing a kernel because silently
        // picking the wrong one would produce wrong data at runtime.
        (op, l, r) => unreachable!(
            "select_arith_kernel reached an unhandled combination \
             after type-check: op={op:?}, left={l}, right={r}"
        ),
    };
    ArithKernel::ArrowKernel(id)
}

/// Pick a [`CompareKernel`] for a comparison op. Both sides share
/// the operand type post type-check; the `operand_type` parameter
/// is the coerced comparison type.
fn select_compare_kernel(op: CompareOp, operand_type: &BqlType) -> CompareKernel {
    use BqlType::*;
    use CompareOp::*;
    let id = match (op, operand_type) {
        (Equal, Int) => ArrowKernelId::EqInt,
        (Equal, Float) => ArrowKernelId::EqFloat,
        (Equal, String) => ArrowKernelId::EqString,
        (Equal, Bool) => ArrowKernelId::EqBool,
        (Equal, Timestamp) => ArrowKernelId::EqTimestamp,
        (NotEqual, Int) => ArrowKernelId::NeInt,
        (NotEqual, Float) => ArrowKernelId::NeFloat,
        (NotEqual, String) => ArrowKernelId::NeString,
        (NotEqual, Bool) => ArrowKernelId::NeBool,
        (NotEqual, Timestamp) => ArrowKernelId::NeTimestamp,
        (Less, Int) => ArrowKernelId::LtInt,
        (Less, Float) => ArrowKernelId::LtFloat,
        (Less, String) => ArrowKernelId::LtString,
        (Less, Timestamp) => ArrowKernelId::LtTimestamp,
        (LessOrEqual, Int) => ArrowKernelId::LeInt,
        (LessOrEqual, Float) => ArrowKernelId::LeFloat,
        (LessOrEqual, String) => ArrowKernelId::LeString,
        (LessOrEqual, Timestamp) => ArrowKernelId::LeTimestamp,
        (Greater, Int) => ArrowKernelId::GtInt,
        (Greater, Float) => ArrowKernelId::GtFloat,
        (Greater, String) => ArrowKernelId::GtString,
        (Greater, Timestamp) => ArrowKernelId::GtTimestamp,
        (GreaterOrEqual, Int) => ArrowKernelId::GeInt,
        (GreaterOrEqual, Float) => ArrowKernelId::GeFloat,
        (GreaterOrEqual, String) => ArrowKernelId::GeString,
        (GreaterOrEqual, Timestamp) => ArrowKernelId::GeTimestamp,
        (op, t) => unreachable!(
            "select_compare_kernel reached an unhandled combination \
             after type-check: op={op:?}, operand_type={t}"
        ),
    };
    CompareKernel::ArrowKernel(id)
}

/// Pick a [`UnaryKernel`] for a unary op against its operand type.
fn select_unary_kernel(op: UnaryOp, operand_type: &BqlType) -> UnaryKernel {
    use BqlType::*;
    use UnaryOp::*;
    let id = match (op, operand_type) {
        (Negate, Int) => ArrowKernelId::NegateInt,
        (Negate, Float) => ArrowKernelId::NegateFloat,
        (Plus, Int) => ArrowKernelId::PlusInt,
        (Plus, Float) => ArrowKernelId::PlusFloat,
        (op, t) => unreachable!(
            "select_unary_kernel reached an unhandled combination \
             after type-check: op={op:?}, operand_type={t}"
        ),
    };
    UnaryKernel::ArrowKernel(id)
}

/// Pick a [`CastKernel`] for a `source → target` type pair.
///
/// Identity casts (`source == target`) are the common post-type-
/// check case for an explicit `CAST(x AS T)` where `x: T` — the
/// type checker does not elide them because the optimizer must
/// preserve explicit casts (§4.3). They resolve to
/// [`ArrowKernelId::CastIdentity`], which the runtime dispatcher
/// treats as a no-op passthrough.
fn select_cast_kernel(source: &BqlType, target: &BqlType) -> CastKernel {
    use BqlType::*;
    if source == target {
        return CastKernel::ArrowKernel(ArrowKernelId::CastIdentity);
    }
    let id = match (source, target) {
        (Int, Float) => ArrowKernelId::CastIntToFloat,
        (Float, Int) => ArrowKernelId::CastFloatToInt,
        (Int, BqlType::String) => ArrowKernelId::CastIntToString,
        (Float, BqlType::String) => ArrowKernelId::CastFloatToString,
        (BqlType::String, Int) => ArrowKernelId::CastStringToInt,
        (BqlType::String, Float) => ArrowKernelId::CastStringToFloat,
        (BqlType::String, Timestamp) => ArrowKernelId::CastStringToTimestamp,
        (Timestamp, Int) => ArrowKernelId::CastTimestampToInt,
        (Int, Timestamp) => ArrowKernelId::CastIntToTimestamp,
        (Bool, Int) => ArrowKernelId::CastBoolToInt,
        (Int, Bool) => ArrowKernelId::CastIntToBool,
        // Every other cast pair is a cast the runtime does not
        // yet implement. Returning `CastIdentity` here would
        // silently produce wrong data; instead we flag it as
        // `Unsupported` by returning a kernel the runtime
        // dispatcher will surface as a typed error. For Wave 2
        // this path is unreachable because the type checker
        // only produces casts the table above handles — if it
        // fires, the planner or type checker has grown a new
        // cast surface that needs a kernel row added here.
        (s, t) => unreachable!(
            "select_cast_kernel reached an unhandled type pair \
             after type-check: source={s}, target={t} — \
             add a kernel row or reject at type-check time"
        ),
    };
    CastKernel::ArrowKernel(id)
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::FunctionRegistry;
    use bqlite_ast::expr::{Expr, Literal, Spanned};
    use bqlite_ast::span::{Name, Span};
    use bqlite_core::{ColumnDef, OperatorSchema};

    fn schema() -> OperatorSchema {
        OperatorSchema::new(vec![
            ColumnDef::required("user_id", BqlType::String),
            ColumnDef::required("ts", BqlType::Timestamp),
            ColumnDef::nullable("amount", BqlType::Float),
            ColumnDef::nullable("count", BqlType::Int),
        ])
        .unwrap()
    }

    fn spanned<T>(t: T) -> Spanned<T> {
        Spanned::new(t, Span::EMPTY)
    }

    fn col(name: &str) -> Spanned<Expr> {
        spanned(Expr::Column(Name::synthetic(name)))
    }

    fn int_lit(i: i64) -> Spanned<Expr> {
        spanned(Expr::Literal(Literal::Int(i)))
    }

    fn float_lit(f: f64) -> Spanned<Expr> {
        spanned(Expr::Literal(Literal::Float(f)))
    }

    fn type_check(expr: Spanned<Expr>) -> TypedExpr {
        let schema = schema();
        let reg = FunctionRegistry::with_builtins();
        TypedExpr::from_ast(&expr, &schema, &reg).unwrap()
    }

    // ── Literal / Column forwarding ─────────────────────────────────────────

    #[test]
    fn literal_round_trips_as_compiled_literal() {
        let t = type_check(int_lit(42));
        let c = CompiledExpr::from_typed(&t);
        assert_eq!(c.result_type, BqlType::Int);
        assert!(!c.nullable);
        match c.node {
            CompiledNode::Literal(PropertyValue::Int(42)) => (),
            other => panic!("expected Literal(Int(42)), got {other:?}"),
        }
    }

    #[test]
    fn column_reference_preserves_index_and_name() {
        let t = type_check(col("amount"));
        let c = CompiledExpr::from_typed(&t);
        match c.node {
            CompiledNode::Column { index, name } => {
                assert_eq!(index, 2); // amount is the third column
                assert_eq!(name, "amount");
            }
            other => panic!("expected Column, got {other:?}"),
        }
        assert_eq!(c.result_type, BqlType::Float);
        assert!(c.nullable);
    }

    // ── Arithmetic kernel selection ─────────────────────────────────────────

    #[test]
    fn int_add_int_picks_add_int_kernel() {
        let t = type_check(spanned(Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(int_lit(1)),
            right: Box::new(int_lit(2)),
        }));
        let c = CompiledExpr::from_typed(&t);
        match c.node {
            CompiledNode::Arith {
                kernel: ArithKernel::ArrowKernel(ArrowKernelId::AddInt),
                ..
            } => (),
            other => panic!("expected AddInt kernel, got {other:?}"),
        }
    }

    #[test]
    fn int_plus_float_promotes_then_picks_add_float_kernel() {
        let t = type_check(spanned(Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(int_lit(1)),
            right: Box::new(float_lit(2.5)),
        }));
        let c = CompiledExpr::from_typed(&t);
        match c.node {
            CompiledNode::Arith {
                kernel: ArithKernel::ArrowKernel(ArrowKernelId::AddFloat),
                left,
                ..
            } => {
                // The int operand was wrapped in ImplicitCoerce
                // during type-check; verify the compiled form
                // preserves that wrapper.
                assert!(matches!(left.node, CompiledNode::ImplicitCoerce { .. }));
            }
            other => panic!("expected AddFloat with coerced lhs, got {other:?}"),
        }
    }

    #[test]
    fn timestamp_minus_timestamp_picks_sub_timestamp_kernel() {
        let t = type_check(spanned(Expr::Binary {
            op: BinaryOp::Subtract,
            left: Box::new(col("ts")),
            right: Box::new(col("ts")),
        }));
        let c = CompiledExpr::from_typed(&t);
        assert_eq!(c.result_type, BqlType::Int);
        match c.node {
            CompiledNode::Arith {
                kernel: ArithKernel::ArrowKernel(ArrowKernelId::SubTimestamp),
                ..
            } => (),
            other => panic!("expected SubTimestamp kernel, got {other:?}"),
        }
    }

    // ── Comparison kernel selection ────────────────────────────────────────

    #[test]
    fn string_equal_picks_eq_string_kernel() {
        let t = type_check(spanned(Expr::Compare {
            op: CompareOp::Equal,
            left: Box::new(col("user_id")),
            right: Box::new(spanned(Expr::Literal(Literal::String("u1".into())))),
        }));
        let c = CompiledExpr::from_typed(&t);
        match c.node {
            CompiledNode::Compare {
                kernel: CompareKernel::ArrowKernel(ArrowKernelId::EqString),
                ..
            } => (),
            other => panic!("expected EqString, got {other:?}"),
        }
    }

    #[test]
    fn float_greater_picks_gt_float_kernel() {
        // `amount > 100` — the literal is coerced to Float so both
        // sides share Float, and the compare kernel is GtFloat.
        let t = type_check(spanned(Expr::Compare {
            op: CompareOp::Greater,
            left: Box::new(col("amount")),
            right: Box::new(int_lit(100)),
        }));
        let c = CompiledExpr::from_typed(&t);
        match c.node {
            CompiledNode::Compare {
                kernel: CompareKernel::ArrowKernel(ArrowKernelId::GtFloat),
                ..
            } => (),
            other => panic!("expected GtFloat, got {other:?}"),
        }
    }

    // ── Logical AND / OR ───────────────────────────────────────────────────

    #[test]
    fn and_picks_arrow_kleene_kernel() {
        let t = type_check(spanned(Expr::And(vec![
            spanned(Expr::Compare {
                op: CompareOp::Equal,
                left: Box::new(col("user_id")),
                right: Box::new(spanned(Expr::Literal(Literal::String("u1".into())))),
            }),
            spanned(Expr::Compare {
                op: CompareOp::Greater,
                left: Box::new(col("amount")),
                right: Box::new(float_lit(10.0)),
            }),
        ])));
        let c = CompiledExpr::from_typed(&t);
        match c.node {
            CompiledNode::And {
                kernel: LogicalKernel::ArrowKleene,
                operands,
            } => {
                assert_eq!(operands.len(), 2);
            }
            other => panic!("expected And, got {other:?}"),
        }
    }

    // ── NOT and IS NULL ─────────────────────────────────────────────────────

    #[test]
    fn not_wraps_input() {
        let t = type_check(spanned(Expr::Not(Box::new(spanned(Expr::Literal(
            Literal::Bool(true),
        ))))));
        let c = CompiledExpr::from_typed(&t);
        assert!(matches!(c.node, CompiledNode::Not(_)));
    }

    #[test]
    fn is_null_is_non_nullable_and_carries_negated_flag() {
        let t = type_check(spanned(Expr::IsNull {
            expr: Box::new(col("amount")),
            negated: true,
        }));
        let c = CompiledExpr::from_typed(&t);
        assert_eq!(c.result_type, BqlType::Bool);
        assert!(!c.nullable);
        match c.node {
            CompiledNode::IsNull { negated, .. } => assert!(negated),
            other => panic!("expected IsNull, got {other:?}"),
        }
    }

    // ── Cast / ImplicitCoerce ───────────────────────────────────────────────

    #[test]
    fn implicit_coerce_wraps_int_as_cast_to_float() {
        // Build through the arithmetic promotion path.
        let t = type_check(spanned(Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(col("count")), // Int
            right: Box::new(float_lit(1.0)),
        }));
        let c = CompiledExpr::from_typed(&t);
        match c.node {
            CompiledNode::Arith { left, .. } => match left.node {
                CompiledNode::ImplicitCoerce {
                    target_type: BqlType::Float,
                    kernel: CastKernel::ArrowKernel(ArrowKernelId::CastIntToFloat),
                    ..
                } => (),
                other => panic!("expected ImplicitCoerce→CastIntToFloat, got {other:?}"),
            },
            other => panic!("expected Arith, got {other:?}"),
        }
    }

    #[test]
    fn explicit_cast_preserves_target_type_and_picks_kernel() {
        let t = type_check(spanned(Expr::Cast {
            expr: Box::new(col("count")),
            target: BqlType::Float,
        }));
        let c = CompiledExpr::from_typed(&t);
        match c.node {
            CompiledNode::Cast {
                target_type: BqlType::Float,
                kernel: CastKernel::ArrowKernel(ArrowKernelId::CastIntToFloat),
                ..
            } => (),
            other => panic!("expected Cast→CastIntToFloat, got {other:?}"),
        }
    }

    #[test]
    fn identity_cast_resolves_to_cast_identity_kernel() {
        // `CAST(count AS Int)` where count is already Int. The
        // type checker preserves the explicit cast; the kernel
        // selector resolves it to `CastIdentity` (runtime no-op).
        let t = type_check(spanned(Expr::Cast {
            expr: Box::new(col("count")),
            target: BqlType::Int,
        }));
        let c = CompiledExpr::from_typed(&t);
        match c.node {
            CompiledNode::Cast {
                target_type: BqlType::Int,
                kernel: CastKernel::ArrowKernel(ArrowKernelId::CastIdentity),
                ..
            } => (),
            other => panic!("expected Cast→CastIdentity, got {other:?}"),
        }
    }

    #[test]
    fn nested_and_preserves_structure_recursively() {
        // `(user_id = 'u1' AND count > 0) AND amount > 100`
        let inner = spanned(Expr::And(vec![
            spanned(Expr::Compare {
                op: CompareOp::Equal,
                left: Box::new(col("user_id")),
                right: Box::new(spanned(Expr::Literal(Literal::String("u1".into())))),
            }),
            spanned(Expr::Compare {
                op: CompareOp::Greater,
                left: Box::new(col("count")),
                right: Box::new(int_lit(0)),
            }),
        ]));
        let outer = spanned(Expr::And(vec![
            inner,
            spanned(Expr::Compare {
                op: CompareOp::Greater,
                left: Box::new(col("amount")),
                right: Box::new(float_lit(100.0)),
            }),
        ]));
        let t = type_check(outer);
        let c = CompiledExpr::from_typed(&t);
        match c.node {
            CompiledNode::And { operands, .. } => {
                assert_eq!(operands.len(), 2);
                // First operand is itself an And, proving the
                // recursive walk preserved the nesting.
                assert!(matches!(operands[0].node, CompiledNode::And { .. }));
                // Second operand is the amount > 100 compare.
                assert!(matches!(operands[1].node, CompiledNode::Compare { .. }));
            }
            other => panic!("expected outer And, got {other:?}"),
        }
    }

    #[test]
    fn cast_inside_arith_subtree_is_lowered_correctly() {
        // `count + CAST(amount AS Int)` — the Cast sits inside
        // the Arith subtree and must lower through the walker.
        let t = type_check(spanned(Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(col("count")),
            right: Box::new(spanned(Expr::Cast {
                expr: Box::new(col("amount")),
                target: BqlType::Int,
            })),
        }));
        let c = CompiledExpr::from_typed(&t);
        match c.node {
            CompiledNode::Arith {
                kernel: ArithKernel::ArrowKernel(ArrowKernelId::AddInt),
                right,
                ..
            } => match right.node {
                CompiledNode::Cast {
                    target_type: BqlType::Int,
                    kernel: CastKernel::ArrowKernel(ArrowKernelId::CastFloatToInt),
                    ..
                } => (),
                other => panic!("expected inner Cast→CastFloatToInt, got {other:?}"),
            },
            other => panic!("expected Arith(AddInt), got {other:?}"),
        }
    }

    // ── IN literal set ──────────────────────────────────────────────────────

    #[test]
    fn in_literal_set_picks_arrow_is_in_kernel() {
        let t = type_check(spanned(Expr::In {
            lhs: vec![col("count")],
            rhs: bqlite_ast::InRhs::List(vec![int_lit(1), int_lit(2), int_lit(3)]),
            negated: false,
        }));
        let c = CompiledExpr::from_typed(&t);
        match c.node {
            CompiledNode::InLiteralSet {
                kernel: InSetKernel::ArrowIsIn,
                values,
                negated: false,
                ..
            } => {
                assert_eq!(values.len(), 3);
            }
            other => panic!("expected InLiteralSet, got {other:?}"),
        }
    }

    // ── Function call ───────────────────────────────────────────────────────

    #[test]
    fn function_call_carries_signature_and_placeholder_kernel() {
        // `like` lowers from Expr::Like to FunctionCall.
        let t = type_check(spanned(Expr::Like {
            expr: Box::new(col("user_id")),
            pattern: "u%".into(),
            negated: false,
        }));
        let c = CompiledExpr::from_typed(&t);
        match c.node {
            CompiledNode::FunctionCall {
                signature,
                kernel: FunctionKernel::Registered(_),
                args,
            } => {
                assert_eq!(signature.name, "like");
                assert_eq!(args.len(), 2);
            }
            other => panic!("expected FunctionCall, got {other:?}"),
        }
    }

    // ── Deep pipeline — the acceptance predicate ────────────────────────────

    // ── supported_pushdown_shape ────────────────────────────────────────────

    #[test]
    fn column_equals_literal_is_pushable() {
        // `user_id = 'u1'` — classic equality against a literal.
        let t = type_check(spanned(Expr::Compare {
            op: CompareOp::Equal,
            left: Box::new(col("user_id")),
            right: Box::new(spanned(Expr::Literal(Literal::String("u1".into())))),
        }));
        let c = CompiledExpr::from_typed(&t);
        assert!(c.supported_pushdown_shape());
    }

    #[test]
    fn literal_less_than_column_is_pushable() {
        // `100 < amount` — literal on the left, column on the
        // right. The pushdown query accepts either orientation so
        // the caller doesn't have to canonicalise first.
        let t = type_check(spanned(Expr::Compare {
            op: CompareOp::Less,
            left: Box::new(float_lit(100.0)),
            right: Box::new(col("amount")),
        }));
        let c = CompiledExpr::from_typed(&t);
        assert!(c.supported_pushdown_shape());
    }

    #[test]
    fn column_is_null_is_pushable() {
        let t = type_check(spanned(Expr::IsNull {
            expr: Box::new(col("amount")),
            negated: false,
        }));
        let c = CompiledExpr::from_typed(&t);
        assert!(c.supported_pushdown_shape());
    }

    #[test]
    fn column_is_not_null_is_pushable() {
        let t = type_check(spanned(Expr::IsNull {
            expr: Box::new(col("amount")),
            negated: true,
        }));
        let c = CompiledExpr::from_typed(&t);
        assert!(c.supported_pushdown_shape());
    }

    #[test]
    fn column_in_literal_set_is_pushable() {
        let t = type_check(spanned(Expr::In {
            lhs: vec![col("count")],
            rhs: bqlite_ast::InRhs::List(vec![int_lit(1), int_lit(2)]),
            negated: false,
        }));
        let c = CompiledExpr::from_typed(&t);
        assert!(c.supported_pushdown_shape());
    }

    #[test]
    fn column_not_in_literal_set_is_not_pushable() {
        // NOT IN requires materializing a row mask and is
        // deferred per expression-compilation.md §8.
        let t = type_check(spanned(Expr::In {
            lhs: vec![col("count")],
            rhs: bqlite_ast::InRhs::List(vec![int_lit(1), int_lit(2)]),
            negated: true,
        }));
        let c = CompiledExpr::from_typed(&t);
        assert!(!c.supported_pushdown_shape());
    }

    #[test]
    fn column_vs_column_compare_is_not_pushable() {
        // `ts = ts` — both sides are columns. Storage needs two
        // random column reads per row, which is not in scope for
        // the Wave 2 pushdown protocol.
        let t = type_check(spanned(Expr::Compare {
            op: CompareOp::Equal,
            left: Box::new(col("ts")),
            right: Box::new(col("ts")),
        }));
        let c = CompiledExpr::from_typed(&t);
        assert!(!c.supported_pushdown_shape());
    }

    #[test]
    fn and_of_comparisons_is_not_pushable_at_top_level() {
        // The pushdown pass (TASK-227) decomposes a top-level
        // `And` into its conjuncts and asks each one individually;
        // this method never returns true for an `And` itself.
        let t = type_check(spanned(Expr::And(vec![
            spanned(Expr::Compare {
                op: CompareOp::Equal,
                left: Box::new(col("user_id")),
                right: Box::new(spanned(Expr::Literal(Literal::String("u1".into())))),
            }),
            spanned(Expr::Compare {
                op: CompareOp::Greater,
                left: Box::new(col("amount")),
                right: Box::new(float_lit(100.0)),
            }),
        ])));
        let c = CompiledExpr::from_typed(&t);
        assert!(!c.supported_pushdown_shape());
    }

    #[test]
    fn literal_by_itself_is_not_pushable() {
        // A raw literal predicate like `WHERE true` is handled
        // above the scan; we do not try to push constants.
        let t = type_check(spanned(Expr::Literal(Literal::Bool(true))));
        let c = CompiledExpr::from_typed(&t);
        assert!(!c.supported_pushdown_shape());
    }

    #[test]
    fn function_call_like_is_not_pushable() {
        // LIKE / REGEX lower to function calls which Wave 2
        // evaluates above the scan.
        let t = type_check(spanned(Expr::Like {
            expr: Box::new(col("user_id")),
            pattern: "u%".into(),
            negated: false,
        }));
        let c = CompiledExpr::from_typed(&t);
        assert!(!c.supported_pushdown_shape());
    }

    #[test]
    fn full_acceptance_predicate_lowers_cleanly() {
        // `user_id = 'u1' AND amount > 100.0`
        let t = type_check(spanned(Expr::And(vec![
            spanned(Expr::Compare {
                op: CompareOp::Equal,
                left: Box::new(col("user_id")),
                right: Box::new(spanned(Expr::Literal(Literal::String("u1".into())))),
            }),
            spanned(Expr::Compare {
                op: CompareOp::Greater,
                left: Box::new(col("amount")),
                right: Box::new(float_lit(100.0)),
            }),
        ])));
        let c = CompiledExpr::from_typed(&t);
        assert_eq!(c.result_type, BqlType::Bool);
        match c.node {
            CompiledNode::And { operands, .. } => {
                assert_eq!(operands.len(), 2);
                assert_eq!(operands[0].result_type, BqlType::Bool);
                assert_eq!(operands[1].result_type, BqlType::Bool);
            }
            other => panic!("expected And, got {other:?}"),
        }
    }
}
