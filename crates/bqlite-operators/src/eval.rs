//! Runtime evaluator for [`CompiledExpr`] — the back half of the
//! type-check / compile / evaluate pipeline described in
//! `docs/design/planner/expression-compilation.md` §5.
//!
//! # Contract
//!
//! [`evaluate`] walks a [`CompiledExpr`] against a single
//! [`RecordBatch`] and returns an [`ArrayRef`] of length
//! `batch.num_rows()`. The returned array's Arrow type matches the
//! compile-time [`CompiledExpr::result_type`], modulo the Wave-2
//! materialization contract from execution-model.md §3.7 (string
//! results are always `StringViewArray`).
//!
//! The evaluator is stateless — it owns no scratch buffers and does
//! no caching across calls. Intermediate arrays are allocated via
//! the Arrow compute kernels the evaluator dispatches to, which is
//! the same allocation pattern DataFusion uses at the inner
//! vectorized kernel layer. Smarter arena reuse / selection-vector
//! propagation is the job of the Wave 5 fused stateless push segment
//! (TASK-503); Wave 2 ships the correct-but-simpler copy-based path.
//!
//! # Literal broadcasting
//!
//! Literal operands are materialized into length-`n` broadcast
//! arrays at evaluation time. This is intentionally simpler than the
//! `arrow::array::Scalar` + `Datum` broadcasting path at the cost of
//! a handful of extra allocations per predicate. The steady-state
//! push-segment fusion design (execution-model.md §3.8) obviates the
//! question entirely by using selection vectors; we optimize here
//! when that work lands, not before.
//!
//! # Unsupported nodes
//!
//! Scalar function calls ([`CompiledNode::FunctionCall`]) are not
//! implemented in Wave 2 — the filter/project tests that need them
//! (`like`, `regex`) can be unblocked in a follow-up checkpoint.
//! The evaluator returns [`BqliteError::Execution`] with a
//! descriptive message when it encounters one, so the operator
//! surfaces a clean plan-time-style error instead of panicking.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringViewArray,
    TimestampNanosecondArray,
};
use arrow::compute::kernels::{boolean, cast, cmp, numeric};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use bqlite_core::{BqlType, BqliteError, PropertyValue, Result};
use bqlite_planner::compiled::{
    ArithKernel, ArrowKernelId, CastKernel, CompareKernel, CompiledExpr, CompiledNode, InSetKernel,
    LogicalKernel, UnaryKernel,
};

// ─────────────────────────────────────────────────────────────────────────────
// Public entry point
// ─────────────────────────────────────────────────────────────────────────────

/// Evaluate `expr` against `batch`, returning an array of length
/// `batch.num_rows()` whose Arrow type matches `expr.result_type`.
pub fn evaluate(expr: &CompiledExpr, batch: &RecordBatch) -> Result<ArrayRef> {
    match &expr.node {
        CompiledNode::Literal(value) => {
            broadcast_literal(value, &expr.result_type, batch.num_rows())
        }
        CompiledNode::Column { index, name } => evaluate_column(*index, name, batch),
        CompiledNode::Arith {
            left,
            right,
            kernel,
            ..
        } => {
            let l = evaluate(left, batch)?;
            let r = evaluate(right, batch)?;
            dispatch_arith(*kernel, l.as_ref(), r.as_ref())
        }
        CompiledNode::Compare {
            left,
            right,
            kernel,
            ..
        } => {
            let l = evaluate(left, batch)?;
            let r = evaluate(right, batch)?;
            let out = dispatch_compare(*kernel, l.as_ref(), r.as_ref())?;
            Ok(Arc::new(out) as ArrayRef)
        }
        CompiledNode::Unary {
            operand, kernel, ..
        } => {
            let operand = evaluate(operand, batch)?;
            dispatch_unary(*kernel, operand.as_ref())
        }
        CompiledNode::And { operands, kernel } => {
            let out = dispatch_logical(*kernel, LogicalOp::And, operands, batch)?;
            Ok(Arc::new(out) as ArrayRef)
        }
        CompiledNode::Or { operands, kernel } => {
            let out = dispatch_logical(*kernel, LogicalOp::Or, operands, batch)?;
            Ok(Arc::new(out) as ArrayRef)
        }
        CompiledNode::Not(inner) => {
            let inner = evaluate(inner, batch)?;
            let bool_array = downcast_bool(&inner, "NOT operand")?;
            let out = boolean::not(bool_array).map_err(arrow_err)?;
            Ok(Arc::new(out) as ArrayRef)
        }
        CompiledNode::IsNull { input, negated } => {
            let inner = evaluate(input, batch)?;
            let out = if *negated {
                boolean::is_not_null(inner.as_ref()).map_err(arrow_err)?
            } else {
                boolean::is_null(inner.as_ref()).map_err(arrow_err)?
            };
            Ok(Arc::new(out) as ArrayRef)
        }
        CompiledNode::Cast {
            input,
            target_type,
            kernel,
        }
        | CompiledNode::ImplicitCoerce {
            input,
            target_type,
            kernel,
        } => {
            let inner = evaluate(input, batch)?;
            dispatch_cast(*kernel, inner.as_ref(), target_type)
        }
        CompiledNode::InLiteralSet {
            input,
            values,
            negated,
            kernel,
        } => {
            let inner = evaluate(input, batch)?;
            let out = dispatch_in_set(*kernel, inner.as_ref(), values, *negated, batch.num_rows())?;
            Ok(Arc::new(out) as ArrayRef)
        }
        CompiledNode::FunctionCall { signature, .. } => Err(BqliteError::Execution(format!(
            "scalar function `{}` is not yet wired into the Wave 2 runtime evaluator",
            signature.name
        ))),
        // `CompiledNode` is `#[non_exhaustive]`; future variants
        // should fail loudly at runtime rather than silently drop
        // through a wildcard.
        other => Err(BqliteError::Execution(format!(
            "unsupported CompiledNode variant: {other:?}"
        ))),
    }
}

/// Convenience wrapper: evaluate `expr` and downcast the result to
/// a `BooleanArray`. Used by the filter operator and by the
/// variadic-logical folder for operand checks.
pub fn evaluate_bool(expr: &CompiledExpr, batch: &RecordBatch) -> Result<BooleanArray> {
    if !matches!(expr.result_type, BqlType::Bool) {
        return Err(BqliteError::Execution(format!(
            "expected Bool expression, found {}",
            expr.result_type
        )));
    }
    let array = evaluate(expr, batch)?;
    let bool_array = downcast_bool(&array, "Bool evaluation")?;
    Ok(bool_array.clone())
}

// ─────────────────────────────────────────────────────────────────────────────
// Column and literal evaluators
// ─────────────────────────────────────────────────────────────────────────────

fn evaluate_column(index: usize, name: &str, batch: &RecordBatch) -> Result<ArrayRef> {
    if index >= batch.num_columns() {
        return Err(BqliteError::Execution(format!(
            "column `{name}` has index {index} but batch only has {} columns",
            batch.num_columns()
        )));
    }
    Ok(batch.column(index).clone())
}

fn broadcast_literal(value: &PropertyValue, result_type: &BqlType, n: usize) -> Result<ArrayRef> {
    match (value, result_type) {
        (PropertyValue::Null, BqlType::Bool) => Ok(Arc::new(BooleanArray::new_null(n)) as ArrayRef),
        (PropertyValue::Null, BqlType::Int) => Ok(Arc::new(Int64Array::new_null(n)) as ArrayRef),
        (PropertyValue::Null, BqlType::Float) => {
            Ok(Arc::new(Float64Array::new_null(n)) as ArrayRef)
        }
        (PropertyValue::Null, BqlType::String) => {
            Ok(Arc::new(StringViewArray::new_null(n)) as ArrayRef)
        }
        (PropertyValue::Null, BqlType::Timestamp) => {
            Ok(Arc::new(TimestampNanosecondArray::new_null(n).with_timezone("UTC")) as ArrayRef)
        }
        (PropertyValue::Bool(b), BqlType::Bool) => {
            Ok(Arc::new(BooleanArray::from(vec![*b; n])) as ArrayRef)
        }
        (PropertyValue::Int(i), BqlType::Int) => {
            Ok(Arc::new(Int64Array::from_value(*i, n)) as ArrayRef)
        }
        (PropertyValue::Float(f), BqlType::Float) => {
            Ok(Arc::new(Float64Array::from_value(*f, n)) as ArrayRef)
        }
        (PropertyValue::String(s), BqlType::String) => Ok(Arc::new(
            StringViewArray::from_iter_values(std::iter::repeat_n(s.as_str(), n)),
        ) as ArrayRef),
        (PropertyValue::Timestamp(ts), BqlType::Timestamp) => Ok(Arc::new(
            TimestampNanosecondArray::from_value(*ts, n).with_timezone("UTC"),
        ) as ArrayRef),
        // Int literals used where Float is expected — happens in
        // pre-coercion corners before the implicit-coerce wrapper
        // folds them. The type checker normally inserts an
        // `ImplicitCoerce` wrapper; this branch is a defensive
        // fallback.
        (PropertyValue::Int(i), BqlType::Float) => {
            Ok(Arc::new(Float64Array::from_value(*i as f64, n)) as ArrayRef)
        }
        // Bags of types we do not materialize at the literal level in
        // Wave 2 (lists, maps, compound types). These should never
        // reach the evaluator because the type checker rejects them
        // upstream; if one slips through we surface an execution
        // error instead of panicking.
        (v, t) => Err(BqliteError::Execution(format!(
            "unsupported literal broadcast: {v:?} as {t}"
        ))),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Kernel dispatch
// ─────────────────────────────────────────────────────────────────────────────

fn dispatch_arith(kernel: ArithKernel, lhs: &dyn Array, rhs: &dyn Array) -> Result<ArrayRef> {
    let id = match kernel {
        ArithKernel::ArrowKernel(id) => id,
        other => {
            return Err(BqliteError::Execution(format!(
                "unsupported arithmetic kernel variant: {other:?}"
            )));
        }
    };
    let op = arith_op_for(id)?;
    let result = match op {
        NumericOp::Add => numeric::add(&lhs, &rhs),
        NumericOp::Sub => numeric::sub(&lhs, &rhs),
        NumericOp::Mul => numeric::mul(&lhs, &rhs),
        NumericOp::Div => numeric::div(&lhs, &rhs),
        NumericOp::Rem => numeric::rem(&lhs, &rhs),
    };
    result.map_err(arrow_err)
}

fn dispatch_compare(
    kernel: CompareKernel,
    lhs: &dyn Array,
    rhs: &dyn Array,
) -> Result<BooleanArray> {
    let id = match kernel {
        CompareKernel::ArrowKernel(id) => id,
        other => {
            return Err(BqliteError::Execution(format!(
                "unsupported compare kernel variant: {other:?}"
            )));
        }
    };
    let op = compare_op_for(id)?;
    let result = match op {
        CompareOpKind::Eq => cmp::eq(&lhs, &rhs),
        CompareOpKind::Ne => cmp::neq(&lhs, &rhs),
        CompareOpKind::Lt => cmp::lt(&lhs, &rhs),
        CompareOpKind::Le => cmp::lt_eq(&lhs, &rhs),
        CompareOpKind::Gt => cmp::gt(&lhs, &rhs),
        CompareOpKind::Ge => cmp::gt_eq(&lhs, &rhs),
    };
    result.map_err(arrow_err)
}

fn dispatch_unary(kernel: UnaryKernel, operand: &dyn Array) -> Result<ArrayRef> {
    let id = match kernel {
        UnaryKernel::ArrowKernel(id) => id,
        other => {
            return Err(BqliteError::Execution(format!(
                "unsupported unary kernel variant: {other:?}"
            )));
        }
    };
    match id {
        ArrowKernelId::NegateInt | ArrowKernelId::NegateFloat => {
            numeric::neg(operand).map_err(arrow_err)
        }
        ArrowKernelId::PlusInt | ArrowKernelId::PlusFloat => {
            // Unary plus is the identity on numeric types. Return a
            // clone of the input to preserve the "new ArrayRef per
            // evaluate" contract without a redundant kernel call.
            Ok(arrow::array::make_array(operand.to_data()))
        }
        other => Err(BqliteError::Execution(format!(
            "unexpected unary arrow kernel id: {other:?}"
        ))),
    }
}

fn dispatch_cast(kernel: CastKernel, input: &dyn Array, target: &BqlType) -> Result<ArrayRef> {
    let id = match kernel {
        CastKernel::ArrowKernel(id) => id,
        other => {
            return Err(BqliteError::Execution(format!(
                "unsupported cast kernel variant: {other:?}"
            )));
        }
    };
    if matches!(id, ArrowKernelId::CastIdentity) {
        return Ok(arrow::array::make_array(input.to_data()));
    }
    let arrow_target = bql_to_arrow_for_cast(target);
    cast::cast(input, &arrow_target).map_err(arrow_err)
}

#[derive(Clone, Copy)]
enum LogicalOp {
    And,
    Or,
}

fn dispatch_logical(
    kernel: LogicalKernel,
    op: LogicalOp,
    operands: &[CompiledExpr],
    batch: &RecordBatch,
) -> Result<BooleanArray> {
    match kernel {
        LogicalKernel::ArrowKleene => {}
        other => {
            return Err(BqliteError::Execution(format!(
                "unsupported logical kernel variant: {other:?}"
            )));
        }
    }
    if operands.is_empty() {
        return Err(BqliteError::Execution(
            "logical connective requires at least one operand".into(),
        ));
    }
    let mut iter = operands.iter();
    let first = iter.next().expect("operands is non-empty");
    let mut acc = evaluate_bool_expr(first, batch)?;
    for next in iter {
        let rhs = evaluate_bool_expr(next, batch)?;
        acc = match op {
            LogicalOp::And => boolean::and_kleene(&acc, &rhs).map_err(arrow_err)?,
            LogicalOp::Or => boolean::or_kleene(&acc, &rhs).map_err(arrow_err)?,
        };
    }
    Ok(acc)
}

fn dispatch_in_set(
    kernel: InSetKernel,
    input: &dyn Array,
    values: &[PropertyValue],
    negated: bool,
    n: usize,
) -> Result<BooleanArray> {
    match kernel {
        InSetKernel::ArrowIsIn => {}
        other => {
            return Err(BqliteError::Execution(format!(
                "unsupported IN-set kernel variant: {other:?}"
            )));
        }
    }
    if values.is_empty() {
        // Empty set: `x IN ()` → false for every row,
        // `x NOT IN ()` → true for every row (three-valued logic
        // under Kleene turns `false` into `true` under `NOT`).
        let outcome = negated;
        return Ok(BooleanArray::from(vec![outcome; n]));
    }
    // Fold a disjunction of `input == literal_i` across the set.
    // Uses the literal's type as reflected in `input.data_type()`
    // for broadcast construction — every value in the set shares
    // the same type because the type checker enforces that in §4.
    let element_type = bql_type_from_arrow(input.data_type())?;
    let mut acc: Option<BooleanArray> = None;
    for v in values {
        let lit = broadcast_literal(v, &element_type, n)?;
        let mask = cmp::eq(&input, &lit.as_ref()).map_err(arrow_err)?;
        acc = Some(match acc {
            None => mask,
            Some(prev) => boolean::or_kleene(&prev, &mask).map_err(arrow_err)?,
        });
    }
    let positive = acc.expect("values non-empty so acc is populated");
    if negated {
        boolean::not(&positive).map_err(arrow_err)
    } else {
        Ok(positive)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Internal support
// ─────────────────────────────────────────────────────────────────────────────

fn evaluate_bool_expr(expr: &CompiledExpr, batch: &RecordBatch) -> Result<BooleanArray> {
    let array = evaluate(expr, batch)?;
    Ok(downcast_bool(&array, "logical operand")?.clone())
}

fn downcast_bool<'a>(array: &'a ArrayRef, context: &'static str) -> Result<&'a BooleanArray> {
    array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| {
            BqliteError::Execution(format!(
                "{context}: expected BooleanArray, got {:?}",
                array.data_type()
            ))
        })
}

fn arrow_err(err: arrow::error::ArrowError) -> BqliteError {
    BqliteError::Arrow(err)
}

#[derive(Clone, Copy)]
enum NumericOp {
    Add,
    Sub,
    Mul,
    Div,
    Rem,
}

fn arith_op_for(id: ArrowKernelId) -> Result<NumericOp> {
    match id {
        ArrowKernelId::AddInt | ArrowKernelId::AddFloat | ArrowKernelId::AddTimestampInt => {
            Ok(NumericOp::Add)
        }
        // `Int + Timestamp` and `Timestamp + Int` both lower to the
        // same Arrow `add` dispatch; the kernel recognizes the
        // operand types and produces the correct temporal result.
        ArrowKernelId::AddIntTimestamp => Ok(NumericOp::Add),
        ArrowKernelId::SubInt
        | ArrowKernelId::SubFloat
        | ArrowKernelId::SubTimestamp
        | ArrowKernelId::SubTimestampInt => Ok(NumericOp::Sub),
        ArrowKernelId::MulInt | ArrowKernelId::MulFloat => Ok(NumericOp::Mul),
        ArrowKernelId::DivInt | ArrowKernelId::DivFloat => Ok(NumericOp::Div),
        ArrowKernelId::ModInt | ArrowKernelId::ModFloat => Ok(NumericOp::Rem),
        other => Err(BqliteError::Execution(format!(
            "unexpected arithmetic arrow kernel id: {other:?}"
        ))),
    }
}

#[derive(Clone, Copy)]
enum CompareOpKind {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

fn compare_op_for(id: ArrowKernelId) -> Result<CompareOpKind> {
    use ArrowKernelId::*;
    Ok(match id {
        EqInt | EqFloat | EqString | EqBool | EqTimestamp => CompareOpKind::Eq,
        NeInt | NeFloat | NeString | NeBool | NeTimestamp => CompareOpKind::Ne,
        LtInt | LtFloat | LtString | LtTimestamp => CompareOpKind::Lt,
        LeInt | LeFloat | LeString | LeTimestamp => CompareOpKind::Le,
        GtInt | GtFloat | GtString | GtTimestamp => CompareOpKind::Gt,
        GeInt | GeFloat | GeString | GeTimestamp => CompareOpKind::Ge,
        other => {
            return Err(BqliteError::Execution(format!(
                "unexpected compare arrow kernel id: {other:?}"
            )));
        }
    })
}

fn bql_to_arrow_for_cast(ty: &BqlType) -> DataType {
    match ty {
        BqlType::Bool => DataType::Boolean,
        BqlType::Int => DataType::Int64,
        BqlType::Float => DataType::Float64,
        BqlType::String => DataType::Utf8View,
        BqlType::Timestamp => DataType::Timestamp(
            arrow::datatypes::TimeUnit::Nanosecond,
            Some(Arc::from("UTC")),
        ),
        // Compound types — Wave 2 expression compilation never
        // produces casts to these, so we surface a descriptive
        // fallback via Utf8View that `cast` will likely reject
        // loudly.
        _ => DataType::Utf8View,
    }
}

fn bql_type_from_arrow(data_type: &DataType) -> Result<BqlType> {
    match data_type {
        DataType::Boolean => Ok(BqlType::Bool),
        DataType::Int64 => Ok(BqlType::Int),
        DataType::Float64 => Ok(BqlType::Float),
        DataType::Utf8View | DataType::Utf8 => Ok(BqlType::String),
        DataType::Timestamp(_, _) => Ok(BqlType::Timestamp),
        // Dictionary-encoded string columns (the common case from
        // storage). The inner value type is what the literal set
        // must broadcast as.
        DataType::Dictionary(_, value_type) => bql_type_from_arrow(value_type),
        other => Err(BqliteError::Execution(format!(
            "unsupported column type `{other:?}` for literal set comparison"
        ))),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{BooleanArray, Float64Array, Int64Array, StringViewArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

    use bqlite_ast::expr::{BinaryOp, CompareOp, Expr, InRhs, Literal, Spanned, UnaryOp};
    use bqlite_ast::span::{Name, Span};
    use bqlite_core::{ColumnDef, OperatorSchema};
    use bqlite_planner::expr::{FunctionRegistry, TypedExpr};

    use super::*;

    // ── Helpers ──────────────────────────────────────────────────────────────

    fn int_col_schema() -> OperatorSchema {
        OperatorSchema::new(vec![
            ColumnDef::required("id", BqlType::Int),
            ColumnDef::required("value", BqlType::Int),
        ])
        .unwrap()
    }

    fn mixed_col_schema() -> OperatorSchema {
        OperatorSchema::new(vec![
            ColumnDef::required("id", BqlType::Int),
            ColumnDef::required("amount", BqlType::Float),
            ColumnDef::required("name", BqlType::String),
            ColumnDef::nullable("flag", BqlType::Bool),
        ])
        .unwrap()
    }

    fn make_int_batch(ids: Vec<i64>, values: Vec<i64>) -> RecordBatch {
        let id_array: ArrayRef = Arc::new(Int64Array::from(ids));
        let val_array: ArrayRef = Arc::new(Int64Array::from(values));
        let schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]);
        RecordBatch::try_new(Arc::new(schema), vec![id_array, val_array]).unwrap()
    }

    fn make_mixed_batch(
        ids: Vec<i64>,
        amounts: Vec<f64>,
        names: Vec<&str>,
        flags: Vec<Option<bool>>,
    ) -> RecordBatch {
        let id_array: ArrayRef = Arc::new(Int64Array::from(ids));
        let amount_array: ArrayRef = Arc::new(Float64Array::from(amounts));
        let name_array: ArrayRef = Arc::new(StringViewArray::from(names));
        let flag_array: ArrayRef = Arc::new(BooleanArray::from(flags));
        let schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("amount", DataType::Float64, false),
            Field::new("name", DataType::Utf8View, false),
            Field::new("flag", DataType::Boolean, true),
        ]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![id_array, amount_array, name_array, flag_array],
        )
        .unwrap()
    }

    // AST builders — keep the tests readable without pulling in the
    // full parser surface, which does not expose a standalone
    // `parse_expression` entry point in Wave 2.

    fn sp<T>(node: T) -> Spanned<T> {
        Spanned::new(node, Span::EMPTY)
    }

    fn col(name: &str) -> Spanned<Expr> {
        sp(Expr::Column(Name::synthetic(name)))
    }

    fn int_lit_ast(i: i64) -> Spanned<Expr> {
        sp(Expr::Literal(Literal::Int(i)))
    }

    fn float_lit_ast(f: f64) -> Spanned<Expr> {
        sp(Expr::Literal(Literal::Float(f)))
    }

    fn str_lit_ast(s: &str) -> Spanned<Expr> {
        sp(Expr::Literal(Literal::String(s.into())))
    }

    fn cmp_expr(op: CompareOp, left: Spanned<Expr>, right: Spanned<Expr>) -> Spanned<Expr> {
        sp(Expr::Compare {
            op,
            left: Box::new(left),
            right: Box::new(right),
        })
    }

    fn binary_expr(op: BinaryOp, left: Spanned<Expr>, right: Spanned<Expr>) -> Spanned<Expr> {
        sp(Expr::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
        })
    }

    fn and_expr(ops: Vec<Spanned<Expr>>) -> Spanned<Expr> {
        sp(Expr::And(ops))
    }

    fn or_expr(ops: Vec<Spanned<Expr>>) -> Spanned<Expr> {
        sp(Expr::Or(ops))
    }

    fn not_expr(inner: Spanned<Expr>) -> Spanned<Expr> {
        sp(Expr::Not(Box::new(inner)))
    }

    fn is_null_expr(inner: Spanned<Expr>, negated: bool) -> Spanned<Expr> {
        sp(Expr::IsNull {
            expr: Box::new(inner),
            negated,
        })
    }

    fn unary_expr(op: UnaryOp, operand: Spanned<Expr>) -> Spanned<Expr> {
        sp(Expr::Unary {
            op,
            operand: Box::new(operand),
        })
    }

    fn in_list_expr(
        lhs: Spanned<Expr>,
        values: Vec<Spanned<Expr>>,
        negated: bool,
    ) -> Spanned<Expr> {
        sp(Expr::In {
            lhs: vec![lhs],
            rhs: InRhs::List(values),
            negated,
        })
    }

    fn function_call_expr(name: &str, args: Vec<Spanned<Expr>>) -> Spanned<Expr> {
        sp(Expr::FunctionCall {
            name: Name::synthetic(name),
            args,
            over: None,
        })
    }

    fn compile(ast: Spanned<Expr>, schema: &OperatorSchema) -> CompiledExpr {
        let reg = FunctionRegistry::with_builtins();
        let typed = TypedExpr::from_ast(&ast, schema, &reg).expect("expression type checks");
        CompiledExpr::from_typed(&typed)
    }

    fn compile_literal(lit: Literal, bql_ty: BqlType) -> CompiledExpr {
        let schema = OperatorSchema::new(vec![]).unwrap();
        let reg = FunctionRegistry::new();
        let typed = TypedExpr::from_ast(&sp(Expr::Literal(lit)), &schema, &reg)
            .expect("literal type checks");
        assert_eq!(typed.result_type, bql_ty);
        CompiledExpr::from_typed(&typed)
    }

    fn as_bool(array: &ArrayRef) -> &BooleanArray {
        array.as_any().downcast_ref::<BooleanArray>().unwrap()
    }

    fn as_int(array: &ArrayRef) -> &Int64Array {
        array.as_any().downcast_ref::<Int64Array>().unwrap()
    }

    fn as_float(array: &ArrayRef) -> &Float64Array {
        array.as_any().downcast_ref::<Float64Array>().unwrap()
    }

    // ── Literals ─────────────────────────────────────────────────────────────

    #[test]
    fn literal_int_broadcasts_to_batch_length() {
        let expr = compile_literal(Literal::Int(42), BqlType::Int);
        let batch = make_int_batch(vec![1, 2, 3], vec![10, 20, 30]);
        let out = evaluate(&expr, &batch).unwrap();
        let col = as_int(&out);
        assert_eq!(col.len(), 3);
        assert_eq!(col.value(0), 42);
        assert_eq!(col.value(1), 42);
        assert_eq!(col.value(2), 42);
    }

    #[test]
    fn literal_string_materializes_stringview() {
        let expr = compile_literal(Literal::String("hi".into()), BqlType::String);
        let batch = make_int_batch(vec![1, 2], vec![10, 20]);
        let out = evaluate(&expr, &batch).unwrap();
        let col = out.as_any().downcast_ref::<StringViewArray>().unwrap();
        assert_eq!(col.len(), 2);
        assert_eq!(col.value(0), "hi");
        assert_eq!(col.value(1), "hi");
        // Must be Utf8View per execution-model.md §3.7 — never flat Utf8.
        assert_eq!(out.data_type(), &DataType::Utf8View);
    }

    #[test]
    fn literal_null_broadcasts_to_typed_null_array() {
        // NULL cannot be constructed via the parser's literal form
        // in Wave 2 (no `NULL` keyword support), so we build a
        // null-literal CompiledExpr by hand to exercise the
        // broadcast path.
        let node = CompiledNode::Literal(PropertyValue::Null);
        let expr = CompiledExpr {
            node,
            result_type: BqlType::Int,
            nullable: true,
        };
        let batch = make_int_batch(vec![1, 2], vec![10, 20]);
        let out = evaluate(&expr, &batch).unwrap();
        let col = as_int(&out);
        assert_eq!(col.len(), 2);
        assert!(col.is_null(0));
        assert!(col.is_null(1));
    }

    // ── Column reference ─────────────────────────────────────────────────────

    #[test]
    fn column_reference_returns_batch_column() {
        let schema = int_col_schema();
        let expr = compile(col("value"), &schema);
        let batch = make_int_batch(vec![1, 2, 3], vec![10, 20, 30]);
        let out = evaluate(&expr, &batch).unwrap();
        let c = as_int(&out);
        assert_eq!(c.value(0), 10);
        assert_eq!(c.value(1), 20);
        assert_eq!(c.value(2), 30);
    }

    // ── Comparison ───────────────────────────────────────────────────────────

    #[test]
    fn compare_eq_int_column_to_literal() {
        let schema = int_col_schema();
        let expr = compile(
            cmp_expr(CompareOp::Equal, col("value"), int_lit_ast(20)),
            &schema,
        );
        let batch = make_int_batch(vec![1, 2, 3, 4], vec![10, 20, 30, 20]);
        let out = evaluate(&expr, &batch).unwrap();
        let bools = as_bool(&out);
        assert!(!bools.value(0));
        assert!(bools.value(1));
        assert!(!bools.value(2));
        assert!(bools.value(3));
    }

    #[test]
    fn compare_gt_int_column_to_literal() {
        let schema = int_col_schema();
        let expr = compile(
            cmp_expr(CompareOp::Greater, col("value"), int_lit_ast(15)),
            &schema,
        );
        let batch = make_int_batch(vec![1, 2, 3], vec![10, 20, 30]);
        let out = evaluate(&expr, &batch).unwrap();
        let bools = as_bool(&out);
        assert!(!bools.value(0));
        assert!(bools.value(1));
        assert!(bools.value(2));
    }

    #[test]
    fn compare_string_eq_literal() {
        let schema = mixed_col_schema();
        let expr = compile(
            cmp_expr(CompareOp::Equal, col("name"), str_lit_ast("bob")),
            &schema,
        );
        let batch = make_mixed_batch(
            vec![1, 2, 3],
            vec![1.0, 2.0, 3.0],
            vec!["alice", "bob", "carol"],
            vec![Some(true), Some(false), Some(true)],
        );
        let out = evaluate(&expr, &batch).unwrap();
        let bools = as_bool(&out);
        assert!(!bools.value(0));
        assert!(bools.value(1));
        assert!(!bools.value(2));
    }

    // ── Arithmetic ───────────────────────────────────────────────────────────

    #[test]
    fn arith_add_column_to_literal() {
        let schema = int_col_schema();
        let expr = compile(
            binary_expr(BinaryOp::Add, col("value"), int_lit_ast(5)),
            &schema,
        );
        let batch = make_int_batch(vec![1, 2, 3], vec![10, 20, 30]);
        let out = evaluate(&expr, &batch).unwrap();
        let c = as_int(&out);
        assert_eq!(c.value(0), 15);
        assert_eq!(c.value(1), 25);
        assert_eq!(c.value(2), 35);
    }

    #[test]
    fn arith_mul_column_by_column() {
        let schema = int_col_schema();
        let expr = compile(
            binary_expr(BinaryOp::Multiply, col("id"), col("value")),
            &schema,
        );
        let batch = make_int_batch(vec![1, 2, 3], vec![10, 20, 30]);
        let out = evaluate(&expr, &batch).unwrap();
        let c = as_int(&out);
        assert_eq!(c.value(0), 10);
        assert_eq!(c.value(1), 40);
        assert_eq!(c.value(2), 90);
    }

    #[test]
    fn arith_float_column_operation() {
        let schema = mixed_col_schema();
        let expr = compile(
            binary_expr(BinaryOp::Multiply, col("amount"), float_lit_ast(2.0)),
            &schema,
        );
        let batch = make_mixed_batch(
            vec![1, 2],
            vec![1.5, 2.5],
            vec!["x", "y"],
            vec![Some(true), Some(false)],
        );
        let out = evaluate(&expr, &batch).unwrap();
        let c = as_float(&out);
        assert!((c.value(0) - 3.0).abs() < 1e-9);
        assert!((c.value(1) - 5.0).abs() < 1e-9);
    }

    // ── Unary ────────────────────────────────────────────────────────────────

    #[test]
    fn unary_negate_int_column() {
        let schema = int_col_schema();
        let expr = compile(unary_expr(UnaryOp::Negate, col("value")), &schema);
        let batch = make_int_batch(vec![1, 2, 3], vec![10, 20, 30]);
        let out = evaluate(&expr, &batch).unwrap();
        let c = as_int(&out);
        assert_eq!(c.value(0), -10);
        assert_eq!(c.value(1), -20);
        assert_eq!(c.value(2), -30);
    }

    // ── Logical connectives ──────────────────────────────────────────────────

    #[test]
    fn logical_and_combines_two_predicates() {
        let schema = int_col_schema();
        let expr = compile(
            and_expr(vec![
                cmp_expr(CompareOp::Greater, col("value"), int_lit_ast(10)),
                cmp_expr(CompareOp::Less, col("id"), int_lit_ast(4)),
            ]),
            &schema,
        );
        let batch = make_int_batch(vec![1, 2, 3, 4], vec![5, 20, 30, 15]);
        let out = evaluate(&expr, &batch).unwrap();
        let bools = as_bool(&out);
        assert!(!bools.value(0)); // value=5 fails first
        assert!(bools.value(1));
        assert!(bools.value(2));
        assert!(!bools.value(3)); // id=4 fails second
    }

    #[test]
    fn logical_or_combines_two_predicates() {
        let schema = int_col_schema();
        let expr = compile(
            or_expr(vec![
                cmp_expr(CompareOp::Greater, col("value"), int_lit_ast(25)),
                cmp_expr(CompareOp::Equal, col("id"), int_lit_ast(1)),
            ]),
            &schema,
        );
        let batch = make_int_batch(vec![1, 2, 3], vec![10, 20, 30]);
        let out = evaluate(&expr, &batch).unwrap();
        let bools = as_bool(&out);
        assert!(bools.value(0)); // id=1 matches
        assert!(!bools.value(1));
        assert!(bools.value(2)); // value=30 > 25
    }

    #[test]
    fn logical_not_flips_predicate() {
        let schema = int_col_schema();
        let expr = compile(
            not_expr(cmp_expr(CompareOp::Equal, col("value"), int_lit_ast(20))),
            &schema,
        );
        let batch = make_int_batch(vec![1, 2, 3], vec![10, 20, 30]);
        let out = evaluate(&expr, &batch).unwrap();
        let bools = as_bool(&out);
        assert!(bools.value(0));
        assert!(!bools.value(1));
        assert!(bools.value(2));
    }

    // ── IS NULL / IS NOT NULL ────────────────────────────────────────────────

    #[test]
    fn is_null_on_nullable_column() {
        let schema = mixed_col_schema();
        let expr = compile(is_null_expr(col("flag"), false), &schema);
        let batch = make_mixed_batch(
            vec![1, 2, 3],
            vec![1.0, 2.0, 3.0],
            vec!["a", "b", "c"],
            vec![Some(true), None, Some(false)],
        );
        let out = evaluate(&expr, &batch).unwrap();
        let bools = as_bool(&out);
        assert!(!bools.value(0));
        assert!(bools.value(1));
        assert!(!bools.value(2));
    }

    #[test]
    fn is_not_null_on_nullable_column() {
        let schema = mixed_col_schema();
        let expr = compile(is_null_expr(col("flag"), true), &schema);
        let batch = make_mixed_batch(
            vec![1, 2],
            vec![1.0, 2.0],
            vec!["a", "b"],
            vec![None, Some(true)],
        );
        let out = evaluate(&expr, &batch).unwrap();
        let bools = as_bool(&out);
        assert!(!bools.value(0));
        assert!(bools.value(1));
    }

    // ── IN (literal set) ─────────────────────────────────────────────────────

    #[test]
    fn in_literal_set_int() {
        let schema = int_col_schema();
        let expr = compile(
            in_list_expr(col("value"), vec![int_lit_ast(10), int_lit_ast(30)], false),
            &schema,
        );
        let batch = make_int_batch(vec![1, 2, 3, 4], vec![10, 20, 30, 40]);
        let out = evaluate(&expr, &batch).unwrap();
        let bools = as_bool(&out);
        assert!(bools.value(0));
        assert!(!bools.value(1));
        assert!(bools.value(2));
        assert!(!bools.value(3));
    }

    #[test]
    fn not_in_literal_set_int() {
        let schema = int_col_schema();
        let expr = compile(
            in_list_expr(col("value"), vec![int_lit_ast(10), int_lit_ast(30)], true),
            &schema,
        );
        let batch = make_int_batch(vec![1, 2, 3], vec![10, 20, 30]);
        let out = evaluate(&expr, &batch).unwrap();
        let bools = as_bool(&out);
        assert!(!bools.value(0));
        assert!(bools.value(1));
        assert!(!bools.value(2));
    }

    // ── Cast ─────────────────────────────────────────────────────────────────

    #[test]
    fn implicit_int_to_float_coercion_column_vs_column() {
        // `amount > id` — `amount` is Float, `id` is Int. The type
        // checker wraps `id` in an `ImplicitCoerce` to Float so both
        // sides agree at comparison time.
        //
        // Literal-vs-column coercion goes the other way (the literal
        // is coerced to the column's type — see
        // `bqlite_planner::expr::type_check_compare`), so this test
        // uses a column-vs-column pair to exercise the Int→Float
        // implicit coerce path specifically.
        let schema = mixed_col_schema();
        let expr = compile(
            cmp_expr(CompareOp::Greater, col("amount"), col("id")),
            &schema,
        );
        let batch = make_mixed_batch(
            vec![1, 2, 3],
            vec![0.5, 2.5, 3.5],
            vec!["a", "b", "c"],
            vec![Some(true), Some(false), Some(true)],
        );
        let out = evaluate(&expr, &batch).unwrap();
        let bools = as_bool(&out);
        assert!(!bools.value(0)); // 0.5 > 1.0 → false
        assert!(bools.value(1)); // 2.5 > 2.0 → true
        assert!(bools.value(2)); // 3.5 > 3.0 → true
    }

    // ── NULL propagation ─────────────────────────────────────────────────────

    #[test]
    fn compare_preserves_null_bitmap_from_nullable_input() {
        // Comparing a nullable column against a literal must produce
        // a Bool array whose null positions match the input's — NULL
        // propagates under three-valued logic. Filter relies on this
        // to drop rows whose predicate is NULL (not keep them).
        let schema = OperatorSchema::new(vec![ColumnDef::nullable("count", BqlType::Int)]).unwrap();
        let expr = compile(
            cmp_expr(CompareOp::Greater, col("count"), int_lit_ast(5)),
            &schema,
        );
        let count_array: ArrayRef = Arc::new(Int64Array::from(vec![Some(3), None, Some(10)]));
        let arrow_schema = ArrowSchema::new(vec![Field::new("count", DataType::Int64, true)]);
        let batch = RecordBatch::try_new(Arc::new(arrow_schema), vec![count_array]).unwrap();
        let out = evaluate(&expr, &batch).unwrap();
        let bools = as_bool(&out);
        assert!(!bools.value(0));
        assert!(bools.is_null(1), "null input must yield null output");
        assert!(bools.value(2));
    }

    // ── Error paths ──────────────────────────────────────────────────────────

    #[test]
    fn function_call_errors_cleanly() {
        let schema = mixed_col_schema();
        let expr = compile(
            function_call_expr("like", vec![col("name"), str_lit_ast("al%")]),
            &schema,
        );
        let batch = make_mixed_batch(vec![1], vec![1.0], vec!["alice"], vec![Some(true)]);
        let err = evaluate(&expr, &batch).expect_err("function call should error");
        match err {
            BqliteError::Execution(msg) => {
                assert!(
                    msg.contains("like"),
                    "message should name the function: {msg}"
                );
            }
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn evaluate_bool_rejects_non_bool_expression() {
        let schema = int_col_schema();
        let expr = compile(
            binary_expr(BinaryOp::Add, col("value"), int_lit_ast(1)),
            &schema,
        );
        let batch = make_int_batch(vec![1], vec![10]);
        let err = evaluate_bool(&expr, &batch).expect_err("non-bool expression");
        assert!(matches!(err, BqliteError::Execution(_)), "{err:?}");
    }
}
