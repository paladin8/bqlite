//! Typed expressions — the schema-resolved, type-checked form
//! carried by `LogicalPlan` nodes per
//! `docs/design/planner/expression-compilation.md` §4 (TASK-205 / TASK-225).
//!
//! # Two-stage pipeline
//!
//! ```text
//! bqlite_ast::Expr  ──type-check──▶  TypedExpr  ──compile──▶  CompiledExpr
//!   (untyped,         (schema-        (plan-time       (runtime,
//!    unresolved)       resolved,       cache)           TASK-226)
//!                      typed)
//! ```
//!
//! This module owns stage 1 — schema resolution, type inference,
//! coercion, and the `TypedExpr` plan-tree form. Stage 2
//! (`CompiledExpr`) lands alongside when TASK-226 wires the physical
//! descriptors that need it.
//!
//! # Contract
//!
//! Holding a `TypedExpr` is **proof** that the expression is
//! well-typed against the `OperatorSchema` it was built from. The only
//! public constructor is [`TypedExpr::from_ast`], which returns a
//! `Result` whose `Err` is a categorized plan-time type error.
//!
//! # Error taxonomy
//!
//! Wave 0 bqlite has a single `BqliteError::Plan(String)` variant that
//! absorbs every plan-time type error. A dedicated `TypeError` enum
//! per §11 of the expression-compilation design note is deferred; this
//! module emits structured `BqliteError::Plan` messages that name the
//! category and the offending construct / span so downstream tooling
//! can match on the message text until the typed error variant lands.
//! Every error produced here passes through the small
//! [`type_error`] helper so the "category:" prefix is stable.

use std::collections::HashMap;
use std::sync::Arc;

use bqlite_ast::expr::{BinaryOp, CompareOp, Expr, InRhs, Literal, Spanned, UnaryOp};
use bqlite_ast::span::Span;
use bqlite_core::{BqlType, BqliteError, OperatorSchema, PropertyValue, Result};

// ─────────────────────────────────────────────────────────────────────────────
// TypedExpr
// ─────────────────────────────────────────────────────────────────────────────

/// A schema-resolved, type-checked expression.
///
/// Constructed exclusively by [`TypedExpr::from_ast`]. Every
/// `TypedExpr` value satisfies the design doc §4.4 nullability rule
/// and the §4.2 type-inference rules by construction; downstream
/// operator-level validation (e.g. "Filter's predicate must be Bool")
/// happens at the enclosing plan node's construction, not here — see
/// §4.5.
#[derive(Debug, Clone, PartialEq)]
pub struct TypedExpr {
    pub kind: TypedExprKind,
    /// Result type of this sub-expression under the schema it was
    /// typed against. Cached once at construction so enclosing
    /// `OperatorSchema` builders can read it in O(1).
    pub result_type: BqlType,
    /// Whether this sub-expression may evaluate to NULL. Computed
    /// per the conservative over-approximation in §4.4; runtime
    /// evaluation uses precise Kleene semantics through the Arrow
    /// kernels, so a `false` here is always correct and a `true` is
    /// a safe over-estimate.
    pub nullable: bool,
    /// Source span of the originating AST node, for diagnostics.
    pub span: Span,
}

/// The variant payload of a [`TypedExpr`].
///
/// Mirrors design doc §4.1. Variants are restricted to the Wave 2
/// surface — later-wave expression forms (window functions, CASE,
/// cohort subqueries, qualified column references) are rejected at
/// `from_ast` with an `unsupported` error.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum TypedExprKind {
    /// A literal that has been coerced to its final `BqlType`
    /// already. Duration literals are folded into `PropertyValue::Int`
    /// nanoseconds; Timestamp literals stay `PropertyValue::Timestamp`.
    Literal(PropertyValue),

    /// A column reference resolved against the enclosing
    /// [`OperatorSchema`]. Carries both the index (for fast
    /// positional access at compile time) and the name (for
    /// pushdown and error messages).
    Column { column_index: usize, name: String },

    /// Arithmetic binary op (`+`, `-`, `*`, `/`, `%`). Result type
    /// follows the §4.4 arithmetic rules.
    Arith {
        op: BinaryOp,
        left: Box<TypedExpr>,
        right: Box<TypedExpr>,
    },

    /// Comparison (`=`, `!=`, `<`, `<=`, `>`, `>=`). Result is
    /// always `Bool`.
    Compare {
        op: CompareOp,
        left: Box<TypedExpr>,
        right: Box<TypedExpr>,
    },

    /// Unary arithmetic operator (negation, identity). Operand must
    /// be `Int` or `Float`; result type matches the operand.
    Unary {
        op: UnaryOp,
        operand: Box<TypedExpr>,
    },

    /// Variadic logical `AND`. All operands are `Bool`; result is
    /// `Bool`.
    And(Vec<TypedExpr>),
    /// Variadic logical `OR`. All operands are `Bool`; result is
    /// `Bool`.
    Or(Vec<TypedExpr>),
    /// Logical `NOT`. Operand is `Bool`; result is `Bool`.
    Not(Box<TypedExpr>),

    /// `expr IS NULL` (`negated = false`) or `expr IS NOT NULL`
    /// (`negated = true`). Accepts any input type; result is `Bool`
    /// and is guaranteed non-nullable.
    IsNull {
        input: Box<TypedExpr>,
        negated: bool,
    },

    /// Scalar function call resolved against the function registry.
    /// Window-form calls (`OVER (...)`) are rejected at `from_ast`.
    FunctionCall {
        signature: Arc<ScalarFunctionSig>,
        args: Vec<TypedExpr>,
    },

    /// Explicit `CAST(expr AS type)`. Preserved as its own variant
    /// so the optimizer does not elide it during later rewrites.
    Cast {
        input: Box<TypedExpr>,
        target_type: BqlType,
    },

    /// Implicit coercion inserted at type-check time — Wave 2's
    /// only source is the Int → Float promotion from §4.3.
    /// Structurally distinct from `Cast` so the optimizer can
    /// safely elide it.
    ImplicitCoerce {
        input: Box<TypedExpr>,
        target_type: BqlType,
    },

    /// `input IN (lit_1, lit_2, ..., lit_k)` — fixed literal set.
    /// Subquery `IN` forms (`IN QUERY`, `IN <alias>`) are
    /// Wave 4 and rejected at `from_ast`.
    InLiteralSet {
        input: Box<TypedExpr>,
        values: Vec<PropertyValue>,
        negated: bool,
    },
}

// ─────────────────────────────────────────────────────────────────────────────
// ScalarFunctionSig and FunctionRegistry
// ─────────────────────────────────────────────────────────────────────────────

/// A scalar function signature — the plan-time contract the type
/// checker looks up during function-call resolution.
///
/// Populated at registry-construction time; the full scalar-function
/// catalog from type-system.md §10.2 lives in
/// [`FunctionRegistry::with_builtins`]. Kernel bodies (the runtime
/// evaluator) are carried separately on `CompiledExpr` — see
/// TASK-226 and later checkpoints for that plumbing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScalarFunctionSig {
    /// Canonical function name, lower-cased.
    pub name: String,
    /// Ordered parameter types. An overload resolves iff the
    /// supplied `arg_types` match element-wise modulo Int→Float
    /// implicit coercion.
    pub params: Vec<BqlType>,
    /// Declared return type.
    pub return_type: BqlType,
    /// Whether the function's result is nullable independent of
    /// its arguments — e.g. `regex` can return NULL on a bad
    /// pattern. Conservative over-approximation per §4.4.
    pub nullable: bool,
}

/// Registry of scalar function signatures available to the type
/// checker.
///
/// Built once at the start of a plan and consulted for every
/// `FunctionCall` expression. TASK-225 ships a minimal Wave 2
/// registry containing the built-ins required by AST lowering
/// (`like`, `regex`); follow-up work fills in the rest of
/// type-system.md §10.2 as scalar-function kernels land.
#[derive(Debug, Clone, Default)]
pub struct FunctionRegistry {
    /// Multi-map keyed by lower-cased function name. Overloads
    /// share a key; resolution walks the bucket and picks the
    /// first signature whose parameter list matches the supplied
    /// argument types (modulo Int→Float coercion).
    by_name: HashMap<String, Vec<Arc<ScalarFunctionSig>>>,
}

impl FunctionRegistry {
    /// Empty registry — used mainly by tests that type-check
    /// expressions with no function calls.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registry pre-populated with the built-in scalar functions
    /// Wave 2 needs for AST lowering:
    ///
    /// - `like(String, String) -> Bool` — AST `Expr::Like` lowers
    ///   to a call on this signature.
    /// - `regex(String, String) -> Bool` — AST `Expr::Regex`
    ///   likewise.
    ///
    /// More signatures land as later checkpoints populate the
    /// runtime kernel table. Registering them here first is safe
    /// because type-check and compile are separate stages — a
    /// signature-only entry simply errors at compile time if a
    /// kernel is not yet wired.
    pub fn with_builtins() -> Self {
        let mut reg = Self::default();
        reg.register(ScalarFunctionSig {
            name: "like".into(),
            params: vec![BqlType::String, BqlType::String],
            return_type: BqlType::Bool,
            nullable: false,
        });
        reg.register(ScalarFunctionSig {
            name: "regex".into(),
            params: vec![BqlType::String, BqlType::String],
            return_type: BqlType::Bool,
            nullable: true,
        });
        reg
    }

    /// Add a signature to the registry.
    pub fn register(&mut self, sig: ScalarFunctionSig) {
        let name = sig.name.to_ascii_lowercase();
        self.by_name.entry(name).or_default().push(Arc::new(sig));
    }

    /// Resolve a call against the registered overloads. Returns
    /// `None` if no signature's parameter list matches the supplied
    /// argument types modulo Int→Float implicit coercion. The
    /// caller surfaces the None as a plan-time type error naming
    /// the function and the supplied types.
    pub fn resolve(&self, name: &str, arg_types: &[BqlType]) -> Option<Arc<ScalarFunctionSig>> {
        let bucket = self.by_name.get(&name.to_ascii_lowercase())?;
        for sig in bucket {
            if sig.params.len() != arg_types.len() {
                continue;
            }
            let ok = sig
                .params
                .iter()
                .zip(arg_types.iter())
                .all(|(expected, actual)| compatible_arg_type(expected, actual));
            if ok {
                return Some(Arc::clone(sig));
            }
        }
        None
    }
}

/// True if `actual` can be used where `expected` is declared,
/// modulo Wave 2's one implicit coercion (`Int → Float`). Used by
/// both overload resolution and by comparison / arithmetic type
/// rules.
fn compatible_arg_type(expected: &BqlType, actual: &BqlType) -> bool {
    expected == actual || (matches!(expected, BqlType::Float) && matches!(actual, BqlType::Int))
}

// ─────────────────────────────────────────────────────────────────────────────
// TypedExpr::from_ast — the type checker
// ─────────────────────────────────────────────────────────────────────────────

impl TypedExpr {
    /// Type-check an AST expression against `schema`, resolving
    /// function calls through `registry`.
    ///
    /// This is the single entry point the planner's logical
    /// lowering (TASK-224) uses for every expression it encounters:
    /// filter predicates, project items, and insert-body expressions.
    /// Every `bqlite_ast::Expr` variant either lowers to a
    /// `TypedExprKind` per design doc §4.2 or returns a structured
    /// `BqliteError::Plan` error categorized by [`type_error`].
    pub fn from_ast(
        expr: &Spanned<Expr>,
        schema: &OperatorSchema,
        registry: &FunctionRegistry,
    ) -> Result<TypedExpr> {
        match &expr.node {
            Expr::Literal(lit) => Ok(typed_literal(lit.clone(), expr.span)),

            Expr::Column(name) => {
                let (column_index, col_def) = schema
                    .column(&name.text)
                    .ok_or_else(|| unknown_column(&name.text, expr.span))?;
                Ok(TypedExpr {
                    kind: TypedExprKind::Column {
                        column_index,
                        name: name.text.clone(),
                    },
                    result_type: col_def.bql_type.clone(),
                    nullable: col_def.nullable,
                    span: expr.span,
                })
            }

            Expr::Qualified { .. } => Err(unsupported(
                "qualified column references `table.col` are deferred to Wave 4 joins (TASK-407)",
                expr.span,
            )),

            Expr::Variable(_) => Err(unsupported(
                "pattern-bound `$variable` references are only valid inside a MATCH pattern (Wave 3)",
                expr.span,
            )),

            Expr::Binary { op, left, right } => {
                let l = TypedExpr::from_ast(left, schema, registry)?;
                let r = TypedExpr::from_ast(right, schema, registry)?;
                type_check_arith(*op, l, r, expr.span)
            }

            Expr::Unary { op, operand } => {
                let inner = TypedExpr::from_ast(operand, schema, registry)?;
                type_check_unary(*op, inner, expr.span)
            }

            Expr::Compare { op, left, right } => {
                let l = TypedExpr::from_ast(left, schema, registry)?;
                let r = TypedExpr::from_ast(right, schema, registry)?;
                type_check_compare(*op, l, r, expr.span)
            }

            Expr::And(operands) => {
                let typed = type_check_bool_list(operands, schema, registry, "AND", expr.span)?;
                let nullable = typed.iter().any(|e| e.nullable);
                Ok(TypedExpr {
                    kind: TypedExprKind::And(typed),
                    result_type: BqlType::Bool,
                    nullable,
                    span: expr.span,
                })
            }

            Expr::Or(operands) => {
                let typed = type_check_bool_list(operands, schema, registry, "OR", expr.span)?;
                let nullable = typed.iter().any(|e| e.nullable);
                Ok(TypedExpr {
                    kind: TypedExprKind::Or(typed),
                    result_type: BqlType::Bool,
                    nullable,
                    span: expr.span,
                })
            }

            Expr::Not(operand) => {
                let inner = TypedExpr::from_ast(operand, schema, registry)?;
                if inner.result_type != BqlType::Bool {
                    return Err(type_mismatch(
                        "NOT",
                        &BqlType::Bool,
                        &inner.result_type,
                        expr.span,
                    ));
                }
                let nullable = inner.nullable;
                Ok(TypedExpr {
                    kind: TypedExprKind::Not(Box::new(inner)),
                    result_type: BqlType::Bool,
                    nullable,
                    span: expr.span,
                })
            }

            Expr::IsNull { expr: inner, negated } => {
                let typed = TypedExpr::from_ast(inner, schema, registry)?;
                Ok(TypedExpr {
                    kind: TypedExprKind::IsNull {
                        input: Box::new(typed),
                        negated: *negated,
                    },
                    result_type: BqlType::Bool,
                    // §4.4: IS NULL / IS NOT NULL are guaranteed T/F.
                    nullable: false,
                    span: expr.span,
                })
            }

            Expr::Between {
                expr: inner,
                low,
                high,
                negated,
            } => {
                // Desugar `x BETWEEN lo AND hi` → `(x >= lo) AND (x <= hi)`.
                // `x NOT BETWEEN lo AND hi` → `NOT ((x >= lo) AND (x <= hi))`.
                let lhs_ge = Spanned::new(
                    Expr::Compare {
                        op: CompareOp::GreaterOrEqual,
                        left: inner.clone(),
                        right: low.clone(),
                    },
                    expr.span,
                );
                let lhs_le = Spanned::new(
                    Expr::Compare {
                        op: CompareOp::LessOrEqual,
                        left: inner.clone(),
                        right: high.clone(),
                    },
                    expr.span,
                );
                let conj = Spanned::new(Expr::And(vec![lhs_ge, lhs_le]), expr.span);
                let typed_conj = TypedExpr::from_ast(&conj, schema, registry)?;
                if *negated {
                    let nullable = typed_conj.nullable;
                    Ok(TypedExpr {
                        kind: TypedExprKind::Not(Box::new(typed_conj)),
                        result_type: BqlType::Bool,
                        nullable,
                        span: expr.span,
                    })
                } else {
                    Ok(typed_conj)
                }
            }

            Expr::Like {
                expr: input,
                pattern,
                negated,
            } => lower_string_fn(input, pattern, *negated, "like", schema, registry, expr.span),

            Expr::Regex { expr: input, pattern } => {
                // `~=` has no negation form in the AST.
                lower_string_fn(input, pattern, false, "regex", schema, registry, expr.span)
            }

            Expr::Contains { .. } => Err(unsupported(
                "CONTAINS on list-typed columns requires Wave 4 list operators",
                expr.span,
            )),

            Expr::In { lhs, rhs, negated } => {
                if lhs.len() != 1 {
                    return Err(unsupported(
                        "multi-column IN predicates are deferred to Wave 4 (TASK-407 cohorts)",
                        expr.span,
                    ));
                }
                let input = TypedExpr::from_ast(&lhs[0], schema, registry)?;
                match rhs {
                    InRhs::List(literals) => {
                        let values = literals
                            .iter()
                            .map(|spanned| match &spanned.node {
                                Expr::Literal(lit) => {
                                    coerce_literal(lit.clone(), &input.result_type, spanned.span)
                                }
                                _ => Err(unsupported(
                                    "IN list elements must be literal values in Wave 2",
                                    spanned.span,
                                )),
                            })
                            .collect::<Result<Vec<_>>>()?;
                        let nullable = input.nullable;
                        Ok(TypedExpr {
                            kind: TypedExprKind::InLiteralSet {
                                input: Box::new(input),
                                values,
                                negated: *negated,
                            },
                            result_type: BqlType::Bool,
                            nullable,
                            span: expr.span,
                        })
                    }
                    InRhs::Query(_) => Err(unsupported(
                        "IN QUERY (cohort subqueries) is deferred to Wave 4 (TASK-407)",
                        expr.span,
                    )),
                    InRhs::Alias(_) => Err(unsupported(
                        "IN <alias> is deferred to Wave 4 (TASK-407)",
                        expr.span,
                    )),
                }
            }

            Expr::Case { .. } => Err(unsupported(
                "CASE expressions are deferred to Wave 3",
                expr.span,
            )),

            Expr::Cast { expr: inner, target } => {
                let input = TypedExpr::from_ast(inner, schema, registry)?;
                // Wave 2 permits explicit casts between any scalar
                // pair; the full legality matrix from type-system.md
                // §4.2 lands with the runtime kernel coverage in a
                // later checkpoint.
                let nullable = true; // explicit casts may produce NULL on parse failure
                Ok(TypedExpr {
                    kind: TypedExprKind::Cast {
                        input: Box::new(input),
                        target_type: target.clone(),
                    },
                    result_type: target.clone(),
                    nullable,
                    span: expr.span,
                })
            }

            Expr::FunctionCall { name, args, over } => {
                if over.is_some() {
                    return Err(unsupported(
                        "window functions (OVER clause) are deferred to Wave 3",
                        expr.span,
                    ));
                }
                let typed_args: Vec<TypedExpr> = args
                    .iter()
                    .map(|a| TypedExpr::from_ast(a, schema, registry))
                    .collect::<Result<Vec<_>>>()?;
                let arg_types: Vec<BqlType> =
                    typed_args.iter().map(|a| a.result_type.clone()).collect();
                let signature = registry.resolve(&name.text, &arg_types).ok_or_else(|| {
                    no_matching_overload(&name.text, &arg_types, expr.span)
                })?;
                // Apply Int→Float implicit coercions where the
                // signature declares Float and the actual arg is Int.
                let coerced_args: Vec<TypedExpr> = typed_args
                    .into_iter()
                    .zip(signature.params.iter())
                    .map(|(arg, expected)| maybe_coerce_to(arg, expected))
                    .collect();
                let nullable =
                    signature.nullable || coerced_args.iter().any(|a| a.nullable);
                let result_type = signature.return_type.clone();
                Ok(TypedExpr {
                    kind: TypedExprKind::FunctionCall {
                        signature,
                        args: coerced_args,
                    },
                    result_type,
                    nullable,
                    span: expr.span,
                })
            }

            Expr::Paren(inner) => TypedExpr::from_ast(inner, schema, registry),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Per-variant type-check helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Turn an AST literal into a typed expression, pinning the
/// `PropertyValue` representation and the result type.
fn typed_literal(lit: Literal, span: Span) -> TypedExpr {
    let (value, result_type, nullable) = match lit {
        Literal::Null => (PropertyValue::Null, BqlType::String, true),
        Literal::Bool(b) => (PropertyValue::Bool(b), BqlType::Bool, false),
        Literal::Int(i) => (PropertyValue::Int(i), BqlType::Int, false),
        Literal::Float(f) => (PropertyValue::Float(f), BqlType::Float, false),
        Literal::String(s) => (PropertyValue::String(s), BqlType::String, false),
        Literal::Duration(ns) => (PropertyValue::Int(ns), BqlType::Int, false),
        Literal::Timestamp(ts) => (PropertyValue::Timestamp(ts), BqlType::Timestamp, false),
    };
    TypedExpr {
        kind: TypedExprKind::Literal(value),
        result_type,
        nullable,
        span,
    }
}

/// Check an arithmetic binary op — `+`, `-`, `*`, `/`, `%`.
fn type_check_arith(
    op: BinaryOp,
    mut left: TypedExpr,
    mut right: TypedExpr,
    span: Span,
) -> Result<TypedExpr> {
    // Timestamp arithmetic per §4.4:
    //   Timestamp - Timestamp = Int (nanos delta)
    //   Timestamp +/- Int     = Timestamp
    //   Int     +   Timestamp = Timestamp (commutative add only)
    let result_type = match (&left.result_type, &right.result_type, op) {
        (BqlType::Timestamp, BqlType::Timestamp, BinaryOp::Subtract) => BqlType::Int,
        (BqlType::Timestamp, BqlType::Int, BinaryOp::Add | BinaryOp::Subtract) => {
            BqlType::Timestamp
        }
        (BqlType::Int, BqlType::Timestamp, BinaryOp::Add) => BqlType::Timestamp,

        // Numeric arithmetic with Int → Float promotion.
        (BqlType::Int, BqlType::Int, _) => BqlType::Int,
        (BqlType::Float, BqlType::Float, _) => BqlType::Float,
        (BqlType::Int, BqlType::Float, _) => {
            left = coerce_to_float(left);
            BqlType::Float
        }
        (BqlType::Float, BqlType::Int, _) => {
            right = coerce_to_float(right);
            BqlType::Float
        }

        // Anything else is a type error.
        (l, r, _) => {
            return Err(type_error(format!(
                "type mismatch: arithmetic `{op:?}` requires numeric operands, \
                 got `{l}` and `{r}` (span {span:?})"
            )));
        }
    };

    let nullable = left.nullable || right.nullable;
    Ok(TypedExpr {
        kind: TypedExprKind::Arith {
            op,
            left: Box::new(left),
            right: Box::new(right),
        },
        result_type,
        nullable,
        span,
    })
}

/// Check a unary arithmetic op — `-x`, `+x`.
fn type_check_unary(op: UnaryOp, operand: TypedExpr, span: Span) -> Result<TypedExpr> {
    if !matches!(operand.result_type, BqlType::Int | BqlType::Float) {
        return Err(type_error(format!(
            "type mismatch: unary `{op:?}` requires a numeric operand, \
             got `{}` (span {span:?})",
            operand.result_type
        )));
    }
    let result_type = operand.result_type.clone();
    let nullable = operand.nullable;
    Ok(TypedExpr {
        kind: TypedExprKind::Unary {
            op,
            operand: Box::new(operand),
        },
        result_type,
        nullable,
        span,
    })
}

/// Check a comparison op — `=`, `!=`, `<`, ... . Result is always
/// `Bool`. Follows §4.3: when one side is a literal and the other a
/// column, the literal is coerced to the column's type at plan time.
fn type_check_compare(
    op: CompareOp,
    mut left: TypedExpr,
    mut right: TypedExpr,
    span: Span,
) -> Result<TypedExpr> {
    // Step 1: if one side is a column and the other a literal,
    // coerce the literal to the column's type.
    let left_is_col = matches!(left.kind, TypedExprKind::Column { .. });
    let right_is_col = matches!(right.kind, TypedExprKind::Column { .. });
    let left_is_lit = matches!(left.kind, TypedExprKind::Literal(_));
    let right_is_lit = matches!(right.kind, TypedExprKind::Literal(_));
    if left_is_col && right_is_lit && left.result_type != right.result_type {
        right = coerce_literal_expr(right, &left.result_type)?;
    } else if right_is_col && left_is_lit && right.result_type != left.result_type {
        left = coerce_literal_expr(left, &right.result_type)?;
    }

    // Step 2: types must match modulo Int → Float promotion.
    if left.result_type != right.result_type {
        let l_ty = left.result_type.clone();
        let r_ty = right.result_type.clone();
        match (&l_ty, &r_ty) {
            (BqlType::Int, BqlType::Float) => {
                left = coerce_to_float(left);
            }
            (BqlType::Float, BqlType::Int) => {
                right = coerce_to_float(right);
            }
            _ => {
                return Err(type_error(format!(
                    "type mismatch: comparison `{op:?}` requires matching types, \
                     got `{l_ty}` and `{r_ty}` (span {span:?})"
                )));
            }
        }
    }

    let nullable = left.nullable || right.nullable;
    Ok(TypedExpr {
        kind: TypedExprKind::Compare {
            op,
            left: Box::new(left),
            right: Box::new(right),
        },
        result_type: BqlType::Bool,
        nullable,
        span,
    })
}

/// Type-check a list of operands all required to produce `Bool`.
/// Used by `AND` and `OR`.
fn type_check_bool_list(
    operands: &[Spanned<Expr>],
    schema: &OperatorSchema,
    registry: &FunctionRegistry,
    kind: &'static str,
    span: Span,
) -> Result<Vec<TypedExpr>> {
    if operands.is_empty() {
        return Err(type_error(format!(
            "internal: empty `{kind}` operand list (parser bug — every `{kind}` \
             should have at least two operands) at span {span:?}"
        )));
    }
    operands
        .iter()
        .map(|op| {
            let typed = TypedExpr::from_ast(op, schema, registry)?;
            if typed.result_type != BqlType::Bool {
                return Err(type_mismatch(
                    kind,
                    &BqlType::Bool,
                    &typed.result_type,
                    op.span,
                ));
            }
            Ok(typed)
        })
        .collect()
}

/// Lower an AST `Like` / `Regex` form into a `FunctionCall` on the
/// named built-in. Both AST shapes share this code path; `Like`'s
/// `negated` flag wraps the call in `Not`.
fn lower_string_fn(
    input: &Spanned<Expr>,
    pattern: &str,
    negated: bool,
    function_name: &str,
    schema: &OperatorSchema,
    registry: &FunctionRegistry,
    span: Span,
) -> Result<TypedExpr> {
    let typed_input = TypedExpr::from_ast(input, schema, registry)?;
    if typed_input.result_type != BqlType::String {
        return Err(type_mismatch(
            function_name,
            &BqlType::String,
            &typed_input.result_type,
            input.span,
        ));
    }
    let signature = registry
        .resolve(function_name, &[BqlType::String, BqlType::String])
        .ok_or_else(|| {
            type_error(format!(
                "built-in function `{function_name}` is not registered in the function registry"
            ))
        })?;
    let input_nullable = typed_input.nullable;
    let nullable = signature.nullable || input_nullable;
    let result_type = signature.return_type.clone();
    let pattern_literal = TypedExpr {
        kind: TypedExprKind::Literal(PropertyValue::String(pattern.to_string())),
        result_type: BqlType::String,
        nullable: false,
        span,
    };
    let call = TypedExpr {
        kind: TypedExprKind::FunctionCall {
            signature,
            args: vec![typed_input, pattern_literal],
        },
        result_type,
        nullable,
        span,
    };
    if negated {
        Ok(TypedExpr {
            kind: TypedExprKind::Not(Box::new(call)),
            result_type: BqlType::Bool,
            nullable,
            span,
        })
    } else {
        Ok(call)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Coercion helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Wrap an expression in [`TypedExprKind::ImplicitCoerce`] with the
/// target `Float`, bumping the result type. Used by arithmetic and
/// comparison rules during Int → Float promotion.
fn coerce_to_float(expr: TypedExpr) -> TypedExpr {
    let nullable = expr.nullable;
    let span = expr.span;
    TypedExpr {
        kind: TypedExprKind::ImplicitCoerce {
            input: Box::new(expr),
            target_type: BqlType::Float,
        },
        result_type: BqlType::Float,
        nullable,
        span,
    }
}

/// Apply an implicit Int→Float coercion when the expected function
/// parameter is `Float` and the actual argument is `Int`. All other
/// pairings pass through unchanged.
fn maybe_coerce_to(expr: TypedExpr, expected: &BqlType) -> TypedExpr {
    if matches!(expected, BqlType::Float) && matches!(expr.result_type, BqlType::Int) {
        coerce_to_float(expr)
    } else {
        expr
    }
}

/// Re-coerce a literal-kind `TypedExpr` to a new target type. Used
/// by comparison type-checking when one side is a column with a
/// different type than the literal. The new `TypedExpr` carries the
/// same span and is non-nullable unless the literal was `Null`.
fn coerce_literal_expr(mut expr: TypedExpr, target: &BqlType) -> Result<TypedExpr> {
    let TypedExprKind::Literal(value) = &expr.kind else {
        return Err(type_error(
            "internal: coerce_literal_expr called on non-literal (planner bug)".to_string(),
        ));
    };
    if matches!(value, PropertyValue::Null) {
        expr.result_type = target.clone();
        return Ok(expr);
    }
    let coerced = value.coerce_to(target).ok_or_else(|| {
        type_error(format!(
            "coercion failed: cannot coerce literal value to `{target}` for comparison"
        ))
    })?;
    expr.kind = TypedExprKind::Literal(coerced);
    expr.result_type = target.clone();
    Ok(expr)
}

/// Coerce a raw AST [`Literal`] directly to a `PropertyValue` of the
/// given target type. Used by `InLiteralSet` which wants the coerced
/// value, not a `TypedExpr`.
fn coerce_literal(literal: Literal, target: &BqlType, span: Span) -> Result<PropertyValue> {
    let raw = match literal {
        Literal::Null => return Ok(PropertyValue::Null),
        Literal::Bool(b) => PropertyValue::Bool(b),
        Literal::Int(i) => PropertyValue::Int(i),
        Literal::Float(f) => PropertyValue::Float(f),
        Literal::String(s) => PropertyValue::String(s),
        Literal::Duration(ns) => PropertyValue::Int(ns),
        Literal::Timestamp(ts) => PropertyValue::Timestamp(ts),
    };
    raw.coerce_to(target).ok_or_else(|| {
        type_error(format!(
            "coercion failed: cannot coerce literal to `{target}` (span {span:?})"
        ))
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// Error-construction helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Wrap a plan-time type error in [`BqliteError::Plan`] so callers
/// can pattern-match on the `Plan` variant until the full
/// `TypeError` enum lands.
fn type_error(msg: String) -> BqliteError {
    BqliteError::Plan(msg)
}

fn unknown_column(name: &str, span: Span) -> BqliteError {
    type_error(format!(
        "unknown column: `{name}` is not in the operator's input schema (span {span:?})"
    ))
}

fn unsupported(msg: &str, span: Span) -> BqliteError {
    type_error(format!("unsupported: {msg} (span {span:?})"))
}

fn type_mismatch(context: &str, expected: &BqlType, actual: &BqlType, span: Span) -> BqliteError {
    type_error(format!(
        "type mismatch: `{context}` requires `{expected}`, got `{actual}` (span {span:?})"
    ))
}

fn no_matching_overload(name: &str, arg_types: &[BqlType], span: Span) -> BqliteError {
    let args = arg_types
        .iter()
        .map(|t| t.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    type_error(format!(
        "no matching overload: function `{name}` has no signature accepting `({args})` (span {span:?})"
    ))
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use bqlite_ast::span::Name;
    use bqlite_core::{ColumnDef, OperatorSchema};

    fn schema_with(columns: Vec<ColumnDef>) -> OperatorSchema {
        OperatorSchema::new(columns).unwrap()
    }

    fn purchases_schema() -> OperatorSchema {
        schema_with(vec![
            ColumnDef::required("user_id", BqlType::String),
            ColumnDef::required("ts", BqlType::Timestamp),
            ColumnDef::required("event", BqlType::String),
            ColumnDef::nullable("amount", BqlType::Float),
            ColumnDef::nullable("count", BqlType::Int),
        ])
    }

    fn spanned<T>(node: T) -> Spanned<T> {
        Spanned::new(node, Span::EMPTY)
    }

    fn column(name: &str) -> Spanned<Expr> {
        spanned(Expr::Column(Name::synthetic(name)))
    }

    fn int_lit(i: i64) -> Spanned<Expr> {
        spanned(Expr::Literal(Literal::Int(i)))
    }

    fn float_lit(f: f64) -> Spanned<Expr> {
        spanned(Expr::Literal(Literal::Float(f)))
    }

    fn str_lit(s: &str) -> Spanned<Expr> {
        spanned(Expr::Literal(Literal::String(s.into())))
    }

    fn bool_lit(b: bool) -> Spanned<Expr> {
        spanned(Expr::Literal(Literal::Bool(b)))
    }

    fn from_ast_with_purchases(expr: Spanned<Expr>) -> Result<TypedExpr> {
        let schema = purchases_schema();
        let reg = FunctionRegistry::with_builtins();
        TypedExpr::from_ast(&expr, &schema, &reg)
    }

    // ── Literals ─────────────────────────────────────────────────────────────

    #[test]
    fn literal_int_has_int_type_and_is_not_nullable() {
        let t = from_ast_with_purchases(int_lit(42)).unwrap();
        assert_eq!(t.result_type, BqlType::Int);
        assert!(!t.nullable);
        assert!(matches!(
            t.kind,
            TypedExprKind::Literal(PropertyValue::Int(42))
        ));
    }

    #[test]
    fn literal_null_is_nullable() {
        let t = from_ast_with_purchases(spanned(Expr::Literal(Literal::Null))).unwrap();
        assert!(t.nullable);
    }

    #[test]
    fn literal_duration_folds_to_int_nanos() {
        let t = from_ast_with_purchases(spanned(Expr::Literal(Literal::Duration(
            7 * 86_400_000_000_000,
        ))))
        .unwrap();
        assert_eq!(t.result_type, BqlType::Int);
        match t.kind {
            TypedExprKind::Literal(PropertyValue::Int(ns)) => {
                assert_eq!(ns, 7 * 86_400_000_000_000);
            }
            other => panic!("expected Int literal, got {other:?}"),
        }
    }

    // ── Column references ──────────────────────────────────────────────────

    #[test]
    fn column_reference_resolves_type_and_nullability() {
        let t = from_ast_with_purchases(column("amount")).unwrap();
        assert_eq!(t.result_type, BqlType::Float);
        assert!(t.nullable); // amount is declared nullable
        match t.kind {
            TypedExprKind::Column { column_index, name } => {
                assert_eq!(column_index, 3);
                assert_eq!(name, "amount");
            }
            other => panic!("expected Column, got {other:?}"),
        }
    }

    #[test]
    fn non_nullable_column_produces_non_nullable_typed_expr() {
        let t = from_ast_with_purchases(column("user_id")).unwrap();
        assert!(!t.nullable);
    }

    #[test]
    fn unknown_column_is_a_type_error() {
        let err = from_ast_with_purchases(column("ghost")).unwrap_err();
        match err {
            BqliteError::Plan(msg) => {
                assert!(msg.contains("unknown column"));
                assert!(msg.contains("ghost"));
            }
            other => panic!("expected Plan error, got {other:?}"),
        }
    }

    // ── Arithmetic type rules ──────────────────────────────────────────────

    #[test]
    fn int_plus_int_is_int() {
        let expr = spanned(Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(int_lit(1)),
            right: Box::new(int_lit(2)),
        });
        let t = from_ast_with_purchases(expr).unwrap();
        assert_eq!(t.result_type, BqlType::Int);
    }

    #[test]
    fn int_plus_float_promotes_to_float_with_implicit_coerce() {
        let expr = spanned(Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(int_lit(1)),
            right: Box::new(float_lit(2.5)),
        });
        let t = from_ast_with_purchases(expr).unwrap();
        assert_eq!(t.result_type, BqlType::Float);
        match t.kind {
            TypedExprKind::Arith { left, .. } => {
                // The int operand should be wrapped in ImplicitCoerce.
                assert!(matches!(left.kind, TypedExprKind::ImplicitCoerce { .. }));
            }
            other => panic!("expected Arith, got {other:?}"),
        }
    }

    #[test]
    fn timestamp_minus_timestamp_is_int() {
        let expr = spanned(Expr::Binary {
            op: BinaryOp::Subtract,
            left: Box::new(column("ts")),
            right: Box::new(column("ts")),
        });
        let t = from_ast_with_purchases(expr).unwrap();
        assert_eq!(t.result_type, BqlType::Int);
    }

    #[test]
    fn timestamp_plus_int_is_timestamp() {
        let expr = spanned(Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(column("ts")),
            right: Box::new(spanned(Expr::Literal(Literal::Duration(
                86_400_000_000_000,
            )))),
        });
        let t = from_ast_with_purchases(expr).unwrap();
        assert_eq!(t.result_type, BqlType::Timestamp);
    }

    #[test]
    fn arith_on_string_is_type_error() {
        let expr = spanned(Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(column("user_id")),
            right: Box::new(int_lit(1)),
        });
        let err = from_ast_with_purchases(expr).unwrap_err();
        match err {
            BqliteError::Plan(msg) => assert!(msg.contains("type mismatch")),
            other => panic!("expected Plan error, got {other:?}"),
        }
    }

    // ── Unary ──────────────────────────────────────────────────────────────

    #[test]
    fn unary_negate_on_int_returns_int() {
        let expr = spanned(Expr::Unary {
            op: UnaryOp::Negate,
            operand: Box::new(int_lit(5)),
        });
        let t = from_ast_with_purchases(expr).unwrap();
        assert_eq!(t.result_type, BqlType::Int);
    }

    #[test]
    fn unary_negate_on_string_is_type_error() {
        let expr = spanned(Expr::Unary {
            op: UnaryOp::Negate,
            operand: Box::new(str_lit("x")),
        });
        assert!(matches!(
            from_ast_with_purchases(expr),
            Err(BqliteError::Plan(_))
        ));
    }

    // ── Comparison + literal-to-column coercion ────────────────────────────

    #[test]
    fn column_equals_literal_is_bool() {
        let expr = spanned(Expr::Compare {
            op: CompareOp::Equal,
            left: Box::new(column("event")),
            right: Box::new(str_lit("checkout")),
        });
        let t = from_ast_with_purchases(expr).unwrap();
        assert_eq!(t.result_type, BqlType::Bool);
        assert!(!t.nullable); // user_id, event are non-nullable
    }

    #[test]
    fn column_greater_than_int_on_float_column_coerces_literal() {
        // `amount > 100` — amount is Float, literal is Int.
        let expr = spanned(Expr::Compare {
            op: CompareOp::Greater,
            left: Box::new(column("amount")),
            right: Box::new(int_lit(100)),
        });
        let t = from_ast_with_purchases(expr).unwrap();
        assert_eq!(t.result_type, BqlType::Bool);
        assert!(t.nullable); // amount is nullable
                             // The literal should have been coerced to Float.
        match &t.kind {
            TypedExprKind::Compare { right, .. } => {
                assert_eq!(right.result_type, BqlType::Float);
                match &right.kind {
                    TypedExprKind::Literal(PropertyValue::Float(f)) => assert_eq!(*f, 100.0),
                    other => panic!("expected Float literal, got {other:?}"),
                }
            }
            other => panic!("expected Compare, got {other:?}"),
        }
    }

    #[test]
    fn compare_mismatched_types_without_literal_coerce_is_error() {
        // `event = amount` — String vs Float, neither side is a literal.
        let expr = spanned(Expr::Compare {
            op: CompareOp::Equal,
            left: Box::new(column("event")),
            right: Box::new(column("amount")),
        });
        let err = from_ast_with_purchases(expr).unwrap_err();
        match err {
            BqliteError::Plan(msg) => assert!(msg.contains("type mismatch")),
            other => panic!("expected Plan error, got {other:?}"),
        }
    }

    // ── AND / OR / NOT ──────────────────────────────────────────────────────

    #[test]
    fn and_of_comparisons_is_bool() {
        // event = 'checkout' AND amount > 100
        let expr = spanned(Expr::And(vec![
            spanned(Expr::Compare {
                op: CompareOp::Equal,
                left: Box::new(column("event")),
                right: Box::new(str_lit("checkout")),
            }),
            spanned(Expr::Compare {
                op: CompareOp::Greater,
                left: Box::new(column("amount")),
                right: Box::new(int_lit(100)),
            }),
        ]));
        let t = from_ast_with_purchases(expr).unwrap();
        assert_eq!(t.result_type, BqlType::Bool);
    }

    #[test]
    fn and_with_non_bool_operand_is_error() {
        let expr = spanned(Expr::And(vec![bool_lit(true), int_lit(1)]));
        let err = from_ast_with_purchases(expr).unwrap_err();
        match err {
            BqliteError::Plan(msg) => assert!(msg.contains("type mismatch")),
            other => panic!("expected Plan error, got {other:?}"),
        }
    }

    #[test]
    fn not_of_bool_is_bool() {
        let expr = spanned(Expr::Not(Box::new(bool_lit(true))));
        let t = from_ast_with_purchases(expr).unwrap();
        assert_eq!(t.result_type, BqlType::Bool);
    }

    #[test]
    fn not_of_int_is_type_error() {
        let expr = spanned(Expr::Not(Box::new(int_lit(1))));
        assert!(matches!(
            from_ast_with_purchases(expr),
            Err(BqliteError::Plan(_))
        ));
    }

    // ── IS NULL ─────────────────────────────────────────────────────────────

    #[test]
    fn is_null_on_any_type_is_bool_and_non_nullable() {
        let expr = spanned(Expr::IsNull {
            expr: Box::new(column("amount")),
            negated: false,
        });
        let t = from_ast_with_purchases(expr).unwrap();
        assert_eq!(t.result_type, BqlType::Bool);
        assert!(!t.nullable); // §4.4: IS NULL / IS NOT NULL always produce T/F
    }

    #[test]
    fn is_not_null_preserves_negated_flag() {
        let expr = spanned(Expr::IsNull {
            expr: Box::new(column("amount")),
            negated: true,
        });
        let t = from_ast_with_purchases(expr).unwrap();
        match t.kind {
            TypedExprKind::IsNull { negated, .. } => assert!(negated),
            other => panic!("expected IsNull, got {other:?}"),
        }
    }

    // ── BETWEEN desugar ────────────────────────────────────────────────────

    #[test]
    fn between_lowers_to_and_of_comparisons() {
        let expr = spanned(Expr::Between {
            expr: Box::new(column("amount")),
            low: Box::new(float_lit(10.0)),
            high: Box::new(float_lit(100.0)),
            negated: false,
        });
        let t = from_ast_with_purchases(expr).unwrap();
        assert_eq!(t.result_type, BqlType::Bool);
        // Desugared form is `And(amount >= 10, amount <= 100)`.
        assert!(matches!(t.kind, TypedExprKind::And(_)));
    }

    #[test]
    fn not_between_wraps_in_not() {
        let expr = spanned(Expr::Between {
            expr: Box::new(column("amount")),
            low: Box::new(float_lit(10.0)),
            high: Box::new(float_lit(100.0)),
            negated: true,
        });
        let t = from_ast_with_purchases(expr).unwrap();
        assert!(matches!(t.kind, TypedExprKind::Not(_)));
    }

    // ── IN (literal set) ────────────────────────────────────────────────────

    #[test]
    fn in_literal_set_coerces_values_to_column_type() {
        // `amount IN (1, 2, 3)` — amount is Float, literals are Int.
        let expr = spanned(Expr::In {
            lhs: vec![column("amount")],
            rhs: InRhs::List(vec![int_lit(1), int_lit(2), int_lit(3)]),
            negated: false,
        });
        let t = from_ast_with_purchases(expr).unwrap();
        assert_eq!(t.result_type, BqlType::Bool);
        match t.kind {
            TypedExprKind::InLiteralSet { values, .. } => {
                assert_eq!(values.len(), 3);
                for v in values {
                    assert!(matches!(v, PropertyValue::Float(_)));
                }
            }
            other => panic!("expected InLiteralSet, got {other:?}"),
        }
    }

    #[test]
    fn not_in_preserves_negated_flag() {
        let expr = spanned(Expr::In {
            lhs: vec![column("event")],
            rhs: InRhs::List(vec![str_lit("view"), str_lit("click")]),
            negated: true,
        });
        let t = from_ast_with_purchases(expr).unwrap();
        match t.kind {
            TypedExprKind::InLiteralSet { negated, .. } => assert!(negated),
            other => panic!("expected InLiteralSet, got {other:?}"),
        }
    }

    // ── LIKE / REGEX → FunctionCall ─────────────────────────────────────────

    #[test]
    fn like_lowers_to_like_function_call() {
        let expr = spanned(Expr::Like {
            expr: Box::new(column("event")),
            pattern: "check%".into(),
            negated: false,
        });
        let t = from_ast_with_purchases(expr).unwrap();
        assert_eq!(t.result_type, BqlType::Bool);
        match t.kind {
            TypedExprKind::FunctionCall { signature, args } => {
                assert_eq!(signature.name, "like");
                assert_eq!(args.len(), 2);
            }
            other => panic!("expected FunctionCall, got {other:?}"),
        }
    }

    #[test]
    fn not_like_wraps_call_in_not() {
        let expr = spanned(Expr::Like {
            expr: Box::new(column("event")),
            pattern: "check%".into(),
            negated: true,
        });
        let t = from_ast_with_purchases(expr).unwrap();
        assert!(matches!(t.kind, TypedExprKind::Not(_)));
    }

    #[test]
    fn regex_on_non_string_is_type_error() {
        let expr = spanned(Expr::Regex {
            expr: Box::new(column("amount")),
            pattern: ".*".into(),
        });
        assert!(matches!(
            from_ast_with_purchases(expr),
            Err(BqliteError::Plan(_))
        ));
    }

    // ── PAREN passthrough ───────────────────────────────────────────────────

    #[test]
    fn paren_is_transparent() {
        let inner = column("user_id");
        let expr = spanned(Expr::Paren(Box::new(inner.clone())));
        let t_inner = from_ast_with_purchases(inner).unwrap();
        let t_paren = from_ast_with_purchases(expr).unwrap();
        assert_eq!(t_inner.kind, t_paren.kind);
    }

    // ── Rejected Wave 3+ shapes ─────────────────────────────────────────────

    #[test]
    fn variable_reference_is_rejected_in_non_match_context() {
        let expr = spanned(Expr::Variable(Name::synthetic("product")));
        let err = from_ast_with_purchases(expr).unwrap_err();
        match err {
            BqliteError::Plan(msg) => assert!(msg.contains("MATCH")),
            other => panic!("expected Plan error, got {other:?}"),
        }
    }

    #[test]
    fn case_expression_is_rejected_until_wave_3() {
        let expr = spanned(Expr::Case {
            arms: vec![],
            else_expr: None,
        });
        let err = from_ast_with_purchases(expr).unwrap_err();
        match err {
            BqliteError::Plan(msg) => assert!(msg.contains("Wave 3")),
            other => panic!("expected Plan error, got {other:?}"),
        }
    }

    #[test]
    fn qualified_column_is_rejected_until_wave_4() {
        let expr = spanned(Expr::Qualified {
            table: Name::synthetic("orders"),
            column: Name::synthetic("amount"),
        });
        let err = from_ast_with_purchases(expr).unwrap_err();
        match err {
            BqliteError::Plan(msg) => assert!(msg.contains("Wave 4")),
            other => panic!("expected Plan error, got {other:?}"),
        }
    }

    // ── Function registry ──────────────────────────────────────────────────

    #[test]
    fn registry_with_builtins_registers_like_and_regex() {
        let reg = FunctionRegistry::with_builtins();
        assert!(reg
            .resolve("like", &[BqlType::String, BqlType::String])
            .is_some());
        assert!(reg
            .resolve("regex", &[BqlType::String, BqlType::String])
            .is_some());
    }

    #[test]
    fn registry_resolves_case_insensitively() {
        let reg = FunctionRegistry::with_builtins();
        assert!(reg
            .resolve("LIKE", &[BqlType::String, BqlType::String])
            .is_some());
    }

    #[test]
    fn registry_accepts_int_where_float_expected_via_coercion() {
        let mut reg = FunctionRegistry::new();
        reg.register(ScalarFunctionSig {
            name: "round".into(),
            params: vec![BqlType::Float],
            return_type: BqlType::Float,
            nullable: false,
        });
        assert!(reg.resolve("round", &[BqlType::Int]).is_some());
    }

    #[test]
    fn registry_rejects_arity_mismatch() {
        let reg = FunctionRegistry::with_builtins();
        assert!(reg.resolve("like", &[BqlType::String]).is_none());
    }

    #[test]
    fn unknown_function_is_type_error() {
        let schema = purchases_schema();
        let reg = FunctionRegistry::with_builtins();
        let expr = spanned(Expr::FunctionCall {
            name: Name::synthetic("nosuch"),
            args: vec![],
            over: None,
        });
        let err = TypedExpr::from_ast(&expr, &schema, &reg).unwrap_err();
        match err {
            BqliteError::Plan(msg) => assert!(msg.contains("no matching overload")),
            other => panic!("expected Plan error, got {other:?}"),
        }
    }
}
