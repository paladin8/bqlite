//! # bqlite-ast
//!
//! Abstract syntax tree types for the BQL query language.
//!
//! This crate defines the AST nodes produced by both the BQL text
//! parser (`bqlite-parser`) and the Rust/Python builder APIs. Both
//! entry points produce the same [`Statement`], which is then consumed
//! by the planner (`bqlite-planner`).
//!
//! The AST is deliberately *untyped* — expressions are syntactic shapes
//! only, with no schema resolution or type checking. The planner wraps
//! these nodes in a separate `TypedExpr` during schema validation
//! (docs/design/planner-pipeline.md §4.6). Keeping type concerns out
//! of the AST is what lets the parser run without a catalog and what
//! lets the builder API share this same type surface.
//!
//! ## Module layout
//!
//! - [`span`] — source spans and identifiers ([`Span`], [`Name`]).
//! - [`expr`] — expression language ([`Expr`], [`Literal`], binary /
//!   unary / comparison ops, `CASE`, `CAST`, function calls, `IN`,
//!   `LIKE`, window `OVER` clauses, `ORDER BY` items).
//! - [`pattern`] — `MATCH` pattern nodes ([`MatchPattern`], [`Step`],
//!   alternation, repetition, negation, brackets).
//! - [`operator`] — pipeline stage types ([`PipelineStage`] and the
//!   per-stage structs: [`SelectItem`], [`AggItem`], [`Funnel`],
//!   [`Sessionize`], etc.).
//! - [`pipeline`] — [`Pipeline`], [`Source`], [`TableRef`], and
//!   [`TimeRange`].
//! - [`statement`] — top-level [`Statement`] and DDL/DML types.
//!
//! ## Source spans
//!
//! Every AST node carries a [`Span`] pointing into the original BQL
//! source text. Large composite types store the span directly; leaf
//! expressions use the [`expr::Spanned<T>`] wrapper so the enum itself
//! does not pay a per-variant storage cost. Synthetic nodes (produced
//! by desugaring, tests, or builder APIs) use [`Span::EMPTY`].
//!
//! ## Relationship to `bqlite-core`
//!
//! The AST reuses [`bqlite_core::BqlType`] as the type language for
//! `CAST` targets and `CREATE TABLE` column definitions — the AST does
//! not redefine a parallel type enum. It does define its own
//! [`expr::Literal`], distinct from [`bqlite_core::PropertyValue`],
//! because AST literals carry source-level distinctions (e.g.
//! [`expr::Literal::Duration`] vs [`expr::Literal::Int`]) that
//! `PropertyValue` collapses.

pub mod expr;
pub mod operator;
pub mod pattern;
pub mod pipeline;
pub mod span;
pub mod statement;

pub use expr::{
    BinaryOp, CaseArm, CompareOp, Expr, InRhs, Literal, OrderItem, OverClause, SortDir, Spanned,
    UnaryOp,
};
pub use operator::{
    AggItem, Attribute, EventSelect, EventSelectKind, Funnel, GroupItem, PipelineStage, Retention,
    Sample, SampleSpec, SelectItem, SelectItemKind, Sessionize,
};
pub use pattern::{
    BracketSpec, EventRef, Exclusion, MatchMode, MatchPattern, MatchWindow, Repetition, Step,
    StepEvent,
};
pub use pipeline::{Pipeline, Source, TableRef, TimeRange};
pub use span::{Name, Span};
pub use statement::{
    AlterAction, AlterTableStmt, ColumnDef, ColumnMapping, ColumnRole, CreateTableStmt, DeleteStmt,
    DescribeStmt, DropTableStmt, InsertBody, InsertOption, InsertStmt, Statement,
};
