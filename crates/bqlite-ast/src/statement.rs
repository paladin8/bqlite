//! Top-level statement AST — queries, DDL, DML, and alias definitions.
//!
//! Every complete BQL input parses to a [`Statement`]. A statement is
//! one of:
//!
//! - A query pipeline (the common case — `Statement::Query`).
//! - An `EXPLAIN` of a pipeline.
//! - DDL: `CREATE TABLE`, `ALTER TABLE`, `DROP TABLE`, `DESCRIBE`.
//! - DML: `INSERT INTO`, `DELETE FROM`.
//! - An alias definition: `name = pipeline` — a named intermediate
//!   result reusable by later statements in the same script
//!   (query-language.md §22).
//!
//! DDL / DML type references reuse [`BqlType`] from `bqlite-core` so
//! the AST does not duplicate the canonical type enum.

use bqlite_core::BqlType;
use serde::{Deserialize, Serialize};

use crate::expr::{Expr, Literal, Spanned};
use crate::pipeline::Pipeline;
use crate::span::{Name, Span};

/// A single BQL statement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Statement {
    /// A query pipeline — the most common statement form.
    Query(Pipeline),

    /// `EXPLAIN <pipeline>` — return the execution plan without
    /// running it. DDL/DML are not EXPLAIN-able in v1
    /// (query-language.md §25).
    Explain(Pipeline),

    /// `DESCRIBE <table>` — emit the catalog schema for a named table.
    Describe(DescribeStmt),

    /// `CREATE TABLE <name> (...)`.
    CreateTable(CreateTableStmt),

    /// `ALTER TABLE <name> ADD COLUMN ...` — v1 only supports ADD COLUMN.
    AlterTable(AlterTableStmt),

    /// `DROP TABLE <name>`.
    DropTable(DropTableStmt),

    /// `INSERT INTO <table> ...`.
    Insert(InsertStmt),

    /// `DELETE FROM <table> WHERE <predicate>`.
    Delete(DeleteStmt),

    /// `<name> = <pipeline>` — define a named pipeline alias that
    /// subsequent statements can reference (query-language.md §22).
    DefineAlias {
        name: Name,
        body: Pipeline,
        span: Span,
    },
}

/// `DESCRIBE <table>` — returns the catalog schema for a table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DescribeStmt {
    pub table: Name,
    pub span: Span,
}

/// `CREATE TABLE <name> (<columns>)`.
///
/// Column definitions follow docs/design/type-system.md §5 — every
/// table must designate an entity-key, event-time, and event-type
/// column. The AST does not validate this; the planner enforces the
/// rule at plan time. The grammar (§26 line 1640) has no
/// `IF NOT EXISTS` modifier.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateTableStmt {
    pub table: Name,
    pub columns: Vec<ColumnDef>,
    pub span: Span,
}

/// A column definition inside `CREATE TABLE`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: Name,
    pub data_type: BqlType,
    /// Column role: ENTITY KEY, EVENT TIME, EVENT TYPE, or regular.
    pub role: ColumnRole,
    /// Whether the column is declared `NOT NULL`.
    pub not_null: bool,
    /// Optional `DEFAULT <literal>` for the column.
    pub default: Option<Literal>,
    pub span: Span,
}

/// The semantic role of a table column — see type-system.md §5.1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ColumnRole {
    /// A regular property column with no special role.
    Regular,
    /// The entity-id column (exactly one per table).
    EntityKey,
    /// The event timestamp column (exactly one per table).
    EventTime,
    /// The event-type column (exactly one per table).
    EventType,
}

/// `ALTER TABLE <name> ADD COLUMN ...` — the only ALTER form in v1.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AlterTableStmt {
    pub table: Name,
    pub action: AlterAction,
    pub span: Span,
}

/// A single ALTER action. v1 only supports ADD COLUMN, but the enum
/// makes room for future ALTER variants without shape changes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AlterAction {
    AddColumn(ColumnDef),
}

/// `DROP TABLE <name>`.
///
/// The grammar (§26 line 1642) has no `IF EXISTS` modifier — a drop
/// of a non-existent table is a planner error.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DropTableStmt {
    pub table: Name,
    pub span: Span,
}

/// `INSERT INTO <table> <body>`.
///
/// v1 INSERT takes positional literal tuples only — no column list,
/// no computed expressions (§20.1, grammar §26 line 1631).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InsertStmt {
    pub table: Name,
    pub body: InsertBody,
    pub span: Span,
}

/// The source of rows for an `INSERT` statement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum InsertBody {
    /// `VALUES (a, b), (c, d)` — one or more literal rows.
    ///
    /// v1 restricts the right-hand side of VALUES to literals only —
    /// no expressions, no subqueries — per query-language.md §20.
    Values(Vec<Vec<Literal>>),
    /// `FROM '<path>' [WITH (opt: val, ...)]` — bulk load.
    ///
    /// The grammar has no dedicated `FORMAT` keyword: format (`csv`,
    /// `jsonl`, …) is either inferred from the file extension or
    /// passed as a `format: 'csv'` entry in the `WITH (...)` option
    /// list like any other option (§26 line 1634).
    From {
        path: String,
        options: Vec<InsertOption>,
    },
}

/// A single option on an `INSERT ... FROM` statement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InsertOption {
    pub key: Name,
    pub value: Literal,
    pub span: Span,
}

/// `DELETE FROM <table> WHERE <predicate>`.
///
/// The predicate is mandatory — unbounded deletes are rejected at
/// parse time.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeleteStmt {
    pub table: Name,
    pub predicate: Spanned<Expr>,
    pub span: Span,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::PipelineStage;
    use crate::pipeline::{Source, TableRef};

    fn empty_pipeline() -> Pipeline {
        Pipeline {
            source: Source {
                primary: TableRef {
                    name: Name::synthetic("events"),
                    span: Span::EMPTY,
                },
                joins: vec![],
                time_range: None,
                span: Span::EMPTY,
            },
            stages: vec![],
            span: Span::EMPTY,
        }
    }

    #[test]
    fn construct_query_statement() {
        let s = Statement::Query(empty_pipeline());
        match s {
            Statement::Query(p) => assert_eq!(p.source.primary.name.text, "events"),
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn construct_explain_statement() {
        let s = Statement::Explain(empty_pipeline());
        match s {
            Statement::Explain(_) => (),
            _ => panic!("expected Explain"),
        }
    }

    #[test]
    fn create_table_with_role_columns() {
        let s = Statement::CreateTable(CreateTableStmt {
            table: Name::synthetic("events"),
            columns: vec![
                ColumnDef {
                    name: Name::synthetic("user_id"),
                    data_type: BqlType::String,
                    role: ColumnRole::EntityKey,
                    not_null: true,
                    default: None,
                    span: Span::EMPTY,
                },
                ColumnDef {
                    name: Name::synthetic("ts"),
                    data_type: BqlType::Timestamp,
                    role: ColumnRole::EventTime,
                    not_null: true,
                    default: None,
                    span: Span::EMPTY,
                },
                ColumnDef {
                    name: Name::synthetic("event"),
                    data_type: BqlType::String,
                    role: ColumnRole::EventType,
                    not_null: true,
                    default: None,
                    span: Span::EMPTY,
                },
                ColumnDef {
                    name: Name::synthetic("amount"),
                    data_type: BqlType::Float,
                    role: ColumnRole::Regular,
                    not_null: false,
                    default: None,
                    span: Span::EMPTY,
                },
            ],
            span: Span::EMPTY,
        });
        match s {
            Statement::CreateTable(c) => {
                assert_eq!(c.columns.len(), 4);
                assert!(matches!(c.columns[0].role, ColumnRole::EntityKey));
            }
            _ => panic!("expected CreateTable"),
        }
    }

    #[test]
    fn insert_values_body() {
        let s = Statement::Insert(InsertStmt {
            table: Name::synthetic("events"),
            body: InsertBody::Values(vec![
                vec![Literal::Int(1), Literal::String("x".into())],
                vec![Literal::Int(2), Literal::String("y".into())],
            ]),
            span: Span::EMPTY,
        });
        match s {
            Statement::Insert(i) => match i.body {
                InsertBody::Values(rows) => assert_eq!(rows.len(), 2),
                _ => panic!("expected Values body"),
            },
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn insert_from_file_with_format_option() {
        // Format is carried as a plain `InsertOption`, not a dedicated
        // field — §26 line 1634 has no `FORMAT` keyword.
        let body = InsertBody::From {
            path: "/tmp/data.csv".into(),
            options: vec![
                InsertOption {
                    key: Name::synthetic("format"),
                    value: Literal::String("csv".into()),
                    span: Span::EMPTY,
                },
                InsertOption {
                    key: Name::synthetic("delimiter"),
                    value: Literal::String(",".into()),
                    span: Span::EMPTY,
                },
            ],
        };
        match body {
            InsertBody::From { path, options } => {
                assert_eq!(path, "/tmp/data.csv");
                assert_eq!(options.len(), 2);
                assert_eq!(options[0].key.text, "format");
            }
            _ => panic!("expected From body"),
        }
    }

    #[test]
    fn delete_requires_predicate_by_type() {
        // The `predicate` field is non-optional, so constructing a
        // `DeleteStmt` always produces a value with a predicate —
        // compile-time enforcement that matches the language rule.
        let d = DeleteStmt {
            table: Name::synthetic("events"),
            predicate: Spanned::new(Expr::Literal(Literal::Bool(true)), Span::EMPTY),
            span: Span::EMPTY,
        };
        assert_eq!(d.table.text, "events");
    }

    #[test]
    fn define_alias_holds_pipeline() {
        let s = Statement::DefineAlias {
            name: Name::synthetic("active_users"),
            body: empty_pipeline(),
            span: Span::EMPTY,
        };
        match s {
            Statement::DefineAlias { name, .. } => {
                assert_eq!(name.text, "active_users");
            }
            _ => panic!("expected DefineAlias"),
        }
    }

    #[test]
    fn drop_table_statement() {
        let s = Statement::DropTable(DropTableStmt {
            table: Name::synthetic("events"),
            span: Span::EMPTY,
        });
        match s {
            Statement::DropTable(d) => assert_eq!(d.table.text, "events"),
            _ => panic!("expected DropTable"),
        }
    }

    #[test]
    fn alter_table_add_column() {
        let s = Statement::AlterTable(AlterTableStmt {
            table: Name::synthetic("events"),
            action: AlterAction::AddColumn(ColumnDef {
                name: Name::synthetic("source"),
                data_type: BqlType::String,
                role: ColumnRole::Regular,
                not_null: false,
                default: None,
                span: Span::EMPTY,
            }),
            span: Span::EMPTY,
        });
        match s {
            Statement::AlterTable(a) => match a.action {
                AlterAction::AddColumn(c) => assert_eq!(c.name.text, "source"),
            },
            _ => panic!("expected AlterTable"),
        }
    }

    #[test]
    fn query_with_pipeline_stages() {
        let pipeline = Pipeline {
            source: Source {
                primary: TableRef {
                    name: Name::synthetic("events"),
                    span: Span::EMPTY,
                },
                joins: vec![],
                time_range: None,
                span: Span::EMPTY,
            },
            stages: vec![PipelineStage::Limit {
                count: 10,
                span: Span::EMPTY,
            }],
            span: Span::EMPTY,
        };
        let s = Statement::Query(pipeline);
        match s {
            Statement::Query(p) => assert_eq!(p.stages.len(), 1),
            _ => panic!("expected Query"),
        }
    }
}
