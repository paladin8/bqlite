//! Schema DDL and EXPLAIN productions.
//!
//! Design: `docs/design/query-language.md` §20.4 (Schema DDL), §20.5
//! (DESCRIBE), §20.6 (EXPLAIN), and §26 grammar lines 1665–1675.
//!
//! This module owns every DDL and the `EXPLAIN` statement at the parser
//! level. Semantic validation (multi-role-per-column rejection, type
//! coercion of DEFAULT literals, role-exclusivity, catalog lookups) is
//! the planner's job (TASK-226 / TASK-232) — the parser returns the
//! AST shape unchanged and trusts the planner to enforce meaning.
//!
//! ## Productions landed here
//!
//! - `CREATE TABLE name "(" column_def ("," column_def)* ")"` — §26 line 1665
//! - `ALTER TABLE name ADD COLUMN name type_expr alter_modifier*` — §26 line 1666
//! - `DROP TABLE name` — §26 line 1667
//! - `DESCRIBE name` — §26 line 1668
//! - `EXPLAIN pipeline` — §26 line 1669 (pipelines only; DDL/DML are not
//!   EXPLAIN-able in v1, per §20.6 and the `Statement::Explain(Pipeline)`
//!   AST shape at `crates/bqlite-ast/src/statement.rs:33`)
//!
//! ## Not landed here (out of Wave 2 / TASK-221 scope)
//!
//! - `INSERT ... FROM` and `INSERT ... VALUES` — TASK-222 / TASK-238.
//! - `DELETE` — grammar exists but Wave 4 (tombstones) — the parser
//!   does not accept it yet; attempting to parse `DELETE ...` falls
//!   through to the dispatcher's `ReservedKeyword` error for now.

#![allow(dead_code)] // The dispatcher routes to this module from parser.rs.

use bqlite_ast::{
    AlterAction, AlterTableStmt, BqlType, ColumnDef, ColumnRole, CreateTableStmt, DescribeStmt,
    DropTableStmt, Literal, Span, Statement,
};

use crate::error::{Expected, NameRole, ParseError};
use crate::lex::{token_span, Keyword, TokenKind};
use crate::parser::Parser;

// ----------------------------------------------------------------------
// DROP TABLE
// ----------------------------------------------------------------------

/// Parse `DROP TABLE <name>`.
///
/// The leading `DROP` keyword is already peeked by the dispatcher;
/// this production consumes both `DROP` and `TABLE`. Per §20.4 and
/// grammar line 1667 there is no `IF EXISTS` modifier — dropping a
/// missing table is a planner error.
pub(crate) fn parse_drop_table(p: &mut Parser) -> Result<Statement, ParseError> {
    let drop_tok = p.expect_kw(Keyword::Drop)?;
    let start_span = token_span(&drop_tok);
    p.expect_kw(Keyword::Table)?;
    let name = p.expect_name(NameRole::TableName)?;
    let span = start_span.merged(name.span);
    Ok(Statement::DropTable(DropTableStmt { table: name, span }))
}

// ----------------------------------------------------------------------
// DESCRIBE
// ----------------------------------------------------------------------

/// Parse `DESCRIBE <name>`.
///
/// Leading `DESCRIBE` is already peeked. Output column formatting and
/// metadata assembly happens in the engine (TASK-232); the parser just
/// records the table name and span.
pub(crate) fn parse_describe(p: &mut Parser) -> Result<Statement, ParseError> {
    let describe_tok = p.expect_kw(Keyword::Describe)?;
    let start_span = token_span(&describe_tok);
    let name = p.expect_name(NameRole::TableName)?;
    let span = start_span.merged(name.span);
    Ok(Statement::Describe(DescribeStmt { table: name, span }))
}

// ----------------------------------------------------------------------
// EXPLAIN
// ----------------------------------------------------------------------

/// Parse `EXPLAIN <pipeline>`.
///
/// Per §20.6 and the `Statement::Explain(Pipeline)` AST shape, EXPLAIN
/// only accepts a pipeline body — DDL and DML are not EXPLAIN-able in
/// v1. The function consumes `EXPLAIN` and then delegates to the same
/// pipeline-body parser used by the plain query form, so any
/// pipeline-level error propagates through with its own span.
pub(crate) fn parse_explain(p: &mut Parser) -> Result<Statement, ParseError> {
    p.expect_kw(Keyword::Explain)?;
    // After the `EXPLAIN` keyword, the next token must be a source
    // name (bare identifier / backtick name). Anything else — DDL
    // keywords (`CREATE`, `DROP`, `ALTER`, `DESCRIBE`), DML (`INSERT`,
    // `DELETE`), or another `EXPLAIN` — is rejected with a specific
    // "EXPLAIN accepts only pipelines" detail.
    match p.peek_kind() {
        TokenKind::Ident(_) | TokenKind::QuotedName(_) => {
            let pipeline = crate::parser::parse_pipeline_body(p)?;
            Ok(Statement::Explain(pipeline))
        }
        _ => Err(p.error_unexpected(
            Expected::Name,
            Some("EXPLAIN accepts pipelines only — DDL and DML are not EXPLAIN-able"),
        )),
    }
}

// ----------------------------------------------------------------------
// CREATE TABLE
// ----------------------------------------------------------------------

/// Parse `CREATE TABLE <name> "(" column_def ("," column_def)* ")"`.
///
/// Column-modifier scope follows `query-language.md` §26 line 1674
/// (`ENTITY KEY`, `EVENT TYPE`, `EVENT TIME`, `NOT NULL`, `NULL`) plus
/// an optional `DEFAULT literal` clause per the TASK-221 task
/// description and the `ColumnDef::default` AST field. A fresh revision
/// to §26 lands alongside this code so the grammar and the parser stay
/// in sync. Multi-role-per-column and role-exclusivity checks are
/// planner-side — the parser returns the AST unchanged even for
/// nonsensical combinations like `ENTITY KEY EVENT TYPE`.
pub(crate) fn parse_create_table(p: &mut Parser) -> Result<Statement, ParseError> {
    let create_tok = p.expect_kw(Keyword::Create)?;
    let start_span = token_span(&create_tok);
    p.expect_kw(Keyword::Table)?;
    let table = p.expect_name(NameRole::TableName)?;
    p.expect_punct(&TokenKind::LParen, "(")?;

    // Require at least one column_def. `CREATE TABLE foo ()` is a
    // parse error — the grammar's repetition is `column_def ("," column_def)*`,
    // not `column_def*`.
    let mut columns = Vec::new();
    columns.push(parse_column_def(p, ColumnDefCtx::Create)?);
    while matches!(p.peek_kind(), TokenKind::Comma) {
        p.bump(); // consume `,`
        columns.push(parse_column_def(p, ColumnDefCtx::Create)?);
    }

    let rparen = p.expect_punct(&TokenKind::RParen, ")")?;
    let span = start_span.merged(token_span(&rparen));

    Ok(Statement::CreateTable(CreateTableStmt {
        table,
        columns,
        span,
    }))
}

// ----------------------------------------------------------------------
// ALTER TABLE
// ----------------------------------------------------------------------

/// Parse `ALTER TABLE <name> ADD COLUMN <column_def>`.
///
/// v1 only supports `ADD COLUMN`. The grammar's per-column modifier
/// set for ALTER is `NOT NULL | NULL | DEFAULT literal` — no role
/// modifiers — so the `ColumnDefCtx::Alter` context suppresses the
/// `ENTITY KEY` / `EVENT TYPE` / `EVENT TIME` branches and surfaces a
/// specific error if the user tries to use them.
pub(crate) fn parse_alter_table(p: &mut Parser) -> Result<Statement, ParseError> {
    let alter_tok = p.expect_kw(Keyword::Alter)?;
    let start_span = token_span(&alter_tok);
    p.expect_kw(Keyword::Table)?;
    let table = p.expect_name(NameRole::TableName)?;
    p.expect_kw(Keyword::Add)?;
    p.expect_kw(Keyword::Column)?;
    let column = parse_column_def(p, ColumnDefCtx::Alter)?;
    let span = start_span.merged(column.span);

    Ok(Statement::AlterTable(AlterTableStmt {
        table,
        action: AlterAction::AddColumn(column),
        span,
    }))
}

// ----------------------------------------------------------------------
// Shared column_def + type_expr productions
// ----------------------------------------------------------------------

/// Where a [`ColumnDef`] is being parsed from — drives the modifier
/// vocabulary (role modifiers are only legal inside CREATE TABLE).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ColumnDefCtx {
    /// Inside a `CREATE TABLE` column list.
    Create,
    /// Inside `ALTER TABLE ... ADD COLUMN`.
    Alter,
}

/// Parse `name type_expr column_modifier*`.
///
/// The returned [`ColumnDef`]'s `span` covers from the column name
/// through the last accepted modifier (or through the type if there
/// are no modifiers).
fn parse_column_def(p: &mut Parser, ctx: ColumnDefCtx) -> Result<ColumnDef, ParseError> {
    let name = p.expect_name(NameRole::ColumnName)?;
    let name_span = name.span;
    let (data_type, type_span) = parse_type_expr(p)?;

    let mut role = ColumnRole::Regular;
    let mut not_null = false;
    let mut default: Option<Literal> = None;
    let mut last_span = name_span.merged(type_span);

    loop {
        // Look ahead at the next token without committing — modifier
        // parsing stops cleanly when the next token isn't a recognized
        // modifier keyword. The outer `CREATE TABLE` / `ALTER TABLE`
        // productions resume from there (consuming `,` or `)` or the
        // end of the statement).
        match p.peek_kind() {
            TokenKind::Kw(Keyword::Entity) => {
                if ctx != ColumnDefCtx::Create {
                    return Err(role_not_allowed_here(p));
                }
                let entity_tok = p.bump();
                let key_tok = p.expect_kw(Keyword::Key)?;
                let span = token_span(&entity_tok).merged(token_span(&key_tok));
                role = ColumnRole::EntityKey;
                last_span = last_span.merged(span);
            }
            TokenKind::Kw(Keyword::Event) => {
                if ctx != ColumnDefCtx::Create {
                    return Err(role_not_allowed_here(p));
                }
                let event_tok = p.bump();
                // EVENT TIME or EVENT TYPE
                let next_role = match p.peek_kind() {
                    TokenKind::Kw(Keyword::Time) => ColumnRole::EventTime,
                    TokenKind::Kw(Keyword::Type) => ColumnRole::EventType,
                    _ => {
                        return Err(p.error_unexpected(
                            Expected::Keyword("TIME"),
                            Some("`EVENT` column modifier must be `EVENT TIME` or `EVENT TYPE`"),
                        ));
                    }
                };
                let second = p.bump();
                let span = token_span(&event_tok).merged(token_span(&second));
                role = next_role;
                last_span = last_span.merged(span);
            }
            TokenKind::Kw(Keyword::Not) => {
                let not_tok = p.bump();
                let null_tok = p.peek();
                if !matches!(null_tok.kind, TokenKind::Null) {
                    return Err(p.error_unexpected(
                        Expected::Keyword("NULL"),
                        Some("`NOT` modifier must be followed by `NULL`"),
                    ));
                }
                let null_tok = p.bump();
                let span = token_span(&not_tok).merged(token_span(&null_tok));
                not_null = true;
                last_span = last_span.merged(span);
            }
            TokenKind::Null => {
                // Explicit `NULL` modifier — the default is already
                // nullable, so this is a no-op that we still accept
                // for parity with the grammar.
                let null_tok = p.bump();
                let span = token_span(&null_tok);
                not_null = false;
                last_span = last_span.merged(span);
            }
            TokenKind::Kw(Keyword::Default) => {
                let default_tok = p.bump();
                let (lit, lit_span) = parse_literal(p)?;
                let span = token_span(&default_tok).merged(lit_span);
                default = Some(lit);
                last_span = last_span.merged(span);
            }
            _ => break,
        }
    }

    Ok(ColumnDef {
        name,
        data_type,
        role,
        not_null,
        default,
        span: last_span,
    })
}

/// Produce a `ParseError::Unexpected` that rejects a role modifier in
/// an ALTER-TABLE column position. Routed through `error_unexpected`
/// so the `found` field uses the real token source text, matching
/// every other error in this module.
fn role_not_allowed_here(p: &Parser) -> ParseError {
    p.error_unexpected(
        Expected::Keyword("NOT"),
        Some(
            "role modifiers (ENTITY KEY / EVENT TIME / EVENT TYPE) are only allowed \
             in CREATE TABLE column definitions",
        ),
    )
}

/// Parse a `type_expr` and return the resolved [`BqlType`] plus the
/// span covering the entire type expression (e.g. from `LIST` through
/// the closing `)`).
fn parse_type_expr(p: &mut Parser) -> Result<(BqlType, Span), ParseError> {
    let tok = p.peek().clone();
    let tok_span = token_span(&tok);
    match &tok.kind {
        TokenKind::Kw(Keyword::Bool_) => {
            p.bump();
            Ok((BqlType::Bool, tok_span))
        }
        TokenKind::Kw(Keyword::Int_) => {
            p.bump();
            Ok((BqlType::Int, tok_span))
        }
        TokenKind::Kw(Keyword::Float_) => {
            p.bump();
            Ok((BqlType::Float, tok_span))
        }
        TokenKind::Kw(Keyword::String_) => {
            p.bump();
            Ok((BqlType::String, tok_span))
        }
        TokenKind::Kw(Keyword::Timestamp) => {
            p.bump();
            Ok((BqlType::Timestamp, tok_span))
        }
        TokenKind::Kw(Keyword::List) => {
            p.bump();
            p.expect_punct(&TokenKind::LParen, "(")?;
            let (inner, _) = parse_type_expr(p)?;
            let rparen = p.expect_punct(&TokenKind::RParen, ")")?;
            let span = tok_span.merged(token_span(&rparen));
            Ok((BqlType::List(Box::new(inner)), span))
        }
        TokenKind::Kw(Keyword::Map) => {
            p.bump();
            p.expect_punct(&TokenKind::LParen, "(")?;
            // `MAP(value_type)` — keys are implicitly STRING per
            // type-system.md §2.6. The parser records the value type
            // and the planner knows the implicit key type.
            let (value, _) = parse_type_expr(p)?;
            let rparen = p.expect_punct(&TokenKind::RParen, ")")?;
            let span = tok_span.merged(token_span(&rparen));
            Ok((BqlType::Map(Box::new(value)), span))
        }
        _ => Err(p.error_unexpected(
            Expected::Keyword("BOOL"),
            Some("expected a column type (BOOL, INT, FLOAT, STRING, TIMESTAMP, LIST(...), or MAP(...))"),
        )),
    }
}

/// Parse a single terminal [`Literal`]. Used by `DEFAULT literal`.
fn parse_literal(p: &mut Parser) -> Result<(Literal, Span), ParseError> {
    let tok = p.peek().clone();
    let span = token_span(&tok);
    match &tok.kind {
        TokenKind::Int(v) => {
            let v = *v;
            p.bump();
            Ok((Literal::Int(v), span))
        }
        TokenKind::Number(v) => {
            let v = *v;
            p.bump();
            Ok((Literal::Float(v), span))
        }
        TokenKind::String(s) => {
            let s = s.clone();
            p.bump();
            Ok((Literal::String(s), span))
        }
        TokenKind::Bool(b) => {
            let b = *b;
            p.bump();
            Ok((Literal::Bool(b), span))
        }
        TokenKind::Null => {
            p.bump();
            Ok((Literal::Null, span))
        }
        TokenKind::Duration(ns) => {
            let ns = *ns;
            p.bump();
            Ok((Literal::Duration(ns), span))
        }
        // DEFAULT -5 — unary minus is not a literal, so `DEFAULT` at the
        // grammar level cannot take a negative integer. This matches
        // the `literal` production in §26 line 1678 and is consistent
        // with the `LIMIT integer` rule (§30 "negative numeric
        // literals").
        _ => Err(p.error_unexpected(Expected::Literal, Some("DEFAULT requires a literal"))),
    }
}

// ----------------------------------------------------------------------
// Tests — DROP TABLE / DESCRIBE / EXPLAIN
// ----------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bqlite_ast::{PipelineStage, Statement};

    fn parse_ok(src: &str) -> Statement {
        crate::parse(src).unwrap_or_else(|e| panic!("parse failed for `{src}`: {e:?}"))
    }

    // --- DROP TABLE ---------------------------------------------------

    #[test]
    fn drop_table_happy_path() {
        match parse_ok("DROP TABLE events") {
            Statement::DropTable(d) => {
                assert_eq!(d.table.text, "events");
                assert_eq!(d.span.start, 0);
                assert_eq!(d.span.end, "DROP TABLE events".len());
            }
            other => panic!("expected DropTable, got {other:?}"),
        }
    }

    #[test]
    fn drop_table_accepts_backtick_name() {
        match parse_ok("DROP TABLE `weird table`") {
            Statement::DropTable(d) => assert_eq!(d.table.text, "weird table"),
            other => panic!("expected DropTable, got {other:?}"),
        }
    }

    #[test]
    fn drop_table_case_insensitive_keywords() {
        match parse_ok("drop table events") {
            Statement::DropTable(d) => assert_eq!(d.table.text, "events"),
            other => panic!("expected DropTable, got {other:?}"),
        }
    }

    #[test]
    fn drop_table_trailing_semicolon() {
        match parse_ok("DROP TABLE events;") {
            Statement::DropTable(d) => assert_eq!(d.table.text, "events"),
            other => panic!("expected DropTable, got {other:?}"),
        }
    }

    #[test]
    fn drop_table_missing_table_keyword_errors() {
        match crate::parse("DROP events") {
            Err(ParseError::Unexpected { expected, .. }) => {
                assert_eq!(expected, Expected::Keyword("TABLE"));
            }
            other => panic!("expected Unexpected/TABLE, got {other:?}"),
        }
    }

    #[test]
    fn drop_table_rejects_if_exists_modifier() {
        // §20.4: no `IF EXISTS`. Parser treats `IF` as an identifier
        // slot, which is a reserved keyword when used as a table name.
        match crate::parse("DROP TABLE IF EXISTS events") {
            Err(ParseError::ReservedKeyword { keyword, role, .. }) => {
                assert_eq!(keyword, "IF");
                assert_eq!(role, NameRole::TableName);
            }
            other => panic!("expected ReservedKeyword(IF), got {other:?}"),
        }
    }

    #[test]
    fn drop_table_missing_name_errors_at_eof() {
        match crate::parse("DROP TABLE") {
            Err(ParseError::UnexpectedEof { expected, .. }) => {
                assert_eq!(expected, Expected::Name);
            }
            other => panic!("expected UnexpectedEof/Name, got {other:?}"),
        }
    }

    // --- DESCRIBE -----------------------------------------------------

    #[test]
    fn describe_happy_path() {
        match parse_ok("DESCRIBE events") {
            Statement::Describe(d) => {
                assert_eq!(d.table.text, "events");
                assert_eq!(d.span.start, 0);
                assert_eq!(d.span.end, "DESCRIBE events".len());
            }
            other => panic!("expected Describe, got {other:?}"),
        }
    }

    #[test]
    fn describe_accepts_backtick_name() {
        match parse_ok("DESCRIBE `quiet table`") {
            Statement::Describe(d) => assert_eq!(d.table.text, "quiet table"),
            other => panic!("expected Describe, got {other:?}"),
        }
    }

    #[test]
    fn describe_case_insensitive_keyword() {
        match parse_ok("describe events") {
            Statement::Describe(d) => assert_eq!(d.table.text, "events"),
            other => panic!("expected Describe, got {other:?}"),
        }
    }

    #[test]
    fn describe_missing_name_errors_at_eof() {
        match crate::parse("DESCRIBE") {
            Err(ParseError::UnexpectedEof { expected, .. }) => {
                assert_eq!(expected, Expected::Name);
            }
            other => panic!("expected UnexpectedEof/Name, got {other:?}"),
        }
    }

    #[test]
    fn describe_rejects_pipeline_tail() {
        // `DESCRIBE events | where x > 0` — DESCRIBE is not a pipeline.
        // The `|` should be rejected as trailing input.
        match crate::parse("DESCRIBE events | where x > 0") {
            Err(ParseError::Unexpected { expected, .. }) => {
                assert_eq!(expected, Expected::Eof);
            }
            other => panic!("expected Unexpected/Eof, got {other:?}"),
        }
    }

    // --- EXPLAIN ------------------------------------------------------

    #[test]
    fn explain_wraps_pipeline() {
        match parse_ok("EXPLAIN events") {
            Statement::Explain(pipe) => {
                assert_eq!(pipe.source.primary.name.text, "events");
                assert!(pipe.stages.is_empty());
            }
            other => panic!("expected Explain, got {other:?}"),
        }
    }

    #[test]
    fn explain_wraps_pipeline_with_stages() {
        match parse_ok("EXPLAIN events | WHERE amount > 0 | LIMIT 10") {
            Statement::Explain(pipe) => {
                assert_eq!(pipe.stages.len(), 2);
                assert!(matches!(pipe.stages[0], PipelineStage::Where { .. }));
                assert!(matches!(pipe.stages[1], PipelineStage::Limit { .. }));
            }
            other => panic!("expected Explain, got {other:?}"),
        }
    }

    #[test]
    fn explain_rejects_ddl() {
        // `EXPLAIN DROP TABLE events` — per §20.6 and §26 line 1669,
        // DDL is not EXPLAIN-able.
        match crate::parse("EXPLAIN DROP TABLE events") {
            Err(ParseError::Unexpected {
                expected, detail, ..
            }) => {
                assert_eq!(expected, Expected::Name);
                assert!(detail
                    .unwrap_or("")
                    .contains("EXPLAIN accepts pipelines only"));
            }
            other => panic!("expected Unexpected/Name, got {other:?}"),
        }
    }

    #[test]
    fn explain_rejects_describe() {
        match crate::parse("EXPLAIN DESCRIBE events") {
            Err(ParseError::Unexpected { detail, .. }) => {
                assert!(detail
                    .unwrap_or("")
                    .contains("EXPLAIN accepts pipelines only"));
            }
            other => panic!("expected Unexpected/EXPLAIN-rejects-DDL, got {other:?}"),
        }
    }

    #[test]
    fn explain_missing_body_errors_at_eof() {
        match crate::parse("EXPLAIN") {
            Err(ParseError::UnexpectedEof { .. }) => {}
            other => panic!("expected UnexpectedEof, got {other:?}"),
        }
    }

    #[test]
    fn explain_case_insensitive_keyword() {
        match parse_ok("explain events | limit 1") {
            Statement::Explain(p) => assert_eq!(p.stages.len(), 1),
            other => panic!("expected Explain, got {other:?}"),
        }
    }

    // --- CREATE TABLE -------------------------------------------------

    fn columns_of(stmt: &Statement) -> &[ColumnDef] {
        match stmt {
            Statement::CreateTable(c) => &c.columns,
            other => panic!("expected CreateTable, got {other:?}"),
        }
    }

    #[test]
    fn create_table_single_scalar_column() {
        let stmt = parse_ok("CREATE TABLE events (amount FLOAT)");
        match stmt {
            Statement::CreateTable(c) => {
                assert_eq!(c.table.text, "events");
                assert_eq!(c.columns.len(), 1);
                assert_eq!(c.columns[0].name.text, "amount");
                assert_eq!(c.columns[0].data_type, BqlType::Float);
                assert_eq!(c.columns[0].role, ColumnRole::Regular);
                assert!(!c.columns[0].not_null);
                assert!(c.columns[0].default.is_none());
            }
            other => panic!("expected CreateTable, got {other:?}"),
        }
    }

    #[test]
    fn create_table_all_scalar_types() {
        let stmt = parse_ok(
            "CREATE TABLE t (\
               b BOOL, \
               i INT, \
               f FLOAT, \
               s STRING, \
               ts TIMESTAMP\
             )",
        );
        let cols = columns_of(&stmt);
        assert_eq!(cols.len(), 5);
        assert_eq!(cols[0].data_type, BqlType::Bool);
        assert_eq!(cols[1].data_type, BqlType::Int);
        assert_eq!(cols[2].data_type, BqlType::Float);
        assert_eq!(cols[3].data_type, BqlType::String);
        assert_eq!(cols[4].data_type, BqlType::Timestamp);
    }

    #[test]
    fn create_table_composite_list_type() {
        let stmt = parse_ok("CREATE TABLE t (tags LIST(STRING))");
        let cols = columns_of(&stmt);
        assert_eq!(cols.len(), 1);
        match &cols[0].data_type {
            BqlType::List(elem) => assert_eq!(**elem, BqlType::String),
            other => panic!("expected List(STRING), got {other:?}"),
        }
    }

    #[test]
    fn create_table_composite_map_type() {
        let stmt = parse_ok("CREATE TABLE t (meta MAP(INT))");
        let cols = columns_of(&stmt);
        match &cols[0].data_type {
            BqlType::Map(val) => assert_eq!(**val, BqlType::Int),
            other => panic!("expected Map(INT), got {other:?}"),
        }
    }

    #[test]
    fn create_table_nested_list_of_list() {
        let stmt = parse_ok("CREATE TABLE t (matrix LIST(LIST(FLOAT)))");
        match &columns_of(&stmt)[0].data_type {
            BqlType::List(inner) => match inner.as_ref() {
                BqlType::List(inner2) => assert_eq!(**inner2, BqlType::Float),
                other => panic!("expected inner List, got {other:?}"),
            },
            other => panic!("expected outer List, got {other:?}"),
        }
    }

    #[test]
    fn create_table_with_all_role_modifiers() {
        // The §20.4 example exactly:
        //
        //   CREATE TABLE events (
        //       user_id STRING ENTITY KEY,
        //       ts TIMESTAMP EVENT TIME,
        //       event_type STRING EVENT TYPE,
        //       amount FLOAT,
        //       device STRING
        //   )
        let stmt = parse_ok(
            "CREATE TABLE events (\
               user_id STRING ENTITY KEY, \
               ts TIMESTAMP EVENT TIME, \
               event_type STRING EVENT TYPE, \
               amount FLOAT, \
               device STRING\
             )",
        );
        let cols = columns_of(&stmt);
        assert_eq!(cols.len(), 5);
        assert_eq!(cols[0].role, ColumnRole::EntityKey);
        assert_eq!(cols[1].role, ColumnRole::EventTime);
        assert_eq!(cols[2].role, ColumnRole::EventType);
        assert_eq!(cols[3].role, ColumnRole::Regular);
        assert_eq!(cols[4].role, ColumnRole::Regular);
    }

    #[test]
    fn create_table_not_null_modifier() {
        let stmt = parse_ok("CREATE TABLE t (amount FLOAT NOT NULL)");
        let cols = columns_of(&stmt);
        assert!(cols[0].not_null);
    }

    #[test]
    fn create_table_explicit_null_modifier_is_noop() {
        // Explicit `NULL` is accepted per §26 and leaves not_null as
        // false (the default).
        let stmt = parse_ok("CREATE TABLE t (amount FLOAT NULL)");
        assert!(!columns_of(&stmt)[0].not_null);
    }

    #[test]
    fn create_table_default_literal() {
        // The task-description allows `[DEFAULT <lit>]` on CREATE TABLE
        // columns even though §26's `column_modifier` production does
        // not yet include `DEFAULT`. This test pins the permissive
        // interpretation; the grammar doc is updated alongside this
        // code.
        let stmt = parse_ok("CREATE TABLE t (amount FLOAT NOT NULL DEFAULT 0.0)");
        let col = &columns_of(&stmt)[0];
        assert!(col.not_null);
        match &col.default {
            Some(Literal::Float(v)) => assert_eq!(*v, 0.0),
            other => panic!("expected Some(Float(0.0)), got {other:?}"),
        }
    }

    #[test]
    fn create_table_modifier_ordering_is_flexible() {
        // `NOT NULL ENTITY KEY` and `ENTITY KEY NOT NULL` should both
        // parse — the parser accepts modifiers in any order and trusts
        // the planner to enforce mutually-exclusive semantics.
        let a = parse_ok("CREATE TABLE t (user_id STRING NOT NULL ENTITY KEY)");
        let b = parse_ok("CREATE TABLE t (user_id STRING ENTITY KEY NOT NULL)");
        let ca = &columns_of(&a)[0];
        let cb = &columns_of(&b)[0];
        assert_eq!(ca.role, ColumnRole::EntityKey);
        assert_eq!(cb.role, ColumnRole::EntityKey);
        assert!(ca.not_null);
        assert!(cb.not_null);
    }

    #[test]
    fn create_table_requires_at_least_one_column() {
        match crate::parse("CREATE TABLE t ()") {
            Err(ParseError::Unexpected { .. }) => {}
            other => panic!("expected Unexpected, got {other:?}"),
        }
    }

    #[test]
    fn create_table_rejects_missing_type() {
        match crate::parse("CREATE TABLE t (col)") {
            Err(ParseError::Unexpected { expected, .. }) => {
                // `parse_type_expr` surfaces a keyword expectation (BOOL).
                assert_eq!(expected, Expected::Keyword("BOOL"));
            }
            other => panic!("expected Unexpected/BOOL, got {other:?}"),
        }
    }

    #[test]
    fn create_table_rejects_event_without_time_or_type() {
        match crate::parse("CREATE TABLE t (col STRING EVENT)") {
            Err(ParseError::Unexpected {
                expected, detail, ..
            }) => {
                assert_eq!(expected, Expected::Keyword("TIME"));
                assert!(detail
                    .unwrap_or("")
                    .contains("`EVENT` column modifier must be `EVENT TIME` or `EVENT TYPE`"));
            }
            other => panic!("expected Unexpected/TIME, got {other:?}"),
        }
    }

    #[test]
    fn create_table_rejects_not_without_null() {
        match crate::parse("CREATE TABLE t (col STRING NOT)") {
            Err(ParseError::UnexpectedEof { expected, .. })
            | Err(ParseError::Unexpected { expected, .. }) => {
                assert_eq!(expected, Expected::Keyword("NULL"));
            }
            other => panic!("expected Unexpected/NULL, got {other:?}"),
        }
    }

    #[test]
    fn create_table_rejects_missing_closing_paren() {
        match crate::parse("CREATE TABLE t (col STRING") {
            Err(ParseError::UnexpectedEof { expected, .. }) => {
                assert_eq!(expected, Expected::Punct(")"));
            }
            other => panic!("expected UnexpectedEof/`)`, got {other:?}"),
        }
    }

    #[test]
    fn create_table_trailing_comma_errors() {
        match crate::parse("CREATE TABLE t (col STRING,)") {
            Err(ParseError::Unexpected { .. }) => {}
            other => panic!("expected Unexpected, got {other:?}"),
        }
    }

    #[test]
    fn create_table_default_null_literal() {
        let stmt = parse_ok("CREATE TABLE t (col STRING DEFAULT NULL)");
        assert_eq!(columns_of(&stmt)[0].default, Some(Literal::Null));
    }

    #[test]
    fn create_table_default_duration_literal() {
        // Durations are valid literals per §26 and tokenize as a single
        // fused lexer token, so they flow through DEFAULT unchanged.
        let stmt = parse_ok("CREATE TABLE t (ttl INT DEFAULT 7d)");
        match columns_of(&stmt)[0].default {
            Some(Literal::Duration(ns)) => {
                assert_eq!(ns, 7 * 86_400 * 1_000_000_000);
            }
            ref other => panic!("expected Duration, got {other:?}"),
        }
    }

    #[test]
    fn create_table_rejects_default_negative_literal() {
        // `DEFAULT -5` — `-5` is not a literal in §26 (`unary := "-"? primary`
        // lives in the expression grammar). The `parse_literal` helper
        // rejects it.
        match crate::parse("CREATE TABLE t (col INT DEFAULT -5)") {
            Err(ParseError::Unexpected { expected, .. }) => {
                assert_eq!(expected, Expected::Literal);
            }
            other => panic!("expected Unexpected/Literal, got {other:?}"),
        }
    }

    // --- ALTER TABLE -------------------------------------------------

    #[test]
    fn alter_table_add_column_with_type() {
        let stmt = parse_ok("ALTER TABLE events ADD COLUMN category STRING");
        match stmt {
            Statement::AlterTable(a) => {
                assert_eq!(a.table.text, "events");
                match a.action {
                    AlterAction::AddColumn(col) => {
                        assert_eq!(col.name.text, "category");
                        assert_eq!(col.data_type, BqlType::String);
                        assert_eq!(col.role, ColumnRole::Regular);
                        assert!(!col.not_null);
                    }
                }
            }
            other => panic!("expected AlterTable, got {other:?}"),
        }
    }

    #[test]
    fn alter_table_add_column_with_not_null_default() {
        // The §20.4 second example, verbatim.
        let stmt = parse_ok("ALTER TABLE events ADD COLUMN score FLOAT NOT NULL DEFAULT 0.0");
        match stmt {
            Statement::AlterTable(a) => match a.action {
                AlterAction::AddColumn(col) => {
                    assert_eq!(col.name.text, "score");
                    assert_eq!(col.data_type, BqlType::Float);
                    assert!(col.not_null);
                    assert_eq!(col.default, Some(Literal::Float(0.0)));
                }
            },
            other => panic!("expected AlterTable, got {other:?}"),
        }
    }

    #[test]
    fn alter_table_rejects_role_modifier() {
        // ALTER TABLE cannot declare role modifiers (§26 `alter_modifier`
        // only allows NOT NULL / NULL / DEFAULT literal).
        match crate::parse("ALTER TABLE events ADD COLUMN user_id STRING ENTITY KEY") {
            Err(ParseError::Unexpected {
                expected, detail, ..
            }) => {
                assert_eq!(expected, Expected::Keyword("NOT"));
                assert!(detail.unwrap_or("").contains("role modifiers"));
            }
            other => panic!("expected Unexpected/NOT, got {other:?}"),
        }
    }

    #[test]
    fn alter_table_rejects_non_add_action() {
        // v1 only supports ADD COLUMN. Any other action (e.g.
        // `DROP COLUMN`) is rejected at the `ADD` expectation.
        match crate::parse("ALTER TABLE events DROP COLUMN amount") {
            Err(ParseError::Unexpected { expected, .. }) => {
                assert_eq!(expected, Expected::Keyword("ADD"));
            }
            other => panic!("expected Unexpected/ADD, got {other:?}"),
        }
    }

    #[test]
    fn alter_table_missing_column_keyword_errors() {
        match crate::parse("ALTER TABLE events ADD col STRING") {
            Err(ParseError::Unexpected { expected, .. }) => {
                assert_eq!(expected, Expected::Keyword("COLUMN"));
            }
            other => panic!("expected Unexpected/COLUMN, got {other:?}"),
        }
    }

    #[test]
    fn alter_table_case_insensitive_keywords() {
        let stmt = parse_ok("alter table events add column flag BOOL");
        assert!(matches!(stmt, Statement::AlterTable(_)));
    }

    #[test]
    fn alter_table_explicit_null_modifier() {
        let stmt = parse_ok("ALTER TABLE t ADD COLUMN flag BOOL NULL");
        match stmt {
            Statement::AlterTable(a) => match a.action {
                AlterAction::AddColumn(col) => assert!(!col.not_null),
            },
            other => panic!("expected AlterTable, got {other:?}"),
        }
    }

    // --- pinned edge cases ------------------------------------------

    #[test]
    fn create_table_accepts_duplicate_column_names_at_parse_time() {
        // The parser does not enforce column-name uniqueness — that's
        // planner territory (TASK-226). Pin the shape so a future
        // regression inside the parser is caught immediately.
        let stmt = parse_ok("CREATE TABLE t (col STRING, col INT)");
        let cols = columns_of(&stmt);
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name.text, "col");
        assert_eq!(cols[1].name.text, "col");
        assert_eq!(cols[0].data_type, BqlType::String);
        assert_eq!(cols[1].data_type, BqlType::Int);
    }

    #[test]
    fn create_table_repeated_default_applies_last_wins() {
        // The modifier loop is permissive — repeated DEFAULT clauses
        // silently last-wins. The parser does not flag this; the
        // planner's job is to reject it if it cares.
        let stmt = parse_ok("CREATE TABLE t (col INT DEFAULT 1 DEFAULT 2)");
        match columns_of(&stmt)[0].default {
            Some(Literal::Int(v)) => assert_eq!(v, 2),
            ref other => panic!("expected Some(Int(2)) as last-wins, got {other:?}"),
        }
    }

    #[test]
    fn create_table_null_after_not_null_clears_flag() {
        // Similar to the DEFAULT case: an explicit `NULL` after
        // `NOT NULL` clears the flag. Pinned so the parser's last-wins
        // loop semantics are documented.
        let stmt = parse_ok("CREATE TABLE t (col INT NOT NULL NULL)");
        assert!(!columns_of(&stmt)[0].not_null);
    }
}
