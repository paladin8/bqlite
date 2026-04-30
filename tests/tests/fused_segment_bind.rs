//! Integration test for the `FusedSegmentPhysical` bind path.
//!
//! Post TASK-519 lowering emits `FusedSegmentPhysical` for every
//! stateless run. This test pins the bind step's translation of a
//! hand-built descriptor onto a runtime `FusedStatelessSegment`.
//!
//! Lives in `tests/` (not in `crates/bqlite-engine/src/bind.rs`)
//! because the test needs to construct `CompiledExpr` values from raw
//! AST, which requires `bqlite-ast` — and `bqlite-engine`'s
//! dependency-direction rule (`scripts/check-dep-direction.sh`)
//! forbids importing `bqlite-ast` even as a dev-dependency.

use bqlite_ast::expr::{CompareOp, Expr, Literal, Spanned};
use bqlite_ast::span::{Name, Span};
use bqlite_core::{Catalog, ColumnDef, OperatorSchema};
use bqlite_engine::{bind_physical, Database, Engine, QueryContext};
use bqlite_planner::compiled::CompiledExpr;
use bqlite_planner::expr::{FunctionRegistry, TypedExpr};
use bqlite_planner::physical::{
    FusedSegmentPhysical, FusedSegmentStep, ProjectPhysicalItem, ScanPhysical,
    DEFAULT_FILTER_TILE_SIZE,
};
use bqlite_planner::PhysicalPlan;
use bqlite_tests::common::TempDb;

/// Build the `events` table and seed it with a few rows so the bind
/// path has something to scan.
fn db_with_events_rows() -> (Database, TempDb) {
    let scratch = TempDb::new();
    let mut db = Database::create(scratch.path()).expect("Database::create");
    let engine = Engine::new();
    engine
        .query(
            "CREATE TABLE events ( \
                 user_id STRING NOT NULL ENTITY KEY, \
                 ts TIMESTAMP NOT NULL EVENT TIME, \
                 event_type STRING NOT NULL EVENT TYPE, \
                 amount INT \
             )",
            &mut db,
        )
        .expect("create events");
    engine
        .query(
            "INSERT INTO events VALUES \
             ('alice', 1700000000000000000, 'click', 10), \
             ('alice', 1700000000100000000, 'view',  20), \
             ('bob',   1700000000200000000, 'click', 30), \
             ('bob',   1700000000300000000, 'view',  40), \
             ('carol', 1700000000400000000, 'click', 50)",
            &mut db,
        )
        .expect("insert");
    (db, scratch)
}

fn sp<T>(node: T) -> Spanned<T> {
    Spanned::new(node, Span::EMPTY)
}

fn col(name: &str) -> Spanned<Expr> {
    sp(Expr::Column(Name::synthetic(name)))
}

fn str_lit(value: &str) -> Spanned<Expr> {
    sp(Expr::Literal(Literal::String(value.into())))
}

fn compile(ast: Spanned<Expr>, schema: &OperatorSchema) -> CompiledExpr {
    let reg = FunctionRegistry::with_builtins();
    let typed = TypedExpr::from_ast(&ast, schema, &reg).expect("type check");
    CompiledExpr::from_typed(&typed)
}

/// Hand-build a `FusedSegmentPhysical` over the events table that
/// filters `event_type = 'click'`, projects `(user_id, event_type)`,
/// and limits to 2 rows. The optimizer never emits this descriptor in
/// CP4, so this test is the canonical bind-path equivalence pin.
#[test]
fn bind_fused_segment_filter_project_limit_returns_expected_rows() {
    let (mut db, _scratch) = db_with_events_rows();

    // Bare scan over events.
    let events_schema = db
        .catalog()
        .resolve_table("events")
        .expect("events table must exist");
    let scan_schema = OperatorSchema::from_table(&events_schema);
    let scan = PhysicalPlan::Scan(ScanPhysical {
        table: "events".to_string(),
        query_range: None,
        reader_range: None,
        scan_predicates: Vec::new(),
        projected_columns: Vec::new(),
        output_schema: scan_schema.clone(),
        entity_key_col: "user_id".to_string(),
        timestamp_col: "ts".to_string(),
        sample: None,
    });

    // Filter: event_type = 'click'.
    let filter_pred = compile(
        sp(Expr::Compare {
            op: CompareOp::Equal,
            left: Box::new(col("event_type")),
            right: Box::new(str_lit("click")),
        }),
        &scan_schema,
    );

    // Project: user_id, event_type.
    let project_items = vec![
        ProjectPhysicalItem {
            expr: compile(col("user_id"), &scan_schema),
            output_name: "user_id".to_string(),
        },
        ProjectPhysicalItem {
            expr: compile(col("event_type"), &scan_schema),
            output_name: "event_type".to_string(),
        },
    ];
    let project_schema = OperatorSchema::new(vec![
        ColumnDef::required("user_id", bqlite_core::BqlType::String),
        ColumnDef::required("event_type", bqlite_core::BqlType::String),
    ])
    .unwrap();

    let seg = FusedSegmentPhysical {
        input: Box::new(scan),
        steps: vec![
            FusedSegmentStep::Filter {
                predicate: filter_pred,
                tile_size: DEFAULT_FILTER_TILE_SIZE,
            },
            FusedSegmentStep::Project(project_items),
            FusedSegmentStep::Limit(2),
        ],
        sparsity_factor: 0.10,
        output_schema: project_schema.clone(),
    };
    let plan = PhysicalPlan::FusedSegment(seg);

    // Bind and drive.
    let mut op =
        bind_physical(&plan, &mut db, &QueryContext::unbounded()).expect("bind must succeed");
    assert_eq!(op.output_schema(), &project_schema);
    op.open().expect("open");

    let mut total_rows = 0usize;
    let mut user_ids: Vec<String> = Vec::new();
    while let Some(batch) = op.next_batch().expect("next_batch") {
        total_rows += batch.num_rows();
        let users = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .expect("user_id column should be StringView");
        let event_types = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .expect("event_type column should be StringView");
        for i in 0..batch.num_rows() {
            // Every surviving row's event_type must be 'click' — the
            // filter rejected the others.
            assert_eq!(event_types.value(i), "click");
            user_ids.push(users.value(i).to_string());
        }
    }
    op.close().expect("close");

    assert_eq!(
        total_rows, 2,
        "LIMIT 2 must cap the chain output, even though three 'click' rows exist"
    );
    assert_eq!(user_ids.len(), 2);
    // Sort because rows may be split across shards / batches.
    user_ids.sort();
    // alice and bob are the first two click rows by (user_id, ts) per
    // entity-sorted scan ordering.
    assert_eq!(user_ids, vec!["alice", "bob"]);
}

// The previous `fused_segment_bind_path_matches_legacy_chain_output`
// test was retired in TASK-519 CP2 alongside the legacy
// FilterPhysical / ProjectPhysical / LimitPhysical descriptors —
// lowering now produces `FusedSegmentPhysical` exclusively, so an
// equivalence comparison against the deleted descriptors is no longer
// constructible. The end-to-end shape is exercised by:
//   - `bind_fused_segment_filter_project_limit_returns_expected_rows`
//     above (this file),
//   - the lowering tests in `crates/bqlite-planner/src/physical.rs`
//     (`lower_pipeline_with_where_produces_fused_segment_over_scan`,
//     `lower_where_select_limit_pipeline_coalesces_into_single_fused_segment`,
//     etc.), and
//   - `fused_stateless_segment_records_selection_vector_metrics` in
//     `tests/tests/wave2_acceptance.rs`.
