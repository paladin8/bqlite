//! Operator-level microbenchmarks for the fused stateless segment
//! (TASK-519, `engine/operator-fusion.md` §7.2).
//!
//! Two named benches per the design doc:
//!
//! - `scan_filter_project_limit_throughput` — drives a hand-built
//!   `FusedStatelessSegment(Filter → Project → Limit)` over an
//!   in-memory `RecordBatch` of 1M rows in dense (selectivity ≥ 0.5)
//!   and sparse (selectivity ≤ 0.1) configurations. Reports rows / sec.
//! - `selection_vector_materializations_per_query` — pins the §7.2
//!   materialization-count microbench: exactly 0 increments on a
//!   dense-selectivity all-pass single-filter chain (the §4.3
//!   full-cover short-circuit), and exactly 1 mid-chain increment when
//!   an upstream filter pushes the chain below the
//!   §3.4.1 sparsity threshold.
//!
//! Both benches operate at the operator boundary (no segment files,
//! no engine bind). They use `bqlite_core::metrics::AtomicMetrics`
//! as the metric sink so the assertions can read counter values.

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;

use bqlite_ast::expr::{CompareOp, Expr, Literal, Spanned};
use bqlite_ast::span::{Name, Span};
use bqlite_core::metrics::{AtomicMetrics, Metrics};
use bqlite_core::{BqlType, ColumnDef, OperatorSchema, Result};
use bqlite_operators::{
    CancellationToken, FilterKernel, FusedStatelessSegment, KernelStep, PhysicalOperator,
    ProjectKernel, ProjectionExpr, StatelessKernel,
};
use bqlite_planner::compiled::CompiledExpr;
use bqlite_planner::expr::{FunctionRegistry, TypedExpr};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

// ─────────────────────────────────────────────────────────────────────────────
// Fixtures
// ─────────────────────────────────────────────────────────────────────────────

const ROW_COUNT: i64 = 1_000_000;

fn int_op_schema() -> OperatorSchema {
    OperatorSchema::new(vec![
        ColumnDef::required("id", BqlType::Int),
        ColumnDef::required("value", BqlType::Int),
    ])
    .unwrap()
}

fn int_arrow_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn make_batch() -> RecordBatch {
    let ids: ArrayRef = Arc::new(Int64Array::from((0..ROW_COUNT).collect::<Vec<_>>()));
    // Values: 0..ROW_COUNT — predicates can pick a fraction by integer
    // threshold, so e.g. `value < ROW_COUNT/2` keeps 50%.
    let values: ArrayRef = Arc::new(Int64Array::from((0..ROW_COUNT).collect::<Vec<_>>()));
    RecordBatch::try_new(int_arrow_schema(), vec![ids, values]).unwrap()
}

fn sp<T>(node: T) -> Spanned<T> {
    Spanned::new(node, Span::EMPTY)
}
fn col(name: &str) -> Spanned<Expr> {
    sp(Expr::Column(Name::synthetic(name)))
}
fn int_lit(i: i64) -> Spanned<Expr> {
    sp(Expr::Literal(Literal::Int(i)))
}
fn compile_expr(ast: Spanned<Expr>, schema: &OperatorSchema) -> CompiledExpr {
    let reg = FunctionRegistry::with_builtins();
    let typed = TypedExpr::from_ast(&ast, schema, &reg).expect("type check");
    CompiledExpr::from_typed(&typed)
}

fn lt_kernel(threshold: i64, schema: &OperatorSchema) -> Arc<dyn StatelessKernel> {
    let pred = compile_expr(
        sp(Expr::Compare {
            op: CompareOp::Less,
            left: Box::new(col("value")),
            right: Box::new(int_lit(threshold)),
        }),
        schema,
    );
    Arc::new(FilterKernel::with_default_tile_size(pred, schema.clone()))
}

fn ge_kernel(threshold: i64, schema: &OperatorSchema) -> Arc<dyn StatelessKernel> {
    let pred = compile_expr(
        sp(Expr::Compare {
            op: CompareOp::GreaterOrEqual,
            left: Box::new(col("value")),
            right: Box::new(int_lit(threshold)),
        }),
        schema,
    );
    Arc::new(FilterKernel::with_default_tile_size(pred, schema.clone()))
}

fn id_project_kernel(schema: &OperatorSchema) -> (Arc<dyn StatelessKernel>, OperatorSchema) {
    let exprs = vec![ProjectionExpr {
        expr: compile_expr(col("id"), schema),
        output_name: "id".to_string(),
    }];
    let out = OperatorSchema::new(vec![ColumnDef::required("id", BqlType::Int)]).unwrap();
    (Arc::new(ProjectKernel::new(exprs, out.clone())), out)
}

/// Single-pull child over a hand-built `RecordBatch`.
struct OneShotChild {
    schema: OperatorSchema,
    batch: Option<RecordBatch>,
}

impl PhysicalOperator for OneShotChild {
    fn output_schema(&self) -> &OperatorSchema {
        &self.schema
    }
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        Ok(self.batch.take())
    }
}

fn build_segment(
    kernels: Vec<KernelStep>,
    out: OperatorSchema,
    limit: Option<u64>,
    metrics: Arc<dyn Metrics>,
) -> FusedStatelessSegment {
    let child = Box::new(OneShotChild {
        schema: int_op_schema(),
        batch: Some(make_batch()),
    });
    FusedStatelessSegment::new(
        child,
        kernels,
        out,
        limit,
        metrics,
        CancellationToken::new(),
    )
}

// ─────────────────────────────────────────────────────────────────────────────
// Bench: scan_filter_project_limit_throughput
// ─────────────────────────────────────────────────────────────────────────────

fn bench_scan_filter_project_limit_throughput(c: &mut Criterion) {
    let schema = int_op_schema();
    let (project, project_schema) = id_project_kernel(&schema);

    // Two selectivity points: dense (~50%) and sparse (~5%). The
    // dense case stays well above the 10% sparsity threshold; the
    // sparse case is below it (forcing the §3.4.1 mid-chain
    // materialization at the project step's entry).
    let selectivity_threshold = [
        ("dense_50pct", ROW_COUNT / 2),
        ("sparse_5pct", ROW_COUNT / 20),
    ];

    let mut group = c.benchmark_group("fused_segment/scan_filter_project_limit_throughput");
    group.throughput(Throughput::Elements(ROW_COUNT as u64));

    for (label, threshold) in selectivity_threshold.iter() {
        let kernels = vec![
            KernelStep::Filter(lt_kernel(*threshold, &schema)),
            KernelStep::Project(Arc::clone(&project)),
            KernelStep::Limit,
        ];
        let project_schema = project_schema.clone();
        group.bench_with_input(
            BenchmarkId::from_parameter(*label),
            &kernels,
            |b, kernels| {
                b.iter_custom(|iters| {
                    let start = Instant::now();
                    for _ in 0..iters {
                        let metrics: Arc<dyn Metrics> = Arc::new(AtomicMetrics::new());
                        let mut seg = build_segment(
                            kernels.clone(),
                            project_schema.clone(),
                            // Don't truncate the dense case to a small
                            // count — the bench measures rows / sec
                            // through the chain, not the LIMIT path
                            // specifically. Cap at ROW_COUNT.
                            Some(ROW_COUNT as u64),
                            Arc::clone(&metrics),
                        );
                        let mut total = 0u64;
                        while let Some(b) = seg.next_batch().expect("next_batch") {
                            total += b.num_rows() as u64;
                        }
                        black_box(total);
                    }
                    start.elapsed()
                })
            },
        );
    }
    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// Bench: selection_vector_materializations_per_query
// ─────────────────────────────────────────────────────────────────────────────
//
// Two scenarios, both pinned with hard expectations:
//
//   - dense_all_pass     — single Filter that keeps every row. The §4.3
//                          full-cover short-circuit must NOT increment the
//                          counter. Expect: 0 materializations.
//   - sparse_two_filters — first filter keeps 5% (below the 10%
//                          sparsity threshold), forcing one
//                          materialization at filter 2's entry. The
//                          outer boundary is full-cover and short-
//                          circuits. Expect: 1 materialization.

fn bench_selection_vector_materializations_per_query(c: &mut Criterion) {
    let schema = int_op_schema();

    let mut group = c.benchmark_group("fused_segment/selection_vector_materializations_per_query");
    group.throughput(Throughput::Elements(ROW_COUNT as u64));

    // ── Scenario 1: dense_all_pass ──────────────────────────────
    {
        // `value >= 0` keeps every row.
        let kernels = vec![KernelStep::Filter(ge_kernel(0, &schema))];
        group.bench_function("dense_all_pass", |b| {
            b.iter_custom(|iters| {
                let mut total_materializations = 0u64;
                let start = Instant::now();
                for _ in 0..iters {
                    let metrics = Arc::new(AtomicMetrics::new());
                    let mut seg = build_segment(
                        kernels.clone(),
                        schema.clone(),
                        None,
                        Arc::clone(&metrics) as Arc<dyn Metrics>,
                    );
                    while let Some(rb) = seg.next_batch().expect("next_batch") {
                        black_box(rb);
                    }
                    total_materializations += metrics.snapshot().selection_vector_materializations;
                }
                let elapsed = start.elapsed();
                // §7.2 expectation: zero materializations in the
                // all-pass case (full-cover short-circuit at the
                // outer boundary).
                assert_eq!(
                    total_materializations, 0,
                    "dense_all_pass must record zero materializations across {iters} iters"
                );
                elapsed
            })
        });
    }

    // ── Scenario 2: sparse_two_filters ──────────────────────────
    {
        // Filter 1: value < ROW_COUNT/20 → keeps 5% (below 10% threshold).
        // Filter 2: value >= 0 (always-true) — exists so the sparsity
        // boundary fires at its entry.
        let kernels = vec![
            KernelStep::Filter(lt_kernel(ROW_COUNT / 20, &schema)),
            KernelStep::Filter(ge_kernel(0, &schema)),
        ];
        group.bench_function("sparse_two_filters", |b| {
            b.iter_custom(|iters| {
                let mut total_materializations = 0u64;
                let start = Instant::now();
                for _ in 0..iters {
                    let metrics = Arc::new(AtomicMetrics::new());
                    let mut seg = build_segment(
                        kernels.clone(),
                        schema.clone(),
                        None,
                        Arc::clone(&metrics) as Arc<dyn Metrics>,
                    );
                    while let Some(rb) = seg.next_batch().expect("next_batch") {
                        black_box(rb);
                    }
                    total_materializations += metrics.snapshot().selection_vector_materializations;
                }
                let elapsed = start.elapsed();
                // §7.2 expectation: exactly one materialization per
                // iteration — the sparsity boundary at filter 2's
                // entry. The outer boundary is full-cover (filter 2
                // is all-pass over the materialized 5% subset) and
                // short-circuits without incrementing.
                assert_eq!(
                    total_materializations, iters,
                    "sparse_two_filters must record exactly 1 materialization per iter \
                     (got {total_materializations} across {iters} iters)"
                );
                elapsed
            })
        });
    }

    group.finish();
}

criterion_group! {
    name = fused_segment_benches;
    config = Criterion::default();
    targets =
        bench_scan_filter_project_limit_throughput,
        bench_selection_vector_materializations_per_query,
}
criterion_main!(fused_segment_benches);
