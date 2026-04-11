//! Wave 2 implementation of the project operator.
//!
//! `ProjectOperator` wraps a child [`PhysicalOperator`] and
//! evaluates a list of output expressions against every batch,
//! assembling a fresh [`RecordBatch`] whose schema is the
//! `output_schema` computed at construction time.
//!
//! ## Scope
//!
//! - **Copy-based output**, not the `FilteredBatch` / selection-
//!   vector chain from execution-model.md §3.8. Wave 2 ships the
//!   straight-line implementation; the fused push-segment refactor
//!   lives in TASK-503 (Wave 5) and replaces this operator rather
//!   than extending it.
//! - **String materialization is always `StringViewArray`** — per
//!   execution-model.md §3.7, expression results that produce
//!   strings must emit `DataType::Utf8View`, never a flat
//!   `Utf8`. The shared [`crate::eval`] evaluator already honors
//!   this contract; project simply passes the arrays through.
//! - **Output schema is cached.** The planner's
//!   [`bqlite_planner::physical::ProjectPhysical`] carries a
//!   pre-built `OperatorSchema`; the operator accepts it from the
//!   caller so EXPLAIN and plan traversal do not pay a rebuild
//!   cost per query.
//!
//! ## Lifecycle
//!
//! `open`, `close`, and `output_schema` have the usual pull-based
//! semantics. `next_batch` pulls one batch from the child, evaluates
//! every output expression against it, and assembles a new
//! `RecordBatch` over the Arrow schema computed from the output
//! `OperatorSchema`.

use std::sync::Arc;

use arrow::datatypes::Schema as ArrowSchema;
use arrow::record_batch::RecordBatch;

use bqlite_core::{BqliteError, OperatorSchema, Result};
use bqlite_planner::compiled::CompiledExpr;
use bqlite_planner::physical::ProjectPhysicalItem;

use crate::eval;
use crate::operator::PhysicalOperator;

/// A single output item for [`ProjectOperator`].
///
/// Re-exposes the shape of [`ProjectPhysicalItem`] without forcing
/// callers outside the planner to import the planner's physical
/// module just to name the type.
#[derive(Debug, Clone)]
pub struct ProjectionExpr {
    /// The compiled expression whose array value becomes the output
    /// column.
    pub expr: CompiledExpr,
    /// Final output column name.
    pub output_name: String,
}

impl From<ProjectPhysicalItem> for ProjectionExpr {
    fn from(item: ProjectPhysicalItem) -> Self {
        Self {
            expr: item.expr,
            output_name: item.output_name,
        }
    }
}

/// Pull-based project operator.
pub struct ProjectOperator {
    child: Box<dyn PhysicalOperator>,
    expressions: Vec<ProjectionExpr>,
    output_schema: OperatorSchema,
    /// Arrow schema derived once from `output_schema` to avoid
    /// rebuilding it per batch.
    arrow_schema: Arc<ArrowSchema>,
}

impl ProjectOperator {
    /// Construct a project operator.
    ///
    /// The caller provides the pre-built `output_schema` from the
    /// physical descriptor ([`bqlite_planner::physical::ProjectPhysical::output_schema`]).
    /// The operator derives the Arrow schema from it once at
    /// construction so per-batch assembly is allocation-free on the
    /// schema side.
    pub fn new(
        child: Box<dyn PhysicalOperator>,
        expressions: Vec<ProjectionExpr>,
        output_schema: OperatorSchema,
    ) -> Self {
        let arrow_schema = Arc::new(output_schema.to_arrow_schema());
        Self {
            child,
            expressions,
            output_schema,
            arrow_schema,
        }
    }

    /// Convenience constructor that accepts the planner's
    /// [`ProjectPhysicalItem`] vector directly.
    pub fn from_physical_items(
        child: Box<dyn PhysicalOperator>,
        items: Vec<ProjectPhysicalItem>,
        output_schema: OperatorSchema,
    ) -> Self {
        let expressions = items.into_iter().map(ProjectionExpr::from).collect();
        Self::new(child, expressions, output_schema)
    }

    /// Borrow the wrapped child. Retained from the Wave 1 stub for
    /// planner tests.
    pub fn child(&self) -> &dyn PhysicalOperator {
        self.child.as_ref()
    }

    /// Borrow the configured output expressions.
    pub fn expressions(&self) -> &[ProjectionExpr] {
        &self.expressions
    }
}

impl PhysicalOperator for ProjectOperator {
    fn output_schema(&self) -> &OperatorSchema {
        &self.output_schema
    }

    fn open(&mut self) -> Result<()> {
        self.child.open()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        let Some(batch) = self.child.next_batch()? else {
            return Ok(None);
        };
        let mut columns = Vec::with_capacity(self.expressions.len());
        for item in &self.expressions {
            let array = eval::evaluate(&item.expr, &batch)?;
            columns.push(array);
        }
        let out = RecordBatch::try_new(Arc::clone(&self.arrow_schema), columns)
            .map_err(BqliteError::Arrow)?;
        Ok(Some(out))
    }

    fn close(&mut self) -> Result<()> {
        self.child.close()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow::array::{Array, ArrayRef, Int64Array, StringViewArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

    use bqlite_ast::expr::{BinaryOp, Expr, Literal, Spanned};
    use bqlite_ast::span::{Name, Span};
    use bqlite_core::{BqlType, ColumnDef};
    use bqlite_planner::expr::{FunctionRegistry, TypedExpr};

    use super::*;
    use crate::filter::FilterOperator;
    use crate::operator::CancellationToken;

    fn input_schema() -> OperatorSchema {
        OperatorSchema::new(vec![
            ColumnDef::required("id", BqlType::Int),
            ColumnDef::required("value", BqlType::Int),
            ColumnDef::required("name", BqlType::String),
        ])
        .unwrap()
    }

    fn input_arrow_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
            Field::new("name", DataType::Utf8View, false),
        ]))
    }

    fn make_batch(ids: Vec<i64>, values: Vec<i64>, names: Vec<&str>) -> RecordBatch {
        let id_array: ArrayRef = Arc::new(Int64Array::from(ids));
        let val_array: ArrayRef = Arc::new(Int64Array::from(values));
        let name_array: ArrayRef = Arc::new(StringViewArray::from(names));
        RecordBatch::try_new(input_arrow_schema(), vec![id_array, val_array, name_array]).unwrap()
    }

    // ── AST helpers ──────────────────────────────────────────────────────────

    fn sp<T>(node: T) -> Spanned<T> {
        Spanned::new(node, Span::EMPTY)
    }

    fn col(name: &str) -> Spanned<Expr> {
        sp(Expr::Column(Name::synthetic(name)))
    }

    fn int_lit(i: i64) -> Spanned<Expr> {
        sp(Expr::Literal(Literal::Int(i)))
    }

    fn str_lit(s: &str) -> Spanned<Expr> {
        sp(Expr::Literal(Literal::String(s.into())))
    }

    fn add_expr(left: Spanned<Expr>, right: Spanned<Expr>) -> Spanned<Expr> {
        sp(Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(left),
            right: Box::new(right),
        })
    }

    fn compile(ast: Spanned<Expr>, schema: &OperatorSchema) -> CompiledExpr {
        let reg = FunctionRegistry::with_builtins();
        let typed = TypedExpr::from_ast(&ast, schema, &reg).expect("type check");
        CompiledExpr::from_typed(&typed)
    }

    fn projection(exprs: Vec<(CompiledExpr, &str)>) -> Vec<ProjectionExpr> {
        exprs
            .into_iter()
            .map(|(expr, name)| ProjectionExpr {
                expr,
                output_name: name.to_string(),
            })
            .collect()
    }

    fn output_schema_from(exprs: &[ProjectionExpr]) -> OperatorSchema {
        let columns = exprs
            .iter()
            .map(|e| {
                if e.expr.nullable {
                    ColumnDef::nullable(&e.output_name, e.expr.result_type.clone())
                } else {
                    ColumnDef::required(&e.output_name, e.expr.result_type.clone())
                }
            })
            .collect();
        OperatorSchema::new(columns).unwrap()
    }

    // ── Recording child ──────────────────────────────────────────────────────

    struct RecordingChild {
        schema: OperatorSchema,
        batches: Vec<RecordBatch>,
        next_idx: usize,
        pulls: Arc<AtomicUsize>,
        opens: Arc<AtomicUsize>,
        closes: Arc<AtomicUsize>,
        cancel: CancellationToken,
        fail_next: bool,
    }

    impl RecordingChild {
        fn new(batches: Vec<RecordBatch>) -> Self {
            Self {
                schema: input_schema(),
                batches,
                next_idx: 0,
                pulls: Arc::new(AtomicUsize::new(0)),
                opens: Arc::new(AtomicUsize::new(0)),
                closes: Arc::new(AtomicUsize::new(0)),
                cancel: CancellationToken::new(),
                fail_next: false,
            }
        }

        fn opens_counter(&self) -> Arc<AtomicUsize> {
            Arc::clone(&self.opens)
        }

        fn closes_counter(&self) -> Arc<AtomicUsize> {
            Arc::clone(&self.closes)
        }

        fn with_cancel(mut self, cancel: CancellationToken) -> Self {
            self.cancel = cancel;
            self
        }

        fn failing() -> Self {
            Self {
                fail_next: true,
                ..Self::new(vec![])
            }
        }
    }

    impl PhysicalOperator for RecordingChild {
        fn output_schema(&self) -> &OperatorSchema {
            &self.schema
        }

        fn open(&mut self) -> Result<()> {
            self.opens.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
            self.pulls.fetch_add(1, Ordering::SeqCst);
            if self.fail_next {
                return Err(BqliteError::Execution("boom".into()));
            }
            if self.cancel.is_cancelled() {
                return Err(BqliteError::Cancelled);
            }
            if self.next_idx >= self.batches.len() {
                return Ok(None);
            }
            let b = self.batches[self.next_idx].clone();
            self.next_idx += 1;
            Ok(Some(b))
        }

        fn close(&mut self) -> Result<()> {
            self.closes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    // ── Correctness ──────────────────────────────────────────────────────────

    #[test]
    fn projects_single_column() {
        let schema = input_schema();
        let exprs = projection(vec![(compile(col("id"), &schema), "id")]);
        let out_schema = output_schema_from(&exprs);
        let child = Box::new(RecordingChild::new(vec![make_batch(
            vec![1, 2, 3],
            vec![10, 20, 30],
            vec!["a", "b", "c"],
        )]));
        let mut project = ProjectOperator::new(child, exprs, out_schema);

        let out = project.next_batch().unwrap().unwrap();
        assert_eq!(out.num_columns(), 1);
        assert_eq!(out.num_rows(), 3);
        let col = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(col.value(0), 1);
        assert_eq!(col.value(1), 2);
        assert_eq!(col.value(2), 3);
        assert_eq!(out.schema().field(0).name(), "id");
    }

    #[test]
    fn projects_with_rename() {
        let schema = input_schema();
        let exprs = projection(vec![(compile(col("value"), &schema), "v")]);
        let out_schema = output_schema_from(&exprs);
        let child = Box::new(RecordingChild::new(vec![make_batch(
            vec![1, 2],
            vec![10, 20],
            vec!["a", "b"],
        )]));
        let mut project = ProjectOperator::new(child, exprs, out_schema);

        let out = project.next_batch().unwrap().unwrap();
        assert_eq!(out.schema().field(0).name(), "v");
        assert_eq!(out.num_rows(), 2);
    }

    #[test]
    fn projects_multiple_columns_with_computed_expression() {
        let schema = input_schema();
        // Output: id, value + 5 AS bumped_value, name
        let exprs = projection(vec![
            (compile(col("id"), &schema), "id"),
            (
                compile(add_expr(col("value"), int_lit(5)), &schema),
                "bumped_value",
            ),
            (compile(col("name"), &schema), "name"),
        ]);
        let out_schema = output_schema_from(&exprs);
        let child = Box::new(RecordingChild::new(vec![make_batch(
            vec![1, 2],
            vec![10, 20],
            vec!["alpha", "beta"],
        )]));
        let mut project = ProjectOperator::new(child, exprs, out_schema);

        let out = project.next_batch().unwrap().unwrap();
        assert_eq!(out.num_columns(), 3);
        let bumped = out.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(bumped.value(0), 15);
        assert_eq!(bumped.value(1), 25);
        // String column round-trips as Utf8View per §3.7.
        assert_eq!(out.schema().field(2).data_type(), &DataType::Utf8View);
        let names = out
            .column(2)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        assert_eq!(names.value(0), "alpha");
        assert_eq!(names.value(1), "beta");
    }

    #[test]
    fn projects_literal_column() {
        // `select 'tag' as label, id` — a literal expression
        // broadcasts to the batch length.
        let schema = input_schema();
        let exprs = projection(vec![
            (compile(str_lit("tag"), &schema), "label"),
            (compile(col("id"), &schema), "id"),
        ]);
        let out_schema = output_schema_from(&exprs);
        let child = Box::new(RecordingChild::new(vec![make_batch(
            vec![1, 2],
            vec![10, 20],
            vec!["a", "b"],
        )]));
        let mut project = ProjectOperator::new(child, exprs, out_schema);

        let out = project.next_batch().unwrap().unwrap();
        let labels = out
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        assert_eq!(labels.value(0), "tag");
        assert_eq!(labels.value(1), "tag");
        // Must be Utf8View, never flat Utf8.
        assert_eq!(out.schema().field(0).data_type(), &DataType::Utf8View);
    }

    #[test]
    fn output_schema_reflects_projection_not_child() {
        let schema = input_schema();
        let exprs = projection(vec![(compile(col("value"), &schema), "v")]);
        let out_schema = output_schema_from(&exprs);
        let child = Box::new(RecordingChild::new(vec![]));
        let project = ProjectOperator::new(child, exprs, out_schema);
        assert_eq!(project.output_schema().columns().len(), 1);
        assert_eq!(project.output_schema().columns()[0].name, "v");
    }

    #[test]
    fn propagates_child_exhaustion() {
        let schema = input_schema();
        let exprs = projection(vec![(compile(col("id"), &schema), "id")]);
        let out_schema = output_schema_from(&exprs);
        let child = Box::new(RecordingChild::new(vec![]));
        let mut project = ProjectOperator::new(child, exprs, out_schema);
        assert!(project.next_batch().unwrap().is_none());
    }

    // ── Composition with filter ──────────────────────────────────────────────

    #[test]
    fn composes_with_filter() {
        // `filter(value > 10) → project(id)` — both operators carry
        // real predicates/expressions.
        let schema = input_schema();
        let filter_predicate = compile(
            sp(Expr::Compare {
                op: bqlite_ast::expr::CompareOp::Greater,
                left: Box::new(col("value")),
                right: Box::new(int_lit(10)),
            }),
            &schema,
        );
        let project_exprs = projection(vec![(compile(col("id"), &schema), "id")]);
        let out_schema = output_schema_from(&project_exprs);

        let child = Box::new(RecordingChild::new(vec![make_batch(
            vec![1, 2, 3, 4],
            vec![5, 15, 25, 10],
            vec!["a", "b", "c", "d"],
        )]));
        let filter = Box::new(FilterOperator::with_default_tile_size(
            child,
            filter_predicate,
        ));
        let mut project = ProjectOperator::new(filter, project_exprs, out_schema);

        let out = project.next_batch().unwrap().unwrap();
        assert_eq!(out.num_columns(), 1);
        let ids = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let ids: Vec<i64> = ids.iter().flatten().collect();
        assert_eq!(ids, vec![2, 3]);
    }

    // ── Lifecycle & error paths ──────────────────────────────────────────────

    #[test]
    fn lifecycle_forwards_to_child() {
        let schema = input_schema();
        let exprs = projection(vec![(compile(col("id"), &schema), "id")]);
        let out_schema = output_schema_from(&exprs);
        let child = RecordingChild::new(vec![make_batch(vec![1], vec![10], vec!["x"])]);
        let opens = child.opens_counter();
        let closes = child.closes_counter();
        let mut project = ProjectOperator::new(Box::new(child), exprs, out_schema);

        project.open().unwrap();
        assert_eq!(opens.load(Ordering::SeqCst), 1);

        assert!(project.next_batch().unwrap().is_some());
        assert!(project.next_batch().unwrap().is_none());

        project.close().unwrap();
        project.close().unwrap();
        assert_eq!(closes.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn errors_propagate_from_child() {
        let schema = input_schema();
        let exprs = projection(vec![(compile(col("id"), &schema), "id")]);
        let out_schema = output_schema_from(&exprs);
        let child = Box::new(RecordingChild::failing());
        let mut project = ProjectOperator::new(child, exprs, out_schema);
        let err = project.next_batch().expect_err("child error must surface");
        assert!(matches!(err, BqliteError::Execution(_)), "{err}");
    }

    #[test]
    fn cancellation_propagates_from_child() {
        let schema = input_schema();
        let exprs = projection(vec![(compile(col("id"), &schema), "id")]);
        let out_schema = output_schema_from(&exprs);
        let cancel = CancellationToken::new();
        let child = Box::new(
            RecordingChild::new(vec![make_batch(vec![1], vec![10], vec!["x"])])
                .with_cancel(cancel.clone()),
        );
        let mut project = ProjectOperator::new(child, exprs, out_schema);
        cancel.cancel();
        let err = project.next_batch().expect_err("cancelled");
        assert!(matches!(err, BqliteError::Cancelled), "{err}");
    }

    #[test]
    fn child_accessor_returns_same_schema() {
        let schema = input_schema();
        let exprs = projection(vec![(compile(col("id"), &schema), "id")]);
        let out_schema = output_schema_from(&exprs);
        let child = Box::new(RecordingChild::new(vec![]));
        let project = ProjectOperator::new(child, exprs, out_schema);
        assert_eq!(project.child().output_schema().columns().len(), 3);
    }

    #[test]
    fn expressions_accessor_surfaces_configured_projection() {
        let schema = input_schema();
        let exprs = projection(vec![
            (compile(col("id"), &schema), "out_id"),
            (compile(col("value"), &schema), "out_value"),
        ]);
        let out_schema = output_schema_from(&exprs);
        let child = Box::new(RecordingChild::new(vec![]));
        let project = ProjectOperator::new(child, exprs, out_schema);
        let got = project.expressions();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].output_name, "out_id");
        assert_eq!(got[1].output_name, "out_value");
    }

    #[test]
    fn trait_object_compiles() {
        let schema = input_schema();
        let exprs = projection(vec![(compile(col("id"), &schema), "id")]);
        let out_schema = output_schema_from(&exprs);
        let child = Box::new(RecordingChild::new(vec![]));
        let project: Box<dyn PhysicalOperator> =
            Box::new(ProjectOperator::new(child, exprs, out_schema));
        let _ = project;
    }
}
