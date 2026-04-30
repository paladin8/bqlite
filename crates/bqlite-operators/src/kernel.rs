//! Stateless kernels for the fused push-segment driver.
//!
//! `docs/design/engine/operator-fusion.md` §3 fixes the runtime
//! contract this module implements: a [`StatelessKernel`] is a pure
//! function over [`FilteredBatch`], invoked by the
//! [`crate::FusedStatelessSegment`] driver as part of a chain of
//! filter / project / limit kernels.
//!
//! Two concrete kernels live here:
//!
//! - [`FilterKernel`] — evaluates a [`CompiledExpr`] predicate over a
//!   `FilteredBatch` and **always** narrows the result to a
//!   `selection: Some(sv)`, even when every row passes (per §3.3.1).
//!   The all-pass case is a `Some(sv)` whose length equals
//!   `batch.num_rows()`; the driver and `materialize_filtered_batch`
//!   both treat that as the dense-equivalent short-circuit.
//! - [`ProjectKernel`] — rewrites columns at the post-selection row
//!   count and returns a dense `FilteredBatch` (`selection: None`)
//!   per §3.3.2.
//!
//! `LIMIT` is not a kernel. The fused-segment driver owns LIMIT's
//! remaining-row budget directly per §3.3.3 / §4.1.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, UInt32Array};
use arrow::compute;
use arrow::datatypes::Schema as ArrowSchema;
use arrow::record_batch::RecordBatch;

use bqlite_core::encoded::SelectionVector;
use bqlite_core::{BqliteError, OperatorSchema, Result};
use bqlite_planner::compiled::CompiledExpr;
use bqlite_planner::physical::{
    DEFAULT_FILTER_TILE_SIZE, MAX_FILTER_TILE_SIZE, MIN_FILTER_TILE_SIZE,
};

use crate::eval;
use crate::filtered_batch::FilteredBatch;
use crate::project::ProjectionExpr;

// ─────────────────────────────────────────────────────────────────────────────
// Trait
// ─────────────────────────────────────────────────────────────────────────────

/// A pure function over [`FilteredBatch`].
///
/// Stateless kernels never observe entity boundaries — the scan layer
/// preserves per-entity contiguity at the batch level — and they
/// never copy row data unless they are crossing a §3.4 materialization
/// trigger (which is the segment driver's responsibility, not the
/// kernel's).
///
/// See `docs/design/engine/operator-fusion.md` §3.2 for the full
/// contract.
pub trait StatelessKernel: Send + Sync {
    /// Apply this kernel to `input`.
    ///
    /// Implementations must respect §3.3 of the design doc:
    /// filter narrows, project rewrites, limit truncates.
    fn apply(&self, input: FilteredBatch) -> Result<FilteredBatch>;

    /// The schema of the `RecordBatch` inside the returned
    /// `FilteredBatch`. Filter and Limit return their child's schema
    /// unchanged; Project returns the post-projection schema.
    fn output_schema(&self) -> &OperatorSchema;

    /// Identifying tag for `--explain-perf` (TASK-524) and the
    /// `kernel_calls` / `kernel_time_ns` per-kernel metrics. Default
    /// is the kernel's Rust type name.
    fn kernel_name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// FilterKernel
// ─────────────────────────────────────────────────────────────────────────────

/// `Filter` stateless kernel.
///
/// Evaluates a compiled boolean predicate against the live rows of the
/// input `FilteredBatch` and returns a new `FilteredBatch` with the
/// **same** underlying `RecordBatch` and a narrowed selection vector.
/// Per §3.3.1 the output always carries `selection: Some(_)` — even
/// when every row passes.
///
/// Tile-loop predicate evaluation (`evaluate_tiled_mask`) is preserved
/// from the legacy `FilterOperator`; the kernel surface is what
/// changes, not the inner predicate kernel choice.
pub struct FilterKernel {
    predicate: CompiledExpr,
    tile_size: usize,
    schema: OperatorSchema,
}

impl FilterKernel {
    /// Construct a filter kernel with a caller-supplied tile size,
    /// clamped into the legal `[MIN_FILTER_TILE_SIZE,
    /// MAX_FILTER_TILE_SIZE]` window.
    pub fn new(predicate: CompiledExpr, tile_size: usize, schema: OperatorSchema) -> Self {
        Self {
            predicate,
            tile_size: tile_size.clamp(MIN_FILTER_TILE_SIZE, MAX_FILTER_TILE_SIZE),
            schema,
        }
    }

    /// Convenience constructor using [`DEFAULT_FILTER_TILE_SIZE`].
    pub fn with_default_tile_size(predicate: CompiledExpr, schema: OperatorSchema) -> Self {
        Self::new(predicate, DEFAULT_FILTER_TILE_SIZE, schema)
    }

    /// The post-clamp tile size in use.
    pub fn tile_size(&self) -> usize {
        self.tile_size
    }

    fn evaluate_tiled_mask(&self, batch: &RecordBatch) -> Result<BooleanArray> {
        let total = batch.num_rows();
        if total == 0 {
            return Ok(BooleanArray::from(Vec::<bool>::new()));
        }
        if total <= self.tile_size {
            return eval::evaluate_bool(&self.predicate, batch);
        }
        let mut tile_masks: Vec<BooleanArray> = Vec::with_capacity(total.div_ceil(self.tile_size));
        let mut start = 0;
        while start < total {
            let len = std::cmp::min(self.tile_size, total - start);
            let tile = batch.slice(start, len);
            tile_masks.push(eval::evaluate_bool(&self.predicate, &tile)?);
            start += len;
        }
        let refs: Vec<&dyn Array> = tile_masks.iter().map(|m| m as &dyn Array).collect();
        let concat = compute::concat(&refs).map_err(BqliteError::Arrow)?;
        concat
            .as_any()
            .downcast_ref::<BooleanArray>()
            .cloned()
            .ok_or_else(|| {
                BqliteError::Execution(
                    "tile-mask concat did not produce a BooleanArray".to_string(),
                )
            })
    }
}

impl StatelessKernel for FilterKernel {
    fn apply(&self, input: FilteredBatch) -> Result<FilteredBatch> {
        let FilteredBatch { batch, selection } = input;
        let mask = self.evaluate_tiled_mask(&batch)?;
        let sv = match selection {
            None => bool_mask_to_selection(&mask, batch.num_rows()),
            Some(sv_in) => intersect_with_mask(&sv_in, &mask),
        };
        Ok(FilteredBatch::with_selection(batch, sv))
    }

    fn output_schema(&self) -> &OperatorSchema {
        &self.schema
    }

    fn kernel_name(&self) -> &'static str {
        "FilterKernel"
    }
}

/// Convert a (possibly-null) boolean mask into a `SelectionVector`,
/// treating null as "drop" (SQL `WHERE` semantics — three-valued
/// logic per the legacy `FilterOperator` contract).
fn bool_mask_to_selection(mask: &BooleanArray, row_count: usize) -> SelectionVector {
    let len = row_count.min(mask.len());
    // Pre-size to true_count() when the kernel can compute it cheaply.
    let cap = mask.true_count();
    let mut out = Vec::with_capacity(cap);
    for i in 0..len {
        if mask.is_valid(i) && mask.value(i) {
            out.push(i as u32);
        }
    }
    SelectionVector::from_sorted(out)
}

/// Intersect an existing selection with a freshly-computed boolean
/// mask. Visits only the indices in `sv_in` and emits surviving ones.
fn intersect_with_mask(sv_in: &SelectionVector, mask: &BooleanArray) -> SelectionVector {
    let mut out = Vec::with_capacity(sv_in.len());
    for &idx in sv_in.as_slice() {
        let i = idx as usize;
        if i < mask.len() && mask.is_valid(i) && mask.value(i) {
            out.push(idx);
        }
    }
    SelectionVector::from_sorted(out)
}

// ─────────────────────────────────────────────────────────────────────────────
// ProjectKernel
// ─────────────────────────────────────────────────────────────────────────────

/// `Project` stateless kernel.
///
/// Walks the input's live rows once per output column and writes the
/// projected values into a fresh `RecordBatch`. The output is dense —
/// `selection: None` — because the projection has already narrowed
/// the rows to `S_in.len()` per §3.3.2.
///
/// In v1 the kernel evaluates each expression against the full input
/// batch and `take`s the live indices for the `Some(sv)` case. The
/// "pre-size and write directly into post-selection buffers"
/// optimization is the responsibility of TASK-519 once the legacy
/// operators are retired and the eval evaluator can be specialized
/// for selection-vector inputs.
pub struct ProjectKernel {
    expressions: Vec<ProjectionExpr>,
    output_schema: OperatorSchema,
    arrow_schema: Arc<ArrowSchema>,
}

impl ProjectKernel {
    /// Construct a project kernel with a pre-built output
    /// `OperatorSchema` (the planner's
    /// `ProjectPhysical::output_schema`).
    pub fn new(expressions: Vec<ProjectionExpr>, output_schema: OperatorSchema) -> Self {
        let arrow_schema = Arc::new(output_schema.to_arrow_schema());
        Self {
            expressions,
            output_schema,
            arrow_schema,
        }
    }

    /// Borrow the configured projection expressions.
    pub fn expressions(&self) -> &[ProjectionExpr] {
        &self.expressions
    }
}

impl StatelessKernel for ProjectKernel {
    fn apply(&self, input: FilteredBatch) -> Result<FilteredBatch> {
        let FilteredBatch { batch, selection } = input;
        let take_indices = selection
            .as_ref()
            .map(|sv| UInt32Array::from(sv.as_slice().to_vec()));
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.expressions.len());
        for item in &self.expressions {
            let evaluated = eval::evaluate(&item.expr, &batch)?;
            let column: ArrayRef = match &take_indices {
                None => evaluated,
                Some(idx) => {
                    compute::take(evaluated.as_ref(), idx, None).map_err(BqliteError::Arrow)?
                }
            };
            columns.push(column);
        }
        let out = RecordBatch::try_new(Arc::clone(&self.arrow_schema), columns)
            .map_err(BqliteError::Arrow)?;
        Ok(FilteredBatch::dense(out))
    }

    fn output_schema(&self) -> &OperatorSchema {
        &self.output_schema
    }

    fn kernel_name(&self) -> &'static str {
        "ProjectKernel"
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, StringViewArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

    use bqlite_ast::expr::{BinaryOp, CompareOp, Expr, Literal, Spanned};
    use bqlite_ast::span::{Name, Span};
    use bqlite_core::{BqlType, ColumnDef, OperatorSchema};
    use bqlite_planner::expr::{FunctionRegistry, TypedExpr};

    use super::*;

    // ── Fixtures ──────────────────────────────────────────────────────

    fn int_schema() -> OperatorSchema {
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

    fn make_batch(ids: Vec<i64>, values: Vec<i64>) -> RecordBatch {
        let id: ArrayRef = Arc::new(Int64Array::from(ids));
        let val: ArrayRef = Arc::new(Int64Array::from(values));
        RecordBatch::try_new(int_arrow_schema(), vec![id, val]).unwrap()
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
    fn compile(ast: Spanned<Expr>, schema: &OperatorSchema) -> CompiledExpr {
        let reg = FunctionRegistry::with_builtins();
        let typed = TypedExpr::from_ast(&ast, schema, &reg).expect("type check");
        CompiledExpr::from_typed(&typed)
    }
    fn value_gt(literal: i64, schema: &OperatorSchema) -> CompiledExpr {
        compile(
            sp(Expr::Compare {
                op: CompareOp::Greater,
                left: Box::new(col("value")),
                right: Box::new(int_lit(literal)),
            }),
            schema,
        )
    }

    // ── FilterKernel ──────────────────────────────────────────────────

    #[test]
    fn filter_narrows_dense_input_to_some_sv() {
        let schema = int_schema();
        let kernel = FilterKernel::with_default_tile_size(value_gt(15, &schema), schema.clone());
        let input = FilteredBatch::dense(make_batch(vec![1, 2, 3, 4], vec![10, 20, 30, 15]));
        let out = kernel.apply(input).unwrap();
        // §3.3.1: output always Some(_) regardless of selectivity.
        let sv = out.selection.expect("filter output must carry Some(sv)");
        assert_eq!(sv.as_slice(), &[1, 2]);
        // Underlying batch is preserved (no copy).
        assert_eq!(out.batch.num_rows(), 4);
    }

    #[test]
    fn filter_intersects_existing_selection() {
        let schema = int_schema();
        let kernel = FilterKernel::with_default_tile_size(value_gt(15, &schema), schema.clone());
        // Selection covers rows 0,1,3 — predicate keeps rows 1,2 →
        // intersection is just row 1.
        let input = FilteredBatch::with_selection(
            make_batch(vec![1, 2, 3, 4], vec![10, 20, 30, 15]),
            SelectionVector::from_sorted(vec![0, 1, 3]),
        );
        let out = kernel.apply(input).unwrap();
        let sv = out.selection.unwrap();
        assert_eq!(sv.as_slice(), &[1]);
    }

    #[test]
    fn filter_all_pass_input_returns_some_sv_with_full_length() {
        // Every row passes → output Some(sv) with sv.len() ==
        // batch.num_rows(). This pins §3.3.1's "always Some(_)" rule
        // and is the dense-equivalent the driver's short-circuit
        // depends on.
        let schema = int_schema();
        let kernel = FilterKernel::with_default_tile_size(value_gt(0, &schema), schema.clone());
        let input = FilteredBatch::dense(make_batch(vec![1, 2, 3], vec![10, 20, 30]));
        let out = kernel.apply(input).unwrap();
        let sv = out.selection.expect("must be Some even on all-pass");
        assert_eq!(sv.len(), out.batch.num_rows());
        assert_eq!(sv.as_slice(), &[0, 1, 2]);
    }

    #[test]
    fn filter_drops_null_predicate_rows() {
        let schema = OperatorSchema::new(vec![
            ColumnDef::required("id", BqlType::Int),
            ColumnDef::nullable("value", BqlType::Int),
        ])
        .unwrap();
        let kernel = FilterKernel::with_default_tile_size(value_gt(10, &schema), schema.clone());

        let id: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4]));
        let val: ArrayRef = Arc::new(Int64Array::from(vec![Some(5), None, Some(20), Some(15)]));
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(arrow_schema, vec![id, val]).unwrap();

        let out = kernel.apply(FilteredBatch::dense(batch)).unwrap();
        let sv = out.selection.unwrap();
        // SQL WHERE drops NULL — rows 2 and 3 survive.
        assert_eq!(sv.as_slice(), &[2, 3]);
    }

    #[test]
    fn filter_zero_rows_input_returns_empty_some_sv() {
        let schema = int_schema();
        let kernel = FilterKernel::with_default_tile_size(value_gt(0, &schema), schema.clone());
        let out = kernel
            .apply(FilteredBatch::dense(make_batch(vec![], vec![])))
            .unwrap();
        let sv = out.selection.unwrap();
        assert!(sv.is_empty());
    }

    #[test]
    fn filter_kernel_clamps_tile_size() {
        let schema = int_schema();
        let too_small = FilterKernel::new(value_gt(0, &schema), 10, schema.clone());
        assert_eq!(too_small.tile_size(), MIN_FILTER_TILE_SIZE);
        let too_big = FilterKernel::new(value_gt(0, &schema), 999_999, schema.clone());
        assert_eq!(too_big.tile_size(), MAX_FILTER_TILE_SIZE);
    }

    #[test]
    fn filter_kernel_name_is_stable() {
        let schema = int_schema();
        let kernel = FilterKernel::with_default_tile_size(value_gt(0, &schema), schema);
        assert_eq!(kernel.kernel_name(), "FilterKernel");
    }

    // ── ProjectKernel ─────────────────────────────────────────────────

    fn make_projection(items: Vec<(CompiledExpr, &str)>) -> Vec<ProjectionExpr> {
        items
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

    #[test]
    fn project_dense_input_returns_dense_output() {
        let schema = int_schema();
        let exprs = make_projection(vec![
            (compile(col("id"), &schema), "id"),
            (
                compile(
                    sp(Expr::Binary {
                        op: BinaryOp::Add,
                        left: Box::new(col("value")),
                        right: Box::new(int_lit(5)),
                    }),
                    &schema,
                ),
                "v_plus_5",
            ),
        ]);
        let out_schema = output_schema_from(&exprs);
        let kernel = ProjectKernel::new(exprs, out_schema);

        let input = FilteredBatch::dense(make_batch(vec![1, 2], vec![10, 20]));
        let out = kernel.apply(input).unwrap();
        // §3.3.2: output is dense.
        assert!(out.is_dense());
        assert_eq!(out.batch.num_rows(), 2);
        let v = out
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(v.value(0), 15);
        assert_eq!(v.value(1), 25);
    }

    #[test]
    fn project_with_selection_walks_live_rows_only() {
        let schema = int_schema();
        let exprs = make_projection(vec![(compile(col("id"), &schema), "id")]);
        let out_schema = output_schema_from(&exprs);
        let kernel = ProjectKernel::new(exprs, out_schema);

        // Live rows 1 and 3 of [1,2,3,4].
        let input = FilteredBatch::with_selection(
            make_batch(vec![1, 2, 3, 4], vec![10, 20, 30, 40]),
            SelectionVector::from_sorted(vec![1, 3]),
        );
        let out = kernel.apply(input).unwrap();
        assert!(out.is_dense());
        // Output row count == S_in.len() per §3.3.2.
        assert_eq!(out.batch.num_rows(), 2);
        let ids = out
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 2);
        assert_eq!(ids.value(1), 4);
    }

    #[test]
    fn project_string_columns_stay_view() {
        // execution-model.md §3.7: string materialization is always
        // StringViewArray — projection must not round-trip through
        // Utf8.
        let schema = OperatorSchema::new(vec![
            ColumnDef::required("id", BqlType::Int),
            ColumnDef::required("name", BqlType::String),
        ])
        .unwrap();
        let exprs = make_projection(vec![(compile(col("name"), &schema), "name")]);
        let out_schema = output_schema_from(&exprs);
        let kernel = ProjectKernel::new(exprs, out_schema);

        let id: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2]));
        let name: ArrayRef = Arc::new(StringViewArray::from(vec!["alpha", "beta"]));
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8View, false),
        ]));
        let batch = RecordBatch::try_new(arrow_schema, vec![id, name]).unwrap();
        let out = kernel
            .apply(FilteredBatch::with_selection(
                batch,
                SelectionVector::from_sorted(vec![1]),
            ))
            .unwrap();
        assert_eq!(out.batch.schema().field(0).data_type(), &DataType::Utf8View);
        let names = out
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        assert_eq!(names.value(0), "beta");
    }

    #[test]
    fn project_empty_selection_yields_zero_rows() {
        let schema = int_schema();
        let exprs = make_projection(vec![(compile(col("id"), &schema), "id")]);
        let out_schema = output_schema_from(&exprs);
        let kernel = ProjectKernel::new(exprs, out_schema);

        let input = FilteredBatch::with_selection(
            make_batch(vec![1, 2, 3], vec![10, 20, 30]),
            SelectionVector::new(),
        );
        let out = kernel.apply(input).unwrap();
        assert!(out.is_dense());
        assert_eq!(out.batch.num_rows(), 0);
    }

    #[test]
    fn project_kernel_name_is_stable() {
        let schema = int_schema();
        let exprs = make_projection(vec![(compile(col("id"), &schema), "id")]);
        let out_schema = output_schema_from(&exprs);
        let kernel = ProjectKernel::new(exprs, out_schema);
        assert_eq!(kernel.kernel_name(), "ProjectKernel");
    }

    // ── Trait object ──────────────────────────────────────────────────

    #[test]
    fn kernels_are_trait_objects() {
        let schema = int_schema();
        let f: Arc<dyn StatelessKernel> = Arc::new(FilterKernel::with_default_tile_size(
            value_gt(0, &schema),
            schema.clone(),
        ));
        let exprs = make_projection(vec![(compile(col("id"), &schema), "id")]);
        let p_schema = output_schema_from(&exprs);
        let p: Arc<dyn StatelessKernel> = Arc::new(ProjectKernel::new(exprs, p_schema));
        // Just exercise the dyn dispatch.
        let _ = f.kernel_name();
        let _ = p.kernel_name();
    }
}
