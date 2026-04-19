//! Sample pushdown optimizer pass (TASK-430).
//!
//! Walks the physical plan looking for a [`PhysicalPlan::Sample`]
//! whose child subtree is a [`PhysicalPlan::Scan`] — optionally
//! reachable through entity-key-independent stateless stages
//! ([`PhysicalPlan::Filter`] and [`PhysicalPlan::Project`]) — and
//! moves the `(fraction, seed)` pair into
//! [`ScanPhysical::sample`]. When the push succeeds the
//! [`PhysicalPlan::Sample`] node is elided from the plan.
//!
//! When the path between `Sample` and the nearest `Scan` contains
//! any stateful or population-mutating operator (aggregate,
//! sequence match, session assignment, event-select, attribute,
//! merge-sources, …) the pass leaves the `Sample` node in place and
//! recurses into its child. The scan operator does not apply a
//! post-merge sample filter in that case — the engine bind step
//! (TASK-438) owns the fallback `SampleOperator` that will run
//! above the stateful stage.
//!
//! # Correctness
//!
//! The design doc §17.1 proves the push is always safe across
//! stateless filters: SAMPLE's population is the source-table entity
//! set, so `SAMPLE ∘ F ≡ F ∘ SAMPLE` for any entity-key-independent
//! predicate `F`. [`PhysicalPlan::Filter`] and
//! [`PhysicalPlan::Project`] are such stages — they select or
//! reshape rows without changing the entity membership of the
//! result set. [`PhysicalPlan::Limit`] is *not* commutative with
//! SAMPLE (it truncates by row order, which depends on the sample
//! filter's output cardinality) and is therefore treated as
//! opaque by this pass.
//!
//! # Ordering
//!
//! This pass must run *before* predicate pushdown so the composite
//! shape `Sample(Filter(Scan))` first becomes
//! `Filter(Scan_with_sample)`; the subsequent predicate-pushdown
//! pass then moves the filter's pushable conjuncts into
//! `Scan::scan_predicates` and elides the residual filter when
//! possible. Running in the other order would leave the filter
//! above the scan because predicate pushdown does not recurse
//! through `Sample` — every `Sample(Filter(Scan))` would land on
//! the bind step with the filter un-pushed.

use crate::physical::{
    FilterPhysical, PhysicalPlan, ProjectPhysical, SamplePhysical, SamplePushdown, ScanPhysical,
};

/// Run the sample-pushdown pass over a [`PhysicalPlan`] tree.
///
/// Returns a new tree with [`PhysicalPlan::Sample`] nodes pushed
/// into the nearest [`ScanPhysical::sample`] whenever the
/// intermediate path is stateless. Every other interior node is
/// walked so nested patterns (e.g. `Aggregate(Sample(Scan))`) still
/// see the rewrite apply to their sub-tree.
pub fn pushdown_sample(plan: PhysicalPlan) -> PhysicalPlan {
    match plan {
        PhysicalPlan::Sample(sample) => push_sample(sample),

        PhysicalPlan::Filter(f) => {
            let FilterPhysical {
                predicate,
                input,
                tile_size,
                output_schema,
            } = f;
            PhysicalPlan::Filter(FilterPhysical {
                predicate,
                input: Box::new(pushdown_sample(*input)),
                tile_size,
                output_schema,
            })
        }

        PhysicalPlan::Project(p) => {
            let ProjectPhysical {
                expressions,
                input,
                output_schema,
            } = p;
            PhysicalPlan::Project(ProjectPhysical {
                expressions,
                input: Box::new(pushdown_sample(*input)),
                output_schema,
            })
        }

        PhysicalPlan::Limit(l) => {
            let crate::physical::LimitPhysical {
                count,
                input,
                output_schema,
            } = l;
            PhysicalPlan::Limit(crate::physical::LimitPhysical {
                count,
                input: Box::new(pushdown_sample(*input)),
                output_schema,
            })
        }

        PhysicalPlan::Explain(e) => {
            let crate::physical::ExplainPhysical {
                plan: child,
                output_schema,
            } = e;
            PhysicalPlan::Explain(crate::physical::ExplainPhysical {
                plan: Box::new(pushdown_sample(*child)),
                output_schema,
            })
        }

        // Stateful / reshaping interior nodes: recurse into children
        // so sub-trees that contain a pushable Sample still benefit,
        // but do not attempt to commute Sample across them.
        PhysicalPlan::SequenceMatch(sm) => {
            let mut sm = *sm;
            sm.input = Box::new(pushdown_sample(*sm.input));
            PhysicalPlan::SequenceMatch(Box::new(sm))
        }
        PhysicalPlan::Aggregate(a) => {
            let crate::physical::AggregatePhysical {
                aggregates,
                group_by,
                max_groups,
                input,
                output_schema,
            } = a;
            PhysicalPlan::Aggregate(crate::physical::AggregatePhysical {
                aggregates,
                group_by,
                max_groups,
                input: Box::new(pushdown_sample(*input)),
                output_schema,
            })
        }
        PhysicalPlan::Sort(s) => {
            let crate::physical::SortPhysical {
                keys,
                max_rows,
                input,
                output_schema,
            } = s;
            PhysicalPlan::Sort(crate::physical::SortPhysical {
                keys,
                max_rows,
                input: Box::new(pushdown_sample(*input)),
                output_schema,
            })
        }
        PhysicalPlan::Distinct(d) => {
            let crate::physical::DistinctPhysical {
                max_groups,
                input,
                output_schema,
            } = d;
            PhysicalPlan::Distinct(crate::physical::DistinctPhysical {
                max_groups,
                input: Box::new(pushdown_sample(*input)),
                output_schema,
            })
        }
        PhysicalPlan::Sessionize(mut s) => {
            s.input = Box::new(pushdown_sample(*s.input));
            PhysicalPlan::Sessionize(s)
        }
        PhysicalPlan::EventSelect(mut e) => {
            e.input = Box::new(pushdown_sample(*e.input));
            PhysicalPlan::EventSelect(e)
        }
        PhysicalPlan::Attribute(mut a) => {
            a.input = Box::new(pushdown_sample(*a.input));
            PhysicalPlan::Attribute(a)
        }
        PhysicalPlan::SubqueryFilter(mut sf) => {
            sf.input = Box::new(pushdown_sample(*sf.input));
            // The subquery itself may contain a pushable Sample.
            sf.subquery = Box::new(pushdown_sample(*sf.subquery));
            PhysicalPlan::SubqueryFilter(sf)
        }

        // Leaf and unaffected nodes.
        other => other,
    }
}

/// Handle a [`PhysicalPlan::Sample`] node: push into the nearest
/// scan if the intermediate path is stateless, otherwise leave the
/// sample node intact.
fn push_sample(sample: SamplePhysical) -> PhysicalPlan {
    let SamplePhysical {
        fraction,
        seed,
        input,
        output_schema,
    } = sample;

    let child = *input;
    if can_push_through(&child) {
        // `can_push_through` only accepts Scan-under-(Filter/Project)*,
        // so `push_into_scan` never hits its panic branch.
        push_into_scan(child, fraction, seed)
    } else {
        // Not pushable — recurse into the child so nested patterns
        // still benefit, then reassemble the sample node unchanged.
        PhysicalPlan::Sample(SamplePhysical {
            fraction,
            seed,
            input: Box::new(pushdown_sample(child)),
            output_schema,
        })
    }
}

/// True iff `node` is (transitively, through stateless stages) a
/// [`PhysicalPlan::Scan`]. Intermediate [`PhysicalPlan::Filter`] and
/// [`PhysicalPlan::Project`] nodes are considered entity-key-
/// independent per the population-invariance proof in design §17.1.
fn can_push_through(node: &PhysicalPlan) -> bool {
    match node {
        PhysicalPlan::Scan(_) => true,
        PhysicalPlan::Filter(f) => can_push_through(&f.input),
        PhysicalPlan::Project(p) => can_push_through(&p.input),
        _ => false,
    }
}

/// Thread `(fraction, seed)` down a chain of stateless nodes and
/// attach it to the underlying [`ScanPhysical::sample`]. Assumes
/// [`can_push_through`] has already approved the shape — any other
/// variant triggers a programming-error panic.
fn push_into_scan(node: PhysicalPlan, fraction: f64, seed: i64) -> PhysicalPlan {
    match node {
        PhysicalPlan::Scan(scan) => {
            // Replace any pre-existing sample (unusual — the lowering
            // does not set it, and nested SAMPLEs would have been
            // merged by the planner's upper layers). Keeping the
            // outer one mirrors the predicate pushdown pass's
            // "extend and keep" semantics.
            let ScanPhysical { sample, .. } = &scan;
            debug_assert!(
                sample.is_none(),
                "pushdown_sample: sample should not already be set on the scan"
            );
            PhysicalPlan::Scan(ScanPhysical {
                sample: Some(SamplePushdown { fraction, seed }),
                ..scan
            })
        }
        PhysicalPlan::Filter(f) => {
            let FilterPhysical {
                predicate,
                input,
                tile_size,
                output_schema,
            } = f;
            PhysicalPlan::Filter(FilterPhysical {
                predicate,
                input: Box::new(push_into_scan(*input, fraction, seed)),
                tile_size,
                output_schema,
            })
        }
        PhysicalPlan::Project(p) => {
            let ProjectPhysical {
                expressions,
                input,
                output_schema,
            } = p;
            PhysicalPlan::Project(ProjectPhysical {
                expressions,
                input: Box::new(push_into_scan(*input, fraction, seed)),
                output_schema,
            })
        }
        other => panic!(
            "pushdown_sample::push_into_scan: unexpected node {other:?}; \
             can_push_through should have rejected this shape"
        ),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use bqlite_core::{BqlType, ColumnDef, OperatorSchema};

    use crate::compiled::{CompiledExpr, CompiledNode, LogicalKernel};
    use crate::physical::{
        FilterPhysical, LimitPhysical, PhysicalPlan, ProjectPhysical, SamplePhysical, ScanPhysical,
        SessionizePhysical, DEFAULT_FILTER_TILE_SIZE,
    };

    fn schema() -> OperatorSchema {
        OperatorSchema::new(vec![
            ColumnDef::required("entity_id", BqlType::String),
            ColumnDef::required("ts", BqlType::Timestamp),
            ColumnDef::required("event_type", BqlType::String),
        ])
        .unwrap()
    }

    fn make_scan() -> ScanPhysical {
        ScanPhysical {
            table: "events".into(),
            query_range: None,
            reader_range: None,
            scan_predicates: vec![],
            projected_columns: vec![],
            output_schema: schema(),
            entity_key_col: "entity_id".into(),
            timestamp_col: "ts".into(),
            sample: None,
        }
    }

    fn trivial_true() -> CompiledExpr {
        CompiledExpr {
            node: CompiledNode::Literal(bqlite_core::PropertyValue::Bool(true)),
            result_type: BqlType::Bool,
            nullable: false,
        }
    }

    fn trivial_and() -> CompiledExpr {
        CompiledExpr {
            node: CompiledNode::And {
                operands: vec![trivial_true(), trivial_true()],
                kernel: LogicalKernel::ArrowKleene,
            },
            result_type: BqlType::Bool,
            nullable: false,
        }
    }

    fn sample_over(child: PhysicalPlan, fraction: f64, seed: i64) -> PhysicalPlan {
        let out = child.output_schema().clone();
        PhysicalPlan::Sample(SamplePhysical {
            fraction,
            seed,
            input: Box::new(child),
            output_schema: out,
        })
    }

    #[test]
    fn sample_over_scan_is_pushed_and_sample_elided() {
        let plan = sample_over(PhysicalPlan::Scan(make_scan()), 0.25, 7);
        let out = pushdown_sample(plan);
        let PhysicalPlan::Scan(scan) = out else {
            panic!("expected Scan after push, got {out:?}");
        };
        let s = scan.sample.expect("sample attached");
        assert!((s.fraction - 0.25).abs() < f64::EPSILON);
        assert_eq!(s.seed, 7);
    }

    #[test]
    fn sample_over_filter_over_scan_pushes_through_filter() {
        let filter = PhysicalPlan::Filter(FilterPhysical {
            predicate: trivial_and(),
            input: Box::new(PhysicalPlan::Scan(make_scan())),
            tile_size: DEFAULT_FILTER_TILE_SIZE,
            output_schema: schema(),
        });
        let plan = sample_over(filter, 0.5, 42);
        let out = pushdown_sample(plan);
        let PhysicalPlan::Filter(f) = out else {
            panic!("expected Filter at top, got {out:?}");
        };
        let PhysicalPlan::Scan(scan) = *f.input else {
            panic!("expected Scan under Filter");
        };
        let s = scan.sample.expect("sample attached");
        assert!((s.fraction - 0.5).abs() < f64::EPSILON);
        assert_eq!(s.seed, 42);
    }

    #[test]
    fn sample_over_project_over_scan_pushes_through_project() {
        let project = PhysicalPlan::Project(ProjectPhysical {
            expressions: vec![],
            input: Box::new(PhysicalPlan::Scan(make_scan())),
            output_schema: schema(),
        });
        let plan = sample_over(project, 0.1, 99);
        let out = pushdown_sample(plan);
        let PhysicalPlan::Project(p) = out else {
            panic!("expected Project, got {out:?}");
        };
        let PhysicalPlan::Scan(scan) = *p.input else {
            panic!("expected Scan under Project");
        };
        assert!(scan.sample.is_some());
    }

    #[test]
    fn sample_over_limit_is_not_pushed() {
        // Limit changes row counts, so pushing Sample past it would
        // change semantics. The pass must leave Sample above Limit.
        let limited = PhysicalPlan::Limit(LimitPhysical {
            count: 10,
            input: Box::new(PhysicalPlan::Scan(make_scan())),
            output_schema: schema(),
        });
        let plan = sample_over(limited, 0.3, 1);
        let out = pushdown_sample(plan);
        let PhysicalPlan::Sample(sample) = out else {
            panic!("expected Sample to remain, got {out:?}");
        };
        assert!((sample.fraction - 0.3).abs() < f64::EPSILON);
        // Child is Limit → Scan, and the scan should have no sample
        // (the sample stays above the limit).
        let PhysicalPlan::Limit(l) = *sample.input else {
            panic!("expected Limit");
        };
        let PhysicalPlan::Scan(scan) = *l.input else {
            panic!("expected Scan under Limit");
        };
        assert!(scan.sample.is_none());
    }

    #[test]
    fn sample_over_sessionize_is_not_pushed_but_subtree_is_walked() {
        use crate::demand::DemandSet;
        // SESSIONIZE is stateful; Sample must stay above it.
        let scan = PhysicalPlan::Scan(make_scan());
        let sessionize = PhysicalPlan::Sessionize(SessionizePhysical {
            gap_ns: 1_000_000_000,
            end_events: Vec::new(),
            demand: DemandSet::empty(),
            forwarded_columns: Vec::new(),
            fused_aggregate: None,
            input: Box::new(scan),
            output_schema: schema(),
        });
        let plan = sample_over(sessionize, 0.2, 5);
        let out = pushdown_sample(plan);
        let PhysicalPlan::Sample(s) = out else {
            panic!("expected Sample to remain above Sessionize, got {out:?}");
        };
        // Walks into child even though the sample can't commute.
        assert!(matches!(*s.input, PhysicalPlan::Sessionize(_)));
    }

    #[test]
    fn bare_scan_is_unchanged() {
        let scan = PhysicalPlan::Scan(make_scan());
        let out = pushdown_sample(scan);
        let PhysicalPlan::Scan(scan) = out else {
            panic!("expected Scan");
        };
        assert!(scan.sample.is_none());
    }

    #[test]
    fn nested_sample_inside_aggregate_is_pushed_into_inner_scan() {
        use crate::physical::AggregatePhysical;
        // Sample inside an Aggregate child: the pass must recurse
        // into Aggregate.input and push the sample into the scan.
        let inner = sample_over(PhysicalPlan::Scan(make_scan()), 0.7, 3);
        let agg_schema =
            OperatorSchema::new(vec![ColumnDef::required("count", BqlType::Int)]).unwrap();
        let agg = PhysicalPlan::Aggregate(AggregatePhysical {
            aggregates: vec![],
            group_by: vec![],
            max_groups: 1_024,
            input: Box::new(inner),
            output_schema: agg_schema,
        });
        let out = pushdown_sample(agg);
        let PhysicalPlan::Aggregate(a) = out else {
            panic!("expected Aggregate");
        };
        let PhysicalPlan::Scan(scan) = *a.input else {
            panic!("expected Scan inside Aggregate after push");
        };
        let s = scan.sample.expect("sample attached to inner scan");
        assert!((s.fraction - 0.7).abs() < f64::EPSILON);
        assert_eq!(s.seed, 3);
    }
}
