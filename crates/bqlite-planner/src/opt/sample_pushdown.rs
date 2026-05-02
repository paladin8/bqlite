//! Sample pushdown optimizer pass (TASK-430, retargeted by TASK-519).
//!
//! Walks the physical plan looking for a [`PhysicalPlan::Sample`]
//! whose child subtree is a [`PhysicalPlan::Scan`] — optionally
//! reachable through entity-key-independent stateless stages
//! (a [`PhysicalPlan::FusedSegment`] whose `steps` are all `Filter`
//! or `Project`) — and moves the `(fraction, seed)` pair into
//! [`ScanPhysical::sample`]. When the push succeeds the
//! [`PhysicalPlan::Sample`] node is elided from the plan.
//!
//! When the path between `Sample` and the nearest `Scan` contains
//! any stateful or population-mutating operator (aggregate,
//! sequence match, session assignment, event-select, attribute,
//! merge-sources, …) — or a `FusedSegment` containing a `Limit`
//! step — the pass leaves the `Sample` node in place and recurses
//! into its child. The scan operator does not apply a post-merge
//! sample filter in that case — the engine bind step (TASK-438)
//! owns the fallback `SampleOperator` that will run above the
//! stateful stage.
//!
//! # Correctness
//!
//! The design doc §17.1 proves the push is always safe across
//! stateless filters: SAMPLE's population is the source-table entity
//! set, so `SAMPLE ∘ F ≡ F ∘ SAMPLE` for any entity-key-independent
//! predicate `F`. `Filter` and `Project` steps inside a
//! [`PhysicalPlan::FusedSegment`] are such stages — they select or
//! reshape rows without changing the entity membership of the
//! result set. `Limit` steps are *not* commutative with SAMPLE
//! (they truncate by row order, which depends on the sample filter's
//! output cardinality) and therefore make the whole segment opaque
//! to the pass.
//!
//! # Joined-source scans
//!
//! [`PhysicalPlan::MergeSources`] is treated as a push-through leaf: the
//! same `(fraction, seed)` pair is stamped onto every sub-scan's
//! [`ScanPhysical::sample`]. Per `cohorts-aliases-joins.md` §3.4 the
//! SAMPLE hash is over the **value** of the entity key, which is
//! identical across tables sharing an entity-key type, so applying the
//! filter at every sub-scan is semantically equivalent to applying it
//! at the merged output. Extension from TASK-436.
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
    FusedSegmentPhysical, FusedSegmentStep, MergeSourcesPhysical, PhysicalPlan, SamplePhysical,
    SamplePushdown, ScanPhysical,
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

        PhysicalPlan::FusedSegment(seg) => {
            let FusedSegmentPhysical {
                input,
                steps,
                sparsity_factor,
                output_schema,
            } = seg;
            PhysicalPlan::FusedSegment(FusedSegmentPhysical {
                input: Box::new(pushdown_sample(*input)),
                steps,
                sparsity_factor,
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

        // Joined-source scans behave as leaves at this level: sub-scans
        // are `ScanPhysical`, not nested `PhysicalPlan` subtrees, so
        // there is nothing to recurse into. The push-through shape is
        // handled by `can_push_through` + `push_into_scan` above when a
        // Sample sits (through stateless stages) atop a MergeSources.
        PhysicalPlan::MergeSources(ms) => PhysicalPlan::MergeSources(ms),

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
/// [`PhysicalPlan::Scan`] or [`PhysicalPlan::MergeSources`].
/// `FusedSegment` is push-through only when every step is `Filter`
/// or `Project` — `Limit` steps are not commutative with Sample.
fn can_push_through(node: &PhysicalPlan) -> bool {
    match node {
        PhysicalPlan::Scan(_) => true,
        // Entity-aligned source JOIN: SAMPLE hashes entity-id value,
        // which is identical across all tables that share the entity
        // key (cohorts-aliases-joins.md §3.4). Pushing the same
        // (fraction, seed) into every sub-scan produces the atomic
        // cross-table entity set the design requires.
        PhysicalPlan::MergeSources(_) => true,
        PhysicalPlan::FusedSegment(seg) => {
            seg.steps.iter().all(|s| {
                matches!(
                    s,
                    FusedSegmentStep::Filter { .. } | FusedSegmentStep::Project(_)
                )
            }) && can_push_through(&seg.input)
        }
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
        PhysicalPlan::FusedSegment(seg) => {
            let FusedSegmentPhysical {
                input,
                steps,
                sparsity_factor,
                output_schema,
            } = seg;
            // `can_push_through` already verified every step is
            // Filter or Project (no Limit), so we recurse without
            // re-checking.
            PhysicalPlan::FusedSegment(FusedSegmentPhysical {
                input: Box::new(push_into_scan(*input, fraction, seed)),
                steps,
                sparsity_factor,
                output_schema,
            })
        }
        PhysicalPlan::MergeSources(ms) => {
            let MergeSourcesPhysical {
                mut tables,
                order,
                table_id_map,
                output_schema,
            } = ms;
            // Stamp the same (fraction, seed) onto every sub-scan.
            // Per cohorts-aliases-joins.md §3.4, SAMPLE's entity hash is
            // over the value of the entity key, so each sub-scan applies
            // the same filter independently and the merged output
            // reflects the atomic sampled entity set.
            for sub in tables.iter_mut() {
                debug_assert!(
                    sub.sample.is_none(),
                    "pushdown_sample: MergeSources sub-scan {} already has a sample",
                    sub.table
                );
                sub.sample = Some(SamplePushdown { fraction, seed });
            }
            PhysicalPlan::MergeSources(MergeSourcesPhysical {
                tables,
                order,
                table_id_map,
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
    use crate::logical::SortDirection;
    use crate::physical::{
        FusedSegmentPhysical, FusedSegmentStep, MergeSourcesPhysical, PhysicalPlan,
        ProjectPhysicalItem, SamplePhysical, ScanPhysical, SessionizePhysical,
        DEFAULT_FILTER_TILE_SIZE, DEFAULT_SPARSITY_FACTOR,
    };

    /// Wrap `input` in a single-step `FusedSegment([Filter(predicate)])`.
    fn filter_segment(predicate: CompiledExpr, input: PhysicalPlan) -> PhysicalPlan {
        let out = input.output_schema().clone();
        PhysicalPlan::FusedSegment(FusedSegmentPhysical {
            input: Box::new(input),
            steps: vec![FusedSegmentStep::Filter {
                predicate,
                tile_size: DEFAULT_FILTER_TILE_SIZE,
            }],
            sparsity_factor: DEFAULT_SPARSITY_FACTOR,
            output_schema: out,
        })
    }

    /// Wrap `input` in a single-step `FusedSegment([Project(items)])`.
    fn project_segment(items: Vec<ProjectPhysicalItem>, input: PhysicalPlan) -> PhysicalPlan {
        let out = input.output_schema().clone();
        PhysicalPlan::FusedSegment(FusedSegmentPhysical {
            input: Box::new(input),
            steps: vec![FusedSegmentStep::Project(items)],
            sparsity_factor: DEFAULT_SPARSITY_FACTOR,
            output_schema: out,
        })
    }

    /// Wrap `input` in a single-step `FusedSegment([Limit(count)])`.
    fn limit_segment(count: u64, input: PhysicalPlan) -> PhysicalPlan {
        let out = input.output_schema().clone();
        PhysicalPlan::FusedSegment(FusedSegmentPhysical {
            input: Box::new(input),
            steps: vec![FusedSegmentStep::Limit(count)],
            sparsity_factor: DEFAULT_SPARSITY_FACTOR,
            output_schema: out,
        })
    }

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
        let filter = filter_segment(trivial_and(), PhysicalPlan::Scan(make_scan()));
        let plan = sample_over(filter, 0.5, 42);
        let out = pushdown_sample(plan);
        let PhysicalPlan::FusedSegment(seg) = out else {
            panic!("expected FusedSegment at top, got {out:?}");
        };
        let PhysicalPlan::Scan(scan) = *seg.input else {
            panic!("expected Scan under FusedSegment");
        };
        let s = scan.sample.expect("sample attached");
        assert!((s.fraction - 0.5).abs() < f64::EPSILON);
        assert_eq!(s.seed, 42);
    }

    #[test]
    fn sample_over_project_over_scan_pushes_through_project() {
        let project = project_segment(vec![], PhysicalPlan::Scan(make_scan()));
        let plan = sample_over(project, 0.1, 99);
        let out = pushdown_sample(plan);
        let PhysicalPlan::FusedSegment(seg) = out else {
            panic!("expected FusedSegment, got {out:?}");
        };
        let PhysicalPlan::Scan(scan) = *seg.input else {
            panic!("expected Scan under FusedSegment");
        };
        assert!(scan.sample.is_some());
    }

    #[test]
    fn sample_over_limit_is_not_pushed() {
        // Limit changes row counts, so pushing Sample past it would
        // change semantics. The pass must leave Sample above the
        // FusedSegment that contains the Limit step.
        let limited = limit_segment(10, PhysicalPlan::Scan(make_scan()));
        let plan = sample_over(limited, 0.3, 1);
        let out = pushdown_sample(plan);
        let PhysicalPlan::Sample(sample) = out else {
            panic!("expected Sample to remain, got {out:?}");
        };
        assert!((sample.fraction - 0.3).abs() < f64::EPSILON);
        let PhysicalPlan::FusedSegment(seg) = *sample.input else {
            panic!("expected FusedSegment");
        };
        let PhysicalPlan::Scan(scan) = *seg.input else {
            panic!("expected Scan under FusedSegment");
        };
        assert!(
            scan.sample.is_none(),
            "Limit step blocks pushdown; scan must have no sample"
        );
    }

    #[test]
    fn sample_over_segment_with_filter_and_limit_is_not_pushed() {
        // FusedSegment([Filter, Limit]) — the Limit step makes the
        // whole segment opaque to sample pushdown, even though the
        // Filter step would individually be pushable.
        let scan = PhysicalPlan::Scan(make_scan());
        let combined = PhysicalPlan::FusedSegment(FusedSegmentPhysical {
            input: Box::new(scan),
            steps: vec![
                FusedSegmentStep::Filter {
                    predicate: trivial_and(),
                    tile_size: DEFAULT_FILTER_TILE_SIZE,
                },
                FusedSegmentStep::Limit(5),
            ],
            sparsity_factor: DEFAULT_SPARSITY_FACTOR,
            output_schema: schema(),
        });
        let plan = sample_over(combined, 0.3, 1);
        let out = pushdown_sample(plan);
        let PhysicalPlan::Sample(_) = out else {
            panic!("expected Sample to remain over a Limit-containing segment");
        };
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
            entity_key_col: "entity_id".into(),
            input: Box::new(scan),
            output_schema: schema(),
            pre_fusion_output_schema: None,
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

    // ── TASK-436: SAMPLE pushdown through MergeSources ───────────────────

    fn make_merge_sources(n: usize) -> PhysicalPlan {
        let tables: Vec<ScanPhysical> = (0..n)
            .map(|i| ScanPhysical {
                table: format!("t{i}"),
                query_range: None,
                reader_range: None,
                scan_predicates: vec![],
                projected_columns: vec![],
                output_schema: schema(),
                entity_key_col: "entity_id".into(),
                timestamp_col: "ts".into(),
                sample: None,
            })
            .collect();
        PhysicalPlan::MergeSources(MergeSourcesPhysical {
            tables,
            order: vec![
                ("entity_id".into(), SortDirection::Asc),
                ("ts".into(), SortDirection::Asc),
            ],
            table_id_map: (0..n).map(|i| format!("t{i}")).collect(),
            output_schema: schema(),
        })
    }

    #[test]
    fn sample_over_merge_sources_pushes_into_every_sub_scan() {
        let plan = sample_over(make_merge_sources(3), 0.25, 7);
        let out = pushdown_sample(plan);
        let PhysicalPlan::MergeSources(ms) = out else {
            panic!("expected MergeSources after push (Sample elided), got {out:?}");
        };
        assert_eq!(ms.tables.len(), 3);
        for (i, sub) in ms.tables.iter().enumerate() {
            let s = sub
                .sample
                .as_ref()
                .unwrap_or_else(|| panic!("sub-scan {i} missing sample"));
            assert!(
                (s.fraction - 0.25).abs() < f64::EPSILON,
                "sub-scan {i} fraction mismatch"
            );
            assert_eq!(s.seed, 7, "sub-scan {i} seed mismatch");
        }
    }

    #[test]
    fn sample_over_filter_over_merge_sources_pushes_through() {
        // Filter step inside a FusedSegment between Sample and MergeSources
        // is still push-through because Filter is stateless /
        // entity-key-independent.
        let ms = make_merge_sources(2);
        let filter = filter_segment(trivial_and(), ms);
        let plan = sample_over(filter, 0.5, 42);
        let out = pushdown_sample(plan);
        let PhysicalPlan::FusedSegment(seg) = out else {
            panic!("expected FusedSegment at top, got {out:?}");
        };
        let PhysicalPlan::MergeSources(ms) = *seg.input else {
            panic!("expected MergeSources under FusedSegment");
        };
        for sub in &ms.tables {
            let s = sub.sample.as_ref().expect("sub-scan sample attached");
            assert!((s.fraction - 0.5).abs() < f64::EPSILON);
            assert_eq!(s.seed, 42);
        }
    }

    #[test]
    fn sample_over_project_over_merge_sources_pushes_through() {
        // Project step is the other stateless stage in `can_push_through`;
        // ensure it composes with the MergeSources leaf the same way
        // Filter does above.
        let ms = make_merge_sources(2);
        let project = project_segment(vec![], ms);
        let plan = sample_over(project, 0.2, 11);
        let out = pushdown_sample(plan);
        let PhysicalPlan::FusedSegment(seg) = out else {
            panic!("expected FusedSegment at top, got {out:?}");
        };
        let PhysicalPlan::MergeSources(ms) = *seg.input else {
            panic!("expected MergeSources under FusedSegment");
        };
        for sub in &ms.tables {
            let s = sub.sample.as_ref().expect("sub-scan sample attached");
            assert!((s.fraction - 0.2).abs() < f64::EPSILON);
            assert_eq!(s.seed, 11);
        }
    }

    #[test]
    fn sample_over_single_sub_merge_sources_still_pushes() {
        // Degenerate shape: MergeSources with a single sub-scan (n=1).
        // The planner doesn't produce this today — single-table queries
        // lower to a plain Scan — but the pass shouldn't rely on n >= 2.
        let plan = sample_over(make_merge_sources(1), 0.9, 99);
        let out = pushdown_sample(plan);
        let PhysicalPlan::MergeSources(ms) = out else {
            panic!("expected MergeSources after push, got {out:?}");
        };
        assert_eq!(ms.tables.len(), 1);
        let s = ms.tables[0]
            .sample
            .as_ref()
            .expect("sub-scan sample attached");
        assert!((s.fraction - 0.9).abs() < f64::EPSILON);
        assert_eq!(s.seed, 99);
    }

    #[test]
    fn merge_sources_without_sample_is_unchanged() {
        let plan = make_merge_sources(2);
        let out = pushdown_sample(plan);
        let PhysicalPlan::MergeSources(ms) = out else {
            panic!("expected MergeSources, got {out:?}");
        };
        assert!(ms.tables.iter().all(|t| t.sample.is_none()));
    }

    #[test]
    fn sample_over_limit_over_merge_sources_is_not_pushed() {
        // Limit is not commutative with Sample (changes row counts), so
        // the Sample must remain above the FusedSegment[Limit] even when
        // the leaf is MergeSources. The sub-scans' sample fields remain
        // None.
        let ms = make_merge_sources(2);
        let limited = limit_segment(10, ms);
        let plan = sample_over(limited, 0.3, 1);
        let out = pushdown_sample(plan);
        let PhysicalPlan::Sample(s) = out else {
            panic!("expected Sample to remain above Limit, got {out:?}");
        };
        let PhysicalPlan::FusedSegment(seg) = *s.input else {
            panic!("expected FusedSegment under Sample");
        };
        let PhysicalPlan::MergeSources(ms) = *seg.input else {
            panic!("expected MergeSources under FusedSegment");
        };
        assert!(ms.tables.iter().all(|t| t.sample.is_none()));
    }
}
