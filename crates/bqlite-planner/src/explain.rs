//! EXPLAIN tree builder and plain-text formatter (TASK-229, TASK-317).
//!
//! Walks a [`PhysicalPlan`] and produces an [`ExplainNode`] tree — a
//! stable, string-based mirror of the plan — then renders it as
//! indented plain-text. The separation lets tests assert on the
//! structured tree and on the rendered text independently.
//!
//! ## Wave 2 scope
//!
//! Four Wave 2 pipeline variants: `Scan`, `Filter`, `Project`, `Limit`.
//! `Explain` is transparently unwrapped so callers can pass a
//! `PhysicalPlan::Explain` directly. DDL / DML leaf nodes are not
//! reachable from `build_explain_node`.
//!
//! ## Wave 3 scope (TASK-317)
//!
//! Four additional Wave 3 pipeline variants: `SequenceMatch`, `Aggregate`,
//! `Sort`, `Distinct`. Their physical descriptors are produced by TASK-318;
//! these arms prevent panics when an `EXPLAIN` wraps a Wave 3 query.
//!
//! ## Wave 4 scope (TASK-424)
//!
//! Six additional Wave 4 pipeline variants: `Sessionize`, `EventSelect`,
//! `Attribute`, `SubqueryFilter`, `Sample`, `MergeSources`. Durations are
//! formatted via `format_nanos`; `EventSelectKind` via
//! `format_event_select_kind`. `MergeSources` renders child scan nodes.

use bqlite_ast::expr::{BinaryOp, CompareOp, UnaryOp};
use bqlite_core::{AggFunction, PropertyValue};

use crate::compiled::{CompiledExpr, CompiledNode};
use crate::logical::SortDirection;
use crate::physical::PhysicalPlan;

// ─────────────────────────────────────────────────────────────────────────────
// ExplainNode
// ─────────────────────────────────────────────────────────────────────────────

/// Stable, string-based mirror of a physical plan node.
///
/// Produced by [`build_explain_node`] and rendered by
/// [`format_explain`]. Using strings as leaves (rather than typed
/// values) means EXPLAIN output is stable across executor changes and
/// refactors that touch `CompiledExpr` internals.
#[derive(Debug, Clone, PartialEq)]
pub enum ExplainNode {
    Scan {
        table: String,
        time_range: String,
        predicates: Vec<String>,
        columns: Vec<String>,
    },
    Filter {
        predicate: String,
        input: Box<ExplainNode>,
    },
    Project {
        columns: Vec<String>,
        input: Box<ExplainNode>,
    },
    Limit {
        count: u64,
        input: Box<ExplainNode>,
    },
    /// Wave 3: NFA-based sequence / pattern-match operator.
    SequenceMatch {
        /// Human-readable strategy name (`"StepCounter"`, `"ConsecutiveMatcher"`,
        /// `"FullNfa"`).
        strategy: String,
        /// Number of NFA states in the compiled program.
        step_count: usize,
        /// Whether the operator emits intermediate step-reached events.
        emit_all: bool,
        input: Box<ExplainNode>,
    },
    /// Wave 3: hash aggregate operator.
    Aggregate {
        /// Formatted aggregate expressions (`"COUNT(*) AS n"`, `"SUM(amount) AS total"`).
        aggregates: Vec<String>,
        /// Output column names of the group-by key expressions.
        group_by: Vec<String>,
        /// Hard cap on group cardinality.
        max_groups: usize,
        input: Box<ExplainNode>,
    },
    /// Wave 3: in-memory sort operator.
    Sort {
        /// Formatted sort key expressions (`"amount ASC"`, `"ts DESC"`).
        keys: Vec<String>,
        /// Hard cap on total input rows.
        max_rows: usize,
        input: Box<ExplainNode>,
    },
    /// Wave 3: hash-based row deduplication operator.
    Distinct {
        /// Hard cap on distinct row count.
        max_groups: usize,
        input: Box<ExplainNode>,
    },

    // ── Wave 4 variants ────────────────────────────────────────────────────
    /// Wave 4: session assignment operator.
    Sessionize {
        /// Gap threshold in nanoseconds formatted as human-readable duration.
        gap: String,
        /// Explicit end-event types (empty = gap-only mode).
        end_events: Vec<String>,
        input: Box<ExplainNode>,
    },
    /// Wave 4: per-entity event sub-selection (FIRST/LAST/NTH).
    EventSelect {
        /// Human-readable selection mode (`"FIRST"`, `"LAST"`, `"NTH(3)"`).
        kind: String,
        /// Eligible event types.
        event_types: Vec<String>,
        /// Whether a per-event predicate is applied.
        has_predicate: bool,
        input: Box<ExplainNode>,
    },
    /// Wave 4: multi-touch attribution operator.
    Attribute {
        /// Conversion event types.
        conversion_events: Vec<String>,
        /// Touchpoint event types.
        touchpoint_events: Vec<String>,
        /// Lookback window formatted as human-readable duration.
        window: String,
        input: Box<ExplainNode>,
    },
    /// Wave 4: cohort-based entity filtering.
    SubqueryFilter {
        /// Number of LHS columns in the IN check.
        column_count: usize,
        input: Box<ExplainNode>,
        subquery: Box<ExplainNode>,
    },
    /// Wave 4: deterministic entity-level sampling.
    Sample {
        /// Fraction of entities to keep.
        fraction: f64,
        /// Resolved sampling seed.
        seed: i64,
        input: Box<ExplainNode>,
    },
    /// Wave 4: N-ary entity-aligned source merge.
    MergeSources {
        /// Table names being merged, in JOIN-clause order.
        tables: Vec<String>,
        /// Merge sort order formatted as column names with direction.
        order: Vec<String>,
        /// Child scan nodes, one per joined table.
        inputs: Vec<ExplainNode>,
    },
}

// ─────────────────────────────────────────────────────────────────────────────
// PhysicalPlan → ExplainNode
// ─────────────────────────────────────────────────────────────────────────────

/// Walk a [`PhysicalPlan`] and produce the corresponding
/// [`ExplainNode`] tree.
///
/// [`PhysicalPlan::Explain`] is transparently unwrapped — callers
/// may pass either the raw pipeline plan or the `Explain`-wrapped
/// version.
///
/// # Panics
///
/// Panics if called on a DDL / DML leaf (`CreateTable`, `DropTable`,
/// `AlterTableAddColumn`, `Describe`, `Insert`). These nodes carry no
/// inner pipeline and cannot be rendered as a query-plan tree.
pub fn build_explain_node(plan: &PhysicalPlan) -> ExplainNode {
    match plan {
        PhysicalPlan::Scan(scan) => {
            // In the normal engine path `projected_columns` is always
            // non-empty after `plan()` runs the pruning pass (at minimum
            // it contains the mandatory entity-key and timestamp columns).
            // The `is_empty()` fallback is only reachable for manually
            // constructed `ScanPhysical` descriptors that bypass `plan()`,
            // e.g., unit tests that build descriptors directly.
            let columns = if scan.projected_columns.is_empty() {
                scan.output_schema
                    .columns()
                    .iter()
                    .map(|c| c.name.clone())
                    .collect()
            } else {
                scan.projected_columns.clone()
            };
            ExplainNode::Scan {
                table: scan.table.clone(),
                time_range: format_time_range(&scan.query_range),
                predicates: scan.scan_predicates.iter().map(format_expr).collect(),
                columns,
            }
        }
        PhysicalPlan::Filter(filter) => ExplainNode::Filter {
            predicate: format_expr(&filter.predicate),
            input: Box::new(build_explain_node(&filter.input)),
        },
        PhysicalPlan::Project(project) => ExplainNode::Project {
            columns: project
                .expressions
                .iter()
                .map(|item| item.output_name.clone())
                .collect(),
            input: Box::new(build_explain_node(&project.input)),
        },
        PhysicalPlan::Limit(limit) => ExplainNode::Limit {
            count: limit.count,
            input: Box::new(build_explain_node(&limit.input)),
        },
        // Transparently unwrap EXPLAIN so callers can pass either form.
        PhysicalPlan::Explain(explain) => build_explain_node(&explain.plan),
        // ── Wave 3 variants ────────────────────────────────────────────────
        PhysicalPlan::SequenceMatch(seq) => ExplainNode::SequenceMatch {
            strategy: format_match_strategy(&seq.strategy),
            step_count: seq.compiled_nfa.states.len(),
            emit_all: seq.demand.needs_step_reached,
            input: Box::new(build_explain_node(&seq.input)),
        },
        PhysicalPlan::Aggregate(agg) => ExplainNode::Aggregate {
            aggregates: agg.aggregates.iter().map(format_compiled_agg).collect(),
            group_by: agg
                .group_by
                .iter()
                .map(|(_expr, name)| name.clone())
                .collect(),
            max_groups: agg.max_groups,
            input: Box::new(build_explain_node(&agg.input)),
        },
        PhysicalPlan::Sort(sort) => ExplainNode::Sort {
            keys: sort
                .keys
                .iter()
                .map(|(expr, dir)| format!("{} {}", format_expr(expr), format_sort_dir(dir)))
                .collect(),
            max_rows: sort.max_rows,
            input: Box::new(build_explain_node(&sort.input)),
        },
        PhysicalPlan::Distinct(dist) => ExplainNode::Distinct {
            max_groups: dist.max_groups,
            input: Box::new(build_explain_node(&dist.input)),
        },
        // ── Wave 4 variants ────────────────────────────────────────────────
        PhysicalPlan::Sessionize(sess) => ExplainNode::Sessionize {
            gap: format_nanos(sess.gap_ns),
            end_events: sess.end_events.clone(),
            input: Box::new(build_explain_node(&sess.input)),
        },
        PhysicalPlan::EventSelect(es) => ExplainNode::EventSelect {
            kind: format_event_select_kind(&es.kind),
            event_types: es.event_types.clone(),
            has_predicate: es.predicate.is_some(),
            input: Box::new(build_explain_node(&es.input)),
        },
        PhysicalPlan::Attribute(attr) => ExplainNode::Attribute {
            conversion_events: attr.conversion_events.clone(),
            touchpoint_events: attr.touchpoint_events.clone(),
            window: format_nanos(attr.window_ns),
            input: Box::new(build_explain_node(&attr.input)),
        },
        PhysicalPlan::SubqueryFilter(sqf) => ExplainNode::SubqueryFilter {
            column_count: sqf.lhs_columns.len(),
            input: Box::new(build_explain_node(&sqf.input)),
            subquery: Box::new(build_explain_node(&sqf.subquery)),
        },
        PhysicalPlan::Sample(sample) => ExplainNode::Sample {
            fraction: sample.fraction,
            seed: sample.seed,
            input: Box::new(build_explain_node(&sample.input)),
        },
        PhysicalPlan::MergeSources(merge) => ExplainNode::MergeSources {
            tables: merge.table_id_map.clone(),
            order: merge
                .order
                .iter()
                .map(|(col, dir)| format!("{col} {}", format_sort_dir(dir)))
                .collect(),
            inputs: merge
                .tables
                .iter()
                .map(|t| build_explain_node(&PhysicalPlan::Scan(t.clone())))
                .collect(),
        },
        other => panic!("build_explain_node: not a query plan node — {other:?}"),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Text formatter
// ─────────────────────────────────────────────────────────────────────────────

/// Render an [`ExplainNode`] tree as indented plain text.
///
/// Uses 2-space indentation per nesting level. No Unicode box-drawing
/// characters — plain ASCII only per TASK-229 spec.
///
/// The returned string has **no trailing newline**.
pub fn format_explain(node: &ExplainNode) -> String {
    let mut buf = String::new();
    write_node(node, 0, &mut buf);
    // Strip the trailing newline added by write_node.
    if buf.ends_with('\n') {
        buf.pop();
    }
    buf
}

fn write_node(node: &ExplainNode, indent: usize, buf: &mut String) {
    let pad = "  ".repeat(indent);
    match node {
        ExplainNode::Scan {
            table,
            time_range,
            predicates,
            columns,
        } => {
            buf.push_str(&format!("{pad}Scan({table})\n"));
            buf.push_str(&format!("{pad}  time_range : {time_range}\n"));
            if predicates.is_empty() {
                buf.push_str(&format!("{pad}  predicates : none\n"));
            } else {
                buf.push_str(&format!("{pad}  predicates : {}\n", predicates[0]));
                for p in &predicates[1..] {
                    buf.push_str(&format!("{pad}               {p}\n"));
                }
            }
            let cols = columns.join(", ");
            buf.push_str(&format!("{pad}  columns    : [{cols}]\n"));
        }
        ExplainNode::Filter { predicate, input } => {
            buf.push_str(&format!("{pad}Filter\n"));
            buf.push_str(&format!("{pad}  predicate : {predicate}\n"));
            write_node(input, indent + 1, buf);
        }
        ExplainNode::Project { columns, input } => {
            let cols = columns.join(", ");
            buf.push_str(&format!("{pad}Project [{cols}]\n"));
            write_node(input, indent + 1, buf);
        }
        ExplainNode::Limit { count, input } => {
            buf.push_str(&format!("{pad}Limit {count}\n"));
            write_node(input, indent + 1, buf);
        }
        ExplainNode::SequenceMatch {
            strategy,
            step_count,
            emit_all,
            input,
        } => {
            buf.push_str(&format!("{pad}SequenceMatch\n"));
            buf.push_str(&format!("{pad}  strategy   : {strategy}\n"));
            buf.push_str(&format!("{pad}  steps      : {step_count}\n"));
            buf.push_str(&format!("{pad}  emit_all   : {emit_all}\n"));
            write_node(input, indent + 1, buf);
        }
        ExplainNode::Aggregate {
            aggregates,
            group_by,
            max_groups,
            input,
        } => {
            buf.push_str(&format!("{pad}Aggregate\n"));
            if aggregates.is_empty() {
                buf.push_str(&format!("{pad}  agg        : none\n"));
            } else {
                buf.push_str(&format!("{pad}  agg        : {}\n", aggregates[0]));
                for a in &aggregates[1..] {
                    buf.push_str(&format!("{pad}               {a}\n"));
                }
            }
            if group_by.is_empty() {
                buf.push_str(&format!("{pad}  group_by   : none\n"));
            } else {
                buf.push_str(&format!("{pad}  group_by   : {}\n", group_by.join(", ")));
            }
            buf.push_str(&format!("{pad}  max_groups : {max_groups}\n"));
            write_node(input, indent + 1, buf);
        }
        ExplainNode::Sort {
            keys,
            max_rows,
            input,
        } => {
            buf.push_str(&format!("{pad}Sort\n"));
            if keys.is_empty() {
                buf.push_str(&format!("{pad}  keys       : none\n"));
            } else {
                buf.push_str(&format!("{pad}  keys       : {}\n", keys[0]));
                for k in &keys[1..] {
                    buf.push_str(&format!("{pad}               {k}\n"));
                }
            }
            buf.push_str(&format!("{pad}  max_rows   : {max_rows}\n"));
            write_node(input, indent + 1, buf);
        }
        ExplainNode::Distinct { max_groups, input } => {
            buf.push_str(&format!("{pad}Distinct\n"));
            buf.push_str(&format!("{pad}  max_groups : {max_groups}\n"));
            write_node(input, indent + 1, buf);
        }
        // ── Wave 4 variants ────────────────────────────────────────────────
        ExplainNode::Sessionize {
            gap,
            end_events,
            input,
        } => {
            buf.push_str(&format!("{pad}Sessionize\n"));
            buf.push_str(&format!("{pad}  gap        : {gap}\n"));
            if end_events.is_empty() {
                buf.push_str(&format!("{pad}  end_events : none\n"));
            } else {
                buf.push_str(&format!("{pad}  end_events : {}\n", end_events.join(", ")));
            }
            write_node(input, indent + 1, buf);
        }
        ExplainNode::EventSelect {
            kind,
            event_types,
            has_predicate,
            input,
        } => {
            buf.push_str(&format!("{pad}EventSelect\n"));
            buf.push_str(&format!("{pad}  kind       : {kind}\n"));
            buf.push_str(&format!("{pad}  events     : {}\n", event_types.join(", ")));
            buf.push_str(&format!("{pad}  predicate  : {has_predicate}\n"));
            write_node(input, indent + 1, buf);
        }
        ExplainNode::Attribute {
            conversion_events,
            touchpoint_events,
            window,
            input,
        } => {
            buf.push_str(&format!("{pad}Attribute\n"));
            buf.push_str(&format!(
                "{pad}  conversion : {}\n",
                conversion_events.join(", ")
            ));
            buf.push_str(&format!(
                "{pad}  touchpoints: {}\n",
                touchpoint_events.join(", ")
            ));
            buf.push_str(&format!("{pad}  window     : {window}\n"));
            write_node(input, indent + 1, buf);
        }
        ExplainNode::SubqueryFilter {
            column_count,
            input,
            subquery,
        } => {
            buf.push_str(&format!("{pad}SubqueryFilter\n"));
            buf.push_str(&format!("{pad}  columns    : {column_count}\n"));
            buf.push_str(&format!("{pad}  subquery:\n"));
            write_node(subquery, indent + 2, buf);
            write_node(input, indent + 1, buf);
        }
        ExplainNode::Sample {
            fraction,
            seed,
            input,
        } => {
            buf.push_str(&format!("{pad}Sample\n"));
            buf.push_str(&format!("{pad}  fraction   : {fraction}\n"));
            buf.push_str(&format!("{pad}  seed       : {seed}\n"));
            write_node(input, indent + 1, buf);
        }
        ExplainNode::MergeSources {
            tables,
            order,
            inputs,
        } => {
            buf.push_str(&format!("{pad}MergeSources\n"));
            buf.push_str(&format!("{pad}  tables     : {}\n", tables.join(", ")));
            if order.is_empty() {
                buf.push_str(&format!("{pad}  order      : default\n"));
            } else {
                buf.push_str(&format!("{pad}  order      : {}\n", order.join(", ")));
            }
            for input in inputs {
                write_node(input, indent + 1, buf);
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Expression formatting
// ─────────────────────────────────────────────────────────────────────────────

/// Format a [`CompiledExpr`] as a human-readable SQL-like string.
pub fn format_expr(expr: &CompiledExpr) -> String {
    match &expr.node {
        CompiledNode::Literal(v) => format_value(v),
        CompiledNode::Column { name, .. } => name.clone(),
        CompiledNode::Arith {
            op, left, right, ..
        } => {
            format!(
                "{} {} {}",
                format_expr(left),
                format_binary_op(*op),
                format_expr(right)
            )
        }
        CompiledNode::Compare {
            op, left, right, ..
        } => {
            format!(
                "{} {} {}",
                format_expr(left),
                format_compare_op(*op),
                format_expr(right)
            )
        }
        CompiledNode::Unary { op, operand, .. } => match op {
            UnaryOp::Negate => format!("-{}", format_expr(operand)),
            UnaryOp::Plus => format!("+{}", format_expr(operand)),
        },
        CompiledNode::And { operands, .. } => {
            let parts: Vec<_> = operands.iter().map(format_expr).collect();
            parts.join(" AND ")
        }
        CompiledNode::Or { operands, .. } => {
            let parts: Vec<_> = operands.iter().map(format_expr).collect();
            parts.join(" OR ")
        }
        CompiledNode::Not(inner) => format!("NOT {}", format_expr(inner)),
        CompiledNode::IsNull { input, negated } => {
            if *negated {
                format!("{} IS NOT NULL", format_expr(input))
            } else {
                format!("{} IS NULL", format_expr(input))
            }
        }
        CompiledNode::FunctionCall {
            signature, args, ..
        } => {
            let arg_strs: Vec<_> = args.iter().map(format_expr).collect();
            format!("{}({})", signature.name, arg_strs.join(", "))
        }
        CompiledNode::Cast {
            input, target_type, ..
        } => {
            format!("CAST({} AS {})", format_expr(input), target_type)
        }
        // Implicit coercions are transparent to the user.
        CompiledNode::ImplicitCoerce { input, .. } => format_expr(input),
        CompiledNode::InLiteralSet {
            input,
            values,
            negated,
            ..
        } => {
            let vals: Vec<_> = values.iter().map(format_value).collect();
            let set = vals.join(", ");
            if *negated {
                format!("{} NOT IN ({})", format_expr(input), set)
            } else {
                format!("{} IN ({})", format_expr(input), set)
            }
        }
    }
}

/// Format a resolved `TimeRange` for EXPLAIN output.
/// Displays the inclusive start and exclusive end as nanosecond epoch values,
/// e.g. `[1700000000000000000, 1702678400000000000)`.
/// `None` (unbounded) is rendered as `none`.
fn format_time_range(tr: &Option<bqlite_core::TimeRange>) -> String {
    match tr {
        None => "none".to_string(),
        Some(r) => format!("[{}, {})", r.start.as_nanos(), r.end.as_nanos()),
    }
}

/// Format nanoseconds as a human-readable duration string. Picks the
/// largest unit that divides evenly, falling back to nanoseconds.
#[cfg(test)]
fn format_duration_ns(ns: i64) -> String {
    const NS_PER_DAY: i64 = 86_400_000_000_000;
    const NS_PER_HOUR: i64 = 3_600_000_000_000;
    const NS_PER_MIN: i64 = 60_000_000_000;
    const NS_PER_SEC: i64 = 1_000_000_000;
    const NS_PER_MS: i64 = 1_000_000;
    const NS_PER_US: i64 = 1_000;

    if ns > 0 && ns % NS_PER_DAY == 0 {
        format!("{}d", ns / NS_PER_DAY)
    } else if ns > 0 && ns % NS_PER_HOUR == 0 {
        format!("{}h", ns / NS_PER_HOUR)
    } else if ns > 0 && ns % NS_PER_MIN == 0 {
        format!("{}m", ns / NS_PER_MIN)
    } else if ns > 0 && ns % NS_PER_SEC == 0 {
        format!("{}s", ns / NS_PER_SEC)
    } else if ns > 0 && ns % NS_PER_MS == 0 {
        format!("{}ms", ns / NS_PER_MS)
    } else if ns > 0 && ns % NS_PER_US == 0 {
        format!("{}us", ns / NS_PER_US)
    } else {
        format!("{ns}ns")
    }
}

fn format_value(v: &PropertyValue) -> String {
    match v {
        PropertyValue::Null => "NULL".to_string(),
        PropertyValue::Bool(b) => b.to_string(),
        PropertyValue::Int(n) => n.to_string(),
        PropertyValue::Float(f) => f.to_string(),
        // Quote strings SQL-style.
        PropertyValue::String(s) => format!("'{s}'"),
        // Timestamp: render nanoseconds — no time-zone dependency.
        PropertyValue::Timestamp(ns) => ns.to_string(),
        // List / Map do not appear in Wave 2 plan predicates.
        PropertyValue::List(elems) => {
            let parts: Vec<_> = elems.iter().map(format_value).collect();
            format!("[{}]", parts.join(", "))
        }
        PropertyValue::Map(pairs) => {
            let parts: Vec<_> = pairs
                .iter()
                .map(|(k, v)| format!("{k}: {}", format_value(v)))
                .collect();
            format!("{{{}}}", parts.join(", "))
        }
    }
}

fn format_binary_op(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Add => "+",
        BinaryOp::Subtract => "-",
        BinaryOp::Multiply => "*",
        BinaryOp::Divide => "/",
        BinaryOp::Modulo => "%",
    }
}

fn format_compare_op(op: CompareOp) -> &'static str {
    match op {
        CompareOp::Equal => "=",
        CompareOp::NotEqual => "!=",
        CompareOp::Less => "<",
        CompareOp::LessOrEqual => "<=",
        CompareOp::Greater => ">",
        CompareOp::GreaterOrEqual => ">=",
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Wave 3 formatting helpers
// ─────────────────────────────────────────────────────────────────────────────

fn format_match_strategy(strategy: &crate::compile::MatchStrategy) -> String {
    match strategy {
        crate::compile::MatchStrategy::StepCounter => "StepCounter".to_string(),
        crate::compile::MatchStrategy::ConsecutiveMatcher => "ConsecutiveMatcher".to_string(),
        crate::compile::MatchStrategy::FullNfa => "FullNfa".to_string(),
    }
}

/// Format a nanosecond duration as a human-readable string.
///
/// Uses the largest whole unit that fits: days, hours, minutes, seconds,
/// milliseconds, or raw nanoseconds.
fn format_nanos(ns: i64) -> String {
    const NS_PER_MS: i64 = 1_000_000;
    const NS_PER_SEC: i64 = 1_000_000_000;
    const NS_PER_MIN: i64 = 60 * NS_PER_SEC;
    const NS_PER_HOUR: i64 = 60 * NS_PER_MIN;
    const NS_PER_DAY: i64 = 24 * NS_PER_HOUR;

    if ns > 0 && ns % NS_PER_DAY == 0 {
        format!("{}d", ns / NS_PER_DAY)
    } else if ns > 0 && ns % NS_PER_HOUR == 0 {
        format!("{}h", ns / NS_PER_HOUR)
    } else if ns > 0 && ns % NS_PER_MIN == 0 {
        format!("{}m", ns / NS_PER_MIN)
    } else if ns > 0 && ns % NS_PER_SEC == 0 {
        format!("{}s", ns / NS_PER_SEC)
    } else if ns > 0 && ns % NS_PER_MS == 0 {
        format!("{}ms", ns / NS_PER_MS)
    } else {
        format!("{ns}ns")
    }
}

/// Format an [`EventSelectKind`] for EXPLAIN output.
fn format_event_select_kind(kind: &crate::logical::EventSelectKind) -> String {
    match kind {
        crate::logical::EventSelectKind::First => "FIRST".to_string(),
        crate::logical::EventSelectKind::Last => "LAST".to_string(),
        crate::logical::EventSelectKind::Nth(n) => format!("NTH({n})"),
    }
}

fn format_compiled_agg(agg: &crate::physical::CompiledAgg) -> String {
    let func_name = format_agg_function(agg.function);
    let arg_str = match &agg.arg {
        None => "*".to_string(),
        Some(e) => format_expr(e),
    };
    format!("{func_name}({arg_str}) AS {}", agg.output_name)
}

fn format_agg_function(func: AggFunction) -> &'static str {
    match func {
        AggFunction::Count => "COUNT",
        AggFunction::CountColumn => "COUNT",
        AggFunction::CountDistinct => "COUNT_DISTINCT",
        AggFunction::Sum => "SUM",
        AggFunction::Min => "MIN",
        AggFunction::Max => "MAX",
        AggFunction::Avg => "AVG",
        AggFunction::P50 => "P50",
        AggFunction::P90 => "P90",
        AggFunction::P95 => "P95",
        AggFunction::P99 => "P99",
    }
}

fn format_sort_dir(dir: &SortDirection) -> &'static str {
    match dir {
        SortDirection::Asc => "ASC",
        SortDirection::Desc => "DESC",
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use bqlite_ast::expr::{CompareOp, Expr, Literal, Spanned};
    use bqlite_ast::pipeline::{Pipeline, Source, TableRef};
    use bqlite_ast::span::{Name, Span};
    use bqlite_ast::{PipelineStage, Statement};
    use bqlite_core::catalog::unknown_table_error;
    use bqlite_core::property::BqlType;
    use bqlite_core::schema::{ColumnDef, TableSchema};
    use bqlite_core::{Catalog, Result};

    use super::*;
    use crate::physical::ScanPhysical;
    use crate::plan;

    // ── Test catalog ─────────────────────────────────────────────────────────

    #[derive(Debug, Default)]
    struct TestCatalog {
        tables: BTreeMap<String, TableSchema>,
    }

    impl TestCatalog {
        fn with(mut self, schema: TableSchema) -> Self {
            self.tables.insert(schema.name().to_string(), schema);
            self
        }
    }

    impl Catalog for TestCatalog {
        fn resolve_table(&self, name: &str) -> Result<TableSchema> {
            self.tables
                .get(name)
                .cloned()
                .ok_or_else(|| unknown_table_error(name))
        }
        fn list_tables(&self) -> Vec<&str> {
            self.tables.keys().map(String::as_str).collect()
        }
    }

    /// 3-column events schema (entity_id, ts, event_type).
    fn events_schema() -> TableSchema {
        TableSchema::new(
            "events",
            vec![
                ColumnDef::required("entity_id", BqlType::String),
                ColumnDef::required("ts", BqlType::Timestamp),
                ColumnDef::required("event_type", BqlType::String),
            ],
            "entity_id",
            "ts",
            "event_type",
        )
        .unwrap()
    }

    fn table_ref(name: &str) -> TableRef {
        TableRef {
            name: Name::synthetic(name),
            span: Span::EMPTY,
        }
    }

    fn bare_pipeline(name: &str) -> Pipeline {
        Pipeline {
            source: Source {
                primary: table_ref(name),
                joins: vec![],
                time_range: None,
                span: Span::EMPTY,
            },
            stages: vec![],
            span: Span::EMPTY,
        }
    }

    fn spanned<T>(t: T) -> Spanned<T> {
        Spanned::new(t, Span::EMPTY)
    }

    // ── format_expr ───────────────────────────────────────────────────────────

    #[test]
    fn format_expr_column_equals_string_literal() {
        use crate::compiled::{ArrowKernelId, CompareKernel, CompiledNode};
        let expr = CompiledExpr {
            node: CompiledNode::Compare {
                op: CompareOp::Equal,
                left: Box::new(CompiledExpr {
                    node: CompiledNode::Column {
                        index: 0,
                        name: "entity_id".to_string(),
                    },
                    result_type: BqlType::String,
                    nullable: false,
                }),
                right: Box::new(CompiledExpr {
                    node: CompiledNode::Literal(PropertyValue::String("user1".into())),
                    result_type: BqlType::String,
                    nullable: false,
                }),
                kernel: CompareKernel::ArrowKernel(ArrowKernelId::EqString),
            },
            result_type: BqlType::Bool,
            nullable: false,
        };
        assert_eq!(format_expr(&expr), "entity_id = 'user1'");
    }

    #[test]
    fn format_expr_is_null() {
        use crate::compiled::CompiledNode;
        let expr = CompiledExpr {
            node: CompiledNode::IsNull {
                input: Box::new(CompiledExpr {
                    node: CompiledNode::Column {
                        index: 1,
                        name: "amount".to_string(),
                    },
                    result_type: BqlType::Float,
                    nullable: true,
                }),
                negated: false,
            },
            result_type: BqlType::Bool,
            nullable: false,
        };
        assert_eq!(format_expr(&expr), "amount IS NULL");
    }

    #[test]
    fn format_expr_in_literal_set() {
        use crate::compiled::{CompiledNode, InSetKernel};
        let expr = CompiledExpr {
            node: CompiledNode::InLiteralSet {
                input: Box::new(CompiledExpr {
                    node: CompiledNode::Column {
                        index: 2,
                        name: "event_type".to_string(),
                    },
                    result_type: BqlType::String,
                    nullable: false,
                }),
                values: vec![
                    PropertyValue::String("click".into()),
                    PropertyValue::String("view".into()),
                ],
                negated: false,
                kernel: InSetKernel::ArrowIsIn,
            },
            result_type: BqlType::Bool,
            nullable: false,
        };
        assert_eq!(format_expr(&expr), "event_type IN ('click', 'view')");
    }

    // ── ExplainNode construction from PhysicalPlan ────────────────────────────

    #[test]
    fn build_node_bare_scan_reports_all_schema_columns() {
        // A bare `FROM events` — projected_columns is empty, so
        // ExplainNode must fall back to the full output schema columns.
        let catalog = TestCatalog::default().with(events_schema());
        let physical = plan(Statement::Query(bare_pipeline("events")), &catalog, 0).unwrap();
        let node = build_explain_node(&physical);
        match node {
            ExplainNode::Scan {
                table,
                time_range,
                predicates,
                columns,
            } => {
                assert_eq!(table, "events");
                assert_eq!(time_range, "none");
                assert!(predicates.is_empty());
                // Schema has entity_id, ts, event_type plus two
                // system columns (__seq_id, __batch_id).
                assert!(columns.contains(&"entity_id".to_string()));
                assert!(columns.contains(&"ts".to_string()));
                assert!(columns.contains(&"event_type".to_string()));
            }
            other => panic!("expected Scan node, got {other:?}"),
        }
    }

    #[test]
    fn build_node_explain_physical_unwraps_to_inner_plan() {
        // `EXPLAIN events` — the outer Explain wrapper must be stripped
        // so the caller gets the inner pipeline's ExplainNode.
        let catalog = TestCatalog::default().with(events_schema());
        let physical = plan(Statement::Explain(bare_pipeline("events")), &catalog, 0).unwrap();
        // The physical plan is Explain(Scan(...)). build_explain_node
        // should unwrap to produce Scan, not some Explain node.
        let node = build_explain_node(&physical);
        assert!(
            matches!(node, ExplainNode::Scan { .. }),
            "expected Scan after unwrapping Explain, got {node:?}"
        );
    }

    #[test]
    fn build_node_filter_over_scan_carries_predicate_string() {
        let catalog = TestCatalog::default().with(events_schema());
        let mut pipeline = bare_pipeline("events");
        pipeline.stages.push(PipelineStage::Where {
            predicate: spanned(Expr::Compare {
                op: CompareOp::Equal,
                left: Box::new(spanned(Expr::Column(Name::synthetic("entity_id")))),
                right: Box::new(spanned(Expr::Literal(Literal::String("u1".into())))),
            }),
            span: Span::EMPTY,
        });
        let physical = plan(Statement::Query(pipeline), &catalog, 0).unwrap();
        let node = build_explain_node(&physical);
        // After pushdown the filter may be elided; either way the
        // predicate string should appear somewhere in the tree.
        let text = format_explain(&node);
        assert!(
            text.contains("entity_id") && text.contains("u1"),
            "predicate not visible in explain output:\n{text}"
        );
    }

    #[test]
    fn build_node_limit_carries_count() {
        let catalog = TestCatalog::default().with(events_schema());
        let mut pipeline = bare_pipeline("events");
        pipeline.stages.push(PipelineStage::Limit {
            count: 10,
            span: Span::EMPTY,
        });
        let physical = plan(Statement::Query(pipeline), &catalog, 0).unwrap();
        let node = build_explain_node(&physical);
        match node {
            ExplainNode::Limit { count, .. } => assert_eq!(count, 10),
            other => panic!("expected Limit node, got {other:?}"),
        }
    }

    // ── format_explain text output ────────────────────────────────────────────

    #[test]
    fn format_bare_scan_contains_key_fields() {
        let node = ExplainNode::Scan {
            table: "events".to_string(),
            time_range: "none".to_string(),
            predicates: vec![],
            columns: vec!["entity_id".to_string(), "ts".to_string()],
        };
        let text = format_explain(&node);
        assert!(text.contains("Scan(events)"), "missing header: {text}");
        assert!(text.contains("time_range"), "missing time_range: {text}");
        assert!(text.contains("none"), "missing time_range value: {text}");
        assert!(text.contains("predicates"), "missing predicates: {text}");
        assert!(text.contains("columns"), "missing columns: {text}");
        assert!(text.contains("entity_id"), "missing column: {text}");
        assert!(text.contains("ts"), "missing column: {text}");
    }

    #[test]
    fn format_scan_with_predicate_shows_predicate() {
        let node = ExplainNode::Scan {
            table: "events".to_string(),
            time_range: "none".to_string(),
            predicates: vec!["entity_id = 'user1'".to_string()],
            columns: vec!["entity_id".to_string()],
        };
        let text = format_explain(&node);
        assert!(
            text.contains("entity_id = 'user1'"),
            "predicate missing: {text}"
        );
    }

    #[test]
    fn format_filter_over_scan_indents_scan() {
        let node = ExplainNode::Filter {
            predicate: "amount > 100".to_string(),
            input: Box::new(ExplainNode::Scan {
                table: "events".to_string(),
                time_range: "none".to_string(),
                predicates: vec![],
                columns: vec!["amount".to_string()],
            }),
        };
        let text = format_explain(&node);
        assert!(text.contains("Filter"), "missing Filter: {text}");
        assert!(text.contains("amount > 100"), "missing predicate: {text}");
        // Scan line must appear after Filter and be indented more.
        let filter_pos = text.find("Filter").unwrap();
        let scan_pos = text.find("Scan(events)").unwrap();
        assert!(scan_pos > filter_pos, "Scan should appear after Filter");
        // The Scan line must start with leading spaces (indented).
        let scan_line = text.lines().find(|l| l.contains("Scan(events)")).unwrap();
        assert!(
            scan_line.starts_with("  "),
            "Scan line should be indented under Filter: {scan_line:?}"
        );
    }

    #[test]
    fn format_project_over_scan_lists_output_columns() {
        let node = ExplainNode::Project {
            columns: vec!["entity_id".to_string(), "ts".to_string()],
            input: Box::new(ExplainNode::Scan {
                table: "events".to_string(),
                time_range: "none".to_string(),
                predicates: vec![],
                columns: vec!["entity_id".to_string(), "ts".to_string()],
            }),
        };
        let text = format_explain(&node);
        assert!(text.contains("Project"), "missing Project: {text}");
        assert!(text.contains("entity_id"), "missing column: {text}");
        assert!(text.contains("ts"), "missing column: {text}");
    }

    #[test]
    fn format_limit_over_scan_shows_count() {
        let node = ExplainNode::Limit {
            count: 50,
            input: Box::new(ExplainNode::Scan {
                table: "events".to_string(),
                time_range: "none".to_string(),
                predicates: vec![],
                columns: vec!["entity_id".to_string()],
            }),
        };
        let text = format_explain(&node);
        assert!(text.contains("Limit"), "missing Limit header: {text}");
        assert!(text.contains("50"), "missing count: {text}");
    }

    #[test]
    fn format_explain_output_contains_no_trailing_newline() {
        let node = ExplainNode::Scan {
            table: "t".to_string(),
            time_range: "none".to_string(),
            predicates: vec![],
            columns: vec!["x".to_string()],
        };
        let text = format_explain(&node);
        assert!(
            !text.ends_with('\n'),
            "format_explain must not end with newline"
        );
    }

    #[test]
    fn format_multi_predicate_scan_aligns_continuations_at_any_indent() {
        // When a Scan has two predicates and is nested under a Filter
        // (indent level 1), the continuation predicate line must be
        // indented by the same {pad} prefix as the first predicate line
        // so both values start at the same column. This guards the
        // format!("{pad}               {p}") continuation formatting.
        let node = ExplainNode::Filter {
            predicate: "x > 0".to_string(),
            input: Box::new(ExplainNode::Scan {
                table: "events".to_string(),
                time_range: "none".to_string(),
                predicates: vec![
                    "entity_id = 'u1'".to_string(),
                    "event_type = 'click'".to_string(),
                ],
                columns: vec!["entity_id".to_string()],
            }),
        };
        let text = format_explain(&node);
        // Both predicate values should start at the same column offset.
        let pred1_line = text
            .lines()
            .find(|l| l.contains("entity_id = 'u1'"))
            .unwrap_or_else(|| panic!("predicate 1 not found:\n{text}"));
        let pred2_line = text
            .lines()
            .find(|l| l.contains("event_type = 'click'"))
            .unwrap_or_else(|| panic!("predicate 2 not found:\n{text}"));
        let col1 = pred1_line.find("entity_id").unwrap();
        let col2 = pred2_line.find("event_type").unwrap();
        assert_eq!(
            col1, col2,
            "continuation predicate must align with first predicate value"
        );
    }

    #[test]
    fn format_explain_output_contains_no_unicode() {
        let node = ExplainNode::Project {
            columns: vec!["a".to_string()],
            input: Box::new(ExplainNode::Scan {
                table: "t".to_string(),
                time_range: "none".to_string(),
                predicates: vec![],
                columns: vec!["a".to_string()],
            }),
        };
        let text = format_explain(&node);
        assert!(
            text.is_ascii(),
            "format_explain must produce ASCII-only output: {text:?}"
        );
    }

    // ── Time-range formatting ───────────────────────────────────────────

    #[test]
    fn format_time_range_none() {
        assert_eq!(format_time_range(&None), "none");
    }

    #[test]
    fn format_time_range_resolved_shows_bounds() {
        use bqlite_core::{TimeRange, Timestamp};
        let tr = Some(TimeRange::new(
            Timestamp::from_nanos(1_000),
            Timestamp::from_nanos(5_000),
        ));
        assert_eq!(format_time_range(&tr), "[1000, 5000)");
    }

    #[test]
    fn format_duration_ns_picks_largest_unit() {
        assert_eq!(format_duration_ns(86_400_000_000_000), "1d");
        assert_eq!(format_duration_ns(3_600_000_000_000), "1h");
        assert_eq!(format_duration_ns(60_000_000_000), "1m");
        assert_eq!(format_duration_ns(1_000_000_000), "1s");
        assert_eq!(format_duration_ns(1_000_000), "1ms");
        assert_eq!(format_duration_ns(1_000), "1us");
        assert_eq!(format_duration_ns(42), "42ns");
    }

    #[test]
    fn explain_scan_shows_last_time_range() {
        let cat = TestCatalog::default().with(events_schema());
        let pipeline = Pipeline {
            source: Source {
                primary: table_ref("events"),
                joins: vec![],
                time_range: Some(bqlite_ast::pipeline::TimeRange::Last(
                    30 * 86_400_000_000_000,
                )),
                span: Span::EMPTY,
            },
            stages: vec![],
            span: Span::EMPTY,
        };
        // Pass now_ns=0 so LAST 30d resolves to [-2592000000000000, 0).
        let physical = plan(Statement::Explain(pipeline), &cat, 0).expect("plan must succeed");
        let node = build_explain_node(&physical);
        let text = format_explain(&node);
        // The resolved time range should appear as ns bounds.
        assert!(
            text.contains("time_range"),
            "EXPLAIN output must show time_range; got:\n{text}"
        );
        assert!(
            text.contains("[-"),
            "EXPLAIN output must show resolved ns range; got:\n{text}"
        );
    }

    #[test]
    fn explain_scan_shows_between_time_range() {
        let cat = TestCatalog::default().with(events_schema());
        let pipeline = Pipeline {
            source: Source {
                primary: table_ref("events"),
                joins: vec![],
                time_range: Some(bqlite_ast::pipeline::TimeRange::Between {
                    start: "2024-01-01T00:00:00Z".into(),
                    end: "2024-02-01T00:00:00Z".into(),
                }),
                span: Span::EMPTY,
            },
            stages: vec![],
            span: Span::EMPTY,
        };
        let physical = plan(Statement::Explain(pipeline), &cat, 0).expect("plan must succeed");
        let node = build_explain_node(&physical);
        let text = format_explain(&node);
        // The resolved time range should appear as ns bounds.
        assert!(
            text.contains("time_range"),
            "EXPLAIN output must show time_range; got:\n{text}"
        );
        assert!(
            text.contains('['),
            "EXPLAIN output must show resolved ns range; got:\n{text}"
        );
    }

    // ── Wave 4 EXPLAIN tests ──────────────────────────────────────────────

    fn scan_node() -> ExplainNode {
        ExplainNode::Scan {
            table: "events".into(),
            time_range: "none".into(),
            predicates: vec![],
            columns: vec!["entity_id".into(), "ts".into()],
        }
    }

    #[test]
    fn format_sessionize_gap_only() {
        let node = ExplainNode::Sessionize {
            gap: "30m".into(),
            end_events: vec![],
            input: Box::new(scan_node()),
        };
        let text = format_explain(&node);
        assert!(text.contains("Sessionize"), "missing Sessionize: {text}");
        assert!(text.contains("gap        : 30m"), "missing gap: {text}");
        assert!(
            text.contains("end_events : none"),
            "gap-only should show none: {text}"
        );
        assert!(text.contains("Scan(events)"), "missing child scan: {text}");
    }

    #[test]
    fn format_sessionize_with_end_events() {
        let node = ExplainNode::Sessionize {
            gap: "1h".into(),
            end_events: vec!["logout".into(), "timeout".into()],
            input: Box::new(scan_node()),
        };
        let text = format_explain(&node);
        assert!(
            text.contains("end_events : logout, timeout"),
            "missing end events: {text}"
        );
    }

    #[test]
    fn format_event_select_first() {
        let node = ExplainNode::EventSelect {
            kind: "FIRST".into(),
            event_types: vec!["purchase".into()],
            has_predicate: false,
            input: Box::new(scan_node()),
        };
        let text = format_explain(&node);
        assert!(text.contains("EventSelect"), "missing EventSelect: {text}");
        assert!(text.contains("kind       : FIRST"), "missing kind: {text}");
        assert!(
            text.contains("events     : purchase"),
            "missing events: {text}"
        );
        assert!(
            text.contains("predicate  : false"),
            "missing predicate flag: {text}"
        );
    }

    #[test]
    fn format_event_select_nth_with_predicate() {
        let node = ExplainNode::EventSelect {
            kind: "NTH(3)".into(),
            event_types: vec!["click".into(), "tap".into()],
            has_predicate: true,
            input: Box::new(scan_node()),
        };
        let text = format_explain(&node);
        assert!(text.contains("NTH(3)"), "missing NTH(3): {text}");
        assert!(
            text.contains("events     : click, tap"),
            "missing events: {text}"
        );
        assert!(
            text.contains("predicate  : true"),
            "should show predicate: {text}"
        );
    }

    #[test]
    fn format_attribute() {
        let node = ExplainNode::Attribute {
            conversion_events: vec!["purchase".into()],
            touchpoint_events: vec!["ad_click".into(), "email_open".into()],
            window: "30d".into(),
            input: Box::new(scan_node()),
        };
        let text = format_explain(&node);
        assert!(text.contains("Attribute"), "missing Attribute: {text}");
        assert!(
            text.contains("conversion : purchase"),
            "missing conversion: {text}"
        );
        assert!(
            text.contains("touchpoints: ad_click, email_open"),
            "missing touchpoints: {text}"
        );
        assert!(text.contains("window     : 30d"), "missing window: {text}");
    }

    #[test]
    fn format_subquery_filter() {
        let node = ExplainNode::SubqueryFilter {
            column_count: 1,
            input: Box::new(scan_node()),
            subquery: Box::new(scan_node()),
        };
        let text = format_explain(&node);
        assert!(
            text.contains("SubqueryFilter"),
            "missing SubqueryFilter: {text}"
        );
        assert!(
            text.contains("columns    : 1"),
            "missing column count: {text}"
        );
        assert!(text.contains("subquery:"), "missing subquery label: {text}");
        // Should have two Scan nodes: one for subquery, one for input.
        assert_eq!(
            text.matches("Scan(events)").count(),
            2,
            "should have two scans: {text}"
        );
    }

    #[test]
    fn format_sample() {
        let node = ExplainNode::Sample {
            fraction: 0.1,
            seed: 42,
            input: Box::new(scan_node()),
        };
        let text = format_explain(&node);
        assert!(text.contains("Sample"), "missing Sample: {text}");
        assert!(
            text.contains("fraction   : 0.1"),
            "missing fraction: {text}"
        );
        assert!(text.contains("seed       : 42"), "missing seed: {text}");
    }

    #[test]
    fn format_merge_sources() {
        let node = ExplainNode::MergeSources {
            tables: vec!["events".into(), "clicks".into()],
            order: vec!["entity_id ASC".into(), "ts ASC".into()],
            inputs: vec![scan_node(), scan_node()],
        };
        let text = format_explain(&node);
        assert!(
            text.contains("MergeSources"),
            "missing MergeSources: {text}"
        );
        assert!(
            text.contains("tables     : events, clicks"),
            "missing tables: {text}"
        );
        assert!(
            text.contains("order      : entity_id ASC, ts ASC"),
            "missing order: {text}"
        );
    }

    #[test]
    fn format_nanos_durations() {
        use super::format_nanos;
        assert_eq!(format_nanos(86_400_000_000_000), "1d");
        assert_eq!(format_nanos(3_600_000_000_000), "1h");
        assert_eq!(format_nanos(1_800_000_000_000), "30m");
        assert_eq!(format_nanos(1_000_000_000), "1s");
        assert_eq!(format_nanos(500_000_000), "500ms");
        assert_eq!(format_nanos(12345), "12345ns");
    }

    #[test]
    fn build_explain_sessionize() {
        use crate::physical::{ScanPhysical, SessionizePhysical};
        let scan = ScanPhysical {
            table: "events".into(),
            query_range: None,
            reader_range: None,
            scan_predicates: vec![],
            projected_columns: vec![],
            output_schema: events_schema_os(),
            entity_key_col: "entity_id".into(),
            timestamp_col: "ts".into(),
            sample: None,
        };
        let sess = SessionizePhysical {
            gap_ns: 1_800_000_000_000,
            end_events: vec!["logout".into()],
            demand: crate::demand::DemandSet::default(),
            forwarded_columns: vec![],
            fused_aggregate: None,
            input: Box::new(PhysicalPlan::Scan(scan)),
            output_schema: events_schema_os(),
            pre_fusion_output_schema: None,
        };
        let node = build_explain_node(&PhysicalPlan::Sessionize(sess));
        let text = format_explain(&node);
        assert!(text.contains("Sessionize"), "missing: {text}");
        assert!(text.contains("30m"), "gap should be 30m: {text}");
        assert!(text.contains("logout"), "missing end event: {text}");
    }

    fn events_schema_os() -> bqlite_core::OperatorSchema {
        bqlite_core::OperatorSchema::new(vec![
            ColumnDef::required("entity_id", BqlType::String),
            ColumnDef::required("ts", BqlType::Timestamp),
            ColumnDef::required("event_type", BqlType::String),
        ])
        .unwrap()
    }

    #[test]
    fn build_explain_sample() {
        let scan = ScanPhysical {
            table: "events".into(),
            query_range: None,
            reader_range: None,
            scan_predicates: vec![],
            projected_columns: vec![],
            output_schema: events_schema_os(),
            entity_key_col: "entity_id".into(),
            timestamp_col: "ts".into(),
            sample: None,
        };
        let sample = crate::physical::SamplePhysical {
            fraction: 0.25,
            seed: 42,
            input: Box::new(PhysicalPlan::Scan(scan)),
            output_schema: events_schema_os(),
        };
        let node = build_explain_node(&PhysicalPlan::Sample(sample));
        let text = format_explain(&node);
        assert!(text.contains("Sample"), "missing: {text}");
        assert!(text.contains("0.25"), "missing fraction: {text}");
        assert!(text.contains("42"), "missing seed: {text}");
    }

    #[test]
    fn build_explain_merge_sources() {
        use crate::logical::SortDirection;
        let scan1 = ScanPhysical {
            table: "events".into(),
            query_range: None,
            reader_range: None,
            scan_predicates: vec![],
            projected_columns: vec![],
            output_schema: events_schema_os(),
            entity_key_col: "entity_id".into(),
            timestamp_col: "ts".into(),
            sample: None,
        };
        let scan2 = ScanPhysical {
            table: "clicks".into(),
            query_range: None,
            reader_range: None,
            scan_predicates: vec![],
            projected_columns: vec![],
            output_schema: events_schema_os(),
            entity_key_col: "entity_id".into(),
            timestamp_col: "ts".into(),
            sample: None,
        };
        let merge = crate::physical::MergeSourcesPhysical {
            tables: vec![scan1, scan2],
            order: vec![
                ("entity_id".into(), SortDirection::Asc),
                ("ts".into(), SortDirection::Asc),
            ],
            table_id_map: vec!["events".into(), "clicks".into()],
            output_schema: events_schema_os(),
        };
        let node = build_explain_node(&PhysicalPlan::MergeSources(merge));
        let text = format_explain(&node);
        assert!(text.contains("MergeSources"), "missing: {text}");
        assert!(text.contains("events, clicks"), "missing tables: {text}");
        assert!(text.contains("entity_id ASC"), "missing order: {text}");
    }
}
