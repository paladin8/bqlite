//! Pattern compiler — `MatchPattern` AST → `CompiledNfa`.
//!
//! Implements the compilation pipeline from sequence-matching.md §14.2:
//!
//! 1. Validate the pattern (non-empty, well-formed)
//! 2. Resolve variable binding order (`$var` → `VariableBindingDef`)
//! 3. Build NFA via Thompson construction
//! 4. Eliminate epsilon transitions (for `*` quantifier)
//! 5. Add poison transitions (`WITHOUT` negation scopes)
//! 6. Compute `state_to_step` via BFS
//! 7. Classify pattern shape (`PatternClass` per matcher-strategy.md §2)
//! 8. Collect relevant event types for scan-level pushdown
//! 9. Package the `CompiledNfa`
//!
//! Lives in `bqlite-planner` because compilation requires schema access
//! for predicate validation and the result is carried on the physical
//! descriptor. Invoked during logical→physical lowering (TASK-318).

use std::collections::{BTreeSet, HashSet, VecDeque};

use bqlite_ast::expr::{CompareOp, Expr, InRhs, Spanned};
use bqlite_ast::pattern::{MatchMode, MatchPattern, MatchWindow, Repetition, Step, StepEvent};
use bqlite_core::{BqliteError, OperatorSchema, Result};

use crate::compiled::CompiledExpr;
use crate::expr::{FunctionRegistry, TypedExpr};

// ─────────────────────────────────────────────────────────────────────────────
// CompiledNfa
// ─────────────────────────────────────────────────────────────────────────────

/// Compiled NFA shared across all shard-tasks (immutable "program").
///
/// Produced by [`compile_pattern`] from a `MatchPattern` AST node.
/// Carried on the physical descriptor (`SequenceMatchPhysical`) and
/// consumed by the `SequenceMatchOperator` at runtime.
#[derive(Debug, Clone)]
pub struct CompiledNfa {
    /// States in the NFA graph. State 0 is the start state.
    pub states: Vec<NfaState>,
    /// Index of the accept state.
    pub accept_state: u16,
    /// Event types referenced by any step or negation (for scan pushdown).
    /// Deduplicated.
    pub relevant_event_types: BTreeSet<String>,
    /// Pattern classification (matcher-strategy.md §2).
    pub pattern_class: PatternClass,
    /// Variable binding definitions.
    pub variable_bindings: Vec<VariableBindingDef>,
    /// Global time window in nanoseconds, if any.
    pub global_window: Option<i64>,
    /// Whether EMIT ALL is enabled.
    pub emit_all: bool,
    /// Map from NFA state index to logical step number (0-indexed).
    /// For linear patterns, state N maps to step N.
    /// Accept state maps to `num_steps`.
    pub state_to_step: Vec<u8>,
}

/// A single NFA state.
#[derive(Debug, Clone)]
pub struct NfaState {
    /// Forward transitions: event predicate → target state.
    pub transitions: Vec<Transition>,
    /// Poison transitions: event predicate → kills candidates at this state.
    pub poison_transitions: Vec<PoisonTransition>,
}

/// A forward transition from one NFA state to another.
#[derive(Debug, Clone)]
pub struct Transition {
    /// Event type to match.
    pub event_type: String,
    /// Additional property predicates (evaluated after event type match).
    pub predicates: Vec<CompiledExpr>,
    /// Variables to bind at this transition (index into `variable_bindings`).
    pub bind_variables: Vec<usize>,
    /// Variables to check at this transition (index into `variable_bindings`).
    pub check_variables: Vec<usize>,
    /// Target state index.
    pub target: u16,
}

/// A poison transition that kills candidates at the owning state.
#[derive(Debug, Clone)]
pub struct PoisonTransition {
    /// Event type that kills candidates at this state.
    pub event_type: String,
    /// Optional additional predicates on the poison event.
    pub predicates: Vec<CompiledExpr>,
}

// ─────────────────────────────────────────────────────────────────────────────
// PatternClass
// ─────────────────────────────────────────────────────────────────────────────

/// Compile-time classification of a MATCH pattern's structural shape.
///
/// Determined by [`classify_pattern`] during NFA compilation.
/// Carried on [`CompiledNfa::pattern_class`] and read by the physical
/// planner to select the execution strategy (matcher-strategy.md §3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PatternClass {
    /// Ordered steps, no negation, no repetition, no variable bindings,
    /// no alternation within steps. The simplest and fastest path.
    LinearSimple,

    /// Linear with one or more `IMMEDIATELY` transitions between steps.
    LinearImmediate,

    /// Linear with poison transitions (WITHOUT clauses) but no
    /// repetition, no variable bindings, no alternation.
    LinearWithNegation,

    /// Linear with variable bindings ($var) but no negation, no
    /// repetition, no alternation.
    LinearWithBindings,

    /// Linear with both negation and variable bindings.
    LinearFull,

    /// Contains branching (alternation within a step), repetition
    /// (`+`/`*`), or any structural feature that prevents linearization.
    GeneralNfa,
}

// ─────────────────────────────────────────────────────────────────────────────
// MatchStrategy
// ─────────────────────────────────────────────────────────────────────────────

/// The execution strategy selected by the physical planner.
///
/// Determines which state variant and execution kernel the
/// `SequenceMatchOperator` uses (matcher-strategy.md §3.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchStrategy {
    /// Step counter fast path for linear non-consecutive patterns.
    StepCounter,
    /// Dedicated consecutive matcher for IMMEDIATELY patterns.
    ConsecutiveMatcher,
    /// Full NFA simulator for general patterns or match-detail demand.
    FullNfa,
}

/// Execution configuration derived from demand propagation.
///
/// Used by [`select_strategy`] to determine the final execution
/// strategy (matcher-strategy.md §7.1).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct MatchExecutionConfig {
    /// Whether downstream demand requires match-duration tracking.
    pub track_match_duration: bool,
    /// Whether downstream demand requires match-events tracking.
    /// Forces NFA for all linear pattern classes.
    pub track_match_events: bool,
}

/// Select the execution strategy for a MATCH operator.
///
/// Called by the physical planner after NFA compilation and demand
/// propagation (matcher-strategy.md §3.3).
pub fn select_strategy(
    pattern_class: PatternClass,
    exec_config: &MatchExecutionConfig,
) -> MatchStrategy {
    // Rule 1: match_events demand forces NFA for all pattern classes.
    if exec_config.track_match_events {
        return MatchStrategy::FullNfa;
    }
    // Rule 2: GeneralNfa always uses the NFA path.
    if pattern_class == PatternClass::GeneralNfa {
        return MatchStrategy::FullNfa;
    }
    // Rule 3: LinearImmediate uses the consecutive matcher.
    if pattern_class == PatternClass::LinearImmediate {
        return MatchStrategy::ConsecutiveMatcher;
    }
    // Rule 4: All other linear variants use the step counter.
    MatchStrategy::StepCounter
}

// ─────────────────────────────────────────────────────────────────────────────
// VariableBindingDef
// ─────────────────────────────────────────────────────────────────────────────

/// Definition of a variable binding resolved during compilation.
///
/// Each `$var` in the pattern produces one `VariableBindingDef`. The
/// NFA transitions reference bindings by index into the
/// `CompiledNfa::variable_bindings` vec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VariableBindingDef {
    /// Variable name (e.g., "plan" for `$plan`).
    pub name: String,
    /// Column to extract the binding value from.
    pub source_column: String,
    /// Column index in the input schema.
    pub column_index: usize,
    /// Step index (0-based) where this variable is first bound.
    pub bind_step: u8,
}

// ─────────────────────────────────────────────────────────────────────────────
// compile_pattern — main entry point
// ─────────────────────────────────────────────────────────────────────────────

/// Compile a `MatchPattern` AST into a `CompiledNfa`.
///
/// This is the single entry point for pattern compilation, called
/// during logical→physical lowering (TASK-318).
///
/// # Errors
///
/// Returns `BqliteError::Plan` for:
/// - Empty pattern (no steps)
/// - Variable used without a corresponding column equality
/// - Variable referenced before binding (first reference must be an
///   equality to establish the source column)
/// - Unknown column in step predicate
/// - Type errors in step predicates
pub fn compile_pattern(
    pattern: &MatchPattern,
    schema: &OperatorSchema,
    registry: &FunctionRegistry,
) -> Result<CompiledNfa> {
    // ── Step 1: Validate ────────────────────────────────────────────
    if pattern.steps.is_empty() {
        return Err(BqliteError::Plan(
            "MATCH pattern must have at least one step".to_string(),
        ));
    }

    // ── Step 2: Resolve variable bindings ───────────────────────────
    let variable_bindings = resolve_variable_bindings(pattern, schema)?;

    // ── Step 3: Thompson construction + epsilon elimination ────────
    // Epsilon elimination is done inline at the end of Thompson
    // construction (sequence-matching.md §3.5).
    let (mut states, accept_state) =
        build_thompson_nfa(pattern, schema, registry, &variable_bindings)?;

    // ── Step 4: Add poison transitions (WITHOUT) ────────────────────
    add_poison_transitions(pattern, &mut states);

    // ── Step 5: Compute state_to_step via BFS ───────────────────────
    let state_to_step = compute_state_to_step(&states);

    // ── Step 6: Classify pattern ────────────────────────────────────
    let pattern_class = classify_pattern(pattern, &states);

    // ── Step 7: NFA optimization (deferred) ─────────────────────────
    // sequence-matching.md §14.2 step 9 specifies merging equivalent
    // states and computing transition priority order. Not needed for
    // correctness — deferred to a future optimization task.

    // ── Step 8: Collect relevant event types ────────────────────────
    let relevant_event_types = collect_event_types(pattern);

    // ── Step 9: Extract global window ───────────────────────────────
    let global_window = match pattern.window {
        Some(MatchWindow::Within(nanos)) => Some(nanos),
        Some(MatchWindow::WithinSession) | None => None,
    };

    let emit_all = pattern.mode == MatchMode::EmitAll;

    Ok(CompiledNfa {
        states,
        accept_state,
        relevant_event_types,
        pattern_class,
        variable_bindings,
        global_window,
        emit_all,
        state_to_step,
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// Variable binding resolution
// ─────────────────────────────────────────────────────────────────────────────

/// A variable comparison extracted from a step predicate.
///
/// Represents a `column = $var` or `$var = column` equality found
/// during predicate analysis.
struct VariableComparison {
    variable_name: String,
    column_name: String,
    step_index: u8,
}

/// Walk all step predicates and resolve variable bindings.
///
/// For each `$var`, the first step referencing it is the bind step.
/// The variable's source column is determined from the equality
/// comparison (`column = $var`).
fn resolve_variable_bindings(
    pattern: &MatchPattern,
    schema: &OperatorSchema,
) -> Result<Vec<VariableBindingDef>> {
    let mut comparisons = Vec::new();

    for (step_idx, step) in pattern.steps.iter().enumerate() {
        if let Some(ref pred) = step.predicate {
            collect_variable_comparisons(&pred.node, step_idx as u8, &mut comparisons)?;
        }
    }

    // Group by variable name — first occurrence determines bind step.
    let mut seen: Vec<VariableBindingDef> = Vec::new();
    for comp in &comparisons {
        if seen.iter().any(|v| v.name == comp.variable_name) {
            // Already resolved — this is a check step, no new def needed.
            continue;
        }

        // Validate column exists in schema.
        let (col_idx, _col_def) = schema.column(&comp.column_name).ok_or_else(|| {
            BqliteError::Plan(format!(
                "variable `${}` references unknown column `{}`",
                comp.variable_name, comp.column_name,
            ))
        })?;

        seen.push(VariableBindingDef {
            name: comp.variable_name.clone(),
            source_column: comp.column_name.clone(),
            column_index: col_idx,
            bind_step: comp.step_index,
        });
    }

    Ok(seen)
}

/// Recursively collect variable equality comparisons from a predicate.
///
/// Recognises `column = $var`, `$var = column`, and their appearance
/// within `AND` / `OR` / `NOT` trees. Any other use of `$var` is an
/// error (e.g. `$var > 10`).
fn collect_variable_comparisons(
    expr: &Expr,
    step_idx: u8,
    out: &mut Vec<VariableComparison>,
) -> Result<()> {
    match expr {
        // column = $var  or  $var = column
        Expr::Compare {
            op: CompareOp::Equal,
            left,
            right,
        } => {
            match (&left.node, &right.node) {
                (Expr::Column(col), Expr::Variable(var))
                | (Expr::Variable(var), Expr::Column(col)) => {
                    out.push(VariableComparison {
                        variable_name: var.text.clone(),
                        column_name: col.text.clone(),
                        step_index: step_idx,
                    });
                }
                _ => {
                    // Recurse into both sides — variable might be nested.
                    collect_variable_comparisons(&left.node, step_idx, out)?;
                    collect_variable_comparisons(&right.node, step_idx, out)?;
                }
            }
            Ok(())
        }

        // Logical connectives — recurse.
        Expr::And(exprs) | Expr::Or(exprs) => {
            for e in exprs {
                collect_variable_comparisons(&e.node, step_idx, out)?;
            }
            Ok(())
        }
        Expr::Not(inner) => collect_variable_comparisons(&inner.node, step_idx, out),

        // Bare variable reference outside an equality — unsupported.
        Expr::Variable(name) => Err(BqliteError::Plan(format!(
            "variable `${}` must be used in an equality comparison (`column = ${}`) \
             within a MATCH step predicate",
            name.text, name.text,
        ))),

        // All other expressions — recurse into sub-expressions to find
        // any variable references (which would be an error).
        Expr::Compare { left, right, .. } | Expr::Binary { left, right, .. } => {
            collect_variable_comparisons(&left.node, step_idx, out)?;
            collect_variable_comparisons(&right.node, step_idx, out)
        }
        Expr::Unary { operand, .. } => collect_variable_comparisons(&operand.node, step_idx, out),
        Expr::IsNull { expr, .. }
        | Expr::Like { expr, .. }
        | Expr::Regex { expr, .. }
        | Expr::Contains { expr, .. }
        | Expr::Cast { expr, .. } => collect_variable_comparisons(&expr.node, step_idx, out),
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_variable_comparisons(&expr.node, step_idx, out)?;
            collect_variable_comparisons(&low.node, step_idx, out)?;
            collect_variable_comparisons(&high.node, step_idx, out)
        }
        Expr::In { lhs, rhs, .. } => {
            for e in lhs {
                collect_variable_comparisons(&e.node, step_idx, out)?;
            }
            if let InRhs::List(exprs) = rhs {
                for e in exprs {
                    collect_variable_comparisons(&e.node, step_idx, out)?;
                }
            }
            Ok(())
        }
        Expr::FunctionCall { args, .. } => {
            for a in args {
                collect_variable_comparisons(&a.node, step_idx, out)?;
            }
            Ok(())
        }
        Expr::Case { arms, else_expr } => {
            for arm in arms {
                collect_variable_comparisons(&arm.condition.node, step_idx, out)?;
                collect_variable_comparisons(&arm.value.node, step_idx, out)?;
            }
            if let Some(ref e) = else_expr {
                collect_variable_comparisons(&e.node, step_idx, out)?;
            }
            Ok(())
        }

        // Parenthesized expression — recurse.
        Expr::Paren(inner) => collect_variable_comparisons(&inner.node, step_idx, out),

        // Leaf nodes — no variables here.
        Expr::Literal(_) | Expr::Column(_) | Expr::Qualified { .. } => Ok(()),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Thompson construction
// ─────────────────────────────────────────────────────────────────────────────

/// Epsilon transition — used internally during construction before
/// epsilon elimination.
#[derive(Debug, Clone)]
struct EpsilonTransition {
    target: u16,
}

/// Intermediate NFA state used during construction (carries epsilon
/// transitions that are eliminated before the final NFA is produced).
#[derive(Debug, Clone)]
struct BuildState {
    transitions: Vec<Transition>,
    poison_transitions: Vec<PoisonTransition>,
    epsilons: Vec<EpsilonTransition>,
}

impl BuildState {
    fn empty() -> Self {
        Self {
            transitions: Vec::new(),
            poison_transitions: Vec::new(),
            epsilons: Vec::new(),
        }
    }
}

/// Build the NFA states via Thompson construction.
///
/// Each step becomes a state; transitions carry event type, compiled
/// predicates, and variable bind/check instructions. Alternation
/// produces multiple transitions from the same source state.
/// Repetition produces self-loops (+ epsilon for `*`).
fn build_thompson_nfa(
    pattern: &MatchPattern,
    schema: &OperatorSchema,
    registry: &FunctionRegistry,
    variable_bindings: &[VariableBindingDef],
) -> Result<(Vec<NfaState>, u16)> {
    // Create one state per step + accept state.
    // For repetition, intermediate states may be added.
    let mut states: Vec<BuildState> = Vec::new();

    // Start state (state 0) — initial state before any step fires.
    states.push(BuildState::empty());

    // For each step, we create the target state and add transitions.
    let mut current_state: u16 = 0;

    for (step_idx, step) in pattern.steps.iter().enumerate() {
        let step_idx_u8 = step_idx as u8;

        // Compile the non-variable portion of the step predicate.
        let predicates = compile_step_predicates(step, schema, registry)?;

        // Determine bind/check variables for this step.
        let bind_vars = variable_bindings
            .iter()
            .enumerate()
            .filter(|(_, vb)| vb.bind_step == step_idx_u8)
            .map(|(i, _)| i)
            .collect::<Vec<_>>();
        let check_vars = variable_bindings
            .iter()
            .enumerate()
            .filter(|(_, vb)| vb.bind_step < step_idx_u8)
            .filter(|(_, vb)| {
                // Check if this variable is actually referenced in this step.
                step.predicate
                    .as_ref()
                    .is_some_and(|p| expr_references_variable(&p.node, &vb.name))
            })
            .map(|(i, _)| i)
            .collect::<Vec<_>>();

        match step.repetition {
            None => {
                // Simple step: current_state --[event]--> next_state.
                let next_state = states.len() as u16;
                states.push(BuildState::empty());

                add_step_transitions(
                    &mut states[current_state as usize],
                    &step.event,
                    &predicates,
                    &bind_vars,
                    &check_vars,
                    next_state,
                );
                current_state = next_state;
            }
            Some(Repetition::OneOrMore) => {
                // A+ pattern: need at least one A.
                // current --[A]--> intermediate
                // intermediate --[A]--> intermediate (self-loop)
                let intermediate = states.len() as u16;
                states.push(BuildState::empty());

                add_step_transitions(
                    &mut states[current_state as usize],
                    &step.event,
                    &predicates,
                    &bind_vars,
                    &check_vars,
                    intermediate,
                );

                // Self-loop on intermediate.
                add_step_transitions(
                    &mut states[intermediate as usize],
                    &step.event,
                    &predicates,
                    &bind_vars,
                    &check_vars,
                    intermediate,
                );

                current_state = intermediate;
            }
            Some(Repetition::ZeroOrMore) => {
                // A* pattern: zero or more A's.
                // current --[A]--> intermediate
                // current --ε--> intermediate (skip)
                // intermediate --[A]--> intermediate (self-loop)
                let intermediate = states.len() as u16;
                states.push(BuildState::empty());

                add_step_transitions(
                    &mut states[current_state as usize],
                    &step.event,
                    &predicates,
                    &bind_vars,
                    &check_vars,
                    intermediate,
                );

                // Epsilon transition (skip).
                states[current_state as usize]
                    .epsilons
                    .push(EpsilonTransition {
                        target: intermediate,
                    });

                // Self-loop on intermediate.
                add_step_transitions(
                    &mut states[intermediate as usize],
                    &step.event,
                    &predicates,
                    &bind_vars,
                    &check_vars,
                    intermediate,
                );

                current_state = intermediate;
            }
        }
    }

    // The last state created after all steps is the accept state.
    let accept_state = current_state;

    // For patterns where the last step has repetition, the accept state
    // is the intermediate state (which already has the self-loop).
    // If the pattern has no repetition, the accept state is num_steps.
    debug_assert!(
        (accept_state as usize) < states.len(),
        "accept state must be valid: accept={accept_state}, states={}",
        states.len(),
    );

    // ── Epsilon elimination ────────────────────────────────────────
    // For each state with epsilon transitions, copy all transitions
    // from the epsilon targets into the source state (epsilon-closure).
    // Iterate to a fixed point since epsilon chains are possible
    // (e.g. A* THEN B* THEN C produces chains: state0 --ε--> state1 --ε--> state2).
    // Transitive epsilons are propagated before clearing.
    loop {
        let mut changed = false;
        for i in 0..states.len() {
            if states[i].epsilons.is_empty() {
                continue;
            }
            let epsilon_targets: Vec<u16> = states[i].epsilons.iter().map(|e| e.target).collect();
            for target in epsilon_targets {
                // Propagate forward transitions from epsilon target.
                let target_transitions: Vec<Transition> =
                    states[target as usize].transitions.clone();
                for t in target_transitions {
                    let already_present = states[i].transitions.iter().any(|existing| {
                        existing.event_type == t.event_type
                            && existing.target == t.target
                            && existing.bind_variables == t.bind_variables
                            && existing.check_variables == t.check_variables
                    });
                    if !already_present {
                        states[i].transitions.push(t);
                        changed = true;
                    }
                }
                // Propagate transitive epsilons (target's epsilons
                // become our epsilons for the next iteration).
                let target_epsilons: Vec<u16> = states[target as usize]
                    .epsilons
                    .iter()
                    .map(|e| e.target)
                    .collect();
                for eps_target in target_epsilons {
                    if !states[i].epsilons.iter().any(|e| e.target == eps_target) {
                        states[i]
                            .epsilons
                            .push(EpsilonTransition { target: eps_target });
                        changed = true;
                    }
                }
            }
        }
        if !changed {
            break;
        }
    }
    // Clear all epsilons after convergence.
    for s in &mut states {
        s.epsilons.clear();
    }

    // Convert BuildState → NfaState (epsilons already eliminated).
    let nfa_states = states
        .into_iter()
        .map(|bs| NfaState {
            transitions: bs.transitions,
            poison_transitions: bs.poison_transitions,
        })
        .collect();

    Ok((nfa_states, accept_state))
}

/// Add forward transitions for a step event to a build state.
fn add_step_transitions(
    state: &mut BuildState,
    event: &StepEvent,
    predicates: &[CompiledExpr],
    bind_vars: &[usize],
    check_vars: &[usize],
    target: u16,
) {
    match event {
        StepEvent::Single(event_ref) => {
            state.transitions.push(Transition {
                event_type: event_ref.event.text.clone(),
                predicates: predicates.to_vec(),
                bind_variables: bind_vars.to_vec(),
                check_variables: check_vars.to_vec(),
                target,
            });
        }
        StepEvent::Alternation(event_refs) => {
            // Multiple transitions from same source to same target.
            for event_ref in event_refs {
                state.transitions.push(Transition {
                    event_type: event_ref.event.text.clone(),
                    predicates: predicates.to_vec(),
                    bind_variables: bind_vars.to_vec(),
                    check_variables: check_vars.to_vec(),
                    target,
                });
            }
        }
    }
}

/// Check whether an expression tree contains a reference to the
/// named variable.
fn expr_references_variable(expr: &Expr, var_name: &str) -> bool {
    match expr {
        Expr::Variable(name) => name.text == var_name,
        Expr::Binary { left, right, .. } | Expr::Compare { left, right, .. } => {
            expr_references_variable(&left.node, var_name)
                || expr_references_variable(&right.node, var_name)
        }
        Expr::Unary { operand, .. } => expr_references_variable(&operand.node, var_name),
        Expr::Not(inner) | Expr::Paren(inner) => expr_references_variable(&inner.node, var_name),
        Expr::And(exprs) | Expr::Or(exprs) => exprs
            .iter()
            .any(|e| expr_references_variable(&e.node, var_name)),
        Expr::FunctionCall { args, .. } => args
            .iter()
            .any(|a| expr_references_variable(&a.node, var_name)),
        Expr::IsNull { expr, .. }
        | Expr::Like { expr, .. }
        | Expr::Regex { expr, .. }
        | Expr::Contains { expr, .. }
        | Expr::Cast { expr, .. } => expr_references_variable(&expr.node, var_name),
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_references_variable(&expr.node, var_name)
                || expr_references_variable(&low.node, var_name)
                || expr_references_variable(&high.node, var_name)
        }
        Expr::In { lhs, rhs, .. } => {
            lhs.iter()
                .any(|e| expr_references_variable(&e.node, var_name))
                || matches!(rhs, InRhs::List(exprs) if exprs.iter().any(|e| expr_references_variable(&e.node, var_name)))
        }
        Expr::Case { arms, else_expr } => {
            arms.iter().any(|arm| {
                expr_references_variable(&arm.condition.node, var_name)
                    || expr_references_variable(&arm.value.node, var_name)
            }) || else_expr
                .as_ref()
                .is_some_and(|e| expr_references_variable(&e.node, var_name))
        }
        Expr::Literal(_) | Expr::Column(_) | Expr::Qualified { .. } => false,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Step predicate compilation
// ─────────────────────────────────────────────────────────────────────────────

/// Compile the non-variable portion of a step's predicate into
/// `CompiledExpr`s.
///
/// Variable equality comparisons (`column = $var`) are handled by the
/// variable binding mechanism and stripped from the predicate before
/// compilation. The remaining predicate (if any) is compiled via
/// `TypedExpr::from_ast` → `CompiledExpr::from_typed`.
fn compile_step_predicates(
    step: &Step,
    schema: &OperatorSchema,
    registry: &FunctionRegistry,
) -> Result<Vec<CompiledExpr>> {
    let pred = match step.predicate {
        Some(ref p) => p,
        None => return Ok(Vec::new()),
    };

    // Strip variable comparisons from the predicate.
    let stripped = strip_variable_exprs(&pred.node);

    match stripped {
        None => Ok(Vec::new()), // Predicate was entirely variable equalities.
        Some(cleaned) => {
            let spanned = Spanned::new(cleaned, pred.span);
            let typed = TypedExpr::from_ast(&spanned, schema, registry)?;
            let compiled = CompiledExpr::from_typed(&typed);
            Ok(vec![compiled])
        }
    }
}

/// Strip variable equality comparisons from an expression tree.
///
/// Returns `None` if the entire expression is a single variable
/// equality (or a conjunction of only variable equalities). Returns
/// `Some(cleaned)` with variable equalities removed.
fn strip_variable_exprs(expr: &Expr) -> Option<Expr> {
    match expr {
        // column = $var → strip entirely.
        Expr::Compare {
            op: CompareOp::Equal,
            left,
            right,
        } if matches!(
            (&left.node, &right.node),
            (Expr::Column(_), Expr::Variable(_)) | (Expr::Variable(_), Expr::Column(_))
        ) =>
        {
            None
        }

        // Paren — recurse into the inner expression.
        Expr::Paren(inner) => strip_variable_exprs(&inner.node),

        // AND — strip variable equalities from children.
        Expr::And(exprs) => {
            let remaining: Vec<Spanned<Expr>> = exprs
                .iter()
                .filter_map(|e| {
                    strip_variable_exprs(&e.node).map(|cleaned| Spanned::new(cleaned, e.span))
                })
                .collect();

            match remaining.len() {
                0 => None,
                1 => Some(remaining.into_iter().next().unwrap().node),
                _ => Some(Expr::And(remaining)),
            }
        }

        // Everything else — no stripping (variables in non-equality
        // positions are caught by collect_variable_comparisons).
        _ => Some(expr.clone()),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Epsilon elimination
// ─────────────────────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────────────────────
// Poison transitions (WITHOUT)
// ─────────────────────────────────────────────────────────────────────────────

/// Add poison transitions for WITHOUT exclusion clauses.
///
/// `step[i].without_next` specifies event types that must not appear
/// between step i and step i+1. The poison transition is added to the
/// state that step[i]'s forward transition targets (the state where
/// we wait for step i+1).
fn add_poison_transitions(pattern: &MatchPattern, states: &mut [NfaState]) {
    // Track which state each step's forward transitions target.
    // For the simple linear case, step i transitions from state i to
    // state i+1, so the poison goes on state i+1.
    //
    // For the general case with repetition, we trace the actual
    // target states from transitions.
    for (step_idx, step) in pattern.steps.iter().enumerate() {
        let exclusion = match step.without_next {
            Some(ref excl) => excl,
            None => continue,
        };

        // Find the target states of this step's forward transitions.
        // The poison goes on those target states.
        let source_state = find_step_source_state(states, step_idx);
        let target_states: Vec<u16> = states
            .get(source_state)
            .map(|s| {
                s.transitions
                    .iter()
                    .map(|t| t.target)
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .collect()
            })
            .unwrap_or_default();

        for target in target_states {
            for event_ref in &exclusion.events {
                states[target as usize]
                    .poison_transitions
                    .push(PoisonTransition {
                        event_type: event_ref.event.text.clone(),
                        predicates: Vec::new(),
                    });
            }
        }
    }
}

/// Find the NFA state that corresponds to the source of step `step_idx`.
///
/// For linear patterns without repetition, step i fires from state i.
/// For patterns with repetition, intermediate states shift the mapping.
/// This function traces the NFA graph to find the correct source.
fn find_step_source_state(states: &[NfaState], step_idx: usize) -> usize {
    // Walk forward from state 0, following transitions step_idx times.
    // Each "hop" through a forward transition corresponds to one step.
    let mut current = 0usize;
    for _ in 0..step_idx {
        // Find the first forward transition's target.
        if let Some(t) = states.get(current).and_then(|s| s.transitions.first()) {
            current = t.target as usize;
        }
    }
    current
}

// ─────────────────────────────────────────────────────────────────────────────
// state_to_step (BFS)
// ─────────────────────────────────────────────────────────────────────────────

/// Compute the mapping from NFA state index to logical step number.
///
/// Uses BFS from the start state. Each state maps to the shortest
/// forward-transition distance from the start (0-indexed step number).
fn compute_state_to_step(states: &[NfaState]) -> Vec<u8> {
    let n = states.len();
    let mut step_map = vec![u8::MAX; n];
    let mut queue = VecDeque::new();

    // State 0 is the start state (before step 0 fires).
    step_map[0] = 0;
    queue.push_back(0u16);

    while let Some(state_idx) = queue.pop_front() {
        let current_step = step_map[state_idx as usize];
        let state = &states[state_idx as usize];

        for t in &state.transitions {
            let target = t.target as usize;
            let new_step = if t.target == state_idx {
                // Self-loop (repetition) — same step.
                current_step
            } else {
                current_step.saturating_add(1)
            };

            if new_step < step_map[target] {
                step_map[target] = new_step;
                queue.push_back(t.target);
            }
        }
    }

    step_map
}

// ─────────────────────────────────────────────────────────────────────────────
// Pattern classification (matcher-strategy.md §2.2–§2.3)
// ─────────────────────────────────────────────────────────────────────────────

/// Classify a compiled pattern for strategy selection.
///
/// Called after the NFA graph is fully constructed, epsilon-free, and
/// poison transitions are in place. Implements the classification
/// predicates from matcher-strategy.md §2.2.
pub fn classify_pattern(pattern: &MatchPattern, states: &[NfaState]) -> PatternClass {
    // 1. Check for IMMEDIATELY transitions.
    let has_immediately = pattern.steps.iter().any(|s| s.immediately_next);
    if has_immediately {
        return PatternClass::LinearImmediate;
    }

    // 2. Check for repetition (forces NFA).
    let has_repetition = pattern.steps.iter().any(|s| s.repetition.is_some());
    if has_repetition {
        return PatternClass::GeneralNfa;
    }

    // 3. Check for alternation within steps (forces NFA).
    let has_alternation = pattern
        .steps
        .iter()
        .any(|s| matches!(s.event, StepEvent::Alternation(_)));
    if has_alternation {
        return PatternClass::GeneralNfa;
    }

    // 4. Check NFA graph for structural branching (safety net).
    if nfa_has_branching(states) {
        return PatternClass::GeneralNfa;
    }

    // 5. Classify among linear variants.
    let has_negation = pattern.steps.iter().any(|s| s.without_next.is_some());
    let has_bindings = pattern_has_variable_bindings(pattern);

    match (has_negation, has_bindings) {
        (true, true) => PatternClass::LinearFull,
        (true, false) => PatternClass::LinearWithNegation,
        (false, true) => PatternClass::LinearWithBindings,
        (false, false) => PatternClass::LinearSimple,
    }
}

/// Returns true if any NFA state has forward transitions to more than
/// one distinct target state or has self-loops.
///
/// Only forward transitions are inspected — poison transitions do not
/// create structural branching (matcher-strategy.md §2.3).
fn nfa_has_branching(states: &[NfaState]) -> bool {
    for (state_idx, state) in states.iter().enumerate() {
        let distinct_targets: HashSet<u16> = state.transitions.iter().map(|t| t.target).collect();
        if distinct_targets.len() > 1 {
            return true;
        }
        // Self-loops indicate repetition.
        if state
            .transitions
            .iter()
            .any(|t| t.target == state_idx as u16)
        {
            return true;
        }
    }
    false
}

/// Check whether any step predicate contains a variable reference.
fn pattern_has_variable_bindings(pattern: &MatchPattern) -> bool {
    pattern.steps.iter().any(|s| {
        s.predicate
            .as_ref()
            .is_some_and(|p| expr_contains_variable(&p.node))
    })
}

/// Recursively check whether an expression tree contains any
/// `Expr::Variable` reference.
fn expr_contains_variable(expr: &Expr) -> bool {
    match expr {
        Expr::Variable(_) => true,
        Expr::Binary { left, right, .. } | Expr::Compare { left, right, .. } => {
            expr_contains_variable(&left.node) || expr_contains_variable(&right.node)
        }
        Expr::Unary { operand, .. } => expr_contains_variable(&operand.node),
        Expr::Not(inner) | Expr::Paren(inner) => expr_contains_variable(&inner.node),
        Expr::And(exprs) | Expr::Or(exprs) => exprs.iter().any(|e| expr_contains_variable(&e.node)),
        Expr::FunctionCall { args, .. } => args.iter().any(|a| expr_contains_variable(&a.node)),
        Expr::IsNull { expr, .. }
        | Expr::Like { expr, .. }
        | Expr::Regex { expr, .. }
        | Expr::Contains { expr, .. }
        | Expr::Cast { expr, .. } => expr_contains_variable(&expr.node),
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_contains_variable(&expr.node)
                || expr_contains_variable(&low.node)
                || expr_contains_variable(&high.node)
        }
        Expr::In { lhs, rhs, .. } => {
            lhs.iter().any(|e| expr_contains_variable(&e.node))
                || matches!(rhs, InRhs::List(exprs) if exprs.iter().any(|e| expr_contains_variable(&e.node)))
        }
        Expr::Case { arms, else_expr } => {
            arms.iter().any(|arm| {
                expr_contains_variable(&arm.condition.node)
                    || expr_contains_variable(&arm.value.node)
            }) || else_expr
                .as_ref()
                .is_some_and(|e| expr_contains_variable(&e.node))
        }
        Expr::Literal(_) | Expr::Column(_) | Expr::Qualified { .. } => false,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Event type collection
// ─────────────────────────────────────────────────────────────────────────────

/// Collect all event types referenced by steps and negation clauses.
///
/// Used for scan-level pushdown — the scan only needs to read events
/// whose types appear in this set.
fn collect_event_types(pattern: &MatchPattern) -> BTreeSet<String> {
    let mut types = BTreeSet::new();

    for step in &pattern.steps {
        match &step.event {
            StepEvent::Single(event_ref) => {
                types.insert(event_ref.event.text.clone());
            }
            StepEvent::Alternation(event_refs) => {
                for event_ref in event_refs {
                    types.insert(event_ref.event.text.clone());
                }
            }
        }

        // Negation event types must be included for poison transitions.
        if let Some(ref excl) = step.without_next {
            for event_ref in &excl.events {
                types.insert(event_ref.event.text.clone());
            }
        }
    }

    types
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use bqlite_ast::expr::{CompareOp, Literal};
    use bqlite_ast::pattern::{EventRef, Exclusion, MatchMode, MatchWindow, Repetition};
    use bqlite_ast::span::{Name, Span};
    use bqlite_core::property::BqlType;
    use bqlite_core::schema::ColumnDef;

    // ── Test helpers ────────────────────────────────────────────────

    fn test_schema() -> OperatorSchema {
        OperatorSchema::new(vec![
            ColumnDef::required("entity_id", BqlType::String),
            ColumnDef::required("ts", BqlType::Timestamp),
            ColumnDef::required("event_type", BqlType::String),
            ColumnDef::nullable("plan", BqlType::String),
            ColumnDef::nullable("price", BqlType::Float),
            ColumnDef::nullable("country", BqlType::String),
        ])
        .unwrap()
    }

    fn test_registry() -> FunctionRegistry {
        FunctionRegistry::with_builtins()
    }

    fn evt(name: &str) -> EventRef {
        EventRef {
            table: None,
            event: Name::synthetic(name),
            span: Span::EMPTY,
        }
    }

    fn simple_step(name: &str) -> Step {
        Step {
            name: None,
            event: StepEvent::Single(evt(name)),
            predicate: None,
            repetition: None,
            immediately_next: false,
            without_next: None,
            span: Span::EMPTY,
        }
    }

    fn pattern(steps: Vec<Step>) -> MatchPattern {
        MatchPattern {
            steps,
            mode: MatchMode::First,
            window: None,
            brackets: None,
            span: Span::EMPTY,
        }
    }

    fn col_eq_lit(col: &str, val: Literal) -> Spanned<Expr> {
        Spanned::new(
            Expr::Compare {
                op: CompareOp::Equal,
                left: Box::new(Spanned::new(
                    Expr::Column(Name::synthetic(col)),
                    Span::EMPTY,
                )),
                right: Box::new(Spanned::new(Expr::Literal(val), Span::EMPTY)),
            },
            Span::EMPTY,
        )
    }

    fn col_eq_var(col: &str, var: &str) -> Spanned<Expr> {
        Spanned::new(
            Expr::Compare {
                op: CompareOp::Equal,
                left: Box::new(Spanned::new(
                    Expr::Column(Name::synthetic(col)),
                    Span::EMPTY,
                )),
                right: Box::new(Spanned::new(
                    Expr::Variable(Name::synthetic(var)),
                    Span::EMPTY,
                )),
            },
            Span::EMPTY,
        )
    }

    // ── Pattern class: LinearSimple ─────────────────────────────────

    #[test]
    fn one_step_linear_simple() {
        let p = pattern(vec![simple_step("signup")]);
        let nfa = compile_pattern(&p, &test_schema(), &test_registry()).unwrap();

        assert_eq!(nfa.pattern_class, PatternClass::LinearSimple);
        assert_eq!(nfa.states.len(), 2); // start + accept
        assert_eq!(nfa.accept_state, 1);
        assert_eq!(nfa.state_to_step, vec![0, 1]);
        assert!(nfa.relevant_event_types.contains("signup"));
        assert!(nfa.variable_bindings.is_empty());
        assert!(!nfa.emit_all);
    }

    #[test]
    fn three_step_linear_simple() {
        let p = pattern(vec![
            simple_step("view"),
            simple_step("cart"),
            simple_step("purchase"),
        ]);
        let nfa = compile_pattern(&p, &test_schema(), &test_registry()).unwrap();

        assert_eq!(nfa.pattern_class, PatternClass::LinearSimple);
        assert_eq!(nfa.states.len(), 4);
        assert_eq!(nfa.accept_state, 3);
        assert_eq!(nfa.state_to_step, vec![0, 1, 2, 3]);

        // State 0 --[view]--> State 1
        assert_eq!(nfa.states[0].transitions.len(), 1);
        assert_eq!(nfa.states[0].transitions[0].event_type, "view");
        assert_eq!(nfa.states[0].transitions[0].target, 1);

        // State 1 --[cart]--> State 2
        assert_eq!(nfa.states[1].transitions.len(), 1);
        assert_eq!(nfa.states[1].transitions[0].event_type, "cart");
        assert_eq!(nfa.states[1].transitions[0].target, 2);

        // State 2 --[purchase]--> State 3
        assert_eq!(nfa.states[2].transitions.len(), 1);
        assert_eq!(nfa.states[2].transitions[0].event_type, "purchase");
        assert_eq!(nfa.states[2].transitions[0].target, 3);

        let expected_types: BTreeSet<String> = ["view", "cart", "purchase"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(nfa.relevant_event_types, expected_types);
    }

    #[test]
    fn five_step_linear_simple() {
        let steps: Vec<Step> = (0..5).map(|i| simple_step(&format!("step_{i}"))).collect();
        let p = pattern(steps);
        let nfa = compile_pattern(&p, &test_schema(), &test_registry()).unwrap();

        assert_eq!(nfa.pattern_class, PatternClass::LinearSimple);
        assert_eq!(nfa.states.len(), 6);
        assert_eq!(nfa.accept_state, 5);
    }

    // ── Pattern class: LinearImmediate ──────────────────────────────

    #[test]
    fn linear_immediate_classification() {
        let mut steps = vec![simple_step("view"), simple_step("purchase")];
        steps[0].immediately_next = true;
        let p = pattern(steps);
        let nfa = compile_pattern(&p, &test_schema(), &test_registry()).unwrap();

        assert_eq!(nfa.pattern_class, PatternClass::LinearImmediate);
        assert_eq!(nfa.states.len(), 3);
    }

    // ── Pattern class: LinearWithNegation ───────────────────────────

    #[test]
    fn linear_with_negation_classification() {
        let mut steps = vec![simple_step("signup"), simple_step("purchase")];
        steps[0].without_next = Some(Exclusion {
            events: vec![evt("churn")],
            span: Span::EMPTY,
        });
        let p = pattern(steps);
        let nfa = compile_pattern(&p, &test_schema(), &test_registry()).unwrap();

        assert_eq!(nfa.pattern_class, PatternClass::LinearWithNegation);
        // State 1 should have a poison transition for "churn".
        assert_eq!(nfa.states[1].poison_transitions.len(), 1);
        assert_eq!(nfa.states[1].poison_transitions[0].event_type, "churn");
        // "churn" should be in relevant event types for scan pushdown.
        assert!(nfa.relevant_event_types.contains("churn"));
    }

    #[test]
    fn negation_with_multiple_exclusion_events() {
        let mut steps = vec![simple_step("signup"), simple_step("purchase")];
        steps[0].without_next = Some(Exclusion {
            events: vec![evt("churn"), evt("unsubscribe")],
            span: Span::EMPTY,
        });
        let p = pattern(steps);
        let nfa = compile_pattern(&p, &test_schema(), &test_registry()).unwrap();

        assert_eq!(nfa.states[1].poison_transitions.len(), 2);
        let poison_types: HashSet<&str> = nfa.states[1]
            .poison_transitions
            .iter()
            .map(|p| p.event_type.as_str())
            .collect();
        assert!(poison_types.contains("churn"));
        assert!(poison_types.contains("unsubscribe"));
    }

    // ── Pattern class: LinearWithBindings ───────────────────────────

    #[test]
    fn linear_with_bindings_classification() {
        let steps = vec![
            Step {
                predicate: Some(col_eq_var("plan", "plan")),
                ..simple_step("signup")
            },
            Step {
                predicate: Some(col_eq_var("plan", "plan")),
                ..simple_step("purchase")
            },
        ];
        let p = pattern(steps);
        let nfa = compile_pattern(&p, &test_schema(), &test_registry()).unwrap();

        assert_eq!(nfa.pattern_class, PatternClass::LinearWithBindings);
        assert_eq!(nfa.variable_bindings.len(), 1);
        assert_eq!(nfa.variable_bindings[0].name, "plan");
        assert_eq!(nfa.variable_bindings[0].source_column, "plan");
        assert_eq!(nfa.variable_bindings[0].bind_step, 0);

        // Step 0 transition should bind the variable.
        assert_eq!(nfa.states[0].transitions[0].bind_variables, vec![0]);
        assert!(nfa.states[0].transitions[0].check_variables.is_empty());

        // Step 1 transition should check the variable.
        assert!(nfa.states[1].transitions[0].bind_variables.is_empty());
        assert_eq!(nfa.states[1].transitions[0].check_variables, vec![0]);
    }

    // ── Pattern class: LinearFull ───────────────────────────────────

    #[test]
    fn linear_full_classification() {
        let steps = vec![
            Step {
                predicate: Some(col_eq_var("plan", "plan")),
                without_next: Some(Exclusion {
                    events: vec![evt("churn")],
                    span: Span::EMPTY,
                }),
                ..simple_step("signup")
            },
            Step {
                predicate: Some(col_eq_var("plan", "plan")),
                ..simple_step("purchase")
            },
        ];
        let p = pattern(steps);
        let nfa = compile_pattern(&p, &test_schema(), &test_registry()).unwrap();

        assert_eq!(nfa.pattern_class, PatternClass::LinearFull);
        assert_eq!(nfa.variable_bindings.len(), 1);
        assert_eq!(nfa.states[1].poison_transitions.len(), 1);
    }

    // ── Pattern class: GeneralNfa (alternation) ─────────────────────

    #[test]
    fn alternation_classified_as_general_nfa() {
        let steps = vec![
            simple_step("view"),
            Step {
                event: StepEvent::Alternation(vec![evt("cart"), evt("wishlist")]),
                ..simple_step("ignored")
            },
            simple_step("purchase"),
        ];
        let p = pattern(steps);
        let nfa = compile_pattern(&p, &test_schema(), &test_registry()).unwrap();

        assert_eq!(nfa.pattern_class, PatternClass::GeneralNfa);
        // State 1 should have two forward transitions (cart and wishlist).
        assert_eq!(nfa.states[1].transitions.len(), 2);
        let types: HashSet<&str> = nfa.states[1]
            .transitions
            .iter()
            .map(|t| t.event_type.as_str())
            .collect();
        assert!(types.contains("cart"));
        assert!(types.contains("wishlist"));
    }

    // ── Pattern class: GeneralNfa (repetition +) ────────────────────

    #[test]
    fn one_or_more_repetition_classified_as_general_nfa() {
        let steps = vec![
            simple_step("view"),
            Step {
                repetition: Some(Repetition::OneOrMore),
                ..simple_step("browse")
            },
            simple_step("purchase"),
        ];
        let p = pattern(steps);
        let nfa = compile_pattern(&p, &test_schema(), &test_registry()).unwrap();

        assert_eq!(nfa.pattern_class, PatternClass::GeneralNfa);
        // State 1 --[browse]--> State 2 (first browse)
        // State 2 --[browse]--> State 2 (self-loop)
        // State 2 --[purchase]--> State 3 (accept)
        assert_eq!(nfa.states.len(), 4);
        assert_eq!(nfa.accept_state, 3);

        // State 2 has self-loop and forward transition.
        let state2 = &nfa.states[2];
        assert!(state2.transitions.iter().any(|t| t.target == 2)); // self-loop
        assert!(state2.transitions.iter().any(|t| t.target == 3)); // forward
    }

    // ── Pattern class: GeneralNfa (repetition *) ────────────────────

    #[test]
    fn zero_or_more_repetition_classified_as_general_nfa() {
        let steps = vec![
            simple_step("view"),
            Step {
                repetition: Some(Repetition::ZeroOrMore),
                ..simple_step("browse")
            },
            simple_step("purchase"),
        ];
        let p = pattern(steps);
        let nfa = compile_pattern(&p, &test_schema(), &test_registry()).unwrap();

        assert_eq!(nfa.pattern_class, PatternClass::GeneralNfa);
    }

    #[test]
    fn zero_or_more_epsilon_elimination_produces_skip_transition() {
        // view THEN browse* THEN purchase
        // State 0 should have a transition for "purchase" (via epsilon
        // closure: state 0 --ε--> state 1, state 1 --[purchase]--> accept).
        let steps = vec![
            simple_step("view"),
            Step {
                repetition: Some(Repetition::ZeroOrMore),
                ..simple_step("browse")
            },
            simple_step("purchase"),
        ];
        let p = pattern(steps);
        let nfa = compile_pattern(&p, &test_schema(), &test_registry()).unwrap();

        // State 1 (after view) should be able to reach purchase directly
        // via epsilon-derived transition (skip browse).
        let state1_targets: HashSet<(&str, u16)> = nfa.states[1]
            .transitions
            .iter()
            .map(|t| (t.event_type.as_str(), t.target))
            .collect();
        assert!(
            state1_targets.iter().any(|(ev, _)| *ev == "purchase"),
            "state 1 should have a 'purchase' transition via epsilon elimination; \
             got targets: {state1_targets:?}",
        );
    }

    #[test]
    fn chained_zero_or_more_epsilon_elimination() {
        // A* THEN B* THEN C — state 0 should have a [C]->accept transition
        // (zero A's, zero B's, straight to C) via chained epsilon closure.
        let steps = vec![
            Step {
                repetition: Some(Repetition::ZeroOrMore),
                ..simple_step("a")
            },
            Step {
                repetition: Some(Repetition::ZeroOrMore),
                ..simple_step("b")
            },
            simple_step("c"),
        ];
        let p = pattern(steps);
        let nfa = compile_pattern(&p, &test_schema(), &test_registry()).unwrap();

        // State 0 should be able to match C directly.
        let state0_event_types: HashSet<&str> = nfa.states[0]
            .transitions
            .iter()
            .map(|t| t.event_type.as_str())
            .collect();
        assert!(
            state0_event_types.contains("a"),
            "state 0 should match 'a'; got: {state0_event_types:?}",
        );
        assert!(
            state0_event_types.contains("b"),
            "state 0 should match 'b' via epsilon; got: {state0_event_types:?}",
        );
        assert!(
            state0_event_types.contains("c"),
            "state 0 should match 'c' via chained epsilon; got: {state0_event_types:?}",
        );
    }

    // ── Global window ───────────────────────────────────────────────

    #[test]
    fn global_window_within_duration() {
        let p = MatchPattern {
            steps: vec![simple_step("signup"), simple_step("purchase")],
            mode: MatchMode::First,
            window: Some(MatchWindow::Within(86_400_000_000_000)),
            brackets: None,
            span: Span::EMPTY,
        };
        let nfa = compile_pattern(&p, &test_schema(), &test_registry()).unwrap();
        assert_eq!(nfa.global_window, Some(86_400_000_000_000));
    }

    #[test]
    fn global_window_session_is_none() {
        let p = MatchPattern {
            steps: vec![simple_step("signup")],
            mode: MatchMode::First,
            window: Some(MatchWindow::WithinSession),
            brackets: None,
            span: Span::EMPTY,
        };
        let nfa = compile_pattern(&p, &test_schema(), &test_registry()).unwrap();
        assert_eq!(nfa.global_window, None);
    }

    #[test]
    fn no_window_is_none() {
        let p = pattern(vec![simple_step("signup")]);
        let nfa = compile_pattern(&p, &test_schema(), &test_registry()).unwrap();
        assert_eq!(nfa.global_window, None);
    }

    // ── emit_all ────────────────────────────────────────────────────

    #[test]
    fn emit_all_flag() {
        let p = MatchPattern {
            steps: vec![simple_step("signup"), simple_step("purchase")],
            mode: MatchMode::EmitAll,
            window: None,
            brackets: None,
            span: Span::EMPTY,
        };
        let nfa = compile_pattern(&p, &test_schema(), &test_registry()).unwrap();
        assert!(nfa.emit_all);
    }

    #[test]
    fn first_mode_not_emit_all() {
        let p = pattern(vec![simple_step("signup")]);
        let nfa = compile_pattern(&p, &test_schema(), &test_registry()).unwrap();
        assert!(!nfa.emit_all);
    }

    // ── Step predicates ─────────────────────────────────────────────

    #[test]
    fn step_with_non_variable_predicate() {
        let steps = vec![Step {
            predicate: Some(col_eq_lit("country", Literal::String("US".to_string()))),
            ..simple_step("signup")
        }];
        let p = pattern(steps);
        let nfa = compile_pattern(&p, &test_schema(), &test_registry()).unwrap();

        assert_eq!(nfa.pattern_class, PatternClass::LinearSimple);
        // The predicate should be compiled on the transition.
        assert_eq!(nfa.states[0].transitions[0].predicates.len(), 1);
    }

    #[test]
    fn step_with_variable_and_regular_predicate() {
        // plan = $plan AND country = 'US'
        let pred = Spanned::new(
            Expr::And(vec![
                col_eq_var("plan", "plan"),
                col_eq_lit("country", Literal::String("US".to_string())),
            ]),
            Span::EMPTY,
        );
        let steps = vec![
            Step {
                predicate: Some(pred),
                ..simple_step("signup")
            },
            Step {
                predicate: Some(col_eq_var("plan", "plan")),
                ..simple_step("purchase")
            },
        ];
        let p = pattern(steps);
        let nfa = compile_pattern(&p, &test_schema(), &test_registry()).unwrap();

        assert_eq!(nfa.pattern_class, PatternClass::LinearWithBindings);
        assert_eq!(nfa.variable_bindings.len(), 1);
        // Step 0 should have the country='US' predicate compiled.
        assert_eq!(nfa.states[0].transitions[0].predicates.len(), 1);
        // Step 0 should bind the variable.
        assert_eq!(nfa.states[0].transitions[0].bind_variables, vec![0]);
    }

    // ── Validation errors ───────────────────────────────────────────

    #[test]
    fn rejects_empty_pattern() {
        let p = pattern(vec![]);
        let err = compile_pattern(&p, &test_schema(), &test_registry()).unwrap_err();
        match err {
            BqliteError::Plan(msg) => assert!(msg.contains("at least one step"), "got: {msg}"),
            other => panic!("expected Plan error, got {other:?}"),
        }
    }

    #[test]
    fn rejects_variable_in_non_equality_context() {
        // price > $plan (variable not in equality)
        let pred = Spanned::new(
            Expr::Compare {
                op: CompareOp::Greater,
                left: Box::new(Spanned::new(
                    Expr::Column(Name::synthetic("price")),
                    Span::EMPTY,
                )),
                right: Box::new(Spanned::new(
                    Expr::Variable(Name::synthetic("threshold")),
                    Span::EMPTY,
                )),
            },
            Span::EMPTY,
        );
        let steps = vec![Step {
            predicate: Some(pred),
            ..simple_step("signup")
        }];
        let p = pattern(steps);
        let err = compile_pattern(&p, &test_schema(), &test_registry()).unwrap_err();
        match err {
            BqliteError::Plan(msg) => {
                assert!(msg.contains("equality comparison"), "got: {msg}");
            }
            other => panic!("expected Plan error, got {other:?}"),
        }
    }

    #[test]
    fn rejects_variable_referencing_unknown_column() {
        let steps = vec![
            Step {
                predicate: Some(col_eq_var("nonexistent", "myvar")),
                ..simple_step("signup")
            },
            Step {
                predicate: Some(col_eq_var("nonexistent", "myvar")),
                ..simple_step("purchase")
            },
        ];
        let p = pattern(steps);
        let err = compile_pattern(&p, &test_schema(), &test_registry()).unwrap_err();
        match err {
            BqliteError::Plan(msg) => {
                assert!(msg.contains("unknown column"), "got: {msg}");
                assert!(msg.contains("nonexistent"), "got: {msg}");
            }
            other => panic!("expected Plan error, got {other:?}"),
        }
    }

    #[test]
    fn paren_wrapped_variable_binding_detected() {
        // (plan = $plan) — the Paren wrapper should not hide the variable.
        let pred = Spanned::new(
            Expr::Paren(Box::new(Spanned::new(
                Expr::Compare {
                    op: CompareOp::Equal,
                    left: Box::new(Spanned::new(
                        Expr::Column(Name::synthetic("plan")),
                        Span::EMPTY,
                    )),
                    right: Box::new(Spanned::new(
                        Expr::Variable(Name::synthetic("plan")),
                        Span::EMPTY,
                    )),
                },
                Span::EMPTY,
            ))),
            Span::EMPTY,
        );
        let steps = vec![
            Step {
                predicate: Some(pred),
                ..simple_step("signup")
            },
            Step {
                predicate: Some(col_eq_var("plan", "plan")),
                ..simple_step("purchase")
            },
        ];
        let p = pattern(steps);
        let nfa = compile_pattern(&p, &test_schema(), &test_registry()).unwrap();

        assert_eq!(nfa.pattern_class, PatternClass::LinearWithBindings);
        assert_eq!(nfa.variable_bindings.len(), 1);
        assert_eq!(nfa.variable_bindings[0].name, "plan");
    }

    // ── Event type collection ───────────────────────────────────────

    #[test]
    fn event_types_include_negation_targets() {
        let mut steps = vec![simple_step("signup"), simple_step("purchase")];
        steps[0].without_next = Some(Exclusion {
            events: vec![evt("churn")],
            span: Span::EMPTY,
        });
        let p = pattern(steps);
        let nfa = compile_pattern(&p, &test_schema(), &test_registry()).unwrap();

        let expected: BTreeSet<String> = ["signup", "purchase", "churn"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(nfa.relevant_event_types, expected);
    }

    #[test]
    fn event_types_include_alternation_variants() {
        let steps = vec![Step {
            event: StepEvent::Alternation(vec![evt("a"), evt("b"), evt("c")]),
            ..simple_step("ignored")
        }];
        let p = pattern(steps);
        let nfa = compile_pattern(&p, &test_schema(), &test_registry()).unwrap();

        let expected: BTreeSet<String> = ["a", "b", "c"].iter().map(|s| s.to_string()).collect();
        assert_eq!(nfa.relevant_event_types, expected);
    }

    // ── Strategy selection ──────────────────────────────────────────

    #[test]
    fn strategy_linear_simple_without_demand() {
        let config = MatchExecutionConfig::default();
        assert_eq!(
            select_strategy(PatternClass::LinearSimple, &config),
            MatchStrategy::StepCounter,
        );
    }

    #[test]
    fn strategy_linear_simple_with_match_events_demand() {
        let config = MatchExecutionConfig {
            track_match_events: true,
            ..Default::default()
        };
        assert_eq!(
            select_strategy(PatternClass::LinearSimple, &config),
            MatchStrategy::FullNfa,
        );
    }

    #[test]
    fn strategy_linear_immediate() {
        let config = MatchExecutionConfig::default();
        assert_eq!(
            select_strategy(PatternClass::LinearImmediate, &config),
            MatchStrategy::ConsecutiveMatcher,
        );
    }

    #[test]
    fn strategy_general_nfa() {
        let config = MatchExecutionConfig::default();
        assert_eq!(
            select_strategy(PatternClass::GeneralNfa, &config),
            MatchStrategy::FullNfa,
        );
    }

    #[test]
    fn strategy_linear_with_negation() {
        let config = MatchExecutionConfig::default();
        assert_eq!(
            select_strategy(PatternClass::LinearWithNegation, &config),
            MatchStrategy::StepCounter,
        );
    }

    #[test]
    fn strategy_linear_with_bindings() {
        let config = MatchExecutionConfig::default();
        assert_eq!(
            select_strategy(PatternClass::LinearWithBindings, &config),
            MatchStrategy::StepCounter,
        );
    }

    #[test]
    fn strategy_linear_full() {
        let config = MatchExecutionConfig::default();
        assert_eq!(
            select_strategy(PatternClass::LinearFull, &config),
            MatchStrategy::StepCounter,
        );
    }

    #[test]
    fn strategy_match_duration_does_not_force_nfa() {
        let config = MatchExecutionConfig {
            track_match_duration: true,
            track_match_events: false,
        };
        assert_eq!(
            select_strategy(PatternClass::LinearSimple, &config),
            MatchStrategy::StepCounter,
        );
    }

    // ── state_to_step mapping ───────────────────────────────────────

    #[test]
    fn state_to_step_with_repetition() {
        // view THEN browse+ THEN purchase
        let steps = vec![
            simple_step("view"),
            Step {
                repetition: Some(Repetition::OneOrMore),
                ..simple_step("browse")
            },
            simple_step("purchase"),
        ];
        let p = pattern(steps);
        let nfa = compile_pattern(&p, &test_schema(), &test_registry()).unwrap();

        // States: 0(start), 1(first browse from view), 2(browse loop), 3(accept)
        // State 0 → step 0, State 1 → step 1, State 2 → step 2, State 3 → step 3
        assert_eq!(nfa.state_to_step.len(), nfa.states.len());
        assert_eq!(nfa.state_to_step[0], 0);
    }

    // ── Variable binding with multiple variables ────────────────────

    #[test]
    fn multiple_variable_bindings() {
        let steps = vec![
            Step {
                predicate: Some(Spanned::new(
                    Expr::And(vec![
                        col_eq_var("plan", "plan"),
                        col_eq_var("country", "country"),
                    ]),
                    Span::EMPTY,
                )),
                ..simple_step("signup")
            },
            Step {
                predicate: Some(Spanned::new(
                    Expr::And(vec![
                        col_eq_var("plan", "plan"),
                        col_eq_var("country", "country"),
                    ]),
                    Span::EMPTY,
                )),
                ..simple_step("purchase")
            },
        ];
        let p = pattern(steps);
        let nfa = compile_pattern(&p, &test_schema(), &test_registry()).unwrap();

        assert_eq!(nfa.variable_bindings.len(), 2);
        assert_eq!(nfa.variable_bindings[0].name, "plan");
        assert_eq!(nfa.variable_bindings[1].name, "country");
        // Both bound at step 0.
        assert_eq!(nfa.variable_bindings[0].bind_step, 0);
        assert_eq!(nfa.variable_bindings[1].bind_step, 0);

        // Step 0 should bind both variables.
        assert_eq!(nfa.states[0].transitions[0].bind_variables, vec![0, 1],);
        // Step 1 should check both variables.
        assert_eq!(nfa.states[1].transitions[0].check_variables, vec![0, 1],);
    }

    // ── Predicate-only step (no variable) ───────────────────────────

    #[test]
    fn step_predicate_with_greater_than() {
        let pred = Spanned::new(
            Expr::Compare {
                op: CompareOp::Greater,
                left: Box::new(Spanned::new(
                    Expr::Column(Name::synthetic("price")),
                    Span::EMPTY,
                )),
                right: Box::new(Spanned::new(
                    Expr::Literal(Literal::Float(100.0)),
                    Span::EMPTY,
                )),
            },
            Span::EMPTY,
        );
        let steps = vec![Step {
            predicate: Some(pred),
            ..simple_step("purchase")
        }];
        let p = pattern(steps);
        let nfa = compile_pattern(&p, &test_schema(), &test_registry()).unwrap();

        assert_eq!(nfa.pattern_class, PatternClass::LinearSimple);
        assert_eq!(nfa.states[0].transitions[0].predicates.len(), 1);
    }

    // ── Negation on non-last step ───────────────────────────────────

    #[test]
    fn negation_on_middle_step() {
        let mut steps = vec![
            simple_step("signup"),
            simple_step("browse"),
            simple_step("purchase"),
        ];
        steps[1].without_next = Some(Exclusion {
            events: vec![evt("churn")],
            span: Span::EMPTY,
        });
        let p = pattern(steps);
        let nfa = compile_pattern(&p, &test_schema(), &test_registry()).unwrap();

        // Poison should be on state 2 (target of step 1's transition).
        assert_eq!(nfa.states[2].poison_transitions.len(), 1);
        assert_eq!(nfa.states[2].poison_transitions[0].event_type, "churn");
        // State 1 should have no poison transitions.
        assert!(nfa.states[1].poison_transitions.is_empty());
    }

    // ── Match mode propagation ──────────────────────────────────────

    #[test]
    fn all_mode_is_not_emit_all() {
        let p = MatchPattern {
            steps: vec![simple_step("signup")],
            mode: MatchMode::All,
            window: None,
            brackets: None,
            span: Span::EMPTY,
        };
        let nfa = compile_pattern(&p, &test_schema(), &test_registry()).unwrap();
        assert!(!nfa.emit_all);
    }

    // ── NFA structural verification for alternation ─────────────────

    #[test]
    fn alternation_transitions_share_target() {
        let steps = vec![
            simple_step("start"),
            Step {
                event: StepEvent::Alternation(vec![evt("a"), evt("b")]),
                ..simple_step("ignored")
            },
        ];
        let p = pattern(steps);
        let nfa = compile_pattern(&p, &test_schema(), &test_registry()).unwrap();

        // State 1 should have two transitions with the same target.
        let targets: HashSet<u16> = nfa.states[1].transitions.iter().map(|t| t.target).collect();
        assert_eq!(
            targets.len(),
            1,
            "alternation transitions should share a target"
        );
    }

    // ── Combined repetition + negation ──────────────────────────────

    #[test]
    fn repetition_with_negation_on_prior_step() {
        let mut steps = vec![
            simple_step("signup"),
            Step {
                repetition: Some(Repetition::OneOrMore),
                ..simple_step("browse")
            },
            simple_step("purchase"),
        ];
        steps[0].without_next = Some(Exclusion {
            events: vec![evt("churn")],
            span: Span::EMPTY,
        });
        let p = pattern(steps);
        let nfa = compile_pattern(&p, &test_schema(), &test_registry()).unwrap();

        assert_eq!(nfa.pattern_class, PatternClass::GeneralNfa);
        // Poison for churn should be on the target of step 0's transition.
        let step0_target = nfa.states[0].transitions[0].target;
        assert!(
            nfa.states[step0_target as usize]
                .poison_transitions
                .iter()
                .any(|p| p.event_type == "churn"),
            "poison transition for 'churn' should be on state {step0_target}",
        );
    }
}
