//! Contract tests: planner-side `DEMAND_CAPS` == operator-side `supported_demands()`.
//!
//! See `docs/design/planner/demand-protocol.md` §6.2. These tests ensure the
//! planner's compile-time capability declaration stays in sync with the runtime
//! trait implementation. If an operator's capabilities change, both sites must
//! be updated in the same commit.

use std::collections::BTreeSet;

use bqlite_operators::EntityOperator;
use bqlite_planner::physical::{
    AttributePhysical, EventSelectPhysical, SequenceMatchPhysical, SessionizePhysical,
};

/// Build a minimal `SequenceMatchOperator` from the simplest possible
/// physical descriptor. We only need `supported_demands()` which reads no
/// instance state, so the pattern content is irrelevant.
fn minimal_sequence_match_operator() -> impl EntityOperator {
    use bqlite_core::{BqlType, ColumnDef, OperatorSchema};
    use bqlite_planner::compile::{
        CompiledNfa, MatchExecutionConfig, MatchStrategy, NfaState, PatternClass, Transition,
    };
    use bqlite_planner::demand::DemandSet;
    use bqlite_planner::physical::{PhysicalPlan, ScanPhysical};

    let schema = OperatorSchema::new(vec![
        ColumnDef::required("entity_id", BqlType::String),
        ColumnDef::required("ts", BqlType::Timestamp),
        ColumnDef::required("event_type", BqlType::String),
    ])
    .unwrap();

    // Minimal 2-state NFA: state 0 --"a"--> state 1 (accept)
    let nfa = CompiledNfa {
        states: vec![
            NfaState {
                transitions: vec![Transition {
                    event_type: "a".to_string(),
                    predicates: vec![],
                    bind_variables: vec![],
                    check_variables: vec![],
                    target: 1,
                }],
                poison_transitions: vec![],
            },
            NfaState {
                transitions: vec![],
                poison_transitions: vec![],
            },
        ],
        accept_state: 1,
        relevant_event_types: BTreeSet::from(["a".to_string()]),
        pattern_class: PatternClass::LinearSimple,
        variable_bindings: vec![],
        emit_all: false,
        global_window: None,
        state_to_step: vec![0, 1],
    };

    let scan = ScanPhysical {
        table: "events".to_string(),
        query_range: None,
        reader_range: None,
        scan_predicates: vec![],
        projected_columns: vec![],
        output_schema: schema.clone(),
        entity_key_col: "entity_id".to_string(),
        timestamp_col: "ts".to_string(),
    };

    let desc = SequenceMatchPhysical {
        compiled_nfa: nfa,
        strategy: MatchStrategy::StepCounter,
        match_all: false,
        demand: DemandSet::empty(),
        execution_config: MatchExecutionConfig::default(),
        fused_aggregate: None,
        input: Box::new(PhysicalPlan::Scan(scan)),
        output_schema: schema,
    };

    bqlite_operators::SequenceMatchOperator::new(&desc)
}

#[test]
fn sequence_match_operator_caps_match_physical() {
    let op = minimal_sequence_match_operator();
    assert_eq!(
        op.supported_demands(),
        SequenceMatchPhysical::DEMAND_CAPS,
        "SequenceMatchOperator::supported_demands() must match \
         SequenceMatchPhysical::DEMAND_CAPS — update both in the same commit"
    );
}

// Wave 4 operator implementations (TASK-428, TASK-429, TASK-431) have not
// landed yet. When they do, each must add a contract test here:
//
//   #[test]
//   fn sessionize_operator_caps_match_physical() { ... }
//
//   #[test]
//   fn event_select_operator_caps_match_physical() { ... }
//
//   #[test]
//   fn attribute_operator_caps_match_physical() { ... }

#[test]
fn sessionize_physical_caps_are_forwarded_only() {
    let caps = SessionizePhysical::DEMAND_CAPS;
    assert!(caps.supports_forwarded_columns);
    assert!(!caps.supports_step_reached);
    assert!(!caps.supports_match_count);
    assert!(!caps.supports_full_detail);
    assert!(!caps.supports_aggregation_fusion);
    assert!(!caps.supports_step_property_forwarding);
    assert!(!caps.supports_eager_group_emit);
}

#[test]
fn event_select_physical_caps_are_forwarded_only() {
    let caps = EventSelectPhysical::DEMAND_CAPS;
    assert!(caps.supports_forwarded_columns);
    assert!(!caps.supports_step_reached);
    assert!(!caps.supports_aggregation_fusion);
}

#[test]
fn attribute_physical_caps_are_forwarded_only() {
    let caps = AttributePhysical::DEMAND_CAPS;
    assert!(caps.supports_forwarded_columns);
    assert!(!caps.supports_step_reached);
    assert!(!caps.supports_aggregation_fusion);
}
