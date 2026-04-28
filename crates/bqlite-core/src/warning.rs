//! Per-query non-fatal diagnostics surfaced through
//! `ExecutionResult::warnings` and `ExecutionFailure::warnings`.
//!
//! Lives in `bqlite-core` so both the operator-side
//! `EntityOperator::take_pending_warnings` (in `bqlite-operators`) and
//! the engine-side `ExecutionResult` (in `bqlite-engine`) can reference
//! it without violating the dependency direction.
//!
//! See `docs/design/engine/cancellation.md` §7 for the protocol.

use std::fmt;

/// Non-fatal diagnostic surfaced after a query completes.
///
/// The enum is **exhaustive** — exhaustive matching is part of the
/// published API so callers can render every variant with full
/// context. Future operators that add a warning shape add a variant
/// here. Do **not** add `#[non_exhaustive]`: per
/// `docs/design/engine/cancellation.md` §7.1 the explicit goal is
/// for downstream renderers (CLI, Python bindings) to compile-error
/// when a new variant lands without rendering coverage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryWarning {
    /// The entity event limit (default 10M) was reached for one entity;
    /// remaining events for that entity were dropped. See
    /// `docs/design/execution-model.md` §5.3.
    EntityEventLimitExceeded {
        entity_id: String,
        count: u64,
        limit: u64,
    },
    /// Sessionize per-entity event cap (default 1M) was reached for
    /// one entity; remaining events for that entity were dropped. See
    /// `docs/design/operators/sessionize.md` §11.3.
    SessionEventCapExceeded {
        entity_id: String,
        event_count: u64,
        cap: u64,
    },
    /// Attribute per-entity touchpoint cap was reached for one entity;
    /// remaining touchpoints for that entity were dropped. See
    /// `docs/design/operators/attribute.md` §10.
    AttributeTouchpointCapExceeded {
        entity_id: String,
        touchpoint_count: u64,
        cap: u64,
    },
    /// MATCH operator's active-state cap was reached for one entity;
    /// further state expansion was suppressed. See
    /// `docs/design/operators/match-operator.md` §13.3.
    ActiveStateLimitExceeded {
        entity_id: String,
        active_states: u64,
        cap: u64,
    },
    /// One or more workers exceeded the per-worker warning cap and
    /// silently dropped further warnings. Aggregated by the engine —
    /// the user sees a single `WarningsOverflow` even when many
    /// workers hit the cap. MUST be the last element of the assembled
    /// warning list when present (per `cancellation.md` §7.3); the
    /// `EntityOperator` implementors MUST NOT emit this variant
    /// themselves.
    WarningsOverflow { suppressed_count: u64 },
}

impl fmt::Display for QueryWarning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryWarning::EntityEventLimitExceeded {
                entity_id,
                count,
                limit,
            } => write!(
                f,
                "entity event limit exceeded: entity={entity_id}, count={count}, limit={limit}"
            ),
            QueryWarning::SessionEventCapExceeded {
                entity_id,
                event_count,
                cap,
            } => write!(
                f,
                "session event cap exceeded: entity={entity_id}, event_count={event_count}, cap={cap}"
            ),
            QueryWarning::AttributeTouchpointCapExceeded {
                entity_id,
                touchpoint_count,
                cap,
            } => write!(
                f,
                "attribute touchpoint cap exceeded: entity={entity_id}, touchpoint_count={touchpoint_count}, cap={cap}"
            ),
            QueryWarning::ActiveStateLimitExceeded {
                entity_id,
                active_states,
                cap,
            } => write!(
                f,
                "active state limit exceeded: entity={entity_id}, active_states={active_states}, cap={cap}"
            ),
            QueryWarning::WarningsOverflow { suppressed_count } => {
                write!(f, "{suppressed_count} further warnings suppressed")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entity_event_limit_display() {
        let w = QueryWarning::EntityEventLimitExceeded {
            entity_id: "u_42".into(),
            count: 10_000_001,
            limit: 10_000_000,
        };
        let s = w.to_string();
        assert!(s.contains("u_42"), "{s}");
        assert!(s.contains("10000001"), "{s}");
        assert!(s.contains("10000000"), "{s}");
    }

    #[test]
    fn session_event_cap_display() {
        let w = QueryWarning::SessionEventCapExceeded {
            entity_id: "u_99".into(),
            event_count: 1_000_001,
            cap: 1_000_000,
        };
        let s = w.to_string();
        assert!(s.contains("session"), "{s}");
        assert!(s.contains("u_99"), "{s}");
    }

    #[test]
    fn attribute_touchpoint_display() {
        let w = QueryWarning::AttributeTouchpointCapExceeded {
            entity_id: "u_5".into(),
            touchpoint_count: 1_000,
            cap: 999,
        };
        assert!(w.to_string().contains("attribute"));
    }

    #[test]
    fn active_state_limit_display() {
        let w = QueryWarning::ActiveStateLimitExceeded {
            entity_id: "u_1".into(),
            active_states: 10_001,
            cap: 10_000,
        };
        assert!(w.to_string().contains("active state"));
    }

    #[test]
    fn warnings_overflow_display_matches_cli_footer() {
        // Per `cancellation.md` §7.5 the CLI formats the overflow as
        // "N further warnings suppressed".
        let w = QueryWarning::WarningsOverflow {
            suppressed_count: 12,
        };
        assert_eq!(w.to_string(), "12 further warnings suppressed");
    }

    #[test]
    fn variants_are_eq_and_clone() {
        let w = QueryWarning::SessionEventCapExceeded {
            entity_id: "e".into(),
            event_count: 1,
            cap: 1,
        };
        assert_eq!(w.clone(), w);
    }
}
