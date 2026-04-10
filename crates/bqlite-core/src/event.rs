//! Entity and event primitives.
//!
//! `EntityId` identifies the subject an event belongs to. `Event` is a
//! row-oriented event record used at the ingest boundary, in schema-default
//! materialization, and in test fixtures. Query execution operates on
//! columnar Arrow batches, not on these types — they live on the boundary,
//! not the hot path.
//!
//! Per docs/design/type-system.md §5.1 the entity-key column of a declared
//! table must be `String` or `Int` and non-nullable; `EntityId` mirrors that
//! constraint with a two-variant enum.
//!
//! The `EntityEventStream` trait is the row-oriented counterpart to the
//! batch-oriented `EntityOperator` trait that ships in `bqlite-operators`
//! (TASK-108). It lives in `bqlite-core` so ingest code, fixture builders,
//! and simple tools can hand a per-entity event sequence to consumers that
//! don't want to pull in the `bqlite-operators` dependency.

use std::fmt;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::property::{BqlType, PropertyValue};
use crate::time::Timestamp;

// ──────────────────────────────────────────────────────────────────────────────
// EntityId
// ──────────────────────────────────────────────────────────────────────────────

/// Identifier for the entity an event belongs to.
///
/// Per docs/design/type-system.md §5.1 the entity-key column must be
/// `String` or `Int`, non-nullable. `EntityId` carries one of those two
/// shapes directly so code that handles row-oriented events never has to
/// pattern-match against `PropertyValue` just to read an id.
///
/// **Ordering.** `Ord` is derived: `String`-keyed ids sort before
/// `Int`-keyed ids (variant-discriminant order), and within a variant the
/// natural ordering of the payload applies. Within a single table the
/// entity-key column has a single declared type, so cross-variant
/// comparisons only arise when mixing ids from unrelated sources.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub enum EntityId {
    /// String-keyed entity (e.g. `"user_42"`, a UUID).
    String(String),
    /// Integer-keyed entity.
    Int(i64),
}

impl EntityId {
    /// Returns the `BqlType` of this entity id (`String` or `Int`).
    pub fn bql_type(&self) -> BqlType {
        match self {
            EntityId::String(_) => BqlType::String,
            EntityId::Int(_) => BqlType::Int,
        }
    }

    /// Borrow the underlying string, or `None` if this is an `Int` id.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            EntityId::String(s) => Some(s.as_str()),
            EntityId::Int(_) => None,
        }
    }

    /// Return the underlying integer, or `None` if this is a `String` id.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            EntityId::String(_) => None,
            EntityId::Int(n) => Some(*n),
        }
    }
}

impl From<String> for EntityId {
    #[inline]
    fn from(s: String) -> Self {
        EntityId::String(s)
    }
}

impl From<&str> for EntityId {
    #[inline]
    fn from(s: &str) -> Self {
        EntityId::String(s.to_owned())
    }
}

impl From<i64> for EntityId {
    #[inline]
    fn from(n: i64) -> Self {
        EntityId::Int(n)
    }
}

impl fmt::Display for EntityId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EntityId::String(s) => f.write_str(s),
            EntityId::Int(n) => write!(f, "{n}"),
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Event
// ──────────────────────────────────────────────────────────────────────────────

/// A single event occurrence in row-oriented form.
///
/// Events are boundary types: they appear at ingest, when materializing
/// schema defaults, and in test fixtures. Query execution operates on
/// columnar Arrow batches and never holds `Event` values on the hot path.
///
/// Properties are stored as a flat `Vec<(String, PropertyValue)>` rather
/// than a `HashMap` because:
///
/// - property bags at ingest are typically small (<~10 entries) where a
///   linear scan beats hashing once you count the hash itself;
/// - preserving declaration order is useful for diagnostics and fixtures;
/// - a flat vector avoids a per-event hash-table allocation.
///
/// Lookups use `Event::get()`; when a key appears more than once the first
/// occurrence wins, matching the parser's left-to-right evaluation order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    /// The entity this event belongs to.
    pub entity: EntityId,

    /// Event time — nanoseconds since the Unix epoch, UTC.
    pub timestamp: Timestamp,

    /// Event-type name (e.g. `"click"`, `"signup"`).
    ///
    /// Corresponds to the event-type column declared by the table schema.
    /// Stored as an owned `String` rather than a borrowed `&str` because
    /// `Event` is an owned, serializable record type.
    pub event_type: String,

    /// Ordered property bag. See the struct-level doc for rationale.
    pub properties: Vec<(String, PropertyValue)>,
}

impl Event {
    /// Construct an event with no properties.
    pub fn new(
        entity: impl Into<EntityId>,
        timestamp: Timestamp,
        event_type: impl Into<String>,
    ) -> Self {
        Self {
            entity: entity.into(),
            timestamp,
            event_type: event_type.into(),
            properties: Vec::new(),
        }
    }

    /// Construct an event with the given property list.
    pub fn with_properties(
        entity: impl Into<EntityId>,
        timestamp: Timestamp,
        event_type: impl Into<String>,
        properties: Vec<(String, PropertyValue)>,
    ) -> Self {
        Self {
            entity: entity.into(),
            timestamp,
            event_type: event_type.into(),
            properties,
        }
    }

    /// Borrow the value of the first property with the given key.
    ///
    /// Returns `None` if no property with that key exists. For duplicate
    /// keys the first occurrence wins.
    pub fn get(&self, key: &str) -> Option<&PropertyValue> {
        self.properties
            .iter()
            .find_map(|(k, v)| (k == key).then_some(v))
    }

    /// Raw nanosecond timestamp — convenient for Arrow interop without
    /// unwrapping the `Timestamp` newtype at the call site.
    #[inline]
    pub fn timestamp_nanos(&self) -> i64 {
        self.timestamp.as_nanos()
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// EntityEventStream
// ──────────────────────────────────────────────────────────────────────────────

/// An ordered stream of events for a single entity.
///
/// Implementors must yield events for one — and only one — entity, in
/// non-decreasing `timestamp` order. Downstream temporal operators
/// (sequence match, sessionize, retention) assume this invariant; violating
/// it is a programming error, not a runtime-checked condition.
///
/// This trait is the row-oriented counterpart to the batch-oriented
/// `EntityOperator` trait that ships in `bqlite-operators` (TASK-108). It
/// lives here so ingest code and test fixtures can hand a per-entity
/// stream to consumers without pulling in `bqlite-operators`.
pub trait EntityEventStream {
    /// The entity this stream represents.
    fn entity_id(&self) -> &EntityId;

    /// Yield the next event, or `None` when the stream is exhausted.
    fn next_event(&mut self) -> Option<Event>;
}

/// An `EntityEventStream` backed by an in-memory `Vec<Event>`.
///
/// Useful for test fixtures and small ingest paths. Input is validated at
/// construction time rather than per `next_event()` call so the hot path
/// is a plain `IntoIter::next()`.
#[derive(Debug, Clone)]
pub struct VecEntityEventStream {
    entity_id: EntityId,
    events: std::vec::IntoIter<Event>,
}

impl VecEntityEventStream {
    /// Construct from a vector of events belonging to a single entity.
    ///
    /// Returns `Err` if any event's `entity` differs from `entity_id`, or
    /// if timestamps are not in non-decreasing order.
    pub fn new(entity_id: EntityId, events: Vec<Event>) -> Result<Self, EntityEventStreamError> {
        let mut last_ts: Option<Timestamp> = None;
        for (index, ev) in events.iter().enumerate() {
            if ev.entity != entity_id {
                return Err(EntityEventStreamError::MismatchedEntity {
                    index,
                    expected: entity_id.clone(),
                    found: ev.entity.clone(),
                });
            }
            if let Some(prev) = last_ts {
                if ev.timestamp < prev {
                    return Err(EntityEventStreamError::OutOfOrder {
                        index,
                        previous: prev,
                        current: ev.timestamp,
                    });
                }
            }
            last_ts = Some(ev.timestamp);
        }
        Ok(Self {
            entity_id,
            events: events.into_iter(),
        })
    }
}

impl EntityEventStream for VecEntityEventStream {
    #[inline]
    fn entity_id(&self) -> &EntityId {
        &self.entity_id
    }

    #[inline]
    fn next_event(&mut self) -> Option<Event> {
        self.events.next()
    }
}

/// Error returned by `VecEntityEventStream::new` when input validation fails.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum EntityEventStreamError {
    /// An event's entity id did not match the stream's declared entity.
    #[error("event at index {index} belongs to entity '{found}', expected '{expected}'")]
    MismatchedEntity {
        /// 0-based index of the offending event in the input vector.
        index: usize,
        /// The entity the stream was declared for.
        expected: EntityId,
        /// The entity actually found on the event.
        found: EntityId,
    },
    /// Events were not provided in non-decreasing timestamp order.
    #[error(
        "event at index {index} out of order: current {cur_ns}ns < previous {prev_ns}ns",
        cur_ns = .current.as_nanos(),
        prev_ns = .previous.as_nanos()
    )]
    OutOfOrder {
        /// 0-based index of the offending event in the input vector.
        index: usize,
        /// The timestamp of the preceding event.
        previous: Timestamp,
        /// The timestamp of the offending event.
        current: Timestamp,
    },
}

// ──────────────────────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── EntityId ──────────────────────────────────────────────────────────────

    #[test]
    fn entity_id_from_string_and_str() {
        let a: EntityId = "user_1".into();
        let b: EntityId = String::from("user_1").into();
        assert_eq!(a, b);
        assert_eq!(a.as_str(), Some("user_1"));
        assert_eq!(a.as_i64(), None);
    }

    #[test]
    fn entity_id_from_i64() {
        let a: EntityId = 42_i64.into();
        assert_eq!(a, EntityId::Int(42));
        assert_eq!(a.as_i64(), Some(42));
        assert_eq!(a.as_str(), None);
    }

    #[test]
    fn entity_id_bql_type_matches_variant() {
        assert_eq!(EntityId::from("x").bql_type(), BqlType::String);
        assert_eq!(EntityId::from(1_i64).bql_type(), BqlType::Int);
    }

    #[test]
    fn entity_id_display() {
        assert_eq!(EntityId::from("abc").to_string(), "abc");
        assert_eq!(EntityId::from(-7_i64).to_string(), "-7");
    }

    #[test]
    fn entity_id_equality_ignores_variant_when_different() {
        // String("1") and Int(1) are NOT equal — they are different types.
        assert_ne!(EntityId::from("1"), EntityId::from(1_i64));
    }

    #[test]
    fn entity_id_serde_round_trip_string() {
        let id = EntityId::from("u42");
        let json = serde_json::to_string(&id).unwrap();
        let back: EntityId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, back);
    }

    #[test]
    fn entity_id_serde_round_trip_int() {
        let id = EntityId::from(9001_i64);
        let json = serde_json::to_string(&id).unwrap();
        let back: EntityId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, back);
    }

    // ── Event ─────────────────────────────────────────────────────────────────

    fn make_event() -> Event {
        Event::with_properties(
            "user_1",
            Timestamp::from_nanos(1_700_000_000_000_000_000),
            "purchase",
            vec![
                ("amount".into(), PropertyValue::Float(12.5)),
                ("plan".into(), PropertyValue::String("pro".into())),
            ],
        )
    }

    #[test]
    fn event_new_has_empty_properties() {
        let ev = Event::new("u", Timestamp::from_nanos(0), "click");
        assert_eq!(ev.entity, EntityId::from("u"));
        assert_eq!(ev.timestamp, Timestamp::from_nanos(0));
        assert_eq!(ev.event_type, "click");
        assert!(ev.properties.is_empty());
    }

    #[test]
    fn event_get_found() {
        let ev = make_event();
        assert_eq!(ev.get("amount"), Some(&PropertyValue::Float(12.5)));
        assert_eq!(ev.get("plan"), Some(&PropertyValue::String("pro".into())));
    }

    #[test]
    fn event_get_not_found_returns_none() {
        let ev = make_event();
        assert_eq!(ev.get("missing"), None);
    }

    #[test]
    fn event_get_duplicate_keys_first_wins() {
        let ev = Event::with_properties(
            "u",
            Timestamp::from_nanos(0),
            "t",
            vec![
                ("k".into(), PropertyValue::Int(1)),
                ("k".into(), PropertyValue::Int(2)),
            ],
        );
        assert_eq!(ev.get("k"), Some(&PropertyValue::Int(1)));
    }

    #[test]
    fn event_timestamp_nanos_matches_timestamp() {
        let ev = make_event();
        assert_eq!(ev.timestamp_nanos(), 1_700_000_000_000_000_000);
    }

    #[test]
    fn event_serde_round_trip() {
        let ev = make_event();
        let json = serde_json::to_string(&ev).unwrap();
        let back: Event = serde_json::from_str(&json).unwrap();
        assert_eq!(ev, back);
    }

    // ── VecEntityEventStream ──────────────────────────────────────────────────

    fn ev_at(entity: &str, ns: i64) -> Event {
        Event::new(entity, Timestamp::from_nanos(ns), "t")
    }

    #[test]
    fn vec_stream_yields_in_order_then_none() {
        let events = vec![ev_at("u", 1), ev_at("u", 2), ev_at("u", 3)];
        let mut stream = VecEntityEventStream::new(EntityId::from("u"), events).unwrap();
        assert_eq!(stream.entity_id(), &EntityId::from("u"));
        assert_eq!(stream.next_event().unwrap().timestamp_nanos(), 1);
        assert_eq!(stream.next_event().unwrap().timestamp_nanos(), 2);
        assert_eq!(stream.next_event().unwrap().timestamp_nanos(), 3);
        assert!(stream.next_event().is_none());
    }

    #[test]
    fn vec_stream_allows_equal_timestamps() {
        let events = vec![ev_at("u", 5), ev_at("u", 5)];
        assert!(VecEntityEventStream::new(EntityId::from("u"), events).is_ok());
    }

    #[test]
    fn vec_stream_rejects_mismatched_entity() {
        let events = vec![ev_at("u", 1), ev_at("v", 2)];
        let err = VecEntityEventStream::new(EntityId::from("u"), events).unwrap_err();
        match err {
            EntityEventStreamError::MismatchedEntity {
                index,
                expected,
                found,
            } => {
                assert_eq!(index, 1);
                assert_eq!(expected, EntityId::from("u"));
                assert_eq!(found, EntityId::from("v"));
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn vec_stream_rejects_out_of_order_timestamps() {
        let events = vec![ev_at("u", 10), ev_at("u", 5)];
        let err = VecEntityEventStream::new(EntityId::from("u"), events).unwrap_err();
        match err {
            EntityEventStreamError::OutOfOrder {
                index,
                previous,
                current,
            } => {
                assert_eq!(index, 1);
                assert_eq!(previous, Timestamp::from_nanos(10));
                assert_eq!(current, Timestamp::from_nanos(5));
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn vec_stream_empty_input_is_ok_and_exhausted() {
        let mut stream = VecEntityEventStream::new(EntityId::from("u"), Vec::new()).unwrap();
        assert!(stream.next_event().is_none());
    }

    #[test]
    fn entity_event_stream_error_display_mismatch() {
        let err = EntityEventStreamError::MismatchedEntity {
            index: 3,
            expected: EntityId::from("u"),
            found: EntityId::from(9_i64),
        };
        assert_eq!(
            err.to_string(),
            "event at index 3 belongs to entity '9', expected 'u'"
        );
    }

    #[test]
    fn entity_event_stream_error_display_out_of_order() {
        let err = EntityEventStreamError::OutOfOrder {
            index: 2,
            previous: Timestamp::from_nanos(100),
            current: Timestamp::from_nanos(50),
        };
        assert_eq!(
            err.to_string(),
            "event at index 2 out of order: current 50ns < previous 100ns"
        );
    }

    #[test]
    fn entity_event_stream_error_is_std_error() {
        // Confirms the thiserror derive yields `std::error::Error`.
        fn assert_error<E: std::error::Error>(_: &E) {}
        let err = EntityEventStreamError::OutOfOrder {
            index: 0,
            previous: Timestamp::from_nanos(1),
            current: Timestamp::from_nanos(0),
        };
        assert_error(&err);
    }
}
