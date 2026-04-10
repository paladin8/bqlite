//! Timestamp and time-range types for bqlite.
//!
//! All timestamps are UTC nanoseconds since the Unix epoch, stored as `i64`.
//! This matches the Arrow physical type `Timestamp(Nanosecond, Some("UTC"))`.
//!
//! Durations are plain `i64` nanoseconds — there is no separate `Duration` type
//! (see docs/design/type-system.md §2.2 for rationale). Duration literals
//! produced by the parser are converted to nanosecond i64 values at plan time.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Nanosecond-precision UTC timestamp.
///
/// The inner `i64` is the number of nanoseconds since the Unix epoch (1970-01-01T00:00:00 UTC).
/// Negative values represent instants before the epoch.
///
/// Arrow mapping: `Timestamp(Nanosecond, Some("UTC"))`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Timestamp(pub i64);

impl Timestamp {
    /// Unix epoch: 1970-01-01T00:00:00 UTC.
    pub const EPOCH: Self = Self(0);

    /// The minimum representable timestamp (most negative nanosecond value).
    pub const MIN: Self = Self(i64::MIN);

    /// The maximum representable timestamp (most positive nanosecond value).
    pub const MAX: Self = Self(i64::MAX);

    /// Create a `Timestamp` from raw nanoseconds since the Unix epoch.
    #[inline]
    pub fn from_nanos(nanos: i64) -> Self {
        Self(nanos)
    }

    /// Return the raw nanoseconds since the Unix epoch.
    #[inline]
    pub fn as_nanos(self) -> i64 {
        self.0
    }

    /// Add a duration (nanoseconds) to this timestamp, returning `None` on overflow.
    #[inline]
    pub fn checked_add_nanos(self, nanos: i64) -> Option<Self> {
        self.0.checked_add(nanos).map(Self)
    }

    /// Subtract a duration (nanoseconds) from this timestamp, returning `None` on overflow.
    #[inline]
    pub fn checked_sub_nanos(self, nanos: i64) -> Option<Self> {
        self.0.checked_sub(nanos).map(Self)
    }

    /// Compute the signed duration between `self` and `other` in nanoseconds
    /// (`self - other`), returning `None` on overflow.
    #[inline]
    pub fn duration_nanos(self, other: Self) -> Option<i64> {
        self.0.checked_sub(other.0)
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Timestamp({}ns)", self.0)
    }
}

impl From<i64> for Timestamp {
    #[inline]
    fn from(nanos: i64) -> Self {
        Self(nanos)
    }
}

impl From<Timestamp> for i64 {
    #[inline]
    fn from(ts: Timestamp) -> Self {
        ts.0
    }
}

/// A half-open time interval `[start, end)`.
///
/// The start bound is **inclusive**; the end bound is **exclusive**.
/// An empty range has `start >= end`.
///
/// Durations are expressed as `i64` nanoseconds — no separate `Duration` type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TimeRange {
    /// Inclusive lower bound.
    pub start: Timestamp,
    /// Exclusive upper bound.
    pub end: Timestamp,
}

impl TimeRange {
    /// Create a new `[start, end)` range.
    ///
    /// The range is valid even if `start >= end`; in that case it is considered empty.
    #[inline]
    pub fn new(start: Timestamp, end: Timestamp) -> Self {
        Self { start, end }
    }

    /// Create a range spanning a single nanosecond: `[ts, ts+1)`.
    #[inline]
    pub fn instant(ts: Timestamp) -> Self {
        Self {
            start: ts,
            end: Timestamp(ts.0.saturating_add(1)),
        }
    }

    /// Create an unbounded range that contains every possible timestamp.
    #[inline]
    pub fn unbounded() -> Self {
        Self {
            start: Timestamp::MIN,
            end: Timestamp::MAX,
        }
    }

    /// Returns `true` if this range contains no timestamps (`start >= end`).
    #[inline]
    pub fn is_empty(self) -> bool {
        self.start >= self.end
    }

    /// Returns the duration of this range in nanoseconds, or `None` on overflow.
    ///
    /// Returns `None` (rather than 0) for empty ranges so callers must handle
    /// the empty case explicitly.
    #[inline]
    pub fn duration_nanos(self) -> Option<i64> {
        if self.is_empty() {
            None
        } else {
            self.end.duration_nanos(self.start)
        }
    }

    /// Returns `true` if `ts` falls within `[start, end)`.
    #[inline]
    pub fn contains(self, ts: Timestamp) -> bool {
        ts >= self.start && ts < self.end
    }

    /// Returns `true` if `other` overlaps with `self` (both non-empty and share at least one nanosecond).
    #[inline]
    pub fn overlaps(self, other: Self) -> bool {
        !self.is_empty() && !other.is_empty() && self.start < other.end && other.start < self.end
    }

    /// Returns the intersection of `self` and `other`, or `None` if they do not overlap.
    #[inline]
    pub fn intersect(self, other: Self) -> Option<Self> {
        let start = self.start.max(other.start);
        let end = self.end.min(other.end);
        if start < end {
            Some(Self { start, end })
        } else {
            None
        }
    }

    /// Shift this range forward by `nanos` nanoseconds, returning `None` on overflow.
    #[inline]
    pub fn shift(self, nanos: i64) -> Option<Self> {
        Some(Self {
            start: self.start.checked_add_nanos(nanos)?,
            end: self.end.checked_add_nanos(nanos)?,
        })
    }
}

impl fmt::Display for TimeRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}, {})", self.start, self.end)
    }
}

/// Well-known nanosecond duration constants.
pub mod duration {
    pub const NANOSECOND: i64 = 1;
    pub const MICROSECOND: i64 = 1_000;
    pub const MILLISECOND: i64 = 1_000_000;
    pub const SECOND: i64 = 1_000_000_000;
    pub const MINUTE: i64 = 60 * SECOND;
    pub const HOUR: i64 = 60 * MINUTE;
    pub const DAY: i64 = 24 * HOUR;
    pub const WEEK: i64 = 7 * DAY;
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Timestamp tests ---

    #[test]
    fn timestamp_ordering() {
        assert!(Timestamp(0) < Timestamp(1));
        assert!(Timestamp(-1) < Timestamp(0));
        assert_eq!(Timestamp(42), Timestamp(42));
    }

    #[test]
    fn timestamp_checked_add_nanos() {
        let t = Timestamp(1_000);
        assert_eq!(t.checked_add_nanos(500), Some(Timestamp(1_500)));
        assert_eq!(Timestamp::MAX.checked_add_nanos(1), None);
    }

    #[test]
    fn timestamp_checked_sub_nanos() {
        let t = Timestamp(1_000);
        assert_eq!(t.checked_sub_nanos(500), Some(Timestamp(500)));
        assert_eq!(Timestamp::MIN.checked_sub_nanos(1), None);
    }

    #[test]
    fn timestamp_duration_nanos() {
        let a = Timestamp(1_000);
        let b = Timestamp(400);
        assert_eq!(a.duration_nanos(b), Some(600));
        assert_eq!(Timestamp::MAX.duration_nanos(Timestamp::MIN), None);
    }

    #[test]
    fn timestamp_from_into() {
        let t: Timestamp = 42_i64.into();
        assert_eq!(t.as_nanos(), 42);
        let n: i64 = t.into();
        assert_eq!(n, 42);
    }

    #[test]
    fn timestamp_display() {
        assert_eq!(Timestamp(0).to_string(), "Timestamp(0ns)");
        assert_eq!(Timestamp(-1).to_string(), "Timestamp(-1ns)");
    }

    // --- TimeRange tests ---

    #[test]
    fn range_contains() {
        let r = TimeRange::new(Timestamp(10), Timestamp(20));
        assert!(r.contains(Timestamp(10)));
        assert!(r.contains(Timestamp(15)));
        assert!(!r.contains(Timestamp(20))); // exclusive end
        assert!(!r.contains(Timestamp(9)));
    }

    #[test]
    fn range_empty() {
        assert!(TimeRange::new(Timestamp(5), Timestamp(5)).is_empty());
        assert!(TimeRange::new(Timestamp(6), Timestamp(5)).is_empty());
        assert!(!TimeRange::new(Timestamp(5), Timestamp(6)).is_empty());
    }

    #[test]
    fn range_duration() {
        let r = TimeRange::new(Timestamp(0), Timestamp(100));
        assert_eq!(r.duration_nanos(), Some(100));
        assert_eq!(TimeRange::new(Timestamp(5), Timestamp(5)).duration_nanos(), None);
    }

    #[test]
    fn range_overlaps() {
        let a = TimeRange::new(Timestamp(0), Timestamp(10));
        let b = TimeRange::new(Timestamp(5), Timestamp(15));
        let c = TimeRange::new(Timestamp(10), Timestamp(20)); // touches but does not overlap (exclusive end)
        assert!(a.overlaps(b));
        assert!(!a.overlaps(c));
        // empty range overlaps nothing
        let empty = TimeRange::new(Timestamp(5), Timestamp(5));
        assert!(!a.overlaps(empty));
    }

    #[test]
    fn range_intersect() {
        let a = TimeRange::new(Timestamp(0), Timestamp(10));
        let b = TimeRange::new(Timestamp(5), Timestamp(15));
        assert_eq!(
            a.intersect(b),
            Some(TimeRange::new(Timestamp(5), Timestamp(10)))
        );
        let c = TimeRange::new(Timestamp(10), Timestamp(20));
        assert_eq!(a.intersect(c), None);
    }

    #[test]
    fn range_shift() {
        let r = TimeRange::new(Timestamp(10), Timestamp(20));
        assert_eq!(
            r.shift(5),
            Some(TimeRange::new(Timestamp(15), Timestamp(25)))
        );
        assert_eq!(r.shift(-5), Some(TimeRange::new(Timestamp(5), Timestamp(15))));
    }

    #[test]
    fn range_instant() {
        let ts = Timestamp(42);
        let r = TimeRange::instant(ts);
        assert!(r.contains(Timestamp(42)));
        assert!(!r.contains(Timestamp(43)));
        assert_eq!(r.duration_nanos(), Some(1));
    }

    #[test]
    fn duration_constants_consistent() {
        use super::duration::*;
        assert_eq!(MICROSECOND, 1_000 * NANOSECOND);
        assert_eq!(MILLISECOND, 1_000 * MICROSECOND);
        assert_eq!(SECOND, 1_000 * MILLISECOND);
        assert_eq!(MINUTE, 60 * SECOND);
        assert_eq!(HOUR, 60 * MINUTE);
        assert_eq!(DAY, 24 * HOUR);
        assert_eq!(WEEK, 7 * DAY);
    }

    #[test]
    fn serde_round_trip() {
        let ts = Timestamp(1_700_000_000_000_000_000_i64);
        let json = serde_json::to_string(&ts).unwrap();
        let back: Timestamp = serde_json::from_str(&json).unwrap();
        assert_eq!(ts, back);

        let r = TimeRange::new(Timestamp(100), Timestamp(200));
        let json = serde_json::to_string(&r).unwrap();
        let back: TimeRange = serde_json::from_str(&json).unwrap();
        assert_eq!(r, back);
    }
}
