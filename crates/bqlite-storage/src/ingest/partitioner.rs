//! Ingest partitioner — routes events into `(window_id, shard_id)`
//! buckets, buffers them in memory, and hands each bucket back as a
//! sorted `(entity_id, timestamp)` stream for the writer.
//!
//! Per `docs/design/storage-format.md`:
//!
//! - **Sharding (§5.1).** `shard_id = xxhash64(entity_id) % shard_count`.
//!   Shard count is fixed at database init and shared across every
//!   table — the partitioner reads it from its constructor rather
//!   than re-deriving it so cross-table entity alignment is trivial.
//! - **Windowing (§4.1–§4.2).** Windows are aligned to UTC day
//!   boundaries and span a fixed number of days (default 30). A
//!   window's id is the days-since-epoch value of its start day —
//!   `0` is the 30-day window covering `1970-01-01..1970-01-31`.
//! - **Row-group ordering (§7.2).** Each `(window, shard)` bucket is
//!   sorted by `(entity_id, timestamp)` before the writer sees it,
//!   so every row group the writer emits is entity-contiguous.
//!
//! # Module shape
//!
//! - Pure helpers — [`shard_id_for`] and [`window_id_for`] — plus
//!   the sharding / windowing invariants they enforce.
//! - The stateful [`Partitioner`] that buffers events, tracks a
//!   memory budget, sorts each `(window, shard)` bucket on drain,
//!   and carries a fresh `batch_id` that the caller obtained from
//!   the manifest counter via
//!   [`crate::database::Database::allocate_batch_id`].
//!
//! Downstream tasks (TASK-214 writer orchestration, TASK-233 CSV
//! ingest) import a single `crate::ingest::partitioner::Partitioner`.

use std::collections::BTreeMap;

use bqlite_core::error::{BqliteError, Result};
use bqlite_core::event::{EntityId, Event};
use bqlite_core::property::PropertyValue;
use bqlite_core::time::Timestamp;
use twox_hash::XxHash64;

/// Nanoseconds per UTC day. Used by [`window_id_for`] to convert a
/// raw nanosecond timestamp into a day index before quantizing it
/// to the window boundary.
const NS_PER_DAY: i64 = 86_400 * 1_000_000_000;

/// Fixed seed for the entity-id shard hash.
///
/// Locked to `0` for the lifetime of the v1 storage format so that
/// a given entity always lands on the same shard across every open
/// of the same database. Changing this constant is a cross-version
/// migration — callers must never pass a different seed through a
/// side channel.
const SHARD_HASH_SEED: u64 = 0;

/// Compute the shard id for an entity.
///
/// `shard_id = xxhash64(entity_id_bytes, seed=0) % shard_count`
///
/// The hash input is the entity id's canonical byte representation:
/// UTF-8 bytes for a string id, or little-endian `i64` bytes for an
/// integer id. Strings and integers therefore land in disjoint
/// hash spaces — the same string `"42"` and integer `42` are
/// deliberately allowed to land on different shards without
/// colliding, because the `TableSchema` constraint in
/// `type-system.md` §5.1 fixes the entity-key column type on a
/// per-table basis so cross-variant collisions cannot happen in
/// practice.
///
/// # Panics
///
/// Panics if `shard_count == 0`. The caller is expected to hold
/// the same `shard_count` invariant the manifest enforces at init
/// (`shard_count >= 1`, storage-format.md §5.1), so an upstream
/// bug is the only path to this panic.
#[inline]
pub fn shard_id_for(entity: &EntityId, shard_count: u16) -> u16 {
    assert!(
        shard_count > 0,
        "shard_id_for: shard_count must be >= 1 (enforced by the manifest at init)"
    );
    let hash = match entity {
        EntityId::String(s) => XxHash64::oneshot(SHARD_HASH_SEED, s.as_bytes()),
        EntityId::Int(n) => XxHash64::oneshot(SHARD_HASH_SEED, &n.to_le_bytes()),
    };
    (hash % u64::from(shard_count)) as u16
}

/// Compute the window id for an event timestamp.
///
/// `window_id` is the days-since-epoch value of the window's start
/// day. Windows span `window_days` days each and are aligned to
/// UTC day zero (1970-01-01). For the default 30-day window the
/// window covering `2025-03-01` (day 20148) has `window_id = 20130`
/// because `20148 / 30 = 671` and `671 * 30 = 20130`.
///
/// # Errors
///
/// - [`BqliteError::Execution`] if `ts` is strictly before the Unix
///   epoch (pre-1970 events). Window ids are defined as unsigned in
///   storage-format.md §4.2 and §5.2; a negative day index has no
///   home in the on-disk layout, and the task spec explicitly
///   defers pre-epoch support. Real event workloads are post-2020.
/// - [`BqliteError::Schema`] if `window_days == 0`. A zero-day
///   window has no meaningful bucketing and is almost certainly a
///   configuration bug.
pub fn window_id_for(ts: Timestamp, window_days: u32) -> Result<u32> {
    if window_days == 0 {
        return Err(BqliteError::Schema(
            "partitioner: window_days must be at least 1".into(),
        ));
    }
    let nanos = ts.as_nanos();
    if nanos < 0 {
        return Err(BqliteError::Execution(format!(
            "partitioner: pre-epoch timestamp {nanos} ns is not supported \
             (window ids in storage-format.md §4.2 start at day 0 = 1970-01-01 UTC)"
        )));
    }
    // `nanos >= 0`, and `i64::MAX / NS_PER_DAY` is ~1.07e8 days —
    // well inside `u32::MAX`, so the cast is total for any
    // representable non-negative ns value.
    let day_idx = nanos / NS_PER_DAY;
    debug_assert!(
        day_idx <= i64::from(u32::MAX),
        "day_idx unexpectedly exceeds u32::MAX; i64::MAX / NS_PER_DAY should keep us well below"
    );
    let day_idx = day_idx as u32;
    let remainder = day_idx % window_days;
    Ok(day_idx - remainder)
}

/// Composite key used inside [`Partitioner`] to bucket events.
///
/// `(window_id, shard_id)` is the natural primary key for the
/// per-window, per-shard directory layout in
/// `docs/design/storage-format.md` §5.2. [`BTreeMap`] sorts its
/// keys lexicographically — ascending by `window_id` first, then
/// by `shard_id` — which matches the order the writer prefers to
/// visit buckets (oldest window first, shard 0 first).
pub type BucketKey = (u32, u16);

/// Ingest partitioner — per-ingest-call buffer that routes events
/// into `(window, shard)` buckets and hands each bucket off as a
/// sorted stream when the caller drains it.
///
/// # Lifecycle
///
/// 1. The ingest driver calls
///    [`crate::database::Database::allocate_batch_id`] to atomically
///    reserve a fresh `batch_id` from the target table's manifest
///    counter.
/// 2. The driver constructs a [`Partitioner`] with that `batch_id`,
///    the database's `shard_count`, the table's `window_days`
///    (defaulted to 30 until per-table config lands), and a
///    memory-budget ceiling.
/// 3. The driver streams events in via
///    [`Partitioner::push_event`]. Each push hashes the entity id
///    to a shard, derives the window id from the timestamp,
///    estimates the event's memory cost, and either appends it to
///    the matching bucket or loudly errors if the buffer would
///    exceed its budget.
/// 4. Once every input row has been pushed, the driver calls
///    [`Partitioner::drain_sorted`]. That consumes the partitioner,
///    sorts each bucket by `(entity_id, timestamp)` (a stable
///    sort — ties retain insertion order), and yields an iterator
///    of `(BucketKey, Vec<Event>)` pairs in ascending `(window_id,
///    shard_id)` order. The writer (TASK-214) consumes those
///    streams one bucket at a time.
///
/// # Wave 2 memory model
///
/// The buffer is tracked as a simple running sum of per-event
/// heap-size estimates (see [`estimated_event_size`]). When a push
/// would cross the configured budget, the partitioner refuses the
/// event with [`BqliteError::Execution`] — the task spec calls this
/// the "error loudly" stub. Real on-disk spill is a Wave 5 concern
/// (TASKS.md TASK-218). The estimate is deliberately cheap and
/// monotonic: it will under-count some exotic `PropertyValue`
/// shapes, but it is stable under equal inputs and never returns
/// zero for a non-empty event, which is all the Wave 2 gate needs.
///
/// # Why `BTreeMap`, not `HashMap`
///
/// Deterministic iteration order on drain keeps snapshot-style
/// tests stable and lets the writer emit segments in time order
/// without a separate sort pass. Wave 2 buckets fit comfortably in
/// an ordered map at any workload the partitioner sees — a handful
/// of windows times a handful of shards times a few hundred
/// thousand events. O(log n) per insert is dwarfed by the
/// per-event allocation cost.
#[derive(Debug)]
pub struct Partitioner {
    shard_count: u16,
    window_days: u32,
    batch_id: u64,
    budget_bytes: usize,
    buffered_bytes: usize,
    buckets: BTreeMap<BucketKey, Vec<Event>>,
}

impl Partitioner {
    /// Construct a fresh partitioner for one ingest call.
    ///
    /// `batch_id` is the fresh counter value returned by
    /// [`crate::database::Database::allocate_batch_id`]; the
    /// partitioner holds it verbatim and the writer reads it back
    /// through [`Partitioner::batch_id`] to stamp the produced
    /// segments.
    ///
    /// # Errors
    ///
    /// - [`BqliteError::Schema`] if `shard_count == 0` — the
    ///   manifest invariant that forbids this is enforced at
    ///   database init, but the constructor double-checks so a
    ///   partitioner cannot land in a state where `shard_id_for`
    ///   would panic.
    /// - [`BqliteError::Schema`] if `window_days == 0` — same
    ///   reasoning for [`window_id_for`].
    /// - [`BqliteError::Schema`] if `budget_bytes == 0` — a zero
    ///   budget cannot hold even one event, which is almost always
    ///   a misconfiguration.
    pub fn new(
        shard_count: u16,
        window_days: u32,
        batch_id: u64,
        budget_bytes: usize,
    ) -> Result<Self> {
        if shard_count == 0 {
            return Err(BqliteError::Schema(
                "partitioner: shard_count must be at least 1".into(),
            ));
        }
        if window_days == 0 {
            return Err(BqliteError::Schema(
                "partitioner: window_days must be at least 1".into(),
            ));
        }
        if budget_bytes == 0 {
            return Err(BqliteError::Schema(
                "partitioner: budget_bytes must be greater than 0".into(),
            ));
        }
        Ok(Self {
            shard_count,
            window_days,
            batch_id,
            budget_bytes,
            buffered_bytes: 0,
            buckets: BTreeMap::new(),
        })
    }

    /// The `batch_id` this partitioner is stamping onto its events.
    #[inline]
    pub fn batch_id(&self) -> u64 {
        self.batch_id
    }

    /// Configured shard count (unchanged for the partitioner's
    /// lifetime).
    #[inline]
    pub fn shard_count(&self) -> u16 {
        self.shard_count
    }

    /// Configured window-days span (unchanged for the partitioner's
    /// lifetime).
    #[inline]
    pub fn window_days(&self) -> u32 {
        self.window_days
    }

    /// Configured memory-budget ceiling in bytes.
    #[inline]
    pub fn budget_bytes(&self) -> usize {
        self.budget_bytes
    }

    /// Current buffered-bytes estimate. Monotonically
    /// non-decreasing — [`Partitioner::drain_sorted`] consumes the
    /// partitioner outright, so there is no observable reset mid
    /// lifetime.
    #[inline]
    pub fn buffered_bytes(&self) -> usize {
        self.buffered_bytes
    }

    /// Total number of events buffered across every bucket.
    pub fn buffered_events(&self) -> usize {
        self.buckets.values().map(Vec::len).sum()
    }

    /// Number of non-empty `(window, shard)` buckets currently held.
    pub fn bucket_count(&self) -> usize {
        self.buckets.len()
    }

    /// Route `event` into its `(window, shard)` bucket.
    ///
    /// The partitioner hashes `event.entity` into a shard and
    /// derives a window id from `event.timestamp`, estimates the
    /// event's in-memory footprint, and refuses the push if adding
    /// it would take the running buffer past [`Self::budget_bytes`].
    ///
    /// # Errors
    ///
    /// - [`BqliteError::Execution`] with a `"pre-epoch"` reason if
    ///   the timestamp is before 1970-01-01 UTC (via
    ///   [`window_id_for`]).
    /// - [`BqliteError::Execution`] with a `"memory budget"`
    ///   reason if the push would cross the configured budget.
    ///   Wave 2 refuses the event outright — spill-to-disk is a
    ///   Wave 5 concern — and leaves every previously-pushed event
    ///   untouched, so the caller can abort the ingest and retry
    ///   with a larger budget.
    pub fn push_event(&mut self, event: Event) -> Result<()> {
        let shard_id = shard_id_for(&event.entity, self.shard_count);
        let window_id = window_id_for(event.timestamp, self.window_days)?;

        let size = estimated_event_size(&event);
        // Refuse the push if adding this event would take the
        // running buffer past the ceiling. Pre-check against
        // `saturating_add` so a pathologically large estimate does
        // not wrap into a small number.
        let projected = self.buffered_bytes.saturating_add(size);
        if projected > self.budget_bytes {
            return Err(BqliteError::Execution(format!(
                "partitioner: memory budget would be exceeded — adding a {size}-byte event \
                 would take buffered_bytes from {} to {projected}, above the {} ceiling \
                 (TASK-218 Wave 2 refuses overflow; on-disk spill lands in a later wave)",
                self.buffered_bytes, self.budget_bytes
            )));
        }

        self.buckets
            .entry((window_id, shard_id))
            .or_default()
            .push(event);
        self.buffered_bytes = projected;
        Ok(())
    }

    /// Consume the partitioner and yield each `(BucketKey,
    /// sorted events)` pair in ascending `(window_id, shard_id)`
    /// order.
    ///
    /// Each bucket is sorted in place by `(entity_id, timestamp)`
    /// using a stable sort, so two events with identical sort keys
    /// retain their insertion order — the property downstream
    /// operators rely on when, for example, two rows of the same
    /// entity share a nanosecond timestamp.
    ///
    /// The returned iterator is just a transformed `BTreeMap`
    /// iterator, so the drain is `O(total_events)` for the sort
    /// pass plus `O(buckets)` for the ordered walk.
    pub fn drain_sorted(self) -> impl Iterator<Item = (BucketKey, Vec<Event>)> {
        let mut buckets = self.buckets;
        for events in buckets.values_mut() {
            events.sort_by(|a, b| {
                a.entity
                    .cmp(&b.entity)
                    .then_with(|| a.timestamp.cmp(&b.timestamp))
            });
        }
        buckets.into_iter()
    }
}

/// Cheap best-effort estimate of an event's in-memory footprint,
/// in bytes.
///
/// This is a Wave 2 heuristic, not an exact measurement. It counts
/// the struct itself plus the heap allocations the fields own
/// (string capacities, property-bag entries, nested
/// [`PropertyValue`]s). The goal is monotonicity — bigger events
/// must estimate bigger — not numerical accuracy. When Wave 5
/// adds real spill-to-disk, this function is the natural place to
/// plug in a tighter measurement (or to reach for `mem::size_of_val`
/// on stable Rust).
fn estimated_event_size(event: &Event) -> usize {
    let mut size = std::mem::size_of::<Event>();
    match &event.entity {
        EntityId::String(s) => size += s.capacity(),
        EntityId::Int(_) => {}
    }
    size += event.event_type.capacity();
    for (key, value) in &event.properties {
        size += std::mem::size_of::<(String, PropertyValue)>();
        size += key.capacity();
        size += estimated_property_size(value);
    }
    size
}

/// Best-effort estimate of a [`PropertyValue`]'s heap footprint.
///
/// Scalar variants are constant-sized; String adds its capacity;
/// List / Map recurse into their elements.
fn estimated_property_size(value: &PropertyValue) -> usize {
    match value {
        PropertyValue::Null
        | PropertyValue::Bool(_)
        | PropertyValue::Int(_)
        | PropertyValue::Float(_)
        | PropertyValue::Timestamp(_) => 0,
        PropertyValue::String(s) => s.capacity(),
        PropertyValue::List(items) => items
            .iter()
            .map(|v| std::mem::size_of::<PropertyValue>() + estimated_property_size(v))
            .sum(),
        PropertyValue::Map(pairs) => pairs
            .iter()
            .map(|(k, v)| {
                std::mem::size_of::<(String, PropertyValue)>()
                    + k.capacity()
                    + estimated_property_size(v)
            })
            .sum(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── shard_id_for ────────────────────────────────────────────────────────

    #[test]
    fn shard_id_is_deterministic_for_string_entity() {
        let entity = EntityId::from("user_42");
        let a = shard_id_for(&entity, 32);
        let b = shard_id_for(&entity, 32);
        let c = shard_id_for(&entity, 32);
        assert_eq!(a, b);
        assert_eq!(b, c);
    }

    #[test]
    fn shard_id_is_deterministic_for_int_entity() {
        let entity = EntityId::from(42_i64);
        let a = shard_id_for(&entity, 16);
        let b = shard_id_for(&entity, 16);
        assert_eq!(a, b);
    }

    #[test]
    fn shard_id_stays_in_range() {
        for n in 0..1_000 {
            let entity = EntityId::from(format!("user_{n}"));
            let shard = shard_id_for(&entity, 32);
            assert!(shard < 32, "shard {shard} out of range for shard_count=32");
        }
    }

    #[test]
    fn shard_id_with_shard_count_one_collapses_to_zero() {
        // `hash % 1` is always 0. Useful as a test-harness escape
        // hatch: running the partitioner with `shard_count = 1`
        // routes every event to the same shard regardless of
        // entity.
        for n in 0..100 {
            let entity = EntityId::from(n as i64);
            assert_eq!(shard_id_for(&entity, 1), 0);
        }
    }

    #[test]
    fn shard_id_distribution_is_non_trivial_across_shards() {
        // A weak sanity check that the hash isn't collapsing every
        // entity onto a single shard. We do not assert a specific
        // distribution because xxhash's exact output is a stable
        // contract, not a design goal; asserting "at least two
        // shards see traffic over 1024 distinct ids" is enough to
        // catch a bug where the hash degenerates.
        use std::collections::HashSet;
        let mut shards = HashSet::new();
        for n in 0..1024 {
            shards.insert(shard_id_for(&EntityId::from(n as i64), 32));
        }
        assert!(
            shards.len() >= 2,
            "1024 distinct integer ids collapsed onto a single shard — hash is wrong"
        );
    }

    #[test]
    fn shard_id_string_and_int_with_same_textual_form_are_independent_inputs() {
        // `"42"` and `42_i64` have different canonical byte
        // representations; the hash must see them as independent
        // inputs even if downstream logic happens to line them up
        // by coincidence. We only assert that both calls are
        // well-defined and in-range — a collision between these
        // two specific inputs is not impossible with any stable
        // hash, so we do not assert they produce different shards.
        let s = shard_id_for(&EntityId::from("42"), 32);
        let i = shard_id_for(&EntityId::from(42_i64), 32);
        assert!(s < 32);
        assert!(i < 32);
    }

    #[test]
    #[should_panic(expected = "shard_count must be >= 1")]
    fn shard_id_with_zero_shard_count_panics() {
        shard_id_for(&EntityId::from("x"), 0);
    }

    // ── window_id_for ──────────────────────────────────────────────────────

    /// `days_ns` returns the nanosecond timestamp at the start of
    /// day `d` (days since 1970-01-01 UTC).
    fn day(d: i64) -> Timestamp {
        Timestamp(d * NS_PER_DAY)
    }

    #[test]
    fn window_id_at_epoch_is_zero() {
        assert_eq!(window_id_for(Timestamp(0), 30).unwrap(), 0);
    }

    #[test]
    fn window_id_within_first_window_is_zero() {
        // Every timestamp in `[1970-01-01, 1970-01-31)` rounds to 0.
        assert_eq!(window_id_for(day(0), 30).unwrap(), 0);
        assert_eq!(window_id_for(day(1), 30).unwrap(), 0);
        assert_eq!(window_id_for(day(15), 30).unwrap(), 0);
        assert_eq!(window_id_for(day(29), 30).unwrap(), 0);
        // A nanosecond before the next window boundary is still window 0.
        assert_eq!(
            window_id_for(Timestamp(day(30).as_nanos() - 1), 30).unwrap(),
            0
        );
    }

    #[test]
    fn window_id_rolls_over_at_window_boundary() {
        // Day 30 is the first instant of the second 30-day window.
        assert_eq!(window_id_for(day(30), 30).unwrap(), 30);
        assert_eq!(window_id_for(day(59), 30).unwrap(), 30);
        assert_eq!(window_id_for(day(60), 30).unwrap(), 60);
    }

    #[test]
    fn window_id_respects_custom_window_days() {
        // 7-day windows: days 0..7 → window 0, 7..14 → window 7.
        assert_eq!(window_id_for(day(0), 7).unwrap(), 0);
        assert_eq!(window_id_for(day(6), 7).unwrap(), 0);
        assert_eq!(window_id_for(day(7), 7).unwrap(), 7);
        assert_eq!(window_id_for(day(13), 7).unwrap(), 7);
        assert_eq!(window_id_for(day(14), 7).unwrap(), 14);

        // 1-day windows trivially collapse onto the day index.
        for d in 0..20 {
            assert_eq!(window_id_for(day(d), 1).unwrap(), d as u32);
        }
    }

    #[test]
    fn window_id_for_march_first_2025_respects_30_day_alignment() {
        // storage-format.md §4.2 calls out day 20148 (2025-03-01) as
        // the example day under discussion. With 30-day windows
        // aligned to day 0 of the epoch, 20148 lands in the window
        // starting at day 20130 (= 671 * 30, roughly 2025-02-11)
        // because `20148 % 30 == 18 != 0`. This test pins that math
        // so any future drift in the floor-division rule is caught
        // loudly.
        let ts = day(20_148);
        assert_eq!(window_id_for(ts, 30).unwrap(), 20_130);
    }

    #[test]
    fn window_id_varies_with_shard_count_argument() {
        // Guard against a regression where `shard_count` gets
        // hard-coded or precomputed. The function under test must
        // apply the modulo per-call against the caller-supplied
        // value.
        let entity = EntityId::from("user_stable");
        let a = shard_id_for(&entity, 4);
        let b = shard_id_for(&entity, 8);
        let c = shard_id_for(&entity, 32);
        assert!(a < 4);
        assert!(b < 8);
        assert!(c < 32);
        // `a` must equal the same xxhash64 modulo 4, and likewise
        // for `b` and `c`. Compute the underlying hash once here
        // and cross-check the three mods.
        let raw = twox_hash::XxHash64::oneshot(SHARD_HASH_SEED, "user_stable".as_bytes());
        assert_eq!(a as u64, raw % 4);
        assert_eq!(b as u64, raw % 8);
        assert_eq!(c as u64, raw % 32);
    }

    #[test]
    fn window_id_rejects_pre_epoch_timestamp() {
        let err = window_id_for(Timestamp(-1), 30).expect_err("pre-epoch must error");
        match err {
            BqliteError::Execution(msg) => {
                assert!(msg.contains("pre-epoch"), "got: {msg}");
                assert!(msg.contains("-1"), "got: {msg}");
            }
            other => panic!("expected Execution, got {other:?}"),
        }

        // Any sub-nanosecond-before-epoch value triggers the same error.
        assert!(matches!(
            window_id_for(Timestamp::MIN, 30),
            Err(BqliteError::Execution(_))
        ));
    }

    #[test]
    fn window_id_rejects_zero_window_days() {
        let err = window_id_for(Timestamp(0), 0).expect_err("zero window_days must error");
        assert!(matches!(err, BqliteError::Schema(_)));
    }

    #[test]
    fn window_id_handles_large_positive_timestamp_without_overflow() {
        // The largest *valid* event timestamp is `Timestamp::MAX_VALID`
        // (i64::MAX - 1, ~year 2262). Confirm it still maps to a
        // well-defined window without overflowing.
        let ts = Timestamp::MAX_VALID;
        let got = window_id_for(ts, 30).unwrap();
        let expected_day = (ts.as_nanos() / NS_PER_DAY) as u32;
        let expected_window = expected_day - (expected_day % 30);
        assert_eq!(got, expected_window);
    }

    // ── Partitioner ────────────────────────────────────────────────────────

    fn simple_event(entity: &str, ts_day: i64, event_type: &str) -> Event {
        Event::new(EntityId::from(entity), day(ts_day), event_type)
    }

    #[test]
    fn partitioner_new_rejects_zero_shard_count() {
        let err = Partitioner::new(0, 30, 1, 1024).expect_err("zero shard_count must error");
        assert!(matches!(err, BqliteError::Schema(_)));
    }

    #[test]
    fn partitioner_new_rejects_zero_window_days() {
        let err = Partitioner::new(4, 0, 1, 1024).expect_err("zero window_days must error");
        assert!(matches!(err, BqliteError::Schema(_)));
    }

    #[test]
    fn partitioner_new_rejects_zero_budget() {
        let err = Partitioner::new(4, 30, 1, 0).expect_err("zero budget must error");
        assert!(matches!(err, BqliteError::Schema(_)));
    }

    #[test]
    fn partitioner_new_returns_configured_values() {
        let p = Partitioner::new(8, 7, 42, 1024).unwrap();
        assert_eq!(p.shard_count(), 8);
        assert_eq!(p.window_days(), 7);
        assert_eq!(p.batch_id(), 42);
        assert_eq!(p.budget_bytes(), 1024);
        assert_eq!(p.buffered_bytes(), 0);
        assert_eq!(p.buffered_events(), 0);
        assert_eq!(p.bucket_count(), 0);
    }

    #[test]
    fn push_event_fills_bucket_and_tracks_buffer_stats() {
        let mut p = Partitioner::new(4, 30, 1, 1 << 20).unwrap();
        p.push_event(simple_event("alice", 0, "click")).unwrap();
        p.push_event(simple_event("alice", 1, "view")).unwrap();
        p.push_event(simple_event("bob", 2, "click")).unwrap();

        assert_eq!(p.buffered_events(), 3);
        assert!(p.buffered_bytes() > 0);
        // "alice" and "bob" could collide on the same shard with
        // shard_count = 4 — that's a hash-dependent accident. We
        // only assert that at least one bucket exists and that the
        // total event count is right.
        assert!(p.bucket_count() >= 1);
    }

    #[test]
    fn push_event_errors_when_budget_would_be_exceeded() {
        // Choose a budget so small that even the empty-event
        // baseline exceeds it. That's the cleanest way to trigger
        // the overflow branch without having to know the exact
        // per-event estimate. `estimated_event_size` is guaranteed
        // to return at least `size_of::<Event>()` bytes, so a
        // 1-byte budget always overflows on the first push.
        let mut p = Partitioner::new(4, 30, 1, 1).unwrap();
        let err = p
            .push_event(simple_event("alice", 0, "click"))
            .expect_err("budget must overflow on first push");
        match err {
            BqliteError::Execution(msg) => {
                assert!(msg.contains("memory budget"), "got: {msg}");
            }
            other => panic!("expected Execution, got {other:?}"),
        }
        // Nothing was inserted into the bucket map and the buffer
        // counter stayed at 0 — the error leaves the partitioner in
        // its pre-push state so the caller can abort and retry.
        assert_eq!(p.buffered_events(), 0);
        assert_eq!(p.buffered_bytes(), 0);
        assert_eq!(p.bucket_count(), 0);
    }

    #[test]
    fn push_event_errors_on_pre_epoch_timestamp() {
        let mut p = Partitioner::new(4, 30, 1, 1 << 20).unwrap();
        let pre_epoch = Event::new(EntityId::from("alice"), Timestamp(-1), "click");
        let err = p
            .push_event(pre_epoch)
            .expect_err("pre-epoch timestamp must error");
        match err {
            BqliteError::Execution(msg) => {
                assert!(msg.contains("pre-epoch"), "got: {msg}");
            }
            other => panic!("expected Execution, got {other:?}"),
        }
        assert_eq!(p.buffered_events(), 0);
        assert_eq!(p.buffered_bytes(), 0);
    }

    #[test]
    fn push_event_recovers_after_a_failed_push() {
        // A failed push (pre-epoch timestamp here) must not poison
        // the partitioner — subsequent valid pushes still land.
        let mut p = Partitioner::new(4, 30, 1, 1 << 20).unwrap();
        let _ = p
            .push_event(Event::new(EntityId::from("alice"), Timestamp(-1), "click"))
            .expect_err("pre-epoch must error");
        p.push_event(simple_event("alice", 0, "click"))
            .expect("valid push after failure must succeed");
        assert_eq!(p.buffered_events(), 1);
        assert!(p.buffered_bytes() > 0);
    }

    #[test]
    fn push_event_at_exact_budget_boundary_succeeds() {
        // The overflow check is `projected > budget_bytes`, so a
        // push that lands exactly on the budget must succeed. Pin
        // the equality branch by sizing the budget to the event.
        let event = simple_event("alice", 0, "click");
        let exact = estimated_event_size(&event);
        let mut p = Partitioner::new(1, 30, 1, exact).unwrap();
        p.push_event(event).expect("exact-fit push must succeed");
        assert_eq!(p.buffered_bytes(), exact);
        assert_eq!(p.buffered_events(), 1);

        // A second push of the same shape now exceeds the budget
        // by one event's worth and must error without mutating.
        let err = p
            .push_event(simple_event("alice", 1, "click"))
            .expect_err("second push must overflow");
        assert!(matches!(err, BqliteError::Execution(_)));
        assert_eq!(
            p.buffered_bytes(),
            exact,
            "failed push must leave the buffer counter untouched"
        );
        assert_eq!(p.buffered_events(), 1);
    }

    #[test]
    fn drain_sorted_returns_buckets_in_window_then_shard_order() {
        // Craft events across multiple windows and shards. With
        // shard_count = 1, every entity lands in shard 0, so the
        // bucket key is just `(window_id, 0)`. We push events in
        // scrambled window order and assert the drain visits them
        // in ascending window order.
        let mut p = Partitioner::new(1, 30, 1, 1 << 20).unwrap();
        p.push_event(simple_event("alice", 90, "click")).unwrap(); // window 90
        p.push_event(simple_event("alice", 0, "click")).unwrap(); // window 0
        p.push_event(simple_event("alice", 60, "click")).unwrap(); // window 60
        p.push_event(simple_event("alice", 30, "click")).unwrap(); // window 30

        let drained: Vec<(BucketKey, Vec<Event>)> = p.drain_sorted().collect();
        let window_ids: Vec<u32> = drained.iter().map(|((w, _s), _)| *w).collect();
        assert_eq!(window_ids, vec![0, 30, 60, 90]);
    }

    #[test]
    fn drain_sorted_sorts_each_bucket_by_entity_then_timestamp() {
        // Insertion order deliberately does not match sort order.
        // After drain, every bucket must be ordered by
        // (entity_id, timestamp) ascending.
        let mut p = Partitioner::new(1, 30, 1, 1 << 20).unwrap();
        // All land in window 0, shard 0.
        p.push_event(simple_event("charlie", 5, "click")).unwrap();
        p.push_event(simple_event("alice", 10, "click")).unwrap();
        p.push_event(simple_event("bob", 7, "click")).unwrap();
        p.push_event(simple_event("alice", 2, "click")).unwrap();
        p.push_event(simple_event("charlie", 1, "click")).unwrap();

        let drained: Vec<(BucketKey, Vec<Event>)> = p.drain_sorted().collect();
        assert_eq!(drained.len(), 1);
        let sorted = &drained[0].1;
        let keys: Vec<(String, i64)> = sorted
            .iter()
            .map(|e| {
                (
                    e.entity.as_str().unwrap().to_string(),
                    e.timestamp.as_nanos(),
                )
            })
            .collect();
        assert_eq!(
            keys,
            vec![
                ("alice".into(), 2 * NS_PER_DAY),
                ("alice".into(), 10 * NS_PER_DAY),
                ("bob".into(), 7 * NS_PER_DAY),
                ("charlie".into(), NS_PER_DAY),
                ("charlie".into(), 5 * NS_PER_DAY),
            ]
        );
    }

    #[test]
    fn drain_sorted_sort_is_stable_for_equal_keys() {
        // Two events with identical (entity, timestamp) must
        // retain their insertion order — a stable sort invariant
        // downstream operators rely on when ties can legitimately
        // happen at nanosecond resolution.
        let mut p = Partitioner::new(1, 30, 1, 1 << 20).unwrap();
        p.push_event(simple_event("alice", 0, "first")).unwrap();
        p.push_event(simple_event("alice", 0, "second")).unwrap();
        p.push_event(simple_event("alice", 0, "third")).unwrap();

        let drained: Vec<(BucketKey, Vec<Event>)> = p.drain_sorted().collect();
        let types: Vec<String> = drained[0].1.iter().map(|e| e.event_type.clone()).collect();
        assert_eq!(types, vec!["first", "second", "third"]);
    }

    #[test]
    fn drain_sorted_distributes_events_across_multiple_shards() {
        // With shard_count > 1, distinct entities must land in
        // different shard buckets for at least one pair (otherwise
        // the hash has collapsed, which the helper tests already
        // guard against). We confirm the drained bucket keys
        // reflect the actual `(window, shard)` distribution.
        let mut p = Partitioner::new(32, 30, 1, 1 << 20).unwrap();
        for n in 0..64_u64 {
            p.push_event(simple_event(&format!("user_{n}"), 0, "click"))
                .unwrap();
        }

        let drained: Vec<(BucketKey, Vec<Event>)> = p.drain_sorted().collect();
        // Every bucket key shares the same window_id (0) because
        // every event is at day 0.
        for ((window_id, _), _) in &drained {
            assert_eq!(*window_id, 0);
        }
        // At least two distinct shards must appear; collapsing all
        // 64 entities onto one shard would be a hash regression.
        let shard_ids: std::collections::HashSet<u16> =
            drained.iter().map(|((_w, s), _)| *s).collect();
        assert!(
            shard_ids.len() >= 2,
            "64 distinct entities collapsed onto a single shard — hash is broken"
        );
    }

    #[test]
    fn push_event_with_custom_shard_count_respects_bounds() {
        let mut p = Partitioner::new(3, 30, 1, 1 << 20).unwrap();
        for n in 0..50 {
            p.push_event(simple_event(&format!("user_{n}"), 0, "click"))
                .unwrap();
        }
        for ((_w, shard_id), _) in p.drain_sorted() {
            assert!(shard_id < 3, "shard_id {shard_id} out of range for 3");
        }
    }

    // ── estimated_event_size ───────────────────────────────────────────────

    #[test]
    fn estimated_event_size_is_monotonic_in_event_content() {
        use bqlite_core::property::PropertyValue;

        let bare = Event::new(EntityId::from("a"), Timestamp(0), "c");
        let mut with_prop = bare.clone();
        with_prop.properties.push((
            "k".into(),
            PropertyValue::String("some long string value".into()),
        ));

        let bare_size = estimated_event_size(&bare);
        let big_size = estimated_event_size(&with_prop);
        assert!(big_size > bare_size);
    }

    #[test]
    fn estimated_event_size_never_returns_zero_for_any_event() {
        let bare = Event::new(EntityId::from(0_i64), Timestamp(0), "");
        assert!(estimated_event_size(&bare) >= std::mem::size_of::<Event>());
    }
}
