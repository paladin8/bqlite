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
//! Wave 2 TASK-218 is split into two checkpoints:
//!
//! 1. Pure helpers — [`shard_id_for`] and [`window_id_for`] — plus
//!    the sharding / windowing invariants they enforce. This is the
//!    current checkpoint.
//! 2. The stateful [`Partitioner`] that buffers events, tracks a
//!    memory budget, sorts on drain, and assigns a fresh `batch_id`
//!    from the manifest counter. Lands in a follow-up checkpoint on
//!    the same branch.
//!
//! Both halves live in this file so downstream tasks (TASK-214
//! writer orchestration, TASK-233 CSV ingest) can import a single
//! `crate::ingest::partitioner::Partitioner`.

use bqlite_core::error::{BqliteError, Result};
use bqlite_core::event::EntityId;
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
}
