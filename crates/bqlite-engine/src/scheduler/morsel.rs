//! `Morsel` descriptor and `MorselGenerator` (design §3).
//!
//! A morsel is the unit of work handed to a worker — a half-open
//! `[entity_lo, entity_hi)` slice within one shard, bundled with the
//! shard's per-window segment metadata for that range. The generator
//! produces morsels lazily on the coordinator thread without ever
//! decoding column data (design §3.2 "metadata-only generator").
//!
//! ## v1 scope
//!
//! The TASK-523 generator emits exactly one morsel per shard covering
//! the full entity range. Per-entity-range subdivision (the §3.4
//! adaptive-halving control loop reading from a `MorselSizeState`)
//! lands once individual operators understand the
//! `(shard_id, [entity_lo, entity_hi))` parameter — see design §11
//! follow-ons. The [`Morsel`] descriptor is shaped so adding the
//! sub-shard generator is purely additive: callers that today receive
//! `EntityRange::All` will instead receive `EntityRange::Bounded`
//! values without any other change.

use std::sync::Arc;

use bqlite_core::storage::SegmentHandle;
use bqlite_core::EntityId;

/// Shard identifier — type-aliased to `u32` to match
/// `SegmentHandle::shard_id` (`bqlite-core`'s storage handle).
pub type ShardId = u32;

/// Window identifier — type-aliased to `u64` to match
/// `SegmentHandle::window_id`.
pub type WindowId = u64;

/// Entity sub-range a morsel covers within its shard.
///
/// Per design §3.3, the generator preserves the single-entity
/// invariant: `entity_lo` and `entity_hi` always fall on entity
/// boundaries, and the next morsel's `entity_lo` equals the previous
/// morsel's `entity_hi`. The v1 degenerate generator emits
/// [`EntityRange::All`] (covers every entity in the shard); future
/// implementations will emit [`EntityRange::Bounded`] for sub-shard
/// morsels without changing the rest of the descriptor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntityRange {
    /// Covers every entity in the shard. The v1 generator only ever
    /// emits this variant.
    All,
    /// Half-open `[lo, hi)` range. `lo` and `hi` must each fall on an
    /// entity boundary in the shard's `(entity_id, ts)` sort order.
    /// Reserved for the per-entity-range generator landing in
    /// follow-on tasks; not produced by [`MorselGenerator::degenerate`].
    Bounded { lo: EntityId, hi: EntityId },
}

impl EntityRange {
    /// True iff this range covers the entire shard. The v1 path uses
    /// this to skip range-restriction work in operators.
    pub fn is_all(&self) -> bool {
        matches!(self, EntityRange::All)
    }
}

/// Per-window segment list contributing to a morsel.
///
/// `segments` is held behind an `Arc<[SegmentHandle]>` so cloning a
/// morsel descriptor across shards / workers is cheap and zero-copy
/// for the segment metadata. Per design §3.1, the generator never
/// opens segment files or decodes zone maps — the slice is whatever
/// the manifest snapshot delivered to the coordinator.
#[derive(Debug, Clone)]
pub struct WindowSegments {
    pub window_id: WindowId,
    pub segments: Arc<[SegmentHandle]>,
}

impl WindowSegments {
    /// Approximate row count over the segments in this window. Sums
    /// `SegmentHandle::row_count`. Used for the morsel's
    /// `estimated_rows` budget metric — not guaranteed exact under
    /// tombstoning.
    pub fn estimated_rows(&self) -> u64 {
        self.segments.iter().map(|s| s.row_count).sum()
    }
}

/// One unit of work handed to a worker (design §3.1).
///
/// Workers run the full pipeline on the morsel and produce nothing
/// externally except updates to the shard's per-shard accumulator and
/// per-worker metrics. The descriptor is plain data — cloning is the
/// cost of cloning a few `Arc`s and a small `Vec` of [`WindowSegments`].
#[derive(Debug, Clone)]
pub struct Morsel {
    /// Shard this morsel belongs to. All entities in the morsel
    /// satisfy `xxhash64(entity_id) % num_shards == shard_id`.
    pub shard_id: ShardId,
    /// Entity sub-range. See [`EntityRange`].
    pub entity_range: EntityRange,
    /// Per-window segment lists for this shard, restricted to segments
    /// whose entity range overlaps `entity_range`. The v1 generator
    /// passes through every segment for the shard regardless of
    /// `entity_range` (because `EntityRange::All` covers everything);
    /// future generators will pre-filter segments here so the worker
    /// does not have to.
    pub segments: Vec<WindowSegments>,
    /// Approximate row count, for budget metrics. Sum of every
    /// `WindowSegments::estimated_rows`.
    pub estimated_rows: u64,
    /// True iff this is the last morsel emitted for the shard.
    /// The accumulator-handle bookkeeping uses this as the trigger
    /// for `total_emitted` (design §4.3).
    pub is_shard_final: bool,
}

/// Manifest snapshot for one shard, used to construct a morsel.
///
/// The coordinator builds one of these per shard the query touches,
/// after zone-map / tombstone / predicate pruning. The snapshot is
/// metadata-only — segment files are not opened.
#[derive(Debug, Clone)]
pub struct ShardSnapshot {
    pub shard_id: ShardId,
    pub windows: Vec<WindowSegments>,
}

impl ShardSnapshot {
    /// Total estimated row count across every window in this shard.
    pub fn estimated_rows(&self) -> u64 {
        self.windows
            .iter()
            .map(WindowSegments::estimated_rows)
            .sum()
    }
}

/// Lazy producer of morsels for one shard (design §3.2).
///
/// The v1 implementation emits exactly one morsel per shard covering
/// the entire entity range. Per design §11, this is the migration
/// boundary: when individual operators learn to take an entity range
/// parameter, this generator is replaced (or extended) with the
/// adaptive halving control loop from design §3.4.
#[derive(Debug)]
pub struct MorselGenerator {
    snapshot: ShardSnapshot,
    /// Whether the single degenerate morsel has already been emitted.
    /// `None` after the morsel is taken; subsequent calls return
    /// `None` for "drained".
    pending: Option<Morsel>,
    /// Total morsels this generator will ever emit, set to `Some(n)`
    /// once the generator drains. `n` is `1` for the degenerate path,
    /// `0` for an empty shard (design §3.6).
    total_emitted: Option<u64>,
}

impl MorselGenerator {
    /// Construct the v1 degenerate generator: one morsel covering the
    /// shard's full entity range, or zero morsels for empty shards.
    pub fn degenerate(snapshot: ShardSnapshot) -> Self {
        let estimated_rows = snapshot.estimated_rows();
        let is_empty =
            snapshot.windows.is_empty() || snapshot.windows.iter().all(|w| w.segments.is_empty());

        let pending = if is_empty {
            None
        } else {
            Some(Morsel {
                shard_id: snapshot.shard_id,
                entity_range: EntityRange::All,
                segments: snapshot.windows.clone(),
                estimated_rows,
                is_shard_final: true,
            })
        };

        let total_emitted = if pending.is_none() { Some(0) } else { None };

        Self {
            snapshot,
            pending,
            total_emitted,
        }
    }

    /// Shard this generator produces morsels for.
    pub fn shard_id(&self) -> ShardId {
        self.snapshot.shard_id
    }

    /// True iff every morsel this generator will ever emit has
    /// already been emitted.
    pub fn is_drained(&self) -> bool {
        self.pending.is_none()
    }

    /// Total morsels produced by this generator, observed only after
    /// the generator drains. Returns `None` while the generator may
    /// still produce more morsels.
    pub fn total_emitted(&self) -> Option<u64> {
        self.total_emitted
    }

    /// Take the next morsel, or `None` if drained.
    ///
    /// On the v1 degenerate path: the first call returns the single
    /// whole-shard morsel; every subsequent call returns `None`.
    /// Named `take_next` rather than `next` so it does not shadow
    /// `std::iter::Iterator::next` — the generator is not an
    /// iterator (the design's lazy-generation contract requires
    /// borrowing the snapshot, which `Iterator::next` cannot
    /// express).
    pub fn take_next(&mut self) -> Option<Morsel> {
        let morsel = self.pending.take()?;
        // The degenerate generator always emits exactly one morsel,
        // so total_emitted is fixed at 1 after the first take.
        self.total_emitted = Some(1);
        Some(morsel)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bqlite_core::storage::SegmentHandle;

    fn handle(segment_id: u64, shard: u32, window: u64, rows: u64) -> SegmentHandle {
        SegmentHandle {
            segment_id,
            shard_id: shard,
            window_id: window,
            row_count: rows,
            schema_version: 1,
            seq_id_first: 0,
            batch_id: 0,
        }
    }

    fn snapshot_with_segments(shard: u32, segments: &[(u64, u64)]) -> ShardSnapshot {
        let segs: Vec<SegmentHandle> = segments
            .iter()
            .enumerate()
            .map(|(i, (window, rows))| handle(i as u64, shard, *window, *rows))
            .collect();
        // Group by window for the WindowSegments shape.
        let mut by_window: std::collections::BTreeMap<u64, Vec<SegmentHandle>> =
            std::collections::BTreeMap::new();
        for s in segs {
            by_window.entry(s.window_id).or_default().push(s);
        }
        let windows = by_window
            .into_iter()
            .map(|(window_id, segs)| WindowSegments {
                window_id,
                segments: Arc::from(segs),
            })
            .collect();
        ShardSnapshot {
            shard_id: shard,
            windows,
        }
    }

    #[test]
    fn degenerate_generator_emits_one_whole_shard_morsel() {
        let snap = snapshot_with_segments(7, &[(0, 100), (0, 50), (1, 200)]);
        let mut generator = MorselGenerator::degenerate(snap);

        assert_eq!(generator.shard_id(), 7);
        assert!(!generator.is_drained());
        assert_eq!(generator.total_emitted(), None);

        let m = generator.take_next().expect("first morsel");
        assert_eq!(m.shard_id, 7);
        assert!(m.entity_range.is_all());
        assert!(m.is_shard_final);
        assert_eq!(m.estimated_rows, 350);
        assert_eq!(m.segments.len(), 2); // two windows

        assert!(generator.is_drained());
        assert_eq!(generator.total_emitted(), Some(1));
        assert!(generator.take_next().is_none());
        // Calling next() repeatedly is idempotent on a drained generator.
        assert!(generator.take_next().is_none());
    }

    #[test]
    fn empty_shard_generator_emits_zero_morsels() {
        let snap = ShardSnapshot {
            shard_id: 3,
            windows: vec![],
        };
        let mut generator = MorselGenerator::degenerate(snap);

        // Per design §3.6: empty shard reports drained immediately
        // and total_emitted = Some(0).
        assert!(generator.is_drained());
        assert_eq!(generator.total_emitted(), Some(0));
        assert!(generator.take_next().is_none());
    }

    #[test]
    fn empty_window_segments_treated_as_empty_shard() {
        // A window with no segments — pruning eliminated everything.
        let snap = ShardSnapshot {
            shard_id: 4,
            windows: vec![WindowSegments {
                window_id: 0,
                segments: Arc::from(Vec::<SegmentHandle>::new()),
            }],
        };
        let generator = MorselGenerator::degenerate(snap);
        assert!(generator.is_drained());
        assert_eq!(generator.total_emitted(), Some(0));
    }

    #[test]
    fn morsel_clone_is_cheap() {
        // The Arc<[SegmentHandle]> inside WindowSegments is shared
        // across clones; cloning a Morsel must not reallocate the
        // segment slice.
        let snap = snapshot_with_segments(0, &[(0, 100), (1, 200)]);
        let mut generator = MorselGenerator::degenerate(snap);
        let m = generator.take_next().unwrap();

        let m2 = m.clone();
        // The Arcs in window 0 still point at the same allocation.
        assert!(Arc::ptr_eq(
            &m.segments[0].segments,
            &m2.segments[0].segments
        ));
    }
}
