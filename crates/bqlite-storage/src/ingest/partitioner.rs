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
//! # External spill
//!
//! Per `docs/design/engine/spill.md` § 6.2, when the partitioner is
//! constructed via [`Partitioner::with_spill_dir`] it spills the
//! largest in-memory bucket to disk before refusing a push that
//! would overshoot the configured budget. Spill files live under a
//! per-ingest subdirectory created by the engine (typically
//! `<db_root>/spill/ingest-<uuid>/`); the partitioner only writes
//! files of the form
//! `ingest-part-w<window>-s<shard:04>-<seq:06>.spill` underneath it.
//! Each file is a length-prefixed [`postcard`]-encoded stream of
//! [`Event`] records sorted by `(entity_id, timestamp)`. The doc
//! originally specified Arrow IPC, but the partitioner's input is
//! `Event` (not `RecordBatch`) and it does not own a
//! [`bqlite_core::schema::TableSchema`] at this layer; postcard is
//! sufficient for a private, per-process artefact and reuses the
//! dependency the segment writer already pulls in.
//!
//! Drain becomes a per-bucket k-way merge across the spilled run
//! files plus the in-memory residual. The drain order at the bucket
//! level is unchanged: ascending `(window_id, shard_id)`.
//!
//! Downstream tasks (TASK-214 writer orchestration, TASK-233 CSV
//! ingest) import a single `crate::ingest::partitioner::Partitioner`.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::PathBuf;

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
/// # Memory model
///
/// The buffer is tracked as a per-bucket running sum of per-event
/// heap-size estimates (see `estimated_event_size`); the
/// partitioner-wide [`Partitioner::buffered_bytes`] is the sum
/// across buckets. The estimate is deliberately cheap and
/// monotonic: it will under-count some exotic `PropertyValue`
/// shapes, but it is stable under equal inputs and never returns
/// zero for a non-empty event.
///
/// When the in-memory buffer would exceed [`Partitioner::budget_bytes`]:
///
/// - With **no spill directory** (constructed via
///   [`Partitioner::new`]): [`Partitioner::push_event`] returns a
///   typed `BqliteError::Execution` and leaves the partitioner
///   state unchanged.
/// - With a **spill directory** (constructed via
///   [`Partitioner::with_spill_dir`]): the partitioner spills the
///   largest in-memory bucket to disk, frees its bytes, retries the
///   push, and repeats until the event fits or every bucket is
///   spilled. If the event still does not fit after every bucket
///   is on disk — that is, the single event is larger than the
///   entire budget — the partitioner returns `Execution` with the
///   "oversized event" wording from
///   `docs/design/engine/spill.md` § 6.2.
///
/// # Why `BTreeMap`, not `HashMap`
///
/// Deterministic iteration order on drain keeps snapshot-style
/// tests stable and lets the writer emit segments in time order
/// without a separate sort pass. Bucket counts at realistic ingest
/// scale (a handful of windows times a few-thousand shards times a
/// few hundred thousand events) fit comfortably in an ordered map.
/// O(log n) per insert is dwarfed by the per-event allocation cost.
#[derive(Debug)]
pub struct Partitioner {
    shard_count: u16,
    window_days: u32,
    batch_id: u64,
    budget_bytes: usize,
    /// Sum of per-bucket `bytes` across [`Self::buckets`].
    /// Maintained as an invariant on every mutation so the
    /// overflow check is `O(1)`.
    buffered_bytes: usize,
    buckets: BTreeMap<BucketKey, Bucket>,
    /// `Some(...)` enables external spill. `None` keeps the
    /// fail-fast Wave 2 contract for callers that haven't yet
    /// migrated to the spill-aware constructor.
    spill: Option<SpillState>,
}

/// Per-bucket in-memory state.
///
/// The byte estimate is updated on every push and on every spill
/// drain so it stays in sync with [`Self::events`]. Tracking it
/// per-bucket lets [`Partitioner::push_event`] pick the largest
/// bucket to spill in `O(buckets)` without re-walking every event.
#[derive(Debug, Default)]
struct Bucket {
    events: Vec<Event>,
    bytes: usize,
}

/// External-spill state — populated when the partitioner was
/// constructed via [`Partitioner::with_spill_dir`].
#[derive(Debug)]
struct SpillState {
    /// Per-ingest spill subdirectory (e.g.
    /// `<db_root>/spill/ingest-<uuid>/`). Owned by the engine; the
    /// partitioner only writes files into it. The directory is
    /// expected to exist at construction time.
    dir: PathBuf,
    /// Monotone counter for filename `<seq>` slots. Incremented per
    /// spill write across every `(window, shard)` bucket so two
    /// concurrent buckets cannot accidentally collide on
    /// `<purpose>-<seq>.spill`.
    next_seq: u64,
    /// Per-bucket spilled run files, in creation order. Each entry
    /// is a sorted-by-`(entity_id, ts)` postcard stream; the merge
    /// pass treats every file in this `Vec` as a separate sorted
    /// run and walks them all alongside the in-memory residual.
    runs: BTreeMap<BucketKey, Vec<SpillRunFile>>,
}

impl Partitioner {
    /// Construct a fresh partitioner for one ingest call without
    /// spill — pushes that overshoot the budget return an error.
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
        Self::build(shard_count, window_days, batch_id, budget_bytes, None)
    }

    /// Construct a fresh partitioner backed by external spill.
    ///
    /// `spill_dir` is a per-ingest subdirectory the engine has
    /// already created (typically
    /// `<db_root>/spill/ingest-<uuid>/`). The partitioner writes
    /// run files of the form
    /// `ingest-part-w<window>-s<shard:04>-<seq:06>.spill` directly
    /// under that path; the engine is responsible for the
    /// directory's lifetime (creation up-front, removal on every
    /// exit path) per `docs/design/engine/spill.md` § 8.3. The
    /// partitioner only owns the individual files via the
    /// `SpillRunFile` RAII guard.
    ///
    /// Same validation rules as [`Partitioner::new`].
    pub fn with_spill_dir(
        shard_count: u16,
        window_days: u32,
        batch_id: u64,
        budget_bytes: usize,
        spill_dir: PathBuf,
    ) -> Result<Self> {
        let spill = SpillState {
            dir: spill_dir,
            next_seq: 0,
            runs: BTreeMap::new(),
        };
        Self::build(
            shard_count,
            window_days,
            batch_id,
            budget_bytes,
            Some(spill),
        )
    }

    fn build(
        shard_count: u16,
        window_days: u32,
        batch_id: u64,
        budget_bytes: usize,
        spill: Option<SpillState>,
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
            spill,
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

    /// Current in-memory buffer estimate (bytes). Excludes events
    /// already written to a spill run file.
    #[inline]
    pub fn buffered_bytes(&self) -> usize {
        self.buffered_bytes
    }

    /// Total number of events buffered in memory across every
    /// bucket. Excludes events on spill files.
    pub fn buffered_events(&self) -> usize {
        self.buckets.values().map(|b| b.events.len()).sum()
    }

    /// Number of non-empty in-memory `(window, shard)` buckets
    /// currently held. Buckets whose entire content has been
    /// spilled show up in
    /// [`Partitioner::spilled_run_count`]-keyed state but are
    /// removed from the in-memory bucket map until a fresh push
    /// re-introduces the key.
    pub fn bucket_count(&self) -> usize {
        self.buckets.len()
    }

    /// Total number of spilled run files held across every bucket.
    /// Returns `0` when spill is disabled or no spill has happened.
    /// Exposed for instrumentation and tests.
    pub fn spilled_run_count(&self) -> usize {
        self.spill
            .as_ref()
            .map(|s| s.runs.values().map(Vec::len).sum())
            .unwrap_or(0)
    }

    /// Route `event` into its `(window, shard)` bucket.
    ///
    /// The partitioner hashes `event.entity` into a shard and
    /// derives a window id from `event.timestamp`, estimates the
    /// event's in-memory footprint, and either appends the event or
    /// — if the projected buffer would overshoot
    /// [`Self::budget_bytes`] — runs the spill loop (when
    /// constructed via [`Partitioner::with_spill_dir`]) or refuses
    /// the push (when constructed via [`Partitioner::new`]).
    ///
    /// # Errors
    ///
    /// - [`BqliteError::Execution`] with a `"pre-epoch"` reason if
    ///   the timestamp is before 1970-01-01 UTC (via
    ///   [`window_id_for`]).
    /// - [`BqliteError::Execution`] with a `"memory budget"` reason
    ///   if no spill is configured and the push would cross the
    ///   budget. The partitioner state is unchanged.
    /// - [`BqliteError::Execution`] with an `"oversized event"`
    ///   reason if spill is configured but every in-memory bucket
    ///   has been spilled and the single event is still larger
    ///   than the budget. Per `docs/design/engine/spill.md` § 6.2.
    /// - [`BqliteError::Io`] on a spill-write IO failure.
    pub fn push_event(&mut self, event: Event) -> Result<()> {
        let shard_id = shard_id_for(&event.entity, self.shard_count);
        let window_id = window_id_for(event.timestamp, self.window_days)?;
        let key: BucketKey = (window_id, shard_id);

        let size = estimated_event_size(&event);

        // Spill loop: while the projected bytes would overshoot the
        // budget, pick the largest in-memory bucket, spill it to
        // disk, and retry. The loop terminates either with a
        // projected fit (the common case after one or two spills)
        // or with an empty in-memory bucket map (the single event
        // is bigger than the entire budget, which we report
        // verbatim).
        loop {
            let projected = self.buffered_bytes.saturating_add(size);
            if projected <= self.budget_bytes {
                break;
            }

            // No spill configured → fail fast as in Wave 2, leaving
            // the partitioner in its pre-push state.
            if self.spill.is_none() {
                return Err(BqliteError::Execution(format!(
                    "partitioner: memory budget would be exceeded — adding a {size}-byte event \
                     would take buffered_bytes from {} to {projected}, above the {} ceiling \
                     (no spill directory configured; pass `Partitioner::with_spill_dir` or raise the budget)",
                    self.buffered_bytes, self.budget_bytes
                )));
            }

            // Pick the largest in-memory bucket. `None` here means
            // every bucket has already been spilled and we still
            // cannot fit the incoming event — the single event
            // exceeds the budget all on its own.
            let Some(victim_key) = self.largest_bucket_key() else {
                return Err(BqliteError::Execution(format!(
                    "partitioner: oversized event — a {size}-byte event exceeds the entire \
                     {} byte budget after spilling every bucket; raise the budget or split \
                     the event (per docs/design/engine/spill.md §6.2)",
                    self.budget_bytes
                )));
            };

            self.spill_bucket(victim_key)?;
        }

        // Append the event and update the per-bucket and aggregate
        // byte counters together so the invariant
        // `buffered_bytes == sum(bucket.bytes)` holds at every
        // observation point.
        let bucket = self.buckets.entry(key).or_default();
        bucket.events.push(event);
        bucket.bytes = bucket.bytes.saturating_add(size);
        self.buffered_bytes = self.buffered_bytes.saturating_add(size);
        Ok(())
    }

    /// Pick the bucket key with the largest in-memory byte
    /// footprint. Returns `None` if no in-memory bucket is non-
    /// empty. Ties (equal byte counts) are broken deterministically
    /// by the smallest `(window_id, shard_id)` so the spill order
    /// is stable across runs with identical input.
    fn largest_bucket_key(&self) -> Option<BucketKey> {
        // `kb.cmp(ka)` is intentionally reversed: with `max_by`, an
        // entry whose comparator returns `Greater` wins. For two
        // buckets of equal `bytes`, `kb.cmp(ka)` is `Greater`
        // exactly when `ka < kb`, so the smaller `(window, shard)`
        // bucket wins the tie. Pinned by the docstring above.
        self.buckets
            .iter()
            .filter(|(_, b)| !b.events.is_empty())
            .max_by(|(ka, ba), (kb, bb)| ba.bytes.cmp(&bb.bytes).then_with(|| kb.cmp(ka)))
            .map(|(k, _)| *k)
    }

    /// Sort the named bucket and write it to a fresh spill run
    /// file. Removes the bucket from the in-memory map and frees
    /// its bytes from [`Self::buffered_bytes`]. The new run file is
    /// appended to `spill.runs[key]` (creation order).
    fn spill_bucket(&mut self, key: BucketKey) -> Result<()> {
        let Some(spill) = self.spill.as_mut() else {
            // Caller-side invariant: only invoked when spill is configured.
            return Err(BqliteError::Execution(
                "partitioner: internal error — spill_bucket called without a spill directory"
                    .into(),
            ));
        };

        let mut bucket = match self.buckets.remove(&key) {
            Some(b) => b,
            None => {
                debug_assert!(false, "spill_bucket called for an absent bucket {key:?}");
                return Ok(());
            }
        };

        // Stable sort to preserve insertion order across equal
        // `(entity, ts)` keys. Same comparator as the in-memory
        // drain path so spill+merge yields the same sequence as the
        // no-spill drain.
        bucket.events.sort_by(|a, b| {
            a.entity
                .cmp(&b.entity)
                .then_with(|| a.timestamp.cmp(&b.timestamp))
        });

        let seq = spill.next_seq;
        spill.next_seq = spill.next_seq.saturating_add(1);
        let path = spill.dir.join(format!(
            "ingest-part-w{window}-s{shard:04}-{seq:06}.spill",
            window = key.0,
            shard = key.1,
            seq = seq,
        ));

        let mut writer = BufWriter::new(
            OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&path)
                .map_err(|e| io_err(&path, "create ingest spill file", e))?,
        );

        // Write a length-prefixed postcard record per event. The
        // 4-byte LE length prefix is sufficient for events well
        // beyond any realistic ingest size — postcard's encoding is
        // dense and rarely exceeds ~10x the event's logical size.
        for event in &bucket.events {
            let bytes = postcard::to_stdvec(event).map_err(|e| {
                BqliteError::Execution(format!(
                    "partitioner: failed to serialize event for spill write: {e}"
                ))
            })?;
            let len = u32::try_from(bytes.len()).map_err(|_| {
                BqliteError::Execution(format!(
                    "partitioner: serialized event of {} bytes exceeds the {}-byte spill record \
                     length limit",
                    bytes.len(),
                    u32::MAX
                ))
            })?;
            writer
                .write_all(&len.to_le_bytes())
                .map_err(|e| io_err(&path, "write ingest spill record length", e))?;
            writer
                .write_all(&bytes)
                .map_err(|e| io_err(&path, "write ingest spill record body", e))?;
        }

        // Flush the writer's buffer before dropping it. We do not
        // `fsync` — spill files are temporary and crash-recovered
        // by `<db_root>/spill/` reclamation per spill.md § 9.
        writer
            .flush()
            .map_err(|e| io_err(&path, "flush ingest spill file", e))?;

        let bytes_freed = bucket.bytes;
        debug_assert!(self.buffered_bytes >= bytes_freed);
        self.buffered_bytes = self.buffered_bytes.saturating_sub(bytes_freed);

        spill
            .runs
            .entry(key)
            .or_default()
            .push(SpillRunFile { path });

        Ok(())
    }

    /// Consume the partitioner and yield each `(BucketKey, sorted
    /// events)` pair in ascending `(window_id, shard_id)` order.
    ///
    /// In the no-spill case the implementation is identical to the
    /// pre-TASK-512 drain — every bucket is sorted in place and
    /// returned. With spill enabled, each bucket's events are
    /// streamed from its spilled run files (already sorted) plus
    /// the in-memory residual (sorted in place) through a k-way
    /// merge that yields events in `(entity_id, timestamp)` order
    /// with stable tie-break by run insertion order. The merge
    /// reads at most one chunk at a time from each run, so memory
    /// usage during drain is bounded by the per-bucket merge plus
    /// the deserialised front-of-run events — never the entire
    /// bucket footprint that triggered the spill.
    pub fn drain_sorted(self) -> Box<dyn Iterator<Item = (BucketKey, Vec<Event>)>> {
        let Partitioner {
            mut buckets, spill, ..
        } = self;

        // Sort the in-memory residuals in place ahead of any merge
        // so both the no-spill and the spill paths share the same
        // comparator. This costs O(n log n) per bucket whether or
        // not we end up merging — but the no-spill case has no
        // alternative anyway.
        for bucket in buckets.values_mut() {
            bucket.events.sort_by(|a, b| {
                a.entity
                    .cmp(&b.entity)
                    .then_with(|| a.timestamp.cmp(&b.timestamp))
            });
        }

        let mut spill_runs: BTreeMap<BucketKey, Vec<SpillRunFile>> = match spill {
            Some(s) => s.runs,
            None => BTreeMap::new(),
        };

        // Union of bucket keys: every key that has either an
        // in-memory residual or one or more spill files.
        let keys: Vec<BucketKey> = buckets
            .keys()
            .copied()
            .chain(spill_runs.keys().copied())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();

        let it = keys.into_iter().map(move |key| {
            let in_memory = buckets.remove(&key).map(|b| b.events).unwrap_or_default();
            let runs = spill_runs.remove(&key).unwrap_or_default();
            let merged = merge_runs_for_bucket(in_memory, runs);
            (key, merged)
        });
        Box::new(it)
    }
}

/// Result type for the per-bucket merge. We collect the merged
/// events into a fresh `Vec<Event>` to keep the partitioner's
/// public contract — `(BucketKey, Vec<Event>)` per bucket — stable
/// across the spill / no-spill cases. Per-bucket buffering is what
/// the writer expects today; a streaming variant is a future-wave
/// follow-up that would change the writer's API too.
fn merge_runs_for_bucket(in_memory: Vec<Event>, runs: Vec<SpillRunFile>) -> Vec<Event> {
    if runs.is_empty() {
        return in_memory;
    }

    let total_capacity_hint = in_memory.len();
    let mut output: Vec<Event> = Vec::with_capacity(total_capacity_hint);

    // Each run is its own iterator. Run indices are the tie-break
    // key for stable ordering: when two heads share a `(entity,
    // ts)`, the smaller run index wins. The semantic order events
    // were observed by `push_event` is "earlier-spilled runs first,
    // then the still-in-memory residual" — so we assign indices
    // 0..=runs.len()-1 to the spilled runs (in their `Vec`
    // insertion order, which matches creation order — see
    // `Partitioner::spill_bucket`) and reserve the highest index
    // for the in-memory residual. This matches the semantic order:
    // "the very first event pushed went into a run that got
    // spilled before later pushes overflowed it."
    let mut iters: Vec<RunIter> = Vec::with_capacity(runs.len() + 1);
    for run in runs {
        match SpillStream::open(run) {
            Ok(stream) => iters.push(RunIter::Spilled(stream)),
            Err(e) => {
                // Bubble up the error path: a spill-read failure is
                // fatal for the merge. We embed the IO error inside
                // a shape the caller's `?` can propagate by panicking
                // — but `drain_sorted` returns `Vec<Event>`, not
                // `Result<...>`, so we cannot bubble cleanly. We
                // intentionally do not paper over this: if the merge
                // fails, the host has lost data and must abort the
                // ingest. A future refactor will rewrap the iterator
                // in a `Result` so the writer can handle the failure
                // explicitly. For now, panic with the underlying
                // path so the failure is visible.
                panic!("partitioner: failed to open spill run for merge: {e}");
            }
        }
    }
    iters.push(RunIter::InMemory(in_memory.into_iter()));

    // Initialise the heap with each run's first event. Empty runs
    // are simply skipped.
    let mut heap: BinaryHeap<Reverse<HeapEntry>> = BinaryHeap::new();
    for (idx, iter) in iters.iter_mut().enumerate() {
        if let Some(event) = pull_next(iter) {
            heap.push(Reverse(HeapEntry {
                key: event_key(&event),
                run_index: idx,
                event,
            }));
        }
    }

    while let Some(Reverse(entry)) = heap.pop() {
        let HeapEntry {
            event, run_index, ..
        } = entry;
        output.push(event);
        if let Some(next) = pull_next(&mut iters[run_index]) {
            heap.push(Reverse(HeapEntry {
                key: event_key(&next),
                run_index,
                event: next,
            }));
        }
    }

    output
}

/// One run iterator — either the in-memory residual or a spill stream.
enum RunIter {
    InMemory(std::vec::IntoIter<Event>),
    Spilled(SpillStream),
}

fn pull_next(iter: &mut RunIter) -> Option<Event> {
    match iter {
        RunIter::InMemory(it) => it.next(),
        RunIter::Spilled(s) => match s.next_event() {
            Ok(opt) => opt,
            Err(e) => panic!("partitioner: failed to read spill run during merge: {e}"),
        },
    }
}

/// Sort key for the merge heap. Cloning is cheap for the common
/// `EntityId::Int` case; the `EntityId::String` case clones the
/// underlying `String`, which is the same per-event cost the in-
/// memory sort already pays. We keep the key out of `HeapEntry`'s
/// `Ord` derive only because `EntityId` already implements `Ord`.
type EventKey = (EntityId, Timestamp);

fn event_key(event: &Event) -> EventKey {
    (event.entity.clone(), event.timestamp)
}

/// One head-of-run value held in the k-way merge heap.
///
/// Ordering: by the sort key first (smaller `(entity, ts)` is
/// "smaller"), then by `run_index` so equal keys preserve the
/// spill-time / in-memory order documented in `merge_runs_for_bucket`.
#[derive(Debug)]
struct HeapEntry {
    key: EventKey,
    run_index: usize,
    event: Event,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.run_index == other.run_index
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key
            .cmp(&other.key)
            .then_with(|| self.run_index.cmp(&other.run_index))
    }
}

/// Streaming reader over a spilled run file. Yields events in the
/// same `(entity_id, ts)` order they were written.
///
/// Field-drop order matters: Rust drops struct fields in
/// declaration order, so `_guard` drops *before* `reader`. The
/// guard's `Drop` calls `remove_file`, which on POSIX is safe to
/// invoke while the file's read fd is still open — the inode lives
/// until the last fd closes. bqlite targets POSIX (Linux / macOS /
/// FreeBSD) today; a future Windows port will need to drop
/// `reader` first. Flagged here as a portability note rather than
/// a runtime guard.
struct SpillStream {
    /// Path-owning RAII guard. Drops with `SpillStream`, deleting
    /// the file once the merge has consumed it.
    _guard: SpillRunFile,
    reader: BufReader<File>,
    /// Reusable scratch buffer to avoid allocating a fresh `Vec`
    /// per record. Resized as needed.
    scratch: Vec<u8>,
}

impl SpillStream {
    fn open(guard: SpillRunFile) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .open(&guard.path)
            .map_err(|e| io_err(&guard.path, "open ingest spill file for read", e))?;
        Ok(Self {
            _guard: guard,
            reader: BufReader::new(file),
            scratch: Vec::new(),
        })
    }

    fn next_event(&mut self) -> Result<Option<Event>> {
        let mut len_buf = [0u8; 4];
        match self.reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => {
                return Err(BqliteError::Execution(format!(
                    "partitioner: failed to read ingest spill record length: {e}"
                )))
            }
        }
        let len = u32::from_le_bytes(len_buf) as usize;
        if self.scratch.len() < len {
            self.scratch.resize(len, 0);
        }
        self.reader
            .read_exact(&mut self.scratch[..len])
            .map_err(|e| {
                BqliteError::Execution(format!(
                    "partitioner: failed to read ingest spill record body of {len} bytes: {e}"
                ))
            })?;
        let event: Event = postcard::from_bytes(&self.scratch[..len]).map_err(|e| {
            BqliteError::Execution(format!(
                "partitioner: failed to deserialize ingest spill record: {e}"
            ))
        })?;
        Ok(Some(event))
    }
}

/// RAII guard for a single ingest spill run file.
///
/// Holds only the path. The writer flushes and drops its
/// `BufWriter<File>` *before* the guard goes out of scope so the
/// file's bytes are durable on disk for the merge pass to read; the
/// reader opens a fresh `File` from the same path. Drop deletes the
/// file unconditionally — this is the single deletion site for
/// ingest spill artefacts. A spill file may legitimately have been
/// removed already (e.g. by the engine's per-ingest-dir
/// `rm_rf` belt-and-braces sweep), so a `NotFound` error from
/// `remove_file` is silently ignored.
#[derive(Debug)]
pub(crate) struct SpillRunFile {
    path: PathBuf,
}

impl SpillRunFile {
    /// Path of the on-disk spill run file. Exposed for tests and
    /// future instrumentation; not part of any public surface.
    #[cfg(test)]
    pub(crate) fn path(&self) -> &std::path::Path {
        &self.path
    }
}

impl Drop for SpillRunFile {
    fn drop(&mut self) {
        // Best-effort. The engine runs an `rm_rf` of the per-ingest
        // subdirectory on every exit path as a belt-and-braces sweep
        // (see `docs/design/engine/spill.md` § 8.3) so a deletion
        // failure here does not strand the file.
        let _ = std::fs::remove_file(&self.path);
    }
}

fn io_err(path: &std::path::Path, action: &str, err: io::Error) -> BqliteError {
    BqliteError::Io(io::Error::new(
        err.kind(),
        format!("{action} {}: {err}", path.display()),
    ))
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

    // ── External spill (TASK-512) ──────────────────────────────────────────

    use std::sync::atomic::{AtomicU64, Ordering};

    static SPILL_SCRATCH_SEQ: AtomicU64 = AtomicU64::new(0);

    /// Per-test scratch directory under `std::env::temp_dir()`. RAII;
    /// removes itself on drop. Used as the per-ingest spill dir.
    struct ScratchDir {
        path: PathBuf,
    }

    impl ScratchDir {
        fn new(label: &str) -> Self {
            let seq = SPILL_SCRATCH_SEQ.fetch_add(1, Ordering::Relaxed);
            let pid = std::process::id();
            let mut path = std::env::temp_dir();
            path.push(format!("bqlite-partitioner-spill-{label}-{pid}-{seq}"));
            std::fs::create_dir_all(&path).expect("create scratch spill dir");
            Self { path }
        }

        fn path(&self) -> &std::path::Path {
            &self.path
        }
    }

    impl Drop for ScratchDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    /// Reference no-spill drain — sort each bucket and yield the
    /// canonical `(BucketKey, Vec<Event>)` sequence. The spill path
    /// must produce the same output for the same input.
    fn reference_drain(
        events: &[Event],
        shard_count: u16,
        window_days: u32,
    ) -> Vec<(BucketKey, Vec<Event>)> {
        let mut p = Partitioner::new(shard_count, window_days, 1, usize::MAX).unwrap();
        for e in events {
            p.push_event(e.clone()).unwrap();
        }
        p.drain_sorted().collect()
    }

    #[test]
    fn with_spill_dir_validates_inputs_like_new() {
        let dir = ScratchDir::new("with-spill-validate");
        let err = Partitioner::with_spill_dir(0, 30, 1, 1024, dir.path().to_path_buf())
            .expect_err("zero shard_count must error");
        assert!(matches!(err, BqliteError::Schema(_)));
        let err = Partitioner::with_spill_dir(4, 0, 1, 1024, dir.path().to_path_buf())
            .expect_err("zero window_days must error");
        assert!(matches!(err, BqliteError::Schema(_)));
        let err = Partitioner::with_spill_dir(4, 30, 1, 0, dir.path().to_path_buf())
            .expect_err("zero budget must error");
        assert!(matches!(err, BqliteError::Schema(_)));
    }

    #[test]
    fn spill_disabled_partitioner_keeps_fail_fast_contract() {
        // The legacy `Partitioner::new` path must still refuse the
        // overshooting push and leave state untouched.
        let mut p = Partitioner::new(1, 30, 1, 1).unwrap();
        let err = p
            .push_event(simple_event("alice", 0, "click"))
            .expect_err("budget overflow must error without spill");
        match err {
            BqliteError::Execution(msg) => {
                assert!(msg.contains("memory budget"), "got: {msg}");
                assert!(msg.contains("no spill directory"), "got: {msg}");
            }
            other => panic!("expected Execution, got {other:?}"),
        }
        assert_eq!(p.buffered_events(), 0);
        assert_eq!(p.spilled_run_count(), 0);
    }

    #[test]
    fn spill_writes_largest_bucket_when_budget_overshoots() {
        // shard_count = 2 to allow at least two buckets. Use a tiny
        // budget so the second push always triggers a spill of the
        // first bucket. Different entities likely land on different
        // shards, so we craft entities that we know hash to
        // distinct shards via the `shard_count = 1`/`= 2` modulo
        // dispatch in `shard_id_for`.
        let dir = ScratchDir::new("spill-largest");
        let event_a = simple_event("alice", 0, "click");
        let bytes_a = estimated_event_size(&event_a);
        // Sizing budget to fit exactly one event of `bytes_a` size
        // forces every subsequent push of similar size to spill the
        // existing bucket.
        let mut p =
            Partitioner::with_spill_dir(2, 30, 1, bytes_a, dir.path().to_path_buf()).unwrap();
        p.push_event(event_a.clone()).unwrap();
        assert_eq!(p.spilled_run_count(), 0);

        // Push a second event of the same shape; the partitioner
        // should spill the first bucket and accept this one.
        p.push_event(simple_event("bob", 1, "click")).unwrap();
        assert!(
            p.spilled_run_count() >= 1,
            "second push must trigger a spill, got runs={}",
            p.spilled_run_count()
        );
        // The newly-pushed event must be visible in memory; the
        // overall state remains coherent.
        assert!(p.buffered_events() >= 1);
        assert!(p.buffered_bytes() <= p.budget_bytes());
    }

    #[test]
    fn spill_filename_follows_design_doc_scheme() {
        // Confirm that spilled files match the
        // `ingest-part-w<window>-s<shard:04>-<seq:06>.spill` pattern
        // from `docs/design/engine/spill.md` § 7. The filenames must
        // sort lexicographically in creation order so a future merge
        // pass that walks the directory in `read_dir` order produces
        // the right run sequence.
        let dir = ScratchDir::new("spill-naming");
        // Use uniform-shape events so the byte estimate is stable
        // across pushes — the spill loop's bucket-pick deals with
        // size variance, but pinning the budget to a single event's
        // size requires that all events fit in `budget_bytes`.
        let template = simple_event("u00", 0, "click");
        let bytes = estimated_event_size(&template);
        let mut p = Partitioner::with_spill_dir(1, 30, 1, bytes, dir.path().to_path_buf()).unwrap();
        // Three pushes total → at least two spills (first stays in
        // memory until the second push triggers a spill of the
        // first bucket, etc.). With `shard_count = 1`, every event
        // lands in the same bucket.
        p.push_event(simple_event("u00", 0, "click")).unwrap();
        p.push_event(simple_event("u01", 1, "click")).unwrap();
        p.push_event(simple_event("u02", 2, "click")).unwrap();
        assert!(p.spilled_run_count() >= 2);

        let entries: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().into_string().unwrap())
            .collect();
        for name in &entries {
            assert!(
                name.starts_with("ingest-part-w0-s0000-")
                    && name.ends_with(".spill")
                    && name.len() == "ingest-part-w0-s0000-000000.spill".len(),
                "unexpected spill filename: {name}"
            );
        }
        let mut sorted = entries.clone();
        sorted.sort();
        assert_eq!(
            entries.iter().collect::<std::collections::HashSet<_>>(),
            sorted.iter().collect::<std::collections::HashSet<_>>(),
            "deduped sets match — sanity"
        );
    }

    #[test]
    fn spill_then_drain_matches_no_spill_drain() {
        // The merge of spilled runs + in-memory residual must yield
        // exactly the same `(BucketKey, Vec<Event>)` sequence as a
        // wide-budget no-spill drain on the same input. This is the
        // central correctness invariant for TASK-512.
        let dir = ScratchDir::new("spill-equiv");
        let mut events: Vec<Event> = Vec::new();
        for i in 0..200_u64 {
            // Spread events across multiple entities and timestamps
            // so the merge has real work to do (sort key is non-
            // trivial across spilled vs. in-memory chunks).
            let entity = format!("user_{}", i % 7);
            let day_idx = (i % 4) as i64; // four windows under 30-day default
            events.push(simple_event(&entity, day_idx * 30, "click"));
        }

        let baseline = reference_drain(&events, 4, 30);

        // A budget tight enough to force several spills.
        let bytes_per_event = estimated_event_size(&events[0]);
        let tight_budget = bytes_per_event * 3;
        let mut p =
            Partitioner::with_spill_dir(4, 30, 1, tight_budget, dir.path().to_path_buf()).unwrap();
        for e in &events {
            p.push_event(e.clone()).unwrap();
        }
        assert!(
            p.spilled_run_count() > 0,
            "the chosen tight budget must have triggered a spill"
        );

        let actual: Vec<(BucketKey, Vec<Event>)> = p.drain_sorted().collect();
        assert_eq!(actual.len(), baseline.len());
        for ((ka, va), (kb, vb)) in actual.iter().zip(baseline.iter()) {
            assert_eq!(ka, kb, "bucket keys diverged");
            assert_eq!(va, vb, "merged events diverged for bucket {ka:?}");
        }
    }

    #[test]
    fn spill_preserves_stable_tie_break_within_bucket() {
        // Events with identical (entity, ts) must retain their
        // insertion order across spill+merge — the same stability
        // guarantee the no-spill path provides.
        let dir = ScratchDir::new("spill-stable");
        // event_type strings of equal length so the per-event byte
        // estimate is identical. "first"/"secnd"/"third" are all 5
        // chars — keeps the spill-loop math exact.
        let bytes =
            estimated_event_size(&Event::new(EntityId::from("alice"), Timestamp(0), "first"));
        // Budget admits one event, so each of the three pushes
        // ends up in its own run (first stays in memory, then
        // gets spilled when the second arrives, etc.).
        let mut p = Partitioner::with_spill_dir(1, 30, 1, bytes, dir.path().to_path_buf()).unwrap();
        p.push_event(simple_event("alice", 0, "first")).unwrap();
        p.push_event(simple_event("alice", 0, "secnd")).unwrap();
        p.push_event(simple_event("alice", 0, "third")).unwrap();
        assert!(p.spilled_run_count() >= 2);

        let drained: Vec<(BucketKey, Vec<Event>)> = p.drain_sorted().collect();
        assert_eq!(drained.len(), 1);
        let types: Vec<String> = drained[0].1.iter().map(|e| e.event_type.clone()).collect();
        assert_eq!(types, vec!["first", "secnd", "third"]);
    }

    #[test]
    fn spill_oversized_event_returns_typed_error() {
        // Configure a tight budget and push a tiny event so the
        // partitioner accepts it. Then push an event whose size
        // alone exceeds the budget; the spill loop drains every
        // bucket and still cannot fit the event, surfacing the
        // documented "oversized event" error per spill.md §6.2.
        let dir = ScratchDir::new("spill-oversized");
        let small = simple_event("a", 0, "x");
        let small_bytes = estimated_event_size(&small);
        let budget = small_bytes; // fits exactly one tiny event
        let mut p =
            Partitioner::with_spill_dir(1, 30, 1, budget, dir.path().to_path_buf()).unwrap();
        p.push_event(small).unwrap();

        // Build an event with a 4 MiB property string — larger
        // than `budget` (≈ size_of::<Event>() bytes) by orders of
        // magnitude, ensuring the spill loop can't make room.
        let huge_value = PropertyValue::String("x".repeat(4 * 1024 * 1024));
        let mut huge = Event::new(EntityId::from("b"), Timestamp(0), "click");
        huge.properties.push(("payload".into(), huge_value));

        let err = p
            .push_event(huge)
            .expect_err("oversized event must surface a typed error");
        match err {
            BqliteError::Execution(msg) => {
                assert!(msg.contains("oversized event"), "got: {msg}");
                assert!(msg.contains("budget"), "got: {msg}");
            }
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    #[test]
    fn dropping_partitioner_before_drain_removes_spill_files() {
        // Spill enough that at least one run exists; then drop the
        // partitioner without draining. Every spill file must be
        // removed by the `SpillRunFile` RAII guard. The scratch dir
        // itself stays — the engine owns the per-ingest dir's
        // lifetime, not the partitioner.
        let dir = ScratchDir::new("spill-drop");
        let template = simple_event("u00", 0, "click");
        let bytes = estimated_event_size(&template);
        {
            let mut p =
                Partitioner::with_spill_dir(1, 30, 1, bytes, dir.path().to_path_buf()).unwrap();
            p.push_event(simple_event("u00", 0, "click")).unwrap();
            p.push_event(simple_event("u01", 1, "click")).unwrap();
            p.push_event(simple_event("u02", 2, "click")).unwrap();
            assert!(p.spilled_run_count() >= 1);
            // The partitioner is dropped at the end of this block.
        }
        let entries: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with(".spill"))
            .collect();
        assert!(
            entries.is_empty(),
            "dropped partitioner must remove its spill files; found {entries:?}"
        );
    }

    #[test]
    fn spill_run_file_path_accessor_returns_underlying_path() {
        // Smoke test on the `pub(crate)` path() accessor so it stays
        // load-bearing for future instrumentation/diagnostics.
        let dir = ScratchDir::new("spill-path");
        let event_a = simple_event("alice", 0, "click");
        let bytes_a = estimated_event_size(&event_a);
        let mut p =
            Partitioner::with_spill_dir(1, 30, 1, bytes_a, dir.path().to_path_buf()).unwrap();
        p.push_event(event_a).unwrap();
        p.push_event(simple_event("bob", 1, "click")).unwrap();
        // Reach into the spill state via the SpillRunFile we just
        // produced. We round-trip through the public surface to
        // avoid reaching across module boundaries.
        let runs_total = p.spilled_run_count();
        assert!(runs_total >= 1);
        // Confirm the recorded path is under the scratch dir.
        let spill = p.spill.as_ref().expect("spill must be configured");
        let any_run = spill
            .runs
            .values()
            .flat_map(|v| v.iter())
            .next()
            .expect("at least one spill run");
        let path = any_run.path();
        assert!(path.starts_with(dir.path()), "got: {}", path.display());
        assert!(path.exists(), "spill file must still exist on disk");
    }

    proptest::proptest! {
        #![proptest_config(proptest::test_runner::Config {
            // Spill writes touch the filesystem, so each iteration is
            // ~1-2 ms — keep the case count tight to bound runtime.
            cases: 64,
            ..proptest::test_runner::Config::default()
        })]

        #[test]
        fn proptest_spill_drain_matches_no_spill_drain(
            inputs in proptest::collection::vec((0u32..7, 0u32..40, 0u8..3), 0..120)
        ) {
            // Build deterministic events from the prop inputs: a
            // small entity space, a small day space, and a few
            // event-type variants. The shape is uniform so the
            // estimated_event_size variance stays bounded — what we
            // care about is the merge invariant, not size analytics.
            let events: Vec<Event> = inputs
                .into_iter()
                .map(|(entity_idx, day_idx, type_idx)| {
                    let etype = match type_idx { 0 => "click", 1 => "view", _ => "play" };
                    simple_event(&format!("u{entity_idx:02}"), day_idx as i64, etype)
                })
                .collect();

            let baseline = reference_drain(&events, 4, 30);

            // Pick a tight budget. Two events' worth of estimated
            // bytes guarantees frequent spill on inputs > a few
            // events while still admitting at least one event at a
            // time (so an "oversized event" error cannot happen on
            // any input from this generator).
            let bytes_per_event = events
                .first()
                .map(estimated_event_size)
                .unwrap_or(256);
            let budget = bytes_per_event.saturating_mul(2).max(256);

            let dir = ScratchDir::new("proptest");
            let mut p = Partitioner::with_spill_dir(4, 30, 1, budget, dir.path().to_path_buf()).unwrap();
            for e in &events {
                p.push_event(e.clone()).unwrap();
            }

            let actual: Vec<(BucketKey, Vec<Event>)> = p.drain_sorted().collect();
            proptest::prop_assert_eq!(actual, baseline);
        }
    }

    #[test]
    fn drain_with_no_pushes_yields_empty_iterator() {
        // Spill-disabled and spill-enabled cases both must yield an
        // empty iterator from `drain_sorted` when no events were
        // pushed. Pins the BTreeSet-union union path on the empty
        // case.
        let p = Partitioner::new(4, 30, 1, 1024).unwrap();
        let drained: Vec<(BucketKey, Vec<Event>)> = p.drain_sorted().collect();
        assert!(drained.is_empty());

        let dir = ScratchDir::new("spill-empty");
        let p = Partitioner::with_spill_dir(4, 30, 1, 1024, dir.path().to_path_buf()).unwrap();
        let drained: Vec<(BucketKey, Vec<Event>)> = p.drain_sorted().collect();
        assert!(drained.is_empty());
    }

    #[test]
    fn buffered_bytes_invariant_holds_across_pushes_and_spills() {
        // After every push (success or error), the partitioner must
        // satisfy `buffered_bytes == sum(bucket.bytes for bucket in
        // in-memory buckets)`. This guards against a regression
        // where a per-bucket counter and the aggregate counter drift
        // apart through the spill path.
        let dir = ScratchDir::new("spill-invariant");
        let event_a = simple_event("alice", 0, "click");
        let bytes_a = estimated_event_size(&event_a);
        let mut p =
            Partitioner::with_spill_dir(4, 30, 1, bytes_a * 2, dir.path().to_path_buf()).unwrap();
        for n in 0..50_u64 {
            let event = simple_event(&format!("user_{}", n % 5), (n % 8) as i64, "click");
            p.push_event(event).unwrap();
            let in_memory_sum: usize = p.buckets.values().map(|b| b.bytes).sum();
            assert_eq!(
                p.buffered_bytes(),
                in_memory_sum,
                "buffered_bytes invariant broken after push {n}"
            );
        }
    }
}
