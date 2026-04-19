//! Database — the bqlite storage entry point.
//!
//! [`Database::create`] initializes a fresh database directory with
//! an empty manifest and exclusive lock. [`Database::open`] opens an
//! existing database, validating its manifest and acquiring the lock.
//! These replace the Wave 1 [`Database::open_or_create`] (deprecated)
//! to align with `query-language.md` §29 ("Database init | CLI-only
//! (`bqlite init`)").
//!
//! # On-disk layout
//!
//! ```text
//! <root>/
//!   .lock                ← exclusive flock, held for the lifetime of Database
//!   manifest.json        ← serialized [`crate::manifest::Manifest`]
//!   manifest.json.tmp    ← (transient) written-and-fsynced during atomic update
//! ```
//!
//! Wave 1 consolidates what `storage-format.md` §5.2 + §12.1 split
//! into `db.json` plus per-table `<table>/manifest.json` into a single
//! top-level `manifest.json`. [`crate::manifest`] documents the split
//! and the forward-compat plan.
//!
//! # Atomicity
//!
//! `manifest.json` updates follow `storage-format.md` §12.3: write to
//! `manifest.json.tmp`, `fsync` the file, `rename` over the old
//! manifest, then best-effort `fsync` the parent directory so the
//! rename is durable. The rename is atomic on POSIX (`rename(2)`),
//! which is the only target platform for v1.
//!
//! On open, any stray `manifest.json.tmp` from a crash mid-rename is
//! deleted as a best-effort cleanup — the rename's atomicity
//! guarantees that either the old or the new manifest is fully
//! present, so the stale temp file carries no information.
//!
//! After the manifest is loaded, the open path runs the TASK-239
//! orphan-segment reconciliation pass
//! ([`crate::segment::cleanup::reconcile_segments`]), which sweeps
//! `.tmp` segment files from crashed ingests/compactions and removes
//! `.seg` files not referenced by the manifest (deferred compaction
//! deletes, dropped-table remnants). See `storage-format.md` §7.4.
//!
//! # Concurrency
//!
//! Both [`Database::create`] and [`Database::open`] acquire an
//! exclusive lock on `<root>/.lock` via `std::fs::File::try_lock`
//! (stabilized in Rust 1.89). A second concurrent open from the same
//! process or from a different process returns
//! [`BqliteError::Execution`] with a clear message. The lock releases
//! when the owning [`Database`] is dropped. See
//! `storage-format.md` §14.1.
//!
//! # Future extensions
//!
//! - Per-table manifests (later wave) — the open path learns to walk
//!   a `tables/` subdirectory instead of the single `manifest.json`.
//! - Format-version migration — a later wave reads older manifests
//!   and upgrades them in-place.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use bqlite_core::error::{BqliteError, Result};
use bqlite_core::schema::{ColumnDef, TableSchema};
use bqlite_core::storage::{
    ColumnProjection, Predicate, SegmentHandle, SegmentReader, SegmentScan,
};
use bqlite_core::time::TimeRange;

use crate::catalog::ManifestCatalog;
use crate::manifest::{
    Manifest, SegmentMeta, TableEntry, WindowManifest, DEFAULT_SHARD_COUNT, MANIFEST_FORMAT_VERSION,
};
use crate::segment::cleanup;
use crate::segment::reader::SegmentFileReader;

/// Name of the advisory lock file at the database root.
pub const LOCK_FILE_NAME: &str = ".lock";

/// Name of the serialized manifest file at the database root.
pub const MANIFEST_FILE_NAME: &str = "manifest.json";

/// Name of the transient file written during an atomic manifest update.
pub const MANIFEST_TMP_FILE_NAME: &str = "manifest.json.tmp";

/// An open bqlite database.
///
/// Owns the exclusive advisory lock on the database directory and
/// holds an in-memory snapshot of the manifest. Reads go through
/// [`Database::manifest`]; segment-level mutations go through
/// [`Database::add_segment`] / [`Database::remove_segment`], which
/// clone-apply-persist-swap via a private atomic update helper so
/// every mutation is crash-safe. Query-time segment enumeration uses
/// [`Database::snapshot_for_query`], which borrows from the current
/// snapshot.
///
/// Dropping a `Database` releases the lock so a subsequent open (in
/// the same process or another) can succeed.
#[derive(Debug)]
pub struct Database {
    root: PathBuf,
    manifest: Manifest,
    /// Exclusive advisory lock on `<root>/.lock`. The field is
    /// prefixed with `_` because nothing reads it directly — its only
    /// job is to hold the lock for the lifetime of the `Database`.
    _lock: File,
    /// Per-`(table, window_id, shard_id)` tombstone-write mutex pool.
    ///
    /// Implements `docs/design/storage/deletes.md` §9: DELETE writes
    /// to a shard's `tombstones.json` hold this lock for the duration
    /// of the read-modify-write cycle. DELETEs to different shards
    /// proceed in parallel because each `(table, window, shard)` key
    /// has its own `Arc<Mutex<()>>`.
    ///
    /// The outer `Mutex<HashMap<...>>` only guards the lazy-insert of
    /// new entries; once an `Arc<Mutex<()>>` is published, callers
    /// hold the inner lock without contending on the outer mutex.
    /// Today every mutation funnels through `&mut Database`, so the
    /// inner locks are uncontested no-ops; the field is here to keep
    /// the public contract honest with the spec and to avoid a silent
    /// correctness break the moment any future task introduces
    /// shared-handle execution.
    ///
    /// Multi-process serialization (SS9.2) remains deferred — would
    /// require promoting these to `flock` on the tombstone files.
    tombstone_locks: TombstoneLockPool,
}

/// Lazy pool of per-`(table, window_id, shard_id)` tombstone-write
/// locks. See [`Database::tombstone_locks`].
type TombstoneLockPool = Mutex<HashMap<(String, u32, u16), Arc<Mutex<()>>>>;

impl Database {
    /// Initialize a fresh database at `path` with [`DEFAULT_SHARD_COUNT`].
    ///
    /// Equivalent to [`Database::create_with_shards`] with the default
    /// shard count. The returned `Database` is already open and
    /// locked — callers do not need a follow-up [`Database::open`].
    ///
    /// # Errors
    ///
    /// - [`BqliteError::Execution`] if `path` already contains a
    ///   manifest (use [`Database::open`] instead).
    /// - [`BqliteError::Io`] on I/O failures creating the directory,
    ///   opening the lock file, or writing the manifest.
    /// - [`BqliteError::Execution`] if the database directory is
    ///   already locked by another process.
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Self::create_with_shards(path, DEFAULT_SHARD_COUNT)
    }

    /// Initialize a fresh database at `path` with a custom shard count.
    ///
    /// Creates the directory (recursively if needed), writes an empty
    /// manifest with zero tables, and acquires the exclusive lock.
    /// The returned `Database` is ready for [`Database::create_table`]
    /// calls — no bootstrap `events` table is seeded (TASK-240).
    ///
    /// # Errors
    ///
    /// - [`BqliteError::Schema`] if `shard_count == 0`.
    /// - [`BqliteError::Execution`] if `path` already contains a
    ///   manifest — the caller should use [`Database::open`] instead.
    /// - [`BqliteError::Io`] on I/O failures.
    /// - [`BqliteError::Execution`] if the directory is already locked.
    pub fn create_with_shards(path: impl AsRef<Path>, shard_count: u16) -> Result<Self> {
        if shard_count == 0 {
            return Err(BqliteError::Schema("shard_count must be at least 1".into()));
        }

        let root = path.as_ref().to_path_buf();
        fs::create_dir_all(&root).map_err(|e| io_ctx("create database directory", &root, e))?;

        let lock = acquire_lock(&root)?;

        // A manifest already on disk means someone initialized this
        // directory before — error out so callers don't accidentally
        // re-initialize an existing database.
        let manifest_path = root.join(MANIFEST_FILE_NAME);
        if manifest_path.exists() {
            return Err(BqliteError::Execution(format!(
                "cannot create database at {}: manifest already exists (use Database::open to open an existing database)",
                root.display()
            )));
        }

        // Clean up any stale tmp file from a prior crashed init.
        clean_stale_tmp(&root);

        let manifest = Manifest::new_empty(shard_count);
        write_manifest_atomic(&root, &manifest)?;

        Ok(Self {
            root,
            manifest,
            _lock: lock,
            tombstone_locks: Mutex::new(HashMap::new()),
        })
    }

    /// Open an existing database at `path`.
    ///
    /// Reads and validates the manifest, acquires the exclusive lock,
    /// and runs the orphan-segment reconciliation pass. Errors if the
    /// directory has no manifest — callers should use
    /// [`Database::create`] (or `bqlite init`) to initialize a fresh
    /// database first.
    ///
    /// # Errors
    ///
    /// - [`BqliteError::Execution`] if the directory has no manifest,
    ///   with a message pointing the caller at `bqlite init`.
    /// - [`BqliteError::Execution`] if the manifest is corrupt or has
    ///   an unsupported `format_version`.
    /// - [`BqliteError::Execution`] if the database is already locked
    ///   by another process.
    /// - [`BqliteError::Io`] on I/O failures.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let root = path.as_ref().to_path_buf();

        if !root.exists() {
            return Err(BqliteError::Execution(format!(
                "database not found at {}: directory does not exist (run `bqlite init` to create a new database)",
                root.display()
            )));
        }

        let lock = acquire_lock(&root)?;

        // Clean up stale tmp files from prior crashes.
        clean_stale_tmp(&root);

        let manifest_path = root.join(MANIFEST_FILE_NAME);
        let manifest = match fs::read(&manifest_path) {
            Ok(bytes) => {
                let m: Manifest = serde_json::from_slice(&bytes).map_err(|e| {
                    BqliteError::Execution(format!(
                        "corrupted manifest at {}: {}",
                        manifest_path.display(),
                        e
                    ))
                })?;
                if !m.is_supported_version() {
                    return Err(BqliteError::Execution(format!(
                        "manifest at {} has unsupported format_version {} (this build reads format_version {})",
                        manifest_path.display(),
                        m.format_version,
                        MANIFEST_FORMAT_VERSION
                    )));
                }
                m
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                return Err(BqliteError::Execution(format!(
                    "database not initialized at {}: no manifest found (run `bqlite init` to create a new database)",
                    root.display()
                )));
            }
            Err(e) => {
                return Err(io_ctx("read manifest", &manifest_path, e));
            }
        };

        // Reconcile on-disk segment files against the manifest.
        let _report = cleanup::reconcile_segments(&root, &manifest)?;

        Ok(Self {
            root,
            manifest,
            _lock: lock,
            tombstone_locks: Mutex::new(HashMap::new()),
        })
    }

    /// Absolute path of the database root directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// In-memory snapshot of the current manifest.
    ///
    /// The borrow reflects the latest committed state — Wave 2
    /// mutation methods ([`Database::add_segment`],
    /// [`Database::remove_segment`]) swap `self.manifest` for the
    /// updated clone only after a successful atomic disk write, so a
    /// borrow taken before a mutation is stale the moment the
    /// mutation commits (and is not held across `&mut self` calls
    /// anyway — the borrow checker enforces that).
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// Return a [`Catalog`](bqlite_core::Catalog) view of this
    /// database's tables.
    ///
    /// The returned [`ManifestCatalog`] borrows `self`, so it lives
    /// only as long as the borrow. Callers that need a trait object
    /// can coerce:
    ///
    /// ```ignore
    /// let cat = db.catalog();
    /// let dyn_cat: &dyn bqlite_core::Catalog = &cat;
    /// planner.plan(&stmt, dyn_cat)?;
    /// ```
    ///
    /// The catalog always reflects the manifest snapshot taken when
    /// the database was opened; Wave 1 has no mutation API, so the
    /// snapshot is effectively static for the database's lifetime.
    pub fn catalog(&self) -> ManifestCatalog<'_> {
        ManifestCatalog::new(&self.manifest)
    }

    /// Open a manifest-backed segment reader for `table_name`.
    ///
    /// Returns [`BqliteError::Plan`] if no such table exists in the
    /// manifest (via [`bqlite_core::catalog::unknown_table_error`] so
    /// the error format matches the `Catalog` trait's).
    ///
    /// The returned reader enumerates every live segment for the table
    /// from the current manifest snapshot. `open_segment` resolves
    /// handles to on-disk segment files via
    /// [`SegmentFileReader::open`]. Schema-evolution backfill (NULL
    /// for columns added after a segment was written) is handled by
    /// `SegmentFileReader` itself.
    pub fn segment_reader(&self, table_name: &str) -> Result<Box<dyn SegmentReader>> {
        self.segment_reader_for_time_range(table_name, TimeRange::unbounded())
    }

    /// Open a manifest-backed segment reader whose visible segment set
    /// is restricted to `time_range`.
    ///
    /// The table schema still reflects the table's full current schema;
    /// only the visible segment inventory is filtered.
    pub fn segment_reader_for_time_range(
        &self,
        table_name: &str,
        time_range: TimeRange,
    ) -> Result<Box<dyn SegmentReader>> {
        let entry = self
            .manifest
            .tables
            .get(table_name)
            .ok_or_else(|| bqlite_core::catalog::unknown_table_error(table_name))?;

        let windows = if time_range == TimeRange::unbounded() {
            entry.windows.clone()
        } else {
            filter_windows_by_time_range(&entry.windows, time_range)
        };

        Ok(Box::new(ManifestSegmentReader {
            root: self.root.clone(),
            table_name: table_name.to_string(),
            schema: Arc::new(entry.schema.clone()),
            windows,
        }))
    }

    /// Register a newly written segment in the on-disk manifest and
    /// update the in-memory snapshot atomically.
    ///
    /// Delegates the in-memory mutation to
    /// [`Manifest::add_segment`] and persists the result through the
    /// same `manifest.json.tmp → fsync → rename` path the open code
    /// uses on init. If the in-memory mutation errors, nothing is
    /// written and the in-memory snapshot is untouched.
    ///
    /// See [`Manifest::add_segment`] for the error taxonomy.
    pub fn add_segment(
        &mut self,
        table_name: &str,
        window_id: u32,
        shard_id: u32,
        segment: SegmentMeta,
    ) -> Result<()> {
        self.update_manifest(|m| m.add_segment(table_name, window_id, shard_id, segment))
    }

    /// Remove a segment from the on-disk manifest and return the
    /// removed [`SegmentMeta`].
    ///
    /// Delegates to [`Manifest::remove_segment`] and persists the
    /// result atomically. The removed meta is returned so callers
    /// (e.g. compaction, batch delete) can reap the underlying
    /// segment file once the manifest update is durable.
    ///
    /// See [`Manifest::remove_segment`] for the error taxonomy.
    pub fn remove_segment(&mut self, table_name: &str, segment_id: u64) -> Result<SegmentMeta> {
        self.update_manifest(|m| m.remove_segment(table_name, segment_id))
    }

    /// Read-only snapshot of segments matching `(table_name,
    /// time_range, shard_id)`, computed against the current
    /// in-memory manifest.
    ///
    /// This is a pure forwarding wrapper around
    /// [`Manifest::snapshot_for_query`]; it does not touch disk.
    pub fn snapshot_for_query(
        &self,
        table_name: &str,
        time_range: TimeRange,
        shard_id: u32,
    ) -> Result<Vec<SegmentMeta>> {
        self.manifest
            .snapshot_for_query(table_name, time_range, shard_id)
    }

    /// Return the per-shard tombstone-write `Mutex<()>` for
    /// `(table, window_id, shard_id)`, lazily creating one on first
    /// access.
    ///
    /// Implements the per-shard serialization contract from
    /// `docs/design/storage/deletes.md` §9. Callers acquire the
    /// returned `Arc<Mutex<()>>` for the duration of one tombstone
    /// read-modify-write cycle. Different `(window, shard)` keys hold
    /// independent locks so DELETEs to disjoint shards proceed in
    /// parallel.
    ///
    /// The outer guard on the lock pool is held only briefly during
    /// the lazy insert — once an entry exists, repeat callers receive
    /// the same `Arc` clone without re-locking the pool.
    pub fn tombstone_shard_lock(
        &self,
        table: &str,
        window_id: u32,
        shard_id: u16,
    ) -> Arc<Mutex<()>> {
        let mut pool = self
            .tombstone_locks
            .lock()
            .expect("tombstone lock pool poisoned");
        pool.entry((table.to_string(), window_id, shard_id))
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    /// Load a frozen tombstone snapshot for the given `(window_id,
    /// shard_id)` pairs.
    ///
    /// Called once at query bind time alongside the manifest snapshot
    /// to establish complete visibility state for a query. See
    /// `docs/design/storage/deletes.md` SS6.
    ///
    /// # Errors
    ///
    /// - [`BqliteError::Execution`] if `table_name` is not registered
    ///   in the manifest.
    /// - [`BqliteError::Io`] or [`BqliteError::Execution`] if a
    ///   tombstone file is present but corrupt.
    pub fn load_tombstone_snapshot(
        &self,
        table_name: &str,
        targets: &[(u32, u16)],
    ) -> Result<crate::tombstone::TombstoneSnapshot> {
        if !self.manifest.tables.contains_key(table_name) {
            return Err(BqliteError::Execution(format!(
                "load_tombstone_snapshot: unknown table '{table_name}'"
            )));
        }
        crate::tombstone::load_tombstone_snapshot(&self.root, table_name, targets)
    }

    /// Atomically reserve a fresh `batch_id` for an ingest call on
    /// `table_name`.
    ///
    /// Bumps the table entry's persisted `next_batch_id` counter by
    /// one and returns the value the counter held *before* the
    /// bump — i.e. the id the caller should stamp onto every
    /// segment it writes in this batch.
    ///
    /// Used by the ingest partitioner (`TASK-218`) at construction
    /// time so each partitioner instance carries its own durable,
    /// monotonic id. Per `docs/design/storage-format.md` §6.2, a
    /// gap in the batch-id sequence is acceptable — a counter bump
    /// that succeeds but is followed by a failed ingest simply
    /// retires the id, and the next ingest starts from a higher
    /// value.
    ///
    /// # Errors
    ///
    /// - [`BqliteError::Execution`] if `table_name` is not
    ///   registered in the manifest.
    /// - [`BqliteError::Execution`] if the counter would overflow
    ///   `u64::MAX` — astronomically unreachable at real ingest
    ///   rates, but the check is here for completeness.
    /// - Any I/O error from the atomic manifest write propagates
    ///   unchanged.
    pub fn allocate_batch_id(&mut self, table_name: &str) -> Result<u64> {
        self.update_manifest(|m| {
            let entry = m.tables.get_mut(table_name).ok_or_else(|| {
                BqliteError::Execution(format!(
                    "allocate_batch_id: unknown table '{table_name}'"
                ))
            })?;
            let issued = entry.next_batch_id;
            entry.next_batch_id = issued.checked_add(1).ok_or_else(|| {
                BqliteError::Execution(format!(
                    "allocate_batch_id: batch_id counter for table '{table_name}' would overflow u64"
                ))
            })?;
            Ok(issued)
        })
    }

    /// Atomically reserve a fresh `segment_id` for a segment about to
    /// land on `table_name`.
    ///
    /// Bumps the table entry's persisted `next_segment_id` counter by
    /// one and returns the value the counter held *before* the bump —
    /// i.e. the id the caller should stamp onto the file it is about
    /// to write. `segment_<segment_id>.seg` is the filename format per
    /// `docs/design/storage-format.md` §5.2; uniqueness within a table
    /// is required, which this counter guarantees.
    ///
    /// Like [`Self::allocate_batch_id`], a counter bump that is
    /// followed by a failed write simply retires the id — a gap in
    /// the segment-id sequence is acceptable per §6.2's gap-tolerance
    /// rule.
    ///
    /// # Errors
    ///
    /// - [`BqliteError::Execution`] if `table_name` is not registered
    ///   in the manifest.
    /// - [`BqliteError::Execution`] if the counter would overflow
    ///   `u64::MAX` — astronomically unreachable at realistic ingest
    ///   rates, but guarded for completeness.
    /// - Any I/O error from the atomic manifest write propagates
    ///   unchanged.
    pub fn allocate_segment_id(&mut self, table_name: &str) -> Result<u64> {
        self.update_manifest(|m| {
            let entry = m.tables.get_mut(table_name).ok_or_else(|| {
                BqliteError::Execution(format!(
                    "allocate_segment_id: unknown table '{table_name}'"
                ))
            })?;
            let issued = entry.next_segment_id;
            entry.next_segment_id = issued.checked_add(1).ok_or_else(|| {
                BqliteError::Execution(format!(
                    "allocate_segment_id: segment_id counter for table '{table_name}' would overflow u64"
                ))
            })?;
            Ok(issued)
        })
    }

    /// Atomically reserve a contiguous range of `count` sequence IDs
    /// for a segment about to land on `table_name`.
    ///
    /// Returns `(first_inclusive, last_inclusive)` — the closed
    /// interval of `__seq_id` values the caller may stamp onto its
    /// rows. Per `docs/design/storage-format.md` §6.2 every row in an
    /// ingest batch gets a monotonically increasing 64-bit sequence
    /// ID. §6.2 describes the bump as "per batch", but Wave 2 ingest
    /// produces exactly one segment per `(window, shard)` bucket per
    /// batch — so per-segment reservation is the correct unit at this
    /// layer, and the segment footer's `seq_id_range` field becomes a
    /// direct slice of the per-table counter.
    ///
    /// A `count` of zero is an error — a zero-row segment is illegal
    /// per the segment-format §6 invariants, and reserving a zero
    /// range would produce a meaningless pair.
    ///
    /// Like the other `allocate_*` helpers, a reserved range that is
    /// followed by a failed write retires cleanly (gaps in
    /// `__seq_id` are allowed).
    ///
    /// # Errors
    ///
    /// - [`BqliteError::Execution`] if `table_name` is not registered
    ///   in the manifest.
    /// - [`BqliteError::Execution`] if `count == 0`.
    /// - [`BqliteError::Execution`] if the reservation would push the
    ///   counter past `u64::MAX`.
    /// - Any I/O error from the atomic manifest write propagates
    ///   unchanged.
    pub fn allocate_sequence_id_range(
        &mut self,
        table_name: &str,
        count: u64,
    ) -> Result<(u64, u64)> {
        if count == 0 {
            return Err(BqliteError::Execution(
                "allocate_sequence_id_range: count must be greater than 0".into(),
            ));
        }
        self.update_manifest(|m| {
            let entry = m.tables.get_mut(table_name).ok_or_else(|| {
                BqliteError::Execution(format!(
                    "allocate_sequence_id_range: unknown table '{table_name}'"
                ))
            })?;
            let first = entry.next_sequence_id;
            // `first + count` is the next free id after the reserved
            // block; `last` is the last id *inside* the block.
            let next_free = first.checked_add(count).ok_or_else(|| {
                BqliteError::Execution(format!(
                    "allocate_sequence_id_range: reserving {count} ids for table '{table_name}' would overflow u64 (next_sequence_id = {first})"
                ))
            })?;
            entry.next_sequence_id = next_free;
            // `count >= 1`, so `next_free - 1 >= first` is safe.
            Ok((first, next_free - 1))
        })
    }

    // ── DDL operations ────────────────────────────────────────────

    /// Atomically register a new table in the manifest.
    ///
    /// Errors with `BqliteError::Execution` if a table with the same
    /// name already exists. The new entry starts with zeroed counters,
    /// no segments, and `bootstrap_events_table: false`.
    pub fn create_table(&mut self, name: String, schema: TableSchema) -> Result<()> {
        self.update_manifest(|m| {
            if m.tables.contains_key(&name) {
                return Err(BqliteError::Execution(format!(
                    "create_table: table '{name}' already exists"
                )));
            }
            m.tables.insert(
                name.clone(),
                TableEntry {
                    schema,
                    next_sequence_id: 0,
                    next_batch_id: 0,
                    next_segment_id: 0,
                    bootstrap_events_table: false,
                    windows: Vec::new(),
                },
            );
            Ok(())
        })
    }

    /// Atomically remove a table and all its segments from the
    /// manifest.
    ///
    /// On-disk segment files are **not** reaped here — that is the
    /// responsibility of the startup orphan-cleanup pass (TASK-239).
    /// This method only removes the manifest entry.
    ///
    /// Errors with `BqliteError::Execution` if the table does not
    /// exist.
    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        self.update_manifest(|m| {
            if m.tables.remove(name).is_none() {
                return Err(BqliteError::Execution(format!(
                    "drop_table: unknown table '{name}'"
                )));
            }
            Ok(())
        })
    }

    /// Atomically append a column to an existing table's schema.
    ///
    /// Bumps the schema version by one. The new column reads as NULL
    /// (or its default) for all existing rows — no segment rewrite is
    /// needed because reads project by column name against the
    /// per-segment schema snapshot.
    ///
    /// # Errors
    ///
    /// - `BqliteError::Execution` if the table does not exist.
    /// - `BqliteError::Execution` if a column with the same name
    ///   already exists on the table.
    pub fn alter_table_add_column(&mut self, name: &str, column: ColumnDef) -> Result<()> {
        self.update_manifest(|m| {
            let entry = m.tables.get_mut(name).ok_or_else(|| {
                BqliteError::Execution(format!("alter_table_add_column: unknown table '{name}'"))
            })?;

            // Reject duplicate column names.
            if entry.schema.column(&column.name).is_some() {
                return Err(BqliteError::Execution(format!(
                    "alter_table_add_column: column '{}' already exists on table '{name}'",
                    column.name
                )));
            }

            // Rebuild the schema with the new column appended and
            // version bumped. We reconstruct via `TableSchema::new`
            // so the §5.1 validation rules re-fire (defensive), then
            // stamp the incremented version.
            let old = &entry.schema;
            let mut columns: Vec<ColumnDef> = old.columns().to_vec();
            columns.push(column);
            let new_version = old.version() + 1;
            let new_schema = TableSchema::new(
                old.name(),
                columns,
                &old.entity_key_column().name,
                &old.timestamp_column().name,
                &old.event_type_column().name,
            )
            .map_err(|e| {
                BqliteError::Execution(format!(
                    "alter_table_add_column: schema rebuild failed: {e}"
                ))
            })?
            .with_version(new_version);

            entry.schema = new_schema;
            Ok(())
        })
    }

    /// Atomically swap a set of compaction inputs for one compacted
    /// output in `(table, window_id, shard_id)`'s manifest inventory.
    ///
    /// One call to this method = one
    /// `manifest.json.tmp → fsync → rename` cycle, so the on-disk
    /// manifest never observes the half-state where inputs are gone
    /// but the output is not yet present. This is the
    /// `docs/design/storage/compaction-concurrency.md` §6 publish
    /// primitive — the sole atomic mutation a compaction worker uses
    /// to land its output.
    ///
    /// `pub(crate)` because the only intended caller is
    /// [`crate::compaction::compact_one`]; external code that wants
    /// segment-level mutation goes through [`Self::add_segment`] /
    /// [`Self::remove_segment`].
    ///
    /// See [`Manifest::replace_segments`] for the full error taxonomy
    /// (unknown table, unknown window, shard out of range, missing
    /// input id, duplicate output id).
    //
    // CP2 of TASK-408 lands this primitive; CP3 wires `compact_one`
    // to it. The non-test lib build has no caller yet, so silence
    // dead_code until CP3 lands.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn replace_segments(
        &mut self,
        table_name: &str,
        window_id: u32,
        shard_id: u32,
        removed_ids: &[u64],
        new_segment: SegmentMeta,
    ) -> Result<()> {
        self.update_manifest(|m| {
            m.replace_segments(table_name, window_id, shard_id, removed_ids, new_segment)
        })
    }

    /// Apply `f` to a mutable copy of the current manifest, persist
    /// the result atomically, then adopt it as the new in-memory
    /// state.
    ///
    /// The clone-apply-persist-swap shape guarantees that a failing
    /// closure or a failing disk write leaves both the on-disk
    /// manifest and `self.manifest` untouched — callers only observe
    /// the mutation when every step succeeds. The clone cost is
    /// insignificant at Wave 2/4 scale (a handful of tables with a
    /// handful of segments each).
    ///
    /// `pub(crate)` so the compaction module can compose multi-step
    /// mutations (specifically [`Manifest::replace_segments`] for the
    /// §6 atomic publish protocol) into a single durable fsync. Other
    /// crates still go through the typed entry points
    /// ([`Self::add_segment`], [`Self::remove_segment`],
    /// [`Self::replace_segments`], [`Self::allocate_batch_id`], etc.).
    pub(crate) fn update_manifest<R, F>(&mut self, f: F) -> Result<R>
    where
        F: FnOnce(&mut Manifest) -> Result<R>,
    {
        let mut updated = self.manifest.clone();
        let result = f(&mut updated)?;
        write_manifest_atomic(&self.root, &updated)?;
        self.manifest = updated;
        Ok(result)
    }
}

/// Construct an empty [`SegmentReader`] for a standalone schema.
///
/// Wave 1 convenience for operator tests (TASK-117) and callers that
/// need a reader before any table has been seeded into the manifest
/// (TASK-125). Returns a reader that knows `schema` and yields zero
/// segments.
pub fn empty_segment_reader(schema: TableSchema) -> Box<dyn SegmentReader> {
    Box::new(EmptySegmentReader { schema })
}

/// Best-effort removal of any stale `manifest.json.tmp` left over
/// from a crash during a prior atomic update. The rename in
/// `write_manifest_atomic` is atomic, so a stale temp file
/// necessarily belongs to an aborted update — safe to delete.
fn clean_stale_tmp(root: &Path) {
    let tmp_path = root.join(MANIFEST_TMP_FILE_NAME);
    match fs::remove_file(&tmp_path) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(_) => {
            // Best-effort: a stale tmp file we can't delete is
            // harmless — the next atomic write will overwrite it.
        }
    }
}

/// Acquire the exclusive advisory lock on `<root>/.lock`.
///
/// A fresh empty file is created if necessary. On success, the
/// returned [`File`] holds the lock until it is dropped. If another
/// process already holds the lock, returns
/// [`BqliteError::Execution`] with a message naming the directory.
fn acquire_lock(root: &Path) -> Result<File> {
    let lock_path = root.join(LOCK_FILE_NAME);
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&lock_path)
        .map_err(|e| io_ctx("open database lock file", &lock_path, e))?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(TryLockError::WouldBlock) => Err(BqliteError::Execution(format!(
            "database at {} is already open by another process",
            root.display()
        ))),
        Err(TryLockError::Error(e)) => Err(io_ctx("acquire database lock", &lock_path, e)),
    }
}

/// Write `manifest` to `<root>/manifest.json` atomically.
///
/// Sequence per `storage-format.md` §12.3:
///
/// 1. Serialize to JSON.
/// 2. Create `manifest.json.tmp`, write bytes, `fsync` the file.
/// 3. `rename` over `manifest.json` — atomic on POSIX.
/// 4. Best-effort `fsync` the parent directory so the rename is durable.
///
/// The final directory fsync is best-effort because it is not
/// supported on every filesystem; skipping it weakens durability
/// against an immediate power loss but does not affect correctness
/// under process crashes.
fn write_manifest_atomic(root: &Path, manifest: &Manifest) -> Result<()> {
    let final_path = root.join(MANIFEST_FILE_NAME);
    let tmp_path = root.join(MANIFEST_TMP_FILE_NAME);

    let body = serde_json::to_vec_pretty(manifest)
        .map_err(|e| BqliteError::Execution(format!("failed to serialize manifest: {e}")))?;

    {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)
            .map_err(|e| io_ctx("create manifest.tmp", &tmp_path, e))?;
        f.write_all(&body)
            .map_err(|e| io_ctx("write manifest.tmp", &tmp_path, e))?;
        f.sync_all()
            .map_err(|e| io_ctx("fsync manifest.tmp", &tmp_path, e))?;
    }

    fs::rename(&tmp_path, &final_path)
        .map_err(|e| io_ctx("rename manifest.tmp to manifest", &tmp_path, e))?;

    if let Ok(dir) = File::open(root) {
        let _ = dir.sync_all();
    }

    Ok(())
}

/// Wrap an `io::Error` with a path-and-action context message while
/// preserving the original `ErrorKind` so callers can still pattern-
/// match on it.
fn io_ctx(action: &str, path: &Path, err: io::Error) -> BqliteError {
    BqliteError::Io(io::Error::new(
        err.kind(),
        format!("{action} {}: {err}", path.display()),
    ))
}

fn filter_windows_by_time_range(
    windows: &[WindowManifest],
    time_range: TimeRange,
) -> Vec<WindowManifest> {
    let start = time_range.start.as_nanos();
    let end = time_range.end.as_nanos();

    windows
        .iter()
        .filter_map(|window| {
            let shards: Vec<Vec<SegmentMeta>> = window
                .shards
                .iter()
                .map(|segments| {
                    segments
                        .iter()
                        .filter(|seg| seg.ts_range.1 >= start && seg.ts_range.0 < end)
                        .cloned()
                        .collect()
                })
                .collect();

            if shards.iter().all(Vec::is_empty) {
                None
            } else {
                Some(WindowManifest {
                    window_id: window.window_id,
                    shards,
                })
            }
        })
        .collect()
}

// ─────────────────────────────────────────────────────────────────────────────
// ManifestSegmentReader — production manifest-backed reader (TASK-244)
// ─────────────────────────────────────────────────────────────────────────────

/// Manifest-backed [`SegmentReader`] that enumerates real segments and
/// opens them from disk via [`SegmentFileReader`].
///
/// Constructed by [`Database::segment_reader`], which snapshots the
/// table's current schema and window inventory from the manifest at
/// call time. Iteration order is `(window_id ascending, shard_id
/// ascending, insertion order within shard)` — stable for the reader's
/// lifetime and monotonically ordered by time window.
///
/// `open_segment` validates that the supplied handle came from *this*
/// reader's snapshot (foreign or stale handles are rejected with
/// `BqliteError::Execution`), resolves the on-disk path using the
/// same layout as `storage-format.md` §5.2, and delegates to
/// [`SegmentFileReader::open`] which handles checksum verification
/// and schema-evolution backfill.
struct ManifestSegmentReader {
    root: PathBuf,
    table_name: String,
    /// TASK-247: stored as `Arc` so every `open_segment` call shares
    /// the same allocation instead of cloning the full `TableSchema`
    /// (columns, entity-key name, timestamp name) per segment.
    schema: Arc<TableSchema>,
    windows: Vec<WindowManifest>,
}

impl SegmentReader for ManifestSegmentReader {
    fn schema(&self) -> &TableSchema {
        &self.schema
    }

    fn segments(&self) -> Box<dyn Iterator<Item = Result<SegmentHandle>> + Send + '_> {
        let iter = self.windows.iter().flat_map(|win| {
            let window_id = win.window_id;
            win.shards
                .iter()
                .enumerate()
                .flat_map(move |(shard_idx, segs)| {
                    let shard_id = shard_idx as u32;
                    segs.iter().map(move |seg| {
                        Ok(SegmentHandle {
                            segment_id: seg.segment_id,
                            shard_id,
                            window_id: u64::from(window_id),
                            row_count: seg.row_count,
                            schema_version: seg.schema_version,
                        })
                    })
                })
        });
        Box::new(iter)
    }

    fn open_segment(
        &self,
        handle: &SegmentHandle,
        projection: &ColumnProjection,
        predicate: Option<Arc<dyn Predicate>>,
    ) -> Result<Box<dyn SegmentScan>> {
        // Validate that the handle came from this reader's snapshot.
        let window_id = u32::try_from(handle.window_id).map_err(|_| {
            BqliteError::Execution(format!(
                "open_segment: window_id {} overflows u32",
                handle.window_id
            ))
        })?;
        let found = self.windows.iter().any(|win| {
            win.window_id == window_id
                && win
                    .shards
                    .get(handle.shard_id as usize)
                    .is_some_and(|segs| segs.iter().any(|s| s.segment_id == handle.segment_id))
        });
        if !found {
            return Err(BqliteError::Execution(format!(
                "open_segment: unknown or stale handle (segment_id={}, window_id={}, shard_id={})",
                handle.segment_id, handle.window_id, handle.shard_id
            )));
        }

        let path = self
            .root
            .join(&self.table_name)
            .join("windows")
            .join(format!("w_{window_id:06}"))
            .join(format!("shard_{:02}", handle.shard_id))
            .join(format!("segment_{}.seg", handle.segment_id));

        let file_reader = SegmentFileReader::open_shared(&path, self.schema.clone())?;
        let scan = file_reader.scan(projection, predicate)?;
        Ok(Box::new(scan))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EmptySegmentReader — test helper only (TASK-244)
// ─────────────────────────────────────────────────────────────────────────────

/// `SegmentReader` implementation that owns a schema but yields no
/// segments. Retained as a test helper via [`empty_segment_reader`];
/// the production query path uses [`ManifestSegmentReader`].
struct EmptySegmentReader {
    schema: TableSchema,
}

impl SegmentReader for EmptySegmentReader {
    fn schema(&self) -> &TableSchema {
        &self.schema
    }

    fn segments(&self) -> Box<dyn Iterator<Item = Result<SegmentHandle>> + Send + '_> {
        Box::new(std::iter::empty())
    }

    fn open_segment(
        &self,
        _handle: &SegmentHandle,
        _projection: &ColumnProjection,
        _predicate: Option<Arc<dyn Predicate>>,
    ) -> Result<Box<dyn SegmentScan>> {
        Err(BqliteError::Execution(
            "no segments are visible in the empty segment reader".into(),
        ))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use bqlite_core::property::BqlType;
    use bqlite_core::schema::{ColumnDef, TableSchema};

    use super::*;
    use crate::manifest::TableEntry;

    // Per-test unique temp directory. We avoid pulling in `tempfile`
    // as a dev-dep and mirror the minimal approach used by
    // `tests/common/mod.rs::TempDb`. Process PID plus a monotonic
    // counter is sufficient for in-process uniqueness.
    static SEQ: AtomicU64 = AtomicU64::new(0);

    struct Scratch {
        path: PathBuf,
    }

    impl Scratch {
        fn new(label: &str) -> Self {
            let seq = SEQ.fetch_add(1, Ordering::Relaxed);
            let pid = std::process::id();
            let mut path = std::env::temp_dir();
            path.push(format!("bqlite-storage-db-{label}-{pid}-{seq}"));
            // We explicitly do NOT create the directory here — the
            // Database::create must be able to create it itself.
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    /// Create a fresh database with an `events` table. Many tests need
    /// a table present before they can add segments / allocate IDs. Now
    /// that `Database::create` no longer seeds the bootstrap events
    /// table (TASK-240), this helper provides a one-liner replacement.
    fn create_db_with_events(path: &Path) -> Database {
        let mut db = Database::create(path).expect("create");
        db.create_table("events".into(), sample_events_schema())
            .expect("create events table");
        db
    }

    fn sample_events_schema() -> TableSchema {
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
        .expect("minimal schema")
    }

    // ── Fresh init (TASK-240: uses Database::create) ─────────────────────

    // ── Database::create / Database::open (TASK-240) ──────────────────────

    #[test]
    fn create_initializes_fresh_database_without_bootstrap() {
        let scratch = Scratch::new("create-fresh");
        let db = Database::create(scratch.path()).expect("create");
        assert!(scratch.path().join(LOCK_FILE_NAME).is_file());
        assert!(scratch.path().join(MANIFEST_FILE_NAME).is_file());
        let m = db.manifest();
        assert_eq!(m.format_version, MANIFEST_FORMAT_VERSION);
        assert_eq!(m.shard_count, DEFAULT_SHARD_COUNT);
        assert_eq!(m.database_uuid.len(), 36);
        // No bootstrap events table — TASK-240 retires the seeding.
        assert!(
            m.tables.is_empty(),
            "Database::create must not seed any tables"
        );
    }

    #[test]
    fn create_with_custom_shards() {
        let scratch = Scratch::new("create-shards");
        let db = Database::create_with_shards(scratch.path(), 8).expect("create");
        assert_eq!(db.manifest().shard_count, 8);
        assert!(db.manifest().tables.is_empty());
    }

    #[test]
    fn create_zero_shards_is_rejected() {
        let scratch = Scratch::new("create-zero-shards");
        let err =
            Database::create_with_shards(scratch.path(), 0).expect_err("zero shards must error");
        assert!(matches!(err, BqliteError::Schema(_)), "got {err:?}");
    }

    #[test]
    fn create_errors_if_manifest_already_exists() {
        let scratch = Scratch::new("create-exists");
        let _db = Database::create(scratch.path()).expect("first create");
        drop(_db);
        let err = Database::create(scratch.path())
            .expect_err("second create on same directory must fail");
        match err {
            BqliteError::Execution(msg) => {
                assert!(
                    msg.contains("manifest already exists"),
                    "error should mention existing manifest, got: {msg}"
                );
            }
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    #[test]
    fn create_then_open_roundtrips() {
        let scratch = Scratch::new("create-open");
        let uuid = {
            let db = Database::create(scratch.path()).expect("create");
            db.manifest().database_uuid.clone()
        };
        let db = Database::open(scratch.path()).expect("open");
        assert_eq!(db.manifest().database_uuid, uuid);
        assert!(db.manifest().tables.is_empty());
    }

    #[test]
    fn open_errors_on_missing_directory() {
        let scratch = Scratch::new("open-missing");
        // Scratch does NOT create the directory, so this should fail.
        let err = Database::open(scratch.path()).expect_err("open on missing directory must fail");
        match err {
            BqliteError::Execution(msg) => {
                assert!(
                    msg.contains("does not exist"),
                    "error should mention missing directory, got: {msg}"
                );
                assert!(
                    msg.contains("bqlite init"),
                    "error should suggest bqlite init, got: {msg}"
                );
            }
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    #[test]
    fn open_errors_on_empty_directory_without_manifest() {
        let scratch = Scratch::new("open-empty");
        fs::create_dir_all(scratch.path()).unwrap();
        let err = Database::open(scratch.path()).expect_err("open on empty directory must fail");
        match err {
            BqliteError::Execution(msg) => {
                assert!(
                    msg.contains("not initialized"),
                    "error should mention not initialized, got: {msg}"
                );
                assert!(
                    msg.contains("bqlite init"),
                    "error should suggest bqlite init, got: {msg}"
                );
            }
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    #[test]
    fn open_reads_wave1_bootstrap_database() {
        // Simulate a Wave 1 database (which seeded a bootstrap events
        // table with bootstrap_events_table: true) by writing the
        // manifest directly. Database::open must read it successfully.
        let scratch = Scratch::new("open-wave1");
        fs::create_dir_all(scratch.path()).unwrap();
        let mut m = Manifest::new_empty(DEFAULT_SHARD_COUNT);
        m.tables.insert(
            crate::catalog::BOOTSTRAP_EVENTS_TABLE_NAME.to_string(),
            TableEntry {
                schema: crate::catalog::bootstrap_events_schema(),
                next_sequence_id: 0,
                next_batch_id: 0,
                next_segment_id: 0,
                bootstrap_events_table: true,
                windows: Vec::new(),
            },
        );
        let body = serde_json::to_vec_pretty(&m).unwrap();
        fs::write(scratch.path().join(MANIFEST_FILE_NAME), body).unwrap();

        let db = Database::open(scratch.path()).expect("open wave1 db");
        assert_eq!(db.manifest().tables.len(), 1);
        let entry = db
            .manifest()
            .tables
            .get("events")
            .expect("bootstrap events table");
        assert!(entry.bootstrap_events_table);
    }

    #[test]
    fn create_then_open_with_table_survives_roundtrip() {
        let scratch = Scratch::new("create-table-reopen");
        {
            let mut db = Database::create(scratch.path()).expect("create");
            db.create_table("users".into(), sample_events_schema())
                .expect("create table");
        }
        let db = Database::open(scratch.path()).expect("open");
        assert!(db.manifest().tables.contains_key("users"));
    }

    #[test]
    fn open_cleans_stale_tmp_on_existing_db() {
        let scratch = Scratch::new("open-clean-tmp");
        {
            let _db = Database::create(scratch.path()).expect("create");
        }
        let tmp_path = scratch.path().join(MANIFEST_TMP_FILE_NAME);
        fs::write(&tmp_path, b"stale crash tmp").unwrap();
        assert!(tmp_path.exists());
        let _db = Database::open(scratch.path()).expect("open");
        assert!(
            !tmp_path.exists(),
            "open should clean stale manifest.json.tmp"
        );
    }

    #[test]
    fn create_and_open_lock_blocks_concurrent_access() {
        let scratch = Scratch::new("create-lock");
        let _db = Database::create(scratch.path()).expect("create");
        let err = Database::open(scratch.path()).expect_err("open while created db holds lock");
        assert!(matches!(err, BqliteError::Execution(_)));
    }

    // ── Atomicity and cleanup ───────────────────────────────────────────────

    #[test]
    fn stale_manifest_tmp_is_cleaned_up_on_open() {
        let scratch = Scratch::new("stale-tmp");
        {
            let _db = Database::create(scratch.path()).expect("create");
        }
        let tmp_path = scratch.path().join(MANIFEST_TMP_FILE_NAME);
        fs::write(&tmp_path, b"garbage left behind by a prior crash").unwrap();
        assert!(tmp_path.exists());

        let _db = Database::open(scratch.path()).expect("open");
        assert!(
            !tmp_path.exists(),
            "stale manifest.json.tmp should be cleaned up on open"
        );
    }

    // ── Error paths ─────────────────────────────────────────────────────────

    #[test]
    fn corrupted_manifest_returns_execution_error() {
        let scratch = Scratch::new("corrupt");
        fs::create_dir_all(scratch.path()).unwrap();
        fs::write(
            scratch.path().join(MANIFEST_FILE_NAME),
            b"this is not valid json",
        )
        .unwrap();
        let err = Database::open(scratch.path()).expect_err("corrupted manifest must error");
        match err {
            BqliteError::Execution(msg) => {
                assert!(msg.contains("corrupted manifest"), "got: {msg}");
            }
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    #[test]
    fn unsupported_format_version_is_rejected() {
        let scratch = Scratch::new("bad-version");
        fs::create_dir_all(scratch.path()).unwrap();
        let future = format!(
            r#"{{"format_version":{},"database_uuid":"00000000-0000-4000-8000-000000000000","shard_count":32,"tables":{{}},"segments":[]}}"#,
            MANIFEST_FORMAT_VERSION + 1
        );
        fs::write(scratch.path().join(MANIFEST_FILE_NAME), future).unwrap();

        let err =
            Database::open(scratch.path()).expect_err("unsupported format_version must error");
        match err {
            BqliteError::Execution(msg) => {
                assert!(msg.contains("unsupported format_version"), "got: {msg}");
            }
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    // ── Lock file ───────────────────────────────────────────────────────────

    #[test]
    fn second_open_in_same_process_is_blocked() {
        let scratch = Scratch::new("lock-blocked");
        let first = Database::create(scratch.path()).expect("create");
        let err = Database::open(scratch.path()).expect_err("second concurrent open must fail");
        match err {
            BqliteError::Execution(msg) => {
                assert!(msg.contains("already open"), "got: {msg}");
            }
            other => panic!("expected Execution, got {other:?}"),
        }
        drop(first);
    }

    #[test]
    fn lock_is_released_on_drop() {
        let scratch = Scratch::new("lock-released");
        {
            let _first = Database::create(scratch.path()).expect("create");
        }
        // After drop, a fresh open must succeed.
        let _second = Database::open(scratch.path()).expect("open after drop");
    }

    // ── SegmentReader ────────────────────────────────────────────────────────

    #[test]
    fn segment_reader_unknown_table_returns_plan_error() {
        let scratch = Scratch::new("reader-unknown");
        let db = Database::create(scratch.path()).expect("create");
        // `Box<dyn SegmentReader>` is not Debug, so we can't use
        // `expect_err` here — pattern-match the Result directly.
        match db.segment_reader("nope") {
            Err(BqliteError::Plan(msg)) => {
                assert!(msg.contains("nope"), "got: {msg}");
            }
            Err(other) => panic!("expected Plan error, got {other:?}"),
            Ok(_) => panic!("expected an error, got Ok"),
        }
    }

    #[test]
    fn empty_segment_reader_helper_yields_zero_segments() {
        let schema = sample_events_schema();
        let reader = empty_segment_reader(schema.clone());
        assert_eq!(reader.schema(), &schema);
        let count = reader.segments().count();
        assert_eq!(count, 0, "empty reader yields no segments");
    }

    #[test]
    fn seeded_table_counters_survive_reopen() {
        // Persisted `next_sequence_id` / `next_batch_id` monotonicity
        // across restarts is the whole point of storing them in the
        // manifest (storage-format.md §6.2 + §12.3). Even though Wave 1
        // never bumps them, exercising the round-trip here proves the
        // TableEntry fields are wired through serde and the atomic
        // write path correctly.
        let scratch = Scratch::new("counter-roundtrip");
        {
            let _db = Database::create(scratch.path()).expect("create");
        }
        let manifest_path = scratch.path().join(MANIFEST_FILE_NAME);
        let bytes = fs::read(&manifest_path).unwrap();
        let mut m: Manifest = serde_json::from_slice(&bytes).unwrap();
        m.tables.insert(
            "events".to_string(),
            TableEntry {
                schema: sample_events_schema(),
                next_sequence_id: 123_456,
                next_batch_id: 78,
                next_segment_id: 9,
                bootstrap_events_table: true,
                windows: Vec::new(),
            },
        );
        fs::write(&manifest_path, serde_json::to_vec_pretty(&m).unwrap()).unwrap();

        let db = Database::open(scratch.path()).expect("open");
        let entry = db.manifest().tables.get("events").expect("events entry");
        assert_eq!(entry.next_sequence_id, 123_456);
        assert_eq!(entry.next_batch_id, 78);
        assert!(entry.bootstrap_events_table);
    }

    #[test]
    fn segment_reader_for_created_table_yields_zero_segments() {
        let scratch = Scratch::new("reader-created");
        let mut db = Database::create(scratch.path()).expect("create");
        db.create_table("events".into(), sample_events_schema())
            .expect("create table");
        let reader = db.segment_reader("events").expect("reader for events");
        assert_eq!(reader.schema().name(), "events");
        assert_eq!(reader.segments().count(), 0);
    }

    // ── ManifestSegmentReader (TASK-244) ────────────────────────────────────

    use crate::writer::SegmentWriter;
    use bqlite_core::event::{EntityId, Event};
    use bqlite_core::storage::ColumnProjection;
    use bqlite_core::time::Timestamp;

    /// Schema with a nullable `amount` column for manifest-reader
    /// round-trip tests. Distinct from the minimal
    /// `sample_events_schema()` (which uses `entity_id` and has no
    /// nullable columns) because the SegmentWriter tests need a
    /// column the encoding selector can exercise.
    fn writer_events_schema() -> TableSchema {
        TableSchema::new(
            "events",
            vec![
                ColumnDef::required("user_id", BqlType::String),
                ColumnDef::required("ts", BqlType::Timestamp),
                ColumnDef::required("event_type", BqlType::String),
                ColumnDef::nullable("amount", BqlType::Int),
            ],
            "user_id",
            "ts",
            "event_type",
        )
        .expect("schema")
    }

    fn test_event(user: &str, ts_ns: i64, event_type: &str) -> Event {
        Event::new(EntityId::from(user), Timestamp(ts_ns), event_type)
    }

    fn create_db_with_writer_schema(path: &Path) -> Database {
        let mut db = Database::create(path).expect("create");
        db.create_table("events".into(), writer_events_schema())
            .expect("create events table");
        db
    }

    #[test]
    fn manifest_reader_enumerates_written_segments() {
        let scratch = Scratch::new("manifest-reader-enum");
        let mut db = create_db_with_writer_schema(scratch.path());

        let events = vec![
            test_event("alice", 1_700_000_000_000_000_000, "click"),
            test_event("bob", 1_700_000_000_100_000_000, "view"),
        ];
        let batch_id = db.allocate_batch_id("events").unwrap();
        {
            let mut writer = SegmentWriter::new(&mut db);
            writer
                .write_bucket("events", 0, 0, batch_id, &events)
                .expect("write_bucket");
        }

        let reader = db.segment_reader("events").expect("reader");
        let handles: Vec<_> = reader.segments().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(handles.len(), 1, "should enumerate exactly one segment");
        assert_eq!(handles[0].row_count, 2);
        assert_eq!(handles[0].shard_id, 0);
    }

    #[test]
    fn manifest_reader_open_segment_returns_real_rows() {
        let scratch = Scratch::new("manifest-reader-open");
        let mut db = create_db_with_writer_schema(scratch.path());

        let events = vec![
            test_event("alice", 1_700_000_000_000_000_000, "click"),
            test_event("bob", 1_700_000_000_100_000_000, "view"),
            test_event("charlie", 1_700_000_000_200_000_000, "purchase"),
        ];
        let batch_id = db.allocate_batch_id("events").unwrap();
        {
            let mut writer = SegmentWriter::new(&mut db);
            writer
                .write_bucket("events", 0, 0, batch_id, &events)
                .expect("write_bucket");
        }

        let reader = db.segment_reader("events").expect("reader");
        let handles: Vec<_> = reader.segments().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(handles.len(), 1);

        let projection = ColumnProjection::all();
        let mut scan = reader
            .open_segment(&handles[0], &projection, None)
            .expect("open_segment");
        assert_eq!(scan.row_group_count(), 1);

        let batch = scan
            .next_row_group()
            .unwrap()
            .expect("should have one row group");
        assert_eq!(batch.num_rows(), 3);

        // Verify exhaustion.
        assert!(scan.next_row_group().unwrap().is_none());
    }

    #[test]
    fn manifest_reader_multiple_segments_across_shards() {
        let scratch = Scratch::new("manifest-reader-multi");
        let mut db = create_db_with_writer_schema(scratch.path());

        // Write one segment to shard 0 and one to shard 1.
        let events1 = vec![test_event("alice", 1_000, "click")];
        let events2 = vec![test_event("bob", 2_000, "view")];

        let batch_id = db.allocate_batch_id("events").unwrap();
        {
            let mut writer = SegmentWriter::new(&mut db);
            writer
                .write_bucket("events", 0, 0, batch_id, &events1)
                .expect("shard 0");
            writer
                .write_bucket("events", 0, 1, batch_id, &events2)
                .expect("shard 1");
        }

        let reader = db.segment_reader("events").expect("reader");
        let handles: Vec<_> = reader.segments().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(handles.len(), 2, "two segments across two shards");
        // Both segments are readable.
        for h in &handles {
            let mut scan = reader
                .open_segment(h, &ColumnProjection::all(), None)
                .expect("open_segment");
            let batch = scan
                .next_row_group()
                .unwrap()
                .expect("should have a row group");
            assert_eq!(batch.num_rows(), 1);
        }
    }

    #[test]
    fn manifest_reader_rejects_foreign_handle() {
        let scratch = Scratch::new("manifest-reader-foreign");
        let mut db = Database::create(scratch.path()).expect("create");
        db.create_table("events".into(), writer_events_schema())
            .expect("create table");

        let reader = db.segment_reader("events").expect("reader");
        let foreign = SegmentHandle {
            segment_id: 999,
            shard_id: 0,
            window_id: 0,
            row_count: 10,
            schema_version: 1,
        };
        match reader.open_segment(&foreign, &ColumnProjection::all(), None) {
            Err(BqliteError::Execution(msg)) => {
                assert!(msg.contains("unknown or stale"), "got: {msg}");
            }
            Err(other) => panic!("expected Execution, got {other:?}"),
            Ok(_) => panic!("expected error for foreign handle"),
        }
    }

    // ── Wave 2 add_segment / remove_segment / snapshot_for_query ────────────

    use bqlite_core::property::PropertyValue;
    use bqlite_core::time::TimeRange;

    use crate::manifest::{ColumnStats, SegmentMeta};

    /// Shared sample SegmentMeta for the Database-level tests.
    /// Mirrors the manifest-module helper but intentionally separate
    /// so both test suites can evolve independently.
    fn sample_segment(segment_id: u64, batch_id: u64, ts_range: (i64, i64)) -> SegmentMeta {
        SegmentMeta {
            segment_id,
            level: 0,
            schema_version: 1,
            row_count: 10,
            byte_size: 256,
            ts_range,
            entity_range: (
                PropertyValue::String("alice".into()),
                PropertyValue::String("zoe".into()),
            ),
            column_stats: vec![ColumnStats {
                column_name: "entity_id".into(),
                min: Some(PropertyValue::String("alice".into())),
                max: Some(PropertyValue::String("zoe".into())),
                null_count: 0,
                distinct_count_estimate: None,
            }],
            created_at: 1_700_000_000_000_000_000,
            batch_id,
            seq_id_range: (0, 9),
        }
    }

    #[test]
    fn add_segment_persists_across_reopen() {
        let scratch = Scratch::new("add-persists");
        {
            let mut db = create_db_with_events(scratch.path());
            db.add_segment("events", 3, 1, sample_segment(42, 7, (0, 100)))
                .expect("add_segment");
            // In-memory snapshot reflects the mutation immediately.
            let windows = &db.manifest().tables["events"].windows;
            assert_eq!(windows.len(), 1);
            assert_eq!(windows[0].window_id, 3);
            assert_eq!(windows[0].shards[1].len(), 1);
            assert_eq!(windows[0].shards[1][0].segment_id, 42);
        }
        // Reopen and confirm the mutation survived the fsync+rename.
        let db = Database::open(scratch.path()).expect("open");
        let entry = db.manifest().tables.get("events").expect("events entry");
        assert_eq!(entry.windows.len(), 1);
        let win = &entry.windows[0];
        assert_eq!(win.window_id, 3);
        assert_eq!(
            win.shards.len(),
            DEFAULT_SHARD_COUNT as usize,
            "window preserves shard_count slots across reopen"
        );
        assert_eq!(win.shards[1].len(), 1);
        let seg = &win.shards[1][0];
        assert_eq!(seg.segment_id, 42);
        assert_eq!(seg.batch_id, 7);
        assert_eq!(seg.ts_range, (0, 100));
    }

    #[test]
    fn remove_segment_persists_across_reopen() {
        let scratch = Scratch::new("remove-persists");
        {
            let mut db = create_db_with_events(scratch.path());
            db.add_segment("events", 0, 0, sample_segment(1, 1, (0, 50)))
                .unwrap();
            db.add_segment("events", 0, 0, sample_segment(2, 1, (50, 100)))
                .unwrap();
            let removed = db.remove_segment("events", 1).expect("remove");
            assert_eq!(removed.segment_id, 1);
            // In-memory snapshot reflects the removal.
            assert_eq!(db.manifest().tables["events"].windows[0].shards[0].len(), 1);
        }
        let db = Database::open(scratch.path()).expect("open");
        let shard = &db.manifest().tables["events"].windows[0].shards[0];
        assert_eq!(shard.len(), 1);
        assert_eq!(shard[0].segment_id, 2);
    }

    #[test]
    fn replace_segments_publish_is_atomic_across_reopen() {
        // Mirrors the compaction publish path: three L0 inputs swap
        // for one compacted output in a single fsync. After reopen,
        // the manifest must reflect exactly the post-publish state —
        // no half-state, no leftover inputs.
        let scratch = Scratch::new("replace-persists");
        let new_id = {
            let mut db = create_db_with_events(scratch.path());
            db.add_segment("events", 0, 0, sample_segment(1, 1, (0, 100)))
                .unwrap();
            db.add_segment("events", 0, 0, sample_segment(2, 1, (100, 200)))
                .unwrap();
            db.add_segment("events", 0, 0, sample_segment(3, 1, (200, 300)))
                .unwrap();
            // Allocate a fresh id for the compacted output, then
            // publish in one durable mutation.
            let new_id = db.allocate_segment_id("events").expect("alloc");
            let mut new_seg = sample_segment(new_id, 99, (0, 300));
            new_seg.level = 1;
            db.replace_segments("events", 0, 0, &[1, 2, 3], new_seg)
                .expect("replace_segments");
            // In-memory snapshot reflects the swap immediately.
            let shard = &db.manifest().tables["events"].windows[0].shards[0];
            assert_eq!(shard.len(), 1);
            assert_eq!(shard[0].segment_id, new_id);
            new_id
        };
        let db = Database::open(scratch.path()).expect("reopen");
        let shard = &db.manifest().tables["events"].windows[0].shards[0];
        assert_eq!(
            shard.len(),
            1,
            "post-reopen shard must contain only the compacted output"
        );
        assert_eq!(shard[0].segment_id, new_id);
        assert_eq!(shard[0].level, 1, "level promotion preserved");
    }

    #[test]
    fn snapshot_for_query_reflects_committed_state() {
        let scratch = Scratch::new("snapshot-wiring");
        let mut db = create_db_with_events(scratch.path());
        db.add_segment("events", 0, 0, sample_segment(1, 1, (0, 100)))
            .unwrap();
        db.add_segment("events", 0, 0, sample_segment(2, 1, (200, 300)))
            .unwrap();
        db.add_segment("events", 0, 1, sample_segment(3, 1, (0, 100)))
            .unwrap();

        // Shard 0, unbounded: two segments, in insertion order.
        let got = db
            .snapshot_for_query("events", TimeRange::unbounded(), 0)
            .unwrap();
        let ids: Vec<u64> = got.iter().map(|s| s.segment_id).collect();
        assert_eq!(ids, vec![1, 2]);

        // Shard 0, narrow range over segment 1 only.
        let range = TimeRange::new(0.into(), 50.into());
        let got = db.snapshot_for_query("events", range, 0).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].segment_id, 1);

        // Shard 1 sees its own segment and nothing else.
        let got = db
            .snapshot_for_query("events", TimeRange::unbounded(), 1)
            .unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].segment_id, 3);
    }

    // ── Tombstone snapshot integration ──────────────────────────────────

    #[test]
    fn load_tombstone_snapshot_unknown_table_errors() {
        let scratch = Scratch::new("tomb-unknown");
        let db = Database::create(scratch.path()).expect("create");
        let err = db.load_tombstone_snapshot("nope", &[(0, 0)]).unwrap_err();
        assert!(matches!(err, BqliteError::Execution(_)));
    }

    #[test]
    fn load_tombstone_snapshot_empty_when_no_files() {
        let scratch = Scratch::new("tomb-empty");
        let db = create_db_with_events(scratch.path());
        let snap = db.load_tombstone_snapshot("events", &[(0, 0)]).unwrap();
        assert!(snap.is_empty());
    }

    #[test]
    fn load_tombstone_snapshot_reads_written_tombstones() {
        let scratch = Scratch::new("tomb-roundtrip");
        let db = create_db_with_events(scratch.path());

        // Write a tombstone file directly to the expected shard path.
        let path = crate::tombstone::tombstone_file_path(scratch.path(), "events", 1, 0);
        let tf = crate::tombstone::TombstoneFile::for_rows([42, 99]);
        crate::tombstone::write_tombstone_atomic(&path, &tf).unwrap();

        let snap = db
            .load_tombstone_snapshot("events", &[(1, 0), (1, 1)])
            .unwrap();
        assert!(!snap.is_empty());
        let loaded = snap.get(1, 0).expect("shard 0 has tombstones");
        assert!(loaded.row_deletes.contains(&42));
        assert!(loaded.row_deletes.contains(&99));
        assert!(snap.get(1, 1).is_none());
    }

    #[test]
    fn add_segment_unknown_table_leaves_manifest_unchanged() {
        // The clone-apply-persist-swap shape means a failing mutation
        // must leave both disk and memory exactly as they were. This
        // test asserts it end-to-end by comparing the serialized
        // manifest before and after a failed call.
        let scratch = Scratch::new("add-unknown");
        let mut db = Database::create(scratch.path()).expect("create");
        let before_mem = db.manifest().clone();
        let before_disk = fs::read(scratch.path().join(MANIFEST_FILE_NAME)).unwrap();

        let err = db
            .add_segment("nope", 0, 0, sample_segment(1, 1, (0, 1)))
            .expect_err("unknown table must error");
        assert!(matches!(err, BqliteError::Execution(_)));

        assert_eq!(&before_mem, db.manifest(), "in-memory manifest preserved");
        let after_disk = fs::read(scratch.path().join(MANIFEST_FILE_NAME)).unwrap();
        assert_eq!(before_disk, after_disk, "on-disk manifest bytes preserved");
    }

    #[test]
    fn add_segment_duplicate_id_leaves_manifest_unchanged() {
        // Second add with the same segment_id must error without
        // corrupting the first insert.
        let scratch = Scratch::new("add-duplicate");
        let mut db = create_db_with_events(scratch.path());
        db.add_segment("events", 0, 0, sample_segment(7, 1, (0, 100)))
            .unwrap();

        let checkpoint_mem = db.manifest().clone();
        let checkpoint_disk = fs::read(scratch.path().join(MANIFEST_FILE_NAME)).unwrap();

        let err = db
            .add_segment("events", 5, 1, sample_segment(7, 2, (100, 200)))
            .expect_err("duplicate id must error");
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("already exists"), "got: {msg}"),
            other => panic!("expected Execution, got {other:?}"),
        }

        assert_eq!(
            &checkpoint_mem,
            db.manifest(),
            "in-memory manifest unchanged after failed duplicate add",
        );
        let after_disk = fs::read(scratch.path().join(MANIFEST_FILE_NAME)).unwrap();
        assert_eq!(
            checkpoint_disk, after_disk,
            "on-disk manifest bytes unchanged after failed duplicate add",
        );
    }

    #[test]
    fn remove_segment_missing_id_leaves_manifest_unchanged() {
        let scratch = Scratch::new("remove-missing");
        let mut db = create_db_with_events(scratch.path());
        db.add_segment("events", 0, 0, sample_segment(1, 1, (0, 1)))
            .unwrap();

        let checkpoint_mem = db.manifest().clone();
        let checkpoint_disk = fs::read(scratch.path().join(MANIFEST_FILE_NAME)).unwrap();

        let err = db
            .remove_segment("events", 999)
            .expect_err("missing id must error");
        assert!(matches!(err, BqliteError::Execution(_)));

        assert_eq!(&checkpoint_mem, db.manifest());
        let after_disk = fs::read(scratch.path().join(MANIFEST_FILE_NAME)).unwrap();
        assert_eq!(checkpoint_disk, after_disk);
    }

    #[test]
    fn add_segment_does_not_leave_manifest_tmp_behind() {
        // A successful atomic update renames the tmp file over the
        // real manifest, so no `manifest.json.tmp` should linger.
        let scratch = Scratch::new("no-tmp-leak");
        let mut db = create_db_with_events(scratch.path());
        db.add_segment("events", 0, 0, sample_segment(1, 1, (0, 1)))
            .unwrap();
        assert!(
            !scratch.path().join(MANIFEST_TMP_FILE_NAME).exists(),
            "tmp file is renamed away by write_manifest_atomic"
        );
    }

    // ── allocate_batch_id ──────────────────────────────────────────────────

    #[test]
    fn allocate_batch_id_returns_monotonic_values_and_persists() {
        let scratch = Scratch::new("batch-id-persist");
        {
            let mut db = create_db_with_events(scratch.path());
            let first = db.allocate_batch_id("events").expect("alloc 1");
            let second = db.allocate_batch_id("events").expect("alloc 2");
            let third = db.allocate_batch_id("events").expect("alloc 3");
            // Fresh databases start the counter at 0, so the first
            // three reservations are 0, 1, 2.
            assert_eq!(first, 0);
            assert_eq!(second, 1);
            assert_eq!(third, 2);
            // In-memory counter reflects the bump.
            assert_eq!(db.manifest().tables["events"].next_batch_id, 3);
        }
        // Reopen and verify the counter persisted through fsync.
        let mut db = Database::open(scratch.path()).expect("open");
        assert_eq!(db.manifest().tables["events"].next_batch_id, 3);
        // Next allocation picks up from where the previous session
        // left off — the gap-tolerance contract.
        let fourth = db.allocate_batch_id("events").expect("alloc 4");
        assert_eq!(fourth, 3);
        assert_eq!(db.manifest().tables["events"].next_batch_id, 4);
    }

    #[test]
    fn allocate_batch_id_unknown_table_leaves_state_untouched() {
        let scratch = Scratch::new("batch-id-unknown");
        let mut db = Database::create(scratch.path()).expect("create");
        let before_mem = db.manifest().clone();
        let before_disk = fs::read(scratch.path().join(MANIFEST_FILE_NAME)).unwrap();

        let err = db
            .allocate_batch_id("missing")
            .expect_err("unknown table must error");
        match err {
            BqliteError::Execution(msg) => {
                assert!(msg.contains("unknown table"), "got: {msg}");
                assert!(msg.contains("missing"), "got: {msg}");
            }
            other => panic!("expected Execution, got {other:?}"),
        }

        assert_eq!(&before_mem, db.manifest());
        let after_disk = fs::read(scratch.path().join(MANIFEST_FILE_NAME)).unwrap();
        assert_eq!(before_disk, after_disk);
    }

    // ── allocate_segment_id ────────────────────────────────────────────────

    #[test]
    fn allocate_segment_id_returns_monotonic_values_and_persists() {
        let scratch = Scratch::new("segment-id-persist");
        {
            let mut db = create_db_with_events(scratch.path());
            let first = db.allocate_segment_id("events").expect("alloc 1");
            let second = db.allocate_segment_id("events").expect("alloc 2");
            let third = db.allocate_segment_id("events").expect("alloc 3");
            assert_eq!(first, 0);
            assert_eq!(second, 1);
            assert_eq!(third, 2);
            assert_eq!(db.manifest().tables["events"].next_segment_id, 3);
        }
        // Counter survives reopen — §5.2 requires `segment_id` to be
        // unique within a table, so its counter must be durable
        // across restarts.
        let mut db = Database::open(scratch.path()).expect("open");
        assert_eq!(db.manifest().tables["events"].next_segment_id, 3);
        let fourth = db.allocate_segment_id("events").expect("alloc 4");
        assert_eq!(fourth, 3);
        assert_eq!(db.manifest().tables["events"].next_segment_id, 4);
    }

    #[test]
    fn allocate_segment_id_unknown_table_leaves_state_untouched() {
        let scratch = Scratch::new("segment-id-unknown");
        let mut db = Database::create(scratch.path()).expect("create");
        let before_mem = db.manifest().clone();
        let before_disk = fs::read(scratch.path().join(MANIFEST_FILE_NAME)).unwrap();

        let err = db
            .allocate_segment_id("missing")
            .expect_err("unknown table must error");
        match err {
            BqliteError::Execution(msg) => {
                assert!(msg.contains("unknown table"), "got: {msg}");
                assert!(msg.contains("missing"), "got: {msg}");
            }
            other => panic!("expected Execution, got {other:?}"),
        }

        assert_eq!(&before_mem, db.manifest());
        let after_disk = fs::read(scratch.path().join(MANIFEST_FILE_NAME)).unwrap();
        assert_eq!(before_disk, after_disk);
    }

    #[test]
    fn allocate_segment_id_is_independent_of_batch_id_counter() {
        // The two counters live side-by-side on TableEntry and must
        // not share state; bumping one must not observe the other.
        let scratch = Scratch::new("segment-id-vs-batch-id");
        let mut db = create_db_with_events(scratch.path());

        let batch0 = db.allocate_batch_id("events").unwrap();
        let seg0 = db.allocate_segment_id("events").unwrap();
        let batch1 = db.allocate_batch_id("events").unwrap();
        let seg1 = db.allocate_segment_id("events").unwrap();

        assert_eq!(batch0, 0);
        assert_eq!(batch1, 1);
        assert_eq!(seg0, 0);
        assert_eq!(seg1, 1);
    }

    // ── allocate_sequence_id_range ─────────────────────────────────────────

    #[test]
    fn allocate_sequence_id_range_reserves_contiguous_block_and_persists() {
        let scratch = Scratch::new("seq-range-persist");
        {
            let mut db = create_db_with_events(scratch.path());
            let (lo, hi) = db.allocate_sequence_id_range("events", 10).unwrap();
            assert_eq!(lo, 0);
            assert_eq!(hi, 9);
            assert_eq!(db.manifest().tables["events"].next_sequence_id, 10);

            // Second call picks up from the next free id.
            let (lo2, hi2) = db.allocate_sequence_id_range("events", 5).unwrap();
            assert_eq!(lo2, 10);
            assert_eq!(hi2, 14);
            assert_eq!(db.manifest().tables["events"].next_sequence_id, 15);
        }
        // Reopen and verify the counter persisted through fsync.
        let db = Database::open(scratch.path()).expect("open");
        assert_eq!(db.manifest().tables["events"].next_sequence_id, 15);
    }

    #[test]
    fn allocate_sequence_id_range_single_row_reservation() {
        // A one-row segment still produces a well-defined inclusive
        // range where `lo == hi`.
        let scratch = Scratch::new("seq-range-one");
        let mut db = create_db_with_events(scratch.path());
        let (lo, hi) = db.allocate_sequence_id_range("events", 1).unwrap();
        assert_eq!(lo, 0);
        assert_eq!(hi, 0);
        assert_eq!(db.manifest().tables["events"].next_sequence_id, 1);
    }

    #[test]
    fn allocate_sequence_id_range_zero_count_is_rejected() {
        let scratch = Scratch::new("seq-range-zero");
        let mut db = create_db_with_events(scratch.path());
        let err = db
            .allocate_sequence_id_range("events", 0)
            .expect_err("zero count must error");
        match err {
            BqliteError::Execution(msg) => {
                assert!(msg.contains("count must be greater than 0"), "got: {msg}");
            }
            other => panic!("expected Execution, got {other:?}"),
        }
        // Nothing changed.
        assert_eq!(db.manifest().tables["events"].next_sequence_id, 0);
    }

    #[test]
    fn allocate_sequence_id_range_unknown_table_leaves_state_untouched() {
        let scratch = Scratch::new("seq-range-unknown");
        let mut db = Database::create(scratch.path()).expect("create");
        let before_mem = db.manifest().clone();
        let before_disk = fs::read(scratch.path().join(MANIFEST_FILE_NAME)).unwrap();

        let err = db
            .allocate_sequence_id_range("missing", 10)
            .expect_err("unknown table must error");
        match err {
            BqliteError::Execution(msg) => {
                assert!(msg.contains("unknown table"), "got: {msg}");
                assert!(msg.contains("missing"), "got: {msg}");
            }
            other => panic!("expected Execution, got {other:?}"),
        }

        assert_eq!(&before_mem, db.manifest());
        let after_disk = fs::read(scratch.path().join(MANIFEST_FILE_NAME)).unwrap();
        assert_eq!(before_disk, after_disk);
    }

    #[test]
    fn allocate_sequence_id_range_overflow_is_rejected() {
        // Pre-load the counter to near-MAX by hand-writing the
        // manifest, then attempt a reservation that would overflow.
        let scratch = Scratch::new("seq-range-overflow");
        {
            let _db = create_db_with_events(scratch.path());
        }
        let manifest_path = scratch.path().join(MANIFEST_FILE_NAME);
        let bytes = fs::read(&manifest_path).unwrap();
        let mut m: Manifest = serde_json::from_slice(&bytes).unwrap();
        m.tables.get_mut("events").unwrap().next_sequence_id = u64::MAX - 5;
        fs::write(&manifest_path, serde_json::to_vec_pretty(&m).unwrap()).unwrap();

        let mut db = Database::open(scratch.path()).expect("open");
        // Reserving exactly 5 is fine (yields [MAX-5, MAX-1]).
        let (lo, hi) = db.allocate_sequence_id_range("events", 5).unwrap();
        assert_eq!(lo, u64::MAX - 5);
        assert_eq!(hi, u64::MAX - 1);
        // Reserving 2 more overflows the counter (next_free = MAX + 1).
        let err = db
            .allocate_sequence_id_range("events", 2)
            .expect_err("overflow must error");
        assert!(matches!(err, BqliteError::Execution(_)));
    }

    // ── Startup orphan reconciliation (TASK-239) ───────────────────

    /// Helper: build a shard directory path matching the writer's
    /// `segment_file_path` layout.
    fn shard_dir_path(root: &Path, table: &str, window_id: u32, shard_id: usize) -> PathBuf {
        root.join(table)
            .join("windows")
            .join(format!("w_{window_id:06}"))
            .join(format!("shard_{shard_id:02}"))
    }

    /// Helper: write a manifest that includes the given segment in the
    /// given `(table, window, shard)` cell. Overwrites whatever was on
    /// disk before, so the test can simulate a particular crash state.
    fn write_manifest_with_segment(
        root: &Path,
        table: &str,
        window_id: u32,
        shard_id: usize,
        segment_id: u64,
        shard_count: u16,
    ) {
        use crate::manifest::{ColumnStats, SegmentMeta, WindowManifest};
        let manifest_path = root.join(MANIFEST_FILE_NAME);
        let bytes = fs::read(&manifest_path).unwrap();
        let mut m: Manifest = serde_json::from_slice(&bytes).unwrap();

        let seg = SegmentMeta {
            segment_id,
            level: 0,
            schema_version: 1,
            row_count: 10,
            byte_size: 512,
            ts_range: (0, 1_000_000),
            entity_range: (
                bqlite_core::property::PropertyValue::String("a".into()),
                bqlite_core::property::PropertyValue::String("z".into()),
            ),
            column_stats: vec![ColumnStats {
                column_name: "entity_id".into(),
                min: Some(bqlite_core::property::PropertyValue::String("a".into())),
                max: Some(bqlite_core::property::PropertyValue::String("z".into())),
                null_count: 0,
                distinct_count_estimate: None,
            }],
            created_at: 1_000_000,
            batch_id: 0,
            seq_id_range: (0, 9),
        };

        let entry = m.tables.get_mut(table).unwrap();
        // Find or create the WindowManifest for the given window_id.
        let wm = entry.windows.iter_mut().find(|w| w.window_id == window_id);
        match wm {
            Some(w) => {
                w.shards[shard_id].push(seg);
            }
            None => {
                let mut shards = vec![Vec::new(); shard_count as usize];
                shards[shard_id] = vec![seg];
                entry.windows.push(WindowManifest { window_id, shards });
            }
        }
        entry.next_segment_id = segment_id + 1;

        fs::write(&manifest_path, serde_json::to_vec_pretty(&m).unwrap()).unwrap();
    }

    #[test]
    fn open_cleans_up_orphan_tmp_segment_file() {
        // Simulate a crashed ingest: a .tmp segment file exists but the
        // manifest was never updated. On reopen the file must be gone.
        let scratch = Scratch::new("cleanup-tmp");
        {
            let _db = create_db_with_events(scratch.path());
        }

        // Plant an orphan .tmp file in a shard directory.
        let shard = shard_dir_path(scratch.path(), "events", 1, 0);
        fs::create_dir_all(&shard).unwrap();
        let orphan = shard.join("segment_42.seg.tmp");
        fs::write(&orphan, b"partial-write").unwrap();
        assert!(orphan.exists());

        // Reopen — the reconciliation pass runs during open.
        {
            let _db = Database::open(scratch.path()).expect("open");
            assert!(!orphan.exists(), ".tmp orphan must be deleted on open");
        }
    }

    #[test]
    fn open_cleans_up_orphaned_seg_file_not_in_manifest() {
        // Simulate a completed compaction whose old input segments were
        // not deleted before the process stopped. The new manifest
        // references only the merged output; the old inputs are orphans.
        let scratch = Scratch::new("cleanup-orphan-seg");
        {
            let _db = create_db_with_events(scratch.path());
        }

        let shard = shard_dir_path(scratch.path(), "events", 1, 0);
        fs::create_dir_all(&shard).unwrap();

        // Register segment 20 as active in the manifest.
        write_manifest_with_segment(
            scratch.path(),
            "events",
            1,
            0,
            20,
            crate::manifest::DEFAULT_SHARD_COUNT,
        );

        // Write the active segment file plus two orphan old inputs.
        fs::write(shard.join("segment_20.seg"), b"active").unwrap();
        fs::write(shard.join("segment_10.seg"), b"orphan-old").unwrap();
        fs::write(shard.join("segment_11.seg"), b"orphan-old").unwrap();

        {
            let _db = Database::open(scratch.path()).expect("open");
            assert!(
                shard.join("segment_20.seg").exists(),
                "active segment must survive"
            );
            assert!(
                !shard.join("segment_10.seg").exists(),
                "orphan input must be removed"
            );
            assert!(
                !shard.join("segment_11.seg").exists(),
                "orphan input must be removed"
            );
        }
    }

    #[test]
    fn open_with_no_orphans_leaves_active_segments_intact() {
        // Happy path: no orphans exist. The manifest's active segment
        // must survive untouched.
        let scratch = Scratch::new("cleanup-happy");
        {
            let _db = create_db_with_events(scratch.path());
        }

        let shard = shard_dir_path(scratch.path(), "events", 1, 0);
        fs::create_dir_all(&shard).unwrap();

        write_manifest_with_segment(
            scratch.path(),
            "events",
            1,
            0,
            5,
            crate::manifest::DEFAULT_SHARD_COUNT,
        );
        fs::write(shard.join("segment_5.seg"), b"data").unwrap();

        {
            let db = Database::open(scratch.path()).expect("open");
            assert!(shard.join("segment_5.seg").exists());
            // The manifest itself is unchanged — still has the segment.
            let entry = db.manifest().tables.get("events").unwrap();
            assert_eq!(entry.windows[0].shards[0].len(), 1);
            assert_eq!(entry.windows[0].shards[0][0].segment_id, 5);
        }
    }

    #[test]
    fn open_after_drop_table_cleans_orphan_segments() {
        // A table was dropped (manifest no longer references it) but
        // its segment files remain on disk. Reopen must clean them.
        let scratch = Scratch::new("cleanup-drop-table");
        {
            let mut db = Database::create(scratch.path()).expect("create");
            // Add a second table, register a segment, then drop it so
            // the manifest no longer references those segments.
            db.create_table("clicks".into(), sample_events_schema())
                .unwrap();
        }

        // Manually plant segment files for the "clicks" table.
        let shard = shard_dir_path(scratch.path(), "clicks", 1, 0);
        fs::create_dir_all(&shard).unwrap();
        fs::write(shard.join("segment_0.seg"), b"orphan").unwrap();
        fs::write(shard.join("segment_1.seg.tmp"), b"orphan-tmp").unwrap();

        // Now drop the table from the manifest (simulate: we write
        // directly because we don't have the segment in manifest).
        {
            let mut db = Database::open(scratch.path()).expect("open-to-drop");
            db.drop_table("clicks").unwrap();
        }

        // Reopen — the reconciliation pass should clean up.
        {
            let _db = Database::open(scratch.path()).expect("open-after-drop");
            assert!(
                !shard.join("segment_0.seg").exists(),
                "orphan .seg must be removed"
            );
            assert!(
                !shard.join("segment_1.seg.tmp").exists(),
                "orphan .tmp must be removed"
            );
        }
    }
}
