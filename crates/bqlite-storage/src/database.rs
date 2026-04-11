//! Database — the Wave 1 storage bootstrap entry point.
//!
//! [`Database::open_or_create`] implements the v0 database-open
//! contract from `docs/design/storage-format.md` §5 + §12 + §14 and
//! `docs/reliability.md`: it creates the directory layout, acquires
//! an exclusive lock, and reads or initializes the manifest atomically.
//!
//! # On-disk layout (Wave 1)
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
//! top-level `manifest.json`. No segments are ever written yet.
//! [`crate::manifest`] documents the split and the forward-compat
//! plan.
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
//! # Concurrency
//!
//! [`Database::open_or_create`] acquires an exclusive lock on
//! `<root>/.lock` via `std::fs::File::try_lock` (stabilized in Rust
//! 1.89). A second concurrent open from the same process or from a
//! different process returns [`BqliteError::Execution`] with a clear
//! message. The lock releases when the owning [`Database`] is
//! dropped. See `storage-format.md` §14.1.
//!
//! # Future extensions
//!
//! - Per-table manifests (later wave) — the open path learns to walk
//!   a `tables/` subdirectory instead of the single `manifest.json`.
//! - Real segment I/O — [`Database::segment_reader`] currently hands
//!   out an empty reader; later waves return a snapshot of the live
//!   segment inventory (`storage-format.md` §7.6).
//! - Format-version migration — a later wave reads older manifests
//!   and upgrades them in-place.

use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bqlite_core::error::{BqliteError, Result};
use bqlite_core::schema::TableSchema;
use bqlite_core::storage::{
    ColumnProjection, Predicate, SegmentHandle, SegmentReader, SegmentScan,
};
use bqlite_core::time::TimeRange;

use crate::catalog::ManifestCatalog;
use crate::manifest::{Manifest, SegmentMeta, DEFAULT_SHARD_COUNT, MANIFEST_FORMAT_VERSION};

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
}

impl Database {
    /// Open an existing database, or initialize a fresh one if `path`
    /// does not yet contain a manifest.
    ///
    /// Equivalent to [`Database::open_or_create_with_shards`] with
    /// [`DEFAULT_SHARD_COUNT`]. The shard count is honoured only on a
    /// fresh init — reopening an existing database preserves the
    /// manifest's recorded value.
    pub fn open_or_create(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_or_create_with_shards(path, DEFAULT_SHARD_COUNT)
    }

    /// Open an existing database, or initialize a fresh one with the
    /// specified shard count.
    ///
    /// # Arguments
    ///
    /// - `path` — database root directory. Created (recursively) if
    ///   it does not yet exist.
    /// - `shard_count` — number of hash shards to stamp into the
    ///   manifest on a fresh init. Honoured only on init: reopening
    ///   an existing database ignores this argument and uses the
    ///   manifest's recorded value, because shard count is fixed for
    ///   the lifetime of a database (see
    ///   `docs/design/storage-format.md` §5.1).
    ///
    /// # Errors
    ///
    /// - [`BqliteError::Schema`] if `shard_count == 0`.
    /// - [`BqliteError::Io`] on I/O failures creating the directory,
    ///   opening the lock file, or writing the manifest.
    /// - [`BqliteError::Execution`] if the database is already open
    ///   by another process (lock held), or if the existing manifest
    ///   is corrupt or has an unsupported `format_version`.
    pub fn open_or_create_with_shards(path: impl AsRef<Path>, shard_count: u16) -> Result<Self> {
        if shard_count == 0 {
            return Err(BqliteError::Schema("shard_count must be at least 1".into()));
        }

        let root = path.as_ref().to_path_buf();
        // `create_dir_all` is idempotent. If two processes race on a
        // brand-new directory, both may observe success here, but only
        // one will win `try_lock` below; the loser gets a clean
        // `Execution("already open")` error.
        fs::create_dir_all(&root).map_err(|e| io_ctx("create database directory", &root, e))?;

        // Acquire the exclusive lock first, so any subsequent manifest
        // read/write is serialized across processes on this directory.
        let lock = acquire_lock(&root)?;

        // Clean up any stale `manifest.json.tmp` left over from a
        // crash during a prior atomic update. The rename in
        // `write_manifest_atomic` is atomic, so a stale temp file
        // necessarily belongs to an aborted update whose target
        // manifest is either still the old contents or has already
        // been replaced by the new contents — either way, the temp
        // file is orphaned and safe to delete. Unconditional
        // `remove_file` plus ignoring `NotFound` is one syscall fewer
        // than the check-then-remove dance.
        let tmp_path = root.join(MANIFEST_TMP_FILE_NAME);
        match fs::remove_file(&tmp_path) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(_) => {
                // Best-effort: a stale tmp file we can't delete is
                // harmless — the next atomic write will overwrite it.
            }
        }

        // Read or initialize the manifest.
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
                // Fresh init — write the new manifest before
                // returning so an observer opening the same directory
                // after this call sees a complete, versioned file.
                //
                // TASK-125: seed the bootstrap `events` table so
                // `bqlite query "events"` can parse-plan-execute
                // against a freshly-created database without a
                // CREATE TABLE DDL path (which does not exist in v0).
                // See `crate::catalog` for the rationale.
                let mut m = Manifest::new_empty(shard_count);
                m.tables.insert(
                    crate::catalog::BOOTSTRAP_EVENTS_TABLE_NAME.to_string(),
                    crate::manifest::TableEntry {
                        schema: crate::catalog::bootstrap_events_schema(),
                        next_sequence_id: 0,
                        next_batch_id: 0,
                        bootstrap_events_table: true,
                        windows: Vec::new(),
                    },
                );
                write_manifest_atomic(&root, &m)?;
                m
            }
            Err(e) => {
                return Err(io_ctx("read manifest", &manifest_path, e));
            }
        };

        Ok(Self {
            root,
            manifest,
            _lock: lock,
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
    /// [`Database::open_or_create`] ran; Wave 1 has no mutation API,
    /// so the snapshot is effectively static for the database's
    /// lifetime.
    pub fn catalog(&self) -> ManifestCatalog<'_> {
        ManifestCatalog::new(&self.manifest)
    }

    /// Open a segment reader for `table_name`.
    ///
    /// Returns [`BqliteError::Plan`] if no such table exists in the
    /// manifest (via [`bqlite_core::catalog::unknown_table_error`] so
    /// the error format matches the `Catalog` trait's).
    ///
    /// The Wave 1 reader yields zero segments — real segment
    /// enumeration lands in later waves. A Wave 1 scan over the
    /// default bootstrap `events` table (seeded by TASK-125) therefore
    /// produces an empty result, which is exactly what the Wave 1
    /// smoke test (TASK-123) expects.
    pub fn segment_reader(&self, table_name: &str) -> Result<Box<dyn SegmentReader>> {
        let entry = self
            .manifest
            .tables
            .get(table_name)
            .ok_or_else(|| bqlite_core::catalog::unknown_table_error(table_name))?;
        Ok(Box::new(EmptySegmentReader {
            schema: entry.schema.clone(),
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

    /// Apply `f` to a mutable copy of the current manifest, persist
    /// the result atomically, then adopt it as the new in-memory
    /// state.
    ///
    /// The clone-apply-persist-swap shape guarantees that a failing
    /// closure or a failing disk write leaves both the on-disk
    /// manifest and `self.manifest` untouched — callers only observe
    /// the mutation when every step succeeds. The clone cost is
    /// insignificant at Wave 2 scale (a handful of tables with a
    /// handful of segments each).
    ///
    /// Private for now because the only callers are the three
    /// `add_segment` / `remove_segment` / `snapshot_for_query`
    /// wrappers above. Future ingest and compaction tasks that want
    /// to batch multiple mutations into one fsync can promote this
    /// to `pub(crate)` — a later wave concern.
    fn update_manifest<R, F>(&mut self, f: F) -> Result<R>
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

/// `SegmentReader` implementation that owns a schema but yields no
/// segments. Used by Wave 1 for both [`Database::segment_reader`] and
/// the standalone [`empty_segment_reader`] constructor.
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
        // segments() returns an empty iterator, so a caller cannot
        // legitimately supply a handle this reader produced; any
        // handle we see here is stale or foreign. The trait docs
        // specify `BqliteError::Execution` for exactly this case.
        Err(BqliteError::Execution(
            "no segments are visible in the Wave 1 empty segment reader".into(),
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
            // open_or_create path must be able to create it itself.
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

    // ── Fresh init ──────────────────────────────────────────────────────────

    #[test]
    fn open_or_create_creates_missing_directory() {
        let scratch = Scratch::new("creates-dir");
        assert!(
            !scratch.path().exists(),
            "precondition: scratch directory should not yet exist"
        );

        let db = Database::open_or_create(scratch.path()).expect("init");
        assert!(scratch.path().is_dir(), "root dir was created");
        assert!(
            scratch.path().join(LOCK_FILE_NAME).is_file(),
            ".lock file was created"
        );
        assert!(
            scratch.path().join(MANIFEST_FILE_NAME).is_file(),
            "manifest.json was created"
        );
        assert!(
            !scratch.path().join(MANIFEST_TMP_FILE_NAME).exists(),
            "manifest.json.tmp is absent after atomic init"
        );
        drop(db);
    }

    #[test]
    fn fresh_manifest_has_default_shape() {
        let scratch = Scratch::new("default-shape");
        let db = Database::open_or_create(scratch.path()).expect("init");
        let m = db.manifest();
        assert_eq!(m.format_version, MANIFEST_FORMAT_VERSION);
        assert_eq!(m.shard_count, DEFAULT_SHARD_COUNT);
        // UUIDv4 hyphenated form: 36 chars, 4 dashes.
        assert_eq!(m.database_uuid.len(), 36);
        assert_eq!(m.database_uuid.matches('-').count(), 4);

        // TASK-125: fresh init seeds a single bootstrap `events`
        // table so the planner has something to resolve on v0
        // databases where CREATE TABLE DDL does not yet exist.
        assert_eq!(m.tables.len(), 1, "exactly one bootstrap table");
        let entry = m
            .tables
            .get(crate::catalog::BOOTSTRAP_EVENTS_TABLE_NAME)
            .expect("events table was seeded");
        assert_eq!(entry.schema, crate::catalog::bootstrap_events_schema());
        assert_eq!(entry.next_sequence_id, 0);
        assert_eq!(entry.next_batch_id, 0);
        assert!(
            entry.bootstrap_events_table,
            "bootstrap entries must carry the flag so later waves can retire the shortcut"
        );
    }

    #[test]
    fn fresh_init_catalog_resolves_bootstrap_events_table() {
        // End-to-end check: the TASK-125 catalog handle handed out
        // from the Database must return the bootstrap schema and
        // list the events table by name.
        use bqlite_core::Catalog;

        let scratch = Scratch::new("catalog-events");
        let db = Database::open_or_create(scratch.path()).expect("init");
        let cat = db.catalog();
        let schema = cat.resolve_table("events").expect("events must resolve");
        assert_eq!(schema, crate::catalog::bootstrap_events_schema());
        assert_eq!(cat.list_tables(), vec!["events"]);
    }

    #[test]
    fn catalog_from_reopened_database_still_sees_bootstrap() {
        // The bootstrap runs only on fresh init (not on reopen), but
        // the seeded entry must survive the round-trip to disk.
        use bqlite_core::Catalog;

        let scratch = Scratch::new("catalog-reopen");
        {
            let _db = Database::open_or_create(scratch.path()).expect("init");
        }
        let db = Database::open_or_create(scratch.path()).expect("reopen");
        let cat = db.catalog();
        assert!(cat.resolve_table("events").is_ok());
        // The flag persists across reopen — important because later
        // waves scan for it to distinguish seeded vs user-created
        // tables.
        let entry = db
            .manifest()
            .tables
            .get("events")
            .expect("events entry survived reopen");
        assert!(entry.bootstrap_events_table);
    }

    #[test]
    fn fresh_init_honours_custom_shard_count() {
        let scratch = Scratch::new("custom-shards");
        let db = Database::open_or_create_with_shards(scratch.path(), 16).expect("init");
        assert_eq!(db.manifest().shard_count, 16);
    }

    #[test]
    fn zero_shard_count_is_rejected() {
        let scratch = Scratch::new("zero-shards");
        let err = Database::open_or_create_with_shards(scratch.path(), 0)
            .expect_err("zero shards must be rejected");
        assert!(matches!(err, BqliteError::Schema(_)), "got {err:?}");
    }

    // ── Reopen semantics ────────────────────────────────────────────────────

    #[test]
    fn reopening_preserves_database_uuid() {
        let scratch = Scratch::new("uuid-stable");
        let original_uuid = {
            let db = Database::open_or_create(scratch.path()).expect("init");
            db.manifest().database_uuid.clone()
        };
        let db2 = Database::open_or_create(scratch.path()).expect("reopen");
        assert_eq!(db2.manifest().database_uuid, original_uuid);
    }

    #[test]
    fn reopening_ignores_shard_count_override() {
        let scratch = Scratch::new("shards-fixed");
        {
            let _db = Database::open_or_create_with_shards(scratch.path(), 4)
                .expect("init with 4 shards");
        }
        let db2 = Database::open_or_create_with_shards(scratch.path(), 99).expect("reopen");
        assert_eq!(
            db2.manifest().shard_count,
            4,
            "reopen must honour the manifest's recorded shard count"
        );
    }

    // ── Atomicity and cleanup ───────────────────────────────────────────────

    #[test]
    fn stale_manifest_tmp_is_cleaned_up_on_open() {
        let scratch = Scratch::new("stale-tmp");
        // Initialize a real manifest so the open path doesn't treat
        // this directory as fresh.
        {
            let _db = Database::open_or_create(scratch.path()).expect("init");
        }
        // Simulate a crash mid-update: leave a stale tmp file behind.
        let tmp_path = scratch.path().join(MANIFEST_TMP_FILE_NAME);
        fs::write(&tmp_path, b"garbage left behind by a prior crash").unwrap();
        assert!(tmp_path.exists());

        let _db = Database::open_or_create(scratch.path()).expect("reopen");
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
        let err =
            Database::open_or_create(scratch.path()).expect_err("corrupted manifest must error");
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
        // Hand-write a manifest with a future format_version.
        let future = format!(
            r#"{{"format_version":{},"database_uuid":"00000000-0000-4000-8000-000000000000","shard_count":32,"tables":{{}},"segments":[]}}"#,
            MANIFEST_FORMAT_VERSION + 1
        );
        fs::write(scratch.path().join(MANIFEST_FILE_NAME), future).unwrap();

        let err = Database::open_or_create(scratch.path())
            .expect_err("unsupported format_version must error");
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
        let first = Database::open_or_create(scratch.path()).expect("first open");
        let err =
            Database::open_or_create(scratch.path()).expect_err("second concurrent open must fail");
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
            let _first = Database::open_or_create(scratch.path()).expect("first open");
        }
        // After drop, a fresh open must succeed.
        let _second = Database::open_or_create(scratch.path()).expect("reopen after drop");
    }

    // ── SegmentReader stub ──────────────────────────────────────────────────

    #[test]
    fn segment_reader_unknown_table_returns_plan_error() {
        let scratch = Scratch::new("reader-unknown");
        let db = Database::open_or_create(scratch.path()).expect("init");
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
            let _db = Database::open_or_create(scratch.path()).expect("init");
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
                bootstrap_events_table: true,
                windows: Vec::new(),
            },
        );
        fs::write(&manifest_path, serde_json::to_vec_pretty(&m).unwrap()).unwrap();

        let db = Database::open_or_create(scratch.path()).expect("reopen");
        let entry = db.manifest().tables.get("events").expect("events entry");
        assert_eq!(entry.next_sequence_id, 123_456);
        assert_eq!(entry.next_batch_id, 78);
        assert!(entry.bootstrap_events_table);
    }

    #[test]
    fn segment_reader_for_bootstrap_events_table_yields_zero_segments() {
        // With TASK-125 merged, fresh init seeds the events table
        // automatically — this test just opens a fresh database and
        // asks for the reader directly.
        let scratch = Scratch::new("reader-bootstrap");
        let db = Database::open_or_create(scratch.path()).expect("init");
        let reader = db.segment_reader("events").expect("reader for events");
        assert_eq!(reader.schema().name(), "events");
        assert_eq!(reader.segments().count(), 0);
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
        }
    }

    #[test]
    fn add_segment_persists_across_reopen() {
        let scratch = Scratch::new("add-persists");
        {
            let mut db = Database::open_or_create(scratch.path()).expect("init");
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
        let db = Database::open_or_create(scratch.path()).expect("reopen");
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
            let mut db = Database::open_or_create(scratch.path()).expect("init");
            db.add_segment("events", 0, 0, sample_segment(1, 1, (0, 50)))
                .unwrap();
            db.add_segment("events", 0, 0, sample_segment(2, 1, (50, 100)))
                .unwrap();
            let removed = db.remove_segment("events", 1).expect("remove");
            assert_eq!(removed.segment_id, 1);
            // In-memory snapshot reflects the removal.
            assert_eq!(db.manifest().tables["events"].windows[0].shards[0].len(), 1);
        }
        let db = Database::open_or_create(scratch.path()).expect("reopen");
        let shard = &db.manifest().tables["events"].windows[0].shards[0];
        assert_eq!(shard.len(), 1);
        assert_eq!(shard[0].segment_id, 2);
    }

    #[test]
    fn snapshot_for_query_reflects_committed_state() {
        let scratch = Scratch::new("snapshot-wiring");
        let mut db = Database::open_or_create(scratch.path()).expect("init");
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

    #[test]
    fn add_segment_unknown_table_leaves_manifest_unchanged() {
        // The clone-apply-persist-swap shape means a failing mutation
        // must leave both disk and memory exactly as they were. This
        // test asserts it end-to-end by comparing the serialized
        // manifest before and after a failed call.
        let scratch = Scratch::new("add-unknown");
        let mut db = Database::open_or_create(scratch.path()).expect("init");
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
        let mut db = Database::open_or_create(scratch.path()).expect("init");
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
        let mut db = Database::open_or_create(scratch.path()).expect("init");
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
        let mut db = Database::open_or_create(scratch.path()).expect("init");
        db.add_segment("events", 0, 0, sample_segment(1, 1, (0, 1)))
            .unwrap();
        assert!(
            !scratch.path().join(MANIFEST_TMP_FILE_NAME).exists(),
            "tmp file is renamed away by write_manifest_atomic"
        );
    }
}
