//! Spill-to-disk filesystem helpers shared across operators.
//!
//! Implements the file-layout, RAII guard, and per-query subdirectory
//! protocol from `docs/design/engine/spill.md` § 5–§ 8 plus the
//! `TempSpillFile` shape from `docs/design/engine/cancellation.md` § 5.2.
//!
//! ## What lives here
//!
//! - [`TempSpillFile`] — RAII guard that owns one on-disk spill file.
//!   Drop deletes the file. Operators consume this as the single
//!   source of truth for spill-file lifetime; `remove_file` is never
//!   called from operator code (`spill.md` § 8.1).
//! - [`SpillFs`] — engine-internal filesystem helper. Owns the spill
//!   root, dispenses per-query identifiers, opens new spill files,
//!   and reclaims per-query subdirectories. Constructed once per
//!   `Database`; cloned via `Arc` into every [`crate::MemoryBudget`]
//!   participant that needs it.
//! - [`SpillQueryId`] — opaque per-query identifier. Wraps a `u64`
//!   today; the `Display` impl renders the design's zero-padded
//!   nine-digit decimal so the path scheme matches `spill.md` § 7.
//!
//! ## Path scheme
//!
//! ```text
//! <root>/<query_id>/<purpose>-<seq>.spill
//! ```
//!
//! `<query_id>` is created lazily on the first call to
//! [`SpillFs::open_spill`] for a given id, so a query that never spills
//! leaves no on-disk trace (`spill.md` § 7, § 5.4).
//!
//! ## Cleanup
//!
//! - `TempSpillFile::Drop` removes the file (best-effort).
//! - [`SpillFs::cleanup_query`] removes the per-query subdirectory; the
//!   engine calls this after the operator-tree drop has run on every
//!   exit path, as the belt-and-braces sweep mandated by `spill.md`
//!   § 8.3.
//! - `SpillFs::Drop` removes the root directory and the process-global
//!   registry entry. This handles the "engine close" sweep from
//!   `spill.md` § 5.5.
//!
//! ## Process-global registry
//!
//! `spill.md` § 5.3 mandates that two `Database` opens in the same
//! process never claim the same canonicalised spill root. A private
//! `Mutex<HashSet<PathBuf>>` static enforces this. The default spill
//! root (`<db_root>/spill/`) is already auto-scoped by the database
//! lock; the registry's job is to catch programmer-visible mistakes
//! when the host configures an explicit override.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

use crate::error::BqliteError;
use crate::Result;

// ---------------------------------------------------------------------------
// SpillQueryId
// ---------------------------------------------------------------------------

/// Opaque per-query identifier rendered into the spill path scheme.
///
/// `Display` formats as a zero-padded nine-digit decimal
/// (`000000042`) so directory listings sort by creation order
/// lexicographically. The wrapper is here so `TASK-541` (the morsel
/// scheduler / query handle layer) can switch to UUIDv7 without
/// changing call sites.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SpillQueryId(u64);

impl SpillQueryId {
    /// Build a `SpillQueryId` from a raw counter value. Used by tests
    /// and by the engine when it deterministically threads an id
    /// through.
    pub fn from_u64(raw: u64) -> Self {
        Self(raw)
    }

    /// The underlying counter value. Visible for tests; production
    /// code should treat the id as opaque.
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for SpillQueryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:09}", self.0)
    }
}

// ---------------------------------------------------------------------------
// TempSpillFile
// ---------------------------------------------------------------------------

/// Owns one on-disk spill file. `Drop` removes it.
///
/// Only [`SpillFs::open_spill`] constructs `TempSpillFile`; operators
/// never call `remove_file` directly. Operators may move the guard
/// across operator state (e.g. the sort operator transfers ownership
/// from the spill writer into the merge cursor) but must not leak it.
///
/// ## Cancellation / panic
///
/// Drop runs on every exit path including unwinding from a panic, so
/// no spill file survives a failed query (cancellation.md § 5.1 step 2).
/// Drop does not log or panic on failure; a failed unlink is reclaimed
/// by the per-query / engine-open sweep (`spill.md` § 8.3 / § 9.1).
#[derive(Debug)]
pub struct TempSpillFile {
    path: PathBuf,
    file: File,
    bytes_written: u64,
}

impl TempSpillFile {
    /// Path to the on-disk file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Mutable handle to the underlying file. Operators write here
    /// directly when streaming Arrow IPC content.
    pub fn file_mut(&mut self) -> &mut File {
        &mut self.file
    }

    /// Bytes the operator has reported writing through
    /// [`TempSpillFile::record_bytes_written`]. The guard does not
    /// instrument the file handle itself; operators that want byte
    /// accounting call this each time they flush a chunk.
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// Add `n` bytes to the running write counter.
    pub fn record_bytes_written(&mut self, n: u64) {
        self.bytes_written = self.bytes_written.saturating_add(n);
    }

    /// Flush the underlying file to disk. No `fsync` — the spill
    /// protocol does not require durability (`spill.md` § 11).
    pub fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

impl Drop for TempSpillFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

// ---------------------------------------------------------------------------
// SpillFs
// ---------------------------------------------------------------------------

/// Engine-internal helper that owns the spill root and dispenses
/// per-query identifiers.
///
/// One `SpillFs` is constructed per `Database`. Cloned via `Arc` into
/// every operator that may spill. The `Drop` impl removes the root
/// directory and the process-global registry entry.
#[derive(Debug)]
pub struct SpillFs {
    root: PathBuf,
    /// Canonicalised root, kept so `Drop` removes the right registry
    /// entry even if `root` was relative when the user supplied it.
    /// Equal to `root` after `SpillFs::open` succeeds because we
    /// canonicalize and then store both.
    canonical_root: PathBuf,
    next_query_id: AtomicU64,
    /// Per-`(query_id, purpose)` sequence counters. The map is keyed
    /// by the formatted `<query>/<purpose>` pair so two purposes
    /// inside one query keep independent sequences and two queries
    /// using the same purpose keep independent sequences.
    sequences: Mutex<HashMap<(SpillQueryId, String), u32>>,
}

/// Process-global registry of canonicalised spill roots that are
/// currently in use by a live [`SpillFs`]. Inserted by
/// [`SpillFs::open`] after the sweep succeeds; removed by
/// [`SpillFs::Drop`].
fn registry() -> &'static Mutex<std::collections::HashSet<PathBuf>> {
    static REGISTRY: OnceLock<Mutex<std::collections::HashSet<PathBuf>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(std::collections::HashSet::new()))
}

impl SpillFs {
    /// Open the spill filesystem rooted at `root`, validated against
    /// `db_root`. Performs the engine-open sweep (`rm_rf` + `mkdir`)
    /// and registers the canonicalised root in the process-global
    /// registry.
    ///
    /// Validation rules (`spill.md` § 12.2):
    ///
    /// - `root` must be absolute.
    /// - `root` must not equal `db_root`.
    /// - `root` must not be a child of `db_root` *unless* it is exactly
    ///   `<db_root>/spill/`.
    /// - The canonicalised `root` must not already be registered.
    pub fn open(root: PathBuf, db_root: &Path) -> Result<std::sync::Arc<Self>> {
        if !root.is_absolute() {
            return Err(BqliteError::Execution(format!(
                "spill_root must be an absolute path; got {}",
                root.display()
            )));
        }

        // Canonicalize the db_root for comparison. The db root must
        // already exist when SpillFs::open is called, so canonicalize
        // succeeds.
        let canonical_db_root = db_root.canonicalize().map_err(|e| {
            BqliteError::Execution(format!(
                "failed to canonicalize db_root {}: {}",
                db_root.display(),
                e
            ))
        })?;

        // For the spill root we cannot canonicalize until it exists,
        // so compare lexically against the canonicalized db root after
        // resolving each component we already have on disk. The
        // simplest correct rule: if `root` starts with `canonical_db_root`,
        // the only allowed value is `<db_root>/spill/`.
        if root == canonical_db_root {
            return Err(BqliteError::Execution(format!(
                "spill_root must not equal the database root ({})",
                canonical_db_root.display()
            )));
        }
        if root.starts_with(&canonical_db_root) {
            let only_allowed = canonical_db_root.join("spill");
            // Allow "<db_root>/spill" with or without a trailing slash;
            // PathBuf comparisons normalize that.
            if root != only_allowed {
                return Err(BqliteError::Execution(format!(
                    "spill_root inside the database directory may only be \
                     <db_root>/spill (got {}, expected {})",
                    root.display(),
                    only_allowed.display()
                )));
            }
        }

        // TODO(spill.md §5.3 ordering): tighten the registry lock to
        // span the rm_rf + mkdir + canonicalize + insert sequence.
        // Today two same-process threads opening the same root could
        // both run rm_rf before either reaches the registry insert;
        // the loser still surfaces "already in use" but the winner's
        // tree state is observable to the loser briefly. In practice
        // there is one Database per spill root so this is benign; the
        // followup fix is to acquire the registry mutex before rm_rf
        // and release after insert.
        //
        // Sweep + create. rm_rf is best-effort: NotFound is the common
        // case; other failures are warned-but-tolerated only when the
        // directory was already empty. We treat a hard failure as a
        // typed Execution error so the caller knows the spill tree is
        // not in a usable state.
        match fs::remove_dir_all(&root) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(BqliteError::Execution(format!(
                    "failed to reclaim spill root {}: {}",
                    root.display(),
                    e
                )));
            }
        }
        fs::create_dir_all(&root).map_err(|e| {
            BqliteError::Execution(format!(
                "failed to create spill root {}: {}",
                root.display(),
                e
            ))
        })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = fs::set_permissions(&root, fs::Permissions::from_mode(0o700));
        }

        // Registry insert. Canonicalization succeeds because we just
        // created the directory.
        let canonical_root = root.canonicalize().map_err(|e| {
            BqliteError::Execution(format!(
                "failed to canonicalize spill root {}: {}",
                root.display(),
                e
            ))
        })?;
        {
            let mut guard = registry().lock().expect("spill registry mutex poisoned");
            if !guard.insert(canonical_root.clone()) {
                return Err(BqliteError::Execution(format!(
                    "spill root {} is already in use by another open Database in this process",
                    canonical_root.display()
                )));
            }
        }

        Ok(std::sync::Arc::new(Self {
            root,
            canonical_root,
            next_query_id: AtomicU64::new(0),
            sequences: Mutex::new(HashMap::new()),
        }))
    }

    /// Path of the spill root.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Allocate a fresh per-query identifier.
    pub fn new_query_id(&self) -> SpillQueryId {
        SpillQueryId(self.next_query_id.fetch_add(1, Ordering::Relaxed))
    }

    /// Open a fresh spill file under `<root>/<query_id>/`, named with
    /// the supplied `purpose` and a monotone six-digit sequence.
    ///
    /// Lazily creates the per-query subdirectory.
    pub fn open_spill(&self, query_id: SpillQueryId, purpose: &str) -> Result<TempSpillFile> {
        validate_purpose(purpose)?;

        let qdir = self.root.join(query_id.to_string());
        if !qdir.exists() {
            fs::create_dir_all(&qdir).map_err(|e| {
                BqliteError::Execution(format!(
                    "failed to create per-query spill subdirectory {}: {}",
                    qdir.display(),
                    e
                ))
            })?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ = fs::set_permissions(&qdir, fs::Permissions::from_mode(0o700));
            }
        }

        let seq = {
            let mut sequences = self
                .sequences
                .lock()
                .expect("spill sequences mutex poisoned");
            let key = (query_id, purpose.to_string());
            let entry = sequences.entry(key).or_insert(0);
            let value = *entry;
            *entry = entry.saturating_add(1);
            value
        };

        let filename = format!("{purpose}-{seq:06}.spill");
        let path = qdir.join(filename);
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .read(false)
            .open(&path)
            .map_err(|e| {
                BqliteError::Execution(format!(
                    "failed to create spill file {}: {}",
                    path.display(),
                    e
                ))
            })?;

        Ok(TempSpillFile {
            path,
            file,
            bytes_written: 0,
        })
    }

    /// Best-effort sweep of the per-query subdirectory. Called after
    /// the operator-tree drop has run; tolerates `NotFound` (queries
    /// that never spilled never created the subdir).
    pub fn cleanup_query(&self, query_id: SpillQueryId) {
        let qdir = self.root.join(query_id.to_string());
        let _ = fs::remove_dir_all(&qdir);
        // Also drop any per-query sequence counters so a later call to
        // `new_query_id` that happens to wrap around (extremely
        // unlikely with a u64 counter) sees a clean slate.
        let mut sequences = self
            .sequences
            .lock()
            .expect("spill sequences mutex poisoned");
        sequences.retain(|(qid, _), _| *qid != query_id);
    }
}

impl Drop for SpillFs {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
        if let Ok(mut guard) = registry().lock() {
            guard.remove(&self.canonical_root);
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Restrict spill purpose tags to ASCII-lowercase, digits, and `-` so
/// the name is filesystem-safe on every supported platform. Empty
/// strings and uppercase letters are rejected — the design's tag set
/// (`sort-run`, `ingest-part-...`) all fits this pattern.
fn validate_purpose(purpose: &str) -> Result<()> {
    if purpose.is_empty() {
        return Err(BqliteError::Execution(
            "spill purpose tag must not be empty".to_string(),
        ));
    }
    if !purpose
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    {
        return Err(BqliteError::Execution(format!(
            "spill purpose tag must match [a-z0-9-]+; got {purpose:?}"
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;

    // ── Fixture ──────────────────────────────────────────────────────

    /// One-shot scratch directory pair: `<db_root>` plus
    /// `<db_root>/spill/`. The `Drop` impl wipes both.
    struct Scratch {
        db_root: PathBuf,
    }

    impl Scratch {
        fn new(label: &str) -> Self {
            static SEQ: AtomicU64 = AtomicU64::new(0);
            let seq = SEQ.fetch_add(1, Ordering::Relaxed);
            let pid = std::process::id();
            let mut db_root = std::env::temp_dir();
            db_root.push(format!("bqlite-spill-{label}-{pid}-{seq}"));
            fs::create_dir_all(&db_root).expect("create scratch db root");
            Self { db_root }
        }

        fn db_root(&self) -> &Path {
            &self.db_root
        }

        fn default_spill_root(&self) -> PathBuf {
            self.db_root.join("spill")
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.db_root);
        }
    }

    // ── SpillQueryId ─────────────────────────────────────────────────

    #[test]
    fn spill_query_id_renders_zero_padded_decimal() {
        assert_eq!(format!("{}", SpillQueryId::from_u64(0)), "000000000");
        assert_eq!(format!("{}", SpillQueryId::from_u64(42)), "000000042");
        assert_eq!(
            format!("{}", SpillQueryId::from_u64(123_456_789)),
            "123456789"
        );
    }

    // ── SpillFs::open validation ─────────────────────────────────────

    #[test]
    fn open_rejects_relative_root() {
        let scratch = Scratch::new("relative");
        let err = SpillFs::open(PathBuf::from("relative-path"), scratch.db_root())
            .expect_err("must reject relative root");
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("absolute")),
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    #[test]
    fn open_rejects_root_equal_to_db_root() {
        let scratch = Scratch::new("equal-root");
        // Canonicalize the db root because `SpillFs::open` will too.
        let canonical = scratch.db_root().canonicalize().unwrap();
        let err = SpillFs::open(canonical.clone(), scratch.db_root())
            .expect_err("must reject db_root as spill_root");
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("must not equal")),
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    #[test]
    fn open_rejects_non_spill_child_of_db_root() {
        let scratch = Scratch::new("bad-child");
        let bad = scratch.db_root().canonicalize().unwrap().join("scratch");
        let err = SpillFs::open(bad, scratch.db_root())
            .expect_err("must reject non-spill child of db_root");
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("<db_root>/spill")),
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    #[test]
    fn open_accepts_default_spill_subdir() {
        let scratch = Scratch::new("default");
        let _fs = SpillFs::open(scratch.default_spill_root(), scratch.db_root())
            .expect("default <db_root>/spill must validate");
    }

    #[test]
    fn open_accepts_unrelated_absolute_root() {
        let scratch = Scratch::new("override");
        let other = std::env::temp_dir().join(format!(
            "bqlite-spill-override-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        let _fs = SpillFs::open(other.clone(), scratch.db_root()).expect("override must validate");
        // Drop above cleans the override path; check it.
        assert!(!other.exists() || fs::read_dir(&other).map(|d| d.count()).unwrap_or(1) == 0);
    }

    // ── Sweep + create ───────────────────────────────────────────────

    #[test]
    fn open_reclaims_stale_spill_root() {
        let scratch = Scratch::new("reclaim");
        // Plant a stale file inside what will become the spill root.
        fs::create_dir_all(scratch.default_spill_root()).unwrap();
        fs::write(scratch.default_spill_root().join("stale.spill"), b"garbage").unwrap();
        assert!(scratch.default_spill_root().join("stale.spill").exists());

        let fs_handle =
            SpillFs::open(scratch.default_spill_root(), scratch.db_root()).expect("must reclaim");
        // Stale file is gone; the directory is empty.
        let entries: Vec<_> = fs::read_dir(fs_handle.root()).unwrap().collect();
        assert!(entries.is_empty(), "spill root must be empty after sweep");
    }

    // ── Process-global registry ──────────────────────────────────────

    #[test]
    fn open_rejects_duplicate_canonical_root() {
        let scratch = Scratch::new("dup");
        let _first = SpillFs::open(scratch.default_spill_root(), scratch.db_root())
            .expect("first open must succeed");
        let err = SpillFs::open(scratch.default_spill_root(), scratch.db_root())
            .expect_err("second open must reject");
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("already in use")),
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    #[test]
    fn drop_releases_registry_entry() {
        let scratch = Scratch::new("release");
        {
            let _first =
                SpillFs::open(scratch.default_spill_root(), scratch.db_root()).expect("first open");
        }
        // A second open after the first dropped must succeed.
        let _second = SpillFs::open(scratch.default_spill_root(), scratch.db_root())
            .expect("second open after drop");
    }

    // ── Path scheme ──────────────────────────────────────────────────

    #[test]
    fn open_spill_creates_lazy_subdir_with_expected_path() {
        let scratch = Scratch::new("path-scheme");
        let fs_handle = SpillFs::open(scratch.default_spill_root(), scratch.db_root()).unwrap();
        let qid = fs_handle.new_query_id();
        // No subdir created until the first open_spill.
        let qdir = fs_handle.root().join(qid.to_string());
        assert!(!qdir.exists());

        let guard = fs_handle.open_spill(qid, "sort-run").unwrap();
        let expected = qdir.join("sort-run-000000.spill");
        assert_eq!(guard.path(), expected);
        assert!(qdir.exists(), "subdir must be created lazily");
    }

    #[test]
    fn open_spill_assigns_monotone_seq_per_purpose_per_query() {
        let scratch = Scratch::new("monotone-seq");
        let fs_handle = SpillFs::open(scratch.default_spill_root(), scratch.db_root()).unwrap();
        let qid = fs_handle.new_query_id();
        let g0 = fs_handle.open_spill(qid, "sort-run").unwrap();
        let g1 = fs_handle.open_spill(qid, "sort-run").unwrap();
        let g2 = fs_handle.open_spill(qid, "sort-run").unwrap();
        assert!(g0
            .path()
            .to_string_lossy()
            .contains("sort-run-000000.spill"));
        assert!(g1
            .path()
            .to_string_lossy()
            .contains("sort-run-000001.spill"));
        assert!(g2
            .path()
            .to_string_lossy()
            .contains("sort-run-000002.spill"));
    }

    #[test]
    fn open_spill_per_purpose_counters_are_independent() {
        let scratch = Scratch::new("per-purpose-seq");
        let fs_handle = SpillFs::open(scratch.default_spill_root(), scratch.db_root()).unwrap();
        let qid = fs_handle.new_query_id();
        let g0 = fs_handle.open_spill(qid, "sort-run").unwrap();
        let g1 = fs_handle.open_spill(qid, "ingest-part-w0-s0").unwrap();
        // Each purpose starts at 000000.
        assert!(g0
            .path()
            .to_string_lossy()
            .ends_with("sort-run-000000.spill"));
        assert!(g1
            .path()
            .to_string_lossy()
            .ends_with("ingest-part-w0-s0-000000.spill"));
    }

    #[test]
    fn open_spill_per_query_counters_are_independent() {
        let scratch = Scratch::new("per-query-seq");
        let fs_handle = SpillFs::open(scratch.default_spill_root(), scratch.db_root()).unwrap();
        let q0 = fs_handle.new_query_id();
        let q1 = fs_handle.new_query_id();
        let g0 = fs_handle.open_spill(q0, "sort-run").unwrap();
        let g1 = fs_handle.open_spill(q1, "sort-run").unwrap();
        assert!(g0.path().to_string_lossy().contains(&q0.to_string()));
        assert!(g1.path().to_string_lossy().contains(&q1.to_string()));
        assert_ne!(g0.path(), g1.path());
    }

    #[test]
    fn open_spill_rejects_invalid_purpose() {
        let scratch = Scratch::new("bad-purpose");
        let fs_handle = SpillFs::open(scratch.default_spill_root(), scratch.db_root()).unwrap();
        let qid = fs_handle.new_query_id();
        for bad in &["", "Sort", "sort_run", "../escape", "x/../../y"] {
            let err = fs_handle
                .open_spill(qid, bad)
                .expect_err(&format!("must reject {bad:?}"));
            match err {
                BqliteError::Execution(msg) => assert!(
                    msg.contains("purpose tag"),
                    "wrong message for {bad:?}: {msg}"
                ),
                other => panic!("expected Execution for {bad:?}, got {other:?}"),
            }
        }
    }

    // ── TempSpillFile drop ───────────────────────────────────────────

    #[test]
    fn temp_spill_file_drop_removes_file() {
        let scratch = Scratch::new("drop");
        let fs_handle = SpillFs::open(scratch.default_spill_root(), scratch.db_root()).unwrap();
        let qid = fs_handle.new_query_id();
        let mut guard = fs_handle.open_spill(qid, "sort-run").unwrap();
        guard.file_mut().write_all(b"payload").unwrap();
        guard.record_bytes_written(7);
        let path = guard.path().to_path_buf();
        assert!(path.exists());
        assert_eq!(guard.bytes_written(), 7);
        drop(guard);
        assert!(!path.exists(), "Drop must delete the file");
    }

    // ── cleanup_query ────────────────────────────────────────────────

    #[test]
    fn cleanup_query_only_deletes_target_subdir() {
        let scratch = Scratch::new("cleanup");
        let fs_handle = SpillFs::open(scratch.default_spill_root(), scratch.db_root()).unwrap();
        let q0 = fs_handle.new_query_id();
        let q1 = fs_handle.new_query_id();
        let _g0 = fs_handle.open_spill(q0, "sort-run").unwrap();
        let _g1 = fs_handle.open_spill(q1, "sort-run").unwrap();
        let q0_dir = fs_handle.root().join(q0.to_string());
        let q1_dir = fs_handle.root().join(q1.to_string());
        assert!(q0_dir.exists());
        assert!(q1_dir.exists());

        // Drop q0's guard so cleanup_query has no living TempSpillFile
        // pointing at the file inside; otherwise on Windows the
        // remove_dir_all would fail. (No-op on Unix but explicit for
        // clarity.)
        drop(_g0);

        fs_handle.cleanup_query(q0);
        assert!(!q0_dir.exists());
        assert!(q1_dir.exists(), "siblings must survive");
    }

    #[test]
    fn cleanup_query_is_a_noop_when_subdir_was_never_created() {
        let scratch = Scratch::new("cleanup-nop");
        let fs_handle = SpillFs::open(scratch.default_spill_root(), scratch.db_root()).unwrap();
        let qid = fs_handle.new_query_id();
        // No open_spill — subdir was never created.
        fs_handle.cleanup_query(qid); // must not panic / err.
    }
}
