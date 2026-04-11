//! Access-pattern hints for segment I/O (TASK-243).
//!
//! Wraps `posix_fadvise(2)` on platforms that expose it so the
//! segment reader can tell the kernel "this file is about to be
//! read sequentially from start to end" at open time. On platforms
//! that do not expose `posix_fadvise` (macOS, iOS, Windows) the
//! hint is a no-op and the kernel falls back on its own readahead
//! heuristics.
//!
//! The hint is advisory — the kernel may ignore it. We still issue
//! it because it is a single cheap syscall and the embeddable-
//! engine constraint (Core Belief 5) means we cannot tune
//! system-wide knobs like `vm.dirty_ratio`. Explicit access-pattern
//! hints are the only lever we have to steer kernel readahead.
//!
//! See `docs/design/storage-format.md` §8.2 for the steady-state
//! hint table that describes every `(scan kind → hint)` pair the
//! engine will eventually emit. Wave 2 ships only the sequential-
//! scan hint; the other hints (`Random`, `WillNeed`,
//! `memmap2::Advice`) are deferred to later waves per TASK-243.

use std::fs::File;

#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

/// Test-only monotonic counter incremented every time
/// [`advise_sequential`] is invoked.
///
/// Tests snapshot the value before opening a segment and assert
/// that the count rose on return. We expose a counter (rather than
/// stubbing the `libc` symbol itself) so the cross-platform
/// invariant — "every full-segment open asks the OS for sequential
/// readahead" — can be verified in a single test on every target,
/// including the no-op platforms where the real syscall is absent.
///
/// The counter is never reset: tests must read it by diff
/// (`after > before`) so concurrent test runs do not interfere.
#[cfg(test)]
pub(crate) static SEQUENTIAL_HINT_COUNT: AtomicUsize = AtomicUsize::new(0);

/// Hint that `file` will be read sequentially from start to end.
///
/// On Linux, FreeBSD, and Android this issues
/// `posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL)`; per POSIX
/// `offset = 0, len = 0` means "advise the entire file". On every
/// other platform this is a no-op — macOS and iOS expose the
/// equivalent as `fcntl(F_RDADVISE)` (a separate follow-up) and
/// Windows has no comparable syscall.
///
/// Errors from the syscall are deliberately ignored: a bad
/// `posix_fadvise` return cannot corrupt the file, the caller has
/// no meaningful recovery path, and every call site is on the
/// happy path of opening a segment for read. Propagating the
/// errno up would force every scan callsite to care about an
/// advisory syscall, which is the opposite of what we want.
pub(crate) fn advise_sequential(file: &File) {
    #[cfg(test)]
    SEQUENTIAL_HINT_COUNT.fetch_add(1, Ordering::Relaxed);

    issue_sequential_hint(file);
}

#[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "android"))]
fn issue_sequential_hint(file: &File) {
    use std::os::unix::io::AsRawFd;

    // SAFETY: `file` is a valid, owned, open file descriptor for
    // the duration of this call. `posix_fadvise` with
    // `offset = 0, len = 0` refers to the entire file per
    // POSIX.1-2001, and the advice flag is strictly informational —
    // it cannot modify file state. The return value (errno on
    // failure) is ignored because the call is advisory.
    let _ = unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_SEQUENTIAL) };
}

#[cfg(not(any(target_os = "linux", target_os = "freebsd", target_os = "android")))]
fn issue_sequential_hint(_file: &File) {
    // No-op on platforms without `posix_fadvise`. macOS/iOS would
    // want `fcntl(F_RDADVISE)`; that path is deferred per TASK-243.
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Shim invocation alone (no segment parsing) must bump the
    /// test counter. Guards against a refactor that silently
    /// drops the increment and leaves the reader-level test
    /// passing only because the reader itself happened to
    /// bump the counter through an unrelated code path.
    #[test]
    fn advise_sequential_bumps_test_counter() {
        let pid = std::process::id();
        let seq = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let mut path = std::env::temp_dir();
        path.push(format!("bqlite-advise-shim-{pid}-{seq}"));

        // Write a tiny file — the shim does not care about
        // content, only that the descriptor is open.
        {
            let mut f = File::create(&path).expect("create temp file");
            f.write_all(b"hello").expect("write");
        }

        let before = SEQUENTIAL_HINT_COUNT.load(Ordering::Relaxed);
        let file = File::open(&path).expect("open temp file");
        advise_sequential(&file);
        let after = SEQUENTIAL_HINT_COUNT.load(Ordering::Relaxed);

        assert!(
            after > before,
            "advise_sequential must bump the test counter (was {before} → {after})",
        );

        let _ = std::fs::remove_file(&path);
    }
}
