#!/usr/bin/env bash
# Reset the container's bqlite clone to a clean checkout of origin/main.
#
# The Python wrapper's first action is task_tool.claim_next → sync_main →
# require_clean_worktree(), which raises (and crashes the wrapper) if the
# worktree is dirty. A previously crashed wrapper often leaves the clone
# on a task branch with uncommitted work, untracked files, or an aborted
# merge/rebase in progress — this script reconciles all of those cases
# before the next wrapper starts.
#
# It is a no-op on a genuinely clean tree (fetch + hard-reset to the
# current HEAD + clean on a worktree that already has nothing dirty just
# re-establishes the same state). Safe to run on every launch, but
# attach-fleet.sh only runs it in targeted/recovery mode right now.

set -euo pipefail

echo "--- resetting worktree to origin/main ---"

git fetch origin main

# Abort any merge/rebase/am left hanging by a crashed process.
git merge --abort 2>/dev/null || true
git rebase --abort 2>/dev/null || true
git am --abort 2>/dev/null || true

# Drop uncommitted changes on whatever branch we are currently on, then
# drop untracked files and directories so the tree is fully clean.
git reset --hard HEAD
git clean -fd

# Switch to main at origin/main (creates main if it does not exist, or
# resets it if it does). -B handles both cases idempotently.
git checkout -B main origin/main

# Belt-and-suspenders: make sure main matches origin/main exactly even if
# the checkout above raced with something.
git reset --hard origin/main

echo "--- worktree reset complete: $(git rev-parse --short HEAD) on main ---"
