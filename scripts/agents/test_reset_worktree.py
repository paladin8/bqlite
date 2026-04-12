#!/usr/bin/env python3

from __future__ import annotations

import pathlib
import subprocess
import tempfile
import textwrap
import unittest


SCRIPTS_DIR = pathlib.Path(__file__).resolve().parent
RESET_SCRIPT = SCRIPTS_DIR / "reset-worktree.sh"


def run_git(cwd: pathlib.Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True
    )


class ResetWorktreeTests(unittest.TestCase):
    def make_repo(self) -> pathlib.Path:
        """Bare origin + work clone with a single seed commit on main."""
        temp_root = pathlib.Path(tempfile.mkdtemp(prefix="reset-worktree-"))
        self.addCleanup(
            lambda: subprocess.run(["rm", "-rf", str(temp_root)], check=False)
        )

        origin = temp_root / "origin.git"
        seed = temp_root / "seed"
        work = temp_root / "work"

        run_git(temp_root, "init", "--bare", origin.name)
        run_git(temp_root, "init", "-b", "main", seed.name)
        run_git(seed, "config", "user.name", "Reset Test")
        run_git(seed, "config", "user.email", "reset@example.com")
        (seed / "seed.txt").write_text("seed content\n")
        run_git(seed, "add", "seed.txt")
        run_git(seed, "commit", "-m", "initial")
        run_git(seed, "remote", "add", "origin", str(origin))
        run_git(seed, "push", "-u", "origin", "main")
        run_git(origin, "symbolic-ref", "HEAD", "refs/heads/main")

        run_git(temp_root, "clone", str(origin), work.name)
        run_git(work, "config", "user.name", "Reset Test")
        run_git(work, "config", "user.email", "reset@example.com")
        return work

    def run_reset(self, repo: pathlib.Path) -> None:
        result = subprocess.run(
            ["bash", str(RESET_SCRIPT)],
            cwd=repo,
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            self.fail(
                f"reset-worktree.sh failed ({result.returncode}):\n"
                f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}"
            )

    def test_noop_on_clean_tree(self) -> None:
        repo = self.make_repo()
        origin_head = run_git(
            repo, "rev-parse", "origin/main"
        ).stdout.strip()
        self.run_reset(repo)
        local_head = run_git(repo, "rev-parse", "HEAD").stdout.strip()
        self.assertEqual(local_head, origin_head)
        branch = run_git(repo, "branch", "--show-current").stdout.strip()
        self.assertEqual(branch, "main")

    def test_wipes_uncommitted_tracked_changes(self) -> None:
        repo = self.make_repo()
        (repo / "seed.txt").write_text("corrupted by crashed agent\n")
        diff = run_git(repo, "diff", "--name-only").stdout.strip()
        self.assertEqual(diff, "seed.txt")

        self.run_reset(repo)
        self.assertEqual((repo / "seed.txt").read_text(), "seed content\n")
        self.assertEqual(run_git(repo, "diff", "--name-only").stdout.strip(), "")

    def test_removes_untracked_files_and_directories(self) -> None:
        repo = self.make_repo()
        (repo / "leftover.txt").write_text("junk\n")
        (repo / "trash").mkdir()
        (repo / "trash" / "nested.txt").write_text("more junk\n")

        self.run_reset(repo)
        self.assertFalse((repo / "leftover.txt").exists())
        self.assertFalse((repo / "trash").exists())

    def test_switches_from_task_branch_back_to_main(self) -> None:
        repo = self.make_repo()
        run_git(repo, "checkout", "-b", "task/TASK-999")
        (repo / "in_progress.txt").write_text("half-done work\n")
        run_git(repo, "add", "in_progress.txt")
        run_git(repo, "commit", "-m", "WIP on TASK-999")
        (repo / "scratch.txt").write_text("uncommitted thoughts\n")

        self.run_reset(repo)
        branch = run_git(repo, "branch", "--show-current").stdout.strip()
        self.assertEqual(branch, "main")
        local_head = run_git(repo, "rev-parse", "HEAD").stdout.strip()
        origin_head = run_git(repo, "rev-parse", "origin/main").stdout.strip()
        self.assertEqual(local_head, origin_head)
        self.assertFalse((repo / "in_progress.txt").exists())
        self.assertFalse((repo / "scratch.txt").exists())
        # The task branch itself is preserved so committed work is not lost.
        branches = run_git(repo, "branch", "--list").stdout
        self.assertIn("task/TASK-999", branches)

    def test_handles_aborted_merge(self) -> None:
        repo = self.make_repo()
        # Create a conflicting branch and try to merge it to leave the
        # working tree in a MERGING state.
        run_git(repo, "checkout", "-b", "conflict")
        (repo / "seed.txt").write_text("conflicting branch content\n")
        run_git(repo, "add", "seed.txt")
        run_git(repo, "commit", "-m", "conflict commit")
        run_git(repo, "checkout", "main")
        (repo / "seed.txt").write_text("main branch content\n")
        run_git(repo, "add", "seed.txt")
        run_git(repo, "commit", "-m", "main edit")
        merge = subprocess.run(
            ["git", "merge", "conflict"],
            cwd=repo,
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(merge.returncode, 0, "expected merge conflict")
        self.assertTrue((repo / ".git" / "MERGE_HEAD").exists())

        self.run_reset(repo)
        self.assertFalse((repo / ".git" / "MERGE_HEAD").exists())
        branch = run_git(repo, "branch", "--show-current").stdout.strip()
        self.assertEqual(branch, "main")
        # HEAD should now match origin/main — local "main edit" commit is
        # discarded along with the uncommitted merge state.
        local_head = run_git(repo, "rev-parse", "HEAD").stdout.strip()
        origin_head = run_git(repo, "rev-parse", "origin/main").stdout.strip()
        self.assertEqual(local_head, origin_head)


if __name__ == "__main__":
    unittest.main()
