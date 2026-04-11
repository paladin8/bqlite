#!/usr/bin/env python3

from __future__ import annotations

import json
import os
import pathlib
import subprocess
import sys
import tempfile
import textwrap
import unittest


SCRIPT_PATH = pathlib.Path(__file__).resolve().parent / "task_tool.py"


def run_git(cwd: pathlib.Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    )


class TaskToolIntegrationTests(unittest.TestCase):
    def make_repo(self, tasks_md: str) -> pathlib.Path:
        temp_root = pathlib.Path(tempfile.mkdtemp(prefix="task-tool-it-"))
        self.addCleanup(lambda: subprocess.run(["rm", "-rf", str(temp_root)], check=False))

        origin = temp_root / "origin.git"
        seed = temp_root / "seed"
        work = temp_root / "work"

        run_git(temp_root, "init", "--bare", origin.name)
        run_git(temp_root, "init", "-b", "main", seed.name)
        run_git(seed, "config", "user.name", "Task Tool Test")
        run_git(seed, "config", "user.email", "task-tool@example.com")

        (seed / "TASKS.md").write_text(textwrap.dedent(tasks_md).lstrip())
        (seed / "tasks" / "active").mkdir(parents=True, exist_ok=True)
        (seed / "tasks" / "completed").mkdir(parents=True, exist_ok=True)
        (seed / "tasks" / "active" / ".gitkeep").write_text("")
        (seed / "tasks" / "completed" / ".gitkeep").write_text("")

        run_git(seed, "add", ".")
        run_git(seed, "commit", "-m", "initial state")
        run_git(seed, "remote", "add", "origin", str(origin))
        run_git(seed, "push", "-u", "origin", "main")
        run_git(origin, "symbolic-ref", "HEAD", "refs/heads/main")

        run_git(temp_root, "clone", str(origin), work.name)
        run_git(work, "config", "user.name", "Task Tool Test")
        run_git(work, "config", "user.email", "task-tool@example.com")
        return work

    def run_task_tool(
        self,
        repo: pathlib.Path,
        *args: str,
        extra_env: dict[str, str] | None = None,
        expected_returncode: int = 0,
    ) -> dict[str, object]:
        env = os.environ.copy()
        env["BQLITE_TASK_TOOL_ROOT"] = str(repo)
        if extra_env:
            env.update(extra_env)
        completed = subprocess.run(
            [sys.executable, str(SCRIPT_PATH), *args],
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.returncode != expected_returncode:
            self.fail(
                f"task_tool returned {completed.returncode}, expected {expected_returncode}\n"
                f"stdout:\n{completed.stdout}\n\nstderr:\n{completed.stderr}"
            )
        return json.loads(completed.stdout)

    def test_claim_next_claims_first_eligible_task(self) -> None:
        repo = self.make_repo(
            """
            ### TASK-101: [EASY][IMPL] First easy task
            **Depends on**: none
            """
        )

        result = self.run_task_tool(
            repo,
            "claim-next",
            "--wave",
            "1",
            "--difficulty",
            "EASY",
            "--agent-id",
            "agent-test",
        )

        self.assertEqual(result["status"], "claimed")
        self.assertEqual(result["task"]["task_id"], "TASK-101")

        lock_path = repo / "tasks" / "active" / "TASK-101.lock"
        self.assertTrue(lock_path.exists())
        lock_payload = json.loads(lock_path.read_text())
        self.assertEqual(lock_payload["agent_id"], "agent-test")
        self.assertEqual(lock_payload["task_id"], "TASK-101")

        branch = run_git(repo, "branch", "--show-current").stdout.strip()
        self.assertEqual(branch, "task/TASK-101")

        last_subject = run_git(repo, "log", "-1", "--format=%s", "main").stdout.strip()
        self.assertEqual(last_subject, "TASK-101: claimed by agent-test")

    def test_claim_next_reports_missing_difficulty(self) -> None:
        repo = self.make_repo(
            """
            ### TASK-101: [IMPL] Missing difficulty task
            **Depends on**: none
            """
        )

        result = self.run_task_tool(
            repo,
            "claim-next",
            "--wave",
            "1",
            "--difficulty",
            "EASY",
            "--agent-id",
            "agent-test",
        )

        self.assertEqual(result["status"], "missing_difficulty")
        self.assertEqual([task["task_id"] for task in result["tasks"]], ["TASK-101"])
        self.assertFalse((repo / "tasks" / "active" / "TASK-101.lock").exists())

    def test_claim_next_prefers_tagged_task_over_untagged(self) -> None:
        repo = self.make_repo(
            """
            ### TASK-101: [EASY][IMPL] Tagged easy task
            **Depends on**: none

            ### TASK-102: [IMPL] Untagged task
            **Depends on**: none
            """
        )

        result = self.run_task_tool(
            repo,
            "claim-next",
            "--wave",
            "1",
            "--difficulty",
            "EASY",
            "--agent-id",
            "agent-test",
        )

        self.assertEqual(result["status"], "claimed")
        self.assertEqual(result["task"]["task_id"], "TASK-101")
        self.assertEqual(
            [task["task_id"] for task in result["missing_difficulty_tasks"]],
            ["TASK-102"],
        )

    def test_claim_next_breaks_stale_lock_and_then_claims(self) -> None:
        repo = self.make_repo(
            """
            ### TASK-101: [EASY][IMPL] Stale-lock task
            **Depends on**: none
            """
        )

        lock_payload = {
            "agent_id": "agent-old",
            "task_id": "TASK-101",
            "claimed_at": "2000-01-01T00:00:00Z",
            "branch": "task/TASK-101",
            "description": "Stale-lock task",
        }
        lock_path = repo / "tasks" / "active" / "TASK-101.lock"
        lock_path.write_text(json.dumps(lock_payload, indent=2) + "\n")
        run_git(repo, "add", "tasks/active/TASK-101.lock")
        run_git(repo, "commit", "-m", "seed stale lock")
        run_git(repo, "push", "origin", "main")

        result = self.run_task_tool(
            repo,
            "claim-next",
            "--wave",
            "1",
            "--difficulty",
            "EASY",
            "--agent-id",
            "agent-test",
        )

        self.assertEqual(result["status"], "claimed")
        self.assertEqual(result["task"]["task_id"], "TASK-101")
        self.assertEqual(result["stale_locks_broken"], ["TASK-101"])

        log_subjects = run_git(repo, "log", "--format=%s", "-2", "main").stdout.splitlines()
        self.assertEqual(log_subjects[0], "TASK-101: claimed by agent-test")
        self.assertEqual(log_subjects[1], "TASK-101: break stale lock")

    def test_cleanup_after_failed_push_resyncs_to_origin(self) -> None:
        """cleanup_after_failed_push must recover even when local and origin
        have diverged (the scenario that used to crash claim_next with
        `git pull --ff-only` failing). After cleanup, the worktree should
        match origin/main exactly with no local-only commits or files."""
        repo = self.make_repo(
            """
            ### TASK-101: [EASY][IMPL] Dummy
            **Depends on**: none
            """
        )

        origin_path = repo.parent / "origin.git"

        # Second clone races against the first, pushing its own commit.
        other = repo.parent / "other"
        run_git(repo.parent, "clone", str(origin_path), other.name)
        run_git(other, "config", "user.name", "Other")
        run_git(other, "config", "user.email", "other@example.com")
        (other / "other_file.txt").write_text("from other agent\n")
        run_git(other, "add", "other_file.txt")
        run_git(other, "commit", "-m", "other agent's commit")
        run_git(other, "push", "origin", "main")

        # Main clone makes a different commit locally and does NOT push.
        # This mirrors the state of an agent mid-claim when its push loses
        # a race: local HEAD has the claim commit, origin/main has moved
        # ahead with someone else's commit, and `pull --ff-only` would fail
        # because the branches have diverged.
        (repo / "my_claim.txt").write_text("my in-flight claim\n")
        run_git(repo, "add", "my_claim.txt")
        run_git(repo, "commit", "-m", "my aborted claim commit")

        env = os.environ.copy()
        env["BQLITE_TASK_TOOL_ROOT"] = str(repo)
        script = (
            f"import sys; sys.path.insert(0, {str(SCRIPT_PATH.parent)!r}); "
            "import task_tool; "
            "task_tool.cleanup_after_failed_push()"
        )
        result = subprocess.run(
            [sys.executable, "-c", script],
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            self.fail(
                f"cleanup_after_failed_push failed ({result.returncode}):\n"
                f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}"
            )

        # After cleanup, HEAD must match origin/main exactly: the other
        # agent's commit is present, our own aborted commit is gone, and
        # our in-flight file is gone too.
        run_git(repo, "fetch", "origin")
        local_head = run_git(repo, "rev-parse", "HEAD").stdout.strip()
        origin_head = run_git(repo, "rev-parse", "origin/main").stdout.strip()
        self.assertEqual(local_head, origin_head)
        self.assertTrue((repo / "other_file.txt").exists())
        self.assertFalse((repo / "my_claim.txt").exists())

    def test_claim_next_reclaims_prior_task_branch_state(self) -> None:
        """Reclaiming a stale-locked task must check out the existing
        task/TASK-NNN branch from origin, not create a fresh branch off main.
        Otherwise prior work from a crashed agent is silently lost."""
        repo = self.make_repo(
            """
            ### TASK-101: [EASY][IMPL] Task with prior work
            **Depends on**: none
            """
        )

        stale_lock_payload = {
            "agent_id": "agent-old",
            "task_id": "TASK-101",
            "claimed_at": "2000-01-01T00:00:00Z",
            "branch": "task/TASK-101",
            "description": "Task with prior work",
        }

        # Simulate a crashed prior agent: push a task branch that has real
        # work on it.
        run_git(repo, "checkout", "-b", "task/TASK-101")
        (repo / "prior_work.txt").write_text("prior agent's work\n")
        run_git(repo, "add", "prior_work.txt")
        run_git(repo, "commit", "-m", "TASK-101: prior work")
        run_git(repo, "push", "-u", "origin", "task/TASK-101")

        # Push the stale lock to origin/main so claim_next has a lock to break.
        run_git(repo, "checkout", "main")
        (repo / "tasks" / "active" / "TASK-101.lock").write_text(
            json.dumps(stale_lock_payload, indent=2) + "\n"
        )
        run_git(repo, "add", "tasks/active/TASK-101.lock")
        run_git(repo, "commit", "-m", "seed stale lock on main")
        run_git(repo, "push", "origin", "main")

        # Simulate a fresh container: delete the local task branch so only the
        # remote-tracking ref remains. This is what ensure_task_branch sees in
        # real reclaim scenarios.
        run_git(repo, "branch", "-D", "task/TASK-101")

        # Force the stale-lock check to fire even though the task branch was
        # just pushed in this test — the default 45-minute timeout would not
        # trigger without real time passing.
        result = self.run_task_tool(
            repo,
            "claim-next",
            "--wave",
            "1",
            "--difficulty",
            "EASY",
            "--agent-id",
            "agent-new",
            extra_env={"STALE_LOCK_TIMEOUT_MINUTES": "0"},
        )

        self.assertEqual(result["status"], "claimed")
        self.assertEqual(result["task"]["task_id"], "TASK-101")
        self.assertIn("TASK-101", result["stale_locks_broken"])

        branch = run_git(repo, "branch", "--show-current").stdout.strip()
        self.assertEqual(branch, "task/TASK-101")

        # The whole point: prior work is visible after the reclaim.
        self.assertTrue((repo / "prior_work.txt").exists())
        self.assertEqual(
            (repo / "prior_work.txt").read_text(), "prior agent's work\n"
        )

    def test_task_state_on_origin_returns_completed_lock_held_or_missing(self) -> None:
        """task_state_on_origin must reflect origin/main's state, not the
        local working tree. Used by agent_wrapper to avoid declaring success
        when claude wrote files locally but failed to push."""
        repo = self.make_repo(
            """
            ### TASK-101: [EASY][IMPL] Done task
            **Depends on**: none

            ### TASK-102: [EASY][IMPL] Locked task
            **Depends on**: none

            ### TASK-103: [EASY][IMPL] Unknown task
            **Depends on**: none
            """
        )

        # Push a done marker for TASK-101 and a lock for TASK-102 to origin/main.
        done_payload = {"task_id": "TASK-101", "completed_at": "2026-04-11T00:00:00Z"}
        lock_payload = {
            "agent_id": "agent-other",
            "task_id": "TASK-102",
            "claimed_at": "2026-04-11T00:00:00Z",
            "branch": "task/TASK-102",
            "description": "Locked task",
        }
        (repo / "tasks" / "completed" / "TASK-101.done").write_text(
            json.dumps(done_payload, indent=2) + "\n"
        )
        (repo / "tasks" / "active" / "TASK-102.lock").write_text(
            json.dumps(lock_payload, indent=2) + "\n"
        )
        run_git(
            repo,
            "add",
            "tasks/completed/TASK-101.done",
            "tasks/active/TASK-102.lock",
        )
        run_git(repo, "commit", "-m", "seed done + lock for state check")
        run_git(repo, "push", "origin", "main")

        # Simulate a local-only done marker for TASK-103: write to the working
        # tree but do NOT commit/push. The origin-aware check must still treat
        # TASK-103 as "missing", not "completed".
        (repo / "tasks" / "completed" / "TASK-103.done").write_text(
            json.dumps({"task_id": "TASK-103"}, indent=2) + "\n"
        )

        env = os.environ.copy()
        env["BQLITE_TASK_TOOL_ROOT"] = str(repo)
        script = (
            f"import sys; sys.path.insert(0, {str(SCRIPT_PATH.parent)!r}); "
            "import task_tool; "
            "print('TASK-101=' + task_tool.task_state_on_origin('TASK-101')); "
            "print('TASK-102=' + task_tool.task_state_on_origin('TASK-102')); "
            "print('TASK-103=' + task_tool.task_state_on_origin('TASK-103'))"
        )
        result = subprocess.run(
            [sys.executable, "-c", script],
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            self.fail(
                f"task_state_on_origin failed ({result.returncode}):\n"
                f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}"
            )
        self.assertIn("TASK-101=completed", result.stdout)
        self.assertIn("TASK-102=lock_held", result.stdout)
        self.assertIn("TASK-103=missing", result.stdout)  # local-only file ignored

    def test_release_lock_removes_lock_and_pushes(self) -> None:
        repo = self.make_repo(
            """
            ### TASK-101: [EASY][IMPL] Releasable task
            **Depends on**: none
            """
        )
        claim = self.run_task_tool(
            repo,
            "claim-next",
            "--wave",
            "1",
            "--difficulty",
            "EASY",
            "--agent-id",
            "agent-1",
        )
        self.assertEqual(claim["status"], "claimed")
        lock_path = repo / "tasks" / "active" / "TASK-101.lock"
        self.assertTrue(lock_path.exists())

        env = os.environ.copy()
        env["BQLITE_TASK_TOOL_ROOT"] = str(repo)
        script = (
            f"import sys; sys.path.insert(0, {str(SCRIPT_PATH.parent)!r}); "
            "import task_tool; "
            "ok = task_tool.release_lock('TASK-101', 'agent-1', 'test run'); "
            "print('release_ok=' + str(ok))"
        )
        result = subprocess.run(
            [sys.executable, "-c", script],
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            self.fail(
                f"release_lock failed ({result.returncode}):\n"
                f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}"
            )
        self.assertIn("release_ok=True", result.stdout)

        run_git(repo, "checkout", "main")
        run_git(repo, "pull", "--ff-only", "origin", "main")
        self.assertFalse(lock_path.exists())
        log = run_git(repo, "log", "--format=%s", "-3", "main").stdout
        self.assertIn("TASK-101: released by agent-1 (test run)", log)


if __name__ == "__main__":
    unittest.main()
