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


if __name__ == "__main__":
    unittest.main()
