#!/usr/bin/env python3

from __future__ import annotations

import os
import pathlib
import subprocess
import sys
import tempfile
import textwrap
import unittest


SCRIPTS_DIR = pathlib.Path(__file__).resolve().parent
WRAPPER_PATH = SCRIPTS_DIR / "agent_wrapper.py"


def run_git(cwd: pathlib.Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True
    )


class AgentWrapperIntegrationTests(unittest.TestCase):
    def make_repo(self, tasks_md: str) -> pathlib.Path:
        temp_root = pathlib.Path(tempfile.mkdtemp(prefix="agent-wrapper-it-"))
        self.addCleanup(
            lambda: subprocess.run(["rm", "-rf", str(temp_root)], check=False)
        )

        origin = temp_root / "origin.git"
        seed = temp_root / "seed"
        work = temp_root / "work"

        run_git(temp_root, "init", "--bare", origin.name)
        run_git(temp_root, "init", "-b", "main", seed.name)
        run_git(seed, "config", "user.name", "Wrapper Test")
        run_git(seed, "config", "user.email", "wrapper@example.com")

        (seed / "TASKS.md").write_text(textwrap.dedent(tasks_md).lstrip())
        (seed / "tasks" / "active").mkdir(parents=True, exist_ok=True)
        (seed / "tasks" / "completed").mkdir(parents=True, exist_ok=True)
        (seed / "tasks" / "active" / ".gitkeep").write_text("")
        (seed / "tasks" / "completed" / ".gitkeep").write_text("")

        run_git(seed, "add", ".")
        run_git(seed, "commit", "-m", "seed")
        run_git(seed, "remote", "add", "origin", str(origin))
        run_git(seed, "push", "-u", "origin", "main")
        run_git(origin, "symbolic-ref", "HEAD", "refs/heads/main")

        run_git(temp_root, "clone", str(origin), work.name)
        run_git(work, "config", "user.name", "Wrapper Test")
        run_git(work, "config", "user.email", "wrapper@example.com")
        return work

    def install_fake_claude(self, repo: pathlib.Path) -> pathlib.Path:
        """Install a fake `claude` executable that finds a TASK-NNN ID in its
        prompt arg, merges any pending work from the task branch to main, and
        moves the lock file to completed/.
        """
        bin_dir = repo.parent / "bin"
        bin_dir.mkdir(exist_ok=True)
        fake = bin_dir / "claude"
        fake.write_text(textwrap.dedent('''\
            #!/usr/bin/env bash
            set -euo pipefail
            prompt="${!#}"
            echo "fake-claude received prompt"
            task_id=$(echo "$prompt" | grep -oE 'TASK-[0-9]{3}' | head -1 || true)
            if [ -z "$task_id" ]; then
              echo "fake-claude: no task id in prompt"
              exit 0
            fi
            repo_root=$(git rev-parse --show-toplevel)
            cd "$repo_root"
            git checkout main >/dev/null 2>&1
            git pull --ff-only origin main >/dev/null 2>&1
            lock="tasks/active/${task_id}.lock"
            done_file="tasks/completed/${task_id}.done"
            if [ -f "$lock" ]; then
              git mv "$lock" "$done_file"
              git commit -m "${task_id}: completed" >/dev/null
              git push origin main >/dev/null
              echo "fake-claude: marked $task_id done"
            else
              echo "fake-claude: lock for $task_id not present"
            fi
        '''))
        fake.chmod(0o755)
        return bin_dir

    def test_single_task_quota_runs_to_completion(self) -> None:
        repo = self.make_repo(
            """
            ### TASK-101: [EASY][IMPL] Single simple task
            **Depends on**: none
            """
        )
        bin_dir = self.install_fake_claude(repo)

        env = os.environ.copy()
        env["PATH"] = f"{bin_dir}:{env['PATH']}"
        env["BQLITE_TASK_TOOL_ROOT"] = str(repo)
        env["AGENT_ID"] = "agent-test-1"
        # Override the absolute /root/.local/bin/claude default so the fake
        # binary on PATH above is the one the wrapper actually spawns.
        env["BQLITE_CLAUDE_BIN"] = "claude"

        result = subprocess.run(
            [sys.executable, str(WRAPPER_PATH), "1", "1"],
            cwd=repo,
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
        )

        if result.returncode != 0:
            self.fail(
                f"wrapper exited {result.returncode}\n"
                f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}"
            )

        run_git(repo, "checkout", "main")
        run_git(repo, "pull", "--ff-only", "origin", "main")
        done = repo / "tasks" / "completed" / "TASK-101.done"
        self.assertTrue(
            done.exists(),
            f"expected TASK-101.done after wrapper run.\n"
            f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}",
        )
        self.assertIn("batch complete", result.stdout)


if __name__ == "__main__":
    unittest.main()
