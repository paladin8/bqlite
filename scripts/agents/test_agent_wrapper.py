#!/usr/bin/env python3

import pathlib
import sys
import unittest


SCRIPTS_DIR = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS_DIR))

import agent_wrapper


class AgentWrapperArgsTests(unittest.TestCase):
    def test_parse_args_accepts_wave_and_pool(self) -> None:
        config = agent_wrapper.parse_args(["3", "EASY"])
        self.assertEqual(config.wave, 3)
        self.assertEqual(config.difficulty_pool, "EASY")
        self.assertEqual(config.model, "claude-sonnet-4-6")
        self.assertEqual(config.effort, "high")
        self.assertIsNone(config.max_tasks)

    def test_parse_args_accepts_max_tasks(self) -> None:
        config = agent_wrapper.parse_args(["3", "HARD", "2"])
        self.assertEqual(config.max_tasks, 2)
        self.assertEqual(config.model, "claude-opus-4-6[1m]")

    def test_parse_args_lowercases_pool_input(self) -> None:
        config = agent_wrapper.parse_args(["1", "easy"])
        self.assertEqual(config.difficulty_pool, "EASY")

    def test_parse_args_rejects_negative_wave(self) -> None:
        with self.assertRaises(agent_wrapper.WrapperConfigError):
            agent_wrapper.parse_args(["-1", "EASY"])

    def test_parse_args_rejects_bad_pool(self) -> None:
        with self.assertRaises(agent_wrapper.WrapperConfigError):
            agent_wrapper.parse_args(["1", "SPICY"])

    def test_parse_args_rejects_zero_max_tasks(self) -> None:
        with self.assertRaises(agent_wrapper.WrapperConfigError):
            agent_wrapper.parse_args(["1", "EASY", "0"])

    def test_wave_range_label_wave_zero(self) -> None:
        self.assertEqual(
            agent_wrapper.wave_range_label(0),
            "TASK-001 through TASK-099",
        )

    def test_wave_range_label_higher_wave(self) -> None:
        self.assertEqual(
            agent_wrapper.wave_range_label(3),
            "TASK-300 through TASK-399",
        )


import os
import subprocess
import tempfile


class AgentWrapperClaudeInvocationTests(unittest.TestCase):
    def _make_fake_claude(self, script_body: str) -> tuple[pathlib.Path, dict]:
        tmpdir = pathlib.Path(tempfile.mkdtemp(prefix="fake-claude-"))
        self.addCleanup(
            lambda: subprocess.run(["rm", "-rf", str(tmpdir)], check=False)
        )
        fake = tmpdir / "claude"
        fake.write_text("#!/usr/bin/env bash\n" + script_body)
        fake.chmod(0o755)
        env = os.environ.copy()
        env["PATH"] = f"{tmpdir}:{env.get('PATH', '')}"
        return fake, env

    def test_run_claude_captures_raw_output(self) -> None:
        """Non-JSON chatter on the pty ends up in raw_output (for debugging),
        but output (assistant text extracted from stream-json) stays empty."""
        _, env = self._make_fake_claude('echo "hello from claude"\nexit 0\n')
        result = agent_wrapper.run_claude(
            prompt="ignored",
            model="fake-model",
            effort="high",
            system_prompt_text="fake sys",
            resume=False,
            env=env,
        )
        self.assertEqual(result.returncode, 0)
        self.assertIn("hello from claude", result.raw_output)
        self.assertEqual(result.output, "")

    def test_run_claude_sets_resume_flag(self) -> None:
        _, env = self._make_fake_claude('printf "ARGV:%s\\n" "$@"\nexit 0\n')
        result = agent_wrapper.run_claude(
            prompt="do the thing",
            model="fake-model",
            effort="high",
            system_prompt_text=None,
            resume=True,
            env=env,
        )
        self.assertEqual(result.returncode, 0)
        self.assertIn("ARGV:-c", result.raw_output)
        self.assertIn("ARGV:do the thing", result.raw_output)
        self.assertNotIn("--append-system-prompt", result.raw_output)

    def test_run_claude_passes_system_prompt_on_fresh(self) -> None:
        _, env = self._make_fake_claude('printf "ARGV:%s\\n" "$@"\nexit 0\n')
        result = agent_wrapper.run_claude(
            prompt="fresh work",
            model="fake-model",
            effort="high",
            system_prompt_text="you are fake agent",
            resume=False,
            env=env,
        )
        self.assertIn("ARGV:--append-system-prompt", result.raw_output)
        self.assertIn("ARGV:you are fake agent", result.raw_output)

    def test_run_claude_propagates_nonzero_exit(self) -> None:
        _, env = self._make_fake_claude('echo "boom" >&2\nexit 7\n')
        result = agent_wrapper.run_claude(
            prompt="ignored",
            model="fake-model",
            effort="high",
            system_prompt_text="fake sys",
            resume=False,
            env=env,
        )
        self.assertEqual(result.returncode, 7)

    def test_run_claude_requests_stream_json_output_format(self) -> None:
        """run_claude must pass --output-format stream-json --include-partial-
        messages so the wrapper sees progress events as they happen (plain
        -p --verbose text mode only emits the final answer)."""
        _, env = self._make_fake_claude('printf "ARGV:%s\\n" "$@"\nexit 0\n')
        result = agent_wrapper.run_claude(
            prompt="dummy",
            model="fake-model",
            effort="high",
            system_prompt_text="sys",
            resume=False,
            env=env,
        )
        self.assertIn("ARGV:--output-format", result.raw_output)
        self.assertIn("ARGV:stream-json", result.raw_output)
        self.assertIn("ARGV:--include-partial-messages", result.raw_output)

    def test_run_claude_extracts_assistant_text_from_stream_json(self) -> None:
        """The stream-json parser must extract text_delta events into
        ClaudeRunResult.output so [NEEDS INPUT] detection and success
        reporting see the actual assistant reply."""
        # Fake emits three stream-json events on separate lines: two text
        # deltas that together spell "hello world", and a final result event.
        script = (
            r'''printf '%s\n' '{"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"text_delta","text":"hello "}}}' '''
            r'''&& printf '%s\n' '{"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"text_delta","text":"world"}}}' '''
            r'''&& printf '%s\n' '{"type":"result","subtype":"success","num_turns":1,"duration_ms":42}' '''
            "&& exit 0\n"
        )
        _, env = self._make_fake_claude(script)
        result = agent_wrapper.run_claude(
            prompt="dummy",
            model="fake-model",
            effort="high",
            system_prompt_text="sys",
            resume=False,
            env=env,
        )
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.output, "hello world")

    def test_run_claude_scans_needs_input_only_in_assistant_text(self) -> None:
        """has_needs_input must ignore [NEEDS INPUT] appearing in thinking
        or tool-call events and only fire on assistant text."""
        script = (
            # thinking_delta contains the marker — must NOT count
            r'''printf '%s\n' '{"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"thinking_delta","thinking":"hmm [NEEDS INPUT] maybe"}}}' '''
            # assistant text does not — must not fire
            r'''&& printf '%s\n' '{"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"text_delta","text":"here is my answer"}}}' '''
            "&& exit 0\n"
        )
        _, env = self._make_fake_claude(script)
        result = agent_wrapper.run_claude(
            prompt="dummy",
            model="fake-model",
            effort="high",
            system_prompt_text="sys",
            resume=False,
            env=env,
        )
        self.assertEqual(result.output, "here is my answer")
        self.assertFalse(agent_wrapper.has_needs_input(result.output))
        # Same input but assistant text *does* carry the marker → must fire.
        script_with_marker = (
            r'''printf '%s\n' '{"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"text_delta","text":"I am blocked. [NEEDS INPUT] which path?"}}}' '''
            "&& exit 0\n"
        )
        _, env2 = self._make_fake_claude(script_with_marker)
        result2 = agent_wrapper.run_claude(
            prompt="dummy",
            model="fake-model",
            effort="high",
            system_prompt_text="sys",
            resume=False,
            env=env2,
        )
        self.assertTrue(agent_wrapper.has_needs_input(result2.output))


class FakeClaude:
    def __init__(self, scripted_runs: list[dict]) -> None:
        self.scripted = list(scripted_runs)
        self.calls: list[dict] = []

    def __call__(
        self,
        *,
        prompt: str,
        model: str,
        effort: str,
        system_prompt_text,
        resume: bool,
        env=None,
    ):
        self.calls.append(
            {
                "prompt": prompt,
                "resume": resume,
                "system_prompt_text": system_prompt_text,
            }
        )
        if not self.scripted:
            raise AssertionError("FakeClaude ran out of scripted runs")
        scripted = self.scripted.pop(0)
        side_effect = scripted.get("side_effect")
        if side_effect is not None:
            side_effect()
        return agent_wrapper.ClaudeRunResult(
            returncode=scripted.get("returncode", 0),
            output=scripted.get("output", ""),
        )


class AgentWrapperExecuteTaskTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = pathlib.Path(tempfile.mkdtemp(prefix="agent-wrapper-ut-"))
        self.addCleanup(
            lambda: subprocess.run(["rm", "-rf", str(self.tmp)], check=False)
        )
        (self.tmp / "tasks" / "active").mkdir(parents=True)
        (self.tmp / "tasks" / "completed").mkdir(parents=True)
        self._orig_active = agent_wrapper.task_tool.ACTIVE_DIR
        self._orig_completed = agent_wrapper.task_tool.COMPLETED_DIR
        agent_wrapper.task_tool.ACTIVE_DIR = self.tmp / "tasks" / "active"
        agent_wrapper.task_tool.COMPLETED_DIR = self.tmp / "tasks" / "completed"
        self.addCleanup(self._restore_dirs)

        self.task = {
            "task_id": "TASK-101",
            "title": "Ship thing",
            "wave": 1,
            "tags": ["EASY"],
            "depends_on": [],
            "difficulty": "EASY",
        }
        (self.tmp / "tasks" / "active" / "TASK-101.lock").write_text("{}")

    def _restore_dirs(self) -> None:
        agent_wrapper.task_tool.ACTIVE_DIR = self._orig_active
        agent_wrapper.task_tool.COMPLETED_DIR = self._orig_completed

    def _mark_done(self) -> None:
        (self.tmp / "tasks" / "active" / "TASK-101.lock").unlink()
        (self.tmp / "tasks" / "completed" / "TASK-101.done").write_text("{}")

    def test_execute_task_returns_completed_when_done_marker_appears(self) -> None:
        fake = FakeClaude([
            {"side_effect": self._mark_done, "output": "all done\n"},
        ])
        outcome = agent_wrapper.execute_task(
            self.task,
            agent_name="agent-1",
            model="m",
            effort="high",
            claude_runner=fake,
            read_human_reply=lambda: "unused",
            task_state_fn=agent_wrapper._filesystem_task_state,
        )
        self.assertEqual(outcome, "completed")
        self.assertEqual(len(fake.calls), 1)
        self.assertFalse(fake.calls[0]["resume"])

    def test_execute_task_retries_once_on_incomplete(self) -> None:
        fake = FakeClaude([
            {"output": "stopped early\n"},
            {"side_effect": self._mark_done, "output": "retry wrap\n"},
        ])
        outcome = agent_wrapper.execute_task(
            self.task,
            agent_name="agent-1",
            model="m",
            effort="high",
            claude_runner=fake,
            read_human_reply=lambda: "unused",
            task_state_fn=agent_wrapper._filesystem_task_state,
        )
        self.assertEqual(outcome, "completed")
        self.assertEqual(len(fake.calls), 2)
        self.assertTrue(fake.calls[1]["resume"])

    def test_execute_task_returns_incomplete_after_retry_also_fails(self) -> None:
        fake = FakeClaude([
            {"output": "first try stopped\n"},
            {"output": "second try also stopped\n"},
        ])
        outcome = agent_wrapper.execute_task(
            self.task,
            agent_name="agent-1",
            model="m",
            effort="high",
            claude_runner=fake,
            read_human_reply=lambda: "unused",
            task_state_fn=agent_wrapper._filesystem_task_state,
        )
        self.assertEqual(outcome, "incomplete")
        self.assertEqual(len(fake.calls), 2)

    def test_execute_task_handles_needs_input_with_resume(self) -> None:
        fake = FakeClaude([
            {"output": "[NEEDS INPUT] which variant?\n"},
            {"side_effect": self._mark_done, "output": "got reply, finishing\n"},
        ])
        replies = iter(["use variant A"])
        outcome = agent_wrapper.execute_task(
            self.task,
            agent_name="agent-1",
            model="m",
            effort="high",
            claude_runner=fake,
            read_human_reply=lambda: next(replies),
            task_state_fn=agent_wrapper._filesystem_task_state,
        )
        self.assertEqual(outcome, "completed")
        self.assertEqual(len(fake.calls), 2)
        self.assertTrue(fake.calls[1]["resume"])
        self.assertEqual(fake.calls[1]["prompt"], "use variant A")


class AgentWrapperMainLoopTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = pathlib.Path(tempfile.mkdtemp(prefix="agent-wrapper-loop-"))
        self.addCleanup(
            lambda: subprocess.run(["rm", "-rf", str(self.tmp)], check=False)
        )
        (self.tmp / "tasks" / "active").mkdir(parents=True)
        (self.tmp / "tasks" / "completed").mkdir(parents=True)
        self._orig_active = agent_wrapper.task_tool.ACTIVE_DIR
        self._orig_completed = agent_wrapper.task_tool.COMPLETED_DIR
        agent_wrapper.task_tool.ACTIVE_DIR = self.tmp / "tasks" / "active"
        agent_wrapper.task_tool.COMPLETED_DIR = self.tmp / "tasks" / "completed"
        self.addCleanup(self._restore_dirs)

    def _restore_dirs(self) -> None:
        agent_wrapper.task_tool.ACTIVE_DIR = self._orig_active
        agent_wrapper.task_tool.COMPLETED_DIR = self._orig_completed

    def _make_task(self, task_id: str, difficulty: str = "EASY") -> dict:
        return {
            "task_id": task_id,
            "title": f"Do {task_id}",
            "wave": 1,
            "tags": [difficulty],
            "depends_on": [],
            "difficulty": difficulty,
        }

    def test_run_fleet_loop_exits_after_max_tasks(self) -> None:
        config = agent_wrapper.parse_args(["1", "EASY", "2"])

        test_self = self

        def fake_claim_next(**kwargs) -> dict:
            task_id = f"TASK-10{fake_claim_next.counter}"
            fake_claim_next.counter += 1
            (test_self.tmp / "tasks" / "active" / f"{task_id}.lock").write_text("{}")
            return {"status": "claimed", "task": test_self._make_task(task_id)}
        fake_claim_next.counter = 1

        def fake_execute_task(task, **kwargs) -> str:
            tid = task["task_id"]
            (test_self.tmp / "tasks" / "active" / f"{tid}.lock").unlink()
            (test_self.tmp / "tasks" / "completed" / f"{tid}.done").write_text("{}")
            return "completed"

        outcome = agent_wrapper.run_fleet_loop(
            config,
            agent_name="agent-1",
            claim_next=fake_claim_next,
            execute_task=fake_execute_task,
            release_lock=lambda *a, **k: True,
            sleep=lambda _: None,
        )
        self.assertEqual(outcome, "batch_complete")
        self.assertEqual(fake_claim_next.counter, 3)  # 2 calls made

    def test_run_fleet_loop_exits_on_wave_drained(self) -> None:
        config = agent_wrapper.parse_args(["1", "EASY"])
        states = iter([
            {"status": "no_claimable", "claimable": [], "blocked": [],
             "active": [], "completed_count": 5,
             "other_pool_claimable": [], "wave": 1, "difficulty": "EASY"},
        ])
        outcome = agent_wrapper.run_fleet_loop(
            config,
            agent_name="agent-1",
            claim_next=lambda **k: next(states),
            execute_task=lambda *a, **k: "completed",
            release_lock=lambda *a, **k: True,
            sleep=lambda _: None,
        )
        self.assertEqual(outcome, "wave_complete")

    def test_incomplete_releases_lock_and_continues_to_next_task(self) -> None:
        """An incomplete task must not count toward -n N; the wrapper releases
        the lock and keeps going. Only successful tasks count toward the quota."""
        config = agent_wrapper.parse_args(["1", "EASY", "1"])
        test_self = self
        claim_calls = iter([
            {"status": "claimed", "task": test_self._make_task("TASK-101")},
            {"status": "claimed", "task": test_self._make_task("TASK-102")},
        ])
        execute_outcomes = iter(["incomplete", "completed"])

        def fake_claim_next(**kwargs) -> dict:
            return next(claim_calls)

        def fake_execute_task(task, **kwargs) -> str:
            return next(execute_outcomes)

        released: list[str] = []

        def fake_release(task_id, agent_id, note):
            released.append(task_id)
            return True

        outcome = agent_wrapper.run_fleet_loop(
            config,
            agent_name="agent-1",
            claim_next=fake_claim_next,
            execute_task=fake_execute_task,
            release_lock=fake_release,
            sleep=lambda _: None,
        )
        self.assertEqual(outcome, "batch_complete")  # 1 real success hit the quota
        self.assertEqual(released, ["TASK-101"])  # TASK-102 was not released

    def test_run_fleet_loop_bails_after_consecutive_failure_cap(self) -> None:
        """Three incompletes in a row with no successes between them must exit
        with too_many_failures rather than looping forever when no -n is set."""
        config = agent_wrapper.parse_args(["1", "EASY"])  # no quota
        test_self = self
        claim_calls = iter([
            {"status": "claimed", "task": test_self._make_task(f"TASK-10{i}")}
            for i in range(1, 10)
        ])

        released: list[str] = []

        def fake_release(task_id, agent_id, note):
            released.append(task_id)
            return True

        outcome = agent_wrapper.run_fleet_loop(
            config,
            agent_name="agent-1",
            claim_next=lambda **k: next(claim_calls),
            execute_task=lambda *a, **k: "incomplete",
            release_lock=fake_release,
            sleep=lambda _: None,
        )
        self.assertEqual(outcome, "too_many_failures")
        self.assertEqual(
            released,
            ["TASK-101", "TASK-102", "TASK-103"],
        )  # exactly CONSECUTIVE_FAILURE_CAP releases, no more

    def test_consecutive_failures_reset_on_success(self) -> None:
        """A successful task between failures must reset the consecutive failure
        counter; two failures then a success then two more failures should not
        trip the cap."""
        config = agent_wrapper.parse_args(["1", "EASY", "1"])  # quota of 1 success
        test_self = self
        claim_calls = iter([
            {"status": "claimed", "task": test_self._make_task(f"TASK-10{i}")}
            for i in range(1, 10)
        ])
        execute_outcomes = iter(
            ["incomplete", "incomplete", "completed"]  # 2 fails then a win
        )

        outcome = agent_wrapper.run_fleet_loop(
            config,
            agent_name="agent-1",
            claim_next=lambda **k: next(claim_calls),
            execute_task=lambda task, **k: next(execute_outcomes),
            release_lock=lambda *a, **k: True,
            sleep=lambda _: None,
        )
        self.assertEqual(outcome, "batch_complete")  # 1 success hit quota
        # If the counter had not reset, we'd have seen too_many_failures

    def test_run_fleet_loop_backs_off_then_retries_on_no_claimable(self) -> None:
        config = agent_wrapper.parse_args(["1", "EASY", "1"])
        test_self = self

        scenarios = iter([
            # no_claimable with blocked work remaining → back off and retry
            {"status": "no_claimable", "claimable": [],
             "blocked": [{"task_id": "TASK-199"}], "active": [],
             "completed_count": 0, "other_pool_claimable": [],
             "wave": 1, "difficulty": "EASY"},
            # Now something claimable appears
            {"status": "claimed", "task": test_self._make_task("TASK-102")},
        ])

        def fake_claim_next(**kwargs) -> dict:
            return next(scenarios)

        sleeps: list[float] = []

        outcome = agent_wrapper.run_fleet_loop(
            config,
            agent_name="agent-1",
            claim_next=fake_claim_next,
            execute_task=lambda *a, **k: "completed",
            release_lock=lambda *a, **k: True,
            sleep=lambda seconds: sleeps.append(seconds),
        )
        self.assertEqual(outcome, "batch_complete")
        self.assertEqual(sleeps, [120])  # first step of 2/5/10/20/60-min ladder

    def test_run_fleet_loop_recovers_from_called_process_error(self) -> None:
        """A git command exploding under claim_next must not crash the
        wrapper. It should log, count against the consecutive-failure cap,
        back off 60s, and retry on the next pass."""
        config = agent_wrapper.parse_args(["1", "EASY", "1"])
        test_self = self
        call_count = [0]

        def fake_claim_next(**kwargs) -> dict:
            call_count[0] += 1
            if call_count[0] == 1:
                raise subprocess.CalledProcessError(
                    returncode=1,
                    cmd=["git", "pull", "--ff-only", "origin", "main"],
                    output="",
                    stderr="fatal: refusing to merge unrelated histories",
                )
            return {
                "status": "claimed",
                "task": test_self._make_task("TASK-101"),
            }

        sleeps: list[float] = []
        outcome = agent_wrapper.run_fleet_loop(
            config,
            agent_name="agent-1",
            claim_next=fake_claim_next,
            execute_task=lambda *a, **k: "completed",
            release_lock=lambda *a, **k: True,
            sleep=lambda s: sleeps.append(s),
        )
        self.assertEqual(outcome, "batch_complete")
        self.assertEqual(call_count[0], 2)  # retried once after the error
        self.assertEqual(sleeps, [60])  # 60s error backoff, no no_claimable ladder

    def test_run_fleet_loop_recovers_from_task_tool_error(self) -> None:
        """A TaskToolError from task_tool (e.g. dirty worktree rejected by
        require_clean_worktree) must not crash the wrapper — it should log,
        count against the consecutive-failure cap, back off 60s, and retry."""
        config = agent_wrapper.parse_args(["1", "EASY", "1"])
        test_self = self
        call_count = [0]

        def fake_claim_next(**kwargs) -> dict:
            call_count[0] += 1
            if call_count[0] == 1:
                raise agent_wrapper.task_tool.TaskToolError(
                    "working tree is not clean; refusing to claim or break "
                    "locks with local changes present"
                )
            return {
                "status": "claimed",
                "task": test_self._make_task("TASK-101"),
            }

        sleeps: list[float] = []
        outcome = agent_wrapper.run_fleet_loop(
            config,
            agent_name="agent-1",
            claim_next=fake_claim_next,
            execute_task=lambda *a, **k: "completed",
            release_lock=lambda *a, **k: True,
            sleep=lambda s: sleeps.append(s),
        )
        self.assertEqual(outcome, "batch_complete")
        self.assertEqual(call_count[0], 2)
        self.assertEqual(sleeps, [60])

    def test_run_fleet_loop_bails_after_repeated_called_process_errors(self) -> None:
        """The consecutive-failure cap catches repeated git errors so a
        genuinely broken repo exits cleanly instead of looping forever."""
        config = agent_wrapper.parse_args(["1", "EASY"])  # no quota

        def always_explode(**kwargs) -> dict:
            raise subprocess.CalledProcessError(
                returncode=1,
                cmd=["git", "fetch", "origin", "main"],
                output="",
                stderr="fatal: unable to access repo",
            )

        outcome = agent_wrapper.run_fleet_loop(
            config,
            agent_name="agent-1",
            claim_next=always_explode,
            execute_task=lambda *a, **k: "completed",
            release_lock=lambda *a, **k: True,
            sleep=lambda _: None,
        )
        self.assertEqual(outcome, "too_many_failures")


class AgentWrapperPromptTests(unittest.TestCase):
    def _sample_task(self) -> dict:
        return {
            "task_id": "TASK-305",
            "title": "Implement fancy operator",
            "wave": 3,
            "tags": ["HARD", "IMPL"],
            "depends_on": ["TASK-304"],
            "difficulty": "HARD",
        }

    def test_task_prompt_includes_task_id_and_title(self) -> None:
        prompt = agent_wrapper.task_prompt(self._sample_task(), "agent-2")
        self.assertIn("TASK-305", prompt)
        self.assertIn("Implement fancy operator", prompt)
        self.assertIn("agent-2", prompt)

    def test_task_prompt_instructs_to_read_design_docs(self) -> None:
        prompt = agent_wrapper.task_prompt(self._sample_task(), "agent-2")
        self.assertIn("docs/design", prompt)
        self.assertIn("before writing code", prompt.lower())

    def test_task_prompt_instructs_to_mark_done_and_exit(self) -> None:
        prompt = agent_wrapper.task_prompt(self._sample_task(), "agent-2")
        self.assertIn("tasks/active/TASK-305.lock", prompt)
        self.assertIn("tasks/completed/TASK-305.done", prompt)
        self.assertIn("Do not claim another task", prompt)

    def test_task_prompt_mentions_needs_input_contract(self) -> None:
        prompt = agent_wrapper.task_prompt(self._sample_task(), "agent-2")
        self.assertIn("[NEEDS INPUT]", prompt)

    def test_scan_needs_input_detects_marker(self) -> None:
        output = "... working on task ...\n[NEEDS INPUT] Which operator variant?\n"
        self.assertTrue(agent_wrapper.has_needs_input(output))

    def test_scan_needs_input_false_without_marker(self) -> None:
        output = "... working on task ...\ndone.\n"
        self.assertFalse(agent_wrapper.has_needs_input(output))

    def test_system_prompt_contains_agent_and_pool(self) -> None:
        text = agent_wrapper.system_prompt("agent-3", "HARD", "[HARD]")
        self.assertIn("agent-3", text)
        self.assertIn("HARD", text)
        self.assertIn("AGENTS.md", text)


if __name__ == "__main__":
    unittest.main()
