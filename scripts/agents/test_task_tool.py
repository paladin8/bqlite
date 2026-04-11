#!/usr/bin/env python3

import pathlib
import sys
import unittest


SCRIPTS_DIR = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS_DIR))

import task_tool


SAMPLE_TASKS = """
### TASK-101: [EASY][IMPL] First easy task
**Depends on**: none

### TASK-102: [HARD][DESIGN] Hard design task
**Depends on**: TASK-101

### TASK-103: [IMPL] Missing difficulty task
**Depends on**: TASK-101

### TASK-104: [EASY][RETIRED] Retired task
**Depends on**: none
"""


class TaskToolTests(unittest.TestCase):
    def test_parse_tasks_text_extracts_tags_and_dependencies(self) -> None:
        tasks = task_tool.parse_tasks_text(SAMPLE_TASKS)

        self.assertEqual(tasks["TASK-101"].difficulty, "EASY")
        self.assertEqual(tasks["TASK-102"].difficulty, "HARD")
        self.assertIsNone(tasks["TASK-103"].difficulty)
        self.assertTrue(tasks["TASK-104"].retired)
        self.assertEqual(tasks["TASK-102"].depends_on, ("TASK-101",))
        self.assertEqual(tasks["TASK-101"].wave, 1)

    def test_classify_tasks_flags_missing_difficulty_before_claim(self) -> None:
        tasks = task_tool.parse_tasks_text(SAMPLE_TASKS)
        classified = task_tool.classify_tasks(
            tasks=tasks,
            wave=1,
            difficulty="EASY",
            active={},
            completed={"TASK-101": {}},
        )

        self.assertEqual([task.task_id for task in classified["claimable"]], [])
        self.assertEqual(
            [task.task_id for task in classified["claimable_missing_difficulty"]],
            ["TASK-103"],
        )

    def test_classify_tasks_filters_other_pool_and_dependencies(self) -> None:
        tasks = task_tool.parse_tasks_text(SAMPLE_TASKS)
        classified = task_tool.classify_tasks(
            tasks=tasks,
            wave=1,
            difficulty="HARD",
            active={},
            completed={"TASK-101": {}},
        )

        self.assertEqual([task.task_id for task in classified["claimable"]], ["TASK-102"])
        self.assertEqual(classified["other_pool_claimable"], [])

    def test_parse_rejects_both_easy_and_hard_tags(self) -> None:
        markdown = "### TASK-101: [EASY][HARD][IMPL] Confused task\n**Depends on**: none\n"
        with self.assertRaises(task_tool.TaskToolError) as ctx:
            task_tool.parse_tasks_text(markdown)
        self.assertIn("TASK-101", str(ctx.exception))


class TaskToolPathHelperTests(unittest.TestCase):
    def test_task_lock_path_points_to_active_dir(self) -> None:
        path = task_tool.task_lock_path("TASK-042")
        self.assertEqual(path.name, "TASK-042.lock")
        self.assertEqual(path.parent.name, "active")

    def test_task_done_path_points_to_completed_dir(self) -> None:
        path = task_tool.task_done_path("TASK-042")
        self.assertEqual(path.name, "TASK-042.done")
        self.assertEqual(path.parent.name, "completed")

    def test_is_wave_drained_when_nothing_claimable(self) -> None:
        classified = {
            "claimable": [],
            "claimable_missing_difficulty": [],
            "blocked": [],
            "active": [],
            "completed": [],
            "other_pool_claimable": [],
        }
        self.assertTrue(task_tool.is_wave_drained(classified))

    def test_is_wave_drained_false_when_blocked_work_remains(self) -> None:
        dummy = task_tool.Task(
            task_id="TASK-201",
            number=201,
            wave=2,
            title="dep-blocked task",
            tags=("EASY",),
            depends_on=("TASK-200",),
        )
        classified = {
            "claimable": [],
            "claimable_missing_difficulty": [],
            "blocked": [dummy],
            "active": [],
            "completed": [],
            "other_pool_claimable": [],
        }
        self.assertFalse(task_tool.is_wave_drained(classified))

    def test_is_wave_drained_false_when_active_work_remains(self) -> None:
        dummy = task_tool.Task(
            task_id="TASK-202",
            number=202,
            wave=2,
            title="active task",
            tags=("EASY",),
            depends_on=(),
        )
        classified = {
            "claimable": [],
            "claimable_missing_difficulty": [],
            "blocked": [],
            "active": [dummy],
            "completed": [],
            "other_pool_claimable": [],
        }
        self.assertFalse(task_tool.is_wave_drained(classified))


if __name__ == "__main__":
    unittest.main()
