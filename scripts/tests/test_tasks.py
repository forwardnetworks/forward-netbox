from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import datetime
from datetime import timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from unittest.mock import patch

from invoke.exceptions import CommandTimedOut
from invoke.exceptions import Exit

import tasks


class SmokeSyncTaskTest(unittest.TestCase):
    def test_smoke_sync_uses_safe_bulk_orm_by_default(self):
        context = Mock()

        with patch.object(tasks, "manage_py") as manage_py:
            tasks.smoke_sync.body(context)

        manage_py.assert_called_once_with(context, "forward_smoke_sync")

    def test_smoke_sync_can_disable_safe_bulk_orm_for_comparison(self):
        context = Mock()

        with patch.object(tasks, "manage_py") as manage_py:
            tasks.smoke_sync.body(context, enable_bulk_orm=False)

        manage_py.assert_called_once_with(
            context,
            "forward_smoke_sync --disable-bulk-orm",
        )


class SyncHealthGateTaskTest(unittest.TestCase):
    def _result(self, payload):
        return SimpleNamespace(
            stdout="🧬 loaded config '/etc/netbox/config/configuration.py'\n"
            + json.dumps(payload, indent=2)
            + "\n"
        )

    def test_parse_json_from_manage_output_handles_prefix_noise(self):
        payload = {"sync_id": 123, "sync_status": "syncing"}
        parsed = tasks._parse_json_from_manage_output(self._result(payload).stdout)
        self.assertEqual(parsed, payload)

    def test_sync_health_gate_passes_when_completed_without_findings(self):
        context = Mock()
        responses = [
            self._result({"sync_id": 51, "sync_status": "completed"}),
            self._result({"counts": {"blocking": 0}}),
            self._result({"warning_count": 0, "error_count": 0}),
        ]
        with patch.object(tasks, "manage_py", side_effect=responses):
            tasks.sync_health_gate.body(
                context,
                sync_id=51,
                max_polls=1,
                interval_seconds=1,
            )

    def test_sync_health_gate_fails_when_warnings_present(self):
        context = Mock()
        responses = [
            self._result({"sync_id": 51, "sync_status": "syncing"}),
            self._result({"counts": {"blocking": 0}}),
            self._result({"warning_count": 2, "error_count": 0}),
        ]
        with patch.object(tasks, "manage_py", side_effect=responses):
            with self.assertRaises(Exit) as raised:
                tasks.sync_health_gate.body(
                    context,
                    sync_id=51,
                    max_polls=1,
                    interval_seconds=1,
                )
        self.assertEqual(raised.exception.code, 3)
        self.assertIn("warning issues detected", str(raised.exception))

    def test_sync_health_gate_can_fail_on_suppressed_warnings(self):
        context = Mock()
        responses = [
            self._result({"sync_id": 51, "sync_status": "syncing"}),
            self._result({"counts": {"blocking": 0}}),
            self._result(
                {
                    "warning_count": 0,
                    "suppressed_warning_count": 2,
                    "error_count": 0,
                }
            ),
        ]
        with patch.object(tasks, "manage_py", side_effect=responses):
            with self.assertRaises(Exit) as raised:
                tasks.sync_health_gate.body(
                    context,
                    sync_id=51,
                    max_polls=1,
                    interval_seconds=1,
                    fail_on_suppressed_warning=True,
                )
        self.assertEqual(raised.exception.code, 3)
        self.assertIn("suppressed warning issues detected", str(raised.exception))

    def test_sync_health_gate_can_pass_nonterminal_when_enabled(self):
        context = Mock()
        responses = [
            self._result({"sync_id": 51, "sync_status": "syncing"}),
            self._result({"counts": {"blocking": 0}}),
            self._result({"warning_count": 0, "error_count": 0}),
        ]
        with patch.object(tasks, "manage_py", side_effect=responses):
            tasks.sync_health_gate.body(
                context,
                sync_id=51,
                max_polls=1,
                interval_seconds=1,
                allow_nonterminal=True,
            )

    def test_sync_health_gate_uses_latest_ingestion_warning_scope_by_default(self):
        context = Mock()
        responses = [
            self._result({"sync_id": 51, "sync_status": "completed"}),
            self._result({"counts": {"blocking": 0}}),
            self._result({"warning_count": 0, "error_count": 0}),
        ]
        with patch.object(tasks, "manage_py", side_effect=responses) as manage_py:
            tasks.sync_health_gate.body(
                context,
                sync_id=51,
                max_polls=1,
                interval_seconds=1,
            )
        commands = [call.args[1] for call in manage_py.call_args_list]
        self.assertTrue(
            any("forward_warning_audit --sync-id 51" in c for c in commands)
        )
        self.assertFalse(any("--all-ingestions" in c for c in commands))

    def test_sync_health_gate_can_include_all_ingestions(self):
        context = Mock()
        responses = [
            self._result({"sync_id": 51, "sync_status": "completed"}),
            self._result({"counts": {"blocking": 0}}),
            self._result({"warning_count": 0, "error_count": 0}),
        ]
        with patch.object(tasks, "manage_py", side_effect=responses) as manage_py:
            tasks.sync_health_gate.body(
                context,
                sync_id=51,
                max_polls=1,
                interval_seconds=1,
                include_all_ingestions=True,
            )
        commands = [call.args[1] for call in manage_py.call_args_list]
        self.assertTrue(
            any(
                "forward_warning_audit --sync-id 51 --all-ingestions" in c
                for c in commands
            )
        )

    def test_sync_health_gate_tolerates_transient_failed_status(self):
        context = Mock()
        responses = [
            self._result({"sync_id": 51, "sync_status": "failed"}),
            self._result({"counts": {"blocking": 0}}),
            self._result({"warning_count": 0, "error_count": 0}),
            self._result({"sync_id": 51, "sync_status": "syncing"}),
            self._result({"counts": {"blocking": 0}}),
            self._result({"warning_count": 0, "error_count": 0}),
        ]
        with (
            patch.object(tasks, "manage_py", side_effect=responses),
            patch.object(tasks.time, "sleep"),
        ):
            tasks.sync_health_gate.body(
                context,
                sync_id=51,
                max_polls=2,
                interval_seconds=1,
                allow_nonterminal=True,
            )

    def test_sync_health_gate_fails_after_consecutive_failed_status(self):
        context = Mock()
        responses = [
            self._result({"sync_id": 51, "sync_status": "failed"}),
            self._result({"counts": {"blocking": 0}}),
            self._result({"warning_count": 0, "error_count": 0}),
            self._result({"sync_id": 51, "sync_status": "failed"}),
            self._result({"counts": {"blocking": 0}}),
            self._result({"warning_count": 0, "error_count": 0}),
        ]
        with (
            patch.object(tasks, "manage_py", side_effect=responses),
            patch.object(tasks.time, "sleep"),
        ):
            with self.assertRaises(Exit) as raised:
                tasks.sync_health_gate.body(
                    context,
                    sync_id=51,
                    max_polls=2,
                    interval_seconds=1,
                )
        self.assertEqual(raised.exception.code, 3)

    def test_sync_health_gate_tolerates_failed_sync_when_run_is_active(self):
        context = Mock()
        responses = [
            self._result(
                {
                    "sync_id": 51,
                    "sync_status": "failed",
                    "execution_run": {"status": "running"},
                }
            ),
            self._result({"counts": {"blocking": 0}}),
            self._result({"warning_count": 0, "error_count": 0}),
        ]
        with patch.object(tasks, "manage_py", side_effect=responses):
            tasks.sync_health_gate.body(
                context,
                sync_id=51,
                max_polls=1,
                interval_seconds=1,
                allow_nonterminal=True,
            )

    def test_sync_health_gate_retries_transient_parse_failure(self):
        context = Mock()
        responses = [
            SimpleNamespace(stdout="WARNING database unavailable\n"),
            self._result({"sync_id": 51, "sync_status": "completed"}),
            self._result({"counts": {"blocking": 0}}),
            self._result({"warning_count": 0, "error_count": 0}),
        ]
        with (
            patch.object(tasks, "manage_py", side_effect=responses),
            patch.object(tasks.time, "sleep"),
        ):
            tasks.sync_health_gate.body(
                context,
                sync_id=51,
                max_polls=1,
                interval_seconds=1,
            )


class SyncHealthMonitorTaskTest(unittest.TestCase):
    def _result(self, payload):
        return SimpleNamespace(
            stdout="🧬 loaded config '/etc/netbox/config/configuration.py'\n"
            + json.dumps(payload, indent=2)
            + "\n"
        )

    def test_sync_health_monitor_writes_evidence_for_multiple_syncs(self):
        context = Mock()
        responses = [
            self._result({"sync_id": 50, "sync_status": "syncing"}),
            self._result({"counts": {"blocking": 0}}),
            self._result({"warning_count": 0, "error_count": 0}),
            self._result({"sync_id": 51, "sync_status": "merging"}),
            self._result({"counts": {"blocking": 0}}),
            self._result({"warning_count": 0, "error_count": 0}),
        ]
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_json = str(Path(tmp_dir) / "sync-health-monitor.json")
            with patch.object(tasks, "manage_py", side_effect=responses):
                tasks.sync_health_monitor.body(
                    context,
                    sync_ids="50,51",
                    max_polls=1,
                    interval_seconds=1,
                    allow_nonterminal=True,
                    output_json=output_json,
                )
            payload = json.loads(Path(output_json).read_text(encoding="utf-8"))
            self.assertEqual(payload["sync_ids"], [50, 51])
            self.assertEqual(len(payload["samples"]), 2)

    def test_sync_health_monitor_fails_on_warning(self):
        context = Mock()
        responses = [
            self._result({"sync_id": 50, "sync_status": "syncing"}),
            self._result({"counts": {"blocking": 0}}),
            self._result({"warning_count": 1, "error_count": 0}),
        ]
        with patch.object(tasks, "manage_py", side_effect=responses):
            with self.assertRaises(Exit) as raised:
                tasks.sync_health_monitor.body(
                    context,
                    sync_ids="50",
                    max_polls=1,
                    interval_seconds=1,
                    allow_nonterminal=True,
                )
        self.assertEqual(raised.exception.code, 3)

    def test_sync_health_monitor_can_fail_on_suppressed_warnings(self):
        context = Mock()
        responses = [
            self._result({"sync_id": 50, "sync_status": "syncing"}),
            self._result({"counts": {"blocking": 0}}),
            self._result(
                {
                    "warning_count": 0,
                    "suppressed_warning_count": 1,
                    "error_count": 0,
                }
            ),
        ]
        with patch.object(tasks, "manage_py", side_effect=responses):
            with self.assertRaises(Exit) as raised:
                tasks.sync_health_monitor.body(
                    context,
                    sync_ids="50",
                    max_polls=1,
                    interval_seconds=1,
                    allow_nonterminal=True,
                    fail_on_suppressed_warning=True,
                )
        self.assertEqual(raised.exception.code, 3)

    def test_sync_health_monitor_writes_partial_evidence_before_failure(self):
        context = Mock()
        responses = [
            self._result({"sync_id": 50, "sync_status": "syncing"}),
            self._result({"counts": {"blocking": 0}}),
            self._result({"warning_count": 1, "error_count": 0}),
        ]
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_json = str(Path(tmp_dir) / "sync-health-monitor-fail.json")
            with patch.object(tasks, "manage_py", side_effect=responses):
                with self.assertRaises(Exit):
                    tasks.sync_health_monitor.body(
                        context,
                        sync_ids="50",
                        max_polls=1,
                        interval_seconds=1,
                        allow_nonterminal=True,
                        output_json=output_json,
                    )
            payload = json.loads(Path(output_json).read_text(encoding="utf-8"))
            self.assertFalse(payload["completed"])
            self.assertEqual(len(payload["samples"]), 1)

    def test_sync_health_monitor_fails_when_execution_run_is_failed(self):
        context = Mock()
        responses = [
            self._result(
                {
                    "sync_id": 50,
                    "sync_status": "syncing",
                    "execution_run": {"status": "failed"},
                }
            ),
            self._result({"counts": {"blocking": 0}}),
            self._result({"warning_count": 0, "error_count": 0}),
        ]
        with patch.object(tasks, "manage_py", side_effect=responses):
            with self.assertRaises(Exit) as raised:
                tasks.sync_health_monitor.body(
                    context,
                    sync_ids="50",
                    max_polls=1,
                    interval_seconds=1,
                    allow_nonterminal=True,
                    failed_status_threshold=1,
                )
        self.assertEqual(raised.exception.code, 3)

    def test_sync_health_monitor_retries_transient_parse_failure(self):
        context = Mock()
        responses = [
            SimpleNamespace(stdout="WARNING database unavailable\n"),
            self._result({"sync_id": 50, "sync_status": "syncing"}),
            self._result({"counts": {"blocking": 0}}),
            self._result({"warning_count": 0, "error_count": 0}),
        ]
        with (
            patch.object(tasks, "manage_py", side_effect=responses),
            patch.object(tasks.time, "sleep"),
        ):
            tasks.sync_health_monitor.body(
                context,
                sync_ids="50",
                max_polls=1,
                interval_seconds=1,
                allow_nonterminal=True,
            )


class SyncAutoRecoverMonitorTaskTest(unittest.TestCase):
    def _result(self, payload):
        return SimpleNamespace(
            stdout="🧬 loaded config '/etc/netbox/config/configuration.py'\n"
            + json.dumps(payload, indent=2)
            + "\n"
        )

    def test_autorecover_monitor_requeues_dead_inflight_job(self):
        context = Mock()
        responses = [
            self._result(
                {
                    "sync_id": 50,
                    "sync_name": "sample-sync",
                    "sync_status": "syncing",
                    "execution_run": {
                        "status": "running",
                        "active_step": {
                            "status": "running",
                            "job_live": False,
                            "job_id": 123,
                        },
                    },
                }
            ),
            self._result({"counts": {"blocking": 0}}),
            self._result({"warning_count": 0, "error_count": 0}),
            self._result({"run": {"status": "running"}}),
        ]
        with patch.object(tasks, "manage_py", side_effect=responses) as manage_py:
            tasks.sync_autorecover_monitor.body(
                context,
                sync_ids="50",
                max_polls=1,
                interval_seconds=1,
                allow_nonterminal=True,
            )
        commands = [call.args[1] for call in manage_py.call_args_list]
        self.assertTrue(
            any(
                'forward_execution_run_recovery --sync-name "sample-sync" --enqueue-next'
                in command
                for command in commands
            )
        )

    def test_autorecover_monitor_fails_on_warning(self):
        context = Mock()
        responses = [
            self._result(
                {
                    "sync_id": 50,
                    "sync_name": "sample-sync",
                    "sync_status": "syncing",
                    "execution_run": {"status": "running", "active_step": {}},
                }
            ),
            self._result({"counts": {"blocking": 0}}),
            self._result({"warning_count": 1, "error_count": 0}),
        ]
        with patch.object(tasks, "manage_py", side_effect=responses):
            with self.assertRaises(Exit) as raised:
                tasks.sync_autorecover_monitor.body(
                    context,
                    sync_ids="50",
                    max_polls=1,
                    interval_seconds=1,
                    allow_nonterminal=True,
                )
        self.assertEqual(raised.exception.code, 3)

    def test_autorecover_monitor_can_fail_on_suppressed_warnings(self):
        context = Mock()
        responses = [
            self._result(
                {
                    "sync_id": 50,
                    "sync_name": "sample-sync",
                    "sync_status": "syncing",
                    "execution_run": {"status": "running", "active_step": {}},
                }
            ),
            self._result({"counts": {"blocking": 0}}),
            self._result(
                {
                    "warning_count": 0,
                    "suppressed_warning_count": 1,
                    "error_count": 0,
                }
            ),
        ]
        with patch.object(tasks, "manage_py", side_effect=responses):
            with self.assertRaises(Exit) as raised:
                tasks.sync_autorecover_monitor.body(
                    context,
                    sync_ids="50",
                    max_polls=1,
                    interval_seconds=1,
                    allow_nonterminal=True,
                    fail_on_suppressed_warning=True,
                )
        self.assertEqual(raised.exception.code, 3)

    def test_autorecover_monitor_requeues_terminal_run_status(self):
        context = Mock()
        responses = [
            self._result(
                {
                    "sync_id": 50,
                    "sync_name": "sample-sync",
                    "sync_status": "syncing",
                    "execution_run": {
                        "status": "timeout",
                        "active_step": {
                            "status": "pending",
                            "job_live": None,
                            "job_id": None,
                        },
                    },
                }
            ),
            self._result({"counts": {"blocking": 0}}),
            self._result({"warning_count": 0, "error_count": 0}),
            self._result({"run": {"status": "running"}}),
        ]
        with patch.object(tasks, "manage_py", side_effect=responses) as manage_py:
            tasks.sync_autorecover_monitor.body(
                context,
                sync_ids="50",
                max_polls=1,
                interval_seconds=1,
                allow_nonterminal=True,
                failed_status_threshold=2,
            )
        commands = [call.args[1] for call in manage_py.call_args_list]
        self.assertTrue(
            any(
                'forward_execution_run_recovery --sync-name "sample-sync" --enqueue-next'
                in command
                for command in commands
            )
        )

    def test_autorecover_monitor_requeues_actionable_step_failure(self):
        context = Mock()
        watch_payload = self._result(
            {
                "sync_id": 50,
                "sync_name": "sample-sync",
                "sync_status": "syncing",
                "execution_run": {
                    "status": "running",
                    "active_step": {
                        "id": 321,
                        "status": "merge_timeout",
                        "job_live": False,
                        "job_id": None,
                    },
                },
            }
        )
        responses = [
            watch_payload,
            self._result({"counts": {"blocking": 0}}),
            self._result({"warning_count": 0, "error_count": 0}),
            watch_payload,
            self._result({"counts": {"blocking": 0}}),
            self._result({"warning_count": 0, "error_count": 0}),
            self._result({"run": {"status": "running"}}),
        ]
        with patch.object(tasks, "manage_py", side_effect=responses) as manage_py:
            tasks.sync_autorecover_monitor.body(
                context,
                sync_ids="50",
                max_polls=2,
                interval_seconds=1,
                allow_nonterminal=True,
                failed_status_threshold=2,
            )
        commands = [call.args[1] for call in manage_py.call_args_list]
        self.assertTrue(
            any(
                'forward_execution_run_recovery --sync-name "sample-sync" --enqueue-next'
                in command
                for command in commands
            )
        )

    def test_autorecover_monitor_does_not_requeue_transient_actionable_failure(self):
        context = Mock()
        responses = [
            self._result(
                {
                    "sync_id": 50,
                    "sync_name": "sample-sync",
                    "sync_status": "syncing",
                    "execution_run": {
                        "status": "running",
                        "active_step": {
                            "id": 321,
                            "status": "merge_timeout",
                            "job_live": False,
                            "job_id": None,
                        },
                    },
                }
            ),
            self._result({"counts": {"blocking": 0}}),
            self._result({"warning_count": 0, "error_count": 0}),
        ]
        with patch.object(tasks, "manage_py", side_effect=responses) as manage_py:
            tasks.sync_autorecover_monitor.body(
                context,
                sync_ids="50",
                max_polls=1,
                interval_seconds=1,
                allow_nonterminal=True,
                failed_status_threshold=2,
            )
        commands = [call.args[1] for call in manage_py.call_args_list]
        self.assertFalse(
            any(
                'forward_execution_run_recovery --sync-name "sample-sync" --enqueue-next'
                in command
                for command in commands
            )
        )

    def test_autorecover_monitor_requeues_orphan_pending_step(self):
        context = Mock()
        responses = [
            self._result(
                {
                    "sync_id": 50,
                    "sync_name": "sample-sync",
                    "sync_status": "syncing",
                    "execution_run": {
                        "status": "running",
                        "active_step": {
                            "status": "pending",
                            "job_live": None,
                            "job_id": None,
                            "id": 123,
                            "created_age_seconds": 300,
                        },
                    },
                }
            ),
            self._result({"counts": {"blocking": 0}}),
            self._result({"warning_count": 0, "error_count": 0}),
            self._result(
                {
                    "sync_id": 50,
                    "sync_name": "sample-sync",
                    "sync_status": "syncing",
                    "execution_run": {
                        "status": "running",
                        "active_step": {
                            "status": "pending",
                            "job_live": None,
                            "job_id": None,
                            "id": 123,
                            "created_age_seconds": 300,
                        },
                    },
                }
            ),
            self._result({"counts": {"blocking": 0}}),
            self._result({"warning_count": 0, "error_count": 0}),
            self._result({"run": {"status": "running"}}),
        ]
        with patch.object(tasks, "manage_py", side_effect=responses) as manage_py:
            tasks.sync_autorecover_monitor.body(
                context,
                sync_ids="50",
                max_polls=2,
                interval_seconds=1,
                allow_nonterminal=True,
                failed_status_threshold=2,
            )
        commands = [call.args[1] for call in manage_py.call_args_list]
        self.assertTrue(
            any(
                'forward_execution_run_recovery --sync-name "sample-sync" --enqueue-next'
                in command
                for command in commands
            )
        )

    def test_autorecover_monitor_requeues_staged_waiting_merge(self):
        context = Mock()
        watch_payload = self._result(
            {
                "sync_id": 50,
                "sync_name": "sample-sync",
                "sync_status": "syncing",
                "execution_run": {
                    "id": 88,
                    "status": "waiting",
                    "next_step_index": 12,
                    "active_step": {
                        "id": 456,
                        "status": "staged",
                        "job_live": False,
                        "job_id": 789,
                        "heartbeat_age_seconds": 1800,
                        "attempted_row_count": 7346,
                        "applied_row_count": 7346,
                        "fetched_row_count": 7346,
                    },
                },
            }
        )
        responses = [
            watch_payload,
            self._result({"counts": {"blocking": 0}}),
            self._result({"warning_count": 0, "error_count": 0}),
            watch_payload,
            self._result({"counts": {"blocking": 0}}),
            self._result({"warning_count": 0, "error_count": 0}),
            self._result({"run": {"status": "running"}}),
        ]
        with patch.object(tasks, "manage_py", side_effect=responses) as manage_py:
            tasks.sync_autorecover_monitor.body(
                context,
                sync_ids="50",
                max_polls=2,
                interval_seconds=1,
                allow_nonterminal=True,
                failed_status_threshold=2,
            )
        commands = [call.args[1] for call in manage_py.call_args_list]
        self.assertTrue(
            any(
                'forward_execution_run_recovery --sync-name "sample-sync" --enqueue-next'
                in command
                for command in commands
            )
        )

    def test_autorecover_monitor_fail_on_recovery_raises(self):
        context = Mock()
        responses = [
            self._result(
                {
                    "sync_id": 50,
                    "sync_name": "sample-sync",
                    "sync_status": "syncing",
                    "execution_run": {
                        "status": "running",
                        "active_step": {
                            "status": "pending",
                            "job_live": None,
                            "job_id": None,
                            "id": 123,
                            "created_age_seconds": 300,
                        },
                    },
                }
            ),
            self._result({"counts": {"blocking": 0}}),
            self._result({"warning_count": 0, "error_count": 0}),
            self._result(
                {
                    "sync_id": 50,
                    "sync_name": "sample-sync",
                    "sync_status": "syncing",
                    "execution_run": {
                        "status": "running",
                        "active_step": {
                            "status": "pending",
                            "job_live": None,
                            "job_id": None,
                            "id": 123,
                            "created_age_seconds": 300,
                        },
                    },
                }
            ),
            self._result({"counts": {"blocking": 0}}),
            self._result({"warning_count": 0, "error_count": 0}),
            self._result({"run": {"status": "running"}}),
        ]
        with patch.object(tasks, "manage_py", side_effect=responses):
            with self.assertRaises(Exit) as raised:
                tasks.sync_autorecover_monitor.body(
                    context,
                    sync_ids="50",
                    max_polls=2,
                    interval_seconds=1,
                    allow_nonterminal=True,
                    failed_status_threshold=2,
                    fail_on_recovery=True,
                )
        self.assertEqual(raised.exception.code, 3)
        self.assertIn("recovery actions were required", str(raised.exception))

    def test_autorecover_monitor_writes_evidence_before_fail_on_recovery(self):
        context = Mock()
        responses = [
            self._result(
                {
                    "sync_id": 50,
                    "sync_name": "sample-sync",
                    "sync_status": "syncing",
                    "execution_run": {
                        "status": "running",
                        "active_step": {
                            "status": "pending",
                            "job_live": None,
                            "job_id": None,
                            "id": 123,
                            "created_age_seconds": 300,
                        },
                    },
                }
            ),
            self._result({"counts": {"blocking": 0}}),
            self._result({"warning_count": 0, "error_count": 0}),
            self._result(
                {
                    "sync_id": 50,
                    "sync_name": "sample-sync",
                    "sync_status": "syncing",
                    "execution_run": {
                        "status": "running",
                        "active_step": {
                            "status": "pending",
                            "job_live": None,
                            "job_id": None,
                            "id": 123,
                            "created_age_seconds": 300,
                        },
                    },
                }
            ),
            self._result({"counts": {"blocking": 0}}),
            self._result({"warning_count": 0, "error_count": 0}),
            self._result({"run": {"status": "running"}}),
        ]
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_json = str(Path(tmp_dir) / "sync-autorecover-fail.json")
            with patch.object(tasks, "manage_py", side_effect=responses):
                with self.assertRaises(Exit):
                    tasks.sync_autorecover_monitor.body(
                        context,
                        sync_ids="50",
                        max_polls=2,
                        interval_seconds=1,
                        allow_nonterminal=True,
                        failed_status_threshold=2,
                        fail_on_recovery=True,
                        output_json=output_json,
                    )
            payload = json.loads(Path(output_json).read_text(encoding="utf-8"))
            self.assertTrue(payload["completed"])
            self.assertGreaterEqual(len(payload["samples"]), 1)
            self.assertEqual(len(payload["recovery_actions"]), 1)

    def test_autorecover_monitor_requeues_stalled_inflight_progress(self):
        context = Mock()
        watch_payload = self._result(
            {
                "sync_id": 50,
                "sync_name": "sample-sync",
                "sync_status": "syncing",
                "execution_run": {
                    "id": 88,
                    "status": "running",
                    "next_step_index": 12,
                    "active_step": {
                        "id": 456,
                        "status": "running",
                        "job_live": True,
                        "job_id": 789,
                        "heartbeat_age_seconds": 1800,
                        "attempted_row_count": 0,
                        "applied_row_count": 0,
                        "fetched_row_count": 7346,
                    },
                },
            }
        )
        responses = [
            watch_payload,
            self._result({"counts": {"blocking": 0}}),
            self._result({"warning_count": 0, "error_count": 0}),
            watch_payload,
            self._result({"counts": {"blocking": 0}}),
            self._result({"warning_count": 0, "error_count": 0}),
            watch_payload,
            self._result({"counts": {"blocking": 0}}),
            self._result({"warning_count": 0, "error_count": 0}),
            watch_payload,
            self._result({"counts": {"blocking": 0}}),
            self._result({"warning_count": 0, "error_count": 0}),
            self._result({"run": {"status": "running"}}),
        ]
        with patch.object(tasks, "manage_py", side_effect=responses) as manage_py:
            tasks.sync_autorecover_monitor.body(
                context,
                sync_ids="50",
                max_polls=4,
                interval_seconds=1,
                allow_nonterminal=True,
                failed_status_threshold=2,
            )
        commands = [call.args[1] for call in manage_py.call_args_list]
        self.assertTrue(
            any(
                'forward_execution_run_recovery --sync-name "sample-sync" --enqueue-next'
                in command
                for command in commands
            )
        )

    def test_autorecover_monitor_does_not_requeue_recent_running_step(self):
        context = Mock()
        watch_payload = self._result(
            {
                "sync_id": 50,
                "sync_name": "sample-sync",
                "sync_status": "syncing",
                "execution_run": {
                    "id": 88,
                    "status": "running",
                    "next_step_index": 12,
                    "active_step": {
                        "id": 456,
                        "status": "running",
                        "job_live": True,
                        "job_id": 789,
                        "heartbeat_age_seconds": 120,
                        "attempted_row_count": 0,
                        "applied_row_count": 0,
                        "fetched_row_count": 7346,
                    },
                },
            }
        )
        responses = [
            watch_payload,
            self._result({"counts": {"blocking": 0}}),
            self._result({"warning_count": 0, "error_count": 0}),
            watch_payload,
            self._result({"counts": {"blocking": 0}}),
            self._result({"warning_count": 0, "error_count": 0}),
            watch_payload,
            self._result({"counts": {"blocking": 0}}),
            self._result({"warning_count": 0, "error_count": 0}),
            watch_payload,
            self._result({"counts": {"blocking": 0}}),
            self._result({"warning_count": 0, "error_count": 0}),
        ]
        with patch.object(tasks, "manage_py", side_effect=responses) as manage_py:
            tasks.sync_autorecover_monitor.body(
                context,
                sync_ids="50",
                max_polls=4,
                interval_seconds=1,
                allow_nonterminal=True,
                failed_status_threshold=2,
            )
        commands = [call.args[1] for call in manage_py.call_args_list]
        self.assertFalse(
            any(
                'forward_execution_run_recovery --sync-name "sample-sync" --enqueue-next'
                in command
                for command in commands
            )
        )

    def test_autorecover_monitor_does_not_requeue_pending_step_with_progress(self):
        context = Mock()
        watch_payload = self._result(
            {
                "sync_id": 50,
                "sync_name": "sample-sync",
                "sync_status": "syncing",
                "execution_run": {
                    "id": 88,
                    "status": "running",
                    "next_step_index": 12,
                    "active_step": {
                        "id": 456,
                        "status": "pending",
                        "job_live": None,
                        "job_id": None,
                        "created_age_seconds": 3600,
                        "attempted_row_count": 500,
                        "applied_row_count": 500,
                        "fetched_row_count": 7346,
                    },
                },
            }
        )
        responses = [
            watch_payload,
            self._result({"counts": {"blocking": 0}}),
            self._result({"warning_count": 0, "error_count": 0}),
            watch_payload,
            self._result({"counts": {"blocking": 0}}),
            self._result({"warning_count": 0, "error_count": 0}),
        ]
        with patch.object(tasks, "manage_py", side_effect=responses) as manage_py:
            tasks.sync_autorecover_monitor.body(
                context,
                sync_ids="50",
                max_polls=2,
                interval_seconds=1,
                allow_nonterminal=True,
                failed_status_threshold=2,
            )
        commands = [call.args[1] for call in manage_py.call_args_list]
        self.assertFalse(
            any(
                'forward_execution_run_recovery --sync-name "sample-sync" --enqueue-next'
                in command
                for command in commands
            )
        )

    def test_autorecover_monitor_does_not_requeue_pending_step_with_fresh_heartbeat(
        self,
    ):
        context = Mock()
        watch_payload = self._result(
            {
                "sync_id": 50,
                "sync_name": "sample-sync",
                "sync_status": "syncing",
                "execution_run": {
                    "id": 88,
                    "status": "running",
                    "next_step_index": 12,
                    "active_step": {
                        "id": 456,
                        "status": "pending",
                        "job_live": None,
                        "job_id": None,
                        "created_age_seconds": 7200,
                        "heartbeat_age_seconds": 5,
                        "attempted_row_count": 0,
                        "applied_row_count": 0,
                        "fetched_row_count": 7346,
                    },
                },
            }
        )
        responses = [
            watch_payload,
            self._result({"counts": {"blocking": 0}}),
            self._result({"warning_count": 0, "error_count": 0}),
            watch_payload,
            self._result({"counts": {"blocking": 0}}),
            self._result({"warning_count": 0, "error_count": 0}),
        ]
        with patch.object(tasks, "manage_py", side_effect=responses) as manage_py:
            tasks.sync_autorecover_monitor.body(
                context,
                sync_ids="50",
                max_polls=2,
                interval_seconds=1,
                allow_nonterminal=True,
                failed_status_threshold=2,
            )
        commands = [call.args[1] for call in manage_py.call_args_list]
        self.assertFalse(
            any(
                'forward_execution_run_recovery --sync-name "sample-sync" --enqueue-next'
                in command
                for command in commands
            )
        )

    def test_autorecover_monitor_requeues_pending_step_with_fresh_heartbeat_after_streak(
        self,
    ):
        context = Mock()
        watch_payload = self._result(
            {
                "sync_id": 50,
                "sync_name": "sample-sync",
                "sync_status": "syncing",
                "execution_run": {
                    "id": 88,
                    "status": "running",
                    "next_step_index": 12,
                    "active_step": {
                        "id": 456,
                        "status": "pending",
                        "job_live": None,
                        "job_id": None,
                        "created_age_seconds": 7200,
                        "heartbeat_age_seconds": 5,
                        "attempted_row_count": 0,
                        "applied_row_count": 0,
                        "fetched_row_count": 7346,
                    },
                },
            }
        )
        responses = []
        for _ in range(6):
            responses.extend(
                [
                    watch_payload,
                    self._result({"counts": {"blocking": 0}}),
                    self._result({"warning_count": 0, "error_count": 0}),
                ]
            )
        responses.append(self._result({"run": {"status": "running"}}))
        with patch.object(tasks, "manage_py", side_effect=responses) as manage_py:
            tasks.sync_autorecover_monitor.body(
                context,
                sync_ids="50",
                max_polls=6,
                interval_seconds=1,
                allow_nonterminal=True,
                failed_status_threshold=2,
            )
        commands = [call.args[1] for call in manage_py.call_args_list]
        self.assertTrue(
            any(
                'forward_execution_run_recovery --sync-name "sample-sync" --enqueue-next'
                in command
                for command in commands
            )
        )

    def test_autorecover_monitor_does_not_requeue_pending_step_when_run_heartbeat_is_fresh(
        self,
    ):
        context = Mock()
        watch_payload = self._result(
            {
                "sync_id": 50,
                "sync_name": "sample-sync",
                "sync_status": "syncing",
                "execution_run": {
                    "id": 88,
                    "status": "running",
                    "next_step_index": 12,
                    "latest_heartbeat_age_seconds": 5,
                    "active_step": {
                        "id": 456,
                        "status": "pending",
                        "job_live": None,
                        "job_id": None,
                        "created_age_seconds": 7200,
                        "heartbeat_age_seconds": 5,
                        "attempted_row_count": 0,
                        "applied_row_count": 0,
                        "fetched_row_count": 7346,
                    },
                },
            }
        )
        responses = []
        for _ in range(6):
            responses.extend(
                [
                    watch_payload,
                    self._result({"counts": {"blocking": 0}}),
                    self._result({"warning_count": 0, "error_count": 0}),
                ]
            )
        with patch.object(tasks, "manage_py", side_effect=responses) as manage_py:
            tasks.sync_autorecover_monitor.body(
                context,
                sync_ids="50",
                max_polls=6,
                interval_seconds=1,
                allow_nonterminal=True,
                failed_status_threshold=2,
            )
        commands = [call.args[1] for call in manage_py.call_args_list]
        self.assertFalse(
            any(
                'forward_execution_run_recovery --sync-name "sample-sync" --enqueue-next'
                in command
                for command in commands
            )
        )


class SyncReleaseGateTaskTest(unittest.TestCase):
    def _context(self):
        context = Mock()
        context.forward_netbox = SimpleNamespace(
            netbox_ver="v4.5.9",
            project_name="forward-netbox",
            compose_dir="/tmp/forward-netbox",
        )
        return context

    def test_sync_release_gate_runs_strict_monitors_and_writes_summary(self):
        context = self._context()
        with tempfile.TemporaryDirectory() as tmp_dir:
            with (
                patch.object(
                    tasks.sync_autorecover_monitor, "body"
                ) as autorecover_body,
                patch.object(tasks.sync_health_monitor, "body") as health_body,
                patch.object(tasks, "_manage_py_json_retry") as manage_json,
                patch.object(tasks, "Path") as path_cls,
            ):
                base_path = Path(tmp_dir)
                path_cls.side_effect = lambda value: (
                    base_path if str(value) == "docs/03_Plans/evidence" else Path(value)
                )
                manage_json.side_effect = [
                    {
                        "warning_count": 0,
                        "suppressed_warning_count": 0,
                        "error_count": 0,
                    },
                    {"counts": {"blocking": 0}},
                    {
                        "warning_count": 0,
                        "suppressed_warning_count": 0,
                        "error_count": 0,
                    },
                    {"counts": {"blocking": 0}},
                ]
                tasks.sync_release_gate.body(
                    context,
                    sync_ids="46,50",
                    max_polls=2,
                    interval_seconds=1,
                    output_prefix="unit-release-gate",
                )

            autorecover_body.assert_called_once()
            health_body.assert_called_once()
            self.assertEqual(manage_json.call_count, 4)
            summary_file = base_path / "unit-release-gate-summary.json"
            self.assertTrue(summary_file.exists())
            payload = json.loads(summary_file.read_text(encoding="utf-8"))
            self.assertEqual(payload["sync_ids"], [46, 50])

    def test_sync_release_gate_fails_on_warning_count(self):
        context = self._context()
        with (
            patch.object(tasks.sync_autorecover_monitor, "body"),
            patch.object(tasks.sync_health_monitor, "body"),
            patch.object(
                tasks,
                "_manage_py_json_retry",
                side_effect=[
                    {
                        "warning_count": 1,
                        "suppressed_warning_count": 0,
                        "error_count": 0,
                    },
                    {"counts": {"blocking": 0}},
                ],
            ),
        ):
            with self.assertRaises(Exit) as raised:
                tasks.sync_release_gate.body(
                    context,
                    sync_ids="46",
                    max_polls=1,
                    interval_seconds=1,
                    output_prefix="unit-release-gate-fail",
                )
        self.assertEqual(raised.exception.code, 3)


class DockerChaosKillTaskTest(unittest.TestCase):
    def setUp(self):
        super().setUp()
        env_patch = patch.dict(
            os.environ,
            {
                "FORWARD_CHAOS_OUTPUT_DIR": "",
                "FORWARD_CHAOS_POLL_SECONDS": "5",
                "FORWARD_CHAOS_SYNC_NAME": "",
                "FORWARD_CHAOS_WAIT_SECONDS": "600",
                "FORWARD_CHAOS_WORKER_REPLICAS": "0",
            },
            clear=False,
        )
        env_patch.start()
        self.addCleanup(env_patch.stop)

    def _context(self):
        context = Mock()
        context.forward_netbox = SimpleNamespace(
            netbox_ver="v4.5.9",
            project_name="forward-netbox",
            compose_dir="/tmp/forward-netbox",
        )
        return context

    def test_requires_explicit_confirm(self):
        with self.assertRaises(Exit) as raised:
            tasks.docker_chaos_kill.body(self._context(), confirm=False)

        self.assertEqual(raised.exception.code, 2)
        self.assertIn("without --confirm=True", str(raised.exception))

    def test_rejects_unknown_scenario(self):
        with self.assertRaises(Exit) as raised:
            tasks.docker_chaos_kill.body(
                self._context(),
                scenario="not-a-real-scenario",
                confirm=True,
            )

        self.assertEqual(raised.exception.code, 2)
        self.assertIn("Unsupported scenario", str(raised.exception))

    def test_kills_first_worker_and_restores_workers(self):
        context = self._context()

        queue_ids = ["worker-a\nworker-b\n"]

        def fake_docker_compose(_context, command, **_kwargs):
            if command == "ps -q netbox-worker":
                return SimpleNamespace(stdout=queue_ids.pop(0))
            return SimpleNamespace(stdout="")

        with (
            patch.object(
                tasks, "docker_compose", side_effect=fake_docker_compose
            ) as compose,
            patch.object(tasks, "_current_worker_replicas", return_value=0),
            patch.object(tasks, "_wait_for_chaos_scenario_ready") as wait_ready,
            patch.object(tasks, "_export_chaos_bundle") as export_bundle,
            patch.dict(os.environ, {}, clear=False),
        ):
            tasks.docker_chaos_kill.body(
                context,
                scenario="stage-after-branch",
                confirm=True,
            )

        context.run.assert_called_once_with("docker kill worker-a")
        wait_ready.assert_not_called()
        export_bundle.assert_not_called()

        compose_calls = [call.args[1] for call in compose.call_args_list]
        self.assertEqual(
            compose_calls,
            [
                "up -d netbox-worker",
                "ps netbox-worker",
                "ps -q netbox-worker",
                "up -d netbox-worker",
                "ps netbox-worker",
            ],
        )

    def test_waits_and_exports_when_sync_name_and_output_dir_are_set(self):
        context = self._context()

        def fake_docker_compose(_context, command, **_kwargs):
            if command == "ps -q netbox-worker":
                return SimpleNamespace(stdout="worker-1\n")
            return SimpleNamespace(stdout="")

        with (
            patch.object(tasks, "docker_compose", side_effect=fake_docker_compose),
            patch.object(tasks, "_current_worker_replicas", return_value=0),
            patch.object(tasks, "_wait_for_chaos_scenario_ready") as wait_ready,
            patch.object(tasks, "_export_chaos_bundle") as export_bundle,
            patch.object(
                tasks,
                "_assert_chaos_bundle_recovery",
                return_value=Path(
                    "/tmp/chaos-bundles/chaos-merge-during-exec-run-1.json"
                ),
            ) as assert_bundle,
            patch.object(tasks, "_write_chaos_kill_metadata") as write_metadata,
            patch.dict(
                os.environ,
                {
                    "FORWARD_CHAOS_SYNC_NAME": "ui-harness-sync",
                    "FORWARD_CHAOS_OUTPUT_DIR": "/tmp/chaos-bundles",
                    "FORWARD_CHAOS_WAIT_SECONDS": "42",
                    "FORWARD_CHAOS_POLL_SECONDS": "3",
                },
                clear=False,
            ),
        ):
            tasks.docker_chaos_kill.body(
                context,
                scenario="merge-during-exec",
                confirm=True,
            )

        wait_ready.assert_called_once_with(
            context,
            sync_name="ui-harness-sync",
            scenario="merge-during-exec",
            timeout_seconds=42,
            poll_seconds=3,
        )
        export_bundle.assert_called_once_with(
            context,
            sync_name="ui-harness-sync",
            scenario="merge-during-exec",
            output_dir="/tmp/chaos-bundles",
        )
        assert_bundle.assert_called_once_with(
            output_dir="/tmp/chaos-bundles",
            scenario="merge-during-exec",
        )
        write_metadata.assert_called_once_with(
            output_dir="/tmp/chaos-bundles",
            scenario="merge-during-exec",
            sync_name="ui-harness-sync",
            killed_worker_id="worker-1",
            restored_worker_replicas=0,
            support_bundle_path=Path(
                "/tmp/chaos-bundles/chaos-merge-during-exec-run-1.json"
            ),
            support_bundle_recovery_verified=True,
        )

    def test_writes_kill_metadata_when_output_dir_is_set_without_sync_name(self):
        context = self._context()

        def fake_docker_compose(_context, command, **_kwargs):
            if command == "ps -q netbox-worker":
                return SimpleNamespace(stdout="worker-9\n")
            return SimpleNamespace(stdout="")

        with tempfile.TemporaryDirectory() as tmp_dir:
            with (
                patch.object(tasks, "docker_compose", side_effect=fake_docker_compose),
                patch.object(tasks, "_current_worker_replicas", return_value=0),
                patch.dict(
                    os.environ,
                    {"FORWARD_CHAOS_OUTPUT_DIR": tmp_dir},
                    clear=False,
                ),
            ):
                tasks.docker_chaos_kill.body(
                    context,
                    scenario="stage-before-branch",
                    confirm=True,
                )

            metadata_files = sorted(
                Path(tmp_dir).glob("chaos-stage-before-branch-metadata-*.json")
            )
            self.assertEqual(len(metadata_files), 1)
            metadata = json.loads(metadata_files[0].read_text(encoding="utf-8"))

        self.assertEqual(metadata["scenario"], "stage-before-branch")
        self.assertEqual(metadata["killed_worker_id"], "worker-9")
        self.assertEqual(metadata["restored_worker_replicas"], 0)
        self.assertFalse(metadata["support_bundle_recovery_verified"])

    def test_fails_when_no_workers_are_present(self):
        context = self._context()

        def fake_docker_compose(_context, command, **_kwargs):
            if command == "ps -q netbox-worker":
                return SimpleNamespace(stdout="")
            return SimpleNamespace(stdout="")

        with patch.object(tasks, "docker_compose", side_effect=fake_docker_compose):
            with self.assertRaises(Exit) as raised:
                tasks.docker_chaos_kill.body(
                    context,
                    scenario="stage-before-branch",
                    confirm=True,
                )

        self.assertEqual(raised.exception.code, 1)
        self.assertIn("No netbox-worker containers found", str(raised.exception))


class ChaosProbeHelperTest(unittest.TestCase):
    def test_is_chaos_scenario_ready_uses_probe_signal(self):
        with patch.object(
            tasks,
            "manage_py",
            return_value=SimpleNamespace(stdout="readiness=1"),
        ):
            self.assertTrue(
                tasks._is_chaos_scenario_ready(
                    Mock(), sync_name="ui-harness-sync", scenario="stage-after-branch"
                )
            )

        with patch.object(
            tasks,
            "manage_py",
            return_value=SimpleNamespace(stdout="readiness=0"),
        ):
            self.assertFalse(
                tasks._is_chaos_scenario_ready(
                    Mock(), sync_name="ui-harness-sync", scenario="stage-after-branch"
                )
            )

    def test_container_export_dir_maps_repo_paths_to_source_mount(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            repo_root = Path(tmp_dir)
            output_dir = repo_root / "docs/03_Plans/evidence/chaos"

            mapped = tasks._container_export_dir(output_dir, repo_root=repo_root)

        self.assertEqual(mapped, "/source/docs/03_Plans/evidence/chaos")
        self.assertEqual(
            tasks._container_export_dir("docs/03_Plans/evidence/chaos"),
            "/source/docs/03_Plans/evidence/chaos",
        )

    def test_export_chaos_bundle_uses_container_mount_path(self):
        context = Mock()
        commands = []

        def fake_manage_py(_context, command):
            commands.append(command)
            return SimpleNamespace(stdout="")

        with patch.object(tasks, "manage_py", side_effect=fake_manage_py):
            result = tasks._export_chaos_bundle(
                context,
                sync_name="ui-harness-sync",
                scenario="stage-after-branch",
                output_dir="docs/03_Plans/evidence/chaos-test-nonexistent",
            )

        self.assertIsNone(result)
        self.assertIn(
            '--export-dir "/source/docs/03_Plans/evidence/chaos-test-nonexistent"',
            commands[0],
        )

    def test_assert_chaos_bundle_recovery_accepts_valid_bundle(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "chaos-stage-after-branch-run-10.json"
            path.write_text(
                (
                    "{\n"
                    '  "run": {"id": 10},\n'
                    '  "steps": [{"index": 1, "kind": "stage", "status": "running", "branch_name": "branch-1"}],\n'
                    '  "recovery_recommendation": {"action": "reconcile"}\n'
                    "}\n"
                ),
                encoding="utf-8",
            )
            result = tasks._assert_chaos_bundle_recovery(
                output_dir=tmp_dir,
                scenario="stage-after-branch",
            )
            self.assertEqual(result, path)

    def test_assert_chaos_bundle_recovery_rejects_missing_recommendation_action(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "chaos-stage-after-branch-run-11.json"
            path.write_text(
                (
                    "{\n"
                    '  "run": {"id": 11},\n'
                    '  "steps": [{"index": 1, "kind": "stage", "status": "running", "branch_name": "branch-1"}],\n'
                    '  "recovery_recommendation": {"action": "unsupported"}\n'
                    "}\n"
                ),
                encoding="utf-8",
            )
            with self.assertRaises(Exit) as raised:
                tasks._assert_chaos_bundle_recovery(
                    output_dir=tmp_dir,
                    scenario="stage-after-branch",
                )
            self.assertEqual(raised.exception.code, 1)
            self.assertIn("unsupported recovery action", str(raised.exception))

    def test_assert_chaos_bundle_recovery_rejects_action_mismatch_for_scenario(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "chaos-merge-during-exec-run-12.json"
            path.write_text(
                (
                    "{\n"
                    '  "run": {"id": 12},\n'
                    '  "steps": [{"index": 1, "kind": "stage", "status": "merge_queued", "merge_job": 99}],\n'
                    '  "recovery_recommendation": {"action": "discard_branch_retry"}\n'
                    "}\n"
                ),
                encoding="utf-8",
            )
            with self.assertRaises(Exit) as raised:
                tasks._assert_chaos_bundle_recovery(
                    output_dir=tmp_dir,
                    scenario="merge-during-exec",
                )
            self.assertEqual(raised.exception.code, 1)
            self.assertIn("does not match scenario", str(raised.exception))

    def test_assert_chaos_bundle_recovery_rejects_scenario_state_mismatch(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "chaos-stage-after-branch-run-13.json"
            path.write_text(
                (
                    "{\n"
                    '  "run": {"id": 13},\n'
                    '  "steps": [{"index": 1, "kind": "stage", "status": "running"}],\n'
                    '  "recovery_recommendation": {"action": "reconcile"}\n'
                    "}\n"
                ),
                encoding="utf-8",
            )
            with self.assertRaises(Exit) as raised:
                tasks._assert_chaos_bundle_recovery(
                    output_dir=tmp_dir,
                    scenario="stage-after-branch",
                )
            self.assertEqual(raised.exception.code, 1)
            self.assertIn("branch linkage", str(raised.exception))

    def test_assert_chaos_bundle_recovery_accepts_valid_merge_scenario_bundle(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "chaos-merge-during-exec-run-14.json"
            path.write_text(
                (
                    "{\n"
                    '  "run": {"id": 14},\n'
                    '  "steps": [{"index": 2, "kind": "stage", "status": "merge_timeout", "merge_job": 199}],\n'
                    '  "recovery_recommendation": {"action": "requeue_merge"}\n'
                    "}\n"
                ),
                encoding="utf-8",
            )
            result = tasks._assert_chaos_bundle_recovery(
                output_dir=tmp_dir,
                scenario="merge-during-exec",
            )
            self.assertEqual(result, path)

    def test_write_chaos_kill_metadata_records_bundle_run_step_and_recovery(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            bundle_path = Path(tmp_dir) / "chaos-stage-after-branch-run-21.json"
            bundle_path.write_text(
                (
                    "{\n"
                    '  "run": {"id": 21},\n'
                    '  "steps": [{"id": 33, "index": 1, "kind": "stage", "status": "running", "branch": 44, "branch_name": "branch-44", "job": 55}],\n'
                    '  "recovery_recommendation": {"action": "reconcile", "severity": "warning", "step_index": 1}\n'
                    "}\n"
                ),
                encoding="utf-8",
            )

            metadata_path = tasks._write_chaos_kill_metadata(
                output_dir=tmp_dir,
                scenario="stage-after-branch",
                sync_name="ui-harness-sync",
                killed_worker_id="worker-21",
                restored_worker_replicas=4,
                support_bundle_path=bundle_path,
                support_bundle_recovery_verified=True,
            )

            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))

        self.assertEqual(metadata["execution_run_id"], 21)
        self.assertEqual(metadata["active_step_id"], 33)
        self.assertEqual(metadata["active_step_job_id"], 55)
        self.assertEqual(metadata["branch_id"], 44)
        self.assertEqual(metadata["recovery_action"], "reconcile")
        self.assertTrue(metadata["support_bundle_recovery_verified"])


class ScaleBenchmarkTaskTest(unittest.TestCase):
    def _context(self):
        context = Mock()
        context.forward_netbox = SimpleNamespace(
            netbox_ver="v4.5.9",
            project_name="forward-netbox",
            compose_dir="/tmp/forward-netbox",
        )
        return context

    def test_scale_benchmark_requires_exactly_one_selector(self):
        with self.assertRaises(Exit) as raised:
            tasks.scale_benchmark.body(self._context())

        self.assertEqual(raised.exception.code, 2)
        self.assertIn("exactly one", str(raised.exception))

    def test_scale_benchmark_uses_sync_name_and_output_path(self):
        context = self._context()
        with patch.object(tasks, "manage_py") as manage_py:
            tasks.scale_benchmark.body(
                context,
                sync_name="ui-harness-sync",
                output_json="docs/03_Plans/evidence/scale.json",
                reconcile=True,
                fail_on_warn=True,
            )

        manage_py.assert_called_once()
        command = manage_py.call_args.args[1]
        self.assertIn("forward_scale_benchmark", command)
        self.assertIn('--sync-name "ui-harness-sync"', command)
        self.assertIn('--output-json "docs/03_Plans/evidence/scale.json"', command)
        self.assertIn("--reconcile", command)
        self.assertIn("--fail-on-warn", command)


class ArchitectureRuntimeEvidenceTaskTest(unittest.TestCase):
    def setUp(self):
        super().setUp()
        collector_patch = patch.object(
            tasks,
            "_collect_destructive_runtime_evidence",
            return_value={
                "status": "passed",
                "evidence": {
                    "scenarios": [
                        {
                            "scenario": "stage-before-branch",
                            "ok": True,
                            "bundle": "docs/03_Plans/evidence/chaos/chaos-stage-before-branch-run-1.json",
                            "metadata": "docs/03_Plans/evidence/chaos/chaos-stage-before-branch-metadata-unit.json",
                            "support_bundle_recovery_verified": True,
                        }
                    ],
                    "output_dir": "docs/03_Plans/evidence/chaos",
                },
            },
        )
        collector_patch.start()
        self.addCleanup(collector_patch.stop)
        preflight_patch = patch.object(
            tasks,
            "_field_scale_runtime_preflight",
            return_value={"ok": True},
        )
        preflight_patch.start()
        self.addCleanup(preflight_patch.stop)

    def _context(self):
        context = Mock()
        context.forward_netbox = SimpleNamespace(
            netbox_ver="v4.5.9",
            project_name="forward-netbox",
            compose_dir="/tmp/forward-netbox",
        )
        return context

    def _capacity_review(self):
        return {
            "status": "passed",
            "evidence": {
                "workers": {"current": 4, "recommended": 4, "status": "pass"},
                "scheduler_overlap_capacity_review": {"status": "pass"},
            },
        }

    def test_runtime_evidence_writes_manifest(self):
        context = self._context()
        with tempfile.TemporaryDirectory():
            output_rel = "docs/03_Plans/evidence/runtime-evidence-unit.json"
            repo_root = Path(tasks.__file__).resolve().parent
            output_abs = repo_root / output_rel
            chaos_dir = repo_root / "docs/03_Plans/evidence/chaos"
            chaos_dir.mkdir(parents=True, exist_ok=True)
            (chaos_dir / "chaos-stage-before-branch-run-1.json").write_text(
                "{}\n", encoding="utf-8"
            )

            run_results = [
                SimpleNamespace(exited=0, ok=True),
                SimpleNamespace(exited=0, ok=True),
                SimpleNamespace(exited=0, ok=True),
                SimpleNamespace(exited=0, ok=True),
            ]
            with (
                patch.object(tasks, "manage_py") as manage_py,
                patch.object(tasks, "docker_compose") as docker_compose,
                patch.object(
                    tasks,
                    "_collect_runtime_capacity_review",
                    return_value=self._capacity_review(),
                ),
                patch.object(context, "run", side_effect=run_results),
            ):
                tasks.architecture_runtime_evidence.body(
                    context,
                    output_path=output_rel,
                    sync_name="ui-harness-sync",
                )

            docker_compose.assert_called_once()
            self.assertGreaterEqual(manage_py.call_count, 1)
            self.assertTrue(output_abs.exists())
            payload = output_abs.read_text(encoding="utf-8")
            self.assertIn("destructive_runtime_worker_kill_evidence_verified", payload)
            self.assertIn("field_scale_runtime_matrix_verified", payload)
            self.assertIn("compatibility_cache_retirement_verified", payload)
            self.assertIn("runtime_fallback_reduction_verified", payload)
            self.assertIn("scheduler_overlap_readiness_verified", payload)
            output_abs.unlink(missing_ok=True)

    def test_runtime_evidence_can_skip_chaos_and_reuse_fresh_evidence(self):
        context = self._context()
        output_rel = "docs/03_Plans/evidence/runtime-evidence-skip-chaos-test.json"
        repo_root = Path(tasks.__file__).resolve().parent
        output_abs = repo_root / output_rel
        report_abs = repo_root / "docs/03_Plans/evidence/scale-runtime-evidence.json"
        original_report = (
            report_abs.read_text(encoding="utf-8") if report_abs.exists() else None
        )
        output_abs.parent.mkdir(parents=True, exist_ok=True)
        output_abs.write_text(
            json.dumps(
                {
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                    "checks": {
                        "destructive_runtime_worker_kill_evidence_verified": {
                            "status": "passed",
                            "evidence": {
                                "scenarios": [{"scenario": "stage-before-branch"}],
                                "output_dir": "docs/03_Plans/evidence/chaos",
                            },
                        }
                    },
                }
            )
            + "\n",
            encoding="utf-8",
        )

        def fake_manage_py(_context, command, *args, **kwargs):
            self.assertNotIn("forward_seed_ui_harness", command)
            if "forward_scale_benchmark" in command:
                report_abs.parent.mkdir(parents=True, exist_ok=True)
                report_abs.write_text(
                    json.dumps(
                        {
                            "status": "pass",
                            "summary": {"step_count": 4},
                            "checks": [
                                {"code": "support_bundle_shape", "status": "pass"},
                                {"code": "run_completion", "status": "pass"},
                                {"code": "row_failures", "status": "pass"},
                                {"code": "pushdown_efficiency", "status": "pass"},
                                {"code": "pushdown_runtime", "status": "pass"},
                                {"code": "partition_retry_pressure", "status": "pass"},
                                {
                                    "code": "throughput_smoothing",
                                    "status": "pass",
                                    "evidence": {
                                        "scheduler_overlap_readiness": {
                                            "status": "not_warranted"
                                        }
                                    },
                                },
                            ],
                        }
                    ),
                    encoding="utf-8",
                )
            return SimpleNamespace(exited=0, ok=True)

        try:
            with (
                patch.object(tasks, "manage_py", side_effect=fake_manage_py),
                patch.object(tasks, "docker_compose") as docker_compose,
                patch.object(
                    tasks,
                    "_collect_runtime_capacity_review",
                    return_value=self._capacity_review(),
                ),
            ):
                tasks.architecture_runtime_evidence.body(
                    context,
                    output_path=output_rel,
                    sync_name="ui-harness-sync",
                    skip_chaos=True,
                )
            payload = json.loads(output_abs.read_text(encoding="utf-8"))
        finally:
            output_abs.unlink(missing_ok=True)
            if original_report is None:
                report_abs.unlink(missing_ok=True)
            else:
                report_abs.write_text(original_report, encoding="utf-8")

        docker_compose.assert_not_called()
        context.run.assert_not_called()
        self.assertTrue(payload["notes"]["skip_chaos"])
        chaos = payload["checks"]["destructive_runtime_worker_kill_evidence_verified"]
        self.assertEqual(chaos["status"], "passed")
        self.assertIn("reused_from_generated_at", chaos["evidence"])

    def test_runtime_evidence_uses_explicit_scale_sync_name(self):
        context = self._context()
        output_rel = "docs/03_Plans/evidence/runtime-evidence-scale-sync-test.json"
        repo_root = Path(tasks.__file__).resolve().parent
        output_abs = repo_root / output_rel
        report_rel = "docs/03_Plans/evidence/scale-runtime-evidence.json"
        report_abs = repo_root / report_rel
        original_report = (
            report_abs.read_text(encoding="utf-8") if report_abs.exists() else None
        )
        run_results = [
            SimpleNamespace(exited=0, ok=True),
            SimpleNamespace(exited=0, ok=True),
            SimpleNamespace(exited=0, ok=True),
            SimpleNamespace(exited=0, ok=True),
        ]
        scale_calls = []

        def fake_manage_py(_context, command, *args, **kwargs):
            if "forward_scale_benchmark" in command:
                scale_calls.append(command)
                report_abs.parent.mkdir(parents=True, exist_ok=True)
                report_abs.write_text(
                    json.dumps(
                        {
                            "status": "pass",
                            "summary": {"step_count": 4},
                            "checks": [
                                {"code": "support_bundle_shape", "status": "pass"},
                                {"code": "run_completion", "status": "pass"},
                                {"code": "row_failures", "status": "pass"},
                                {"code": "pushdown_efficiency", "status": "pass"},
                                {"code": "pushdown_runtime", "status": "pass"},
                                {
                                    "code": "partition_retry_pressure",
                                    "status": "pass",
                                },
                                {
                                    "code": "throughput_smoothing",
                                    "status": "pass",
                                    "evidence": {
                                        "scheduler_overlap_readiness": {
                                            "status": "not_warranted"
                                        }
                                    },
                                },
                            ],
                        }
                    )
                    + "\n",
                    encoding="utf-8",
                )
            return SimpleNamespace(exited=0, ok=True)

        try:
            with (
                patch.object(tasks, "manage_py", side_effect=fake_manage_py),
                patch.object(tasks, "docker_compose"),
                patch.object(
                    tasks,
                    "_collect_runtime_capacity_review",
                    return_value=self._capacity_review(),
                ),
                patch.object(context, "run", side_effect=run_results),
            ):
                tasks.architecture_runtime_evidence.body(
                    context,
                    output_path=output_rel,
                    sync_name="ui-harness-sync",
                    scale_sync_name="field-scale-sync",
                )
        finally:
            if original_report is None:
                report_abs.unlink(missing_ok=True)
            else:
                report_abs.write_text(original_report, encoding="utf-8")

        self.assertTrue(scale_calls)
        self.assertIn('--sync-name "field-scale-sync"', scale_calls[0])
        output_abs.unlink(missing_ok=True)

    def test_runtime_evidence_uses_scale_input_json_selector(self):
        context = self._context()
        repo_root = Path(tasks.__file__).resolve().parent
        report_rel = "docs/03_Plans/evidence/scale-runtime-evidence.json"
        report_abs = repo_root / report_rel
        original_report = (
            report_abs.read_text(encoding="utf-8") if report_abs.exists() else None
        )
        scale_calls = []

        def fake_manage_py(_context, command, *args, **kwargs):
            if "forward_scale_benchmark" in command:
                scale_calls.append(command)
                report_abs.parent.mkdir(parents=True, exist_ok=True)
                report_abs.write_text(
                    json.dumps(
                        {
                            "status": "pass",
                            "summary": {"step_count": 4},
                            "checks": [
                                {"code": "support_bundle_shape", "status": "pass"},
                                {"code": "run_completion", "status": "pass"},
                                {"code": "row_failures", "status": "pass"},
                                {"code": "pushdown_efficiency", "status": "pass"},
                                {"code": "pushdown_runtime", "status": "pass"},
                                {
                                    "code": "partition_retry_pressure",
                                    "status": "pass",
                                },
                                {
                                    "code": "throughput_smoothing",
                                    "status": "pass",
                                    "evidence": {
                                        "scheduler_overlap_readiness": {
                                            "status": "not_warranted"
                                        }
                                    },
                                },
                            ],
                        }
                    )
                    + "\n",
                    encoding="utf-8",
                )
            return SimpleNamespace(exited=0, ok=True)

        try:
            with patch.object(tasks, "manage_py", side_effect=fake_manage_py):
                evidence = tasks._collect_scale_runtime_evidence(
                    context=context,
                    repo_root=repo_root,
                    input_json="/tmp/support-bundle.json",
                )
        finally:
            if original_report is None:
                report_abs.unlink(missing_ok=True)
            else:
                report_abs.write_text(original_report, encoding="utf-8")

        self.assertTrue(scale_calls)
        self.assertIn('--input-json "/tmp/support-bundle.json"', scale_calls[0])
        self.assertNotIn("--sync-name", scale_calls[0])
        self.assertEqual(
            evidence["runtime_fallback_reduction_verified"]["status"],
            "passed",
        )

    def test_runtime_evidence_can_reconcile_scale_run_selector(self):
        context = self._context()
        output_rel = "docs/03_Plans/evidence/runtime-evidence-reconcile-test.json"
        repo_root = Path(tasks.__file__).resolve().parent
        output_abs = repo_root / output_rel
        report_rel = "docs/03_Plans/evidence/scale-runtime-evidence.json"
        report_abs = repo_root / report_rel
        original_report = (
            report_abs.read_text(encoding="utf-8") if report_abs.exists() else None
        )
        scale_calls = []

        def fake_manage_py(_context, command, *args, **kwargs):
            if "forward_scale_benchmark" in command:
                scale_calls.append(command)
                report_abs.parent.mkdir(parents=True, exist_ok=True)
                report_abs.write_text(
                    json.dumps(
                        {
                            "status": "pass",
                            "summary": {"step_count": 4},
                            "checks": [
                                {"code": "support_bundle_shape", "status": "pass"},
                                {"code": "run_completion", "status": "pass"},
                                {"code": "row_failures", "status": "pass"},
                                {"code": "pushdown_efficiency", "status": "pass"},
                                {"code": "pushdown_runtime", "status": "pass"},
                                {
                                    "code": "partition_retry_pressure",
                                    "status": "pass",
                                },
                                {
                                    "code": "throughput_smoothing",
                                    "status": "pass",
                                    "evidence": {
                                        "scheduler_overlap_readiness": {
                                            "status": "not_warranted"
                                        }
                                    },
                                },
                            ],
                        }
                    )
                    + "\n",
                    encoding="utf-8",
                )
            return SimpleNamespace(exited=0, ok=True)

        run_results = [SimpleNamespace(exited=0, ok=True) for _ in range(4)]
        try:
            with (
                patch.object(tasks, "manage_py", side_effect=fake_manage_py),
                patch.object(tasks, "docker_compose"),
                patch.object(
                    tasks,
                    "_collect_runtime_capacity_review",
                    return_value=self._capacity_review(),
                ),
                patch.object(context, "run", side_effect=run_results),
            ):
                tasks.architecture_runtime_evidence.body(
                    context,
                    output_path=output_rel,
                    sync_name="ui-harness-sync",
                    scale_run_id="123",
                    scale_reconcile=True,
                )
        finally:
            if original_report is None:
                report_abs.unlink(missing_ok=True)
            else:
                report_abs.write_text(original_report, encoding="utf-8")

        self.assertTrue(scale_calls)
        self.assertIn("--run-id 123", scale_calls[0])
        self.assertIn("--reconcile", scale_calls[0])
        payload = json.loads(output_abs.read_text(encoding="utf-8"))
        self.assertTrue(payload["notes"]["scale_reconcile"])
        output_abs.unlink(missing_ok=True)

    def test_collect_scale_runtime_evidence_uses_benchmark_report(self):
        context = self._context()
        with tempfile.TemporaryDirectory() as tmp_dir:
            repo_root = Path(tmp_dir)
            report_rel = "docs/03_Plans/evidence/scale-runtime-evidence.json"
            report_abs = repo_root / report_rel
            report_abs.parent.mkdir(parents=True, exist_ok=True)
            report_abs.write_text(
                json.dumps(
                    {
                        "status": "pass",
                        "summary": {"step_count": 4},
                        "checks": [
                            {
                                "code": "support_bundle_shape",
                                "status": "pass",
                            },
                            {
                                "code": "run_completion",
                                "status": "pass",
                            },
                            {
                                "code": "row_failures",
                                "status": "pass",
                            },
                            {
                                "code": "pushdown_efficiency",
                                "status": "pass",
                                "evidence": {"fallback_steps": 0},
                            },
                            {
                                "code": "pushdown_runtime",
                                "status": "pass",
                                "evidence": {"fallback_runtime_share": 0.0},
                            },
                            {
                                "code": "partition_retry_pressure",
                                "status": "pass",
                                "evidence": {"retry_count": 0},
                            },
                            {
                                "code": "throughput_smoothing",
                                "status": "pass",
                                "evidence": {
                                    "scheduler_overlap_readiness": {
                                        "status": "not_warranted"
                                    }
                                },
                            },
                        ],
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            with patch.object(tasks, "manage_py"):
                evidence = tasks._collect_scale_runtime_evidence(
                    context=context,
                    repo_root=repo_root,
                    sync_name="ui-harness-sync",
                )

            self.assertEqual(
                evidence["runtime_fallback_reduction_verified"]["status"],
                "passed",
            )
            self.assertEqual(
                evidence["scheduler_overlap_readiness_verified"]["status"],
                "passed",
            )

    def test_collect_scale_runtime_evidence_accepts_scheduler_candidate_with_capacity_review(
        self,
    ):
        context = self._context()
        with tempfile.TemporaryDirectory() as tmp_dir:
            repo_root = Path(tmp_dir)
            report_rel = "docs/03_Plans/evidence/scale-runtime-evidence.json"
            report_abs = repo_root / report_rel
            report_abs.parent.mkdir(parents=True, exist_ok=True)
            report_abs.write_text(
                json.dumps(
                    {
                        "status": "warn",
                        "summary": {"step_count": 4},
                        "checks": [
                            {"code": "support_bundle_shape", "status": "pass"},
                            {"code": "run_completion", "status": "pass"},
                            {"code": "row_failures", "status": "pass"},
                            {"code": "pushdown_efficiency", "status": "pass"},
                            {"code": "pushdown_runtime", "status": "pass"},
                            {"code": "partition_retry_pressure", "status": "pass"},
                            {
                                "code": "throughput_smoothing",
                                "status": "warn",
                                "evidence": {
                                    "scheduler_overlap_readiness": {
                                        "status": "candidate"
                                    }
                                },
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )

            with patch.object(tasks, "manage_py"):
                evidence = tasks._collect_scale_runtime_evidence(
                    context=context,
                    repo_root=repo_root,
                    sync_name="ui-harness-sync",
                    capacity_review=self._capacity_review(),
                )

        self.assertEqual(
            evidence["scheduler_overlap_readiness_verified"]["status"],
            "passed",
        )
        self.assertEqual(
            evidence["scheduler_overlap_readiness_verified"]["evidence"][
                "capacity_review"
            ]["status"],
            "passed",
        )

    def test_collect_scale_runtime_evidence_accepts_capacity_blocked_readiness_with_capacity_review(
        self,
    ):
        context = self._context()
        with tempfile.TemporaryDirectory() as tmp_dir:
            repo_root = Path(tmp_dir)
            report_rel = "docs/03_Plans/evidence/scale-runtime-evidence.json"
            report_abs = repo_root / report_rel
            report_abs.parent.mkdir(parents=True, exist_ok=True)
            report_abs.write_text(
                json.dumps(
                    {
                        "status": "warn",
                        "summary": {"step_count": 4},
                        "checks": [
                            {"code": "support_bundle_shape", "status": "pass"},
                            {"code": "run_completion", "status": "pass"},
                            {"code": "row_failures", "status": "pass"},
                            {"code": "pushdown_efficiency", "status": "pass"},
                            {"code": "pushdown_runtime", "status": "pass"},
                            {"code": "partition_retry_pressure", "status": "pass"},
                            {
                                "code": "throughput_smoothing",
                                "status": "warn",
                                "evidence": {
                                    "scheduler_overlap_readiness": {
                                        "status": "blocked",
                                        "blocking_reasons": [
                                            "capacity_evidence_missing"
                                        ],
                                    }
                                },
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )

            with patch.object(tasks, "manage_py"):
                evidence = tasks._collect_scale_runtime_evidence(
                    context=context,
                    repo_root=repo_root,
                    sync_name="ui-harness-sync",
                    capacity_review=self._capacity_review(),
                )

        self.assertEqual(
            evidence["scheduler_overlap_readiness_verified"]["status"],
            "passed",
        )

    def test_run_field_scale_runtime_matrix_allows_existing_source_without_secret_env(
        self,
    ):
        context = self._context()
        run_results = [
            SimpleNamespace(ok=True, exited=0),
            SimpleNamespace(ok=True, exited=0),
            SimpleNamespace(ok=True, exited=0),
        ]
        with tempfile.TemporaryDirectory() as tmp_dir:
            artifact_path = Path(tmp_dir) / "field-scale.json"
            with (
                patch.dict(
                    os.environ,
                    {
                        "FORWARD_SMOKE_SOURCE_NAME": "smoke-source-release-smoke-20260601",
                        "FORWARD_SMOKE_SYNC_NAME": "smoke-sync-release-smoke-20260601",
                        "FORWARD_SMOKE_DATASET_LABEL": "release-smoke",
                        "FORWARD_SMOKE_MAX_CHANGES_PER_BRANCH": "42",
                        "FORWARD_FIELD_SCALE_EVIDENCE_PATH": str(artifact_path),
                    },
                    clear=True,
                ),
                patch.object(
                    tasks, "_field_scale_manage_py", side_effect=run_results
                ) as manage_py,
            ):
                evidence, status = tasks._run_field_scale_runtime_matrix(context)
            payload = json.loads(artifact_path.read_text(encoding="utf-8"))

        self.assertEqual(status, "completed")
        self.assertEqual(evidence["status"], "passed")
        self.assertEqual(payload["status"], "passed")
        self.assertEqual(payload["metadata"]["dataset_label"], "release-smoke")
        self.assertEqual(payload["metadata"]["max_changes_per_branch"], 42)
        commands = [call.args[1] for call in manage_py.call_args_list]
        self.assertTrue(
            any("--max-changes-per-branch 42" in command for command in commands)
        )
        self.assertTrue(all("--source-name" not in command for command in commands))
        self.assertTrue(all("--username" not in command for command in commands))
        self.assertTrue(all("--password" not in command for command in commands))
        self.assertTrue(all("--network-id" not in command for command in commands))

    def test_run_field_scale_runtime_matrix_collects_sanitized_run_results(self):
        context = self._context()
        run_results = [
            SimpleNamespace(ok=True, exited=0),
            SimpleNamespace(ok=True, exited=0),
            SimpleNamespace(ok=True, exited=0),
        ]
        with tempfile.TemporaryDirectory() as tmp_dir:
            artifact_path = Path(tmp_dir) / "field-scale.json"
            with (
                patch.dict(
                    os.environ,
                    {
                        "FORWARD_SMOKE_USERNAME": "user",
                        "FORWARD_SMOKE_PASSWORD": "secret",
                        "FORWARD_SMOKE_NETWORK_ID": "123",
                        "FORWARD_SMOKE_MODELS": "dcim.site",
                        "FORWARD_SMOKE_QUERY_LIMIT": "3",
                        "FORWARD_SMOKE_STEP_TIMEOUT_SECONDS": "7",
                        "FORWARD_FIELD_SCALE_EVIDENCE_PATH": str(artifact_path),
                    },
                    clear=True,
                ),
                patch.object(
                    tasks, "_field_scale_manage_py", side_effect=run_results
                ) as manage_py,
            ):
                evidence, status = tasks._run_field_scale_runtime_matrix(context)
            payload = json.loads(artifact_path.read_text(encoding="utf-8"))
            artifact_text = artifact_path.read_text(encoding="utf-8")

        self.assertEqual(status, "completed")
        self.assertEqual(evidence["status"], "passed")
        self.assertEqual(len(evidence["evidence"]["runs"]), 3)
        self.assertNotIn("secret", str(evidence))
        self.assertNotIn("secret", artifact_text)
        self.assertEqual(payload["status"], "passed")
        self.assertEqual(payload["metadata"]["dataset_label"], "")
        self.assertEqual(payload["metadata"]["models"], "dcim.site")
        self.assertEqual(len(payload["runs"]), 3)
        commands = [call.args[1] for call in manage_py.call_args_list]
        self.assertIn("--query-limit 3", commands[0])
        self.assertTrue(
            all("--models dcim.site" in command for command in commands[:2])
        )
        self.assertIn("--models dcim.device,dcim.inventoryitem", commands[2])
        self.assertTrue(
            all(call.kwargs.get("timeout") == 7 for call in manage_py.call_args_list)
        )

    def test_run_field_scale_runtime_matrix_records_dataset_label(self):
        context = self._context()
        run_results = [
            SimpleNamespace(ok=True, exited=0),
            SimpleNamespace(ok=True, exited=0),
            SimpleNamespace(ok=True, exited=0),
        ]
        with tempfile.TemporaryDirectory() as tmp_dir:
            artifact_path = Path(tmp_dir) / "field-scale.json"
            with (
                patch.dict(
                    os.environ,
                    {
                        "FORWARD_SMOKE_USERNAME": "user",
                        "FORWARD_SMOKE_PASSWORD": "secret",
                        "FORWARD_SMOKE_NETWORK_ID": "123",
                        "FORWARD_SMOKE_DATASET_LABEL": "release-smoke-prod",
                        "FORWARD_FIELD_SCALE_EVIDENCE_PATH": str(artifact_path),
                    },
                    clear=True,
                ),
                patch.object(tasks, "_field_scale_manage_py", side_effect=run_results),
            ):
                evidence, status = tasks._run_field_scale_runtime_matrix(context)
            payload = json.loads(artifact_path.read_text(encoding="utf-8"))

        self.assertEqual(status, "completed")
        self.assertEqual(evidence["status"], "passed")
        self.assertEqual(payload["metadata"]["dataset_label"], "release-smoke-prod")

    def test_run_field_scale_runtime_matrix_records_step_timeout(self):
        context = self._context()
        timeout_result = SimpleNamespace(command="forward_smoke_sync")
        run_results = [
            CommandTimedOut(timeout_result, timeout=1),
            SimpleNamespace(ok=True, exited=0),
            SimpleNamespace(ok=True, exited=0),
        ]

        def fake_manage_py(*_args, **_kwargs):
            result = run_results.pop(0)
            if isinstance(result, Exception):
                raise result
            return result

        with tempfile.TemporaryDirectory() as tmp_dir:
            artifact_path = Path(tmp_dir) / "field-scale.json"
            with (
                patch.dict(
                    os.environ,
                    {
                        "FORWARD_SMOKE_USERNAME": "user",
                        "FORWARD_SMOKE_PASSWORD": "secret",
                        "FORWARD_SMOKE_NETWORK_ID": "123",
                        "FORWARD_SMOKE_STEP_TIMEOUT_SECONDS": "1",
                        "FORWARD_FIELD_SCALE_EVIDENCE_PATH": str(artifact_path),
                    },
                    clear=True,
                ),
                patch.object(
                    tasks, "_field_scale_manage_py", side_effect=fake_manage_py
                ),
            ):
                evidence, status = tasks._run_field_scale_runtime_matrix(context)
            payload = json.loads(artifact_path.read_text(encoding="utf-8"))

        self.assertEqual(status, "failed")
        self.assertEqual(evidence["status"], "failed")
        self.assertTrue(evidence["evidence"]["runs"][0]["timed_out"])
        self.assertEqual(evidence["evidence"]["runs"][0]["timeout_seconds"], 1)
        self.assertEqual(
            evidence["evidence"]["runs"][0]["failure_code"], "step_timeout"
        )
        self.assertEqual(payload["status"], "failed")
        self.assertTrue(payload["runs"][0]["timed_out"])
        self.assertEqual(payload["runs"][0]["failure_code"], "step_timeout")

    def test_run_field_scale_runtime_matrix_classifies_docker_api_failure(self):
        context = self._context()
        run_results = [
            SimpleNamespace(
                ok=False,
                exited=1,
                stderr=(
                    "permission denied while trying to connect to the docker API "
                    "at unix:///var/run/docker.sock"
                ),
                stdout="",
            ),
            SimpleNamespace(ok=True, exited=0, stderr="", stdout=""),
            SimpleNamespace(ok=True, exited=0, stderr="", stdout=""),
        ]
        with tempfile.TemporaryDirectory() as tmp_dir:
            artifact_path = Path(tmp_dir) / "field-scale.json"
            with (
                patch.dict(
                    os.environ,
                    {
                        "FORWARD_SMOKE_USERNAME": "user",
                        "FORWARD_SMOKE_PASSWORD": "secret",
                        "FORWARD_SMOKE_NETWORK_ID": "123",
                        "FORWARD_FIELD_SCALE_EVIDENCE_PATH": str(artifact_path),
                    },
                    clear=True,
                ),
                patch.object(tasks, "_field_scale_manage_py", side_effect=run_results),
            ):
                evidence, status = tasks._run_field_scale_runtime_matrix(context)
            payload = json.loads(artifact_path.read_text(encoding="utf-8"))

        self.assertEqual(status, "failed")
        self.assertEqual(evidence["status"], "failed")
        self.assertEqual(
            evidence["evidence"]["runs"][0]["failure_code"], "docker_api_unreachable"
        )
        self.assertEqual(payload["runs"][0]["failure_code"], "docker_api_unreachable")

    def test_run_field_scale_runtime_matrix_fails_fast_on_preflight(self):
        context = self._context()
        with tempfile.TemporaryDirectory() as tmp_dir:
            artifact_path = Path(tmp_dir) / "field-scale.json"
            with (
                patch.dict(
                    os.environ,
                    {
                        "FORWARD_SMOKE_USERNAME": "user",
                        "FORWARD_SMOKE_PASSWORD": "secret",
                        "FORWARD_SMOKE_NETWORK_ID": "123",
                        "FORWARD_FIELD_SCALE_EVIDENCE_PATH": str(artifact_path),
                    },
                    clear=True,
                ),
                patch.object(
                    tasks,
                    "_field_scale_runtime_preflight",
                    return_value={
                        "ok": False,
                        "exit_code": 1,
                        "failure_code": "docker_api_unreachable",
                        "failure_hint": "cannot connect to local Docker API",
                    },
                ),
                patch.object(tasks, "_field_scale_manage_py") as manage_py,
            ):
                evidence, status = tasks._run_field_scale_runtime_matrix(context)
            payload = json.loads(artifact_path.read_text(encoding="utf-8"))

        self.assertEqual(status, "failed")
        self.assertEqual(evidence["status"], "failed")
        self.assertEqual(
            evidence["evidence"]["preflight_failure_code"], "docker_api_unreachable"
        )
        self.assertEqual(len(evidence["evidence"]["runs"]), 3)
        self.assertTrue(all(not run["ok"] for run in evidence["evidence"]["runs"]))
        self.assertEqual(
            payload["metadata"]["preflight_failure_code"], "docker_api_unreachable"
        )
        manage_py.assert_not_called()

    def test_run_field_scale_runtime_matrix_resumes_successful_steps(self):
        context = self._context()
        run_results = [
            SimpleNamespace(ok=True, exited=0),
            SimpleNamespace(ok=True, exited=0),
        ]
        with tempfile.TemporaryDirectory() as tmp_dir:
            artifact_path = Path(tmp_dir) / "field-scale.json"
            artifact_path.write_text(
                json.dumps(
                    {
                        "generated_at": datetime.now(timezone.utc).isoformat(),
                        "status": "failed",
                        "metadata": {},
                        "runs": [
                            {
                                "name": "run_a_single_branch_validate_only",
                                "command": "forward_smoke_sync --validate-only --query-limit 3",
                                "ok": True,
                                "exit_code": 0,
                                "elapsed_ms": 10,
                                "timed_out": False,
                                "timeout_seconds": 7,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            with (
                patch.dict(
                    os.environ,
                    {
                        "FORWARD_SMOKE_USERNAME": "user",
                        "FORWARD_SMOKE_PASSWORD": "secret",
                        "FORWARD_SMOKE_NETWORK_ID": "123",
                        "FORWARD_SMOKE_QUERY_LIMIT": "3",
                        "FORWARD_SMOKE_STEP_TIMEOUT_SECONDS": "7",
                        "FORWARD_FIELD_SCALE_EVIDENCE_PATH": str(artifact_path),
                    },
                    clear=True,
                ),
                patch.object(
                    tasks, "_field_scale_manage_py", side_effect=run_results
                ) as manage_py,
            ):
                evidence, status = tasks._run_field_scale_runtime_matrix(
                    context,
                    resume=True,
                )
            payload = json.loads(artifact_path.read_text(encoding="utf-8"))

        self.assertEqual(status, "completed")
        self.assertEqual(evidence["status"], "passed")
        self.assertEqual(manage_py.call_count, 2)
        self.assertEqual(payload["status"], "passed")
        self.assertEqual(
            [run["name"] for run in payload["runs"]],
            [
                "run_a_single_branch_validate_only",
                "run_b_single_branch_plan_only",
                "run_c_focused_validate_only",
            ],
        )

    def test_run_field_scale_runtime_matrix_step_only_writes_partial_artifact(self):
        context = self._context()
        with tempfile.TemporaryDirectory() as tmp_dir:
            artifact_path = Path(tmp_dir) / "field-scale.json"
            with (
                patch.dict(
                    os.environ,
                    {
                        "FORWARD_SMOKE_USERNAME": "user",
                        "FORWARD_SMOKE_PASSWORD": "secret",
                        "FORWARD_SMOKE_NETWORK_ID": "123",
                        "FORWARD_FIELD_SCALE_EVIDENCE_PATH": str(artifact_path),
                    },
                    clear=True,
                ),
                patch.object(
                    tasks,
                    "_field_scale_manage_py",
                    return_value=SimpleNamespace(ok=True, exited=0),
                ) as manage_py,
            ):
                evidence, status = tasks._run_field_scale_runtime_matrix(
                    context,
                    step="run_a_single_branch_validate_only",
                )
            payload = json.loads(artifact_path.read_text(encoding="utf-8"))

        self.assertEqual(status, "partial")
        self.assertEqual(evidence["status"], "partial")
        self.assertEqual(manage_py.call_count, 1)
        self.assertEqual(payload["status"], "partial")
        self.assertEqual(
            payload["metadata"]["selected_step"], "run_a_single_branch_validate_only"
        )

    def test_field_scale_evidence_from_artifact_reuses_fresh_passed_artifact(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            artifact_path = Path(tmp_dir) / "field-scale.json"
            artifact_path.write_text(
                json.dumps(
                    {
                        "generated_at": datetime.now(timezone.utc).isoformat(),
                        "status": "passed",
                        "metadata": {"models": "default_required_models"},
                        "runs": [
                            {
                                "name": "run_a_single_branch_validate_only",
                                "ok": True,
                                "timed_out": False,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            with patch.dict(
                os.environ,
                {"FORWARD_FIELD_SCALE_EVIDENCE_PATH": str(artifact_path)},
                clear=True,
            ):
                evidence, status = tasks._field_scale_evidence_from_artifact()

        self.assertEqual(status, "artifact-passed")
        self.assertEqual(evidence["status"], "passed")
        self.assertTrue(evidence["evidence"]["fresh"])
        self.assertEqual(
            evidence["evidence"]["runs"][0]["name"],
            "run_a_single_branch_validate_only",
        )

    def test_field_scale_evidence_from_artifact_rejects_stale_passed_artifact(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            artifact_path = Path(tmp_dir) / "field-scale.json"
            artifact_path.write_text(
                json.dumps(
                    {
                        "generated_at": "2026-05-01T00:00:00+00:00",
                        "status": "passed",
                        "metadata": {},
                        "runs": [],
                    }
                ),
                encoding="utf-8",
            )

            with patch.dict(
                os.environ,
                {"FORWARD_FIELD_SCALE_EVIDENCE_PATH": str(artifact_path)},
                clear=True,
            ):
                evidence, status = tasks._field_scale_evidence_from_artifact()

        self.assertEqual(status, "artifact-failed")
        self.assertEqual(evidence["status"], "failed")
        self.assertIn("older than 7 days", evidence["evidence"]["reason"])

    def test_release_dataset_gate_passes_with_fresh_matching_label(self):
        context = self._context()
        with tempfile.TemporaryDirectory() as tmp_dir:
            artifact_path = Path(tmp_dir) / "field-scale.json"
            artifact_path.write_text(
                json.dumps(
                    {
                        "generated_at": datetime.now(timezone.utc).isoformat(),
                        "status": "passed",
                        "metadata": {"dataset_label": "release-smoke", "resume": False},
                        "runs": [
                            {
                                "name": "run_a_single_branch_validate_only",
                                "ok": True,
                                "timed_out": False,
                            },
                            {
                                "name": "run_b_single_branch_plan_only",
                                "ok": True,
                                "timed_out": False,
                            },
                            {
                                "name": "run_c_focused_validate_only",
                                "ok": True,
                                "timed_out": False,
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )
            with patch.dict(
                os.environ,
                {"FORWARD_FIELD_SCALE_EVIDENCE_PATH": str(artifact_path)},
                clear=True,
            ):
                tasks.release_dataset_gate.body(context, dataset_label="release-smoke")

    def test_release_dataset_gate_fails_when_label_mismatches(self):
        context = self._context()
        with tempfile.TemporaryDirectory() as tmp_dir:
            artifact_path = Path(tmp_dir) / "field-scale.json"
            artifact_path.write_text(
                json.dumps(
                    {
                        "generated_at": datetime.now(timezone.utc).isoformat(),
                        "status": "passed",
                        "metadata": {
                            "dataset_label": "release-staging",
                            "resume": False,
                        },
                        "runs": [
                            {
                                "name": "run_a_single_branch_validate_only",
                                "ok": True,
                                "timed_out": False,
                            },
                            {
                                "name": "run_b_single_branch_plan_only",
                                "ok": True,
                                "timed_out": False,
                            },
                            {
                                "name": "run_c_focused_validate_only",
                                "ok": True,
                                "timed_out": False,
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )
            with patch.dict(
                os.environ,
                {"FORWARD_FIELD_SCALE_EVIDENCE_PATH": str(artifact_path)},
                clear=True,
            ):
                with self.assertRaises(Exit) as raised:
                    tasks.release_dataset_gate.body(
                        context, dataset_label="release-smoke"
                    )

        self.assertEqual(raised.exception.code, 1)
        self.assertIn("dataset label", str(raised.exception))

    def test_release_dataset_gate_fails_when_artifact_is_resumed(self):
        context = self._context()
        with tempfile.TemporaryDirectory() as tmp_dir:
            artifact_path = Path(tmp_dir) / "field-scale.json"
            artifact_path.write_text(
                json.dumps(
                    {
                        "generated_at": datetime.now(timezone.utc).isoformat(),
                        "status": "passed",
                        "metadata": {"dataset_label": "release-smoke", "resume": True},
                        "runs": [
                            {
                                "name": "run_a_single_branch_validate_only",
                                "ok": True,
                                "timed_out": False,
                            },
                            {
                                "name": "run_b_single_branch_plan_only",
                                "ok": True,
                                "timed_out": False,
                            },
                            {
                                "name": "run_c_focused_validate_only",
                                "ok": True,
                                "timed_out": False,
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )
            with patch.dict(
                os.environ,
                {"FORWARD_FIELD_SCALE_EVIDENCE_PATH": str(artifact_path)},
                clear=True,
            ):
                with self.assertRaises(Exit) as raised:
                    tasks.release_dataset_gate.body(
                        context, dataset_label="release-smoke"
                    )

        self.assertEqual(raised.exception.code, 1)
        self.assertIn("regenerate field-scale evidence", str(raised.exception))

    def test_release_dataset_gate_can_allow_resumed_artifact(self):
        context = self._context()
        with tempfile.TemporaryDirectory() as tmp_dir:
            artifact_path = Path(tmp_dir) / "field-scale.json"
            artifact_path.write_text(
                json.dumps(
                    {
                        "generated_at": datetime.now(timezone.utc).isoformat(),
                        "status": "passed",
                        "metadata": {"dataset_label": "release-smoke", "resume": True},
                        "runs": [
                            {
                                "name": "run_a_single_branch_validate_only",
                                "ok": True,
                                "timed_out": False,
                            },
                            {
                                "name": "run_b_single_branch_plan_only",
                                "ok": True,
                                "timed_out": False,
                            },
                            {
                                "name": "run_c_focused_validate_only",
                                "ok": True,
                                "timed_out": False,
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )
            with patch.dict(
                os.environ,
                {"FORWARD_FIELD_SCALE_EVIDENCE_PATH": str(artifact_path)},
                clear=True,
            ):
                tasks.release_dataset_gate.body(
                    context,
                    dataset_label="release-smoke",
                    allow_resumed_artifact=True,
                )

    def test_release_dataset_gate_fails_when_required_step_missing(self):
        context = self._context()
        with tempfile.TemporaryDirectory() as tmp_dir:
            artifact_path = Path(tmp_dir) / "field-scale.json"
            artifact_path.write_text(
                json.dumps(
                    {
                        "generated_at": datetime.now(timezone.utc).isoformat(),
                        "status": "passed",
                        "metadata": {"dataset_label": "release-smoke", "resume": False},
                        "runs": [
                            {
                                "name": "run_a_single_branch_validate_only",
                                "ok": True,
                                "timed_out": False,
                            },
                            {
                                "name": "run_b_single_branch_plan_only",
                                "ok": True,
                                "timed_out": False,
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )
            with patch.dict(
                os.environ,
                {"FORWARD_FIELD_SCALE_EVIDENCE_PATH": str(artifact_path)},
                clear=True,
            ):
                with self.assertRaises(Exit) as raised:
                    tasks.release_dataset_gate.body(
                        context, dataset_label="release-smoke"
                    )

        self.assertEqual(raised.exception.code, 1)
        self.assertIn("regenerate field-scale evidence", str(raised.exception))

    def test_release_dataset_gate_reports_failed_step_codes(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            artifact_path = Path(tmp_dir) / "field-scale.json"
            artifact_path.write_text(
                json.dumps(
                    {
                        "generated_at": datetime.now(timezone.utc).isoformat(),
                        "status": "failed",
                        "metadata": {"dataset_label": "release-smoke", "resume": False},
                        "runs": [
                            {
                                "name": "run_a_single_branch_validate_only",
                                "ok": False,
                                "timed_out": False,
                                "failure_code": "docker_api_unreachable",
                            },
                            {
                                "name": "run_b_single_branch_plan_only",
                                "ok": True,
                                "timed_out": False,
                                "failure_code": "",
                            },
                            {
                                "name": "run_c_focused_validate_only",
                                "ok": False,
                                "timed_out": False,
                                "failure_code": "docker_api_unreachable",
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )
            evidence = tasks._collect_release_dataset_gate_evidence(
                dataset_label="release-smoke",
                max_age_days=7,
                allow_resumed_artifact=False,
                artifact_path=str(artifact_path),
            )

        self.assertEqual(evidence["status"], "failed")
        self.assertEqual(
            evidence["evidence"]["failed_step_codes"][
                "run_a_single_branch_validate_only"
            ],
            "docker_api_unreachable",
        )
        self.assertIn(
            "failure codes: docker_api_unreachable", evidence["evidence"]["reason"]
        )

    def test_release_runtime_preflight_passes_with_automatic_existing_source(self):
        context = self._context()
        with (
            patch.dict(
                os.environ,
                {"FORWARD_SMOKE_DATASET_LABEL": "release-smoke"},
                clear=True,
            ),
            patch.object(
                tasks, "_field_scale_runtime_preflight", return_value={"ok": True}
            ),
            patch.object(
                tasks,
                "_field_scale_source_preflight",
                return_value={"ok": True, "selection": "automatic_existing"},
            ),
        ):
            evidence = tasks._collect_release_runtime_preflight_evidence(
                context=context,
                dataset_label="release-smoke",
            )
        self.assertEqual(evidence["status"], "passed")
        self.assertEqual(evidence["evidence"]["missing_env"], [])
        self.assertTrue(evidence["evidence"]["dataset_label_matches"])
        self.assertEqual(evidence["evidence"]["source_selection"], "automatic_existing")

    def test_release_runtime_preflight_does_not_emit_source_name(self):
        context = self._context()
        private_name = "private-release-source"
        with (
            patch.dict(
                os.environ,
                {
                    "FORWARD_SMOKE_SOURCE_NAME": private_name,
                    "FORWARD_SMOKE_DATASET_LABEL": "release-smoke",
                },
                clear=True,
            ),
            patch.object(
                tasks, "_field_scale_runtime_preflight", return_value={"ok": True}
            ),
            patch.object(
                tasks,
                "_field_scale_source_preflight",
                return_value={"ok": True, "selection": "automatic_existing"},
            ),
        ):
            evidence = tasks._collect_release_runtime_preflight_evidence(
                context=context,
                dataset_label="release-smoke",
            )
        self.assertEqual(evidence["status"], "passed")
        self.assertEqual(evidence["evidence"]["missing_env"], [])
        self.assertTrue(evidence["evidence"]["source_backed"])
        self.assertNotIn(private_name, str(evidence))

    def test_release_runtime_preflight_fails_when_label_or_docker_invalid(self):
        context = self._context()
        with (
            patch.dict(
                os.environ,
                {
                    "FORWARD_SMOKE_USERNAME": "user",
                    "FORWARD_SMOKE_DATASET_LABEL": "release-staging",
                },
                clear=True,
            ),
            patch.object(
                tasks,
                "_field_scale_runtime_preflight",
                return_value={
                    "ok": False,
                    "exit_code": 1,
                    "failure_code": "docker_api_unreachable",
                    "failure_hint": "cannot connect to local Docker API",
                },
            ),
        ):
            evidence = tasks._collect_release_runtime_preflight_evidence(
                context=context,
                dataset_label="release-smoke",
            )
        self.assertEqual(evidence["status"], "failed")
        self.assertEqual(evidence["evidence"]["missing_env"], [])
        self.assertFalse(evidence["evidence"]["dataset_label_matches"])
        self.assertIn("docker preflight failed", evidence["evidence"]["reason"])

    def test_release_runtime_preflight_fails_when_source_is_unavailable(self):
        context = self._context()
        with (
            patch.dict(
                os.environ,
                {"FORWARD_SMOKE_DATASET_LABEL": "release-smoke"},
                clear=True,
            ),
            patch.object(
                tasks, "_field_scale_runtime_preflight", return_value={"ok": True}
            ),
            patch.object(
                tasks,
                "_field_scale_source_preflight",
                return_value={
                    "ok": False,
                    "failure_code": "command_failed",
                    "failure_hint": "configured source is unavailable",
                },
            ),
        ):
            evidence = tasks._collect_release_runtime_preflight_evidence(
                context=context,
                dataset_label="release-smoke",
            )
        self.assertEqual(evidence["status"], "failed")
        self.assertIn("source preflight failed", evidence["evidence"]["reason"])

    def test_release_runtime_preflight_task_raises_on_failure(self):
        context = self._context()
        with (
            patch.dict(os.environ, {}, clear=True),
            patch.object(
                tasks,
                "_field_scale_runtime_preflight",
                return_value={
                    "ok": False,
                    "exit_code": 1,
                    "failure_code": "docker_api_unreachable",
                    "failure_hint": "cannot connect to local Docker API",
                },
            ),
        ):
            with self.assertRaises(Exit) as raised:
                tasks.release_runtime_preflight.body(
                    context, dataset_label="release-smoke"
                )
        self.assertEqual(raised.exception.code, 1)

    def test_release_readiness_audit_passes_when_all_checks_pass(self):
        context = self._context()
        with (
            patch.object(
                tasks,
                "_collect_release_runtime_preflight_evidence",
                return_value={"status": "passed", "evidence": {}},
            ),
            patch.object(
                tasks,
                "_collect_release_dataset_gate_evidence",
                return_value={"status": "passed", "evidence": {}},
            ),
            patch.object(
                tasks,
                "_collect_validation_org_query_audit_evidence",
                return_value={"status": "passed", "evidence": {}},
            ),
            patch.object(
                tasks,
                "_run_tests_with_shared_runtime_fallback",
            ),
        ):
            audit = tasks._collect_release_readiness_audit(
                context=context,
                dataset_label="release-smoke",
            )

        self.assertEqual(audit["status"], "passed")
        self.assertEqual(audit["failed_checks"], [])
        self.assertEqual(
            audit["checks"]["architecture_completion_gate"]["status"],
            "passed",
        )
        self.assertEqual(
            audit["checks"]["validation_org_query_audit"]["status"],
            "passed",
        )

    def test_release_readiness_audit_fails_when_architecture_command_fails(self):
        context = self._context()
        with (
            patch.object(
                tasks,
                "_collect_release_runtime_preflight_evidence",
                return_value={"status": "failed", "evidence": {"reason": "x"}},
            ),
            patch.object(
                tasks,
                "_collect_release_dataset_gate_evidence",
                return_value={"status": "failed", "evidence": {"reason": "y"}},
            ),
            patch.object(
                tasks,
                "_collect_validation_org_query_audit_evidence",
                return_value={"status": "passed", "evidence": {}},
            ),
            patch.object(
                tasks,
                "_run_tests_with_shared_runtime_fallback",
                side_effect=Exit("architecture tests failed", code=1),
            ),
        ):
            audit = tasks._collect_release_readiness_audit(
                context=context,
                dataset_label="release-smoke",
            )

        self.assertEqual(audit["status"], "failed")
        self.assertIn("release_runtime_preflight", audit["failed_checks"])
        self.assertIn("release_dataset_gate", audit["failed_checks"])
        self.assertIn("architecture_completion_gate", audit["failed_checks"])
        self.assertEqual(
            audit["checks"]["architecture_completion_gate"]["evidence"]["failure_code"],
            "architecture_contract_failed",
        )

    def test_validation_org_gate_uses_redacted_existing_source_command(self):
        context = self._context()
        result = SimpleNamespace(
            ok=True,
            exited=0,
            stdout=json.dumps(
                {
                    "status": "pass",
                    "matched_count": 25,
                    "missing_count": 0,
                    "stale_count": 0,
                }
            ),
            stderr="",
        )
        with (
            patch.dict(
                os.environ,
                {"FORWARD_VALIDATION_SOURCE_NAME": ""},
            ),
            patch.object(
                tasks, "_field_scale_manage_py", return_value=result
            ) as manage_py,
        ):
            evidence = tasks._collect_validation_org_query_audit_evidence(context)

        self.assertEqual(evidence["status"], "passed")
        command = manage_py.call_args.args[1]
        self.assertIn("--summary-only", command)
        self.assertNotIn("--username", command)
        self.assertNotIn("--password", command)
        self.assertNotIn("--network-id", command)
        self.assertNotIn("--source-name", command)

    def test_validation_org_gate_redacts_explicit_source_name(self):
        context = self._context()
        result = SimpleNamespace(
            ok=True,
            exited=0,
            stdout=json.dumps({"status": "pass"}),
            stderr="",
        )
        with patch.object(
            tasks, "_field_scale_manage_py", return_value=result
        ) as manage_py:
            evidence = tasks._collect_validation_org_query_audit_evidence(
                context,
                source_name="private-validation-source",
            )

        self.assertEqual(evidence["status"], "passed")
        self.assertEqual(evidence["evidence"]["source_selection"], "explicit_existing")
        self.assertNotIn(
            "private-validation-source",
            evidence["evidence"]["command"],
        )
        self.assertIn("--source-name <redacted>", evidence["evidence"]["command"])
        self.assertIn(
            "--source-name private-validation-source",
            manage_py.call_args.args[1],
        )

    def test_release_readiness_audit_task_raises_on_failure(self):
        context = self._context()
        with patch.object(
            tasks,
            "_collect_release_readiness_audit",
            return_value={"status": "failed", "checks": {}, "failed_checks": ["x"]},
        ):
            with self.assertRaises(Exit) as raised:
                tasks.release_readiness_audit.body(
                    context,
                    dataset_label="release-smoke",
                    output_json="",
                )
        self.assertEqual(raised.exception.code, 1)

    def test_collect_compatibility_cache_evidence_reports_passed_when_no_stale(self):
        context = self._context()
        with tempfile.TemporaryDirectory() as tmp_dir:
            repo_root = Path(tmp_dir)
            report_path = (
                repo_root / "docs/03_Plans/evidence/compat-cache-prune-runtime.json"
            )
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(
                json.dumps(
                    {
                        "inspected_syncs": 3,
                        "stale_payload_syncs": 0,
                        "pruned_syncs": 0,
                        "sync_name_filter": "ui-harness-sync",
                    }
                ),
                encoding="utf-8",
            )

            with patch.object(tasks, "manage_py") as manage_py:
                evidence = tasks._collect_compatibility_cache_evidence(
                    context=context,
                    repo_root=repo_root,
                    sync_name="ui-harness-sync",
                )

        manage_py.assert_called_once()
        self.assertEqual(evidence["status"], "passed")
        self.assertEqual(evidence["evidence"]["stale_payload_syncs"], 0)

    def test_collect_compatibility_cache_evidence_reports_failed_when_report_missing(
        self,
    ):
        context = self._context()
        with tempfile.TemporaryDirectory() as tmp_dir:
            repo_root = Path(tmp_dir)
            with patch.object(tasks, "manage_py"):
                evidence = tasks._collect_compatibility_cache_evidence(
                    context=context,
                    repo_root=repo_root,
                    sync_name="",
                )

        self.assertEqual(evidence["status"], "failed")
        self.assertIn("not generated", evidence["evidence"]["reason"])

    def test_architecture_audit_check_uses_strict_mode(self):
        context = self._context()
        with patch.object(
            tasks, "_run_tests_with_shared_runtime_fallback"
        ) as run_tests:
            tasks.architecture_audit_check.body(context)

        run_tests.assert_called_once_with(
            context,
            test_label=tasks.ARCHITECTURE_AUDIT_TEST_LABELS,
        )
        self.assertIn(
            "test_apply_engine_classifies_all_supported_models",
            tasks.ARCHITECTURE_AUDIT_TEST_LABELS,
        )
        self.assertIn(
            "test_builtin_query_contract_summary_passes_for_parameterized_maps",
            tasks.ARCHITECTURE_AUDIT_TEST_LABELS,
        )


class RuntimeOptimizationTaskTest(unittest.TestCase):
    def setUp(self):
        super().setUp()
        collector_patch = patch.object(
            tasks,
            "_collect_destructive_runtime_evidence",
            return_value={
                "status": "passed",
                "evidence": {
                    "scenarios": [
                        {
                            "scenario": "stage-before-branch",
                            "ok": True,
                            "bundle": "docs/03_Plans/evidence/chaos/chaos-stage-before-branch-run-1.json",
                            "metadata": "docs/03_Plans/evidence/chaos/chaos-stage-before-branch-metadata-unit.json",
                            "support_bundle_recovery_verified": True,
                        }
                    ],
                    "output_dir": "docs/03_Plans/evidence/chaos",
                },
            },
        )
        collector_patch.start()
        self.addCleanup(collector_patch.stop)

    def _context(self):
        context = Mock()
        context.forward_netbox = SimpleNamespace(
            netbox_ver="v4.5.9",
            project_name="forward-netbox",
            compose_dir="/tmp/forward-netbox",
        )
        return context

    def _capacity_review(self):
        return {
            "status": "passed",
            "evidence": {
                "workers": {"current": 4, "recommended": 4, "status": "pass"},
                "scheduler_overlap_capacity_review": {"status": "pass"},
            },
        }

    def test_ingestion_delete_regression_runs_expected_tests(self):
        context = self._context()
        with (
            patch.object(tasks, "_guard_shared_runtime_tests"),
            patch.object(tasks, "manage_py") as manage_py,
        ):
            tasks.ingestion_delete_regression.body(context)

        manage_py.assert_called_once()
        command = manage_py.call_args.args[1]
        self.assertIn("test_single_branch_repeat_run_applies_delete_phase", command)
        self.assertIn("test_branch_plan_splits_mixed_workloads", command)

    def test_scale_chaos_uses_current_scenario_tests(self):
        context = self._context()
        with patch.object(tasks, "_run_tests_with_shared_runtime_fallback") as run:
            tasks.scale_chaos_test.body(context)

        test_label = run.call_args.kwargs["test_label"]
        self.assertIn("BulkMergeIntegrationTest", test_label)
        self.assertIn("SingleBranchExecutorTest", test_label)
        self.assertIn("StuckRecoveryTest", test_label)
        self.assertIn("forward_netbox.tests.test_api_views", test_label)
        self.assertNotIn("ForwardExecutionRunAPIViewTest", test_label)
        self.assertNotIn("test_synthetic_scenarios", test_label)

    def test_github_ci_uses_current_scenario_tests(self):
        repo_root = Path(__file__).resolve().parents[2]
        workflow = (repo_root / ".github/workflows/ci.yml").read_text(encoding="utf-8")

        for test_label in tasks.SCENARIO_TEST_LABELS.split():
            self.assertIn(test_label, workflow)
        self.assertNotIn("test_synthetic_scenarios", workflow)

    def test_optimize_runtime_scales_workers_and_tunes_postgres(self):
        context = self._context()
        context.run.return_value = SimpleNamespace(stdout="2\n")
        with (
            patch.object(tasks, "docker_compose") as docker_compose,
            patch.object(tasks, "manage_py") as manage_py,
            patch.object(tasks, "_recommended_worker_replicas", return_value=12),
        ):
            tasks.optimize_runtime.body(
                context,
                worker_replicas=0,
                query_fetch_concurrency=16,
                nqe_page_size=10000,
                source_name="",
                apply_postgres=True,
            )

        self.assertGreaterEqual(docker_compose.call_count, 4)
        commands = [call.args[1] for call in docker_compose.call_args_list]
        self.assertIn("up -d", commands[0])
        self.assertIn("restart postgres", commands)
        self.assertIn("up -d --scale netbox-worker=12 netbox netbox-worker", commands)
        manage_py.assert_not_called()

    def test_optimize_runtime_updates_source_parameters_when_source_name_set(self):
        context = self._context()
        with (
            patch.object(tasks, "docker_compose") as docker_compose,
            patch.object(
                context,
                "run",
                return_value=SimpleNamespace(stdout="4\n"),
            ),
        ):
            tasks.optimize_runtime.body(
                context,
                worker_replicas=4,
                query_fetch_concurrency=15,
                nqe_page_size=9000,
                source_name="live-source",
                apply_postgres=False,
            )

        command = " ".join(call.args[1] for call in docker_compose.call_args_list)
        self.assertIn("ForwardSource.objects.get", command)
        self.assertIn("query_fetch_concurrency", command)
        self.assertIn("nqe_page_size", command)
        self.assertIn("live-source", command)

    def test_docker_chaos_preserves_requested_worker_replicas(self):
        context = self._context()
        killed = []

        def fake_docker_compose(_context, command, *args, **kwargs):
            if "ps -q netbox-worker" in command:
                return SimpleNamespace(stdout="worker-1\nworker-2\n", ok=True)
            return SimpleNamespace(stdout="", ok=True)

        def fake_run(command, *args, **kwargs):
            killed.append(command)
            return SimpleNamespace(stdout="", ok=True)

        with (
            patch.dict(os.environ, {"FORWARD_CHAOS_WORKER_REPLICAS": "4"}, clear=True),
            patch.object(
                tasks, "docker_compose", side_effect=fake_docker_compose
            ) as docker_compose,
            patch.object(context, "run", side_effect=fake_run),
        ):
            tasks.docker_chaos_kill.body(
                context,
                scenario="stage-before-branch",
                confirm=True,
            )

        commands = [call.args[1] for call in docker_compose.call_args_list]
        self.assertGreaterEqual(
            commands.count("up -d --scale netbox-worker=4 netbox netbox-worker"),
            2,
        )
        self.assertEqual(killed, ["docker kill worker-1"])

    def test_runtime_evidence_scales_capacity_workers(self):
        context = self._context()
        output_rel = "docs/03_Plans/evidence/runtime-evidence-worker-scale-test.json"
        repo_root = Path(tasks.__file__).resolve().parent
        output_abs = repo_root / output_rel
        report_abs = repo_root / "docs/03_Plans/evidence/scale-runtime-evidence.json"
        original_report = (
            report_abs.read_text(encoding="utf-8") if report_abs.exists() else None
        )
        scale_calls = []

        def fake_manage_py(_context, command, *args, **kwargs):
            if "forward_scale_benchmark" in command:
                report = (
                    repo_root / "docs/03_Plans/evidence/scale-runtime-evidence.json"
                )
                report.parent.mkdir(parents=True, exist_ok=True)
                report.write_text(
                    json.dumps(
                        {
                            "status": "pass",
                            "summary": {"step_count": 4},
                            "checks": [
                                {"code": "support_bundle_shape", "status": "pass"},
                                {"code": "run_completion", "status": "pass"},
                                {"code": "row_failures", "status": "pass"},
                                {"code": "pushdown_efficiency", "status": "pass"},
                                {"code": "pushdown_runtime", "status": "pass"},
                                {"code": "partition_retry_pressure", "status": "pass"},
                                {
                                    "code": "throughput_smoothing",
                                    "status": "pass",
                                    "evidence": {
                                        "scheduler_overlap_readiness": {
                                            "status": "not_warranted"
                                        }
                                    },
                                },
                            ],
                        }
                    ),
                    encoding="utf-8",
                )
            return SimpleNamespace(ok=True, exited=0, stdout="")

        def fake_docker_compose(_context, command, *args, **kwargs):
            scale_calls.append(command)
            return SimpleNamespace(ok=True, exited=0, stdout="4\n")

        try:
            with (
                patch.object(tasks, "manage_py", side_effect=fake_manage_py),
                patch.object(tasks, "docker_compose", side_effect=fake_docker_compose),
                patch.object(tasks, "_current_worker_replicas", return_value=4),
                patch.object(
                    tasks,
                    "_collect_runtime_capacity_review",
                    return_value=self._capacity_review(),
                ),
                patch.object(
                    context,
                    "run",
                    return_value=SimpleNamespace(ok=True, exited=0, stdout=""),
                ),
            ):
                tasks.architecture_runtime_evidence.body(
                    context,
                    output_path=output_rel,
                    sync_name="ui-harness-sync",
                    capacity_worker_replicas=4,
                )
        finally:
            output_abs.unlink(missing_ok=True)
            if original_report is None:
                report_abs.unlink(missing_ok=True)
            else:
                report_abs.write_text(original_report, encoding="utf-8")

        self.assertIn("up -d --scale netbox-worker=4 netbox netbox-worker", scale_calls)

    def test_runtime_evidence_applies_capacity_source_tuning_after_seed(self):
        context = self._context()
        output_rel = "docs/03_Plans/evidence/runtime-evidence-source-tuning-test.json"
        repo_root = Path(tasks.__file__).resolve().parent
        output_abs = repo_root / output_rel
        report_abs = repo_root / "docs/03_Plans/evidence/scale-runtime-evidence.json"
        original_report = (
            report_abs.read_text(encoding="utf-8") if report_abs.exists() else None
        )
        tuning_calls = []

        def fake_manage_py(_context, command, *args, **kwargs):
            if "forward_scale_benchmark" in command:
                report = (
                    repo_root / "docs/03_Plans/evidence/scale-runtime-evidence.json"
                )
                report.parent.mkdir(parents=True, exist_ok=True)
                report.write_text(
                    json.dumps(
                        {
                            "status": "pass",
                            "summary": {"step_count": 4},
                            "checks": [
                                {"code": "support_bundle_shape", "status": "pass"},
                                {"code": "run_completion", "status": "pass"},
                                {"code": "row_failures", "status": "pass"},
                                {"code": "pushdown_efficiency", "status": "pass"},
                                {"code": "pushdown_runtime", "status": "pass"},
                                {"code": "partition_retry_pressure", "status": "pass"},
                                {
                                    "code": "throughput_smoothing",
                                    "status": "pass",
                                    "evidence": {
                                        "scheduler_overlap_readiness": {
                                            "status": "not_warranted"
                                        }
                                    },
                                },
                            ],
                        }
                    ),
                    encoding="utf-8",
                )
            return SimpleNamespace(ok=True, exited=0, stdout="")

        def fake_apply_tuning(_context, **kwargs):
            tuning_calls.append(kwargs)
            return True

        try:
            with (
                patch.object(tasks, "manage_py", side_effect=fake_manage_py),
                patch.object(
                    tasks,
                    "_apply_source_fetch_tuning",
                    side_effect=fake_apply_tuning,
                ),
                patch.object(tasks, "docker_compose"),
                patch.object(
                    tasks,
                    "_collect_runtime_capacity_review",
                    return_value=self._capacity_review(),
                ),
                patch.object(
                    context,
                    "run",
                    return_value=SimpleNamespace(ok=True, exited=0, stdout=""),
                ),
            ):
                tasks.architecture_runtime_evidence.body(
                    context,
                    output_path=output_rel,
                    sync_name="ui-harness-sync",
                    capacity_source_name="live-source",
                    capacity_query_fetch_concurrency=6,
                    capacity_nqe_page_size=10000,
                )

            payload = json.loads(output_abs.read_text(encoding="utf-8"))
        finally:
            output_abs.unlink(missing_ok=True)
            if original_report is None:
                report_abs.unlink(missing_ok=True)
            else:
                report_abs.write_text(original_report, encoding="utf-8")

        self.assertEqual(
            tuning_calls,
            [
                {
                    "source_name": "live-source",
                    "query_fetch_concurrency": 6,
                    "nqe_page_size": 10000,
                }
            ],
        )
        self.assertTrue(payload["notes"]["capacity_source_tuning_applied"])
        self.assertEqual(payload["notes"]["capacity_query_fetch_concurrency"], 6)
        self.assertEqual(payload["notes"]["capacity_nqe_page_size"], 10000)

    def test_runtime_capacity_review_reports_worker_and_source_state(self):
        context = self._context()

        def fake_docker_compose(_context, command, *args, **kwargs):
            if command == "ps -q postgres":
                return SimpleNamespace(stdout="postgres-container\n")
            self.assertIn("ForwardSource.objects.filter", command)
            return SimpleNamespace(
                stdout='noise\n{"available": true, "query_fetch_concurrency": 6}\n'
            )

        run_outputs = [
            SimpleNamespace(stdout="4\n"),
            SimpleNamespace(stdout='"/mnt/fwd-vmstore/docker-data"\n'),
            SimpleNamespace(
                stdout=json.dumps(
                    [
                        {
                            "Type": "volume",
                            "Source": (
                                "/mnt/fwd-vmstore/docker-data/volumes/"
                                "forward-netbox_netbox-postgres-data/_data"
                            ),
                            "Destination": "/var/lib/postgresql/data",
                        }
                    ]
                )
                + "\n"
            ),
        ]

        with (
            patch.object(tasks, "_recommended_worker_replicas", return_value=4),
            patch.object(tasks, "_host_memory_gib", return_value=64),
            patch.object(context, "run", side_effect=run_outputs),
            patch.object(tasks, "docker_compose", side_effect=fake_docker_compose),
        ):
            report = tasks._runtime_capacity_review(
                context,
                source_name="live-source",
            )

        self.assertEqual(report["workers"]["status"], "pass")
        self.assertEqual(report["workers"]["current"], 4)
        self.assertEqual(report["source"]["query_fetch_concurrency"], 6)
        self.assertEqual(
            report["scheduler_overlap_capacity_review"]["status"],
            "pass",
        )
        self.assertEqual(report["storage"]["status"], "pass")
        self.assertIn("postgres-data", report["storage"]["postgres_data_source"])


class SharedRuntimeTestGuardTaskTest(unittest.TestCase):
    def _context(self):
        context = Mock()
        context.forward_netbox = SimpleNamespace(
            netbox_ver="v4.5.9",
            project_name="forward-netbox",
            compose_dir="/tmp/forward-netbox",
        )
        return context

    def test_guard_blocks_tests_when_execution_run_is_active(self):
        context = self._context()
        payload = {
            "active_count": 1,
            "runs": [
                {
                    "id": 119,
                    "sync__name": "field-scale-sync",
                    "status": "running",
                }
            ],
        }

        with (
            patch.object(
                tasks,
                "docker_compose",
                return_value=SimpleNamespace(stdout=json.dumps(payload) + "\n"),
            ),
            patch.dict(os.environ, {}, clear=False),
        ):
            with self.assertRaises(Exit) as raised:
                tasks._guard_shared_runtime_tests(context)

        self.assertEqual(raised.exception.code, 2)
        self.assertIn("Active Forward execution run", str(raised.exception))
        self.assertIn("run 119", str(raised.exception))
        self.assertIn(tasks.ALLOW_SHARED_RUNTIME_TESTS_ENV, str(raised.exception))

    def test_shared_runtime_probe_reports_unavailable_on_command_failure(self):
        context = self._context()
        with patch.object(
            tasks,
            "docker_compose",
            return_value=SimpleNamespace(
                stdout="",
                stderr="FATAL:  sorry, too many clients already",
                exited=2,
            ),
        ):
            payload = tasks._shared_runtime_active_execution_runs(context)

        self.assertFalse(payload["guard_available"])
        self.assertIn("too many clients", payload["reason"])

    def test_guard_blocks_tests_when_shared_runtime_probe_is_unavailable(self):
        context = self._context()
        with patch.object(
            tasks,
            "_shared_runtime_active_execution_runs",
            return_value={
                "active_count": 0,
                "runs": [],
                "guard_available": False,
                "reason": "shared_runtime_probe_failed",
            },
        ):
            with self.assertRaises(Exit) as raised:
                tasks._guard_shared_runtime_tests(context)

        self.assertEqual(raised.exception.code, 2)
        self.assertIn("Could not inspect", str(raised.exception))
        self.assertIn("test-isolated", str(raised.exception))

    def test_guard_allows_bypass_for_intentional_shared_runtime_tests(self):
        context = self._context()
        with (
            patch.object(tasks, "docker_compose") as docker_compose,
            patch.dict(
                os.environ,
                {tasks.ALLOW_SHARED_RUNTIME_TESTS_ENV: "1"},
                clear=False,
            ),
        ):
            tasks._guard_shared_runtime_tests(context)

        docker_compose.assert_not_called()

    def test_test_task_runs_guard_before_django_tests(self):
        context = self._context()
        calls = []

        def fake_guard(_context):
            calls.append("guard")

        def fake_manage_py(_context, command):
            calls.append("test")
            self.assertIn("forward_netbox.tests", command)

        with (
            patch.object(tasks, "_guard_shared_runtime_tests", side_effect=fake_guard),
            patch.object(tasks, "manage_py", side_effect=fake_manage_py),
        ):
            tasks.test.body(context)

        self.assertEqual(calls, ["guard", "test"])

    def test_test_ci_uses_shared_runtime_when_no_active_runs(self):
        context = self._context()
        with (
            patch.object(
                tasks,
                "_shared_runtime_active_execution_runs",
                return_value={"active_count": 0, "runs": []},
            ),
            patch.object(tasks, "manage_py") as manage_py,
            patch.object(tasks, "_run_tests_in_isolated_runtime") as isolated_run,
            patch.dict(os.environ, {}, clear=False),
        ):
            tasks.test_ci.body(context)

        manage_py.assert_called_once_with(
            context,
            "test --keepdb --noinput forward_netbox.tests",
        )
        isolated_run.assert_not_called()

    def test_test_ci_uses_isolated_runtime_when_guard_is_unavailable(self):
        context = self._context()
        with (
            patch.object(
                tasks,
                "_shared_runtime_active_execution_runs",
                return_value={
                    "active_count": 0,
                    "runs": [],
                    "guard_available": False,
                    "reason": "shared_runtime_probe_failed",
                },
            ),
            patch.object(tasks, "manage_py") as manage_py,
            patch.object(tasks, "_run_tests_in_isolated_runtime") as isolated_run,
            patch.dict(os.environ, {}, clear=False),
        ):
            tasks.test_ci.body(context)

        manage_py.assert_not_called()
        isolated_run.assert_called_once_with(
            context,
            test_label="forward_netbox.tests",
            project_name=f"{tasks.ISOLATED_TEST_PROJECT_NAME}-ci",
            keep_runtime=False,
        )

    def test_test_ci_uses_isolated_runtime_when_active_runs_exist(self):
        context = self._context()
        with (
            patch.object(
                tasks,
                "_shared_runtime_active_execution_runs",
                return_value={"active_count": 2, "runs": [{"id": 1}, {"id": 2}]},
            ),
            patch.object(tasks, "manage_py") as manage_py,
            patch.object(tasks, "_run_tests_in_isolated_runtime") as isolated_run,
            patch.dict(os.environ, {}, clear=False),
        ):
            tasks.test_ci.body(context)

        manage_py.assert_not_called()
        isolated_run.assert_called_once_with(
            context,
            test_label="forward_netbox.tests",
            project_name=f"{tasks.ISOLATED_TEST_PROJECT_NAME}-ci",
            keep_runtime=False,
        )

    def test_playwright_test_uses_shared_runtime_when_no_active_runs(self):
        context = self._context()
        with (
            patch.object(
                tasks,
                "_shared_runtime_active_execution_runs",
                return_value={"active_count": 0, "runs": []},
            ),
            patch.object(tasks, "_run_playwright_ui") as playwright_run,
            patch.object(tasks, "_run_playwright_in_isolated_runtime") as isolated_run,
            patch.dict(os.environ, {}, clear=False),
        ):
            tasks.playwright_test.body(context)

        playwright_run.assert_called_once_with(context)
        isolated_run.assert_not_called()

    def test_playwright_test_uses_isolated_runtime_when_guard_is_unavailable(self):
        context = self._context()
        with (
            patch.object(
                tasks,
                "_shared_runtime_active_execution_runs",
                return_value={
                    "active_count": 0,
                    "runs": [],
                    "guard_available": False,
                    "reason": "shared_runtime_probe_failed",
                },
            ),
            patch.object(tasks, "_run_playwright_ui") as playwright_run,
            patch.object(tasks, "_run_playwright_in_isolated_runtime") as isolated_run,
            patch.dict(os.environ, {}, clear=False),
        ):
            tasks.playwright_test.body(context)

        playwright_run.assert_not_called()
        isolated_run.assert_called_once_with(context)

    def test_playwright_test_uses_isolated_runtime_when_active_runs_exist(self):
        context = self._context()
        with (
            patch.object(
                tasks,
                "_shared_runtime_active_execution_runs",
                return_value={"active_count": 2, "runs": [{"id": 1}, {"id": 2}]},
            ),
            patch.object(tasks, "_run_playwright_ui") as playwright_run,
            patch.object(tasks, "_run_playwright_in_isolated_runtime") as isolated_run,
            patch.dict(os.environ, {}, clear=False),
        ):
            tasks.playwright_test.body(context)

        playwright_run.assert_not_called()
        isolated_run.assert_called_once_with(context)

    def test_playwright_isolated_runtime_uses_separate_project_and_port(self):
        context = self._context()
        compose_calls = []

        def fake_docker_compose(compose_context, command, **kwargs):
            compose_calls.append(
                (
                    compose_context.forward_netbox.project_name,
                    command,
                    kwargs.get("env"),
                )
            )
            return SimpleNamespace(stdout="")

        with (
            patch.object(tasks, "docker_compose", side_effect=fake_docker_compose),
            patch.object(tasks, "_run_playwright_ui") as playwright_run,
        ):
            tasks._run_playwright_in_isolated_runtime(
                context,
                project_name="forward-netbox-ui-test",
                host_port="18081",
            )

        self.assertEqual(
            compose_calls[0],
            (
                "forward-netbox-ui-test",
                "down --remove-orphans -v",
                {"FORWARD_NETBOX_HOST_PORT": "18081"},
            ),
        )
        self.assertEqual(
            compose_calls[1],
            (
                "forward-netbox-ui-test",
                "up -d --build --wait --wait-timeout 300 netbox",
                {"FORWARD_NETBOX_HOST_PORT": "18081"},
            ),
        )
        self.assertEqual(compose_calls[-1][0], "forward-netbox-ui-test")
        playwright_run.assert_called_once()
        playwright_env = playwright_run.call_args.kwargs["env"]
        self.assertEqual(playwright_env["NETBOX_URL"], "http://127.0.0.1:18081")
        self.assertEqual(
            playwright_env["PLAYWRIGHT_DOCKER_PROJECT_NAME"],
            "forward-netbox-ui-test",
        )
        self.assertEqual(
            playwright_env["PLAYWRIGHT_DOCKER_PROJECT_DIRECTORY"],
            "/tmp/forward-netbox",
        )

    def test_test_isolated_uses_separate_compose_project(self):
        context = self._context()
        compose_calls = []

        def fake_docker_compose(compose_context, command, **_kwargs):
            compose_calls.append((compose_context.forward_netbox.project_name, command))
            return SimpleNamespace(stdout="")

        with patch.object(tasks, "docker_compose", side_effect=fake_docker_compose):
            tasks.test_isolated.body(
                context,
                test_label="forward_netbox.tests.test_sync",
                project_name="forward-netbox-test",
                keep_runtime=True,
            )

        self.assertEqual(
            compose_calls[0], ("forward-netbox-test", "down --remove-orphans -v")
        )
        self.assertEqual(
            compose_calls[1], ("forward-netbox-test", "build netbox netbox-worker")
        )
        self.assertEqual(compose_calls[3][0], "forward-netbox-test")
        self.assertEqual(
            compose_calls[2], ("forward-netbox-test", "up -d postgres redis")
        )
        self.assertEqual(compose_calls[3][0], "forward-netbox-test")
        self.assertIn("exec -T postgres", compose_calls[3][1])
        self.assertIn("pg_isready", compose_calls[3][1])
        self.assertEqual(compose_calls[4][0], "forward-netbox-test")
        self.assertIn("run --rm -T netbox", compose_calls[4][1])
        self.assertIn("forward_netbox.tests.test_sync", compose_calls[4][1])
        self.assertEqual(len(compose_calls), 5)

    def test_test_isolated_can_remove_runtime_volume(self):
        context = self._context()
        compose_calls = []

        def fake_docker_compose(compose_context, command, **_kwargs):
            compose_calls.append((compose_context.forward_netbox.project_name, command))
            return SimpleNamespace(stdout="")

        with patch.object(tasks, "docker_compose", side_effect=fake_docker_compose):
            tasks.test_isolated.body(
                context,
                project_name="forward-netbox-test",
                keep_runtime=False,
            )

        self.assertEqual(
            compose_calls[-1], ("forward-netbox-test", "down --remove-orphans -v")
        )


class PruneCompatCacheTaskTest(unittest.TestCase):
    def _context(self):
        context = Mock()
        context.forward_netbox = SimpleNamespace(
            netbox_ver="v4.5.9",
            project_name="forward-netbox",
            compose_dir="/tmp/forward-netbox",
        )
        return context

    def test_prune_compat_cache_defaults_to_dry_run(self):
        context = self._context()
        with patch.object(tasks, "manage_py") as manage_py:
            tasks.prune_compat_cache.body(context)

        manage_py.assert_called_once()
        command = manage_py.call_args.args[1]
        self.assertIn("forward_prune_compatibility_cache", command)
        self.assertIn("--dry-run", command)

    def test_prune_compat_cache_allows_write_mode(self):
        context = self._context()
        with patch.object(tasks, "manage_py") as manage_py:
            tasks.prune_compat_cache.body(
                context,
                sync_name="ui-harness-sync",
                dry_run=False,
                output_json="docs/03_Plans/evidence/prune.json",
            )

        manage_py.assert_called_once()
        command = manage_py.call_args.args[1]
        self.assertIn(
            'forward_prune_compatibility_cache --sync-name "ui-harness-sync"',
            command,
        )
        self.assertIn('--output-json "docs/03_Plans/evidence/prune.json"', command)
        self.assertNotIn("--dry-run", command)


if __name__ == "__main__":
    unittest.main()
