import json
import sys
import types
from importlib import import_module
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock
from unittest.mock import patch

from django.test import TestCase

sys.modules.setdefault(
    "dotenv", types.SimpleNamespace(load_dotenv=lambda *args, **kwargs: None)
)

tasks = import_module("tasks")


class ForwardReleaseReadinessAuditTest(TestCase):
    def test_sync_release_gate_writes_strict_clean_evidence(self):
        context = Mock()
        with TemporaryDirectory() as temporary_directory, patch(
            "tasks.Path",
            return_value=Path(temporary_directory),
        ), patch.object(
            tasks.sync_health_monitor,
            "body",
        ) as health_monitor, patch(
            "tasks._manage_py_json_retry",
            side_effect=(
                {"release_ready": True},
                {
                    "warning_count": 0,
                    "suppressed_warning_count": 0,
                    "error_count": 0,
                },
                {"counts": {"blocking": 0}},
            ),
        ) as audit:
            tasks.sync_release_gate.body(
                context,
                sync_ids="23",
                max_polls=4,
                interval_seconds=2,
                output_prefix="release-test",
            )
            summary_path = Path(temporary_directory) / "release-test-summary.json"
            payload = json.loads(summary_path.read_text(encoding="utf-8"))

        self.assertEqual(payload["sync_ids"], [23])
        self.assertTrue(payload["ownership"]["release_ready"])
        self.assertEqual(payload["sync_results"][0]["blocking_count"], 0)
        health_monitor.assert_called_once_with(
            context,
            sync_ids="23",
            max_polls=4,
            interval_seconds=2,
            allow_nonterminal=False,
            include_all_ingestions=False,
            fail_on_suppressed_warning=True,
            output_json=str(Path(temporary_directory) / "release-test-health.json"),
        )
        self.assertEqual(audit.call_count, 3)
