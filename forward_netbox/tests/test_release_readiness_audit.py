import os
import sys
import types
from importlib import import_module
from unittest.mock import Mock
from unittest.mock import patch

from django.test import TestCase

sys.modules.setdefault(
    "dotenv", types.SimpleNamespace(load_dotenv=lambda *args, **kwargs: None)
)

tasks = import_module("tasks")


class ForwardReleaseReadinessAuditTest(TestCase):
    def test_release_runtime_preflight_passes_with_source_backed_smoke_config(self):
        context = Mock()
        with patch.dict(
            os.environ,
            {
                "FORWARD_SMOKE_SOURCE_NAME": "smoke-source-release-smoke-20260601",
                "FORWARD_SMOKE_DATASET_LABEL": "release-smoke",
                "FORWARD_SMOKE_USERNAME": "",
                "FORWARD_SMOKE_PASSWORD": "",
                "FORWARD_SMOKE_NETWORK_ID": "",
            },
            clear=False,
        ), patch(
            "tasks._field_scale_runtime_preflight",
            return_value={"ok": True},
        ):
            payload = tasks._collect_release_runtime_preflight_evidence(
                context=context,
                dataset_label="release-smoke",
            )

        self.assertEqual(payload["status"], "passed")
        evidence = payload["evidence"]
        self.assertTrue(evidence["source_backed"])
        self.assertEqual(evidence["source_name"], "smoke-source-release-smoke-20260601")
        self.assertTrue(evidence["dataset_label_matches"])
        self.assertEqual(evidence["missing_env"], [])
        self.assertEqual(
            sorted(evidence["credential_env_missing"]),
            [
                "FORWARD_SMOKE_NETWORK_ID",
                "FORWARD_SMOKE_PASSWORD",
                "FORWARD_SMOKE_USERNAME",
            ],
        )
