from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from unittest.mock import patch

from invoke.exceptions import Exit

import tasks


class DockerChaosKillTaskTest(unittest.TestCase):
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
            patch.object(tasks, "_wait_for_chaos_scenario_ready") as wait_ready,
            patch.object(tasks, "_export_chaos_bundle") as export_bundle,
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


class ArchitectureAuditTaskTest(unittest.TestCase):
    def _context(self):
        context = Mock()
        context.forward_netbox = SimpleNamespace(
            netbox_ver="v4.5.9",
            project_name="forward-netbox",
            compose_dir="/tmp/forward-netbox",
        )
        return context

    def test_architecture_audit_passes_fail_on_gap_flag(self):
        context = self._context()
        with patch.object(tasks, "manage_py") as manage_py:
            tasks.architecture_audit.body(context, fail_on_gap=True)

        manage_py.assert_called_once()
        command = manage_py.call_args.args[1]
        self.assertIn("forward_architecture_audit", command)
        self.assertIn("--fail-on-gap", command)


class ArchitectureRuntimeEvidenceTaskTest(unittest.TestCase):
    def _context(self):
        context = Mock()
        context.forward_netbox = SimpleNamespace(
            netbox_ver="v4.5.9",
            project_name="forward-netbox",
            compose_dir="/tmp/forward-netbox",
        )
        return context

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
                patch.object(context, "run", side_effect=run_results),
            ):
                tasks.architecture_runtime_evidence.body(
                    context,
                    output_path=output_rel,
                    sync_name="ui-harness-sync",
                )

            docker_compose.assert_called_once()
            self.assertGreaterEqual(manage_py.call_count, 5)
            self.assertTrue(output_abs.exists())
            payload = output_abs.read_text(encoding="utf-8")
            self.assertIn("destructive_runtime_worker_kill_evidence_verified", payload)
            self.assertIn("adp_scale_runtime_matrix_verified", payload)
            output_abs.unlink(missing_ok=True)

    def test_run_adp_runtime_matrix_reports_missing_env(self):
        with patch.dict(os.environ, {}, clear=True):
            evidence, status = tasks._run_adp_runtime_matrix(self._context())
        self.assertEqual(status, "missing-env")
        self.assertEqual(evidence["status"], "failed")
        self.assertIn("missing", evidence["evidence"])

    def test_run_adp_runtime_matrix_collects_sanitized_run_results(self):
        context = self._context()
        run_results = [
            SimpleNamespace(ok=True, exited=0),
            SimpleNamespace(ok=True, exited=0),
            SimpleNamespace(ok=True, exited=0),
        ]
        with (
            patch.dict(
                os.environ,
                {
                    "FORWARD_SMOKE_USERNAME": "user",
                    "FORWARD_SMOKE_PASSWORD": "secret",
                    "FORWARD_SMOKE_NETWORK_ID": "123",
                },
                clear=True,
            ),
            patch.object(tasks, "manage_py", side_effect=run_results),
        ):
            evidence, status = tasks._run_adp_runtime_matrix(context)

        self.assertEqual(status, "completed")
        self.assertEqual(evidence["status"], "passed")
        self.assertEqual(len(evidence["evidence"]["runs"]), 3)
        self.assertNotIn("secret", str(evidence))

    def test_architecture_audit_check_uses_strict_mode(self):
        context = self._context()
        with patch.object(tasks, "manage_py") as manage_py:
            tasks.architecture_audit_check.body(context)

        manage_py.assert_called_once()
        command = manage_py.call_args.args[1]
        self.assertIn("forward_architecture_audit", command)
        self.assertIn("--fail-on-gap", command)


if __name__ == "__main__":
    unittest.main()
