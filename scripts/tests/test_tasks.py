from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from unittest.mock import patch

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


class DockerComposeIsolationTest(unittest.TestCase):
    def test_alternate_project_forces_project_scoped_postgres_volume(self):
        context = SimpleNamespace(
            run=Mock(),
            forward_netbox=SimpleNamespace(
                netbox_ver="v4.6.5",
                project_name="forward-netbox",
                compose_dir="/tmp/forward-netbox",
            ),
        )
        isolated = tasks._compose_project_context(context, "forward-netbox-test-ci")

        tasks.docker_compose(
            isolated,
            "config",
            env={"FORWARD_NETBOX_POSTGRES_DATA_PATH": "/shared/postgres"},
        )

        self.assertEqual(
            context.run.call_args.kwargs["env"]["FORWARD_NETBOX_POSTGRES_DATA_PATH"],
            "netbox-postgres-data",
        )
        self.assertIn(
            "--project-name forward-netbox-test-ci",
            context.run.call_args.args[0],
        )

    def test_alternate_project_rejects_shared_project_name(self):
        context = SimpleNamespace(
            run=Mock(),
            forward_netbox=SimpleNamespace(
                netbox_ver="v4.6.5",
                project_name="forward-netbox",
                compose_dir="/tmp/forward-netbox",
            ),
        )

        with self.assertRaises(Exit) as raised:
            tasks._compose_project_context(context, "forward-netbox")

        self.assertEqual(raised.exception.code, 2)
        context.run.assert_not_called()


class ReleaseArtifactTaskTest(unittest.TestCase):
    def _context(self, netbox_version="v4.6.5"):
        return SimpleNamespace(
            run=Mock(),
            forward_netbox=SimpleNamespace(
                netbox_ver=netbox_version,
                project_name="forward-netbox",
                compose_dir=str(tasks.REPO_ROOT / "development"),
            ),
        )

    def test_artifact_test_uses_wheel_without_source_fallback(self):
        context = self._context()
        wheel = tasks.REPO_ROOT / "dist/forward_netbox-2.6.0-py3-none-any.whl"

        with (
            patch.object(
                tasks,
                "_release_artifact_inputs",
                return_value=("2.6.0", wheel),
            ),
            patch.object(
                tasks,
                "_prepare_sbom_output",
                return_value=Path("/tmp/forward-netbox-2.6.0-runtime.cdx.json"),
            ),
            patch.object(tasks, "docker_compose") as docker_compose,
        ):
            tasks.artifact_test.body(context)

        commands = [call.args[0] for call in context.run.call_args_list]
        self.assertIn("--build-arg NETBOX_VER=v4.6.5", commands[0])
        self.assertIn(
            "--build-arg PACKAGE=/source/dist/forward_netbox-2.6.0-py3-none-any.whl",
            commands[0],
        )
        self.assertIn("rm -rf /source/forward_netbox", commands[1])
        self.assertIn("validate_installed_artifact.py", commands[1])
        self.assertIn("--env LOGLEVEL=WARNING", commands[1])
        self.assertIn("--tmpfs /var/log/netbox:rw,mode=1777", commands[1])
        self.assertIn("socket.create_connection", commands[1])
        self.assertIn("python manage.py migrate --noinput", commands[1])
        self.assertIn("python manage.py check", commands[1])
        self.assertIn(
            "python manage.py makemigrations --check --dry-run forward_netbox",
            commands[1],
        )
        self.assertIn("cyclonedx-bom==7.3.0", commands[2])
        self.assertIn("uv tool run --isolated", commands[2])
        self.assertIn("cyclonedx-py environment", commands[2])
        self.assertIn("--pyproject /tmp/netbox-runtime-pyproject.toml", commands[2])
        self.assertIn('version = "4.6.5"', commands[2])
        self.assertIn("forward-netbox==2.6.0", commands[2])
        self.assertIn("/opt/netbox/venv/bin/python", commands[2])
        self.assertIn("--output-reproducible", commands[2])
        self.assertIn("validate_sbom.py", commands[3])
        self.assertEqual(
            docker_compose.call_args_list[0].args[1], "up -d postgres redis"
        )
        self.assertEqual(
            docker_compose.call_args_list[-1].args[1],
            "down --volumes --remove-orphans",
        )

    def test_artifact_test_rejects_any_other_netbox_version(self):
        context = self._context(netbox_version="v4.6.4")
        wheel = tasks.REPO_ROOT / "dist/forward_netbox-2.6.0-py3-none-any.whl"

        with patch.object(
            tasks,
            "_release_artifact_inputs",
            return_value=("2.6.0", wheel),
        ):
            with self.assertRaises(Exit) as raised:
                tasks.artifact_test.body(context)

        self.assertEqual(raised.exception.code, 2)
        context.run.assert_not_called()

    def test_release_workflow_blocks_publish_on_artifact_and_pinned_sbom(self):
        workflow = (tasks.REPO_ROOT / ".github/workflows/release.yml").read_text()

        self.assertIn("poetry-core==2.4.1", workflow)
        self.assertIn("python -m invoke artifact-test", workflow)
        self.assertIn("sbom/", workflow)
        self.assertIn("gh release create", workflow)
        self.assertIn("needs: publish", workflow)
        self.assertNotRegex(workflow, r"uses:\s+[^\s#]+@(v\d+|release/)")
        self.assertNotIn("sbom-reqs.txt", workflow)
        self.assertNotIn("echo httpx", workflow)

        self.assertIn("npm ci", workflow)
        self.assertIn("playwright install --with-deps chromium", workflow)

    def test_package_uses_the_pinned_preinstalled_build_backend(self):
        context = Mock()

        tasks.package.body(context)

        command = context.run.call_args.args[0]
        self.assertIn("-m build --no-isolation", command)

    def test_artifact_wheel_is_present_in_the_docker_build_context(self):
        dockerignore = (tasks.REPO_ROOT / ".dockerignore").read_text().splitlines()

        self.assertIn("dist", dockerignore)
        self.assertIn("!dist/*.whl", dockerignore)
        self.assertGreater(
            dockerignore.index("!dist/*.whl"),
            dockerignore.index("dist"),
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

    def test_sync_health_monitor_fails_when_sync_is_failed(self):
        context = Mock()
        responses = [
            self._result(
                {
                    "sync_id": 50,
                    "sync_status": "failed",
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
                patch.object(tasks.sync_health_monitor, "body") as health_body,
                patch.object(tasks, "_manage_py_json_retry") as manage_json,
                patch.object(tasks, "Path") as path_cls,
            ):
                base_path = Path(tmp_dir)
                path_cls.side_effect = lambda value: (
                    base_path if str(value) == "docs/03_Plans/evidence" else Path(value)
                )
                manage_json.side_effect = [
                    {"release_ready": True},
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

            health_body.assert_called_once()
            self.assertEqual(manage_json.call_count, 5)
            summary_file = base_path / "unit-release-gate-summary.json"
            self.assertTrue(summary_file.exists())
            payload = json.loads(summary_file.read_text(encoding="utf-8"))
            self.assertEqual(payload["sync_ids"], [46, 50])

    def test_sync_release_gate_fails_on_warning_count(self):
        context = self._context()
        with (
            patch.object(tasks.sync_health_monitor, "body"),
            patch.object(
                tasks,
                "_manage_py_json_retry",
                side_effect=[
                    {"release_ready": True},
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


class RuntimeOptimizationTaskTest(unittest.TestCase):

    def _context(self):
        context = Mock()
        context.forward_netbox = SimpleNamespace(
            netbox_ver="v4.5.9",
            project_name="forward-netbox",
            compose_dir="/tmp/forward-netbox",
        )
        return context

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
        self.assertEqual(report["status"], "pass")
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

    def test_guard_blocks_tests_when_sync_is_active(self):
        context = self._context()
        payload = {
            "active_count": 1,
            "syncs": [
                {
                    "id": 119,
                    "name": "active-sync",
                    "status": "syncing",
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
        self.assertIn("Active Forward sync", str(raised.exception))
        self.assertIn("sync 119", str(raised.exception))
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
            payload = tasks._shared_runtime_active_syncs(context)

        self.assertFalse(payload["guard_available"])
        self.assertIn("too many clients", payload["reason"])

    def test_guard_blocks_tests_when_shared_runtime_probe_is_unavailable(self):
        context = self._context()
        with patch.object(
            tasks,
            "_shared_runtime_active_syncs",
            return_value={
                "active_count": 0,
                "syncs": [],
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

    def test_test_ci_always_uses_isolated_runtime(self):
        context = self._context()
        with (
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

    def test_test_ci_explicit_shared_runtime_override(self):
        context = self._context()
        with (
            patch.object(tasks, "manage_py") as manage_py,
            patch.object(tasks, "_run_tests_in_isolated_runtime") as isolated_run,
            patch.dict(
                os.environ,
                {tasks.ALLOW_SHARED_RUNTIME_TESTS_ENV: "1"},
                clear=False,
            ),
        ):
            tasks.test_ci.body(context)

        manage_py.assert_called_once_with(
            context,
            "test --keepdb --noinput forward_netbox.tests",
        )
        isolated_run.assert_not_called()

    def test_playwright_test_uses_shared_runtime_when_no_active_runs(self):
        context = self._context()
        with (
            patch.object(
                tasks,
                "_shared_runtime_active_syncs",
                return_value={"active_count": 0, "syncs": []},
            ),
            patch.object(tasks, "_run_playwright_ui") as playwright_run,
            patch.object(tasks, "_run_playwright_in_isolated_runtime") as isolated_run,
            patch.dict(os.environ, {}, clear=False),
        ):
            tasks.playwright_test.body(context)

        playwright_run.assert_called_once_with(context)
        isolated_run.assert_not_called()

    def test_playwright_ui_targets_selected_compose_runtime(self):
        context = self._context()

        tasks._run_playwright_ui(context)

        context.run.assert_called_once()
        self.assertEqual(context.run.call_args.args[0], "npm run test:ui")
        playwright_env = context.run.call_args.kwargs["env"]
        self.assertEqual(
            playwright_env["PLAYWRIGHT_DOCKER_PROJECT_NAME"],
            "forward-netbox",
        )
        self.assertEqual(
            playwright_env["PLAYWRIGHT_DOCKER_PROJECT_DIRECTORY"],
            "/tmp/forward-netbox",
        )

    def test_playwright_test_uses_isolated_runtime_when_guard_is_unavailable(self):
        context = self._context()
        with (
            patch.object(
                tasks,
                "_shared_runtime_active_syncs",
                return_value={
                    "active_count": 0,
                    "syncs": [],
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
                "_shared_runtime_active_syncs",
                return_value={"active_count": 2, "syncs": [{"id": 1}, {"id": 2}]},
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
                "up -d --build --wait --wait-timeout 600 netbox",
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
