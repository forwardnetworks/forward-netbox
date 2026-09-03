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

    def test_resolve_query_ids_uses_aggregate_management_command(self):
        context = Mock()

        with patch.object(tasks, "manage_py") as manage_py:
            tasks.resolve_query_ids.body(context, sync_id=23)

        manage_py.assert_called_once_with(
            context,
            "forward_resolve_query_ids --sync-id 23",
        )


class DockerComposeIsolationTest(unittest.TestCase):
    def test_alternate_project_forces_project_scoped_postgres_volume(self):
        context = SimpleNamespace(
            run=Mock(),
            forward_netbox=SimpleNamespace(
                netbox_ver="v4.7.0",
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
                netbox_ver="v4.7.0",
                project_name="forward-netbox",
                compose_dir="/tmp/forward-netbox",
            ),
        )

        with self.assertRaises(Exit) as raised:
            tasks._compose_project_context(context, "forward-netbox")

        self.assertEqual(raised.exception.code, 2)
        context.run.assert_not_called()


class ReleaseArtifactTaskTest(unittest.TestCase):
    def _context(self, netbox_version="v4.7.0"):
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
        self.assertIn("--build-arg NETBOX_VER=v4.7.0", commands[0])
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
        self.assertIn("validate_installed_routes.py", commands[1])
        self.assertIn("cyclonedx-bom==7.3.0", commands[2])
        self.assertIn("uv tool run --isolated", commands[2])
        self.assertIn("cyclonedx-py environment", commands[2])
        self.assertIn("--pyproject /tmp/netbox-runtime-pyproject.toml", commands[2])
        self.assertIn('version = "4.7.0"', commands[2])
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
        context = self._context(netbox_version="v4.6.8")
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

        self.assertIn("--require-hashes", workflow)
        self.assertIn("requirements-release.txt", workflow)
        self.assertIn("refs/tags/v2.9.1", workflow)
        self.assertIn("scripts/build_reproducible_distribution.py", workflow)
        self.assertIn("python -m invoke artifact-test", workflow)
        self.assertIn("sbom/", workflow)
        self.assertIn("gh release create", workflow)
        self.assertIn("needs: publish", workflow)
        self.assertNotRegex(workflow, r"uses:\s+[^\s#]+@(v\d+|release/)")
        self.assertNotIn("sbom-reqs.txt", workflow)
        self.assertNotIn("echo httpx", workflow)

        self.assertIn("npm ci", workflow)
        self.assertIn("playwright install --with-deps chromium", workflow)
        self.assertNotIn("pip install --upgrade", workflow)

    def test_package_requires_reproducible_distribution_builder(self):
        context = Mock()

        tasks.package.body(context)

        command = context.run.call_args.args[0]
        self.assertIn("scripts/build_reproducible_distribution.py", command)

    def test_artifact_wheel_is_present_in_the_docker_build_context(self):
        dockerignore = (tasks.REPO_ROOT / ".dockerignore").read_text().splitlines()

        self.assertIn("dist", dockerignore)
        self.assertIn("!dist/*.whl", dockerignore)
        self.assertGreater(
            dockerignore.index("!dist/*.whl"),
            dockerignore.index("dist"),
        )


class PreviousReleasedVersionTest(unittest.TestCase):
    """Cover the index resolution itself, against a fixed payload.

    The three upgrade-task tests stub this helper so the release gate does not
    depend on a live PyPI request. The selection rules still need coverage, so
    they are exercised here without a socket.
    """

    PAYLOAD = {
        "releases": {
            "2.6.6": [{"yanked": False}],
            "2.6.5": [{"yanked": False}],
            "2.6.9": [{"yanked": False}],
            "2.6.4": [{"yanked": True}],
            "2.6.3": [],
            "2.6.2.post1": [{"yanked": False}],
        }
    }

    def _resolve(self, version, payload=None):
        import contextlib
        import io

        body = json.dumps(self.PAYLOAD if payload is None else payload)

        @contextlib.contextmanager
        def fake_urlopen(url, timeout=None):
            yield io.StringIO(body)

        with patch("urllib.request.urlopen", fake_urlopen):
            return tasks._previous_released_version(Mock(), version)

    def test_a_dev_marker_resolves_from_the_release_it_heads_for(self):
        # `main` is deliberately moved onto a `.dev0` marker after a release so
        # an install from it is not indistinguishable from the published one.
        # Splitting on "." and calling int() crashed on exactly that
        # (`invalid literal for int() with base 10: 'dev0'`), so the marker and
        # this gate could not coexist and `main` kept being left on the released
        # version. A dev tree upgrades from the newest release below the version
        # it is heading for.
        self.assertEqual(self._resolve("2.6.7.dev0"), "2.6.6")

    def test_a_dev_marker_does_not_sort_lexicographically(self):
        payload = {
            "releases": {"2.6.9": [{"yanked": False}], "2.6.10": [{"yanked": False}]}
        }
        self.assertEqual(self._resolve("2.6.11.dev0", payload), "2.6.10")

    def test_an_unparseable_version_fails_closed(self):
        # Better to stop and ask for --from-version than to resolve an upgrade
        # source from a version nobody can read.
        with self.assertRaises(Exit):
            self._resolve("not-a-version")

    def test_picks_the_highest_release_below_the_target(self):
        # 2.6.9 is published but above the target, so it is not what an
        # operator upgrading to 2.6.7 could be coming from.
        self.assertEqual(self._resolve("2.6.7"), "2.6.6")

    def test_skips_yanked_and_fileless_and_non_three_part_entries(self):
        # 2.6.4 is yanked, 2.6.3 has no files, 2.6.2.post1 is not a three-part
        # version: the highest installable below 2.6.6 is 2.6.5.
        self.assertEqual(self._resolve("2.6.6"), "2.6.5")

    def test_fails_closed_when_nothing_is_installable_below_the_target(self):
        with self.assertRaises(Exit) as raised:
            self._resolve("2.0.0")
        self.assertEqual(raised.exception.code, 2)

    def test_a_connection_reset_reports_how_to_run_offline(self):
        # A reset during the read is not a URLError; it used to escape raw and
        # fail the release gate with a bare socket error.
        import contextlib

        @contextlib.contextmanager
        def reset(url, timeout=None):
            raise ConnectionResetError(104, "Connection reset by peer")
            yield  # pragma: no cover

        with patch("urllib.request.urlopen", reset):
            with self.assertRaises(Exit) as raised:
                tasks._previous_released_version(Mock(), "2.6.7")
        self.assertEqual(raised.exception.code, 2)
        self.assertIn("--from-version", str(raised.exception))


class ArtifactUpgradeTaskTest(unittest.TestCase):
    """The upgrade gate: a clean install cannot show an upgrade defect.

    `artifact-test` migrates an empty database, so a migration that drops a
    column, a default that never backfills, or a field whose meaning changed all
    pass it and break a real deployment. This gate seeds rows under the previous
    release and reads them back under the built wheel.
    """

    def setUp(self):
        # The gate driver exports FORWARD_NETBOX_UPGRADE_FROM_VERSION for the
        # whole `invoke ci` run so the upgrade gate can resolve offline when
        # PyPI is unreachable. These tests must not inherit it: one of them is
        # named for not touching the network, and both assert on the version
        # the task RESOLVES, which an ambient override silently replaces.
        patcher = patch.dict(
            os.environ, {"FORWARD_NETBOX_UPGRADE_FROM_VERSION": ""}, clear=False
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    WHEEL = "dist/forward_netbox-2.6.7-py3-none-any.whl"

    def _context(self, netbox_version="v4.7.0", tags="v2.6.5\nv2.6.6\n"):
        run = Mock(return_value=SimpleNamespace(stdout=tags))
        return SimpleNamespace(
            run=run,
            forward_netbox=SimpleNamespace(
                netbox_ver=netbox_version,
                project_name="forward-netbox",
                compose_dir=str(tasks.REPO_ROOT / "development"),
            ),
        )

    def _run(self, context, **kwargs):
        # Stub the index lookup. Resolving it for real made these tests - and
        # therefore the release gate - depend on a live PyPI request, which
        # failed twice in one release with a read timeout and a connection
        # reset. The resolution logic itself is covered against a fixed payload
        # by PreviousReleasedVersionTest below.
        with (
            patch.object(
                tasks,
                "_release_artifact_inputs",
                return_value=("2.6.7", tasks.REPO_ROOT / self.WHEEL),
            ),
            patch.object(tasks, "_previous_released_version", return_value="2.6.6"),
            patch.object(tasks, "docker_compose") as docker_compose,
        ):
            tasks.artifact_upgrade_test.body(context, **kwargs)
        return [call.args[0] for call in context.run.call_args_list], docker_compose

    def test_builds_the_previous_release_then_the_built_wheel(self):
        context = self._context()
        commands, docker_compose = self._run(context)

        builds = [command for command in commands if command.startswith("docker build")]
        self.assertIn("--build-arg PACKAGE=forward-netbox==2.6.6", builds[0])
        self.assertIn(
            "--build-arg PACKAGE=/source/dist/forward_netbox-2.6.7-py3-none-any.whl",
            builds[1],
        )
        self.assertEqual(
            docker_compose.call_args_list[0].args[1], "up -d postgres redis"
        )
        self.assertEqual(
            docker_compose.call_args_list[-1].args[1],
            "down --volumes --remove-orphans",
        )

    def test_seeds_under_the_old_release_and_verifies_under_the_new(self):
        context = self._context()
        commands, _ = self._run(context)

        runs = [command for command in commands if command.startswith("docker run")]
        self.assertIn("validate_upgrade_state.py --mode seed", runs[0])
        self.assertNotIn("--mode verify", runs[0])
        self.assertIn("validate_upgrade_state.py --mode verify", runs[1])
        # The upgraded run must prove it is the built wheel, migrate on top of
        # the seeded database, and leave no unmade migrations behind.
        self.assertIn("validate_installed_artifact.py", runs[1])
        self.assertIn("--expected-version 2.6.7", runs[1])
        self.assertIn("python manage.py migrate --noinput", runs[1])
        self.assertIn(
            "python manage.py makemigrations --check --dry-run forward_netbox",
            runs[1],
        )
        self.assertIn("validate_installed_routes.py", runs[1])

    def test_runs_in_its_own_compose_project(self):
        # Sharing the artifact-test project would let a clean-install run and an
        # upgrade run collide over the same database volume.
        context = self._context()
        commands, _ = self._run(context)

        runs = [command for command in commands if command.startswith("docker run")]
        for command in runs:
            self.assertIn("forward-netbox-artifact-upgrade_default", command)
            self.assertNotIn("forward-netbox-artifact-test_default", command)

    def test_honours_an_explicit_from_version(self):
        context = self._context()
        commands, _ = self._run(context, from_version="2.6.4")

        builds = [command for command in commands if command.startswith("docker build")]
        self.assertIn("--build-arg PACKAGE=forward-netbox==2.6.4", builds[0])

    def test_resolution_is_covered_without_touching_the_network(self):
        # Guard the stub in _run: if the index lookup ever stops being patched
        # there, the release gate silently depends on a live PyPI request again.
        context = self._context()
        with (
            patch.object(
                tasks,
                "_release_artifact_inputs",
                return_value=("2.6.7", tasks.REPO_ROOT / self.WHEEL),
            ),
            patch.object(tasks, "docker_compose"),
            patch.object(tasks, "_previous_released_version") as resolve,
        ):
            resolve.return_value = "2.6.6"
            tasks.artifact_upgrade_test.body(context)
        resolve.assert_called_once()

    def test_rejects_upgrading_a_version_from_itself(self):
        context = self._context()
        with (
            patch.object(
                tasks,
                "_release_artifact_inputs",
                return_value=("2.6.7", tasks.REPO_ROOT / self.WHEEL),
            ),
            patch.object(tasks, "docker_compose"),
        ):
            with self.assertRaises(Exit) as raised:
                tasks.artifact_upgrade_test.body(context, from_version="2.6.7")
        self.assertEqual(raised.exception.code, 2)

    def test_rejects_any_other_netbox_version(self):
        context = self._context(netbox_version="v4.6.8")
        with patch.object(
            tasks,
            "_release_artifact_inputs",
            return_value=("2.6.7", tasks.REPO_ROOT / self.WHEEL),
        ):
            with self.assertRaises(Exit) as raised:
                tasks.artifact_upgrade_test.body(context)
        self.assertEqual(raised.exception.code, 2)
        context.run.assert_not_called()

    def _pypi(self, *versions, yanked=()):
        import io
        import json

        payload = {
            "releases": {
                v: [{"filename": f"forward_netbox-{v}.whl", "yanked": v in yanked}]
                for v in versions
            }
        }
        return io.BytesIO(json.dumps(payload).encode())

    def test_previous_version_picks_the_highest_published_below_the_build(self):
        with patch(
            "urllib.request.urlopen",
            return_value=self._pypi("2.5.11", "2.6.5", "2.6.6", "2.6.9"),
        ) as urlopen:
            urlopen.return_value.__enter__ = lambda s: s
            urlopen.return_value.__exit__ = lambda *a: None
            self.assertEqual(
                tasks._previous_released_version(self._context(), "2.6.9"), "2.6.6"
            )

    def test_previous_version_ignores_a_version_that_was_never_published(self):
        # The defect this replaced: v2.6.7 and v2.6.8 are git tags with no PyPI
        # artifact, so resolving from tags produced an uninstallable version and
        # the release failed *after* the tag was pushed.
        with patch(
            "urllib.request.urlopen", return_value=self._pypi("2.6.5", "2.6.6")
        ) as urlopen:
            urlopen.return_value.__enter__ = lambda s: s
            urlopen.return_value.__exit__ = lambda *a: None
            self.assertEqual(
                tasks._previous_released_version(self._context(), "2.6.9"), "2.6.6"
            )

    def test_previous_version_skips_a_yanked_release(self):
        with patch(
            "urllib.request.urlopen",
            return_value=self._pypi("2.6.5", "2.6.6", yanked=("2.6.6",)),
        ) as urlopen:
            urlopen.return_value.__enter__ = lambda s: s
            urlopen.return_value.__exit__ = lambda *a: None
            self.assertEqual(
                tasks._previous_released_version(self._context(), "2.6.9"), "2.6.5"
            )

    def test_previous_version_orders_numerically_not_lexically(self):
        with patch(
            "urllib.request.urlopen", return_value=self._pypi("2.5.9", "2.5.11")
        ) as urlopen:
            urlopen.return_value.__enter__ = lambda s: s
            urlopen.return_value.__exit__ = lambda *a: None
            self.assertEqual(
                tasks._previous_released_version(self._context(), "2.6.0"), "2.5.11"
            )

    def test_previous_version_fails_closed_with_nothing_published(self):
        with patch(
            "urllib.request.urlopen", return_value=self._pypi("2.7.0")
        ) as urlopen:
            urlopen.return_value.__enter__ = lambda s: s
            urlopen.return_value.__exit__ = lambda *a: None
            with self.assertRaises(Exit) as raised:
                tasks._previous_released_version(self._context(), "2.6.9")
        self.assertEqual(raised.exception.code, 2)

    def test_previous_version_fails_closed_when_pypi_is_unreachable(self):
        import urllib.error

        with patch(
            "urllib.request.urlopen", side_effect=urllib.error.URLError("offline")
        ):
            with self.assertRaises(Exit) as raised:
                tasks._previous_released_version(self._context(), "2.6.9")
        self.assertEqual(raised.exception.code, 2)


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

    def test_local_ci_uses_current_scenario_tests(self):
        # This asserted the same thing about `.github/workflows/ci.yml` until
        # the CI gates were removed. The local `ci` task is the gate now, so the
        # scenario labels have to reach it instead.
        repo_root = Path(__file__).resolve().parents[2]
        tasks_source = (repo_root / "tasks.py").read_text(encoding="utf-8")
        pre_list = tasks_source.rsplit("@task(", 1)[-1].split("def ci(", 1)[0]

        self.assertIn("scenario_test_ci", pre_list)
        self.assertNotIn("test_synthetic_scenarios", tasks.SCENARIO_TEST_LABELS)

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

    def test_playwright_test_always_uses_isolated_runtime(self):
        context = self._context()
        with (
            patch.object(tasks, "_run_playwright_ui") as playwright_run,
            patch.object(tasks, "_run_playwright_in_isolated_runtime") as isolated_run,
            patch.dict(os.environ, {}, clear=False),
        ):
            tasks.playwright_test.body(context)

        playwright_run.assert_not_called()
        isolated_run.assert_called_once_with(context)

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
        self.assertEqual(playwright_env["FORWARD_UI_HARNESS_ISOLATED"], "true")
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
        self.assertIn("REDIS_DATABASE=14", compose_calls[4][1])
        self.assertIn("REDIS_CACHE_DATABASE=15", compose_calls[4][1])
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


class UpgradeFromConstraintsTests(unittest.TestCase):
    """The two constraint sets must differ only in the branching pin.

    The upgrade gate's from side needs its own constraints because releases
    before 2.6.7 pin `netboxlabs-netbox-branching==1.1.1` exactly, which the
    current pin of 1.1.3 cannot satisfy. That is the only difference the
    upgrade is meant to exercise; anything else drifting between the two sides
    would silently change what the gate compares.
    """

    @staticmethod
    def _pins(path):
        pins = {}
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            name, _, pinned = line.partition("==")
            pins[name.strip().lower()] = pinned.strip()
        return pins

    def setUp(self):
        self.current = self._pins(tasks.REPO_ROOT / "constraints.txt")
        self.upgrade_from = self._pins(
            tasks.REPO_ROOT / "development/constraints-upgrade-from.txt"
        )

    def test_branching_is_absent_from_the_upgrade_source_constraints(self):
        self.assertIn("netboxlabs-netbox-branching", self.current)
        self.assertNotIn("netboxlabs-netbox-branching", self.upgrade_from)

    def test_the_optional_plugin_pin_may_lag_on_the_upgrade_source(self):
        # A release that widens an optional plugin's supported range cannot pin
        # the new version on the FROM side: the previous release's own metadata
        # caps it, so the install would be unsatisfiable rather than stale. The
        # pin must still be present - dropping it entirely would let the
        # resolver pick anything - and must not run ahead of the current one.
        self.assertIn("netbox-dlm", self.upgrade_from)
        self.assertLessEqual(
            tuple(int(part) for part in self.upgrade_from["netbox-dlm"].split(".")),
            tuple(int(part) for part in self.current["netbox-dlm"].split(".")),
        )

    def test_a_from_release_seeds_on_a_runtime_it_can_actually_run_on(self):
        """2.8.0 cannot be installed on the default from-side NetBox.

        Its migration `0052_device_absence_quarantine` depends on
        `dcim.0241_nullify_empty_cable_end`, a 4.6.6 migration, so the graph
        will not build on 4.6.5 - despite the plugin declaring
        `min_version = "4.6.5"`. 2.8.1 fixes the migration; the published 2.8.0
        wheel cannot be fixed, so the upgrade gate seeds it where it runs.

        Keyed by release rather than set globally on purpose: a blanket override
        would also move the scenario suite's upgrade fixtures off 4.6.5 and drop
        the NetBox upgrade path they exercise, which is real coverage and
        unrelated to this one broken release.
        """
        self.assertEqual(tasks.UPGRADE_FROM_NETBOX_OVERRIDES.get("2.8.0"), "v4.6.6")
        self.assertEqual(tasks.UPGRADE_FROM_NETBOX_VER, "v4.6.5")

    def test_only_releases_that_need_an_override_carry_one(self):
        # Each entry costs the 4.6.5 seeding for that release, so the list stays
        # short and every addition has to be justified where it is written.
        self.assertEqual(set(tasks.UPGRADE_FROM_NETBOX_OVERRIDES), {"2.8.0"})

    def test_every_other_pin_is_identical(self):
        expected = {
            name: pin
            for name, pin in self.current.items()
            if name not in ("netboxlabs-netbox-branching", "netbox-dlm")
        }
        self.assertEqual(
            expected,
            {
                name: pin
                for name, pin in self.upgrade_from.items()
                if name != "netbox-dlm"
            },
        )
