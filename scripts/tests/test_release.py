from __future__ import annotations

import importlib.util
import tempfile
import unittest.mock
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

_SPEC = importlib.util.spec_from_file_location(
    "release_tool", Path(__file__).resolve().parents[1] / "release.py"
)
release = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(release)


class RunTest(unittest.TestCase):
    def test_command_arguments_are_absent_from_logs_and_errors(self):
        secret = "secret-command-argument"
        output = StringIO()
        result = release.subprocess.CompletedProcess([], 7)

        with (
            patch.object(release.subprocess, "run", return_value=result),
            redirect_stdout(output),
            self.assertRaises(release.ReleaseError) as error,
        ):
            release.run(["gh", "api", "--field", secret])

        self.assertEqual(output.getvalue(), "  $ [redacted release command]\n")
        self.assertNotIn(secret, output.getvalue())
        self.assertNotIn(secret, str(error.exception))

    def test_required_capture_redacts_failed_command(self):
        secret = "secret-command-argument"
        result = release.subprocess.CompletedProcess([], 22, stdout="", stderr=secret)

        with (
            patch.object(release.subprocess, "run", return_value=result),
            self.assertRaises(release.ReleaseError) as error,
        ):
            release._capture_required(
                ["gh", "api", "--field", secret],
                purpose="GitHub workflow query",
            )

        self.assertNotIn(secret, str(error.exception))
        self.assertEqual(
            str(error.exception),
            "GitHub workflow query failed with exit code 22",
        )


class BumpVersionTest(unittest.TestCase):
    def test_bumps_single_assignment(self):
        out = release.bump_version_text(
            'name = "x"\nversion = "1.5.10"\n', "1.5.10", "1.5.11", key="version"
        )
        self.assertIn('version = "1.5.11"', out)

    def test_raises_when_old_version_absent(self):
        with self.assertRaises(release.ReleaseError):
            release.bump_version_text(
                'version = "9.9.9"', "1.5.10", "1.5.11", key="version"
            )


class InsertReleaseRowTest(unittest.TestCase):
    TABLE = (
        "| Plugin Release | NetBox Version | Status |\n"
        "| --- | --- | --- |\n"
        "| `v1.5.10` | `4.5.9` and `4.6.2` validated | Current release; did a thing. |\n"
        "| `v1.5.9` | `4.5.9` and `4.6.2` validated | Superseded by `v1.5.10`; older. |\n"
    )

    def test_inserts_new_current_row_and_demotes_prior(self):
        out = release.insert_release_row(self.TABLE, "1.5.11", "new feature.")
        lines = out.splitlines()
        # New candidate row first, reusing the support cell while the published
        # current release remains authoritative until finalization.
        self.assertIn("| `v1.5.11` |", lines[2])
        self.assertIn("Release candidate; new feature.", lines[2])
        self.assertIn("`4.5.9` and `4.6.2` validated", lines[2])
        self.assertIn("| `v1.5.10` |", lines[3])
        self.assertIn("Current release; did a thing.", lines[3])
        self.assertEqual(out.count("Current release;"), 1)
        self.assertEqual(out.count("Release candidate;"), 1)

    def test_promotes_candidate_and_demotes_published_release(self):
        candidate = release.insert_release_row(self.TABLE, "1.5.11", "new feature.")

        out = release.promote_release_candidate_text(candidate, "1.5.11")

        self.assertIn("| `v1.5.11` |", out)
        self.assertIn("Current release; new feature.", out)
        self.assertIn("Superseded by `v1.5.11`; did a thing.", out)
        self.assertNotIn("Release candidate;", out)
        self.assertEqual(out.count("Current release;"), 1)

        self.assertEqual(
            release.promote_release_candidate_text(out, "1.5.11"),
            out,
        )

    def test_raises_without_current_row(self):
        with self.assertRaises(release.ReleaseError):
            release.insert_release_row("no current row here", "1.5.11", "x")

    def test_rejects_second_candidate(self):
        candidate = release.insert_release_row(self.TABLE, "1.5.11", "first")

        with self.assertRaises(release.ReleaseError):
            release.insert_release_row(candidate, "1.5.12", "second")


class ReleaseIntroTest(unittest.TestCase):
    INTRO = (
        "The `1.5.10` release requires NetBox `4.6.6`. "
        "Expand for the published release history and release notes."
    )

    def test_prepare_sets_candidate_version_and_wording(self):
        out = release.set_release_intro_text(
            self.INTRO,
            "1.5.11",
            candidate=True,
        )

        self.assertEqual(
            out,
            "The `1.5.11` release candidate requires NetBox `4.6.6`. "
            "Expand for the published release history and candidate notes.",
        )

    def test_promotion_sets_published_wording_and_is_idempotent(self):
        candidate = release.set_release_intro_text(
            self.INTRO,
            "1.5.11",
            candidate=True,
        )

        out = release.set_release_intro_text(
            candidate,
            "1.5.11",
            candidate=False,
        )

        self.assertEqual(
            out,
            "The `1.5.11` release requires NetBox `4.6.6`. "
            "Expand for the published release history and release notes.",
        )
        self.assertEqual(
            release.set_release_intro_text(out, "1.5.11", candidate=False),
            out,
        )

    def test_rejects_missing_canonical_intro(self):
        with self.assertRaises(release.ReleaseError):
            release.set_release_intro_text(
                "No release compatibility introduction.",
                "1.5.11",
                candidate=True,
            )

    def test_rejects_duplicate_canonical_intro(self):
        with self.assertRaises(release.ReleaseError):
            release.set_release_intro_text(
                f"{self.INTRO}\n\n{self.INTRO}",
                "1.5.11",
                candidate=True,
            )


class SemverArgTest(unittest.TestCase):
    def test_semver_regex(self):
        self.assertIsNotNone(release.SEMVER_RE.match("1.5.11"))
        self.assertIsNone(release.SEMVER_RE.match("1.5"))
        self.assertIsNone(release.SEMVER_RE.match("v1.5.11"))


class DistributionArtifactTest(unittest.TestCase):
    def test_selects_only_exact_current_wheel_and_sdist(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            dist = root / "dist"
            dist.mkdir()
            wheel = dist / "forward_netbox-2.6.0-py3-none-any.whl"
            sdist = dist / "forward_netbox-2.6.0.tar.gz"
            wheel.touch()
            sdist.touch()
            (dist / "forward_netbox-2.5.11-py3-none-any.whl").touch()
            (dist / "unrelated.txt").touch()

            with patch.object(release, "REPO_ROOT", root):
                self.assertEqual(
                    release.release_distribution_artifacts("2.6.0"),
                    [wheel, sdist],
                )

    def test_rejects_incomplete_current_artifact_set(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            dist = root / "dist"
            dist.mkdir()
            (dist / "forward_netbox-2.6.0-py3-none-any.whl").touch()

            with (
                patch.object(release, "REPO_ROOT", root),
                self.assertRaises(release.ReleaseError),
            ):
                release.release_distribution_artifacts("2.6.0")


class FinishReleaseTest(unittest.TestCase):
    @patch.object(release, "run")
    def test_live_release_controls_use_redacted_environment_token(self, run):
        with patch.object(release, "_capture", return_value="secret-token"):
            release._verify_live_release_controls()

        command = run.call_args.args[0]
        self.assertIn("--controls-only", command)
        self.assertEqual(run.call_args.kwargs["env"]["GH_TOKEN"], "secret-token")

    @patch.object(release, "_open_release_pull_request")
    @patch.object(release, "_assert_branch_head")
    @patch.object(release, "run")
    def test_finish_on_the_production_branch_does_not_promote(
        self, run, _assert_head, open_pull_request
    ):
        """The tables flip in the anchor commit, after the tag - never here.

        `--finish` used to commit `release: promote` on the release branch and
        run the harness against it, which refused it by construction on every
        release since 2.8.6: the harness ties the anchor to the release the
        table calls current, and the anchor cannot move until the bridge
        exists, which cannot exist until the tag does.
        """
        with patch.object(release, "_capture", return_value="release/2.6.0"):
            release.stage_finish("2.6.0")

        commands = [" ".join(call.args[0]) for call in run.call_args_list]
        self.assertFalse(
            any("commit" in command for command in commands),
            commands,
        )
        self.assertFalse(
            any("gen_changelog" in command for command in commands),
            commands,
        )
        self.assertTrue(any("check_harness.py" in command for command in commands))
        self.assertTrue(any(command.startswith("git push") for command in commands))
        open_pull_request.assert_called_once_with(
            "2.6.0", "release/2.6.0", evidence=False
        )

    def test_the_promotion_helper_no_longer_exists(self):
        # Its commit-push-wait body is what stranded a `release: promote` on
        # local main and on the release branch; only the file edit survives.
        self.assertFalse(hasattr(release, "_promote_release_candidate"))

    def test_release_head_requires_exact_local_and_remote_commit(self):
        expected = "a" * 40

        def capture(command):
            if command == ["git", "branch", "--show-current"]:
                return "release/2.6.0"
            if command == ["git", "rev-parse", "HEAD"]:
                return expected
            if command == [
                "git",
                "ls-remote",
                "--heads",
                "origin",
                "release/2.6.0",
            ]:
                return f"{expected}\trefs/heads/release/2.6.0"
            raise AssertionError(command)

        with patch.object(release, "_capture", side_effect=capture):
            release._assert_release_head("2.6.0", expected)

    @patch.object(release, "_verify_live_release_controls")
    @patch.object(release, "run")
    def test_tag_creation_uses_standard_annotated_tag_flow(
        self,
        run,
        verify_controls,
    ):
        expected = "a" * 40
        with patch.object(
            release,
            "_capture",
            side_effect=[
                "",
                "",
                (
                    f"{'f' * 40}\trefs/tags/v2.6.0\n"
                    f"{expected}\trefs/tags/v2.6.0^{{}}"
                ),
            ],
        ):
            release.ensure_release_tag("v2.6.0", expected)

        verify_controls.assert_called_once_with()
        commands = [call.args[0] for call in run.call_args_list]
        self.assertIn(
            [
                "git",
                "tag",
                "-a",
                "v2.6.0",
                expected,
                "-m",
                "Forward NetBox v2.6.0",
            ],
            commands,
        )
        self.assertIn(
            ["git", "push", "origin", "refs/tags/v2.6.0"],
            commands,
        )

    @patch.object(release, "run")
    def test_existing_release_tag_must_be_annotated(self, run):
        expected = "a" * 40
        with patch.object(
            release,
            "_capture",
            side_effect=[
                expected,
                (
                    f"{'f' * 40}\trefs/tags/v2.6.0\n"
                    f"{expected}\trefs/tags/v2.6.0^{{}}"
                ),
                "commit",
            ],
        ):
            with self.assertRaisesRegex(release.ReleaseError, "annotated"):
                release.ensure_release_tag("v2.6.0", expected)

    @patch.object(release, "_verify_live_release_controls")
    @patch.object(release, "run")
    def test_remote_release_tag_must_peel_to_expected_commit(
        self,
        run,
        verify_controls,
    ):
        expected = "a" * 40
        with (
            patch.object(
                release,
                "_capture",
                side_effect=[
                    "",
                    "",
                    (
                        f"{'f' * 40}\trefs/tags/v2.6.0\n"
                        f"{'b' * 40}\trefs/tags/v2.6.0^{{}}"
                    ),
                ],
            ),
            self.assertRaisesRegex(release.ReleaseError, "does not peel"),
        ):
            release.ensure_release_tag("v2.6.0", expected)

        verify_controls.assert_called_once_with()

    @patch.object(release, "_verify_live_release_controls")
    @patch.object(release, "run")
    def test_local_only_tag_is_pushed_on_retry(self, run, verify_controls):
        expected = "a" * 40
        with patch.object(
            release,
            "_capture",
            side_effect=[
                expected,
                "",
                "tag",
                (
                    f"{'f' * 40}\trefs/tags/v2.6.0\n"
                    f"{expected}\trefs/tags/v2.6.0^{{}}"
                ),
            ],
        ):
            release.ensure_release_tag("v2.6.0", expected)

        verify_controls.assert_called_once_with()
        commands = [call.args[0] for call in run.call_args_list]
        self.assertFalse(any(command[:2] == ["git", "tag"] for command in commands))
        self.assertIn(
            ["git", "push", "origin", "refs/tags/v2.6.0"],
            commands,
        )


class VersionSurfaceEditTest(unittest.TestCase):
    """Every version surface must move together.

    `stage_prepare` bumped only pyproject and `__init__`, leaving the
    fast-baseline runtime pin and the runtime version test behind. That drift
    cost the 2.6.3 release six full gate runs, and the fast-baseline pin is
    load-bearing: a stale value silently reverts a first sync to the slow path.
    """

    def _tree(self, directory, version):
        root = Path(directory)
        (root / "forward_netbox" / "utilities").mkdir(parents=True)
        (root / "forward_netbox" / "tests").mkdir(parents=True)
        (root / "pyproject.toml").write_text(
            f'version = "{version}"\n', encoding="utf-8"
        )
        (root / "forward_netbox" / "__init__.py").write_text(
            f'    version = "{version}"\n', encoding="utf-8"
        )
        (root / "forward_netbox" / "utilities" / "fast_baseline.py").write_text(
            f'    "forward_netbox": "{version}",\n', encoding="utf-8"
        )
        (
            root / "forward_netbox" / "tests" / "test_runtime_dependency_check.py"
        ).write_text(
            f'        NetboxForwardConfig.version, "{version}"\n', encoding="utf-8"
        )
        return root

    def _patched(self, root):
        return (
            patch.object(release, "REPO_ROOT", root),
            patch.object(release, "PYPROJECT", root / "pyproject.toml"),
            patch.object(release, "INIT_PY", root / "forward_netbox/__init__.py"),
            patch.object(
                release,
                "FAST_BASELINE",
                root / "forward_netbox/utilities/fast_baseline.py",
            ),
            patch.object(
                release,
                "RUNTIME_VERSION_TEST",
                root / "forward_netbox/tests/test_runtime_dependency_check.py",
            ),
        )

    def test_all_four_surfaces_are_rewritten(self):
        with tempfile.TemporaryDirectory() as directory:
            root = self._tree(directory, "1.2.3")
            patches = self._patched(root)
            for item in patches:
                item.start()
            try:
                edits = release.version_surface_edits("1.2.3", "1.2.4")
            finally:
                for item in patches:
                    item.stop()
            self.assertEqual(len(edits), 4)
            for text in edits.values():
                self.assertIn("1.2.4", text)
                self.assertNotIn("1.2.3", text)

    def test_a_missing_surface_is_reported_not_skipped(self):
        with tempfile.TemporaryDirectory() as directory:
            root = self._tree(directory, "1.2.3")
            (root / "forward_netbox" / "utilities" / "fast_baseline.py").write_text(
                "nothing to bump here\n", encoding="utf-8"
            )
            patches = self._patched(root)
            for item in patches:
                item.start()
            try:
                with self.assertRaisesRegex(release.ReleaseError, "fast_baseline"):
                    release.version_surface_edits("1.2.3", "1.2.4")
            finally:
                for item in patches:
                    item.stop()


class NoDevMarkerOnMainTest(unittest.TestCase):
    """`main` carries the released version. Decided, and the machinery gone.

    `--open-next` put `X.Y.Z.dev0` on `main`; the operating rule since 2.7.13
    said it must never be there; six release plans recorded the contradiction
    as undecided, and the post-release step failed on it every release. A
    customer installs this plugin from source, so a dev marker on `main`
    offered a version that was never gated, tagged or published. The window it
    described - between the release PR merging and the tag - is minutes wide
    and `--auto-finish` runs straight through it.
    """

    def test_the_stage_and_its_flag_are_gone(self):
        self.assertFalse(hasattr(release, "stage_open_next"))
        with patch.object(release, "stage_finish"):
            with self.assertRaises(SystemExit):
                release.main(["2.9.2", "--open-next"])

    def test_nothing_in_the_tool_writes_a_dev_marker(self):
        # Prose may still say `.dev0` when recording the decision; no string
        # literal may build one.
        source = Path(release.__file__).read_text(encoding="utf-8")
        body = source[source.index("def bump_version_text") :]
        self.assertNotIn('.dev0"', body)
        self.assertNotIn(".dev0'", body)


class PromoteReleaseTablesTest(unittest.TestCase):
    """Promotion is a file edit. The commit belongs to the anchor."""

    def _tables(self):
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        paths = tuple(Path(scratch.name) / f"table-{index}.md" for index in range(3))
        for path in paths:
            path.write_text("candidate table\n", encoding="utf-8")
        return paths

    def test_it_rewrites_the_tables_and_regenerates_the_changelog_only(self):
        paths = self._tables()
        with (
            patch.object(release, "README_TABLES", paths),
            patch.object(
                release,
                "promote_release_candidate_text",
                side_effect=lambda text, version: text.replace("candidate", "current"),
            ),
            patch.object(
                release,
                "set_release_intro_text",
                side_effect=lambda text, v, candidate: text,
            ),
            patch.object(release, "run") as run,
        ):
            self.assertTrue(release.promote_release_tables("2.9.2"))

        for path in paths:
            self.assertEqual(path.read_text(encoding="utf-8"), "current table\n")
        commands = [" ".join(call.args[0]) for call in run.call_args_list]
        self.assertEqual(len(commands), 1, commands)
        self.assertIn("gen_changelog.py", commands[0])

    def test_an_already_promoted_table_is_reported_not_rewritten(self):
        paths = self._tables()
        with (
            patch.object(release, "README_TABLES", paths),
            patch.object(
                release, "promote_release_candidate_text", side_effect=lambda t, v: t
            ),
            patch.object(
                release, "set_release_intro_text", side_effect=lambda t, v, candidate: t
            ),
            patch.object(release, "run") as run,
        ):
            self.assertFalse(release.promote_release_tables("2.9.2"))
        run.assert_not_called()


class GateSummaryTest(unittest.TestCase):
    def test_it_reads_every_suite_summary_from_the_log(self):
        log = (
            "Ran 334 tests in 81.300s\nOK\n...\n"
            "Ran 2324 tests in 906.639s\nOK (skipped=4)\n"
            "validated SBOM of 178 components\n"
            "7 authenticated menu routes returned 200\n"
        )
        summary = release._gate_summary(log)
        self.assertEqual(summary["test_runs"], [334, 2324])
        self.assertEqual(summary["tests_total"], 2658)
        self.assertFalse(summary["failed"])
        self.assertEqual(summary["sbom_components"], 178)
        self.assertEqual(summary["routes"], 7)

    def test_a_failed_suite_is_visible(self):
        summary = release._gate_summary("Ran 5 tests in 1s\nFAILED (failures=2)\n")
        self.assertTrue(summary["failed"])
        self.assertEqual(summary["verdicts"], ["FAILED"])


class EvidenceCommandTest(unittest.TestCase):
    ENVIRONMENT = {
        "FORWARD_NETBOX_DOCKER_PROJECT": "forward-netbox-release-gate",
        "FORWARD_NETBOX_POSTGRES_DATA_PATH": "netbox-postgres-data",
        "FORWARD_NETBOX_WORKER_AUTORELOAD": "0",
        "NETBOX_VER": "v4.7.0",
        "FORWARD_NETBOX_HOST_PORT": "18080",
        "NETBOX_URL": "http://127.0.0.1:18080",
        "HOME": "/nowhere",
    }

    def test_the_gate_command_omits_the_host_port_pair(self):
        command = release._evidence_command("ci", self.ENVIRONMENT, with_url=False)
        self.assertEqual(
            command,
            "rtk env FORWARD_NETBOX_DOCKER_PROJECT=forward-netbox-release-gate "
            "FORWARD_NETBOX_POSTGRES_DATA_PATH=netbox-postgres-data "
            "FORWARD_NETBOX_WORKER_AUTORELOAD=0 NETBOX_VER=v4.7.0 invoke ci",
        )

    def test_the_artifact_command_carries_the_pair_and_nothing_foreign(self):
        command = release._evidence_command(
            "artifact-test", self.ENVIRONMENT, with_url=True
        )
        self.assertIn(
            "FORWARD_NETBOX_HOST_PORT=18080 NETBOX_URL=http://127.0.0.1:18080", command
        )
        self.assertNotIn("HOME", command)
        self.assertTrue(command.endswith("invoke artifact-test"))


class RenderReleaseAuthorizationTest(unittest.TestCase):
    """The rendered section must satisfy the checker that gates the tag.

    Asked of the checker's own predicate, not of a restated rule, so the
    renderer cannot drift from what `check_release_authorization.py` accepts.
    """

    def _record(self, **overrides):
        environment = EvidenceCommandTest.ENVIRONMENT
        record = {
            "version": "2.9.2",
            "netbox_version": "4.7.0",
            "branching_version": "1.2.0",
            "python_version": "3.14",
            "wheel": "forward_netbox-2.9.2-py3-none-any.whl",
            "gate": {
                "command": release._evidence_command("ci", environment, with_url=False),
                "exit_status": 0,
                "test_runs": [334, 70, 2324],
                "tests_total": 2728,
            },
            "artifact": {
                "command": release._evidence_command(
                    "artifact-test", environment, with_url=True
                ),
                "exit_status": 0,
                "sbom_components": 178,
                "routes": 7,
            },
        }
        record.update(overrides)
        return record

    def _checker(self):
        import sys

        scripts = str(Path(release.__file__).resolve().parent)
        if scripts not in sys.path:
            sys.path.insert(0, scripts)
        import check_release_authorization

        return check_release_authorization

    def test_every_required_evidence_line_is_concrete_to_the_checker(self):
        checker = self._checker()
        section = release.render_release_authorization(self._record(), "e" * 40)

        self.assertIn("- Evidence base commit: `" + "e" * 40 + "`", section)
        for evidence_id in checker.REQUIRED_EVIDENCE_IDS:
            line = next(
                line for line in section.splitlines() if f"`{evidence_id}`" in line
            )
            evidence = line.split(" - ", 1)[1]
            self.assertTrue(
                checker._evidence_is_concrete(evidence_id, evidence),
                f"{evidence_id}: {evidence}",
            )

    def test_a_failed_gate_cannot_be_rendered_as_passing(self):
        record = self._record()
        record["gate"]["exit_status"] = 1
        with self.assertRaisesRegex(release.ReleaseError, "re-run verify"):
            release.render_release_authorization(record, "e" * 40)


class WaitForPullRequestMergeTest(unittest.TestCase):
    def _pull(self, state):
        return {"state": state, "url": "https://example.invalid/pull/1", "number": 1}

    def test_it_returns_once_the_pull_request_is_merged(self):
        states = iter([self._pull("OPEN"), self._pull("MERGED")])
        with (
            patch.object(
                release, "_pull_request_for_branch", side_effect=lambda b: next(states)
            ),
            patch("time.sleep") as sleep,
        ):
            pull = release._wait_for_pull_request_merge("release/2.9.2", poll_seconds=1)
        self.assertEqual(pull["state"], "MERGED")
        sleep.assert_called_once_with(1)

    def test_a_closed_pull_request_is_a_refusal_not_a_wait(self):
        with patch.object(
            release, "_pull_request_for_branch", return_value=self._pull("CLOSED")
        ):
            with self.assertRaisesRegex(release.ReleaseError, "closed without merging"):
                release._wait_for_pull_request_merge("release/2.9.2")

    def test_a_missing_pull_request_is_named(self):
        with patch.object(release, "_pull_request_for_branch", return_value=None):
            with self.assertRaisesRegex(release.ReleaseError, "no pull request"):
                release._wait_for_pull_request_merge("release/2.9.2")

    def test_the_wait_is_bounded_and_says_how_to_resume(self):
        with (
            patch.object(
                release, "_pull_request_for_branch", return_value=self._pull("OPEN")
            ),
            patch("time.sleep"),
        ):
            with self.assertRaisesRegex(release.ReleaseError, "resume"):
                release._wait_for_pull_request_merge(
                    "release/2.9.2", poll_seconds=1, max_polls=2
                )


class UnattendedFinishTest(unittest.TestCase):
    """One invocation from the pushed production branch to the anchor."""

    STEP_FUNCTIONS = (
        "stage_finish",
        "_wait_for_pull_request_merge",
        "stage_authorize",
        "_checkout_merged_main",
        "stage_anchor",
    )

    def _patched(self):
        calls = []
        patches = {
            name: patch.object(
                release,
                name,
                side_effect=lambda *a, _n=name, **k: calls.append((_n, a)),
            )
            for name in self.STEP_FUNCTIONS
        }
        return calls, patches

    def test_the_steps_run_in_release_order(self):
        calls, patches = self._patched()
        for patcher in patches.values():
            patcher.start()
            self.addCleanup(patcher.stop)

        release.stage_finish_unattended("2.9.2")

        self.assertEqual(
            [name for name, _args in calls],
            [
                "stage_finish",
                "_wait_for_pull_request_merge",
                "stage_authorize",
                "stage_finish",
                "_wait_for_pull_request_merge",
                "_checkout_merged_main",
                "stage_finish",
                "_wait_for_pull_request_merge",
                "stage_anchor",
                "_wait_for_pull_request_merge",
            ],
        )
        waited_for = [
            args[0] for name, args in calls if name == "_wait_for_pull_request_merge"
        ]
        self.assertEqual(
            waited_for,
            [
                "release/2.9.2",
                "release/2.9.2-evidence",
                "docs/post-release-2.9.2",
                "chore/anchor-2.9.2",
            ],
        )

    def test_a_failure_names_the_step_and_how_to_resume(self):
        _calls, patches = self._patched()
        for patcher in patches.values():
            patcher.start()
            self.addCleanup(patcher.stop)
        with patch.object(
            release, "stage_authorize", side_effect=release.ReleaseError("no record")
        ):
            with self.assertRaises(release.ReleaseError) as caught:
                release.stage_finish_unattended("2.9.2")
        message = str(caught.exception)
        self.assertIn("stopped at --authorize", message)
        self.assertIn("no record", message)
        self.assertIn("resume", message)


class BridgeCommitForTest(unittest.TestCase):
    def _capture(self, changed):
        def capture(cmd):
            if cmd[:2] == ["git", "rev-list"] and "--first-parent" in cmd:
                return "b" * 40 + "\n" + "c" * 40
            if cmd[:2] == ["git", "rev-list"]:
                return "t" * 40
            if cmd[:2] == ["git", "diff"]:
                return "\n".join(changed)
            raise AssertionError(cmd)

        return capture

    def test_the_first_documentation_only_commit_after_the_tag_is_the_bridge(self):
        with (
            patch.object(release, "run"),
            patch.object(
                release,
                "_capture",
                side_effect=self._capture(
                    ["docs/03_Plans/active/post-release-2.9.2.md"]
                ),
            ),
        ):
            self.assertEqual(release._bridge_commit_for("v2.9.2"), "b" * 40)

    def test_a_code_commit_in_the_slot_is_refused_by_name(self):
        with (
            patch.object(release, "run"),
            patch.object(
                release, "_capture", side_effect=self._capture(["scripts/release.py"])
            ),
        ):
            with self.assertRaisesRegex(release.ReleaseError, "documentation-only"):
                release._bridge_commit_for("v2.9.2")

    def test_nothing_after_the_tag_is_reported_not_guessed(self):
        def capture(cmd):
            return ""

        with patch.object(release, "run"), patch.object(release, "_capture", capture):
            with self.assertRaisesRegex(release.ReleaseError, "nothing has landed"):
                release._bridge_commit_for("v2.9.2")


class StageAnchorTest(unittest.TestCase):
    """The anchor and the promotion move together, as one reviewed commit."""

    def _run(self, *, failing=None):
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        root = Path(scratch.name)
        provenance = root / "verify_release_provenance.py"
        provenance.write_text(
            'PRIOR_RELEASE_TAG = "v2.9.1"\n'
            'PRIOR_POST_RELEASE_DOC_COMMIT = "' + "a" * 40 + '"\n',
            encoding="utf-8",
        )
        plan = root / "anchor-2.9.2.md"
        calls = []

        def run(cmd, **kwargs):
            calls.append(cmd)
            if failing and failing in " ".join(cmd):
                raise release.ReleaseError("release command failed with exit code 1")

        with (
            patch.object(release, "PROVENANCE", provenance),
            patch.object(release, "REPO_ROOT", root),
            patch.object(release, "README_TABLES", ()),
            patch.object(release, "_bridge_commit_for", return_value="b" * 40),
            patch.object(release, "_anchor_plan_path", return_value=plan),
            patch.object(release, "promote_release_tables") as promote,
            patch.object(release, "_open_pull_request") as open_pull_request,
            patch.object(release, "_capture", return_value="main"),
            patch.object(release, "run", side_effect=run),
        ):
            try:
                release.stage_anchor("2.9.2", "v2.9.2")
            except release.ReleaseError:
                if not failing:
                    raise
        return (
            provenance.read_text(encoding="utf-8"),
            plan,
            calls,
            promote,
            open_pull_request,
        )

    def test_it_advances_both_constants_and_promotes(self):
        text, plan, calls, promote, open_pull_request = self._run()
        self.assertIn('PRIOR_RELEASE_TAG = "v2.9.2"', text)
        self.assertIn('PRIOR_POST_RELEASE_DOC_COMMIT = "' + "b" * 40 + '"', text)
        promote.assert_called_once_with("2.9.2")
        open_pull_request.assert_called_once()
        joined = [" ".join(call) for call in calls]
        self.assertIn("git checkout -B chore/anchor-2.9.2 origin/main", joined)
        commit = next(i for i, c in enumerate(joined) if "git commit" in c)
        harness = next(i for i, c in enumerate(joined) if "check_harness.py" in c)
        push = next(i for i, c in enumerate(joined) if c.startswith("git push"))
        self.assertLess(commit, harness)
        self.assertLess(harness, push)
        self.assertEqual(joined[-1], "git checkout --force main")

    def test_the_generated_plan_carries_the_harness_headings(self):
        _text, plan, _calls, _promote, _open = self._run()
        text = plan.read_text(encoding="utf-8")
        for heading in (
            "## Goal",
            "## Constraints",
            "## Touched Surfaces",
            "## Approach",
            "## Validation",
            "## Rollback",
            "## Decision Log",
        ):
            self.assertIn(heading, text)
        self.assertIn("b" * 40, text)

    def test_a_failed_stage_deletes_the_branch_it_made(self):
        _text, _plan, calls, _promote, open_pull_request = self._run(
            failing="check_harness.py"
        )
        joined = [" ".join(call) for call in calls]
        self.assertIn("git branch -D chore/anchor-2.9.2", joined)
        open_pull_request.assert_not_called()


class StageAuthorizeTest(unittest.TestCase):
    def _run(self, *, existing_section=False):
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        root = Path(scratch.name)
        plan = root / "2026-09-02-release-2.9.2.md"
        plan.write_text(
            "# Release 2.9.2\n\n## Goal\n\nShip.\n"
            + ("\n## Release Authorization\n" if existing_section else ""),
            encoding="utf-8",
        )
        calls = []
        with (
            patch.object(release, "REPO_ROOT", root),
            patch.object(
                release,
                "read_evidence_record",
                return_value=RenderReleaseAuthorizationTest()._record(),
            ),
            patch.object(release, "_checkout_merged_main", return_value="e" * 40),
            patch.object(release, "_release_plan_path", return_value=plan),
            patch.object(
                release, "run", side_effect=lambda cmd, **k: calls.append(cmd)
            ),
        ):
            release.stage_authorize("2.9.2")
        return plan.read_text(encoding="utf-8"), calls

    def test_it_appends_the_rendered_section_on_the_evidence_branch(self):
        text, calls = self._run()
        self.assertIn("## Release Authorization", text)
        self.assertIn("- Evidence base commit: `" + "e" * 40 + "`", text)
        joined = [" ".join(call) for call in calls]
        self.assertIn("git checkout -B release/2.9.2-evidence origin/main", joined)
        self.assertTrue(any("release: authorize v2.9.2" in c for c in joined))
        self.assertTrue(any("check_release_authorization.py" in c for c in joined))

    def test_it_refuses_to_authorize_twice(self):
        with self.assertRaisesRegex(release.ReleaseError, "already carries"):
            self._run(existing_section=True)


class PullRequestLookupIgnoresDeadHistoryTest(unittest.TestCase):
    """A PR merged into rewritten-away history is not this release's PR.

    It looks identical to a real one - same head branch, state MERGED - so
    `stage_finish` concluded the release was already cut and stopped, with
    nothing to fix. Purging two live Forward identifiers from public history
    orphaned the v3.0.0 release PRs exactly this way.
    """

    def _pull(self, oid, state="MERGED"):
        return {
            "number": 348,
            "state": state,
            "url": "https://example.invalid/pr/348",
            "mergeCommit": {"oid": oid} if oid else None,
        }

    def test_an_unreachable_merge_commit_is_not_live(self):
        completed = SimpleNamespace(returncode=1)
        with unittest.mock.patch.object(
            release.subprocess, "run", return_value=completed
        ):
            self.assertFalse(release._merge_is_live(self._pull("deadbeef" * 5)))

    def test_a_reachable_merge_commit_is_live(self):
        completed = SimpleNamespace(returncode=0)
        with unittest.mock.patch.object(
            release.subprocess, "run", return_value=completed
        ):
            self.assertTrue(release._merge_is_live(self._pull("cafebabe" * 5)))

    def test_a_pull_without_a_merge_commit_is_assumed_live(self):
        # Assume live rather than silently reopening a release that really did
        # complete: a false "already merged" stops the flow loudly, a false
        # "not merged" would re-cut a shipped release.
        self.assertTrue(release._merge_is_live(self._pull(None)))

    def test_a_merge_that_just_landed_is_live_after_a_fetch(self):
        """The race this check creates on the release it is meant to protect.

        Reachability is read from local refs, so the production merge the
        script itself queued moments earlier is invisible until the
        remote-tracking ref catches up. Unretried, that reads as "no pull
        request exists for release/3.0.0" and the release stops one step short
        of the tag - which is exactly how v3.0.0's first cut ended.
        """
        fetched = []

        def fake_run(argv, **kwargs):
            if argv[:2] == ["git", "fetch"]:
                fetched.append(argv)
                return SimpleNamespace(returncode=0)
            if argv[:2] == ["git", "cat-file"]:
                return SimpleNamespace(returncode=0)
            # merge-base: unreachable until the fetch has happened
            return SimpleNamespace(returncode=1 if not fetched else 0)

        with unittest.mock.patch.object(release.subprocess, "run", fake_run):
            self.assertTrue(release._merge_is_live(self._pull("abadcafe" * 5)))
        self.assertEqual(len(fetched), 1, "expected exactly one fetch retry")

    def test_a_merged_pull_from_an_earlier_attempt_does_not_skip_the_push(self):
        """ "Already merged" must mean THIS head shipped, not a same-named PR.

        v3.0.0's authorization was regenerated after the first tag was refused.
        The evidence branch kept its name, so the lookup found the FIRST
        attempt's pull request - merged, live, and irrelevant - and reported
        the release finished while the new commit sat unpushed. main then had
        no authorization section at all.
        """
        opened = []

        # The stale merged PR first, then the freshly opened one.
        lookups = [
            {"state": "MERGED", "url": "https://example.invalid/pr/353"},
            {
                "state": "OPEN",
                "url": "https://example.invalid/pr/355",
                "number": 355,
            },
        ]

        with (
            unittest.mock.patch.object(
                release, "_pull_request_for_branch", side_effect=lookups
            ),
            unittest.mock.patch.object(release, "_head_is_merged", return_value=False),
            unittest.mock.patch.object(
                release, "_verify_live_release_controls", lambda: None
            ),
            unittest.mock.patch.object(
                release, "run", lambda *a, **k: opened.append(a)
            ),
        ):
            release._open_release_pull_request(
                "3.0.0", "release/3.0.0-evidence", evidence=True
            )

        commands = [a[0] for a in opened]
        self.assertTrue(
            any(c[:3] == ["gh", "pr", "create"] for c in commands),
            "expected a new pull request to be opened",
        )

    def test_a_merged_pull_for_this_head_does_return_early(self):
        """The opposite branch: a real completion must still short-circuit."""
        opened = []

        with (
            unittest.mock.patch.object(
                release,
                "_pull_request_for_branch",
                return_value={
                    "state": "MERGED",
                    "url": "https://example.invalid/pr/353",
                },
            ),
            unittest.mock.patch.object(release, "_head_is_merged", return_value=True),
            unittest.mock.patch.object(
                release, "run", lambda *a, **k: opened.append(a)
            ),
        ):
            release._open_release_pull_request(
                "3.0.0", "release/3.0.0-evidence", evidence=True
            )

        self.assertEqual(opened, [], "a shipped release must not be re-opened")

    def test_a_genuinely_dead_merge_stays_dead_across_the_fetch(self):
        """The retry must not turn the dead-history verdict back into a pass."""
        calls = []

        def fake_run(argv, **kwargs):
            calls.append(argv[:2])
            if argv[:2] == ["git", "fetch"]:
                return SimpleNamespace(returncode=0)
            return SimpleNamespace(returncode=1)

        with unittest.mock.patch.object(release.subprocess, "run", fake_run):
            self.assertFalse(release._merge_is_live(self._pull("deadbeef" * 5)))
        self.assertIn(["git", "fetch"], calls)
