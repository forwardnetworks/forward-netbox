from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
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
        "The `1.5.10` release requires NetBox `4.6.5`. "
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
            "The `1.5.11` release candidate requires NetBox `4.6.5`. "
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
            "The `1.5.11` release requires NetBox `4.6.5`. "
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

    @patch.object(release, "run")
    @patch.object(release, "_promote_release_candidate", return_value=True)
    def test_first_finish_stops_after_metadata_promotion(self, promote, run):
        with patch.object(
            release,
            "_capture",
            return_value="release/2.6.0",
        ):
            release.stage_finish("2.6.0")

        promote.assert_called_once_with("2.6.0")
        run.assert_not_called()

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


class RequiredReleaseWorkflowTest(unittest.TestCase):
    @staticmethod
    def _run(
        path,
        *,
        run_id,
        commit,
        branch="release/2.6.0",
        status="completed",
        conclusion="success",
    ):
        return {
            "id": run_id,
            "path": path,
            "head_sha": commit,
            "head_branch": branch,
            "event": "push",
            "status": status,
            "conclusion": conclusion,
        }

    def _payload(self, *runs):
        return json.dumps({"workflow_runs": list(runs)})

    @patch("time.sleep")
    def test_required_workflows_pass_only_for_exact_runs(self, sleep):
        commit = "a" * 40
        payloads = [
            self._payload(self._run(path, run_id=index, commit=commit))
            for index, path in enumerate(
                release.REQUIRED_RELEASE_WORKFLOWS,
                start=1,
            )
        ]

        with patch.object(
            release, "_capture_required", side_effect=payloads
        ) as capture:
            self.assertTrue(
                release.wait_for_required_workflows(
                    commit,
                    expected_branch="release/2.6.0",
                    max_polls=1,
                )
            )

        sleep.assert_not_called()
        self.assertEqual(capture.call_count, 2)
        first_command = capture.call_args_list[0].args[0]
        self.assertIn(
            "repos/forwardnetworks/forward-netbox/actions/workflows/ci.yml/runs",
            first_command,
        )
        self.assertNotIn(
            "repos/forwardnetworks/forward-netbox/actions/workflows/"
            ".github/workflows/ci.yml/runs",
            first_command,
        )

    @patch("time.sleep")
    def test_required_workflows_wait_for_nonterminal_run(self, sleep):
        commit = "b" * 40
        ci_path, codeql_path = release.REQUIRED_RELEASE_WORKFLOWS
        ci_pending = self._payload(
            self._run(
                ci_path,
                run_id=1,
                commit=commit,
                status="in_progress",
                conclusion=None,
            )
        )
        ci_complete = self._payload(self._run(ci_path, run_id=1, commit=commit))
        codeql_complete = self._payload(self._run(codeql_path, run_id=2, commit=commit))

        with patch.object(
            release,
            "_capture_required",
            side_effect=[
                ci_pending,
                codeql_complete,
                ci_complete,
                codeql_complete,
            ],
        ):
            self.assertTrue(
                release.wait_for_required_workflows(
                    commit,
                    expected_branch="release/2.6.0",
                    poll_seconds=0,
                    max_polls=2,
                )
            )

        sleep.assert_called_once_with(0)

    @patch("time.sleep")
    def test_required_workflows_reject_latest_failure(self, sleep):
        commit = "c" * 40
        ci_path = release.REQUIRED_RELEASE_WORKFLOWS[0]
        payload = self._payload(
            self._run(ci_path, run_id=1, commit=commit),
            self._run(
                ci_path,
                run_id=99,
                commit=commit,
                conclusion="failure",
            ),
        )

        with patch.object(release, "_capture_required", return_value=payload):
            self.assertFalse(
                release.wait_for_required_workflows(
                    commit,
                    expected_branch="release/2.6.0",
                    max_polls=1,
                )
            )

        sleep.assert_not_called()

    @patch("time.sleep")
    def test_required_workflows_reject_wrong_path_or_branch(self, sleep):
        commit = "d" * 40
        payload = self._payload(
            self._run(
                ".github/workflows/fake.yml",
                run_id=1,
                commit=commit,
                branch="main",
            )
        )

        with patch.object(release, "_capture_required", return_value=payload):
            self.assertFalse(
                release.wait_for_required_workflows(
                    commit,
                    expected_branch="release/2.6.0",
                    poll_seconds=0,
                    max_polls=1,
                )
            )

        sleep.assert_called_once_with(0)

    @patch("time.sleep")
    def test_required_workflows_reject_empty_response_immediately(self, sleep):
        with (
            patch.object(release, "_capture_required", return_value=""),
            self.assertRaisesRegex(release.ReleaseError, "empty response"),
        ):
            release.wait_for_required_workflows(
                "f" * 40,
                expected_branch="release/2.6.0",
                max_polls=1,
            )

        sleep.assert_not_called()

    @patch("time.sleep")
    def test_required_workflows_reject_invalid_json_immediately(self, sleep):
        with (
            patch.object(release, "_capture_required", return_value="not-json"),
            self.assertRaisesRegex(release.ReleaseError, "invalid JSON"),
        ):
            release.wait_for_required_workflows(
                "f" * 40,
                expected_branch="release/2.6.0",
                max_polls=1,
            )

        sleep.assert_not_called()

    def test_workflow_payload_rejects_wrong_schema_without_echoing_response(self):
        secret = "secret-response-value"
        raw = json.dumps({"message": secret})

        with self.assertRaises(release.ReleaseError) as error:
            release._workflow_runs_payload(raw, purpose="GitHub workflow query")

        self.assertNotIn(secret, str(error.exception))
        self.assertIn("invalid workflow-runs payload", str(error.exception))

    def test_release_head_rejects_commit_changed_after_ci(self):
        expected = "a" * 40
        changed = "b" * 40

        def capture(command):
            if command == ["git", "branch", "--show-current"]:
                return "release/2.6.0"
            if command == ["git", "rev-parse", "HEAD"]:
                return changed
            if command == [
                "git",
                "ls-remote",
                "--heads",
                "origin",
                "release/2.6.0",
            ]:
                return f"{changed}\trefs/heads/release/2.6.0"
            raise AssertionError(command)

        with (
            patch.object(release, "_capture", side_effect=capture),
            self.assertRaisesRegex(release.ReleaseError, "HEAD changed after CI"),
        ):
            release._assert_release_head("2.6.0", expected)

    @patch("time.sleep")
    def test_release_workflow_waiter_uses_checked_exact_query(self, sleep):
        commit = "e" * 40
        payload = self._payload(
            {
                "id": 101,
                "path": ".github/workflows/release.yml",
                "head_sha": commit,
                "head_branch": "v2.6.0",
                "event": "push",
                "status": "completed",
                "conclusion": "success",
            }
        )

        with (
            patch.object(release, "_capture", return_value=commit),
            patch.object(
                release,
                "_capture_required",
                return_value=payload,
            ) as capture,
        ):
            self.assertEqual(
                release.wait_for_release_workflow("2.6.0", max_polls=1),
                "success",
            )

        sleep.assert_not_called()
        command = capture.call_args.args[0]
        self.assertIn(
            "repos/forwardnetworks/forward-netbox/actions/workflows/"
            "release.yml/runs",
            command,
        )
        self.assertIn(f"head_sha={commit}", command)


if __name__ == "__main__":
    unittest.main()
