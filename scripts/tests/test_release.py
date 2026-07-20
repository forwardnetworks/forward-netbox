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

        with patch.object(release, "_capture", side_effect=payloads) as capture:
            self.assertTrue(
                release.wait_for_required_workflows(
                    commit,
                    expected_branch="release/2.6.0",
                    max_polls=1,
                )
            )

        sleep.assert_not_called()
        self.assertEqual(capture.call_count, 2)

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
            "_capture",
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

        with patch.object(release, "_capture", return_value=payload):
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

        with patch.object(release, "_capture", return_value=payload):
            self.assertFalse(
                release.wait_for_required_workflows(
                    commit,
                    expected_branch="release/2.6.0",
                    poll_seconds=0,
                    max_polls=1,
                )
            )

        sleep.assert_called_once_with(0)

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


if __name__ == "__main__":
    unittest.main()
