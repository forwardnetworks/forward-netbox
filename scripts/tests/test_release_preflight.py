"""Tests for the fast release preflight.

These pin the two failures that cost six full gate runs during the 2.6.3
release: a version surface left at the previous release, and the UI harness
dependencies never installed.
"""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import check_release_preflight as preflight  # noqa: E402


class VersionSurfaceTest(unittest.TestCase):
    def test_all_surfaces_agree_in_the_real_tree(self):
        self.assertEqual(
            preflight.check_version_surfaces(), preflight.declared_version()
        )

    def test_rejects_a_surface_left_at_the_previous_release(self):
        surfaces = preflight.version_surfaces()
        stale = dict(surfaces)
        stale["forward_netbox/tests/test_runtime_dependency_check.py"] = "0.0.1"
        with mock.patch.object(preflight, "version_surfaces", return_value=stale):
            with self.assertRaisesRegex(preflight.PreflightError, "0.0.1"):
                preflight.check_version_surfaces()

    def test_names_every_drifted_surface(self):
        stale = {path: "0.0.1" for path in preflight.version_surfaces()}
        with mock.patch.object(preflight, "version_surfaces", return_value=stale):
            with self.assertRaises(preflight.PreflightError) as caught:
                preflight.check_version_surfaces()
        for path in stale:
            self.assertIn(path, str(caught.exception))

    def test_covers_the_fast_baseline_runtime_pin(self):
        # The pin gates the fast baseline engine; a stale value silently
        # reverts a first sync to the slow path, so it must stay checked.
        self.assertIn(
            "forward_netbox/utilities/fast_baseline.py",
            preflight.version_surfaces(),
        )

    def test_ignores_the_neighbouring_netbox_version_pins(self):
        # min_version/max_version sit beside the plugin version in __init__.
        self.assertEqual(
            preflight.version_surfaces()["forward_netbox/__init__.py"],
            preflight.declared_version(),
        )


class UiHarnessDependencyTest(unittest.TestCase):
    def test_passes_when_dependencies_are_installed(self):
        # Built rather than read from the working tree: CI runs the harness
        # tests before `npm install`, so asserting against the real
        # node_modules would make this test depend on the environment.
        with tempfile.TemporaryDirectory() as root:
            installed = Path(root)
            for name in json.loads(preflight._read(preflight.PACKAGE_JSON))[
                "devDependencies"
            ]:
                (installed / name).mkdir(parents=True)
            with mock.patch.object(preflight, "NODE_MODULES", installed):
                self.assertIn("playwright", preflight.check_ui_harness_dependencies())

    def test_rejects_missing_dependencies(self):
        with mock.patch.object(preflight, "NODE_MODULES", Path("/nonexistent")):
            with self.assertRaisesRegex(preflight.PreflightError, "npm install"):
                preflight.check_ui_harness_dependencies()

    def test_rejects_a_manifest_declaring_no_dependencies(self):
        with mock.patch.object(preflight, "_read", return_value=json.dumps({})):
            with self.assertRaisesRegex(preflight.PreflightError, "no UI harness"):
                preflight.check_ui_harness_dependencies()


if __name__ == "__main__":
    unittest.main()


class EvidenceBaseCommitTest(unittest.TestCase):
    """The check that turns a 40-minute rebind round trip into a 2-second one.

    Authorization binds the tagged commit to its parent, but a release branch's
    own parent is not what `main` will hold: the release squash-merges. Recording
    the branch-side parent therefore always mismatches once merged, and 2.6.5
    needed an extra PR and a full CI cycle to correct one line.
    """

    MAIN = "a" * 40
    BRANCH_PARENT = "b" * 40

    def _git(self, mapping, *, tag=""):
        def fake(*arguments):
            if arguments[:2] == ("tag", "--list"):
                return tag
            return mapping.get(arguments, "")

        return fake

    def _plan(self, directory, version, commit):
        plans = Path(directory) / "docs" / "03_Plans" / "active"
        plans.mkdir(parents=True)
        plan = plans / f"2026-01-01-release-{version}-tranche.md"
        plan.write_text(
            "## Release Authorization\n\n" f"- Evidence base commit: `{commit}`\n",
            encoding="utf-8",
        )
        return plan

    def test_rejects_the_branch_side_parent(self):
        with tempfile.TemporaryDirectory() as directory:
            self._plan(directory, "9.9.9", self.BRANCH_PARENT)
            with (
                mock.patch.object(preflight, "REPO_ROOT", Path(directory)),
                mock.patch.object(
                    preflight,
                    "_git",
                    self._git({("rev-parse", "origin/main"): self.MAIN}),
                ),
            ):
                with self.assertRaisesRegex(preflight.PreflightError, "squash merge"):
                    preflight.check_release_plan_evidence_base("9.9.9")

    def test_accepts_the_origin_main_head(self):
        with tempfile.TemporaryDirectory() as directory:
            self._plan(directory, "9.9.9", self.MAIN)
            with (
                mock.patch.object(preflight, "REPO_ROOT", Path(directory)),
                mock.patch.object(
                    preflight,
                    "_git",
                    self._git({("rev-parse", "origin/main"): self.MAIN}),
                ),
            ):
                self.assertIn(
                    "matches origin/main",
                    preflight.check_release_plan_evidence_base("9.9.9"),
                )

    def test_skips_an_already_tagged_release(self):
        # A shipped release's recorded value is history, not a defect.
        with tempfile.TemporaryDirectory() as directory:
            self._plan(directory, "9.9.9", self.BRANCH_PARENT)
            with (
                mock.patch.object(preflight, "REPO_ROOT", Path(directory)),
                mock.patch.object(
                    preflight,
                    "_git",
                    self._git({("rev-parse", "origin/main"): self.MAIN}, tag="v9.9.9"),
                ),
            ):
                self.assertIn(
                    "already tagged",
                    preflight.check_release_plan_evidence_base("9.9.9"),
                )

    def test_skips_when_origin_main_is_unknown(self):
        # A fresh clone or offline run must not fail the gate.
        with tempfile.TemporaryDirectory() as directory:
            self._plan(directory, "9.9.9", self.BRANCH_PARENT)
            with (
                mock.patch.object(preflight, "REPO_ROOT", Path(directory)),
                mock.patch.object(preflight, "_git", self._git({})),
            ):
                self.assertIn(
                    "origin/main is unknown",
                    preflight.check_release_plan_evidence_base("9.9.9"),
                )

    def test_accepts_the_tag_parent_once_the_release_has_merged(self):
        # Between merge and tag, origin/main IS the commit about to be tagged.
        # Comparing against it would demand the plan record the tagged commit in
        # place of its parent - which release_evidence_commit_binding rejects,
        # because it binds against HEAD^. The two checks contradicted each other
        # and no value could satisfy both, so the release could not be tagged.
        with tempfile.TemporaryDirectory() as directory:
            self._plan(directory, "9.9.9", self.BRANCH_PARENT)
            with (
                mock.patch.object(preflight, "REPO_ROOT", Path(directory)),
                mock.patch.object(
                    preflight,
                    "_git",
                    self._git(
                        {
                            ("rev-parse", "origin/main"): self.MAIN,
                            ("rev-parse", "HEAD"): self.MAIN,
                            ("rev-parse", "HEAD^"): self.BRANCH_PARENT,
                        }
                    ),
                ),
            ):
                self.assertIn(
                    "matches HEAD^",
                    preflight.check_release_plan_evidence_base("9.9.9"),
                )

    def test_rejects_the_tagged_commit_itself_once_merged(self):
        with tempfile.TemporaryDirectory() as directory:
            self._plan(directory, "9.9.9", self.MAIN)
            with (
                mock.patch.object(preflight, "REPO_ROOT", Path(directory)),
                mock.patch.object(
                    preflight,
                    "_git",
                    self._git(
                        {
                            ("rev-parse", "origin/main"): self.MAIN,
                            ("rev-parse", "HEAD"): self.MAIN,
                            ("rev-parse", "HEAD^"): self.BRANCH_PARENT,
                        }
                    ),
                ),
            ):
                with self.assertRaisesRegex(
                    preflight.PreflightError, "the release has merged"
                ):
                    preflight.check_release_plan_evidence_base("9.9.9")

    def test_skips_a_plan_that_records_nothing_yet(self):
        with tempfile.TemporaryDirectory() as directory:
            plans = Path(directory) / "docs" / "03_Plans" / "active"
            plans.mkdir(parents=True)
            (plans / "2026-01-01-release-9.9.9-tranche.md").write_text(
                "## Goal\n", encoding="utf-8"
            )
            with (
                mock.patch.object(preflight, "REPO_ROOT", Path(directory)),
                mock.patch.object(
                    preflight,
                    "_git",
                    self._git({("rev-parse", "origin/main"): self.MAIN}),
                ),
            ):
                self.assertIn(
                    "records no evidence base commit",
                    preflight.check_release_plan_evidence_base("9.9.9"),
                )


class SensitivePatternParityTest(unittest.TestCase):
    """The gap that spent v2.7.7.

    The release feed is a repository secret and a strict superset of the local
    pattern file, so a local run cannot see what the publish gate will refuse.
    Moving that scan ahead of the tag is only useful if its absence is loud.
    """

    def test_a_checkout_without_the_feed_is_refused(self):
        with self.assertRaises(preflight.PreflightError) as caught:
            preflight.check_sensitive_pattern_parity(environment={})

        message = str(caught.exception)
        self.assertIn(preflight.PATTERN_FEED_VARIABLE, message)
        # The reason the timing matters has to survive in the message.
        self.assertIn("immutable", message)

    def test_an_acknowledged_gap_is_allowed_and_says_so(self):
        result = preflight.check_sensitive_pattern_parity(
            environment={preflight.PATTERN_PARITY_ACKNOWLEDGEMENT: "1"}
        )

        self.assertIn("UNVERIFIED", result)
        # It must direct the operator to record it, not merely pass quietly.
        self.assertIn("release authorization", result)

    def test_a_present_feed_runs_the_real_scan_and_refuses_a_match(self):
        environment = {preflight.PATTERN_FEED_VARIABLE: "definitely-a-customer-name"}
        with mock.patch.object(preflight.subprocess, "run") as run:
            run.return_value = mock.Mock(
                returncode=1,
                stdout="path:x.py:1: env literal pattern line 1\n",
                stderr="",
            )
            with self.assertRaises(preflight.PreflightError) as caught:
                preflight.check_sensitive_pattern_parity(environment=environment)

        self.assertIn("refused this tree", str(caught.exception))
        self.assertIn("env literal pattern", str(caught.exception))

    def test_a_present_feed_that_scans_clean_reports_verified(self):
        environment = {preflight.PATTERN_FEED_VARIABLE: "definitely-a-customer-name"}
        with mock.patch.object(preflight.subprocess, "run") as run:
            run.return_value = mock.Mock(returncode=0, stdout="", stderr="")
            result = preflight.check_sensitive_pattern_parity(environment=environment)

        self.assertIn("verified", result)
        # The scan must be the release-time one, superset feed included.
        arguments = run.call_args.args[0]
        self.assertIn("--require-env-patterns", arguments)
        self.assertIn("--protected-history", arguments)


def _dependabot_alert(*, ghsa, severity, package="sqlparse"):
    return {
        "security_advisory": {"ghsa_id": ghsa, "severity": severity},
        "dependency": {"package": {"name": package}},
    }


class DependabotAlertsTest(unittest.TestCase):
    """Pins the gap `constraints.txt`-only `pip-audit` cannot see: a
    transitive dependency's advisory (the real sqlparse alerts Dependabot
    caught after v2.9.0 shipped, unpinned in `constraints.txt`)."""

    def setUp(self):
        patcher = mock.patch.object(
            preflight, "_dependabot_token", return_value="test-token"
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_no_open_alerts_passes_silently(self):
        with mock.patch.object(preflight.provenance, "_github_json", return_value=[]):
            result = preflight.check_dependabot_alerts()
        self.assertIn("no open high/critical", result)

    def test_a_medium_severity_alert_does_not_block(self):
        alerts = [_dependabot_alert(ghsa="GHSA-medium", severity="medium")]
        with mock.patch.object(
            preflight.provenance, "_github_json", return_value=alerts
        ):
            result = preflight.check_dependabot_alerts()
        self.assertIn("no open high/critical", result)

    def test_an_unaccepted_high_severity_alert_blocks(self):
        alerts = [_dependabot_alert(ghsa="GHSA-high-one", severity="high")]
        with mock.patch.object(
            preflight.provenance, "_github_json", return_value=alerts
        ):
            with self.assertRaises(preflight.PreflightError) as caught:
                preflight.check_dependabot_alerts()
        self.assertIn("sqlparse", str(caught.exception))
        self.assertIn("GHSA-high-one", str(caught.exception))

    def test_an_accepted_high_severity_alert_does_not_block(self):
        alerts = [_dependabot_alert(ghsa="GHSA-high-one", severity="high")]
        with mock.patch.object(
            preflight, "ACCEPTED_DEPENDABOT_ALERTS", {"GHSA-high-one"}
        ):
            with mock.patch.object(
                preflight.provenance, "_github_json", return_value=alerts
            ):
                result = preflight.check_dependabot_alerts()
        self.assertIn("GHSA-high-one", result)

    def test_no_token_available_fails_closed(self):
        with mock.patch.object(preflight, "_dependabot_token") as token:
            token.side_effect = preflight.PreflightError("no token")
            with self.assertRaises(preflight.PreflightError):
                preflight.check_dependabot_alerts()

    def test_a_transport_failure_is_translated_and_does_not_pass_silently(self):
        with mock.patch.object(
            preflight.provenance,
            "_github_json",
            side_effect=preflight.provenance.ProvenanceError("boom"),
        ):
            with self.assertRaises(preflight.PreflightError) as caught:
                preflight.check_dependabot_alerts()
        self.assertIn("boom", str(caught.exception))
