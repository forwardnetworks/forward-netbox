# `v2.7.10` was tagged and refused for a lineage of three commits where four are
# required. That is a pure-git fact, knowable before the tag existed - and a tag
# is immutable, so discovering it afterwards costs a version number.
#
# These tests pin the arithmetic and the pairing rule, which are the two things
# a release author gets wrong by squashing the production content into the
# release commit.
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = REPO_ROOT / "scripts"

if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import check_release_lineage  # noqa: E402
import verify_release_provenance as provenance  # noqa: E402


class ReleaseLineageTest(unittest.TestCase):
    def setUp(self):
        self.bridge = "b" * 40
        self.control = "c" * 40
        self.production = "p" * 40
        self.release = "r" * 40
        self.prior_tag_commit = "t" * 40

        self._patched = {}
        self._patch(provenance, "PRIOR_POST_RELEASE_DOC_COMMIT", self.bridge)
        self._patch(
            provenance, "_require_annotated_tag", lambda tag: self.prior_tag_commit
        )
        self._patch(
            provenance,
            "_commit_parent",
            lambda commit: (
                self.prior_tag_commit if commit == self.bridge else self.production
            ),
        )
        self._patch(provenance, "_git_capture", lambda *args: self.release)
        self._patch(provenance, "_require_release_plan", lambda *a, **k: "plan.md")
        self._patch(provenance, "_require_security_bootstrap", lambda commit: None)

    def _patch(self, module, name, value):
        self._patched[(module, name)] = getattr(module, name)
        setattr(module, name, value)

    def tearDown(self):
        for (module, name), value in self._patched.items():
            setattr(module, name, value)

    def _lineage(self, commits):
        provenance._first_parent_commits = lambda start, end: commits

    def test_a_three_commit_lineage_is_refused(self):
        # Exactly what burned v2.7.10: bridge, anchor, release.
        self._lineage([self.bridge, self.production, self.release])

        with self.assertRaises(check_release_lineage.LineageError) as caught:
            check_release_lineage.check_release_lineage(self.release, "2.7.11")

        message = str(caught.exception)
        self.assertIn("3 commits", message)
        self.assertIn("at least 4", message)

    def test_a_four_commit_lineage_is_accepted(self):
        self._lineage([self.bridge, self.control, self.production, self.release])

        result = check_release_lineage.check_release_lineage(self.release, "2.7.11")

        self.assertEqual(result["lineage_length"], 4)
        self.assertEqual(result["production_commit"], self.production)

    def test_the_last_two_commits_must_be_the_production_and_release_pair(self):
        # A lineage long enough but ending on the wrong pair - the release
        # commit's parent is not the commit immediately before it.
        self._lineage([self.bridge, self.control, "d" * 40, self.release])

        with self.assertRaises(check_release_lineage.LineageError) as caught:
            check_release_lineage.check_release_lineage(self.release, "2.7.11")

        self.assertIn("production commit", str(caught.exception))

    def test_a_lineage_starting_off_the_recorded_bridge_is_refused(self):
        self._lineage([self.control, self.production, self.release, "e" * 40])

        with self.assertRaises(check_release_lineage.LineageError) as caught:
            check_release_lineage.check_release_lineage(self.release, "2.7.11")

        self.assertIn("post-release bridge", str(caught.exception))

    def test_it_does_not_claim_to_check_what_needs_github(self):
        # A second implementation of a security check is a second thing to get
        # wrong. The report says plainly what remains unverified locally.
        self._lineage([self.bridge, self.control, self.production, self.release])

        result = check_release_lineage.check_release_lineage(self.release, "2.7.11")

        self.assertIn("not_checked_here", result)
        self.assertTrue(
            any("pull request" in item for item in result["not_checked_here"])
        )


if __name__ == "__main__":
    unittest.main()
