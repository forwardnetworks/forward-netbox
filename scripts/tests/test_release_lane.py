# Which branch this checkout is allowed to release from.
#
# The lane is declared rather than derived, so the failure worth guarding is a
# merge that carries one lane's declaration onto the other's branch. Ancestry
# would still refuse the release, but with an error about merge-base that reads
# as a git problem rather than as "you are releasing the wrong series here".
import unittest

from scripts.release_lane import LANE
from scripts.release_lane import RELEASE_BRANCH
from scripts.release_lane import ReleaseLane
from scripts.release_lane import ReleaseLaneError
from scripts.release_lane import REMOTE_RELEASE_REF
from scripts.release_lane import require_version_in_lane


class TheLaneNamesOneBranchTest(unittest.TestCase):
    def test_the_refs_all_derive_from_the_branch(self):
        lane = ReleaseLane(branch="maint/1.2.x", series="1.2", ruleset="whatever")
        self.assertEqual(lane.remote_ref, "origin/maint/1.2.x")
        self.assertEqual(lane.remote_tracking_ref, "refs/remotes/origin/maint/1.2.x")
        self.assertEqual(lane.ref_pattern, "refs/heads/maint/1.2.x")

    def test_the_module_constants_agree_with_the_lane(self):
        self.assertEqual(RELEASE_BRANCH, LANE.branch)
        self.assertEqual(REMOTE_RELEASE_REF, LANE.remote_ref)

    def test_a_branch_with_a_slash_survives_every_ref_form(self):
        # `maint/2.9.x` is the first release branch with a slash in it, and a
        # ref built by concatenation is the obvious place for that to break.
        self.assertIn("/", LANE.branch)
        self.assertTrue(LANE.ref_pattern.endswith(LANE.branch))
        self.assertTrue(LANE.remote_tracking_ref.endswith(LANE.branch))


class AVersionFromAnotherSeriesIsRefusedTest(unittest.TestCase):
    def test_a_version_in_the_lane_passes(self):
        require_version_in_lane(f"{LANE.series}.0")
        require_version_in_lane(f"{LANE.series}.99")

    def test_a_version_from_another_series_is_refused_by_name(self):
        with self.assertRaises(ReleaseLaneError) as raised:
            require_version_in_lane("3.0.1")
        message = str(raised.exception)
        # The message must name BOTH series and the branch: the operator's next
        # move is to release it from its own lane, and a bare "wrong series"
        # does not say which lane that is.
        self.assertIn("3.0", message)
        self.assertIn(LANE.series, message)
        self.assertIn(LANE.branch, message)

    def test_a_longer_version_is_still_matched_on_its_series(self):
        with self.assertRaises(ReleaseLaneError):
            require_version_in_lane("10.9.3")
