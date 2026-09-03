# Which branch this checkout is allowed to release from.
#
# The lane is declared rather than derived, so the failure worth guarding is a
# merge that carries one lane's declaration onto the other's branch. Ancestry
# would still refuse the release, but with an error about merge-base that reads
# as a git problem rather than as "you are releasing the wrong series here".
import unittest.mock

from scripts.release_lane import LANE
from scripts.release_lane import RELEASE_BRANCH
from scripts.release_lane import ReleaseLane
from scripts.release_lane import ReleaseLaneError
from scripts.release_lane import REMOTE_RELEASE_REF
from scripts.release_lane import require_version_in_lane


class TheLaneNamesOneBranchTest(unittest.TestCase):
    def test_the_refs_all_derive_from_the_branch(self):
        lane = ReleaseLane(branch="maint/1.2.x", ruleset="whatever", series="1.2")
        self.assertEqual(lane.remote_ref, "origin/maint/1.2.x")
        self.assertEqual(lane.remote_tracking_ref, "refs/remotes/origin/maint/1.2.x")
        self.assertEqual(lane.ref_pattern, "refs/heads/maint/1.2.x")

    def test_the_module_constants_agree_with_the_lane(self):
        self.assertEqual(RELEASE_BRANCH, LANE.branch)
        self.assertEqual(REMOTE_RELEASE_REF, LANE.remote_ref)

    def test_a_branch_with_a_slash_survives_every_ref_form(self):
        # `maint/2.9.x` is the first release branch with a slash in it, and a
        # ref built by concatenation is the obvious place for that to break.
        # Asserted against a constructed lane rather than this one, so the
        # property is pinned on every branch and not only where it happens to
        # apply.
        lane = ReleaseLane(branch="maint/2.9.x", ruleset="whatever", series="2.9")
        self.assertTrue(lane.ref_pattern.endswith(lane.branch))
        self.assertTrue(lane.remote_tracking_ref.endswith(lane.branch))


class AConfinedLaneRefusesAnotherSeriesTest(unittest.TestCase):
    """Asserted against a constructed maintenance lane.

    This branch is the trunk and declares no series, so the guard does not
    apply to it - but the guard still has to work, because the branch that
    needs it carries the same module.
    """

    MAINTENANCE = ReleaseLane(
        branch="maint/2.9.x", ruleset="maint-2-9-x-release-integrity", series="2.9"
    )

    def test_a_version_in_the_lane_passes(self):
        with unittest.mock.patch("scripts.release_lane.LANE", self.MAINTENANCE):
            require_version_in_lane("2.9.0")
            require_version_in_lane("2.9.99")

    def test_a_version_from_another_series_is_refused_by_name(self):
        with unittest.mock.patch("scripts.release_lane.LANE", self.MAINTENANCE):
            with self.assertRaises(ReleaseLaneError) as raised:
                require_version_in_lane("3.0.1")
        message = str(raised.exception)
        # The message must name BOTH series and the branch: the operator's next
        # move is to release it from its own lane, and a bare "wrong series"
        # does not say which lane that is.
        self.assertIn("3.0", message)
        self.assertIn("2.9", message)
        self.assertIn("maint/2.9.x", message)

    def test_a_longer_version_is_still_matched_on_its_series(self):
        with unittest.mock.patch("scripts.release_lane.LANE", self.MAINTENANCE):
            with self.assertRaises(ReleaseLaneError):
                require_version_in_lane("10.9.3")


class TheTrunkAcceptsAnySeriesTest(unittest.TestCase):
    """The trunk is where the next series is born.

    Pinning a series here would refuse the next minor bump and would have to be
    edited on every one of them. Ancestry stays the real gate.
    """

    def test_this_lane_declares_no_series(self):
        self.assertIsNone(LANE.series)

    def test_this_branch_releases_the_trunk(self):
        # A merge that carried the maintenance branch's declaration here would
        # otherwise be caught only by a release failing on ancestry.
        self.assertEqual(LANE.branch, "main")
        self.assertEqual(LANE.ruleset, "main-release-integrity")

    def test_it_refuses_nothing_on_series(self):
        require_version_in_lane("3.0.1")
        require_version_in_lane("4.0.0")
        require_version_in_lane("2.9.3")
