# Which branch this checkout releases from.
#
# Until 3.0.0 there was one lane and "main" was written into the release
# tooling about thirty times. That was correct while it was true, and it stopped
# being true the moment `main` moved to NetBox 4.7 only: the 4.6 lane lives on
# `maint/2.9.x`, and a tag cut there was refused by every gate that asked
# whether the commit was on main.
#
# The lane is DECLARED here rather than derived, for the same reason
# `tested_runtime.py` declares runtime pins: a value inferred from the current
# checkout is a value that silently follows a bad merge, and this one decides
# which branch's history a release is allowed to come from.
#
# Each lane carries its own copy of this file, so the constants differ between
# branches by design. That is safe in both directions - a wrong value fails
# closed, because the tagged commit will not be an ancestor of the branch the
# file names - and `require_version_in_lane` turns that failure from a confusing
# ancestry error into a sentence naming the mistake.
from dataclasses import dataclass


@dataclass(frozen=True)
class ReleaseLane:
    """The branch a release comes from, and what proves it is protected.

    `series` is the version series this lane is CONFINED to, and it is optional
    on purpose. A maintenance lane exists to carry exactly one series, so
    pinning it there is the point. The trunk is where new series are born - it
    released 2.9, then 3.0, and will release whatever comes next - so pinning it
    would refuse the next minor bump and would have to be edited on every one of
    them. `None` means "this lane is not confined", and ancestry remains the
    real gate either way.
    """

    branch: str
    ruleset: str
    series: str | None = None

    @property
    def remote_ref(self) -> str:
        """`origin/<branch>`, for rev-parse and `--base` arguments."""
        return f"origin/{self.branch}"

    @property
    def remote_tracking_ref(self) -> str:
        """The full remote-tracking ref, for an unambiguous rev-parse."""
        return f"refs/remotes/origin/{self.branch}"

    @property
    def ref_pattern(self) -> str:
        """The ruleset ref pattern that must protect this branch."""
        return f"refs/heads/{self.branch}"


# `main` is the trunk: the NetBox 4.7 line today, and whatever line comes after
# it. No series, because the trunk is where the next one is born - see
# `ReleaseLane.series`.
LANE = ReleaseLane(
    branch="main",
    ruleset="main-release-integrity",
)

RELEASE_BRANCH = LANE.branch
REMOTE_RELEASE_REF = LANE.remote_ref


class ReleaseLaneError(RuntimeError):
    """This version does not belong to the lane this checkout releases."""


def require_version_in_lane(version: str) -> None:
    """Refuse a version from another series before anything else runs.

    The failure this exists to catch is a merge that carries one lane's
    `LANE` onto the other's branch. Ancestry would still refuse the release,
    but with an error about merge-base that reads as a git problem rather than
    as "you are releasing 3.0.1 from the 2.9 branch".

    A lane with no declared series accepts any version. That is not a weakened
    check, it is the absence of one that never applied: the trunk is where the
    next series comes from, and refusing it there would be refusing the normal
    case.
    """
    if LANE.series is None:
        return
    series = ".".join(str(version).split(".")[:2])
    if series != LANE.series:
        raise ReleaseLaneError(
            f"version {version} is in the {series} series, but this checkout "
            f"releases the {LANE.series} series from {LANE.branch}. Release it "
            f"from its own lane."
        )
