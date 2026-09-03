# How many approvals a change needs, and from whom.
#
# One helper for both gates so the count rule lives in one place. Before this,
# ForwardChangePolicy was inert: nothing read `min_approvals`, nothing
# evaluated a rule, and `_approved()` accepted any single approving row.
from .choices import ForwardReviewPhaseChoices

# With no policy matching a change, require one approval at each gate rather
# than none. An unmatched change must not be EASIER to merge than a matched
# one - that is the failure mode where adding a policy to cover one estate
# quietly leaves everything else ungoverned.
DEFAULT_REQUIRED_APPROVALS = 1


def applicable_policies(change):
    """Enabled policies whose rules match the change's device scope.

    Scoped by device role, site or tag - the properties a NETWORK change has -
    rather than by NetBox object type, because that is how an operator reasons
    about which changes need which approvers.

    A policy with NO rules applies to every change. A policy with rules
    applies when any rule matches any device in scope.
    """
    from ..models import ForwardChangePolicy

    devices = [scoped.device for scoped in change.devices.select_related("device")]
    matched = []
    for policy in ForwardChangePolicy.objects.filter(enabled=True).prefetch_related(
        "rules"
    ):
        rules = list(policy.rules.all())
        if not rules:
            matched.append(policy)
            continue
        if any(_rule_matches(rule, devices) for rule in rules):
            matched.append(policy)
    return matched


def _rule_matches(rule, devices) -> bool:
    for device in devices:
        if rule.device_role_id and device.role_id != rule.device_role_id:
            continue
        if rule.site_id and device.site_id != rule.site_id:
            continue
        if rule.tag_slug and not device.tags.filter(slug=rule.tag_slug).exists():
            continue
        # Every populated facet matched, and an empty facet is a wildcard.
        if rule.device_role_id or rule.site_id or rule.tag_slug:
            return True
    return False


def required_approvals(change, phase: str) -> int:
    """How many distinct approvals this gate needs.

    The STRICTEST applicable policy wins. Overlapping policies are a way to
    add requirements, never to relax them - otherwise attaching a permissive
    policy would weaken a stricter one that already covered the estate.
    """
    policies = applicable_policies(change)
    if not policies:
        return DEFAULT_REQUIRED_APPROVALS
    field = (
        "min_pre_approvals"
        if phase == ForwardReviewPhaseChoices.PRE
        else "min_post_approvals"
    )
    return max(
        getattr(policy, field, DEFAULT_REQUIRED_APPROVALS) for policy in policies
    )


def distinct_approvers(change, phase: str) -> set:
    """Reviewer ids that approved this gate, excluding the requester.

    Self-approval is not an approval. The requester is excluded here rather
    than at write time so an existing review cannot become valid by someone
    later being made the requester.
    """
    approved = change.reviews.filter(phase=phase, approved=True)
    return {
        review.reviewer_id
        for review in approved
        if review.reviewer_id and review.reviewer_id != change.requester_id
    }


def stale_approvals(change, phase: str) -> list:
    """Approvals whose anchors no longer match the change.

    An approval that survives an edit to the thing it approved is not an
    approval. The two gates anchor to different evidence because they are
    approving different things.
    """
    stale = []
    for review in change.reviews.filter(phase=phase, approved=True):
        if phase == ForwardReviewPhaseChoices.PRE:
            moved = (
                review.branch_change_time != change.branch_last_change_time
                or review.baseline_snapshot_id != change.before_snapshot_id
            )
        else:
            moved = (
                review.after_snapshot_id != change.after_snapshot_id
                or review.verdict != change.verdict
            )
        if moved:
            stale.append(review)
    return stale
