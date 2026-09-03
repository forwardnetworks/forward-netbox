# Turning two snapshots of criterion results into a verdict.
#
# The regression-flip comparison is the whole reason evidence is recorded at a
# baseline as well as after the change. Without a before-phase row, a criterion
# that was ALREADY failing gets counted as damage this change caused, and the
# gate blocks a change that fixed nothing and broke nothing.
from dataclasses import dataclass

from .choices import ForwardChangeVerdictChoices
from .choices import ForwardCriterionFamilyChoices


# What a before/after pair means. Only two of the four block, and saying so in
# one table is clearer than four branches in the gate.
FIX = "fix"
REGRESSION = "regression"
PRE_EXISTING = "pre-existing"
PRESERVED = "preserved"

_FLIP = {
    (False, True): FIX,
    (True, False): REGRESSION,
    (False, False): PRE_EXISTING,
    (True, True): PRESERVED,
}

# State preservation blocks only on a regression: a criterion that was already
# failing is pre-existing and this change neither caused it nor was asked to
# fix it.
BLOCKING_FLIPS = frozenset({REGRESSION})

# Acceptance is a different question and needs a different rule. It asserts the
# change ACHIEVED ITS INTENT, so what matters is the after state on its own:
# `fail -> fail` means the change did not work, and letting that through
# because "it was already failing" would verify a change that accomplished
# nothing. The before phase is still recorded, because it is what distinguishes
# "did not work" from "broke something".
ACCEPTANCE_BLOCKING_FLIPS = frozenset({REGRESSION, PRE_EXISTING})


@dataclass(frozen=True)
class CriterionOutcome:
    """One criterion across both phases."""

    criterion: object
    before_passed: bool | None
    after_passed: bool | None
    flip: str
    blocking: bool

    @property
    def blocks(self) -> bool:
        if not self.blocking:
            return False
        return self.flip in self.blocking_flips

    @property
    def blocking_flips(self) -> frozenset:
        """Which flips block, which depends on what the criterion asserts."""
        from .choices import ForwardCriterionFamilyChoices

        family = getattr(self.criterion, "family", None)
        if family == ForwardCriterionFamilyChoices.ACCEPTANCE:
            return ACCEPTANCE_BLOCKING_FLIPS
        return BLOCKING_FLIPS

    @property
    def block_reason(self) -> str:
        """Why it blocks, in the operator's terms rather than the flip's."""
        if not self.blocks:
            return ""
        if self.flip == REGRESSION:
            return "passed at the baseline and fails now: this change regressed it"
        return "still fails: the change did not achieve what it asserted"


def classify_flip(before_passed: bool | None, after_passed: bool | None) -> str:
    """The before/after pair, named.

    A missing phase is NOT coerced to a pass or a fail. "Not measured" is a
    third answer and it must reach the verdict as one, because a criterion
    nobody evaluated is not a criterion that passed - that coercion is exactly
    how an empty comparison once read as a successful one.
    """
    if before_passed is None or after_passed is None:
        return ""
    return _FLIP[(bool(before_passed), bool(after_passed))]


def criterion_outcomes(change) -> list[CriterionOutcome]:
    """Pair up each criterion's before and after evidence."""
    from ..models import ForwardChangeEvidence
    from .choices import ForwardEvidencePhaseChoices as Phase

    criteria = list(change.criteria.all())
    if not criteria:
        # Nothing to pair, so nothing to fetch. The caller turns this into an
        # honest HOLD rather than an empty PROCEED.
        return []

    rows = ForwardChangeEvidence.objects.filter(criterion__change=change)
    by_criterion: dict[int, dict[str, bool]] = {}
    for row in rows:
        by_criterion.setdefault(row.criterion_id, {})[row.phase] = row.passed

    outcomes = []
    for criterion in criteria:
        phases = by_criterion.get(criterion.pk, {})
        before = phases.get(Phase.BEFORE)
        after = phases.get(Phase.AFTER)
        outcomes.append(
            CriterionOutcome(
                criterion=criterion,
                before_passed=before,
                after_passed=after,
                flip=classify_flip(before, after),
                blocking=criterion.blocking,
            )
        )
    return outcomes


def compute_verdict(change) -> tuple[str, list[str]]:
    """PROCEED or HOLD, and the reasons for a HOLD.

    A HOLD is a valid result. It is returned with its reasons, not raised, and
    the workflow keeps it open for a fix and a re-verify rather than closing
    it.
    """
    outcomes = criterion_outcomes(change)
    reasons = []

    if not outcomes:
        return ForwardChangeVerdictChoices.HOLD, [
            "No criteria were evaluated, so there is nothing to verify against."
        ]

    unmeasured = [o for o in outcomes if o.blocking and not o.flip]
    for outcome in unmeasured:
        missing = "before" if outcome.before_passed is None else "after"
        reasons.append(
            f"'{outcome.criterion.name}' has no {missing} evidence, so it was "
            f"not measured. That is not the same as passing."
        )

    for outcome in outcomes:
        if outcome.blocks:
            reasons.append(f"'{outcome.criterion.name}' {outcome.block_reason}.")

    moved = [
        o
        for o in outcomes
        if o.criterion.family == ForwardCriterionFamilyChoices.STATE_PRESERVATION
        and o.flip == REGRESSION
    ]
    if moved:
        reasons.append(
            f"{len(moved)} state-preservation criterion(s) moved, so something "
            f"outside the change's declared scope is no longer as it was."
        )

    verdict = (
        ForwardChangeVerdictChoices.HOLD
        if reasons
        else ForwardChangeVerdictChoices.PROCEED
    )
    return verdict, reasons
