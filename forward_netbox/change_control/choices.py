# The workflow vocabulary. Deliberately the terms already used in the field
# rather than invented ones, because a HOLD that is a VALID RESULT rather than
# an error is the property most home-grown change gates get wrong, and naming
# it the same way everywhere is most of what stops that.
from django.utils.translation import gettext_lazy as _
from utilities.choices import ChoiceSet


class ForwardChangeStateChoices(ChoiceSet):
    """Eleven states, ten of them mandatory.

    `PREDICTED` is the optional one: `STAGED -> APPROVED` is a first-class
    transition, not a degradation. Forward's predict workflow is not live, is
    licence-gated when it ships, and is limited in scope today, so the machine
    is designed to be correct without it and to treat its arrival as additive.
    """

    DRAFT = "draft"
    SCOPED = "scoped"
    BASELINED = "baselined"
    STAGED = "staged"
    PREDICTED = "predicted"
    APPROVED = "approved"
    APPLIED = "applied"
    COLLECTED = "collected"
    VERIFIED_PROCEED = "verified-proceed"
    VERIFIED_HOLD = "verified-hold"
    CLOSED = "closed"
    ABANDONED = "abandoned"

    CHOICES = [
        (DRAFT, _("Draft"), "gray"),
        (SCOPED, _("Scoped"), "gray"),
        (BASELINED, _("Baselined"), "cyan"),
        (STAGED, _("Staged"), "cyan"),
        (PREDICTED, _("Predicted"), "blue"),
        (APPROVED, _("Approved"), "blue"),
        (APPLIED, _("Applied"), "purple"),
        (COLLECTED, _("Collected"), "purple"),
        (VERIFIED_PROCEED, _("Verified - proceed"), "green"),
        (VERIFIED_HOLD, _("Verified - hold"), "orange"),
        (CLOSED, _("Closed"), "green"),
        (ABANDONED, _("Abandoned"), "red"),
    ]

    # A hold is not terminal and is never closed: it is fixed and re-verified.
    # Only PROCEED reaches CLOSED.
    TERMINAL = frozenset({CLOSED, ABANDONED})
    OPEN = frozenset(
        {
            DRAFT,
            SCOPED,
            BASELINED,
            STAGED,
            PREDICTED,
            APPROVED,
            APPLIED,
            COLLECTED,
            VERIFIED_PROCEED,
            VERIFIED_HOLD,
        }
    )


class ForwardChangeVerdictChoices(ChoiceSet):
    """The verify gate's answer. `HOLD` is a result, not a failure to answer."""

    PROCEED = "proceed"
    HOLD = "hold"

    CHOICES = [
        (PROCEED, _("Proceed"), "green"),
        (HOLD, _("Hold"), "orange"),
    ]


class ForwardCriterionFamilyChoices(ChoiceSet):
    """What a criterion is for.

    ACCEPTANCE asserts the change achieved its intent. STATE_PRESERVATION
    asserts nothing else moved - the denominator for that is a genuinely open
    question, recorded in the plan, and is why the family is a field rather
    than an assumption.
    """

    ACCEPTANCE = "acceptance"
    STATE_PRESERVATION = "state-preservation"

    CHOICES = [
        (ACCEPTANCE, _("Acceptance"), "blue"),
        (STATE_PRESERVATION, _("State preservation"), "purple"),
    ]


class ForwardCriterionExpectationChoices(ChoiceSet):
    """How a criterion's rows are turned into pass or fail."""

    NO_ROWS = "no-rows"
    SOME_ROWS = "some-rows"
    NO_DIFF = "no-diff"

    CHOICES = [
        (NO_ROWS, _("No rows"), "green"),
        (SOME_ROWS, _("At least one row"), "blue"),
        (NO_DIFF, _("No change from baseline"), "purple"),
    ]


class ForwardEvidencePhaseChoices(ChoiceSet):
    """Evidence is recorded at two pinned snapshots, never at "now"."""

    BEFORE = "before"
    AFTER = "after"

    CHOICES = [
        (BEFORE, _("Before")),
        (AFTER, _("After")),
    ]


class ForwardReviewPhaseChoices(ChoiceSet):
    """Which gate a review opens.

    PRE approves the plan: the branch and the baseline evidence. POST approves
    the CLOSURE, after verification, which is where the branch actually
    merges - the consequential act, and the reason a machine verdict alone is
    not enough to reach it.
    """

    PRE = "pre"
    POST = "post"

    CHOICES = [
        (PRE, _("Pre-change"), "blue"),
        (POST, _("Post-change"), "green"),
    ]
