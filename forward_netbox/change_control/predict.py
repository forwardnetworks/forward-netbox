# Forward's predict workflow, stubbed behind a real interface.
#
# Three facts shape this. Predict is not live at Forward. It is licence-gated
# when it does ship. What exists today is limited in scope. So the workflow is
# designed to be correct with predict entirely absent, and its arrival is
# additive: filling this in must not reshape the state machine or any model,
# which is why the advisory columns ship in the first migration.
#
# Availability is discovered by ASKING and reading the refusal, never by
# consulting a licence tier first. `utilities/license_tier.py` says the tier is
# not readable from the API and warns against exactly that gate; a wrong
# capability table sends an operator to buy the wrong thing.
from dataclasses import dataclass


@dataclass(frozen=True)
class PredictOutcome:
    """What a prediction attempt produced. Never an exception at the caller.

    `status` is one of:
      unavailable  - not live, or this licence does not include it
      unsupported  - live, but it cannot answer THIS question yet
      answered     - a real prediction

    The two non-answers are distinct on purpose. "We could not ask" and "we
    asked and it could not say" mean different things to someone deciding
    whether to approve, and collapsing them into one greyed-out panel is how a
    reader concludes the change was checked when it was not.
    """

    status: str
    reason: str = ""
    predicted_snapshot_id: str = ""
    criteria: tuple = ()
    pre_verdict: str = ""

    UNAVAILABLE = "unavailable"
    UNSUPPORTED = "unsupported"
    ANSWERED = "answered"

    @property
    def is_answer(self) -> bool:
        return self.status == self.ANSWERED


def predict_change(
    *,
    source,
    snapshot_id: str,
    devices=(),
    proposal=None,
    criteria=(),
) -> PredictOutcome:
    """Ask Forward what this change would do. Today: always `unavailable`.

    The signature is the contract, not a placeholder. When predict ships, this
    body calls it and returns `answered`; everything above it - the state
    machine, the models, the page - already handles all three statuses.

    It returns rather than raises even for a licence denial, because a
    capability the customer has not bought is not a fault. `ForwardLicenseTierError`
    is already documented that way, and the caller must not have to know.
    """
    if not predict_enabled(source):
        return PredictOutcome(
            status=PredictOutcome.UNAVAILABLE,
            reason=(
                "Predict is not enabled on this Forward source, so it was not "
                "asked. The change can still be approved and verified: the "
                "verdict is computed from the post-change snapshot, which "
                "does not depend on predict."
            ),
        )

    # Enabled, and still unavailable: the workflow is not generally available
    # at Forward yet. Kept distinct from the not-enabled case above because an
    # operator who turned it ON deserves to know the answer is upstream, not a
    # setting they missed.
    return PredictOutcome(
        status=PredictOutcome.UNAVAILABLE,
        reason=(
            "Predict is enabled for this source but Forward's predict "
            "workflow is not available on this deployment - it is licensed "
            "separately and is limited in scope today. Nothing is wrong with "
            "the change, and the verdict does not depend on predict."
        ),
    )


def predict_enabled(source) -> bool:
    """Whether this source opts in to asking Forward to predict.

    Off unless a deployment says otherwise, and read with `.get(..., False)`
    so an existing source with no such key is off without a data migration.
    Fails closed for the same reason every other capability here does: a
    missing setting must never read as permission.
    """
    parameters = getattr(source, "parameters", None) or {}
    return bool(parameters.get("enable_predict", False))


def render_predict_panel(outcome: PredictOutcome) -> dict:
    """How the page shows it. Informational, never an error.

    A non-answer renders as a plain panel with a sentence. No red, no retry
    button, no empty result table that reads as "nothing found" - because
    "we asked and got no answer" must never render as "we asked and it was
    fine".
    """
    if outcome.is_answer:
        return {
            "level": "info",
            "heading": "Forward prediction",
            "message": outcome.reason or "Forward predicted this change.",
            "verdict": outcome.pre_verdict,
            "advisory": True,
        }
    heading = {
        PredictOutcome.UNAVAILABLE: "Forward prediction not available",
        PredictOutcome.UNSUPPORTED: "Forward cannot predict this change yet",
    }.get(outcome.status, "Forward prediction")
    return {
        "level": "info",
        "heading": heading,
        "message": outcome.reason,
        "verdict": "",
        "advisory": True,
    }
