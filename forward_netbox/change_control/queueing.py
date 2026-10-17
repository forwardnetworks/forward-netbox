# Starting Forward-facing work from a request without holding the request open.
#
# A validation run or a verification can take as long as Forward takes to answer
# a handful of queries, so the web page and the REST API queue a NetBox job and
# say so. The job calls the same service functions the tests call inline.
from django.utils import timezone

from .choices import ForwardValidationTriggerChoices as Trigger
from .validation import start_run
from .validation import ValidationOutcome


def enqueue_validation(change, user, trigger=Trigger.MANUAL, policy=None):
    """Create a pending run and queue its job."""
    from ..jobs import ChangeValidationJob

    run, reason = start_run(change, user, trigger, policy)
    if run is None:
        return ValidationOutcome(False, reason)
    try:
        ChangeValidationJob.enqueue(
            instance=change,
            user=user,
            name=f"validate change {change.pk}",
            run_id=run.pk,
        )
    except (
        Exception
    ) as exc:  # noqa: BLE001 - the queue being down must not strand the run
        run.status = "failed"
        run.error = (
            f"The job could not be queued: {type(exc).__name__}: {str(exc)[:300]}"
        )
        run.completed_at = timezone.now()
        run.save(update_fields=["status", "error", "completed_at"])
        return ValidationOutcome(False, run.error, run.pk)
    return ValidationOutcome(True, "Validation queued.", run.pk)


def enqueue_verify(change, user):
    """Queue verification of a change, unless it cannot be verified at all."""
    from ..jobs import ChangeVerifyJob
    from .actions import _journal
    from .choices import ForwardChangeStateChoices as State

    if change.state not in (State.APPLIED, State.COLLECTED):
        return ValidationOutcome(
            False, "Only an applied or collected change can be verified."
        )
    try:
        ChangeVerifyJob.enqueue(
            instance=change, user=user, name=f"verify change {change.pk}"
        )
    except Exception as exc:  # noqa: BLE001
        return ValidationOutcome(
            False,
            f"The job could not be queued: {type(exc).__name__}: {str(exc)[:300]}",
        )
    _journal(change, user, "Verification queued.")
    return ValidationOutcome(True, "Verification queued.")


def validate_on_submission(change, user):
    """Queue a run when the change is staged, if a policy asks for one.

    Best effort: a change that cannot be validated yet (no workspace, no
    snapshot) is staged all the same, and the journal says why nothing ran.
    """
    from .actions import _journal
    from .validation import policies_for

    wanted = [p for p in policies_for(change) if p.run_on_submission]
    if not wanted:
        return None
    outcome = enqueue_validation(change, user, Trigger.SUBMISSION)
    if not outcome.ok:
        _journal(
            change, user, f"Validation on submission did not run: {outcome.message}"
        )
    return outcome
