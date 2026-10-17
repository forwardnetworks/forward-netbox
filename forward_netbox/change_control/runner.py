# Running a change's criteria against Forward and turning the answers into a
# verdict. This is the part that makes the verify gate real: before it existed
# the gate, the evidence table and the verdict maths were all in place and
# nothing wrote a single evidence row.
#
# The rules, in the order they matter:
# - A criterion is evaluated in the change's workspace, at a pinned snapshot, by
#   a pinned query version. Never "now", never "whatever the path says today".
# - Evidence keeps the result's SHAPE and count, never its rows.
# - Completeness and timing come before the criteria. A device that failed
#   collection is absent from the model rather than flagged, so a criterion
#   evaluated over an incomplete snapshot is a confident wrong answer; the gate
#   holds without writing any "after" evidence at all.
import time
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime

from django.db import transaction
from django.utils import timezone

from .actions import _journal
from .actions import _lock
from .actions import transition
from .choices import ForwardChangeStateChoices as State
from .choices import ForwardChangeVerdictChoices as Verdict
from .choices import ForwardCriterionExpectationChoices as Expect
from .choices import ForwardEvidencePhaseChoices as Phase
from .criteria import evaluate_expectation
from .criteria import result_shape
from .gates import collection_postdates_apply
from .gates import device_set_complete
from .gates import verify
from .workspace import ForwardWorkspaceApi
from .workspace import refresh_snapshots


@dataclass(frozen=True)
class RunOutcome:
    ok: bool
    message: str = ""

    def __bool__(self):
        return self.ok


@dataclass(frozen=True)
class VerifyOutcome:
    ok: bool
    verdict: str = ""
    reasons: tuple = field(default_factory=tuple)
    message: str = ""

    def __bool__(self):
        return self.ok


NO_WORKSPACE = (
    "This change has no Forward workspace. Checks run in the change's workspace, "
    "never against the whole network; create or adopt one first."
)


def _network(change):
    return change.workspace_network_id


def _api(change, api):
    if api is not None:
        return api, None
    try:
        return ForwardWorkspaceApi(change.source), None
    except Exception as exc:  # noqa: BLE001
        return None, f"The Forward source could not be used: {exc}"


def record_evidence(change, phase, *, api=None):
    """Run every bound criterion at the phase's pinned snapshot and record it.

    All-or-nothing: the queries run first and nothing is written unless every one
    answered, so a half-measured phase cannot read as a complete one. An unbound
    criterion is skipped and named; it then shows as not measured, which is not
    the same as passing.
    """
    snapshot_id = (
        change.before_snapshot_id if phase == Phase.BEFORE else change.after_snapshot_id
    )
    if not change.workspace_network_id:
        return RunOutcome(False, NO_WORKSPACE)
    if not snapshot_id:
        return RunOutcome(False, f"No {phase} snapshot is pinned on this change.")
    criteria = list(change.criteria.all())
    api, error = _api(change, api)
    if error:
        return RunOutcome(False, error)
    network = _network(change)
    results = []
    skipped = []
    try:
        for criterion in criteria:
            if not criterion.is_bound:
                skipped.append(criterion.name)
                continue
            if (
                criterion.expectation == Expect.NO_DIFF
                and phase == Phase.AFTER
                and not change.before_snapshot_id
            ):
                # Nothing earlier to compare with: that is "not measured", never a pass.
                skipped.append(criterion.name)
                continue
            started = time.monotonic()
            rows = api.run_query(
                criterion.query_id, criterion.commit_id, network, snapshot_id
            )
            baseline = None
            if criterion.expectation == Expect.NO_DIFF:
                if phase == Phase.AFTER:
                    baseline = api.run_query(
                        criterion.query_id,
                        criterion.commit_id,
                        network,
                        change.before_snapshot_id,
                    )
                else:
                    # At the baseline there is nothing earlier to differ from.
                    baseline = rows
            results.append(
                (
                    criterion,
                    rows,
                    evaluate_expectation(criterion.expectation, rows, baseline),
                    int((time.monotonic() - started) * 1000),
                )
            )
    except Exception as exc:  # noqa: BLE001 - reported to the operator verbatim
        return RunOutcome(False, f"Forward could not run the criteria: {exc}")

    from ..models import ForwardChangeEvidence

    with transaction.atomic():
        for criterion, rows, passed, duration_ms in results:
            # One row per criterion and phase: an older snapshot's answer must not
            # sit beside the pinned one and win by being read last.
            ForwardChangeEvidence.objects.filter(
                criterion=criterion, phase=phase
            ).exclude(snapshot_id=snapshot_id).delete()
            shape = result_shape(rows)
            ForwardChangeEvidence.objects.update_or_create(
                criterion=criterion,
                phase=phase,
                snapshot_id=snapshot_id,
                defaults={
                    "passed": passed,
                    "row_count": shape["row_count"],
                    "commit_id": criterion.commit_id,
                    "duration_ms": duration_ms,
                    "shape": shape,
                },
            )
    message = f"Recorded {len(results)} criterion result(s) at {phase}."
    if skipped:
        message += f" Not bound, so not measured: {', '.join(skipped)}."
    return RunOutcome(True, message)


def _parse_time(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else timezone.make_aware(value, timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else timezone.make_aware(parsed, timezone.utc)


def verify_change(change, user, *, api=None, workspace_api=None):
    """Collect the evidence for the after snapshot and decide PROCEED or HOLD.

    A HOLD is a valid result, returned with its reasons and journaled; the change
    goes back to APPLIED for a fix and a re-verify rather than closing.

    `api` answers queries; `workspace_api` refreshes the workspace's snapshots. With
    neither given, both are the real Forward API.
    """
    if change.state not in (State.APPLIED, State.COLLECTED):
        return VerifyOutcome(
            False,
            message=f"A change is verified after it is applied; this one is {change.state}.",
        )
    if not change.workspace_network_id:
        return VerifyOutcome(False, message=NO_WORKSPACE)
    real = api is None
    api, error = _api(change, api)
    if error:
        return VerifyOutcome(False, message=error)
    refresher = api if real else workspace_api
    if refresher is not None:
        refreshed = refresh_snapshots(change, api=refresher)
        if not refreshed.ok:
            return VerifyOutcome(False, message=refreshed.message)
        change.refresh_from_db()
    if change.state == State.APPLIED:
        moved = transition(change, State.COLLECTED, user)
        if not moved.ok:
            return VerifyOutcome(False, reasons=tuple(moved.blockers))
        change.refresh_from_db()

    pinned = change.after_snapshot_id
    network = _network(change)
    try:
        collected = api.device_collection(network, pinned)
    except Exception as exc:  # noqa: BLE001
        return VerifyOutcome(False, message=f"Forward could not be read: {exc}")
    times = {name: _parse_time(value) for name, value in collected.items()}
    times = {name: value for name, value in times.items() if value is not None}

    # Completeness and timing first, with no evidence written when they fail.
    reasons = []
    for gate in (
        device_set_complete(change, collected.keys()),
        collection_postdates_apply(change, times),
    ):
        if not gate:
            reasons.extend(gate.reasons)
    if not reasons:
        recorded = record_evidence(change, Phase.AFTER, api=api)
        if not recorded.ok:
            return VerifyOutcome(False, message=recorded.message)
        verdict, reasons = verify(
            change, observed_device_keys=collected.keys(), device_collection_times=times
        )
    else:
        verdict = Verdict.HOLD

    target = (
        State.VERIFIED_PROCEED if verdict == Verdict.PROCEED else State.VERIFIED_HOLD
    )
    # Verdict and state change together under one lock, and only if the snapshot
    # this verdict was computed on is still the pinned one.
    with transaction.atomic():
        locked = _lock(change)
        if locked is None or locked.after_snapshot_id != pinned:
            return VerifyOutcome(
                False,
                message="The after snapshot changed while verifying; verify again.",
            )
        if locked.state != State.COLLECTED:
            return VerifyOutcome(
                False,
                message=f"The change moved to {locked.state} while verifying.",
            )
        locked.verdict = verdict
        locked.save(update_fields=["verdict", "last_updated"])
        moved = transition(locked, target, user)
        if not moved.ok:
            transaction.set_rollback(True)
            return VerifyOutcome(False, verdict, tuple(moved.blockers))
        _journal(
            locked,
            user,
            f"Verified against snapshot {pinned}: {verdict}."
            + (" " + " ".join(reasons) if reasons else ""),
        )
    change.refresh_from_db()
    return VerifyOutcome(True, verdict, tuple(reasons))
