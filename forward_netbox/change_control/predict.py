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
import hashlib
import hmac
import logging
from dataclasses import dataclass
from types import SimpleNamespace

from django.conf import settings
from django.db import transaction
from django.utils import timezone

from .actions import _journal
from .choices import ForwardChangeStateChoices as State
from .workspace import _open_api
from .workspace import _refuse
from .workspace import workspace_device_names
from .workspace import WorkspaceOutcome

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PredictOutcome:
    """What a prediction attempt produced. Never an exception at the caller.

    `status` is one of:
      unavailable  - not live, or this licence does not include it
      unsupported  - live, but it cannot answer THIS question yet
      answered     - a real prediction
      pending      - Forward is processing the predicted snapshot
      failed       - Forward ended without a prediction

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
    # Asked, and Forward is still working / ended without an answer. Neither is
    # an answer, and neither may read as "the change was checked".
    PENDING = "pending"
    FAILED = "failed"

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
    capabilities = _known_capabilities(source)
    if capabilities is not None and capabilities["predict"]["available"] is False:
        who = (
            "the Forward account lacks the permission"
            if str(capabilities["predict"]["reason"]).startswith("rbac")
            else "Forward refused Predict for this organisation"
        )
        return PredictOutcome(
            status=PredictOutcome.UNAVAILABLE,
            reason=(
                f"{who} ({capabilities['predict']['reason']}). The change can still be "
                "approved and verified: the verdict is computed from the "
                "post-change snapshot, which does not depend on predict."
            ),
        )
    if capabilities is not None and capabilities["predict"]["available"]:
        return PredictOutcome(
            status=PredictOutcome.UNSUPPORTED,
            reason=(
                "Predict is available to this organisation, but a proposal cannot "
                "be authored from NetBox yet, so nothing was predicted. The "
                "verdict does not depend on it."
            ),
        )
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


def _known_capabilities(source):
    """What the last capability probe found for the source, if anyone asked."""
    from .capabilities import cached_capabilities

    try:
        return cached_capabilities(source)
    except Exception:  # noqa: BLE001 - a cache problem must not hide the panel
        return None


def predict_enabled(source) -> bool:
    """Whether this source opts in to asking Forward to predict.

    Off unless a deployment says otherwise, and read with `.get(..., False)`
    so an existing source with no such key is off without a data migration.
    Fails closed for the same reason every other capability here does: a
    missing setting must never read as permission.
    """
    parameters = getattr(source, "parameters", None) or {}
    return bool(parameters.get("enable_predict", False))


SCOPE_NOTE = (
    "The prediction covers the devices in this change's Forward workspace, on the "
    "workspace's baseline snapshot, and nothing else in the network. Predict is "
    "licensed separately by Forward and does not model every protocol. It is "
    "advisory: approval, closing and the verdict never read it."
)


def render_predict_panel(outcome: PredictOutcome, *, stale: bool = False) -> dict:
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
            "stale": stale,
            "scope": SCOPE_NOTE,
        }
    heading = {
        PredictOutcome.UNAVAILABLE: "Forward prediction not available",
        PredictOutcome.UNSUPPORTED: "Forward cannot predict this change yet",
        PredictOutcome.PENDING: "Forward prediction in progress",
        PredictOutcome.FAILED: "Forward prediction failed",
    }.get(outcome.status, "Forward prediction")
    return {
        "level": "info",
        "heading": heading,
        "message": outcome.reason,
        "verdict": "",
        "advisory": True,
        "stale": stale,
        "scope": (
            SCOPE_NOTE
            if outcome.status in (PredictOutcome.PENDING, PredictOutcome.FAILED)
            else ""
        ),
    }


# -- the proposal and the prediction ------------------------------------------------
#
# A prediction is asked for from the change page, never in the background: Forward
# returns the predicted snapshot's id at once and processes it for as long as it
# takes (up to an hour), so nothing here waits. The id is written the moment
# Forward returns it. A change set that Forward holds and NetBox has forgotten pins
# its predicted snapshots against Forward's retention for good, so every path that
# creates one either records it or deletes it.

# A proposal is edited while the change is unapproved.
PROPOSAL_STATES = frozenset(
    {State.DRAFT, State.SCOPED, State.BASELINED, State.STAGED, State.PREDICTED}
)
# A prediction needs the workspace baseline, so it cannot be asked before it.
PREDICTABLE_STATES = frozenset({State.BASELINED, State.STAGED, State.PREDICTED})
# Forward ends a prediction in one of these states when it cannot produce one.
FAILED_SNAPSHOT_STATES = frozenset(
    {"FAILED", "CANCELED", "TIMED_OUT", "RESTORE_FAILED"}
)
# Staging a PAN-OS device by CLI is accepted and fails only at prediction, with no
# reason; firewall changes go through a different Forward route. Refused up front.
FIREWALL_MARKERS = ("pan-os", "panos", "pan_os", "palo alto")
# Forward processes a predicted snapshot for up to an hour.
UNLISTED_FAILS_AFTER_SECONDS = 2 * 60 * 60
PREDICT_NOTE = "Predicted from NetBox change control"


def proposal_entries(change):
    """`{forward device name: commands}` for the change's CLI entries."""
    from ..models import ForwardChangeProposal

    return {
        row.target: row.body
        for row in change.proposals.filter(kind=ForwardChangeProposal.CLI)
        if (row.body or "").strip()
    }


def proposal_digest(change) -> str:
    """A keyed digest of the proposal, so an edit after a prediction is noticed.

    Keyed with the deployment's secret so the stored value, which sits on a
    change-logged row, cannot be used to confirm a guess at a short command (an
    SNMP community, say). It is stored, shown and compared, never reversed.
    """
    mac = hmac.new(
        settings.SECRET_KEY.encode("utf-8"), b"forward-netbox-proposal", hashlib.sha256
    )
    for target, body in sorted(proposal_entries(change).items()):
        mac.update(f"{target}\0{body}\0".encode("utf-8"))
    return mac.hexdigest()


def prediction_is_stale(change) -> bool:
    """The proposal changed after it was predicted."""
    return bool(change.predict_digest) and change.predict_digest != proposal_digest(
        change
    )


def save_proposal(change, user, bodies):
    """Store the commands for each named device; an empty text removes the entry.

    Only devices in the change's Forward workspace scope can have an entry: the
    prediction stages by Forward's own device name, so an entry for any other name
    could never be predicted and would sit there looking like it was.
    """
    from ..models import ForwardChange
    from ..models import ForwardChangeProposal

    with transaction.atomic():
        locked = ForwardChange.objects.select_for_update().get(pk=change.pk)
        if locked.state not in PROPOSAL_STATES:
            return _refuse(
                f"A proposal cannot be edited once the change is {locked.state}."
            )
        if not predict_enabled(locked.source) and any(
            (body or "").strip() for body in bodies.values()
        ):
            return _refuse(
                "Predict is off for this source, so a proposal cannot be added. "
                "Turn on asking Forward to predict change-control changes on the "
                "source first."
            )
        known = set(workspace_device_names(locked))
        # Clearing is always allowed, so an entry for a device that has left the
        # change (or been renamed) can still be removed; adding one is not.
        unknown = sorted(
            target
            for target, body in bodies.items()
            if target not in known and (body or "").strip()
        )
        if unknown:
            return _refuse(
                "These are not devices of this change with a Forward name: "
                + ", ".join(unknown)
                + "."
            )
        saved = removed = 0
        for target, body in bodies.items():
            body = (body or "").replace("\r\n", "\n").strip("\n")
            if not body.strip():
                removed += locked.proposals.filter(
                    kind=ForwardChangeProposal.CLI, target=target
                ).delete()[0]
                continue
            ForwardChangeProposal.objects.update_or_create(
                change=locked,
                kind=ForwardChangeProposal.CLI,
                target=target,
                defaults={"body": body},
            )
            saved += 1
        # The journal says how many, never what.
        _journal(
            locked,
            user,
            f"Proposal updated: {saved} device(s) with commands, {removed} cleared.",
        )
    return WorkspaceOutcome(True, f"{saved} saved, {removed} cleared")


def classify_refusal(exc):
    """Why Forward said no, as a sentence, or None if this was not a refusal.

    The kind comes from the SDK's reading of Forward's wording and is a hint, not
    a fact: a 403 with no kind is reported as refused without claiming a cause. No
    licence table is consulted, and `enable_predict` or the org property is not a
    gate, because the SDK saw a licence refusal with the property on.
    """
    cause = getattr(exc, "__cause__", None) or exc
    denial = getattr(cause, "denial", None)
    kind = getattr(denial, "kind", "") if denial is not None else ""
    if kind in ("license", "license_expired"):
        return (
            "Forward's licence does not include Predict for this organization "
            f"({kind})."
        )
    if kind == "rbac":
        return (
            "This Forward account does not have permission to manage change sets "
            "on the workspace."
        )
    if kind == "org_setting":
        return "Predict is not enabled for this Forward organization."
    status = getattr(cause, "status", None) or getattr(exc, "status", None)
    if status == 403:
        return "Forward refused the request (HTTP 403) without saying why."
    return None


def _failure_reason(exc):
    """What to record for a failure that is not a refusal.

    The exception's class and status only: Forward's error text can echo what was
    sent, and what was sent here is commands.
    """
    cause = getattr(exc, "__cause__", None) or exc
    status = getattr(cause, "status", None) or getattr(exc, "status", None)
    detail = f" (HTTP {status})" if status else ""
    return f"Forward could not run the prediction: {type(cause).__name__}{detail}."


# A claim with no predicted snapshot behind it means a request died between
# claiming and asking Forward; after this long it no longer blocks a new one.
CLAIM_EXPIRES_AFTER_SECONDS = 15 * 60


def _claim_is_stale(change):
    if change.predict_snapshot_id or change.predict_started_at is None:
        return False
    age = (timezone.now() - change.predict_started_at).total_seconds()
    return age > CLAIM_EXPIRES_AFTER_SECONDS


def _firewall_devices(change):
    rows = change.devices.select_related("device__platform", "device__device_type")
    names = []
    for row in rows:
        device = row.device
        text = " ".join(
            filter(
                None,
                [
                    getattr(device.platform, "name", ""),
                    (
                        getattr(device.device_type.manufacturer, "name", "")
                        if device.device_type
                        else ""
                    ),
                    getattr(device.device_type, "model", ""),
                ],
            )
        ).lower()
        if any(marker in text for marker in FIREWALL_MARKERS):
            names.append((row.forward_device_key or str(device)).strip())
    return sorted(names)


def _record(change, **fields):
    for name, value in fields.items():
        setattr(change, name, value)
    change.save(update_fields=[*fields, "last_updated"])


def _is_gone(exc) -> bool:
    """Forward says the change set (or its network) does not exist any more."""
    cause = getattr(exc, "__cause__", None) or exc
    return (getattr(cause, "status", None) or getattr(exc, "status", None)) == 404


def delete_remote_change_set(source, network_id, change_set_id, *, api=None) -> bool:
    """Delete a change set in Forward; True when it is gone (including already gone).

    Takes plain values rather than a change so it can run after the change row
    has been deleted. Best-effort: False means Forward could not be asked or
    refused, and the caller keeps the id so a later attempt can finish the job.
    """
    if not change_set_id:
        return True
    if api is None:
        api, error = _open_api(SimpleNamespace(source=source), None)
        if error:
            return False
    try:
        api.delete_change_set(network_id, change_set_id)
    except Exception as exc:  # noqa: BLE001 - cleanup must not block anything
        if _is_gone(exc):
            return True
        logger.warning(
            "Could not delete Forward change set %s in network %s: %s",
            change_set_id,
            network_id,
            type(getattr(exc, "__cause__", None) or exc).__name__,
        )
        return False
    return True


def discard_prediction(change, *, api=None) -> bool:
    """Delete the change's Forward change set (and its predicted snapshots).

    Never blocks a transition: it returns whether the change set is gone. The id
    is cleared only once Forward confirms (or says it never existed), so a failure
    leaves it recorded and a later attempt can finish the job.
    """
    if not change.predict_change_set_id:
        return True
    if not delete_remote_change_set(
        change.source,
        change.workspace_network_id,
        change.predict_change_set_id,
        api=api,
    ):
        return False
    # The panel must not keep describing a prediction that no longer exists.
    _record(
        change,
        predict_change_set_id="",
        predict_snapshot_id="",
        predict_digest="",
        predict_status="",
        predict_reason="",
        predict_pre_verdict="",
    )
    return True


def start_prediction(change, user, *, confirmed=False, api=None):
    """Ask Forward to predict the change's proposal on the workspace baseline.

    A Forward write (a change set and a predicted snapshot in the workspace
    network), so it needs the operator's confirmation every time. It does not
    wait: the predicted snapshot is picked up by `refresh_prediction`.
    """
    from ..models import ForwardChange

    if not confirmed:
        return _refuse(
            "Confirm that you understand what asking Forward to predict does "
            "before starting one."
        )
    if not predict_enabled(change.source):
        return _refuse(
            "Predict is off for this source. Turn on asking Forward to predict "
            "change-control changes on the source first."
        )
    with transaction.atomic():
        locked = ForwardChange.objects.select_for_update().get(pk=change.pk)
        if locked.state not in PREDICTABLE_STATES:
            return _refuse(
                "A prediction is asked for once the change has its baseline and "
                f"before it is approved; this change is {locked.state}."
            )
        if not locked.workspace_network_id or not locked.before_snapshot_id:
            return _refuse(
                "This change has no workspace baseline to predict against. Create "
                "the workspace and refresh its snapshots first."
            )
        entries = proposal_entries(locked)
        if not entries:
            return _refuse("The proposal has no commands for any device.")
        outside = sorted(set(entries) - set(workspace_device_names(locked)))
        if outside:
            return _refuse(
                "The proposal names devices that are no longer in the change: "
                + ", ".join(outside)
                + "."
            )
        firewalls = sorted(set(_firewall_devices(locked)) & set(entries))
        if firewalls:
            return _refuse(
                "Forward cannot predict firewall changes staged as CLI text: "
                + ", ".join(firewalls)
                + "."
            )
        if locked.predict_status == PredictOutcome.PENDING and not _claim_is_stale(
            locked
        ):
            return _refuse(
                "A prediction is already in progress. Refresh it, or wait for it "
                "to finish."
            )
        digest = proposal_digest(locked)
        # Claimed under the lock, so a second request cannot start another while
        # this one is still talking to Forward.
        previous = (
            locked.predict_status,
            locked.predict_reason,
            locked.predict_started_at,
        )
        claim_at = timezone.now()
        _record(
            locked,
            predict_status=PredictOutcome.PENDING,
            predict_reason="Starting the prediction in Forward.",
            predict_started_at=claim_at,
        )

    def release_claim(message):
        # A start refused before Forward was asked leaves the page as it was.
        _record(
            change,
            predict_status=previous[0],
            predict_reason=previous[1],
            predict_started_at=previous[2],
        )
        return _refuse(message)

    def still_mine():
        # The claim can lapse (a slow start past its expiry) or the change can
        # reach a terminal state while Forward is being asked.
        current = ForwardChange.objects.filter(pk=change.pk).first()
        return (
            current is not None
            and current.predict_started_at == claim_at
            and current.state not in State.TERMINAL
        )

    api, error = _open_api(change, api)
    if error:
        return release_claim(error)
    change.refresh_from_db()
    # The baseline must still exist in the workspace: a workspace past its
    # retention, or one that was swapped, has no snapshot to predict from.
    try:
        rows = api.snapshots(change.workspace_network_id)
        listed = {row["id"] for row in rows if str(row["state"]).upper() == "PROCESSED"}
    except Exception as exc:  # noqa: BLE001
        return release_claim(classify_refusal(exc) or _failure_reason(exc))
    if change.before_snapshot_id not in listed:
        return release_claim(
            "The workspace does not list this change's baseline snapshot as "
            "processed (a new workspace takes about half a minute). Refresh the "
            "workspace snapshots and try again."
        )
    # A superseded prediction is deleted first; if Forward will not delete it,
    # stop rather than orphan it by overwriting its id.
    if change.predict_change_set_id and not discard_prediction(change, api=api):
        return release_claim(
            "The earlier prediction could not be removed from Forward, so a new "
            "one was not started. Try again."
        )
    network_id = change.workspace_network_id
    set_id = ""
    try:
        set_id = api.create_change_set(
            network_id,
            name=f"{(change.ref or '').strip() or 'change'}-{change.pk}"[:100],
            snapshot_id=change.before_snapshot_id,
        )
        # Recorded before anything else is asked of it, and only while this run
        # still owns the claim: a lapsed run must not overwrite a newer run's id.
        if not ForwardChange.objects.filter(
            pk=change.pk, predict_started_at=claim_at
        ).update(predict_change_set_id=set_id):
            delete_remote_change_set(change.source, network_id, set_id, api=api)
            return _refuse("Another prediction was started in the meantime.")
        change.predict_change_set_id = set_id
        for target, body in sorted(entries.items()):
            errors = api.validate_commands(network_id, set_id, target, body)
            if errors:
                discard_prediction(change, api=api)
                reason = (
                    f"Forward found {errors} error(s) in the commands for {target}."
                )
                _record(
                    change,
                    predict_status=PredictOutcome.FAILED,
                    predict_reason=reason,
                    predict_snapshot_id="",
                )
                return _refuse(reason)
            api.set_commands(network_id, set_id, target, body)
        api.commit(network_id, set_id, PREDICT_NOTE)
        snapshot_id = api.predict(network_id, set_id, PREDICT_NOTE)
        if not snapshot_id:
            raise ValueError("Forward returned no predicted snapshot id")
    except Exception as exc:  # noqa: BLE001 - classified, recorded, never raised
        refusal = classify_refusal(exc)
        if not still_mine():
            # Another start owns the page now; clean up only what this run made.
            if set_id:
                delete_remote_change_set(change.source, network_id, set_id, api=api)
            return _refuse(refusal or _failure_reason(exc))
        if set_id:
            discard_prediction(change, api=api)
        status = PredictOutcome.UNAVAILABLE if refusal else PredictOutcome.FAILED
        reason = refusal or _failure_reason(exc)
        _record(
            change,
            predict_status=status,
            predict_reason=reason,
            predict_snapshot_id="",
            predict_digest="",
        )
        return _refuse(reason)
    if not still_mine():
        delete_remote_change_set(change.source, network_id, set_id, api=api)
        return _refuse(
            "The change was closed or another prediction was started while this "
            "one was being asked for, so this one was removed from Forward."
        )
    _record(
        change,
        predict_snapshot_id=snapshot_id,
        predict_status=PredictOutcome.PENDING,
        predict_reason="Forward is processing the predicted snapshot.",
        predict_pre_verdict="",
        predict_digest=digest,
        predict_started_at=timezone.now(),
    )
    _journal(
        change,
        user,
        f"Asked Forward to predict the proposal for {len(entries)} device(s).",
    )
    return WorkspaceOutcome(True, f"Prediction started for {len(entries)} device(s).")


def refresh_prediction(change, user=None, *, api=None):
    """Read the predicted snapshot's state and record what Forward says.

    Processed is an answer; a failed state is a failure with the state named
    (Forward gives no reason); anything else is still pending. A wait that ran
    out is never an answer.
    """
    if change.predict_status != PredictOutcome.PENDING:
        return _refuse("No prediction is in progress for this change.")
    if not change.predict_snapshot_id:
        # A start that died after claiming and before Forward answered. Recent:
        # it may still be running. Expired: it is over, so say so and let the
        # operator ask again.
        if not _claim_is_stale(change):
            return WorkspaceOutcome(True, "The prediction is still being started.")
        _record(
            change,
            predict_status=PredictOutcome.FAILED,
            predict_reason=(
                "The request to start the prediction did not finish. Ask again."
            ),
        )
        return _refuse(change.predict_reason)
    api, error = _open_api(change, api)
    if error:
        return _refuse(error)
    try:
        snapshots = api.snapshots(change.workspace_network_id)
    except Exception as exc:  # noqa: BLE001
        return _refuse(classify_refusal(exc) or _failure_reason(exc))
    found = next((s for s in snapshots if s["id"] == change.predict_snapshot_id), None)
    if found is None:
        # The workspace's snapshot list showed the predicted snapshot in a live
        # check, but an unlisted one is still pending, not failed, until it has
        # been missing for a long time.
        started = change.predict_started_at
        waited = (timezone.now() - started).total_seconds() if started else 0
        if waited < UNLISTED_FAILS_AFTER_SECONDS:
            return WorkspaceOutcome(
                True, "Forward does not list the predicted snapshot yet."
            )
        reason = "Forward still does not list the predicted snapshot."
        _record(change, predict_status=PredictOutcome.FAILED, predict_reason=reason)
        return _refuse(reason)
    state = str(found["state"]).upper()
    if state == "PROCESSED":
        _record(
            change,
            predict_status=PredictOutcome.ANSWERED,
            predict_reason=(
                "Forward predicted this change; the predicted snapshot is "
                f"{change.predict_snapshot_id} in the workspace network."
            ),
        )
        return WorkspaceOutcome(True, "Forward predicted the change.")
    if state in FAILED_SNAPSHOT_STATES:
        reason = (
            f"Forward's prediction ended in state {state}. Forward gives no "
            "further reason."
        )
        _record(change, predict_status=PredictOutcome.FAILED, predict_reason=reason)
        return _refuse(reason)
    return WorkspaceOutcome(True, f"Still processing (state {state or 'unknown'}).")
