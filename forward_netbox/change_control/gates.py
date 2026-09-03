# The two gates that need Forward: collect and verify.
#
# The single most important thing in this file is device-set completeness, and
# it is step one rather than a report at the end. A device that failed
# collection is ABSENT from the Forward model rather than flagged in it, so
# "no violations found" and "the broken device was not in the snapshot" are
# indistinguishable from the rows alone. That is the same shape as a customer's
# 552 "uncovered" devices: absence reads as health unless something asks.
from dataclasses import dataclass
from dataclasses import field

from .choices import ForwardChangeVerdictChoices
from .evidence import compute_verdict
from .evidence import criterion_outcomes
from .evidence import FIX
from .evidence import REGRESSION


@dataclass(frozen=True)
class GateResult:
    passed: bool
    reasons: tuple[str, ...] = ()
    context: dict = field(default_factory=dict)

    def __bool__(self):
        return self.passed


def device_set_complete(change, observed_device_keys) -> GateResult:
    """Every in-scope device must appear in the snapshot being verified.

    Refuses rather than warns. A verdict computed over a snapshot that is
    missing some of the devices the change touched is not a weaker verdict, it
    is a different question answered confidently.
    """
    scoped = {
        d.forward_device_key: d for d in change.devices.all() if d.forward_device_key
    }
    observed = {str(k) for k in (observed_device_keys or ())}
    missing = sorted(set(scoped) - observed)
    if not missing:
        return GateResult(True, context={"devices": len(scoped)})

    names = ", ".join(str(scoped[k].device) for k in missing[:5])
    return GateResult(
        False,
        (
            f"{len(missing)} of {len(scoped)} in-scope device(s) are absent "
            f"from this snapshot, so nothing can be concluded about them: "
            f"{names}. A device that failed collection is missing from the "
            f"model rather than marked failed, which is why this refuses "
            f"instead of reporting a pass over a smaller set.",
        ),
        {"missing": missing, "scoped": len(scoped)},
    )


def collection_postdates_apply(change, device_collection_times) -> GateResult:
    """The snapshot must have seen the network AFTER the change was applied.

    The weakest joint in the workflow, and it is named as such rather than
    presented as a computed fact: `applied_at` is a human attestation, and
    per-device collection times are only as precise as Forward's collectors
    report. When a device has no collection time this HOLDS rather than
    assuming the snapshot is new enough.
    """
    if not change.applied_at:
        return GateResult(False, ("No apply time was attested.",))

    scoped = {
        d.forward_device_key: d for d in change.devices.all() if d.forward_device_key
    }
    stale, unknown = [], []
    for key, scoped_device in scoped.items():
        collected = (device_collection_times or {}).get(key)
        if collected is None:
            unknown.append(str(scoped_device.device))
        elif collected < change.applied_at:
            stale.append(str(scoped_device.device))

    reasons = []
    if stale:
        reasons.append(
            f"{len(stale)} device(s) were last collected before the change was "
            f"applied, so this snapshot cannot show its effect: "
            f"{', '.join(sorted(stale)[:5])}."
        )
    if unknown:
        reasons.append(
            f"{len(unknown)} device(s) report no collection time, so whether "
            f"this snapshot postdates the change is unknown for them: "
            f"{', '.join(sorted(unknown)[:5])}. Unknown is held, not assumed."
        )
    return GateResult(not reasons, tuple(reasons))


def attestation_looks_premature(change, outcomes) -> GateResult:
    """Forward sees no difference at all between the two snapshots.

    `APPLIED` is a human claim and cannot be proved from here. But its most
    common failure - verifying before the change actually landed, or attesting
    a change that silently did nothing - IS detectable: if every criterion
    reports an identical result before and after, the network Forward observed
    did not move.

    That is not proof the change was never applied. A change can legitimately
    leave every criterion where it was. So this is a HOLD with a question
    rather than an accusation, and it names the alternative explanation.
    """
    measured = [o for o in outcomes if o.flip]
    if not measured:
        return GateResult(True)

    moved = [o for o in measured if o.flip in (FIX, REGRESSION)]
    if moved:
        return GateResult(True, context={"moved": len(moved)})

    return GateResult(
        False,
        (
            "Every criterion returned the same result before and after, so "
            "Forward observed no change to the network. Either the change has "
            "not landed yet and this was verified too early, or it landed and "
            "changed nothing the criteria ask about. The apply time is "
            "attested rather than measured, so this cannot be settled from "
            "here - check the snapshot is newer than the work.",
        ),
        {"measured": len(measured)},
    )


def verify(change, *, observed_device_keys=None, device_collection_times=None):
    """The verify gate, in order. Returns (verdict, reasons).

    Order matters: completeness and timing come before the criteria, because a
    criterion evaluated over an incomplete or stale snapshot produces a
    confident wrong answer rather than an obviously missing one.
    """
    reasons: list[str] = []

    completeness = device_set_complete(change, observed_device_keys)
    if not completeness:
        reasons.extend(completeness.reasons)

    timing = collection_postdates_apply(change, device_collection_times)
    if not timing:
        reasons.extend(timing.reasons)

    if reasons:
        # Short-circuit deliberately: the criteria are not evaluated at all,
        # so no evidence row is written that would later read as a real
        # measurement of this snapshot.
        return ForwardChangeVerdictChoices.HOLD, reasons

    verdict, criteria_reasons = compute_verdict(change)

    # Asked after the criteria, not before: it needs their outcomes, and a
    # change that already has a real reason to hold does not need this one too.
    if not criteria_reasons:
        premature = attestation_looks_premature(change, criterion_outcomes(change))
        if not premature:
            return ForwardChangeVerdictChoices.HOLD, list(premature.reasons)

    return verdict, criteria_reasons
