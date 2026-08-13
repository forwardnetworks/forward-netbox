# Shared device-scope reconciliation + orphan prune logic.
#
# Used by both the forward_device_scope_reconciliation_audit management command
# and the sync-detail UI panel so the CLI and UI always agree.
import heapq
import re
from datetime import datetime
from datetime import timedelta
from datetime import timezone as dt_timezone

from dcim.models import Device
from django.db import transaction
from django.db.models.deletion import ProtectedError
from django.utils import timezone
from rq.timeouts import JobTimeoutException

from .bulk_delete import lock_related_writes_for_delete
from .forward_api import build_device_tag_scope_where
from .forward_api import build_endpoint_device_eligibility_where
from .forward_api import build_endpoint_tag_scope_where
from .post_sync import current_post_sync_snapshot
from .sync_facade import device_tag_scope
from .sync_facade import effective_scope_endpoints_by_include_tags

SAMPLE_LIMIT = 25

# Forward renders a failed collection result as
# ``DeviceSnapshotResult.collectionFailed(DeviceCollectionError.AUTHENTICATION_FAILED)``.
# Pull the specific DeviceCollectionError token so operators can see *why* a
# device is backfilled (auth vs timeout vs incomplete setup) without a manual
# Forward API probe.
_COLLECTION_ERROR_RE = re.compile(r"DeviceCollectionError\.([A-Za-z0-9_]+)")


def _collection_failure_reason(reason_str):
    """Map a stringified ``device.snapshotInfo.result`` to a short reason token.

    ``DeviceSnapshotResult.collectionFailed(DeviceCollectionError.X)`` -> ``X``;
    ``DeviceSnapshotResult.completed`` -> ``completed``; anything unparseable
    (including a missing reason on older payloads) -> ``unknown``.
    """
    if not reason_str:
        return "unknown"
    match = _COLLECTION_ERROR_RE.search(str(reason_str))
    if match:
        return match.group(1)
    token = str(reason_str).rsplit(".", 1)[-1].strip()
    return token or "unknown"


def _stale_days(ts_str):
    """Whole days between an ISO collection/backfill timestamp and now.

    Returns ``None`` when the timestamp is missing or unparseable so callers can
    render a placeholder instead of a misleading ``0``.
    """
    if not ts_str:
        return None
    try:
        parsed = datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt_timezone.utc)
    return max(0, (timezone.now() - parsed).days)


# NetBox tag applied to devices that are tagged-in-scope but were backfilled
# (not freshly collected) in the latest Forward snapshot, so operators can find
# them with a normal device-list filter (?tag=forward-backfilled).
BACKFILLED_TAG_SLUG = "forward-backfilled"
BACKFILLED_TAG_NAME = "Forward Backfilled"
BACKFILLED_TAG_COLOR = "ffc107"
BACKFILLED_TAG_DESCRIPTION = (
    "Tagged in scope but backfilled (not freshly collected) in the latest "
    "Forward snapshot. Maintained by the Forward sync scope reconciliation."
)

# NetBox tag applied to devices that match NONE of the sync's included Forward
# tags (out of scope). Unlike backfilled devices (in scope, kept), these are the
# removable orphans — review and delete them via Scope Reconciliation -> Prune
# orphans. Maintained alongside the backfilled tag so operators can filter
# /dcim/devices/?tag=forward-out-of-scope.
OUT_OF_SCOPE_TAG_SLUG = "forward-out-of-scope"
OUT_OF_SCOPE_TAG_NAME = "Forward Out Of Scope"
OUT_OF_SCOPE_TAG_COLOR = "f44336"
OUT_OF_SCOPE_TAG_DESCRIPTION = (
    "Matches none of the sync's included Forward device tags (out of scope). "
    "Removable via Scope Reconciliation -> Prune orphans. Maintained by the "
    "Forward sync scope reconciliation."
)


# How long an absence must persist before the prune is allowed to believe it.
#
# A device disabled in Forward is absent from `network.devices` and from the REST
# inventory alike, so the plugin cannot tell "disabled for a maintenance window"
# from "decommissioned". The deletion is permanent; the disabling usually is not.
# Both thresholds must be met, because either alone is defeated by a plausible
# sync schedule: three runs is three hours on an hourly sync, and 72 hours is one
# confirmation on a weekly one. Requiring both means at least three confirmations
# AND at least three days, whatever the schedule.
DEFAULT_PRUNE_ABSENCE_RUNS = 3
DEFAULT_PRUNE_ABSENCE_HOURS = 72


def absence_quarantine_thresholds(sync) -> tuple:
    """Resolve (runs, hours) for this sync, falling back to the defaults.

    Source parameters, not plugin settings, so an operator changes them in the
    same form as the prune toggle itself rather than on a shell.
    """
    parameters = getattr(getattr(sync, "source", None), "parameters", None) or {}

    def _positive_int(key, default):
        raw = parameters.get(key)
        if raw is None or raw == "":
            return default
        try:
            value = int(raw)
        except (TypeError, ValueError):
            return default
        # A negative or non-numeric override must not silently disable the
        # quarantine; zero is a deliberate "no delay" and is honoured.
        return value if value >= 0 else default

    return (
        _positive_int("device_tag_prune_absence_runs", DEFAULT_PRUNE_ABSENCE_RUNS),
        _positive_int("device_tag_prune_absence_hours", DEFAULT_PRUNE_ABSENCE_HOURS),
    )


def record_device_absence(sync, out_of_scope_pks, *, snapshot_id="") -> dict:
    """Advance the absence streak for out-of-scope devices, clear it for the rest.

    Called once per promoted sync from ``tag_backfilled_devices`` - the
    post-sync "reconcile device scope tags" job - which already holds the
    out-of-scope set, so this costs no Forward call.

    That job maintains the backfilled and out-of-scope tags for every sync,
    whether or not scope tags are applied, so every sync accumulates streaks.
    When it fails, streaks stand still and the prune deletes less, which is the
    direction a failure should push.

    A device that is no longer out of scope has its row DELETED rather than
    decremented. The streak only means anything as an unbroken run: two absences
    either side of a presence are two separate absences, and the second one has
    to earn the operator's trust from the start.

    A run that fails before this point never advances anything, which is the
    right direction - an absence we could not confirm is not evidence.
    """
    from ..models import ForwardDeviceAbsence

    now = timezone.now()
    absent_ids = set(out_of_scope_pks or ())
    returned = ForwardDeviceAbsence.objects.filter(sync=sync).exclude(
        device_id__in=absent_ids
    )
    cleared = returned.count()
    returned.delete()
    if not absent_ids:
        return {"absent": 0, "started": 0, "advanced": 0, "cleared": cleared}

    existing = {
        row.device_id: row
        for row in ForwardDeviceAbsence.objects.filter(
            sync=sync,
            device_id__in=absent_ids,
        )
    }
    started = 0
    for device_id in sorted(absent_ids):
        row = existing.get(device_id)
        if row is None:
            ForwardDeviceAbsence.objects.create(
                sync=sync,
                device_id=device_id,
                consecutive_absent_runs=1,
                first_absent_at=now,
                last_absent_at=now,
                last_absent_snapshot_id=snapshot_id or "",
            )
            started += 1
            continue
        row.consecutive_absent_runs += 1
        row.last_absent_at = now
        row.last_absent_snapshot_id = snapshot_id or ""
        row.save(
            update_fields=[
                "consecutive_absent_runs",
                "last_absent_at",
                "last_absent_snapshot_id",
            ]
        )
    return {
        "absent": len(absent_ids),
        "started": started,
        "advanced": len(absent_ids) - started,
        "cleared": cleared,
    }


def partition_quarantined_orphans(sync, orphan_pks) -> dict:
    """Split orphans into those past the quarantine and those still inside it.

    Fails closed: an orphan with no absence row at all is held, not released. A
    device we have never recorded as absent has, by this table's reckoning, been
    absent for zero confirmed runs.
    """
    from ..models import ForwardDeviceAbsence

    candidate_ids = list(orphan_pks or ())
    required_runs, required_hours = absence_quarantine_thresholds(sync)
    if not candidate_ids:
        return {
            "eligible_pks": [],
            "held_pks": [],
            "required_runs": required_runs,
            "required_hours": required_hours,
        }
    if not required_runs and not required_hours:
        # Both thresholds zeroed is an operator saying "no quarantine", and it
        # has to mean that. Falling through would hold everything forever
        # instead, because the fail-closed branch below holds any orphan with no
        # absence row - which, with no quarantine ever recorded, is all of them.
        return {
            "eligible_pks": list(candidate_ids),
            "held_pks": [],
            "required_runs": required_runs,
            "required_hours": required_hours,
        }
    cutoff = timezone.now() - timedelta(hours=required_hours)
    rows = {
        device_id: (runs, first_absent_at)
        for device_id, runs, first_absent_at in ForwardDeviceAbsence.objects.filter(
            sync=sync,
            device_id__in=candidate_ids,
        ).values_list("device_id", "consecutive_absent_runs", "first_absent_at")
    }
    eligible = []
    held = []
    for device_id in candidate_ids:
        row = rows.get(device_id)
        if row is None:
            held.append(device_id)
            continue
        runs, first_absent_at = row
        if runs >= required_runs and first_absent_at <= cutoff:
            eligible.append(device_id)
        else:
            held.append(device_id)
    return {
        "eligible_pks": eligible,
        "held_pks": held,
        "required_runs": required_runs,
        "required_hours": required_hours,
    }


def _quarantine_summary(sync, out_of_scope_pks) -> dict:
    """Report-shaped view of the quarantine, for the panel and the audit command."""
    partition = partition_quarantined_orphans(sync, out_of_scope_pks)
    return {
        "required_runs": partition["required_runs"],
        "required_hours": partition["required_hours"],
        "prune_eligible": len(partition["eligible_pks"]),
        "held": len(partition["held_pks"]),
    }


def compute_scope_reconciliation(sync, *, snapshot_id=None) -> dict:
    """Compare NetBox devices against the sync's Forward device tag scope.

    Returns counts plus the resolved sets (so callers can prune). Raises the
    underlying client/query exception on failure.
    """
    network_id = sync.get_network_id()
    if not network_id:
        raise ValueError("Sync source has no network configured.")

    include_tags, exclude_tags, include_match = device_tag_scope(sync)
    scope_where = build_device_tag_scope_where(
        include_tags, exclude_tags, include_match
    )

    client = sync.source.get_client()
    snapshot_id = str(snapshot_id or "").strip() or sync.resolve_snapshot_id(client)
    query = "\n".join(
        [
            "foreach device in network.devices",
            "where device.platform.vendor != Vendor.FORWARD_CUSTOM",
            *scope_where,
            "select {",
            "  name: device.name,",
            "  completed: device.snapshotInfo.result "
            "== DeviceSnapshotResult.completed,",
            "  reason: toString(device.snapshotInfo.result),",
            "  collectionTime: device.snapshotInfo.collectionTime,",
            "  backfillTime: device.snapshotInfo.backfillTime,",
            "  tagNames: device.tagNames,",
            '  location: if isPresent(device.locationName) then toLowerCase(device.locationName) else ""',
            "}",
        ]
    )
    rows = client.run_nqe_query(
        query=query,
        network_id=network_id,
        snapshot_id=snapshot_id,
        fetch_all=True,
    )
    endpoint_names, endpoint_matched_tags = _endpoint_scope_names(
        sync,
        client=client,
        network_id=network_id,
        snapshot_id=snapshot_id,
        include_tags=include_tags,
        exclude_tags=exclude_tags,
        include_match=include_match,
    )

    from django.utils.text import slugify as _slugify

    row_by_name = {}
    matched_include_tags_by_name = {}
    include_tag_set = set(include_tags)
    device_tagged_names = set()
    device_completed_names = set()
    forward_site_slugs = set()
    for row in rows:
        name = str(row.get("name") or "").strip()
        if not name:
            continue
        device_tagged_names.add(name)
        row_by_name[name] = row
        matched_tags = sorted(
            include_tag_set.intersection(
                str(tag) for tag in (row.get("tagNames") or [])
            )
        )
        if matched_tags:
            matched_include_tags_by_name[name] = matched_tags
        if row.get("completed"):
            device_completed_names.add(name)
        loc = str(row.get("location") or "").strip()
        if loc:
            sl = _slugify(loc)
            if sl:
                forward_site_slugs.add(sl)
    backfilled_names = device_tagged_names - device_completed_names
    tagged_names = device_tagged_names | endpoint_names
    completed_names = device_completed_names | endpoint_names
    matched_include_tags_by_name.update(endpoint_matched_tags)

    netbox_names = {
        name
        for name in Device.objects.values_list("name", flat=True)
        if (name or "").strip()
    }

    from ..models import ForwardDeviceTagClaim

    previously_managed = list(
        ForwardDeviceTagClaim.objects.filter(sync=sync, claim_type="scope")
        .select_related("device")
        .values_list("device_id", "device__name")
    )
    previously_managed_names = {name for _, name in previously_managed}
    # A sync may classify only devices it previously claimed. Treating every
    # NetBox device absent from this sync as out of scope creates contradictory
    # negative claims in multi-source deployments.
    out_of_scope = (previously_managed_names & netbox_names) - tagged_names
    present_backfilled = netbox_names & backfilled_names
    missing_in_netbox = completed_names - netbox_names
    missing_scope_tag_targets = set(matched_include_tags_by_name) - netbox_names
    present_scope_tags_by_name = {
        name: tag_names
        for name, tag_names in matched_include_tags_by_name.items()
        if name in netbox_names
    }

    out_of_scope_pks = [
        device_id for device_id, name in previously_managed if name in out_of_scope
    ]

    unmanaged = _unmanaged_device_summary(sync, tagged_names)

    absence = _classify_out_of_scope_absence(
        out_of_scope,
        client=client,
        network_id=network_id,
        snapshot_id=snapshot_id,
    )

    # Why are the in-scope devices backfilled? Group by the Forward collection
    # error so operators can act (rotate creds for AUTHENTICATION_FAILED, check
    # reachability for CONNECTION_TIMEOUT, finish onboarding for INCOMPLETE_SETUP)
    # without running a manual probe.
    reason_breakdown = {}
    for name in backfilled_names:
        reason = _collection_failure_reason((row_by_name.get(name) or {}).get("reason"))
        reason_breakdown[reason] = reason_breakdown.get(reason, 0) + 1
    reason_breakdown = dict(
        sorted(reason_breakdown.items(), key=lambda kv: (-kv[1], kv[0]))
    )

    present_backfilled_detail = []
    for name in sorted(present_backfilled)[:SAMPLE_LIMIT]:
        row = row_by_name.get(name) or {}
        present_backfilled_detail.append(
            {
                "name": name,
                "reason": _collection_failure_reason(row.get("reason")),
                "stale_days": _stale_days(
                    row.get("backfillTime") or row.get("collectionTime")
                ),
            }
        )

    # Compute empty orphan sites for the preview (current DB state; prune re-queries
    # after device deletion so sites that become empty then are also removed).
    from dcim.models import Site

    if forward_site_slugs:
        occupied_site_ids = _occupied_site_ids()
        empty_orphan_sites = list(
            Site.objects.exclude(slug__in=forward_site_slugs)
            .exclude(pk__in=occupied_site_ids)
            .values_list("name", flat=True)
            .order_by("name")
        )
    else:
        empty_orphan_sites = []

    return {
        "sync_id": sync.pk,
        "sync_name": sync.name,
        "snapshot_selector": sync.get_snapshot_id(),
        "include_tags": sorted(include_tags),
        "exclude_tags": sorted(exclude_tags),
        "include_match": include_match,
        "netbox_device_count": len(netbox_names),
        "forward_in_scope_completed": len(device_completed_names),
        "forward_in_scope_endpoints": len(endpoint_names),
        "forward_tagged_backfilled": len(backfilled_names),
        "netbox_present_backfilled": len(present_backfilled),
        "netbox_out_of_scope": len(out_of_scope),
        # The denominator the prune guard measures a shrink against: how many
        # devices this sync had claimed before this result came back.
        "forward_previously_managed": len(previously_managed_names),
        "netbox_empty_orphan_site_count": len(empty_orphan_sites),
        "forward_missing_in_netbox": len(missing_in_netbox),
        "scope_tag_targets_missing_in_netbox": len(missing_scope_tag_targets),
        "backfilled_reason_breakdown": reason_breakdown,
        # Absence is what defines an orphan, so which KIND of absence is the
        # first question to ask before deleting anything.
        "out_of_scope_absence": absence,
        # And the second question is how long the absence has lasted. A device
        # disabled in Forward looks exactly like one that left, so orphans wait
        # out a quarantine before the prune will touch them.
        "out_of_scope_quarantine": _quarantine_summary(sync, out_of_scope_pks),
        # "Carries neither include tag" covers two opposite situations. Orphans
        # can read zero while hundreds of devices are untagged, because a device
        # this sync never claimed is not an orphan of it.
        "unmanaged": unmanaged,
        "out_of_scope_sample": sorted(out_of_scope)[:SAMPLE_LIMIT],
        "empty_orphan_site_sample": empty_orphan_sites[:SAMPLE_LIMIT],
        "present_backfilled_sample": sorted(present_backfilled)[:SAMPLE_LIMIT],
        "present_backfilled_detail_sample": present_backfilled_detail,
        "missing_in_netbox_sample": sorted(missing_in_netbox)[:SAMPLE_LIMIT],
        "scope_tag_targets_missing_sample": sorted(missing_scope_tag_targets)[
            :SAMPLE_LIMIT
        ],
        # Internal sets for prune/tag; not meant for JSON serialization.
        "_tagged_names": tagged_names,
        "_device_tagged_names": device_tagged_names,
        "_forward_site_slugs": forward_site_slugs,
        "_out_of_scope": out_of_scope,
        "_out_of_scope_pks": out_of_scope_pks,
        "_present_backfilled": present_backfilled,
        "_matched_include_tags_by_name": present_scope_tags_by_name,
    }


def _unmanaged_device_summary(sync, tagged_names):
    """Split NetBox devices the current result does not cover by ownership.

    "This device carries neither include tag" is one observation covering two
    opposite situations, and an operator cannot act until they are separated:

      `owned_untagged`   - this sync holds a ForwardDeviceIdentity for the
                           device, so it created it, but the device is absent
                           from the current tag-scope result. Either it left
                           scope, or the tag was never applied. Ours either way,
                           and worth investigating.
      `unclaimed`        - no identity from this sync. Not ours to reason about:
                           another source created it, an operator did, or it is
                           a leftover from a configuration that no longer
                           applies - imported SNMP endpoints predate the change
                           that stopped generic endpoints being imported by
                           default, and nothing has ever revisited them.

    Deliberately read-only, local, and free: no Forward call, no deletion, and
    no inference about which of the two an operator should care about. The
    counts and the filters are the product; the judgement stays with them.
    """
    from ..models import ForwardDeviceIdentity

    untagged = [
        (device_id, name)
        for device_id, name in Device.objects.values_list("pk", "name")
        if (name or "").strip() and name not in tagged_names
    ]
    if not untagged:
        return {
            "untagged_total": 0,
            "owned_untagged": 0,
            "unclaimed": 0,
            "owned_untagged_sample": [],
            "unclaimed_sample": [],
        }
    owned_ids = set(
        ForwardDeviceIdentity.objects.filter(
            sync=sync,
            device_id__in=[device_id for device_id, _ in untagged],
        ).values_list("device_id", flat=True)
    )
    owned = sorted(name for device_id, name in untagged if device_id in owned_ids)
    unclaimed = sorted(
        name for device_id, name in untagged if device_id not in owned_ids
    )
    return {
        "untagged_total": len(untagged),
        "owned_untagged": len(owned),
        "unclaimed": len(unclaimed),
        "owned_untagged_sample": owned[:SAMPLE_LIMIT],
        "unclaimed_sample": unclaimed[:SAMPLE_LIMIT],
    }


def _classify_out_of_scope_absence(
    out_of_scope,
    *,
    client,
    network_id,
    snapshot_id,
):
    """Say WHICH kind of absence put each orphan out of scope.

    Membership is decided purely by absence from the tag-scope result, and three
    very different situations produce that absence:

      `absent_from_snapshot`  - Forward does not have the device at all. It was
                                removed from the network, or collection stopped
                                returning it.
      `present_untagged`      - Forward has it and may well have collected it,
                                but it no longer matches the include/exclude tag
                                predicate. A Forward-side tag edit looks like
                                this.
      `vendor_excluded`       - Forward has it but classifies it as a custom
                                command source, which every bundled query
                                filters out.

    Without this the panel can only say "absent from the result", and telling
    the three apart needs a live NQE probe the operator cannot run. That matters
    most when it is most dangerous: a query that silently narrowed presents as a
    large `present_untagged` set, and Prune orphans would delete live devices.

    Costs one NQE execution, and only when orphans exist - a converged sync adds
    no calls at all. The query carries no tag predicate and no vendor guard on
    purpose: it must see the devices the scope query filtered OUT.
    """
    if not out_of_scope:
        return {
            "available": True,
            "absent_from_snapshot": 0,
            "present_untagged": 0,
            "vendor_excluded": 0,
            "absent_from_snapshot_sample": [],
            "present_untagged_sample": [],
            "vendor_excluded_sample": [],
        }

    query = "\n".join(
        [
            "foreach device in network.devices",
            "select {",
            "  name: device.name,",
            "  vendor: toString(device.platform.vendor)",
            "}",
        ]
    )
    try:
        rows = client.run_nqe_query(
            query=query,
            network_id=network_id,
            snapshot_id=snapshot_id,
            fetch_all=True,
        )
    except JobTimeoutException:
        # The worker is being torn down; swallowing this would let the job look
        # like it finished. Never a classification failure.
        raise
    except Exception:
        # Advisory only. This must never fail the report that operators use to
        # decide whether a prune is safe - a missing classification is far
        # better than no scope report at all.
        return {"available": False}

    vendor_by_name = {}
    for row in rows:
        name = str(row.get("name") or "").strip()
        if name:
            vendor_by_name[name] = str(row.get("vendor") or "")

    absent = []
    untagged = []
    vendor_excluded = []
    for name in sorted(out_of_scope):
        vendor = vendor_by_name.get(name)
        if vendor is None:
            absent.append(name)
        elif vendor.endswith("FORWARD_CUSTOM"):
            vendor_excluded.append(name)
        else:
            untagged.append(name)
    return {
        "available": True,
        "absent_from_snapshot": len(absent),
        "present_untagged": len(untagged),
        "vendor_excluded": len(vendor_excluded),
        "absent_from_snapshot_sample": absent[:SAMPLE_LIMIT],
        "present_untagged_sample": untagged[:SAMPLE_LIMIT],
        "vendor_excluded_sample": vendor_excluded[:SAMPLE_LIMIT],
    }


def _endpoint_scope_names(
    sync,
    *,
    client,
    network_id,
    snapshot_id,
    include_tags,
    exclude_tags,
    include_match,
) -> tuple[set[str], dict[str, list[str]]]:
    """Return endpoint-import names protected by reconciliation and prune."""
    source_parameters = dict(getattr(sync.source, "parameters", {}) or {})
    if not source_parameters.get("sync_endpoints"):
        return set(), {}

    endpoint_include_tags = (
        list(include_tags)
        if effective_scope_endpoints_by_include_tags(source_parameters)
        else []
    )
    query = "\n".join(
        [
            "foreach endpoint in network.endpoints",
            "where !isEmpty(endpoint.snmpOutputs)",
            *build_endpoint_tag_scope_where(
                endpoint_include_tags,
                exclude_tags,
                include_match,
            ),
            *build_endpoint_device_eligibility_where(
                sync_generic_endpoints=bool(
                    source_parameters.get("sync_generic_endpoints")
                )
            ),
            "select { name: endpoint.name, tagNames: endpoint.tagNames }",
        ]
    )
    rows = client.run_nqe_query(
        query=query,
        network_id=network_id,
        snapshot_id=snapshot_id,
        fetch_all=True,
    )
    names = {
        str(row.get("name") or "").strip()
        for row in rows
        if str(row.get("name") or "").strip()
    }
    include_tag_set = set(include_tags)
    matched = {}
    for row in rows:
        name = str(row.get("name") or "").strip()
        if not name:
            continue
        tag_names = sorted(
            include_tag_set.intersection(
                str(tag) for tag in (row.get("tagNames") or [])
            )
        )
        if tag_names:
            matched[name] = tag_names
    return names, matched


class EmptyForwardScopeError(RuntimeError):
    """Raised when an empty Forward scope would make a mutation unsafe."""


class ScopeShrinkGuardError(RuntimeError):
    """Raised when the scope shrank far enough that a prune looks like a fault.

    The zero-device guard only catches a query that returned nothing at all. A
    query that returns most of the fleet - a Forward-side tag edit, a partial
    result, an org query that starts failing - passes it cleanly, and every
    device missing from that result is then deleted as an orphan.
    """


# A prune deleting more than this share of what the sync previously claimed is
# treated as a scope fault rather than attrition. Real decommissioning arrives
# in small batches; a query returning half the fleet does not.
SCOPE_SHRINK_REFUSAL_RATIO = 0.25

# ...but only once the absolute count is past what an operator can read. A
# ratio over small numbers is noise - three orphans out of eight claimed is
# 38% and means nothing - and a guard that fires on a lab or a small sync is a
# guard that gets switched off. `SAMPLE_LIMIT` is the natural line: at or below
# it the report shows every orphan by name, so the blast radius is reviewable
# by eye and the ratio adds nothing.
SCOPE_SHRINK_REFUSAL_FLOOR = SAMPLE_LIMIT


def _require_survivable_scope_shrink(report, *, allow_scope_shrink):
    previously_managed = int(report.get("forward_previously_managed") or 0)
    orphan_count = len(report.get("_out_of_scope") or ())
    if allow_scope_shrink or not previously_managed or not orphan_count:
        return
    if orphan_count <= SCOPE_SHRINK_REFUSAL_FLOOR:
        return
    ratio = orphan_count / previously_managed
    if ratio <= SCOPE_SHRINK_REFUSAL_RATIO:
        return
    raise ScopeShrinkGuardError(
        f"Refusing to prune: {orphan_count} of {previously_managed} devices "
        f"this sync previously claimed ({ratio:.0%}) are absent from the "
        "current Forward scope result. Above "
        f"{SCOPE_SHRINK_REFUSAL_RATIO:.0%} this is treated as a scope or "
        "query fault rather than devices leaving scope. Confirm the Forward "
        "query and its include tags still return the whole fleet, then re-run "
        "with the scope-shrink override if the removal is genuinely intended."
    )


def _require_nonempty_forward_scope(report, *, operation):
    if not report.get("_tagged_names"):
        raise EmptyForwardScopeError(
            "The Forward scope query returned 0 devices or endpoints; refusing "
            f"to {operation} because every NetBox device would be treated as "
            "out of scope."
        )


def _prunable_device_order(device_ids):
    """Return child-before-parent order and fail-closed cyclic identities."""
    from ..models import ForwardVirtualParentClaim

    candidate_ids = set(device_ids)
    dependencies = {device_id: set() for device_id in candidate_ids}
    dependents = {device_id: set() for device_id in candidate_ids}
    for child_id, parent_id in ForwardVirtualParentClaim.objects.filter(
        device_id__in=candidate_ids,
        parent_device_id__in=candidate_ids,
    ).values_list("device_id", "parent_device_id"):
        if child_id == parent_id:
            dependencies[parent_id].add(child_id)
            continue
        dependencies[parent_id].add(child_id)
        dependents[child_id].add(parent_id)

    ready = [
        device_id
        for device_id, required_ids in dependencies.items()
        if not required_ids
    ]
    heapq.heapify(ready)
    ordered = []
    while ready:
        device_id = heapq.heappop(ready)
        ordered.append(device_id)
        for parent_id in sorted(dependents[device_id]):
            dependencies[parent_id].discard(device_id)
            if not dependencies[parent_id]:
                heapq.heappush(ready, parent_id)
    cyclic_ids = candidate_ids.difference(ordered)
    return ordered, cyclic_ids


def _prune_result(
    *,
    required_runs,
    required_hours,
    pruned_device_count=0,
    pruned_object_count=0,
    out_of_scope_sample=(),
    ownership_blocked_device_count=0,
    protected_device_count=0,
    held_device_count=0,
    overridden_device_count=0,
) -> dict:
    """One shape for every exit from the prune, however early it returns.

    The early returns used to omit keys the later one carried, so a caller that
    read `result["ownership_blocked_device_count"]` worked or raised KeyError
    depending on how far the prune got - a difference nothing in the signature
    hints at.
    """
    return {
        "pruned_device_count": pruned_device_count,
        "pruned_object_count": pruned_object_count,
        "out_of_scope_sample": list(out_of_scope_sample),
        "ownership_blocked_device_count": ownership_blocked_device_count,
        "protected_device_count": protected_device_count,
        "quarantine_required_runs": required_runs,
        "quarantine_required_hours": required_hours,
        "quarantine_held_device_count": held_device_count,
        "quarantine_overridden_device_count": overridden_device_count,
    }


def prune_orphan_devices(
    sync,
    *,
    report=None,
    allow_scope_shrink=False,
    include_quarantined=False,
) -> dict:
    """Delete NetBox devices not present in the sync's Forward scope.

    Safety, in order: refuses when the Forward query returned 0 devices; refuses
    when the result shrank far enough that a query fault is likelier than devices
    genuinely leaving scope; and holds back any orphan whose absence has not yet
    persisted through the quarantine. Tagged-but-backfilled devices are
    preserved. Returns counts. Pass ``report`` (from
    ``compute_scope_reconciliation``) to avoid re-running the Forward query.

    ``include_quarantined`` is for the manual button only. A person looking at a
    named list of orphans and choosing to delete them is a different act from a
    scheduled job doing it unattended, and it is the unattended path that caused
    the harm. The automated caller does not pass it.
    """
    if report is None:
        report = compute_scope_reconciliation(sync)
    out_of_scope = report["_out_of_scope"]
    if not report.get("_device_tagged_names", report["_tagged_names"]):
        raise EmptyForwardScopeError(
            "The Forward scope query returned 0 devices; refusing to prune because "
            "every NetBox device would be treated as an orphan."
        )
    _require_survivable_scope_shrink(report, allow_scope_shrink=allow_scope_shrink)
    if not out_of_scope:
        required_runs, required_hours = absence_quarantine_thresholds(sync)
        return _prune_result(
            required_runs=required_runs,
            required_hours=required_hours,
        )

    orphans = sorted(out_of_scope)
    # Delete by the explicit device PKs resolved at scope-compute time
    # (identity-aware) rather than re-matching the non-unique device name at delete
    # time. Reports without exact identity evidence fail closed.
    orphan_pks = list(report.get("_out_of_scope_pks") or [])
    if not orphan_pks and orphans:
        raise ValueError(
            "Orphan prune requires exact device identity evidence from the current "
            "scope reconciliation report."
        )
    partition = partition_quarantined_orphans(sync, orphan_pks)
    held_device_count = len(partition["held_pks"])
    if not include_quarantined:
        orphan_pks = partition["eligible_pks"]
    quarantine_counts = {
        "required_runs": partition["required_runs"],
        "required_hours": partition["required_hours"],
        "held_device_count": 0 if include_quarantined else held_device_count,
        "overridden_device_count": held_device_count if include_quarantined else 0,
    }
    if not orphan_pks:
        return _prune_result(
            out_of_scope_sample=orphans[:SAMPLE_LIMIT],
            **quarantine_counts,
        )
    from .ownership import ownership_write_lock
    from .ownership import _release_prunable_device_ownership_locked

    deleted_total = 0
    pruned_device_ids = []
    protected_tally = {}
    ownership_blocked_ids = set()
    pending_device_ids = set(orphan_pks)
    while pending_device_ids:
        ordered_device_ids, cyclic_device_ids = _prunable_device_order(
            pending_device_ids
        )
        if cyclic_device_ids:
            ownership_blocked_ids.update(cyclic_device_ids)
            protected_tally["forward_netbox.forwardvirtualparentclaim"] = (
                protected_tally.get(
                    "forward_netbox.forwardvirtualparentclaim",
                    0,
                )
                + len(cyclic_device_ids)
            )
        retry_device_ids = set()
        pass_progress = False
        for device_id in ordered_device_ids:
            try:
                with ownership_write_lock():
                    release = _release_prunable_device_ownership_locked(
                        sync,
                        [device_id],
                    )
                    if release["blocked_device_ids"]:
                        retry_device_ids.add(device_id)
                        continue
                    lock_related_writes_for_delete(
                        Device,
                        using=Device.objects.db,
                    )
                    deleted, _ = Device.objects.filter(pk=device_id).delete()
                    deleted_total += deleted
                    pruned_device_ids.append(device_id)
                    pass_progress = True
            except ProtectedError as exc:
                for obj in exc.protected_objects:
                    label = obj._meta.label_lower
                    protected_tally[label] = protected_tally.get(label, 0) + 1
        if not retry_device_ids:
            break
        if not pass_progress:
            ownership_blocked_ids.update(retry_device_ids)
            break
        pending_device_ids = retry_device_ids
    result = _prune_result(
        pruned_device_count=len(pruned_device_ids),
        pruned_object_count=deleted_total,
        out_of_scope_sample=orphans[:SAMPLE_LIMIT],
        ownership_blocked_device_count=len(ownership_blocked_ids),
        protected_device_count=len(orphan_pks)
        - len(pruned_device_ids)
        - len(ownership_blocked_ids),
        **quarantine_counts,
    )
    if protected_tally:
        result["protected_by_model"] = protected_tally
    return result


def _occupied_site_ids() -> set:
    """Site PKs referenced by ANY related object (FK), across every relation.

    A site is "truly empty" only when nothing points to it. We union the site
    foreign keys of every reverse relation (devices, racks, prefixes, VLANs, VMs,
    power panels, locations, clusters, wireless LANs, circuit/cable terminations,
    …) rather than just devices+racks. This matters for two reasons NetBox's own
    FK ``on_delete`` rules impose:
      * PROTECT (Device, Rack, PowerPanel, VLAN, VirtualMachine) — deleting a site
        that still has one of these raises ``ProtectedError``.
      * CASCADE (Prefix, Location, Cluster, WirelessLAN, CircuitTermination) —
        deleting the site would silently destroy those children.
    Either way such a site is not "truly empty" and must be kept. Many-to-many
    relations (e.g. ConfigContext.sites) do not pin a site and are skipped.
    """
    from dcim.models import Site

    occupied = set()
    for rel in Site._meta.related_objects:
        if rel.many_to_many:
            continue
        attname = rel.field.attname  # e.g. "site_id" / "_site_id"
        occupied.update(
            rel.related_model.objects.exclude(**{attname: None}).values_list(
                attname, flat=True
            )
        )
    occupied.discard(None)
    return occupied


def prune_orphan_sites(sync, *, report=None) -> dict:
    """Delete truly-empty NetBox sites absent from the sync's Forward location scope.

    Only removes sites that nothing references (no devices, racks, prefixes, VLANs,
    VMs, power panels, locations, clusters, …) — see ``_occupied_site_ids``. A site
    with any remaining object is kept, so the prune neither hits a NetBox PROTECT
    error nor cascade-deletes child objects. Re-queries current DB state so sites
    emptied by the device prune in the same job are also removed. Deletes one site
    at a time and skips any that unexpectedly raise ``ProtectedError`` so a single
    surprise relation cannot abort the whole prune. Safety: refuses when the
    Forward scope returned 0 devices or no location data.
    """
    from django.db.models.deletion import ProtectedError

    from dcim.models import Site

    if report is None:
        report = compute_scope_reconciliation(sync)
    if not report.get("_device_tagged_names", report["_tagged_names"]):
        raise EmptyForwardScopeError(
            "Forward scope returned 0 devices; refusing site prune."
        )
    forward_site_slugs = report.get("_forward_site_slugs") or set()
    if not forward_site_slugs:
        return {"pruned_site_count": 0, "pruned_site_object_count": 0, "skipped": 0}
    occupied_site_ids = _occupied_site_ids()
    prunable_pks = list(
        Site.objects.exclude(slug__in=forward_site_slugs)
        .exclude(pk__in=occupied_site_ids)
        .values_list("pk", flat=True)
    )
    if not prunable_pks:
        return {"pruned_site_count": 0, "pruned_site_object_count": 0, "skipped": 0}
    pruned_sites = 0
    pruned_objects = 0
    skipped = 0
    for pk in prunable_pks:
        try:
            with transaction.atomic():
                deleted, _ = Site.objects.filter(pk=pk).delete()
            pruned_sites += 1
            pruned_objects += deleted
        except ProtectedError:
            # A relation not covered by the occupancy union still pins this site;
            # leave it rather than fail the whole prune.
            skipped += 1
    return {
        "pruned_site_count": pruned_sites,
        "pruned_site_object_count": pruned_objects,
        "skipped": skipped,
    }


def _apply_maintained_device_tag(
    sync,
    device_names,
    *,
    slug,
    name,
    color,
    description,
    claim_type,
    generation,
    snapshot_id,
    mark_domain=True,
    materialize=True,
    live_source_keys=None,
):
    """Reconcile one sync generation's claims for a maintained status tag."""
    from .ownership import reconcile_source_device_tag_claims

    result = reconcile_source_device_tag_claims(
        sync,
        device_names,
        slug=slug,
        name=name,
        color=color,
        description=description,
        claim_type=claim_type,
        generation=generation,
        snapshot_id=snapshot_id,
        mark_domain=mark_domain,
        materialize=materialize,
        live_source_keys=live_source_keys,
    )
    # `_ambiguous_names` carries device names. It is dropped here rather than
    # relied on to go unread: this dict is spread into a job payload, and a
    # customer's device names must not reach a persisted diagnostic.
    return {
        "added": result["assignments_added"],
        "removed": result["assignments_removed"],
        **{key: value for key, value in result.items() if not key.startswith("_")},
    }


def tag_backfilled_devices(
    sync,
    *,
    report=None,
    snapshot_id=None,
    ingestion_id=None,
) -> dict:
    """Maintain the ``forward-backfilled`` and ``forward-out-of-scope`` device tags.

    ``forward-backfilled`` marks devices that are tagged-in-scope but were not
    freshly collected in the latest snapshot (kept on purpose).
    ``forward-out-of-scope`` marks NetBox devices that match none of the sync's
    included Forward tags (the removable orphans). Both are idempotent — after
    running, each tag's device set exactly matches the current bucket, so operators
    can filter ``/dcim/devices/?tag=forward-backfilled`` or
    ``?tag=forward-out-of-scope``.
    """
    if report is None:
        report = compute_scope_reconciliation(sync, snapshot_id=snapshot_id)
    _require_nonempty_forward_scope(
        report,
        operation="maintain device scope tags",
    )

    with transaction.atomic(), current_post_sync_snapshot(
        sync,
        snapshot_id,
        ingestion_id=ingestion_id,
    ) as generation:
        # The full tag-scope result. It is what makes stale-binding retirement
        # safe: a key absent from THIS set is absent from everything Forward
        # currently reports under these tags, not merely from one tag's slice.
        live_source_keys = report["_tagged_names"]
        backfilled = _apply_maintained_device_tag(
            sync,
            report["_present_backfilled"],
            slug=BACKFILLED_TAG_SLUG,
            name=BACKFILLED_TAG_NAME,
            color=BACKFILLED_TAG_COLOR,
            description=BACKFILLED_TAG_DESCRIPTION,
            claim_type="backfilled",
            generation=generation["generation"],
            snapshot_id=generation["snapshot_id"],
            mark_domain=False,
            materialize=False,
            live_source_keys=live_source_keys,
        )
        out_of_scope = _apply_maintained_device_tag(
            sync,
            report["_out_of_scope"],
            slug=OUT_OF_SCOPE_TAG_SLUG,
            name=OUT_OF_SCOPE_TAG_NAME,
            color=OUT_OF_SCOPE_TAG_COLOR,
            description=OUT_OF_SCOPE_TAG_DESCRIPTION,
            claim_type="out_of_scope",
            generation=generation["generation"],
            snapshot_id=generation["snapshot_id"],
            mark_domain=False,
            materialize=False,
            live_source_keys=live_source_keys,
        )
        source_parameters = getattr(sync.source, "parameters", None) or {}
        managed_scope_cleanup = {
            "claims_added": 0,
            "claims_released": 0,
            "assignments_added": 0,
            "assignments_removed": 0,
            "current": True,
        }
        from ..models import ForwardDeviceTagClaim
        from ..models import ForwardOwnershipReconciliation

        has_scope_ownership = (
            ForwardDeviceTagClaim.objects.filter(
                sync=sync,
                claim_type="scope",
            ).exists()
            or ForwardOwnershipReconciliation.objects.filter(
                sync=sync,
                domain=ForwardOwnershipReconciliation.Domain.SCOPE_TAGS,
            ).exists()
        )
        if source_parameters.get("apply_device_scope_tags") or has_scope_ownership:
            from .ownership import reconcile_sync_scope_tag_claims

            managed_scope_cleanup = reconcile_sync_scope_tag_claims(
                sync,
                (
                    report.get("_matched_include_tags_by_name", {})
                    if source_parameters.get("apply_device_scope_tags")
                    else {}
                ),
                generation=generation["generation"],
                snapshot_id=generation["snapshot_id"],
                live_source_keys=live_source_keys,
            )
        from .ownership import finalize_device_tag_domain

        status_materialized = finalize_device_tag_domain(
            sync,
            ForwardOwnershipReconciliation.Domain.STATUS_TAGS,
            generation["generation"],
            generation["snapshot_id"],
        )
        # Inside the same transaction as the tagging it derives from, so a run
        # that fails partway does not leave a streak claiming an absence the
        # tags never recorded.
        absence_streak = record_device_absence(
            sync,
            report.get("_out_of_scope_pks") or (),
            snapshot_id=generation["snapshot_id"],
        )
    return {
        "tag_slug": BACKFILLED_TAG_SLUG,
        "tagged": status_materialized["by_claim_type"]
        .get("backfilled", {})
        .get("assignments_added", 0),
        "untagged": status_materialized["by_claim_type"]
        .get("backfilled", {})
        .get("assignments_removed", 0),
        "backfilled_claims_added": backfilled["claims_added"],
        "backfilled_claims_released": backfilled["claims_released"],
        "total_backfilled": backfilled["total"],
        "out_of_scope_tag_slug": OUT_OF_SCOPE_TAG_SLUG,
        "out_of_scope_tagged": status_materialized["by_claim_type"]
        .get("out_of_scope", {})
        .get("assignments_added", 0),
        "out_of_scope_untagged": status_materialized["by_claim_type"]
        .get("out_of_scope", {})
        .get("assignments_removed", 0),
        "out_of_scope_claims_added": out_of_scope["claims_added"],
        "out_of_scope_claims_released": out_of_scope["claims_released"],
        "total_out_of_scope": out_of_scope["total"],
        "scope_claims_released": managed_scope_cleanup["claims_released"],
        "out_of_scope_scope_tags_removed": managed_scope_cleanup["assignments_removed"],
        "scope_claims_added": managed_scope_cleanup["claims_added"],
        "scope_tags_added": managed_scope_cleanup["assignments_added"],
        "ownership_current": bool(
            status_materialized["current"] and managed_scope_cleanup["current"]
        ),
        # Names that resolve to more than one NetBox device are held: their
        # existing tag state is neither extended nor withdrawn. This used to
        # refuse the whole job, so the count is what tells an operator that
        # de-duplicating those devices is worth doing - and that ownership
        # completed anyway.
        "ambiguous_device_names": max(
            backfilled["ambiguous_device_names"],
            out_of_scope["ambiguous_device_names"],
            managed_scope_cleanup.get("ambiguous_device_names", 0),
        ),
        "held_ambiguous_devices": (
            backfilled["held_ambiguous_devices"]
            + out_of_scope["held_ambiguous_devices"]
            + managed_scope_cleanup.get("held_ambiguous_devices", 0)
        ),
        "skipped_absent_devices": max(
            backfilled["skipped_absent_devices"],
            out_of_scope["skipped_absent_devices"],
        ),
        "absence_streaks_started": absence_streak["started"],
        "absence_streaks_advanced": absence_streak["advanced"],
        "absence_streaks_cleared": absence_streak["cleared"],
    }
