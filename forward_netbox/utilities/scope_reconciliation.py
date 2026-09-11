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
from .json_safe import json_safe_value
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

# NetBox tag applied to devices this sync CREATED that the current Forward
# result no longer covers - the `owned_untagged` half of "carry no include tag".
#
# It exists because that bucket is the only one an operator could not enumerate.
# The panel showed a count and a 25-name sample, and the count is the one that
# grows: a device disabled in Forward drops out of the tag-scope result, and
# since the absence quarantine it is deliberately kept rather than pruned, so it
# lands here and stays. A customer reading 552 had no way, in the UI or on a
# shell, to list them - the CLI audit prints the same truncated sample.
#
# Deliberately NOT applied to the `unclaimed` half. A device this sync never
# created is not this sync's to label, and the claim machinery enforces that on
# its own: an unclaimed device has no `ForwardDeviceIdentity`, so it resolves as
# `missing` and is skipped.
UNCOVERED_TAG_SLUG = "forward-uncovered"
UNCOVERED_TAG_NAME = "Forward Uncovered"
UNCOVERED_TAG_COLOR = "ff9800"
UNCOVERED_TAG_DESCRIPTION = (
    "Created by this sync but absent from the current Forward tag-scope "
    "result. Not an orphan, and never pruned on this tag alone. Maintained by "
    "the Forward sync scope reconciliation."
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


def record_device_absence(
    sync, out_of_scope_pks, *, uncovered_pks=(), snapshot_id=""
) -> dict:
    """Advance the absence streak for absent devices, clear it for the rest.

    Two sets are tracked, because two sets can be deleted. Orphans are devices
    this sync claimed in the run that produced the current result. The
    uncovered set is devices it created at some point and the current result no
    longer covers - which is not a subset of the first, and at a customer whose
    orphan count reads zero it is the only one that has members. Streaks were
    kept for orphans alone, so an uncovered device could never leave quarantine
    no matter how long it had been gone, and nothing could clean it up.

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
    absent_ids = set(out_of_scope_pks or ()) | set(uncovered_pks or ())
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

    from ..models import ForwardDeviceAbsence
    from ..models import ForwardDeviceTagClaim

    claimed = list(
        ForwardDeviceTagClaim.objects.filter(sync=sync, claim_type="scope")
        .select_related("device")
        .values_list("device_id", "device__name")
    )
    # A device absent from Forward loses its scope claim on the FIRST run that
    # observes the absence: the claim reconciliation releases every claim whose
    # device is not in the current result. So by the second run the claim table
    # no longer remembers that this sync ever managed the device, it drops out of
    # `out_of_scope` entirely, and both the quarantine streak and the prune lose
    # sight of it - the streak cannot pass 1 and the orphan can never be deleted.
    #
    # The absence row is the memory that survives the release, so it is the other
    # half of "this sync previously managed this device". It is created only from
    # a device that held a claim, and it is cleared the moment the device returns
    # to the Forward result, so this widens what counts as previously managed
    # without inventing a claim the sync never made.
    quarantined = list(
        ForwardDeviceAbsence.objects.filter(sync=sync)
        .select_related("device")
        .values_list("device_id", "device__name")
    )
    managed_by_id = dict(claimed)
    for device_id, name in quarantined:
        managed_by_id.setdefault(device_id, name)
    previously_managed = sorted(managed_by_id.items())
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
    # Primary keys, never names: the panel's red Prune-orphans button deletes
    # this exact set and the page could only ever show 25 of it, so the full
    # list needs a route, and a route needs the ids in the PERSISTED payload
    # (the `_`-prefixed entries below are stripped before the job is stored).
    # Names are customer data in a job payload; keys are not.
    present_backfilled_pks = list(
        Device.objects.filter(name__in=sorted(present_backfilled)).values_list(
            "pk", flat=True
        )
    )

    unmanaged, owned_untagged_names = _unmanaged_device_summary(sync, tagged_names)

    # One census query classifies BOTH absent sets. The orphans have carried
    # this split since 2.7.x; the owned-uncovered devices never had it, and
    # "why is this device uncovered" - disabled in Forward, untagged in Forward,
    # or a custom-command source - is the question a customer actually asks
    # when that count grows. The query carries no predicate, so widening the
    # set it classifies costs no extra NQE execution; `forward_api_usage` reads
    # the same after this change as before it.
    # Both prunes delete on `absent`, and an endpoint-derived device that
    # left endpoint scope is missing from `network.devices` while Forward
    # still has it. One extra unfiltered query is the price of `absent`
    # meaning what the prune assumes it means - and it is paid whether or not
    # endpoint sync is on, because turning it off is exactly the moment every
    # endpoint-derived device would otherwise become `absent` and prunable.
    census = _absence_census(
        out_of_scope | owned_untagged_names,
        client=client,
        network_id=network_id,
        snapshot_id=snapshot_id,
        endpoint_scope=_endpoint_scope_settings(
            sync, include_tags, exclude_tags, include_match
        ),
    )
    kinds, details = census if census is not None else (None, None)
    absence = _absence_summary(out_of_scope, kinds, details)
    unmanaged["owned_absence"] = _absence_summary(owned_untagged_names, kinds, details)
    # What the uncovered cleanup would actually act on, and how much of it the
    # quarantine is still holding. Without this the panel shows a count of 105
    # and a button that deletes 3, with nothing on the page explaining the gap.
    owned_absent_names = (
        {name for name in owned_untagged_names if (kinds or {}).get(name) == "absent"}
        if kinds is not None
        else set()
    )
    owned_absent_pks = []
    owned_endpoint_detail_by_id = {}
    for device_id, name in Device.objects.filter(
        pk__in=list(unmanaged.get("owned_untagged_device_ids") or ())
    ).values_list("pk", "name"):
        name = (name or "").strip()
        if name in owned_absent_names:
            owned_absent_pks.append(device_id)
        detail = (details or {}).get(name)
        if detail:
            owned_endpoint_detail_by_id[str(device_id)] = detail
    unmanaged["owned_prune_candidates"] = len(owned_absent_pks)
    # The endpoint-scope rule behind each uncovered endpoint device, by pk, so
    # the device page can say which setting or tag would cover it again.
    unmanaged["owned_endpoint_detail_by_id"] = owned_endpoint_detail_by_id
    # Persisted (no leading underscore) so the device page can tell whether one
    # device is in the prune's target set without recomputing the census. Keys
    # only - names are customer data and do not belong in a job payload.
    unmanaged["owned_absent_device_ids"] = sorted(owned_absent_pks)
    unmanaged["owned_quarantine"] = (
        _quarantine_summary(sync, owned_absent_pks)
        if kinds is not None
        else {"available": False}
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
        "_owned_untagged": owned_untagged_names,
        "_missing_in_netbox": missing_in_netbox,
        # Persisted (no leading underscore): the two full-list views read
        # these. `_out_of_scope_pks` is kept for the prune, which runs inside
        # the same call and wants the unserialized form.
        "out_of_scope_device_ids": sorted(out_of_scope_pks),
        "present_backfilled_device_ids": sorted(present_backfilled_pks),
        "_out_of_scope_pks": out_of_scope_pks,
        # The census verdict per name. A cleanup acts only on "absent"; a device
        # Forward still reports is a scoping question, not a dead device.
        "_absence_kinds": kinds,
        "_absence_details": details,
        "_owned_untagged_pks": list(unmanaged.get("owned_untagged_device_ids") or ()),
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

    Returns ``(summary, owned_names)``. The summary is JSON-safe and carries
    only counts and truncated samples; the full owned name set is handed back
    separately because it is what the ``forward-uncovered`` tag is maintained
    from, and it must not reach a persisted diagnostic.
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
            "owned_untagged_device_ids": [],
            "unclaimed_device_ids": [],
        }, set()
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
    # The primary keys, so the page can LIST either half in full. Names stay
    # capped at the sample size because they are customer data in a persisted
    # job payload; keys are not, and NetBox's own device table renders them.
    # The unclaimed half is the one nothing else can list: it is not this
    # sync's data, so no tag is maintained for it, and until these keys were
    # kept an operator whose count was mostly unclaimed could see 25 names of
    # it and nothing more.
    return {
        "untagged_total": len(untagged),
        "owned_untagged": len(owned),
        "unclaimed": len(unclaimed),
        "owned_untagged_sample": owned[:SAMPLE_LIMIT],
        "unclaimed_sample": unclaimed[:SAMPLE_LIMIT],
        "owned_untagged_device_ids": sorted(
            device_id for device_id, _ in untagged if device_id in owned_ids
        ),
        "unclaimed_device_ids": sorted(
            device_id for device_id, _ in untagged if device_id not in owned_ids
        ),
    }, set(owned)


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
    return _absence_summary(
        out_of_scope,
        _absence_kinds(
            out_of_scope,
            client=client,
            network_id=network_id,
            snapshot_id=snapshot_id,
        ),
    )


# Why an endpoint-derived device is `untagged` rather than `absent`: Forward
# still reports it under `network.endpoints`, and one of these rules keeps it
# out of the endpoint scope query. Each names the setting or the Forward-side
# fact an operator would change, which is the point of splitting them.
ENDPOINT_ABSENCE_DETAILS = {
    "endpoint_scope_off": "endpoint sync is off on this source",
    "endpoint_no_snmp": "Forward has no SNMP output for it in this snapshot",
    "endpoint_cimc": "a CIMC controller, modelled as inventory rather than a device",
    "endpoint_generic": "a generic SNMP endpoint, and generic endpoints are not imported",
    "endpoint_untagged": "its endpoint tags match none of the include tags",
    "endpoint_in_scope": "in endpoint scope by every rule, yet not in the result",
}


def _endpoint_scope_settings(sync, include_tags, exclude_tags, include_match):
    """The endpoint-scope rules, as the census needs them to explain a miss."""
    parameters = dict(getattr(sync.source, "parameters", {}) or {})
    return {
        "enabled": bool(parameters.get("sync_endpoints")),
        "generic": bool(parameters.get("sync_generic_endpoints")),
        "include_tags": (
            list(include_tags)
            if effective_scope_endpoints_by_include_tags(parameters)
            else []
        ),
        "exclude_tags": list(exclude_tags or ()),
        "include_match": include_match,
    }


def _endpoint_absence_detail(row, scope):
    """Which endpoint-scope rule keeps this Forward endpoint out of the result."""
    if not scope.get("enabled"):
        return "endpoint_scope_off"
    if not row.get("has_snmp"):
        return "endpoint_no_snmp"
    if row.get("cimc"):
        return "endpoint_cimc"
    if not row.get("console") and not scope.get("generic"):
        return "endpoint_generic"
    tags = {str(tag) for tag in (row.get("tags") or [])}
    include_tags = scope.get("include_tags") or []
    if include_tags:
        matched = [tag for tag in include_tags if tag in tags]
        required = len(include_tags) if scope.get("include_match") == "all" else 1
        if len(matched) < required:
            return "endpoint_untagged"
    if any(tag in tags for tag in scope.get("exclude_tags") or []):
        return "endpoint_untagged"
    return "endpoint_in_scope"


def _absence_census(names, *, client, network_id, snapshot_id, endpoint_scope=None):
    """Classify each name: ``(kinds, details)``, or ``None`` if the census
    could not run so every caller renders "unavailable" rather than a zero.

    ``kinds`` is three-valued - ``absent``, ``untagged`` or ``vendor_excluded``
    - and is what both prunes gate on; that vocabulary must not widen.
    ``details`` refines ``untagged`` for endpoint-derived devices with the
    endpoint-scope rule that excludes them (see ``ENDPOINT_ABSENCE_DETAILS``),
    because "in Forward but untagged" is one badge covering a console server
    whose tags need adding to the include set and a generic SNMP endpoint that
    is out by design.

    One query per Forward table for however many names are asked about. Neither
    carries a tag predicate or a vendor guard, on purpose: the census must see
    the devices the scope query filtered OUT. `absent` means "Forward does not
    have this at all", and it is what both prunes delete on; an SNMP-endpoint
    device that drops out of endpoint scope is missing from ``network.devices``
    while Forward still reports it under ``network.endpoints``, so the endpoint
    table is always probed.
    """
    if not names:
        return {}, {}
    scope = endpoint_scope or {"enabled": False}

    query = "\n".join(
        [
            "foreach device in network.devices",
            "select {",
            "  name: device.name,",
            "  vendor: toString(device.platform.vendor)",
            "}",
        ]
    )
    endpoint_query = "\n".join(
        [
            "foreach endpoint in network.endpoints",
            'let sysDescrOpt = max(foreach o in endpoint.snmpOutputs where o.requestedOid == "1.3.6.1.2.1.1.1" select max(foreach e in o.rawOidEntries select e.rawValue))',
            'let sysObjIdOpt = max(foreach o in endpoint.snmpOutputs where o.requestedOid == "1.3.6.1.2.1.1.2" select max(foreach e in o.rawOidEntries select e.rawValue))',
            'let sysDescr = if isPresent(sysDescrOpt) then toLowerCase(sysDescrOpt) else ""',
            'let sysObjId = if isPresent(sysObjIdOpt) then sysObjIdOpt else ""',
            "let nameLower = toLowerCase(toString(endpoint.name))",
            "let profileLower = toLowerCase(toString(endpoint.profileName))",
            "select {",
            "  name: endpoint.name,",
            "  has_snmp: !isEmpty(endpoint.snmpOutputs),",
            '  cimc: matches(nameLower, "*cimc*") || matches(profileLower, "*cimc*") || matches(sysDescr, "*cisco integrated management controller*"),',
            '  console: matches(sysObjId, "1.3.6.1.4.1.10418.*") || matches(sysObjId, "1.3.6.1.4.1.2925.*") || matches(sysDescr, "*avocent*") || matches(sysDescr, "*cyclades*") || matches(sysDescr, "*alterpath*") || matches(sysObjId, "1.3.6.1.4.1.25049.*") || matches(sysDescr, "*opengear*"),',
            "  tags: endpoint.tagNames",
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
        # Unfiltered by tag on purpose, exactly like the device half: a name
        # Forward still reports anywhere is not `absent`, whatever scope it has
        # fallen out of.
        endpoint_rows = client.run_nqe_query(
            query=endpoint_query,
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
        return None

    vendor_by_name = {}
    for row in rows:
        name = str(row.get("name") or "").strip()
        if name:
            vendor_by_name[name] = str(row.get("vendor") or "")

    endpoint_by_name = {}
    for row in endpoint_rows:
        name = str(row.get("name") or "").strip()
        if name:
            endpoint_by_name[name] = row

    kinds = {}
    details = {}
    for name in names:
        vendor = vendor_by_name.get(name)
        if vendor is None:
            # Forward has no DEVICE by this name. Before calling it absent,
            # check the endpoint table: an endpoint-derived device that left
            # endpoint scope is still reported by Forward, so it is a scoping
            # decision (`untagged`) rather than a removal, and must never be
            # deleted by a prune that acts on absence.
            endpoint = endpoint_by_name.get(name)
            if endpoint is None:
                kinds[name] = "absent"
            else:
                kinds[name] = "untagged"
                details[name] = _endpoint_absence_detail(endpoint, scope)
        elif vendor.endswith("FORWARD_CUSTOM"):
            kinds[name] = "vendor_excluded"
        else:
            kinds[name] = "untagged"
    return kinds, details


def _absence_kinds(names, *, client, network_id, snapshot_id, include_endpoints=True):
    """``kinds`` alone; see ``_absence_census``. Kept for callers that gate."""
    census = _absence_census(
        names, client=client, network_id=network_id, snapshot_id=snapshot_id
    )
    return None if census is None else census[0]


def _absence_summary(names, kinds, details=None):
    """The panel's three counts and samples for one absent set, plus the
    endpoint breakdown of ``present_untagged`` (reason -> count and sample)."""
    if not names:
        return {
            "available": True,
            "absent_from_snapshot": 0,
            "present_untagged": 0,
            "vendor_excluded": 0,
            "absent_from_snapshot_sample": [],
            "present_untagged_sample": [],
            "vendor_excluded_sample": [],
            "endpoint_detail": [],
        }
    if kinds is None:
        return {"available": False}
    absent = sorted(name for name in names if kinds.get(name) == "absent")
    untagged = sorted(name for name in names if kinds.get(name) == "untagged")
    vendor_excluded = sorted(
        name for name in names if kinds.get(name) == "vendor_excluded"
    )
    by_detail = {}
    for name in untagged:
        detail = (details or {}).get(name)
        if detail:
            by_detail.setdefault(detail, []).append(name)
    endpoint_detail = [
        {
            "reason": reason,
            "label": ENDPOINT_ABSENCE_DETAILS.get(reason, reason),
            "count": len(members),
            "sample": members[:SAMPLE_LIMIT],
        }
        for reason, members in sorted(by_detail.items())
    ]
    return {
        "available": True,
        "absent_from_snapshot": len(absent),
        "present_untagged": len(untagged),
        "vendor_excluded": len(vendor_excluded),
        "absent_from_snapshot_sample": absent[:SAMPLE_LIMIT],
        "present_untagged_sample": untagged[:SAMPLE_LIMIT],
        "vendor_excluded_sample": vendor_excluded[:SAMPLE_LIMIT],
        "endpoint_detail": endpoint_detail,
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


class ScopeCensusUnavailableError(RuntimeError):
    """The census that says why a device is uncovered did not run.

    Raised rather than treated as "no devices are absent": a census failure and
    a genuinely empty absent set look identical from the counts, and only one of
    them makes deleting safe.
    """


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
    restricted_refusals=None,
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
        # Why a specifically-requested device was not deleted, by reason. Empty
        # lists on every unrestricted exit, so the key is always present and
        # always means the same thing.
        "restricted_refusals": dict(
            restricted_refusals or {"not_owned": [], "not_absent": [], "held": []}
        ),
    }


def _delete_prunable_devices(sync, device_pks):
    """Delete devices, releasing ownership first and respecting PROTECT.

    One implementation, shared by every prune. A second copy of this loop is
    how a delete path acquires a guard the other one does not have, and the
    deletes here are permanent.

    Returns the ids actually deleted, the total objects removed, a tally of what
    refused by model label, and the ids ownership would not release.
    """
    from .ownership import ownership_write_lock
    from .ownership import _release_prunable_device_ownership_locked

    deleted_total = 0
    pruned_device_ids = []
    protected_tally = {}
    ownership_blocked_ids = set()
    pending_device_ids = set(device_pks)
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
    return pruned_device_ids, deleted_total, protected_tally, ownership_blocked_ids


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
    # Gate on the CAUSE of the absence, exactly as the uncovered prune does.
    # Out-of-scope membership is decided purely by absence from the current tag
    # result, and that covers three situations with opposite remedies: a device
    # Forward no longer has, one Forward still reports under different tags, and
    # one Forward classifies as a custom-command source. Only the first is a
    # removal. Without this gate a Forward-side tag edit, or a query that
    # narrowed, made live devices prune-eligible - the panel showed 63 gone and
    # 2 still in Forward while offering to delete all 65.
    kinds = report.get("_absence_kinds")
    if kinds is None:
        raise ScopeCensusUnavailableError(
            "The Forward census that classifies each absence did not run, so an "
            "orphan prune cannot tell a removed device from one Forward still "
            "reports. Refresh the scope reconciliation report and retry."
        )
    absent_names = {name for name in out_of_scope if kinds.get(name) == "absent"}
    # Resolve through the pks the report already established rather than
    # re-matching a name NetBox does not hold unique.
    orphan_pks = [
        device_id
        for device_id, name in Device.objects.filter(pk__in=orphan_pks).values_list(
            "pk", "name"
        )
        if (name or "").strip() in absent_names
    ]
    if not orphan_pks:
        required_runs, required_hours = absence_quarantine_thresholds(sync)
        return _prune_result(
            out_of_scope_sample=orphans[:SAMPLE_LIMIT],
            required_runs=required_runs,
            required_hours=required_hours,
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
    (
        pruned_device_ids,
        deleted_total,
        protected_tally,
        ownership_blocked_ids,
    ) = _delete_prunable_devices(sync, orphan_pks)
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


def _require_survivable_uncovered_shrink(sync, absent_names, *, allow_scope_shrink):
    """Refuse an uncovered cleanup that is too large to be ordinary attrition.

    The orphan guard cannot stand in for this one. It measures orphans against
    what the run previously claimed, and at the customer this was built for the
    orphan count is zero while hundreds of devices are uncovered - so it returns
    early and guards nothing. This measures the set actually being deleted,
    against every device this sync has ever created.

    The same two-part threshold as the orphan guard, and for the same reason: a
    ratio alone fires on a small estate where three of eight is normal, so an
    absolute floor has to clear first.
    """
    from ..models import ForwardDeviceIdentity

    if allow_scope_shrink or not absent_names:
        return
    owned_total = ForwardDeviceIdentity.objects.filter(sync=sync).count()
    absent_count = len(absent_names)
    if not owned_total or absent_count <= SCOPE_SHRINK_REFUSAL_FLOOR:
        return
    ratio = absent_count / owned_total
    if ratio <= SCOPE_SHRINK_REFUSAL_RATIO:
        return
    raise ScopeShrinkGuardError(
        f"Refusing to delete uncovered devices: {absent_count} of "
        f"{owned_total} devices this sync created ({ratio:.0%}) are absent "
        "from Forward. Above "
        f"{SCOPE_SHRINK_REFUSAL_RATIO:.0%} a collection or query fault is "
        "likelier than that many devices being decommissioned. Confirm in "
        "Forward that they are genuinely gone, then re-run with the "
        "scope-shrink override."
    )


def prune_uncovered_devices(
    sync,
    *,
    report=None,
    allow_scope_shrink=False,
    include_quarantined=False,
    restrict_to_device_pks=None,
) -> dict:
    """Delete devices this sync created that Forward no longer reports at all.

    What it deletes, exactly: a device is eligible only when all four hold.

      * this sync holds a `ForwardDeviceIdentity` for it - it created the
        device, so removing it undoes its own work rather than someone else's;
      * the current scope result does not cover it;
      * the census says `absent`, meaning Forward did not return the device at
        all. A device Forward still reports, but which carries no include tag,
        is a scoping decision and is never touched here. Nor is one excluded by
        the vendor guard;
      * its absence has outlasted the quarantine.

    What it does not bypass: the empty-scope refusal, the scope-shrink refusal,
    and the quarantine are the same gates the orphan prune passes, for the same
    reasons. It also cannot reach a device this sync never created - the
    unclaimed half of "untagged" is not ours to delete and has no code path
    here at all.

    Why it exists: the orphan prune acts on devices claimed by the run that
    produced the current result. A device created by an earlier run and dropped
    from scope since is not in that set, so at a customer whose orphan count
    reads zero the prune is a no-op while the uncovered count climbs. That was
    the whole of the reported problem: the count was diagnosable and not
    actionable.

    The quarantine is what makes this safe rather than merely gated. Disabling
    a device in Forward removes it from the API exactly as decommissioning does
    - confirmed against a live customer snapshot, from both the NQE result and
    the REST inventory - so `absent` cannot distinguish the two. Absence
    sustained across the configured runs and hours can: a maintenance window
    does not span them.
    """
    if report is None:
        report = compute_scope_reconciliation(sync)

    _require_nonempty_forward_scope(report, operation="prune uncovered devices")
    _require_survivable_scope_shrink(report, allow_scope_shrink=allow_scope_shrink)

    required_runs, required_hours = absence_quarantine_thresholds(sync)
    owned_names = report.get("_owned_untagged") or set()
    if not owned_names:
        return _prune_result(
            required_runs=required_runs,
            required_hours=required_hours,
        )

    kinds = report.get("_absence_kinds")
    if kinds is None:
        # The census could not run, so nothing is known about WHY these devices
        # are uncovered. Refusing beats deleting on an assumption.
        raise ScopeCensusUnavailableError(
            "Refusing to delete uncovered devices: the Forward census that "
            "says whether each device is absent or merely untagged did not "
            "run, so eligibility cannot be established."
        )

    absent_names = {name for name in owned_names if kinds.get(name) == "absent"}
    _require_survivable_uncovered_shrink(
        sync, absent_names, allow_scope_shrink=allow_scope_shrink
    )

    # Resolve to the pks the report already established, then keep only those
    # whose name is absent. Matching on the name at delete time would re-resolve
    # a value NetBox does not hold unique.
    owned_pks = set(report.get("_owned_untagged_pks") or ())
    absent_pks = [
        device_id
        for device_id, name in Device.objects.filter(pk__in=owned_pks).values_list(
            "pk", "name"
        )
        if (name or "").strip() in absent_names
    ]
    # A caller may narrow this to named devices - the device page acts on one.
    # It can only ever INTERSECT what the whole-set prune would already delete:
    # every gate above has run over the full picture first, and a pk the
    # unrestricted prune would not touch is refused here with the reason, never
    # widened to. A second deletion path is how a delete acquires a guard the
    # other one lacks, so there is only this one.
    #
    # Classified in two halves, because the reason has to survive every exit -
    # including the one directly below, for a sync with no absent device at
    # all. Ownership and absence are decidable now; "held" needs the quarantine
    # partition, which only exists when something is absent. Classifying after
    # any exit meant a device Forward still reports was refused with NO reason
    # recorded, indistinguishable from a request that was never dispatched.
    restricted_refusals = {"not_owned": [], "not_absent": [], "held": []}
    wanted = (
        {int(pk) for pk in restrict_to_device_pks}
        if restrict_to_device_pks is not None
        else None
    )
    if wanted is not None:
        absent_set = set(absent_pks)
        for pk in sorted(wanted):
            if pk not in owned_pks:
                restricted_refusals["not_owned"].append(pk)
            elif pk not in absent_set:
                restricted_refusals["not_absent"].append(pk)

    if not absent_pks:
        return _prune_result(
            out_of_scope_sample=sorted(absent_names)[:SAMPLE_LIMIT],
            restricted_refusals=restricted_refusals,
            required_runs=required_runs,
            required_hours=required_hours,
        )

    partition = partition_quarantined_orphans(sync, absent_pks)
    held_device_count = len(partition["held_pks"])
    eligible_pks = absent_pks if include_quarantined else partition["eligible_pks"]
    quarantine_counts = {
        "required_runs": partition["required_runs"],
        "required_hours": partition["required_hours"],
        "held_device_count": 0 if include_quarantined else held_device_count,
        "overridden_device_count": held_device_count if include_quarantined else 0,
    }
    if wanted is not None:
        held = set(partition["held_pks"])
        for pk in sorted(wanted):
            if (
                pk in owned_pks
                and pk in set(absent_pks)
                and pk in held
                and not include_quarantined
            ):
                restricted_refusals["held"].append(pk)
        eligible_pks = [pk for pk in eligible_pks if pk in wanted]
    if not eligible_pks:
        return _prune_result(
            out_of_scope_sample=sorted(absent_names)[:SAMPLE_LIMIT],
            restricted_refusals=restricted_refusals,
            **quarantine_counts,
        )

    (
        pruned_device_ids,
        deleted_total,
        protected_tally,
        ownership_blocked_ids,
    ) = _delete_prunable_devices(sync, eligible_pks)

    result = _prune_result(
        pruned_device_count=len(pruned_device_ids),
        pruned_object_count=deleted_total,
        out_of_scope_sample=sorted(absent_names)[:SAMPLE_LIMIT],
        restricted_refusals=restricted_refusals,
        ownership_blocked_device_count=len(ownership_blocked_ids),
        protected_device_count=len(eligible_pks)
        - len(pruned_device_ids)
        - len(ownership_blocked_ids),
        **quarantine_counts,
    )
    # `_delete_prunable_devices` computes this tally for both prunes and the
    # orphan half has reported it since 2.5.5; this half discarded it, so a
    # device another plugin's rows refuse - ten netbox_routing BGP peers on its
    # addresses, in the case that found this - counted as protected and named
    # nothing. The operator needs the model to act on it.
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


def latest_scope_report_job(sync):
    """The newest completed job carrying a scope report, from either producer.

    Two jobs compute one: the Refresh button's `scope reconciliation` job,
    which stores the report at the top level, and the post-sync
    `reconcile device scope tags (auto)` job, which already computes the same
    report for its own tagging and stores it under `scope_reconciliation`.

    Only the first used to be read, so the panel showed whatever Refresh last
    produced and never moved on its own - a customer read the same counts for
    days across several syncs and concluded the sync was creating uncovered
    devices, when nothing had recomputed the page.

    Ordered by `-completed`, not `-created`: a Refresh started before a sync
    but finishing after it is the newer answer.

    Lives here rather than in the view layer so the device-page panel can read
    a stored report without importing views.
    """
    from core.choices import JobStatusChoices
    from core.models import Job
    from django.contrib.contenttypes.models import ContentType
    from django.db.models import Q

    from ..models import ForwardSync

    return (
        Job.objects.filter(
            Q(name__icontains="scope reconciliation")
            | Q(
                name__icontains="reconcile device scope tags",
                data__has_key="scope_reconciliation",
            ),
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=sync.pk,
            status=JobStatusChoices.STATUS_COMPLETED,
        )
        .order_by("-completed", "-pk")
        .first()
    )


def stored_scope_report(job):
    """``(payload, generated_at, error)`` for a stored reconciliation job."""
    if job is None or not isinstance(job.data, dict) or not job.data:
        return {}, None, ""
    if job.data.get("error"):
        return {}, job.completed, str(job.data.get("error"))
    # The tag job nests the report; the Refresh job stores it flat. Unwrap so
    # every caller sees one shape regardless of which job ran last.
    nested = job.data.get("scope_reconciliation")
    if isinstance(nested, dict) and nested:
        return nested, job.completed, ""
    return job.data, job.completed, ""


def latest_scope_report(sync):
    """``(job, payload, generated_at, error)`` for the newest stored report."""
    job = latest_scope_report_job(sync)
    payload, generated_at, error = stored_scope_report(job)
    return job, payload, generated_at, error


def public_scope_report(report) -> dict:
    """The JSON-safe half of a report: every key not prefixed with ``_``.

    The `_`-prefixed entries are working sets (name sets, pk lists) kept for
    the prune and tag paths inside the same call. They are deliberately not
    persisted: some hold device names, which this module does not write to a
    job payload.

    One definition, used by both producers of a stored report, so the panel
    cannot be handed two different shapes depending on which job ran last.
    """
    return json_safe_value(
        {key: value for key, value in report.items() if not key.startswith("_")}
    )


def tag_backfilled_devices(
    sync,
    *,
    report=None,
    snapshot_id=None,
    ingestion_id=None,
) -> dict:
    """Maintain the three maintained device status tags.

    ``forward-backfilled`` marks devices that are tagged-in-scope but were not
    freshly collected in the latest snapshot (kept on purpose).
    ``forward-out-of-scope`` marks NetBox devices that match none of the sync's
    included Forward tags AND were previously claimed by it (the removable
    orphans). ``forward-uncovered`` marks devices this sync created that the
    current result no longer covers, whether or not a claim survives - the
    bucket the panel counts as "created by this sync" under "carry no include
    tag", and the one that grows.

    All three are idempotent — after running, each tag's device set exactly
    matches the current bucket, so operators can filter
    ``/dcim/devices/?tag=forward-backfilled``, ``?tag=forward-out-of-scope`` or
    ``?tag=forward-uncovered``.
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
        # `owned_untagged`, not the whole "carry no include tag" bucket: the
        # unclaimed half is another source's or an operator's, and this sync has
        # no standing to label it. That is enforced rather than trusted - an
        # unclaimed device has no identity row, so it resolves as `missing`.
        uncovered = _apply_maintained_device_tag(
            sync,
            report["_owned_untagged"],
            slug=UNCOVERED_TAG_SLUG,
            name=UNCOVERED_TAG_NAME,
            color=UNCOVERED_TAG_COLOR,
            description=UNCOVERED_TAG_DESCRIPTION,
            claim_type="uncovered",
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
            uncovered_pks=report.get("_owned_untagged_pks") or (),
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
        "uncovered_tag_slug": UNCOVERED_TAG_SLUG,
        "uncovered_tagged": status_materialized["by_claim_type"]
        .get("uncovered", {})
        .get("assignments_added", 0),
        "uncovered_untagged": status_materialized["by_claim_type"]
        .get("uncovered", {})
        .get("assignments_removed", 0),
        "uncovered_claims_added": uncovered["claims_added"],
        "uncovered_claims_released": uncovered["claims_released"],
        "total_uncovered": uncovered["total"],
        # What the uncovered prune would act on before the quarantine is
        # applied. Recorded here so `_job_data_count_trend` can read it from
        # consecutive post-sync jobs, the way it already reads the two totals
        # above.
        "total_owned_prune_candidates": int(
            (report.get("unmanaged") or {}).get("owned_prune_candidates") or 0
        ),
        # The whole report, so the panel has a current one after every sync
        # rather than only after someone presses Refresh. This job already
        # computed it - `tag_backfilled_devices` calls
        # `compute_scope_reconciliation` when handed no report - so storing it
        # costs no additional NQE execution.
        "scope_reconciliation": public_scope_report(report),
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
