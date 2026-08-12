# Find DLM hardware notices Forward no longer emits.
#
# Removals reach NetBox one way only: a Forward NQE diff, which reports what the
# CURRENT query stopped returning. A full run computes removals against the
# promoted contributor baseline (see `full_removal_reconciliation`), which
# collects rows a re-pointed map orphaned - but only from the point that
# baseline was written. Rows orphaned earlier fell out of the baseline lineage
# and are invisible to it.
#
# THE SIGNAL THAT WORKS, and the one this module now uses: does Forward still
# emit a hardware notice for that device type? The hardware-notice query is
# network-complete - it is not tag-scoped and not sharded, so it emits for every
# completed device in the network - which makes its result the authoritative
# full set. A notice on a device type absent from it is stale regardless of when
# it was written.
#
# THE SIGNAL THAT DOES NOT WORK, and what this module used to use: "the device
# type holds no devices". It produced 33 candidates at a customer where only 5
# were stale, for two independent reasons:
#
#   1. A Device Type Library import leaves thousands of legitimately empty
#      device types - 5879 of them there - so emptiness is the ordinary state,
#      not a signal.
#   2. Notices are written network-wide while devices are imported tag-scoped.
#      Hardware that exists in Forward but sits outside the include tags will
#      ALWAYS have zero devices in NetBox. Those notices are correct and
#      permanent, and the old rule flagged every one of them.
#
# Deleting on that rule would have removed 20 notices Forward re-creates on the
# next sync, along with any comments or journal entries on them.
from django.apps import apps

SAMPLE_LIMIT = 25


def _hardware_notice_model():
    if not apps.is_installed("netbox_dlm"):
        return None
    try:
        return apps.get_model("netbox_dlm", "HardwareNotice")
    except LookupError:
        return None


def fetch_emitted_hardware_notice_rows(sync, *, client=None):
    """Run this sync's enabled hardware-notice map and return its rows.

    Executed by query id, not by source. The bundled queries import
    `netbox_utilities` and the alias-aware variant reads a Forward data file;
    neither resolves for an ad-hoc raw query, so re-sending the source would
    fail where the sync succeeds. Going through the map also guarantees this
    audit sees exactly what the sync sees - including which variant is enabled,
    which is the whole subject of the question being asked.
    """
    from django.contrib.contenttypes.models import ContentType

    from ..models import ForwardNQEMap

    app_label, model_name = "netbox_dlm", "hardwarenotice"
    try:
        content_type = ContentType.objects.get(app_label=app_label, model=model_name)
    except ContentType.DoesNotExist:
        return None, "netbox_dlm.hardwarenotice is not installed"
    maps = list(
        ForwardNQEMap.objects.filter(netbox_model=content_type, enabled=True).order_by(
            "weight", "pk"
        )
    )
    if not maps:
        return None, "no enabled hardware-notice map is configured for this sync"

    client = client or sync.source.get_client()
    network_id = sync.get_network_id()
    snapshot_id = sync.resolve_snapshot_id(client)
    rows = []
    for nqe_map in maps:
        if not nqe_map.query_id:
            return None, (
                f"map `{nqe_map.name}` is not bound to a Forward query id, so "
                "this audit cannot reproduce what the sync executes"
            )
        rows.extend(
            client.run_nqe_query(
                query_id=nqe_map.query_id,
                commit_id=nqe_map.commit_id or None,
                network_id=network_id,
                snapshot_id=snapshot_id,
                parameters=dict(nqe_map.parameters or {}),
                fetch_all=True,
            )
            or []
        )
    return rows, ""


def emitted_device_type_slugs(rows):
    """The device-type slugs a hardware-notice query result covers."""
    slugs = set()
    for row in rows or ():
        slug = str((row or {}).get("device_type_slug") or "").strip()
        if slug:
            slugs.add(slug)
    return slugs


def stale_hardware_notices(emitted_slugs, *, sample_limit=SAMPLE_LIMIT):
    """Notices whose device type is absent from Forward's current result.

    `emitted_slugs` must come from a COMPLETE hardware-notice result. Passing a
    partial one - a shard, a failed fetch, an empty result - would make the
    whole table look stale, so callers must establish completeness first and
    this function refuses an empty set outright.

    `sample_limit=None` returns every row rather than a sample. A caller that
    deletes MUST pass None: truncating there would silently act on the first
    page and report the full count.
    """
    HardwareNotice = _hardware_notice_model()
    if HardwareNotice is None:
        return {
            "available": False,
            "reason": "netbox_dlm is not installed",
            "stale_notice_count": 0,
            "stale_notices": [],
            "stale_notice_ids": [],
        }
    if not emitted_slugs:
        # An empty result is indistinguishable from a broken fetch, and acting
        # on it would delete every notice. The same reasoning refuses an empty
        # scope result before an orphan prune.
        return {
            "available": False,
            "reason": (
                "Forward returned no hardware notices, which cannot be "
                "distinguished from a failed fetch; nothing is considered stale"
            ),
            "stale_notice_count": 0,
            "stale_notices": [],
            "stale_notice_ids": [],
        }

    notices = (
        HardwareNotice.objects.exclude(device_type__slug__in=sorted(emitted_slugs))
        .exclude(device_type__isnull=True)
        .select_related("device_type", "device_type__manufacturer")
        .order_by("device_type__manufacturer__name", "device_type__model")
    )
    rows = []
    for notice in notices:
        device_type = notice.device_type
        rows.append(
            {
                "notice_id": notice.pk,
                "device_type_id": device_type.pk,
                "manufacturer": getattr(device_type.manufacturer, "name", ""),
                "model": device_type.model,
                "slug": device_type.slug,
                "end_of_support": str(getattr(notice, "end_of_support", "") or ""),
            }
        )
    return {
        "available": True,
        "reason": "",
        "hardware_notice_count": HardwareNotice.objects.count(),
        "forward_emitted_device_types": len(emitted_slugs),
        "stale_notice_count": len(rows),
        "stale_notices": rows if sample_limit is None else rows[:sample_limit],
        "stale_notice_ids": [row["notice_id"] for row in rows],
    }


def delete_stale_hardware_notices(notice_ids):
    """Delete the notices the audit identified. Device types are left alone."""
    HardwareNotice = _hardware_notice_model()
    if HardwareNotice is None or not notice_ids:
        return {"deleted_notice_count": 0}
    # Device types are deliberately NOT deleted. A Device Type Library import
    # creates them intentionally and an empty one is not evidence of a mistake;
    # the notice is derived data the sync owns, and the device type is not.
    deleted, _ = HardwareNotice.objects.filter(pk__in=list(notice_ids)).delete()
    return {"deleted_notice_count": deleted}
