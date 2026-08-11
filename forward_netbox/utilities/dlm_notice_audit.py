# Find DLM hardware notices left behind by a query the sync no longer runs.
#
# Removals reach NetBox one way only: a Forward NQE diff, which reports what the
# CURRENT query stopped returning. A full run computes no removals at all
# (`delete_rows` is empty outside device-tag scope pruning). So every row a map
# wrote before it was re-pointed at a different query is orphaned permanently -
# nothing ever revisits it.
#
# Switching the device-type maps to their alias-aware variants does exactly
# that. The base query emits Forward's model string (`N9K-C93180YC-FX`); the
# alias variant emits the NetBox Device Type Library name for the same hardware
# (`Nexus 93180YC-FX`). Both device types then exist, each carrying a hardware
# notice with identical dates, and the list shows what looks like duplicates. It
# is not a duplicate write - it is one row the sync has forgotten it owns.
#
# The rule here is deliberately conservative and locally computable: a notice
# whose device type has NO devices is not describing any hardware in the
# inventory. It needs no Forward call and no alias data to decide, and a notice
# is derived data - if it is still relevant the next sync writes it back.
from dcim.models import Device
from dcim.models import DeviceType
from django.apps import apps
from django.db.models import Count

SAMPLE_LIMIT = 25


def _hardware_notice_model():
    if not apps.is_installed("netbox_dlm"):
        return None
    try:
        return apps.get_model("netbox_dlm", "HardwareNotice")
    except LookupError:
        return None


def audit_stale_hardware_notices(*, sample_limit=SAMPLE_LIMIT):
    """Report hardware notices attached to device types that hold no devices."""
    HardwareNotice = _hardware_notice_model()
    if HardwareNotice is None:
        return {
            "available": False,
            "reason": "netbox_dlm is not installed",
            "stale_notice_count": 0,
            "stale_notices": [],
        }

    empty_type_ids = set(
        DeviceType.objects.annotate(_devices=Count("instances"))
        .filter(_devices=0)
        .values_list("pk", flat=True)
    )
    notices = (
        HardwareNotice.objects.filter(device_type_id__in=empty_type_ids)
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
    total_notices = HardwareNotice.objects.count()
    return {
        "available": True,
        "reason": "",
        "hardware_notice_count": total_notices,
        "device_types_without_devices": len(empty_type_ids),
        "stale_notice_count": len(rows),
        "stale_notices": rows[:sample_limit],
        "stale_notice_ids": [row["notice_id"] for row in rows],
    }


def delete_stale_hardware_notices(notice_ids):
    """Delete the notices the audit identified. Device types are left alone."""
    HardwareNotice = _hardware_notice_model()
    if HardwareNotice is None or not notice_ids:
        return {"deleted_notice_count": 0}
    # Device types are deliberately NOT deleted here. A Device Type Library
    # import creates them intentionally and an empty one is not evidence of a
    # mistake; the notice is derived data the sync owns, and the device type
    # is not.
    deleted, _ = HardwareNotice.objects.filter(pk__in=list(notice_ids)).delete()
    return {"deleted_notice_count": deleted}


def device_count_for_type(device_type_id):
    """Small helper kept public for the audit command's confirmation output."""
    return Device.objects.filter(device_type_id=device_type_id).count()
