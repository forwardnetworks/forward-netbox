# How the device numbers fit together.
#
# The Health page and the drift table each show a device count, and they count
# different things: the devices Forward returns inside the sync's tag scope, the
# devices NetBox holds, the ones that carry no include tag. Nothing said what each
# counted, so a customer compared 3,862 with their NetBox total, Forward's whole
# network and a primary-IP count and saw a discrepancy where there was only a
# difference of denominator. This reads the stored scope report (no Forward call)
# and lays the numbers out, each with what it counts, in the labels the Scope
# Reconciliation page already uses.
#
# Only an identity the report guarantees is computed: the devices carrying no include
# tag are split into the ones this sync created and the ones it never owned, and the
# NetBox devices matched to a tagged Forward device are the named devices minus the
# ones carrying no include tag. Everything else is a count the report states,
# described and never added up, because the report counts some things by device and
# some by name and the sets overlap in ways a sum would hide.
from django.urls import reverse

OLD_AFTER_DAYS = 7

# The report keys this reads. A test checks each is still produced, so a renamed key
# cannot turn into a silent zero.
KEYS_READ = (
    "include_tags",
    "exclude_tags",
    "netbox_device_count",
    "netbox_named_device_total",
    "forward_in_scope_completed",
    "forward_in_scope_endpoints",
    "forward_tagged_backfilled",
    "forward_missing_in_netbox",
    "netbox_out_of_scope",
    "unmanaged",
    "untagged_total",
    "owned_untagged",
    "unclaimed",
)


def _step(label, count, meaning, *, group, indent=0):
    return {
        "group": group,
        "label": label,
        "count": count,
        "meaning": meaning,
        "indent": indent,
    }


def _int(value):
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def reconciliation_strip(sync):
    """The device-number flow for a sync, or why it cannot be shown."""
    from django.utils import timezone

    from .scope_reconciliation import latest_scope_report

    url = reverse(
        "plugins:forward_netbox:forwardsync_scope_reconciliation",
        kwargs={"pk": sync.pk},
    )
    _job, payload, generated_at, error = latest_scope_report(sync)
    if not payload:
        return {
            "available": False,
            "url": url,
            "message": (
                "No scope report has been made yet, so the device numbers cannot be "
                "laid out. Run Scope Reconciliation to produce one."
                if not error
                else "The last Scope Reconciliation run did not produce a report, so "
                "the device numbers cannot be laid out. Run it again."
            ),
        }
    unmanaged = payload.get("unmanaged") or {}
    completed = _int(payload.get("forward_in_scope_completed"))
    endpoints = _int(payload.get("forward_in_scope_endpoints"))
    missing = _int(payload.get("forward_missing_in_netbox"))
    backfilled = _int(payload.get("forward_tagged_backfilled"))
    names = _int(payload.get("netbox_device_count"))
    untagged = _int(unmanaged.get("untagged_total"))
    owned = _int(unmanaged.get("owned_untagged"))
    unclaimed = _int(unmanaged.get("unclaimed"))
    orphans = _int(payload.get("netbox_out_of_scope"))
    named_total = payload.get("netbox_named_device_total")
    forward = [
        _step(
            "In scope (collected)",
            completed,
            "Devices Forward collected that match the sync's tags. Devices without "
            "them are not fetched, which is why this is smaller than your whole "
            "Forward network. For the same snapshot, with the SNMP endpoints below "
            "added, it should be close to the Forward rows figure for devices in the "
            "drift table, which counts rows.",
            group="Forward",
        )
    ]
    if endpoints:
        forward.append(
            _step(
                "In scope (SNMP endpoints)",
                endpoints,
                "SNMP endpoints the sync also imports as devices, found under the "
                "same tags.",
                group="Forward",
            )
        )
    forward += [
        _step(
            "In scope, missing from NetBox",
            missing,
            "Of the collected devices and endpoints above, the ones NetBox has no "
            "device of that name for. The next sync creates them unless that row is "
            "skipped or fails.",
            group="Forward",
            indent=1,
        ),
        _step(
            "Tagged but backfilled",
            backfilled,
            "A separate group: Forward lists these under the sync's tags but has no "
            "completed collection for them, so they are not fetched and not in the "
            "numbers above.",
            group="Forward",
        ),
    ]
    netbox = []
    if named_total is not None:
        total = _int(named_total)
        matched = max(total - untagged, 0)
        netbox += [
            _step(
                "NetBox devices (with a name)",
                total,
                "Every device NetBox holds that has a name; devices with no name are not "
                "part of these numbers. This counts devices, while the Scope "
                "Reconciliation page's NetBox devices counts distinct names, so it "
                "is lower there when two devices share a name.",
                group="NetBox",
            ),
            _step(
                "matched to a tagged Forward device",
                matched,
                "The named devices minus the ones that carry no include tag: they "
                "match a device Forward lists under the sync's tags. Forward may not "
                "have collected every one of them; see the backfilled line above.",
                group="NetBox",
                indent=1,
            ),
        ]
    else:
        netbox.append(
            _step(
                "NetBox devices",
                names,
                "Distinct device names in NetBox. This report predates the device "
                "total, so the matched count is not shown; run Scope Reconciliation "
                "again for it.",
                group="NetBox",
            )
        )
    netbox += [
        _step(
            "Carry no include tag",
            untagged,
            "Named devices not matched to a tagged Forward device now: the two "
            "groups below, which add up to this.",
            group="NetBox",
            indent=1,
        ),
        _step(
            "created by this sync",
            owned,
            "This sync made them (it holds a record of each) but Forward does not list "
            "them under its tags now: they left scope, lost the tag, were disabled "
            "or are gone. These are the uncovered devices. Scope Reconciliation "
            "shows what happens to them.",
            group="NetBox",
            indent=2,
        ),
        _step(
            "of those, also listed as out-of-scope orphans",
            orphans,
            "Devices this sync managed before whose name is not in Forward's tagged "
            "set now, which Scope Reconciliation lists as out-of-scope (orphans). "
            "After a sync's tag step this normally matches the count above; it can "
            "differ for devices that became uncovered since.",
            group="NetBox",
            indent=3,
        ),
        _step(
            "not claimed by this sync",
            unclaimed,
            "Another source, an operator or a leftover created these. The sync "
            "neither updates nor removes them.",
            group="NetBox",
            indent=2,
        ),
    ]
    as_of = generated_at.isoformat() if hasattr(generated_at, "isoformat") else None
    as_of_date = ""
    age_days = None
    if hasattr(generated_at, "isoformat"):
        try:
            age_days = (timezone.now() - generated_at).days
            as_of_date = timezone.localtime(generated_at).strftime("%Y-%m-%d")
        except (TypeError, ValueError):
            pass
    return {
        "available": True,
        "url": url,
        "as_of": as_of,
        "as_of_date": as_of_date,
        "age_days": age_days,
        "old": bool(age_days is not None and age_days >= OLD_AFTER_DAYS),
        # Kept out of the step text so the descriptions carry no tag names.
        "include_tags": list(payload.get("include_tags") or []),
        "exclude_tags": list(payload.get("exclude_tags") or []),
        "steps": forward + netbox,
    }
