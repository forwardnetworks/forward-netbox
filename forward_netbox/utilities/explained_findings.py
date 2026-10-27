# Things that look wrong but may not be, each with a reason and what to do.
#
# A sync leaves behind counts an operator cannot read at a glance: devices with
# no primary IP, devices marked uncovered, addresses assigned to nothing. A
# customer asked what each was before anyone could say whether it mattered. Every
# row here carries a reason in plain words, whether it is fixable, and the next
# action, so the question is answered where the count appears. Counts only.
from .primary_ip_reasons import BY_DESIGN
from .primary_ip_reasons import NEEDS_A_PERSON


def _row(
    finding,
    count,
    reason,
    fixable,
    action,
    url="",
    url_label="",
    post_url="",
    post_label="",
):
    # `url` is where to go to do it: a list already filtered to the rows, so the
    # operator is one click from fixing them rather than told where to look.
    return {
        "finding": finding,
        "count": count,
        "reason": reason,
        "fixable": fixable,
        "action": action,
        "url": url,
        "url_label": url_label,
        # A button that does it, for what a click can fix (queues a job).
        "post_url": post_url,
        "post_label": post_label,
    }


def _link(route, query=None, **kwargs):
    from urllib.parse import urlencode

    from django.urls import reverse

    base = reverse(route, kwargs=kwargs or None)
    return f"{base}?{urlencode(query)}" if query else base


def _primary_ip_rows(sync):
    from dcim.models import Device

    from ..models import ForwardDeviceIdentity
    from .primary_ip import primary_ip_from_management_ip_enabled
    from .primary_ip import primary_ip_from_mgmt_tag_enabled
    from .primary_ip_reasons import primary_ip_reasons

    settings_on = primary_ip_from_mgmt_tag_enabled(
        sync
    ) and primary_ip_from_management_ip_enabled(sync)
    bound = ForwardDeviceIdentity.objects.filter(sync=sync).values_list(
        "device_id", flat=True
    )
    missing = Device.objects.filter(
        pk__in=bound, primary_ip4__isnull=True, primary_ip6__isnull=True
    )
    rows = []
    from .primary_ip import primary_ip_log_facts

    fallback_reasons, outcomes = primary_ip_log_facts(sync)
    for item in primary_ip_reasons(
        missing,
        settings_on=settings_on,
        fallback_reasons=fallback_reasons,
        outcomes=outcomes,
    ):
        rows.append(
            _row(
                "Devices without a primary IP (this sync)",
                item["count"],
                item["why"],
                item["fixable"],
                item["what_to_do"],
                url=_link(
                    "plugins:forward_netbox:forwardsync_explained",
                    {"reason": item["reason"]},
                    pk=sync.pk,
                ),
                url_label="List these devices",
            )
        )
    return rows


# Uncovered devices are split by what the sync's last scope report says about
# them. The report separates a device Forward no longer returns ("absent") from one
# Forward still returns under different tags or as a custom-command source, and
# names the specific rule. A device the report does not cover (uncovered by another
# sync, or no report yet) is its own group, so none is described by a guess.
UNCOVERED_REASONS = {
    "uncovered_absent": (
        "Forward no longer returns the device (scope report): it is disabled, was "
        "not collected, or is not a configured device. Its interfaces and addresses "
        "are from the last run it appeared in and will not update, so anything "
        "Forward reports on it now never arrives.",
        NEEDS_A_PERSON,
        "In Forward, re-collect the device or restore it to the network. If it is "
        "really gone, prune it in Scope Reconciliation (devices missing for several "
        "runs are eligible).",
    ),
    "uncovered_in_forward": (
        "Forward still returns the device, but not under this sync's device-tag "
        "scope (different tags, or it is a custom-command source), so the sync no "
        "longer updates it.",
        NEEDS_A_PERSON,
        "Put the Forward tag the sync selects on back on the device, or change the "
        "sync's tag scope. If it should not sync, prune it in Scope Reconciliation.",
    ),
    "uncovered_unclassified": (
        "This sync owns the device, but its last scope report could not tell whether "
        "Forward still returns it (the check failed, or the report is older than "
        "that check).",
        NEEDS_A_PERSON,
        "Run Scope Reconciliation on this sync again to classify it.",
    ),
    "uncovered_unrecorded": (
        "This sync's last scope report does not cover the device: another sync owns "
        "it, the report is older than the tag, or no scope report has run (or its "
        "last run failed).",
        NEEDS_A_PERSON,
        "Run Scope Reconciliation on this sync to classify it.",
    ),
}


def rule_label(code):
    """The words for a scope report's rule code, or the code if it has none."""
    from .scope_reconciliation import ABSENT_DETAILS
    from .scope_reconciliation import ENDPOINT_ABSENCE_DETAILS

    return ABSENT_DETAILS.get(code) or ENDPOINT_ABSENCE_DETAILS.get(code) or code


def uncovered_devices(sync):
    """``({key: [device pk]}, {device pk: detail}, {device pk: absence row})``."""
    from dcim.models import Device

    from ..models import ForwardDeviceAbsence
    from .scope_reconciliation import latest_scope_report
    from .scope_reconciliation import UNCOVERED_TAG_SLUG

    tagged = set(
        Device.objects.filter(tags__slug=UNCOVERED_TAG_SLUG).values_list(
            "pk", flat=True
        )
    )
    _job, payload, _at, _error = latest_scope_report(sync)
    unmanaged = (payload or {}).get("unmanaged") or {}
    owned = {int(pk) for pk in unmanaged.get("owned_untagged_device_ids") or ()}
    absent = {int(pk) for pk in unmanaged.get("owned_absent_device_ids") or ()}
    details = {
        int(pk): rule_label(code)
        for pk, code in (unmanaged.get("owned_detail_by_id") or {}).items()
        if code
    }
    # Absent devices are known only if the report could classify them. When that
    # check failed, or the report predates it, "absent" is empty because nothing
    # was classified, not because every device is still in Forward.
    classified = (
        "owned_absent_device_ids" in unmanaged
        and (unmanaged.get("owned_absence") or {}).get("available") is not False
    )
    absences = {
        row.device_id: row
        for row in ForwardDeviceAbsence.objects.filter(sync=sync, device_id__in=tagged)
    }
    return (
        {
            "uncovered_absent": sorted(tagged & absent),
            "uncovered_in_forward": (
                sorted((tagged & owned) - absent) if classified else []
            ),
            "uncovered_unclassified": (
                [] if classified else sorted((tagged & owned) - absent)
            ),
            "uncovered_unrecorded": sorted(tagged - owned),
        },
        details,
        absences,
    )


def _uncovered_rows(sync):
    groups, _details, _absences = uncovered_devices(sync)
    rows = []
    for key, (why, fixable, action) in UNCOVERED_REASONS.items():
        count = len(groups[key])
        if not count:
            continue
        rows.append(
            _row(
                "Uncovered devices (all of NetBox)",
                count,
                why,
                fixable,
                action,
                url=_link(
                    "plugins:forward_netbox:forwardsync_explained",
                    {"reason": key},
                    pk=sync.pk,
                ),
                url_label="List these devices",
            )
        )
    return rows


def _unassigned_ip_row(sync):
    from .bgp_neighbour_ips import bgp_neighbour_ip_addresses
    from .bgp_neighbour_ips import routing_installed
    from .bgp_neighbour_ips import unassigned_ip_addresses

    finding = "IP addresses assigned to nothing (all of NetBox)"
    unassigned = unassigned_ip_addresses()
    total = unassigned.count()
    if not total:
        return []
    needed = (
        bgp_neighbour_ip_addresses(unassigned).count() if routing_installed() else 0
    )
    rows = []
    if needed:
        rows.append(
            _row(
                finding,
                needed,
                "A BGP peer's neighbour is a remote device, so the sync holds its "
                "address with no interface. Expected, not an orphan.",
                BY_DESIGN,
                "Tag them so they can be filtered out of IPAM. This queues a job and "
                "can be run again any time.",
                url=_link("ipam:ipaddress_list", {"tag": "forward-bgp-neighbour"}),
                url_label="List the tagged addresses",
                post_url=_link(
                    "plugins:forward_netbox:forwardsync_tag_bgp_neighbour_ips",
                    pk=sync.pk,
                ),
                post_label="Tag these addresses",
            )
        )
    if total - needed > 0:
        rows.append(
            _row(
                finding,
                total - needed,
                "No interface and no BGP peer uses the address, so it may be a "
                "real orphan (or one added by hand).",
                NEEDS_A_PERSON,
                "Review them in IPAM and delete the ones nothing needs.",
                url=_link("ipam:ipaddress_list", {"assigned_to_interface": "false"}),
                url_label="List addresses with no interface",
            )
        )
    return rows


# The device and module types that look sync-created (no component templates): what
# each group means and what to do. See `sync_created_types` for how they are found.
TYPE_GROUPS = {
    "equivalent": (
        "{noun} that look sync-created, with an equivalent in NetBox (this sync)",
        "The type has no component templates, which is how a type the sync created "
        "looks, and NetBox holds a type that does have them under a name that "
        "matches (usually from the device-type library). Forward's model name "
        "differs from that type, which is how a duplicate is made; the sync now "
        "reuses an existing type, but ones made earlier stay.",
        NEEDS_A_PERSON,
        "In NetBox, select the {users} of each type, bulk edit their type to the "
        "equivalent, and delete the old type straight away. Until it is deleted the "
        "next sync matches it exactly and moves the {users} back, so do this between "
        "syncs and not while a sync branch is waiting to merge.",
    ),
    "unmatched": (
        "{noun} that look sync-created, with no equivalent (this sync)",
        "The type has no component templates and nothing in NetBox matches it, so "
        "it is probably not in your device-type library (virtual or endpoint "
        "models, or new hardware).",
        BY_DESIGN,
        "Nothing to fix. Add the type to your library if you want its templates.",
    ),
    "unused": (
        "{noun} nothing uses (all of NetBox)",
        "No {user} uses them and they have no component templates: left behind by "
        "earlier runs, or made by hand.",
        NEEDS_A_PERSON,
        "Review the list and delete the ones you do not need, with NetBox's bulk "
        "delete.",
    ),
}
# kind -> (plural noun for the types, what uses them (plural), what uses one (singular))
TYPE_KINDS = {
    "device": ("Device types", "devices", "device"),
    "module": ("Module types", "modules", "module"),
}


def type_group_text(kind, group):
    """``(finding, why, fixable, action)`` for a kind and group of types."""
    noun, users, user = TYPE_KINDS[kind]
    finding, why, fixable, action = TYPE_GROUPS[group]
    return (
        finding.format(noun=noun),
        why.format(user=user) if "{user}" in why else why,
        fixable,
        action.format(users=users),
    )


def _type_rows(sync):
    from .sync_created_types import sync_created_types

    rows = []
    for kind in TYPE_KINDS:
        groups = sync_created_types(kind, sync)
        for group in TYPE_GROUPS:
            count = len(groups[group])
            if not count:
                continue
            finding, why, fixable, action = type_group_text(kind, group)
            if group == "equivalent":
                users = sum(entry[2] for entry in groups[group])
                noun = TYPE_KINDS[kind][2]
                why = (
                    f"{why} ({users} {noun} uses them.)"
                    if users == 1
                    else f"{why} ({users} {TYPE_KINDS[kind][1]} use them.)"
                )
            rows.append(
                _row(
                    finding,
                    count,
                    why,
                    fixable,
                    action,
                    url=_link(
                        "plugins:forward_netbox:forwardsync_types",
                        {"kind": f"{kind}_{group}"},
                        pk=sync.pk,
                    ),
                    url_label="List these types",
                )
            )
    return rows


def _issue_rows(sync):
    """The latest ingestion's skipped rows, grouped by why, each with its fix."""
    from ..models import ForwardIngestionIssue
    from ..views import _ingestion_issue_summary
    from .skip_reason_catalogue import explain_skip_reason

    ingestion = getattr(sync, "last_ingestion", None)
    if ingestion is None:
        return []
    summary = _ingestion_issue_summary(
        ForwardIngestionIssue.objects.filter(ingestion=ingestion)
    )
    rows = []
    for item in summary.get("by_skip_reason") or []:
        why, fixable, action = explain_skip_reason(item["reason"])
        rows.append(
            _row(
                f"Skipped rows in {item['model']} (latest ingestion)",
                item["count"],
                why,
                fixable,
                action,
                url=_link(
                    "plugins:forward_netbox:forwardingestion_issues",
                    {"model": item["model"]},
                    pk=ingestion.pk,
                ),
                url_label="List the issues",
            )
        )
    return rows


def explained_findings(sync):
    """Every explained finding for a sync; empty when there is nothing to explain."""
    rows = _primary_ip_rows(sync)
    rows.extend(_issue_rows(sync))
    rows.extend(_type_rows(sync))
    rows.extend(_uncovered_rows(sync))
    rows.extend(_unassigned_ip_row(sync))
    return rows
