# One place that says what to do about each cleanup count.
#
# Scope Reconciliation shows a dozen counts across separate cards, and an operator
# reading them cannot tell which are problems, which are expected, which need a
# Forward-side fix and which end in a delete. This turns the stored report into a
# short ordered checklist: what it is, whether anything gets deleted, and the exact
# steps. The plugin never deletes on its own here: every delete is a button a
# person presses after reviewing a list, and is checked again against Forward.
#
# Counts and plain text only. No device names.
# What an item asks of the operator, shown as a badge.
DELETES_AFTER_REVIEW = "deletes after you review"
NO_DELETE_FIX_IN_FORWARD = "no delete: fix in Forward"
NO_DELETE_FIX_IN_NETBOX = "no delete: fix in NetBox"
NO_DELETE_AUTOMATIC = "no action: clears itself"


def _count(value):
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def cleanup_items(
    payload,
    *,
    twin_count=0,
    relabel=None,
    urls=None,
    shared_primary=0,
    context_copies_on=False,
    on_no_interface=0,
    management_interfaces_on=False,
    platform_restorable=0,
    no_primary_ip_unrecorded=0,
):
    """The ordered checklist for one stored scope report.

    ``payload`` is the stored report, ``twin_count`` the unmanaged devices that
    share a name with a managed one, ``relabel`` the `site_relabel_pairs`
    summary (``pair_count``/``held_count``) and ``urls`` maps an item key to the
    page that lists its devices. ``platform_restorable`` is the restorable count of
    the stored platform-restore preview. ``no_primary_ip_unrecorded`` is the devices
    with an address but no primary IP that the management step recorded nothing
    for. Items with nothing to do are left out.
    """
    payload = payload or {}
    unmanaged = payload.get("unmanaged") or {}
    urls = urls or {}
    relabel = relabel or {}
    items = []

    pairs = _count(relabel.get("pair_count")) + _count(relabel.get("held_count"))
    if pairs:
        items.append(
            {
                "key": "site_relabel_duplicates",
                "title": "Duplicate devices left by a site relabel",
                "count": pairs,
                "kind": DELETES_AFTER_REVIEW,
                "what": (
                    "Forward moved a device to another site and NetBox kept both "
                    "the old and the new copy. NetBox holds a name once per site, "
                    "so the sync cannot tell which to update and may hold the "
                    "device."
                ),
                "steps": [
                    "Open the Site-Relabel Duplicates card below.",
                    "For a mergeable pair press Merge site-relabel duplicates: it "
                    "keeps the older device, moves it to the site Forward reports "
                    "and removes the newer copy.",
                ]
                + [
                    f"{row['count']} held ({row['reason'].replace('_', ' ')}): "
                    f"{row['remedy']}"
                    for row in relabel.get("held_by_reason") or ()
                    if row.get("remedy")
                ]
                + (
                    []
                    if relabel.get("held_by_reason")
                    else [
                        "A held group names why it is held. Fix or delete the "
                        "copy that is wrong in NetBox, then refresh the report."
                    ]
                )
                + ["Run the sync again."],
                "url": "",
            }
        )

    twins = _count(twin_count)
    if twins:
        items.append(
            {
                "key": "same_name_more_than_once",
                "title": "Same name as a device this sync manages",
                "count": twins,
                "kind": DELETES_AFTER_REVIEW,
                "what": (
                    "Another copy of a device this sync manages exists at a "
                    "different site, left by another source or an older version. "
                    "The sync updates only the copy it manages; these older "
                    "copies keep stale data."
                ),
                "steps": [
                    "Open the list: each is the extra copy, not the one the sync "
                    "manages.",
                    "Check the managed copy is the one you want to keep.",
                    "Select the extra copies and Delete Selected (the "
                    "confirmation page names anything that blocks it).",
                    "Run the sync again.",
                ],
                "url": urls.get("same_name_more_than_once", ""),
            }
        )

    # The count the list and the prune use: absent from the snapshot AND not in
    # Forward's device configuration. The census count alone is larger when the
    # configuration read failed, and the list would then be empty.
    gone = len(unmanaged.get("unclaimed_absent_device_ids") or ())
    census_gone = _count(
        (unmanaged.get("unclaimed_absence") or {}).get("absent_from_snapshot")
    )
    if (
        not gone
        and census_gone
        and unmanaged.get("unclaimed_configuration_checked") is False
    ):
        items.append(
            {
                "key": "unmanaged_not_in_forward_unverified",
                "title": "Devices no sync manages that look absent from Forward",
                "count": census_gone,
                "kind": NO_DELETE_FIX_IN_FORWARD,
                "what": (
                    "These are missing from Forward's snapshot, but Forward's "
                    "device configuration could not be read, so none can be "
                    "confirmed gone and none are offered for deletion."
                ),
                "steps": [
                    "Refresh the report once Forward is reachable; the list "
                    "returns when the configuration can be read."
                ],
                "url": "",
            }
        )
    if gone:
        items.append(
            {
                "key": "unmanaged_not_in_forward",
                "title": "Devices no sync manages that are not in Forward",
                "count": gone,
                "kind": DELETES_AFTER_REVIEW,
                "what": (
                    "Nothing ever created these through a sync and Forward does "
                    "not return them. Many are old leftovers. Some are hardware "
                    "Forward never collects (patch panels, PDUs, servers), which "
                    "is normal and should stay."
                ),
                "steps": [
                    "Open the list.",
                    "Select only the devices you have confirmed are decommissioned.",
                    "Press Delete (verified gone), read the warning and type the "
                    "count. Each device is checked against Forward again and "
                    "refused unless it is still absent.",
                ],
                "url": urls.get("unmanaged_not_in_forward", ""),
            }
        )

    created_gone = _count(unmanaged.get("owned_prune_candidates"))
    eligible = _count((unmanaged.get("owned_quarantine") or {}).get("prune_eligible"))
    if created_gone:
        items.append(
            {
                "key": "created_by_sync_not_in_forward",
                "title": "Devices this sync created that Forward no longer reports",
                "count": created_gone,
                "kind": DELETES_AFTER_REVIEW,
                "what": (
                    "The sync created these and Forward has not returned them "
                    "for several runs, so they are most likely decommissioned. "
                    "A device that was only disabled in Forward looks the same, "
                    "so each waits out a quarantine before it can be deleted."
                ),
                "steps": ["Open the list to see them."]
                + (
                    [
                        f"{eligible} have waited long enough: press Delete "
                        "eligible now in the Uncovered card."
                    ]
                    if eligible
                    else []
                )
                + ["The rest stay held until the quarantine passes. Nothing to do."],
                "url": urls.get("created_by_sync_not_in_forward", ""),
            }
        )

    orphans = _count(
        (payload.get("out_of_scope_quarantine") or {}).get("prune_eligible")
    )
    if orphans:
        items.append(
            {
                "key": "out_of_scope_orphans",
                "title": "Devices that left this sync's scope",
                "count": orphans,
                "kind": DELETES_AFTER_REVIEW,
                "what": (
                    "These match none of the sync's include tags and Forward no "
                    "longer returns them. They waited out the quarantine."
                ),
                "steps": [
                    "Open the list and check none should still be in scope.",
                    "Press Prune orphans in the Out of scope card. It also "
                    "deletes empty sites outside Forward's location scope; its "
                    "confirmation says so.",
                ],
                "url": urls.get("out_of_scope_orphans", ""),
            }
        )

    untagged = _count((unmanaged.get("owned_absence") or {}).get("present_untagged"))
    # Generic SNMP endpoints this sync created are untagged for a different
    # reason - the source's own setting - so they get their own item, and the
    # "add a tag" advice below is not offered for them.
    generic = sum(
        _count(row.get("count"))
        for row in (unmanaged.get("owned_absence") or {}).get("endpoint_detail") or ()
        if row.get("reason") == "endpoint_generic"
    )
    if generic:
        items.append(
            {
                "key": "generic_endpoints_not_imported",
                "title": "Generic SNMP endpoints this sync created, now uncovered",
                "count": generic,
                "kind": DELETES_AFTER_REVIEW,
                "what": (
                    "This sync created these from Forward's SNMP endpoints. "
                    "Import generic SNMP endpoints is off on the source, so "
                    "Forward still reports them but the sync no longer reads "
                    "them, and it cannot prune them: they stay uncovered until "
                    "you choose."
                ),
                "steps": [
                    "To keep them: edit the source and turn on Import generic "
                    "SNMP endpoints, then run the sync again.",
                    "To remove them: open the list, select them and Delete "
                    "Selected (the device list bulk delete), or delete each by "
                    "hand. This also deletes their interfaces and IP addresses.",
                ],
                "url": urls.get("generic_endpoints_not_imported", ""),
            }
        )
    untagged = max(untagged - generic, 0)
    if untagged:
        items.append(
            {
                "key": "in_forward_not_in_scope",
                "title": "In Forward but outside this sync's include tags",
                "count": untagged,
                "kind": NO_DELETE_FIX_IN_FORWARD,
                "what": (
                    "Forward still has these devices; they just do not carry an "
                    "include tag the sync reads. The plugin never deletes them."
                ),
                "steps": [
                    "To keep them in NetBox: add an include tag to the device "
                    "in Forward. Tags added by hand in NetBox are removed on the "
                    "next run, because the sync owns them.",
                    "To drop them from this sync on purpose: leave them untagged.",
                ],
                "url": urls.get("in_forward_not_in_scope", ""),
            }
        )

    # The count the list shows: devices in NetBox that Forward could not collect.
    backfilled = _count(
        payload.get("netbox_present_backfilled")
        if payload.get("netbox_present_backfilled") is not None
        else payload.get("forward_tagged_backfilled")
    )
    if backfilled:
        reasons = payload.get("backfilled_reason_breakdown") or {}
        top = ", ".join(
            f"{count} {str(name).rstrip(')')}"
            for name, count in sorted(reasons.items(), key=lambda item: -item[1])[:3]
        )
        items.append(
            {
                "key": "forward_could_not_collect",
                "title": "Devices Forward could not collect",
                "count": backfilled,
                "kind": NO_DELETE_FIX_IN_FORWARD,
                "what": (
                    "Forward has these devices but their data collection failed"
                    + (f" ({top})" if top else "")
                    + ". Their addresses, modules and routing read as missing "
                    "until Forward collects them again, which can block a run "
                    "on row-count drops. Nothing is deleted."
                ),
                "steps": [
                    "Fix the cause in Forward: credentials for AUTHENTICATION_FAILED, "
                    "reachability for CONNECTION_TIMEOUT or REFUSED, onboarding for "
                    "INCOMPLETE_SETUP.",
                    "Wait for the next snapshot, then run the sync again.",
                ],
                "url": urls.get("forward_could_not_collect", ""),
            }
        )
    shared = _count(shared_primary)
    if shared:
        items.append(
            {
                "key": "shared_management_address",
                "title": "Devices with no primary IP because they share one address",
                "count": shared,
                "kind": NO_DELETE_FIX_IN_NETBOX,
                "what": (
                    "Their only management address is another device's primary "
                    "IP: usually a firewall context (vsys, vdom) and its "
                    "chassis, or HA peers. NetBox allows one primary-IP owner "
                    "per address, and Forward gives a context no address of its "
                    "own."
                ),
                "steps": (
                    [
                        "Edit the sync and turn on Give firewall contexts their "
                        "own copy of a shared management IP (Execution section; "
                        "needs Fall back to Forward's management IP).",
                        "Run the sync again: each context gets its own Anycast "
                        "copy of the address as its primary IP.",
                    ]
                    if not context_copies_on
                    else [
                        "The per-context copy is on. These still have none: the "
                        "Explained tab names why (usually no interface named "
                        "like the chassis's).",
                        *(
                            []
                            if management_interfaces_on
                            else [
                                "For a context with no interface named like the "
                                "chassis's (a vdom has no mgmt), edit the sync, "
                                "turn on Add a management interface for an "
                                "address no interface carries, and run the sync "
                                "again."
                            ]
                        ),
                    ]
                ),
                "url": urls.get("shared_management_address", ""),
            }
        )
    on_none = _count(on_no_interface)
    if on_none:
        items.append(
            {
                "key": "management_address_on_no_interface",
                "title": "Devices with no primary IP because no interface carries "
                "their management address",
                "count": on_none,
                "kind": NO_DELETE_FIX_IN_NETBOX,
                "what": (
                    "Forward records their management address on none of their "
                    "interfaces: a Palo Alto's management port, a FortiGate "
                    "Forward models with no interfaces. NetBox sets a primary IP "
                    "only from an address on one of the device's interfaces."
                ),
                "steps": (
                    [
                        "Edit the sync and turn on Add a management interface for "
                        "an address no interface carries (Execution section; "
                        "needs Fall back to Forward's management IP).",
                        "Run the sync again: each gets a virtual mgmt interface "
                        "tagged forward-management-interface carrying the address "
                        "as its primary IP.",
                    ]
                    if not management_interfaces_on
                    else [
                        "The management interface is on. These still have none: "
                        "the Explained tab names why (an interface named mgmt "
                        "already exists, or NetBox holds the address elsewhere)."
                    ]
                ),
                "url": urls.get("shared_management_address", ""),
            }
        )
    unrecorded = _count(no_primary_ip_unrecorded)
    if unrecorded:
        items.append(
            {
                "key": "primary_ip_none_recorded",
                "title": "Devices with an address, no primary IP and no recorded reason",
                "count": unrecorded,
                "kind": NO_DELETE_FIX_IN_FORWARD,
                "what": (
                    "These devices have an address in NetBox, but the "
                    "management-address step recorded nothing for them, so the "
                    "sync has no interface to take the primary IP from."
                ),
                "steps": [
                    "In Forward, add a Mgmt_<interface> device tag to each, "
                    "naming the interface to use (for example Mgmt_eth0), then "
                    "run the sync again.",
                    "Or set the primary IP by hand on the device in NetBox.",
                ],
                "url": urls.get("shared_management_address", ""),
            }
        )
    blanked = _count(platform_restorable)
    if blanked:
        items.append(
            {
                "key": "platform_blanked_by_sync",
                "title": "Devices a sync run blanked the platform on",
                "count": blanked,
                "kind": NO_DELETE_FIX_IN_NETBOX,
                "what": (
                    "Before 3.0.14 a run in which the include tag matched no "
                    "device cleared the platform on every device that had one. "
                    "A device outside the sync's scope is never revisited, so "
                    "it stays blank. NetBox's change log shows which devices "
                    "this sync's own runs blanked; nothing is deleted."
                ),
                "steps": [
                    "Open the Platforms a Sync Run Blanked card below.",
                    "Press Preview to recount from NetBox's change log.",
                    "Press Restore: it puts back only what the preview counted, "
                    "and each edit is change-logged to you.",
                ],
                "url": "",
            }
        )
    return items
