# Forward NetBox plugin template content hooks.
from netbox.plugins import PluginTemplateExtension


class ForwardDeviceAnalysisPanel(PluginTemplateExtension):
    """Read-only Forward Analysis panel on the device detail page.

    Renders the most recently refreshed ForwardDeviceAnalysis row for the device
    (see the Refresh device analysis action on the sync). No live Forward call.
    """

    models = ["dcim.device"]

    def right_page(self):
        device = self.context["object"]
        from forward_netbox.models import ForwardDeviceAnalysis

        analysis = (
            ForwardDeviceAnalysis.objects.filter(device=device)
            .order_by("-last_updated")
            .first()
        )
        if analysis is None:
            return ""
        return self.render(
            "forward_netbox/inc/device_analysis_panel.html",
            extra_context={"analysis": analysis},
        )


def _uncovered_prune_offer(device, identities, foreign_blockers):
    """Whether this device can be removed from its own page, and how.

    Offered only when the whole-set prune would already delete it: exactly one
    owning sync, the device in that sync's latest report as owned-and-absent,
    and no blocker belonging to another plugin. The view narrows that prune
    rather than running its own deletion, so this is a rendering decision, not
    a second gate - but offering a control that the prune will refuse is its
    own kind of lie, so the conditions are checked here too.

    Two owning syncs offer nothing: "remove via which sync" has no answer a
    button can imply, and picking one silently would delete another sync's
    evidence.
    """
    from django.urls import reverse

    from .utilities.scope_reconciliation import absence_quarantine_thresholds
    from .utilities.scope_reconciliation import latest_scope_report
    from .utilities.scope_reconciliation import partition_quarantined_orphans

    offer = {
        "offered": False,
        "held": False,
        "still_reported": False,
        "url": "",
        "sync_name": "",
        "report_at": None,
        "required_runs": None,
        "required_hours": None,
        "endpoint_detail": "",
        "endpoint_detail_label": "",
        "absent_detail": "",
        "absent_detail_label": "",
    }
    sync_ids = {row.sync_id for row in identities}
    if len(sync_ids) != 1:
        return offer
    sync = identities[0].sync
    _job, payload, generated_at, _error = latest_scope_report(sync)
    unmanaged = (payload or {}).get("unmanaged") or {}
    absent_ids = {
        int(pk)
        for pk in (unmanaged.get("owned_absent_device_ids") or ())
        if isinstance(pk, int)
    }
    uncovered_ids = {
        int(pk)
        for pk in (unmanaged.get("owned_untagged_device_ids") or ())
        if isinstance(pk, int)
    }
    required_runs, required_hours = absence_quarantine_thresholds(sync)
    offer.update(
        {
            "sync_name": sync.name,
            "report_at": generated_at,
            "required_runs": required_runs,
            "required_hours": required_hours,
            "url": reverse(
                "plugins:forward_netbox:forwardsync_prune_uncovered_device",
                kwargs={"pk": sync.pk},
            ),
        }
    )
    detail = (unmanaged.get("owned_detail_by_id") or {}).get(str(device.pk), "")
    if device.pk in absent_ids:
        from .utilities.scope_reconciliation import ABSENT_DETAILS

        partition = partition_quarantined_orphans(sync, [device.pk])
        offer["held"] = device.pk not in set(partition["eligible_pks"])
        offer["offered"] = not foreign_blockers
        # Gone from the snapshot, but Forward's configuration may still list
        # it under an include tag - which is what the operator sees in Forward's
        # UI, and why "still tagged in Forward" reads as a sync bug. Name the
        # configuration fact next to the button, so enabling collection is as
        # visible a remedy as deleting.
        offer["absent_detail"] = detail
        offer["absent_detail_label"] = ABSENT_DETAILS.get(detail, "")
    elif device.pk in uncovered_ids:
        # Uncovered, but Forward still reports it: a scoping decision, and the
        # prune will never touch it. Say that instead of offering a button -
        # and for an endpoint-derived device, say WHICH endpoint-scope rule.
        from .utilities.scope_reconciliation import ENDPOINT_ABSENCE_DETAILS

        offer["still_reported"] = True
        offer["endpoint_detail"] = detail
        offer["endpoint_detail_label"] = ENDPOINT_ABSENCE_DETAILS.get(detail, "")
    return offer


def _release_foreign_blockers_offer(foreign_blockers, primary_sync_pk):
    """Whether the release action can clear every foreign blocker at once.

    Offered only when EVERY foreign blocker belongs to an allowlisted app
    (`RELEASABLE_FOREIGN_APP_LABELS`) - a button that would just fail on the
    first non-allowlisted row is worse than no button, since it invites a
    confirm click that does nothing. A mix names both: what the button would
    release, and what still needs a manual delete elsewhere regardless.
    """
    import json

    from django.urls import reverse

    from .utilities.workload_state import RELEASABLE_FOREIGN_APP_LABELS

    offer = {
        "offered": False,
        "url": "",
        "releasable": [],
        "expected_blockers_json": "{}",
        "unreleasable": list(foreign_blockers),
    }
    if not foreign_blockers or primary_sync_pk is None:
        return offer
    releasable = [
        (label, count)
        for label, count in foreign_blockers
        if label.split(".", 1)[0] in RELEASABLE_FOREIGN_APP_LABELS
    ]
    if len(releasable) != len(foreign_blockers):
        # Partial coverage refuses the whole action rather than releasing
        # some rows and leaving the operator to discover the rest still
        # blocks the delete - see the panel's "unreleasable" list instead.
        return offer
    offer.update(
        {
            "offered": True,
            "url": reverse(
                "plugins:forward_netbox:forwardsync_release_foreign_delete_blockers",
                kwargs={"pk": primary_sync_pk},
            ),
            "releasable": releasable,
            "expected_blockers_json": json.dumps(dict(releasable)),
            "unreleasable": [],
        }
    )
    return offer


class ForwardDeviceOwnershipPanel(PluginTemplateExtension):
    """What the plugin holds on this device, and what a delete does about it.

    The ownership tables held their device with ``PROTECT`` and
    ``related_name="+"``, so a manual delete raised ``ProtectedError`` and
    NetBox rendered the refusal as a list of our record names with no cause
    and no remedy - a customer hit exactly that trying to clear an uncovered
    device by hand, twice, the second time from the device list's bulk
    delete where this panel never appears. Since 2.9.6 an operator's delete
    on main releases the identity and tag claims itself
    (`release_on_operator_delete`); the engine paths keep the PROTECT they
    rely on. So the panel now says what a delete takes with it and still
    links the gated remedy - the prune, with its quarantine and its shrink
    refusal - and still names the records from other plugins that refuse
    both.

    Read-only, local database only, and rendered only for a device the plugin
    actually holds - an unowned device gets no panel.
    """

    models = ["dcim.device"]

    def right_page(self):
        from django.db.models import Q
        from django.urls import reverse

        from forward_netbox.models import ForwardDeviceAbsence
        from forward_netbox.models import ForwardDeviceIdentity
        from forward_netbox.models import ForwardDeviceTagClaim
        from forward_netbox.models import ForwardPreservedDeviceTagAssignment
        from forward_netbox.models import ForwardVirtualParentClaim
        from forward_netbox.utilities.workload_state import describe_delete_blockers
        from forward_netbox.utilities.scope_reconciliation import BACKFILLED_TAG_SLUG
        from forward_netbox.utilities.scope_reconciliation import OUT_OF_SCOPE_TAG_SLUG
        from forward_netbox.utilities.scope_reconciliation import UNCOVERED_TAG_SLUG

        device = self.context["object"]

        identities = list(
            ForwardDeviceIdentity.objects.filter(device=device).select_related("sync")
        )
        claims = list(
            ForwardDeviceTagClaim.objects.filter(device=device).select_related(
                "sync", "tag"
            )
        )
        # Both FK sides: a physical parent is held by its children's claims too,
        # which is why a parent can refuse a delete with nothing of its own.
        parent_claims = list(
            ForwardVirtualParentClaim.objects.filter(
                Q(device=device) | Q(parent_device=device)
            ).select_related("sync")
        )
        preserved = ForwardPreservedDeviceTagAssignment.objects.filter(
            device=device
        ).count()
        if not identities and not claims and not parent_claims and not preserved:
            return ""

        # Ordered, de-duplicated: a device claimed by two syncs names both, and
        # the first is the one the footer links to.
        syncs = []
        seen = set()
        for row in (*identities, *claims, *parent_claims):
            if row.sync_id in seen:
                continue
            seen.add(row.sync_id)
            syncs.append(
                {
                    "sync_id": row.sync_id,
                    "name": row.sync.name,
                    "url": reverse(
                        "plugins:forward_netbox:forwardsync",
                        kwargs={"pk": row.sync_id},
                    ),
                    "scope_url": reverse(
                        "plugins:forward_netbox:forwardsync_scope_reconciliation",
                        kwargs={"pk": row.sync_id},
                    ),
                }
            )

        blockers = describe_delete_blockers(device)
        foreign_blockers = [
            (label, count)
            for label, count in blockers
            if not label.startswith("forward_netbox.")
        ]
        claim_slugs = {claim.tag.slug for claim in claims}
        prune = _uncovered_prune_offer(device, identities, foreign_blockers)
        release_blockers = _release_foreign_blockers_offer(
            foreign_blockers, syncs[0]["sync_id"] if syncs else None
        )
        holders = []
        if identities:
            holders.append(("Device identity", len(identities)))
        if claims:
            holders.append(("Managed tag claim", len(claims)))
        if parent_claims:
            holders.append(("Virtual parent claim", len(parent_claims)))
        if preserved:
            holders.append(("Preserved operator tag", preserved))

        return self.render(
            "forward_netbox/inc/device_ownership_panel.html",
            extra_context={
                "ownership": {
                    "syncs": syncs,
                    "primary_sync_url": syncs[0]["scope_url"] if syncs else "",
                    "holders": holders,
                    "uncovered": UNCOVERED_TAG_SLUG in claim_slugs,
                    "out_of_scope": OUT_OF_SCOPE_TAG_SLUG in claim_slugs,
                    "backfilled": BACKFILLED_TAG_SLUG in claim_slugs,
                    "absence": ForwardDeviceAbsence.objects.filter(device=device)
                    .order_by("-consecutive_absent_runs")
                    .first(),
                    "prune": prune,
                    # Everything that would refuse the delete, not just ours.
                    # A customer's second attempt was refused by ten
                    # netbox_routing BGP rows reached through the cascade,
                    # which no count of our own records would have explained.
                    "delete_blockers": blockers,
                    # The prune releases only this plugin's rows, so a blocker
                    # belonging to anything else refuses the prune too. The
                    # panel has to say so rather than send an operator to a
                    # button that cannot clear their device.
                    "foreign_delete_blockers": [
                        (label, count)
                        for label, count in blockers
                        if not label.startswith("forward_netbox.")
                    ],
                    # Whether that foreign list can be cleared with one
                    # confirm, and the form fields the button needs.
                    "release_blockers": release_blockers,
                    "device_pk": device.pk,
                }
            },
        )


template_extensions = [ForwardDeviceAnalysisPanel, ForwardDeviceOwnershipPanel]
