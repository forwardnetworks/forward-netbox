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
    if device.pk in absent_ids:
        partition = partition_quarantined_orphans(sync, [device.pk])
        offer["held"] = device.pk not in set(partition["eligible_pks"])
        offer["offered"] = not foreign_blockers
    elif device.pk in uncovered_ids:
        # Uncovered, but Forward still reports it: a scoping decision, and the
        # prune will never touch it. Say that instead of offering a button.
        offer["still_reported"] = True
    return offer


class ForwardDeviceOwnershipPanel(PluginTemplateExtension):
    """Why NetBox refuses to delete a plugin-owned device, and what does.

    The ownership tables hold their device with ``PROTECT`` and
    ``related_name="+"``, so a manual delete raises ``ProtectedError`` and
    NetBox renders the refusal as a list of our record names with no cause and
    no remedy - a customer hit exactly that trying to clear an uncovered
    device by hand. The PROTECT is deliberate (the apply path relies on it to
    decline a delete that has not released ownership, and it is the only thing
    standing between one sync's prune and another sync's claim), so the fix is
    to explain it where the operator already is and link the delete that does
    work.

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
                }
            },
        )


template_extensions = [ForwardDeviceAnalysisPanel, ForwardDeviceOwnershipPanel]
