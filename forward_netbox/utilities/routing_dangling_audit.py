# Read-only companion to the post-prune dangler sweep (2.5.6): netbox_routing
# rows reference devices through GenericFKs that never PROTECT, so deletions
# outside the plugin's prune path (manual device deletes, other tooling) can
# leave BGP rows pointing at nothing. This reports them without deleting
# anything; the sweep itself only runs for devices the plugin pruned.
from django.apps import apps
from django.contrib.contenttypes.models import ContentType

DANGLING_ROUTING_NOTE = (
    "Rows descend from routers whose device GenericFK points at a missing "
    "device. Read-only: delete via the plugin's prune (which sweeps its own "
    "deletions) or manually."
)


def audit_routing_dangling_rows(*, sample_limit=10):
    if not apps.is_installed("netbox_routing"):
        return {"skipped": "netbox_routing is not installed"}

    from dcim.models import Device

    device_ct = ContentType.objects.get_for_model(Device)
    device_pks = set(Device.objects.values_list("pk", flat=True))

    BGPRouter = apps.get_model("netbox_routing", "bgprouter")
    BGPScope = apps.get_model("netbox_routing", "bgpscope")
    BGPAddressFamily = apps.get_model("netbox_routing", "bgpaddressfamily")
    BGPPeer = apps.get_model("netbox_routing", "bgppeer")

    dangling_router_pks = set(
        BGPRouter.objects.filter(assigned_object_type=device_ct)
        .exclude(assigned_object_id__in=device_pks)
        .values_list("pk", flat=True)
    )
    scope_pks = set(
        BGPScope.objects.filter(router_id__in=dangling_router_pks).values_list(
            "pk", flat=True
        )
    )
    af_pks = set(
        BGPAddressFamily.objects.filter(scope_id__in=scope_pks).values_list(
            "pk", flat=True
        )
    )
    peer_pks = set(
        BGPPeer.objects.filter(scope_id__in=scope_pks).values_list("pk", flat=True)
    )
    return {
        "dangling": {
            "bgprouter": len(dangling_router_pks),
            "bgpscope": len(scope_pks),
            "bgpaddressfamily": len(af_pks),
            "bgppeer": len(peer_pks),
        },
        "sample_router_pks": sorted(dangling_router_pks)[:sample_limit],
        "note": DANGLING_ROUTING_NOTE,
    }
