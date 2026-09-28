"""Routing policy catalogue entries no in-scope device holds.

The prefix-list, community-list and route-map maps are global catalogues: one
definition is shared by every device configured with it. A definition only
devices outside the sync's include tags hold is dropped by the device-tag
scope, so it is never staged again - and a NetBox copy imported before, or
left by a source that does not prune out-of-scope rows, stays indefinitely.

This module names that set, by the definition's stored name, for the drift
report's count and for the operator's cleanup. It never decides that
something should be deleted on its own; see `prune_out_of_scope_catalogue`.
"""

from django.db.models.functions import Lower

# Entry model -> (entry model name, parent FK on the entry, parent model name).
# The cleanup's allowlist: nothing outside these six models is ever touched.
CATALOGUE_ENTRY_PARENTS = {
    "netbox_routing.prefixlistentry": ("PrefixListEntry", "prefix_list", "PrefixList"),
    "netbox_routing.communitylistentry": (
        "CommunityListEntry",
        "community_list",
        "CommunityList",
    ),
    "netbox_routing.routemapentry": ("RouteMapEntry", "route_map", "RouteMap"),
}
CATALOGUE_CLEANUP_MODELS = frozenset(
    f"netbox_routing.{name.lower()}"
    for entry, _fk, parent in CATALOGUE_ENTRY_PARENTS.values()
    for name in (entry, parent)
)


def catalogue_models(model_string):
    """``(entry model, parent fk name, parent model)`` for a catalogue model."""
    from django.apps import apps

    entry_name, parent_fk, parent_name = CATALOGUE_ENTRY_PARENTS[model_string]
    return (
        apps.get_model("netbox_routing", entry_name),
        parent_fk,
        apps.get_model("netbox_routing", parent_name),
    )


def outside_scope_entry_queryset(model_string, out_of_scope_names):
    """NetBox entries whose parent is one of these out-of-scope definitions.

    Matched case-insensitively on the parent's name, the way the write path
    matches it: netbox-routing's name uniqueness is case-insensitive and the
    catalogue's chosen spelling can change between runs.
    """
    entry_model, parent_fk, _parent_model = catalogue_models(model_string)
    lowered = sorted({str(name).lower() for name in out_of_scope_names if name})
    if not lowered:
        return entry_model.objects.none()
    return entry_model.objects.annotate(
        _parent_lname=Lower(f"{parent_fk}__name")
    ).filter(_parent_lname__in=lowered)
