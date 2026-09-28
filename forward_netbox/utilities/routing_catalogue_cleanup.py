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


# Above this share of a model's entries, a cleanup is refused as a probable
# scope or query fault rather than a real backlog: a narrowed include-tag
# result looks exactly like a fleet of out-of-scope definitions. The floor
# keeps a small estate (or a fixture) from tripping it, as elsewhere.
CATALOGUE_CLEANUP_MAX_FRACTION = 0.5
CATALOGUE_CLEANUP_FRACTION_FLOOR = 25

HOLD_NO_CREATE_RECORD = "no_create_record_from_this_sync"
HOLD_ASSERTED_BY_A_SYNC = "definition_still_asserted_by_a_sync"
HOLD_ROUTE_MAP_REFERENCE = "referenced_by_a_route_map_that_stays"
HOLD_PROTECTED = "protected_by_another_object"

REFUSED_NOT_ENABLED = "model_not_enabled_on_this_sync"
REFUSED_NO_SCOPE = "no_device_tag_scope_configured"
REFUSED_EMPTY_SCOPE = "forward_returned_no_in_scope_definitions"
REFUSED_FRACTION = "too_large_a_share_of_the_catalogue"


class CatalogueCleanupRefused(RuntimeError):
    """The whole cleanup is refused before anything is read for deletion."""


def _sync_request_ids(sync):
    from ..models import ForwardIngestion

    return list(
        ForwardIngestion.objects.filter(sync=sync)
        .exclude(change_request_id=None)
        .values_list("change_request_id", flat=True)
    )


def _created_by_sync(entry_model, pks, request_ids):
    """The pks NetBox's change log shows this sync's ingestions created."""
    from core.models import ObjectChange
    from core.models import ObjectType

    if not pks or not request_ids:
        return set()
    return set(
        ObjectChange.objects.filter(
            changed_object_type=ObjectType.objects.get_for_model(entry_model),
            changed_object_id__in=list(pks),
            action="create",
            request_id__in=request_ids,
        ).values_list("changed_object_id", flat=True)
    )


def _asserted_definition_names(model_string):
    """Definition names any sync's current durable state still asserts."""
    from ..models import ForwardWorkloadState
    from .workload_state import decode_state_entries

    names = set()
    for state in ForwardWorkloadState.objects.filter(
        model_string=model_string, is_current=True
    ):
        entries = decode_state_entries(state.payload, state.payload_checksum)
        for value in entries.values():
            if value.get("action") == "upsert":
                name = str((value.get("row") or {}).get("name") or "")
                if name:
                    names.add(name.lower())
    return names


def _fetch_scope_names(sync, model_strings):
    """A fresh read of which definitions the device-tag scope keeps and drops."""
    from .logging import SyncLogging
    from .query_fetch import ForwardQueryFetcher

    client = sync.source.get_client()
    fetcher = ForwardQueryFetcher(sync, client, SyncLogging())
    context = fetcher.resolve_context()
    if not context.scoped_device_names:
        raise CatalogueCleanupRefused(REFUSED_NO_SCOPE)
    fetcher.fetch_workloads(
        context,
        model_strings=list(model_strings),
        validate_rows=False,
        include_diagnostics=False,
    )
    return fetcher.catalogue_scope_names


def plan_out_of_scope_catalogue_cleanup(sync, *, scope_names=None):
    """What the cleanup would delete and hold, per model. Deletes nothing.

    ``scope_names`` is a fetcher's `catalogue_scope_names`; a fresh Forward
    read is made when it is omitted.
    """
    model_strings = [
        model_string
        for model_string in CATALOGUE_ENTRY_PARENTS
        if sync.is_model_enabled(model_string)
    ]
    if scope_names is None:
        scope_names = _fetch_scope_names(sync, model_strings)
    request_ids = _sync_request_ids(sync)

    plans = {}
    for model_string in CATALOGUE_ENTRY_PARENTS:
        if model_string not in model_strings:
            plans[model_string] = {"refused": REFUSED_NOT_ENABLED}
            continue
        names = (scope_names or {}).get(model_string) or {}
        if not names.get("in_scope"):
            plans[model_string] = {"refused": REFUSED_EMPTY_SCOPE}
            continue
        out_of_scope = set(names.get("out_of_scope") or ()) - set(names["in_scope"])
        entry_model, parent_fk, _parent_model = catalogue_models(model_string)
        candidates = list(
            outside_scope_entry_queryset(model_string, out_of_scope).values_list(
                "pk", f"{parent_fk}_id", f"{parent_fk}__name"
            )
        )
        total = entry_model.objects.count()
        if (
            len(candidates) > CATALOGUE_CLEANUP_FRACTION_FLOOR
            and total
            and len(candidates) > total * CATALOGUE_CLEANUP_MAX_FRACTION
        ):
            plans[model_string] = {
                "refused": REFUSED_FRACTION,
                "candidate_count": len(candidates),
                "entry_count": total,
            }
            continue
        created = _created_by_sync(
            entry_model, [pk for pk, _parent, _name in candidates], request_ids
        )
        asserted = _asserted_definition_names(model_string)
        deletable, held = [], {}
        for pk, parent_id, parent_name in candidates:
            if pk not in created:
                reason = HOLD_NO_CREATE_RECORD
            elif str(parent_name or "").lower() in asserted:
                reason = HOLD_ASSERTED_BY_A_SYNC
            else:
                deletable.append((pk, parent_id))
                continue
            held[reason] = held.get(reason, 0) + 1
        plans[model_string] = {
            "candidate_count": len(candidates),
            "entry_count": total,
            "deletable": deletable,
            "held": held,
        }

    _hold_route_map_references(plans)
    return plans


def _hold_route_map_references(plans):
    """Keep a list a route map outside the cleanup still matches on.

    Deleting a prefix or community list removes its route-map match links
    silently (they are many-to-many, not PROTECT); a route map that stays
    would lose part of its policy.
    """
    route_map_plan = plans.get("netbox_routing.routemapentry") or {}
    leaving_route_map_entries = {
        pk for pk, _parent in route_map_plan.get("deletable") or ()
    }
    try:
        RouteMapEntry, _fk, _parent = catalogue_models("netbox_routing.routemapentry")
    except LookupError:
        return
    for model_string, link in (
        ("netbox_routing.prefixlistentry", "match_prefix_list"),
        ("netbox_routing.communitylistentry", "match_community_list"),
    ):
        plan = plans.get(model_string) or {}
        deletable = plan.get("deletable")
        if not deletable:
            continue
        parent_ids = {parent for _pk, parent in deletable}
        referenced = set(
            RouteMapEntry.objects.filter(**{f"{link}__in": parent_ids})
            .exclude(pk__in=leaving_route_map_entries)
            .values_list(f"{link}", flat=True)
        )
        if not referenced:
            continue
        kept = [(pk, parent) for pk, parent in deletable if parent not in referenced]
        plan["held"][HOLD_ROUTE_MAP_REFERENCE] = plan["held"].get(
            HOLD_ROUTE_MAP_REFERENCE, 0
        ) + (len(deletable) - len(kept))
        plan["deletable"] = kept


def prune_out_of_scope_catalogue(sync, *, scope_names=None):
    """Delete routing policy entries no in-scope device holds. Operator-initiated.

    Only the six catalogue models (`CATALOGUE_CLEANUP_MODELS`) are ever
    touched, and only entries this sync provably created
    (`plan_out_of_scope_catalogue_cleanup`). A list is deleted when its last
    entry goes and nothing protects it. Returns value-free counts per model.
    """
    from django.db import transaction
    from django.db.models.deletion import ProtectedError

    from .ownership import ownership_write_lock

    plans = plan_out_of_scope_catalogue_cleanup(sync, scope_names=scope_names)
    results = {}
    for model_string, plan in plans.items():
        if plan.get("refused"):
            results[model_string] = {
                key: value for key, value in plan.items() if key != "deletable"
            }
            continue
        entry_model, parent_fk, parent_model = catalogue_models(model_string)
        for model in (entry_model, parent_model):
            label = f"{model._meta.app_label}.{model._meta.model_name}"
            if label not in CATALOGUE_CLEANUP_MODELS:
                raise CatalogueCleanupRefused(f"{label} is not a catalogue model")
        held = dict(plan["held"])
        deleted_entries = deleted_lists = 0
        by_parent = {}
        for pk, parent_id in plan["deletable"]:
            by_parent.setdefault(parent_id, []).append(pk)
        for parent_id, pks in sorted(by_parent.items()):
            try:
                with ownership_write_lock(), transaction.atomic():
                    deleted_entries += (
                        entry_model.objects.filter(
                            pk__in=pks, **{f"{parent_fk}_id": parent_id}
                        )
                        .delete()[1]
                        .get(entry_model._meta.label, 0)
                    )
                    parent = parent_model.objects.filter(pk=parent_id).first()
                    if (
                        parent is not None
                        and not entry_model.objects.filter(
                            **{f"{parent_fk}_id": parent_id}
                        ).exists()
                    ):
                        parent.delete()
                        deleted_lists += 1
            except ProtectedError:
                held[HOLD_PROTECTED] = held.get(HOLD_PROTECTED, 0) + len(pks)
        results[model_string] = {
            "candidate_count": plan["candidate_count"],
            "entry_count": plan["entry_count"],
            "deleted_entries": deleted_entries,
            "deleted_lists": deleted_lists,
            "held": held,
        }
    return results
