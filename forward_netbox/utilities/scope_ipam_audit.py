# Read-only audit of network-global IPAM that the sync's Forward fetch no longer
# reports. Device-tag scope prune is device-derived: it only removes rows tied to
# an out-of-scope device. Network-global objects (prefixes, VLANs, VRFs) carry no
# device, so they are imported/updated but never scope-deleted (see the Operations
# Guide). This module surfaces NetBox prefixes/VLANs/VRFs whose identity is absent
# from the latest Forward fetch so an operator can review and remove them by hand.
#
# It NEVER deletes anything. Identity matching reuses the apply engine's own key
# helpers (lookup_key_from_object / lookup_key_from_values) and the same read-only
# FK resolution apply uses, so a "stale" verdict matches what the sync would
# consider the same object. An object whose identity is indeterminate (no non-null
# lookup key) is reported as `unmatchable` and never flagged stale, so the audit
# cannot produce a false delete candidate.
from .apply_engine_bulk import lookup_key_from_object
from .apply_engine_bulk import lookup_key_from_values
from .query_fetch import ForwardQueryFetcher

# Identity mirrors forward_netbox.utilities.apply_engine_bulk specs for these
# models (bulk_orm_apply_simple_models). These are NetBox natural-key lookup sets;
# keep them in sync with that spec.
GLOBAL_IPAM_IDENTITY = {
    "ipam.vrf": {"lookup_sets": (("rd",), ("name",)), "nullable": ()},
    "ipam.vlan": {"lookup_sets": (("site", "vid"),), "nullable": ()},
    "ipam.prefix": {"lookup_sets": (("prefix", "vrf"),), "nullable": ("vrf",)},
}
GLOBAL_IPAM_AUDIT_MODELS = ("ipam.prefix", "ipam.vlan", "ipam.vrf")


def _model_class(model_string):
    from ipam.models import Prefix
    from ipam.models import VLAN
    from ipam.models import VRF

    return {"ipam.vrf": VRF, "ipam.vlan": VLAN, "ipam.prefix": Prefix}[model_string]


def _resolve_forward_fks(model_string, rows):
    """Read-only FK resolution mirroring apply's normalization. Returns lookup
    dicts; never creates objects (apply would create missing VRFs/Sites — the
    audit must not)."""
    if model_string == "ipam.prefix":
        from ipam.models import VRF

        names = {row.get("vrf") for row in rows if row.get("vrf")}
        return {
            "vrf_by_name": {
                vrf.name: vrf for vrf in VRF.objects.filter(name__in=names) if vrf.name
            }
        }
    if model_string == "ipam.vlan":
        from dcim.models import Site
        from django.db.models import Q

        values = set()
        for row in rows:
            for value in (row.get("site_slug"), row.get("site")):
                if value:
                    values.add(value)
        sites = Site.objects.filter(Q(slug__in=values) | Q(name__in=values))
        site_by_key = {}
        for site in sites:
            if site.slug:
                site_by_key[site.slug] = site
            if site.name:
                site_by_key[site.name] = site
        return {"site_by_key": site_by_key}
    return {}


def _normalize_forward_row(model_string, row, fks):
    if model_string == "ipam.vrf":
        return {"rd": row.get("rd") or None, "name": row.get("name")}
    if model_string == "ipam.vlan":
        try:
            vid = int(row.get("vid"))
        except (TypeError, ValueError):
            return None
        site = None
        if row.get("site"):
            site = fks["site_by_key"].get(row.get("site_slug")) or fks[
                "site_by_key"
            ].get(row.get("site"))
        return {"site": site, "vid": vid}
    if model_string == "ipam.prefix":
        vrf = fks["vrf_by_name"].get(row.get("vrf")) if row.get("vrf") else None
        return {"prefix": row.get("prefix"), "vrf": vrf}
    return None


def _forward_key_sets(model_string, rows):
    identity = GLOBAL_IPAM_IDENTITY[model_string]
    lookup_sets = identity["lookup_sets"]
    nullable = identity["nullable"]
    fks = _resolve_forward_fks(model_string, rows)
    keys_by_set = {lookup_set: set() for lookup_set in lookup_sets}
    for row in rows:
        values = _normalize_forward_row(model_string, row, fks)
        if values is None:
            continue
        for lookup_set in lookup_sets:
            key = lookup_key_from_values(
                values,
                lookup_set,
                model_string=model_string,
                nullable_fields=nullable,
            )
            if key is not None:
                keys_by_set[lookup_set].add(key)
    return lookup_sets, nullable, keys_by_set


def audit_model_rows(model_string, rows, *, sample_limit=20):
    """Compare every NetBox object of ``model_string`` against the Forward
    ``rows`` and report which NetBox objects are absent from the Forward set."""
    if model_string not in GLOBAL_IPAM_IDENTITY:
        raise ValueError(f"Unsupported audit model: {model_string}")
    lookup_sets, nullable, keys_by_set = _forward_key_sets(model_string, rows)
    model = _model_class(model_string)

    netbox_count = 0
    unmatchable_count = 0
    stale = []
    for obj in model.objects.all().iterator():
        netbox_count += 1
        obj_keys = [
            lookup_key_from_object(
                obj,
                lookup_set,
                model_string=model_string,
                nullable_fields=nullable,
            )
            for lookup_set in lookup_sets
        ]
        non_null = [key for key in obj_keys if key is not None]
        if not non_null:
            # Indeterminate identity — never flag as stale (avoid false deletes).
            unmatchable_count += 1
            continue
        matched = any(
            key is not None and key in keys_by_set[lookup_set]
            for lookup_set, key in zip(lookup_sets, obj_keys)
        )
        if not matched:
            stale.append(obj)

    return {
        "model": model_string,
        "forward_rows": len(rows),
        "netbox_count": netbox_count,
        "unmatchable_count": unmatchable_count,
        "stale_count": len(stale),
        "stale_sample": [str(obj) for obj in stale[:sample_limit]],
    }


def _default_fetch_rows(sync, client, logger, model_string):
    fetcher = ForwardQueryFetcher(sync, client, logger)
    context = fetcher.resolve_context()
    fetcher.run_preflight(context, model_strings=[model_string])
    workloads = fetcher.fetch_workloads(
        context,
        model_strings=[model_string],
        validate_rows=False,
        include_diagnostics=False,
    )
    rows = []
    for workload in workloads:
        rows.extend(workload.upsert_rows or [])
    return rows


def audit_global_ipam_scope(
    sync, client, logger, *, models=None, sample_limit=20, fetch_rows=None
):
    """Audit each enabled network-global IPAM model for NetBox objects the
    Forward fetch no longer reports. Read-only.

    ``fetch_rows(sync, client, logger, model_string) -> list[dict]`` is injectable
    for tests; the default fetches via ``ForwardQueryFetcher``.
    """
    enabled = set(sync.get_model_strings())
    requested = models or GLOBAL_IPAM_AUDIT_MODELS
    selected = [model for model in requested if model in enabled]
    fetch_rows = fetch_rows or _default_fetch_rows

    results = []
    for model_string in selected:
        rows = fetch_rows(sync, client, logger, model_string)
        results.append(audit_model_rows(model_string, rows, sample_limit=sample_limit))
    return {
        "models_audited": selected,
        "results": results,
        "total_stale": sum(result["stale_count"] for result in results),
    }
