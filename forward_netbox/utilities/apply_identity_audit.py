# Read-only diagnostic for "1 created + 1 deleted every sync" idempotency churn.
#
# That pattern is an identity mismatch: one object whose natural key as Forward
# computes it (the slug/name the query emits) does not equal its key as NetBox
# stored it, so every sync the apply engine creates the Forward version and
# deletes the NetBox orphan. Single-snapshot dup scans cannot see it (both keys
# are individually unique); you have to compare the two key spaces.
#
# For each simple-dimension model this audits:
#   * Forward computed keys  — from the emitted query rows.
#   * NetBox stored keys     — from the live objects (lookup_key_from_object).
# An object/row is matched if ANY of the model's lookup_sets matches (mirroring
# the apply engine's multi-set lookup), so a slug change that still name-matches
# is not a false positive. The leftovers are the churn:
#   * would_create — Forward rows matched by no NetBox object.
#   * would_delete — NetBox objects matched by no Forward row.
# A model with would_create AND would_delete both small (often 1/1) names the
# churning object and shows the differing key. NEVER writes anything.
from .apply_engine_bulk import lookup_key_from_object
from .apply_engine_bulk import lookup_key_from_values

# Identity mirrors apply_engine_bulk's simple-model specs. The Forward query for
# each model emits the lookup fields directly (slug/name/rd/vid/site_slug); we
# read those off the row rather than recomputing slugify.
SIMPLE_MODEL_IDENTITY = {
    "dcim.site": {"lookup_sets": (("slug",), ("name",)), "row": ("slug", "name")},
    "dcim.manufacturer": {
        "lookup_sets": (("slug",), ("name",)),
        "row": ("slug", "name"),
    },
    "dcim.devicerole": {
        "lookup_sets": (("slug",), ("name",)),
        "row": ("slug", "name"),
    },
    "dcim.platform": {"lookup_sets": (("slug",), ("name",)), "row": ("slug", "name")},
    "dcim.devicetype": {"lookup_sets": (("slug",),), "row": ("slug",)},
    "ipam.vrf": {"lookup_sets": (("rd",), ("name",)), "row": ("rd", "name")},
}
SIMPLE_MODELS = tuple(SIMPLE_MODEL_IDENTITY)


def _model_class(model_string):
    from dcim.models import DeviceRole
    from dcim.models import DeviceType
    from dcim.models import Manufacturer
    from dcim.models import Platform
    from dcim.models import Site
    from ipam.models import VRF

    return {
        "dcim.site": Site,
        "dcim.manufacturer": Manufacturer,
        "dcim.devicerole": DeviceRole,
        "dcim.platform": Platform,
        "dcim.devicetype": DeviceType,
        "ipam.vrf": VRF,
    }[model_string]


def audit_model_identity(model_string, rows, *, sample_limit=15):
    """Compare Forward ``rows`` against NetBox objects for ``model_string`` and
    report the would-create / would-delete leftovers (the churn candidates)."""
    identity = SIMPLE_MODEL_IDENTITY[model_string]
    lookup_sets = identity["lookup_sets"]
    row_fields = identity["row"]
    model = _model_class(model_string)

    # Forward key sets (per lookup_set) from emitted row fields.
    forward_keys = {lookup_set: set() for lookup_set in lookup_sets}
    for row in rows:
        values = {field: (row.get(field) or None) for field in row_fields}
        for lookup_set in lookup_sets:
            key = lookup_key_from_values(values, lookup_set, model_string=model_string)
            if key is not None:
                forward_keys[lookup_set].add(key)

    netbox_keys = {lookup_set: set() for lookup_set in lookup_sets}
    netbox_count = 0
    would_delete = []
    for obj in model.objects.all().iterator():
        netbox_count += 1
        obj_keys = {
            lookup_set: lookup_key_from_object(
                obj, lookup_set, model_string=model_string
            )
            for lookup_set in lookup_sets
        }
        for lookup_set, key in obj_keys.items():
            if key is not None:
                netbox_keys[lookup_set].add(key)
        matched = any(
            key is not None and key in forward_keys[lookup_set]
            for lookup_set, key in obj_keys.items()
        )
        if not matched:
            # Surface the stored key fields so the slug/name Forward and NetBox
            # disagree on is visible next to the would_create label.
            key_parts = ", ".join(
                f"{field}={getattr(obj, field, None)}" for field in lookup_sets[0]
            )
            would_delete.append(f"{obj} ({key_parts})")

    would_create = []
    for row in rows:
        values = {field: (row.get(field) or None) for field in row_fields}
        matched = False
        for lookup_set in lookup_sets:
            key = lookup_key_from_values(values, lookup_set, model_string=model_string)
            if key is not None and key in netbox_keys[lookup_set]:
                matched = True
                break
        if not matched:
            label = " / ".join(
                str(values.get(field)) for field in row_fields if values.get(field)
            )
            would_create.append(label or "?")

    return {
        "model": model_string,
        "forward_rows": len(rows),
        "netbox_count": netbox_count,
        "would_create_count": len(would_create),
        "would_delete_count": len(would_delete),
        "would_create_sample": sorted(would_create)[:sample_limit],
        "would_delete_sample": sorted(would_delete)[:sample_limit],
        # The churn signature: both sides non-empty and small => one logical
        # object with two different keys (create the new, delete the orphan).
        "churn_suspect": bool(would_create) and bool(would_delete),
    }


def audit_apply_identity(sync, *, models=None, sample_limit=15, fetch_rows=None):
    """Audit each enabled simple-dimension model for create/delete key mismatches.

    ``fetch_rows(sync, model_string) -> list[dict]`` is injectable for tests; the
    default fetches via ``ForwardQueryFetcher``. Read-only.
    """
    enabled = set(sync.get_model_strings())
    requested = models or SIMPLE_MODELS
    selected = [m for m in requested if m in enabled and m in SIMPLE_MODEL_IDENTITY]
    fetch_rows = fetch_rows or _default_fetch_rows

    results = []
    for model_string in selected:
        rows = fetch_rows(sync, model_string)
        results.append(
            audit_model_identity(model_string, rows, sample_limit=sample_limit)
        )
    return {
        "models_audited": selected,
        "results": results,
        "churn_suspect_models": [r["model"] for r in results if r["churn_suspect"]],
    }


def _default_fetch_rows(sync, model_string):
    from .logging import SyncLogging
    from .query_fetch import ForwardQueryFetcher

    client = sync.source.get_client()
    fetcher = ForwardQueryFetcher(sync, client, SyncLogging())
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
