import json
from functools import lru_cache
from importlib import resources

from django.contrib.contenttypes.models import ContentType
from django.db import transaction


def _load_json_defaults() -> dict[str, dict[str, object]]:
    try:
        with resources.files("forward_netbox.data").joinpath("nqe_map.json").open(
            "r", encoding="utf-8"
        ) as handle:
            return json.load(handle)
    except FileNotFoundError:
        return {}


@lru_cache(maxsize=1)
def get_default_nqe_map() -> dict[str, dict[str, object]]:
    """Return the default NQE query mapping."""

    from ..models import ForwardNQEQuery
    from ..models import ForwardSync

    mapping: dict[str, dict[str, object]] = {}
    for entry in ForwardNQEQuery.objects.select_related("content_type").all():
        label = f"{entry.content_type.app_label}.{entry.content_type.model}"
        mapping[label] = {
            "query_id": entry.query_id,
            "enabled": entry.enabled,
        }
        if entry.description:
            mapping[label]["description"] = entry.description

    if mapping:
        return mapping

    return _load_json_defaults()


@transaction.atomic
def restore_default_nqe_map() -> None:
    """Restore the NQE query map to the shipped defaults."""

    from ..models import ForwardNQEQuery

    defaults = _load_json_defaults()
    content_types = {
        f"{ct.app_label}.{ct.model}": ct
        for ct in ContentType.objects.filter(
            app_label__in=["dcim", "ipam"]
        ).iterator()
    }

    seen_ids = []
    for model_label, meta in defaults.items():
        content_type = content_types.get(model_label)
        if not content_type:
            continue
        obj, _ = ForwardNQEQuery.objects.update_or_create(
            content_type=content_type,
            defaults={
                "query_id": meta.get("query_id", ""),
                "enabled": meta.get("enabled", True),
                "description": meta.get("description", ""),
            },
        )
        seen_ids.append(obj.content_type_id)

    if seen_ids:
        ForwardNQEQuery.objects.exclude(content_type_id__in=seen_ids).delete()
    else:
        ForwardNQEQuery.objects.all().delete()

    # Clear per-sync overrides so restored defaults take effect
    for sync in ForwardSync.objects.exclude(parameters__isnull=True):
        params = dict(sync.parameters)
        if params.pop("nqe_map", None) is not None:
            sync.parameters = params
            sync.save(update_fields=["parameters"])

    get_default_nqe_map.cache_clear()
