from django.contrib.contenttypes.models import ContentType
from django.db import connection
from django.db.models.signals import post_migrate
from django.dispatch import receiver

from .models import ForwardNQEMap
from .utilities.query_registry import builtin_nqe_map_rows
from .utilities.query_registry import BUILTIN_OPTIONAL_QUERY_MAPS


@receiver(post_migrate)
def seed_builtin_nqe_maps(sender, **kwargs):
    if sender.label not in {
        "forward_netbox",
        "netbox_routing",
        "netbox_peering_manager",
    }:
        return
    if ForwardNQEMap._meta.db_table not in connection.introspection.table_names():
        return

    enabled_optional_maps = {
        (query_default["model_string"], query_default["name"])
        for query_default in BUILTIN_OPTIONAL_QUERY_MAPS
        if query_default.get("enabled", True)
    }

    for row in builtin_nqe_map_rows():
        app_label, model = row["model_string"].split(".", 1)
        try:
            netbox_model = ContentType.objects.get(app_label=app_label, model=model)
        except ContentType.DoesNotExist:
            continue

        query_map, created = ForwardNQEMap.objects.get_or_create(
            netbox_model=netbox_model,
            name=row["name"],
            built_in=True,
            defaults={
                "query_id": row["query_id"],
                "query_repository": row["query_repository"],
                "query_path": row["query_path"],
                "query": row["query"],
                "commit_id": row["commit_id"],
                "parameters": row["parameters"],
                "coalesce_fields": row["coalesce_fields"],
                "weight": row["weight"],
            },
        )
        if created:
            if query_map.enabled != row.get("enabled", True):
                query_map.enabled = row.get("enabled", True)
                query_map.save(update_fields=["enabled"])
            continue

        update_fields = []
        if not query_map.query_id and not query_map.query_path:
            for field_name in ("query", "commit_id"):
                value = row[field_name]
                if getattr(query_map, field_name) != value:
                    setattr(query_map, field_name, value)
                    update_fields.append(field_name)
        for field_name in ("parameters", "coalesce_fields", "weight"):
            value = row[field_name]
            if getattr(query_map, field_name) != value:
                setattr(query_map, field_name, value)
                update_fields.append(field_name)
        if (
            not query_map.query_id
            and not query_map.query_path
            and query_map.query == row["query"]
            and (row["model_string"], row["name"]) in enabled_optional_maps
            and not query_map.enabled
        ):
            query_map.enabled = True
            update_fields.append("enabled")
        if update_fields:
            query_map.save(update_fields=update_fields)
