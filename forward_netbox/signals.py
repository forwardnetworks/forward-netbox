from django.contrib.contenttypes.models import ContentType
from django.db import connection
from django.db.models.signals import post_migrate
from django.dispatch import receiver

from .models import ForwardNQEMap
from .utilities.query_registry import builtin_nqe_map_rows


@receiver(post_migrate)
def seed_builtin_nqe_maps(sender, **kwargs):
    if sender.label != "forward_netbox":
        return
    if ForwardNQEMap._meta.db_table not in connection.introspection.table_names():
        return

    for row in builtin_nqe_map_rows():
        app_label, model = row["model_string"].split(".", 1)
        try:
            netbox_model = ContentType.objects.get(app_label=app_label, model=model)
        except ContentType.DoesNotExist:
            continue

        ForwardNQEMap.objects.update_or_create(
            netbox_model=netbox_model,
            name=row["name"],
            built_in=True,
            defaults={
                "query_id": row["query_id"],
                "query": row["query"],
                "commit_id": row["commit_id"],
                "parameters": row["parameters"],
                "coalesce_fields": row["coalesce_fields"],
                "weight": row["weight"],
                "enabled": True,
            },
        )
