from core.choices import JobStatusChoices
from core.models import Job
from django.contrib.contenttypes.models import ContentType
from django.db import connection
from django.db.models import Q
from django.db.models.deletion import ProtectedError
from django.db.models.signals import post_migrate
from django.db.models.signals import pre_delete
from django.dispatch import receiver
from netbox.constants import ADVISORY_LOCK_KEYS

from .models import ForwardIngestion
from .models import ForwardNQEMap
from .models import ForwardSync
from .utilities.bulk_delete import install_device_generic_relation_guards
from .utilities.query_registry import builtin_nqe_map_rows
from .utilities.query_registry import BUILTIN_OPTIONAL_QUERY_MAPS


@receiver(post_migrate)
def ensure_device_generic_relation_guards(sender, using, **kwargs):
    if sender.label not in {"forward_netbox", "netbox_routing"}:
        return
    install_device_generic_relation_guards(using=using)


@receiver(post_migrate)
def seed_builtin_nqe_maps(sender, **kwargs):
    if sender.label not in {
        "forward_netbox",
        "netbox_routing",
        "netbox_peering_manager",
        "netbox_cisco_aci",
        "netbox_dlm",
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


@receiver(pre_delete, sender=ForwardSync)
def cancel_enqueued_jobs_on_sync_delete(sender, instance, **kwargs):
    """Cancel queued work and reject deletion while a worker is running.

    The JobsMixin GenericRelation cascade removes Job rows through the SQL
    collector, which skips Job.delete()'s RQ-cancel override — a standing
    schedule (2.5.6 JobRunner recurrence) would leave a live RQ scheduler
    entry firing against a deleted sync forever. A running Job cannot be
    cancelled safely: deleting it loses terminal diagnostics while its worker
    continues, so the sync remains protected until that occurrence finishes.
    """
    acquire_job_schedule_transaction_lock()
    ingestion_rows = list(
        ForwardIngestion.objects.filter(sync=instance).values_list(
            "pk", "job_id", "merge_job_id"
        )
    )
    ingestion_ids = [row[0] for row in ingestion_rows]
    referenced_job_ids = {
        job_id for row in ingestion_rows for job_id in row[1:] if job_id is not None
    }
    sync_type = ContentType.objects.get_for_model(ForwardSync, for_concrete_model=False)
    ingestion_type = ContentType.objects.get_for_model(
        ForwardIngestion, for_concrete_model=False
    )
    bound_jobs = Job.objects.filter(
        Q(object_type=sync_type, object_id=instance.pk)
        | Q(object_type=ingestion_type, object_id__in=ingestion_ids)
        | Q(pk__in=referenced_job_ids)
    )
    running = list(
        bound_jobs.filter(status=JobStatusChoices.STATUS_RUNNING).order_by("pk")
    )
    if running:
        raise ProtectedError(
            "Cannot delete a Forward sync while one of its jobs is running.",
            running,
        )
    for job in bound_jobs.filter(
        status__in=[
            JobStatusChoices.STATUS_PENDING,
            JobStatusChoices.STATUS_SCHEDULED,
        ]
    ):
        job.delete()


def acquire_job_schedule_transaction_lock():
    """Serialize deletion through the surrounding transaction's commit."""
    if not connection.in_atomic_block:
        raise RuntimeError("ForwardSync deletion requires an atomic transaction.")
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT pg_advisory_xact_lock(%s)",
            [ADVISORY_LOCK_KEYS["job-schedules"]],
        )
