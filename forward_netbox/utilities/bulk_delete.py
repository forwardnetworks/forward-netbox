import time
from collections import Counter
from functools import reduce
from operator import attrgetter
from operator import or_

from django.db import connections
from django.db import models
from django.db import OperationalError
from django.db import transaction
from django.db.models import signals
from django.db.models import sql
from django.db.models.deletion import Collector
from django.db.models.deletion import get_candidate_relations_to_delete


RELATIONSHIP_LOCK_RETRY_ATTEMPTS = 20
RELATIONSHIP_LOCK_RETRY_MAX_DELAY_SECONDS = 0.5
DEVICE_GENERIC_RELATION_GUARDS = (
    ("ipam_service", "parent_object_type_id", "parent_object_id"),
    ("tenancy_contactassignment", "object_type_id", "object_id"),
    ("extras_imageattachment", "object_type_id", "object_id"),
    ("extras_bookmark", "object_type_id", "object_id"),
    ("extras_journalentry", "assigned_object_type_id", "assigned_object_id"),
    ("extras_subscription", "object_type_id", "object_id"),
    ("extras_taggeditem", "content_type_id", "object_id"),
    (
        "netbox_routing_bgprouter",
        "assigned_object_type_id",
        "assigned_object_id",
    ),
)
DEVICE_GENERIC_RELATION_GUARD_FUNCTION = (
    "forward_netbox_enforce_device_generic_relation"
)
DEVICE_GENERIC_RELATION_GUARD_TRIGGER = "forward_netbox_device_gfk_guard"


class RelationshipWriteBarrierTimeout(RuntimeError):
    pass


def install_device_generic_relation_guards(*, using):
    """Install missing guards when an optional GenericRelation table appears."""
    database = connections[using]
    if database.vendor != "postgresql":
        return
    with database.cursor() as cursor:
        cursor.execute(
            "SELECT to_regprocedure(%s) IS NOT NULL",
            [f"{DEVICE_GENERIC_RELATION_GUARD_FUNCTION}()"],
        )
        if not cursor.fetchone()[0]:
            return
        existing_tables = set(database.introspection.table_names(cursor))
        for (
            table_name,
            content_type_column,
            object_id_column,
        ) in DEVICE_GENERIC_RELATION_GUARDS:
            if table_name not in existing_tables:
                continue
            cursor.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_trigger
                    WHERE tgrelid = %s::regclass
                      AND tgname = %s
                      AND NOT tgisinternal
                )
                """,
                [table_name, DEVICE_GENERIC_RELATION_GUARD_TRIGGER],
            )
            if cursor.fetchone()[0]:
                continue
            quoted_table = database.ops.quote_name(table_name)
            quoted_trigger = database.ops.quote_name(
                DEVICE_GENERIC_RELATION_GUARD_TRIGGER
            )
            quoted_content_type_column = database.ops.quote_name(content_type_column)
            quoted_object_id_column = database.ops.quote_name(object_id_column)
            content_type_argument = content_type_column.replace("'", "''")
            object_id_argument = object_id_column.replace("'", "''")
            cursor.execute(
                f"""
                CREATE TRIGGER {quoted_trigger}
                BEFORE INSERT OR UPDATE OF
                    {quoted_content_type_column}, {quoted_object_id_column}
                ON {quoted_table}
                FOR EACH ROW
                EXECUTE FUNCTION {DEVICE_GENERIC_RELATION_GUARD_FUNCTION}(
                    '{content_type_argument}',
                    '{object_id_argument}'
                )
                """
            )


def _is_lock_not_available(exc):
    current = exc
    while current is not None:
        if getattr(current, "sqlstate", None) == "55P03":
            return True
        current = getattr(current, "__cause__", None)
    return False


def lock_tables_for_writes(table_names, *, using):
    """Acquire a bounded, deadlock-safe write barrier for database tables."""
    table_names = sorted(set(table_names))
    if not table_names:
        return
    database = connections[using]
    if database.vendor != "postgresql":
        raise RuntimeError("Relationship write barriers require PostgreSQL.")
    if not database.in_atomic_block:
        raise RuntimeError("Relationship write barriers require an atomic block.")

    quoted_tables = ", ".join(
        database.ops.quote_name(table_name) for table_name in table_names
    )
    for attempt in range(RELATIONSHIP_LOCK_RETRY_ATTEMPTS):
        try:
            with transaction.atomic(using=using):
                with database.cursor() as cursor:
                    cursor.execute(
                        "LOCK TABLE "
                        f"{quoted_tables} IN SHARE ROW EXCLUSIVE MODE NOWAIT"
                    )
            return
        except OperationalError as exc:
            if not _is_lock_not_available(exc):
                raise
            if attempt + 1 == RELATIONSHIP_LOCK_RETRY_ATTEMPTS:
                raise RelationshipWriteBarrierTimeout(
                    "Timed out acquiring relationship write barrier."
                ) from exc
            time.sleep(
                min(
                    0.05 * (2**attempt),
                    RELATIONSHIP_LOCK_RETRY_MAX_DELAY_SECONDS,
                )
            )


def lock_related_writes_for_delete(model, *, using):
    """Block new reverse relations while ``model`` rows are being deleted.

    Django's Collector discovers related rows before applying field updates and
    deletes. Without a write barrier, a concurrent writer can add a relation
    after discovery and have it updated or deleted without the Collector ever
    evaluating that row's on-delete policy.
    """
    related_models = {
        relation.related_model
        for relation in get_candidate_relations_to_delete(model._meta)
    }
    related_models.update(
        field.related_model
        for field in model._meta.private_fields
        if hasattr(field, "bulk_related_objects")
    )
    lock_tables_for_writes(
        {
            related_model._meta.db_table
            for related_model in related_models
            if related_model is not None and related_model._meta.managed
        },
        using=using,
    )


def collector_delete_without_model_signals(
    queryset,
    *,
    signal_free_models,
    ignored_related_models=frozenset(),
):
    """Delete a queryset with Collector semantics and bounded signal suppression.

    Django's Collector still discovers cascades, protected/restricted relations,
    fast deletes, and SET_NULL/SET_DEFAULT updates. Signals are suppressed only
    for the explicitly supplied high-volume models; related models retain their
    normal pre/post-delete behavior. Callers own the surrounding transaction and
    any aggregate repair required after the delete.
    """
    using = queryset.db
    ignored_related_models = frozenset(ignored_related_models)

    class ScopedCollector(Collector):
        def related_objects(self, related_model, related_fields, objs):
            if related_model in ignored_related_models:
                return related_model._base_manager.using(self.using).none()
            return super().related_objects(related_model, related_fields, objs)

    collector = ScopedCollector(using=using, origin=queryset)
    collector.collect(queryset)
    signal_free_models = frozenset(signal_free_models)

    for model, instances in collector.data.items():
        collector.data[model] = sorted(instances, key=attrgetter("pk"))
    collector.sort()
    deleted_counter = Counter()

    with transaction.atomic(using=using, savepoint=False):
        for model, obj in collector.instances_with_model():
            if model._meta.auto_created or model in signal_free_models:
                continue
            signals.pre_delete.send(
                sender=model,
                instance=obj,
                using=using,
                origin=collector.origin,
            )

        for related_queryset in collector.fast_deletes:
            count = related_queryset._raw_delete(using=using)
            if count:
                deleted_counter[related_queryset.model._meta.label] += count

        for (field, value), instances_list in collector.field_updates.items():
            updates = []
            objects = []
            for instances in instances_list:
                if (
                    isinstance(instances, models.QuerySet)
                    and instances._result_cache is None
                ):
                    updates.append(instances)
                else:
                    objects.extend(instances)
            if updates:
                reduce(or_, updates).update(**{field.name: value})
            if objects:
                model = objects[0].__class__
                query = sql.UpdateQuery(model)
                query.update_batch(
                    list({obj.pk for obj in objects}),
                    {field.name: value},
                    using,
                )

        for instances in collector.data.values():
            instances.reverse()

        for model, instances in collector.data.items():
            query = sql.DeleteQuery(model)
            count = query.delete_batch([obj.pk for obj in instances], using)
            if count:
                deleted_counter[model._meta.label] += count
            if model._meta.auto_created or model in signal_free_models:
                continue
            for obj in instances:
                signals.post_delete.send(
                    sender=model,
                    instance=obj,
                    using=using,
                    origin=collector.origin,
                )

    for model, instances in collector.data.items():
        for instance in instances:
            setattr(instance, model._meta.pk.attname, None)
    return sum(deleted_counter.values()), dict(deleted_counter)
