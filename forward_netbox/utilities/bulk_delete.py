from collections import Counter
from functools import reduce
from operator import attrgetter
from operator import or_

from django.db import models
from django.db import transaction
from django.db.models import signals
from django.db.models import sql
from django.db.models.deletion import Collector


def collector_delete_without_model_signals(queryset, *, signal_free_models):
    """Delete a queryset with Collector semantics and bounded signal suppression.

    Django's Collector still discovers cascades, protected/restricted relations,
    fast deletes, and SET_NULL/SET_DEFAULT updates. Signals are suppressed only
    for the explicitly supplied high-volume models; related models retain their
    normal pre/post-delete behavior. Callers own the surrounding transaction and
    any aggregate repair required after the delete.
    """
    using = queryset.db
    collector = Collector(using=using, origin=queryset)
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
