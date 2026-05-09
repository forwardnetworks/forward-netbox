from collections import Counter

from core.models import ObjectChange
from core.models import ObjectType
from django.apps import apps


def object_types_for_model_strings(model_strings):
    object_types = []
    for model_string in model_strings:
        try:
            app_label, model_name = model_string.split(".", 1)
            model = apps.get_model(app_label, model_name)
        except (LookupError, ValueError):
            continue
        object_types.append(ObjectType.objects.get_for_model(model))
    return object_types


def object_changes_for_request(sync, request_id):
    if not request_id:
        return ObjectChange.objects.none()
    object_types = object_types_for_model_strings(sync.get_model_strings())
    if not object_types:
        return ObjectChange.objects.none()
    return ObjectChange.objects.filter(
        request_id=request_id,
        changed_object_type__in=object_types,
    )


def action_counts_for_request(sync, request_id):
    queryset = object_changes_for_request(sync, request_id)
    return Counter(queryset.values_list("action", flat=True))


def any_object_changes_for_request(request_id):
    if not request_id:
        return False
    return ObjectChange.objects.filter(request_id=request_id).exists()


def object_changes_for_ingestion(ingestion):
    return object_changes_for_request(ingestion.sync, ingestion.change_request_id)
