from collections import Counter

from core.choices import ObjectChangeActionChoices


DEFAULT_MAX_CHANGE_DIFFS = 5000
EXCLUDED_DIFF_FIELDS = {"last_updated"}


def change_explainability_summary(ingestion, *, max_changes=DEFAULT_MAX_CHANGE_DIFFS):
    if ingestion is None:
        return _unavailable("ingestion_missing")
    branch = getattr(ingestion, "branch", None)
    if branch is None:
        return _unavailable("branch_missing")

    from netbox_branching.models import ChangeDiff

    queryset = (
        ChangeDiff.objects.filter(branch=branch)
        .exclude(object_type__model="objectchange")
        .select_related("object_type")
        .order_by("pk")
    )
    total_change_count = queryset.count()
    sampled_changes = list(queryset[:max_changes])
    action_counts = Counter()
    model_counts = Counter()
    field_counts = Counter()
    field_counts_by_model: dict[str, Counter] = {}
    update_changes_with_field_detail = 0
    update_changes_without_field_detail = 0

    for change in sampled_changes:
        action = str(getattr(change, "action", "") or "unknown")
        model_label = _change_model_label(change)
        action_counts[action] += 1
        model_counts[model_label] += 1
        if action != ObjectChangeActionChoices.ACTION_UPDATE:
            continue

        fields = _changed_fields(
            getattr(change, "original", None),
            getattr(change, "modified", None),
        )
        if fields:
            update_changes_with_field_detail += 1
            model_field_counts = field_counts_by_model.setdefault(model_label, Counter())
            for field in fields:
                field_counts[field] += 1
                model_field_counts[field] += 1
        else:
            update_changes_without_field_detail += 1

    return {
        "available": True,
        "source": "netbox_branching.changediff",
        "branch": getattr(branch, "name", "") or "",
        "total_change_count": total_change_count,
        "sampled_change_count": len(sampled_changes),
        "truncated": total_change_count > len(sampled_changes),
        "max_changes": int(max_changes),
        "action_counts": dict(sorted(action_counts.items())),
        "model_counts": dict(sorted(model_counts.items())),
        "top_changed_fields": _top_counter(field_counts),
        "top_changed_fields_by_model": {
            model: _top_counter(counter)
            for model, counter in sorted(field_counts_by_model.items())
        },
        "update_changes_with_field_detail": update_changes_with_field_detail,
        "update_changes_without_field_detail": update_changes_without_field_detail,
    }


def _unavailable(reason):
    return {
        "available": False,
        "reason": reason,
        "source": "netbox_branching.changediff",
    }


def _change_model_label(change):
    object_type = getattr(change, "object_type", None)
    app_label = str(getattr(object_type, "app_label", "") or "").strip()
    model = str(getattr(object_type, "model", "") or "").strip()
    if app_label and model:
        return f"{app_label}.{model}"
    return model or "unknown"


def _changed_fields(original, modified):
    if not isinstance(original, dict) or not isinstance(modified, dict):
        return []
    fields = set(original) | set(modified)
    return sorted(
        field
        for field in fields
        if field not in EXCLUDED_DIFF_FIELDS and original.get(field) != modified.get(field)
    )


def _top_counter(counter, *, limit=10):
    return [
        {"field": str(field), "count": int(count)}
        for field, count in counter.most_common(limit)
    ]
