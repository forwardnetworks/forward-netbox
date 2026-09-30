from collections import Counter

from core.choices import ObjectChangeActionChoices
from django.db.models import Count


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
    # Counted over EVERY change, not the sample. The sample is the first
    # `max_changes` rows by pk, so on a large run it is whatever model happened
    # to stage first: a run of ~96k changes reported a model mix drawn from the
    # first 5,000 and hid which model held the rest.
    action_counts, model_counts, model_action_counts = _full_counts(branch)
    field_counts = Counter()
    field_counts_by_model: dict[str, Counter] = {}
    update_changes_with_field_detail = 0
    update_changes_without_field_detail = 0

    for change in sampled_changes:
        action = str(getattr(change, "action", "") or "unknown")
        model_label = _change_model_label(change)
        if action != ObjectChangeActionChoices.ACTION_UPDATE:
            continue

        fields = _changed_fields(
            getattr(change, "original", None),
            getattr(change, "modified", None),
        )
        if fields:
            update_changes_with_field_detail += 1
            model_field_counts = field_counts_by_model.setdefault(
                model_label, Counter()
            )
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
        "model_action_counts": {
            model: dict(sorted(actions.items()))
            for model, actions in sorted(model_action_counts.items())
        },
        # The field detail below still comes from the sample; only the counts
        # above are complete.
        "counts_cover_all_changes": True,
        "top_changed_fields": _top_counter(field_counts),
        "top_changed_fields_by_model": {
            model: _top_counter(counter)
            for model, counter in sorted(field_counts_by_model.items())
        },
        "update_changes_with_field_detail": update_changes_with_field_detail,
        "update_changes_without_field_detail": update_changes_without_field_detail,
    }


def _full_counts(branch):
    """Action, model and model-by-action counts over every change in the branch."""
    from netbox_branching.models import ChangeDiff

    rows = (
        ChangeDiff.objects.filter(branch=branch)
        .exclude(object_type__model="objectchange")
        .order_by()
        .values("object_type__app_label", "object_type__model", "action")
        .annotate(total=Count("pk"))
    )
    action_counts = Counter()
    model_counts = Counter()
    model_action_counts: dict[str, Counter] = {}
    for row in rows:
        app_label = str(row["object_type__app_label"] or "").strip()
        model = str(row["object_type__model"] or "").strip()
        label = f"{app_label}.{model}" if app_label and model else model or "unknown"
        action = str(row["action"] or "unknown")
        total = int(row["total"])
        action_counts[action] += total
        model_counts[label] += total
        model_action_counts.setdefault(label, Counter())[action] += total
    return action_counts, model_counts, model_action_counts


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
        if field not in EXCLUDED_DIFF_FIELDS
        and original.get(field) != modified.get(field)
    )


def _top_counter(counter, *, limit=10):
    return [
        {"field": str(field), "count": int(count)}
        for field, count in counter.most_common(limit)
    ]
