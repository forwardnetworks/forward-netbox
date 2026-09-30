"""The evidence an operator used to have to gather by hand.

A customer investigating an unexpectedly large run was asked to run a script in
a NetBox shell to learn four things: how many rows NetBox held for a model
before the run and after it, whether any rows were duplicated, what each recent
ingestion did to which models, and whether the merge had happened at all. None
of it was in the support bundle, so the diagnosis waited on a person with shell
access. It is all here now, counts and ids only, and it travels with Health and
the bundle.

Everything is bounded. Each aggregate runs under a statement timeout inside its
own savepoint, and a query that trips it - or a model that cannot be counted -
is reported as skipped with the reason, never omitted: a missing number that
says nothing looks the same as a number nobody thought to collect.

Nothing in this module reads names, configuration text or credentials.
"""

from contextlib import contextmanager
from datetime import datetime

from django.apps import apps
from django.db import connection
from django.db import DatabaseError
from django.db import transaction
from django.db.models import Count

# How long any one aggregate may run before it is reported as skipped.
QUERY_TIME_LIMIT_SECONDS = 15
# Ingestions listed per sync, newest first.
RECENT_INGESTION_LIMIT = 3
# Model-and-action rows listed per ingestion; the total is always reported.
MERGED_CHANGE_ROW_LIMIT = 60
# Natural keys that identify "the same row" per model, where one exists and is
# unique in a healthy table. A group of more than one is a duplicate.
DUPLICATE_KEYS = {
    "netbox_routing.bgppeer": ("scope", "peer"),
}


@contextmanager
def _time_limited():
    """Run the enclosed queries in a savepoint with a statement timeout."""
    with transaction.atomic():
        with connection.cursor() as cursor:
            cursor.execute(
                "SET LOCAL statement_timeout = %s",
                [int(QUERY_TIME_LIMIT_SECONDS * 1000)],
            )
        yield


def _guarded(fn):
    """Return ``(value, skipped_reason)``; the reason is None on success."""
    try:
        with _time_limited():
            return fn(), None
    except DatabaseError as exc:
        return None, f"skipped: {type(exc).__name__} (time limit or database error)"


def _iso(value):
    return value.isoformat() if isinstance(value, datetime) else None


def _model_class(model_string):
    try:
        app_label, model_name = str(model_string).split(".", 1)
        return apps.get_model(app_label, model_name)
    except (LookupError, ValueError):
        return None


def existing_row_count(model_string):
    """``(count, note)`` for a model's NetBox table; the note says why not."""
    model = _model_class(model_string)
    if model is None:
        return None, "not installed"
    count, skipped = _guarded(lambda: model.objects.count())
    return count, skipped or ""


def _forward_row_counts(sync):
    """Rows Forward returned per model in the latest validation run."""
    run = sync.latest_validation_run
    models = (getattr(run, "drift_summary", None) or {}).get("models") or {}
    counts = {}
    for model_string, entry in models.items():
        try:
            counts[model_string] = int(entry.get("row_count"))
        except (AttributeError, TypeError, ValueError):
            continue
    return counts


def _latest_ingestion_started(sync):
    ingestion = sync.forwardingestion_set.select_related("job").order_by("-pk").first()
    job = getattr(ingestion, "job", None)
    return getattr(job, "started", None)


def row_count_evidence(sync, enabled_models):
    """NetBox's row count beside Forward's for every enabled model.

    ``netbox_rows`` is the whole NetBox table, including rows this sync does not
    manage, so it can exceed Forward's count legitimately; a NetBox count far
    BELOW Forward's is what says the last runs never loaded the model.
    ``rows_before_latest_run`` and ``rows_created_since`` split the table at the
    moment the latest ingestion started, using each row's own creation time.
    """
    forward_rows = _forward_row_counts(sync)
    started = _latest_ingestion_started(sync)
    entries = []
    for model_string in sorted(enabled_models or ()):
        entry = {
            "model": model_string,
            "forward_rows": forward_rows.get(model_string),
            "netbox_rows": None,
            "rows_before_latest_run": None,
            "rows_created_since": None,
            "duplicate_groups": None,
            "note": "",
        }
        model = _model_class(model_string)
        if model is None:
            entry["note"] = "not installed"
            entries.append(entry)
            continue
        value, skipped = _guarded(lambda model=model: model.objects.count())
        if skipped:
            entry["note"] = skipped
            entries.append(entry)
            continue
        entry["netbox_rows"] = value
        has_created = any(field.name == "created" for field in model._meta.get_fields())
        if started is not None and has_created:
            before, skipped = _guarded(
                lambda model=model: model.objects.filter(created__lt=started).count()
            )
            if skipped:
                entry["note"] = skipped
            else:
                entry["rows_before_latest_run"] = before
                entry["rows_created_since"] = value - before
        key = DUPLICATE_KEYS.get(model_string)
        if key:
            groups, skipped = _guarded(
                lambda model=model, key=key: model.objects.order_by()
                .values(*key)
                .annotate(total=Count("pk"))
                .filter(total__gt=1)
                .count()
            )
            if skipped:
                entry["note"] = (entry["note"] + " " + skipped).strip()
            else:
                entry["duplicate_groups"] = groups
        entries.append(entry)
    return {
        "latest_run_started": _iso(started),
        "models": entries,
        "note": (
            "netbox_rows is the whole NetBox table, including rows this sync "
            "does not manage."
        ),
    }


def _merge_facts(ingestion):
    """How the ingestion was (or was not) merged.

    An Auto merge runs inside the sync job itself, so ``merge_job`` stays empty
    for it: that field is only set when a merge is queued from the UI. An empty
    ``merge_job`` on an ingestion with a merge timestamp is therefore an
    automatic merge, not a merge that never happened.
    """
    merge_job = ingestion.merge_job
    if merge_job is not None:
        mode = "queued_merge_job"
    elif ingestion.merge_applied_at is not None:
        mode = "automatic_inside_sync_job"
    else:
        mode = "not_merged"
    return {
        "mode": mode,
        "merge_job_status": getattr(merge_job, "status", None),
        "merge_job_started": _iso(getattr(merge_job, "started", None)),
        "merge_job_completed": _iso(getattr(merge_job, "completed", None)),
        "merge_applied_at": _iso(ingestion.merge_applied_at),
        "merge_finalized_at": _iso(ingestion.merge_finalized_at),
        "counts": {
            "applied": ingestion.applied_change_count,
            "failed": ingestion.failed_change_count,
            "skipped": ingestion.skipped_change_count,
            "created": ingestion.created_change_count,
            "updated": ingestion.updated_change_count,
            "deleted": ingestion.deleted_change_count,
        },
    }


def _job_statistics(job):
    statistics = (getattr(job, "data", None) or {}).get("statistics") or {}
    if not isinstance(statistics, dict):
        return {}
    return {
        str(model): {
            str(key): value
            for key, value in counts.items()
            if isinstance(value, (int, float))
        }
        for model, counts in statistics.items()
        if isinstance(counts, dict)
    }


def _merged_changes(ingestion):
    """Changes the merge wrote, by ``model.action``, over ALL of them."""
    if not ingestion.change_request_id:
        return {"counts": {}, "total": 0, "note": "no change request id"}
    from core.models import ObjectChange

    def query():
        return list(
            ObjectChange.objects.filter(request_id=ingestion.change_request_id)
            .order_by()
            .values(
                "changed_object_type__app_label",
                "changed_object_type__model",
                "action",
            )
            .annotate(total=Count("pk"))
        )

    rows, skipped = _guarded(query)
    if skipped:
        return {"counts": {}, "total": None, "note": skipped}
    counts = {}
    for row in rows:
        label = (
            f"{row['changed_object_type__app_label']}."
            f"{row['changed_object_type__model']}.{row['action']}"
        )
        counts[label] = counts.get(label, 0) + int(row["total"])
    ordered = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    truncated = len(ordered) > MERGED_CHANGE_ROW_LIMIT
    return {
        "counts": dict(ordered[:MERGED_CHANGE_ROW_LIMIT]),
        "total": sum(counts.values()),
        "note": (
            f"truncated to the {MERGED_CHANGE_ROW_LIMIT} largest of {len(ordered)}"
            if truncated
            else ""
        ),
    }


def recent_ingestions_evidence(sync, *, limit=RECENT_INGESTION_LIMIT):
    ingestions = list(
        sync.forwardingestion_set.select_related("job", "merge_job", "branch").order_by(
            "-pk"
        )[:limit]
    )
    entries = []
    for ingestion in ingestions:
        job = ingestion.job
        entries.append(
            {
                "ingestion": ingestion.pk,
                "sync_mode": ingestion.sync_mode,
                "baseline_ready": ingestion.baseline_ready,
                "snapshot_id": ingestion.snapshot_id,
                "created": _iso(ingestion.created),
                "job_status": getattr(job, "status", None),
                "job_started": _iso(getattr(job, "started", None)),
                "job_completed": _iso(getattr(job, "completed", None)),
                "branch_present": ingestion.branch_id is not None,
                "merge": _merge_facts(ingestion),
                "statistics_by_model": _job_statistics(job),
                "merged_changes_by_model_action": _merged_changes(ingestion),
            }
        )
    return {
        "sync": sync.pk,
        "auto_merge": bool(sync.auto_merge),
        "status": sync.status,
        "ingestions": entries,
    }


def applied_change_history(sync, *, skip_latest=True, limit=6):
    """Changes earlier runs applied, per model, from their job statistics.

    Feeds the typical-run norm. ``skip_latest`` leaves out the newest ingestion
    so a run is never compared with itself.
    """
    queryset = sync.forwardingestion_set.select_related("job").order_by("-pk")
    ingestions = list(queryset[: limit + (1 if skip_latest else 0)])
    if skip_latest:
        ingestions = ingestions[1:]
    history = {}
    for ingestion in ingestions:
        for model, counts in _job_statistics(ingestion.job).items():
            applied = counts.get("applied")
            if isinstance(applied, int) and applied > 0:
                history.setdefault(model, []).append(applied)
    return history
