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


def _merge_accounting(ingestion):
    """What the merge recorded about its staged changes, from whichever job ran it.

    The merge job is checked first: after an auto-merge attempt and a retry it is
    the later record."""
    for job in (ingestion.merge_job, ingestion.job):
        recorded = (getattr(job, "data", None) or {}).get("merge_accounting")
        if isinstance(recorded, dict):
            return recorded
    return None


def _written_deletes(ingestion):
    """Deletes the merge wrote, by model, over ALL of them (never truncated)."""
    if not ingestion.change_request_id:
        return None
    from core.models import ObjectChange

    def query():
        return list(
            ObjectChange.objects.filter(
                request_id=ingestion.change_request_id, action="delete"
            )
            .order_by()
            .values("changed_object_type__app_label", "changed_object_type__model")
            .annotate(total=Count("pk"))
        )

    rows, skipped = _guarded(query)
    if skipped:
        return None
    return {
        f"{row['changed_object_type__app_label']}.{row['changed_object_type__model']}": int(
            row["total"]
        )
        for row in rows
    }


def _uncollected_device_networks(device_ids):
    """`(vrf id, network)` for every address on an interface of these devices."""
    from dcim.models import Interface
    from django.contrib.contenttypes.models import ContentType
    from ipam.models import IPAddress

    interface_ct = ContentType.objects.get_for_model(Interface)
    interface_ids = Interface.objects.filter(device_id__in=device_ids).values("pk")
    networks = set()
    for vrf_id, address in IPAddress.objects.filter(
        assigned_object_type=interface_ct, assigned_object_id__in=interface_ids
    ).values_list("vrf_id", "address"):
        networks.add((vrf_id, str(address.cidr)))
    return networks


def _scope_report_after_run(ingestion):
    """The first completed scope report computed after this run began, or None.

    A run's own cause has to be read from a report that saw the run's result: the
    newest report may describe a later snapshot, and one computed before the run
    describes the world the run replaced."""
    from core.choices import JobStatusChoices
    from core.models import Job
    from django.contrib.contenttypes.models import ContentType
    from django.db.models import Q

    from ..models import ForwardSync

    started = getattr(ingestion.job, "started", None)
    if started is None:
        return None
    return (
        Job.objects.filter(
            Q(name__icontains="scope reconciliation")
            | Q(
                name__icontains="reconcile device scope tags",
                data__has_key="scope_reconciliation",
            ),
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=ingestion.sync_id,
            status=JobStatusChoices.STATUS_COMPLETED,
            completed__gte=started,
        )
        .order_by("completed", "pk")
        .first()
    )


def _prefix_delete_cause(ingestion):
    """Why the merge deleted prefixes: did a device Forward did not collect host them?

    A prefix is derived from the addresses on in-scope device interfaces. When a
    device fails collection its prefixes stop coming back and read as deletes.
    Devices the scope report lists as in scope and uncollected stay in NetBox with
    their addresses, so a deleted prefix that one of those addresses still falls
    in was hosted by an uncollected device. Counts only: no prefix, VRF or device
    is named. Read from the deletes the merge wrote and from the first scope
    report computed after the run, whose time is reported."""
    if not ingestion.change_request_id:
        return None
    if ingestion.merge_applied_at is None and ingestion.merge_job_id is None:
        return {
            "note": (
                "The run is not merged, so its prefix deletes are staged in the "
                "branch and not in the change log yet; no cause was derived."
            )
        }
    from core.models import ObjectChange
    from django.contrib.contenttypes.models import ContentType
    from ipam.models import Prefix

    from .scope_reconciliation import stored_scope_report

    def report():
        job = _scope_report_after_run(ingestion)
        payload, generated_at, error = stored_scope_report(job)
        return job, payload, generated_at, error

    found, skipped = _guarded(report)
    if skipped:
        return {"note": skipped}
    job, payload, generated_at, error = found
    if job is None or error or "present_backfilled_device_ids" not in (payload or {}):
        return {
            "note": (
                "No scope report computed after this run carries the uncollected "
                "devices, so no cause was derived."
                + (f" The report errored: {error}" if error else "")
            )
        }
    device_ids = list(payload["present_backfilled_device_ids"] or [])

    def query():
        networks = _uncollected_device_networks(device_ids) if device_ids else set()
        total = hosted = no_data = 0
        for prefix, vrf in (
            ObjectChange.objects.filter(
                request_id=ingestion.change_request_id,
                action="delete",
                changed_object_type=ContentType.objects.get_for_model(Prefix),
            )
            .values_list("prechange_data__prefix", "prechange_data__vrf")
            .iterator(chunk_size=2000)
        ):
            total += 1
            if prefix is None:
                no_data += 1
            elif (vrf, str(prefix)) in networks:
                hosted += 1
        return total, hosted, no_data

    result, skipped = _guarded(query)
    if skipped:
        return {"note": skipped}
    total, hosted, no_data = result
    return {
        "deleted_prefixes": total,
        "hosted_by_uncollected_device": hosted,
        "not_explained_by_uncollected_devices": total - hosted - no_data,
        "deletes_without_recorded_prefix": no_data,
        "uncollected_devices_in_scope": len(device_ids),
        "scope_report_generated_at": _iso(generated_at),
        "note": (
            "A deleted prefix counts as hosted by an uncollected device when an "
            "address on an interface of an in-scope device that Forward did not "
            "collect still falls in it, in the same VRF. The remainder had no such "
            "address: the device is gone, was retired or re-addressed, or its "
            "address was removed in this run. The report is the first one "
            "computed after the run began; its device list is by device name."
        ),
    }


def _delete_accounting(ingestion, written=None):
    """Reconcile the run's staged deletes with the deletes the merge wrote.

    `deleted` on the ingestion counts LOGICAL deletes staged in the branch. The
    change log counts the deletes the merge wrote. They differ legitimately, so
    this reports the difference per model rather than calling it a loss:

    - a delete a surviving PROTECT reference holds back is staged, never written
      (`held_by_protecting_reference`);
    - a delete of an object already gone from the destination succeeds without a
      change-log row;
    - a MAC address delete goes through a path that writes no change-log row;
    - a delete that failed or was skipped as unsatisfiable (`failed`, `skipped`);
    - a cascade logs deletes under other models, so logged can exceed staged.
    """
    staged = int(ingestion.deleted_change_count or 0)
    out = {
        "staged_deletes": staged,
        "written_deletes": sum(written.values()) if written is not None else None,
        "written_deletes_by_model": (
            dict(sorted(written.items())) if written is not None else None
        ),
        "causes_of_a_difference": [
            "held by a protecting reference (recorded below)",
            "object already absent from the destination (no change-log row)",
            "MAC address deletes (no change-log row)",
            "failed or skipped delete (recorded below)",
            "cascade: logged under other models, so logged can exceed staged",
        ],
    }
    notes = []
    if written is None:
        notes.append("The change-log delete counts were not available.")
    recorded = _merge_accounting(ingestion)
    out["recorded_at_merge"] = recorded is not None
    if recorded is None:
        notes.append(
            "Staged deletes by model and protected-reference holds were not "
            "recorded for this ingestion (it ran before the record existed, it "
            "merged with no staged changes, or it was merged outside a job)."
        )
        out["note"] = " ".join(notes)
        return out
    staged_by_model = {
        label.rsplit(".", 1)[0]: count
        for label, count in (recorded.get("logical_action_by_model") or {}).items()
        if label.endswith(".delete")
    }
    out["staged_deletes_by_model"] = dict(sorted(staged_by_model.items()))
    out["held_by_protecting_reference"] = recorded.get("held_deletes_by_model") or {}
    out["failed_at_merge"] = int(recorded.get("failed") or 0)
    out["skipped_at_merge"] = int(recorded.get("skipped") or 0)
    if written is not None:
        models = sorted(set(staged_by_model) | set(written))
        out["staged_more_than_logged"] = {
            model: staged_by_model.get(model, 0) - written.get(model, 0)
            for model in models
            if staged_by_model.get(model, 0) > written.get(model, 0)
        }
        out["logged_more_than_staged"] = {
            model: written.get(model, 0) - staged_by_model.get(model, 0)
            for model in models
            if written.get(model, 0) > staged_by_model.get(model, 0)
        }
    if notes:
        out["note"] = " ".join(notes)
    return out


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
                "delete_accounting": _delete_accounting(
                    ingestion, _written_deletes(ingestion)
                ),
                "prefix_delete_cause": _prefix_delete_cause(ingestion),
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
