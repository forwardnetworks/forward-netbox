import logging
import traceback
from datetime import datetime
from datetime import timedelta

from core.choices import JobStatusChoices
from core.exceptions import JobFailed
from core.exceptions import SyncError
from core.models import Job
from django.db import IntegrityError
from django.db import transaction
from django_pglocks import advisory_lock
from netbox.constants import ADVISORY_LOCK_KEYS
from netbox.context_managers import event_tracking
from netbox.jobs import JobRunner
from netbox_branching.choices import BranchStatusChoices
from netbox_branching.models import Branch
from rq.timeouts import JobTimeoutException
from utilities.datetime import local_now
from utilities.request import NetBoxFakeRequest

from .choices import ForwardIngestionPhaseChoices
from .choices import ForwardSyncStatusChoices
from .exceptions import ForwardOwnershipDispatchError
from .exceptions import ForwardPartialMergeError
from .exceptions import ForwardSyncError
from .models import ForwardIngestion
from .models import ForwardIngestionIssue
from .models import ForwardSync
from .utilities.job_queue import enqueue_forward_job
from .utilities.json_safe import json_safe_value
from .utilities.logging import SyncLogging
from .utilities.post_sync import StalePostSyncSnapshotError
from .utilities.validation import ForwardValidationRunner

logger = logging.getLogger(__name__)
STAGE_LIVENESS_HEARTBEAT_SECONDS = 30
STAGE_LIVENESS_LOG_SECONDS = 300
# Ceiling on automatic stage-job retries for transient DB-connection pressure.
# Without it, sustained pressure (e.g. "too many clients already" on a large
# fabric) requeues the same shard forever. After this many retries we stop and
# fail the run so an operator can intervene instead of spinning indefinitely.
STAGE_DB_RETRY_LIMIT = 5
# Ceiling on automatic retries when a resumed shard's claimed index cannot be
# resolved against the rebuilt plan (e.g. a transient query failure truncated
# the rebuild). A bounded retry re-runs the plan build — usually resolving once
# the transient condition clears — instead of hard-crashing the whole sync.
STAGE_SHARD_RESOLUTION_RETRY_LIMIT = 3
JOB_NOTIFICATION_UNIQUE_CONSTRAINT = "extras_notification_unique_per_object_and_user"


def _resolve_request_user(*, sync, job=None):
    if job is not None and getattr(job, "user", None) is not None:
        return job.user
    if getattr(sync, "user", None) is not None:
        return sync.user
    raise ForwardSyncError(
        "Forward sync has no invoking user or owner. Edit the sync as the intended "
        "owner, then retry so every inventory write has durable attribution."
    )


def _normalize_job_log_level(level):
    return {
        "success": "info",
        "failure": "error",
    }.get(level, level)


def _build_job_log_entries(log_data):
    entries = []
    for entry in (log_data or {}).get("logs", []):
        if not isinstance(entry, (list, tuple)) or len(entry) < 5:
            continue
        timestamp = entry[0]
        if isinstance(timestamp, str):
            try:
                timestamp = datetime.fromisoformat(timestamp)
            except ValueError:
                timestamp = local_now()
        entries.append(
            {
                "timestamp": timestamp,
                "level": _normalize_job_log_level(entry[1]),
                "message": entry[4],
            }
        )
    return entries


def safe_save_job_data(job, obj_with_logger):
    try:
        obj_with_logger.logger.flush()
        log_data = json_safe_value(obj_with_logger.logger.log_data)
        job.data = log_data
        job.log_entries = _build_job_log_entries(log_data)
        job.save(update_fields=["data", "log_entries"])
    except JobTimeoutException:
        raise
    except Exception as exc:
        logger.warning("Failed to save job data for job %s: %s", job.pk, exc)


def start_job_once(job):
    """Start a persisted NetBox job without reviving a terminal row."""
    if not isinstance(job, Job):
        job.start()
        return True

    with transaction.atomic():
        try:
            persisted_job = Job.objects.select_for_update().get(pk=job.pk)
        except Job.DoesNotExist:
            return False
        if (
            persisted_job.status
            not in (
                JobStatusChoices.STATUS_PENDING,
                JobStatusChoices.STATUS_SCHEDULED,
            )
            or persisted_job.started is not None
        ):
            job.status = persisted_job.status
            job.started = persisted_job.started
            job.completed = persisted_job.completed
            job.error = persisted_job.error
            return False
        persisted_job.start()
        job.status = persisted_job.status
        job.started = persisted_job.started
    return True


def _merge_job_runtime_evidence(persisted_job, job, *, worker_error=None):
    update_fields = []
    persisted_data = persisted_job.data
    worker_data = json_safe_value(job.data)
    if isinstance(persisted_data, dict) and isinstance(worker_data, dict):
        merged_data = {**persisted_data, **worker_data}
    elif worker_data not in (None, {}, []):
        merged_data = worker_data
    else:
        merged_data = persisted_data
    if worker_error:
        if not isinstance(merged_data, dict):
            merged_data = (
                {"worker_data": merged_data} if merged_data is not None else {}
            )
        merged_data = {
            **merged_data,
            "worker_terminal_error": str(worker_error),
        }
    if merged_data != persisted_job.data:
        persisted_job.data = merged_data
        update_fields.append("data")

    merged_logs = list(persisted_job.log_entries or [])
    for entry in job.log_entries or []:
        if entry not in merged_logs:
            merged_logs.append(entry)
    if merged_logs != list(persisted_job.log_entries or []):
        persisted_job.log_entries = merged_logs
        update_fields.append("log_entries")
    return update_fields


def terminate_job_once(
    job,
    status=JobStatusChoices.STATUS_COMPLETED,
    error=None,
):
    """Terminate a NetBox job without duplicating terminal notifications.

    Recovery and timeout handling can make the persisted job terminal while a
    long-running worker still holds a stale in-memory instance. NetBox's
    ``Job.terminate()`` always inserts a notification, so calling it again
    raises the unique notification constraint and obscures the actual sync
    outcome. Refresh first and tolerate only that exact terminal race.
    """
    if not isinstance(job, Job):
        if status == JobStatusChoices.STATUS_COMPLETED:
            job.terminate()
        else:
            job.terminate(status=status)
        return True

    try:
        with transaction.atomic():
            persisted_job = Job.objects.select_for_update().get(pk=job.pk)
            if persisted_job.status in JobStatusChoices.TERMINAL_STATE_CHOICES:
                update_fields = _merge_job_runtime_evidence(
                    persisted_job,
                    job,
                    worker_error=error,
                )
                if update_fields:
                    persisted_job.save(update_fields=update_fields)
                job.status = persisted_job.status
                job.completed = persisted_job.completed
                job.error = persisted_job.error
                job.data = persisted_job.data
                job.log_entries = persisted_job.log_entries
                return False
            _merge_job_runtime_evidence(persisted_job, job)
            persisted_job.interval = job.interval
            if error is None:
                if status == JobStatusChoices.STATUS_COMPLETED:
                    persisted_job.terminate()
                else:
                    persisted_job.terminate(status=status)
            else:
                persisted_job.terminate(status=status, error=error)
            job.status = persisted_job.status
            job.completed = persisted_job.completed
            job.error = persisted_job.error
    except IntegrityError as exc:
        if JOB_NOTIFICATION_UNIQUE_CONSTRAINT not in str(exc):
            raise
        job.refresh_from_db(fields=["status", "completed"])
        if job.status not in JobStatusChoices.TERMINAL_STATE_CHOICES:
            raise
        logger.info(
            "Job %s became terminal before duplicate notification handling; "
            "preserving status %s.",
            job.pk,
            job.status,
        )
        return False
    return True


def record_timeout_issue(ingestion, phase, message):
    if ingestion is None:
        return None
    existing = ForwardIngestionIssue.objects.filter(
        ingestion=ingestion,
        phase=phase,
        exception=JobTimeoutException.__name__,
    ).first()
    if existing:
        return existing
    return ForwardIngestionIssue.objects.create(
        ingestion=ingestion,
        phase=phase,
        message=message,
        exception=JobTimeoutException.__name__,
        raw_data={},
        coalesce_fields={},
        defaults={},
    )


def _sync_has_active_job(sync, name, *, exclude_job_id=None):
    """True if a pending/running job with ``name`` already exists for ``sync``.

    Post-sync overlays are enqueued after EVERY sync; a slow/large overlay (e.g.
    the vsys parent-link's full-network fetch) can still be running when the next
    sync fires, so without this guard duplicate jobs pile up in PENDING behind it
    and look 'hung'. Skipping the enqueue when one is already active keeps at most
    one overlay of each kind queued per sync.
    """
    active_jobs = sync.jobs.filter(
        name=name,
        status__in=[
            JobStatusChoices.STATUS_PENDING,
            JobStatusChoices.STATUS_RUNNING,
        ],
    )
    if exclude_job_id is not None:
        active_jobs = active_jobs.exclude(pk=exclude_job_id)
    return active_jobs.exists()


def _maybe_enqueue_device_analysis_refresh(
    sync,
    *,
    snapshot_id=None,
    ingestion_id=None,
    exclude_job_id=None,
):
    """Opt-in: after a successful sync, refresh the device-analysis overlay.

    Enabled per sync via the ``auto_refresh_device_analysis`` parameter. Never
    lets an analysis-refresh problem affect the sync result.
    """
    snapshot_id = str(snapshot_id or "").strip()
    if (
        sync.status != ForwardSyncStatusChoices.COMPLETED
        or not snapshot_id
        or not (sync.parameters or {}).get("auto_refresh_device_analysis")
    ):
        return
    try:
        name = f"{sync.name} - refresh device analysis (auto)"
        if _sync_has_active_job(sync, name, exclude_job_id=exclude_job_id):
            return
        return DeviceAnalysisRefreshJob.enqueue(
            instance=sync,
            user=sync.user,
            name=name,
            snapshot_id=snapshot_id,
            ingestion_id=ingestion_id,
        )
    except JobTimeoutException:
        raise
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Auto device-analysis refresh enqueue failed: %s", exc)


def _maybe_enqueue_backfilled_tag_refresh(
    sync,
    *,
    snapshot_id=None,
    ingestion_id=None,
    exclude_job_id=None,
):
    """Reconcile all plugin-managed device tags after a successful sync."""
    snapshot_id = str(snapshot_id or "").strip()
    if sync.status != ForwardSyncStatusChoices.COMPLETED or not snapshot_id:
        return None
    name = f"{sync.name} - reconcile device scope tags (auto)"
    if _sync_has_active_job(sync, name, exclude_job_id=exclude_job_id):
        return None
    return DeviceScopeTagReconciliationJob.enqueue(
        instance=sync,
        user=sync.user,
        name=name,
        snapshot_id=snapshot_id,
        ingestion_id=ingestion_id,
    )


def _maybe_enqueue_vsys_parent_link(
    sync,
    *,
    snapshot_id=None,
    ingestion_id=None,
    exclude_job_id=None,
):
    """After a successful sync, link virtual-context firewalls (Palo vsys /
    Fortinet vdom) to their physical chassis via the ``forward_parent_device``
    custom field. Non-destructive, idempotent, and never affects the sync result.

    Runs by DEFAULT (unlike the opt-in overlays): a blank ``Parent Device`` on
    every vsys/vdom is a confusing default, so the link auto-refreshes each sync
    unless the sync explicitly opts out with ``auto_link_vsys_parents=False``.
    """
    snapshot_id = str(snapshot_id or "").strip()
    from .models import ForwardOwnershipReconciliation
    from .models import ForwardVirtualParentClaim

    disabled = (sync.parameters or {}).get("auto_link_vsys_parents") is False
    has_parent_ownership = (
        ForwardVirtualParentClaim.objects.filter(sync=sync).exists()
        or ForwardOwnershipReconciliation.objects.filter(
            sync=sync,
            domain=ForwardOwnershipReconciliation.Domain.VIRTUAL_PARENTS,
        ).exists()
    )
    if (
        sync.status != ForwardSyncStatusChoices.COMPLETED
        or not snapshot_id
        or (disabled and not has_parent_ownership)
    ):
        return None
    name = f"{sync.name} - link vsys/vdom parents (auto)"
    if _sync_has_active_job(sync, name, exclude_job_id=exclude_job_id):
        return None
    return VirtualParentReconciliationJob.enqueue(
        instance=sync,
        user=sync.user,
        name=name,
        snapshot_id=snapshot_id,
        ingestion_id=ingestion_id,
    )


def _enqueue_post_sync_overlays(
    sync,
    *,
    snapshot_id=None,
    ingestion_id=None,
    exclude_job_id=None,
):
    """Persist and dispatch required post-merge ownership work."""
    try:
        sync.refresh_from_db(fields=["status"])
        if sync.status != ForwardSyncStatusChoices.COMPLETED:
            return {"scheduled": False, "reason": "sync_not_completed"}
        snapshot_id = str(snapshot_id or "").strip()
        baseline = sync.latest_baseline_ingestion()
        if not snapshot_id:
            snapshot_id = str(getattr(baseline, "snapshot_id", "") or "").strip()
        if ingestion_id is None:
            ingestion_id = getattr(baseline, "pk", None)
        from .utilities.ownership import mark_ownership_pending
        from .utilities.ownership import required_ownership_domains

        domains = required_ownership_domains(sync)
        if domains and ingestion_id is None:
            raise RuntimeError(
                "Ownership reconciliation cannot be dispatched without a "
                "baseline ingestion."
            )
        if ingestion_id is not None:
            mark_ownership_pending(sync, ingestion_id, snapshot_id)
        analysis_job = _maybe_enqueue_device_analysis_refresh(
            sync,
            snapshot_id=snapshot_id,
            ingestion_id=ingestion_id,
            exclude_job_id=exclude_job_id,
        )
        tag_job = _maybe_enqueue_backfilled_tag_refresh(
            sync,
            snapshot_id=snapshot_id,
            ingestion_id=ingestion_id,
            exclude_job_id=exclude_job_id,
        )
        parent_job = _maybe_enqueue_vsys_parent_link(
            sync,
            snapshot_id=snapshot_id,
            ingestion_id=ingestion_id,
            exclude_job_id=exclude_job_id,
        )
        return {
            "scheduled": True,
            "ingestion_id": ingestion_id,
            "domains": domains,
            "tag_job_id": getattr(tag_job, "pk", None),
            "parent_job_id": getattr(parent_job, "pk", None),
            "analysis_job_id": getattr(analysis_job, "pk", None),
        }
    except JobTimeoutException:
        raise
    except Exception as exc:
        logger.exception(
            "Durable post-sync ownership dispatch failed for ForwardSync %s.",
            sync.pk,
        )
        raise ForwardOwnershipDispatchError(
            "Ownership reconciliation is durable but could not be enqueued; "
            "run forward_stuck_job_recover --apply to redispatch it."
        ) from exc


def _finish_completed_job_with_overlays(
    job,
    sync,
    *,
    snapshot_id=None,
    ingestion_id=None,
):
    """Dispatch durable ownership work before making the producer terminal."""
    try:
        dispatch = _enqueue_post_sync_overlays(
            sync,
            snapshot_id=snapshot_id,
            ingestion_id=ingestion_id,
        )
    except ForwardOwnershipDispatchError as exc:
        if getattr(sync, "logger", None) is None:
            sync.logger = SyncLogging(job=job.pk)
        sync.logger.log_failure(str(exc), obj=sync)
        safe_save_job_data(job, sync)
        terminate_job_once(
            job,
            status=JobStatusChoices.STATUS_ERRORED,
            error=str(exc),
        )
        return False
    if not dispatch.get("domains") and dispatch.get("ingestion_id") is not None:
        _reconcile_completed_ingestion_catchup(
            sync,
            dispatch["ingestion_id"],
            current_job=job,
        )
    terminate_job_once(job)
    return True


def _reconcile_completed_ingestion_catchup(sync, ingestion_id, *, current_job=None):
    """Attempt catch-up after a worker may have completed the last overlay."""
    from .utilities.ingestion_merge import reconcile_catchup_if_ownership_complete

    if ingestion_id is None:
        return {"checked": False, "reason": "missing_ingestion", "job_id": None}
    try:
        ingestion = ForwardIngestion.objects.select_related("sync", "sync__source").get(
            pk=ingestion_id,
            sync=sync,
        )
        return reconcile_catchup_if_ownership_complete(
            ingestion,
            current_job=current_job,
        )
    except JobTimeoutException:
        raise
    except Exception:
        logger.exception(
            "Snapshot catch-up check failed after ownership convergence for "
            "ForwardIngestion %s; durable recovery will retry it.",
            ingestion_id,
        )
        return {"checked": False, "reason": "catchup_failed", "job_id": None}


def _mark_overlay_ownership_failed(sync, kwargs, domains, exc):
    from .utilities.ownership import mark_ownership_failed

    generation = kwargs.get("ingestion_id")
    if generation is None:
        baseline = sync.latest_baseline_ingestion()
        generation = getattr(baseline, "pk", None)
    mark_ownership_failed(sync, generation, domains, exc)


def _overlay_job_data(payload, kwargs):
    """Bind overlay result evidence to the ingestion generation it evaluated."""
    data = dict(payload or {})
    generation = kwargs.get("ingestion_id")
    if generation is not None:
        data["forward_ingestion_id"] = int(generation)
    return data


def _complete_stale_post_sync_overlay(job, sync, **kwargs):
    """Complete an obsolete overlay and request the latest safe catch-up."""
    baseline = sync.latest_baseline_ingestion()
    latest_snapshot_id = str(getattr(baseline, "snapshot_id", "") or "").strip()
    latest_ingestion_id = getattr(baseline, "pk", None)
    job.data = _overlay_job_data(
        {
            "skipped": "stale_post_sync_snapshot",
            "catch_up_requested": bool(latest_snapshot_id),
        },
        kwargs,
    )
    job.save(update_fields=["data"])
    if latest_snapshot_id:
        try:
            _enqueue_post_sync_overlays(
                sync,
                snapshot_id=latest_snapshot_id,
                ingestion_id=latest_ingestion_id,
                exclude_job_id=job.pk,
            )
        except ForwardOwnershipDispatchError:
            logger.exception(
                "Latest ownership generation remains pending after stale overlay %s.",
                job.pk,
            )
            return


def sync_forwardsync(job, *args, **kwargs):
    sync = ForwardSync.objects.get(pk=job.object_id)
    with advisory_lock(ADVISORY_LOCK_KEYS["job-schedules"]):
        if not start_job_once(job):
            return

    try:
        execution_performed = sync.sync(
            job=job,
            force_unchanged=bool(kwargs.get("force_unchanged")),
        )
        safe_save_job_data(job, sync)
        if sync.status in (
            ForwardSyncStatusChoices.FAILED,
            ForwardSyncStatusChoices.TIMEOUT,
        ):
            terminate_job_once(
                job,
                status=JobStatusChoices.STATUS_ERRORED,
                error=f"Forward sync ended with status {sync.status}.",
            )
            return
        if execution_performed is False:
            terminate_job_once(job)
            return
        _finish_completed_job_with_overlays(job, sync)
    except Exception as exc:
        timeout = isinstance(exc, JobTimeoutException)
        ForwardSync.objects.filter(pk=sync.pk).update(
            status=(
                ForwardSyncStatusChoices.TIMEOUT
                if timeout
                else ForwardSyncStatusChoices.FAILED
            )
        )
        sync.status = (
            ForwardSyncStatusChoices.TIMEOUT
            if timeout
            else ForwardSyncStatusChoices.FAILED
        )
        if timeout:
            ingestion = (
                ForwardIngestion.objects.filter(sync=sync).order_by("-pk").first()
            )
            message = "Forward sync job timed out. Increase RQ worker timeout and rerun the sync."
            record_timeout_issue(
                ingestion,
                ForwardIngestionPhaseChoices.SYNC,
                message,
            )
            sync.logger.log_failure(message, obj=sync)
        expected_failure = isinstance(
            exc,
            (ForwardSyncError, SyncError, JobTimeoutException),
        )
        if expected_failure:
            logger.error(exc)
        safe_save_job_data(job, sync)
        terminate_job_once(
            job,
            status=JobStatusChoices.STATUS_ERRORED,
            error=repr(exc),
        )
        if timeout:
            raise
        if not expected_failure:
            raise
    finally:
        _reconcile_sync_run_schedules(
            sync,
            job,
            adhoc=bool(kwargs.get("adhoc")),
        )


def _reconcile_sync_run_schedules(sync, job, *, adhoc):
    """Restore both sync recurrence and standing schedules after an occurrence.

    Stuck-run recovery calls this same function after terminating a hard-killed
    producer so the recovery path cannot silently drop either schedule chain.
    """
    if sync.interval and not adhoc:
        new_scheduled_time = local_now() + timedelta(minutes=sync.interval)
        sync.refresh_from_db()
        from .utilities.sync_facade import sync_run_job_names

        job_anchor = job.started or job.created
        should_skip = not sync.scheduled or (
            sync.scheduled
            and job_anchor is not None
            and sync.scheduled > job_anchor
            # Name-scoped to sync RUNS: standing-schedule rows are permanently
            # SCHEDULED and would satisfy a status-only check.
            and sync.jobs.filter(
                status__in=[
                    JobStatusChoices.STATUS_SCHEDULED,
                    JobStatusChoices.STATUS_PENDING,
                    JobStatusChoices.STATUS_RUNNING,
                ],
                name__in=sync_run_job_names(sync),
            )
            .exclude(pk=job.pk)
            .exists()
        )
        if should_skip:
            logger.info(
                "Not scheduling a new job for ForwardSync %s because scheduling changed while the current job was running.",
                sync.pk,
            )
        if not should_skip:
            request = NetBoxFakeRequest(
                {
                    "META": {},
                    "POST": sync.parameters,
                    "GET": {},
                    "FILES": {},
                    "user": _resolve_request_user(sync=sync, job=job),
                    "path": "",
                    "id": job.job_id,
                }
            )

            with event_tracking(request):
                sync.scheduled = new_scheduled_time
                sync.full_clean()
                sync.save()
            logger.info(
                "Scheduled next sync for ForwardSync %s at %s.",
                sync.pk,
                new_scheduled_time,
            )
    # Self-heal standing schedules: core recurrence lives in JobRunner
    # handle()'s finally, so a hard-killed worker mid-occurrence silently drops
    # the chain. This is a no-op while the chain is healthy.
    try:
        from .utilities.sync_facade import reconcile_standing_schedules

        sync.refresh_from_db(fields=["parameters"])
        reconcile_standing_schedules(sync)
    except JobTimeoutException:
        raise
    except Exception:
        logger.warning(
            "Standing-schedule reconcile failed for ForwardSync %s.",
            sync.pk,
            exc_info=True,
        )


def _trim_validation_runs(sync):
    """Retention for recurring validation: keep the newest N runs per sync.

    NetBox job housekeeping prunes old Job rows but the runs (job FK
    SET_NULL) would accumulate forever under a standing schedule. Configure
    with PLUGINS_CONFIG["forward_netbox"]["validation_run_retention"]
    (default 100; 0 disables trimming)."""
    from .choices import forward_plugin_settings
    from .models import ForwardValidationRun

    keep = int(forward_plugin_settings().get("validation_run_retention", 100) or 0)
    if keep <= 0:
        return
    stale_pks = (
        ForwardValidationRun.objects.filter(sync=sync)
        .order_by("-pk")
        .values_list("pk", flat=True)[keep:]
    )
    if stale_pks:
        deleted, _ = ForwardValidationRun.objects.filter(
            pk__in=list(stale_pks)
        ).delete()
        logger.info(
            "Trimmed %s old validation runs for ForwardSync %s (retention %s).",
            deleted,
            sync.pk,
            keep,
        )


def _validate_forwardsync_work(job):
    """Run validation for a JobRunner-managed sync job."""
    from .utilities.api_usage import record_forward_api_usage

    sync = ForwardSync.objects.get(pk=job.object_id)
    client = None
    try:
        sync.logger = SyncLogging(job=job.pk)
        client = sync.source.get_client()
        validation_run = ForwardValidationRunner(
            sync,
            client,
            sync.logger,
            job=job,
        ).run_query_validation()
        record_forward_api_usage(sync, client)
        safe_save_job_data(job, sync)
        # Keep the job bound to the SYNC. The pre-2.6 rebind of
        # object_type/object_id to the validation run would make JobRunner
        # recurrence re-enqueue with instance=job.object and silently re-target
        # the run instead of the sync; nothing consumed the rebind, so the run
        # is exposed via job.data instead.
        job.data = {**(job.data or {}), "validation_run_id": validation_run.pk}
        job.save(update_fields=["data"])
        try:
            _trim_validation_runs(sync)
        except JobTimeoutException:
            raise
        except Exception:
            # Housekeeping must never mark a successful validation ERRORED.
            logger.warning(
                "Validation-run retention trim failed for sync %s.",
                sync.pk,
                exc_info=True,
            )
    except Exception as exc:
        if client is not None:
            record_forward_api_usage(sync, client)
        safe_save_job_data(job, sync)
        if type(exc) in (SyncError, JobTimeoutException):
            logger.error(exc)
        raise


def _prune_forward_orphans_work(job):
    """Run reviewed orphan pruning for a JobRunner-managed sync job."""
    from .utilities.scope_reconciliation import compute_scope_reconciliation
    from .utilities.scope_reconciliation import EmptyForwardScopeError
    from .utilities.scope_reconciliation import prune_orphan_devices
    from .utilities.scope_reconciliation import prune_orphan_sites

    sync = ForwardSync.objects.get(pk=job.object_id)
    try:
        report = compute_scope_reconciliation(sync)
        device_result = prune_orphan_devices(sync, report=report)
        site_result = prune_orphan_sites(sync, report=report)
        job.data = {
            "pruned_device_count": device_result.get("pruned_device_count", 0),
            "pruned_object_count": device_result.get("pruned_object_count", 0),
            "out_of_scope_sample": device_result.get("out_of_scope_sample", []),
            "pruned_site_count": site_result.get("pruned_site_count", 0),
            "ownership_blocked_device_count": device_result.get(
                "ownership_blocked_device_count", 0
            ),
            "protected_device_count": device_result.get("protected_device_count", 0),
            "protected_by_model": device_result.get("protected_by_model", {}),
        }
        job.save(update_fields=["data"])
    except EmptyForwardScopeError as exc:
        job.data = {"error": str(exc)}
        job.save(update_fields=["data"])
        logger.error(exc)
        raise
    except Exception as exc:
        # Record the failure on the job so it is visible in the UI (the Data panel)
        # instead of an empty Error field with null data.
        job.data = {
            "error": str(exc) or exc.__class__.__name__,
            "error_type": exc.__class__.__name__,
        }
        job.save(update_fields=["data"])
        if type(exc) in (SyncError, JobTimeoutException):
            logger.error(exc)
        raise


def _refresh_forward_device_analysis_work(job, *args, **kwargs):
    """Background refresh of per-device Forward analysis (reachability proxy,
    connectivity-degree blast radius, CVE exposure) into ForwardDeviceAnalysis."""
    from .utilities.device_analysis import refresh_device_analysis

    sync = ForwardSync.objects.get(pk=job.object_id)
    try:
        job.data = _overlay_job_data(
            refresh_device_analysis(
                sync,
                snapshot_id=kwargs.get("snapshot_id"),
                ingestion_id=kwargs.get("ingestion_id"),
            ),
            kwargs,
        )
        job.save(update_fields=["data"])
    except StalePostSyncSnapshotError:
        _complete_stale_post_sync_overlay(job, sync, **kwargs)
    except Exception as exc:
        # Record the failure on the job so it is visible in the UI (the Data
        # panel) instead of an empty Error field with null data.
        job.data = _overlay_job_data(
            {
                "error": str(exc) or exc.__class__.__name__,
                "error_type": exc.__class__.__name__,
            },
            kwargs,
        )
        job.save(update_fields=["data"])
        if type(exc) in (SyncError, JobTimeoutException):
            logger.error(exc)
        raise


def _reconcile_forward_device_scope_tags_work(job, *args, **kwargs):
    """Background sync of the ``forward-backfilled`` tag for a sync.

    Runs as a job because it issues a live Forward scope query and may tag/untag
    many devices (with change-logging signals), which can exceed an HTTP gateway
    timeout on large fabrics.
    """
    from .utilities.scope_reconciliation import tag_backfilled_devices

    sync = ForwardSync.objects.get(pk=job.object_id)
    try:
        job.data = _overlay_job_data(
            tag_backfilled_devices(
                sync,
                snapshot_id=kwargs.get("snapshot_id"),
                ingestion_id=kwargs.get("ingestion_id"),
            ),
            kwargs,
        )
        job.save(update_fields=["data"])
        _reconcile_completed_ingestion_catchup(
            sync,
            kwargs.get("ingestion_id"),
            current_job=job,
        )
    except StalePostSyncSnapshotError:
        _complete_stale_post_sync_overlay(job, sync, **kwargs)
    except Exception as exc:
        from .models import ForwardOwnershipReconciliation

        _mark_overlay_ownership_failed(
            sync,
            kwargs,
            [
                ForwardOwnershipReconciliation.Domain.SCOPE_TAGS,
                ForwardOwnershipReconciliation.Domain.STATUS_TAGS,
            ],
            exc,
        )
        # Record the failure on the job so it is visible in the UI (the Data
        # panel) instead of an empty Error field with null data.
        job.data = _overlay_job_data(
            {
                "error": str(exc) or exc.__class__.__name__,
                "error_type": exc.__class__.__name__,
            },
            kwargs,
        )
        job.save(update_fields=["data"])
        if type(exc) in (SyncError, JobTimeoutException):
            logger.error(exc)
        raise


def _link_forward_vsys_parents_work(job, *args, **kwargs):
    """Background linkage of virtual-context firewalls (Palo vsys / Fortinet vdom)
    to their physical chassis via the ``forward_parent_device`` custom field.

    Runs as a job because it issues a live Forward query and may update many
    devices. Non-destructive — only sets/clears the custom field.
    """
    from .utilities.logging import SyncLogging
    from .utilities.vsys_parent import link_vsys_parents

    sync = ForwardSync.objects.get(pk=job.object_id)
    try:
        client = sync.source.get_client()
        job.data = _overlay_job_data(
            link_vsys_parents(
                sync,
                client,
                SyncLogging(),
                snapshot_id=kwargs.get("snapshot_id"),
                ingestion_id=kwargs.get("ingestion_id"),
            ),
            kwargs,
        )
        job.save(update_fields=["data"])
        _reconcile_completed_ingestion_catchup(
            sync,
            kwargs.get("ingestion_id"),
            current_job=job,
        )
    except StalePostSyncSnapshotError:
        _complete_stale_post_sync_overlay(job, sync, **kwargs)
    except Exception as exc:
        from .models import ForwardOwnershipReconciliation

        _mark_overlay_ownership_failed(
            sync,
            kwargs,
            [ForwardOwnershipReconciliation.Domain.VIRTUAL_PARENTS],
            exc,
        )
        job.data = _overlay_job_data(
            {
                "error": str(exc) or exc.__class__.__name__,
                "error_type": exc.__class__.__name__,
            },
            kwargs,
        )
        job.save(update_fields=["data"])
        if type(exc) in (SyncError, JobTimeoutException):
            logger.error(exc)
        raise


def _tag_delete_eligible_ipam_work(job):
    """Run tag-only delete-eligibility reconciliation for a sync job."""
    from .utilities.logging import SyncLogging
    from .utilities.scope_ipam_audit import tag_delete_eligible_ipam

    sync = ForwardSync.objects.get(pk=job.object_id)
    try:
        client = sync.source.get_client()
        job.data = tag_delete_eligible_ipam(sync, client, SyncLogging())
        job.save(update_fields=["data"])
    except Exception as exc:
        # Record the failure on the job so it is visible in the UI (the Data
        # panel) instead of an empty Error field with null data.
        job.data = {
            "error": str(exc) or exc.__class__.__name__,
            "error_type": exc.__class__.__name__,
        }
        job.save(update_fields=["data"])
        if type(exc) in (SyncError, JobTimeoutException):
            logger.error(exc)
        raise


def _dependency_preview_work(job):
    """Run a dependency preview for a JobRunner-managed sync job."""
    from .views import _dependency_dry_run_payload

    sync = ForwardSync.objects.get(pk=job.object_id)
    client = None
    try:
        sync.logger = SyncLogging(job=job.pk)
        client = sync.source.get_client()
        payload = _dependency_dry_run_payload(sync, client=client)
        job.data = json_safe_value(payload)
        job.save(update_fields=["data"])
    except Exception as exc:
        from .utilities.api_usage import record_forward_api_usage

        # Record the failure on the job so it is visible in the UI (the Data
        # panel) instead of an empty Error field with null data.
        job.data = {
            "error": str(exc) or exc.__class__.__name__,
            "error_type": exc.__class__.__name__,
            "forward_api_usage": record_forward_api_usage(sync, client),
        }
        job.save(update_fields=["data"])
        if type(exc) in (SyncError, JobTimeoutException):
            logger.error(exc)
        raise


def _complete_recovered_sync_producers(sync, producer_job_pks):
    producer_job_pks = list(dict.fromkeys(producer_job_pks or []))
    if not producer_job_pks:
        from .utilities.sync_facade import reconcile_standing_schedules

        sync.refresh_from_db(fields=["parameters"])
        reconcile_standing_schedules(sync)
        return
    producers = list(Job.objects.filter(pk__in=producer_job_pks))
    Job.objects.filter(
        pk__in=producer_job_pks,
        status__in=JobStatusChoices.ENQUEUED_STATE_CHOICES,
    ).update(
        status=JobStatusChoices.STATUS_COMPLETED,
        completed=local_now(),
        error="",
    )
    scheduled_name = f"{sync.name} - scheduled"
    scheduled = [producer for producer in producers if producer.name == scheduled_name]
    if scheduled:
        producer = max(
            scheduled,
            key=lambda item: (item.started or item.created, item.pk),
        )
        _reconcile_sync_run_schedules(sync, producer, adhoc=False)
    else:
        producer = max(
            producers,
            key=lambda item: (item.started or item.created, item.pk),
        )
        _reconcile_sync_run_schedules(sync, producer, adhoc=True)


def merge_forwardingestion(
    job,
    remove_branch=True,
    recovery_sync_job_pks=None,
    *args,
    **kwargs,
):
    ingestion = ForwardIngestion.objects.get(pk=job.object_id)
    with advisory_lock(ADVISORY_LOCK_KEYS["job-schedules"]):
        if not start_job_once(job):
            return
    try:
        request = NetBoxFakeRequest(
            {
                "META": {},
                "POST": ingestion.sync.parameters,
                "GET": {},
                "FILES": {},
                "user": _resolve_request_user(sync=ingestion.sync, job=job),
                "path": "",
                "id": job.job_id,
            }
        )

        if ingestion.merge_applied_at is not None:
            sync = ingestion.sync
            sync.logger = SyncLogging(job=job.pk)
            from .utilities.ingestion_merge import resume_post_merge_bookkeeping

            if not resume_post_merge_bookkeeping(
                ingestion,
                remove_branch=remove_branch,
            ):
                raise SyncError(
                    "Ingestion branch is not merged; post-merge bookkeeping "
                    "cannot be resumed."
                )
            ingestion.refresh_from_db()
            sync.refresh_from_db()
            sync.logger.log_info(
                "Forward ingestion branch is already merged or no longer present; "
                "post-merge bookkeeping is complete.",
                obj=ingestion,
            )
            safe_save_job_data(job, sync)
            _finish_completed_job_with_overlays(
                job,
                sync,
                snapshot_id=ingestion.snapshot_id,
                ingestion_id=ingestion.pk,
            )
            _complete_recovered_sync_producers(sync, recovery_sync_job_pks)
            return
        if not _claim_ingestion_merge_job(ingestion, job):
            ingestion.sync.logger = SyncLogging(job=job.pk)
            ingestion.sync.logger.log_info(
                "Skipping a stale or duplicate merge job; only the current "
                "ingestion merge job may apply this branch.",
                obj=ingestion,
            )
            safe_save_job_data(job, ingestion.sync)
            terminate_job_once(job)
            return
        if isinstance(job, Job):
            ingestion.merge_job = job
        ingestion.save(update_fields=["merge_job"])
        ingestion.sync.logger = SyncLogging(job=job.pk)
        with event_tracking(request):
            ingestion.sync_merge(remove_branch=remove_branch, claimed_job=job)
        safe_save_job_data(job, ingestion.sync)
        _finish_completed_job_with_overlays(
            job,
            ingestion.sync,
            snapshot_id=ingestion.snapshot_id,
            ingestion_id=ingestion.pk,
        )
        _complete_recovered_sync_producers(
            ingestion.sync,
            recovery_sync_job_pks,
        )
    except Exception as exc:
        timeout = isinstance(exc, JobTimeoutException)
        partial_merge = isinstance(exc, ForwardPartialMergeError)
        merge_not_ready_retryable = _is_merge_not_ready_retryable(exc)
        if not (timeout or partial_merge or merge_not_ready_retryable):
            logger.exception(
                "Error during merge for ForwardIngestion %s: %s", ingestion.pk, exc
            )
        if partial_merge:
            if getattr(ingestion.sync, "logger", None) is None:
                ingestion.sync.logger = SyncLogging(job=job.pk)
            ingestion.sync.logger.log_failure(str(exc), obj=ingestion)
            ingestion.sync.status = ForwardSyncStatusChoices.READY_TO_MERGE
            ForwardSync.objects.filter(pk=ingestion.sync.pk).update(
                status=ForwardSyncStatusChoices.READY_TO_MERGE
            )
            safe_save_job_data(job, ingestion.sync)
            terminate_job_once(
                job,
                status=JobStatusChoices.STATUS_ERRORED,
                error=str(exc),
            )
            logger.error(exc)
            return
        if timeout or merge_not_ready_retryable:
            message = (
                "Forward merge job timed out. Increase RQ worker timeout and rerun the merge."
                if timeout
                else str(exc)
            )
            outcome, branch_status, transitioned = _resolve_authoritative_merge_failure(
                ingestion,
                retry_interrupted=True,
            )
            if outcome == "retryable":
                if timeout:
                    record_timeout_issue(
                        ingestion,
                        ForwardIngestionPhaseChoices.MERGE,
                        message,
                    )
                if transitioned:
                    ingestion.sync.logger.log_info(
                        "Reset the interrupted Branching merge state to ready.",
                        obj=ingestion,
                    )
                if timeout:
                    ingestion.sync.logger.log_failure(message, obj=ingestion)
                else:
                    ingestion.sync.logger.log_info(message, obj=ingestion)
                ingestion.sync.logger.log_info(
                    "The same branch remains ready for an operator merge retry.",
                    obj=ingestion,
                )
            elif outcome == "finalization":
                message = (
                    "Forward branch merge was applied while the interrupted job "
                    "was unwinding; post-merge finalization requires recovery."
                )
                ingestion.sync.logger.log_failure(message, obj=ingestion)
            elif outcome == "finalized":
                message = (
                    "Forward merge finalization completed while the interrupted "
                    "job was unwinding; the completed sync state was preserved."
                )
                ingestion.sync.logger.log_info(message, obj=ingestion)
            else:
                authoritative = str(branch_status or "missing")
                message = (
                    "Forward merge cannot be retried after the interrupted job; "
                    f"the authoritative branch state is {authoritative}."
                )
                if timeout:
                    record_timeout_issue(
                        ingestion,
                        ForwardIngestionPhaseChoices.MERGE,
                        message,
                    )
                ingestion.sync.logger.log_failure(message, obj=ingestion)
            safe_save_job_data(job, ingestion.sync)
            terminate_job_once(
                job,
                status=JobStatusChoices.STATUS_ERRORED,
                error=message,
            )
            logger.warning(exc)
            if timeout:
                raise
            return
        outcome, branch_status, transitioned = _resolve_authoritative_merge_failure(
            ingestion,
            retry_interrupted=False,
        )
        if outcome == "finalization":
            message = (
                "Forward branch merge was applied, but post-merge finalization "
                f"requires recovery: {exc}"
            )
            if getattr(ingestion.sync, "logger", None) is None:
                ingestion.sync.logger = SyncLogging(job=job.pk)
            ingestion.sync.logger.log_failure(message, obj=ingestion)
            safe_save_job_data(job, ingestion.sync)
            terminate_job_once(
                job,
                status=JobStatusChoices.STATUS_ERRORED,
                error=message,
            )
            logger.error(message)
            return
        if outcome == "finalized":
            message = (
                "Forward merge finalization completed before the failed job "
                f"unwound; the completed sync state was preserved: {exc}"
            )
        else:
            message = f"Forward merge job failed: {exc}"
            if transitioned and branch_status == BranchStatusChoices.FAILED:
                ingestion.sync.logger.log_failure(
                    "Marked the authoritative Branching branch failed after a "
                    "non-retryable merge error.",
                    obj=ingestion,
                )
        if getattr(ingestion.sync, "logger", None) is None:
            ingestion.sync.logger = SyncLogging(job=job.pk)
        ingestion.sync.logger.log_failure(message, obj=ingestion)
        safe_save_job_data(job, ingestion.sync)
        terminate_job_once(
            job,
            status=JobStatusChoices.STATUS_ERRORED,
            error=message,
        )
        if type(exc) in (SyncError, JobTimeoutException):
            logger.error(exc)
        else:
            raise


def _claim_ingestion_merge_job(ingestion, job):
    """Claim one ingestion merge using persisted job and sync state."""
    if not isinstance(job, Job):
        return True
    with transaction.atomic():
        locked = (
            ForwardIngestion.objects.select_for_update()
            .select_related("sync")
            .get(pk=ingestion.pk)
        )
        sync = ForwardSync.objects.select_for_update().get(pk=locked.sync_id)
        if locked.merge_job_id not in (None, job.pk):
            return False
        if sync.status == ForwardSyncStatusChoices.MERGING:
            return False
        if locked.merge_job_id is None:
            ForwardIngestion.objects.filter(pk=locked.pk).update(merge_job=job)
            ingestion.merge_job = job
        ForwardSync.objects.filter(pk=sync.pk).update(
            status=ForwardSyncStatusChoices.MERGING
        )
        ingestion.sync.status = ForwardSyncStatusChoices.MERGING
    return True


def _is_merge_not_ready_retryable(exc):
    if not isinstance(exc, SyncError):
        return False
    message = str(exc or "").lower()
    return "not ready to merge" in message and "branch" in message


def _resolve_authoritative_merge_failure(ingestion, *, retry_interrupted):
    """Commit branch and sync recovery state under one lock order."""
    with transaction.atomic():
        locked_ingestion = ForwardIngestion.objects.select_for_update().get(
            pk=ingestion.pk
        )
        locked_sync = ForwardSync.objects.select_for_update().get(
            pk=locked_ingestion.sync_id
        )
        authoritative_branch = (
            Branch.objects.select_for_update()
            .filter(pk=locked_ingestion.branch_id)
            .first()
            if locked_ingestion.branch_id is not None
            else None
        )
        branch_status = (
            str(authoritative_branch.status or "")
            if authoritative_branch is not None
            else None
        )
        if locked_ingestion.merge_finalized_at is not None:
            outcome = "finalized"
            sync_status = ForwardSyncStatusChoices.COMPLETED
            transitioned = False
        else:
            transitioned = branch_status == BranchStatusChoices.MERGING
            if transitioned:
                target_status = (
                    BranchStatusChoices.READY
                    if retry_interrupted
                    else BranchStatusChoices.FAILED
                )
                last_updated = local_now()
                Branch.objects.filter(pk=authoritative_branch.pk).update(
                    status=target_status,
                    last_updated=last_updated,
                )
                branch_status = target_status
                authoritative_branch.status = target_status
                authoritative_branch.last_updated = last_updated

            if retry_interrupted and branch_status == BranchStatusChoices.READY:
                outcome = "retryable"
                sync_status = ForwardSyncStatusChoices.READY_TO_MERGE
            elif branch_status == BranchStatusChoices.MERGED or (
                authoritative_branch is None
                and locked_ingestion.merge_applied_at is not None
            ):
                outcome = "finalization"
                sync_status = ForwardSyncStatusChoices.MERGING
            else:
                outcome = "failed"
                sync_status = ForwardSyncStatusChoices.FAILED

        if locked_sync.status != sync_status:
            ForwardSync.objects.filter(pk=locked_sync.pk).update(status=sync_status)
            locked_sync.status = sync_status

    ingestion.merge_applied_at = locked_ingestion.merge_applied_at
    ingestion.merge_finalized_at = locked_ingestion.merge_finalized_at
    ingestion.sync.status = locked_sync.status
    branch = getattr(ingestion, "branch", None)
    if branch is not None and branch_status is not None:
        branch.status = branch_status
        if authoritative_branch is not None:
            branch.last_updated = authoritative_branch.last_updated
    return outcome, branch_status, transitioned


def _skip_if_immediate_equivalent_active(job, per_sync_suffix):
    """Standing-schedule occurrence guard, three checks in one:

    1. sync deleted -> stop the recurrence chain;
    2. stored intent disagrees with this occurrence -> cancelled (0) stops
       the chain, while a different interval re-aligns it;
    3. an immediate per-sync-named equivalent is pending/running -> skip
       this occurrence instead of stacking a duplicate heavy run.

    Returns True when the occurrence should be skipped."""
    sync = ForwardSync.objects.filter(pk=job.object_id).first()
    if sync is None:
        # Sync deleted out from under the schedule: record why and stop the
        # recurrence (handle()'s finally re-enqueues only when job.interval
        # is set on this in-memory instance).
        job.data = {"skipped": "sync_deleted"}
        job.interval = None
        job.save(update_fields=["data", "interval"])
        logger.warning(
            "Stopping standing '%s' schedule: bound ForwardSync %s no longer exists.",
            job.name,
            job.object_id,
        )
        return True
    if not job.interval:
        return False
    kind = {
        "validation": "validation",
        "dependency preview": "dependency_preview",
    }[job.name]
    from .utilities.sync_facade import STANDING_SCHEDULE_PARAM_KEYS

    key = STANDING_SCHEDULE_PARAM_KEYS[kind]
    parameters = sync.parameters or {}
    desired = int(parameters.get(key) or 0)
    if desired <= 0:
        job.data = {"skipped": "schedule_cancelled"}
        job.interval = None
        job.save(update_fields=["data", "interval"])
        logger.info(
            "Stopping standing '%s' schedule for sync %s: cancelled.",
            job.name,
            sync.pk,
        )
        return True
    if desired != job.interval:
        # Intent changed (e.g. mid-run replace): re-align this chain's
        # recurrence instead of leaving a duplicate interval behind.
        job.interval = desired
        job.save(update_fields=["interval"])
    duplicate = (
        sync.jobs.filter(
            name__startswith=f"{sync.name} - {per_sync_suffix}",
            status__in=[
                JobStatusChoices.STATUS_PENDING,
                JobStatusChoices.STATUS_RUNNING,
            ],
        )
        .exclude(pk=job.pk)
        .first()
    )
    if duplicate is not None:
        job.data = {"skipped": "immediate_equivalent_active", "job_id": duplicate.pk}
        job.save(update_fields=["data"])
        logger.info(
            "Skipping standing '%s' occurrence for sync %s: job %s is already %s.",
            job.name,
            sync.pk,
            duplicate.pk,
            duplicate.status,
        )
        return True
    return False


def _standing_schedule_sync(job):
    if job.name not in {"validation", "dependency preview"}:
        return None
    sync = job.object
    return sync if isinstance(sync, ForwardSync) else None


def _reconcile_terminal_standing_schedule(job):
    sync = _standing_schedule_sync(job)
    if sync is None:
        return
    try:
        from .utilities.sync_facade import STANDING_SCHEDULE_PARAM_KEYS
        from .utilities.sync_facade import reconcile_standing_schedules

        sync.refresh_from_db(fields=["parameters"])
        kind = "validation" if job.name == "validation" else "dependency_preview"
        desired = int(
            (sync.parameters or {}).get(STANDING_SCHEDULE_PARAM_KEYS[kind]) or 0
        )
        schedule_at_by_kind = {}
        if desired > 0:
            now = local_now()
            anchor = job.scheduled or job.started or now
            schedule_at_by_kind[kind] = max(
                anchor + timedelta(minutes=desired),
                now + timedelta(minutes=1),
            )
        reconcile_standing_schedules(
            sync,
            user=job.user,
            schedule_at_by_kind=schedule_at_by_kind,
        )
    except JobTimeoutException:
        raise
    except Exception:
        logger.warning(
            "Standing-schedule recovery failed for terminal job %s.",
            job.pk,
            exc_info=True,
        )


class ForwardJobRunner(JobRunner):
    """NetBox job lifecycle with serialized standing recurrence."""

    @classmethod
    def enqueue(cls, *args, **kwargs):
        name = kwargs.pop("name", None) or cls.name
        return enqueue_forward_job(cls.handle, name=name, *args, **kwargs)

    @classmethod
    def handle(cls, job, *args, **kwargs):
        standing = _standing_schedule_sync(job) is not None
        with advisory_lock(ADVISORY_LOCK_KEYS["job-schedules"]):
            started = start_job_once(job)
            if not started:
                if standing:
                    _reconcile_terminal_standing_schedule(job)
                return None

        status = JobStatusChoices.STATUS_COMPLETED
        error = None
        try:
            cls(job).run(*args, **kwargs)
        except JobFailed:
            logger.warning("Job %s failed", job)
            status = JobStatusChoices.STATUS_FAILED
        except JobTimeoutException as exc:
            traceback_record = logging.makeLogRecord(
                {
                    "levelno": logging.ERROR,
                    "levelname": "ERROR",
                    "msg": traceback.format_exc(),
                }
            )
            job.log(traceback_record)
            status = JobStatusChoices.STATUS_ERRORED
            error = repr(exc)
            logger.error(exc)
            raise
        except Exception as exc:  # noqa: BLE001 - preserve NetBox JobRunner semantics
            traceback_record = logging.makeLogRecord(
                {
                    "levelno": logging.ERROR,
                    "levelname": "ERROR",
                    "msg": traceback.format_exc(),
                }
            )
            job.log(traceback_record)
            status = JobStatusChoices.STATUS_ERRORED
            error = repr(exc)
        finally:
            if standing:
                # Termination and desired-state reconciliation are one critical
                # section. An interval edit or cancellation either happens
                # before both operations or after both, so NetBox's unlocked
                # recurrence window cannot resurrect or duplicate the chain.
                with advisory_lock(ADVISORY_LOCK_KEYS["job-schedules"]):
                    terminate_job_once(job, status=status, error=error)
                    _reconcile_terminal_standing_schedule(job)
            else:
                terminate_job_once(job, status=status, error=error)
        return None


class DependencyPreviewJob(ForwardJobRunner):
    """Recurring-capable dependency preview.

    Standing schedules use the fixed ``Meta.name`` as their serialized identity;
    the name still satisfies the ``icontains "dependency preview"`` lookups on
    the preview/drift pages, and per-sync scoping rides on the ``instance=sync``
    binding. Immediate button/API runs use this same runner with an
    operator-facing per-sync name.
    """

    class Meta:
        name = "dependency preview"

    def run(self, *args, **kwargs):
        if _skip_if_immediate_equivalent_active(self.job, "dependency preview"):
            return
        _dependency_preview_work(self.job)


class ValidationJob(ForwardJobRunner):
    """Recurring-capable sync validation (see DependencyPreviewJob notes)."""

    class Meta:
        name = "validation"

    def run(self, *args, **kwargs):
        if _skip_if_immediate_equivalent_active(self.job, "validation"):
            return
        _validate_forwardsync_work(self.job)


class DeviceAnalysisRefreshJob(ForwardJobRunner):
    """Snapshot-guarded refresh of the auxiliary device-analysis read model."""

    class Meta:
        name = "refresh device analysis"

    def run(self, *args, **kwargs):
        _refresh_forward_device_analysis_work(self.job, *args, **kwargs)


class DeviceScopeTagReconciliationJob(ForwardJobRunner):
    """Generation-guarded materialization of managed scope and status tags."""

    class Meta:
        name = "reconcile device scope tags"

    def run(self, *args, **kwargs):
        _reconcile_forward_device_scope_tags_work(self.job, *args, **kwargs)


class VirtualParentReconciliationJob(ForwardJobRunner):
    """Generation-guarded materialization of virtual-parent ownership."""

    class Meta:
        name = "link vsys/vdom parents"

    def run(self, *args, **kwargs):
        _link_forward_vsys_parents_work(self.job, *args, **kwargs)


class PruneOrphansJob(ForwardJobRunner):
    """Reviewed orphan-prune runner used by the HTML and REST actions."""

    class Meta:
        # Byte-identical to BUTTON_JOB_SPECS["prune_orphans"][1]: the overlap
        # guard's exact-name arm depends on it.
        name = "prune orphans"

    def run(self, *args, **kwargs):
        _prune_forward_orphans_work(self.job)


class TagDeleteEligibleIpamJob(ForwardJobRunner):
    """Tag-only delete-eligibility runner used by HTML and REST actions."""

    class Meta:
        name = "tag delete-eligible IPAM"

    def run(self, *args, **kwargs):
        _tag_delete_eligible_ipam_work(self.job)
