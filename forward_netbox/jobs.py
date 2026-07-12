import logging
from datetime import datetime
from datetime import timedelta

from core.choices import JobStatusChoices
from core.exceptions import SyncError
from core.models import Job
from django.contrib.auth import get_user_model
from netbox.context_managers import event_tracking
from netbox.jobs import JobRunner
from netbox_branching.choices import BranchStatusChoices
from netbox_branching.models import Branch
from rq.timeouts import JobTimeoutException
from utilities.datetime import local_now
from utilities.request import NetBoxFakeRequest

from .choices import ForwardExecutionStepStatusChoices
from .choices import ForwardIngestionPhaseChoices
from .choices import ForwardSyncStatusChoices
from .exceptions import ForwardSyncError
from .models import ForwardIngestion
from .models import ForwardIngestionIssue
from .models import ForwardSync
from .utilities.execution_ledger import active_execution_run
from .utilities.execution_ledger import claim_ingestion_merge_step
from .utilities.execution_ledger import execution_step_for_ingestion
from .utilities.execution_ledger import latest_execution_run
from .utilities.execution_ledger import mark_ingestion_step_merged
from .utilities.execution_ledger import reconcile_execution_run
from .utilities.execution_ledger import update_run_from_branch_state
from .utilities.ingestion_merge import maybe_enqueue_next_branch_stage
from .utilities.json_safe import json_safe_value
from .utilities.logging import SyncLogging
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


def _resolve_request_user(*, sync, job=None):
    if job is not None and getattr(job, "user", None) is not None:
        return job.user
    if getattr(sync, "user", None) is not None:
        return sync.user
    # No invoking or owning user. Inventory-wide writes and their ObjectChange
    # attribution would otherwise SILENTLY run as an arbitrary superuser, leaving
    # an unexplainable audit trail. Keep the run working (a user FK is required
    # downstream) but surface the fallback loudly so an operator assigns a proper
    # sync owner.
    User = get_user_model()
    fallback = (
        User.objects.filter(is_active=True, is_superuser=True).order_by("pk").first()
    )
    logger.warning(
        "Forward sync %s has no invoking or owning user; attributing changes to "
        "fallback superuser '%s'. Assign an owner to the sync so inventory writes "
        "are attributed correctly.",
        getattr(sync, "pk", "?"),
        getattr(fallback, "username", None) or "<none>",
    )
    return fallback


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
        if hasattr(obj_with_logger, "logger") and hasattr(
            obj_with_logger.logger, "log_data"
        ):
            log_data = json_safe_value(obj_with_logger.logger.log_data)
            update_fields = ["data"]
            job.data = log_data
            job.log_entries = _build_job_log_entries(log_data)
            update_fields.append("log_entries")
            job.save(update_fields=update_fields)
    except Exception as exc:
        logger.warning("Failed to save job data for job %s: %s", job.pk, exc)


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


def _sync_has_active_job(sync, name):
    """True if a pending/running job with ``name`` already exists for ``sync``.

    Post-sync overlays are enqueued after EVERY sync; a slow/large overlay (e.g.
    the vsys parent-link's full-network fetch) can still be running when the next
    sync fires, so without this guard duplicate jobs pile up in PENDING behind it
    and look 'hung'. Skipping the enqueue when one is already active keeps at most
    one overlay of each kind queued per sync.
    """
    return sync.jobs.filter(
        name=name,
        status__in=[
            JobStatusChoices.STATUS_PENDING,
            JobStatusChoices.STATUS_RUNNING,
        ],
    ).exists()


def _maybe_enqueue_device_analysis_refresh(sync):
    """Opt-in: after a successful sync, refresh the device-analysis overlay.

    Enabled per sync via the ``auto_refresh_device_analysis`` parameter. Never
    lets an analysis-refresh problem affect the sync result.
    """
    if not (sync.parameters or {}).get("auto_refresh_device_analysis"):
        return
    try:
        from django.utils.module_loading import import_string

        name = f"{sync.name} - refresh device analysis (auto)"
        if _sync_has_active_job(sync, name):
            return
        Job.enqueue(
            import_string("forward_netbox.jobs.refresh_forward_device_analysis"),
            instance=sync,
            user=sync.user,
            name=name,
        )
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Auto device-analysis refresh enqueue failed: %s", exc)


def _maybe_enqueue_backfilled_tag_refresh(sync):
    """Opt-in: after a successful sync, refresh the ``forward-backfilled`` tag.

    Enabled per sync via the ``auto_tag_backfilled`` parameter. Without it the
    tag (and the Collection Gap health signal that counts it) only updates when
    an operator clicks Tag backfilled devices, so the count drifts from reality
    between manual refreshes. Never lets a tag-refresh problem affect the sync
    result.
    """
    if not (sync.parameters or {}).get("auto_tag_backfilled"):
        return
    try:
        from django.utils.module_loading import import_string

        name = f"{sync.name} - tag backfilled devices (auto)"
        if _sync_has_active_job(sync, name):
            return
        Job.enqueue(
            import_string("forward_netbox.jobs.tag_forward_backfilled_devices"),
            instance=sync,
            user=sync.user,
            name=name,
        )
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Auto backfilled-tag refresh enqueue failed: %s", exc)


def _maybe_enqueue_vsys_parent_link(sync):
    """After a successful sync, link virtual-context firewalls (Palo vsys /
    Fortinet vdom) to their physical chassis via the ``forward_parent_device``
    custom field. Non-destructive, idempotent, and never affects the sync result.

    Runs by DEFAULT (unlike the opt-in overlays): a blank ``Parent Device`` on
    every vsys/vdom is a confusing default, so the link auto-refreshes each sync
    unless the sync explicitly opts out with ``auto_link_vsys_parents=False``.
    """
    if (sync.parameters or {}).get("auto_link_vsys_parents") is False:
        return
    try:
        from django.utils.module_loading import import_string

        name = f"{sync.name} - link vsys/vdom parents (auto)"
        if _sync_has_active_job(sync, name):
            return
        Job.enqueue(
            import_string("forward_netbox.jobs.link_forward_vsys_parents"),
            instance=sync,
            user=sync.user,
            name=name,
        )
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Auto vsys parent-link enqueue failed: %s", exc)


def _maybe_enqueue_auto_prune(sync):
    """Opt-in: after a successful sync, run "Prune orphans" (delete out-of-scope
    devices + empty orphan sites). OFF by default because it deletes NetBox data;
    enable per sync with ``auto_prune_orphans=True``. The prune keeps its own
    guards (refuses when the Forward scope returned 0 devices). Never lets a prune
    problem affect the sync result.
    """
    if not (sync.parameters or {}).get("auto_prune_orphans"):
        return
    try:
        from .utilities.sync_facade import enqueue_button_job
        from .utilities.sync_facade import JobAlreadyActive

        # Shares the button-job guard (prefix match also blocks when a MANUAL
        # prune is running). during_sync_ok: this hook fires from inside the
        # still-running sync job, after its apply work completed.
        try:
            enqueue_button_job(
                sync,
                "prune_orphans",
                sync.user,
                name_suffix_extra=" (auto)",
                during_sync_ok=True,
            )
        except JobAlreadyActive:
            return
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Auto prune-orphans enqueue failed: %s", exc)


def sync_forwardsync(job, *args, **kwargs):
    sync = ForwardSync.objects.get(pk=job.object_id)

    try:
        job.start()
        sync.sync(job=job, adhoc=bool(kwargs.get("adhoc")))
        safe_save_job_data(job, sync)
        _maybe_enqueue_device_analysis_refresh(sync)
        _maybe_enqueue_backfilled_tag_refresh(sync)
        _maybe_enqueue_vsys_parent_link(sync)
        _maybe_enqueue_auto_prune(sync)
        job.terminate()
    except Exception as exc:
        safe_save_job_data(job, sync)
        timeout = isinstance(exc, JobTimeoutException)
        job.terminate(status=JobStatusChoices.STATUS_ERRORED)
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
        update_run_from_branch_state(sync)
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
        if isinstance(exc, (ForwardSyncError, SyncError, JobTimeoutException)):
            logger.error(exc)
        else:
            raise
    finally:
        if sync.interval and not kwargs.get("adhoc"):
            new_scheduled_time = local_now() + timedelta(minutes=sync.interval)
            sync.refresh_from_db()
            from .utilities.sync_facade import sync_run_job_names

            should_skip = not sync.scheduled or (
                sync.scheduled
                and sync.scheduled > job.started
                # Name-scoped to sync RUNS: standing-schedule rows are
                # permanently SCHEDULED and would satisfy a status-only check.
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
        # handle()'s finally, so a hard-killed worker mid-occurrence silently
        # drops the chain. Recreate from the intent stored in sync.parameters
        # (no-op while the chain is healthy).
        try:
            from .utilities.sync_facade import reconcile_standing_schedules

            # Re-read parameters: adhoc runs never refresh the start-of-run
            # snapshot, and a schedule change made mid-run must not be
            # reverted by a stale reconcile.
            sync.refresh_from_db(fields=["parameters"])
            reconcile_standing_schedules(sync)
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
    """Shared validation body (legacy dotted-path shim + ValidationJob)."""
    sync = ForwardSync.objects.get(pk=job.object_id)
    try:
        sync.logger = SyncLogging(job=job.pk)
        validation_run = ForwardValidationRunner(
            sync,
            sync.source.get_client(),
            sync.logger,
            job=job,
        ).run_query_validation()
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
        except Exception:
            # Housekeeping must never mark a successful validation ERRORED.
            logger.warning(
                "Validation-run retention trim failed for sync %s.",
                sync.pk,
                exc_info=True,
            )
    except Exception as exc:
        safe_save_job_data(job, sync)
        if type(exc) in (SyncError, JobTimeoutException):
            logger.error(exc)
        raise


def validate_forwardsync(job, *args, **kwargs):
    # Legacy dotted-path shim: pre-2.6 queued Job rows and the immediate
    # (non-scheduled) enqueue path reference this callable directly.
    try:
        job.start()
        _validate_forwardsync_work(job)
        job.terminate()
    except Exception as exc:
        job.terminate(status=JobStatusChoices.STATUS_ERRORED)
        if type(exc) not in (SyncError, JobTimeoutException):
            raise


def _prune_forward_orphans_work(job):
    """Shared prune body (legacy dotted-path shim + PruneOrphansJob). Writes
    job.data (success or error dict) and re-raises; the caller decides
    terminate status and swallow-vs-propagate."""
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
            "out_of_scope_sample": device_result.get("out_of_scope_sample", []),
            "pruned_site_count": site_result.get("pruned_site_count", 0),
            # PROTECT-ing optional-plugin rows (e.g. netbox_routing BGP peers)
            # swept so the device cascade could proceed.
            "pruned_dependent_rows": device_result.get("pruned_dependent_rows", {}),
            # netbox_routing rows whose GenericFKs pointed at the pruned
            # devices, swept post-delete (they never PROTECT, so they would
            # otherwise dangle silently).
            "pruned_dangling_rows": device_result.get("pruned_dangling_rows", {}),
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


def prune_forward_orphans(job, *args, **kwargs):
    """Background prune of out-of-scope NetBox devices for a sync.

    Run as a job because deleting many devices cascades to their interfaces and
    IP addresses (plus change-logging signals) and easily exceeds an HTTP gateway
    timeout on large fabrics. Legacy dotted-path shim: pre-existing queued Job
    rows and the immediate button/API path reference this callable directly.
    """
    from .utilities.scope_reconciliation import EmptyForwardScopeError

    try:
        job.start()
        _prune_forward_orphans_work(job)
        job.terminate()
    except Exception as exc:
        job.terminate(status=JobStatusChoices.STATUS_ERRORED)
        if type(exc) not in (SyncError, JobTimeoutException) and not isinstance(
            exc, EmptyForwardScopeError
        ):
            raise


def refresh_forward_device_analysis(job, *args, **kwargs):
    """Background refresh of per-device Forward analysis (reachability proxy,
    connectivity-degree blast radius, CVE exposure) into ForwardDeviceAnalysis."""
    from .utilities.device_analysis import refresh_device_analysis

    sync = ForwardSync.objects.get(pk=job.object_id)
    try:
        job.start()
        job.data = refresh_device_analysis(sync)
        job.save(update_fields=["data"])
        job.terminate()
    except Exception as exc:
        # Record the failure on the job so it is visible in the UI (the Data
        # panel) instead of an empty Error field with null data.
        job.data = {
            "error": str(exc) or exc.__class__.__name__,
            "error_type": exc.__class__.__name__,
        }
        job.save(update_fields=["data"])
        job.terminate(status=JobStatusChoices.STATUS_ERRORED)
        if type(exc) in (SyncError, JobTimeoutException):
            logger.error(exc)
        else:
            raise


def tag_forward_backfilled_devices(job, *args, **kwargs):
    """Background sync of the ``forward-backfilled`` tag for a sync.

    Runs as a job because it issues a live Forward scope query and may tag/untag
    many devices (with change-logging signals), which can exceed an HTTP gateway
    timeout on large fabrics.
    """
    from .utilities.scope_reconciliation import tag_backfilled_devices

    sync = ForwardSync.objects.get(pk=job.object_id)
    try:
        job.start()
        job.data = tag_backfilled_devices(sync)
        job.save(update_fields=["data"])
        job.terminate()
    except Exception as exc:
        # Record the failure on the job so it is visible in the UI (the Data
        # panel) instead of an empty Error field with null data.
        job.data = {
            "error": str(exc) or exc.__class__.__name__,
            "error_type": exc.__class__.__name__,
        }
        job.save(update_fields=["data"])
        job.terminate(status=JobStatusChoices.STATUS_ERRORED)
        if type(exc) in (SyncError, JobTimeoutException):
            logger.error(exc)
        else:
            raise


def link_forward_vsys_parents(job, *args, **kwargs):
    """Background linkage of virtual-context firewalls (Palo vsys / Fortinet vdom)
    to their physical chassis via the ``forward_parent_device`` custom field.

    Runs as a job because it issues a live Forward query and may update many
    devices. Non-destructive — only sets/clears the custom field.
    """
    from .utilities.logging import SyncLogging
    from .utilities.vsys_parent import link_vsys_parents

    sync = ForwardSync.objects.get(pk=job.object_id)
    try:
        job.start()
        client = sync.source.get_client()
        job.data = link_vsys_parents(sync, client, SyncLogging())
        job.save(update_fields=["data"])
        job.terminate()
    except Exception as exc:
        job.data = {
            "error": str(exc) or exc.__class__.__name__,
            "error_type": exc.__class__.__name__,
        }
        job.save(update_fields=["data"])
        job.terminate(status=JobStatusChoices.STATUS_ERRORED)
        if type(exc) in (SyncError, JobTimeoutException):
            logger.error(exc)
        else:
            raise


def tag_forward_delete_eligible_ipam(job, *args, **kwargs):
    """Background sync of the ``forward-delete-eligible`` tag across network-global
    IPAM (prefixes/VLANs/VRFs) for a sync.

    Runs as a job because it issues live Forward fetches for each IPAM model and
    may tag/untag many objects (with change-logging signals). Tag-only — never
    deletes.
    """
    try:
        job.start()
        _tag_delete_eligible_ipam_work(job)
        job.terminate()
    except Exception as exc:
        job.terminate(status=JobStatusChoices.STATUS_ERRORED)
        if type(exc) not in (SyncError, JobTimeoutException):
            raise


def _tag_delete_eligible_ipam_work(job):
    """Shared tag-eligible-IPAM body (shim + TagDeleteEligibleIpamJob)."""
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


def create_forward_module_bays(job, *args, **kwargs):
    """Background creation of missing module bays for a sync (out-of-band ORM)."""
    try:
        job.start()
        _create_module_bays_work(job)
        job.terminate()
    except Exception as exc:
        job.terminate(status=JobStatusChoices.STATUS_ERRORED)
        if type(exc) not in (SyncError, JobTimeoutException):
            raise


def _create_module_bays_work(job):
    """Shared module-bay-creation body (shim + CreateModuleBaysJob)."""
    from .utilities.module_readiness import compute_module_readiness_for_sync
    from .utilities.module_readiness import create_missing_module_bays

    sync = ForwardSync.objects.get(pk=job.object_id)
    try:
        report = compute_module_readiness_for_sync(sync)
        result = create_missing_module_bays(report)
        job.data = result
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
    """Shared preview body (legacy dotted-path shim + DependencyPreviewJob)."""
    from .views import _dependency_dry_run_payload

    sync = ForwardSync.objects.get(pk=job.object_id)
    try:
        sync.logger = SyncLogging(job=job.pk)
        payload = _dependency_dry_run_payload(sync)
        job.data = json_safe_value(payload)
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


def forward_dependency_preview(job, *args, **kwargs):
    """Background dependency dry-run preview for a sync.

    The dry-run builds a full single-branch plan against live Forward data, which
    far exceeds an HTTP gateway timeout on large fabrics. Run it as a job and
    cache the JSON payload on ``job.data`` so the preview page can render it
    later without a Forward round-trip. Legacy dotted-path shim: pre-2.6 queued
    Job rows and the immediate button path reference this callable directly.
    """
    try:
        job.start()
        _dependency_preview_work(job)
        job.terminate()
    except Exception as exc:
        job.terminate(status=JobStatusChoices.STATUS_ERRORED)
        if type(exc) in (SyncError, JobTimeoutException):
            logger.error(exc)
        else:
            raise


def merge_forwardingestion(job, remove_branch=True, *args, **kwargs):
    ingestion = ForwardIngestion.objects.get(pk=job.object_id)
    try:
        run = active_execution_run(ingestion.sync) or latest_execution_run(
            ingestion.sync
        )
        if run is not None:
            reconcile_execution_run(run)
            run.refresh_from_db()
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

        job.start()
        if not ingestion.branch or getattr(ingestion.branch, "status", "") == "merged":
            ingestion.sync.logger = SyncLogging(job=job.pk)
            ingestion.sync.logger.log_info(
                "Forward ingestion branch is already merged or no longer present; "
                "skipping duplicate merge job.",
                obj=ingestion,
            )
            step = mark_ingestion_step_merged(
                ingestion,
                baseline_ready=bool(getattr(ingestion, "baseline_ready", False)),
                merge_job=job if isinstance(job, Job) else None,
            )
            if step is not None:
                ingestion.sync.logger.log_info(
                    (
                        "merge_queued -> merged "
                        f"(job={getattr(job, 'pk', 'n/a')}, step={step.index})"
                    ),
                    obj=ingestion,
                )
            next_stage_job = maybe_enqueue_next_branch_stage(ingestion, job.user)
            if next_stage_job is not None:
                ingestion.sync.logger.log_info(
                    f"Queued next stage step job {next_stage_job.pk} after merge completion.",
                    obj=ingestion,
                )
            safe_save_job_data(job, ingestion.sync)
            job.terminate()
            return
        if not claim_ingestion_merge_step(ingestion, job):
            ingestion.sync.logger = SyncLogging(job=job.pk)
            ingestion.sync.logger.log_info(
                "Forward ingestion merge is already claimed or completed; "
                "skipping duplicate merge job.",
                obj=ingestion,
            )
            if run is not None:
                reconcile_execution_run(run)
                run.refresh_from_db()
            next_stage_job = maybe_enqueue_next_branch_stage(ingestion, job.user)
            if next_stage_job is not None:
                ingestion.sync.logger.log_info(
                    f"Queued next stage step job {next_stage_job.pk} after merge reconciliation.",
                    obj=ingestion,
                )
            safe_save_job_data(job, ingestion.sync)
            job.terminate()
            return
        if isinstance(job, Job):
            ingestion.merge_job = job
        ingestion.save(update_fields=["merge_job"])
        ingestion.sync.logger = SyncLogging(job=job.pk)
        with event_tracking(request):
            ingestion.sync_merge(remove_branch=remove_branch)
        step = execution_step_for_ingestion(ingestion)
        if step is not None and step.status == ForwardExecutionStepStatusChoices.MERGED:
            ingestion.sync.logger.log_info(
                (
                    "merge_queued -> merged "
                    f"(job={getattr(job, 'pk', 'n/a')}, step={step.index})"
                ),
                obj=ingestion,
            )
        maybe_enqueue_next_branch_stage(ingestion, job.user)

        safe_save_job_data(job, ingestion.sync)
        job.terminate()
    except Exception as exc:
        logger.exception(
            "Error during merge for ForwardIngestion %s: %s", ingestion.pk, exc
        )
        timeout = isinstance(exc, JobTimeoutException)
        merge_not_ready_retryable = _is_merge_not_ready_retryable(exc)
        if timeout or merge_not_ready_retryable:
            message = (
                "Forward merge job timed out. Increase RQ worker timeout and rerun the merge."
                if timeout
                else str(exc)
            )
            if timeout:
                record_timeout_issue(
                    ingestion,
                    ForwardIngestionPhaseChoices.MERGE,
                    message,
                )
            if timeout:
                ingestion.sync.logger.log_failure(message, obj=ingestion)
            else:
                if _reset_merge_not_ready_branch_state(ingestion):
                    ingestion.sync.logger.log_info(
                        "Reset transient Branching merge state to ready before automatic retry.",
                        obj=ingestion,
                    )
                ingestion.sync.logger.log_info(
                    (
                        "Merge job hit a transient Branching readiness guard; "
                        "attempting automatic requeue."
                    ),
                    obj=ingestion,
                )
            update_run_from_branch_state(ingestion.sync)
            auto_retry_job = maybe_enqueue_next_branch_stage(ingestion, job.user)
            if auto_retry_job is not None:
                ingestion.sync.logger.log_info(
                    (
                        "Queued automatic merge-timeout recovery job "
                        f"{auto_retry_job.pk}."
                    ),
                    obj=ingestion,
                )
                if type(exc) in (SyncError, JobTimeoutException):
                    logger.warning(exc)
                safe_save_job_data(job, ingestion.sync)
                job.terminate(status=JobStatusChoices.STATUS_ERRORED)
                return
        else:
            message = f"Forward merge job failed: {exc}"
            if getattr(ingestion.sync, "logger", None) is None:
                ingestion.sync.logger = SyncLogging(job=job.pk)
            ingestion.sync.logger.log_failure(message, obj=ingestion)
            _fail_nonretryable_merging_branch(ingestion, message)

        ForwardSync.objects.filter(pk=ingestion.sync.pk).update(
            status=(
                ForwardSyncStatusChoices.TIMEOUT
                if timeout
                else ForwardSyncStatusChoices.FAILED
            )
        )
        ingestion.sync.status = (
            ForwardSyncStatusChoices.TIMEOUT
            if timeout
            else ForwardSyncStatusChoices.FAILED
        )
        update_run_from_branch_state(ingestion.sync)
        safe_save_job_data(job, ingestion.sync)
        job.terminate(status=JobStatusChoices.STATUS_ERRORED)
        if type(exc) in (SyncError, JobTimeoutException):
            logger.error(exc)
        else:
            raise


def _is_merge_not_ready_retryable(exc):
    if not isinstance(exc, SyncError):
        return False
    message = str(exc or "").lower()
    return "not ready to merge" in message and "branch" in message


def _reset_merge_not_ready_branch_state(ingestion):
    branch = getattr(ingestion, "branch", None)
    if branch is None:
        return False
    if str(getattr(branch, "status", "") or "") != BranchStatusChoices.MERGING:
        return False
    branch.status = BranchStatusChoices.READY
    branch.save(update_fields=["status", "last_updated"])
    return True


def _fail_nonretryable_merging_branch(ingestion, message):
    branch = getattr(ingestion, "branch", None)
    if branch is None:
        return False
    if str(getattr(branch, "status", "") or "") != BranchStatusChoices.MERGING:
        return False
    Branch.objects.filter(pk=branch.pk).update(status=BranchStatusChoices.FAILED)
    branch.status = BranchStatusChoices.FAILED
    ingestion.sync.logger.log_failure(
        (
            "Marked Branching branch failed after non-retryable merge error: "
            f"{message}"
        ),
        obj=ingestion,
    )
    return True


def _skip_if_immediate_equivalent_active(job, per_sync_suffix):
    """Standing-schedule occurrence guard, three checks in one:

    1. sync deleted -> stop the recurrence chain;
    2. stored intent disagrees with this occurrence -> cancelled (0) stops
       the chain, a different interval re-aligns it (this is what makes a
       cancel/replace that raced a RUNNING occurrence self-terminate instead
       of resurrecting the old schedule), an ABSENT key is backfilled from
       this occurrence (pre-intent 2.5.6 chains);
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
    kind = {
        "validation": "validation",
        "dependency preview": "dependency_preview",
    }[job.name]
    from .utilities.sync_facade import persist_standing_schedule_interval
    from .utilities.sync_facade import STANDING_SCHEDULE_PARAM_KEYS

    key = STANDING_SCHEDULE_PARAM_KEYS[kind]
    parameters = sync.parameters or {}
    if job.interval:
        if key not in parameters:
            # Pre-intent chain (2.5.6): adopt it.
            persist_standing_schedule_interval(sync, kind, job.interval)
        else:
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
                # Intent changed (e.g. mid-run replace): re-align this
                # chain's recurrence instead of leaving a stale-interval
                # duplicate behind.
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


class DependencyPreviewJob(JobRunner):
    """Recurring-capable dependency preview.

    Standing schedules use the fixed ``Meta.name`` so ``enqueue_once`` dedup
    (which filters on ``cls.name`` + instance) works; the name still satisfies
    the ``icontains "dependency preview"`` lookups on the preview/drift pages,
    and per-sync scoping rides on the ``instance=sync`` binding. Immediate
    button/API runs keep the legacy per-sync name via the plain-function shim.
    """

    class Meta:
        name = "dependency preview"

    def run(self, *args, **kwargs):
        if _skip_if_immediate_equivalent_active(self.job, "dependency preview"):
            return
        _dependency_preview_work(self.job)


class ValidationJob(JobRunner):
    """Recurring-capable sync validation (see DependencyPreviewJob notes)."""

    class Meta:
        name = "validation"

    def run(self, *args, **kwargs):
        if _skip_if_immediate_equivalent_active(self.job, "validation"):
            return
        _validate_forwardsync_work(self.job)


class PruneOrphansJob(JobRunner):
    """JobRunner parity for the prune button job. Immediate runs keep the
    legacy per-sync name via the prune_forward_orphans shim; nothing enqueues
    this class today. NO schedule exposure: the API rejects schedule bodies
    for this action — if that ever changes, run() must first gate on an
    active sync run (JobBlockedBySyncRun in enqueue_button_job does not cover
    runner occurrences) and enqueue_once callers MUST pass instance=sync
    (get_jobs(instance=None) matches the fixed name globally)."""

    class Meta:
        # Byte-identical to BUTTON_JOB_SPECS["prune_orphans"][1]: the overlap
        # guard's exact-name arm depends on it.
        name = "prune orphans"

    def run(self, *args, **kwargs):
        _prune_forward_orphans_work(self.job)


class TagDeleteEligibleIpamJob(JobRunner):
    """JobRunner parity for the delete-eligible IPAM tag job (see
    PruneOrphansJob notes)."""

    class Meta:
        name = "tag delete-eligible IPAM"

    def run(self, *args, **kwargs):
        _tag_delete_eligible_ipam_work(self.job)


class CreateModuleBaysJob(JobRunner):
    """JobRunner parity for the module-bay creation job (see PruneOrphansJob
    notes)."""

    class Meta:
        name = "create module bays"

    def run(self, *args, **kwargs):
        _create_module_bays_work(self.job)
