import logging
import traceback

from core.exceptions import SyncError
from core.signals import pre_sync
from django.utils import timezone

from ..choices import ForwardIngestionPhaseChoices
from ..choices import ForwardSourceStatusChoices
from ..choices import ForwardSyncStatusChoices
from ..models import ForwardIngestion
from ..models import ForwardIngestionIssue
from ..models import ForwardSource
from ..models import ForwardSync
from ..models import ForwardValidationRun
from ..utilities.logging import SyncLogging

logger = logging.getLogger("forward_netbox.models")


def _prepare_forward_sync(sync, job=None):
    if job:
        sync.logger = SyncLogging(job=job.pk)
        user = job.user
    else:
        sync.logger = SyncLogging(job=sync.pk)
        user = sync.user

    pre_sync.send(sender=sync.__class__, instance=sync)

    sync.status = ForwardSyncStatusChoices.SYNCING
    ForwardSync.objects.filter(pk=sync.pk).update(status=sync.status)
    sync.source.status = ForwardSourceStatusChoices.SYNCING
    ForwardSource.objects.filter(pk=sync.source.pk).update(status=sync.source.status)
    return user


def _build_forward_ingestion(sync, job, executor):
    validation_run = getattr(executor, "last_validation_run", None)
    if not isinstance(validation_run, ForwardValidationRun):
        validation_run = None
    model_results = getattr(executor, "last_model_results", [])
    if not isinstance(model_results, list):
        model_results = []
    return ForwardIngestion.objects.create(
        sync=sync,
        job=job,
        validation_run=validation_run,
        model_results=model_results,
    )


def _record_forward_sync_failure(sync, job, executor, ingestion, exc):
    logger.exception("Forward sync failed")
    sync.status = ForwardSyncStatusChoices.FAILED
    if ingestion is None:
        ingestion = getattr(executor, "current_ingestion", None)
    if ingestion is None:
        ingestion = _build_forward_ingestion(sync, job, executor)
    else:
        validation_run = getattr(executor, "last_validation_run", None)
        if isinstance(validation_run, ForwardValidationRun) and not ingestion.validation_run:
            ingestion.validation_run = validation_run
            ingestion.save(update_fields=["validation_run"])
    sync.logger.log_failure(f"Forward ingestion failed: {exc}", obj=ingestion)
    ForwardIngestionIssue.objects.create(
        ingestion=ingestion,
        phase=ForwardIngestionPhaseChoices.SYNC,
        message=str(exc),
        exception=exc.__class__.__name__,
        raw_data={"traceback": traceback.format_exc()},
    )
    return ingestion


def _finalize_forward_sync(sync, job):
    sync.last_synced = timezone.now()
    sync.source.last_synced = sync.last_synced
    sync.source.status = (
        ForwardSourceStatusChoices.READY
        if sync.status
        in (
            ForwardSyncStatusChoices.READY_TO_MERGE,
            ForwardSyncStatusChoices.MERGING,
            ForwardSyncStatusChoices.COMPLETED,
        )
        else ForwardSourceStatusChoices.FAILED
    )
    ForwardSource.objects.filter(pk=sync.source.pk).update(
        last_synced=sync.source.last_synced,
        status=sync.source.status,
    )
    ForwardSync.objects.filter(pk=sync.pk).update(
        status=sync.status,
        last_synced=sync.last_synced,
    )
    if job:
        job.data = sync.logger.log_data
        job.save(update_fields=["data"])


def run_forward_sync(sync, job=None, *, max_changes_per_branch=None):
    from .multi_branch import ForwardMultiBranchExecutor

    if sync.is_waiting_for_branch_merge:
        sync.logger.log_warning(
            "Forward sync is waiting for the current shard branch to be merged.",
            obj=sync,
        )
        return

    if sync.status in (
        ForwardSyncStatusChoices.SYNCING,
        ForwardSyncStatusChoices.MERGING,
    ):
        raise SyncError(
            "Cannot initiate sync; a Forward ingestion is already in progress."
        )

    user = _prepare_forward_sync(sync, job=job)
    if max_changes_per_branch is None:
        max_changes_per_branch = sync.get_max_changes_per_branch()

    ingestion = None
    executor = None
    try:
        executor = ForwardMultiBranchExecutor(
            sync,
            sync.source.get_client(),
            sync.logger,
            user=user,
            job=job,
        )
        ingestions = executor.run(
            max_changes_per_branch=max_changes_per_branch,
        )
        if not ingestions:
            sync.status = ForwardSyncStatusChoices.COMPLETED
            sync.logger.log_success("Forward ingestion completed.", obj=sync)
            return
        ingestion = ingestions[-1]
        if sync.status == ForwardSyncStatusChoices.READY_TO_MERGE:
            sync.logger.log_success(
                "Forward multi-branch shard staged for review.",
                obj=sync,
            )
            return
        sync.status = ForwardSyncStatusChoices.COMPLETED
        sync.logger.log_success(
            "Forward multi-branch ingestion completed.",
            obj=sync,
        )
        return
    except Exception as exc:
        ingestion = _record_forward_sync_failure(
            sync,
            job,
            executor,
            ingestion,
            exc,
        )
    finally:
        _finalize_forward_sync(sync, job)
