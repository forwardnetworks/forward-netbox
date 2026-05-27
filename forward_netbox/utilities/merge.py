"""
Custom merge orchestrator for ForwardIngestion.

This preserves the existing branch-backed merge workflow closely:
- merge one ObjectChange at a time inside its own savepoint
- record failed changes as ForwardIngestionIssue(phase='merge')
- report progress through SyncLogging statistics
- preserve branching lifecycle signals and status transitions
"""

import logging
import time
import traceback
from collections import Counter
from functools import partial
from typing import TYPE_CHECKING

from core.exceptions import SyncError
from core.models import ObjectChange as ObjectChange_
from django.db import connection
from django.db import DEFAULT_DB_ALIAS
from django.db.models.signals import post_save
from django.test import RequestFactory
from django.urls import reverse
from django.utils import timezone
from netbox.context_managers import event_tracking
from netbox_branching.choices import BranchEventTypeChoices
from netbox_branching.choices import BranchStatusChoices
from netbox_branching.merge_strategies import get_merge_strategy
from netbox_branching.models import Branch
from netbox_branching.models import BranchEvent
from netbox_branching.signals import post_merge
from netbox_branching.utilities import record_applied_change

from ..choices import ForwardIngestionPhaseChoices
from ..models import ForwardIngestionIssue

if TYPE_CHECKING:
    from ..models import ForwardIngestion
    from ..utilities.logging import SyncLogging

logger = logging.getLogger("forward_netbox.merge")

MERGE_HEARTBEAT_ROW_INTERVAL = 1000
MERGE_HEARTBEAT_SECONDS = 60
MERGE_LOG_ROW_INTERVAL = 5000
MERGE_LOG_SECONDS = 300


def merge_branch(
    ingestion: "ForwardIngestion", sync_logger: "SyncLogging | None" = None
) -> None:
    branch = ingestion.branch
    user = ingestion.sync.user

    if not branch:
        raise SyncError("Ingestion has no staged branch to merge.")
    if not branch.ready:
        raise SyncError(f"Branch {branch} is not ready to merge")

    changes = branch.get_unmerged_changes().order_by("time")
    total_changes = changes.count()
    action_counts = Counter(changes.values_list("action", flat=True))
    if not total_changes:
        if sync_logger:
            sync_logger.log_info("No changes to merge.")
        return

    if sync_logger:
        model_counts: Counter = Counter()
        for app_label, model_name in changes.values_list(
            "changed_object_type__app_label", "changed_object_type__model"
        ):
            model_counts[f"{app_label}.{model_name}"] += 1
        for model_string, count in model_counts.items():
            sync_logger.init_statistics(model_string, total=count)
            sync_logger.log_info(
                f"Going to merge {count} changes for `{model_string}`."
            )

    Branch.objects.filter(pk=branch.pk).update(status=BranchStatusChoices.MERGING)

    handler = partial(record_applied_change, branch=branch)
    post_save.connect(handler, sender=ObjectChange_, weak=False)
    request = RequestFactory().get(reverse("home"))

    models_touched = set()
    failed = 0

    processed = 0
    last_heartbeat_at = time.monotonic()
    last_log_at = last_heartbeat_at
    step_index = _merge_step_index(ingestion)

    try:
        for change in changes:
            processed += 1
            model_class = change.changed_object_type.model_class()
            app_label, model_name = change.changed_object_type.natural_key()
            model_string = f"{app_label}.{model_name}"
            savepoint = connection.savepoint()
            try:
                with event_tracking(request):
                    request.id = change.request_id
                    request.user = change.user
                    change.apply(branch, using=DEFAULT_DB_ALIAS, logger=logger)

                connection.savepoint_commit(savepoint)
                models_touched.add(model_class)
                if sync_logger:
                    sync_logger.increment_statistics(model_string)
            except Exception as exc:
                connection.savepoint_rollback(savepoint)
                failed += 1
                message = (
                    f"Failed to apply change {change.pk} "
                    f"({change.action} {model_string}: {change.changed_object_id}): {exc}"
                )
                logger.error(message, exc_info=True)
                if sync_logger:
                    sync_logger.log_failure(message)
                ForwardIngestionIssue.objects.create(
                    ingestion=ingestion,
                    phase=ForwardIngestionPhaseChoices.MERGE,
                    model=model_string,
                    message=message,
                    exception=exc.__class__.__name__,
                    raw_data={"traceback": traceback.format_exc()},
                )
            last_heartbeat_at, last_log_at = _report_merge_progress(
                ingestion,
                sync_logger=sync_logger,
                model_string=model_string,
                step_index=step_index,
                processed=processed,
                total_changes=total_changes,
                last_heartbeat_at=last_heartbeat_at,
                last_log_at=last_log_at,
            )
    finally:
        post_save.disconnect(handler, sender=ObjectChange_)

    if models_touched:
        strategy_class = get_merge_strategy(branch.merge_strategy)
        strategy_class()._clean(models_touched)

    branch.status = BranchStatusChoices.MERGED
    branch.merged_time = timezone.now()
    branch.merged_by = user
    branch.save()

    BranchEvent.objects.create(
        branch=branch,
        user=user,
        type=BranchEventTypeChoices.MERGED,
    )
    post_merge.send(sender=Branch, branch=branch, user=user)

    failed_message = "no failed."
    if failed:
        failed_message = f"{failed} skipped (recorded as ingestion issues)."
    summary = f"Merge completed: {total_changes - failed} applied, {failed_message}"
    ingestion.record_change_totals(
        applied=total_changes - failed,
        failed=failed,
        created=action_counts.get("create", 0),
        updated=action_counts.get("update", 0),
        deleted=action_counts.get("delete", 0),
    )
    logger.info(summary)
    if sync_logger:
        sync_logger.log_info(summary)


def _report_merge_progress(
    ingestion: "ForwardIngestion",
    *,
    sync_logger: "SyncLogging | None",
    model_string: str,
    step_index: int | None,
    processed: int,
    total_changes: int,
    last_heartbeat_at: float,
    last_log_at: float,
) -> tuple[float, float]:
    now = time.monotonic()
    heartbeat_due = (
        processed == total_changes
        or processed % MERGE_HEARTBEAT_ROW_INTERVAL == 0
        or now - last_heartbeat_at >= MERGE_HEARTBEAT_SECONDS
    )
    log_due = (
        processed == total_changes
        or processed % MERGE_LOG_ROW_INTERVAL == 0
        or now - last_log_at >= MERGE_LOG_SECONDS
    )

    if heartbeat_due:
        try:
            from .execution_ledger import touch_execution_step_progress

            touch_execution_step_progress(
                ingestion.sync,
                model_string=model_string,
                shard_index=step_index,
            )
        except Exception:
            logger.debug("Unable to update merge progress heartbeat.", exc_info=True)
        last_heartbeat_at = now

    if sync_logger and log_due:
        sync_logger.log_info(
            f"Merged {processed}/{total_changes} branch changes "
            f"(current model `{model_string}`)."
        )
        last_log_at = now

    return last_heartbeat_at, last_log_at


def _merge_step_index(ingestion: "ForwardIngestion") -> int | None:
    try:
        from .execution_ledger import execution_step_for_ingestion

        step = execution_step_for_ingestion(ingestion)
    except Exception:
        logger.debug("Unable to resolve merge execution step.", exc_info=True)
        return None
    return getattr(step, "index", None)
