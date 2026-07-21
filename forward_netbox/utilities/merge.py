"""
Custom merge orchestrator for ForwardIngestion.

This preserves the branch-backed merge lifecycle while scaling its apply path:
- batch supported changes and isolate exceptional rows in per-object savepoints
- record failed changes as ForwardIngestionIssue(phase='merge')
- report progress through SyncLogging statistics
- preserve branching lifecycle signals and status transitions
"""

import logging
import time
import traceback
import uuid
from functools import partial
from typing import TYPE_CHECKING

from core.exceptions import SyncError
from core.models import ObjectChange as ObjectChange_
from django.db import DEFAULT_DB_ALIAS
from django.db import transaction
from django.db.models import Count
from django.db.models.signals import post_save
from django.test import RequestFactory
from django.urls import reverse
from django.utils import timezone
from netbox.context_managers import event_tracking
from netbox_branching.choices import BranchEventTypeChoices
from netbox_branching.choices import BranchStatusChoices
from netbox_branching.merge_strategies import get_merge_strategy
from rq.timeouts import JobTimeoutException

from .bulk_merge import _ApplyOneFailure
from .bulk_merge import bulk_merge_changes
from .bulk_delete import lock_related_writes_for_delete
from netbox_branching.models import Branch
from netbox_branching.models import BranchEvent
from netbox_branching.signals import post_merge
from netbox_branching.utilities import record_applied_change

from ..choices import ForwardIngestionPhaseChoices
from ..exceptions import ForwardPartialMergeError
from ..models import ForwardIngestionIssue

if TYPE_CHECKING:
    from ..models import ForwardIngestion
    from ..utilities.logging import SyncLogging

logger = logging.getLogger("forward_netbox.merge")

MERGE_HEARTBEAT_ROW_INTERVAL = 1000
MERGE_HEARTBEAT_SECONDS = 60
MERGE_LOG_ROW_INTERVAL = 5000
MERGE_LOG_SECONDS = 300

RESOLVED_MERGE_ISSUES_KEY = "resolved_merge_issues"


def _replication_side_effect_exists(collapsed_change) -> bool:
    """Return true when main already materialized a redundant branch create."""
    model_class = collapsed_change.model_class
    model_string = f"{model_class._meta.app_label}.{model_class._meta.model_name}"
    action = getattr(
        collapsed_change.final_action, "value", collapsed_change.final_action
    )
    if model_string != "dcim.modulebay" or action != "create":
        return False
    data = collapsed_change.postchange_data or {}
    device_id = data.get("device") or data.get("device_id")
    name = str(data.get("name") or "")
    if not device_id or not name:
        return False
    return model_class.objects.filter(device_id=device_id, name=name).exists()


class _MergeIssueRecorder:
    """Record merge-time change failures as ForwardIngestionIssue rows."""

    def __init__(self, ingestion, sync_logger):
        self._ingestion = ingestion
        self._sync_logger = sync_logger

    def record(self, *, model_string, message, exc):
        exception_info = (type(exc), exc, exc.__traceback__)
        logger.error(message, exc_info=exception_info)
        if self._sync_logger:
            self._sync_logger.log_failure(message)
        ForwardIngestionIssue.objects.create(
            ingestion=self._ingestion,
            phase=ForwardIngestionPhaseChoices.MERGE,
            model=model_string,
            message=message,
            exception=exc.__class__.__name__,
            raw_data={
                "traceback": "".join(traceback.format_exception(*exception_info))
            },
        )


def _attest_branch_merged(ingestion, branch, user) -> None:
    """Atomically persist Branching completion and durable ingestion evidence."""
    merged_at = timezone.now()
    with transaction.atomic():
        branch.status = BranchStatusChoices.MERGED
        branch.merged_time = merged_at
        branch.merged_by = user
        branch.save(
            update_merge_sync_fields=True,
            update_fields=["status", "merged_time", "merged_by", "last_updated"],
        )
        BranchEvent.objects.create(
            branch=branch,
            user=user,
            type=BranchEventTypeChoices.MERGED,
        )
        ingestion.__class__.objects.filter(pk=ingestion.pk).update(
            merge_applied_at=merged_at
        )
        ingestion.merge_applied_at = merged_at
    post_merge.send(sender=Branch, branch=branch, user=user)


def _retire_resolved_merge_issues(ingestion, issue_ids) -> int:
    """Archive and remove merge issues superseded by a successful retry."""
    issue_ids = tuple(issue_ids)
    if not issue_ids:
        return 0

    with transaction.atomic():
        locked_ingestion = ingestion.__class__.objects.select_for_update().get(
            pk=ingestion.pk
        )
        issues = list(
            ForwardIngestionIssue.objects.filter(
                pk__in=issue_ids,
                ingestion=locked_ingestion,
                phase=ForwardIngestionPhaseChoices.MERGE,
            )
            .order_by("pk")
            .values(
                "pk",
                "timestamp",
                "model",
                "message",
                "coalesce_fields",
                "defaults",
                "raw_data",
                "exception",
            )
        )
        if not issues:
            return 0

        resolved_at = timezone.now().isoformat()
        archived = list(
            (locked_ingestion.snapshot_info or {}).get(RESOLVED_MERGE_ISSUES_KEY) or []
        )
        for issue in issues:
            timestamp = issue["timestamp"]
            archived.append(
                {
                    "issue_id": issue["pk"],
                    "timestamp": timestamp.isoformat() if timestamp else None,
                    "resolved_at": resolved_at,
                    "model": issue["model"],
                    "message": issue["message"],
                    "coalesce_fields": issue["coalesce_fields"],
                    "defaults": issue["defaults"],
                    "raw_data": issue["raw_data"],
                    "exception": issue["exception"],
                }
            )

        snapshot_info = dict(locked_ingestion.snapshot_info or {})
        snapshot_info[RESOLVED_MERGE_ISSUES_KEY] = archived
        locked_ingestion.snapshot_info = snapshot_info
        locked_ingestion.save(update_fields=["snapshot_info"])
        ForwardIngestionIssue.objects.filter(
            pk__in=[issue["pk"] for issue in issues],
            ingestion=locked_ingestion,
            phase=ForwardIngestionPhaseChoices.MERGE,
        ).delete()

    ingestion.snapshot_info = snapshot_info
    return len(issues)


def merge_branch(
    ingestion: "ForwardIngestion",
    sync_logger: "SyncLogging | None" = None,
    *,
    user=None,
) -> None:
    branch = ingestion.branch
    user = user or ingestion.sync.user

    if not branch:
        raise SyncError("Ingestion has no staged branch to merge.")
    if user is None:
        raise SyncError("Merge attribution requires an invoking user or sync owner.")
    if not branch.ready:
        raise SyncError(f"Branch {branch} is not ready to merge")

    previous_applied = int(ingestion.applied_change_count or 0)
    previous_failed = int(ingestion.failed_change_count or 0)
    retrying_partial = previous_failed > 0
    prior_merge_issue_ids = list(
        ingestion.issues.filter(phase=ForwardIngestionPhaseChoices.MERGE).values_list(
            "pk", flat=True
        )
    )
    changes = branch.get_unmerged_changes().order_by("time")
    if not changes.exists():
        if retrying_partial:
            ingestion.record_change_totals(
                applied=previous_applied + previous_failed,
                failed=0,
                created=int(ingestion.created_change_count or 0),
                updated=int(ingestion.updated_change_count or 0),
                deleted=int(ingestion.deleted_change_count or 0),
            )
        else:
            ingestion.record_change_totals(
                applied=0,
                failed=0,
                created=0,
                updated=0,
                deleted=0,
            )
        _attest_branch_merged(ingestion, branch, user)
        if retrying_partial:
            _retire_resolved_merge_issues(ingestion, prior_merge_issue_ids)
        if sync_logger:
            sync_logger.log_info("No changes to merge.")
        return

    if sync_logger:
        for model_count in (
            changes.order_by()
            .values(
                "changed_object_type__app_label",
                "changed_object_type__model",
            )
            .annotate(total=Count("changed_object_id", distinct=True))
        ):
            model_string = (
                f"{model_count['changed_object_type__app_label']}."
                f"{model_count['changed_object_type__model']}"
            )
            sync_logger.init_statistics(model_string, total=model_count["total"])
            sync_logger.log_info(
                f"Going to merge {model_count['total']} changes for `{model_string}`."
            )

    logical_total_changes = (
        changes.order_by()
        .values("changed_object_type_id", "changed_object_id")
        .distinct()
        .count()
    )
    previous_logical_total = previous_applied + previous_failed
    if retrying_partial and logical_total_changes != previous_logical_total:
        raise RuntimeError(
            "Partial-merge retry changed the logical branch total: "
            f"previously {previous_logical_total}, now {logical_total_changes}. "
            "Refusing to overwrite cumulative merge evidence."
        )

    Branch.objects.filter(pk=branch.pk).update(status=BranchStatusChoices.MERGING)

    handler = partial(record_applied_change, branch=branch)
    post_save.connect(handler, sender=ObjectChange_, weak=False)
    if ingestion.change_request_id is None:
        ingestion.change_request_id = uuid.uuid4()
        ingestion.save(update_fields=["change_request_id"])

    request = RequestFactory().get(reverse("home"))
    request.user = user
    request.id = ingestion.change_request_id

    models_touched = set()
    failed = 0
    issue_recorder = _MergeIssueRecorder(ingestion, sync_logger)

    processed = 0
    last_heartbeat_at = time.monotonic()
    last_log_at = last_heartbeat_at

    def _model_string(model_class):
        return f"{model_class._meta.app_label}.{model_class._meta.model_name}"

    def _apply_one(collapsed_change):
        # Per-object fallback: apply one collapsed change via the framework's
        # ObjectChange.apply (handles MPTT tree depth, deletion-aware update
        # payloads, ProtectedError), savepoint-isolated so one bad row records an
        # issue and the merge continues.
        model_class = collapsed_change.model_class
        model_string = _model_string(model_class)
        dummy_change = collapsed_change.generate_object_change()
        try:
            with transaction.atomic():
                if (
                    model_string == "dcim.device"
                    and getattr(
                        collapsed_change.final_action,
                        "value",
                        collapsed_change.final_action,
                    )
                    == "delete"
                ):
                    from .ownership import (
                        release_authoritative_device_delete_ownership,
                    )

                    release = release_authoritative_device_delete_ownership(
                        ingestion.sync,
                        [collapsed_change.key[1]],
                    )
                    if release["blocked_device_ids"]:
                        raise RuntimeError(
                            "Authoritative device deletion is blocked by current "
                            "ownership evidence."
                        )
                    lock_related_writes_for_delete(
                        model_class,
                        using=DEFAULT_DB_ALIAS,
                    )
                with event_tracking(request):
                    dummy_change.apply(branch, using=DEFAULT_DB_ALIAS, logger=logger)
            models_touched.add(model_class)
            return True
        except JobTimeoutException:
            raise
        except Exception as exc:
            if _replication_side_effect_exists(collapsed_change):
                logger.info(
                    "Treating redundant %s create for %s as applied; main "
                    "materialized it while creating the parent device.",
                    model_string,
                    collapsed_change.key[1],
                )
                return True
            return _ApplyOneFailure(exc)

    def _record_failed(collapsed_change, exc):
        model_string = _model_string(collapsed_change.model_class)
        issue_recorder.record(
            model_string=model_string,
            message=(
                "Failed to apply collapsed change "
                f"({collapsed_change.final_action} {model_string}: "
                f"{collapsed_change.key[1]}): {exc}"
            ),
            exc=exc,
        )

    def _record_applied(model_class):
        nonlocal processed, last_heartbeat_at, last_log_at
        processed += 1
        if sync_logger:
            sync_logger.increment_statistics(_model_string(model_class))
        last_heartbeat_at, last_log_at = _report_merge_progress(
            ingestion,
            sync_logger=sync_logger,
            model_string=_model_string(model_class),
            processed=processed,
            total_changes=logical_total_changes,
            last_heartbeat_at=last_heartbeat_at,
            last_log_at=last_log_at,
        )

    merge_metadata = {}
    try:
        applied_count, bulk_failed, bulk_models = bulk_merge_changes(
            branch,
            changes,
            request,
            user,
            logger,
            apply_one=_apply_one,
            record_applied=_record_applied,
            record_failed=_record_failed,
            result_metadata=merge_metadata,
        )
        failed += bulk_failed
        models_touched |= bulk_models
    finally:
        post_save.disconnect(handler, sender=ObjectChange_)

    if models_touched:
        strategy_class = get_merge_strategy(branch.merge_strategy)
        strategy_class()._clean(models_touched)

    reported_logical_total = int(merge_metadata.get("logical_total", -1))
    if reported_logical_total != logical_total_changes:
        raise RuntimeError(
            "Branch logical-change count changed during merge: "
            f"expected {logical_total_changes}, merged {reported_logical_total}."
        )
    if applied_count + failed != logical_total_changes:
        raise RuntimeError(
            "Bulk merge returned inconsistent logical totals: "
            f"{applied_count} applied + {failed} failed != "
            f"{logical_total_changes} staged."
        )

    failed_message = "no failed."
    if failed:
        failed_message = f"{failed} skipped (recorded as ingestion issues)."
    summary = f"Merge completed: {applied_count} applied, {failed_message}"
    cumulative_applied = applied_count
    logical_action_counts = merge_metadata.get("logical_action_counts", {})
    created = int(logical_action_counts.get("create", 0))
    updated = int(logical_action_counts.get("update", 0))
    deleted = int(logical_action_counts.get("delete", 0))
    ingestion.record_change_totals(
        applied=cumulative_applied,
        failed=failed,
        created=created,
        updated=updated,
        deleted=deleted,
    )

    if failed:
        branch.status = BranchStatusChoices.READY
        branch.save(update_fields=["status", "last_updated"])
        summary = (
            f"Merge incomplete: {applied_count} applied, {failed} failed. "
            "The branch remains ready for inspection and retry."
        )
        logger.error(summary)
        if sync_logger:
            sync_logger.log_failure(summary)
        raise ForwardPartialMergeError(
            summary,
            applied=applied_count,
            failed=failed,
        )

    _attest_branch_merged(ingestion, branch, user)
    if retrying_partial:
        _retire_resolved_merge_issues(ingestion, prior_merge_issue_ids)

    logger.info(summary)
    if sync_logger:
        sync_logger.log_info(summary)


def _report_merge_progress(
    ingestion: "ForwardIngestion",
    *,
    sync_logger: "SyncLogging | None",
    model_string: str,
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
        last_heartbeat_at = now

    if sync_logger and log_due:
        sync_logger.log_info(
            f"Merged {processed}/{total_changes} branch changes "
            f"(current model `{model_string}`)."
        )
        last_log_at = now

    return last_heartbeat_at, last_log_at
