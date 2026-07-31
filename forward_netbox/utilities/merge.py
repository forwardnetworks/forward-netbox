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
import uuid
from collections import defaultdict
from functools import partial
from typing import TYPE_CHECKING

from core.exceptions import SyncError
from core.models import ObjectChange as ObjectChange_
from django.db import DEFAULT_DB_ALIAS, transaction
from django.db.models import Count
from django.db.models.signals import post_save
from django.test import RequestFactory
from django.urls import reverse
from django.utils import timezone
from netbox.context_managers import event_tracking
from netbox_branching.choices import BranchEventTypeChoices, BranchStatusChoices
from netbox_branching.merge_strategies import get_merge_strategy
from netbox_branching.models import Branch
from netbox_branching.models import BranchEvent
from netbox_branching.signals import post_merge
from netbox_branching.utilities import record_applied_change
from rq.timeouts import JobTimeoutException

from ..choices import ForwardIngestionPhaseChoices
from ..exceptions import ForwardPartialMergeError
from ..models import ForwardIngestionIssue
from .bulk_delete import lock_related_writes_for_delete
from .bulk_merge import _ApplyOneFailure
from .bulk_merge import bulk_merge_changes
from .diagnostics import exception_type
from .diagnostics import safe_operation_failure
from .diagnostics import structured_failure_diagnosis
from .merge_observability import checkpoint_merge_attempt
from .merge_observability import initialize_merge_attempt
from .merge_observability import mark_merge_attempt_applied
from .merge_profiling import profile_scope
from .merge_set_based import set_based_merge_decision

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

    def record(self, *, model_string, exc):
        # The diagnosis has always been stored in `raw_data`, but the issue list
        # shows only the message — so the constraint or field sat one click away
        # while the list said nothing but the exception class. The sync-phase
        # recorders were given this treatment and the merge recorder was not,
        # which left the answer visible in two of three places.
        diagnosis = structured_failure_diagnosis(exc)
        constraint = diagnosis.get("constraint_name") or ""
        invalid_fields = diagnosis.get("invalid_fields") or []
        message = safe_operation_failure(f"Merge for {model_string}", exc)
        if constraint:
            message = f"{message[:-1]} on constraint {constraint}."
        elif invalid_fields:
            message = f"{message[:-1]} on invalid field(s) {', '.join(invalid_fields)}."
        logger.error(
            "Merge row failed for %s (%s).",
            model_string,
            exception_type(exc),
        )
        if self._sync_logger:
            self._sync_logger.log_failure(message)
        # `raw_data` used to be left empty, so a merge failure recorded nothing
        # but its exception class. Four IntegrityError rows then blocked a
        # customer's baseline for a day with no way — GUI, API, CLI or support
        # bundle — to learn which constraint they violated. The diagnosis holds
        # schema identifiers only; the key values a Postgres DETAIL line embeds
        # are deliberately still not captured.
        ForwardIngestionIssue.objects.create(
            ingestion=self._ingestion,
            phase=ForwardIngestionPhaseChoices.MERGE,
            model=model_string,
            message=message,
            exception=exc.__class__.__name__,
            raw_data=diagnosis,
        )


def _attest_branch_merged(ingestion, branch, user) -> None:
    """Atomically persist Branching completion and durable ingestion evidence."""
    with profile_scope("branch_attestation", owner="ours"):
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


ACCEPTED_MERGE_FAILURES_KEY = "accepted_merge_failures"


def _record_accepted_failures(ingestion, *, failed, user) -> None:
    """Write durable evidence that a baseline was promoted over known failures.

    Without this the ingestion would be indistinguishable from a clean merge,
    which is the 2.6.4 defect in a new costume: a run that did not fully apply
    must never look like one that did.
    """
    with transaction.atomic():
        locked = ingestion.__class__.objects.select_for_update().get(pk=ingestion.pk)
        snapshot_info = dict(locked.snapshot_info or {})
        accepted = list(snapshot_info.get(ACCEPTED_MERGE_FAILURES_KEY) or [])
        accepted.append(
            {
                "accepted_at": timezone.now().isoformat(),
                "accepted_by": getattr(user, "username", "") or "",
                "failed_change_count": int(failed),
                "issue_ids": sorted(
                    ForwardIngestionIssue.objects.filter(
                        ingestion=locked,
                        phase=ForwardIngestionPhaseChoices.MERGE,
                    ).values_list("pk", flat=True)
                ),
            }
        )
        snapshot_info[ACCEPTED_MERGE_FAILURES_KEY] = accepted
        locked.snapshot_info = snapshot_info
        locked.save(update_fields=["snapshot_info"])
        ingestion.snapshot_info = snapshot_info


def merge_branch(
    ingestion: "ForwardIngestion",
    sync_logger: "SyncLogging | None" = None,
    *,
    user=None,
    merge_attempt=None,
    accept_reported_failures: bool = False,
) -> None:
    """Merge one staged branch.

    `accept_reported_failures` completes the merge even though rows failed. It
    exists because the alternative is a permanent dead end: a failed row leaves
    the branch READY and never attests the merge, so `merge_applied_at` stays
    null and `resume_post_merge_bookkeeping` cannot recover it either. The only
    exit is a retry with zero failures — and when a failure is deterministic,
    such as a unique-constraint clash on a row the source keeps re-sending, that
    retry never arrives. A customer sat with 5 failures out of 24,748 blocking
    the baseline, and with it drift measurement and diff-based syncs, forever.

    It must never be automatic. Nothing in the sync or retry path sets it; it is
    reachable only through an explicit operator action, the failures stay
    recorded as ingestion issues, and the acceptance is written into durable
    evidence so a later reader can see the baseline was promoted over known
    exceptions rather than a clean run.
    """
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
        initialize_merge_attempt(merge_attempt, total_changes=0)
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
        mark_merge_attempt_applied(merge_attempt)
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
    initialize_merge_attempt(
        merge_attempt,
        total_changes=logical_total_changes,
    )
    if sync_logger:
        sync_logger.log_info(
            f"Merge progress: 0/{logical_total_changes} changes merged; rate pending."
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

    set_based_decision = set_based_merge_decision(
        sync=ingestion.sync,
        branch=branch,
    )
    if sync_logger and bool(
        (getattr(ingestion.sync, "parameters", {}) or {}).get(
            "enable_set_based_merge", False
        )
    ):
        if set_based_decision.enabled:
            sync_logger.log_info(
                "Set-based merge enabled for `dcim.macaddress` "
                f"(spec {set_based_decision.context['model_spec_version']})."
            )
        else:
            sync_logger.log_info(
                "Set-based merge requested but failed closed "
                f"({set_based_decision.reason_code}); using the current path."
            )

    models_touched = set()
    failed = 0
    issue_recorder = _MergeIssueRecorder(ingestion, sync_logger)

    progress_merged = 0
    progress_failed = 0
    model_progress = defaultdict(lambda: {"merged": 0, "failed": 0})
    progress_started_at = time.monotonic()
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
            with (
                profile_scope(
                    "objectchange_apply",
                    model=model_string,
                    owner="upstream_netbox_branching",
                    rows=1,
                ),
                transaction.atomic(),
            ):
                if (
                    model_string == "dcim.device"
                    and getattr(
                        collapsed_change.final_action,
                        "value",
                        collapsed_change.final_action,
                    )
                    == "delete"
                ):
                    with profile_scope(
                        "authoritative_device_delete_guard",
                        model=model_string,
                        owner="ours",
                        rows=1,
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
                                "Authoritative device deletion is blocked by "
                                "current ownership evidence."
                            )
                        lock_related_writes_for_delete(
                            model_class,
                            using=DEFAULT_DB_ALIAS,
                        )
                with event_tracking(request):
                    dummy_change.apply(
                        branch,
                        using=DEFAULT_DB_ALIAS,
                        logger=logger,
                    )
            models_touched.add(model_class)
            return True
        except JobTimeoutException:
            raise
        except Exception as exc:  # noqa: BLE001 - preserve row-level isolation
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
        nonlocal progress_failed, last_heartbeat_at, last_log_at
        model_string = _model_string(collapsed_change.model_class)
        issue_recorder.record(
            model_string=model_string,
            exc=exc,
        )
        progress_failed += 1
        model_progress[model_string]["failed"] += 1
        if sync_logger:
            sync_logger.increment_statistics(model_string, outcome="failed")
        last_heartbeat_at, last_log_at = _report_merge_progress(
            merge_attempt,
            sync_logger=sync_logger,
            model_string=model_string,
            merged_changes=progress_merged,
            failed_changes=progress_failed,
            total_changes=logical_total_changes,
            model_progress=model_progress,
            started_at=progress_started_at,
            last_heartbeat_at=last_heartbeat_at,
            last_log_at=last_log_at,
        )

    def _record_applied(model_class):
        nonlocal progress_merged, last_heartbeat_at, last_log_at
        progress_merged += 1
        model_string = _model_string(model_class)
        model_progress[model_string]["merged"] += 1
        if sync_logger:
            sync_logger.increment_statistics(model_string)
        last_heartbeat_at, last_log_at = _report_merge_progress(
            merge_attempt,
            sync_logger=sync_logger,
            model_string=model_string,
            merged_changes=progress_merged,
            failed_changes=progress_failed,
            total_changes=logical_total_changes,
            model_progress=model_progress,
            started_at=progress_started_at,
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
            set_based_decision=set_based_decision,
        )
        failed += bulk_failed
        models_touched |= bulk_models
    finally:
        post_save.disconnect(handler, sender=ObjectChange_)

    last_heartbeat_at, last_log_at = _report_merge_progress(
        merge_attempt,
        sync_logger=sync_logger,
        model_string=(merge_attempt.current_model if merge_attempt is not None else ""),
        merged_changes=progress_merged,
        failed_changes=progress_failed,
        total_changes=logical_total_changes,
        model_progress=model_progress,
        started_at=progress_started_at,
        last_heartbeat_at=last_heartbeat_at,
        last_log_at=last_log_at,
        force=True,
    )

    if models_touched:
        strategy_class = get_merge_strategy(branch.merge_strategy)
        with profile_scope(
            "merge_cleanup",
            owner="upstream_netbox_branching",
            rows=len(models_touched),
        ):
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

    if failed and not accept_reported_failures:
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

    if failed:
        # Operator-accepted. Record it durably before attesting, so evidence of
        # the exception can never be lost by a later crash between the two.
        _record_accepted_failures(ingestion, failed=failed, user=user)
        accepted = (
            f"Merge completed with {failed} accepted failure(s): "
            f"{applied_count} applied. The failures remain recorded as "
            "ingestion issues and the baseline was promoted over them by an "
            "explicit operator action."
        )
        logger.warning(accepted)
        if sync_logger:
            sync_logger.log_warning(accepted)

    _attest_branch_merged(ingestion, branch, user)
    mark_merge_attempt_applied(merge_attempt)
    if retrying_partial:
        _retire_resolved_merge_issues(ingestion, prior_merge_issue_ids)

    logger.info(summary)
    if sync_logger:
        sync_logger.log_info(summary)


def _report_merge_progress(
    merge_attempt,
    *,
    sync_logger: "SyncLogging | None",
    model_string: str,
    merged_changes: int,
    failed_changes: int,
    total_changes: int,
    model_progress,
    started_at: float,
    last_heartbeat_at: float,
    last_log_at: float,
    force: bool = False,
) -> tuple[float, float]:
    now = time.monotonic()
    processed = merged_changes + failed_changes
    heartbeat_due = (
        force
        or processed == total_changes
        or processed % MERGE_HEARTBEAT_ROW_INTERVAL == 0
        or now - last_heartbeat_at >= MERGE_HEARTBEAT_SECONDS
    )
    log_due = (
        force
        or processed == total_changes
        or processed % MERGE_LOG_ROW_INTERVAL == 0
        or now - last_log_at >= MERGE_LOG_SECONDS
    )

    if heartbeat_due:
        checkpoint_merge_attempt(
            merge_attempt,
            merged_changes=merged_changes,
            failed_changes=failed_changes,
            current_model=model_string,
            model_progress=model_progress,
        )
        last_heartbeat_at = now

    if sync_logger and log_due:
        elapsed = max(0.0, now - started_at)
        rate = processed / elapsed if elapsed > 0 else 0.0
        remaining = max(0, total_changes - processed)
        eta = remaining / rate if rate > 0 else 0.0
        failure_suffix = f", {failed_changes} failed" if failed_changes else ""
        sync_logger.log_info(
            f"Merge progress: {merged_changes}/{total_changes} changes merged"
            f"{failure_suffix}; current model `{model_string or 'pending'}`; "
            f"rate {rate:.2f} changes/sec; elapsed {elapsed:.1f}s; "
            f"ETA {eta:.1f}s."
        )
        last_log_at = now

    return last_heartbeat_at, last_log_at
