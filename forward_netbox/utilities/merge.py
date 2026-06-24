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
from django.db import DEFAULT_DB_ALIAS
from django.db import transaction
from django.db.models.signals import post_save
from django.test import RequestFactory
from django.urls import reverse
from django.utils import timezone
from netbox.context_managers import event_tracking
from netbox_branching.choices import BranchEventTypeChoices
from netbox_branching.choices import BranchStatusChoices
from netbox_branching.merge_strategies import get_merge_strategy

from .bulk_merge import bulk_merge_changes
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

# Models the plugin never syncs directly: their branch changes are NetBox
# component-replication side effects. Creating a Device or Module instantiates
# ModuleBay rows from the device/module type's templates. ModuleBay has a custom
# MPTT save() that takes an UPDATE path when Branching deserializes the create
# with a pk, so the row never lands in main and every such change fails the
# merge with NotUpdated ("Save with update_fields did not affect any rows").
# This is a NetBox Branching <-> MPTT-ModuleBay limitation, not a plugin sync
# failure; one device with module bays can emit dozens of identical failures
# plus cascading module failures. Collapse them into a single actionable summary
# that points at the out-of-band remediation (forward_module_readiness) instead
# of flooding the ingestion issues list. Device/interface sync is unaffected.
REPLICATION_SIDE_EFFECT_MODELS = frozenset({"dcim.modulebay"})

MODULE_BAY_MERGE_REMEDIATION = (
    "{count} module-bay change(s) could not be merged because NetBox Branching "
    "cannot create MPTT module bays during a merge (a NetBox limitation, not a "
    "data error). The affected module bays were not created, so any modules "
    "targeting them were skipped. Run the `forward_module_readiness` management "
    "command and import the generated module-bay CSV directly into NetBox, then "
    "re-run module sync. Device and interface sync are unaffected."
)


class _MergeIssueRecorder:
    """Record merge-time change failures as ForwardIngestionIssue rows.

    Failures for models the plugin syncs directly are recorded one issue per
    change. Failures for replication side-effect models (see
    REPLICATION_SIDE_EFFECT_MODELS) are collapsed into a single actionable
    summary issue at ``flush()`` time, so one device's worth of unmergeable
    module bays does not flood the list with dozens of identical rows.
    """

    def __init__(self, ingestion, sync_logger):
        self._ingestion = ingestion
        self._sync_logger = sync_logger
        self._side_effect_counts: Counter = Counter()
        self._side_effect_samples: dict[str, str] = {}

    def record(self, *, model_string, message, exc):
        if model_string in REPLICATION_SIDE_EFFECT_MODELS:
            self._side_effect_counts[model_string] += 1
            self._side_effect_samples.setdefault(model_string, str(exc))
            logger.debug(message, exc_info=True)
            return
        logger.error(message, exc_info=True)
        if self._sync_logger:
            self._sync_logger.log_failure(message)
        ForwardIngestionIssue.objects.create(
            ingestion=self._ingestion,
            phase=ForwardIngestionPhaseChoices.MERGE,
            model=model_string,
            message=message,
            exception=exc.__class__.__name__,
            raw_data={"traceback": traceback.format_exc()},
        )

    def flush(self):
        for model_string, count in self._side_effect_counts.items():
            summary = MODULE_BAY_MERGE_REMEDIATION.format(count=count)
            if self._sync_logger:
                self._sync_logger.log_warning(summary)
            ForwardIngestionIssue.objects.create(
                ingestion=self._ingestion,
                phase=ForwardIngestionPhaseChoices.MERGE,
                model=model_string,
                message=summary,
                exception="ModuleBayMergeUnsupported",
                raw_data={
                    "sample_error": self._side_effect_samples.get(model_string, "")
                },
            )


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
    issue_recorder = _MergeIssueRecorder(ingestion, sync_logger)

    processed = 0
    last_heartbeat_at = time.monotonic()
    last_log_at = last_heartbeat_at
    step_index = _merge_step_index(ingestion)

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
        last = collapsed_change.last_change
        try:
            with transaction.atomic():
                with event_tracking(request):
                    request.id = getattr(last, "request_id", None)
                    request.user = getattr(last, "user", None) or user
                    dummy_change.apply(branch, using=DEFAULT_DB_ALIAS, logger=logger)
            models_touched.add(model_class)
            return True
        except Exception as exc:
            issue_recorder.record(
                model_string=model_string,
                message=(
                    f"Failed to apply collapsed change "
                    f"({collapsed_change.final_action} {model_string}: "
                    f"{collapsed_change.key[1]}): {exc}"
                ),
                exc=exc,
            )
            return False

    def _record_applied(model_class):
        nonlocal processed, last_heartbeat_at, last_log_at
        processed += 1
        if sync_logger:
            sync_logger.increment_statistics(_model_string(model_class))
        last_heartbeat_at, last_log_at = _report_merge_progress(
            ingestion,
            sync_logger=sync_logger,
            model_string=_model_string(model_class),
            step_index=step_index,
            processed=processed,
            total_changes=total_changes,
            last_heartbeat_at=last_heartbeat_at,
            last_log_at=last_log_at,
        )

    try:
        applied_count, bulk_failed, bulk_models = bulk_merge_changes(
            branch,
            changes,
            request,
            user,
            logger,
            apply_one=_apply_one,
            record_applied=_record_applied,
        )
        failed += bulk_failed
        models_touched |= bulk_models
    finally:
        post_save.disconnect(handler, sender=ObjectChange_)

    issue_recorder.flush()

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
