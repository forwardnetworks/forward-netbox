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
from .diagnostics import describe_failure
from .diagnostics import diagnostic_shape
from .diagnostics import exception_type
from .diagnostics import is_caused_rule_rejection
from .diagnostics import safe_operation_failure
from .diagnostics import structured_failure_diagnosis
from .diagnostics import with_raise_site
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


def _is_destination_rule_rejection(exc) -> bool:
    """True when the destination refused the row on a validation rule.

    A `ValidationError` at merge is NetBox declining a row on one of its own
    model rules — an address that may not sit on an interface, a reassignment
    it forbids. Re-running cannot change the answer: nothing about the incoming
    data is retryable, because the rule is a property of the destination.

    That distinction matters because any failed row blocks baseline promotion
    outright, so a handful of permanently unsatisfiable rows could hold a
    customer's entire convergence hostage indefinitely — one address stopping
    thousands of correct changes from ever being attested. Integrity errors and
    everything else stay failures: those are usually ordering or contention and
    a retry is exactly the right response.

    The one exception is a rejection the merge itself caused: a rule the
    catalogue can name, landing on a field this change writes. Retrying that is
    exactly the right response, and calling it unsatisfiable hides a real defect
    behind a disposition invented for rows nothing can fix.

    Deliberately NOT symmetric with the sync path, which skips only what it can
    name and fails everything else. The defaults are opposite, so narrowing here
    the same way would flip every uncatalogued rejection to a failure and wedge
    the baselines this classifier exists to protect. An uncatalogued rule, and a
    caller that never said what it writes, both stay unsatisfiable.
    """
    from django.core.exceptions import ValidationError

    if not isinstance(exc, ValidationError):
        return False
    written_fields = getattr(exc, "forward_written_fields", None)
    if written_fields is None:
        return True
    return not is_caused_rule_rejection(exc, written_fields)


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


def _row_identity(pk, change_data):
    """Name the failed row by its NetBox pk, never by its own values.

    Returns ``None`` when there is no pk to report, so a caller that has no
    row in hand — the orchestration paths that record a whole-model failure —
    keeps the message it always had, byte for byte.
    """
    if pk is None:
        return None
    # `CollapsedChange.key` is `(label_lower, pk)` and the pk can be an int or a
    # UUID depending on the model. `str` is what makes it both readable in the
    # message and safe to store in a JSONField; nothing downstream does
    # arithmetic on it.
    rendered = str(pk).strip()
    if not rendered:
        return None
    return {
        "pk": rendered,
        # The row's fields, not their values — same treatment the sync-phase
        # recorder gives `raw_data`.
        "shape": diagnostic_shape(change_data) if change_data is not None else None,
        "sentence": f"Affected NetBox row: pk {rendered}.",
    }


class _MergeIssueRecorder:
    """Record merge-time change failures as ForwardIngestionIssue rows."""

    def __init__(self, ingestion, sync_logger):
        self._ingestion = ingestion
        self._sync_logger = sync_logger

    def record(self, *, model_string, exc, pk=None, change_data=None):
        # The diagnosis has always been stored in `raw_data`, but the issue list
        # shows only the message — so the constraint or field sat one click away
        # while the list said nothing but the exception class. The sync-phase
        # recorders were given this treatment and the merge recorder was not,
        # which left the answer visible in two of three places.
        diagnosis = structured_failure_diagnosis(exc)
        message = describe_failure(
            safe_operation_failure(f"Merge for {model_string}", exc),
            diagnosis,
        )
        # Same treatment as the sync recorder: name the line it came from. The
        # merge recorder was the one left behind last time this file gained a
        # diagnostic, which is the reason for doing both together.
        message = with_raise_site(message, diagnosis)
        if _is_destination_rule_rejection(exc):
            # The issue list is where an operator actually looks, and a row that
            # was skipped reads there exactly like one that will be retried.
            # Keep the machine-readable prefix; state the disposition after it.
            message = (
                f"{message} Recorded and skipped: re-running cannot change a "
                "NetBox validation rejection, so the baseline was promoted "
                "over this row."
            )
        # An unsatisfiable row is resolved by editing it in NetBox, so "which
        # row" is the one thing the operator has to know and the only thing the
        # record did not say. A rejection reading "Merge for ipam.ipaddress
        # failed ... violating primary-ip-reassignment-blocked" left them
        # hunting for the address by hand.
        #
        # The identity is the NetBox primary key, NOT the row's own values. The
        # pk is a NetBox-assigned surrogate that resolves to the object page in
        # the operator's own NetBox, which is exactly where the edit has to
        # happen; the address, device name and interface are customer data that
        # this module refuses to persist anywhere else (`diagnostic_shape` keeps
        # field names only, and the note below says the same about DETAIL
        # values). Naming the pk answers the question without opening a hole in
        # that boundary — and the pk was already being logged in the clear a few
        # frames down, in the authoritative-delete guard.
        row_identity = _row_identity(pk, change_data)
        if row_identity:
            message = f"{message} {row_identity['sentence']}"
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
        raw_data = dict(diagnosis)
        if row_identity:
            # Same split the sync recorder uses: the pk is an identifier, the
            # change data is recorded as a shape. Keys stay disjoint from the
            # diagnosis so anything already reading `raw_data` is unaffected.
            raw_data["row_pk"] = row_identity["pk"]
            if row_identity["shape"] is not None:
                raw_data["row"] = row_identity["shape"]
        ForwardIngestionIssue.objects.create(
            ingestion=self._ingestion,
            phase=ForwardIngestionPhaseChoices.MERGE,
            model=model_string,
            message=message,
            exception=exc.__class__.__name__,
            raw_data=raw_data,
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
    previous_skipped = int(ingestion.skipped_change_count or 0)
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
            # Previously skipped rows were never applied, so they stay skipped
            # rather than being folded into the applied total.
            ingestion.record_change_totals(
                applied=previous_applied + previous_failed,
                failed=0,
                skipped=previous_skipped,
                created=int(ingestion.created_change_count or 0),
                updated=int(ingestion.updated_change_count or 0),
                deleted=int(ingestion.deleted_change_count or 0),
            )
        else:
            ingestion.record_change_totals(
                applied=0,
                failed=0,
                skipped=0,
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
    previous_logical_total = previous_applied + previous_failed + previous_skipped
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
    progress_unsatisfiable = 0
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
        nonlocal progress_failed, progress_unsatisfiable
        nonlocal last_heartbeat_at, last_log_at
        model_string = _model_string(collapsed_change.model_class)
        key = getattr(collapsed_change, "key", None)
        issue_recorder.record(
            model_string=model_string,
            exc=exc,
            pk=key[1] if isinstance(key, (tuple, list)) and len(key) > 1 else None,
            change_data=getattr(collapsed_change, "postchange_data", None),
        )
        unsatisfiable = _is_destination_rule_rejection(exc)
        if unsatisfiable:
            progress_unsatisfiable += 1
        progress_failed += 1
        model_progress[model_string]["failed"] += 1
        if sync_logger:
            # The row is still an exception and still an ingestion issue; what
            # differs is that no retry can satisfy it, so per-model statistics
            # must not present it as something a rerun would clear.
            sync_logger.increment_statistics(
                model_string,
                outcome="skipped" if unsatisfiable else "failed",
            )
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
            merge_sync_id=ingestion.sync_id,
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

    # Rows the destination refused on its own validation rules are recorded and
    # skipped rather than treated as retryable failures. Retrying cannot change
    # the outcome, so counting them as failures is what turns a handful of
    # unsatisfiable addresses into a baseline that can never promote at all.
    # They stay visible as ingestion issues and reappear in drift every run.
    #
    # The split has to reach the persisted counters, not just the raise decision
    # below. Every readiness surface downstream - drift evidence, the ingestion
    # health check, throughput - keys off `failed_change_count`, so recording
    # the raw total here left a skipped row presenting exactly like the hard
    # block it was meant to stop being.
    unsatisfiable = min(progress_unsatisfiable, failed)
    retryable_failed = failed - unsatisfiable

    reported = []
    if retryable_failed:
        reported.append(f"{retryable_failed} failed")
    if unsatisfiable:
        reported.append(f"{unsatisfiable} skipped")
    failed_message = (
        f"{', '.join(reported)} (recorded as ingestion issues)."
        if reported
        else "no failed."
    )
    summary = f"Merge completed: {applied_count} applied, {failed_message}"
    cumulative_applied = applied_count
    logical_action_counts = merge_metadata.get("logical_action_counts", {})
    created = int(logical_action_counts.get("create", 0))
    updated = int(logical_action_counts.get("update", 0))
    deleted = int(logical_action_counts.get("delete", 0))
    # `applied + failed + skipped` must stay equal to the logical branch total:
    # a READY branch reports every change as unmerged, so a partial-merge retry
    # recomputes the same total and refuses to run when it disagrees with the
    # cumulative evidence recorded here.
    ingestion.record_change_totals(
        applied=cumulative_applied,
        failed=retryable_failed,
        skipped=unsatisfiable,
        created=created,
        updated=updated,
        deleted=deleted,
    )

    if unsatisfiable:
        skipped_note = (
            f"{unsatisfiable} row(s) were rejected by NetBox validation rules "
            "and skipped; they remain recorded as ingestion issues."
        )
        logger.warning(skipped_note)
        if sync_logger:
            sync_logger.log_warning(skipped_note)

    if retryable_failed and not accept_reported_failures:
        branch.status = BranchStatusChoices.READY
        branch.save(update_fields=["status", "last_updated"])
        summary = (
            f"Merge incomplete: {applied_count} applied, "
            f"{retryable_failed} failed. "
            "The branch remains ready for inspection and retry."
        )
        logger.error(summary)
        if sync_logger:
            sync_logger.log_failure(summary)
        raise ForwardPartialMergeError(
            summary,
            applied=applied_count,
            failed=retryable_failed,
        )

    if retryable_failed:
        # Operator-accepted. Record it durably before attesting, so evidence of
        # the exception can never be lost by a later crash between the two.
        _record_accepted_failures(ingestion, failed=retryable_failed, user=user)
        accepted = (
            f"Merge completed with {retryable_failed} accepted failure(s): "
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
