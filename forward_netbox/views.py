import logging
import time

from core.choices import ObjectChangeActionChoices
from core.exceptions import SyncError
from core.models import ObjectChange
from django.apps import apps
from django.contrib import messages
from django.core.exceptions import ValidationError
from django.db import models
from django.db.models.functions import Greatest
from django.http import HttpResponseBadRequest
from django.http import JsonResponse
from django.shortcuts import get_object_or_404
from django.shortcuts import redirect
from django.shortcuts import render
from django.urls import reverse
from django.utils import timezone
from django.utils.translation import gettext as _
from netbox.object_actions import AddObject
from netbox.object_actions import BulkDelete
from netbox.object_actions import BulkEdit
from netbox.object_actions import BulkExport
from netbox.object_actions import BulkRename
from netbox.views import generic
from netbox.views.generic.base import BaseObjectView
from netbox_branching.models import ChangeDiff
from utilities.data import shallow_compare_dict
from utilities.forms import restrict_form_fields
from utilities.views import get_viewname
from utilities.views import register_model_view
from utilities.views import ViewTab

from .exceptions import ForwardQueryError
from .filtersets import ForwardDeviceAnalysisFilterSet
from .filtersets import ForwardDriftPolicyFilterSet
from .filtersets import ForwardIngestionChangeFilterSet
from .filtersets import ForwardIngestionFilterSet
from .filtersets import ForwardIngestionIssueFilterSet
from .filtersets import ForwardIngestionObjectChangeFilterSet
from .filtersets import ForwardNQEMapFilterSet
from .filtersets import ForwardSourceFilterSet
from .filtersets import ForwardSyncFilterSet
from .filtersets import ForwardValidationRunFilterSet
from .forms import ForwardDriftPolicyBulkEditForm
from .forms import ForwardDriftPolicyForm
from .forms import ForwardIngestionMergeForm
from .forms import ForwardNQEMapBulkEditForm
from .forms import ForwardNQEMapForm
from .forms import ForwardSourceBulkEditForm
from .forms import ForwardSourceForm
from .forms import ForwardSyncBulkEditForm
from .forms import ForwardSyncForm
from .forms import ForwardValidationRunForceAllowForm
from .models import ForwardDeviceAnalysis
from .models import ForwardDriftPolicy
from .models import ForwardIngestion
from .models import ForwardIngestionIssue
from .models import ForwardNQEMap
from .models import ForwardSource
from .models import ForwardSync
from .models import ForwardValidationRun
from .tables import ForwardDeviceAnalysisTable
from .tables import ForwardDriftPolicyTable
from .tables import ForwardIngestionChangesTable
from .tables import ForwardIngestionIssueTable
from .tables import ForwardIngestionObjectChangesTable
from .tables import ForwardIngestionTable
from .tables import ForwardNQEMapTable
from .tables import ForwardSourceTable
from .tables import ForwardSyncTable
from .tables import ForwardValidationRunTable
from .utilities.bulk_merge import describe_protecting_references
from .utilities.change_explainability import change_explainability_summary
from .utilities.diagnostics import diff_fallback_summary
from .utilities.diagnostics import model_failure_summary
from .utilities.diagnostics import safe_job_error_summary
from .utilities.diagnostics import sanitize_job_diagnostics
from .utilities.direct_changes import object_changes_for_ingestion
from .utilities.execution_telemetry import build_plan_preview
from .utilities.health import live_data_file_health_check
from .utilities.health import live_source_health_check
from .utilities.health import sync_health_summary
from .utilities.json_safe import json_safe_value
from .utilities.query_binding import apply_explicit_nqe_map_bindings
from .utilities.query_binding import build_nqe_map_bindings
from .utilities.query_binding import live_query_binding_drifts
from .utilities.query_binding import NQE_LIBRARY_WRITE_PERMISSION_MESSAGE
from .utilities.query_binding import NQELibraryWritePermissionError
from .utilities.query_binding import publish_builtin_nqe_map_queries
from .utilities.query_binding import resolve_nqe_map_query_ids
from .utilities.query_binding import restore_builtin_raw_query_bindings
from .utilities.support_bundle_archive import support_bundle_zip_response

logger = logging.getLogger(__name__)


_PREVIEW_PLAN_ITEM_LIMIT = 25


def annotate_statistics(queryset):
    counted_changes = models.Q(
        branch__changediff__action__in=[
            ObjectChangeActionChoices.ACTION_CREATE,
            ObjectChangeActionChoices.ACTION_UPDATE,
            ObjectChangeActionChoices.ACTION_DELETE,
        ]
    ) & ~models.Q(branch__changediff__object_type__model="objectchange")
    return queryset.annotate(
        num_created=Greatest(
            models.F("created_change_count"),
            models.Count(
                "branch__changediff",
                filter=models.Q(
                    branch__changediff__action=ObjectChangeActionChoices.ACTION_CREATE
                )
                & ~models.Q(branch__changediff__object_type__model="objectchange"),
            ),
            output_field=models.IntegerField(),
        ),
        num_updated=Greatest(
            models.F("updated_change_count"),
            models.Count(
                "branch__changediff",
                filter=models.Q(
                    branch__changediff__action=ObjectChangeActionChoices.ACTION_UPDATE
                )
                & ~models.Q(branch__changediff__object_type__model="objectchange"),
            ),
            output_field=models.IntegerField(),
        ),
        num_deleted=Greatest(
            models.F("deleted_change_count"),
            models.Count(
                "branch__changediff",
                filter=models.Q(
                    branch__changediff__action=ObjectChangeActionChoices.ACTION_DELETE
                )
                & ~models.Q(branch__changediff__object_type__model="objectchange"),
            ),
            output_field=models.IntegerField(),
        ),
        description=models.F("branch__description"),
        user=models.F("sync__user__username"),
        staged_changes=Greatest(
            models.F("applied_change_count"),
            models.Count("branch__changediff", filter=counted_changes),
            output_field=models.IntegerField(),
        ),
        branch_name=models.F("branch__name"),
        sync_name=models.F("sync__name"),
    )


def _job_export_payload(job):
    if not job:
        return None
    return {
        "pk": job.pk,
        "status": getattr(job, "status", ""),
        "created": getattr(job, "created", None),
        "started": getattr(job, "started", None),
        "completed": getattr(job, "completed", None),
        "duration": getattr(job, "duration", None),
        "error": safe_job_error_summary(getattr(job, "error", "")),
        "data": json_safe_value(
            sanitize_job_diagnostics(getattr(job, "data", {}) or {})
        ),
        "log_entries": json_safe_value(
            sanitize_job_diagnostics(
                {"log_entries": list(getattr(job, "log_entries", []) or [])}
            )["log_entries"]
        ),
    }


def _latest_dependency_preview_bundle_payload(sync, latest_ingestion):
    """Return aggregate preview evidence without customer inventory rows."""
    from core.choices import JobStatusChoices
    from core.models import Job
    from django.contrib.contenttypes.models import ContentType

    from .utilities.drift_report import build_latest_sync_evidence, compute_drift_report

    job = (
        Job.objects.filter(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=sync.pk,
            name__icontains="dependency preview",
            status=JobStatusChoices.STATUS_COMPLETED,
        )
        .order_by("-created")
        .first()
    )
    if job is None or not isinstance(job.data, dict) or not job.data:
        return None

    data = job.data
    context = data.get("context") if isinstance(data.get("context"), dict) else {}
    return json_safe_value(
        {
            "job": {
                "pk": job.pk,
                "status": getattr(job, "status", ""),
                "created": getattr(job, "created", None),
                "completed": getattr(job, "completed", None),
                "duration": getattr(job, "duration", None),
            },
            "generated_at": data.get("generated_at"),
            "context": {
                "snapshot_id": context.get("snapshot_id") or "",
                "snapshot_selector": context.get("snapshot_selector") or "",
            },
            "change_estimate_kind": data.get("change_estimate_kind") or "",
            "plan_preview": data.get("plan_preview") or {},
            "model_results": data.get("model_results") or [],
            "forward_api_usage": data.get("forward_api_usage") or {},
            "drift_report": compute_drift_report(data),
            "latest_sync_evidence": build_latest_sync_evidence(
                latest_ingestion,
                data,
            ),
        }
    )


def _ingestion_log_export_payload(ingestion, *, active_stage):
    from .utilities.ownership import (
        ownership_finalization_summary,
        ownership_integrity_summary,
    )

    return {
        "exported_at": timezone.now().isoformat(),
        "active_stage": active_stage,
        "ingestion": {
            "pk": ingestion.pk,
            "name": ingestion.name,
            "sync_mode": ingestion.sync_mode or "",
            "baseline_ready": bool(ingestion.baseline_ready),
            "snapshot_id": ingestion.snapshot_id or "",
            "snapshot_selector": ingestion.snapshot_selector or "",
            "branch": ingestion.branch.name if ingestion.branch else "",
            "sync_status": ingestion.sync.status,
            "job": _job_export_payload(ingestion.job),
            "merge_job": _job_export_payload(ingestion.merge_job),
            "fast_baseline_attestation": json_safe_value(
                (ingestion.snapshot_info or {}).get("fast_baseline_load") or {}
            ),
        },
        "sync": {
            "pk": ingestion.sync.pk,
            "name": ingestion.sync.name,
            "status": ingestion.sync.status,
            "current_activity": ingestion.sync.get_sync_activity(),
            "analysis_summary": ingestion.sync.get_analysis_summary(),
            "workload_summary": ingestion.sync.get_workload_summary(),
            "advisory_summary": ingestion.sync.get_advisory_summary(),
        },
        "change_explainability": json_safe_value(
            change_explainability_summary(ingestion)
        ),
        "ownership_integrity": json_safe_value(ownership_integrity_summary()),
        "ownership_finalization": json_safe_value(
            ownership_finalization_summary(
                ingestion.sync,
                generation=ingestion.pk,
            )
        ),
        # The issues are the reason anybody downloads this. Without them the
        # export described everything around a failure except the failure, and
        # answering "which rule rejected the row" still meant clicking through
        # the GUI one issue at a time - or reading server logs that never
        # contained it.
        "issues": _ingestion_issue_bundle_payload(ingestion),
        "job_results": json_safe_value(ingestion.get_job_logs(ingestion.job)),
        "merge_job_results": json_safe_value(
            ingestion.get_job_logs(ingestion.merge_job)
        ),
    }


_SUPPORT_BUNDLE_ISSUE_LIMIT = 200


def _ingestion_issue_bundle_payload(ingestion):
    """The issue rows behind a failed ingestion, for the support bundle.

    The bundle carried change *counts* but never the issues themselves, so it
    could not answer the one question anybody exports it to ask: what actually
    failed. A customer's merge failure was diagnosable only by reading `raw_data`
    in the GUI one row at a time, and the exported bundle - the thing meant to
    remove that step - omitted it entirely.

    `raw_data` is the structured diagnosis, which holds schema identifiers and
    matched rule slugs rather than row values, so including it does not widen
    what the bundle discloses beyond what the issue list already shows.
    """
    from .models import ForwardIngestionIssue

    if ingestion is None:
        return {"total": 0, "returned": 0, "issues": []}

    queryset = ForwardIngestionIssue.objects.filter(ingestion=ingestion).order_by("pk")
    total = queryset.count()
    rows = []
    for issue in queryset[:_SUPPORT_BUNDLE_ISSUE_LIMIT]:
        rows.append(
            {
                "pk": issue.pk,
                "timestamp": (issue.timestamp.isoformat() if issue.timestamp else None),
                "phase": issue.phase,
                "model": issue.model or "",
                "exception": issue.exception or "",
                "message": issue.message or "",
                "diagnosis": json_safe_value(issue.raw_data or {}),
            }
        )
    # A truncated export that looks complete is worse than one that says so:
    # a large ingestion can produce thousands of rows, and silently keeping the
    # first 200 would read as "these are all of them".
    return {
        "total": total,
        "returned": len(rows),
        "truncated": total > len(rows),
        "issues": rows,
    }


def _sync_support_bundle_payload(sync):
    from .utilities.ownership import (
        ownership_finalization_summary,
        ownership_integrity_summary,
    )
    from .utilities.sync_facade import effective_scope_endpoints_by_include_tags
    from .utilities.upgrade_reconciliation import compute_upgrade_reconciliation

    latest_ingestion = sync.last_ingestion
    source_parameters = dict(sync.source.parameters or {})
    include_tags = source_parameters.get("device_tag_include_tags") or []
    exclude_tags = source_parameters.get("device_tag_exclude_tags") or []
    sync_device_tags = source_parameters.get("sync_device_tags") or []
    health = sync_health_summary(sync)
    return {
        "exported_at": timezone.now().isoformat(),
        "sync": {
            "pk": sync.pk,
            "name": sync.name,
            "status": sync.status,
            "source": sync.source_id,
            "scope_configuration": {
                "sync_endpoints": bool(source_parameters.get("sync_endpoints")),
                "sync_generic_endpoints": bool(
                    source_parameters.get("sync_generic_endpoints")
                ),
                "scope_endpoints_by_include_tags": (
                    effective_scope_endpoints_by_include_tags(source_parameters)
                ),
                "apply_device_scope_tags": bool(
                    source_parameters.get("apply_device_scope_tags")
                ),
                "sync_device_tag_count": len(sync_device_tags),
                "include_tag_count": len(include_tags),
                "exclude_tag_count": len(exclude_tags),
                "include_match": str(
                    source_parameters.get("device_tag_include_match") or "any"
                ),
                "filter_mode": str(
                    source_parameters.get("device_tag_filter_mode") or "local"
                ),
                "prune_out_of_scope": bool(
                    source_parameters.get("device_tag_prune_out_of_scope")
                ),
            },
            "current_activity": sync.get_sync_activity(),
            "analysis_summary": sync.get_analysis_summary(),
            "workload_summary": sync.get_workload_summary(),
            "advisory_summary": sync.get_advisory_summary(),
        },
        "query_drift_summary": health.get("query_drift_summary", {}),
        "query_drift_results": health.get("query_modes", {}).get("local_drift", []),
        "upgrade_reconciliation": json_safe_value(
            compute_upgrade_reconciliation(include_samples=False)
        ),
        "ownership_integrity": json_safe_value(ownership_integrity_summary()),
        "ownership_finalization": json_safe_value(
            ownership_finalization_summary(
                sync,
                generation=getattr(latest_ingestion, "pk", None),
            )
        ),
        "latest_dependency_preview": _latest_dependency_preview_bundle_payload(
            sync,
            latest_ingestion,
        ),
        "latest_ingestion_issues": _ingestion_issue_bundle_payload(latest_ingestion),
        "latest_ingestion": (
            {
                "pk": latest_ingestion.pk,
                "name": latest_ingestion.name,
                "sync_mode": latest_ingestion.sync_mode or "",
                "baseline_ready": bool(latest_ingestion.baseline_ready),
                "snapshot_id": latest_ingestion.snapshot_id or "",
                "snapshot_selector": latest_ingestion.snapshot_selector or "",
                "catchup": {
                    "status": latest_ingestion.catchup_status,
                    "target_snapshot_id": (
                        latest_ingestion.catchup_target_snapshot_id or ""
                    ),
                    "reason": latest_ingestion.catchup_reason or "",
                    "error_type": latest_ingestion.catchup_error_type or "",
                    "checked_at": (
                        latest_ingestion.catchup_checked_at.isoformat()
                        if latest_ingestion.catchup_checked_at
                        else None
                    ),
                },
                "change_counts": {
                    "applied": int(latest_ingestion.applied_change_count or 0),
                    "failed": int(latest_ingestion.failed_change_count or 0),
                    "skipped": int(latest_ingestion.skipped_change_count or 0),
                    "created": int(latest_ingestion.created_change_count or 0),
                    "updated": int(latest_ingestion.updated_change_count or 0),
                    "deleted": int(latest_ingestion.deleted_change_count or 0),
                },
                "diff_fallbacks": diff_fallback_summary(latest_ingestion.model_results),
                # On a successful dependency preview the bundle carries per-model
                # evidence; on the run that actually failed it carried none, so a
                # wholesale fetch failure exported as N copies of one sentence.
                "model_failures": model_failure_summary(latest_ingestion.model_results),
                "branch": (
                    latest_ingestion.branch.name if latest_ingestion.branch else ""
                ),
                "job": _job_export_payload(latest_ingestion.job),
                "merge_job": _job_export_payload(latest_ingestion.merge_job),
                "change_explainability": json_safe_value(
                    change_explainability_summary(latest_ingestion)
                ),
                "fast_baseline_attestation": json_safe_value(
                    (latest_ingestion.snapshot_info or {}).get("fast_baseline_load")
                    or {}
                ),
            }
            if latest_ingestion is not None
            else None
        ),
        "health": json_safe_value(health),
    }


def _download_json_response(payload, filename):
    response = JsonResponse(payload, json_dumps_params={"indent": 2}, safe=True)
    response["Content-Disposition"] = f'attachment; filename="{filename}"'
    return response


def _dependency_plan_item_summary(item):
    return {
        "index": item.index,
        "model": item.model_string,
        "label": item.label,
        "estimated_changes": item.estimated_changes,
        "upsert_count": len(item.upsert_rows),
        "delete_count": len(item.delete_rows),
        "operation": item.operation,
        "sync_mode": item.sync_mode,
        "query_name": item.query_name,
        "execution_mode": item.execution_mode or "unknown",
        "fetch_mode": item.fetch_mode or "unknown",
        "fetch_key_family": item.fetch_key_family or "",
        "query_runtime_ms": item.query_runtime_ms,
        "apply_engine": item.apply_engine,
        "shard_key_count": len(item.shard_keys or ()),
    }


def _dependency_model_result_summary(
    result, comparison=None, comparison_runtime_ms=None
):
    # ``fetcher.model_results`` are ForwardModelResult dataclasses, not dicts —
    # calling result.get(...) on them raised AttributeError and errored the whole
    # dependency preview (hidden as null-data until 2.2.4 surfaced job errors).
    from .utilities.query_fetch_execution import ForwardModelResult

    if not isinstance(result, ForwardModelResult):
        raise TypeError("Dependency preview results must be ForwardModelResult values.")
    data = result.as_dict()
    row_count = int(data.get("row_count") or 0)
    delete_count = int(data.get("delete_count") or 0)
    durable_state = next(
        (
            diagnostic
            for diagnostic in data.get("diagnostics") or []
            if diagnostic.get("type") == "durable_workload_state"
        ),
        None,
    )
    return {
        "model": data.get("model") or "",
        "query_name": data.get("query_name") or "",
        "execution_mode": data.get("execution_mode") or "unknown",
        "fetch_mode": data.get("fetch_mode") or "unknown",
        "row_count": row_count,
        "delete_count": delete_count,
        "failure_count": int(data.get("failure_count") or 0),
        "failure_exception": str(data.get("failure_exception") or ""),
        "failure_reason": str(data.get("failure_reason") or ""),
        # Per-model change estimate. With a comparison this is how many objects
        # actually differ - creates plus updates, deletes counted separately by
        # the drift report - so the figure means what the page has always said
        # it meant. Without one it stays the upper bound: every fetched row,
        # because nothing compared them.
        "estimated_changes": (
            comparison["creates"] + comparison["updates"]
            if comparison
            else row_count + delete_count
        ),
        "change_estimate_kind": (
            "exact_comparison" if comparison else "workload_upper_bound"
        ),
        # Rows the comparison could not classify because they carry no usable
        # identity. Reported rather than folded into drift, which would read as
        # a difference between the two systems when it is a defect in the row.
        "comparison_rejected_rows": (comparison or {}).get("rejected", 0),
        "unchanged_rows": (comparison or {}).get("unchanged", 0),
        # How long this model's comparison took. Recorded for every model that
        # was compared, so a slow one can be named rather than inferred from a
        # total. `None` where there was no comparison to time.
        "comparison_runtime_ms": (
            comparison_runtime_ms if comparison is not None else None
        ),
        "runtime_ms": float(data.get("runtime_ms") or 0.0),
        # Preserve aggregate state evidence without copying diagnostic samples or
        # source identifiers into the preview job payload.
        "durable_workload_state": durable_state,
    }


def _dependency_dry_run_payload(sync, *, client=None):
    from .utilities.api_usage import record_forward_api_usage
    from .utilities.branch_budget import build_branch_plan
    from .utilities.query_fetch import ForwardQueryFetcher

    client = client or sync.source.get_client()
    fetcher = ForwardQueryFetcher(sync, client, sync.logger)
    context = fetcher.resolve_context()
    workloads = fetcher.fetch_workloads(
        context, include_diagnostics=True, capture_comparison_rows=True
    )
    failed_models = [
        result.model_string
        for result in fetcher.model_results
        if int(result.failure_count or 0) > 0
    ]
    if failed_models:
        sample = ", ".join(failed_models[:5])
        suffix = "" if len(failed_models) <= 5 else ", ..."
        raise ForwardQueryError(
            "Dependency preview query validation failed for "
            f"{len(failed_models)} model(s): {sample}{suffix}."
        )
    plan = build_branch_plan(
        workloads,
        max_changes_per_staging_item=sync.get_max_changes_per_staging_item(),
        oversized_bucket_policy="warn",
    )
    plan_preview = build_plan_preview(
        plan, max_changes_per_staging_item=sync.get_max_changes_per_staging_item()
    )
    plan_items = [_dependency_plan_item_summary(item) for item in plan]
    context_dict = context.as_dict()

    # Compare the fetched rows against NetBox so the drift report can state how
    # many objects actually differ. This is the NetBox-side read the preview
    # never did; it costs no Forward calls, because the Forward half is already
    # in `workloads`. Models with no comparison yet return None and keep their
    # upper-bound estimate rather than reporting a confident zero.
    from .utilities.drift_comparison import compare_model_rows

    # Measure against the rows the fetch produced, NOT against `workloads`.
    #
    # `workloads` is the PLAN, and the plan legitimately excludes any model with
    # nothing to stage: `apply_durable_workload_deltas` drops a workload whose
    # upsert and delete lists are both empty. Building the comparison from it
    # meant a model that had not changed since the last run was absent from
    # `comparison_by_model`, so its `.get()` returned None, the report called it
    # "Not measured", and its estimate fell back to every fetched row.
    #
    # The result was an exact inversion: the models in perfect sync were the
    # ones displayed as maximally uncertain. One deployment saw 30 of 32 models
    # that way, each showing change candidates precisely equal to its Forward
    # row count, which is the signature of the fallback rather than of drift.
    rows_by_model = fetcher.comparison_rows_by_model
    # Time it, per model.
    #
    # This comparison is work the preview did not do before: on a converged
    # estate every model now classifies its full row set, where previously the
    # whole thing was skipped and reported as unmeasured. The job cannot time
    # out over it - `enqueue_forward_job` floors every Forward job at 7200s and
    # the entire 1M-row merge projection is under 1800s - but "cannot fail" is
    # not the same as "costs nothing", and how long it actually takes on a large
    # fabric has never been observed.
    #
    # So the preview reports its own cost instead of anyone estimating it. The
    # numbers arrive with the payload, from the estate that has the scale.
    comparison_by_model = {}
    comparison_runtime_ms_by_model = {}
    comparison_rows_by_model_count = {}
    for model_string, rows in rows_by_model.items():
        started = time.perf_counter()
        comparison_by_model[model_string] = compare_model_rows(sync, model_string, rows)
        comparison_runtime_ms_by_model[model_string] = round(
            (time.perf_counter() - started) * 1000, 1
        )
        comparison_rows_by_model_count[model_string] = len(rows)
    measured_models = sum(
        1 for value in comparison_by_model.values() if value is not None
    )
    # Rows the comparison actually read, counted only for models it could
    # compare. Rows handed to a model with no comparison were not measured, and
    # including them would make the cost per row look better than it is.
    compared_rows = sum(
        count
        for model_string, count in comparison_rows_by_model_count.items()
        if comparison_by_model.get(model_string) is not None
    )
    comparison_runtime_ms = round(
        sum(
            runtime
            for model_string, runtime in comparison_runtime_ms_by_model.items()
            if comparison_by_model.get(model_string) is not None
        ),
        1,
    )
    return {
        "generated_at": timezone.now().isoformat(),
        "sync": {
            "pk": sync.pk,
            "name": sync.name,
            "source": sync.source_id,
        },
        "context": {
            "network_id": context_dict.get("network_id"),
            "snapshot_id": context_dict.get("snapshot_id"),
            "snapshot_selector": context_dict.get("snapshot_selector"),
        },
        "plan_preview": plan_preview,
        "plan_items_count": len(plan_items),
        "plan_items_truncated": len(plan_items) > _PREVIEW_PLAN_ITEM_LIMIT,
        "plan_items": plan_items[:_PREVIEW_PLAN_ITEM_LIMIT],
        # Payload-level kind describes the weakest model in the set, so a
        # partially compared preview never advertises itself as exact.
        "change_estimate_kind": (
            "exact_comparison"
            if comparison_by_model and measured_models == len(comparison_by_model)
            else "workload_upper_bound"
        ),
        "comparison_coverage": {
            "measured_models": measured_models,
            "total_models": len(comparison_by_model),
            # What the measurement cost, reported rather than estimated.
            "runtime_ms": comparison_runtime_ms,
            "rows_compared": compared_rows,
        },
        "model_results": [
            _dependency_model_result_summary(
                result,
                comparison=comparison_by_model.get(
                    (result.as_dict().get("model") or "")
                ),
                comparison_runtime_ms=comparison_runtime_ms_by_model.get(
                    (result.as_dict().get("model") or "")
                ),
            )
            for result in fetcher.model_results
        ],
        "forward_api_usage": record_forward_api_usage(sync, client),
    }


@register_model_view(ForwardSource, "list", path="", detail=False)
class ForwardSourceListView(generic.ObjectListView):
    queryset = ForwardSource.objects.all()
    filterset = ForwardSourceFilterSet
    table = ForwardSourceTable
    actions = (AddObject, BulkExport, BulkEdit, BulkRename, BulkDelete)


@register_model_view(ForwardSource, "add", detail=False)
@register_model_view(ForwardSource, "edit")
class ForwardSourceEditView(generic.ObjectEditView):
    queryset = ForwardSource.objects.all()
    form = ForwardSourceForm


@register_model_view(ForwardSource)
class ForwardSourceView(generic.ObjectView):
    queryset = ForwardSource.objects.all()
    template_name = "forward_netbox/forwardsource.html"

    def get_extra_context(self, request, instance):
        return {
            "masked_parameters": instance.get_masked_parameters(),
            "tag_scope_preview": instance.get_tag_scope_preview(),
        }


@register_model_view(ForwardSource, "delete")
class ForwardSourceDeleteView(generic.ObjectDeleteView):
    queryset = ForwardSource.objects.all()


@register_model_view(ForwardSource, "bulk_edit", path="edit", detail=False)
class ForwardSourceBulkEditView(generic.BulkEditView):
    queryset = ForwardSource.objects.all()
    table = ForwardSourceTable
    form = ForwardSourceBulkEditForm


@register_model_view(ForwardSource, "bulk_rename", path="rename", detail=False)
class ForwardSourceBulkRenameView(generic.BulkRenameView):
    queryset = ForwardSource.objects.all()


@register_model_view(ForwardSource, "bulk_delete", path="delete", detail=False)
class ForwardSourceBulkDeleteView(generic.BulkDeleteView):
    queryset = ForwardSource.objects.all()
    table = ForwardSourceTable


@register_model_view(ForwardNQEMap, "list", path="", detail=False)
class ForwardNQEMapListView(generic.ObjectListView):
    queryset = ForwardNQEMap.objects.select_related("netbox_model")
    filterset = ForwardNQEMapFilterSet
    table = ForwardNQEMapTable
    actions = (AddObject, BulkExport, BulkEdit, BulkRename, BulkDelete)


@register_model_view(ForwardNQEMap, "add", detail=False)
@register_model_view(ForwardNQEMap, "edit")
class ForwardNQEMapEditView(generic.ObjectEditView):
    queryset = ForwardNQEMap.objects.select_related("netbox_model")
    form = ForwardNQEMapForm
    default_return_url = "plugins:forward_netbox:forwardnqemap_list"


@register_model_view(ForwardNQEMap)
class ForwardNQEMapView(generic.ObjectView):
    queryset = ForwardNQEMap.objects.select_related("netbox_model")
    template_name = "forward_netbox/forwardnqemap.html"


@register_model_view(ForwardNQEMap, "delete")
class ForwardNQEMapDeleteView(generic.ObjectDeleteView):
    queryset = ForwardNQEMap.objects.all()
    default_return_url = "plugins:forward_netbox:forwardnqemap_list"


@register_model_view(ForwardNQEMap, "bulk_edit", path="edit", detail=False)
class ForwardNQEMapBulkEditView(generic.BulkEditView):
    queryset = ForwardNQEMap.objects.select_related("netbox_model")
    table = ForwardNQEMapTable
    form = ForwardNQEMapBulkEditForm

    def _update_objects(self, form, request):
        query_bulk_operation = form.get_query_bulk_operation()
        if not query_bulk_operation:
            return super()._update_objects(form, request)

        selected_queryset = self.queryset.filter(pk__in=form.cleaned_data["pk"])
        if form.has_query_restore_request():
            try:
                results = restore_builtin_raw_query_bindings(queryset=selected_queryset)
            except Exception as exc:
                logger.warning(
                    "Bundled NQE map restore failed (%s)", type(exc).__name__
                )
                raise ValidationError(
                    "Unable to restore bundled Forward NQE map queries. Review "
                    "server logs and the selected map configuration."
                ) from exc

            updated_ids = [result.map_id for result in results if result.matched]
            if not updated_ids:
                raise ValidationError(
                    "No selected Forward NQE maps could be restored to bundled raw query text."
                )
            skipped_count = len([result for result in results if result.skipped_reason])
            if skipped_count:
                messages.warning(
                    request,
                    _(
                        "Skipped %(count)s selected Forward NQE maps because their "
                        "bundled raw query could not be identified unambiguously."
                    )
                    % {"count": skipped_count},
                )
            return list(self.queryset.filter(pk__in=updated_ids))

        if form.has_query_publish_request():
            bind_source = form.cleaned_data.get("bind_query_source")
            bind_folder = form.cleaned_data.get("bind_query_folder")
            if not bind_source or not bind_folder:
                raise ValidationError(
                    "Select a Forward source and Org Repository folder for NQE publishing."
                )
            try:
                results = publish_builtin_nqe_map_queries(
                    client=bind_source.get_client(),
                    directory=bind_folder,
                    queryset=selected_queryset,
                    overwrite=form.cleaned_data.get("publish_overwrite", False),
                    commit_message=form.cleaned_data.get("publish_commit_message", ""),
                    pin_commit=form.cleaned_data.get("bind_pin_commit", False),
                )
            except NQELibraryWritePermissionError as exc:
                logger.info("Bundled NQE map publication permission pre-check failed")
                raise ValidationError(NQE_LIBRARY_WRITE_PERMISSION_MESSAGE) from exc
            except Exception as exc:
                logger.warning(
                    "Bundled NQE map publication failed (%s)", type(exc).__name__
                )
                raise ValidationError(
                    "Unable to publish bundled Forward NQE maps. Review server "
                    "logs and Forward repository permissions."
                ) from exc

            updated_ids = [result.map_id for result in results if result.matched]
            if not updated_ids:
                raise ValidationError(
                    "No selected Forward NQE maps could be published and bound."
                )
            skipped_count = len([result for result in results if result.skipped_reason])
            if skipped_count:
                messages.warning(
                    request,
                    _(
                        "Skipped %(count)s selected Forward NQE maps because their "
                        "bundled source could not be identified unambiguously."
                    )
                    % {"count": skipped_count},
                )
            messages.info(
                request,
                _(
                    "Published bundled Forward NQE source to the Org Repository "
                    "and bound %(count)s selected maps."
                )
                % {"count": len(updated_ids)},
            )
            return list(self.queryset.filter(pk__in=updated_ids))

        bind_source = form.cleaned_data.get("bind_query_source")
        bind_folder = form.cleaned_data.get("bind_query_folder")
        selected_query_paths = form.selected_query_paths_by_map_id()
        if not bind_source or not bind_folder:
            raise ValidationError(
                "Select a Forward source and repository folder for query path binding."
            )

        try:
            bindings = build_nqe_map_bindings(
                client=bind_source.get_client(),
                repository=form.cleaned_data.get("bind_query_repository") or "org",
                directory=bind_folder,
                pin_commit=form.cleaned_data.get("bind_pin_commit", False),
            )
            results = apply_explicit_nqe_map_bindings(
                bindings,
                query_path_by_map_id=selected_query_paths,
                queryset=selected_queryset,
            )
        except Exception as exc:
            logger.warning("Forward NQE map binding failed (%s)", type(exc).__name__)
            raise ValidationError(
                "Unable to bind Forward NQE maps. Review server logs and the "
                "selected repository paths."
            ) from exc

        updated_ids = [result.map_id for result in results if result.matched]
        if not updated_ids:
            raise ValidationError(
                "No selected Forward NQE maps matched queries in the selected folder."
            )
        skipped_count = len([result for result in results if result.skipped_reason])
        if skipped_count:
            messages.warning(
                request,
                _(
                    "Skipped %(count)s selected Forward NQE maps because their "
                    "selected repository query path was blank, missing, or targeted "
                    "a different NetBox model."
                )
                % {"count": skipped_count},
            )
        return list(self.queryset.filter(pk__in=updated_ids))


@register_model_view(ForwardNQEMap, "bulk_rename", path="rename", detail=False)
class ForwardNQEMapBulkRenameView(generic.BulkRenameView):
    queryset = ForwardNQEMap.objects.all()


@register_model_view(ForwardNQEMap, "bulk_delete", path="delete", detail=False)
class ForwardNQEMapBulkDeleteView(generic.BulkDeleteView):
    queryset = ForwardNQEMap.objects.all()
    table = ForwardNQEMapTable


@register_model_view(ForwardSync, "list", path="", detail=False)
class ForwardSyncListView(generic.ObjectListView):
    queryset = ForwardSync.objects.all()
    filterset = ForwardSyncFilterSet
    table = ForwardSyncTable
    actions = (AddObject, BulkExport, BulkEdit, BulkRename, BulkDelete)


@register_model_view(ForwardSync, "add", detail=False)
@register_model_view(ForwardSync, "edit")
class ForwardSyncEditView(generic.ObjectEditView):
    queryset = ForwardSync.objects.all()
    form = ForwardSyncForm

    def alter_object(self, obj, request, url_args, url_kwargs):
        obj.user = request.user
        return obj


@register_model_view(ForwardSync)
class ForwardSyncView(generic.ObjectView):
    queryset = ForwardSync.objects.all()
    template_name = "forward_netbox/forwardsync.html"

    def get_extra_context(self, request, instance):
        health = sync_health_summary(instance)
        data = {
            "last_ingestion": instance.last_ingestion,
            "latest_validation_run": instance.latest_validation_run,
            "enabled_models": instance.enabled_models(),
            "query_drift_summary": health.get("query_drift_summary", {}),
            "query_drift_results": health.get("query_modes", {}).get("local_drift", []),
            "dependency_preview_url": reverse(
                "plugins:forward_netbox:forwardsync_dependency_preview",
                kwargs={"pk": instance.pk},
            ),
            "health_url": reverse(
                "plugins:forward_netbox:forwardsync_health",
                kwargs={"pk": instance.pk},
            ),
            "support_bundle_url": reverse(
                "plugins:forward_netbox:forwardsync_support_bundle",
                kwargs={"pk": instance.pk},
            ),
            "support_bundle_zip_url": reverse(
                "plugins:forward_netbox:forwardsync_support_bundle_zip",
                kwargs={"pk": instance.pk},
            ),
            "scope_reconciliation_url": reverse(
                "plugins:forward_netbox:forwardsync_scope_reconciliation",
                kwargs={"pk": instance.pk},
            ),
            "module_readiness_url": reverse(
                "plugins:forward_netbox:forwardsync_module_readiness",
                kwargs={"pk": instance.pk},
            ),
            "refresh_device_analysis_url": reverse(
                "plugins:forward_netbox:forwardsync_refresh_device_analysis",
                kwargs={"pk": instance.pk},
            ),
            "drift_report_url": reverse(
                "plugins:forward_netbox:forwardsync_drift_report",
                kwargs={"pk": instance.pk},
            ),
        }
        if instance.last_ingestion:
            data.update(instance.last_ingestion.get_statistics())
        data["standing_schedules"] = self._standing_schedules(instance)
        return data

    @staticmethod
    def _standing_schedules(instance):
        from core.choices import JobStatusChoices

        from .utilities.sync_facade import (
            STANDING_SCHEDULE_JOB_NAMES,
            STANDING_SCHEDULE_PARAM_KEYS,
        )

        parameters = instance.parameters or {}
        rows = []
        labels = {
            "validation": "Validation",
            "dependency_preview": "Dependency preview",
        }
        for kind, key in STANDING_SCHEDULE_PARAM_KEYS.items():
            interval = int(parameters.get(key) or 0)
            job = (
                instance.jobs.filter(
                    name=STANDING_SCHEDULE_JOB_NAMES[kind],
                    status__in=JobStatusChoices.ENQUEUED_STATE_CHOICES,
                )
                .order_by("pk")
                .first()
            )
            if interval or job:
                rows.append(
                    {
                        "label": labels[kind],
                        "interval": interval,
                        "job": job,
                        "next_run": getattr(job, "scheduled", None),
                    }
                )
        return rows


@register_model_view(
    ForwardSync, "refresh_device_analysis", path="refresh-device-analysis"
)
class ForwardSyncRefreshDeviceAnalysisView(BaseObjectView):
    queryset = ForwardSync.objects.all()

    def get_required_permission(self):
        return "forward_netbox.view_forwardsync"

    def get(self, request, pk):
        sync = get_object_or_404(self.queryset, pk=pk)
        return redirect(sync.get_absolute_url())

    def post(self, request, pk):
        # Live NQE over all devices — runs as a background job.
        from .jobs import DeviceAnalysisRefreshJob

        sync = get_object_or_404(self.queryset, pk=pk)
        job = DeviceAnalysisRefreshJob.enqueue(
            instance=sync,
            user=request.user,
            name=f"{sync.name} - refresh device analysis",
        )
        messages.success(
            request,
            _(
                "Queued job #%(pk)d to refresh device analysis. The results appear "
                "on each device's Forward Analysis panel."
            )
            % {"pk": job.pk},
        )
        return redirect(sync.get_absolute_url())


@register_model_view(ForwardSync, "drift_report", path="drift-report")
class ForwardSyncDriftReportView(BaseObjectView):
    queryset = ForwardSync.objects.all()
    template_name = "forward_netbox/forwardsync_drift_report.html"
    # A cached preview older than this is flagged stale even when no sync ran
    # after it — an old "everything to create" preview misleads operators.
    STALE_PREVIEW_AGE = timezone.timedelta(hours=24)

    def get_required_permission(self):
        return "forward_netbox.view_forwardsync"

    def get(self, request, pk):
        # Derives a per-model drift table from the latest completed dependency
        # preview job's cached payload — no extra dry-run.
        from core.choices import JobStatusChoices
        from core.models import Job
        from django.contrib.contenttypes.models import ContentType

        from .utilities.drift_report import (
            build_latest_sync_evidence,
            compute_drift_report,
        )

        sync = get_object_or_404(self.queryset, pk=pk)
        job = (
            Job.objects.filter(
                object_type=ContentType.objects.get_for_model(ForwardSync),
                object_id=sync.pk,
                name__icontains="dependency preview",
                status=JobStatusChoices.STATUS_COMPLETED,
            )
            .order_by("-created")
            .first()
        )
        if job is None or not job.data:
            messages.info(
                request,
                _(
                    "No drift data yet. Run Preview Dependencies first, then open "
                    "the drift report."
                ),
            )
            return redirect(sync.get_absolute_url())
        report = compute_drift_report(job.data)
        # The drift is computed from the cached preview payload, not live, so it
        # goes stale two ways: (1) a sync ran AFTER the preview, or (2) the
        # preview is simply old. Either leaves misleading "everything to create"
        # numbers, so flag it and point the operator at Preview Dependencies.
        last_ingestion = sync.last_ingestion
        newer_sync_ran = bool(
            last_ingestion is not None
            and job.created is not None
            and last_ingestion.created > job.created
        )
        preview_age = timezone.now() - job.created if job.created else None
        preview_is_old = bool(preview_age and preview_age > self.STALE_PREVIEW_AGE)
        drift_stale = newer_sync_ran or preview_is_old
        latest_sync_evidence = build_latest_sync_evidence(last_ingestion, job.data)
        return render(
            request,
            self.template_name,
            {
                "object": sync,
                "report": report,
                "drift_stale": drift_stale,
                "drift_stale_newer_sync": newer_sync_ran,
                "drift_stale_old_preview": preview_is_old,
                "last_sync_at": last_ingestion.created if last_ingestion else None,
                "preview_at": job.created,
                "latest_sync_evidence": latest_sync_evidence,
            },
        )


@register_model_view(ForwardSync, "dependency_preview", path="dependency-preview")
class ForwardSyncDependencyPreviewView(BaseObjectView):
    queryset = ForwardSync.objects.all()
    template_name = "forward_netbox/forwardsync_dependency_preview.html"

    def get_required_permission(self):
        return "forward_netbox.run_forwardsync"

    def get(self, request, pk):
        # Render the most recent completed preview job's cached payload. The heavy
        # live dry-run runs in the background job (see post()), never in this GET,
        # so this page never blocks on Forward and cannot 504 on large fabrics.
        from core.choices import JobStatusChoices
        from core.models import Job
        from django.contrib.contenttypes.models import ContentType

        sync = get_object_or_404(self.queryset, pk=pk)
        job = (
            Job.objects.filter(
                object_type=ContentType.objects.get_for_model(ForwardSync),
                object_id=sync.pk,
                name__icontains="dependency preview",
                status=JobStatusChoices.STATUS_COMPLETED,
            )
            .order_by("-created")
            .first()
        )
        if job is None or not job.data:
            messages.info(
                request,
                _(
                    "No dependency preview available yet. Use Preview Dependencies "
                    "to queue one, then watch the Jobs tab."
                ),
            )
            return redirect(sync.get_absolute_url())
        payload = job.data
        if request.GET.get("format") == "json":
            filename = f"forward-sync-{sync.pk}-dependency-preview.json"
            return _download_json_response(payload, filename)
        return render(
            request,
            self.template_name,
            {
                "object": sync,
                "payload": payload,
                "plan_preview": payload.get("plan_preview"),
                "plan_items": payload.get("plan_items"),
                "preview_job": job,
            },
        )

    def post(self, request, pk):
        # Building the dependency plan is a heavy live Forward dry-run that exceeds
        # an HTTP gateway timeout on large fabrics, so it runs as a background job.
        from .utilities.sync_facade import JobAlreadyActive, enqueue_button_job

        sync = get_object_or_404(self.queryset, pk=pk)
        try:
            job = enqueue_button_job(sync, "dependency_preview", request.user)
        except JobAlreadyActive:
            messages.warning(
                request, _("An equivalent dependency preview is already running.")
            )
            return redirect(sync.get_absolute_url())
        messages.success(
            request,
            _(
                "Queued job #%(pk)d to build the dependency preview. Watch the Jobs "
                "tab; the result then appears under View Last Preview."
            )
            % {"pk": job.pk},
        )
        return redirect(sync.get_absolute_url())


@register_model_view(ForwardSync, "run")
class ForwardStartSyncView(BaseObjectView):
    queryset = ForwardSync.objects.all()

    def get_required_permission(self):
        return "forward_netbox.run_forwardsync"

    def get(self, request, pk):
        sync = get_object_or_404(self.queryset, pk=pk)
        return redirect(sync.get_absolute_url())

    def post(self, request, pk):
        sync = get_object_or_404(self.queryset, pk=pk)
        try:
            job = sync.enqueue_sync_job(
                user=request.user,
                adhoc=True,
                force_unchanged=True,
            )
        except SyncError as exc:
            logger.warning("Forward sync enqueue was rejected (%s)", type(exc).__name__)
            messages.error(
                request,
                _(
                    "Forward sync could not be queued. Review the current sync and "
                    "branch state before retrying."
                ),
            )
            return redirect(sync.get_absolute_url())
        messages.success(request, f"Queued job #{job.pk} to run {sync}.")
        return redirect(sync.get_absolute_url())


@register_model_view(ForwardSync, "validate")
class ForwardStartValidationView(BaseObjectView):
    queryset = ForwardSync.objects.all()

    def get_required_permission(self):
        return "forward_netbox.run_forwardsync"

    def get(self, request, pk):
        sync = get_object_or_404(self.queryset, pk=pk)
        return redirect(sync.get_absolute_url())

    def post(self, request, pk):
        from .utilities.sync_facade import JobAlreadyActive

        sync = get_object_or_404(self.queryset, pk=pk)
        try:
            job = sync.enqueue_validation_job(user=request.user, adhoc=True)
        except JobAlreadyActive:
            messages.warning(
                request, _("An equivalent validation job is already running.")
            )
            return redirect(sync.get_absolute_url())
        messages.success(request, f"Queued job #{job.pk} to validate {sync}.")
        return redirect(sync.get_absolute_url())


def _latest_scope_reconciliation_job(sync):
    """The most recent completed scope reconciliation job for this sync."""
    from core.choices import JobStatusChoices
    from core.models import Job
    from django.contrib.contenttypes.models import ContentType

    return (
        Job.objects.filter(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=sync.pk,
            name__icontains="scope reconciliation",
            status=JobStatusChoices.STATUS_COMPLETED,
        )
        .order_by("-created")
        .first()
    )


def _scope_reconciliation_payload(job):
    """Return (payload, generated_at, error) for a stored reconciliation job."""
    if job is None or not isinstance(job.data, dict) or not job.data:
        return {}, None, ""
    if job.data.get("error"):
        return {}, job.completed, str(job.data.get("error"))
    return job.data, job.completed, ""


@register_model_view(
    ForwardSync,
    "refresh_scope_reconciliation",
    path="scope-reconciliation/refresh",
)
class ForwardSyncRefreshScopeReconciliationView(BaseObjectView):
    """Enqueue the reconciliation report instead of computing it in a request."""

    queryset = ForwardSync.objects.all()

    def get_required_permission(self):
        return "forward_netbox.view_forwardsync"

    def get(self, request, pk):
        sync = get_object_or_404(self.queryset, pk=pk)
        return redirect(
            reverse(
                "plugins:forward_netbox:forwardsync_scope_reconciliation",
                kwargs={"pk": sync.pk},
            )
        )

    def post(self, request, pk):
        from .jobs import ScopeReconciliationJob

        sync = get_object_or_404(self.queryset, pk=pk)
        # Go through the runner's own `enqueue`, which passes `cls.handle` to
        # RQ. Handing `enqueue_forward_job` the class instead reached
        # `queue.enqueue()` as a class object, which RQ rejects outright -
        # "Expected a callable or a string" - after the Job row was already
        # written, so the work looked queued and the operator got a 500.
        ScopeReconciliationJob.enqueue(
            instance=sync,
            user=request.user,
            name=f"{sync.name} - scope reconciliation",
        )
        messages.success(
            request,
            _("Scope reconciliation queued. Reload this page when it completes."),
        )
        return redirect(
            reverse(
                "plugins:forward_netbox:forwardsync_scope_reconciliation",
                kwargs={"pk": sync.pk},
            )
        )


@register_model_view(ForwardSync, "scope_reconciliation", path="scope-reconciliation")
class ForwardSyncScopeReconciliationView(BaseObjectView):
    queryset = ForwardSync.objects.all()
    template_name = "forward_netbox/forwardsync_scope_reconciliation.html"

    def get_required_permission(self):
        return "forward_netbox.view_forwardsync"

    def get(self, request, pk):
        # `compute_scope_reconciliation` is deliberately NOT imported here: this
        # view must never compute the report. `compute_upgrade_reconciliation`
        # stays, since it only reads the local database.
        from .utilities.upgrade_reconciliation import compute_upgrade_reconciliation

        sync = get_object_or_404(self.queryset, pk=pk)
        # Never compute inline. `compute_scope_reconciliation` issues two live
        # NQE `fetch_all` queries — every device and every endpoint — so on any
        # real fabric this exceeded the gateway timeout and returned 504 with
        # the queries still running, while hitting Forward on every page load.
        report_job = _latest_scope_reconciliation_job(sync)
        payload, generated_at, report_error = _scope_reconciliation_payload(report_job)
        from .utilities.scope_reconciliation import BACKFILLED_TAG_SLUG
        from .utilities.scope_reconciliation import UNCOVERED_TAG_SLUG

        device_list_url = reverse("dcim:device_list")
        backfilled_tag_url = f"{device_list_url}?tag={BACKFILLED_TAG_SLUG}"
        uncovered_tag_url = f"{device_list_url}?tag={UNCOVERED_TAG_SLUG}"
        return render(
            request,
            self.template_name,
            {
                "object": sync,
                "payload": payload,
                "report_generated_at": generated_at,
                "report_error": report_error,
                "report_pending": report_job is None,
                "refresh_report_url": reverse(
                    "plugins:forward_netbox:forwardsync_refresh_scope_reconciliation",
                    kwargs={"pk": sync.pk},
                ),
                "upgrade_reconciliation": compute_upgrade_reconciliation(
                    include_samples=True
                ),
                "backfilled_tag_url": backfilled_tag_url,
                "uncovered_tag_url": uncovered_tag_url,
                "tag_backfilled_url": reverse(
                    "plugins:forward_netbox:forwardsync_tag_backfilled",
                    kwargs={"pk": sync.pk},
                ),
                "tag_delete_eligible_ipam_url": reverse(
                    "plugins:forward_netbox:forwardsync_tag_delete_eligible_ipam",
                    kwargs={"pk": sync.pk},
                ),
                # Gates the hardware-notice cleanup button. The count it would
                # clear needs a live Forward query, which has no business
                # running on a page render, so the button is offered whenever
                # the plugin is installed and the job reports what it found.
                "dlm_available": apps.is_installed("netbox_dlm"),
            },
        )


@register_model_view(ForwardSync, "tag_backfilled", path="tag-backfilled")
class ForwardSyncTagBackfilledView(BaseObjectView):
    queryset = ForwardSync.objects.all()

    def get_required_permission(self):
        return "dcim.change_device"

    def get(self, request, pk):
        sync = get_object_or_404(self.queryset, pk=pk)
        return redirect(
            reverse(
                "plugins:forward_netbox:forwardsync_scope_reconciliation",
                kwargs={"pk": sync.pk},
            )
        )

    def post(self, request, pk):
        # Reconciles plugin-maintained scope tags in the standard device list.
        # Runs as a background job (live Forward query + tag writes).
        from .jobs import DeviceScopeTagReconciliationJob

        sync = get_object_or_404(self.queryset, pk=pk)
        job = DeviceScopeTagReconciliationJob.enqueue(
            instance=sync,
            user=request.user,
            name=f"{sync.name} - reconcile device scope tags",
        )
        messages.success(
            request,
            _(
                "Queued job #%(pk)d to reconcile device scope tags. It maintains "
                "backfilled and out-of-scope labels and clears stale managed "
                "include tags from out-of-scope devices."
            )
            % {"pk": job.pk},
        )
        return redirect(
            reverse(
                "plugins:forward_netbox:forwardsync_scope_reconciliation",
                kwargs={"pk": sync.pk},
            )
        )


@register_model_view(
    ForwardSync, "tag_delete_eligible_ipam", path="tag-delete-eligible-ipam"
)
class ForwardSyncTagDeleteEligibleIpamView(BaseObjectView):
    queryset = ForwardSync.objects.all()

    def get_required_permission(self):
        return "ipam.change_prefix"

    def get(self, request, pk):
        sync = get_object_or_404(self.queryset, pk=pk)
        return redirect(
            reverse(
                "plugins:forward_netbox:forwardsync_scope_reconciliation",
                kwargs={"pk": sync.pk},
            )
        )

    def post(self, request, pk):
        # Tags network-global IPAM (prefixes/VLANs/VRFs) that the latest Forward
        # fetch no longer reports so an operator can review and delete them by
        # hand. Runs as a background job: it issues live Forward fetches per IPAM
        # model and may tag/untag many objects. Tag-only — never deletes.
        from .utilities.sync_facade import JobAlreadyActive, enqueue_button_job

        sync = get_object_or_404(self.queryset, pk=pk)
        try:
            job = enqueue_button_job(sync, "tag_delete_eligible_ipam", request.user)
        except JobAlreadyActive:
            messages.warning(
                request,
                _("An equivalent delete-eligible IPAM job is already running."),
            )
            return redirect(sync.get_absolute_url())
        messages.success(
            request,
            _(
                "Queued job #%(pk)d to tag delete-eligible IPAM. When it finishes, "
                "filter prefixes/VLANs/VRFs by the forward-delete-eligible tag to "
                "review and delete them."
            )
            % {"pk": job.pk},
        )
        return redirect(
            reverse(
                "plugins:forward_netbox:forwardsync_scope_reconciliation",
                kwargs={"pk": sync.pk},
            )
        )


@register_model_view(
    ForwardSync, "prune_stale_hardware_notices", path="prune-stale-hardware-notices"
)
class ForwardSyncPruneStaleHardwareNoticesView(BaseObjectView):
    queryset = ForwardSync.objects.all()

    def get_required_permission(self):
        return "netbox_dlm.delete_hardwarenotice"

    def get(self, request, pk):
        sync = get_object_or_404(self.queryset, pk=pk)
        return redirect(
            reverse(
                "plugins:forward_netbox:forwardsync_scope_reconciliation",
                kwargs={"pk": sync.pk},
            )
        )

    def post(self, request, pk):
        # Issues a live Forward fetch to establish what the hardware-notice
        # query still emits, so it runs as a background job rather than in the
        # request. Deletes notices only; device types are never touched.
        from .utilities.sync_facade import enqueue_button_job, JobAlreadyActive

        sync = get_object_or_404(self.queryset, pk=pk)
        try:
            job = enqueue_button_job(sync, "prune_stale_hardware_notices", request.user)
        except JobAlreadyActive:
            messages.warning(
                request,
                _("An equivalent hardware-notice cleanup job is already running."),
            )
            return redirect(sync.get_absolute_url())
        messages.success(
            request,
            _(
                "Queued job #%(pk)d to remove hardware notices Forward no longer "
                "reports. The job records which device types were cleared."
            )
            % {"pk": job.pk},
        )
        return redirect(
            reverse(
                "plugins:forward_netbox:forwardsync_scope_reconciliation",
                kwargs={"pk": sync.pk},
            )
        )


@register_model_view(ForwardSync, "prune_orphans", path="prune-orphans")
class ForwardSyncPruneOrphansView(BaseObjectView):
    queryset = ForwardSync.objects.all()

    def get_required_permission(self):
        return "dcim.delete_device"

    def get(self, request, pk):
        sync = get_object_or_404(self.queryset, pk=pk)
        return redirect(
            reverse(
                "plugins:forward_netbox:forwardsync_scope_reconciliation",
                kwargs={"pk": sync.pk},
            )
        )

    def post(self, request, pk):
        # Pruning cascades device deletes (interfaces, IPs, change-log signals)
        # and can far exceed an HTTP gateway timeout on large fabrics, so it runs
        # as a background job rather than synchronously in the request.
        from .utilities.sync_facade import JobAlreadyActive, enqueue_button_job

        sync = get_object_or_404(self.queryset, pk=pk)
        # Only ever true when an operator ticked the box beside a named list of
        # held devices. Absent from the standing-schedule path by construction.
        include_quarantined = bool(request.POST.get("include_quarantined"))
        try:
            job = enqueue_button_job(
                sync,
                "prune_orphans",
                request.user,
                job_kwargs={"include_quarantined": include_quarantined},
            )
        except JobAlreadyActive:
            messages.warning(
                request, _("An equivalent orphan-prune job is already running.")
            )
            return redirect(sync.get_absolute_url())
        messages.success(
            request,
            _(
                "Queued job #%(pk)d to prune out-of-scope devices. Watch the Jobs "
                "tab for the result."
            )
            % {"pk": job.pk},
        )
        return redirect(sync.get_absolute_url())


@register_model_view(ForwardSync, "module_readiness", path="module-readiness")
class ForwardSyncModuleReadinessView(BaseObjectView):
    queryset = ForwardSync.objects.all()
    template_name = "forward_netbox/forwardsync_module_readiness.html"

    def get_required_permission(self):
        return "forward_netbox.view_forwardsync"

    def get(self, request, pk):
        from .utilities.module_readiness import compute_module_readiness_for_sync

        sync = get_object_or_404(self.queryset, pk=pk)
        try:
            report = compute_module_readiness_for_sync(sync)
        except Exception as exc:
            logger.warning("Module readiness report failed (%s)", type(exc).__name__)
            messages.error(
                request,
                _("Module readiness check failed. Review server logs before retrying."),
            )
            return redirect(sync.get_absolute_url())
        return render(
            request,
            self.template_name,
            {
                "object": sync,
                "payload": report.as_dict(),
                "module_bay_plan_rows": report.module_bay_plan_rows,
                "missing_device_names": report.missing_device_names,
            },
        )


@register_model_view(ForwardSync, "support_bundle", path="support-bundle")
class ForwardSyncSupportBundleView(BaseObjectView):
    queryset = ForwardSync.objects.all()

    def get_required_permission(self):
        return "forward_netbox.view_forwardsync"

    def get(self, request, pk):
        sync = get_object_or_404(self.queryset, pk=pk)
        filename = f"forward-sync-{sync.pk}-support-bundle.json"
        return _download_json_response(_sync_support_bundle_payload(sync), filename)


@register_model_view(ForwardSync, "support_bundle_zip", path="support-bundle-zip")
class ForwardSyncSupportBundleZipView(BaseObjectView):
    queryset = ForwardSync.objects.all()

    def get_required_permission(self):
        return "forward_netbox.view_forwardsync"

    def get(self, request, pk):
        sync = get_object_or_404(self.queryset, pk=pk)
        if "password" in request.GET:
            return HttpResponseBadRequest(
                "Archive passwords must be submitted in a POST request."
            )
        return self._download(sync, password="")

    def post(self, request, pk):
        sync = get_object_or_404(self.queryset, pk=pk)
        return self._download(sync, password=request.POST.get("password") or "")

    def _download(self, sync, *, password):
        filename = f"forward-sync-{sync.pk}-support-bundle.zip"
        try:
            return support_bundle_zip_response(
                _sync_support_bundle_payload(sync),
                filename=filename,
                json_filename=f"forward-sync-{sync.pk}-support-bundle.json",
                password=password,
            )
        except RuntimeError as exc:
            logger.warning(
                "Support-bundle archive creation failed (%s)", type(exc).__name__
            )
            return HttpResponseBadRequest(
                "Support-bundle archive creation failed. Review server logs."
            )


@register_model_view(ForwardSync, "health")
class ForwardSyncHealthView(generic.ObjectView):
    queryset = ForwardSync.objects.all()
    template_name = "forward_netbox/forwardsync_health.html"
    tab = ViewTab(
        label=_("Health"),
        permission="forward_netbox.view_forwardsync",
    )

    def get_extra_context(self, request, instance):
        return {"health": sync_health_summary(instance)}


@register_model_view(ForwardSync, "query_drift", path="query-drift")
class ForwardSyncQueryDriftView(BaseObjectView):
    queryset = ForwardSync.objects.all()

    def get_required_permission(self):
        return "forward_netbox.view_forwardsync"

    def get(self, request, pk):
        sync = get_object_or_404(self.queryset, pk=pk)
        client = sync.source.get_client()
        health = sync_health_summary(sync)
        maps = [
            query_map
            for query_map in sync.get_maps()
            if sync.is_model_enabled(query_map.model_string)
        ]
        payload = {
            "exported_at": timezone.now().isoformat(),
            "sync": {
                "pk": sync.pk,
                "name": sync.name,
                "source": sync.source_id,
            },
            "query_drift_summary": health.get("query_drift_summary", {}),
            "results": live_query_binding_drifts(client=client, query_maps=maps),
        }
        filename = f"forward-sync-{sync.pk}-live-query-drift.json"
        return _download_json_response(json_safe_value(payload), filename)


@register_model_view(ForwardSync, "resolve_query_ids", path="resolve-query-ids")
class ForwardSyncResolveQueryIDsView(BaseObjectView):
    queryset = ForwardSync.objects.all()

    def get_required_permission(self):
        return "forward_netbox.change_forwardnqemap"

    def post(self, request, pk):
        sync = get_object_or_404(self.queryset, pk=pk)
        map_ids = [
            query_map.pk
            for query_map in sync.get_maps()
            if sync.is_model_enabled(query_map.model_string)
        ]
        queryset = ForwardNQEMap.objects.filter(pk__in=map_ids).select_related(
            "netbox_model"
        )
        try:
            results = resolve_nqe_map_query_ids(
                client=sync.source.get_client(),
                queryset=queryset,
            )
        except Exception as exc:  # noqa: BLE001 - emit only a safe error class
            logger.warning(
                "Read-only query ID resolution failed (%s)", type(exc).__name__
            )
            messages.error(
                request,
                _(
                    "Unable to resolve query IDs from read-only Forward "
                    "repository metadata. Review server logs and source access."
                ),
            )
        else:
            resolved_count = len([result for result in results if result.matched])
            unchanged_count = len(results) - resolved_count
            if resolved_count:
                messages.success(
                    request,
                    _(
                        "Resolved and persisted query IDs for %(count)s enabled "
                        "NQE map(s) using read-only Forward repository metadata. "
                        "No Forward queries were published or changed."
                    )
                    % {"count": resolved_count},
                )
            if unchanged_count:
                messages.warning(
                    request,
                    _(
                        "%(count)s enabled NQE map(s) remained unchanged because "
                        "they were already bound, not path-bound, ambiguous, or "
                        "unresolved."
                    )
                    % {"count": unchanged_count},
                )
            if not results:
                messages.info(
                    request,
                    _("No enabled NQE maps were available to resolve."),
                )
        return redirect(
            reverse("plugins:forward_netbox:forwardsync_health", kwargs={"pk": sync.pk})
        )


@register_model_view(
    ForwardSync, "publish_bundled_queries", path="publish-bundled-queries"
)
class ForwardSyncPublishBundledQueriesView(BaseObjectView):
    queryset = ForwardSync.objects.all()
    default_directory = "/forward_netbox_validation/"

    def get_required_permission(self):
        return "forward_netbox.change_forwardnqemap"

    def post(self, request, pk):
        sync = get_object_or_404(self.queryset, pk=pk)
        client = sync.source.get_client()
        directory = str(
            request.POST.get("query_directory") or self.default_directory
        ).strip()
        if not directory.startswith("/"):
            directory = f"/{directory}"
        path_segments = [segment for segment in directory.split("/") if segment]
        if (
            not path_segments
            or any(segment in {".", ".."} for segment in path_segments)
            or "\\" in directory
            or "\x00" in directory
        ):
            messages.error(request, _("Enter a valid Forward query folder."))
            return redirect(
                reverse(
                    "plugins:forward_netbox:forwardsync_health", kwargs={"pk": sync.pk}
                )
            )
        directory = f"/{'/'.join(path_segments)}/"
        maps = [
            query_map.pk
            for query_map in sync.get_maps()
            if sync.is_model_enabled(query_map.model_string)
        ]
        queryset = ForwardNQEMap.objects.filter(pk__in=maps).select_related(
            "netbox_model"
        )
        try:
            results = publish_builtin_nqe_map_queries(
                client=client,
                directory=directory,
                queryset=queryset,
                overwrite=True,
                commit_message="Publish Forward NetBox NQE maps",
                pin_commit=False,
            )
        except NQELibraryWritePermissionError:
            logger.info("Bundled query publication permission pre-check failed")
            messages.error(request, _(NQE_LIBRARY_WRITE_PERMISSION_MESSAGE))
            return redirect(
                reverse(
                    "plugins:forward_netbox:forwardsync_health", kwargs={"pk": sync.pk}
                )
            )
        except Exception as exc:  # noqa: BLE001 - emit only a safe error class
            logger.warning("Bundled query publication failed (%s)", type(exc).__name__)
            messages.error(
                request,
                _(
                    "Unable to publish bundled queries to the Forward org library. "
                    "Review server logs and the Forward repository state."
                ),
            )
            return redirect(
                reverse(
                    "plugins:forward_netbox:forwardsync_health", kwargs={"pk": sync.pk}
                )
            )
        published = [result for result in results if result.matched]
        skipped = [result for result in results if not result.matched]
        if published:
            messages.success(
                request,
                _(
                    "Published %(count)s bundled quer(y/ies) to the Forward org "
                    "library and bound the enabled maps to query IDs. Current "
                    "folder metadata is retained for display only."
                )
                % {"count": len(published)},
            )
        if skipped:
            messages.warning(
                request,
                _(
                    "%(count)s NQE map(s) could not be published; confirm the "
                    "source can write to %(directory)s."
                )
                % {"count": len(skipped), "directory": directory},
            )
        if not published and not skipped:
            messages.info(request, _("No enabled NQE maps were available to publish."))
        return redirect(
            reverse("plugins:forward_netbox:forwardsync_health", kwargs={"pk": sync.pk})
        )


@register_model_view(ForwardSync, "source_health", path="source-health")
class ForwardSyncSourceHealthView(BaseObjectView):
    queryset = ForwardSync.objects.all()

    def get_required_permission(self):
        return "forward_netbox.view_forwardsync"

    def get(self, request, pk):
        sync = get_object_or_404(self.queryset, pk=pk)
        payload = {
            "exported_at": timezone.now().isoformat(),
            "sync": {
                "pk": sync.pk,
                "name": sync.name,
                "source": sync.source_id,
            },
            "source_health": live_source_health_check(sync),
        }
        filename = f"forward-sync-{sync.pk}-live-source-health.json"
        return _download_json_response(json_safe_value(payload), filename)


@register_model_view(ForwardSync, "data_file_health", path="data-file-health")
class ForwardSyncDataFileHealthView(BaseObjectView):
    queryset = ForwardSync.objects.all()

    def get_required_permission(self):
        return "forward_netbox.view_forwardsync"

    def get(self, request, pk):
        sync = get_object_or_404(self.queryset, pk=pk)
        payload = {
            "exported_at": timezone.now().isoformat(),
            "sync": {
                "pk": sync.pk,
                "name": sync.name,
                "source": sync.source_id,
            },
            "data_file_health": live_data_file_health_check(sync),
        }
        filename = f"forward-sync-{sync.pk}-live-data-file-health.json"
        return _download_json_response(json_safe_value(payload), filename)


@register_model_view(ForwardSync, "delete")
class ForwardSyncDeleteView(generic.ObjectDeleteView):
    queryset = ForwardSync.objects.all()


@register_model_view(ForwardSync, "bulk_edit", path="edit", detail=False)
class ForwardSyncBulkEditView(generic.BulkEditView):
    queryset = ForwardSync.objects.all()
    table = ForwardSyncTable
    form = ForwardSyncBulkEditForm


@register_model_view(ForwardSync, "bulk_rename", path="rename", detail=False)
class ForwardSyncBulkRenameView(generic.BulkRenameView):
    queryset = ForwardSync.objects.all()


@register_model_view(ForwardSync, "bulk_delete", path="delete", detail=False)
class ForwardSyncBulkDeleteView(generic.BulkDeleteView):
    queryset = ForwardSync.objects.all()
    table = ForwardSyncTable


@register_model_view(ForwardSync, "ingestions")
class ForwardIngestionTabView(generic.ObjectChildrenView):
    queryset = ForwardSync.objects.all()
    child_model = ForwardIngestion
    table = ForwardIngestionTable
    filterset = ForwardIngestionFilterSet
    tab = ViewTab(
        label=_("Ingestions"),
        badge=lambda obj: ForwardIngestion.objects.filter(sync=obj).count(),
        permission="forward_netbox.view_forwardingestion",
    )

    def get_children(self, request, parent):
        return annotate_statistics(ForwardIngestion.objects.filter(sync=parent))


@register_model_view(ForwardIngestion, "list", path="", detail=False)
class ForwardIngestionListView(generic.ObjectListView):
    queryset = annotate_statistics(ForwardIngestion.objects.all())
    filterset = ForwardIngestionFilterSet
    table = ForwardIngestionTable
    # BulkDelete so a backlog of ingestion records can actually be cleared;
    # there is still no edit view, since an ingestion is a record of what a sync
    # did and is not editable after the fact.
    actions = (BulkExport, BulkDelete)


@register_model_view(ForwardIngestion, name="logs", path="logs")
class ForwardIngestionLogView(BaseObjectView):
    queryset = annotate_statistics(ForwardIngestion.objects.all())
    template_name = "forward_netbox/partials/ingestion_all.html"

    def get_required_permission(self):
        return "forward_netbox.view_forwardingestion"

    def get(self, request, **kwargs):
        ingestion = get_object_or_404(self.queryset, pk=kwargs["pk"])
        active_stage = request.GET.get("stage", "sync")
        data = ingestion.get_statistics(stage=active_stage)
        data["object"] = ingestion
        data["job"] = ingestion.job
        data["merge_job"] = ingestion.merge_job
        data["merge_job_results"] = ingestion.get_job_logs(ingestion.merge_job)
        data["active_stage"] = active_stage
        data["merge_disabled"] = not ingestion.merge_job
        sync_running = ingestion.job and not ingestion.job.completed
        merge_running = ingestion.merge_job and not ingestion.merge_job.completed
        job_running = bool(sync_running or merge_running)
        # Keep change explainability unavailable while the job is running: it
        # is only meaningful once staging/merge completes, and recomputing it on
        # every poll piles DB load onto the web workers during a long settling
        # merge (a large platform reclassification can run for minutes) — that
        # contention is what produces the 504 gateway timeouts the customer sees.
        data["change_explainability"] = (
            {"available": False, "reason": "unavailable_while_running"}
            if job_running
            else change_explainability_summary(ingestion)
        )
        data["export_logs_url"] = reverse(
            "plugins:forward_netbox:forwardingestion_export_logs",
            kwargs={"pk": ingestion.pk},
        )

        if request.htmx:
            anything_ever_ran = ingestion.job or ingestion.merge_job
            data["polling_done"] = bool(anything_ever_ran) and not job_running
        return render(request, self.template_name, data)


@register_model_view(ForwardIngestion, name="export_logs", path="logs/export")
class ForwardIngestionLogExportView(BaseObjectView):
    queryset = annotate_statistics(ForwardIngestion.objects.all())

    def get_required_permission(self):
        return "forward_netbox.view_forwardingestion"

    def get(self, request, pk):
        ingestion = get_object_or_404(self.queryset, pk=pk)
        active_stage = request.GET.get("stage", "sync")
        filename_stage = "merge" if active_stage == "merge" else "sync"
        payload = _ingestion_log_export_payload(
            ingestion,
            active_stage=filename_stage,
        )
        filename = f"forward-ingestion-{ingestion.pk}-{filename_stage}-logs.json"
        return _download_json_response(payload, filename)


@register_model_view(ForwardIngestion, name="progress", path="progress")
class ForwardIngestionProgressView(BaseObjectView):
    queryset = annotate_statistics(ForwardIngestion.objects.all())
    template_name = "forward_netbox/partials/ingestion_progress.html"

    def get_required_permission(self):
        return "forward_netbox.view_forwardingestion"

    def get(self, request, **kwargs):
        ingestion = get_object_or_404(self.queryset, pk=kwargs["pk"])
        active_stage = request.GET.get("stage", "sync")
        if active_stage not in ("sync", "merge"):
            active_stage = "sync"
        data = ingestion.get_statistics(stage=active_stage)
        data["job"] = ingestion.job
        data["merge_job"] = ingestion.merge_job
        data["active_stage"] = active_stage
        data["merge_disabled"] = not ingestion.merge_job
        return render(request, self.template_name, data)


@register_model_view(ForwardIngestion)
class ForwardIngestionView(generic.ObjectView):
    queryset = annotate_statistics(ForwardIngestion.objects)
    template_name = "forward_netbox/forwardingestion.html"

    def get_extra_context(self, request, instance):
        active_stage = request.GET.get("stage", "sync")
        data = instance.get_statistics(stage=active_stage)
        sync_running = instance.job and not instance.job.completed
        merge_running = instance.merge_job and not instance.merge_job.completed
        data["job_running"] = bool(sync_running or merge_running)
        data["merge_job"] = instance.merge_job
        data["merge_job_results"] = instance.get_job_logs(instance.merge_job)
        data["active_stage"] = active_stage
        data["merge_disabled"] = not instance.merge_job
        # Keep change explainability unavailable while the job is running (see
        # ForwardIngestionLogView): avoids recomputing it under merge contention.
        data["change_explainability"] = (
            {"available": False, "reason": "unavailable_while_running"}
            if data["job_running"]
            else change_explainability_summary(instance)
        )
        data["export_logs_url"] = reverse(
            "plugins:forward_netbox:forwardingestion_export_logs",
            kwargs={"pk": instance.pk},
        )
        return data


@register_model_view(ForwardIngestion, "merge")
class ForwardIngestionMergeView(BaseObjectView):
    queryset = annotate_statistics(ForwardIngestion.objects.all())
    template_name = "forward_netbox/inc/merge_form.html"
    form = ForwardIngestionMergeForm

    def get_required_permission(self):
        return "forward_netbox.merge_forwardingestion"

    def get(self, request, pk):
        obj = get_object_or_404(self.queryset, pk=pk)
        if request.htmx:
            viewname = get_viewname(self.queryset.model, action="merge")
            form_url = reverse(viewname, kwargs={"pk": obj.pk})
            form = self.form(initial=request.GET)
            restrict_form_fields(form, request.user)
            return render(
                request,
                self.template_name,
                {
                    "object": obj,
                    "object_type": self.queryset.model._meta.verbose_name,
                    "form": form,
                    "form_url": form_url,
                },
            )
        return redirect(obj.get_absolute_url())

    def post(self, request, pk):
        ingestion = get_object_or_404(self.queryset, pk=pk)
        form = self.form(request.POST)
        restrict_form_fields(form, request.user)
        if form.is_valid():
            job = ingestion.enqueue_merge_job(
                user=request.user,
                remove_branch=form.cleaned_data["remove_branch"],
            )
            messages.success(request, f"Queued job #{job.pk} to merge {ingestion}.")
            return redirect(ingestion.get_absolute_url())

        for field, errors in form.errors.items():
            for error in errors:
                messages.error(request, f"{field}: {error}")
        for error in form.non_field_errors():
            messages.error(request, error)
        return redirect(ingestion.get_absolute_url())


@register_model_view(ForwardIngestion, "accept_failures", path="accept-failures")
class ForwardIngestionAcceptFailuresView(BaseObjectView):
    """Complete a merge that recorded row failures, from the UI.

    Any failed row returns the branch to READY without attesting the merge, so
    the baseline never promotes and every retry hits the same rows. When the
    failure is deterministic that is a permanent dead end — a customer sat with
    5 failures out of 24,748 blocking drift measurement and diff syncs.

    Deliberately a separate action from Merge, behind its own permission, so it
    can never be reached by clicking the ordinary button twice.
    """

    queryset = ForwardIngestion.objects.all()

    def get_required_permission(self):
        return "forward_netbox.merge_forwardingestion"

    def get(self, request, pk):
        return redirect(get_object_or_404(self.queryset, pk=pk).get_absolute_url())

    def post(self, request, pk):
        ingestion = get_object_or_404(self.queryset, pk=pk)
        if not ingestion.can_accept_merge_failures:
            messages.error(
                request,
                _(
                    "This ingestion is not stalled behind accepted-failure "
                    "eligible failures."
                ),
            )
            return redirect(ingestion.get_absolute_url())

        failed = int(ingestion.failed_change_count or 0)
        job = ingestion.enqueue_merge_job(
            user=request.user,
            remove_branch=True,
            accept_reported_failures=True,
        )
        messages.warning(
            request,
            _(
                "Queued job #%(job)s to complete the merge accepting "
                "%(failed)s failed change(s). They remain recorded as ingestion "
                "issues and the acceptance is attributed to you."
            )
            % {"job": job.pk, "failed": failed},
        )
        return redirect(ingestion.get_absolute_url())


@register_model_view(
    ForwardIngestion,
    name="change_diff",
    path="change/<int:change_pk>",
    kwargs={"model": ForwardIngestion},
)
class ForwardIngestionChangesDiffView(BaseObjectView):
    queryset = ForwardIngestion.objects.all()
    template_name = "forward_netbox/inc/diff.html"

    def get_required_permission(self):
        return "forward_netbox.view_forwardingestion"

    def get(self, request, **kwargs):
        ingestion = get_object_or_404(self.queryset, pk=kwargs["pk"])
        change_id = kwargs.get("change_pk")
        if not request.htmx or not change_id:
            return render(
                request,
                self.template_name,
                {
                    "change": None,
                    "prechange_data": None,
                    "postchange_data": None,
                    "diff_added": None,
                    "diff_removed": None,
                    "size": "lg",
                },
            )

        change = get_object_or_404(
            ChangeDiff,
            pk=change_id,
            branch_id=ingestion.branch_id,
        )
        if change.original and change.modified:
            diff_added = shallow_compare_dict(
                change.original or {},
                change.modified or {},
                exclude=["last_updated"],
            )
            diff_removed = {key: change.original.get(key) for key in diff_added}
        else:
            diff_added = None
            diff_removed = None

        return render(
            request,
            self.template_name,
            {
                "change": change,
                "prechange_data": change.original,
                "postchange_data": change.modified,
                "diff_added": diff_added,
                "diff_removed": diff_removed,
                "size": "lg",
            },
        )


@register_model_view(ForwardIngestion, "change")
class ForwardIngestionChangesView(generic.ObjectChildrenView):
    queryset = ForwardIngestion.objects.all()
    child_model = ChangeDiff
    table = ForwardIngestionChangesTable
    filterset = ForwardIngestionChangeFilterSet
    actions = ()
    template_name = "generic/object_children.html"
    tab = ViewTab(
        label=_("Changes"),
        badge=lambda obj: (
            ChangeDiff.objects.filter(branch=obj.branch).count()
            if obj.branch_id
            else object_changes_for_ingestion(obj).count()
        ),
        permission="forward_netbox.view_forwardingestion",
    )

    def get(self, request, *args, **kwargs):
        parent = self.get_object(**kwargs)
        if parent.branch_id:
            self.child_model = ChangeDiff
            self.table = ForwardIngestionChangesTable
            self.filterset = ForwardIngestionChangeFilterSet
        else:
            self.child_model = ObjectChange
            self.table = ForwardIngestionObjectChangesTable
            self.filterset = ForwardIngestionObjectChangeFilterSet
        return super().get(request, *args, **kwargs)

    def get_children(self, request, parent):
        if parent.branch_id:
            return ChangeDiff.objects.filter(branch=parent.branch)
        return object_changes_for_ingestion(parent)


@register_model_view(ForwardIngestion, "issues")
class ForwardIngestionIssuesView(generic.ObjectChildrenView):
    queryset = ForwardIngestion.objects.all()
    child_model = ForwardIngestionIssue
    table = ForwardIngestionIssueTable
    template_name = "generic/object_children.html"
    filterset = ForwardIngestionIssueFilterSet
    tab = ViewTab(
        label=_("Ingestion Issues"),
        badge=lambda obj: ForwardIngestionIssue.objects.filter(ingestion=obj).count(),
        permission="forward_netbox.view_forwardingestionissue",
    )

    def get_children(self, request, parent):
        return ForwardIngestionIssue.objects.filter(ingestion=parent)


def _ingestion_delete_refusal(ingestion) -> str:
    """Why the database will refuse this delete, or "" when it will not.

    Four relations to an ingestion are PROTECT, not one. `ForwardContributorBaseline`
    holds durable convergence evidence, and three of the four ownership models
    carried by `ForwardIngestionProvenanceMixin` hold identity and claim
    evidence. (The fourth, `ForwardOwnershipReconciliation`, cascades - see
    `_ingestion_holds_current_ownership` for what guards it instead.) This
    docstring previously named only the baseline, and the check behind it could
    only see the baseline, because every provenance relation declares
    ``related_name="+"`` and Django hides those from `_meta.related_objects` — see
    `protecting_relations`. Reporting all of them up front turns an unhandled
    `ProtectedError` into something an operator can act on.
    """
    return _ingestion_delete_refusal_detail(ingestion)[0]


def _ingestion_holds_current_ownership(ingestion) -> bool:
    """Whether this ingestion is the one proving ownership complete right now.

    `ForwardOwnershipReconciliation` rows cascade with their ingestion, which is
    what makes a stale ingestion deletable at all. That leaves one case the
    database can no longer refuse on its own: a sync whose ingestion is held
    *only* by reconciliation rows, where deleting it would discard the very
    evidence that ownership has converged and silently regress the sync to
    Incomplete. "Currently complete" is not a guess here - it is exactly
    `ownership_generation_complete`, which requires a COMPLETED row at this
    generation for every domain `required_ownership_domains` names. Since there
    is one row per (sync, domain) and STATUS_TAGS is always required, at most
    one ingestion per sync can satisfy it.

    A check that cannot complete refuses. A wrong refusal costs a support
    question; a wrong delete costs evidence that no reverse migration can bring
    back.
    """
    from .utilities.ownership import ownership_generation_complete

    sync = getattr(ingestion, "sync", None)
    if sync is None or not ingestion.pk:
        return False
    try:
        return bool(ownership_generation_complete(sync, ingestion.pk))
    except Exception:  # noqa: BLE001 - unprovable safety is not safety
        return True


def _ingestion_holds_live_baseline(ingestion) -> bool:
    """Whether the sync's live contributor baseline belongs to this ingestion.

    Exactly the condition `refuse_ingestion_delete_with_live_baseline` enforces,
    and it has to stay exactly that condition: the receiver is what actually
    refuses, so a view that answered a different question would either refuse a
    delete the database allows or offer one it will reject.

    `is_current`, not `status`. Every ingestion that ever promoted leaves a
    baseline behind and promotion only marks the previous one SUPERSEDED, so
    reading anything wider than "is this the live one" is what made three of a
    customer's ingestions each claim to be the baseline for the sync.

    Fails closed: an unprovable answer refuses rather than offering a delete the
    receiver would abort.
    """
    from .models import ForwardContributorBaseline

    if not ingestion.pk:
        return False
    try:
        return ForwardContributorBaseline.objects.filter(
            ingestion=ingestion,
            is_current=True,
        ).exists()
    except Exception:  # noqa: BLE001 - a refusal must not become a 500
        return True


def _ingestion_delete_refusal_detail(ingestion) -> tuple[str, bool]:
    """The refusal text, and whether it is expected rather than a fault.

    A customer reported this twice as a bug: one ingestion in a sequence would
    not delete while every other one did, and the UI announced it in red as an
    error. It is neither an error nor specific to that ingestion - it is the
    baseline, the record of what has already converged, and NetBox protects it
    on purpose. Saying so plainly is the difference between a support ticket and
    a shrug.

    Two refusals are expected rather than faults, and both name a way forward
    that the product actually offers. The baseline is permanent. Current
    ownership evidence is not: it moves to the next ingestion as soon as one
    reconciles, so the answer is "sync again, then delete this" rather than
    "remove these records", which was the instruction that had no supported
    action behind it.
    """
    references = describe_protecting_references(type(ingestion), ingestion.pk)
    # A spent baseline cascades with its ingestion now, so it no longer appears
    # here at all and no longer has to be described. A live one does not cascade
    # and is not a PROTECT relation either, so it has to be asked about directly
    # rather than read off the protecting references the way it used to be.
    if _ingestion_holds_live_baseline(ingestion):
        held_by = ", ".join(f"{label} ({count})" for label, count in references)
        also_held = f" It is also held by {held_by}." if held_by else ""
        return (
            f"{ingestion} holds the current contributor baseline for this sync - "
            "the durable record of what has already converged - so it is kept "
            "until a later sync promotes a new one. This is expected, not a "
            "failure, and it is why one ingestion in a sequence will not delete "
            f"when the others do.{also_held} Run the sync again; once a newer "
            "ingestion has promoted its own baseline, this one deletes normally "
            "and takes the spent record with it."
        ), True
    if _ingestion_holds_current_ownership(ingestion):
        return (
            f"{ingestion} is the ingestion whose ownership reconciliation is "
            "currently complete for this sync, so it is the record that proves "
            "every ownership domain this sync reconciles - status tags, and "
            "scope tags and virtual parents where enabled - has converged. "
            "Deleting it would discard that proof and report this sync's "
            "ownership as incomplete until a later ingestion reconciles. This "
            "is expected, not a failure. Run the sync again; once a newer "
            "ingestion has taken over the reconciliation, this one deletes "
            "normally."
        ), True
    if not references:
        return "", False
    held_by = ", ".join(f"{label} ({count})" for label, count in references)
    return (
        f"{ingestion} cannot be deleted while it is still referenced by "
        f"{held_by}. Those records are convergence evidence for this ingestion; "
        "remove them first if you genuinely intend to discard it."
    ), False


@register_model_view(ForwardIngestion, "delete")
class ForwardIngestionDeleteView(generic.ObjectDeleteView):
    """Delete a single ingestion, reporting a protected reference rather than 500.

    Restores an action removed in 2.6.3: a `NoReverseMatch` crash on the
    Ingestions list was fixed by dropping the delete action instead of
    registering the view it pointed at, which left ingestions undeletable.
    """

    queryset = ForwardIngestion.objects.all()

    def get(self, request, *args, **kwargs):
        # Refuse on the confirmation page too, not only on submit. NetBox's
        # delete view renders every dependent object, and an ingestion owns one
        # `ForwardDeviceIdentity` per synced device — so the operator met a wall
        # of several hundred device names and only learned the delete was
        # impossible after confirming it. The one ingestion in the system that
        # legitimately cannot be deleted should say so first.
        refusal, expected = _ingestion_delete_refusal_detail(self.get_object(**kwargs))
        if refusal:
            # An expected protection is a warning, not an error: red text is
            # what made a customer report this as a defect twice.
            (messages.warning if expected else messages.error)(request, refusal)
            return redirect(self.get_return_url(request))
        return super().get(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        refusal, expected = _ingestion_delete_refusal_detail(self.get_object(**kwargs))
        if refusal:
            # An expected protection is a warning, not an error: red text is
            # what made a customer report this as a defect twice.
            (messages.warning if expected else messages.error)(request, refusal)
            return redirect(self.get_return_url(request))
        return super().post(request, *args, **kwargs)


@register_model_view(ForwardIngestion, "bulk_delete", path="delete", detail=False)
class ForwardIngestionBulkDeleteView(generic.BulkDeleteView):
    """Bulk delete, skipping protected ingestions instead of failing the batch.

    One protected ingestion must not abandon the rest of the selection, so the
    protected rows are reported by name and the remainder proceed.
    """

    queryset = ForwardIngestion.objects.all()
    table = ForwardIngestionTable

    def _filter_deletable(self, request, queryset):
        protected = [
            ingestion for ingestion in queryset if _ingestion_delete_refusal(ingestion)
        ]
        if not protected:
            return queryset
        for item in protected:
            refusal, expected = _ingestion_delete_refusal_detail(item)
            (messages.warning if expected else messages.error)(request, refusal)
        return queryset.exclude(pk__in=[item.pk for item in protected])

    def post(self, request, *args, **kwargs):
        self.queryset = self._filter_deletable(request, self.get_queryset(request))
        return super().post(request, *args, **kwargs)


@register_model_view(ForwardIngestionIssue, "list", path="", detail=False)
class ForwardIngestionIssueListView(generic.ObjectListView):
    """The list route the issue detail page reverses for its breadcrumb.

    Registering a detail view without its list is a 500: NetBox reverses
    `<app>:<model>_list` from the object view, so opening an issue raised
    `NoReverseMatch: Reverse for 'forwardingestionissue_list' not found`. A
    customer hit it on the one page that exists to explain a failed merge.

    2.6.3 met the same class of bug on the Ingestions list and resolved it by
    dropping the action that pointed at the missing view, which left ingestions
    undeletable for three releases. Register the route instead.

    The installed-route probe did not catch this because it renders menu lists,
    and this model has no menu entry - it is reached only from an ingestion.
    """

    queryset = ForwardIngestionIssue.objects.all()
    filterset = ForwardIngestionIssueFilterSet
    table = ForwardIngestionIssueTable
    # Read-only diagnostic evidence: written by a sync, never hand-edited.
    actions = (BulkExport,)


@register_model_view(ForwardIngestionIssue)
class ForwardIngestionIssueView(generic.ObjectView):
    """Detail for one ingestion issue.

    The list truncates `message` and shows only the exception class, so a
    failure could not be inspected at all — four IntegrityError rows blocked a
    customer's baseline with no way to see which constraint they violated.
    """

    queryset = ForwardIngestionIssue.objects.all()
    template_name = "forward_netbox/forwardingestionissue.html"


@register_model_view(ForwardDeviceAnalysis, "list", path="", detail=False)
class ForwardDeviceAnalysisListView(generic.ObjectListView):
    queryset = ForwardDeviceAnalysis.objects.all()
    filterset = ForwardDeviceAnalysisFilterSet
    table = ForwardDeviceAnalysisTable
    # Read-only overlay: refreshed by the device-analysis job, not hand-edited.
    actions = (BulkExport, BulkDelete)


@register_model_view(ForwardDeviceAnalysis)
class ForwardDeviceAnalysisView(generic.ObjectView):
    queryset = ForwardDeviceAnalysis.objects.all()


@register_model_view(ForwardDeviceAnalysis, "delete")
class ForwardDeviceAnalysisDeleteView(generic.ObjectDeleteView):
    queryset = ForwardDeviceAnalysis.objects.all()


@register_model_view(ForwardDeviceAnalysis, "bulk_delete", path="delete", detail=False)
class ForwardDeviceAnalysisBulkDeleteView(generic.BulkDeleteView):
    queryset = ForwardDeviceAnalysis.objects.all()
    table = ForwardDeviceAnalysisTable


@register_model_view(ForwardDriftPolicy, "list", path="", detail=False)
class ForwardDriftPolicyListView(generic.ObjectListView):
    queryset = ForwardDriftPolicy.objects.all()
    filterset = ForwardDriftPolicyFilterSet
    table = ForwardDriftPolicyTable
    actions = (AddObject, BulkExport, BulkEdit, BulkRename, BulkDelete)


@register_model_view(ForwardDriftPolicy, "add", detail=False)
@register_model_view(ForwardDriftPolicy, "edit")
class ForwardDriftPolicyEditView(generic.ObjectEditView):
    queryset = ForwardDriftPolicy.objects.all()
    form = ForwardDriftPolicyForm


@register_model_view(ForwardDriftPolicy)
class ForwardDriftPolicyView(generic.ObjectView):
    queryset = ForwardDriftPolicy.objects.all()
    template_name = "forward_netbox/forwarddriftpolicy.html"


@register_model_view(ForwardDriftPolicy, "delete")
class ForwardDriftPolicyDeleteView(generic.ObjectDeleteView):
    queryset = ForwardDriftPolicy.objects.all()


@register_model_view(ForwardDriftPolicy, "bulk_edit", path="edit", detail=False)
class ForwardDriftPolicyBulkEditView(generic.BulkEditView):
    queryset = ForwardDriftPolicy.objects.all()
    table = ForwardDriftPolicyTable
    form = ForwardDriftPolicyBulkEditForm


@register_model_view(ForwardDriftPolicy, "bulk_rename", path="rename", detail=False)
class ForwardDriftPolicyBulkRenameView(generic.BulkRenameView):
    queryset = ForwardDriftPolicy.objects.all()


@register_model_view(ForwardDriftPolicy, "bulk_delete", path="delete", detail=False)
class ForwardDriftPolicyBulkDeleteView(generic.BulkDeleteView):
    queryset = ForwardDriftPolicy.objects.all()
    table = ForwardDriftPolicyTable


@register_model_view(ForwardValidationRun, "list", path="", detail=False)
class ForwardValidationRunListView(generic.ObjectListView):
    queryset = ForwardValidationRun.objects.select_related("sync", "policy")
    filterset = ForwardValidationRunFilterSet
    table = ForwardValidationRunTable
    actions = (BulkExport, BulkDelete)


@register_model_view(ForwardValidationRun)
class ForwardValidationRunView(generic.ObjectView):
    queryset = ForwardValidationRun.objects.select_related("sync", "policy", "job")
    template_name = "forward_netbox/forwardvalidationrun.html"


@register_model_view(ForwardValidationRun, "force_allow")
class ForwardValidationRunForceAllowView(BaseObjectView):
    queryset = ForwardValidationRun.objects.select_related("sync", "policy", "job")
    template_name = "forward_netbox/inc/validation_force_allow_form.html"
    form = ForwardValidationRunForceAllowForm

    def get_required_permission(self):
        return "forward_netbox.change_forwardvalidationrun"

    def get(self, request, pk):
        obj = get_object_or_404(self.queryset, pk=pk)
        if request.htmx:
            viewname = get_viewname(self.queryset.model, action="force_allow")
            form_url = reverse(viewname, kwargs={"pk": obj.pk})
            form = self.form(initial=request.GET)
            restrict_form_fields(form, request.user)
            return render(
                request,
                self.template_name,
                {
                    "object": obj,
                    "object_type": self.queryset.model._meta.verbose_name,
                    "form": form,
                    "form_url": form_url,
                },
            )
        return redirect(obj.get_absolute_url())

    def post(self, request, pk):
        validation_run = get_object_or_404(self.queryset, pk=pk)
        form = self.form(request.POST)
        restrict_form_fields(form, request.user)
        if form.is_valid():
            validation_run.force_allow(
                user=request.user,
                reason=form.cleaned_data["reason"],
            )
            messages.success(
                request,
                f"Marked {validation_run} as force-allowed for the blocked validation reasons.",
            )
            return redirect(validation_run.get_absolute_url())

        for field, errors in form.errors.items():
            for error in errors:
                messages.error(request, f"{field}: {error}")
        for error in form.non_field_errors():
            messages.error(request, error)
        return redirect(validation_run.get_absolute_url())


@register_model_view(ForwardValidationRun, "delete")
class ForwardValidationRunDeleteView(generic.ObjectDeleteView):
    queryset = ForwardValidationRun.objects.all()


@register_model_view(ForwardValidationRun, "bulk_delete", path="delete", detail=False)
class ForwardValidationRunBulkDeleteView(generic.BulkDeleteView):
    queryset = ForwardValidationRun.objects.all()
    table = ForwardValidationRunTable


# --- Device CVE tab (optional netbox_dlm integration) -----------------------
# The 2.5.2 Vulnerability feed lands one netbox_dlm row per device+CVE; this
# tab surfaces the actual CVEs behind a device's exposure count without a
# Forward round-trip. Registered only when the plugin is installed so core
# installs carry no dead tab.
from django.apps import apps as django_apps  # noqa: E402

if django_apps.is_installed("netbox_dlm"):
    from dcim.models import Device  # noqa: E402

    def _device_vulnerabilities(device):
        Vulnerability = django_apps.get_model("netbox_dlm", "vulnerability")
        return (
            Vulnerability.objects.filter(device=device)
            .select_related("cve", "software_version")
            .order_by("cve__severity", "cve__cve_id")
        )

    @register_model_view(Device, "forward_cves", path="forward-cves")
    class ForwardDeviceCVEView(generic.ObjectView):
        queryset = Device.objects.all()
        template_name = "forward_netbox/device_cves.html"
        tab = ViewTab(
            label=_("CVEs"),
            badge=lambda obj: _device_vulnerabilities(obj).count(),
            permission="dcim.view_device",
            hide_if_empty=True,
        )

        def get_extra_context(self, request, instance):
            vulnerabilities = list(_device_vulnerabilities(instance))
            order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
            vulnerabilities.sort(
                key=lambda v: (
                    order.get((v.cve.severity or "").lower(), 4),
                    v.cve.cve_id,
                )
            )
            return {
                "vulnerabilities": vulnerabilities,
                "severity_totals": {
                    label: sum(
                        1
                        for v in vulnerabilities
                        if (v.cve.severity or "").lower() == label
                    )
                    for label in ("critical", "high", "medium", "low")
                },
            }
