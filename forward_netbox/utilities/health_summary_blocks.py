from collections import Counter

from django.conf import settings

from .. import NetboxForwardConfig
from ..choices import forward_configured_models
from ..choices import ForwardDiffFallbackModeChoices
from .branch_budget import apply_dependency_dry_run
from .branch_budget import DEFAULT_MODEL_CHANGE_DENSITY
from .density_learning import density_profile_summary
from .execution_telemetry import _build_query_mode_summary
from .forward_api import DEFAULT_NQE_PAGE_SIZE
from .model_contracts import architecture_contract_for_model
from .runtime_guidance import configured_rq_default_timeout
from .runtime_guidance import effective_forward_job_timeout
from .runtime_guidance import source_pushdown_alert_thresholds
from .runtime_guidance import source_query_fetch_concurrency
from .runtime_guidance import source_timeout_seconds
from .sync_primitives import DEPENDENCY_PARENT_DEVICE_MODELS


DEPENDENCY_PREFLIGHT_RULES = (
    {
        "code": "interface_routing_dependency_omitted",
        "selected_model": "dcim.interface",
        "omitted_models": (
            "netbox_routing.bgppeer",
            "netbox_routing.bgppeeraddressfamily",
            "netbox_peering_manager.peeringsession",
        ),
        "message": (
            "dcim.interface delete/prune rows can be blocked by protected "
            "routing or peering references when these models are omitted."
        ),
    },
    {
        "code": "ipaddress_routing_dependency_omitted",
        "selected_model": "ipam.ipaddress",
        "omitted_models": (
            "netbox_routing.bgppeer",
            "netbox_routing.bgppeeraddressfamily",
            "netbox_peering_manager.peeringsession",
        ),
        "message": (
            "ipam.ipaddress delete/prune rows can be blocked by protected "
            "routing or peering references when these models are omitted."
        ),
    },
    {
        "code": "device_child_dependency_omitted",
        "selected_model": "dcim.device",
        "omitted_models": (
            "dcim.interface",
            "dcim.cable",
            "dcim.module",
            "dcim.inventoryitem",
            "ipam.ipaddress",
            "ipam.prefix",
            "netbox_routing.bgppeer",
            "netbox_routing.bgppeeraddressfamily",
            "netbox_peering_manager.peeringsession",
        ),
        "message": (
            "dcim.device delete/prune rows can be blocked by child, IPAM, "
            "routing, or peering references when these models are omitted."
        ),
    },
)

PARENT_DEVICE_DEPENDENT_MODELS = DEPENDENCY_PARENT_DEVICE_MODELS


def _safe_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _job_data(ingestion):
    if ingestion is None:
        return {}
    data = ingestion.get_job_logs(ingestion.job)
    return data if isinstance(data, dict) else {}


def _job_summary(ingestion, key):
    value = _job_data(ingestion).get(key) or {}
    return dict(value) if isinstance(value, dict) else {}


def source_summary(sync):
    source = sync.source
    return {
        "id": source.pk,
        "name": source.name,
        "url": source.url,
        "status": source.status,
        "type": source.type,
        "last_synced": source.last_synced.isoformat() if source.last_synced else None,
    }


def runtime_summary(sync):
    branch_plugin_available = True
    try:
        import netbox_branching  # noqa: F401
    except Exception:
        branch_plugin_available = False
    parameters = sync.parameters or {}
    return {
        "plugin_version": NetboxForwardConfig.version,
        "netbox_version": getattr(settings, "VERSION", ""),
        "branching_available": branch_plugin_available,
        "auto_merge": bool(sync.auto_merge),
        "enable_bulk_orm": bool(parameters.get("enable_bulk_orm", False)),
        "diff_fallback_mode": parameters.get(
            "diff_fallback_mode",
            ForwardDiffFallbackModeChoices.ALLOW_FALLBACK,
        ),
        "max_changes_per_staging_item": sync.get_max_changes_per_staging_item(),
        "source_timeout_seconds": source_timeout_seconds(sync),
        "query_fetch_concurrency": source_query_fetch_concurrency(sync),
        "pushdown_alert_thresholds": source_pushdown_alert_thresholds(sync),
        "rq_default_timeout_seconds": configured_rq_default_timeout(),
        "forward_job_timeout_seconds": effective_forward_job_timeout(),
        "snapshot_selector": sync.get_snapshot_id(),
    }


def query_map_summary(query_map):
    return {
        "id": query_map.pk,
        "name": query_map.name,
        "model": query_map.model_string,
        "mode": query_map.execution_mode,
        "query_repository": query_map.query_repository or "",
        "query_path": query_map.query_path or "",
        "has_query_id": bool(query_map.query_id),
        "has_commit_id": bool(query_map.commit_id),
        "built_in": bool(query_map.built_in),
    }


def validation_summary(validation_run):
    if validation_run is None:
        return None
    return {
        "id": validation_run.pk,
        "status": validation_run.status,
        "allowed": bool(validation_run.allowed),
        "snapshot_selector": validation_run.snapshot_selector,
        "snapshot_id": validation_run.snapshot_id,
        "blocking_reason_count": len(validation_run.blocking_reasons or []),
        "created": (
            validation_run.created.isoformat() if validation_run.created else None
        ),
        "completed": (
            validation_run.completed.isoformat() if validation_run.completed else None
        ),
    }


def ingestion_summary(ingestion):
    if ingestion is None:
        return None
    model_results = list(getattr(ingestion, "model_results", None) or [])
    execution_summary = ingestion.get_execution_summary()
    return {
        "id": ingestion.pk,
        "name": ingestion.name,
        "sync_mode": ingestion.sync_mode or "",
        "baseline_ready": bool(ingestion.baseline_ready),
        "snapshot_selector": ingestion.snapshot_selector or "",
        "snapshot_id": ingestion.snapshot_id or "",
        "branch": ingestion.branch.name if ingestion.branch else "",
        "branch_status": ingestion.branch.status if ingestion.branch else "",
        "stage_job_status": ingestion.job.status if ingestion.job else "",
        "merge_job_status": ingestion.merge_job.status if ingestion.merge_job else "",
        "issue_count": ingestion.issues.count(),
        "applied_change_count": ingestion.applied_change_count,
        "failed_change_count": ingestion.failed_change_count,
        "analysis_summary": ingestion.get_analysis_summary(),
        "execution_summary": execution_summary,
        "workload_preview": ingestion.get_workload_summary(),
        "dependency_lookup_cache": _job_summary(ingestion, "dependency_lookup_cache"),
        "dependency_parent_coverage": _job_summary(
            ingestion, "dependency_parent_coverage"
        ),
        "forward_api_usage": _job_summary(ingestion, "forward_api_usage"),
        "query_path_resolution": query_path_resolution_summary(ingestion),
        "query_modes": _build_query_mode_summary(model_results),
        "created": ingestion.created.isoformat() if ingestion.created else None,
    }


def query_path_resolution_summary(ingestion):
    if ingestion is None:
        return {}
    summary = ingestion.get_execution_summary().get("query_path_resolution") or {}
    return dict(summary) if isinstance(summary, dict) else {}


def dependency_preflight_summary(sync, enabled_models):
    enabled_models = sorted(str(model) for model in (enabled_models or []) if model)
    enabled_model_set = set(enabled_models)
    configured_models = set(forward_configured_models())
    delete_or_prune = _delete_or_prune_evidence(sync)
    apply_dry_run = apply_dependency_dry_run(enabled_models)
    warnings = []

    for rule in DEPENDENCY_PREFLIGHT_RULES:
        selected_model = rule["selected_model"]
        if selected_model not in enabled_model_set:
            continue
        if selected_model == "dcim.device" and not delete_or_prune:
            continue
        omitted_models = [
            model
            for model in rule["omitted_models"]
            if model in configured_models and model not in enabled_model_set
        ]
        if not omitted_models:
            continue
        warnings.append(
            {
                "code": rule["code"],
                "status": "warn",
                "selected_model": selected_model,
                "omitted_models": omitted_models,
                "suggested_models": omitted_models,
                "message": (
                    f"{rule['message']} Omitted model(s): "
                    f"{', '.join(omitted_models)}."
                ),
                "delete_dependency_rank": _delete_dependency_rank(selected_model),
                "omitted_delete_dependency_ranks": {
                    model: _delete_dependency_rank(model) for model in omitted_models
                },
            }
        )

    if "dcim.device" not in enabled_model_set and "dcim.device" in configured_models:
        for selected_model in sorted(PARENT_DEVICE_DEPENDENT_MODELS):
            if selected_model not in enabled_model_set:
                continue
            warnings.append(
                {
                    "code": "parent_device_model_omitted",
                    "status": "warn",
                    "selected_model": selected_model,
                    "omitted_models": ["dcim.device"],
                    "suggested_models": ["dcim.device"],
                    "message": (
                        f"{selected_model} rows rely on dcim.device coverage in the "
                        "same sync. Include dcim.device or expect child rows to be "
                        "skipped when parent device rows are missing."
                    ),
                    "delete_dependency_rank": _delete_dependency_rank(selected_model),
                    "omitted_delete_dependency_ranks": {
                        "dcim.device": _delete_dependency_rank("dcim.device"),
                    },
                }
            )

    if warnings:
        return {
            "status": "warn",
            "message": (
                f"{len(warnings)} scoped dependency warning(s) found; include the "
                "suggested models or expect protected delete skips to remain "
                "non-blocking row issues."
            ),
            "enabled_models": enabled_models,
            "delete_or_prune_possible": bool(delete_or_prune),
            "delete_or_prune_evidence": delete_or_prune,
            "apply_dry_run": apply_dry_run,
            "warnings": warnings,
        }
    return {
        "status": "warn" if apply_dry_run.get("status") == "warn" else "pass",
        "message": (
            apply_dry_run.get("message")
            if apply_dry_run.get("status") == "warn"
            else "No scoped dependency warnings were found for enabled models."
        ),
        "enabled_models": enabled_models,
        "delete_or_prune_possible": bool(delete_or_prune),
        "delete_or_prune_evidence": delete_or_prune,
        "apply_dry_run": apply_dry_run,
        "warnings": [],
    }


def _delete_or_prune_evidence(sync):
    evidence = []
    source_parameters = getattr(getattr(sync, "source", None), "parameters", {}) or {}
    if source_parameters.get("device_tag_prune_out_of_scope"):
        evidence.append("device_tag_prune_out_of_scope")
    latest_ingestion = getattr(sync, "last_ingestion", None)
    if latest_ingestion is not None and getattr(
        latest_ingestion, "baseline_ready", False
    ):
        evidence.append("baseline_ready_for_diff_deletes")
    return sorted(set(evidence))


def _delete_dependency_rank(model_string):
    try:
        return architecture_contract_for_model(model_string).delete_dependency_rank
    except Exception:
        return None


def _delete_wave_ingestion_summary(ingestion):
    if ingestion is None:
        return {
            "id": None,
            "deleted_change_count": 0,
            "dependency_skip_issues": {"count": 0, "models": {}},
        }
    issues = ingestion.issues.filter(exception="ForwardDependencySkipError")
    issue_models = Counter(
        model or "unknown" for model in issues.values_list("model", flat=True)
    )
    return {
        "id": ingestion.pk,
        "deleted_change_count": int(ingestion.deleted_change_count or 0),
        "dependency_skip_issues": {
            "count": sum(issue_models.values()),
            "models": dict(issue_models),
        },
    }


def delete_wave_summary(latest_ingestion):
    ingestion = _delete_wave_ingestion_summary(latest_ingestion)
    deleted = ingestion["deleted_change_count"]
    dependency_skips = ingestion["dependency_skip_issues"]["count"]
    if latest_ingestion is None:
        status = "info"
        message = "No ingestion is available yet."
    elif dependency_skips:
        status = "warn"
        message = (
            f"Latest ingestion deleted {deleted} object(s) and recorded "
            f"{dependency_skips} protected dependency skip(s)."
        )
    else:
        status = "pass"
        message = (
            f"Latest ingestion deleted {deleted} object(s) without dependency skips."
        )
    return {
        "available": latest_ingestion is not None,
        "status": status,
        "phase": "complete" if latest_ingestion is not None else "unavailable",
        "message": message,
        "latest_ingestion": ingestion,
    }


def throughput_summary(sync, latest_ingestion):
    branch = getattr(latest_ingestion, "branch", None)
    stage_job = getattr(latest_ingestion, "job", None)
    merge_job = getattr(latest_ingestion, "merge_job", None)
    failed = int(getattr(latest_ingestion, "failed_change_count", 0) or 0)
    issue_count = latest_ingestion.issues.count() if latest_ingestion is not None else 0
    return {
        "available": latest_ingestion is not None,
        "status": "warn" if failed else "pass" if latest_ingestion else "info",
        "message": (
            "Latest ingestion has failed changes and remains incomplete."
            if failed
            else (
                "Latest ingestion branch and job state is available."
                if latest_ingestion is not None
                else "No ingestion is available yet."
            )
        ),
        "ingestion_id": getattr(latest_ingestion, "pk", None),
        "branch": getattr(branch, "name", ""),
        "branch_status": getattr(branch, "status", ""),
        "stage_job_status": getattr(stage_job, "status", ""),
        "merge_job_status": getattr(merge_job, "status", ""),
        "applied_change_count": int(
            getattr(latest_ingestion, "applied_change_count", 0) or 0
        ),
        "failed_change_count": failed,
        "issue_count": issue_count,
        "worker_timeout_seconds": configured_rq_default_timeout(),
        "forward_job_timeout_seconds": effective_forward_job_timeout(),
        "query_fetch_concurrency": source_query_fetch_concurrency(sync),
        "nqe_page_size": _source_nqe_page_size(sync),
    }


def _source_nqe_page_size(sync):
    parameters = getattr(getattr(sync, "source", None), "parameters", None) or {}
    value = parameters.get("nqe_page_size")
    if value in ("", None):
        return DEFAULT_NQE_PAGE_SIZE
    try:
        return int(value)
    except (TypeError, ValueError):
        return DEFAULT_NQE_PAGE_SIZE


def density_learning_summary(sync):
    return density_profile_summary(
        density_map=sync.get_model_change_density(),
        density_profile=sync.get_model_change_density_profile(),
        default_density_map=DEFAULT_MODEL_CHANGE_DENSITY,
    )
