from django.core.exceptions import ValidationError
from django.utils import timezone
from django.utils.translation import gettext as _

from ..choices import forward_configured_models
from ..choices import FORWARD_SUPPORTED_MODELS
from ..choices import ForwardDiffFallbackModeChoices
from ..choices import ForwardSourceDeploymentChoices
from ..utilities.forward_api import DEFAULT_FORWARD_SAAS_API_REQUESTS_PER_MINUTE
from ..utilities.forward_api import LATEST_PROCESSED_SNAPSHOT
from ..utilities.forward_api import MAX_FORWARD_API_REQUESTS_PER_MINUTE
from ..utilities.forward_api import MAX_NQE_ASYNC_MAX_POLLS
from ..utilities.forward_api import MAX_NQE_ASYNC_POLL_INTERVAL_SECONDS
from ..utilities.forward_api import MAX_NQE_FETCH_ALL_MAX_PAGES
from ..utilities.forward_api import MAX_NQE_IDENTICAL_FULL_PAGE_STREAK_LIMIT
from ..utilities.forward_api import MAX_NQE_PAGE_SIZE
from ..utilities.forward_api import MAX_QUERY_FETCH_CONCURRENCY
from .branch_budget import MODEL_CHANGE_DENSITY_PARAMETER
from .branch_budget import MODEL_CHANGE_DENSITY_PROFILE_PARAMETER
from .sync_contracts import normalize_coalesce_fields
from .sync_contracts import validate_query_shape_for_model
from .sync_facade import DEFAULT_ENABLE_BULK_ORM_FOR_NEW_SYNCS
from .sync_primitives import DEPENDENCY_PARENT_DEVICE_MODELS
from .tag_contracts import validate_scope_tag_names


def clean_forward_source(source):
    source.url = source.url.rstrip("/")
    parameters = dict(source.parameters or {})
    invalid = sorted(
        set(parameters.keys())
        - {
            "username",
            "password",
            "verify",
            "timeout",
            "retries",
            "api_requests_per_minute",
            "network_id",
            "nqe_page_size",
            "query_fetch_concurrency",
            "nqe_fetch_all_max_pages",
            "nqe_identical_full_page_streak_limit",
            "nqe_async_poll_interval_seconds",
            "nqe_async_max_polls",
            "workload_fetch_timeout_seconds",
            "query_diagnostics_enabled",
            "pushdown_fallback_warn_rate",
            "pushdown_runtime_fallback_warn_share",
            "pushdown_diff_warn_ratio",
            "device_tag_include_tags",
            "device_tag_exclude_tags",
            "device_tag_include_match",
            "device_tag_filter_mode",
            "device_tag_prune_out_of_scope",
            "apply_device_scope_tags",
            "sync_device_tags",
            "sync_endpoints",
            "sync_generic_endpoints",
            "scope_endpoints_by_include_tags",
        }
    )
    if invalid:
        raise ValidationError(_(f"Unsupported Forward source keys: {invalid}"))
    if source.type == ForwardSourceDeploymentChoices.SAAS:
        source.url = "https://fwd.app"
        parameters["verify"] = True
        parameters.setdefault(
            "api_requests_per_minute",
            DEFAULT_FORWARD_SAAS_API_REQUESTS_PER_MINUTE,
        )
    if not (parameters.get("username") and parameters.get("password")):
        raise ValidationError(_("Provide a Forward username and password."))
    if not isinstance(parameters.get("verify", True), bool):
        raise ValidationError(_("`verify` must be a boolean."))
    if not isinstance(parameters.get("sync_endpoints", False), bool):
        raise ValidationError(_("`sync_endpoints` must be a boolean."))
    if not isinstance(parameters.get("sync_generic_endpoints", False), bool):
        raise ValidationError(_("`sync_generic_endpoints` must be a boolean."))
    parameters.setdefault("scope_endpoints_by_include_tags", True)
    if not isinstance(parameters.get("scope_endpoints_by_include_tags"), bool):
        raise ValidationError(_("`scope_endpoints_by_include_tags` must be a boolean."))
    if parameters.get("network_id") is not None and not isinstance(
        parameters.get("network_id"), str
    ):
        raise ValidationError(_("`network_id` must be a string."))
    for key in (
        "device_tag_include_tags",
        "device_tag_exclude_tags",
        "sync_device_tags",
    ):
        if parameters.get(key) is None:
            continue
        if not isinstance(parameters.get(key), list) or any(
            not isinstance(item, str) for item in parameters.get(key)
        ):
            raise ValidationError(_(f"`{key}` must be a list of strings."))
    validate_scope_tag_names(parameters.get("device_tag_include_tags") or [])
    include_match = parameters.get("device_tag_include_match")
    if include_match is not None:
        if not isinstance(include_match, str):
            raise ValidationError(_("`device_tag_include_match` must be a string."))
        if include_match not in {"any", "all"}:
            raise ValidationError(
                _("`device_tag_include_match` must be `any` or `all`.")
            )
    filter_mode = parameters.get("device_tag_filter_mode")
    if filter_mode is not None:
        if not isinstance(filter_mode, str):
            raise ValidationError(_("`device_tag_filter_mode` must be a string."))
        if filter_mode not in {"local", "query_parameters"}:
            raise ValidationError(
                _("`device_tag_filter_mode` must be `local` or `query_parameters`.")
            )
    if parameters.get("nqe_page_size") is not None:
        try:
            nqe_page_size = int(parameters.get("nqe_page_size"))
        except (TypeError, ValueError) as exc:
            raise ValidationError(_("`nqe_page_size` must be an integer.")) from exc
        if nqe_page_size < 1 or nqe_page_size > MAX_NQE_PAGE_SIZE:
            raise ValidationError(
                _(f"`nqe_page_size` must be between 1 and {MAX_NQE_PAGE_SIZE}.")
            )
        parameters["nqe_page_size"] = nqe_page_size
    if parameters.get("query_fetch_concurrency") is not None:
        try:
            query_fetch_concurrency = int(parameters.get("query_fetch_concurrency"))
        except (TypeError, ValueError) as exc:
            raise ValidationError(
                _("`query_fetch_concurrency` must be an integer.")
            ) from exc
        if (
            query_fetch_concurrency < 1
            or query_fetch_concurrency > MAX_QUERY_FETCH_CONCURRENCY
        ):
            raise ValidationError(
                _(
                    "`query_fetch_concurrency` must be between 1 and "
                    f"{MAX_QUERY_FETCH_CONCURRENCY}."
                )
            )
        parameters["query_fetch_concurrency"] = query_fetch_concurrency
    if parameters.get("api_requests_per_minute") is not None:
        try:
            api_requests_per_minute = int(parameters.get("api_requests_per_minute"))
        except (TypeError, ValueError) as exc:
            raise ValidationError(
                _("`api_requests_per_minute` must be an integer.")
            ) from exc
        if (
            api_requests_per_minute < 0
            or api_requests_per_minute > MAX_FORWARD_API_REQUESTS_PER_MINUTE
        ):
            raise ValidationError(
                _(
                    "`api_requests_per_minute` must be between 0 and "
                    f"{MAX_FORWARD_API_REQUESTS_PER_MINUTE}."
                )
            )
        parameters["api_requests_per_minute"] = api_requests_per_minute
        # Forward SaaS enforces a per-tenant request-rate ceiling; a value above
        # the safe SaaS rate can get the tenant throttled or blocked. Clamp SaaS
        # sources down to the known-safe rate so a misconfigured ceiling cannot
        # lock the tenant out (custom deployments keep the full range).
        if (
            source.type == ForwardSourceDeploymentChoices.SAAS
            and api_requests_per_minute > DEFAULT_FORWARD_SAAS_API_REQUESTS_PER_MINUTE
        ):
            parameters["api_requests_per_minute"] = (
                DEFAULT_FORWARD_SAAS_API_REQUESTS_PER_MINUTE
            )
    if parameters.get("workload_fetch_timeout_seconds") is not None:
        try:
            wf_timeout = int(parameters.get("workload_fetch_timeout_seconds"))
        except (TypeError, ValueError):
            raise ValidationError(
                _("`workload_fetch_timeout_seconds` must be an integer.")
            )
        if wf_timeout < 0:
            raise ValidationError(
                _("`workload_fetch_timeout_seconds` must be >= 0 (0 disables).")
            )
        parameters["workload_fetch_timeout_seconds"] = wf_timeout
    if parameters.get("nqe_fetch_all_max_pages") is not None:
        try:
            nqe_fetch_all_max_pages = int(parameters.get("nqe_fetch_all_max_pages"))
        except (TypeError, ValueError) as exc:
            raise ValidationError(
                _("`nqe_fetch_all_max_pages` must be an integer.")
            ) from exc
        if nqe_fetch_all_max_pages < 1 or nqe_fetch_all_max_pages > int(
            MAX_NQE_FETCH_ALL_MAX_PAGES
        ):
            raise ValidationError(
                _(
                    "`nqe_fetch_all_max_pages` must be between 1 and "
                    f"{MAX_NQE_FETCH_ALL_MAX_PAGES}."
                )
            )
        parameters["nqe_fetch_all_max_pages"] = nqe_fetch_all_max_pages
    if parameters.get("nqe_identical_full_page_streak_limit") is not None:
        try:
            nqe_identical_full_page_streak_limit = int(
                parameters.get("nqe_identical_full_page_streak_limit")
            )
        except (TypeError, ValueError) as exc:
            raise ValidationError(
                _("`nqe_identical_full_page_streak_limit` must be an integer.")
            ) from exc
        if (
            nqe_identical_full_page_streak_limit < 1
            or nqe_identical_full_page_streak_limit
            > int(MAX_NQE_IDENTICAL_FULL_PAGE_STREAK_LIMIT)
        ):
            raise ValidationError(
                _(
                    "`nqe_identical_full_page_streak_limit` must be between 1 and "
                    f"{MAX_NQE_IDENTICAL_FULL_PAGE_STREAK_LIMIT}."
                )
            )
        parameters["nqe_identical_full_page_streak_limit"] = (
            nqe_identical_full_page_streak_limit
        )
    if parameters.get("query_diagnostics_enabled") is not None and not isinstance(
        parameters.get("query_diagnostics_enabled"), bool
    ):
        raise ValidationError(_("`query_diagnostics_enabled` must be a boolean."))
    if parameters.get("nqe_async_poll_interval_seconds") is not None:
        try:
            nqe_async_poll_interval_seconds = float(
                parameters.get("nqe_async_poll_interval_seconds")
            )
        except (TypeError, ValueError) as exc:
            raise ValidationError(
                _("`nqe_async_poll_interval_seconds` must be a number.")
            ) from exc
        if (
            nqe_async_poll_interval_seconds < 0.0
            or nqe_async_poll_interval_seconds > MAX_NQE_ASYNC_POLL_INTERVAL_SECONDS
        ):
            raise ValidationError(
                _(
                    "`nqe_async_poll_interval_seconds` must be between 0 and "
                    f"{MAX_NQE_ASYNC_POLL_INTERVAL_SECONDS}."
                )
            )
        parameters["nqe_async_poll_interval_seconds"] = nqe_async_poll_interval_seconds
    if parameters.get("nqe_async_max_polls") is not None:
        try:
            nqe_async_max_polls = int(parameters.get("nqe_async_max_polls"))
        except (TypeError, ValueError) as exc:
            raise ValidationError(
                _("`nqe_async_max_polls` must be an integer.")
            ) from exc
        if nqe_async_max_polls < 1 or nqe_async_max_polls > MAX_NQE_ASYNC_MAX_POLLS:
            raise ValidationError(
                _(
                    "`nqe_async_max_polls` must be between 1 and "
                    f"{MAX_NQE_ASYNC_MAX_POLLS}."
                )
            )
        parameters["nqe_async_max_polls"] = nqe_async_max_polls
    for key in (
        "pushdown_fallback_warn_rate",
        "pushdown_runtime_fallback_warn_share",
        "pushdown_diff_warn_ratio",
    ):
        if parameters.get(key) is None:
            continue
        try:
            ratio_value = float(parameters.get(key))
        except (TypeError, ValueError) as exc:
            raise ValidationError(
                _(f"`{key}` must be a number between 0 and 1.")
            ) from exc
        if ratio_value < 0.0 or ratio_value > 1.0:
            raise ValidationError(_(f"`{key}` must be between 0 and 1."))
        parameters[key] = ratio_value
    source.parameters = parameters


def clean_forward_nqe_map(nqe_map):
    query_reference_count = sum(
        bool(value)
        for value in (
            nqe_map.query_id,
            getattr(nqe_map, "query_path", ""),
            nqe_map.query,
        )
    )
    if query_reference_count != 1:
        raise ValidationError(
            _("Set exactly one of `Query ID`, `Query Path`, or `Query`.")
        )
    if getattr(nqe_map, "query_path", "") and not getattr(
        nqe_map, "query_repository", ""
    ):
        raise ValidationError(_("Set `Query Repository` when `Query Path` is set."))
    if nqe_map.parameters and not isinstance(nqe_map.parameters, dict):
        raise ValidationError(_("Parameters must be a JSON object."))
    try:
        normalized = normalize_coalesce_fields(
            nqe_map.model_string,
            nqe_map.coalesce_fields,
            allow_default=True,
        )
    except ValueError as exc:
        raise ValidationError(_(str(exc)))
    nqe_map.coalesce_fields = normalized
    if nqe_map.query:
        try:
            validate_query_shape_for_model(
                nqe_map.model_string,
                nqe_map.query,
                nqe_map.coalesce_fields,
            )
        except ValueError as exc:
            raise ValidationError(_(str(exc)))


def _enabled_parent_device_dependency_models(sync):
    return [
        model_string
        for model_string in DEPENDENCY_PARENT_DEVICE_MODELS
        if sync.is_model_enabled(model_string)
    ]


def clean_forward_sync(sync):
    parameters = dict(sync.parameters or {})
    invalid = sorted(
        set(parameters.keys())
        - {
            "auto_merge",
            "max_changes_per_staging_item",
            "snapshot_id",
            "enable_bulk_orm",
            "set_primary_ip_from_mgmt_tag",
            "diff_fallback_mode",
            "webhook_secret",
            "validation_schedule_interval",
            "preview_schedule_interval",
            "stuck_recovery",
            # Post-sync controls. Device status-tag ownership is mandatory;
            # vsys parent linking defaults on and can be explicitly disabled.
            "auto_refresh_device_analysis",
            "auto_link_vsys_parents",
            MODEL_CHANGE_DENSITY_PARAMETER,
            MODEL_CHANGE_DENSITY_PROFILE_PARAMETER,
            *FORWARD_SUPPORTED_MODELS,
        }
    )
    if invalid:
        raise ValidationError(_(f"Unsupported Forward sync keys: {invalid}"))
    snapshot_id = parameters.get("snapshot_id") or LATEST_PROCESSED_SNAPSHOT
    if not isinstance(snapshot_id, str):
        raise ValidationError(_("`snapshot_id` must be a string."))
    parameters["snapshot_id"] = snapshot_id
    if not isinstance(parameters.get("webhook_secret", ""), str):
        raise ValidationError(_("`webhook_secret` must be a string."))
    for schedule_key in (
        "validation_schedule_interval",
        "preview_schedule_interval",
    ):
        value = parameters.get(schedule_key, 0)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValidationError(
                _(f"`{schedule_key}` must be a non-negative integer (minutes).")
            )
        if schedule_key == "preview_schedule_interval" and 0 < value < 60:
            raise ValidationError(
                _(
                    "`preview_schedule_interval` must be at least 60 minutes "
                    "(the dependency preview is a full live dry-run)."
                )
            )
        parameters[schedule_key] = value
    parameters["auto_merge"] = bool(parameters.get("auto_merge", sync.auto_merge))
    parameters["set_primary_ip_from_mgmt_tag"] = bool(
        parameters.get("set_primary_ip_from_mgmt_tag", False)
    )
    diff_fallback_mode = parameters.get(
        "diff_fallback_mode",
        ForwardDiffFallbackModeChoices.ALLOW_FALLBACK,
    )
    valid_diff_fallback_modes = {
        choice[0] for choice in ForwardDiffFallbackModeChoices.CHOICES
    }
    if diff_fallback_mode not in valid_diff_fallback_modes:
        raise ValidationError(_("`diff_fallback_mode` is not supported."))
    parameters["diff_fallback_mode"] = diff_fallback_mode
    if diff_fallback_mode == ForwardDiffFallbackModeChoices.REQUIRE_DIFF:
        source_parameters = dict(
            getattr(getattr(sync, "source", None), "parameters", {}) or {}
        )
        if source_parameters.get("device_tag_prune_out_of_scope"):
            raise ValidationError(
                _(
                    "`Require diff` is incompatible with prune-out-of-scope. Pruning "
                    "out-of-scope devices needs a full query of the complete in-scope "
                    "device set, which `Require diff` forbids, so every model would "
                    "fail the diff fetch and the sync would block. Set diff fallback "
                    "mode to `Allow full fallback`, or turn off prune-out-of-scope on "
                    "the source."
                )
            )
    parameters["enable_bulk_orm"] = bool(
        parameters.get(
            "enable_bulk_orm",
            DEFAULT_ENABLE_BULK_ORM_FOR_NEW_SYNCS,
        )
    )
    try:
        max_changes_per_staging_item = int(
            parameters.get(
                "max_changes_per_staging_item", sync.get_max_changes_per_staging_item()
            )
        )
    except (TypeError, ValueError):
        raise ValidationError(
            _("`max_changes_per_staging_item` must be a positive integer.")
        )
    if max_changes_per_staging_item < 1:
        raise ValidationError(
            _("`max_changes_per_staging_item` must be a positive integer.")
        )
    parameters["max_changes_per_staging_item"] = max_changes_per_staging_item
    sync.auto_merge = parameters["auto_merge"]
    sync.parameters = parameters


def validate_forward_sync_runtime(sync):
    if sync.scheduled and sync.scheduled < timezone.now():
        raise ValidationError({"scheduled": _("Scheduled time must be in the future.")})
    enabled_parent_device_models = _enabled_parent_device_dependency_models(sync)
    if enabled_parent_device_models and not sync.is_model_enabled("dcim.device"):
        raise ValidationError(
            _(
                "`dcim.device` must be enabled when syncing child models that "
                "depend on device coverage: "
                f"{', '.join(enabled_parent_device_models)}."
            )
        )
    if not any(
        sync.is_model_enabled(model_string)
        for model_string in forward_configured_models()
    ):
        raise ValidationError(_("Select at least one NetBox model to sync."))
