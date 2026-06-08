from django.core.exceptions import ValidationError
from django.utils import timezone
from django.utils.translation import gettext as _

from ..choices import forward_configured_models
from ..choices import FORWARD_SUPPORTED_MODELS
from ..choices import ForwardDiffFallbackModeChoices
from ..choices import ForwardExecutionBackendChoices
from ..choices import ForwardSourceDeploymentChoices
from ..utilities.forward_api import DEFAULT_FORWARD_SAAS_API_REQUESTS_PER_MINUTE
from ..utilities.forward_api import LATEST_PROCESSED_SNAPSHOT
from ..utilities.forward_api import MAX_FORWARD_API_REQUESTS_PER_MINUTE
from ..utilities.forward_api import MAX_NQE_FETCH_ALL_MAX_PAGES
from ..utilities.forward_api import MAX_NQE_IDENTICAL_FULL_PAGE_STREAK_LIMIT
from ..utilities.forward_api import MAX_NQE_PAGE_SIZE
from ..utilities.forward_api import MAX_QUERY_FETCH_CONCURRENCY
from ..utilities.query_fetch import MAX_PREFLIGHT_ROW_LIMIT
from .branch_budget import BRANCH_RUN_STATE_PARAMETER
from .branch_budget import MODEL_CHANGE_DENSITY_PARAMETER
from .branch_budget import MODEL_CHANGE_DENSITY_PROFILE_PARAMETER
from .sync_contracts import normalize_coalesce_fields
from .sync_contracts import validate_query_shape_for_model
from .sync_facade import DEFAULT_ENABLE_BULK_ORM_FOR_NEW_SYNCS


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
            "query_preflight_enabled",
            "query_preflight_row_limit",
            "query_diagnostics_enabled",
            "pushdown_fallback_warn_rate",
            "pushdown_runtime_fallback_warn_share",
            "pushdown_diff_warn_ratio",
            "device_tag_include",
            "device_tag_exclude",
            "device_tag_include_tags",
            "device_tag_exclude_tags",
            "device_tag_include_match",
            "device_tag_filter_mode",
            "device_tag_prune_out_of_scope",
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
    if parameters.get("network_id") is not None and not isinstance(
        parameters.get("network_id"), str
    ):
        raise ValidationError(_("`network_id` must be a string."))
    for key in ("device_tag_include", "device_tag_exclude"):
        if parameters.get(key) is not None and not isinstance(parameters.get(key), str):
            raise ValidationError(_(f"`{key}` must be a string."))
    for key in ("device_tag_include_tags", "device_tag_exclude_tags"):
        if parameters.get(key) is None:
            continue
        if not isinstance(parameters.get(key), list) or any(
            not isinstance(item, str) for item in parameters.get(key)
        ):
            raise ValidationError(_(f"`{key}` must be a list of strings."))
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
    if parameters.get("query_preflight_enabled") is not None and not isinstance(
        parameters.get("query_preflight_enabled"), bool
    ):
        raise ValidationError(_("`query_preflight_enabled` must be a boolean."))
    if parameters.get("query_preflight_row_limit") is not None:
        try:
            query_preflight_row_limit = int(parameters.get("query_preflight_row_limit"))
        except (TypeError, ValueError) as exc:
            raise ValidationError(
                _("`query_preflight_row_limit` must be an integer.")
            ) from exc
        if query_preflight_row_limit < 1 or query_preflight_row_limit > int(
            MAX_PREFLIGHT_ROW_LIMIT
        ):
            raise ValidationError(
                _(
                    "`query_preflight_row_limit` must be between 1 and "
                    f"{MAX_PREFLIGHT_ROW_LIMIT}."
                )
            )
        parameters["query_preflight_row_limit"] = query_preflight_row_limit
    if parameters.get("query_diagnostics_enabled") is not None and not isinstance(
        parameters.get("query_diagnostics_enabled"), bool
    ):
        raise ValidationError(_("`query_diagnostics_enabled` must be a boolean."))
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


def clean_forward_sync(sync):
    parameters = dict(sync.parameters or {})
    invalid = sorted(
        set(parameters.keys())
        - {
            "auto_merge",
            "execution_backend",
            "multi_branch",
            "max_changes_per_branch",
            "snapshot_id",
            "enable_bulk_orm",
            "bulk_orm_models",
            "scheduler_overlap",
            "diff_fallback_mode",
            BRANCH_RUN_STATE_PARAMETER,
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
    execution_backend = parameters.get(
        "execution_backend",
        ForwardExecutionBackendChoices.BRANCHING,
    )
    valid_backends = {choice[0] for choice in ForwardExecutionBackendChoices.CHOICES}
    if execution_backend not in valid_backends:
        raise ValidationError(_("`execution_backend` is not supported."))
    parameters["execution_backend"] = execution_backend
    parameters["auto_merge"] = bool(parameters.get("auto_merge", sync.auto_merge))
    parameters["scheduler_overlap"] = bool(
        parameters.get("scheduler_overlap", False) and parameters["auto_merge"]
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
    parameters["enable_bulk_orm"] = bool(
        parameters.get(
            "enable_bulk_orm",
            DEFAULT_ENABLE_BULK_ORM_FOR_NEW_SYNCS,
        )
    )
    bulk_orm_models = parameters.get("bulk_orm_models") or []
    if not isinstance(bulk_orm_models, list) or any(
        not isinstance(model_string, str) for model_string in bulk_orm_models
    ):
        raise ValidationError(_("`bulk_orm_models` must be a list of model strings."))
    parameters["bulk_orm_models"] = sorted(set(bulk_orm_models))
    parameters["multi_branch"] = True
    try:
        max_changes_per_branch = int(
            parameters.get("max_changes_per_branch", sync.get_max_changes_per_branch())
        )
    except (TypeError, ValueError):
        raise ValidationError(_("`max_changes_per_branch` must be a positive integer."))
    if max_changes_per_branch < 1:
        raise ValidationError(_("`max_changes_per_branch` must be a positive integer."))
    parameters["max_changes_per_branch"] = max_changes_per_branch
    sync.auto_merge = parameters["auto_merge"]
    sync.parameters = parameters


def validate_forward_sync_runtime(sync):
    if sync.scheduled and sync.scheduled < timezone.now():
        raise ValidationError({"scheduled": _("Scheduled time must be in the future.")})
    if not any(
        sync.is_model_enabled(model_string)
        for model_string in forward_configured_models()
    ):
        raise ValidationError(_("Select at least one NetBox model to sync."))
