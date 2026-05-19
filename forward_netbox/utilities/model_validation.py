from django.core.exceptions import ValidationError
from django.utils import timezone
from django.utils.translation import gettext as _

from ..choices import forward_configured_models
from ..choices import FORWARD_SUPPORTED_MODELS
from ..choices import ForwardExecutionBackendChoices
from ..choices import ForwardSourceDeploymentChoices
from ..utilities.forward_api import LATEST_PROCESSED_SNAPSHOT
from ..utilities.forward_api import MAX_NQE_PAGE_SIZE
from ..utilities.forward_api import MAX_QUERY_FETCH_CONCURRENCY
from .sync_contracts import normalize_coalesce_fields
from .sync_contracts import validate_query_shape_for_model


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
            "network_id",
            "nqe_page_size",
            "query_fetch_concurrency",
            "device_tag_include",
            "device_tag_exclude",
            "device_tag_include_tags",
            "device_tag_exclude_tags",
            "device_tag_include_match",
            "device_tag_filter_mode",
        }
    )
    if invalid:
        raise ValidationError(_(f"Unsupported Forward source keys: {invalid}"))
    if source.type == ForwardSourceDeploymentChoices.SAAS:
        source.url = "https://fwd.app"
        parameters["verify"] = True
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
    parameters["enable_bulk_orm"] = bool(parameters.get("enable_bulk_orm", False))
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
