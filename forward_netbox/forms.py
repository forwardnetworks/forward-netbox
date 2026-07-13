from core.choices import JobIntervalChoices
from django import forms
from django.contrib.contenttypes.models import ContentType
from netbox.forms import NetBoxModelBulkEditForm
from netbox.forms import NetBoxModelForm
from utilities.datetime import local_now
from utilities.forms import add_blank_choice
from utilities.forms import ConfirmationForm
from utilities.forms import get_field_value
from utilities.forms.fields import CommentField
from utilities.forms.rendering import FieldSet
from utilities.forms.widgets import APISelect
from utilities.forms.widgets import APISelectMultiple
from utilities.forms.widgets import DateTimePicker
from utilities.forms.widgets import HTMXSelect
from utilities.forms.widgets import NumberWithOptions

from .choices import forward_configured_models
from .choices import FORWARD_OPTIONAL_MODELS
from .choices import ForwardDiffFallbackModeChoices
from .choices import ForwardExecutionBackendChoices
from .choices import ForwardSourceDeploymentChoices
from .choices import ForwardSourceStatusChoices
from .choices import ForwardSyncStatusChoices
from .exceptions import ForwardConnectivityError
from .exceptions import ForwardSyncError
from .models import ForwardDriftPolicy
from .models import ForwardNQEMap
from .models import ForwardSource
from .models import ForwardSync
from .utilities.forward_api import DEFAULT_FORWARD_API_REQUESTS_PER_MINUTE
from .utilities.forward_api import DEFAULT_FORWARD_API_TIMEOUT_SECONDS
from .utilities.forward_api import DEFAULT_FORWARD_SAAS_API_REQUESTS_PER_MINUTE
from .utilities.forward_api import DEFAULT_NQE_ASYNC_MAX_POLLS
from .utilities.forward_api import DEFAULT_NQE_ASYNC_POLL_INTERVAL_SECONDS
from .utilities.forward_api import DEFAULT_NQE_FETCH_ALL_MAX_PAGES
from .utilities.forward_api import DEFAULT_NQE_IDENTICAL_FULL_PAGE_STREAK_LIMIT
from .utilities.forward_api import DEFAULT_NQE_PAGE_SIZE
from .utilities.forward_api import DEFAULT_QUERY_DIAGNOSTICS_ENABLED
from .utilities.forward_api import DEFAULT_QUERY_FETCH_CONCURRENCY
from .utilities.forward_api import DEFAULT_QUERY_PREFLIGHT_ENABLED
from .utilities.forward_api import FORWARD_SAAS_API_HARD_BLOCK_REQUESTS_PER_MINUTE
from .utilities.forward_api import LATEST_COLLECTED_SNAPSHOT
from .utilities.forward_api import LATEST_PROCESSED_SNAPSHOT
from .utilities.forward_api import MAX_FORWARD_API_REQUESTS_PER_MINUTE
from .utilities.forward_api import MAX_NQE_ASYNC_MAX_POLLS
from .utilities.forward_api import MAX_NQE_ASYNC_POLL_INTERVAL_SECONDS
from .utilities.forward_api import MAX_NQE_FETCH_ALL_MAX_PAGES
from .utilities.forward_api import MAX_NQE_IDENTICAL_FULL_PAGE_STREAK_LIMIT
from .utilities.forward_api import MAX_NQE_PAGE_SIZE
from .utilities.forward_api import MAX_QUERY_FETCH_CONCURRENCY
from .utilities.query_fetch import DEFAULT_PREFLIGHT_ROW_LIMIT
from .utilities.query_fetch import MAX_PREFLIGHT_ROW_LIMIT
from .utilities.runtime_guidance import DEFAULT_PUSHDOWN_DIFF_WARN_RATIO
from .utilities.runtime_guidance import DEFAULT_PUSHDOWN_FALLBACK_WARN_RATE
from .utilities.runtime_guidance import (
    DEFAULT_PUSHDOWN_RUNTIME_FALLBACK_WARN_SHARE,
)
from .utilities.sync_facade import DEFAULT_ENABLE_BULK_ORM_FOR_NEW_SYNCS


def _configure_api_select(widget, query_params=None):
    widget.attrs.setdefault("selector", False)
    widget.attrs.setdefault("disabled", False)
    if query_params:
        widget.add_query_params(query_params)


def _selected_choice(selected_value):
    choices = [("", "---------")]
    if selected_value:
        choices.append((selected_value, selected_value))
    return choices


def _snapshot_selected_choice(selected_value):
    choices = []
    if selected_value == LATEST_PROCESSED_SNAPSHOT:
        choices.append((LATEST_PROCESSED_SNAPSHOT, "latestProcessed"))
    elif selected_value == LATEST_COLLECTED_SNAPSHOT:
        choices.append((LATEST_COLLECTED_SNAPSHOT, "latestCollected (skip backfilled)"))
    elif selected_value:
        choices.append((selected_value, selected_value))
    return choices


def _model_string_from_form(form):
    if form.is_bound:
        model_value = form.data.get("netbox_model")
        if not model_value:
            return ""
        if "." in str(model_value):
            return str(model_value).strip().lower()
        try:
            content_type = ContentType.objects.get(pk=model_value)
        except (ContentType.DoesNotExist, TypeError, ValueError):
            return ""
        return f"{content_type.app_label}.{content_type.model}".lower()
    instance_model = getattr(form.instance, "netbox_model", None)
    if instance_model:
        return f"{instance_model.app_label}.{instance_model.model}".lower()
    return ""


FORWARD_NQE_QUERY_MODE_CHOICES = (
    ("query_path", "Repository Query Path"),
    ("query_id", "Direct Query ID"),
    ("query", "Raw Query Text"),
)

FORWARD_NQE_QUERY_REPOSITORY_CHOICES = (
    ("org", "Org Repository"),
    ("fwd", "Forward Library"),
)

FORWARD_NQE_BULK_QUERY_OPERATION_CHOICES = (
    ("", "No query reference change"),
    (
        "bind_query_path",
        "Use repository query paths (query IDs resolve at sync time)",
    ),
    (
        "publish_bundled_query_path",
        "Publish bundled queries and use repository query paths",
    ),
    ("restore_raw_query", "Restore bundled raw query text"),
)


class FlexibleMultipleChoiceField(forms.MultipleChoiceField):
    """Accept scalar widget payloads and coerce them into list form."""

    def to_python(self, value):
        if isinstance(value, str):
            value = [value]
        return super().to_python(value)


class ForwardSourceForm(NetBoxModelForm):
    comments = CommentField()

    class Meta:
        model = ForwardSource
        fields = [
            "name",
            "type",
            "url",
            "description",
            "owner",
            "comments",
        ]
        widgets = {
            "type": HTMXSelect(),
        }

    @staticmethod
    def _normalize_tag_values(value):
        if not value:
            return []
        if isinstance(value, str):
            return [part.strip() for part in value.split(",") if part.strip()]
        if isinstance(value, (list, tuple)):
            return [str(part).strip() for part in value if str(part).strip()]
        return [str(value).strip()] if str(value).strip() else []

    def _bound_tag_values(self, field_name):
        if not self.is_bound:
            return []
        if hasattr(self.data, "getlist"):
            values = self.data.getlist(field_name) or self.data.getlist(
                f"{field_name}[]"
            )
            if values:
                return values
            scalar = self.data.get(field_name) or self.data.get(f"{field_name}[]")
            return self._normalize_tag_values(scalar)
        raw = self.data.get(field_name) or self.data.get(f"{field_name}[]")
        return self._normalize_tag_values(raw)

    def _default_api_requests_per_minute(self, source_type=None):
        source_type = source_type or getattr(
            self, "source_type", ForwardSourceDeploymentChoices.SAAS
        )
        if source_type == ForwardSourceDeploymentChoices.SAAS:
            return DEFAULT_FORWARD_SAAS_API_REQUESTS_PER_MINUTE
        return DEFAULT_FORWARD_API_REQUESTS_PER_MINUTE

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.source_type = (
            get_field_value(self, "type")
            or getattr(self.instance, "type", None)
            or ForwardSourceDeploymentChoices.SAAS
        )

        self.fieldsets = [FieldSet("name", "type", "url", name="Source")]

        self.fields["url"] = forms.URLField(
            required=True,
            label="Base URL",
            widget=forms.TextInput(attrs={"class": "form-control"}),
            help_text="For example https://fwd.app or https://my-forward.example.com.",
        )
        self.fields["username"] = forms.CharField(
            required=False,
            label="Username",
            help_text="Forward username used for basic authentication.",
        )
        self.fields["password"] = forms.CharField(
            required=False,
            label="Password",
            widget=forms.PasswordInput(render_value=False),
            help_text=(
                "Required for network discovery and sync. Leave blank only when editing "
                "an existing source to preserve the stored password."
            ),
        )
        self.fields["timeout"] = forms.IntegerField(
            required=False,
            min_value=1,
            label="Timeout",
            help_text="Timeout for Forward API requests in seconds.",
            widget=forms.NumberInput(attrs={"class": "form-control"}),
        )
        self.fields["nqe_page_size"] = forms.IntegerField(
            required=False,
            min_value=1,
            max_value=MAX_NQE_PAGE_SIZE,
            label="NQE Page Size",
            help_text=f"Rows requested per NQE page. Default: {DEFAULT_NQE_PAGE_SIZE}.",
            widget=forms.NumberInput(attrs={"class": "form-control"}),
        )
        self.fields["query_fetch_concurrency"] = forms.IntegerField(
            required=False,
            min_value=1,
            max_value=MAX_QUERY_FETCH_CONCURRENCY,
            label="Query Fetch Concurrency",
            help_text=(
                "Maximum concurrent NQE map fetch jobs per sync preflight/workload "
                f"phase. Default: {DEFAULT_QUERY_FETCH_CONCURRENCY}."
            ),
            widget=forms.NumberInput(attrs={"class": "form-control"}),
        )
        self.fields["api_requests_per_minute"] = forms.IntegerField(
            required=False,
            min_value=0,
            max_value=MAX_FORWARD_API_REQUESTS_PER_MINUTE,
            label="Forward API Requests Per Minute",
            help_text=(
                "Optional per-source API request cap for this Forward user. "
                f"Forward SaaS defaults to {DEFAULT_FORWARD_SAAS_API_REQUESTS_PER_MINUTE} "
                "to stay below the "
                f"{FORWARD_SAAS_API_HARD_BLOCK_REQUESTS_PER_MINUTE} requests/minute "
                "hard-block threshold. Set 0 to disable."
            ),
            widget=forms.NumberInput(attrs={"class": "form-control"}),
        )
        self.fields["nqe_fetch_all_max_pages"] = forms.IntegerField(
            required=False,
            min_value=1,
            max_value=MAX_NQE_FETCH_ALL_MAX_PAGES,
            label="NQE Fetch-All Page Cap",
            help_text=(
                "Maximum pages allowed in one fetch-all query before failing fast. "
                f"Default: {DEFAULT_NQE_FETCH_ALL_MAX_PAGES}."
            ),
            widget=forms.NumberInput(attrs={"class": "form-control"}),
        )
        self.fields["nqe_identical_full_page_streak_limit"] = forms.IntegerField(
            required=False,
            min_value=1,
            max_value=MAX_NQE_IDENTICAL_FULL_PAGE_STREAK_LIMIT,
            label="NQE Identical-Page Streak Cap",
            help_text=(
                "Fail fetch-all when identical full pages repeat with no progress. "
                f"Default: {DEFAULT_NQE_IDENTICAL_FULL_PAGE_STREAK_LIMIT}."
            ),
            widget=forms.NumberInput(attrs={"class": "form-control"}),
        )
        self.fields["query_preflight_enabled"] = forms.BooleanField(
            required=False,
            label="Query Preflight",
            help_text=(
                "Run the preflight sample query phase before full workload fetch. "
                "Disable to reduce startup query overhead on large runs."
            ),
        )
        self.fields["query_preflight_row_limit"] = forms.IntegerField(
            required=False,
            min_value=1,
            max_value=MAX_PREFLIGHT_ROW_LIMIT,
            label="Query Preflight Row Limit",
            help_text=(
                "Sample rows fetched per query during preflight validation. "
                f"Default: {DEFAULT_PREFLIGHT_ROW_LIMIT}."
            ),
            widget=forms.NumberInput(attrs={"class": "form-control"}),
        )
        self.fields["query_diagnostics_enabled"] = forms.BooleanField(
            required=False,
            label="Query Diagnostics",
            help_text=(
                "Run additional NQE diagnostic queries for importability summaries. "
                "Disable to reduce query overhead during large ingestion runs."
            ),
        )
        self.fields["nqe_async_poll_interval_seconds"] = forms.FloatField(
            required=False,
            min_value=0.0,
            max_value=MAX_NQE_ASYNC_POLL_INTERVAL_SECONDS,
            label="Async NQE Poll Interval",
            help_text=(
                "Seconds between async status polls. "
                f"Default: {DEFAULT_NQE_ASYNC_POLL_INTERVAL_SECONDS}."
            ),
            widget=forms.NumberInput(attrs={"class": "form-control", "step": "0.1"}),
        )
        self.fields["nqe_async_max_polls"] = forms.IntegerField(
            required=False,
            min_value=1,
            max_value=MAX_NQE_ASYNC_MAX_POLLS,
            label="Async NQE Max Polls",
            help_text=(
                "Maximum async status polls before the query fails fast. "
                f"Default: {DEFAULT_NQE_ASYNC_MAX_POLLS}."
            ),
            widget=forms.NumberInput(attrs={"class": "form-control"}),
        )
        self.fields["pushdown_fallback_warn_rate"] = forms.FloatField(
            required=False,
            min_value=0.0,
            max_value=1.0,
            label="Pushdown Fallback Warn Rate",
            help_text=(
                "Warn in health/support when fallback fetch step rate meets or "
                f"exceeds this ratio. Default: {DEFAULT_PUSHDOWN_FALLBACK_WARN_RATE}."
            ),
            widget=forms.NumberInput(attrs={"class": "form-control", "step": "0.01"}),
        )
        self.fields["pushdown_runtime_fallback_warn_share"] = forms.FloatField(
            required=False,
            min_value=0.0,
            max_value=1.0,
            label="Pushdown Runtime Fallback Warn Share",
            help_text=(
                "Warn in health/support when fallback fetch runtime share meets or "
                "exceeds this ratio. Default: "
                f"{DEFAULT_PUSHDOWN_RUNTIME_FALLBACK_WARN_SHARE}."
            ),
            widget=forms.NumberInput(attrs={"class": "form-control", "step": "0.01"}),
        )
        self.fields["pushdown_diff_warn_ratio"] = forms.FloatField(
            required=False,
            min_value=0.0,
            max_value=1.0,
            label="Pushdown Diff Warn Ratio",
            help_text=(
                "Warn in health/support when diff execution ratio for eligible "
                f"query maps is at or below this ratio. Default: {DEFAULT_PUSHDOWN_DIFF_WARN_RATIO}."
            ),
            widget=forms.NumberInput(attrs={"class": "form-control", "step": "0.01"}),
        )
        self.fields["verify"] = forms.BooleanField(
            required=False,
            initial=True,
            label="Verify",
            help_text="Certificate validation. Uncheck only for custom deployments using self-signed certificates.",
        )
        self.fields["network_id"] = forms.ChoiceField(
            required=True,
            label="Network",
            choices=(),
            widget=APISelect(api_url="/api/plugins/forward/source/available-networks/"),
            help_text="Forward network used as the default for syncs using this source.",
        )
        self.fields["device_tag_include_tags"] = FlexibleMultipleChoiceField(
            required=False,
            choices=(),
            widget=APISelectMultiple(
                api_url="/api/plugins/forward/source/available-tags/"
            ),
            label="Device Tags Include",
            help_text=(
                "Optional Forward device tags. Devices must match the selected include logic."
            ),
        )
        self.fields["device_tag_exclude_tags"] = FlexibleMultipleChoiceField(
            required=False,
            choices=(),
            widget=APISelectMultiple(
                api_url="/api/plugins/forward/source/available-tags/"
            ),
            label="Device Tags Exclude",
            help_text=(
                "Optional Forward device tags. Devices with any selected tag are excluded."
            ),
        )
        self.fields["device_tag_include_match"] = forms.ChoiceField(
            required=False,
            label="Include Tag Match",
            choices=(
                ("any", "Any selected tag (OR)"),
                ("all", "All selected tags (AND)"),
            ),
            help_text="How include tags are matched.",
        )
        self.fields["device_tag_filter_mode"] = forms.ChoiceField(
            required=False,
            label="Tag Filter Mode",
            choices=(
                ("local", "Plugin Local Filter (default)"),
                (
                    "query_parameters",
                    "Forward Query Parameters (query_id/query compatible only)",
                ),
            ),
            help_text=(
                "Use Local Filter for maximum compatibility. Use Query Parameters only when "
                "your Forward query IDs support device_tag_include/device_tag_exclude."
            ),
        )
        self.fields["device_tag_prune_out_of_scope"] = forms.BooleanField(
            required=False,
            label="Prune Out-of-Scope Rows",
            help_text=(
                "When enabled, rows excluded by device-tag scope are treated as delete "
                "candidates during full query execution."
            ),
        )
        self.fields["apply_device_scope_tags"] = forms.BooleanField(
            required=False,
            label="Apply Device Scope Tags",
            help_text=(
                "When enabled, each synced device is tagged in NetBox with its "
                "Forward device-scope include tag(s), so in-scope devices can be "
                "filtered in NetBox."
            ),
        )
        self.fields["sync_endpoints"] = forms.BooleanField(
            required=False,
            label="Import SNMP Endpoints as Devices",
            help_text=(
                "Also import Forward SNMP endpoints (generic SSH/SNMP devices "
                "Forward collects but does not model as first-class devices, "
                "e.g. Avocent console servers) as NetBox devices, scoped by the "
                "same device tags."
            ),
        )
        self.fields["scope_endpoints_by_include_tags"] = forms.BooleanField(
            required=False,
            label="Scope SNMP Endpoints by Include Tags",
            help_text=(
                "Require imported SNMP endpoints to also carry the device "
                "include tags above (all/any per the include match). Only "
                "applies when include tags are set and endpoint import is on. "
                "Endpoints without an include tag will not import — and may be "
                "pruned if prune-out-of-scope is enabled. Leave off to import "
                "every SNMP endpoint regardless of include tags (exclude tags "
                "always apply)."
            ),
        )
        self.fields["sync_device_tags"] = FlexibleMultipleChoiceField(
            required=False,
            choices=(),
            widget=APISelectMultiple(
                api_url="/api/plugins/forward/source/available-tags/"
            ),
            label="Sync Device Tags",
            help_text=(
                "Forward device tags (e.g. Mgmt_*) to sync onto NetBox devices as "
                "device tags. Select one or more; leave empty to sync none. "
                "Independent of the include/exclude scope filters above."
            ),
        )
        _configure_api_select(
            self.fields["network_id"].widget,
            {
                "type": "$type",
                "url": "$url",
                "username": "$username",
                "password": "$password",
                "verify": "$verify",
            },
        )
        self.fields["device_tag_include_tags"].widget.attrs["multiple"] = "multiple"
        self.fields["device_tag_exclude_tags"].widget.attrs["multiple"] = "multiple"
        self.fields["sync_device_tags"].widget.attrs["multiple"] = "multiple"
        _configure_api_select(
            self.fields["device_tag_include_tags"].widget,
            {
                "type": "$type",
                "url": "$url",
                "username": "$username",
                "password": "$password",
                "verify": "$verify",
                "network_id": "$network_id",
            },
        )
        _configure_api_select(
            self.fields["device_tag_exclude_tags"].widget,
            {
                "type": "$type",
                "url": "$url",
                "username": "$username",
                "password": "$password",
                "verify": "$verify",
                "network_id": "$network_id",
            },
        )
        _configure_api_select(
            self.fields["sync_device_tags"].widget,
            {
                "type": "$type",
                "url": "$url",
                "username": "$username",
                "password": "$password",
                "verify": "$verify",
                "network_id": "$network_id",
            },
        )
        if self.instance.pk:
            self.fields["network_id"].widget.add_query_param(
                "source_id", self.instance.pk
            )
            self.fields["device_tag_include_tags"].widget.add_query_param(
                "source_id", self.instance.pk
            )
            self.fields["device_tag_exclude_tags"].widget.add_query_param(
                "source_id", self.instance.pk
            )
            self.fields["sync_device_tags"].widget.add_query_param(
                "source_id", self.instance.pk
            )

        parameters = self.instance.parameters or {}
        existing_username = parameters.get("username")
        existing_network_id = (
            self.data.get("network_id")
            if self.is_bound
            else (parameters.get("network_id") or "")
        )
        self.fields["username"].initial = existing_username
        self.fields["timeout"].initial = (
            parameters.get("timeout") or DEFAULT_FORWARD_API_TIMEOUT_SECONDS
        )
        self.fields["nqe_page_size"].initial = (
            parameters.get("nqe_page_size") or DEFAULT_NQE_PAGE_SIZE
        )
        self.fields["query_fetch_concurrency"].initial = (
            parameters.get("query_fetch_concurrency") or DEFAULT_QUERY_FETCH_CONCURRENCY
        )
        self.fields["api_requests_per_minute"].initial = (
            parameters.get("api_requests_per_minute")
            if parameters.get("api_requests_per_minute") not in ("", None)
            else self._default_api_requests_per_minute()
        )
        self.fields["nqe_fetch_all_max_pages"].initial = (
            parameters.get("nqe_fetch_all_max_pages") or DEFAULT_NQE_FETCH_ALL_MAX_PAGES
        )
        self.fields["nqe_identical_full_page_streak_limit"].initial = (
            parameters.get("nqe_identical_full_page_streak_limit")
            or DEFAULT_NQE_IDENTICAL_FULL_PAGE_STREAK_LIMIT
        )
        self.fields["query_preflight_enabled"].initial = bool(
            parameters.get("query_preflight_enabled", DEFAULT_QUERY_PREFLIGHT_ENABLED)
        )
        self.fields["query_preflight_row_limit"].initial = (
            parameters.get("query_preflight_row_limit") or DEFAULT_PREFLIGHT_ROW_LIMIT
        )
        self.fields["query_diagnostics_enabled"].initial = bool(
            parameters.get(
                "query_diagnostics_enabled", DEFAULT_QUERY_DIAGNOSTICS_ENABLED
            )
        )
        self.fields["nqe_async_poll_interval_seconds"].initial = (
            parameters.get("nqe_async_poll_interval_seconds")
            if parameters.get("nqe_async_poll_interval_seconds") not in ("", None)
            else DEFAULT_NQE_ASYNC_POLL_INTERVAL_SECONDS
        )
        self.fields["nqe_async_max_polls"].initial = (
            parameters.get("nqe_async_max_polls") or DEFAULT_NQE_ASYNC_MAX_POLLS
        )
        self.fields["pushdown_fallback_warn_rate"].initial = (
            parameters.get("pushdown_fallback_warn_rate")
            if parameters.get("pushdown_fallback_warn_rate") not in ("", None)
            else DEFAULT_PUSHDOWN_FALLBACK_WARN_RATE
        )
        self.fields["pushdown_runtime_fallback_warn_share"].initial = (
            parameters.get("pushdown_runtime_fallback_warn_share")
            if parameters.get("pushdown_runtime_fallback_warn_share") not in ("", None)
            else DEFAULT_PUSHDOWN_RUNTIME_FALLBACK_WARN_SHARE
        )
        self.fields["pushdown_diff_warn_ratio"].initial = (
            parameters.get("pushdown_diff_warn_ratio")
            if parameters.get("pushdown_diff_warn_ratio") not in ("", None)
            else DEFAULT_PUSHDOWN_DIFF_WARN_RATIO
        )
        self.fields["verify"].initial = parameters.get("verify", True)
        self.fields["network_id"].initial = existing_network_id
        self.fields["network_id"].choices = _selected_choice(existing_network_id)
        include_bound = []
        exclude_bound = []
        sync_tags_bound = []
        if self.is_bound:
            include_bound = self._bound_tag_values("device_tag_include_tags")
            exclude_bound = self._bound_tag_values("device_tag_exclude_tags")
            sync_tags_bound = self._bound_tag_values("sync_device_tags")
        include_initial = (
            include_bound
            if self.is_bound
            else parameters.get("device_tag_include_tags")
        )
        if include_initial is None and parameters.get("device_tag_include"):
            include_initial = [parameters.get("device_tag_include")]
        include_initial = self._normalize_tag_values(include_initial)
        exclude_initial = (
            exclude_bound
            if self.is_bound
            else parameters.get("device_tag_exclude_tags")
        )
        if exclude_initial is None and parameters.get("device_tag_exclude"):
            exclude_initial = [parameters.get("device_tag_exclude")]
        exclude_initial = self._normalize_tag_values(exclude_initial)
        sync_tags_initial = (
            sync_tags_bound if self.is_bound else parameters.get("sync_device_tags")
        )
        sync_tags_initial = self._normalize_tag_values(sync_tags_initial)
        self.fields["device_tag_include_tags"].initial = include_initial
        self.fields["device_tag_exclude_tags"].initial = exclude_initial
        self.fields["sync_device_tags"].initial = sync_tags_initial
        self.fields["sync_endpoints"].initial = bool(parameters.get("sync_endpoints"))
        self.fields["scope_endpoints_by_include_tags"].initial = bool(
            parameters.get("scope_endpoints_by_include_tags")
        )
        self.fields["device_tag_include_tags"].choices = [
            (tag, tag) for tag in include_initial
        ]
        self.fields["device_tag_exclude_tags"].choices = [
            (tag, tag) for tag in exclude_initial
        ]
        self.fields["sync_device_tags"].choices = [
            (tag, tag) for tag in sync_tags_initial
        ]
        self.fields["device_tag_include_match"].initial = (
            parameters.get("device_tag_include_match") or "any"
        )
        self.fields["device_tag_filter_mode"].initial = (
            parameters.get("device_tag_filter_mode") or "local"
        )
        self.fields["device_tag_prune_out_of_scope"].initial = bool(
            parameters.get("device_tag_prune_out_of_scope")
        )
        self.fields["apply_device_scope_tags"].initial = bool(
            parameters.get("apply_device_scope_tags")
        )

        if self.source_type == ForwardSourceDeploymentChoices.SAAS:
            self.fields["url"].initial = "https://fwd.app"
            self.fields["url"].disabled = True
            self.fields["url"].required = False
            self.fields["verify"].initial = True
            self.fieldsets.append(
                FieldSet(
                    "username",
                    "password",
                    "network_id",
                    "timeout",
                    "nqe_page_size",
                    "query_fetch_concurrency",
                    "api_requests_per_minute",
                    "nqe_fetch_all_max_pages",
                    "nqe_identical_full_page_streak_limit",
                    "query_preflight_enabled",
                    "query_preflight_row_limit",
                    "query_diagnostics_enabled",
                    "nqe_async_poll_interval_seconds",
                    "nqe_async_max_polls",
                    "pushdown_fallback_warn_rate",
                    "pushdown_runtime_fallback_warn_share",
                    "pushdown_diff_warn_ratio",
                    "device_tag_include_tags",
                    "device_tag_include_match",
                    "device_tag_exclude_tags",
                    "device_tag_filter_mode",
                    "device_tag_prune_out_of_scope",
                    "apply_device_scope_tags",
                    "sync_device_tags",
                    "sync_endpoints",
                    "scope_endpoints_by_include_tags",
                    name="Parameters",
                )
            )
        else:
            self.fieldsets.append(
                FieldSet(
                    "username",
                    "password",
                    "verify",
                    "network_id",
                    "timeout",
                    "nqe_page_size",
                    "query_fetch_concurrency",
                    "api_requests_per_minute",
                    "nqe_fetch_all_max_pages",
                    "nqe_identical_full_page_streak_limit",
                    "query_preflight_enabled",
                    "query_preflight_row_limit",
                    "query_diagnostics_enabled",
                    "nqe_async_poll_interval_seconds",
                    "nqe_async_max_polls",
                    "pushdown_fallback_warn_rate",
                    "pushdown_runtime_fallback_warn_share",
                    "pushdown_diff_warn_ratio",
                    "device_tag_include_tags",
                    "device_tag_include_match",
                    "device_tag_exclude_tags",
                    "device_tag_filter_mode",
                    "device_tag_prune_out_of_scope",
                    "apply_device_scope_tags",
                    "sync_device_tags",
                    "sync_endpoints",
                    "scope_endpoints_by_include_tags",
                    name="Parameters",
                )
            )

        self.fieldsets.append(FieldSet("description", "owner", name="Metadata"))

    def clean(self):
        cleaned = dict(self.cleaned_data)
        existing_parameters = self.instance.parameters or {}
        source_type = (
            cleaned.get("type")
            or self.source_type
            or ForwardSourceDeploymentChoices.SAAS
        )
        username = cleaned.get("username") or existing_parameters.get("username") or ""
        password = cleaned.get("password") or existing_parameters.get("password") or ""

        if not username or not password:
            raise forms.ValidationError("Provide a Forward username and password.")

        if source_type == ForwardSourceDeploymentChoices.SAAS:
            cleaned["url"] = "https://fwd.app"
            cleaned["verify"] = True
        elif not cleaned.get("url"):
            raise forms.ValidationError("Custom Forward sources require a base URL.")

        selected_network_id = cleaned.get("network_id") or ""
        include_tags = self._normalize_tag_values(
            cleaned.get("device_tag_include_tags")
        )
        exclude_tags = self._normalize_tag_values(
            cleaned.get("device_tag_exclude_tags")
        )
        if not include_tags:
            include_tags = self._normalize_tag_values(
                self._bound_tag_values("device_tag_include_tags")
            )
        if not exclude_tags:
            exclude_tags = self._normalize_tag_values(
                self._bound_tag_values("device_tag_exclude_tags")
            )
        sync_device_tags = self._normalize_tag_values(cleaned.get("sync_device_tags"))
        if not sync_device_tags:
            sync_device_tags = self._normalize_tag_values(
                self._bound_tag_values("sync_device_tags")
            )
        candidate_parameters = {
            "username": username,
            "password": password,
            "verify": (
                True
                if source_type == ForwardSourceDeploymentChoices.SAAS
                else cleaned.get("verify", True)
            ),
            "timeout": cleaned.get("timeout")
            or existing_parameters.get("timeout")
            or DEFAULT_FORWARD_API_TIMEOUT_SECONDS,
            "nqe_page_size": cleaned.get("nqe_page_size")
            or existing_parameters.get("nqe_page_size")
            or DEFAULT_NQE_PAGE_SIZE,
            "query_fetch_concurrency": cleaned.get("query_fetch_concurrency")
            or existing_parameters.get("query_fetch_concurrency")
            or DEFAULT_QUERY_FETCH_CONCURRENCY,
            "api_requests_per_minute": (
                cleaned.get("api_requests_per_minute")
                if cleaned.get("api_requests_per_minute") is not None
                else (
                    existing_parameters.get("api_requests_per_minute")
                    if existing_parameters.get("api_requests_per_minute")
                    not in ("", None)
                    else self._default_api_requests_per_minute(source_type)
                )
            ),
            "nqe_fetch_all_max_pages": cleaned.get("nqe_fetch_all_max_pages")
            or existing_parameters.get("nqe_fetch_all_max_pages")
            or DEFAULT_NQE_FETCH_ALL_MAX_PAGES,
            "nqe_identical_full_page_streak_limit": cleaned.get(
                "nqe_identical_full_page_streak_limit"
            )
            or existing_parameters.get("nqe_identical_full_page_streak_limit")
            or DEFAULT_NQE_IDENTICAL_FULL_PAGE_STREAK_LIMIT,
            "query_preflight_enabled": bool(
                cleaned.get("query_preflight_enabled", DEFAULT_QUERY_PREFLIGHT_ENABLED)
            ),
            "query_preflight_row_limit": cleaned.get("query_preflight_row_limit")
            or existing_parameters.get("query_preflight_row_limit")
            or DEFAULT_PREFLIGHT_ROW_LIMIT,
            "query_diagnostics_enabled": bool(
                cleaned.get(
                    "query_diagnostics_enabled", DEFAULT_QUERY_DIAGNOSTICS_ENABLED
                )
            ),
            "nqe_async_poll_interval_seconds": (
                cleaned.get("nqe_async_poll_interval_seconds")
                if cleaned.get("nqe_async_poll_interval_seconds") is not None
                else (
                    existing_parameters.get("nqe_async_poll_interval_seconds")
                    if existing_parameters.get("nqe_async_poll_interval_seconds")
                    not in ("", None)
                    else DEFAULT_NQE_ASYNC_POLL_INTERVAL_SECONDS
                )
            ),
            "nqe_async_max_polls": cleaned.get("nqe_async_max_polls")
            or existing_parameters.get("nqe_async_max_polls")
            or DEFAULT_NQE_ASYNC_MAX_POLLS,
            "pushdown_fallback_warn_rate": (
                cleaned.get("pushdown_fallback_warn_rate")
                if cleaned.get("pushdown_fallback_warn_rate") is not None
                else (
                    existing_parameters.get("pushdown_fallback_warn_rate")
                    if existing_parameters.get("pushdown_fallback_warn_rate")
                    not in ("", None)
                    else DEFAULT_PUSHDOWN_FALLBACK_WARN_RATE
                )
            ),
            "pushdown_runtime_fallback_warn_share": (
                cleaned.get("pushdown_runtime_fallback_warn_share")
                if cleaned.get("pushdown_runtime_fallback_warn_share") is not None
                else (
                    existing_parameters.get("pushdown_runtime_fallback_warn_share")
                    if existing_parameters.get("pushdown_runtime_fallback_warn_share")
                    not in ("", None)
                    else DEFAULT_PUSHDOWN_RUNTIME_FALLBACK_WARN_SHARE
                )
            ),
            "pushdown_diff_warn_ratio": (
                cleaned.get("pushdown_diff_warn_ratio")
                if cleaned.get("pushdown_diff_warn_ratio") is not None
                else (
                    existing_parameters.get("pushdown_diff_warn_ratio")
                    if existing_parameters.get("pushdown_diff_warn_ratio")
                    not in ("", None)
                    else DEFAULT_PUSHDOWN_DIFF_WARN_RATIO
                )
            ),
            "network_id": selected_network_id,
            "device_tag_include_tags": include_tags,
            "device_tag_exclude_tags": exclude_tags,
            "device_tag_include_match": (
                cleaned.get("device_tag_include_match") or "any"
            ),
            "device_tag_include": include_tags[0] if len(include_tags) == 1 else "",
            "device_tag_exclude": exclude_tags[0] if len(exclude_tags) == 1 else "",
            "device_tag_filter_mode": (
                cleaned.get("device_tag_filter_mode") or "local"
            ),
            "device_tag_prune_out_of_scope": bool(
                cleaned.get("device_tag_prune_out_of_scope")
            ),
            "apply_device_scope_tags": bool(cleaned.get("apply_device_scope_tags")),
            "sync_device_tags": sync_device_tags,
            "sync_endpoints": bool(cleaned.get("sync_endpoints")),
            "scope_endpoints_by_include_tags": bool(
                cleaned.get("scope_endpoints_by_include_tags")
            ),
        }
        self.instance.type = source_type
        self.instance.url = (
            "https://fwd.app"
            if source_type == ForwardSourceDeploymentChoices.SAAS
            else (cleaned.get("url") or "").rstrip("/")
        )
        self.instance.parameters = candidate_parameters
        super().clean()
        candidate_source = ForwardSource(
            type=source_type,
            url=cleaned.get("url") or "",
            parameters=candidate_parameters,
        )
        try:
            candidate_source.validate_connection()
        except ForwardSyncError as error:
            message = str(error)
            if isinstance(error, ForwardConnectivityError):
                message = (
                    "Could not connect to Forward. Verify the Forward URL and "
                    "network connectivity from NetBox to Forward."
                )
            if "Forward API request failed with HTTP" in message:
                message = (
                    "Could not authenticate to Forward. Verify username and password. "
                    "For new Forward accounts, set the account password in the Forward "
                    "web UI before using NetBox."
                )
            raise forms.ValidationError(message)

        if not selected_network_id:
            raise forms.ValidationError("Select a Forward network for this source.")
        self.fields["network_id"].choices = _selected_choice(selected_network_id)
        self.fields["device_tag_include_tags"].choices = [
            (tag, tag) for tag in include_tags
        ]
        self.fields["device_tag_exclude_tags"].choices = [
            (tag, tag) for tag in exclude_tags
        ]
        self.fields["sync_device_tags"].choices = [
            (tag, tag) for tag in sync_device_tags
        ]
        return cleaned

    def save(self, *args, **kwargs):
        existing_parameters = self.instance.parameters or {}
        source_type = (
            self.cleaned_data.get("type") or ForwardSourceDeploymentChoices.SAAS
        )
        self.instance.type = source_type
        self.instance.url = (
            "https://fwd.app"
            if source_type == ForwardSourceDeploymentChoices.SAAS
            else (self.cleaned_data.get("url") or "").rstrip("/")
        )
        include_tags = self._normalize_tag_values(
            self.cleaned_data.get("device_tag_include_tags")
        )
        exclude_tags = self._normalize_tag_values(
            self.cleaned_data.get("device_tag_exclude_tags")
        )
        if not include_tags:
            include_tags = self._normalize_tag_values(
                self._bound_tag_values("device_tag_include_tags")
            )
        if not exclude_tags:
            exclude_tags = self._normalize_tag_values(
                self._bound_tag_values("device_tag_exclude_tags")
            )
        sync_device_tags = self._normalize_tag_values(
            self.cleaned_data.get("sync_device_tags")
        )
        if not sync_device_tags:
            sync_device_tags = self._normalize_tag_values(
                self._bound_tag_values("sync_device_tags")
            )
        self.instance.parameters = {
            "username": self.cleaned_data.get("username")
            or existing_parameters.get("username")
            or "",
            "password": self.cleaned_data.get("password")
            or existing_parameters.get("password")
            or "",
            "verify": (
                True
                if source_type == ForwardSourceDeploymentChoices.SAAS
                else self.cleaned_data.get("verify", True)
            ),
            "timeout": self.cleaned_data.get("timeout")
            or existing_parameters.get("timeout")
            or DEFAULT_FORWARD_API_TIMEOUT_SECONDS,
            "nqe_page_size": self.cleaned_data.get("nqe_page_size")
            or existing_parameters.get("nqe_page_size")
            or DEFAULT_NQE_PAGE_SIZE,
            "query_fetch_concurrency": self.cleaned_data.get("query_fetch_concurrency")
            or existing_parameters.get("query_fetch_concurrency")
            or DEFAULT_QUERY_FETCH_CONCURRENCY,
            "api_requests_per_minute": (
                self.cleaned_data.get("api_requests_per_minute")
                if self.cleaned_data.get("api_requests_per_minute") is not None
                else (
                    existing_parameters.get("api_requests_per_minute")
                    if existing_parameters.get("api_requests_per_minute")
                    not in ("", None)
                    else self._default_api_requests_per_minute(source_type)
                )
            ),
            "nqe_fetch_all_max_pages": self.cleaned_data.get("nqe_fetch_all_max_pages")
            or existing_parameters.get("nqe_fetch_all_max_pages")
            or DEFAULT_NQE_FETCH_ALL_MAX_PAGES,
            "nqe_identical_full_page_streak_limit": self.cleaned_data.get(
                "nqe_identical_full_page_streak_limit"
            )
            or existing_parameters.get("nqe_identical_full_page_streak_limit")
            or DEFAULT_NQE_IDENTICAL_FULL_PAGE_STREAK_LIMIT,
            "query_preflight_enabled": bool(
                self.cleaned_data.get(
                    "query_preflight_enabled", DEFAULT_QUERY_PREFLIGHT_ENABLED
                )
            ),
            "query_preflight_row_limit": self.cleaned_data.get(
                "query_preflight_row_limit"
            )
            or existing_parameters.get("query_preflight_row_limit")
            or DEFAULT_PREFLIGHT_ROW_LIMIT,
            "query_diagnostics_enabled": bool(
                self.cleaned_data.get(
                    "query_diagnostics_enabled", DEFAULT_QUERY_DIAGNOSTICS_ENABLED
                )
            ),
            "nqe_async_poll_interval_seconds": (
                self.cleaned_data.get("nqe_async_poll_interval_seconds")
                if self.cleaned_data.get("nqe_async_poll_interval_seconds") is not None
                else (
                    existing_parameters.get("nqe_async_poll_interval_seconds")
                    if existing_parameters.get("nqe_async_poll_interval_seconds")
                    not in ("", None)
                    else DEFAULT_NQE_ASYNC_POLL_INTERVAL_SECONDS
                )
            ),
            "nqe_async_max_polls": self.cleaned_data.get("nqe_async_max_polls")
            or existing_parameters.get("nqe_async_max_polls")
            or DEFAULT_NQE_ASYNC_MAX_POLLS,
            "pushdown_fallback_warn_rate": (
                self.cleaned_data.get("pushdown_fallback_warn_rate")
                if self.cleaned_data.get("pushdown_fallback_warn_rate") is not None
                else (
                    existing_parameters.get("pushdown_fallback_warn_rate")
                    if existing_parameters.get("pushdown_fallback_warn_rate")
                    not in ("", None)
                    else DEFAULT_PUSHDOWN_FALLBACK_WARN_RATE
                )
            ),
            "pushdown_runtime_fallback_warn_share": (
                self.cleaned_data.get("pushdown_runtime_fallback_warn_share")
                if self.cleaned_data.get("pushdown_runtime_fallback_warn_share")
                is not None
                else (
                    existing_parameters.get("pushdown_runtime_fallback_warn_share")
                    if existing_parameters.get("pushdown_runtime_fallback_warn_share")
                    not in ("", None)
                    else DEFAULT_PUSHDOWN_RUNTIME_FALLBACK_WARN_SHARE
                )
            ),
            "pushdown_diff_warn_ratio": (
                self.cleaned_data.get("pushdown_diff_warn_ratio")
                if self.cleaned_data.get("pushdown_diff_warn_ratio") is not None
                else (
                    existing_parameters.get("pushdown_diff_warn_ratio")
                    if existing_parameters.get("pushdown_diff_warn_ratio")
                    not in ("", None)
                    else DEFAULT_PUSHDOWN_DIFF_WARN_RATIO
                )
            ),
            "network_id": self.cleaned_data.get("network_id") or "",
            "device_tag_include_tags": include_tags,
            "device_tag_exclude_tags": exclude_tags,
            "device_tag_include_match": (
                self.cleaned_data.get("device_tag_include_match") or "any"
            ),
            "device_tag_include": include_tags[0] if len(include_tags) == 1 else "",
            "device_tag_exclude": exclude_tags[0] if len(exclude_tags) == 1 else "",
            "device_tag_filter_mode": (
                self.cleaned_data.get("device_tag_filter_mode") or "local"
            ),
            "device_tag_prune_out_of_scope": bool(
                self.cleaned_data.get("device_tag_prune_out_of_scope")
            ),
            "apply_device_scope_tags": bool(
                self.cleaned_data.get("apply_device_scope_tags")
            ),
            "sync_device_tags": sync_device_tags,
            "sync_endpoints": bool(self.cleaned_data.get("sync_endpoints")),
            "scope_endpoints_by_include_tags": bool(
                self.cleaned_data.get("scope_endpoints_by_include_tags")
            ),
        }
        self.instance.status = ForwardSourceStatusChoices.NEW
        return super().save(*args, **kwargs)


class ForwardSourceBulkEditForm(NetBoxModelBulkEditForm):
    comments = CommentField()
    type = forms.ChoiceField(
        choices=add_blank_choice(ForwardSourceDeploymentChoices),
        required=False,
        initial="",
    )

    model = ForwardSource
    fields = (
        "type",
        "url",
        "description",
        "comments",
    )


class ForwardIngestionMergeForm(ConfirmationForm):
    remove_branch = forms.BooleanField(
        initial=True,
        required=False,
        label="Remove branch",
        help_text="Leave unchecked to keep the branch for inspection or rollback.",
    )


class ForwardValidationRunForceAllowForm(ConfirmationForm):
    reason = forms.CharField(
        label="Reason",
        widget=forms.Textarea(attrs={"rows": 4}),
        help_text="Record why the blocked validation run is being accepted.",
    )


class ForwardSyncForm(NetBoxModelForm):
    source = forms.ModelChoiceField(
        queryset=ForwardSource.objects.all(),
        required=True,
        label="Forward Source",
        widget=HTMXSelect(),
    )
    auto_merge = forms.BooleanField(
        required=False,
        label="Auto merge",
        initial=True,
        help_text=(
            "Automatically merge each native Branching shard and continue to the next shard. "
            "Leave unchecked to pause for review after each shard."
        ),
    )
    enable_bulk_orm = forms.BooleanField(
        required=False,
        label="Use safe bulk ORM models",
        help_text=(
            "Use the parity-tested bulk ORM apply engine for eligible low-risk "
            "models. Models with dependency, relationship, IPAM hierarchy, or "
            "plugin-specific contracts remain on the adapter path."
        ),
    )
    skip_unchanged_snapshot = forms.BooleanField(
        required=False,
        label="Skip scheduled runs on an unchanged snapshot",
        help_text=(
            "When a scheduled run would target the same snapshot as the last "
            "successful baseline, skip query execution entirely (no-op) to reduce "
            "Forward API load. Manual/adhoc runs always execute. Off by default."
        ),
    )
    set_primary_ip_from_mgmt_tag = forms.BooleanField(
        required=False,
        label="Set primary IP from Mgmt_ tag",
        help_text=(
            "When a Forward device carries a `Mgmt_<interface>` tag (e.g. "
            "`Mgmt_Vl211`), set the device's primary IP to the address on that "
            "interface (e.g. Vlan211). Off by default."
        ),
    )
    diff_fallback_mode = forms.ChoiceField(
        choices=tuple(
            (value, label)
            for value, label, _color in ForwardDiffFallbackModeChoices.CHOICES
        ),
        required=False,
        label="Diff fallback mode",
        help_text=(
            "Allow full-query fallback when diff execution cannot run, or require "
            "diff-only execution once a baseline exists."
        ),
    )
    snapshot_id = forms.ChoiceField(
        required=False,
        label="Snapshot",
        choices=(),
        widget=APISelect(api_url="/api/plugins/forward/sync/available-snapshots/"),
        help_text="Choose a specific snapshot or leave the default `latestProcessed` selection.",
    )
    scheduled = forms.DateTimeField(
        required=False,
        widget=DateTimePicker(),
        label="Schedule at",
    )
    interval = forms.IntegerField(
        required=False,
        min_value=1,
        label="Recurs every",
        widget=NumberWithOptions(options=JobIntervalChoices),
    )
    drift_policy = forms.ModelChoiceField(
        queryset=ForwardDriftPolicy.objects.all(),
        required=False,
        label="Drift policy",
        help_text="Optional validation policy applied before branch creation.",
    )
    webhook_secret = forms.CharField(
        required=False,
        label="Webhook secret",
        widget=forms.PasswordInput(render_value=True),
        help_text=(
            "Shared secret for the push-triggered sync endpoint "
            "(POST /api/plugins/forward/sync/<id>/webhook/ with the "
            "X-Forward-Webhook-Secret header, or ?secret= for senders that "
            "cannot set headers). Leave empty to disable the webhook endpoint. "
            "Use a long random value; an already-running sync is not re-queued."
        ),
    )
    validation_schedule_interval = forms.IntegerField(
        required=False,
        min_value=1,
        label="Recurring validation (minutes)",
        help_text=(
            "Run query validation on a standing schedule every N minutes. "
            "Blank disables. The first run starts on save; the schedule "
            "self-heals after worker restarts."
        ),
    )
    preview_schedule_interval = forms.IntegerField(
        required=False,
        min_value=60,
        label="Recurring dependency preview (minutes)",
        help_text=(
            "Refresh the dependency preview on a standing schedule every N "
            "minutes (minimum 60 — it is a full live dry-run; daily or less "
            "is recommended on large fabrics). Blank disables."
        ),
    )

    class Meta:
        model = ForwardSync
        fields = ("name", "source", "drift_policy", "tags", "scheduled", "interval")

    def __init__(self, *args, **kwargs):
        initial = kwargs.get("initial", {}).copy()
        for name, value in initial.items():
            if (
                name in self.base_fields
                and isinstance(self.base_fields[name], forms.BooleanField)
                and isinstance(value, list)
                and len(value) > 1
            ):
                initial[name] = value[-1]
        kwargs["initial"] = initial
        super().__init__(*args, **kwargs)
        parameters = self.instance.parameters or {}
        self.fields["auto_merge"].initial = parameters.get("auto_merge", True)
        self.fields["enable_bulk_orm"].initial = parameters.get(
            "enable_bulk_orm",
            DEFAULT_ENABLE_BULK_ORM_FOR_NEW_SYNCS,
        )
        self.fields["skip_unchanged_snapshot"].initial = parameters.get(
            "skip_unchanged_snapshot",
            False,
        )
        self.fields["set_primary_ip_from_mgmt_tag"].initial = parameters.get(
            "set_primary_ip_from_mgmt_tag",
            False,
        )
        self.fields["diff_fallback_mode"].initial = parameters.get(
            "diff_fallback_mode",
            ForwardDiffFallbackModeChoices.ALLOW_FALLBACK,
        )
        self.fields["webhook_secret"].initial = parameters.get("webhook_secret", "")
        self.fields["validation_schedule_interval"].initial = (
            parameters.get("validation_schedule_interval") or None
        )
        self.fields["preview_schedule_interval"].initial = (
            parameters.get("preview_schedule_interval") or None
        )
        selected_snapshot_id = (
            self.data.get("snapshot_id")
            if self.is_bound
            else (parameters.get("snapshot_id") or LATEST_PROCESSED_SNAPSHOT)
        )
        _configure_api_select(
            self.fields["snapshot_id"].widget, {"source_id": "$source"}
        )
        self.fields["snapshot_id"].initial = selected_snapshot_id
        self.fields["snapshot_id"].choices = _snapshot_selected_choice(
            selected_snapshot_id
        )
        configured_models = forward_configured_models()
        for model_string in configured_models:
            self.fields[model_string] = forms.BooleanField(
                required=False,
                initial=parameters.get(
                    model_string,
                    model_string not in FORWARD_OPTIONAL_MODELS,
                ),
                label=model_string,
            )

        self.fieldsets = [
            FieldSet("name", "source", "drift_policy", name="Forward Sync"),
            FieldSet("snapshot_id", name="Snapshot"),
            FieldSet(*configured_models, name="Model Selection"),
            FieldSet(
                "auto_merge",
                "enable_bulk_orm",
                "skip_unchanged_snapshot",
                "set_primary_ip_from_mgmt_tag",
                "diff_fallback_mode",
                "scheduled",
                "interval",
                "webhook_secret",
                name="Execution",
            ),
            FieldSet(
                "validation_schedule_interval",
                "preview_schedule_interval",
                name="Standing Schedules",
            ),
            FieldSet("tags", name="Tags"),
        ]

    def clean(self):
        super().clean()
        cleaned = dict(self.cleaned_data)
        if cleaned.get("scheduled") and cleaned["scheduled"] < local_now():
            raise forms.ValidationError("Scheduled time must be in the future.")
        if cleaned.get("interval") and not cleaned.get("scheduled"):
            cleaned["scheduled"] = local_now()
        source = cleaned.get("source")
        network_id = (source.parameters or {}).get("network_id") if source else None
        if not network_id:
            raise forms.ValidationError(
                "Set a network on the source before creating the sync."
            )
        snapshot_id = cleaned.get("snapshot_id") or LATEST_PROCESSED_SNAPSHOT
        self.fields["snapshot_id"].choices = _snapshot_selected_choice(snapshot_id)
        dynamic_snapshot_selectors = {
            LATEST_PROCESSED_SNAPSHOT,
            LATEST_COLLECTED_SNAPSHOT,
        }
        if source and snapshot_id not in dynamic_snapshot_selectors:
            snapshot_ids = {
                snapshot["id"]
                for snapshot in source.get_client().get_snapshots(network_id)
            }
            if snapshot_id not in snapshot_ids:
                raise forms.ValidationError(
                    "Selected snapshot is not available for the source network."
                )
        if not any(
            cleaned.get(model_string, False)
            for model_string in forward_configured_models()
        ):
            raise forms.ValidationError("Select at least one NetBox model to sync.")
        parameters = {
            "execution_backend": ForwardExecutionBackendChoices.SINGLE_BRANCH,
            "auto_merge": cleaned.get("auto_merge", False),
            "snapshot_id": snapshot_id,
            "enable_bulk_orm": bool(cleaned.get("enable_bulk_orm", False)),
            "bulk_orm_models": list(
                (self.instance.parameters or {}).get("bulk_orm_models") or []
            ),
            "skip_unchanged_snapshot": bool(
                cleaned.get("skip_unchanged_snapshot", False)
            ),
            "set_primary_ip_from_mgmt_tag": bool(
                cleaned.get("set_primary_ip_from_mgmt_tag", False)
            ),
            "diff_fallback_mode": cleaned.get("diff_fallback_mode")
            or ForwardDiffFallbackModeChoices.ALLOW_FALLBACK,
            "webhook_secret": str(cleaned.get("webhook_secret") or ""),
            "validation_schedule_interval": int(
                cleaned.get("validation_schedule_interval") or 0
            ),
            "preview_schedule_interval": int(
                cleaned.get("preview_schedule_interval") or 0
            ),
        }
        for model_string in forward_configured_models():
            parameters[model_string] = cleaned.get(model_string, False)
        self.instance.parameters = parameters
        self.instance.auto_merge = cleaned.get("auto_merge", False)
        return cleaned

    def save(self, *args, **kwargs):
        parameters = {
            "execution_backend": ForwardExecutionBackendChoices.SINGLE_BRANCH,
            "auto_merge": self.cleaned_data.get("auto_merge", False),
            "snapshot_id": self.cleaned_data.get("snapshot_id")
            or LATEST_PROCESSED_SNAPSHOT,
            "enable_bulk_orm": bool(self.cleaned_data.get("enable_bulk_orm", False)),
            "bulk_orm_models": list(
                (self.instance.parameters or {}).get("bulk_orm_models") or []
            ),
            "skip_unchanged_snapshot": bool(
                self.cleaned_data.get("skip_unchanged_snapshot", False)
            ),
            "set_primary_ip_from_mgmt_tag": bool(
                self.cleaned_data.get("set_primary_ip_from_mgmt_tag", False)
            ),
            "diff_fallback_mode": self.cleaned_data.get("diff_fallback_mode")
            or ForwardDiffFallbackModeChoices.ALLOW_FALLBACK,
            "webhook_secret": str(self.cleaned_data.get("webhook_secret") or ""),
            "validation_schedule_interval": int(
                self.cleaned_data.get("validation_schedule_interval") or 0
            ),
            "preview_schedule_interval": int(
                self.cleaned_data.get("preview_schedule_interval") or 0
            ),
        }
        for model_string in forward_configured_models():
            parameters[model_string] = self.cleaned_data.get(model_string, False)
        self.instance.parameters = parameters
        self.instance.auto_merge = self.cleaned_data.get("auto_merge", False)
        self.instance.status = ForwardSyncStatusChoices.NEW
        instance = super().save(*args, **kwargs)
        # Make the enqueued Job rows match the stored schedule intent
        # (creates, replaces, or cancels the standing schedules).
        from .utilities.sync_facade import reconcile_standing_schedules

        reconcile_standing_schedules(instance)
        return instance


class ForwardSyncBulkEditForm(NetBoxModelBulkEditForm):
    model = ForwardSync
    fields = ("scheduled", "interval")


class ForwardNQEMapForm(NetBoxModelForm):
    query_mode = forms.ChoiceField(
        choices=FORWARD_NQE_QUERY_MODE_CHOICES,
        required=False,
        initial="query_path",
        label="Query Definition Mode",
        help_text="Choose whether this map resolves a repository query path, runs a direct query ID, or stores raw NQE text in NetBox.",
    )
    query_source = forms.ModelChoiceField(
        queryset=ForwardSource.objects.all(),
        required=False,
        label="Forward Source for Query Lookup",
        help_text=(
            "Used only to populate Forward query selectors. Query choices are "
            "filtered by the selected NetBox model."
        ),
    )
    query_repository = forms.ChoiceField(
        choices=FORWARD_NQE_QUERY_REPOSITORY_CHOICES,
        required=False,
        initial="org",
        label="Query Repository",
        help_text="Select Org Repository for custom queries or Forward Library for built-in Forward queries.",
    )
    query_folder = forms.ChoiceField(
        required=False,
        label="Query Folder",
        choices=(),
        widget=APISelect(
            api_url="/api/plugins/forward/nqe-map/available-query-folders/"
        ),
        help_text="Optional folder filter for the query selector.",
    )
    query_id = forms.ChoiceField(
        required=False,
        label="Direct Query ID",
        choices=(),
        widget=APISelect(api_url="/api/plugins/forward/nqe-map/available-queries/"),
        help_text=(
            "Org-specific published Forward query ID. Query choices are "
            "filtered by the selected NetBox model. Prefer `Repository Query Path` "
            "for portable maps."
        ),
    )
    query_path = forms.ChoiceField(
        required=False,
        label="Query Path",
        choices=(),
        widget=APISelect(api_url="/api/plugins/forward/nqe-map/available-queries/"),
        help_text=(
            "Repository path to resolve at sync time. Query choices are "
            "filtered by the selected NetBox model. Required when mode is "
            "`Repository Query Path`."
        ),
    )
    query = forms.CharField(
        required=False,
        label="Query",
        help_text="Use this for raw NQE text. Leave `Query ID` blank when `Query` is set.",
        widget=forms.Textarea(attrs={"class": "font-monospace", "rows": 10}),
    )
    commit_id = forms.ChoiceField(
        required=False,
        label="Commit ID",
        choices=(),
        widget=APISelect(
            api_url="/api/plugins/forward/nqe-map/available-query-commits/"
        ),
        help_text="Optional published query revision. Leave blank to use the latest committed revision.",
    )

    class Meta:
        model = ForwardNQEMap
        fields = (
            "name",
            "netbox_model",
            "query_id",
            "query_repository",
            "query_path",
            "query",
            "commit_id",
            "enabled",
            "weight",
        )

    fieldsets = (
        FieldSet("name", "netbox_model", name="NQE Map"),
        FieldSet(
            "query_mode",
            "query_source",
            "query_repository",
            "query_folder",
            "query_path",
            "query_id",
            "commit_id",
            "query",
            "enabled",
            "weight",
            name="Query Definition",
        ),
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        selected_query_mode = (
            self.data.get("query_mode")
            if self.is_bound
            else (
                "query_path"
                if getattr(self.instance, "query_path", "")
                else (
                    "query_id"
                    if getattr(self.instance, "query_id", "")
                    else (
                        "query" if getattr(self.instance, "query", "") else "query_path"
                    )
                )
            )
        )
        selected_repository = (
            self.data.get("query_repository")
            if self.is_bound
            else (
                getattr(self.instance, "query_repository", "")
                or (
                    "fwd"
                    if str(getattr(self.instance, "query_id", "")).startswith("FQ_")
                    else "org"
                )
            )
        )
        selected_folder = self.data.get("query_folder") if self.is_bound else "/"
        selected_query_path = (
            self.data.get("query_path")
            if self.is_bound
            else getattr(self.instance, "query_path", "")
        )
        selected_query_id = (
            self.data.get("query_id")
            if self.is_bound
            else getattr(self.instance, "query_id", "")
        )
        selected_commit_id = (
            self.data.get("commit_id")
            if self.is_bound
            else getattr(self.instance, "commit_id", "")
        )
        self.fields["query_mode"].initial = selected_query_mode
        self.fields["query_repository"].initial = selected_repository
        self.fields["query_folder"].choices = _selected_choice(selected_folder)
        self.fields["query_path"].choices = _selected_choice(selected_query_path)
        self.fields["query_id"].choices = _selected_choice(selected_query_id)
        self.fields["commit_id"].choices = _selected_choice(selected_commit_id)
        _configure_api_select(
            self.fields["query_folder"].widget,
            {
                "source_id": "$query_source",
                "repository": "$query_repository",
            },
        )
        _configure_api_select(
            self.fields["query_path"].widget,
            {
                "source_id": "$query_source",
                "repository": "$query_repository",
                "directory": "$query_folder",
                "value_mode": "path",
                "model_string": "$netbox_model",
            },
        )
        _configure_api_select(
            self.fields["query_id"].widget,
            {
                "source_id": "$query_source",
                "repository": "$query_repository",
                "directory": "$query_folder",
                "value_mode": "query_id",
                "model_string": "$netbox_model",
            },
        )
        _configure_api_select(
            self.fields["commit_id"].widget,
            {
                "source_id": "$query_source",
                "repository": "$query_repository",
                "query_path": "$query_path",
                "query_id": "$query_id",
            },
        )
        if not self.is_bound:
            first_source = ForwardSource.objects.order_by("pk").first()
            if first_source is not None:
                self.fields["query_source"].initial = first_source.pk

    def clean(self):
        cleaned = super().clean() or self.cleaned_data
        query_id = (cleaned.get("query_id") or "").strip()
        query_path = (cleaned.get("query_path") or "").strip()
        query_repository = (cleaned.get("query_repository") or "").strip()
        query = (cleaned.get("query") or "").strip()
        query_mode = cleaned.get("query_mode") or (
            "query_path" if query_path else ("query_id" if query_id else "query")
        )
        if query_mode == "query_path":
            if not query_path:
                self.add_error("query_path", "Select a Forward query path.")
            if not query_repository:
                cleaned["query_repository"] = "org"
            cleaned["query_id"] = ""
            cleaned["query"] = ""
        elif query_mode == "query_id":
            if not query_id:
                self.add_error("query_id", "Select a published Forward query.")
            cleaned["query_path"] = ""
            cleaned["query"] = ""
        elif query_mode == "query":
            if not query:
                self.add_error("query", "Enter raw NQE query text.")
            cleaned["query_id"] = ""
            cleaned["query_repository"] = ""
            cleaned["query_path"] = ""
            cleaned["commit_id"] = ""
        return cleaned


class ForwardNQEMapBulkEditForm(NetBoxModelBulkEditForm):
    query_bulk_operation = forms.ChoiceField(
        choices=FORWARD_NQE_BULK_QUERY_OPERATION_CHOICES,
        required=False,
        label="Query Bulk Operation",
        help_text=(
            "Repository-path operations clear direct query IDs and save the "
            "selected repository paths so each sync resolves the current query. "
            "Enable commit pinning only when a fixed revision is required."
        ),
    )
    bind_query_source = forms.ModelChoiceField(
        queryset=ForwardSource.objects.all(),
        required=False,
        label="Forward Source for Query Lookup",
        help_text=(
            "Choose the Forward source used to read the repository folder and "
            "validate the repository paths selected for these maps."
        ),
    )
    bind_query_repository = forms.ChoiceField(
        choices=FORWARD_NQE_QUERY_REPOSITORY_CHOICES,
        required=False,
        initial="org",
        label="Query Repository",
        help_text="Repository containing the committed Forward NetBox query folder.",
    )
    bind_query_folder = forms.ChoiceField(
        required=False,
        label="Repository Folder",
        choices=(),
        widget=APISelect(
            api_url="/api/plugins/forward/nqe-map/available-query-folders/"
        ),
        help_text=(
            "Folder containing the committed query set. For customer-published "
            "Forward NetBox queries, select `/forward_netbox_validation`; the "
            "selectors below bind paths from that folder, not static query IDs."
        ),
    )
    bind_pin_commit = forms.BooleanField(
        required=False,
        label="Pin current commit",
        help_text="Store the current query commit ID instead of resolving latest at sync time.",
    )
    publish_overwrite = forms.BooleanField(
        required=False,
        label="Overwrite existing repository queries",
        help_text="Update existing Org Repository query files before committing. Leave disabled to publish only missing files and still bind selected maps to existing paths.",
    )
    publish_commit_message = forms.CharField(
        required=False,
        label="Commit message",
        initial="Publish Forward NetBox NQE maps",
        help_text="Commit title for Forward Org Repository writes. Requires Forward Network Operator or equivalent NQE-library write permission on the selected source.",
    )
    enabled = forms.NullBooleanField(required=False, label="Enabled")
    model = ForwardNQEMap
    fields = (
        "query_bulk_operation",
        "bind_query_source",
        "bind_query_repository",
        "bind_query_folder",
        "bind_pin_commit",
        "publish_overwrite",
        "publish_commit_message",
        "enabled",
    )

    fieldsets = (
        FieldSet(
            "query_bulk_operation",
            "bind_query_source",
            "bind_query_repository",
            "bind_query_folder",
            "bind_pin_commit",
            "publish_overwrite",
            "publish_commit_message",
            name="Bulk Query Reference",
        ),
        FieldSet(name="Map Query Path Choices"),
        FieldSet("enabled", name="Map State"),
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.selected_query_path_fields = []
        selected_folder = self.data.get("bind_query_folder") if self.is_bound else "/"
        self.fields["bind_query_folder"].choices = _selected_choice(selected_folder)
        _configure_api_select(
            self.fields["bind_query_folder"].widget,
            {
                "source_id": "$bind_query_source",
                "repository": "$bind_query_repository",
            },
        )
        selected_maps = self._selected_maps()
        for query_map in selected_maps:
            field_name = self.query_path_field_name(query_map.pk)
            existing_query_path = getattr(query_map, "query_path", "")
            selected_query_path = (
                self.data.get(field_name) if self.is_bound else existing_query_path
            )
            self.fields[field_name] = forms.ChoiceField(
                required=False,
                label=f"{query_map.name} ({query_map.model_string})",
                choices=_selected_choice(selected_query_path),
                initial=existing_query_path,
                widget=APISelect(
                    api_url="/api/plugins/forward/nqe-map/available-queries/"
                ),
                help_text=(
                    "Select the repository query path for this NetBox map. Saving "
                    "this field stores the path so the current Forward query ID is "
                    "resolved at sync time. Leave blank to keep this map unchanged."
                ),
            )
            _configure_api_select(
                self.fields[field_name].widget,
                {
                    "source_id": "$bind_query_source",
                    "repository": "$bind_query_repository",
                    "directory": "$bind_query_folder",
                    "value_mode": "path",
                    "model_string": query_map.model_string,
                },
            )
            self.selected_query_path_fields.append(field_name)

        self.fieldsets = (
            FieldSet(
                "query_bulk_operation",
                "bind_query_source",
                "bind_query_repository",
                "bind_query_folder",
                "bind_pin_commit",
                "publish_overwrite",
                "publish_commit_message",
                name="Bulk Query Reference",
            ),
            FieldSet(
                *self.selected_query_path_fields,
                name="Map Query Path Choices",
            ),
            FieldSet("enabled", name="Map State"),
        )

    @staticmethod
    def query_path_field_name(map_id):
        return f"bind_query_path_{map_id}"

    def _selected_maps(self):
        pk_values = []
        if self.is_bound:
            if hasattr(self.data, "getlist"):
                pk_values = self.data.getlist("pk")
            else:
                raw_pk_values = self.data.get("pk", [])
                pk_values = (
                    raw_pk_values
                    if isinstance(raw_pk_values, (list, tuple))
                    else [raw_pk_values]
                )
        else:
            initial_pk = self.initial.get("pk", [])
            pk_values = [getattr(value, "pk", value) for value in initial_pk]
        pk_values = [pk for pk in pk_values if str(pk).isdigit()]
        if not pk_values:
            return ForwardNQEMap.objects.none()
        return ForwardNQEMap.objects.filter(pk__in=pk_values).select_related(
            "netbox_model"
        )

    def selected_query_paths_by_map_id(self):
        selected = {}
        for field_name in self.selected_query_path_fields:
            query_path = (self.cleaned_data.get(field_name) or "").strip()
            if not query_path:
                continue
            map_id = int(field_name.rsplit("_", 1)[-1])
            selected[map_id] = query_path
        return selected

    def get_query_bulk_operation(self):
        return self.cleaned_data.get("query_bulk_operation") or ""

    def has_query_binding_request(self):
        return self.get_query_bulk_operation() == "bind_query_path"

    def has_query_publish_request(self):
        return self.get_query_bulk_operation() == "publish_bundled_query_path"

    def has_query_restore_request(self):
        return self.get_query_bulk_operation() == "restore_raw_query"

    def clean(self):
        cleaned = super().clean() or self.cleaned_data
        query_bulk_operation = cleaned.get("query_bulk_operation") or ""
        bind_source = cleaned.get("bind_query_source")
        bind_folder = (cleaned.get("bind_query_folder") or "").strip()
        selected_query_paths = [
            (cleaned.get(field_name) or "").strip()
            for field_name in self.selected_query_path_fields
        ]
        has_query_path_selection = any(selected_query_paths)
        if not query_bulk_operation:
            return cleaned
        if query_bulk_operation == "restore_raw_query":
            return cleaned
        if query_bulk_operation == "publish_bundled_query_path":
            if not bind_source:
                self.add_error(
                    "bind_query_source",
                    "Select a Forward source for Org Repository publishing.",
                )
            if not bind_folder:
                self.add_error(
                    "bind_query_folder",
                    "Select the Org Repository folder to publish into.",
                )
            if cleaned.get("bind_query_repository") not in ("", "org"):
                self.add_error(
                    "bind_query_repository",
                    "Bundled query publishing writes only to the Forward Org Repository.",
                )
            if bind_source and not cleaned.get("bind_query_repository"):
                cleaned["bind_query_repository"] = "org"
            if not (cleaned.get("publish_commit_message") or "").strip():
                cleaned["publish_commit_message"] = "Publish Forward NetBox NQE maps"
            return cleaned
        if query_bulk_operation != "bind_query_path":
            self.add_error(
                "query_bulk_operation", "Select a valid query bulk operation."
            )
            return cleaned
        if bind_source and not bind_folder:
            self.add_error(
                "bind_query_folder",
                "Select a repository folder to bind selected maps.",
            )
        if (bind_folder or has_query_path_selection) and not bind_source:
            self.add_error(
                "bind_query_source",
                "Select a Forward source for query path binding.",
            )
        if has_query_path_selection and not bind_folder:
            self.add_error(
                "bind_query_folder",
                "Select the repository folder for the selected query paths.",
            )
        if bind_source and bind_folder and not has_query_path_selection:
            self.add_error(
                None,
                "Select at least one per-map repository query path to bind.",
            )
        if bind_source and not cleaned.get("bind_query_repository"):
            cleaned["bind_query_repository"] = "org"
        return cleaned


class ForwardDriftPolicyForm(NetBoxModelForm):
    class Meta:
        model = ForwardDriftPolicy
        fields = (
            "name",
            "enabled",
            "baseline_mode",
            "require_processed_snapshot",
            "block_on_query_errors",
            "block_on_zero_rows",
            "max_deleted_objects",
            "max_deleted_percent",
        )

    fieldsets = (
        FieldSet("name", "enabled", "baseline_mode", name="Policy"),
        FieldSet(
            "require_processed_snapshot",
            "block_on_query_errors",
            "block_on_zero_rows",
            name="Blocking Checks",
        ),
        FieldSet(
            "max_deleted_objects",
            "max_deleted_percent",
            name="Destructive Change Limits",
        ),
    )


class ForwardDriftPolicyBulkEditForm(NetBoxModelBulkEditForm):
    enabled = forms.NullBooleanField(required=False, label="Enabled")
    model = ForwardDriftPolicy
    fields = ("enabled",)
