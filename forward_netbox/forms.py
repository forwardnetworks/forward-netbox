from core.choices import JobIntervalChoices
from django import forms
from netbox.forms import NetBoxModelBulkEditForm
from netbox.forms import NetBoxModelForm
from utilities.datetime import local_now
from utilities.forms import add_blank_choice
from utilities.forms import ConfirmationForm
from utilities.forms import get_field_value
from utilities.forms.fields import CommentField
from utilities.forms.rendering import FieldSet
from utilities.forms.widgets import APISelect
from utilities.forms.widgets import DateTimePicker
from utilities.forms.widgets import HTMXSelect
from utilities.forms.widgets import NumberWithOptions

from .choices import forward_configured_models
from .choices import FORWARD_OPTIONAL_MODELS
from .choices import ForwardSourceDeploymentChoices
from .choices import ForwardSourceStatusChoices
from .choices import ForwardSyncStatusChoices
from .exceptions import ForwardConnectivityError
from .exceptions import ForwardSyncError
from .models import ForwardDriftPolicy
from .models import ForwardNQEMap
from .models import ForwardSource
from .models import ForwardSync
from .utilities.branch_budget import DEFAULT_MAX_CHANGES_PER_BRANCH
from .utilities.forward_api import DEFAULT_FORWARD_API_TIMEOUT_SECONDS
from .utilities.forward_api import DEFAULT_NQE_PAGE_SIZE
from .utilities.forward_api import LATEST_PROCESSED_SNAPSHOT
from .utilities.forward_api import MAX_NQE_PAGE_SIZE


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
    elif selected_value:
        choices.append((selected_value, selected_value))
    return choices


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
        if self.instance.pk:
            self.fields["network_id"].widget.add_query_param(
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
        self.fields["verify"].initial = parameters.get("verify", True)
        self.fields["network_id"].initial = existing_network_id
        self.fields["network_id"].choices = _selected_choice(existing_network_id)

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
            "network_id": selected_network_id,
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
            "network_id": self.cleaned_data.get("network_id") or "",
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
    max_changes_per_branch = forms.IntegerField(
        required=False,
        min_value=1,
        label="Max changes per branch",
        help_text="Maximum planned changes per native Branching shard.",
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
        self.fields["max_changes_per_branch"].initial = parameters.get(
            "max_changes_per_branch",
            DEFAULT_MAX_CHANGES_PER_BRANCH,
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
                "max_changes_per_branch",
                "auto_merge",
                "scheduled",
                "interval",
                name="Execution",
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
        if source and snapshot_id != LATEST_PROCESSED_SNAPSHOT:
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
            "auto_merge": cleaned.get("auto_merge", False),
            "multi_branch": True,
            "max_changes_per_branch": cleaned.get("max_changes_per_branch")
            or DEFAULT_MAX_CHANGES_PER_BRANCH,
            "snapshot_id": snapshot_id,
        }
        for model_string in forward_configured_models():
            parameters[model_string] = cleaned.get(model_string, False)
        self.instance.parameters = parameters
        self.instance.auto_merge = cleaned.get("auto_merge", False)
        return cleaned

    def save(self, *args, **kwargs):
        parameters = {
            "auto_merge": self.cleaned_data.get("auto_merge", False),
            "multi_branch": True,
            "max_changes_per_branch": self.cleaned_data.get("max_changes_per_branch")
            or DEFAULT_MAX_CHANGES_PER_BRANCH,
            "snapshot_id": self.cleaned_data.get("snapshot_id")
            or LATEST_PROCESSED_SNAPSHOT,
        }
        for model_string in forward_configured_models():
            parameters[model_string] = self.cleaned_data.get(model_string, False)
        self.instance.parameters = parameters
        self.instance.auto_merge = self.cleaned_data.get("auto_merge", False)
        self.instance.status = ForwardSyncStatusChoices.NEW
        return super().save(*args, **kwargs)


class ForwardSyncBulkEditForm(NetBoxModelBulkEditForm):
    model = ForwardSync
    fields = ("scheduled", "interval")


class ForwardNQEMapForm(NetBoxModelForm):
    query_id = forms.CharField(
        required=False,
        label="Query ID",
        help_text="Use this for a published Forward query. Leave `Query` blank when `Query ID` is set.",
    )
    query = forms.CharField(
        required=False,
        label="Query",
        help_text="Use this for raw NQE text. Leave `Query ID` blank when `Query` is set.",
        widget=forms.Textarea(attrs={"class": "font-monospace", "rows": 10}),
    )
    commit_id = forms.CharField(
        required=False,
        label="Commit ID",
        help_text="Optional published query revision. Only applies when `Query ID` is used.",
    )

    class Meta:
        model = ForwardNQEMap
        fields = (
            "name",
            "netbox_model",
            "query_id",
            "query",
            "commit_id",
            "enabled",
            "weight",
        )

    fieldsets = (
        FieldSet("name", "netbox_model", name="NQE Map"),
        FieldSet(
            "query_id",
            "query",
            "commit_id",
            "enabled",
            "weight",
            name="Query Definition",
        ),
    )


class ForwardNQEMapBulkEditForm(NetBoxModelBulkEditForm):
    enabled = forms.NullBooleanField(required=False, label="Enabled")
    model = ForwardNQEMap
    fields = ("enabled",)


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
