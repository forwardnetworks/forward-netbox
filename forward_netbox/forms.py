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
from utilities.forms.widgets import DateTimePicker
from utilities.forms.widgets import HTMXSelect
from utilities.forms.widgets import NumberWithOptions

from .choices import forward_configured_models
from .choices import FORWARD_OPTIONAL_MODELS
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
    execution_backend = forms.ChoiceField(
        choices=tuple(
            (value, label)
            for value, label, _color in ForwardExecutionBackendChoices.CHOICES
        ),
        required=False,
        label="Execution backend",
        help_text=(
            "Use Branching for reviewable changes. Use Fast bootstrap for large "
            "initial ingests that should write directly after validation."
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
        self.fields["execution_backend"].initial = parameters.get(
            "execution_backend",
            ForwardExecutionBackendChoices.BRANCHING,
        )
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
                "execution_backend",
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
            "execution_backend": cleaned.get("execution_backend")
            or ForwardExecutionBackendChoices.BRANCHING,
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
            "execution_backend": self.cleaned_data.get("execution_backend")
            or ForwardExecutionBackendChoices.BRANCHING,
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
            "Bulk edit does not store direct query IDs. Choose a repository-path "
            "operation to set selected maps to Repository Query Path mode; the "
            "plugin resolves the current query ID from the selected source during "
            "sync and diff execution."
        ),
    )
    bind_query_source = forms.ModelChoiceField(
        queryset=ForwardSource.objects.all(),
        required=False,
        label="Forward Source for Query Lookup",
        help_text=(
            "Choose the Forward source used to read the repository folder. "
            "The source credentials are also used later to resolve each selected "
            "repository path into its current Forward query ID."
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
                    "this field sets execution to Repository Query Path; the query "
                    "ID is resolved from this path during sync and diff execution. "
                    "Leave blank to keep this map unchanged."
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
