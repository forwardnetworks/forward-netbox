import base64
import copy

from django import forms
from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ValidationError
from django.utils import timezone
from django.utils.translation import gettext_lazy as _

from core.choices import DataSourceStatusChoices, JobIntervalChoices
from netbox.forms import NetBoxModelFilterSetForm, NetBoxModelForm
from netbox.forms.mixins import SavedFiltersMixin
from utilities.datetime import local_now
from utilities.forms import FilterForm, get_field_value
from utilities.forms.fields import DynamicModelChoiceField, DynamicModelMultipleChoiceField
from utilities.forms.rendering import FieldSet
from utilities.forms.widgets import DateTimePicker, NumberWithOptions, HTMXSelect

from .choices import ForwardSnapshotStatusModelChoices
from .models import (
    ForwardSnapshot,
    ForwardSource,
    ForwardSync,
    ForwardNQEMap,
    ForwardSupportedSyncModels,
)

def get_sync_parameters():
    sync_parameters = {"dcim": {}, "ipam": {}}
    for ct in ContentType.objects.filter(app_label__in=["dcim", "ipam"]).order_by("app_label", "model"):
        qualified_key = f"{ct.app_label}.{ct.model}"
        label = ct.model.replace("_", " ").title()
        sync_parameters[ct.app_label][qualified_key] = forms.BooleanField(
            required=False,
            label=_(label),
            initial=True if ct.model in ["site", "manufacturer", "device"] else False
        )
    return sync_parameters


class ForwardNQEMapForm(NetBoxModelForm):
    class Meta:
        model = ForwardNQEMap
        fields = ("name", "query_id", "netbox_model")


class ForwardSnapshotFilterForm(NetBoxModelFilterSetForm):
    model = ForwardSnapshot
    status = forms.CharField(required=False, label=_("Status"))
    source_id = DynamicModelMultipleChoiceField(
        queryset=ForwardSource.objects.all(), required=False, label=_("Source")
    )
    snapshot_id = forms.CharField(required=False, label=_("Snapshot ID"))

    fieldsets = (
        FieldSet("q", "filter_id"),
        FieldSet("source_id", "status", "snapshot_id", name=_("Source")),
    )


class ForwardSourceFilterForm(NetBoxModelFilterSetForm):
    model = ForwardSource
    status = forms.MultipleChoiceField(choices=DataSourceStatusChoices, required=False)

    fieldsets = (
        FieldSet("q", "filter_id"),
        FieldSet("status", name=_("Source")),
    )


class ForwardSourceForm(NetBoxModelForm):
    class Meta:
        model = ForwardSource
        fields = [
            "name",
            "url",
            "network_id",
            "description",
        ]

    @property
    def fieldsets(self):
        return [
            FieldSet("name", "url", "network_id", name=_("Source")),
            FieldSet("username", "password", "verify", "timeout", name=_("Parameters")),
        ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.fields["timeout"] = forms.IntegerField(
            required=False,
            label=_("Timeout"),
            help_text=_("Timeout for the API request."),
            widget=forms.NumberInput(attrs={"class": "form-control"}),
        )

        self.fields["username"] = forms.CharField(
            required=True,
            label=_("Username"),
            widget=forms.TextInput(attrs={"class": "form-control"}),
            help_text=_("Forward Enterprise API Username."),
        )

        self.fields["password"] = forms.CharField(
            required=True,
            label=_("Password"),
            widget=forms.PasswordInput(attrs={"class": "form-control"}),
            help_text=_("Forward Enterprise API Password."),
        )

        self.fields["verify"] = forms.BooleanField(
            required=False,
            initial=True,
            help_text=_("Certificate validation. Uncheck if using self signed certificate."),
        )

        if self.instance and self.instance.parameters:
            params = self.instance.parameters
            for name in self.fields:
                if name in params:
                    self.fields[name].initial = params[name]

            if "auth" in params:
                try:
                    decoded = base64.b64decode(params["auth"]).decode()
                    user, pw = decoded.split(":", 1)
                    self.fields["username"].initial = user
                    self.fields["password"].initial = pw
                except Exception:
                    pass

    def save(self, *args, **kwargs):
        parameters = {}

        if "verify" in self.cleaned_data:
            parameters["verify"] = self.cleaned_data["verify"]
        if "timeout" in self.cleaned_data:
            parameters["timeout"] = self.cleaned_data["timeout"]

        user = self.cleaned_data.get("username")
        pw = self.cleaned_data.get("password")
        if user and pw:
            token = base64.b64encode(f"{user}:{pw}".encode()).decode()
            parameters["auth"] = token

        self.instance.parameters = parameters
        self.instance.status = DataSourceStatusChoices.NEW

        instance = super().save(*args, **kwargs)

        if not ForwardSnapshot.objects.filter(source=instance, snapshot_id="$latestProcessed").exists():
            ForwardSnapshot.objects.create(
                source=instance,
                snapshot_id="$latestProcessed",
                status=ForwardSnapshotStatusModelChoices.STATUS_PROCESSED,
                last_updated=timezone.now(),
            )

        return instance


class ForwardSyncForm(NetBoxModelForm):
    source = forms.ModelChoiceField(
        queryset=ForwardSource.objects.all(),
        required=True,
        label=_("Forward Enterprise Source"),
        widget=HTMXSelect(),
    )
    snapshot_data = forms.ModelChoiceField(
        queryset=ForwardSnapshot.objects.filter(status="processed"),
        required=False,
        label=_("Snapshot"),
        help_text=_("Forward Enterprise snapshot to query. Defaults to $latestProcessed if not specified."),
    )
    scheduled = forms.DateTimeField(
        required=False,
        widget=DateTimePicker(),
        label=_("Schedule at"),
        help_text=_("Schedule execution of sync to a set time"),
    )
    interval = forms.IntegerField(
        required=False,
        min_value=1,
        label=_("Recurs every"),
        widget=NumberWithOptions(options=JobIntervalChoices),
        help_text=_("Interval at which this sync is re-run (in minutes)"),
    )
    auto_merge = forms.BooleanField(
        required=False,
        label=_("Auto Merge"),
        help_text=_("Automatically merge staged changes into NetBox"),
    )
    allow_deletes = forms.BooleanField(
        required=False,
        label=_("Allow Deletes"),
        help_text=_("Remove existing objects from the branch if they are not present in the current snapshot."),
    )

    class Meta:
        model = ForwardSync
        fields = (
            "name",
            "source",
            "snapshot_data",
            "auto_merge",
            "allow_deletes",
            "type",
            "tags",
            "scheduled",
            "interval",
        )
        widgets = {
            "type": HTMXSelect(),
        }

    @property
    def fieldsets(self):
        fieldsets = [
            FieldSet("name", "source", name=_("Forward Enterprise Source")),
            FieldSet("snapshot_data", name=_("Snapshot Information")),
            FieldSet("type", name=_("Ingestion Type")),
        ]
        for k, v in self.backend_fields.items():
            fieldsets.append(FieldSet(*v, name=f"{k.upper()} Parameters"))
        fieldsets.extend([
            FieldSet("scheduled", "interval", name=_("Ingestion Execution Parameters")),
            FieldSet("auto_merge", "allow_deletes", name=_("Extras")),
            FieldSet("tags", name=_("Tags")),
        ])
        return fieldsets

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.fields["name"] = forms.CharField(
            required=True,
            label=_("Name"),
            help_text=_("Name for this sync configuration."),
            widget=forms.TextInput(attrs={"class": "form-control"}),
        )

        backend_type = get_field_value(self, "type")
        sync_parameters = get_sync_parameters()
        backend = sync_parameters if backend_type == "all" else {backend_type: sync_parameters.get(backend_type)}

        now = local_now().strftime("%Y-%m-%d %H:%M:%S")
        self.fields["scheduled"].help_text += f" (current time: <strong>{now}</strong>)"

        self.backend_fields = {}
        for k, v in backend.items():
            self.backend_fields[k] = []
            for qualified_key, field in v.items():
                field_name = f"fwd_{qualified_key.replace('.', '_')}"
                self.backend_fields[k].append(field_name)
                self.fields[field_name] = copy.copy(field)
                if self.instance and self.instance.parameters:
                    self.fields[field_name].initial = self.instance.parameters.get(field_name)

    def save(self, *args, **kwargs):
        parameters = {}
        for name in self.fields:
            if name.startswith("fwd_"):
                original_key = name[4:].replace("_", ".", 1)  # only the first underscore becomes a dot
                parameters[original_key] = self.cleaned_data[name]
        self.instance.parameters = parameters
        self.instance.status = DataSourceStatusChoices.NEW
        return super().save(*args, **kwargs)


class ForwardTableForm(forms.Form):
    source = forms.ModelChoiceField(
        queryset=ForwardSource.objects.all(),
        required=False,
        label=_("Forward Enterprise Source"),
    )
    snapshot_data = forms.ModelChoiceField(
        queryset=ForwardSnapshot.objects.filter(status="processed"),
        label=_("Snapshot"),
        required=False,
        help_text=_("Forward Enterprise snapshot to query. Defaults to $latestProcessed if not specified."),
    )
    table = forms.ChoiceField(
        choices=[("vlans.device_summary", "Vlans - DEVICE_SUMMARY")],
        required=True,
    )
    cache_enable = forms.ChoiceField(
        choices=((True, "Yes"), (False, "No")),
        required=False,
        label=_("Cache"),
        initial=True,
        help_text=_("Cache results for 24 hours"),
    )
