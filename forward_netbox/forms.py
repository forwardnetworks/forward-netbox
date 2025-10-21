import copy

from core.choices import DataSourceStatusChoices
from core.choices import JobIntervalChoices
from django import forms
from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ValidationError
from django.utils.translation import gettext_lazy as _
from netbox.forms import NetBoxModelFilterSetForm
from netbox.forms import NetBoxModelForm
from netbox.forms.mixins import SavedFiltersMixin
from utilities.datetime import local_now
from utilities.forms import ConfirmationForm
from utilities.forms import FilterForm
from utilities.forms import get_field_value
from utilities.forms.fields import CommentField
from utilities.forms.fields import DynamicModelChoiceField
from utilities.forms.fields import DynamicModelMultipleChoiceField
from utilities.forms.rendering import FieldSet
from utilities.forms.widgets import APISelectMultiple
from utilities.forms.widgets import DateTimePicker
from utilities.forms.widgets import HTMXSelect
from utilities.forms.widgets import NumberWithOptions

from .models import ForwardIngestion
from .models import ForwardNQEQuery
from .models import ForwardSnapshot
from .models import ForwardSource
from .models import ForwardSync
from .utilities.nqe_map import get_default_nqe_map


def build_sync_parameters() -> dict[str, dict[str, forms.BooleanField]]:
    mapping = get_default_nqe_map()
    parameters: dict[str, dict[str, forms.BooleanField]] = {"dcim": {}, "ipam": {}}

    for model_key, meta in mapping.items():
        try:
            app_label, model = model_key.split(".", 1)
        except ValueError:
            continue
        if app_label not in parameters:
            continue
        label = model.replace("_", " ").title()
        parameters[app_label][model] = forms.BooleanField(
            required=False,
            label=label,
            initial=meta.get("enabled", True),
        )

    return parameters


class ForwardNQEQueryForm(NetBoxModelForm):
    content_type = forms.ModelChoiceField(
        queryset=ContentType.objects.filter(app_label__in=["dcim", "ipam"]).order_by(
            "app_label", "model"
        ),
        label=_("Model"),
    )
    query_id = forms.CharField(
        label=_("NQE Query ID"),
        required=True,
        help_text=_("Identifier of the Forward Enterprise NQE query."),
    )
    enabled = forms.BooleanField(
        label=_("Enabled"),
        required=False,
        initial=True,
        help_text=_("Uncheck to disable syncing this model using NQE."),
    )
    description = CommentField(required=False)

    fieldsets = (
        FieldSet("content_type", "query_id", "enabled", name=_("Target Model")),
        FieldSet("description", "tags", name=_("Extras")),
    )

    class Meta:
        model = ForwardNQEQuery
        fields = (
            "content_type",
            "query_id",
            "enabled",
            "description",
            "tags",
        )
        widgets = {
            "content_type": HTMXSelect(),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.instance and self.instance.pk:
            self.fields["content_type"].disabled = True
            self.fields["content_type"].widget.attrs["readonly"] = True

def str_to_list(_str: str | list) -> list[str]:
    if not isinstance(_str, list):
        return [_str]
    else:
        return _str


def list_to_choices(choices: list[str]) -> tuple[tuple[str, str], ...]:
    new_choices = ()
    for choice in choices:
        new_choices = new_choices + ((choice, choice),)
    return new_choices


class ForwardSnapshotFilterForm(NetBoxModelFilterSetForm):
    model = ForwardSnapshot
    fieldsets = (
        FieldSet("q", "filter_id"),
        FieldSet("name", "source_id", "status", "snapshot_id", name=_("Source")),
    )
    name = forms.CharField(required=False, label=_("Name"))
    status = forms.CharField(required=False, label=_("Status"))
    source_id = DynamicModelMultipleChoiceField(
        queryset=ForwardSource.objects.all(), required=False, label=_("Source")
    )
    snapshot_id = forms.CharField(required=False, label=_("Snapshot ID"))


class ForwardSourceFilterForm(NetBoxModelFilterSetForm):
    model = ForwardSource
    fieldsets = (
        FieldSet("q", "filter_id"),
        FieldSet("status", "network_id", name=_("Source")),
    )
    status = forms.MultipleChoiceField(choices=DataSourceStatusChoices, required=False)
    network_id = forms.CharField(required=False, label=_("Network ID"))


class ForwardIngestionFilterForm(SavedFiltersMixin, FilterForm):
    fieldsets = (
        FieldSet("q", "filter_id"),
        FieldSet("sync_id", name=_("Source")),
    )
    model = ForwardIngestion
    sync_id = DynamicModelMultipleChoiceField(
        queryset=ForwardSync.objects.all(), required=False, label=_("Sync")
    )


class ForwardIngestionMergeForm(ConfirmationForm):
    remove_branch = forms.BooleanField(
        initial=True,
        required=False,
        label=_("Remove branch"),
        help_text=_("Leave unchecked to keep Branch for possible revert."),
    )


class ForwardSourceForm(NetBoxModelForm):
    comments = CommentField()
    DEFAULT_SAAS_URL = "https://fwd.app"

    class Meta:
        model = ForwardSource
        fields = [
            "name",
            "url",
            "network_id",
            "description",
            "comments",
        ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Deployment mode (SaaS vs. On-Premises)
        self.fields["deployment_mode"] = forms.ChoiceField(
            choices=(
                ("saas", _("Forward SaaS")),
                ("on_prem", _("On-Premises")),
            ),
            initial="saas",
            label=_("Deployment"),
            widget=HTMXSelect(),
        )

        mode = None
        if self.data and self.data.get("deployment_mode"):
            mode = self.data.get("deployment_mode")
        elif "deployment_mode" in kwargs.get("initial", {}):
            mode = kwargs["initial"]["deployment_mode"]
        elif self.instance and self.instance.pk:
            params = self.instance.parameters or {}
            mode = params.get("deployment_mode")
            if not mode:
                if (
                    self.instance.url == self.DEFAULT_SAAS_URL
                    and params.get("verify") is False
                ):
                    mode = "saas"
                else:
                    mode = "on_prem"
        if mode not in {"saas", "on_prem"}:
            mode = "saas"
        self.fields["deployment_mode"].initial = mode

        self.fields["url"] = forms.URLField(
            required=True,
            label=_("Base URL"),
            widget=forms.TextInput(attrs={"class": "form-control"}),
            help_text=_(
                "for example https://myinstance.com or https://192.168.0.1 etc."
            ),
        )

        self.fields["timeout"] = forms.IntegerField(
            required=False,
            label=_("Timeout"),
            help_text=_("Timeout for the API request."),
            widget=forms.NumberInput(attrs={"class": "form-control"}),
        )

        self.fields["auth"] = forms.CharField(
            required=True,
            label=_("API Token"),
            widget=forms.TextInput(attrs={"class": "form-control"}),
            help_text=_("Forward Enterprise API Token."),
        )
        self.fields["verify"] = forms.BooleanField(
            required=False,
            initial=True,
            help_text=_(
                "Certificate validation. Uncheck if using self signed certificate."
            ),
        )

        if mode == "saas":
            self.fields["url"].required = False
            self.fields["url"].initial = self.DEFAULT_SAAS_URL
            self.fields["url"].widget = forms.HiddenInput()
            self.fields["verify"].initial = False
            self.fields["verify"].widget = forms.HiddenInput()
        else:
            self.fields["url"].required = True
            self.fields["verify"].widget = forms.CheckboxInput()
            if self.instance and self.instance.parameters:
                verify_value = self.instance.parameters.get("verify")
                if verify_value is not None:
                    self.fields["verify"].initial = verify_value

        if self.instance.pk and isinstance(self.instance.parameters, dict):
            for name, value in self.instance.parameters.items():
                if name in self.fields:
                    self.fields[name].initial = value

        source_fields = ["name", "deployment_mode"]
        if mode == "on_prem":
            source_fields.append("url")
        source_fields.extend(["network_id", "description", "comments"])

        parameter_fields = ["auth"]
        if mode == "on_prem":
            parameter_fields.append("verify")
        parameter_fields.append("timeout")

        self.fieldsets: list[FieldSet] = [
            FieldSet(*source_fields, name=_("Source")),
            FieldSet(*parameter_fields, name=_("Parameters")),
        ]

    def save(self, *args, **kwargs):
        parameters = {}
        for name in self.fields:
            if name.startswith("auth") and name in self.cleaned_data:
                parameters["auth"] = self.cleaned_data[name]
            if name.startswith("verify") and name in self.cleaned_data:
                parameters["verify"] = self.cleaned_data[name]
            if name.startswith("timeout") and name in self.cleaned_data:
                parameters["timeout"] = self.cleaned_data[name]

        mode = self.cleaned_data.get("deployment_mode", "saas")
        parameters["deployment_mode"] = mode

        if mode == "saas":
            self.instance.url = self.DEFAULT_SAAS_URL
            parameters["verify"] = False
        else:
            url = self.cleaned_data.get("url", "").rstrip("/")
            self.instance.url = url
            if "verify" in parameters:
                parameters["verify"] = bool(parameters["verify"])

        self.instance.parameters = parameters or {}
        self.instance.network_id = self.cleaned_data.get("network_id")
        self.instance.status = DataSourceStatusChoices.NEW

        return super().save(*args, **kwargs)


class ForwardSyncForm(NetBoxModelForm):
    source = forms.ModelChoiceField(
        queryset=ForwardSource.objects.all(),
        required=True,
        label=_("Forward Enterprise Source"),
        widget=HTMXSelect(),
    )
    snapshot_data = DynamicModelChoiceField(
        queryset=ForwardSnapshot.objects.filter(status="loaded"),
        required=True,
        label=_("Snapshot"),
        query_params={
            "source_id": "$source",
            "status": "loaded",
        },
    )

    sites = forms.MultipleChoiceField(
        required=False,
        label=_("Sites"),
        help_text=_("Defaults to all sites if none selected."),
        widget=APISelectMultiple(
            api_url="/api/plugins/forward/snapshot/{{snapshot_data}}/sites/",
        ),
    )

    update_custom_fields = forms.BooleanField(
        required=False,
        label=_("Custom Fields Updating"),
        help_text=_("Update object custom fields where applicable."),
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

    class Meta:
        model = ForwardSync
        fields = (
            "name",
            "source",
            "snapshot_data",
            "auto_merge",
            "update_custom_fields",
            "sites",
            "tags",
            "scheduled",
            "interval",
        )
        widgets = {"source": HTMXSelect()}

    def __init__(self, *args, **kwargs):
        # The initial data for BooleanFields change to list on HTMX requests.
        # This causes URL to bloat, so we need to sanitize it before it's used.
        initial = kwargs.get("initial", {}).copy()
        for name, value in initial.items():
            if (
                (
                    name.startswith("fwd_")
                    or (
                        name in self.base_fields
                        and isinstance(self.base_fields[name], forms.BooleanField)
                    )
                )
                and isinstance(value, list)
                and len(value) > 1
            ):
                initial[name] = value[-1]  # Keep only the last value
        kwargs["initial"] = initial
        super().__init__(*args, **kwargs)

        source = get_field_value(self, "source")

        if not self.data:
            if sites := get_field_value(self, "sites"):
                sites = list_to_choices(str_to_list(sites))
                self.fields["sites"].choices = sites
                self.fields["sites"].initial = sites
        else:
            if snapshot_id := self.data.get("snapshot_data"):
                snapshot_sites = ForwardSnapshot.objects.get(pk=snapshot_id).sites
                choices = list_to_choices(str_to_list(snapshot_sites))
                self.fields["sites"].choices = choices
            source = self.data.get("source")

        # These values are not populated by data on instance, so we need to set them manually
        if self.instance and self.instance.pk:
            if not kwargs.get("initial"):
                source = self.instance.snapshot_data.source
                if not self.data:
                    self.fields["sites"].choices = list_to_choices(
                        self.instance.snapshot_data.sites
                    )
            else:
                source = kwargs["initial"].get(
                    "source", self.instance.snapshot_data.source
                )
            self.initial["source"] = source

            # Handle sites field initialization
            if "sites" not in self.initial:
                selected_sites = self.instance.parameters.get("sites", [])
                self.initial["sites"] = selected_sites

                # Ensure the field has the selected sites as both choices and initial values
                if not self.data and selected_sites:
                    # Get current choices and ensure selected sites are included
                    current_choices = (
                        list(self.fields["sites"].choices)
                        if hasattr(self.fields["sites"], "choices")
                        else []
                    )
                    current_choice_values = [choice[0] for choice in current_choices]

                    # Add any missing selected sites to choices
                    for site in str_to_list(selected_sites):
                        if site not in current_choice_values:
                            current_choices.append((site, site))

                    self.fields["sites"].choices = current_choices
                self.fields["sites"].initial = selected_sites
            else:
                self.fields["sites"].initial = self.initial["sites"]

        now = local_now().strftime("%Y-%m-%d %H:%M:%S")
        self.fields["scheduled"].help_text += f" (current time: <strong>{now}</strong>)"

        # Add backend-specific form fields
        self.backend_fields = {}

        # Prepare buttons for each target Model
        parameter_definitions = build_sync_parameters()
        for k, v in parameter_definitions.items():
            self.backend_fields[k] = []
            for name, form_field in v.items():
                field_name = f"fwd_{name}"
                self.backend_fields[k].append(field_name)
                self.fields[field_name] = copy.deepcopy(form_field)
                if self.instance and self.instance.parameters:
                    value = self.instance.parameters.get(name)
                    self.fields[field_name].initial = value

        # Set fieldsets dynamically based and backend_fields
        fieldsets = [FieldSet("name", "source", name=_("Forward Enterprise Source"))]
        # Only show snapshot and sites if source is selected
        if source:
            fieldsets.append(
                FieldSet("snapshot_data", "sites", name=_("Snapshot Information")),
            )
        for k, v in self.backend_fields.items():
            fieldsets.append(FieldSet(*v, name=f"{k.upper()} Parameters"))
        fieldsets.append(
            FieldSet("scheduled", "interval", name=_("Ingestion Execution Parameters"))
        )
        fieldsets.append(
            FieldSet("auto_merge", "update_custom_fields", name=_("Extras"))
        )
        fieldsets.append(FieldSet("tags", name=_("Tags")))

        self.fieldsets = fieldsets

    def clean(self):
        super().clean()

        source = self.cleaned_data.get("source")
        snapshot = self.cleaned_data.get("snapshot_data")

        if (
            source
            and snapshot
            and ForwardSource.objects.get(pk=source.pk)
            != ForwardSnapshot.objects.get(pk=snapshot.pk).source
        ):
            raise ValidationError(
                {"snapshot_data": _("Snapshot does not belong to the selected source.")}
            )

        sites = self.data.get("sites")
        self.fields["sites"].choices = list_to_choices(str_to_list(sites))
        if sites and "snapshot_data" in self.cleaned_data:
            # Check if all sites are valid - fail if any site is not found in snapshot.sites
            if not all(
                any(site in snapshot_site for snapshot_site in snapshot.sites)
                for site in sites
            ):
                invalid_sites = [
                    site
                    for site in sites
                    if not any(
                        site in snapshot_site for snapshot_site in snapshot.sites
                    )
                ]
                raise ValidationError(
                    {"sites": f"Sites {invalid_sites} not part of the snapshot."}
                )

        scheduled_time = self.cleaned_data.get("scheduled")
        if scheduled_time and scheduled_time < local_now():
            raise forms.ValidationError(_("Scheduled time must be in the future."))

        # When interval is used without schedule at, schedule for the current time
        if self.cleaned_data.get("interval") and not scheduled_time:
            self.cleaned_data["scheduled"] = local_now()

        return self.cleaned_data

    def save(self, *args, **kwargs):
        parameters = {}
        for name in self.fields:
            if name.startswith("fwd_"):
                parameters[name[4:]] = self.cleaned_data[name]
            elif name == "sites":
                parameters["sites"] = self.cleaned_data["sites"]
        if self.instance and self.instance.pk and self.instance.parameters:
            existing_overrides = self.instance.parameters.get("nqe_map")
            if existing_overrides:
                parameters["nqe_map"] = existing_overrides
        self.instance.parameters = parameters
        self.instance.status = DataSourceStatusChoices.NEW

        object = super().save(*args, **kwargs)
        if object.scheduled:
            object.enqueue_sync_job()
        return object


tableChoices = [
    ("eol_details", "Inventory - EOL_DETAILS"),
    ("fans", "Inventory - FANS"),
    ("interfaces", "Inventory - INTERFACES"),
    ("modules", "Inventory - MODULES"),
    ("pn", "Inventory - PN"),
    ("addressing.arp_table", "Addressing - ARP_TABLE"),
    ("addressing.ipv6_neighbor_discovery", "Addressing - IPV6_NEIGHBOR_DISCOVERY"),
    ("addressing.mac_table", "Addressing - MAC_TABLE"),
    ("addressing.managed_ip_ipv4", "Addressing - MANAGED_IP_IPV4"),
    ("addressing.managed_ip_ipv6", "Addressing - MANAGED_IP_IPV6"),
    ("addressing.nat44", "Addressing - NAT44"),
    ("cloud.virtual_interfaces", "Cloud - VIRTUAL_INTERFACES"),
    ("cloud.virtual_machines", "Cloud - VIRTUAL_MACHINES"),
    ("dhcp.relay_global_stats_received", "Dhcp - RELAY_GLOBAL_STATS_RECEIVED"),
    ("dhcp.relay_global_stats_relayed", "Dhcp - RELAY_GLOBAL_STATS_RELAYED"),
    ("dhcp.relay_global_stats_sent", "Dhcp - RELAY_GLOBAL_STATS_SENT"),
    ("dhcp.relay_global_stats_summary", "Dhcp - RELAY_GLOBAL_STATS_SUMMARY"),
    ("dhcp.relay_interfaces", "Dhcp - RELAY_INTERFACES"),
    ("dhcp.relay_interfaces_stats_received", "Dhcp - RELAY_INTERFACES_STATS_RECEIVED"),
    ("dhcp.relay_interfaces_stats_relayed", "Dhcp - RELAY_INTERFACES_STATS_RELAYED"),
    ("dhcp.relay_interfaces_stats_sent", "Dhcp - RELAY_INTERFACES_STATS_SENT"),
    ("dhcp.server_excluded_interfaces", "Dhcp - SERVER_EXCLUDED_INTERFACES"),
    ("dhcp.server_excluded_ranges", "Dhcp - SERVER_EXCLUDED_RANGES"),
    ("dhcp.server_leases", "Dhcp - SERVER_LEASES"),
    ("dhcp.server_pools", "Dhcp - SERVER_POOLS"),
    ("dhcp.server_summary", "Dhcp - SERVER_SUMMARY"),
    ("fhrp.glbp_forwarders", "Fhrp - GLBP_FORWARDERS"),
    ("fhrp.group_state", "Fhrp - GROUP_STATE"),
    ("fhrp.stproot_alignment", "Fhrp - STPROOT_ALIGNMENT"),
    ("fhrp.virtual_gateways", "Fhrp - VIRTUAL_GATEWAYS"),
    (
        "interfaces.average_rates_data_bidirectional",
        "Interfaces - AVERAGE_RATES_DATA_BIDIRECTIONAL",
    ),
    (
        "interfaces.average_rates_data_bidirectional_per_device",
        "Interfaces - AVERAGE_RATES_DATA_BIDIRECTIONAL_PER_DEVICE",
    ),
    (
        "interfaces.average_rates_data_inbound",
        "Interfaces - AVERAGE_RATES_DATA_INBOUND",
    ),
    (
        "interfaces.average_rates_data_inbound_per_device",
        "Interfaces - AVERAGE_RATES_DATA_INBOUND_PER_DEVICE",
    ),
    (
        "interfaces.average_rates_data_outbound",
        "Interfaces - AVERAGE_RATES_DATA_OUTBOUND",
    ),
    (
        "interfaces.average_rates_data_outbound_per_device",
        "Interfaces - AVERAGE_RATES_DATA_OUTBOUND_PER_DEVICE",
    ),
    (
        "interfaces.average_rates_drops_bidirectional",
        "Interfaces - AVERAGE_RATES_DROPS_BIDIRECTIONAL",
    ),
    (
        "interfaces.average_rates_drops_bidirectional_per_device",
        "Interfaces - AVERAGE_RATES_DROPS_BIDIRECTIONAL_PER_DEVICE",
    ),
    (
        "interfaces.average_rates_drops_inbound",
        "Interfaces - AVERAGE_RATES_DROPS_INBOUND",
    ),
    (
        "interfaces.average_rates_drops_inbound_per_device",
        "Interfaces - AVERAGE_RATES_DROPS_INBOUND_PER_DEVICE",
    ),
    (
        "interfaces.average_rates_drops_outbound",
        "Interfaces - AVERAGE_RATES_DROPS_OUTBOUND",
    ),
    (
        "interfaces.average_rates_drops_outbound_per_device",
        "Interfaces - AVERAGE_RATES_DROPS_OUTBOUND_PER_DEVICE",
    ),
    (
        "interfaces.average_rates_errors_bidirectional",
        "Interfaces - AVERAGE_RATES_ERRORS_BIDIRECTIONAL",
    ),
    (
        "interfaces.average_rates_errors_bidirectional_per_device",
        "Interfaces - AVERAGE_RATES_ERRORS_BIDIRECTIONAL_PER_DEVICE",
    ),
    (
        "interfaces.average_rates_errors_inbound",
        "Interfaces - AVERAGE_RATES_ERRORS_INBOUND",
    ),
    (
        "interfaces.average_rates_errors_inbound_per_device",
        "Interfaces - AVERAGE_RATES_ERRORS_INBOUND_PER_DEVICE",
    ),
    (
        "interfaces.average_rates_errors_outbound",
        "Interfaces - AVERAGE_RATES_ERRORS_OUTBOUND",
    ),
    (
        "interfaces.average_rates_errors_outbound_per_device",
        "Interfaces - AVERAGE_RATES_ERRORS_OUTBOUND_PER_DEVICE",
    ),
    (
        "interfaces.connectivity_matrix_unmanaged_neighbors_detail",
        "Interfaces - CONNECTIVITY_MATRIX_UNMANAGED_NEIGHBORS_DETAIL",
    ),
    (
        "interfaces.connectivity_matrix_unmanaged_neighbors_summary",
        "Interfaces - CONNECTIVITY_MATRIX_UNMANAGED_NEIGHBORS_SUMMARY",
    ),
    ("interfaces.counters_inbound", "Interfaces - COUNTERS_INBOUND"),
    ("interfaces.counters_outbound", "Interfaces - COUNTERS_OUTBOUND"),
    (
        "interfaces.current_rates_data_bidirectional",
        "Interfaces - CURRENT_RATES_DATA_BIDIRECTIONAL",
    ),
    (
        "interfaces.current_rates_data_inbound",
        "Interfaces - CURRENT_RATES_DATA_INBOUND",
    ),
    (
        "interfaces.current_rates_data_outbound",
        "Interfaces - CURRENT_RATES_DATA_OUTBOUND",
    ),
    ("interfaces.err_disabled", "Interfaces - ERR_DISABLED"),
    (
        "interfaces.point_to_point_over_ethernet",
        "Interfaces - POINT_TO_POINT_OVER_ETHERNET",
    ),
    (
        "interfaces.point_to_point_over_ethernet_sessions",
        "Interfaces - POINT_TO_POINT_OVER_ETHERNET_SESSIONS",
    ),
    ("interfaces.storm_control_all", "Interfaces - STORM_CONTROL_ALL"),
    ("interfaces.storm_control_broadcast", "Interfaces - STORM_CONTROL_BROADCAST"),
    ("interfaces.storm_control_multicast", "Interfaces - STORM_CONTROL_MULTICAST"),
    ("interfaces.storm_control_unicast", "Interfaces - STORM_CONTROL_UNICAST"),
    ("interfaces.switchport", "Interfaces - SWITCHPORT"),
    ("interfaces.transceivers", "Interfaces - TRANSCEIVERS"),
    ("interfaces.transceivers_errors", "Interfaces - TRANSCEIVERS_ERRORS"),
    ("interfaces.transceivers_statistics", "Interfaces - TRANSCEIVERS_STATISTICS"),
    (
        "interfaces.transceivers_triggered_thresholds",
        "Interfaces - TRANSCEIVERS_TRIGGERED_THRESHOLDS",
    ),
    ("interfaces.tunnels_ipv4", "Interfaces - TUNNELS_IPV4"),
    ("interfaces.tunnels_ipv6", "Interfaces - TUNNELS_IPV6"),
    ("load_balancing.virtual_servers", "Load_balancing - VIRTUAL_SERVERS"),
    (
        "load_balancing.virtual_servers_f5_partitions",
        "Load_balancing - VIRTUAL_SERVERS_F5_PARTITIONS",
    ),
    (
        "load_balancing.virtual_servers_pool_members",
        "Load_balancing - VIRTUAL_SERVERS_POOL_MEMBERS",
    ),
    ("load_balancing.virtual_servers_pools", "Load_balancing - VIRTUAL_SERVERS_POOLS"),
    ("management.aaa_accounting", "Management - AAA_ACCOUNTING"),
    ("management.aaa_authentication", "Management - AAA_AUTHENTICATION"),
    ("management.aaa_authorization", "Management - AAA_AUTHORIZATION"),
    ("management.aaa_lines", "Management - AAA_LINES"),
    ("management.aaa_password_strength", "Management - AAA_PASSWORD_STRENGTH"),
    ("management.aaa_servers", "Management - AAA_SERVERS"),
    ("management.aaa_users", "Management - AAA_USERS"),
    (
        "management.cisco_smart_licenses_authorization",
        "Management - CISCO_SMART_LICENSES_AUTHORIZATION",
    ),
    (
        "management.cisco_smart_licenses_registration",
        "Management - CISCO_SMART_LICENSES_REGISTRATION",
    ),
    (
        "management.cisco_smart_licenses_reservations",
        "Management - CISCO_SMART_LICENSES_RESERVATIONS",
    ),
    ("management.dns_resolver_servers", "Management - DNS_RESOLVER_SERVERS"),
    ("management.dns_resolver_settings", "Management - DNS_RESOLVER_SETTINGS"),
    ("management.flow_overview", "Management - FLOW_OVERVIEW"),
    ("management.license_summary", "Management - LICENSE_SUMMARY"),
    ("management.licenses", "Management - LICENSES"),
    ("management.licenses_detail", "Management - LICENSES_DETAIL"),
    ("management.logging_local", "Management - LOGGING_LOCAL"),
    ("management.logging_remote", "Management - LOGGING_REMOTE"),
    ("management.logging_summary", "Management - LOGGING_SUMMARY"),
    ("management.netflow_collectors", "Management - NETFLOW_COLLECTORS"),
    ("management.netflow_devices", "Management - NETFLOW_DEVICES"),
    ("management.netflow_interfaces", "Management - NETFLOW_INTERFACES"),
    ("management.ntp_sources", "Management - NTP_SOURCES"),
    ("management.ntp_summary", "Management - NTP_SUMMARY"),
    ("management.port_mirroring", "Management - PORT_MIRRORING"),
    ("management.ptp_interfaces", "Management - PTP_INTERFACES"),
    ("management.ptp_local_clock", "Management - PTP_LOCAL_CLOCK"),
    ("management.ptp_masters", "Management - PTP_MASTERS"),
    ("management.saved_config_consistency", "Management - SAVED_CONFIG_CONSISTENCY"),
    ("management.sflow_collectors", "Management - SFLOW_COLLECTORS"),
    ("management.sflow_devices", "Management - SFLOW_DEVICES"),
    ("management.sflow_sources", "Management - SFLOW_SOURCES"),
    ("management.snmp_communities", "Management - SNMP_COMMUNITIES"),
    ("management.snmp_summary", "Management - SNMP_SUMMARY"),
    ("management.snmp_trap_hosts", "Management - SNMP_TRAP_HOSTS"),
    ("management.snmp_users", "Management - SNMP_USERS"),
    ("management.telnet_access", "Management - TELNET_ACCESS"),
    ("mpls.forwarding", "Mpls - FORWARDING"),
    ("mpls.l2vpn_circuit_cross_connect", "Mpls - L2VPN_CIRCUIT_CROSS_CONNECT"),
    ("mpls.l2vpn_point_to_multipoint", "Mpls - L2VPN_POINT_TO_MULTIPOINT"),
    ("mpls.l2vpn_point_to_point_vpws", "Mpls - L2VPN_POINT_TO_POINT_VPWS"),
    ("mpls.l2vpn_pseudowires", "Mpls - L2VPN_PSEUDOWIRES"),
    ("mpls.l3vpn_pe_routers", "Mpls - L3VPN_PE_ROUTERS"),
    ("mpls.l3vpn_pe_vrfs", "Mpls - L3VPN_PE_VRFS"),
    ("mpls.l3vpn_vrf_targets", "Mpls - L3VPN_VRF_TARGETS"),
    ("mpls.ldp_interfaces", "Mpls - LDP_INTERFACES"),
    ("mpls.ldp_neighbors", "Mpls - LDP_NEIGHBORS"),
    ("mpls.rsvp_interfaces", "Mpls - RSVP_INTERFACES"),
    ("mpls.rsvp_neighbors", "Mpls - RSVP_NEIGHBORS"),
    ("multicast.igmp_groups", "Multicast - IGMP_GROUPS"),
    ("multicast.igmp_interfaces", "Multicast - IGMP_INTERFACES"),
    (
        "multicast.igmp_snooping_global_config",
        "Multicast - IGMP_SNOOPING_GLOBAL_CONFIG",
    ),
    ("multicast.igmp_snooping_groups", "Multicast - IGMP_SNOOPING_GROUPS"),
    ("multicast.igmp_snooping_vlans", "Multicast - IGMP_SNOOPING_VLANS"),
    ("multicast.mac_table", "Multicast - MAC_TABLE"),
    ("multicast.mroute_counters", "Multicast - MROUTE_COUNTERS"),
    ("multicast.mroute_first_hop_router", "Multicast - MROUTE_FIRST_HOP_ROUTER"),
    ("multicast.mroute_oil_detail", "Multicast - MROUTE_OIL_DETAIL"),
    ("multicast.mroute_overview", "Multicast - MROUTE_OVERVIEW"),
    ("multicast.mroute_sources", "Multicast - MROUTE_SOURCES"),
    ("multicast.mroute_table", "Multicast - MROUTE_TABLE"),
    ("multicast.pim_interfaces", "Multicast - PIM_INTERFACES"),
    ("multicast.pim_neighbors", "Multicast - PIM_NEIGHBORS"),
    ("multicast.rp_bsr", "Multicast - RP_BSR"),
    ("multicast.rp_mappings", "Multicast - RP_MAPPINGS"),
    ("multicast.rp_mappings_groups", "Multicast - RP_MAPPINGS_GROUPS"),
    (
        "oam.unidirectional_link_detection_interfaces",
        "Oam - UNIDIRECTIONAL_LINK_DETECTION_INTERFACES",
    ),
    (
        "oam.unidirectional_link_detection_neighbors",
        "Oam - UNIDIRECTIONAL_LINK_DETECTION_NEIGHBORS",
    ),
    (
        "platforms.cisco_fabric_path_isis_neighbors",
        "Platforms - CISCO_FABRIC_PATH_ISIS_NEIGHBORS",
    ),
    ("platforms.cisco_fabric_path_routes", "Platforms - CISCO_FABRIC_PATH_ROUTES"),
    ("platforms.cisco_fabric_path_summary", "Platforms - CISCO_FABRIC_PATH_SUMMARY"),
    ("platforms.cisco_fabric_path_switches", "Platforms - CISCO_FABRIC_PATH_SWITCHES"),
    ("platforms.cisco_fex_interfaces", "Platforms - CISCO_FEX_INTERFACES"),
    ("platforms.cisco_fex_modules", "Platforms - CISCO_FEX_MODULES"),
    ("platforms.cisco_vdc_devices", "Platforms - CISCO_VDC_DEVICES"),
    ("platforms.cisco_vss_chassis", "Platforms - CISCO_VSS_CHASSIS"),
    ("platforms.cisco_vss_vsl", "Platforms - CISCO_VSS_VSL"),
    ("platforms.environment_fans", "Platforms - ENVIRONMENT_FANS"),
    ("platforms.environment_modules", "Platforms - ENVIRONMENT_MODULES"),
    ("platforms.environment_power_supplies", "Platforms - ENVIRONMENT_POWER_SUPPLIES"),
    (
        "platforms.environment_power_supplies_fans",
        "Platforms - ENVIRONMENT_POWER_SUPPLIES_FANS",
    ),
    (
        "platforms.environment_temperature_sensors",
        "Platforms - ENVIRONMENT_TEMPERATURE_SENSORS",
    ),
    ("platforms.juniper_cluster", "Platforms - JUNIPER_CLUSTER"),
    ("platforms.logical_devices", "Platforms - LOGICAL_DEVICES"),
    ("platforms.platform_cisco_vss", "Platforms - PLATFORM_CISCO_VSS"),
    ("platforms.poe_devices", "Platforms - POE_DEVICES"),
    ("platforms.poe_interfaces", "Platforms - POE_INTERFACES"),
    ("platforms.poe_modules", "Platforms - POE_MODULES"),
    ("platforms.stacks", "Platforms - STACKS"),
    ("platforms.stacks_members", "Platforms - STACKS_MEMBERS"),
    ("platforms.stacks_stack_ports", "Platforms - STACKS_STACK_PORTS"),
    (
        "port_channels.inbound_balancing_table",
        "Port_channels - INBOUND_BALANCING_TABLE",
    ),
    ("port_channels.member_status_table", "Port_channels - MEMBER_STATUS_TABLE"),
    ("port_channels.mlag_cisco_vpc", "Port_channels - MLAG_CISCO_VPC"),
    ("port_channels.mlag_peers", "Port_channels - MLAG_PEERS"),
    ("port_channels.mlag_switches", "Port_channels - MLAG_SWITCHES"),
    (
        "port_channels.outbound_balancing_table",
        "Port_channels - OUTBOUND_BALANCING_TABLE",
    ),
    ("qos.marking", "Qos - MARKING"),
    ("qos.policing", "Qos - POLICING"),
    ("qos.policy_maps", "Qos - POLICY_MAPS"),
    ("qos.priority_queuing", "Qos - PRIORITY_QUEUING"),
    ("qos.queuing", "Qos - QUEUING"),
    ("qos.random_drops", "Qos - RANDOM_DROPS"),
    ("qos.shaping", "Qos - SHAPING"),
    ("routing.bgp_address_families", "Routing - BGP_ADDRESS_FAMILIES"),
    ("routing.bgp_neighbors", "Routing - BGP_NEIGHBORS"),
    ("routing.eigrp_interfaces", "Routing - EIGRP_INTERFACES"),
    ("routing.eigrp_neighbors", "Routing - EIGRP_NEIGHBORS"),
    ("routing.isis_interfaces", "Routing - ISIS_INTERFACES"),
    ("routing.isis_levels", "Routing - ISIS_LEVELS"),
    ("routing.isis_neighbors", "Routing - ISIS_NEIGHBORS"),
    ("routing.lisp_map_resolvers_ipv4", "Routing - LISP_MAP_RESOLVERS_IPV4"),
    ("routing.lisp_map_resolvers_ipv6", "Routing - LISP_MAP_RESOLVERS_IPV6"),
    ("routing.lisp_routes_ipv4", "Routing - LISP_ROUTES_IPV4"),
    ("routing.lisp_routes_ipv6", "Routing - LISP_ROUTES_IPV6"),
    ("routing.ospf_interfaces", "Routing - OSPF_INTERFACES"),
    ("routing.ospf_neighbors", "Routing - OSPF_NEIGHBORS"),
    ("routing.ospfv3_interfaces", "Routing - OSPFV3_INTERFACES"),
    ("routing.ospfv3_neighbors", "Routing - OSPFV3_NEIGHBORS"),
    ("routing.policies", "Routing - POLICIES"),
    ("routing.policies_interfaces", "Routing - POLICIES_INTERFACES"),
    ("routing.policies_pbr", "Routing - POLICIES_PBR"),
    ("routing.policies_prefix_list", "Routing - POLICIES_PREFIX_LIST"),
    ("routing.policies_prefix_list_ipv6", "Routing - POLICIES_PREFIX_LIST_IPV6"),
    ("routing.rip_interfaces", "Routing - RIP_INTERFACES"),
    ("routing.rip_neighbors", "Routing - RIP_NEIGHBORS"),
    ("routing.routes_ipv4", "Routing - ROUTES_IPV4"),
    ("routing.routes_ipv6", "Routing - ROUTES_IPV6"),
    ("routing.summary_protocols", "Routing - SUMMARY_PROTOCOLS"),
    ("routing.summary_protocols_bgp", "Routing - SUMMARY_PROTOCOLS_BGP"),
    ("routing.summary_protocols_eigrp", "Routing - SUMMARY_PROTOCOLS_EIGRP"),
    ("routing.summary_protocols_isis", "Routing - SUMMARY_PROTOCOLS_ISIS"),
    ("routing.summary_protocols_ospf", "Routing - SUMMARY_PROTOCOLS_OSPF"),
    ("routing.summary_protocols_ospfv3", "Routing - SUMMARY_PROTOCOLS_OSPFV3"),
    ("routing.summary_protocols_rip", "Routing - SUMMARY_PROTOCOLS_RIP"),
    ("routing.vrf_detail", "Routing - VRF_DETAIL"),
    ("routing.vrf_interfaces", "Routing - VRF_INTERFACES"),
    ("sdn.aci_dtep", "Sdn - ACI_DTEP"),
    ("sdn.aci_endpoints", "Sdn - ACI_ENDPOINTS"),
    ("sdn.aci_vlan", "Sdn - ACI_VLAN"),
    ("sdn.aci_vrf", "Sdn - ACI_VRF"),
    ("sdn.apic_controllers", "Sdn - APIC_CONTROLLERS"),
    ("sdn.vxlan_interfaces", "Sdn - VXLAN_INTERFACES"),
    ("sdn.vxlan_peers", "Sdn - VXLAN_PEERS"),
    ("sdn.vxlan_vni", "Sdn - VXLAN_VNI"),
    ("sdn.vxlan_vtep", "Sdn - VXLAN_VTEP"),
    ("sdwan.links", "Sdwan - LINKS"),
    ("sdwan.sites", "Sdwan - SITES"),
    ("security.acl", "Security - ACL"),
    ("security.acl_global_policies", "Security - ACL_GLOBAL_POLICIES"),
    ("security.acl_interface", "Security - ACL_INTERFACE"),
    ("security.dhcp_snooping", "Security - DHCP_SNOOPING"),
    ("security.dhcp_snooping_bindings", "Security - DHCP_SNOOPING_BINDINGS"),
    ("security.dmvpn", "Security - DMVPN"),
    ("security.ipsec_gateways", "Security - IPSEC_GATEWAYS"),
    ("security.ipsec_tunnels", "Security - IPSEC_TUNNELS"),
    ("security.secure_ports_devices", "Security - SECURE_PORTS_DEVICES"),
    ("security.secure_ports_interfaces", "Security - SECURE_PORTS_INTERFACES"),
    ("security.secure_ports_users", "Security - SECURE_PORTS_USERS"),
    ("security.zone_firewall_interfaces", "Security - ZONE_FIREWALL_INTERFACES"),
    ("security.zone_firewall_policies", "Security - ZONE_FIREWALL_POLICIES"),
    ("technology.serial_ports", "Technology - SERIAL_PORTS"),
    ("stp.bridges", "Stp - BRIDGES"),
    ("stp.guards", "Stp - GUARDS"),
    ("stp.inconsistencies", "Stp - INCONSISTENCIES"),
    ("stp.inconsistencies_details", "Stp - INCONSISTENCIES_DETAILS"),
    (
        "stp.inconsistencies_stp_cdp_ports_mismatch",
        "Stp - INCONSISTENCIES_STP_CDP_PORTS_MISMATCH",
    ),
    ("stp.instances", "Stp - INSTANCES"),
    ("stp.neighbors", "Stp - NEIGHBORS"),
    ("stp.ports", "Stp - PORTS"),
    ("stp.vlans", "Stp - VLANS"),
    ("vlans.device_detail", "Vlans - DEVICE_DETAIL"),
    ("vlans.device_summary", "Vlans - DEVICE_SUMMARY"),
]


class ForwardTableForm(forms.Form):
    source = DynamicModelChoiceField(
        queryset=ForwardSource.objects.all(),
        required=False,
        label=_("Forward Enterprise Source"),
    )
    snapshot_data = DynamicModelChoiceField(
        queryset=ForwardSnapshot.objects.filter(status="loaded"),
        label=_("Snapshot"),
        required=False,
        query_params={
            "source_id": "$source",
            "status": "loaded",
        },
        help_text=_("Forward Enterprise snapshot to query. Defaults to $last if not specified."),
    )
    table = forms.ChoiceField(choices=tableChoices, required=True)
    cache_enable = forms.ChoiceField(
        choices=((True, "Yes"), (False, "No")),
        required=False,
        label=_("Cache"),
        initial=True,
        help_text=_("Cache results for 24 hours"),
    )
