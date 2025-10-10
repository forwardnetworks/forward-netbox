import django_tables2 as tables
from django.utils.html import format_html
from django.utils.translation import gettext_lazy as _
from django_tables2 import Column
from netbox.tables import columns
from netbox.tables import NetBoxTable
from netbox_branching.models import ChangeDiff

from .models import IPFabricData
from .models import IPFabricIngestion
from .models import IPFabricIngestionIssue
from .models import IPFabricRelationshipField
from .models import IPFabricSnapshot
from .models import IPFabricSource
from .models import IPFabricSync
from .models import IPFabricTransformField
from .models import IPFabricTransformMap
from .models import IPFabricTransformMapGroup


DIFF_BUTTON = """
    <a href="#"
          hx-get="{% url 'plugins:ipfabric_netbox:ipfabricingestion_change_diff' pk=record.branch.pk change_pk=record.pk %}"
          hx-target="#htmx-modal-content"
          data-bs-toggle="modal"
          data-bs-target="#htmx-modal"
          class="btn btn-success btn-sm"
        >
        <i class="mdi mdi-code-tags">Diff</i>
    </a>
"""

DATA_BUTTON = """
    <a href="#"
          hx-get="{% url 'plugins:ipfabric_netbox:ipfabricdata_data' pk=record.pk %}"
          hx-target="#htmx-modal-content"
          data-bs-toggle="modal"
          data-bs-target="#htmx-modal"
          class="btn btn-success btn-sm"
        >
        <i class="mdi mdi-code-tags">JSON</i>
    </a>
"""


class IPFabricRelationshipFieldTable(NetBoxTable):
    actions = columns.ActionsColumn(actions=("edit", "delete"))

    class Meta(NetBoxTable.Meta):
        model = IPFabricRelationshipField
        fields = ("source_model", "target_field", "coalesce", "actions")
        default_columns = ("source_model", "target_field", "coalesce", "actions")


class IPFabricTransformFieldTable(NetBoxTable):
    id = tables.Column()
    actions = columns.ActionsColumn(actions=("edit", "delete"))

    class Meta(NetBoxTable.Meta):
        model = IPFabricTransformField
        fields = ("id", "source_field", "target_field", "coalesce", "actions")
        default_columns = ("source_field", "target_field", "coalesce", "actions")


class IPFabricTransformMapGroupTable(NetBoxTable):
    name = tables.Column(linkify=True)
    maps_count = columns.LinkedCountColumn(
        viewname="plugins:ipfabric_netbox:ipfabrictransformmap_list",
        url_params={"group_id": "pk"},
        verbose_name="Transform Maps",
    )

    class Meta(NetBoxTable.Meta):
        model = IPFabricTransformMapGroup
        fields = ("name", "description", "maps_count")
        default_columns = ("name", "description", "maps_count")


class IPFabricTransformMapTable(NetBoxTable):
    name = tables.Column(linkify=True)
    group = tables.Column(linkify=True)

    class Meta(NetBoxTable.Meta):
        model = IPFabricTransformMap
        fields = ("name", "group", "source_model", "target_model")
        default_columns = ("name", "group", "source_model", "target_model")


class IPFabricIngestionTable(NetBoxTable):
    name = tables.Column(linkify=True)
    sync = tables.Column(verbose_name="IP Fabric Sync", linkify=True)
    branch = tables.Column(linkify=True)
    changes = tables.Column(accessor="staged_changes", verbose_name="Number of Changes")
    actions = columns.ActionsColumn(actions=("delete",))

    class Meta(NetBoxTable.Meta):
        model = IPFabricIngestion
        fields = ("name", "sync", "branch", "description", "user", "changes")
        default_columns = ("name", "sync", "branch", "description", "user", "changes")


class IPFabricSnapshotTable(NetBoxTable):
    name = tables.Column(linkify=True)
    source = tables.Column(linkify=True)
    tags = columns.TagColumn(url_name="core:datasource_list")
    actions = columns.ActionsColumn(actions=("delete",))
    status = columns.ChoiceFieldColumn()

    class Meta(NetBoxTable.Meta):
        model = IPFabricSnapshot
        fields = (
            "pk",
            "id",
            "name",
            "snapshot_id",
            "status",
            "date",
            "created",
            "last_updated",
        )
        default_columns = ("pk", "name", "source", "snapshot_id", "status", "date")


class IPFabricSourceTable(NetBoxTable):
    name = tables.Column(linkify=True)
    status = columns.ChoiceFieldColumn()
    snapshot_count = tables.Column(verbose_name="Snapshots")
    tags = columns.TagColumn(url_name="core:datasource_list")

    class Meta(NetBoxTable.Meta):
        model = IPFabricSource
        fields = (
            "pk",
            "id",
            "name",
            "status",
            "description",
            "comments",
            "created",
            "last_updated",
        )
        default_columns = ("pk", "name", "status", "description", "snapshot_count")


class IPFabricSyncTable(NetBoxTable):
    name = tables.Column(linkify=True)
    status = columns.ChoiceFieldColumn()
    snapshot_name = tables.Column(
        verbose_name="Snapshot Name",
        accessor="snapshot_data",
        linkify=True,
    )
    last_ingestion = tables.Column(
        accessor="last_ingestion",
        verbose_name="Last Ingestion",
        linkify=True,
    )

    def render_last_ingestion(self, value: IPFabricIngestion):
        return getattr(value, "name", "---") if value else "---"

    def render_snapshot_name(self, value: IPFabricSnapshot):
        return getattr(value, "name", "---") if value else "---"

    class Meta(NetBoxTable.Meta):
        model = IPFabricSync
        fields = (
            "auto_merge",
            "id",
            "interval",
            "last_synced",
            "last_ingestion",
            "name",
            "scheduled",
            "status",
            "snapshot_name",
            "user",
        )
        default_columns = ("name", "status", "last_ingestion", "snapshot_name")


class IPFabricIngestionChangesTable(NetBoxTable):
    # There is no view for single change, remove the link in ID
    id = tables.Column(verbose_name=_("ID"))
    pk = None
    object_type = tables.Column(
        accessor="object_type.model", verbose_name="Object Type"
    )
    object = tables.Column(verbose_name="Object")
    actions = columns.TemplateColumn(template_code=DIFF_BUTTON)

    def render_object(self, value, record):
        model_templates = {
            "Device": lambda v: v.name,
            "DeviceRole": lambda v: v.name,
            "DeviceType": lambda v: v.model,
            "IPAddress": lambda v: v.address,
            "Interface": lambda v: f"{v.name} (Device {v.device.name})",
            "InventoryItem": lambda v: f"{v.name} (Device {v.device.name})",
            "MACAddress": lambda v: v.mac_address,
            "Manufacturer": lambda v: v.name,
            "Platform": lambda v: v.name,
            "Prefix": lambda v: f"{v.prefix} (VRF {v.vrf})",
            "Site": lambda v: v.name,
            "VirtualChassis": lambda v: v.name,
            "VLAN": lambda v: f"{v.name} (VID {v.vid})",
            "VRF": lambda v: v.name,
        }
        if value and (class_name := value.__class__.__name__) in model_templates:
            field_value = model_templates[class_name](value)
            if url := value.get_absolute_url():
                return format_html("<a href={}>{}</a>", url, field_value)
        else:
            field_value = record.object_repr
        return field_value

    class Meta(NetBoxTable.Meta):
        model = ChangeDiff
        name = "staged_changes"
        fields = ("object", "action", "object_type", "actions")
        default_columns = ("object", "action", "object_type", "actions")


class IPFabricIngestionIssuesTable(NetBoxTable):
    id = tables.Column(verbose_name=_("ID"))
    exception = tables.Column(verbose_name="Exception Type")
    message = tables.Column(verbose_name="Error Message")
    actions = None

    class Meta(NetBoxTable.Meta):
        model = IPFabricIngestionIssue
        fields = (
            "model",
            "timestamp",
            "raw_data",
            "coalesce_fields",
            "defaults",
            "exception",
            "message",
        )
        default_columns = ("model", "exception", "message")
        empty_text = _("No Ingestion Issues found")
        order_by = "id"


class DeviceIPFTable(tables.Table):
    hostname = Column()

    class Meta:
        attrs = {
            "class": "table table-hover object-list",
        }
        empty_text = _("No results found")

    def __init__(self, data, **kwargs):
        super().__init__(data, **kwargs)


class IPFabricDataTable(NetBoxTable):
    JSON = columns.TemplateColumn(template_code=DATA_BUTTON)
    actions = columns.ActionsColumn(actions=("delete",))

    class Meta(NetBoxTable.Meta):
        model = IPFabricData
        fields = ("snapshot_data", "JSON")
        default_columns = ("snapshot_data", "JSON")
