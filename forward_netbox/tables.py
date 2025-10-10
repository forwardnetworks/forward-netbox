import django_tables2 as tables
from django.utils.html import format_html
from django.utils.translation import gettext_lazy as _
from django_tables2 import Column
from netbox.tables import columns
from netbox.tables import NetBoxTable
from netbox_branching.models import ChangeDiff

from .models import ForwardData
from .models import ForwardIngestion
from .models import ForwardIngestionIssue
from .models import ForwardRelationshipField
from .models import ForwardSnapshot
from .models import ForwardSource
from .models import ForwardSync
from .models import ForwardTransformField
from .models import ForwardTransformMap
from .models import ForwardTransformMapGroup


DIFF_BUTTON = """
    <a href="#"
          hx-get="{% url 'plugins:forward_netbox:forwardingestion_change_diff' pk=record.branch.pk change_pk=record.pk %}"
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
          hx-get="{% url 'plugins:forward_netbox:forwarddata_data' pk=record.pk %}"
          hx-target="#htmx-modal-content"
          data-bs-toggle="modal"
          data-bs-target="#htmx-modal"
          class="btn btn-success btn-sm"
        >
        <i class="mdi mdi-code-tags">JSON</i>
    </a>
"""


class ForwardRelationshipFieldTable(NetBoxTable):
    actions = columns.ActionsColumn(actions=("edit", "delete"))

    class Meta(NetBoxTable.Meta):
        model = ForwardRelationshipField
        fields = ("source_model", "target_field", "coalesce", "actions")
        default_columns = ("source_model", "target_field", "coalesce", "actions")


class ForwardTransformFieldTable(NetBoxTable):
    id = tables.Column()
    actions = columns.ActionsColumn(actions=("edit", "delete"))

    class Meta(NetBoxTable.Meta):
        model = ForwardTransformField
        fields = ("id", "source_field", "target_field", "coalesce", "actions")
        default_columns = ("source_field", "target_field", "coalesce", "actions")


class ForwardTransformMapGroupTable(NetBoxTable):
    name = tables.Column(linkify=True)
    maps_count = columns.LinkedCountColumn(
        viewname="plugins:forward_netbox:forwardtransformmap_list",
        url_params={"group_id": "pk"},
        verbose_name="Transform Maps",
    )

    class Meta(NetBoxTable.Meta):
        model = ForwardTransformMapGroup
        fields = ("name", "description", "maps_count")
        default_columns = ("name", "description", "maps_count")


class ForwardTransformMapTable(NetBoxTable):
    name = tables.Column(linkify=True)
    group = tables.Column(linkify=True)

    class Meta(NetBoxTable.Meta):
        model = ForwardTransformMap
        fields = ("name", "group", "source_model", "target_model")
        default_columns = ("name", "group", "source_model", "target_model")


class ForwardIngestionTable(NetBoxTable):
    name = tables.Column(linkify=True)
    sync = tables.Column(verbose_name="Forward Sync", linkify=True)
    branch = tables.Column(linkify=True)
    changes = tables.Column(accessor="staged_changes", verbose_name="Number of Changes")
    actions = columns.ActionsColumn(actions=("delete",))

    class Meta(NetBoxTable.Meta):
        model = ForwardIngestion
        fields = ("name", "sync", "branch", "description", "user", "changes")
        default_columns = ("name", "sync", "branch", "description", "user", "changes")


class ForwardSnapshotTable(NetBoxTable):
    name = tables.Column(linkify=True)
    source = tables.Column(linkify=True)
    tags = columns.TagColumn(url_name="core:datasource_list")
    actions = columns.ActionsColumn(actions=("delete",))
    status = columns.ChoiceFieldColumn()

    class Meta(NetBoxTable.Meta):
        model = ForwardSnapshot
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


class ForwardSourceTable(NetBoxTable):
    name = tables.Column(linkify=True)
    status = columns.ChoiceFieldColumn()
    snapshot_count = tables.Column(verbose_name="Snapshots")
    tags = columns.TagColumn(url_name="core:datasource_list")

    class Meta(NetBoxTable.Meta):
        model = ForwardSource
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


class ForwardSyncTable(NetBoxTable):
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

    def render_last_ingestion(self, value: ForwardIngestion):
        return getattr(value, "name", "---") if value else "---"

    def render_snapshot_name(self, value: ForwardSnapshot):
        return getattr(value, "name", "---") if value else "---"

    class Meta(NetBoxTable.Meta):
        model = ForwardSync
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


class ForwardIngestionChangesTable(NetBoxTable):
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


class ForwardIngestionIssuesTable(NetBoxTable):
    id = tables.Column(verbose_name=_("ID"))
    exception = tables.Column(verbose_name="Exception Type")
    message = tables.Column(verbose_name="Error Message")
    actions = None

    class Meta(NetBoxTable.Meta):
        model = ForwardIngestionIssue
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


class DeviceFWDTable(tables.Table):
    hostname = Column()

    class Meta:
        attrs = {
            "class": "table table-hover object-list",
        }
        empty_text = _("No results found")

    def __init__(self, data, **kwargs):
        super().__init__(data, **kwargs)


class ForwardDataTable(NetBoxTable):
    JSON = columns.TemplateColumn(template_code=DATA_BUTTON)
    actions = columns.ActionsColumn(actions=("delete",))

    class Meta(NetBoxTable.Meta):
        model = ForwardData
        fields = ("snapshot_data", "JSON")
        default_columns = ("snapshot_data", "JSON")
