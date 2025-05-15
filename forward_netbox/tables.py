import django_tables2 as tables
from django.utils.translation import gettext_lazy as _

from netbox.tables import columns
from netbox.tables import NetBoxTable

from .models import (
    ForwardData,
    ForwardNQEMap,
    ForwardSnapshot,
    ForwardSource,
    ForwardSync,
)

DATA_BUTTON = """
<a href="#"
   class="btn btn-sm btn-outline-info"
   data-bs-toggle="modal"
   data-bs-target="#htmx-modal"
   hx-get="{% url 'plugins:forward_netbox:forwarddata_data' pk=record.pk %}">
   JSON
</a>
"""


class ForwardNQEMapTable(NetBoxTable):
    name = tables.Column(linkify=True)
    query_id = tables.Column()
    netbox_model = tables.Column()
    actions = columns.ActionsColumn(actions=("edit", "delete"))

    class Meta(NetBoxTable.Meta):
        model = ForwardNQEMap
        fields = ("pk", "name", "query_id", "netbox_model")
        default_columns = ("pk", "name", "query_id", "netbox_model")
        empty_text = _("No results found")


class ForwardSnapshotTable(NetBoxTable):
    snapshot_id = tables.Column(linkify=True, verbose_name="Snapshot ID")
    source = tables.Column(linkify=True)
    status = columns.ChoiceFieldColumn()  # ✅ Ensures badge color from choices.py
    date = tables.Column()
    created = tables.Column()
    last_updated = tables.Column()
    tags = columns.TagColumn(url_name="core:datasource_list")
    actions = columns.ActionsColumn(actions=("delete",))

    class Meta(NetBoxTable.Meta):
        model = ForwardSnapshot
        fields = ("pk", "snapshot_id", "source", "status", "date", "created", "last_updated")
        default_columns = ("pk", "snapshot_id", "source", "status", "date")
        empty_text = _("No results found")


class ForwardSourceTable(NetBoxTable):
    name = tables.Column(linkify=True)
    status = columns.ChoiceFieldColumn()  # ✅ Ensures badge color from choices.py
    description = tables.Column()
    comments = tables.Column()
    created = tables.Column()
    last_updated = tables.Column()
    snapshot_count = tables.Column(verbose_name="Snapshots")
    tags = columns.TagColumn(url_name="core:datasource_list")
    actions = columns.ActionsColumn(actions=("edit", "delete"))

    class Meta(NetBoxTable.Meta):
        model = ForwardSource
        fields = ("pk", "id", "name", "status", "description", "comments", "created", "last_updated", "snapshot_count")
        default_columns = ("pk", "name", "status", "description", "snapshot_count")
        empty_text = _("No results found")


class ForwardSyncTable(NetBoxTable):
    name = tables.Column(linkify=True)
    source = tables.Column(accessor="snapshot_data.source", verbose_name="Source", linkify=True)
    snapshot_id = tables.Column(
        verbose_name="Snapshot ID",
        accessor="snapshot_data",
        linkify=lambda record: record.snapshot_data.get_absolute_url() if record.snapshot_data else None,
    )
    scheduled = tables.Column(verbose_name="Schedule")
    last_synced = tables.Column(verbose_name="Last Synced")
    parameters = tables.Column(verbose_name="Enabled Parameters")
    actions = columns.ActionsColumn(actions=("edit", "delete"))

    def render_snapshot_id(self, value):
        return value.snapshot_id if value else "---"

    def render_parameters(self, value):
        if not value:
            return "---"
        return ", ".join(k for k, v in value.items() if v is True)

    class Meta(NetBoxTable.Meta):
        model = ForwardSync
        fields = ("pk", "name", "source", "snapshot_id", "scheduled", "last_synced", "parameters")
        default_columns = ("pk", "name", "source", "snapshot_id", "scheduled", "last_synced", "parameters")
        empty_text = _("No results found")


class ForwardDataTable(NetBoxTable):
    JSON = columns.TemplateColumn(template_code=DATA_BUTTON)
    actions = columns.ActionsColumn(actions=("delete",))

    class Meta(NetBoxTable.Meta):
        model = ForwardData
        fields = ("snapshot_data", "type", "JSON")
        default_columns = ("snapshot_data", "type", "JSON")
        empty_text = _("No results found")