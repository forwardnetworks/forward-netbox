import json

import django_tables2 as tables
from django.utils.html import format_html
from django.utils.translation import gettext_lazy as _
from netbox.tables import columns
from netbox.tables import NetBoxTable
from netbox_branching.models import ChangeDiff

from .models import ForwardDriftPolicy
from .models import ForwardIngestion
from .models import ForwardIngestionIssue
from .models import ForwardNQEMap
from .models import ForwardSource
from .models import ForwardSync
from .models import ForwardValidationRun


DIFF_BUTTON = """
    <a href="#"
          hx-get="{% url 'plugins:forward_netbox:forwardingestion_change_diff' pk=record.branch.forwardingestion.pk change_pk=record.pk %}"
          hx-target="#htmx-modal-content"
          data-bs-toggle="modal"
          data-bs-target="#htmx-modal"
          class="btn btn-success btn-sm"
        >
        <i class="mdi mdi-code-tags">Diff</i>
    </a>
"""


class ForwardSourceTable(NetBoxTable):
    name = tables.Column(linkify=True)
    status = columns.ChoiceFieldColumn()
    type = columns.ChoiceFieldColumn()

    class Meta(NetBoxTable.Meta):
        model = ForwardSource
        fields = ("pk", "name", "status", "type", "url", "description", "last_synced")
        default_columns = ("pk", "name", "status", "type", "url", "last_synced")


class ForwardNQEMapTable(NetBoxTable):
    name = tables.Column(linkify=True)
    netbox_model = columns.ContentTypeColumn(verbose_name=_("NetBox Model"))
    execution_mode = tables.Column(verbose_name=_("Execution"))
    execution_value = tables.Column(verbose_name=_("Query ID or Query Name"))

    class Meta(NetBoxTable.Meta):
        model = ForwardNQEMap
        fields = (
            "name",
            "netbox_model",
            "execution_mode",
            "execution_value",
            "coalesce_fields",
            "commit_id",
            "enabled",
            "built_in",
            "weight",
        )
        default_columns = (
            "name",
            "netbox_model",
            "execution_mode",
            "execution_value",
            "enabled",
            "weight",
        )


class ForwardSyncTable(NetBoxTable):
    name = tables.Column(linkify=True)
    status = columns.ChoiceFieldColumn()
    source = tables.Column(linkify=True, verbose_name=_("Source"))
    last_ingestion = tables.Column(accessor="last_ingestion", linkify=True)
    drift_policy = tables.Column(linkify=True, verbose_name=_("Drift Policy"))

    def render_last_ingestion(self, value):
        return getattr(value, "name", "---") if value else "---"

    class Meta(NetBoxTable.Meta):
        model = ForwardSync
        fields = (
            "pk",
            "name",
            "status",
            "source",
            "auto_merge",
            "drift_policy",
            "last_synced",
            "last_ingestion",
            "scheduled",
            "interval",
            "user",
        )
        default_columns = (
            "pk",
            "name",
            "status",
            "source",
            "scheduled",
            "auto_merge",
            "drift_policy",
            "last_ingestion",
            "last_synced",
        )


class ForwardIngestionTable(NetBoxTable):
    name = tables.Column(linkify=True, order_by=("branch_name", "sync_name", "id"))
    sync = tables.Column(linkify=True)
    branch = tables.Column(linkify=True)
    validation_run = tables.Column(linkify=True)
    changes = tables.Column(
        accessor="staged_changes",
        verbose_name=_("Number of Changes"),
    )
    actions = columns.ActionsColumn(actions=("delete",))

    def render_name(self, record):
        if getattr(record, "branch_name", None):
            return record.branch_name
        if getattr(record, "sync_name", None):
            return f"{record.sync_name} (Ingestion {record.pk})"
        return f"Ingestion {record.pk}"

    class Meta(NetBoxTable.Meta):
        model = ForwardIngestion
        fields = ("name", "sync", "branch", "validation_run", "user", "changes")
        default_columns = ("name", "sync", "branch", "user", "changes")


class ForwardDriftPolicyTable(NetBoxTable):
    name = tables.Column(linkify=True)
    baseline_mode = columns.ChoiceFieldColumn()

    class Meta(NetBoxTable.Meta):
        model = ForwardDriftPolicy
        fields = (
            "pk",
            "name",
            "enabled",
            "baseline_mode",
            "require_processed_snapshot",
            "block_on_query_errors",
            "block_on_zero_rows",
            "max_deleted_objects",
            "max_deleted_percent",
        )
        default_columns = (
            "pk",
            "name",
            "enabled",
            "baseline_mode",
            "require_processed_snapshot",
            "block_on_query_errors",
        )


class ForwardValidationRunTable(NetBoxTable):
    sync = tables.Column(linkify=True)
    policy = tables.Column(linkify=True)
    status = columns.ChoiceFieldColumn()
    actions = columns.ActionsColumn(actions=("delete",))

    class Meta(NetBoxTable.Meta):
        model = ForwardValidationRun
        fields = (
            "pk",
            "sync",
            "policy",
            "status",
            "allowed",
            "snapshot_id",
            "baseline_snapshot_id",
            "created",
            "completed",
        )
        default_columns = (
            "pk",
            "sync",
            "status",
            "allowed",
            "snapshot_id",
            "baseline_snapshot_id",
            "completed",
        )


class ForwardIngestionChangesTable(NetBoxTable):
    id = tables.Column(verbose_name=_("ID"))
    pk = None
    object_type = tables.Column(
        accessor="object_type.model",
        verbose_name=_("Object Type"),
    )
    object = tables.Column(verbose_name=_("Object"), order_by="object_repr")
    actions = None
    diffs = columns.TemplateColumn(template_code=DIFF_BUTTON, orderable=False)

    def render_object(self, value, record):
        if value and hasattr(value, "get_absolute_url"):
            label = (
                getattr(value, "name", None)
                or getattr(value, "model", None)
                or getattr(value, "address", None)
                or getattr(value, "prefix", None)
                or getattr(value, "mac_address", None)
            )
            if label:
                return format_html(
                    "<a href='{}'>{}</a>", value.get_absolute_url(), label
                )
        return record.object_repr

    class Meta(NetBoxTable.Meta):
        model = ChangeDiff
        fields = ("object", "action", "object_type", "diffs")
        default_columns = ("object", "action", "object_type", "diffs")


class ForwardIngestionIssueTable(NetBoxTable):
    phase = columns.ChoiceFieldColumn()
    coalesce_fields = tables.Column(verbose_name=_("Coalesce Fields"))
    defaults = tables.Column(verbose_name=_("Defaults"))
    actions = None

    def _render_json(self, value):
        payload = value or {}
        return format_html(
            "<code>{}</code>",
            json.dumps(payload, sort_keys=True, separators=(",", ":")),
        )

    def render_coalesce_fields(self, value):
        return self._render_json(value)

    def render_defaults(self, value):
        return self._render_json(value)

    class Meta(NetBoxTable.Meta):
        model = ForwardIngestionIssue
        fields = (
            "timestamp",
            "phase",
            "model",
            "exception",
            "coalesce_fields",
            "defaults",
            "message",
        )
        default_columns = (
            "timestamp",
            "phase",
            "model",
            "exception",
            "coalesce_fields",
            "defaults",
            "message",
        )
