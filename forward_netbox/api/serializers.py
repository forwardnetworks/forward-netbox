from django.contrib.contenttypes.models import ContentType

from core.choices import DataSourceStatusChoices
from netbox.api.fields import ChoiceField
from netbox.api.fields import ContentTypeField
from netbox.api.serializers import NestedGroupModelSerializer
from netbox_branching.api.serializers import BranchSerializer
from rest_framework import serializers

from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardIngestionIssue
from forward_netbox.models import ForwardSnapshot
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.models import ForwardNQEQuery

__all__ = (
    "ForwardSyncSerializer",
    "ForwardSnapshotSerializer",
    "ForwardIngestionSerializer",
    "ForwardIngestionIssueSerializer",
    "ForwardSourceSerializer",
    "ForwardNQEQuerySerializer",
)


class ForwardNQEQuerySerializer(NestedGroupModelSerializer):
    content_type = ContentTypeField(
        queryset=ContentType.objects.filter(app_label__in=["dcim", "ipam"])
    )

    class Meta:
        model = ForwardNQEQuery
        fields = (
            "id",
            "display",
            "content_type",
            "query_id",
            "enabled",
            "description",
            "tags",
            "created",
            "last_updated",
        )
        brief_fields = (
            "id",
            "display",
            "content_type",
            "query_id",
            "enabled",
        )


class ForwardSourceSerializer(NestedGroupModelSerializer):
    status = ChoiceField(choices=DataSourceStatusChoices, read_only=True)
    url = serializers.URLField()

    class Meta:
        model = ForwardSource
        fields = (
            "id",
            "url",
            "display",
            "name",
            "type",
            "status",
            "last_synced",
            "network_id",
            "description",
            "comments",
            "parameters",
            "created",
            "last_updated",
        )
        brief_fields = (
            "display",
            "id",
            "name",
            "status",
            "type",
            "url",
            "network_id",
        )


class ForwardSnapshotSerializer(NestedGroupModelSerializer):
    source = ForwardSourceSerializer(nested=True, read_only=True)
    data = serializers.JSONField()
    display = serializers.CharField(source="__str__", read_only=True)

    class Meta:
        model = ForwardSnapshot
        fields = (
            "id",
            "display",
            "name",
            "source",
            "snapshot_id",
            "status",
            "data",
            "date",
            "created",
            "last_updated",
        )
        brief_fields = (
            "display",
            "id",
            "name",
            "source",
            "snapshot_id",
            "status",
            "data",
            "date",
        )


class ForwardSyncSerializer(NestedGroupModelSerializer):
    snapshot_data = ForwardSnapshotSerializer(nested=True)

    class Meta:
        model = ForwardSync
        fields = (
            "id",
            "name",
            "snapshot_data",
            "status",
            "parameters",
            "auto_merge",
            "last_synced",
            "scheduled",
            "interval",
            "user",
        )
        brief_fields = (
            "auto_merge",
            "id",
            "last_synced",
            "name",
            "parameters",
            "status",
        )


class ForwardIngestionSerializer(NestedGroupModelSerializer):
    branch = BranchSerializer(read_only=True)
    sync = ForwardSyncSerializer(nested=True)

    class Meta:
        model = ForwardIngestion
        fields = (
            "id",
            "name",
            "branch",
            "sync",
        )
        brief_fields = (
            "id",
            "name",
            "branch",
            "sync",
        )


class ForwardIngestionIssueSerializer(NestedGroupModelSerializer):
    ingestion = ForwardIngestionSerializer(nested=True)

    class Meta:
        model = ForwardIngestionIssue
        fields = (
            "id",
            "ingestion",
            "timestamp",
            "model",
            "message",
            "raw_data",
            "coalesce_fields",
            "defaults",
            "exception",
        )
        brief_fields = (
            "exception",
            "id",
            "ingestion",
            "message",
            "model",
        )
