from core.choices import DataSourceStatusChoices
from django.contrib.contenttypes.models import ContentType
from netbox.api.fields import ChoiceField
from netbox.api.fields import ContentTypeField
from netbox.api.fields import RelatedObjectCountField
from netbox.api.serializers import NestedGroupModelSerializer
from netbox_branching.api.serializers import BranchSerializer
from rest_framework import serializers

from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardIngestionIssue
from forward_netbox.models import ForwardRelationshipField
from forward_netbox.models import ForwardRelationshipFieldSourceModels
from forward_netbox.models import ForwardSnapshot
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSupportedSyncModels
from forward_netbox.models import ForwardSync
from forward_netbox.models import ForwardTransformField
from forward_netbox.models import ForwardTransformMap
from forward_netbox.models import ForwardTransformMapGroup

__all__ = (
    "ForwardSyncSerializer",
    "ForwardSnapshotSerializer",
    "ForwardRelationshipFieldSerializer",
    "ForwardTransformFieldSerializer",
    "ForwardTransformMapSerializer",
    "ForwardTransformMapGroupSerializer",
    "ForwardIngestionSerializer",
    "ForwardIngestionIssueSerializer",
    "ForwardSourceSerializer",
)


class ForwardTransformMapGroupSerializer(NestedGroupModelSerializer):
    transform_maps_count = RelatedObjectCountField("transform_maps")

    class Meta:
        model = ForwardTransformMapGroup
        fields = (
            "id",
            "name",
            "description",
            "transform_maps_count",
            "created",
            "last_updated",
        )
        brief_fields = (
            "id",
            "name",
            "description",
        )


class ForwardTransformMapSerializer(NestedGroupModelSerializer):
    group = ForwardTransformMapGroupSerializer(
        nested=True, required=False, allow_null=True
    )
    target_model = ContentTypeField(
        queryset=ContentType.objects.filter(ForwardSupportedSyncModels)
    )

    class Meta:
        model = ForwardTransformMap
        fields = (
            "id",
            "name",
            "group",
            "source_model",
            "target_model",
            "created",
            "last_updated",
        )
        brief_fields = (
            "id",
            "name",
            "group",
            "source_model",
            "target_model",
        )


class ForwardTransformFieldSerializer(NestedGroupModelSerializer):
    transform_map = ForwardTransformMapSerializer(nested=True)

    class Meta:
        model = ForwardTransformField
        fields = (
            "id",
            "transform_map",
            "source_field",
            "target_field",
            "coalesce",
            "template",
        )


class ForwardRelationshipFieldSerializer(NestedGroupModelSerializer):
    transform_map = ForwardTransformMapSerializer(nested=True)
    source_model = ContentTypeField(
        queryset=ContentType.objects.filter(ForwardRelationshipFieldSourceModels)
    )

    class Meta:
        model = ForwardRelationshipField
        fields = (
            "id",
            "transform_map",
            "source_model",
            "target_field",
            "coalesce",
            "template",
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
