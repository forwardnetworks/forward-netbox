from core.choices import DataSourceStatusChoices
from django.contrib.contenttypes.models import ContentType
from netbox.api.fields import ChoiceField
from netbox.api.fields import ContentTypeField
from netbox.api.fields import RelatedObjectCountField
from netbox.api.serializers import NestedGroupModelSerializer
from netbox_branching.api.serializers import BranchSerializer
from rest_framework import serializers

from ipfabric_netbox.models import IPFabricIngestion
from ipfabric_netbox.models import IPFabricIngestionIssue
from ipfabric_netbox.models import IPFabricRelationshipField
from ipfabric_netbox.models import IPFabricRelationshipFieldSourceModels
from ipfabric_netbox.models import IPFabricSnapshot
from ipfabric_netbox.models import IPFabricSource
from ipfabric_netbox.models import IPFabricSupportedSyncModels
from ipfabric_netbox.models import IPFabricSync
from ipfabric_netbox.models import IPFabricTransformField
from ipfabric_netbox.models import IPFabricTransformMap
from ipfabric_netbox.models import IPFabricTransformMapGroup

__all__ = (
    "IPFabricSyncSerializer",
    "IPFabricSnapshotSerializer",
    "IPFabricRelationshipFieldSerializer",
    "IPFabricTransformFieldSerializer",
    "IPFabricTransformMapSerializer",
    "IPFabricTransformMapGroupSerializer",
    "IPFabricIngestionSerializer",
    "IPFabricIngestionIssueSerializer",
    "IPFabricSourceSerializer",
)


class IPFabricTransformMapGroupSerializer(NestedGroupModelSerializer):
    transform_maps_count = RelatedObjectCountField("transform_maps")

    class Meta:
        model = IPFabricTransformMapGroup
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


class IPFabricTransformMapSerializer(NestedGroupModelSerializer):
    group = IPFabricTransformMapGroupSerializer(
        nested=True, required=False, allow_null=True
    )
    target_model = ContentTypeField(
        queryset=ContentType.objects.filter(IPFabricSupportedSyncModels)
    )

    class Meta:
        model = IPFabricTransformMap
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


class IPFabricTransformFieldSerializer(NestedGroupModelSerializer):
    transform_map = IPFabricTransformMapSerializer(nested=True)

    class Meta:
        model = IPFabricTransformField
        fields = (
            "id",
            "transform_map",
            "source_field",
            "target_field",
            "coalesce",
            "template",
        )


class IPFabricRelationshipFieldSerializer(NestedGroupModelSerializer):
    transform_map = IPFabricTransformMapSerializer(nested=True)
    source_model = ContentTypeField(
        queryset=ContentType.objects.filter(IPFabricRelationshipFieldSourceModels)
    )

    class Meta:
        model = IPFabricRelationshipField
        fields = (
            "id",
            "transform_map",
            "source_model",
            "target_field",
            "coalesce",
            "template",
        )


class IPFabricSourceSerializer(NestedGroupModelSerializer):
    status = ChoiceField(choices=DataSourceStatusChoices, read_only=True)
    url = serializers.URLField()

    class Meta:
        model = IPFabricSource
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


class IPFabricSnapshotSerializer(NestedGroupModelSerializer):
    source = IPFabricSourceSerializer(nested=True, read_only=True)
    data = serializers.JSONField()
    display = serializers.CharField(source="__str__", read_only=True)

    class Meta:
        model = IPFabricSnapshot
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


class IPFabricSyncSerializer(NestedGroupModelSerializer):
    snapshot_data = IPFabricSnapshotSerializer(nested=True)

    class Meta:
        model = IPFabricSync
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


class IPFabricIngestionSerializer(NestedGroupModelSerializer):
    branch = BranchSerializer(read_only=True)
    sync = IPFabricSyncSerializer(nested=True)

    class Meta:
        model = IPFabricIngestion
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


class IPFabricIngestionIssueSerializer(NestedGroupModelSerializer):
    ingestion = IPFabricIngestionSerializer(nested=True)

    class Meta:
        model = IPFabricIngestionIssue
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
