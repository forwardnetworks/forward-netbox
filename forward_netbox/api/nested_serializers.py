from drf_spectacular.types import OpenApiTypes
from drf_spectacular.utils import extend_schema_field
from netbox.api.fields import ContentTypeField
from netbox.api.serializers import NetBoxModelSerializer, WritableNestedSerializer
from rest_framework import serializers

from forward_netbox.models import (
    ForwardSnapshot,
    ForwardSource,
    ForwardSync,
    ForwardNQEMap,
)

__all__ = (
    "NestedForwardSourceSerializer",
    "NestedForwardSnapshotSerializer",
    "NestedForwardNQEMapSerializer",
    "NestedForwardSyncSerializer",
)


class NestedForwardSourceSerializer(WritableNestedSerializer):
    url = serializers.HyperlinkedIdentityField(view_name="plugins-api:forward_netbox-api:forwardsource-detail")

    class Meta:
        model = ForwardSource
        fields = ["id", "url", "display", "name"]


class NestedForwardSnapshotSerializer(NetBoxModelSerializer):
    source = NestedForwardSourceSerializer(read_only=True)
    display = serializers.SerializerMethodField(read_only=True)

    class Meta:
        model = ForwardSnapshot
        fields = ["id", "source", "snapshot_id", "status", "date", "display"]

    @extend_schema_field(OpenApiTypes.STR)
    def get_display(self, obj):
        return f"{obj.source.name} ({obj.snapshot_id})"


class NestedForwardSyncSerializer(NetBoxModelSerializer):
    snapshot_data = NestedForwardSnapshotSerializer(read_only=True)
    display = serializers.SerializerMethodField(read_only=True)

    class Meta:
        model = ForwardSync
        fields = [
            "id",
            "name",
            "display",
            "snapshot_data",
            "type",
            "status",
            "parameters",
            "last_synced",
        ]

    @extend_schema_field(OpenApiTypes.STR)
    def get_display(self, obj):
        return obj.name


class NestedForwardNQEMapSerializer(NetBoxModelSerializer):
    netbox_model = ContentTypeField(read_only=True)

    class Meta:
        model = ForwardNQEMap
        fields = ["id", "query_id", "netbox_model"]
