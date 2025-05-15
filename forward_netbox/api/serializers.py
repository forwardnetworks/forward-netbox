from core.choices import DataSourceStatusChoices
from netbox.api.fields import ChoiceField, ContentTypeField
from netbox.api.serializers import NetBoxModelSerializer
from rest_framework import serializers
from users.api.serializers_.nested import NestedUserSerializer

from .nested_serializers import (
    NestedForwardSnapshotSerializer,
    NestedForwardSourceSerializer,
    NestedForwardNQEMapSerializer,
)
from forward_netbox.models import (
    ForwardSnapshot,
    ForwardSource,
    ForwardSync,
    ForwardNQEMap,
    ForwardData,
)


class ForwardSyncSerializer(NetBoxModelSerializer):
    snapshot_data = NestedForwardSnapshotSerializer(read_only=True)

    class Meta:
        model = ForwardSync
        fields = [
            "id", "name", "display", "snapshot_data", "status", "parameters",
            "last_synced", "created", "last_updated",
        ]


class ForwardSnapshotSerializer(NetBoxModelSerializer):
    source = NestedForwardSourceSerializer()
    data = serializers.JSONField(read_only=True)
    include_data = serializers.BooleanField(write_only=True, required=False, default=False)

    class Meta:
        model = ForwardSnapshot
        fields = [
            "id", "source", "snapshot_id", "status", "date", "display", "data",
            "include_data", "created", "last_updated",
        ]

    def to_representation(self, instance):
        rep = super().to_representation(instance)
        if not self.context["request"].query_params.get("include_data", False):
            rep.pop("data", None)
        return rep


class ForwardNQEMapSerializer(NetBoxModelSerializer):
    netbox_model = ContentTypeField(read_only=True)

    class Meta:
        model = ForwardNQEMap
        fields = ["id", "query_id", "netbox_model", "created", "last_updated"]


class ForwardSourceSerializer(NetBoxModelSerializer):
    status = ChoiceField(choices=DataSourceStatusChoices)
    url = serializers.URLField()

    class Meta:
        model = ForwardSource
        fields = [
            "id", "url", "display", "name", "status", "last_synced",
            "description", "comments", "parameters", "created", "last_updated",
        ]


class ForwardDataSerializer(NetBoxModelSerializer):
    snapshot_data = NestedForwardSnapshotSerializer()
    data = serializers.JSONField()

    class Meta:
        model = ForwardData
        fields = ["id", "snapshot_data", "type", "data"]
