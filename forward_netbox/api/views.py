from django.db import transaction
from netbox.api.viewsets import NetBoxModelViewSet, NetBoxReadOnlyModelViewSet
from rest_framework.decorators import action
from rest_framework.response import Response
from utilities.query import count_related

from forward_netbox.models import (
    ForwardSnapshot,
    ForwardSource,
    ForwardSync,
    ForwardNQEMap,
)
from forward_netbox.filtersets import (
    ForwardSnapshotFilterSet,
    ForwardSourceFilterSet,
)
from forward_netbox.api.serializers import (
    ForwardSnapshotSerializer,
    ForwardSourceSerializer,
    ForwardSyncSerializer,
    ForwardNQEMapSerializer,
)


class ForwardSourceViewSet(NetBoxModelViewSet):
    queryset = ForwardSource.objects.annotate(
        snapshot_count=count_related(ForwardSnapshot, "source")
    )
    serializer_class = ForwardSourceSerializer
    filterset_class = ForwardSourceFilterSet


class ForwardSnapshotViewSet(NetBoxModelViewSet):
    queryset = ForwardSnapshot.objects.all()
    serializer_class = ForwardSnapshotSerializer
    filterset_class = ForwardSnapshotFilterSet

    def get_queryset(self):
        queryset = super().get_queryset()
        request = self.request

        source_id = request.query_params.get("source_id")
        status = request.query_params.get("status")

        if source_id:
            queryset = queryset.filter(source_id=source_id)
        if status:
            queryset = queryset.filter(status__iexact=status)

        return queryset

    @action(detail=True, methods=["get"], url_path="sites")
    def sites(self, request, pk=None):
        return Response({"detail": "Not implemented"}, status=501)

    @action(detail=True, methods=["patch", "delete"], url_path="raw")
    def raw(self, request, pk=None):
        return Response({"detail": "Not implemented"}, status=501)


class ForwardSyncViewSet(NetBoxReadOnlyModelViewSet):
    queryset = ForwardSync.objects.all()
    serializer_class = ForwardSyncSerializer


class ForwardNQEMapViewSet(NetBoxModelViewSet):
    queryset = ForwardNQEMap.objects.all()
    serializer_class = ForwardNQEMapSerializer
