from django.db import transaction
from netbox.api.viewsets import NetBoxModelViewSet
from netbox.api.viewsets import NetBoxReadOnlyModelViewSet
from rest_framework.decorators import action
from rest_framework.response import Response
from utilities.query import count_related

from .serializers import IPFabricIngestionIssueSerializer
from .serializers import IPFabricIngestionSerializer
from .serializers import IPFabricRelationshipFieldSerializer
from .serializers import IPFabricSnapshotSerializer
from .serializers import IPFabricSourceSerializer
from .serializers import IPFabricSyncSerializer
from .serializers import IPFabricTransformFieldSerializer
from .serializers import IPFabricTransformMapGroupSerializer
from .serializers import IPFabricTransformMapSerializer
from ipfabric_netbox.filtersets import IPFabricRelationshipFieldFilterSet
from ipfabric_netbox.filtersets import IPFabricSnapshotFilterSet
from ipfabric_netbox.filtersets import IPFabricSourceFilterSet
from ipfabric_netbox.filtersets import IPFabricTransformFieldFilterSet
from ipfabric_netbox.models import IPFabricData
from ipfabric_netbox.models import IPFabricIngestion
from ipfabric_netbox.models import IPFabricIngestionIssue
from ipfabric_netbox.models import IPFabricRelationshipField
from ipfabric_netbox.models import IPFabricSnapshot
from ipfabric_netbox.models import IPFabricSource
from ipfabric_netbox.models import IPFabricSync
from ipfabric_netbox.models import IPFabricTransformField
from ipfabric_netbox.models import IPFabricTransformMap
from ipfabric_netbox.models import IPFabricTransformMapGroup


class IPFabricTransformMapGroupViewSet(NetBoxModelViewSet):
    queryset = IPFabricTransformMapGroup.objects.all()
    serializer_class = IPFabricTransformMapGroupSerializer


class IPFabricTransformMapViewSet(NetBoxModelViewSet):
    queryset = IPFabricTransformMap.objects.all()
    serializer_class = IPFabricTransformMapSerializer


class IPFabricTransformFieldViewSet(NetBoxModelViewSet):
    queryset = IPFabricTransformField.objects.all()
    serializer_class = IPFabricTransformFieldSerializer
    filterset_class = IPFabricTransformFieldFilterSet


class IPFabricRelationshipFieldViewSet(NetBoxModelViewSet):
    queryset = IPFabricRelationshipField.objects.all()
    serializer_class = IPFabricRelationshipFieldSerializer
    filterset_class = IPFabricRelationshipFieldFilterSet


class IPFabricSyncViewSet(NetBoxModelViewSet):
    queryset = IPFabricSync.objects.all()
    serializer_class = IPFabricSyncSerializer


class IPFabricIngestionViewSet(NetBoxReadOnlyModelViewSet):
    queryset = IPFabricIngestion.objects.all()
    serializer_class = IPFabricIngestionSerializer


class IPFabricIngestionIssueViewSet(NetBoxReadOnlyModelViewSet):
    queryset = IPFabricIngestionIssue.objects.all()
    serializer_class = IPFabricIngestionIssueSerializer


class IPFabricSnapshotViewSet(NetBoxReadOnlyModelViewSet):
    queryset = IPFabricSnapshot.objects.all()
    serializer_class = IPFabricSnapshotSerializer
    filterset_class = IPFabricSnapshotFilterSet

    @action(detail=True, methods=["patch", "delete"], url_path="raw")
    def raw(self, request, pk):
        snapshot = self.get_object()
        if request.method == "DELETE":
            raw_data = IPFabricData.objects.filter(snapshot_data=snapshot)
            raw_data._raw_delete(raw_data.db)
            return Response({"status": "success"})
        elif request.method == "PATCH":
            with transaction.atomic():
                IPFabricData.objects.bulk_create(
                    [
                        IPFabricData(snapshot_data=snapshot, data=item["data"])
                        for item in request.data["data"]
                    ],
                    batch_size=5000,
                )
            return Response({"status": "success"})

    @action(detail=True, methods=["get"], url_path="sites")
    def sites(self, request, pk):
        q = request.GET.get("q", None)
        snapshot = IPFabricSnapshot.objects.get(pk=pk)
        new_sites = {"count": 0, "results": []}
        if snapshot.data:
            sites = snapshot.data.get("sites", None)
            num = 0
            if sites:
                for site in sites:
                    if q:
                        if q.lower() in site.lower():
                            new_sites["results"].append(
                                {"display": site, "name": site, "id": site}
                            )
                    else:
                        new_sites["results"].append(
                            {"display": site, "name": site, "id": site}
                        )
                    num += 1
                new_sites["count"] = num
                return Response(new_sites)
        else:
            return Response([])


class IPFabricSourceViewSet(NetBoxModelViewSet):
    queryset = IPFabricSource.objects.annotate(
        snapshot_count=count_related(IPFabricSnapshot, "source")
    )
    serializer_class = IPFabricSourceSerializer
    filterset_class = IPFabricSourceFilterSet
