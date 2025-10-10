from django.db import transaction
from netbox.api.viewsets import NetBoxModelViewSet
from netbox.api.viewsets import NetBoxReadOnlyModelViewSet
from rest_framework.decorators import action
from rest_framework.response import Response
from utilities.query import count_related

from .serializers import ForwardIngestionIssueSerializer
from .serializers import ForwardIngestionSerializer
from .serializers import ForwardRelationshipFieldSerializer
from .serializers import ForwardSnapshotSerializer
from .serializers import ForwardSourceSerializer
from .serializers import ForwardSyncSerializer
from .serializers import ForwardTransformFieldSerializer
from .serializers import ForwardTransformMapGroupSerializer
from .serializers import ForwardTransformMapSerializer
from forward_netbox.filtersets import ForwardRelationshipFieldFilterSet
from forward_netbox.filtersets import ForwardSnapshotFilterSet
from forward_netbox.filtersets import ForwardSourceFilterSet
from forward_netbox.filtersets import ForwardTransformFieldFilterSet
from forward_netbox.models import ForwardData
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardIngestionIssue
from forward_netbox.models import ForwardRelationshipField
from forward_netbox.models import ForwardSnapshot
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.models import ForwardTransformField
from forward_netbox.models import ForwardTransformMap
from forward_netbox.models import ForwardTransformMapGroup


class ForwardTransformMapGroupViewSet(NetBoxModelViewSet):
    queryset = ForwardTransformMapGroup.objects.all()
    serializer_class = ForwardTransformMapGroupSerializer


class ForwardTransformMapViewSet(NetBoxModelViewSet):
    queryset = ForwardTransformMap.objects.all()
    serializer_class = ForwardTransformMapSerializer


class ForwardTransformFieldViewSet(NetBoxModelViewSet):
    queryset = ForwardTransformField.objects.all()
    serializer_class = ForwardTransformFieldSerializer
    filterset_class = ForwardTransformFieldFilterSet


class ForwardRelationshipFieldViewSet(NetBoxModelViewSet):
    queryset = ForwardRelationshipField.objects.all()
    serializer_class = ForwardRelationshipFieldSerializer
    filterset_class = ForwardRelationshipFieldFilterSet


class ForwardSyncViewSet(NetBoxModelViewSet):
    queryset = ForwardSync.objects.all()
    serializer_class = ForwardSyncSerializer


class ForwardIngestionViewSet(NetBoxReadOnlyModelViewSet):
    queryset = ForwardIngestion.objects.all()
    serializer_class = ForwardIngestionSerializer


class ForwardIngestionIssueViewSet(NetBoxReadOnlyModelViewSet):
    queryset = ForwardIngestionIssue.objects.all()
    serializer_class = ForwardIngestionIssueSerializer


class ForwardSnapshotViewSet(NetBoxReadOnlyModelViewSet):
    queryset = ForwardSnapshot.objects.all()
    serializer_class = ForwardSnapshotSerializer
    filterset_class = ForwardSnapshotFilterSet

    @action(detail=True, methods=["patch", "delete"], url_path="raw")
    def raw(self, request, pk):
        snapshot = self.get_object()
        if request.method == "DELETE":
            raw_data = ForwardData.objects.filter(snapshot_data=snapshot)
            raw_data._raw_delete(raw_data.db)
            return Response({"status": "success"})
        elif request.method == "PATCH":
            with transaction.atomic():
                ForwardData.objects.bulk_create(
                    [
                        ForwardData(snapshot_data=snapshot, data=item["data"])
                        for item in request.data["data"]
                    ],
                    batch_size=5000,
                )
            return Response({"status": "success"})

    @action(detail=True, methods=["get"], url_path="sites")
    def sites(self, request, pk):
        q = request.GET.get("q", None)
        snapshot = ForwardSnapshot.objects.get(pk=pk)
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


class ForwardSourceViewSet(NetBoxModelViewSet):
    queryset = ForwardSource.objects.annotate(
        snapshot_count=count_related(ForwardSnapshot, "source")
    )
    serializer_class = ForwardSourceSerializer
    filterset_class = ForwardSourceFilterSet
