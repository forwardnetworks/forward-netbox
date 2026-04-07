from core.api.serializers_.jobs import JobSerializer
from django.core.exceptions import PermissionDenied
from drf_spectacular.utils import extend_schema
from netbox.api.viewsets import NetBoxModelViewSet
from netbox.api.viewsets import NetBoxReadOnlyModelViewSet
from rest_framework.decorators import action
from rest_framework.response import Response

from ..exceptions import ForwardConnectivityError
from ..exceptions import ForwardSyncError
from .serializers import EmptySerializer
from .serializers import ForwardIngestionIssueSerializer
from .serializers import ForwardIngestionSerializer
from .serializers import ForwardNQEMapSerializer
from .serializers import ForwardSourceSerializer
from .serializers import ForwardSyncSerializer
from forward_netbox.filtersets import ForwardNQEMapFilterSet
from forward_netbox.filtersets import ForwardSourceFilterSet
from forward_netbox.filtersets import ForwardSyncFilterSet
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardIngestionIssue
from forward_netbox.models import ForwardNQEMap
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.forward_api import LATEST_PROCESSED_SNAPSHOT


class ForwardSourceViewSet(NetBoxModelViewSet):
    queryset = ForwardSource.objects.all()
    serializer_class = ForwardSourceSerializer
    filterset_class = ForwardSourceFilterSet

    @action(detail=False, methods=["get"], url_path="available-networks")
    def available_networks(self, request):
        source = None
        source_id = request.GET.get("source_id")
        if source_id:
            try:
                source = ForwardSource.objects.get(pk=source_id)
            except (ForwardSource.DoesNotExist, TypeError, ValueError):
                source = None

        if source is None:
            source_type = (
                request.GET.get("type") or ForwardSource._meta.get_field("type").default
            )
            url = request.GET.get("url") or "https://fwd.app"
            if source_type == "saas":
                url = "https://fwd.app"
                verify = True
            else:
                verify = str(request.GET.get("verify", "true")).lower() in {
                    "1",
                    "true",
                    "yes",
                    "on",
                }
            username = request.GET.get("username") or ""
            password = request.GET.get("password") or ""
            if username and password:
                source = ForwardSource(
                    type=source_type,
                    url=url,
                    parameters={
                        "username": username,
                        "password": password,
                        "verify": verify,
                    },
                )
            else:
                return Response(
                    {
                        "count": 0,
                        "results": [],
                        "detail": (
                            "Enter Forward username and password so the plugin can "
                            "load networks."
                        ),
                    }
                )

        q = (request.GET.get("q") or "").strip().lower()
        results = []
        if source is not None:
            try:
                for network in source.get_client().get_networks():
                    if (
                        q
                        and q not in network["label"].lower()
                        and q not in network["id"].lower()
                    ):
                        continue
                    results.append(
                        {
                            "id": network["id"],
                            "name": network["name"],
                            "display": network["label"],
                        }
                    )
            except ForwardSyncError as error:
                message = str(error)
                if isinstance(error, ForwardConnectivityError):
                    detail = (
                        "Could not contact the Forward API endpoint. Check URL reachability, "
                        "DNS/network connectivity, and whether Forward is reachable from this "
                        "NetBox host."
                    )
                elif (
                    "Forward API request failed with HTTP 401" in message
                    or "HTTP 403" in message
                ):
                    detail = (
                        "Could not authenticate to Forward. Verify username and password. "
                        "For new Forward accounts, set the account password in the Forward "
                        "web UI before using NetBox."
                    )
                elif (
                    "Forward credentials are valid, but no networks are available."
                    in message
                ):
                    detail = (
                        "The Forward account is valid but no networks are available for "
                        "the provided credentials."
                    )
                else:
                    detail = (
                        "Could not load networks from this Forward account. "
                        "Check URL, username, and password."
                    )
                return Response(
                    {
                        "count": 0,
                        "results": [],
                        "detail": detail,
                    }
                )
            except Exception:
                return Response(
                    {
                        "count": 0,
                        "results": [],
                        "detail": (
                            "Could not load networks from this Forward account. "
                            "Check URL, username, and password."
                        ),
                    }
                )
        return Response({"count": len(results), "results": results})


class ForwardNQEMapViewSet(NetBoxModelViewSet):
    queryset = ForwardNQEMap.objects.select_related("netbox_model")
    serializer_class = ForwardNQEMapSerializer
    filterset_class = ForwardNQEMapFilterSet


class ForwardSyncViewSet(NetBoxModelViewSet):
    queryset = ForwardSync.objects.all()
    serializer_class = ForwardSyncSerializer
    filterset_class = ForwardSyncFilterSet

    @action(detail=False, methods=["get"], url_path="available-snapshots")
    def available_snapshots(self, request):
        source_id = request.GET.get("source_id")
        q = (request.GET.get("q") or "").strip().lower()
        results = [
            {
                "id": LATEST_PROCESSED_SNAPSHOT,
                "name": "latestProcessed",
                "display": "latestProcessed",
            }
        ]
        try:
            source = ForwardSource.objects.get(pk=source_id)
        except (ForwardSource.DoesNotExist, TypeError, ValueError):
            return Response({"count": len(results), "results": results})

        network_id = (source.parameters or {}).get("network_id")
        if not network_id:
            return Response({"count": len(results), "results": results})

        try:
            for snapshot in source.get_client().get_snapshots(network_id):
                display = snapshot["label"]
                if q and q not in display.lower() and q not in snapshot["id"].lower():
                    continue
                results.append(
                    {
                        "id": snapshot["id"],
                        "name": snapshot["id"],
                        "display": display,
                    }
                )
        except Exception:
            pass
        return Response({"count": len(results), "results": results})

    @extend_schema(
        methods=["post"], request=EmptySerializer(), responses={201: JobSerializer()}
    )
    @action(detail=True, methods=["post"])
    def sync(self, request, pk):
        if not request.user.has_perm("forward_netbox.run_forwardsync"):
            raise PermissionDenied(
                "This user does not have permission to run a Forward sync."
            )
        sync = self.get_object()
        job = sync.enqueue_sync_job(user=request.user, adhoc=True)
        return Response(
            JobSerializer(job, context={"request": request}).data, status=201
        )


class ForwardIngestionViewSet(NetBoxReadOnlyModelViewSet):
    queryset = ForwardIngestion.objects.all()
    serializer_class = ForwardIngestionSerializer


class ForwardIngestionIssueViewSet(NetBoxReadOnlyModelViewSet):
    queryset = ForwardIngestionIssue.objects.all()
    serializer_class = ForwardIngestionIssueSerializer
