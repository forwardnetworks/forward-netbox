from core.api.serializers_.jobs import JobSerializer
from core.exceptions import SyncError
from django.core.exceptions import PermissionDenied
from drf_spectacular.utils import extend_schema
from netbox.api.viewsets import NetBoxModelViewSet
from netbox.api.viewsets import NetBoxReadOnlyModelViewSet
from rest_framework import status
from rest_framework.decorators import action
from rest_framework.response import Response

from ..exceptions import ForwardConnectivityError
from ..exceptions import ForwardSyncError
from .serializers import EmptySerializer
from .serializers import ForwardDriftPolicySerializer
from .serializers import ForwardIngestionIssueSerializer
from .serializers import ForwardIngestionSerializer
from .serializers import ForwardNQEMapSerializer
from .serializers import ForwardSourceSerializer
from .serializers import ForwardSyncSerializer
from .serializers import ForwardValidationRunOverrideSerializer
from .serializers import ForwardValidationRunSerializer
from forward_netbox.filtersets import ForwardDriftPolicyFilterSet
from forward_netbox.filtersets import ForwardNQEMapFilterSet
from forward_netbox.filtersets import ForwardSourceFilterSet
from forward_netbox.filtersets import ForwardSyncFilterSet
from forward_netbox.filtersets import ForwardValidationRunFilterSet
from forward_netbox.models import ForwardDriftPolicy
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardIngestionIssue
from forward_netbox.models import ForwardNQEMap
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.models import ForwardValidationRun
from forward_netbox.utilities.forward_api import LATEST_PROCESSED_SNAPSHOT
from forward_netbox.utilities.query_binding import builtin_filename_to_query_default
from forward_netbox.utilities.query_binding import query_filename_from_path


NQE_REPOSITORY_LABELS = {
    "org": "Org Repository",
    "fwd": "Forward Library",
}


def _normalize_query_directory(directory):
    directory = str(directory or "/").strip() or "/"
    if not directory.startswith("/"):
        directory = f"/{directory}"
    if not directory.endswith("/"):
        directory = f"{directory}/"
    return directory


def _query_parent_directories(query_path):
    parts = [part for part in str(query_path or "").strip("/").split("/")[:-1] if part]
    directories = ["/"]
    current = ""
    for part in parts:
        current = f"{current}/{part}"
        directories.append(f"{current}/")
    return directories


def _commit_display(commit):
    commit_id = str(commit.get("id") or "").strip()
    committed_at = str(commit.get("committedAt") or "").strip()
    message = commit.get("message") or {}
    if isinstance(message, dict):
        subject = str(message.get("subject") or message.get("summary") or "").strip()
    else:
        subject = str(message or "").strip()
    if not subject:
        subject = str(commit.get("title") or "").strip()
    if len(subject) > 140:
        subject = f"{subject[:137]}..."
    short_commit_id = commit_id[:12] if commit_id else ""
    parts = [part for part in (short_commit_id, committed_at, subject) if part]
    return " | ".join(parts) or commit_id


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

    @action(detail=False, methods=["get"], url_path="available-query-folders")
    def available_query_folders(self, request):
        source_id = request.GET.get("source_id")
        repository = (request.GET.get("repository") or "org").strip().lower()
        q = (request.GET.get("q") or "").strip().lower()
        try:
            source = ForwardSource.objects.get(pk=source_id)
        except (ForwardSource.DoesNotExist, TypeError, ValueError):
            return Response(
                {
                    "count": 0,
                    "results": [],
                    "detail": "Select a Forward Source to load query folders.",
                }
            )

        try:
            queries = source.get_client().get_nqe_repository_queries(
                repository=repository,
                directory="/",
            )
        except Exception:
            return Response(
                {
                    "count": 0,
                    "results": [],
                    "detail": "Could not load Forward query folders from this source.",
                }
            )

        folders = sorted(
            {
                folder
                for query in queries
                for folder in _query_parent_directories(query.get("path"))
            }
        )
        results = []
        repository_label = NQE_REPOSITORY_LABELS.get(repository, repository)
        for folder in folders:
            display = f"{repository_label} {folder}"
            if q and q not in display.lower():
                continue
            results.append(
                {
                    "id": folder,
                    "name": folder,
                    "display": display,
                }
            )
        return Response({"count": len(results), "results": results})

    @action(detail=False, methods=["get"], url_path="available-queries")
    def available_queries(self, request):
        source_id = request.GET.get("source_id")
        repository = (request.GET.get("repository") or "org").strip().lower()
        directory = _normalize_query_directory(request.GET.get("directory") or "/")
        value_mode = (request.GET.get("value_mode") or "path").strip().lower()
        model_string = (request.GET.get("model_string") or "").strip().lower()
        q = (request.GET.get("q") or "").strip().lower()
        try:
            source = ForwardSource.objects.get(pk=source_id)
        except (ForwardSource.DoesNotExist, TypeError, ValueError):
            return Response(
                {
                    "count": 0,
                    "results": [],
                    "detail": "Select a Forward Source to load Org Repository queries.",
                }
            )

        results = []
        try:
            queries = source.get_client().get_nqe_repository_queries(
                repository=repository,
                directory=directory,
            )
        except Exception:
            return Response(
                {
                    "count": 0,
                    "results": [],
                    "detail": "Could not load Forward queries from this source.",
                }
            )

        repository_label = NQE_REPOSITORY_LABELS.get(repository, repository)
        filename_to_query_default = builtin_filename_to_query_default()
        for query in queries:
            query_id = str(query.get("queryId") or "").strip()
            path = str(query.get("path") or "").strip()
            intent = str(query.get("intent") or "").strip()
            last_commit_id = str(query.get("lastCommitId") or "").strip()
            if not query_id or not path:
                continue
            query_default = filename_to_query_default.get(
                query_filename_from_path(path)
            )
            if model_string and (
                not query_default
                or str(query_default["model_string"]).lower() != model_string
            ):
                continue
            display = f"{repository_label} | {path} | {query_id}"
            if intent:
                display = f"{repository_label} | {intent} | {path} | {query_id}"
            if q and q not in display.lower() and q not in query_id.lower():
                continue
            result_id = query_id if value_mode == "query_id" else path
            results.append(
                {
                    "id": result_id,
                    "name": path.rsplit("/", 1)[-1] or path,
                    "display": display,
                    "query_id": query_id,
                    "path": path,
                    "intent": intent,
                    "repository": repository,
                    "last_commit_id": last_commit_id,
                }
            )
        return Response({"count": len(results), "results": results})

    @action(detail=False, methods=["get"], url_path="available-query-commits")
    def available_query_commits(self, request):
        source_id = request.GET.get("source_id")
        query_id = (request.GET.get("query_id") or "").strip()
        repository = (request.GET.get("repository") or "org").strip().lower()
        query_path = (request.GET.get("query_path") or "").strip()
        q = (request.GET.get("q") or "").strip().lower()
        if not query_id and not query_path:
            return Response(
                {
                    "count": 0,
                    "results": [],
                    "detail": "Select a Query Path or Query ID to load committed revisions.",
                }
            )
        try:
            source = ForwardSource.objects.get(pk=source_id)
        except (ForwardSource.DoesNotExist, TypeError, ValueError):
            return Response(
                {
                    "count": 0,
                    "results": [],
                    "detail": "Select a Forward Source to load query revisions.",
                }
            )

        try:
            client = source.get_client()
            if query_path and not query_id:
                resolved = client.resolve_nqe_query_reference(
                    repository=repository,
                    query_path=query_path,
                )
                query_id = str(resolved.get("queryId") or "").strip()
            commits = client.get_nqe_query_history(query_id)
        except Exception:
            return Response(
                {
                    "count": 0,
                    "results": [],
                    "detail": "Could not load Forward query revisions from this source.",
                }
            )

        results = []
        for commit in commits:
            commit_id = str(commit.get("id") or "").strip()
            if not commit_id:
                continue
            display = _commit_display(commit)
            if q and q not in display.lower() and q not in commit_id.lower():
                continue
            results.append(
                {
                    "id": commit_id,
                    "name": commit_id,
                    "display": display,
                    "path": str(commit.get("path") or "").strip(),
                }
            )
        return Response({"count": len(results), "results": results})


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
        try:
            job = sync.enqueue_sync_job(user=request.user, adhoc=True)
        except SyncError as exc:
            return Response({"detail": str(exc)}, status=status.HTTP_409_CONFLICT)
        return Response(
            JobSerializer(job, context={"request": request}).data, status=201
        )

    @extend_schema(
        methods=["post"], request=EmptySerializer(), responses={201: JobSerializer()}
    )
    @action(detail=True, methods=["post"])
    def validate(self, request, pk):
        if not request.user.has_perm("forward_netbox.run_forwardsync"):
            raise PermissionDenied(
                "This user does not have permission to validate a Forward sync."
            )
        sync = self.get_object()
        job = sync.enqueue_validation_job(user=request.user, adhoc=True)
        return Response(
            JobSerializer(job, context={"request": request}).data, status=201
        )


class ForwardIngestionViewSet(NetBoxReadOnlyModelViewSet):
    queryset = ForwardIngestion.objects.all()
    serializer_class = ForwardIngestionSerializer


class ForwardIngestionIssueViewSet(NetBoxReadOnlyModelViewSet):
    queryset = ForwardIngestionIssue.objects.all()
    serializer_class = ForwardIngestionIssueSerializer


class ForwardDriftPolicyViewSet(NetBoxModelViewSet):
    queryset = ForwardDriftPolicy.objects.all()
    serializer_class = ForwardDriftPolicySerializer
    filterset_class = ForwardDriftPolicyFilterSet


class ForwardValidationRunViewSet(NetBoxReadOnlyModelViewSet):
    queryset = ForwardValidationRun.objects.all()
    serializer_class = ForwardValidationRunSerializer
    filterset_class = ForwardValidationRunFilterSet

    @extend_schema(
        methods=["post"],
        request=ForwardValidationRunOverrideSerializer,
        responses={200: ForwardValidationRunSerializer},
    )
    @action(detail=True, methods=["post"])
    def force_allow(self, request, pk):
        if not request.user.has_perm("forward_netbox.change_forwardvalidationrun"):
            raise PermissionDenied(
                "This user does not have permission to update a Forward validation run."
            )
        validation_run = self.get_object()
        serializer = ForwardValidationRunOverrideSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        validation_run.force_allow(
            user=request.user,
            reason=serializer.validated_data["reason"],
        )
        return Response(
            ForwardValidationRunSerializer(
                validation_run, context={"request": request}
            ).data
        )
