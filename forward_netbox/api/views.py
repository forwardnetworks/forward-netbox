import hmac

from core.api.serializers_.jobs import JobSerializer
from core.exceptions import SyncError
from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import PermissionDenied
from drf_spectacular.utils import extend_schema
from drf_spectacular.utils import OpenApiResponse
from netbox.api.viewsets import NetBoxModelViewSet
from netbox.api.viewsets import NetBoxReadOnlyModelViewSet
from rest_framework import status
from rest_framework.decorators import action
from rest_framework.response import Response

from ..exceptions import ForwardConnectivityError
from ..exceptions import ForwardSyncError
from .serializers import EmptySerializer
from .serializers import ForwardDeviceAnalysisSerializer
from .serializers import ForwardDriftPolicySerializer
from .serializers import ForwardIngestionIssueSerializer
from .serializers import ForwardIngestionSerializer
from .serializers import ForwardNQEMapSerializer
from .serializers import ForwardSourceSerializer
from .serializers import ForwardSyncSerializer
from .serializers import ForwardValidationRunOverrideSerializer
from .serializers import ForwardValidationRunSerializer
from .serializers import JobScheduleRequestSerializer
from forward_netbox.filtersets import ForwardDeviceAnalysisFilterSet
from forward_netbox.filtersets import ForwardDriftPolicyFilterSet
from forward_netbox.filtersets import ForwardNQEMapFilterSet
from forward_netbox.filtersets import ForwardSourceFilterSet
from forward_netbox.filtersets import ForwardSyncFilterSet
from forward_netbox.filtersets import ForwardValidationRunFilterSet
from forward_netbox.models import ForwardDeviceAnalysis
from forward_netbox.models import ForwardDriftPolicy
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardIngestionIssue
from forward_netbox.models import ForwardNQEMap
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.models import ForwardValidationRun
from forward_netbox.utilities.forward_api import LATEST_COLLECTED_SNAPSHOT
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


def _normalize_model_string(model_string):
    model_string = str(model_string or "").strip().lower()
    if not model_string:
        return ""
    if "." in model_string:
        return model_string
    try:
        content_type = ContentType.objects.get(pk=model_string)
    except (ContentType.DoesNotExist, TypeError, ValueError):
        return ""
    return f"{content_type.app_label}.{content_type.model}".lower()


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

    @action(detail=False, methods=["get"], url_path="available-tags")
    def available_tags(self, request):
        source = None
        source_id = request.GET.get("source_id")
        network_id = (request.GET.get("network_id") or "").strip()
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
                        "network_id": network_id,
                    },
                )
            else:
                return Response(
                    {
                        "count": 0,
                        "results": [],
                        "detail": (
                            "Enter Forward username and password so the plugin can "
                            "load device tags."
                        ),
                    }
                )
        elif not network_id:
            network_id = str((source.parameters or {}).get("network_id") or "").strip()

        if not network_id:
            return Response(
                {
                    "count": 0,
                    "results": [],
                    "detail": "Select a Forward network to load device tags.",
                }
            )

        q = (request.GET.get("q") or "").strip().lower()
        tag_set = set()
        try:
            client = source.get_client()
            snapshot = client.get_latest_processed_snapshot(network_id)
            snapshot_id = str(snapshot.get("id") or "").strip()
            if not snapshot_id:
                return Response(
                    {
                        "count": 0,
                        "results": [],
                        "detail": (
                            "Could not determine a processed snapshot for this "
                            "Forward network."
                        ),
                    }
                )

            rows = client.run_nqe_query(
                query=(
                    "foreach device in network.devices\n"
                    "where device.snapshotInfo.result == DeviceSnapshotResult.completed\n"
                    "where device.platform.vendor != Vendor.FORWARD_CUSTOM\n"
                    "select {tagNames: device.tagNames}"
                ),
                network_id=network_id,
                snapshot_id=snapshot_id,
                fetch_all=True,
            )
            for row in rows:
                for tag in row.get("tagNames") or []:
                    candidate = str(tag or "").strip()
                    if not candidate:
                        continue
                    if q and q not in candidate.lower():
                        continue
                    tag_set.add(candidate)
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
            else:
                detail = "Could not load device tags from this Forward account."
            return Response({"count": 0, "results": [], "detail": detail})
        except Exception:
            return Response(
                {
                    "count": 0,
                    "results": [],
                    "detail": "Could not load device tags from this Forward account.",
                }
            )

        results = [
            {"id": tag, "name": tag, "display": tag}
            for tag in sorted(tag_set, key=lambda value: value.lower())
        ]
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
            query_index = source.get_client().get_nqe_repository_query_index(
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
                for query in query_index.get("rows") or []
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
        model_string = _normalize_model_string(request.GET.get("model_string"))
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
            query_index = source.get_client().get_nqe_repository_query_index(
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
        for query in query_index.get("rows") or []:
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
                try:
                    query_index = client.get_nqe_repository_query_index(
                        repository=repository,
                        directory="/",
                    )
                except Exception:
                    query_index = {}
                indexed_query = (query_index.get("by_path") or {}).get(query_path)
                if indexed_query and indexed_query.get("queryId"):
                    query_id = str(indexed_query.get("queryId") or "").strip()
                else:
                    committed_query = client.get_committed_nqe_query(
                        repository=repository,
                        query_path=query_path,
                        commit_id="head",
                        query_index=query_index,
                    )
                    query_id = str(committed_query.get("queryId") or "").strip()
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

    # A REST PATCH/PUT that changes the standing-schedule intent keys must
    # take effect immediately (the form path already reconciles on save).
    # Hooked HERE, not in the serializer: core perform_update wraps
    # serializer.save() in transaction.atomic, so a serializer-level hook
    # would enqueue RQ entries inside a transaction that can still roll
    # back. After super() returns, the write is committed. Intent-key-only
    # comparison keeps this inert for every other parameters change.
    def perform_update(self, serializer):
        from forward_netbox.utilities.sync_facade import (
            reconcile_standing_schedules,
        )
        from forward_netbox.utilities.sync_facade import (
            standing_schedule_intent,
        )

        # Snapshot from the DB: NetBox's ValidatedModelSerializer applies the
        # incoming attrs to serializer.instance during validation (before
        # perform_update runs), so the in-memory instance already holds the
        # NEW parameters here.
        stored = (
            ForwardSync.objects.filter(pk=serializer.instance.pk)
            .values_list("parameters", flat=True)
            .first()
        )
        before = standing_schedule_intent(stored)
        super().perform_update(serializer)
        if standing_schedule_intent(serializer.instance.parameters) != before:
            reconcile_standing_schedules(serializer.instance, user=self.request.user)

    def perform_create(self, serializer):
        from forward_netbox.utilities.sync_facade import (
            reconcile_standing_schedules,
        )
        from forward_netbox.utilities.sync_facade import (
            standing_schedule_intent,
        )

        super().perform_create(serializer)
        intent = standing_schedule_intent(serializer.instance.parameters)
        if any(present and desired > 0 for present, desired in intent.values()):
            reconcile_standing_schedules(serializer.instance, user=self.request.user)

    @action(detail=False, methods=["get"], url_path="available-snapshots")
    def available_snapshots(self, request):
        source_id = request.GET.get("source_id")
        q = (request.GET.get("q") or "").strip().lower()
        results = [
            {
                "id": LATEST_PROCESSED_SNAPSHOT,
                "name": "latestProcessed",
                "display": "latestProcessed",
            },
            {
                "id": LATEST_COLLECTED_SNAPSHOT,
                "name": "latestCollected",
                "display": "latestCollected (skip backfilled / collection-canceled)",
            },
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

    @extend_schema(methods=["post"], request=EmptySerializer())
    @action(
        detail=True,
        methods=["post"],
        url_path="webhook",
        authentication_classes=[],
        permission_classes=[],
    )
    def webhook(self, request, pk):
        """Push-triggered sync for external webhook senders (e.g. Forward).

        The NetBox-native inbound path is ``POST .../sync/`` with an API token
        (``Authorization: Token ...``) — prefer it whenever the sender can set
        headers. This endpoint exists for senders that cannot: it authenticates
        with a per-sync shared secret (the sync's ``webhook_secret`` parameter)
        via the ``X-Forward-Webhook-Secret`` header or, as a last resort, the
        ``?secret=`` query parameter. An empty/unset secret disables the
        endpoint. Responses are deliberately opaque on failure (one 403, never
        revealing whether the sync exists or a secret is configured), and an
        already-queued/running sync is acknowledged without re-queueing so
        webhook retries stay idempotent.
        """
        sync = ForwardSync.objects.filter(pk=pk).first()
        provided = str(
            request.headers.get("X-Forward-Webhook-Secret")
            or request.GET.get("secret")
            or ""
        )
        configured = (
            str((sync.parameters or {}).get("webhook_secret") or "") if sync else ""
        )
        if not sync or not configured or not hmac.compare_digest(provided, configured):
            raise PermissionDenied("Invalid webhook credentials.")
        from ..jobs import _sync_has_active_job

        for job_suffix in ("adhoc", "scheduled"):
            if _sync_has_active_job(sync, f"{sync.name} - {job_suffix}"):
                return Response(
                    {"status": "already_running"},
                    status=status.HTTP_202_ACCEPTED,
                )
        try:
            # user=None falls back to the sync owner, so job provenance is the
            # configured sync user rather than an anonymous request.
            job = sync.enqueue_sync_job(adhoc=True)
        except SyncError as exc:
            return Response({"detail": str(exc)}, status=status.HTTP_409_CONFLICT)
        return Response(
            {"status": "queued", "job_id": job.pk},
            status=status.HTTP_202_ACCEPTED,
        )

    def _standing_schedule_pk(self, sync, kind):
        """pk of the current enqueued standing-schedule row (or None) — lets
        the schedule actions answer 200 for an idempotent re-post vs 201 for
        a created/replaced schedule."""
        from core.choices import JobStatusChoices

        from ..utilities.sync_facade import STANDING_SCHEDULE_JOB_NAMES

        row = (
            sync.jobs.filter(
                name=STANDING_SCHEDULE_JOB_NAMES[kind],
                status__in=JobStatusChoices.ENQUEUED_STATE_CHOICES,
            )
            .order_by("-created")
            .first()
        )
        return row.pk if row else None

    def _parse_schedule(self, request, min_interval=1):
        """Validate optional schedule_at/interval from the request body.

        Returns (schedule_at, interval) — both None means immediate run."""
        serializer = JobScheduleRequestSerializer(
            data=request.data, min_interval=min_interval
        )
        serializer.is_valid(raise_exception=True)
        return (
            serializer.validated_data.get("schedule_at"),
            serializer.validated_data.get("interval"),
        )

    @extend_schema(
        methods=["post"],
        request=JobScheduleRequestSerializer(),
        responses={
            200: OpenApiResponse(
                description=(
                    "Idempotent schedule re-post (job returned) or "
                    '{"status": "cancelled", "removed": N} for interval 0.'
                )
            ),
            201: JobSerializer(),
            202: OpenApiResponse(
                description='{"status": "already_running", "job_id": N}'
            ),
        },
    )
    @action(detail=True, methods=["post"])
    def validate(self, request, pk):
        if not request.user.has_perm("forward_netbox.run_forwardsync"):
            raise PermissionDenied(
                "This user does not have permission to validate a Forward sync."
            )
        sync = self.get_object()
        schedule_at, interval = self._parse_schedule(request)
        from ..utilities.sync_facade import cancel_standing_schedule
        from ..utilities.sync_facade import JobAlreadyActive

        if interval == 0:
            removed = cancel_standing_schedule(sync, "validation")
            return Response({"status": "cancelled", "removed": removed})
        existing_pk = self._standing_schedule_pk(sync, "validation")
        try:
            job = sync.enqueue_validation_job(
                user=request.user,
                adhoc=True,
                schedule_at=schedule_at,
                interval=interval,
            )
        except JobAlreadyActive as exc:
            return Response(
                {
                    "status": "already_running",
                    "job_id": exc.job.pk,
                    "detail": str(exc),
                },
                status=status.HTTP_202_ACCEPTED,
            )
        return Response(
            JobSerializer(job, context={"request": request}).data,
            status=200 if interval and job.pk == existing_pk else 201,
        )

    def _enqueue_button_job_response(self, request, kind):
        """Shared body for the button-job actions: permission check mirroring
        the HTML view, overlap guard via enqueue_button_job (202
        already_running on an active equivalent job — idempotent for
        retry-blind schedulers, matching the webhook endpoint)."""
        from ..utilities.sync_facade import button_job_permission
        from ..utilities.sync_facade import enqueue_button_job
        from ..utilities.sync_facade import JobAlreadyActive
        from ..utilities.sync_facade import JobBlockedBySyncRun

        permission = button_job_permission(kind)
        if not request.user.has_perm(permission):
            raise PermissionDenied(
                f"This user does not have the `{permission}` permission."
            )
        # Only validate and dependency-preview accept schedule parameters;
        # silently ignoring them here would 201 a one-shot run the caller
        # believes is a standing schedule.
        if isinstance(request.data, dict) and (
            "schedule_at" in request.data or "interval" in request.data
        ):
            return Response(
                {
                    "detail": (
                        "This action does not support scheduling; only the "
                        "validate and dependency-preview actions accept "
                        "schedule_at/interval."
                    )
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        sync = self.get_object()
        try:
            job = enqueue_button_job(sync, kind, request.user)
        except JobBlockedBySyncRun as exc:
            # Distinct from already_running: the requested work is NOT
            # queued (prune refuses while a sync run is active). Still 2xx
            # so retry-blind crons stay green; the status string says why,
            # and the post-sync auto-prune covers the gap when enabled.
            return Response(
                {
                    "status": "blocked_by_sync_run",
                    "job_id": exc.job.pk,
                    "detail": str(exc),
                },
                status=status.HTTP_202_ACCEPTED,
            )
        except JobAlreadyActive as exc:
            return Response(
                {
                    "status": "already_running",
                    "job_id": exc.job.pk,
                    "detail": str(exc),
                },
                status=status.HTTP_202_ACCEPTED,
            )
        return Response(
            JobSerializer(job, context={"request": request}).data, status=201
        )

    @extend_schema(
        methods=["post"],
        request=JobScheduleRequestSerializer(),
        responses={
            200: OpenApiResponse(
                description=(
                    "Idempotent schedule re-post (job returned) or "
                    '{"status": "cancelled", "removed": N} for interval 0.'
                )
            ),
            201: JobSerializer(),
            202: OpenApiResponse(
                description='{"status": "already_running", "job_id": N}'
            ),
        },
    )
    @action(detail=True, methods=["post"], url_path="dependency-preview")
    def dependency_preview(self, request, pk):
        # The preview is a full live dry-run against Forward; a sub-hourly
        # standing schedule degenerates to back-to-back runs on large fabrics.
        schedule_at, interval = self._parse_schedule(request, min_interval=60)
        if interval == 0 or schedule_at or interval:
            # Standing-schedule management: fixed JobRunner name +
            # enqueue_once dedup (one schedule per sync); interval=0 cancels.
            # Immediate runs below keep the legacy per-sync job name the
            # drift report and preview GET match on.
            from ..utilities.sync_facade import button_job_permission
            from ..utilities.sync_facade import cancel_standing_schedule
            from ..utilities.sync_facade import enqueue_preview_schedule

            permission = button_job_permission("dependency_preview")
            if not request.user.has_perm(permission):
                raise PermissionDenied(
                    f"This user does not have the `{permission}` permission."
                )
            sync = self.get_object()
            if interval == 0:
                removed = cancel_standing_schedule(sync, "dependency_preview")
                return Response({"status": "cancelled", "removed": removed})
            existing_pk = self._standing_schedule_pk(sync, "dependency_preview")
            job = enqueue_preview_schedule(
                sync,
                user=request.user,
                schedule_at=schedule_at,
                interval=interval,
            )
            return Response(
                JobSerializer(job, context={"request": request}).data,
                status=200 if job.pk == existing_pk else 201,
            )
        return self._enqueue_button_job_response(request, "dependency_preview")

    @extend_schema(
        methods=["post"],
        request=EmptySerializer(),
        responses={
            201: JobSerializer(),
            202: OpenApiResponse(
                description=(
                    '{"status": "already_running"|"blocked_by_sync_run", '
                    '"job_id": N}'
                )
            ),
        },
    )
    @action(detail=True, methods=["post"], url_path="prune-orphans")
    def prune_orphans(self, request, pk):
        return self._enqueue_button_job_response(request, "prune_orphans")

    @extend_schema(
        methods=["post"],
        request=EmptySerializer(),
        responses={
            201: JobSerializer(),
            202: OpenApiResponse(
                description='{"status": "already_running", "job_id": N}'
            ),
        },
    )
    @action(detail=True, methods=["post"], url_path="tag-delete-eligible-ipam")
    def tag_delete_eligible_ipam(self, request, pk):
        return self._enqueue_button_job_response(request, "tag_delete_eligible_ipam")

    @extend_schema(
        methods=["post"],
        request=EmptySerializer(),
        responses={
            201: JobSerializer(),
            202: OpenApiResponse(
                description='{"status": "already_running", "job_id": N}'
            ),
        },
    )
    @action(detail=True, methods=["post"], url_path="create-module-bays")
    def create_module_bays(self, request, pk):
        return self._enqueue_button_job_response(request, "create_module_bays")


class ForwardIngestionViewSet(NetBoxReadOnlyModelViewSet):
    queryset = ForwardIngestion.objects.all()
    serializer_class = ForwardIngestionSerializer


class ForwardIngestionIssueViewSet(NetBoxReadOnlyModelViewSet):
    queryset = ForwardIngestionIssue.objects.all()
    serializer_class = ForwardIngestionIssueSerializer


class ForwardDeviceAnalysisViewSet(NetBoxModelViewSet):
    queryset = ForwardDeviceAnalysis.objects.all()
    serializer_class = ForwardDeviceAnalysisSerializer
    filterset_class = ForwardDeviceAnalysisFilterSet


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
