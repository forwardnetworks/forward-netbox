import logging
import traceback
from uuid import uuid4

from core.exceptions import SyncError
from core.models import Job
from core.models import ObjectType
from core.signals import pre_sync
from django.conf import settings
from django.contrib.contenttypes.models import ContentType
from django.core.cache import cache
from django.core.exceptions import ValidationError
from django.core.validators import MinValueValidator
from django.db import models
from django.db.models import Q
from django.urls import reverse
from django.utils import timezone
from django.utils.module_loading import import_string
from django.utils.translation import gettext as _
from netbox.context import current_request
from netbox.models import ChangeLoggedModel
from netbox.models import PrimaryModel
from netbox.models.features import JobsMixin
from netbox.models.features import TagsMixin
from netbox_branching.choices import BranchStatusChoices
from netbox_branching.contextvars import active_branch
from netbox_branching.models import Branch
from utilities.querysets import RestrictedQuerySet
from utilities.request import NetBoxFakeRequest

from .choices import FORWARD_SUPPORTED_MODELS
from .choices import ForwardIngestionPhaseChoices
from .choices import ForwardSourceDeploymentChoices
from .choices import ForwardSourceStatusChoices
from .choices import ForwardSyncStatusChoices
from .exceptions import ForwardSyncError
from .utilities.forward_api import ForwardClient
from .utilities.forward_api import LATEST_PROCESSED_SNAPSHOT
from .utilities.logging import SyncLogging
from .utilities.sync_contracts import normalize_coalesce_fields
from .utilities.sync_contracts import validate_query_shape_for_model

logger = logging.getLogger("forward_netbox.models")


FORWARD_SUPPORTED_SYNC_MODELS = Q()
for model_string in FORWARD_SUPPORTED_MODELS:
    app_label, model_name = model_string.split(".")
    FORWARD_SUPPORTED_SYNC_MODELS |= Q(app_label=app_label, model=model_name)


class ForwardSource(JobsMixin, PrimaryModel):
    objects = RestrictedQuerySet.as_manager()

    name = models.CharField(max_length=100, unique=True)
    type = models.CharField(
        verbose_name=_("type"),
        max_length=50,
        choices=ForwardSourceDeploymentChoices,
        default=ForwardSourceDeploymentChoices.SAAS,
    )
    url = models.CharField(max_length=200, verbose_name=_("URL"))
    status = models.CharField(
        max_length=50,
        choices=ForwardSourceStatusChoices,
        default=ForwardSourceStatusChoices.NEW,
        editable=False,
    )
    parameters = models.JSONField(blank=True, null=True, default=dict)
    last_synced = models.DateTimeField(blank=True, null=True, editable=False)

    class Meta:
        ordering = ("name",)
        verbose_name = _("Forward Source")
        verbose_name_plural = _("Forward Sources")
        db_table = "forward_netbox_source"

    def __str__(self):
        return self.name

    def get_absolute_url(self):
        return reverse("plugins:forward_netbox:forwardsource", args=[self.pk])

    def clean(self):
        super().clean()
        self.url = self.url.rstrip("/")
        parameters = dict(self.parameters or {})
        invalid = sorted(
            set(parameters.keys())
            - {
                "username",
                "password",
                "verify",
                "timeout",
                "network_id",
            }
        )
        if invalid:
            raise ValidationError(_(f"Unsupported Forward source keys: {invalid}"))
        if self.type == ForwardSourceDeploymentChoices.SAAS:
            self.url = "https://fwd.app"
            parameters["verify"] = True
        if not (parameters.get("username") and parameters.get("password")):
            raise ValidationError(_("Provide a Forward username and password."))
        if not isinstance(parameters.get("verify", True), bool):
            raise ValidationError(_("`verify` must be a boolean."))
        if parameters.get("network_id") is not None and not isinstance(
            parameters.get("network_id"), str
        ):
            raise ValidationError(_("`network_id` must be a string."))
        self.parameters = parameters

    def get_client(self):
        return ForwardClient(self)

    def get_masked_parameters(self):
        allowed = {
            "username",
            "password",
            "verify",
            "timeout",
            "network_id",
        }
        parameters = {
            key: value
            for key, value in dict(self.parameters or {}).items()
            if key in allowed
        }
        if parameters.get("password"):
            parameters["password"] = "********"
        return parameters

    @property
    def network_id(self):
        return (self.parameters or {}).get("network_id") or ""

    def validate_connection(self):
        client = self.get_client()
        networks = client.get_networks()
        if not networks:
            raise ForwardSyncError(
                "Forward credentials are valid, but no networks are available."
            )
        network_id = self.network_id
        if network_id and network_id not in {network["id"] for network in networks}:
            raise ForwardSyncError(
                f"Network {network_id} is not available to this Forward user."
            )


class ForwardNQEMap(ChangeLoggedModel):
    objects = RestrictedQuerySet.as_manager()

    name = models.CharField(max_length=200)
    netbox_model = models.ForeignKey(
        to=ContentType,
        on_delete=models.PROTECT,
        related_name="+",
        verbose_name=_("NetBox Model"),
        limit_choices_to=FORWARD_SUPPORTED_SYNC_MODELS,
    )
    query_id = models.CharField(max_length=100, blank=True)
    query = models.TextField(blank=True)
    commit_id = models.CharField(max_length=100, blank=True)
    parameters = models.JSONField(blank=True, default=dict)
    coalesce_fields = models.JSONField(blank=True, default=list)
    weight = models.PositiveIntegerField(default=100)
    enabled = models.BooleanField(default=True)
    built_in = models.BooleanField(default=False, editable=False)

    class Meta:
        ordering = ("weight", "pk")
        verbose_name = _("Forward NQE Map")
        verbose_name_plural = _("Forward NQE Maps")
        db_table = "forward_netbox_nqe_map"

    def __str__(self):
        return self.name or "Forward NQE Map"

    @property
    def model_string(self):
        return f"{self.netbox_model.app_label}.{self.netbox_model.model}"

    @property
    def execution_mode(self):
        return "query_id" if self.query_id else "query"

    @property
    def execution_value(self):
        return self.query_id or self.name

    def get_absolute_url(self):
        return reverse("plugins:forward_netbox:forwardnqemap", args=[self.pk])

    def clean(self):
        super().clean()
        if bool(self.query_id) == bool(self.query):
            raise ValidationError(_("Set exactly one of `Query ID` or `Query`."))
        if self.parameters and not isinstance(self.parameters, dict):
            raise ValidationError(_("Parameters must be a JSON object."))
        try:
            normalized = normalize_coalesce_fields(
                self.model_string,
                self.coalesce_fields,
                allow_default=True,
            )
        except ValueError as exc:
            raise ValidationError(_(str(exc)))
        self.coalesce_fields = normalized
        if self.query:
            try:
                validate_query_shape_for_model(
                    self.model_string,
                    self.query,
                    self.coalesce_fields,
                )
            except ValueError as exc:
                raise ValidationError(_(str(exc)))


class ForwardSync(JobsMixin, TagsMixin, ChangeLoggedModel):
    objects = RestrictedQuerySet.as_manager()

    name = models.CharField(max_length=100, unique=True)
    source = models.ForeignKey(
        to=ForwardSource,
        on_delete=models.CASCADE,
        related_name="syncs",
    )
    status = models.CharField(
        max_length=50,
        choices=ForwardSyncStatusChoices,
        default=ForwardSyncStatusChoices.NEW,
        editable=False,
    )
    parameters = models.JSONField(blank=True, null=True, default=dict)
    last_synced = models.DateTimeField(blank=True, null=True, editable=False)
    auto_merge = models.BooleanField(default=False)
    scheduled = models.DateTimeField(null=True, blank=True)
    interval = models.PositiveIntegerField(
        blank=True,
        null=True,
        validators=(MinValueValidator(1),),
        help_text=_("Recurrence interval (in minutes)"),
    )
    user = models.ForeignKey(
        to=settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        related_name="+",
        blank=True,
        null=True,
    )

    class Meta:
        ordering = ("pk",)
        verbose_name = _("Forward Sync")
        verbose_name_plural = _("Forward Syncs")
        db_table = "forward_netbox_sync"
        permissions = (("run_forwardsync", "Can run Forward sync"),)

    def __str__(self):
        return self.name

    def get_absolute_url(self):
        return reverse("plugins:forward_netbox:forwardsync", args=[self.pk])

    @property
    def logger(self):
        return getattr(self, "_logger", SyncLogging(job=self.pk))

    @logger.setter
    def logger(self, value):
        self._logger = value

    @property
    def ready_for_sync(self):
        return self.status not in (
            ForwardSyncStatusChoices.QUEUED,
            ForwardSyncStatusChoices.SYNCING,
            ForwardSyncStatusChoices.MERGING,
        )

    @property
    def last_ingestion(self):
        return self.forwardingestion_set.last()

    def clean(self):
        super().clean()
        parameters = dict(self.parameters or {})
        invalid = sorted(
            set(parameters.keys())
            - {"auto_merge", "snapshot_id", *FORWARD_SUPPORTED_MODELS}
        )
        if invalid:
            raise ValidationError(_(f"Unsupported Forward sync keys: {invalid}"))
        snapshot_id = parameters.get("snapshot_id") or LATEST_PROCESSED_SNAPSHOT
        if not isinstance(snapshot_id, str):
            raise ValidationError(_("`snapshot_id` must be a string."))
        parameters["snapshot_id"] = snapshot_id
        parameters["auto_merge"] = bool(parameters.get("auto_merge", self.auto_merge))
        if self.scheduled and self.scheduled < timezone.now():
            raise ValidationError(
                {"scheduled": _("Scheduled time must be in the future.")}
            )
        if not any(
            parameters.get(model_string, True)
            for model_string in FORWARD_SUPPORTED_MODELS
        ):
            raise ValidationError(_("Select at least one NetBox model to sync."))
        self.auto_merge = parameters["auto_merge"]
        self.parameters = parameters

    def save(self, *args, **kwargs):
        super().save(*args, **kwargs)
        if self.scheduled:
            self.enqueue_sync_job()

    def get_network_id(self):
        return (self.source.parameters or {}).get("network_id")

    def get_snapshot_id(self):
        return (self.parameters or {}).get("snapshot_id") or LATEST_PROCESSED_SNAPSHOT

    def resolve_snapshot_id(self, client=None):
        snapshot_id = self.get_snapshot_id()
        if snapshot_id != LATEST_PROCESSED_SNAPSHOT:
            return snapshot_id
        client = client or self.source.get_client()
        network_id = self.get_network_id()
        if not network_id:
            raise ForwardSyncError(
                "Forward sync requires a network on the source before resolving latestProcessed."
            )
        return client.get_latest_processed_snapshot_id(network_id)

    def get_maps(self):
        return list(
            ForwardNQEMap.objects.select_related("netbox_model")
            .filter(enabled=True)
            .order_by("weight", "pk")
        )

    def get_query_parameters(self):
        return {}

    def get_model_strings(self):
        return self.enabled_models()

    def get_display_parameters(self):
        parameters = {}
        network_id = self.get_network_id() or ""
        if network_id:
            parameters["network_id"] = network_id
        parameters["snapshot_id"] = self.get_snapshot_id()
        parameters["auto_merge"] = bool(
            (self.parameters or {}).get("auto_merge", self.auto_merge)
        )
        parameters["models"] = self.get_model_strings()
        return parameters

    def is_model_enabled(self, model_string):
        parameters = self.parameters or {}
        return parameters.get(model_string, True)

    def enabled_models(self):
        return [
            model_string
            for model_string in FORWARD_SUPPORTED_MODELS
            if self.is_model_enabled(model_string)
        ]

    def enqueue_sync_job(self, adhoc=False, user=None):
        if not user:
            user = self.user
        self.status = ForwardSyncStatusChoices.QUEUED
        ForwardSync.objects.filter(pk=self.pk).update(status=self.status)
        return Job.enqueue(
            import_string("forward_netbox.jobs.sync_forwardsync"),
            instance=self,
            user=user,
            name=f"{self.name} - {'adhoc' if adhoc else 'scheduled'}",
            adhoc=adhoc,
            schedule_at=None if adhoc else self.scheduled,
            interval=None if adhoc else self.interval,
        )

    def sync(self, job=None):
        from .utilities.sync import ForwardSyncRunner

        if self.status in (
            ForwardSyncStatusChoices.SYNCING,
            ForwardSyncStatusChoices.MERGING,
        ):
            raise SyncError(
                "Cannot initiate sync; a Forward ingestion is already in progress."
            )

        if job:
            self.logger = SyncLogging(job=job.pk)
            user = job.user
        else:
            self.logger = SyncLogging(job=self.pk)
            user = self.user

        pre_sync.send(sender=self.__class__, instance=self)

        self.status = ForwardSyncStatusChoices.SYNCING
        ForwardSync.objects.filter(pk=self.pk).update(status=self.status)

        ingestion = ForwardIngestion.objects.create(sync=self, job=job)
        try:
            branch = Branch(name=f"Forward Sync {self.name} - {timezone.now()}")
            branch.save(provision=False)
            ingestion.branch = branch
            ingestion.save(update_fields=["branch"])

            if job:
                job.object_type = ObjectType.objects.get_for_model(ingestion)
                job.object_id = ingestion.pk
                job.save(update_fields=["object_type", "object_id"])

            branch.provision(user=user)
            branch.refresh_from_db()
            if branch.status == BranchStatusChoices.FAILED:
                self.logger.log_failure(f"Branch failed: `{branch}`", obj=branch)
                raise SyncError("Branch creation failed.")

            self.logger.log_info(f"New branch created {branch.name}", obj=branch)
            runner = ForwardSyncRunner(
                sync=self,
                ingestion=ingestion,
                client=self.source.get_client(),
                logger_=self.logger,
            )
            current_branch = active_branch.get()
            request_token = None
            if current_request.get() is None:
                request_token = current_request.set(
                    NetBoxFakeRequest({"id": uuid4(), "user": user})
                )
            try:
                active_branch.set(branch)
                runner.run()
            finally:
                active_branch.set(current_branch)
                if request_token is not None:
                    current_request.reset(request_token)

            if self.status != ForwardSyncStatusChoices.FAILED:
                self.status = ForwardSyncStatusChoices.READY_TO_MERGE
            self.logger.log_success("Forward ingestion completed.", obj=self)
            if (
                self.auto_merge
                and self.status == ForwardSyncStatusChoices.READY_TO_MERGE
            ):
                ingestion.enqueue_merge_job(user=user, remove_branch=True)
                self.logger.log_info("Auto merge job enqueued.", obj=ingestion)
        except Exception as exc:
            logger.exception("Forward sync failed")
            self.status = ForwardSyncStatusChoices.FAILED
            self.logger.log_failure(f"Forward ingestion failed: {exc}", obj=ingestion)
            ForwardIngestionIssue.objects.create(
                ingestion=ingestion,
                phase=ForwardIngestionPhaseChoices.SYNC,
                message=str(exc),
                exception=exc.__class__.__name__,
                raw_data={"traceback": traceback.format_exc()},
            )
        finally:
            self.last_synced = timezone.now()
            self.source.last_synced = self.last_synced
            self.source.status = (
                ForwardSourceStatusChoices.READY
                if self.status
                in (
                    ForwardSyncStatusChoices.READY_TO_MERGE,
                    ForwardSyncStatusChoices.MERGING,
                    ForwardSyncStatusChoices.COMPLETED,
                )
                else ForwardSourceStatusChoices.FAILED
            )
            ForwardSource.objects.filter(pk=self.source.pk).update(
                last_synced=self.source.last_synced,
                status=self.source.status,
            )
            ForwardSync.objects.filter(pk=self.pk).update(
                status=self.status,
                last_synced=self.last_synced,
            )
            if job:
                job.data = self.logger.log_data
                job.save(update_fields=["data"])


class ForwardIngestion(JobsMixin, models.Model):
    objects = RestrictedQuerySet.as_manager()

    sync = models.ForeignKey(ForwardSync, on_delete=models.CASCADE)
    job = models.ForeignKey(Job, on_delete=models.SET_NULL, null=True)
    merge_job = models.ForeignKey(
        Job,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="merge_ingestion",
    )
    branch = models.OneToOneField(
        Branch, on_delete=models.SET_NULL, null=True, blank=True
    )
    snapshot_selector = models.CharField(max_length=100, blank=True, default="")
    snapshot_id = models.CharField(max_length=100, blank=True, default="")
    snapshot_info = models.JSONField(blank=True, default=dict)
    snapshot_metrics = models.JSONField(blank=True, default=dict)
    created = models.DateTimeField(default=timezone.now, editable=False)

    class Meta:
        ordering = ("pk",)
        verbose_name = _("Forward Ingestion")
        verbose_name_plural = _("Forward Ingestions")
        db_table = "forward_netbox_ingestion"
        permissions = (("merge_forwardingestion", "Can merge Forward ingestion"),)

    def __str__(self):
        return self.name

    @property
    def name(self):
        if self.branch:
            return self.branch.name
        try:
            return f"{self.sync.name} (Ingestion {self.pk})"
        except ForwardIngestion.sync.RelatedObjectDoesNotExist:
            return f"Ingestion {self.pk}"

    def get_absolute_url(self):
        return reverse("plugins:forward_netbox:forwardingestion", args=[self.pk])

    def get_snapshot_summary(self):
        info = dict(self.snapshot_info or {})
        return {
            "snapshot_selector": self.snapshot_selector or "",
            "snapshot_id": self.snapshot_id or "",
            "state": info.get("state") or "",
            "created_at": info.get("createdAt") or "",
            "processed_at": info.get("processedAt") or "",
        }

    def get_snapshot_metrics_summary(self):
        metrics = dict(self.snapshot_metrics or {})
        keys = (
            "snapshotState",
            "numSuccessfulDevices",
            "numCollectionFailureDevices",
            "numProcessingFailureDevices",
            "numSuccessfulEndpoints",
            "numCollectionFailureEndpoints",
            "numProcessingFailureEndpoints",
            "collectionDuration",
            "processingDuration",
        )
        return {key: metrics[key] for key in keys if key in metrics}

    @staticmethod
    def get_job_logs(job):
        if not job:
            return {}
        if job.data:
            return job.data
        return cache.get(f"forward_sync_{job.pk}") or {}

    def enqueue_merge_job(self, user, remove_branch=False):
        self.sync.status = ForwardSyncStatusChoices.QUEUED
        ForwardSync.objects.filter(pk=self.sync.pk).update(status=self.sync.status)
        job = Job.enqueue(
            import_string("forward_netbox.jobs.merge_forwardingestion"),
            name=f"{self.name} Merge",
            instance=self,
            user=user,
            remove_branch=remove_branch,
        )
        ForwardIngestion.objects.filter(pk=self.pk).update(merge_job=job)
        self.merge_job = job
        return job

    def get_statistics(self, stage="sync"):
        job = self.merge_job if stage == "merge" else self.job
        job_results = self.get_job_logs(job)
        raw_stats = job_results.get("statistics", {})
        statistics = {}
        for model_string, stats in raw_stats.items():
            total = stats.get("total", 0)
            if total:
                statistics[model_string] = stats.get("current", 0) / total * 100
        return {"job_results": job_results, "statistics": statistics}

    def sync_merge(self):
        from .utilities.merge import merge_branch

        forwardsync = self.sync
        if forwardsync.status == ForwardSyncStatusChoices.MERGING:
            raise SyncError("Cannot initiate merge; merge already in progress.")

        pre_sync.send(sender=self.__class__, instance=self)

        forwardsync.status = ForwardSyncStatusChoices.MERGING
        ForwardSync.objects.filter(pk=self.sync.pk).update(status=forwardsync.status)

        try:
            merge_branch(ingestion=self, sync_logger=forwardsync.logger)
            forwardsync.status = ForwardSyncStatusChoices.COMPLETED
        except Exception:
            forwardsync.status = ForwardSyncStatusChoices.FAILED
            raise

        forwardsync.last_synced = timezone.now()
        ForwardSync.objects.filter(pk=self.sync.pk).update(
            status=forwardsync.status,
            last_synced=forwardsync.last_synced,
        )


class ForwardIngestionIssue(models.Model):
    objects = RestrictedQuerySet.as_manager()

    ingestion = models.ForeignKey(
        to=ForwardIngestion,
        on_delete=models.CASCADE,
        related_name="issues",
    )
    timestamp = models.DateTimeField(default=timezone.now)
    phase = models.CharField(
        max_length=10,
        choices=ForwardIngestionPhaseChoices,
        default=ForwardIngestionPhaseChoices.SYNC,
        verbose_name=_("Phase"),
    )
    model = models.CharField(max_length=100, blank=True, null=True)
    message = models.TextField()
    coalesce_fields = models.JSONField(blank=True, default=dict)
    defaults = models.JSONField(blank=True, default=dict)
    raw_data = models.JSONField(blank=True, default=dict)
    exception = models.TextField()

    class Meta:
        ordering = ("timestamp",)
        verbose_name = _("Forward Ingestion Issue")
        verbose_name_plural = _("Forward Ingestion Issues")
        db_table = "forward_netbox_ingestion_issue"

    def __str__(self):
        return f"[{self.timestamp}] {self.message}"
