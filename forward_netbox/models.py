import json
import logging
from copy import deepcopy

from django.conf import settings
from django.contrib.contenttypes.models import ContentType
from django.core.validators import MinValueValidator
from django.db import models
from django.urls import reverse
from django.utils import timezone
from django.utils.translation import gettext as _

from core.choices import DataSourceStatusChoices
from core.models import Job
from netbox.models import NetBoxModel, PrimaryModel
from netbox.models.features import JobsMixin, TagsMixin
from utilities.querysets import RestrictedQuerySet

from .choices import (
    ForwardRawDataTypeChoices,
    ForwardSnapshotStatusModelChoices,
    ForwardSyncTypeChoices,
)

logger = logging.getLogger("forward_netbox.models")

# Dynamically generate supported sync models from dcim and ipam apps
_forward_supported_apps = ["dcim", "ipam"]
ForwardSupportedSyncModels = models.Q(app_label__in=_forward_supported_apps)

def get_forward_supported_ctypes():
    try:
        return ContentType.objects.filter(app_label__in=_forward_supported_apps).order_by("app_label", "model")
    except Exception:
        return ContentType.objects.none()


class ForwardNQEMap(NetBoxModel):
    name = models.CharField(max_length=100, unique=True)

    query_id = models.CharField(
        max_length=100,
        verbose_name="NQE Query ID",
        help_text=_("The Forward Networks NQE Query ID to run."),
    )

    netbox_model = models.ForeignKey(
        to=ContentType,
        related_name="+",
        verbose_name="NetBox Model",
        limit_choices_to=ForwardSupportedSyncModels,
        help_text=_("The NetBox model this NQE query maps to."),
        on_delete=models.PROTECT,
    )

    class Meta:
        verbose_name = "Forward NQE Map"
        verbose_name_plural = "Forward NQE Maps"

    def __str__(self):
        return f"{self.name} → {self.netbox_model}"

    def get_absolute_url(self):
        return reverse("plugins:forward_netbox:forwardnqemap", args=[self.pk])


class ForwardSource(JobsMixin, PrimaryModel):
    name = models.CharField(max_length=100, unique=True)
    url = models.CharField(max_length=200, verbose_name=_("URL"))
    network_id = models.PositiveIntegerField(
        null=False,
        blank=False,
        verbose_name=_("Network ID"),
        help_text=_("The Forward Enterprise network ID to query"),
        validators=[MinValueValidator(1)],
    )
    status = models.CharField(
        max_length=50,
        choices=ForwardSnapshotStatusModelChoices,
        default=ForwardSnapshotStatusModelChoices.STATUS_UNPROCESSED,
        editable=False,
    )
    parameters = models.JSONField(blank=True, null=True)
    last_synced = models.DateTimeField(blank=True, null=True, editable=True)

    class Meta:
        ordering = ("name",)
        verbose_name = "Forward Source"
        verbose_name_plural = "Forward Sources"

    def __str__(self):
        return self.name

    @property
    def ready_for_sync(self):
        return self.status not in (
            DataSourceStatusChoices.QUEUED,
            DataSourceStatusChoices.SYNCING,
        )

    def get_absolute_url(self):
        return reverse("plugins:forward_netbox:forwardsource", args=[self.pk])

    def sync(self, job=None):
        from .utilities.logging import SyncLogging
        from .utilities.fwdutils import Forward

        self.logger = SyncLogging(job=job.pk if job else None)

        try:
            self.logger.log_info(f"Syncing snapshots for {self.name}", obj=self)

            parameters = dict(self.parameters or {})
            parameters["base_url"] = self.url
            fwd = Forward(parameters=parameters)
            snapshots = fwd.get_snapshots(self.network_id)

            for snapshot_id, snapshot in snapshots.items():
                ForwardSnapshot.objects.update_or_create(
                    source=self,
                    snapshot_id=snapshot_id,
                    defaults={
                        "data": snapshot,
                        "date": snapshot.get("start"),
                        "status": ForwardSnapshotStatusModelChoices.STATUS_PROCESSED,
                        "last_updated": timezone.now(),
                    },
                )
                self.logger.log_info(f"Saved snapshot {snapshot_id}", obj=self)

            self.status = DataSourceStatusChoices.COMPLETED
            self.last_synced = timezone.now()
            self.save()
            self.logger.log_success("Snapshot sync completed successfully", obj=self)

        except Exception as e:
            self.status = DataSourceStatusChoices.FAILED
            self.save()
            self.logger.log_failure(f"Snapshot sync failed: {str(e)}", obj=self)
            raise


class ForwardSnapshot(models.Model):
    created = models.DateTimeField(auto_now_add=True)
    last_updated = models.DateTimeField(editable=False)
    source = models.ForeignKey(
        to=ForwardSource,
        on_delete=models.CASCADE,
        related_name="snapshots",
        editable=False,
    )
    snapshot_id = models.CharField(max_length=100)
    data = models.JSONField(blank=True, null=True)
    date = models.DateTimeField(blank=True, null=True, editable=False)
    status = models.CharField(
        max_length=50,
        choices=ForwardSnapshotStatusModelChoices,
        default=ForwardSnapshotStatusModelChoices.STATUS_UNPROCESSED,
    )

    objects = RestrictedQuerySet.as_manager()

    class Meta:
        ordering = ("source", "-date")
        verbose_name = "Forward Snapshot"
        verbose_name_plural = "Forward Snapshots"

    def __str__(self):
        return f"{self.source.name} - {self.snapshot_id}"

    def get_absolute_url(self):
        return reverse("plugins:forward_netbox:forwardsnapshot", args=[self.pk])


class ForwardSync(JobsMixin, PrimaryModel):
    name = models.CharField(max_length=100, unique=True)
    snapshot_data = models.ForeignKey(
        to=ForwardSnapshot,
        on_delete=models.CASCADE,
        related_name="syncs",
    )
    type = models.CharField(
        max_length=50,
        choices=ForwardSyncTypeChoices,
        default=ForwardSyncTypeChoices.DCIM,
    )
    status = models.CharField(
        max_length=50,
        choices=ForwardSnapshotStatusModelChoices,
        default=ForwardSnapshotStatusModelChoices.STATUS_UNPROCESSED,
        editable=False,
    )
    parameters = models.JSONField(blank=True, null=True)
    auto_merge = models.BooleanField(default=False)
    last_synced = models.DateTimeField(blank=True, null=True, editable=False)
    scheduled = models.DateTimeField(null=True, blank=True)
    interval = models.PositiveIntegerField(
        blank=True,
        null=True,
        validators=[MinValueValidator(1)],
        help_text="Recurrence interval (in minutes)",
    )
    user = models.ForeignKey(
        to=settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        related_name="+",
        blank=True,
        null=True,
    )

    objects = RestrictedQuerySet.as_manager()

    class Meta:
        ordering = ["pk"]
        verbose_name = "Forward Sync"
        permissions = [
            ("sync_ingest", "Can run adhoc Forward Sync ingestion"),
        ]

    def __str__(self):
        return self.name

    def get_absolute_url(self):
        return reverse("plugins:forward_netbox:forwardsync", args=[self.pk])

    def get_status_color(self):
        return DataSourceStatusChoices.colors.get(self.status)

    @property
    def ready_for_sync(self):
        return self.status not in (DataSourceStatusChoices.SYNCING,)

    @property
    def docs_url(self):
        return None

    def run_ingestion(self, job=None):
        from .utilities.fwdutils import Forward
        from utilities.datetime import local_now

        self.logger.log_info(
            f"Starting ingestion for snapshot {self.snapshot_data.snapshot_id}", obj=self
        )

        try:
            parameters = dict(self.snapshot_data.source.parameters or {})
            if self.parameters:
                parameters.update(self.parameters)

            parameters.setdefault("base_url", self.snapshot_data.source.url)
            parameters.setdefault("auth", self.snapshot_data.source.parameters.get("auth"))

            fwd = Forward(parameters=parameters)

            result = fwd.run_ingestion(sync=self, job=job)

            self.last_synced = local_now()
            self.status = DataSourceStatusChoices.COMPLETED
            self.save()

            self.logger.log_success("Ingestion completed successfully", obj=self)
            return result

        except Exception as e:
            self.status = DataSourceStatusChoices.FAILED
            self.save()
            self.logger.log_failure(f"Ingestion failed: {str(e)}", obj=self)
            raise

class ForwardData(models.Model):
    snapshot_data = models.ForeignKey(
        to=ForwardSnapshot,
        on_delete=models.CASCADE,
        related_name="fwd_data",
    )
    data = models.JSONField(blank=True, null=True)
    type = models.CharField(
        max_length=50,
        choices=ForwardRawDataTypeChoices,
    )

    objects = RestrictedQuerySet.as_manager()
