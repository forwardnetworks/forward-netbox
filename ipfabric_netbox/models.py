import ast
import functools
import json
import logging
import traceback
from copy import deepcopy
from uuid import uuid4

import httpx
from core.choices import DataSourceStatusChoices
from core.exceptions import SyncError
from core.models import Job
from core.models import ObjectType
from core.signals import pre_sync
from dcim.models import Device
from dcim.models import Site
from dcim.models import VirtualChassis
from dcim.signals import assign_virtualchassis_master
from django.apps import apps
from django.conf import settings
from django.contrib.contenttypes.models import ContentType
from django.core.cache import cache
from django.core.exceptions import ValidationError
from django.core.validators import MinValueValidator
from django.db import models
from django.db import transaction
from django.db.models import Q
from django.db.models import signals
from django.urls import reverse
from django.utils import timezone
from django.utils.module_loading import import_string
from django.utils.translation import gettext as _
from netbox.context import current_request
from netbox.models import ChangeLoggedModel
from netbox.models import NetBoxModel
from netbox.models import PrimaryModel
from netbox.models.features import JobsMixin
from netbox.models.features import TagsMixin
from netbox_branching.choices import BranchStatusChoices
from netbox_branching.contextvars import active_branch
from netbox_branching.models import Branch
from netbox_branching.utilities import supports_branching
from utilities.querysets import RestrictedQuerySet
from utilities.request import NetBoxFakeRequest

from .choices import IPFabricRawDataTypeChoices
from .choices import IPFabricSnapshotStatusModelChoices
from .choices import IPFabricSourceTypeChoices
from .choices import IPFabricTransformMapSourceModelChoices
from .choices import required_transform_map_contenttypes
from .signals import clear_other_primary_ip
from .utilities.ipfutils import IPFabric
from .utilities.ipfutils import IPFabricSyncRunner
from .utilities.ipfutils import render_jinja2
from .utilities.logging import SyncLogging


logger = logging.getLogger("ipfabric_netbox.models")


def apply_tags(object, tags, connection_name=None):
    def _apply(object):
        object.snapshot()
        for tag in tags:
            if hasattr(object, "tags"):
                object.tags.add(tag)
        object.save(using=connection_name)

    _apply(object)


IPFabricSupportedSyncModels = Q(
    Q(app_label="dcim", model="site")
    | Q(app_label="dcim", model="manufacturer")
    | Q(app_label="dcim", model="platform")
    | Q(app_label="dcim", model="devicerole")
    | Q(app_label="dcim", model="devicetype")
    | Q(app_label="dcim", model="device")
    | Q(app_label="dcim", model="virtualchassis")
    | Q(app_label="dcim", model="interface")
    | Q(app_label="dcim", model="macaddress")
    | Q(app_label="ipam", model="vlan")
    | Q(app_label="ipam", model="vrf")
    | Q(app_label="ipam", model="prefix")
    | Q(app_label="ipam", model="ipaddress")
    | Q(app_label="contenttypes", model="contenttype")
    | Q(app_label="tenancy", model="tenant")
    | Q(app_label="dcim", model="inventoryitem")
)


IPFabricRelationshipFieldSourceModels = Q(
    Q(app_label="dcim")
    | Q(app_label="ipam")
    | Q(app_label="tenancy")
    | Q(app_label="contenttypes", model="contenttype")
)


class IPFabricTransformMapGroup(NetBoxModel):
    name = models.CharField(max_length=100, unique=True)
    description = models.TextField(blank=True, null=True)

    class Meta:
        ordering = ("pk",)
        verbose_name = "IP Fabric Transform Map Group"
        verbose_name_plural = "IP Fabric Transform Map Groups"

    def __str__(self):
        return self.name

    def get_absolute_url(self):
        return reverse(
            "plugins:ipfabric_netbox:ipfabrictransformmapgroup", args=[self.pk]
        )


class IPFabricTransformMap(NetBoxModel):
    name = models.CharField(max_length=100)
    source_model = models.CharField(
        max_length=50, choices=IPFabricTransformMapSourceModelChoices
    )
    target_model = models.ForeignKey(
        to=ContentType,
        related_name="+",
        verbose_name="Target Model",
        limit_choices_to=IPFabricSupportedSyncModels,
        help_text=_("The object(s) to which transform map target applies."),
        on_delete=models.PROTECT,
        blank=False,
        null=False,
    )
    group = models.ForeignKey(
        to=IPFabricTransformMapGroup,
        on_delete=models.CASCADE,
        related_name="transform_maps",
        blank=True,
        null=True,
    )

    class Meta:
        ordering = ("pk",)
        verbose_name = "IP Fabric Transform Map"
        verbose_name_plural = "IP Fabric Transform Maps"

    def __str__(self):
        if self.source_model and self.target_model:
            return f"{self.source_model} - {self.target_model}"
        else:
            return "Transform Map"

    def get_absolute_url(self):
        return reverse("plugins:ipfabric_netbox:ipfabrictransformmap", args=[self.pk])

    @property
    def docs_url(self):
        # TODO: Add docs url
        return ""

    def clean(self):
        cleaned_data = super().clean()
        qs = IPFabricTransformMap.objects.filter(
            group=self.group,
            target_model_id=self.target_model_id,
        )
        if self.pk:
            qs = qs.exclude(pk=self.pk)
        if qs.exists():
            err_msg = _(
                "A transform map with this group and target model already exists."
            )
            raise ValidationError(
                {
                    "group": err_msg,
                    "target_model": err_msg,
                }
            )
        return cleaned_data

    @functools.cache
    def get_models(self):
        _context = dict()

        for app, app_models in apps.all_models.items():
            _context.setdefault(app, {})
            for model in app_models:
                if isinstance(model, str):
                    model = apps.get_registered_model(app, model)
                if not supports_branching(model):
                    continue
                _context[app][model.__name__] = model
        _context["contenttypes"] = {}
        _context["contenttypes"]["ContentType"] = ContentType
        return _context

    def build_relationships(self, source_data):
        relationship_maps = self.relationship_maps.all()
        rel_dict = {}
        rel_dict_coalesce = {}

        for field in relationship_maps:
            if not field.template:
                continue
            context = {
                "object": source_data,
            }
            context.update(self.get_models())
            text = render_jinja2(field.template, context).strip()
            if text:
                try:
                    pk = int(text)
                except ValueError:
                    pk = text

                if isinstance(pk, int):
                    related_object = field.source_model.model_class().objects.get(pk=pk)
                else:
                    related_object = ast.literal_eval(pk)

                if not field.coalesce:
                    rel_dict[field.target_field] = related_object
                else:
                    if related_object is None:
                        # We are searching by this field, so we need to set it to None
                        rel_dict_coalesce[field.target_field + "__isnull"] = True
                    else:
                        rel_dict_coalesce[field.target_field] = related_object
        return rel_dict, rel_dict_coalesce

    def get_context(self, source_data):
        new_data = deepcopy(source_data)
        relationship, coalesce_relationship = self.build_relationships(
            source_data=source_data
        )
        if relationship:
            new_data["relationship"] = relationship
        if coalesce_relationship:
            new_data["relationship_coalesce"] = coalesce_relationship
        context = self.render(new_data)
        return context

    def update_or_create_instance(self, context, tags=[], connection_name=None):
        target_class = self.target_model.model_class()
        queryset = target_class.objects.using(connection_name)

        # Don't change context since it's used in case of exception for IPFabricIngestionIssue
        context = deepcopy(context)
        defaults = context.pop("defaults", {})

        with transaction.atomic(using=connection_name):
            try:
                # For correct ObjectChange on UPDATE we need to create snapshot
                # NetBox does this in UI using views, we need to do it manually
                # See NetBox docs Customization -> Custom Scripts -> Change Logging
                instance = queryset.get(**context)
                instance.snapshot()
                changed = False
                for attr, value in defaults.items():
                    # Only run data validation and save if something has changed
                    if getattr(instance, attr) == value:
                        continue
                    changed = True
                    setattr(instance, attr, value)
                if changed:
                    instance.full_clean()
                    instance.save(using=connection_name)
            except target_class.DoesNotExist:
                for field in list(context.keys()):
                    # When assigning we need to replace `field__isnull=True` with `field=None`
                    if field.endswith("__isnull"):
                        context[field[:-8]] = None
                        del context[field]
                instance = queryset.create(**context, **defaults)
                instance.full_clean()

            apply_tags(instance, tags, connection_name)

        return instance

    def render(self, source_data):
        data = {"defaults": {}}
        for field in self.field_maps.all():
            if field.template:
                context = {
                    "object": source_data,
                    field.source_field: source_data[field.source_field],
                }
                context.update(self.get_models())
                text = render_jinja2(field.template, context).strip()
            else:
                text = source_data[field.source_field]

            if text is not None:
                if isinstance(text, str):
                    if text.lower() in ["true"]:
                        text = True
                    elif text.lower() in ["false"]:
                        text = False
                    elif text.lower() in ["none"]:
                        text = None

                    if text:
                        target_field = getattr(
                            self.target_model.model_class(), field.target_field
                        )
                        target_field_type = target_field.field.get_internal_type()
                        if "integer" in target_field_type.lower():
                            text = int(text)

            if not field.coalesce:
                data["defaults"][field.target_field] = text
            else:
                if text is None:
                    data[field.target_field + "__isnull"] = True
                else:
                    data[field.target_field] = text

        if relationship := source_data.get("relationship"):
            data["defaults"].update(relationship)

        if relationship_coalesce := source_data.get("relationship_coalesce"):
            data.update(relationship_coalesce)

        return data


class IPFabricRelationshipField(models.Model):
    transform_map = models.ForeignKey(
        to=IPFabricTransformMap,
        on_delete=models.CASCADE,
        related_name="relationship_maps",
        editable=True,
    )
    source_model = models.ForeignKey(
        ContentType,
        related_name="ipfabric_transform_fields",
        limit_choices_to=IPFabricRelationshipFieldSourceModels,
        verbose_name="Source Model",
        on_delete=models.PROTECT,
        blank=False,
        null=False,
    )
    target_field = models.CharField(max_length=100)
    coalesce = models.BooleanField(default=False)
    template = models.TextField(
        help_text=_(
            "Jinja2 template code, return an integer to create a relationship between the source and target model. True, False and None are also supported."
        ),
        blank=True,
        default="",
    )

    objects = RestrictedQuerySet.as_manager()

    class Meta:
        ordering = ("transform_map",)
        verbose_name = "IP Fabric Relationship Field"
        verbose_name_plural = "IP Fabric Relationship Fields"

    @property
    def docs_url(self):
        # TODO: Add docs url
        return ""


class IPFabricTransformField(models.Model):
    transform_map = models.ForeignKey(
        to=IPFabricTransformMap,
        on_delete=models.CASCADE,
        related_name="field_maps",
        editable=True,
    )
    source_field = models.CharField(max_length=100)
    target_field = models.CharField(max_length=100)
    coalesce = models.BooleanField(default=False)

    objects = RestrictedQuerySet.as_manager()

    template = models.TextField(
        help_text=_("Jinja2 template code to be rendered into the target field."),
        blank=True,
        default="",
    )

    class Meta:
        ordering = ("transform_map",)
        verbose_name = "IP Fabric Transform Field"
        verbose_name_plural = "IP Fabric Transform Fields"

    @property
    def docs_url(self):
        # TODO: Add docs url
        return ""


class IPFabricClient:
    def get_client(self, parameters):
        try:
            ipf = IPFabric(parameters=parameters)
            return ipf.ipf
        except httpx.ConnectError as e:
            if "CERTIFICATE_VERIFY_FAILED" in str(e):
                error_message = (
                    "SSL certificate verification failed, self-signed cert? "
                    "<a href='https://docs.ipfabric.io/main/integrations/netbox-plugin/user_guide/10_FAQ/' target='_blank'>Check out our FAQ documentation.</a>"
                )
            else:
                error_message = str(e)
            self.handle_sync_failure("ConnectError", e, error_message)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                error_message = "Authentication failed, check API key."
            else:
                error_message = str(e)
            self.handle_sync_failure("HTTPStatusError", e, error_message)
        except Exception as e:
            self.handle_sync_failure("Error", e)

    def handle_sync_failure(self, failure_type, exception, message=None):
        self.status = DataSourceStatusChoices.FAILED

        if message:
            self.logger.log_failure(
                f"{message} ({failure_type}): `{exception}`", obj=self
            )
        else:
            self.logger.log_failure(f"Syncing Snapshot Failed: `{exception}`", obj=self)


class IPFabricSource(IPFabricClient, JobsMixin, PrimaryModel):
    name = models.CharField(max_length=100, unique=True)
    type = models.CharField(
        verbose_name=_("type"),
        max_length=50,
        choices=IPFabricSourceTypeChoices,
        default=IPFabricSourceTypeChoices.LOCAL,
    )
    url = models.CharField(max_length=200, verbose_name=_("URL"))
    status = models.CharField(
        max_length=50,
        choices=DataSourceStatusChoices,
        default=DataSourceStatusChoices.NEW,
        editable=False,
    )
    parameters = models.JSONField(blank=True, null=True)
    last_synced = models.DateTimeField(blank=True, null=True, editable=True)

    class Meta:
        ordering = ("name",)
        verbose_name = "IP Fabric Source"
        verbose_name_plural = "IP Fabric Sources"

    def __str__(self):
        return f"{self.name}"

    def get_absolute_url(self):
        return reverse("plugins:ipfabric_netbox:ipfabricsource", args=[self.pk])

    @property
    def ready_for_sync(self):
        return self.status not in (
            DataSourceStatusChoices.QUEUED,
            DataSourceStatusChoices.SYNCING,
        )

    @property
    def docs_url(self):
        # TODO: Add docs url
        return ""

    def clean(self):
        super().clean()

        self.url = self.url.rstrip("/")

    def enqueue_sync_job(self, request):
        # Set the status to "syncing"
        self.status = DataSourceStatusChoices.QUEUED
        IPFabricSource.objects.filter(pk=self.pk).update(status=self.status)

        # Enqueue a sync job
        return Job.enqueue(
            import_string("ipfabric_netbox.jobs.sync_ipfabricsource"),
            name=f"{self.name} Snapshot Sync",
            instance=self,
            user=request.user,
        )

    def sync(self, job):
        self.logger = SyncLogging(job=job.pk)
        if self.status == DataSourceStatusChoices.SYNCING:
            self.logger.log_failure(
                "Cannot initiate sync; syncing already in progress.", obj=self
            )
            raise SyncError("Cannot initiate sync; syncing already in progress.")

        pre_sync.send(sender=self.__class__, instance=self)

        self.status = DataSourceStatusChoices.SYNCING
        IPFabricSource.objects.filter(pk=self.pk).update(status=self.status)

        # Begin Sync
        try:
            self.logger.log_info(f"Syncing snapshots from {self.name}", obj=self)
            logger.debug(f"Syncing snapshots from {self.url}")

            self.parameters["base_url"] = self.url
            ipf = self.get_client(parameters=self.parameters)

            if not ipf:
                raise SyncError("Unable to connect to IP Fabric.")

            for snapshot_id, value in ipf.snapshots.items():
                if snapshot_id not in ["$prev", "$lastLocked"]:
                    if value.name:
                        name = (
                            value.name
                            + " - "
                            + value.start.strftime("%d-%b-%y %H:%M:%S")
                        )
                    else:
                        name = value.start.strftime("%d-%b-%y %H:%M:%S")

                    if value.status == "done":
                        status = "loaded"
                    else:
                        status = value.status

                    data = {
                        "name": name,
                        "data": json.loads(value.model_dump_json(exclude={"client"})),
                        "date": value.start,
                        "created": timezone.now(),
                        "last_updated": timezone.now(),
                        "status": status,
                    }
                    snapshot, _ = IPFabricSnapshot.objects.update_or_create(
                        source=self, snapshot_id=snapshot_id, defaults=data
                    )
                    self.logger.log_info(
                        f"Created/Updated Snapshot {snapshot.name} ({snapshot.snapshot_id})",
                        obj=snapshot,  # noqa E225
                    )
            self.status = DataSourceStatusChoices.COMPLETED
            self.logger.log_success(f"Completed syncing snapshots from {self.name}")
            logger.debug(f"Completed syncing snapshots from {self.url}")
        except Exception as e:
            self.handle_sync_failure(type(e).__name__, e)
        finally:
            self.last_synced = timezone.now()
            IPFabricSource.objects.filter(pk=self.pk).update(
                status=self.status, last_synced=self.last_synced
            )
            self.logger.log_info("Sync job completed.", obj=self)
            if job:
                job.data = self.logger.log_data
        # Emit the post_sync signal
        # post_sync.send(sender=self.__class__, instance=self)

    @classmethod
    def get_for_site(cls, site: Site):
        """Get all snapshots containing the given site."""
        return cls.objects.filter(
            Q(snapshots__data__sites__contains=[site.name])
        ).distinct()


class IPFabricSnapshot(TagsMixin, ChangeLoggedModel):
    source = models.ForeignKey(
        to=IPFabricSource,
        on_delete=models.CASCADE,
        related_name="snapshots",
        editable=False,
    )
    name = models.CharField(max_length=200)
    snapshot_id = models.CharField(max_length=100)
    data = models.JSONField(blank=True, null=True)
    date = models.DateTimeField(blank=True, null=True, editable=False)
    status = models.CharField(
        max_length=50,
        choices=IPFabricSnapshotStatusModelChoices,
        default=IPFabricSnapshotStatusModelChoices.STATUS_UNLOADED,
    )

    objects = RestrictedQuerySet.as_manager()

    class Meta:
        ordering = ("source", "-date")
        verbose_name = "IP Fabric Snapshot"
        verbose_name_plural = "IP Fabric Snapshots"

    def __str__(self):
        return f"{self.name} - {self.snapshot_id}"

    def get_absolute_url(self):
        return reverse("plugins:ipfabric_netbox:ipfabricsnapshot", args=[self.pk])

    def get_status_color(self):
        return IPFabricSnapshotStatusModelChoices.colors.get(self.status)

    @property
    def sites(self):
        if self.data:
            sites = self.data.get("sites", None)
            if sites:
                return sites
            else:
                return []
        else:
            return []


class IPFabricSync(IPFabricClient, JobsMixin, TagsMixin, ChangeLoggedModel):
    objects = RestrictedQuerySet.as_manager()
    name = models.CharField(max_length=100, unique=True)
    snapshot_data = models.ForeignKey(
        to=IPFabricSnapshot,
        on_delete=models.CASCADE,
        related_name="snapshots",
    )
    status = models.CharField(
        max_length=50,
        choices=DataSourceStatusChoices,
        default=DataSourceStatusChoices.NEW,
        editable=False,
    )
    parameters = models.JSONField(blank=True, null=True)
    auto_merge = models.BooleanField(default=False)
    update_custom_fields = models.BooleanField(default=True)
    last_synced = models.DateTimeField(blank=True, null=True, editable=False)
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
        ordering = ["pk"]
        verbose_name = "IP Fabric Sync"

    def __str__(self):
        return f"{self.name}"

    @property
    def docs_url(self):
        # TODO: Add docs url
        return ""

    @property
    def logger(self):
        return getattr(self, "_logger", SyncLogging(job=self.pk))

    @logger.setter
    def logger(self, value):
        self._logger = value

    def get_absolute_url(self):
        return reverse("plugins:ipfabric_netbox:ipfabricsync", args=[self.pk])

    def get_status_color(self):
        return DataSourceStatusChoices.colors.get(self.status)

    @property
    def ready_for_sync(self):
        if self.status not in (DataSourceStatusChoices.SYNCING,):
            if self.snapshot_data.source.type == "remote":
                if self.snapshot_data.ipf_data.count() > 0:
                    return True
                else:
                    return False
            else:
                return True
        else:
            return False

    @property
    def last_ingestion(self):
        return self.ipfabricingestion_set.last()

    @staticmethod
    def get_transform_maps(group_ids=None):
        """
        Returns a queryset of IPFabricTransformMap objects that would be used by this sync,
        following group and default precedence logic.
        """
        default_maps = IPFabricTransformMap.objects.filter(group__isnull=True)
        group_ids = group_ids or []
        maps_by_target = {tm.target_model_id: tm for tm in default_maps}
        # Replace default maps with the ones from the groups, in given order.
        if group_ids:
            for group_id in group_ids:
                group_maps = IPFabricTransformMap.objects.filter(group_id=group_id)
                for tm in group_maps:
                    maps_by_target[tm.target_model_id] = tm
        return IPFabricTransformMap.objects.filter(
            pk__in=[tm.pk for tm in maps_by_target.values()]
        )

    def enqueue_sync_job(self, adhoc=False, user=None):
        # Set the status to "syncing"
        self.status = DataSourceStatusChoices.QUEUED
        IPFabricSync.objects.filter(pk=self.pk).update(status=self.status)

        Job.enqueue(
            import_string("ipfabric_netbox.jobs.sync_ipfabricsource"),
            name=f"{self.name} Snapshot Sync (Pre Ingestion)",
            instance=self.snapshot_data.source,
            user=self.user,
        )

        # Enqueue a sync job
        if not user:
            user = self.user

        if not adhoc and self.scheduled:
            job = Job.enqueue(
                import_string("ipfabric_netbox.jobs.sync_ipfabric"),
                name=f"{self.name} - (scheduled)",
                instance=self,
                user=self.user,
                schedule_at=self.scheduled,
                interval=self.interval,
            )
        elif adhoc:
            job = Job.enqueue(
                import_string("ipfabric_netbox.jobs.sync_ipfabric"),
                instance=self,
                user=user,
                name=f"{self.name} - (adhoc)",
                adhoc=adhoc,
            )
        return job

    def sync(self, job=None):
        if job:
            self.logger = SyncLogging(job=job.pk)
            user = job.user
        else:
            self.logger = SyncLogging(job=self.pk)
            user = None

        maps = self.get_transform_maps(self.parameters.get("groups", []))
        missing = []
        for app_label, model in required_transform_map_contenttypes:
            if not maps.filter(
                target_model=ContentType.objects.get(app_label=app_label, model=model)
            ):
                missing.append(f"{app_label}.{model}")
        if missing:
            self.logger.log_failure(
                f"Combination of these transform map groups failed validation. Missing maps: {missing}.",
                obj=self,
            )
            raise SyncError(
                f"Combination of these transform map groups failed validation. Missing maps: {missing}."
            )

        if self.status == DataSourceStatusChoices.SYNCING:
            raise SyncError("Cannot initiate sync; ingestion already in progress.")

        pre_sync.send(sender=self.__class__, instance=self)

        self.status = DataSourceStatusChoices.SYNCING
        IPFabricSync.objects.filter(pk=self.pk).update(status=self.status)

        # Begin Sync
        self.logger.log_info(
            f"Ingesting data from {self.snapshot_data.source.name}", obj=self
        )
        logger.info(f"Ingesting data from {self.snapshot_data.source.name}")

        self.snapshot_data.source.parameters["base_url"] = self.snapshot_data.source.url
        self.parameters["snapshot_id"] = self.snapshot_data.snapshot_id
        self.logger.log_info(
            f"Syncing with the following data {json.dumps(self.parameters)}", obj=self
        )
        logger.info(f"Syncing with the following data {json.dumps(self.parameters)}")

        current_time = str(timezone.now())
        ingestion = IPFabricIngestion.objects.create(sync=self, job=job)
        try:
            branch = Branch(name=f"IP Fabric Sync {current_time}")
            branch.save(provision=False)
            ingestion.branch = branch
            ingestion.save()

            if job:
                # Re-assign the Job from IPFSync to IPFabricIngestion so it is listed in the ingestion
                job.object_type = ObjectType.objects.get_for_model(ingestion)
                job.object_id = ingestion.pk
                job.save()
            branch.provision(user=user)
            branch.refresh_from_db()
            if branch.status == BranchStatusChoices.FAILED:
                print("Branch Failed")
                self.logger.log_failure(f"Branch Failed: `{branch}`", obj=branch)
                raise SyncError("Branch Creation Failed")

            self.logger.log_info(f"New branch Created {branch.name}", obj=branch)
            logger.info(f"New branch Created {branch.name}")

            self.logger.log_info("Fetching IP Fabric Client", obj=branch)
            logger.info("Fetching IP Fabric Client")

            if self.snapshot_data.source.type == IPFabricSourceTypeChoices.LOCAL:
                ipf = self.get_client(parameters=self.snapshot_data.source.parameters)
                if not ipf:
                    logger.debug("Unable to connect to IP Fabric.")
                    raise SyncError("Unable to connect to IP Fabric.")
            else:
                ipf = None

            runner = IPFabricSyncRunner(
                client=ipf,
                ingestion=ingestion,
                settings=self.parameters,
                sync=self,
            )

            # Not using `deactivate_branch` since that does not clean up on Exception
            current_branch = active_branch.get()
            if not (token := current_request.get()):
                # This allows for ChangeLoggingMiddleware to create ObjectChanges
                token = current_request.set(
                    NetBoxFakeRequest({"id": uuid4(), "user": user})
                )
            try:
                active_branch.set(branch)
                try:
                    runner.collect_and_sync(
                        ingestion=IPFabricIngestion.objects.get(pk=ingestion.pk)
                    )
                finally:
                    active_branch.set(None)
            finally:
                current_request.set(token.old_value)
                active_branch.set(current_branch)

            if self.status != DataSourceStatusChoices.FAILED:
                self.status = DataSourceStatusChoices.COMPLETED

        except Exception as e:
            self.status = DataSourceStatusChoices.FAILED
            self.logger.log_failure(f"Ingestion Failed: `{e}`", obj=ingestion)
            self.logger.log_failure(
                f"Stack Trace: `{traceback.format_exc()}`", obj=ingestion
            )
            logger.debug(f"Ingestion Failed: `{e}`")

        logger.debug(f"Completed ingesting data from {self.snapshot_data.source.name}")
        self.logger.log_info(
            f"Completed ingesting data from {self.snapshot_data.source.name}", obj=self
        )

        self.last_synced = timezone.now()

        if self.auto_merge and self.status == DataSourceStatusChoices.COMPLETED:
            self.logger.log_info("Auto Merging Ingestion", obj=ingestion)
            logger.info("Auto Merging Ingestion")
            try:
                ingestion.enqueue_merge_job(user=user)
                self.logger.log_info("Auto Merge Job Enqueued", obj=ingestion)
                logger.info("Auto Merge Job Enqueued")
            except NameError:
                self.logger.log_failure(
                    "Failed to Auto Merge, IPFabricIngestion does not exist",
                    obj=ingestion,
                )
                logger.debug("Failed to Auto Merge, IPFabricIngestion does not exist")

        IPFabricSync.objects.filter(pk=self.pk).update(
            status=self.status, last_synced=self.last_synced
        )
        if job:
            job.data = self.logger.log_data


class IPFabricIngestion(JobsMixin, models.Model):
    """
    Links IP Fabric Sync to its Branches.
    """

    objects = RestrictedQuerySet.as_manager()

    sync = models.ForeignKey(IPFabricSync, on_delete=models.CASCADE)
    job = models.ForeignKey(Job, on_delete=models.SET_NULL, null=True)
    branch = models.OneToOneField(Branch, on_delete=models.SET_NULL, null=True)

    class Meta:
        ordering = ("pk",)
        verbose_name = "IP Fabric Ingestion"
        verbose_name_plural = "IP Fabric Ingestions"

    def __str__(self):
        return self.name

    @property
    def name(self):
        if self.branch:
            return self.branch.name
        try:
            return f"{self.sync.name} (Ingestion {self.pk})"
        except IPFabricIngestion.sync.RelatedObjectDoesNotExist:
            return f"Ingestion {self.pk} (No Sync)"

    def get_absolute_url(self):
        return reverse("plugins:ipfabric_netbox:ipfabricingestion", args=[self.pk])

    def enqueue_merge_job(self, user, remove_branch=False):
        # Set the status to "queued"
        self.status = DataSourceStatusChoices.QUEUED
        IPFabricSync.objects.filter(ipfabricingestion=self.pk).update(
            status=self.status
        )

        # Enqueue a sync job
        return Job.enqueue(
            import_string("ipfabric_netbox.jobs.merge_ipfabric_ingestion"),
            name=f"{self.name} Merge",
            instance=self,
            user=user,
            remove_branch=remove_branch,
        )

    def get_logs(self):
        if self.job.data:
            job_results = self.job.data
        else:
            job_results = cache.get(f"ipfabric_sync_{self.job.pk}")
            if not job_results:
                job_results = cache.get(f"ipfabric_sync_{self.sync.pk}")
        return job_results

    def get_statistics(self):
        job_results = self.get_logs()
        statistics = {}
        if job_results:
            for model, stats in job_results["statistics"].items():
                if not stats["total"]:
                    continue
                if stats["total"] > 0:
                    statistics[model] = stats["current"] / stats["total"] * 100
                else:
                    statistics[model] = stats["current"] / 1 * 100
        return {"job_results": job_results, "statistics": statistics}

    def sync_merge(self):
        ipfabricsync = self.sync
        if ipfabricsync.status == DataSourceStatusChoices.SYNCING:
            raise SyncError("Cannot initiate merge; merge already in progress.")

        pre_sync.send(sender=self.__class__, instance=self)

        ipfabricsync.status = DataSourceStatusChoices.SYNCING
        IPFabricSync.objects.filter(ipfabricingestion=self.pk).update(
            status=self.sync.status
        )

        # Begin Sync
        logger.debug(f"Merging {self.name}")
        try:
            signals.pre_save.connect(clear_other_primary_ip, sender=Device)
            signals.post_save.disconnect(
                assign_virtualchassis_master, sender=VirtualChassis
            )
            self.branch.merge(user=self.sync.user)
            signals.post_save.connect(
                assign_virtualchassis_master, sender=VirtualChassis
            )
            signals.pre_save.disconnect(clear_other_primary_ip, sender=Device)
            ipfabricsync.status = DataSourceStatusChoices.COMPLETED
        except Exception as e:
            ipfabricsync.status = DataSourceStatusChoices.FAILED
            logger.debug(f"Merging {self.name} Failed: `{e}`")

        logger.debug(f"Completed merge {self.name}")

        ipfabricsync.last_synced = timezone.now()
        IPFabricSync.objects.filter(ipfabricingestion=self.pk).update(
            status=ipfabricsync.status, last_synced=ipfabricsync.last_synced
        )


class IPFabricIngestionIssue(models.Model):
    objects = RestrictedQuerySet.as_manager()

    ingestion = models.ForeignKey(
        to="IPFabricIngestion", on_delete=models.CASCADE, related_name="issues"
    )
    timestamp = models.DateTimeField(default=timezone.now)
    model = models.CharField(max_length=100, blank=True, null=True)
    message = models.TextField()
    raw_data = models.TextField(blank=True, default="")
    coalesce_fields = models.TextField(blank=True, default="")
    defaults = models.TextField(blank=True, default="")
    exception = models.TextField()

    class Meta:
        ordering = ["timestamp"]
        verbose_name = "IP Fabric Ingestion Issue"
        verbose_name_plural = "IP Fabric Ingestion Issues"

    def __str__(self):
        return f"[{self.timestamp}] {self.message}"


class IPFabricData(models.Model):
    snapshot_data = models.ForeignKey(
        to=IPFabricSnapshot,
        on_delete=models.CASCADE,
        related_name="ipf_data",
    )
    data = models.JSONField(blank=True, null=True)
    type = models.CharField(
        max_length=50,
        choices=IPFabricRawDataTypeChoices,
    )
    objects = RestrictedQuerySet.as_manager()

    def get_absolute_url(self):
        return reverse("plugins:ipfabric_netbox:ipfabricdata_data", args=[self.pk])
