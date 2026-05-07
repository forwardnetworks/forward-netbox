import logging
import traceback
from contextlib import contextmanager

from core.exceptions import SyncError
from core.models import Job
from core.signals import pre_sync
from dcim.models import Site
from dcim.models import VirtualChassis
from django.conf import settings
from django.contrib.contenttypes.models import ContentType
from django.core.cache import cache
from django.core.exceptions import ValidationError
from django.core.validators import MaxValueValidator
from django.core.validators import MinValueValidator
from django.db import models
from django.db.models import Q
from django.db.models import signals
from django.urls import reverse
from django.utils import timezone
from django.utils.dateparse import parse_datetime
from django.utils.module_loading import import_string
from django.utils.translation import gettext as _
from netbox.models import ChangeLoggedModel
from netbox.models import PrimaryModel
from netbox.models.features import JobsMixin
from netbox.models.features import TagsMixin
from netbox_branching.models import Branch
from utilities.querysets import RestrictedQuerySet

from .choices import forward_configured_models
from .choices import FORWARD_OPTIONAL_MODELS
from .choices import FORWARD_SUPPORTED_MODELS
from .choices import ForwardDriftPolicyBaselineChoices
from .choices import ForwardIngestionPhaseChoices
from .choices import ForwardSourceDeploymentChoices
from .choices import ForwardSourceStatusChoices
from .choices import ForwardSyncStatusChoices
from .choices import ForwardValidationStatusChoices
from .exceptions import ForwardSyncError
from .utilities.branch_budget import BRANCH_RUN_STATE_PARAMETER
from .utilities.branch_budget import build_branch_budget_hints
from .utilities.branch_budget import DEFAULT_MAX_CHANGES_PER_BRANCH
from .utilities.branch_budget import MODEL_CHANGE_DENSITY_PARAMETER
from .utilities.execution_telemetry import build_branch_run_summary
from .utilities.execution_telemetry import build_ingestion_execution_summary
from .utilities.execution_telemetry import build_sync_execution_summary
from .utilities.forward_api import ForwardClient
from .utilities.forward_api import LATEST_PROCESSED_SNAPSHOT
from .utilities.forward_api import MAX_NQE_PAGE_SIZE
from .utilities.logging import SyncLogging
from .utilities.sync_contracts import normalize_coalesce_fields
from .utilities.sync_contracts import validate_query_shape_for_model

logger = logging.getLogger("forward_netbox.models")

try:
    from dcim.signals import sync_cached_scope_fields
except ImportError:  # pragma: no cover - compatibility with older NetBox point releases
    sync_cached_scope_fields = None


FORWARD_SUPPORTED_SYNC_MODELS = Q()
for model_string in FORWARD_SUPPORTED_MODELS:
    app_label, model_name = model_string.split(".")
    FORWARD_SUPPORTED_SYNC_MODELS |= Q(app_label=app_label, model=model_name)

FORWARD_INGESTION_SYNC_MODE_CHOICES = (
    ("full", _("Full")),
    ("diff", _("Diff")),
    ("hybrid", _("Hybrid")),
)


class ForwardPluginModelDocsMixin:
    @property
    def docs_url(self):
        return ""


@contextmanager
def suppress_branch_merge_side_effect_signals():
    from dcim.signals import assign_virtualchassis_master

    signals.post_save.disconnect(
        assign_virtualchassis_master,
        sender=VirtualChassis,
    )
    if sync_cached_scope_fields is not None:
        signals.post_save.disconnect(sync_cached_scope_fields, sender=Site)
    try:
        yield
    finally:
        signals.post_save.connect(
            assign_virtualchassis_master,
            sender=VirtualChassis,
        )
        if sync_cached_scope_fields is not None:
            signals.post_save.connect(sync_cached_scope_fields, sender=Site)


class ForwardSource(ForwardPluginModelDocsMixin, JobsMixin, PrimaryModel):
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
                "nqe_page_size",
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
        if parameters.get("nqe_page_size") is not None:
            try:
                nqe_page_size = int(parameters.get("nqe_page_size"))
            except (TypeError, ValueError) as exc:
                raise ValidationError(_("`nqe_page_size` must be an integer.")) from exc
            if nqe_page_size < 1 or nqe_page_size > MAX_NQE_PAGE_SIZE:
                raise ValidationError(
                    _(f"`nqe_page_size` must be between 1 and {MAX_NQE_PAGE_SIZE}.")
                )
            parameters["nqe_page_size"] = nqe_page_size
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
            "nqe_page_size",
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


class ForwardNQEMap(ForwardPluginModelDocsMixin, ChangeLoggedModel):
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


class ForwardSync(ForwardPluginModelDocsMixin, JobsMixin, TagsMixin, ChangeLoggedModel):
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
    drift_policy = models.ForeignKey(
        to="ForwardDriftPolicy",
        on_delete=models.SET_NULL,
        related_name="syncs",
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
        return not self.is_waiting_for_branch_merge and self.status not in (
            ForwardSyncStatusChoices.QUEUED,
            ForwardSyncStatusChoices.SYNCING,
            ForwardSyncStatusChoices.MERGING,
        )

    @property
    def ready_to_continue_sync(self):
        return self.has_pending_branch_run and self.ready_for_sync

    @property
    def last_ingestion(self):
        return self.forwardingestion_set.last()

    @property
    def latest_validation_run(self):
        return self.validation_runs.order_by("-pk").first()

    def latest_baseline_ingestion(self, *, exclude_ingestion_id=None):
        queryset = self.forwardingestion_set.filter(
            baseline_ready=True,
        ).exclude(snapshot_id="")
        if exclude_ingestion_id is not None:
            queryset = queryset.exclude(pk=exclude_ingestion_id)
        return queryset.order_by("-pk").first()

    def incremental_diff_baseline(
        self,
        *,
        specs,
        current_snapshot_id,
        exclude_ingestion_id=None,
    ):
        if self.get_snapshot_id() != LATEST_PROCESSED_SNAPSHOT:
            return None
        if not specs or any(not getattr(spec, "query_id", None) for spec in specs):
            return None
        baseline = self.latest_baseline_ingestion(
            exclude_ingestion_id=exclude_ingestion_id
        )
        if baseline is None:
            return None
        if baseline.snapshot_id == current_snapshot_id:
            return None
        return baseline

    def clean(self):
        super().clean()
        parameters = dict(self.parameters or {})
        invalid = sorted(
            set(parameters.keys())
            - {
                "auto_merge",
                "multi_branch",
                "max_changes_per_branch",
                BRANCH_RUN_STATE_PARAMETER,
                "snapshot_id",
                *FORWARD_SUPPORTED_MODELS,
            }
        )
        if invalid:
            raise ValidationError(_(f"Unsupported Forward sync keys: {invalid}"))
        snapshot_id = parameters.get("snapshot_id") or LATEST_PROCESSED_SNAPSHOT
        if not isinstance(snapshot_id, str):
            raise ValidationError(_("`snapshot_id` must be a string."))
        parameters["snapshot_id"] = snapshot_id
        parameters["auto_merge"] = bool(parameters.get("auto_merge", self.auto_merge))
        parameters["multi_branch"] = True
        try:
            max_changes_per_branch = int(
                parameters.get(
                    "max_changes_per_branch",
                    DEFAULT_MAX_CHANGES_PER_BRANCH,
                )
            )
        except (TypeError, ValueError):
            raise ValidationError(
                _("`max_changes_per_branch` must be a positive integer.")
            )
        if max_changes_per_branch < 1:
            raise ValidationError(
                _("`max_changes_per_branch` must be a positive integer.")
            )
        parameters["max_changes_per_branch"] = max_changes_per_branch
        if self.scheduled and self.scheduled < timezone.now():
            raise ValidationError(
                {"scheduled": _("Scheduled time must be in the future.")}
            )
        if not any(
            self.is_model_enabled(model_string)
            for model_string in forward_configured_models()
        ):
            raise ValidationError(_("Select at least one NetBox model to sync."))
        self.auto_merge = parameters["auto_merge"]
        self.parameters = parameters

    def _force_native_branching_execution(self):
        parameters = dict(self.parameters or {})
        parameters["multi_branch"] = True
        try:
            max_changes_per_branch = int(
                parameters.get(
                    "max_changes_per_branch",
                    DEFAULT_MAX_CHANGES_PER_BRANCH,
                )
            )
        except (TypeError, ValueError):
            max_changes_per_branch = DEFAULT_MAX_CHANGES_PER_BRANCH
        parameters["max_changes_per_branch"] = max(1, max_changes_per_branch)
        self.auto_merge = bool(parameters.get("auto_merge", self.auto_merge))
        self.parameters = parameters

    def save(self, *args, **kwargs):
        self._force_native_branching_execution()
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

    def uses_multi_branch(self):
        return True

    def get_branch_run_state(self):
        state = (self.parameters or {}).get(BRANCH_RUN_STATE_PARAMETER) or {}
        return state if isinstance(state, dict) else {}

    def get_model_change_density(self):
        density = (self.parameters or {}).get(MODEL_CHANGE_DENSITY_PARAMETER) or {}
        return density if isinstance(density, dict) else {}

    @property
    def has_pending_branch_run(self):
        state = self.get_branch_run_state()
        return bool(
            state
            and int(state.get("next_plan_index") or 1)
            <= int(state.get("total_plan_items") or 0)
        )

    @property
    def is_waiting_for_branch_merge(self):
        return bool(self.get_branch_run_state().get("awaiting_merge"))

    def set_branch_run_state(self, state):
        parameters = dict(self.parameters or {})
        parameters[BRANCH_RUN_STATE_PARAMETER] = dict(state)
        self.parameters = parameters
        ForwardSync.objects.filter(pk=self.pk).update(parameters=parameters)

    def clear_branch_run_state(self):
        parameters = dict(self.parameters or {})
        if BRANCH_RUN_STATE_PARAMETER in parameters:
            parameters.pop(BRANCH_RUN_STATE_PARAMETER, None)
            self.parameters = parameters
            ForwardSync.objects.filter(pk=self.pk).update(parameters=parameters)

    def set_model_change_density(self, model_change_density):
        normalized = {}
        for model_string, density in (model_change_density or {}).items():
            try:
                density_value = float(density)
            except (TypeError, ValueError):
                continue
            if density_value <= 0:
                continue
            normalized[str(model_string)] = density_value
        parameters = dict(self.parameters or {})
        parameters[MODEL_CHANGE_DENSITY_PARAMETER] = normalized
        self.parameters = parameters
        ForwardSync.objects.filter(pk=self.pk).update(parameters=parameters)

    def get_max_changes_per_branch(self):
        try:
            value = int(
                (self.parameters or {}).get(
                    "max_changes_per_branch",
                    DEFAULT_MAX_CHANGES_PER_BRANCH,
                )
            )
        except (TypeError, ValueError):
            return DEFAULT_MAX_CHANGES_PER_BRANCH
        return max(1, value)

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
        parameters["multi_branch"] = self.uses_multi_branch()
        parameters["max_changes_per_branch"] = self.get_max_changes_per_branch()
        model_change_density = self.get_model_change_density()
        if model_change_density:
            parameters["model_change_density"] = model_change_density
        enabled_models = self.get_model_strings()
        if enabled_models:
            parameters["branch_budget_hints"] = build_branch_budget_hints(
                enabled_models,
                max_changes_per_branch=parameters["max_changes_per_branch"],
                model_change_density=model_change_density,
            )
        state = self.get_branch_run_state()
        if state:
            parameters["branch_run"] = build_branch_run_summary(state)
        parameters["models"] = enabled_models
        return parameters

    def get_execution_summary(self):
        enabled_models = self.get_model_strings()
        max_changes_per_branch = self.get_max_changes_per_branch()
        model_change_density = self.get_model_change_density()
        state = self.get_branch_run_state()
        last_ingestion = self.last_ingestion
        return build_sync_execution_summary(
            enabled_models=enabled_models,
            max_changes_per_branch=max_changes_per_branch,
            model_change_density=model_change_density,
            branch_run_state=state,
            latest_ingestion_summary=(
                last_ingestion.get_execution_summary() if last_ingestion else None
            ),
        )

    def get_sync_activity(self):
        state = self.get_branch_run_state()
        phase_message = state.get("phase_message") or ""
        phase = state.get("phase") or ""
        elapsed = self._format_phase_elapsed(state.get("phase_started"))
        if phase_message:
            return f"{phase_message} ({elapsed})" if elapsed else phase_message
        if phase:
            phase_label = phase.replace("_", " ")
            return f"{phase_label} ({elapsed})" if elapsed else phase_label
        if self.status == ForwardSyncStatusChoices.SYNCING:
            return "Sync is running."
        if self.is_waiting_for_branch_merge:
            return "Waiting for branch merge."
        return ""

    def _format_phase_elapsed(self, phase_started):
        if not phase_started:
            return ""
        started = parse_datetime(str(phase_started))
        if started is None:
            return ""
        if timezone.is_naive(started):
            started = timezone.make_aware(started, timezone.get_current_timezone())
        elapsed_seconds = max(0, int((timezone.now() - started).total_seconds()))
        minutes, seconds = divmod(elapsed_seconds, 60)
        hours, minutes = divmod(minutes, 60)
        if hours:
            return f"{hours}h {minutes}m"
        if minutes:
            return f"{minutes}m {seconds}s"
        return f"{seconds}s"

    def is_model_enabled(self, model_string):
        if model_string not in forward_configured_models():
            return False
        parameters = self.parameters or {}
        return parameters.get(
            model_string,
            model_string not in FORWARD_OPTIONAL_MODELS,
        )

    def enabled_models(self):
        return [
            model_string
            for model_string in forward_configured_models()
            if self.is_model_enabled(model_string)
        ]

    def enqueue_sync_job(self, adhoc=False, user=None):
        if self.is_waiting_for_branch_merge:
            raise SyncError(
                "Forward sync is waiting for the current shard branch to be merged."
            )
        if not user:
            user = self.user
        if adhoc or self.status == ForwardSyncStatusChoices.NEW:
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

    def enqueue_validation_job(self, adhoc=False, user=None):
        if not user:
            user = self.user
        return Job.enqueue(
            import_string("forward_netbox.jobs.validate_forwardsync"),
            instance=self,
            user=user,
            name=f"{self.name} - validation",
            adhoc=adhoc,
            schedule_at=None,
            interval=None,
        )

    def sync(self, job=None, *, max_changes_per_branch=None):
        from .utilities.multi_branch import ForwardMultiBranchExecutor

        if self.is_waiting_for_branch_merge:
            self.logger.log_warning(
                "Forward sync is waiting for the current shard branch to be merged.",
                obj=self,
            )
            return

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
        self.source.status = ForwardSourceStatusChoices.SYNCING
        ForwardSource.objects.filter(pk=self.source.pk).update(
            status=self.source.status
        )
        if max_changes_per_branch is None:
            max_changes_per_branch = self.get_max_changes_per_branch()

        ingestion = None
        executor = None
        try:
            executor = ForwardMultiBranchExecutor(
                self,
                self.source.get_client(),
                self.logger,
                user=user,
                job=job,
            )
            ingestions = executor.run(
                max_changes_per_branch=max_changes_per_branch,
            )
            if not ingestions:
                self.status = ForwardSyncStatusChoices.COMPLETED
                self.logger.log_success("Forward ingestion completed.", obj=self)
                return
            ingestion = ingestions[-1]
            if self.status == ForwardSyncStatusChoices.READY_TO_MERGE:
                self.logger.log_success(
                    "Forward multi-branch shard staged for review.",
                    obj=self,
                )
                return
            self.status = ForwardSyncStatusChoices.COMPLETED
            self.logger.log_success(
                "Forward multi-branch ingestion completed.",
                obj=self,
            )
            return
        except Exception as exc:
            logger.exception("Forward sync failed")
            self.status = ForwardSyncStatusChoices.FAILED
            if ingestion is None:
                ingestion = getattr(executor, "current_ingestion", None)
            if ingestion is None:
                validation_run = getattr(executor, "last_validation_run", None)
                if not isinstance(validation_run, ForwardValidationRun):
                    validation_run = None
                model_results = getattr(executor, "last_model_results", [])
                if not isinstance(model_results, list):
                    model_results = []
                ingestion = ForwardIngestion.objects.create(
                    sync=self,
                    job=job,
                    validation_run=validation_run,
                    model_results=model_results,
                )
            else:
                validation_run = getattr(executor, "last_validation_run", None)
                if (
                    isinstance(validation_run, ForwardValidationRun)
                    and not ingestion.validation_run
                ):
                    ingestion.validation_run = validation_run
                    ingestion.save(update_fields=["validation_run"])
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


class ForwardDriftPolicy(ForwardPluginModelDocsMixin, ChangeLoggedModel):
    objects = RestrictedQuerySet.as_manager()

    name = models.CharField(max_length=100, unique=True)
    enabled = models.BooleanField(default=True)
    baseline_mode = models.CharField(
        max_length=30,
        choices=ForwardDriftPolicyBaselineChoices,
        default=ForwardDriftPolicyBaselineChoices.LATEST_MERGED,
    )
    require_processed_snapshot = models.BooleanField(default=True)
    block_on_query_errors = models.BooleanField(default=True)
    block_on_zero_rows = models.BooleanField(default=False)
    max_deleted_objects = models.PositiveIntegerField(blank=True, null=True)
    max_deleted_percent = models.PositiveIntegerField(
        blank=True,
        null=True,
        validators=(MinValueValidator(0), MaxValueValidator(100)),
    )

    class Meta:
        ordering = ("name",)
        verbose_name = _("Forward Drift Policy")
        verbose_name_plural = _("Forward Drift Policies")
        db_table = "forward_netbox_drift_policy"

    def __str__(self):
        return self.name

    def get_absolute_url(self):
        return reverse("plugins:forward_netbox:forwarddriftpolicy", args=[self.pk])

    def clean(self):
        super().clean()
        if self.max_deleted_objects is None and self.max_deleted_percent is None:
            return
        if self.baseline_mode == ForwardDriftPolicyBaselineChoices.NONE:
            raise ValidationError(
                _("Deletion thresholds require a baseline-enabled policy.")
            )


class ForwardValidationRun(ForwardPluginModelDocsMixin, models.Model):
    objects = RestrictedQuerySet.as_manager()

    sync = models.ForeignKey(
        ForwardSync,
        on_delete=models.CASCADE,
        related_name="validation_runs",
    )
    policy = models.ForeignKey(
        ForwardDriftPolicy,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        related_name="validation_runs",
    )
    job = models.ForeignKey(Job, on_delete=models.SET_NULL, null=True, blank=True)
    status = models.CharField(
        max_length=20,
        choices=ForwardValidationStatusChoices,
        default=ForwardValidationStatusChoices.QUEUED,
    )
    allowed = models.BooleanField(default=False)
    snapshot_selector = models.CharField(max_length=100, blank=True, default="")
    snapshot_id = models.CharField(max_length=100, blank=True, default="")
    baseline_snapshot_id = models.CharField(max_length=100, blank=True, default="")
    snapshot_info = models.JSONField(blank=True, default=dict)
    snapshot_metrics = models.JSONField(blank=True, default=dict)
    model_results = models.JSONField(blank=True, default=list)
    drift_summary = models.JSONField(blank=True, default=dict)
    blocking_reasons = models.JSONField(blank=True, default=list)
    override_applied = models.BooleanField(default=False)
    override_user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="+",
    )
    override_reason = models.TextField(blank=True, default="")
    override_blocking_reasons = models.JSONField(blank=True, default=list)
    override_at = models.DateTimeField(blank=True, null=True)
    created = models.DateTimeField(default=timezone.now, editable=False)
    started = models.DateTimeField(blank=True, null=True)
    completed = models.DateTimeField(blank=True, null=True)

    class Meta:
        ordering = ("-pk",)
        verbose_name = _("Forward Validation Run")
        verbose_name_plural = _("Forward Validation Runs")
        db_table = "forward_netbox_validation_run"

    def __str__(self):
        return f"{self.sync} validation {self.pk or ''}".strip()

    def get_absolute_url(self):
        return reverse("plugins:forward_netbox:forwardvalidationrun", args=[self.pk])

    def force_allow(self, *, user, reason):
        reason = str(reason or "").strip()
        if not reason:
            raise SyncError(
                "Provide a force-allow reason before overriding validation."
            )
        if not self.blocking_reasons:
            raise SyncError("Only blocked validation runs can be force-allowed.")
        self.override_applied = True
        self.allowed = True
        self.status = ForwardValidationStatusChoices.PASSED
        self.override_user = user
        self.override_reason = reason
        self.override_blocking_reasons = list(self.blocking_reasons or [])
        self.override_at = timezone.now()
        self.save(
            update_fields=[
                "override_applied",
                "allowed",
                "status",
                "override_user",
                "override_reason",
                "override_blocking_reasons",
                "override_at",
            ]
        )
        return self


class ForwardIngestion(ForwardPluginModelDocsMixin, JobsMixin, models.Model):
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
    validation_run = models.ForeignKey(
        ForwardValidationRun,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="ingestions",
    )
    branch = models.OneToOneField(
        Branch, on_delete=models.SET_NULL, null=True, blank=True
    )
    snapshot_selector = models.CharField(max_length=100, blank=True, default="")
    snapshot_id = models.CharField(max_length=100, blank=True, default="")
    sync_mode = models.CharField(
        max_length=10,
        choices=FORWARD_INGESTION_SYNC_MODE_CHOICES,
        default="full",
    )
    baseline_ready = models.BooleanField(default=False)
    applied_change_count = models.PositiveIntegerField(default=0)
    failed_change_count = models.PositiveIntegerField(default=0)
    created_change_count = models.PositiveIntegerField(default=0)
    updated_change_count = models.PositiveIntegerField(default=0)
    deleted_change_count = models.PositiveIntegerField(default=0)
    snapshot_info = models.JSONField(blank=True, default=dict)
    snapshot_metrics = models.JSONField(blank=True, default=dict)
    model_results = models.JSONField(blank=True, default=list)
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

    def get_model_results_summary(self):
        return list(self.model_results or [])

    def get_execution_summary(self):
        return build_ingestion_execution_summary(
            model_results=self.get_model_results_summary(),
            job_logs=self.get_job_logs(self.job).get("logs", []),
            applied_change_count=self.applied_change_count,
            failed_change_count=self.failed_change_count,
            created_change_count=self.created_change_count,
            updated_change_count=self.updated_change_count,
            deleted_change_count=self.deleted_change_count,
        )

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
        if not getattr(self, "num_created", 0):
            self.num_created = self.created_change_count
        if not getattr(self, "num_updated", 0):
            self.num_updated = self.updated_change_count
        if not getattr(self, "num_deleted", 0):
            self.num_deleted = self.deleted_change_count
        if not getattr(self, "staged_changes", 0):
            self.staged_changes = self.applied_change_count
        return {"job_results": job_results, "statistics": statistics}

    def record_change_totals(
        self,
        *,
        applied,
        failed,
        created=0,
        updated=0,
        deleted=0,
    ):
        self.applied_change_count = max(0, int(applied))
        self.failed_change_count = max(0, int(failed))
        self.created_change_count = max(0, int(created))
        self.updated_change_count = max(0, int(updated))
        self.deleted_change_count = max(0, int(deleted))
        ForwardIngestion.objects.filter(pk=self.pk).update(
            applied_change_count=self.applied_change_count,
            failed_change_count=self.failed_change_count,
            created_change_count=self.created_change_count,
            updated_change_count=self.updated_change_count,
            deleted_change_count=self.deleted_change_count,
        )

    def _cleanup_merged_branch(self):
        if not self.branch:
            return
        branching_branch = self.branch
        self.branch = None
        ForwardIngestion.objects.filter(pk=self.pk).update(branch=None)
        branching_branch.delete()

    def sync_merge(self, *, mark_baseline_ready=None, remove_branch=True):
        from .utilities.merge import merge_branch

        forwardsync = self.sync
        if forwardsync.status == ForwardSyncStatusChoices.MERGING:
            raise SyncError("Cannot initiate merge; merge already in progress.")

        pre_sync.send(sender=self.__class__, instance=self)

        branch_run_state = forwardsync.get_branch_run_state()
        is_pending_branch_run = branch_run_state.get(
            "pending_ingestion_id"
        ) == self.pk and branch_run_state.get("awaiting_merge")
        if mark_baseline_ready is None:
            mark_baseline_ready = not is_pending_branch_run or bool(
                branch_run_state.get("pending_is_final")
            )

        forwardsync.status = ForwardSyncStatusChoices.MERGING
        ForwardSync.objects.filter(pk=self.sync.pk).update(status=forwardsync.status)

        try:
            with suppress_branch_merge_side_effect_signals():
                merge_branch(ingestion=self, sync_logger=forwardsync.logger)
            if mark_baseline_ready:
                self.baseline_ready = True
                ForwardIngestion.objects.filter(pk=self.pk).update(baseline_ready=True)
            if is_pending_branch_run:
                if branch_run_state.get("pending_is_final"):
                    forwardsync.clear_branch_run_state()
                else:
                    branch_run_state["awaiting_merge"] = False
                    branch_run_state.pop("pending_ingestion_id", None)
                    branch_run_state.pop("pending_plan_index", None)
                    branch_run_state.pop("pending_is_final", None)
                    forwardsync.set_branch_run_state(branch_run_state)
            if remove_branch:
                self._cleanup_merged_branch()
            forwardsync.status = ForwardSyncStatusChoices.COMPLETED
        except Exception:
            forwardsync.status = ForwardSyncStatusChoices.FAILED
            ForwardSync.objects.filter(pk=self.sync.pk).update(
                status=forwardsync.status,
            )
            forwardsync.source.status = ForwardSourceStatusChoices.FAILED
            ForwardSource.objects.filter(pk=forwardsync.source.pk).update(
                status=forwardsync.source.status,
            )
            raise

        forwardsync.last_synced = timezone.now()
        ForwardSync.objects.filter(pk=self.sync.pk).update(
            status=forwardsync.status,
            last_synced=forwardsync.last_synced,
        )


class ForwardIngestionIssue(ForwardPluginModelDocsMixin, models.Model):
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
