import logging

from core.models import Job
from django.conf import settings
from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ValidationError
from django.core.validators import MaxValueValidator
from django.core.validators import MinValueValidator
from django.db import models
from django.db.models import Q
from django.urls import reverse
from django.utils import timezone
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
from .choices import ForwardApplyEngineChoices
from .choices import ForwardDriftPolicyBaselineChoices
from .choices import ForwardExecutionBackendChoices
from .choices import ForwardExecutionRunStatusChoices
from .choices import ForwardExecutionStepKindChoices
from .choices import ForwardExecutionStepStatusChoices
from .choices import ForwardIngestionPhaseChoices
from .choices import ForwardSourceDeploymentChoices
from .choices import ForwardSourceStatusChoices
from .choices import ForwardSyncStatusChoices
from .choices import ForwardValidationStatusChoices
from .exceptions import ForwardQueryError
from .exceptions import ForwardSyncError
from .utilities.branch_budget import DEFAULT_MAX_CHANGES_PER_BRANCH
from .utilities.forward_api import ForwardClient
from .utilities.forward_api import LATEST_PROCESSED_SNAPSHOT
from .utilities.ingestion_merge import (
    cleanup_merged_branch as cleanup_forward_merged_branch,
)
from .utilities.ingestion_merge import (
    enqueue_merge_job as enqueue_forward_merge_job,
)
from .utilities.ingestion_merge import (
    record_change_totals as record_forward_change_totals,
)
from .utilities.ingestion_presentation import (
    get_advisory_summary as build_ingestion_advisory_summary,
)
from .utilities.ingestion_presentation import (
    get_analysis_summary as build_ingestion_analysis_summary,
)
from .utilities.ingestion_presentation import (
    get_execution_summary as build_ingestion_execution_summary_from_presentation,
)
from .utilities.ingestion_presentation import (
    get_model_results_summary as build_ingestion_model_results_summary,
)
from .utilities.ingestion_presentation import (
    get_snapshot_metrics_summary as build_ingestion_snapshot_metrics_summary,
)
from .utilities.ingestion_presentation import (
    get_snapshot_summary as build_ingestion_snapshot_summary,
)
from .utilities.ingestion_presentation import (
    get_statistics as build_ingestion_statistics,
)
from .utilities.ingestion_presentation import (
    get_workload_summary as build_ingestion_workload_summary,
)
from .utilities.logging import SyncLogging
from .utilities.model_validation import clean_forward_nqe_map
from .utilities.model_validation import clean_forward_source
from .utilities.model_validation import clean_forward_sync
from .utilities.sync_facade import enabled_models as build_enabled_models
from .utilities.sync_facade import enqueue_sync_job as enqueue_forward_sync_job
from .utilities.sync_facade import (
    enqueue_validation_job as enqueue_forward_validation_job,
)
from .utilities.sync_facade import get_maps as build_sync_maps
from .utilities.sync_facade import get_query_parameters as build_sync_query_parameters
from .utilities.sync_facade import normalize_forward_sync
from .utilities.sync_facade import resolve_snapshot_id as resolve_forward_snapshot_id
from .utilities.sync_facade import uses_multi_branch as uses_forward_multi_branch
from .utilities.sync_state import clear_branch_run_state as clear_sync_branch_run_state
from .utilities.sync_state import get_advisory_summary as build_sync_advisory_summary
from .utilities.sync_state import get_analysis_summary as build_sync_analysis_summary
from .utilities.sync_state import (
    get_branch_run_display_state as get_sync_branch_run_display_state,
)
from .utilities.sync_state import get_branch_run_state as get_sync_branch_run_state
from .utilities.sync_state import (
    get_display_parameters as build_sync_display_parameters,
)
from .utilities.sync_state import (
    get_execution_summary as build_sync_execution_summary_from_state,
)
from .utilities.sync_state import get_job_logs as get_sync_job_logs
from .utilities.sync_state import (
    get_max_changes_per_branch as get_state_max_changes_per_branch,
)
from .utilities.sync_state import (
    get_model_change_density as get_sync_model_change_density,
)
from .utilities.sync_state import (
    get_model_change_density_profile as get_sync_model_change_density_profile,
)
from .utilities.sync_state import get_sync_activity as build_sync_activity
from .utilities.sync_state import get_workload_summary as build_sync_workload_summary
from .utilities.sync_state import has_pending_branch_run as has_pending_sync_branch_run
from .utilities.sync_state import (
    is_waiting_for_branch_merge as is_sync_waiting_for_branch_merge,
)
from .utilities.sync_state import ready_for_sync as is_sync_ready_for_sync
from .utilities.sync_state import (
    ready_to_continue_sync as is_sync_ready_to_continue_sync,
)
from .utilities.sync_state import set_branch_run_state as set_sync_branch_run_state
from .utilities.sync_state import (
    set_model_change_density as set_sync_model_change_density,
)
from .utilities.sync_state import (
    set_model_change_density_profile as set_sync_model_change_density_profile,
)
from .utilities.validation import force_allow_validation_run

logger = logging.getLogger("forward_netbox.models")

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
        clean_forward_source(self)

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
            "query_fetch_concurrency",
            "nqe_fetch_all_max_pages",
            "nqe_identical_full_page_streak_limit",
            "query_preflight_enabled",
            "query_diagnostics_enabled",
            "pushdown_fallback_warn_rate",
            "pushdown_runtime_fallback_warn_share",
            "pushdown_diff_warn_ratio",
            "device_tag_include",
            "device_tag_exclude",
            "device_tag_include_tags",
            "device_tag_exclude_tags",
            "device_tag_include_match",
            "device_tag_filter_mode",
            "device_tag_prune_out_of_scope",
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

    def get_tag_scope_preview(self):
        parameters = dict(self.parameters or {})
        network_id = str(parameters.get("network_id") or "").strip()
        include_tags = parameters.get("device_tag_include_tags") or []
        exclude_tags = parameters.get("device_tag_exclude_tags") or []
        include_match = str(parameters.get("device_tag_include_match") or "any")
        if include_match not in {"any", "all"}:
            include_match = "any"

        include_tags = [str(tag).strip() for tag in include_tags if str(tag).strip()]
        exclude_tags = [str(tag).strip() for tag in exclude_tags if str(tag).strip()]

        preview = {
            "enabled": bool(include_tags or exclude_tags),
            "network_id": network_id,
            "include_tags": include_tags,
            "exclude_tags": exclude_tags,
            "include_match": include_match,
            "total_devices": None,
            "matched_devices": None,
            "excluded_devices": None,
            "error": "",
        }
        if not preview["enabled"] or not network_id:
            return preview

        try:
            client = self.get_client()
            snapshot = client.get_latest_processed_snapshot(network_id)
            snapshot_id = str(snapshot.get("id") or "").strip()
            if not snapshot_id:
                preview["error"] = (
                    "No processed snapshot is available for the configured network."
                )
                return preview

            base_where = (
                "where device.snapshotInfo.result == DeviceSnapshotResult.completed\n"
                "where device.platform.vendor != Vendor.FORWARD_CUSTOM\n"
            )
            total_rows = client.run_nqe_query(
                query=(
                    "foreach device in network.devices\n"
                    f"{base_where}"
                    "select {name: device.name}"
                ),
                network_id=network_id,
                snapshot_id=snapshot_id,
                fetch_all=True,
            )
            total_devices = {
                str(row.get("name") or "").strip()
                for row in total_rows
                if str(row.get("name") or "").strip()
            }

            where_clauses = []
            include_exprs = [
                f'"{tag.replace("\"", "\\\"")}" in device.tagNames'
                for tag in include_tags
            ]
            if include_exprs:
                if include_match == "all":
                    where_clauses.extend([f"where {expr}" for expr in include_exprs])
                else:
                    where_clauses.append(f"where ({' || '.join(include_exprs)})")
            for tag in exclude_tags:
                escaped = tag.replace('"', '\\"')
                where_clauses.append(f'where !("{escaped}" in device.tagNames)')

            scoped_rows = client.run_nqe_query(
                query=(
                    "foreach device in network.devices\n"
                    f"{base_where}"
                    + ("\n".join(where_clauses) + "\n" if where_clauses else "")
                    + "select {name: device.name}"
                ),
                network_id=network_id,
                snapshot_id=snapshot_id,
                fetch_all=True,
            )
            matched_devices = {
                str(row.get("name") or "").strip()
                for row in scoped_rows
                if str(row.get("name") or "").strip()
            }

            preview["total_devices"] = len(total_devices)
            preview["matched_devices"] = len(matched_devices)
            preview["excluded_devices"] = max(
                len(total_devices) - len(matched_devices), 0
            )
            return preview
        except (ForwardSyncError, ForwardQueryError, Exception) as exc:
            preview["error"] = str(exc)
            return preview


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
    query_repository = models.CharField(max_length=10, blank=True, default="")
    query_path = models.CharField(max_length=500, blank=True, default="")
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
        if self.query_path:
            return "query_path"
        return "query_id" if self.query_id else "query"

    @property
    def execution_value(self):
        if self.query_path:
            repository = self.query_repository or "org"
            return f"{repository}:{self.query_path}"
        return self.query_id or self.name

    def get_absolute_url(self):
        return reverse("plugins:forward_netbox:forwardnqemap", args=[self.pk])

    def clean(self):
        super().clean()
        clean_forward_nqe_map(self)


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
        return is_sync_ready_for_sync(self)

    @property
    def ready_to_continue_sync(self):
        return is_sync_ready_to_continue_sync(self)

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
        if not specs or any(not getattr(spec, "diff_query_id", None) for spec in specs):
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
        clean_forward_sync(self)
        from .utilities.model_validation import validate_forward_sync_runtime

        validate_forward_sync_runtime(self)

    def _force_native_branching_execution(self):
        normalize_forward_sync(self)

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
        return resolve_forward_snapshot_id(self, client=client)

    def get_maps(self):
        return build_sync_maps(self)

    def get_query_parameters(self):
        return build_sync_query_parameters(self)

    def uses_multi_branch(self):
        return uses_forward_multi_branch(self)

    def get_branch_run_state(self):
        return get_sync_branch_run_state(self)

    def get_model_change_density(self):
        return get_sync_model_change_density(self)

    def get_model_change_density_profile(self):
        return get_sync_model_change_density_profile(self)

    @property
    def has_pending_branch_run(self):
        return has_pending_sync_branch_run(self)

    @property
    def has_pending_execution(self):
        return has_pending_sync_branch_run(self)

    @property
    def is_waiting_for_branch_merge(self):
        return is_sync_waiting_for_branch_merge(self)

    def set_branch_run_state(self, state):
        set_sync_branch_run_state(self, state)

    def clear_branch_run_state(self):
        clear_sync_branch_run_state(self)

    def set_model_change_density(self, model_change_density):
        set_sync_model_change_density(self, model_change_density)

    def set_model_change_density_profile(self, model_change_density_profile):
        set_sync_model_change_density_profile(self, model_change_density_profile)

    def get_max_changes_per_branch(self):
        return get_state_max_changes_per_branch(
            self,
            DEFAULT_MAX_CHANGES_PER_BRANCH,
        )

    def get_model_strings(self):
        return build_enabled_models(self)

    def get_display_parameters(self):
        return build_sync_display_parameters(
            self,
            max_changes_per_branch_default=DEFAULT_MAX_CHANGES_PER_BRANCH,
        )

    def get_execution_summary(self):
        return build_sync_execution_summary_from_state(self)

    def get_analysis_summary(self):
        return build_sync_analysis_summary(self)

    def get_workload_summary(self):
        return build_sync_workload_summary(self)

    def get_advisory_summary(self):
        return build_sync_advisory_summary(self)

    def get_sync_activity(self):
        return build_sync_activity(self)

    def is_model_enabled(self, model_string):
        if model_string not in forward_configured_models():
            return False
        parameters = self.parameters or {}
        return parameters.get(
            model_string,
            model_string not in FORWARD_OPTIONAL_MODELS,
        )

    def enabled_models(self):
        return build_enabled_models(self)

    def enqueue_sync_job(self, adhoc=False, user=None):
        return enqueue_forward_sync_job(self, adhoc=adhoc, user=user)

    def enqueue_validation_job(self, adhoc=False, user=None):
        return enqueue_forward_validation_job(self, adhoc=adhoc, user=user)

    def sync(self, job=None, *, max_changes_per_branch=None):
        from .utilities.sync_orchestration import run_forward_sync

        run_forward_sync(
            self,
            job=job,
            max_changes_per_branch=max_changes_per_branch,
        )


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
        return force_allow_validation_run(self, user=user, reason=reason)


class ForwardExecutionRun(ForwardPluginModelDocsMixin, models.Model):
    objects = RestrictedQuerySet.as_manager()

    sync = models.ForeignKey(
        ForwardSync,
        on_delete=models.CASCADE,
        related_name="execution_runs",
    )
    source = models.ForeignKey(
        ForwardSource,
        on_delete=models.SET_NULL,
        related_name="execution_runs",
        null=True,
        blank=True,
    )
    job = models.ForeignKey(Job, on_delete=models.SET_NULL, null=True, blank=True)
    validation_run = models.ForeignKey(
        ForwardValidationRun,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="execution_runs",
    )
    backend = models.CharField(
        max_length=30,
        choices=ForwardExecutionBackendChoices,
        default=ForwardExecutionBackendChoices.BRANCHING,
    )
    status = models.CharField(
        max_length=30,
        choices=ForwardExecutionRunStatusChoices,
        default=ForwardExecutionRunStatusChoices.QUEUED,
    )
    phase = models.CharField(max_length=50, blank=True, default="")
    phase_message = models.TextField(blank=True, default="")
    snapshot_selector = models.CharField(max_length=100, blank=True, default="")
    snapshot_id = models.CharField(max_length=100, blank=True, default="")
    max_changes_per_branch = models.PositiveIntegerField(
        default=DEFAULT_MAX_CHANGES_PER_BRANCH
    )
    auto_merge = models.BooleanField(default=False)
    total_steps = models.PositiveIntegerField(default=0)
    next_step_index = models.PositiveIntegerField(default=1)
    plan_preview = models.JSONField(blank=True, default=dict)
    model_change_density = models.JSONField(blank=True, default=dict)
    reconciliation_events = models.JSONField(blank=True, default=list)
    latest_heartbeat = models.DateTimeField(blank=True, null=True)
    last_error = models.TextField(blank=True, default="")
    baseline_ready = models.BooleanField(default=False)
    created = models.DateTimeField(default=timezone.now, editable=False)
    updated = models.DateTimeField(auto_now=True)
    completed = models.DateTimeField(blank=True, null=True)

    class Meta:
        ordering = ("-pk",)
        verbose_name = _("Forward Execution Run")
        verbose_name_plural = _("Forward Execution Runs")
        db_table = "forward_netbox_execution_run"

    def __str__(self):
        return f"{self.sync} execution {self.pk or ''}".strip()

    def get_absolute_url(self):
        return reverse("plugins:forward_netbox:forwardexecutionrun", args=[self.pk])

    def as_support_summary(self):
        return {
            "id": self.pk,
            "sync": self.sync_id,
            "source": self.source_id,
            "job": self.job_id,
            "validation_run": self.validation_run_id,
            "backend": self.backend,
            "status": self.status,
            "phase": self.phase,
            "phase_message": self.phase_message,
            "snapshot_selector": self.snapshot_selector,
            "snapshot_id": self.snapshot_id,
            "max_changes_per_branch": self.max_changes_per_branch,
            "auto_merge": self.auto_merge,
            "total_steps": self.total_steps,
            "next_step_index": self.next_step_index,
            "plan_preview": self.plan_preview,
            "model_change_density": self.model_change_density,
            "reconciliation_events": self.reconciliation_events,
            "latest_heartbeat": (
                self.latest_heartbeat.isoformat() if self.latest_heartbeat else None
            ),
            "last_error": self.last_error,
            "baseline_ready": self.baseline_ready,
            "created": self.created.isoformat() if self.created else None,
            "updated": self.updated.isoformat() if self.updated else None,
            "completed": self.completed.isoformat() if self.completed else None,
        }


class ForwardExecutionStep(ForwardPluginModelDocsMixin, models.Model):
    objects = RestrictedQuerySet.as_manager()

    run = models.ForeignKey(
        ForwardExecutionRun,
        on_delete=models.CASCADE,
        related_name="steps",
    )
    index = models.PositiveIntegerField()
    kind = models.CharField(
        max_length=30,
        choices=ForwardExecutionStepKindChoices,
        default=ForwardExecutionStepKindChoices.STAGE,
    )
    status = models.CharField(
        max_length=30,
        choices=ForwardExecutionStepStatusChoices,
        default=ForwardExecutionStepStatusChoices.PENDING,
    )
    model_string = models.CharField(max_length=100, blank=True, default="")
    label = models.CharField(max_length=250, blank=True, default="")
    query_name = models.CharField(max_length=200, blank=True, default="")
    execution_mode = models.CharField(max_length=30, blank=True, default="")
    execution_value = models.CharField(max_length=600, blank=True, default="")
    commit_id = models.CharField(max_length=100, blank=True, default="")
    sync_mode = models.CharField(max_length=20, blank=True, default="")
    operation = models.CharField(max_length=20, blank=True, default="mixed")
    baseline_snapshot_id = models.CharField(max_length=100, blank=True, default="")
    estimated_changes = models.PositiveIntegerField(default=0)
    actual_changes = models.PositiveIntegerField(default=0)
    fetched_row_count = models.PositiveIntegerField(default=0)
    query_runtime_ms = models.FloatField(blank=True, null=True)
    attempted_row_count = models.PositiveIntegerField(default=0)
    applied_row_count = models.PositiveIntegerField(default=0)
    skipped_row_count = models.PositiveIntegerField(default=0)
    failed_row_count = models.PositiveIntegerField(default=0)
    shard_keys = models.JSONField(blank=True, default=list)
    fetch_mode = models.CharField(max_length=30, blank=True, default="")
    fetch_key_family = models.CharField(max_length=50, blank=True, default="")
    fetch_parameters = models.JSONField(blank=True, default=dict)
    query_parameters = models.JSONField(blank=True, default=dict)
    fetch_column_filters = models.JSONField(blank=True, default=list)
    apply_engine = models.CharField(
        max_length=30,
        choices=ForwardApplyEngineChoices,
        blank=True,
        default=ForwardApplyEngineChoices.ADAPTER,
    )
    branch = models.ForeignKey(Branch, on_delete=models.SET_NULL, null=True, blank=True)
    branch_name = models.CharField(max_length=255, blank=True, default="")
    ingestion = models.ForeignKey(
        "ForwardIngestion",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="execution_steps",
    )
    job = models.ForeignKey(
        Job,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="forward_execution_steps",
    )
    merge_job = models.ForeignKey(
        Job,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="forward_execution_merge_steps",
    )
    retry_count = models.PositiveIntegerField(default=0)
    last_error = models.TextField(blank=True, default="")
    heartbeat = models.DateTimeField(blank=True, null=True)
    started = models.DateTimeField(blank=True, null=True)
    completed = models.DateTimeField(blank=True, null=True)
    created = models.DateTimeField(default=timezone.now, editable=False)
    updated = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ("run", "index", "kind")
        unique_together = (("run", "index", "kind"),)
        verbose_name = _("Forward Execution Step")
        verbose_name_plural = _("Forward Execution Steps")
        db_table = "forward_netbox_execution_step"

    def __str__(self):
        return f"{self.run} step {self.index} {self.kind}".strip()

    def get_absolute_url(self):
        return reverse("plugins:forward_netbox:forwardexecutionstep", args=[self.pk])

    def as_support_summary(self):
        return {
            "id": self.pk,
            "run": self.run_id,
            "index": self.index,
            "kind": self.kind,
            "status": self.status,
            "model": self.model_string,
            "label": self.label,
            "query_name": self.query_name,
            "execution_mode": self.execution_mode,
            "execution_value": self.execution_value,
            "commit_id": self.commit_id,
            "sync_mode": self.sync_mode,
            "operation": self.operation,
            "baseline_snapshot_id": self.baseline_snapshot_id,
            "estimated_changes": self.estimated_changes,
            "actual_changes": self.actual_changes,
            "fetched_row_count": self.fetched_row_count,
            "query_runtime_ms": self.query_runtime_ms,
            "attempted_row_count": self.attempted_row_count,
            "applied_row_count": self.applied_row_count,
            "skipped_row_count": self.skipped_row_count,
            "failed_row_count": self.failed_row_count,
            "shard_keys": self.shard_keys,
            "fetch_mode": self.fetch_mode,
            "fetch_key_family": self.fetch_key_family,
            "fetch_parameters": self.fetch_parameters,
            "query_parameters": self.query_parameters,
            "fetch_column_filters": self.fetch_column_filters,
            "apply_engine": self.apply_engine,
            "branch": self.branch_id,
            "branch_name": self.branch_name,
            "ingestion": self.ingestion_id,
            "job": self.job_id,
            "merge_job": self.merge_job_id,
            "retry_count": self.retry_count,
            "last_error": self.last_error,
            "heartbeat": self.heartbeat.isoformat() if self.heartbeat else None,
            "started": self.started.isoformat() if self.started else None,
            "completed": self.completed.isoformat() if self.completed else None,
            "created": self.created.isoformat() if self.created else None,
            "updated": self.updated.isoformat() if self.updated else None,
        }


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
    change_request_id = models.UUIDField(blank=True, null=True, db_index=True)
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
        return build_ingestion_snapshot_summary(self)

    def get_snapshot_metrics_summary(self):
        return build_ingestion_snapshot_metrics_summary(self)

    def get_model_results_summary(self):
        return build_ingestion_model_results_summary(self)

    def get_execution_summary(self):
        return build_ingestion_execution_summary_from_presentation(self)

    def get_analysis_summary(self):
        return build_ingestion_analysis_summary(self)

    def get_workload_summary(self):
        return build_ingestion_workload_summary(self)

    def get_advisory_summary(self):
        return build_ingestion_advisory_summary(self)

    @staticmethod
    def get_job_logs(job):
        return get_sync_job_logs(job)

    def enqueue_merge_job(self, user, remove_branch=False):
        return enqueue_forward_merge_job(
            self,
            user,
            remove_branch=remove_branch,
        )

    @property
    def can_queue_merge(self):
        if not self.branch or getattr(self.branch, "status", "") == "merged":
            return False
        from .utilities.execution_ledger import (
            ingestion_has_mergeable_execution_step,
            ingestion_has_requeueable_merge_timeout_step,
        )

        if ingestion_has_requeueable_merge_timeout_step(self):
            return True
        if self.merge_job and not self.merge_job.completed:
            return False
        if ingestion_has_mergeable_execution_step(self):
            return True
        state = get_sync_branch_run_display_state(self.sync)
        return bool(
            self.sync.status == ForwardSyncStatusChoices.READY_TO_MERGE
            or state.get("pending_ingestion_id") == self.pk
        )

    def get_statistics(self, stage="sync"):
        return build_ingestion_statistics(self, stage=stage)

    def record_change_totals(
        self,
        *,
        applied,
        failed,
        created=0,
        updated=0,
        deleted=0,
    ):
        record_forward_change_totals(
            self,
            applied=applied,
            failed=failed,
            created=created,
            updated=updated,
            deleted=deleted,
        )

    def _cleanup_merged_branch(self):
        cleanup_forward_merged_branch(self)

    def sync_merge(self, *, mark_baseline_ready=None, remove_branch=True):
        from .utilities.ingestion_merge import sync_merge_ingestion

        sync_merge_ingestion(
            self,
            mark_baseline_ready=mark_baseline_ready,
            remove_branch=remove_branch,
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
