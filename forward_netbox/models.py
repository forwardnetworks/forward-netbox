import json
import logging
from collections.abc import Iterable

from core.models import Job
from django.conf import settings
from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ValidationError
from django.core.validators import MaxValueValidator
from django.core.validators import MinValueValidator
from django.db import models
from django.db import transaction
from django.db.models import Q
from django.urls import reverse
from django.utils import timezone
from django.utils.translation import gettext as _
from netbox.models import ChangeLoggedModel
from netbox.models import PrimaryModel
from netbox.models.features import JobsMixin
from netbox.models.features import TagsMixin
from netbox_branching.models import Branch
from rq.timeouts import JobTimeoutException
from utilities.querysets import RestrictedQuerySet

from .choices import forward_configured_models
from .choices import FORWARD_OPTIONAL_MODELS
from .choices import FORWARD_SUPPORTED_MODELS
from .choices import ForwardCatchupStatusChoices
from .choices import ForwardDriftPolicyBaselineChoices
from .choices import ForwardIngestionPhaseChoices
from .choices import ForwardSourceDeploymentChoices
from .choices import ForwardSourceStatusChoices
from .choices import ForwardSyncStatusChoices
from .choices import ForwardValidationStatusChoices
from .exceptions import ForwardQueryError
from .exceptions import ForwardSyncError
from .utilities.branch_budget import DEFAULT_MAX_CHANGES_PER_STAGING_ITEM
from .utilities.diagnostics import safe_operation_failure
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
from .utilities.sync_state import get_advisory_summary as build_sync_advisory_summary
from .utilities.sync_state import get_analysis_summary as build_sync_analysis_summary
from .utilities.sync_state import (
    get_display_parameters as build_sync_display_parameters,
)
from .utilities.sync_state import (
    get_execution_summary as build_sync_execution_summary_from_state,
)
from .utilities.sync_state import get_job_logs as get_sync_job_logs
from .utilities.sync_state import (
    get_max_changes_per_staging_item as get_state_max_changes_per_staging_item,
)
from .utilities.sync_state import (
    get_model_change_density as get_sync_model_change_density,
)
from .utilities.sync_state import (
    get_model_change_density_profile as get_sync_model_change_density_profile,
)
from .utilities.sync_state import get_sync_activity as build_sync_activity
from .utilities.sync_state import get_workload_summary as build_sync_workload_summary
from .utilities.sync_state import ready_for_sync as is_sync_ready_for_sync
from .utilities.sync_state import (
    set_model_change_density as set_sync_model_change_density,
)
from .utilities.sync_state import (
    set_model_change_density_profile as set_sync_model_change_density_profile,
)
from .utilities.validation import DEFAULT_MAX_ROW_SHRINK_PERCENT
from .utilities.validation import force_allow_validation_run

logger = logging.getLogger("forward_netbox.models")


def _nqe_string_literal(value: str) -> str:
    return json.dumps(value)


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


class ForwardOwnershipReleaseQuerySet(RestrictedQuerySet):
    """Route bulk source/sync deletion through model ownership cleanup."""

    def delete(self):
        from .utilities.ownership import ownership_write_lock

        total = 0
        details = {}
        with ownership_write_lock():
            objects = list(self.select_for_update())
            for obj in objects:
                deleted, deleted_by_model = obj.delete()
                total += deleted
                for model_label, count in deleted_by_model.items():
                    details[model_label] = details.get(model_label, 0) + count
        return total, details


class ForwardSource(ForwardPluginModelDocsMixin, JobsMixin, PrimaryModel):
    objects = ForwardOwnershipReleaseQuerySet.as_manager()

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

    def save(self, *args, **kwargs):
        # Encrypt the Forward credential at rest so a DB dump/backup never holds a
        # usable password. Idempotent: an already-encrypted value is untouched, so
        # re-saving a source that reuses the stored ciphertext does not
        # double-encrypt. Decryption happens where the password is actually used
        # (the ForwardClient); every other reader only checks presence.
        from .utilities.crypto import encrypt_secret

        parameters = self.parameters or {}
        password = parameters.get("password")
        if password:
            encrypted = encrypt_secret(password)
            if encrypted != password:
                parameters = dict(parameters)
                parameters["password"] = encrypted
                self.parameters = parameters
        super().save(*args, **kwargs)

    def delete(self, *args, **kwargs):
        from .utilities.ownership import release_source_ownership

        with transaction.atomic():
            release_source_ownership(self)
            return super().delete(*args, **kwargs)

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
            "api_requests_per_minute",
            "nqe_async_poll_interval_seconds",
            "nqe_async_max_polls",
            "nqe_fetch_all_max_pages",
            "nqe_identical_full_page_streak_limit",
            "diff_fetch_timeout_seconds",
            "diff_timeout_circuit_breaker_threshold",
            "contributor_cache_max_rows",
            "contributor_cache_max_compressed_bytes",
            "query_diagnostics_enabled",
            "pushdown_fallback_warn_rate",
            "pushdown_runtime_fallback_warn_share",
            "pushdown_diff_warn_ratio",
            "device_tag_include_tags",
            "device_tag_exclude_tags",
            "device_tag_include_match",
            "device_tag_filter_mode",
            "device_tag_prune_out_of_scope",
            "apply_device_scope_tags",
            "sync_device_tags",
            "sync_endpoints",
            "sync_generic_endpoints",
            "scope_endpoints_by_include_tags",
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
                f"{_nqe_string_literal(tag)} in device.tagNames" for tag in include_tags
            ]
            if include_exprs:
                if include_match == "all":
                    where_clauses.extend([f"where {expr}" for expr in include_exprs])
                else:
                    where_clauses.append(f"where ({' || '.join(include_exprs)})")
            for tag in exclude_tags:
                where_clauses.append(
                    f"where !({_nqe_string_literal(tag)} in device.tagNames)"
                )

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
        except JobTimeoutException:
            raise
        except (ForwardSyncError, ForwardQueryError, Exception) as exc:
            preview["error"] = safe_operation_failure("Tag scope preview", exc)
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
    diff_commit_id = models.CharField(
        max_length=100,
        blank=True,
        default="",
        editable=False,
    )
    full_source_sha256 = models.CharField(
        max_length=64,
        blank=True,
        default="",
        editable=False,
    )
    diff_source_sha256 = models.CharField(
        max_length=64,
        blank=True,
        default="",
        editable=False,
    )
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
        if self.query_id:
            return "query_id"
        return "query_path" if self.query_path else "query"

    @property
    def execution_value(self):
        if self.query_id:
            return self.query_id
        if not self.query_path:
            return self.name
        repository = self.query_repository or "org"
        return f"{repository}:{self.query_path}"

    def get_absolute_url(self):
        return reverse("plugins:forward_netbox:forwardnqemap", args=[self.pk])

    def clean(self):
        super().clean()
        clean_forward_nqe_map(self)


class ForwardSync(ForwardPluginModelDocsMixin, JobsMixin, TagsMixin, ChangeLoggedModel):
    objects = ForwardOwnershipReleaseQuerySet.as_manager()

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

    def delete(self, *args, **kwargs):
        from .utilities.ownership import release_sync_ownership

        with transaction.atomic():
            release_sync_ownership(self)
            return super().delete(*args, **kwargs)

    @property
    def logger(self):
        return getattr(self, "_logger", SyncLogging())

    @logger.setter
    def logger(self, value):
        self._logger = value

    @property
    def ready_for_sync(self):
        return is_sync_ready_for_sync(self)

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
        model_contract=None,
        specs=None,
        current_snapshot_id,
        exclude_ingestion_id=None,
        client=None,
    ):
        if self.get_snapshot_id() != LATEST_PROCESSED_SNAPSHOT:
            return None
        if model_contract is None or not getattr(
            model_contract, "diff_eligible", False
        ):
            return None
        baseline = self.latest_baseline_ingestion(
            exclude_ingestion_id=exclude_ingestion_id
        )
        if baseline is None:
            return None
        if baseline.snapshot_id == current_snapshot_id:
            return None
        if client is not None and not self._baseline_snapshot_exists(
            baseline.snapshot_id,
            client=client,
        ):
            return None
        from .utilities.query_execution_contract import compatible_baseline_evidence

        if compatible_baseline_evidence(baseline, model_contract) is None:
            return None
        return baseline

    def _baseline_snapshot_exists(self, snapshot_id, *, client):
        network_id = self.get_network_id()
        if not network_id:
            return False
        try:
            snapshots = client.get_snapshots(network_id)
        except JobTimeoutException:
            raise
        except Exception:
            return False
        if not isinstance(snapshots, Iterable) or isinstance(
            snapshots, (str, bytes, dict)
        ):
            return False
        return any(
            str(snapshot.get("id") or "") == str(snapshot_id) for snapshot in snapshots
        )

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

    def get_model_change_density(self):
        return get_sync_model_change_density(self)

    def get_model_change_density_profile(self):
        return get_sync_model_change_density_profile(self)

    def set_model_change_density(self, model_change_density):
        set_sync_model_change_density(self, model_change_density)

    def set_model_change_density_profile(self, model_change_density_profile):
        set_sync_model_change_density_profile(self, model_change_density_profile)

    def get_max_changes_per_staging_item(self):
        return get_state_max_changes_per_staging_item(
            self,
            DEFAULT_MAX_CHANGES_PER_STAGING_ITEM,
        )

    def get_model_strings(self):
        return build_enabled_models(self)

    def get_display_parameters(self):
        return build_sync_display_parameters(
            self,
            max_changes_per_staging_item_default=DEFAULT_MAX_CHANGES_PER_STAGING_ITEM,
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

    def enqueue_sync_job(
        self,
        adhoc=False,
        user=None,
        current_job=None,
        force_unchanged=False,
    ):
        return enqueue_forward_sync_job(
            self,
            adhoc=adhoc,
            user=user,
            current_job=current_job,
            force_unchanged=force_unchanged,
        )

    def enqueue_validation_job(
        self, adhoc=False, user=None, schedule_at=None, interval=None
    ):
        return enqueue_forward_validation_job(
            self, adhoc=adhoc, user=user, schedule_at=schedule_at, interval=interval
        )

    def sync(
        self,
        job=None,
        *,
        max_changes_per_staging_item=None,
        force_unchanged=False,
    ):
        from .utilities.sync_orchestration import run_forward_sync

        return run_forward_sync(
            self,
            job=job,
            max_changes_per_staging_item=max_changes_per_staging_item,
            force_unchanged=force_unchanged,
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
    # The row-count floor. Unlike the two delete limits above it is not
    # optional-and-unset: a guard nobody enables does not protect anybody, and
    # this one stands in for a source-hash check that used to run on every sync.
    # It is therefore a default-on boolean with a concrete default threshold,
    # matching `block_on_query_errors` rather than `max_deleted_objects`.
    block_on_row_shrink = models.BooleanField(
        default=True,
        help_text=_(
            "Refuse a sync when a model returns far fewer rows than it did in "
            "the last successful ingestion. The missing rows would be "
            "reconciled as deletions."
        ),
    )
    max_row_shrink_percent = models.PositiveIntegerField(
        default=DEFAULT_MAX_ROW_SHRINK_PERCENT,
        validators=(MinValueValidator(0), MaxValueValidator(100)),
        help_text=_(
            "How far a model's row count may fall below the last successful "
            "ingestion before the sync is refused."
        ),
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
    merge_applied_at = models.DateTimeField(blank=True, null=True, db_index=True)
    merge_finalized_at = models.DateTimeField(blank=True, null=True, db_index=True)
    catchup_status = models.CharField(
        max_length=20,
        choices=ForwardCatchupStatusChoices,
        default=ForwardCatchupStatusChoices.NOT_APPLICABLE,
        db_index=True,
    )
    catchup_target_snapshot_id = models.CharField(
        max_length=100,
        blank=True,
        default="",
    )
    catchup_reason = models.CharField(max_length=100, blank=True, default="")
    catchup_error_type = models.CharField(max_length=255, blank=True, default="")
    catchup_checked_at = models.DateTimeField(blank=True, null=True)
    applied_change_count = models.PositiveIntegerField(default=0)
    failed_change_count = models.PositiveIntegerField(default=0)
    # Rows the destination refused on one of its own validation rules. They are
    # exceptions, but no retry can satisfy them, so they are held apart from
    # `failed_change_count`: everything that reads readiness treats a failed row
    # as something a rerun could clear, and these rows never will be.
    skipped_change_count = models.PositiveIntegerField(default=0)
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

    def enqueue_merge_job(
        self,
        user,
        remove_branch=False,
        *,
        recovery_sync_job_pks=None,
        accept_reported_failures=False,
    ):
        return enqueue_forward_merge_job(
            self,
            user,
            remove_branch=remove_branch,
            recovery_sync_job_pks=recovery_sync_job_pks,
            accept_reported_failures=accept_reported_failures,
        )

    @property
    def can_queue_merge(self):
        if not self.branch or getattr(self.branch, "status", "") == "merged":
            return False
        if self.merge_job and not self.merge_job.completed:
            return False
        return self.sync.status == ForwardSyncStatusChoices.READY_TO_MERGE

    @property
    def can_accept_merge_failures(self):
        """Whether this ingestion is stalled behind failures an operator can accept.

        Offered only when a merge has actually run and left failures behind: any
        failed row returns the branch to READY without attesting, so the
        baseline never promotes and every retry hits the same rows. Never
        offered for a clean ingestion, one already promoted, or one with no
        branch left to merge.
        """
        if not self.can_queue_merge:
            return False
        if self.baseline_ready:
            return False
        return int(self.failed_change_count or 0) > 0

    def get_statistics(self, stage="sync"):
        return build_ingestion_statistics(self, stage=stage)

    def record_change_totals(
        self,
        *,
        applied,
        failed,
        skipped=0,
        created=0,
        updated=0,
        deleted=0,
    ):
        record_forward_change_totals(
            self,
            applied=applied,
            failed=failed,
            skipped=skipped,
            created=created,
            updated=updated,
            deleted=deleted,
        )

    def _cleanup_merged_branch(self):
        cleanup_forward_merged_branch(self)

    def sync_merge(
        self,
        *,
        mark_baseline_ready=None,
        remove_branch=True,
        claimed_job=None,
        merge_attempt=None,
        accept_reported_failures=False,
    ):
        from .utilities.ingestion_merge import sync_merge_ingestion

        sync_merge_ingestion(
            self,
            mark_baseline_ready=mark_baseline_ready,
            remove_branch=remove_branch,
            claimed_job=claimed_job,
            merge_attempt=merge_attempt,
            accept_reported_failures=accept_reported_failures,
        )


class ForwardMergeAttempt(ForwardPluginModelDocsMixin, models.Model):
    """Durable progress and failure evidence for one complete merge replay."""

    class Status(models.TextChoices):
        RUNNING = "running", _("Running")
        APPLIED = "applied", _("Branch changes applied")
        COMPLETED = "completed", _("Completed")
        FAILED = "failed", _("Failed")
        INTERRUPTED = "interrupted", _("Interrupted")

    ingestion = models.ForeignKey(
        ForwardIngestion,
        on_delete=models.CASCADE,
        related_name="merge_attempts",
    )
    job = models.ForeignKey(
        Job,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="+",
    )
    attempt_number = models.PositiveIntegerField()
    status = models.CharField(
        max_length=20,
        choices=Status,
        default=Status.RUNNING,
        db_index=True,
    )
    phase = models.CharField(max_length=32, default="preparing")
    total_changes = models.PositiveBigIntegerField(default=0)
    merged_changes = models.PositiveBigIntegerField(default=0)
    failed_changes = models.PositiveBigIntegerField(default=0)
    current_model = models.CharField(max_length=100, blank=True, default="")
    model_progress = models.JSONField(blank=True, default=dict)
    checkpoint_sequence = models.PositiveBigIntegerField(default=0)
    started_at = models.DateTimeField(default=timezone.now, editable=False)
    heartbeat_at = models.DateTimeField(default=timezone.now, db_index=True)
    finished_at = models.DateTimeField(blank=True, null=True)
    failure_kind = models.CharField(max_length=32, blank=True, default="")
    exception_type = models.CharField(max_length=255, blank=True, default="")
    failure_summary = models.TextField(blank=True, default="")
    traceback = models.TextField(blank=True, default="")
    process_wait_status = models.IntegerField(blank=True, null=True)
    process_exit_code = models.IntegerField(blank=True, null=True)
    process_signal = models.PositiveSmallIntegerField(blank=True, null=True)
    process_signal_name = models.CharField(max_length=32, blank=True, default="")

    class Meta:
        ordering = ("ingestion_id", "-attempt_number")
        verbose_name = _("Forward Merge Attempt")
        verbose_name_plural = _("Forward Merge Attempts")
        db_table = "forward_netbox_merge_attempt"
        constraints = [
            models.UniqueConstraint(
                fields=["ingestion", "attempt_number"],
                name="forward_merge_attempt_ingestion_number",
            ),
        ]

    def __str__(self):
        return f"{self.ingestion}: merge attempt {self.attempt_number} ({self.status})"

    @property
    def processed_changes(self):
        return int(self.merged_changes or 0) + int(self.failed_changes or 0)


class ForwardWorkloadState(ForwardPluginModelDocsMixin, models.Model):
    """Compressed target rows for one sync/model at an exact ingestion."""

    sync = models.ForeignKey(
        ForwardSync,
        on_delete=models.CASCADE,
        related_name="workload_states",
    )
    ingestion = models.ForeignKey(
        ForwardIngestion,
        on_delete=models.CASCADE,
        related_name="workload_states",
    )
    model_string = models.CharField(max_length=100)
    parameter_hash = models.CharField(max_length=64)
    identity_contract_hash = models.CharField(max_length=64)
    payload = models.BinaryField()
    payload_checksum = models.CharField(max_length=64)
    row_count = models.PositiveIntegerField(default=0)
    snapshot_id = models.CharField(max_length=100, blank=True, default="")
    is_current = models.BooleanField(default=False, db_index=True)
    created = models.DateTimeField(default=timezone.now, editable=False)

    class Meta:
        ordering = ("sync_id", "model_string", "-ingestion_id")
        verbose_name = _("Forward Workload State")
        verbose_name_plural = _("Forward Workload States")
        db_table = "forward_netbox_workload_state"
        constraints = [
            models.UniqueConstraint(
                fields=["ingestion", "model_string"],
                name="forward_workload_state_ingestion_model",
            ),
            models.UniqueConstraint(
                fields=["sync", "model_string"],
                condition=Q(is_current=True),
                name="forward_workload_state_current_model",
            ),
        ]

    def __str__(self):
        status = "current" if self.is_current else "pending"
        return f"{self.sync}: {self.model_string} ({status})"


class ForwardContributorBaseline(ForwardPluginModelDocsMixin, models.Model):
    """Merge-gated generation containing complete contributor relations."""

    class Status(models.TextChoices):
        PENDING = "pending", _("Pending")
        CURRENT = "current", _("Current")
        SUPERSEDED = "superseded", _("Superseded")
        INVALID = "invalid", _("Invalid")

    sync = models.ForeignKey(
        ForwardSync,
        on_delete=models.CASCADE,
        related_name="contributor_baselines",
    )
    ingestion = models.OneToOneField(
        ForwardIngestion,
        # CASCADE, not PROTECT: promotion leaves the previous generation
        # SUPERSEDED with its relations deleted and its payload emptied, and
        # nothing ever removed that husk, so every ingestion that promoted
        # became permanently undeletable. PROTECT cannot express "keep the live
        # one, collect the spent one"; the live one is kept by a `pre_delete`
        # receiver instead - see `refuse_ingestion_delete_with_live_baseline`.
        on_delete=models.CASCADE,
        related_name="contributor_baseline",
    )
    parent_baseline = models.ForeignKey(
        "self",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="child_baselines",
    )
    snapshot_id = models.CharField(max_length=100)
    network_fingerprint = models.CharField(max_length=64)
    map_set_fingerprint = models.CharField(max_length=64)
    scope_config_fingerprint = models.CharField(max_length=64)
    scope_membership_fingerprint = models.CharField(max_length=64)
    scope_payload_version = models.PositiveSmallIntegerField(default=1)
    scope_payload = models.BinaryField(blank=True, default=b"")
    scope_payload_checksum = models.CharField(max_length=64)
    status = models.CharField(
        max_length=20,
        choices=Status,
        default=Status.PENDING,
        db_index=True,
    )
    is_current = models.BooleanField(default=False, db_index=True)
    created = models.DateTimeField(default=timezone.now, editable=False)
    promoted_at = models.DateTimeField(blank=True, null=True)

    class Meta:
        ordering = ("sync_id", "-ingestion_id")
        verbose_name = _("Forward Contributor Baseline")
        verbose_name_plural = _("Forward Contributor Baselines")
        db_table = "forward_netbox_contributor_baseline"
        constraints = [
            models.UniqueConstraint(
                fields=["sync"],
                condition=Q(is_current=True),
                name="forward_contributor_baseline_current_sync",
            ),
        ]

    def __str__(self):
        return f"{self.sync}: contributor baseline {self.snapshot_id} ({self.status})"


class ForwardContributorRelation(ForwardPluginModelDocsMixin, models.Model):
    """Metadata for one chunked contributor relation in a baseline."""

    baseline = models.ForeignKey(
        ForwardContributorBaseline,
        on_delete=models.CASCADE,
        related_name="relations",
    )
    query_map = models.ForeignKey(
        ForwardNQEMap,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="+",
    )
    model_string = models.CharField(max_length=100)
    contract_key = models.CharField(max_length=255)
    query_path = models.CharField(max_length=500)
    query_id = models.CharField(max_length=100)
    full_commit_id = models.CharField(max_length=100)
    full_source_sha256 = models.CharField(max_length=64)
    diff_query_id = models.CharField(max_length=100)
    diff_commit_id = models.CharField(max_length=100)
    diff_source_sha256 = models.CharField(max_length=64)
    contract_fingerprint = models.CharField(max_length=64)
    reducer_id = models.CharField(max_length=100)
    reducer_version = models.PositiveIntegerField()
    normalization_version = models.PositiveIntegerField()
    identity_version = models.PositiveIntegerField()
    provenance_identity_version = models.PositiveIntegerField(default=1)
    payload_version = models.PositiveIntegerField(default=1)
    row_count = models.PositiveIntegerField(default=0)
    uncompressed_bytes = models.PositiveBigIntegerField(default=0)
    compressed_bytes = models.PositiveBigIntegerField(default=0)
    relation_checksum = models.CharField(max_length=64)

    class Meta:
        ordering = ("baseline_id", "contract_key")
        verbose_name = _("Forward Contributor Relation")
        verbose_name_plural = _("Forward Contributor Relations")
        db_table = "forward_netbox_contributor_relation"
        constraints = [
            models.UniqueConstraint(
                fields=["baseline", "contract_key"],
                name="forward_contributor_relation_baseline_contract",
            ),
        ]

    def __str__(self):
        return f"{self.baseline}: {self.contract_key}"


class ForwardContributorRelationChunk(
    ForwardPluginModelDocsMixin,
    models.Model,
):
    """One independently checksummed and compressed contributor payload frame."""

    relation = models.ForeignKey(
        ForwardContributorRelation,
        on_delete=models.CASCADE,
        related_name="chunks",
    )
    sequence = models.PositiveIntegerField()
    payload = models.BinaryField()
    payload_checksum = models.CharField(max_length=64)
    compressed_bytes = models.PositiveIntegerField()

    class Meta:
        ordering = ("relation_id", "sequence")
        verbose_name = _("Forward Contributor Relation Chunk")
        verbose_name_plural = _("Forward Contributor Relation Chunks")
        db_table = "forward_netbox_contributor_relation_chunk"
        constraints = [
            models.UniqueConstraint(
                fields=["relation", "sequence"],
                name="forward_contributor_chunk_relation_sequence",
            ),
        ]

    def __str__(self):
        return f"{self.relation}: chunk {self.sequence}"


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

    def get_absolute_url(self):
        from django.urls import reverse

        return reverse(
            "plugins:forward_netbox:forwardingestionissue", kwargs={"pk": self.pk}
        )


class ForwardManagedDeviceTag(ForwardPluginModelDocsMixin, models.Model):
    """Declares a NetBox tag whose assignments are materialized from claims."""

    class ClaimType(models.TextChoices):
        SCOPE = "scope", _("Managed scope")
        BACKFILLED = "backfilled", _("Backfilled status")
        OUT_OF_SCOPE = "out_of_scope", _("Out-of-scope status")

    tag = models.ForeignKey(
        "extras.Tag",
        on_delete=models.PROTECT,
        related_name="+",
    )
    claim_type = models.CharField(max_length=32, choices=ClaimType.choices)

    class Meta:
        ordering = ("tag__name", "claim_type")
        verbose_name = _("Forward Managed Device Tag")
        verbose_name_plural = _("Forward Managed Device Tags")
        db_table = "forward_netbox_managed_device_tag"
        constraints = [
            models.UniqueConstraint(
                fields=["tag"],
                name="forward_managed_device_tag_identity",
            )
        ]

    def __str__(self):
        return f"{self.tag} ({self.claim_type})"


class ForwardPreservedDeviceTagAssignment(ForwardPluginModelDocsMixin, models.Model):
    """Pre-adoption assignment that plugin reconciliation must not remove."""

    device = models.ForeignKey(
        "dcim.Device",
        on_delete=models.CASCADE,
        related_name="+",
    )
    tag = models.ForeignKey(
        "extras.Tag",
        on_delete=models.CASCADE,
        related_name="+",
    )
    recorded_at = models.DateTimeField(default=timezone.now)

    class Meta:
        ordering = ("tag__name", "device__name")
        verbose_name = _("Forward Preserved Device Tag Assignment")
        verbose_name_plural = _("Forward Preserved Device Tag Assignments")
        db_table = "forward_netbox_preserved_device_tag_assignment"
        constraints = [
            models.UniqueConstraint(
                fields=["device", "tag"],
                name="forward_preserved_device_tag_assignment_identity",
            )
        ]

    def __str__(self):
        return f"{self.device} -> {self.tag}"


class ForwardIngestionProvenanceMixin(models.Model):
    """Ownership evidence stamped with the ingestion that last asserted it.

    `ForwardOwnershipReconciliation` overrides ``ingestion`` to cascade; it is a
    child record of the ingestion rather than evidence held against it. See that
    model for why.

    ``ingestion`` is a PROVENANCE STAMP, not a dependency, and that distinction
    is why it is nullable. The substance of the evidence - which sync owns which
    device, under which source key, and the ``snapshot_id`` it was last seen in
    - is carried on the row itself and stays true whatever happens to the run
    that recorded it.

    It was PROTECT, which made a stamp behave like a dependency: a device that
    left Forward's scope stopped being re-pointed, froze on the last ingestion
    that saw it, and pinned that ingestion permanently. A customer accumulated
    one undeletable ingestion for every scope change. The rows doing the pinning
    were not stale - the devices still existed and were still owned - so every
    attempt to fix this by pruning the evidence risked releasing a live device,
    which is why three successive models of the problem were wrong.

    SET_NULL keeps the ownership and drops only the pointer to a run that no
    longer exists. A null stamp reads as "asserted before the oldest retained
    ingestion", which the generation comparisons already treat as stale.
    """

    ingestion = models.ForeignKey(
        ForwardIngestion,
        db_column="generation",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="+",
    )

    class Meta:
        abstract = True

    @property
    def generation(self):
        return self.ingestion_id

    @generation.setter
    def generation(self, value):
        self.ingestion_id = value

    def _validate_ingestion_sync(self):
        if not self.ingestion_id or not self.sync_id:
            return
        ingestion_sync_id = (
            ForwardIngestion.objects.filter(pk=self.ingestion_id)
            .values_list("sync_id", flat=True)
            .first()
        )
        if ingestion_sync_id != self.sync_id:
            raise ValidationError(
                {
                    "ingestion": "Ownership evidence must reference an ingestion from the same sync."
                }
            )

    def clean(self):
        super().clean()
        self._validate_ingestion_sync()

    def save(self, *args, **kwargs):
        self._validate_ingestion_sync()
        return super().save(*args, **kwargs)


class ForwardDeviceIdentity(
    ForwardIngestionProvenanceMixin,
    ForwardPluginModelDocsMixin,
):
    """Stable mapping from one Forward-network device identity to a NetBox row."""

    sync = models.ForeignKey(
        ForwardSync,
        on_delete=models.PROTECT,
        related_name="device_identities",
    )
    source_device_key = models.CharField(max_length=255)
    device = models.ForeignKey(
        "dcim.Device",
        on_delete=models.PROTECT,
        related_name="+",
    )
    snapshot_id = models.CharField(max_length=100, blank=True, default="")

    class Meta:
        ordering = ("sync__name", "source_device_key")
        verbose_name = _("Forward Device Identity")
        verbose_name_plural = _("Forward Device Identities")
        db_table = "forward_netbox_device_identity"
        constraints = [
            models.UniqueConstraint(
                fields=["sync", "source_device_key"],
                name="forward_device_identity_source_key",
            ),
            models.UniqueConstraint(
                fields=["sync", "device"],
                name="forward_device_identity_device",
            ),
        ]

    def __str__(self):
        return f"{self.sync}: {self.source_device_key} -> {self.device}"


class ForwardDeviceTagClaim(
    ForwardIngestionProvenanceMixin,
    ForwardPluginModelDocsMixin,
):
    """Latest-ingestion assertion for one managed NetBox tag assignment."""

    class ClaimType(models.TextChoices):
        SCOPE = "scope", _("Managed scope")
        BACKFILLED = "backfilled", _("Backfilled status")
        OUT_OF_SCOPE = "out_of_scope", _("Out-of-scope status")

    sync = models.ForeignKey(
        ForwardSync,
        on_delete=models.PROTECT,
        related_name="device_tag_claims",
    )
    device = models.ForeignKey(
        "dcim.Device",
        on_delete=models.PROTECT,
        related_name="+",
    )
    tag = models.ForeignKey(
        "extras.Tag",
        on_delete=models.PROTECT,
        related_name="+",
    )
    claim_type = models.CharField(max_length=32, choices=ClaimType.choices)
    snapshot_id = models.CharField(max_length=100, blank=True, default="")

    class Meta:
        ordering = ("sync__name", "device__name", "tag__name", "claim_type")
        verbose_name = _("Forward Device Tag Claim")
        verbose_name_plural = _("Forward Device Tag Claims")
        db_table = "forward_netbox_device_tag_claim"
        constraints = [
            models.UniqueConstraint(
                fields=["sync", "device", "tag", "claim_type"],
                name="forward_device_tag_claim_identity",
            )
        ]

    def __str__(self):
        return f"{self.sync}: {self.device} -> {self.tag} ({self.claim_type})"


class ForwardManagedVirtualContext(ForwardPluginModelDocsMixin, models.Model):
    """Marks a VirtualDeviceContext created and lifecycle-owned by the plugin."""

    virtual_context = models.OneToOneField(
        "dcim.VirtualDeviceContext",
        on_delete=models.PROTECT,
        related_name="+",
    )

    class Meta:
        ordering = ("virtual_context__device__name", "virtual_context__name")
        verbose_name = _("Forward Managed Virtual Context")
        verbose_name_plural = _("Forward Managed Virtual Contexts")
        db_table = "forward_netbox_managed_virtual_context"

    def __str__(self):
        return str(self.virtual_context)


class ForwardVirtualParentClaim(
    ForwardIngestionProvenanceMixin,
    ForwardPluginModelDocsMixin,
):
    """Latest-ingestion assertion for a virtual device and physical parent."""

    sync = models.ForeignKey(
        ForwardSync,
        on_delete=models.PROTECT,
        related_name="virtual_parent_claims",
    )
    device = models.ForeignKey(
        "dcim.Device",
        on_delete=models.PROTECT,
        related_name="+",
    )
    parent_device = models.ForeignKey(
        "dcim.Device",
        on_delete=models.PROTECT,
        related_name="+",
    )
    virtual_context = models.ForeignKey(
        "dcim.VirtualDeviceContext",
        blank=True,
        null=True,
        on_delete=models.PROTECT,
        related_name="+",
    )
    snapshot_id = models.CharField(max_length=100, blank=True, default="")

    class Meta:
        ordering = ("sync__name", "device__name")
        verbose_name = _("Forward Virtual Parent Claim")
        verbose_name_plural = _("Forward Virtual Parent Claims")
        db_table = "forward_netbox_virtual_parent_claim"
        constraints = [
            models.UniqueConstraint(
                fields=["sync", "device"],
                name="forward_virtual_parent_claim_identity",
            )
        ]

    def __str__(self):
        return f"{self.sync}: {self.device} -> {self.parent_device}"


class ForwardOwnershipReconciliation(
    ForwardIngestionProvenanceMixin,
    ForwardPluginModelDocsMixin,
):
    """Latest baseline generation reconciled for one ownership domain.

    Unlike its three sibling provenance models, this one cascades with the
    ingestion it names rather than protecting it. Those three describe a live
    NetBox object - a device identity, a tag assignment, a virtual parent - so
    they outlive any single ingestion and must keep their provenance intact.
    A reconciliation row describes no NetBox object at all: it records only
    that this sync finished this domain at this exact ingestion. Once that
    ingestion is gone the row is a statement about nothing, and it can never be
    repaired, because the only thing that rewrites it is the next reconciliation
    of the same domain.

    Holding it as PROTECT made a sequence of ingestions permanently undeletable
    with no supported way out: no UI, API, or command deletes these rows, so the
    refusal named records an operator could not reach. Two conditions produce
    that. The row re-points only when its domain reconciles again, so a domain
    that stops running - scope tags switched off, virtual parents disabled -
    freezes its row on an old ingestion and pins it forever. And because
    `required_ownership_domains` treats the row's existence as proof the domain
    is required, that same frozen row also keeps ownership permanently
    incomplete. Cascading clears both together.

    What must not be lost is the *current* evidence: the ingestion whose rows
    prove ownership complete right now. The database cannot express "protect
    only the newest", so that one is refused explicitly at the delete path
    instead - see `_ingestion_delete_refusal_detail`.
    """

    ingestion = models.ForeignKey(
        ForwardIngestion,
        db_column="generation",
        on_delete=models.CASCADE,
        related_name="+",
    )

    class Domain(models.TextChoices):
        SCOPE_TAGS = "scope_tags", _("Managed scope tags")
        STATUS_TAGS = "status_tags", _("Scope status tags")
        VIRTUAL_PARENTS = "virtual_parents", _("Virtual parents")

    class Status(models.TextChoices):
        PENDING = "pending", _("Pending")
        COMPLETED = "completed", _("Completed")
        FAILED = "failed", _("Failed")

    sync = models.ForeignKey(
        ForwardSync,
        on_delete=models.PROTECT,
        related_name="ownership_reconciliations",
    )
    domain = models.CharField(max_length=32, choices=Domain.choices)
    snapshot_id = models.CharField(max_length=100, blank=True, default="")
    status = models.CharField(
        max_length=16,
        choices=Status.choices,
        default=Status.PENDING,
    )
    error_type = models.CharField(max_length=100, blank=True, default="")
    started_at = models.DateTimeField(default=timezone.now)
    completed_at = models.DateTimeField(blank=True, null=True)

    class Meta:
        ordering = ("sync__name", "domain")
        verbose_name = _("Forward Ownership Reconciliation")
        verbose_name_plural = _("Forward Ownership Reconciliations")
        db_table = "forward_netbox_ownership_reconciliation"
        constraints = [
            models.UniqueConstraint(
                fields=["sync", "domain"],
                name="forward_ownership_reconciliation_identity",
            )
        ]

    def __str__(self):
        return f"{self.sync}: {self.domain} @ {self.generation}"


class ForwardDeviceAnalysis(ForwardPluginModelDocsMixin, ChangeLoggedModel):
    """Read-only per-device operational analysis surfaced from Forward.

    Populated by a snapshot-guarded NetBox JobRunner and rendered on the device
    detail panel and a fleet-wide list view without a live Forward call. This is
    an auxiliary plugin read model, not authoritative inventory and not a
    Branching-managed sync model.
    """

    objects = RestrictedQuerySet.as_manager()

    sync = models.ForeignKey(
        ForwardSync,
        on_delete=models.CASCADE,
        related_name="device_analyses",
    )
    device = models.ForeignKey(
        "dcim.Device",
        on_delete=models.CASCADE,
        related_name="+",
    )
    reachable = models.BooleanField(default=False)
    # Specific Forward collection result token (e.g. "completed",
    # "AUTHENTICATION_FAILED", "CONNECTION_TIMEOUT") so the panel can show *why*
    # an unreachable device failed, not just a Yes/No.
    collection_result = models.CharField(max_length=64, blank=True, default="")
    blast_radius = models.PositiveIntegerField(default=0)
    cve_count = models.PositiveIntegerField(default=0)
    # The actual confirmed-vulnerable CVE IDs behind cve_count, so the device
    # panel can list them (not just the exposure number).
    cve_ids = models.JSONField(default=list, blank=True)
    up_interfaces = models.PositiveIntegerField(default=0)
    detail = models.CharField(max_length=255, blank=True, default="")
    snapshot_id = models.CharField(max_length=100, blank=True, default="")

    class Meta:
        ordering = ("device__name",)
        verbose_name = _("Forward Device Analysis")
        verbose_name_plural = _("Forward Device Analyses")
        db_table = "forward_netbox_device_analysis"
        constraints = [
            models.UniqueConstraint(
                fields=["sync", "device"],
                name="forward_device_analysis_sync_device",
            )
        ]

    def __str__(self):
        return f"{self.device} analysis"

    def get_absolute_url(self):
        return reverse("plugins:forward_netbox:forwarddeviceanalysis", args=[self.pk])

    @property
    def forward_ui_url(self):
        """Pivot link into the Forward app (path search / blast radius live there).

        Best-effort: the Forward app base URL. Device-specific frontend routes are
        not a stable public contract, so we land the operator in Forward rather
        than risk a 404 deep link.
        """
        source = getattr(self.sync, "source", None)
        url = (getattr(source, "url", "") or "").rstrip("/")
        return url or None
