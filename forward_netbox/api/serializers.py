from dcim.api.serializers import DeviceSerializer
from django.contrib.contenttypes.models import ContentType
from netbox.api.fields import ChoiceField
from netbox.api.fields import ContentTypeField
from netbox.api.serializers import NestedGroupModelSerializer
from netbox_branching.api.serializers import BranchSerializer
from rest_framework import serializers

from forward_netbox.choices import ForwardDriftPolicyBaselineChoices
from forward_netbox.choices import ForwardIngestionPhaseChoices
from forward_netbox.choices import ForwardSourceDeploymentChoices
from forward_netbox.choices import ForwardSourceStatusChoices
from forward_netbox.choices import ForwardSyncStatusChoices
from forward_netbox.choices import ForwardValidationStatusChoices
from forward_netbox.models import FORWARD_SUPPORTED_SYNC_MODELS
from forward_netbox.models import ForwardDeviceAnalysis
from forward_netbox.models import ForwardDriftPolicy
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardIngestionIssue
from forward_netbox.models import ForwardNQEMap
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.models import ForwardValidationRun
from forward_netbox.utilities.json_safe import json_safe_value


class EmptySerializer(serializers.Serializer):
    pass


class JobScheduleRequestSerializer(serializers.Serializer):
    """Optional standing-schedule parameters for job-enqueue actions. Both
    fields absent = immediate one-shot run."""

    schedule_at = serializers.DateTimeField(required=False, allow_null=True)
    interval = serializers.IntegerField(
        required=False,
        allow_null=True,
        min_value=0,
        help_text=(
            "Recurrence interval in minutes; requires the RQ scheduler. "
            "0 cancels the standing schedule."
        ),
    )

    def __init__(self, *args, min_interval=1, **kwargs):
        super().__init__(*args, **kwargs)
        self.min_interval = min_interval

    def validate_schedule_at(self, value):
        from django.utils import timezone

        if value and value < timezone.now():
            raise serializers.ValidationError("schedule_at must be in the future.")
        return value

    def validate(self, data):
        # schedule_at without interval would occupy the same enqueue_once
        # dedup slot as the standing schedule and silently replace it, so
        # one-shot delayed runs are rejected outright: an empty body runs
        # immediately, interval creates the standing schedule.
        interval = data.get("interval")
        if interval == 0 and data.get("schedule_at"):
            raise serializers.ValidationError(
                {"interval": "interval 0 cancels the schedule; omit schedule_at."}
            )
        if data.get("schedule_at") and interval is None:
            raise serializers.ValidationError(
                {"interval": "schedule_at requires interval."}
            )
        if interval and interval < self.min_interval:
            raise serializers.ValidationError(
                {
                    "interval": (
                        f"interval must be at least {self.min_interval} "
                        "minutes for this action."
                    )
                }
            )
        return data


class ForwardNQEMapSerializer(NestedGroupModelSerializer):
    netbox_model = ContentTypeField(
        queryset=ContentType.objects.filter(FORWARD_SUPPORTED_SYNC_MODELS)
    )
    execution_mode = serializers.CharField(read_only=True)
    execution_value = serializers.CharField(read_only=True)

    class Meta:
        model = ForwardNQEMap
        fields = (
            "id",
            "name",
            "display",
            "netbox_model",
            "query_id",
            "query_repository",
            "query_path",
            "query",
            "commit_id",
            "parameters",
            "coalesce_fields",
            "execution_mode",
            "execution_value",
            "enabled",
            "built_in",
            "weight",
            "created",
            "last_updated",
        )
        brief_fields = (
            "id",
            "name",
            "display",
            "netbox_model",
            "execution_mode",
            "execution_value",
            "enabled",
        )


class ForwardSourceSerializer(NestedGroupModelSerializer):
    status = ChoiceField(choices=ForwardSourceStatusChoices, read_only=True)
    type = ChoiceField(choices=ForwardSourceDeploymentChoices)

    class Meta:
        model = ForwardSource
        fields = (
            "id",
            "display",
            "name",
            "type",
            "url",
            "status",
            "parameters",
            "description",
            "comments",
            "last_synced",
            "created",
            "last_updated",
        )
        brief_fields = (
            "id",
            "display",
            "name",
            "type",
            "url",
            "status",
        )

    def to_representation(self, instance):
        data = super().to_representation(instance)
        data["parameters"] = instance.get_masked_parameters()
        return data


class ForwardSyncSerializer(NestedGroupModelSerializer):
    status = ChoiceField(choices=ForwardSyncStatusChoices, read_only=True)
    source = ForwardSourceSerializer(nested=True)
    enabled_models = serializers.SerializerMethodField(read_only=True)
    latest_validation_run = serializers.SerializerMethodField(read_only=True)
    analysis_summary = serializers.SerializerMethodField(read_only=True)
    workload_summary = serializers.SerializerMethodField(read_only=True)
    advisory_summary = serializers.SerializerMethodField(read_only=True)

    class Meta:
        model = ForwardSync
        fields = (
            "id",
            "display",
            "name",
            "source",
            "enabled_models",
            "status",
            "parameters",
            "auto_merge",
            "drift_policy",
            "latest_validation_run",
            "analysis_summary",
            "workload_summary",
            "advisory_summary",
            "last_synced",
            "scheduled",
            "interval",
            "user",
            "created",
            "last_updated",
        )
        brief_fields = (
            "id",
            "display",
            "name",
            "source",
            "enabled_models",
            "status",
            "auto_merge",
            "analysis_summary",
            "workload_summary",
            "advisory_summary",
        )

    def get_enabled_models(self, obj):
        return obj.enabled_models()

    def get_latest_validation_run(self, obj):
        validation_run = obj.latest_validation_run
        return validation_run.pk if validation_run else None

    def get_analysis_summary(self, obj):
        return obj.get_analysis_summary()

    def get_workload_summary(self, obj):
        return obj.get_workload_summary()

    def get_advisory_summary(self, obj):
        return obj.get_advisory_summary()

    def to_representation(self, instance):
        data = super().to_representation(instance)
        data["parameters"] = instance.get_display_parameters()
        return data


class ForwardDeviceAnalysisSerializer(NestedGroupModelSerializer):
    sync = ForwardSyncSerializer(nested=True)
    device = DeviceSerializer(nested=True)

    class Meta:
        model = ForwardDeviceAnalysis
        fields = (
            "id",
            "display",
            "sync",
            "device",
            "reachable",
            "blast_radius",
            "cve_count",
            "up_interfaces",
            "detail",
            "snapshot_id",
            "created",
            "last_updated",
        )
        brief_fields = (
            "id",
            "display",
            "device",
            "reachable",
            "blast_radius",
            "cve_count",
        )


class ForwardDriftPolicySerializer(NestedGroupModelSerializer):
    baseline_mode = ChoiceField(choices=ForwardDriftPolicyBaselineChoices)

    class Meta:
        model = ForwardDriftPolicy
        fields = (
            "id",
            "display",
            "name",
            "enabled",
            "baseline_mode",
            "require_processed_snapshot",
            "block_on_query_errors",
            "block_on_zero_rows",
            "max_deleted_objects",
            "max_deleted_percent",
            "created",
            "last_updated",
        )
        brief_fields = ("id", "display", "name", "enabled", "baseline_mode")


class ForwardValidationRunSerializer(NestedGroupModelSerializer):
    status = ChoiceField(choices=ForwardValidationStatusChoices, read_only=True)
    sync = ForwardSyncSerializer(nested=True)
    policy = ForwardDriftPolicySerializer(nested=True, required=False, allow_null=True)
    override_user = serializers.CharField(read_only=True)

    class Meta:
        model = ForwardValidationRun
        fields = (
            "id",
            "display",
            "sync",
            "policy",
            "job",
            "status",
            "allowed",
            "snapshot_selector",
            "snapshot_id",
            "baseline_snapshot_id",
            "snapshot_info",
            "snapshot_metrics",
            "model_results",
            "drift_summary",
            "blocking_reasons",
            "override_applied",
            "override_user",
            "override_reason",
            "override_blocking_reasons",
            "override_at",
            "created",
            "started",
            "completed",
        )
        brief_fields = ("id", "display", "sync", "status", "allowed", "snapshot_id")


class ForwardValidationRunOverrideSerializer(serializers.Serializer):
    reason = serializers.CharField()


class ForwardIngestionSerializer(NestedGroupModelSerializer):
    branch = BranchSerializer(read_only=True)
    sync = ForwardSyncSerializer(nested=True)
    validation_run = ForwardValidationRunSerializer(nested=True, required=False)
    analysis_summary = serializers.SerializerMethodField(read_only=True)
    workload_summary = serializers.SerializerMethodField(read_only=True)
    advisory_summary = serializers.SerializerMethodField(read_only=True)

    class Meta:
        model = ForwardIngestion
        fields = (
            "id",
            "display",
            "name",
            "branch",
            "sync",
            "validation_run",
            "analysis_summary",
            "workload_summary",
            "advisory_summary",
            "snapshot_selector",
            "snapshot_id",
            "snapshot_info",
            "snapshot_metrics",
            "model_results",
            "created",
        )
        brief_fields = ("id", "display", "name", "branch", "sync", "snapshot_id")

    def get_analysis_summary(self, obj):
        return obj.get_analysis_summary()

    def get_workload_summary(self, obj):
        return obj.get_workload_summary()

    def get_advisory_summary(self, obj):
        return obj.get_advisory_summary()


class ForwardIngestionIssueSerializer(NestedGroupModelSerializer):
    phase = ChoiceField(choices=ForwardIngestionPhaseChoices, read_only=True)
    ingestion = ForwardIngestionSerializer(nested=True)

    def to_representation(self, instance):
        data = super().to_representation(instance)
        data["coalesce_fields"] = json_safe_value(data.get("coalesce_fields"))
        data["defaults"] = json_safe_value(data.get("defaults"))
        data["raw_data"] = json_safe_value(data.get("raw_data"))
        return data

    class Meta:
        model = ForwardIngestionIssue
        fields = (
            "id",
            "ingestion",
            "timestamp",
            "phase",
            "model",
            "message",
            "coalesce_fields",
            "defaults",
            "raw_data",
            "exception",
        )
