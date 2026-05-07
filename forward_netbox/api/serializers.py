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
from forward_netbox.models import ForwardDriftPolicy
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardIngestionIssue
from forward_netbox.models import ForwardNQEMap
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.models import ForwardValidationRun


class EmptySerializer(serializers.Serializer):
    pass


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
        )

    def get_enabled_models(self, obj):
        return obj.enabled_models()

    def get_latest_validation_run(self, obj):
        validation_run = obj.latest_validation_run
        return validation_run.pk if validation_run else None

    def to_representation(self, instance):
        data = super().to_representation(instance)
        data["parameters"] = instance.get_display_parameters()
        return data


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

    class Meta:
        model = ForwardIngestion
        fields = (
            "id",
            "display",
            "name",
            "branch",
            "sync",
            "validation_run",
            "snapshot_selector",
            "snapshot_id",
            "snapshot_info",
            "snapshot_metrics",
            "model_results",
            "created",
        )
        brief_fields = ("id", "display", "name", "branch", "sync", "snapshot_id")


class ForwardIngestionIssueSerializer(NestedGroupModelSerializer):
    phase = ChoiceField(choices=ForwardIngestionPhaseChoices, read_only=True)
    ingestion = ForwardIngestionSerializer(nested=True)

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
