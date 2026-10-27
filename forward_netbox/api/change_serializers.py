# REST serializers for change control.
#
# They are also what NetBox's event machinery needs: it serializes every object
# deleted in a web request (and every object an event rule matches) through
# `forward_netbox.api.serializers.<Model>Serializer`, so a change-logged model
# without one cannot be deleted from the UI at all.
#
# Writes that must follow a rule (the state machine, the freeze on criteria, the
# gate on reviews) do not go through these serializers: `state`, `verdict` and the
# snapshot ids are read-only here and move only through the actions on the viewset,
# which call the same service functions the web pages do.
from dcim.models import DeviceRole
from dcim.models import Platform
from dcim.models import Region
from dcim.models import Site
from dcim.models import SiteGroup
from extras.models import Tag
from netbox.api.fields import ChoiceField
from netbox.api.serializers import PrimaryModelSerializer
from netbox.api.serializers import ValidatedModelSerializer
from netbox_branching.api.serializers import BranchSerializer
from rest_framework import serializers
from users.api.serializers import UserSerializer

from forward_netbox.change_control.choices import ForwardChangeStateChoices
from forward_netbox.change_control.choices import ForwardChangeVerdictChoices
from forward_netbox.change_control.choices import ForwardCriterionExpectationChoices
from forward_netbox.change_control.choices import ForwardCriterionFamilyChoices
from forward_netbox.change_control.choices import ForwardEvidencePhaseChoices
from forward_netbox.change_control.choices import ForwardReviewPhaseChoices
from forward_netbox.change_control.choices import ForwardValidationEngineChoices
from forward_netbox.change_control.choices import ForwardValidationResultStatusChoices
from forward_netbox.change_control.choices import ForwardValidationRunStatusChoices
from forward_netbox.change_control.choices import ForwardValidationSeverityChoices
from forward_netbox.change_control.choices import ForwardValidationTriggerChoices
from forward_netbox.models import ForwardChange
from forward_netbox.models import ForwardChangeApprovalRule
from forward_netbox.models import ForwardChangeComment
from forward_netbox.models import ForwardChangeCommentReply
from forward_netbox.models import ForwardChangeCriterion
from forward_netbox.models import ForwardChangeDevice
from forward_netbox.models import ForwardChangeEvidence
from forward_netbox.models import ForwardChangePolicy
from forward_netbox.models import ForwardChangePolicyRule
from forward_netbox.models import ForwardChangeReview
from forward_netbox.models import ForwardChangeValidationPolicy
from forward_netbox.models import ForwardChangeValidationResult
from forward_netbox.models import ForwardChangeValidationRule
from forward_netbox.models import ForwardChangeValidationRun
from forward_netbox.models import ForwardSource


class ForwardChangePolicySerializer(PrimaryModelSerializer):
    class Meta:
        model = ForwardChangePolicy
        fields = (
            "id",
            "display",
            "name",
            "enabled",
            "min_pre_approvals",
            "min_post_approvals",
            "description",
            "comments",
            "created",
            "last_updated",
        )
        brief_fields = ("id", "display", "name", "enabled")


class ForwardChangePolicyRuleSerializer(ValidatedModelSerializer):
    policy = ForwardChangePolicySerializer(nested=True)

    class Meta:
        model = ForwardChangePolicyRule
        fields = (
            "id",
            "display",
            "policy",
            "device_role",
            "site",
            "tag_slug",
            "created",
            "last_updated",
        )
        brief_fields = ("id", "display", "policy")


class ForwardChangeApprovalRuleSerializer(ValidatedModelSerializer):
    policy = ForwardChangePolicySerializer(nested=True)

    class Meta:
        model = ForwardChangeApprovalRule
        fields = (
            "id",
            "display",
            "policy",
            "name",
            "min_reviews",
            "any_reviewer",
            "groups",
            "reviewers",
            "created",
            "last_updated",
        )
        brief_fields = ("id", "display", "name", "min_reviews")

    def validate(self, attrs):
        attrs = super().validate(attrs)
        instance = self.instance
        any_reviewer = attrs.get(
            "any_reviewer", instance.any_reviewer if instance else False
        )
        groups = attrs.get("groups", instance.groups.all() if instance else [])
        reviewers = attrs.get("reviewers", instance.reviewers.all() if instance else [])
        if not any_reviewer and not groups and not reviewers:
            raise serializers.ValidationError(
                "Name at least one group or reviewer, or mark the rule open to any "
                "reviewer. A rule that names nobody would block every change."
            )
        return attrs


class ForwardChangeDeviceSerializer(serializers.ModelSerializer):
    device = serializers.PrimaryKeyRelatedField(read_only=True)
    device_name = serializers.CharField(source="device.name", read_only=True)

    class Meta:
        model = ForwardChangeDevice
        fields = ("id", "device", "device_name", "forward_device_key")
        read_only_fields = fields


class ForwardChangeSerializer(PrimaryModelSerializer):
    source = serializers.PrimaryKeyRelatedField(queryset=ForwardSource.objects.all())
    state = ChoiceField(choices=ForwardChangeStateChoices, read_only=True)
    verdict = ChoiceField(
        choices=ForwardChangeVerdictChoices, read_only=True, allow_blank=True
    )
    status = serializers.SerializerMethodField()
    review_anchor = serializers.SerializerMethodField()
    requester = UserSerializer(nested=True, read_only=True)
    branch = BranchSerializer(nested=True, required=False, allow_null=True)
    comment_count = serializers.IntegerField(read_only=True)
    devices = ForwardChangeDeviceSerializer(many=True, read_only=True)

    class Meta:
        model = ForwardChange
        fields = (
            "id",
            "display",
            "ref",
            "title",
            "source",
            "state",
            "status",
            "review_anchor",
            "verdict",
            "requester",
            "branch",
            "branch_name",
            "priority",
            "summary",
            "window_start",
            "window_end",
            "before_snapshot_id",
            "after_snapshot_id",
            "workspace_network_id",
            "workspace_origin",
            "applied_at",
            "applied_ref",
            "predict_status",
            "predict_reason",
            "predict_snapshot_id",
            "predict_change_set_id",
            "comment_count",
            "devices",
            "description",
            "comments",
            "created",
            "last_updated",
        )
        read_only_fields = (
            "branch_name",
            "before_snapshot_id",
            "after_snapshot_id",
            "workspace_network_id",
            "workspace_origin",
            "applied_at",
            "applied_ref",
            "predict_status",
            "predict_reason",
            "predict_snapshot_id",
            "predict_change_set_id",
        )
        brief_fields = ("id", "display", "ref", "title", "state", "status")

    def get_status(self, obj):
        from forward_netbox.change_control.status import review_status

        return review_status(obj)

    def get_review_anchor(self, obj):
        """What a review must quote back, so it is of what the reviewer saw."""
        from forward_netbox.change_control.actions import review_anchor

        return review_anchor(obj)

    def validate_branch(self, branch):
        from forward_netbox.change_control.branch_link import branch_blocker

        reason = branch_blocker(self.instance, branch)
        if reason:
            raise serializers.ValidationError(reason)
        return branch


class ForwardChangeCriterionSerializer(ValidatedModelSerializer):
    change = serializers.PrimaryKeyRelatedField(queryset=ForwardChange.objects.all())
    family = ChoiceField(choices=ForwardCriterionFamilyChoices, required=False)
    expectation = ChoiceField(
        choices=ForwardCriterionExpectationChoices, required=False
    )
    is_bound = serializers.BooleanField(read_only=True)

    class Meta:
        model = ForwardChangeCriterion
        fields = (
            "id",
            "display",
            "change",
            "name",
            "family",
            "expectation",
            "blocking",
            "query_path",
            "query_id",
            "commit_id",
            "source_sha256",
            "is_bound",
            "created",
            "last_updated",
        )
        read_only_fields = ("query_id", "commit_id", "source_sha256")
        brief_fields = ("id", "display", "name", "blocking")

    def validate(self, attrs):
        from forward_netbox.change_control.criteria_admin import criterion_blocker

        # Read the stored criterion before NetBox's validation copies the incoming
        # values onto the instance: a criterion belongs to one change for its whole
        # life, and the freeze is judged against the change it is on now.
        stored = None
        if self.instance is not None:
            stored = ForwardChangeCriterion.objects.select_related("change").get(
                pk=self.instance.pk
            )
        attrs = super().validate(attrs)
        if stored is not None:
            if attrs.get("change", stored.change) != stored.change:
                raise serializers.ValidationError(
                    {"change": "A criterion cannot move to another change."}
                )
            probe = stored
        else:
            probe = ForwardChangeCriterion(change=attrs["change"])
        reason = criterion_blocker(probe)
        if reason:
            raise serializers.ValidationError(reason)
        if "query_path" in attrs and not (attrs["query_path"] or "").strip():
            raise serializers.ValidationError(
                {"query_path": "A criterion needs the path of an NQE query to run."}
            )
        return attrs

    def update(self, instance, validated_data):
        from forward_netbox.change_control.criteria_admin import (
            clear_binding_if_path_changed,
        )

        # NetBox's validation applies the incoming values to the instance before
        # `update` runs, so the path it was bound for is read from the database.
        previous = (
            ForwardChangeCriterion.objects.filter(pk=instance.pk)
            .values_list("query_path", flat=True)
            .first()
        )
        for key, value in validated_data.items():
            setattr(instance, key, value)
        clear_binding_if_path_changed(instance, previous)
        instance.save()
        return instance


class ForwardChangeEvidenceSerializer(ValidatedModelSerializer):
    phase = ChoiceField(choices=ForwardEvidencePhaseChoices, read_only=True)
    criterion = serializers.PrimaryKeyRelatedField(read_only=True)
    change = serializers.PrimaryKeyRelatedField(
        source="criterion.change", read_only=True
    )

    class Meta:
        model = ForwardChangeEvidence
        fields = (
            "id",
            "change",
            "criterion",
            "phase",
            "passed",
            "row_count",
            "snapshot_id",
            "commit_id",
            "executed_at",
            "duration_ms",
            "shape",
        )
        read_only_fields = fields
        brief_fields = ("id", "phase", "passed")


class ForwardChangeReviewSerializer(ValidatedModelSerializer):
    phase = ChoiceField(choices=ForwardReviewPhaseChoices, read_only=True)
    reviewer = UserSerializer(nested=True, read_only=True)

    class Meta:
        model = ForwardChangeReview
        fields = (
            "id",
            "display",
            "change",
            "reviewer",
            "phase",
            "approved",
            "comment",
            "created",
            "last_updated",
        )
        read_only_fields = fields
        brief_fields = ("id", "display", "phase", "approved")


class ForwardChangeCommentReplySerializer(ValidatedModelSerializer):
    author = UserSerializer(nested=True, read_only=True)

    class Meta:
        model = ForwardChangeCommentReply
        fields = ("id", "display", "comment", "author", "body", "created")
        read_only_fields = fields
        brief_fields = ("id", "display", "author")


class ForwardChangeCommentSerializer(ValidatedModelSerializer):
    author = UserSerializer(nested=True, read_only=True)
    resolved_by = UserSerializer(nested=True, read_only=True)
    replies = ForwardChangeCommentReplySerializer(many=True, read_only=True)

    class Meta:
        model = ForwardChangeComment
        fields = (
            "id",
            "display",
            "change",
            "author",
            "body",
            "resolved",
            "resolved_by",
            "resolved_at",
            "replies",
            "created",
        )
        read_only_fields = fields
        brief_fields = ("id", "display", "author", "resolved")


class ForwardChangeValidationPolicySerializer(PrimaryModelSerializer):
    sites = serializers.PrimaryKeyRelatedField(
        many=True, required=False, queryset=Site.objects.all()
    )
    regions = serializers.PrimaryKeyRelatedField(
        many=True, required=False, queryset=Region.objects.all()
    )
    site_groups = serializers.PrimaryKeyRelatedField(
        many=True, required=False, queryset=SiteGroup.objects.all()
    )
    device_roles = serializers.PrimaryKeyRelatedField(
        many=True, required=False, queryset=DeviceRole.objects.all()
    )
    platforms = serializers.PrimaryKeyRelatedField(
        many=True, required=False, queryset=Platform.objects.all()
    )
    scope_tags = serializers.PrimaryKeyRelatedField(
        many=True, required=False, queryset=Tag.objects.all()
    )

    class Meta:
        model = ForwardChangeValidationPolicy
        fields = (
            "id",
            "display",
            "name",
            "enabled",
            "min_score",
            "require_passed_run",
            "run_on_submission",
            "sites",
            "regions",
            "site_groups",
            "device_roles",
            "platforms",
            "scope_tags",
            "description",
            "comments",
            "created",
            "last_updated",
        )
        brief_fields = ("id", "display", "name", "enabled")


class ForwardChangeValidationRuleSerializer(ValidatedModelSerializer):
    engine = ChoiceField(choices=ForwardValidationEngineChoices)
    severity = ChoiceField(choices=ForwardValidationSeverityChoices, required=False)

    class Meta:
        model = ForwardChangeValidationRule
        fields = (
            "id",
            "display",
            "policy",
            "name",
            "engine",
            "category",
            "severity",
            "enabled",
            "parameters",
            "created",
            "last_updated",
        )
        brief_fields = ("id", "display", "name", "engine")

    def validate(self, attrs):
        from forward_netbox.change_control.validation import rule_parameter_problems

        attrs = super().validate(attrs)
        engine = attrs.get("engine", self.instance.engine if self.instance else None)
        params = attrs.get(
            "parameters", self.instance.parameters if self.instance else {}
        )
        problems = rule_parameter_problems(engine, params)
        if problems:
            raise serializers.ValidationError({"parameters": problems})
        return attrs


class ForwardChangeValidationResultSerializer(ValidatedModelSerializer):
    engine = ChoiceField(choices=ForwardValidationEngineChoices, read_only=True)
    status = ChoiceField(choices=ForwardValidationResultStatusChoices, read_only=True)
    severity = ChoiceField(choices=ForwardValidationSeverityChoices, read_only=True)

    class Meta:
        model = ForwardChangeValidationResult
        fields = (
            "id",
            "display",
            "run",
            "rule",
            "engine",
            "category",
            "check_name",
            "device_name",
            "status",
            "severity",
            "message",
            "row_count",
        )
        read_only_fields = fields
        brief_fields = ("id", "display", "check_name", "status")


class ForwardChangeValidationRunSerializer(ValidatedModelSerializer):
    status = ChoiceField(choices=ForwardValidationRunStatusChoices, read_only=True)
    trigger = ChoiceField(choices=ForwardValidationTriggerChoices, read_only=True)
    started_by = UserSerializer(nested=True, read_only=True)

    class Meta:
        model = ForwardChangeValidationRun
        fields = (
            "id",
            "display",
            "change",
            "policy",
            "trigger",
            "status",
            "started_by",
            "started_at",
            "completed_at",
            "snapshot_id",
            "before_snapshot_id",
            "after_snapshot_id",
            "score",
            "summary",
            "error",
            "created",
        )
        read_only_fields = fields
        brief_fields = ("id", "display", "status", "score")
