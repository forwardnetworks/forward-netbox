import django_filters
from core.choices import ObjectChangeActionChoices
from core.models import ObjectChange
from django.db.models import Q
from netbox.filtersets import BaseFilterSet
from netbox.filtersets import ChangeLoggedModelFilterSet
from netbox.filtersets import NetBoxModelFilterSet
from netbox_branching.models import ChangeDiff

from .choices import ForwardDriftPolicyBaselineChoices
from .choices import ForwardIngestionPhaseChoices
from .choices import ForwardSourceDeploymentChoices
from .choices import ForwardSourceStatusChoices
from .choices import ForwardSyncStatusChoices
from .choices import ForwardValidationStatusChoices
from .models import ForwardDriftPolicy
from .models import ForwardIngestion
from .models import ForwardIngestionIssue
from .models import ForwardNQEMap
from .models import ForwardSource
from .models import ForwardSync
from .models import ForwardValidationRun


class ForwardSourceFilterSet(NetBoxModelFilterSet):
    q = django_filters.CharFilter(method="search")
    status = django_filters.MultipleChoiceFilter(
        choices=ForwardSourceStatusChoices,
        null_value=None,
    )
    type = django_filters.MultipleChoiceFilter(
        choices=ForwardSourceDeploymentChoices,
        null_value=None,
    )

    class Meta:
        model = ForwardSource
        fields = ("id", "name", "status", "type")

    def search(self, queryset, name, value):
        if not value.strip():
            return queryset
        return queryset.filter(
            Q(name__icontains=value)
            | Q(description__icontains=value)
            | Q(url__icontains=value)
            | Q(comments__icontains=value)
        )


class ForwardNQEMapFilterSet(ChangeLoggedModelFilterSet):
    q = django_filters.CharFilter(method="search")

    class Meta:
        model = ForwardNQEMap
        fields = ("id", "name", "netbox_model", "enabled", "built_in")

    def search(self, queryset, name, value):
        if not value.strip():
            return queryset
        return queryset.filter(
            Q(name__icontains=value)
            | Q(query_id__icontains=value)
            | Q(query__icontains=value)
            | Q(commit_id__icontains=value)
            | Q(netbox_model__app_label__icontains=value)
            | Q(netbox_model__model__icontains=value)
        )


class ForwardSyncFilterSet(ChangeLoggedModelFilterSet):
    q = django_filters.CharFilter(method="search")
    status = django_filters.MultipleChoiceFilter(
        choices=ForwardSyncStatusChoices,
        null_value=None,
    )

    class Meta:
        model = ForwardSync
        fields = ("id", "name", "status", "source", "drift_policy")

    def search(self, queryset, name, value):
        if not value.strip():
            return queryset
        return queryset.filter(
            Q(name__icontains=value) | Q(source__name__icontains=value)
        )


class ForwardIngestionChangeFilterSet(BaseFilterSet):
    q = django_filters.CharFilter(method="search")
    action = django_filters.MultipleChoiceFilter(choices=ObjectChangeActionChoices)

    class Meta:
        model = ChangeDiff
        fields = ("branch", "action", "object_type")

    def search(self, queryset, name, value):
        if not value.strip():
            return queryset
        return queryset.filter(
            Q(current__icontains=value)
            | Q(modified__icontains=value)
            | Q(original__icontains=value)
            | Q(action__icontains=value)
            | Q(object_type__model__icontains=value)
        )


class ForwardIngestionObjectChangeFilterSet(BaseFilterSet):
    q = django_filters.CharFilter(method="search")
    action = django_filters.MultipleChoiceFilter(choices=ObjectChangeActionChoices)

    class Meta:
        model = ObjectChange
        fields = ("action", "changed_object_type")

    def search(self, queryset, name, value):
        if not value.strip():
            return queryset
        return queryset.filter(
            Q(prechange_data__icontains=value)
            | Q(postchange_data__icontains=value)
            | Q(action__icontains=value)
            | Q(changed_object_type__model__icontains=value)
            | Q(object_repr__icontains=value)
        )


class ForwardIngestionFilterSet(BaseFilterSet):
    q = django_filters.CharFilter(method="search")
    sync_id = django_filters.ModelMultipleChoiceFilter(
        field_name="sync",
        queryset=ForwardSync.objects.all(),
        label="Sync (ID)",
    )
    sync = django_filters.ModelMultipleChoiceFilter(
        field_name="sync__name",
        queryset=ForwardSync.objects.all(),
        to_field_name="name",
        label="Sync (name)",
    )

    class Meta:
        model = ForwardIngestion
        fields = ("id", "branch", "sync", "validation_run")

    def search(self, queryset, name, value):
        if not value.strip():
            return queryset
        return queryset.filter(
            Q(branch__name__icontains=value) | Q(sync__name__icontains=value)
        )


class ForwardIngestionIssueFilterSet(BaseFilterSet):
    q = django_filters.CharFilter(method="search")
    phase = django_filters.MultipleChoiceFilter(
        choices=ForwardIngestionPhaseChoices,
        null_value=None,
    )

    class Meta:
        model = ForwardIngestionIssue
        fields = ("phase", "model", "timestamp", "exception", "message")

    def search(self, queryset, name, value):
        if not value.strip():
            return queryset
        return queryset.filter(
            Q(phase__icontains=value)
            | Q(model__icontains=value)
            | Q(exception__icontains=value)
            | Q(message__icontains=value)
        )


class ForwardDriftPolicyFilterSet(ChangeLoggedModelFilterSet):
    q = django_filters.CharFilter(method="search")
    baseline_mode = django_filters.MultipleChoiceFilter(
        choices=ForwardDriftPolicyBaselineChoices,
        null_value=None,
    )

    class Meta:
        model = ForwardDriftPolicy
        fields = ("id", "name", "enabled", "baseline_mode")

    def search(self, queryset, name, value):
        if not value.strip():
            return queryset
        return queryset.filter(Q(name__icontains=value))


class ForwardValidationRunFilterSet(BaseFilterSet):
    q = django_filters.CharFilter(method="search")
    status = django_filters.MultipleChoiceFilter(
        choices=ForwardValidationStatusChoices,
        null_value=None,
    )

    class Meta:
        model = ForwardValidationRun
        fields = ("id", "sync", "policy", "status", "allowed", "snapshot_id")

    def search(self, queryset, name, value):
        if not value.strip():
            return queryset
        return queryset.filter(
            Q(sync__name__icontains=value)
            | Q(snapshot_id__icontains=value)
            | Q(baseline_snapshot_id__icontains=value)
        )
