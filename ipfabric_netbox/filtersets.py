import django_filters
from core.choices import DataSourceStatusChoices
from core.choices import ObjectChangeActionChoices
from django.db.models import Q
from django.utils.translation import gettext as _
from netbox.filtersets import BaseFilterSet
from netbox.filtersets import ChangeLoggedModelFilterSet
from netbox.filtersets import NetBoxModelFilterSet
from netbox_branching.models import ChangeDiff

from .models import IPFabricData
from .models import IPFabricIngestion
from .models import IPFabricIngestionIssue
from .models import IPFabricRelationshipField
from .models import IPFabricSnapshot
from .models import IPFabricSource
from .models import IPFabricSync
from .models import IPFabricTransformField
from .models import IPFabricTransformMap
from .models import IPFabricTransformMapGroup


class IPFabricIngestionChangeFilterSet(BaseFilterSet):
    q = django_filters.CharFilter(method="search")
    action = django_filters.MultipleChoiceFilter(choices=ObjectChangeActionChoices)

    class Meta:
        model = ChangeDiff
        fields = ["branch", "action", "object_type"]

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


class IPFabricIngestionIssueFilterSet(BaseFilterSet):
    q = django_filters.CharFilter(method="search")

    class Meta:
        model = IPFabricIngestionIssue
        fields = [
            "model",
            "timestamp",
            "raw_data",
            "coalesce_fields",
            "defaults",
            "exception",
            "message",
        ]

    def search(self, queryset, name, value):
        if not value.strip():
            return queryset
        return queryset.filter(
            Q(model__icontains=value)
            | Q(timestamp__icontains=value)
            | Q(raw_data__icontains=value)
            | Q(coalesce_fields__icontains=value)
            | Q(defaults__icontains=value)
            | Q(exception__icontains=value)
            | Q(message__icontains=value)
        )


class IPFabricDataFilterSet(BaseFilterSet):
    q = django_filters.CharFilter(method="search")

    class Meta:
        model = IPFabricData
        fields = ["snapshot_data"]

    def search(self, queryset, name, value):
        if not value.strip():
            return queryset
        return queryset.filter(Q(snapshot_data__icontains=value))


class IPFabricSnapshotFilterSet(ChangeLoggedModelFilterSet):
    q = django_filters.CharFilter(method="search")
    source_id = django_filters.ModelMultipleChoiceFilter(
        queryset=IPFabricSource.objects.all(),
        label=_("Source (ID)"),
    )
    source = django_filters.ModelMultipleChoiceFilter(
        field_name="source__name",
        queryset=IPFabricSource.objects.all(),
        to_field_name="name",
        label=_("Source (name)"),
    )
    snapshot_id = django_filters.CharFilter(
        label=_("Snapshot ID"), lookup_expr="icontains"
    )

    class Meta:
        model = IPFabricSnapshot
        fields = ("id", "name", "status", "snapshot_id")

    def search(self, queryset, name, value):
        if not value.strip():
            return queryset
        return queryset.filter(Q(name__icontains=value))


class IPFabricSourceFilterSet(NetBoxModelFilterSet):
    status = django_filters.MultipleChoiceFilter(
        choices=DataSourceStatusChoices, null_value=None
    )

    class Meta:
        model = IPFabricSource
        fields = ("id", "name")

    def search(self, queryset, name, value):
        if not value.strip():
            return queryset
        return queryset.filter(
            Q(name__icontains=value)
            | Q(description__icontains=value)
            | Q(comments__icontains=value)
        )


class IPFabricIngestionFilterSet(BaseFilterSet):
    q = django_filters.CharFilter(method="search")
    sync_id = django_filters.ModelMultipleChoiceFilter(
        queryset=IPFabricSync.objects.all(),
        label=_("Sync (ID)"),
    )
    sync = django_filters.ModelMultipleChoiceFilter(
        field_name="sync__name",
        queryset=IPFabricSync.objects.all(),
        to_field_name="branch__name",
        label=_("Sync (name)"),
    )

    class Meta:
        model = IPFabricIngestion
        fields = ("id", "branch", "sync")

    def search(self, queryset, branch, value):
        if not value.strip():
            return queryset
        return queryset.filter(
            Q(branch__name__icontains=value) | Q(sync__name__icontains=value)
        )


class IPFabricTransformMapGroupFilterSet(NetBoxModelFilterSet):
    q = django_filters.CharFilter(method="search")

    class Meta:
        model = IPFabricTransformMapGroup
        fields = ("id", "name", "description")

    def search(self, queryset, name, value):
        if not value.strip():
            return queryset
        return queryset.filter(
            Q(name__icontains=value) | Q(description__icontains=value)
        )


class IPFabricTransformMapFilterSet(NetBoxModelFilterSet):
    q = django_filters.CharFilter(method="search")
    group_id = django_filters.ModelMultipleChoiceFilter(
        queryset=IPFabricTransformMapGroup.objects.all(),
        label=_("Transform Map Group (ID)"),
    )
    group = django_filters.ModelMultipleChoiceFilter(
        queryset=IPFabricTransformMapGroup.objects.all(), label=_("Transform Map Group")
    )

    class Meta:
        model = IPFabricTransformMap
        fields = ("id", "name", "group", "source_model", "target_model")

    def search(self, queryset, name, value):
        if not value.strip():
            return queryset
        return queryset.filter(
            Q(group__name__icontains=value) | Q(name__icontains=value)
        )


class IPFabricTransformFieldFilterSet(BaseFilterSet):
    transform_map = django_filters.ModelMultipleChoiceFilter(
        queryset=IPFabricTransformMap.objects.all(), label=_("Transform Map")
    )

    class Meta:
        model = IPFabricTransformField
        fields = (
            "id",
            "transform_map",
            "source_field",
            "target_field",
            "coalesce",
            "template",
        )


class IPFabricRelationshipFieldFilterSet(BaseFilterSet):
    transform_map = django_filters.ModelMultipleChoiceFilter(
        queryset=IPFabricTransformMap.objects.all(), label=_("Transform Map")
    )

    class Meta:
        model = IPFabricRelationshipField
        fields = (
            "id",
            "transform_map",
            "source_model",
            "target_field",
            "coalesce",
            "template",
        )


class IPFabricSyncFilterSet(ChangeLoggedModelFilterSet):
    q = django_filters.CharFilter(method="search")
    snapshot_data_id = django_filters.ModelMultipleChoiceFilter(
        queryset=IPFabricSnapshot.objects.all(),
        label=_("Snapshot (ID)"),
    )
    snapshot_data = django_filters.ModelMultipleChoiceFilter(
        field_name="snapshot_data__name",
        queryset=IPFabricSnapshot.objects.all(),
        to_field_name="name",
        label=_("Snapshot (name)"),
    )

    class Meta:
        model = IPFabricSync
        fields = (
            "id",
            "name",
            "snapshot_data",
            "snapshot_data_id",
            "status",
            "auto_merge",
            "last_synced",
            "scheduled",
            "interval",
        )

    def search(self, queryset, name, value):
        if not value.strip():
            return queryset
        return queryset.filter(
            Q(name__icontains=value) | Q(snapshot_data__name__icontains=value)
        )
