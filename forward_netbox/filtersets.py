import django_filters
from core.choices import DataSourceStatusChoices
from core.choices import ObjectChangeActionChoices
from django.contrib.contenttypes.models import ContentType
from django.db.models import Q
from django.utils.translation import gettext as _
from netbox.filtersets import BaseFilterSet
from netbox.filtersets import ChangeLoggedModelFilterSet
from netbox.filtersets import NetBoxModelFilterSet
from netbox_branching.models import ChangeDiff

from .models import ForwardData
from .models import ForwardIngestion
from .models import ForwardIngestionIssue
from .models import ForwardNQEQuery
from .models import ForwardSnapshot
from .models import ForwardSource
from .models import ForwardSync


class ForwardIngestionChangeFilterSet(BaseFilterSet):
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


class ForwardIngestionIssueFilterSet(BaseFilterSet):
    q = django_filters.CharFilter(method="search")

    class Meta:
        model = ForwardIngestionIssue
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


class ForwardDataFilterSet(BaseFilterSet):
    q = django_filters.CharFilter(method="search")

    class Meta:
        model = ForwardData
        fields = ["snapshot_data"]

    def search(self, queryset, name, value):
        if not value.strip():
            return queryset
        return queryset.filter(Q(snapshot_data__icontains=value))


class ForwardNQEQueryFilterSet(NetBoxModelFilterSet):
    q = django_filters.CharFilter(method="search")
    content_type = django_filters.ModelMultipleChoiceFilter(
        queryset=ContentType.objects.filter(app_label__in=["dcim", "ipam"]),
        label=_("Model"),
    )

    class Meta:
        model = ForwardNQEQuery
        fields = (
            "id",
            "content_type",
            "enabled",
        )

    def search(self, queryset, name, value):
        if not value.strip():
            return queryset
        return queryset.filter(
            Q(content_type__app_label__icontains=value)
            | Q(content_type__model__icontains=value)
            | Q(query_id__icontains=value)
            | Q(description__icontains=value)
        )


class ForwardSnapshotFilterSet(ChangeLoggedModelFilterSet):
    q = django_filters.CharFilter(method="search")
    source_id = django_filters.ModelMultipleChoiceFilter(
        queryset=ForwardSource.objects.all(),
        label=_("Source (ID)"),
    )
    source = django_filters.ModelMultipleChoiceFilter(
        field_name="source__name",
        queryset=ForwardSource.objects.all(),
        to_field_name="name",
        label=_("Source (name)"),
    )
    snapshot_id = django_filters.CharFilter(
        label=_("Snapshot ID"), lookup_expr="icontains"
    )

    class Meta:
        model = ForwardSnapshot
        fields = ("id", "name", "status", "snapshot_id")

    def search(self, queryset, name, value):
        if not value.strip():
            return queryset
        return queryset.filter(Q(name__icontains=value))


class ForwardSourceFilterSet(NetBoxModelFilterSet):
    status = django_filters.MultipleChoiceFilter(
        choices=DataSourceStatusChoices, null_value=None
    )
    network_id = django_filters.CharFilter(lookup_expr="icontains")

    class Meta:
        model = ForwardSource
        fields = ("id", "name", "network_id")

    def search(self, queryset, name, value):
        if not value.strip():
            return queryset
        return queryset.filter(
            Q(name__icontains=value)
            | Q(description__icontains=value)
            | Q(comments__icontains=value)
        )


class ForwardIngestionFilterSet(BaseFilterSet):
    q = django_filters.CharFilter(method="search")
    sync_id = django_filters.ModelMultipleChoiceFilter(
        queryset=ForwardSync.objects.all(),
        label=_("Sync (ID)"),
    )
    sync = django_filters.ModelMultipleChoiceFilter(
        field_name="sync__name",
        queryset=ForwardSync.objects.all(),
        to_field_name="branch__name",
        label=_("Sync (name)"),
    )

    class Meta:
        model = ForwardIngestion
        fields = ("id", "branch", "sync")

    def search(self, queryset, branch, value):
        if not value.strip():
            return queryset
        return queryset.filter(
            Q(branch__name__icontains=value) | Q(sync__name__icontains=value)
        )
class ForwardSyncFilterSet(ChangeLoggedModelFilterSet):
    q = django_filters.CharFilter(method="search")
    snapshot_data_id = django_filters.ModelMultipleChoiceFilter(
        queryset=ForwardSnapshot.objects.all(),
        label=_("Snapshot (ID)"),
    )
    snapshot_data = django_filters.ModelMultipleChoiceFilter(
        field_name="snapshot_data__name",
        queryset=ForwardSnapshot.objects.all(),
        to_field_name="name",
        label=_("Snapshot (name)"),
    )

    class Meta:
        model = ForwardSync
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
