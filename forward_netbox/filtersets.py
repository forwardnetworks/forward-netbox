import django_filters
from django.db.models import Q
from django.utils.translation import gettext as _

from core.choices import DataSourceStatusChoices
from netbox.filtersets import BaseFilterSet, ChangeLoggedModelFilterSet, NetBoxModelFilterSet

from .models import ForwardData, ForwardSnapshot, ForwardSource
from .choices import ForwardSnapshotStatusModelChoices


class ForwardDataFilterSet(BaseFilterSet):
    q = django_filters.CharFilter(method="search")

    class Meta:
        model = ForwardData
        fields = ["snapshot_data", "type"]

    def search(self, queryset, name, value):
        if not value.strip():
            return queryset
        return queryset.filter(
            Q(snapshot_data__snapshot_id__icontains=value) | Q(type__icontains=value)
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
    status = django_filters.MultipleChoiceFilter(
        choices=ForwardSnapshotStatusModelChoices,
        null_value=None,
    )

    class Meta:
        model = ForwardSnapshot
        fields = ("id", "status", "snapshot_id")

    def search(self, queryset, name, value):
        if not value.strip():
            return queryset
        return queryset.filter(Q(snapshot_id__icontains=value))


class ForwardSourceFilterSet(NetBoxModelFilterSet):
    status = django_filters.MultipleChoiceFilter(
        choices=DataSourceStatusChoices, null_value=None
    )

    class Meta:
        model = ForwardSource
        fields = ("id", "name")

    def search(self, queryset, name, value):
        if not value.strip():
            return queryset
        return queryset.filter(
            Q(name__icontains=value)
            | Q(description__icontains=value)
            | Q(comments__icontains=value)
        )
