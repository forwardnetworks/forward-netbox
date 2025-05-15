from datetime import timezone

from django.contrib import messages
from django.contrib.auth.mixins import LoginRequiredMixin
from django.core.cache import cache
from django.db import models
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils.module_loading import import_string
from django.views.generic import View

from core.choices import DataSourceStatusChoices
from core.models import Job

from dcim.models import Device, Site
from netbox.views import generic
from netbox.views.generic.base import BaseObjectView
from utilities.forms import ConfirmationForm
from utilities.query import count_related
from utilities.views import get_viewname, register_model_view, ViewTab

from .filtersets import (
    ForwardDataFilterSet,
    ForwardSnapshotFilterSet,
    ForwardSourceFilterSet,
)
from .forms import (
    ForwardNQEMapForm,
    ForwardSnapshotFilterForm,
    ForwardSourceFilterForm,
    ForwardSourceForm,
    ForwardSyncForm,
    ForwardTableForm,
)
from .models import (
    ForwardData,
    ForwardSnapshot,
    ForwardSource,
    ForwardSync,
    ForwardNQEMap,
)
from .tables import (
    ForwardDataTable,
    ForwardSnapshotTable,
    ForwardSourceTable,
    ForwardNQEMapTable,
    ForwardSyncTable,
)
from .utilities.fwdutils import Forward
from .utilities.nqe_map import build_nqe_maps, get_nqe_map


class ForwardSourceListView(generic.ObjectListView):
    queryset = ForwardSource.objects.all()
    filterset = ForwardSourceFilterSet
    filterset_form = ForwardSourceFilterForm
    table = ForwardSourceTable


@register_model_view(ForwardSource)
class ForwardSourceView(generic.ObjectView):
    queryset = ForwardSource.objects.all()


@register_model_view(ForwardSource, "edit")
class ForwardSourceEditView(generic.ObjectEditView):
    queryset = ForwardSource.objects.all()
    form = ForwardSourceForm
    default_return_url = "plugins:forward_netbox:forwardsource_list"


@register_model_view(ForwardSource, "delete")
class ForwardSourceDeleteView(generic.ObjectDeleteView):
    queryset = ForwardSource.objects.all()
    default_return_url = "plugins:forward_netbox:forwardsource_list"


class ForwardSourceBulkDeleteView(generic.BulkDeleteView):
    queryset = ForwardSource.objects.all()
    table = ForwardSourceTable


@register_model_view(ForwardSource, "sync")
class ForwardSourceSyncView(View):
    def post(self, request, pk):
        source = get_object_or_404(ForwardSource, pk=pk)

        try:
            source.status = DataSourceStatusChoices.QUEUED
            source.save()

            job = Job.enqueue(
                import_string("forward_netbox.jobs.sync_forwardsource"),
                name=f"{source.name} Snapshot Sync",
                instance=source,
                user=request.user,
                adhoc=True
            )

            messages.success(request, f"Sync job for source {source.name} has been enqueued.")
        except Exception as e:
            messages.error(request, f"Failed to enqueue sync job: {str(e)}")

        return redirect(source.get_absolute_url())


class ForwardSnapshotListView(generic.ObjectListView):
    queryset = ForwardSnapshot.objects.all()
    filterset = ForwardSnapshotFilterSet
    filterset_form = ForwardSnapshotFilterForm
    table = ForwardSnapshotTable


@register_model_view(ForwardSnapshot)
class ForwardSnapshotView(generic.ObjectView):
    queryset = ForwardSnapshot.objects.all()


@register_model_view(ForwardSnapshot, "delete")
class ForwardSnapshotDeleteView(generic.ObjectDeleteView):
    queryset = ForwardSnapshot.objects.all()
    default_return_url = "plugins:forward_netbox:forwardsnapshot_list"


class ForwardSnapshotBulkDeleteView(generic.BulkDeleteView):
    queryset = ForwardSnapshot.objects.all()
    table = ForwardSnapshotTable


@register_model_view(ForwardSnapshot, "data")
class ForwardSnapshotRawView(generic.ObjectChildrenView):
    queryset = ForwardSnapshot.objects.all()
    child_model = ForwardData
    table = ForwardDataTable
    template_name = "forward_netbox/inc/snapshotdata.html"
    tab = ViewTab(
        label="Raw Data",
        badge=lambda obj: ForwardData.objects.filter(snapshot_data=obj).count(),
        permission="forward_netbox.view_forwardsnapshot",
        hide_if_empty=True,
    )

    def get_children(self, request, parent):
        return self.child_model.objects.filter(snapshot_data=parent)


@register_model_view(ForwardData)
class ForwardDataView(generic.ObjectView):
    queryset = ForwardData.objects.all()

@register_model_view(ForwardData, "delete")
class ForwardSnapshotDataDeleteView(generic.ObjectDeleteView):
    queryset = ForwardData.objects.all()
    default_return_url = "plugins:forward_netbox:forwarddata_list"

class ForwardSnapshotDataBulkDeleteView(generic.BulkDeleteView):
    queryset = ForwardData.objects.all()
    filterset = ForwardDataFilterSet
    table = ForwardDataTable

@register_model_view(
    ForwardData,
    name="data",
    path="json",
    kwargs={},
)
class ForwardDataJSONView(LoginRequiredMixin, View):
    template_name = "forward_netbox/inc/json.html"

    def get(self, request, **kwargs):
        if request.htmx:
            data = get_object_or_404(ForwardData, pk=kwargs.get("pk"))
            return render(
                request,
                self.template_name,
                {
                    "object": data,
                },
            )

class ForwardDataListView(generic.ObjectListView):
    queryset = ForwardData.objects.all()
    filterset = ForwardDataFilterSet
    table = ForwardDataTable


class ForwardSyncListView(generic.ObjectListView):
    queryset = ForwardSync.objects.all()
    table = ForwardSyncTable


@register_model_view(ForwardSync)
class ForwardSyncView(generic.ObjectView):
    queryset = ForwardSync.objects.all()


@register_model_view(ForwardSync, "edit")
class ForwardSyncEditView(generic.ObjectEditView):
    queryset = ForwardSync.objects.all()
    form = ForwardSyncForm

    def alter_object(self, obj, request, url_args, url_kwargs):
        obj.user = request.user
        return obj


@register_model_view(ForwardSync, "sync")
class ForwardIngestSyncView(BaseObjectView):
    queryset = ForwardSync.objects.all()

    def get_required_permission(self):
        return "forward_netbox.sync_ingest"

    def get(self, request, pk):
        obj = get_object_or_404(self.queryset, pk=pk)
        return redirect(obj.get_absolute_url())

    def post(self, request, pk):
        obj = get_object_or_404(self.queryset, pk=pk)

        try:
            job = Job.enqueue(
                import_string("forward_netbox.jobs.sync_forward"),
                name=f"{obj.name} Ingest Sync",
                instance=obj,
                user=request.user,
                adhoc=True
            )
            messages.success(request, f"Queued job #{job.pk} to sync {obj}")
        except Exception as e:
            messages.error(request, f"Failed to enqueue sync job: {str(e)}")

        return redirect(obj.get_absolute_url())


@register_model_view(ForwardSync, "delete")
class ForwardSyncDeleteView(generic.ObjectDeleteView):
    queryset = ForwardSync.objects.all()
    default_return_url = "plugins:forward_netbox:forwardsync_list"

    def get_extra_context(self, request, instance):
        return {}


class ForwardSyncBulkDeleteView(generic.BulkDeleteView):
    queryset = ForwardSync.objects.all()
    table = ForwardSyncTable


@register_model_view(ForwardNQEMap)
class ForwardNQEMapView(generic.ObjectView):
    queryset = ForwardNQEMap.objects.all()


@register_model_view(ForwardNQEMap, "edit")
class ForwardNQEMapEditView(generic.ObjectEditView):
    queryset = ForwardNQEMap.objects.all()
    form = ForwardNQEMapForm
    default_return_url = "plugins:forward_netbox:forwardnqemap_list"


class ForwardNQEMapDeleteView(generic.ObjectDeleteView):
    queryset = ForwardNQEMap.objects.all()
    default_return_url = "plugins:forward_netbox:forwardnqemap_list"


class ForwardNQEMapBulkDeleteView(generic.BulkDeleteView):
    queryset = ForwardNQEMap.objects.all()
    table = ForwardNQEMapTable


class ForwardNQEMapRestoreView(View):
    def post(self, request):
        ForwardNQEMap.objects.all().delete()
        build_nqe_maps(data=get_nqe_map())
        messages.success(request, "Forward NQE Map has been restored to defaults.")
        return redirect("plugins:forward_netbox:forwardnqemap_list")


class ForwardNQEMapListView(generic.ObjectListView):
    queryset = ForwardNQEMap.objects.all()
    table = ForwardNQEMapTable
