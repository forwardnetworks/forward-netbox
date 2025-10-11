from core.choices import ObjectChangeActionChoices
from dcim.models import Device
from dcim.models import Site
from django.contrib import messages
from django.contrib.auth.mixins import LoginRequiredMixin, PermissionRequiredMixin
from django.core.cache import cache
from django.core.exceptions import ValidationError
from django.db import models
from django.db import transaction
from django.http import HttpResponse
from django.shortcuts import get_object_or_404
from django.shortcuts import redirect
from django.shortcuts import render
from django.urls import reverse
from django.utils import timezone
from django.views.generic import View
from django_tables2 import RequestConfig
from netbox.views import generic
from netbox.views.generic.base import BaseObjectView
from netbox_branching.models import ChangeDiff
from utilities.data import shallow_compare_dict
from utilities.forms import ConfirmationForm
from utilities.forms import restrict_form_fields
from utilities.paginator import EnhancedPaginator
from utilities.paginator import get_paginate_count
from utilities.query import count_related
from utilities.views import get_viewname
from utilities.views import GetRelatedModelsMixin
from utilities.views import register_model_view
from utilities.views import ViewTab

from .filtersets import ForwardDataFilterSet
from .filtersets import ForwardIngestionChangeFilterSet
from .filtersets import ForwardIngestionFilterSet
from .filtersets import ForwardIngestionIssueFilterSet
from .filtersets import ForwardNQEQueryFilterSet
from .filtersets import ForwardSnapshotFilterSet
from .filtersets import ForwardSourceFilterSet
from .filtersets import ForwardSyncFilterSet
from .forms import ForwardIngestionFilterForm
from .forms import ForwardIngestionMergeForm
from .forms import ForwardNQEQueryForm
from .forms import ForwardSnapshotFilterForm
from .forms import ForwardSourceFilterForm
from .forms import ForwardSourceForm
from .forms import ForwardSyncForm
from .forms import ForwardTableForm
from .models import ForwardData
from .models import ForwardIngestion
from .models import ForwardIngestionIssue
from .models import ForwardNQEQuery
from .models import ForwardSnapshot
from .models import ForwardSource
from .models import ForwardSync
from .tables import DeviceFWDTable
from .tables import ForwardDataTable
from .tables import ForwardIngestionChangesTable
from .tables import ForwardIngestionIssuesTable
from .tables import ForwardIngestionTable
from .tables import ForwardNQEQueryTable
from .tables import ForwardSnapshotTable
from .tables import ForwardSourceTable
from .tables import ForwardSyncTable
from .utilities.fwdutils import Forward
from .utilities.nqe_map import restore_default_nqe_map


# NQE Maps


class ForwardNQEQueryListView(generic.ObjectListView):
    queryset = ForwardNQEQuery.objects.select_related("content_type")
    table = ForwardNQEQueryTable
    filterset = ForwardNQEQueryFilterSet
    template_name = "forward_netbox/forwardnqequery_list.html"


@register_model_view(ForwardNQEQuery)
class ForwardNQEQueryView(generic.ObjectView):
    queryset = ForwardNQEQuery.objects.select_related("content_type")


@register_model_view(ForwardNQEQuery, "edit")
class ForwardNQEQueryEditView(generic.ObjectEditView):
    queryset = ForwardNQEQuery.objects.select_related("content_type")
    form = ForwardNQEQueryForm


class ForwardNQEQueryDeleteView(generic.ObjectDeleteView):
    queryset = ForwardNQEQuery.objects.all()
    default_return_url = "plugins:forward_netbox:forwardnqequery_list"


class ForwardNQEQueryBulkDeleteView(generic.BulkDeleteView):
    queryset = ForwardNQEQuery.objects.all()
    filterset = ForwardNQEQueryFilterSet
    table = ForwardNQEQueryTable


class ForwardNQEQueryRestoreView(PermissionRequiredMixin, View):
    template_name = "forward_netbox/forwardnqequery_restore.html"
    permission_required = "forward_netbox.add_forwardnqequery"
    raise_exception = True

    def get(self, request):
        form = ConfirmationForm(initial=request.GET)
        dependent_summary = [
            {
                "label": "Forward NQE Queries",
                "count": ForwardNQEQuery.objects.count(),
            },
            {
                "label": "Forward Sync overrides",
                "count": ForwardSync.objects.filter(
                    parameters__has_key="nqe_map"
                ).count(),
            },
        ]
        dependent_summary = [item for item in dependent_summary if item["count"]]
        return render(
            request,
            self.template_name,
            {
                "form": form,
                "dependent_summary": dependent_summary,
            },
        )

    def post(self, request):
        restore_default_nqe_map()
        messages.success(request, "Forward NQE map restored to defaults.")
        response = redirect("plugins:forward_netbox:forwardnqequery_list")
        if request.htmx:
            response.status_code = 204
            response["HX-Redirect"] = response.url
        return response


# Snapshot


class ForwardSnapshotListView(generic.ObjectListView):
    queryset = ForwardSnapshot.objects.all()
    table = ForwardSnapshotTable
    filterset = ForwardSnapshotFilterSet
    filterset_form = ForwardSnapshotFilterForm


@register_model_view(ForwardSnapshot)
class ForwardSnapshotView(generic.ObjectView):
    queryset = ForwardSnapshot.objects.all()


class ForwardSnapshotDeleteView(generic.ObjectDeleteView):
    queryset = ForwardSnapshot.objects.all()


class ForwardSnapshotBulkDeleteView(generic.BulkDeleteView):
    queryset = ForwardSnapshot.objects.all()
    filterset = ForwardSnapshotFilterSet
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


class ForwardSnapshotDataDeleteView(generic.ObjectDeleteView):
    queryset = ForwardData.objects.all()


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
class ForwardSnapshotDataJSONView(generic.ObjectView):
    queryset = ForwardData.objects.all()
    template_name = "forward_netbox/inc/json.html"

    def get(self, request, **kwargs):
        data = get_object_or_404(ForwardData, pk=kwargs.get("pk"))
        if request.htmx:
            return render(
                request,
                self.template_name,
                {
                    "object": data,
                },
            )
        return render(
            request,
            self.template_name,
            {
                "object": data,
            },
        )


# Source


class ForwardSourceListView(generic.ObjectListView):
    queryset = ForwardSource.objects.annotate(
        snapshot_count=count_related(ForwardSnapshot, "source")
    )
    filterset = ForwardSourceFilterSet
    filterset_form = ForwardSourceFilterForm
    table = ForwardSourceTable


@register_model_view(ForwardSource, "edit")
class ForwardSourceEditView(generic.ObjectEditView):
    queryset = ForwardSource.objects.all()
    form = ForwardSourceForm


@register_model_view(ForwardSource)
class ForwardSourceView(GetRelatedModelsMixin, generic.ObjectView):
    queryset = ForwardSource.objects.all()

    def get_extra_context(self, request, instance):
        job = instance.jobs.order_by("id").last()
        data = {
            "related_models": self.get_related_models(request, instance),
            "job": job,
        }
        if job:
            data["job_results"] = job.data
        return data


@register_model_view(ForwardSource, "sync")
class ForwardSourceSyncView(BaseObjectView):
    queryset = ForwardSource.objects.all()

    def get_required_permission(self):
        return "forward_netbox.sync_forwardsource"

    def get(self, request, pk):
        forwardsource = get_object_or_404(self.queryset, pk=pk)
        return redirect(forwardsource.get_absolute_url())

    def post(self, request, pk):
        forwardsource = get_object_or_404(self.queryset, pk=pk)
        job = forwardsource.enqueue_sync_job(request=request)

        messages.success(request, f"Queued job #{job.pk} to sync {forwardsource}")
        return redirect(forwardsource.get_absolute_url())


@register_model_view(ForwardSource, "delete")
class ForwardSourceDeleteView(generic.ObjectDeleteView):
    queryset = ForwardSource.objects.all()


class ForwardSourceBulkDeleteView(generic.BulkDeleteView):
    queryset = ForwardSource.objects.all()
    filterset = ForwardSourceFilterSet
    table = ForwardSourceTable


# Sync
class ForwardSyncListView(generic.ObjectListView):
    queryset = ForwardSync.objects.all()
    table = ForwardSyncTable
    filterset = ForwardSyncFilterSet


@register_model_view(ForwardSync, "edit")
class ForwardSyncEditView(generic.ObjectEditView):
    queryset = ForwardSync.objects.all()
    form = ForwardSyncForm

    def alter_object(self, obj, request, url_args, url_kwargs):
        obj.user = request.user
        return obj


@register_model_view(ForwardSync)
class ForwardSyncView(generic.ObjectView):
    queryset = ForwardSync.objects.all()

    def get(self, request, **kwargs):
        # Handle HTMX requests separately
        if request.htmx:
            instance = self.get_object(**kwargs)
            last_ingestion = instance.forwardingestion_set.last()

            response = render(
                request,
                "forward_netbox/partials/sync_last_ingestion.html",
                {"last_ingestion": last_ingestion},
            )

            if instance.status not in ["queued", "syncing"]:
                messages.success(
                    request,
                    f"Ingestion ({instance.name}) {instance.status}. Ingestion {last_ingestion.name} {last_ingestion.job.status}.",
                )
                response["HX-Refresh"] = "true"
            return response

        # For regular requests, use the parent method which includes actions
        return super().get(request, **kwargs)

    def get_extra_context(self, request, instance):
        if request.GET.get("format") in ["json", "yaml"]:
            format = request.GET.get("format")
            if request.user.is_authenticated:
                request.user.config.set("data_format", format, commit=True)
        elif request.user.is_authenticated:
            format = request.user.config.get("data_format", "json")
        else:
            format = "json"

        last_ingestion = instance.forwardingestion_set.last()

        return {"format": format, "last_ingestion": last_ingestion}


@register_model_view(ForwardSync, "sync")
class ForwardStartSyncView(BaseObjectView):
    queryset = ForwardSync.objects.all()

    def get_required_permission(self):
        return "forward_netbox.start_forwardsync"

    def get(self, request, pk):
        forward = get_object_or_404(self.queryset, pk=pk)
        return redirect(forward.get_absolute_url())

    def post(self, request, pk):
        forward = get_object_or_404(self.queryset, pk=pk)
        job = forward.enqueue_sync_job(user=request.user, adhoc=True)

        messages.success(request, f"Queued job #{job.pk} to sync {forward}")
        return redirect(forward.get_absolute_url())


@register_model_view(ForwardSync, "delete")
class ForwardSyncDeleteView(generic.ObjectDeleteView):
    queryset = ForwardSync.objects.all()
    default_return_url = "plugins:forward_netbox:forwardsync_list"


class ForwardSyncBulkDeleteView(generic.BulkDeleteView):
    queryset = ForwardSync.objects.all()
    filterset = ForwardSnapshotFilterSet
    table = ForwardSyncTable


@register_model_view(ForwardSync, "ingestion")
class ForwardIngestionTabView(generic.ObjectChildrenView):
    queryset = ForwardSync.objects.all()
    child_model = ForwardIngestion
    table = ForwardIngestionTable
    filterset = ForwardIngestionFilterSet
    tab = ViewTab(
        label="Ingestions",
        badge=lambda obj: ForwardIngestion.objects.filter(sync=obj).count(),
        permission="forward_netbox.view_forwardingestion",
    )

    def get_children(self, request, parent):
        return self.child_model.objects.filter(sync=parent).annotate(
            description=models.F("branch__description"),
            user=models.F("sync__user__username"),
            staged_changes=models.Count(models.F("branch__changediff")),
        )


# Ingestion
class ForwardIngestionListView(generic.ObjectListView):
    queryset = ForwardIngestion.objects.annotate(
        description=models.F("branch__description"),
        user=models.F("sync__user__username"),
        staged_changes=models.Count(models.F("branch__changediff")),
    )
    filterset = ForwardIngestionFilterSet
    filterset_form = ForwardIngestionFilterForm
    table = ForwardIngestionTable


def annotate_statistics(queryset):
    return queryset.annotate(
        num_created=models.Count(
            "branch__changediff",
            filter=models.Q(
                branch__changediff__action=ObjectChangeActionChoices.ACTION_CREATE
            )
            & ~models.Q(branch__changediff__object_type__model="objectchange"),
        ),
        num_updated=models.Count(
            "branch__changediff",
            filter=models.Q(
                branch__changediff__action=ObjectChangeActionChoices.ACTION_UPDATE
            )
            & ~models.Q(branch__changediff__object_type__model="objectchange"),
        ),
        num_deleted=models.Count(
            "branch__changediff",
            filter=models.Q(
                branch__changediff__action=ObjectChangeActionChoices.ACTION_DELETE
            )
            & ~models.Q(branch__changediff__object_type__model="objectchange"),
        ),
        description=models.F("branch__description"),
        user=models.F("sync__user__username"),
        staged_changes=models.Count(models.F("branch__changediff")),
    )


@register_model_view(
    ForwardIngestion,
    name="logs",
    path="logs",
)
class ForwardIngestionLogView(LoginRequiredMixin, View):
    template_name = "forward_netbox/partials/ingestion_all.html"

    def get(self, request, **kwargs):
        ingestion_id = kwargs.get("pk")
        ingestion = annotate_statistics(ForwardIngestion.objects).get(pk=ingestion_id)
        data = ingestion.get_statistics()
        data["object"] = ingestion
        data["job"] = ingestion.jobs.first()

        if request.htmx:
            response = render(
                request,
                self.template_name,
                data,
            )
            if ingestion.job.completed:
                response["HX-Refresh"] = "true"
            return response
        return render(request, self.template_name, data)


@register_model_view(ForwardIngestion)
class ForwardIngestionView(generic.ObjectView):
    queryset = annotate_statistics(ForwardIngestion.objects)

    def get_extra_context(self, request, instance):
        data = instance.get_statistics()
        return data


@register_model_view(ForwardIngestion, "merge")
class ForwardIngestionMergeView(BaseObjectView):
    queryset = ForwardIngestion.objects.annotate(
        description=models.F("branch__description"),
        user=models.F("sync__user__username"),
        staged_changes=models.Count(models.F("branch__changediff")),
    )
    template_name = "forward_netbox/inc/merge_form.html"
    form = ForwardIngestionMergeForm

    def get_required_permission(self):
        return "forward_netbox.merge_forwardingestion"

    def get(self, request, pk):
        obj = get_object_or_404(self.queryset, pk=pk)

        if request.htmx:
            viewname = get_viewname(self.queryset.model, action="merge")
            form_url = reverse(viewname, kwargs={"pk": obj.pk})
            form = self.form(initial=request.GET)
            restrict_form_fields(form, request.user)
            return render(
                request,
                "forward_netbox/inc/merge_form.html",
                {
                    "object": obj,
                    "object_type": self.queryset.model._meta.verbose_name,
                    "form": form,
                    "form_url": form_url,
                    **self.get_extra_context(request, obj),
                },
            )

        return redirect(obj.get_absolute_url())

    def post(self, request, pk):
        ingestion = get_object_or_404(self.queryset, pk=pk)
        form = self.form(request.POST)
        restrict_form_fields(form, request.user)
        if form.is_valid():
            job = ingestion.enqueue_merge_job(
                user=request.user, remove_branch=form.cleaned_data["remove_branch"]
            )
            messages.success(request, f"Queued job #{job.pk} to sync {ingestion}")
            return redirect(ingestion.get_absolute_url())

        # Handle invalid form - add form errors to messages and redirect back
        for field, errors in form.errors.items():
            for error in errors:
                messages.error(request, f"{field}: {error}")
        if form.non_field_errors():
            for error in form.non_field_errors():
                messages.error(request, error)

        return redirect(ingestion.get_absolute_url())


@register_model_view(
    ForwardIngestion,
    name="change_diff",
    path="change/<int:change_pk>",
    kwargs={"model": ForwardIngestion},
)
class ForwardIngestionChangesDiffView(LoginRequiredMixin, View):
    template_name = "forward_netbox/inc/diff.html"

    def get(self, request, **kwargs):
        change_id = kwargs.get("change_pk", None)

        if not request.htmx or not change_id:
            return render(
                request,
                self.template_name,
                {
                    "change": None,
                    "prechange_data": None,
                    "postchange_data": None,
                    "diff_added": None,
                    "diff_removed": None,
                    "size": "lg",
                },
            )

        change = ChangeDiff.objects.get(pk=change_id)
        if change.original and change.modified:
            diff_added = shallow_compare_dict(
                change.original or dict(),
                change.modified or dict(),
                exclude=["last_updated"],
            )
            diff_removed = (
                {x: change.original.get(x) for x in diff_added}
                if change.modified
                else {}
            )
        else:
            diff_added = None
            diff_removed = None

        return render(
            request,
            self.template_name,
            {
                "change": change,
                "prechange_data": change.original,
                "postchange_data": change.modified,
                "diff_added": diff_added,
                "diff_removed": diff_removed,
                "size": "lg",
            },
        )


@register_model_view(ForwardIngestion, "change")
class ForwardIngestionChangesView(generic.ObjectChildrenView):
    queryset = ForwardIngestion.objects.all()
    child_model = ChangeDiff
    table = ForwardIngestionChangesTable
    filterset = ForwardIngestionChangeFilterSet
    template_name = "generic/object_children.html"
    tab = ViewTab(
        label="Changes",
        badge=lambda obj: ChangeDiff.objects.filter(branch=obj.branch).count(),
        permission="forward_netbox.view_forwardingestion",
    )

    def get_children(self, request, parent):
        return self.child_model.objects.filter(branch=parent.branch)


@register_model_view(ForwardIngestion, "ingestion_issues")
class ForwardIngestionIssuesView(generic.ObjectChildrenView):
    queryset = ForwardIngestion.objects.all()
    child_model = ForwardIngestionIssue
    table = ForwardIngestionIssuesTable
    template_name = "generic/object_children.html"
    filterset = ForwardIngestionIssueFilterSet
    tab = ViewTab(
        label="Ingestion Issues",
        badge=lambda obj: ForwardIngestionIssue.objects.filter(ingestion=obj).count(),
        permission="forward_netbox.view_forwardingestionissue",
    )

    def get_children(self, request, parent):
        return ForwardIngestionIssue.objects.filter(ingestion=parent)


@register_model_view(ForwardIngestion, "delete")
class ForwardIngestionDeleteView(generic.ObjectDeleteView):
    queryset = ForwardIngestion.objects.all()


@register_model_view(Device, "forward")
class ForwardTable(generic.ObjectView):
    template_name = "forward_netbox/forward_table.html"
    tab = ViewTab("Forward Networks", permission="forward_netbox.view_devicetable")
    queryset = Device.objects.all()

    def get_extra_context(self, request, instance):
        """Process form and prepare table data for the template."""
        device = instance
        form = (
            ForwardTableForm(request.GET)
            if "table" in request.GET
            else ForwardTableForm()
        )
        restrict_form_fields(form, request.user)
        data = None
        source = None

        if form.is_valid():
            table_name = form.cleaned_data["table"]
            test = {
                "True": True,
                "False": False,
            }
            cache_enable = test.get(form.cleaned_data["cache_enable"])
            source = form.cleaned_data.get("source")

            if not form.cleaned_data["snapshot_data"]:
                snapshot_id = "$last"
                source = (
                    source
                    or ForwardSource.objects.filter(
                        pk=device.custom_field_data.get("forward_source")
                    ).first()
                    or ForwardSource.get_for_site(device.site).first()
                )
            else:
                snapshot_id = form.cleaned_data["snapshot_data"].snapshot_id
                source = source or form.cleaned_data["snapshot_data"].source

            if source is not None:
                params = dict(source.parameters or {})
                params["snapshot_id"] = snapshot_id
                params["base_url"] = source.url
                if source.network_id:
                    params["network_id"] = source.network_id

                network_component = params.get("network_id") or "default"
                cache_key = (
                    f"forward_{table_name}_{device.serial}_{snapshot_id}_{network_component}"
                )
                if cache_enable:
                    data = cache.get(cache_key)

                if not data:
                    try:
                        fwd = Forward(parameters=params)
                        raw_data, columns = fwd.get_table_data(
                            table=table_name, device=device
                        )
                        data = {"data": raw_data, "columns": columns}
                        cache.set(cache_key, data, 60 * 60 * 24)
                    except Exception as e:
                        messages.error(request, e)
                    finally:
                        if "fwd" in locals():
                            fwd.close()

        if not data:
            data = {"data": [], "columns": []}

        table = DeviceFWDTable(data["data"], extra_columns=data["columns"])

        RequestConfig(
            request,
            {
                "paginator_class": EnhancedPaginator,
                "per_page": get_paginate_count(request),
            },
        ).configure(table)

        if not source:
            if source_id := device.custom_field_data.get("forward_source"):
                source = ForwardSource.objects.filter(pk=source_id).first()
            else:
                source = ForwardSource.get_for_site(device.site).first()

        return {
            "source": source,
            "form": form,
            "table": table,
        }

    def get(self, request, **kwargs):
        """Handle GET requests, with special handling for HTMX table updates."""
        # For HTMX requests, we only need to return the table HTML
        if request.htmx:
            device = get_object_or_404(Device, pk=kwargs.get("pk"))
            context = self.get_extra_context(request, device)
            return render(
                request,
                "htmx/table.html",
                {
                    "table": context["table"],
                },
            )

        # For regular requests, use the parent's get() method which will call get_extra_context()
        return super().get(request, **kwargs)


@register_model_view(
    ForwardSource,
    name="topology",
    path="topology/<int:site>",
    kwargs={"snapshot": ""},
)
class ForwardSourceTopology(LoginRequiredMixin, View):
    template_name = "forward_netbox/inc/site_topology_modal.html"

    def get(self, request, pk, site, **kwargs):
        if request.htmx:
            site_obj = get_object_or_404(Site, pk=site)
            source_id = request.GET.get("source")
            source = (
                get_object_or_404(ForwardSource, pk=source_id)
                if source_id
                else None
            )

            try:
                if not source:
                    raise Exception("Source ID not available in request.")
                snapshot = request.GET.get("snapshot")
                if not snapshot:
                    raise Exception("Snapshot ID not available in request.")

                params = dict(source.parameters or {})
                params.update({"snapshot_id": snapshot, "base_url": source.url})
                if source.network_id:
                    params["network_id"] = source.network_id

                fwd_client = Forward(parameters=params)
                snapshot_data = fwd_client.api.get_snapshot(snapshot)
                if not snapshot_data:
                    raise Exception(
                        f"Snapshot ({snapshot}) not available in Forward Networks."  # noqa E713
                    )

                sites = fwd_client.api.inventory(
                    "sites",
                    snapshot_id=snapshot,
                    filters={"siteName": ["eq", site.name]},
                )
                if not sites:
                    raise Exception(
                        f"{site.name} not available in snapshot ({snapshot}) for Forward Networks."  # noqa E713
                    )

                diagram_settings = {
                    "hiddenProtocols": ["xdp"],
                    "hiddenDeviceTypes": ["transit", "cloud"],
                }
                topology = fwd_client.api.get_site_topology(
                    site.name, snapshot, settings=diagram_settings
                )
                link = topology.get("share_link") or topology.get("link")
                svg_content = topology.get("svg")
                svg_data = svg_content if isinstance(svg_content, str) else None
                error = None
            except Exception as e:
                error = e
                svg_data = link = snapshot_data = source = None
            finally:
                if "fwd_client" in locals():
                    fwd_client.close()

            return render(
                request,
                self.template_name,
                {
                    "site": site_obj,
                    "source": source,
                    "svg": svg_data,
                    "size": "xl",
                    "link": link,
                    "time": timezone.now(),
                    "snapshot": snapshot_data,
                    "error": error,
                },
            )
        return render(
            request,
            self.template_name,
            {
                "site": get_object_or_404(Site, pk=site),
                "size": "xl",
                "time": timezone.now(),
            },
        )
