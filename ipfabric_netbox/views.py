from core.choices import ObjectChangeActionChoices
from dcim.models import Device
from dcim.models import Site
from django.contrib import messages
from django.contrib.auth.mixins import LoginRequiredMixin
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
from ipfabric.diagrams import Network
from ipfabric.diagrams import NetworkSettings
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

from .filtersets import IPFabricDataFilterSet
from .filtersets import IPFabricIngestionChangeFilterSet
from .filtersets import IPFabricIngestionFilterSet
from .filtersets import IPFabricIngestionIssueFilterSet
from .filtersets import IPFabricSnapshotFilterSet
from .filtersets import IPFabricSourceFilterSet
from .filtersets import IPFabricSyncFilterSet
from .filtersets import IPFabricTransformMapFilterSet
from .filtersets import IPFabricTransformMapGroupFilterSet
from .forms import IPFabricIngestionFilterForm
from .forms import IPFabricIngestionMergeForm
from .forms import IPFabricRelationshipFieldForm
from .forms import IPFabricSnapshotFilterForm
from .forms import IPFabricSourceFilterForm
from .forms import IPFabricSourceForm
from .forms import IPFabricSyncForm
from .forms import IPFabricTableForm
from .forms import IPFabricTransformFieldForm
from .forms import IPFabricTransformMapCloneForm
from .forms import IPFabricTransformMapForm
from .forms import IPFabricTransformMapGroupForm
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
from .tables import DeviceIPFTable
from .tables import IPFabricDataTable
from .tables import IPFabricIngestionChangesTable
from .tables import IPFabricIngestionIssuesTable
from .tables import IPFabricIngestionTable
from .tables import IPFabricRelationshipFieldTable
from .tables import IPFabricSnapshotTable
from .tables import IPFabricSourceTable
from .tables import IPFabricSyncTable
from .tables import IPFabricTransformFieldTable
from .tables import IPFabricTransformMapGroupTable
from .tables import IPFabricTransformMapTable
from .utilities.ipfutils import IPFabric
from .utilities.transform_map import build_transform_maps
from .utilities.transform_map import get_transform_map


# Transform Map Relationship Field


@register_model_view(IPFabricRelationshipField, "edit")
class IPFabricRelationshipFieldEditView(generic.ObjectEditView):
    queryset = IPFabricRelationshipField.objects.all()
    form = IPFabricRelationshipFieldForm
    default_return_url = "plugins:ipfabric_netbox:ipfabricrelationshipfield_list"


class IPFabricRelationshipFieldDeleteView(generic.ObjectDeleteView):
    queryset = IPFabricRelationshipField.objects.all()
    default_return_url = "plugins:ipfabric_netbox:ipfabricrelationshipfield_list"


@register_model_view(IPFabricTransformMap, "relationships")
class IPFabricTransformRelationshipView(generic.ObjectChildrenView):
    queryset = IPFabricTransformMap.objects.all()
    child_model = IPFabricRelationshipField
    table = IPFabricRelationshipFieldTable
    template_name = "ipfabric_netbox/inc/transform_map_relationship_map.html"
    tab = ViewTab(
        label="Relationship Maps",
        badge=lambda obj: IPFabricRelationshipField.objects.filter(
            transform_map=obj
        ).count(),
        permission="ipfabric_netbox.view_ipfabricrelationshipfield",
    )

    def get_children(self, request, parent):
        return self.child_model.objects.filter(transform_map=parent)


class IPFabricRelationshipFieldListView(generic.ObjectListView):
    queryset = IPFabricRelationshipField.objects.all()
    table = IPFabricRelationshipFieldTable


# Transform Map Group


class IPFabricTransformMapGroupListView(generic.ObjectListView):
    queryset = IPFabricTransformMapGroup.objects.annotate(
        maps_count=models.Count("transform_maps")
    )
    table = IPFabricTransformMapGroupTable
    filterset = IPFabricTransformMapGroupFilterSet


@register_model_view(IPFabricTransformMapGroup, "edit")
class IPFabricTransformMapGroupEditView(generic.ObjectEditView):
    queryset = IPFabricTransformMapGroup.objects.all()
    form = IPFabricTransformMapGroupForm
    default_return_url = "plugins:ipfabric_netbox:ipfabrictransformmapgroup_list"


class IPFabricTransformMapGroupDeleteView(generic.ObjectDeleteView):
    queryset = IPFabricTransformMapGroup.objects.all()
    default_return_url = "plugins:ipfabric_netbox:ipfabrictransformmapgroup_list"


class IPFabricTransformMapGroupBulkDeleteView(generic.BulkDeleteView):
    queryset = IPFabricTransformMapGroup.objects.all()
    table = IPFabricTransformMapGroupTable


@register_model_view(IPFabricTransformMapGroup)
class IPFabricTransformMapGroupView(GetRelatedModelsMixin, generic.ObjectView):
    queryset = IPFabricTransformMapGroup.objects.all()

    def get_extra_context(self, request, instance):
        return {
            "related_models": self.get_related_models(request, instance, omit=[]),
        }


# Transform Map


class IPFabricTransformMapListView(generic.ObjectListView):
    queryset = IPFabricTransformMap.objects.all()
    table = IPFabricTransformMapTable
    template_name = "ipfabric_netbox/ipfabrictransformmap_list.html"
    filterset = IPFabricTransformMapFilterSet


class IPFabricTransformMapRestoreView(generic.ObjectListView):
    queryset = IPFabricTransformMap.objects.all()
    table = IPFabricTransformMapTable

    def get_required_permission(self):
        return "ipfabric_netbox.restore_ipfabrictransformmap"

    def get(self, request):
        if request.htmx:
            viewname = get_viewname(self.queryset.model, action="restore")
            form_url = reverse(viewname)
            form = ConfirmationForm(initial=request.GET)
            dependent_objects = {
                IPFabricTransformMap: IPFabricTransformMap.objects.filter(
                    group__isnull=True
                ),
                IPFabricTransformField: IPFabricTransformField.objects.filter(
                    transform_map__group__isnull=True
                ),
                IPFabricRelationshipField: IPFabricRelationshipField.objects.filter(
                    transform_map__group__isnull=True
                ),
            }
            return render(
                request,
                "ipfabric_netbox/ipfabrictransformmap_restore.html",
                {
                    "form": form,
                    "form_url": form_url,
                    "dependent_objects": dependent_objects,
                },
            )
        return redirect(reverse("plugins:ipfabric_netbox:ipfabrictransformmap_list"))

    def post(self, request):
        IPFabricTransformMap.objects.filter(group__isnull=True).delete()
        build_transform_maps(data=get_transform_map())
        return redirect("plugins:ipfabric_netbox:ipfabrictransformmap_list")


@register_model_view(IPFabricTransformMap, "edit")
class IPFabricTransformMapEditView(generic.ObjectEditView):
    queryset = IPFabricTransformMap.objects.all()
    form = IPFabricTransformMapForm
    default_return_url = "plugins:ipfabric_netbox:ipfabrictransformmap_list"


@register_model_view(IPFabricTransformMap, "clone")
class IPFabricTransformMapCloneView(BaseObjectView):
    queryset = IPFabricTransformMap.objects.all()
    template_name = "ipfabric_netbox/inc/clone_form.html"
    form = IPFabricTransformMapCloneForm

    def get_required_permission(self):
        return "ipfabric_netbox.clone_ipfabrictransformmap"

    def get(self, request, pk):
        obj = get_object_or_404(self.queryset, pk=pk)
        if request.htmx:
            viewname = get_viewname(self.queryset.model, action="clone")
            form_url = reverse(viewname, kwargs={"pk": obj.pk})
            initial = request.GET.copy()
            initial["name"] = f"Clone of {obj.name}"
            form = self.form(initial=initial)
            restrict_form_fields(form, request.user)
            return render(
                request,
                self.template_name,
                {
                    "object": obj,
                    "form": form,
                    "pk": pk,
                    "form_url": form_url,
                },
            )
        return redirect(obj.get_absolute_url())

    def post(self, request, pk):
        obj = get_object_or_404(self.queryset, pk=pk)
        form = self.form(request.POST)
        restrict_form_fields(form, request.user)
        try:
            if form.is_valid():
                with transaction.atomic():
                    fields = IPFabricTransformField.objects.filter(transform_map=obj)
                    relationships = IPFabricRelationshipField.objects.filter(
                        transform_map=obj
                    )
                    # Clone the transform map - create a proper copy using Django model copying
                    new_map = IPFabricTransformMap(
                        name=form.cleaned_data["name"],
                        source_model=obj.source_model,
                        target_model=obj.target_model,
                        group=form.cleaned_data["group"],
                    )
                    new_map.full_clean()
                    new_map.save()

                    # Clone related transform fields
                    if form.cleaned_data["clone_fields"]:
                        for field in fields:
                            IPFabricTransformField.objects.create(
                                transform_map=new_map,
                                source_field=field.source_field,
                                target_field=field.target_field,
                                coalesce=field.coalesce,
                                template=field.template,
                            )

                    # Clone related relationship fields
                    if form.cleaned_data["clone_relationships"]:
                        for rel in relationships:
                            IPFabricRelationshipField.objects.create(
                                transform_map=new_map,
                                source_model=rel.source_model,
                                target_field=rel.target_field,
                                coalesce=rel.coalesce,
                                template=rel.template,
                            )

                return_url = reverse(
                    "plugins:ipfabric_netbox:ipfabrictransformmap", args=[new_map.pk]
                )
                if request.htmx:
                    response = HttpResponse()
                    response["HX-Redirect"] = return_url
                    return response
                return redirect(return_url)
        except ValidationError as err:
            if not hasattr(err, "error_dict") or not err.error_dict:
                form.add_error(None, err)
            else:
                # This serves to show errors in the form directly
                for field, error in err.error_dict.items():
                    if field in form.fields:
                        form.add_error(field, error)
                    else:
                        form.add_error(None, error)
        if request.htmx:
            viewname = get_viewname(self.queryset.model, action="clone")
            form_url = reverse(viewname, kwargs={"pk": obj.pk})
            response = render(
                request,
                "ipfabric_netbox/inc/clone_form.html",
                {
                    "form": form,
                    "object": obj,
                    "pk": pk,
                    "form_url": form_url,
                },
            )
            response["X-Debug-HTMX-Partial"] = "true"
            return response
        return render(
            request,
            self.template_name,
            {
                "form": form,
                "object": obj,
                "pk": pk,
            },
        )


class IPFabricTransformMapDeleteView(generic.ObjectDeleteView):
    queryset = IPFabricTransformMap.objects.all()
    default_return_url = "plugins:ipfabric_netbox:ipfabrictransformmap_list"


class IPFabricTransformMapBulkDeleteView(generic.BulkDeleteView):
    queryset = IPFabricTransformMap.objects.all()
    table = IPFabricTransformMapTable


@register_model_view(IPFabricTransformMap)
class IPFabricTransformMapView(generic.ObjectView):
    queryset = IPFabricTransformMap.objects.all()


# Transform Map Field


class IPFabricTransformFieldListView(generic.ObjectListView):
    queryset = IPFabricTransformField.objects.all()
    table = IPFabricTransformFieldTable


@register_model_view(IPFabricTransformField, "edit")
class IPFabricTransformFieldEditView(generic.ObjectEditView):
    queryset = IPFabricTransformField.objects.all()
    form = IPFabricTransformFieldForm


class IPFabricTransformFieldDeleteView(generic.ObjectDeleteView):
    queryset = IPFabricTransformField.objects.all()


@register_model_view(IPFabricTransformMap, "fields")
class IPFabricTransformFieldView(generic.ObjectChildrenView):
    queryset = IPFabricTransformMap.objects.all()
    child_model = IPFabricTransformField
    table = IPFabricTransformFieldTable
    template_name = "ipfabric_netbox/inc/transform_map_field_map.html"
    tab = ViewTab(
        label="Field Maps",
        badge=lambda obj: IPFabricTransformField.objects.filter(
            transform_map=obj
        ).count(),
        permission="ipfabric_netbox.view_ipfabrictransformfield",
    )

    def get_children(self, request, parent):
        return self.child_model.objects.filter(transform_map=parent)


# Snapshot


class IPFabricSnapshotListView(generic.ObjectListView):
    queryset = IPFabricSnapshot.objects.all()
    table = IPFabricSnapshotTable
    filterset = IPFabricSnapshotFilterSet
    filterset_form = IPFabricSnapshotFilterForm


@register_model_view(IPFabricSnapshot)
class IPFabricSnapshotView(generic.ObjectView):
    queryset = IPFabricSnapshot.objects.all()


class IPFabricSnapshotDeleteView(generic.ObjectDeleteView):
    queryset = IPFabricSnapshot.objects.all()


class IPFabricSnapshotBulkDeleteView(generic.BulkDeleteView):
    queryset = IPFabricSnapshot.objects.all()
    filterset = IPFabricSnapshotFilterSet
    table = IPFabricSnapshotTable


@register_model_view(IPFabricSnapshot, "data")
class IPFabricSnapshotRawView(generic.ObjectChildrenView):
    queryset = IPFabricSnapshot.objects.all()
    child_model = IPFabricData
    table = IPFabricDataTable
    template_name = "ipfabric_netbox/inc/snapshotdata.html"
    tab = ViewTab(
        label="Raw Data",
        badge=lambda obj: IPFabricData.objects.filter(snapshot_data=obj).count(),
        permission="ipfabric_netbox.view_ipfabricsnapshot",
        hide_if_empty=True,
    )

    def get_children(self, request, parent):
        return self.child_model.objects.filter(snapshot_data=parent)


class IPFabricSnapshotDataDeleteView(generic.ObjectDeleteView):
    queryset = IPFabricData.objects.all()


class IPFabricSnapshotDataBulkDeleteView(generic.BulkDeleteView):
    queryset = IPFabricData.objects.all()
    filterset = IPFabricDataFilterSet
    table = IPFabricDataTable


@register_model_view(
    IPFabricData,
    name="data",
    path="json",
    kwargs={},
)
class IPFabricSnapshotDataJSONView(generic.ObjectView):
    queryset = IPFabricData.objects.all()
    template_name = "ipfabric_netbox/inc/json.html"

    def get(self, request, **kwargs):
        data = get_object_or_404(IPFabricData, pk=kwargs.get("pk"))
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


class IPFabricSourceListView(generic.ObjectListView):
    queryset = IPFabricSource.objects.annotate(
        snapshot_count=count_related(IPFabricSnapshot, "source")
    )
    filterset = IPFabricSourceFilterSet
    filterset_form = IPFabricSourceFilterForm
    table = IPFabricSourceTable


@register_model_view(IPFabricSource, "edit")
class IPFabricSourceEditView(generic.ObjectEditView):
    queryset = IPFabricSource.objects.all()
    form = IPFabricSourceForm


@register_model_view(IPFabricSource)
class IPFabricSourceView(GetRelatedModelsMixin, generic.ObjectView):
    queryset = IPFabricSource.objects.all()

    def get_extra_context(self, request, instance):
        job = instance.jobs.order_by("id").last()
        data = {
            "related_models": self.get_related_models(request, instance),
            "job": job,
        }
        if job:
            data["job_results"] = job.data
        return data


@register_model_view(IPFabricSource, "sync")
class IPFabricSourceSyncView(BaseObjectView):
    queryset = IPFabricSource.objects.all()

    def get_required_permission(self):
        return "ipfabric_netbox.sync_ipfabricsource"

    def get(self, request, pk):
        ipfabricsource = get_object_or_404(self.queryset, pk=pk)
        return redirect(ipfabricsource.get_absolute_url())

    def post(self, request, pk):
        ipfabricsource = get_object_or_404(self.queryset, pk=pk)
        job = ipfabricsource.enqueue_sync_job(request=request)

        messages.success(request, f"Queued job #{job.pk} to sync {ipfabricsource}")
        return redirect(ipfabricsource.get_absolute_url())


@register_model_view(IPFabricSource, "delete")
class IPFabricSourceDeleteView(generic.ObjectDeleteView):
    queryset = IPFabricSource.objects.all()


class IPFabricSourceBulkDeleteView(generic.BulkDeleteView):
    queryset = IPFabricSource.objects.all()
    filterset = IPFabricSourceFilterSet
    table = IPFabricSourceTable


# Sync
class IPFabricSyncListView(generic.ObjectListView):
    queryset = IPFabricSync.objects.all()
    table = IPFabricSyncTable
    filterset = IPFabricSyncFilterSet


@register_model_view(IPFabricSync, "edit")
class IPFabricSyncEditView(generic.ObjectEditView):
    queryset = IPFabricSync.objects.all()
    form = IPFabricSyncForm

    def alter_object(self, obj, request, url_args, url_kwargs):
        obj.user = request.user
        return obj


@register_model_view(IPFabricSync)
class IPFabricSyncView(generic.ObjectView):
    queryset = IPFabricSync.objects.all()

    def get(self, request, **kwargs):
        # Handle HTMX requests separately
        if request.htmx:
            instance = self.get_object(**kwargs)
            last_ingestion = instance.ipfabricingestion_set.last()

            response = render(
                request,
                "ipfabric_netbox/partials/sync_last_ingestion.html",
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

        last_ingestion = instance.ipfabricingestion_set.last()

        return {"format": format, "last_ingestion": last_ingestion}


@register_model_view(IPFabricSync, "sync")
class IPFabricStartSyncView(BaseObjectView):
    queryset = IPFabricSync.objects.all()

    def get_required_permission(self):
        return "ipfabric_netbox.start_ipfabricsync"

    def get(self, request, pk):
        ipfabric = get_object_or_404(self.queryset, pk=pk)
        return redirect(ipfabric.get_absolute_url())

    def post(self, request, pk):
        ipfabric = get_object_or_404(self.queryset, pk=pk)
        job = ipfabric.enqueue_sync_job(user=request.user, adhoc=True)

        messages.success(request, f"Queued job #{job.pk} to sync {ipfabric}")
        return redirect(ipfabric.get_absolute_url())


@register_model_view(IPFabricSync, "delete")
class IPFabricSyncDeleteView(generic.ObjectDeleteView):
    queryset = IPFabricSync.objects.all()
    default_return_url = "plugins:ipfabric_netbox:ipfabricsync_list"


class IPFabricSyncBulkDeleteView(generic.BulkDeleteView):
    queryset = IPFabricSync.objects.all()
    filterset = IPFabricSnapshotFilterSet
    table = IPFabricSyncTable


@register_model_view(IPFabricSync, "transformmaps")
class IPFabricTransformMapTabView(generic.ObjectChildrenView):
    queryset = IPFabricSync.objects.all()
    child_model = IPFabricTransformMap
    table = IPFabricTransformMapTable
    template_name = "generic/object_children.html"
    tab = ViewTab(
        label="Transform Maps",
        badge=lambda obj: obj.get_transform_maps(
            obj.parameters.get("groups", []) if obj.parameters else []
        ).count(),
        permission="ipfabric_netbox.view_ipfabrictransformmap",
    )

    def get_children(self, request, parent):
        return parent.get_transform_maps(
            parent.parameters.get("groups", []) if parent.parameters else []
        )


@register_model_view(IPFabricSync, "ingestion")
class IPFabricIngestionTabView(generic.ObjectChildrenView):
    queryset = IPFabricSync.objects.all()
    child_model = IPFabricIngestion
    table = IPFabricIngestionTable
    filterset = IPFabricIngestionFilterSet
    tab = ViewTab(
        label="Ingestions",
        badge=lambda obj: IPFabricIngestion.objects.filter(sync=obj).count(),
        permission="ipfabric_netbox.view_ipfabricingestion",
    )

    def get_children(self, request, parent):
        return self.child_model.objects.filter(sync=parent).annotate(
            description=models.F("branch__description"),
            user=models.F("sync__user__username"),
            staged_changes=models.Count(models.F("branch__changediff")),
        )


# Ingestion
class IPFabricIngestionListView(generic.ObjectListView):
    queryset = IPFabricIngestion.objects.annotate(
        description=models.F("branch__description"),
        user=models.F("sync__user__username"),
        staged_changes=models.Count(models.F("branch__changediff")),
    )
    filterset = IPFabricIngestionFilterSet
    filterset_form = IPFabricIngestionFilterForm
    table = IPFabricIngestionTable


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
    IPFabricIngestion,
    name="logs",
    path="logs",
)
class IPFabricIngestionLogView(LoginRequiredMixin, View):
    template_name = "ipfabric_netbox/partials/ingestion_all.html"

    def get(self, request, **kwargs):
        ingestion_id = kwargs.get("pk")
        ingestion = annotate_statistics(IPFabricIngestion.objects).get(pk=ingestion_id)
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


@register_model_view(IPFabricIngestion)
class IPFabricIngestionView(generic.ObjectView):
    queryset = annotate_statistics(IPFabricIngestion.objects)

    def get_extra_context(self, request, instance):
        data = instance.get_statistics()
        return data


@register_model_view(IPFabricIngestion, "merge")
class IPFabricIngestionMergeView(BaseObjectView):
    queryset = IPFabricIngestion.objects.annotate(
        description=models.F("branch__description"),
        user=models.F("sync__user__username"),
        staged_changes=models.Count(models.F("branch__changediff")),
    )
    template_name = "ipfabric_netbox/inc/merge_form.html"
    form = IPFabricIngestionMergeForm

    def get_required_permission(self):
        return "ipfabric_netbox.merge_ipfabricingestion"

    def get(self, request, pk):
        obj = get_object_or_404(self.queryset, pk=pk)

        if request.htmx:
            viewname = get_viewname(self.queryset.model, action="merge")
            form_url = reverse(viewname, kwargs={"pk": obj.pk})
            form = self.form(initial=request.GET)
            restrict_form_fields(form, request.user)
            return render(
                request,
                "ipfabric_netbox/inc/merge_form.html",
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
    IPFabricIngestion,
    name="change_diff",
    path="change/<int:change_pk>",
    kwargs={"model": IPFabricIngestion},
)
class IPFabricIngestionChangesDiffView(LoginRequiredMixin, View):
    template_name = "ipfabric_netbox/inc/diff.html"

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


@register_model_view(IPFabricIngestion, "change")
class IPFabricIngestionChangesView(generic.ObjectChildrenView):
    queryset = IPFabricIngestion.objects.all()
    child_model = ChangeDiff
    table = IPFabricIngestionChangesTable
    filterset = IPFabricIngestionChangeFilterSet
    template_name = "generic/object_children.html"
    tab = ViewTab(
        label="Changes",
        badge=lambda obj: ChangeDiff.objects.filter(branch=obj.branch).count(),
        permission="ipfabric_netbox.view_ipfabricingestion",
    )

    def get_children(self, request, parent):
        return self.child_model.objects.filter(branch=parent.branch)


@register_model_view(IPFabricIngestion, "ingestion_issues")
class IPFabricIngestionIssuesView(generic.ObjectChildrenView):
    queryset = IPFabricIngestion.objects.all()
    child_model = IPFabricIngestionIssue
    table = IPFabricIngestionIssuesTable
    template_name = "generic/object_children.html"
    filterset = IPFabricIngestionIssueFilterSet
    tab = ViewTab(
        label="Ingestion Issues",
        badge=lambda obj: IPFabricIngestionIssue.objects.filter(ingestion=obj).count(),
        permission="ipfabric_netbox.view_ipfabricingestionissue",
    )

    def get_children(self, request, parent):
        return IPFabricIngestionIssue.objects.filter(ingestion=parent)


@register_model_view(IPFabricIngestion, "delete")
class IPFabricIngestionDeleteView(generic.ObjectDeleteView):
    queryset = IPFabricIngestion.objects.all()


@register_model_view(Device, "ipfabric")
class IPFabricTable(generic.ObjectView):
    template_name = "ipfabric_netbox/ipfabric_table.html"
    tab = ViewTab("IP Fabric", permission="ipfabric_netbox.view_devicetable")
    queryset = Device.objects.all()

    def get_extra_context(self, request, instance):
        """Process form and prepare table data for the template."""
        device = instance
        form = (
            IPFabricTableForm(request.GET)
            if "table" in request.GET
            else IPFabricTableForm()
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
                    or IPFabricSource.objects.filter(
                        pk=device.custom_field_data.get("ipfabric_source")
                    ).first()
                    or IPFabricSource.get_for_site(device.site).first()
                )
            else:
                snapshot_id = form.cleaned_data["snapshot_data"].snapshot_id
                source = source or form.cleaned_data["snapshot_data"].source

            if source is not None:
                source.parameters["snapshot_id"] = snapshot_id
                source.parameters["base_url"] = source.url

                cache_key = f"ipfabric_{table_name}_{device.serial}_{source.parameters['snapshot_id']}"
                if cache_enable:
                    data = cache.get(cache_key)

                if not data:
                    try:
                        ipf = IPFabric(parameters=source.parameters)
                        raw_data, columns = ipf.get_table_data(
                            table=table_name, device=device
                        )
                        data = {"data": raw_data, "columns": columns}
                        cache.set(cache_key, data, 60 * 60 * 24)
                    except Exception as e:
                        messages.error(request, e)

        if not data:
            data = {"data": [], "columns": []}

        table = DeviceIPFTable(data["data"], extra_columns=data["columns"])

        RequestConfig(
            request,
            {
                "paginator_class": EnhancedPaginator,
                "per_page": get_paginate_count(request),
            },
        ).configure(table)

        if not source:
            if source_id := device.custom_field_data.get("ipfabric_source"):
                source = IPFabricSource.objects.filter(pk=source_id).first()
            else:
                source = IPFabricSource.get_for_site(device.site).first()

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
    IPFabricSource,
    name="topology",
    path="topology/<int:site>",
    kwargs={"snapshot": ""},
)
class IPFabricSourceTopology(LoginRequiredMixin, View):
    template_name = "ipfabric_netbox/inc/site_topology_modal.html"

    def get(self, request, pk, site, **kwargs):
        if request.htmx:
            try:
                site = get_object_or_404(Site, pk=site)
                source_id = request.GET.get("source")
                if not source_id:
                    raise Exception("Source ID not available in request.")
                source = get_object_or_404(IPFabricSource, pk=source_id)
                snapshot = request.GET.get("snapshot")
                if not snapshot:
                    raise Exception("Snapshot ID not available in request.")

                source.parameters.update(
                    {"snapshot_id": snapshot, "base_url": source.url}
                )

                ipf = IPFabric(parameters=source.parameters)
                snapshot_data = ipf.ipf.snapshots.get(snapshot)
                if not snapshot_data:
                    raise Exception(
                        f"Snapshot ({snapshot}) not available in IP Fabric."  # noqa E713
                    )

                sites = ipf.ipf.inventory.sites.all(
                    filters={"siteName": ["eq", site.name]}
                )
                if not sites:
                    raise Exception(
                        f"{site.name} not available in snapshot ({snapshot})."  # noqa E713
                    )

                net = Network(sites=site.name, all_network=False)
                settings = NetworkSettings()
                settings.hide_protocol("xdp")
                settings.hiddenDeviceTypes.extend(["transit", "cloud"])

                link = ipf.ipf.diagram.share_link(net, graph_settings=settings)
                svg_data = ipf.ipf.diagram.svg(net, graph_settings=settings).decode(
                    "utf-8"
                )
                error = None
            except Exception as e:
                error = e
                svg_data = link = snapshot_data = source = None

            return render(
                request,
                self.template_name,
                {
                    "site": site,
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
                "site": site,
                "size": "xl",
                "time": timezone.now(),
            },
        )
