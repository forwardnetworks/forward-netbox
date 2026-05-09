from core.choices import ObjectChangeActionChoices
from core.exceptions import SyncError
from core.models import ObjectChange
from django.contrib import messages
from django.contrib.auth.mixins import LoginRequiredMixin
from django.db import models
from django.shortcuts import get_object_or_404
from django.shortcuts import redirect
from django.shortcuts import render
from django.urls import reverse
from django.utils.translation import gettext as _
from django.views.generic import View
from netbox.object_actions import AddObject
from netbox.object_actions import BulkDelete
from netbox.object_actions import BulkEdit
from netbox.object_actions import BulkExport
from netbox.object_actions import BulkRename
from netbox.views import generic
from netbox.views.generic.base import BaseObjectView
from netbox_branching.models import ChangeDiff
from utilities.data import shallow_compare_dict
from utilities.forms import restrict_form_fields
from utilities.views import get_viewname
from utilities.views import register_model_view
from utilities.views import ViewTab

from .filtersets import ForwardDriftPolicyFilterSet
from .filtersets import ForwardIngestionChangeFilterSet
from .filtersets import ForwardIngestionFilterSet
from .filtersets import ForwardIngestionIssueFilterSet
from .filtersets import ForwardIngestionObjectChangeFilterSet
from .filtersets import ForwardNQEMapFilterSet
from .filtersets import ForwardSourceFilterSet
from .filtersets import ForwardSyncFilterSet
from .filtersets import ForwardValidationRunFilterSet
from .forms import ForwardDriftPolicyBulkEditForm
from .forms import ForwardDriftPolicyForm
from .forms import ForwardIngestionMergeForm
from .forms import ForwardNQEMapBulkEditForm
from .forms import ForwardNQEMapForm
from .forms import ForwardSourceBulkEditForm
from .forms import ForwardSourceForm
from .forms import ForwardSyncBulkEditForm
from .forms import ForwardSyncForm
from .forms import ForwardValidationRunForceAllowForm
from .models import ForwardDriftPolicy
from .models import ForwardIngestion
from .models import ForwardIngestionIssue
from .models import ForwardNQEMap
from .models import ForwardSource
from .models import ForwardSync
from .models import ForwardValidationRun
from .tables import ForwardDriftPolicyTable
from .tables import ForwardIngestionChangesTable
from .tables import ForwardIngestionIssueTable
from .tables import ForwardIngestionObjectChangesTable
from .tables import ForwardIngestionTable
from .tables import ForwardNQEMapTable
from .tables import ForwardSourceTable
from .tables import ForwardSyncTable
from .tables import ForwardValidationRunTable
from .utilities.direct_changes import object_changes_for_ingestion


def annotate_statistics(queryset):
    return queryset.annotate(
        num_created=models.Case(
            models.When(
                branch__isnull=True,
                then=models.F("created_change_count"),
            ),
            default=models.Count(
                "branch__changediff",
                filter=models.Q(
                    branch__changediff__action=ObjectChangeActionChoices.ACTION_CREATE
                )
                & ~models.Q(branch__changediff__object_type__model="objectchange"),
            ),
            output_field=models.IntegerField(),
        ),
        num_updated=models.Case(
            models.When(
                branch__isnull=True,
                then=models.F("updated_change_count"),
            ),
            default=models.Count(
                "branch__changediff",
                filter=models.Q(
                    branch__changediff__action=ObjectChangeActionChoices.ACTION_UPDATE
                )
                & ~models.Q(branch__changediff__object_type__model="objectchange"),
            ),
            output_field=models.IntegerField(),
        ),
        num_deleted=models.Case(
            models.When(
                branch__isnull=True,
                then=models.F("deleted_change_count"),
            ),
            default=models.Count(
                "branch__changediff",
                filter=models.Q(
                    branch__changediff__action=ObjectChangeActionChoices.ACTION_DELETE
                )
                & ~models.Q(branch__changediff__object_type__model="objectchange"),
            ),
            output_field=models.IntegerField(),
        ),
        description=models.F("branch__description"),
        user=models.F("sync__user__username"),
        staged_changes=models.Case(
            models.When(
                branch__isnull=True,
                then=models.F("applied_change_count"),
            ),
            default=models.Count(models.F("branch__changediff")),
            output_field=models.IntegerField(),
        ),
        branch_name=models.F("branch__name"),
        sync_name=models.F("sync__name"),
    )


@register_model_view(ForwardSource, "list", path="", detail=False)
class ForwardSourceListView(generic.ObjectListView):
    queryset = ForwardSource.objects.all()
    filterset = ForwardSourceFilterSet
    table = ForwardSourceTable
    actions = (AddObject, BulkExport, BulkEdit, BulkRename, BulkDelete)


@register_model_view(ForwardSource, "add", detail=False)
@register_model_view(ForwardSource, "edit")
class ForwardSourceEditView(generic.ObjectEditView):
    queryset = ForwardSource.objects.all()
    form = ForwardSourceForm


@register_model_view(ForwardSource)
class ForwardSourceView(generic.ObjectView):
    queryset = ForwardSource.objects.all()
    template_name = "forward_netbox/forwardsource.html"

    def get_extra_context(self, request, instance):
        return {"masked_parameters": instance.get_masked_parameters()}


@register_model_view(ForwardSource, "delete")
class ForwardSourceDeleteView(generic.ObjectDeleteView):
    queryset = ForwardSource.objects.all()


@register_model_view(ForwardSource, "bulk_edit", path="edit", detail=False)
class ForwardSourceBulkEditView(generic.BulkEditView):
    queryset = ForwardSource.objects.all()
    table = ForwardSourceTable
    form = ForwardSourceBulkEditForm


@register_model_view(ForwardSource, "bulk_rename", path="rename", detail=False)
class ForwardSourceBulkRenameView(generic.BulkRenameView):
    queryset = ForwardSource.objects.all()


@register_model_view(ForwardSource, "bulk_delete", path="delete", detail=False)
class ForwardSourceBulkDeleteView(generic.BulkDeleteView):
    queryset = ForwardSource.objects.all()
    table = ForwardSourceTable


@register_model_view(ForwardNQEMap, "list", path="", detail=False)
class ForwardNQEMapListView(generic.ObjectListView):
    queryset = ForwardNQEMap.objects.select_related("netbox_model")
    filterset = ForwardNQEMapFilterSet
    table = ForwardNQEMapTable
    actions = (AddObject, BulkExport, BulkEdit, BulkRename, BulkDelete)


@register_model_view(ForwardNQEMap, "add", detail=False)
@register_model_view(ForwardNQEMap, "edit")
class ForwardNQEMapEditView(generic.ObjectEditView):
    queryset = ForwardNQEMap.objects.select_related("netbox_model")
    form = ForwardNQEMapForm
    default_return_url = "plugins:forward_netbox:forwardnqemap_list"


@register_model_view(ForwardNQEMap)
class ForwardNQEMapView(generic.ObjectView):
    queryset = ForwardNQEMap.objects.select_related("netbox_model")
    template_name = "forward_netbox/forwardnqemap.html"


@register_model_view(ForwardNQEMap, "delete")
class ForwardNQEMapDeleteView(generic.ObjectDeleteView):
    queryset = ForwardNQEMap.objects.all()
    default_return_url = "plugins:forward_netbox:forwardnqemap_list"


@register_model_view(ForwardNQEMap, "bulk_edit", path="edit", detail=False)
class ForwardNQEMapBulkEditView(generic.BulkEditView):
    queryset = ForwardNQEMap.objects.select_related("netbox_model")
    table = ForwardNQEMapTable
    form = ForwardNQEMapBulkEditForm


@register_model_view(ForwardNQEMap, "bulk_rename", path="rename", detail=False)
class ForwardNQEMapBulkRenameView(generic.BulkRenameView):
    queryset = ForwardNQEMap.objects.all()


@register_model_view(ForwardNQEMap, "bulk_delete", path="delete", detail=False)
class ForwardNQEMapBulkDeleteView(generic.BulkDeleteView):
    queryset = ForwardNQEMap.objects.all()
    table = ForwardNQEMapTable


@register_model_view(ForwardSync, "list", path="", detail=False)
class ForwardSyncListView(generic.ObjectListView):
    queryset = ForwardSync.objects.all()
    filterset = ForwardSyncFilterSet
    table = ForwardSyncTable
    actions = (AddObject, BulkExport, BulkEdit, BulkRename, BulkDelete)


@register_model_view(ForwardSync, "add", detail=False)
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
    template_name = "forward_netbox/forwardsync.html"

    def get_extra_context(self, request, instance):
        data = {
            "last_ingestion": instance.last_ingestion,
            "latest_validation_run": instance.latest_validation_run,
            "enabled_models": instance.enabled_models(),
        }
        if instance.last_ingestion:
            data.update(instance.last_ingestion.get_statistics())
        return data


@register_model_view(ForwardSync, "run")
class ForwardStartSyncView(BaseObjectView):
    queryset = ForwardSync.objects.all()

    def get_required_permission(self):
        return "forward_netbox.run_forwardsync"

    def get(self, request, pk):
        sync = get_object_or_404(self.queryset, pk=pk)
        return redirect(sync.get_absolute_url())

    def post(self, request, pk):
        sync = get_object_or_404(self.queryset, pk=pk)
        try:
            job = sync.enqueue_sync_job(user=request.user, adhoc=True)
        except SyncError as exc:
            messages.error(request, str(exc))
            return redirect(sync.get_absolute_url())
        action = "continue" if sync.has_pending_branch_run else "run"
        messages.success(request, f"Queued job #{job.pk} to {action} {sync}.")
        return redirect(sync.get_absolute_url())


@register_model_view(ForwardSync, "validate")
class ForwardStartValidationView(BaseObjectView):
    queryset = ForwardSync.objects.all()

    def get_required_permission(self):
        return "forward_netbox.run_forwardsync"

    def get(self, request, pk):
        sync = get_object_or_404(self.queryset, pk=pk)
        return redirect(sync.get_absolute_url())

    def post(self, request, pk):
        sync = get_object_or_404(self.queryset, pk=pk)
        job = sync.enqueue_validation_job(user=request.user, adhoc=True)
        messages.success(request, f"Queued job #{job.pk} to validate {sync}.")
        return redirect(sync.get_absolute_url())


@register_model_view(ForwardSync, "delete")
class ForwardSyncDeleteView(generic.ObjectDeleteView):
    queryset = ForwardSync.objects.all()


@register_model_view(ForwardSync, "bulk_edit", path="edit", detail=False)
class ForwardSyncBulkEditView(generic.BulkEditView):
    queryset = ForwardSync.objects.all()
    table = ForwardSyncTable
    form = ForwardSyncBulkEditForm


@register_model_view(ForwardSync, "bulk_rename", path="rename", detail=False)
class ForwardSyncBulkRenameView(generic.BulkRenameView):
    queryset = ForwardSync.objects.all()


@register_model_view(ForwardSync, "bulk_delete", path="delete", detail=False)
class ForwardSyncBulkDeleteView(generic.BulkDeleteView):
    queryset = ForwardSync.objects.all()
    table = ForwardSyncTable


@register_model_view(ForwardSync, "ingestions")
class ForwardIngestionTabView(generic.ObjectChildrenView):
    queryset = ForwardSync.objects.all()
    child_model = ForwardIngestion
    table = ForwardIngestionTable
    filterset = ForwardIngestionFilterSet
    tab = ViewTab(
        label=_("Ingestions"),
        badge=lambda obj: ForwardIngestion.objects.filter(sync=obj).count(),
        permission="forward_netbox.view_forwardingestion",
    )

    def get_children(self, request, parent):
        return annotate_statistics(ForwardIngestion.objects.filter(sync=parent))


@register_model_view(ForwardIngestion, "list", path="", detail=False)
class ForwardIngestionListView(generic.ObjectListView):
    queryset = annotate_statistics(ForwardIngestion.objects.all())
    filterset = ForwardIngestionFilterSet
    table = ForwardIngestionTable
    actions = (BulkExport, BulkDelete)


@register_model_view(ForwardIngestion, name="logs", path="logs")
class ForwardIngestionLogView(LoginRequiredMixin, View):
    template_name = "forward_netbox/partials/ingestion_all.html"

    def get(self, request, **kwargs):
        ingestion = annotate_statistics(ForwardIngestion.objects).get(pk=kwargs["pk"])
        active_stage = request.GET.get("stage", "sync")
        data = ingestion.get_statistics(stage=active_stage)
        data["object"] = ingestion
        data["job"] = ingestion.job
        data["merge_job"] = ingestion.merge_job
        data["merge_job_results"] = ingestion.get_job_logs(ingestion.merge_job)
        data["active_stage"] = active_stage
        data["merge_disabled"] = not ingestion.merge_job

        if request.htmx:
            sync_running = ingestion.job and not ingestion.job.completed
            merge_running = ingestion.merge_job and not ingestion.merge_job.completed
            anything_ever_ran = ingestion.job or ingestion.merge_job
            data["polling_done"] = (
                bool(anything_ever_ran) and not sync_running and not merge_running
            )
        return render(request, self.template_name, data)


@register_model_view(ForwardIngestion, name="progress", path="progress")
class ForwardIngestionProgressView(LoginRequiredMixin, View):
    template_name = "forward_netbox/partials/ingestion_progress.html"

    def get(self, request, **kwargs):
        ingestion = annotate_statistics(ForwardIngestion.objects).get(pk=kwargs["pk"])
        active_stage = request.GET.get("stage", "sync")
        if active_stage not in ("sync", "merge"):
            active_stage = "sync"
        data = ingestion.get_statistics(stage=active_stage)
        data["job"] = ingestion.job
        data["merge_job"] = ingestion.merge_job
        data["active_stage"] = active_stage
        data["merge_disabled"] = not ingestion.merge_job
        return render(request, self.template_name, data)


@register_model_view(ForwardIngestion)
class ForwardIngestionView(generic.ObjectView):
    queryset = annotate_statistics(ForwardIngestion.objects)
    template_name = "forward_netbox/forwardingestion.html"

    def get_extra_context(self, request, instance):
        active_stage = request.GET.get("stage", "sync")
        data = instance.get_statistics(stage=active_stage)
        sync_running = instance.job and not instance.job.completed
        merge_running = instance.merge_job and not instance.merge_job.completed
        data["job_running"] = bool(sync_running or merge_running)
        data["merge_job"] = instance.merge_job
        data["merge_job_results"] = instance.get_job_logs(instance.merge_job)
        data["active_stage"] = active_stage
        data["merge_disabled"] = not instance.merge_job
        return data


@register_model_view(ForwardIngestion, "merge")
class ForwardIngestionMergeView(BaseObjectView):
    queryset = annotate_statistics(ForwardIngestion.objects.all())
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
                self.template_name,
                {
                    "object": obj,
                    "object_type": self.queryset.model._meta.verbose_name,
                    "form": form,
                    "form_url": form_url,
                },
            )
        return redirect(obj.get_absolute_url())

    def post(self, request, pk):
        ingestion = get_object_or_404(self.queryset, pk=pk)
        form = self.form(request.POST)
        restrict_form_fields(form, request.user)
        if form.is_valid():
            job = ingestion.enqueue_merge_job(
                user=request.user,
                remove_branch=form.cleaned_data["remove_branch"],
            )
            messages.success(request, f"Queued job #{job.pk} to merge {ingestion}.")
            return redirect(ingestion.get_absolute_url())

        for field, errors in form.errors.items():
            for error in errors:
                messages.error(request, f"{field}: {error}")
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
        change_id = kwargs.get("change_pk")
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
                change.original or {},
                change.modified or {},
                exclude=["last_updated"],
            )
            diff_removed = {key: change.original.get(key) for key in diff_added}
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
    actions = ()
    template_name = "generic/object_children.html"
    tab = ViewTab(
        label=_("Changes"),
        badge=lambda obj: (
            ChangeDiff.objects.filter(branch=obj.branch).count()
            if obj.branch_id
            else object_changes_for_ingestion(obj).count()
        ),
        permission="forward_netbox.view_forwardingestion",
    )

    def get(self, request, *args, **kwargs):
        parent = self.get_object(**kwargs)
        if parent.branch_id:
            self.child_model = ChangeDiff
            self.table = ForwardIngestionChangesTable
            self.filterset = ForwardIngestionChangeFilterSet
        else:
            self.child_model = ObjectChange
            self.table = ForwardIngestionObjectChangesTable
            self.filterset = ForwardIngestionObjectChangeFilterSet
        return super().get(request, *args, **kwargs)

    def get_children(self, request, parent):
        if parent.branch_id:
            return ChangeDiff.objects.filter(branch=parent.branch)
        return object_changes_for_ingestion(parent)


@register_model_view(ForwardIngestion, "issues")
class ForwardIngestionIssuesView(generic.ObjectChildrenView):
    queryset = ForwardIngestion.objects.all()
    child_model = ForwardIngestionIssue
    table = ForwardIngestionIssueTable
    template_name = "generic/object_children.html"
    filterset = ForwardIngestionIssueFilterSet
    tab = ViewTab(
        label=_("Ingestion Issues"),
        badge=lambda obj: ForwardIngestionIssue.objects.filter(ingestion=obj).count(),
        permission="forward_netbox.view_forwardingestionissue",
    )

    def get_children(self, request, parent):
        return ForwardIngestionIssue.objects.filter(ingestion=parent)


@register_model_view(ForwardIngestion, "delete")
class ForwardIngestionDeleteView(generic.ObjectDeleteView):
    queryset = ForwardIngestion.objects.all()


@register_model_view(ForwardIngestion, "bulk_delete", path="delete", detail=False)
class ForwardIngestionBulkDeleteView(generic.BulkDeleteView):
    queryset = ForwardIngestion.objects.all()
    table = ForwardIngestionTable


@register_model_view(ForwardDriftPolicy, "list", path="", detail=False)
class ForwardDriftPolicyListView(generic.ObjectListView):
    queryset = ForwardDriftPolicy.objects.all()
    filterset = ForwardDriftPolicyFilterSet
    table = ForwardDriftPolicyTable
    actions = (AddObject, BulkExport, BulkEdit, BulkRename, BulkDelete)


@register_model_view(ForwardDriftPolicy, "add", detail=False)
@register_model_view(ForwardDriftPolicy, "edit")
class ForwardDriftPolicyEditView(generic.ObjectEditView):
    queryset = ForwardDriftPolicy.objects.all()
    form = ForwardDriftPolicyForm


@register_model_view(ForwardDriftPolicy)
class ForwardDriftPolicyView(generic.ObjectView):
    queryset = ForwardDriftPolicy.objects.all()
    template_name = "forward_netbox/forwarddriftpolicy.html"


@register_model_view(ForwardDriftPolicy, "delete")
class ForwardDriftPolicyDeleteView(generic.ObjectDeleteView):
    queryset = ForwardDriftPolicy.objects.all()


@register_model_view(ForwardDriftPolicy, "bulk_edit", path="edit", detail=False)
class ForwardDriftPolicyBulkEditView(generic.BulkEditView):
    queryset = ForwardDriftPolicy.objects.all()
    table = ForwardDriftPolicyTable
    form = ForwardDriftPolicyBulkEditForm


@register_model_view(ForwardDriftPolicy, "bulk_rename", path="rename", detail=False)
class ForwardDriftPolicyBulkRenameView(generic.BulkRenameView):
    queryset = ForwardDriftPolicy.objects.all()


@register_model_view(ForwardDriftPolicy, "bulk_delete", path="delete", detail=False)
class ForwardDriftPolicyBulkDeleteView(generic.BulkDeleteView):
    queryset = ForwardDriftPolicy.objects.all()
    table = ForwardDriftPolicyTable


@register_model_view(ForwardValidationRun, "list", path="", detail=False)
class ForwardValidationRunListView(generic.ObjectListView):
    queryset = ForwardValidationRun.objects.select_related("sync", "policy")
    filterset = ForwardValidationRunFilterSet
    table = ForwardValidationRunTable
    actions = (BulkExport, BulkDelete)


@register_model_view(ForwardValidationRun)
class ForwardValidationRunView(generic.ObjectView):
    queryset = ForwardValidationRun.objects.select_related("sync", "policy", "job")
    template_name = "forward_netbox/forwardvalidationrun.html"


@register_model_view(ForwardValidationRun, "force_allow")
class ForwardValidationRunForceAllowView(BaseObjectView):
    queryset = ForwardValidationRun.objects.select_related("sync", "policy", "job")
    template_name = "forward_netbox/inc/validation_force_allow_form.html"
    form = ForwardValidationRunForceAllowForm

    def get_required_permission(self):
        return "forward_netbox.change_forwardvalidationrun"

    def get(self, request, pk):
        obj = get_object_or_404(self.queryset, pk=pk)
        if request.htmx:
            viewname = get_viewname(self.queryset.model, action="force_allow")
            form_url = reverse(viewname, kwargs={"pk": obj.pk})
            form = self.form(initial=request.GET)
            restrict_form_fields(form, request.user)
            return render(
                request,
                self.template_name,
                {
                    "object": obj,
                    "object_type": self.queryset.model._meta.verbose_name,
                    "form": form,
                    "form_url": form_url,
                },
            )
        return redirect(obj.get_absolute_url())

    def post(self, request, pk):
        validation_run = get_object_or_404(self.queryset, pk=pk)
        form = self.form(request.POST)
        restrict_form_fields(form, request.user)
        if form.is_valid():
            validation_run.force_allow(
                user=request.user,
                reason=form.cleaned_data["reason"],
            )
            messages.success(
                request,
                f"Marked {validation_run} as force-allowed for the blocked validation reasons.",
            )
            return redirect(validation_run.get_absolute_url())

        for field, errors in form.errors.items():
            for error in errors:
                messages.error(request, f"{field}: {error}")
        for error in form.non_field_errors():
            messages.error(request, error)
        return redirect(validation_run.get_absolute_url())


@register_model_view(ForwardValidationRun, "delete")
class ForwardValidationRunDeleteView(generic.ObjectDeleteView):
    queryset = ForwardValidationRun.objects.all()


@register_model_view(ForwardValidationRun, "bulk_delete", path="delete", detail=False)
class ForwardValidationRunBulkDeleteView(generic.BulkDeleteView):
    queryset = ForwardValidationRun.objects.all()
    table = ForwardValidationRunTable
