from core.exceptions import SyncError
from core.models import ObjectType
from netbox.context import current_request
from netbox_branching.choices import BranchStatusChoices
from netbox_branching.contextvars import active_branch
from netbox_branching.models import Branch

from ..choices import ForwardSyncStatusChoices
from ..exceptions import ForwardClientError
from ..exceptions import ForwardConnectivityError
from ..exceptions import ForwardQueryError
from .branch_budget import BranchWorkload
from .branch_budget import build_branch_plan
from .branch_budget import DEFAULT_MAX_CHANGES_PER_BRANCH
from .branching import build_branch_request
from .query_registry import get_query_specs
from .sync import ForwardSyncRunner
from .sync_contracts import default_coalesce_fields_for_model
from .sync_contracts import validate_row_shape_for_model


class ForwardMultiBranchPlanner:
    def __init__(self, sync, client, logger_):
        self.sync = sync
        self.client = client
        self.logger = logger_

    def build_plan(self, *, max_changes_per_branch=DEFAULT_MAX_CHANGES_PER_BRANCH):
        context = self._resolve_context()
        workloads = self._fetch_workloads(context)
        plan = build_branch_plan(
            workloads,
            max_changes_per_branch=max_changes_per_branch,
        )
        return context, plan

    def _resolve_context(self):
        network_id = self.sync.get_network_id()
        snapshot_selector = self.sync.get_snapshot_id()
        snapshot_id = self.sync.resolve_snapshot_id(self.client)
        if not network_id:
            raise ForwardQueryError(
                "Forward sync requires a network ID on the sync or its source."
            )
        if not snapshot_id:
            raise ForwardQueryError(
                "Forward sync requires a snapshot ID for NQE execution."
            )
        snapshot_info = {}
        if snapshot_selector == snapshot_id:
            for snapshot in self.client.get_snapshots(network_id):
                if snapshot["id"] == snapshot_id:
                    snapshot_info = {
                        "id": snapshot["id"],
                        "state": snapshot.get("state") or "",
                        "createdAt": snapshot.get("created_at") or "",
                        "processedAt": snapshot.get("processed_at") or "",
                    }
                    break
        else:
            snapshot_info = self.client.get_latest_processed_snapshot(network_id)

        snapshot_metrics = {}
        try:
            snapshot_metrics = self.client.get_snapshot_metrics(snapshot_id)
        except Exception as exc:
            self.logger.log_warning(
                f"Unable to fetch Forward snapshot metrics for `{snapshot_id}`: {exc}",
                obj=self.sync,
            )
        return {
            "network_id": network_id,
            "snapshot_selector": snapshot_selector,
            "snapshot_id": snapshot_id,
            "snapshot_info": snapshot_info or {},
            "snapshot_metrics": snapshot_metrics or {},
            "query_parameters": self.sync.get_query_parameters(),
            "maps": self.sync.get_maps(),
        }

    def _fetch_workloads(self, context):
        workloads = []
        for model_string in self.sync.get_model_strings():
            specs = get_query_specs(model_string, maps=context["maps"])
            if specs:
                coalesce_fields = [
                    list(field_set) for field_set in specs[0].coalesce_fields
                ] or default_coalesce_fields_for_model(model_string)
            else:
                coalesce_fields = default_coalesce_fields_for_model(model_string)

            baseline = self.sync.incremental_diff_baseline(
                specs=specs,
                current_snapshot_id=context["snapshot_id"],
            )
            for spec in specs:
                rows, delete_rows, sync_mode = self._fetch_spec_rows(
                    model_string,
                    spec,
                    baseline,
                    context,
                    coalesce_fields,
                )
                if not rows and not delete_rows:
                    continue
                workloads.append(
                    BranchWorkload(
                        model_string=model_string,
                        label=f"{model_string} | {spec.query_name}",
                        upsert_rows=rows,
                        delete_rows=delete_rows,
                        sync_mode=sync_mode,
                        coalesce_fields=coalesce_fields,
                    )
                )
        return workloads

    def _fetch_spec_rows(self, model_string, spec, baseline, context, coalesce_fields):
        runner = ForwardSyncRunner(
            sync=self.sync,
            ingestion=None,
            client=self.client,
            logger_=self.logger,
        )
        runner._model_coalesce_fields[model_string] = coalesce_fields

        if baseline is not None and spec.query_id:
            try:
                diff_rows = self.client.run_nqe_diff(
                    query_id=spec.query_id,
                    commit_id=spec.commit_id,
                    before_snapshot_id=baseline.snapshot_id,
                    after_snapshot_id=context["snapshot_id"],
                    fetch_all=True,
                )
                rows, delete_rows = runner._split_diff_rows(model_string, diff_rows)
                sync_mode = "diff"
            except (ForwardClientError, ForwardConnectivityError) as exc:
                self.logger.log_warning(
                    f"Forward NQE diff failed for {model_string} using `{spec.execution_value}`; "
                    f"falling back to full query execution: {exc}",
                    obj=self.sync,
                )
                rows = self.client.run_nqe_query(
                    query=spec.query,
                    query_id=spec.query_id,
                    commit_id=spec.commit_id,
                    network_id=context["network_id"],
                    snapshot_id=context["snapshot_id"],
                    parameters=spec.merged_parameters(context["query_parameters"]),
                    fetch_all=True,
                )
                delete_rows = []
                sync_mode = "full"
        else:
            rows = self.client.run_nqe_query(
                query=spec.query,
                query_id=spec.query_id,
                commit_id=spec.commit_id,
                network_id=context["network_id"],
                snapshot_id=context["snapshot_id"],
                parameters=spec.merged_parameters(context["query_parameters"]),
                fetch_all=True,
            )
            delete_rows = []
            sync_mode = "full"

        for row in rows:
            validate_row_shape_for_model(model_string, row, coalesce_fields)
        for row in delete_rows:
            validate_row_shape_for_model(model_string, row, coalesce_fields)
        return rows, delete_rows, sync_mode


class ForwardMultiBranchExecutor:
    def __init__(self, sync, client, logger_, *, user=None, job=None):
        self.sync = sync
        self.client = client
        self.logger = logger_
        self.user = user
        self.job = job

    def plan(self, *, max_changes_per_branch=DEFAULT_MAX_CHANGES_PER_BRANCH):
        planner = ForwardMultiBranchPlanner(self.sync, self.client, self.logger)
        return planner.build_plan(max_changes_per_branch=max_changes_per_branch)

    def run(self, *, max_changes_per_branch=DEFAULT_MAX_CHANGES_PER_BRANCH):
        self.max_changes_per_branch = max_changes_per_branch
        context, plan = self.plan(max_changes_per_branch=max_changes_per_branch)
        if not plan:
            self.logger.log_info("No Forward changes were returned for this run.")
            return [self._create_noop_ingestion(context)]

        ingestions = []
        for offset, item in enumerate(plan):
            is_final = offset == len(plan) - 1
            ingestion = self._run_plan_item(item, context, mark_baseline_ready=is_final)
            ingestions.append(ingestion)
        return ingestions

    def _create_noop_ingestion(self, context):
        from ..models import ForwardIngestion

        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            job=self.job,
            snapshot_selector=context["snapshot_selector"],
            snapshot_id=context["snapshot_id"],
            snapshot_info=context["snapshot_info"],
            snapshot_metrics=context["snapshot_metrics"],
            baseline_ready=True,
        )
        if self.job:
            self.job.object_type = ObjectType.objects.get_for_model(ingestion)
            self.job.object_id = ingestion.pk
            self.job.save(update_fields=["object_type", "object_id"])
        return ingestion

    def _run_plan_item(self, item, context, *, mark_baseline_ready):
        from ..models import ForwardIngestion

        self.sync.status = ForwardSyncStatusChoices.SYNCING
        self.sync.__class__.objects.filter(pk=self.sync.pk).update(
            status=self.sync.status
        )
        ingestion = ForwardIngestion.objects.create(sync=self.sync, job=self.job)
        branch = Branch(
            name=f"Forward Sync {self.sync.name} - part {item.index} {item.model_string}"
        )
        branch.save(provision=False)
        ingestion.branch = branch
        ingestion.save(update_fields=["branch"])

        if self.job:
            self.job.object_type = ObjectType.objects.get_for_model(ingestion)
            self.job.object_id = ingestion.pk
            self.job.save(update_fields=["object_type", "object_id"])

        branch.provision(user=self.user)
        branch.refresh_from_db()
        if branch.status == BranchStatusChoices.FAILED:
            self.logger.log_failure(f"Branch failed: `{branch}`", obj=branch)
            raise SyncError("Branch creation failed.")

        self.logger.log_info(
            f"New branch created {branch.name} for {item.estimated_changes} estimated changes.",
            obj=branch,
        )
        self._run_item_in_branch(item, context, ingestion, branch)
        if ingestion.issues.exists():
            messages = list(ingestion.issues.values_list("message", flat=True)[:5])
            raise SyncError(
                "Forward multi-branch shard completed with issues: "
                + "; ".join(messages)
            )

        actual_changes = branch.get_unmerged_changes().count()
        if actual_changes > self.max_changes_per_branch:
            raise SyncError(
                f"Branch `{branch}` produced {actual_changes} changes, exceeding "
                f"the branch budget of {self.max_changes_per_branch}."
            )

        ingestion.sync_merge(mark_baseline_ready=mark_baseline_ready)
        return ingestion

    def _run_item_in_branch(self, item, context, ingestion, branch):
        runner = ForwardSyncRunner(
            sync=self.sync,
            ingestion=ingestion,
            client=self.client,
            logger_=self.logger,
        )
        runner._model_coalesce_fields[item.model_string] = item.coalesce_fields
        ingestion.snapshot_selector = context["snapshot_selector"]
        ingestion.snapshot_id = context["snapshot_id"]
        ingestion.snapshot_info = context["snapshot_info"]
        ingestion.snapshot_metrics = context["snapshot_metrics"]
        ingestion.sync_mode = item.sync_mode
        ingestion.save(
            update_fields=[
                "snapshot_selector",
                "snapshot_id",
                "snapshot_info",
                "snapshot_metrics",
                "sync_mode",
            ],
        )
        self.logger.init_statistics(item.model_string, 0)
        self.logger.add_statistics_total(item.model_string, item.estimated_changes)

        current_branch = active_branch.get()
        request_token = None
        if current_request.get() is None:
            request_token = current_request.set(build_branch_request(self.user))
        try:
            active_branch.set(branch)
            try:
                runner._apply_model_rows(item.model_string, item.upsert_rows)
                if item.delete_rows:
                    runner._delete_model_rows(item.model_string, item.delete_rows)
            finally:
                active_branch.set(None)
        finally:
            active_branch.set(current_branch)
            if request_token is not None:
                current_request.reset(request_token)
