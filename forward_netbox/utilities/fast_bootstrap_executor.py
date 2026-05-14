from core.exceptions import SyncError
from core.models import ObjectType
from netbox.context import current_request
from netbox.context_managers import event_tracking

from ..choices import FORWARD_OPTIONAL_MODELS
from .branching import build_branch_request
from .direct_changes import action_counts_for_request
from .direct_changes import any_object_changes_for_request
from .multi_branch_lifecycle import create_noop_ingestion
from .multi_branch_lifecycle import set_runtime_phase
from .query_fetch import ForwardQueryFetcher
from .sync import ForwardSyncRunner
from .validation import ForwardValidationRunner


class ForwardFastBootstrapExecutor:
    """Direct-write execution backend for large initial syncs."""

    def __init__(self, sync, client, logger_, *, user=None, job=None):
        self.sync = sync
        self.client = client
        self.logger = logger_
        self.user = user
        self.job = job
        self.current_ingestion = None
        self.last_model_results = []
        self.last_validation_run = None

    def _set_runtime_phase(
        self, phase, message, *, next_plan_index=None, total_plan_items=None
    ):
        return set_runtime_phase(
            self,
            phase,
            message,
            next_plan_index=next_plan_index,
            total_plan_items=total_plan_items,
        )

    def _create_ingestion(self, context, *, change_request_id=None):
        from ..models import ForwardIngestion

        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            job=self.job,
            validation_run=self.last_validation_run,
            snapshot_selector=context["snapshot_selector"],
            snapshot_id=context["snapshot_id"],
            change_request_id=change_request_id,
            snapshot_info=context["snapshot_info"],
            snapshot_metrics=context["snapshot_metrics"],
            model_results=self.last_model_results,
        )
        self.current_ingestion = ingestion
        if self.job:
            self.job.object_type = ObjectType.objects.get_for_model(ingestion)
            self.job.object_id = ingestion.pk
            self.job.save(update_fields=["object_type", "object_id"])
        return ingestion

    def _sync_mode(self):
        modes = {
            result.get("sync_mode")
            for result in self.last_model_results
            if result.get("sync_mode") in {"full", "diff"}
        }
        if modes == {"full", "diff"}:
            return "hybrid"
        if modes == {"diff"}:
            return "diff"
        return "full"

    def _record_change_totals(self, ingestion, *, request_id=None):
        statistics = self.logger.log_data.get("statistics", {})
        failed = sum(int(stats.get("failed") or 0) for stats in statistics.values())
        if request_id and any_object_changes_for_request(request_id):
            action_counts = action_counts_for_request(self.sync, request_id)
            created = action_counts.get("create", 0)
            updated = action_counts.get("update", 0)
            deleted = action_counts.get("delete", 0)
            ingestion.record_change_totals(
                applied=created + updated + deleted,
                failed=failed,
                created=created,
                updated=updated,
                deleted=deleted,
            )
            return

        applied = sum(int(stats.get("applied") or 0) for stats in statistics.values())
        ingestion.record_change_totals(applied=applied, failed=failed)

    def _raise_if_blocking_issues_exist(self, ingestion, *, request_id=None):
        if not ingestion.issues.exists():
            return False
        self._record_change_totals(ingestion, request_id=request_id)
        blocking_issues = ingestion.issues.exclude(model__in=FORWARD_OPTIONAL_MODELS)
        if not blocking_issues.exists():
            self.logger.log_warning(
                "Forward fast bootstrap completed with non-blocking optional-model issues; "
                "review ingestion issues for skipped beta/optional rows.",
                obj=ingestion,
            )
            return True
        messages = list(blocking_issues.values_list("message", flat=True)[:5])
        raise SyncError(
            "Forward fast bootstrap completed with issues: " + "; ".join(messages)
        )

    def run(self):
        self._set_runtime_phase("initializing", "Starting fast bootstrap preflight.")
        fetcher = ForwardQueryFetcher(self.sync, self.client, self.logger)
        context = fetcher.resolve_context()
        self._set_runtime_phase(
            "planning",
            "Resolving snapshot, running query preflight, and building fast bootstrap workload.",
            next_plan_index=1,
        )
        fetcher.run_preflight(context)
        workloads = fetcher.fetch_workloads(context)
        self.last_model_results = [result.as_dict() for result in fetcher.model_results]
        self._set_runtime_phase(
            "validating",
            "Recording fast bootstrap validation results.",
            total_plan_items=len(workloads),
        )
        self.last_validation_run = ForwardValidationRunner(
            self.sync,
            self.client,
            self.logger,
            job=self.job,
        ).record_plan_validation(
            context.as_dict(),
            workloads,
            self.last_model_results,
        )
        if not workloads:
            self.sync.clear_branch_run_state()
            self.logger.log_info("No Forward changes were returned for this run.")
            return [create_noop_ingestion(self, context.as_dict())]

        request = build_branch_request(self.user)
        ingestion = self._create_ingestion(
            context.as_dict(),
            change_request_id=request.id,
        )
        runner = ForwardSyncRunner(
            sync=self.sync,
            ingestion=ingestion,
            client=self.client,
            logger_=self.logger,
        )
        pending_deletes = {}
        initialized_models = set()
        total_workloads = len(workloads)
        request_token = None
        if current_request.get() is None:
            request_token = current_request.set(request)
        try:
            with event_tracking(request):
                for index, workload in enumerate(workloads, start=1):
                    runner._model_coalesce_fields[workload.model_string] = (
                        workload.coalesce_fields
                    )
                    pending_deletes.setdefault(workload.model_string, []).extend(
                        workload.delete_rows
                    )
                    self._set_runtime_phase(
                        "executing",
                        f"Fast bootstrap applying {workload.model_string} ({index}/{total_workloads}).",
                        next_plan_index=index,
                        total_plan_items=total_workloads,
                    )
                    if workload.model_string not in initialized_models:
                        self.logger.init_statistics(workload.model_string, 0)
                        initialized_models.add(workload.model_string)
                    self.logger.add_statistics_total(
                        workload.model_string,
                        workload.estimated_changes,
                    )
                    runner._apply_model_rows(
                        workload.model_string, workload.upsert_rows
                    )
                    self._record_change_totals(ingestion, request_id=request.id)

                for model_string in reversed(self.sync.get_model_strings()):
                    delete_rows = pending_deletes.get(model_string, [])
                    if not delete_rows:
                        continue
                    self._set_runtime_phase(
                        "executing",
                        f"Fast bootstrap deleting {model_string}.",
                        total_plan_items=total_workloads,
                    )
                    runner._delete_model_rows(model_string, delete_rows)
                    self._record_change_totals(ingestion, request_id=request.id)

                self._raise_if_blocking_issues_exist(ingestion, request_id=request.id)
                ingestion.sync_mode = self._sync_mode()
                ingestion.baseline_ready = True
                ingestion.model_results = self.last_model_results
                ingestion.save(
                    update_fields=["sync_mode", "baseline_ready", "model_results"],
                )
                self._record_change_totals(ingestion, request_id=request.id)
        finally:
            if request_token is not None:
                current_request.reset(request_token)

        self.sync.clear_branch_run_state()
        self.logger.log_info(
            "Forward fast bootstrap ingestion completed.", obj=ingestion
        )
        return [ingestion]
