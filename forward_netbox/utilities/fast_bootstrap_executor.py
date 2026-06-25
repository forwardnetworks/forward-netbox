from core.models import ObjectType

from .multi_branch_lifecycle import set_runtime_phase


class ForwardFastBootstrapExecutor:
    """Shared executor base for the single-branch sync.

    Originally the direct-write "fast bootstrap" backend. 2.0 made
    ``ForwardSingleBranchExecutor`` the only execution path and it overrides
    ``run()`` entirely, so the old direct-write ``run()`` (and its
    ``_record_change_totals`` / ``_raise_if_blocking_issues_exist`` helpers) were
    dead and have been removed. What remains are the construction and bookkeeping
    helpers the subclass reuses: ingestion creation, the runtime-phase heartbeat,
    the query-preflight opt-out, and sync-mode derivation. (The class name is kept
    to avoid a cross-module rename; a rename is tracked as follow-up.)
    """

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

    def _query_preflight_enabled(self) -> bool:
        source = getattr(self.sync, "source", None)
        parameters = dict(getattr(source, "parameters", {}) or {})
        configured = parameters.get("query_preflight_enabled")
        if configured is None:
            return True
        if isinstance(configured, str):
            return configured.strip().lower() in {"1", "true", "yes", "on"}
        return bool(configured)

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
