from netbox.context import current_request
from netbox_branching.contextvars import active_branch

from .branching import build_branch_request
from .query_fetch import plan_item_model_result
from .sync import ForwardSyncRunner
from .turbobulk import TurboBulkError


class NativeBranchExecutionBackend:
    """Apply one branch plan item through the existing NetBox adapter path."""

    key = "native"

    def __init__(self, *, sync, client, logger_, user=None):
        self.sync = sync
        self.client = client
        self.logger = logger_
        self.user = user

    def apply_plan_item(self, item, context, ingestion, branch, *, total_plan_items):
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
        ingestion.model_results = [
            plan_item_model_result(
                item,
                context,
                total_plan_items=total_plan_items,
            )
        ]
        ingestion.save(
            update_fields=[
                "snapshot_selector",
                "snapshot_id",
                "snapshot_info",
                "snapshot_metrics",
                "sync_mode",
                "model_results",
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


class TurboBulkBranchExecutionBackend:
    """Placeholder for the optional future TurboBulk-backed executor."""

    key = "turbobulk"

    def __init__(self, *, capability):
        self.capability = capability

    def apply_plan_item(self, item, context, ingestion, branch, *, total_plan_items):
        if not self.capability.usable:
            raise TurboBulkError(self.capability.reason)
        raise NotImplementedError("TurboBulk branch execution is not implemented yet.")
