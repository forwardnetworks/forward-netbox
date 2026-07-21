from core.models import ObjectType


class ForwardExecutorBase:
    """Shared construction and bookkeeping for the single-branch executor."""

    def __init__(self, sync, client, logger_, *, user=None, job=None):
        self.sync = sync
        self.client = client
        self.logger = logger_
        self.user = user
        self.job = job
        self.current_ingestion = None
        self.last_model_results = []
        self.last_validation_run = None

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
