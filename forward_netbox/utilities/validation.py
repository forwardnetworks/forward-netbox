from django.utils import timezone

from ..choices import ForwardDriftPolicyBaselineChoices
from ..choices import ForwardValidationStatusChoices
from ..exceptions import ForwardSyncError
from .query_fetch import ForwardQueryFetcher


class ForwardValidationRunner:
    def __init__(self, sync, client, logger_, *, job=None):
        self.sync = sync
        self.client = client
        self.logger = logger_
        self.job = job

    def run_query_validation(self):
        from ..models import ForwardValidationRun

        validation_run = self._create_run()
        try:
            fetcher = ForwardQueryFetcher(self.sync, self.client, self.logger)
            context = fetcher.resolve_context()
            fetcher.run_preflight(context)
            workloads = fetcher.fetch_workloads(context)
            return self.record_plan_validation(
                context.as_dict(),
                workloads,
                [result.as_dict() for result in fetcher.model_results],
                validation_run=validation_run,
                raise_on_block=False,
            )
        except Exception as exc:
            ForwardValidationRun.objects.filter(pk=validation_run.pk).update(
                status=ForwardValidationStatusChoices.FAILED,
                allowed=False,
                completed=timezone.now(),
                blocking_reasons=[str(exc)],
            )
            raise

    def record_plan_validation(
        self,
        context,
        plan,
        model_results,
        *,
        validation_run=None,
        raise_on_block=True,
    ):
        validation_run = validation_run or self._create_run()
        policy = self.sync.drift_policy
        blocking_reasons = self._blocking_reasons(context, plan, model_results, policy)
        allowed = not blocking_reasons
        status = (
            ForwardValidationStatusChoices.PASSED
            if allowed
            else ForwardValidationStatusChoices.BLOCKED
        )
        drift_summary = self._drift_summary(plan, model_results)

        validation_run.policy = policy
        validation_run.status = status
        validation_run.allowed = allowed
        validation_run.snapshot_selector = context["snapshot_selector"]
        validation_run.snapshot_id = context["snapshot_id"]
        validation_run.baseline_snapshot_id = self._baseline_snapshot_id(
            model_results,
            policy,
        )
        validation_run.snapshot_info = context["snapshot_info"]
        validation_run.snapshot_metrics = context["snapshot_metrics"]
        validation_run.model_results = list(model_results or [])
        validation_run.drift_summary = drift_summary
        validation_run.blocking_reasons = blocking_reasons
        validation_run.completed = timezone.now()
        validation_run.save(
            update_fields=[
                "policy",
                "status",
                "allowed",
                "snapshot_selector",
                "snapshot_id",
                "baseline_snapshot_id",
                "snapshot_info",
                "snapshot_metrics",
                "model_results",
                "drift_summary",
                "blocking_reasons",
                "completed",
            ]
        )
        if not allowed and raise_on_block:
            raise ForwardSyncError(
                "Forward validation blocked sync: " + "; ".join(blocking_reasons)
            )
        return validation_run

    def _create_run(self):
        from ..models import ForwardValidationRun

        return ForwardValidationRun.objects.create(
            sync=self.sync,
            policy=self.sync.drift_policy,
            job=self.job,
            status=ForwardValidationStatusChoices.RUNNING,
            started=timezone.now(),
        )

    def _blocking_reasons(self, context, plan, model_results, policy):
        if policy is None or not policy.enabled:
            return []

        reasons = []
        if policy.require_processed_snapshot and not self._snapshot_is_processed(
            context
        ):
            reasons.append("Target snapshot is not processed.")

        if policy.block_on_query_errors:
            failures = sum(
                int(result.get("failure_count") or 0) for result in model_results
            )
            if failures:
                reasons.append(f"{failures} query failures were reported.")

        if policy.block_on_zero_rows:
            counts_by_model = {}
            for result in model_results:
                counts_by_model.setdefault(result.get("model"), 0)
                counts_by_model[result.get("model")] += int(
                    result.get("row_count") or 0
                ) + int(result.get("delete_count") or 0)
            empty_models = [
                model_string
                for model_string in self.sync.get_model_strings()
                if counts_by_model.get(model_string, 0) == 0
            ]
            if empty_models:
                reasons.append(
                    "No rows were returned for enabled models: "
                    + ", ".join(sorted(empty_models))
                    + "."
                )

        total_deletes = sum(
            int(result.get("delete_count") or 0) for result in model_results
        )
        if (
            policy.max_deleted_objects is not None
            and total_deletes > policy.max_deleted_objects
        ):
            reasons.append(
                f"Delete count {total_deletes} exceeds policy limit {policy.max_deleted_objects}."
            )
        total_changes = sum(
            int(result.get("row_count") or 0) + int(result.get("delete_count") or 0)
            for result in model_results
        )
        if (
            policy.max_deleted_percent is not None
            and total_changes
            and (total_deletes / total_changes * 100) > policy.max_deleted_percent
        ):
            reasons.append(
                f"Delete percentage exceeds policy limit {policy.max_deleted_percent}%."
            )
        return reasons

    def _snapshot_is_processed(self, context):
        info = context.get("snapshot_info") or {}
        metrics = context.get("snapshot_metrics") or {}
        state = (
            info.get("state")
            or info.get("snapshotState")
            or metrics.get("snapshotState")
            or ""
        )
        return str(state).upper() == "PROCESSED"

    def _baseline_snapshot_id(self, model_results, policy):
        if (
            policy is not None
            and policy.baseline_mode == ForwardDriftPolicyBaselineChoices.NONE
        ):
            return ""
        for result in model_results:
            baseline_snapshot_id = result.get("baseline_snapshot_id") or ""
            if baseline_snapshot_id:
                return baseline_snapshot_id
        baseline = self.sync.latest_baseline_ingestion()
        return baseline.snapshot_id if baseline else ""

    def _drift_summary(self, plan, model_results):
        by_model = {}
        for result in model_results:
            model_string = result.get("model") or ""
            model_summary = by_model.setdefault(
                model_string,
                {
                    "row_count": 0,
                    "delete_count": 0,
                    "failure_count": 0,
                    "runtime_ms": 0,
                },
            )
            model_summary["row_count"] += int(result.get("row_count") or 0)
            model_summary["delete_count"] += int(result.get("delete_count") or 0)
            model_summary["failure_count"] += int(result.get("failure_count") or 0)
            model_summary["runtime_ms"] += float(result.get("runtime_ms") or 0)

        return {
            "model_count": len([model for model in by_model if model]),
            "branch_count": len(plan or []),
            "total_rows": sum(item["row_count"] for item in by_model.values()),
            "total_deletes": sum(item["delete_count"] for item in by_model.values()),
            "total_failures": sum(item["failure_count"] for item in by_model.values()),
            "models": by_model,
        }
