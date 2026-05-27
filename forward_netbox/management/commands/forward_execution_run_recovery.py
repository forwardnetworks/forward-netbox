import json
from pathlib import Path

from django.core.management.base import BaseCommand
from django.core.management.base import CommandError

from forward_netbox.choices import ForwardExecutionStepKindChoices
from forward_netbox.models import ForwardExecutionRun
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.execution_ledger import execution_run_support_bundle
from forward_netbox.utilities.execution_ledger import latest_execution_run
from forward_netbox.utilities.execution_ledger import reconcile_execution_run
from forward_netbox.utilities.ingestion_merge import maybe_enqueue_next_branch_stage
from forward_netbox.utilities.resumable_branching import enqueue_branch_stage_job


class Command(BaseCommand):
    help = (
        "Inspect, reconcile, and optionally resume a Branching execution run "
        "through the native execution ledger and NetBox job queue."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--run-id",
            default="",
            help="Specific ForwardExecutionRun primary key to inspect.",
        )
        parser.add_argument(
            "--sync-name",
            default="",
            help="ForwardSync name whose latest execution run should be inspected.",
        )
        parser.add_argument(
            "--skip-reconcile",
            action="store_true",
            help="Report current state without reconciling stale ledger/job state.",
        )
        parser.add_argument(
            "--enqueue-next",
            action="store_true",
            help=(
                "After reconciliation, enqueue the next eligible shard through "
                "the native Branching stage job path."
            ),
        )
        parser.add_argument(
            "--output-json",
            default="",
            help="Optional path to write report JSON.",
        )

    def handle(self, *args, **options):
        run = self._select_run(options)
        reconcile_result = None
        if not options.get("skip_reconcile"):
            reconcile_result = reconcile_execution_run(run)
            run.refresh_from_db()

        enqueued_job = None
        if options.get("enqueue_next"):
            recommendation = (
                execution_run_support_bundle(run).get("recovery_recommendation") or {}
            )
            enqueued_job = _enqueue_recovery_job(run, recommendation)
            run.refresh_from_db()

        bundle = execution_run_support_bundle(run)
        report = {
            "run": _run_summary(run),
            "recovery_recommendation": bundle.get("recovery_recommendation") or {},
            "next_step": _next_step_summary(run),
            "reconcile": _reconcile_summary(reconcile_result),
            "enqueued_job": _job_summary(enqueued_job),
        }
        rendered = json.dumps(report, indent=2, sort_keys=True, default=str)
        self.stdout.write(rendered)

        output_json = (options.get("output_json") or "").strip()
        if output_json:
            output_file = Path(output_json)
            if not output_file.is_absolute():
                output_file = Path(__file__).resolve().parents[3] / output_file
            output_file.parent.mkdir(parents=True, exist_ok=True)
            with open(output_file, "w", encoding="utf-8") as handle:
                handle.write(rendered + "\n")
            output_file.chmod(0o666)
            self.stdout.write(
                self.style.SUCCESS(
                    f"Wrote execution-run recovery report to {output_json}"
                )
            )

    def _select_run(self, options):
        run_id = (options.get("run_id") or "").strip()
        sync_name = (options.get("sync_name") or "").strip()
        if bool(run_id) == bool(sync_name):
            raise CommandError("Provide exactly one of --run-id or --sync-name.")
        if run_id:
            run = ForwardExecutionRun.objects.filter(pk=run_id).first()
            if run is None:
                raise CommandError(f"Forward execution run `{run_id}` was not found.")
            return run
        sync = ForwardSync.objects.filter(name=sync_name).first()
        if sync is None:
            raise CommandError(f"Forward sync `{sync_name}` was not found.")
        run = latest_execution_run(sync)
        if run is None:
            raise CommandError(
                f"Forward sync `{sync_name}` has no execution runs to inspect."
            )
        return run


def _run_summary(run):
    return {
        "id": run.pk,
        "sync": run.sync_id,
        "source": run.source_id,
        "backend": run.backend,
        "status": run.status,
        "phase": run.phase,
        "total_steps": int(run.total_steps or 0),
        "next_step_index": int(run.next_step_index or 0),
        "auto_merge": bool(run.auto_merge),
        "baseline_ready": bool(run.baseline_ready),
        "latest_heartbeat": (
            run.latest_heartbeat.isoformat() if run.latest_heartbeat else None
        ),
        "completed": run.completed.isoformat() if run.completed else None,
        "reconciliation_event_count": len(run.reconciliation_events or []),
        "last_error_present": bool(run.last_error),
        "last_error_length": len(run.last_error or ""),
    }


def _next_step_summary(run):
    step = (
        run.steps.filter(
            kind=ForwardExecutionStepKindChoices.STAGE,
            index=int(run.next_step_index or 0),
        )
        .order_by("pk")
        .first()
    )
    if step is None:
        return None
    return {
        "id": step.pk,
        "index": step.index,
        "kind": step.kind,
        "status": step.status,
        "model": step.model_string,
        "job": step.job_id,
        "merge_job": step.merge_job_id,
        "ingestion": step.ingestion_id,
        "branch": step.branch_id,
        "retry_count": int(step.retry_count or 0),
        "estimated_changes": int(step.estimated_changes or 0),
        "actual_changes": int(step.actual_changes or 0),
        "attempted_row_count": int(step.attempted_row_count or 0),
        "applied_row_count": int(step.applied_row_count or 0),
        "skipped_row_count": int(step.skipped_row_count or 0),
        "failed_row_count": int(step.failed_row_count or 0),
        "fetch_mode": step.fetch_mode or "",
        "fetch_column_filter_count": len(step.fetch_column_filters or []),
        "shard_key_count": len(step.shard_keys or []),
        "last_error_present": bool(step.last_error),
        "last_error_length": len(step.last_error or ""),
    }


def _reconcile_summary(result):
    if result is None:
        return {"skipped": True}
    return {
        "skipped": False,
        "updated_steps": int(result.get("updated_steps") or 0),
        "updated_run": bool(result.get("updated_run")),
        "messages": list(result.get("messages") or []),
    }


def _job_summary(job):
    if job is None:
        return None
    return {
        "id": getattr(job, "pk", None),
        "name": getattr(job, "name", ""),
        "status": getattr(job, "status", ""),
    }


def _enqueue_recovery_job(run, recommendation):
    action = str((recommendation or {}).get("action") or "").strip()
    if bool(run.auto_merge):
        step = (
            run.steps.filter(
                kind=ForwardExecutionStepKindChoices.STAGE,
                index=int(run.next_step_index or 0),
            )
            .select_related("ingestion")
            .order_by("pk")
            .first()
        )
        if step is not None and step.ingestion is not None:
            recovered_job = maybe_enqueue_next_branch_stage(
                step.ingestion,
                user=None,
                allow_failed_recovery=True,
            )
            if recovered_job is not None:
                return recovered_job
    if action in {"requeue_merge", "wait_for_review"}:
        return None
    return enqueue_branch_stage_job(run.sync, user=None, adhoc=True)
