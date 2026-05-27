import json
import time

from core.models import Job
from django.contrib.contenttypes.models import ContentType
from django.core.management.base import BaseCommand
from django.core.management.base import CommandError
from django.utils import timezone

from forward_netbox.models import ForwardExecutionRunStatusChoices
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.execution_ledger import active_execution_run
from forward_netbox.utilities.execution_ledger import latest_execution_run
from forward_netbox.utilities.execution_ledger import reconcile_execution_run
from forward_netbox.utilities.ingestion_issues import blocking_issues_queryset
from forward_netbox.utilities.job_liveness import job_has_live_execution
from forward_netbox.utilities.resumable_branching import enqueue_branch_stage_job


TERMINAL_SYNC_STATUSES = {"completed", "failed", "ready_to_merge"}
TERMINAL_RUN_STATUSES = {
    ForwardExecutionRunStatusChoices.COMPLETED,
    ForwardExecutionRunStatusChoices.FAILED,
    ForwardExecutionRunStatusChoices.TIMEOUT,
    ForwardExecutionRunStatusChoices.CANCELLED,
}


class Command(BaseCommand):
    help = "Poll a Forward sync until completion and summarize blocker state."

    def add_arguments(self, parser):
        parser.add_argument("--sync-id", type=int, default=0)
        parser.add_argument("--sync-name", default="")
        parser.add_argument(
            "--interval-seconds",
            type=int,
            default=30,
            help="Polling interval in seconds.",
        )
        parser.add_argument(
            "--max-polls",
            type=int,
            default=0,
            help="Maximum polls before stopping; 0 means unlimited.",
        )
        parser.add_argument(
            "--fail-on-blocking",
            action="store_true",
            help="Exit non-zero if latest ingestion has blocking issues.",
        )
        parser.add_argument(
            "--fail-on-failed-changes",
            action="store_true",
            help="Exit non-zero if latest ingestion has failed changes.",
        )
        parser.add_argument(
            "--allow-nonterminal",
            action="store_true",
            help=(
                "Exit successfully when --max-polls is reached even if the sync "
                "is still running."
            ),
        )

    def handle(self, *args, **options):
        sync = self._resolve_sync(options)
        interval_seconds = max(1, int(options.get("interval_seconds") or 30))
        max_polls = max(0, int(options.get("max_polls") or 0))
        poll_count = 0

        while True:
            poll_count += 1
            sync.refresh_from_db()
            ingestion = (
                ForwardIngestion.objects.filter(sync=sync).order_by("-id").first()
            )
            run = self._current_execution_run(sync)
            summary = self._summary(sync, ingestion, run, poll_count=poll_count)
            self.stdout.write(json.dumps(summary, indent=2, default=str))

            if self._is_terminal(sync, run):
                self._enforce(summary, options)
                return

            if max_polls and poll_count >= max_polls:
                if options.get("allow_nonterminal"):
                    return
                raise CommandError(
                    f"Reached max polls ({max_polls}) before terminal sync status."
                )

            time.sleep(interval_seconds)

    def _resolve_sync(self, options):
        sync_id = int(options.get("sync_id") or 0)
        sync_name = (options.get("sync_name") or "").strip()
        if sync_id and sync_name:
            raise CommandError("Use either --sync-id or --sync-name, not both.")
        if sync_id:
            sync = ForwardSync.objects.filter(pk=sync_id).first()
            if sync is None:
                raise CommandError(f"Forward sync `{sync_id}` was not found.")
            return sync
        if sync_name:
            sync = ForwardSync.objects.filter(name=sync_name).first()
            if sync is None:
                raise CommandError(f"Forward sync `{sync_name}` was not found.")
            return sync
        sync = ForwardSync.objects.order_by("-id").first()
        if sync is None:
            raise CommandError("No Forward sync exists.")
        return sync

    def _summary(self, sync, ingestion, run, *, poll_count):
        if run is not None and self._run_needs_reconcile(run):
            reconcile_execution_run(run)
            run.refresh_from_db()
            if ingestion is not None:
                ingestion.refresh_from_db()
        if run is not None and self._run_needs_stage_enqueue(run):
            enqueued_job = enqueue_branch_stage_job(sync, user=None, adhoc=True)
            if enqueued_job is not None:
                sync.refresh_from_db()
                run.refresh_from_db()
                if ingestion is not None:
                    ingestion.refresh_from_db()
        active_step = self._active_step_for_run(run)
        active_step_job = self._active_step_job(active_step)
        sync_job = self._latest_sync_job(sync)
        selected_job = active_step_job or sync_job
        if ingestion is None:
            return {
                "poll_count": poll_count,
                "sync_id": sync.pk,
                "sync_name": sync.name,
                "sync_status": sync.status,
                "execution_run": self._execution_run_summary(
                    run, active_step=active_step
                ),
                "job": self._job_summary(selected_job),
                "ingestion": None,
            }
        job = active_step_job or ingestion.job or sync_job
        blocking = blocking_issues_queryset(ingestion)
        return {
            "poll_count": poll_count,
            "sync_id": sync.pk,
            "sync_name": sync.name,
            "sync_status": sync.status,
            "last_synced": sync.last_synced,
            "execution_run": self._execution_run_summary(run, active_step=active_step),
            "job": self._job_summary(job),
            "ingestion": {
                "id": ingestion.pk,
                "sync_mode": ingestion.sync_mode,
                "snapshot_id": ingestion.snapshot_id,
                "baseline_ready": bool(ingestion.baseline_ready),
                "applied": int(ingestion.applied_change_count or 0),
                "created": int(ingestion.created_change_count or 0),
                "updated": int(ingestion.updated_change_count or 0),
                "deleted": int(ingestion.deleted_change_count or 0),
                "failed": int(ingestion.failed_change_count or 0),
                "issue_count": ingestion.issues.count(),
                "blocking_issue_count": blocking.count(),
            },
        }

    def _enforce(self, summary, options):
        ingestion = summary.get("ingestion") or {}
        sync_status = summary.get("sync_status")
        run_status = (summary.get("execution_run") or {}).get("status")
        if sync_status == "failed":
            raise CommandError("Sync ended in failed status.")
        if run_status in {
            ForwardExecutionRunStatusChoices.FAILED,
            ForwardExecutionRunStatusChoices.TIMEOUT,
            ForwardExecutionRunStatusChoices.CANCELLED,
        }:
            raise CommandError(f"Execution run ended in {run_status} status.")
        if options.get("fail_on_failed_changes") and ingestion.get("failed", 0) > 0:
            raise CommandError(
                f"Ingestion {ingestion.get('id')} has failed changes ({ingestion.get('failed')})."
            )
        if (
            options.get("fail_on_blocking")
            and ingestion.get("blocking_issue_count", 0) > 0
        ):
            raise CommandError(
                f"Ingestion {ingestion.get('id')} has blocking issues ({ingestion.get('blocking_issue_count')})."
            )

    def _current_execution_run(self, sync):
        run = active_execution_run(sync)
        if run is not None:
            return run
        return latest_execution_run(sync)

    def _is_terminal(self, sync, run):
        if run is not None and run.status not in TERMINAL_RUN_STATUSES:
            return False
        return sync.status in TERMINAL_SYNC_STATUSES

    def _execution_run_summary(self, run, *, active_step=None):
        if run is None:
            return None
        step = active_step or self._active_step_for_run(run)
        heartbeat_age = None
        if run.latest_heartbeat is not None:
            heartbeat_age = max(
                0.0, (timezone.now() - run.latest_heartbeat).total_seconds()
            )
        step_heartbeat_age = None
        if step is not None and step.heartbeat is not None:
            step_heartbeat_age = max(
                0.0, (timezone.now() - step.heartbeat).total_seconds()
            )
        step_created_age = None
        if step is not None and getattr(step, "created", None) is not None:
            step_created_age = max(0.0, (timezone.now() - step.created).total_seconds())
        step_started_age = None
        if step is not None and getattr(step, "started", None) is not None:
            step_started_age = max(0.0, (timezone.now() - step.started).total_seconds())
        step_job = self._active_step_job(step)
        step_job_live = (
            job_has_live_execution(step_job) if step_job is not None else None
        )
        return {
            "id": run.pk,
            "status": run.status,
            "phase": run.phase,
            "phase_message": run.phase_message,
            "next_step_index": run.next_step_index,
            "total_steps": run.total_steps,
            "latest_heartbeat": run.latest_heartbeat,
            "latest_heartbeat_age_seconds": heartbeat_age,
            "active_step": {
                "id": step.pk if step is not None else None,
                "index": step.index if step is not None else None,
                "status": step.status if step is not None else "",
                "model": step.model_string if step is not None else "",
                "created": step.created if step is not None else None,
                "created_age_seconds": step_created_age,
                "started": step.started if step is not None else None,
                "started_age_seconds": step_started_age,
                "job_id": step.job_id if step is not None else None,
                "job_live": step_job_live,
                "heartbeat": step.heartbeat if step is not None else None,
                "heartbeat_age_seconds": step_heartbeat_age,
                "fetched_row_count": (
                    step.fetched_row_count if step is not None else None
                ),
                "attempted_row_count": (
                    step.attempted_row_count if step is not None else None
                ),
                "applied_row_count": (
                    step.applied_row_count if step is not None else None
                ),
                "last_error": step.last_error if step is not None else "",
            },
        }

    def _active_step_for_run(self, run):
        if run is None:
            return None
        step = (
            run.steps.filter(kind="stage", index=int(run.next_step_index or 1))
            .order_by("index")
            .first()
        )
        if step is not None:
            return step
        return (
            run.steps.filter(kind="stage", status="running").order_by("index").first()
        )

    def _active_step_job(self, step):
        if step is None:
            return None
        if step.status == "merge_queued" and step.merge_job is not None:
            return step.merge_job
        return step.job

    def _run_needs_reconcile(self, run):
        step = self._active_step_for_run(run)
        if step is None:
            return False
        if step.status not in {"queued", "running", "merge_queued"}:
            return False
        job = self._active_step_job(step)
        if job is None:
            return False
        return not job_has_live_execution(job)

    def _run_needs_stage_enqueue(self, run):
        if run.status in TERMINAL_RUN_STATUSES:
            return False
        step = self._active_step_for_run(run)
        if step is None:
            return False
        if step.status != "pending":
            return False
        if step.job_id is not None or step.merge_job_id is not None:
            return False
        inflight_exists = run.steps.filter(
            kind="stage",
            status__in={"queued", "running", "merge_queued", "staged"},
        ).exists()
        if inflight_exists:
            return False
        return True

    def _latest_sync_job(self, sync):
        content_type = ContentType.objects.get_for_model(ForwardSync)
        return (
            Job.objects.filter(
                object_type=content_type,
                object_id=sync.pk,
            )
            .order_by("-id")
            .first()
        )

    def _job_summary(self, job):
        latest_log_entry = None
        latest_log_age_seconds = None
        if job is not None:
            job_logs = job.log_entries or []
            if job_logs:
                latest_log_entry = job_logs[-1]
                timestamp = latest_log_entry.get("timestamp")
                if timestamp is not None:
                    latest_log_age_seconds = max(
                        0.0, (timezone.now() - timestamp).total_seconds()
                    )
        return {
            "id": job.pk if job is not None else None,
            "status": job.status if job is not None else "",
            "started": job.started if job is not None else None,
            "completed": job.completed if job is not None else None,
            "latest_log": latest_log_entry,
            "latest_log_age_seconds": latest_log_age_seconds,
        }
