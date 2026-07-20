import json
import time

from core.models import Job
from django.contrib.contenttypes.models import ContentType
from django.core.management.base import BaseCommand
from django.core.management.base import CommandError
from netbox_branching.choices import BranchStatusChoices

from forward_netbox.choices import ForwardSyncStatusChoices
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.ingestion_issues import blocking_issues_queryset
from forward_netbox.utilities.ownership import ownership_finalization_summary


TERMINAL_SYNC_STATUSES = {
    ForwardSyncStatusChoices.COMPLETED,
    ForwardSyncStatusChoices.FAILED,
    ForwardSyncStatusChoices.READY_TO_MERGE,
    ForwardSyncStatusChoices.TIMEOUT,
}


def _job_summary(job):
    if job is None:
        return None
    return {
        "id": job.pk,
        "status": job.status,
        "created": job.created,
        "started": job.started,
        "completed": job.completed,
    }


class Command(BaseCommand):
    help = "Poll a Forward sync until terminal single-branch state."

    def add_arguments(self, parser):
        parser.add_argument("--sync-id", type=int, default=0)
        parser.add_argument("--sync-name", default="")
        parser.add_argument("--interval-seconds", type=int, default=30)
        parser.add_argument("--max-polls", type=int, default=0)
        parser.add_argument("--fail-on-blocking", action="store_true")
        parser.add_argument("--fail-on-failed-changes", action="store_true")
        parser.add_argument("--allow-nonterminal", action="store_true")
        parser.add_argument("--allow-ready-to-merge", action="store_true")

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
            summary = self._summary(sync, ingestion, poll_count=poll_count)
            self.stdout.write(json.dumps(summary, indent=2, default=str))

            if self._is_terminal(summary, options):
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
        elif sync_name:
            sync = ForwardSync.objects.filter(name=sync_name).first()
        else:
            sync = ForwardSync.objects.order_by("-id").first()
        if sync is None:
            raise CommandError("Forward sync was not found.")
        return sync

    def _summary(self, sync, ingestion, *, poll_count):
        sync_job = self._latest_sync_job(sync)
        ownership = ownership_finalization_summary(sync)
        if ingestion is None:
            return {
                "poll_count": poll_count,
                "sync_id": sync.pk,
                "sync_name": sync.name,
                "sync_status": sync.status,
                "job": _job_summary(sync_job),
                "ingestion": None,
                "ownership": ownership,
            }

        blocking = blocking_issues_queryset(ingestion)
        branch = ingestion.branch
        reconciliations = list(
            sync.ownership_reconciliations.order_by("domain").values(
                "domain", "ingestion_id", "snapshot_id", "status", "error_type"
            )
        )
        for reconciliation in reconciliations:
            reconciliation["generation"] = reconciliation.pop("ingestion_id")
        return {
            "poll_count": poll_count,
            "sync_id": sync.pk,
            "sync_name": sync.name,
            "sync_status": sync.status,
            "last_synced": sync.last_synced,
            "job": _job_summary(ingestion.job or sync_job),
            "merge_job": _job_summary(ingestion.merge_job),
            "branch": {
                "id": branch.pk if branch is not None else None,
                "name": branch.name if branch is not None else "",
                "status": branch.status if branch is not None else "",
            },
            "ownership_reconciliations": reconciliations,
            "ownership": ownership,
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

    def _is_terminal(self, summary, options):
        sync_status = summary.get("sync_status")
        if sync_status not in TERMINAL_SYNC_STATUSES:
            return False
        if sync_status == ForwardSyncStatusChoices.READY_TO_MERGE:
            return bool(options.get("allow_ready_to_merge"))
        if sync_status != ForwardSyncStatusChoices.COMPLETED:
            return True
        branch_status = (summary.get("branch") or {}).get("status")
        branch_terminal = not branch_status or branch_status in {
            BranchStatusChoices.MERGED,
            BranchStatusChoices.ARCHIVED,
        }
        return branch_terminal and bool(
            (summary.get("ownership") or {}).get("complete")
        )

    def _enforce(self, summary, options):
        ingestion = summary.get("ingestion") or {}
        sync_status = summary.get("sync_status")
        if sync_status in {
            ForwardSyncStatusChoices.FAILED,
            ForwardSyncStatusChoices.TIMEOUT,
        }:
            raise CommandError(f"Sync ended in {sync_status} status.")
        if sync_status == ForwardSyncStatusChoices.READY_TO_MERGE:
            if options.get("allow_ready_to_merge"):
                return
            raise CommandError("Sync is staged but has not been merged.")
        if sync_status != ForwardSyncStatusChoices.COMPLETED:
            raise CommandError(f"Sync did not complete (status={sync_status}).")
        if not ingestion.get("baseline_ready"):
            raise CommandError("Completed sync has no baseline-ready ingestion.")
        if not (summary.get("ownership") or {}).get("complete"):
            raise CommandError("Completed sync has unconverged ownership.")
        if options.get("fail_on_failed_changes") and ingestion.get("failed", 0) > 0:
            raise CommandError(
                f"Ingestion {ingestion.get('id')} has failed changes "
                f"({ingestion.get('failed')})."
            )
        if (
            options.get("fail_on_blocking")
            and ingestion.get("blocking_issue_count", 0) > 0
        ):
            raise CommandError(
                f"Ingestion {ingestion.get('id')} has blocking issues "
                f"({ingestion.get('blocking_issue_count')})."
            )

    def _latest_sync_job(self, sync):
        content_type = ContentType.objects.get_for_model(ForwardSync)
        return (
            Job.objects.filter(object_type=content_type, object_id=sync.pk)
            .order_by("-id")
            .first()
        )
