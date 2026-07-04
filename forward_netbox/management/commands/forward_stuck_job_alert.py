import json
import logging

from core.choices import JobStatusChoices
from core.models import Job
from django.contrib.contenttypes.models import ContentType
from django.core.management.base import BaseCommand

from forward_netbox.utilities.job_liveness import job_has_live_execution

logger = logging.getLogger("forward_netbox.stuck_job")

# NetBox statuses that mean the job should still be making progress.
ACTIVE_JOB_STATUSES = (
    JobStatusChoices.STATUS_PENDING,
    JobStatusChoices.STATUS_RUNNING,
)


class Command(BaseCommand):
    help = (
        "Detect wedged forward_netbox background jobs: rows whose status is still "
        "PENDING/RUNNING but which have no live RQ execution (e.g. a worker died "
        "or the started-job heartbeat went stale). Schedule it (cron / NetBox "
        "script) to be alerted to a stuck sync without watching the job list."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--fail-on-stuck",
            action="store_true",
            help="Exit non-zero when at least one stuck job is found.",
        )

    def handle(self, *args, **options):
        forward_type_ids = list(
            ContentType.objects.filter(app_label="forward_netbox").values_list(
                "pk", flat=True
            )
        )
        candidates = Job.objects.filter(
            status__in=ACTIVE_JOB_STATUSES,
            object_type_id__in=forward_type_ids,
        ).order_by("pk")

        stuck = [job for job in candidates if not job_has_live_execution(job)]

        payload = {
            "active_job_count": candidates.count(),
            "stuck_job_count": len(stuck),
            "stuck_jobs": [
                {
                    "job_id": job.pk,
                    "name": job.name,
                    "status": job.status,
                    "object_type": str(job.object_type),
                    "object_id": job.object_id,
                    "created": str(getattr(job, "created", "")),
                }
                for job in stuck
            ],
        }

        if stuck:
            message = (
                f"{len(stuck)} forward_netbox job(s) are wedged (DB-active but no "
                "live worker execution); a sync may be stuck. Job IDs: "
                + ", ".join(str(job.pk) for job in stuck)
            )
            payload["alert"] = message
            logger.warning(message)

        self.stdout.write(json.dumps(payload, indent=2, default=str))

        if stuck and options["fail_on_stuck"]:
            raise SystemExit(1)
