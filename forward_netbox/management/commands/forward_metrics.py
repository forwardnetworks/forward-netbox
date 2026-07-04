import logging

from core.choices import JobStatusChoices
from core.models import Job
from django.contrib.contenttypes.models import ContentType
from django.core.management.base import BaseCommand
from django.utils import timezone

from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.job_liveness import job_has_live_execution

logger = logging.getLogger("forward_netbox.metrics")

ACTIVE_JOB_STATUSES = (
    JobStatusChoices.STATUS_PENDING,
    JobStatusChoices.STATUS_RUNNING,
)


def _metric(lines, name, help_text, value, mtype="gauge", labels=""):
    lines.append(f"# HELP {name} {help_text}")
    lines.append(f"# TYPE {name} {mtype}")
    label_block = f"{{{labels}}}" if labels else ""
    lines.append(f"{name}{label_block} {value}")


class Command(BaseCommand):
    help = (
        "Emit forward_netbox metrics in Prometheus text-exposition format on "
        "stdout, for a node_exporter textfile collector or a scrape sidecar. "
        "Read-only; safe to run on a schedule."
    )

    def handle(self, *args, **options):
        lines = []

        _metric(
            lines,
            "forward_sources_total",
            "Configured Forward sources.",
            ForwardSource.objects.count(),
        )
        _metric(
            lines,
            "forward_syncs_total",
            "Configured Forward syncs.",
            ForwardSync.objects.count(),
        )
        _metric(
            lines,
            "forward_ingestions_total",
            "Recorded Forward ingestion runs.",
            ForwardIngestion.objects.count(),
        )

        forward_type_ids = list(
            ContentType.objects.filter(app_label="forward_netbox").values_list(
                "pk", flat=True
            )
        )
        jobs = Job.objects.filter(object_type_id__in=forward_type_ids)

        status_counts = {}
        for status, _label in JobStatusChoices.CHOICES:
            status_counts[status] = jobs.filter(status=status).count()
        lines.append("# HELP forward_jobs Forward background jobs by status.")
        lines.append("# TYPE forward_jobs gauge")
        for status, count in status_counts.items():
            lines.append(f'forward_jobs{{status="{status}"}} {count}')

        active = jobs.filter(status__in=ACTIVE_JOB_STATUSES)
        stuck = sum(1 for job in active if not job_has_live_execution(job))
        _metric(
            lines,
            "forward_stuck_jobs",
            "Active jobs with no live worker execution (wedged).",
            stuck,
        )

        last_completed = (
            jobs.filter(status=JobStatusChoices.STATUS_COMPLETED)
            .exclude(completed__isnull=True)
            .order_by("-completed")
            .first()
        )
        if last_completed and last_completed.completed:
            _metric(
                lines,
                "forward_last_completed_job_timestamp_seconds",
                "Unix time of the most recent completed Forward job.",
                int(last_completed.completed.timestamp()),
            )
            _metric(
                lines,
                "forward_last_completed_job_age_seconds",
                "Seconds since the most recent completed Forward job.",
                int((timezone.now() - last_completed.completed).total_seconds()),
            )

        self.stdout.write("\n".join(lines) + "\n")
