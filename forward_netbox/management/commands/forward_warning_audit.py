import json
from collections import Counter

from core.models import Job
from django.contrib.contenttypes.models import ContentType
from django.core.management.base import BaseCommand
from django.core.management.base import CommandError
from django.db.models import Q

from ...models import ForwardIngestion
from ...models import ForwardSync
from ...utilities.execution_ledger import active_execution_run
from ...utilities.execution_ledger import latest_execution_run


NON_ACTIONABLE_WARNING_PREFIXES = (
    "Forward diffs require a newer processed snapshot than the latest baseline;",
    "Merge job hit a transient Branching readiness guard; attempting automatic requeue.",
    "Execution context returned a different shard index than claimed; executing claimed shard ",
)


def _is_info_equivalent_warning(message):
    normalized = str(message or "").strip()
    if not normalized:
        return False
    if (
        "partition fetch failed; retrying as " in normalized
        and "smaller partition(s):" in normalized
        and "transient HTTP" in normalized
    ):
        return True
    if (
        "single-value partition fetch failed; retrying with alternate "
        "column-filter operator before full fallback:" in normalized
        and "transient HTTP" in normalized
    ):
        return True
    return False


def _is_non_actionable_warning(message):
    normalized = str(message or "").strip()
    if not normalized:
        return False
    return any(
        normalized.startswith(prefix) for prefix in NON_ACTIONABLE_WARNING_PREFIXES
    )


def _job_log_entries(job):
    data = getattr(job, "data", {}) or {}
    logs = data.get("logs") or []
    emitted = 0
    for entry in logs:
        # NetBox job logs are stored as:
        # [timestamp, level, object, object_url, message]
        if not isinstance(entry, list) or len(entry) < 5:
            continue
        emitted += 1
        yield {
            "timestamp": entry[0],
            "level": str(entry[1] or "").lower(),
            "message": str(entry[4] or ""),
            "job_id": job.id,
        }
    if emitted:
        return

    # Running jobs persist incremental entries on `job.log_entries` before
    # final `job.data["logs"]` serialization. Use that as a fallback so warning
    # audits can inspect live shard activity.
    for entry in getattr(job, "log_entries", None) or []:
        if not isinstance(entry, dict):
            continue
        yield {
            "timestamp": entry.get("timestamp"),
            "level": str(entry.get("level") or "").lower(),
            "message": str(entry.get("message") or ""),
            "job_id": job.id,
        }


class Command(BaseCommand):
    help = "Summarize warning/error/info log levels for Forward sync jobs."

    def add_arguments(self, parser):
        parser.add_argument("--sync-id", type=int, default=0)
        parser.add_argument("--sync-name", default="")
        parser.add_argument("--all-ingestions", action="store_true")
        parser.add_argument("--top", type=int, default=20)
        parser.add_argument("--fail-on-warning", action="store_true")
        parser.add_argument("--fail-on-error", action="store_true")

    def _resolve_sync(self, *, sync_id, sync_name):
        if bool(sync_id) == bool(sync_name):
            raise CommandError("Provide exactly one of --sync-id or --sync-name.")
        if sync_id:
            sync = ForwardSync.objects.filter(pk=sync_id).first()
        else:
            sync = ForwardSync.objects.filter(name=sync_name).first()
        if sync is None:
            raise CommandError("Forward sync not found.")
        return sync

    def handle(self, *args, **options):
        sync = self._resolve_sync(
            sync_id=options.get("sync_id", 0),
            sync_name=(options.get("sync_name") or "").strip(),
        )
        top = max(1, int(options.get("top") or 20))

        ingestions_qs = ForwardIngestion.objects.filter(sync=sync).order_by("-id")
        if not options.get("all_ingestions"):
            ingestions_qs = ingestions_qs[:1]
        ingestions = list(ingestions_qs)
        scoped_ingestion_ids = {ingestion.id for ingestion in ingestions}
        execution_run_jobs = self._execution_run_jobs(
            sync,
            scoped_ingestion_ids=scoped_ingestion_ids,
            include_all_ingestions=bool(options.get("all_ingestions")),
        )
        sync_jobs = [] if ingestions else self._sync_jobs(sync)
        if not ingestions and not execution_run_jobs and not sync_jobs:
            raise CommandError(
                "No ingestions, execution-run jobs, or sync jobs found for this sync."
            )

        level_counts = Counter()
        warning_messages = Counter()
        suppressed_warning_messages = Counter()
        error_messages = Counter()
        scanned_job_ids = []
        scanned_jobs = []
        seen_job_ids = set()
        actionable_warning_count = 0
        suppressed_warning_count = 0

        for ingestion in ingestions:
            for job in (ingestion.job, ingestion.merge_job):
                if job is None or job.id in seen_job_ids:
                    continue
                seen_job_ids.add(job.id)
                scanned_jobs.append(job)

        for job in execution_run_jobs:
            if job is None or job.id in seen_job_ids:
                continue
            seen_job_ids.add(job.id)
            scanned_jobs.append(job)
        for job in sync_jobs:
            if job is None or job.id in seen_job_ids:
                continue
            seen_job_ids.add(job.id)
            scanned_jobs.append(job)

        for job in scanned_jobs:
            scanned_job_ids.append(job.id)
            for entry in _job_log_entries(job):
                level = entry["level"]
                message = entry["message"]
                if level == "warning" and _is_info_equivalent_warning(message):
                    level_counts["info"] += 1
                    continue
                level_counts[level] += 1
                if level == "warning":
                    if _is_non_actionable_warning(message):
                        suppressed_warning_count += 1
                        suppressed_warning_messages[message] += 1
                        continue
                    actionable_warning_count += 1
                    warning_messages[message] += 1
                if level in {"failure", "error", "critical"}:
                    error_messages[message] += 1

        payload = {
            "sync_id": sync.id,
            "sync_name": sync.name,
            "ingestion_ids": [ing.id for ing in ingestions],
            "job_ids": sorted(set(scanned_job_ids)),
            "execution_run_job_ids": sorted({job.id for job in execution_run_jobs}),
            "sync_job_ids": sorted({job.id for job in sync_jobs}),
            "levels": dict(level_counts),
            "warning_count": int(actionable_warning_count),
            "raw_warning_count": int(level_counts.get("warning", 0)),
            "suppressed_warning_count": int(suppressed_warning_count),
            "error_count": int(
                level_counts.get("failure", 0)
                + level_counts.get("error", 0)
                + level_counts.get("critical", 0)
            ),
            "top_warnings": [
                {"count": count, "message": message}
                for message, count in warning_messages.most_common(top)
            ],
            "top_errors": [
                {"count": count, "message": message}
                for message, count in error_messages.most_common(top)
            ],
            "top_suppressed_warnings": [
                {"count": count, "message": message}
                for message, count in suppressed_warning_messages.most_common(top)
            ],
        }

        self.stdout.write(json.dumps(payload, indent=2, sort_keys=True))

        if options.get("fail_on_error") and payload["error_count"] > 0:
            raise CommandError("Forward warning audit found one or more errors.")
        if options.get("fail_on_warning") and payload["warning_count"] > 0:
            raise CommandError("Forward warning audit found one or more warnings.")

    def _execution_run_jobs(
        self, sync, *, scoped_ingestion_ids=None, include_all_ingestions=False
    ):
        run = active_execution_run(sync)
        if run is None:
            run = latest_execution_run(sync)
        if run is None:
            return []
        scoped_ids = {int(value) for value in (scoped_ingestion_ids or set()) if value}
        active_statuses = {"queued", "running", "waiting"}
        jobs = []
        steps = run.steps.select_related("job", "merge_job").all()
        if not include_all_ingestions and scoped_ids:
            steps = steps.filter(
                Q(ingestion_id__in=scoped_ids)
                | Q(
                    ingestion_id__isnull=True,
                    status__in=active_statuses,
                )
            )
        for step in steps:
            if step.job is not None:
                jobs.append(step.job)
            if step.merge_job is not None:
                jobs.append(step.merge_job)
        return jobs

    def _sync_jobs(self, sync):
        content_type = ContentType.objects.get_for_model(ForwardSync)
        return list(
            Job.objects.filter(
                object_type=content_type,
                object_id=sync.pk,
            ).order_by("-id")
        )
