import json
from collections import Counter

from core.models import Job
from django.contrib.contenttypes.models import ContentType
from django.core.management.base import BaseCommand
from django.core.management.base import CommandError

from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSync


def _job_log_entries(job):
    data = getattr(job, "data", {}) or {}
    serialized_logs = data.get("logs") or []
    if serialized_logs:
        for entry in serialized_logs:
            if not isinstance(entry, list) or len(entry) < 5:
                continue
            yield str(entry[1] or "").lower(), str(entry[4] or "")
        return
    for entry in getattr(job, "log_entries", None) or []:
        if isinstance(entry, dict):
            yield (
                str(entry.get("level") or "").lower(),
                str(entry.get("message") or ""),
            )


class Command(BaseCommand):
    help = "Summarize warning and error logs for Forward single-branch jobs."

    def add_arguments(self, parser):
        parser.add_argument("--sync-id", type=int, default=0)
        parser.add_argument("--sync-name", default="")
        parser.add_argument("--all-ingestions", action="store_true")
        parser.add_argument("--top", type=int, default=20)
        parser.add_argument("--fail-on-warning", action="store_true")
        parser.add_argument("--fail-on-error", action="store_true")

    def handle(self, *args, **options):
        sync = self._resolve_sync(options)
        top = max(1, int(options.get("top") or 20))
        ingestions = ForwardIngestion.objects.filter(sync=sync).order_by("-id")
        if not options.get("all_ingestions"):
            ingestions = ingestions[:1]
        ingestions = list(ingestions)

        jobs = []
        seen_job_ids = set()
        for ingestion in ingestions:
            for job in (ingestion.job, ingestion.merge_job):
                if job is not None and job.pk not in seen_job_ids:
                    seen_job_ids.add(job.pk)
                    jobs.append(job)
        for job in self._sync_jobs(
            sync,
            overlays_only=bool(ingestions),
            ingestions=ingestions,
        ):
            if job.pk not in seen_job_ids:
                seen_job_ids.add(job.pk)
                jobs.append(job)
        if not jobs:
            raise CommandError("No Forward sync jobs were found for this sync.")

        levels = Counter()
        warnings = Counter()
        errors = Counter()
        for job in jobs:
            for level, message in _job_log_entries(job):
                levels[level] += 1
                if level == "warning":
                    warnings[message] += 1
                if level in {"failure", "error", "critical"}:
                    errors[message] += 1

        payload = {
            "sync_id": sync.pk,
            "sync_name": sync.name,
            "ingestion_ids": [ingestion.pk for ingestion in ingestions],
            "job_ids": sorted(seen_job_ids),
            "levels": dict(levels),
            "warning_count": int(levels.get("warning", 0)),
            "error_count": int(
                levels.get("failure", 0)
                + levels.get("error", 0)
                + levels.get("critical", 0)
            ),
            "top_warnings": [
                {"count": count, "message": message}
                for message, count in warnings.most_common(top)
            ],
            "top_errors": [
                {"count": count, "message": message}
                for message, count in errors.most_common(top)
            ],
        }
        self.stdout.write(json.dumps(payload, indent=2, sort_keys=True))

        if options.get("fail_on_error") and payload["error_count"]:
            raise CommandError("Forward warning audit found one or more errors.")
        if options.get("fail_on_warning") and payload["warning_count"]:
            raise CommandError("Forward warning audit found one or more warnings.")

    def _resolve_sync(self, options):
        sync_id = int(options.get("sync_id") or 0)
        sync_name = (options.get("sync_name") or "").strip()
        if bool(sync_id) == bool(sync_name):
            raise CommandError("Provide exactly one of --sync-id or --sync-name.")
        if sync_id:
            sync = ForwardSync.objects.filter(pk=sync_id).first()
        else:
            sync = ForwardSync.objects.filter(name=sync_name).first()
        if sync is None:
            raise CommandError("Forward sync was not found.")
        return sync

    def _sync_jobs(self, sync, *, overlays_only=False, ingestions=None):
        content_type = ContentType.objects.get_for_model(ForwardSync)
        jobs = Job.objects.filter(object_type=content_type, object_id=sync.pk)
        if overlays_only:
            jobs = jobs.filter(
                name__in=[
                    f"{sync.name} - refresh device analysis (auto)",
                    f"{sync.name} - reconcile device scope tags (auto)",
                    f"{sync.name} - clear stale managed scope tags (auto)",
                    f"{sync.name} - link vsys/vdom parents (auto)",
                ]
            )
            jobs = self._latest_overlay_attempts(sync, jobs, ingestions or [])
        return list(jobs.order_by("-id"))

    def _latest_overlay_attempts(self, sync, jobs, ingestions):
        selected_ids = {ingestion.pk for ingestion in ingestions}
        timeline = list(
            ForwardIngestion.objects.filter(sync=sync).order_by("created", "id")
        )
        latest_by_generation_and_name = {}
        for job in jobs.order_by("created", "id"):
            generation = (job.data or {}).get("forward_ingestion_id")
            if generation is None:
                generation = self._generation_at_job_time(job, timeline)
            try:
                generation = int(generation)
            except (TypeError, ValueError):
                continue
            if generation not in selected_ids:
                continue
            latest_by_generation_and_name[(generation, job.name)] = job.pk
        return Job.objects.filter(pk__in=latest_by_generation_and_name.values())

    @staticmethod
    def _generation_at_job_time(job, timeline):
        generation = None
        for ingestion in timeline:
            if ingestion.created > job.created:
                break
            generation = ingestion.pk
        return generation
