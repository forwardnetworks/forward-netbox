import json

from django.core.management.base import BaseCommand
from django.core.management.base import CommandError

from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.ingestion_issues import blocking_issues_queryset


class Command(BaseCommand):
    help = "Summarize ingestion issues by blocking vs non-blocking classification."

    def add_arguments(self, parser):
        parser.add_argument(
            "--sync-id",
            type=int,
            default=0,
            help="ForwardSync primary key; audits latest ingestion for that sync.",
        )
        parser.add_argument(
            "--sync-name",
            default="",
            help="ForwardSync name; audits latest ingestion for that sync.",
        )
        parser.add_argument(
            "--ingestion-id",
            type=int,
            default=0,
            help="Specific ForwardIngestion primary key to audit.",
        )
        parser.add_argument(
            "--fail-on-blocking",
            action="store_true",
            help="Exit non-zero when blocking issues are present.",
        )

    def handle(self, *args, **options):
        if options["sync_id"] and options["sync_name"]:
            raise CommandError("Use either --sync-id or --sync-name, not both.")

        ingestion = self._resolve_ingestion(options)
        if ingestion is None:
            raise CommandError("No ingestion found for the requested selector.")

        all_issues = ingestion.issues.all()
        blocking_issues = blocking_issues_queryset(ingestion)
        blocking_ids = set(blocking_issues.values_list("pk", flat=True))
        non_blocking_issues = all_issues.exclude(pk__in=blocking_ids)

        payload = {
            "ingestion_id": ingestion.pk,
            "sync_id": ingestion.sync_id,
            "sync_name": ingestion.sync.name,
            "snapshot_id": ingestion.snapshot_id,
            "sync_mode": ingestion.sync_mode,
            "baseline_ready": bool(ingestion.baseline_ready),
            "counts": {
                "total": all_issues.count(),
                "blocking": len(blocking_ids),
                "non_blocking": non_blocking_issues.count(),
            },
            "blocking_samples": list(
                blocking_issues.order_by("-id").values(
                    "timestamp",
                    "phase",
                    "model",
                    "exception",
                    "message",
                )[:10]
            ),
            "non_blocking_samples": list(
                non_blocking_issues.order_by("-id").values(
                    "timestamp",
                    "phase",
                    "model",
                    "exception",
                    "message",
                )[:10]
            ),
        }

        self.stdout.write(json.dumps(payload, indent=2, default=str))

        if options["fail_on_blocking"] and blocking_ids:
            raise CommandError(
                f"Ingestion {ingestion.pk} has {len(blocking_ids)} blocking issue(s)."
            )

    def _resolve_ingestion(self, options):
        ingestion_id = int(options.get("ingestion_id") or 0)
        if ingestion_id:
            return ForwardIngestion.objects.filter(pk=ingestion_id).first()

        sync_id = int(options.get("sync_id") or 0)
        sync_name = (options.get("sync_name") or "").strip()

        sync = None
        if sync_id:
            sync = ForwardSync.objects.filter(pk=sync_id).first()
        elif sync_name:
            sync = ForwardSync.objects.filter(name=sync_name).first()
        else:
            sync = ForwardSync.objects.order_by("-id").first()

        if sync is None:
            return None
        return ForwardIngestion.objects.filter(sync=sync).order_by("-id").first()
