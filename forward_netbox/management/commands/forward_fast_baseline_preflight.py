import json

from core.exceptions import SyncError
from django.core.management.base import BaseCommand
from django.core.management.base import CommandError
from django.db import DatabaseError

from forward_netbox.exceptions import ForwardSyncError
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.fast_baseline import fast_baseline_preflight


class Command(BaseCommand):
    help = (
        "Read-only complete eligibility proof for the opted-in fast first "
        "baseline. Executes the configured Forward NQE workload but creates no "
        "ingestion, branch, audit, or target inventory rows."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--sync",
            required=True,
            help="ForwardSync primary key or exact name.",
        )
        parser.add_argument(
            "--fail-on-ineligible",
            action="store_true",
            help="Exit non-zero when any eligibility condition fails.",
        )

    def _resolve_sync(self, value):
        if str(value).isdigit():
            sync = ForwardSync.objects.filter(pk=int(value)).first()
            if sync is not None:
                return sync
        matches = list(ForwardSync.objects.filter(name=value)[:2])
        if len(matches) == 1:
            return matches[0]
        if not matches:
            raise CommandError("No Forward sync matched --sync.")
        raise CommandError(
            "Multiple Forward syncs matched --sync; use its primary key."
        )

    def handle(self, *args, **options):
        sync = self._resolve_sync(options["sync"])
        try:
            report = fast_baseline_preflight(sync=sync)
        except (DatabaseError, ForwardSyncError, RuntimeError, SyncError) as exc:
            report = {
                "eligible": False,
                "reason_code": "preflight_execution_failed",
                "context": {"error_type": type(exc).__name__},
                "workload_fetch_performed": True,
            }
        self.stdout.write(json.dumps(report, indent=2, sort_keys=True, default=str))
        if options["fail_on_ineligible"] and not report["eligible"]:
            raise CommandError(
                "Fast baseline is ineligible: " + str(report["reason_code"])
            )
