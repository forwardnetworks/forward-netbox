import json
import logging

from django.core.management.base import BaseCommand
from django.core.management.base import CommandError

from forward_netbox.models import ForwardSync
from forward_netbox.utilities.scope_reconciliation import compute_scope_reconciliation
from forward_netbox.utilities.scope_reconciliation import tag_backfilled_devices

logger = logging.getLogger("forward_netbox.collection_gap")


class Command(BaseCommand):
    help = (
        "Proactive collection-gap alert. Computes the backfilled (tagged in scope "
        "but not freshly collected) device count for a sync, optionally refreshes "
        "the forward-backfilled tag, and warns/exits non-zero when the count "
        "crosses a threshold. Schedule it (cron / NetBox script) to be alerted to "
        "a Forward collection problem without polling the health page."
    )

    def add_arguments(self, parser):
        parser.add_argument("--sync-id", type=int, default=0)
        parser.add_argument("--sync-name", default="")
        parser.add_argument(
            "--threshold",
            type=int,
            default=0,
            help="Backfilled-count threshold; a count above this is a breach.",
        )
        parser.add_argument(
            "--tag",
            action="store_true",
            help="Also refresh the forward-backfilled device tag.",
        )
        parser.add_argument(
            "--fail-on-breach",
            action="store_true",
            help="Exit non-zero when the backfilled count exceeds the threshold.",
        )

    def handle(self, *args, **options):
        if options.get("sync_id") and options.get("sync_name"):
            raise CommandError("Use either --sync-id or --sync-name, not both.")
        sync = self._resolve_sync(options)
        if sync is None:
            raise CommandError("No sync found for the requested selector.")
        if not sync.get_network_id():
            raise CommandError("Sync source has no network configured.")

        report = compute_scope_reconciliation(sync)
        backfilled = int(report["netbox_present_backfilled"])
        threshold = int(options["threshold"])
        breached = backfilled > threshold

        payload = {
            "sync_id": sync.pk,
            "sync_name": sync.name,
            "backfilled_count": backfilled,
            "threshold": threshold,
            "breached": breached,
            "present_backfilled_sample": report["present_backfilled_sample"],
        }

        if options["tag"]:
            payload["tag_result"] = tag_backfilled_devices(sync, report=report)

        if breached:
            message = (
                f"Collection gap on sync '{sync.name}': {backfilled} device(s) "
                f"backfilled (threshold {threshold}). Investigate Forward collection."
            )
            payload["alert"] = message
            logger.warning(message)

        self.stdout.write(json.dumps(payload, indent=2, default=str))

        if breached and options["fail_on_breach"]:
            raise SystemExit(1)

    def _resolve_sync(self, options):
        sync_id = int(options.get("sync_id") or 0)
        sync_name = (options.get("sync_name") or "").strip()
        if sync_id:
            return ForwardSync.objects.filter(pk=sync_id).first()
        if sync_name:
            return ForwardSync.objects.filter(name=sync_name).first()
        return ForwardSync.objects.order_by("-id").first()
