import json

from django.core.management.base import BaseCommand
from django.core.management.base import CommandError

from forward_netbox.exceptions import ForwardSyncError
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.apic_cimc_readiness import audit_apic_cimc_readiness

# The same report is a page on the sync (Audits > APIC CIMC readiness).


class Command(BaseCommand):
    help = (
        "Audit APIC CIMC inventory readiness for a sync: report whether the "
        "synced snapshot's APIC devices carry the controller-detail and "
        "`moquery -c eqptCh -a all` custom command the CIMC inventory map needs."
    )

    def add_arguments(self, parser):
        parser.add_argument("--sync-id", type=int, default=0)
        parser.add_argument("--sync-name", default="")
        parser.add_argument(
            "--fail-on-missing",
            action="store_true",
            help="Exit non-zero when no APIC has eqptCh on a completed device.",
        )

    def handle(self, *args, **options):
        if options["sync_id"] and options["sync_name"]:
            raise CommandError("Use either --sync-id or --sync-name, not both.")
        sync = self._resolve_sync(options)
        if sync is None:
            raise CommandError("No sync found for the requested selector.")

        try:
            payload = audit_apic_cimc_readiness(sync)
        except ForwardSyncError as exc:
            raise CommandError(str(exc)) from exc
        ready = payload["cimc_inventory_ready"]
        self.stdout.write(json.dumps(payload, indent=2, default=str))

        if options["fail_on_missing"] and not ready:
            raise SystemExit(1)

    def _resolve_sync(self, options):
        sync_id = int(options.get("sync_id") or 0)
        sync_name = (options.get("sync_name") or "").strip()
        if sync_id:
            return ForwardSync.objects.filter(pk=sync_id).first()
        if sync_name:
            return ForwardSync.objects.filter(name=sync_name).first()
        return ForwardSync.objects.order_by("-id").first()
