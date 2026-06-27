import json

from django.core.management.base import BaseCommand
from django.core.management.base import CommandError

from forward_netbox.models import ForwardSync
from forward_netbox.utilities.primary_ip_audit import audit_primary_ip_resolution


class Command(BaseCommand):
    help = (
        "Read-only audit of the Mgmt_<iface> primary-IP feature. For each Forward "
        "Mgmt_-tagged device, report whether its primary IP resolves and, if not, "
        "why: the device is absent from NetBox, the Mgmt target interface is not on "
        "the device in NetBox, or the interface is present but has no IP assigned "
        "(the apply/assignment gap). Never writes."
    )

    def add_arguments(self, parser):
        parser.add_argument("--sync-id", type=int, default=0)
        parser.add_argument("--sync-name", default="")
        parser.add_argument("--limit", type=int, default=10, help="Sample size.")

    def handle(self, *args, **options):
        if options["sync_id"] and options["sync_name"]:
            raise CommandError("Use either --sync-id or --sync-name, not both.")
        sync = self._resolve_sync(options)
        if sync is None:
            raise CommandError("No sync found for the requested selector.")
        if not sync.get_network_id():
            raise CommandError("Sync source has no network configured.")

        client = sync.source.get_client()
        payload = audit_primary_ip_resolution(
            sync, client, sample_limit=int(options["limit"] or 10)
        )
        if payload["unresolved"]:
            payload["remediation"] = (
                "Most unresolved devices in `interface_present_no_ip` means the "
                "Mgmt interface exists in NetBox but no IP is assigned to it — a "
                "Forward IP-import/assignment gap (the resolver reads NetBox "
                "assignments). `interface_not_matched` means the target interface "
                "name was not imported. `device_not_in_netbox` means the device is "
                "out of the synced set."
            )
        self.stdout.write(json.dumps(payload, indent=2, default=str))

    def _resolve_sync(self, options):
        sync_id = int(options.get("sync_id") or 0)
        sync_name = (options.get("sync_name") or "").strip()
        if sync_id:
            return ForwardSync.objects.filter(pk=sync_id).first()
        if sync_name:
            return ForwardSync.objects.filter(name=sync_name).first()
        return ForwardSync.objects.order_by("-id").first()
