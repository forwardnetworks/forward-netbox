import json

from django.core.management.base import BaseCommand
from django.core.management.base import CommandError

from forward_netbox.models import ForwardSync
from forward_netbox.utilities.logging import SyncLogging
from forward_netbox.utilities.vsys_parent import link_vsys_parents


class Command(BaseCommand):
    help = (
        "Link virtual-context firewalls (Palo Alto vsys / Fortinet vdom) to their "
        "physical chassis by setting the `forward_parent_device` custom field. "
        "Forward collects each context as its own device whose system.physicalName "
        "names the chassis; this stamps the parent on every such device present in "
        "NetBox. Non-destructive and idempotent; never deletes a device."
    )

    def add_arguments(self, parser):
        parser.add_argument("--sync-id", type=int, default=0)
        parser.add_argument("--sync-name", default="")

    def handle(self, *args, **options):
        if options["sync_id"] and options["sync_name"]:
            raise CommandError("Use either --sync-id or --sync-name, not both.")
        sync = self._resolve_sync(options)
        if sync is None:
            raise CommandError("No sync found for the requested selector.")
        if not sync.get_network_id():
            raise CommandError("Sync source has no network configured.")

        client = sync.source.get_client()
        payload = link_vsys_parents(sync, client, SyncLogging())
        self.stdout.write(json.dumps(payload, indent=2, default=str))

    def _resolve_sync(self, options):
        sync_id = int(options.get("sync_id") or 0)
        sync_name = (options.get("sync_name") or "").strip()
        if sync_id:
            return ForwardSync.objects.filter(pk=sync_id).first()
        if sync_name:
            return ForwardSync.objects.filter(name=sync_name).first()
        return ForwardSync.objects.order_by("-id").first()
