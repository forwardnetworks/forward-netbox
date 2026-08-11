import json

from django.core.management.base import BaseCommand
from django.core.management.base import CommandError

from forward_netbox.models import ForwardSync
from forward_netbox.utilities.ownership import device_name_ambiguity_report


class Command(BaseCommand):
    help = (
        "Read-only diagnostic for device names that identify more than one "
        "NetBox device. Ownership reconciliation cannot bind such a name to a "
        "row, so it HOLDS it: the devices keep whatever Forward tags they "
        "already carry, and neither gains nor loses one. This command names "
        "the devices behind that count so the duplicate can be resolved. "
        "Covers duplicate NAMES only: a device is also held when its Forward "
        "name changed and its old identity row still binds it, which this "
        "cannot see because it does not know Forward's current name set. "
        "Never writes."
    )

    def add_arguments(self, parser):
        parser.add_argument("--sync-id", type=int, default=0)
        parser.add_argument("--sync-name", default="")
        parser.add_argument(
            "--fail-on-ambiguous",
            action="store_true",
            help="Exit non-zero when any device name identifies two rows.",
        )

    def handle(self, *args, **options):
        if options["sync_id"] and options["sync_name"]:
            raise CommandError("Use either --sync-id or --sync-name, not both.")
        sync = self._resolve_sync(options)
        if sync is None:
            raise CommandError("No sync found for the requested selector.")

        report = device_name_ambiguity_report(sync)
        report["remediation"] = (
            ""
            if not report["ambiguous_names"]
            else (
                f"{len(report['ambiguous_names'])} device name(s) identify more "
                "than one NetBox device, and Forward reports that name for one "
                "device. Ownership holds them rather than guessing, so the sync "
                "completes but those devices' Forward tags are frozen. NetBox "
                "scopes device-name uniqueness to the site, so the usual cause "
                "is a device that moved site or was re-created alongside its "
                "predecessor. Resolve each pair - delete the stale row, or "
                "rename it - and the hold clears on the next sync. Devices "
                "already bound to a different Forward source key are excluded "
                "here; they were never candidates."
            )
        )
        self.stdout.write(json.dumps(report, indent=2, default=str))

        if options["fail_on_ambiguous"] and report["ambiguous_names"]:
            raise SystemExit(1)

    def _resolve_sync(self, options):
        sync_id = int(options.get("sync_id") or 0)
        sync_name = (options.get("sync_name") or "").strip()
        if sync_id:
            return ForwardSync.objects.filter(pk=sync_id).first()
        if sync_name:
            return ForwardSync.objects.filter(name=sync_name).first()
        return ForwardSync.objects.order_by("-id").first()
