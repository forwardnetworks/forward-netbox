import json

from django.core.management.base import BaseCommand
from django.core.management.base import CommandError

from forward_netbox.models import ForwardSync

# The CIMC inventory map (Forward ACI APIC CIMC Inventory -> dcim.inventoryitem)
# parses this APIC custom command. Without it collected on the APICs in the
# synced snapshot, the map yields zero inventory items even though APIC devices
# exist. This audit reports whether the data the map needs is present.
EQPTCH_COMMAND_TEXT = "moquery -c eqptCh -a all"

READINESS_QUERY = """
foreach device in network.devices
where matches(toLowerCase(replace(toString(device.platform.os), "OS.", "")), "*apic*")
let has_controller_detail = isPresent(
  min(
    foreach c in device.outputs.commands
    where c.commandType == CommandType.CISCO_APIC_CONTROLLER_DETAIL
    select c.commandType
  )
)
let has_eqptch = isPresent(
  min(
    foreach c in device.outputs.commands
    where c.commandType == CommandType.CUSTOM
      && c.commandText == "moquery -c eqptCh -a all"
    select c.commandText
  )
)
let completed = device.snapshotInfo.result == DeviceSnapshotResult.completed
select {
  has_controller_detail: has_controller_detail,
  has_eqptch: has_eqptch,
  completed: completed
}
"""


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

        network_id = sync.get_network_id()
        if not network_id:
            raise CommandError("Sync source has no network configured.")

        client = sync.source.get_client()
        snapshot_id = sync.resolve_snapshot_id(client)
        rows = client.run_nqe_query(
            query=READINESS_QUERY,
            network_id=network_id,
            snapshot_id=snapshot_id,
            fetch_all=True,
        )

        apic_count = len(rows)
        with_controller_detail = sum(1 for r in rows if r.get("has_controller_detail"))
        with_eqptch = sum(1 for r in rows if r.get("has_eqptch"))
        completed_with_eqptch = sum(
            1 for r in rows if r.get("has_eqptch") and r.get("completed")
        )

        ready = completed_with_eqptch > 0
        payload = {
            "sync_id": sync.pk,
            "sync_name": sync.name,
            "snapshot_selector": sync.get_snapshot_id(),
            "apic_device_count": apic_count,
            "with_controller_detail": with_controller_detail,
            "with_eqptch_command": with_eqptch,
            "completed_with_eqptch": completed_with_eqptch,
            "cimc_inventory_ready": ready,
            "remediation": (
                ""
                if ready
                else (
                    "No completed APIC device carries "
                    f"`{EQPTCH_COMMAND_TEXT}`. Add it as a recurring custom command "
                    "on the APICs in Forward so it is collected in a completed "
                    "snapshot, then enable the `Forward ACI APIC CIMC Inventory` "
                    "map. APIC and ACI device sync are unaffected."
                )
            ),
        }
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
