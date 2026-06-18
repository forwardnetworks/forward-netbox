import json

from dcim.models import Device
from django.core.management.base import BaseCommand
from django.core.management.base import CommandError

from forward_netbox.models import ForwardSync
from forward_netbox.utilities.forward_api import build_device_tag_scope_where
from forward_netbox.utilities.sync_facade import device_tag_scope

# Cap on how many example names are echoed per bucket so the report stays small
# on large fabrics.
SAMPLE_LIMIT = 25


class Command(BaseCommand):
    help = (
        "Reconcile NetBox device count against a sync's Forward device tag "
        "scope. Reports which NetBox devices are in scope, which are tagged but "
        "backfilled (not collected in the resolved snapshot), and which are "
        "out of scope entirely (stale leftovers from earlier syncs)."
    )

    def add_arguments(self, parser):
        parser.add_argument("--sync-id", type=int, default=0)
        parser.add_argument("--sync-name", default="")
        parser.add_argument(
            "--fail-on-drift",
            action="store_true",
            help="Exit non-zero when NetBox holds devices outside the tag scope.",
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

        include_tags, exclude_tags, include_match = device_tag_scope(sync)
        scope_where = build_device_tag_scope_where(
            include_tags, exclude_tags, include_match
        )

        client = sync.source.get_client()
        snapshot_id = sync.resolve_snapshot_id(client)
        query = "\n".join(
            [
                "foreach device in network.devices",
                "where device.platform.vendor != Vendor.FORWARD_CUSTOM",
                *scope_where,
                "select {",
                "  name: device.name,",
                "  completed: device.snapshotInfo.result "
                "== DeviceSnapshotResult.completed",
                "}",
            ]
        )
        rows = client.run_nqe_query(
            query=query,
            network_id=network_id,
            snapshot_id=snapshot_id,
            fetch_all=True,
        )

        tagged_names = {
            str(row.get("name") or "").strip()
            for row in rows
            if str(row.get("name") or "").strip()
        }
        completed_names = {
            str(row.get("name") or "").strip()
            for row in rows
            if row.get("completed") and str(row.get("name") or "").strip()
        }
        backfilled_names = tagged_names - completed_names

        netbox_names = {
            name
            for name in Device.objects.values_list("name", flat=True)
            if (name or "").strip()
        }

        out_of_scope = netbox_names - tagged_names
        present_backfilled = netbox_names & backfilled_names
        missing_in_netbox = completed_names - netbox_names

        payload = {
            "sync_id": sync.pk,
            "sync_name": sync.name,
            "snapshot_selector": sync.get_snapshot_id(),
            "include_tags": sorted(include_tags),
            "exclude_tags": sorted(exclude_tags),
            "include_match": include_match,
            "netbox_device_count": len(netbox_names),
            "forward_in_scope_completed": len(completed_names),
            "forward_tagged_backfilled": len(backfilled_names),
            "netbox_present_backfilled": len(present_backfilled),
            "netbox_out_of_scope": len(out_of_scope),
            "forward_missing_in_netbox": len(missing_in_netbox),
            "out_of_scope_sample": sorted(out_of_scope)[:SAMPLE_LIMIT],
            "present_backfilled_sample": sorted(present_backfilled)[:SAMPLE_LIMIT],
            "missing_in_netbox_sample": sorted(missing_in_netbox)[:SAMPLE_LIMIT],
            "remediation": (
                ""
                if not out_of_scope
                else (
                    f"{len(out_of_scope)} NetBox devices are not in the Forward "
                    "tag scope (neither collected nor backfilled under this tag). "
                    "These are typically leftovers from an earlier, broader sync. "
                    "Enable `device_tag_prune_out_of_scope` on the sync to delete "
                    "them automatically, or remove them after reviewing the "
                    "out_of_scope_sample."
                )
            ),
        }
        self.stdout.write(json.dumps(payload, indent=2, default=str))

        if options["fail_on_drift"] and out_of_scope:
            raise SystemExit(1)

    def _resolve_sync(self, options):
        sync_id = int(options.get("sync_id") or 0)
        sync_name = (options.get("sync_name") or "").strip()
        if sync_id:
            return ForwardSync.objects.filter(pk=sync_id).first()
        if sync_name:
            return ForwardSync.objects.filter(name=sync_name).first()
        return ForwardSync.objects.order_by("-id").first()
