import json

from dcim.models import Device
from django.core.management.base import BaseCommand
from django.core.management.base import CommandError
from django.db import transaction

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
        parser.add_argument(
            "--prune-orphans",
            action="store_true",
            help=(
                "Delete the out-of-scope NetBox devices (those not tagged in the "
                "Forward result). Reports what would be deleted unless --apply is "
                "also passed. Tagged-but-backfilled devices are never pruned."
            ),
        )
        parser.add_argument(
            "--apply",
            action="store_true",
            help="With --prune-orphans, actually delete instead of dry-run.",
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
                    "These are leftovers from an earlier, broader sync. "
                    "`device_tag_prune_out_of_scope` does NOT remove them (it only "
                    "deletes rows the sync query returns, and these are absent from "
                    "the result). Review `out_of_scope_sample`, then re-run with "
                    "`--prune-orphans --apply` to delete them."
                )
            ),
        }

        if options["prune_orphans"] and out_of_scope:
            payload["prune_requested"] = True
            payload["prune_applied"] = False
            payload["prune_candidate_count"] = len(out_of_scope)
            # Safety guard: if the Forward query returned no scoped devices, every
            # NetBox device looks out-of-scope. That is almost always a failed or
            # empty query, not "delete everything" — refuse to prune.
            if not tagged_names:
                payload["prune_aborted"] = "forward-scope-empty"
                payload["prune_abort_reason"] = (
                    "The Forward scope query returned 0 devices; refusing to prune "
                    "because every NetBox device would be treated as an orphan. "
                    "Check connectivity, the snapshot, and the tag scope, then retry."
                )
                self.stdout.write(json.dumps(payload, indent=2, default=str))
                raise SystemExit(2)
            if options["apply"]:
                payload["prune_applied"] = True
                deleted_total = 0
                with transaction.atomic():
                    # Delete in batches so a very large orphan set does not build a
                    # single oversized IN clause.
                    orphans = sorted(out_of_scope)
                    for start in range(0, len(orphans), 500):
                        batch = orphans[start : start + 500]
                        deleted, _ = Device.objects.filter(name__in=batch).delete()
                        deleted_total += deleted
                payload["pruned_object_count"] = deleted_total
                payload["pruned_device_count"] = len(out_of_scope)
            else:
                payload["prune_dry_run_note"] = (
                    "Dry run: re-run with --apply to delete these devices."
                )

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
