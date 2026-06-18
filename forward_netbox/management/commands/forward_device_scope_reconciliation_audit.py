import json

from django.core.management.base import BaseCommand
from django.core.management.base import CommandError

from forward_netbox.models import ForwardSync
from forward_netbox.utilities.scope_reconciliation import compute_scope_reconciliation
from forward_netbox.utilities.scope_reconciliation import prune_orphan_devices


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

        if not sync.get_network_id():
            raise CommandError("Sync source has no network configured.")

        report = compute_scope_reconciliation(sync)
        out_of_scope = report["_out_of_scope"]
        tagged_names = report["_tagged_names"]
        payload = {
            key: value for key, value in report.items() if not key.startswith("_")
        }
        payload["remediation"] = (
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
        )

        if options["prune_orphans"] and out_of_scope:
            payload["prune_requested"] = True
            payload["prune_applied"] = False
            payload["prune_candidate_count"] = len(out_of_scope)
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
                result = prune_orphan_devices(sync, report=report)
                payload["prune_applied"] = True
                payload["pruned_object_count"] = result["pruned_object_count"]
                payload["pruned_device_count"] = result["pruned_device_count"]
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
