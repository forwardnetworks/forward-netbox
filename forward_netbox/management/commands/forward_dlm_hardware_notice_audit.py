import json

from django.core.management.base import BaseCommand
from django.core.management.base import CommandError

from forward_netbox.models import ForwardSync
from forward_netbox.utilities.dlm_notice_audit import delete_stale_hardware_notices
from forward_netbox.utilities.dlm_notice_audit import emitted_device_type_slugs
from forward_netbox.utilities.dlm_notice_audit import fetch_emitted_hardware_notice_rows
from forward_netbox.utilities.dlm_notice_audit import stale_hardware_notices


class Command(BaseCommand):
    help = (
        "Audit DLM hardware notices against what Forward currently emits. A "
        "notice whose device type is absent from the live hardware-notice "
        "result is stale: usually a device-type map re-pointed at its "
        "alias-aware variant, which writes the same hardware under the Device "
        "Type Library name and leaves the previous row behind. Removals reach "
        "NetBox only from a Forward diff of the query now in use, so nothing "
        "revisits rows the previous query wrote. Queries Forward; reports "
        "unless --apply."
    )

    def add_arguments(self, parser):
        parser.add_argument("--sync-id", type=int, default=0)
        parser.add_argument("--sync-name", default="")
        parser.add_argument(
            "--prune",
            action="store_true",
            help="Delete the stale notices. Reports what would go unless --apply.",
        )
        parser.add_argument(
            "--apply",
            action="store_true",
            help="With --prune, actually delete instead of dry-run.",
        )
        parser.add_argument("--limit", type=int, default=25, help="Sample size.")
        parser.add_argument(
            "--fail-on-stale",
            action="store_true",
            help="Exit non-zero when any stale notice exists.",
        )

    def handle(self, *args, **options):
        if options["sync_id"] and options["sync_name"]:
            raise CommandError("Use either --sync-id or --sync-name, not both.")
        sync = self._resolve_sync(options)
        if sync is None:
            raise CommandError("No sync found for the requested selector.")
        if not sync.get_network_id():
            raise CommandError("Sync source has no network configured.")

        rows, fetch_error = fetch_emitted_hardware_notice_rows(sync)
        if rows is None:
            self.stdout.write(
                json.dumps(
                    {"available": False, "reason": fetch_error},
                    indent=2,
                )
            )
            raise SystemExit(2)
        # Deleting needs every row, not a page: a sample would act on the first
        # 25 while reporting the full count.
        report = stale_hardware_notices(
            emitted_device_type_slugs(rows),
            sample_limit=None if options["prune"] else int(options["limit"] or 25),
        )
        if not report["available"]:
            self.stdout.write(json.dumps(report, indent=2, default=str))
            raise SystemExit(2 if options["prune"] else 0)

        payload = {
            key: value for key, value in report.items() if key != "stale_notice_ids"
        }
        payload["remediation"] = (
            ""
            if not report["stale_notice_count"]
            else (
                f"{report['stale_notice_count']} hardware notice(s) are attached "
                "to device types Forward no longer emits a notice for, so "
                "nothing will ever refresh or remove them. The usual cause is a "
                "device-type map re-pointed at its alias-aware variant, which "
                "writes the same hardware under the Device Type Library name "
                "and leaves the previous row behind. A notice is derived data: "
                "if it still applied, Forward would still be emitting it. "
                "Re-run with --prune --apply to delete them. The device types "
                "themselves are left alone; an empty one may have come from a "
                "Device Type Library import and is not evidence of a mistake. "
                "Note that a notice is NOT stale merely because its device type "
                "holds no devices - notices are written network-wide while "
                "devices are imported tag-scoped, so hardware outside the "
                "include tags legitimately has none."
            )
        )

        if options["prune"]:
            payload["prune_requested"] = True
            payload["prune_applied"] = False
            payload["prune_candidate_count"] = report["stale_notice_count"]
            if options["apply"] and report["stale_notice_count"]:
                result = delete_stale_hardware_notices(report["stale_notice_ids"])
                payload["prune_applied"] = True
                payload["deleted_notice_count"] = result["deleted_notice_count"]
            elif report["stale_notice_count"]:
                payload["prune_dry_run_note"] = (
                    "Dry run: re-run with --apply to delete these notices."
                )

        self.stdout.write(json.dumps(payload, indent=2, default=str))

        if options["fail_on_stale"] and report["stale_notice_count"]:
            raise SystemExit(1)

    def _resolve_sync(self, options):
        sync_id = int(options.get("sync_id") or 0)
        sync_name = (options.get("sync_name") or "").strip()
        if sync_id:
            return ForwardSync.objects.filter(pk=sync_id).first()
        if sync_name:
            return ForwardSync.objects.filter(name=sync_name).first()
        return ForwardSync.objects.order_by("-id").first()
