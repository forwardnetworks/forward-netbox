import json

from django.core.management.base import BaseCommand

from forward_netbox.utilities.dlm_notice_audit import audit_stale_hardware_notices
from forward_netbox.utilities.dlm_notice_audit import delete_stale_hardware_notices


class Command(BaseCommand):
    help = (
        "Read-only audit of DLM hardware notices attached to device types that "
        "hold no devices. These are usually left behind by re-pointing the "
        "device-type maps at their alias-aware variants: the base query emits "
        "Forward's model string and the alias variant emits the NetBox Device "
        "Type Library name for the same hardware, so both device types end up "
        "with a notice carrying identical dates and the list appears to hold "
        "duplicates. Removals only ever reach NetBox through a Forward diff, "
        "which reports what the CURRENT query stopped returning, so nothing "
        "revisits rows the previous query wrote. Reports unless --apply."
    )

    def add_arguments(self, parser):
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
        payload = audit_stale_hardware_notices(sample_limit=int(options["limit"] or 25))
        if not payload["available"]:
            self.stdout.write(json.dumps(payload, indent=2, default=str))
            return

        payload["remediation"] = (
            ""
            if not payload["stale_notice_count"]
            else (
                f"{payload['stale_notice_count']} hardware notice(s) are attached "
                "to device types that hold no devices, so they describe hardware "
                "this NetBox does not have. The usual cause is a device-type map "
                "re-pointed at its alias-aware variant, which writes the same "
                "hardware under the Device Type Library name and leaves the "
                "previous row behind - no code path revisits it, because "
                "removals come only from a Forward diff of the query now in use. "
                "A notice is derived data: if it still applies, the next sync "
                "writes it back. Re-run with --prune --apply to delete them. The "
                "device types themselves are left alone; an empty one may have "
                "come from a Device Type Library import and is not evidence of a "
                "mistake."
            )
        )

        if options["prune"]:
            payload["prune_requested"] = True
            payload["prune_applied"] = False
            payload["prune_candidate_count"] = payload["stale_notice_count"]
            if options["apply"] and payload["stale_notice_count"]:
                result = delete_stale_hardware_notices(payload["stale_notice_ids"])
                payload["prune_applied"] = True
                payload["deleted_notice_count"] = result["deleted_notice_count"]
            elif payload["stale_notice_count"]:
                payload["prune_dry_run_note"] = (
                    "Dry run: re-run with --apply to delete these notices."
                )

        # The id list is for the prune path, not for reading.
        payload.pop("stale_notice_ids", None)
        self.stdout.write(json.dumps(payload, indent=2, default=str))

        if options["fail_on_stale"] and payload["stale_notice_count"]:
            raise SystemExit(1)
