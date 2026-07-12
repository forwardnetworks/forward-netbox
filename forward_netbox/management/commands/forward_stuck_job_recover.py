import json
import logging

from django.core.management.base import BaseCommand

from forward_netbox.utilities.stuck_recovery import recover_all_stuck_syncs
from forward_netbox.utilities.stuck_recovery import RECOVERY_GRACE_SECONDS

logger = logging.getLogger("forward_netbox.stuck_recovery")


class Command(BaseCommand):
    help = (
        "Recover syncs wedged by a dead worker: a MERGING sync whose worker "
        "died is re-enqueued for merge (idempotent — resumes the unmerged "
        "suffix); a dead sync-run is failed cleanly so schedules resume. "
        "Detection-only by default; pass --apply to act. The companion "
        "forward_stuck_job_alert command stays detect-only."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--apply",
            action="store_true",
            help="Actually recover (default: classify and report only).",
        )
        parser.add_argument(
            "--grace-seconds",
            type=int,
            default=RECOVERY_GRACE_SECONDS,
            help=(
                "Seconds a sync must be wedged before it is eligible "
                f"(default {RECOVERY_GRACE_SECONDS}; must exceed the 180s "
                "liveness heartbeat window)."
            ),
        )
        parser.add_argument(
            "--fail-on-stuck",
            action="store_true",
            help="Exit non-zero when at least one stuck sync is found.",
        )

    def handle(self, *args, **options):
        results = recover_all_stuck_syncs(
            apply=options["apply"],
            grace_seconds=options["grace_seconds"],
        )
        payload = {
            "applied": options["apply"],
            "stuck_sync_count": len(results),
            "results": results,
        }
        self.stdout.write(json.dumps(payload, indent=2, default=str))
        if results and options["fail_on_stuck"]:
            raise SystemExit(1)
