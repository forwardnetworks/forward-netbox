import json
from pathlib import Path

from django.core.management.base import BaseCommand

from forward_netbox.models import ForwardSync
from forward_netbox.utilities.branch_budget import BRANCH_RUN_STATE_PARAMETER
from forward_netbox.utilities.execution_ledger import active_execution_run
from forward_netbox.utilities.execution_ledger import latest_execution_run
from forward_netbox.utilities.sync_state import prune_stale_branch_run_state


class Command(BaseCommand):
    help = (
        "Prune stale legacy compatibility branch-state cache payloads "
        "(`_branch_run`) when execution-ledger history exists and no run is active."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--sync-name",
            default="",
            help="Optional exact sync name filter.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Report what would be pruned without writing changes.",
        )
        parser.add_argument(
            "--output-json",
            default="",
            help="Optional path to write report JSON.",
        )

    def handle(self, *args, **options):
        sync_name = (options.get("sync_name") or "").strip()
        dry_run = bool(options.get("dry_run"))
        output_json = (options.get("output_json") or "").strip()

        syncs = ForwardSync.objects.all().order_by("pk")
        if sync_name:
            syncs = syncs.filter(name=sync_name)

        rows = []
        for sync in syncs:
            latest_run = latest_execution_run(sync)
            active_run = active_execution_run(sync)
            parameters = sync.parameters or {}
            compatibility_present = BRANCH_RUN_STATE_PARAMETER in parameters
            stale_payload = bool(
                latest_run is not None and active_run is None and compatibility_present
            )
            pruned = False
            if stale_payload and not dry_run:
                pruned = prune_stale_branch_run_state(sync)
            rows.append(
                {
                    "sync_id": sync.pk,
                    "sync_name": sync.name,
                    "ledger_history": bool(latest_run is not None),
                    "active_execution_run": bool(active_run is not None),
                    "compatibility_payload_present": bool(compatibility_present),
                    "stale_payload": stale_payload,
                    "pruned": bool(pruned),
                }
            )

        report = {
            "sync_name_filter": sync_name,
            "dry_run": dry_run,
            "inspected_syncs": len(rows),
            "stale_payload_syncs": sum(1 for row in rows if row["stale_payload"]),
            "pruned_syncs": sum(1 for row in rows if row["pruned"]),
            "rows": rows,
        }
        rendered = json.dumps(report, indent=2, sort_keys=True)
        self.stdout.write(rendered)

        if output_json:
            output_file = Path(output_json)
            if not output_file.is_absolute():
                output_file = Path(__file__).resolve().parents[3] / output_file
            output_file.parent.mkdir(parents=True, exist_ok=True)
            with open(output_file, "w", encoding="utf-8") as handle:
                handle.write(rendered + "\n")
            output_file.chmod(0o666)
            self.stdout.write(
                self.style.SUCCESS(
                    f"Wrote compatibility-cache prune report to {output_json}"
                )
            )
