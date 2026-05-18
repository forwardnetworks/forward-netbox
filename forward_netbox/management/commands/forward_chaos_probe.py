import json
from pathlib import Path

from django.core.management.base import BaseCommand
from django.core.management.base import CommandError

from forward_netbox.models import ForwardExecutionStep
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.execution_ledger import execution_run_support_bundle
from forward_netbox.utilities.execution_ledger import latest_execution_run


class Command(BaseCommand):
    help = "Probe scenario readiness for chaos worker-kill tests."

    def add_arguments(self, parser):
        parser.add_argument("--sync-name", required=True)
        parser.add_argument(
            "--scenario",
            required=True,
            choices=(
                "stage-before-branch",
                "stage-after-branch",
                "stage-during-apply",
                "merge-during-exec",
            ),
        )
        parser.add_argument(
            "--export-dir",
            default="",
            help="Optional directory to write execution-run support bundle JSON.",
        )

    def handle(self, *args, **options):
        sync = ForwardSync.objects.filter(name=options["sync_name"]).first()
        if sync is None:
            raise CommandError(f"Forward sync `{options['sync_name']}` was not found.")

        run = latest_execution_run(sync)
        if run is None:
            self.stdout.write("0")
            return

        step = self._candidate_step(run, options["scenario"])
        ready = self._is_ready(step, options["scenario"])
        self.stdout.write("1" if ready else "0")

        export_dir = (options.get("export_dir") or "").strip()
        if export_dir:
            target_dir = Path(export_dir)
            target_dir.mkdir(parents=True, exist_ok=True)
            target = target_dir / f"chaos-{options['scenario']}-run-{run.pk}.json"
            target.write_text(
                json.dumps(
                    execution_run_support_bundle(run),
                    indent=2,
                    sort_keys=True,
                    default=str,
                )
                + "\n",
                encoding="utf-8",
            )
            self.stdout.write(str(target))

    def _is_ready(self, step, scenario):
        if step is None:
            return False
        if scenario == "stage-before-branch":
            return (
                step.status in ("queued", "running")
                and not step.branch_name
                and not step.ingestion_id
            )
        if scenario == "stage-after-branch":
            return step.status in ("queued", "running") and bool(step.branch_name)
        if scenario == "stage-during-apply":
            if step.status != "running":
                return False
            attempted = int(step.attempted_row_count or 0)
            applied = int(step.applied_row_count or 0)
            fetched = int(step.fetched_row_count or 0)
            return attempted > 0 or applied > 0 or fetched > 0
        if scenario == "merge-during-exec":
            return step.status in ("merge_queued", "merge_timeout") and bool(
                step.merge_job_id
            )
        return False

    def _candidate_step(self, run, scenario):
        steps = ForwardExecutionStep.objects.filter(run=run).exclude(kind="finalize")
        if scenario == "merge-during-exec":
            merge_step = (
                steps.filter(status__in=("merge_queued", "merge_timeout"))
                .order_by("index")
                .first()
            )
            if merge_step:
                return merge_step
        active_step = (
            steps.filter(status__in=("running", "queued")).order_by("index").first()
        )
        if active_step:
            return active_step
        return steps.order_by("index").first()
