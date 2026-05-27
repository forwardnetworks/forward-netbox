import json
from pathlib import Path

from django.core.management.base import BaseCommand
from django.core.management.base import CommandError

from forward_netbox.models import ForwardExecutionRun
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.execution_ledger import execution_run_support_bundle
from forward_netbox.utilities.execution_ledger import latest_execution_run
from forward_netbox.utilities.execution_ledger import reconcile_execution_run
from forward_netbox.utilities.scale_benchmark import scale_benchmark_report
from forward_netbox.utilities.sensitive_content import format_finding
from forward_netbox.utilities.sensitive_content import load_sensitive_patterns
from forward_netbox.utilities.sensitive_content import scan_text


class Command(BaseCommand):
    help = "Emit a scale benchmark report from execution-run support evidence."

    def add_arguments(self, parser):
        parser.add_argument(
            "--sync-name",
            default="",
            help="ForwardSync name whose latest execution run should be evaluated.",
        )
        parser.add_argument(
            "--run-id",
            default="",
            help="Specific ForwardExecutionRun primary key to evaluate.",
        )
        parser.add_argument(
            "--input-json",
            default="",
            help="Optional execution-run support bundle JSON to evaluate offline.",
        )
        parser.add_argument(
            "--output-json",
            default="",
            help="Optional path to write the benchmark report JSON.",
        )
        parser.add_argument(
            "--reconcile",
            action="store_true",
            help=(
                "Reconcile the selected live execution run before exporting the "
                "support bundle. Not supported with --input-json."
            ),
        )
        parser.add_argument(
            "--fail-on-warn",
            action="store_true",
            help="Exit non-zero when benchmark status is warn or fail.",
        )
        parser.add_argument(
            "--fail-on-fail",
            action="store_true",
            help="Exit non-zero when benchmark status is fail.",
        )

    def handle(self, *args, **options):
        bundle = self._support_bundle(options)
        report = scale_benchmark_report(bundle)
        rendered = json.dumps(report, indent=2, sort_keys=True, default=str)
        self.stdout.write(rendered)

        output_path = (options.get("output_json") or "").strip()
        if output_path:
            output_file = Path(output_path)
            if not output_file.is_absolute():
                output_file = Path(__file__).resolve().parents[3] / output_file
            output_file.parent.mkdir(parents=True, exist_ok=True)
            with open(output_file, "w", encoding="utf-8") as handle:
                handle.write(rendered + "\n")
            output_file.chmod(0o666)
            self.stdout.write(
                self.style.SUCCESS(f"Wrote scale benchmark report to {output_path}")
            )

        status = report.get("status")
        if options.get("fail_on_warn") and status in {"warn", "fail"}:
            raise CommandError(f"Scale benchmark status is `{status}`.")
        if options.get("fail_on_fail") and status == "fail":
            raise CommandError("Scale benchmark status is `fail`.")

    def _support_bundle(self, options):
        input_json = (options.get("input_json") or "").strip()
        if input_json:
            if options.get("reconcile"):
                raise CommandError("--reconcile cannot be used with --input-json.")
            try:
                with open(input_json, encoding="utf-8") as handle:
                    raw_text = handle.read()
                self._check_sensitive_input(raw_text, source=input_json)
                return json.loads(raw_text)
            except (OSError, json.JSONDecodeError) as exc:
                raise CommandError(
                    f"Unable to read support bundle JSON `{input_json}`: {exc}"
                ) from exc

        run_id = (options.get("run_id") or "").strip()
        sync_name = (options.get("sync_name") or "").strip()
        if run_id and sync_name:
            raise CommandError("Use either --run-id or --sync-name, not both.")
        if run_id:
            run = ForwardExecutionRun.objects.filter(pk=run_id).first()
            if run is None:
                raise CommandError(f"Forward execution run `{run_id}` was not found.")
            if options.get("reconcile"):
                reconcile_execution_run(run)
                run.refresh_from_db()
            return execution_run_support_bundle(run)
        if sync_name:
            sync = ForwardSync.objects.filter(name=sync_name).first()
            if sync is None:
                raise CommandError(f"Forward sync `{sync_name}` was not found.")
            run = latest_execution_run(sync)
            if run is None:
                raise CommandError(
                    f"Forward sync `{sync_name}` has no execution runs to benchmark."
                )
            if options.get("reconcile"):
                reconcile_execution_run(run)
                run.refresh_from_db()
            return execution_run_support_bundle(run)
        raise CommandError("Provide --input-json, --run-id, or --sync-name.")

    def _check_sensitive_input(self, raw_text: str, *, source: str):
        repo_root = Path(__file__).resolve().parents[3]
        patterns = load_sensitive_patterns(repo_root)
        findings = scan_text(raw_text, source=source, patterns=patterns)
        if not findings:
            return
        rendered = "\n".join(format_finding(finding) for finding in findings[:10])
        extra = ""
        if len(findings) > 10:
            extra = f"\n... and {len(findings) - 10} more finding(s)"
        raise CommandError(
            "Support bundle input contains configured sensitive content. "
            "Sanitize the bundle or add local-only patterns before using it as "
            f"architecture evidence.\n{rendered}{extra}"
        )
