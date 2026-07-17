import json
import os
from pathlib import Path

from django.core.management.base import BaseCommand
from django.core.management.base import CommandError
from django.db.models import F

from forward_netbox.choices import ForwardSourceStatusChoices
from forward_netbox.models import ForwardNQEMap
from forward_netbox.models import ForwardSource
from forward_netbox.utilities.query_binding import (
    builtin_query_repository_sync_summary,
)
from forward_netbox.utilities.query_binding import (
    publish_builtin_nqe_map_queries,
)
from forward_netbox.utilities.validation_org_query import (
    build_validation_org_query_source,
)


class Command(BaseCommand):
    help = (
        "Validate that the bundled NQE maps are published and source-matched "
        "in the validation org repository folder."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--source-name",
            default=os.getenv("FORWARD_VALIDATION_SOURCE_NAME", ""),
            help=(
                "Existing Forward source to use. When omitted, the most recent "
                "configured source is selected without exposing its identifier."
            ),
        )
        parser.add_argument(
            "--url",
            default=os.getenv("FORWARD_VALIDATION_URL", "https://fwd.app"),
        )
        parser.add_argument(
            "--username",
            default=os.getenv("FORWARD_VALIDATION_USERNAME"),
        )
        parser.add_argument(
            "--password",
            default=os.getenv("FORWARD_VALIDATION_PASSWORD"),
        )
        parser.add_argument(
            "--network-id",
            default=os.getenv("FORWARD_VALIDATION_NETWORK_ID"),
        )
        parser.add_argument(
            "--repository",
            default=os.getenv("FORWARD_VALIDATION_REPOSITORY", "org"),
        )
        parser.add_argument(
            "--directory",
            default=os.getenv(
                "FORWARD_VALIDATION_DIRECTORY", "/forward_netbox_validation/"
            ),
        )
        parser.add_argument(
            "--commit-message",
            default=os.getenv(
                "FORWARD_VALIDATION_COMMIT_MESSAGE",
                "Sync bundled Forward NQE maps to validation org",
            ),
        )
        parser.add_argument(
            "--repair",
            action="store_true",
            help=(
                "Publish the bundled query set to the validation org before "
                "running the verification gate."
            ),
        )
        parser.add_argument(
            "--overwrite",
            action="store_true",
            help=(
                "When combined with --repair, overwrite existing queries that "
                "differ from the bundled source (not just publish missing ones)."
            ),
        )
        parser.add_argument(
            "--output-json",
            default="",
            help="Optional path to write the report JSON.",
        )
        parser.add_argument(
            "--fail-on-gap",
            action="store_true",
            help=(
                "Exit non-zero when the validation org query folder is missing, "
                "stale, or the bundled query contract has gaps."
            ),
        )
        parser.add_argument(
            "--summary-only",
            action="store_true",
            help=(
                "Print and persist only aggregate status/counts, omitting query "
                "IDs, paths, and detailed gap rows."
            ),
        )

    def handle(self, *args, **options):
        source = self._build_source(options)
        try:
            source.validate_connection()
        except Exception as exc:
            raise CommandError(
                "The configured Forward source failed connection validation."
            ) from exc
        client = source.get_client()

        if options.get("repair"):
            publish_builtin_nqe_map_queries(
                client=client,
                directory=options["directory"],
                queryset=ForwardNQEMap.objects.select_related("netbox_model"),
                overwrite=bool(options.get("overwrite")),
                commit_message=options["commit_message"],
                pin_commit=True,
            )

        report = builtin_query_repository_sync_summary(
            client=client,
            repository=options["repository"],
            directory=options["directory"],
        )
        rendered_report = (
            self._summary_report(report) if options.get("summary_only") else report
        )
        rendered = json.dumps(rendered_report, indent=2, sort_keys=True, default=str)
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
                self.style.SUCCESS(
                    f"Wrote validation org query audit report to {output_path}"
                )
            )

        if options.get("fail_on_gap") and report["status"] != "pass":
            raise CommandError(
                "Validation org query audit detected gaps. "
                "Inspect `gaps` in the JSON output."
            )

    def _summary_report(self, report):
        contract = report.get("query_contract_summary") or {}
        return {
            "status": report.get("status"),
            "gate_status": report.get("gate_status"),
            "query_count": int(report.get("query_count") or 0),
            "published_count": int(report.get("published_count") or 0),
            "matched_count": int(report.get("matched_count") or 0),
            "missing_count": int(report.get("missing_count") or 0),
            "stale_count": int(report.get("stale_count") or 0),
            "lookup_error_count": int(report.get("lookup_error_count") or 0),
            "query_contract_status": contract.get("status"),
            "query_contract_gap_count": len(contract.get("gaps") or []),
        }

    def _build_source(self, options):
        source_name = (options.get("source_name") or "").strip()
        existing_source = None
        if source_name:
            existing_source = ForwardSource.objects.filter(name=source_name).first()
        if existing_source is not None:
            if not self._source_is_configured(existing_source):
                raise CommandError(
                    "The selected Forward source is not fully configured."
                )
            return existing_source

        username = (options.get("username") or "").strip()
        password = (options.get("password") or "").strip()
        network_id = (options.get("network_id") or "").strip()
        supplied = {
            name
            for name, value in {
                "username": username,
                "password": password,
                "network_id": network_id,
            }.items()
            if value
        }
        if not supplied and not source_name:
            source = self._automatic_source()
            if source is not None:
                return source
            raise CommandError(
                "No configured Forward source is available. Configure one in "
                "NetBox or provide the approved bootstrap credential fields."
            )
        if source_name and not supplied:
            raise CommandError("The selected Forward source is unavailable.")
        if "username" not in supplied:
            raise CommandError("Direct source bootstrap requires the username field.")
        if "password" not in supplied:
            raise CommandError("Direct source bootstrap requires the password field.")
        if "network_id" not in supplied:
            raise CommandError("Direct source bootstrap requires the network_id field.")

        return build_validation_org_query_source(
            source_name=source_name or "validation-source",
            url=(options.get("url") or "").strip() or "https://fwd.app",
            username=username,
            password=password,
            network_id=network_id,
        )

    def _automatic_source(self):
        order = (F("last_synced").desc(nulls_last=True), "-pk")
        seen = set()
        configured_sources = []
        querysets = (
            ForwardSource.objects.filter(status=ForwardSourceStatusChoices.READY),
            ForwardSource.objects.all(),
        )
        for queryset in querysets:
            for source in queryset.order_by(*order):
                if source.pk in seen:
                    continue
                seen.add(source.pk)
                if self._source_is_configured(source):
                    configured_sources.append(source)
        if len(configured_sources) == 1:
            return configured_sources[0]
        if len(configured_sources) > 1:
            raise CommandError(
                "Multiple configured Forward sources are available. Select the "
                "validation source explicitly with --source-name."
            )
        return None

    def _source_is_configured(self, source):
        parameters = dict(getattr(source, "parameters", {}) or {})
        return bool(
            str(getattr(source, "url", "") or "").strip()
            and parameters.get("username")
            and parameters.get("password")
            and parameters.get("network_id")
        )
