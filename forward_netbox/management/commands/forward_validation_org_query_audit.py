import json
import os
from pathlib import Path

from django.core.management.base import BaseCommand
from django.core.management.base import CommandError

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
            default=os.getenv("FORWARD_VALIDATION_SOURCE_NAME", "validation-source"),
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

    def handle(self, *args, **options):
        source = self._build_source(options)
        source.validate_connection()
        client = source.get_client()

        if options.get("repair"):
            publish_builtin_nqe_map_queries(
                client=client,
                directory=options["directory"],
                queryset=ForwardNQEMap.objects.select_related("netbox_model"),
                overwrite=False,
                commit_message=options["commit_message"],
                pin_commit=True,
            )

        report = builtin_query_repository_sync_summary(
            client=client,
            repository=options["repository"],
            directory=options["directory"],
        )
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
                self.style.SUCCESS(
                    f"Wrote validation org query audit report to {output_path}"
                )
            )

        if options.get("fail_on_gap") and report["status"] != "pass":
            raise CommandError(
                "Validation org query audit detected gaps. "
                "Inspect `gaps` in the JSON output."
            )

    def _build_source(self, options):
        source_name = (options.get("source_name") or "").strip()
        existing_source = None
        if source_name:
            existing_source = ForwardSource.objects.filter(name=source_name).first()
        existing_parameters = dict(getattr(existing_source, "parameters", {}) or {})
        username = (options.get("username") or "").strip() or existing_parameters.get(
            "username"
        )
        password = (options.get("password") or "").strip() or existing_parameters.get(
            "password"
        )
        network_id = (
            options.get("network_id") or ""
        ).strip() or existing_parameters.get("network_id")
        if not username:
            raise CommandError(
                "Set --username or FORWARD_VALIDATION_USERNAME, or provide an existing source."
            )
        if not password:
            raise CommandError(
                "Set --password or FORWARD_VALIDATION_PASSWORD, or provide an existing source."
            )
        if not network_id:
            raise CommandError(
                "Set --network-id or FORWARD_VALIDATION_NETWORK_ID, or provide an existing source."
            )

        return build_validation_org_query_source(
            source_name=source_name,
            url=(options.get("url") or "").strip()
            or getattr(existing_source, "url", "")
            or "https://fwd.app",
            username=username,
            password=password,
            network_id=network_id,
        )
