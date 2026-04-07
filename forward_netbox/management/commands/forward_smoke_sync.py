import os
import time

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand
from django.core.management.base import CommandError

from forward_netbox.choices import FORWARD_SUPPORTED_MODELS
from forward_netbox.choices import ForwardSourceDeploymentChoices
from forward_netbox.choices import ForwardSyncStatusChoices
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.query_registry import get_query_specs


class Command(BaseCommand):
    help = "Run a live Forward smoke sync using local environment variables."

    def add_arguments(self, parser):
        parser.add_argument(
            "--source-name",
            default=os.getenv("FORWARD_SMOKE_SOURCE_NAME", "smoke-source"),
        )
        parser.add_argument(
            "--sync-name",
            default=os.getenv("FORWARD_SMOKE_SYNC_NAME", "smoke-sync"),
        )
        parser.add_argument(
            "--url",
            default=os.getenv("FORWARD_SMOKE_URL", "https://fwd.app"),
        )
        parser.add_argument(
            "--username",
            default=os.getenv("FORWARD_SMOKE_USERNAME"),
        )
        parser.add_argument(
            "--password",
            default=os.getenv("FORWARD_SMOKE_PASSWORD"),
        )
        parser.add_argument(
            "--network-id",
            default=os.getenv("FORWARD_SMOKE_NETWORK_ID"),
        )
        parser.add_argument(
            "--snapshot-id",
            default=os.getenv("FORWARD_SMOKE_SNAPSHOT_ID", "latestProcessed"),
        )
        parser.add_argument(
            "--models",
            default=os.getenv("FORWARD_SMOKE_MODELS", ""),
            help="Comma-separated NetBox models to enable. Defaults to all supported models.",
        )
        parser.add_argument(
            "--merge",
            action="store_true",
            help="Merge the resulting branch after the sync completes successfully.",
        )
        parser.add_argument(
            "--validate-only",
            action="store_true",
            help="Validate snapshot resolution and built-in/custom query execution without running an ingestion.",
        )
        parser.add_argument(
            "--query-limit",
            type=int,
            default=int(os.getenv("FORWARD_SMOKE_QUERY_LIMIT", "5")),
            help="Maximum rows to fetch per query during --validate-only. Defaults to 5.",
        )

    def handle(self, *args, **options):
        username = options["username"]
        password = options["password"]
        network_id = options["network_id"]
        if not username:
            raise CommandError("Set --username or FORWARD_SMOKE_USERNAME.")
        if not password:
            raise CommandError("Set --password or FORWARD_SMOKE_PASSWORD.")
        if not network_id:
            raise CommandError("Set --network-id or FORWARD_SMOKE_NETWORK_ID.")
        if options["query_limit"] < 1:
            raise CommandError("--query-limit must be at least 1.")

        user_model = get_user_model()
        user = user_model.objects.filter(is_superuser=True).order_by("pk").first()
        if user is None:
            raise CommandError(
                "Create a NetBox superuser before running the smoke sync."
            )

        url = options["url"].rstrip("/")
        source_type = (
            ForwardSourceDeploymentChoices.SAAS
            if url == "https://fwd.app"
            else ForwardSourceDeploymentChoices.CUSTOM
        )

        selected_models = self._selected_models(options["models"])

        source = self._build_source(
            source_name=options["source_name"],
            source_type=source_type,
            url=url,
            username=username,
            password=password,
            network_id=network_id,
        )
        source.validate_connection()

        sync = self._build_sync(
            sync_name=options["sync_name"],
            source=source,
            user=user,
            snapshot_id=options["snapshot_id"],
            selected_models=selected_models,
        )

        if options["validate_only"]:
            self._run_validation_only(sync, query_limit=options["query_limit"])
            return

        self.stdout.write(
            self.style.NOTICE(
                f"Running smoke sync '{sync.name}' against source '{source.name}'"
            )
        )
        sync.sync()
        sync.refresh_from_db()
        ingestion = sync.last_ingestion
        if ingestion is None:
            raise CommandError("Smoke sync finished without creating an ingestion.")

        self.stdout.write(
            f"Sync status: {sync.status}, snapshot: {ingestion.snapshot_id}, issues: {ingestion.issues.count()}"
        )
        self.stdout.write(f"Ingestion URL: {ingestion.get_absolute_url()}")

        if sync.status not in (
            ForwardSyncStatusChoices.READY_TO_MERGE,
            ForwardSyncStatusChoices.COMPLETED,
        ):
            raise CommandError(f"Smoke sync did not finish cleanly: {sync.status}")

        issue_count = ingestion.issues.count()
        if issue_count:
            messages = list(ingestion.issues.values_list("message", flat=True)[:5])
            raise CommandError(
                "Smoke sync completed with issues: "
                + "; ".join(messages)
                + ("" if issue_count <= 5 else f" (+{issue_count - 5} more)")
            )

        if options["merge"]:
            self.stdout.write(self.style.NOTICE("Merging smoke sync branch"))
            ingestion.sync_merge()
            sync.refresh_from_db()
            self.stdout.write(f"Post-merge sync status: {sync.status}")

        self.stdout.write(self.style.SUCCESS("Forward smoke sync completed cleanly."))

    def _build_source(
        self,
        *,
        source_name,
        source_type,
        url,
        username,
        password,
        network_id,
    ):
        source, _ = ForwardSource.objects.update_or_create(
            name=source_name,
            defaults={
                "type": source_type,
                "url": url,
                "parameters": {
                    "username": username,
                    "password": password,
                    "verify": True,
                    "network_id": network_id,
                },
            },
        )
        source.full_clean()
        source.save()
        return source

    def _build_sync(self, *, sync_name, source, user, snapshot_id, selected_models):
        sync_parameters = {"snapshot_id": snapshot_id, "auto_merge": False}
        for model_string in FORWARD_SUPPORTED_MODELS:
            sync_parameters[model_string] = model_string in selected_models

        sync, _ = ForwardSync.objects.update_or_create(
            name=sync_name,
            defaults={
                "source": source,
                "user": user,
                "auto_merge": False,
                "parameters": sync_parameters,
            },
        )
        sync.full_clean()
        sync.save()
        return sync

    def _run_validation_only(self, sync, *, query_limit):
        client = sync.source.get_client()
        network_id = sync.get_network_id()
        snapshot_selector = sync.get_snapshot_id()
        snapshot_id = sync.resolve_snapshot_id(client)
        query_parameters = sync.get_query_parameters()
        maps = sync.get_maps()

        self.stdout.write(
            self.style.NOTICE(
                f"Validating Forward queries for sync '{sync.name}' against network '{network_id}' "
                f"and snapshot '{snapshot_id}' (selector: {snapshot_selector})"
            )
        )

        for model_string in sync.get_model_strings():
            specs = get_query_specs(model_string, maps=maps)
            if not specs:
                raise CommandError(
                    f"No enabled built-in or custom query maps were resolved for {model_string}."
                )

            for spec in specs:
                started = time.perf_counter()
                rows = client.run_nqe_query(
                    query=spec.query,
                    query_id=spec.query_id,
                    commit_id=spec.commit_id,
                    network_id=network_id,
                    snapshot_id=snapshot_id,
                    parameters=spec.merged_parameters(query_parameters),
                    limit=query_limit,
                )
                elapsed_ms = round((time.perf_counter() - started) * 1000, 1)
                self.stdout.write(
                    f"{model_string} | {spec.query_name} | {spec.execution_mode} | "
                    f"rows={len(rows)} | runtime_ms={elapsed_ms}"
                )

        self.stdout.write(
            self.style.SUCCESS("Forward query validation completed cleanly.")
        )

    def _selected_models(self, raw_models):
        if not raw_models.strip():
            return set(FORWARD_SUPPORTED_MODELS)

        selected_models = {
            model.strip() for model in raw_models.split(",") if model.strip()
        }
        invalid_models = sorted(selected_models - set(FORWARD_SUPPORTED_MODELS))
        if invalid_models:
            raise CommandError(
                f"Unsupported smoke-sync models: {', '.join(invalid_models)}"
            )
        if not selected_models:
            raise CommandError("Smoke sync requires at least one NetBox model.")
        return selected_models
