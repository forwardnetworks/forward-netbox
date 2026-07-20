import os

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand
from django.core.management.base import CommandError
from django.db.models import F

from forward_netbox.choices import forward_configured_models
from forward_netbox.choices import FORWARD_OPTIONAL_MODELS
from forward_netbox.choices import ForwardSourceDeploymentChoices
from forward_netbox.choices import ForwardSourceStatusChoices
from forward_netbox.choices import ForwardSyncStatusChoices
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.branch_budget import build_branch_plan
from forward_netbox.utilities.branch_budget import DEFAULT_MAX_CHANGES_PER_STAGING_ITEM
from forward_netbox.utilities.ingestion_issues import blocking_issues_queryset
from forward_netbox.utilities.query_fetch import ForwardQueryFetcher


class Command(BaseCommand):
    help = (
        "Run a redacted Forward validation or smoke sync using an existing "
        "configured source or explicitly supplied bootstrap credentials."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--source-name",
            default=os.getenv("FORWARD_SMOKE_SOURCE_NAME", ""),
            help=(
                "Existing Forward source to use. When omitted, the most recent "
                "configured source is selected without exposing its identifier."
            ),
        )
        parser.add_argument(
            "--sync-name",
            default=os.getenv("FORWARD_SMOKE_SYNC_NAME", "smoke-sync"),
        )
        parser.add_argument(
            "--url",
            default=os.getenv("FORWARD_SMOKE_URL", "https://fwd.app"),
        )
        parser.add_argument("--username", default=os.getenv("FORWARD_SMOKE_USERNAME"))
        parser.add_argument("--password", default=os.getenv("FORWARD_SMOKE_PASSWORD"))
        parser.add_argument(
            "--network-id", default=os.getenv("FORWARD_SMOKE_NETWORK_ID")
        )
        parser.add_argument(
            "--snapshot-id",
            default=os.getenv("FORWARD_SMOKE_SNAPSHOT_ID", "latestProcessed"),
        )
        parser.add_argument(
            "--models",
            default=os.getenv("FORWARD_SMOKE_MODELS", ""),
            help=(
                "Comma-separated NetBox models to enable. Defaults to required "
                "supported models."
            ),
        )
        parser.add_argument(
            "--check-source",
            action="store_true",
            help="Verify that a configured source can be selected and reached.",
        )
        parser.add_argument(
            "--validate-only",
            action="store_true",
            help="Resolve the snapshot and validate bounded query samples.",
        )
        parser.add_argument(
            "--query-limit",
            type=int,
            default=int(os.getenv("FORWARD_SMOKE_QUERY_LIMIT", "5")),
        )
        parser.add_argument(
            "--plan-only",
            action="store_true",
            help="Build the current single-branch workload plan without applying it.",
        )
        parser.add_argument(
            "--no-auto-merge",
            action="store_true",
            help="Stage the smoke sync for operator review without merging it.",
        )
        parser.add_argument(
            "--max-changes-per-staging-item",
            type=int,
            default=int(
                os.getenv(
                    "FORWARD_SMOKE_MAX_CHANGES_PER_BRANCH",
                    str(DEFAULT_MAX_CHANGES_PER_STAGING_ITEM),
                )
            ),
        )
        parser.add_argument(
            "--enable-bulk-orm",
            action="store_true",
            default=None,
        )
        parser.add_argument("--disable-bulk-orm", action="store_true")

    def handle(self, *args, **options):
        self._validate_options(options)
        source = self._resolve_source(options)
        self._validate_source_connection(source)

        if options["check_source"]:
            self.stdout.write(
                self.style.SUCCESS(
                    "A configured Forward source is available and reachable."
                )
            )
            return

        user = get_user_model().objects.filter(is_superuser=True).order_by("pk").first()
        if user is None:
            raise CommandError(
                "Create a NetBox superuser before running Forward smoke validation."
            )

        sync = self._build_sync(
            sync_name=options["sync_name"],
            source=source,
            user=user,
            snapshot_id=options["snapshot_id"],
            selected_models=self._selected_models(options["models"]),
            auto_merge=not options["no_auto_merge"],
            max_changes_per_staging_item=options["max_changes_per_staging_item"],
            enable_bulk_orm=self._enable_bulk_orm(options),
        )

        if options["validate_only"]:
            self._run_validation_only(sync, query_limit=options["query_limit"])
            return
        if options["plan_only"]:
            self._run_plan_only(
                sync,
                max_changes_per_staging_item=options["max_changes_per_staging_item"],
            )
            return

        sync.sync(max_changes_per_staging_item=options["max_changes_per_staging_item"])
        sync.refresh_from_db()
        ingestion = sync.last_ingestion
        if ingestion is None:
            raise CommandError("Smoke sync finished without creating an ingestion.")

        blocking_count = blocking_issues_queryset(ingestion).count()
        issue_count = ingestion.issues.count()
        self.stdout.write(
            f"Sync status={sync.status} issues={issue_count} "
            f"blocking_issues={blocking_count}"
        )
        if sync.status not in {
            ForwardSyncStatusChoices.READY_TO_MERGE,
            ForwardSyncStatusChoices.COMPLETED,
        }:
            raise CommandError("Forward smoke sync did not finish cleanly.")
        if blocking_count:
            raise CommandError(
                f"Forward smoke sync completed with {blocking_count} blocking issue(s)."
            )
        self.stdout.write(self.style.SUCCESS("Forward smoke sync completed cleanly."))

    def _validate_options(self, options):
        selected_modes = sum(
            bool(options[name])
            for name in ("check_source", "validate_only", "plan_only")
        )
        if selected_modes > 1:
            raise CommandError(
                "--check-source, --validate-only, and --plan-only are mutually exclusive."
            )
        if options["query_limit"] < 1:
            raise CommandError("--query-limit must be at least 1.")
        if options["max_changes_per_staging_item"] < 1:
            raise CommandError("--max-changes-per-staging-item must be at least 1.")

    def _resolve_source(self, options):
        source_name = str(options.get("source_name") or "").strip()
        if source_name:
            source = ForwardSource.objects.filter(name=source_name).first()
            if source is not None:
                if not self._source_is_configured(source):
                    raise CommandError(
                        "The selected Forward source is not fully configured."
                    )
                return source

        credentials = {
            "username": options.get("username"),
            "password": options.get("password"),
            "network_id": options.get("network_id"),
        }
        supplied = {name for name, value in credentials.items() if value}
        if supplied and len(supplied) != len(credentials):
            missing = sorted(set(credentials) - supplied)
            raise CommandError(
                "Direct source bootstrap requires all credential fields; missing: "
                + ", ".join(missing)
            )
        if len(supplied) == len(credentials):
            return self._build_source(
                source_name=source_name or "smoke-source",
                url=str(options.get("url") or "https://fwd.app"),
                username=credentials["username"],
                password=credentials["password"],
                network_id=credentials["network_id"],
            )
        if source_name:
            raise CommandError("The selected Forward source is unavailable.")

        source = self._automatic_source()
        if source is None:
            raise CommandError(
                "No configured Forward source is available. Configure one in NetBox "
                "or provide the approved bootstrap credential fields."
            )
        return source

    def _automatic_source(self):
        order = (F("last_synced").desc(nulls_last=True), "-pk")
        seen = set()
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
                    return source
        return None

    def _source_is_configured(self, source):
        parameters = dict(getattr(source, "parameters", {}) or {})
        return bool(
            str(getattr(source, "url", "") or "").strip()
            and parameters.get("username")
            and parameters.get("password")
            and parameters.get("network_id")
        )

    def _validate_source_connection(self, source):
        try:
            source.validate_connection()
        except Exception as exc:
            raise CommandError(
                "The configured Forward source failed connection validation."
            ) from exc

    def _build_source(self, *, source_name, url, username, password, network_id):
        source_type = (
            ForwardSourceDeploymentChoices.SAAS
            if url.rstrip("/") == "https://fwd.app"
            else ForwardSourceDeploymentChoices.CUSTOM
        )
        source, _ = ForwardSource.objects.update_or_create(
            name=source_name,
            defaults={
                "type": source_type,
                "url": url.rstrip("/"),
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

    def _build_sync(
        self,
        *,
        sync_name,
        source,
        user,
        snapshot_id,
        selected_models,
        auto_merge,
        max_changes_per_staging_item,
        enable_bulk_orm,
    ):
        existing_sync = ForwardSync.objects.filter(name=sync_name).first()
        parameters = dict(getattr(existing_sync, "parameters", {}) or {})
        parameters.update(
            {
                "snapshot_id": snapshot_id,
                "auto_merge": auto_merge,
                "max_changes_per_staging_item": max_changes_per_staging_item,
                "enable_bulk_orm": enable_bulk_orm,
            }
        )
        for model_string in forward_configured_models():
            parameters[model_string] = model_string in selected_models
        sync, _ = ForwardSync.objects.update_or_create(
            name=sync_name,
            defaults={
                "source": source,
                "user": user,
                "auto_merge": auto_merge,
                "parameters": parameters,
            },
        )
        sync.full_clean()
        sync.save()
        return sync

    def _run_validation_only(self, sync, *, query_limit):
        fetcher = ForwardQueryFetcher(sync, sync.source.get_client(), sync.logger)
        context = fetcher.resolve_context()
        results = fetcher.fetch_sample_results(context, row_limit=query_limit)
        for result in results:
            self.stdout.write(
                f"{result.model_string} | rows={result.row_count} | "
                f"runtime_ms={result.runtime_ms}"
            )
        self.stdout.write(
            self.style.SUCCESS(
                f"Forward query validation completed for {len(results)} model(s)."
            )
        )

    def _run_plan_only(self, sync, *, max_changes_per_staging_item):
        fetcher = ForwardQueryFetcher(sync, sync.source.get_client(), sync.logger)
        context = fetcher.resolve_context()
        fetcher.run_preflight(context)
        workloads = fetcher.fetch_workloads(context)
        plan = build_branch_plan(
            workloads,
            max_changes_per_staging_item=max_changes_per_staging_item,
        )
        for item in plan:
            self.stdout.write(
                f"{item.index} | {item.model_string} | "
                f"changes={item.estimated_changes} | "
                f"upserts={len(item.upsert_rows)} | deletes={len(item.delete_rows)}"
            )
        self.stdout.write(
            self.style.SUCCESS(
                f"Single-branch workload planning completed with {len(plan)} item(s)."
            )
        )

    def _enable_bulk_orm(self, options):
        if options.get("disable_bulk_orm"):
            return False
        if options.get("enable_bulk_orm") is True:
            return True
        env_value = os.getenv("FORWARD_SMOKE_ENABLE_BULK_ORM")
        if env_value is None or env_value == "":
            return True
        return str(env_value).strip().lower() in {"1", "true", "yes", "on"}

    def _selected_models(self, raw_models):
        if not str(raw_models or "").strip():
            return set(forward_configured_models()) - set(FORWARD_OPTIONAL_MODELS)
        selected_models = {
            model.strip() for model in raw_models.split(",") if model.strip()
        }
        invalid_models = sorted(selected_models - set(forward_configured_models()))
        if invalid_models:
            raise CommandError(
                "Unsupported smoke-sync models: " + ", ".join(invalid_models)
            )
        if not selected_models:
            raise CommandError("Smoke sync requires at least one NetBox model.")
        return selected_models
