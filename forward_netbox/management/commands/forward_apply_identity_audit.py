import json

from django.core.management.base import BaseCommand
from django.core.management.base import CommandError

from forward_netbox.models import ForwardSync
from forward_netbox.utilities.apply_identity_audit import audit_apply_identity
from forward_netbox.utilities.apply_identity_audit import SIMPLE_MODELS


class Command(BaseCommand):
    help = (
        "Read-only diagnostic for the '1 created + 1 deleted every sync' churn. "
        "For each simple-dimension model it compares Forward-computed natural keys "
        "against NetBox-stored keys and reports the would-create / would-delete "
        "leftovers. A model flagged churn_suspect (both non-empty, often 1/1) "
        "names the object whose key Forward and NetBox disagree on. Never writes."
    )

    def add_arguments(self, parser):
        parser.add_argument("--sync-id", type=int, default=0)
        parser.add_argument("--sync-name", default="")
        parser.add_argument(
            "--models",
            default="",
            help=(
                "Comma-separated subset of "
                + ",".join(SIMPLE_MODELS)
                + " (default: all enabled)."
            ),
        )
        parser.add_argument("--limit", type=int, default=15, help="Sample size.")

    def handle(self, *args, **options):
        if options["sync_id"] and options["sync_name"]:
            raise CommandError("Use either --sync-id or --sync-name, not both.")
        sync = self._resolve_sync(options)
        if sync is None:
            raise CommandError("No sync found for the requested selector.")
        if not sync.get_network_id():
            raise CommandError("Sync source has no network configured.")

        requested = [m.strip() for m in options["models"].split(",") if m.strip()]
        unsupported = [m for m in requested if m not in SIMPLE_MODELS]
        if unsupported:
            raise CommandError(
                "Unsupported --models: "
                + ", ".join(unsupported)
                + ". Supported: "
                + ", ".join(SIMPLE_MODELS)
                + "."
            )

        payload = audit_apply_identity(
            sync,
            models=requested or None,
            sample_limit=int(options["limit"] or 15),
        )
        payload["remediation"] = (
            ""
            if not payload["churn_suspect_models"]
            else (
                "Churn suspected in: "
                + ", ".join(payload["churn_suspect_models"])
                + ". Compare each model's would_create_sample vs "
                "would_delete_sample — they are the same object with a differing "
                "key (e.g. a slug/name that Forward and NetBox compute "
                "differently). That key difference is the bug to fix."
            )
        )
        self.stdout.write(json.dumps(payload, indent=2, default=str))

    def _resolve_sync(self, options):
        sync_id = int(options.get("sync_id") or 0)
        sync_name = (options.get("sync_name") or "").strip()
        if sync_id:
            return ForwardSync.objects.filter(pk=sync_id).first()
        if sync_name:
            return ForwardSync.objects.filter(name=sync_name).first()
        return ForwardSync.objects.order_by("-id").first()
