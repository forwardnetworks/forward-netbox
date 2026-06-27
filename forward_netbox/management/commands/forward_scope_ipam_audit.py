import json

from django.core.management.base import BaseCommand
from django.core.management.base import CommandError

from forward_netbox.models import ForwardSync
from forward_netbox.utilities.logging import SyncLogging
from forward_netbox.utilities.scope_ipam_audit import audit_global_ipam_scope
from forward_netbox.utilities.scope_ipam_audit import GLOBAL_IPAM_AUDIT_MODELS


class Command(BaseCommand):
    help = (
        "Read-only audit of network-global IPAM (prefixes, VLANs, VRFs) that a "
        "sync's Forward fetch no longer reports. Device-tag scope prune is "
        "device-derived and never removes these global objects, so this reports "
        "NetBox objects whose identity is absent from the latest Forward result "
        "as review candidates. It never deletes anything."
    )

    def add_arguments(self, parser):
        parser.add_argument("--sync-id", type=int, default=0)
        parser.add_argument("--sync-name", default="")
        parser.add_argument(
            "--models",
            default="",
            help=(
                "Comma-separated subset of "
                + ",".join(GLOBAL_IPAM_AUDIT_MODELS)
                + " (default: all enabled)."
            ),
        )
        parser.add_argument("--limit", type=int, default=20, help="Sample size.")
        parser.add_argument(
            "--fail-on-stale",
            action="store_true",
            help="Exit non-zero when any NetBox global IPAM is absent from Forward.",
        )

    def handle(self, *args, **options):
        if options["sync_id"] and options["sync_name"]:
            raise CommandError("Use either --sync-id or --sync-name, not both.")
        sync = self._resolve_sync(options)
        if sync is None:
            raise CommandError("No sync found for the requested selector.")
        if not sync.get_network_id():
            raise CommandError("Sync source has no network configured.")

        requested = [
            model.strip() for model in options["models"].split(",") if model.strip()
        ]
        unsupported = [
            model for model in requested if model not in GLOBAL_IPAM_AUDIT_MODELS
        ]
        if unsupported:
            raise CommandError(
                "Unsupported --models: "
                + ", ".join(unsupported)
                + ". Supported: "
                + ", ".join(GLOBAL_IPAM_AUDIT_MODELS)
                + "."
            )

        client = sync.source.get_client()
        logger = SyncLogging()
        payload = audit_global_ipam_scope(
            sync,
            client,
            logger,
            models=requested or None,
            sample_limit=int(options["limit"] or 20),
        )
        payload["remediation"] = (
            ""
            if not payload["total_stale"]
            else (
                f"{payload['total_stale']} NetBox global IPAM object(s) are absent "
                "from the latest Forward fetch. Device-tag scope prune does not "
                "remove global IPAM (prefixes/VLANs/VRFs are not device-owned). "
                "Review the `stale_sample` per model and delete confirmed-stale "
                "objects manually in NetBox; nothing is deleted automatically."
            )
        )
        self.stdout.write(json.dumps(payload, indent=2, default=str))

        if options["fail_on_stale"] and payload["total_stale"]:
            raise SystemExit(1)

    def _resolve_sync(self, options):
        sync_id = int(options.get("sync_id") or 0)
        sync_name = (options.get("sync_name") or "").strip()
        if sync_id:
            return ForwardSync.objects.filter(pk=sync_id).first()
        if sync_name:
            return ForwardSync.objects.filter(name=sync_name).first()
        return ForwardSync.objects.order_by("-id").first()
