import json

from django.core.management.base import BaseCommand
from django.core.management.base import CommandError

from forward_netbox.models import ForwardSync
from forward_netbox.utilities.logging import SyncLogging
from forward_netbox.utilities.scope_ipam_audit import GLOBAL_IPAM_AUDIT_MODELS
from forward_netbox.utilities.scope_ipam_audit import tag_delete_eligible_ipam


class Command(BaseCommand):
    help = (
        "Tag network-global IPAM (prefixes, VLANs, VRFs) that a sync's latest "
        "Forward fetch no longer reports with `forward-delete-eligible`, for "
        "operator review. Self-healing: the tag set is reconciled to exactly the "
        "stale set on every run, so an object that returns to Forward is untagged "
        "automatically. Never deletes; a model whose Forward fetch returns zero "
        "rows is skipped so an empty/failed fetch cannot flag everything."
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
        payload = tag_delete_eligible_ipam(
            sync,
            client,
            logger,
            models=requested or None,
            sample_limit=int(options["limit"] or 20),
        )
        payload["remediation"] = (
            ""
            if not payload["total_eligible"]
            else (
                f"{payload['total_eligible']} NetBox global IPAM object(s) are "
                f"tagged `{payload['tag_slug']}`. Review them in NetBox (filter "
                f"e.g. /ipam/prefixes/?tag={payload['tag_slug']}) and delete "
                "confirmed-stale objects manually; nothing is deleted "
                "automatically. NetBox PROTECTs objects that still have "
                "dependents, so a delete of an in-use VRF/VLAN will be refused."
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
