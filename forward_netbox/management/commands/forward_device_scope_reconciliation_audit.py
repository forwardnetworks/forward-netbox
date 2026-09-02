import json

from django.core.management.base import BaseCommand
from django.core.management.base import CommandError

from forward_netbox.models import ForwardSync
from forward_netbox.utilities.scope_reconciliation import compute_scope_reconciliation
from forward_netbox.utilities.scope_reconciliation import prune_orphan_devices


class Command(BaseCommand):
    help = (
        "Reconcile NetBox device count against a sync's Forward device tag "
        "scope. Reports which NetBox devices are in scope, which are tagged but "
        "backfilled (not collected in the resolved snapshot), and which are "
        "out of scope entirely (stale leftovers from earlier syncs)."
    )

    def add_arguments(self, parser):
        parser.add_argument("--sync-id", type=int, default=0)
        parser.add_argument("--sync-name", default="")
        parser.add_argument(
            "--fail-on-drift",
            action="store_true",
            help="Exit non-zero when NetBox holds devices outside the tag scope.",
        )
        parser.add_argument(
            "--prune-orphans",
            action="store_true",
            help=(
                "Delete the out-of-scope NetBox devices (those not tagged in the "
                "Forward result). Reports what would be deleted unless --apply is "
                "also passed. Tagged-but-backfilled devices are never pruned."
            ),
        )
        parser.add_argument(
            "--apply",
            action="store_true",
            help="With --prune-orphans, actually delete instead of dry-run.",
        )
        parser.add_argument(
            "--include-quarantined",
            action="store_true",
            help=(
                "Delete out-of-scope devices whose absence has not yet persisted "
                "long enough to be believed. A device disabled in Forward is "
                "indistinguishable from one that was removed, so by default an "
                "absence must repeat across several syncs before it is acted on."
            ),
        )
        parser.add_argument(
            "--allow-scope-shrink",
            action="store_true",
            help=(
                "Proceed even when the out-of-scope set is a large share of "
                "what this sync previously claimed. Without this, a prune that "
                "would remove more than a quarter of the previously claimed "
                "devices is refused as a likely query or tag fault."
            ),
        )

        parser.add_argument(
            "--full",
            action="store_true",
            help=(
                "Print every device name in each bucket, not the 25-name sample "
                "the panel shows. Names are customer data: they go to this "
                "console and nowhere else."
            ),
        )

    def handle(self, *args, **options):
        if options["sync_id"] and options["sync_name"]:
            raise CommandError("Use either --sync-id or --sync-name, not both.")
        sync = self._resolve_sync(options)
        if sync is None:
            raise CommandError("No sync found for the requested selector.")

        if not sync.get_network_id():
            raise CommandError("Sync source has no network configured.")

        report = compute_scope_reconciliation(sync)
        out_of_scope = report["_out_of_scope"]
        device_tagged_names = report.get(
            "_device_tagged_names", report["_tagged_names"]
        )
        payload = {
            key: value for key, value in report.items() if not key.startswith("_")
        }
        absence = report.get("out_of_scope_absence") or {}
        breakdown = (
            ""
            if not absence.get("available")
            else (
                f"Of those, {absence['absent_from_snapshot']} are gone from the "
                f"Forward snapshot entirely, {absence['present_untagged']} are "
                "still in Forward but no longer match the tag predicate, and "
                f"{absence['vendor_excluded']} are classified as custom-command "
                "sources. Only the first group unambiguously left; a large "
                "second group is what a narrowed query or a Forward-side tag "
                "edit looks like. "
            )
        )
        payload["remediation"] = (
            ""
            if not out_of_scope
            else (
                f"{len(out_of_scope)} NetBox devices this sync previously "
                "claimed are absent from the current Forward tag scope result "
                "(neither collected nor backfilled under these tags). That is "
                "what a device leaving scope looks like - and also what a "
                "Forward-side tag edit, a narrowed query, or a partial result "
                "looks like, because membership is decided purely by absence "
                "from the result. " + breakdown + "Confirm in Forward that "
                "these devices really no longer carry the include tags BEFORE "
                "deleting anything; they are ordinary NetBox devices otherwise. "
                "`device_tag_prune_out_of_scope` does NOT remove them (it only "
                "deletes rows the sync query returns, and these are absent from "
                "the result). Once confirmed, re-run with `--prune-orphans "
                "--apply` to delete them."
            )
        )

        if options["full"]:
            # The whole sets, straight from the report's internal keys. The
            # persisted payload keeps names capped at the sample size because
            # it is a diagnostic; a console is not, and "which ones" is the
            # question an operator runs this command to answer.
            payload["full"] = {
                "out_of_scope": sorted(out_of_scope),
                "tagged_but_backfilled": sorted(report.get("_present_backfilled") or ()),
                "owned_uncovered": sorted(report.get("_owned_untagged") or ()),
                "in_scope_missing_from_netbox": sorted(
                    report.get("_missing_in_netbox") or ()
                ),
            }
        if options["prune_orphans"]:
            payload["prune_requested"] = True
            payload["prune_applied"] = False
            payload["prune_candidate_count"] = len(out_of_scope)
            if not device_tagged_names:
                payload["prune_aborted"] = "forward-scope-empty"
                payload["prune_abort_reason"] = (
                    "The Forward scope query returned 0 devices; refusing to prune "
                    "because every NetBox device would be treated as an orphan. "
                    "Check connectivity, the snapshot, and the tag scope, then retry."
                )
                self.stdout.write(json.dumps(payload, indent=2, default=str))
                raise SystemExit(2)
            if options["apply"] and out_of_scope:
                result = prune_orphan_devices(
                    sync,
                    report=report,
                    allow_scope_shrink=options["allow_scope_shrink"],
                    include_quarantined=options["include_quarantined"],
                )
                payload["prune_applied"] = True
                payload["quarantine_held_device_count"] = result.get(
                    "quarantine_held_device_count", 0
                )
                payload["quarantine_overridden_device_count"] = result.get(
                    "quarantine_overridden_device_count", 0
                )
                payload["pruned_object_count"] = result["pruned_object_count"]
                payload["pruned_device_count"] = result["pruned_device_count"]
                if result.get("pruned_dependent_rows"):
                    payload["pruned_dependent_rows"] = result["pruned_dependent_rows"]
            elif out_of_scope:
                payload["prune_dry_run_note"] = (
                    "Dry run: re-run with --apply to delete these devices."
                )

        self.stdout.write(json.dumps(payload, indent=2, default=str))

        if options["fail_on_drift"] and out_of_scope:
            raise SystemExit(1)

    def _resolve_sync(self, options):
        sync_id = int(options.get("sync_id") or 0)
        sync_name = (options.get("sync_name") or "").strip()
        if sync_id:
            return ForwardSync.objects.filter(pk=sync_id).first()
        if sync_name:
            return ForwardSync.objects.filter(name=sync_name).first()
        return ForwardSync.objects.order_by("-id").first()
